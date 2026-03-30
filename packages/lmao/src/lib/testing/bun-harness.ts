/// <reference types="bun" />

/**
 * bun:test integration for LMAO trace-testing.
 *
 * Transparently wraps bun:test's it()/test() via mock.module so every test
 * automatically creates a trace span — no import changes needed in test files.
 *
 * Architecture:
 * - initTraceTestRun() in preload sets up a lifecycle tracer pipeline
 * - mock.module('bun:test', ...) in preload patches it()/test() with trace spans
 * - One root trace per test run (tracer.trace('test-run', ...))
 * - Each it()/test() creates a child span of the root (rootCtx.span(name, fn))
 * - describe() is wrapped to track nesting — path written to `describe` schema column
 * - After all tests: flush tracer outputs (SQLite, stdio, or both)
 *
 * mock.module MUST be called from the preload file itself — bun only intercepts
 * subsequent imports when the mock is registered from the entry module context.
 *
 * @example
 * ```typescript
 * // test-setup.ts (preload)
 * import { mock } from 'bun:test';
 * import * as bunTest from 'bun:test';
 * import { initTraceTestRun, createBunTestMock } from '@smoothbricks/lmao/testing/bun';
 * import { myOpContext } from './src/opContext.js';
 *
 * initTraceTestRun(myOpContext, { sqlite: { dbPath: '.trace-results.db' } });
 * mock.module('bun:test', () => createBunTestMock(bunTest));
 *
 * // my-test.test.ts — uses bun:test directly, no import changes needed
 * import { describe, it, expect } from 'bun:test';
 *
 * describe('Order Processing', () => {
 *   it('validates order', () => {
 *     // This it() is automatically wrapped in a child span of the root
 *   });
 * });
 * ```
 *
 * @module testing/bun
 */

import { Database } from 'bun:sqlite';
import * as bunTest from 'bun:test';
import {
  afterAll as _afterAll,
  afterEach as _afterEach,
  beforeAll as _beforeAll,
  beforeEach as _beforeEach,
  describe as _describe,
  expect as _expect,
  it as _it,
  mock,
} from 'bun:test';
import { AsyncLocalStorage } from 'node:async_hooks';
import { defineOpContext } from '../defineOpContext.js';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { SpanContext } from '../opContext/spanContextTypes.js';
import type { DepsConfig, FeatureFlagSchema, OpContext, OpContextBinding, OpContextOf } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { isSpanContext } from '../spanContext.js';
import type { SQLiteWriterConfig } from '../sqlite/sqlite-writer.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import type { Tracer } from '../tracer.js';
import { CompositeTracer } from '../tracers/CompositeTracer.js';
import { SQLiteTracer } from '../tracers/SQLiteTracer.js';
import { StdioTracer } from '../tracers/StdioTracer.js';
import { TestTracer as InMemoryTestTracer } from '../tracers/TestTracer.js';

/** bun:test expect() errors start with 'expect(received).' — distinguishes assertion failures from other throws */
function isExpectError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith('expect(');
}

function writeDescribeTag(tag: unknown, describePath: string | null): void {
  if (!describePath || typeof tag !== 'object' || tag === null) {
    return;
  }

  const describeWriter = Reflect.get(tag, 'describe');
  if (typeof describeWriter === 'function') {
    describeWriter.call(tag, describePath);
    return;
  }

  const batchWriter = Reflect.get(tag, 'with');
  if (typeof batchWriter === 'function') {
    batchWriter.call(tag, { describe: describePath });
  }
}

type TestBody = () => unknown | Promise<unknown>;
type HarnessSpanContext<B extends OpContextBinding> = SpanContext<OpContextOf<B>>;
type BunTestHarnessBuiltins = { describe: ReturnType<typeof S.category> };
type HarnessSchema<TExt extends SchemaFields> = BunTestHarnessBuiltins & TExt;
type BunDescribe = typeof _describe;
type BunIt = typeof _it;

export interface BunTestModuleShape {
  describe: BunDescribe;
  it: BunIt;
  test: BunIt;
  [key: string]: unknown;
}

// WHY: Package-local `nx test` runs Bun from that package directory, so the
// shared preload must not eagerly import unrelated packages' tracer modules just
// to find one opContext. A tiny fallback binding keeps tracing active for
// packages that only need SQLite-backed test spans and do not define their own
// `src/test-suite-tracer.ts`.
const defaultAutoSetupOpContext = defineOpContext({ logSchema: new LogSchema({}) });

type ExtendBindingLogSchema<B extends OpContextBinding, TExt extends SchemaFields> =
  B extends OpContextBinding<infer T, infer FF, infer Deps, infer UserCtx>
    ? OpContextBinding<T extends LogSchema<infer Fields> ? LogSchema<Fields & TExt> : never, FF, Deps, UserCtx>
    : never;

export type BunTestSetupOptions<TExt extends SchemaFields = Record<never, never>> = {
  /**
   * Optional SQLite output path.
   *
   * Omit this to run traced tests without DB persistence.
   */
  sqlite?: SQLiteWriterConfig;
  /** Optional verbose stdout tracing; defaults to env flag detection. */
  verbose?: boolean;
  /** Optional test-only schema columns merged into span tag/log methods. */
  extraTestColumns?: TExt;
};

function buildHarnessSchemaExtensions(binding: OpContextBinding, extraTestColumns: undefined): BunTestHarnessBuiltins;
function buildHarnessSchemaExtensions<TExt extends SchemaFields>(
  binding: OpContextBinding,
  extraTestColumns: TExt,
): HarnessSchema<TExt>;
function buildHarnessSchemaExtensions<TExt extends SchemaFields>(
  binding: OpContextBinding,
  extraTestColumns: TExt | undefined,
): BunTestHarnessBuiltins | HarnessSchema<TExt> {
  const extensionBase: BunTestHarnessBuiltins = { describe: S.category() };
  const baseColumnNames = new Set(binding.logBinding.logSchema._columnNames);
  for (const fieldName of Object.keys(extraTestColumns ?? {})) {
    if (baseColumnNames.has(fieldName)) {
      throw new Error(`Test harness schema column '${fieldName}' already exists in the bound log schema`);
    }
  }

  return extraTestColumns ? { ...extensionBase, ...extraTestColumns } : extensionBase;
}

function extendBindingLogSchema<
  B extends OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields,
>(binding: B, extension: TExt): OpContextBinding<LogSchema<TFields & TExt>, FF, Deps, UserCtx> {
  const extendedBinding: OpContextBinding<LogSchema<TFields & TExt>, FF, Deps, UserCtx> = {
    logBinding: {
      ...binding.logBinding,
      logSchema: binding.logBinding.logSchema.extend(extension),
    },
    flags: binding.flags,
    ctxDefaults: binding.ctxDefaults,
    deps: binding.deps,
  };

  return extendedBinding;
}

function isTruthyEnvFlag(value: unknown): boolean {
  return value === true || value === '1' || value === 'true';
}

function isSchemaFieldsRecord(value: unknown): value is SchemaFields {
  return typeof value === 'object' && value !== null;
}

function isOpContextBindingLike(value: unknown): value is OpContextBinding {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, 'logBinding') === 'object';
}

function isSpanContextForBinding<B extends OpContextBinding>(
  value: unknown,
  binding: B,
): value is SpanContext<OpContextOf<B>> {
  return isSpanContext<OpContextOf<B>>(value) && Reflect.get(value, '_schema') === binding.logBinding.logSchema;
}

function isTracerForBinding<B extends OpContextBinding>(value: unknown, binding: B): value is Tracer<B> {
  const logBinding = typeof value === 'object' && value !== null ? Reflect.get(value, 'logBinding') : null;
  return (
    typeof logBinding === 'object' &&
    logBinding !== null &&
    Reflect.get(logBinding, 'logSchema') === binding.logBinding.logSchema
  );
}

function isTracerForBinding<B extends OpContextBinding>(value: unknown, binding: B): value is Tracer<B> {
  const logBinding = typeof value === 'object' && value !== null ? Reflect.get(value, 'logBinding') : null;
  return (
    typeof logBinding === 'object' &&
    logBinding !== null &&
    Reflect.get(logBinding, 'logSchema') === binding.logBinding.logSchema
  );
}

function isVerboseTraceEnabled(explicitVerbose: boolean | undefined): boolean {
  if (explicitVerbose !== undefined) {
    return explicitVerbose;
  }

  const globalVerbose = Reflect.get(globalThis, '__LMAO_TEST_TRACE_VERBOSE__');
  if (isTruthyEnvFlag(globalVerbose)) {
    return true;
  }

  return isTruthyEnvFlag(process.env.LMAO_TEST_TRACE_VERBOSE);
}

type TracerFactoryOptions<B extends OpContextBinding> = {
  binding: B;
  sqlite: SQLiteWriterConfig | undefined;
  verbose: boolean;
};

function createRootTracer<B extends OpContextBinding>({
  binding,
  sqlite,
  verbose,
}: TracerFactoryOptions<B>): Tracer<B> {
  const tracerOptions = {
    bufferStrategy: new JsBufferStrategy<B['logBinding']['logSchema']>(),
    createTraceRoot,
  } as const;

  if (sqlite) {
    const db = new Database(sqlite.dbPath ?? '.trace-results.db');
    const sqliteTracer = new SQLiteTracer(binding, {
      ...tracerOptions,
      db,
    });

    if (verbose) {
      const stdioTracer = new StdioTracer(binding, {
        ...tracerOptions,
      });

      return new CompositeTracer(binding, {
        ...tracerOptions,
        delegates: [stdioTracer, sqliteTracer],
      });
    }

    return sqliteTracer;
  }

  if (verbose) {
    return new StdioTracer(binding, {
      ...tracerOptions,
    });
  }

  return new InMemoryTestTracer(binding, {
    ...tracerOptions,
  });
}

async function closeTracer<B extends OpContextBinding>(tracer: Tracer<B>): Promise<void> {
  const close = Reflect.get(tracer, 'close');
  if (typeof close === 'function') {
    await close.call(tracer);
  }
}

export interface BunTestTracerInstance<B extends OpContextBinding> {
  setup(): void;
  useTestSpan(): HarnessSpanContext<B>;
  getTracer(): Tracer<B>;
  createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule;
}

export interface BunTestSuiteTracer<B extends OpContextBinding> {
  useTestTracer: BunTestTracerInstance<B>;
  useTestSpan(): HarnessSpanContext<B>;
  setupBunTestSuiteTracing(): void;
}

export type BunTestSuiteUseTestTracer<
  B extends OpContextBinding,
  TExt extends SchemaFields = Record<never, never>,
> = ReturnType<typeof makeBunTestSuiteTracer<B, TExt>>['useTestTracer'];

export type TestTracer<
  B extends OpContextBinding,
  TExt extends SchemaFields = Record<never, never>,
> = BunTestSuiteUseTestTracer<B, TExt>;

type ActiveBunTestTracer = BunTestTracerInstance<OpContextBinding>;

let _activeSuiteTracer: ActiveBunTestTracer | null = null;

/** Returns the active suite tracer set by installBunTestTracing(), or null if none is active. */
export function getActiveSuiteTracer(): ActiveBunTestTracer | null {
  return _activeSuiteTracer;
}

export function installBunTestTracing(tracer: ActiveBunTestTracer): void {
  _activeSuiteTracer = tracer;
  tracer.setup();
  mock.module('bun:test', () => tracer.createBunTestMock(bunTest));
}

export function makeBunTestSuiteTracer<B extends OpContextBinding>(
  binding: B,
  options?: Omit<BunTestSetupOptions, 'extraTestColumns'>,
): BunTestSuiteTracer<ExtendBindingLogSchema<B, BunTestHarnessBuiltins>>;
export function makeBunTestSuiteTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestSuiteTracer<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;
export function makeBunTestSuiteTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestSuiteTracer<OpContextBinding> {
  const useTestTracer = createTestTracerInstance(binding, options);
  return {
    useTestTracer,
    useTestSpan: () => useTestTracer.useTestSpan(),
    setupBunTestSuiteTracing: () => installBunTestTracing(useTestTracer),
  };
}

/**
 * Define a per-package test tracer — metadata for preload discovery + typed span accessor.
 *
 * Returns plain data that `autoSetupBunTestTracing` reads at discovery time, plus a
 * `useTestSpan()` that threads the package-local binding through the generic
 * `useTestSpan<B>()` helper.
 *
 * WHY generic threading: `autoSetupBunTestTracing` creates the active tracer from the
 * same `opContext` binding passed here, so the exported helper can preserve the real
 * span type without a local assertion bridge.
 */
export function defineTestTracer<B extends OpContextBinding>(
  binding: B,
  options?: { extraTestColumns?: SchemaFields },
): {
  useTestSpan: () => SpanContext<OpContextOf<B>>;
  opContext: B;
  extraTestColumns: SchemaFields | undefined;
} {
  return {
    opContext: binding,
    extraTestColumns: options?.extraTestColumns,
    // WHY no re-validation: the global useTestSpan() already validates against
    // the active suite tracer's (extended) binding. Re-checking against the
    // original binding fails when extraTestColumns extend the schema, because
    // the span's _schema is the extended LogSchema, not the original.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: useTestSpan() returns SpanContext typed to the suite binding; narrowing to OpContextOf<B> is safe because initTraceTestRun validates the binding matches
    useTestSpan: () => useTestSpan() as SpanContext<OpContextOf<B>>,
  };
}

/**
 * Create an instance-scoped Bun test tracer harness.
 *
 * Unlike initTraceTestRun(), this keeps tracer/root/ALS state isolated per instance.
 */
export function makeTestTracer<B extends OpContextBinding>(
  binding: B,
  options?: Omit<BunTestSetupOptions, 'extraTestColumns'>,
): BunTestTracerInstance<ExtendBindingLogSchema<B, BunTestHarnessBuiltins>>;
export function makeTestTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;
export function makeTestTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<OpContextBinding> {
  return createTestTracerInstance(binding, options);
}
function createTestTracerInstance<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<OpContextBinding> {
  let activeBinding: OpContextBinding | null = null;
  let tracer: Tracer<OpContextBinding> | null = null;
  let verboseTrace = false;
  let rootCtx: SpanContext<OpContext> | null = null;
  let resolveTestRun: (() => void) | null = null;
  let rootTracePromise: Promise<unknown> | null = null;
  let isSetup = false;

  const als = new AsyncLocalStorage<SpanContext<OpContext>>();

  function assertRootCtx(): SpanContext<OpContext> {
    if (!rootCtx || !activeBinding || !isSpanContextForBinding(rootCtx, activeBinding)) {
      throw new Error('Call setup() before wiring bun:test wrappers');
    }
    return rootCtx;
  }

  function assertTracer(): Tracer<OpContextBinding> {
    if (!tracer || !activeBinding || !isTracerForBinding(tracer, activeBinding)) {
      throw new Error('Call setup() in preload before tests');
    }
    return tracer;
  }

  function runTracedTest(name: string, fn: TestBody, describePath: string | null): Promise<unknown> {
    const currentRoot = assertRootCtx();
    return currentRoot.span(name, async (ctx) => {
      writeDescribeTag(ctx.tag, describePath);
      try {
        await als.run(ctx, fn);
        return ctx.ok(undefined); // pass -> span-ok
      } catch (error) {
        if (isExpectError(error)) {
          ctx.err(error); // record assertion failure in span
          throw error; // re-throw so bun:test sees the failure
        }
        throw error; // other throw -> span-exception
      }
    });
  }

  function createWrappedDescribe(origDescribe: typeof _describe, describeStack: string[]): typeof _describe {
    const wrappedDescribeBase = (name: string, fn: () => void) =>
      origDescribe(name, () => {
        describeStack.push(name);
        try {
          fn();
        } finally {
          describeStack.pop();
        }
      });

    const wrappedDescribe: typeof _describe = Object.assign(wrappedDescribeBase, origDescribe);
    wrappedDescribe.each = origDescribe.each.bind(origDescribe);
    wrappedDescribe.skipIf = (condition: boolean) => (condition ? origDescribe.skip : wrappedDescribe);
    wrappedDescribe.if = (condition: boolean) => (condition ? wrappedDescribe : origDescribe.skip);

    return wrappedDescribe;
  }

  function createWrappedIt(origIt: typeof _it, describeStack: string[]): typeof _it {
    const wrappedItBase = (name: string, fn: TestBody) => {
      const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
      return origIt(name, () => runTracedTest(name, fn, describePath));
    };

    const wrappedIt: typeof _it = Object.assign(wrappedItBase, origIt);
    wrappedIt.each = origIt.each.bind(origIt);
    wrappedIt.todo = origIt.todo?.bind(origIt);
    wrappedIt.skipIf = (condition: boolean) => (condition ? origIt.skip : wrappedIt);
    wrappedIt.if = (condition: boolean) => (condition ? wrappedIt : origIt.skip);

    return wrappedIt;
  }

  return {
    setup(): void {
      if (isSetup) {
        return;
      }
      isSetup = true;

      const harnessSchema =
        options?.extraTestColumns === undefined
          ? buildHarnessSchemaExtensions(binding, undefined)
          : buildHarnessSchemaExtensions(binding, options.extraTestColumns);
      const extendedBinding = extendBindingLogSchema(binding, harnessSchema);
      activeBinding = extendedBinding;

      verboseTrace = isVerboseTraceEnabled(options?.verbose);

      const activeTracer = createRootTracer({
        binding: extendedBinding,
        sqlite: options?.sqlite,
        verbose: verboseTrace,
      });

      tracer = activeTracer;
      rootTracePromise = activeTracer.trace('test-run', (ctx: SpanContext<OpContext>) => {
        rootCtx = ctx;
        return new Promise<void>((resolve) => {
          resolveTestRun = resolve;
        });
      });

      _afterAll(async () => {
        if (resolveTestRun) {
          resolveTestRun();
          resolveTestRun = null;
        }

        if (rootTracePromise) {
          await rootTracePromise;
          rootTracePromise = null;
        }

        if (tracer) {
          try {
            await tracer.flush();
            if (options?.sqlite && rootCtx) {
              const traceId = rootCtx.buffer.trace_id;
              const dbPath = options.sqlite.dbPath ?? '.trace-results.db';
              console.log(`\n[trace] trace_id: ${traceId} -> ${dbPath}`);
            }
          } catch (e) {
            console.error('[lmao/testing] SQLite flush error:', e);
          } finally {
            await closeTracer(tracer);
          }
        }
      });
    },

    useTestSpan(): SpanContext<OpContext> {
      const ctx = als.getStore();
      if (!ctx || !activeBinding || !isSpanContextForBinding(ctx, activeBinding)) {
        throw new Error('useTestSpan() called outside of a traced it()');
      }
      return ctx;
    },

    getTracer(): Tracer<OpContextBinding> {
      return assertTracer();
    },

    createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule {
      const origIt = bunTestModule.it;
      const origDescribe = bunTestModule.describe;
      const describeStack: string[] = [];

      const wrappedDescribe = createWrappedDescribe(origDescribe, describeStack);
      const wrappedIt = createWrappedIt(origIt, describeStack);

      return {
        ...bunTestModule,
        describe: wrappedDescribe,
        it: wrappedIt,
        test: wrappedIt,
      };
    },
  };
}

// Global singleton — one root tracer for the entire bun test run
let _tracer: Tracer<OpContextBinding> | null = null;
let _verboseTrace = false;

// Root span context, its resolver, and the trace promise — one root per test run
let _rootCtx: SpanContext<OpContext> | null = null;
let _resolveTestRun: (() => void) | null = null;
let _rootTracePromise: Promise<unknown> | null = null;

// AsyncLocalStorage for propagating SpanContext to test bodies.
// Each it() runs in its own async context so concurrent tests don't collide.
const _als = new AsyncLocalStorage<SpanContext<OpContext>>();

/** Initialize the root tracer for the entire bun test run. Call once in preload. */
export function initTraceTestRun<B extends OpContextBinding>(opContext: B, options?: BunTestSetupOptions): void {
  _activeSuiteTracer = null;

  // Extend user's schema with `describe` column for test grouping
  const harnessSchema =
    options?.extraTestColumns === undefined
      ? buildHarnessSchemaExtensions(opContext, undefined)
      : buildHarnessSchemaExtensions(opContext, options.extraTestColumns);
  const extendedSchema = opContext.logBinding.logSchema.extend(harnessSchema);
  const extendedBinding = {
    ...opContext,
    logBinding: { ...opContext.logBinding, logSchema: extendedSchema },
  };

  _verboseTrace = isVerboseTraceEnabled(options?.verbose);

  _tracer = createRootTracer({
    binding: extendedBinding,
    sqlite: options?.sqlite,
    verbose: _verboseTrace,
  });

  // Create the single root trace — a long-lived promise keeps it alive
  const tracer = _tracer;
  _rootTracePromise = tracer.trace('test-run', (ctx: SpanContext<OpContext>) => {
    _rootCtx = ctx;
    return new Promise<void>((resolve) => {
      _resolveTestRun = resolve;
    });
  }) as Promise<unknown>;

  // Register global teardown — resolve root, wait for span-end, then flush tracer outputs
  _afterAll(async () => {
    // Resolve the root promise — triggers span-end write via _executeWithContext .then()
    if (_resolveTestRun) {
      _resolveTestRun();
      _resolveTestRun = null;
    }
    // Await the trace promise so span-end is written to the buffer before flushing
    if (_rootTracePromise) {
      await _rootTracePromise;
      _rootTracePromise = null;
    }

    if (_tracer) {
      try {
        await _tracer.flush();
        if (options?.sqlite && _rootCtx) {
          const traceId = _rootCtx.buffer.trace_id;
          const dbPath = options.sqlite.dbPath ?? '.trace-results.db';
          console.log(`\n[trace] trace_id: ${traceId} → ${dbPath}`);
        }
      } catch (e) {
        console.error('[lmao/testing] SQLite flush error:', e);
      } finally {
        await closeTracer(_tracer);
      }
    }
  });
}

/**
 * Create a bun:test mock replacement that wraps it()/test() in trace spans.
 *
 * Call this from the preload file and pass the result to mock.module:
 * ```typescript
 * mock.module('bun:test', () => createBunTestMock(bunTest));
 * ```
 *
 * @param bunTestModule - The original bun:test namespace (`import * as bunTest from 'bun:test'`)
 */
export function createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule {
  if (!_rootCtx) {
    if (_activeSuiteTracer) {
      return _activeSuiteTracer.createBunTestMock(bunTestModule);
    }
    throw new Error('Call initTraceTestRun() before createBunTestMock()');
  }

  const origIt = bunTestModule.it;
  const origDescribe = bunTestModule.describe;
  const rootCtx = _rootCtx;
  const als = _als;
  const describeStack: string[] = [];

  // describe() callbacks run synchronously (just registering tests)
  function wrappedDescribe(name: string, fn: () => void) {
    return origDescribe(name, () => {
      describeStack.push(name);
      try {
        fn();
      } finally {
        describeStack.pop();
      }
    });
  }
  // skipIf/if must return wrappedDescribe (not origDescribe) when the suite should run,
  // so that describe-path tracking stays active for nested it() calls.
  Object.assign(wrappedDescribe, {
    skip: origDescribe.skip,
    only: origDescribe.only,
    todo: origDescribe.todo,
    each: origDescribe.each.bind(origDescribe),
    skipIf: (condition: boolean) => (condition ? origDescribe.skip : wrappedDescribe),
    if: (condition: boolean) => (condition ? wrappedDescribe : origDescribe.skip),
  });

  function wrappedIt(name: string, fn: TestBody) {
    // Capture describe path at registration time (synchronous)
    const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
    return origIt(name, () =>
      rootCtx.span(name, async (ctx: SpanContext<OpContext>) => {
        writeDescribeTag(ctx.tag, describePath);
        try {
          await als.run(ctx, fn);
          return ctx.ok(undefined); // pass → span-ok
        } catch (error) {
          if (isExpectError(error)) {
            ctx.err(error); // record assertion failure in span
            throw error; // re-throw so bun:test sees the failure
          }
          throw error; // other throw → span-exception
        }
      }),
    );
  }
  // skipIf/if must return wrappedIt (not origIt) when the test should run,
  // so the ALS context is set up and useTestSpan() works inside the test body.
  Object.assign(wrappedIt, {
    skip: origIt.skip,
    only: origIt.only,
    todo: origIt.todo,
    each: origIt.each.bind(origIt),
    skipIf: (condition: boolean) => (condition ? origIt.skip : wrappedIt),
    if: (condition: boolean) => (condition ? wrappedIt : origIt.skip),
  });

  return {
    ...bunTestModule,
    describe: wrappedDescribe,
    it: wrappedIt,
    test: wrappedIt,
  };
}

/**
 * Get the current span context from AsyncLocalStorage (inside an it() block).
 *
 * Package-local tracer modules pass their own exported span-context type here so
 * `useTestSpan()` preserves the suite-local schema extension instead of falling
 * back to the erased global singleton shape.
 */
export function useTestSpan<TCtx extends SpanContext<OpContextOf<OpContextBinding>>>(): TCtx;
export function useTestSpan(): SpanContext<OpContextOf<OpContextBinding>> {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.useTestSpan();
  }

  const ctx = _als.getStore();
  if (!ctx) throw new Error('useTestSpan() called outside of a traced it()');
  return ctx;
}

/**
 * Get the root tracer instance.
 *
 * The optional type parameter narrows the return type for consumers that know
 * the concrete binding (e.g. `getTracer<typeof opContext>()`). The underlying
 * tracer is always the one created by `installBunTestTracing()` or `initTraceTestRun()`.
 */
export function getTracer(): Tracer<OpContextBinding> {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.getTracer();
  }

  if (!_tracer) throw new Error('Call initTraceTestRun() in preload before tests');
  return _tracer;
}

/**
 * Wrapped describe — tracks describe nesting for the standalone export path.
 * describe() callbacks run synchronously (just registering tests).
 */
export function describe(name: string, fn: () => void) {
  return _describe(name, () => {
    _standaloneDescribeStack.push(name);
    try {
      fn();
    } finally {
      _standaloneDescribeStack.pop();
    }
  });
}

// Describe stack for standalone exports (non-mock-module path)
const _standaloneDescribeStack: string[] = [];

function assignOptionalBoundTestFunction(target: object, key: string, source: object, thisArg: unknown): void {
  const value = Reflect.get(source, key);
  if (typeof value === 'function') {
    Reflect.set(target, key, value.bind(thisArg));
  }
}

describe.skip = _describe.skip;
describe.only = _describe.only;
describe.todo = _describe.todo;
describe.each = _describe.each;
assignOptionalBoundTestFunction(describe, 'skipIf', _describe, _describe);
assignOptionalBoundTestFunction(describe, 'if', _describe, _describe);

/** Wrapped it — creates a child span of the root trace for the test case */
export function it(name: string, fn: () => void | Promise<void>): void {
  const describePath = _standaloneDescribeStack.length > 0 ? _standaloneDescribeStack.join(' > ') : null;
  _it(name, () => {
    if (!_rootCtx) throw new Error('Call initTraceTestRun() in preload before tests');
    return _rootCtx.span(name, async (ctx: SpanContext<OpContext>) => {
      writeDescribeTag(ctx.tag, describePath);
      try {
        await _als.run(ctx, fn);
        return ctx.ok(undefined); // pass → span-ok
      } catch (error) {
        if (isExpectError(error)) return ctx.err(error); // expect() fail → span-err
        throw error; // other throw → span-exception
      }
    });
  });
}

it.skip = _it.skip;
it.only = _it.only;
it.todo = _it.todo;
it.each = _it.each;
assignOptionalBoundTestFunction(it, 'skipIf', _it, _it);
assignOptionalBoundTestFunction(it, 'if', _it, _it);

// Re-export everything else unchanged
export {
  _afterAll as afterAll,
  _afterEach as afterEach,
  _beforeAll as beforeAll,
  _beforeEach as beforeEach,
  _expect as expect,
};

// =============================================================================
// Auto-discovery preload
// =============================================================================

export type AutoSetupOptions = {
  /** Absolute path to the packages directory to scan for test-suite-tracer.ts files. */
  packagesDir: string;
};

/**
 * Auto-discover package test-suite-tracers and create the root tracer for the
 * current bun test run.
 *
 * When Bun is running from inside `packages/<name>`, prefer that package's own
 * `src/test-suite-tracer.ts` and avoid importing unrelated packages. Repo-root
 * runs still fall back to the full scan so cross-package test runs keep the
 * merged-schema behavior.
 *
 * Per-package `useTestSpan()` delegates to the global `useTestSpan()` from this
 * module which checks `_activeSuiteTracer` — set by the root preload via
 * `installBunTestTracing()`.
 *
 * @example
 * ```ts
 * // test-trace-preload.ts (monorepo root)
 * import { autoSetupBunTestTracing } from './packages/lmao/src/lib/testing/bun-harness.js';
 * await autoSetupBunTestTracing({ packagesDir: './packages' });
 * ```
 */
export async function autoSetupBunTestTracing(options: AutoSetupOptions): Promise<boolean> {
  const { join, relative, resolve, sep } = await import('node:path');
  const { existsSync, readdirSync } = await import('node:fs');

  const packagesDir = resolve(options.packagesDir);
  if (!existsSync(packagesDir)) return false;

  const installSuite = (opContext: OpContextBinding, mergedSchema: Record<string, unknown>): boolean => {
    const hasCustomSchema = Object.keys(mergedSchema).length > 0;
    const suite = makeBunTestSuiteTracer(opContext, {
      sqlite: { dbPath: '.trace-results.db' },
      extraTestColumns: hasCustomSchema && isSchemaFieldsRecord(mergedSchema) ? mergedSchema : undefined,
    });

    suite.setupBunTestSuiteTracing();
    return true;
  };

  const cwd = resolve(process.cwd());
  const relativeToPackagesDir = relative(packagesDir, cwd);
  const isPackageLocalRun =
    relativeToPackagesDir !== '' &&
    !relativeToPackagesDir.startsWith('..') &&
    !relativeToPackagesDir.startsWith(`.${sep}`);

  if (isPackageLocalRun) {
    const [packageName] = relativeToPackagesDir.split(sep);
    const tracerPath = join(packagesDir, packageName, 'src', 'test-suite-tracer.ts');

    if (!existsSync(tracerPath)) {
      return installSuite(defaultAutoSetupOpContext, {});
    }

    const mod = await import(tracerPath);
    const mergedSchema = isSchemaFieldsRecord(mod.extraTestColumns) ? mod.extraTestColumns : {};
    const opContext = isOpContextBindingLike(mod.opContext) ? mod.opContext : defaultAutoSetupOpContext;

    return installSuite(opContext, mergedSchema);
  }

  const mergedSchema: Record<string, unknown> = {};
  let opContext: OpContextBinding | null = null;

  for (const entry of readdirSync(packagesDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const tracerPath = join(packagesDir, entry.name, 'src', 'test-suite-tracer.ts');
    if (!existsSync(tracerPath)) continue;

    const mod = await import(tracerPath);

    // Collect custom schema columns from each package
    if (mod.extraTestColumns && typeof mod.extraTestColumns === 'object') {
      Object.assign(mergedSchema, mod.extraTestColumns);
    }

    // Use the first available opContext (they all bind the same opContext)
    if (!opContext && isOpContextBindingLike(mod.opContext)) {
      opContext = mod.opContext;
    }
  }

  return installSuite(opContext ?? defaultAutoSetupOpContext, mergedSchema);
}
