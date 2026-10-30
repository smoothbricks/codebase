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
 * - Each it()/test() creates a child span of the root (rootCtx.span(name, fn)), whatever modifier registered it
 *   (.only, .concurrent, .serial, .failing, .todo, the *If forms, .each); an each row's span takes its label template,
 *   because Bun formats each row's title itself
 * - describe() and its modifiers are wrapped to track nesting — path written to `describe` schema column
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
 * import { DEFAULT_TRACE_DB_PATH, initTraceTestRun, createBunTestMock } from '@smoothbricks/lmao/testing/bun';
 * import { myOpContext } from './src/opContext.js';
 *
 * initTraceTestRun(myOpContext, { sqlite: { dbPath: DEFAULT_TRACE_DB_PATH } });
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
import { afterAll as _afterAll, describe as _describe, it as _it, mock } from 'bun:test';
import { AsyncLocalStorage } from 'node:async_hooks';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { cleanupDebug, cleanupDebugActiveHandles } from '../cleanupDiagnostics.js';
import { defineOpContext } from '../defineOpContext.js';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { SpanContext } from '../opContext/spanContextTypes.js';
import type { DepsConfig, FeatureFlagSchema, OpContext, OpContextBinding, OpContextOf } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { isSpanContext } from '../spanContext.js';
import { openWalDatabase } from '../sqlite/sqlite-wal.js';
import type { SQLiteWriterConfig } from '../sqlite/sqlite-writer.js';
import { DEFAULT_TRACE_DB_PATH, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME } from '../sqlite/trace-db-path.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import type { Tracer } from '../tracer.js';
import { CompositeTracer } from '../tracers/CompositeTracer.js';
import { SQLiteTracer } from '../tracers/SQLiteTracer.js';
import { StdioTracer } from '../tracers/StdioTracer.js';
import { TestTracer as InMemoryTestTracer } from '../tracers/TestTracer.js';

// Preloads wire `sqlite.dbPath` from here rather than spelling the sink path, which is only safe under a directory
// project walkers and watchers ignore.
export { DEFAULT_TRACE_DB_PATH, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME } from '../sqlite/trace-db-path.js';

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

type HarnessSpanContext<B extends OpContextBinding> = SpanContext<OpContextOf<B>>;
type BunTestHarnessBuiltins = { describe: ReturnType<typeof S.category> };
type HarnessSchema<TExt extends SchemaFields> = BunTestHarnessBuiltins & TExt;
type BunDescribe = typeof _describe;
type BunIt = typeof _it;

const ciOnlyDisabledMessage =
  '.only is disabled in CI environments to prevent accidentally skipping tests. To override, set the environment variable CI=false.';

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

type RuntimeHarnessBinding<
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields,
> = OpContextBinding<LogSchema<TFields & (BunTestHarnessBuiltins | HarnessSchema<TExt>)>, FF, Deps, UserCtx>;

function createRuntimeHarnessBinding<
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields,
>(
  binding: OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
  options: BunTestSetupOptions<TExt> | undefined,
): RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt> {
  const harnessSchema: BunTestHarnessBuiltins | HarnessSchema<TExt> =
    options?.extraTestColumns === undefined
      ? buildHarnessSchemaExtensions(binding, undefined)
      : buildHarnessSchemaExtensions(binding, options.extraTestColumns);
  return extendBindingLogSchema<
    OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
    TFields,
    FF,
    Deps,
    UserCtx,
    BunTestHarnessBuiltins | HarnessSchema<TExt>
  >(binding, harnessSchema);
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

function isTracerInstance(value: unknown): value is Tracer<OpContextBinding> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, 'trace') === 'function' &&
    typeof Reflect.get(value, 'flush') === 'function'
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
    // Every test process opens this one sink at once; it must already be WAL when its path appears.
    const db = openWalDatabase(sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH, (path) => new Database(path));
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
    cleanupDebug('tracer.close:start', { tracer: tracer.constructor.name });
    await close.call(tracer);
    cleanupDebug('tracer.close:end', { tracer: tracer.constructor.name });
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
> = BunTestTracerInstance<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;

export type TestTracer<
  B extends OpContextBinding,
  TExt extends SchemaFields = Record<never, never>,
> = BunTestSuiteUseTestTracer<B, TExt>;

export interface ActiveBunTestTracer {
  setup(): void;
  useTestSpan(): unknown;
  getTracer(): unknown;
  createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule;
}

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
):
  | BunTestSuiteTracer<ExtendBindingLogSchema<B, BunTestHarnessBuiltins>>
  | BunTestSuiteTracer<ExtendBindingLogSchema<B, HarnessSchema<TExt>>> {
  // Keep the caller's binding and extension through the public typed factory.
  // Inferring the lower-level factory here widens B to its schema constraint.
  const useTestTracer = makeTestTracer<B, TExt>(binding, options);
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
    useTestSpan: () => useTestSpan<B>(),
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
) {
  return createTestTracerInstance(binding, options);
}
function createTestTracerInstance<
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields = Record<never, never>,
>(
  binding: OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> {
  let activeBinding: RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt> | null = null;
  let tracer: Tracer<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> | null = null;
  let verboseTrace = false;
  let rootCtx: HarnessSpanContext<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> | null = null;
  let resolveTestRun: (() => void) | null = null;
  let rootTracePromise: Promise<unknown> | null = null;
  let isSetup = false;

  const als = new AsyncLocalStorage<HarnessSpanContext<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>>>();

  function assertRootCtx(): HarnessSpanContext<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> {
    if (!rootCtx || !activeBinding || !isSpanContextForBinding(rootCtx, activeBinding)) {
      throw new Error('Call setup() before wiring bun:test wrappers');
    }
    return rootCtx;
  }

  function assertTracer(): Tracer<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> {
    if (!tracer || !activeBinding || !isTracerForBinding(tracer, activeBinding)) {
      throw new Error('Call setup() in preload before tests');
    }
    return tracer;
  }

  function runTracedTest(name: string, describePath: string | null, run: () => unknown): Promise<unknown> {
    const currentRoot = assertRootCtx();
    return currentRoot.span(name, async (ctx) => {
      writeDescribeTag(ctx.tag, describePath);
      try {
        await als.run(ctx, run);
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

  return {
    setup(): void {
      if (isSetup) {
        return;
      }
      isSetup = true;

      const extendedBinding = createRuntimeHarnessBinding(binding, options);
      activeBinding = extendedBinding;

      verboseTrace = isVerboseTraceEnabled(options?.verbose);

      const activeTracer = createRootTracer({
        binding: extendedBinding,
        sqlite: options?.sqlite,
        verbose: verboseTrace,
      });

      tracer = activeTracer;
      rootTracePromise = activeTracer.trace('test-run', (ctx) => {
        rootCtx = ctx;
        return new Promise<void>((resolve) => {
          resolveTestRun = resolve;
        });
      });

      _afterAll(async () => {
        cleanupDebug('instance.afterAll:start');
        cleanupDebugActiveHandles('instance.afterAll:handles:start');
        if (resolveTestRun) {
          cleanupDebug('instance.rootTrace:resolve');
          resolveTestRun();
          resolveTestRun = null;
        }

        if (rootTracePromise) {
          cleanupDebug('instance.rootTrace:await:start');
          await rootTracePromise;
          cleanupDebug('instance.rootTrace:await:end');
          rootTracePromise = null;
        }

        if (tracer) {
          try {
            cleanupDebug('instance.tracer.flush:start', { tracer: tracer.constructor.name });
            await tracer.flush();
            cleanupDebug('instance.tracer.flush:end', { tracer: tracer.constructor.name });
            if (options?.sqlite && rootCtx) {
              const traceId = rootCtx.buffer.trace_id;
              const dbPath = options.sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH;
              console.log(`\n[trace] trace_id: ${traceId} -> ${dbPath}`);
            }
          } catch (e) {
            console.error('[lmao/testing] SQLite flush error:', e);
          } finally {
            await closeTracer(tracer);
            cleanupDebugActiveHandles('instance.afterAll:handles:end');
            cleanupDebug('instance.afterAll:end');
          }
        }
      });
    },

    useTestSpan(): HarnessSpanContext<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> {
      const ctx = als.getStore();
      if (!ctx || !activeBinding || !isSpanContextForBinding(ctx, activeBinding)) {
        throw new Error('useTestSpan() called outside of a traced it()');
      }
      return ctx;
    },

    getTracer(): Tracer<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> {
      return assertTracer();
    },

    createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule {
      const describeStack: string[] = [];
      const wrappedIt = wrapBunRegistration(bunTestModule.it, traceTests(describeStack, runTracedTest));
      return {
        ...bunTestModule,
        describe: wrapBunRegistration(bunTestModule.describe, trackDescribes(describeStack)),
        it: wrappedIt,
        test: wrappedIt,
      };
    },
  };
}

// Global singleton — one root tracer for the entire bun test run
let _tracer: unknown = null;
let _verboseTrace = false;

// Root span context, its resolver, and the trace promise — one root per test run
let _rootCtx: unknown = null;
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

  const tracer = createRootTracer({
    binding: extendedBinding,
    sqlite: options?.sqlite,
    verbose: _verboseTrace,
  });
  _tracer = tracer;

  // Create the single root trace — a long-lived promise keeps it alive
  _rootTracePromise = tracer.trace('test-run', (ctx) => {
    _rootCtx = ctx;
    return new Promise<void>((resolve) => {
      _resolveTestRun = resolve;
    });
  });

  // Register global teardown — resolve root, wait for span-end, then flush tracer outputs
  _afterAll(async () => {
    cleanupDebug('global.afterAll:start');
    cleanupDebugActiveHandles('global.afterAll:handles:start');
    // Resolve the root promise — triggers span-end write via _executeWithContext .then()
    if (_resolveTestRun) {
      cleanupDebug('global.rootTrace:resolve');
      _resolveTestRun();
      _resolveTestRun = null;
    }
    // Await the trace promise so span-end is written to the buffer before flushing
    if (_rootTracePromise) {
      cleanupDebug('global.rootTrace:await:start');
      await _rootTracePromise;
      cleanupDebug('global.rootTrace:await:end');
      _rootTracePromise = null;
    }

    if (isTracerInstance(_tracer)) {
      try {
        cleanupDebug('global.tracer.flush:start', { tracer: _tracer.constructor.name });
        await _tracer.flush();
        cleanupDebug('global.tracer.flush:end', { tracer: _tracer.constructor.name });
        if (options?.sqlite && isSpanContext(_rootCtx)) {
          const traceId = _rootCtx.buffer.trace_id;
          const dbPath = options.sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH;
          console.log(`\n[trace] trace_id: ${traceId} → ${dbPath}`);
        }
      } catch (e) {
        console.error('[lmao/testing] SQLite flush error:', e);
      } finally {
        await closeTracer(_tracer);
        cleanupDebugActiveHandles('global.afterAll:handles:end');
        cleanupDebug('global.afterAll:end');
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
  if (!isSpanContext(_rootCtx)) {
    if (_activeSuiteTracer) {
      return _activeSuiteTracer.createBunTestMock(bunTestModule);
    }
    throw new Error('Call initTraceTestRun() before createBunTestMock()');
  }

  const rootCtx = _rootCtx;
  const als = _als;
  const describeStack: string[] = [];

  function runTracedTest(name: string, describePath: string | null, run: () => unknown) {
    return rootCtx.span(name, async (ctx) => {
      writeDescribeTag(ctx.tag, describePath);
      try {
        await als.run(ctx, run);
        return ctx.ok(undefined); // pass → span-ok
      } catch (error) {
        if (isExpectError(error)) {
          ctx.err(error); // record assertion failure in span
          throw error; // re-throw so bun:test sees the failure
        }
        throw error; // other throw → span-exception
      }
    });
  }

  const wrappedIt = wrapBunRegistration(bunTestModule.it, traceTests(describeStack, runTracedTest));
  return {
    ...bunTestModule,
    describe: wrapBunRegistration(bunTestModule.describe, trackDescribes(describeStack)),
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
export function useTestSpan<B extends OpContextBinding>(): SpanContext<OpContextOf<B>>;
export function useTestSpan(): unknown {
  if (_activeSuiteTracer) {
    const ctx = _activeSuiteTracer.useTestSpan();
    if (!isSpanContext<OpContextOf<OpContextBinding>>(ctx)) {
      throw new TypeError('Active test tracer returned an invalid span context');
    }
    return ctx;
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
    const tracer = _activeSuiteTracer.getTracer();
    if (!isTracerInstance(tracer)) {
      throw new TypeError('Active test tracer returned an invalid tracer');
    }
    return tracer;
  }

  if (!isTracerInstance(_tracer)) throw new Error('Call initTraceTestRun() in preload before tests');
  return _tracer;
}

/** Runs one registered callback, given the values Bun passed, the callback's declared parameter count, and the call. */
type CallbackRun = (values: unknown[], arity: number, call: (args: unknown[]) => unknown) => unknown;

/**
 * Wraps a bun:test registration (`describe`, `it`/`test`, or any member chain of them) so every callback it registers
 * runs through `wrapCallback(label)`, the vitest harness's pattern (vitest-harness.ts `wrapVitestRegistration`). The
 * modifiers (`.only`, `.skip`, `.todo`, `.failing`, `.concurrent`, `.serial`) are wrapped the same way, and the
 * factories (`.each` and the `*If` forms) have the registration they return wrapped. Bun keeps its own argument checks
 * and title formatting, so an each row's label is its template. A function label is named by its `name`.
 */
function wrapBunRegistration<F extends object>(
  registration: F,
  wrapCallback: (label: string) => CallbackRun,
  thisArg?: object,
): F {
  return new Proxy(registration, {
    apply(target, _receiver: unknown, args: unknown[]) {
      if (typeof target !== 'function') throw new TypeError('bun:test registration is not callable');
      // Bun takes `(label, fn, options?)` or the options-first `(label, options, fn)`; `describe(fn)` has no label.
      const index = typeof args[1] === 'function' ? 1 : 2;
      const callback = args[index];
      if (typeof callback === 'function') {
        const label = args[0];
        const run = wrapCallback(typeof label === 'function' ? label.name : String(label));
        // A plain function, not a Proxy: Bun's each refuses a Proxy ("test.each() expects a function as the second
        // argument"). It declares no parameters, so Bun never passes `done`; settleTestCallback stands in for it.
        args[index] = function (this: unknown, ...values: unknown[]): unknown {
          return run(values, callback.length, (callArgs) => Reflect.apply(callback, this, callArgs));
        };
      }
      return Reflect.apply(target, thisArg, args);
    },
    get(target, key) {
      const member: unknown = Reflect.get(target, key);
      if (typeof member !== 'function') return member;
      switch (key) {
        case 'only':
          return process.env.CI ? disabledOnly : wrapBunRegistration(member, wrapCallback, target);
        case 'skip':
        case 'todo':
        case 'failing':
        case 'concurrent':
        case 'serial':
          return wrapBunRegistration(member, wrapCallback, target);
        case 'each':
        case 'if':
        case 'skipIf':
        case 'todoIf':
        case 'failingIf':
        case 'concurrentIf':
        case 'serialIf':
          return new Proxy(member, {
            apply(factory, _factoryReceiver: unknown, factoryArgs: unknown[]) {
              const chosen: unknown = Reflect.apply(factory, target, factoryArgs);
              if (typeof chosen !== 'function') {
                throw new TypeError(`bun:test ${key}() returned no registration function`);
              }
              return wrapBunRegistration(chosen, wrapCallback, target);
            },
          });
        default:
          return member;
      }
    },
  });
}

/** Runs each test callback in its own span, named by the label it registered, under the describe path it registered in. */
function traceTests(
  describeStack: readonly string[],
  runTracedTest: (name: string, describePath: string | null, run: () => unknown) => unknown,
): (label: string) => CallbackRun {
  return (label) => {
    const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
    return (values, arity, call) => runTracedTest(label, describePath, () => settleTestCallback(values, arity, call));
  };
}

/** Runs each describe callback with its label on the describe stack, so the tests it registers record the path. */
function trackDescribes(describeStack: string[]): (label: string) => CallbackRun {
  return (label) => (values, _arity, call) => {
    describeStack.push(label);
    try {
      return call(values);
    } finally {
      describeStack.pop();
    }
  };
}

/**
 * Settles a test callback as Bun settles it (Bun 1.4.3, measured). Bun never passes `done` to the harness's callback,
 * which declares no parameters, so a callback that declares more parameters than the values Bun passed gets the
 * harness's `done` and settles when it is called: `undefined` or `null` passes, and any other value fails the test
 * with that value. A callback that returns a promise settles with that promise, even when it also takes `done`.
 */
function settleTestCallback(values: unknown[], arity: number, call: (args: unknown[]) => unknown): unknown {
  if (arity <= values.length) return call(values);
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  const result = call([
    ...values,
    (error?: unknown) => (error === undefined || error === null ? resolve() : reject(error)),
  ]);
  return result instanceof Promise ? result : promise;
}

function disabledOnly(): never {
  throw new Error(ciOnlyDisabledMessage);
}

// =============================================================================
// Auto-discovery preload
// =============================================================================

export type AutoSetupTarget =
  | {
      /** Root of the single consumer package whose tests are running. */
      readonly packageRoot: string;
    }
  | {
      /** Workspace root for a cross-package run, with its member package roots. */
      readonly workspaceRoot: string;
      readonly packageRoots: readonly string[];
    };

/**
 * Create the root tracer for the current bun test run from a resolved consumer
 * target (see `resolveConsumerTarget` in `./consumer-package.js`).
 *
 * A package target imports that package's own `src/test-suite-tracer.ts` (if
 * present) and writes the sink under the package root. A workspace-root target
 * scans the member packages' tracer modules so cross-package runs keep the
 * merged-schema behavior, writing the sink under the workspace root.
 *
 * Per-package `useTestSpan()` delegates to the global `useTestSpan()` from this
 * module which checks `_activeSuiteTracer` — set by the root preload via
 * `installBunTestTracing()`.
 *
 * @example
 * ```ts
 * // bun trace preload
 * const target = resolveConsumerTarget(process.cwd());
 * if (target.kind === 'package') await autoSetupBunTestTracing({ packageRoot: target.packageRoot });
 * ```
 */
export async function autoSetupBunTestTracing(target: AutoSetupTarget): Promise<boolean> {
  const installSuite = (opContext: OpContextBinding, mergedSchema: Record<string, unknown>, dbDir: string): boolean => {
    const hasCustomSchema = Object.keys(mergedSchema).length > 0;
    const suite = makeBunTestSuiteTracer(opContext, {
      sqlite: { dbPath: join(dbDir, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME) },
      extraTestColumns: hasCustomSchema && isSchemaFieldsRecord(mergedSchema) ? mergedSchema : undefined,
    });

    suite.setupBunTestSuiteTracing();
    return true;
  };

  if ('packageRoot' in target) {
    const tracerPath = join(target.packageRoot, 'src', 'test-suite-tracer.ts');

    if (!existsSync(tracerPath)) {
      return installSuite(defaultAutoSetupOpContext, {}, target.packageRoot);
    }

    const mod = await import(tracerPath);
    const mergedSchema = isSchemaFieldsRecord(mod.extraTestColumns) ? mod.extraTestColumns : {};
    const opContext = isOpContextBindingLike(mod.opContext) ? mod.opContext : defaultAutoSetupOpContext;

    return installSuite(opContext, mergedSchema, target.packageRoot);
  }

  const mergedSchema: Record<string, unknown> = {};
  let opContext: OpContextBinding | null = null;

  for (const packageRoot of target.packageRoots) {
    const tracerPath = join(packageRoot, 'src', 'test-suite-tracer.ts');
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

  return installSuite(opContext ?? defaultAutoSetupOpContext, mergedSchema, target.workspaceRoot);
}
