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
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { SpanContext } from '../opContext/spanContextTypes.js';
import type { OpContext, OpContextBinding, OpContextOf } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type { SQLiteWriterConfig } from '../sqlite/sqlite-writer.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import type { Tracer } from '../tracer.js';
import { CompositeTracer } from '../tracers/CompositeTracer.js';
import { NoOpTracer } from '../tracers/NoOpTracer.js';
import { SQLiteTracer } from '../tracers/SQLiteTracer.js';
import { StdioTracer } from '../tracers/StdioTracer.js';

/** bun:test expect() errors start with 'expect(received).' — distinguishes assertion failures from other throws */
function isExpectError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith('expect(');
}

function writeDescribeTag(tag: unknown, describePath: string | null): void {
  if (!describePath || typeof tag !== 'object' || tag === null) {
    return;
  }

  const record = tag as Record<string, unknown>;
  const describeWriter = record.describe;
  if (typeof describeWriter === 'function') {
    describeWriter.call(record, describePath);
    return;
  }

  const batchWriter = record.with;
  if (typeof batchWriter === 'function') {
    batchWriter.call(record, { describe: describePath });
  }
}

type TestBody = () => unknown | Promise<unknown>;
type HarnessSpanContext<B extends OpContextBinding> = SpanContext<OpContextOf<B>>;
type BunTestHarnessBuiltins = { describe: ReturnType<typeof S.category> };
type HarnessSchema<TExt extends SchemaFields> = BunTestHarnessBuiltins & TExt;

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
  /** Optional test-only schema fields merged into span tag/log methods. */
  testLogSchema?: TExt;
};

function buildHarnessSchemaExtensions<TExt extends SchemaFields>(
  binding: OpContextBinding,
  testLogSchema: TExt | undefined,
): HarnessSchema<TExt> {
  const extension = {
    describe: S.category(),
    ...(testLogSchema ?? ({} as TExt)),
  } as HarnessSchema<TExt>;

  const baseColumnNames = new Set(binding.logBinding.logSchema._columnNames);
  const safeExtension = Object.create(null) as Record<string, unknown>;
  for (const [fieldName, fieldSchema] of Object.entries(extension)) {
    if (!baseColumnNames.has(fieldName)) {
      safeExtension[fieldName] = fieldSchema;
    }
  }
  return safeExtension as HarnessSchema<TExt>;
}

function isTruthyEnvFlag(value: unknown): boolean {
  return value === true || value === '1' || value === 'true';
}

function isVerboseTraceEnabled(explicitVerbose: boolean | undefined): boolean {
  if (explicitVerbose !== undefined) {
    return explicitVerbose;
  }

  const globalVerbose = (globalThis as { __LMAO_TEST_TRACE_VERBOSE__?: unknown }).__LMAO_TEST_TRACE_VERBOSE__;
  if (isTruthyEnvFlag(globalVerbose)) {
    return true;
  }

  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
  return isTruthyEnvFlag(env?.LMAO_TEST_TRACE_VERBOSE);
}

type RootSpanRunner<Ctx extends OpContext> = (
  name: string,
  fn: (ctx: SpanContext<Ctx>) => Promise<unknown>,
) => Promise<unknown>;

type ClosableTracer = {
  close?: () => void | Promise<void>;
};

type TracerFactoryOptions = {
  binding: OpContextBinding;
  sqlite: SQLiteWriterConfig | undefined;
  verbose: boolean;
};

function createRootTracer({ binding, sqlite, verbose }: TracerFactoryOptions): Tracer<OpContextBinding> {
  const tracerOptions = {
    bufferStrategy: new JsBufferStrategy(),
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

  return new NoOpTracer(binding, {
    ...tracerOptions,
  });
}

async function closeTracer(tracer: Tracer<OpContextBinding>): Promise<void> {
  const close = (tracer as ClosableTracer).close;
  if (typeof close === 'function') {
    await close.call(tracer);
  }
}

export interface BunTestTracerInstance<B extends OpContextBinding> {
  setup(): void;
  useTestSpan(): HarnessSpanContext<B>;
  getTracer(): Tracer<B>;
  createBunTestMock(bunTestModule: Record<string, unknown>): Record<string, unknown>;
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

let _activeSuiteTracer: BunTestTracerInstance<OpContextBinding> | null = null;

export function installBunTestTracing<B extends OpContextBinding>(tracer: BunTestTracerInstance<B>): void {
  _activeSuiteTracer = tracer as unknown as BunTestTracerInstance<OpContextBinding>;
  tracer.setup();
  mock.module('bun:test', () => tracer.createBunTestMock(bunTest));
}

export function makeBunTestSuiteTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestSuiteTracer<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;
export function makeBunTestSuiteTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestSuiteTracer<ExtendBindingLogSchema<B, HarnessSchema<TExt>>> {
  const useTestTracer = makeTestTracer(binding, options);
  return {
    useTestTracer,
    useTestSpan: () => useTestTracer.useTestSpan(),
    setupBunTestSuiteTracing: () => installBunTestTracing(useTestTracer),
  };
}

/**
 * Create an instance-scoped Bun test tracer harness.
 *
 * Unlike initTraceTestRun(), this keeps tracer/root/ALS state isolated per instance.
 */
export function makeTestTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;
export function makeTestTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<ExtendBindingLogSchema<B, HarnessSchema<TExt>>> {
  type ExtendedBinding = ExtendBindingLogSchema<B, HarnessSchema<TExt>>;
  type SpanCtx = HarnessSpanContext<ExtendedBinding>;

  let tracer: Tracer<ExtendedBinding> | null = null;
  let verboseTrace = false;
  let rootCtx: SpanCtx | null = null;
  let resolveTestRun: (() => void) | null = null;
  let rootTracePromise: Promise<unknown> | null = null;
  let isSetup = false;

  const als = new AsyncLocalStorage<SpanCtx>();

  function assertRootCtx(): SpanCtx {
    if (!rootCtx) throw new Error('Call setup() before wiring bun:test wrappers');
    return rootCtx;
  }

  function assertTracer(): Tracer<ExtendedBinding> {
    if (!tracer) throw new Error('Call setup() in preload before tests');
    return tracer;
  }

  function runTracedTest(name: string, fn: TestBody, describePath: string | null): Promise<unknown> {
    const currentRoot = assertRootCtx();
    return (currentRoot.span as RootSpanRunner<OpContextOf<ExtendedBinding>>)(name, async (ctx) => {
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
    const wrappedDescribe = Object.assign(
      (name: string, fn: () => void) =>
        origDescribe(name, () => {
          describeStack.push(name);
          try {
            fn();
          } finally {
            describeStack.pop();
          }
        }),
      {
        skip: origDescribe.skip,
        only: origDescribe.only,
        todo: origDescribe.todo,
        each: origDescribe.each.bind(origDescribe),
        skipIf: (condition: boolean) => (condition ? origDescribe.skip : wrappedDescribe),
        if: (condition: boolean) => (condition ? wrappedDescribe : origDescribe.skip),
      },
    ) as typeof _describe;

    return wrappedDescribe;
  }

  function createWrappedIt(origIt: typeof _it, describeStack: string[]): typeof _it {
    const wrappedIt = Object.assign(
      (name: string, fn: TestBody) => {
        const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
        return origIt(name, () => runTracedTest(name, fn, describePath));
      },
      {
        skip: origIt.skip,
        only: origIt.only,
        todo: origIt.todo,
        each: origIt.each.bind(origIt),
        skipIf: (condition: boolean) => (condition ? origIt.skip : wrappedIt),
        if: (condition: boolean) => (condition ? wrappedIt : origIt.skip),
      },
    ) as typeof _it;

    return wrappedIt;
  }

  return {
    setup(): void {
      if (isSetup) {
        return;
      }
      isSetup = true;

      const harnessSchema = buildHarnessSchemaExtensions(binding, options?.testLogSchema);
      const extendedSchema = binding.logBinding.logSchema.extend(harnessSchema);
      const extendedBinding = {
        ...binding,
        logBinding: {
          ...binding.logBinding,
          logSchema: extendedSchema,
        },
      } as unknown as ExtendedBinding;

      verboseTrace = isVerboseTraceEnabled(options?.verbose);

      tracer = createRootTracer({
        binding: extendedBinding,
        sqlite: options?.sqlite,
        verbose: verboseTrace,
      }) as Tracer<ExtendedBinding>;

      rootTracePromise = tracer.trace('test-run', (ctx: SpanCtx) => {
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
            await closeTracer(tracer as unknown as Tracer<OpContextBinding>);
          }
        }
      });
    },

    useTestSpan(): SpanCtx {
      const ctx = als.getStore();
      if (!ctx) throw new Error('useTestSpan() called outside of a traced it()');
      return ctx;
    },

    getTracer(): Tracer<ExtendedBinding> {
      return assertTracer();
    },

    createBunTestMock(bunTestModule: Record<string, unknown>): Record<string, unknown> {
      const origIt = bunTestModule.it as typeof _it;
      const origDescribe = bunTestModule.describe as typeof _describe;
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
type RootSpanInvoker = (name: string, fn: (ctx: SpanContext<OpContext>) => Promise<unknown>) => Promise<unknown>;

/** Initialize the root tracer for the entire bun test run. Call once in preload. */
export function initTraceTestRun<B extends OpContextBinding>(opContext: B, options?: BunTestSetupOptions): void {
  _activeSuiteTracer = null;

  // Extend user's schema with `describe` column for test grouping
  const harnessSchema = buildHarnessSchemaExtensions(opContext, options?.testLogSchema);
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
export function createBunTestMock(bunTestModule: Record<string, unknown>): Record<string, unknown> {
  if (!_rootCtx) {
    if (_activeSuiteTracer) {
      return _activeSuiteTracer.createBunTestMock(bunTestModule);
    }
    throw new Error('Call initTraceTestRun() before createBunTestMock()');
  }

  const origIt = bunTestModule.it as typeof _it;
  const origDescribe = bunTestModule.describe as typeof _describe;
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
      (rootCtx.span as RootSpanInvoker)(name, async (ctx: SpanContext<OpContext>) => {
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

/** Get the current span context from AsyncLocalStorage (inside an it() block) */
export function useTestSpan<Ctx extends SpanContext<OpContext> = SpanContext<OpContext>>(): Ctx {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.useTestSpan() as unknown as Ctx;
  }

  const ctx = _als.getStore();
  if (!ctx) throw new Error('useTestSpan() called outside of a traced it()');
  return ctx as unknown as Ctx;
}

/** Get the root tracer instance */
export function getTracer(): Tracer<OpContextBinding> {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.getTracer() as Tracer<OpContextBinding>;
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

describe.skip = _describe.skip;
describe.only = _describe.only;
describe.todo = _describe.todo;
describe.each = _describe.each;
describe.skipIf = _describe.skipIf.bind(_describe);
describe.if = _describe.if.bind(_describe);

/** Wrapped it — creates a child span of the root trace for the test case */
export function it(name: string, fn: () => void | Promise<void>): void {
  const describePath = _standaloneDescribeStack.length > 0 ? _standaloneDescribeStack.join(' > ') : null;
  _it(name, () => {
    if (!_rootCtx) throw new Error('Call initTraceTestRun() in preload before tests');
    return (_rootCtx.span as RootSpanInvoker)(name, async (ctx: SpanContext<OpContext>) => {
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
it.skipIf = _it.skipIf.bind(_it);
it.if = _it.if.bind(_it);

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

/**
 * Auto-discover and load the nearest package's test-suite-tracer from a root preload.
 *
 * Bun only loads `bunfig.toml` from the directory where `bun test` is invoked.
 * When running `bun test packages/foo/src/bar.test.ts` from the monorepo root,
 * the package-level `bunfig.toml` (and its preload) is skipped.
 *
 * This function solves that: call it from a root-level preload and it will:
 * 1. Find the test file path from `process.argv`
 * 2. Walk up to the nearest `package.json` (the package root)
 * 3. Dynamically import `src/test-suite-tracer.ts` if it exists
 * 4. Call `setupBunTestSuiteTracing()` to wire up traced `it()`
 *
 * Packages with custom `bunfig.toml` still take precedence (Bun uses the nearest config).
 *
 * @example
 * ```ts
 * // bunfig.toml (monorepo root)
 * // [test]
 * // preload = ["./test-trace-preload.ts"]
 *
 * // test-trace-preload.ts (monorepo root)
 * import { autoSetupBunTestTracing } from '@smoothbricks/lmao/testing/bun';
 * await autoSetupBunTestTracing();
 * ```
 */
export async function autoSetupBunTestTracing(): Promise<boolean> {
  const testFile = process.argv[1];
  if (!testFile || testFile.endsWith('/bun')) return false;

  const { dirname, join, resolve } = await import('node:path');
  const { existsSync } = await import('node:fs');

  // Walk up from the test file to find the nearest package.json
  let dir = dirname(resolve(testFile));
  const root = dirname(dir); // stop before filesystem root
  let pkgRoot: string | null = null;
  while (dir !== dirname(dir)) {
    if (existsSync(join(dir, 'package.json'))) {
      pkgRoot = dir;
      break;
    }
    dir = dirname(dir);
  }
  if (!pkgRoot) return false;

  // Look for the package's test-suite-tracer
  const tracerPath = join(pkgRoot, 'src', 'test-suite-tracer.ts');
  if (!existsSync(tracerPath)) return false;

  const mod = await import(tracerPath);
  if (typeof mod.setupBunTestSuiteTracing === 'function') {
    mod.setupBunTestSuiteTracing();
    return true;
  }
  return false;
}
