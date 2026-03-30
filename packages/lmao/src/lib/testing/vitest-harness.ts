/// <reference types="node" />

/**
 * vitest integration for LMAO trace-testing.
 *
 * Same architecture as bun-harness: one root trace per test run,
 * each it() creates a child span, describe path written to `describe` schema column.
 *
 * Supports lifecycle tracer outputs (SQLite, stdio, or both via CompositeTracer).
 *
 * @example
 * ```typescript
 * // vitest.config.ts
 * export default defineConfig({
 *   test: { setupFiles: ['./test-setup.ts'] },
 * });
 *
 * // test-setup.ts
 * import { initTraceTestRun } from '@smoothbricks/lmao/testing/vitest';
 * initTraceTestRun(myOpContext, { sqlite: { dbPath: '.trace-results.db' } });
 *
 * // my-test.test.ts
 * import { describe, it, expect, useTestSpan } from '@smoothbricks/lmao/testing/vitest';
 * ```
 *
 * @module testing/vitest
 */

import {
  afterAll as _afterAll,
  afterEach as _afterEach,
  beforeAll as _beforeAll,
  beforeEach as _beforeEach,
  describe as _describe,
  expect as _expect,
  it as _it,
} from 'vitest';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { SpanContext } from '../opContext/spanContextTypes.js';
import type { OpContextBinding, OpContextOf } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import { isSpanContext } from '../spanContext.js';
import type { AsyncSQLiteDatabase, SyncSQLiteDatabase } from '../sqlite/sqlite-db.js';
import type { SQLiteWriterConfig } from '../sqlite/sqlite-writer.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { Tracer } from '../tracer.js';
import { CompositeTracer } from '../tracers/CompositeTracer.js';
import { SQLiteAsyncTracer, SQLiteTracer } from '../tracers/SQLiteTracer.js';
import { StdioTracer } from '../tracers/StdioTracer.js';
import { TestTracer } from '../tracers/TestTracer.js';

/** vitest expect() errors start with 'expect(received).' — distinguishes assertion failures from other throws */
function isExpectError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith('expect(');
}

function readGlobalValue(key: string): unknown {
  return Reflect.get(globalThis, key);
}

function readProcessEnv(name: string): string | undefined {
  const processValue = Reflect.get(globalThis, 'process');
  if (typeof processValue !== 'object' || processValue === null) {
    return undefined;
  }

  const env = Reflect.get(processValue, 'env');
  if (typeof env !== 'object' || env === null) {
    return undefined;
  }

  const value = Reflect.get(env, name);
  return typeof value === 'string' ? value : undefined;
}

function isVitestHarnessDebugEnabled(): boolean {
  const globalDebug = readGlobalValue('__LMAO_VITEST_DEBUG__');
  if (globalDebug === true) {
    return true;
  }

  const injectedDebug = readGlobalValue('__LMAO_VITEST_DEBUG_ENV__');
  if (injectedDebug === '1' || injectedDebug === 'true') {
    return true;
  }

  const envDebug = readProcessEnv('LMAO_VITEST_DEBUG');
  return envDebug === '1' || envDebug === 'true';
}

function vitestHarnessDebug(message: string, data?: unknown): void {
  if (!isVitestHarnessDebugEnabled()) {
    return;
  }

  if (data === undefined) {
    console.error(`[lmao/vitest-harness] ${message}`);
    return;
  }

  console.error(`[lmao/vitest-harness] ${message}`, data);
}

type TestBody = () => unknown | Promise<unknown>;
type SpanCtx<B extends OpContextBinding> = SpanContext<OpContextOf<B>>;
type VitestDescribeCallback = () => void;
type VitestDescribeBranch = (name: string, fn: VitestDescribeCallback) => unknown;
type VitestPublicTestCallback = () => void | Promise<void>;
type VitestPublicTestBranch = (name: string, fn: VitestPublicTestCallback) => unknown;
type VitestModuleIt = ((name: string, fn: TestBody) => unknown) & {
  skip: (name: string, fn: VitestPublicTestCallback) => unknown;
  only: (name: string, fn: VitestPublicTestCallback) => unknown;
  todo: (name: string, fn: VitestPublicTestCallback) => unknown;
  each: VitestEach;
  skipIf: (condition: boolean) => (name: string, fn: VitestPublicTestCallback) => unknown;
};
type VitestEach = (...args: unknown[]) => unknown;

export type VitestDescribe = VitestDescribeBranch & {
  skip: VitestDescribeBranch;
  only: VitestDescribeBranch;
  todo: VitestDescribeBranch;
  each: VitestEach;
  skipIf: (condition: boolean) => VitestDescribeBranch;
};

export type VitestIt = VitestPublicTestBranch & {
  skip: VitestPublicTestBranch;
  only: VitestPublicTestBranch;
  todo: VitestPublicTestBranch;
  each: VitestEach;
  skipIf: (condition: boolean) => VitestPublicTestBranch;
};

export interface VitestModuleShape {
  describe: VitestDescribe;
  it: VitestModuleIt;
  test: VitestModuleIt;
  [key: string]: unknown;
}

type SpanContextStore<Ctx> = {
  run<R>(ctx: Ctx, fn: () => R): R;
  getStore(): Ctx | undefined;
};

type AsyncLocalStorageLike<Ctx> = {
  run<R>(store: Ctx, callback: () => R): R;
  getStore(): Ctx | undefined;
};

type AsyncLocalStorageCtor = new <Ctx>() => AsyncLocalStorageLike<Ctx>;

function isAsyncLocalStorageCtor(value: unknown): value is AsyncLocalStorageCtor {
  return typeof value === 'function';
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

type SQLiteRuntimeConfig = SQLiteWriterConfig &
  (
    | {
        /** Node/bun style synchronous SQLite driver (for example better-sqlite3). */
        createDatabase: (path: string) => SyncSQLiteDatabase;
        createAsyncDatabase?: never;
      }
    | {
        /** Async SQLite driver (for example Cloudflare D1 adapter). */
        createAsyncDatabase: (path: string) => AsyncSQLiteDatabase | Promise<AsyncSQLiteDatabase>;
        createDatabase?: never;
      }
  );

type InitTraceTestRunOptions = {
  /**
   * Optional SQLite persistence target.
   *
   * Omit this to run traced tests without DB persistence.
   */
  sqlite?: SQLiteRuntimeConfig;
  /** Optional verbose stdout tracing; defaults to env flag detection. */
  verbose?: boolean;
};

type TracerFactoryOptions<B extends OpContextBinding> = {
  binding: B;
  sqlite: InitTraceTestRunOptions['sqlite'];
  verbose: boolean;
};

function isTruthyEnvFlag(value: unknown): boolean {
  return value === true || value === '1' || value === 'true';
}

function isVerboseTraceEnabled(explicitVerbose: boolean | undefined): boolean {
  if (explicitVerbose !== undefined) {
    return explicitVerbose;
  }

  const globalVerbose = readGlobalValue('__LMAO_TEST_TRACE_VERBOSE__');
  if (isTruthyEnvFlag(globalVerbose)) {
    return true;
  }

  const injectedVerbose = readGlobalValue('__LMAO_TEST_TRACE_VERBOSE_ENV__');
  if (isTruthyEnvFlag(injectedVerbose)) {
    return true;
  }

  return isTruthyEnvFlag(readProcessEnv('LMAO_TEST_TRACE_VERBOSE'));
}

class FallbackSpanContextStore<Ctx> implements SpanContextStore<Ctx> {
  private current: Ctx | undefined;

  run<R>(ctx: Ctx, fn: () => R): R {
    const prev = this.current;
    this.current = ctx;
    try {
      return fn();
    } finally {
      this.current = prev;
    }
  }

  getStore(): Ctx | undefined {
    return this.current;
  }
}

function tryCreateAsyncLocalStorageStore<Ctx>(): SpanContextStore<Ctx> | null {
  const maybeCtor = Reflect.get(globalThis, 'AsyncLocalStorage');
  if (!isAsyncLocalStorageCtor(maybeCtor)) {
    return null;
  }

  const storage = new maybeCtor<Ctx>();
  return {
    run<R>(ctx: Ctx, fn: () => R): R {
      return storage.run(ctx, fn);
    },
    getStore(): Ctx | undefined {
      return storage.getStore();
    },
  };
}

function createDefaultSpanContextStore<Ctx>(): SpanContextStore<Ctx> {
  const store = tryCreateAsyncLocalStorageStore<Ctx>();
  if (store) {
    vitestHarnessDebug('using AsyncLocalStorage span store');
    return store;
  }

  // Workers (e.g. Cloudflare Vitest pool) do not expose node:async_hooks.
  // Fallback is safe for awaited test bodies, but detached async work will not
  // retain the context automatically.
  vitestHarnessDebug('using fallback span store');
  return new FallbackSpanContextStore<Ctx>();
}

function createRootTracer<B extends OpContextBinding>({
  binding,
  sqlite,
  verbose,
}: TracerFactoryOptions<B>): Tracer<B> {
  const tracerOptions = {
    bufferStrategy: new JsBufferStrategy<B['logBinding']['logSchema']>(),
    createTraceRoot,
  } as const;

  let sqliteTracer: Tracer<B> | null = null;
  if (sqlite?.createAsyncDatabase) {
    const dbPath = sqlite.dbPath ?? '.trace-results.db';
    sqliteTracer = new SQLiteAsyncTracer(binding, {
      ...tracerOptions,
      db: sqlite.createAsyncDatabase(dbPath),
    });
  } else if (sqlite?.createDatabase) {
    const db = sqlite.createDatabase(sqlite.dbPath ?? '.trace-results.db');
    sqliteTracer = new SQLiteTracer(binding, {
      ...tracerOptions,
      db,
    });
  }

  if (sqliteTracer) {
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

  return new TestTracer(binding, {
    ...tracerOptions,
  });
}

async function closeTracer(tracer: Tracer<OpContextBinding>): Promise<void> {
  const close = Reflect.get(tracer, 'close');
  if (typeof close === 'function') {
    await close.call(tracer);
  }
}

type VitestHarnessConfig<B extends OpContextBinding> = {
  binding: B;
  createSpanContextStore?: () => SpanContextStore<SpanCtx<B>>;
};

export type VitestTestTracer<B extends OpContextBinding> = {
  initTraceTestRun(options?: InitTraceTestRunOptions): void;
  useTestSpan(): SpanCtx<B>;
  getTracer(): Tracer<B>;
  createVitestMock<T extends VitestModuleShape>(vitestModule: T): T;
  describe(name: string, fn: () => void): unknown;
  it(name: string, fn: () => void | Promise<void>): void;
};

export type VitestTestSuiteTracer<B extends OpContextBinding> = {
  useTestTracer: VitestTestTracer<B>;
  useTestSpan(): SpanCtx<B>;
  setupVitestTestSuiteTracing(): void;
};

type ActiveVitestTestTracer = {
  initTraceTestRun(options?: InitTraceTestRunOptions): void;
  useTestSpan(): unknown;
  getTracer(): unknown;
  createVitestMock<T extends VitestModuleShape>(vitestModule: T): T;
  describe(name: string, fn: () => void): unknown;
  it(name: string, fn: () => void | Promise<void>): void;
};

let _activeSuiteTracer: ActiveVitestTestTracer | null = null;

function createActiveVitestTestTracer<B extends OpContextBinding>(tracer: VitestTestTracer<B>): ActiveVitestTestTracer {
  return {
    initTraceTestRun: (options) => tracer.initTraceTestRun(options),
    useTestSpan: () => tracer.useTestSpan(),
    getTracer: () => tracer.getTracer(),
    createVitestMock: (vitestModule) => tracer.createVitestMock(vitestModule),
    describe: (name, fn) => tracer.describe(name, fn),
    it: (name, fn) => tracer.it(name, fn),
  };
}

export function installVitestTestTracing<B extends OpContextBinding>(
  tracer: VitestTestTracer<B>,
  options?: InitTraceTestRunOptions,
): void {
  _activeSuiteTracer = createActiveVitestTestTracer(tracer);
  tracer.initTraceTestRun(options);
}

export function makeVitestTestSuiteTracer<B extends OpContextBinding>(
  config: VitestHarnessConfig<B>,
  options?: InitTraceTestRunOptions,
): VitestTestSuiteTracer<B> {
  const useTestTracer = makeVitestTestTracer(config);
  return {
    useTestTracer,
    useTestSpan: () => useTestTracer.useTestSpan(),
    setupVitestTestSuiteTracing: () => installVitestTestTracing(useTestTracer, options),
  };
}

export function makeVitestTestTracer<B extends OpContextBinding>(config: VitestHarnessConfig<B>): VitestTestTracer<B> {
  const { binding } = config;
  const spanStore = config.createSpanContextStore?.() ?? createDefaultSpanContextStore<SpanCtx<B>>();
  vitestHarnessDebug('created vitest test tracer instance');

  let initialized = false;
  let tracer: Tracer<B> | null = null;
  let verboseTrace = false;
  let rootCtx: SpanCtx<B> | null = null;
  let resolveTestRun: (() => void) | null = null;
  let rootTracePromise: Promise<unknown> | null = null;
  const standaloneDescribeStack: string[] = [];

  function getRootCtx(): SpanCtx<B> {
    if (!rootCtx) {
      throw new Error('Call initTraceTestRun() in setupFiles before tests');
    }
    return rootCtx;
  }

  function createExtendedBinding(): B {
    const extendedSchema = binding.logBinding.logSchema.extend({ describe: S.category() });
    const extendedBinding: B = {
      ...binding,
      logBinding: { ...binding.logBinding, logSchema: extendedSchema },
    };
    return extendedBinding;
  }

  function initTraceTestRun(options?: InitTraceTestRunOptions): void {
    if (initialized) {
      throw new Error('initTraceTestRun() already called for this vitest tracer instance');
    }
    initialized = true;
    verboseTrace = isVerboseTraceEnabled(options?.verbose);
    vitestHarnessDebug('initTraceTestRun start', {
      sqliteConfigured:
        options?.sqlite?.createDatabase !== undefined || options?.sqlite?.createAsyncDatabase !== undefined,
      verboseTrace,
    });

    // Extend user's schema with `describe` column for test grouping.
    const extendedBinding = createExtendedBinding();

    tracer = createRootTracer({
      binding: extendedBinding,
      sqlite: options?.sqlite,
      verbose: verboseTrace,
    });
    vitestHarnessDebug('created root tracer for vitest harness');

    const activeTracer = tracer;
    rootTracePromise = activeTracer.trace('test-run', (ctx: SpanCtx<B>) => {
      rootCtx = ctx;
      vitestHarnessDebug('root test-run span started', { trace_id: ctx.buffer.trace_id });
      return new Promise<void>((resolve) => {
        resolveTestRun = resolve;
      });
    });
    vitestHarnessDebug('root trace promise created');

    _afterAll(async () => {
      vitestHarnessDebug('afterAll hook start');
      if (resolveTestRun) {
        resolveTestRun();
        resolveTestRun = null;
        vitestHarnessDebug('resolved root test-run promise');
      }

      if (rootTracePromise) {
        await rootTracePromise;
        rootTracePromise = null;
        vitestHarnessDebug('awaited root trace promise');
      }

      if (tracer && rootCtx) {
        try {
          vitestHarnessDebug('flushing tracer');
          await tracer.flush();
          if (options?.sqlite) {
            const traceId = rootCtx.buffer.trace_id;
            const dbPath = options.sqlite.dbPath ?? '.trace-results.db';
            console.log(`\n[trace] trace_id: ${traceId} → ${dbPath}`);
          }
        } catch (error) {
          console.error('[lmao/testing] SQLite flush error:', error);
        } finally {
          await closeTracer(tracer);
        }
      }
      vitestHarnessDebug('afterAll hook complete');
    });
  }

  function useTestSpan(): SpanCtx<B> {
    const ctx = spanStore.getStore();
    if (!ctx) {
      throw new Error('useTestSpan() called outside of a traced it()');
    }
    return ctx;
  }

  function getTracer(): Tracer<B> {
    if (!tracer) {
      throw new Error('Call initTraceTestRun() in setupFiles before tests');
    }
    return tracer;
  }

  function createVitestMock<T extends VitestModuleShape>(vitestModule: T): T {
    vitestHarnessDebug('createVitestMock called');
    const currentRootCtx = getRootCtx();
    const origIt = vitestModule.it;
    const origDescribe = vitestModule.describe;
    const describeStack: string[] = [];

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

    Object.assign(wrappedDescribe, {
      skip: origDescribe.skip,
      only: origDescribe.only,
      todo: origDescribe.todo,
      each: origDescribe.each,
      skipIf: origDescribe.skipIf.bind(origDescribe),
    });

    function wrappedIt(name: string, fn: TestBody) {
      const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
      return origIt(name, () =>
        currentRootCtx.span(name, async (ctx) => {
          writeDescribeTag(ctx.tag, describePath);
          try {
            await spanStore.run(ctx, fn);
            return ctx.ok(undefined);
          } catch (error) {
            if (isExpectError(error)) {
              return ctx.err(error);
            }
            throw error;
          }
        }),
      );
    }

    Object.assign(wrappedIt, {
      skip: origIt.skip,
      only: origIt.only,
      todo: origIt.todo,
      each: origIt.each,
      skipIf: origIt.skipIf.bind(origIt),
    });

    return {
      ...vitestModule,
      describe: wrappedDescribe,
      it: wrappedIt,
      test: wrappedIt,
    };
  }

  function describe(name: string, fn: () => void) {
    return _describe(name, () => {
      standaloneDescribeStack.push(name);
      try {
        fn();
      } finally {
        standaloneDescribeStack.pop();
      }
    });
  }

  function it(name: string, fn: () => void | Promise<void>): void {
    const describePath = standaloneDescribeStack.length > 0 ? standaloneDescribeStack.join(' > ') : null;
    _it(name, () => {
      const currentRootCtx = getRootCtx();
      return currentRootCtx.span(name, async (ctx) => {
        writeDescribeTag(ctx.tag, describePath);
        try {
          await spanStore.run(ctx, fn);
          return ctx.ok(undefined);
        } catch (error) {
          if (isExpectError(error)) {
            return ctx.err(error);
          }
          throw error;
        }
      });
    });
  }

  return {
    initTraceTestRun,
    useTestSpan,
    getTracer,
    createVitestMock,
    describe,
    it,
  };
}

// Global singleton path for shared Vitest helpers.
let _defaultHarness: VitestTestTracer<OpContextBinding> | null = null;

/** Initialize the root tracer for the entire vitest run. Call once in setupFiles. */
export function initTraceTestRun<B extends OpContextBinding>(opContext: B, options?: InitTraceTestRunOptions): void {
  _activeSuiteTracer = null;
  const harness = makeVitestTestTracer({ binding: opContext });
  harness.initTraceTestRun(options);
  _defaultHarness = harness;
}

/** Get the current span context from AsyncLocalStorage (inside an it() block) */
export function useTestSpan(): SpanContext<OpContextOf<OpContextBinding>> {
  if (_activeSuiteTracer) {
    const ctx = _activeSuiteTracer.useTestSpan();
    if (!isSpanContext(ctx)) {
      throw new Error('Active suite tracer returned a non-span context');
    }
    return ctx;
  }

  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() in setupFiles before tests');
  }
  return _defaultHarness.useTestSpan();
}

/** Get the root tracer instance */
export function getTracer(): Tracer<OpContextBinding> {
  if (_activeSuiteTracer) {
    const tracer = _activeSuiteTracer.getTracer();
    if (!(tracer instanceof Tracer)) {
      throw new Error('Active suite tracer returned a non-tracer');
    }
    return tracer;
  }

  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() in setupFiles before tests');
  }
  return _defaultHarness.getTracer();
}

/**
 * Create a vitest mock replacement that wraps it()/test()/describe() with trace spans.
 *
 * Call this from a vitest setupFile via vi.mock:
 * ```typescript
 * vi.mock('vitest', async (importOriginal) => {
 *   const [mod, { createVitestMock }] = await Promise.all([
 *     importOriginal(),
 *     import('@smoothbricks/lmao/testing/vitest'),
 *   ]);
 *   return createVitestMock(mod as Record<string, unknown>);
 * });
 * ```
 *
 * @param vitestModule - The original vitest namespace from importOriginal()
 */
export function createVitestMock<T extends VitestModuleShape>(vitestModule: T): T {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.createVitestMock(vitestModule);
  }

  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() before createVitestMock()');
  }
  return _defaultHarness.createVitestMock(vitestModule);
}

/**
 * Wrapped describe — tracks describe nesting for the standalone export path.
 * describe() callbacks run synchronously (just registering tests).
 */
const describeBase: VitestDescribeBranch = (name, fn) => {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.describe(name, fn);
  }

  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() in setupFiles before tests');
  }
  return _defaultHarness.describe(name, fn);
};

export const describe: VitestDescribe = Object.assign(describeBase, {
  skip: (name: string, fn: VitestDescribeCallback) => _describe.skip(name, fn),
  only: (name: string, fn: VitestDescribeCallback) => _describe.only(name, fn),
  todo: (name: string, fn: VitestDescribeCallback) => _describe.todo(name, fn),
  each: (...args: unknown[]) => Reflect.apply(_describe.each, _describe, args),
  skipIf: (condition: boolean) => (condition ? _describe.skip : describe),
});

/** Wrapped it — creates a child span of the root trace for the test case */
const itBase: VitestPublicTestBranch = (name, fn) => {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.it(name, fn);
  }

  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() in setupFiles before tests');
  }
  return _defaultHarness.it(name, fn);
};

export const it: VitestIt = Object.assign(itBase, {
  skip: (name: string, fn: TestBody) => _it.skip(name, fn),
  only: (name: string, fn: TestBody) => _it.only(name, fn),
  todo: (name: string, fn: TestBody) => _it.todo(name, fn),
  each: (...args: unknown[]) => Reflect.apply(_it.each, _it, args),
  skipIf: (condition: boolean) => (condition ? _it.skip : it),
});

// Re-export everything else unchanged
export {
  _afterAll as afterAll,
  _afterEach as afterEach,
  _beforeAll as beforeAll,
  _beforeEach as beforeEach,
  _expect as expect,
};
