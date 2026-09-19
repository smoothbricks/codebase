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
 * import { DEFAULT_TRACE_DB_PATH, initTraceTestRun } from '@smoothbricks/lmao/testing/vitest';
 * initTraceTestRun(myOpContext, { sqlite: { dbPath: DEFAULT_TRACE_DB_PATH } });
 *
 * // my-test.test.ts
 * import { describe, it, expect, useTestSpan } from '@smoothbricks/lmao/testing/vitest';
 * ```
 *
 * @module testing/vitest
 */

import { AsyncLocalStorage } from 'node:async_hooks';
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
import { DEFAULT_TRACE_DB_PATH } from '../sqlite/trace-db-path.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { Tracer } from '../tracer.js';
import { CompositeTracer } from '../tracers/CompositeTracer.js';
import { SQLiteAsyncTracer, SQLiteTracer } from '../tracers/SQLiteTracer.js';
import { StdioTracer } from '../tracers/StdioTracer.js';
import { TestTracer } from '../tracers/TestTracer.js';

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

type SpanCtx<B extends OpContextBinding> = SpanContext<OpContextOf<B>>;
export type VitestDescribe = typeof _describe;
export type VitestIt = typeof _it;
// Injection requires only registration functions. T carries the framework's
// concrete overloads through every proxy; never[] permits no untyped calls.
export type VitestModuleShape = Record<'describe' | 'it' | 'test', (...args: never[]) => unknown>;

/** Decorate registration without restating Vitest's overloads or rebuilding its chain context. */
function wrapVitestRegistration<F extends object>(
  original: F,
  wrapBody: (name: string) => (body: () => unknown) => unknown,
  wrapSuite: typeof wrapBody = wrapBody,
): F {
  return new Proxy(original, {
    apply(target, receiver: unknown, args: unknown[]) {
      if (typeof target !== 'function') throw new Error('Vitest registration is not callable');
      const index = typeof args[1] === 'function' ? 1 : typeof args[2] === 'function' ? 2 : -1;
      const callback = args[index];
      if (typeof callback === 'function') {
        // Capture describe ancestry at registration, but pass the framework's row/context arguments at execution.
        const name = args[0];
        const run = wrapBody(typeof name === 'function' ? name.name : String(name));
        args[index] = new Proxy(callback, {
          apply(body, thisArg: unknown, params: unknown[]) {
            // Each/concurrent invocations need their own closure, not a shared argument slot.
            return run(() => Reflect.apply(body, thisArg, params));
          },
          get(body, key) {
            // Vitest discovers fixture dependencies by parsing callback.toString().
            return key === 'toString' ? body.toString.bind(body) : Reflect.get(body, key);
          },
        });
      }
      return Reflect.apply(target, receiver, args);
    },
    get(target, key) {
      const member: unknown = Reflect.get(target, key);
      if (typeof member !== 'function') return member;
      switch (key) {
        case 'each':
        case 'for':
        case 'skipIf':
        case 'runIf':
        case 'extend':
          return new Proxy(member, {
            apply(factory, _receiver: unknown, args: unknown[]) {
              const registration: unknown = Reflect.apply(factory, target, args);
              return typeof registration === 'function'
                ? wrapVitestRegistration(registration, wrapBody, wrapSuite)
                : registration;
            },
          });
        case 'skip':
        case 'only':
        case 'todo':
        case 'concurrent':
        case 'sequential':
        case 'shuffle':
        case 'fails':
          return wrapVitestRegistration(member, wrapBody, wrapSuite);
        case 'describe':
        case 'suite':
          return wrapVitestRegistration(member, wrapSuite);
        default:
          return member;
      }
    },
  });
}

type SpanContextStore<Ctx> = {
  run<R>(ctx: Ctx, fn: () => R): R;
  getStore(): Ctx | undefined;
};

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
    const dbPath = sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH;
    sqliteTracer = new SQLiteAsyncTracer(binding, {
      ...tracerOptions,
      db: sqlite.createAsyncDatabase(dbPath),
    });
  } else if (sqlite?.createDatabase) {
    const db = sqlite.createDatabase(sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH);
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

async function closeTracer<B extends OpContextBinding>(tracer: Tracer<B>): Promise<void> {
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
  describe: VitestDescribe;
  it: VitestIt;
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
  describe: VitestDescribe;
  it: VitestIt;
};

let _activeSuiteTracer: ActiveVitestTestTracer | null = null;

function createActiveVitestTestTracer<B extends OpContextBinding>(tracer: VitestTestTracer<B>): ActiveVitestTestTracer {
  return {
    initTraceTestRun: (options) => tracer.initTraceTestRun(options),
    useTestSpan: () => tracer.useTestSpan(),
    getTracer: () => tracer.getTracer(),
    createVitestMock: (vitestModule) => tracer.createVitestMock(vitestModule),
    describe: tracer.describe,
    it: tracer.it,
  };
}

export function installVitestTestTracing<B extends OpContextBinding>(
  tracer: VitestTestTracer<B>,
  options?: InitTraceTestRunOptions,
): void {
  tracer.initTraceTestRun(options);
  _activeSuiteTracer = createActiveVitestTestTracer(tracer);
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
  const spanStore = config.createSpanContextStore?.() ?? new AsyncLocalStorage<SpanCtx<B>>();
  vitestHarnessDebug('created vitest test tracer instance');

  let initialized = false;
  let tracer: Tracer<B> | null = null;
  let verboseTrace = false;
  let rootCtx: SpanCtx<B> | null = null;
  let resolveTestRun: (() => void) | null = null;
  let rootTracePromise: Promise<unknown> | null = null;
  let describePath: string | null = null;

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
            const dbPath = options.sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH;
            console.log(`\n[trace] trace_id: ${traceId} → ${dbPath}`);
          }
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

  function wrapDescribe(name: string): (body: () => unknown) => Promise<unknown> {
    const parent = describePath;
    return async (body) => {
      const previous = describePath;
      describePath = parent ? `${parent} > ${name}` : name;
      try {
        return await body();
      } finally {
        describePath = previous;
      }
    };
  }

  function wrapTest(name: string): (body: () => unknown) => unknown {
    const path = describePath;
    return (body) =>
      getRootCtx().span(name, async (ctx) => {
        writeDescribeTag(ctx.tag, path);
        await spanStore.run(ctx, body);
        return ctx.ok(undefined);
      });
  }

  function createVitestMock<T extends VitestModuleShape>(vitestModule: T): T {
    return {
      ...vitestModule,
      describe: wrapVitestRegistration(vitestModule.describe, wrapDescribe),
      it: wrapVitestRegistration(vitestModule.it, wrapTest, wrapDescribe),
      test: wrapVitestRegistration(vitestModule.test, wrapTest, wrapDescribe),
    };
  }

  const describe = wrapVitestRegistration(_describe, wrapDescribe);
  const it = wrapVitestRegistration(_it, wrapTest, wrapDescribe);

  return {
    initTraceTestRun,
    useTestSpan,
    getTracer,
    createVitestMock,
    describe,
    it,
  };
}

function requireActiveSuiteTracer(message: string): ActiveVitestTestTracer {
  if (!_activeSuiteTracer) {
    throw new Error(message);
  }

  return _activeSuiteTracer;
}

/** Initialize the root tracer for the entire vitest run. Call once in setupFiles. */
export function initTraceTestRun<B extends OpContextBinding>(opContext: B, options?: InitTraceTestRunOptions): void {
  installVitestTestTracing(makeVitestTestTracer({ binding: opContext }), options);
}

/** Get the current span context from AsyncLocalStorage (inside an it() block) */
export function useTestSpan(): SpanContext<OpContextOf<OpContextBinding>> {
  const ctx = requireActiveSuiteTracer('Call initTraceTestRun() in setupFiles before tests').useTestSpan();
  if (!isSpanContext(ctx)) {
    throw new Error('Active suite tracer returned a non-span context');
  }
  return ctx;
}

/** Get the root tracer instance */
export function getTracer(): Tracer<OpContextBinding> {
  const tracer = requireActiveSuiteTracer('Call initTraceTestRun() in setupFiles before tests').getTracer();
  if (!(tracer instanceof Tracer)) {
    throw new Error('Active suite tracer returned a non-tracer');
  }
  return tracer;
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
 *   return createVitestMock(mod);
 * });
 * ```
 *
 * @param vitestModule - The original vitest namespace from importOriginal()
 */
export function createVitestMock<T extends VitestModuleShape>(vitestModule: T): T {
  return requireActiveSuiteTracer('Call initTraceTestRun() before createVitestMock()').createVitestMock(vitestModule);
}

/** Resolve the active tracer lazily, including Vitest's chainable APIs. */
export const describe: VitestDescribe = new Proxy(_describe, {
  apply(_target, receiver: unknown, args: unknown[]) {
    return Reflect.apply(
      requireActiveSuiteTracer('Call initTraceTestRun() in setupFiles before tests').describe,
      receiver,
      args,
    );
  },
  get(_target, key) {
    return Reflect.get(requireActiveSuiteTracer('Call initTraceTestRun() in setupFiles before tests').describe, key);
  },
});

/** Wrapped it — creates a child span of the root trace for the test case */
export const it: VitestIt = new Proxy(_it, {
  apply(_target, receiver: unknown, args: unknown[]) {
    return Reflect.apply(
      requireActiveSuiteTracer('Call initTraceTestRun() in setupFiles before tests').it,
      receiver,
      args,
    );
  },
  get(_target, key) {
    return Reflect.get(requireActiveSuiteTracer('Call initTraceTestRun() in setupFiles before tests').it, key);
  },
});

// Setup files wire `sqlite.dbPath` from here rather than spelling the sink path, which is only safe under a directory
// project walkers and watchers ignore.
export { DEFAULT_TRACE_DB_PATH, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME } from '../sqlite/trace-db-path.js';
// Re-export everything else unchanged
export {
  _afterAll as afterAll,
  _afterEach as afterEach,
  _beforeAll as beforeAll,
  _beforeEach as beforeEach,
  _expect as expect,
};
