/// <reference types="node" />

/**
 * vitest integration for LMAO trace-testing.
 *
 * Same architecture as bun-harness: one root trace per test run,
 * each it() creates a child span, describe path written to `describe` schema column.
 *
 * Uses better-sqlite3 for SQLite persistence (vitest runs on Node.js).
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
import type { SyncSQLiteDatabase } from '../sqlite/sqlite-db.js';
import { type TraceSQLiteConfig, TraceSQLiteSink } from '../sqlite/sqlite-sink.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { TestTracer } from '../tracers/TestTracer.js';

/** vitest expect() errors start with 'expect(received).' — distinguishes assertion failures from other throws */
function isExpectError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith('expect(');
}

type TestBody = () => unknown | Promise<unknown>;
type SpanCtx<B extends OpContextBinding> = SpanContext<OpContextOf<B>>;

type SpanContextStore<Ctx> = {
  run<R>(ctx: Ctx, fn: () => R): R;
  getStore(): Ctx | undefined;
};

type AsyncLocalStorageLike<Ctx> = {
  run<R>(store: Ctx, callback: () => R): R;
  getStore(): Ctx | undefined;
};

type AsyncLocalStorageCtor = new <Ctx>() => AsyncLocalStorageLike<Ctx>;

type InitTraceTestRunOptions = {
  sqlite?: TraceSQLiteConfig & {
    /** better-sqlite3 Database constructor. Pass `import('better-sqlite3').default` */
    createDatabase?: (path: string) => SyncSQLiteDatabase;
  };
};

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
  const maybeCtor = (globalThis as { AsyncLocalStorage?: unknown }).AsyncLocalStorage;
  if (typeof maybeCtor !== 'function') {
    return null;
  }

  const ctor = maybeCtor as AsyncLocalStorageCtor;
  const storage = new ctor<Ctx>();
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
    return store;
  }

  // Workers (e.g. Cloudflare Vitest pool) do not expose node:async_hooks.
  // Fallback is safe for awaited test bodies, but detached async work will not
  // retain the context automatically.
  return new FallbackSpanContextStore<Ctx>();
}

type VitestHarnessConfig<B extends OpContextBinding> = {
  binding: B;
  createSpanContextStore?: () => SpanContextStore<SpanCtx<B>>;
};

export type VitestTestTracer<B extends OpContextBinding> = {
  initTraceTestRun(options?: InitTraceTestRunOptions): void;
  useTestSpan(): SpanCtx<B>;
  getTracer(): TestTracer<B>;
  createVitestMock(vitestModule: Record<string, unknown>): Record<string, unknown>;
  describe(name: string, fn: () => void): unknown;
  it(name: string, fn: () => void | Promise<void>): void;
};

export type VitestTestSuiteTracer<B extends OpContextBinding> = {
  useTestTracer: VitestTestTracer<B>;
  useTestSpan(): SpanCtx<B>;
  setupVitestTestSuiteTracing(): void;
};

export function installVitestTestTracing<B extends OpContextBinding>(tracer: VitestTestTracer<B>): void {
  tracer.initTraceTestRun();
}

export function makeVitestTestSuiteTracer<B extends OpContextBinding>(
  config: VitestHarnessConfig<B>,
): VitestTestSuiteTracer<B> {
  const useTestTracer = makeVitestTestTracer(config);
  return {
    useTestTracer,
    useTestSpan: () => useTestTracer.useTestSpan(),
    setupVitestTestSuiteTracing: () => installVitestTestTracing(useTestTracer),
  };
}

function tagDescribePath(ctx: { tag: unknown }, describePath: string | null): void {
  if (!describePath) {
    return;
  }

  const maybeDescribe = (ctx.tag as { describe?: (path: string) => unknown }).describe;
  if (typeof maybeDescribe === 'function') {
    maybeDescribe(describePath);
  }
}

export function makeVitestTestTracer<B extends OpContextBinding>(config: VitestHarnessConfig<B>): VitestTestTracer<B> {
  const { binding } = config;
  const spanStore = config.createSpanContextStore?.() ?? createDefaultSpanContextStore<SpanCtx<B>>();

  let initialized = false;
  let tracer: TestTracer<B> | null = null;
  let sink: TraceSQLiteSink | null = null;
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

  function initTraceTestRun(options?: InitTraceTestRunOptions): void {
    if (initialized) {
      throw new Error('initTraceTestRun() already called for this vitest tracer instance');
    }
    initialized = true;

    // Extend user's schema with `describe` column for test grouping.
    const extendedSchema = binding.logBinding.logSchema.extend({ describe: S.category() });
    const extendedBinding = {
      ...binding,
      logBinding: { ...binding.logBinding, logSchema: extendedSchema },
    } as B;

    tracer = new TestTracer(extendedBinding, {
      bufferStrategy: new JsBufferStrategy(),
      createTraceRoot,
    });

    if (options?.sqlite?.createDatabase) {
      const db = options.sqlite.createDatabase(options.sqlite.dbPath ?? '.trace-results.db');
      sink = new TraceSQLiteSink(db);
    }

    const activeTracer = tracer;
    rootTracePromise = activeTracer.trace('test-run', (ctx: SpanCtx<B>) => {
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

      if (sink && rootCtx) {
        try {
          sink.flush(rootCtx.buffer);
          const traceId = rootCtx.buffer.trace_id;
          const dbPath = options?.sqlite?.dbPath ?? '.trace-results.db';
          console.log(`\n[trace] trace_id: ${traceId} → ${dbPath}`);
        } catch (error) {
          console.error('[lmao/testing] SQLite flush error:', error);
        }
        sink.close();
        sink = null;
      }
    });
  }

  function useTestSpan(): SpanCtx<B> {
    const ctx = spanStore.getStore();
    if (!ctx) {
      throw new Error('useTestSpan() called outside of a traced it()');
    }
    return ctx;
  }

  function getTracer(): TestTracer<B> {
    if (!tracer) {
      throw new Error('Call initTraceTestRun() in setupFiles before tests');
    }
    return tracer;
  }

  function createVitestMock(vitestModule: Record<string, unknown>): Record<string, unknown> {
    const currentRootCtx = getRootCtx();
    const origIt = vitestModule.it as typeof _it;
    const origDescribe = vitestModule.describe as typeof _describe;
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
          tagDescribePath(ctx, describePath);
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

    return { ...vitestModule, describe: wrappedDescribe, it: wrappedIt, test: wrappedIt };
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
        tagDescribePath(ctx, describePath);
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

// Global singleton compatibility path (legacy API)
let _defaultHarness: VitestTestTracer<OpContextBinding> | null = null;

/** Initialize the root tracer for the entire vitest run. Call once in setupFiles. */
export function initTraceTestRun<B extends OpContextBinding>(opContext: B, options?: InitTraceTestRunOptions): void {
  const harness = makeVitestTestTracer({ binding: opContext });
  harness.initTraceTestRun(options);
  _defaultHarness = harness;
}

/** Get the current span context from AsyncLocalStorage (inside an it() block) */
export function useTestSpan(): SpanContext<OpContextOf<OpContextBinding>> {
  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() in setupFiles before tests');
  }
  return _defaultHarness.useTestSpan();
}

/** Get the root tracer instance */
export function getTracer(): TestTracer<OpContextBinding> {
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
export function createVitestMock(vitestModule: Record<string, unknown>): Record<string, unknown> {
  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() before createVitestMock()');
  }
  return _defaultHarness.createVitestMock(vitestModule);
}

/**
 * Wrapped describe — tracks describe nesting for the standalone export path.
 * describe() callbacks run synchronously (just registering tests).
 */
export function describe(name: string, fn: () => void) {
  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() in setupFiles before tests');
  }
  return _defaultHarness.describe(name, fn);
}

describe.skip = _describe.skip;
describe.only = _describe.only;
describe.todo = _describe.todo;
describe.each = _describe.each;
describe.skipIf = _describe.skipIf.bind(_describe);

/** Wrapped it — creates a child span of the root trace for the test case */
export function it(name: string, fn: () => void | Promise<void>): void {
  if (!_defaultHarness) {
    throw new Error('Call initTraceTestRun() in setupFiles before tests');
  }
  _defaultHarness.it(name, fn);
}

it.skip = _it.skip;
it.only = _it.only;
it.todo = _it.todo;
it.each = _it.each;
it.skipIf = _it.skipIf.bind(_it);

// Re-export everything else unchanged
export {
  _afterAll as afterAll,
  _afterEach as afterEach,
  _beforeAll as beforeAll,
  _beforeEach as beforeEach,
  _expect as expect,
};
