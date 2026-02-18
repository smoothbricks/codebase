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
import type { OpContext, OpContextBinding } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import type { SyncSQLiteDatabase } from '../sqlite/sqlite-db.js';
import { type TraceSQLiteConfig, TraceSQLiteSink } from '../sqlite/sqlite-sink.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { TestTracer } from '../tracers/TestTracer.js';

/** vitest expect() errors start with 'expect(received).' — distinguishes assertion failures from other throws */
function isExpectError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith('expect(');
}

// Global singleton — one root tracer for the entire vitest run
let _tracer: TestTracer<OpContextBinding> | null = null;
let _sink: TraceSQLiteSink | null = null;

// Root span context, its resolver, and the trace promise — one root per test run
let _rootCtx: SpanContext<OpContext> | null = null;
let _resolveTestRun: (() => void) | null = null;
let _rootTracePromise: Promise<unknown> | null = null;

// AsyncLocalStorage for propagating SpanContext to test bodies
const _als = new AsyncLocalStorage<SpanContext<OpContext>>();

/** Initialize the root tracer for the entire vitest run. Call once in setupFiles. */
export function initTraceTestRun<B extends OpContextBinding>(
  opContext: B,
  options?: {
    sqlite?: TraceSQLiteConfig & {
      /** better-sqlite3 Database constructor. Pass `import('better-sqlite3').default` */
      createDatabase?: (path: string) => SyncSQLiteDatabase;
    };
  },
): void {
  // Extend user's schema with `describe` column for test grouping
  const extendedSchema = opContext.logBinding.logSchema.extend({ describe: S.category() });
  const extendedBinding = {
    ...opContext,
    logBinding: { ...opContext.logBinding, logSchema: extendedSchema },
  };

  _tracer = new TestTracer(extendedBinding, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  }) as TestTracer<OpContextBinding>;

  if (options?.sqlite?.createDatabase) {
    const db = options.sqlite.createDatabase(options.sqlite.dbPath ?? '.trace-results.db');
    _sink = new TraceSQLiteSink(db);
  }

  // Create the single root trace — a long-lived promise keeps it alive
  const tracer = _tracer;
  _rootTracePromise = tracer.trace('test-run', (ctx: SpanContext<OpContext>) => {
    _rootCtx = ctx;
    return new Promise<void>((resolve) => {
      _resolveTestRun = resolve;
    });
  }) as Promise<unknown>;

  // Register global teardown — resolve root, wait for span-end, then flush to SQLite
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
    if (_sink && _rootCtx) {
      try {
        _sink.flush(_rootCtx.buffer);
        const traceId = _rootCtx.buffer.trace_id;
        const dbPath = options?.sqlite?.dbPath ?? '.trace-results.db';
        console.log(`\n[trace] trace_id: ${traceId} → ${dbPath}`);
      } catch (e) {
        console.error('[lmao/testing] SQLite flush error:', e);
      }
      _sink.close();
      _sink = null;
    }
  });
}

/** Get the current span context from AsyncLocalStorage (inside an it() block) */
export function useTestSpan(): SpanContext<OpContext> {
  const ctx = _als.getStore();
  if (!ctx) throw new Error('useTestSpan() called outside of a traced it()');
  return ctx;
}

/** Get the root tracer instance */
export function getTracer(): TestTracer<OpContextBinding> {
  if (!_tracer) throw new Error('Call initTraceTestRun() in setupFiles before tests');
  return _tracer;
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
  if (!_rootCtx) throw new Error('Call initTraceTestRun() before createVitestMock()');

  const origIt = vitestModule.it as typeof _it;
  const origDescribe = vitestModule.describe as typeof _describe;
  const rootCtx = _rootCtx;
  const als = _als;
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

  function wrappedIt(name: string, fn: any) {
    const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
    return origIt(name, () =>
      (rootCtx.span as Function)(name, async (ctx: SpanContext<OpContext>) => {
        if (describePath) (ctx.tag as any).describe(describePath);
        try {
          await als.run(ctx, fn);
          return ctx.ok(undefined); // pass → span-ok
        } catch (error) {
          if (isExpectError(error)) return ctx.err(error); // expect() fail → span-err
          throw error; // other throw → span-exception
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

// Describe stack for standalone exports (non-vi.mock path)
const _standaloneDescribeStack: string[] = [];

describe.skip = _describe.skip;
describe.only = _describe.only;
describe.todo = _describe.todo;
describe.each = _describe.each;
describe.skipIf = _describe.skipIf.bind(_describe);

/** Wrapped it — creates a child span of the root trace for the test case */
export function it(name: string, fn: () => void | Promise<void>): void {
  const describePath = _standaloneDescribeStack.length > 0 ? _standaloneDescribeStack.join(' > ') : null;
  _it(name, () => {
    if (!_rootCtx) throw new Error('Call initTraceTestRun() in setupFiles before tests');
    return (_rootCtx.span as Function)(name, async (ctx: SpanContext<OpContext>) => {
      if (describePath) (ctx.tag as any).describe(describePath);
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

// Re-export everything else unchanged
export {
  _afterAll as afterAll,
  _afterEach as afterEach,
  _beforeAll as beforeAll,
  _beforeEach as beforeEach,
  _expect as expect,
};
