/**
 * bun:test integration for LMAO trace-testing.
 *
 * Provides wrapped describe/it/expect that create traced spans for each test.
 * One root TestTracer is shared across the entire `bun test` run — initialized
 * via `initTraceTestRun()` in a preload file.
 *
 * Architecture:
 * - Each it() creates a root trace (tracer.trace(name, fn))
 * - Operations within tests create child spans via ctx.span()
 * - describe() is pass-through (grouping only, no span)
 * - After all tests: flush span tree to SQLite (if configured)
 *
 * @example
 * ```typescript
 * // test-setup.ts (preload)
 * import { initTraceTestRun } from '@smoothbricks/lmao/testing/bun';
 * initTraceTestRun(myOpContext, { sqlite: { dbPath: '.trace-results.db' } });
 *
 * // my-test.test.ts
 * import { describe, it, expect, useTestSpan, getTracer } from '@smoothbricks/lmao/testing/bun';
 *
 * describe('Order Processing', () => {
 *   it('validates order', () => {
 *     const ctx = useTestSpan();
 *     // ctx.span(), ctx.tag, ctx.log available
 *   });
 * });
 * ```
 *
 * @module testing/bun
 */

import { Database } from 'bun:sqlite';
import {
  afterAll as _afterAll,
  afterEach as _afterEach,
  beforeAll as _beforeAll,
  beforeEach as _beforeEach,
  describe as _describe,
  expect as _expect,
  it as _it,
} from 'bun:test';
import { AsyncLocalStorage } from 'node:async_hooks';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { SpanContext } from '../opContext/spanContextTypes.js';
import type { OpContext, OpContextBinding } from '../opContext/types.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { type TraceSQLiteConfig, TraceSQLiteSink } from './sqlite-sink.js';

// Global singleton — one root tracer for the entire bun test run
let _tracer: TestTracer<OpContextBinding> | null = null;
let _sink: TraceSQLiteSink | null = null;

// AsyncLocalStorage for propagating SpanContext to test bodies.
// Each it() runs in its own async context so concurrent tests don't collide.
const _als = new AsyncLocalStorage<SpanContext<OpContext>>();

/** Initialize the root tracer for the entire bun test run. Call once in preload. */
export function initTraceTestRun<B extends OpContextBinding>(
  opContext: B,
  options?: {
    sqlite?: TraceSQLiteConfig;
  },
): void {
  _tracer = new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  }) as TestTracer<OpContextBinding>;

  if (options?.sqlite) {
    const db = new Database(options.sqlite.dbPath ?? '.trace-results.db');
    _sink = new TraceSQLiteSink(db, options.sqlite);
  }

  // Register global teardown — flush all spans to SQLite after all tests complete
  _afterAll(() => {
    if (_sink && _tracer) {
      _sink.flushAll(_tracer);
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
  if (!_tracer) throw new Error('Call initTraceTestRun() in preload before tests');
  return _tracer;
}

/**
 * Wrapped describe — pass-through to bun:test describe.
 * Grouping only, no trace span (spans are created per-test in it()).
 */
export const describe: typeof _describe = _describe;

/** Wrapped it — creates a root trace span for the test case */
export function it(name: string, fn: () => void | Promise<void>): void {
  _it(name, () => {
    if (!_tracer) throw new Error('Call initTraceTestRun() in preload before tests');
    // tracer.trace() creates a root span and calls fn(ctx)
    // For non-Result returns, it writes SPAN_OK automatically
    return _tracer.trace(name, (ctx: SpanContext<OpContext>) => _als.run(ctx, fn));
  });
}

// Attach static methods from bun:test
it.skip = _it.skip;
it.only = _it.only;
it.todo = _it.todo;
it.each = _it.each;
it.skipIf = _it.skipIf;
it.if = _it.if;

// Re-export everything else unchanged
export {
  _afterAll as afterAll,
  _afterEach as afterEach,
  _beforeAll as beforeAll,
  _beforeEach as beforeEach,
  _expect as expect,
};
