/**
 * bun:test integration for LMAO trace-testing.
 *
 * Transparently wraps bun:test's it()/test() via mock.module so every test
 * automatically creates a trace span — no import changes needed in test files.
 *
 * Architecture:
 * - initTraceTestRun() in preload sets up tracer + SQLite sink
 * - mock.module('bun:test', ...) in preload patches it()/test() with trace spans
 * - Each it()/test() creates a root trace (tracer.trace(name, fn))
 * - Operations within tests create child spans via ctx.span()
 * - describe() is wrapped to track nesting — the describe path is stored on root spans
 * - After all tests: flush span tree to SQLite (if configured)
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
 *     // This it() is automatically wrapped in a trace span
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

// Maps root SpanBuffers to their describe path at registration time.
// WeakMap so buffers can be GC'd if the test runner drops them.
const _describePathMap = new WeakMap<object, string>();

// Describe stack for standalone exports (non-mock-module path)
const _standaloneDescribeStack: string[] = [];

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
      try {
        _sink.flushAll(_tracer, _describePathMap);
        const dbPath = options?.sqlite?.dbPath ?? '.trace-results.db';
        console.log(`\n[trace] run_id: ${_sink.runId} → ${dbPath}`);
      } catch (e) {
        console.error('[lmao/testing] SQLite flush error:', e);
      }
      _sink.close();
      _sink = null;
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
  if (!_tracer) throw new Error('Call initTraceTestRun() before createBunTestMock()');

  const origIt = bunTestModule.it as typeof _it;
  const origDescribe = bunTestModule.describe as typeof _describe;
  const tracer = _tracer;
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
  Object.assign(wrappedDescribe, {
    skip: origDescribe.skip,
    only: origDescribe.only,
    todo: origDescribe.todo,
    each: origDescribe.each,
    skipIf: origDescribe.skipIf,
    if: origDescribe.if,
  });

  function wrappedIt(name: string, fn: any) {
    // Capture describe path at registration time (synchronous)
    const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
    return origIt(
      name,
      () =>
        tracer.trace(name, (ctx: SpanContext<OpContext>) => {
          if (describePath) _describePathMap.set(ctx.buffer, describePath);
          return als.run(ctx, fn);
        }) as Promise<void>,
    );
  }
  Object.assign(wrappedIt, {
    skip: origIt.skip,
    only: origIt.only,
    todo: origIt.todo,
    each: origIt.each,
    skipIf: origIt.skipIf,
    if: origIt.if,
  });

  return {
    ...bunTestModule,
    describe: wrappedDescribe,
    it: wrappedIt,
    test: wrappedIt,
  };
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
describe.skip = _describe.skip;
describe.only = _describe.only;
describe.todo = _describe.todo;
describe.each = _describe.each;
describe.skipIf = _describe.skipIf;
describe.if = _describe.if;

/** Wrapped it — creates a root trace span for the test case */
export function it(name: string, fn: () => void | Promise<void>): void {
  const describePath = _standaloneDescribeStack.length > 0 ? _standaloneDescribeStack.join(' > ') : null;
  _it(name, () => {
    if (!_tracer) throw new Error('Call initTraceTestRun() in preload before tests');
    return _tracer.trace(name, (ctx: SpanContext<OpContext>) => {
      if (describePath) _describePathMap.set(ctx.buffer, describePath);
      return _als.run(ctx, fn);
    });
  });
}

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
