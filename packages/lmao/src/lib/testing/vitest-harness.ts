/**
 * vitest integration for LMAO trace-testing.
 *
 * Same architecture as bun-harness: one root TestTracer for the entire vitest
 * run, each it() creates a root trace span, operations create child spans.
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
import { createTraceRoot } from '../traceRoot.universal.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { SyncSQLiteDatabase } from './sqlite-db.js';
import { type TraceSQLiteConfig, TraceSQLiteSink } from './sqlite-sink.js';

// Global singleton — one root tracer for the entire vitest run
let _tracer: TestTracer<OpContextBinding> | null = null;
let _sink: TraceSQLiteSink | null = null;

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
  _tracer = new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  }) as TestTracer<OpContextBinding>;

  if (options?.sqlite?.createDatabase) {
    const db = options.sqlite.createDatabase(options.sqlite.dbPath ?? '.trace-results.db');
    _sink = new TraceSQLiteSink(db, options.sqlite);
  }

  // Register global teardown
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
  if (!_tracer) throw new Error('Call initTraceTestRun() in setupFiles before tests');
  return _tracer;
}

/** Wrapped describe — pass-through grouping (no trace span) */
export const describe: typeof _describe = _describe;

/** Wrapped it — creates a root trace span for the test case */
export function it(name: string, fn: () => void | Promise<void>): void {
  _it(name, () => {
    if (!_tracer) throw new Error('Call initTraceTestRun() in setupFiles before tests');
    return _tracer.trace(name, (ctx: SpanContext<OpContext>) => _als.run(ctx, fn));
  });
}

it.skip = _it.skip;
it.only = _it.only;
it.todo = _it.todo;
it.each = _it.each;
it.skipIf = _it.skipIf;

// Re-export everything else unchanged
export {
  _afterAll as afterAll,
  _afterEach as afterEach,
  _beforeAll as beforeAll,
  _beforeEach as beforeEach,
  _expect as expect,
};
