/// <reference types="bun" />

/**
 * bun:test integration for LMAO trace-testing.
 *
 * Transparently wraps bun:test's it()/test() via mock.module so every test
 * automatically creates a trace span — no import changes needed in test files.
 *
 * Architecture:
 * - initTraceTestRun() in preload sets up tracer + SQLite sink
 * - mock.module('bun:test', ...) in preload patches it()/test() with trace spans
 * - One root trace per test run (tracer.trace('test-run', ...))
 * - Each it()/test() creates a child span of the root (rootCtx.span(name, fn))
 * - describe() is wrapped to track nesting — path written to `describe` schema column
 * - After all tests: flush root buffer tree to SQLite (if configured)
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
import { type OpContext, type OpContextBinding, type OpContextOf, opContextType } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { InferSchema } from '../schema/types.js';
import { type TraceSQLiteConfig, TraceSQLiteSink } from '../sqlite/sqlite-sink.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { TestTracer } from '../tracers/TestTracer.js';

/** bun:test expect() errors start with 'expect(received).' — distinguishes assertion failures from other throws */
function isExpectError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith('expect(');
}

type TestBody = () => unknown | Promise<unknown>;

type DescribeFieldSchema = { describe: ReturnType<typeof S.category> };

type ExtendedLogSchema<T extends LogSchema> =
  T extends LogSchema<infer Fields> ? LogSchema<Fields & DescribeFieldSchema> : never;

type ExtendedOpContext<Ctx extends OpContext> = Omit<Ctx, 'logSchema'> & {
  logSchema: ExtendedLogSchema<Ctx['logSchema']>;
};

type BindingWithDescribe<B extends OpContextBinding> = B & {
  readonly [opContextType]: ExtendedOpContext<OpContextOf<B>>;
  readonly logBinding: Omit<B['logBinding'], 'logSchema'> & {
    readonly logSchema: ExtendedLogSchema<B['logBinding']['logSchema']>;
  };
};

type HarnessSpanContext<B extends OpContextBinding> = SpanContext<OpContextOf<BindingWithDescribe<B>>>;

type RootSpanRunner<Ctx extends OpContext> = (
  name: string,
  fn: (ctx: SpanContext<Ctx>) => Promise<unknown>,
) => Promise<unknown>;

export interface BunTestTracerInstance<B extends OpContextBinding> {
  setup(): void;
  useTestSpan(): HarnessSpanContext<B>;
  getTracer(): TestTracer<BindingWithDescribe<B>>;
  createBunTestMock(bunTestModule: Record<string, unknown>): Record<string, unknown>;
}

/**
 * Create an instance-scoped Bun test tracer harness.
 *
 * Unlike initTraceTestRun(), this keeps tracer/root/ALS state isolated per instance.
 */
export function makeTestTracer<B extends OpContextBinding>(
  binding: B,
  options?: {
    sqlite?: TraceSQLiteConfig;
  },
): BunTestTracerInstance<B> {
  type ExtendedBinding = BindingWithDescribe<B>;
  type SpanCtx = HarnessSpanContext<B>;

  let tracer: TestTracer<ExtendedBinding> | null = null;
  let sink: TraceSQLiteSink | null = null;
  let rootCtx: SpanCtx | null = null;
  let resolveTestRun: (() => void) | null = null;
  let rootTracePromise: Promise<unknown> | null = null;
  let isSetup = false;

  const als = new AsyncLocalStorage<SpanCtx>();

  function assertRootCtx(): SpanCtx {
    if (!rootCtx) throw new Error('Call setup() before wiring bun:test wrappers');
    return rootCtx;
  }

  function assertTracer(): TestTracer<ExtendedBinding> {
    if (!tracer) throw new Error('Call setup() in preload before tests');
    return tracer;
  }

  function runTracedTest(name: string, fn: TestBody, describePath: string | null): Promise<unknown> {
    const currentRoot = assertRootCtx();
    return (currentRoot.span as RootSpanRunner<OpContextOf<ExtendedBinding>>)(name, async (ctx) => {
      if (describePath) {
        ctx.tag.with({ describe: describePath } as Partial<InferSchema<OpContextOf<ExtendedBinding>['logSchema']>>);
      }
      try {
        await als.run(ctx, fn);
        return ctx.ok(undefined); // pass -> span-ok
      } catch (error) {
        if (isExpectError(error)) {
          return ctx.err(error); // expect() fail -> span-err
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
        each: origDescribe.each,
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
        each: origIt.each,
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

      const extendedSchema = binding.logBinding.logSchema.extend({ describe: S.category() });
      const extendedBinding = {
        ...binding,
        logBinding: {
          ...binding.logBinding,
          logSchema: extendedSchema,
        },
      } as ExtendedBinding;

      tracer = new TestTracer(extendedBinding, {
        bufferStrategy: new JsBufferStrategy(),
        createTraceRoot,
      });

      if (options?.sqlite) {
        const db = new Database(options.sqlite.dbPath ?? '.trace-results.db');
        sink = new TraceSQLiteSink(db);
      }

      rootTracePromise = tracer.trace('test-run', (ctx) => {
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
            console.log(`\n[trace] trace_id: ${traceId} -> ${dbPath}`);
          } catch (e) {
            console.error('[lmao/testing] SQLite flush error:', e);
          }

          sink.close();
          sink = null;
        }
      });
    },

    useTestSpan(): SpanCtx {
      const ctx = als.getStore();
      if (!ctx) throw new Error('useTestSpan() called outside of a traced it()');
      return ctx;
    },

    getTracer(): TestTracer<ExtendedBinding> {
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
let _tracer: TestTracer<OpContextBinding> | null = null;
let _sink: TraceSQLiteSink | null = null;

// Root span context, its resolver, and the trace promise — one root per test run
let _rootCtx: SpanContext<OpContext> | null = null;
let _resolveTestRun: (() => void) | null = null;
let _rootTracePromise: Promise<unknown> | null = null;

// AsyncLocalStorage for propagating SpanContext to test bodies.
// Each it() runs in its own async context so concurrent tests don't collide.
const _als = new AsyncLocalStorage<SpanContext<OpContext>>();
type RootSpanInvoker = (name: string, fn: (ctx: SpanContext<OpContext>) => Promise<unknown>) => Promise<unknown>;
type DescribeTag = { describe: (path: string) => unknown };

/** Initialize the root tracer for the entire bun test run. Call once in preload. */
export function initTraceTestRun<B extends OpContextBinding>(
  opContext: B,
  options?: {
    sqlite?: TraceSQLiteConfig;
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

  if (options?.sqlite) {
    const db = new Database(options.sqlite.dbPath ?? '.trace-results.db');
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
  if (!_rootCtx) throw new Error('Call initTraceTestRun() before createBunTestMock()');

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
    each: origDescribe.each,
    skipIf: (condition: boolean) => (condition ? origDescribe.skip : wrappedDescribe),
    if: (condition: boolean) => (condition ? wrappedDescribe : origDescribe.skip),
  });

  function wrappedIt(name: string, fn: TestBody) {
    // Capture describe path at registration time (synchronous)
    const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
    return origIt(name, () =>
      (rootCtx.span as RootSpanInvoker)(name, async (ctx: SpanContext<OpContext>) => {
        if (describePath) (ctx.tag as unknown as DescribeTag).describe(describePath);
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
  // skipIf/if must return wrappedIt (not origIt) when the test should run,
  // so the ALS context is set up and useTestSpan() works inside the test body.
  Object.assign(wrappedIt, {
    skip: origIt.skip,
    only: origIt.only,
    todo: origIt.todo,
    each: origIt.each,
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
      if (describePath) (ctx.tag as unknown as DescribeTag).describe(describePath);
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
