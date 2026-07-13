import { describe, expect, it } from 'bun:test';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { Transient } from '../errors/Transient.js';
import type { OpContext, OpContextOf, SpanContext } from '../opContext/types.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_FF,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_RESERVED_MASK,
  RUNTIME_HINT_RESULT,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { InMemoryFlagEvaluator, type SpanContextWithoutFf } from '../schema/evaluator.js';
import {
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_RETRY,
} from '../schema/systemSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

const schema = defineLogSchema({ marker: S.category() });
const opContext = defineOpContext({ logSchema: schema, ctx: { inheritedMarker: 'parent' } });
const analyzedResult = RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 2;

function childOf(tracer: TestTracer<typeof opContext>) {
  const child = tracer.rootBuffers[0]?._children[0];
  if (!child) throw new Error('expected one child span buffer');
  return child;
}

class GetterCountingPromise<T> implements Promise<T> {
  readonly [Symbol.toStringTag] = 'Promise';

  constructor(
    private readonly value: T,
    private readonly onThenAccess: () => void,
  ) {}

  get then(): Promise<T>['then'] {
    this.onThenAccess();
    return <TResult1 = T, TResult2 = never>(
      onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
    ) => Promise.resolve(this.value).then(onfulfilled, onrejected);
  }

  catch<TResult = never>(
    onrejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
  ): Promise<T | TResult> {
    return Promise.resolve(this.value).catch(onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<T> {
    return Promise.resolve(this.value).finally(onfinally);
  }
}

describe('spanAuto synchronous and fallback execution', () => {
  it('returns a synchronous Result and fires exact hooks around one terminal row', async () => {
    const op = opContext.defineOp('sync', (ctx) => ctx.ok('done'), undefined, {
      runtimeHint: analyzedResult,
    });
    const plan = op.callsitePlan;
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    const hooks: string[] = [];
    tracer.onSpanStart = (buffer) => hooks.push(`start:${buffer.message_values[0]}`);
    tracer.onSpanEnd = (buffer) => hooks.push(`end:${buffer.message_values[0]}`);

    await tracer.trace('root', async (ctx) => {
      const result = ctx.spanAuto0(41, 'sync-child', op);
      expect(result).not.toBeInstanceOf(Promise);
      if (result instanceof Promise) throw new Error('sync spanAuto unexpectedly returned a Promise');
      expect(result.success).toBe(true);
      expect(hooks).toEqual(['start:sync-child', 'end:sync-child']);
      return ctx.ok(null);
    });

    const child = childOf(tracer);
    expect(child._writeIndex).toBe(2);
    expect(child.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    expect(child._overflow).toBeUndefined();
    expect(hooks).toEqual(['start:sync-child', 'end:sync-child']);
    expect(op.callsitePlan).toBe(plan);
    expect(child).toBeInstanceOf(plan.SpanBufferClass);
  });

  it('propagates a synchronous onSpanEnd failure without a second hook or exception rewrite', async () => {
    const op = opContext.defineOp('hook-throw', (ctx) => ctx.ok('done'), undefined, {
      runtimeHint: analyzedResult,
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    const hookFailure = new Error('hook exploded');
    let starts = 0;
    let ends = 0;
    tracer.onSpanStart = () => starts++;
    tracer.onSpanEnd = () => {
      ends++;
      throw hookFailure;
    };

    await tracer.trace('root', async (ctx) => {
      expect(() => ctx.spanAuto0(42, 'hook-throw', op)).toThrow('hook exploded');
      return ctx.ok(null);
    });

    const child = childOf(tracer);
    expect(starts).toBe(1);
    expect(ends).toBe(1);
    expect(child._writeIndex).toBe(2);
    expect(child.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    expect(child.message_values[1]).not.toBe('hook exploded');
  });

  it('falls back to a Promise for a thenable and still writes one terminal row', async () => {
    const op = opContext.defineOp('async', async (ctx) => ctx.ok('later'), undefined, {
      runtimeHint: analyzedResult,
    });
    const plan = op.callsitePlan;
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    await tracer.trace('root', async (ctx) => {
      const pending = ctx.spanAuto0(5, 'async-child', op);
      expect(pending).toBeInstanceOf(Promise);
      const result = await pending;
      expect(result.success).toBe(true);
      return ctx.ok(null);
    });

    const child = childOf(tracer);
    expect(child._writeIndex).toBe(2);
    expect(child.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    expect(op.callsitePlan).toBe(plan);
    expect(child).toBeInstanceOf(plan.SpanBufferClass);
  });

  it('observes a custom Promise then getter exactly once', async () => {
    let thenAccesses = 0;
    const op = opContext.defineOp(
      'custom-then',
      (ctx) =>
        new GetterCountingPromise(ctx.ok('custom'), () => {
          thenAccesses++;
        }),
      undefined,
      { runtimeHint: analyzedResult },
    );
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    await tracer.trace('root', async (ctx) => {
      const result = await ctx.spanAuto0(6, 'custom-then', op);
      expect(result.success).toBe(true);
      return ctx.ok(null);
    });

    expect(thenAccesses).toBe(1);
    expect(childOf(tracer).entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
  });

  it('falls back to retry when the first synchronous Result is transient', async () => {
    const RETRY = Transient<Record<string, never>>('RETRY', {
      backoff: 'fixed',
      baseDelayMs: 0,
      jitter: false,
      maxAttempts: 2,
    });
    let attempts = 0;
    const op = opContext.defineOp(
      'retry',
      (ctx) => {
        attempts++;
        return attempts === 1 ? ctx.err(RETRY({})) : ctx.ok('recovered');
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 3 },
    );
    const plan = op.callsitePlan;
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    await tracer.trace('root', async (ctx) => {
      const pending = ctx.spanAuto0(9, 'retry-child', op);
      expect(pending).toBeInstanceOf(Promise);
      const result = await pending;
      expect(result.success).toBe(true);
      return ctx.ok(null);
    });

    const child = childOf(tracer);
    expect(attempts).toBe(2);
    expect(child.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    expect(child.entry_type[2]).toBe(ENTRY_TYPE_SPAN_RETRY);
    expect(child._writeIndex).toBe(3);
    expect(op.callsitePlan).toBe(plan);
    expect(child).toBeInstanceOf(plan.SpanBufferClass);
  });

  it('records a rejected Promise as an exception and ends hooks exactly once', async () => {
    const failure = new Error('async exploded');
    const op = opContext.defineOp('reject', () => Promise.reject(failure), undefined, {
      runtimeHint: analyzedResult,
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    let starts = 0;
    let ends = 0;
    tracer.onSpanStart = () => starts++;
    tracer.onSpanEnd = () => ends++;

    await tracer.trace('root', async (ctx) => {
      const pending = ctx.spanAuto0(7, 'rejected-child', op);
      expect(pending).toBeInstanceOf(Promise);
      await expect(pending).rejects.toBe(failure);
      return ctx.ok(null);
    });

    const child = childOf(tracer);
    expect(starts).toBe(1);
    expect(ends).toBe(1);
    expect(child._writeIndex).toBe(2);
    expect(child.entry_type[1]).toBe(ENTRY_TYPE_SPAN_EXCEPTION);
    expect(child.message_values[1]).toBe('async exploded');
  });
});

describe('runtime hint specialization', () => {
  it('uses full setup for zero and invalid hints', async () => {
    for (const { name, hint } of [
      { name: 'zero', hint: 0 },
      { name: 'reserved', hint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESERVED_MASK },
    ]) {
      let childContext: SpanContext<OpContextOf<typeof opContext>> | undefined;
      const op = opContext.defineOp(
        name,
        (ctx) => {
          childContext = ctx;
          return ctx.ok(name);
        },
        undefined,
        { runtimeHint: hint },
      );
      const tracer = new TestTracer(opContext, createTestTracerOptions());
      await tracer.trace('root', async (ctx) => {
        await ctx.spanAuto0(1, name, op);
        return ctx.ok(null);
      });
      if (!childContext) throw new Error(`missing ${name} child context`);
      for (const capability of ['tag', 'log', 'ff', 'span', 'ok', 'err', 'setScope', 'deps']) {
        expect(Object.hasOwn(childContext, capability)).toBe(true);
      }
    }
  });

  it('omits unused capabilities while terminal writes stay on the child buffer', async () => {
    let childContext: SpanContext<OpContextOf<typeof opContext>> | undefined;
    const op = opContext.defineOp(
      'result-only',
      (ctx) => {
        childContext = ctx;
        return ctx.ok('child');
      },
      undefined,
      { runtimeHint: analyzedResult },
    );
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    await tracer.trace('root', async (ctx) => {
      const result = ctx.spanAuto0(1, 'result-only', op);
      if (result instanceof Promise) throw new Error('result-only spanAuto unexpectedly returned a Promise');
      expect(result.success).toBe(true);
      return ctx.ok('parent');
    });

    if (!childContext) throw new Error('missing captured child context');
    expect(Object.hasOwn(childContext, 'ok')).toBe(true);
    expect(Object.hasOwn(childContext, 'tag')).toBe(false);
    expect(Object.hasOwn(childContext, 'log')).toBe(false);
    expect(tracer.rootBuffers[0].entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    expect(childOf(tracer).entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
  });

  it('honors static capacity including the two-row minimum and preserves overflow rows', async () => {
    const minimumOp = opContext.defineOp('minimum', (ctx) => ctx.ok(null), undefined, {
      runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 1,
    });
    const loggingOp = opContext.defineOp(
      'overflow',
      (ctx) => {
        ctx.log.info('first');
        ctx.log.info('second');
        return ctx.ok(null);
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | 3 },
    );
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    await tracer.trace('root', async (ctx) => {
      ctx.spanAuto0(1, 'minimum', minimumOp);
      ctx.spanAuto0(2, 'overflow', loggingOp);
      return ctx.ok(null);
    });

    const minimum = tracer.rootBuffers[0]._children[0];
    const overflow = tracer.rootBuffers[0]._children[1];
    expect(minimum._capacity).toBe(2);
    expect(minimum._overflow).toBeUndefined();
    expect(overflow._capacity).toBe(3);
    expect(overflow.entry_type[2]).toBe(ENTRY_TYPE_INFO);
    expect(overflow._overflow?.entry_type[0]).toBe(ENTRY_TYPE_INFO);
    expect(overflow._overflow?.message_values[0]).toBe('second');
    expect(convertSpanTreeToArrowTable(tracer.rootBuffers[0]).numRows).toBe(8);
  });

  it('maps defineOps hints by key and preserves them through prefixed and mapped clones', () => {
    const existing = opContext.defineOp('existing', (ctx) => ctx.ok(null), undefined, {
      runtimeHint: analyzedResult | 7,
    });
    const group = opContext.defineOps(
      {
        existing,
        raw: (ctx) => ctx.ok(null),
      },
      { raw: { runtimeHint: analyzedResult | 5 } },
    );

    expect(group.existing).toBe(existing);
    expect(group.existing.callsitePlan.runtimeHint).toBe(analyzedResult | 7);
    expect(group.raw.callsitePlan.runtimeHint).toBe(analyzedResult | 5);
    expect(group.prefix('lib').existing.callsitePlan.runtimeHint).toBe(analyzedResult | 7);
    expect(group.mapColumns({ marker: 'renamedMarker' }).raw.callsitePlan.runtimeHint).toBe(analyzedResult | 5);

    const overridden = opContext.defineOps(
      { existing },
      { existing: { runtimeHint: analyzedResult | 9 } },
    );
    expect(overridden.existing).not.toBe(existing);
    expect(overridden.existing.callsitePlan.runtimeHint).toBe(analyzedResult | 9);
  });
});

describe('feature evaluator child context identity', () => {
  const flags = defineFeatureFlags({ enabled: S.boolean().default(false).sync() });
  const featureContext = defineOpContext({
    logSchema: defineLogSchema({}),
    flags: flags.schema,
    ctx: { inheritedMarker: 'from-parent' },
  });
  type FeatureOpContext = OpContextOf<typeof featureContext>;

  class CapturingEvaluator extends InMemoryFlagEvaluator {
    readonly contexts: SpanContextWithoutFf<OpContext>[] = [];

    override getSync<K extends string>(ctx: SpanContextWithoutFf<OpContext>, flag: K) {
      this.contexts.push(ctx);
      return super.getSync(ctx, flag);
    }
  }

  it('binds the evaluator to the direct inherited child rather than a copied context', async () => {
    const evaluator = new CapturingEvaluator(flags.schema, { enabled: true });
    let childContext: SpanContext<FeatureOpContext> | undefined;
    const op = featureContext.defineOp(
      'flagged',
      (ctx) => {
        childContext = ctx;
        expect(ctx.ff.enabled?.value).toBe(true);
        return ctx.ok(null);
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_FF | RUNTIME_HINT_RESULT | 3 },
    );
    const tracer = new TestTracer(featureContext, { ...createTestTracerOptions(), flagEvaluator: evaluator });

    await tracer.trace('root', async (ctx) => {
      ctx.spanAuto0(1, 'flagged', op);
      return ctx.ok(null);
    });

    if (!childContext) throw new Error('missing captured feature context');
    expect(evaluator.contexts).toEqual([childContext]);
    expect(childContext.inheritedMarker).toBe('from-parent');
  });
});
