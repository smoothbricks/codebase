/**
 * Span ownership is checked against the captured creating context at runtime.
 * TypeScript preserves result inference, but cannot distinguish repeated calls
 * of the same callback. These tests deliberately exercise well-typed misuse.
 */
import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { Transient } from '../../errors/Transient.js';
import { resolveEntryType, resolveMessage } from '../../resolveMessage.js';
import { Err, Ok, type Result } from '../../result.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_ERR, ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_OK } from '../../schema/systemSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { iterateSpanChildren } from '../../traceTopology.js';
import type { AnySpanBuffer } from '../../types.js';
import type { OpFn } from '../opTypes.js';
import type { OpContextOf, SpanContext } from '../types.js';

const testOpContext = defineOpContext({
  logSchema: defineLogSchema({ step: S.category() }),
});
const { defineOp } = testOpContext;
type TestCtx = OpContextOf<typeof testOpContext>;

function spanEndEntry(buffer: AnySpanBuffer): number {
  return resolveEntryType(buffer, 1);
}

function requireChild(buffer: AnySpanBuffer): AnySpanBuffer {
  const [child] = iterateSpanChildren(buffer);
  if (!child) throw new Error('Expected child span');
  return child;
}

async function expectOwnershipFailure(run: () => unknown, buffer: () => AnySpanBuffer): Promise<void> {
  let failure: unknown;
  try {
    await run();
  } catch (error) {
    failure = error;
  }
  expect(failure).toBeInstanceOf(TypeError);
  if (!(failure instanceof TypeError)) throw new Error('Expected ownership failure');
  expect(spanEndEntry(buffer())).toBe(ENTRY_TYPE_SPAN_EXCEPTION);
  // The trace explains the same failure delivered to the caller, including
  // failures thrown while finalizing an already-resolved async callback.
  expect(resolveMessage(buffer(), 1)).toBe(failure.message);
}

const RETRYABLE = Transient<Record<string, never>>('OWNERSHIP_RETRY', {
  backoff: 'fixed',
  maxAttempts: 3,
  baseDelayMs: 0,
  jitter: false,
});

describe('span-result ownership', () => {
  it('preserves precise sync and async result inference and explicit propagation', async () => {
    const child = defineOp('ownership-child', async (ctx, fail: boolean) => {
      if (fail) return ctx.err({ code: 'child-failed', retry: false });
      return ctx.ok({ count: 42 }).with({ step: 'child' });
    });
    const parent = defineOp('ownership-parent', async (ctx, fail: boolean) => {
      const result = await ctx.span('ownership-child-span', child, fail);
      if (!result.success) return ctx.err(result.error).with({ step: 'parent' });
      return ctx.ok(result.value.count.toFixed()).with({ step: 'parent' });
    });
    const sync = defineOp('ownership-sync', (ctx) => ctx.ok('sync'));
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    const syncResult = tracer.trace('ownership-sync-root', sync);
    expect(syncResult.value.toUpperCase()).toBe('SYNC');
    expect(spanEndEntry(tracer.rootBuffers[0])).toBe(ENTRY_TYPE_SPAN_OK);

    const success = await tracer.trace('ownership-parent-root', parent, false);
    if (!success.success) throw new Error('Expected successful parent');
    expect(success.value.toUpperCase()).toBe('42');
    const root = tracer.rootBuffers[1];
    expect(spanEndEntry(root)).toBe(ENTRY_TYPE_SPAN_OK);
    expect(spanEndEntry(requireChild(root))).toBe(ENTRY_TYPE_SPAN_OK);

    const failed = await tracer.trace('ownership-parent-error', parent, true);
    if (failed.success) throw new Error('Expected operational failure');
    expect(failed.error.code.toUpperCase()).toBe('CHILD-FAILED');
    expect(failed.error.retry).toBe(false);
    const failedRoot = tracer.rootBuffers[2];
    expect(spanEndEntry(failedRoot)).toBe(ENTRY_TYPE_SPAN_ERR);
    expect(spanEndEntry(requireChild(failedRoot))).toBe(ENTRY_TYPE_SPAN_ERR);
  });

  it('retains creating context through helpers, narrowing and transformations', async () => {
    function checkStep(ctx: SpanContext<TestCtx>, step: string): Result<number, string, TestCtx['logSchema']> {
      return step.length === 0 ? ctx.err('empty') : ctx.ok(step.length);
    }
    const ready: OpFn<TestCtx, [], boolean, string> = (ctx) => ctx.ok(true);
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    const result = await tracer.trace('ownership-helper', async (ctx) => {
      const readyResult = await ctx.span('ownership-ready', ready);
      if (!readyResult.success) return ctx.err(readyResult.error);
      const length = checkStep(ctx, 'validate');
      if (length.isErr()) return length.mapErr((error) => error.toUpperCase());
      return length.map((value) => value + 1).flatMap((value) => ctx.ok(String(value)));
    });
    if (!result.success) throw new Error('Expected helper success');
    expect(result.value).toBe('9');
    const error = tracer.trace('ownership-map-error', (ctx) =>
      ctx.err('invalid').mapErr((value) => value.toUpperCase()),
    );
    expect(error.error).toBe('INVALID');
    expect(spanEndEntry(tracer.rootBuffers[1])).toBe(ENTRY_TYPE_SPAN_ERR);
  });

  it('rejects unbound root results for inline and Op callbacks, sync and async', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    const unboundOp = defineOp('ownership-unbound-op', () => new Ok(1));
    const unboundAsyncOp = defineOp('ownership-unbound-async-op', async () => new Err('invalid'));
    await expectOwnershipFailure(
      () => tracer.trace('unbound-inline', () => new Ok(1)),
      () => tracer.rootBuffers[0],
    );
    await expectOwnershipFailure(
      () => tracer.trace('unbound-inline-async', async () => new Err('invalid')),
      () => tracer.rootBuffers[1],
    );
    await expectOwnershipFailure(
      () => tracer.trace('unbound-op', unboundOp),
      () => tracer.rootBuffers[2],
    );
    await expectOwnershipFailure(
      () => tracer.trace('unbound-op-async', unboundAsyncOp),
      () => tracer.rootBuffers[3],
    );
  });

  it('rejects a result retained across two invocations of the same callback', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    let retained: Ok<number, TestCtx['logSchema']> | undefined;
    const repeat = defineOp('ownership-repeat', (ctx) => {
      retained ??= ctx.ok(7);
      return retained;
    });
    expect(tracer.trace('ownership-first', repeat).value).toBe(7);
    await expectOwnershipFailure(
      () => tracer.trace('ownership-second', repeat),
      () => tracer.rootBuffers[1],
    );
    expect(spanEndEntry(tracer.rootBuffers[0])).toBe(ENTRY_TYPE_SPAN_OK);
  });

  it('rejects parent results in synchronous child callbacks', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    await expectOwnershipFailure(
      () => tracer.trace('ownership-parent-capture', (ctx) => ctx.spanSync('ownership-sync-child', () => ctx.ok(1))),
      () => requireChild(tracer.rootBuffers[0]),
    );
    expect(spanEndEntry(tracer.rootBuffers[0])).toBe(ENTRY_TYPE_SPAN_EXCEPTION);
  });

  it('rejects completed child results and transformations in the parent callback', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    await expectOwnershipFailure(
      () =>
        tracer.trace('ownership-passthrough', async (ctx) => {
          const result = await ctx.span('ownership-finished-child', (childCtx) => childCtx.ok(1));
          return result.map((value) => value + 1);
        }),
      () => tracer.rootBuffers[0],
    );
    expect(spanEndEntry(requireChild(tracer.rootBuffers[0]))).toBe(ENTRY_TYPE_SPAN_OK);
  });

  it('rejects a sibling result selected by flatMap', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    await expectOwnershipFailure(
      () =>
        tracer.trace('ownership-siblings', async (ctx) => {
          const sibling = await ctx.span('ownership-first-sibling', (childCtx) => childCtx.ok(1));
          await ctx.span('ownership-second-sibling', (childCtx) => childCtx.ok(2).flatMap(() => sibling));
          return ctx.ok(null);
        }),
      () => tracer.rootBuffers[0],
    );
    const children = Array.from(iterateSpanChildren(tracer.rootBuffers[0]));
    expect(children.map(spanEndEntry)).toEqual([ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_EXCEPTION]);
  });

  it('rejects foreign transient results before retrying ordinary child dispatch', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    let attempts = 0;
    await expectOwnershipFailure(
      () =>
        tracer.trace('ownership-retry-parent', async (ctx) => {
          await ctx.span('ownership-retry-child', () => {
            attempts++;
            return ctx.err(RETRYABLE({}));
          });
          return ctx.ok(null);
        }),
      () => requireChild(tracer.rootBuffers[0]),
    );
    expect(attempts).toBe(1);
  });

  it('checks transformed spanAuto dispatch before synchronous success or async retry', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    const unbound = defineOp('ownership-auto-unbound', () => new Ok(1));
    await expectOwnershipFailure(
      () => tracer.trace('ownership-auto-sync', (ctx) => ctx.spanAuto0(1, 'ownership-auto-child', unbound)),
      () => requireChild(tracer.rootBuffers[0]),
    );
    let attempts = 0;
    const retry = defineOp('ownership-auto-retry', async () => {
      attempts++;
      return new Err(RETRYABLE({}));
    });
    await expectOwnershipFailure(
      () => tracer.trace('ownership-auto-async', (ctx) => ctx.spanAuto0(1, 'ownership-auto-retry-child', retry)),
      () => requireChild(tracer.rootBuffers[1]),
    );
    expect(attempts).toBe(1);
  });

  it('checks later results after accepting a synchronous first retry', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    const foreign = tracer.trace('ownership-foreign-source', (ctx) => ctx.ok('foreign'));
    let attempts = 0;
    const retry = defineOp('ownership-retry-then-foreign', (ctx) => {
      attempts++;
      return attempts === 1 ? ctx.err(RETRYABLE({})) : foreign;
    });
    await expectOwnershipFailure(
      () => tracer.trace('ownership-later-result', (ctx) => ctx.spanAuto0(1, 'ownership-later-child', retry)),
      () => requireChild(tracer.rootBuffers[1]),
    );
    expect(attempts).toBe(2);
  });
});
