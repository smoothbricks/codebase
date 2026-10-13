/**
 * Type safety test - verify Ops with incompatible schemas cause compile errors
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import type { AnyOp, Op } from '../op.js';
import type { OpDefinition } from '../opContext/opTypes.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

// Two different schemas
const schemaA = defineLogSchema({
  userId: S.category(),
});

const schemaB = defineLogSchema({
  orderId: S.category(),
});

// Two different op contexts
const opContextA = defineOpContext({
  logSchema: schemaA,
});
const { defineOp: defineOpA } = opContextA;

const opContextB = defineOpContext({
  logSchema: schemaB,
});
const { defineOp: defineOpB } = opContextB;

// Ops from each context
const opA = defineOpA('op-a', (ctx) => {
  ctx.tag.userId('user-123'); // Should work - userId is in schemaA
  return ctx.ok({ a: 1 });
});

const opB = defineOpB('op-b', (ctx) => {
  ctx.tag.orderId('order-456'); // Should work - orderId is in schemaB
  return ctx.ok({ b: 2 });
});

describe('Op type safety', () => {
  it('should allow Op with matching schema', async () => {
    // New API: pass opContext directly, no explicit type parameter needed
    const { trace } = new TestTracer(opContextA, { ...createTestTracerOptions() });

    // This SHOULD work - opA was created with opContextA's schema
    await trace('test-a', opA);
  });

  it('rejects an incompatible schema at compile time while its callsite plan stays self-contained at runtime', async () => {
    const { trace } = new TestTracer(opContextA, { ...createTestTracerOptions() });

    // @ts-expect-error - opB has schemaB but tracer expects schemaA
    const result = await trace('test-b', opB);

    expect(result.success).toBe(true);
  });

  it('accepts ops with typed args in defineOps and keeps their argument types and sealed plans', async () => {
    const get = defineOpA('get', async (ctx, url: string, retries?: number) => ctx.ok({ url, retries }));
    const post = async (ctx: Parameters<typeof get.fn>[0], url: string, body: { id: number }) =>
      ctx.ok({ url, id: body.id });
    const ops = opContextA.defineOps({ get, post });

    type ArgsOf<F> = F extends (ctx: never, ...args: infer A) => unknown ? A : never;
    const getArgs: ArgsOf<typeof ops.get.fn> = ['/a', 2];
    const postArgs: ArgsOf<typeof ops.post.fn> = ['/b', { id: 1 }];
    // @ts-expect-error - get's url is a string
    const badGetArgs: ArgsOf<typeof ops.get.fn> = [42];
    // @ts-expect-error - post's body needs an id
    const badPostArgs: ArgsOf<typeof ops.post.fn> = ['/b', {}];
    expect([badGetArgs, badPostArgs]).toHaveLength(2);

    type CtxOf<T> = T extends Op<infer C, infer _A, infer _S, infer _E, infer _R> ? C : never;
    const widened: AnyOp<CtxOf<typeof ops.get>> = ops.get;
    // @ts-expect-error - a widened op admits no call, so get's url cannot be omitted
    const callWidened = (ctx: Parameters<typeof widened.fn>[0]) => widened.fn(ctx);
    const widenedFn: OpDefinition<CtxOf<typeof ops.get>> = post;
    // @ts-expect-error - a widened definition also admits no call with missing arguments
    const callWidenedFn = (ctx: Parameters<typeof post>[0]) => widenedFn(ctx);
    expect([callWidened, callWidenedFn].every((fn) => typeof fn === 'function')).toBe(true);
    expect(ops.get).toBe(get);
    expect(ops.get.callsitePlan).toBe(get.callsitePlan);
    expect(Object.isFrozen(ops.get.callsitePlan)).toBe(true);

    const { trace } = new TestTracer(opContextA, { ...createTestTracerOptions() });
    const result = await trace('typed-args', async (ctx) => {
      const got = await ctx.span('get', ops.get, ...getArgs);
      if (!got.success) return ctx.err(got.error);
      const posted = await ctx.span('post', ops.post, ...postArgs);
      if (!posted.success) return ctx.err(posted.error);
      return ctx.ok({ url: got.value.url, id: posted.value.id });
    });

    expect(result.success && result.value).toEqual({ url: '/a', id: 1 });
  });

  it('contextually types a raw definition and preserves the synchronous Op return kind', () => {
    const ops = opContextA.defineOps({
      get: (ctx, url: string) => ctx.ok({ url }),
    });
    const { trace } = new TestTracer(opContextA, { ...createTestTracerOptions() });
    const result = trace('sync-typed-args', ops.get, '/sync');

    expect(result).not.toBeInstanceOf(Promise);
    expect(result.success && result.value).toEqual({ url: '/sync' });
  });
});
