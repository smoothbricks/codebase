/**
 * Type safety test - verify Ops with incompatible schemas cause compile errors
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createTraceRoot } from '../traceRoot.node.js';
import { TestTracer } from '../tracers/TestTracer.js';

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
    const { trace } = new TestTracer(opContextA, { createTraceRoot });

    // This SHOULD work - opA was created with opContextA's schema
    await trace('test-a', opA);
  });

  it('should ERROR when passing Op with incompatible schema', async () => {
    // New API: pass opContext directly
    const { trace } = new TestTracer(opContextA, { createTraceRoot });

    // This SHOULD cause a type error - opB has different schema than tracer
    // The @ts-expect-error verifies the type system catches this at compile time
    // At runtime, calling an op with incompatible schema will throw because
    // the tag methods don't exist on the buffer
    let threw = false;
    try {
      // @ts-expect-error - opB has schemaB but tracer expects schemaA
      await trace('test-b', opB);
    } catch (e) {
      threw = true;
      // Expected: ctx.tag.orderId is not a function because schemaA doesn't have orderId
      expect(e).toBeInstanceOf(TypeError);
    }
    expect(threw).toBe(true);
  });
});
