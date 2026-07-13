/**
 * Benchmark for real CallsitePlan-backed Op dispatch performance.
 *
 * Compares the root trace path with a minimal context shape and with three
 * user-context properties while keeping the trace ID and result semantics fixed.
 */

import { bench, do_not_optimize, group, run } from 'mitata';
import { defineLogSchema, defineOpContext, JsBufferStrategy, S, TestTracer } from '../src/index.js';
import { createTraceId } from '../src/lib/traceId.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';

const schema = defineLogSchema({
  value: S.number(),
  tagValue: S.category(),
});

const baseContext = defineOpContext({ logSchema: schema });
const extraContext = defineOpContext({
  logSchema: schema,
  ctx: {
    user_id: 'user-123',
    request_id: 'req-456',
    is_admin: true,
  },
});

const baseOp = baseContext.defineOp('trivial', (ctx) => ctx.ok(42));
const extraOp = extraContext.defineOp('trivial-with-context', (ctx) => ctx.ok(42));

const baseTracer = new TestTracer(baseContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});
const extraTracer = new TestTracer(extraContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});

const traceId = createTraceId('bench-trace');
const baseOverrides = { trace_id: traceId };
const extraOverrides = {
  trace_id: traceId,
  user_id: 'user-123',
  request_id: 'req-456',
  is_admin: true,
};

const baseSemantic = await baseTracer.trace(123, 'bench-span', baseOverrides, baseOp);
const extraSemantic = await extraTracer.trace(123, 'bench-span', extraOverrides, extraOp);
if (!baseSemantic.success || !extraSemantic.success || baseSemantic.value !== 42 || extraSemantic.value !== 42) {
  throw new Error('Op benchmark semantic check failed');
}
baseTracer.clear();
extraTracer.clear();

// 1. CallsitePlan-backed Op dispatch through the real tracer path.
group('Op dispatch performance', () => {
  bench('trivial op', async () => {
    const result = await baseTracer.trace(123, 'bench-span', baseOverrides, baseOp);
    if (!result.success) throw new Error('Trivial op benchmark unexpectedly failed');
    do_not_optimize(result.value);
    baseTracer.clear();
    return result.value;
  });
});

// 2. Context shape impact through a separate real OpContext/CallsitePlan.
group('Op dispatch with extra context properties', () => {
  bench('op with 3 extras', async () => {
    const result = await extraTracer.trace(123, 'bench-span', extraOverrides, extraOp);
    if (!result.success) throw new Error('Extra-context op benchmark unexpectedly failed');
    do_not_optimize(result.value);
    extraTracer.clear();
    return result.value;
  });
});

await run();
