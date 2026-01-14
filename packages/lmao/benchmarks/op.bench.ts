/**
 * Benchmark for Op._invoke performance
 *
 * This benchmark verifies:
 * 1. V8 monomorphic call site optimization when using span_op/span_fn
 * 2. Performance impact of Object.keys() removal in hot paths
 */

import { bench, group, run } from 'mitata';
import { defineModule, S } from '../src/index.js';
import { createTraceId } from '../src/lib/traceId.js';

// Setup module and ops
const module = defineModule('bench-module', {
  schema: {
    value: S.number(),
    tag: S.category(),
  },
});

const trivialOp = module.op('trivial', async (_ctx) => {
  return 42;
});

const traceId = createTraceId('bench-trace');
const traceCtx = module.traceContext();
// Mock traceId since we're using the real TraceContext but need it stable
(traceCtx as any).traceId = traceId;

// 1. Direct Op._invoke (simulated)
group('Op._invoke performance', () => {
  bench('trivial op', async () => {
    return await (trivialOp as any)._invoke(
      traceCtx,
      null, // parentBuffer
      module._context, // callsiteModule
      'bench-span',
      123, // lineNumber
      [], // args
    );
  });
});

// 2. Different context properties impact
const traceCtxWithExtra = module.traceContext({
  user_id: 'user-123',
  request_id: 'req-456',
  is_admin: true,
});

group('Op._invoke with Extra properties', () => {
  bench('op with 3 extras', async () => {
    return await (trivialOp as any)._invoke(traceCtxWithExtra, null, module._context, 'bench-span', 123, []);
  });
});

await run();
