/**
 * Test helpers for lmao-transformer tests
 */

import { createTraceId, type TraceId, type TraceRoot } from '@smoothbricks/lmao';

/**
 * Create a TraceRoot for testing in transformer tests.
 *
 * @param traceId - Trace ID (string or TraceId)
 */
export function createTestTraceRoot(traceId: TraceId | string): TraceRoot {
  const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
  const anchorPerfNow =
    typeof process !== 'undefined' && process.hrtime ? Number(process.hrtime.bigint()) : performance.now();

  // Accept string or TraceId - convert string to TraceId
  const resolvedTraceId = typeof traceId === 'string' ? createTraceId(traceId) : traceId;

  // No-op tracer for transformer tests
  const noOpTracer = {
    onTraceStart: () => {},
    onTraceEnd: () => {},
    onSpanStart: () => {},
    onSpanEnd: () => {},
    onStatsWillResetFor: () => {},
  };

  return {
    trace_id: resolvedTraceId,
    anchorEpochNanos,
    anchorPerfNow,
    tracer: noOpTracer,
  };
}
