/**
 * Test helpers for lmao-transformer tests
 */

import type { ITraceRoot, TracerLifecycleHooks } from '@smoothbricks/lmao';
import { createTraceRoot } from '@smoothbricks/lmao/es';

/** No-op tracer satisfying TracerLifecycleHooks for transformer tests */
const noOpTracer: TracerLifecycleHooks = {
  onTraceStart: () => {},
  onTraceEnd: () => {},
  onSpanStart: () => {},
  onSpanEnd: () => {},
  onStatsWillResetFor: () => {},
  bufferStrategy: {
    // Transformer tests never call these - stubs to satisfy the interface
    createChildSpanBuffer() {
      throw new Error('not implemented in transformer test helper');
    },
    createOverflowBuffer() {
      throw new Error('not implemented in transformer test helper');
    },
  },
};

/**
 * Create a TraceRoot for testing in transformer tests.
 *
 * @param traceId - Trace ID (string)
 */
export function createTestTraceRoot(traceId: string): ITraceRoot {
  return createTraceRoot(traceId, noOpTracer);
}
