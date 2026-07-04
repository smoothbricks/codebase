/**
 * Cloudflare trace-sink adapters (specs/lmao/01u_cloudflare_trace_segments.md).
 *
 * Two delivery lanes over the base Tracer lifecycle, no Cloudflare types required — every
 * platform binding (Pipelines stream send, Queues producer, collector shard) is injected
 * structurally:
 * - billing-grade: CollectorClient (awaited within the request; ack = DO storage commit)
 * - diagnostic: DiagnosticDrainTracer + transports (waitUntil, fire-and-forget)
 * - both: ClassSplitTracer routes rows per delivery class via an injected classifier
 */

export {
  type ClassSplitOptions,
  ClassSplitTracer,
  type DeliveryClass,
  type DeliveryClassifier,
} from './lib/cloudflare/classSplit.js';
export {
  COLLECTOR_ACK,
  type CollectorAck,
  type CollectorClient,
  type CollectorSendFailure,
  FakeCollectorClient,
} from './lib/cloudflare/collectorClient.js';
export {
  type DiagnosticDrainOptions,
  DiagnosticDrainTracer,
  type DiagnosticTransport,
  type PipelinesStreamSend,
  PipelinesStreamTransport,
  type QueuesFallbackOptions,
  QueuesFallbackTransport,
  type TraceChunkQueueMessage,
} from './lib/cloudflare/diagnosticDrain.js';
export { spanBufferToTraceRows, type TraceRow, type TraceRowValue } from './lib/cloudflare/traceRows.js';
