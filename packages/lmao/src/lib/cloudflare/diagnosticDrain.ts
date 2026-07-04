/**
 * Diagnostic-class drain adapter.
 *
 * Per specs/lmao/01u_cloudflare_trace_segments.md §LMAO Sink Fit: no new abstraction — the base
 * Tracer lifecycle (onTraceEnd queueing + flush) already models this. The drain tracer extends
 * ArrayQueueTracer; flush() drains completed root buffers, flattens them to rows, and hands them
 * to a constructor-injected transport.
 *
 * Transports (per 01u §Collection Lanes and Fallbacks):
 * - primary: a Pipelines-stream-binding-shaped send fn (`env.TRACE_STREAM.send(rows)`) — rows,
 *   because Pipelines owns batching and Parquet encoding for the raw tier
 * - fallback: Cloudflare Queues — batched TraceChunkEnvelope messages built from the existing
 *   lib/archive primitives (chunk_id idempotency); NEVER per-span messages (128 KB messages,
 *   per-64 KB op billing make per-span ~160x more expensive)
 *
 * Delivery is fire-and-forget: flush() is scheduled via ctx.waitUntil and never rejects; enqueue
 * failures surface through the onSendError hook (01s §Failure Handling — emit metrics, rely on
 * queue policy/backoff), they never block.
 */

import { buildTraceChunkEnvelope, type TraceChunkEnvelope } from '../archive/chunkEnvelope.js';
import type { OpContextBinding } from '../opContext/types.js';
import type { TracerOptions } from '../tracer.js';
import { ArrayQueueTracer } from '../tracers/ArrayQueueTracer.js';
import { spanBufferToTraceRows, type TraceRow } from './traceRows.js';

/** Shape of a Cloudflare Pipelines stream binding's send — promise resolves on confirmed ingest. */
export type PipelinesStreamSend = (rows: TraceRow[]) => Promise<void>;

/** Diagnostic-lane transport seam: delivers one drained batch of rows, at most best-effort. */
export interface DiagnosticTransport {
  send(rows: readonly TraceRow[]): Promise<void>;
}

/** Primary diagnostic transport: rows straight into the injected Pipelines stream binding. */
export class PipelinesStreamTransport implements DiagnosticTransport {
  constructor(private readonly sendToStream: PipelinesStreamSend) {}

  send(rows: readonly TraceRow[]): Promise<void> {
    // The binding signature takes a mutable array of records.
    return this.sendToStream([...rows]);
  }
}

/** One Queues-fallback message: chunk envelope (idempotency + routing metadata) plus its rows. */
export interface TraceChunkQueueMessage {
  readonly envelope: TraceChunkEnvelope;
  readonly rows: readonly TraceRow[];
}

export interface QueuesFallbackOptions {
  /** Logical file reference recorded in the envelopes, e.g. 'queue://worker-name'. */
  fileRef: string;
  /** Max rows per queue message — bounds message size under the 128 KB Queues limit. */
  maxRowsPerMessage: number;
  /** Injected queue producer, e.g. (msg) => env.TRACE_QUEUE.send(msg). */
  sendMessage: (message: TraceChunkQueueMessage) => Promise<void>;
}

function fnv1a32Hex(input: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}

function timestampBoundsMs(rows: readonly TraceRow[]): { startedAtMs: number; endedAtMs: number } {
  let minNs = Number.POSITIVE_INFINITY;
  let maxNs = Number.NEGATIVE_INFINITY;
  for (const row of rows) {
    const ts = row.timestamp_ns;
    if (typeof ts !== 'number') continue;
    if (ts < minNs) minNs = ts;
    if (ts > maxNs) maxNs = ts;
  }
  if (minNs === Number.POSITIVE_INFINITY) return { startedAtMs: 0, endedAtMs: 0 };
  return { startedAtMs: Math.floor(minNs / 1_000_000), endedAtMs: Math.floor(maxNs / 1_000_000) };
}

/**
 * Queues-fallback transport: rows are batched into chunk-envelope messages.
 *
 * The chunk_ref is a deterministic content hash of the batch, so a retried send of the same
 * batch produces the same chunk_id and the consumer's chunk_id dedup makes redelivery a no-op.
 */
export class QueuesFallbackTransport implements DiagnosticTransport {
  constructor(private readonly options: QueuesFallbackOptions) {
    if (options.maxRowsPerMessage < 1) {
      throw new Error('QueuesFallbackTransport: maxRowsPerMessage must be >= 1');
    }
  }

  async send(rows: readonly TraceRow[]): Promise<void> {
    const { fileRef, maxRowsPerMessage, sendMessage } = this.options;
    for (let offset = 0; offset < rows.length; offset += maxRowsPerMessage) {
      const batch = rows.slice(offset, offset + maxRowsPerMessage);
      const { startedAtMs, endedAtMs } = timestampBoundsMs(batch);
      const envelope = buildTraceChunkEnvelope({
        file_ref: fileRef,
        chunk_ref: `rows_${fnv1a32Hex(JSON.stringify(batch))}`,
        row_count: batch.length,
        started_at_ms: startedAtMs,
        ended_at_ms: endedAtMs,
      });
      await sendMessage({ envelope, rows: batch });
    }
  }
}

export interface DiagnosticDrainOptions<
  T extends import('../schema/LogSchema.js').LogSchema = import('../schema/LogSchema.js').LogSchema,
> extends TracerOptions<T> {
  transport: DiagnosticTransport;
  /** Failure hook (01s §Failure Handling): emit runtime metrics/alerts here; never rethrows. */
  onSendError?: (error: unknown, rows: readonly TraceRow[]) => void;
}

/**
 * Diagnostic-lane sink tracer: queue completed roots (ArrayQueueTracer), drain + send on flush().
 *
 * flush() is waitUntil-schedulable: it never rejects, and buffers are always released back to
 * the strategy regardless of transport outcome.
 */
export class DiagnosticDrainTracer<B extends OpContextBinding = OpContextBinding> extends ArrayQueueTracer<B> {
  private readonly transport: DiagnosticTransport;
  private readonly onSendError: ((error: unknown, rows: readonly TraceRow[]) => void) | undefined;

  constructor(binding: B, options: DiagnosticDrainOptions<B['logBinding']['logSchema']>) {
    super(binding, options);
    this.transport = options.transport;
    this.onSendError = options.onSendError;
  }

  override async flush(): Promise<void> {
    const buffers = this.drain();
    for (const buffer of buffers) {
      let rows: TraceRow[];
      try {
        rows = spanBufferToTraceRows(buffer);
      } finally {
        this.bufferStrategy.releaseBuffer(buffer);
      }
      try {
        await this.transport.send(rows);
      } catch (error) {
        this.onSendError?.(error, rows);
      }
    }
  }
}
