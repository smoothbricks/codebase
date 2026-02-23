import type { TraceChunkEnvelope } from './chunkEnvelope.js';
import type { PartitionSplit } from './chunkRouting.js';

export interface TraceChunkStats {
  readonly chunk_id: string;
  readonly row_count: number;
  readonly partition_count: number;
  readonly started_at_ms: number;
  readonly ended_at_ms: number;
  readonly duration_ms: number;
}

export function extractChunkStats<T>(
  envelope: TraceChunkEnvelope,
  partitions: readonly PartitionSplit<T>[] = [],
): TraceChunkStats {
  return {
    chunk_id: envelope.chunk_id,
    row_count: envelope.row_count,
    partition_count: partitions.length,
    started_at_ms: envelope.started_at_ms,
    ended_at_ms: envelope.ended_at_ms,
    duration_ms: Math.max(0, envelope.ended_at_ms - envelope.started_at_ms),
  };
}
