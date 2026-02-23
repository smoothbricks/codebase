import type { TraceChunkEnvelope } from './chunkEnvelope.js';

export interface CompactionWindow {
  readonly started_at_ms: number;
  readonly ended_at_ms: number;
}

export interface CompactedChunkSet {
  readonly compaction_id: string;
  readonly source_set_hash: string;
  readonly source_chunk_ids: readonly string[];
  readonly source_chunk_count: number;
  readonly total_rows: number;
  readonly window: CompactionWindow;
}

export function compactTraceChunks(chunks: readonly TraceChunkEnvelope[], window: CompactionWindow): CompactedChunkSet {
  const sourceChunkIds = [...new Set(chunks.map((chunk) => chunk.chunk_id))].sort();
  const totalRows = chunks.reduce((sum, chunk) => sum + chunk.row_count, 0);
  const source_set_hash = hashDeterministic(sourceChunkIds.join('|'));
  const compaction_id = `compact_${hashDeterministic(`${source_set_hash}:${window.started_at_ms}:${window.ended_at_ms}`)}`;

  return {
    compaction_id,
    source_set_hash,
    source_chunk_ids: sourceChunkIds,
    source_chunk_count: sourceChunkIds.length,
    total_rows: totalRows,
    window,
  };
}

function hashDeterministic(input: string): string {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const mod = 0xffffffffffffffffn;

  for (let i = 0; i < input.length; i++) {
    hash ^= BigInt(input.charCodeAt(i));
    hash = (hash * prime) & mod;
  }

  return hash.toString(16).padStart(16, '0');
}
