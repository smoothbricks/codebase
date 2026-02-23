# Trace Archive Primitives

## Overview

This spec defines LMAO library primitives for trace archival data-plane operations.

LMAO owns chunk construction, Arrow/Parquet conversion utilities, generic partition helpers, and deterministic
per-consumer cursor state.

## Ownership Boundary

### LMAO Owns (This Spec)

- Trace chunk envelope construction from Arrow tables
- Deterministic chunk identity helpers (`chunk_id`, schema hash, lineage hash)
- Generic partition-cardinality inspection and split helpers for tabular batches
- Compaction primitives for time-windowed Arrow/Parquet outputs
- Chunk statistics extraction helpers for downstream predicate pre-filtering


- Archive catalog/state model and listener registration
- Expression evaluation policy and wake strategy
- Multi-destination signal fan-out decisions
- Per-consumer cursor progression and replay policy

LMAO primitives in this spec do not parse, compile, or evaluate `ax` expressions.


## Primitive 1: Chunk Envelope Construction

```typescript
type TraceChunkEnvelope = {
  chunk_id: string; // deterministic idempotency identity
  emitted_at: string; // ISO timestamp
  format: 'arrow';
  codec: 'none' | 'gzip' | 'zstd';
  payload_bytes: Uint8Array; // Arrow IPC bytes
  metadata: {
    row_count: number;
    first_ts?: string;
    last_ts?: string;
    schema_hash: string;
    group_hints?: string[];
  };
};

interface TraceChunkBuilder {
  fromArrowTable(input: {
    table: unknown;
    emittedAt: Date;
    codec?: 'none' | 'gzip' | 'zstd';
    idSeed?: string;
  }): TraceChunkEnvelope;
}
```

`chunk_id` must be deterministic from envelope content identity (not random per retry).

## Primitive 2: Partition Shape Inspection

```typescript
type PartitionCardinality =
  | { kind: 'single'; partition: Record<string, string | number | bigint | null> }
  | { kind: 'mixed'; partition_count: number }
  | { kind: 'unknown'; reason: string };

interface ChunkPartitionInspector {
  inspectPartitionCardinality(input: {
    chunk: TraceChunkEnvelope;
    partitionColumns: readonly string[];
  }): PartitionCardinality;
}
```

This primitive does not derive AxDomainGroup or destination AxId. It only reports whether rows are single-partition,
mixed-partition, or unresolvable from provided columns.

## Primitive 3: Partition Split Helper

```typescript
type PartitionSlice = {
  partition: Record<string, string | number | bigint | null>;
  selector: {
    row_count: number;
    row_bitmap?: Uint8Array; // optional compact row selector
  };
  chunk_ref: {
    chunk_id: string;
    uri?: string;
  };
};

interface ChunkPartitioner {
  splitByPartition(input: { chunk: TraceChunkEnvelope; partitionColumns: readonly string[] }): PartitionSlice[];
}
```

Split output must be deterministic for the same input chunk and partition-column configuration.

## Primitive 4: Compaction

```typescript
type CompactionTargetFormat = 'arrow' | 'parquet';

type CompactionOutput = {
  output_chunk_id: string;
  output_format: CompactionTargetFormat;
  payload_bytes: Uint8Array;
  lineage: {
    source_chunk_ids: string[];
    source_set_hash: string;
    window: string; // e.g. '5m', '1h'
  };
  metadata: {
    row_count: number;
    schema_hash: string;
    first_ts?: string;
    last_ts?: string;
  };
};

interface TraceChunkCompactor {
  compact(input: {
    chunks: TraceChunkEnvelope[];
    window: string;
    targetFormat: CompactionTargetFormat;
  }): CompactionOutput;
}
```

Compaction must be append-only: source chunks remain immutable, output emits new chunk identity + lineage.

## Primitive 5: Chunk Stats Extraction

```typescript
type ChunkColumnStat = {
  column: string;
  null_count: number;
  min?: string | number | bigint;
  max?: string | number | bigint;
};

interface TraceChunkStats {
  summarize(input: { chunk: TraceChunkEnvelope; columns: readonly string[] }): ChunkColumnStat[];
}
```

These stats are optional optimization primitives for downstream expression pre-filtering.

## Determinism and Retry Invariants

- Same logical input produces same `chunk_id` and partition split output.
- Retrying ingest/compaction must not require random IDs.
- Compaction identity is stable by `(window, source_set_hash, targetFormat)`.
- Split helper never mutates input payload bytes.

## Op Integration Pattern


Example call chain:

1. Op receives queue envelope
2. Op builds chunk via `TraceChunkBuilder`
3. Op inspects/splits via `ChunkPartitionInspector` + `ChunkPartitioner`
4. Op compacts via `TraceChunkCompactor` on schedule
   fan-out and cursor behavior

## Related

- [Cloudflare Fetch Trace Wrapper](./01s_cloudflare_fetch_trace_wrapper.md)
- [Arrow Table Structure](./01f_arrow_table_structure.md)
- [Trace Logging System](./01_trace_logging_system.md)
