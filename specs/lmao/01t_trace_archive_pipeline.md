# Trace Archive Primitives <a id="smoo/lmao!n/trace-archive"></a>

## Overview <a id="smoo/lmao!n/trace-archive-overview"></a>

This spec defines LMAO library primitives for trace archival data-plane operations.

LMAO owns chunk construction, Arrow/Parquet conversion utilities, generic partition helpers, and deterministic
per-consumer cursor state. The consuming system owns processor lifecycle, listener registration, fan-out decisions,
routing-destination derivation, and control-plane policy.

## Ownership Boundary <a id="smoo/lmao!n/trace-archive-ownership"></a>

### LMAO Owns (This Spec) <a id="smoo/lmao!n/trace-archive-ownership.lmao-owns"></a>

- Trace chunk envelope construction from Arrow tables
- Deterministic chunk identity helpers (`chunk_id`, schema hash, lineage hash)
- Generic partition-cardinality inspection and split helpers for tabular batches
- Compaction primitives for time-windowed Arrow/Parquet outputs
- Chunk statistics extraction helpers for downstream predicate pre-filtering

### The Consuming System Owns (Out of Scope Here) <a id="smoo/lmao!n/trace-archive-ownership.consumer-owns"></a>

- Archive catalog/state model and listener registration
- Predicate evaluation policy and scheduling strategy
- Multi-destination delivery fan-out decisions
- Per-consumer cursor progression and replay policy

LMAO primitives in this spec do not parse, compile, or evaluate consumer predicate expressions.

## Primitive 1: Chunk Envelope Construction <a id="smoo/lmao!n/trace-archive-envelope"></a>

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

> **Implementation status (shipped, leaner contract).** `packages/lmao/src/lib/archive/chunkEnvelope.ts` ships
> `buildTraceChunkEnvelope(input)` — the deterministic `chunk_id` (FNV-1a over a stable-serialized canonical body) is
> implemented as specified. The shipped envelope addresses an already-flushed chunk **by reference** (`file_ref` +
> `chunk_ref`) rather than carrying inline `payload_bytes` / `codec` / `format`, and its time bounds are numeric
> `started_at_ms` / `ended_at_ms` (not ISO `emitted_at` / `first_ts`). The inline-Arrow-payload envelope
> (`payload_bytes` Arrow IPC, `codec`, `schema_hash`, `group_hints`) below is the future shape — staged as a `smoo/lmao`
> node, not yet built.

## Primitive 2: Partition Shape Inspection <a id="smoo/lmao!n/trace-archive-partition-inspect"></a>

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

This primitive does not derive routing groups or delivery destinations for the consuming system. It only reports whether
rows are single-partition, mixed-partition, or unresolvable from provided columns.

> **Implementation status (shipped).** `packages/lmao/src/lib/archive/chunkRouting.ts:inspectPartitionCardinality`
> reports `'single' | 'mixed' | 'unknown'` over rows + a `partitionOf` accessor, exactly as specified (an empty/missing
> partition value yields `'unknown'`). It derives no routing destinations.

## Primitive 3: Partition Split Helper <a id="smoo/lmao!n/trace-archive-partition-split"></a>

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

> **Implementation status (shipped, leaner contract).**
> `packages/lmao/src/lib/archive/chunkRouting.ts:splitChunkByPartition` splits rows deterministically (sorted partition
> keys) and emits, per partition, the row indexes, the materialized rows, and a stable string `selector`
> (`group == "value"`). It returns the rows directly rather than a `row_bitmap` + `chunk_ref` indirection; the
> compact-bitmap selector below is a future optimization, not yet built.

## Primitive 4: Compaction <a id="smoo/lmao!n/trace-archive-compaction"></a>

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

> **Implementation status (shipped, identity-only).**
> `packages/lmao/src/lib/archive/chunkCompaction.ts:compactTraceChunks` ships the append-only **identity + lineage**
> half: a deterministic `compaction_id` and `source_set_hash` stable by `(window, sorted source_chunk_ids)`, plus
> `total_rows`. It does **not** yet read source payloads, re-encode to Arrow/Parquet, or emit `payload_bytes` — i.e. it
> computes the compaction manifest, not the compacted bytes. The payload-producing compaction (`targetFormat` →
> `payload_bytes`) below is staged as a `smoo/lmao` node, not yet built.

## Primitive 5: Chunk Stats Extraction <a id="smoo/lmao!n/trace-archive-stats"></a>

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

These stats are optional optimization primitives for downstream predicate pre-filtering.

> **Implementation status (shipped, chunk-level).** `packages/lmao/src/lib/archive/chunkStats.ts:extractChunkStats`
> ships **chunk-level** rollups — `chunk_id`, `row_count`, `partition_count`, time bounds, and `duration_ms`. The
> **per-column** min/max/null_count stats below (for predicate pre-filtering) are not yet built; they are staged as a
> `smoo/lmao` node. Both are optional optimization primitives.

## Determinism and Retry Invariants <a id="smoo/lmao!n/trace-archive-determinism"></a>

- Same logical input produces same `chunk_id` and partition split output.
- Retrying ingest/compaction must not require random IDs.
- Compaction identity is stable by `(window, source_set_hash, targetFormat)`.
- Split helper never mutates input payload bytes.

## Op Integration Pattern <a id="smoo/lmao!n/trace-archive-op-integration"></a>

These primitives are designed to be called from the consuming system's archival jobs. Example call chain:

1. Job receives queue envelope
2. Job builds chunk via `TraceChunkBuilder`
3. Job inspects/splits via `ChunkPartitionInspector` + `ChunkPartitioner`
4. Job compacts via `TraceChunkCompactor` on schedule
5. The consuming system's archive processor maps partition values to routing destinations using its route configuration
   or predicate expressions, then decides fan-out and cursor behavior

## Related <a id="smoo/lmao!n/trace-archive.related"></a>

- [Cloudflare Fetch Trace Wrapper](./01s_cloudflare_fetch_trace_wrapper.md)
- [Arrow Table Structure](./01f_arrow_table_structure.md)
- [Trace Logging System](./01_trace_logging_system.md)
