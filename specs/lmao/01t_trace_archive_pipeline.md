# Trace Archive Primitives <a id="smoo/lmao!n/trace-archive"></a>

## Overview <a id="smoo/lmao!n/trace-archive-overview"></a>

This spec defines LMAO library primitives for trace archival data-plane operations.

LMAO owns chunk construction, Arrow/Parquet conversion utilities, generic partition helpers, and deterministic
per-consumer cursor state. The consuming system owns processor lifecycle, listener registration, fan-out decisions,
routing-destination derivation, and control-plane policy.

## Ownership Boundary <a id="smoo/lmao!n/trace-archive-ownership"></a>

### LMAO Owns (This Spec) <a id="smoo/lmao!n/trace-archive-ownership.lmao-owns"></a>

- Trace chunk envelope construction for persisted chunks
- Deterministic chunk identity helpers (`chunk_id`, schema hash, lineage hash)
- Generic partition-cardinality inspection and split helpers for tabular batches
- Compaction primitives for time-windowed Arrow/Parquet outputs
- Chunk-level statistics rollups for downstream predicate pre-filtering

### The Consuming System Owns (Out of Scope Here) <a id="smoo/lmao!n/trace-archive-ownership.consumer-owns"></a>

- Archive catalog/state model and listener registration
- Predicate evaluation policy and scheduling strategy
- Multi-destination delivery fan-out decisions
- Per-consumer cursor progression and replay policy

LMAO primitives in this spec do not parse, compile, or evaluate consumer predicate expressions.

## Primitive 1: Chunk Envelope Construction <a id="smoo/lmao!n/trace-archive-envelope"></a>

```typescript
type TraceChunkEnvelopeInput = {
  file_ref: string; // the object that holds the chunk's bytes
  chunk_ref: string; // the chunk within that object
  row_count: number;
  started_at_ms: number;
  ended_at_ms: number;
  partition_keys?: readonly string[];
  metadata?: Record<string, unknown>;
};

type TraceChunkEnvelope = {
  chunk_id: string; // deterministic idempotency identity
  file_ref: string;
  chunk_ref: string;
  row_count: number;
  started_at_ms: number;
  ended_at_ms: number;
  partition_keys: readonly string[]; // sorted
  metadata?: Record<string, unknown>;
};

function buildTraceChunkEnvelope(input: TraceChunkEnvelopeInput): TraceChunkEnvelope;
```

The envelope describes a chunk by reference: the chunk's bytes live in the object that `file_ref` names, at `chunk_ref`
within it, and the envelope carries no payload.

`chunk_id` must be deterministic from envelope content identity (not random per retry), computed as FNV-1a over a
stable-serialized canonical body of the input fields, with `partition_keys` sorted. The caller derives `file_ref` or
`chunk_ref` from the chunk's content, so a retry of the same content yields the same `chunk_id`.

## Primitive 2: Partition Shape Inspection <a id="smoo/lmao!n/trace-archive-partition-inspect"></a>

```typescript
type PartitionCardinality = 'single' | 'mixed' | 'unknown';

function inspectPartitionCardinality<T>(
  rows: readonly T[],
  partitionOf: (row: T) => string | null | undefined
): PartitionCardinality;
```

This primitive does not derive routing groups or delivery destinations for the consuming system. It only reports whether
rows are single-partition, mixed-partition, or unresolvable through `partitionOf`. The caller decodes the rows from the
object that the chunk's `file_ref` names.

No rows, or an empty or missing partition value, yields `unknown`. The scan stops at the second distinct partition.

## Primitive 3: Partition Split Helper <a id="smoo/lmao!n/trace-archive-partition-split"></a>

```typescript
type PartitionSplit<T> = {
  partition: string;
  selector: string; // `${partitionField} == ${JSON.stringify(partition)}`
  row_indexes: readonly number[]; // ascending indexes into the input rows
  rows: readonly T[];
};

function splitChunkByPartition<T>(
  rows: readonly T[],
  partitionOf: (row: T) => string | null | undefined,
  partitionField?: string // default 'group'
): readonly PartitionSplit<T>[];
```

Split output must be deterministic for the same input rows and partition function.

Partitions are emitted in sorted partition-key order. A row with an empty or missing partition value is in no split.

The split takes rows, not an envelope, so a split carries no parent identity: the caller records the parent envelope's
`chunk_id` in each split chunk's lineage, and a split chunk's identity derives from `(parent chunk_id, partition)`
([01u §Metering Boundary](./01u_cloudflare_trace_segments.md#smoo/lmao!n/cf-trace-segments-billing-boundary)).

## Primitive 4: Compaction <a id="smoo/lmao!n/trace-archive-compaction"></a>

```typescript
type CompactionTargetFormat = 'arrow' | 'parquet';

type CompactionWindow = {
  started_at_ms: number;
  ended_at_ms: number;
};

type CompactionOutput = {
  output_chunk_id: string;
  output_format: CompactionTargetFormat;
  payload_bytes: Uint8Array;
  lineage: {
    source_chunk_ids: string[];
    source_set_hash: string;
    window: CompactionWindow;
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
    sources: readonly { envelope: TraceChunkEnvelope; payload_bytes: Uint8Array }[];
    window: CompactionWindow;
    targetFormat: CompactionTargetFormat;
  }): CompactionOutput;
}
```

Each source pairs an envelope with the Arrow IPC bytes the caller read from the object that its `file_ref` names.

Compaction must be append-only: source chunks remain immutable, output emits new chunk identity + lineage.

## Primitive 5: Chunk Stats Extraction <a id="smoo/lmao!n/trace-archive-stats"></a>

```typescript
type TraceChunkStats = {
  chunk_id: string;
  row_count: number;
  partition_count: number;
  started_at_ms: number;
  ended_at_ms: number;
  duration_ms: number; // max(0, ended_at_ms - started_at_ms)
};

function extractChunkStats<T>(envelope: TraceChunkEnvelope, partitions?: readonly PartitionSplit<T>[]): TraceChunkStats;
```

These chunk-level rollups are optional optimization primitives for downstream predicate pre-filtering. `partition_count`
is the number of splits passed in, and zero for an unsplit chunk. Per-column statistics are not a chunk primitive: the
compacted segments carry them as column statistics and zone maps
([01u §Segment Layout](./01u_cloudflare_trace_segments.md#smoo/lmao!n/cf-trace-segments-layout)).

## Determinism and Retry Invariants <a id="smoo/lmao!n/trace-archive-determinism"></a>

- Same logical input produces same `chunk_id` and partition split output.
- Retrying ingest/compaction must not require random IDs.
- Compaction identity is stable by `(window, source_set_hash, targetFormat)`.
- Split helper never mutates its input rows.

## Op Integration Pattern <a id="smoo/lmao!n/trace-archive-op-integration"></a>

These primitives are designed to be called from the consuming system's archival jobs. Example call chain:

1. Job receives a queue message (01s `TraceChunkQueueMessage`: one envelope plus its rows)
2. Job persists the rows as an immutable chunk and builds its envelope via `buildTraceChunkEnvelope`
3. Job inspects/splits the rows via `inspectPartitionCardinality` + `splitChunkByPartition`
4. Job compacts via `TraceChunkCompactor` on schedule
5. The consuming system's archive processor maps partition values to routing destinations using its route configuration
   or predicate expressions, then decides fan-out and cursor behavior

## Related <a id="smoo/lmao!n/trace-archive.related"></a>

- [Cloudflare Fetch Trace Wrapper](./01s_cloudflare_fetch_trace_wrapper.md)
- [Arrow Table Structure](./01f_arrow_table_structure.md)
- [Trace Logging System](./01_trace_logging_system.md)
