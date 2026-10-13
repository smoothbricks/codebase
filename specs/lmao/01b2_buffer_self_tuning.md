# Buffer Self-Tuning <a id="smoo/lmao!n/buffer-tuning"></a>

> **📚 PART OF COLUMNAR BUFFER ARCHITECTURE**
>
> This document details the zero-config memory management that makes columnar buffers "just work" in any environment.
> Read the [main overview](./01b_columnar_buffer_architecture.md) first.

## WHY: Developers Shouldn't Configure Memory

Traditional logging requires configuration:

- Buffer sizes
- Flush intervals
- Memory limits
- Growth strategies

This is wrong. The system should adapt automatically.

**Key Insight**: Self-tuning is part of the trace logging system, not a general-purpose buffer library. The tuning
mechanism has clear metrics (overflow rate, utilization) from the specific use case of span logging. Buffer chaining
handles overflow gracefully while the system learns optimal capacity per span buffer class. The op's internal wrapper
creates SpanBuffers; ops with the same physical layout share one generated class and its stats.

### Self-Tuning Benefits

1. **Zero Configuration** - Works out of the box
2. **Adaptive Performance** - Adjusts to workload
3. **Measured Memory Safety** - Enforces effective-memory limits and observable backpressure; arbitrary workloads can
   still exhaust process memory
4. **Optimal Throughput** - Balances memory vs performance
5. **Environment Agnostic** - Same code everywhere
6. **Per-Span Buffers** - Each span gets its own buffer (created by op wrapper), avoiding trace_id/span_id TypedArrays
7. **Buffer Chaining** - Graceful overflow handling while learning optimal capacity
8. **Freelist Pooling** - Pooling buffers for GC optimization. The WASM strategy (`bufferStrategy.ts`,
   `wasmSpanBuffer.ts`) returns blocks to a freelist; the JS strategy relies on V8 GC instead.
9. **Lazy-to-Eager Column Promotion** - Frequently-used columns automatically pre-allocate (see
   [Column Promotion](#lazy-to-eager-column-promotion))

## Self-Tuning Architecture <a id="smoo/lmao!n/buffer-tuning-capacity"></a>

**Utilization ratchet.** `checkCapacityTuning` calls `shouldTuneCapacity` (`packages/lmao/src/lib/capacityTuning.ts`),
which adjusts capacity from one utilization signal: `utilization = totalWrites / (spansCreated × (capacity − 2))`.
Capacity doubles when utilization exceeds `GROW_THRESHOLD = 1.5` and halves when it falls below
`SHRINK_THRESHOLD = 0.5`, bounded by `MIN_CAPACITY = 8` and `MAX_CAPACITY = 1024`. Tuning requires at least
`MIN_SPANS_FOR_TUNING = 10` spans, and stats reset after each adjustment. The ratchet is the whole capacity tuner: it
does not predict growth from write-rate history, classify workloads, compact live buffers, or coordinate capacity across
buffer classes. Memory-pressure and idle response are `FlushScheduler` concerns
([§FlushScheduler Design](#smoo/lmao!n/buffer-tuning-flush)), not capacity-tuner concerns.

### Performance contract

The tuner is the utilization ratchet above. The only other self-tuning mechanism is usage-driven lazy-to-eager column
promotion ([§Lazy-to-Eager Column Promotion](#smoo/lmao!n/buffer-tuning-promotion)).

Tuning decisions MUST use **effective memory**: JS heap plus external/backing-store bytes, committed WASM pages
(including free/orphaned blocks), Arrow-owned output, source bytes pinned by active `ArrowLease`s, temporary
dictionary/UTF-8/null buffers, and cache/allocator overhead. Shared startup `PhysicalLayoutPlan` and `VocabularyBinding`
state is reported separately and may be amortized only with a published divisor.

The warmed fixed-capacity write allocates zero new heap memory. Capacity adjustment, first lazy-column write,
overflow-chain creation, statistics emission, `memory.grow()`, and flush are slow/cold paths. Tuning MUST NOT add
shape-changing properties or polymorphic access to generated buffers. The common capacity check is one predictable
not-overflow branch; overflow frequency and latency are reported separately.

## Flush Strategy <a id="smoo/lmao!n/buffer-tuning-flush"></a>

### FlushScheduler Design

The `FlushScheduler` manages background flushing of registered root buffers (one per HTTP request/trace) to Arrow
tables.

**Key Design Decisions**:

1. **Set-Based Buffer Collection**: Root buffers are added to a `Set<AnySpanBuffer>` with `register(buffer)`.
   Registering a root twice keeps one entry.

2. **Removal After a Successful Handler**: A flush snapshots the set, converts the snapshot, and awaits the handler.
   Only when the handler resolves does the scheduler reset each flushed root (`_writeIndex = 0`, overflow chain
   unlinked) and delete it from the set. A failed handler leaves every root registered and unreset, so the next flush
   converts the same rows again and nothing is lost. Roots registered while the handler runs stay for the next flush. A
   flushed root must be registered again before it is flushed again.

3. **One Record Batch Per Request**: Each root's span tree converts to its own table with its own dictionaries
   (`convertSpanTreeToLeasedArrowTable`). The scheduler concatenates the per-root batches column by column into one
   `Table` for the handler: one table per flush, one record batch per request.

4. **Columns per Root**: A root's conversion merges the schema fields of every span in its tree
   ([01k Pass 0](./01k_tree_walker_and_arrow_conversion.md)), so roots in one flush may carry different column sets. The
   combined table has the union of the roots' columns, and a batch without a column contributes nulls for it.
   Capacity-stats rows appear once per stats-logging flush, not once per root.

**Ownership.** Each registered root buffer is one request/trace. The scheduler converts each registered root's span tree
with `convertSpanTreeToLeasedArrowTable`, so it holds one logical request/trace flush measurement and one `ArrowLease`
per request result, and releases every lease once the handler settles. Transport or Arrow-native chunks may be
coalesced, but request boundaries remain visible for semantic checksums, p50/p95/p99/p99.9, effective-memory
attribution, cancellation, and release.

Before conversion the trace is frozen. Contiguous primitive/null regions of JS-heap buffers may be borrowed; WASM
columns are always copied into Arrow-owned buffers. Overflow remains chunked when supported, otherwise one explicit
Arrow-owned concatenation copy is counted. UTF-8, dictionary, synthesized-null, and concatenated buffers are
Arrow-owned. Mutable source buffers cannot reset, pool, or recycle while a lease is active. Asynchronous transport owns
the lease until consumption completes.

**Flush Cycle**:

```typescript
class FlushScheduler {
  private buffers = new Set<AnySpanBuffer>();

  register(buffer: AnySpanBuffer): void {
    this.buffers.add(buffer);
    // Start scheduler if not running
  }

  private async doFlush(reason: FlushMetadata['flushReason']): Promise<void> {
    if (this.buffers.size === 0) return;
    const buffersToFlush = Array.from(this.buffers);

    // One leased table (one record batch with its own dictionaries) per root buffer, i.e. per request;
    // capacity-stats rows ride on the first root only
    const leases = buffersToFlush.map((root, i) =>
      convertSpanTreeToLeasedArrowTable(root, i === 0 ? statsToLog : undefined, periodStartNs)
    );
    try {
      const [first, ...rest] = leases.map((lease) => lease.table);
      await this.handler(mergeTables(first, rest), metadata); // union of columns, nulls where a batch lacks one
      for (const root of buffersToFlush) {
        root._writeIndex = 0; // reset the root and unlink its overflow chain
        root._overflow = undefined;
        this.buffers.delete(root); // removed only after the handler succeeded
      }
    } catch (error) {
      // Handler failed: roots stay registered and unreset; the next flush converts them again
    } finally {
      for (const lease of leases) lease.release();
    }
  }
}
```

### Adaptive Flushing

Balance latency vs memory usage. Thresholds are configurable via `FlushSchedulerConfig`:

```typescript
interface FlushSchedulerConfig {
  /** Flush when buffer reaches this capacity ratio (default: 0.8) */
  capacityThreshold: number;
  /** Maximum interval between flushes in ms (default: 10000) */
  maxIntervalMs: number;
  /** Minimum interval between flushes in ms (default: 1000) */
  minIntervalMs: number;
  /** Flush after this many ms of inactivity (default: 5000) */
  idleTimeoutMs: number;
}

const DEFAULT_CONFIG: FlushSchedulerConfig = {
  capacityThreshold: 0.8,
  maxIntervalMs: 10000,
  minIntervalMs: 1000,
  idleTimeoutMs: 5000,
};

class FlushScheduler {
  private config: FlushSchedulerConfig;
  private flushTimer?: NodeJS.Timer;
  private lastFlushTime = Date.now();
  private flushInterval: number;

  constructor(config: Partial<FlushSchedulerConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.flushInterval = this.config.minIntervalMs;
  }

  private checkFlush() {
    const reasons = this.getFlushReasons();

    if (reasons.length > 0) {
      this.flush(reasons);
      this.adaptFlushInterval(reasons);
    }
  }

  private getFlushReasons(): string[] {
    const reasons: string[] = [];
    const now = Date.now();

    // Size-based flush
    if (this._writeIndex > this._capacity * this.config.capacityThreshold) {
      reasons.push('capacity');
    }

    // Time-based flush
    if (now - this.lastFlushTime > this.flushInterval) {
      reasons.push('interval');
    }

    // Memory pressure flush
    if (this.isMemoryPressure()) {
      reasons.push('memory');
    }

    // Idle flush (no writes for a while)
    if (this._writeIndex > 0 && now - this.lastWriteTime > this.config.idleTimeoutMs) {
      reasons.push('idle');
    }

    return reasons;
  }

  private adaptFlushInterval(reasons: string[]) {
    // Frequent capacity flushes = need larger buffer or faster flushing
    if (reasons.includes('capacity')) {
      this.flushInterval = Math.max(this.config.minIntervalMs, this.flushInterval * 0.8);
    }

    // Idle flushes = can flush less frequently
    else if (reasons.includes('idle')) {
      this.flushInterval = Math.min(this.config.maxIntervalMs, this.flushInterval * 1.2);
    }
  }
}
```

**Note**: These thresholds are sensible defaults that work for most workloads. Configuration is available for advanced
use cases (e.g., low-latency requirements, memory-constrained environments) but is not required for normal operation.

### Memory Pressure Response

The response to memory pressure is an early flush. When `memoryPressureDetection` is on (the default) and `process`
exists, `FlushScheduler` reads `process.memoryUsage()` every 2 seconds and flushes with reason `'memory'` once the heap
headroom `heapTotal - heapUsed` falls below `memoryPressureThreshold` (default 100 MiB). The flush obeys
`minFlushInterval` like every other non-manual flush. A successful flush resets the flushed root buffers and unlinks
their overflow chains; live buffers are never compacted or resized in place.

## Capacity Constraints <a id="smoo/lmao!n/buffer-tuning-constraints"></a>

### Multiple of 8 Requirement <a id="smoo/lmao!n/buffer-tuning-multiple-of-8"></a>

**All buffer capacities MUST be multiples of 8.**

`getAlignedCapacity` rounds up to a multiple of 8 with `Math.ceil(elementCount / 8) * 8`
(`packages/arrow-builder/src/lib/buffer/bufferHelpers.ts`, fenced
[`smoo/lmao!n/buffer-perf-cache-alignment`](./01b1_buffer_performance_optimizations.md#smoo/lmao!n/buffer-perf-cache-alignment))
inside the generated constructor. `createOverflowBuffer` re-applies it
([01b5](./01b5_spanbuffer_memory_layout.md#smoo/lmao!n/spanbuffer-layout.create-overflow)). Self-tuning capacities are
powers of two (8…1024), so the constraint is naturally satisfied. This constraint enables efficient null bitmap
operations:

1. **Byte-aligned concatenation**: When converting multiple buffers to Arrow, null bitmaps can be bulk-copied with
   `TypedArray.set()` instead of bit-by-bit loops
2. **Efficient clearing**: Clearing null bits for a buffer's rows uses `TypedArray.fill(0)` for full bytes
3. **Simple offset math**: `rowOffset / 8` gives exact byte offset, no bit shifting needed at boundaries

The constraint is enforced in the generated constructor that `createSpanBuffer()` and `createOverflowBuffer()` both
construct through:

```typescript
// Align capacity to multiple of 8
const alignedCapacity = (requestedCapacity + 7) & ~7;
```

Since self-tuning uses powers of 2 (8, 16, 32, 64, 128, 256, 512, 1024), this constraint is naturally satisfied. The
explicit alignment ensures correctness for any custom capacity input.

## Initial Capacity Selection

Every generated span buffer class starts its `SpanBufferStats.capacity` at `DEFAULT_BUFFER_CAPACITY = 8`
(`packages/arrow-builder/src/lib/buffer/types.ts`), the ratchet's minimum. The utilization ratchet moves it from there;
no environment probe picks a different starting capacity.

## Performance Monitoring

### Benchmark acceptance gates

Capacity/tuning Mitata scenarios MUST follow
[01b1 §Required Mitata Protocol](./01b1_buffer_performance_optimizations.md#smoo/lmao!n/buffer-perf-benchmark-protocol):
plugin-off, plugin-on/current, and candidates use identical semantic checksums, position-balanced ordering,
machine-readable raw samples, and p50/p95/p99/p99.9 for startup, span setup, warmed writes, overflow/adjustment, and
complete per-request Arrow flush. Allocation, GC, branch/IC, and effective-memory observations are auxiliary
instrumentation alongside Mitata and may be unavailable. Reject warmed fixed-capacity allocations, post-warmup
hidden-class transitions or polymorphic hot ICs, an unstable common capacity branch, unbounded lease queues, semantic
differences, or regressions beyond the predeclared noise threshold.

## Lazy-to-Eager Column Promotion <a id="smoo/lmao!n/buffer-tuning-promotion"></a>

### Overview

Beyond capacity self-tuning, the system also adapts column access patterns. Columns start as lazy (memory-efficient) and
automatically promote to eager (performance-optimized) when heavily used.

Promotion has a static half and a runtime half. The static half is arrow-builder's codegen honouring a per-column
`__eager` flag (`columnBufferGenerator.ts` `getTypedArrayInfo` / `generateColumnBufferClass`, fenced
[`smoo/lmao!n/buffer-arch-eager-vs-lazy`](./01b_columnar_buffer_architecture.md#smoo/lmao!n/buffer-arch-eager-vs-lazy)),
with the schema builder marking `number`/`boolean`/`bigUint64` columns eager by type. The runtime half is the
usage-driven ratchet below: `LazyColumnStats` tracks per-column `instantiationCount`, and `promoteColumnsToEager`
performs the 100-sample / 80%-ratio recompilation.

**Key Insight**: Promotion happens during background flush (cold path), ensuring zero hot-path impact while optimizing
for actual usage patterns.

**Ownership**: This is LMAO's concern, not arrow-builder's. Arrow-builder provides the codegen infrastructure to support
both eager and lazy columns, but LMAO owns the tracking, promotion logic, and class recompilation.

### Stats Object Design

Stats are tracked via a single `stats` object passed to `generateColumnBufferClass()` and captured in the generated
class closure. This design minimizes closure size while enabling direct property access.

```typescript
// Stats object shape - one per schema, created at module initialization
// Uses fixed shape (not a Map) for V8 hidden class optimization
interface LazyColumnStats {
  _totalSpanBuffersCreated: number; // Prefixed with _ to avoid collision with user column names
  userId: { instantiationCount: number }; // One property per lazy column
  requestId: { instantiationCount: number };
  // ... one property per lazy column in schema
}
```

**Why this design:**

1. **Single closure reference**: The generated class captures one `stats` object, not N per-column references. The
   closure already contains only `helpers` + extension dependencies - keeping it minimal matters for V8.

2. **Direct property access**: `stats.userId.instantiationCount++` is a direct property access on a fixed-shape object,
   V8 hidden class optimized.

3. **No Map lookups**: Fixed object shape means no `Map.get()` in hot path.

4. **Underscore prefix**: `_totalSpanBuffersCreated` prevents collision with user column names like
   `totalSpanBuffersCreated`.

### Stats Injection via Closure

The stats object is passed to `generateColumnBufferClass()` via `extension.dependencies` and captured in the generated
class closure:

```typescript
// At module initialization (cold path)
const stats: LazyColumnStats = {
  _totalSpanBuffersCreated: 0,
  userId: { instantiationCount: 0 },
  requestId: { instantiationCount: 0 },
};

// Pass to class generator
const SpanBufferClass = getColumnBufferClass(schema, {
  dependencies: { stats },
  // ... other extension options
});
```

The generated constructor increments `stats._totalSpanBuffersCreated++`, and each lazy getter increments
`stats.{columnName}.instantiationCount++` on first allocation.

### Generated Code

```typescript
// Generated constructor (stats captured in closure)
constructor(requestedCapacity) {
  // ... system columns ...
  stats._totalSpanBuffersCreated++;
  // ... rest of initialization ...
}

// Generated lazy getter (stats captured in closure)
get userId_nulls() {
  let v = this._userId_nulls;
  if (v === undefined) {
    stats.userId.instantiationCount++;  // Direct property access, no lookup
    // ... allocation code ...
  }
  return v;
}
```

**Hot path cost**: One integer increment in constructor, one integer increment on first allocation of each lazy column.
Both are direct property access on the closure-captured `stats` object.

### Promotion Ratio Calculation

To decide if a column should promote to eager:

```typescript
const ratio = stats.userId.instantiationCount / stats._totalSpanBuffersCreated;
if (ratio >= 0.8 && stats._totalSpanBuffersCreated >= 100) {
  // Promote userId to eager
}
```

The `>= 100` sample threshold prevents premature promotion based on small sample sizes.

### Promotion Criteria

A lazy column is promoted to eager when **ALL** of the following are true:

1. **Statistical Significance**: `_totalSpanBuffersCreated >= 100`
   - Ensures we have enough data to make a confident decision
   - Prevents premature promotion on small samples

2. **High Usage Ratio**: `stats[column].instantiationCount / stats._totalSpanBuffersCreated >= 0.80`
   - Column is used in 80% or more of span buffers
   - Indicates the getter overhead outweighs memory savings

3. **Still Lazy**: the column has an entry in the class's `LazyColumnStats` (eager columns have none)
   - Prevents redundant recompilation

### Recompilation via `new Function()`

When promotion criteria are met, the system recompiles the SpanBuffer class with promoted columns as eager. The new
`stats` object is passed via `extension.dependencies`, and the generated class captures it in its closure.
`promoteColumnsToEager(SpanBufferClass)` returns the names it promoted, in schema order, and an empty array when no
column qualifies.

```typescript
// Find columns to promote based on stats
const eagerColumns: string[] = [];
for (const columnName of schemaFields) {
  const ratio = stats[columnName].instantiationCount / stats._totalSpanBuffersCreated;
  if (ratio >= 0.8) {
    eagerColumns.push(columnName);
  }
}

// Create fresh stats for new class (reset counters)
const newStats: LazyColumnStats = {
  _totalSpanBuffersCreated: 0,
  // ... one property per remaining lazy column
};

// Generate new class with promoted columns eager, new stats in closure
const newSpanBufferClass = getColumnBufferClass(schema, {
  eagerColumns,
  dependencies: { stats: newStats },
});

// Atomic replacement: later span buffers of this schema are created from newSpanBufferClass,
// and newSpanBufferClass.lazyColumnStats is newStats
// promoteColumnsToEager returns eagerColumns
```

The new class is generated with promoted columns as eager properties (allocated in constructor) rather than lazy
getters.

### Arrow-Builder Codegen

The `generateColumnBufferClass()` function already supports eager vs lazy columns via the schema's `__eager` flag. When
a column is marked eager, it's allocated in the constructor. When lazy, it uses a getter that allocates on first access.

For promotion tracking, the generated code references `stats` from the closure:

```typescript
// Generated constructor
constructor(requestedCapacity) {
  // ... system columns ...
  stats._totalSpanBuffersCreated++;  // stats from closure
  // ... lazy column initialization (this._col_nulls = undefined) ...
}

// Generated lazy getter
get userId_nulls() {
  let v = this._userId_nulls;
  if (v === undefined) {
    stats.userId.instantiationCount++;  // stats from closure
    const cap = this._alignedCapacity;
    // ... allocation code ...
  }
  return v;
}
```

The `stats` object is passed via `extension.dependencies` and becomes available in the generated class closure.

### In-Flight Buffer Handling

The system handles the transition gracefully:

- **Old buffers** continue using the original class with the original lazy/eager pattern
- **New buffers** instantiate from the recompiled class with promoted columns as eager
- **Natural convergence** occurs as old buffers flush and new ones are created

No migration or data copying is needed. The system simply creates new SpanBuffer instances using the updated class.

### Performance Trade-offs

| Aspect             | Before (Lazy)           | After (Eager)          | Impact                 |
| ------------------ | ----------------------- | ---------------------- | ---------------------- |
| Memory per buffer  | Optimal (no allocation) | +256-512 bytes         | Minimal for 80%+ usage |
| Hot path access    | Getter overhead         | Direct property access | Faster: no getter call |
| Best for           | <80% column usage       | ≥80% column usage      | Auto-optimizes         |
| Recompilation cost | N/A                     | One-time, cold path    | Zero hot-path impact   |

### Example: Promotion in Action

```typescript
// Initial state: All columns lazy
const schema = defineLogSchema({
  userId: S.category(),
  requestId: S.category(),
  sessionId: S.category(),
  experimentId: S.category(),
});
const SpanBufferClass = getSpanBufferClass(schema);

// After 100 span buffers:
// - userId: accessed in 95 buffers (95% ratio) → Promotes to eager
// - requestId: accessed in 90 buffers (90% ratio) → Promotes to eager
// - sessionId: accessed in 45 buffers (45% ratio) → Stays lazy
// - experimentId: accessed in 10 buffers (10% ratio) → Stays lazy

// Promotion check (runs automatically during background flush)
promoteColumnsToEager(SpanBufferClass); // returns ['userId', 'requestId']

// New SpanBuffer class generated with:
// - userId: eager (allocated in constructor)
// - requestId: eager (allocated in constructor)
// - sessionId: lazy (getter with symbol-based allocation)
// - experimentId: lazy (getter with symbol-based allocation)
```

### Testing Promotion

```typescript
import { describe, expect, it } from 'bun:test';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { getSpanBufferClass } from '../spanBuffer.js';

describe('Lazy-to-eager column promotion', () => {
  it('promotes high-usage columns to eager after 100 buffers', () => {
    const SpanBufferClass = getSpanBufferClass(
      defineLogSchema({
        userId: S.category(),
        sessionId: S.category(),
      })
    );

    // 100 buffers (the constructor counts each one) with userId used 90% of the time
    for (let i = 0; i < 100; i++) {
      const buffer = new SpanBufferClass(64);

      // Access userId in 90% of buffers (triggers lazy instantiation)
      if (i < 90) {
        buffer.userId_values[0] = i;
      }
    }

    const stats = SpanBufferClass.lazyColumnStats;
    expect(stats._totalSpanBuffersCreated).toBe(100);
    expect(stats.userId.instantiationCount).toBe(90);
    expect(stats.sessionId.instantiationCount).toBe(0);

    // userId qualifies (90%); sessionId does not (0%)
    expect(promoteColumnsToEager(SpanBufferClass)).toEqual(['userId']);
  });

  it('does not promote columns below 80% usage threshold', () => {
    const SpanBufferClass = getSpanBufferClass(
      defineLogSchema({
        userId: S.category(),
        sessionId: S.category(),
      })
    );

    // 100 buffers with userId used 75% of the time (below threshold)
    for (let i = 0; i < 100; i++) {
      const buffer = new SpanBufferClass(64);

      if (i < 75) {
        buffer.userId_values[0] = i;
      }
    }

    expect(SpanBufferClass.lazyColumnStats.userId.instantiationCount).toBe(75);

    // No promotion should occur
    expect(promoteColumnsToEager(SpanBufferClass)).toEqual([]);
  });

  it('requires 100 samples before promotion', () => {
    const SpanBufferClass = getSpanBufferClass(
      defineLogSchema({
        userId: S.category(),
      })
    );

    // Only 50 buffers (below minimum)
    for (let i = 0; i < 50; i++) {
      const buffer = new SpanBufferClass(64);
      buffer.userId_values[0] = i; // 100% usage
    }

    expect(SpanBufferClass.lazyColumnStats._totalSpanBuffersCreated).toBe(50);

    // No promotion due to insufficient samples
    expect(promoteColumnsToEager(SpanBufferClass)).toEqual([]);
  });
});
```

## Buffer Statistics Logging <a id="smoo/lmao!n/buffer-tuning-stats-logging"></a>

Buffer statistics are logged periodically (every 100 flushes by default) to provide visibility into buffer tuning
performance. Statistics use structured entry types with the `uint64_value` system column for efficient columnar storage.

Statistics are built by `createCapacityStatsTable` (`packages/lmao/src/lib/arrow/capacityStats.ts`) and driven from
`FlushScheduler.doFlush` via `CAPACITY_STATS_FLUSH_INTERVAL = 100`.

### Entry Types

| Entry Type        | Description                                                      | uint64_value         |
| ----------------- | ---------------------------------------------------------------- | -------------------- |
| `period-start`    | Start of the stats period                                        | Period start (ns)    |
| `buffer-writes`   | Completed user/event rows across the class's spans (rows 2+)     | `stats.totalWrites`  |
| `buffer-spans`    | Non-chained spans created (root and child, not overflow buffers) | `stats.spansCreated` |
| `buffer-capacity` | Capacity for new buffers of the class                            | `stats.capacity`     |

The three counters are the inputs of the utilization ratchet, so a query can recompute
`utilization = buffer-writes / (buffer-spans × (buffer-capacity − 2))` for any period.

### Why Structured Entry Types

1. **Proper columnar data**: No JSON parsing needed - `uint64_value` is a native column type
2. **Consistent with op metrics**: Same pattern as `op-invocations`, `op-errors`, etc.
3. **Dictionary-encoded strings**: `package_name`, `entry_type` are already dictionary types - efficient storage
4. **Single `uint64_value` column**: Reused across all metric types (ops, buffers, feature flags)

### How It Works

1. **Buffer Collection**: The `FlushScheduler` keeps registered root buffers in a `Set`; a flushed root leaves the set
   only after the handler succeeds ([§FlushScheduler Design](#smoo/lmao!n/buffer-tuning-flush)).

2. **Buffer Class Tracking**: During flush, the scheduler collects a `Map<SpanBufferConstructor, OpMetadata>`: one entry
   per span buffer class flushed since the last stats log (the class owns the `stats`), with the first `OpMetadata` seen
   for that class as its attribution.

3. **Flush Counter**: A global flush counter tracks how many flushes have occurred. When this counter reaches
   `CAPACITY_STATS_FLUSH_INTERVAL` (default: 100), buffer stats are logged.

4. **Stats Emission**: When the interval is reached, each tracked buffer class emits 4 entries with:
   - `timestamp`: The period start
   - `thread_id`: 0
   - `package_name`, `package_file`, `git_sha`: From the class's `OpMetadata` (dictionary-encoded)
   - `entry_type`: One of `period-start`, `buffer-writes`, `buffer-spans`, `buffer-capacity`
   - `uint64_value`: The metric value
   - All other columns: null or zero (buffer stats don't have trace/span context)

5. **Reset After Logging**: After a successful flush that logged buffer stats, the flush counter and the buffer class
   map are reset.

### Flush Output Example

```
timestamp | thread_id | package_name | entry_type       | uint64_value
----------|-----------|--------------|------------------|---------------
1000      | 0         | @myco/http   | period-start     | 1000
1000      | 0         | @myco/http   | buffer-writes    | 50000
1000      | 0         | @myco/http   | buffer-spans     | 1200
1000      | 0         | @myco/http   | buffer-capacity  | 64
```

### Query Example (ClickHouse)

```sql
SELECT
  package_name,
  anyIf(uint64_value, entry_type = 'buffer-writes') as total_writes,
  anyIf(uint64_value, entry_type = 'buffer-spans') as spans_created,
  anyIf(uint64_value, entry_type = 'buffer-capacity') as capacity,
  total_writes / nullIf(spans_created * (capacity - 2), 0) as utilization
FROM traces
WHERE entry_type = 'period-start' OR entry_type LIKE 'buffer-%'
GROUP BY timestamp, package_name
ORDER BY utilization DESC
```

### Benefits

- **Native columnar storage**: `uint64_value` is a typed column, no JSON parsing overhead
- **Efficient aggregation**: ClickHouse/DuckDB can use SIMD on `uint64_value` column
- **Consistent pattern**: Same structure as op metrics (`op-invocations`, `op-errors`, etc.)
- **Dictionary compression**: Entry type strings compress well (only 4 unique values)

## Observability Hook <a id="smoo/lmao!n/buffer-tuning-observability"></a>

Before each capacity-tuning check, which may reset the stats, the Tracer's `onStatsWillResetFor` hook is called.

The hook is declared on `TracerLifecycleHooks` (`packages/lmao/src/lib/traceRoot.ts`) and implemented by every tracer
(`TestTracer`, `StdioTracer`, `ArrayQueueTracer`, `SQLiteTracer`, `SQLiteAsyncTracer`, `NoOpTracer`, `CompositeTracer`).
It is invoked from the generated `getOrCreateOverflow` method
([01b5 §Constructor](./01b5_spanbuffer_memory_layout.md#smoo/lmao!n/spanbuffer-layout.constructor)), which calls
`tracer.onStatsWillResetFor(this)` then `checkCapacityTuning(stats)`.

```typescript
// In Tracer (abstract)
abstract onStatsWillResetFor(buffer: SpanBuffer<T>): void;

// TestTracer captures stats for test assertions
class TestTracer<T> extends Tracer<T> {
  readonly statsSnapshots: StatsSnapshot[] = [];

  onStatsWillResetFor(buffer: SpanBuffer<T>): void {
    const stats = buffer._stats;
    this.statsSnapshots.push({
      buffer,
      totalWrites: stats.totalWrites,
      spansCreated: stats.spansCreated,
      capacity: stats.capacity,
    });
  }
}
```

### Use Cases

1. **Test assertions**: `TestTracer.statsSnapshots` captures stats for verifying capacity tuning behavior
2. **Production metrics**: `ArrayQueueTracer` can emit stats as structured entries (see "Buffer Statistics Logging"
   above) using the `uint64_value` column pattern
3. **Development debugging**: `StdioTracer` can log capacity changes to console

### When Called

The hook is called:

- On every overflow, when `getOrCreateOverflow()` creates the next buffer of a chain
- Before `checkCapacityTuning()` makes its decision, so before any reset of the counters
- Whether or not that check changes the capacity

`getOrCreateOverflow()` first adds the full buffer's completed rows to `stats.totalWrites`, then calls the hook, then
`checkCapacityTuning()`:

```typescript
getOrCreateOverflow() {
  if (this._overflow) return this._overflow;
  this._sealStats(); // completed rows → stats.totalWrites
  const tracer = this._traceRoot.tracer;
  // Notify tracer before the tuning check (tracer captures for observability)
  tracer.onStatsWillResetFor(this);
  checkCapacityTuning(this.constructor.stats); // This may change capacity and reset stats
  return tracer.bufferStrategy.createOverflowBuffer(this);
}
```

This ensures tracers can capture the complete stats picture before they're lost to the reset.

## Summary

Self-tuning provides the utilization ratchet, aligned capacities, overflow chaining, scheduler-driven flush (including
the early flush under memory pressure), WASM freelist reuse, structured capacity statistics, and usage-driven
lazy-to-eager column promotion. Success means bounded, observable effective memory and better phase-specific benchmark
results—not universal zero overhead, zero copy, or immunity from OOM.
