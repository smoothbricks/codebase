# Columnar Buffer Architecture

## Overview

The columnar buffer architecture is the core performance engine of the trace logging system. It provides:

1. **Data-oriented design** with columnar TypedArrays for maximum performance
2. **Per-span buffers** - each span gets its own buffer for sorted output and simple implementation
3. **Fixed row layout** - rows 0-1 reserved for span lifecycle, events append from row 2
4. **Lazy column initialization** - attribute columns allocated only when first accessed
5. **Self-tuning capacity management** that adapts to usage patterns
6. **Buffer chaining for overflow** - part of self-tuning mechanism when capacity is exceeded
7. **Tree-structured spans** with efficient parent-child relationships
8. **Background processing pipeline** for Arrow/Parquet serialization

**Key Design Insight**: Every span gets its own buffer. This eliminates the need for traceId/spanId TypedArrays (they're
constant per buffer), keeps logs sorted in Arrow output, and enables efficient Arrow conversion.

## Design Philosophy

**Key Insight**: Traditional logging creates objects at runtime and serializes them later. This approach separates the
hot path (TypedArray writes) from the cold path (background processing), achieving <0.1ms runtime overhead.

**Data-Oriented Principles**:

- All arrays maintain equal length (columnar storage)
- Null bitmaps track which attributes have values
- Push nulls instead of using spanIndex for missing data
- Flat deferred structure, not nested objects
- Cache line alignment for optimal CPU performance

## Fixed Row Layout

Each SpanBuffer uses a fixed row layout that reserves specific positions for span lifecycle events:

```
SpanBuffer row layout:
┌──────────────────────────────────────────────────────────────────────────┐
│ Row 0: span-start     │ ctx.tag writes HERE (overwrites)                 │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 1: span-exception │ Pre-initialized at span creation                 │
│        (pre-init)     │ Overwritten by ctx.ok() → span-ok                │
│                       │            or ctx.err() → span-err               │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 2+: events        │ ctx.log.info/debug/warn/error appends here       │
└──────────────────────────────────────────────────────────────────────────┘

Span completion entry types (all written to row 1):
  - span-ok (6):        Success, written by ctx.ok()
  - span-err (7):       Expected business error, written by ctx.err()
  - span-exception (8): Uncaught exception, pre-initialized at row 1
```

### Key Points

1. **Row 0 is ALWAYS span-start**: `ctx.tag.*` overwrites `column[0]`, does NOT append new rows
2. **Row 1 is pre-initialized as span-exception**: Set to `ENTRY_TYPE_SPAN_EXCEPTION` at span creation
3. **Row 1 overwritten on completion**: `ctx.ok()` → span-ok, `ctx.err()` → span-err
4. **Exception safety**: If exception thrown and never caught, row 1 remains span-exception (valid data)
5. **Duration always valid**: `timestamps[1] - timestamps[0]` computes duration
6. **Events append from row 2**: `ctx.log.*` creates new rows starting at index 2
7. **writeIndex starts at 2**: After span initialization, ready for event appends

### ctx.tag Semantics (Datadog/Jaeger Pattern)

The `ctx.tag.*` API follows Datadog's `span.set_tag()` and OpenTelemetry's `Span.setAttribute()` semantics:

```typescript
// ctx.tag writes to row 0 (span-start row) - OVERWRITES, not appends
ctx.tag.userId('user-123'); // Writes to attr_userId[0]
ctx.tag.requestId('req-456'); // Writes to attr_requestId[0]
ctx.tag.userId('user-999'); // Overwrites attr_userId[0] - last write wins

// ctx.log.* creates new rows (events) - APPENDS
ctx.log.info('Processing...'); // Appends at row 2, increments writeIndex
ctx.log.debug('Details...'); // Appends at row 3, increments writeIndex
```

**Why Overwrite Semantics for Tags**:

- Matches Datadog's `span.set_tag()` - tags are span-level attributes, not events
- Matches Jaeger/OpenTelemetry's `Span.setAttribute()` - last write wins
- Enables progressive enrichment: set initial values early, refine later
- No wasted rows: multiple `ctx.tag` calls don't bloat the buffer

### Buffer Initialization

```typescript
function initializeSpanBuffer(buffer: SpanBuffer, spanName: string): void {
  const now = getTimestamp();

  // Row 0: span-start (always present)
  buffer.timestamps[0] = now;
  buffer.operations[0] = ENTRY_TYPE_SPAN_START;

  // Row 1: pre-initialized as span-exception (exception safety)
  buffer.timestamps[1] = now; // Updated by ctx.ok()/ctx.err()
  buffer.operations[1] = ENTRY_TYPE_SPAN_EXCEPTION; // Overwritten on completion

  // Ready for events at row 2
  buffer.writeIndex = 2;
}

function completeSpanOk(buffer: SpanBuffer, result?: any): void {
  buffer.timestamps[1] = getTimestamp();
  buffer.operations[1] = ENTRY_TYPE_SPAN_OK;
  // Result data written to row 1's attribute columns
}

function completeSpanErr(buffer: SpanBuffer, error: string): void {
  buffer.timestamps[1] = getTimestamp();
  buffer.operations[1] = ENTRY_TYPE_SPAN_ERR;
  // Error details written to row 1's attribute columns
}
```

### Duration Calculation

Duration is always computable from the fixed row layout:

```typescript
function getSpanDuration(buffer: SpanBuffer): number {
  // Duration = completion timestamp - start timestamp
  // Works for span-ok, span-err, AND span-exception (uncaught)
  return buffer.timestamps[1] - buffer.timestamps[0];
}
```

This design ensures every span has valid duration data, even when exceptions bypass normal completion.

## High-Precision Timestamp System

The trace logging system uses a high-precision timestamp design that provides sub-millisecond accuracy while minimizing
overhead. The design captures a single time anchor at trace root creation, then uses high-resolution timers for all
subsequent timestamps.

### Core Design Principles

1. **Zero allocations** - No object creation per timestamp
2. **High precision** - `performance.now()` gives ~5μs resolution in browsers
3. **Comparable** - All spans in a trace share the same anchor
4. **DST/NTP safe** - Anchor per trace, traces are short-lived
5. **Cross-platform** - Same API, different implementations

### Timestamp Precision Guarantees

**Storage Format**: All timestamps stored as microseconds (μs) in `Float64Array` during hot path.

**Precision by Platform**:

- **Browser**: ~5μs resolution from `performance.now()`, stored as microseconds
- **Node.js**: Nanosecond resolution from `process.hrtime.bigint()`, truncated to microseconds for consistency
- **Fallback**: Millisecond resolution from `Date.now()` when high-resolution timers unavailable

**Safe Range**: `Float64Array` can exactly represent integers up to `Number.MAX_SAFE_INTEGER` (2^53 - 1):

- Maximum safe microsecond timestamp: `9,007,199,254,740,991` microseconds
- Equivalent date: ~September 2255 (over 230 years from Unix epoch)
- **Conclusion**: BigInt→Number conversion is SAFE for all practical trace timestamps

**Why Microseconds**:

- `Float64Array` can exactly represent microseconds until year 2255
- Sub-millisecond precision enables detailed performance analysis
- Compatible with ClickHouse's `DateTime64(6)` type
- Matches OpenTelemetry's timestamp precision recommendation

**Arrow Format**: During cold path conversion, timestamps converted to Arrow `TimestampMicrosecond` type.

### Date.now() Usage Guidelines

**CRITICAL**: `Date.now()` has different usage rules depending on context:

**For Trace Timestamps** (Performance-Critical Path):

- ❌ **NEVER use `Date.now()` for span/log timestamps**
- ✅ **ALWAYS use anchored approach**: `getTimestampMicros(anchorEpochMicros, anchorPerfNow)`
- Why: Anchored timestamps provide sub-millisecond precision and avoid repeated system calls

**For Scheduling/Background Tasks** (Non-Performance-Critical):

- ✅ **OK to use `Date.now()` for scheduling**: Background flush intervals, timeout calculations
- ✅ **OK for file naming**: `traces-${Date.now()}.parquet`
- ✅ **OK for logging system metadata**: Capacity tuning events, system diagnostics
- Why: Millisecond precision acceptable, system clock alignment desired

**Example**:

```typescript
// ✅ CORRECT: Trace timestamps use anchored approach
buffer.timestamps[idx] = getTimestampMicros(ctx.anchorEpochMicros, ctx.anchorPerfNow);

// ❌ WRONG: Don't use Date.now() for trace timestamps
buffer.timestamps[idx] = Date.now() * 1000; // No sub-ms precision, repeated syscalls

// ✅ CORRECT: Scheduling uses Date.now()
const nextFlushTime = Date.now() + flushIntervalMs;

// ✅ CORRECT: File naming uses Date.now()
const filename = `traces-${Date.now()}.parquet`;
```

### Browser Implementation

```typescript
// ONE Date.now() captured at trace root (RequestContext creation)
// ONE performance.now() captured at trace root
// All subsequent timestamps use delta calculation

interface RequestContext {
  traceId: string;
  requestId: string;
  userId?: string;

  // Time anchor - flat primitives, not nested object
  anchorEpochMicros: number; // Date.now() * 1000 at trace root
  anchorPerfNow: number; // performance.now() at trace root

  ff: FeatureFlagEvaluator;
  env: Env;
}

function createRequestContext(params: RequestParams): RequestContext {
  const epochMs = Date.now();
  const perfNow = performance.now();

  return {
    traceId: generateTraceId(),
    requestId: params.requestId,
    userId: params.userId,

    // Capture time anchor once at trace root
    anchorEpochMicros: epochMs * 1000,
    anchorPerfNow: perfNow,

    ff: createFeatureFlagEvaluator(params),
    env: params.env,
  };
}

// All subsequent timestamps derived from anchor
function getTimestamp(ctx: RequestContext): number {
  // Returns microseconds since epoch
  // performance.now() - anchorPerfNow gives elapsed ms with sub-ms precision
  return ctx.anchorEpochMicros + (performance.now() - ctx.anchorPerfNow) * 1000;
}
```

**Benefits**:

- No `Date.now()` calls per span - only `performance.now()` deltas
- Sub-millisecond precision for all spans within a trace
- Single anchor ensures all timestamps in trace are comparable

### Node.js Implementation

```typescript
// ONE Date.now() captured at trace root
// ONE process.hrtime.bigint() captured at trace root
// All subsequent timestamps use hrtime delta

interface RequestContext {
  traceId: string;
  requestId: string;
  userId?: string;

  // Time anchor - flat primitives
  anchorEpochMicros: number; // Date.now() * 1000 at trace root
  anchorHrTime: bigint; // process.hrtime.bigint() at trace root

  ff: FeatureFlagEvaluator;
  env: Env;
}

function createRequestContext(params: RequestParams): RequestContext {
  const epochMs = Date.now();
  const hrTime = process.hrtime.bigint();

  return {
    traceId: generateTraceId(),
    requestId: params.requestId,
    userId: params.userId,

    // Capture time anchor once at trace root
    anchorEpochMicros: epochMs * 1000,
    anchorHrTime: hrTime,

    ff: createFeatureFlagEvaluator(params),
    env: params.env,
  };
}

// All subsequent timestamps derived from anchor
function getTimestamp(ctx: RequestContext): number {
  // Returns microseconds since epoch
  // hrtime.bigint() gives nanoseconds, convert to microseconds
  const elapsedNanos = process.hrtime.bigint() - ctx.anchorHrTime;
  return ctx.anchorEpochMicros + Number(elapsedNanos / 1000n);
}
```

**Benefits**:

- Nanosecond precision available from hrtime, stored as microseconds
- No `Date.now()` calls during span execution
- Single anchor ensures all timestamps in trace are comparable

### Arrow Timestamp Format

Timestamps are stored as microseconds since epoch in Float64Array during the hot path, then converted to Arrow's
TimestampMicrosecond type during cold path conversion:

```typescript
// Hot path: Float64Array storage (microseconds since epoch)
buffer.timestamps[idx] = getTimestamp(ctx); // e.g., 1704067200000000

// Cold path: Arrow conversion
const arrowTimestamps = arrow.TimestampMicrosecond.from(buffer.timestamps.subarray(0, buffer.writeIndex));
```

**Why Microseconds**:

- `Float64Array` can exactly represent microseconds up to year 285,616
- Sub-millisecond precision enables detailed performance analysis
- Compatible with ClickHouse's `DateTime64(6)` type
- Matches OpenTelemetry's timestamp precision

### Why Flattened RequestContext

The `RequestContext` uses flat primitives instead of nested objects:

```typescript
// ✅ CORRECT: Flat primitives
interface RequestContext {
  anchorEpochMicros: number;
  anchorPerfNow: number;
  // ...
}

// ❌ WRONG: Nested object
interface RequestContext {
  timeAnchor: {
    epochMicros: number;
    perfNow: number;
  };
  // ...
}
```

**Benefits of Flat Structure**:

- Better V8 hidden class optimization
- One less pointer chase per timestamp read
- Simpler serialization if context needs to cross boundaries
- Matches the "flat deferred structure" design principle

## Cache Line Alignment Benefits

**Memory Performance Optimization**: Each TypedArray is sized to align with 64-byte cache line boundaries, providing
several key benefits:

1. **Reduced Cache Misses**: When the CPU accesses array elements, entire cache lines (64 bytes) are loaded. Aligned
   arrays ensure no partial cache line loads.

2. **Improved Prefetching**: CPU prefetchers work more efficiently with aligned data structures, reducing memory
   latency.

3. **Better SIMD Performance**: Vectorized operations (when available) perform optimally on cache-aligned data.

4. **Reduced False Sharing**: Different arrays won't share cache lines between CPU cores, eliminating false sharing
   penalties.

**Example Alignment Calculations**:

```typescript
// Starting with 64 elements (cache-friendly initial capacity):

// Uint8Array (operations column): 64 × 1 = 64 bytes → no alignment needed
// Uint16Array (small bitmaps): 64 × 2 = 128 bytes → no alignment needed
// Uint32Array (string indices): 64 × 4 = 256 bytes → no alignment needed
// Float64Array (timestamps): 64 × 8 = 512 bytes → no alignment needed

// Example with smaller capacity showing alignment impact:
// Uint8Array: 16 × 1 = 16 bytes → aligned to 64 bytes = 64 elements (4x increase!)
// Uint16Array: 16 × 2 = 32 bytes → aligned to 64 bytes = 32 elements (2x increase!)
```

**Memory vs Performance Trade-off**: Cache alignment increases memory usage for small arrays but provides significant
performance benefits. By starting with 64 elements, we minimize unexpected capacity increases while maintaining
cache-friendly allocation patterns.

**Critical Design Decision - Equal Length Constraint**: The most important constraint is that ALL TypedArrays in a
SpanBuffer must have exactly the same length. This enables:

- Direct row indexing: `buffer.timestamps[i]`, `buffer.operations[i]`, `buffer.attr_userId[i]` all refer to the same
  logical row
- Zero-copy Arrow conversion: arrays can be sliced directly to Arrow vectors without data copying
- Consistent null bitmap indexing: `buffer.nullBitmap[i]` tracks attribute presence for row `i`

**Cache Alignment Strategy**: We calculate alignment using 1-byte elements (worst case) to ensure ALL array types are
cache-aligned:

- Uint8Array gets optimal 1 cache line alignment
- Larger types (Uint16Array, Uint32Array, Float64Array) are also aligned (or over-aligned)
- All arrays have identical element count, preserving columnar storage requirements

**Performance Impact**: In high-throughput logging scenarios, this alignment can improve memory bandwidth utilization by
10-30% and reduce CPU cache misses significantly.

## Lazy Column Initialization

**Memory Optimization**: Attribute columns (`attr_*`) use **lazy getters** that allocate shared ArrayBuffers on first
access. This provides significant memory savings for sparse columns.

**Important**: Scope values are stored in a SEPARATE Scope class (see 01i_span_scope_attributes.md), NOT in the buffer
columns.

### Why Lazy Initialization

1. **Sparse Columns are Free**: If a tag attribute is defined in the schema but never used in a particular span, no
   memory is allocated for that column at all.

2. **Matches Arrow Semantics**: Arrow uses per-column null bitmaps. If a column is never written, there's nothing to
   convert - it simply doesn't exist in the output.

3. **Schema Flexibility**: Modules can define comprehensive schemas with many optional attributes without paying memory
   cost for unused ones.

4. **Shared ArrayBuffer**: Each column uses ONE ArrayBuffer for both nulls and values, partitioned with cache alignment.

### What Gets Allocated When

**Immediately Allocated (Core Columns)**:

- `timestamps: Float64Array` - Every entry has a timestamp
- `operations: Uint8Array` - Every entry has an operation/entry type

**Lazily Allocated (via Lazy Getters)**:

- `attr_X_values: TypedArray` - Allocated on first getter access
- `attr_X_nulls: Uint8Array` - Same allocator as values (shares ArrayBuffer)
- Both use the SAME underlying ArrayBuffer, partitioned with cache alignment

**Scope Values** (SEPARATE from buffer):

- Stored in Scope class instances (see 01i_span_scope_attributes.md)
- NOT stored in buffer columns
- Applied to buffer columns during writes or pre-fill operations

### Lazy Getter Implementation (Generated via `new Function()`)

The actual implementation generates a concrete SpanBuffer class with lazy getters for each column:

```typescript
// Generated at module creation time (cold path)
// Example for userId attribute:

// Symbol for per-instance storage
const attr_userId_sym = Symbol('attr_userId');

// Allocator function (called by getters)
function allocate_attr_userId(self) {
  if (self[attr_userId_sym]) return self[attr_userId_sym];

  const capacity = self._alignedCapacity;
  const nullBitmapSize = Math.ceil(capacity / 8);

  // Cache-align null bitmap end for optimal values array placement
  const bytesPerElement = 4; // Uint32Array
  const alignedNullOffset = Math.ceil(nullBitmapSize / bytesPerElement) * bytesPerElement;

  const totalSize = alignedNullOffset + capacity * bytesPerElement;

  // ONE ArrayBuffer for both nulls and values
  const buffer = new ArrayBuffer(totalSize);

  const storage = {
    buffer: buffer,
    nulls: new Uint8Array(buffer, 0, nullBitmapSize),
    values: new Uint32Array(buffer, alignedNullOffset, capacity),
  };

  self[attr_userId_sym] = storage;
  return storage;
}

// Generated class with lazy getters
class GeneratedColumnBuffer {
  constructor(requestedCapacity) {
    const alignedCapacity = getCacheAlignedCapacity(requestedCapacity);
    this._alignedCapacity = alignedCapacity;
    this.timestamps = new Float64Array(alignedCapacity);
    this.operations = new Uint8Array(alignedCapacity);
    this.writeIndex = 0;
    this.capacity = requestedCapacity;
  }

  // Lazy getters for userId column
  get attr_userId_nulls() {
    return allocate_attr_userId(this).nulls;
  }
  get attr_userId_values() {
    return allocate_attr_userId(this).values;
  }
  get attr_userId() {
    return allocate_attr_userId(this).values;
  } // Shorthand

  // ... same pattern for all schema attributes

  // Helpers to check allocation without triggering it
  getColumnIfAllocated(columnName) {
    const sym = columnSymbols[columnName];
    return sym ? this[sym]?.values : undefined;
  }

  getNullsIfAllocated(columnName) {
    const sym = columnSymbols[columnName];
    return sym ? this[sym]?.nulls : undefined;
  }
}
```

**Key Design Points**:

1. **Shared ArrayBuffer**: Each column uses ONE ArrayBuffer for both nulls and values
2. **Cache Alignment**: Null bitmap end is aligned to value type's element size
3. **Per-Instance Storage**: Symbol-keyed properties ensure each buffer instance has its own arrays
4. **Zero Indirection**: Direct property access (`buffer.attr_userId_values[i]`)
5. **V8 Optimization**: Generated classes optimize well with hidden classes and inline caching

### Memory Impact Example

Consider a schema with 20 attributes where a typical span only uses 3:

```typescript
// Without lazy initialization (ALL columns allocated):
// 2 core columns: Float64Array(64) + Uint8Array(64) = 576 bytes
// 20 attr columns: 20 × (Uint32Array(64) + null bitmap) = 20 × (256 + 8) = 5,280 bytes
// Total: 5,856 bytes per span

// With lazy getters (ONLY used columns allocated):
// 2 core columns: Float64Array(64) + Uint8Array(64) = 576 bytes (always)
// 20 lazy getter closures: ~0 bytes (generated code, shared across instances)
// 3 used attr columns with shared ArrayBuffers:
//   - Each: ceil(64/8) = 8 bytes (nulls) + padding + 256 bytes (values) ≈ 272 bytes
//   - Total: 3 × 272 = 816 bytes (on demand)
// Total: ~1,392 bytes per span (76% memory savings!)
//
// Note: Each column's nulls and values share ONE ArrayBuffer, reducing allocations
```

## Base SpanBuffer Interface

**Purpose**: Provide a generic interface that can be extended with schema-generated columns.

**CRITICAL**: Column properties are **direct properties** on the SpanBuffer via lazy getters (no nested
`columns: Record<...>`). This design provides zero indirection for hot path access.

```typescript
interface SpanBuffer {
  // Core columns - always present (allocated immediately in constructor)
  timestamps: Float64Array; // Every operation appends timestamp
  operations: Uint8Array; // Operation type: tag, ok, err, etc.

  // Attribute columns - DIRECT PROPERTIES with LAZY GETTERS (no nested Record!)
  // Each attribute has TWO properties sharing ONE ArrayBuffer:
  // - attr_X_nulls: Uint8Array for null bitmap (Arrow format: 1=valid, 0=null)
  // - attr_X_values: TypedArray for actual values
  // Schema-generated via new Function() at module creation time
  attr_userId_nulls: Uint8Array; // Lazy getter
  attr_userId_values: Uint32Array; // Lazy getter
  attr_userId: Uint32Array; // Shorthand getter (alias for _values)

  attr_requestId_nulls: Uint8Array;
  attr_requestId_values: Uint32Array;
  attr_requestId: Uint32Array;

  attr_http_status_nulls: Uint8Array;
  attr_http_status_values: Uint16Array;
  attr_http_status: Uint16Array;
  // ... same pattern for all schema attributes

  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer; // Reference to parent SpanBuffer
  task: TaskContext; // Reference to task + module metadata

  // Buffer management
  writeIndex: number; // Current write position (0 to capacity-1)
  capacity: number; // Logical capacity for bounds checking
  next?: SpanBuffer; // Chain to next buffer when overflow (part of self-tuning)

  // Span Identification (see "Distributed Span ID Design" section below)
  // A span represents a unit of work within a single thread of execution.
  threadId: bigint; // 64-bit random ID, set from worker context (same for all spans in worker)
  spanId: number; // 32-bit THREAD-LOCAL counter - "nth span on this thread", NOT "nth span in trace"
  traceId: string; // Root trace ID (constant per span, no TypedArray needed)

  // Helpers (don't trigger allocation)
  getColumnIfAllocated(columnName: string): TypedArray | undefined;
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;

  // NOTE: Each span gets its own buffer, so traceId and span IDs are constant
  // No need for traceId/spanId TypedArrays - they're the same for every row in this buffer
  // This keeps logs sorted and enables efficient Arrow conversion
  //
  // Span Definition: A span represents a unit of work within a single thread of execution.
  // This justifies having both threadId and spanId as separate concepts.
  //
  // Span ID Design: threadId + spanId form a globally unique identifier
  // - threadId: 64-bit BigInt, generated once per worker/process, shared across all spans
  // - spanId: 32-bit THREAD-LOCAL counter, incremented per span on this thread (cheap i++)
  //   NOTE: spanId is NOT per-trace! It's "nth span on this thread" across ALL traces.
  //   Use timestamps if you need trace-relative ordering.
  // - Parent reference: derived from parent SpanBuffer's threadId + spanId
  //
  // Global Uniqueness:
  // - Within a trace: (threadId, spanId) is unique
  // - Globally: (traceId, threadId, spanId) is globally unique
}

// Access pattern - DIRECT property access, zero indirection:
buffer.attr_userId_values[idx] = value; // ✅ Direct TypedArray access
buffer.attr_userId_nulls[byteIdx] |= bitmask; // ✅ Direct bitmap access
buffer.attr_userId[idx] = value; // ✅ Shorthand (alias for _values)

// Check allocation without triggering it:
const values = buffer.getColumnIfAllocated('attr_userId'); // Returns undefined if not allocated

// NOT this pattern (extra indirection via nested Record):
// buffer.columns.attr_userId[idx] = value;  // ❌ One extra lookup
```

## Span Definition

> **A span represents a unit of work within a single thread of execution.**

This definition is the foundation of LMAO's span identification design. Unlike OpenTelemetry which defines a span as
simply a "unit of work" with random 64-bit IDs, LMAO explicitly ties spans to their thread of execution.

### Comparison to OpenTelemetry

| Aspect              | OpenTelemetry                | LMAO                                               |
| ------------------- | ---------------------------- | -------------------------------------------------- |
| **Span Definition** | "Unit of work"               | "Unit of work within a single thread of execution" |
| **Span ID**         | Random 64-bit                | `(thread_id, span_id)` composite                   |
| **Generation**      | Crypto random per span       | `thread_id` once per thread, `span_id++` per span  |
| **Hot Path Cost**   | Random generation + BigInt   | Simple `i++` increment                             |
| **Thread Concept**  | None (spans are independent) | Explicit (spans belong to threads)                 |
| **Timeline View**   | Requires timestamp sorting   | `span_id` gives within-thread ordering             |

## Distributed Span ID Design

### Problem: Span ID Collisions in Distributed Tracing

A simple incrementing `span_id: number` works for single-process tracing, but causes collisions in distributed
scenarios:

```
Machine A: trace_id="req-123", span_id=1, 2, 3...
Machine B: trace_id="req-123", span_id=1, 2, 3... ← collision!
```

This occurs with:

- **Worker threads**: `pmap()` distributing work across threads
- **Distributed services**: Same trace spanning multiple machines
- **Serverless**: Multiple Lambda invocations for the same request

### Solution: Separate Columns (threadId + spanId)

The span ID is split into two components with different lifecycles:

```typescript
// Generated ONCE per worker/process at startup (cold path)
const threadId: bigint = generateRandom64Bit();

interface SpanBuffer {
  threadId: bigint; // Reference to worker's 64-bit random ID
  spanId: number; // 32-bit incrementing counter (i++ per span)
  parent?: SpanBuffer; // Tree link to derive parent IDs
}
```

### Arrow Column Schema

| Column             | Type                 | Description                                           |
| ------------------ | -------------------- | ----------------------------------------------------- |
| `trace_id`         | `dictionary<string>` | Request correlation across services                   |
| `thread_id`        | `uint64`             | Thread/worker identifier (64-bit random, once/thread) |
| `span_id`          | `uint32`             | Unit of work within thread (incrementing counter)     |
| `parent_thread_id` | `uint64` (nullable)  | Parent span's thread                                  |
| `parent_span_id`   | `uint32` (nullable)  | Parent span's ID                                      |

### Global Uniqueness

- **Within a trace**: `(thread_id, span_id)` is unique
- **Globally**: `(trace_id, thread_id, span_id)` is globally unique

### Parent Reference

To find a parent span, you need:

- Same `trace_id` (parent is always in same trace)
- Match `thread_id = parent_thread_id` AND `span_id = parent_span_id`

### Why This Design?

**Hot Path Performance**:

- `spanId = nextSpanId++` is a simple increment (no BigInt, no crypto)
- No random generation per span
- No 64-bit arithmetic in the hot path

**Cold Path Efficiency**:

- `threadId` generated once at worker startup using `crypto.getRandomValues()`
- BigInt conversion happens once, not per span
- Arrow conversion references the same `threadId` for all spans in a buffer

**Collision Resistance**:

- 64-bit random thread ID: ~4 billion threads before 50% collision (birthday paradox)
- Combined with 32-bit local counter: effectively unlimited unique spans
- In practice, collision probability is negligible

**Query Flexibility**:

- Separate columns allow efficient querying by `thread_id` alone
- No struct unpacking needed for common queries
- Direct column filtering in WHERE clauses

### Thread-Local Counter Design (CRITICAL)

**The `span_id` is a THREAD-LOCAL counter**, not a per-trace counter:

```
Main thread (thread_id: 0xAAA): span_id 1, 2, 3, 4, 5...  (all traces combined)
Worker A (thread_id: 0xBBB): span_id 1, 2, 3...           (all traces combined)
Worker B (thread_id: 0xCCC): span_id 1, 2...              (all traces combined)
```

**What `span_id` IS**:

- A simple `i++` counter local to each thread/worker
- Each thread starts at 1 and increments independently
- Counts ALL spans created on that thread, across ALL traces
- Zero coordination between threads

**What `span_id` is NOT**:

- ❌ NOT a global counter shared across threads (would need synchronization)
- ❌ NOT a per-trace counter (would require Map lookups and cleanup)
- ❌ NOT semantically "nth span in this trace"

**Semantic Meaning**:

- `span_id` means "nth span created on this thread" (across all traces)
- If you need trace-relative ordering, use timestamps
- The tuple `(thread_id, span_id)` is globally unique, that's what matters

**Why This Design**:

- **Zero overhead**: Just `i++`, nothing else
- **No synchronization**: Each thread has its own counter
- **No Map lookups**: No per-trace state to manage
- **No cleanup logic**: No trace completion tracking needed
- **Still globally unique**: `(thread_id, span_id)` never collides

### Runtime Representation

```typescript
// Worker initialization (ONCE at startup)
const workerThreadId = generateRandom64Bit(); // crypto.getRandomValues() → BigInt

// Thread-local counter - each thread/worker has its own counter
// NOT per-trace, NOT global - just a simple i++ per thread
// Increments across ALL traces on this thread
let nextSpanId = 1;

function createSpanBuffer(/* ... */): SpanBuffer {
  return {
    threadId: workerThreadId, // Reference to worker's threadId (same for all spans on this thread)
    spanId: nextSpanId++, // Just i++ (cheap!) - nth span on THIS THREAD, not nth span in trace
    parent: parentBuffer, // Tree link for parent ID derivation
    // ... other fields
  };
}
```

### Arrow Conversion

During cold path conversion to Arrow, span IDs become separate columns:

```typescript
// Arrow columns (separate, not a Struct)
thread_id: Uint64; // buffer.threadId (BigInt → Uint64)
span_id: Uint32; // buffer.spanId (number → Uint32)
parent_thread_id: Uint64; // buffer.parent?.threadId (nullable)
parent_span_id: Uint32; // buffer.parent?.spanId (nullable)
```

**Conversion efficiency**:

- `threadId` BigInt conversion happens once per buffer (not per row)
- `spanId` uses Uint32Array directly (no conversion needed)
- Parent IDs derived from tree structure (no separate storage)

### Cross-Thread Parent References

Child spans on different threads can reference parent spans on other threads:

```typescript
// Parent span on Thread A
const parentBuffer = {
  threadId: 0x1a2b3c4d5e6f7890n, // Thread A's ID
  spanId: 42,
  // ...
};

// Child span on Thread B (via pmap or worker)
const childBuffer = {
  threadId: 0x9876543210fedcban, // Thread B's ID (different!)
  spanId: 1,
  parent: parentBuffer, // References parent on Thread A
  // ...
};

// Arrow output (separate columns):
// child: thread_id=0x9876543210fedcba, span_id=1, parent_thread_id=0x1a2b3c4d5e6f7890, parent_span_id=42
```

### Query Examples

```sql
-- Find all ancestors (recursive)
WITH RECURSIVE ancestors AS (
  SELECT * FROM spans
  WHERE trace_id = @trace_id AND thread_id = @tid AND span_id = @sid
  UNION ALL
  SELECT s.* FROM spans s
  JOIN ancestors a
    ON s.trace_id = a.trace_id
   AND s.thread_id = a.parent_thread_id
   AND s.span_id = a.parent_span_id
)
SELECT * FROM ancestors;

-- All spans from a specific thread (timeline view)
SELECT * FROM spans
WHERE trace_id = @trace_id AND thread_id = @tid
ORDER BY span_id;

-- Work distribution across threads
SELECT thread_id, COUNT(*) as span_count
FROM spans
WHERE trace_id = @trace_id
GROUP BY thread_id;

-- Find root spans (null parent)
SELECT * FROM spans
WHERE parent_thread_id IS NULL AND parent_span_id IS NULL;
```

### Benefits Summary

| Aspect           | Design Choice                        | Benefit                                  |
| ---------------- | ------------------------------------ | ---------------------------------------- |
| **Hot path**     | `spanId++`                           | Zero crypto/BigInt overhead per span     |
| **Cold path**    | BigInt conversion once per buffer    | Minimal conversion overhead              |
| **Collisions**   | 64-bit random thread ID              | Negligible collision probability         |
| **Parent refs**  | Tree structure with SpanBuffer links | Cross-thread parents naturally supported |
| **Arrow output** | Separate columns (not Struct)        | Direct column filtering, no unpacking    |
| **JS compat**    | 64-bit BigInt + 32-bit number        | No BigInt in hot path                    |

**Why This Design**:

- **Zero indirection**: Direct property access is faster than nested Record lookup
- **Lazy allocation**: Columns are only allocated on first access (via lazy getters)
- **Shared ArrayBuffer**: Each column's nulls and values share ONE ArrayBuffer (cache-friendly)
- **Per-span buffers**: Each span gets its own buffer for sorted logs and simple implementation
- **No traceId/spanId arrays**: These are constant per buffer, stored as properties
- **Symbol-based storage**: Per-instance storage via Symbol keys (no closure sharing bugs)
- **Minimal interface**: Only essential fields, no capacity/length bloat
- **Shared references**: Module context shared across all tasks
- **Tree structure**: Efficient parent-child span relationships
- **Buffer chaining**: Handle overflow with linked buffers (part of self-tuning mechanism)
- **Freelist consideration**: May keep pool of buffers if long-lived TypedArrays help V8's GC

**Scope Values** (SEPARATE from buffer):

Scope values are stored in a **separate Scope class** (see 01i_span_scope_attributes.md), NOT in buffer columns. The
Scope class is generated via `new Function()` and contains only schema attributes with getters/setters. Scope values are
applied to buffer columns during writes or pre-fill operations.

## Schema-Generated Buffer Extensions

**Purpose**: Extend the base interface with typed columns based on tag attribute schemas.

**See Also**: [Trace Schema System](./01a_trace_schema_system.md) for how these schemas are defined.

```typescript
// Generated from composed schema (HTTP + DB + user attributes)
// Each attribute becomes DIRECT properties via lazy getters on buffer
interface ComposedSpanBuffer extends SpanBuffer {
  // HTTP library attributes (attr_ prefix prevents conflicts with SpanBuffer internals)
  attr_http_status: Uint16Array; // HTTP status codes
  attr_http_method: Uint8Array; // enum index for GET/POST/PUT/DELETE
  attr_http_url: Uint32Array; // string registry index (masked)
  attr_http_duration: Float32Array; // request duration in ms

  // Database library attributes (attr_ prefix prevents conflicts)
  attr_db_query: Uint32Array; // string registry index (masked SQL)
  attr_db_duration: Float32Array; // query duration in ms
  attr_db_rows: Uint32Array; // rows affected/returned
  attr_db_table: Uint32Array; // string registry index

  // User-defined attributes (attr_ prefix prevents conflicts)
  attr_user_id: Uint32Array; // string registry index (hashed)
  attr_business_metric: Float64Array; // custom metric value
}

// attr_ prefix prevents conflicts with SpanBuffer internal fields
// Without prefix, user attribute "parent" would conflict with buffer.parent
// Without prefix, user attribute "task" would conflict with buffer.task
// Without prefix, user attribute "writeIndex" would conflict with buffer.writeIndex

function createSpanBuffer<T extends TagAttributeSchema>(
  schema: T,
  taskContext: TaskContext,
  traceId: string,
  workerThreadId: bigint, // 64-bit random ID from worker context
  parentBuffer?: SpanBuffer // Optional parent buffer for tree linking
): SpanBuffer {
  const spanId = nextSpanId++; // Simple increment (cheap!)
  return createEmptySpanBuffer(spanId, traceId, workerThreadId, schema, taskContext, parentBuffer);
}

// Per-worker (thread-local) counter - NOT per-trace, NOT global
// Each thread starts at 1 and increments independently across all traces
let nextSpanId = 1;

function createEmptySpanBuffer<T extends TagAttributeSchema>(
  spanId: number,
  traceId: string,
  workerThreadId: bigint,
  schema: T,
  taskContext: TaskContext,
  parentBuffer?: SpanBuffer
): SpanBuffer {
  // Use runtime-generated class from arrow-builder
  // The class has lazy getters for all attribute columns
  const columnBuffer = createColumnBuffer(schema, taskContext.module.spanBufferCapacityStats.currentCapacity);

  // Extend with span-specific metadata
  const buffer = columnBuffer as SpanBuffer;

  // Span ID: threadId + spanId form globally unique identifier
  // A span represents a unit of work within a single thread of execution.
  buffer.threadId = workerThreadId; // 64-bit, same for all spans in this worker
  buffer.spanId = spanId; // 32-bit, incrementing per span on this thread

  buffer.traceId = traceId;
  buffer.children = [];
  buffer.parent = parentBuffer; // Tree link for parent ID derivation

  // Link to parent's children array
  if (parentBuffer) {
    parentBuffer.children.push(buffer);
  }

  taskContext.module.spanBufferCapacityStats.totalBuffersCreated++;
  return buffer;
}

// NOTE: Scope inheritance happens at the SpanLogger/context level via the Scope class,
// NOT at the buffer level. The Scope class (_getScopeValues()) provides parent values
// that are copied to child Scope instances and pre-filled into child buffers.
// See 01i_span_scope_attributes.md for details.

function createNextBuffer(buffer: SpanBuffer): SpanBuffer {
  // Buffer chaining is part of the self-tuning mechanism (see 01b2_buffer_self_tuning.md)
  // When a buffer overflows, we chain to a new buffer for the SAME logical span
  // The chained buffer inherits threadId + spanId and uses the same task context
  return createEmptySpanBuffer(
    buffer.spanId, // Same logical span (same span ID)
    buffer.traceId,
    buffer.threadId, // Same thread ID (chained buffer is in same worker)
    getSchemaFromBuffer(buffer), // Re-use schema
    buffer.task, // Re-use task context
    buffer.parent // Parent is the same as the current buffer's parent
  );
}

function createChildSpan(parentBuffer: SpanBuffer, label: string, childFn: SpanFunction) {
  const childTaskContext: TaskContext = {
    module: parentBuffer.task.module,
    spanNameId: internString(label),
    lineNumber: getCurrentLineNumber(), // Build tool injected
  };

  // Each child span gets its own NEW buffer with its own spanId
  // NOTE: Scope values are inherited via Scope class, NOT buffer columns
  const childBuffer = createChildSpanBuffer(
    parentBuffer, // Links to parent and inherits traceId
    childTaskContext
  );

  return childBuffer;
}
```

**Why This Design**:

- **Type safety**: Schema drives column generation and TypeScript types
- **Memory efficiency**: Lazy column initialization - only allocate what's used
- **Cache line alignment**: All TypedArrays are aligned to 64-byte cache line boundaries for optimal CPU performance
- **Conflict prevention**: `attr_` prefix prevents conflicts with SpanBuffer internal fields
- **Clean Arrow output**: Prefix stripped during Arrow conversion for queryable column names
- **Flexible storage**: Different TypedArray types for different data types

## Cache-Aligned Attribute Column Generation

```typescript
/**
 * Generate attribute columns with consistent capacity for columnar storage
 *
 * DESIGN PRINCIPLES:
 * 1. EQUAL LENGTH: All generated arrays use the same alignedCapacity
 * 2. TYPE SAFETY: Each field type maps to an appropriate TypedArray
 * 3. NAMING: 'attr_' prefix prevents conflicts with SpanBuffer internal fields
 * 4. CACHE ALIGNMENT: alignedCapacity is pre-calculated for optimal memory layout
 *
 * TYPE MAPPING RATIONALE:
 * - string/categorical/enum → Uint32Array: Store string registry indices (4 bytes = good balance)
 * - number → Float64Array: Full precision for numeric values (8 bytes)
 * - integer → Int32Array: Signed integers up to 2^31 (4 bytes)
 * - boolean → Uint8Array: Minimal storage for true/false (1 byte)
 * - duration → Float32Array: Sufficient precision for timing (4 bytes)
 *
 * @param schema - TagAttributeSchema defining field names and types
 * @param alignedCapacity - Pre-calculated cache-aligned capacity (same for all arrays)
 * @returns Object with attr_* properties containing TypedArrays of equal length
 */
function generateAttributeColumns<T extends TagAttributeSchema>(
  schema: T,
  alignedCapacity: number // Single capacity for ALL arrays (already cache-aligned)
): Record<string, TypedArray> {
  const attributeColumns: Record<string, TypedArray> = {};

  for (const [fieldName, fieldConfig] of Object.entries(schema.fields)) {
    const columnName = `attr_${fieldName}`;

    // ALL arrays use the SAME aligned capacity (equal length requirement)
    let typedArray: TypedArray;
    switch (fieldConfig.type) {
      case 'string':
      case 'categorical':
      case 'enum':
        // String registry indices stored as Uint32Array
        typedArray = new Uint32Array(alignedCapacity);
        break;
      case 'number':
        // Numbers stored as Float64Array
        typedArray = new Float64Array(alignedCapacity);
        break;
      case 'integer':
        // Integers stored as Int32Array
        typedArray = new Int32Array(alignedCapacity);
        break;
      case 'boolean':
        // Booleans stored as Uint8Array
        typedArray = new Uint8Array(alignedCapacity);
        break;
      case 'duration':
        // Durations stored as Float32Array
        typedArray = new Float32Array(alignedCapacity);
        break;
      default:
        // Fallback to Uint32Array for unknown types
        typedArray = new Uint32Array(alignedCapacity);
    }

    attributeColumns[columnName] = typedArray;
  }

  return attributeColumns;
}
```

## Tag Operation Implementation

**Purpose**: Tag operations write span attributes to row 0 (span-start row), using **overwrite** semantics.

### ctx.tag - Overwrites Row 0 (Datadog/OpenTelemetry Pattern)

```typescript
// ctx.tag writes to row 0 - OVERWRITES, not appends
// Follows Datadog's span.set_tag() and OpenTelemetry's Span.setAttribute()
// Accesses lazy getters which trigger allocation on first write
// Column properties are DIRECT on buffer (not nested in .columns)

const tagOperations = {
  requestId: (buffer: SpanBuffer, value: string) => {
    // ALWAYS write to row 0 (span-start row) - overwrite semantics
    // Accessing attr_requestId_values triggers lazy allocation (if first write)
    const internedIndex = categoryInterner.intern(value);
    buffer.attr_requestId_values[0] = internedIndex; // Direct TypedArray access
    buffer.attr_requestId_nulls[0] |= 1; // Set bit 0 as valid (Arrow format)

    // Return this for chaining
    return this;
  },

  userId: (buffer: SpanBuffer, value: string) => {
    // Same pattern - always row 0, direct property access
    const internedIndex = categoryInterner.intern(value);
    buffer.attr_userId_values[0] = internedIndex;
    buffer.attr_userId_nulls[0] |= 1;
    return this;
  },
};

// Usage: ctx.tag writes to row 0
ctx.tag.requestId('req-123'); // Writes to attr_requestId_values[0]
ctx.tag.userId('user-456'); // Writes to attr_userId_values[0]
ctx.tag.requestId('req-789'); // OVERWRITES attr_requestId_values[0] - last write wins!
```

### ctx.scope - Separate Generated Class (NOT stored in buffer columns)

```typescript
// ctx.scope is a SEPARATE generated class that stores scope values
// Scope values are NOT stored in buffer columns - they're in the Scope instance
// See 01i_span_scope_attributes.md for full details

// Generated at module creation time (cold path) using new Function()
class GeneratedScope {
  // Private fields for each schema attribute (initialized to undefined)
  _userId = undefined;
  _requestId = undefined;

  // Getter - read scope value
  get userId() {
    return this._userId;
  }

  // Setter - set scope value (just sets property, NO buffer allocation)
  set userId(value) {
    this._userId = value;
  }

  get requestId() {
    return this._requestId;
  }
  set requestId(value) {
    this._requestId = value;
  }

  // Get all scope values for inheritance
  _getScopeValues() {
    return {
      userId: this._userId,
      requestId: this._requestId,
    };
  }
}

// Usage: ctx.scope uses property assignment syntax
ctx.scope.userId = 'user-123'; // Just sets _userId, NO TypedArray allocated
ctx.scope.requestId = 'req-456'; // Just sets _requestId, NO TypedArray allocated

// Reading scope values
const currentUserId = ctx.scope.userId; // Returns 'user-123' or undefined
if (ctx.scope.requestId !== undefined) {
  console.log('Request ID:', ctx.scope.requestId);
}
```

**Scope values are applied to buffer columns during:**

1. **Write operations** (`ctx.log.info()`) - scope values written to each row
2. **Pre-fill operations** (child span creation) - parent scope values fill child buffer via TypedArray.fill()
3. **Arrow conversion** (cold path) - scope values fill unallocated columns

### ctx.log - Appends New Rows (Events)

```typescript
// ctx.log creates NEW rows starting at row 2 - APPENDS
// Row 0 = span-start, Row 1 = completion, Row 2+ = events
// Column properties are DIRECT on buffer via lazy getters

const logOperations = {
  info: (buffer: SpanBuffer, message: string, scope: GeneratedScope, attributes?: Record<string, any>) => {
    // Append to current writeIndex (starts at 2)
    const index = buffer.writeIndex++;

    // Core columns - always written
    buffer.timestamps[index] = getTimestamp(buffer.task.requestContext);
    buffer.operations[index] = ENTRY_TYPE_INFO;

    // Message attribute via direct property access (triggers lazy allocation)
    const msgIndex = textStorage.store(message);
    buffer.attr_message_values[index] = msgIndex;
    const byteIdx = Math.floor(index / 8);
    const bitOffset = index % 8;
    buffer.attr_message_nulls[byteIdx] |= 1 << bitOffset;

    // Apply scope values (from Scope class, NOT stored in buffer)
    const scopeValues = scope._getScopeValues();
    for (const [fieldName, value] of Object.entries(scopeValues)) {
      if (value !== undefined) {
        const valuesKey = `attr_${fieldName}_values`;
        const nullsKey = `attr_${fieldName}_nulls`;
        buffer[valuesKey][index] = processValue(value, fieldName);
        buffer[nullsKey][byteIdx] |= 1 << bitOffset;
      }
    }

    // Optional attributes
    if (attributes) {
      writeAttributes(buffer, index, attributes);
    }

    return this;
  },
};

// Usage: ctx.log appends new rows
ctx.log.info('Processing...'); // Writes to row 2, writeIndex → 3
ctx.log.debug('Details...'); // Writes to row 3, writeIndex → 4
```

**Why This Approach**:

- **Datadog/OpenTelemetry alignment**: `ctx.tag` matches `span.set_tag()` and `Span.setAttribute()` semantics
- **No wasted rows**: Multiple tag calls don't bloat the buffer
- **Progressive enrichment**: Set initial values early, refine with more context later
- **Clear separation**: Tags (span attributes) vs logs (events) have distinct semantics
- **Separate Scope class**: Scope values stored separately, not mixed with buffer columns
- **Hot path optimization**: Direct TypedArray access via lazy getters
- **Null tracking**: Arrow-format null bitmaps (1=valid, 0=null)

## Self-Tuning Capacity Management

**Purpose**: Each module learns its optimal buffer size based on real usage patterns.

### Alternative Strategy: Single TypedArray per Data Type

**Concept**: Instead of one TypedArray per attribute, use one per data type:

```typescript
// Single buffer for all integer-like attributes
interface IntegerSpanBuffer extends SpanBuffer {
  integers: Uint32Array; // Sized as capacity * numIntegerAttributes
}

// Write to specific offset in the shared array
const index = buffer.writeIndex * NUM_INT_ATTRS;
buffer.integers[index + INT_ATTR_OFFSET.userId] = value;
buffer.integers[index + INT_ATTR_OFFSET.rowCount] = value;
```

**Tradeoffs**:

- **Pro**: Better memory locality if integer attributes are often used together
- **Con**: More complex offset calculations at runtime
- **Con**: Conversion to Arrow/Parquet might require buffer copies
- **Con**: Less intuitive for debugging

**Experiment Needed**: Benchmark single vs multiple TypedArrays for memory usage and CPU performance. The ability to
directly convert each attribute's TypedArray to an Arrow vector (no copying) is a major advantage of the multi-array
approach, especially if the background processor is a separate service (e.g., in Rust).

```typescript
function createModuleContext(config: { moduleMetadata: ModuleMetadata; tagAttributes: TagAttributeSchema }) {
  const moduleContext: ModuleContext = {
    moduleId: registerModule(config.moduleMetadata),
    gitSha: config.moduleMetadata.gitSha,
    filePath: config.moduleMetadata.filePath,

    // Initialize self-tuning capacity stats
    spanBufferCapacityStats: {
      /**
       * INITIAL CAPACITY: 64 elements chosen for cache alignment optimization
       *
       * RATIONALE FOR 64-ELEMENT START:
       * - Uint8Array: 64 × 1 = 64 bytes (exactly 1 cache line - optimal)
       * - Most small tasks fit within 64 operations without overflow
       * - Prevents dramatic memory inflation from cache alignment padding
       * - Self-tuning will adjust up/down based on actual usage patterns
       *
       * WHY 64 ELEMENTS:
       * Starting at 64 eliminates unexpected capacity inflations from cache alignment.
       * Smaller sizes (e.g., 16 elements) would get padded to 64 anyway for cache alignment,
       * causing unexpected memory increases (16 → 64 for Uint8Array = 4x waste).
       */
      currentCapacity: 64, // Start with cache-friendly size - most tasks fit in 64 operations
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };

  return { task: createTaskWrapper(moduleContext, config.tagAttributes) };
}

function appendToBuffer(buffer: SpanBuffer, data: any) {
  const originalBuffer = buffer;

  // Find the buffer with space (CPU branch predictor friendly)
  // Buffer chaining is part of self-tuning - handles overflow gracefully
  while (buffer.writeIndex >= buffer.timestamps.length) {
    buffer = !buffer.next ? createNextBuffer(buffer) : buffer.next;
  }

  // Hot path - always taken after loop
  const index = buffer.writeIndex++;

  // Count stats ONCE for self-tuning
  const stats = originalBuffer.task.module.spanBufferCapacityStats;
  stats.totalWrites++;
  if (buffer !== originalBuffer) {
    stats.overflowWrites++; // Went to a chained buffer (triggers tuning)
  }

  // Tune capacity if needed (see 01b2_buffer_self_tuning.md)
  shouldTuneCapacity(stats);

  // Write data (no branches) - direct TypedArray assignments
  buffer.timestamps[index] = data.timestamp;
  buffer.operations[index] = data.operation;
  // ... write other columns based on data.attributes and schema
}

// NOTE: Since each span has its own buffer, traceId and spanId are NOT written per row
// They're constant properties on the SpanBuffer itself, eliminating two TypedArray writes per operation

function shouldTuneCapacity(stats: ModuleContext['spanBufferCapacityStats']): boolean {
  const minSamples = 100; // Need enough data
  if (stats.totalWrites < minSamples) return false;

  const overflowRatio = stats.overflowWrites / stats.totalWrites;

  // Increase if >15% writes overflow
  if (overflowRatio > 0.15 && stats.currentCapacity < 1024) {
    const newCapacity = Math.min(stats.currentCapacity * 2, 1024);

    // Trace the tuning event as structured data!
    traceCapacityTuning({
      action: 'increase',
      oldCapacity: stats.currentCapacity,
      newCapacity,
      overflowRatio,
      totalWrites: stats.totalWrites,
      reason: 'high_overflow',
    });

    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }

  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && stats.totalBuffersCreated >= 10 && stats.currentCapacity > 8) {
    const newCapacity = Math.max(8, stats.currentCapacity / 2);

    traceCapacityTuning({
      action: 'decrease',
      oldCapacity: stats.currentCapacity,
      newCapacity,
      overflowRatio,
      totalWrites: stats.totalWrites,
      totalBuffers: stats.totalBuffersCreated,
      reason: 'low_utilization',
    });

    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }

  return false;
}

function resetStats(stats: ModuleContext['spanBufferCapacityStats']) {
  stats.totalWrites = 0;
  stats.overflowWrites = 0;
  stats.totalBuffersCreated = 0;
}
```

**Why This Design**:

- **Adaptive performance**: Each module learns its optimal buffer size
- **Memory efficient**: Starts small (64 entries), grows only when needed
- **Production ready**: Handles overflow gracefully with chaining
- **Bounded growth**: Won't grow beyond reasonable limits (8-1024 entries)
- **CPU friendly**: Linear search with ternary, no recursion
- **Self-documenting**: System traces its own tuning decisions

## Buffer Creation Optimization Phases

Understanding when allocations happen is critical for performance. The system is designed with distinct cold and hot
paths:

### Module Initialization (COLD PATH - once per module)

Heavy setup work happens once when a module is first loaded:

```typescript
// COLD PATH: All expensive operations happen here, ONCE per module
const moduleContext = createModuleContext({
  moduleMetadata: { name: 'my-service', filePath: __filename },
  tagAttributes: mySchema,
});

// What happens during module init:
// 1. SpanLogger class generation via new Function()
//    - Generates optimized class with schema-specific methods
//    - Creates prototype methods for each tag attribute
//    - Compiles enum mapping functions (switch statements)
// 2. Schema compilation
//    - Validates schema structure
//    - Pre-computes bitmap positions for each attribute
//    - Creates type mapping for TypedArray selection
// 3. Module registration
//    - Assigns unique moduleId
//    - Initializes capacity statistics
```

### Task Definition (COLD PATH - once per task)

Task context creation is also cold path, but lighter than module init:

```typescript
// COLD PATH: Happens once when defining a task
const processUser = moduleContext.task('processUser', async (ctx) => {
  // ...
});

// What happens during task definition:
// 1. TaskContext creation
//    - spanNameId = internString('processUser')  // One-time string interning
//    - Reference to module context (no copy)
// 2. Wrapper function creation
//    - Captures task context in closure
```

### Span Creation (HOT PATH - every span execution)

This is where performance matters most. Keep allocations minimal:

```typescript
// HOT PATH: Happens on EVERY call to processUser()
await processUser({ userId: '123' });

// What MUST happen (unavoidable):
// 1. Allocate SpanBuffer object
//    - Core columns: timestamps (Float64Array), operations (Uint8Array)
//    - Children array (empty)
//    - writeIndex = 0
// 2. Create SpanLogger instance
//    - Uses pre-generated class from cold path
//    - Single object allocation

// What MUST NOT happen:
// ❌ Object spreads: { ...parentContext, newField }  // Breaks V8 hidden classes
// ❌ New function creation: () => { ... }            // Function allocation
// ❌ String concatenation: prefix + name             // String allocation
// ❌ Array methods: fields.map(...)                  // Creates new array
```

### Per-Entry Write (HOT PATH - every log entry)

The hottest path. Zero allocations allowed:

```typescript
// HOTTEST PATH: Happens on EVERY tag/log call
ctx.tag.userId('123'); // ctx.tag directly on context

// What happens (lazy getters handle allocation):
buffer.timestamps[index] = performance.now(); // Float64 write
buffer.operations[index] = ENTRY_TYPE.TAG; // Uint8 write

// Accessing lazy getter triggers allocation on first access
buffer.attr_userId_values[index] = internedStringIndex; // Uint32 write (lazy alloc on first access)

// Set null bitmap bit (same ArrayBuffer as values)
const byteIdx = Math.floor(index / 8);
const bitOffset = index % 8;
buffer.attr_userId_nulls[byteIdx] |= 1 << bitOffset; // Uint8 bitwise OR (same lazy alloc)

index++;

// What MUST NOT happen:
// ❌ Creating objects: { timestamp, value }
// ❌ Array push: buffer.entries.push(...)
// ❌ String operations: value.trim(), value.toLowerCase()
// ❌ Conditionals that allocate: isValid ? new Error() : null
```

### Summary Table

| Phase           | Frequency       | Allocations Allowed  | Key Operations                         |
| --------------- | --------------- | -------------------- | -------------------------------------- |
| Module Init     | Once per module | Unlimited            | Class generation, schema compilation   |
| Task Definition | Once per task   | Minimal              | String interning, closure creation     |
| Span Creation   | Every execution | Buffer + logger only | TypedArray allocation, object creation |
| Per-Entry Write | Every log call  | **ZERO**             | Direct TypedArray writes only          |

## Background Processing Pipeline

**Purpose**: Convert columnar buffers to Apache Arrow RecordBatches and then to Parquet files.

### Copy Semantics and Performance

**Important Clarification**: The Arrow conversion is NOT zero-copy in all cases. Understanding the actual semantics:

**What IS Efficient (Minimal Copy)**:

- **Lazy columns that were never accessed**: Zero conversion cost - they don't exist
- **TypedArray slicing**: Creates a view for numeric types when using `subarray()`
- **Arrow dictionary encoding**: Strings are deduplicated, indices are compact

**What DOES Copy Data**:

- **`TypedArray.slice()`**: Creates a NEW ArrayBuffer with copied data
- **`TypedArray.subarray()`**: Creates a VIEW into the same ArrayBuffer (no copy)
- **Null bitmap transformation**: Converts our per-column format to Arrow's packed format
- **String resolution**: Category/text indices must be looked up in interners
- **Dictionary building**: Arrow builders accumulate values before finalizing

```typescript
// COPY: slice() creates new ArrayBuffer (avoid on hot path!)
const copied = buffer.timestamps.slice(0, buffer.writeIndex); // BAD for zero-copy

// VIEW: subarray() shares the underlying ArrayBuffer
const view = buffer.timestamps.subarray(0, buffer.writeIndex);

// IMPORTANT: For zero-copy Arrow conversion, use subarray()!
// subarray() creates a view, slice() copies - significant difference for large buffers
```

### Arrow Conversion Process

```typescript
import * as arrow from 'apache-arrow';

async function writeSpanBuffersToArrow(buffers: SpanBuffer[]) {
  // 1. Log a snapshot of capacity stats for system monitoring
  logCapacityStats(buffers);

  // 2. Create Arrow RecordBatches from SpanBuffers
  //    Uses buffer helper methods for checking allocation
  const recordBatches = buffers.map((buffer) => createRecordBatch(buffer, scope));

  // 3. Create Arrow Table from multiple RecordBatches
  const arrowTable = new arrow.Table(recordBatches);

  // 4. Write to Parquet
  await arrow.writeParquet(arrowTable, `traces-${Date.now()}.parquet`);
}

function createRecordBatch(buffer: SpanBuffer, scope: GeneratedScope): arrow.RecordBatch {
  const tagAttributes = buffer.task.module.tagAttributes;
  const arrowVectors: Record<string, arrow.Vector> = {};

  // --- Core SpanBuffer Columns ---
  // Span ID columns (separate columns, not a Struct)
  // threadId is constant per buffer, spanId is constant per buffer
  // Parent IDs derived from buffer.parent tree link
  arrowVectors.thread_id = createUint64Column(buffer.threadId, buffer.writeIndex);
  arrowVectors.span_id = createUint32Column(buffer.spanId, buffer.writeIndex);
  arrowVectors.parent_thread_id = buffer.parent
    ? createUint64Column(buffer.parent.threadId, buffer.writeIndex)
    : createNullUint64Column(buffer.writeIndex); // null for root spans
  arrowVectors.parent_span_id = buffer.parent
    ? createUint32Column(buffer.parent.spanId, buffer.writeIndex)
    : createNullUint32Column(buffer.writeIndex); // null for root spans

  // Timestamp and operation columns (zero-copy views)
  arrowVectors.timestamp = arrow.Float64Vector.from(
    buffer.timestamps.subarray(0, buffer.writeIndex) // subarray() = zero-copy view
  );
  arrowVectors.operation = arrow.Utf8Vector.from(buffer.operations.subarray(0, buffer.writeIndex));

  // Helpers for span ID column creation (separate columns, not Struct)
  function createUint64Column(value: bigint, length: number) {
    // threadId BigInt conversion happens once (not per row)
    return arrow.makeData({
      type: new arrow.Uint64(),
      length,
      data: new BigUint64Array(length).fill(value),
    });
  }

  function createUint32Column(value: number, length: number) {
    // spanId fills Uint32Array directly
    return arrow.makeData({
      type: new arrow.Uint32(),
      length,
      data: new Uint32Array(length).fill(value),
    });
  }

  function createNullUint64Column(length: number) {
    // All nulls for root spans
    const nullBitmap = new Uint8Array(Math.ceil(length / 8)); // All zeros = all null
    return arrow.makeData({
      type: new arrow.Uint64(),
      length,
      nullCount: length,
      data: new BigUint64Array(length),
      nullBitmap,
    });
  }

  function createNullUint32Column(length: number) {
    // All nulls for root spans
    const nullBitmap = new Uint8Array(Math.ceil(length / 8)); // All zeros = all null
    return arrow.makeData({
      type: new arrow.Uint32(),
      length,
      nullCount: length,
      data: new Uint32Array(length),
      nullBitmap,
    });
  }

  // --- Module Metadata Columns (expanded from shared reference) ---
  arrowVectors.gitSha = arrow.Utf8Vector.from(getModuleMetadataColumn(buffer, 'gitSha'));
  arrowVectors.filePath = arrow.Utf8Vector.from(getModuleMetadataColumn(buffer, 'filePath'));
  arrowVectors.functionName = arrow.Utf8Vector.from(
    getModuleMetadataColumn(buffer, 'functionNameId').map((id) => stringRegistry.get(id))
  );
  arrowVectors.lineNumber = arrow.Int32Vector.from(getModuleMetadataColumn(buffer, 'lineNumber'));

  // --- Attribute Columns (via lazy getters) ---
  // Get scope values for columns that weren't directly written
  const scopeValues = scope._getScopeValues();

  for (const [attrName, fieldConfig] of Object.entries(tagAttributes.fields)) {
    const columnName = `attr_${attrName}`;

    // Check if column was allocated (without triggering allocation)
    const values = buffer.getColumnIfAllocated(columnName);
    const nulls = buffer.getNullsIfAllocated(columnName);

    if (values) {
      // Column was allocated - use zero-copy subarray
      const arrowData = values.subarray(0, buffer.writeIndex);
      const arrowNullBitmap = nulls ? nulls.subarray(0, Math.ceil(buffer.writeIndex / 8)) : null;

      const arrowVector = createArrowVector(fieldConfig.type, arrowData, arrowNullBitmap);
      arrowVectors[attrName] = arrowVector; // Strip attr_ prefix for clean column names
    } else if (scopeValues[attrName] !== undefined) {
      // Column NOT allocated but scope has value
      // Allocate and fill NOW (cold path - only happens for scope-only columns)
      const value = scopeValues[attrName];
      const ArrayType = getArrayTypeForField(fieldConfig);
      const arr = new ArrayType(buffer.writeIndex);
      arr.fill(value);

      // Create full null bitmap (all values present)
      const neededBytes = Math.ceil(buffer.writeIndex / 8);
      const bitmap = new Uint8Array(neededBytes);
      bitmap.fill(0xff);
      const extraBits = buffer.writeIndex % 8;
      if (extraBits > 0) {
        bitmap[neededBytes - 1] &= (1 << extraBits) - 1;
      }

      const arrowVector = createArrowVector(fieldConfig.type, arr, bitmap);
      arrowVectors[attrName] = arrowVector;
    }
    // If neither allocated nor scope value: column omitted (zero cost)
  }

  return new arrow.RecordBatch(arrowVectors);
}

function logCapacityStats(buffers: SpanBuffer[]) {
  const seenModules = new Set<number>();

  for (const buffer of buffers) {
    const moduleId = buffer.task.module.moduleId;

    if (!seenModules.has(moduleId)) {
      seenModules.add(moduleId);

      const moduleStats = buffer.task.module.spanBufferCapacityStats;
      const efficiency = moduleStats.totalWrites / (moduleStats.totalBuffersCreated * moduleStats.currentCapacity);
      const overflowRatio = moduleStats.overflowWrites / moduleStats.totalWrites;

      systemTracer.tag.capacityStats({
        moduleId: moduleId.toString(),
        currentCapacity: moduleStats.currentCapacity,
        totalWrites: moduleStats.totalWrites,
        overflowWrites: moduleStats.overflowWrites,
        totalBuffers: moduleStats.totalBuffersCreated,
        efficiency,
        overflowRatio,
        timestamp: Date.now(),
      });
    }
  }
}
```

## Arrow Conversion Integration

**Purpose**: The columnar buffer architecture enables efficient conversion to Apache Arrow format.

**Key Integration Points**:

1. **Buffer helper methods**: Use `getColumnIfAllocated()` and `getNullsIfAllocated()` to check allocation without
   triggering it
2. **TypedArray subarray()**: Core columns and allocated attribute columns use subarray() for zero-copy views
3. **Scope Value Handling**: Scope-only columns (from Scope class) allocate and fill during cold path (not hot path)
4. **Attribute Prefix Stripping**: `attr_` prefixes removed during conversion for clean column names
5. **String Registry Resolution**: Category/text indices resolve to actual strings during conversion

**Copy vs View Semantics**:

```typescript
// IMPORTANT: Use subarray() for zero-copy conversion
const view = buffer.timestamps.subarray(0, writeIndex); // Zero-copy VIEW into same ArrayBuffer

// Check if column was allocated without triggering allocation
const values = buffer.getColumnIfAllocated('attr_userId');
if (values) {
  // Column was allocated - use zero-copy subarray
  const arrowData = values.subarray(0, writeIndex);
}

// If column wasn't allocated but scope has value:
// - Allocate new array and fill (cold path allocation)
// If neither allocated nor scope value:
// - Column omitted entirely (zero cost)
```

**Example Conversion**:

```typescript
// SpanBuffer with lazy getters (in-memory)    →  Arrow Table (queryable)
// Column properties are DIRECT on buffer (not nested in .columns)
buffer.attr_http_status_values[0]              →  http_status: 200
buffer.attr_db_query_values[0]                 →  db_query: "SELECT * FROM users WHERE id = ?"
buffer.attr_http_status_nulls                  →  Arrow validity bitmap: 0x01
// Column allocated but not written → subarray() with existing null bitmap
// Column not allocated + scope value → allocate + fill at conversion (cold path)
// Column not allocated + no scope value → Column omitted entirely (zero cost)
```

**See Also**:

- **[Span Scope Attributes](./01i_span_scope_attributes.md)**: Scope class definition and scope value handling
- **Arrow Table Structure** (future document): Complete Arrow schema, examples with realistic trace data, ClickHouse
  query patterns
- **Background Processing Pipeline** (future document): Detailed Arrow/Parquet conversion process, performance
  optimizations

This columnar architecture ensures that the high-performance buffer operations flow into efficient analytical storage
and querying.
