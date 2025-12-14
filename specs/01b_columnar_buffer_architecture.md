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

// Legacy example with 16 elements (shows why we increased initial capacity):
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

**Memory Optimization**: Attribute columns (`attr_*`) are created lazily using `Object.defineProperty` getters,
allocating memory only when the column is first accessed. This provides significant memory savings for sparse columns.

### Why Lazy Initialization

1. **Sparse Columns are Free**: If a tag attribute is defined in the schema but never used in a particular span, no
   memory is allocated for that column at all.

2. **Matches Arrow Semantics**: Arrow uses per-column null bitmaps. If a column is never written, there's nothing to
   convert - it simply doesn't exist in the output.

3. **Schema Flexibility**: Modules can define comprehensive schemas with many optional attributes without paying memory
   cost for unused ones.

### What Gets Allocated When

**Immediately Allocated (Core Columns)**:

- `timestamps: Float64Array` - Every entry has a timestamp
- `operations: Uint8Array` - Every entry has an operation/entry type

**Lazily Allocated (Attribute Columns)**:

- `attr_*` columns - Only allocated on first property access
- Per-column null bitmaps - Only allocated when their column is accessed

### Implementation Pattern

```typescript
// From createColumnBuffer.ts
function createColumnBuffer(schema: TagAttributeSchema, requestedCapacity: number = 64): ColumnBuffer {
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity);

  // Core columns are ALWAYS allocated - they're used for every entry
  const timestamps = new Float64Array(alignedCapacity);
  const operations = new Uint8Array(alignedCapacity);

  // Storage for lazily-initialized attribute columns
  const lazyColumnStorage: Record<`attr_${string}`, { nulls?: Uint8Array; data?: TypedArray }> = {};

  const buffer: ColumnBuffer = {
    timestamps,
    operations,
    nullBitmaps: {}, // Empty object, getters added below
    writeIndex: 0,
    capacity: requestedCapacity,
    next: undefined,
  };

  // Define lazy getters for each attribute column
  for (const fieldName of Object.keys(schema)) {
    const columnName = `attr_${fieldName}` as `attr_${string}`;

    // Initialize empty storage - no TypedArray allocated yet
    lazyColumnStorage[columnName] = {};

    // Getter creates TypedArray on first access
    Object.defineProperty(buffer, columnName, {
      get() {
        const storage = lazyColumnStorage[columnName];
        if (!storage.data) {
          // LAZY: Only now do we allocate the TypedArray
          storage.data = getArrayConstructorForField(schema, fieldName, alignedCapacity);
        }
        return storage.data;
      },
      enumerable: true,
      configurable: true,
    });

    // Same pattern for null bitmaps
    Object.defineProperty(buffer.nullBitmaps, columnName, {
      get() {
        const storage = lazyColumnStorage[columnName];
        if (!storage.nulls) {
          storage.nulls = new Uint8Array(Math.ceil(alignedCapacity / 8));
        }
        return storage.nulls;
      },
      enumerable: true,
      configurable: true,
    });
  }

  return buffer;
}
```

### Memory Impact Example

Consider a schema with 20 attributes where a typical span only uses 3:

```typescript
// Without lazy initialization:
// 20 columns × 64 elements × 4 bytes/element = 5,120 bytes per span

// With lazy initialization:
// 2 core columns (timestamps + operations) = 576 bytes (always)
// 3 used attr columns × 64 × 4 = 768 bytes (on demand)
// Total: 1,344 bytes per span (74% memory savings!)
```

## Base SpanBuffer Interface

**Purpose**: Provide a generic interface that can be extended with schema-generated columns.

```typescript
interface SpanBuffer {
  // Core columns - always present (allocated immediately)
  timestamps: Float64Array; // Every operation appends timestamp
  operations: Uint8Array; // Operation type: tag, ok, err, etc.

  // Null bitmaps - lazily allocated per column
  nullBitmaps: Record<`attr_${string}`, Uint8Array>; // Arrow-format bitmaps

  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer; // Reference to parent SpanBuffer
  task: TaskContext; // Reference to task + module metadata

  // Buffer management
  writeIndex: number; // Current write position (0 to capacity-1)
  next?: SpanBuffer; // Chain to next buffer when overflow (part of self-tuning)

  spanId: number; // Incremental ID for THIS SpanBuffer (assigned at creation)
  traceId: string; // Root trace ID (constant per span, no TypedArray needed)

  // Attribute columns: attr_* properties are added via lazy getters
  // They appear as TypedArrays but are only allocated on first access

  // NOTE: Each span gets its own buffer, so traceId and spanId are constant
  // No need for traceId/spanId TypedArrays - they're the same for every row in this buffer
  // This keeps logs sorted and enables efficient Arrow conversion
}
```

**Why This Design**:

- **Per-span buffers**: Each span gets its own buffer for sorted logs and simple implementation
- **No traceId/spanId arrays**: These are constant per buffer, stored as properties
- **Minimal interface**: Only essential fields, no capacity/length bloat
- **Shared references**: Module context shared across all tasks
- **Tree structure**: Efficient parent-child span relationships
- **Buffer chaining**: Handle overflow with linked buffers (part of self-tuning mechanism)
- **Freelist consideration**: May keep pool of buffers if long-lived TypedArrays help V8's GC

## Schema-Generated Buffer Extensions

**Purpose**: Extend the base interface with typed columns based on tag attribute schemas.

**See Also**: [Trace Schema System](./01a_trace_schema_system.md) for how these schemas are defined.

```typescript
// Generated from composed schema (HTTP + DB + user attributes)
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

// Bitmap sizing based on total attribute count
const COMPOSED_ATTR_BITS = {
  http_status: 0, // Maps to attr_http_status
  http_method: 1, // Maps to attr_http_method
  http_url: 2, // Maps to attr_http_url
  http_duration: 3, // Maps to attr_http_duration
  db_query: 4, // Maps to attr_db_query
  db_duration: 5, // Maps to attr_db_duration
  db_rows: 6, // Maps to attr_db_rows
  db_table: 7, // Maps to attr_db_table
  user_id: 8, // Maps to attr_user_id
  business_metric: 9, // Maps to attr_business_metric
  // 10 attributes → Uint16Array bitmap (16 bits available)
};

function createSpanBuffer<T extends TagAttributeSchema>(
  schema: T,
  taskContext: TaskContext,
  parentBuffer?: SpanBuffer // Optional parent buffer for tree linking
): SpanBuffer {
  const spanId = nextGlobalSpanId++; // Assign unique ID at creation
  return createEmptySpanBuffer(spanId, schema, taskContext, parentBuffer);
}

function createEmptySpanBuffer<T extends TagAttributeSchema>(
  spanId: number,
  schema: T,
  taskContext: TaskContext,
  parentBuffer: SpanBuffer
): SpanBuffer {
  /**
   * Cache line alignment utility - ensures TypedArrays are aligned to 64-byte boundaries
   *
   * DESIGN RATIONALE:
   * - CPU cache lines are 64 bytes on most modern processors (x86, ARM)
   * - Aligning arrays to cache line boundaries reduces cache misses and improves memory bandwidth
   * - Vectorized operations (SIMD) perform better on aligned data
   * - Prevents false sharing between different arrays in multi-threaded scenarios
   *
   * @param elementCount - Number of elements requested
   * @param bytesPerElement - Size of each element in bytes
   * @returns Element count rounded up to nearest cache line boundary
   */
  function getCacheAlignedCapacity(elementCount: number, bytesPerElement: number): number {
    const CACHE_LINE_SIZE = 64; // Cache line size in bytes (standard for x86/ARM)
    const totalBytes = elementCount * bytesPerElement;
    const alignedBytes = Math.ceil(totalBytes / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
    return Math.ceil(alignedBytes / bytesPerElement);
  }

  // Choose smallest bitmap type that fits all attributes
  let BitmapType: Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor;
  const attributeCount = Object.keys(schema.fields).length;
  if (attributeCount <= 8) {
    BitmapType = Uint8Array; // 8 bits
  } else if (attributeCount <= 16) {
    BitmapType = Uint16Array; // 16 bits
  } else if (attributeCount <= 32) {
    BitmapType = Uint32Array; // 32 bits
  } else {
    throw new Error(`Too many attributes: ${attributeCount}. Maximum 32 supported.`);
  }

  const requestedCapacity = taskContext.module.spanBufferCapacityStats.currentCapacity;

  /**
   * CRITICAL DESIGN CONSTRAINT: Columnar Storage Equal Length Requirement
   *
   * ALL TypedArrays in a SpanBuffer MUST have exactly the same length to maintain
   * columnar storage invariants. This enables:
   * - Direct indexing: buffer.timestamps[i], buffer.operations[i], buffer.attr_userId[i] all refer to the same row
   * - Zero-copy Arrow conversion: slicing arrays directly to Arrow vectors
   * - Consistent null bitmap indexing: buffer.nullBitmap[i] tracks nulls for row i across all attributes
   *
   * CACHE ALIGNMENT STRATEGY:
   * We calculate alignment using the SMALLEST element size (1 byte = Uint8Array) as the worst case.
   * This ensures ALL array types are cache-aligned (or over-aligned):
   *
   * With 64-element capacity:
   * - Uint8Array:   64 × 1 = 64 bytes   (exactly 1 cache line - optimal)
   * - Uint16Array:  64 × 2 = 128 bytes  (exactly 2 cache lines - optimal)
   * - Uint32Array:  64 × 4 = 256 bytes  (exactly 4 cache lines - optimal)
   * - Float64Array: 64 × 8 = 512 bytes  (exactly 8 cache lines - optimal)
   *
   * MEMORY TRADE-OFF:
   * Using 1-byte alignment may over-allocate for larger types, but:
   * 1. Preserves equal length requirement (fundamental)
   * 2. Ensures cache alignment for smallest arrays (performance critical)
   * 3. Starting capacity of 64 elements minimizes unexpected size increases
   * 4. Self-tuning capacity management adapts to actual usage patterns
   */
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity, 1);

  const buffer = {
    spanId,

    /**
     * CORE COLUMNS: All use identical cache-aligned capacity
     *
     * timestamps: Float64Array - High-precision timestamps for every operation
     * operations: Uint8Array - Operation type codes (tag, ok, err, etc.)
     * nullBitmap: Variable type - Bit flags tracking which attributes have values per row
     *
     * EQUAL LENGTH GUARANTEE: All arrays have length = alignedCapacity
     * This enables direct row-based indexing across all columns.
     */
    timestamps: new Float64Array(alignedCapacity),
    operations: new Uint8Array(alignedCapacity),
    nullBitmap: new BitmapType(alignedCapacity),

    /**
     * SCHEMA-GENERATED ATTRIBUTE COLUMNS
     *
     * Generated dynamically based on TagAttributeSchema, all using the same alignedCapacity.
     * Each attribute gets its own TypedArray with 'attr_' prefix to prevent naming conflicts.
     *
     * Examples:
     * - attr_http_status: Uint16Array(alignedCapacity)  // HTTP status codes
     * - attr_user_id: Uint32Array(alignedCapacity)      // String registry indices
     * - attr_duration: Float32Array(alignedCapacity)    // Timing measurements
     */
    ...generateAttributeColumns(schema, alignedCapacity),

    // Tree structure
    children: [],
    parent: parentBuffer, // Set parent reference
    task: taskContext,

    /**
     * BUFFER MANAGEMENT
     *
     * writeIndex: Current write position (0 to capacity-1)
     * capacity: LOGICAL capacity for bounds checking (original requested size)
     * next: Chain to next buffer when logical capacity is exceeded
     *
     * IMPORTANT: Physical array length (alignedCapacity) may be larger than logical capacity
     * due to cache alignment. The writeIndex and overflow logic use the original capacity
     * to maintain consistent buffer chaining behavior regardless of alignment padding.
     *
     * Example:
     * - Requested capacity: 100 elements
     * - Aligned capacity: 128 elements (due to cache alignment)
     * - Logical capacity: 100 (used for overflow detection)
     * - Array lengths: 128 (actual memory allocation)
     */
    writeIndex: 0,
    capacity: requestedCapacity, // Keep original capacity for logical bounds checking
    next: undefined, // Chain to next buffer when full
  };

  // Define lazy getters for each schema attribute
  for (const fieldName of Object.keys(schema.fields)) {
    const columnName = `attr_${fieldName}` as `attr_${string}`;
    lazyColumnStorage[columnName] = {};

    // Lazy getter for attribute column
    Object.defineProperty(buffer, columnName, {
      get() {
        const storage = lazyColumnStorage[columnName];
        if (!storage.data) {
          storage.data = createTypedArrayForField(schema.fields[fieldName], alignedCapacity);
        }
        return storage.data;
      },
      enumerable: true,
      configurable: true,
    });

    // Lazy getter for null bitmap
    Object.defineProperty(buffer.nullBitmaps, columnName, {
      get() {
        const storage = lazyColumnStorage[columnName];
        if (!storage.nulls) {
          storage.nulls = new Uint8Array(Math.ceil(alignedCapacity / 8));
        }
        return storage.nulls;
      },
      enumerable: true,
      configurable: true,
    });
  }

  taskContext.module.spanBufferCapacityStats.totalBuffersCreated++;
  return buffer;
}

// Global span ID counter
let nextGlobalSpanId = 1;

function createNextBuffer(buffer: SpanBuffer): SpanBuffer {
  // Buffer chaining is part of the self-tuning mechanism (see 01b2_buffer_self_tuning.md)
  // When a buffer overflows, we chain to a new buffer for the SAME logical span
  // The chained buffer inherits spanId and uses the same task context
  return createEmptySpanBuffer(
    buffer.spanId, // Same logical span
    getSchemaFromBuffer(buffer), // Re-use schema
    buffer.task, // Re-use task context (includes traceId)
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
  // This keeps child logs separate from parent logs in Arrow output
  const childBuffer = createSpanBuffer(
    getSchemaFromBuffer(parentBuffer), // Child inherits parent's schema
    childTaskContext,
    parentBuffer // Set parent reference
  );

  // Link parent-child relationship in tree
  parentBuffer.children.push(childBuffer);

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

const tagOperations = {
  requestId: (buffer: DbSpanBuffer, value: string) => {
    // ALWAYS write to row 0 (span-start row) - overwrite semantics
    const index = 0;

    // Set bit for this attribute in null bitmap (row 0)
    buffer.nullBitmaps.attr_requestId[0] |= 1 << 0;

    // Write to THIS attribute's column at row 0
    buffer.attr_requestId[index] = hashString(value);

    // Return this for chaining
    return this;
  },

  userId: (buffer: DbSpanBuffer, value: string) => {
    // Same pattern - always row 0
    const index = 0;
    buffer.nullBitmaps.attr_userId[0] |= 1 << 0;
    buffer.attr_userId[index] = hashString(value);
    return this;
  },
};

// Usage: ctx.tag writes to row 0
ctx.tag.requestId('req-123'); // Writes to attr_requestId[0]
ctx.tag.userId('user-456'); // Writes to attr_userId[0]
ctx.tag.requestId('req-789'); // OVERWRITES attr_requestId[0] - last write wins!
```

### ctx.log - Appends New Rows (Events)

```typescript
// ctx.log creates NEW rows starting at row 2 - APPENDS
// Row 0 = span-start, Row 1 = completion, Row 2+ = events

const logOperations = {
  info: (buffer: SpanBuffer, message: string, attributes?: Record<string, any>) => {
    // Append to current writeIndex (starts at 2)
    const index = buffer.writeIndex++;

    // Core columns - always written
    buffer.timestamps[index] = getTimestamp(buffer.task.requestContext);
    buffer.operations[index] = ENTRY_TYPE_INFO;
    buffer.messages[index] = message;

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
- **Hot path optimization**: Just TypedArray writes and bitwise operations
- **Null tracking**: Bitmap efficiently tracks which attributes have values at row 0

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
       * HISTORICAL NOTE:
       * Previously started at 16 elements, but cache alignment caused:
       * - Uint8Array: 16 → 64 elements (4x memory increase!)
       * - Uint16Array: 16 → 32 elements (2x memory increase!)
       * Starting at 64 eliminates these unexpected capacity inflations.
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

// What happens (all direct writes, no allocations):
buffer.timestamps[index] = performance.now(); // Float64 write
buffer.operations[index] = ENTRY_TYPE.TAG; // Uint8 write
buffer.attr_userId[index] = internedStringIndex; // Uint32 write
buffer.nullBitmaps.attr_userId[byteIdx] |= bitmask; // Uint8 bitwise OR
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
// COPY: slice() creates new ArrayBuffer
const copied = buffer.timestamps.slice(0, buffer.writeIndex);

// VIEW: subarray() shares the underlying ArrayBuffer
const view = buffer.timestamps.subarray(0, buffer.writeIndex);

// Current implementation uses slice() for safety (cold path, correctness > micro-optimization)
// Future optimization: use subarray() with careful lifetime management
```

### Arrow Conversion Process

```typescript
import * as arrow from 'apache-arrow';

async function writeSpanBuffersToArrow(buffers: SpanBuffer[]) {
  // 1. Log a snapshot of capacity stats for system monitoring
  logCapacityStats(buffers);

  // 2. Create Arrow RecordBatches from SpanBuffers
  //    Uses Arrow builders for correctness and null handling
  const recordBatches = buffers.map((buffer) => createRecordBatch(buffer));

  // 3. Create Arrow Table from multiple RecordBatches
  const arrowTable = new arrow.Table(recordBatches);

  // 4. Write to Parquet
  await arrow.writeParquet(arrowTable, `traces-${Date.now()}.parquet`);
}

function createRecordBatch(buffer: SpanBuffer): arrow.RecordBatch {
  const tagAttributes = buffer.task.module.tagAttributes;
  const arrowVectors: Record<string, arrow.Vector> = {};

  // --- Core SpanBuffer Columns ---
  // These are sliced from TypedArrays (creates copies for safety)
  arrowVectors.spanId = arrow.Int64Vector.from(generateSpanIds(buffer));
  arrowVectors.parentId = arrow.Int64Vector.from(generateParentIds(buffer));
  arrowVectors.timestamp = arrow.Float64Vector.from(
    buffer.timestamps.slice(0, buffer.writeIndex) // slice() = copy
  );
  arrowVectors.operation = arrow.Utf8Vector.from(buffer.operations.slice(0, buffer.writeIndex));

  // --- Module Metadata Columns (expanded from shared reference) ---
  arrowVectors.gitSha = arrow.Utf8Vector.from(getModuleMetadataColumn(buffer, 'gitSha'));
  arrowVectors.filePath = arrow.Utf8Vector.from(getModuleMetadataColumn(buffer, 'filePath'));
  arrowVectors.functionName = arrow.Utf8Vector.from(
    getModuleMetadataColumn(buffer, 'functionNameId').map((id) => stringRegistry.get(id))
  );
  arrowVectors.lineNumber = arrow.Int32Vector.from(getModuleMetadataColumn(buffer, 'lineNumber'));

  // --- Attribute Columns ---
  for (const [attrName, fieldConfig] of Object.entries(tagAttributes.fields)) {
    const columnName = `attr_${attrName}` as `attr_${string}`;

    // Check if column was ever accessed (lazy initialization)
    // If the getter was never called, the column doesn't exist - skip it!
    const rawColumnData = Object.getOwnPropertyDescriptor(buffer, columnName)?.value;
    if (!rawColumnData) {
      // Column was never written to - no conversion needed
      // This is where lazy initialization saves work
      continue;
    }

    // Get null bitmap for this column
    const nullBitmap = buffer.nullBitmaps[columnName];
    const slicedData = rawColumnData.slice(0, buffer.writeIndex);

    // Convert null bitmap from our format to Arrow's format
    const arrowNullBitmap = convertNullBitmapToArrowFormat(nullBitmap, buffer.writeIndex);

    // Create Arrow Vector with null bitmap
    const arrowVector = createArrowVector(fieldConfig.type, slicedData, arrowNullBitmap);
    arrowVectors[attrName] = arrowVector; // Strip attr_ prefix for clean column names
  }

  return new arrow.RecordBatch(arrowVectors);
}

/**
 * Convert our null bitmap format to Arrow's format
 *
 * Our format: 1 bit per row, per column (set = value present)
 * Arrow format: 1 bit per row (set = value valid/non-null)
 *
 * This is NOT zero-copy - we must transform the bitmap
 */
function convertNullBitmapToArrowFormat(nullBitmap: Uint8Array | undefined, numRows: number): Uint8Array {
  const arrowBitmap = new Uint8Array(Math.ceil(numRows / 8));

  if (!nullBitmap) {
    // No bitmap means all values are null
    return arrowBitmap;
  }

  // Copy bits from our format to Arrow format
  // (In practice, formats are similar so this is mostly a copy)
  for (let i = 0; i < numRows; i++) {
    const byteIndex = Math.floor(i / 8);
    const bitOffset = i % 8;
    const isPresent = (nullBitmap[byteIndex] & (1 << bitOffset)) !== 0;

    if (isPresent) {
      arrowBitmap[byteIndex] |= 1 << bitOffset;
    }
  }

  return arrowBitmap;
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

1. **Lazy Columns**: Columns never accessed have zero conversion cost - they simply don't exist
2. **TypedArray Slicing**: Core numeric columns slice efficiently from source TypedArrays
3. **Null Bitmap Transformation**: Per-column bitmaps convert to Arrow's validity format
4. **Attribute Prefix Stripping**: `attr_` prefixes removed during conversion for clean column names
5. **String Registry Resolution**: Category/text indices resolve to actual strings during conversion

**Copy vs View Semantics**:

```typescript
// Current implementation (safe, some copying):
const data = buffer.timestamps.slice(0, writeIndex); // Creates copy

// Potential optimization (view, careful lifetime management needed):
const view = buffer.timestamps.subarray(0, writeIndex); // Shares buffer

// Arrow conversion uses builders which copy data anyway,
// so slice() vs subarray() is less important in practice
```

**Example Conversion**:

```typescript
// SpanBuffer (in-memory)                    →  Arrow Table (queryable)
buffer.attr_http_status: Uint16Array[200]   →  http_status: 200
buffer.attr_db_query: Array<string>["..."]  →  db_query: "SELECT * FROM users WHERE id = ?"
buffer.nullBitmaps.attr_http_status[0]=0x01 →  Arrow validity bitmap: 0x01
// Column never accessed (lazy getter)      →  Column omitted entirely (zero cost)
```

**See Also**:

- **Arrow Table Structure** (future document): Complete Arrow schema, examples with realistic trace data, ClickHouse
  query patterns
- **Background Processing Pipeline** (future document): Detailed Arrow/Parquet conversion process, performance
  optimizations

This columnar architecture ensures that the high-performance buffer operations flow into efficient analytical storage
and querying.
