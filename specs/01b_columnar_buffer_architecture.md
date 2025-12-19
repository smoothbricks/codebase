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
  - span-ok (2):        Success, written by ctx.ok()
  - span-err (3):       Expected business error, written by ctx.err()
  - span-exception (4): Uncaught exception, pre-initialized at row 1
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
ctx.tag.userId('user-123'); // Writes to userId[0]
ctx.tag.requestId('req-456'); // Writes to requestId[0]
ctx.tag.userId('user-999'); // Overwrites userId[0] - last write wins

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

See **[High-Precision Timestamps](./01b3_high_precision_timestamps.md)** for:

- Nanosecond precision via anchored timestamps (`BigInt64Array`)
- Browser vs Node.js implementations
- Date.now() usage guidelines (only for scheduling, NOT for trace timestamps)
- Arrow conversion format (`TimestampNanosecond`)

## Cache Line Alignment and Capacity

See **[Buffer Performance Optimizations](./01b1_buffer_performance_optimizations.md#memory-layout-optimization)** for:

- 64-byte cache line alignment strategy
- Initial capacity selection (8 elements default)
- Multiple-of-8 constraint for null bitmap alignment
- Equal length constraint for all TypedArrays
- Performance impact (10-30% improvement)

## Lazy Column Initialization

**Memory Optimization**: Attribute columns use **lazy getters** that allocate shared ArrayBuffers on first access. This
provides significant memory savings for sparse columns.

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

- `timestamps: BigInt64Array` - Every entry has a timestamp
- `operations: Uint8Array` - Every entry has an operation/entry type

**Lazily Allocated (via Lazy Getters)**:

- `X_values: TypedArray` - Allocated on first getter access
- `X_nulls: Uint8Array` - Same allocator as values (shares ArrayBuffer)
- Both use the SAME underlying ArrayBuffer, partitioned with cache alignment

**Scope Values** (SEPARATE from buffer):

- Stored in Scope class instances (see 01i_span_scope_attributes.md)
- NOT stored in buffer columns
- Applied to buffer columns during writes or pre-fill operations

### Lazy Getter Implementation (Generated via `new Function()`)

The actual implementation generates a concrete SpanBuffer class with lazy getters for each column:

```typescript
// Pseudo-code: Generated at module creation time (cold path)
// PACKAGE: arrow-builder provides generateColumnBufferClass()
// PACKAGE: lmao extends with span-specific properties via ColumnBufferExtension
// Example for userId attribute (category type, Uint32Array):

class GeneratedColumnBuffer {
  constructor(requestedCapacity) {
    // Initialize system columns (always allocated)
    this._timestamps = new BigInt64Array(alignedCapacity);
    this._operations = new Uint8Array(alignedCapacity);

    // Lazy columns initialized as undefined (not allocated yet)
    this._userId_nulls = undefined;
    this._userId_values = undefined;
  }

  // Lazy getter - allocates BOTH nulls and values on first access
  get userId_nulls() {
    if (this._userId_nulls === undefined) {
      // Allocate shared ArrayBuffer for nulls + values
      // Calculate aligned sizes for cache line optimization
      // Create TypedArray views into shared buffer
      // Set both _userId_nulls and _userId_values
    }
    return this._userId_nulls;
  }

  // Values getter triggers allocation via nulls getter
  get userId_values() {
    if (this._userId_values === undefined) this.userId_nulls; // Trigger allocation
    return this._userId_values;
  }

  // Setter method writes to TypedArrays and updates null bitmap
  userId(pos, val) {
    if (val == null) {
      // Clear null bit (mark as null)
    } else {
      // Set value and mark as valid in null bitmap
    }
    return this;
  }

  // Helpers to check allocation without triggering it
  getColumnIfAllocated(columnName) {
    return this[`_${columnName}_values`];
  }
}
```

**Key Design Points**:

1. **Shared ArrayBuffer**: Each column uses ONE ArrayBuffer for both nulls and values
2. **Cache Alignment**: Null bitmap end is aligned to value type's element size
3. **Direct Properties**: Uses `this._userId_nulls` and `this._userId_values` (not Symbols)
4. **Inline Allocation**: Allocation happens directly in the getter, not via separate allocator function
5. **Zero Indirection**: Direct property access (`buffer.userId_values[i]`)
6. **V8 Optimization**: Generated classes optimize well with hidden classes and inline caching

### Memory Impact Example

Consider a schema with 20 attributes where a typical span only uses 3:

```typescript
// Without lazy initialization (ALL columns allocated):
// 2 core columns: BigInt64Array(64) + Uint8Array(64) = 576 bytes
// 20 attr columns: 20 × (Uint32Array(64) + null bitmap) = 20 × (256 + 8) = 5,280 bytes
// Total: 5,856 bytes per span

// With lazy getters (ONLY used columns allocated):
// 2 core columns: BigInt64Array(64) + Uint8Array(64) = 576 bytes (always)
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

**PACKAGE**: This interface is defined in **lmao** (`packages/lmao/src/lib/types.ts`). It extends arrow-builder's
`TypedColumnBuffer` with span-specific properties (tree structure, identity, context).

**CRITICAL**: Column properties are **direct properties** on the SpanBuffer via lazy getters (no nested
`columns: Record<...>`). This design provides zero indirection for hot path access.

```typescript
// PACKAGE: lmao - SpanBuffer extends arrow-builder's TypedColumnBuffer
interface SpanBuffer {
  // Core columns - always present (allocated immediately in constructor)
  timestamps: BigInt64Array; // Every operation appends timestamp (nanoseconds)
  operations: Uint8Array; // Operation type: tag, ok, err, etc.
  lineNumber_values: Int32Array; // Line numbers for each entry
  lineNumber_nulls: Uint8Array; // Null bitmap for line numbers

  // Attribute columns - DIRECT PROPERTIES with LAZY GETTERS (no nested Record!)
  // Each attribute has TWO properties sharing ONE ArrayBuffer:
  // - X_nulls: Uint8Array for null bitmap (Arrow format: 1=valid, 0=null)
  // - X_values: TypedArray for actual values
  // Schema-generated via new Function() at module creation time
  userId_nulls: Uint8Array; // Lazy getter
  userId_values: Uint32Array; // Lazy getter
  userId: Uint32Array; // Shorthand getter (alias for _values)

  requestId_nulls: Uint8Array;
  requestId_values: Uint32Array;
  requestId: Uint32Array;

  http_status_nulls: Uint8Array; // Prefixed library column (optional prefix for conflict avoidance)
  http_status_values: Uint16Array;
  http_status: Uint16Array;
  // ... same pattern for all schema attributes

  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer; // Reference to parent SpanBuffer (child spans walk this for traceId)

  // Dual module references for accurate source attribution
  callsiteModule?: ModuleContext; // Caller's module (where span() was invoked) - for row 0's gitSha/packageName/packagePath
  module: ModuleContext; // Op's module (what code is executing) - for rows 1+ gitSha/packageName/packagePath
  spanName: string; // Span name for this invocation
  // NOTE: lineNumber is in lineNumber_values TypedArray, NOT a property on SpanBuffer

  // Buffer management
  writeIndex: number; // Current write position (0 to capacity-1)
  capacity: number; // Logical capacity for bounds checking
  next?: SpanBuffer; // Chain to next buffer when overflow (part of self-tuning)

  // Context (flattened from TaskContext)
  module: ModuleContext; // Op's module (for rows 1+ metadata)
  spanName: string; // Span name for this invocation
  callsiteModule?: ModuleContext; // Caller's module (for row 0 metadata)

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

// ModuleContext - module-level metadata with flattened stats
interface ModuleContext {
  packageName: string; // npm package name from package.json
  packagePath: string; // Path within package, relative to package.json
  gitSha: string; // Git SHA at build time
  prefix?: string; // Optional prefix for library integration
  logSchema: LogSchema; // Schema definition for this module

  // Self-tuning buffer capacity stats (flattened, not nested)
  sb_capacity: number; // Current buffer capacity
  sb_totalWrites: number; // Total writes for tuning decisions
  sb_overflows: number; // Overflow count for tuning decisions
  sb_totalCreated: number; // Total buffers created
}

// Access patterns from buffer:
// Module metadata (row 0 uses callsiteModule, rows 1+ use module)
buffer.callsiteModule?.packageName; // Caller's '@mycompany/http' (for row 0)
buffer.callsiteModule?.gitSha; // Caller's 'abc123' (for row 0)
buffer.module.packageName; // Op's '@mycompany/http' (for rows 1+)
buffer.module.gitSha; // Op's 'abc123' (for rows 1+)
buffer.spanName; // Span name (direct property)
buffer.module.gitSha; // Op's 'abc123' (for rows 1+)

// Self-tuning stats (flattened on module)
buffer.module.sb_capacity; // 8 (default, self-tuning adapts)
buffer.module.sb_totalWrites; // 1234

// Per-span invocation data (direct properties)
buffer.spanName; // 'fetchData'
buffer.lineNumber_values[0]; // 42 - line number for row 0 (NOT a property, TypedArray access)
buffer.traceId; // Walks parent chain to root if child span

// Access pattern - DIRECT property access, zero indirection:
buffer.userId_values[idx] = value; // ✅ Direct TypedArray access
buffer.userId_nulls[byteIdx] |= bitmask; // ✅ Direct bitmap access
buffer.userId[idx] = value; // ✅ Shorthand (alias for _values)

// Check allocation without triggering it:
const values = buffer.getColumnIfAllocated('userId'); // Returns undefined if not allocated

// NOT this pattern (extra indirection via nested Record):
// buffer.columns.userId[idx] = value;  // ❌ One extra lookup
```

## Span Identity

See **[Span Identity](./01b4_span_identity.md)** for:

- Span definition: "unit of work within a single thread of execution"
- Comparison to OpenTelemetry's span model
- Distributed span ID design (`threadId` + `spanId` composite)
- Thread-local counter design (zero overhead)
- TraceId branded string type (W3C compatible)

## SpanBuffer Memory Layout

See **[SpanBuffer Memory Layout](./01b5_spanbuffer_memory_layout.md)** for:

- Unified `_system` ArrayBuffer layout (timestamps + operations + identity)
- Buffer type layouts (Root/Child/Chained diagrams)
- Parent-based ancestry (pointer comparison)
- Thread ID generation and caching
- Constructor implementation
- Performance characteristics

## ColumnBuffer Extension Mechanism

See **[Buffer Code Generation and Extension](./01b6_buffer_codegen_extension.md)** for:

- `ColumnBufferExtensionOptions` interface
- How lmao extends arrow-builder's buffer classes
- Generated class structure with V8 hidden class optimization
- Usage examples and benefits

**Quick Summary**: arrow-builder provides `getColumnBufferClass()` with extension callbacks. lmao injects span-specific
code (identity, tree structure, context) without arrow-builder knowing what it does. This maintains clean package
separation while enabling V8-optimized class generation.

## Schema-Generated Buffer Extensions

See **[Buffer Code Generation and Extension](./01b6_buffer_codegen_extension.md#schema-generated-buffer-extensions)**
for:

- `createSpanBuffer()` and `getSpanBufferClass()` factory functions
- Composed buffer interface examples (HTTP + DB + user attributes)
- `createNextBuffer()` for overflow chaining
- Full constructor code generation details

**Quick Summary**: Schema drives typed column generation. Each attribute becomes direct properties via lazy getters.
Optional prefixes (e.g., `http_`, `db_`) prevent conflicts with SpanBuffer internals.

## Eager vs Lazy Column Allocation

See **[Buffer Self-Tuning](./01b2_buffer_self_tuning.md#lazy-to-eager-column-promotion)** for the full allocation
strategy.

**Quick Summary**:

- **System columns (EAGER)**: `timestamps`, `operations` - allocated in constructor, written every entry
- **User columns (LAZY)**: Schema attributes - allocated on first access via lazy getters
- **Self-tuning promotion**: Columns used in ≥80% of spans promote to eager allocation

| Schema Type | TypedArray       | Bytes/Element | Notes                                |
| ----------- | ---------------- | ------------- | ------------------------------------ |
| `enum`      | Uint8/16/32Array | 1/2/4         | Size based on enum value count       |
| `category`  | Array (JS)       | ~8 + string   | Raw strings, dict built in cold path |
| `text`      | Array (JS)       | ~8 + string   | Raw strings, no dictionary           |
| `number`    | Float64Array     | 8             | Full precision                       |
| `boolean`   | Uint8Array       | 1             | Bit-packed (8 per byte)              |

## Deferred String Interning

See
**[Buffer Performance Optimizations - String Interning](./01b1_buffer_performance_optimizations.md#string-interning-and-utf-8-caching-architecture)**
for:

- Why hot path stores raw `string[]` (zero Map lookups)
- Cold path dictionary building during Arrow conversion
- CATEGORY vs TEXT handling strategies
- Two-tier interner design (global + SIEVE cache)

**Quick Summary**: String columns store raw JavaScript strings on hot path. Dictionary encoding happens during
background flush - simpler code, better V8 optimization, bounded memory.

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
    // Accessing requestId_strings triggers lazy allocation (if first write)
    // NOTE: Stores RAW STRING, not interned index (deferred interning pattern)
    buffer.requestId_strings[0] = value; // Direct array assignment
    buffer.requestId_nulls[0] |= 1; // Set bit 0 as valid (Arrow format)

    // Return this for chaining
    return this;
  },

  userId: (buffer: SpanBuffer, value: string) => {
    // Same pattern - always row 0, direct property access
    // Raw string storage - dictionary built during Arrow conversion
    buffer.userId_strings[0] = value;
    buffer.userId_nulls[0] |= 1;
    return this;
  },
};

// Usage: ctx.tag writes to row 0
ctx.tag.requestId('req-123'); // Stores raw string at requestId_strings[0]
ctx.tag.userId('user-456'); // Stores raw string at userId_strings[0]
ctx.tag.requestId('req-789'); // OVERWRITES requestId_strings[0] - last write wins!
```

### ctx.scope - Scope Values Applied to Buffer Columns

Scope values are stored in a **separate generated Scope class** (see
[Span Scope Attributes](./01i_span_scope_attributes.md) for implementation details), NOT in buffer columns. This
separation enables zero-allocation scope setting while allowing efficient buffer operations.

**Scope values interact with buffer columns in three ways:**

1. **Write operations** (`ctx.log.info()`) - scope values written to each row during logging
2. **Pre-fill operations** (child span creation) - parent scope values fill child buffer via `TypedArray.fill()` for
   SIMD optimization
3. **Arrow conversion** (cold path) - scope values fill unallocated columns if they weren't written during hot path

**Key buffer architecture point**: Scope values do NOT trigger buffer column allocation when set. Allocation only
happens when:

- Values are written to buffer columns (via log operations)
- Child buffers are pre-filled with parent scope values
- Arrow conversion materializes scope-only columns

### ctx.log - Appends New Rows (Events)

```typescript
// ctx.log creates NEW rows starting at row 2 - APPENDS
// Row 0 = span-start, Row 1 = completion, Row 2+ = events
// Column properties are DIRECT on buffer via lazy getters

const logOperations = {
  info: (
    ctx: SpanContext,
    buffer: SpanBuffer,
    message: string,
    scope: GeneratedScope,
    attributes?: Record<string, any>
  ) => {
    // Append to current writeIndex (starts at 2)
    const index = buffer.writeIndex++;

    // Core columns - always written
    // ctx has anchorEpochMicros and anchorPerfNow from TraceContext
    buffer.timestamps[index] = getTimestamp(ctx);
    buffer.operations[index] = ENTRY_TYPE_INFO;

    // Message attribute - DEFERRED INTERNING: store raw string
    // Dictionary building happens during Arrow conversion (cold path)
    buffer.message_strings[index] = message; // Just array assignment
    const byteIdx = Math.floor(index / 8);
    const bitOffset = index % 8;
    buffer.message_nulls[byteIdx] |= 1 << bitOffset;

    // Apply scope values (from Scope class, NOT stored in buffer)
    const scopeValues = scope._getScopeValues();
    for (const [fieldName, value] of Object.entries(scopeValues)) {
      if (value !== undefined) {
        const stringsKey = `${fieldName}_strings`;
        const nullsKey = `${fieldName}_nulls`;
        // Raw string storage for string types (deferred interning)
        buffer[stringsKey][index] = value;
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
- **Deferred interning**: String columns store raw strings, dictionary built on cold path
- **Null tracking**: Arrow-format null bitmaps (1=valid, 0=null)

## Self-Tuning Capacity Management

See **[Buffer Self-Tuning](./01b2_buffer_self_tuning.md)** for:

- Zero-configuration memory management
- Adaptive buffer sizing per module
- Buffer chaining for overflow
- Capacity learning algorithm
- Lazy-to-eager column promotion

### Allocation Timing Summary

Understanding when allocations happen is critical for performance:

| Phase           | Frequency       | Allocations Allowed  | Key Operations                             |
| --------------- | --------------- | -------------------- | ------------------------------------------ |
| Module Init     | Once per module | Unlimited            | Class generation, schema compilation       |
| Op Definition   | Once per op     | Minimal              | Closure creation, module context binding   |
| Span Creation   | Every execution | Buffer + logger only | TypedArray/array allocation, object create |
| Per-Entry Write | Every log call  | **ZERO**             | Direct array writes (strings deferred)     |

**Per-Entry Write Rules** (zero allocations allowed):

- ✅ Direct TypedArray writes: `buffer.timestamps[index] = value`
- ✅ Raw string storage: `buffer.userId_strings[index] = value`
- ✅ Null bitmap updates: `buffer.userId_nulls[byteIdx] |= 1 << bitOffset`
- ❌ Object creation: `{ timestamp, value }`
- ❌ Array methods: `fields.map(...)`
- ❌ String operations: `value.trim()`

## Background Processing Pipeline

See **[Tree Walker and Arrow Conversion](./01k_tree_walker_and_arrow_conversion.md)** for:

- Two-pass tree conversion algorithm
- Copy vs view semantics (`subarray()` vs `slice()`)
- RecordBatch creation from SpanBuffers
- Column allocation during conversion

See **[Arrow Table Structure](./01f_arrow_table_structure.md)** for:

- Complete Arrow schema definition
- Column types and encoding
- ClickHouse query patterns

### Key Points (Quick Reference)

**What IS Efficient (Minimal Copy)**:

- Lazy columns never accessed: Zero conversion cost
- TypedArray `subarray()`: Creates a VIEW (no copy)
- Arrow dictionary encoding: Strings deduplicated

**What DOES Copy Data**:

- `TypedArray.slice()`: Creates NEW ArrayBuffer
- Null bitmap transformation
- String dictionary building from `string[]` arrays

## Arrow Conversion Integration

See **[Tree Walker and Arrow Conversion](./01k_tree_walker_and_arrow_conversion.md)** for implementation details.

**Key Integration Points**:

1. **Buffer helper methods**: Use `getColumnIfAllocated()` and `getNullsIfAllocated()` to check allocation without
   triggering it
2. **TypedArray subarray()**: Core columns use `subarray()` for zero-copy views
3. **Scope Value Handling**: Scope-only columns allocate and fill during cold path
4. **Column Name Mapping**: Column names match schema field names directly

```typescript
// IMPORTANT: Use subarray() for zero-copy conversion
const view = buffer.timestamps.subarray(0, writeIndex); // Zero-copy VIEW

// Check if column was allocated without triggering allocation
const values = buffer.getColumnIfAllocated('userId');
if (values) {
  const arrowData = values.subarray(0, writeIndex); // Zero-copy
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
buffer.http_status_values[0]              →  http_status: 200
buffer.db_query_values[0]                 →  db_query: "SELECT * FROM users WHERE id = ?"
buffer.http_status_nulls                  →  Arrow validity bitmap: 0x01
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
