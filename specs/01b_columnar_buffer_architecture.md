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

See **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md#buffer-initialization-code)** for
buffer initialization and completion code.

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

See **[Buffer Code Generation and Extension](./01b6_buffer_codegen_extension.md#lazy-column-getter-implementation)** for
the lazy getter implementation code and key design points.

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

See
**[SpanBuffer Memory Layout - Complete Interface](./01b5_spanbuffer_memory_layout.md#complete-spanbuffer-interface)**
for the full interface definition including:

- Core columns (`timestamps`, `operations`)
- Attribute columns with lazy getters (`X_nulls`, `X_values`)
- Tree structure (`children`, `parent`)
- Dual module references (`callsiteModule`, `module`)
- Span identification (`threadId`, `spanId`, `traceId`)
- `ModuleContext` interface with self-tuning stats

**Key Architectural Point**: Column properties are **direct properties** on SpanBuffer via lazy getters (no nested
`columns: Record<...>`). This provides zero indirection for hot path access.

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

## Tag and Log Operations

See **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md#span-attributes-tag)** for
tag/log operation semantics and implementation.

**Key Architectural Points**:

- **ctx.tag**: Overwrites row 0 (span-start) - matches Datadog/OpenTelemetry semantics
- **ctx.log**: Appends new rows starting at row 2
- **Scope values**: Stored in separate Scope class (see [01i](./01i_span_scope_attributes.md)), NOT in buffer columns
- **Deferred interning**: String columns store raw strings, dictionary built on cold path

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
- Buffer access patterns for conversion (`subarray()` vs `slice()`)
- RecordBatch creation from SpanBuffers
- Column allocation during conversion

See **[Arrow Table Structure](./01f_arrow_table_structure.md)** for:

- Complete Arrow schema definition
- Column types and encoding
- ClickHouse query patterns

**Key Efficiency Points**:

- Lazy columns never accessed: Zero conversion cost
- TypedArray `subarray()`: Creates a VIEW (no copy)
- TypedArray `slice()`: Creates NEW ArrayBuffer (copies)
- Scope-only columns: Allocate and fill during cold path

This columnar architecture ensures that high-performance buffer operations flow into efficient analytical storage and
querying.
