# Tree Walker and Arrow Conversion

## Overview

This specification defines the **tree walker pattern** for converting SpanBuffer trees to Apache Arrow tables. It
establishes a clean separation between:

- **arrow-builder**: Generic columnar conversion utilities (no trace/span knowledge)
- **lmao**: Trace-specific tree traversal, hierarchy management, and span/trace ID tracking

The walker pattern enables two conversion modes (copying vs streaming) while keeping arrow-builder reusable for
non-trace columnar data.

## Problem Statement

### The Challenge

**SpanBuffers form a tree structure** (spans have children), but **arrow-builder shouldn't know about span trees**
(that's lmao's concept). However, **arrow-builder needs to know the total record count** to allocate RecordBatch columns
efficiently.

Additionally, the **output Arrow table should group each span's data together** for:

1. Query efficiency (related entries co-located)
2. Compression (similar data stored together)
3. Trace reconstruction (parents before children enables streaming)

### Implementation Status

**✅ ZERO-COPY COMPLETE**: The `convertToArrowTable()` function in `/packages/lmao/src/lib/convertToArrow.ts` now uses
`arrow.makeData()` exclusively for all column types. No `arrow.makeBuilder()` calls remain.

**Known Limitation**: The `convertSpanTreeToArrowTable()` function (lines 800-896) still has one inefficiency:

1. When spans have different schemas, it must merge schemas and align columns
2. This uses `vector.get(rowIdx)` to extract values during alignment (copies data)
3. Future optimization: pre-compute merged schema and build vectors directly

### Design Goals

1. **Separation of concerns**: arrow-builder remains generic and reusable
2. **Control traversal order**: lmao determines the exact order spans appear in Arrow table
3. **Zero-copy efficiency**: Direct TypedArray references without intermediate copies
4. **Two conversion modes**: Copying (for files) vs streaming (for network)
5. **Buffer overflow handling**: Natural accommodation of chained buffers (same spanId)
6. **Testability**: Tree traversal can be tested independently from Arrow conversion

## Solution: Tree Walker Pattern

### Core Concept

**lmao controls the traversal order** by implementing an iterator that yields buffers. **arrow-builder consumes the
iterator** without knowing about trace semantics.

### Package Separation Principle

**CRITICAL**: The tree concept (spans having children) is **LMAO-specific**. Arrow-builder should NOT know about trees.

**Separation strategy:**

1. **arrow-builder**: Defines a **generic `BufferIterable<T>` interface** - just an iterable with row counting
   capability. No tree semantics, no traversal order assumptions.

2. **lmao**: Implements `SpanTreeWalker` that satisfies `BufferIterable<SpanBuffer>` with span-specific traversal logic.

This keeps arrow-builder reusable for non-tree use cases (streaming buffers, batch processing, etc.) while giving lmao
full control over traversal order.

```typescript
// ────────────────────────────────────────────────────────────────────────────
// arrow-builder package: Generic interface (no tree knowledge)
// File: /packages/arrow-builder/src/lib/buffer/types.ts
// ────────────────────────────────────────────────────────────────────────────

/**
 * Generic interface for iterating over buffers with known total row count.
 *
 * Arrow-builder defines this minimal interface for consumption during
 * Arrow conversion. The interface is deliberately simple:
 * - No tree semantics (arrow-builder doesn't know about parent/child)
 * - No traversal order requirements (caller controls iteration order)
 * - Just: "How many rows total?" and "Give me buffers in your chosen order"
 *
 * @typeParam T - Buffer type (must have writeIndex for row counting)
 */
export interface BufferIterable<T extends { writeIndex: number }> {
  /**
   * Total number of rows across all buffers.
   * Used for pre-allocation in COPY mode.
   */
  getTotalRowCount(): number;

  /**
   * Iterate over buffers in caller-defined order.
   * Arrow-builder makes no assumptions about order.
   */
  [Symbol.iterator](): Iterator<T>;
}

// ────────────────────────────────────────────────────────────────────────────
// lmao package: Span-specific implementation (tree-aware)
// File: /packages/lmao/src/lib/spanTreeWalker.ts
// ────────────────────────────────────────────────────────────────────────────

/**
 * SpanTreeWalker implements BufferIterable with span-specific tree traversal.
 * This is LMAO's implementation that knows about span trees.
 */
class SpanTreeWalker implements BufferIterable<SpanBuffer> {
  // Implementation details in "SpanTreeWalker Implementation" section below
}
```

### Why Iterator Pattern?

The iterator pattern provides:

1. **Lazy evaluation**: Buffers yielded on demand (memory efficient)
2. **Type safety**: TypeScript's built-in `Iterator<T>` provides strong typing
3. **Standard protocol**: Works with `for...of`, spread operators, etc.
4. **Flexibility**: Easy to change traversal order without affecting consumers
5. **Composability**: Can chain, filter, or transform iterators

## Tree Traversal Order

### Depth-First Pre-Order Traversal

**Definition**: Visit parent before its children, then recursively visit children left-to-right.

```
Example trace tree:
  span1 (root)
  ├── span2
  │   ├── span4
  │   └── span5
  └── span3

Traversal order: span1 → span2 → span4 → span5 → span3
```

### Why Depth-First Pre-Order?

1. **Trace reconstruction**: Parent spans appear before children → enables streaming reconstruction
2. **Query efficiency**: Related spans (parent + children) co-located in Arrow table
3. **Compression**: Similar data (same trace branch) stored together → better Parquet compression
4. **Natural ordering**: Matches execution order for most synchronous code paths
5. **Memory locality**: Children immediately follow parents → cache-friendly queries

### Alternative: Breadth-First (Not Chosen)

```
Breadth-first order: span1 → span2 → span3 → span4 → span5
```

**Why not breadth-first?**

- Scatters children away from parents (poor query locality)
- Harder to reconstruct trace streaming (must buffer levels)
- Worse compression (interleaves unrelated branches)

## Buffer Overflow Chain Handling

### The Problem

Multiple buffers can share the same `spanId` due to buffer overflow:

```typescript
// When a span generates more entries than fit in one buffer:
// - Initial buffer: spanId=5, rowCount=1000, writeIndex=1000
// - Overflow buffer (next): spanId=5, rowCount=500, writeIndex=500
// Both buffers have SAME spanId, different row ranges
```

This is implemented in `createNextBuffer()` (spanBuffer.ts:248-265):

```typescript
export function createNextBuffer(buffer: SpanBuffer): SpanBuffer {
  const nextBuffer = createEmptySpanBuffer(
    buffer.spanId, // ← SAME spanId
    buffer.traceId, // ← SAME traceId
    schema,
    buffer.task,
    buffer.parent,
    capacity
  );

  buffer.next = nextBuffer; // Linked list
  return nextBuffer;
}
```

### Tree Walker Responsibilities

When traversing, the walker MUST:

1. **Yield overflow buffers in sequence** after the primary buffer
2. **Maintain correct traversal order** across overflow chains
3. **Ensure all entries for a span are written contiguously** in Arrow table

```typescript
// Example: span with overflow
// Primary buffer: span2, rows 0-999
// Overflow buffer: span2, rows 1000-1499
//
// Walker yields:
// 1. span1 primary buffer
// 2. span2 primary buffer (rows 0-999)
// 3. span2 overflow buffer (rows 1000-1499)  ← Same spanId!
// 4. span4 primary buffer
// ...
```

### arrow-builder Responsibilities

arrow-builder should:

1. **Accept buffers in provided order** (no reordering)
2. **Copy/stream buffer contents** without span ID awareness
3. **Trust walker's traversal order** (no semantic understanding)

## Two Conversion Modes

### COPY Mode (Pre-Allocated)

**Use case**: Writing to memory-mapped files, Parquet files, or any scenario where you want a single contiguous Arrow
structure.

**Strategy**:

1. Call `walker.getTotalRowCount()` upfront
2. Pre-allocate full RecordBatch with total capacity
3. Iterate through walker, copying each buffer's data into allocated columns
4. Return single Arrow Table with all data

**Benefits**:

- Single contiguous memory region (optimal for compression)
- One allocation upfront (no reallocation)
- Sequential writes (cache-friendly)
- Ideal for Parquet conversion (expects full batches)

**Trade-offs**:

- Requires knowing total row count upfront
- Full memory allocation before any conversion
- Not suitable for unbounded streams

**Memory usage**: `totalRows × columnCount × bytesPerValue`

### STREAMING Mode (Zero-Copy Batches)

**Use case**: Streaming to stdout, sockets, network endpoints, or any scenario where you want minimal memory overhead.

**Strategy**:

1. Iterate through walker without pre-allocation
2. For each buffer (or small group), convert directly to Arrow RecordBatch
3. Yield RecordBatch immediately (caller can write to stream)
4. Only one batch in memory at a time

**Benefits**:

- Minimal memory overhead (one batch at a time)
- No upfront allocation or total count needed
- Can handle unbounded/unknown-size traces
- Ideal for network streaming (progressive sending)

**Trade-offs**:

- Multiple RecordBatches (may need concatenation for some tools)
- Repeated schema transmission (each batch has schema)
- Less optimal for Parquet (prefers large batches)

**Memory usage**: `maxBatchSize × columnCount × bytesPerValue`

### When to Use Each Mode

| Scenario                | Mode      | Why                                                      |
| ----------------------- | --------- | -------------------------------------------------------- |
| Write to Parquet file   | COPY      | Parquet prefers large batches, benefits from compression |
| Write to mmap file      | COPY      | Single memory region, no fragmentation                   |
| Stream to stdout/socket | STREAMING | Minimal memory, progressive output                       |
| Large traces (>100MB)   | STREAMING | Avoid giant allocation                                   |
| Small traces (<1MB)     | COPY      | Simpler, fewer batches                                   |
| Unknown trace size      | STREAMING | Can't pre-allocate                                       |

### Output Target Details

#### Memory-Mapped Files (mmap)

For mmap'd files, use **COPY mode** because:

- Memory-mapped files work best with a single contiguous memory region
- The OS can efficiently page data in/out from contiguous layouts
- File size must be known upfront for mmap (fits COPY's pre-allocation model)
- Sequential writes to contiguous memory = optimal I/O performance

```typescript
// Example: Write to mmap'd Arrow IPC file
const table = convertSpanTreeToArrowTable(rootBuffer, ...interners);
const writer = await RecordBatchFileWriter.writeAll(table);
await Bun.write('traces.arrow', writer.toNodeStream());
```

#### Standard I/O and Sockets (stdio/sockets)

For streaming outputs, use **STREAMING mode** because:

- No need to buffer entire trace in memory
- Progressive output enables backpressure handling
- Enables processing arbitrarily large traces
- Works with Arrow IPC streaming format

```typescript
// Example: Stream to stdout using Arrow IPC streaming format
for (const batch of convertSpanTreeToArrowBatches(rootBuffer, ...interners)) {
  const writer = RecordBatchStreamWriter.writeAll(new arrow.Table([batch]));
  process.stdout.write(writer.toUint8Array());
}
```

#### Network Transmission

For network transmission, choice depends on protocol:

- **HTTP response with Content-Length**: COPY mode (need size upfront)
- **HTTP chunked transfer**: STREAMING mode (progressive sending)
- **WebSocket**: STREAMING mode (message-by-message)
- **gRPC streaming**: STREAMING mode (native streaming support)

## Interface Design

### BufferIterable Interface (arrow-builder - Generic)

Located in **arrow-builder** package (`/packages/arrow-builder/src/lib/buffer/types.ts`):

**IMPORTANT**: This interface is intentionally named `BufferIterable` rather than `TreeWalker` because:

- Arrow-builder should NOT know about trees (that's a LMAO concept)
- The interface is just "give me buffers in some order with a row count"
- Any traversal semantics (tree, list, batch) are implementation details

```typescript
/**
 * Generic interface for iterating over buffers with known total row count.
 *
 * This is a MINIMAL interface for consumption during Arrow conversion:
 * - No tree semantics (arrow-builder doesn't know about parent/child)
 * - No traversal order requirements (caller controls iteration order)
 * - Just: "How many rows total?" and "Give me buffers in your chosen order"
 *
 * Use cases beyond span trees:
 * - Batch processing: yield buffers from a queue
 * - Streaming: yield buffers as they arrive
 * - Single buffer: wrap one buffer for conversion
 *
 * @typeParam T - Buffer type (must have writeIndex for row counting)
 */
export interface BufferIterable<T extends { writeIndex: number }> {
  /**
   * Total number of rows across all buffers.
   *
   * Used for pre-allocation in COPY mode. Implementations may perform
   * traversal to calculate this value, so cache the result if calling
   * multiple times.
   *
   * @returns Total row count (sum of all buffer writeIndex values)
   */
  getTotalRowCount(): number;

  /**
   * Iterate over buffers in implementation-defined order.
   *
   * Arrow-builder makes NO assumptions about iteration order.
   * The implementation decides the order (depth-first, breadth-first, FIFO, etc.)
   *
   * @returns Iterator yielding buffers in caller-controlled order
   */
  [Symbol.iterator](): Iterator<T>;
}
```

### SpanTreeWalker Implementation (lmao - Span-Specific)

Located in **lmao** package (`/packages/lmao/src/lib/spanTreeWalker.ts`):

```typescript
import type { BufferIterable } from '@smoothbricks/arrow-builder';
import type { SpanBuffer } from './types.js';

/**
 * Tree walker for SpanBuffer hierarchies.
 *
 * Implements depth-first pre-order traversal:
 * 1. Yield primary buffer
 * 2. Yield overflow buffers (same spanId, chained via .next)
 * 3. Recursively yield children (depth-first)
 *
 * This ensures:
 * - All entries for a span are contiguous in Arrow table
 * - Parent spans appear before children (enables streaming reconstruction)
 * - Related spans are co-located (better compression and query performance)
 *
 * NOTE: Implements arrow-builder's generic BufferIterable interface.
 * Arrow-builder doesn't know this is a tree walker - it just sees an iterable.
 */
export class SpanTreeWalker implements BufferIterable<SpanBuffer> {
  private readonly rootSpans: SpanBuffer[];
  private totalRowCount?: number; // Cache for efficiency

  /**
   * Create a walker for one or more root spans.
   *
   * @param rootSpans - Root span buffer(s) to traverse
   */
  constructor(rootSpans: SpanBuffer | SpanBuffer[]) {
    this.rootSpans = Array.isArray(rootSpans) ? rootSpans : [rootSpans];
  }

  /**
   * Calculate total row count across all spans and overflow buffers.
   *
   * Performs full tree traversal to count rows. Result is cached for efficiency.
   *
   * @returns Total number of rows that will be yielded by the iterator
   */
  getTotalRowCount(): number {
    if (this.totalRowCount !== undefined) {
      return this.totalRowCount;
    }

    this.totalRowCount = this.rootSpans.reduce((total, root) => total + this.countRowsRecursive(root), 0);

    return this.totalRowCount;
  }

  /**
   * Iterate over all span buffers in depth-first pre-order.
   *
   * Yields primary buffers, then overflow chains, then children recursively.
   */
  *[Symbol.iterator](): Iterator<SpanBuffer> {
    for (const root of this.rootSpans) {
      yield* this.traverseDepthFirst(root);
    }
  }

  /**
   * Depth-first pre-order traversal of a single span subtree.
   *
   * Order:
   * 1. Primary buffer for this span
   * 2. Overflow buffers for this span (via .next chain)
   * 3. Children (recursive depth-first)
   *
   * @param node - Span buffer to traverse
   */
  private *traverseDepthFirst(node: SpanBuffer): Generator<SpanBuffer> {
    // 1. Yield primary buffer
    yield node;

    // 2. Yield overflow buffers (same spanId, chained via .next)
    let overflow = node.next as SpanBuffer | undefined;
    while (overflow) {
      yield overflow;
      overflow = overflow.next as SpanBuffer | undefined;
    }

    // 3. Recursively yield children (depth-first)
    for (const child of node.children) {
      yield* this.traverseDepthFirst(child);
    }
  }

  /**
   * Count rows in a span subtree (including overflow and children).
   *
   * @param node - Span buffer to count
   * @returns Total rows in this subtree
   */
  private countRowsRecursive(node: SpanBuffer): number {
    // Count rows in primary buffer
    let rowCount = node.writeIndex;

    // Count rows in overflow chain
    let overflow = node.next as SpanBuffer | undefined;
    while (overflow) {
      rowCount += overflow.writeIndex;
      overflow = overflow.next as SpanBuffer | undefined;
    }

    // Count rows in children (recursive)
    for (const child of node.children) {
      rowCount += this.countRowsRecursive(child);
    }

    return rowCount;
  }
}
```

## Arrow Conversion Functions

### Package Location Decision

Arrow conversion functions can live in either package depending on their genericity:

| Function                         | Package       | Why                                                       |
| -------------------------------- | ------------- | --------------------------------------------------------- |
| `convertBuffersToArrowTable()`   | arrow-builder | Generic: works with any BufferIterable                    |
| `convertBuffersToArrowBatches()` | arrow-builder | Generic: streaming conversion for any BufferIterable      |
| `convertSpanTreeToArrowTable()`  | lmao          | Span-specific: knows about SpanBuffer, interners, modules |
| `buildDefaultSystemVectors()`    | lmao          | Span-specific: trace_id, span_id, parent_span_id columns  |
| Column converter functions       | lmao          | Span-specific: need access to interners and schema        |

**Current Implementation**: All conversion is in `lmao` package because it needs access to span-specific metadata
(interners, schema, system columns). Arrow-builder provides the types and interfaces.

### COPY Mode Implementation

Located in **lmao** package (uses arrow-builder's `BufferIterable` interface):

```typescript
/**
 * Convert buffers to Arrow Table (COPY mode).
 *
 * Pre-allocates full capacity based on iterable.getTotalRowCount(), then
 * copies all buffer data into contiguous Arrow columns.
 *
 * @param iterable - BufferIterable providing buffers in order
 * @param schema - Arrow schema for the table
 * @param columnConverter - Function to convert buffer columns to Arrow vectors
 * @returns Arrow Table with all data copied into single RecordBatch
 */
export function convertBuffersToArrowTable<T extends ColumnBuffer>(
  iterable: BufferIterable<T>,
  schema: arrow.Schema,
  columnConverter: (buffers: T[], schema: arrow.Schema) => arrow.Vector[]
): arrow.Table {
  const totalRows = iterable.getTotalRowCount();

  if (totalRows === 0) {
    return new arrow.Table(schema);
  }

  // Collect all buffers from iterable
  const buffers: T[] = [];
  for (const buffer of iterable) {
    buffers.push(buffer);
  }

  // Convert to Arrow vectors (implementation-specific)
  const vectors = columnConverter(buffers, schema);

  // Create single RecordBatch
  const data = arrow.makeData({
    type: new arrow.Struct(schema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  const recordBatch = new arrow.RecordBatch(schema, data);
  return new arrow.Table([recordBatch]);
}
```

### STREAMING Mode Implementation

Located in **lmao** package (uses arrow-builder's `BufferIterable` interface):

```typescript
/**
 * Configuration for streaming Arrow conversion.
 */
export interface StreamingOptions {
  /**
   * Maximum rows per RecordBatch.
   * Larger batches = more memory but fewer batches to transmit.
   * Smaller batches = less memory but more protocol overhead.
   *
   * Default: 8192 (reasonable balance for most use cases)
   */
  maxBatchSize?: number;
}

/**
 * Convert buffers to Arrow RecordBatches (STREAMING mode).
 *
 * Yields RecordBatches as they're created, enabling streaming output without
 * buffering all buffers in memory.
 *
 * @param iterable - BufferIterable providing buffers in order
 * @param schema - Arrow schema for the table
 * @param columnConverter - Function to convert buffer batch to Arrow vectors
 * @param options - Streaming options (batch size, etc.)
 * @yields Arrow RecordBatches ready for streaming
 */
export function* convertBuffersToArrowBatches<T extends ColumnBuffer>(
  iterable: BufferIterable<T>,
  schema: arrow.Schema,
  columnConverter: (buffers: T[], schema: arrow.Schema) => arrow.Vector[],
  options: StreamingOptions = {}
): Generator<arrow.RecordBatch> {
  const maxBatchSize = options.maxBatchSize ?? 8192;

  let batchBuffers: T[] = [];
  let batchRowCount = 0;

  for (const buffer of iterable) {
    batchBuffers.push(buffer);
    batchRowCount += buffer.writeIndex;

    // Yield batch when it reaches max size
    if (batchRowCount >= maxBatchSize) {
      yield createRecordBatch(batchBuffers, schema, columnConverter);
      batchBuffers = [];
      batchRowCount = 0;
    }
  }

  // Yield final batch (may be smaller than maxBatchSize)
  if (batchBuffers.length > 0) {
    yield createRecordBatch(batchBuffers, schema, columnConverter);
  }
}

/**
 * Helper: Create a RecordBatch from a set of buffers.
 */
function createRecordBatch<T extends ColumnBuffer>(
  buffers: T[],
  schema: arrow.Schema,
  columnConverter: (buffers: T[], schema: arrow.Schema) => arrow.Vector[]
): arrow.RecordBatch {
  const vectors = columnConverter(buffers, schema);
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);

  const data = arrow.makeData({
    type: new arrow.Struct(schema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new arrow.RecordBatch(schema, data);
}
```

## Zero-Copy Column Conversion

### Why Zero-Copy Matters

The builder pattern (`arrow.makeBuilder()`) is **PROHIBITED** because it copies every value during `append()`:

```typescript
// ❌ WRONG: Builder pattern (copies data) - DO NOT USE
const builder = arrow.makeBuilder({ type: new arrow.Float64() });
for (const buf of buffers) {
  for (let i = 0; i < buf.writeIndex; i++) {
    builder.append(buf.timestamps[i]); // ← Copies each value!
  }
}
const vector = builder.finish().toVector();
```

This is inefficient because SpanBuffer already has data in correct TypedArray format.

### Zero-Copy Solution: arrow.makeData() (IMPLEMENTED)

**Correct approach** (implemented in `convertToArrow.ts`):

```typescript
// ✅ CORRECT: Zero-copy with arrow.makeData()
function convertTimestamps(buffers: SpanBuffer[]): arrow.Vector {
  // Concatenate TypedArrays from all buffers
  const arrays = buffers.map((buf) => buf.timestamps.subarray(0, buf.writeIndex));
  const timestamps = concatenateTypedArrays(arrays);

  // Wrap TypedArray directly (zero-copy)
  const data = arrow.makeData({
    type: new arrow.TimestampMicrosecond(),
    offset: 0,
    length: timestamps.length,
    data: timestamps, // ← Direct reference, no copy!
    nullBitmap: null, // Timestamps are never null
  });

  return arrow.makeVector(data);
}

/**
 * Concatenate TypedArrays efficiently (one copy only).
 */
function concatenateTypedArrays<T extends TypedArray>(arrays: T[]): T {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new (arrays[0].constructor as any)(totalLength);

  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset); // ← VM-optimized bulk copy
    offset += arr.length;
  }

  return result;
}
```

### Strategies by Column Type

Different column types require different zero-copy strategies:

#### 1. Primitive Types (Float64, Uint8, etc.)

**Direct subarray() reference** (simplest case):

```typescript
// Single buffer - zero-copy slice
const data = buffer.attr_httpDuration.subarray(0, buffer.writeIndex);

// Multiple buffers - concatenate once
const arrays = buffers.map((buf) => buf.attr_httpDuration.subarray(0, buf.writeIndex));
const allData = concatenateTypedArrays(arrays);

return arrow.makeData({
  type: new arrow.Float64(),
  offset: 0,
  length: allData.length,
  data: allData,
  nullBitmap: buildNullBitmap(buffers, 'attr_httpDuration'),
});
```

#### 2. Dictionary-Encoded Types (category, enum)

**makeData with dictionary vector**:

```typescript
// Enum: Dictionary with compile-time values
const enumValues = ['GET', 'POST', 'PUT', 'DELETE']; // From schema

// Concatenate index arrays
const indexArrays = buffers.map((buf) => buf.attr_httpMethod_values.subarray(0, buf.writeIndex));
const allIndices = concatenateTypedArrays(indexArrays);

return arrow.makeData({
  type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
  offset: 0,
  length: allIndices.length,
  data: allIndices, // Index array (Uint8Array)
  nullBitmap: buildNullBitmap(buffers, 'attr_httpMethod'),
  dictionary: arrow.makeVector(enumValues), // String array → vector
});
```

#### 3. String Types (text) - Conditional Dictionary

**Space-aware strategy** (use dictionary only if it saves >128 bytes):

```typescript
// Calculate space savings
const { uniqueStrings, totalRows } = analyzeTextColumn(buffers, 'attr_errorMessage');

const uniqueStringBytes = uniqueStrings.reduce((sum, s) => sum + s.length, 0);
const dictionarySize = uniqueStringBytes + totalRows * 4; // 4 bytes per index
const plainSize = totalStringBytes; // Sum of all string bytes
const spaceSavings = plainSize - dictionarySize;

if (spaceSavings > 128) {
  // Use dictionary encoding (same as category above)
  return createDictionaryColumn(buffers, uniqueStrings, 'attr_errorMessage');
} else {
  // Use plain UTF-8 (requires building offset buffer)
  const { valueOffsets, utf8Data } = buildUtf8Buffers(buffers, 'attr_errorMessage');

  return arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: totalRows,
    data: utf8Data, // Raw UTF-8 bytes
    valueOffsets, // Offsets into utf8Data
    nullBitmap: buildNullBitmap(buffers, 'attr_errorMessage'),
  });
}
```

### Null Bitmap Construction

Arrow format: **bit-packed, 1 = valid, 0 = null**

```typescript
/**
 * Build null bitmap from SpanBuffer null tracking.
 *
 * Concatenates null bitmaps from all buffers, handling bit alignment.
 */
function buildNullBitmap(
  buffers: SpanBuffer[],
  columnName: `attr_${string}`
): { nullBitmap: Uint8Array | null; nullCount: number } {
  const nullsName = `${columnName}_nulls` as const;

  // Check if any buffer has nulls
  const hasNulls = buffers.some((buf) => buf[nullsName] !== undefined);
  if (!hasNulls) {
    return { nullBitmap: null, nullCount: 0 };
  }

  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  const bitmapBytes = Math.ceil(totalRows / 8);
  const bitmap = new Uint8Array(bitmapBytes);

  let bitOffset = 0;
  let nullCount = 0;

  for (const buf of buffers) {
    const nullBitmap = buf[nullsName];

    for (let i = 0; i < buf.writeIndex; i++) {
      const isValid = nullBitmap ? (nullBitmap[Math.floor(i / 8)] & (1 << i % 8)) !== 0 : true; // No null bitmap = all valid

      if (isValid) {
        const byteIdx = Math.floor(bitOffset / 8);
        const bitIdx = bitOffset % 8;
        bitmap[byteIdx] |= 1 << bitIdx;
      } else {
        nullCount++;
      }

      bitOffset++;
    }
  }

  return { nullBitmap: bitmap, nullCount };
}
```

## Current Implementation Status

### ✅ Zero-Copy Implementation Complete

**File**: `/packages/lmao/src/lib/convertToArrow.ts`

The implementation now uses `arrow.makeData()` exclusively for all column types:

```typescript
// ✅ CORRECT: Zero-copy with arrow.makeData()
const timestampData = arrow.makeData({
  type: new arrow.TimestampMicrosecond(),
  offset: 0,
  length: totalRows,
  nullCount: 0,
  data: allTimestamps, // Direct TypedArray reference
});
vectors.push(arrow.makeVector(timestampData));
```

**Implemented Patterns**:

1. ✅ `createZeroCopyData()` helper - wraps TypedArrays without copying
2. ✅ `concatenateTypedArrays()` - efficient buffer chain concatenation (one copy only)
3. ✅ `concatenateNullBitmaps()` - null bitmap merging across buffers
4. ✅ Primitive columns (Float64, Uint8, Bool) - direct `subarray()` + concatenation
5. ✅ Dictionary columns (enum, category, text) - index arrays + dictionary vectors
6. ✅ System columns (timestamp, trace_id, span_id, entry_type, module, span_name) - all zero-copy

### Known Limitation: Schema Alignment in `convertSpanTreeToArrowTable()`

When converting span trees where different spans have different schemas (different tag attributes), the current
implementation:

1. Converts each buffer to separate Arrow table
2. Merges schemas to find union of all columns
3. Uses `vector.get(rowIdx)` to extract values (copies data)

This is necessary for schema alignment but is inefficient. Future optimization could pre-compute the merged schema and
build vectors directly.

## Migration Plan (Reference for Future Enhancements)

### Future Steps

#### Step 1: Create SpanTreeWalker (New File)

**File**: `/packages/lmao/src/lib/spanTreeWalker.ts`

```typescript
/**
 * Tree walker for SpanBuffer hierarchies.
 * Implements depth-first pre-order traversal with overflow chain handling.
 */
export class SpanTreeWalker implements TreeWalker<SpanBuffer> {
  // ... (implementation from "Interface Design" section above)
}
```

**Test**: `/packages/lmao/src/lib/__tests__/spanTreeWalker.test.ts`

```typescript
describe('SpanTreeWalker', () => {
  it('traverses single span', () => {
    /* ... */
  });
  it('traverses parent with children (depth-first)', () => {
    /* ... */
  });
  it('handles overflow chains', () => {
    /* ... */
  });
  it('calculates total row count correctly', () => {
    /* ... */
  });
  it('caches total row count', () => {
    /* ... */
  });
});
```

#### Step 2: Refactor Zero-Copy Column Converters

**File**: `/packages/lmao/src/lib/arrowColumnConverters.ts` (new)

Extract zero-copy conversion logic from `convertToArrow.ts`:

```typescript
/**
 * Convert SpanBuffer columns to Arrow vectors using zero-copy makeData().
 */

export function convertTimestamps(buffers: SpanBuffer[]): arrow.Vector {
  // ... (implementation from "Zero-Copy Column Conversion" section)
}

export function convertTraceIds(buffers: SpanBuffer[]): arrow.Vector {
  // Dictionary with runtime values
  // ... (implementation)
}

export function convertEnumColumn(
  buffers: SpanBuffer[],
  columnName: string,
  enumValues: readonly string[]
): arrow.Vector {
  // Dictionary with compile-time values
  // ... (implementation)
}

export function convertCategoryColumn(
  buffers: SpanBuffer[],
  columnName: string,
  interner: StringInterner
): arrow.Vector {
  // Dictionary with runtime values from interner
  // ... (implementation)
}

export function convertTextColumn(
  buffers: SpanBuffer[],
  columnName: string,
  textStorage: StringInterner
): arrow.Vector {
  // Conditional dictionary (only if saves >128 bytes)
  // ... (implementation)
}

export function convertNumberColumn(buffers: SpanBuffer[], columnName: string): arrow.Vector {
  // Plain Float64
  // ... (implementation)
}

export function convertBooleanColumn(buffers: SpanBuffer[], columnName: string): arrow.Vector {
  // Plain Bool
  // ... (implementation)
}

// Helper functions
function concatenateTypedArrays<T extends TypedArray>(arrays: T[]): T {
  // ... (implementation)
}

function buildNullBitmap(
  buffers: SpanBuffer[],
  columnName: string
): { nullBitmap: Uint8Array | null; nullCount: number } {
  // ... (implementation)
}
```

#### Step 3: Update convertToArrowTable()

**File**: `/packages/lmao/src/lib/convertToArrow.ts`

Replace current implementation:

```typescript
/**
 * Convert SpanBuffer tree to Arrow Table (COPY mode).
 *
 * Uses SpanTreeWalker for depth-first traversal and zero-copy column conversion.
 */
export function convertSpanTreeToArrowTable(
  rootBuffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: StringInterner,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  systemColumnBuilder?: SystemColumnBuilder
): arrow.Table {
  // Create walker
  const walker = new SpanTreeWalker(rootBuffer);
  const totalRows = walker.getTotalRowCount();

  if (totalRows === 0) {
    return new arrow.Table();
  }

  // Collect all buffers via walker
  const buffers: SpanBuffer[] = [];
  for (const buffer of walker) {
    buffers.push(buffer);
  }

  // Build schema
  const schema = buildArrowSchema(buffers[0].task.module.tagAttributes);

  // Convert columns using zero-copy helpers
  const vectors: arrow.Vector[] = [
    convertTimestamps(buffers),
    convertTraceIds(buffers),
    convertSpanIds(buffers),
    convertParentSpanIds(buffers),
    convertEntryTypes(buffers),
    convertModules(buffers, moduleIdInterner),
    convertSpanNames(buffers, spanNameInterner),
  ];

  // Add attribute columns
  for (const [fieldName, fieldSchema] of Object.entries(schema.tagAttributes)) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const columnName = `attr_${fieldName}`;

    if (lmaoType === 'enum') {
      const enumValues = getEnumValues(fieldSchema)!;
      vectors.push(convertEnumColumn(buffers, columnName, enumValues));
    } else if (lmaoType === 'category') {
      vectors.push(convertCategoryColumn(buffers, columnName, categoryInterner));
    } else if (lmaoType === 'text') {
      vectors.push(convertTextColumn(buffers, columnName, textStorage));
    } else if (lmaoType === 'number') {
      vectors.push(convertNumberColumn(buffers, columnName));
    } else if (lmaoType === 'boolean') {
      vectors.push(convertBooleanColumn(buffers, columnName));
    }
  }

  // Create single RecordBatch
  const data = arrow.makeData({
    type: new arrow.Struct(schema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  const recordBatch = new arrow.RecordBatch(schema, data);
  return new arrow.Table([recordBatch]);
}
```

#### Step 4: Add Streaming API (New)

**File**: `/packages/lmao/src/lib/convertToArrow.ts`

```typescript
/**
 * Convert SpanBuffer tree to Arrow RecordBatches (STREAMING mode).
 *
 * Yields RecordBatches as they're created for streaming output.
 */
export function* convertSpanTreeToArrowBatches(
  rootBuffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: StringInterner,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  options: StreamingOptions = {}
): Generator<arrow.RecordBatch> {
  const walker = new SpanTreeWalker(rootBuffer);
  const maxBatchSize = options.maxBatchSize ?? 8192;

  let batchBuffers: SpanBuffer[] = [];
  let batchRowCount = 0;

  for (const buffer of walker) {
    batchBuffers.push(buffer);
    batchRowCount += buffer.writeIndex;

    if (batchRowCount >= maxBatchSize) {
      yield convertBuffersToRecordBatch(
        batchBuffers,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      batchBuffers = [];
      batchRowCount = 0;
    }
  }

  if (batchBuffers.length > 0) {
    yield convertBuffersToRecordBatch(batchBuffers, categoryInterner, textStorage, moduleIdInterner, spanNameInterner);
  }
}
```

#### Step 5: Update Tests

Update existing tests to verify:

1. **Traversal order**: Parents before children
2. **Overflow handling**: Same spanId buffers appear contiguously
3. **Zero-copy**: No unnecessary data copying
4. **Streaming mode**: RecordBatches yielded correctly

## Benefits Summary

### 1. Clean Separation of Concerns

- **arrow-builder**: Generic, reusable for non-trace data
- **lmao**: Trace-specific traversal and hierarchy management

### 2. Controlled Traversal Order

- **Depth-first pre-order**: Parents before children
- **Overflow handling**: Same spanId buffers contiguous
- **Query efficiency**: Related data co-located

### 3. Zero-Copy Efficiency

- **Direct TypedArray references**: No intermediate copies
- **Single concatenation**: One copy per column maximum
- **Minimal GC pressure**: No temporary builder objects

### 4. Two Conversion Modes

- **COPY**: Optimal for files and Parquet
- **STREAMING**: Optimal for network and large traces

### 5. Testability

- **Walker tests**: Independent of Arrow conversion
- **Converter tests**: Independent of traversal logic
- **Integration tests**: End-to-end validation

## Future Enhancements

### 1. Parallel Conversion

For very large traces, parallelize column conversion:

```typescript
// Convert columns in parallel (requires worker threads or Web Workers)
const vectors = await Promise.all([
  convertTimestampsAsync(buffers),
  convertTraceIdsAsync(buffers),
  // ... other columns
]);
```

### 2. Incremental Streaming

Stream RecordBatches as spans complete (not waiting for full trace):

```typescript
// Yield RecordBatch immediately when a top-level span completes
for await (const completedSpan of traceStream) {
  const walker = new SpanTreeWalker(completedSpan);
  yield * convertTreeToArrowBatches(walker /* ... */);
}
```

### 3. Compression Pipeline

Integrate with Parquet compression during streaming:

```typescript
// Stream directly to Parquet without intermediate Arrow Table
for (const batch of convertTreeToArrowBatches(walker)) {
  parquetWriter.write(batch);
}
```

### 4. Schema Evolution

Handle traces with different schemas (across versions):

```typescript
// Merge schemas from multiple trace versions
const mergedSchema = mergeSchemas(walker.getSchemas());
const table = convertWithSchemaAlignment(walker, mergedSchema);
```

## Integration Points

This specification integrates with:

- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Final Arrow schema and column types
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: SpanBuffer layout and TypedArrays
- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Entry type enum and operations
- **[Trace Schema System](./01a_trace_schema_system.md)**: Schema definition and type system
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Attribute column prefixing

## References

- **Implementation**: `/packages/lmao/src/lib/convertToArrow.ts`
- **Buffer types**: `/packages/arrow-builder/src/lib/buffer/types.ts`
- **Spec**: `/specs/01f_arrow_table_structure.md` (Arrow conversion interface section)
