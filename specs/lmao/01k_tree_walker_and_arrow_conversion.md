# Tree Walker and Arrow Conversion <a id="smoo/lmao!n/arrow-tree-walker"></a>

## Overview <a id="smoo/lmao!n/arrow-tree-walker.overview"></a>

This specification defines the **two-pass tree conversion** approach for converting SpanBuffer trees to Apache Arrow
tables. The approach is designed for:

- **Zero intermediate allocations**: No buffer collection arrays during tree walking
- **Exact-size allocations**: Pre-calculated sizes for all Arrow buffers
- **UTF-8 caching**: Encode once, copy on reuse for repeated strings
- **Shared dictionaries**: All RecordBatches reference the same dictionary vectors

## Problem Statement <a id="smoo/lmao!n/arrow-tree-walker.problem"></a>

### The Challenge <a id="smoo/lmao!n/arrow-tree-walker.problem-challenge"></a>

**SpanBuffers form a tree structure** (spans have children), and arrow-builder shouldn't know about span trees (that's
lmao's concept). When converting to Arrow:

1. **Dictionary building requires full traversal**: Must see all string values before allocating dictionary buffers
2. **RecordBatch creation needs dictionaries**: Can't create batches until dictionaries are finalized
3. **Memory efficiency is critical**: Avoid collecting buffers into intermediate arrays

### Design Goals <a id="smoo/lmao!n/arrow-tree-walker.problem-goals"></a>

1. **No intermediate buffer collection**: Walk the tree twice instead of collecting into an array
2. **Exact-size allocations**: Know total bytes/rows before allocating
3. **UTF-8 optimization**: Cache encodings for repeated strings, use `encodeInto()` for unique ones
4. **Single dictionary per column**: All RecordBatches share the same dictionary vectors

## Solution: Two-Pass Tree Conversion <a id="smoo/lmao!n/arrow-tree-walker-two-pass"></a>

### Architecture Overview <a id="smoo/lmao!n/arrow-tree-walker-two-pass.architecture"></a>

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PASS 1: Build Dictionaries                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Walk tree with the allocation-free walk (no buffer array)                 │
│                     │                                                        │
│                     ▼                                                        │
│   For each buffer:                                                           │
│   ├── Count rows (accumulate total)                                         │
│   └── For each string column (category/text):                               │
│       └── Add to DictionaryBuilder                                          │
│                     │                                                        │
│                     ▼                                                        │
│   DictionaryBuilder per column:                                             │
│   ├── Map<string, { utf8?: Uint8Array, byteLength: number, count: number }> │
│   ├── Track totalBytes (sum of unique string byte lengths)                  │
│   ├── On first occurrence: compute utf8ByteLength() (no allocation)        │
│   └── On second occurrence: cache UTF-8 bytes (utf8Encoder.encode())        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FINALIZE DICTIONARIES                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   For each DictionaryBuilder (finalize):                                    │
│   ├── Pre-allocate sorted array: new Array(map.size)                        │
│   ├── Copy keys into array, sort in place (V8 Timsort: O(n log n))         │
│   ├── Pre-allocate data: new Uint8Array(totalBytes)                         │
│   ├── Pre-allocate offsets: new Int32Array(uniqueCount + 1)                │
│   │                                                                          │
│   │   For each string in sorted order:                                       │
│   │   ├── If cached utf8: data.set(utf8, offset)                            │
│   │   └── Else: encoder.encodeInto(str, data.subarray(offset))             │
│   │                                                                          │
│   └── Build indexMap: Map<string, number> for Pass 2 lookups               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PASS 2: Convert to RecordBatches                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Walk tree again with the same walk                                        │
│                     │                                                        │
│                     ▼                                                        │
│   For each buffer, convert to RecordBatch:                                  │
│   ├── TypedArray columns: output.set(buf.col.subarray(0, writeIdx), offset)│
│   ├── String columns: lookup index in indexMap, write to indices array     │
│   └── Each RecordBatch references the SAME dictionary vectors              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                FINAL                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   new arrow.Table(batches)                                                   │
│   └── Arrow Tables can hold multiple batches sharing dictionaries           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pass 1: Build Dictionaries <a id="smoo/lmao!n/arrow-tree-walker-two-pass.pass1"></a>

Walk the tree. For each buffer encountered:

1. **Count rows** - Accumulate `writeIndex` for total row count
2. **Process string columns** - For each category/text column, add strings to that column's `DictionaryBuilder`

**Key Design**: every `DictionaryBuilder` in a conversion takes the process-wide `globalUtf8Cache` as its `Utf8Encoder`.
The cache is bounded (SIEVE eviction), so UTF-8 encodings are reused across columns and across conversions without
unbounded growth. `DictionaryBuilder`, `Utf8Encoder`, and `utf8ByteLength` live in `arrow-builder`; `Utf8Cache` lives in
lmao and implements `Utf8Encoder`.

```typescript
/** arrow-builder: the encoder a DictionaryBuilder uses for strings the interner does not hold. */
interface Utf8Encoder {
  byteLength(str: string): number;
  encode(str: string): Uint8Array;
  /** Encode into buffer at offset; returns bytes written. */
  encodeInto(str: string, buffer: Uint8Array, offset: number): number;
}

/**
 * lmao: bounded UTF-8 encoding cache with SIEVE eviction (default 4096 entries).
 * encode, byteLength, and encodeInto read the cache and fill it on a miss.
 */
class Utf8Cache implements Utf8Encoder {
  constructor(maxSize?: number);
  encode(str: string): Uint8Array;
  byteLength(str: string): number;
  encodeInto(str: string, buffer: Uint8Array, offset: number): number;
  /** Encode a batch into concatenated bytes plus Arrow offsets (length strings.length + 1). */
  encodeMany(strings: readonly string[]): { data: Uint8Array; offsets: Int32Array };
  get stats(): { hits: number; misses: number; hitRate: number; size: number };
  resetStats(): void;
  clear(): void;
}

declare const globalUtf8Cache: Utf8Cache;
declare function createUtf8Cache(maxSize?: number): Utf8Cache;

/**
 * arrow-builder: UTF-8 byte length without allocating. It counts exactly what TextEncoder writes: a lone surrogate
 * encodes as U+FFFD (3 bytes).
 */
function utf8ByteLength(str: string): number {
  let bytes = 0;
  for (let i = 0; i < str.length; i++) {
    const code = str.charCodeAt(i);
    if (code < 0x80) bytes += 1;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code < 0xdc00 && i + 1 < str.length) {
      const next = str.charCodeAt(i + 1);
      if (next >= 0xdc00 && next < 0xe000) {
        // High surrogate followed by low surrogate: one 4-byte code point
        i++;
        bytes += 4;
      } else {
        // Lone high surrogate: U+FFFD
        bytes += 3;
      }
    } else {
      // BMP code point, or a lone surrogate (U+FFFD)
      bytes += 3;
    }
  }
  return bytes;
}

/**
 * arrow-builder: DictionaryBuilder tracks the unique strings of one column.
 *
 * - Interned string (enum values, module names): reuse its pre-encoded UTF-8
 * - Other first occurrence: compute the byte length only (no encoding allocation)
 * - Second occurrence: encode once through the Utf8Encoder and keep the bytes on the entry
 * - Track totalBytes for exact-size allocation in finalize
 */
class DictionaryBuilder {
  private entries = new Map<string, { utf8?: Uint8Array; byteLength: number; count: number }>();
  private totalBytes = 0;

  constructor(private utf8Encoder: Utf8Encoder = defaultUtf8Encoder) {}

  add(str: string): void {
    const existing = this.entries.get(str);
    if (existing) {
      existing.count++;
      // Second occurrence: likely to repeat, and finalize needs the bytes anyway
      if (existing.count === 2 && !existing.utf8) existing.utf8 = this.utf8Encoder.encode(str);
      return;
    }

    const interned = getInterned(str);
    if (interned) {
      this.entries.set(str, { utf8: interned, byteLength: interned.length, count: 1 });
      this.totalBytes += interned.length;
      return;
    }

    // First occurrence: count bytes only
    const byteLength = utf8ByteLength(str);
    this.entries.set(str, { byteLength, count: 1 });
    this.totalBytes += byteLength;
  }

  get size(): number {
    return this.entries.size;
  }

  finalize(sorted: boolean): FinalizedDictionary; // see Finalize Dictionaries
  clear(): void;
}
```

A first occurrence counts its bytes with `utf8ByteLength` directly and does not ask the encoder: `Utf8Cache.byteLength`
encodes and caches on a miss, which would allocate for strings that appear once.

### Finalize Dictionaries <a id="smoo/lmao!n/arrow-tree-walker-two-pass.finalize"></a>

After Pass 1, `DictionaryBuilder.finalize(sorted)` turns each builder into a `FinalizedDictionary` with exact-size
allocations. Category columns finalize sorted, so their values support binary search; text columns and the trace id
column finalize in insertion order.

```typescript
class FinalizedDictionary {
  constructor(
    readonly data: Uint8Array, // concatenated UTF-8 bytes
    readonly offsets: Int32Array, // Arrow offsets, length uniqueCount + 1
    readonly indexMap: Map<string, number>, // string → dictionary index
    readonly indexArrayCtor: Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor,
    readonly arrowIndexType: IntType // uint8, uint16, or uint32 by dictionary size
  ) {}
}

// DictionaryBuilder.finalize; `encoder` is arrow-builder's module-level TextEncoder
finalize(sorted: boolean): FinalizedDictionary {
  const uniqueCount = this.entries.size;
  const strings = [...this.entries.keys()];
  if (sorted) strings.sort(); // V8's Timsort is O(n log n), hard to beat with manual insertion

  const indexArrayCtor = uniqueCount <= 255 ? Uint8Array : uniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const arrowIndexType = uniqueCount <= 255 ? uint8() : uniqueCount <= 65535 ? uint16() : uint32();

  // Exact-size buffers: totalBytes is the sum of the unique strings' byte lengths
  const data = new Uint8Array(this.totalBytes);
  const offsets = new Int32Array(uniqueCount + 1);
  const indexMap = new Map<string, number>();

  let offset = 0;
  for (let i = 0; i < strings.length; i++) {
    const str = strings[i];
    const entry = this.entries.get(str);
    if (!entry) continue;
    offsets[i] = offset;
    indexMap.set(str, i);

    if (entry.utf8) {
      // Interned or second-occurrence bytes: copy directly
      data.set(entry.utf8, offset);
      offset += entry.utf8.length;
    } else {
      // Encode directly into the output buffer (zero intermediate allocation)
      const result = encoder.encodeInto(str, data.subarray(offset));
      if (result.read !== str.length) {
        // The byte count sized `data` exactly, so a short write is a broken invariant (a byte-count bug),
        // never an input condition: fail loudly instead of truncating this entry and shifting every later one.
        throw new DictionaryEncodeInvariantError(str, result.read, result.written);
      }
      offset += result.written;
    }
  }
  offsets[uniqueCount] = offset;

  return new FinalizedDictionary(data, offsets, indexMap, indexArrayCtor, arrowIndexType);
}
```

Conversion builds each Arrow dictionary vector once from `data` and `offsets`, and every batch of the table references
that one vector.

### Pass 2: Convert Buffers to RecordBatches <a id="smoo/lmao!n/arrow-tree-walker-single-batch"></a>

Walk the tree again, converting each buffer to a RecordBatch using the shared dictionaries:

```typescript
/**
 * Convert a single buffer to a RecordBatch.
 * Uses shared dictionaries from Pass 1 finalization.
 */
function convertBufferToRecordBatch(
  buffer: SpanBuffer,
  schema: arrow.Schema,
  dictionaries: Map<string, FinalizedDictionary>,
  dictionaryVectors: Map<string, arrow.Vector<arrow.Utf8>> // built once per column from data + offsets
): arrow.RecordBatch {
  const length = buffer._writeIndex;
  const vectors: arrow.Vector[] = [];

  for (const field of schema.fields) {
    const columnName = field.name;

    if (isTypedArrayColumn(buffer, columnName)) {
      // TypedArray column: chunk copy
      const srcArray = getTypedArray(buffer, columnName);
      const data = arrow.makeData({
        type: field.type,
        offset: 0,
        length,
        nullCount: getNullCount(buffer, columnName),
        data: srcArray.subarray(0, length),
        nullBitmap: getNullBitmap(buffer, columnName),
      });
      vectors.push(arrow.makeVector(data));
    } else if (isStringColumn(buffer, columnName)) {
      // String column: lookup indices in dictionary
      const dict = dictionaries.get(columnName)!;
      const indices = new dict.indexArrayCtor(length);

      for (let i = 0; i < length; i++) {
        const value = getStringValue(buffer, columnName, i);
        if (value !== null) {
          indices[i] = dict.indexMap.get(value)!;
        }
      }

      const data = arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), dict.arrowIndexType),
        offset: 0,
        length,
        nullCount: getNullCount(buffer, columnName),
        data: indices,
        nullBitmap: getNullBitmap(buffer, columnName),
        dictionary: dictionaryVectors.get(columnName), // SAME vector for all batches!
      });
      vectors.push(arrow.makeVector(data));
    }
  }

  // Create RecordBatch
  const structData = arrow.makeData({
    type: new arrow.Struct(schema.fields),
    length,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });

  return new arrow.RecordBatch(schema, structData);
}
```

### Tree Walking: Allocation-Free Preorder <a id="smoo/lmao!n/arrow-tree-walker-walk"></a>

The walk iterates the trace's topology (`firstChild`/`nextSibling` node arrays) without recursion and without collecting
buffers. Each visit passes the logical span's `RemapDescriptor`, which is `undefined` for a span that is not remapped:

```typescript
type SpanTreeVisitor = (buffer: AnySpanBuffer, remapDescriptor: RemapDescriptor | undefined) => void;

/** Allocation-free iterative preorder: logical node overflow chain, then children in insertion order. */
function walkSpanTree(root: AnySpanBuffer, visitor: SpanTreeVisitor): void {
  const topology = root._traceRoot._topology;
  topology.assertLive(root);
  const subtreeRoot = root._nodeIndex;
  let nodeIndex = subtreeRoot;

  for (;;) {
    // Visit the logical span, then its overflow chain (same span_id, linked via _overflow)
    const logical = topology.buffers[nodeIndex];
    if (!logical) throw new Error('Trace topology references a released span buffer');
    let segment: AnySpanBuffer | undefined = logical;
    while (segment) {
      visitor(segment, logical._remapDescriptor);
      segment = segment._overflow;
    }

    // Descend to the first child, else climb to the next sibling of the nearest ancestor
    const child = topology.firstChild[nodeIndex];
    if (child !== NO_NODE) {
      nodeIndex = child;
      continue;
    }
    for (;;) {
      if (nodeIndex === subtreeRoot) return;
      const sibling = topology.nextSibling[nodeIndex];
      if (sibling !== NO_NODE) {
        nodeIndex = sibling;
        break;
      }
      const current = topology.buffers[nodeIndex];
      if (!current?._parent) throw new Error('Trace topology parent link is missing');
      nodeIndex = current._parent._nodeIndex;
    }
  }
}
```

### Tree Conversion Entry Point <a id="smoo/lmao!n/arrow-tree-walker-two-pass.entry"></a>

One call converts one trace tree into one table:

```typescript
function convertSpanTreeToArrowTable(
  rootBuffer: AnySpanBuffer,
  modulesToLogStats?: CapacityStatsEntry[],
  periodStartNs?: bigint,
  borrowChunks = false
): Table;
```

1. **Pass 0: merge schema fields.** Child spans may carry library schemas, so the walk collects every schema field in
   the tree, first occurrence wins. A remapped span contributes its descriptor's output columns.
2. **Pass 1: build dictionaries.** One `DictionaryBuilder(globalUtf8Cache)` per category column, per text column, and
   one for the trace id.
3. **Finalize.** Category dictionaries finalize sorted; text and trace id dictionaries finalize unsorted.
4. **Pass 2: convert.** Every buffer with rows becomes part of one table that shares the finalized dictionaries.
5. **Capacity stats.** When `modulesToLogStats` is non-empty, the capacity-stats rows for the period starting at
   `periodStartNs` (supplied by the flush scheduler) are appended to the same table.

`borrowChunks` lets JS-heap buffers lend their physical chunks to the table instead of copying them. Only
`convertSpanTreeToLeasedArrowTable(rootBuffer, modulesToLogStats?, periodStartNs?)` sets it, because only an
`ArrowLease` keeps the lent chunks alive; it returns the lease rather than a bare table. A thread-lane view converts
through its native row-store provider instead of the JS walk.

## Why This Approach? <a id="smoo/lmao!n/arrow-tree-walker.rationale"></a>

### 1. No Intermediate Buffer Collection <a id="smoo/lmao!n/arrow-tree-walker.rationale-no-collect"></a>

**Problem**: Collecting all buffers into an array before processing wastes memory and adds allocation overhead.

**Solution**: Walk the tree once per pass with the same allocation-free walk. Only the visitor callback changes between
passes.

**Benefit**: Zero intermediate array allocation. Only the data structures we actually need (dictionaries,
RecordBatches).

### 2. Exact-Size Allocations <a id="smoo/lmao!n/arrow-tree-walker.rationale-exact-size"></a>

**Problem**: Growing arrays/buffers during construction causes repeated reallocations and copies.

**Solution**:

- Pass 1 counts exact sizes (totalUtf8Bytes, uniqueCount, totalRows)
- Finalization allocates with exact sizes
- No resizing, no over-allocation

**Benefit**: Optimal memory usage, no GC pressure from intermediate allocations.

### 3. UTF-8 Caching Strategy <a id="smoo/lmao!n/arrow-tree-walker.utf8-cache"></a>

**Problem**: `TextEncoder.encode()` allocates a new Uint8Array for every call. For repeated strings, this is wasteful.

**Solution**:

- **Shared `Utf8Cache`**: `globalUtf8Cache`, bounded by SIEVE eviction, persists across conversions
- Interned strings (enum values, module names): reuse their pre-encoded bytes
- First occurrence: compute byte length only (no allocation)
- Second occurrence: encode once through the shared cache and keep the bytes on the dictionary entry
- Finalization: copy kept bytes if present, else `encodeInto()` directly into the final buffer

**Benefit**:

- Strings that appear once: zero intermediate allocation (encodeInto directly)
- Strings that appear multiple times: one encode, reused for copy
- Most strings in logging are repeated (module names, span names, etc.)
- Cache persists across conversions: frequent strings stay encoded for as long as the SIEVE cache keeps them

### 4. V8's Native Sort Beats Manual Insertion <a id="smoo/lmao!n/arrow-tree-walker-sort"></a>

**Problem**: Sorted dictionary values enable binary search and better compression.

**Solution**: Copy keys to array, call native sort.

**Why not sorted insertion?**

- Sorted insertion is O(n²) for each insert (shift elements right)
- V8's Timsort is O(n log n) for the whole array
- For 1000 unique strings: sorted insertion = 500,000 operations, Timsort ≈ 10,000 operations

### 5. Chunk Copies for TypedArrays <a id="smoo/lmao!n/arrow-tree-walker.rationale-chunk-copy"></a>

**Problem**: Row-by-row copying is slow due to function call overhead and poor cache utilization.

**Solution**: Use `TypedArray.set()` with `subarray()` for bulk copies:

```typescript
output.set(buffer.col.subarray(0, writeIndex), offset);
```

**Benefit**: V8/Bun optimize this to `memcpy` - orders of magnitude faster than row iteration.

### 6. Shared Dictionary Vectors <a id="smoo/lmao!n/arrow-tree-walker-single-batch.shared-dicts"></a>

**Problem**: Each RecordBatch having its own dictionary wastes memory and breaks DuckDB/ClickHouse optimizations.

**Solution**: All RecordBatches reference the SAME dictionary vectors created during finalization.

**Benefit**:

- Memory: one dictionary per column, not one per batch
- Query engines can optimize dictionary comparisons
- Arrow IPC format supports shared dictionaries

## Tree Traversal Order <a id="smoo/lmao!n/arrow-tree-walker-walk.traversal"></a>

### Depth-First Pre-Order <a id="smoo/lmao!n/arrow-tree-walker-walk.depth-first"></a>

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

### Why Depth-First Pre-Order? <a id="smoo/lmao!n/arrow-tree-walker-walk.depth-first-why"></a>

1. **Trace reconstruction**: Parent spans appear before children → enables streaming reconstruction
2. **Query efficiency**: Related spans (parent + children) co-located in Arrow table
3. **Compression**: Similar data (same trace branch) stored together → better Parquet compression
4. **Natural ordering**: Matches execution order for most synchronous code paths
5. **Memory locality**: Children immediately follow parents → cache-friendly queries

### Buffer Overflow Chain Handling <a id="smoo/lmao!n/arrow-tree-walker-walk.overflow"></a>

Multiple buffers can share the same `span_id` due to buffer overflow:

```typescript
// When a span generates more entries than fit in one buffer:
// - Initial buffer: span_id=5, rowCount=1000, writeIndex=1000
// - Overflow buffer (next): span_id=5, rowCount=500, writeIndex=500
// Both buffers have SAME span_id, different row ranges
```

The walker yields overflow buffers immediately after the primary buffer:

```
Traversal with overflow:
1. span1 primary buffer
2. span2 primary buffer (rows 0-999)
3. span2 overflow buffer (rows 1000-1499)  ← Same span_id!
4. span4 primary buffer
...
```

This ensures all entries for a span are contiguous in the Arrow table.

### Library Integration: RemapDescriptor <a id="smoo/lmao!n/arrow-tree-walker.remapped-view"></a>

When libraries use prefixed schemas (see [Library Integration Pattern](./01e_library_integration_pattern.md)), every
span buffer in the tree is still a canonical SpanBuffer with its own library schema and storage layout. A prefixed or
mapped span carries an immutable `RemapDescriptor` (`packages/lmao/src/lib/logBinding.ts`) that only cold Arrow
conversion reads:

```typescript
/** Dense cold-path remap entry with schema and canonical storage property metadata. */
type RemappedColumn = readonly [
  outputName: string, // 'http_status'
  sourceName: string, // 'status'
  schema: unknown,
  valuesProperty: `${string}_values`, // 'status_values'
  nullsProperty: `${string}_nulls`, // 'status_nulls'
];

interface RemapDescriptor {
  /** Fully composed output-name to canonical source-name lookup. */
  readonly sourceNames: Readonly<Record<string, string>>;
  /** Dense output/source/schema entries in deterministic composition order. */
  readonly columns: readonly RemappedColumn[];
}
```

```
Application Root Buffer (schema: { userId, http_status, http_method })
│
└── child: HTTP Library Buffer (schema: { status, method })
        _remapDescriptor.sourceNames:
          http_status → status
          http_method → method
```

**Why a descriptor and no wrapper**:

1. **Library code** is compiled with unprefixed column names (`status_values`) and writes its own buffer directly
2. **Application schema** has prefixed columns (`http_status`)
3. **Arrow conversion** emits application field names, so it must find each output column's source storage
4. **RemapDescriptor** supplies that mapping as data. No buffer is wrapped, so the hot path never sees a second object
   shape, and the tree holds only canonical buffers.

The walker passes each span's descriptor to the visitor (see Tree Walking). Pass 0 takes a remapped span's output
columns from `columns`; column reads resolve an output name through `sourceNames` and read the canonical
`valuesProperty`/`nullsProperty` of the child buffer. An output name the descriptor does not map is null on a remapped
span's rows; it never falls back to a same-named source column. A span without a descriptor is read by its own field
names. `createRemapDescriptor(sourceSchema, outputToSourceMapping)` (`packages/lmao/src/lib/library.ts`) builds the
descriptor once from the fully composed `.prefix()`/`.mapColumns()` mapping, so nested library composition yields one
descriptor per span, not a chain of views.

### Buffer Registration <a id="smoo/lmao!n/arrow-tree-walker.op-registration"></a>

SpanBuffer constructors do **not** register themselves. The buffer strategy's `createChildSpanBuffer` registers each
child in the trace topology (`topology.registerChild(parent, child)`), and the span setup then attaches the callsite
plan's descriptor to the raw child:

```typescript
// Child span setup (spanContext.ts, simplified)
const childBuffer = bufferStrategy.createChildSpanBuffer(parentBuffer, ...); // registers in the topology
childBuffer._remapDescriptor = callsitePlan.remapDescriptor ?? undefined;
```

The callsite plan owns the descriptor because it already knows the Op's schema and its composed prefix mapping, and the
descriptor is resolved once per plan, never per span.

## One Trace Tree → One Table <a id="smoo/lmao!n/arrow-tree-walker-single-batch.multi-root"></a>

`convertSpanTreeToArrowTable` converts one root buffer's tree into one table whose dictionaries all buffers of that tree
share. The flush scheduler converts each finished trace with its own `convertSpanTreeToLeasedArrowTable` call,
concatenates the resulting tables, and releases the leases once the flush has consumed them. A trace whose conversion
fails is reported and left out; the other traces of the flush still convert.

### Schema Merge <a id="smoo/lmao!n/arrow-tree-walker-single-batch.schema-req"></a>

Buffers in one tree need not share a schema. Library spans keep their canonical library schema, so Pass 0 merges the
schema fields of every buffer in the tree, taking a remapped span's output columns from its `RemapDescriptor`. A field
name seen twice keeps its first schema.

### Benefits of One Table per Tree <a id="smoo/lmao!n/arrow-tree-walker-single-batch.benefits"></a>

- **Dictionary reuse within the trace**: All buffers of the tree share the same dictionary vectors
- **Per-trace failure isolation**: One trace's conversion error never discards another trace's rows
- **No cross-trace schema check**: A flush never needs every trace to come from one schema

### Single Buffer Conversion <a id="smoo/lmao!n/arrow-tree-walker-single-batch.single-buffer"></a>

For simple cases (single buffer, no tree), use `convertToRecordBatch`:

```typescript
/**
 * Convert a single buffer to a RecordBatch.
 * Building block for tree conversion.
 */
function convertToRecordBatch(
  buffer: SpanBuffer,
  schema: arrow.Schema,
  interners: {
    category: StringInterner;
    text: StringInterner;
    module: StringInterner;
    spanName: StringInterner;
  }
): arrow.RecordBatch {
  const length = buffer._writeIndex;
  const vectors: arrow.Vector[] = [];

  // ... convert each column using makeData() ...

  return new arrow.RecordBatch(schema, structData);
}
```

This is the building block used by `convertBuffersToRecordBatch` in Pass 2.

### Span Start Conversion <a id="smoo/lmao!n/arrow-tree-walker-single-batch.span-start"></a>

A reader sees a span that has not ended by its start row. Row 1 of an open span is its completion, pre-armed as
`span-exception` at timestamp 0 so that a span abandoned mid-flight still ends; converting an open span's whole buffer
would report it failed at the epoch. `convertSpanStartToArrowTable(buffer)` converts row 0 alone, as it reads at that
moment, with the same column builders and output schema as `convertToArrowTable`. A host that hands spans over as they
happen converts the start row when the span opens and the whole buffer when it ends; the later copy of the start row
carries the final attribute values, as every later copy does in a streaming flush. The table owns its values, and the
buffer is left as it was. A thread-lane view is refused: its rows live in the native row store, whose streaming flush
already emits an open span's start row without its completion.

## Package Separation <a id="smoo/lmao!n/arrow-tree-walker-package-separation"></a>

### What Lives Where <a id="smoo/lmao!n/arrow-tree-walker-package-separation.what-lives-where"></a>

| Responsibility              | Package       | Why                                                    |
| --------------------------- | ------------- | ------------------------------------------------------ |
| Tree walking                | lmao          | Knows about the trace topology and overflow chains     |
| DictionaryBuilder           | arrow-builder | Generic: unique strings, interner reuse, UTF-8 sizing  |
| Dictionary finalization     | arrow-builder | `DictionaryBuilder.finalize`: exact-size data, offsets |
| Utf8Cache                   | lmao          | Bounded encoding cache passed as the `Utf8Encoder`     |
| Single buffer → RecordBatch | lmao          | Uses lmao's interners, schema, system columns          |
| TypedArray utilities        | arrow-builder | Generic: concatenation, alignment, null bitmaps        |
| ColumnBuffer interface      | arrow-builder | Generic buffer type with writeIndex                    |
| arrow.makeData() usage      | lmao          | Arrow library import, uses arrow-builder's TypedArrays |

### Key Principle <a id="smoo/lmao!n/arrow-tree-walker-package-separation.key-principle"></a>

**arrow-builder doesn't know about trees**. It provides:

- Generic TypedArray utilities
- ColumnBuffer interface
- Null bitmap helpers
- `DictionaryBuilder`, `FinalizedDictionary`, `Utf8Encoder`, and `utf8ByteLength`

**lmao owns tree walking and conversion orchestration**:

- `walkSpanTree()` function
- `Utf8Cache` and `globalUtf8Cache`
- `convertSpanTreeToArrowTable()` orchestration

## Performance Characteristics <a id="smoo/lmao!n/arrow-tree-walker.perf"></a>

### Memory Allocation Profile <a id="smoo/lmao!n/arrow-tree-walker.perf-memory"></a>

| Phase               | Allocations                                  | Size                            |
| ------------------- | -------------------------------------------- | ------------------------------- |
| Pass 1 tree walk    | DictionaryBuilder maps only                  | O(unique strings)               |
| Dictionary finalize | sorted array, data buffer, offsets, indexMap | Exact size (no over-allocation) |
| Pass 2 tree walk    | RecordBatch per buffer                       | O(buffers)                      |
| Final Table         | One Table wrapping batches                   | Minimal overhead                |

### UTF-8 Encoding Cost <a id="smoo/lmao!n/arrow-tree-walker.perf-utf8-cost"></a>

| String Frequency | First Occurrence                                  | Subsequent Occurrences       |
| ---------------- | ------------------------------------------------- | ---------------------------- |
| Once             | utf8ByteLength (no alloc) → encodeInto (no alloc) | N/A                          |
| Twice+           | utf8ByteLength (no alloc)                         | encode() cached → set() copy |

### Comparison to Naive Approach <a id="smoo/lmao!n/arrow-tree-walker.perf-naive"></a>

| Aspect                | Naive (collect buffers)     | Two-Pass                                    |
| --------------------- | --------------------------- | ------------------------------------------- |
| Buffer collection     | O(n) array growth           | Zero (walk twice)                           |
| Dictionary allocation | Unknown size, resize needed | Exact size, one allocation                  |
| UTF-8 encoding        | encode() for every string   | encodeInto() for unique, cache for repeated |
| Sorted dictionary     | Sort after collection       | Sort once after counting                    |

## Integration Points <a id="smoo/lmao!n/arrow-tree-walker.integration"></a>

### Buffer Access Patterns for Conversion <a id="smoo/lmao!n/arrow-tree-walker.integration-access"></a>

When converting SpanBuffers to Arrow, use these patterns for efficient access:

```typescript
// IMPORTANT: Use subarray() for zero-copy conversion
const view = buffer.timestamp.subarray(0, writeIndex); // Zero-copy VIEW

// Check if column was allocated without triggering allocation
const values = buffer.getColumnIfAllocated('userId');
if (values) {
  const arrowData = values.subarray(0, writeIndex); // Zero-copy
}

// Conversion outcomes by column state:
// - Column allocated + written → subarray() with existing null bitmap
// - Column not allocated + scope value → allocate + fill at conversion (cold path)
// - Column not allocated + no scope value → Column omitted entirely (zero cost)
```

**What IS Efficient (Minimal Copy)**:

- Lazy columns never accessed: Zero conversion cost
- TypedArray `subarray()`: Creates a VIEW (no copy)
- Arrow dictionary encoding: Strings deduplicated

**What DOES Copy Data**:

- `TypedArray.slice()`: Creates NEW ArrayBuffer
- Null bitmap transformation
- String dictionary building from `string[]` arrays

### Related Specifications <a id="smoo/lmao!n/arrow-tree-walker.integration-related"></a>

This specification integrates with:

- **[Package Architecture](./00_package_architecture.md)**: Defines arrow-builder vs lmao responsibilities
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Final Arrow schema and column types
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: SpanBuffer layout and TypedArrays
- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Entry type enum and operations
- **[Trace Schema System](./01a_trace_schema_system.md)**: Schema definition and type system

## References <a id="smoo/lmao!n/arrow-tree-walker.references"></a>

- **Implementation**: `/packages/lmao/src/lib/convertToArrow.ts`
- **Buffer types**: `/packages/arrow-builder/src/lib/buffer/types.ts`
- **Package architecture**: `/specs/lmao/00_package_architecture.md`
- **Arrow table structure**: `/specs/lmao/01f_arrow_table_structure.md`
