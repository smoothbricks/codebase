# Tree Walker and Arrow Conversion

## Overview

This specification defines the **two-pass tree conversion** approach for converting SpanBuffer trees to Apache Arrow
tables. The approach is designed for:

- **Zero intermediate allocations**: No buffer collection arrays during tree walking
- **Exact-size allocations**: Pre-calculated sizes for all Arrow buffers
- **UTF-8 caching**: Encode once, copy on reuse for repeated strings
- **Shared dictionaries**: All RecordBatches reference the same dictionary vectors

## Problem Statement

### The Challenge

**SpanBuffers form a tree structure** (spans have children), and arrow-builder shouldn't know about span trees (that's
lmao's concept). When converting to Arrow:

1. **Dictionary building requires full traversal**: Must see all string values before allocating dictionary buffers
2. **RecordBatch creation needs dictionaries**: Can't create batches until dictionaries are finalized
3. **Memory efficiency is critical**: Avoid collecting buffers into intermediate arrays

### Design Goals

1. **No intermediate buffer collection**: Walk the tree twice instead of collecting into an array
2. **Exact-size allocations**: Know total bytes/rows before allocating
3. **UTF-8 optimization**: Cache encodings for repeated strings, use `encodeInto()` for unique ones
4. **Single dictionary per column**: All RecordBatches share the same dictionary vectors

## Solution: Two-Pass Tree Conversion

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PASS 1: Build Dictionaries                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Walk tree with simple recursive function (no buffer array)                │
│                     │                                                        │
│                     ▼                                                        │
│   For each buffer:                                                           │
│   ├── Count rows (accumulate total)                                         │
│   └── For each string column (category/text):                               │
│       └── Add to ColumnDictionary                                           │
│                     │                                                        │
│                     ▼                                                        │
│   ColumnDictionary per column:                                              │
│   ├── Map<string, { utf8?: Uint8Array, count: number }>                     │
│   ├── Track totalUtf8Bytes (sum of unique string byte lengths)             │
│   ├── On first occurrence: compute utf8ByteLength() (no allocation)        │
│   └── On second occurrence: cache UTF-8 encoding (encoder.encode())        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FINALIZE DICTIONARIES                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   For each ColumnDictionary:                                                │
│   ├── Pre-allocate sorted array: new Array(map.size)                        │
│   ├── Copy keys into array, sort in place (V8 Timsort: O(n log n))         │
│   ├── Pre-allocate data: new Uint8Array(totalUtf8Bytes + alignment)        │
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
│   Walk tree again with same recursive function                               │
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

### Pass 1: Build Dictionaries

Walk the tree with a simple recursive function. For each buffer encountered:

1. **Count rows** - Accumulate `writeIndex` for total row count
2. **Process string columns** - For each category/text column, add strings to `ColumnDictionary`

**Key Design**: The `Utf8Cache` is shared across the entire conversion process (passed as argument, not global). This
allows UTF-8 encodings to be reused across columns and across multiple conversion calls if the cache persists.

```typescript
/**
 * Shared UTF-8 encoding cache.
 * Passed to ColumnDictionary and finalization functions.
 * Can be reused across multiple conversions to amortize encoding costs.
 */
class Utf8Cache {
  private cache = new Map<string, Uint8Array>();
  private encoder = new TextEncoder();

  /**
   * Get or compute UTF-8 encoding for a string.
   * Caches the result for future lookups.
   */
  getOrEncode(str: string): Uint8Array {
    let utf8 = this.cache.get(str);
    if (utf8 === undefined) {
      utf8 = this.encoder.encode(str);
      this.cache.set(str, utf8);
    }
    return utf8;
  }

  /**
   * Check if string is already cached (without encoding).
   */
  has(str: string): boolean {
    return this.cache.has(str);
  }

  /**
   * Get cached encoding if available.
   */
  get(str: string): Uint8Array | undefined {
    return this.cache.get(str);
  }

  /**
   * Get the encoder for encodeInto() calls.
   */
  get textEncoder(): TextEncoder {
    return this.encoder;
  }
}

/**
 * ColumnDictionary tracks unique strings for a single column.
 *
 * Key optimizations:
 * - First occurrence: compute byte length only (no UTF-8 encoding allocation)
 * - Second occurrence: cache UTF-8 encoding via shared Utf8Cache
 * - Track totalUtf8Bytes for exact-size allocation later
 */
class ColumnDictionary {
  private map = new Map<string, { count: number }>();
  private totalUtf8Bytes = 0;

  constructor(private utf8Cache: Utf8Cache) {}

  /**
   * Add a string value to the dictionary.
   */
  add(value: string): void {
    const entry = this.map.get(value);

    if (entry === undefined) {
      // First occurrence: compute byte length only (no allocation)
      const byteLength = this.utf8ByteLength(value);
      this.totalUtf8Bytes += byteLength;
      this.map.set(value, { count: 1 });
    } else {
      entry.count++;

      // Second occurrence: cache the UTF-8 encoding in shared cache
      // Rationale: if it appeared twice, likely to appear more times
      // and we'll need the encoding during finalization anyway
      if (entry.count === 2 && !this.utf8Cache.has(value)) {
        this.utf8Cache.getOrEncode(value); // Cache it
      }
    }
  }

  /**
   * Compute UTF-8 byte length without allocating.
   */
  private utf8ByteLength(str: string): number {
    // Check cache first - if already encoded, use that length
    const cached = this.utf8Cache.get(str);
    if (cached) return cached.length;

    // Fast path for ASCII-only strings
    let bytes = 0;
    for (let i = 0; i < str.length; i++) {
      const code = str.charCodeAt(i);
      if (code < 0x80) bytes += 1;
      else if (code < 0x800) bytes += 2;
      else if (code < 0xd800 || code >= 0xe000) bytes += 3;
      else {
        // Surrogate pair
        i++;
        bytes += 4;
      }
    }
    return bytes;
  }

  get size(): number {
    return this.map.size;
  }

  get utf8TotalBytes(): number {
    return this.totalUtf8Bytes;
  }

  keys(): IterableIterator<string> {
    return this.map.keys();
  }
}
```

### Finalize Dictionaries

After Pass 1, finalize each dictionary with exact-size allocations:

```typescript
interface FinalizedDictionary {
  /** Arrow dictionary vector (shared by all RecordBatches) */
  vector: arrow.Vector<arrow.Utf8>;
  /** Lookup: string → dictionary index */
  indexMap: Map<string, number>;
  /** Index data type (Uint8, Uint16, or Uint32 based on dictionary size) */
  indexType: arrow.Uint8 | arrow.Uint16 | arrow.Uint32;
}

function finalizeDictionary(dict: ColumnDictionary, utf8Cache: Utf8Cache): FinalizedDictionary {
  const uniqueCount = dict.size;

  // 1. Pre-allocate sorted array with exact size
  const sorted: string[] = new Array(uniqueCount);
  let i = 0;
  for (const key of dict.keys()) {
    sorted[i++] = key;
  }

  // 2. Sort in place - V8's Timsort is O(n log n), hard to beat with manual insertion
  sorted.sort();

  // 3. Determine index type based on dictionary size
  const indexType =
    uniqueCount <= 255 ? new arrow.Uint8() : uniqueCount <= 65535 ? new arrow.Uint16() : new arrow.Uint32();

  // 4. Pre-allocate UTF-8 data buffer with exact size (+ 64-byte alignment padding)
  const alignedSize = Math.ceil(dict.utf8TotalBytes / 64) * 64;
  const data = new Uint8Array(alignedSize);

  // 5. Pre-allocate offsets array with exact size
  const offsets = new Int32Array(uniqueCount + 1);

  // 6. Build index map and encode strings
  const indexMap = new Map<string, number>();
  let offset = 0;
  const encoder = utf8Cache.textEncoder;

  for (let idx = 0; idx < sorted.length; idx++) {
    const str = sorted[idx];
    indexMap.set(str, idx);
    offsets[idx] = offset;

    // Check shared UTF-8 cache first
    const cached = utf8Cache.get(str);

    if (cached) {
      // Cached UTF-8: copy directly
      data.set(cached, offset);
      offset += cached.length;
    } else {
      // Not cached: encode directly into buffer (zero intermediate allocation)
      const result = encoder.encodeInto(str, data.subarray(offset));
      offset += result.written!;
    }
  }
  offsets[uniqueCount] = offset;

  // 7. Create Arrow dictionary vector
  const dictData = arrow.makeData({
    type: new arrow.Utf8(),
    offset: 0,
    length: uniqueCount,
    nullCount: 0,
    data: data.subarray(0, offset),
    valueOffsets: offsets,
  });
  const vector = arrow.makeVector(dictData);

  return { vector, indexMap, indexType };
}
```

### Pass 2: Convert Buffers to RecordBatches

Walk the tree again, converting each buffer to a RecordBatch using the shared dictionaries:

```typescript
/**
 * Convert a single buffer to a RecordBatch.
 * Uses shared dictionaries from Pass 1 finalization.
 */
function convertBufferToRecordBatch(
  buffer: SpanBuffer,
  schema: arrow.Schema,
  dictionaries: Map<string, FinalizedDictionary>
): arrow.RecordBatch {
  const length = buffer.writeIndex;
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
      const indices = new (getIndexArrayType(dict.indexType))(length);

      for (let i = 0; i < length; i++) {
        const value = getStringValue(buffer, columnName, i);
        if (value !== null) {
          indices[i] = dict.indexMap.get(value)!;
        }
      }

      const data = arrow.makeData({
        type: new arrow.Dictionary(new arrow.Utf8(), dict.indexType),
        offset: 0,
        length,
        nullCount: getNullCount(buffer, columnName),
        data: indices,
        nullBitmap: getNullBitmap(buffer, columnName),
        dictionary: dict.vector, // SAME vector for all batches!
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

### Tree Walking: Simple Recursive Function

The tree walking is a simple recursive function - no intermediate buffer collection:

```typescript
/**
 * Walk span tree depth-first, calling visitor for each buffer.
 * Handles overflow chains (buffer.next) automatically.
 *
 * Note: Children may be RemappedBufferViews (for library integration).
 * RemappedBufferView implements the same interface as SpanBuffer for
 * read-only traversal - see 01e_library_integration_pattern.md.
 */
function walkSpanTree(root: SpanBuffer, visitor: (buffer: SpanBuffer) => void): void {
  // Visit this buffer (or RemappedBufferView)
  visitor(root);

  // Visit overflow chain (same spanId, linked via .next)
  let overflow = root.next as SpanBuffer | undefined;
  while (overflow) {
    visitor(overflow);
    overflow = overflow.next as SpanBuffer | undefined;
  }

  // Recursively visit children (depth-first)
  // Children may be SpanBuffer or RemappedBufferView
  for (const child of root.children) {
    walkSpanTree(child, visitor);
  }
}

/**
 * Convert multiple span trees to a single Arrow RecordBatch using two-pass approach.
 *
 * **Schema Requirement**: All root buffers must share the same schema. The application composes
 * all library schemas into a single ModuleContext at startup, so all buffers created from that
 * module share the same logSchema schema. This requirement is enforced at runtime.
 *
 * **Why Single RecordBatch**: Converting multiple root buffers (e.g., multiple HTTP requests)
 * into a single RecordBatch maximizes dictionary reuse - all buffers share the same dictionary
 * vectors, reducing memory and improving query performance.
 *
 * @param rootBuffers - Array of root SpanBuffers (one per request/trace tree)
 * @param schema - Arrow schema for the output table (from first root buffer's module)
 * @param utf8Cache - Shared UTF-8 cache (can be reused across conversions)
 */
function convertSpanTreeToArrowTable(
  rootBuffers: SpanBuffer[],
  schema: arrow.Schema,
  utf8Cache: Utf8Cache
): arrow.Table {
  if (rootBuffers.length === 0) {
    return new arrow.Table();
  }

  // Schema requirement: All buffers must share the same schema
  // This is enforced because the application composes all library schemas into one ModuleContext
  const expectedSchema = rootBuffers[0].op.module.logSchema;
  for (let i = 1; i < rootBuffers.length; i++) {
    if (rootBuffers[i].op.module.logSchema !== expectedSchema) {
      throw new Error(
        `Schema mismatch: All buffers in a flush must share the same schema. ` +
          `Buffer 0 has schema from module ${rootBuffers[0].op.module.packageName}, ` +
          `but buffer ${i} has schema from module ${rootBuffers[i].op.module.packageName}`
      );
    }
  }

  // Collect dictionaries per string column
  const columnDicts = new Map<string, ColumnDictionary>();
  let totalRows = 0;

  // Pass 1: Build dictionaries from ALL root buffers (uses shared utf8Cache)
  for (const rootBuffer of rootBuffers) {
    walkSpanTree(rootBuffer, (buffer) => {
      totalRows += buffer.writeIndex;

      for (const field of schema.fields) {
        if (isStringColumn(buffer, field.name)) {
          let dict = columnDicts.get(field.name);
          if (!dict) {
            dict = new ColumnDictionary(utf8Cache); // Shared cache!
            columnDicts.set(field.name, dict);
          }

          // Add all string values from this buffer
          for (let i = 0; i < buffer.writeIndex; i++) {
            const value = getStringValue(buffer, field.name, i);
            if (value !== null) {
              dict.add(value);
            }
          }
        }
      }
    });
  }

  // Finalize dictionaries (uses same shared utf8Cache)
  const finalDicts = new Map<string, FinalizedDictionary>();
  for (const [name, dict] of columnDicts) {
    finalDicts.set(name, finalizeDictionary(dict, utf8Cache));
  }

  // Pass 2: Collect all buffers from ALL root trees, convert to single RecordBatch
  const allBuffers: SpanBuffer[] = [];
  for (const rootBuffer of rootBuffers) {
    walkSpanTree(rootBuffer, (buffer) => {
      if (buffer.writeIndex > 0) {
        allBuffers.push(buffer);
      }
    });
  }

  // Convert all buffers to a single RecordBatch with shared dictionaries
  // This maximizes dictionary reuse across all requests/traces in the flush
  const recordBatch = convertBuffersToRecordBatch(allBuffers, schema, finalDicts);

  // Create Table from single RecordBatch
  return new arrow.Table([recordBatch]);
}
```

## Why This Approach?

### 1. No Intermediate Buffer Collection

**Problem**: Collecting all buffers into an array before processing wastes memory and adds allocation overhead.

**Solution**: Walk the tree twice with a simple recursive function. The function is the same for both passes - just the
visitor callback changes.

**Benefit**: Zero intermediate array allocation. Only the data structures we actually need (dictionaries,
RecordBatches).

### 2. Exact-Size Allocations

**Problem**: Growing arrays/buffers during construction causes repeated reallocations and copies.

**Solution**:

- Pass 1 counts exact sizes (totalUtf8Bytes, uniqueCount, totalRows)
- Finalization allocates with exact sizes
- No resizing, no over-allocation

**Benefit**: Optimal memory usage, no GC pressure from intermediate allocations.

### 3. UTF-8 Caching Strategy

**Problem**: `TextEncoder.encode()` allocates a new Uint8Array for every call. For repeated strings, this is wasteful.

**Solution**:

- **Shared `Utf8Cache`**: Passed as argument, can persist across multiple conversions
- First occurrence: compute byte length only (no allocation)
- Second occurrence: cache the UTF-8 encoding in shared cache
- Finalization: use cached encoding if available, else `encodeInto()` directly into final buffer

**Benefit**:

- Strings that appear once: zero intermediate allocation (encodeInto directly)
- Strings that appear multiple times: one encode, reused for copy
- Most strings in logging are repeated (module names, span names, etc.)
- Cache persists across conversions: module/span names encoded once across entire application lifetime

### 4. V8's Native Sort Beats Manual Insertion

**Problem**: Sorted dictionary values enable binary search and better compression.

**Solution**: Copy keys to array, call native sort.

**Why not sorted insertion?**

- Sorted insertion is O(n²) for each insert (shift elements right)
- V8's Timsort is O(n log n) for the whole array
- For 1000 unique strings: sorted insertion = 500,000 operations, Timsort ≈ 10,000 operations

### 5. Chunk Copies for TypedArrays

**Problem**: Row-by-row copying is slow due to function call overhead and poor cache utilization.

**Solution**: Use `TypedArray.set()` with `subarray()` for bulk copies:

```typescript
output.set(buffer.col.subarray(0, writeIndex), offset);
```

**Benefit**: V8/Bun optimize this to `memcpy` - orders of magnitude faster than row iteration.

### 6. Shared Dictionary Vectors

**Problem**: Each RecordBatch having its own dictionary wastes memory and breaks DuckDB/ClickHouse optimizations.

**Solution**: All RecordBatches reference the SAME dictionary vectors created during finalization.

**Benefit**:

- Memory: one dictionary per column, not one per batch
- Query engines can optimize dictionary comparisons
- Arrow IPC format supports shared dictionaries

## Tree Traversal Order

### Depth-First Pre-Order

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

### Buffer Overflow Chain Handling

Multiple buffers can share the same `spanId` due to buffer overflow:

```typescript
// When a span generates more entries than fit in one buffer:
// - Initial buffer: spanId=5, rowCount=1000, writeIndex=1000
// - Overflow buffer (next): spanId=5, rowCount=500, writeIndex=500
// Both buffers have SAME spanId, different row ranges
```

The walker yields overflow buffers immediately after the primary buffer:

```
Traversal with overflow:
1. span1 primary buffer
2. span2 primary buffer (rows 0-999)
3. span2 overflow buffer (rows 1000-1499)  ← Same spanId!
4. span4 primary buffer
...
```

This ensures all entries for a span are contiguous in the Arrow table.

### Library Integration: RemappedBufferView

When libraries use prefixed schemas (see [Library Integration Pattern](./01e_library_integration_pattern.md)), the tree
may contain **RemappedBufferView** objects instead of raw SpanBuffers:

```
Application Root Buffer (schema: { userId, http_status, http_method })
│
└── children[0]: RemappedBufferView  ← NOT a raw SpanBuffer
        │
        │   Maps prefixed → unprefixed:
        │   - http_status → status
        │   - http_method → method
        │
        └── wraps: HTTP Library Buffer (schema: { status, method })
```

**Why RemappedBufferView exists**:

1. **Library code** is compiled with unprefixed column names (`status_values`)
2. **Application schema** has prefixed columns (`http_status`)
3. **Arrow conversion** iterates using application schema field names
4. **RemappedBufferView** bridges the gap by mapping prefixed → unprefixed in `getColumnIfAllocated()`

**Tree walker doesn't need to distinguish** - RemappedBufferView implements the same read-only interface:

```typescript
// Both SpanBuffer and RemappedBufferView support:
interface TreeTraversable {
  children: TreeTraversable[];
  next: TreeTraversable | undefined;
  writeIndex: number;
  timestamps: BigInt64Array;
  operations: Uint8Array;
  traceId: TraceId;
  threadId: bigint;
  spanId: number;
  parentSpanId: number;
  op: SpanContext;
  getColumnIfAllocated(name: string): ColumnValueType | undefined;
  getNullsIfAllocated(name: string): Uint8Array | undefined;
}
```

**Column access through RemappedBufferView**:

```typescript
// Arrow conversion iterates with prefixed names from root schema
for (const [fieldName, _] of schemaFields) {
  // fieldName = 'http_status'
  const col = buffer.getColumnIfAllocated(fieldName);

  // If buffer is RemappedBufferView:
  // 1. Maps 'http_status' → 'status'
  // 2. Calls ownBuffer.getColumnIfAllocated('status')
  // 3. Returns the actual TypedArray

  // If buffer is regular SpanBuffer (no http library columns):
  // Returns undefined (column not allocated)
}
```

**Nested library calls** work naturally - each library registers its own RemappedBufferView:

```
App Root → RemappedBufferView(HTTP) → RemappedBufferView(Auth) → Auth Buffer
           http_status → status       auth_token → token
```

### Op's Responsibility: Buffer Registration

SpanBuffer constructors do **not** auto-register with parent's `children[]` array. The **Op's wrapper** handles
registration explicitly:

```typescript
// Inside op's wrapper (conceptual):
async invoke(parentCtx, spanName, line, ...args) {
  // Op creates its own SpanBuffer with unprefixed schema
  const ownBuffer = createSpanBuffer(unprefixedSchema, callsite, traceId);

  // Op registers with parent - wrap with RemappedBufferView if prefixed
  if (prefix && parentCtx?.buffer) {
    const remappedView = new RemappedViewClass(ownBuffer);
    parentCtx.buffer.children.push(remappedView); // Register the view, not raw buffer
  } else if (parentCtx?.buffer) {
    parentCtx.buffer.children.push(ownBuffer);
  }

  // ... execute user function
}
```

**Why op owns registration (not span())?**

1. **Schema knowledge**: Op knows its module's schema and prefix mapping
2. **RemappedBufferView creation**: Op creates the view if prefixed
3. **Testing flexibility**: Ops can be tested without polluting parent's children
4. **Clear control flow**: No hidden side effects - registration happens in op wrapper

**Where registration happens**:

| Location              | Description                        |
| --------------------- | ---------------------------------- |
| Op's internal wrapper | Primary registration point         |
| `span()` inline child | Delegates to op wrapper            |
| Library `.use()` impl | Op wrapper with RemappedBufferView |

## Multiple Root Buffers → Single RecordBatch

**Key Design Decision**: Multiple root buffers (e.g., multiple HTTP requests) are converted into a **single
RecordBatch** rather than multiple RecordBatches. This maximizes dictionary reuse - all buffers share the same
dictionary vectors.

### Schema Requirement

**All buffers in a flush must share the same schema.** This is enforced because:

1. **Application Composition**: The application declares dependencies on libraries and wires them with prefixes:

   ```typescript
   // Define module with dependencies (see 01l_module_builder_pattern.md)
   const appModule = defineModule({
     metadata: { ... },
     schema: appSchema,
     deps: { http: httpModule, db: dbModule },
   });

   // Wire dependencies with prefixes at use time
   const appRoot = appModule.use({
     http: httpModule.prefix('http').use(),
     db: dbModule.prefix('db').use(),
   });
   ```

2. **Per-Module Schema**: Each module has one unified schema. All buffers created from that module share the same
   schema. Library buffers use RemappedBufferView to map prefixed names to unprefixed columns.

3. **Runtime Enforcement**: The conversion function validates that all root buffers share the same schema, throwing an
   error if they differ.

### Benefits of Single RecordBatch

- **Maximum Dictionary Reuse**: All buffers share the same dictionary vectors, reducing memory
- **Better Query Performance**: Query engines can optimize dictionary lookups across all data
- **Simpler Implementation**: No need to union schemas or handle missing columns
- **Efficient Flushing**: One RecordBatch per flush cycle, matching the flush scheduler's design

### Single Buffer Conversion

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
  const length = buffer.writeIndex;
  const vectors: arrow.Vector[] = [];

  // ... convert each column using makeData() ...

  return new arrow.RecordBatch(schema, structData);
}
```

This is the building block used by `convertBuffersToRecordBatch` in Pass 2.

## Package Separation

### What Lives Where

| Responsibility              | Package       | Why                                                    |
| --------------------------- | ------------- | ------------------------------------------------------ |
| Tree walking                | lmao          | Knows about SpanBuffer.children, SpanBuffer.next       |
| ColumnDictionary            | lmao          | Knows about category/text types from schema            |
| Dictionary finalization     | lmao          | Builds Arrow Dictionary vectors with lmao's schema     |
| Single buffer → RecordBatch | lmao          | Uses lmao's interners, schema, system columns          |
| TypedArray utilities        | arrow-builder | Generic: concatenation, alignment, null bitmaps        |
| ColumnBuffer interface      | arrow-builder | Generic buffer type with writeIndex                    |
| arrow.makeData() usage      | lmao          | Arrow library import, uses arrow-builder's TypedArrays |

### Key Principle

**arrow-builder doesn't know about trees**. It provides:

- Generic TypedArray utilities
- ColumnBuffer interface
- Null bitmap helpers

**lmao owns tree walking and dictionary building**:

- `walkSpanTree()` function
- `ColumnDictionary` class
- `convertSpanTreeToArrowTable()` orchestration

## Performance Characteristics

### Memory Allocation Profile

| Phase               | Allocations                                  | Size                            |
| ------------------- | -------------------------------------------- | ------------------------------- |
| Pass 1 tree walk    | ColumnDictionary maps only                   | O(unique strings)               |
| Dictionary finalize | sorted array, data buffer, offsets, indexMap | Exact size (no over-allocation) |
| Pass 2 tree walk    | RecordBatch per buffer                       | O(buffers)                      |
| Final Table         | One Table wrapping batches                   | Minimal overhead                |

### UTF-8 Encoding Cost

| String Frequency | First Occurrence                                  | Subsequent Occurrences       |
| ---------------- | ------------------------------------------------- | ---------------------------- |
| Once             | utf8ByteLength (no alloc) → encodeInto (no alloc) | N/A                          |
| Twice+           | utf8ByteLength (no alloc)                         | encode() cached → set() copy |

### Comparison to Naive Approach

| Aspect                | Naive (collect buffers)     | Two-Pass                                    |
| --------------------- | --------------------------- | ------------------------------------------- |
| Buffer collection     | O(n) array growth           | Zero (walk twice)                           |
| Dictionary allocation | Unknown size, resize needed | Exact size, one allocation                  |
| UTF-8 encoding        | encode() for every string   | encodeInto() for unique, cache for repeated |
| Sorted dictionary     | Sort after collection       | Sort once after counting                    |

## Integration Points

This specification integrates with:

- **[Package Architecture](./00_package_architecture.md)**: Defines arrow-builder vs lmao responsibilities
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Final Arrow schema and column types
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: SpanBuffer layout and TypedArrays
- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Entry type enum and operations
- **[Trace Schema System](./01a_trace_schema_system.md)**: Schema definition and type system

## References

- **Implementation**: `/packages/lmao/src/lib/convertToArrow.ts`
- **Buffer types**: `/packages/arrow-builder/src/lib/buffer/types.ts`
- **Package architecture**: `/specs/00_package_architecture.md`
- **Arrow table structure**: `/specs/01f_arrow_table_structure.md`
