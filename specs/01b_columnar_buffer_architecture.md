# Columnar Buffer Architecture

## Overview

The columnar buffer architecture is the core performance engine of the trace logging system. It provides:

1. **Data-oriented design** with columnar TypedArrays for maximum performance
2. **Per-span buffers** - each span gets its own buffer for sorted output and simple implementation
3. **Self-tuning capacity management** that adapts to usage patterns
4. **Buffer chaining for overflow** - part of self-tuning mechanism when capacity is exceeded
5. **Tree-structured spans** with efficient parent-child relationships
6. **Background processing pipeline** for Arrow/Parquet serialization

**Key Design Insight**: Every span gets its own buffer. This eliminates the need for traceId/spanId TypedArrays (they're constant per buffer), keeps logs sorted in Arrow output, and enables zero-copy conversion.

## Design Philosophy

**Key Insight**: Traditional logging creates objects at runtime and serializes them later. This approach separates the hot path (TypedArray writes) from the cold path (background processing), achieving <0.1ms runtime overhead.

**Data-Oriented Principles**:
- All arrays maintain equal length (columnar storage)
- Null bitmaps track which attributes have values
- Push nulls instead of using spanIndex for missing data
- Flat deferred structure, not nested objects
- Cache line alignment for optimal CPU performance

## Cache Line Alignment Benefits

**Memory Performance Optimization**: Each TypedArray is sized to align with 64-byte cache line boundaries, providing several key benefits:

1. **Reduced Cache Misses**: When the CPU accesses array elements, entire cache lines (64 bytes) are loaded. Aligned arrays ensure no partial cache line loads.

2. **Improved Prefetching**: CPU prefetchers work more efficiently with aligned data structures, reducing memory latency.

3. **Better SIMD Performance**: Vectorized operations (when available) perform optimally on cache-aligned data.

4. **Reduced False Sharing**: Different arrays won't share cache lines between CPU cores, eliminating false sharing penalties.

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

**Memory vs Performance Trade-off**: Cache alignment increases memory usage for small arrays but provides significant performance benefits. By starting with 64 elements, we minimize unexpected capacity increases while maintaining cache-friendly allocation patterns.

**Critical Design Decision - Equal Length Constraint**: 
The most important constraint is that ALL TypedArrays in a SpanBuffer must have exactly the same length. This enables:
- Direct row indexing: `buffer.timestamps[i]`, `buffer.operations[i]`, `buffer.attr_userId[i]` all refer to the same logical row
- Zero-copy Arrow conversion: arrays can be sliced directly to Arrow vectors without data copying
- Consistent null bitmap indexing: `buffer.nullBitmap[i]` tracks attribute presence for row `i`

**Cache Alignment Strategy**: 
We calculate alignment using 1-byte elements (worst case) to ensure ALL array types are cache-aligned:
- Uint8Array gets optimal 1 cache line alignment
- Larger types (Uint16Array, Uint32Array, Float64Array) are also aligned (or over-aligned)
- All arrays have identical element count, preserving columnar storage requirements

**Performance Impact**: In high-throughput logging scenarios, this alignment can improve memory bandwidth utilization by 10-30% and reduce CPU cache misses significantly.

## Base SpanBuffer Interface

**Purpose**: Provide a generic interface that can be extended with schema-generated columns.

```typescript
interface SpanBuffer {
  // Core columns - always present
  timestamps: Float64Array;    // Every operation appends timestamp
  operations: Uint8Array;      // Operation type: tag, ok, err, etc.
  
  // Null bitmap - dynamically sized based on attribute count
  nullBitmap: Uint8Array | Uint16Array | Uint32Array;  // Bit flags for which attributes have values
  
  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer;         // Reference to parent SpanBuffer
  task: TaskContext;           // Reference to task + module metadata
  
  // Buffer management
  writeIndex: number;          // Current write position (0 to capacity-1)
  next?: SpanBuffer;           // Chain to next buffer when overflow (part of self-tuning)

  spanId: number;              // Incremental ID for THIS SpanBuffer (assigned at creation)
  traceId: string;             // Root trace ID (constant per span, no TypedArray needed)
  
  // NOTE: Each span gets its own buffer, so traceId and spanId are constant
  // No need for traceId/spanId TypedArrays - they're the same for every row in this buffer
  // This keeps logs sorted and enables zero-copy Arrow conversion
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
  attr_http_status: Uint16Array;        // HTTP status codes
  attr_http_method: Uint8Array;         // enum index for GET/POST/PUT/DELETE
  attr_http_url: Uint32Array;           // string registry index (masked)
  attr_http_duration: Float32Array;     // request duration in ms
  
  // Database library attributes (attr_ prefix prevents conflicts)  
  attr_db_query: Uint32Array;           // string registry index (masked SQL)
  attr_db_duration: Float32Array;       // query duration in ms
  attr_db_rows: Uint32Array;            // rows affected/returned
  attr_db_table: Uint32Array;           // string registry index
  
  // User-defined attributes (attr_ prefix prevents conflicts)
  attr_user_id: Uint32Array;            // string registry index (hashed)
  attr_business_metric: Float64Array;   // custom metric value
}

// attr_ prefix prevents conflicts with SpanBuffer internal fields
// Without prefix, user attribute "parent" would conflict with buffer.parent
// Without prefix, user attribute "task" would conflict with buffer.task  
// Without prefix, user attribute "writeIndex" would conflict with buffer.writeIndex

// Bitmap sizing based on total attribute count
const COMPOSED_ATTR_BITS = {
  http_status: 0,     // Maps to attr_http_status
  http_method: 1,     // Maps to attr_http_method
  http_url: 2,        // Maps to attr_http_url
  http_duration: 3,   // Maps to attr_http_duration
  db_query: 4,        // Maps to attr_db_query
  db_duration: 5,     // Maps to attr_db_duration
  db_rows: 6,         // Maps to attr_db_rows
  db_table: 7,        // Maps to attr_db_table
  user_id: 8,         // Maps to attr_user_id
  business_metric: 9, // Maps to attr_business_metric
  // 10 attributes → Uint16Array bitmap (16 bits available)
};

function createSpanBuffer<T extends TagAttributeSchema>(
  schema: T, 
  taskContext: TaskContext,
  parentBuffer?: SpanBuffer, // Optional parent buffer for tree linking
): SpanBuffer {
  const spanId = nextGlobalSpanId++; // Assign unique ID at creation
  return createEmptySpanBuffer(spanId, schema, taskContext, parentBuffer);
}

function createEmptySpanBuffer<T extends TagAttributeSchema>(
  spanId: number,
  schema: T, 
  taskContext: TaskContext,
  parentBuffer: SpanBuffer,
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
    BitmapType = Uint8Array;   // 8 bits
  } else if (attributeCount <= 16) {
    BitmapType = Uint16Array;  // 16 bits
  } else if (attributeCount <= 32) {
    BitmapType = Uint32Array;  // 32 bits
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
  
  taskContext.module.spanBufferCapacityStats.totalBuffersCreated++;
  return buffer;
}

// Global span ID counter
let nextGlobalSpanId = 1;

function createNextBuffer(buffer: SpanBuffer): SpanBuffer {
  // Buffer chaining is part of the self-tuning mechanism (see 01b2_buffer_self_tuning.md)
  // When a buffer overflows, we chain to a new buffer for the SAME logical span
  // The chained buffer inherits spanId and traceId since it's a continuation
  return createEmptySpanBuffer(
    buffer.spanId,     // Same logical span
    buffer.traceId,    // Same trace
    getSchemaFromBuffer(buffer), // Re-use schema
    buffer.task,       // Re-use task context
    buffer.parent      // Parent is the same as the current buffer's parent
  );
}

function createChildSpan(
  parentBuffer: SpanBuffer, 
  label: string, 
  childFn: SpanFunction
) {
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
- **Memory efficiency**: Optimal bitmap sizing based on attribute count
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
  alignedCapacity: number  // Single capacity for ALL arrays (already cache-aligned)
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

## Generated Tag Functions

```typescript
// Schema compilation generates functions that write to columns
function generateTagFunction(fieldName: string, schema: FieldSchema, allFields: FieldSchema[]) {
  return `
    ${fieldName}: (value) => {
      const index = buffer.length++;
      buffer.timestamps[index] = performance.now();
      buffer.operations[index] = OPERATION_TAG;
      
      // Write to target column
      buffer.attr_${fieldName}[index] = processValue(value);
      
      // Write null sentinels to ALL other deferred columns
      ${allFields.filter(f => f.name !== fieldName && f.type === 'deferred').map(f => 
        `buffer.attr_${f.name}[index] = null;`
      ).join('\n      ')}
    }
  `;
}
```

## Tag Operation Implementation

**Purpose**: Every tag operation writes to ALL columns to maintain equal length.

```typescript
// Generated tag operation for DbSpanBuffer
const tagOperations = {
  requestId: (buffer: DbSpanBuffer, value: string) => {
    const index = buffer.writeIndex++;
    
    // Core columns - always written
    buffer.timestamps[index] = performance.now();
    buffer.operations[index] = OPERATION_TAG;
    
    // Set bit for this attribute in null bitmap
    buffer.nullBitmap[index] |= (1 << DB_ATTR_BITS.requestId);
    
    // Write to THIS attribute's column
    buffer.attr_requestId[index] = hashString(value);
    
    // All OTHER columns get default values (0 for numbers, null for strings)
    // NOTE: This can be optimized by pre-filling the buffer with default values.
    //       Then, each tag operation only needs to write to ONE column.
    buffer.attr_userId[index] = 0;
    buffer.attr_duration[index] = 0;
    buffer.attr_rowCount[index] = 0;
    buffer.attr_operation[index] = 0;
    buffer.attr_query[index] = null;
  },
  
  duration: (buffer: DbSpanBuffer, value: number) => {
    const index = buffer.writeIndex++;
    
    // Core columns
    buffer.timestamps[index] = performance.now();
    buffer.operations[index] = OPERATION_TAG;
    
    // Set bit for duration
    buffer.nullBitmap[index] |= (1 << DB_ATTR_BITS.duration);
    
    // Write to duration column
    buffer.attr_duration[index] = value;
    
    // All other columns get defaults
    buffer.attr_requestId[index] = 0;
    buffer.attr_userId[index] = 0;
    buffer.attr_rowCount[index] = 0;
    buffer.attr_operation[index] = 0;
    buffer.attr_query[index] = null;
  },
  
  // ... other tag operations
};
```

**Why This Approach**:
- **Columnar consistency**: All arrays maintain equal length
- **Null tracking**: Bitmap efficiently tracks which attributes have values
- **Hot path optimization**: Just TypedArray writes and bitwise operations
- **Background processing**: Null bitmap converts directly to Arrow null arrays

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

**Experiment Needed**: Benchmark single vs multiple TypedArrays for memory usage and CPU performance. The ability to directly convert each attribute's TypedArray to an Arrow vector (no copying) is a major advantage of the multi-array approach, especially if the background processor is a separate service (e.g., in Rust).

```typescript
function createModuleContext(config: {
  moduleMetadata: ModuleMetadata,
  tagAttributes: TagAttributeSchema
}) {
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
    }
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
    stats.overflowWrites++;  // Went to a chained buffer (triggers tuning)
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
      reason: 'high_overflow'
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
      reason: 'low_utilization'
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
- **Memory efficient**: Starts small (16 entries), grows only when needed
- **Production ready**: Handles overflow gracefully with chaining
- **Bounded growth**: Won't grow beyond reasonable limits (8-1024 entries)
- **CPU friendly**: Linear search with ternary, no recursion
- **Self-documenting**: System traces its own tuning decisions

## Background Processing Pipeline

**Purpose**: Convert columnar buffers directly to Apache Arrow RecordBatches (zero-copy) and then to Parquet files.

```typescript
import * as arrow from 'apache-arrow'; // Assuming apache-arrow library is available

async function writeSpanBuffersToArrow(buffers: SpanBuffer[]) {
  // 1. Log a snapshot of capacity stats for system monitoring and historical analysis
  logCapacityStats(buffers);
  
  // 2. Create one Arrow RecordBatch per SpanBuffer (zero-copy for TypedArrays)
  //    This step processes each SpanBuffer and converts its internal TypedArrays
  //    into Arrow Vectors, maintaining column-orientation. Span IDs and Parent IDs
  //    are assigned here based on the span tree structure.
  const recordBatches = buffers.map(buffer => createRecordBatch(buffer));
  
  // 3. Create Arrow Table from multiple RecordBatches
  //    Apache Arrow handles concatenation of these batches efficiently (zero-copy for data).
  const arrowTable = new arrow.Table(recordBatches);
  
  // 4. Write the Arrow Table directly to a Parquet file
  await arrow.writeParquet(arrowTable, `traces-${Date.now()}.parquet`);
}

function createRecordBatch(buffer: SpanBuffer): arrow.RecordBatch {
  // Attribute definitions from the module's tag attribute schema. Used to iterate over expected attributes.
  const tagAttributes = buffer.task.module.tagAttributes; 

  // Store constructed Arrow Vectors before creating the RecordBatch
  const arrowVectors: Record<string, arrow.Vector> = {};

  // --- Core SpanBuffer Columns (always present) ---
  // These are directly taken from the SpanBuffer's TypedArrays, sliced to `writeIndex`.
  arrowVectors.spanId = arrow.Int64Vector.from(generateSpanIds(buffer)); // Assigns unique IDs and flattens tree for this buffer
  arrowVectors.parentId = arrow.Int64Vector.from(generateParentIds(buffer)); // Assigns parent IDs for this buffer
  arrowVectors.timestamp = arrow.Float64Vector.from(buffer.timestamps.slice(0, buffer.writeIndex));
  arrowVectors.operation = arrow.Utf8Vector.from(buffer.operations.slice(0, buffer.writeIndex));

  // --- Module Metadata Columns (expanded from shared reference) ---
  // These values are the same for all entries within a single SpanBuffer. They are expanded to full columns.
  arrowVectors.gitSha = arrow.Utf8Vector.from(getModuleMetadataColumn(buffer, 'gitSha'));
  arrowVectors.filePath = arrow.Utf8Vector.from(getModuleMetadataColumn(buffer, 'filePath'));
  // functionNameId needs to be mapped back to string via stringRegistry, then converted to Arrow Vector.
  arrowVectors.functionName = arrow.Utf8Vector.from(getModuleMetadataColumn(buffer, 'functionNameId').map(id => stringRegistry.get(id))); // Assuming stringRegistry is global
  arrowVectors.lineNumber = arrow.Int32Vector.from(getModuleMetadataColumn(buffer, 'lineNumber'));

  // --- Attribute Columns (schema-defined, potentially with nulls) ---
  // Iterate over the fields defined in the specific TagAttributeSchema for this buffer's module.
  for (const [attrName, fieldConfig] of Object.entries(tagAttributes.fields)) {
    const bitPos = (tagAttributes.fields as any)[attrName].bitPosition; // Retrieve bit position for null bitmap

    // Get the raw TypedArray from the buffer using attr_ prefix (prevents conflicts with SpanBuffer internals)
    const rawColumnData = (buffer as any)[`attr_${attrName}`];
    if (!rawColumnData) continue; // Should not happen if generateAttributeColumns is correctly implemented

    // Slice the TypedArray to the actual written length (zero-copy slice)
    const slicedData = rawColumnData.slice(0, buffer.writeIndex);

    // Convert our null bitmap for THIS attribute's column into Arrow's null bitmap format
    const arrowNullBitmap = convertNullBitmapToArrowVector(
      buffer.nullBitmap.slice(0, buffer.writeIndex), // Pass only the relevant part of the buffer's nullBitmap
      buffer.writeIndex, // Number of rows in this segment
      bitPos // The specific bit position for this attribute
    );

    // Create the appropriate Arrow Vector based on the attribute's type defined in the schema.
    // This ensures correct data representation and type safety in the Arrow Table.
    let arrowVector: arrow.Vector;
    switch (fieldConfig.type) {
      case 'string':
      case 'categorical':
      case 'enum': // String-based enums stored as Utf8Vector
        arrowVector = arrow.Utf8Vector.from(slicedData, arrowNullBitmap);
        break;
      case 'number':
        arrowVector = arrow.Float64Vector.from(slicedData, arrowNullBitmap); // Assuming numbers are Float64
        break;
      case 'boolean':
        arrowVector = arrow.BoolVector.from(slicedData, arrowNullBitmap);
        break;
      // Add cases for other complex types (e.g., objects as JSON strings/binary blobs)
      default:
        arrowVector = arrow.Vector.from(slicedData, arrowNullBitmap); // Fallback for 'unknown' or 'any' types
    }
    arrowVectors[attrName] = arrowVector; // Strip attr_ prefix for clean Arrow table columns
  }

  return new arrow.RecordBatch(arrowVectors);
}

// Helper to flatten the SpanBuffer tree into a list of buffers (defined in 01c_context_flow_and_task_wrappers.md)
// function flattenSpanTreeToBuffers(rootBuffer: SpanBuffer): SpanBuffer[] { ... }

// Helper to convert our null bitmap format to Arrow's Uint8Array null vectors
// This function takes a single TypedArray (Uint8Array, Uint16Array, or Uint32Array) which represents
// the null bitmap for a segment of rows in our SpanBuffer. It then extracts the null flags for a
// specific attribute (identified by bitPosition) and converts them into the format expected by Apache Arrow.
function convertNullBitmapToArrowVector(
  nullBitmapBuffer: Uint8Array | Uint16Array | Uint32Array, // The buffer's nullBitmap for a segment of rows
  numRows: number, // Number of rows in this segment
  bitPosition: number // The specific bit position (0-31) for the attribute being processed
): Uint8Array {
  const arrowNullVector = new Uint8Array(Math.ceil(numRows / 8)); // Arrow null vector format (1 bit per value)

  for (let i = 0; i < numRows; i++) {
    // Check if the bit for this attribute is SET in our nullBitmapBuffer.
    // In our SpanBuffer, a SET bit means the value IS PRESENT (non-null).
    // Apache Arrow's null bitmap expects a 1 if the value IS PRESENT (non-null).
    const isPresent = (nullBitmapBuffer[i] & (1 << bitPosition)) !== 0;

    if (isPresent) {
      // Set the corresponding bit in Arrow's null vector if the value is present
      const byteIndex = Math.floor(i / 8);
      const bitOffset = i % 8; // Bit position within the byte (0-7)
      arrowNullVector[byteIndex] |= (1 << bitOffset); // Set the bit
    }
  }
  return arrowNullVector;
}

// Placeholder functions (actual implementation would involve traversing the SpanBuffer tree
// to assign IDs and collect parent IDs dynamically before creating RecordBatches)
function generateSpanIds(buffer: SpanBuffer): number[] { /* ... */ return Array(buffer.writeIndex).fill(0); }
function generateParentIds(buffer: SpanBuffer): number[] { /* ... */ return Array(buffer.writeIndex).fill(0); }

// Helper to extract a column of module metadata (e.g., gitSha) for all rows in a buffer.
// This is used for module-level attributes that are constant per SpanBuffer.
function getModuleMetadataColumn(buffer: SpanBuffer, key: keyof ModuleContext['moduleMetadata'] | 'functionNameId' | 'lineNumber'): any[] { 
  const column: any[] = [];
  for (let i = 0; i < buffer.writeIndex; i++) {
    // Special handling for functionNameId and lineNumber which are on TaskContext, not moduleMetadata
    if (key === 'functionNameId') {
      column.push(buffer.task.functionNameId); // Use task context's functionNameId
    } else if (key === 'lineNumber') {
      column.push(buffer.task.lineNumber); // Use task context's lineNumber
    } else {
      // Access the shared module context and extract the specific metadata field
      column.push((buffer.task.module.moduleMetadata as any)[key]); // Cast to any to access properties dynamically
    }
  }
  return column;
}

function logCapacityStats(buffers: SpanBuffer[]) {
  const seenModules = new Set<number>(); // Track modules whose stats have been logged in this batch

  for (const buffer of buffers) {
    const moduleId = buffer.task.module.moduleId; // Get the raw numeric moduleId
    
    // Log stats for this module only if not already seen in this flush
    if (!seenModules.has(moduleId)) {
      seenModules.add(moduleId);
      
      const moduleStats = buffer.task.module.spanBufferCapacityStats; // Direct access to live stats
      const efficiency = moduleStats.totalWrites / (moduleStats.totalBuffersCreated * moduleStats.currentCapacity);
      const overflowRatio = moduleStats.overflowWrites / moduleStats.totalWrites;
      
      // Trace the capacity stats as structured data to the system trace buffer
      systemTracer.tag.capacityStats({
        moduleId: moduleId.toString(), // Convert to string for the log
        currentCapacity: moduleStats.currentCapacity,
        totalWrites: moduleStats.totalWrites,
        overflowWrites: moduleStats.overflowWrites,
        totalBuffers: moduleStats.totalBuffersCreated,
        efficiency,
        overflowRatio,
        timestamp: Date.now()
      });
    }
  }
}

## Arrow Conversion Integration

**Purpose**: The columnar buffer architecture enables direct, zero-copy conversion to Apache Arrow format.

**Key Integration Points**:

1. **Direct TypedArray Slicing**: SpanBuffer columns slice directly into Arrow vectors with zero data copying
2. **Null Bitmap Mapping**: SpanBuffer null bitmaps convert directly to Arrow's null representation  
3. **Attribute Prefix Stripping**: `attr_` prefixes are removed during Arrow conversion to create clean, queryable column names
4. **String Registry Resolution**: String indices resolve to actual values during Arrow table creation

**Example Conversion**:
```typescript
// SpanBuffer (in-memory)                    →  Arrow Table (queryable)
buffer.attr_http_status: Uint16Array[200]   →  http_status: 200
buffer.attr_db_query: Uint32Array[42]       →  db_query: "SELECT * FROM users WHERE id = ?"  
buffer.nullBitmap: 0b1101                   →  Arrow null bitmap: valid values marked
```

**See Also**: 
- **Arrow Table Structure** (future document): Complete Arrow schema, examples with realistic trace data, ClickHouse query patterns
- **Background Processing Pipeline** (future document): Detailed Arrow/Parquet conversion process, performance optimizations

This columnar architecture ensures that the high-performance buffer operations flow seamlessly into efficient analytical storage and querying.
```
