# Buffer Performance Optimizations

> **📚 PART OF COLUMNAR BUFFER ARCHITECTURE**
>
> This document details the V8 and memory optimization tricks that make columnar buffers 10-100x faster than object-based logging. Read the [main overview](./01b_columnar_buffer_architecture_overview.md) first.

## WHY: V8 Optimizations Matter

JavaScript engines like V8 are highly optimized for specific patterns. By understanding these patterns, we can write code that's as fast as native implementations.

### The Cost of Objects

Traditional logging creates objects:
```javascript
// This looks innocent but is expensive
logger.info({ userId: '123', action: 'login', timestamp: Date.now() });
```

Problems:
1. **Object allocation** - New memory, new garbage
2. **Property bags** - Dynamic property lookup
3. **Polymorphic access** - Different object shapes
4. **Hidden class transitions** - V8 creates new maps
5. **Escape analysis failure** - Objects escape to heap

### TypedArrays: The Fast Path

TypedArrays bypass all these costs:
```javascript
// Direct memory access, no allocations
buffer.timestamps[idx] = Date.now();  // 8 bytes written directly
buffer.userIds[idx] = 123;           // 4 bytes written directly
```

## V8 Optimization Patterns

### 1. Monomorphic Array Access

**WHY**: V8 creates optimized machine code for consistent access patterns.

```typescript
// BAD: Polymorphic access
function writeToBuffer(buffer: any, index: number, value: any) {
  buffer[index] = value; // V8 doesn't know the type
}

// GOOD: Monomorphic access
class SpanBuffer {
  timestamps: Float64Array;
  operations: Uint8Array;
  
  writeTimestamp(idx: number, value: number) {
    this.timestamps[idx] = value; // V8 knows it's Float64Array
  }
  
  writeOperation(idx: number, value: number) {
    this.operations[idx] = value; // V8 knows it's Uint8Array
  }
}
```

**Implementation**:
```typescript
// Generate specialized write methods
function generateWriteMethods(schema: BufferSchema) {
  const methods: any = {};
  
  for (const [name, type] of Object.entries(schema)) {
    methods[`write_${name}`] = new Function('idx', 'value',
      `this.${name}[idx] = value;`
    );
  }
  
  return methods;
}
```

### 2. Hidden Class Stability

**WHY**: V8 creates hidden classes for object shapes. Stable shapes = fast property access.

```typescript
// BAD: Dynamic property addition
class BadBuffer {
  constructor() {
    this.timestamps = new Float64Array(64);
    // Properties added later cause hidden class transitions
  }
  
  addColumn(name: string) {
    this[name] = new Uint32Array(64); // New hidden class!
  }
}

// GOOD: Fixed shape from construction
class GoodBuffer {
  // All properties defined upfront
  readonly timestamps: Float64Array;
  readonly operations: Uint8Array;
  readonly attr_userId: Uint32Array;
  readonly attr_action: Uint32Array;
  
  constructor(capacity: number) {
    // Single hidden class, never changes
    this.timestamps = new Float64Array(capacity);
    this.operations = new Uint8Array(capacity);
    this.attr_userId = new Uint32Array(capacity);
    this.attr_action = new Uint32Array(capacity);
  }
}
```

### 3. Inline Caching

**WHY**: V8 caches property lookups. Consistent call sites = fast access.

```typescript
// BAD: Dynamic property access
function writeAttribute(buffer: any, attr: string, idx: number, value: any) {
  buffer[`attr_${attr}`][idx] = value; // Cache miss on different attrs
}

// GOOD: Static property access
class BufferWriter {
  // Each attribute gets its own method = stable call site
  writeUserId(buffer: SpanBuffer, idx: number, value: number) {
    buffer.attr_userId[idx] = value; // Inline cache hit
  }
  
  writeAction(buffer: SpanBuffer, idx: number, value: number) {
    buffer.attr_action[idx] = value; // Different cache slot
  }
}
```

### 4. Cache Line Alignment

**WHY**: CPUs load memory in 64-byte cache lines. Aligned access = fewer cache misses.

```typescript
// Cache line aware sizing
const CACHE_LINE_SIZE = 64;

function alignedCapacity(requestedSize: number, bytesPerElement: number): number {
  const bytesNeeded = requestedSize * bytesPerElement;
  const cacheLines = Math.ceil(bytesNeeded / CACHE_LINE_SIZE);
  return (cacheLines * CACHE_LINE_SIZE) / bytesPerElement;
}

class AlignedBuffer {
  constructor(capacity: number) {
    // Each array starts on cache line boundary
    this.timestamps = new Float64Array(alignedCapacity(capacity, 8));
    this.operations = new Uint8Array(alignedCapacity(capacity, 1));
    
    // Ensure arrays are cache-aligned in memory
    if (this.timestamps.byteOffset % CACHE_LINE_SIZE !== 0) {
      console.warn('Array not cache-aligned, performance may suffer');
    }
  }
}
```

### 5. Sequential Access Patterns

**WHY**: CPUs prefetch sequential memory. Random access defeats prefetching.

```typescript
// BAD: Random access
class RandomBuffer {
  write(data: LogEntry[]) {
    data.forEach((entry, i) => {
      const idx = Math.floor(Math.random() * this.capacity);
      this.timestamps[idx] = entry.timestamp; // Cache miss likely
    });
  }
}

// GOOD: Sequential access
class SequentialBuffer {
  private writeIndex = 0;
  
  write(data: LogEntry[]) {
    let idx = this.writeIndex;
    
    for (const entry of data) {
      this.timestamps[idx] = entry.timestamp;   // Sequential
      this.operations[idx] = entry.operation;   // Sequential
      this.attr_userId[idx] = entry.userId;     // Sequential
      idx++;
    }
    
    this.writeIndex = idx;
  }
}
```

## String Interning Optimization

### The String Problem

Strings are expensive:
- Variable length = heap allocation
- Comparison = byte-by-byte
- No cache locality
- GC pressure

### Solution: String Interning

```typescript
class StringInterner {
  private strings: string[] = [];
  private indices = new Map<string, number>();
  
  intern(str: string): number {
    let idx = this.indices.get(str);
    
    if (idx === undefined) {
      idx = this.strings.length;
      this.strings.push(str);
      this.indices.set(str, idx);
    }
    
    return idx;
  }
  
  getString(idx: number): string {
    return this.strings[idx];
  }
  
  // Direct Arrow dictionary creation
  toArrowDictionary(): ArrowDictionary {
    return {
      values: this.strings,
      // Dictionary already built during logging!
    };
  }
}

// Usage in buffer
class OptimizedBuffer {
  private interner = new StringInterner();
  attr_action: Uint32Array; // Stores indices, not strings
  
  writeAction(idx: number, action: string) {
    this.attr_action[idx] = this.interner.intern(action);
  }
}
```

### Interning Benefits

1. **Memory**: "login" stored once, not 10,000 times
2. **Speed**: Integer comparison vs string comparison
3. **Arrow**: Direct dictionary creation without scanning
4. **Cache**: Integers fit in cache lines

## Memory Layout Optimization

### Array of Structs vs Struct of Arrays

```typescript
// BAD: Array of Structs (AoS)
interface LogEntry {
  timestamp: number;
  operation: number;
  userId: number;
  action: number;
}
const logs: LogEntry[] = []; // Random memory layout

// GOOD: Struct of Arrays (SoA) - Our approach
class ColumnarBuffer {
  timestamps: Float64Array;  // All timestamps together
  operations: Uint8Array;    // All operations together
  userIds: Uint32Array;      // All userIds together
  actions: Uint32Array;      // All actions together
}
```

**WHY**: 
- Cache efficiency: Reading all timestamps loads only timestamp data
- SIMD potential: Process multiple values in parallel
- Compression: Similar values are adjacent

### Memory Pooling

Prevent allocation churn:

```typescript
class BufferPool {
  private pools = new Map<number, TypedArray[]>();
  
  acquire(size: number, ArrayType: any): TypedArray {
    const pool = this.pools.get(size) || [];
    
    if (pool.length > 0) {
      return pool.pop()!;
    }
    
    return new ArrayType(size);
  }
  
  release(array: TypedArray) {
    const pool = this.pools.get(array.length) || [];
    pool.push(array);
    this.pools.set(array.length, pool);
  }
}

// Reuse arrays during growth
class PooledBuffer {
  private pool = new BufferPool();
  
  grow() {
    const newSize = this.capacity * 2;
    
    // Acquire new arrays from pool
    const newTimestamps = this.pool.acquire(newSize, Float64Array);
    
    // Copy data
    newTimestamps.set(this.timestamps);
    
    // Release old array to pool
    this.pool.release(this.timestamps);
    
    this.timestamps = newTimestamps;
  }
}
```

## Write Path Optimization

### Hot Path Isolation

```typescript
class FastBuffer {
  // Hot path: Just array writes
  writeHot(timestamp: number, operation: number, userId: number) {
    const idx = this.writeIndex++;
    
    // No function calls, no checks, just writes
    this.timestamps[idx] = timestamp;
    this.operations[idx] = operation;
    this.userIds[idx] = userId;
    
    // Capacity check at end (predictable branch)
    if (idx === this.capacity - 1) {
      this._grow(); // Cold path
    }
  }
  
  // Cold path: Growth, compaction, etc
  private _grow() {
    // Expensive operations isolated here
  }
}
```

### Batch Operations

Amortize overhead:

```typescript
class BatchBuffer {
  // Single-write has overhead
  write(entry: LogEntry) {
    // Check capacity, update index, etc
  }
  
  // Batch write amortizes overhead
  writeBatch(entries: LogEntry[]) {
    const startIdx = this.writeIndex;
    const count = entries.length;
    
    // Single capacity check
    if (startIdx + count >= this.capacity) {
      this._ensureCapacity(startIdx + count);
    }
    
    // Tight loop, no per-item overhead
    for (let i = 0; i < count; i++) {
      const idx = startIdx + i;
      const entry = entries[i];
      
      this.timestamps[idx] = entry.timestamp;
      this.operations[idx] = entry.operation;
      this.userIds[idx] = entry.userId;
    }
    
    this.writeIndex = startIdx + count;
  }
}
```

## Null Bitmap Optimization

Handle sparse attributes efficiently:

```typescript
class NullBitmapBuffer {
  // Bit manipulation is fast
  private nullBitmap: Uint32Array;
  
  setNull(idx: number, attrIndex: number) {
    const bitmapIdx = Math.floor(idx / 32);
    const bitPos = idx % 32;
    const attrBit = 1 << attrIndex;
    
    this.nullBitmap[bitmapIdx] |= (attrBit << bitPos);
  }
  
  isNull(idx: number, attrIndex: number): boolean {
    const bitmapIdx = Math.floor(idx / 32);
    const bitPos = idx % 32;
    const attrBit = 1 << attrIndex;
    
    return (this.nullBitmap[bitmapIdx] & (attrBit << bitPos)) !== 0;
  }
}
```

## Benchmarking

### Micro-benchmarks

```typescript
// Measure hot path performance
function benchmarkWrites() {
  const buffer = new SpanBuffer(10000);
  const iterations = 1000000;
  
  console.time('writes');
  
  for (let i = 0; i < iterations; i++) {
    buffer.writeHot(Date.now(), 1, 123);
  }
  
  console.timeEnd('writes');
  
  const nanosPerWrite = (performance.now() * 1e6) / iterations;
  console.log(`${nanosPerWrite.toFixed(1)}ns per write`);
}

// Compare with object allocation
function benchmarkObjects() {
  const logs = [];
  const iterations = 1000000;
  
  console.time('objects');
  
  for (let i = 0; i < iterations; i++) {
    logs.push({
      timestamp: Date.now(),
      operation: 1,
      userId: 123
    });
  }
  
  console.timeEnd('objects');
}
```

### Memory profiling

```typescript
// Track memory usage
function profileMemory() {
  const before = process.memoryUsage();
  
  const buffer = new SpanBuffer(1000000);
  
  // Fill buffer
  for (let i = 0; i < 1000000; i++) {
    buffer.write(Date.now(), 1, i);
  }
  
  const after = process.memoryUsage();
  
  console.log('Memory used:', {
    heap: (after.heapUsed - before.heapUsed) / 1024 / 1024 + ' MB',
    external: (after.external - before.external) / 1024 / 1024 + ' MB',
    perEntry: (after.external - before.external) / 1000000 + ' bytes'
  });
}
```

## Common Pitfalls

### 1. Property Access in Hot Path

```typescript
// BAD
write(entry: LogEntry) {
  const idx = this.writeIndex++;
  this.arrays.timestamps[idx] = entry.timestamp; // Property lookup!
}

// GOOD
write(entry: LogEntry) {
  const idx = this.writeIndex++;
  this.timestamps[idx] = entry.timestamp; // Direct access
}
```

### 2. Type Checks

```typescript
// BAD
write(timestamp: number | Date) {
  if (timestamp instanceof Date) { // Type check in hot path
    timestamp = timestamp.getTime();
  }
}

// GOOD - Type safety at compile time
write(timestamp: number) {
  // No runtime checks needed
}
```

### 3. Bounds Checking

```typescript
// BAD
write(idx: number, value: number) {
  if (idx < 0 || idx >= this.capacity) { // Every write
    throw new Error('Index out of bounds');
  }
  this.data[idx] = value;
}

// GOOD
write(value: number) {
  const idx = this.writeIndex++;
  this.data[idx] = value;
  
  // Single check at end
  if (idx === this.capacity - 1) {
    this._grow();
  }
}
```

## Summary

Buffer performance optimizations leverage:
- **V8 hidden classes** - Stable object shapes
- **Monomorphic access** - Predictable types
- **Cache alignment** - CPU-friendly layout
- **String interning** - Integer comparisons
- **Sequential access** - Prefetch friendly
- **Hot path isolation** - Fast common case

Result: <100ns writes with zero allocations.