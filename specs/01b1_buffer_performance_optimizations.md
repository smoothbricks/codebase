# Buffer Performance Optimizations

> **📚 PART OF COLUMNAR BUFFER ARCHITECTURE**
>
> This document details the V8 and memory optimization tricks that make columnar buffers 10-100x faster than
> object-based logging. Read the [main overview](./01b_columnar_buffer_architecture_overview.md) first.

## WHY: V8 Optimizations Matter

JavaScript engines like V8 are highly optimized for specific patterns. By understanding these patterns, we can write
code that's as fast as native implementations.

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
buffer.timestamps[idx] = Date.now(); // 8 bytes written directly
buffer.userIds[idx] = 123; // 4 bytes written directly
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
  timestamps: BigInt64Array;
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
    methods[`write_${name}`] = new Function('idx', 'value', `this.${name}[idx] = value;`);
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
    this.timestamps = new BigInt64Array(64);
    // Properties added later cause hidden class transitions
  }

  addColumn(name: string) {
    this[name] = new Uint32Array(64); // New hidden class!
  }
}

// GOOD: Fixed shape from construction
class GoodBuffer {
  // All properties defined upfront
  readonly timestamps: BigInt64Array;
  readonly operations: Uint8Array;
  readonly attr_userId: Uint32Array;
  readonly attr_action: Uint32Array;

  constructor(capacity: number) {
    // Single hidden class, never changes
    this.timestamps = new BigInt64Array(capacity);
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
    this.timestamps = new BigInt64Array(alignedCapacity(capacity, 8));
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
      this.timestamps[idx] = entry.timestamp; // Sequential
      this.operations[idx] = entry.operation; // Sequential
      this.attr_userId[idx] = entry.userId; // Sequential
      idx++;
    }

    this.writeIndex = idx;
  }
}
```

## String Column Optimization

### The String Problem

Strings are expensive:

- Variable length = heap allocation
- Comparison = byte-by-byte
- No cache locality
- GC pressure
- UTF-8 conversion is CPU-intensive

### Hot Path vs Cold Path: Deferred Processing

**CRITICAL DESIGN PRINCIPLE**: The hot path (logging) should be as lightweight as possible. All expensive string
operations (dictionary building, UTF-8 encoding, sorting) are deferred to the cold path (Arrow conversion).

```
HOT PATH (logging)                    COLD PATH (Arrow conversion)
────────────────────                  ────────────────────────────
• ENUM: Map lookup → Uint8 write      • Zero work (pre-built)
• CATEGORY: string[] assignment       • Sort + dedupe + UTF-8 encode
• TEXT: string[] assignment           • 2-pass conditional dictionary

No interning, no UTF-8, no sorting    All dictionary/UTF-8 work here
```

**Why this matters**: A busy service might log 100,000+ entries per second. Even a simple `Map.get()` call for string
interning adds measurable overhead. By storing raw JS strings and deferring dictionary building to flush time, we keep
logging latency minimal.

### Three String Types, Three Strategies

The system provides three string types with different performance/memory tradeoffs. **See
[01a_trace_schema_system.md](./01a_trace_schema_system.md) for complete API documentation.**

#### 1. ENUM: Pre-built Sorted Dictionary (Zero Hot-Path Cost)

**Use Case**: Known values at compile time (entry types, log levels, status codes)

**Key Insight**: All work done ONCE at startup. Hot path is just a Map lookup + array write.

```typescript
// Schema definition
const schema = {
  entryType: S.enum(['span-start', 'span-ok', 'span-err', 'info', 'debug']),
  logLevel: S.enum(['debug', 'info', 'warn', 'error']),
};

// Implementation
class EnumColumn {
  // IMMUTABLE after construction
  private readonly sortedDictionary: string[];
  private readonly utf8Dictionary: Uint8Array[]; // Pre-encoded at startup
  private readonly reverseMap: Map<string, number>;

  // Mutable: per-entry indices
  private values: Uint8Array;

  constructor(possibleValues: string[], capacity: number) {
    // ═══════════════════════════════════════════════════════════
    // STARTUP (once): Sort + UTF-8 encode ALL values
    // ═══════════════════════════════════════════════════════════
    this.sortedDictionary = [...possibleValues].sort();
    this.utf8Dictionary = this.sortedDictionary.map((s) => new TextEncoder().encode(s));
    this.reverseMap = new Map(this.sortedDictionary.map((v, i) => [v, i]));
    this.values = new Uint8Array(capacity);
  }

  // HOT PATH: Map lookup + array write (zero allocation)
  write(idx: number, value: string): void {
    const dictIdx = this.reverseMap.get(value);
    if (dictIdx === undefined) {
      throw new Error(`Invalid enum value: ${value}`);
    }
    this.values[idx] = dictIdx;
  }

  // COLD PATH: Zero work - everything pre-computed
  toArrow(): ArrowColumn {
    return {
      type: 'dictionary',
      indices: this.values.slice(0, this.writeIndex),
      dictionary: this.utf8Dictionary, // Already sorted + UTF-8 encoded!
    };
  }
}
```

**Why Sorted Dictionary:**

- Enables binary search for "does value X exist?" queries
- Arrow/Parquet can skip unused dictionary entries
- ClickHouse can push down predicates efficiently

**Performance Characteristics:** | Operation | Cost | Allocations | |-----------|------|-------------| | Startup | O(n
log n) sort + O(n) UTF-8 encode | Dictionary arrays | | Hot path write | O(1) Map lookup + array write | Zero | | Cold
path flush | O(1) slice | Index array copy |

#### 2. CATEGORY: Raw String Storage + Cold-Path Dictionary (Repeated Values)

**Use Case**: Runtime values that repeat (user IDs, actions, regions)

**Key Insight**: Store raw JS strings in `string[]` on hot path (zero cost). Build sorted dictionary with SIEVE-cached
UTF-8 encoding only during Arrow conversion (cold path). **NO interning on hot path.**

```typescript
// Schema definition
const schema = {
  userId: S.category(), // Same users appear multiple times
  action: S.category(), // 'login', 'logout', 'purchase' repeat
  region: S.category(), // 'us-east-1', 'eu-west-1', limited set
};

// Implementation - same as TEXT for hot path!
class CategoryColumn {
  private strings: string[] = []; // Just JS string references

  // HOT PATH: Just store reference (zero work, NO interning)
  write(idx: number, value: string): void {
    this.strings[idx] = value; // Direct array assignment only
  }

  // COLD PATH: Build SORTED dictionary + SIEVE-cached UTF-8
  toArrow(): ArrowColumn {
    // 1. Collect unique strings (deduplication happens HERE, not on hot path)
    const uniqueStrings = new Set<string>();
    for (const str of this.strings) {
      if (str != null) uniqueStrings.add(str);
    }

    // 2. Sort for query optimization (binary search)
    const dictionary = [...uniqueStrings].sort();
    const stringToIndex = new Map(dictionary.map((s, i) => [s, i]));

    // 3. Build indices (remap to sorted positions)
    const indices = new Uint32Array(this.strings.length);
    for (let i = 0; i < this.strings.length; i++) {
      if (this.strings[i] != null) {
        indices[i] = stringToIndex.get(this.strings[i])!;
      }
    }

    // 4. UTF-8 encode with SIEVE cache (globalUtf8Cache)
    const { data, offsets } = globalUtf8Cache.encodeMany(dictionary);

    // 5. Clear strings (per-flush bounded)
    this.strings = [];

    return { type: 'dictionary', indices, data, offsets };
  }
}
```

**SIEVE Cache for UTF-8 Encoding (Cold Path Only):**

Uses SIEVE algorithm (NSDI'24) - simpler AND better than LRU. **Note:** This cache is only used during Arrow conversion,
not on the hot path.

```typescript
import { SieveCache } from '@neophi/sieve-cache';

class Utf8Cache {
  private cache: SieveCache<string, Uint8Array>;
  private encoder = new TextEncoder();

  constructor(maxSize = 4096) {
    this.cache = new SieveCache(maxSize);
  }

  // Called ONLY during cold path (Arrow conversion)
  encode(str: string): Uint8Array {
    const cached = this.cache.get(str);
    if (cached) return cached;

    const encoded = this.encoder.encode(str);
    this.cache.set(str, encoded);
    return encoded;
  }

  encodeMany(strings: string[]): { data: Uint8Array; offsets: Int32Array } {
    // Encode all strings using cache, return concatenated + offsets
  }
}

export const globalUtf8Cache = new Utf8Cache();
```

**Why SIEVE over LRU:**

- ~9% lower miss ratio than LRU-K, ARC, 2Q (NSDI'24)
- Simpler: single pointer scan vs linked list manipulation
- No frequency counters or ghost queues needed
- Better for skewed access patterns (common in web workloads)

**Memory Growth Prevention:**

| Mechanism          | What it bounds           | Default      |
| ------------------ | ------------------------ | ------------ |
| Per-flush clearing | String array cleared     | Every flush  |
| SIEVE cache size   | UTF-8 encoded bytes      | 4096 entries |
| SIEVE eviction     | Auto-removes cold values | On insert    |

**Performance Characteristics:**

| Operation       | Cost                         | Allocations       |
| --------------- | ---------------------------- | ----------------- |
| Hot path write  | O(1) array assignment        | Zero              |
| Cold path flush | O(n log n) sort + O(n) UTF-8 | Dictionary arrays |
| UTF-8 (cached)  | O(1) SIEVE get               | Zero              |
| UTF-8 (miss)    | O(k) encode + O(1) SIEVE set | Uint8Array        |

**Why No Hot-Path Interning for CATEGORY:**

An alternative approach would intern strings on the hot path (Map lookup → integer index). This was rejected because:

1. **Map lookups add latency**: Even O(1) Map.get() has overhead vs direct array assignment
2. **Global state complexity**: A global interner requires careful lifecycle management
3. **Premature optimization**: Most category columns have low cardinality; deduplication at flush time is fast enough
4. **Memory tradeoff**: Storing raw strings temporarily uses more memory, but flushes happen frequently enough that this
   is bounded

#### 3. TEXT: No Interning, Conditional Dictionary (Unique Values)

**Use Case**: Strings that rarely repeat (error messages, SQL queries, stack traces)

**Key Insight**: Don't bother interning - just store references. Decide dictionary encoding at flush time.

```typescript
// Schema definition
const schema = {
  errorMessage: S.text(), // Each error might be unique
  sqlQuery: S.text(), // Parameterized queries vary
  stackTrace: S.text(), // Unique per error location
};

// Implementation
class TextColumn {
  private strings: string[] = []; // Just JS string references

  // HOT PATH: Just store reference (zero work)
  write(idx: number, value: string): void {
    this.strings[idx] = value;
  }

  // COLD PATH: 2-pass conditional dictionary
  toArrow(): ArrowColumn {
    // ═══════════════════════════════════════════════════════════
    // PASS 1: Count occurrences + calculate sizes
    // ═══════════════════════════════════════════════════════════
    const occurrences = new Map<string, number>();
    let totalUtf8Bytes = 0;

    for (const str of this.strings) {
      if (str === undefined) continue;
      occurrences.set(str, (occurrences.get(str) || 0) + 1);
      totalUtf8Bytes += this.utf8ByteLength(str);
    }

    const uniqueUtf8Bytes = [...occurrences.keys()].reduce((sum, s) => sum + this.utf8ByteLength(s), 0);

    // ═══════════════════════════════════════════════════════════
    // Calculate space savings
    // ═══════════════════════════════════════════════════════════
    const plainSize = totalUtf8Bytes + (this.strings.length + 1) * 4;
    const dictSize = uniqueUtf8Bytes + (occurrences.size + 1) * 4 + this.strings.length * 4;
    const savings = plainSize - dictSize;

    // ═══════════════════════════════════════════════════════════
    // PASS 2: Build Arrow column
    // ═══════════════════════════════════════════════════════════
    let result: ArrowColumn;

    if (savings > 128) {
      // Dictionary encoding (sorted for queries)
      const sorted = [...occurrences.keys()].sort();
      const indexMap = new Map(sorted.map((s, i) => [s, i]));

      const indices = new Uint32Array(this.strings.length);
      for (let i = 0; i < this.strings.length; i++) {
        indices[i] = indexMap.get(this.strings[i])!;
      }

      result = {
        type: 'dictionary',
        indices,
        dictionary: sorted.map((s) => new TextEncoder().encode(s)),
      };
    } else {
      // Plain UTF-8 (no dictionary overhead)
      result = {
        type: 'utf8',
        values: this.strings.map((s) => new TextEncoder().encode(s)),
      };
    }

    // CRITICAL: Clear strings after flush to prevent unbounded growth
    this.strings = [];

    return result;
  }

  private utf8ByteLength(str: string): number {
    // Fast approximation (exact would require encoding)
    let len = 0;
    for (let i = 0; i < str.length; i++) {
      const code = str.charCodeAt(i);
      if (code < 0x80) len += 1;
      else if (code < 0x800) len += 2;
      else len += 3;
    }
    return len;
  }
}
```

**Why No Hot-Path Interning:**

- TEXT is for unique values - interner would have near-zero hit rate
- LRU would just churn through unique values
- Better to defer all work to cold path

**Memory Growth Prevention:**

- Strings cleared after each flush (`this.strings = []`)
- No global interner - per-column, per-flush
- GC can collect strings after flush completes

**Performance Characteristics:** | Operation | Cost | Allocations | |-----------|------|-------------| | Hot path write
| O(1) array assignment | Zero | | Cold path flush | O(n) count + O(n log n) sort + O(n) UTF-8 | Dictionary/values
arrays | | Post-flush | Strings array cleared | Zero (GC releases) |

### String Type Comparison

| Aspect                 | ENUM                      | CATEGORY                    | TEXT                        |
| ---------------------- | ------------------------- | --------------------------- | --------------------------- |
| **Hot path storage**   | Uint8Array (index)        | string[] (raw JS strings)   | string[] (raw JS strings)   |
| **Hot path work**      | Map lookup + array write  | Array assignment only       | Array assignment only       |
| **Hot path cost**      | O(1)                      | O(1)                        | O(1)                        |
| **Hot path interning** | No (compile-time map)     | **No** (deferred to cold)   | **No** (deferred to cold)   |
| **Cold path**          | Zero (pre-computed)       | Sort + dedupe + SIEVE UTF-8 | 2-pass + conditional dict   |
| **Memory bound**       | Fixed (schema)            | Per-flush (strings cleared) | Per-flush (strings cleared) |
| **Dictionary sorted**  | ✓ At startup              | ✓ At flush                  | ✓ If used                   |
| **UTF-8 timing**       | At startup (pre-computed) | At flush (SIEVE cached)     | At flush                    |
| **Best for**           | Compile-time known        | Runtime repeated            | Unique/rare repeat          |

### Memory Growth Prevention Summary

| Type     | Hot Path Storage   | Bound Mechanism       | When Cleared      | Risk if Misused             |
| -------- | ------------------ | --------------------- | ----------------- | --------------------------- |
| ENUM     | Uint8Array indices | Fixed at construction | Never (immutable) | None - compile-time known   |
| CATEGORY | string[] (raw)     | Per-flush clearing    | After each flush  | High memory between flushes |
| TEXT     | string[] (raw)     | Per-flush clearing    | After each flush  | High memory between flushes |

**Note:** Both CATEGORY and TEXT use the same hot-path strategy: store raw JS strings in a `string[]` array. **No
interning happens on the hot path.** The difference is in cold-path dictionary building: CATEGORY always builds a sorted
dictionary, TEXT only if it saves >128 bytes.

### Deferred Dictionary Benefits

1. **Minimal hot-path overhead**: No Map lookups, no UTF-8 encoding during logging
2. **Memory deduplication**: Unique strings identified during cold-path flush
3. **Arrow optimization**: Sorted dictionaries enable binary search queries
4. **Cache efficiency**: Dictionary indices (integers) fit in CPU cache lines
5. **Bounded growth**: Strings cleared after each flush, SIEVE cache bounds UTF-8 memory
6. **Simpler implementation**: No global interner state to manage

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
  timestamps: Float64Array; // All timestamps together
  operations: Uint8Array; // All operations together
  userIds: Uint32Array; // All userIds together
  actions: Uint32Array; // All actions together
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

    this.nullBitmap[bitmapIdx] |= attrBit << bitPos;
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
      userId: 123,
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
    perEntry: (after.external - before.external) / 1000000 + ' bytes',
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

## Memory Lifecycle and Flush Strategy

### The Flush Cycle

Memory is managed in flush cycles. Each cycle:

1. **Hot path**: Accumulate data in buffers (fast writes)
2. **Flush trigger**: Capacity threshold, time interval, or explicit flush
3. **Cold path**: Convert to Arrow, serialize, send
4. **Cleanup**: Release memory for GC

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           FLUSH CYCLE                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  HOT PATH                    COLD PATH                    CLEANUP       │
│  ─────────                   ─────────                    ───────       │
│                                                                         │
│  ┌──────────────┐           ┌──────────────┐            ┌────────────┐ │
│  │ Write data   │  trigger  │ Convert to   │  send      │ Clear TEXT │ │
│  │ to buffers   │──────────▶│ Arrow tables │───────────▶│ strings[]  │ │
│  │              │           │              │            │            │ │
│  │ ENUM: idx    │           │ ENUM: zero   │            │ CATEGORY:  │ │
│  │ CATEGORY: idx│           │ CATEGORY:    │            │ reset flush│ │
│  │ TEXT: string │           │  sort+UTF-8  │            │ state only │ │
│  │              │           │ TEXT: 2-pass │            │            │ │
│  └──────────────┘           └──────────────┘            └────────────┘ │
│        │                                                       │        │
│        │                                                       │        │
│        └───────────────────────────────────────────────────────┘        │
│                              REPEAT                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### What Gets Cleared on Flush

| Component                | What's Cleared                    | What Persists         | Why                                    |
| ------------------------ | --------------------------------- | --------------------- | -------------------------------------- |
| **ENUM dictionary**      | Nothing                           | Everything            | Immutable - pre-built at startup       |
| **ENUM indices**         | Values array reset                | -                     | Ready for next flush                   |
| **CATEGORY SIEVE cache** | Nothing                           | String→entry mappings | Track occurrence counts across flushes |
| **CATEGORY flush state** | `flushDictionary`, `flushIndices` | -                     | Per-flush dictionary rebuilt each time |
| **CATEGORY UTF-8 cache** | Nothing                           | Hot value UTF-8 bytes | Avoid re-encoding hot values           |
| **TEXT strings**         | `strings[]` array                 | -                     | No interning needed - per-flush        |
| **TypedArrays**          | `writeIndex` reset to 0           | Array buffers         | Reuse allocations                      |

### Memory Bounds

```typescript
interface MemoryBounds {
  // ENUM: Fixed at schema definition
  enum: {
    dictionary: 'O(|values|) - known at compile time';
    utf8Cache: 'O(|values|) - pre-encoded at startup';
    indices: 'O(capacity) - Uint8Array per column';
  };

  // CATEGORY: Configurable SIEVE limits
  category: {
    sieveCache: 'O(maxEntries) - configurable, default 10k';
    utf8Cache: 'O(maxEntries/2) - only hot values';
    flushDict: 'O(unique values in flush) - cleared after flush';
    indices: 'O(capacity) - Uint32Array per column';
  };

  // TEXT: Per-flush, cleared after conversion
  text: {
    strings: 'O(entries in flush) - cleared after flush';
    // No persistent storage between flushes
  };

  // Total bound
  total: 'O(enumValues + categoryLruMax + textPerFlush + capacity × columns)';
}
```

### Flush Triggers

Flush is triggered by any of:

1. **Capacity threshold** (default 80%): Buffer nearly full
2. **Time interval** (default 10s): Periodic flush for freshness
3. **Idle timeout** (default 5s): No writes for a while
4. **Memory pressure**: Node.js `memoryUsage()` exceeds threshold
5. **Explicit flush**: `scheduler.flush()` called

```typescript
class FlushScheduler {
  private config = {
    capacityThreshold: 0.8, // Flush at 80% capacity
    maxIntervalMs: 10_000, // Max 10s between flushes
    idleTimeoutMs: 5_000, // Flush after 5s idle
    memoryThresholdMb: 512, // Flush if heap > 512MB
  };

  shouldFlush(buffer: SpanBuffer): boolean {
    return (
      buffer.writeIndex / buffer.capacity > this.config.capacityThreshold ||
      Date.now() - this.lastFlushTime > this.config.maxIntervalMs ||
      Date.now() - this.lastWriteTime > this.config.idleTimeoutMs ||
      process.memoryUsage().heapUsed > this.config.memoryThresholdMb * 1024 * 1024
    );
  }
}
```

### Per-Flush String Storage (Both CATEGORY and TEXT)

**Decision: No hot-path interning. Both CATEGORY and TEXT store raw strings, dictionary built per-flush.**

| Aspect                | CATEGORY                         | TEXT                                         |
| --------------------- | -------------------------------- | -------------------------------------------- |
| **Hot path storage**  | string[] (raw JS strings)        | string[] (raw JS strings)                    |
| **Cold path**         | Always builds sorted dictionary  | Conditional dictionary (if saves >128 bytes) |
| **Cross-flush dedup** | ✗ New dictionary each flush      | ✗ New dictionary each flush                  |
| **UTF-8 caching**     | SIEVE cache helps across flushes | No caching (unique values)                   |

**Why no hot-path interning:**

- **Simpler**: No global interner state to manage
- **Faster**: Array assignment faster than Map lookup
- **Bounded**: Memory naturally bounded by flush interval
- **Sufficient**: Cold-path deduplication works well for typical flush sizes

**Why SIEVE cache still helps CATEGORY:**

- Same strings often appear across multiple flushes (userIds, spanNames)
- SIEVE caches UTF-8 encoded bytes, avoiding re-encoding on each flush
- Cache is bounded (4096 entries default), so memory stays controlled

### Preventing Unbounded Growth

**The Problem:** Long-running servers can accumulate string data indefinitely, eventually exhausting memory.

**The Solution:** Each string type has a different bounding mechanism:

```typescript
// ENUM: Bounded by schema (fixed at compile time)
S.enum(['span-start', 'span-ok', 'span-err']); // Max 3 values, forever

// CATEGORY: UTF-8 cache bounded by SIEVE (configurable limit)
// Hot path stores raw strings, cold path builds dictionary
// SIEVE cache speeds up UTF-8 encoding during Arrow conversion
new CategoryUtf8Cache({ maxEntries: 10_000, maxBytes: 10_000_000 });
// - Max 10k cached UTF-8 encodings
// - Max 10MB total cache bytes
// - SIEVE eviction removes cold values

// TEXT: Bounded per-flush (cleared after Arrow conversion)
class TextColumn {
  flush(): ArrowColumn {
    const result = convert(this.strings);
    this.strings = []; // CRITICAL: Clear after flush
    return result;
  }
}
```

**Monitoring for Memory Issues:**

```typescript
// Add telemetry to detect memory problems
class MemoryMonitor {
  checkHealth(): HealthReport {
    return {
      categoryUtf8CacheSize: categoryUtf8Cache.cache.size,
      categoryUtf8CacheEvictionRate: categoryUtf8Cache.evictionCount / categoryUtf8Cache.insertCount,
      textStringsBeforeFlush: textColumn.strings.length,
      heapUsedMb: process.memoryUsage().heapUsed / 1024 / 1024,
    };
  }

  alert(): string[] {
    const alerts = [];
    const health = this.checkHealth();

    // High eviction rate suggests CATEGORY misuse (unique values)
    if (health.categoryUtf8CacheEvictionRate > 0.5) {
      alerts.push('CATEGORY eviction rate >50% - consider using TEXT for unique values');
    }

    // Large TEXT buffer suggests slow flush rate
    if (health.textStringsBeforeFlush > 100_000) {
      alerts.push('TEXT buffer >100k strings - consider more frequent flushes');
    }

    return alerts;
  }
}
```

## String Interning and UTF-8 Caching Architecture

The system uses a two-tier architecture for efficient string handling:

1. **Global Interner (arrow-builder)**: Pre-encodes UTF-8 for compile-time known strings
2. **SIEVE Cache (lmao)**: Bounded cache for runtime strings during Arrow conversion

### StringInterner (arrow-builder owns)

Located in `packages/arrow-builder/src/lib/arrow/interner.ts`.

A global `Map<string, Uint8Array>` that caches UTF-8 pre-encoded bytes for known strings.

```typescript
// Internal implementation
const strings = new Map<string, Uint8Array>();

export function intern(str: string): Uint8Array {
  let utf8 = strings.get(str);
  if (!utf8) {
    utf8 = encoder.encode(str);
    strings.set(str, utf8);
  }
  return utf8;
}

export function getInterned(str: string): Uint8Array | undefined {
  return strings.get(str);
}
```

**When `intern()` is called** (all at startup/definition time, NOT hot path):

- **Enum schema creation**: `S.enum(['A', 'B', 'C'])` calls `intern()` for all values
- **Module context creation**: `new ModuleContext(...)` interns `packageName`, `packagePath`, and `gitSha`
- **Span buffer creation**: `createSpanBuffer(...)` interns `spanName`

**Why unbounded**: All interned values are source code constants (enum values, file paths, span names) that already
exist in memory. The interner just caches their UTF-8 representations.

**Usage pattern**:

```typescript
// At schema definition (startup)
const schema = S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']);
// → intern() called 4 times, UTF-8 bytes cached forever

// At module creation (startup)
const moduleCtx = new ModuleContext(
  gitSha, // intern(gitSha) → UTF-8 bytes cached
  packageName, // intern(packageName) → UTF-8 bytes cached (e.g., '@mycompany/user-service')
  packagePath, // intern(packagePath) → UTF-8 bytes cached (e.g., 'src/services/user.ts')
  logSchema
);

// At span creation (per-span, but span names are finite)
// SpanName is passed to op via span(lineNumber, 'name', op, args)
// lineNumber is written directly to lineNumber_values[0] inside _invoke()
// spanName is interned for UTF-8 encoding
const buffer = createSpanBuffer(
  callsiteModule, // Caller's module for row 0 metadata
  opModule, // Op's module for rows 1+ metadata
  'processOrder' // intern(spanName) → UTF-8 bytes cached
);
buffer.lineNumber_values[0] = lineNumber; // Direct TypedArray write, NOT stored as property
```

### Utf8Cache (lmao owns)

Located in `packages/lmao/src/lib/utf8Cache.ts`.

A SIEVE-based cache for runtime strings (category/text column values like userIds, error messages) that implements the
`Utf8Encoder` interface from arrow-builder.

```typescript
export class Utf8Cache implements Utf8Encoder {
  private readonly cache: SieveCache<string, Uint8Array>;

  encode(str: string): Uint8Array {
    const cached = this.cache.get(str);
    if (cached !== undefined) return cached;

    const encoded = this.encoder.encode(str);
    this.cache.set(str, encoded);
    return encoded;
  }

  encodeMany(strings: readonly string[]): { data: Uint8Array; offsets: Int32Array } {
    // Encode all strings using cache, return concatenated + offsets
  }
}

export const globalUtf8Cache = new Utf8Cache(); // Singleton
```

**Characteristics**:

- Bounded (default 4096 entries) with SIEVE eviction
- Cross-flush benefit: repeated strings avoid re-encoding across conversions
- Used during Arrow conversion (cold path), NOT during logging (hot path)
- SIEVE algorithm: ~9% lower miss ratio than LRU-K, ARC, 2Q (NSDI'24)

**Why separate from interner**: Runtime strings have high cardinality (userIds, error messages) and need bounded memory.
The interner is for finite, known strings; the cache is for unbounded, runtime strings.

### DictionaryBuilder (arrow-builder owns)

Located in `packages/arrow-builder/src/lib/arrow/dictionary.ts`.

Builds Arrow dictionaries efficiently with a 2nd-occurrence caching pattern:

```typescript
export class DictionaryBuilder {
  private entries = new Map<string, TrackedEntry>();
  private utf8Encoder: Utf8Encoder; // lmao passes globalUtf8Cache

  add(str: string): void {
    const existing = this.entries.get(str);
    if (existing) {
      existing.count++;
      // Cache UTF-8 on 2nd occurrence if not already cached
      if (existing.count === 2 && !existing.utf8) {
        existing.utf8 = this.utf8Encoder.encode(str);
      }
      return;
    }

    // Check interner for pre-encoded UTF-8 (enum values, module names)
    const interned = getInterned(str);
    if (interned) {
      this.entries.set(str, { utf8: interned, byteLength: interned.length, count: 1 });
      return;
    }

    // 1st occurrence: just track byte length (no encoding yet)
    const byteLength = utf8ByteLength(str);
    this.entries.set(str, { byteLength, count: 1 });
  }

  finalize(sorted: boolean): FinalizedDictionary {
    // For each string:
    // - If utf8 cached (interned or 2nd occurrence): use cached bytes
    // - If 1-time string: encodeInto() directly to output buffer
    // Auto-selects index type (uint8/uint16/uint32) based on dictionary size
  }
}
```

**Lookup order**:

1. Check global interner via `getInterned()` for pre-encoded UTF-8 (enums, span names)
2. Fall back to encoder (lmao's `globalUtf8Cache`) with 2nd-occurrence pattern:
   - 1st occurrence: just track byte length (no encoding)
   - 2nd occurrence: encode and cache locally
   - Finalize: use cached bytes or `encodeInto()` directly for one-time strings

**Why 2nd-occurrence caching**: Most one-time strings (unique error messages, UUIDs) don't benefit from caching. By
waiting until the 2nd occurrence, we avoid wasting memory on truly unique strings while still caching repeated values.

### Pre-encoded UTF-8 in ModuleContext (lmao owns)

ModuleContext stores pre-encoded UTF-8 for frequently-used strings:

```typescript
// ModuleContext (created once per module at startup)
class ModuleContext {
  readonly utf8PackageName: Uint8Array; // intern(packageName)
  readonly utf8PackagePath: Uint8Array; // intern(packagePath)
  readonly utf8GitSha: Uint8Array; // intern(gitSha)

  constructor(gitSha, packageName, packagePath, logSchema) {
    this.utf8PackageName = intern(packageName);
    this.utf8PackagePath = intern(packagePath);
    this.utf8GitSha = intern(gitSha);
  }
}

// SpanBuffer stores dual module references for source attribution:
// - callsiteModule: Caller's module (for row 0's gitSha/packageName/packagePath)
// - module: Op's module (for rows 1+ gitSha/packageName/packagePath)
// spanName is interned during buffer creation
// lineNumber is written directly to lineNumber_values TypedArray (NOT stored as property)
```

**Why pre-encode**: These strings are written to Arrow columns frequently. Pre-encoding at module creation means zero
UTF-8 encoding cost during Arrow conversion.

### Design Decisions

#### 1. No ID Pools

The interner caches UTF-8 bytes directly, not numeric IDs. This avoids:

- Extra Map lookup at write time (string → ID)
- Extra array lookup at conversion time (ID → string → UTF-8)
- Complex lifecycle management (when to release IDs?)

Direct string access is simpler: contexts hold the original strings, interner holds pre-encoded UTF-8.

#### 2. Separate SIEVE Cache from Interner

| Aspect      | Interner (arrow-builder)   | Utf8Cache (lmao)              |
| ----------- | -------------------------- | ----------------------------- |
| **Purpose** | Pre-encode known strings   | Cache runtime string encoding |
| **Bounded** | No (finite source strings) | Yes (SIEVE, default 4096)     |
| **Called**  | Startup/definition time    | Arrow conversion (cold path)  |
| **Strings** | Enum values, paths, names  | userIds, errors, categories   |
| **Memory**  | O(source code constants)   | O(cache max size)             |
| **Package** | arrow-builder              | lmao                          |

#### 3. Batch-Local Dictionary Remapping

Arrow requires contiguous 0-based indices for dictionary encoding. Each batch builds its own dictionary during
`finalize()`:

```typescript
// During finalize(), strings are sorted and assigned indices 0, 1, 2, ...
const strings = [...this.entries.keys()];
if (sorted) strings.sort();

const indexMap = new Map<string, number>();
for (let i = 0; i < strings.length; i++) {
  indexMap.set(strings[i], i); // Fresh 0-based index per batch
}
```

This means the same string may have different indices in different batches, which is correct for Arrow's dictionary
encoding model.

#### 4. Sorted Dictionaries

`DictionaryBuilder.finalize(sorted: true)` produces alphabetically sorted dictionaries:

- **Storage**: Better compression (similar strings adjacent)
- **Querying**: Enables binary search for "does value X exist?"
- **Batch merging**: Easier to merge sorted dictionaries from multiple batches

### Package Ownership Summary

| Component                   | Package       | Purpose                                      |
| --------------------------- | ------------- | -------------------------------------------- |
| `intern()`, `getInterned()` | arrow-builder | UTF-8 pre-encoding for known strings         |
| `Utf8Encoder` interface     | arrow-builder | Contract for UTF-8 encoding                  |
| `defaultUtf8Encoder`        | arrow-builder | Simple TextEncoder wrapper, no caching       |
| `utf8ByteLength()`          | arrow-builder | Calculate UTF-8 length without encoding      |
| `DictionaryBuilder`         | arrow-builder | Build Arrow dictionaries with 2nd-occ cache  |
| `Utf8Cache`                 | lmao          | SIEVE-cached Utf8Encoder for runtime strings |
| `globalUtf8Cache`           | lmao          | Singleton Utf8Cache instance                 |
| Pre-encoded contexts        | lmao          | ModuleContext with utf8\* fields             |

### Data Flow During Arrow Conversion

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ARROW CONVERSION (Cold Path)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  String from buffer                                                          │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────┐                                                         │
│  │ DictionaryBuilder│                                                        │
│  │     .add(str)    │                                                        │
│  └────────┬────────┘                                                         │
│           │                                                                  │
│           ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │ 1. getInterned(str)                                              │        │
│  │    ├─ HIT: Use pre-encoded UTF-8 (enum value, span name)        │        │
│  │    └─ MISS: Continue to step 2                                   │        │
│  └──────────────────────────────────────────────────────────────────┘        │
│           │                                                                  │
│           ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │ 2. Track occurrence count                                        │        │
│  │    ├─ 1st occurrence: Track byteLength only (no encoding)       │        │
│  │    └─ 2nd occurrence: utf8Encoder.encode(str) → cache locally   │        │
│  └──────────────────────────────────────────────────────────────────┘        │
│           │                                                                  │
│           ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │ utf8Encoder = globalUtf8Cache (SIEVE)                           │        │
│  │    ├─ HIT: Return cached Uint8Array                             │        │
│  │    └─ MISS: TextEncoder.encode() → cache → return               │        │
│  └──────────────────────────────────────────────────────────────────┘        │
│                                                                              │
│  On finalize():                                                              │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │ For each string:                                                 │        │
│  │    ├─ If utf8 cached: Copy bytes to output                       │        │
│  │    └─ If 1-time: encodeInto() directly (no cache pollution)     │        │
│  └──────────────────────────────────────────────────────────────────┘        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Summary

Buffer performance optimizations leverage:

- **V8 hidden classes** - Stable object shapes
- **Monomorphic access** - Predictable types
- **Cache alignment** - CPU-friendly layout
- **ENUM compile-time mapping** - Integer comparisons via switch statement
- **Global string interner** - Pre-encoded UTF-8 for known strings (arrow-builder)
- **SIEVE UTF-8 cache** - Bounded cache for runtime strings (lmao)
- **2nd-occurrence caching** - Avoids encoding truly unique strings
- **SIEVE bounding** - Prevents unbounded memory growth (CATEGORY)
- **Per-flush clearing** - Releases TEXT memory after conversion
- **Sequential access** - Prefetch friendly
- **Hot path isolation** - Fast common case

Result: <100ns writes with zero allocations, bounded memory growth.
