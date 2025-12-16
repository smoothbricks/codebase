# Columnar Buffer Architecture

> **📚 HIGH-PERFORMANCE LOGGING CORE**
>
> This document introduces the columnar buffer architecture that powers the trace logging system with <0.1ms runtime
> overhead. For implementation details, see the sub-documents linked below.

## Core Insight: Columnar Storage for Logs

Instead of creating objects and serializing them, we write directly to columnar TypedArrays. This separates the hot path
(array writes) from the cold path (Arrow/Parquet conversion).

## WHY: Traditional Logging is Too Slow

### Objects Kill Performance

Traditional logging:

```javascript
logger.info({ userId: '123', action: 'login', timestamp: Date.now() });
// Creates object → Serializes to JSON → Writes to disk
```

Problems:

1. **Object allocation** - Triggers garbage collection
2. **Property access** - Dynamic lookups
3. **Serialization cost** - JSON.stringify overhead
4. **Memory fragmentation** - Random heap allocation
5. **No zero-copy** - Must transform for analytics

### Our Approach: Direct Array Writes

```typescript
// Write directly to typed arrays - NO interning on hot path
buffer._timestamps[writeIndex] = Date.now();
buffer._operations[writeIndex] = OP_TAG;
buffer.userId[writeIndex] = '123'; // Raw string stored in string[]
buffer.action[writeIndex] = 'login'; // Raw string stored in string[]
// Dictionary building happens in cold path (Arrow conversion)
```

Benefits:

- **Zero allocations** - Reuse pre-allocated arrays
- **Cache-friendly** - Sequential memory access
- **Direct Arrow conversion** - Arrays become Arrow columns
- **Deferred dictionary building** - Heavy work happens in cold path
- **Self-tuning** - Automatically adapts capacity

## System Architecture

### Core Components

1. **[Performance Optimizations](./01b1_buffer_performance_optimizations.md)** - V8 and memory tricks
   - **WHY**: 10-100x faster than object-based logging
   - Cache alignment, monomorphic dispatch, string interning

2. **[Self-Tuning Buffers](./01b2_buffer_self_tuning.md)** - Zero-config memory management
   - **WHY**: Works everywhere without configuration
   - Adaptive sizing, smart compaction, memory pressure handling

## Key Design Principles

### 1. Equal Length Arrays

All TypedArrays maintain identical length:

```typescript
(buffer._timestamps.length === buffer._operations.length) === buffer.userId.length;
// This enables direct row indexing and Arrow conversion
```

### 2. Cache Line Alignment

Arrays sized to 64-byte boundaries:

```typescript
// 64 elements = optimal for all array types
timestamps: new BigInt64Array(64),  // 64 × 8 = 512 bytes (8 cache lines)
operations: new Uint8Array(64),    // 64 × 1 = 64 bytes (1 cache line)
```

### 3. Deferred String Processing

Store raw strings on hot path, build Arrow dictionaries on cold path:

```typescript
// HOT PATH: Just store raw strings (zero overhead)
buffer.action[i] = 'login'; // Raw string reference

// COLD PATH: Build sorted dictionary during Arrow conversion
// Dictionary built per-flush, strings cleared after conversion
// SIEVE cache helps with UTF-8 encoding across flushes
```

### 4. Self-Tuning Capacity

Buffers chain automatically when capacity is exceeded:

```typescript
// Start with initial capacity
const buffer = createSpanBuffer(64);

// Overflows chain to next buffer transparently
// When buffer.writeIndex >= buffer.capacity, createNextBuffer() is called
// The system learns optimal capacity per module over time
```

## Quick Example

```typescript
// Create a buffer - no size configuration needed
const buffer = createSpanBuffer();

// Hot path - just array writes (NO interning)
function logUserAction(userId: string, action: string) {
  const idx = buffer._writeIndex++;

  buffer._timestamps[idx] = Date.now();
  buffer._operations[idx] = OP_TAG;
  buffer.userId[idx] = userId; // Raw string stored
  buffer.action[idx] = action; // Raw string stored

  // Self-tuning happens automatically via buffer chaining
  // When capacity is exceeded, createNextBuffer() chains a new buffer
}

// Background conversion to Arrow (via convertToArrowTable)
setInterval(() => {
  const arrowTable = convertToArrowTable(buffer);
  // Dictionary built during conversion (cold path)
  // Strings cleared after conversion
  sendToStorage(arrowTable);
}, 1000);
```

## Performance Characteristics

- **Write latency**: <100ns per log entry
- **Memory overhead**: ~100 bytes per entry
- **GC pressure**: Near zero (pre-allocated arrays, strings cleared per-flush)
- **Arrow conversion**: <1ms for 10K entries
- **String deduplication**: 10-100x compression (built during cold-path conversion)

## Prerequisites

Before implementing columnar buffers, understand:

1. **[Trace Logging System](./01_trace_logging_system.md)**
   - Overall architecture and goals
   - How buffers fit in the system

2. **JavaScript TypedArrays**
   - ArrayBuffer, views, and memory layout
   - Performance characteristics

3. **Apache Arrow Format**
   - Columnar data representation
   - Dictionary encoding

## Implementation Checklist

- [ ] Understand cache line alignment benefits
- [ ] Design schema for your attributes
- [ ] Implement basic buffer with core columns
- [ ] Store raw strings in string[] arrays (deferred dictionary)
- [ ] Implement self-tuning growth/compaction
- [ ] Create Arrow conversion pipeline (with dictionary building)
- [ ] Add null bitmap for sparse attributes
- [ ] Test memory pressure scenarios
- [ ] Benchmark against object-based logging

## Common Patterns

### Sparse Attributes

Use null bitmap for optional fields:

```typescript
if (error) {
  buffer.errorCode[idx] = error.code; // Raw string
  buffer.setAttributeNotNull(idx, ATTR_ERROR_CODE);
}
```

### String Type Selection

Choose based on cardinality (see 01a_trace_schema_system.md):

```typescript
// S.enum() - Known values at compile time (Map lookup on hot path)
operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']);

// S.category() - Values that repeat (raw strings, dictionary built on cold path)
userId: S.category();

// S.text() - Unique values (raw strings, conditional dictionary on cold path)
errorMessage: S.text();
```

### Nested Data

Flatten to columns:

```typescript
// Instead of: { user: { id: '123', name: 'Alice' } }
buffer.userId[idx] = '123';
buffer.userName[idx] = 'Alice';
```

## Next Steps

1. Study **[Performance Optimizations](./01b1_buffer_performance_optimizations.md)** for V8 tricks
2. Understand **[Self-Tuning Buffers](./01b2_buffer_self_tuning.md)** for automatic memory management

## Summary

Columnar buffers revolutionize logging performance by:

- Writing directly to typed arrays (no objects)
- Storing raw strings on hot path (no Map lookups during logging)
- Building Arrow dictionaries on cold path (heavy work deferred to flush)
- Self-tuning capacity (no configuration)
- Cache-aligned memory layout (CPU-friendly)

The result is logging that's faster than console.log with built-in analytics support.
