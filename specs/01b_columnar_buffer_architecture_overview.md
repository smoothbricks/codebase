# Columnar Buffer Architecture

> **📚 HIGH-PERFORMANCE LOGGING CORE**
>
> This document introduces the columnar buffer architecture that powers the trace logging system with <0.1ms runtime overhead. For implementation details, see the sub-documents linked below.

## Core Insight: Columnar Storage for Logs

Instead of creating objects and serializing them, we write directly to columnar TypedArrays. This separates the hot path (array writes) from the cold path (Arrow/Parquet conversion).

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
// Write directly to typed arrays
buffer.timestamps[writeIndex] = Date.now();
buffer.operations[writeIndex] = OP_TAG;
buffer.attr_userId[writeIndex] = internString('123');
buffer.attr_action[writeIndex] = internString('login');
```

Benefits:
- **Zero allocations** - Reuse pre-allocated arrays
- **Cache-friendly** - Sequential memory access
- **Direct Arrow conversion** - Arrays become Arrow columns
- **String interning** - Build dictionaries during logging
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
buffer.timestamps.length === buffer.operations.length === buffer.attr_userId.length
// This enables direct row indexing and zero-copy Arrow conversion
```

### 2. Cache Line Alignment
Arrays sized to 64-byte boundaries:
```typescript
// 64 elements = optimal for all array types
timestamps: new Float64Array(64),  // 64 × 8 = 512 bytes (8 cache lines)
operations: new Uint8Array(64),    // 64 × 1 = 64 bytes (1 cache line)
```

### 3. String Interning
Build Arrow dictionaries while logging:
```typescript
const internedStrings = ['user-123', 'login', 'dashboard']; // Pre-built dictionary
buffer.attr_action[i] = 1; // Index into dictionary, not string
// Direct Arrow Dictionary creation - no second scan needed!
```

### 4. Self-Tuning Capacity
Buffers grow and shrink automatically:
```typescript
// Start small
const buffer = createSpanBuffer(64);

// Grows as needed
buffer.write(); // 64 → 128 → 256 → ...

// Compacts when mostly empty
buffer.compact(); // 2048 → 512 if only using 400 entries
```

## Quick Example

```typescript
// Create a buffer - no size configuration needed
const buffer = createSpanBuffer();

// Hot path - just array writes
function logUserAction(userId: string, action: string) {
  const idx = buffer.writeIndex++;
  
  buffer.timestamps[idx] = Date.now();
  buffer.operations[idx] = OP_TAG;
  buffer.attr_userId[idx] = internString(userId);
  buffer.attr_action[idx] = internString(action);
  
  // Self-tuning happens automatically
  if (idx >= buffer.capacity - 1) {
    buffer.grow(); // Transparent to caller
  }
}

// Background conversion to Arrow (zero-copy)
setInterval(() => {
  const arrowBatch = buffer.toArrow();
  // Dictionary already built during logging!
  sendToStorage(arrowBatch);
}, 1000);
```

## Performance Characteristics

- **Write latency**: <100ns per log entry
- **Memory overhead**: ~100 bytes per entry
- **GC pressure**: Near zero (pre-allocated arrays)
- **Arrow conversion**: Zero-copy, <1ms for 10K entries
- **String deduplication**: 10-100x compression

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
- [ ] Add string interning for repeated values
- [ ] Implement self-tuning growth/compaction
- [ ] Create Arrow conversion pipeline
- [ ] Add null bitmap for sparse attributes
- [ ] Test memory pressure scenarios
- [ ] Benchmark against object-based logging

## Common Patterns

### Sparse Attributes
Use null bitmap for optional fields:
```typescript
if (error) {
  buffer.attr_errorCode[idx] = internString(error.code);
  buffer.setAttributeNotNull(idx, ATTR_ERROR_CODE);
}
```

### High-Cardinality Strings
Intern only common values:
```typescript
const value = isCommon(str) ? internString(str) : storeRawString(str);
```

### Nested Data
Flatten to columns:
```typescript
// Instead of: { user: { id: '123', name: 'Alice' } }
buffer.attr_userId[idx] = internString('123');
buffer.attr_userName[idx] = internString('Alice');
```

## Next Steps

1. Study **[Performance Optimizations](./01b1_buffer_performance_optimizations.md)** for V8 tricks
2. Understand **[Self-Tuning Buffers](./01b2_buffer_self_tuning.md)** for automatic memory management

## Summary

Columnar buffers revolutionize logging performance by:
- Writing directly to typed arrays (no objects)
- Building Arrow dictionaries during logging (no second scan)
- Self-tuning capacity (no configuration)
- Cache-aligned memory layout (CPU-friendly)

The result is logging that's faster than console.log with built-in analytics support.