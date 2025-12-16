# Package Architecture: arrow-builder and lmao

## Overview

This monorepo contains two distinct packages with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│                        APPLICATION CODE                          │
│                    (uses lmao for logging)                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     @smoothbricks/lmao                           │
│              High-Level Structured Logging Library               │
│                                                                  │
│  • Schema DSL (S.enum/category/text/number/boolean)             │
│  • Context flow (request→module→task→span)                      │
│  • SpanBuffer classes with direct properties (attr_$name_*)     │
│  • Scope classes (SEPARATE from buffer columns)                 │
│  • Feature flag evaluation                                       │
│  • Fluent logging API (ctx.tag, ctx.log)                        │
│  • System columns (timestamps, operations) - ALWAYS eager       │
│  • User attribute columns - lazy by default                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  @smoothbricks/arrow-builder                     │
│    Low-Level Alternative to apache-arrow for Building Tables    │
│                                                                  │
│  • Explicit, visible allocations (NOT hidden resizing)          │
│  • Cache-aligned TypedArray creation                             │
│  • Lazy column storage (nulls + values share ONE ArrayBuffer)   │
│  • Null bitmap management                                        │
│  • Schema extensibility via composition (NOT inheritance)       │
│  • Runtime class generation (new Function()) for V8 optimization│
│  • Zero-copy Arrow conversion                                    │
│  • NO knowledge of logging/tracing                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        apache-arrow                              │
│              For final Arrow Table types only                    │
└─────────────────────────────────────────────────────────────────┘
```

**CRITICAL DEPENDENCY RULE**:

- `lmao` depends on `arrow-builder` ✅
- `arrow-builder` MUST NOT depend on `lmao` ❌

---

## Design Philosophy: Why Two Packages?

### Problem: Apache Arrow's Hidden Allocations

Apache Arrow's JavaScript builder pattern has hidden resizing and allocations:

```typescript
// Apache Arrow builder - allocations HIDDEN
const builder = new arrow.Utf8Builder();
builder.append('value1'); // May resize internally (hidden allocation)
builder.append('value2'); // May resize again (hidden allocation)
builder.append('value3'); // When does allocation happen? Unknown!
```

This is problematic for high-performance logging because:

1. **Unpredictable latency**: Hidden resizes cause GC pauses
2. **No control over capacity**: Can't pre-allocate based on workload patterns
3. **No lazy initialization**: All columns allocated eagerly, even if unused

### Solution: Explicit Allocation Control

**arrow-builder** provides explicit, visible allocations:

```typescript
// arrow-builder - allocations EXPLICIT
const buffer = createColumnBuffer(schema, 64); // Explicit capacity
buffer.attr_userId_values[idx] = internedId; // Direct write, no hidden alloc

// Lazy columns: allocation happens when getter is first accessed
const values = buffer.attr_userId_values; // Allocates HERE, once, explicitly
```

**lmao** builds on this with logging-specific optimizations:

```typescript
// lmao - logging-aware allocation strategy
// System columns: ALWAYS eager (hot path critical)
// User attributes: lazy by default (sparse data)
const span = ctx.span('operation');
span.log.info('Starting'); // timestamps/operations already allocated (eager)
span.tag.userId('u123'); // attr_userId allocated on first use (lazy)
```

---

## Key Architectural Principles

### 1. Explicit Allocation Visibility (arrow-builder)

**WHY**: Traditional Arrow builders hide allocations and resizes. This makes it impossible to:

- Pre-allocate optimal buffer sizes based on workload patterns
- Track memory usage accurately for self-tuning
- Control WHEN allocations happen (hot path vs cold path)
- Implement custom allocation strategies (like lmao's lazy columns)

**HOW arrow-builder implements this**:

- Every allocation happens through explicit function calls
- Lazy columns use getters that allocate on first access (visible in generated code)
- No automatic resizing - buffer chaining is explicit
- Capacity is always specified at creation time

### 2. Zero Overhead Hot Path (lmao)

**WHY**: System columns (timestamps, operations) are written on EVERY log entry. Even a single `if (values === null)`
check adds overhead per entry, compounding to milliseconds on high-throughput spans.

**HOW lmao implements this**:

- **System columns**: Always EAGER - pre-allocated at buffer creation, zero conditionals
- **User attributes**: Lazy by default - allocated only when first written or scope set
- Clear separation enforced by codegen: system columns are direct TypedArray properties

### 3. V8-Friendly Extensibility via Composition

**WHY**: V8 optimizes objects with stable property layouts (hidden classes). Dynamic property access and inheritance
break these optimizations.

**HOW both packages implement this**:

- **Composition over inheritance**: SpanBuffer wraps ColumnBuffer, doesn't extend it
- **Direct properties**: `buffer.attr_userId_values`, not `buffer.columns['userId'].values`
- **Runtime codegen**: `new Function()` generates monomorphic code with stable call sites
- **No dynamic property access**: All property names known at codegen time

### 4. Application-Agnostic Primitives (arrow-builder)

**WHY**: arrow-builder should be usable for ANY columnar data use case, not just logging. This ensures clean separation
and prevents feature creep.

**TEST**: If you can build these using ONLY arrow-builder, the separation is correct:

- ✅ Time-series metrics collector
- ✅ CSV-to-Arrow converter
- ✅ Event sourcing buffer
- ✅ Database query result caching
- ❌ Structured logging (needs lmao's spans, scopes, entry types)

### 5. Scope is SEPARATE from Buffer Columns (lmao)

**WHY**: Scope values are per-span inheritable attributes. Buffer columns are per-entry storage. Mixing them creates
complexity:

- Scope changes would need to backfill arrays
- Lazy columns would need to track both "unallocated" and "scope value"
- Arrow conversion would need to distinguish scope-filled vs explicitly-written values

**HOW lmao implements this**:

- `Scope` class: Plain JavaScript object with getters/setters (NOT TypedArrays)
- `SpanBuffer`: TypedArray columns for per-entry data
- When converting to Arrow, scope values fill null positions in the TypedArray

---

## @smoothbricks/arrow-builder

### Purpose

A low-level alternative to `apache-arrow` for building Arrow tables and record-batches. The library focuses on:

- **Explicit allocations**: Every memory allocation is visible and controllable
- **Lazy column pattern**: Columns allocate on first access (getter-based)
- **Shared ArrayBuffer**: Each column's nulls + values share ONE buffer
- **V8-friendly codegen**: Uses `new Function()` for direct property access
- **Zero knowledge of logging**: Generic columnar storage only

### What arrow-builder OWNS

1. **Cache-aligned TypedArray creation** - 64-byte aligned buffers for CPU cache efficiency
2. **Lazy column storage pattern** - Nulls and values share ONE ArrayBuffer per column
3. **Null bitmap management** - Arrow-format null bitmaps with bit manipulation
4. **Buffer capacity management** - Explicit capacity, no hidden resizing
5. **Zero-copy Arrow conversion utilities** - TypedArray concatenation, null bitmap merging helpers
6. **Generic schema types** - Type definitions (enum, category, text, number, boolean)
7. **Runtime class generation** - `generateColumnBufferClass()` using `new Function()`
8. **Column buffer codegen utilities** - Create optimized buffer classes with direct properties

### What arrow-builder MUST NOT know about

- ❌ Logging or tracing concepts (spans, traces, contexts)
- ❌ Entry types (info, warn, error, span-start, span-end)
- ❌ The `attr_` prefix convention (lmao-specific naming)
- ❌ Scope or scoped attributes
- ❌ Feature flags or evaluation contexts
- ❌ Masking functions (hash, url, sql, email)
- ❌ Context propagation or hierarchy
- ❌ System vs user column distinction
- ❌ Tree structures (parent/child spans, buffer.children, buffer.next)
- ❌ Any `@smoothbricks/lmao` dependency

### Lazy Column Storage Pattern

**WHY**: Most spans only use a subset of schema attributes. Eager allocation wastes memory.

**HOW**: Each column stores nulls and values in ONE ArrayBuffer, partitioned with cache-line alignment:

```
┌──────────────────────────────────────────────────────────────────┐
│                    ONE ArrayBuffer per Column                     │
├──────────────────────────────────────────────────────────────────┤
│ [null bitmap bytes] | [padding to 64-byte line] | [value bytes]  │
│      (Uint8Array)   |         (gap)             | (TypedArray)   │
└──────────────────────────────────────────────────────────────────┘
```

**Generated code pattern** (from `columnBufferGenerator.ts`):

```typescript
// Symbol for per-instance lazy storage
const attr_userId_sym = Symbol('attr_userId');

function allocate_attr_userId(self) {
  if (self[attr_userId_sym]) return self[attr_userId_sym];

  const capacity = self._alignedCapacity;
  const nullBitmapSize = Math.ceil(capacity / 8);
  const alignedNullOffset = Math.ceil(nullBitmapSize / 4) * 4; // 4 = bytesPerElement
  const totalSize = alignedNullOffset + capacity * 4;

  const buffer = new ArrayBuffer(totalSize); // ONE allocation
  const storage = {
    buffer: buffer,
    nulls: new Uint8Array(buffer, 0, nullBitmapSize),
    values: new Uint32Array(buffer, alignedNullOffset, capacity),
  };
  self[attr_userId_sym] = storage;
  return storage;
}

class GeneratedColumnBuffer {
  get attr_userId_nulls() {
    return allocate_attr_userId(this).nulls;
  }
  get attr_userId_values() {
    return allocate_attr_userId(this).values;
  }
}
```

**Benefits**:

- Memory locality: nulls and values in same cache region
- Single allocation per column (not two separate TypedArrays)
- Lazy: zero memory cost for unused columns
- Cache-aligned: no false sharing, optimal CPU access

### Schema Extensibility (for lmao's codegen)

arrow-builder's schema system supports a "lazy" option that consumers can use:

```typescript
// arrow-builder: Generic schema with metadata
export interface SchemaWithMetadata {
  __schema_type?: 'enum' | 'category' | 'text' | 'number' | 'boolean';
  __enum_values?: readonly string[]; // For enum types
  __lazy?: boolean; // Hint for consumers (default: true for most)
}
```

This allows lmao to generate different code for eager vs lazy columns without arrow-builder knowing about logging
concepts.

---

## @smoothbricks/lmao

### Purpose

A high-level structured logging library providing excellent developer experience with minimal runtime overhead:

- **Zero-allocation hot path**: Avoid string interpolation and object allocation during logging
- **Schema-driven type safety**: Compile-time and runtime validation of logged data
- **Context propagation**: Automatic trace correlation through request→task→span hierarchy
- **System column optimization**: timestamps/operations are NEVER lazy

### What lmao OWNS

1. **Schema DSL** - `S.enum()`, `S.category()`, `S.text()`, `S.number()`, `S.boolean()`
2. **Tag attribute definitions** - Schema definitions with masking transforms and `attr_` prefix
3. **Feature flag evaluation** - `defineFeatureFlags()` with sync/async evaluation
4. **Context flow** - Request context → Module context → Task context → Span context
5. **SpanBuffer creation** - Extends arrow-builder's ColumnBuffer with span metadata
6. **Scope class generation** - SEPARATE from buffer columns, holds inheritable values
7. **SpanLogger generation** - Typed methods per schema field
8. **Fluent logging API** - `ctx.tag.userId()`, `ctx.log.info()`, `ctx.ok()`, `ctx.err()`
9. **Entry type semantics** - Span lifecycle (start/ok/err), log levels (info/debug/warn/error)
10. **System column management** - timestamps, operations (ALWAYS eager, never lazy)
11. **Library integration** - Prefix-based attribute namespacing for third-party libraries
12. **Background flush scheduling** - Adaptive flush timing based on buffer capacity
13. **Tree walking** - Recursive traversal of SpanBuffer trees (parent/children/overflow chains)
14. **Dictionary building** - Two-pass conversion: build dictionaries across tree, then convert to RecordBatches
15. **Arrow Table creation** - Orchestrates conversion using shared dictionaries and `arrow.makeData()`

### System Columns vs User Attributes

**CRITICAL DESIGN PRINCIPLE**: System columns and user attribute columns have fundamentally different performance
requirements.

#### System Columns (ALWAYS Eager)

These columns are written on EVERY log entry and MUST have zero overhead:

| Column       | Type           | Description                                     | Allocation       |
| ------------ | -------------- | ----------------------------------------------- | ---------------- |
| `timestamps` | `Float64Array` | Microsecond-precision anchored timestamps       | **ALWAYS eager** |
| `operations` | `Uint8Array`   | Entry type enum (span-start, info, error, etc.) | **ALWAYS eager** |

**WHY never lazy**: Adding `if (values === null)` checks to the hottest path would add microseconds per entry. These
columns are pre-allocated in the constructor.

**Generated code** (system columns in constructor):

```typescript
class GeneratedSpanBuffer {
  constructor(requestedCapacity) {
    // System columns (EAGER - written on every entry)
    const alignedCapacity = getCacheAlignedCapacity(requestedCapacity);
    this._alignedCapacity = alignedCapacity;
    this.timestamps = new Float64Array(alignedCapacity); // Allocated HERE
    this.operations = new Uint8Array(alignedCapacity); // Allocated HERE

    this.writeIndex = 0;
    this.capacity = requestedCapacity;
  }

  // User attributes are LAZY getters (not in constructor)
  get attr_userId_values() {
    return allocate_attr_userId(this).values;
  }
}
```

#### User Attribute Columns (Lazy by Default)

User-defined attributes from schema are sparse and optional:

```typescript
const mySchema = defineTagAttributes({
  userId: S.category(), // May not be set on every span
  orderId: S.category(), // Only on order-related spans
  httpStatus: S.number(), // Only on HTTP spans
  sqlQuery: S.text(), // Only on DB spans
});
```

**WHY lazy**: In a schema with 20 attributes, a typical span uses 3-5. Lazy allocation saves 70-85% memory.

**Memory impact example**:

```
Without lazy initialization (20 columns × 64 elements × 4 bytes):
  20 columns × 64 × 4 = 5,120 bytes per span
  20 null bitmaps × 8 = 160 bytes
  Total: 5,280 bytes per span

With lazy getters (system + 3 used attributes):
  System columns: 64 × 8 + 64 × 1 = 576 bytes (always)
  Lazy getter closures: ~0 bytes (shared code, symbols only)
  3 used columns × (nulls + values via shared ArrayBuffer):
    Each: ~272 bytes (8 nulls + padding + 256 values)
    Total: 3 × 272 = 816 bytes (on demand)
  Total: ~1,392 bytes (74% savings!)
```

### Scope Class: SEPARATE from Buffer Columns

**WHY separate**: Scope values are per-span inheritable attributes. Buffer columns are per-entry storage.

If scope values were stored IN lazy columns:

- Scope changes would need to backfill all previous entries
- Lazy columns would need to distinguish "unallocated" from "has scope value"
- Arrow conversion would need complex logic for scope-filled vs written values

**HOW lmao implements this**:

```typescript
// Scope: Plain JavaScript object (NOT TypedArrays)
class GeneratedScope {
  _userId = undefined;
  _requestId = undefined;

  get userId() {
    return this._userId;
  }
  set userId(value) {
    this._userId = value;
  }

  _getScopeValues() {
    return { userId: this._userId, requestId: this._requestId };
  }
}

// SpanBuffer: TypedArray columns for per-entry data
class GeneratedSpanBuffer {
  // System columns (eager)
  timestamps: Float64Array;
  operations: Uint8Array;

  // User attributes (lazy getters)
  get attr_userId_values() {
    /* lazy allocation */
  }
  get attr_userId_nulls() {
    /* lazy allocation */
  }
}
```

**Arrow conversion** fills null positions with scope values:

```typescript
// During Arrow conversion (cold path)
for (let i = 0; i < entryCount; i++) {
  if (!isValid(nullBitmap, i)) {
    // Entry has no explicit value - use scope value if available
    if (scopeValues.userId !== undefined) {
      arrowColumn[i] = scopeValues.userId;
    }
  }
}
```

### SpanBuffer Structure

SpanBuffer extends ColumnBuffer with span-specific metadata:

```typescript
interface SpanBuffer extends ColumnBuffer {
  // Span identity (see 01b_columnar_buffer_architecture.md "Span Definition")
  // A span represents a unit of work within a single thread of execution.
  threadId: bigint; // 64-bit random, generated once per worker/process
  spanId: number; // 32-bit counter, incremented per span on this thread
  traceId: string; // Shared across all spans in a request

  // Tree structure
  parent?: SpanBuffer; // Parent span (for child spans) - provides parent_thread_id/parent_span_id
  children: SpanBuffer[]; // Child spans

  // Context link
  task: TaskContext; // Module context, capacity stats

  // Buffer chaining (for overflow)
  next?: SpanBuffer; // Continuation buffer when capacity exceeded
}
```

### Direct Properties (Zero Indirection)

**WHY**: V8's hidden class optimization works best with stable, known properties.

```typescript
// ❌ WRONG - Dynamic lookup, megamorphic
buffer.columns['attr_userId'].values[idx] = value;

// ✅ CORRECT - Direct property, monomorphic
buffer.attr_userId_values[idx] = value;
```

The generated SpanBuffer class has direct properties for each column:

- `attr_${name}_nulls` - Uint8Array null bitmap
- `attr_${name}_values` - TypedArray for values
- `attr_${name}` - Alias for `_values` (convenience)

---

## Runtime Code Generation

Both packages use `new Function()` for performance-critical code:

### arrow-builder generates:

- `generateColumnBufferClass()` - Creates buffer class with:
  - Eager system column allocation in constructor
  - Lazy getters for attribute columns
  - Per-instance storage via Symbol keys
  - Cache-aligned capacity calculation

### lmao generates:

- `generateSpanLoggerClass()` - Creates SpanLogger with:
  - Typed attribute methods (`userId(value)`, `httpStatus(code)`)
  - Compile-time enum mapping (string → Uint8 index)
  - Fluent API (methods return `this`)
- `generateScopeClass()` - Creates Scope with:
  - Private fields per attribute
  - Getters/setters for type-safe access
  - `_getScopeValues()` for inheritance

**WHY `new Function()`**:

- Generates monomorphic code that V8 can optimize
- All property names known at generation time
- Avoids megamorphic call sites from generic implementations
- Hidden class stability - all properties defined in constructor

---

## Package Dependencies

```json
// packages/arrow-builder/package.json
{
  "name": "@smoothbricks/arrow-builder",
  "dependencies": {
    "apache-arrow": "^21.1.0",  // For arrow.makeData types only
    "@sury/sury": "..."          // For schema validation types
  }
  // NO @smoothbricks/lmao dependency!
}

// packages/lmao/package.json
{
  "name": "@smoothbricks/lmao",
  "dependencies": {
    "@smoothbricks/arrow-builder": "workspace:*",  // Uses arrow-builder
    "apache-arrow": "^21.1.0"
  }
}
```

---

## Testing Strategy

### arrow-builder tests

- Generic buffer operations (allocation, capacity)
- Lazy column allocation behavior
- Null bitmap correctness
- Arrow conversion for all supported types
- Codegen output verification
- **NO logging concepts in tests**

### lmao tests

- Schema validation and type safety
- System column eager allocation
- User attribute lazy allocation
- Scope class generation and inheritance
- Context propagation correctness
- SpanLogger method generation
- Entry type semantics
- **Uses arrow-builder, verifies logging behavior**

---

## File Organization

```
packages/
├── arrow-builder/
│   └── src/lib/
│       ├── buffer/
│       │   ├── columnBufferGenerator.ts  # new Function() codegen
│       │   ├── createColumnBuffer.ts     # Buffer factory
│       │   ├── types.ts                  # ColumnBuffer interface
│       │   └── microseconds.ts           # Branded timestamp type
│       ├── schema-types.ts               # Generic schema types
│       └── index.ts                      # Public exports
│
└── lmao/
    └── src/lib/
        ├── schema/                       # S.enum, S.category, etc.
        │   ├── builder.ts                # Schema DSL
        │   ├── types.ts                  # TagAttributeSchema
        │   └── defineTagAttributes.ts    # Schema factory
        ├── codegen/
        │   ├── spanLoggerGenerator.ts    # SpanLogger codegen
        │   └── scopeGenerator.ts         # Scope class codegen
        ├── lmao.ts                       # Main entry, context creation
        ├── spanBuffer.ts                 # SpanBuffer factory (extends ColumnBuffer)
        ├── types.ts                      # SpanBuffer, TaskContext interfaces
        ├── convertToArrow.ts             # Tree walking, dictionary building, Arrow conversion
        └── flushScheduler.ts             # Background processing
```

---

## Summary

| Aspect             | arrow-builder                           | lmao                                         |
| ------------------ | --------------------------------------- | -------------------------------------------- |
| **Purpose**        | Generic columnar buffer engine          | Structured logging library                   |
| **Level**          | Low-level primitives                    | High-level API                               |
| **Focus**          | Explicit allocations, memory efficiency | Developer experience, zero overhead hot path |
| **Knowledge**      | Generic columnar data                   | Logging/tracing semantics                    |
| **Allocations**    | Visible, controllable                   | Delegates to arrow-builder                   |
| **Schema**         | Generic types, extensible metadata      | Extends with masking, `attr_` prefix         |
| **Lazy Columns**   | Provides pattern (shared ArrayBuffer)   | Uses for user attributes                     |
| **System Columns** | No concept                              | ALWAYS eager (timestamps, operations)        |
| **Scope**          | No concept                              | SEPARATE class from buffer columns           |
| **Codegen**        | `generateColumnBufferClass()`           | Extends with SpanLogger, Scope               |
| **Tree Walking**   | No concept                              | Owns tree traversal and dictionary building  |
| **Dependencies**   | apache-arrow only                       | arrow-builder + apache-arrow                 |

---

## Quick Reference: Who Owns What?

### Decision Flowchart

```
Is it logging/tracing specific?
├── YES → lmao
│   Examples: spans, scopes, entry types, masking
└── NO → Could a metrics app use it?
    ├── YES → arrow-builder
    │   Examples: TypedArray creation, null bitmaps, Arrow conversion
    └── NO → Probably lmao
```

### Concrete Examples

| Feature                             | Package       | Why                               |
| ----------------------------------- | ------------- | --------------------------------- |
| Cache-aligned TypedArray allocation | arrow-builder | Generic optimization              |
| Lazy column with shared ArrayBuffer | arrow-builder | Generic memory pattern            |
| Null bitmap management              | arrow-builder | Generic Arrow format              |
| Buffer capacity, no hidden resize   | arrow-builder | Generic allocation control        |
| Runtime class generation            | arrow-builder | Generic V8 optimization           |
| `attr_` prefix convention           | **lmao**      | Logging-specific naming           |
| System columns (timestamps, ops)    | **lmao**      | Logging-specific hot path         |
| Scope class generation              | **lmao**      | Logging-specific inheritance      |
| Entry types (span-start, info)      | **lmao**      | Logging-specific lifecycle        |
| SpanLogger with typed methods       | **lmao**      | Logging-specific API              |
| Masking functions                   | **lmao**      | Logging-specific privacy          |
| Context flow                        | **lmao**      | Logging-specific hierarchy        |
| Tree walking (span trees)           | **lmao**      | Logging-specific tree structure   |
| Dictionary building across tree     | **lmao**      | Logging-specific Arrow conversion |
| RecordBatch creation with dicts     | **lmao**      | Logging-specific Arrow output     |

---

## Design Decision Checklist

### Adding to arrow-builder

- [ ] Feature is generic (could be used by metrics, CSV parsing, etc.)
- [ ] No dependency on lmao types or concepts
- [ ] No knowledge of logging semantics (tags, spans, scopes)
- [ ] Provides explicit allocation control (no hidden allocations)
- [ ] Documentation doesn't mention logging/tracing

### Adding to lmao

- [ ] Feature is logging/tracing specific
- [ ] Uses arrow-builder primitives, doesn't duplicate them
- [ ] Clear documentation of why it's not in arrow-builder
- [ ] System columns remain eager (no lazy checks in hot path)
- [ ] Scope values stay separate from buffer columns
