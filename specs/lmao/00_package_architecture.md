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
│  • Op + Span pattern (op wraps, span invokes)                   │
│  • Context flow (traceContext→module→op→span)                   │
│  • SpanBuffer classes with direct properties ($name_*)          │
│  • Scope classes (SEPARATE from buffer columns)                 │
│  • Feature flag evaluation                                       │
│  • Fluent logging API (tag, log destructured from ctx)          │
│  • System columns (timestamp, entry_type) - ALWAYS eager       │
│  • User attribute columns - lazy by default                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  @smoothbricks/arrow-builder                     │
│     Low-Level Alternative to direct flechette table building    │
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
│                       @uwdata/flechette                          │
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
buffer.userId_values[idx] = value; // Direct write, no hidden alloc

// Lazy columns: allocation happens when getter is first accessed
const values = buffer.userId_values; // Allocates HERE, once, explicitly
```

**lmao** builds on this with logging-specific optimizations:

```typescript
// lmao - logging-aware allocation strategy
// System columns: ALWAYS eager (hot path critical)
// User attributes: lazy by default (sparse data)
const GET = op(async ({ span, log, tag }) => {
  log.info('Starting'); // _timestamps/_operations already allocated (eager)
  tag.userId('u123'); // userId column allocated on first use (lazy)
});
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

**WHY**: System columns (timestamp, entry_type) are written on EVERY log entry. Even a single `if (values === null)`
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
- **Direct properties**: `buffer.userId_values`, not `buffer.columns['userId'].values`
- **Runtime codegen**: `new Function()` generates monomorphic code with stable call sites
- **No dynamic property access**: All property names known at codegen time
- **Op is a plain class**: Stable hidden class, no Proxy, no Function subclassing

### 4. Application-Agnostic Primitives (arrow-builder)

**WHY**: arrow-builder should be usable for ANY columnar data use case, not just logging. This ensures clean separation
and prevents feature creep.

**TEST**: If you can build these using ONLY arrow-builder, the separation is correct:

- ✅ Time-series metrics collector
- ✅ CSV-to-Arrow converter
- ✅ Event sourcing buffer
- ✅ Database query result caching
- ❌ Structured logging (needs lmao's ops, spans, scopes, entry types)

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

A low-level alternative to building Arrow tables directly with flechette constructors. The library focuses on:

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
9. **String interner** - Global `Map<string, Uint8Array>` for pre-encoding UTF-8 of known strings
10. **Utf8Encoder interface** - Contract for UTF-8 encoding with `encode()`, `byteLength()`, `encodeInto()`
11. **DictionaryBuilder** - Builds Arrow dictionaries with 2nd-occurrence caching pattern

### What arrow-builder MUST NOT know about

- ❌ Logging or tracing concepts (ops, spans, traces, contexts)
- ❌ Entry types (info, warn, error, span-start, span-end)
- ❌ Scope or scoped attributes
- ❌ Feature flags or evaluation contexts
- ❌ Masking functions (hash, url, sql, email)
- ❌ Context propagation or hierarchy
- ❌ System vs user column distinction
- ❌ Tree structures (parent/child spans, buffer.\_children, buffer.\_next)
- ❌ Any `@smoothbricks/lmao` dependency

### Column Naming Convention

**User columns** use the field name directly from the schema (no prefix):

```typescript
buffer.userId; // User attribute column (lazy)
buffer.requestId; // User attribute column (lazy)
buffer.httpStatus; // User attribute column (lazy)
```

**System properties** use `_` prefix to prevent namespace collisions:

```typescript
buffer._timestamps; // System column (eager)
buffer._operations; // System column (eager)
buffer._writeIndex; // Internal state
buffer._capacity; // Internal state
buffer._next; // Buffer chaining
```

This keeps the user API clean while preventing collisions between user-defined field names and internal buffer
properties.

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
const userId_sym = Symbol('userId');

function allocate_userId(self) {
  if (self[userId_sym]) return self[userId_sym];

  const capacity = self._capacity;
  const nullBitmapSize = Math.ceil(capacity / 8);
  const alignedNullOffset = Math.ceil(nullBitmapSize / 4) * 4; // 4 = bytesPerElement
  const totalSize = alignedNullOffset + capacity * 4;

  const buffer = new ArrayBuffer(totalSize); // ONE allocation
  const storage = {
    buffer: buffer,
    nulls: new Uint8Array(buffer, 0, nullBitmapSize),
    values: new Uint32Array(buffer, alignedNullOffset, capacity),
  };
  self[userId_sym] = storage;
  return storage;
}

class GeneratedColumnBuffer {
  get userId_nulls() {
    return allocate_userId(this).nulls;
  }
  get userId_values() {
    return allocate_userId(this).values;
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
- **Context propagation**: Automatic trace correlation through traceContext→op→span hierarchy
- **System column optimization**: timestamp/entry_type are NEVER lazy

### What lmao OWNS

1. **Schema DSL** - `S.enum()`, `S.category()`, `S.text()`, `S.number()`, `S.boolean()`
2. **Tag attribute definitions** - Schema definitions with masking transforms
3. **Feature flag evaluation** - `defineFeatureFlags()` with sync/async evaluation
4. **Op class** - Wraps functions with module binding, handles buffer creation
5. **span() invocation** - Unified API for calling ops with contextual naming
6. **Context flow** - TraceContext → Op execution → Span hierarchy
7. **SpanBuffer creation** - Extends arrow-builder's ColumnBuffer with span metadata
8. **Scope class generation** - SEPARATE from buffer columns, holds inheritable values
9. **SpanLogger generation** - Typed methods per schema field
10. **Fluent logging API** - `tag.userId()`, `log.info()`, destructured from context
11. **Entry type semantics** - Span lifecycle (start/ok/err), log levels (info/debug/warn/error)
12. **System column management** - timestamp, entry_type (ALWAYS eager, never lazy)
13. **Library integration** - Prefix-based attribute namespacing for third-party libraries
14. **Background flush scheduling** - Adaptive flush timing based on buffer capacity
15. **Tree walking** - Recursive traversal of SpanBuffer trees (parent/children/overflow chains)
16. **Dictionary building** - Two-pass conversion: build dictionaries across tree, then convert to RecordBatches
17. **Arrow Table creation** - Orchestrates conversion using shared dictionaries and `arrow.makeData()`
18. **Utf8Cache** - SIEVE-based cache implementing arrow-builder's `Utf8Encoder` interface
19. **globalUtf8Cache singleton** - Shared cache for cross-flush UTF-8 encoding benefits
20. **Pre-encoded contexts** - ModuleContext with `utf8PackageName`, `utf8PackagePath`, `utf8GitSha`
21. **Line number injection** - Transformer inserts line as first arg to span()

### System Columns vs User Attributes

**System columns** (timestamp, entry_type) are written on EVERY entry - ALWAYS eager, pre-allocated in constructor.

**User attribute columns** are sparse and optional - lazy by default, allocated only when first written.

See `01b_columnar_buffer_architecture.md` for implementation details.

### SpanBuffer Property Naming Convention

SpanBuffer public properties correspond exactly to Arrow table column names for obvious data flow:

- **Core Arrow Columns** (exact underscore names):
  - `trace_id`, `thread_id`, `span_id`, `parent_thread_id`, `parent_span_id`
  - `timestamp`, `entry_type`

- **System Schema Fields** (camelCase for API ergonomics):
  - `message`, `lineNumber`, `errorCode`, `exceptionStack`, `ffValue`, `uint64Value`

- **Internal Properties** (`_` prefix for encapsulation):
  - `_system`, `_identity`, `_writeIndex`, `_capacity`, `_next`, `_hasParent`
  - `_children`, `_parent`, `_module`, `_spanName`, `_callsiteModule`, `_scopeValues`

This ensures SpanBuffer properties match Arrow columns 1:1, enabling users to define custom columns with consistent
naming.

---

## Package Dependencies

```json
// packages/arrow-builder/package.json
{
  "name": "@smoothbricks/arrow-builder",
  "dependencies": {
    "@uwdata/flechette": "^2.3.0"
  }
  // NO @smoothbricks/lmao dependency!
}

// packages/lmao/package.json
{
  "name": "@smoothbricks/lmao",
  "dependencies": {
    "@smoothbricks/arrow-builder": "workspace:*", // Uses arrow-builder
    "@uwdata/flechette": "^2.3.0"
  }
}
```

---

## Summary

| Aspect               | arrow-builder                                | lmao                                         |
| -------------------- | -------------------------------------------- | -------------------------------------------- |
| **Purpose**          | Generic columnar buffer engine               | Structured logging library                   |
| **Level**            | Low-level primitives                         | High-level API                               |
| **Focus**            | Explicit allocations, memory efficiency      | Developer experience, zero overhead hot path |
| **Knowledge**        | Generic columnar data                        | Logging/tracing semantics                    |
| **Allocations**      | Visible, controllable                        | Delegates to arrow-builder                   |
| **Schema**           | Generic types, extensible metadata           | Extends with masking                         |
| **Naming**           | User-defined column names                    | `_` prefix for system, direct for user       |
| **Lazy Columns**     | Provides pattern (shared ArrayBuffer)        | Uses for user attributes                     |
| **System Columns**   | No concept                                   | ALWAYS eager (\_timestamps, \_operations)    |
| **Scope**            | No concept                                   | SEPARATE class from buffer columns           |
| **Op/Span**          | No concept                                   | Op wraps fn, span() invokes                  |
| **Codegen**          | `generateColumnBufferClass()`                | Extends with SpanLogger, TagAPI, Scope       |
| **String Interning** | `intern()` for known strings (unbounded)     | `Utf8Cache` for runtime strings (SIEVE)      |
| **UTF-8 Encoding**   | `Utf8Encoder` interface, `DictionaryBuilder` | `globalUtf8Cache`, pre-encoded contexts      |
| **Tree Walking**     | No concept                                   | Owns tree traversal and dictionary building  |
| **Dependencies**     | @uwdata/flechette only                       | arrow-builder + @uwdata/flechette            |

---

## Quick Reference: Who Owns What?

### Decision Flowchart

```
Is it logging/tracing specific?
├── YES → lmao
│   Examples: ops, spans, scopes, entry types, masking
└── NO → Could a metrics app use it?
    ├── YES → arrow-builder
    │   Examples: TypedArray creation, null bitmaps, Arrow conversion
    └── NO → Probably lmao
```

### Concrete Examples

| Feature                              | Package       | Why                                   |
| ------------------------------------ | ------------- | ------------------------------------- |
| Cache-aligned TypedArray allocation  | arrow-builder | Generic optimization                  |
| Lazy column with shared ArrayBuffer  | arrow-builder | Generic memory pattern                |
| Null bitmap management               | arrow-builder | Generic Arrow format                  |
| Buffer capacity, no hidden resize    | arrow-builder | Generic allocation control            |
| Runtime class generation             | arrow-builder | Generic V8 optimization               |
| `intern()` / `getInterned()`         | arrow-builder | Generic UTF-8 pre-encoding            |
| `Utf8Encoder` interface              | arrow-builder | Generic encoding contract             |
| `DictionaryBuilder`                  | arrow-builder | Generic Arrow dictionary construction |
| `_` prefix for system properties     | **lmao**      | Logging-specific namespace mgmt       |
| System columns (\_timestamps, \_ops) | **lmao**      | Logging-specific hot path             |
| Scope class generation               | **lmao**      | Logging-specific inheritance          |
| Entry types (span-start, info)       | **lmao**      | Logging-specific lifecycle            |
| Op class (module binding)            | **lmao**      | Logging-specific operation wrapping   |
| span() invocation                    | **lmao**      | Logging-specific call-site naming     |
| SpanLogger with typed methods        | **lmao**      | Logging-specific API                  |
| Masking functions                    | **lmao**      | Logging-specific privacy              |
| Context flow                         | **lmao**      | Logging-specific hierarchy            |
| `Utf8Cache` (SIEVE)                  | **lmao**      | Logging-specific bounded caching      |
| `globalUtf8Cache` singleton          | **lmao**      | Logging-specific cross-flush cache    |
| Pre-encoded contexts (utf8\*)        | **lmao**      | Logging-specific context optimization |
| Tree walking (span trees)            | **lmao**      | Logging-specific tree structure       |
| Dictionary building across tree      | **lmao**      | Logging-specific Arrow conversion     |
| RecordBatch creation with dicts      | **lmao**      | Logging-specific Arrow output         |

---

## Design Decision Checklist

### Adding to arrow-builder

- [ ] Feature is generic (could be used by metrics, CSV parsing, etc.)
- [ ] No dependency on lmao types or concepts
- [ ] No knowledge of logging semantics (tags, ops, spans, scopes)
- [ ] Provides explicit allocation control (no hidden allocations)
- [ ] Documentation doesn't mention logging/tracing

### Adding to lmao

- [ ] Feature is logging/tracing specific
- [ ] Uses arrow-builder primitives, doesn't duplicate them
- [ ] System columns remain eager (no lazy checks in hot path)
- [ ] Scope values stay separate from buffer columns
