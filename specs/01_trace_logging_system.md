# Project: Trace Logging System

## Core Insight

**Observation**: Most logging systems are either too slow (string concatenation at runtime) or too hard to query
(unstructured). We need something that's blazing fast at runtime but produces rich, queryable data.

## Design Rationale: Why op() + span()?

Understanding the design rationale helps explain WHY the current `op()` + `span()` pattern was chosen over alternatives.

### Alternative Considered: task('name', fn)

```typescript
// ALTERNATIVE (rejected): Name at definition time, ctx passed everywhere
const { task } = httpModule;

const GET = task('http-get', async (ctx, url: string) => {
  ctx.tag.method('GET');
  await ctx.deps.retry.attempt(1); // deps would be bound closures
  return ctx.ok(response);
});

// Invocation
await GET(httpRoot, 'https://example.com');
```

**Why this approach was rejected**:

1. **Per-span allocation for deps**: `ctx.deps.retry` would be a closure recreated for EVERY span, even though most
   spans don't use all deps
2. **ctx. prefix everywhere**: Verbose - every tag, log, span call needed `ctx.`
3. **Fixed span names**: Caller couldn't provide contextual names like `'fetch-user-123'`
4. **Unclear responsibility**: `task()` would do definition AND implied invocation semantics

### Chosen Approach: op() + span()

```typescript
// op() captures module binding, span() provides name at call site
const { op } = httpModule;

const GET = op(async ({ span, log, tag, deps }, url: string) => {
  // Destructure for ergonomics - no ctx. prefix
  tag.method('GET');
  // span() is the unified invocation - NAME at call site
  const response = await span('fetch', fetchOp, url);
  return response;
});

// Root invocation - NAME provided here
await httpRoot.span('GET', GET, 'https://example.com');
```

**Why this approach was chosen**:

| Aspect          | Alternative (rejected) | Chosen Approach                  |
| --------------- | ---------------------- | -------------------------------- |
| Deps allocation | Per-span closures      | Zero - just Op refs              |
| Ergonomics      | `ctx.tag.userId()`     | `tag.userId()`                   |
| Span naming     | Definition time        | Call site (flexible)             |
| Module binding  | Implicit               | Explicit via Op class            |
| V8 optimization | Closure-heavy          | Plain class, stable hidden class |

### Line Number Injection

The TypeScript transformer injects line numbers as the first argument to `span()`:

```typescript
// User writes:
await span('retry', deps.retry, 1);

// Transformer outputs:
await span(42, 'retry', deps.retry, 1);
```

This enables source code linking without runtime stack trace parsing.

## System Overview

The trace logging system provides a complete solution for high-performance, structured observability with AI agent
integration. It consists of these main components:

### 1. [Trace Schema System](./01a_trace_schema_system.md)

**Purpose**: Type-safe configuration and attribute management

- **Tag Attributes**: Structured data logged to spans with automatic masking
- **Feature Flags**: Dynamic behavior configuration with analytics tracking
- **WHY**: Provides a single source of truth for data shapes, validation, and privacy rules, enabling type-safe
  operations and automatic masking.

### 2. [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)

**Purpose**: High-performance runtime log buffers with fixed row layout.

- Implements data-oriented storage with columnar TypedArrays and self-tuning capacity.
- **Fixed Row Layout**: Row 0 = span-start (tag overwrites), Row 1 = completion (pre-initialized as span-exception), Row
  2+ = events (log appends).
- **Exception Safety**: Row 1 pre-initialized as `span-exception` ensures valid data even if exception is never caught.
- **Buffer Overflow**: Graceful chaining via `createNextBuffer()` when capacity exceeded - see
  [Self-Tuning](./01b2_buffer_self_tuning.md) for capacity learning.
- **Distributed Tracing**: `threadId` (64-bit random, once per worker) + `spanId` (32-bit counter) for collision-free
  span identification across workers.
- **High-Precision Timestamps**: Anchored design with sub-millisecond precision from performance.now()/hrtime.
- **WHY**: Achieves <0.1ms runtime overhead and >90% storage compression by separating the hot path (writes) from the
  cold path (serialization).

### 3. [Arrow Table Structure](./01f_arrow_table_structure.md)

**Purpose**: Queryable data format for analysis and storage.

- Zero-copy conversion from runtime buffers to Apache Arrow format.
- **WHY**: Enables efficient querying, compression, and integration with data analysis tools.

### 4. [Context Flow and Op/Span Pattern](./01c_context_flow_and_op_wrappers.md)

**Purpose**: Hierarchical context management with span correlation.

- **op()**: Wraps functions with module binding, captures gitSha/packageName/packagePath
- **span()**: Unified invocation API - name provided at call site
- **Dual Module Attribution**: `callsiteModule` for row 0 (where span was invoked), `module` for rows 1+ (where code
  executes)
- **Context destructuring**: `{ span, log, tag, deps }` for ergonomic access
- **WHY**: Zero per-span allocation for deps, flexible naming, clean business logic

### 5. [AI Agent Integration](./01d_ai_agent_integration.md)

**Purpose**: Structured trace access for automated analysis and debugging.

- Details the MCP server, test framework plugins for AI test run correlation, and production log access.
- **WHY**: Allows AI agents to query and understand real system behavior, moving from static code analysis to dynamic
  trace analysis.

### 6. [Library Integration Pattern](./01e_library_integration_pattern.md)

**Purpose**: Enable third-party libraries to provide traced operations with clean APIs.

- Defines the core pattern for library authors to create traced functionality without naming conflicts.
- **WHY**: Enables a rich ecosystem of traced libraries while maintaining performance and avoiding attribute name
  collisions through prefixing.

## Core Architecture Principles

- **Two-Phase Logging**: Separate runtime writes from background processing.
- **Data-Oriented Design**: Use columnar storage and null bitmaps for performance, and near instant conversion to
  columnar formats like Apache Arrow.
- **CPU-Friendly Performance**: Design patterns that leverage V8 optimizations like hidden classes and inline caches,
  and are friendly to the CPU's branch predictor.
- **Runtime Codegen**: Use new Function() code generation at application startup to avoid runtime overhead.
- **Zero-Allocation Deps**: Dependencies are Op references, not per-span closures.
- **Call-Site Naming**: Span names provided at invocation for contextual flexibility.

## The Op + Span Pattern

### op() - Definition Time

`op()` wraps a function and captures module binding:

```typescript
const { op } = httpModule;

// op() captures: gitSha, packageName, packagePath, schema
const GET = op(async ({ span, log, tag, deps }, url: string) => {
  tag.method('GET');
  tag.url(url);

  // deps are just Op references - zero allocation
  const result = await span('fetch', fetchOp, url);

  log.info('Request completed');
  return result;
});
```

**What op() does**:

1. Creates an Op instance with module metadata
2. Stores the function reference
3. Returns the Op (NOT a wrapper function)

**What op() does NOT do**:

- Create closures for deps
- Allocate per-span
- Bind context

### span() - Invocation Time

`span()` is the unified API for invoking ops:

```typescript
// Root invocation (from request handler)
await httpRoot.span('GET', GET, 'https://example.com');

// Nested invocation (inside an op)
await span('retry', deps.retry, 1);
await span('validate', validateOp, data);
```

**What span() does**:

1. Creates SpanBuffer with op's module schema
2. Registers buffer with parent (for tree structure)
3. Builds destructurable context: `{ span, log, tag, deps, ff, env }`
4. Executes op.fn with context + args
5. Handles try/catch for span-exception entries
6. Records completion (span-ok or span-err)

### Context Destructuring

Inside an op, context is destructured for ergonomics:

```typescript
const processUser = op(async ({ span, log, tag, deps, ff, env }, user: User) => {
  // Direct access - no ctx. prefix
  tag.userId(user.id);
  log.info('Processing user');

  // Feature flags
  const { newAlgorithm } = ff;
  if (newAlgorithm) {
    newAlgorithm.track();
    await span('new-algo', deps.algo.newProcess, user);
  } else {
    await span('old-algo', deps.algo.oldProcess, user);
  }

  // Environment
  const region = env.AWS_REGION;

  return { success: true };
});
```

### Dependencies

`deps` are just references to Op instances - no per-span allocation:

```typescript
const { op } = httpModule;

// deps defined at module level
const GET = op(async ({ span, deps }, url: string) => {
  // deps.retry is an Op instance, NOT a bound closure
  // Calling it via span() is explicit
  await span('retry-1', deps.retry, 1);
  await span('retry-2', deps.retry, 2);

  // Can destructure for convenience
  const { retry, auth } = deps;
  await span('auth', auth, token);
});
```

**Why this matters**:

- Old design: `ctx.deps.retry.attempt(1)` created a closure for EVERY span
- New design: `deps.retry` is just an Op reference, shared across all spans

## Key Innovations

1. **Zero-Allocation Deps**: Op references instead of per-span closures
2. **Call-Site Naming**: `span('name', op, args)` for contextual flexibility
3. **Context Destructuring**: `{ span, log, tag, deps }` for clean code
4. **Self-Tuning Buffers**: Each module learns optimal capacity from usage patterns
5. **Span-Aware Configuration**: Feature flags correlated to specific operations
6. **System Self-Tracing**: The trace system traces its own optimization decisions
7. **AI Agent Integration**: Structured access to trace data for automated analysis
8. **Line Number Injection**: Transformer adds line as first arg to span()

## V8 Optimization Patterns

The library is designed around V8's optimization characteristics. Understanding these patterns explains many design
decisions.

### 1. Hidden Classes and Inline Caching

V8 creates "hidden classes" (also called "shapes" or "maps") for objects with the same property layout. Objects sharing
a hidden class enable **inline caching** - V8 can remember the exact memory offset for each property, making access O(1)
instead of dictionary lookup.

**Pattern**: Define all properties in constructors, never add properties dynamically.

```typescript
// ✅ GOOD: All properties defined upfront
class TraceContext {
  constructor() {
    this.traceId = undefined;
    this.span = undefined;
    this.ff = undefined;
    this.env = undefined; // Even if undefined, declare it!
  }
}

// ❌ BAD: Properties added dynamically break hidden class
const ctx = {};
ctx.traceId = 'abc'; // Hidden class transition #1
ctx.span = spanFn; // Hidden class transition #2
```

**Application in LMAO**:

- `new Function()` codegen generates classes with ALL properties in constructor
- `.ctx<Extra>(defaults)` requires all keys to be enumerable for codegen
- `Object.create(proto)` used instead of object spreads `{...ctx}`

### 2. Monomorphic Call Sites

V8 optimizes function calls that always receive the same types. A call site that sees multiple types becomes
"polymorphic" (slower) or "megamorphic" (much slower, dictionary lookup).

**Pattern**: Ensure hot-path functions always receive same-shaped objects.

```typescript
// ✅ GOOD: All SpanBuffers have same shape (from generated class)
function writeTimestamp(buffer: SpanBuffer, idx: number, value: bigint) {
  buffer._timestamps[idx] = value; // Monomorphic - same hidden class every time
}

// ❌ BAD: Different buffer shapes at same call site
function writeTimestamp(buffer: any, idx: number, value: bigint) {
  buffer.timestamps[idx] = value; // Polymorphic - could be any shape
}
```

**Application in LMAO**:

- Generated buffer classes have fixed shapes per schema
- SpanContext prototype chain is consistent across all spans
- Enum setters use switch-case (JIT-inlined) not Map.get()

### 3. Runtime Codegen with `new Function()`

V8 optimizes generated code the same as handwritten code. By generating specialized code at startup, we get:

- Unrolled loops for known field counts
- Direct property access without string lookups
- Switch statements that become jump tables

**Pattern**: Generate code at module initialization (cold path), execute it in hot path.

```typescript
// At module init (cold path):
const SpanLoggerClass = new Function(
  'return class SpanLogger { ' +
    'userId(v) { this._buffer.userId(this._idx, v); return this; } ' +
    'requestId(v) { this._buffer.requestId(this._idx, v); return this; } ' +
    '}'
)();

// At runtime (hot path) - zero overhead:
logger.userId('123').requestId('req-456');
```

**Application in LMAO**:

- `SpanLogger` classes generated per schema
- `ColumnBuffer` classes generated per schema
- Enum mapping functions use generated switch-case
- Scope `_setScope()` has unrolled per-field code

### 4. Prototype-Based Context Inheritance

`Object.create(proto)` is faster than object spreads `{...parent}` because:

- No property enumeration
- No copying - just prototype chain link
- Maintains hidden class stability

**Why Object Spread Breaks Hidden Classes:**

V8 creates hidden classes based on the **order** properties are added. Object spread `{...obj}` can create new hidden
classes even when properties are in the same order, because:

- The spread operation is seen as dynamic property addition
- V8 may not recognize that the resulting object has the same shape as the source
- Each spread operation can trigger a new hidden class creation

**Pattern**: Use prototype chain for context inheritance, or generated classes with fixed property order.

```typescript
// ✅ GOOD: Prototype inheritance - preserves hidden class
const childCtx = Object.create(parentCtx);
childCtx.spanName = 'child-op'; // Only override what changes

// ✅ GOOD: Generated class with fixed property order - stable hidden class
class GeneratedScope {
  userId = undefined; // Property 1 - always same order
  requestId = undefined; // Property 2 - always same order
}
const scope1 = new GeneratedScope();
const scope2 = new GeneratedScope(); // Same hidden class as scope1

// ❌ BAD: Object spread creates new hidden class each time
const childCtx = { ...parentCtx, spanName: 'child-op' }; // New hidden class!
const childScope = { ...parentScope }; // New hidden class!
```

**Key Insight from V8 Documentation:**

- V8 reuses hidden classes when objects have **the same properties in the same order**
- Property order matters: `{a: 1, b: 2}` and `{b: 2, a: 1}` get different hidden classes
- Object spread `{...obj}` can create new hidden classes even with same properties
- Generated classes guarantee consistent property order = stable hidden classes

**References:**

- [V8 Fast Properties Blog](https://v8.dev/blog/fast-properties) - Hidden class internals
- [Web.dev V8 Performance Tips](https://web.dev/articles/speed-v8) - Best practices
- [V8 Hidden Classes and Inline Caching](https://richardartoul.github.io/jekyll/update/2015/04/26/hidden-classes.html) -
  Detailed explanation

**Application in LMAO**:

- `createTraceContext()` uses `Object.create(TraceContextProto)`
- Child spans use `Object.create(parentCtx)`
- User `Extra` properties inherit through prototype chain
- Scope classes are generated with fixed property order for stable hidden classes

### 5. TypedArray Performance

TypedArrays (Uint8Array, Float64Array, BigInt64Array) are faster than regular arrays for numeric data:

- No type checking per access
- Contiguous memory layout
- Cache-friendly access patterns

**Pattern**: Use TypedArrays for all numeric columns, align to cache lines.

```typescript
// ✅ GOOD: TypedArray with cache-aligned capacity
const timestamps = new BigInt64Array(alignedCapacity); // 64-byte aligned
timestamps[idx] = value; // Direct memory write

// ❌ BAD: Regular array with boxing
const timestamps: bigint[] = [];
timestamps[idx] = value; // Boxing overhead
```

**Application in LMAO**:

- All numeric columns use TypedArrays
- Capacity aligned to 64-byte cache lines (8 elements × 8 bytes)
- Null bitmaps use Uint8Array with bit operations

### 6. Lazy Allocation Pattern

For sparse data (user attributes), defer allocation until first write:

**Pattern**: Getter-based lazy allocation with `undefined` check.

```typescript
// Generated code:
get userId_values() {
  let v = this._userId_values;
  if (v === undefined) {
    v = this._userId_values = new Array(this._capacity);
    this._userId_nulls = new Uint8Array((this._capacity + 7) >>> 3);
  }
  return v;
}
```

**Application in LMAO**:

- System columns (`_timestamps`, `_operations`) always eager
- User attribute columns lazy by default
- `S.number().eager()` forces eager allocation for hot columns

### Summary: Why These Patterns Matter

| Pattern                | Without Optimization | With Optimization  | Speedup   |
| ---------------------- | -------------------- | ------------------ | --------- |
| Hidden class stability | Dictionary lookup    | Direct offset      | 10-100x   |
| Monomorphic calls      | Polymorphic dispatch | Inline cached      | 2-10x     |
| Generated code         | Interpreted loops    | JIT-compiled       | 5-50x     |
| Prototype inheritance  | Object copy          | Pointer assignment | 10-100x   |
| TypedArrays            | Boxed numbers        | Raw memory         | 2-5x      |
| Lazy allocation        | Allocate all upfront | On-demand          | Memory 2x |

These patterns compound - a logging call that would take 10μs with naive implementation takes <0.1μs with all
optimizations applied.

## Future Experiments

- **Buffer Performance**: Benchmark different columnar storage strategies (e.g., single TypedArray vs. multiple) for
  memory and CPU efficiency
- **Schema Evolution**: Design and test schema versioning and migration strategies
- **Prototype schema-driven masking** with runtime codegen

## Integration with Development Platform

The trace logging system integrates with these platform components:

- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Foundational entry type system
- **[ValueObjects Schema System](./02_valueobjects_schema_system.md)**: Shared schema definitions
- **[Context API Framework](./03_context_api_framework.md)**: Promise-local context propagation
- **[AI Agent Development System](./08_ai_agent_development_system.md)**: MCP integration

## Related Documents

1. **[Package Architecture](./00_package_architecture.md)** - arrow-builder vs lmao separation, op() + span() evolution
2. **[Schema System](./01a_trace_schema_system.md)** - Attribute definitions and type safety
3. **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)** - Memory layout and performance
4. **[Context Flow and Op/Span Pattern](./01c_context_flow_and_op_wrappers.md)** - Execution model and span hierarchy
5. **[AI Integration](./01d_ai_agent_integration.md)** - LLM-powered analysis and insights
6. **[Library Integration Pattern](./01e_library_integration_pattern.md)** - Library attribute conflict resolution
7. **[Arrow Table Structure](./01f_arrow_table_structure.md)** - Final queryable format
8. **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)** - Runtime API generation
9. **[TypeScript Transformer](./01o_typescript_transformer.md)** - Compile-time optimizations for V8

## Quick Reference: op() + span() Design

| Aspect             | Alternative (rejected)      | Chosen Approach                |
| ------------------ | --------------------------- | ------------------------------ |
| **Definition**     | `task('name', fn)`          | `op(fn)`                       |
| **Invocation**     | `await GET(ctx, url)`       | `await span('GET', GET, url)`  |
| **Name timing**    | Definition time             | Call site                      |
| **Deps**           | Per-span closures           | Op references (zero alloc)     |
| **Context**        | `ctx.tag.userId()`          | `tag.userId()` (destructured)  |
| **Nested calls**   | `ctx.deps.retry.attempt(1)` | `span('retry', deps.retry, 1)` |
| **Module binding** | Implicit in closure         | Explicit in Op class           |
| **Line numbers**   | `.line(N)` fluent method    | First arg to span()            |
