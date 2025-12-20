# Span Identity

> **Part of [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**
>
> This document covers span identification: how spans are uniquely identified across distributed systems. For memory
> layout details, see [SpanBuffer Memory Layout](./01b5_spanbuffer_memory_layout.md).

## Span Definition

> **A span represents a unit of work within a single thread of execution.**

This definition is the foundation of LMAO's span identification design. Unlike OpenTelemetry which defines a span as
simply a "unit of work" with random 64-bit IDs, LMAO explicitly ties spans to their thread of execution.

### Comparison to OpenTelemetry

| Aspect              | OpenTelemetry                | LMAO                                               |
| ------------------- | ---------------------------- | -------------------------------------------------- |
| **Span Definition** | "Unit of work"               | "Unit of work within a single thread of execution" |
| **Span ID**         | Random 64-bit                | `(thread_id, span_id)` composite                   |
| **Generation**      | Crypto random per span       | `thread_id` once per thread, `span_id++` per span  |
| **Hot Path Cost**   | Random generation + BigInt   | Simple `i++` increment                             |
| **Thread Concept**  | None (spans are independent) | Explicit (spans belong to threads)                 |
| **Timeline View**   | Requires timestamp sorting   | `span_id` gives within-thread ordering             |

## Distributed Span ID Design

### Problem: Span ID Collisions in Distributed Tracing

A simple incrementing `span_id: number` works for single-process tracing, but causes collisions in distributed
scenarios:

```
Machine A: trace_id="req-123", span_id=1, 2, 3...
Machine B: trace_id="req-123", span_id=1, 2, 3... ← collision!
```

This occurs with:

- **Worker threads**: `pmap()` distributing work across threads
- **Distributed services**: Same trace spanning multiple machines
- **Serverless**: Multiple Lambda invocations for the same request

### Solution: Separate Columns (thread_id + span_id)

The span ID is split into two components with different lifecycles:

```typescript
// Generated ONCE per worker/process at startup (cold path)
const thread_id: bigint = generateRandom64Bit();

interface SpanBuffer {
  thread_id: bigint; // Reference to worker's 64-bit random ID
  span_id: number; // 32-bit incrementing counter (i++ per span)
  parent?: SpanBuffer; // Tree link to derive parent IDs
}
```

### Arrow Column Schema

| Column             | Type                 | Description                                           |
| ------------------ | -------------------- | ----------------------------------------------------- |
| `trace_id`         | `dictionary<string>` | Request correlation across services                   |
| `thread_id`        | `uint64`             | Thread/worker identifier (64-bit random, once/thread) |
| `span_id`          | `uint32`             | Unit of work within thread (incrementing counter)     |
| `parent_thread_id` | `uint64` (nullable)  | Parent span's thread                                  |
| `parent_span_id`   | `uint32` (nullable)  | Parent span's ID                                      |

### Global Uniqueness

- **Within a trace**: `(thread_id, span_id)` is unique
- **Globally**: `(trace_id, thread_id, span_id)` is globally unique

### Parent Reference

To find a parent span, you need:

- Same `trace_id` (parent is always in same trace)
- Match `thread_id = parent_thread_id` AND `span_id = parent_span_id`

### Why This Design?

**Hot Path Performance**:

- `span_id = nextSpanId++` is a simple increment (no BigInt, no crypto)
- No random generation per span
- No 64-bit arithmetic in the hot path

**Cold Path Efficiency**:

- `thread_id` generated once at worker startup using `crypto.getRandomValues()`
- BigInt conversion happens once, not per span
- Arrow conversion references the same `thread_id` for all spans in a buffer

**Collision Resistance**:

- 64-bit random thread ID: ~4 billion threads before 50% collision (birthday paradox)
- Combined with 32-bit local counter: effectively unlimited unique spans
- In practice, collision probability is negligible

**Query Flexibility**:

- Separate columns allow efficient querying by `thread_id` alone
- No struct unpacking needed for common queries
- Direct column filtering in WHERE clauses

### Thread-Local Counter Design (CRITICAL)

**The `span_id` is a THREAD-LOCAL counter**, not a per-trace counter:

```
Main thread (thread_id: 0xAAA): span_id 1, 2, 3, 4, 5...  (all traces combined)
Worker A (thread_id: 0xBBB): span_id 1, 2, 3...           (all traces combined)
Worker B (thread_id: 0xCCC): span_id 1, 2...              (all traces combined)
```

**What `span_id` IS**:

- A simple `i++` counter local to each thread/worker
- Each thread starts at 1 and increments independently
- Counts ALL spans created on that thread, across ALL traces
- Zero coordination between threads

**What `span_id` is NOT**:

- ❌ NOT a global counter shared across threads (would need synchronization)
- ❌ NOT a per-trace counter (would require Map lookups and cleanup)
- ❌ NOT semantically "nth span in this trace"

**Semantic Meaning**:

- `span_id` means "nth span created on this thread" (across all traces)
- If you need trace-relative ordering, use timestamps
- The tuple `(thread_id, span_id)` is globally unique, that's what matters

**Why This Design**:

- **Zero overhead**: Just `i++`, nothing else
- **No synchronization**: Each thread has its own counter
- **No Map lookups**: No per-trace state to manage
- **No cleanup logic**: No trace completion tracking needed
- **Still globally unique**: `(thread_id, span_id)` never collides

### Runtime Representation

```typescript
// Module-level thread-local counter (per-worker, initialized once at startup)
// NOT per-class, NOT per-trace - one counter per worker that increments across all traces
let nextSpanId = 1;

// thread_id comes from thread_id.ts module-level singleton (not a parameter)
// Module and spanName are passed directly (flattened from TaskContext)

function createSpanBuffer(schema, module, spanName, trace_id, capacity): SpanBuffer {
  const SpanBufferClass = getSpanBufferClass(schema); // Cached per schema
  return new SpanBufferClass(
    capacity,
    module, // Op's module context
    spanName, // Span name
    undefined, // parent
    false, // isChained
    trace_id,
    undefined // callsiteModule
  );
}
```

### Arrow Conversion

During cold path conversion to Arrow, span IDs become separate columns:

```typescript
// Arrow columns (separate, not a Struct)
thread_id: Uint64; // buffer.thread_id (BigInt → Uint64)
span_id: Uint32; // buffer.span_id (number → Uint32)
parent_thread_id: Uint64; // buffer.parent?.thread_id (nullable)
parent_span_id: Uint32; // buffer.parent?.span_id (nullable)
```

**Conversion efficiency**:

- `thread_id` BigInt conversion happens once per buffer (not per row)
- `span_id` uses Uint32Array directly (no conversion needed)
- Parent IDs derived from tree structure (no separate storage)

### Cross-Thread Parent References

Child spans on different threads can reference parent spans on other threads:

```typescript
// Parent span on Thread A
const parentBuffer = {
  thread_id: 0x1a2b3c4d5e6f7890n, // Thread A's ID
  span_id: 42,
  // ...
};

// Child span on Thread B (via pmap or worker)
const childBuffer = {
  thread_id: 0x9876543210fedcban, // Thread B's ID (different!)
  span_id: 1,
  parent: parentBuffer, // References parent on Thread A
  // ...
};

// Arrow output (separate columns):
// child: thread_id=0x9876543210fedcba, span_id=1, parent_thread_id=0x1a2b3c4d5e6f7890, parent_span_id=42
```

### Query Examples

```sql
-- Find all ancestors (recursive)
WITH RECURSIVE ancestors AS (
  SELECT * FROM spans
  WHERE trace_id = @trace_id AND thread_id = @tid AND span_id = @sid
  UNION ALL
  SELECT s.* FROM spans s
  JOIN ancestors a
    ON s.trace_id = a.trace_id
   AND s.thread_id = a.parent_thread_id
   AND s.span_id = a.parent_span_id
)
SELECT * FROM ancestors;

-- All spans from a specific thread (timeline view)
SELECT * FROM spans
WHERE trace_id = @trace_id AND thread_id = @tid
ORDER BY span_id;

-- Work distribution across threads
SELECT thread_id, COUNT(*) as span_count
FROM spans
WHERE trace_id = @trace_id
GROUP BY thread_id;

-- Find root spans (null parent)
SELECT * FROM spans
WHERE parent_thread_id IS NULL AND parent_span_id IS NULL;
```

### Benefits Summary

| Aspect           | Design Choice                        | Benefit                                  |
| ---------------- | ------------------------------------ | ---------------------------------------- |
| **Hot path**     | `span_id++`                          | Zero crypto/BigInt overhead per span     |
| **Cold path**    | BigInt conversion once per buffer    | Minimal conversion overhead              |
| **Collisions**   | 64-bit random thread ID              | Negligible collision probability         |
| **Parent refs**  | Tree structure with SpanBuffer links | Cross-thread parents naturally supported |
| **Arrow output** | Separate columns (not Struct)        | Direct column filtering, no unpacking    |
| **JS compat**    | 64-bit BigInt + 32-bit number        | No BigInt in hot path                    |

## TraceId: Branded String Type

**Purpose**: Provide a type-safe, validated trace identifier that is shared by reference across all spans in a trace,
avoiding string copies.

### Design Principles

1. **Shared Reference**: TraceId is a string that is passed by reference to all spans in a trace - no copying
2. **Validated**: Non-empty, max 128 characters, ASCII only (fast regex validation)
3. **Branded Type**: TypeScript branded type prevents accidental string assignment
4. **W3C Compatible**: `generateTraceId()` produces W3C Trace Context format (32 hex characters)

### Type Definition

```typescript
/**
 * TraceId is a branded string type for type safety.
 * The brand prevents accidental assignment of arbitrary strings.
 */
type TraceId = string & { readonly __brand: 'TraceId' };

/**
 * Maximum length for trace IDs.
 * 128 chars is generous - W3C format is 32 chars.
 */
const MAX_TRACE_ID_LENGTH = 128;

/**
 * Regex for validation - ASCII printable characters only.
 * Regex validation is 2x faster than character loop.
 */
const TRACE_ID_REGEX = /^[\x20-\x7E]+$/;
```

### Validation

```typescript
/**
 * Validate and create a TraceId from a string.
 *
 * Validation rules:
 * - Non-empty
 * - Max 128 characters
 * - ASCII printable only (0x20-0x7E)
 *
 * @throws Error if validation fails
 */
function createTraceId(value: string): TraceId {
  if (!value || value.length === 0) {
    throw new Error('TraceId cannot be empty');
  }
  if (value.length > MAX_TRACE_ID_LENGTH) {
    throw new Error(`TraceId exceeds max length of ${MAX_TRACE_ID_LENGTH}`);
  }
  if (!TRACE_ID_REGEX.test(value)) {
    throw new Error('TraceId must contain only ASCII printable characters');
  }
  return value as TraceId;
}
```

**Why Regex Validation**:

- Regex `test()` is ~2x faster than iterating characters
- Single call vs loop with multiple comparisons
- V8 optimizes regex well

### Generation (W3C Format)

```typescript
/**
 * Generate a new TraceId in W3C Trace Context format.
 *
 * Format: 32 lowercase hexadecimal characters (128 bits)
 * Example: "4bf92f3577b34da6a3ce929d0e0e4736"
 */
function generateTraceId(): TraceId {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);

  // Convert to hex string (lowercase, no dashes)
  let hex = '';
  for (let i = 0; i < 16; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return hex as TraceId;
}
```

### Usage Pattern

```typescript
// At request boundary - generate or accept trace ID
const trace_id = request.headers['x-trace-id'] ? createTraceId(request.headers['x-trace-id']) : generateTraceId();

// Pass to root span, _children walk parent chain
// Op's internal wrapper creates the buffer
// lineNumber is written directly to lineNumber_values[0] inside _invoke(), NOT passed to createSpanBuffer
const rootSpan = createSpanBuffer(schema, module, spanName, trace_id);
rootSpan.lineNumber_values[0] = lineNumber; // Direct TypedArray write

const childSpan = createChildSpanBuffer(rootSpan, childModule, childSpanName);
childSpan.lineNumber_values[0] = childLineNumber; // Direct TypedArray write

// All spans in trace share the SAME string reference
console.log(rootSpan.trace_id === childSpan.trace_id); // true (same reference)
```

### SpanBuffer Integration

```typescript
interface SpanBuffer {
  // TraceId is a shared string reference (zero-copy across spans)
  trace_id: TraceId;

  // SpanIdentity is per-span (25-byte ArrayBuffer)
  span_id: SpanIdentity;

  // Comparison methods check BOTH trace_id AND span_id
  isParentOf(other: SpanBuffer): boolean;
  isChildOf(other: SpanBuffer): boolean;
}

// Implementation
function isParentOf(this: SpanBuffer, other: SpanBuffer): boolean {
  // Fast string reference comparison first
  if (this.trace_id !== other.trace_id) return false;
  // Then SpanIdentity comparison
  return this.span_id.isParentOf(other.span_id);
}
```

### Benefits

| Aspect              | Plain String        | Branded TraceId             |
| ------------------- | ------------------- | --------------------------- |
| **Type Safety**     | Any string accepted | Compile-time type checking  |
| **Validation**      | None                | Enforced at creation        |
| **Reference Share** | May be copied       | Guaranteed shared reference |
| **Generation**      | Ad-hoc              | Standardized W3C format     |
| **Interop**         | Format varies       | Compatible with trace tools |
