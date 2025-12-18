# Arrow Table Structure

## Overview

The Arrow Table Structure defines the final queryable format produced by the trace logging system. It provides:

1. **Clean column schema** optimized for analytical queries
2. **Realistic trace data examples** showing spans, tags, and console.log compatibility
3. **ClickHouse query patterns** for common analytical use cases
4. **Performance characteristics** of the columnar format
5. **Zero-copy conversion patterns** for efficient cold-path processing
6. **Arrow conversion interface** defining how lmao and arrow-builder coordinate during conversion

## Zero-Copy Mandate

**CRITICAL**: All Arrow conversions MUST use `arrow.makeData()` with direct TypedArray references. The builder pattern
(`arrow.makeBuilder()`) is **PROHIBITED** because it copies every value during append operations.

### Reference Pattern

The correct zero-copy approach is demonstrated in `extern/arrow-js-ffi/src/vector.ts`:

```typescript
// CORRECT: Zero-copy with arrow.makeData()
return arrow.makeData({
  type: dataType,
  offset: 0,
  length: buffer.writeIndex,
  nullCount,
  data: buffer.timestamps.subarray(0, buffer.writeIndex), // Direct reference!
  nullBitmap,
});

// WRONG: Builder pattern (copies data)
const builder = arrow.makeBuilder({ type: dataType });
for (let i = 0; i < length; i++) {
  builder.append(values[i]); // ❌ Copies every value!
}
```

**Why Zero-Copy Matters**:

- **Performance**: Builder pattern iterates through every value, calling append() for each one
- **Memory**: Creates intermediate buffers during build process
- **GC Pressure**: Generates temporary objects that need collection
- **Hot Path Optimization**: SpanBuffer already stores data in correct TypedArray format

### Conversion Strategies by Column Type

Different column types require different zero-copy strategies:

#### 1. Primitive Types (Float64, Uint8, etc.)

**Direct subarray() reference** - simplest case:

```typescript
// Number column (Float64Array)
const data = buffer.attr_httpDuration.subarray(0, buffer.writeIndex);
return arrow.makeData({
  type: new arrow.Float64(),
  offset: 0,
  length: buffer.writeIndex,
  nullCount,
  data,
  nullBitmap,
});
```

#### 2. Dictionary-Encoded Types (category, enum)

**makeData with dictionary vector** - dictionary built from raw strings in cold path:

```typescript
// Category column: raw strings stored in hot path, dictionary built here
const rawStrings = buffer.attr_httpMethod_strings.slice(0, buffer.writeIndex);

// Build dictionary from unique strings (COLD PATH - deferred interning)
const uniqueStrings = [...new Set(rawStrings.filter((s) => s != null))].sort();
const stringToIndex = new Map(uniqueStrings.map((s, i) => [s, i]));

// Create indices by mapping raw strings to dictionary positions
const indicesData = new Uint32Array(rawStrings.length);
for (let i = 0; i < rawStrings.length; i++) {
  if (rawStrings[i] != null) {
    indicesData[i] = stringToIndex.get(rawStrings[i])!;
  }
}

return arrow.makeData({
  type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
  offset: 0,
  length: buffer.writeIndex,
  nullCount,
  data: indicesData, // Indices built from raw strings
  nullBitmap,
  dictionary: arrow.makeVector(uniqueStrings), // Sorted unique strings
});
```

#### 3. String Types (text) - Conditional Dictionary

**Space-aware strategy**: Use dictionary encoding only if it saves >128 bytes:

```typescript
// Calculate space savings
const dictionarySize = uniqueStringBytes + totalRows * 4; // 4 bytes per index
const plainSize = totalStringBytes;
const useDictionary = plainSize - dictionarySize > 128;

if (useDictionary) {
  // Use dictionary encoding (same as category above)
  return arrow.makeData({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
    data: indicesData,
    dictionary: arrow.makeVector(uniqueStrings),
    // ...
  });
} else {
  // Use plain UTF-8 (requires building offset buffer)
  const { valueOffsets, utf8Data } = buildUtf8Buffers(textStorage, indices);
  return arrow.makeData({
    type: new arrow.Utf8(),
    data: utf8Data,
    valueOffsets,
    // ...
  });
}
```

### Buffer Concatenation for Chained Buffers

When SpanBuffer chains need concatenation (buffer.next), use this helper:

```typescript
/**
 * Concatenate multiple TypedArrays into a single array (zero-copy until final result)
 *
 * This is needed when converting chained SpanBuffers to a single Arrow column.
 * Uses typed array .set() method which is optimized by VMs.
 */
function concatenateTypedArrays<T extends TypedArray>(arrays: T[]): T {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new (arrays[0].constructor as any)(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

// Usage with buffer chain
const buffers: SpanBuffer[] = collectBufferChain(rootBuffer);
const timestampArrays = buffers.map((buf) => buf.timestamps.subarray(0, buf.writeIndex));
const allTimestamps = concatenateTypedArrays(timestampArrays);

return arrow.makeData({
  type: new arrow.TimestampNanosecond(),
  offset: 0,
  length: allTimestamps.length,
  data: allTimestamps, // Single concatenated array
  // ...
});
```

**Note**: While concatenation requires one copy, it's still more efficient than the builder pattern which copies during
every append() call.

### Null Bitmap Construction

Null bitmaps must be constructed from SpanBuffer null tracking:

```typescript
/**
 * Build null bitmap from buffer's null tracking
 * Arrow format: bit-packed, 1 = valid, 0 = null
 */
function buildNullBitmap(
  buffer: SpanBuffer,
  columnName: `attr_${string}`
): { nullBitmap: Uint8Array | null; nullCount: number } {
  const nullBitmap = buffer.nullBitmaps[columnName];
  if (!nullBitmap) {
    return { nullBitmap: null, nullCount: 0 }; // All valid
  }

  const validBits = nullBitmap.subarray(0, Math.ceil(buffer.writeIndex / 8));

  // Count nulls (0 bits)
  let nullCount = 0;
  for (let i = 0; i < buffer.writeIndex; i++) {
    const byteIndex = Math.floor(i / 8);
    const bitOffset = i % 8;
    const isValid = (validBits[byteIndex] & (1 << bitOffset)) !== 0;
    if (!isValid) nullCount++;
  }

  return { nullBitmap: validBits, nullCount };
}
```

## Design Philosophy

**Key Insight**: The Arrow table structure must balance query performance with data completeness. Every entry in the
system becomes a row in the final table, enabling rich analytical queries while maintaining efficient storage.

**Core Principles**:

- **Flat structure**: All data flattened to a single table for maximum query flexibility
- **Nullable columns**: Sparse data handled efficiently with null values
- **Dictionary encoding**: Repeated strings stored efficiently
- **Type optimization**: Appropriate data types for storage and performance
- **Zero-copy conversion**: Direct TypedArray references without intermediate copies

## Column Schema

### Core System Columns (Always Present)

| Column Name        | Type                 | Description                                                                                                   | Example Values                                                                                                 |
| ------------------ | -------------------- | ------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `timestamp`        | `timestamp[ns]`      | When event occurred (nanoseconds, BigInt64 storage)                                                           | `2024-01-01T10:00:00.000000123Z`                                                                               |
| `trace_id`         | `dictionary<string>` | Request correlation (TraceId branded string, W3C format)                                                      | `'4bf92f3577b34da6a3ce929d0e0e4736'`, `'req-abc123'`                                                           |
| `thread_id`        | `uint64`             | Thread/worker identifier (crypto-secure random, once/thread)                                                  | `0x1a2b3c4d5e6f7890`                                                                                           |
| `span_id`          | `uint32`             | Unit of work within thread (incrementing counter)                                                             | `1`, `2`, `42`                                                                                                 |
| `parent_thread_id` | `uint64` (nullable)  | Parent span's thread (null for root spans)                                                                    | `0x1a2b3c4d5e6f7890` or `null`                                                                                 |
| `parent_span_id`   | `uint32` (nullable)  | Parent span's ID (null for root spans)                                                                        | `1`, `2` or `null`                                                                                             |
| `entry_type`       | `dictionary<string>` | Log entry type (see [Entry Types](#entry-type-system))                                                        | `'span-start'`, `'span-ok'`, `'op-invocations'`, `'buffer-writes'`, etc.                                       |
| `package_name`     | `dictionary<string>` | npm package name (see [Module Identification](#module-identification) section)                                | `'@smoothbricks/lmao'`, `'@mycompany/user-service'`                                                            |
| `package_path`     | `dictionary<string>` | Path within package, relative to package.json (see [Module Identification](#module-identification) section)   | `'src/services/user.ts'`, `'lib/handlers/auth.ts'`                                                             |
| `message`          | `dictionary<string>` | Span name, log message template, exception message, result message, OR flag name (see Message Column section) | `'create-user'`, `'User ${userId} created'`, `'Processing ${count} items'`, `'TypeError: x is not a function'` |

### Lazy System Columns (Sparse/Nullable)

These system columns are allocated lazily (only when first written to) because they are used by specific entry types
rather than every row.

| Column Name    | Type     | Storage          | Description                                                       | Example Values                              |
| -------------- | -------- | ---------------- | ----------------------------------------------------------------- | ------------------------------------------- |
| `uint64_value` | `uint64` | `BigUint64Array` | Metric values (counts, durations) and user `.uint64()` API values | `1523`, `68535000000`, `0x1a2b3c4d5e6f7890` |

**Why `uint64_value` is a Lazy System Column**:

1. **Metrics as structured logs**: Same Arrow table, same flush path, same query tools - no separate metrics
   infrastructure
2. **Shared by metrics and users**: Both `op-invocations` (count) and user code
   (`log.info('Done').uint64(bytesProcessed)`) use the same column
3. **Sparse data**: Most trace rows don't have a uint64 value - only metrics rows and entries where `.uint64()` is
   called
4. **Storage efficiency**: `BigUint64Array` with null bitmap means zero overhead for rows that don't use it

**Note on Span Identification**:

- `trace_id`: Branded `TraceId` string, validated (non-empty, max 128 chars, ASCII). Shared by reference across all
  spans in a trace (zero-copy).
- `thread_id` + `span_id`: Extracted from `SpanIdentity` 25-byte ArrayBuffer during Arrow conversion.
- `parent_thread_id` + `parent_span_id`: Also from `SpanIdentity`, null for root spans (hasParent flag = 0).

## Module Identification

The `package_name` and `package_path` columns work together to identify the source location of each trace entry. This
two-column design provides significant benefits over a single combined path:

### Why `packageName` + `packagePath` (Not a Combined Path)

1. **Cross-repo log aggregation**: When logs from multiple repositories or services are combined (e.g., in a data
   warehouse), the package name provides a globally unique namespace. npm enforces package name uniqueness, so
   `@mycompany/user-service` is unambiguous across all repos. A git-relative path like `src/services/user.ts` would be
   ambiguous—many repos have files at that path.

2. **Prefix compression via dictionary encoding**: The package name is stored once in the dictionary and referenced by
   index. If a service logs 100 spans, the full package name string (e.g., `'@smoothbricks/lmao'`) is stored only once.
   Each row just stores a 4-byte index. This typically saves 20-50 bytes per row compared to storing full paths.

3. **Better queryability**: Filter by package without string parsing:

   ```sql
   -- Clean filter (uses dictionary index)
   WHERE package_name = '@smoothbricks/lmao'

   -- vs. string parsing (slower, error-prone)
   WHERE module LIKE '@smoothbricks/lmao/%'
   ```

4. **Source lookup path**: To find actual source code from a trace entry:
   - `package_name` → npm registry → git repo URL (via `repository` field in package.json)
   - Git repo → find package location within monorepo (search for package.json with matching name)
   - `package_path` → actual source file relative to that package.json

5. **TypeScript transformer compatibility**: The TypeScript transformer computes both pieces via `findNearestPackage()`,
   which returns both `packageName` and `packageDir`. Storing them separately avoids redundant string concatenation.

### Relation to Operations (Ops)

Module metadata comes from the op's bound module at runtime:

```typescript
const { op } = httpModule;

const GET = op(async ({ span, log, tag }, url: string) => {
  tag.method('GET');
  await span('fetch', fetchOp, url); // Span name comes from call site
});
```

- **`package_name`**: From the module the op is bound to (`httpModule`)
- **`package_path`**: From where the module was defined (TypeScript transformer)
- **Span names**: Come from `span('name', op, ...args)` call sites, stored in `message` column

### Why NOT Git-Based Identity

We considered using git repository info (git root path, git SHA for identity) but rejected it:

- **Not all code is in git repos**: Build artifacts, generated code, node_modules may not have git info
- **Monorepos have one git root but many packages**: The git path doesn't uniquely identify a package
- **Package name from `package.json` is the canonical npm identity**: Already defined, already unique
- **Git SHA is for version tracking**: Answers "which version of the code?" not "which module?"
  - We keep `gitSha` as a separate field for version tracking, not as part of module identity

### ModuleMetadata Interface

The `moduleMetadata` object passed to `createModuleContext()` contains:

```typescript
interface ModuleMetadata {
  gitSha: string; // Git commit SHA for version tracking (NOT identity)
  packageName: string; // npm package name from nearest package.json
  packagePath: string; // Path within package, relative to package.json
}
```

Example:

```typescript
moduleMetadata: {
  gitSha: 'abc123def456...',
  packageName: '@smoothbricks/lmao',
  packagePath: 'src/services/user.ts',
}
```

### ModuleContext Properties

The `ModuleContext` (created at module initialization) stores:

- `packageName: string` — npm package name from nearest package.json (e.g., `'@smoothbricks/lmao'`)
- `packagePath: string` — Path within package, including file extension (e.g., `'src/services/user.ts'`)
- `gitSha: string` — Git commit SHA for version tracking

Both `packageName` and `packagePath` values are interned at module creation time for efficient dictionary encoding.

## Span Definition

> **A span represents a unit of work within a single thread of execution.**

This definition is the foundation of LMAO's span identification design. Unlike OpenTelemetry which defines a span as
simply a "unit of work" with random 64-bit IDs, LMAO explicitly ties spans to their thread of execution. This enables:

- **Cheap span creation**: Just `span_id++` instead of random generation
- **Thread timeline visibility**: See how async concurrency interleaves requests
- **Cross-thread tracing**: Parent spans can be on different threads

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

In distributed tracing, the same `trace_id` can exist across multiple machines/workers. If each uses a simple
incrementing counter (`span_id++`), collisions occur:

```
Machine A: trace_id="req-123", span_id=1, 2, 3...
Machine B: trace_id="req-123", span_id=1, 2, 3... ← collision!
```

This happens in several scenarios:

- **Worker threads**: `pmap()` distributing work across threads
- **Distributed services**: Same trace spanning multiple machines
- **Serverless**: Multiple Lambda invocations for the same request

### Chosen Approach: SpanIdentity + TraceId

LMAO uses a combination of `SpanIdentity` (25-byte ArrayBuffer) and `TraceId` (branded string) for span identification.
In the Arrow output, these are expanded to separate columns for query flexibility:

| Column             | Type                 | Description                                           |
| ------------------ | -------------------- | ----------------------------------------------------- |
| `trace_id`         | `dictionary<string>` | Request correlation (W3C format, shared by reference) |
| `thread_id`        | `uint64`             | Thread/worker identifier (crypto-secure, once/thread) |
| `span_id`          | `uint32`             | Unit of work within thread (incrementing counter)     |
| `parent_thread_id` | `uint64` (nullable)  | Parent span's thread                                  |
| `parent_span_id`   | `uint32` (nullable)  | Parent span's ID                                      |

### SpanIdentity Memory Layout

In memory, span identification is packed into a 25-byte `SpanIdentity` ArrayBuffer:

```
SpanIdentity (25 bytes):
┌──────────────────────────────────────────────────────────────────────────┐
│ Byte 0:      flags           (bit 0 = hasParent)                         │
│ Bytes 1-8:   threadId        (8 bytes, crypto-secure random)             │
│ Bytes 9-12:  spanId          (4 bytes, thread-local counter)             │
│ Bytes 13-20: parentThreadId  (8 bytes, zeroed if root)                   │
│ Bytes 21-24: parentSpanId    (4 bytes, zeroed if root)                   │
└──────────────────────────────────────────────────────────────────────────┘
```

### TraceId: Branded String

`TraceId` is a branded string type that is:

- **Shared by reference**: All spans in a trace reference the same string (zero-copy)
- **Validated**: Non-empty, max 128 characters, ASCII only
- **W3C Compatible**: `generateTraceId()` produces 32 lowercase hex characters

```typescript
type TraceId = string & { readonly __brand: 'TraceId' };

// All spans share the same reference
rootSpan.traceId === childSpan.traceId; // true (same string reference)
```

### Global Uniqueness

**Within a trace**: `(thread_id, span_id)` is unique **Globally**: `(trace_id, thread_id, span_id)` is globally unique

- `trace_id` correlates spans across services (W3C Trace Context compatible)
- `thread_id` provides cross-process uniqueness (crypto-secure 64-bit random)
- `span_id` provides within-thread ordering (32-bit counter)

### Parent Reference

To find a parent span, you need:

- Same `trace_id` (parent is always in same trace)
- Match `thread_id = parent_thread_id` AND `span_id = parent_span_id`

SpanBuffer provides convenience methods:

```typescript
// These check BOTH traceId equality AND SpanIdentity relationship
spanA.isParentOf(spanB); // true if spanA is spanB's parent
spanB.isChildOf(spanA); // true if spanB is spanA's child
```

### Why This Design

**Performance (Hot Path)**:

- `span_id` is just `i++` - no random generation, no BigInt operations per span
- Thread ID generated once at worker startup using crypto-secure random (cold path)
- TraceId shared by reference - no string copies per span
- SpanIdentity comparison uses simple byte loop (6x faster than DataView)

**Collision Resistance**:

- Crypto-secure 64-bit thread ID provides strong collision resistance
- Birthday paradox: ~4 billion processes before 50% collision probability
- Combined with 32-bit local counter: effectively unlimited spans per thread

**Query Flexibility**:

- Separate columns allow efficient querying by `thread_id` alone
- No struct unpacking needed for common queries
- Direct column filtering in WHERE clauses

**Cross-Thread Parent Support**:

- Child span on thread B can reference parent span on thread A
- Natural for `pmap()` and worker scenarios
- `parent_thread_id` + `parent_span_id` identifies the parent

**JavaScript Compatibility**:

- `thread_id` (64-bit): Stored as raw bytes in SpanIdentity, converted to BigUint64 for Arrow
- `span_id` (32-bit): Fits in `number`, used per span (no BigInt in hot path)
- `trace_id`: Plain string reference (branded for type safety)

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
- **Thread timeline visibility**: See exactly how async concurrency interleaves different traces on a thread

### Collision Math

For the 64-bit thread ID (birthday paradox):

- 2^32 (~4 billion) threads → 50% collision probability
- 2^20 (~1 million) threads → 0.00003% collision probability
- In practice, collision is negligible for any realistic deployment

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

-- Join parent-child relationships
SELECT child.*, parent.message as parent_message
FROM spans child
LEFT JOIN spans parent ON
  child.trace_id = parent.trace_id AND
  child.parent_thread_id = parent.thread_id AND
  child.parent_span_id = parent.span_id
WHERE child.trace_id = 'req-123';

-- Thread timeline: see how async concurrency interleaves requests
SELECT trace_id, span_id, timestamp
FROM spans
WHERE thread_id = 0x1a2b3c4d5e6f7890
ORDER BY span_id;
-- Result shows interleaved request handling:
-- trace_id: req-1, span_id: 1    (started req-1)
-- trace_id: req-1, span_id: 2    (child span)
-- trace_id: req-2, span_id: 3    (started req-2 while req-1 async waiting)
-- trace_id: req-1, span_id: 4    (back to req-1)
-- trace_id: req-2, span_id: 5    (req-2 continues)
```

### SpanIdentity to Arrow Conversion

During Arrow conversion, the `SpanIdentity` 25-byte ArrayBuffer is expanded to separate columns:

```typescript
function convertSpanIdentityToArrowColumns(spanId: SpanIdentity, rowCount: number): ArrowColumns {
  // Thread ID: extract 8 bytes, convert to BigUint64
  const threadIdBytes = spanId.threadId; // Uint8Array view of bytes 1-8
  const threadIdView = new DataView(threadIdBytes.buffer, threadIdBytes.byteOffset, 8);
  const threadIdValue = threadIdView.getBigUint64(0, true); // little-endian
  const threadIdData = new BigUint64Array(rowCount).fill(threadIdValue);

  // Span ID: direct 32-bit value
  const spanIdData = new Uint32Array(rowCount).fill(spanId.spanId);

  // Parent columns (nullable based on hasParent flag)
  if (spanId.hasParent) {
    const parentThreadIdBytes = spanId.parentThreadId;
    const parentThreadIdView = new DataView(parentThreadIdBytes.buffer, parentThreadIdBytes.byteOffset, 8);
    const parentThreadIdValue = parentThreadIdView.getBigUint64(0, true);
    const parentThreadIdData = new BigUint64Array(rowCount).fill(parentThreadIdValue);
    const parentSpanIdData = new Uint32Array(rowCount).fill(spanId.parentSpanId);

    return {
      thread_id: { data: threadIdData, nullBitmap: null },
      span_id: { data: spanIdData, nullBitmap: null },
      parent_thread_id: { data: parentThreadIdData, nullBitmap: null },
      parent_span_id: { data: parentSpanIdData, nullBitmap: null },
    };
  } else {
    // Root span: parent columns are all null
    const nullBitmap = new Uint8Array(Math.ceil(rowCount / 8)); // All zeros = all null
    return {
      thread_id: { data: threadIdData, nullBitmap: null },
      span_id: { data: spanIdData, nullBitmap: null },
      parent_thread_id: { data: new BigUint64Array(rowCount), nullBitmap, nullCount: rowCount },
      parent_span_id: { data: new Uint32Array(rowCount), nullBitmap, nullCount: rowCount },
    };
  }
}
```

### Library-Specific Attribute Columns (Sparse/Nullable)

| Column Name       | Type                 | Description                               | Example Values                                   |
| ----------------- | -------------------- | ----------------------------------------- | ------------------------------------------------ |
| `http_status`     | `uint16`             | HTTP status code (nullable)               | `200`, `404`, `500` or `null`                    |
| `http_method`     | `dictionary<string>` | HTTP method (nullable)                    | `'GET'`, `'POST'`, `'PUT'` or `null`             |
| `http_url`        | `string`             | Masked URL (nullable)                     | `'https://api.*****.com/users'` or `null`        |
| `http_duration`   | `float32`            | HTTP request duration ms (nullable)       | `125.5` or `null`                                |
| `db_query`        | `string`             | Masked SQL query (nullable)               | `'SELECT * FROM users WHERE id = ?'` or `null`   |
| `db_duration`     | `float32`            | Query duration ms (nullable)              | `12.3` or `null`                                 |
| `db_rows`         | `uint32`             | Rows affected/returned (nullable)         | `1`, `0`, `1000` or `null`                       |
| `db_table`        | `dictionary<string>` | Table name (nullable)                     | `'users'`, `'orders'` or `null`                  |
| `user_id`         | `binary[8]`          | Hashed user ID (nullable)                 | `0x8a7b6c5d...` or `null`                        |
| `business_metric` | `float64`            | Custom metric value (nullable)            | `42.7`, `1.0` or `null`                          |
| `ff_value`        | `dictionary<string>` | Feature flag value (nullable, S.category) | `'true'`, `'false'`, `'blue'`, `'100'` or `null` |

**Note**: Feature flag names are stored in the unified `message` column for `ff-access` and `ff-usage` entries. The
`ff_value` column uses `S.category()` (dictionary encoding) because flag values repeat frequently (e.g., `true`/`false`,
`'blue'`/`'green'`/`'red'`, etc.).

## Complete Trace Example: User Registration Flow

This example shows a complete user registration request with multiple spans, HTTP calls, database operations, and
console.log compatibility traces.

**Note on Span IDs**: For readability, this example shows only the `span_id` component (1, 2, 3...). In the actual Arrow
output, span identification uses separate columns: `thread_id` (uint64), `span_id` (uint32), `parent_thread_id` (uint64,
nullable), and `parent_span_id` (uint32, nullable). For single-process traces, all spans share the same `thread_id`:

```
-- Example table representation:
thread_id: 0x1a2b3c4d5e6f7890, span_id: 1, parent_thread_id: null, parent_span_id: null
thread_id: 0x1a2b3c4d5e6f7890, span_id: 2, parent_thread_id: 0x1a2b3c4d5e6f7890, parent_span_id: 1
```

| trace_id     | span_id | parent_span_id | timestamp                  | entry_type   | package_name              | package_path                   | message                                         | http_status | http_method | http_url                               | http_duration | db_query                                                                | db_duration | db_rows | db_table | user_id         | business_metric | ff_value |
| ------------ | ------- | -------------- | -------------------------- | ------------ | ------------------------- | ------------------------------ | ----------------------------------------------- | ----------- | ----------- | -------------------------------------- | ------------- | ----------------------------------------------------------------------- | ----------- | ------- | -------- | --------------- | --------------- | -------- |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.000Z` | `span-start` | `@mycompany/user-service` | `src/controllers/user.ts`      | `register-user`                                 | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.002Z` | `ff-access`  | `@mycompany/user-service` | `src/controllers/user.ts`      | `advancedValidation`                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | `true`   |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.005Z` | `info`       | `@mycompany/user-service` | `src/controllers/user.ts`      | `Starting registration for ${userId}`           | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.010Z` | `span-start` | `@mycompany/user-service` | `src/services/validation.ts`   | `validate-email`                                | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.015Z` | `tag`        | `@mycompany/user-service` | `src/services/validation.ts`   | `validate-email`                                | 200         | `POST`      | `https://api.*****.com/validate-email` | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.045Z` | `tag`        | `@mycompany/user-service` | `src/services/validation.ts`   | `validate-email`                                | 200         | `POST`      | `https://api.*****.com/validate-email` | 30.2          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.046Z` | `span-ok`    | `@mycompany/user-service` | `src/services/validation.ts`   | `validate-email`                                | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.050Z` | `span-start` | `@mycompany/user-service` | `src/repositories/user.ts`     | `check-user-exists`                             | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.052Z` | `tag`        | `@mycompany/user-service` | `src/repositories/user.ts`     | `check-user-exists`                             | null        | null        | null                                   | null          | `SELECT id FROM users WHERE email = ?`                                  | null        | null    | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.067Z` | `tag`        | `@mycompany/user-service` | `src/repositories/user.ts`     | `check-user-exists`                             | null        | null        | null                                   | null          | `SELECT id FROM users WHERE email = ?`                                  | 15.3        | 0       | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.068Z` | `debug`      | `@mycompany/user-service` | `src/repositories/user.ts`     | `User does not exist, proceeding with creation` | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.069Z` | `span-ok`    | `@mycompany/user-service` | `src/repositories/user.ts`     | `check-user-exists`                             | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.070Z` | `span-start` | `@mycompany/user-service` | `src/repositories/user.ts`     | `create-user`                                   | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.072Z` | `tag`        | `@mycompany/user-service` | `src/repositories/user.ts`     | `create-user`                                   | null        | null        | null                                   | null          | `INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)` | null        | null    | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.095Z` | `tag`        | `@mycompany/user-service` | `src/repositories/user.ts`     | `create-user`                                   | null        | null        | null                                   | null          | `INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)` | 23.1        | 1       | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.096Z` | `span-ok`    | `@mycompany/user-service` | `src/repositories/user.ts`     | `create-user`                                   | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.100Z` | `span-start` | `@mycompany/user-service` | `src/services/notification.ts` | `send-welcome-email`                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.105Z` | `tag`        | `@mycompany/user-service` | `src/services/notification.ts` | `send-welcome-email`                            | 202         | `POST`      | `https://email.*****.com/send`         | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.245Z` | `tag`        | `@mycompany/user-service` | `src/services/notification.ts` | `send-welcome-email`                            | 202         | `POST`      | `https://email.*****.com/send`         | 140.3         | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.246Z` | `span-ok`    | `@mycompany/user-service` | `src/services/notification.ts` | `send-welcome-email`                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.250Z` | `tag`        | `@mycompany/user-service` | `src/controllers/user.ts`      | `register-user`                                 | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | 1.0             | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.251Z` | `info`       | `@mycompany/user-service` | `src/controllers/user.ts`      | `Registration completed for ${userId}`          | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.252Z` | `span-ok`    | `@mycompany/user-service` | `src/controllers/user.ts`      | `register-user`                                 | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |

## The `message` System Column

### Unified Purpose

The `message` column serves different purposes based on entry type:

- **For span entries** (`span-start`, `span-ok`, `span-err`): Contains the **span name**
- **For exception entries** (`span-exception`): Contains the **exception message**
- **For log entries** (`info`, `debug`, `warn`, `error`): Contains the **message template**
- **For feature flag entries** (`ff-access`, `ff-usage`): Contains the **flag name**

| Entry Type                          | `message` Contains                                           | `uint64_value` Contains     |
| ----------------------------------- | ------------------------------------------------------------ | --------------------------- |
| `span-start`, `span-ok`, `span-err` | Span name (e.g., `'create-user'`)                            | User `.uint64()` value      |
| `span-exception`                    | Exception message (e.g., `'TypeError: x is not a function'`) | -                           |
| `info`, `debug`, `warn`, `error`    | Log message template (e.g., `'User ${userId} created'`)      | User `.uint64()` value      |
| `ff-access`, `ff-usage`             | Flag name (e.g., `'advancedValidation'`, `'darkMode'`)       | -                           |
| `period-start`                      | -                                                            | Period start timestamp (ns) |
| `op-*` (all 8 op metric types)      | Op name (e.g., `'GET'`, `'createUser'`)                      | Metric value (count or ns)  |
| `buffer-*` (all 4 buffer types)     | -                                                            | Metric value (count)        |

### Format String Pattern (CRITICAL)

**Log messages use FORMAT STRINGS, not interpolated strings.**

When you write:

```typescript
// Inside an op function:
const myOp = op(async ({ log }) => {
  log.info('User ${userId} created with ${itemCount} items').userId(123).itemCount(5);
});
```

The system stores:

| Column           | Value                                              |
| ---------------- | -------------------------------------------------- |
| `message`        | `'User ${userId} created with ${itemCount} items'` |
| `attr_userId`    | `123`                                              |
| `attr_itemCount` | `5`                                                |

**The message is NOT interpolated.** The template string `'User ${userId} created...'` is stored verbatim in the
`message` column, while the actual values (`123`, `5`) are stored in their respective typed attribute columns.

### Why This Design?

**1. Efficient Storage via String Interning (S.category)**

```typescript
// In systemSchema:
message: S.category(),  // Span name, log message template, exception message, OR flag name
```

The `message` column uses `S.category()` type, which means:

- Templates are **string-interned** - each unique template stored once
- Repeated log messages (even with different values) share the same interned template
- Much more efficient than storing `"User 123 created"`, `"User 456 created"`, `"User 789 created"` as separate strings

**2. Better Analytics Through Template Grouping**

Because templates are stored separately from values, you can:

```sql
-- Find all occurrences of a specific log pattern
SELECT * FROM traces WHERE message = 'User ${userId} created with ${itemCount} items';

-- Group by log template to find most frequent messages
SELECT message, count(*) as occurrences
FROM traces
WHERE entry_type IN ('info', 'debug', 'warn', 'error')
GROUP BY message
ORDER BY occurrences DESC;

-- Analyze specific template with different values
SELECT attr_userId, attr_itemCount, timestamp
FROM traces
WHERE message = 'User ${userId} created with ${itemCount} items'
ORDER BY timestamp;
```

**3. Simpler Schema - One Column for Multiple Purposes**

Instead of separate `span_name`, `message`, and `ffName` columns (most always null), we have:

- Single `message` column that's always populated for relevant entry types
- Reduces schema complexity
- Better column utilization (less sparsity)
- Consistent pattern across all entry types

### Example Data

| entry_type   | message                                            | attr_userId | attr_itemCount |
| ------------ | -------------------------------------------------- | ----------- | -------------- |
| `span-start` | `'create-user'`                                    | `123`       | `null`         |
| `info`       | `'User ${userId} created with ${itemCount} items'` | `123`       | `5`            |
| `debug`      | `'Processing batch for ${userId}'`                 | `123`       | `null`         |
| `span-ok`    | `'create-user'`                                    | `123`       | `null`         |

### Contrast with Traditional Logging

**Traditional (interpolated strings):**

```typescript
console.log(`User ${userId} created with ${itemCount} items`);
// Stores: "User 123 created with 5 items" - unique string, no structure
```

**LMAO (format strings):**

```typescript
// Inside an op function:
log.info('User ${userId} created with ${itemCount} items').userId(123).itemCount(5);
// Stores: template in message, values in typed columns - structured, queryable
```

## Key Patterns in the Data

### 1. Span Hierarchy & Lifecycle

- **Root span** (span_id=1): `register-user` with no parent
- **Child spans** (span_id=2,3,4,5): All have parent_span_id=1
- **Span lifecycle**: `span-start` → entries → `span-ok`/`span-err`/`span-exception`
- **Success/failure tracking**: `span-ok` vs `span-err` vs `span-exception` captures span outcome without extra columns

### 2. Structured Logging via Entry Type Enum

- **Log levels with structure**: `log.info('Template ${var}').var(value)` → `entry_type='info'` with typed attributes
- **Template storage**: Log message TEMPLATES stored in unified `message` column (NOT interpolated strings)
- **Values in attribute columns**: Actual values stored separately in `attr_*` columns for type safety and queryability
- **Optional attributes**: Structured data can accompany log messages
- **Gradual migration**: Familiar log levels but with structured data instead of string concatenation

### 3. Entry Type System

The `entry_type` column uses a dictionary-encoded enum that covers all possible trace events. For complete definitions
and low-level API details, see **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**.

**Entry Types** (23 total):

- **Span lifecycle** (4): `span-start`, `span-ok`, `span-err`, `span-exception`
- **Log levels** (4): `info`, `debug`, `warn`, `error`
- **Feature flags** (2): `ff-access`, `ff-usage`
- **Metrics - Period** (1): `period-start`
- **Metrics - Op** (8): `op-invocations`, `op-errors`, `op-exceptions`, `op-duration-total`, `op-duration-ok`,
  `op-duration-err`, `op-duration-min`, `op-duration-max`
- **Metrics - Buffer** (4): `buffer-writes`, `buffer-overflow-writes`, `buffer-created`, `buffer-overflows`

**Key Benefits**:

- **Dictionary encoding**: Entry type strings stored once, referenced by index (1 byte per row)
- **Self-documenting**: `op-invocations` is clearer than generic `metric` + `name` columns
- **Unified system**: All trace events (including metrics) use the same table and flush path

### 4. Metrics as Structured Logs

Metrics are emitted during flush cycles as structured log rows. This design avoids a separate metrics infrastructure -
same Arrow table, same query tools, same export path.

**Example Metrics Rows** (all from same flush have identical `timestamp`):

| timestamp | thread_id | package_name | entry_type        | message | uint64_value |
| --------- | --------- | ------------ | ----------------- | ------- | ------------ |
| 1000      | 1         | @myco/http   | period-start      |         | 0            |
| 1000      | 1         | @myco/http   | op-invocations    | GET     | 1523         |
| 1000      | 1         | @myco/http   | op-errors         | GET     | 8            |
| 1000      | 1         | @myco/http   | op-exceptions     | GET     | 4            |
| 1000      | 1         | @myco/http   | op-duration-total | GET     | 68535000000  |
| 1000      | 1         | @myco/http   | op-duration-ok    | GET     | 65000000000  |
| 1000      | 1         | @myco/http   | op-duration-err   | GET     | 3535000000   |
| 1000      | 1         | @myco/http   | op-duration-min   | GET     | 2100000      |
| 1000      | 1         | @myco/http   | op-duration-max   | GET     | 1203500000   |
| 1000      | 1         | @myco/http   | buffer-writes     |         | 50000        |
| 1000      | 1         | @myco/http   | buffer-created    |         | 47           |

**Why This Design**:

1. **Same infrastructure**: No separate metrics store, exporter, or query language
2. **Specific entry types**: `op-invocations` is self-documenting (vs generic `metric` + `name` columns)
3. **Duration by outcome**: `op-duration-ok` vs `op-duration-err` reveals if errors are fast-fails or slow timeouts
4. **Period boundaries**: `period-start` with nanosecond timestamp enables calculating period duration

**Query Example**:

```sql
-- Op performance summary for last hour
SELECT
  message as op_name,
  sum(CASE WHEN entry_type = 'op-invocations' THEN uint64_value END) as invocations,
  sum(CASE WHEN entry_type = 'op-errors' THEN uint64_value END) as errors,
  sum(CASE WHEN entry_type = 'op-duration-total' THEN uint64_value END) / 1e9 as total_seconds,
  max(CASE WHEN entry_type = 'op-duration-max' THEN uint64_value END) / 1e6 as max_ms
FROM traces
WHERE entry_type LIKE 'op-%'
  AND timestamp > now() - interval 1 hour
GROUP BY message
ORDER BY invocations DESC;
```

### 5. Library Integration

- **HTTP entries**: Multiple rows for single request (start tag, end tag with duration)
- **Database entries**: Query logged, then duration and row count added
- **Attribute isolation**: Each library's attributes are cleanly separated in dedicated columns

### 6. Feature Flag Integration via Entry Type Enum

- **Flag evaluation**: `ff-access` entry types capture when flags are checked
- **Usage tracking**: `ff-usage` entry types capture when flag-gated features are used
- **Context via attributes**: Flag evaluation context stored in regular attribute columns (user_id, user_plan, etc.)
- **Type safety**: Feature flag context uses same typed attribute system as other entry types
- **Query efficiency**: No JSON parsing needed - direct column access for flag context

### 7. Sparse Data Efficiency

- **Core columns always present**: 9 system columns in every row (8 eager + 1 lazy `uint64_value` when used)
- **Attribute columns sparse**: Library-specific columns mostly null
- **Efficient storage**: Arrow's null bitmap handles sparsity with minimal overhead
- **Targeted information**: Each row contains only relevant attributes

## ClickHouse Query Examples

### Request Performance Analysis

```sql
-- Average request duration by endpoint
SELECT
  trace_id,
  max(timestamp) - min(timestamp) as total_duration_ms,
  count(*) as total_entries,
  count(CASE WHEN entry_type = 'span-start' THEN 1 END) as span_count
FROM traces
WHERE package_name = '@mycompany/user-service'
  AND package_path = 'src/controllers/user.ts'
  AND message = 'register-user'
GROUP BY trace_id
ORDER BY total_duration_ms DESC
LIMIT 10;
```

### Database Performance Monitoring

```sql
-- Slow database queries
SELECT
  db_table,
  db_query,
  avg(db_duration) as avg_duration,
  max(db_duration) as max_duration,
  count(*) as query_count
FROM traces
WHERE db_duration IS NOT NULL
  AND db_duration > 100  -- Queries slower than 100ms
GROUP BY db_table, db_query
ORDER BY avg_duration DESC;
```

### HTTP Error Analysis

```sql
-- HTTP error rates by service
SELECT
  package_name,
  package_path,
  message as span_name,
  count(*) as total_requests,
  count(CASE WHEN http_status >= 400 THEN 1 END) as error_count,
  (error_count / total_requests) * 100 as error_rate_percent
FROM traces
WHERE http_status IS NOT NULL
GROUP BY package_name, package_path, message
HAVING total_requests > 100  -- Only services with significant traffic
ORDER BY error_rate_percent DESC;
```

### User Journey Analysis

```sql
-- Trace user journey through registration flow
SELECT
  span_id,
  parent_span_id,
  package_name,
  package_path,
  message,
  entry_type,
  timestamp,
  CASE
    WHEN entry_type IN ('info', 'debug', 'warn', 'error') THEN message  -- Log template
    WHEN http_url IS NOT NULL THEN concat('HTTP ', http_method, ' ', http_url)
    WHEN db_query IS NOT NULL THEN concat('DB: ', db_table)
    ELSE message  -- Span name
  END as description
FROM traces
WHERE trace_id = 'req-abc123'
ORDER BY timestamp;
```

### Structured Logging Analysis

```sql
-- Analyze structured logging usage patterns
-- Note: message contains MESSAGE TEMPLATES, not interpolated strings
SELECT
  package_name,
  package_path,
  entry_type as log_level,
  count(*) as message_count,
  count(DISTINCT trace_id) as trace_count,
  count(DISTINCT message) as unique_templates,  -- Templates, not interpolated messages!
  count(CASE WHEN http_status IS NOT NULL OR user_id IS NOT NULL THEN 1 END) as structured_entries
FROM traces
WHERE entry_type IN ('info', 'debug', 'warn', 'error')
GROUP BY package_name, package_path, entry_type
ORDER BY message_count DESC;

-- Find most common log message templates
SELECT
  message as template,
  count(*) as occurrences,
  count(DISTINCT trace_id) as unique_traces
FROM traces
WHERE entry_type IN ('info', 'debug', 'warn', 'error')
GROUP BY message
ORDER BY occurrences DESC
LIMIT 20;
```

### Feature Flag Analysis

```sql
-- Feature flag usage and performance impact
-- Note: flag name is in the unified `message` column, value in `ff_value`
SELECT
  message as flag_name,
  ff_value,
  count(*) as access_count,
  count(DISTINCT trace_id) as trace_count,
  avg(CASE WHEN ff_value = 'true' THEN 1.0 ELSE 0.0 END) as enabled_ratio
FROM traces
WHERE entry_type = 'ff-access'
GROUP BY message, ff_value
ORDER BY access_count DESC;

-- Correlation between feature flags and request performance by user plan
WITH flag_traces AS (
  SELECT DISTINCT trace_id, message as flag_name, ff_value, user_id
  FROM traces
  WHERE entry_type = 'ff-access' AND message = 'advancedValidation'
),
user_plans AS (
  SELECT DISTINCT trace_id, user_plan
  FROM traces
  WHERE user_plan IS NOT NULL
),
trace_performance AS (
  SELECT
    trace_id,
    max(timestamp) - min(timestamp) as duration_ms
  FROM traces
  GROUP BY trace_id
)
SELECT
  ft.ff_value,
  up.user_plan,
  avg(tp.duration_ms) as avg_duration,
  count(*) as request_count
FROM flag_traces ft
JOIN user_plans up ON ft.trace_id = up.trace_id
JOIN trace_performance tp ON ft.trace_id = tp.trace_id
GROUP BY ft.ff_value, up.user_plan;
```

### Op and Buffer Metrics Analysis

```sql
-- Op performance dashboard by package
SELECT
  package_name,
  message as op_name,
  sum(CASE WHEN entry_type = 'op-invocations' THEN uint64_value END) as invocations,
  sum(CASE WHEN entry_type = 'op-errors' THEN uint64_value END) as errors,
  sum(CASE WHEN entry_type = 'op-exceptions' THEN uint64_value END) as exceptions,
  sum(CASE WHEN entry_type = 'op-duration-total' THEN uint64_value END) / 1e9 as total_sec,
  sum(CASE WHEN entry_type = 'op-duration-ok' THEN uint64_value END) / 1e9 as ok_sec,
  sum(CASE WHEN entry_type = 'op-duration-err' THEN uint64_value END) / 1e9 as err_sec,
  max(CASE WHEN entry_type = 'op-duration-max' THEN uint64_value END) / 1e6 as max_ms,
  min(CASE WHEN entry_type = 'op-duration-min' THEN uint64_value END) / 1e6 as min_ms
FROM traces
WHERE entry_type LIKE 'op-%'
  AND timestamp > now() - interval 1 hour
GROUP BY package_name, message
ORDER BY invocations DESC;

-- Error rate by op (errors are expected, exceptions are bugs)
SELECT
  message as op_name,
  sum(CASE WHEN entry_type = 'op-invocations' THEN uint64_value END) as total,
  sum(CASE WHEN entry_type = 'op-errors' THEN uint64_value END) as errors,
  sum(CASE WHEN entry_type = 'op-exceptions' THEN uint64_value END) as exceptions,
  errors * 100.0 / total as error_rate_pct,
  exceptions * 100.0 / total as exception_rate_pct
FROM traces
WHERE entry_type IN ('op-invocations', 'op-errors', 'op-exceptions')
GROUP BY message
HAVING total > 100
ORDER BY exception_rate_pct DESC;

-- Latency analysis: are errors fast-fails or slow timeouts?
SELECT
  message as op_name,
  sum(CASE WHEN entry_type = 'op-duration-ok' THEN uint64_value END) /
    nullif(sum(CASE WHEN entry_type = 'op-invocations' THEN uint64_value END) -
           sum(CASE WHEN entry_type = 'op-errors' THEN uint64_value END) -
           sum(CASE WHEN entry_type = 'op-exceptions' THEN uint64_value END), 0) / 1e6 as avg_ok_ms,
  sum(CASE WHEN entry_type = 'op-duration-err' THEN uint64_value END) /
    nullif(sum(CASE WHEN entry_type = 'op-errors' THEN uint64_value END), 0) / 1e6 as avg_err_ms,
  CASE
    WHEN avg_err_ms < avg_ok_ms * 0.5 THEN 'fast-fail'
    WHEN avg_err_ms > avg_ok_ms * 2 THEN 'slow-timeout'
    ELSE 'similar'
  END as error_pattern
FROM traces
WHERE entry_type LIKE 'op-%'
GROUP BY message
ORDER BY avg_err_ms DESC;

-- Buffer health metrics
SELECT
  package_name,
  sum(CASE WHEN entry_type = 'buffer-writes' THEN uint64_value END) as total_writes,
  sum(CASE WHEN entry_type = 'buffer-overflow-writes' THEN uint64_value END) as overflow_writes,
  sum(CASE WHEN entry_type = 'buffer-created' THEN uint64_value END) as buffers_created,
  sum(CASE WHEN entry_type = 'buffer-overflows' THEN uint64_value END) as overflow_events,
  overflow_writes * 100.0 / total_writes as overflow_pct
FROM traces
WHERE entry_type LIKE 'buffer-%'
GROUP BY package_name
ORDER BY overflow_pct DESC;
```

## Performance Characteristics

### Timestamp Precision (High-Resolution Anchored Design)

The timestamp system uses a high-precision anchored design that captures a single time reference at trace root creation,
then uses high-resolution timers for all subsequent timestamps. See
[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md) for full implementation details.

**Core Design**:

- ONE `Date.now()` captured at trace root (TraceContext creation via `module.traceContext()`)
- ONE high-resolution timer captured at trace root (`performance.now()` or `process.hrtime.bigint()`)
- All subsequent timestamps: `anchorEpochMicros + (highResNow - anchorHighRes) * scale`

**Platform Implementations**:

- **Browser**: `performance.now()` deltas from anchor
  - ~5μs resolution for sub-millisecond precision
  - No `Date.now()` calls per span
  - Stored as `timestamp[ns]` (nanosecond) in Arrow
- **Node.js**: `process.hrtime.bigint()` deltas from anchor
  - Nanosecond precision available, stored directly
  - Full nanosecond precision preserved
  - Arrow type: `timestamp[ns]` (nanosecond)
- **Fallback**: `Date.now()` (millisecond precision)
  - Used when high-resolution timing is unavailable
  - Converted to nanoseconds for consistency
  - Arrow type: `timestamp[ns]` (nanosecond)

**Hot Path Storage**: All timestamps stored as `BigInt64Array` (nanoseconds since epoch) during logging. No object
allocations, no `Date.now()` calls per entry.

**Cold Path Conversion**: Converted to Arrow `TimestampNanosecond` type, compatible with ClickHouse `DateTime64(9)`.

**Precision Guarantees**:

- **Storage**: Nanoseconds stored as `BigInt64Array`
- **Safe Range**: BigInt64 can represent nanoseconds for ~292 years from epoch (signed 64-bit)
  - Well beyond any practical trace lifetime
  - **Conclusion**: Full nanosecond precision preserved without loss
- **Why Nanoseconds**:
  - Sub-microsecond precision for detailed performance analysis
  - Compatible with ClickHouse `DateTime64(9)`
  - Matches Arrow's native timestamp precision

**Date.now() Usage**:

- ❌ **NEVER** use `Date.now()` for trace timestamps (use anchored approach)
- ✅ **OK** to use `Date.now()` for scheduling, file naming, system metadata

**Benefits**:

- Zero allocations per timestamp
- Sub-millisecond precision enables detailed performance analysis
- All spans in trace share same anchor (comparable, consistent)
- DST/NTP safe - anchor per trace, traces are short-lived
- Safe numeric conversion until year 2255

### Storage Efficiency

- **Dictionary encoding**: Module names, span names, HTTP methods stored once
- **Null bitmap compression**: Sparse columns compressed efficiently
- **Type optimization**: Appropriate numeric types minimize storage
- **Parquet compression**: Additional compression when written to storage
- **Timestamp precision**: Platform-optimized precision minimizes storage while maximizing accuracy

### Query Performance

- **Columnar scanning**: Only relevant columns read for queries
- **Predicate pushdown**: Filters applied at storage level
- **Parallel processing**: ClickHouse can parallelize across columns
- **Index support**: Dictionary columns enable efficient filtering
- **Timestamp indexing**: Nanosecond/microsecond precision enables precise time-based queries

### Data Characteristics

- **High sparsity**: Most columns null for most rows (efficient with Arrow nulls)
- **Temporal ordering**: Timestamp allows efficient time-range queries
- **Hierarchical structure**: Span relationships enable trace reconstruction
- **Multi-dimensional**: Can slice by module, user, time, or entry type

## Arrow Conversion Interface

The conversion from LMAO's trace buffers to Arrow tables uses a **two-pass tree conversion** approach for optimal memory
efficiency and shared dictionaries.

**For complete details on the two-pass approach, dictionary building, and UTF-8 caching, see:**
**[Tree Walker and Arrow Conversion](./01k_tree_walker_and_arrow_conversion.md)**

### Key Concepts

- **Two-pass conversion**: Pass 1 builds dictionaries, Pass 2 creates RecordBatches
- **No intermediate buffer collection**: Walk tree twice instead of collecting into an array
- **Shared dictionaries**: All RecordBatches reference the same dictionary vectors
- **UTF-8 caching**: Encode once, copy on reuse for repeated strings
- **Depth-first pre-order traversal**: Parents before children (optimal for queries and compression)
- **Buffer overflow handling**: Multiple buffers with same spanId yielded contiguously

## Current Implementation Status

**✅ ZERO-COPY IMPLEMENTATION COMPLETE**: The implementation in `packages/lmao/src/lib/convertToArrow.ts` uses
`arrow.makeData()` exclusively.

**Implementation Details**:

1. ✅ `createZeroCopyData()` helper function - direct TypedArray wrapping with arrow.makeData()
2. ✅ `concatenateTypedArrays()` helper - efficient buffer chain concatenation
3. ✅ `concatenateNullBitmaps()` helper - null bitmap merging across buffers
4. ✅ Primitive columns (number, boolean, timestamp) - direct subarray() + concatenation
5. ✅ Dictionary columns (enum, category, text, trace_id, package_name, package_path, span_name, entry_type) - index
   arrays + dictionary vectors
6. ✅ System columns - all use zero-copy with direct TypedArray construction

**Key Implementation Patterns**:

### Primitive Columns (Float64, Uint8, etc.)

```typescript
// Collect value arrays from each buffer
const valueArrays = buffers.map((buf) => buf.column.subarray(0, buf.writeIndex));
// Concatenate (one copy, but still better than builder's per-value copy)
const allValues = concatenateTypedArrays(valueArrays);
const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);
// Zero-copy wrap
const data = arrow.makeData({ type, length, nullCount, data: allValues, nullBitmap });
vectors.push(arrow.makeVector(data));
```

### Dictionary Columns (enum, category, text)

```typescript
// Collect index arrays
const indexArrays = buffers.map((buf) => buf.column_values.subarray(0, buf.writeIndex));
const allIndices = concatenateTypedArrays(indexArrays);
const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);
// Create dictionary vector from interner
const dictVector = arrow.makeVector(interner.getStrings());
// Zero-copy wrap indices with dictionary
const data = arrow.makeData({
  type: new arrow.Dictionary(new arrow.Utf8(), indexType),
  length,
  nullCount,
  data: allIndices,
  nullBitmap,
  dictionary: dictVector,
});
vectors.push(arrow.makeVector(data));
```

**Performance Benefits**:

- **No per-value iteration**: Builder pattern called append() for every value
- **Minimal allocation**: One concatenation vs builder's internal buffer growth
- **Direct memory references**: TypedArrays passed directly to Arrow without copying
- **Dictionary efficiency**: Indices already in correct format, just wrap with dictionary

## Integration Points

This Arrow table structure integrates with:

- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Foundational entry type system
  and low-level logging API
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Direct conversion from SpanBuffer columns
  with `attr_` prefix stripping. Also documents:
  - **SpanIdentity**: 25-byte ArrayBuffer with comparison methods (`equals()`, `isParentOf()`, `isChildOf()`)
  - **TraceId**: Branded string type with validation and W3C format generation
  - **ColumnBuffer Extension**: Mechanism for lmao to inject span-specific code into arrow-builder generated classes
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Prefixed columns from different libraries
  cleanly separated
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Runtime generation of APIs that populate these
  Arrow columns
- **[Tree Walker and Arrow Conversion](./01k_tree_walker_and_arrow_conversion.md)**: Two-pass tree conversion with
  dictionary building, UTF-8 caching, and shared dictionaries across RecordBatches
- **Background Processing Pipeline** (future document): Batch conversion process from buffers to Arrow/Parquet
- **extern/arrow-js-ffi/src/vector.ts**: Reference implementation for zero-copy Arrow data construction

The flat table structure enables rich analytical queries while maintaining the performance benefits of columnar storage
and efficient null handling.
