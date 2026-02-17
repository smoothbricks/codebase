# Entry Types and Logging Primitives

## Design Philosophy

**Key Insight**: All trace events in the system are unified under a single entry type enum. Whether it's a span
lifecycle event, console.log call, structured data tag, or feature flag evaluation - everything becomes a row in the
trace with a specific entry type.

**Core Principles**:

- **Unified event model**: One enum covers all possible trace events
- **Zero ambiguity**: Each entry type has a precise, well-defined meaning
- **Performance first**: Entry types are enum-encoded for minimal overhead
- **Extensible**: New entry types can be added without breaking existing code
- **Destructured context**: Context properties available via destructuring in op signatures

## Entry Type Definitions

The entry type system defines exactly what each row in a trace represents:

### Span Lifecycle Entry Types

Spans represent units of work with a clear beginning and end. These entry types use **fixed row positions** in the
SpanBuffer (see [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md) for details):

```
SpanBuffer row layout:
┌──────────────────────────────────────────────────────────────────────────┐
│ Row 0: span-start     │ tag writes HERE (overwrites)                     │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 1: span-exception │ Pre-initialized at span creation                 │
│        (pre-init)     │ Overwritten by ok() → span-ok                    │
│                       │            or err() → span-err                   │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 2+: events        │ log.info/debug/warn/error appends here           │
│                       │ span-retry also appends here (transient failures)│
└──────────────────────────────────────────────────────────────────────────┘

Span completion entry types (all written to row 1):
  - span-ok (2):        Success, written by ok()
  - span-err (3):       Expected business error, written by err()
  - span-exception (4): Uncaught exception, pre-initialized at row 1
```

- **`span-start`** - Beginning of a work unit (span) - **Always row 0**
  - Created when entering a traced function or operation
  - Always paired with exactly one completion entry type
  - Contains span metadata (module, span_name, parent relationships)
  - `tag.*` writes span attributes to this row (overwrites, not appends)
  - Can optionally include structured attributes and message

- **`span-ok`** - Span completed with `return ok()` - **Always row 1**
  - Normal successful completion path
  - Overwrites the pre-initialized span-exception entry
  - May contain result data in attribute columns
  - Indicates the span achieved its intended outcome

- **`span-err`** - Span completed with `return err()` - **Always row 1**
  - Expected error/failure completion path
  - Overwrites the pre-initialized span-exception entry
  - May contain error details in attribute columns
  - Still considered "handled" - not an exception

- **`span-exception`** - Span threw unexpected exception - **Always row 1**
  - Pre-initialized at span creation (row 1 defaults to this)
  - Remains if exception bypassed normal `ok()`/`err()` flow
  - Contains exception details in the `message` column (see "The `message` Column" below)
  - Indicates truly exceptional circumstances
  - Duration still valid: `timestamps[1] - timestamps[0]`

- **`span-retry`** - Transient failure triggering retry - **Row 2+ (appended)**
  - Written by Op class when result is TransientError and retry will occur
  - Contains timing info for the failed attempt
  - Parent span has full context tags (op name, key, etc.)
  - Minimal child span with retry-specific data:
    - `retry_attempt`: Attempt number (1, 2, 3...)
    - `retry_error`: Error message that triggered retry
    - `retry_delay_ms`: Delay before next attempt
  - Duration measures the failed attempt (not including retry delay)

#### Buffer Initialization Code

The fixed row layout is established at span creation and updated on completion:

```typescript
function initializeSpanBuffer(buffer: SpanBuffer, spanName: string): void {
  const now = getTimestamp();

  // Row 0: span-start (always present)
  buffer.timestamp[0] = now;
  buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

  // Row 1: pre-initialized as span-exception (exception safety)
  buffer.timestamp[1] = now; // Updated by ok()/err()
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION; // Overwritten on completion

  // Ready for events at row 2
  buffer._writeIndex = 2;
}

function completeSpanOk(buffer: SpanBuffer, result?: any): void {
  buffer.timestamp[1] = getTimestamp();
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
  // Result data written to row 1's attribute columns
}

function completeSpanErr(buffer: SpanBuffer, error: string): void {
  buffer.timestamp[1] = getTimestamp();
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_ERR;
  // Error details written to row 1's attribute columns
}
```

This design ensures every span has valid duration data, even when exceptions bypass normal completion.

### Fluent Result Integration

`ctx.ok()` and `ctx.err()` return **buffer-agnostic** fluent builders. Tags are captured as closures and applied by
`span()`/`trace()` when the function returns:

```typescript
// FluentOk - tags captured, applied at span-end
return ctx.ok(value).with({ cached: true }).message('Success');

// FluentErr - tags captured, applied at span-end
return ctx.err('NOT_FOUND', { id }).with({ searched: true }).message('Not found');
```

#### Deferred Tag Application

**Key Design**: `FluentOk` and `FluentErr` do NOT hold buffer references. This ensures type safety - tags are always
applied to the correct buffer, preventing accidental writes to wrong buffers if a result is captured.

Each chained method (`.with()`, `.message()`, `.line()`) captures the write operation as a closure:

```typescript
class FluentOk<V, T extends LogSchema> implements Ok<V> {
  readonly value: V;
  applyTags?: (buffer: SpanBuffer<T>) => void; // Closure chain

  constructor(value: V) {
    this.value = value;
    // No buffer reference - buffer-agnostic!
  }

  get success(): true {
    return true;
  } // Derived from class identity

  message(text: string): this {
    const prev = this.applyTags;
    this.applyTags = (buffer) => {
      if (prev) prev(buffer);
      buffer.message(1, text);
    };
    return this;
  }

  // Similar pattern for .with() and .line()
}
```

#### Span-End Write Sequence

When `span()` or `trace()` receives the result, it writes to row 1:

```typescript
function writeSpanEnd(buffer: SpanBuffer, result: Result): void {
  // 1. Write entry_type based on result type
  if (result instanceof FluentOk) {
    buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
  } else if (result instanceof FluentErr) {
    buffer.entry_type[1] = ENTRY_TYPE_SPAN_ERR;
    buffer.error_code(1, result.error.code);  // Write error code
  }

  // 2. Write timestamp
  buffer.timestamp[1] = getTimestampNanos(...);

  // 3. Apply deferred tags
  if (result.applyTags) {
    result.applyTags(buffer);
  }
}
```

This enables:

1. **Type safety**: Result objects have no buffer reference - can't write to wrong buffer
2. **Attribute chaining**: `.with()` captures attributes to apply at span-end
3. **Custom messages**: `.message()` overwrites the default span name in row 1
4. **Source tracking**: `.line()` sets source line number

#### Message Column Semantics

The `message` column has different semantics for row 0 vs row 1:

| Row | Default Value | Can Be Overwritten? | How to Overwrite |
| --- | ------------- | ------------------- | ---------------- |
| 0   | Span name     | No (immutable)      | N/A              |
| 1   | Span name     | Yes                 | `.message()`     |

```typescript
// Row 1 message = "create-user" (span name, default)
return ctx.ok(user);

// Row 1 message = "User created successfully" (custom)
return ctx.ok(user).message('User created successfully');
```

#### Row 1 Entry Types

| Return Type                  | Entry Type Written   | Written By     |
| ---------------------------- | -------------------- | -------------- |
| `FluentOk` (from ctx.ok())   | `span-ok` (2)        | span()/trace() |
| `FluentErr` (from ctx.err()) | `span-err` (3)       | span()/trace() |
| Plain `Ok<T>`                | `span-ok` (2)        | span()/trace() |
| Plain `Err<E>`               | `span-err` (3)       | span()/trace() |
| Exception thrown             | `span-exception` (4) | span()/trace() |

**Note**: Entry types are always written by `span()`/`trace()` at span-end, never by the result constructor.

#### When to Return vs Throw

- **Known operational failures MUST return `Err`** (including `ctx.err(...)`), which produces `span-err`.
- **Unexpected invariant/programmer failures MAY throw**, which produces `span-exception`.
- Operational retry signaling must stay in `Err` values; do not rely on thrown exceptions for known retry paths.
- A thrown exception represents a bug or violated invariant, not normal control flow.

### Retry Entry Type

The `span-retry` entry type provides observability for transient failure handling in Op execution:

**When written:**

- Op returns TransientError (via `ctx.err(TRANSIENT_CODE({ data }))`)
- Op has retry policy and attempt limit not reached
- Logged BEFORE the delay wait, captures the failed attempt

**What it contains:**

- Timestamp of the failure
- Entry type: span-retry (5)
- Message: retry:op:{opName} (for prefix-based querying)
- retry_attempt: Which attempt failed (1-indexed)
- retry_error: Error message or code
- retry_delay_ms: How long until next attempt

**Key design decisions:**

- **Appended to parent span buffer**: Not a separate span, just a log entry
- **Prefix naming**: `retry:op:{name}` enables `retry:*` queries
- **Minimal data**: Parent span has full context (op_name, op_key, args)

**Counting retries:**

Count `span-retry` entries for a span to know how many retries occurred. If the final result is `span-ok`, all retries
succeeded eventually. If the final result is `span-err` with code RETRIES_EXHAUSTED, retries failed.

**Example trace:**

```
span-start: op:fetchPayment
span-retry: retry:op:fetchPayment (attempt=1, error="503 Service Unavailable", delay=100ms)
span-retry: retry:op:fetchPayment (attempt=2, error="503 Service Unavailable", delay=200ms)
span-ok: op:fetchPayment (success on attempt 3)
```

### Log Level Entry Types

Structured logging with message templates and typed attributes - **APPENDS new rows starting at row 2**:

- **`info`** - Information messages with optional structured data
- **`debug`** - Debug messages with optional structured data
- **`warn`** - Warning messages with optional structured data
- **`error`** - Error messages with optional structured data

These entry types enable gradual migration from console.log by providing structured logging with the familiar log
levels, but with typed attributes instead of just string concatenation.

#### The `message` Column: Format Strings, NOT Interpolation

**CRITICAL DESIGN DECISION**: Log messages use FORMAT STRINGS stored in the `message` column.

When you write:

```typescript
const processUser = op(async ({ log }, userData) => {
  log.info('User ${userId} processed ${count} items').with({ userId: 'user-123', count: 42 });
});
```

The system stores:

| Column    | Value                                       | Type         |
| --------- | ------------------------------------------- | ------------ |
| `message` | `'User ${userId} processed ${count} items'` | S.category() |
| `userId`  | `'user-123'`                                | S.category() |
| `count`   | `42`                                        | S.number()   |

**The message is NOT interpolated.** The template `'User ${userId} processed ${count} items'` is stored verbatim.

**Why Format Strings?**

1. **String Interning**: `message` uses `S.category()` type. Each unique template is interned once. Even if you log
   `"User ${userId} processed ${count} items"` 10,000 times with different values, the template string is stored ONCE.

2. **Queryable Templates**: You can find all logs matching a specific template:

   ```sql
   SELECT * FROM traces WHERE message = 'User ${userId} processed ${count} items';
   ```

3. **Analytics on Values**: Group and aggregate by the actual values:

   ```sql
   SELECT userId, count(*), avg(count)
   FROM traces
   WHERE message = 'User ${userId} processed ${count} items'
   GROUP BY userId;
   ```

4. **Type Safety**: Values are stored in typed columns (`count` as Float64, not as part of a string).

**Contrast with Traditional Logging**:

```typescript
// Traditional - interpolated string, no structure
console.log(`User ${userId} processed ${count} items`);
// Stores: "User user-123 processed 42 items" - unique string every time!

// LMAO - format string with typed values
const processUser = op(async ({ log }) => {
  log.info('User ${userId} processed ${count} items').with({ userId: 'user-123', count: 42 });
});
// Stores: template once, values in typed columns - structured and efficient!
```

**Row Behavior**: Unlike `tag.*` which overwrites row 0, `log.*` methods APPEND new rows:

```typescript
const processUser = op(async ({ log }) => {
  // writeIndex starts at 2 after span initialization
  log.info('Starting process'); // Writes to row 2, writeIndex → 3
  log.debug('Details...'); // Writes to row 3, writeIndex → 4
  log.warn('Slow operation'); // Writes to row 4, writeIndex → 5
});
```

### Span Attributes (tag)

**Note**: The `tag` API does NOT create a separate entry type. It updates attributes on the span-start row (row 0).

- `tag.attribute()` calls update **row 0** (span-start row) directly
- **OVERWRITES** row 0 attribute columns, does NOT append new rows
- Multiple `tag` calls update the same row - last write wins
- Follows Datadog's `span.set_tag()` / OpenTelemetry's `Span.setAttribute()` pattern
- Contrast with `log.*` which APPENDS new rows (events)

```typescript
const processUser = op(async ({ tag, log }) => {
  // tag writes to row 0 (span-start) - OVERWRITES, not appends
  tag.userId('user-123'); // Writes to userId_values[0]
  tag.requestId('req-456'); // Writes to requestId_values[0]
  tag.userId('user-999'); // Overwrites userId_values[0]

  // log creates new rows - APPENDS
  log.info('Processing...'); // Appends at row 2
  log.debug('Details...'); // Appends at row 3
});
```

### Feature Flag Entry Types

- **`ff-access`** - When feature flags are evaluated
  - Logged automatically by `FeatureFlagEvaluator`
  - Flag name stored in unified `message` column (consistent with span names and log templates)
  - Flag value stored in `ff_value` column (S.category for efficient storage of repeated values)
  - Tracks when decisions are made based on flags

- **`ff-usage`** - When flag-gated features are actually used
  - Logged when flag-controlled code paths execute
  - Flag name stored in unified `message` column
  - Contains usage context and outcome data (user-defined attribute columns)
  - Tracks actual feature utilization vs just evaluation

See **[Feature Flags](./01p_feature_flags.md)** for details on the evaluator implementation and tracking API.

### Metrics Entry Types

Metrics are structured logs that capture operational statistics during flush cycles. All metrics rows from the same
flush share identical `timestamp` (the period end time).

**Period Marker**:

- **`period-start`** - Marks the beginning of a metrics period
  - `uint64_value`: Nanosecond timestamp when the period began
  - `message`: unused (null)
  - Enables calculating period duration: `row.timestamp - row.uint64_value`

**Op Metrics** (8 entry types for operation statistics):

- **`op-invocations`** - Total invocation count for an operation
  - `message`: Op name (e.g., `'GET'`, `'createUser'`)
  - `uint64_value`: Total count of invocations during the period

- **`op-errors`** - Count of operations that returned `err()` (span-err)
  - `message`: Op name
  - `uint64_value`: Error count

- **`op-exceptions`** - Count of operations that threw exceptions (span-exception)
  - `message`: Op name
  - `uint64_value`: Exception count

- **`op-duration-total`** - Sum of all operation durations
  - `message`: Op name
  - `uint64_value`: Total nanoseconds spent in this operation

- **`op-duration-ok`** - Sum of durations for successful operations (span-ok)
  - `message`: Op name
  - `uint64_value`: Nanoseconds spent in successful invocations

- **`op-duration-err`** - Sum of durations for error operations (span-err)
  - `message`: Op name
  - `uint64_value`: Nanoseconds spent in error invocations
  - **Design insight**: Comparing `op-duration-err` to `op-duration-ok` reveals if errors are fast-fails or slow
    timeouts

- **`op-duration-min`** - Minimum operation duration observed
  - `message`: Op name
  - `uint64_value`: Shortest duration in nanoseconds

- **`op-duration-max`** - Maximum operation duration observed
  - `message`: Op name
  - `uint64_value`: Longest duration in nanoseconds

**Buffer Metrics** (4 entry types for buffer statistics):

- **`buffer-writes`** - Total entries written to buffers
  - `uint64_value`: Count of all entries written during the period
  - `message`: unused (null)

- **`buffer-overflow-writes`** - Entries written to overflow buffers
  - `uint64_value`: Count of entries that went to chained overflow buffers
  - `message`: unused (null)

- **`buffer-created`** - SpanBuffers allocated
  - `uint64_value`: Number of new SpanBuffer instances created
  - `message`: unused (null)

- **`buffer-overflows`** - Times a buffer overflowed and chained
  - `uint64_value`: Number of overflow events
  - `message`: unused (null)

## Fluent API Design

With unified tag attributes across all entry types, we use a fluent/chainable API pattern that can be mixed into
different operations:

```typescript
const processOrder = op(async ({ ok, err, span, log }, order) => {
  // Fluent span completion API
  return ok(result).with({ userId: user.id, operation: 'CREATE' }).message('User created successfully');

  return err('VALIDATION_FAILED')
    .with({
      field: 'email',
      attemptedValue: userData.email,
      validationRule: 'unique_constraint',
    })
    .message('Email validation failed');

  // Fluent logging API
  log.info('Processing user data').with({ userId: user.id, operation: 'PROCESS' });

  // Simple usage when no additional context needed
  return ok(result);
  return err('VALIDATION_FAILED');
  log.info('Simple message');
});
```

This fluent pattern provides a consistent, composable API across all trace operations.

### The `.uint64()` Fluent Method

The `.uint64()` method attaches a large integer value to any entry type. It writes to the `uint64_value` lazy system
column (see [Arrow Table Structure](./01f_arrow_table_structure.md)).

**Use Cases**:

- Large record counts that exceed safe JavaScript integer range
- Byte counts for data processing
- External IDs (Snowflake IDs, Discord IDs, etc.)
- Any uint64 value that needs to be queryable

**API Examples**:

```typescript
const processRecords = op(async ({ log, tag, ok }, records) => {
  // Attach to tag entry (row 0)
  tag.batchId(batchId).uint64(recordCount);

  // Attach to log entry (appended row)
  log.info('Processing complete').uint64(bytesProcessed);

  // Attach to result (row 1)
  return ok(result).uint64(totalRecords);
});
```

**Why a Dedicated Column**:

1. **JavaScript's number limitation**: `Number.MAX_SAFE_INTEGER` is 2^53-1 (~9 quadrillion), but many systems use full
   uint64 values
2. **Shared with metrics**: Both metrics (counts, nanoseconds) and users need large integers - one lazy column serves
   all
3. **Type safety**: Stored as `BigUint64Array`, converted to Arrow `uint64`
4. **Query efficiency**: Direct column access without JSON parsing

### SpanLogger Zero-Allocation Design

After exploring several approaches, we arrived at a zero-allocation design where the `SpanLogger` instance serves
multiple roles:

#### Design Evolution and Trade-offs

**❌ Approach 1: Separate Builder Objects**

```typescript
// Creates new objects on every call - expensive!
info(message): FluentLogBuilder { return new FluentLogBuilder(...); }
tag: TagProxy { return createTagProxy(...); }
```

_Problem_: Object allocation on every logging call defeats performance goals.

**❌ Approach 2: Shared Objects with Buffer Swapping**

```typescript
// Single shared tag object, swap buffer per span
sharedTag.buffer = currentBuffer;
```

_Problem_: Async/promises would cause buffer conflicts between concurrent spans.

**❌ Approach 3: Runtime Proxy**

```typescript
tag = new Proxy(target, handler); // Dynamic property access
```

_Problem_: Proxy overhead on every property access contradicts zero-overhead goals.

**❌ Approach 4: Object Creation in Constructor**

```typescript
constructor(buffer) {
  this.tag = new CompiledTagOps(buffer); // One allocation per SpanLogger
}
```

_Problem_: Still allocating objects, even if only once per span.

**✅ Final Approach: Separate tag and log APIs via Destructuring**

The API is simplified by destructuring `tag` and `log` directly from the context:

```typescript
// Op context provides separate API surfaces via destructuring
const processUser = op(async ({ tag, log, scope, ok, err }, userData) => {
  // tag: For span attributes (chainable setters)
  // log: For log messages (info/debug/warn/error)
  // scope: For scoped attributes
  // ok/err: For span completion
});

// TagAPI handles span attributes (chainable)
class TagAPI {
  constructor(buffer) {
    this.buffer = buffer;
    // Runtime class generation adds attribute methods to prototype
  }

  // Callable for object-based API: tag({ userId: "123" })
  (attributes: Record<string, any>) {
    this._writeAttributes(attributes);
    return this;
  }

  // Attribute methods compiled onto prototype at module context creation time
  // These directly write to TypedArrays for minimal overhead
  userId(value) {
    this.buffer.userId_values[0] = value;  // Always row 0
    return this; // Chainable!
  }

  httpStatus(value) {
    this.buffer.httpStatus_values[0] = value;
    return this;
  }
}

// LogAPI handles log messages
class LogAPI {
  constructor(buffer) {
    this.buffer = buffer;
  }

  // Log methods return fluent interface for .with() chaining
  info(message) {
    this._writeLogEntry('info', message);
    return this;
  }

  with(attributes) {
    this._writeAttributes(attributes);
    return this;
  }
}
```

#### Key Insights

1. **Destructured APIs**: `tag` and `log` destructured directly from context - cleaner than nested access
2. **Zero allocation chaining**: All fluent methods return the same instance for continued chaining
3. **Runtime class generation**: TagAPI and LogAPI classes built at runtime with `new Function` for typed methods
4. **Per-span instances**: Each span creates new tag/log instances with direct buffer references
5. **Sorted output**: Each span's entries stay together in final Arrow output
6. **Prototype compilation**: Attribute methods compiled onto prototypes at module context creation time
7. **Module boundary safety**: Unknown columns written as null when called from deeper contexts
8. **Reserved method names**: `with` is reserved and cannot be used as attribute column names
9. **OpenTelemetry alignment**: `tag` mirrors Span.setAttribute(), `ok()/err()` mirrors Span.setStatus()

#### Usage Examples

```typescript
const processUser = op(async ({ tag, log, scope, span, ok, err }, userData) => {
  // ===== SPAN ATTRIBUTES (tag) =====
  // Set span attributes - chainable methods
  tag.userId('user-123').requestId('req-456').operation('CREATE');

  // Object-based API for multiple attributes at once
  tag({ userId: 'user-123', requestId: 'req-456', operation: 'CREATE' });

  // ===== LOG MESSAGES (log) =====
  // FORMAT STRING PATTERN - template stored in message, values in typed columns
  // The message is NOT interpolated - template and values stored separately!

  log.info('Processing user ${userId}').with({ userId: 123 });
  // Stores: message='Processing user ${userId}', userId=123

  log.debug('Query on ${table} took ${duration}ms').with({ table: 'users', duration: 12.5 });
  // Stores: message='Query on ${table} took ${duration}ms', table='users', duration=12.5

  log.warn('Rate limit ${current}/${max}').with({ current: 95, max: 100 });
  // Stores: message='Rate limit ${current}/${max}', current=95, max=100

  log.error('Connection to ${host} failed').with({ host: 'db.example.com' });
  // Stores: message='Connection to ${host} failed', host='db.example.com'

  // ===== SCOPED ATTRIBUTES (scope) =====
  // Set once, propagates to all entries and child spans
  scope({ requestId: 'req-456', userId: 'user-123' });
  // All subsequent entries in this span include these attributes

  // ===== SPAN COMPLETION (ok/err) =====
  // Success with result data
  return ok(result).with({ rowsAffected: 5, duration: 12.5 });

  // Error with context
  return err('VALIDATION_FAILED').with({ field: 'email', rule: 'unique' });

  // Simple usage when no additional context needed
  return ok(result);
  return err('NOT_FOUND');

  // ===== CHILD SPANS (span) =====
  const payment = await span('process-payment', processPaymentOp, order);
});
```

#### Performance Characteristics

- **Zero allocation**: Only the SpanLogger instance is allocated
- **Prototype methods**: All tag operations pre-compiled at module context creation time
- **V8 optimization**: Single object with stable hidden class for optimal JIT compilation
- **Memory efficiency**: No intermediate objects or proxies

**Transformer Optimization for `with()` Bulk Setter:**

The `with()` bulk setter is syntactic sugar that the transformer can unroll into individual setter calls:

```typescript
// What you write:
tag.with({ userId: 'user-123', requestId: 'req-456' });

// What the transformer produces (unrolled for V8 inline caching):
tag.userId('user-123').requestId('req-456');
```

This unrolling eliminates the object allocation and `Object.keys()` iteration at runtime, making `with()` zero-cost when
the transformer is enabled. Without the transformer, `with()` still works but has minor overhead from iterating the
object keys.

This design achieves the fluent API ergonomics while maintaining the zero-overhead performance goals.

### Type Safety with Generics

The API uses TypeScript generics to provide full type safety for attribute operations:

```typescript
// Reserved method names that cannot be used as attributes
type ReservedTagNames = 'with';  // Fewer reserved names since tag is separate
type ReservedLogNames = 'with' | 'info' | 'debug' | 'warn' | 'error';

// TagAPI - for span attributes (tag)
interface TagAPI<TAttributes extends ValidAttributes<TAttributes>> {
  // Callable for object-based API
  (attributes: Partial<TAttributes>): TagAPI<TAttributes>;

  // Dynamically generated attribute methods
  [K in keyof TAttributes]: (value: TAttributes[K]) => TagAPI<TAttributes>;
}

// LogAPI - for log messages (log)
interface LogAPI<TAttributes extends ValidAttributes<TAttributes>> {
  info(message: string): FluentLog<TAttributes>;
  debug(message: string): FluentLog<TAttributes>;
  warn(message: string): FluentLog<TAttributes>;
  error(message: string): FluentLog<TAttributes>;
}

interface FluentLog<TAttributes> {
  with(attributes: Partial<TAttributes>): FluentLog<TAttributes>;
}
```

**Usage with Full Type Safety**:

```typescript
// Define attribute schema for module
interface UserServiceAttributes {
  user_id: string;
  http_status: number;
  operation: 'CREATE' | 'UPDATE' | 'DELETE';
  email?: string;
  // with: string;  // ❌ Compile error: reserved name
}

// Op context is typed with attribute schema (Schema, Deps, FF, Extra)
const createUser = op(
  async ({ tag, log, scope, ok }: SpanContext<UserServiceAttributes, {}, {}, {}>, userData: UserData) => {
    // ===== tag - Span Attributes =====
    // Chainable attribute setters (destructured from context)
    tag
      .user_id(userData.id) // ✅ Generated: (value: string) => this
      .http_status(201) // ✅ Generated: (value: number) => this
      .operation('CREATE'); // ✅ Generated: (value: 'CREATE' | 'UPDATE' | 'DELETE') => this

    // Object-based API
    tag({
      user_id: userData.id,
      operation: 'CREATE',
      email: userData.email, // ✅ optional property
    });

    // ===== log - Log Messages =====
    log.info('Creating user').with({
      user_id: userData.id, // ✅ string
      operation: 'CREATE', // ✅ literal type
    });

    // Scoped attributes for the span
    scope({ user_id: userData.id });

    // TypeScript knows exactly what's available:
    // tag.user_id        ✅ Available
    // tag.http_status    ✅ Available
    // tag.invalidProp    ❌ TypeScript error: doesn't exist
  }
);
```

This provides complete compile-time safety while maintaining zero runtime overhead through prototype compilation.

### Reserved Method Names

The APIs reserve certain method names that cannot be used as attribute column names:

**On `tag` (TagAPI)**:

- **`with`**: Reserved for future bulk attribute setting (currently callable via `tag({ ... })`)

**On `log` (LogAPI)**:

- **`with`**: Used for fluent attribute chaining
- **`info`, `debug`, `warn`, `error`**: Used for log level methods

When defining attribute schemas, these names must be avoided to prevent conflicts with the API methods.

### Public vs Low-Level API Separation

**Span Attributes** (`tag`):

- `tag.attribute(value)` - chainable attribute setters
- `tag({ ... })` - object-based attribute setting
- Follows OpenTelemetry's `Span.setAttribute()` pattern

**Log Messages** (`log`):

- `info()`, `debug()`, `warn()`, `error()` - structured logging
- `with({ ... })` - fluent attribute chaining
- Every span creates a new `log` instance with reference to its buffer

**Scoped Attributes** (`scope`):

- `scope({ ... })` - scoped attributes that propagate to all entries

**Span Completion** (`ok`, `err`):

- `ok(result)` - success with optional result data (creates `span-ok` entry)
- `err(code)` - error with optional context (creates `span-err` entry)
- Follows OpenTelemetry's `Span.setStatus()` pattern

**Child Spans** (`span`):

- `span(name, op, ...args)` - invoke another op as child span

**Low-Level Operations** (handled by context system):

- `span-start` entries created automatically when spans begin
- `span-exception` entries created when exceptions bypass normal completion
- Direct TypedArray writes for minimal overhead

Each span's context (tag + log) references its own buffer, avoiding traceid+spanid appends and keeping entries neatly
sorted in Arrow output.

The `log` API is for explicit logging during execution, while `ok()`/`err()` are for span completion. Both use the same
underlying entry type system. Each span's log instance references its own buffer, avoiding traceid+spanid appends and
keeping logs neatly sorted in Arrow output.

## Fluent Result API

The `ok()` and `err()` functions return **buffer-agnostic** fluent result objects that support method chaining while
maintaining TypeScript type narrowing for Result pattern consumption.

### Deferred Tag Application Design

**Key Insight**: FluentOk and FluentErr do NOT hold buffer references. This ensures:

1. **Type safety**: Can't accidentally write to wrong buffer if result is captured
2. **Clean semantics**: `return ctx.ok(value)` is clearly a terminal expression
3. **Correct buffer targeting**: `span()`/`trace()` applies tags to the correct buffer

Each chained method captures the write operation as a closure in `applyTags`:

```typescript
// What happens when you write:
return ctx.ok(user).with({ cached: true }).message('Created');

// 1. ctx.ok(user) creates FluentOk with value, no buffer reference
// 2. .with({ cached: true }) captures attribute writes in closure
// 3. .message('Created') chains another closure
// 4. span() receives result, calls writeSpanEnd(buffer, result)
// 5. writeSpanEnd writes entry_type, timestamp, then calls result.applyTags(buffer)
```

### FluentOk

Returned by `ctx.ok(value)`. Implements the `Ok<V>` interface with deferred chaining methods:

```typescript
class FluentOk<V, T extends LogSchema> implements Ok<V> {
  readonly value: V;
  applyTags?: (buffer: SpanBuffer<T>) => void; // Closure chain

  constructor(value: V) {
    this.value = value;
    // No buffer reference!
  }

  get success(): true {
    return true;
  } // Derived from class identity

  // Capture attribute writes as closure
  with(attributes: Partial<InferSchema<T>>): this;

  // Capture message write as closure (overwrites span name in row 1)
  message(text: string): this;

  // Capture line number write as closure
  line(lineNumber: number): this;
}
```

### FluentErr

Returned by `ctx.err(code, details)`. Implements the `Err<E>` interface with deferred chaining methods:

```typescript
class FluentErr<E, T extends LogSchema> implements Err<E> {
  readonly error: { code: string; details: E };
  applyTags?: (buffer: SpanBuffer<T>) => void; // Closure chain

  constructor(code: string, details: E) {
    this.error = { code, details };
    // No buffer reference! error_code written by span()/trace()
  }

  get success(): false {
    return false;
  } // Derived from class identity

  // Same chaining methods as FluentOk
  with(attributes: Partial<InferSchema<T>>): this;
  message(text: string): this;
  line(lineNumber: number): this;
}
```

### Buffer Write Timing

All writes to row 1 happen when `span()`/`trace()` completes:

1. **Entry type**: Written based on `instanceof FluentOk` vs `FluentErr`
2. **Timestamp**: Written at span-end time
3. **Error code**: Written from `result.error.code` for FluentErr
4. **Deferred tags**: Applied via `result.applyTags(buffer)` if present

This ensures row 1 is never partially written - it's atomic at span completion.

### Usage Examples

```typescript
const createUser = op(async ({ ok, err }, userData) => {
  // Simple success - just the value
  return ok(user);

  // Success with attributes
  return ok(user).with({ rowsAffected: 1, cacheHit: false });

  // Success with message
  return ok(user).message('User created successfully');

  // Success with both attributes and message
  return ok(user).with({ rowsAffected: 1, duration: 42.5 }).message('User created successfully');

  // Simple error - code and details
  return err('NOT_FOUND', { userId: 'user-123' });

  // Error with additional attributes
  return err('VALIDATION_FAILED', { field: 'email' }).with({
    attemptedValue: userData.email,
    validationRule: 'unique_constraint',
  });

  // Error with message
  return err('NOT_FOUND', { userId }).message('User not found in database');

  // Error with both attributes and message
  return err('VALIDATION_FAILED', { field: 'email' })
    .with({ validationRule: 'format' })
    .message('Invalid email format provided');
});
```

### Result Pattern Compatibility

The fluent result objects are fully compatible with TypeScript's discriminated union narrowing:

```typescript
const result = await userService.createUser(userData);

if (result.success) {
  // TypeScript knows: result is FluentSuccessResult
  // result.value is available
  console.log('Created user:', result.value.id);
} else {
  // TypeScript knows: result is FluentErrorResult
  // result.error.code and result.error.details are available
  console.log('Failed:', result.error.code, result.error.details);
}
```

### Error Code Storage

When `err(code, details)` is called, the error code is automatically written to the `errorCode` column. This enables
efficient querying of errors by code:

```sql
SELECT * FROM traces
WHERE entry_type = 'span-err'
  AND errorCode = 'VALIDATION_FAILED';
```

### Message Storage

Both `.message()` methods write to the unified `message` column, enabling message-based queries:

```sql
SELECT span_name, message, errorCode
FROM traces
WHERE entry_type IN ('span-ok', 'span-err')
  AND message IS NOT NULL;
```

## Low-Level Logging API

The entry type system is implemented through low-level column writers that directly populate trace buffers:

### Column Writer Interface (Design TBD)

The exact API for writing to columns is still being designed. The examples below use a placeholder `writeColumnName()`
pattern to illustrate the concepts, but the actual implementation will likely be much cleaner:

```typescript
// PLACEHOLDER - actual API design TBD
interface ColumnWriters {
  // Core system columns
  writeTimestamp(value: bigint): void;
  writeTraceId(value: string): void;
  writeSpanId(value: bigint): void;
  writeParentSpanId(value: bigint | null): void;
  writeEntryType(value: EntryType): void;
  writeModule(value: string): void;
  writeMessage(value: string): void; // Span name, log message template, OR flag name (S.category)

  // Generated attribute columns (example - actual API TBD)
  writeHttpStatus(value: number): void;
  writeHttpMethod(value: string): void;
  writeUserId(value: string): void;
  // ... more generated based on schema
}
```

### Entry Type Creation Patterns (Conceptual)

Each entry type follows specific patterns for populating columns. The examples below use placeholder `writeColumnName()`
calls to illustrate the concepts - the actual column writing API is still being designed:

#### Span Start Pattern

```typescript
function createSpanStart(module: string, spanName: string, parentSpanId?: bigint, attributes?: Record<string, any>) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(generateSpanId());
  writers.writeParentSpanId(parentSpanId ?? null);
  writers.writeEntryType('span-start');
  writers.writeModule(module);
  writers.writeMessage(spanName); // message = span name for span entries

  // Optional structured attributes for span start
  // TODO: Actual column writing API design TBD
  if (attributes) {
    for (const [key, value] of Object.entries(attributes)) {
      const writerMethod = `write${capitalize(key)}`;
      if (writers[writerMethod]) {
        writers[writerMethod](value);
      }
    }
  }
}
```

#### Span Completion Pattern

```typescript
function createSpanCompletion(entryType: 'span-ok' | 'span-err', result?: any, attributes?: Record<string, any>) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(entryType);
  writers.writeModule(getCurrentModule());
  writers.writeMessage(getCurrentSpanName()); // message = span name for span entries

  // Structured attributes from completion
  // TODO: Actual column writing API design TBD
  if (attributes) {
    for (const [key, value] of Object.entries(attributes)) {
      const writerMethod = `write${capitalize(key)}`;
      if (writers[writerMethod]) {
        writers[writerMethod](value);
      }
    }
  }

  // Result data may also populate attribute columns
  // (implementation depends on how result data is structured)
}
```

#### Log Entry Pattern

```typescript
// IMPORTANT: messageTemplate is a FORMAT STRING, not an interpolated message!
// Example: 'User ${userId} created' - the template is stored verbatim
// Values like userId are written to their own typed columns (userId_values)
function createLogEntry(
  level: 'info' | 'debug' | 'warn' | 'error',
  messageTemplate: string, // FORMAT STRING - stored as-is, NOT interpolated
  attributes?: Record<string, any>
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(level);
  writers.writeModule(getCurrentModule());
  writers.writeMessage(messageTemplate); // message = message TEMPLATE (not interpolated!)

  // Attribute VALUES go in their own columns
  // Template references like ${userId} are NOT replaced - stored verbatim
  // TODO: Actual column writing API design TBD
  if (attributes) {
    for (const [key, value] of Object.entries(attributes)) {
      const writerMethod = `write${capitalize(key)}`;
      if (writers[writerMethod]) {
        writers[writerMethod](value);
      }
    }
  }
}
```

#### Span Attribute Pattern (tag)

**Note**: `tag` does NOT create a new entry. It updates row 0 (span-start row) directly.

```typescript
// tag writes directly to row 0's attribute columns
// No new entry type - just updating the span-start row
function writeTagAttribute(buffer: SpanBuffer, columnName: string, value: any) {
  // ALWAYS write to row 0 (span-start row) - overwrite semantics
  buffer[`${columnName}_values`][0] = value;
  buffer[`${columnName}_nulls`][0] |= 1; // Mark as valid
}

// Usage: tag.userId('123') → writeTagAttribute(buffer, 'userId', '123')
```

#### Feature Flag Pattern

```typescript
function createFeatureFlagEntry(
  entryType: 'ff-access' | 'ff-usage',
  flagName: string,
  flagValue: boolean | number | string,
  context: Record<string, any>
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(entryType);
  writers.writeModule(getCurrentModule());
  writers.writeMessage(flagName); // message = flag name for ff entries (unified column)

  // Feature flag value column (S.category for efficient storage of repeated values)
  writers.writeFfValue(String(flagValue)); // Serialized to string, category-interned

  if (entryType === 'ff-usage' && context) {
    // track(context) is applied as track().with(context)
    // Unknown keys are ignored by with(); known schema fields are written to this row.
    writers.with(context);
  }
}
```

## Span Duration Calculation

The fixed row layout guarantees that duration is always computable:

```typescript
// Duration = timestamps[1] - timestamps[0]
// Works for ALL completion types: span-ok, span-err, AND span-exception
function getSpanDuration(buffer: SpanBuffer): number {
  return buffer.timestamp[1] - buffer.timestamp[0];
}
```

**Why This Works**:

- Row 0 (span-start) timestamp set at span creation
- Row 1 timestamp set at completion (ok/err) OR remains at creation time (exception)
- Even uncaught exceptions have valid duration (time from span-start to when exception was thrown)

**Query Pattern** (ClickHouse):

```sql
-- Duration from fixed row positions
SELECT
  thread_id,
  span_id,
  span_name,
  entry_type,
  -- Rows 0 and 1 are guaranteed to exist for every span
  max(timestamp) - min(timestamp) as duration_ms
FROM traces
WHERE entry_type IN ('span-start', 'span-ok', 'span-err', 'span-exception')
GROUP BY thread_id, span_id, span_name, entry_type;
```

## Entry Type Validation

The system enforces entry type constraints at the API level:

### Required Columns by Entry Type

- **All entry types**: `timestamp`, `trace_id`, `thread_id`, `span_id`, `entry_type`, `module`, `message`
- **Span lifecycle** (`span-start`, `span-ok`, `span-err`, `span-exception`): `message` contains span name
- **Log level types** (`info`, `debug`, `warn`, `error`): `message` contains message TEMPLATE (format string, not
  interpolated)
- **Feature flag types** (`ff-access`, `ff-usage`): `message` contains flag name, `ff_value` contains the evaluated
  value
- **Op metrics** (`op-invocations`, `op-errors`, `op-exceptions`, `op-duration-*`): `message` contains op name,
  `uint64_value` contains the metric value
- **Buffer metrics** (`buffer-writes`, `buffer-overflow-writes`, `buffer-created`, `buffer-overflows`): `uint64_value`
  contains the metric value
- **Period marker** (`period-start`): `uint64_value` contains the period start timestamp (nanoseconds)

### The `message` Column by Entry Type

| Entry Type                                            | `message` Contains                                      |
| ----------------------------------------------------- | ------------------------------------------------------- |
| `span-start`, `span-ok`, `span-err`, `span-exception` | Span name (e.g., `'create-user'`)                       |
| `info`, `debug`, `warn`, `error`                      | Log message template (e.g., `'User ${userId} created'`) |
| `ff-access`, `ff-usage`                               | Flag name (e.g., `'advancedValidation'`, `'darkMode'`)  |
| `op-*` (all 8 op metric types)                        | Op name (e.g., `'GET'`, `'createUser'`)                 |
| `period-start`, `buffer-*` (5 types)                  | unused (null)                                           |

### Fixed Row Constraints

- **Row 0**: Always `span-start` - created at span initialization
- **Row 1**: Always completion type (`span-ok`, `span-err`, or `span-exception`) - pre-initialized as `span-exception`
- **Row 2+**: Event entries (`info`, `debug`, `warn`, `error`, `ff-access`, `ff-usage`)

**Metrics rows** (`period-start`, `op-*`, `buffer-*`) are written during flush cycles, not during span execution. They
are appended to the global trace buffer, not to individual SpanBuffers.

### Forbidden Combinations

- **Span completion without start**: `span-ok`/`span-err`/`span-exception` must have matching `span-start`
- **Orphaned spans**: All spans except root must have valid `parent_thread_id` and `parent_span_id`
- **Mixed concerns**: Feature flag columns only valid with `ff-access`/`ff-usage` entry types

## Performance Characteristics

### Dictionary Encoding Benefits

Entry types use Arrow's dictionary encoding:

- **Storage efficiency**: Each unique entry type string stored once
- **Query performance**: Numeric comparisons instead of string matching
- **Memory efficiency**: References are small integers

### Hot Path Optimization

The entry type system is designed for minimal hot path overhead:

- **Pre-generated writers**: Column writers generated at op definition time
- **No conditionals**: Entry type determines exact code path
- **Direct memory writes**: No intermediate objects or transformations

## Integration Points

This entry type system integrates with:

- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Entry types become the `entry_type` column
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Generated APIs use these primitives
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Column writers populate buffer arrays
- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Op lifecycle creates span entry types

The entry type system provides the foundational vocabulary for all trace events, ensuring consistency and performance
across the entire logging system.
