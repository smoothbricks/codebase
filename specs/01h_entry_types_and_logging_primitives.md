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

## Entry Type Definitions

The entry type system defines exactly what each row in a trace represents:

### Span Lifecycle Entry Types

Spans represent units of work with a clear beginning and end. These entry types use **fixed row positions** in the
SpanBuffer (see [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md) for details):

```
SpanBuffer row layout:
┌──────────────────────────────────────────────────────────────────────────┐
│ Row 0: span-start     │ ctx.tag writes HERE (overwrites)                 │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 1: span-exception │ Pre-initialized at span creation                 │
│        (pre-init)     │ Overwritten by ctx.ok() → span-ok                │
│                       │            or ctx.err() → span-err               │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 2+: events        │ ctx.log.info/debug/warn/error appends here       │
└──────────────────────────────────────────────────────────────────────────┘

Span completion entry types (all written to row 1):
  - span-ok (2):        Success, written by ctx.ok()
  - span-err (3):       Expected business error, written by ctx.err()
  - span-exception (4): Uncaught exception, pre-initialized at row 1
```

- **`span-start`** - Beginning of a work unit (span) - **Always row 0**
  - Created when entering a traced function or operation
  - Always paired with exactly one completion entry type
  - Contains span metadata (module, span_name, parent relationships)
  - `ctx.tag.*` writes span attributes to this row (overwrites, not appends)
  - Can optionally include structured attributes and message

- **`span-ok`** - Span completed with `return ctx.ok()` - **Always row 1**
  - Normal successful completion path
  - Overwrites the pre-initialized span-exception entry
  - May contain result data in attribute columns
  - Indicates the span achieved its intended outcome

- **`span-err`** - Span completed with `return ctx.err()` - **Always row 1**
  - Expected error/failure completion path
  - Overwrites the pre-initialized span-exception entry
  - May contain error details in attribute columns
  - Still considered "handled" - not an exception

- **`span-exception`** - Span threw unexpected exception - **Always row 1**
  - Pre-initialized at span creation (row 1 defaults to this)
  - Remains if exception bypassed normal `ctx.ok()`/`ctx.err()` flow
  - Contains exception details in the `message` column (see "The `message` Column" below)
  - Indicates truly exceptional circumstances
  - Duration still valid: `timestamps[1] - timestamps[0]`

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
ctx.log.info('User ${userId} processed ${count} items').userId('user-123').count(42);
```

The system stores:

| Column        | Value                                       | Type         |
| ------------- | ------------------------------------------- | ------------ |
| `message`     | `'User ${userId} processed ${count} items'` | S.category() |
| `attr_userId` | `'user-123'`                                | S.category() |
| `attr_count`  | `42`                                        | S.number()   |

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
   SELECT attr_userId, count(*), avg(attr_count)
   FROM traces
   WHERE message = 'User ${userId} processed ${count} items'
   GROUP BY attr_userId;
   ```

4. **Type Safety**: Values are stored in typed columns (`attr_count` as Float64, not as part of a string).

**Contrast with Traditional Logging**:

```typescript
// Traditional - interpolated string, no structure
console.log(`User ${userId} processed ${count} items`);
// Stores: "User user-123 processed 42 items" - unique string every time!

// LMAO - format string with typed values
ctx.log.info('User ${userId} processed ${count} items').userId('user-123').count(42);
// Stores: template once, values in typed columns - structured and efficient!
```

**Row Behavior**: Unlike `ctx.tag.*` which overwrites row 0, `ctx.log.*` methods APPEND new rows:

```typescript
// writeIndex starts at 2 after span initialization
ctx.log.info('Starting process'); // Writes to row 2, writeIndex → 3
ctx.log.debug('Details...'); // Writes to row 3, writeIndex → 4
ctx.log.warn('Slow operation'); // Writes to row 4, writeIndex → 5
```

### Span Attributes (ctx.tag)

**Note**: The `ctx.tag` API does NOT create a separate entry type. It updates attributes on the span-start row (row 0).

- `ctx.tag.attribute()` calls update **row 0** (span-start row) directly
- **OVERWRITES** row 0 attribute columns, does NOT append new rows
- Multiple `ctx.tag` calls update the same row - last write wins
- Follows Datadog's `span.set_tag()` / OpenTelemetry's `Span.setAttribute()` pattern
- Contrast with `ctx.log.*` which APPENDS new rows (events)

```typescript
// ctx.tag writes to row 0 (span-start) - OVERWRITES, not appends
ctx.tag.userId('user-123'); // Writes to attr_userId[0]
ctx.tag.requestId('req-456'); // Writes to attr_requestId[0]
ctx.tag.userId('user-999'); // Overwrites attr_userId[0]

// ctx.log creates new rows - APPENDS
ctx.log.info('Processing...'); // Appends at row 2
ctx.log.debug('Details...'); // Appends at row 3
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

## Fluent API Design

With unified tag attributes across all entry types, we use a fluent/chainable API pattern that can be mixed into
different operations:

```typescript
// Fluent span completion API
return ctx.ok(result).with({ userId: user.id, operation: 'CREATE' }).message('User created successfully');

return ctx
  .err('VALIDATION_FAILED')
  .with({
    field: 'email',
    attemptedValue: userData.email,
    validationRule: 'unique_constraint',
  })
  .message('Email validation failed');

// Fluent span creation API
const payment = await ctx
  .span('process-payment')
  .with({ paymentMethod: order.paymentMethod, amount: order.total })
  .message('Processing payment for order')
  .run(async (childCtx) => {
    // implementation
  });

// Fluent logging API
ctx.log.info('Processing user data').with({ userId: user.id, operation: 'PROCESS' });

// Simple usage when no additional context needed
return ctx.ok(result);
return ctx.err('VALIDATION_FAILED');
ctx.log.info('Simple message');
```

This fluent pattern provides a consistent, composable API across all trace operations.

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

**✅ Final Approach: Separate ctx.tag and ctx.log APIs**

The API is simplified to have `ctx.tag` directly on the context (not nested under `ctx.log`):

```typescript
// SpanContext provides the main API surface
class SpanContext {
  log: SpanLogger;  // For log messages: info/debug/warn/error
  tag: TagAPI;      // For span attributes: chainable setters

  constructor(buffer) {
    this.buffer = buffer;
    this.log = new SpanLogger(buffer);
    this.tag = new TagAPI(buffer);  // Separate instance, direct on ctx
  }
}

// TagAPI handles span attributes (chainable)
class TagAPI {
  constructor(buffer) {
    this.buffer = buffer;
    // Runtime class generation adds attribute methods to prototype
  }

  // Callable for object-based API: ctx.tag({ userId: "123" })
  (attributes: Record<string, any>) {
    this._writeTagEntry();
    this._writeAttributes(attributes);
    return this;
  }

  // Attribute methods compiled onto prototype at module context creation time
  // These directly write to TypedArrays for minimal overhead
  userId(value) {
    this._writeTagEntry();
    this.buffer.attr_userId[this.buffer.writeIndex] = value;
    return this; // Chainable!
  }

  httpStatus(value) {
    this._writeTagEntry();
    this.buffer.attr_httpStatus[this.buffer.writeIndex] = value;
    return this;
  }
}

// SpanLogger handles log messages
class SpanLogger {
  constructor(buffer) {
    this.buffer = buffer;
  }

  // Log methods return fluent interface for .with() chaining
  info(message, attributes?) {
    this._writeLogEntry('info', message);
    if (attributes) this._writeAttributes(attributes);
    return this;
  }

  // Scoped attributes (pre-fill remaining buffer)
  scope(attributes) {
    this._prefillRemainingCapacity(attributes);
    return this;
  }

  with(attributes) {
    this._writeAttributes(attributes);
    return this;
  }
}
```

#### Key Insights

1. **Separate APIs**: `ctx.tag` for span attributes, `ctx.log` for log messages - cleaner than nested `ctx.log.tag`
2. **Zero allocation chaining**: All fluent methods return the same instance for continued chaining
3. **Runtime class generation**: TagAPI and SpanLogger classes built at runtime with `new Function` for typed methods
4. **Per-span instances**: Each span creates new tag/log instances with direct buffer references
5. **Sorted output**: Each span's entries stay together in final Arrow output
6. **Prototype compilation**: Attribute methods compiled onto prototypes at module context creation time
7. **Module boundary safety**: Unknown columns written as null when called from deeper contexts
8. **Reserved method names**: `with`, `scope` are reserved and cannot be used as attribute column names
9. **OpenTelemetry alignment**: `ctx.tag` mirrors Span.setAttribute(), `ctx.ok()/ctx.err()` mirrors Span.setStatus()

#### Usage Examples

```typescript
// ===== SPAN ATTRIBUTES (ctx.tag) =====
// Set span attributes - chainable methods
ctx.tag.userId('user-123').requestId('req-456').operation('CREATE');

// Object-based API for multiple attributes at once
ctx.tag({ userId: 'user-123', requestId: 'req-456', operation: 'CREATE' });

// ===== LOG MESSAGES (ctx.log) =====
// FORMAT STRING PATTERN - template stored in message, values in attr_* columns
// The message is NOT interpolated - template and values stored separately!

ctx.log.info('Processing user ${userId}').userId(123);
// Stores: message='Processing user ${userId}', attr_userId=123

ctx.log.debug('Query on ${table} took ${duration}ms').table('users').duration(12.5);
// Stores: message='Query on ${table} took ${duration}ms', attr_table='users', attr_duration=12.5

ctx.log.warn('Rate limit ${current}/${max}').current(95).max(100);
// Stores: message='Rate limit ${current}/${max}', attr_current=95, attr_max=100

ctx.log.error('Connection to ${host} failed').host('db.example.com');
// Stores: message='Connection to ${host} failed', attr_host='db.example.com'

// Object-based API for multiple attributes at once
ctx.log.info('Processing user ${userId}', { userId: 123, step: 'validation' });

// Scoped attributes (pre-fill remaining buffer capacity)
ctx.log.scope({ requestId: 'req-456', userId: 'user-123' });
// All subsequent entries in this span include these attributes

// ===== SPAN COMPLETION (ctx.ok/ctx.err) =====
// Success with result data
return ctx.ok(result).with({ rowsAffected: 5, duration: 12.5 });

// Error with context
return ctx.err('VALIDATION_FAILED').with({ field: 'email', rule: 'unique' });

// Simple usage when no additional context needed
return ctx.ok(result);
return ctx.err('NOT_FOUND');

// ===== CHILD SPANS (ctx.span) =====
const payment = await ctx
  .span('process-payment')
  .with({ paymentMethod: order.paymentMethod, amount: order.total })
  .run(async (childCtx) => {
    childCtx.tag.provider('stripe');
    return await processPayment(order);
  });
```

#### Performance Characteristics

- **Zero allocation**: Only the SpanLogger instance is allocated
- **Prototype methods**: All tag operations pre-compiled at module context creation time
- **V8 optimization**: Single object with stable hidden class for optimal JIT compilation
- **Memory efficiency**: No intermediate objects or proxies

This design achieves the fluent API ergonomics while maintaining the zero-overhead performance goals.

### Type Safety with Generics

The API uses TypeScript generics to provide full type safety for attribute operations:

```typescript
// Reserved method names that cannot be used as attributes
type ReservedTagNames = 'with';  // Fewer reserved names since tag is separate
type ReservedLogNames = 'with' | 'scope' | 'info' | 'debug' | 'warn' | 'error';

// TagAPI - for span attributes (ctx.tag)
interface TagAPI<TAttributes extends ValidAttributes<TAttributes>> {
  // Callable for object-based API
  (attributes: Partial<TAttributes>): TagAPI<TAttributes>;

  // Dynamically generated attribute methods
  [K in keyof TAttributes]: (value: TAttributes[K]) => TagAPI<TAttributes>;
}

// SpanLogger - for log messages (ctx.log)
interface SpanLogger<TAttributes extends ValidAttributes<TAttributes>> {
  info(message: string, attributes?: TAttributes): FluentLog<TAttributes>;
  debug(message: string, attributes?: TAttributes): FluentLog<TAttributes>;
  warn(message: string, attributes?: TAttributes): FluentLog<TAttributes>;
  error(message: string, attributes?: TAttributes): FluentLog<TAttributes>;
  scope(attributes: Partial<TAttributes>): SpanLogger<TAttributes>;
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

// Task context is typed with attribute schema
const createUser = task('create-user', async (ctx: TaskContext<UserServiceAttributes>, userData: UserData) => {
  // ===== ctx.tag - Span Attributes =====
  // Chainable attribute setters (directly on ctx, not ctx.log.tag)
  ctx.tag
    .user_id(userData.id) // ✅ Generated: (value: string) => this
    .http_status(201) // ✅ Generated: (value: number) => this
    .operation('CREATE'); // ✅ Generated: (value: 'CREATE' | 'UPDATE' | 'DELETE') => this

  // Object-based API
  ctx.tag({
    user_id: userData.id,
    operation: 'CREATE',
    email: userData.email, // ✅ optional property
  });

  // ===== ctx.log - Log Messages =====
  ctx.log.info('Creating user', {
    user_id: userData.id, // ✅ string
    operation: 'CREATE', // ✅ literal type
  });

  // Scoped attributes for the span
  ctx.log.scope({ user_id: userData.id });

  // TypeScript knows exactly what's available:
  // ctx.tag.user_id        ✅ Available
  // ctx.tag.http_status    ✅ Available
  // ctx.tag.invalidProp    ❌ TypeScript error: doesn't exist
});
```

This provides complete compile-time safety while maintaining zero runtime overhead through prototype compilation.

### Reserved Method Names

The APIs reserve certain method names that cannot be used as attribute column names:

**On `ctx.tag` (TagAPI)**:

- **`with`**: Reserved for future bulk attribute setting (currently callable via `ctx.tag({ ... })`)

**On `ctx.log` (SpanLogger)**:

- **`with`**: Used for fluent attribute chaining
- **`scope`**: Used for scoped attributes
- **`info`, `debug`, `warn`, `error`**: Used for log level methods

When defining attribute schemas, these names must be avoided to prevent conflicts with the API methods.

### Public vs Low-Level API Separation

**Span Attributes** (`ctx.tag`):

- `ctx.tag.attribute(value)` - chainable attribute setters (creates `tag` entries)
- `ctx.tag({ ... })` - object-based attribute setting
- Follows OpenTelemetry's `Span.setAttribute()` pattern

**Log Messages** (`ctx.log`):

- `info()`, `debug()`, `warn()`, `error()` - structured logging
- `scope({ ... })` - scoped attributes that propagate to all entries
- `with({ ... })` - fluent attribute chaining
- Every span creates a new `log` instance with reference to its buffer

**Span Completion** (`ctx.ok`, `ctx.err`):

- `ctx.ok(result)` - success with optional result data (creates `span-ok` entry)
- `ctx.err(code)` - error with optional context (creates `span-err` entry)
- Follows OpenTelemetry's `Span.setStatus()` pattern

**Child Spans** (`ctx.span`):

- `ctx.span(name)` - child span creation with fluent setup

**Low-Level Operations** (handled by context system):

- `span-start` entries created automatically when spans begin
- `span-exception` entries created when exceptions bypass normal completion
- Direct TypedArray writes for minimal overhead

Each span's context (tag + log) references its own buffer, avoiding traceid+spanid appends and keeping entries neatly
sorted in Arrow output.

The `ctx.log` API is for explicit logging during execution, while `ctx.ok()`/`ctx.err()` are for span completion. Both
use the same underlying entry type system. Each span's log instance references its own buffer, avoiding traceid+spanid
appends and keeping logs neatly sorted in Arrow output.

## Fluent Result API

The `ctx.ok()` and `ctx.err()` methods return fluent result objects that support method chaining while maintaining
TypeScript type narrowing for Result pattern consumption.

### FluentSuccessResult

Returned by `ctx.ok(value)`. Implements the `SuccessResult<V>` interface with additional chaining methods:

```typescript
class FluentSuccessResult<V, T extends TagAttributeSchema> implements SuccessResult<V> {
  readonly success = true;
  readonly value: V;

  // Chain additional attributes onto the span-ok entry
  with(attributes: Partial<InferTagAttributes<T>>): this;

  // Add a result message
  message(text: string): this;
}
```

### FluentErrorResult

Returned by `ctx.err(code, details)`. Implements the `ErrorResult<E>` interface with additional chaining methods:

```typescript
class FluentErrorResult<E, T extends TagAttributeSchema> implements ErrorResult<E> {
  readonly success = false;
  readonly error: { code: string; details: E };

  // Chain additional attributes onto the span-err entry
  with(attributes: Partial<InferTagAttributes<T>>): this;

  // Add an error message
  message(text: string): this;
}
```

### Buffer Layout

Both fluent result types write to **row 1** (the pre-allocated span-end row):

- Row 1 is pre-initialized as `span-exception` at span creation
- `ctx.ok()` overwrites row 1 with `span-ok` entry type
- `ctx.err()` overwrites row 1 with `span-err` entry type
- Chained `.with()` and `.message()` calls write attributes to the same row 1

### Usage Examples

```typescript
// Simple success - just the value
return ctx.ok(user);

// Success with attributes
return ctx.ok(user).with({ rowsAffected: 1, cacheHit: false });

// Success with message
return ctx.ok(user).message('User created successfully');

// Success with both attributes and message
return ctx.ok(user).with({ rowsAffected: 1, duration: 42.5 }).message('User created successfully');

// Simple error - code and details
return ctx.err('NOT_FOUND', { userId: 'user-123' });

// Error with additional attributes
return ctx.err('VALIDATION_FAILED', { field: 'email' }).with({
  attemptedValue: userData.email,
  validationRule: 'unique_constraint',
});

// Error with message
return ctx.err('NOT_FOUND', { userId }).message('User not found in database');

// Error with both attributes and message
return ctx
  .err('VALIDATION_FAILED', { field: 'email' })
  .with({ validationRule: 'format' })
  .message('Invalid email format provided');
```

### Result Pattern Compatibility

The fluent result objects are fully compatible with TypeScript's discriminated union narrowing:

```typescript
const result = await userService.createUser(ctx, userData);

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

When `ctx.err(code, details)` is called, the error code is automatically written to the `attr_errorCode` column. This
enables efficient querying of errors by code:

```sql
SELECT * FROM traces
WHERE entry_type = 'span-err'
  AND attr_errorCode = 'VALIDATION_FAILED';
```

### Message Storage

Both `.message()` methods write to the unified `message` column, enabling message-based queries:

```sql
SELECT span_name, message, attr_errorCode
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
// Values like userId are written to their own attr_* columns
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

#### Span Attribute Pattern (ctx.tag)

**Note**: `ctx.tag` does NOT create a new entry. It updates row 0 (span-start row) directly.

```typescript
// ctx.tag writes directly to row 0's attribute columns
// No new entry type - just updating the span-start row
function writeTagAttribute(buffer: SpanBuffer, columnName: string, value: any) {
  // ALWAYS write to row 0 (span-start row) - overwrite semantics
  buffer[`attr_${columnName}`][0] = value;
  buffer[`attr_${columnName}_nulls`][0] |= 1; // Mark as valid
}

// Usage: ctx.tag.userId('123') → writeTagAttribute(buffer, 'userId', '123')
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

  // Context flows into regular attribute columns
  // TODO: Actual column writing API design TBD
  for (const [key, value] of Object.entries(context)) {
    const writerMethod = `write${capitalize(key)}`;
    if (writers[writerMethod]) {
      writers[writerMethod](value);
    }
  }
}
```

## Span Duration Calculation

The fixed row layout guarantees that duration is always computable:

```typescript
// Duration = timestamps[1] - timestamps[0]
// Works for ALL completion types: span-ok, span-err, AND span-exception
function getSpanDuration(buffer: SpanBuffer): number {
  return buffer.timestamps[1] - buffer.timestamps[0];
}
```

**Why This Works**:

- Row 0 (span-start) timestamp set at span creation
- Row 1 timestamp set at completion (ctx.ok/ctx.err) OR remains at creation time (exception)
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

### The `message` Column by Entry Type

| Entry Type                                            | `message` Contains                                      |
| ----------------------------------------------------- | ------------------------------------------------------- |
| `span-start`, `span-ok`, `span-err`, `span-exception` | Span name (e.g., `'create-user'`)                       |
| `info`, `debug`, `warn`, `error`                      | Log message template (e.g., `'User ${userId} created'`) |
| `ff-access`, `ff-usage`                               | Flag name (e.g., `'advancedValidation'`, `'darkMode'`)  |

### Fixed Row Constraints

- **Row 0**: Always `span-start` - created at span initialization
- **Row 1**: Always completion type (`span-ok`, `span-err`, or `span-exception`) - pre-initialized as `span-exception`
- **Row 2+**: Event entries (`info`, `debug`, `warn`, `error`, `ff-access`, `ff-usage`)

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

- **Pre-generated writers**: Column writers generated at task creation time
- **No conditionals**: Entry type determines exact code path
- **Direct memory writes**: No intermediate objects or transformations

## Integration Points

This entry type system integrates with:

- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Entry types become the `entry_type` column
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Generated APIs use these primitives
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Column writers populate buffer arrays
- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Task lifecycle creates span entry types

The entry type system provides the foundational vocabulary for all trace events, ensuring consistency and performance
across the entire logging system.
