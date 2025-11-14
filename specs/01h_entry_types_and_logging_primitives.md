# Entry Types and Logging Primitives

## Design Philosophy

**Key Insight**: All trace events in the system are unified under a single entry type enum. Whether it's a span lifecycle event, console.log call, structured data tag, or feature flag evaluation - everything becomes a row in the trace with a specific entry type.

**Core Principles**:
- **Unified event model**: One enum covers all possible trace events
- **Zero ambiguity**: Each entry type has a precise, well-defined meaning
- **Performance first**: Entry types are enum-encoded for minimal overhead
- **Extensible**: New entry types can be added without breaking existing code

## Entry Type Definitions

The entry type system defines exactly what each row in a trace represents:

### Span Lifecycle Entry Types

Spans represent units of work with a clear beginning and end:

- **`span-start`** - Beginning of a work unit (span)
  - Created when entering a traced function or operation
  - Always paired with exactly one completion entry type
  - Contains span metadata (module, span_name, parent relationships)
  - Can optionally include structured attributes and message

- **`span-ok`** - Span completed with `return ctx.ok()`
  - Normal successful completion path
  - May contain result data in attribute columns
  - Indicates the span achieved its intended outcome

- **`span-err`** - Span completed with `return ctx.err()`
  - Expected error/failure completion path
  - May contain error details in attribute columns
  - Still considered "handled" - not an exception

- **`span-exception`** - Span threw unexpected exception
  - Unhandled exception that bypassed normal `ctx.ok()`/`ctx.err()` flow
  - Contains exception details in message column
  - Indicates truly exceptional circumstances

### Log Level Entry Types

Structured logging with message and optional typed attributes:

- **`info`** - Information messages with optional structured data
- **`debug`** - Debug messages with optional structured data
- **`warn`** - Warning messages with optional structured data
- **`error`** - Error messages with optional structured data

These entry types enable gradual migration from console.log by providing structured logging with the familiar log levels, but with typed attributes instead of just string concatenation.

### Structured Data Entry Types

- **`tag`** - General structured data logging during execution
  - Used for `ctx.tag({ key: value })` calls
  - Contains arbitrary attribute data
  - Can occur at any point during span execution

### Feature Flag Entry Types

- **`ff-access`** - When feature flags are evaluated
  - Logged automatically by `FeatureFlagEvaluator`
  - Contains flag name, evaluated value, and evaluation context
  - Tracks when decisions are made based on flags

- **`ff-usage`** - When flag-gated features are actually used
  - Logged when flag-controlled code paths execute
  - Contains usage context and outcome data
  - Tracks actual feature utilization vs just evaluation

## Fluent API Design

With unified tag attributes across all entry types, we use a fluent/chainable API pattern that can be mixed into different operations:

```typescript
// Fluent span completion API
return ctx.ok(result)
  .with({ userId: user.id, operation: 'CREATE' })
  .message("User created successfully");

return ctx.err('VALIDATION_FAILED')
  .with({ 
    field: 'email', 
    attemptedValue: userData.email,
    validationRule: 'unique_constraint' 
  })
  .message("Email validation failed");

// Fluent span creation API
const payment = await ctx.span('process-payment')
  .with({ paymentMethod: order.paymentMethod, amount: order.total })
  .message("Processing payment for order")
  .run(async (childCtx) => {
    // implementation
  });

// Fluent logging API
ctx.info("Processing user data")
  .with({ userId: user.id, operation: 'PROCESS' });

// Simple usage when no additional context needed
return ctx.ok(result);
return ctx.err('VALIDATION_FAILED');
ctx.info("Simple message");
```

This fluent pattern provides a consistent, composable API across all trace operations.

### SpanLogger Zero-Allocation Design

After exploring several approaches, we arrived at a zero-allocation design where the `SpanLogger` instance serves multiple roles:

#### Design Evolution and Trade-offs

**❌ Approach 1: Separate Builder Objects**
```typescript
// Creates new objects on every call - expensive!
info(message): FluentLogBuilder { return new FluentLogBuilder(...); }
tag: TagProxy { return createTagProxy(...); }
```
*Problem*: Object allocation on every logging call defeats performance goals.

**❌ Approach 2: Shared Objects with Buffer Swapping**
```typescript
// Single shared tag object, swap buffer per span
sharedTag.buffer = currentBuffer;
```
*Problem*: Async/promises would cause buffer conflicts between concurrent spans.

**❌ Approach 3: Runtime Proxy**
```typescript
tag = new Proxy(target, handler); // Dynamic property access
```
*Problem*: Proxy overhead on every property access contradicts zero-overhead goals.

**❌ Approach 4: Object Creation in Constructor**
```typescript
constructor(buffer) {
  this.tag = new CompiledTagOps(buffer); // One allocation per SpanLogger
}
```
*Problem*: Still allocating objects, even if only once per span.

**✅ Final Approach: Self-Reference Pattern with Tag Getter**
```typescript
class SpanLogger {
  constructor(buffer) {
    this.buffer = buffer;
  }
  
  // Tag getter creates new entry and returns this for chaining
  get tag() {
    this._writeTagEntry(); // Creates new tag entry
    return this; // Return for chaining with individual attribute methods
  }
  
  // Fluent methods return this for chaining
  info(message, attributes) { 
    this._writeLogEntry('info', message);
    if (attributes) {
      this._writeAttributes(attributes);
    }
    return this; // Same instance, zero allocation
  }
  
  with(attributes) {
    this._writeAttributes(attributes);
    return this; // Fluent chaining on same object
  }
  
  message(text) {
    this._writeMessage(text);
    return this;
  }
  
  // Tag methods compiled directly onto prototype at module context creation time
  userId(value) { 
    this.buffer.writeUserId(value);
    return this; // Even tag calls can chain!
  }
  
  httpStatus(value) {
    this.buffer.writeHttpStatus(value);
    return this;
  }
}
```

#### Key Insights

1. **Tag getter creates entries**: `get tag()` creates a new tag entry and returns `this` for chaining
2. **Zero allocation chaining**: All fluent methods return the same instance for continued chaining
3. **Prototype compilation**: Tag methods are compiled onto the SpanLogger prototype at module context creation time
4. **Module boundary safety**: Unknown columns written as null when called from deeper contexts
5. **Reserved method names**: `with`, `message`, `tag` are reserved and cannot be used as attribute column names
6. **Separate APIs**: `ctx.log` for explicit logging, `ctx.ok()`/`ctx.err()` for span completion

#### Usage Examples

```typescript
// Logging API - traditional pattern with optional attributes
ctx.log.info("Processing user", { userId: 123, step: 'validation' });
ctx.log.debug("Details", { step: 'validation' });

// Logging API - fluent chaining pattern (message already set)
ctx.log.info("Processing user").with({ userId: 123 });
ctx.log.debug("Details").with({ step: 'validation' });

// Tag API - getter creates entry, then chain individual attributes  
ctx.log.tag.userId(123).httpStatus(200).message("Tagged user data");

// Tag API - use .with() for bulk attributes
ctx.log.tag.with({ userId: 123, httpStatus: 200, operation: 'CREATE' })
  .message("Bulk tagged user data");

// Mixed usage - individual + bulk chaining
ctx.log.tag.userId(123).with({ httpStatus: 200 }).message("Mixed tagging");

// Context API - span completion with fluent chaining  
return ctx.ok(result)
  .with({ userId: user.id, operation: 'CREATE' })
  .message("User created successfully");

return ctx.err('VALIDATION_FAILED')
  .with({ field: 'email', rule: 'unique' })
  .message("Email validation failed");

// Child span creation with fluent setup
const payment = await ctx.span('process-payment')
  .with({ paymentMethod: order.paymentMethod, amount: order.total })
  .message("Processing payment for order")
  .run(async (childCtx) => {
    // Child span operations
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

The `SpanLogger` uses TypeScript generics to provide full type safety for attribute operations:

```typescript
// Reserved method names that cannot be used as attributes
type ReservedNames = 'with' | 'message' | 'tag' | 'info' | 'debug' | 'warn' | 'error';

// Utility type that prevents reserved names from being used as keys
type ValidAttributes<T> = {
  [K in keyof T]: K extends ReservedNames 
    ? never 
    : T[K]
} & Record<string, any>;

interface SpanLogger<TAttributes extends ValidAttributes<TAttributes> = {}> {
  // Traditional logging with attributes (reserved names prevented at schema level)
  info(message: string, attributes?: TAttributes): FluentAttributes<TAttributes>;
  debug(message: string, attributes?: TAttributes): FluentAttributes<TAttributes>;
  warn(message: string, attributes?: TAttributes): FluentAttributes<TAttributes>;
  error(message: string, attributes?: TAttributes): FluentAttributes<TAttributes>;
  
  // Tag getter creates new entry and returns fluent interface
  get tag(): FluentAttributes<TAttributes>;
}

type FluentAttributes<TAttributes extends ValidAttributes<TAttributes>> = {
  // Dynamically generate methods for each attribute key
  [K in keyof TAttributes]: (value: TAttributes[K]) => FluentAttributes<TAttributes>;
} & {
  // Fluent methods for all entry types
  with(attributes: TAttributes): FluentAttributes<TAttributes>;
  message(text: string): FluentAttributes<TAttributes>;
};
```

**Usage with Full Type Safety and Reserved Name Protection**:
```typescript
// Define attribute schema for module - reserved names cause compile errors here!
interface UserServiceAttributes {
  user_id: string;
  http_status: number;
  operation: 'CREATE' | 'UPDATE' | 'DELETE';
  email?: string;
  // with: string;          // ❌ Compile error: reserved name in schema
  // message: string;       // ❌ Compile error: reserved name in schema
}

// This would fail at the interface definition level:
// interface BadAttributes {
//   with: string;          // ❌ Error: Type 'string' is not assignable to type 'never'
//   message: number;       // ❌ Error: Type 'number' is not assignable to type 'never'  
// }

// Task context is typed with attribute schema
const createUser = task('create-user', async (
  ctx: { log: SpanLogger<UserServiceAttributes> }, 
  userData: UserData
) => {
  // TypeScript enforces attribute types and prevents reserved names
  ctx.log.info("Creating user", { 
    user_id: userData.id,    // ✅ string
    operation: 'CREATE',     // ✅ literal type
    http_status: 201,        // ✅ number
    // with: "invalid"       // ❌ TypeScript error: reserved name
    // message: "invalid"    // ❌ TypeScript error: reserved name
  });
  
  // All operations return FluentAttributes for consistent chaining
  ctx.log.info("Creating user", { user_id: userData.id })
    .with({ operation: 'CREATE' });  // ✅ Can chain .with() after info()
  
  // Tag methods are dynamically generated from UserServiceAttributes
  ctx.log.tag
    .user_id(userData.id)    // ✅ Generated: (value: string) => FluentAttributes
    .http_status(201)        // ✅ Generated: (value: number) => FluentAttributes  
    .operation('CREATE')     // ✅ Generated: (value: 'CREATE' | 'UPDATE' | 'DELETE') => FluentAttributes
    .message("User tagged"); // ✅ Built-in fluent method
  
  // .with() calls stay in the same fluent interface
  ctx.log.tag.with({
    user_id: userData.id,        // ✅ all properties typed
    operation: 'CREATE',
    email: userData.email,       // ✅ optional property
    // tag: "invalid"            // ❌ TypeScript error: reserved name
  }).message("Bulk tagged")      // ✅ Still FluentAttributes, can continue chaining
    .with({ http_status: 201 }); // ✅ Can chain more .with() calls
  
  // TypeScript knows exactly what methods are available:
  // ctx.log.tag.user_id      ✅ Available
  // ctx.log.tag.http_status  ✅ Available  
  // ctx.log.tag.operation    ✅ Available
  // ctx.log.tag.email        ✅ Available (optional)
  // ctx.log.tag.invalidProp  ❌ TypeScript error: doesn't exist
});
```

This provides complete compile-time safety while maintaining zero runtime overhead through prototype compilation.

### Reserved Method Names

The fluent API reserves certain method names that cannot be used as attribute column names:

- **`with`**: Used for bulk attribute setting
- **`message`**: Used for setting entry message text  
- **`tag`**: Used as the tag entry getter
- **`info`, `debug`, `warn`, `error`**: Used for log level methods

When defining attribute schemas, these names must be avoided to prevent conflicts with the fluent API methods.

### Public vs Low-Level API Separation

**Public Logging API** (`ctx.log`):
- `info()`, `debug()`, `warn()`, `error()` - structured logging
- `tag.method()` - structured attribute logging  
- `with()`, `message()` - fluent chaining

**Public Context API** (`ctx`):
- `ctx.ok()`, `ctx.err()` - span completion with fluent chaining
- `ctx.span()` - child span creation with fluent setup

**Low-Level Operations** (handled by context system):
- `span-ok`, `span-err`, `span-exception` entries created when functions return `ctx.ok()`, `ctx.err()`, or throw exceptions
- `span-start` entries created automatically when spans begin
- Direct column writing operations

The `ctx.log` API is for explicit logging during execution, while `ctx.ok()`/`ctx.err()` are for span completion. Both use the same underlying entry type system.

## Low-Level Logging API

The entry type system is implemented through low-level column writers that directly populate trace buffers:

### Column Writer Interface (Design TBD)

The exact API for writing to columns is still being designed. The examples below use a placeholder `writeColumnName()` pattern to illustrate the concepts, but the actual implementation will likely be much cleaner:

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
  writeSpanName(value: string): void;
  writeMessage(value: string | null): void;
  
  // Generated attribute columns (example - actual API TBD)
  writeHttpStatus(value: number): void;
  writeHttpMethod(value: string): void;
  writeUserId(value: string): void;
  // ... more generated based on schema
}
```

### Entry Type Creation Patterns (Conceptual)

Each entry type follows specific patterns for populating columns. The examples below use placeholder `writeColumnName()` calls to illustrate the concepts - the actual column writing API is still being designed:

#### Span Start Pattern
```typescript
function createSpanStart(
  module: string, 
  spanName: string, 
  parentSpanId?: bigint,
  attributes?: Record<string, any>,
  message?: string
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(generateSpanId());
  writers.writeParentSpanId(parentSpanId ?? null);
  writers.writeEntryType('span-start');
  writers.writeModule(module);
  writers.writeSpanName(spanName);
  writers.writeMessage(message ?? null);
  
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
function createSpanCompletion(
  entryType: 'span-ok' | 'span-err',
  result?: any,
  attributes?: Record<string, any>,
  message?: string
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(entryType);
  writers.writeModule(getCurrentModule());
  writers.writeSpanName(getCurrentSpanName());
  writers.writeMessage(message ?? null);
  
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
function createLogEntry(
  level: 'info' | 'debug' | 'warn' | 'error', 
  message: string, 
  attributes?: Record<string, any>
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(level);
  writers.writeModule(getCurrentModule());
  writers.writeSpanName(getCurrentSpanName());
  writers.writeMessage(message);
  
  // Optional structured attributes
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

#### Tag Pattern
```typescript
function createTagEntry(attributes: Record<string, any>) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType('tag');
  writers.writeModule(getCurrentModule());
  writers.writeSpanName(getCurrentSpanName());
  writers.writeMessage(null);
  
  // Populate attribute columns based on provided data
  // TODO: Actual column writing API design TBD
  for (const [key, value] of Object.entries(attributes)) {
    const writerMethod = `write${capitalize(key)}`;
    if (writers[writerMethod]) {
      writers[writerMethod](value);
    }
  }
}
```

#### Feature Flag Pattern
```typescript
function createFeatureFlagEntry(
  entryType: 'ff-access' | 'ff-usage',
  flagName: string,
  flagValue: boolean,
  context: Record<string, any>
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(entryType);
  writers.writeModule(getCurrentModule());
  writers.writeSpanName(getCurrentSpanName());
  writers.writeMessage(null);
  
  // Feature flag specific columns
  writers.writeFfName(flagName);
  writers.writeFfValue(flagValue);
  
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

## Entry Type Validation

The system enforces entry type constraints at the API level:

### Required Columns by Entry Type

- **All entry types**: `timestamp`, `trace_id`, `span_id`, `entry_type`, `module`
- **Span lifecycle**: `span_name` required, `message` and attributes optional
- **Log level types**: `message` required, attributes optional
- **Feature flag types**: `ff_name`, `ff_value` required
- **Tag entries**: At least one attribute column must be populated

### Forbidden Combinations

- **Span completion without start**: `span-ok`/`span-err`/`span-exception` must have matching `span-start`
- **Orphaned spans**: All spans except root must have valid `parent_span_id`
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

The entry type system provides the foundational vocabulary for all trace events, ensuring consistency and performance across the entire logging system. 
