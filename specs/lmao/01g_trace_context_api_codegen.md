# Trace Context API Codegen

## Overview

The trace context API provides a clean, type-safe interface for logging structured data while generating highly
optimized column writers at runtime. This document details how the codegen transforms schema definitions into efficient
APIs.

## Design Philosophy

**Key Insight**: All trace entries (`tag`, `info`, `debug`, `warn`, `error`) follow the same pattern - they write
structured data with an entry type. The codegen unifies this into a single system that generates both object-based and
property-based APIs.

**Core Principles**:

- **Unified backend**: All entry types use the same generated column writers
- **Dual API patterns**: Support both `tag({ key: value })` and `tag.key(value)`
- **Zero runtime overhead**: All expensive work happens at op definition time
- **Type safety**: Full TypeScript support with schema-driven types
- **Destructured context**: Context properties available via destructuring in op signatures

## Destructured Context Pattern

The context is destructured directly in op signatures, eliminating `ctx.xxx` drilling:

```typescript
const GET = op(async ({ span, log, tag, deps, ff, env, scope }, url: string) => {
  // All context properties available via destructuring

  // Logging
  log.info('Starting request');

  // Span attributes (writes to row 0)
  tag.method('GET').url(url);

  // Scope (propagates to all entries)
  scope({ requestId: req.id });

  // Feature flags
  const { premiumFeatures } = ff;

  // Call other ops via span()
  await span('fetch', fetchOp, url);

  // Deps can be destructured too
  const { retry, auth } = deps;
  await span('auth', auth, token);
});
```

### Context Properties Available via Destructuring

| Property | Purpose                                       |
| -------- | --------------------------------------------- |
| `span`   | Invoke other ops: `span('name', op, ...args)` |
| `log`    | Logging: `log.info()`, `log.warn()`, etc.     |
| `tag`    | Span attributes: `tag.method('GET')`          |
| `deps`   | Dependencies (Op instances)                   |
| `ff`     | Feature flags                                 |
| `env`    | Environment config                            |
| `scope`  | Scoped attributes                             |
| `ok`     | Success result helper                         |
| `err`    | Error result helper                           |

## API Patterns

### Object-Based API (Primary)

```typescript
const processUser = op(async ({ log, tag }, userData) => {
  // Console.log operations with optional structured data
  log.info('Starting user registration');
  log.info('User validation complete').with({ userId: '123', duration: 45.2 });
  log.debug('Database query').with({ query: 'SELECT * FROM users', rows: 5 });
  log.error('Connection failed').with({ host: 'db.example.com', retries: 3 });

  // Tag operations with structured data
  tag({ userId: '123', requestId: 'req_456' });
  tag({ httpStatus: 200, duration: 45.2, cacheHit: true });
});
```

### Property-Based API (Alternative)

```typescript
const processUser = op(async ({ tag }, userData) => {
  // Individual property setters (generated from schema)
  tag.userId('123');
  tag.httpStatus(200);
  tag.duration(45.2);

  // Can be chained
  tag.userId('123').httpStatus(200).duration(45.2);
});
```

## Codegen Architecture

### 1. Schema-Driven Column Writers

The codegen starts with attribute schema definitions and generates low-level column writers:

```typescript
// Input: Schema definition
interface HttpLibrarySchema {
  httpStatus: number;
  httpMethod: string;
  httpUrl: string;
  httpDuration: number;
}

// Generated: Low-level column writers
interface GeneratedColumnWriters {
  writeHttpStatus(entryType: EntryType, value: number): void;
  writeHttpMethod(entryType: EntryType, value: string): void;
  writeHttpUrl(entryType: EntryType, value: string): void;
  writeHttpDuration(entryType: EntryType, value: number): void;
}
```

### 2. Entry-Type-Bound API Generation

Each entry type gets bound versions of the column writers:

```typescript
// Generated for tag API - writes directly to row 0 (span-start row)
// Note: tag does NOT create a separate entry type - it updates span-start attributes
class TagAPI {
  constructor(private buffer: SpanBuffer) {}

  // Object-based API - writes to row 0
  (attributes: Partial<HttpLibrarySchema>): this {
    for (const [key, value] of Object.entries(attributes)) {
      this.buffer[`${key}_values`][0] = value;  // Always row 0
    }
    return this;
  }

  // Property-based API (generated for each schema property)
  httpStatus(value: number): this {
    this.buffer.httpStatus_values[0] = value;  // Always row 0
    return this;
  }

  httpMethod(value: string): this {
    this.buffer.httpMethod_values[0] = value;  // Always row 0
    return this;
  }

  // ... etc for all schema properties
}

class LogAPI {
  constructor(private buffer: SpanBuffer) {}

  // Console.log compatible with fluent .with() for attributes
  info(message: string): FluentLog {
    this._writeLogEntry('info', message);
    return this;
  }

  debug(message: string): FluentLog {
    this._writeLogEntry('debug', message);
    return this;
  }

  warn(message: string): FluentLog {
    this._writeLogEntry('warn', message);
    return this;
  }

  error(message: string): FluentLog {
    this._writeLogEntry('error', message);
    return this;
  }

  with(attributes: Partial<HttpLibrarySchema>): this {
    for (const [key, value] of Object.entries(attributes)) {
      this.writers[`write${capitalize(key)}`](value);
    }
    return this;
  }
}
```

### 3. Context Assembly

The trace context assembles all entry type APIs for destructuring:

```typescript
// Generated trace context (destructurable)
// Extra is spread in for user-defined fields (e.g., env with CF Worker bindings)
type SpanContext<Schema, Deps, FF, Extra = {}> = {
  readonly tag: TagAPI<Schema>;
  readonly log: LogAPI<Schema>;
  readonly scope: ScopeAPI<Schema>;
  readonly span: SpanFn<SpanContext<Schema, Deps, FF, Extra>>;
  readonly deps: Deps;
  readonly ff: FeatureFlagEvaluator<FF>;
  readonly ok: <V>(value: V) => FluentSuccessResult<V>;
  readonly err: <E>(code: string, details?: E) => FluentErrorResult<E>;
} & Extra;

// Usage in op signature - destructure what you need
const processUser = op(async ({ log, tag, scope, span, ok, err }, userData) => {
  // Direct access to all context properties
});
```

## Scope API Code Generation

### scope() as Function (NOT stored in buffer columns)

The `scope()` function is a `new Function()` compiled callable that is **SEPARATE from buffer columns**. Scope values
are stored as plain JavaScript properties in the Scope instance, not in TypedArrays.

This separation enables:

1. **Zero allocation on scope set** - just sets a property
2. **SIMD pre-filling** - child spans can use TypedArray.fill() with parent scope values
3. **Clean inheritance** - \_getScopeValues() extracts all values for copying to child spans

```typescript
// Generated at module creation time (cold path)
function generateScopeFunction(schema: Schema): ScopeFunction {
  // The scope function accepts an object of attributes
  // and stores them for propagation to all entries

  return function scope(attributes: Partial<Schema>) {
    for (const [key, value] of Object.entries(attributes)) {
      this._scopeValues[key] = value;
    }
  };
}

// Example usage in op:
const handleRequest = op(async ({ scope, log }, req) => {
  // Set scoped attributes via function call
  scope({ requestId: req.id, userId: req.userId });

  log.info('Processing'); // Includes requestId, userId
});
```

**Key Design Point**: The Scope function does NOT reference the buffer at all. Scope values are applied to buffer
columns during write operations or pre-fill operations, NOT stored in the buffer. See 01i_span_scope_attributes.md for
the full scope lifecycle.

### Usage Pattern

```typescript
const handleRequest = op(async ({ scope, log, span }, req) => {
  // Setting scope values (no allocation)
  scope({ userId: 'user-123', requestId: 'req-456' });

  // All log entries include scoped attributes
  log.info('Processing request'); // Includes userId, requestId

  // Child spans inherit scope values
  await span('process-order', processOrder, order);
});

// Child op - inherits scope from parent
const processOrder = op(async ({ scope, log }, order) => {
  // Inherited: userId, requestId from parent scope
  scope({ orderId: order.id }); // Add to scope

  log.info('Processing order'); // Includes userId, requestId, orderId
});
```

### Key Differences from tag

| Aspect     | tag                                                           | scope                                     |
| ---------- | ------------------------------------------------------------- | ----------------------------------------- |
| API Style  | Method calls: `tag.userId('123')` or `tag({ userId: '123' })` | Function call: `scope({ userId: '123' })` |
| Can Read   | No                                                            | No (write-only)                           |
| Allocates  | Yes (on first write)                                          | No (first assignment)                     |
| Appears on | Row 0 only                                                    | ALL rows                                  |
| Inherited  | No                                                            | Yes                                       |

## Cold Path vs Hot Path Optimization

### Cold Path (Op Definition Time)

All expensive operations happen once when the op is defined:

```typescript
// Cold path: Generate optimized functions
function createOp(schema: Schema) {
  // 1. Generate column writers using new Function()
  const writers = generateColumnWriters(schema);

  // 2. Create bound entry type API instances
  const contextFactory = createContextFactory(writers);

  // 3. Return optimized op function
  return (userFunction) => {
    return async (...args) => {
      const context = contextFactory();
      // Hot path starts here...
      return userFunction(context, ...args);
    };
  };
}
```

### Hot Path (Op Execution Time)

Zero overhead during op execution:

```typescript
// Hot path: Direct property access and function calls
const processUser = op(async ({ tag, log }, userData) => {
  tag({ userId: '123' }); // → Direct column write
  log.info('Processing user').with({ userId: '123' }); // → Direct column writes
});
```

## Type Safety Integration

### Schema-Driven Types

```typescript
// Schema definition with types
interface LibrarySchema {
  userId: string;
  httpStatus: number;
  duration: number;
  cacheHit: boolean;
}

// Generated TypeScript interfaces
interface TagAPI<T> {
  (attributes: Partial<T>): TagAPI<T>;
  userId(value: string): this;
  httpStatus(value: number): this;
  duration(value: number): this;
  cacheHit(value: boolean): this;
}

interface LogAPI<T> {
  info(message: string): FluentLog<T>;
  debug(message: string): FluentLog<T>;
  warn(message: string): FluentLog<T>;
  error(message: string): FluentLog<T>;
}

interface FluentLog<T> {
  with(attributes: Partial<T>): FluentLog<T>;
}
```

### Library Integration Pattern

Libraries define their schema using `defineModule` and get fully typed APIs:

> **See**: [Op Context Pattern](./01l_op_context_pattern.md) for the complete API design.

```typescript
// Library defines module with logSchema (unprefixed)
const httpLib = defineModule({
  metadata: { packageName: '@my-company/http', packagePath: 'src/index.ts', gitSha: 'abc123' },
  logSchema: {
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    url: S.text(),
    duration: S.number(),
  },
  deps: {},
  ff: {},
})
  .ctx<{ env: WorkerEnv }>({ env: null! })
  .make();

const { op } = httpLib;

// Usage is fully typed - library uses clean names
export const apiCall = op(async ({ tag, log, ok }) => {
  // TypeScript knows these properties exist and their types
  tag.method('GET').url('https://api.example.com');
  log.info('Making API call');
  tag.status(200).duration(45.2);
  return ok({ success: true });
});

// Consumer applies prefix at use time:
// const httpRoot = httpLib.prefix('http').use();
// await apiCall(httpRoot);
// Result: writes to http_status, http_method, http_url, http_duration columns
```

## Implementation Details (Design TBD)

### Function Generation Strategy

The codegen approach using `new Function()` is conceptual - the actual column writing API is still being designed:

```typescript
// PLACEHOLDER - actual implementation TBD
function generateColumnWriter(columnName: string, columnType: string): Function {
  // Generate optimized function code
  const functionCode = `
    return function write${capitalize(columnName)}(entryType, value) {
      const rowIndex = this.currentRowIndex;
      this.columns.${columnName}[rowIndex] = value;
      this.columns.entry_type[rowIndex] = entryType;
      this.nullBitmaps.${columnName}[rowIndex] = false;
    };
  `;

  return new Function(functionCode)();
}
```

### Object API Implementation

The object-based API approach is conceptual - actual implementation TBD:

```typescript
// PLACEHOLDER - actual implementation TBD
function generateObjectAPI(schema: Schema): Function {
  const writerCalls = Object.keys(schema)
    .map((key) => `if ('${key}' in attributes) this.writers.write${capitalize(key)}(entryType, attributes.${key});`)
    .join('\n');

  const functionCode = `
    return function(attributes) {
      const entryType = this.entryType;
      ${writerCalls}
    };
  `;

  return new Function(functionCode)();
}
```

## Performance Characteristics

### Memory Efficiency

- **Pre-allocated TypedArrays**: All column storage allocated upfront
- **Null bitmaps**: Efficient sparse data handling
- **Zero allocations**: No object creation during hot path execution

### CPU Efficiency

- **Direct property access**: No dynamic property lookup
- **Inlined operations**: Function calls optimized away by V8
- **Batch writes**: Multiple attributes written in single call

### Type System Integration

- **Compile-time checking**: All attribute names and types validated
- **IDE support**: Full autocomplete and error detection
- **Runtime safety**: Type mismatches caught during development

## Feature Flag Entry Types

Feature flag entry types (`ff-access` and `ff-usage`) are handled automatically by the `FeatureFlagEvaluator` using
internal methods on `SpanLogger`:

```typescript
const processUser = op(async ({ ff }, userData) => {
  // Clean user API - no logging methods exposed
  if (ff.advancedValidation) {
    // Internally creates ff-access entry via log.ffAccess()
    // ...
  }

  // Creates ff-usage entry via log.ffUsage()
  ff.advancedValidation.track({ action: 'feature_used' });
});
```

### SpanLoggerInternal Type Pattern

**Public Type** (`SpanLogger<T>`) - what users see via `ctx.log`:

- Includes: `info()`, `debug()`, `warn()`, `error()`, `trace()`, `scope`, `_setScope()`
- Does NOT include: `ffAccess()`, `ffUsage()`, `_nextRow()`

**Internal Type** (`SpanLoggerInternal<T>`) - what evaluator uses:

- Extends `SpanLogger<T>` with internal methods:
  - `ffAccess(flagName: string, value: unknown): void`
  - `ffUsage(flagName: string, context?: Record<string, unknown>): FluentLogEntry<T>`
  - `_nextRow(): this` (renamed from `nextRow()` to avoid schema field conflicts)

**Implementation**:

```typescript
// Public type - users see this
export type SpanLogger<T extends LogSchema> = {
  info(message: string): FluentLogEntry<T>;
  debug(message: string): FluentLogEntry<T>;
  warn(message: string): FluentLogEntry<T>;
  error(message: string): FluentLogEntry<T>;
  trace(message: string): FluentLogEntry<T>;
  readonly scope: Readonly<Partial<InferSchema<T>>>;
  _setScope(attributes: Partial<InferSchema<T>>): void;
};

// Internal type - evaluator casts to this
export type SpanLoggerInternal<T extends LogSchema> = SpanLogger<T> & {
  ffAccess(flagName: string, value: unknown): void;
  ffUsage(flagName: string, context?: Record<string, unknown>): FluentLogEntry<T>;
};
```

**Generated Methods** (exist at runtime, not on public type):

```typescript
// Generated in SpanLogger class
_nextRow() {
  if (this._writeIndex >= this._buffer._capacity - 1) {
    this._buffer = this._getNextBuffer();
    this._writeIndex = -1;
    this._buffer._writeIndex = 0;
  }
  this._writeIndex++;
  this._buffer._writeIndex = this._writeIndex + 1;
  return this;
}

ffAccess(flagName, value) {
  this._nextRow(); // Internal method - handles overflow
  const idx = this._writeIndex;
  this._buffer._timestamps[idx] = helpers.getTimestampNanos();
  this._buffer._operations[idx] = ENTRY_TYPE_FF_ACCESS;
  if (this._buffer.message_values) {
    this._buffer.message_values[idx] = flagName;
  }
  if (this._buffer.ffValue_values) {
    const strValue = value === null || value === undefined ? 'null' : String(value);
    this._buffer.ffValue_values[idx] = strValue;
  }
  // Track write for capacity tuning
  this._buffer.module.sb_totalWrites++;
}

ffUsage(flagName, context) {
  this._nextRow(); // Internal method - handles overflow
  const idx = this._writeIndex;
  this._buffer._timestamps[idx] = helpers.getTimestampNanos();
  this._buffer._operations[idx] = ENTRY_TYPE_FF_USAGE;
  if (this._buffer.message_values) {
    this._buffer.message_values[idx] = flagName;
  }
  this._buffer.module.sb_totalWrites++;
  if (context) {
    this.with(context); // Apply to this ff-usage row
  }
  return this; // FluentLogEntry for further tagging
}
```

**Evaluator Usage**:

```typescript
// Evaluator casts to internal type to access FF methods
#getFlag(flagName) {
  const log = this.#spanContext.log as SpanLoggerInternal<T>;

  // Check if already logged (buffer chain scan)
  if (!this.#hasLoggedAccess(flagName)) {
    log.ffAccess(flagName, rawValue); // Available on internal type
  }

  return value;
}

track(flagName, context?) {
  const log = this.#spanContext.log as SpanLoggerInternal<T>;
  return log.ffUsage(flagName, context); // Always logged, not deduplicated
}
```

**Why This Pattern?**

- **Encapsulation**: Logger owns row lifecycle (`_nextRow()` + overflow handling)
- **Type safety**: Users can't accidentally call `ctx.log.ffAccess()` - TypeScript error
- **Zero allocation**: Methods exist at runtime, but hidden from public type
- **Fluent usage tagging**: `ffUsage()` returns `FluentLogEntry` for usage-row attributes
- **Reserved key protection**: `_nextRow()` uses underscore prefix to avoid conflicts with user schema fields

## Integration Points

This codegen system integrates with:

- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Uses the foundational entry
  type system and column writer patterns
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Generated APIs populate the final Arrow table structure
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Column writers populate buffer arrays
- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Op lifecycle integrates with generated APIs

## uint64 Method Generation

### Overview

The `.uint64(value: bigint)` method is generated on:

- **TagAPI** - for `tag.userId('123').uint64(n)`
- **SpanLogger** - for `log.info('msg').uint64(n)`
- **Result helpers** - for `ok().uint64(n)`, `err().uint64(n)`

### Generated Code Example

```typescript
// Generated on TagAPI, SpanLogger, and result helpers
uint64(value) {
  const idx = this._buffer._writeIndex - 1;  // Current entry
  this._buffer.uint64_value_nulls[Math.floor(idx / 8)] |= (1 << (idx % 8));
  this._buffer.uint64_value_values[idx] = value;
  return this;
}
```

### Usage Examples

```typescript
const processRecords = op(async ({ log, tag, ok }, records) => {
  // Attach large integer to tag entry
  tag.batchId(batchId).uint64(recordCount);

  // Attach to log entry
  log.info('Processing complete').uint64(bytesProcessed);

  // Attach to result
  return ok({ success: true }).uint64(totalRecords);
});
```

### Why uint64?

1. **Large integers**: JavaScript numbers lose precision above 2^53, BigInt handles full uint64 range
2. **Metrics reuse**: Same column used by internal metrics (`op-invocations`, etc.) and user code
3. **Lazy allocation**: `uint64_value` column only allocated when first written (most entries don't use it)

### Storage

| Property | Value                                               |
| -------- | --------------------------------------------------- |
| Column   | `uint64_value`                                      |
| Type     | `BigUint64Array`                                    |
| Lazy     | Yes (only allocated on first write)                 |
| Nullable | Yes (null bitmap tracks which entries have a value) |

## Integration with Arrow Tables

The generated column writers directly populate Arrow-compatible TypedArrays:

```typescript
// Arrow conversion uses EXISTING SpanBuffer TypedArrays directly (zero-copy)
const createArrowVectors = (spanBuffer: SpanBuffer) => {
  const writeIndex = spanBuffer.writeIndex;
  return {
    // Zero-copy: slice existing cache-aligned TypedArrays to actual write length
    timestamp: arrow.Float64Vector.from(spanBuffer.timestamp.slice(0, writeIndex)),

    // Span identification columns (separate columns, not Struct)
    // thread_id and span_id are constant per buffer, filled for each row
    thread_id: createUint64Column(spanBuffer.thread_id, writeIndex),
    span_id: createUint32Column(spanBuffer.span_id, writeIndex),
    parent_thread_id: spanBuffer.parent
      ? createUint64Column(spanBuffer.parent.thread_id, writeIndex)
      : createNullUint64Column(writeIndex),
    parent_span_id: spanBuffer.parent
      ? createUint32Column(spanBuffer.parent.span_id, writeIndex)
      : createNullUint32Column(writeIndex),

    entry_type: arrow.Utf8Vector.from(spanBuffer.entry_type.slice(0, writeIndex)),

    // Generated attribute columns - zero-copy from existing arrays
    http_status: arrow.Int32Vector.from(spanBuffer.httpStatus_values.slice(0, writeIndex)),
    http_method: arrow.Utf8Vector.from(spanBuffer.httpMethod_values.slice(0, writeIndex)),
    http_url: arrow.Utf8Vector.from(spanBuffer.httpUrl_values.slice(0, writeIndex)),
    http_duration: arrow.Float64Vector.from(spanBuffer.httpDuration_values.slice(0, writeIndex)),
  };
};
```

## Future Extensions

### Dynamic Schema Updates

- Hot-reload schema changes without restarting
- Schema evolution with version compatibility for stored traces
- Schema versioning and migration support

### Advanced Type Features

- Union types for polymorphic attributes
- Generic constraints for library composition
- Conditional types based on operation context

### Performance Optimizations

- SIMD operations for bulk attribute writes
- Memory pool management for large traces
- Compression-aware column layouts
