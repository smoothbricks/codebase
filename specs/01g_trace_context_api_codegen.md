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
- **Dual API patterns**: Support both `ctx.tag({ key: value })` and `ctx.tag.key(value)`
- **Zero runtime overhead**: All expensive work happens at task creation time
- **Type safety**: Full TypeScript support with schema-driven types

## API Patterns

### Object-Based API (Primary)

```typescript
// Console.log operations with optional structured data
ctx.info('Starting user registration');
ctx.info('User validation complete', { userId: '123', duration: 45.2 });
ctx.debug('Database query', { query: 'SELECT * FROM users', rows: 5 });
ctx.error('Connection failed', { host: 'db.example.com', retries: 3 });

// Tag operations with structured data
ctx.tag({ userId: '123', requestId: 'req_456' });
ctx.tag({ httpStatus: 200, duration: 45.2, cacheHit: true });
```

### Property-Based API (Alternative)

```typescript
// Individual property setters (generated from schema)
ctx.tag.userId('123');
ctx.tag.httpStatus(200);
ctx.tag.duration(45.2);

// Can be chained
ctx.tag.userId('123').httpStatus(200).duration(45.2);
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
// Generated for each entry type
class TagAPI {
  constructor(private writers: GeneratedColumnWriters) {}

  // Object-based API
  (attributes: Partial<HttpLibrarySchema>): void {
    for (const [key, value] of Object.entries(attributes)) {
      this.writers[`write${capitalize(key)}`]('tag', value);
    }
  }

  // Property-based API (generated for each schema property)
  httpStatus(value: number): this {
    this.writers.writeHttpStatus('tag', value);
    return this;
  }

  httpMethod(value: string): this {
    this.writers.writeHttpMethod('tag', value);
    return this;
  }

  // ... etc for all schema properties
}

class InfoAPI {
  constructor(private writers: GeneratedColumnWriters) {}

  // Console.log compatible with optional attributes
  (message: string, attributes?: Partial<HttpLibrarySchema>): void {
    this.writers.writeMessage('info', message);
    if (attributes) {
      for (const [key, value] of Object.entries(attributes)) {
        this.writers[`write${capitalize(key)}`]('info', value);
      }
    }
  }
}
```

### 3. Context Assembly

The trace context assembles all entry type APIs:

```typescript
// Generated trace context
class TraceContext {
  public readonly tag: TagAPI;
  public readonly scope: ScopeAPI; // Compiled class with getters/setters
  public readonly info: InfoAPI;
  public readonly debug: DebugAPI;
  public readonly warn: WarnAPI;
  public readonly error: ErrorAPI;

  constructor(buffer: SpanBuffer, writers: GeneratedColumnWriters) {
    this.tag = new TagAPI(writers);
    this.scope = new ScopeAPI(buffer); // Scope needs buffer reference
    this.info = new InfoAPI(writers);
    this.debug = new DebugAPI(writers);
    this.warn = new WarnAPI(writers);
    this.error = new ErrorAPI(writers);
  }
}
```

## Scope API Code Generation

### ctx.scope as Compiled Class with Getters/Setters

The `ctx.scope` API is a `new Function()` compiled class (like ctx.tag) that provides getters and setters for each
schema attribute. This allows user code to both read and write scope values.

```typescript
// Generated at module creation time (cold path)
function generateScopeClass(schema: Schema): typeof ScopeAPI {
  const getterSetterCode = Object.keys(schema)
    .map(
      (key) => `
    get ${key}() {
      return this.buffer.attr_${key}.scopeValue;
    }
    set ${key}(value) {
      this.buffer.attr_${key}.setScope(value);
    }
  `
    )
    .join('\n');

  const classCode = `
    return class GeneratedScope {
      constructor(buffer) {
        this.buffer = buffer;
      }
      ${getterSetterCode}
    };
  `;

  return new Function(classCode)();
}

// Example generated class for a schema with userId and requestId:
class GeneratedScope {
  constructor(private buffer: SpanBuffer) {}

  get userId(): string | undefined {
    return this.buffer.attr_userId.scopeValue as string | undefined;
  }

  set userId(value: string) {
    this.buffer.attr_userId.setScope(value);
  }

  get requestId(): string | undefined {
    return this.buffer.attr_requestId.scopeValue as string | undefined;
  }

  set requestId(value: string) {
    this.buffer.attr_requestId.setScope(value);
  }
}
```

### Usage Pattern

```typescript
// Setting scope values (no allocation)
ctx.scope.userId = 'user-123';
ctx.scope.requestId = 'req-456';

// Reading scope values (including inherited from parent)
const currentUserId = ctx.scope.userId;
if (ctx.scope.requestId !== undefined) {
  console.log('Request ID is set:', ctx.scope.requestId);
}

// Child spans inherit scope values
await ctx.span('child-operation', async (childCtx) => {
  console.log(childCtx.scope.userId); // 'user-123' (inherited)
  childCtx.scope.orderId = 'ord-789'; // Add to scope
});
```

### Key Differences from ctx.tag

| Aspect     | ctx.tag                               | ctx.scope                                       |
| ---------- | ------------------------------------- | ----------------------------------------------- |
| API Style  | Method calls: `ctx.tag.userId('123')` | Property assignment: `ctx.scope.userId = '123'` |
| Can Read   | No                                    | Yes (via getter)                                |
| Allocates  | Yes (on first write)                  | No (first assignment)                           |
| Appears on | Row 0 only                            | ALL rows                                        |
| Inherited  | No                                    | Yes                                             |

## Cold Path vs Hot Path Optimization

### Cold Path (Task Creation Time)

All expensive operations happen once when the task is created:

```typescript
// Cold path: Generate optimized functions
function createTask(schema: Schema) {
  // 1. Generate column writers using new Function()
  const writers = generateColumnWriters(schema);

  // 2. Create bound entry type API instances
  const context = new TraceContext(writers);

  // 3. Return optimized task function
  return (userFunction) => {
    return async (...args) => {
      // Hot path starts here...
      return userFunction(context, ...args);
    };
  };
}
```

### Hot Path (Task Execution Time)

Zero overhead during task execution:

```typescript
// Hot path: Direct property access and function calls
ctx.tag({ userId: '123' }); // → Direct column write
ctx.info('Processing user', { userId: '123' }); // → Direct column writes
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
interface TagAPI {
  (attributes: Partial<LibrarySchema>): void;
  userId(value: string): this;
  httpStatus(value: number): this;
  duration(value: number): this;
  cacheHit(value: boolean): this;
}

interface InfoAPI {
  (message: string, attributes?: Partial<LibrarySchema>): void;
}
```

### Library Integration Pattern

Libraries define their schema and get fully typed APIs:

```typescript
// Library defines schema
const httpLibrary = createLibrary('http', {
  httpStatus: 'number',
  httpMethod: 'string',
  httpUrl: 'string',
  httpDuration: 'number',
});

// Usage is fully typed
const httpTask = httpLibrary.task('api-call', async (ctx) => {
  // TypeScript knows these properties exist and their types
  ctx.tag({ httpMethod: 'GET', httpUrl: 'https://api.example.com' });
  ctx.info('Making API call', { httpMethod: 'GET' });

  // Property-based API is also typed
  ctx.tag.httpStatus(200).httpDuration(45.2);
});
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
low-level column writers:

```typescript
// Clean user API - no logging methods exposed
if (ctx.ff.advancedValidation) {
  // Internally creates ff-access entry
  // ...
}

// Creates ff-usage entry
ctx.ff.trackUsage('advancedValidation', { action: 'validation_performed' });
```

**Implementation Notes** (design TBD):

- **Low-level access**: `FeatureFlagEvaluator` uses same column writers as codegen system
- **Generic typing**: Column writers likely parameterized by attribute schema
- **Clean separation**: User API stays clean, logging is internal implementation detail
- **Unified backend**: Same zero-overhead approach as other entry types

## Integration Points

This codegen system integrates with:

- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Uses the foundational entry
  type system and column writer patterns
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Generated APIs populate the final Arrow table structure
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Column writers populate buffer arrays
- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Task lifecycle integrates with
  generated APIs

## Integration with Arrow Tables

The generated column writers directly populate Arrow-compatible TypedArrays:

```typescript
// Arrow conversion uses EXISTING SpanBuffer TypedArrays directly (zero-copy)
const createArrowVectors = (spanBuffer: SpanBuffer) => {
  return {
    // Zero-copy: slice existing cache-aligned TypedArrays to actual write length
    timestamp: arrow.Float64Vector.from(spanBuffer.timestamps.slice(0, spanBuffer.writeIndex)),
    span_id: arrow.Int64Vector.from(generateSpanIds(spanBuffer)),
    entry_type: arrow.Utf8Vector.from(spanBuffer.operations.slice(0, spanBuffer.writeIndex)),

    // Generated attribute columns - zero-copy from existing arrays
    http_status: arrow.Int32Vector.from(spanBuffer.attr_http_status.slice(0, spanBuffer.writeIndex)),
    http_method: arrow.Utf8Vector.from(spanBuffer.attr_http_method.slice(0, spanBuffer.writeIndex)),
    http_url: arrow.Utf8Vector.from(spanBuffer.attr_http_url.slice(0, spanBuffer.writeIndex)),
    http_duration: arrow.Float64Vector.from(spanBuffer.attr_http_duration.slice(0, spanBuffer.writeIndex)),
  };
};
```

## Future Extensions

### Dynamic Schema Updates

- Hot-reload schema changes without restarting
- Backward compatibility with existing traces
- Schema versioning and migration support

### Advanced Type Features

- Union types for polymorphic attributes
- Generic constraints for library composition
- Conditional types based on operation context

### Performance Optimizations

- SIMD operations for bulk attribute writes
- Memory pool management for large traces
- Compression-aware column layouts
