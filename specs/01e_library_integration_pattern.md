# Library Integration Pattern

## Overview

The Library Integration Pattern enables third-party libraries to provide traced operations with clean APIs while
avoiding attribute name conflicts. It solves the challenge of:

1. **Clean library authoring**: Libraries write unprefixed code (`tag.status(200)`)
2. **Collision avoidance**: Final columns are prefixed (`http_status`, `db_status`)
3. **Zero hot path overhead**: Library writes directly to its own SpanBuffer with unprefixed columns
4. **Type safety**: Full TypeScript inference through explicit dependency injection
5. **Tree traversal compatibility**: RemappedBufferView allows Arrow conversion to access prefixed names

> **Note**: This spec describes the runtime mechanics of library integration. For the high-level API design
> (`defineModule`, `.prefix()`, `.use()`), see [Module Builder Pattern](./01l_module_builder_pattern.md).

## Design Philosophy

**Key Insight**: Library authors should write clean, domain-focused code without worrying about global naming conflicts.
The tracing system handles prefixing transparently while maintaining maximum performance.

**The Challenge**: Multiple libraries might define the same attribute names:

- HTTP library: `status: number` (HTTP status codes)
- Process library: `status: string` (process states like "running", "failed")
- Database library: `status: string` (connection states)

**The Solution**: Libraries create their own SpanBuffer with unprefixed schema, then register a RemappedBufferView with
the parent that maps prefixed names to unprefixed columns. This enables:

- **Hot path**: Direct TypedArray writes to unprefixed columns (zero overhead)
- **Cold path**: Arrow conversion uses RemappedBufferView to access columns via prefixed names

## Core Architecture: RemappedBufferView

### The Problem

When a library op creates a child span:

1. **Library's transformed code** writes to unprefixed columns: `buffer.status_values[0] = 200`
2. **Parent's tree traversal** (Arrow conversion) iterates with prefixed names from root schema:
   `getColumnIfAllocated('http_status')`
3. **Application** may have its own `status` column with different meaning

### The Solution: RemappedBufferView

Instead of trying to remap writes at runtime (which adds hot-path overhead), we:

1. **Library creates its own SpanBuffer** with unprefixed schema (`{ status, method }`)
2. **Library's buffer** is used for direct writes - transformed code works directly
3. **RemappedBufferView** wraps the library buffer, mapping prefixed -> unprefixed for tree traversal
4. **Parent's children array** contains the RemappedBufferView (not the raw buffer)

```
Application Root Buffer (schema: { userId, http_status, http_method, db_query })
│
├── _children[0]: RemappedBufferView
│   │   Maps: http_status → status, http_method → method
│   │
│   └── wraps: HTTP Library Buffer (schema: { status, method })
│               │
│               └── _children[0]: RemappedBufferView (for nested auth library)
│                       Maps: auth_token → token
│                       └── wraps: Auth Library Buffer (schema: { token })
│
└── _children[1]: App's own child span buffer (same schema as root)
```

### RemappedBufferView Implementation

Generated via `new Function()` at library composition time (cold path):

```typescript
function generateRemappedBufferViewClass(
  prefixToUnprefixedMapping: Record<string, string> // { 'http_status': 'status', ... }
): new (buffer: SpanBuffer) => SpanBuffer {
  const mappingCode = JSON.stringify(prefixToUnprefixedMapping);

  const code = `
    const mapping = ${mappingCode};
    return class RemappedBufferView {
      constructor(buffer) {
        this._buffer = buffer;
      }

      // Tree traversal (pass-through)
      get _children() { return this._buffer._children; }
      get _next() { return this._buffer._next; }

    // Row count
    get _writeIndex() { return this._buffer._writeIndex; }

    // System columns (NOT remapped - same in all buffers)
    get timestamp() { return this._buffer.timestamp; }
    get entry_type() { return this._buffer.entry_type; }
      get message_values() { return this._buffer.message_values; }
      get message_nulls() { return this._buffer.message_nulls; }
      get lineNumber_values() { return this._buffer.lineNumber_values; }
      get lineNumber_nulls() { return this._buffer.lineNumber_nulls; }
      get errorCode_values() { return this._buffer.errorCode_values; }
      get errorCode_nulls() { return this._buffer.errorCode_nulls; }
      get exceptionStack_values() { return this._buffer.exceptionStack_values; }
      get exceptionStack_nulls() { return this._buffer.exceptionStack_nulls; }
      get ffValue_values() { return this._buffer.ffValue_values; }
      get ffValue_nulls() { return this._buffer.ffValue_nulls; }

      // Identity (pass-through)
      get trace_id() { return this._buffer.trace_id; }
      get thread_id() { return this._buffer.thread_id; }
      get span_id() { return this._buffer.span_id; }
      get parentSpanId() { return this._buffer.parentSpanId; }
      get _identity() { return this._buffer._identity; }

      // Metadata (pass-through)
      get op() { return this._buffer.op; }

      // Remapped column access (for Arrow conversion iteration)
      getColumnIfAllocated(name) {
        const unprefixedName = mapping[name] ?? name;
        return this._buffer.getColumnIfAllocated(unprefixedName);
      }

      getNullsIfAllocated(name) {
        const unprefixedName = mapping[name] ?? name;
        return this._buffer.getNullsIfAllocated(unprefixedName);
      }
    }
  `;

  return new Function(code)();
}
```

### Why This Works

**Hot Path (library code execution)**:

- Library's buffer is the raw SpanBuffer with unprefixed columns
- Transformed code writes directly: `buffer.status_values[0] = 200`
- Zero overhead - direct TypedArray access

**Cold Path (Arrow conversion)**:

- Tree walker encounters RemappedBufferView in parent's `_children[]`
- Calls `view.getColumnIfAllocated('http_status')`
- RemappedBufferView maps to `buffer.getColumnIfAllocated('status')`
- Returns the actual TypedArray for that column

**Columns the library doesn't own** (e.g., `userId` from app schema):

- `view.getColumnIfAllocated('userId')` → `buffer.getColumnIfAllocated('userId')` → `undefined`
- Arrow conversion handles `undefined` as null column - correct behavior

## Op's Responsibility: Buffer Creation and Registration

**Critical Design Point**: The **Op's internal wrapper** (not `span()`) is responsible for buffer creation and
registration. `span()` just invokes the op and passes metadata (name, line number).

### What the Op Wrapper Does

```typescript
// Inside op's wrapper (conceptual):
async invoke(parentCtx, spanName, line, ...args) {
  // 1. Create SpanBuffer with module's UNPREFIXED schema
  const ownBuffer = createSpanBuffer(unprefixedSchema, callsite, trace_id);

  // 2. Register with parent - wrap with RemappedBufferView if prefixed
  if (prefix && parentCtx?.buffer) {
    const remappedView = new RemappedViewClass(ownBuffer);
    parentCtx.buffer._children.push(remappedView);
  } else if (parentCtx?.buffer) {
    parentCtx.buffer._children.push(ownBuffer);
  }

  // 3. Setup context with destructured helpers
  const ctx = {
    span: boundSpan,      // For child spans
    log: boundLog,        // For logging
    tag: tagProxy,        // For setting attributes
    deps: boundDeps,      // Pre-bound dependencies
  };

  // 4. Execute user function with try/catch for span-exception
  try {
    return await userFn(ctx, ...args);
  } catch (error) {
    // Write span-exception entry
    writeSpanException(ownBuffer, error);
    throw error;
  }
}
```

### Why Op Owns Buffer Creation

1. **Schema isolation**: Each library has its own schema - op knows its module's schema
2. **Prefix mapping**: Op knows if it's prefixed and can create RemappedBufferView
3. **Exception handling**: Op wrapper catches exceptions for proper span-exception logging
4. **Dependency injection**: Op wrapper binds deps with their contexts

### What span() Does (Minimal)

```typescript
// span() is just an invocation helper
await span('fetch-data', GET, 'https://api.example.com');

// Equivalent to:
await GET.invoke(currentCtx, 'fetch-data', __LINE__, 'https://api.example.com');
```

`span()` passes:

- Span name (for logging/tracing)
- The op to invoke
- Arguments for the op

It does **NOT**:

- Create buffers
- Register \_children
- Handle exceptions

## Library Definition with defineModule

Libraries use `defineModule()` to define their schema and dependencies:

```typescript
// @my-company/http-tracing/src/index.ts
import { defineModule, S } from '@smoothbricks/lmao';

// Define the library's schema (unprefixed)
export const httpModule = defineModule({
  metadata: {
    packageName: '@my-company/http-tracing',
    packagePath: 'src/index.ts',
  },
  schema: {
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
    url: S.text().masked('url'),
    duration: S.number(),
  },
  deps: {
    retry: retryModule, // Declare dependency on retry library
  },
});

// Get op factory from module
const { op } = httpModule;

// Define ops - clean unprefixed API with destructured context
export const GET = op(async ({ span, log, tag, deps }, url: string, options?: RequestInit) => {
  const startTime = performance.now();

  tag.method('GET');
  tag.url(url);

  // Destructure deps for ergonomics
  const { retry } = deps;

  try {
    const response = await fetch(url, options);
    tag.status(response.status);
    tag.duration(performance.now() - startTime);

    if (!response.ok && retry) {
      // Use dependency via span()
      await span('retry', retry.attempt, 1);
    }

    return response;
  } catch (error) {
    tag.status(0);
    tag.duration(performance.now() - startTime);
    throw error;
  }
});

export const POST = op(async ({ span, log, tag, deps }, url: string, body: unknown) => {
  // Similar implementation...
});
```

## Application Composition

Applications wire libraries with prefixes using the fluent API:

```typescript
// Application code
import { httpModule, GET, POST } from '@my-company/http-tracing';
import { dbModule, query } from '@my-company/db-tracing';
import { retryModule } from '@my-company/retry';

// Wire dependencies with prefixes
const httpRoot = httpModule.prefix('http').use({
  retry: retryModule.prefix('http_retry').use(),
});

const dbRoot = dbModule.prefix('db').use();

// Invoke ops via span()
const response = await httpRoot.span('fetch-users', GET, 'https://api.example.com/users');
const users = await dbRoot.span('load-users', query, 'SELECT * FROM users');
```

### Application Module with Dependencies

For applications that compose multiple libraries:

```typescript
import { defineModule, S } from '@smoothbricks/lmao';
import { httpModule, GET } from '@my-company/http-tracing';
import { dbModule, query } from '@my-company/db-tracing';

// Application defines its own schema and declares library dependencies
const appModule = defineModule({
  metadata: {
    gitSha: process.env.GIT_SHA,
    packageName: '@my-company/api-server',
    packagePath: 'src/app.ts',
  },
  schema: {
    userId: S.category(),
    requestId: S.category(),
    endpoint: S.category(),
  },
  deps: {
    http: httpModule,
    db: dbModule,
  },
});

const { op } = appModule;

export const handleRequest = op(async ({ span, log, tag, deps }, req: Request) => {
  tag.userId(req.userId);
  tag.requestId(req.id);
  tag.endpoint(req.path);

  // Destructure deps for ergonomics
  const { http, db } = deps;

  // Invoke library ops via span()
  const users = await span('query-users', db.query, 'SELECT * FROM users');
  const external = await span('fetch-external', http.GET, req.externalUrl);

  return { users, external };
});

// Wire all dependencies with prefixes
const appRoot = appModule.use({
  http: httpModule.prefix('http').use({
    retry: retryModule.prefix('http_retry').use(),
  }),
  db: dbModule.prefix('db').use(),
});

// Entry point
await appRoot.span('handle-request', handleRequest, incomingRequest);
```

## Shared Dependencies

Multiple consumers can share the same dependency instance:

```typescript
// GraphQL library needs HTTP internally
const graphqlModule = defineModule({
  metadata: { packageName: '@my-company/graphql', packagePath: 'src/index.ts' },
  schema: {
    query: S.text(),
    operationName: S.category(),
  },
  deps: {
    http: httpModule,
  },
});

// Application uses both GraphQL and HTTP directly
const appModule = defineModule({
  metadata: { ... },
  schema: { userId: S.category() },
  deps: {
    graphql: graphqlModule,
    http: httpModule,  // App also uses HTTP directly
  },
});

// Wire so GraphQL and App share the SAME http instance
const httpInstance = httpModule.prefix('http').use({
  retry: retryModule.prefix('http_retry').use(),
});

const appRoot = appModule.use({
  http: httpInstance,                              // App's direct HTTP
  graphql: graphqlModule.prefix('graphql').use({
    http: httpInstance,                            // GraphQL's HTTP = SAME instance!
  }),
});

// Result: Both write to http_status, http_method, etc.
// No duplicate graphql_http_status columns!
```

## Type Safety: Collision Detection

The pattern provides compile-time collision detection:

```typescript
// BAD PATTERN (spread) - SILENT COLLISION:
const { op } = createModuleContext({
  logSchema: {
    ...httpLib.logSchema, // http_status: number
    ...processLib.logSchema, // http_status: string -- SILENTLY OVERWRITES!
  },
});

// CORRECT PATTERN (defineModule) - COMPILE-TIME ERROR:
const appModule = defineModule({
  schema: { ... },
  deps: {
    http: httpModule,
    process: processModule,
  },
});

// When both use same prefix in .use(), TypeScript catches it:
appModule.use({
  http: httpModule.prefix('http').use(),
  process: processModule.prefix('http').use(), // TYPE ERROR: collision on http_status
});
```

## Performance Benefits

### Cold Path vs Hot Path Optimization

**Cold Path** (Library Composition Time - `.prefix().use()`):

- Schema analysis and prefix mapping
- RemappedBufferView class generation via `new Function()`
- One-time class compilation and V8 optimization
- Type inference and validation

**Hot Path** (Op Execution Time):

- Op creates its own SpanBuffer with unprefixed schema
- Transformed code writes directly to TypedArrays: `buffer.status_values[0] = 200`
- **Zero remapping overhead** - no proxy, no function calls for column access
- Optimal V8 optimization (hidden classes, inline caches)

### Why No Proxy?

Previous approaches considered using JavaScript Proxy for remapping. This was rejected because:

1. **Proxy traps on every property access** - hot path overhead
2. **Breaks V8 hidden class optimization** - Proxy objects have dynamic shape
3. **Unnecessary** - the RemappedBufferView is only needed for cold-path tree traversal

### Transformed Code Example

The lmao-transformer converts fluent tag calls to direct array writes:

```typescript
// Library source code (unprefixed):
tag.status(200);
tag.method('POST');

// Transformed (direct array access to unprefixed columns):
{
  buffer.status_nulls[0] |= 1;
  buffer.status_values[0] = 200;
  buffer.method_nulls[0] |= 1;
  buffer.method_values[0] = 0; // enum index
}
```

Since the op's buffer has unprefixed schema, these writes work directly.

### Arrow Conversion Example

When Arrow conversion walks the tree:

```typescript
// Root buffer's schema has prefixed columns
const schema = rootBuffer.op.module.logSchema;
// { userId, http_status, http_method, db_query, ... }

walkSpanTree(rootBuffer, (buffer) => {
  // For library's RemappedBufferView:
  // buffer.getColumnIfAllocated('http_status')
  //   → mapping['http_status'] = 'status'
  //   → ownBuffer.getColumnIfAllocated('status')
  //   → returns the actual TypedArray

  for (const [fieldName, _] of schemaFields) {
    const col = buffer.getColumnIfAllocated(fieldName);
    // col is undefined for columns this buffer doesn't own
    // col is the TypedArray for columns it does own
  }
});
```

## Arrow Table Output

The final Arrow table has clean, collision-free columns:

| Column             | Type                 | Description                                   | Source              |
| ------------------ | -------------------- | --------------------------------------------- | ------------------- |
| `timestamp`        | `timestamp[ns]`      | Event timestamp                               | Core system         |
| `trace_id`         | `dictionary<string>` | Trace identifier                              | Core system         |
| `thread_id`        | `uint64`             | Thread/worker identifier                      | Core system         |
| `span_id`          | `uint32`             | Unit of work within thread                    | Core system         |
| `parent_thread_id` | `uint64` (nullable)  | Parent span's thread                          | Core system         |
| `parent_span_id`   | `uint32` (nullable)  | Parent span's ID                              | Core system         |
| `entry_type`       | `dictionary<string>` | Log entry type                                | Core system         |
| `package_name`     | `dictionary<string>` | npm package name                              | Core system         |
| `package_path`     | `dictionary<string>` | Path within package, relative to package.json | Core system         |
| `message`          | `dictionary<string>` | Span name, log template, or flag name         | Core system         |
| `http_status`      | `uint16`             | HTTP status code                              | HTTP library        |
| `http_method`      | `dictionary<string>` | HTTP method                                   | HTTP library        |
| `http_url`         | `string`             | Masked URL                                    | HTTP library        |
| `http_duration`    | `float32`            | HTTP request duration                         | HTTP library        |
| `db_query`         | `string`             | Masked SQL query                              | Database library    |
| `db_duration`      | `float32`            | Query duration                                | Database library    |
| `db_rows`          | `uint32`             | Rows affected                                 | Database library    |
| `db_table`         | `dictionary<string>` | Table name                                    | Database library    |
| `redis_command`    | `dictionary<string>` | Redis command                                 | Redis library       |
| `redis_key`        | `string`             | Redis key                                     | Redis library       |
| `redis_duration`   | `float32`            | Redis operation duration                      | Redis library       |
| `user_id`          | `binary[8]`          | Hashed user ID                                | User-defined        |
| `business_metric`  | `float64`            | Custom metric                                 | User-defined        |
| `ff_value`         | `dictionary<string>` | Feature flag value (S.category)               | Feature flag system |

**Note**: Feature flag names are stored in the unified `message` column for `ff-access` and `ff-usage` entries.

See [Arrow Table Structure - Module Identification](./01f_arrow_table_structure.md#module-identification) for rationale
on the `package` and `package_path` column design.

### ClickHouse Query Examples

```sql
-- Analyze HTTP performance by endpoint
SELECT
  http_url,
  avg(http_duration) as avg_response_time,
  count(*) as request_count
FROM traces
WHERE http_method = 'POST'
  AND http_status >= 200
  AND http_status < 300
GROUP BY http_url
ORDER BY avg_response_time DESC;

-- Cross-service performance correlation
SELECT
  user_id,
  avg(http_duration) as avg_http_time,
  avg(db_duration) as avg_db_time,
  avg(redis_duration) as avg_redis_time
FROM traces
GROUP BY user_id
HAVING count(*) > 10;

-- Feature flag usage analysis
-- Note: flag name is in the unified `message` column
SELECT
  message as flag_name,
  count(*) as access_count,
  sum(if(ff_value = 'true', 1, 0)) as enabled_count
FROM traces
WHERE entry_type = 'ff-access'
GROUP BY message
ORDER BY access_count DESC;
```

## Integration with Core System

This pattern integrates with other components:

- **[Module Builder Pattern](./01l_module_builder_pattern.md)**: High-level API design (`defineModule`, `.prefix()`,
  `.use()`)
- **[Trace Schema System](./01a_trace_schema_system.md)**: Provides the schema definition and composition mechanisms
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generates prefixed TypedArray columns based
  on composed schemas
- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Implements the op wrapper pattern that
  libraries build upon
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Details the runtime code generation that creates
  the optimized library APIs

## Benefits Summary

1. **Library Author Experience**: Clean, domain-focused APIs without global naming concerns
2. **User Experience**: Simple composition with explicit dependency injection
3. **Performance**: Zero hot path overhead through compile-time optimization
4. **Type Safety**: Full TypeScript inference with compile-time collision detection
5. **Ecosystem**: Enables rich third-party library ecosystem for tracing
6. **Maintainability**: Modular pattern that scales to many libraries
7. **Query Performance**: Clean, optimized columnar data for analytics
8. **Shared Dependencies**: Multiple consumers can share a dependency instance

This pattern enables a rich ecosystem of traced libraries while maintaining the performance and type safety goals of the
overall system.
