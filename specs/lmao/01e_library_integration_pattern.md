# Library Integration Pattern

## Overview

The Library Integration Pattern enables third-party libraries to provide traced operations with clean APIs while
avoiding attribute name conflicts. It solves the challenge of:

1. **Clean library authoring**: Libraries write unprefixed code (`tag.status(200)`)
2. **Collision avoidance**: Final columns are prefixed or mapped (`http_status`, `db_status`)
3. **Zero hot path overhead**: Library writes directly to its own SpanBuffer with unprefixed columns
4. **Type safety**: Full TypeScript inference through explicit dependency injection
5. **Tree traversal compatibility**: RemappedBufferView allows Arrow conversion to access mapped names

> **Note**: This spec describes the runtime mechanics of library integration. For the high-level API design
> (`defineOpContext`, `.prefix()`, `.mapColumns()`), see [Op Context Pattern](./01l_op_context_pattern.md).

## Design Philosophy

**Key Insight**: Library authors should write clean, domain-focused code without worrying about global naming conflicts.
The tracing system handles prefixing/mapping transparently while maintaining maximum performance.

**The Challenge**: Multiple libraries might define the same attribute names:

- HTTP library: `status: number` (HTTP status codes)
- Process library: `status: string` (process states like "running", "failed")
- Database library: `status: string` (connection states)

**The Solution**: Libraries create their own SpanBuffer with unprefixed schema, then register a RemappedBufferView with
the parent that maps target names to unprefixed columns. This enables:

- **Hot path**: Direct TypedArray writes to unprefixed columns (zero overhead)
- **Cold path**: Arrow conversion uses RemappedBufferView to access columns via mapped names

## Column Mapping API

Applications control how library columns appear in the final Arrow output using two methods:

### `.prefix(name)` - Shorthand for Prefixing All Columns

Adds a prefix to all columns from the library:

```typescript
httpOps.prefix('http');
// status → http_status
// url → http_url
// method → http_method
```

### `.mapColumns({ ... })` - Explicit Column Mapping

Fine-grained control over column naming:

```typescript
postgresOps.mapColumns({
  // Renaming: library column → app column
  rows: 'pg_rows', // rows → pg_rows

  // Column sharing: multiple libs write to same column
  query: 'query', // Both pg and mysql write to 'query'

  // Column dropping: library writes are ignored
  _debug: null, // _debug writes discarded
});
```

### Mapping Capabilities

| Pattern                    | Effect                   | Use Case                          |
| -------------------------- | ------------------------ | --------------------------------- |
| `{ col: 'new_name' }`      | Rename column            | Avoid collisions, semantic naming |
| `{ col: 'shared_col' }`    | Share column across libs | Multiple DBs share `query` column |
| `{ col: null }`            | Drop column              | Ignore debug/internal writes      |
| `.prefix('x')` (shorthand) | All cols get `x_` prefix | Simple namespacing                |

## Core Architecture: RemappedBufferView

### The Problem

When a library op creates a child span:

1. **Library's transformed code** writes to unprefixed columns: `buffer.status_values[0] = 200`
2. **Parent's tree traversal** (Arrow conversion) iterates with mapped names from root schema:
   `getColumnIfAllocated('http_status')`
3. **Application** may have its own `status` column with different meaning

### The Solution: RemappedBufferView

Instead of trying to remap writes at runtime (which adds hot-path overhead), we:

1. **Library creates its own SpanBuffer** with unprefixed schema (`{ status, method }`)
2. **Library's buffer** is used for direct writes - transformed code works directly
3. **RemappedBufferView** wraps the library buffer, mapping app-column-name → library-column-name for tree traversal
4. **Parent's children array** contains the RemappedBufferView (not the raw buffer)

```
Application Root Buffer (schema: { userId, http_status, http_method, query, pg_rows })
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
├── _children[1]: RemappedBufferView
│   │   Maps: query → query, pg_rows → rows (shared 'query' column!)
│   │
│   └── wraps: Postgres Library Buffer (schema: { query, rows })
│
└── _children[2]: App's own child span buffer (same schema as root)
```

### RemappedBufferView Implementation

Generated via `new Function()` at library composition time (cold path):

```typescript
function generateRemappedBufferViewClass(
  appToLibraryMapping: Record<string, string | null> // { 'http_status': 'status', '_debug': null }
): new (buffer: SpanBuffer) => SpanBuffer {
  const mappingCode = JSON.stringify(appToLibraryMapping);

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
      get line_values() { return this._buffer.line_values; }
      get line_nulls() { return this._buffer.line_nulls; }
      get error_code_values() { return this._buffer.error_code_values; }
      get error_code_nulls() { return this._buffer.error_code_nulls; }
      get exception_stack_values() { return this._buffer.exception_stack_values; }
      get exception_stack_nulls() { return this._buffer.exception_stack_nulls; }
      get ff_value_values() { return this._buffer.ff_value_values; }
      get ff_value_nulls() { return this._buffer.ff_value_nulls; }
    
      // Identity (pass-through)
      get trace_id() { return this._buffer.trace_id; }
      get thread_id() { return this._buffer.thread_id; }
      get span_id() { return this._buffer.span_id; }
      get parent_span_id() { return this._buffer.parent_span_id; }
      get parent_thread_id() { return this._buffer.parent_thread_id; }
      get _identity() { return this._buffer._identity; }
    
      // Metadata (pass-through)
      get module() { return this._buffer._module; }
      get spanName() { return this._buffer.message_values[0]; }

      // Remapped column access (for Arrow conversion iteration)
      getColumnIfAllocated(name) {
        const libName = mapping[name];
        if (libName === null) return undefined; // Dropped column
        if (libName === undefined) return this._buffer.getColumnIfAllocated(name); // No mapping
        return this._buffer.getColumnIfAllocated(libName);
      }

      getNullsIfAllocated(name) {
        const libName = mapping[name];
        if (libName === null) return undefined; // Dropped column
        if (libName === undefined) return this._buffer.getNullsIfAllocated(name); // No mapping
        return this._buffer.getNullsIfAllocated(libName);
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

- `view.getColumnIfAllocated('userId')` → no mapping → `buffer.getColumnIfAllocated('userId')` → `undefined`
- Arrow conversion handles `undefined` as null column - correct behavior

**Dropped columns** (mapped to `null`):

- `view.getColumnIfAllocated('_debug')` → mapping returns `null` → returns `undefined`
- Library can write `tag._debug(...)` but it never appears in Arrow output

## Op's Responsibility: Buffer Creation and Registration

**Critical Design Point**: The **Op's internal wrapper** (not `span()`) is responsible for buffer creation and
registration. `span()` just invokes the op and passes metadata (name, line number).

### What the Op Wrapper Does

```typescript
// Inside op's wrapper (conceptual):
async invoke(parentCtx, spanName, line, ...args) {
  // 1. Create SpanBuffer with op's UNPREFIXED schema
  const ownBuffer = createSpanBuffer(unprefixedSchema, callsite, trace_id);

  // 2. Register with parent - wrap with RemappedBufferView if mapped/prefixed
  if (columnMapping && parentCtx?.buffer) {
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

1. **Schema isolation**: Each library has its own schema - op knows its context's schema
2. **Column mapping**: Op knows if it's mapped/prefixed and can create RemappedBufferView
3. **Exception handling**: Op wrapper catches exceptions for proper span-exception logging
4. **Dependency injection**: Op wrapper binds deps with their contexts

### What span() Does (Minimal)

```typescript
// span() is just an invocation helper
await ctx.span('fetch-data', ctx.deps.http.get, 'https://api.example.com');

// Equivalent to:
await ctx.deps.http.get.invoke(currentCtx, 'fetch-data', __LINE__, 'https://api.example.com');
```

`span()` passes:

- Span name (for logging/tracing)
- The op to invoke
- Arguments for the op

It does **NOT**:

- Create buffers
- Register \_children
- Handle exceptions

## Library Definition with defineOpContext

Libraries use `defineOpContext()` to define their schema and ops:

```typescript
// @my-company/http-tracing/src/index.ts
import { defineOpContext, defineLogSchema, S } from '@smoothbricks/lmao';

// Define the library's schema (unprefixed)
const httpSchema = defineLogSchema({
  status: S.number(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
  url: S.text().masked('url'),
  duration: S.number(),
});

// Create op context for this library
const { defineOp, defineOps, createTrace, logSchema, flags } = defineOpContext({
  logSchema: httpSchema,
  deps: {
    retry: retryOps, // Declare dependency (wiring happens at app level)
  },
  ctx: {
    timeout: 30000,
  },
});

// Define ops - clean unprefixed API with destructured context
const get = defineOp('get', async (ctx, url: string, options?: RequestInit) => {
  const startTime = performance.now();

  ctx.tag.method('GET').url(url);

  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(ctx.timeout), ...options });
    ctx.tag.status(response.status).duration(performance.now() - startTime);

    if (!response.ok && ctx.deps.retry) {
      // Use dependency via span()
      await ctx.span('retry', ctx.deps.retry.attempt, 1);
    }

    return response.ok ? ctx.ok(response) : ctx.err('HTTP_ERROR', { status: response.status });
  } catch (error) {
    ctx.tag.status(0).duration(performance.now() - startTime);
    throw error;
  }
});

const post = defineOp('post', async (ctx, url: string, body: unknown) => {
  // Similar implementation...
});

// Export as ops group for consumers
export const httpOps = defineOps({ get, post, put, delete: del });
```

## Application Composition

Applications wire libraries with column mapping using the fluent API:

```typescript
// Application code
import { defineOpContext, defineLogSchema, S } from '@smoothbricks/lmao';
import { httpOps } from '@my-company/http-tracing';
import { postgresOps } from '@my-company/postgres-tracing';
import { mysqlOps } from '@my-company/mysql-tracing';

// Application defines its own schema
const appSchema = defineLogSchema({
  user_id: S.category(),
  request_id: S.category(),
  endpoint: S.category(),
});

// Wire dependencies with column mapping
const { defineOp, defineOps, createTrace, logSchema, flags } = defineOpContext({
  logSchema: appSchema,
  deps: {
    // Simple prefixing
    http: httpOps.prefix('http'),

    // Explicit column mapping - pg and mysql share 'query' column
    pg: postgresOps.mapColumns({
      query: 'query', // Shared column
      rows: 'pg_rows', // Renamed
      _debug: null, // Dropped
    }),
    mysql: mysqlOps.mapColumns({
      query: 'query', // Same shared column!
      rows: 'mysql_rows',
    }),
  },
  ctx: {
    env: null as Env,
  },
});

// Define app ops
const handleRequest = defineOp('handleRequest', async (ctx, req: Request) => {
  ctx.tag.user_id(req.userId).request_id(req.id).endpoint(req.path);

  // Invoke library ops via span()
  const users = await ctx.span('query-users', ctx.deps.pg.query, 'SELECT * FROM users');
  const external = await ctx.span('fetch-external', ctx.deps.http.get, req.externalUrl);

  return ctx.ok({ users, external });
});

// Entry point with Tracer
const { trace } = new TestTracer(opContext);

export default {
  async fetch(req: Request, env: Env) {
    return trace('handle-request', { env }, handleRequest, req);
  },
};
```

## Shared Columns Across Libraries

Multiple libraries can write to the same column when they have semantically similar data:

```typescript
const { defineOp } = defineOpContext({
  logSchema: defineLogSchema({
    query: S.text(), // Shared SQL query column
    pg_rows: S.number(),
    mysql_rows: S.number(),
    duration: S.number(), // Could also share duration!
  }),
  deps: {
    pg: postgresOps.mapColumns({
      query: 'query', // pg.query → query
      rows: 'pg_rows',
      duration: 'duration', // Shared
    }),
    mysql: mysqlOps.mapColumns({
      query: 'query', // mysql.query → query (same!)
      rows: 'mysql_rows',
      duration: 'duration', // Shared
    }),
  },
});
```

This allows unified querying:

```sql
-- Query ALL database operations regardless of backend
SELECT query, duration
FROM traces
WHERE entry_type = 'span-ok'
  AND query IS NOT NULL;
```

## Type Safety: Collision Detection

The pattern provides compile-time collision detection:

```typescript
// BAD PATTERN (spread) - SILENT COLLISION:
const { defineOp } = defineOpContext({
  logSchema: {
    ...httpLib.logSchema, // http_status: number
    ...processLib.logSchema, // http_status: string -- SILENTLY OVERWRITES!
  },
});

// CORRECT PATTERN (deps with mapping) - TYPE SAFE:
const { defineOp } = defineOpContext({
  logSchema: appSchema,
  deps: {
    http: httpOps.prefix('http'),
    process: processOps.prefix('process'),
  },
});
// http_status and process_status are separate columns

// EXPLICIT SHARING (intentional):
deps: {
  pg: postgresOps.mapColumns({ query: 'query' }),
  mysql: mysqlOps.mapColumns({ query: 'query' }), // Intentional sharing - type must match!
}
```

## Performance Benefits

### Cold Path vs Hot Path Optimization

**Cold Path** (Library Composition Time - `.prefix()` / `.mapColumns()`):

- Schema analysis and column mapping
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
// Root buffer's schema has mapped columns
const schema = rootBuffer.op.context.logSchema;
// { userId, http_status, http_method, query, pg_rows, mysql_rows, ... }

walkSpanTree(rootBuffer, (buffer) => {
  // For library's RemappedBufferView:
  // buffer.getColumnIfAllocated('http_status')
  //   → mapping['http_status'] = 'status'
  //   → ownBuffer.getColumnIfAllocated('status')
  //   → returns the actual TypedArray

  // For shared columns:
  // buffer.getColumnIfAllocated('query')
  //   → mapping['query'] = 'query'
  //   → ownBuffer.getColumnIfAllocated('query')
  //   → returns the TypedArray

  // For dropped columns:
  // buffer.getColumnIfAllocated('pg__debug')
  //   → mapping['pg__debug'] = null
  //   → returns undefined (column ignored)

  for (const [fieldName, _] of schemaFields) {
    const col = buffer.getColumnIfAllocated(fieldName);
    // col is undefined for columns this buffer doesn't own or dropped
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
| `query`            | `string`             | Masked SQL query (shared!)                    | PG/MySQL libraries  |
| `pg_rows`          | `uint32`             | Rows from Postgres                            | Postgres library    |
| `mysql_rows`       | `uint32`             | Rows from MySQL                               | MySQL library       |
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

-- Query across ALL database backends (shared column!)
SELECT
  query,
  sum(pg_rows) as pg_total,
  sum(mysql_rows) as mysql_total
FROM traces
WHERE query IS NOT NULL
GROUP BY query
ORDER BY pg_total + mysql_total DESC;

-- Cross-service performance correlation
SELECT
  user_id,
  avg(http_duration) as avg_http_time,
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

- **[Op Context Pattern](./01l_op_context_pattern.md)**: High-level API design (`defineOpContext`, `.prefix()`,
  `.mapColumns()`)
- **[Trace Schema System](./01a_trace_schema_system.md)**: Provides the schema definition and composition mechanisms
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generates TypedArray columns based on
  composed schemas
- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Implements the op wrapper pattern that
  libraries build upon
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Details the runtime code generation that creates
  the optimized library APIs

## Benefits Summary

1. **Library Author Experience**: Clean, domain-focused APIs without global naming concerns
2. **User Experience**: Simple composition with explicit column mapping
3. **Performance**: Zero hot path overhead through compile-time optimization
4. **Type Safety**: Full TypeScript inference with compile-time collision detection
5. **Ecosystem**: Enables rich third-party library ecosystem for tracing
6. **Maintainability**: Modular pattern that scales to many libraries
7. **Query Performance**: Clean, optimized columnar data for analytics
8. **Column Sharing**: Multiple libraries can intentionally share columns for unified querying

This pattern enables a rich ecosystem of traced libraries while maintaining the performance and type safety goals of the
overall system.
