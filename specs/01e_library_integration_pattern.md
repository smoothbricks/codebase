# Library Integration Pattern

## Overview

The Library Integration Pattern enables third-party libraries to provide traced operations with clean APIs while
avoiding attribute name conflicts. It solves the challenge of:

1. **Clean library authoring**: Libraries write unprefixed code (`ctx.tag.status(200)`)
2. **Collision avoidance**: Final columns are prefixed (`http_status`, `db_status`)
3. **Zero hot path overhead**: Library writes directly to its own SpanBuffer with unprefixed columns
4. **Type safety**: Full TypeScript inference through composition
5. **Tree traversal compatibility**: RemappedBufferView allows Arrow conversion to access prefixed names

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

When a library task creates a child span:

1. **Library's transformed code** writes to unprefixed columns: `ctx._buffer.status_values[0] = 200`
2. **Parent's tree traversal** (Arrow conversion) iterates with prefixed names from root schema:
   `getColumnIfAllocated('http_status')`
3. **Application** may have its own `status` column with different meaning

### The Solution: RemappedBufferView

Instead of trying to remap writes at runtime (which adds hot-path overhead), we:

1. **Library creates its own SpanBuffer** with unprefixed schema (`{ status, method }`)
2. **Library's ctx.\_buffer** points to this unprefixed buffer - transformed code works directly
3. **RemappedBufferView** wraps the library buffer, mapping prefixed → unprefixed for tree traversal
4. **Parent's children array** contains the RemappedBufferView (not the raw buffer)

```
Application Root Buffer (schema: { userId, http_status, http_method, db_query })
│
├── children[0]: RemappedBufferView
│   │   Maps: http_status → status, http_method → method
│   │
│   └── wraps: HTTP Library Buffer (schema: { status, method })
│               │
│               └── children[0]: RemappedBufferView (for nested auth library)
│                       Maps: auth_token → token
│                       └── wraps: Auth Library Buffer (schema: { token })
│
└── children[1]: App's own child span buffer (same schema as root)
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
      get children() { return this._buffer.children; }
      get next() { return this._buffer.next; }

      // Row count
      get writeIndex() { return this._buffer.writeIndex; }

      // System columns (NOT remapped - same in all buffers)
      get timestamps() { return this._buffer.timestamps; }
      get operations() { return this._buffer.operations; }
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
      get traceId() { return this._buffer.traceId; }
      get threadId() { return this._buffer.threadId; }
      get spanId() { return this._buffer.spanId; }
      get parentSpanId() { return this._buffer.parentSpanId; }
      get _identity() { return this._buffer._identity; }

      // Metadata (pass-through)
      get task() { return this._buffer.task; }

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

- Library's `ctx._buffer` is the raw SpanBuffer with unprefixed columns
- Transformed code writes directly: `ctx._buffer.status_values[0] = 200`
- Zero overhead - direct TypedArray access

**Cold Path (Arrow conversion)**:

- Tree walker encounters RemappedBufferView in parent's `children[]`
- Calls `view.getColumnIfAllocated('http_status')`
- RemappedBufferView maps to `buffer.getColumnIfAllocated('status')`
- Returns the actual TypedArray for that column

**Columns the library doesn't own** (e.g., `userId` from app schema):

- `view.getColumnIfAllocated('userId')` → `buffer.getColumnIfAllocated('userId')` → `undefined`
- Arrow conversion handles `undefined` as null column - correct behavior

## Explicit Child Registration

SpanBuffer constructors do NOT auto-register with parent. Registration is explicit:

```typescript
// In library's task wrapper:
const ownBuffer = createSpanBuffer(unprefixedSchema, taskContext, traceId);

if (prefix && parentCtx?.buffer) {
  // Create remapped view and register with parent
  const remappedView = new RemappedViewClass(ownBuffer);
  parentCtx.buffer.children.push(remappedView);
} else if (parentCtx?.buffer) {
  // No prefix - register buffer directly
  parentCtx.buffer.children.push(ownBuffer);
}

// Library code uses raw buffer
ctx._buffer = ownBuffer;
```

This explicit registration enables:

1. Libraries to register RemappedBufferView instead of raw buffer
2. Testing scenarios where auto-registration is undesirable
3. Clear control flow for debugging

## Core Library Pattern

```typescript
// @trace-system/core - provides the pattern for all libraries
export function createLibraryModule<T extends TagAttributeSchema>(unprefixedSchema: T, prefix?: string) {
  // Generate RemappedBufferView class once (cold path)
  const RemappedViewClass = prefix
    ? generateRemappedBufferViewClass(createPrefixMapping(unprefixedSchema, prefix))
    : null;

  return {
    // Prefixed schema for application composition
    tagAttributes: prefix ? applyPrefix(unprefixedSchema, prefix) : unprefixedSchema,

    // Unprefixed schema for library's own buffer creation
    unprefixedSchema,

    createTask: <Args extends any[]>(
      spanName: string,
      taskFn: (ctx: ContextWithUnprefixedTags<T>, ...args: Args) => any
    ) => {
      return async (parentCtx: RequestContext, ...args: Args) => {
        // Create library's own buffer with unprefixed schema
        const taskContext = new TaskContext(moduleContext, spanName);
        const ownBuffer = createSpanBuffer(unprefixedSchema, taskContext, parentCtx.traceId);

        // Register with parent (remapped if prefixed)
        if (parentCtx.buffer) {
          if (RemappedViewClass) {
            parentCtx.buffer.children.push(new RemappedViewClass(ownBuffer));
          } else {
            parentCtx.buffer.children.push(ownBuffer);
          }
        }

        // Create context with unprefixed buffer
        const ctx = createSpanContext(ownBuffer, unprefixedSchema, parentCtx);

        // Library code writes to unprefixed columns
        return await taskFn(ctx, ...args);
      };
    },
  };
}
```

## Library Implementation Examples

### HTTP Tracing Library

```typescript
// @my-company/http-tracing
const cleanHttpSchema = defineTagAttributes({
  status: S.number(),
  method: S.category(),
  url: S.text().masked('url'),
  duration: S.number(),
});

export function createHttpLibrary(prefix: string = 'http') {
  const module = createLibraryModule(cleanHttpSchema, prefix);

  return {
    ...module,

    // Provide actual traced operations
    operations: {
      async request(ctx: Context, options: RequestOptions) {
        return await module.createTask('http-request', async (ctx, opts) => {
          const startTime = performance.now();

          // Clean, unprefixed API - but writes to prefixed columns
          ctx.tag.method(opts.method);
          ctx.tag.url(opts.url);

          try {
            const response = await fetch(opts.url, opts);
            ctx.tag.status(response.status);
            ctx.tag.duration(performance.now() - startTime);
            return ctx.ok(response);
          } catch (error) {
            ctx.tag.status(0);
            ctx.tag.duration(performance.now() - startTime);
            return ctx.err('HTTP_ERROR', error);
          }
        })(ctx, options);
      },
    },
  };
}
```

### Database Tracing Library

```typescript
// @my-company/db-tracing
const cleanDbSchema = defineTagAttributes({
  query: S.text().masked('sql'),
  duration: S.number(),
  rows: S.number().optional(),
  table: S.category(),
});

export function createDatabaseLibrary(prefix: string = 'db') {
  const module = createLibraryModule(cleanDbSchema, prefix);

  return {
    ...module,

    operations: {
      async query(ctx: Context, sql: string) {
        return await module.createTask('db-query', async (ctx, query) => {
          const startTime = performance.now();

          ctx.tag.query(query);
          ctx.tag.table(extractTableName(query));

          try {
            const result = await db.query(query);
            ctx.tag.duration(performance.now() - startTime);
            ctx.tag.rows(result.rowCount);
            return ctx.ok(result);
          } catch (error) {
            ctx.tag.duration(performance.now() - startTime);
            return ctx.err('DB_ERROR', error);
          }
        })(ctx, sql);
      },
    },
  };
}
```

## User Composition

### Clean Composition Without Conflicts

```typescript
const httpLib = createHttpLibrary('http');
const dbLib = createDatabaseLibrary('db');
const redisLib = createRedisLibrary('redis');

// Compose all library schemas with user-defined attributes
const { task } = createModuleContext({
  tagAttributes: {
    // Library schemas (prefixed)
    ...httpLib.tagAttributes, // { http_status: number, http_method: string, http_url: string, http_duration: number }
    ...dbLib.tagAttributes, // { db_query: string, db_duration: number, db_rows?: number, db_table: string }
    ...redisLib.tagAttributes, // { redis_command: string, redis_key: string, redis_duration: number }

    // User's own attributes (unprefixed)
    user_id: S.category().masked('hash'),
    business_metric: S.number(),
    custom_flag: S.boolean(),
  },
});
```

### Type Safety Enforcement

```typescript
// TypeScript prevents composition of incompatible libraries
const httpLib = createHttpLibrary('http'); // status: number
const processLib = createProcessLibrary('http'); // ❌ Same prefix with different types

// This would fail at compile time:
const { task } = createModuleContext({
  tagAttributes: {
    ...httpLib.tagAttributes, // http_status: number
    ...processLib.tagAttributes, // http_status: string ❌ Conflict!
  },
});

// Solution: Use different prefixes
const processLib = createProcessLibrary('process'); // ✅ Different prefix
```

## Performance Benefits

### Cold Path vs Hot Path Optimization

**Cold Path** (Library Module Creation Time):

- Schema analysis and prefix mapping
- RemappedBufferView class generation via `new Function()`
- One-time class compilation and V8 optimization
- Type inference and validation

**Hot Path** (Task Execution Time):

- Library creates its own SpanBuffer with unprefixed schema
- Transformed code writes directly to TypedArrays: `ctx._buffer.status_values[0] = 200`
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
ctx.tag.status(200).method('POST');

// Transformed (direct array access to unprefixed columns):
{
  ctx._buffer.status_nulls[0] |= 1;
  ctx._buffer.status_values[0] = 200;
  ctx._buffer.method_nulls[0] |= 1;
  ctx._buffer.method_values[0] = 0; // enum index
}
```

Since `ctx._buffer` is the library's own buffer with unprefixed schema, these writes work directly.

### Arrow Conversion Example

When Arrow conversion walks the tree:

```typescript
// Root buffer's schema has prefixed columns
const schema = rootBuffer.task.module.tagAttributes;
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

- **[Trace Schema System](./01a_trace_schema_system.md)**: Provides the schema definition and composition mechanisms
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generates prefixed TypedArray columns based
  on composed schemas
- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Implements the task wrapper pattern
  that libraries build upon
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Details the runtime code generation that creates
  the optimized library APIs

## Benefits Summary

1. **Library Author Experience**: Clean, domain-focused APIs without global naming concerns
2. **User Experience**: Simple composition with automatic conflict resolution
3. **Performance**: Zero hot path overhead through compile-time optimization
4. **Type Safety**: Full TypeScript inference prevents composition errors
5. **Ecosystem**: Enables rich third-party library ecosystem for tracing
6. **Maintainability**: Modular pattern that scales to many libraries
7. **Query Performance**: Clean, optimized columnar data for analytics

This pattern enables a rich ecosystem of traced libraries while maintaining the performance and type safety goals of the
overall system.
