# Library Integration Pattern

## Overview

The Library Integration Pattern enables third-party libraries to provide traced operations with clean APIs while
avoiding attribute name conflicts. It solves the challenge of:

1. **Clean library authoring**: Libraries write unprefixed code (`ctx.tag.status(200)`)
2. **Collision avoidance**: Final columns are prefixed (`http_status`, `db_status`)
3. **Zero hot path overhead**: All mapping work happens at task creation time
4. **Type safety**: Full TypeScript inference through composition

## Design Philosophy

**Key Insight**: Library authors should write clean, domain-focused code without worrying about global naming conflicts.
The tracing system handles prefixing transparently while maintaining maximum performance.

**The Challenge**: Multiple libraries might define the same attribute names:

- HTTP library: `status: number` (HTTP status codes)
- Process library: `status: string` (process states like "running", "failed")
- Database library: `status: string` (connection states)

**The Solution**: Libraries define clean schemas and get prefixed at composition time.

## Core Library Pattern

```typescript
// @trace-system/core - provides the pattern for all libraries
export function createLibraryModule<T extends TagAttributeSchema>(cleanSchema: T, prefix?: string) {
  return {
    tagAttributes: prefix ? applyPrefix(cleanSchema, prefix) : cleanSchema,

    createTask: <Args extends any[]>(
      spanName: string,
      taskFn: (ctx: ContextWithCleanTags<T>, ...args: Args) => any
    ) => {
      if (prefix) {
        // Do ALL the work HERE - cold path, at task creation time
        const cleanNames = Object.keys(cleanSchema.fields);
        const prefixedNames = cleanNames.map((name) => `${prefix}_${name}`);

        // Generate optimized wrapper function code
        const mappingCode = cleanNames.map((cleanName, i) => `${cleanName}: ctx.tag.${prefixedNames[i]}`).join(', ');

        const wrapperFunctionCode = `
          return function(ctx, ...args) {
            const libraryCtx = { ...ctx, tag: { ${mappingCode} } };
            return taskFn(libraryCtx, ...args);
          }
        `;

        // Create the optimized function ONCE - not in hot path
        return new Function('taskFn', wrapperFunctionCode)(taskFn);
      } else {
        return taskFn;
      }
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

**Cold Path** (Task Creation Time):

- Schema analysis and prefix mapping
- JavaScript code generation via `new Function()`
- Function compilation and optimization
- Type inference and validation

**Hot Path** (Task Execution Time):

- Single object creation with pre-computed property references
- Direct TypedArray writes to correct columns
- No runtime conditionals or proxy overhead
- Optimal V8 optimization (hidden classes, inline caches)

### Generated Code Example

At module context creation time, we create a remapped `TagAPI` class:

```typescript
// Library defines clean schema
const cleanHttpSchema = {
  status: S.number(),
  method: S.category(),
  url: S.text(),
  duration: S.number(),
};

// Gets prefixed to avoid conflicts
const prefixedSchema = {
  http_status: S.number(),
  http_method: S.category(),
  http_url: S.text(),
  http_duration: S.number(),
};

// At module context creation, generate remapped TagAPI class using new Function()
const createRemappedTagAPI = (cleanNames, prefixedNames) => {
  const attributeMethods = cleanNames
    .map(
      (cleanName, i) => `
    ${cleanName}(value) {
      this._writeTagEntry();
      this.buffer.write${capitalize(prefixedNames[i])}(value);
      return this;
    }
  `
    )
    .join('\n');

  // Generate the complete class at module context creation time
  const RemappedTagAPI = new Function(
    'BaseTagAPI',
    `
    return class extends BaseTagAPI {
      ${attributeMethods}
    }
  `
  )(TagAPI);

  return RemappedTagAPI;
};

// Library code uses clean names (directly on ctx.tag, not ctx.log.tag):
ctx.tag.status(200); // Writes to http_status column
ctx.tag.method('POST'); // Writes to http_method column
```

## Arrow Table Output

The final Arrow table has clean, collision-free columns:

| Column             | Type                 | Description                     | Source              |
| ------------------ | -------------------- | ------------------------------- | ------------------- |
| `timestamp`        | `timestamp[μs]`      | Event timestamp                 | Core system         |
| `trace_id`         | `dictionary<string>` | Trace identifier                | Core system         |
| `thread_id`        | `uint64`             | Thread/worker identifier        | Core system         |
| `span_id`          | `uint32`             | Unit of work within thread      | Core system         |
| `parent_thread_id` | `uint64` (nullable)  | Parent span's thread            | Core system         |
| `parent_span_id`   | `uint32` (nullable)  | Parent span's ID                | Core system         |
| `entry_type`       | `dictionary<string>` | Log entry type                  | Core system         |
| `module`           | `dictionary<string>` | Module name                     | Core system         |
| `span_name`        | `dictionary<string>` | Span/task name                  | Core system         |
| `message`          | `string`             | Log message                     | Core system         |
| `http_status`      | `uint16`             | HTTP status code                | HTTP library        |
| `http_method`      | `dictionary<string>` | HTTP method                     | HTTP library        |
| `http_url`         | `string`             | Masked URL                      | HTTP library        |
| `http_duration`    | `float32`            | HTTP request duration           | HTTP library        |
| `db_query`         | `string`             | Masked SQL query                | Database library    |
| `db_duration`      | `float32`            | Query duration                  | Database library    |
| `db_rows`          | `uint32`             | Rows affected                   | Database library    |
| `db_table`         | `dictionary<string>` | Table name                      | Database library    |
| `redis_command`    | `dictionary<string>` | Redis command                   | Redis library       |
| `redis_key`        | `string`             | Redis key                       | Redis library       |
| `redis_duration`   | `float32`            | Redis operation duration        | Redis library       |
| `user_id`          | `binary[8]`          | Hashed user ID                  | User-defined        |
| `business_metric`  | `float64`            | Custom metric                   | User-defined        |
| `ff_value`         | `dictionary<string>` | Feature flag value (S.category) | Feature flag system |

**Note**: Feature flag names are stored in the unified `label` column for `ff-access` and `ff-usage` entries.

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
-- Note: flag name is in the unified `label` column
SELECT
  label as flag_name,
  count(*) as access_count,
  sum(if(ff_value = 'true', 1, 0)) as enabled_count
FROM traces
WHERE entry_type = 'ff-access'
GROUP BY label
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
