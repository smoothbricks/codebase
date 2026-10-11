# Library Integration Pattern <a id="smoo/lmao!n/library-integration"></a>

## Overview

The Library Integration Pattern enables third-party libraries to provide traced operations with clean APIs while
avoiding attribute name conflicts. It solves the challenge of:

1. **Clean library authoring**: Libraries write unprefixed code (`tag.status(200)`)
2. **Collision avoidance**: Final columns are prefixed or mapped (`http_status`, `db_status`)
3. **Zero hot path overhead**: Library writes directly to its own SpanBuffer with unprefixed columns
4. **Type safety**: Full TypeScript inference through explicit dependency injection
5. **Tree traversal compatibility**: A `RemapDescriptor` lets Arrow conversion read library columns under their mapped
   names

> **Note**: This spec describes the runtime mechanics of library integration. For the high-level API design
> (`defineOpContext`, `.prefix()`, `.mapColumns()`), see [Op Context Pattern](./01l_op_context_pattern.md).

## Design Philosophy

**Key Insight**: Library authors should write clean, domain-focused code without worrying about global naming conflicts.
The tracing system handles prefixing/mapping transparently while maintaining maximum performance.

**The Challenge**: Multiple libraries might define the same attribute names:

- HTTP library: `status: number` (HTTP status codes)
- Process library: `status: string` (process states like "running", "failed")
- Database library: `status: string` (connection states)

**The Solution**: Libraries write their own SpanBuffer with the unprefixed schema. The mapped Op carries an immutable
`RemapDescriptor` from output names to unprefixed columns, and the child buffer holds a pointer to it. This enables:

- **Hot path**: Direct TypedArray writes to unprefixed columns (zero overhead)
- **Cold path**: Arrow conversion reads each mapped output column through the span's `RemapDescriptor`

## Column Mapping API <a id="smoo/lmao!n/library-integration-column-mapping"></a>

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

## Core Architecture: RemapDescriptor <a id="smoo/lmao!n/library-integration-remapped-view"></a>

### The Problem

When a library op creates a child span:

1. **Library's transformed code** writes to unprefixed columns: `buffer.status_values[0] = 200`
2. **Arrow conversion** emits the names the application wired: `http_status`
3. **Application** may have its own `status` column with different meaning

### The Solution: RemapDescriptor

Writes are never remapped at runtime, because a remap on the write path would cost every write. Instead:

1. **The library Op writes its own canonical SpanBuffer** with the unprefixed schema (`{ status, method }`), so
   transformed code works unchanged
2. **`.prefix()` / `.mapColumns()` build one immutable `RemapDescriptor`** with
   `createRemapDescriptor(sourceSchema, outputToSourceMapping)` (`packages/lmao/src/lib/library.ts`) and clone the
   group's Ops with it, so each mapped Op's callsite plan carries the descriptor
3. **Child span setup attaches the descriptor** to the raw child buffer as `childBuffer._remapDescriptor`; no buffer is
   wrapped
4. **The child registers in the trace topology** (`parentBuffer._traceRoot._topology.registerChild(parent, child)`),
   like every other child span
5. **`walkSpanTree(root, visitor)` passes `(buffer, remapDescriptor)`** to the visitor, and Arrow conversion resolves
   each output column name to its source column through the descriptor

```typescript
// packages/lmao/src/lib/logBinding.ts
type RemappedColumn = readonly [
  outputName: string,
  sourceName: string,
  schema: unknown,
  valuesProperty: `${string}_values`,
  nullsProperty: `${string}_nulls`,
];

interface RemapDescriptor {
  /** Fully composed output-name to canonical source-name lookup. */
  readonly sourceNames: Readonly<Record<string, string>>;
  /** Dense output/source/schema entries in deterministic composition order. */
  readonly columns: readonly RemappedColumn[];
}
```

The descriptor and its arrays are frozen. A column mapped to `null` gets no entry, so a dropped column has no output
column.

```
Application Root Buffer (schema: { user_id, …, http_status, http_method, auth_token, query, pg_rows, … })
│
├── child node: HTTP Library Buffer (schema: { status, method })
│   │   _remapDescriptor.sourceNames: http_status → status, http_method → method
│   │
│   └── child node: Auth Library Buffer (schema: { token })
│           _remapDescriptor.sourceNames: auth_token → token   (the application's deps.auth: .prefix('auth'))
│
├── child node: Postgres Library Buffer (schema: { query, rows })
│       _remapDescriptor.sourceNames: query → query, pg_rows → rows (shared 'query' column!)
│
└── child node: App's own child span buffer (same schema as root, no _remapDescriptor)
```

Child nodes live in the trace's `TraceTopology` arena (`firstChild`/`nextSibling` node arrays), not in per-buffer child
arrays. An overflow segment shares its span's node, and the walk hands every segment that span's descriptor.

### How Nested Libraries Compose

- **Chained mapping on one group composes into one descriptor.** `libraryOps.prefix('api').prefix('v1')` yields
  `sourceNames: { v1_api_metric: 'metric', v1_api_label: 'label' }`: each later `.prefix()` prefixes the earlier output
  names, and `createRemapDescriptor` builds the descriptor once from the fully composed mapping.
- **Deps resolve at the root, and descriptors of ancestor spans are not applied.** Every span of a trace sees the `deps`
  of the Tracer's context, since each child span shares its parent's
  ([Context Flow](./01c_context_flow_and_op_wrappers.md)). A library's `deps` therefore declares the groups it calls,
  and the application wires each of them. A child span carries only the descriptor of the Op it runs: the auth spans in
  the diagram emit `auth_token`, the name the application's `deps.auth` wiring gave them, not `http_auth_token`.
- **The root wiring must satisfy every included library's deps, checked by the type system.** The application's `deps`
  must hold, under the same key, a group compatible with each dep that any group it includes declares, transitively.
  `defineOpContext` rejects a wiring that lacks such a key or wires an incompatible group as a type error, so a library
  span never reads an unwired dep at runtime and a library never guards a declared dep for presence.
- **A mapped group contributes only its own schema fields.** `httpOps.prefix('http')` contributes `http_status` and
  `http_method`, and its descriptor maps only those. The columns of the HTTP library's own deps come from the groups the
  application wires for them (`auth: authOps.prefix('auth')` contributes `auth_token`), so the application's effective
  schema lists exactly the columns its spans write.
- **An Op without a descriptor called inside a remapped span inherits the parent's descriptor** when it shares the
  parent's schema. A prefixed library calling one of its own unmapped Ops by direct reference therefore still emits the
  prefixed names (`http_method`, not `method`).

### Why This Works

**Hot Path (library code execution)**:

- Library's buffer is the raw SpanBuffer with unprefixed columns
- Transformed code writes directly: `buffer.status_values[0] = 200`
- Zero overhead - direct TypedArray access, one buffer shape whether or not the Op is mapped

**Cold Path (Arrow conversion)**:

- `walkSpanTree` visits the library buffer with its `RemapDescriptor`
- Pass 0 takes the span's output columns from `remapDescriptor.columns` (`http_status` with the `status` field schema)
- Column passes resolve `http_status` through `remapDescriptor.sourceNames['http_status']` → `status` and read
  `buffer.getColumnIfAllocated('status')`
- Returns the actual TypedArray for that column

**Columns the library doesn't own** (e.g., `user_id` from app schema):

- A mapped span exposes only its descriptor's output columns. An output name missing from `sourceNames` reads as null
  cells for that span, even when the library's schema has a column of that name: with an application `status` column
  beside `httpOps.prefix('http')`, the library's `status` reaches Arrow only as `http_status`, and the application's
  `status` stays null on the library's rows
- Scope values resolve the same way, so a library's scope never fills an application column it was not mapped to

**Dropped columns** (mapped to `null`):

- The descriptor has no entry for `_debug`, so no output column comes from it
- Library can write `tag._debug(...)` but it never appears in Arrow output

## Op's Responsibility: Buffer Creation and Registration <a id="smoo/lmao!n/library-integration-op-buffer"></a>

**Critical Design Point**: The **called Op's callsite plan** decides how its child buffer is built. The plan is sealed
once, when the Op is defined or cloned by `.prefix()` / `.mapColumns()`, and holds the Op's `SpanBufferClass` and
schema, its `SpanContextClass`, its capacity tier and its `remapDescriptor`. `span()` hands the target Op's plan to the
parent context's child setup (`_spanPre`, `packages/lmao/src/lib/spanContext.ts`).

### What Child Setup Does

```typescript
// SpanContext._spanPre (abridged)
const childBuffer = tracer.bufferStrategy.createChildSpanBuffer(
  parentBuffer,
  parentBuffer._opMetadata, // callsite metadata (row 0)
  callsitePlan.metadata, // the called Op's metadata (rows 1+)
  callsitePlan.capacityTier,
  callsitePlan.schema, // the Op's UNPREFIXED schema
  callsitePlan.SpanBufferClass
); // registers the child: parentBuffer._traceRoot._topology.registerChild(parentBuffer, childBuffer)
childBuffer._remapDescriptor = callsitePlan.remapDescriptor ?? undefined; // set for mapped/prefixed Ops
childBuffer._appenders.writeSpanStart(childBuffer, name);
childBuffer.line(0, line);
return new callsitePlan.SpanContextClass(childBuffer /* , schema, plan, deps, flag evaluator */);
```

The `span0`-`span8` method then runs the Op's function in that context, writes the span end from its result, and records
a thrown error as `span-exception` before rethrowing.

### Why the Op's Plan Owns These Choices

1. **Schema isolation**: Each library has its own schema; the plan carries the Op's own buffer class
2. **Column mapping**: The plan carries the descriptor its group's `.prefix()` / `.mapColumns()` composed
3. **One shape on the hot path**: Every child is a canonical SpanBuffer; the descriptor is a pointer only Arrow
   conversion reads
4. **No per-call work**: The plan is built once, so child setup only reads it

### What the Caller Writes

```typescript
await ctx.span('fetch-data', ctx.deps.http.get, 'https://api.example.com');
```

The caller supplies the span name, the Op and the Op's arguments, and the transformer adds the call line. The caller
does **NOT**:

- Choose the child's schema or buffer class
- Register the child in the trace topology
- Attach a remap descriptor

## Library Definition with defineOpContext <a id="smoo/lmao!n/library-integration-define"></a>

Libraries use `defineOpContext()` to define their schema and ops:

```typescript
// @my-company/http-tracing/src/index.ts
import { defineCodeError, defineOpContext, defineLogSchema, S } from '@smoothbricks/lmao';

// Define the library's schema (unprefixed)
const httpSchema = defineLogSchema({
  status: S.number(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
  url: S.text().masked('url'),
  duration: S.number(),
});

// Define the library's errors: the code becomes error_code, the fields are typed
const HTTP_ERROR = defineCodeError('HTTP_ERROR')<{ status: number }>();

// Create op context for this library
const { defineOp, defineOps, logSchema, flags } = defineOpContext({
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

    if (!response.ok) {
      // Use dependency via span(); the application's wiring is type-checked to provide `retry`
      await ctx.span('retry', ctx.deps.retry.attempt, 1);
    }

    return response.ok ? ctx.ok(response) : ctx.err(HTTP_ERROR({ status: response.status }));
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

## Application Composition <a id="smoo/lmao!n/library-integration-app-composition"></a>

Applications wire libraries with column mapping using the fluent API:

```typescript
// Application code
import { defineOpContext, defineLogSchema, S } from '@smoothbricks/lmao';
import { httpOps } from '@my-company/http-tracing';
import { retryOps } from '@my-company/retry-tracing';
import { postgresOps } from '@my-company/postgres-tracing';
import { mysqlOps } from '@my-company/mysql-tracing';

// Application defines its own schema
const appSchema = defineLogSchema({
  user_id: S.category(),
  request_id: S.category(),
  endpoint: S.category(),
});

// Wire dependencies with column mapping
const { defineOp, defineOps, logSchema, flags } = defineOpContext({
  logSchema: appSchema,
  deps: {
    // Simple prefixing
    http: httpOps.prefix('http'),
    // The HTTP library declares `retry` in its deps; omitting this key is a type error
    retry: retryOps.prefix('http_retry'),

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

## Shared Columns Across Libraries <a id="smoo/lmao!n/library-integration-shared-columns"></a>

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

## Type Safety: Collision Detection <a id="smoo/lmao!n/library-integration-collision"></a>

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

## Performance Benefits <a id="smoo/lmao!n/library-integration-performance"></a>

### Cold Path vs Hot Path Optimization

**Cold Path** (Library Composition Time - `.prefix()` / `.mapColumns()`):

- Schema analysis and column mapping
- One frozen `RemapDescriptor` per `.prefix()` / `.mapColumns()` call, built by `createRemapDescriptor`
- The group's Ops cloned with the descriptor; each clone seals its callsite plan once
- Type inference and validation

**Hot Path** (Op Execution Time):

- Child setup creates the Op's own SpanBuffer with the unprefixed schema and stores one descriptor pointer
- Transformed code writes directly to TypedArrays: `buffer.status_values[0] = 200`
- **Zero remapping overhead** - no proxy, no function calls for column access
- Optimal V8 optimization (hidden classes, inline caches)

### Why No Proxy?

A JavaScript Proxy for remapping is rejected because:

1. **Proxy traps on every property access** - hot path overhead
2. **Breaks V8 hidden class optimization** - Proxy objects have dynamic shape
3. **Unnecessary** - the `RemapDescriptor` is plain data that only the cold-path tree traversal reads

### Transformed Code Example

The lmao-ttsc plugin converts fluent tag calls to direct array writes:

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

When Arrow conversion walks the tree, the visitor receives each span's descriptor (`undefined` for an unmapped span):

```typescript
walkSpanTree(rootBuffer, (buffer, remapDescriptor) => {
  // Pass 0: a mapped span contributes remapDescriptor.columns, not its own field names:
  //   ['http_status', 'status', statusSchema, 'status_values', 'status_nulls'], ...
  // An unmapped span contributes its own schema fields (buffer._columns).
});

// Column passes read each output column through the span's descriptor.
// A mapped span exposes only its descriptor's output columns:
function resolveColumnName(buffer: AnySpanBuffer, outputName: string, remaps: RemapsByBuffer): string | undefined {
  const remap = remaps.get(buffer);
  if (remap === undefined) return outputName; // unmapped span: its own field names
  return remap.sourceNames[outputName]; // mapped span: no entry → null cells
}

// 'http_status' → sourceNames['http_status'] = 'status' → buffer.getColumnIfAllocated('status')
// 'query' (shared) → sourceNames['query'] = 'query' → buffer.getColumnIfAllocated('query')
// 'user_id' (app column) → no entry → null cells
// 'status' (app column) → no entry → null cells, although the library buffer has a 'status' column
// '_debug' (dropped) → no output column exists for it
```

Scope values resolve the same way: a mapped span's scope value for `http_status` is read from `_scopeValues.status`. See
[Tree Walker and Arrow Conversion](./01k_tree_walker_and_arrow_conversion.md) for the walk and Pass 0 merge.

## Arrow Table Output <a id="smoo/lmao!n/library-integration-arrow-output"></a>

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
| `package_file`     | `dictionary<string>` | Path within package, relative to package.json | Core system         |
| `git_sha`          | `dictionary<string>` | Git commit SHA of the op's source             | Core system         |
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
on the `package_name` and `package_file` column design.

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
