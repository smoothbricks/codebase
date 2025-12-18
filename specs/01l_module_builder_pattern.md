# Module Builder Pattern

## Overview

This spec defines the **Module Builder Pattern** - a fluent API for defining traced modules and composing them with
type-safe dependency injection. This pattern supersedes the previous `createModuleContext` and `moduleContextFactory`
APIs, providing a cleaner, more composable design.

## Design Evolution: How We Got Here

### Problem 1: Schema Collision via Spread

The original design used object spread for composition:

```typescript
const { task } = createModuleContext({
  tagAttributes: {
    ...httpLib.tagAttributes, // { http_status, http_method }
    ...dbLib.tagAttributes, // { db_status, db_query }
    userId: S.category(),
  },
});
```

**Issue**: TypeScript's `...spread` silently overwrites on collision. If two libraries define the same prefixed column,
the last one wins with no compile-time error.

### Problem 2: Type Erasure

The `moduleContextFactory` returned erased types:

```typescript
function moduleContextFactory(...): ModuleContextBuilder<TagAttributeSchema, FF, Env>
//                                                       ^^^^^^^^^^^^^^^^^ Erased!
```

This allowed invalid code to compile:

```typescript
const httpTask = httpLib.task('request', async (ctx) => {
  await dbTask(ctx); // Compiles but ctx doesn't have db columns!
});
```

### Problem 3: Ad-hoc Library Nesting

Without explicit dependency declaration, libraries could call each other ad-hoc:

```typescript
// httpLib internally calling dbLib - but who wires the deps?
// Who controls the column prefixes?
```

This led to runtime schema merging (PASS 0 in Arrow conversion) to collect all column names from all buffers in the
tree.

### Problem 4: GraphQL → HTTP Shared Columns

A key insight came from considering real library relationships:

- GraphQL library needs HTTP library internally
- Application also uses HTTP library directly
- Should both use the SAME `http_status` column, not duplicate as `graphql_http_status`

This requires **explicit dependency injection** where the application controls which HTTP instance GraphQL uses.

### Solution: Module Builder Pattern

The new design addresses all these issues:

1. **Fluent builder with collision detection** - TypeScript errors on column conflicts
2. **Explicit dependency declaration** - Modules declare what they need
3. **Dependency injection at composition** - Application wires dependencies
4. **Pre-bound context** - Dependencies receive context automatically

## Core API

### Defining a Module

```typescript
// Library author defines a module
const schema = {
  status: S.number(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  url: S.text(),
};

export const httpLib = defineModule({
  metadata: {
    packageName: '@mycompany/http-tracing',
    packagePath: 'src/index.ts',
  },
  schema,
  deps: {
    retry: retryLib, // Declares dependency on retry library
  },
});
```

### Defining Tasks

```typescript
// Destructure task factory from module
const { task } = httpLib;

// Define tasks - ctx has typed schema + deps
export const GET = task('http-get', async (ctx, url: string) => {
  ctx.tag.method('GET');
  ctx.tag.url(url);

  // Use dependency - ctx is pre-bound
  ctx.deps.retry.attempt(1);

  try {
    const response = await fetch(url);
    ctx.tag.status(response.status);
    return ctx.ok(response);
  } catch (error) {
    ctx.tag.status(0);
    return ctx.err('HTTP_ERROR', error);
  }
});

export const POST = task('http-post', async (ctx, url: string, body: unknown) => {
  ctx.tag.method('POST');
  ctx.tag.url(url);
  // ...
});
```

### Creating a Root Context (Using the Module)

```typescript
// Application wires dependencies and applies prefix
const httpRoot = httpLib
  .prefix('http') // status → http_status, method → http_method
  .use({
    retry: retryLib.prefix('http_retry').use(),
  });

// Invoke tasks with root context
const result = await GET(httpRoot, 'https://api.example.com/data');
```

### Module with No Dependencies

```typescript
// Simple module - no deps
export const metricsLib = defineModule({
  metadata: { packageName: '@mycompany/metrics', packagePath: 'src/index.ts' },
  schema: {
    count: S.number(),
    gauge: S.number(),
    histogram: S.number(),
  },
});

const { task } = metricsLib;

export const increment = task('increment', (ctx, name: string, value: number) => {
  ctx.tag.count(value);
  return ctx.ok({ name, value });
});

// Usage - just prefix, no deps to wire
const metricsRoot = metricsLib.prefix('metrics').use();
await increment(metricsRoot, 'requests', 1);
```

## Application Composition

### Basic Composition

```typescript
// Application module composes libraries
const appModule = defineModule({
  metadata: {
    gitSha: process.env.GIT_SHA,
    packageName: '@mycompany/api-server',
    packagePath: 'src/app.ts',
  },
  schema: {
    userId: S.category(),
    requestId: S.category(),
    endpoint: S.category(),
  },
  deps: {
    http: httpLib,
    db: dbLib,
  },
});

const { task } = appModule;

export const handleRequest = task('handle-request', async (ctx, req: Request) => {
  ctx.tag.userId(req.userId);
  ctx.tag.requestId(req.id);
  ctx.tag.endpoint(req.path);

  // Deps have ctx pre-bound - just call with args
  const data = await ctx.deps.db.query('SELECT * FROM users');
  const response = await ctx.deps.http.GET(req.externalUrl);

  return ctx.ok({ data, response });
});
```

### Wiring Dependencies

```typescript
// Create root context with all deps wired
const appRoot = appModule
  // No prefix for app's own columns (userId, requestId, endpoint)
  // Or use prefix if you want: .prefix('app')
  .use({
    http: httpLib.prefix('http').use({
      retry: retryLib.prefix('http_retry').use(),
    }),
    db: dbLib.prefix('db').use(),
  });

// Invoke app task
await handleRequest(appRoot, incomingRequest);
```

### Shared Dependencies (GraphQL + HTTP)

```typescript
// GraphQL library needs HTTP
const graphqlLib = defineModule({
  metadata: { packageName: '@mycompany/graphql', packagePath: 'src/index.ts' },
  schema: {
    query: S.text(),
    operationName: S.category(),
    variables: S.text(),
  },
  deps: {
    http: httpLib,  // GraphQL uses HTTP
  },
});

// Application uses both GraphQL and HTTP directly
const appModule = defineModule({
  metadata: { ... },
  schema: { userId: S.category() },
  deps: {
    graphql: graphqlLib,
    http: httpLib,  // App also uses HTTP directly
  },
});

// Wire so GraphQL and App share the SAME http instance
const httpInstance = httpLib.prefix('http').use({
  retry: retryLib.prefix('http_retry').use(),
});

const appRoot = appModule.use({
  http: httpInstance,                              // App's direct HTTP
  graphql: graphqlLib.prefix('graphql').use({
    http: httpInstance,                            // GraphQL's HTTP = SAME instance!
  }),
});

// Result: Both write to http_status, http_method, etc.
// No duplicate graphql_http_status columns!
```

## Type System

### Module Type

```typescript
interface Module<Schema extends TagAttributeSchema, Deps extends Record<string, Module<any, any>>> {
  // Module identity
  metadata: ModuleMetadata;
  schema: Schema;
  deps: Deps;

  // Task factory - creates task functions
  task<Args extends unknown[], Result>(
    name: string,
    fn: (ctx: ModuleContext<Schema, Deps>, ...args: Args) => Promise<Result>
  ): TaskFunction<Args, Result, Schema, Deps>;

  // Prefix application - returns PrefixedModule
  prefix<P extends string>(prefix: P): PrefixedModule<Schema, Deps, P>;
}
```

### Prefixed Module Type

```typescript
interface PrefixedModule<
  Schema extends TagAttributeSchema,
  Deps extends Record<string, Module<any, any>>,
  Prefix extends string,
> {
  // Schema with prefixed keys
  prefixedSchema: PrefixSchema<Schema, Prefix>;

  // Create root context with deps wired
  use(deps: WiredDeps<Deps>): RootContext<Schema, Deps, Prefix>;
}

// Type-level prefix application
type PrefixSchema<S, P extends string> = {
  [K in keyof S as `${P}_${K & string}`]: S[K];
};
```

### Root Context Type

```typescript
interface RootContext<
  Schema extends TagAttributeSchema,
  Deps extends Record<string, Module<any, any>>,
  Prefix extends string,
> {
  // Tag API for this module's schema
  tag: TagAPI<Schema>;

  // Logging
  log: LogAPI;

  // Dependencies with ctx pre-bound
  deps: BoundDeps<Deps>;

  // Create child context with overridden deps (for testing)
  with(overrides: Partial<WiredDeps<Deps>>): RootContext<Schema, Deps, Prefix>;
}
```

### Collision Detection

```typescript
// Type-level collision detection
type DetectCollision<A, B> = Extract<keyof A, keyof B> extends never
  ? never  // No collision
  : Extract<keyof A, keyof B>;  // Return conflicting keys

// Used in .use() to produce compile-time errors
type WiredDeps<Deps> = {
  [K in keyof Deps]: Deps[K] extends Module<infer S, infer D>
    ? PrefixedModule<S, D, string>  // Each dep must be prefixed
    : never;
};

// When composing, check for collisions across all dep schemas
type ComposedSchema<Deps> = /* merge all prefixed schemas, error on collision */;
```

## Context Flow

### Task Invocation

When a task is invoked:

```typescript
await GET(httpRoot, 'https://example.com');
```

1. **Generate traceId** (or inherit from parent context if nested)
2. **Create SpanBuffer** with module's prefixed schema
3. **Create full context** = `{ ...httpRoot, traceId, _buffer, spanId, ... }`
4. **Pre-bind deps** - each dep's tasks receive this context automatically
5. **Execute task function** with full context + arguments
6. **Register buffer** with parent's children (if nested)

### Nested Task Calls

When a task calls a dependency's task:

```typescript
// Inside httpLib task:
ctx.deps.retry.attempt(1); // No need to pass ctx!
```

The `ctx.deps.retry.attempt` is already bound to receive `ctx`. Internally:

```typescript
// ctx.deps.retry is a bound version where each task has ctx injected:
ctx.deps.retry = {
  attempt: (n: number) => retryLib.attempt(ctx, n),
  reset: () => retryLib.reset(ctx),
  // ...
};
```

This creates a child span under the current span, sharing the same traceId.

## RemappedBufferView Integration

The RemappedBufferView (from spec 01e) is still used for Arrow conversion:

1. **Library creates SpanBuffer** with unprefixed schema (`{ status, method }`)
2. **Library writes** to unprefixed columns (hot path, zero overhead)
3. **RemappedBufferView wraps buffer** for parent's tree traversal
4. **Arrow conversion** uses prefixed names to access unprefixed columns

The key difference is that dependency injection is now explicit, and the prefix is applied via the fluent `.prefix()`
API rather than being passed to a factory function.

## Testing Support

### Mocking Dependencies

```typescript
// Create mock implementations
const mockRetry = retryLib.prefix('mock_retry').use();
const mockHttp = httpLib.prefix('mock_http').use({
  retry: mockRetry,
});

// Override deps for testing
const testRoot = httpRoot.with({
  retry: mockRetry, // Override just retry dep
});

// Or create fresh root with all mocks
const testRoot = httpLib.prefix('test_http').use({
  retry: mockRetryLib.prefix('test_retry').use(),
});

await GET(testRoot, 'https://example.com');
```

### Testing Tasks in Isolation

```typescript
// Import task directly
import { GET } from '@mycompany/http-tracing';

// Create minimal root context for testing
const testRoot = httpLib.prefix('test').use({
  retry: mockRetryLib.prefix('retry').use(),
});

const result = await GET(testRoot, 'https://test.example.com');

expect(result.success).toBe(true);
```

## Migration from Previous APIs

### From `createModuleContext`

```typescript
// OLD
const { task } = createModuleContext({
  moduleMetadata: { gitSha, packageName, packagePath },
  tagAttributes: {
    ...httpLib.tagAttributes,
    ...dbLib.tagAttributes,
    userId: S.category(),
  },
});

// NEW
const appModule = defineModule({
  metadata: { gitSha, packageName, packagePath },
  schema: { userId: S.category() },
  deps: { http: httpLib, db: dbLib },
});

const appRoot = appModule.use({
  http: httpLib.prefix('http').use({ ... }),
  db: dbLib.prefix('db').use(),
});

const { task } = appRoot;  // Or: const { task } = appModule;
```

### From `moduleContextFactory`

```typescript
// OLD
const httpLib = moduleContextFactory(
  'http',
  { gitSha, packageName, packagePath },
  { status: S.number(), method: S.enum([...]) },
);

// NEW
export const httpLib = defineModule({
  metadata: { packageName, packagePath },
  schema: { status: S.number(), method: S.enum([...]) },
});
```

## Benefits Summary

1. **Type-safe composition** - Collision detection at compile time
2. **Explicit dependencies** - Clear declaration of what a module needs
3. **Dependency injection** - Application controls wiring
4. **Shared instances** - Multiple consumers can share a dependency (GraphQL + App sharing HTTP)
5. **Pre-bound context** - No need to pass ctx to every dep call
6. **Testing support** - Easy to mock/override dependencies
7. **Clean library authoring** - Define schema once, prefix applied at use time
8. **Zero hot-path overhead** - Same RemappedBufferView approach for Arrow conversion

## Integration with Other Specs

- **[Trace Schema System](./01a_trace_schema_system.md)**: Schema definition (`S.number()`, `S.category()`, etc.)
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemappedBufferView for Arrow conversion
- **[Context Flow](./01c_context_flow_and_task_wrappers.md)**: How context propagates through task calls
- **[Tree Walker](./01k_tree_walker_and_arrow_conversion.md)**: How Arrow conversion traverses the span tree
