# Op Context Pattern

> **Status**: This spec REPLACES `01l_module_builder_pattern.md` which should be DELETED.

## Quick Reference

```typescript
import { defineOpContext, defineLogSchema, S, defineFeatureFlags } from '@smoothbricks/lmao';

// 1. Define your schema (columns use snake_case)
const appSchema = defineLogSchema({
  user_id: S.category(),
  operation: S.enum(['create', 'read', 'update', 'delete']),
  duration_ms: S.number(),
  request_path: S.category(),
});

// 2. Define feature flags (camelCase - not stored as columns)
const flags = defineFeatureFlags({
  newCheckoutFlow: S.boolean().default(false).async(),
  maxRetries: S.number().default(3).sync(),
  experimentGroup: S.category().default('control').sync(),
});

// 3. Create op context
const { defineOp, defineOps, createTrace, logSchema } = defineOpContext({
  logSchema: appSchema,
  deps: {
    http: httpOps.prefix('http'),
    db: dbOps.mapColumns({ query: 'query', rows: 'db_rows' }),
  },
  flags,
  flagEvaluator: async (ctx, flag, defaultValue) => {
    return launchDarkly.variation(flag, defaultValue, { user: ctx.userId });
  },
  ctx: {
    env: null as Env, // REQUIRED at createTrace
    requestId: null as string, // REQUIRED at createTrace
    userId: undefined as string | undefined, // OPTIONAL - may not be known yet
    config: { retryCount: 3 }, // has default, can override
  },
});

// 4. Define ops
const fetchUser = defineOp('fetchUser', async (ctx, id: string) => {
  ctx.tag.user_id(id).operation('read');
  const user = await ctx.deps.db.query(ctx, `SELECT * FROM users WHERE id = ?`, [id]);
  return ctx.ok(user);
});

// 5. Export as group (for other packages to use)
export const userOps = defineOps({ fetchUser, updateUser, deleteUser });

// 6. Create trace at request boundary
export default {
  async fetch(req: Request, env: Env) {
    const ctx = createTrace({
      env, // required
      requestId: crypto.randomUUID(), // required
      userId: req.headers.get('x-user-id') ?? undefined, // optional
      // config not provided - uses default { retryCount: 3 }
    });
    return ctx.span('request', fetchUser, req.params.id);
  },
};
```

## Core Concepts

### Context Properties (`ctx`) - Runtime Context

**IMPORTANT**: `ctx` properties are **runtime context** (environment, config, services). They are NOT stored in Arrow
columns. For column data, use `setScope()` or `tag`.

Declare ALL properties upfront for V8 hidden class optimization. Three patterns for different initialization needs:

```typescript
ctx: {
  // 1. REQUIRED - must provide at createTrace()
  env: null as Env,                         // null sentinel → Env in SpanContext
  requestId: null as string,                // null sentinel → string in SpanContext

  // 2. OPTIONAL - may or may not be provided
  userId: undefined as string | undefined,  // undefined → string | undefined in SpanContext
  sessionId: undefined as string | undefined,

  // 3. HAS DEFAULT - can override at createTrace()
  config: { retryCount: 3, timeout: 30000 },
  debug: false,
}
```

| Pattern  | Declaration                   | At createTrace() | In SpanContext   |
| -------- | ----------------------------- | ---------------- | ---------------- |
| Required | `null as T`                   | Must provide     | `T`              |
| Optional | `undefined as T \| undefined` | Can omit         | `T \| undefined` |
| Default  | `value`                       | Can override     | `typeof value`   |

**Why all three?**

- **Required**: Environment, request-scoped IDs that MUST exist
- **Optional**: User ID after auth (may not be known yet), optional features
- **Default**: Configuration with sensible defaults

### Scope vs Context Properties

**Two DIFFERENT concepts that serve DIFFERENT purposes:**

| Concept                | Purpose                       | Storage       | Example                               |
| ---------------------- | ----------------------------- | ------------- | ------------------------------------- |
| `ctx` properties       | Runtime context (env, config) | NOT in Arrow  | `ctx.env`, `ctx.config.retryCount`    |
| `setScope({ col: v })` | Column values for ALL rows    | Arrow columns | `ctx.setScope({ request_id: 'abc' })` |

```typescript
// ctx properties - runtime objects, NOT columns
ctx: {
  env: null as Env,        // Access via ctx.env - not stored in Arrow
  config: { retries: 3 },  // Access via ctx.config - not stored in Arrow
}

// setScope - sets COLUMN values for ALL rows in this span
// The column must exist in your logSchema!
ctx.setScope({ request_id: 'abc123' });  // request_id column = 'abc123' on ALL rows
ctx.setScope({ user_id: 'u456' });       // user_id column = 'u456' on ALL rows

// Child spans inherit scope
await ctx.span('child', async (child) => {
  // child.scope.request_id === 'abc123' (inherited)
  // Can read scope values
  child.log.info('Processing request').request_id(child.scope.request_id);
});
```

**When to use which:**

- **ctx properties**: For runtime dependencies (env, services, config objects)
- **setScope**: For column data that should appear on EVERY row in the span (request_id, correlation_id, tenant_id)
- **tag**: For column data on a SPECIFIC row (row 0 typically)

### Dependencies with Column Mapping

Libraries don't know how they'll be wired. App controls column naming:

```typescript
deps: {
  // Prefix: all columns get 'http_' prefix
  http: httpOps.prefix('http'),  // status → http_status

  // Explicit mapping
  pg: postgresOps.mapColumns({
    query: 'query',       // Share column with other deps
    rows: 'pg_rows',      // Rename
    _debug: null,         // Drop - writes ignored
  }),

  // Multiple deps can share columns
  mysql: mysqlOps.mapColumns({ query: 'query', duration: 'mysql_duration' }),
}
```

The **effective schema** = `appSchema & http_* & pg_* & mysql_* & query`.

### Feature Flag Evaluator

Ctx-first pattern. Receives SpanContext without `ff` (prevents recursion):

```typescript
flagEvaluator: async (ctx, flag, defaultValue) => {
  // Can log
  ctx.log.debug(`Evaluating ${flag}`);

  // Can create child spans for external calls
  return ctx.span('launchDarkly', async (child) => {
    child.tag.flag(flag);
    return launchDarkly.variation(flag, defaultValue, { user: ctx.userId });
  });
};
```

### SpanContext API

Inside an Op, `ctx` provides:

```typescript
async (ctx, ...args) => {
  // Tag attributes (row 0)
  ctx.tag.user_id(id).operation('read');
  ctx.tag.with({ user_id: id, operation: 'read' });

  // Logging (rows 2+)
  ctx.log.info('Starting operation');
  ctx.log.debug('Details').user_id(id);
  ctx.log.warn('Slow query').duration_ms(500);
  ctx.log.error('Failed').with({ error_code: 'E001' });

  // Scoped attributes (propagate to children, appear on ALL rows)
  ctx.setScope({ request_id: req.id }); // ALL rows get request_id = req.id
  console.log(ctx.scope.request_id); // Read current scope

  // Feature flags
  if (await ctx.ff.newCheckoutFlow.get()) {
    ctx.ff.newCheckoutFlow.track(); // Record usage
  }
  const retries = ctx.ff.maxRetries.get(); // sync flag

  // Child spans
  const result = await ctx.span('validate', async (child) => {
    child.tag.step('validation');
    return child.ok({ valid: true });
  });

  // Call dependency ops
  const data = await ctx.span('fetch', ctx.deps.http.request, url);

  // Access user context (NOT columns!)
  console.log(ctx.userId, ctx.config.retryCount);

  // Results
  return ctx.ok(data); // Success
  return ctx.err('NOT_FOUND', { id }); // Error with code
  return ctx.ok(data).with({ cached: true }); // With attributes
  return ctx.err('TIMEOUT', {}).message('Request timed out');

  // Buffer access (for Arrow conversion)
  const table = convertToArrowTable(ctx.buffer);
};
```

### Defining Ops

**Single op:**

```typescript
const myOp = defineOp('opName', async (ctx, arg1: string, arg2: number) => {
  return ctx.ok(result);
});
```

**Batch with `this` binding:**

```typescript
const ops = defineOps({
  fetchUser: async (ctx, id: string) => { ... },
  updateUser: async (ctx, id: string, data: Data) => {
    // Can reference other ops in batch via this
    const user = await ctx.span('fetch', this.fetchUser, id);
    return ctx.ok(updated);
  },
});
```

**Export for other packages:**

```typescript
export const myOps = defineOps({ ... });
// Consumer: myOps.prefix('my') or myOps.mapColumns({ ... })
```

### Creating Traces

At request/invocation boundary:

```typescript
const ctx = createTrace({
  // Required (null sentinel) - MUST provide
  env: workerEnv,
  requestId: crypto.randomUUID(),

  // Optional (undefined sentinel) - CAN provide
  userId: user?.id, // undefined if not authenticated yet

  // Default - CAN override
  config: { retryCount: 5, timeout: 60000 },

  // Trace ID - auto-generated if not provided
  traceId: 'custom-trace-id',
});

// Then invoke ops
await ctx.span('main', myOp, arg1, arg2);
```

## Common Patterns

### Library Package

```typescript
// my-http-lib/index.ts
const { defineOp, defineOps } = defineOpContext({
  logSchema: defineLogSchema({
    status: S.number(),
    url: S.category(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  }),
  ctx: { timeout: 30000 },
});

const request = defineOp('request', async (ctx, url: string, opts: RequestOpts) => {
  ctx.tag.url(url).method(opts.method ?? 'GET');
  const res = await fetch(url, { signal: AbortSignal.timeout(ctx.timeout), ...opts });
  ctx.tag.status(res.status);
  return res.ok ? ctx.ok(res) : ctx.err('HTTP_ERROR', { status: res.status });
});

export const httpOps = defineOps({ request, get, post, put, delete: del });
```

### App Wiring Libraries

```typescript
// app/index.ts
import { httpOps } from 'my-http-lib';
import { dbOps } from 'my-db-lib';

const { defineOp, createTrace } = defineOpContext({
  logSchema: defineLogSchema({ request_id: S.category() }),
  deps: {
    http: httpOps.prefix('http'),
    db: dbOps.mapColumns({ query: 'query', rows: 'db_rows' }),
  },
  ctx: { env: null as Env },
});
```

### Nested Dependencies

Libraries declare deps without wiring. App wires everything:

```typescript
// auth-lib uses http-lib internally
const { defineOps } = defineOpContext({
  logSchema: authSchema,
  deps: { http: httpOps }, // Declaration only, no prefix
});
export const authOps = defineOps({ login, logout });

// App wires ALL deps at top level
const { defineOp } = defineOpContext({
  deps: {
    http: httpOps.prefix('http'),
    auth: authOps.prefix('auth'),
    // auth's internal http calls use app's http wiring
  },
});
```

## Reserved Names

Cannot use in `ctx`:

- `buffer`, `tag`, `log`, `scope`, `setScope`, `ok`, `err`, `span`, `ff`, `deps`
- Any name starting with `_`

## Type Inference

```typescript
// logSchema is the EFFECTIVE schema (app + all deps)
const { logSchema } = defineOpContext({ ... });
// logSchema includes: appSchema & http_status & http_url & db_rows & query & ...

// InferSchema extracts the value types
type Schema = InferSchema<typeof logSchema>;
// { user_id: string, http_status: number, query: string, ... }
```

## V8 Optimization

All `ctx` properties must be declared upfront because:

1. **Hidden Class Stability**: V8 optimizes property access when object shape is known at creation
2. **Enumerable Keys**: `defineOpContext` reads all keys via `Object.keys()` to generate optimized classes
3. **No Dynamic Properties**: Adding properties after creation de-optimizes the object

The `null as T` / `undefined as T | undefined` patterns ensure:

- All keys exist in the config object (enumerable)
- TypeScript correctly infers required vs optional at `createTrace()`
- Generated SpanContext class has stable hidden class

## Metadata Injection (Transformer)

The TypeScript transformer injects metadata at the `defineOpContext` call site:

```typescript
// Before transform
const { defineOp } = defineOpContext({ logSchema });

// After transform
const { defineOp } = defineOpContext({
  logSchema,
  __metadata: {
    package_name: '@myorg/mypackage',
    package_file: 'src/operations.ts',
    git_sha: 'abc123',
  },
});
```

This metadata flows to all Ops created from this context, enabling:

- Package attribution in traces
- Source file tracking
- Git SHA for deployment correlation
