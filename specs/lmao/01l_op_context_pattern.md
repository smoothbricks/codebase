# Op Context Pattern <a id="smoo/lmao!n/op-context-pattern"></a>

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
const opContext = defineOpContext({
  logSchema: appSchema,
  deps: {
    http: httpOps.prefix('http'),
    db: dbOps.mapColumns({ query: 'query', rows: 'db_rows' }),
  },
  flags,
  ctx: {
    env: null as Env, // REQUIRED at trace()
    requestId: null as string, // REQUIRED at trace()
    userId: undefined as string | undefined, // OPTIONAL - may not be known yet
    config: { retryCount: 3 }, // has default, can override
  },
});

const { defineOp, defineOps } = opContext;

// 4. Define ops
const fetchUser = defineOp('fetchUser', async (ctx, id: string) => {
  ctx.tag.user_id(id).operation('read');
  const user = await ctx.span('db-query', ctx.deps.db.query, `SELECT * FROM users WHERE id = ?`, [id]);
  return user.isOk() ? ctx.ok(user.value) : ctx.err(user.error); // Rebuild the child's outcome on this context
});

// 5. Export as group (for other packages to use)
export const userOps = defineOps({ fetchUser, updateUser, deleteUser });

// 6. Create tracer and trace at request boundary; the flag evaluator is a TracerOptions value
import { InMemoryFlagEvaluator, TestTracer } from '@smoothbricks/lmao';

const { trace } = new TestTracer(opContext, {
  flagEvaluator: new InMemoryFlagEvaluator(flags.schema, { newCheckoutFlow: true }),
});

export default {
  async fetch(req: Request, env: Env) {
    const user = await trace(
      'request',
      {
        env, // required
        requestId: crypto.randomUUID(), // required
        userId: req.headers.get('x-user-id') ?? undefined, // optional
        // config not provided - uses default { retryCount: 3 }
      },
      fetchUser,
      req.params.id
    );
    return user.isOk() ? Response.json(user.value) : new Response('Not found', { status: 404 });
  },
};
```

## Core Concepts

### What defineOpContext Returns <a id="smoo/lmao!n/op-context-define-returns"></a>

`defineOpContext()` returns an `OpContextFactory` which extends `OpContextBinding`:

```typescript
interface OpContextBinding<T, FF, Deps, UserCtx> {
  readonly [opContextType]: OpContext<T, FF, Deps, UserCtx>; // Phantom type for inference
  readonly logBinding: LogBinding<T>; // Schema + optional remap descriptor
  readonly ctxDefaults: UserCtx; // User context defaults (with null sentinels)
  readonly deps: Deps; // Wired dependencies
}

interface OpContextFactory<T, FF, Deps, UserCtx> extends OpContextBinding<T, FF, Deps, UserCtx> {
  readonly logSchema: LogSchema<EffectiveSchema<T, Deps>>; // Computed effective schema
  readonly flags: FF; // Feature flag schema
  defineOp: DefineOpFn; // Create single Op
  defineOps: DefineOpsFn; // Create multiple Ops + OpGroup
}
```

**Usage:**

```typescript
// Store the full factory
const opContext = defineOpContext({
  logSchema: mySchema,
  ctx: { env: null as Env },
});

// Destructure what you need
const { defineOp, defineOps, logSchema } = opContext;

// Pass to Tracer (accepts OpContextBinding)
const { trace } = new TestTracer(opContext);
```

The phantom type `[opContextType]` enables TypeScript to infer the full context type from the factory, so
`new TestTracer(opContext)` has fully typed `trace()` methods.

### Context Properties (`ctx`) - Runtime Context <a id="smoo/lmao!n/op-context-ctx-props"></a>

**IMPORTANT**: `ctx` properties are **runtime context** (environment, config, services). They are NOT stored in Arrow
columns. For column data, use `setScope()` or `tag`.

Declare ALL properties upfront for V8 hidden class optimization. Three patterns for different initialization needs:

```typescript
ctx: {
  // 1. REQUIRED - must provide at trace()
  env: null as Env,                         // null sentinel → Env in SpanContext
  requestId: null as string,                // null sentinel → string in SpanContext

  // 2. OPTIONAL - may or may not be provided
  userId: undefined as string | undefined,  // undefined → string | undefined in SpanContext
  sessionId: undefined as string | undefined,

  // 3. HAS DEFAULT - can override at trace()
  config: { retryCount: 3, timeout: 30000 },
  debug: false,
}
```

| Pattern  | Declaration                   | At trace()   | In SpanContext   |
| -------- | ----------------------------- | ------------ | ---------------- |
| Required | `null as T`                   | Must provide | `T`              |
| Optional | `undefined as T \| undefined` | Can omit     | `T \| undefined` |
| Default  | `value`                       | Can override | `typeof value`   |

**Why all three?**

- **Required**: Environment, request-scoped IDs that MUST exist
- **Optional**: User ID after auth (may not be known yet), optional features
- **Default**: Configuration with sensible defaults

### Scope vs Context Properties <a id="smoo/lmao!n/op-context-scope-vs-ctx"></a>

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

### Dependencies with Column Mapping <a id="smoo/lmao!n/column-mapping-api"></a>

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

### Feature Flag Evaluator <a id="smoo/lmao!n/op-context-flag-evaluator"></a>

The evaluator is a `FlagEvaluator<Ctx>` object passed as `TracerOptions.flagEvaluator`
([Feature Flags](./01p_feature_flags.md#smoo/lmao!n/op-context-flag-evaluator.evaluator-interface)). Its methods are
ctx-first and receive SpanContext without `ff` (prevents recursion):

```typescript
// Inside a class implementing FlagEvaluator<AppCtx>
async getAsync(ctx: SpanContextWithoutFf<AppCtx>, flag: string) {
  // Can log
  ctx.log.debug('Evaluating flag');

  // Can create child spans for external calls
  const evaluated = await ctx.span('launchDarkly', async (child) => {
    child.tag.flag(flag);
    return child.ok(await launchDarkly.variation(flag, null, { user: ctx.userId }));
  });
  return evaluated.isOk() ? evaluated.value : null;
}
```

### SpanContext API <a id="smoo/lmao!n/op-context-spancontext-api"></a>

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
  const newCheckoutFlow = await ctx.ff.get('newCheckoutFlow');
  if (newCheckoutFlow) {
    newCheckoutFlow.track({ action: 'checkout_route', outcome: 'new' });
  }
  const retries = ctx.ff.maxRetries; // sync flag wrapper
  const retryCount = retries?.value ?? 0;

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
  return ctx.err(NOT_FOUND({ id })); // Error with code (NOT_FOUND from defineCodeError)
  return ctx.ok(data).with({ cached: true }); // With attributes
  return ctx.err(TIMEOUT({})).message('Request timed out');

  // Buffer access (for Arrow conversion)
  const table = convertToArrowTable(ctx.buffer);
};
```

## Result Types <a id="smoo/lmao!n/op-context-result-types"></a>

LMAO results serve two purposes with one pair of classes:

1. **`Result<V, E>` type** - For type-safe error handling with `isErr(Tag)` discrimination
2. **`Ok`/`Err`** - For fluent result builders that write row 1 eagerly (returned by `ctx.ok()`/`ctx.err()`)

`Result<V, E>` is the union `Ok<V> | Err<E>` (`packages/lmao/src/lib/result.ts`). The same `Ok`/`Err` instances are both
the type-safe result (`isOk`/`isErr(Tag)`/`map`/`match`/`unwrapOr`) and the fluent buffer-row-1 writer
(`.with()`/`.message()`/`.line()`) — there is no separate fluent-builder class.

### Result Type

A span returns a `Result<V, E>`, and its methods provide type-safe error handling. An op completes only with a result
from its own `ctx.ok()` or `ctx.err()`; to propagate a child's outcome, rebuild it on the receiving context
([Entry Types](./01h_entry_types_and_logging_primitives.md#smoo/lmao!n/lmao-entry-fluent-result-integration)):

```typescript
import type { Result } from '@smoothbricks/lmao';

// Receiving a child's result (inside an op)
const result: Result<User, NotFound> = await ctx.span('fetch-user', fetchUser, userId);

// Basic checks
if (result.isOk()) {
  console.log(result.value);
}
if (result.isErr()) {
  console.log(result.error);
}

// Tagged error discrimination (for use with a workflow engine or custom error types)
if (result.isErr(Blocked)) {
  return ctx.err(result.error); // Propagate on this context - engine handles retry
}
if (result.isErr(NotFound)) {
  return ctx.ok(null); // Handle specifically
}

// Predicate-based checks
if (result.isErr((e) => e.code === 'TIMEOUT')) {
  // Handle timeout
}

// Functional methods
const mapped = result.map((user) => user.name);
const recovered = result.unwrapOr(defaultUser);
const matched = result.match({
  ok: (user) => `Found: ${user.name}`,
  err: (error) => `Error: ${error.message}`,
});
```

### Tagged Errors <a id="smoo/lmao!n/op-context-tagged-errors"></a>

Tagged errors implement `TaggedError<Tag>` for use with `result.isErr(Tag)`:

```typescript
import { TaggedError } from '@smoothbricks/lmao';

// Define a tagged error class
class NotFound implements TaggedError<'NotFound'> {
  static readonly _tag = 'NotFound' as const;
  readonly _tag = 'NotFound' as const;

  constructor(
    readonly resource: string,
    readonly id: string
  ) {}
}

// Use with isErr()
const result = await fetchUser(id);
if (result.isErr(NotFound)) {
  // TypeScript knows result.error is NotFound
  console.log(`${result.error.resource} ${result.error.id} not found`);
}
```

**Built-in Tagged Errors (tree-shakable):**

LMAO provides `Blocked` and `RetriesExhausted` for engine integration. Import only if needed:

```typescript
import { Blocked, RetriesExhausted } from '@smoothbricks/lmao';

// In an Op that calls external services
const result = await ctx.deps.indexStore.query(args);
if (result.isErr(Blocked)) {
  return ctx.err(result.error); // Propagate on this context - the engine handles retry
}

// After max retries exhausted
if (result.isErr(RetriesExhausted)) {
  // Service was unavailable too long - compensate
  return ctx.err(SERVICE_UNAVAILABLE({ service: result.error.reason }));
}
```

### Fluent Builders (ctx.ok / ctx.err) <a id="smoo/lmao!n/op-context-fluent-builders"></a>

`ctx.ok()` and `ctx.err()` return fluent builders that write to buffer row 1:

```typescript
// Success with attributes
return ctx.ok(user).with({ cached: true }).message('User fetched from cache');

// Error with attributes
return ctx
  .err(NOT_FOUND({ id }))
  .message('User not found')
  .with({ searched_tables: ['users', 'archived_users'] });
```

### Chaining Methods

| Method           | Purpose                               |
| ---------------- | ------------------------------------- |
| `.with(attrs)`   | Set schema attributes on result entry |
| `.message(text)` | Set message column                    |
| `.line(n)`       | Set source line number                |

### Entry Type Detection <a id="smoo/lmao!n/op-context-entry-detection"></a>

The Tracer writes the span-end entry for every completion:

- If the function returns an `Ok` or `Err` created by its own context: the chained methods already wrote row 1's
  attributes, message, and line; the Tracer writes `span-ok` or `span-err`, the timestamp, and `error_code` to row 1
- If it returns anything else, including an `Ok`/`Err` from another context: the Tracer throws a `TypeError` and writes
  `span-exception` to row 1
- If it throws: the Tracer writes `span-exception` to row 1

### Error Handling Policy (Return vs Throw)

- Use `ctx.err(...)`/`Err` for known operational outcomes (validation failures, business rules, transient conditions,
  blocked state, missing optional dependencies).
- Reserve `throw` for invariant violations or programmer/configuration bugs that should not occur in normal execution.
- `span-err` means expected failure represented as a value; `span-exception` means an unexpected thrown exception.
- Do not use `throw` to signal known retry behavior; retry decisions should flow through typed error values.

### Defining Ops <a id="smoo/lmao!n/op-context-defining-ops"></a>

**Single op:**

```typescript
const myOp = defineOp('opName', async (ctx, arg1: string, arg2: number) => {
  return ctx.ok(result);
});
```

**Batch:** `defineOps` wraps each function with `defineOp` under its key and returns an `OpGroup`. An op calls a sibling
through the sibling's own `Op` value; nothing binds `this`.

```typescript
const fetchUser = defineOp('fetchUser', async (ctx, id: string) => { ... });

const updateUser = defineOp('updateUser', async (ctx, id: string, data: Data) => {
  const user = await ctx.span('fetch', fetchUser, id);
  return ctx.ok(updated);
});

const ops = defineOps({ fetchUser, updateUser });
```

**Export for other packages:**

```typescript
export const myOps = defineOps({ ... });
// Consumer: myOps.prefix('my') or myOps.mapColumns({ ... })
```

## Tracer Architecture <a id="smoo/lmao!n/tracer-architecture"></a>

The Tracer is an abstract base class that manages trace execution and lifecycle. Concrete implementations define how
trace data is collected, batched, and processed.

### Abstract Tracer Class

The `Tracer` class provides the foundation for all trace collection:

**Constructor Signature:**

```typescript
constructor(binding: B, options: TracerOptions<B['logBinding']['logSchema']>)
```

- `binding` - The OpContextBinding returned from `defineOpContext()`
- `options.bufferStrategy` - Buffer strategy for memory management and Arrow conversion (required)
- `options.createTraceRoot` - Factory for platform-specific TraceRoot instances (time anchors, 01b3) (required)
- `options.flagEvaluator` - Optional feature flag evaluator for runtime flag resolution

The constructor binds `trace`, `trace_op`, `trace_fn`, and `flush` so they can be destructured.

**Lifecycle Hooks (Abstract Methods):**

All subclasses must implement these five lifecycle hooks:

| Hook                    | When Called                                      | Purpose                               |
| ----------------------- | ------------------------------------------------ | ------------------------------------- |
| `onTraceStart()`        | Before root span function executes               | Initialize trace (e.g., start timer)  |
| `onTraceEnd()`          | After root span completes (in finally block)     | Collect completed trace (e.g., queue) |
| `onSpanStart()`         | Before child span function executes              | Track child span lifecycle            |
| `onSpanEnd()`           | After child span completes (in finally block)    | Process completed child span          |
| `onStatsWillResetFor()` | Before buffer stats reset during capacity tuning | Capture stats before they're lost     |

All hooks receive the `SpanBuffer` as their argument, providing access to:

- `buffer._stats` - Current stats (totalWrites, spansCreated, capacity)
- `buffer._opMetadata` - Which Op/module this buffer belongs to
- `buffer.trace_id` - The trace identifier
- `walkSpanTree(buffer, visitor)` - Child span tree, from the trace's topology

**Public Methods:**

- `trace()` - Polymorphic trace creation function (see overloads below)
- `trace_op()` - Monomorphic Op execution (transformer optimization)
- `trace_fn()` - Monomorphic function execution (transformer optimization)
- `flush()` - Optional flush method (default no-op, override for batching)

### Concrete Implementations <a id="smoo/lmao!n/tracer-implementations"></a>

| Tracer             | Purpose                                           | Key Features                                           |
| ------------------ | ------------------------------------------------- | ------------------------------------------------------ |
| `TestTracer`       | Test inspection and Arrow conversion              | `rootBuffers[]`, `statsSnapshots[]`, `clear()`         |
| `NoOpTracer`       | Supported no-op implementation (not repo-default) | All hooks are no-ops, zero overhead                    |
| `StdioTracer`      | Development debugging with console output         | Color-coded trace IDs, indented tree output, durations |
| `ArrayQueueTracer` | Production batching before backend send           | `queue[]`, `drain()` method for batch processing       |

**TestTracer:**

Accumulates root buffers for inspection and Arrow table conversion. Child spans are reached with
`walkSpanTree(root, visitor)`.

```typescript
const tracer = new TestTracer(opContext);
const { trace } = tracer;
await trace('fetch', fetchOp);

expect(tracer.rootBuffers).toHaveLength(1);
const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);
```

**NoOpTracer:**

`NoOpTracer` in `@smoothbricks/lmao` is provided for API proof, comparison, and overhead benchmarking, but this repo's
default policy is to use `TestTracer`, `ArrayQueueTracer`, or `StdioTracer` so tracing stays observable during tests and
runtime debugging.

**StdioTracer:**

Prints spans to stdout/stderr with colors, indentation, and timestamps. Ideal for development.

```typescript
const { trace } = new StdioTracer(opContext);
await trace('request', handleRequest);
// [2025-12-25T10:30:45.123Z] [123456] request
// [2025-12-25T10:30:45.125Z] [123456]   ├─ db-query
// [2025-12-25T10:30:45.225Z] [123456]   └─ db-query [OK] (100.00ms)
```

**ArrayQueueTracer:**

Batches completed traces in memory. Use `drain()` to consume and process batches.

```typescript
const tracer = new ArrayQueueTracer(opContext);
const { trace } = tracer;

await trace('req-1', handleRequest);
await trace('req-2', handleRequest);

const batch = tracer.drain(); // Returns and clears queue
for (const buf of batch) {
  const table = convertSpanTreeToArrowTable(buf);
  await sendToBackend(table);
}
```

### trace() Method Signatures <a id="smoo/lmao!n/tracer-architecture.trace-overloads"></a>

The `trace()` method has 8 overloads supporting various combinations:

**With Op (4 overloads):**

```typescript
// Without line number (manual calls)
trace<S, E>(name: string, op: Op<Ctx, [], S, E>): Promise<Result<S, E>>
trace<S, E>(name: string, overrides: TraceOverrides<UserCtx>, op: Op<Ctx, [], S, E>): Promise<Result<S, E>>

// With line number (transformer-injected)
trace<S, E>(line: number, name: string, op: Op<Ctx, [], S, E>): Promise<Result<S, E>>
trace<S, E>(line: number, name: string, overrides: TraceOverrides<UserCtx>, op: Op<Ctx, [], S, E>): Promise<Result<S, E>>
```

**With inline function (4 overloads):**

```typescript
// Without line number (manual calls)
trace<R>(name: string, fn: (ctx: SpanContext<Ctx>) => R): R
trace<R>(name: string, overrides: TraceOverrides<UserCtx>, fn: (ctx: SpanContext<Ctx>) => R): R

// With line number (transformer-injected)
trace<R>(line: number, name: string, fn: (ctx: SpanContext<Ctx>) => R): R
trace<R>(line: number, name: string, overrides: TraceOverrides<UserCtx>, fn: (ctx: SpanContext<Ctx>) => R): R
```

**TraceOverrides:**

Allows providing required context and optional distributed trace ID:

```typescript
type TraceOverrides<UserCtx> = Partial<UserCtx> & { trace_id?: TraceId };
```

- All `ctx` properties with `null` sentinel must be provided
- Optional properties (`undefined` sentinel) can be omitted
- Default properties can be overridden
- `trace_id` is optional (auto-generated if not provided)

### Usage Examples

**Basic usage with TestTracer:**

```typescript
import { defineOpContext, defineLogSchema, S, TestTracer } from '@smoothbricks/lmao';

const opContext = defineOpContext({
  logSchema: defineLogSchema({
    user_id: S.category(),
    operation: S.enum(['create', 'read', 'update', 'delete']),
  }),
  ctx: { env: null as Env }, // Required at trace time
});

const tracer = new TestTracer(opContext);
const { trace } = tracer;

await trace('fetch-user', fetchUserOp, userId);

// Inspect results
const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);
```

**With context overrides:**

```typescript
// Required ctx properties must be provided in overrides
await trace('handle-request', { env: myEnv, requestId: 'req-123' }, handleRequestOp);

// With distributed tracing
await trace('downstream', { env: myEnv, trace_id: incomingTraceId }, processOp);
```

**Inline functions:**

```typescript
await trace('custom-logic', async (ctx) => {
  ctx.tag.user_id('u123');
  ctx.log.info('Processing');
  return ctx.ok({ processed: true });
});

// With overrides
await trace('custom-logic', { env: myEnv }, async (ctx) => {
  return ctx.ok('done');
});
```

**Production batching with ArrayQueueTracer:**

```typescript
// Cloudflare Worker example
export default {
  async fetch(req: Request, env: Env, execCtx: ExecutionContext) {
    const tracer = new ArrayQueueTracer(opContext);
    const { trace } = tracer;

    const result = await trace('request', { env }, handleRequest, req); // handleRequest returns ctx.ok(Response)

    // Batch send in background
    execCtx.waitUntil(
      (async () => {
        const batch = tracer.drain();
        for (const buf of batch) {
          const table = convertSpanTreeToArrowTable(buf);
          await env.TRACES_QUEUE.send(table);
        }
      })()
    );

    return result.isOk() ? result.value : new Response('Internal error', { status: 500 });
  },
};
```

**Development debugging with StdioTracer:**

```typescript
const { trace } = new StdioTracer(opContext);
await trace('server-start', initializeServer);
// See colored, indented output in terminal
```

**Feature flags:**

```typescript
const tracer = new TestTracer(opContext, {
  flagEvaluator: new LaunchDarklyEvaluator(ldClient, flags.schema), // A FlagEvaluator<Ctx> (see 01p)
});
```

## Common Patterns

### Hybrid Lambda Direct Invoke

When an OpGroup is used as a direct invoke command surface (`{ op: 'name', ...fields }`), map payload fields to a single
object arg:

- preferred Op signatures: `(ctx)` or `(ctx, argObject)`
- avoid multi-positional args for JSON invoke transports

This keeps invoke handling deterministic and compatible with JSON shape-based dispatch contracts.

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
  return res.ok ? ctx.ok(res) : ctx.err(HTTP_ERROR({ status: res.status }));
});

export const httpOps = defineOps({ request, get, post, put, delete: del });
```

### App Wiring Libraries

```typescript
// app/index.ts
import { httpOps } from 'my-http-lib';
import { dbOps } from 'my-db-lib';

const { defineOp } = defineOpContext({
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

## Reserved Names <a id="smoo/lmao!n/op-context-reserved-names"></a>

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
- TypeScript correctly infers required vs optional at `trace()`
- Generated SpanContext class has stable hidden class

## Metadata Injection (Transformer) <a id="smoo/lmao!n/metadata-injection"></a>

The TypeScript transformer injects metadata as the third argument of each `defineOp` call:

```typescript
// Before transform
const fetchUser = defineOp('fetchUser', fn);

// After transform
const fetchUser = defineOp('fetchUser', fn, {
  package_name: '@myorg/mypackage',
  package_file: 'src/operations.ts',
  git_sha: 'abc123',
  line: 12,
});
```

This metadata becomes the Op's `OpMetadata`, enabling:

- Package attribution in traces
- Source file tracking
- Git SHA for deployment correlation

## Retry Handling <a id="smoo/lmao!n/op-retry"></a>

Ops support automatic retry for transient failures. Retry policy is embedded in the error itself via
`TransientError.policy`.

### Defining Retryable Errors

Use the `Transient()` factory with a default retry policy:

```typescript
import { Transient, exponentialBackoff, fixedDelay } from '@smoothbricks/lmao/errors/Transient';

// Define transient error codes with retry policies
const SERVICE_UNAVAILABLE = Transient<{ service: string }>('SERVICE_UNAVAILABLE', exponentialBackoff(5));
const RATE_LIMITED = Transient<{ retryAfter?: number }>('RATE_LIMITED', fixedDelay(3, 5000));

// In op body:
const fetchData = defineOp('fetchData', async (ctx, url: string) => {
  const response = await fetch(url);

  if (response.status === 503) {
    // Uses default exponential backoff (5 attempts)
    return ctx.err(SERVICE_UNAVAILABLE({ service: url }));
  }

  if (response.status === 429) {
    const retryAfter = parseInt(response.headers.get('Retry-After') ?? '5000', 10);
    // Override policy based on server hint
    return ctx.err(RATE_LIMITED({ retryAfter }, fixedDelay(1, retryAfter)));
  }

  return ctx.ok(await response.json());
});
```

### How Retry Works

1. Op `fn` is invoked
2. If result is `TransientError`:
   - Check `error.policy.maxAttempts` - if exhausted, return error
   - Calculate delay based on `error.policy.backoff`
   - Write `span-retry` entry to buffer (observability)
   - Wait for delay
   - Re-invoke `fn` (same SpanContext, fresh execution)
3. If result is `BlockedError`: return immediately (no retry)
4. If result is any other error: return immediately (no retry)
5. If result is success: return immediately

### Key Design Decisions

- **Retry in span execution, not caller**: Retry is transparent to span() callers
- **Policy on error, not op**: Each error type declares its own retry behavior
- **Per-call override**: Can adjust policy for specific cases (e.g., Retry-After header)
- **Blocked is NOT transient**: BlockedError returns to decide() for canary pattern
- **Trace-only logging**: span-retry entries are in trace, not event log

### Retry vs Blocked

| Error Type     | instanceof Check                  | Retry Behavior                 |
| -------------- | --------------------------------- | ------------------------------ |
| TransientError | `error instanceof TransientError` | Retry up to policy.maxAttempts |
| BlockedError   | `error instanceof Blocked`        | Return to caller immediately   |
| Other errors   | Neither                           | Return to caller immediately   |

**Why the distinction?**

- `TransientError`: Temporary failure, same call will likely succeed soon (network blip, service restart)
- `BlockedError`: Structural unavailability, needs coordination (service down, quota exceeded)

### Observability

Each retry attempt writes a `span-retry` entry:

```
span-start: op:fetchPayment
span-retry: retry:op:fetchPayment (attempt=1, delay=100ms)
span-retry: retry:op:fetchPayment (attempt=2, delay=200ms)
span-ok: op:fetchPayment (success on attempt 3)
```

Count span-retry entries to measure retry frequency. Alert on patterns like "P99 of retries > 2".
