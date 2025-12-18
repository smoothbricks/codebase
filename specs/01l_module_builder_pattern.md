# Module Builder Pattern

## Overview

This spec defines the **Module Builder Pattern** - a fluent API for defining traced modules with the `op()` pattern for
creating traced operations. This design provides:

1. **Destructured context** - Ops receive `{ span, log, tag, deps, ff }` plus user-extensible `Extra` properties
2. **Caller-controlled span naming** - `span('name', op, ...args)` lets callers name spans contextually
3. **Zero-allocation deps** - Dependencies are plain object references, not per-call closures
4. **Type-safe composition** - Compile-time collision detection and dependency validation
5. **User-extensible context** - `.ctx<Extra>()` allows adding custom properties like `env`, `requestId`, `userId`
6. **Module-owned TraceContext creation** - `module.traceContext()` creates the root context with type-safe Extra

## Design Rationale

### Why Destructured Context

Without destructuring, every function signature needs a `ctx` parameter:

```typescript
// ctx drilling is repetitive
const createUser = (ctx, userData) => {
  ctx.tag.userId(userData.id);
  await validateUser(ctx, userData); // Pass ctx everywhere
};
```

Destructuring solves this by letting ops declare what they need:

```typescript
// Only destructure what you use
const createUser = op(async ({ tag, span }, userData) => {
  tag.userId(userData.id);
  await span('validate', validateUser, userData);
});
```

### Why Caller Names Spans

If the callee names its span, the caller loses context:

```typescript
// Callee-named: always "validate-user" regardless of context
await validateUser(userData);
```

With caller-controlled naming, the same op can have contextual names:

```typescript
// Caller provides contextual name
await span('validate-new-user', validateUser, newUserData);
await span('validate-existing-user', validateUser, existingUserData);
```

### Why Zero-Allocation Deps

Creating closures per invocation causes allocation overhead:

```typescript
// Closure allocation per call (avoid this)
deps.retry = {
  attempt: (n) => retryLib.attempt(ctx, n), // New closure each time
};
```

The op pattern uses plain object references - deps are Op instances, not closures.

### Why Explicit Deps Declaration

Object spread can silently overwrite properties:

```typescript
// Silent collision (bad)
const schema = { ...httpLib.schema, ...dbLib.schema, status: S.number() };
```

Explicit deps with TypeScript catches collisions at compile time:

```typescript
// TypeScript error on collision
const appModule = defineModule({
  schema: { userId: S.category() },
  deps: { http: httpModule, db: dbModule }, // Collision detected!
});
```

### The Op Pattern

The op pattern addresses all these concerns:

1. **Destructured context** - `op(async ({ span, log, tag }, ...) => {})`
2. **Caller names spans** - `span('contextual-name', someOp, args)`
3. **Zero-allocation deps** - Plain object references to Op instances
4. **Explicit deps declaration** - TypeScript catches collisions

## Core API

### Defining a Module

```typescript
// Define module with schema, dependencies, and feature flags
// metadata is injected by TypeScript transformer
const httpModule = defineModule({
  metadata: { packageName: '@mycompany/http', packagePath: 'src/index.ts' }, // Transformer injects
  schema: {
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    url: S.text(),
    duration: S.number(),
  },
  deps: {
    retry: retryModule,
  },
  ff: {
    premiumApi: ff.boolean(),
  },
}).ctx<{
  env: { apiTimeout: number; region: string };
  requestId: string; // User-defined, NOT a system prop
  userId?: string; // User-defined, NOT a system prop
}>();
```

**Key Design: `.ctx<Extra>()`**

The `.ctx<Extra>()` method specifies user-extensible context properties beyond the built-in `span`, `log`, `tag`,
`deps`, and `ff`. This is where you add environment bindings, request identifiers, or other request-scoped data:

```typescript
// Cloudflare Worker bindings example
const workerModule = defineModule({
  metadata: { packageName: '@mycompany/worker', packagePath: 'src/index.ts' }, // Transformer injects
  schema: { ... },
  deps: { ... },
  ff: { ... },
}).ctx<{
  env: {
    KV: KVNamespace;
    R2: R2Bucket;
    AI: AIBinding;
  };
  requestId: string;  // User-defined in Extra
  userId?: string;    // User-defined in Extra
}>();

// Create trace context at request entry
const ctx = workerModule.traceContext({
  ff: ffEvaluator,
  env: workerEnv,
  requestId: request.headers.get('x-request-id')!,
  userId: session?.userId,
});

// The Extra type flows through to all ops created from this module
const { op } = workerModule;

const processRequest = op(async ({ env, log, requestId }) => {
  const value = await env.KV.get('key');  // Fully typed!
  log.info('Retrieved from KV');
  // requestId available via Extra
});
```

### Defining Ops

```typescript
// Destructure op factory
const { op } = httpModule;

// Define ops - context destructured in signature
const request = op(async ({ span, log, tag }, url: string, opts: RequestOpts) => {
  tag.method(opts.method).url(url);
  log.info('Making HTTP request');

  const startTime = performance.now();
  const res = await fetch(url, opts);

  tag.status(res.status).duration(performance.now() - startTime);
  return res;
});

// Ops can call other ops via span()
const GET = op(async ({ span, log, tag, deps }, url: string) => {
  log.info('Starting GET request');

  const { retry } = deps; // Destructure deps!

  try {
    return await span('fetch', request, url, { method: 'GET' });
  } catch (e) {
    log.warn('Request failed, retrying');
    await span('retry', retry, 1);
    throw e;
  }
});
```

### Creating Root Context

```typescript
// Wire dependencies with prefixes at composition time
const httpRoot = httpModule.prefix('http').use({
  retry: retryModule.prefix('http_retry').use(),
});

// Root invocation via span()
const result = await httpRoot.span('fetch-users', GET, 'https://api.example.com/users');
```

## The Op Pattern Explained

### What op() Does

The `op()` factory wraps a user function in an Op class:

```typescript
const { op } = someModule;

// op() captures module metadata
const myOp = op(async ({ span, log, tag }, arg1, arg2) => {
  // User code here
});

// myOp is an Op instance that knows:
// - Its source module (for observability)
// - How to create its span buffer
// - How to set up the OpContext
```

### What span() Does

When `span('name', op, ...args)` is called:

1. **Creates SpanBuffer** with the op's module schema (unprefixed internally)
2. **Registers with parent** via children array (RemappedBufferView if prefixed)
3. **Writes span-start entry** with the caller-provided name
4. **Creates OpContext** with `{ span, log, tag, deps, ff }` plus user `Extra` properties
5. **Executes user function** with try/catch for span-exception
6. **Writes span-ok or span-exception** on completion

```typescript
// Caller provides contextual name
await span('validate-new-user', validateUser, newUserData);
await span('validate-update', validateUser, existingUserData);

// Same op, different span names based on context!
```

### span() Overloads (6 Total)

The `span()` function has 6 overloads to support different use cases. The TypeScript transformer injects line numbers as
the first argument for source mapping:

```typescript
// WITH line number (transformer output):
span(line, name, ctx, op, ...args); // Context override + Op
span(line, name, op, ...args); // Op only
span(line, name, fn); // Inline closure

// WITHOUT line number (user writes):
span(name, ctx, op, ...args); // Context override + Op
span(name, op, ...args); // Op only
span(name, fn); // Inline closure
```

**Overload Details**:

1. **`span(name, op, ...args)`** - Most common. Invoke an Op with arguments:

   ```typescript
   await span('fetch-user', fetchUserOp, userId);
   ```

2. **`span(name, fn)`** - Inline closure for one-off operations:

   ```typescript
   await span('compute', async () => {
     return heavyComputation();
   });
   ```

3. **`span(name, ctx, op, ...args)`** - Context override for passing custom properties:
   ```typescript
   // Override env for this specific call
   await span('external-call', { env: prodEnv }, externalOp, url);
   ```

**Contravariance at span()**:

When invoking `span(name, op, ...args)`, the current context must be a SUPERSET of what the Op requires. If the Op was
defined with `{ env: { region: string } }`, the invoking context must have at least those properties.

```typescript
// Op requires { env: { region: string } }
const regionOp = op(async ({ env }) => {
  return env.region;
});

// Current context has { env: { region: string; debug: boolean } }
// This works - current context is a superset
await span('get-region', regionOp);
```

### Why Caller Names Spans

The caller often has better context than the callee:

```typescript
const notify = op(async ({ log, tag }, userId: string, type: string) => {
  // Generic notification op
});

// Caller provides context-specific names
await span('notify-order-confirmed', notify, customerId, 'order_confirmed');
await span('notify-payment-failed', notify, customerId, 'payment_failed');
await span('notify-shipping-update', notify, customerId, 'shipping');
```

## Context Destructuring

### Full Destructuring

```typescript
const processOrder = op(async ({ span, log, tag, deps, ff, env }, order: Order) => {
  // All context properties available
  // - span, log, tag, deps, ff are built-in
  // - env comes from .ctx<Extra>() declaration
  tag.orderId(order.id);
  log.info('Processing order');

  const { inventory, payments } = deps;
  const { premiumProcessing } = ff;
  const region = env.region; // Typed from .ctx<{ env: { region: string } }>()

  // ...
});
```

### Partial Destructuring

Ops only destructure what they need:

```typescript
// Only needs logging
const logMetrics = op(async ({ log }, metrics: Metrics) => {
  log.info('Recording metrics');
  // ...
});

// Only needs tagging and span
const enrichData = op(async ({ span, tag }, data: Data) => {
  tag.dataType(data.type);
  await span('transform', transformOp, data);
  // ...
});

// Only needs deps
const orchestrate = op(async ({ span, deps }, request: Request) => {
  const { auth, validation, processing } = deps;
  await span('authenticate', auth.verify, request.token);
  await span('validate', validation.check, request.body);
  await span('process', processing.handle, request);
});
```

### Deps Destructuring

Dependencies can be destructured for cleaner code:

```typescript
const handleRequest = op(async ({ span, log, deps }, req: Request) => {
  // Destructure deps
  const { auth, cache, db, notify } = deps;

  log.info('Handling request');

  // Use deps via span()
  const user = await span('authenticate', auth.verify, req.token);
  const cached = await span('check-cache', cache.get, req.key);

  if (!cached) {
    const data = await span('query-db', db.fetch, req.query);
    await span('update-cache', cache.set, req.key, data);
  }

  await span('notify', notify.send, user.id, 'request_complete');
});
```

## Module Composition

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
    endpoint: S.category(),
    status: S.number(),
  },
  deps: {
    http: httpModule,
    db: dbModule,
  },
  ff: {
    premiumApi: ff.boolean(),
  },
}).ctx<{
  env: { region: string; KV: KVNamespace };
  requestId: string;
  userId?: string;
}>();

const { op } = appModule;

const handleRequest = op(async ({ span, log, tag, deps, requestId, userId }, req: Request) => {
  // requestId and userId are available via Extra - user-defined, NOT system props
  tag.endpoint(req.path);
  log.info('Processing request');

  const { http, db } = deps;

  const data = await span('query', db.fetch, req.query);
  const response = await span('external-call', http.GET, req.externalUrl);

  return { data, response };
});
```

### Wiring Dependencies

```typescript
// Create root context with all deps wired
const appRoot = appModule.use({
  http: httpModule.prefix('http').use({
    retry: retryModule.prefix('http_retry').use(),
  }),
  db: dbModule.prefix('db').use(),
});

// Invoke app ops
await appRoot.span('handle-request', handleRequest, incomingRequest);
```

### Shared Dependencies

Multiple consumers can share a dependency instance:

```typescript
// GraphQL library needs HTTP
const graphqlModule = defineModule({
  metadata: { packageName: '@mycompany/graphql', packagePath: 'src/index.ts' },
  schema: {
    query: S.text(),
    operationName: S.category(),
  },
  deps: {
    http: httpModule,  // GraphQL uses HTTP
  },
});

// Application uses both GraphQL and HTTP directly
const appModule = defineModule({
  metadata: { ... },
  schema: { userId: S.category() },
  deps: {
    graphql: graphqlModule,
    http: httpModule,  // App also uses HTTP
  },
});

// Wire so GraphQL and App share the SAME http instance
const httpInstance = httpModule.prefix('http').use({
  retry: retryModule.prefix('http_retry').use(),
});

const appRoot = appModule.use({
  http: httpInstance,                              // App's HTTP
  graphql: graphqlModule.prefix('graphql').use({
    http: httpInstance,                            // GraphQL's HTTP = SAME!
  }),
});

// Result: Both write to http_status, http_method, etc.
// No duplicate graphql_http_status columns!
```

## TraceContext and Module.traceContext()

### TraceContext Type

`TraceContext` is the root context created at request entry points. It combines system properties with user-defined
`Extra` properties:

```typescript
// Reserved keys that Extra cannot contain (compile-time enforcement)
type ReservedTraceContextKeys = keyof {
  traceId: unknown;
  anchorEpochMicros: unknown;
  anchorPerfNow: unknown;
  threadId: unknown;
  ff: unknown;
  span: unknown;
};

// TraceContext = System props + Extra
type TraceContext<FF, Extra> = {
  // System properties (always present)
  traceId: string;
  anchorEpochMicros: number;
  anchorPerfNow: number;
  threadId: bigint;
  ff: FeatureFlagEvaluator<FF>;
  span: RootSpanFn<FF, Extra>;
} & Extra; // User-defined properties (e.g., requestId, userId, env)
```

**Key Design**: `requestId` and `userId` are NOT system properties - they are user-defined in `Extra` via
`.ctx<Extra>()`. This allows applications to define whatever request-scoped data they need.

### Module.traceContext() Method

The module provides `traceContext()` to create a type-safe root context:

```typescript
// At request entry - create trace context
const ctx = appModule.traceContext({
  ff: ffEvaluator,
  env: workerEnv,
  requestId: req.headers.get('x-request-id')!,
  userId: session?.userId,
});

// Invoke first op via ctx.span()
await ctx.span('handle-request', handleRequestOp, req);
```

### Reserved Keys Enforcement

TypeScript prevents Extra from containing reserved keys at compile time:

```typescript
// ✅ Valid - user-defined properties
const appModule = defineModule({ ... }).ctx<{
  env: { region: string };
  requestId: string;
  userId?: string;
}>();

// ❌ Compile error - traceId is reserved
const badModule = defineModule({ ... }).ctx<{
  traceId: string;  // Error: 'traceId' is a reserved TraceContext key
}>();

// ❌ Compile error - ff is reserved
const badModule2 = defineModule({ ... }).ctx<{
  ff: { custom: true };  // Error: 'ff' is a reserved TraceContext key
}>();
```

## Type System

### Op Type (Core Abstraction)

The `Op<Ctx, Args, Result>` type is the core abstraction. Type parameters are ordered to match the function signature
`(ctx, ...args) => Promise<Result>`:

```typescript
class Op<Ctx, Args extends unknown[], Result> {
  // fn type params MUST match the class type params
  readonly fn: (ctx: Ctx, ...args: Args) => Promise<Result>;
  readonly module: Module;
  readonly definitionLine?: number;

  // Internal - called by span()
  _invoke(traceCtx: TraceContext, parentBuffer: SpanBuffer | null, spanName: string, args: Args): Promise<Result>;
}
```

**Why this order?** The type params `<Ctx, Args, Result>` match how you read the function signature: context first, then
arguments, then return type.

### OpContext Type

The full context type passed to op functions. It combines built-in properties with user-extensible `Extra`:

```typescript
type OpContext<Schema, Deps, FF, Extra> = {
  // Invoke child op - supports multiple overloads (see span() section)
  span: SpanFn<OpContext<Schema, Deps, FF, Extra>>;

  // Logging
  log: LogAPI;

  // Span attributes
  tag: TagAPI<Schema>;

  // Dependencies - can destructure!
  deps: BoundDeps<Deps>;

  // Feature flags
  ff: FeatureFlagEvaluator<FF>;
} & Extra; // User-extensible properties via .ctx<Extra>()
```

**Key Design**: `Extra` is spread into the context type, so `{ env: { region: string } }` becomes a direct property on
the context that you can destructure: `({ env }) => ...`.

### Module Type

```typescript
interface Module<Schema, Deps, FF, Extra> {
  readonly metadata: ModuleMetadata;
  readonly schema: Schema;
  readonly deps: Deps;
  readonly ff: FF;

  // Create root TraceContext - entry point for traces
  traceContext(params: { ff: FeatureFlagEvaluator<FF> } & Extra): TraceContext<FF, Extra>;

  // Op factory - creates typed Op instances
  op<Args extends unknown[], Result>(
    fn: (ctx: OpContext<Schema, Deps, FF, Extra>, ...args: Args) => Promise<Result>
  ): Op<OpContext<Schema, Deps, FF, Extra>, Args, Result>;

  // Prefix application for library composition
  prefix<P extends string>(prefix: P): PrefixedModule<Schema, Deps, FF, Extra, P>;

  // Wire dependencies
  use(wiredDeps: WiredDeps<Deps>): BoundModule<Schema, Deps, FF, Extra>;
}

// defineModule returns a builder, .ctx<Extra>() finalizes types
interface ModuleBuilder<Schema, Deps, FF> {
  // Add user-extensible context properties
  ctx<Extra extends Record<string, unknown>>(): Module<Schema, Deps, FF, Extra>;
}

interface ModuleMetadata {
  packageName: string; // npm package name (injected by transformer)
  packagePath: string; // Path within package (injected by transformer)
  gitSha?: string; // Git commit SHA (optional, injected by transformer)
}
```

## Op Metrics Tracking

The Op class tracks runtime metrics that are flushed alongside trace data:

```typescript
class Op<Ctx, Args extends unknown[], Result> {
  readonly fn: (ctx: Ctx, ...args: Args) => Promise<Result>;
  readonly module: Module;
  readonly definitionLine?: number;

  // Metrics (reset on flush)
  private invocationCount: number = 0;
  private errorCount: number = 0; // span-err outcomes
  private exceptionCount: number = 0; // span-exception outcomes

  private totalDurationNs: bigint = 0n;
  private okDurationNs: bigint = 0n; // span-ok only
  private errDurationNs: bigint = 0n; // span-err + span-exception
  private minDurationNs: bigint = BigInt(Number.MAX_SAFE_INTEGER);
  private maxDurationNs: bigint = 0n;

  private periodStartNs: bigint = hrtime.bigint();
}
```

### Why Op Tracks Metrics

1. **Zero extra timing overhead**: Reuses span timestamps already written to buffer
2. **Per-op granularity**: Each op independently tracks its performance
3. **Duration by outcome**: Separate tracking for ok/err reveals fast-fail vs slow-timeout patterns
4. **Structured output**: Metrics flushed as entry types (`op-invocations`, `op-duration-total`, etc.)

### Metrics Collection Flow

Inside the Op's `_invoke` method:

1. **Increment** `invocationCount`
2. **After span completes**, read timestamps from buffer (already written by span-start/span-ok)
3. **Compute duration**, update totals/min/max
4. **On error/exception**, increment appropriate counter and add to `errDurationNs`

```typescript
// Simplified flow inside _invoke
async _invoke(requestCtx, parentBuffer, spanName, args) {
  this.invocationCount++;
  const startIdx = buffer.count;  // Remember where span-start was written

  try {
    const result = await this.fn(opContext, ...args);
    // span-ok already written, read timestamps
    const duration = buffer.timestamp[buffer.count - 1] - buffer.timestamp[startIdx];
    this.okDurationNs += duration;
    this.updateMinMax(duration);
    return result;
  } catch (e) {
    // span-exception already written
    const duration = buffer.timestamp[buffer.count - 1] - buffer.timestamp[startIdx];
    this.errDurationNs += duration;
    this.exceptionCount++;
    this.updateMinMax(duration);
    throw e;
  }
}
```

See [Op and Buffer Metrics](./01n_op_and_buffer_metrics.md) for complete metrics documentation including flush behavior
and entry type formats.

### uint64 Values in Tag/Log API

For metrics and timestamps that need full 64-bit precision, use `uint64`:

```typescript
const myOp = op(async ({ log, tag }, data: Data) => {
  // Tag API for span attributes
  tag.uint64(someTimestampNs);

  // Log API for individual entries
  log.info('Processing').uint64(data.bigCounter);
});
```

### Collision Detection

```typescript
// Type-level collision detection
type DetectCollision<A, B> = Extract<keyof A, keyof B> extends never ? never : Extract<keyof A, keyof B>;

// Produces compile error on collision
type WiredDeps<Deps> = {
  [K in keyof Deps]: Deps[K] extends Module<infer S, infer D> ? PrefixedModule<S, D, string> : never;
};
```

## Context Flow

### Op Invocation

When an op is invoked via span():

```typescript
await span('fetch-users', GET, 'https://example.com');
```

1. **Create SpanBuffer** with module's prefixed schema
2. **Link to parent** buffer (if nested)
3. **Write span-start** with name from caller
4. **Create OpContext** = `{ span, log, tag, deps, ff }` + Extra properties
5. **Execute op function** with context + arguments
6. **Write span-ok** or **span-exception** on completion

### Root Invocation

At the application entry point, use `module.traceContext()`:

```typescript
// Create trace context via module - type-safe Extra properties
const ctx = appModule.traceContext({
  ff: ffEvaluator,
  env: workerEnv,
  requestId: req.headers.get('x-request-id')!,
  userId: session?.userId,
});

// Root span via trace context
await ctx.span('handle-request', handleRequest, req);

// Or via pre-wired module root
const appRoot = appModule.use({
  http: httpModule.prefix('http').use({ ... }),
  db: dbModule.prefix('db').use(),
});
await appRoot.span('handle-request', handleRequest, req);
```

## RemappedBufferView Integration

The RemappedBufferView (from spec 01e) maps prefixed columns for Arrow conversion:

1. **Op creates SpanBuffer** with unprefixed schema (`{ status, method }`)
2. **Op writes** to unprefixed columns (hot path, zero overhead)
3. **RemappedBufferView wraps buffer** for parent's tree traversal
4. **Arrow conversion** uses prefixed names to access unprefixed columns

The key insight: ops always write with clean names, prefixing happens at the view layer.

## Testing Support

### Mocking Dependencies

```typescript
// Create mock implementations
const mockRetry = retryModule.prefix('mock_retry').use();
const mockHttp = httpModule.prefix('mock_http').use({
  retry: mockRetry,
});

// Override deps for testing
const testRoot = httpModule.prefix('test_http').use({
  retry: mockRetry,
});

await testRoot.span('test-get', GET, 'https://example.com');
```

### Testing Ops in Isolation

```typescript
import { GET } from '@mycompany/http-tracing';

// Create minimal root context for testing
const testRoot = httpModule.prefix('test').use({
  retry: mockRetryModule.prefix('retry').use(),
});

const result = await testRoot.span('test', GET, 'https://test.example.com');
expect(result.ok).toBe(true);
```

## Op Pattern Examples

### Defining an Op

```typescript
const createUser = op(async ({ span, log, tag }, userData: UserData) => {
  tag.userId(userData.id);
  log.info('Creating user');
  await span('validate', validateUser, userData);
  return { success: true, user };
});
```

### Calling Dependencies via span()

```typescript
// Explicit span with caller-chosen name
const { retry } = deps;
await span('retry-attempt', retry, 1);
```

### Module Composition with Deps

```typescript
// Explicit deps with collision detection
const appModule = defineModule({
  schema: { userId: S.category() },
  deps: { http: httpModule, db: dbModule },
});

const appRoot = appModule.use({
  http: httpModule.prefix('http').use({ ... }),
  db: dbModule.prefix('db').use(),
});
```

## Benefits Summary

1. **Clean signatures** - Destructure `{ span, log, tag }` instead of `(ctx, ...)`
2. **Caller names spans** - Contextually meaningful span names
3. **Zero-allocation deps** - Plain object references, not closures
4. **V8 friendly** - Op is a simple class, deps is a plain object
5. **Type-safe composition** - Collision detection at compile time
6. **Shared instances** - Multiple consumers share dependency instances
7. **Easy testing** - Simple mocking via wired deps
8. **Clean library authoring** - Schema once, prefix at use time

## Integration with Other Specs

- **[Trace Schema System](./01a_trace_schema_system.md)**: Schema definition (`S.number()`, `S.category()`, etc.)
- **[Context Flow and Op Wrappers](./01c_context_flow_and_task_wrappers.md)**: How context flows through span() calls
- **[Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md)**: TagAPI/SpanLogger
  generation
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemappedBufferView for Arrow conversion
- **[Tree Walker](./01k_tree_walker_and_arrow_conversion.md)**: How Arrow conversion traverses the span tree
- **[Op and Buffer Metrics](./01n_op_and_buffer_metrics.md)**: Complete metrics tracking, flush behavior, and entry
  types
