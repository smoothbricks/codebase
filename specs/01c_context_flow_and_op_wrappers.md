# Context Flow and Op Wrappers

## Overview

The context flow system manages how trace context, feature flags, and user-extensible properties flow through the
application. It provides:

1. **Hierarchical context creation** from trace → op → child span
2. **Op wrapper pattern** that creates span-aware contexts
3. **Performance optimization** through single allocation and direct references
4. **Type-safe context destructuring** with automatic span correlation
5. **User-extensible context** via `ctx` property in `defineOpContext()` for custom properties like env bindings

## Context Hierarchy

```
OpMetadata (object) - ONE per Op, injected by transformer
├── package_name: string
├── package_file: string
├── git_sha: string
└── line: number (line where defineOp was called)

LogBinding (object) - ONE per defineOpContext() call
├── logSchema: LogSchema (tag attribute definitions)
├── remappedViewClass?: RemappedViewConstructor (for prefixed libraries)
├── sb_capacity: number (current buffer capacity for new buffers)
├── sb_totalWrites: number (total entries written across all buffers)
├── sb_overflowWrites: number (writes that triggered overflow)
├── sb_totalCreated: number (total buffers created)
└── sb_overflows: number (number of overflow events)

SpanBuffer - ONE per span (internal interface)
├── _callsiteMetadata?: OpMetadata  ← CALLER's metadata (where span() was invoked) - for row 0 metadata
├── _opMetadata: OpMetadata         ← THIS OP's metadata (what code is executing) - for rows 1+ metadata
├── _logBinding: LogBinding         ← LogBinding with schema and capacity stats
├── _spanName: string               ← span name for this invocation
├── lineNumber_values: Int32Array  ← line numbers per row (written directly, NOT stored as property)
├── _parent?: SpanBuffer            ← reference to parent (child spans walk this for trace_id)
├── _children: SpanBuffer[]         ← child spans
├── trace_id (getter)               ← root stores it, children walk parent chain
└── columns, writeIndex, etc.

SpanContext (interface) - user-facing, what ops receive
├── tag: TagWriter<T>
├── log: SpanLogger<T>
├── scope / setScope()
├── ok() / err()
├── span()
├── buffer (getter)
├── ff: FeatureFlagEvaluator
└── deps: Deps
```

**Dual Metadata References - Row 0 vs Rows 1+:**

- **Row 0 (span-start)**: Uses `callsiteMetadata` for `git_sha`, `package_name`, `package_file`
- **Rows 1+ (span-ok/err/exception, logs)**: Uses `metadata` for `git_sha`, `package_name`, `package_file`

This design enables accurate source attribution - the span-start entry records WHERE the span was invoked from, while
subsequent entries record WHERE the code is actually executing.

**lineNumber is NEVER a property on any object:**

Line numbers flow directly from transformer injection to TypedArray writes:

```typescript
// Transformer output:
await ctx.span(42, 'fetch-user', userLib.fetchUser, userId);
//             ^^ lineNumber argument

// Inside span():
buffer.lineNumber_values[0] = 42; // Direct TypedArray write for row 0

// For logs (rows 1+):
ctx.log.info('Processing').line(55); // .line(N) writes to lineNumber_values[writeIndex]
```

**Access chain:**

```typescript
buffer._callsiteMetadata.package_name; // Caller's metadata (for row 0)
buffer._callsiteMetadata.git_sha; // Caller's git SHA (for row 0)
buffer._opMetadata.package_name; // This op's metadata (for rows 1+)
buffer._opMetadata.git_sha; // This op's git SHA (for rows 1+)
buffer._logBinding.sb_capacity; // Self-tuning stats
buffer._logBinding.logSchema; // Schema definitions
buffer._spanName; // Span name (direct property)
buffer.lineNumber_values[0]; // Line number for row 0 (written directly, NO lineNumber property)
buffer.trace_id; // Walks parent chain to root if child span
```

## Design Rationale: From ctx Parameter to Destructured Context

### Problem with Traditional ctx Parameter

An alternative approach would pass a `ctx` object through every function:

```typescript
// Alternative approach (rejected): ctx drilling problem
const createUser = defineOp('createUser', async (ctx, userData) => {
  ctx.tag.userId(userData.id);
  await validateUser(ctx, userData); // Pass ctx everywhere
  await saveUser(ctx, userData); // Every function needs ctx
});
```

**Issues**:

- **Parameter drilling**: Every function signature needs `ctx` as first parameter
- **Repetitive boilerplate**: `ctx.tag.`, `ctx.log.` everywhere
- **Span naming at definition**: Name decided by callee, not caller
- **Closure overhead**: Binding deps requires creating closures per invocation

### Solution: Op Pattern with Destructuring

The chosen design uses **ops** with context destructuring at the call site:

```typescript
// Destructure what you need from ctx
const request = defineOp('request', async (ctx, url: string, opts: RequestOpts) => {
  const { tag, log, deps } = ctx;
  tag.method(opts.method);
  log.info('Making request');
  const res = await fetch(url, opts);
  tag.status(res.status);
  return res;
});

// Caller controls span name via span()
await ctx.span('fetch-user', request, '/users/123', { method: 'GET' });
```

**Benefits**:

1. **No ctx drilling**: Destructure only what you need `{ span, log, tag }`
2. **Span name at call site**: Caller decides contextually meaningful name
3. **Zero allocation for deps**: Plain object references, not bound closures
4. **V8 friendly**: Op is a plain class, deps is a plain object

## Design Philosophy

**Key Insight**: Each span needs its own buffer for proper trace correlation. The op wrapper handles buffer creation,
entry writing, and context setup automatically.

**TraceContext vs SpanContext**:

- `TraceContext` is created via `createTrace()` at request entry
- `SpanContext` is what op functions receive
- TraceContext has system props: `trace_id`, `thread_id`
- SpanContext has: `tag`, `log`, `scope`, `setScope`, `ok`, `err`, `span`, `buffer`, `ff`, `deps`
- Time anchors (`anchorEpochMicros`, `anchorPerfNow`) are **module-scoped** (see
  [High-Precision Timestamps](./01b3_high_precision_timestamps.md))

```
TraceContext (created via createTrace())
├── trace_id: string (system prop)
├── thread_id: bigint (system prop)
├── ff: FeatureFlagEvaluator (system prop)
├── span: RootSpanFn (system prop)
├── env: EnvironmentConfig (ctx - user-defined)
├── requestId: string (ctx - user-defined)
├── userId?: string (ctx - user-defined)
└── ctx.span('create-user', createUserOp, userData) creates:
    ├── SpanContext (create-user span buffer)
    ├── tag: TagAPI (writes to create-user buffer)
    ├── log: SpanLogger (logs to create-user buffer)
    ├── span: ChildSpanCreator (creates child spans)
    ├── deps: BoundDeps (references to dep Op instances)
    ├── ff: traceCtx.ff.withBuffer(buffer) → NEW evaluator bound to buffer
    ├── env: SAME from TraceContext (ctx props passed through)
    ├── requestId: SAME from TraceContext (ctx props passed through)
    ├── userId: SAME from TraceContext (ctx props passed through)
    └── span('validate-user', validateOp) creates:
        ├── Child SpanContext (validate-user span buffer)
        ├── tag: TagAPI (writes to validate-user buffer)
        ├── log: SpanLogger (logs to validate-user buffer)
        ├── span: ChildSpanCreator (for nested ops)
        ├── deps: SAME deps references
        ├── ff: parentCtx.ff.withBuffer(childBuffer)
        └── ctx props: SAME (env, requestId, userId)
```

**SpanContext Properties**:

- `span(name, op, ...args)`: Invoke an op as a child span
- `log`: SpanLogger for info/debug/warn/error
- `tag`: TagAPI for span attributes
- `deps`: Bound dependency ops (can be destructured: `const { retry } = deps`)
- `ff`: Feature flag evaluator bound to current span's buffer
- ctx props from TraceContext (e.g., `env`, `requestId`, `userId` - user-defined via `ctx` in `defineOpContext()`)

## Trace-Level Context Creation

**Purpose**: Create the initial context at request boundaries with user-specific feature flag evaluation and
high-precision time anchor.

### TraceContext Interface

```typescript
// Reserved keys that ctx cannot contain (compile-time enforcement)
type ReservedTraceContextKeys = keyof {
  ff: unknown;
  span: unknown;
  tag: unknown;
  log: unknown;
  scope: unknown;
  setScope: unknown;
  ok: unknown;
  err: unknown;
  buffer: unknown;
  deps: unknown;
  anchorEpochMicros: unknown;
  anchorPerfNow: unknown;
};

// TraceContext = System props + Ctx
interface TraceContext<FF, Ctx> {
  // System properties (always present)
  // NOTE: Time anchors are MODULE-SCOPED (not per-trace).
  // See timestamp.ts and timestamp.node.ts for implementation details.
  // Browser: performance.timeOrigin (set once at page load)
  // Node.js: anchorHrtime + anchorEpochNanos (captured once at module load)
  anchorEpochMicros: number; // Date.now() * 1000 at trace root
  anchorPerfNow: number; // performance.now() at trace root (browser)
  // OR anchorHrTime: bigint; // process.hrtime.bigint() at trace root (Node.js)

  ff: FeatureFlagEvaluator<FF>;
  span: RootSpanFn<FF, Ctx>; // Root span creation - entry point for ops
} & Ctx; // User-defined properties (e.g., requestId, userId, env)
```

**IMPORTANT**: `trace_id` and `thread_id` are NOT on TraceContext - they are SpanBuffer properties:

- `buffer.trace_id` - Generated at root span creation, stored in `_identity` bytes, child spans walk parent chain
- `buffer.thread_id` - Worker-level 64-bit random ID (generated once at startup), stored in `_identity` bytes

**Key Design**: `requestId` and `userId` are NOT system properties - they are user-defined in `ctx` via
`defineOpContext()`. The `Ctx` type comes from the `ctx` property and is spread into SpanContext when ops execute.

### Context Creation via createTrace()

**IMPORTANT**: `createTrace()` actually returns a `SpanContext` (root span), not a separate TraceContext object. The
implementation creates a SpanBuffer immediately.

```typescript
// Internal implementation (simplified):
function createTraceImpl<T, FF, Deps, UserCtx>(
  factoryConfig: OpContextConfig<T, FF, Deps, UserCtx>,
  logBinding: LogBinding,
  params: CreateTraceParams<UserCtx>
): SpanContext<T, FF, Deps, UserCtx> {
  // 1. Generate trace ID
  const traceId: TraceId = params.traceId ?? generateTraceId();

  // 2. Create root SpanBuffer with trace_id
  const buffer = createSpanBuffer(schema, logBinding, 'root', traceId);

  // 3. Build SpanContext with user properties + system properties
  const ctx = {
    ...resolvedUserCtx, // User properties (env, requestId, etc.)
    ff: /* feature flag evaluator */,
    tag: /* tag writer */,
    log: /* span logger */,
    span: /* span function */,
    // ... other system properties
  };

  return ctx as SpanContext<T, FF, Deps, UserCtx>;
}

// Usage at request entry point:
const ctx = createTrace({
  env: workerEnv,
  requestId: req.headers.get('x-request-id')!,
  userId: session?.userId,
});

// ctx is already a SpanContext - can use span() directly
await ctx.span('handle-request', handleRequestOp, req);
```

**Note**: `trace_id` and `thread_id` are stored in `ctx.buffer._identity`, not as top-level context properties.

## Op Definition and the defineOp() Factory

### What is an Op?

An **Op** is a traced operation. It's created via `defineOp()` from an op context:

```typescript
const { defineOp, defineOps, createTrace } = defineOpContext({
  logSchema: defineLogSchema({
    status: S.number(),
    method: S.enum(['GET', 'POST']),
    url: S.category(),
  }),
  deps: { retry: retryOps },
  flags: defineFeatureFlags({ premiumApi: S.boolean() }),
  ctx: {
    env: null as { apiTimeout: number }, // Required at createTrace
  },
});

// Define ops - context is first parameter
const request = defineOp('request', async (ctx, url: string, opts: RequestOpts) => {
  ctx.tag.method(opts.method);
  ctx.tag.url(url);
  ctx.log.info('Making HTTP request');

  const res = await fetch(url, { ...opts, timeout: ctx.env.apiTimeout });
  ctx.tag.status(res.status);
  return res;
});
```

### Op Internal Structure

The `Op<Ctx, Args, Result>` type parameters match the function signature order:

```typescript
class Op<Ctx, Args extends unknown[], Result> {
  constructor(
    readonly name: string, // For Op metrics (invocations, errors, duration)
    readonly metadata: OpMetadata, // For git_sha/package_name/package_file attribution (THIS op)
    readonly logBinding: LogBinding, // LogBinding with schema and capacity stats
    private fn: (ctx: Ctx, ...args: Args) => Promise<Result>
  ) {}

  /**
   * Internal invocation - called by span()
   * @param traceCtx - The root trace context (actually a SpanContext for root)
   * @param parentBuffer - Parent span's buffer (null for root)
   * @param callsiteMetadata - The CALLER's metadata (where span() was invoked) - for row 0 attribution
   * @param spanName - Name decided by caller
   * @param lineNumber - Line number where span() was called (injected by transformer)
   * @param args - User arguments to the op
   */
  async _invoke(
    traceCtx: TraceContext<FeatureFlagSchema, Record<string, unknown>>,
    parentBuffer: SpanBuffer | null,
    callsiteMetadata: OpMetadata,
    spanName: string,
    lineNumber: number,
    args: Args
  ): Promise<Result> {
    // 1. Create SpanBuffer with dual metadata:
    //    - callsiteMetadata: CALLER's metadata (for row 0's git_sha/package_name/package_file)
    //    - this.metadata: THIS OP's metadata (for rows 1+ git_sha/package_name/package_file)
    // - Root: auto-generates trace_id
    // - Child: walks parent chain for trace_id (no duplication)
    const buffer = parentBuffer
      ? createChildSpanBuffer(parentBuffer, this.logBinding, spanName, callsiteMetadata)
      : createSpanBuffer(schema, this.logBinding, spanName); // Auto-generates trace_id

    // 2. Register with parent's _children (RemappedBufferView if prefixed)
    if (parentBuffer) {
      if (this.logBinding.remappedViewClass) {
        // Op has prefix - wrap buffer in RemappedBufferView for parent's tree traversal
        const view = new this.logBinding.remappedViewClass(buffer);
        parentBuffer._children.push(view);
      } else {
        // No prefix - push raw buffer directly
        parentBuffer._children.push(buffer);
      }
    }

    // 3. Write span-start entry (row 0)
    //    - Uses callsiteMetadata for git_sha/package_name/package_file
    //    - lineNumber written DIRECTLY to lineNumber_values[0] (NO intermediate object)
    buffer.lineNumber_values[0] = lineNumber; // Direct TypedArray write
    buffer.writeSpanStart(); // Writes timestamp, operation, uses callsiteMetadata for metadata

    // 4. Set up SpanContext using prototype-based inheritance from parentSpanContext
    // CRITICAL DESIGN: Object.create(parentSpanContext) provides:
    // - User properties (env, requestId, userId, deps) inherited via prototype chain
    // - Context overrides work automatically (override props become own properties on child)
    // - V8 inline caches make prototype access fast after warmup (monomorphic)
    // - Zero per-call allocation for user properties - just prototype link
    // - No _extraKeys iteration needed - prototype chain handles inheritance
    //
    // span() captures THIS OP's metadata (this.metadata) as callsiteMetadata for child spans
    const spanCtx = Object.create(parentSpanContext);

    // Set instance properties (own properties shadow prototype values)
    spanCtx.tag = createTagWriter(schema, buffer);
    spanCtx.log = createSpanLogger(schema, buffer);
    spanCtx._buffer = buffer;
    spanCtx._spanLogger = spanCtx.log;
    spanCtx.ff = parentSpanContext.ff.forContext(spanCtx);

    // 5. Execute user function with try/catch for span-exception
    //    Rows 1+ use this.metadata for git_sha/package_name/package_file
    try {
      const result = await this.fn(spanCtx, ...args);
      buffer.writeSpanOk(); // Row 1 - uses metadata
      return result;
    } catch (error) {
      buffer.writeSpanException(error); // Row 1 - uses metadata
      throw error;
    }
  }
}
```

### Why This Design

1. **Span name at call site**: `span('retry-attempt', retry, 1)` - caller provides contextually meaningful name
2. **Dual metadata references**: `callsiteMetadata` for row 0's git_sha/package_name/package_file, `metadata` for rows
   1+
3. **Zero allocation deps**: `deps` is a plain object, not closures created per call
4. **V8 hidden class friendly**: Op is a simple class with fixed structure
5. **Single Ctx type param**: Op carries full Ctx requirement, contravariance at span() ensures compatibility
6. **Direct lineNumber writes**: lineNumber passed as argument to span(), written directly to `lineNumber_values[0]` (NO
   lineNumber property on any context object)
7. **Prototype-based user property inheritance**: `Object.create(parentSpanContext)` instead of copying properties
   - V8 inline caches make prototype access fast after warmup
   - Context overrides work naturally (override becomes own property on child, shadows prototype)
   - Memory efficient - no copying of user properties at each span creation
   - No `_extraKeys` tracking needed - prototype chain handles inheritance automatically

## SpanContext Interface

The context passed to op functions combines built-in properties with user-extensible `Ctx`:

```typescript
type SpanContext<Schema, Deps, FF, Ctx> = {
  // Invoke another op as a child span - supports multiple overloads
  span: SpanFn<SpanContext<Schema, Deps, FF, Ctx>>;

  // Log messages (info/debug/warn/error)
  log: LogAPI;

  // Span attributes (chainable)
  tag: TagAPI<Schema>;

  // Dependencies - can be destructured!
  deps: BoundDeps<Deps>;

  // Feature flags (logs access to current span)
  ff: FeatureFlagEvaluator<FF>;

  // Result helpers
  ok<T>(value: T): OkResult<T>;
  err<E>(code: string, data?: E): ErrResult<E>;

  // Scoped attributes (read-only view)
  scope: Readonly<Partial<Schema>>;

  // Set scoped attributes (merge semantics, null to clear)
  setScope(values: Partial<Schema>): void;

  // Access to underlying buffer (advanced use)
  buffer: SpanBuffer;
} & Ctx; // User-extensible properties via ctx in defineOpContext()
```

**Key Design**: The `Ctx` type is spread directly into the context, so properties like `env` become top-level and can be
accessed directly: `ctx.env.apiTimeout`.

### Destructuring Pattern

Ops can destructure the context, taking only what they need:

```typescript
// Full destructuring
const processUser = defineOp('processUser', async (ctx, user: User) => {
  const { span, log, tag, deps, ff, env } = ctx;
  // ...
});

// Direct access - simpler for most cases
const validateEmail = defineOp('validateEmail', async (ctx, email: string) => {
  ctx.log.debug('Validating email');
  ctx.tag.email(email);
  // ...
});

// With deps destructuring
const fetchWithRetry = defineOp('fetchWithRetry', async (ctx, url: string) => {
  const { retry, auth } = ctx.deps; // Destructure deps too!

  try {
    return await ctx.span('fetch', request, url, { method: 'GET' });
  } catch (e) {
    await ctx.span('retry', retry.attempt, 1);
    throw e;
  }
});
```

## Child Span Creation via span()

The `span()` method is how ops call other ops:

```typescript
const createUser = defineOp('createUser', async (ctx, userData: UserData) => {
  ctx.tag.userId(userData.id);

  // Validate via child span
  const valid = await ctx.span('validate', validateUser, userData);
  if (!valid.success) {
    return ctx.err('VALIDATION_FAILED');
  }

  // Save via child span
  const saved = await ctx.span('save', saveUser, userData);

  ctx.log.info('User created');
  return ctx.ok(saved);
});
```

### How span() Works

1. **lineNumber as first argument**: Injected by transformer as first argument to span()
2. **Name provided by caller**: The span name - caller decides contextually
3. **Op provided by caller**: The Op instance to invoke
4. **Args passed through**: Remaining arguments go to the op function
5. **lineNumber written directly**: Written to `lineNumber_values[0]` inside \_invoke() (NO intermediate object storage)
6. **SpanBuffer created with callsiteMetadata**: `callsiteMetadata` for row 0's metadata, `metadata` for rows 1+
7. **Buffer linking**: Child buffer registered with parent's children array
8. **RemappedBufferView**: If op has prefix, view maps prefixed columns for Arrow

### Example: Multiple Child Spans

```typescript
const processOrder = defineOp('processOrder', async (ctx, order: Order) => {
  ctx.tag.orderId(order.id);

  // Multiple child spans with contextual names
  const validated = await ctx.span('validate-order', ctx.deps.validation.check, order);
  const inventory = await ctx.span('check-inventory', ctx.deps.inventory.check, order.items);
  const payment = await ctx.span('process-payment', ctx.deps.payments.charge, order.total);

  // Even same op can have different names based on context
  await ctx.span('notify-customer', ctx.deps.notify.send, order.customerId, 'order_confirmed');
  await ctx.span('notify-warehouse', ctx.deps.notify.send, order.warehouseId, 'prepare_shipment');

  return ctx.ok({ validated, inventory, payment });
});
```

## Root Invocation

At the application entry point, use `createTrace()` then `.span()`:

```typescript
// Wire dependencies at composition time via defineOpContext
const { defineOp, createTrace } = defineOpContext({
  logSchema: appSchema,
  deps: {
    http: httpOps.prefix('http'),
    retry: retryOps.prefix('http_retry'),
  },
  ctx: {
    env: null as Env, // Required
    requestId: null as string, // Required
    userId: undefined as string | undefined, // Optional
  },
});

// Request handler
app.post('/users', async (req, res) => {
  // Create trace context - type-safe ctx properties
  const ctx = createTrace({
    env: workerEnv,
    requestId: req.id,
    userId: req.user?.id,
  });

  // Root invocation via span()
  const result = await ctx.span('create-user', createUser, req.body);

  if (result.success) {
    res.json(result.value);
  } else {
    res.status(400).json({ error: result.error });
  }
});
```

## Thread ID and Distributed Span Identification

The `thread_id` in TraceContext enables collision-resistant span identification:

**Problem**: Simple incrementing span IDs (`span_id++`) collide across workers.

**Solution**: Separate columns for `thread_id` + `span_id`:

- `thread_id`: 64-bit random BigInt, generated once per worker at startup
- `span_id`: 32-bit incrementing counter, assigned per span creation

```
Worker A: parentBuffer = { thread_id: 0xAAA..., span_id: 1 }
    │
    └─► pmap() dispatches to Thread B
        │
        ▼
Worker B: childBuffer = { thread_id: 0xBBB..., span_id: 1, parent: parentBuffer }
          Arrow output: parent_thread_id=0xAAA..., parent_span_id=1
```

## High-Precision Timestamp Design

The context captures a single time anchor at creation:

```typescript
function getTimestamp(ctx: TraceContext): bigint {
  // Returns nanoseconds since epoch with sub-millisecond precision
  return ctx.anchorEpochNanos + BigInt(Math.round((performance.now() - ctx.anchorPerfNow) * 1_000_000));
}
```

**Benefits**:

- No `Date.now()` calls per span - only high-resolution timer deltas
- Sub-millisecond precision for performance analysis
- All spans in trace share same anchor (comparable timestamps)

## Feature Flag Integration

Feature flags are accessed via `ff` in the op context:

```typescript
const createUser = defineOp('createUser', async (ctx, userData: UserData) => {
  // Access flags via ctx.ff
  if (await ctx.ff.advancedValidation.get()) {
    await ctx.span('advanced-validate', advancedValidate, userData);
    ctx.ff.advancedValidation.track(); // Log ff-usage
  }

  if (await ctx.ff.betaFeatures.get()) {
    ctx.log.info('Beta features enabled');
    ctx.ff.betaFeatures.track({ feature: 'new_ui' });
  }

  return ctx.ok({ success: true });
});
```

## Usage Examples

### Basic Op Usage

```typescript
const { defineOp, createTrace } = defineOpContext({
  logSchema: defineLogSchema({
    userId: S.category(),
    operation: S.enum(['INSERT', 'UPDATE', 'DELETE']),
  }),
});

const createUser = defineOp('createUser', async (ctx, userData: UserData) => {
  // Tag span attributes
  ctx.tag.userId(userData.id).operation('INSERT');

  // Log messages
  ctx.log.info('Creating new user');

  // Call child ops via span()
  const validated = await ctx.span('validate', validateUser, userData);
  if (!validated.success) {
    ctx.log.warn('Validation failed');
    return ctx.err('VALIDATION_FAILED', validated.error);
  }

  const user = await ctx.span('save', saveUser, userData);
  ctx.log.info('User created successfully');

  return ctx.ok(user);
});
```

### Using Dependencies

```typescript
const { defineOp } = defineOpContext({
  logSchema: httpSchema,
  deps: {
    retry: retryOps,
    auth: authOps,
  },
});

const GET = defineOp('GET', async (ctx, url: string) => {
  // Access deps via ctx.deps
  const { retry, auth } = ctx.deps;

  ctx.tag.method('GET').url(url);
  ctx.log.info('Starting GET request');

  // Get auth token via child span
  const token = await ctx.span('get-token', auth.getToken);

  const headers = { Authorization: `Bearer ${token}` };

  try {
    const res = await fetch(url, { headers });
    ctx.tag.status(res.status);
    return ctx.ok(res);
  } catch (e) {
    ctx.log.error('Request failed, retrying');
    await ctx.span('retry', retry.attempt, 1);
    throw e;
  }
});
```

### Feature Flags and Environment

```typescript
const { defineOp, createTrace } = defineOpContext({
  logSchema: orderSchema,
  deps: {
    premiumValidation: premiumValidationOps,
    newPayment: newPaymentOps,
    legacyPayment: legacyPaymentOps,
  },
  flags: defineFeatureFlags({
    premiumProcessing: S.boolean(),
    newPaymentFlow: S.boolean(),
  }),
  ctx: {
    env: null as { paymentProvider: string },
  },
});

const processOrder = defineOp('processOrder', async (ctx, order: Order) => {
  ctx.tag.orderId(order.id).total(order.total);

  if (await ctx.ff.premiumProcessing.get()) {
    await ctx.span('premium-validate', ctx.deps.premiumValidation.validate, order);
    ctx.ff.premiumProcessing.track();
  }

  const paymentProvider = ctx.env.paymentProvider;

  if (await ctx.ff.newPaymentFlow.get()) {
    await ctx.span('new-payment', ctx.deps.newPayment.charge, order, paymentProvider);
    ctx.ff.newPaymentFlow.track({ provider: paymentProvider });
  } else {
    await ctx.span('legacy-payment', ctx.deps.legacyPayment.charge, order);
  }

  return ctx.ok({ success: true });
});
```

## Performance Characteristics

### Op Creation

- **OpContext setup**: One allocation for metadata at defineOpContext() call
- **Schema compilation**: Done once at op context definition time
- **Deps binding**: Plain object reference, zero per-call allocation

### Op Invocation

- **Buffer creation**: One SpanBuffer per span with `callsiteMetadata` reference
- **lineNumber**: Passed as argument to span(), written directly to `lineNumber_values[0]` (NO lineNumber property on
  any object)
- **SpanContext creation**: One SpanContext object per invocation
- **No closure allocation**: deps is shared object reference

### Memory Usage

- **Shared references**: OpMetadata and deps shared across all ops
- **Direct buffer access**: TagAPI/SpanLogger hold buffer reference directly
- **Buffer management**: Self-tuning capacity per op context

## Integration Points

This context flow system integrates with:

- **[Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md)**: Provides the
  SpanLogger/TagAPI class generation
- **[Op Context Pattern](./01l_op_context_pattern.md)**: Defines `defineOpContext()`, `defineOp()`, and `createTrace()`
  API
- **[Span Scope Attributes](./01i_span_scope_attributes.md)**: Provides span-level attribute scoping
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Shows how libraries define ops
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Defines the SpanBuffer structure

## Line Number System

**CRITICAL: lineNumber is NEVER a property on any object.** It flows directly from transformer injection to TypedArray
writes:

- **Injection**: TypeScript transformer injects line numbers at compile time
- **Storage**: Written directly to `lineNumber_values` TypedArray at the appropriate row index
- **No intermediate object**: lineNumber is NOT stored on any context object - just passed as argument and written to
  TypedArray
- **Overhead**: Zero runtime overhead - literal passed to span(), written directly to TypedArray

```typescript
// Source code:
ctx.log.info('Processing user');
await ctx.span('validate', validateUser, userData);

// Transformed (line number injected):
ctx.log.info('Processing user').line(42); // .line(N) appended to log calls
await ctx.span(43, 'validate', validateUser, userData); // line number FIRST for span()
```

**Row-based lineNumber storage:**

- **Row 0 (span-start)**: lineNumber argument written directly to `lineNumber_values[0]` inside `_invoke()`
- **Rows 1+ (logs, span-ok/err)**: lineNumber written to `lineNumber_values[writeIndex]` via `.line(N)` calls

**SpanBuffer stores:**

- `callsiteMetadata`: The caller's OpMetadata (for row 0's git_sha/package_name/package_file)
- `metadata`: The Op's OpMetadata (for rows 1+ git_sha/package_name/package_file)
- `spanName`: The contextual name provided by caller
- `lineNumber_values`: Int32Array for line numbers per row (NOT a lineNumber property)
