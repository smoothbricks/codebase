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
└── remappedViewClass?: RemappedViewConstructor (for prefixed libraries)

Tracer (abstract class) - ONE per application/test
├── binding: OpContextBinding (from defineOpContext)
├── trace(): creates root span, executes fn, returns result
└── lifecycle hooks: onTraceStart, onTraceEnd, onSpanStart, onSpanEnd, onStatsWillResetFor

SpanBuffer - ONE per span (internal)
├── _traceRoot: TraceRoot         ← trace_id, anchors, tracer reference
├── _callsiteMetadata?: OpMetadata ← CALLER's metadata (for row 0)
├── _opMetadata: OpMetadata        ← THIS OP's metadata (for rows 1+)
├── _spanName: string              ← span name for this invocation
├── _parent?: SpanBuffer           ← reference to parent
├── _children: SpanBuffer[]        ← child spans
├── _scopeValues: Record<string, unknown> ← scope values for this span
├── timestamp: BigInt64Array       ← nanosecond timestamps
├── entry_type: Uint8Array         ← entry type enum
├── line_values: Float64Array      ← line numbers per row (lazy column)
└── [schema columns]               ← user-defined columns

SpanContext (interface) - user-facing, what ops receive
├── buffer: SpanBuffer (underlying storage)
├── tag: TagWriter<T> (writes to row 0)
├── log: SpanLogger<T> (writes to rows 2+)
├── ok() / err() → FluentOk / FluentErr (writes to row 1)
├── span() (creates child span)
├── scope / setScope()
├── ff: FeatureFlagEvaluator
├── deps: Deps
└── [user context properties from ctx]
```

**Dual Metadata References - Row 0 vs Rows 1+:**

- **Row 0 (span-start)**: Uses `_callsiteMetadata` for `git_sha`, `package_name`, `package_file`
- **Rows 1+ (span-ok/err/exception, logs)**: Uses `_opMetadata` for `git_sha`, `package_name`, `package_file`

This design enables accurate source attribution - the span-start entry records WHERE the span was invoked from, while
subsequent entries record WHERE the code is actually executing.

**Line numbers stored in lazy column:**

Line numbers are stored in a Float64Array lazy column (like other user attributes):

```typescript
// Transformer output:
await ctx.span(42, 'fetch-user', userLib.fetchUser, userId);
//             ^^ lineNumber argument

// Inside span():
buffer.line(0, 42); // Lazy column write for row 0

// For logs (rows 1+):
ctx.log.info('Processing').line(55); // .line(N) writes to line_values[writeIndex]
```

**Access chain:**

```typescript
buffer._callsiteMetadata.package_name; // Caller's metadata (for row 0)
buffer._callsiteMetadata.git_sha; // Caller's git SHA (for row 0)
buffer._opMetadata.package_name; // This op's metadata (for rows 1+)
buffer._opMetadata.git_sha; // This op's git SHA (for rows 1+)
buffer._traceRoot.trace_id; // Trace ID (root stores it)
buffer._traceRoot.tracer; // Tracer reference for lifecycle hooks
buffer._spanName; // Span name (direct property)
buffer.line_values[0]; // Line number for row 0 (lazy column)
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

**Tracer vs SpanContext**:

- `Tracer` is an abstract class that manages trace execution via `trace()` method
- `SpanContext` is what op functions receive (what `trace()` creates for root, what `span()` creates for children)
- Tracer calls lifecycle hooks: `onTraceStart`, `onTraceEnd`, `onSpanStart`, `onSpanEnd`, `onStatsWillResetFor`
- SpanContext has: `tag`, `log`, `scope`, `setScope`, `ok`, `err`, `span`, `buffer`, `ff`, `deps`, plus user properties
- Time anchors (`anchorEpochNanos`, `anchorPerfNow`) are stored in `TraceRoot` (see
  [High-Precision Timestamps](./01b3_high_precision_timestamps.md))

```
Tracer (abstract class) - ONE per application entry point
├── trace() creates root SpanContext with SpanBuffer
├── Calls onTraceStart(rootBuffer) before execution
├── Calls onTraceEnd(rootBuffer) after execution (in finally)
└── Child spans created via ctx.span():
    ├── Calls onSpanStart(childBuffer) before child execution
    └── Calls onSpanEnd(childBuffer) after child execution

trace('create-user', { env, requestId }, createUserOp, userData) creates:
├── Root SpanContext (create-user span buffer)
├── tag: TagAPI (writes to create-user buffer)
├── log: SpanLogger (logs to create-user buffer)
├── span: ChildSpanCreator (creates child spans)
├── deps: BoundDeps (references to dep Op instances)
├── ff: FlagEvaluator bound to buffer
├── env: from overrides (user-defined via ctx in defineOpContext())
├── requestId: from overrides (user-defined)
└── ctx.span('validate-user', validateOp) creates:
    ├── Child SpanContext (validate-user span buffer)
    ├── tag: TagAPI (writes to validate-user buffer)
    ├── log: SpanLogger (logs to validate-user buffer)
    ├── span: ChildSpanCreator (for nested ops)
    ├── deps: SAME deps references
    ├── ff: parentCtx.ff.forContext(childCtx)
    └── ctx props: SAME (env, requestId, etc.)
```

**SpanContext Properties**:

- `span(name, op, ...args)`: Invoke an op as a child span
- `log`: SpanLogger for info/debug/warn/error
- `tag`: TagAPI for span attributes
- `deps`: Bound dependency ops (can be destructured: `const { retry } = deps`)
- `ff`: Feature flag evaluator bound to current span's buffer
- User context properties (e.g., `env`, `requestId`, `userId` - defined via `ctx` in `defineOpContext()`)

## Trace-Level Context Creation

**Purpose**: Create the initial context at request boundaries with user-specific feature flag evaluation and
high-precision time anchor.

### Tracer Class

The `Tracer` abstract class manages trace execution. Concrete implementations handle what happens with completed traces:

```typescript
abstract class Tracer<T extends LogSchema> {
  constructor(binding: OpContextBinding<T, any, any, any>, options?: TracerOptions);

  // Bound for destructuring: const { trace } = new TestTracer(...)
  trace: TraceFn;

  // Lifecycle hooks (subclasses implement)
  protected abstract onTraceStart(rootBuffer: SpanBuffer<T>): void;
  protected abstract onTraceEnd(rootBuffer: SpanBuffer<T>): void;
  protected abstract onSpanStart(childBuffer: SpanBuffer<T>): void;
  protected abstract onSpanEnd(childBuffer: SpanBuffer<T>): void;
  protected abstract onStatsWillResetFor(buffer: SpanBuffer<T>): void;
}
```

**Concrete implementations** (from `packages/lmao/src/lib/tracer/`):

- `TestTracer` - Accumulates root buffers in `rootBuffers` array for inspection
- `NoOpTracer` - Discards all buffers (no-op for tests that don't need output)
- `StdioTracer` - Prints colored trace output to console
- `ArrayQueueTracer` - Batches buffers, drained via `drain()` method

**IMPORTANT**: `trace_id` and `thread_id` are NOT on Tracer - they are SpanBuffer properties:

- `buffer.trace_id` - Generated at root span creation (or provided via `trace_id` override), stored in `TraceRoot`,
  child spans walk parent chain
- `buffer.thread_id` - Worker-level 64-bit random ID (generated once at startup), stored in `_identity` bytes

**Key Design**: User context properties (e.g., `env`, `requestId`, `userId`) are defined via `ctx` in
`defineOpContext()` and passed as overrides to `trace()`.

### Trace Creation via Tracer

Traces are created by calling `trace()` on a Tracer instance:

```typescript
const opContext = defineOpContext({
  logSchema: mySchema,
  ctx: { env: null as Env, requestId: null as string },
});

const { trace } = new TestTracer(opContext);

// trace() creates root span, executes function, returns result
const result = await trace('handle-request', { env, requestId }, async (ctx) => {
  ctx.tag.userId(user.id);
  await ctx.span('validate', validateOp, user);
  return ctx.ok({ success: true });
});
```

The Tracer:

1. Creates root SpanBuffer with trace_id (auto-generated or from `trace_id` override)
2. Creates SpanContext with user properties from overrides (merged with ctxDefaults)
3. Calls `onTraceStart(rootBuffer)` before execution
4. Executes the function
5. Writes span-ok/span-err/span-exception to row 1
6. Calls `onTraceEnd(rootBuffer)` after execution (in finally)

**Trace Overrides**: The second parameter to `trace()` can include:

- User context properties (partial overrides for `ctx` config)
- `trace_id` for distributed tracing (optional `TraceId`)

```typescript
// With trace_id for distributed tracing:
await trace('handle-request', { trace_id: incomingTraceId, env, requestId }, handleOp);

// Without trace_id (auto-generated):
await trace('handle-request', { env, requestId }, handleOp);
```

## Op Definition and the defineOp() Factory

### What is an Op?

An **Op** is a traced operation. It's created via `defineOp()` from an op context:

```typescript
const flags = defineFeatureFlags({
  premiumApi: S.boolean().default(false).sync(),
});

const opContext = defineOpContext({
  logSchema: defineLogSchema({
    status: S.number(),
    method: S.enum(['GET', 'POST']),
    url: S.category(),
  }),
  deps: { retry: retryOps },
  flags: flags.schema,
  ctx: {
    env: null as { apiTimeout: number }, // Required at trace() time
  },
});

// Define ops - context is first parameter
const request = opContext.defineOp('request', async (ctx, url: string, opts: RequestOpts) => {
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
   * @param parentSpanContext - The parent SpanContext (root context for first-level spans)
   * @param parentBuffer - Parent span's buffer (null for root)
   * @param _callsiteMetadata - The CALLER's metadata (where span() was invoked) - for row 0 attribution
   * @param spanName - Name decided by caller
   * @param lineNumber - Line number where span() was called (injected by transformer)
   * @param args - User arguments to the op
   */
  /**
   * NOTE: This is pseudo-code for illustration. The actual implementation is in:
   * - Root spans: Tracer._createRootContext() and _executeWithContext() (tracer.ts)
   * - Child spans: SpanContext._spanPre() and span0-span8 methods (spanContext.ts)
   *
   * The flow below shows the CONCEPTUAL structure. Key differences in real code:
   * - Root spans create TraceRoot BEFORE createSpanBuffer (in Tracer)
   * - Child spans use createChildSpanBuffer with SpanBufferClass from Op
   * - FluentOk/FluentErr detection happens in execution wrappers
   * - Context setup happens via SpanContext class factory pattern
   */
  async _invoke(parentCtx: SpanContext | null, spanName: string, lineNumber: number, args: Args): Promise<Result> {
    // 1. Create SpanBuffer - ROOT vs CHILD differ in TraceRoot creation:
    let buffer: SpanBuffer;

    if (!parentCtx) {
      // ROOT SPAN (in Tracer.trace):
      // TraceRoot created FIRST with trace_id, anchors, tracer reference
      const traceRoot: TraceRoot = {
        trace_id: generateTraceId(), // or from overrides for distributed tracing
        anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
        anchorPerfNow: process.hrtime?.bigint() ?? performance.now(),
        tracer: this, // Reference to Tracer instance
      };

      // createSpanBuffer takes pre-built TraceRoot
      buffer = createSpanBuffer(
        schema,
        spanName,
        traceRoot, // Pre-built TraceRoot (NOT generated inside)
        opMetadata, // OpMetadata for both callsite and op at root
        capacity
      );
    } else {
      // CHILD SPAN (in SpanContext.span):
      // Extract TraceRoot from parent buffer (already exists)
      // Use Op's SpanBufferClass for correct schema and shared stats
      buffer = createChildSpanBuffer(
        parentCtx._buffer, // Parent buffer provides TraceRoot via ._traceRoot
        this.SpanBufferClass, // Op's buffer class (may have different schema)
        spanName,
        parentCtx._buffer._opMetadata, // Callsite metadata (WHO called span - for row 0)
        this.metadata // Op metadata (WHICH op executes - for rows 1+)
      );

      // Register with parent (wrap in RemappedBufferView if Op has prefix)
      const view = this.remappedViewClass ? new this.remappedViewClass(buffer) : buffer;
      parentCtx._buffer._children.push(view);
    }

    // 2. Write span-start entry (row 0) and pre-initialize row 1 as span-exception
    writeSpanStart(buffer, spanName);
    buffer.line(0, lineNumber); // Write line number to row 0

    // 3. Create SpanContext instance
    // In real code: SpanContextClass is created via createSpanContextClass(schema, logBinding)
    // and instantiated with direct arguments (no temp object allocation)
    const tagWriter = createTagWriter(schema, buffer);
    const spanLogger = createSpanLogger(schema, buffer);
    const spanCtx = new SpanContextClass(buffer, schema, spanLogger, tagWriter);

    // Set up deps, ff, and user context properties
    spanCtx.deps = this.deps;
    spanCtx.ff = this.flagEvaluator?.forContext(spanCtx) ?? {};
    Object.assign(spanCtx, resolvedUserCtx); // Spread user context (env, etc.)

    // 4. Execute function with FluentOk/FluentErr detection
    // Tracer calls onSpanStart/onSpanEnd hooks for lifecycle events
    try {
      const result = await this.fn(spanCtx, ...args);

      // If result is FluentOk/FluentErr (from ctx.ok()/ctx.err()),
      // entry type was already written by constructor. Skip overwriting.
      if (!(result instanceof FluentOk || result instanceof FluentErr)) {
        // Fallback: write span-ok to row 1 for non-FluentResult returns
        buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
        buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);
      }

      return result;
    } catch (error) {
      // Write span-exception to row 1
      buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);

      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;

      buffer.message(1, errorMessage);
      if (errorStack) {
        buffer.exception_stack(1, errorStack);
      }

      throw error;
    }
  }
}
```

### Why This Design

1. **Span name at call site**: `span('retry-attempt', retry, 1)` - caller provides contextually meaningful name
2. **Dual metadata references**: `_callsiteMetadata` for row 0's git_sha/package_name/package_file, `metadata` for rows
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
6. **SpanBuffer created with \_callsiteMetadata**: `_callsiteMetadata` for row 0's metadata, `metadata` for rows 1+
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

At the application entry point, create a Tracer and use its `trace()` method:

```typescript
// Wire dependencies at composition time via defineOpContext
const opContext = defineOpContext({
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

// Create tracer (concrete implementation)
const { trace } = new TestTracer(opContext);

// Request handler
app.post('/users', async (req, res) => {
  // Invoke via trace() - type-safe ctx properties
  const result = await trace(
    'create-user',
    {
      env: workerEnv,
      requestId: req.id,
      userId: req.user?.id,
    },
    createUser,
    req.body
  );

  if (result.success) {
    res.json(result.value);
  } else {
    res.status(400).json({ error: result.error });
  }
});
```

## Thread ID and Distributed Span Identification

The `thread_id` stored in SpanBuffer enables collision-resistant span identification:

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

The trace root captures time anchors at creation, stored in `TraceRoot`:

```typescript
// TraceRoot created by Tracer for root span
const traceRoot: TraceRoot = {
  trace_id: traceId,
  anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
  anchorPerfNow: typeof process !== 'undefined' && process.hrtime ? Number(process.hrtime.bigint()) : performance.now(),
  tracer: this, // Reference back to tracer for lifecycle hooks
};

// Timestamp calculation uses anchors from buffer's trace root
function getTimestamp(buffer: SpanBuffer): bigint {
  // Returns nanoseconds since epoch with sub-millisecond precision
  return (
    buffer._traceRoot.anchorEpochNanos +
    BigInt(Math.round((performance.now() - buffer._traceRoot.anchorPerfNow) * 1_000_000))
  );
}
```

**Benefits**:

- No `Date.now()` calls per span - only high-resolution timer deltas
- Sub-millisecond precision for performance analysis
- All spans in trace share same anchor via `_traceRoot` reference (comparable timestamps)
- Root buffer stores `TraceRoot`, children access via parent chain

## Feature Flag Integration

Feature flags are accessed via `ff` in the op context:

```typescript
const createUser = defineOp('createUser', async (ctx, userData: UserData) => {
  // Access flags via ctx.ff
  const advancedValidation = await ctx.ff.get('advancedValidation');
  if (advancedValidation) {
    await ctx.span('advanced-validate', advancedValidate, userData);
    advancedValidation.track({ action: 'validation', outcome: 'advanced' }); // Log ff-usage
  }

  const betaFeatures = await ctx.ff.get('betaFeatures');
  if (betaFeatures) {
    ctx.log.info('Beta features enabled');
    betaFeatures.track({ action: 'beta_features', outcome: 'enabled' });
  }

  return ctx.ok({ success: true });
});
```

## Usage Examples

### Basic Op Usage

```typescript
const opContext = defineOpContext({
  logSchema: defineLogSchema({
    userId: S.category(),
    operation: S.enum(['INSERT', 'UPDATE', 'DELETE']),
  }),
  ctx: { env: null as Env },
});

const createUser = opContext.defineOp('createUser', async (ctx, userData: UserData) => {
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

// Invoke via tracer
const { trace } = new TestTracer(opContext);
await trace('create-user', { env: myEnv }, createUser, userData);
```

### Using Dependencies

```typescript
const opContext = defineOpContext({
  logSchema: httpSchema,
  deps: {
    retry: retryOps,
    auth: authOps,
  },
  ctx: { env: null as Env },
});

const GET = opContext.defineOp('GET', async (ctx, url: string) => {
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
const flags = defineFeatureFlags({
  premiumProcessing: S.boolean().default(false).async(),
  newPaymentFlow: S.boolean().default(false).async(),
});

const opContext = defineOpContext({
  logSchema: orderSchema,
  deps: {
    premiumValidation: premiumValidationOps,
    newPayment: newPaymentOps,
    legacyPayment: legacyPaymentOps,
  },
  flags: flags.schema,
  ctx: {
    env: null as { paymentProvider: string },
  },
});

const processOrder = opContext.defineOp('processOrder', async (ctx, order: Order) => {
  ctx.tag.orderId(order.id).total(order.total);

  const premiumProcessing = await ctx.ff.get('premiumProcessing');
  if (premiumProcessing) {
    await ctx.span('premium-validate', ctx.deps.premiumValidation.validate, order);
    premiumProcessing.track({ action: 'premium_validate', outcome: 'applied' });
  }

  const paymentProvider = ctx.env.paymentProvider;

  const newPaymentFlow = await ctx.ff.get('newPaymentFlow');
  if (newPaymentFlow) {
    await ctx.span('new-payment', ctx.deps.newPayment.charge, order, paymentProvider);
    newPaymentFlow.track({ action: 'payment_flow', outcome: 'new' });
  } else {
    await ctx.span('legacy-payment', ctx.deps.legacyPayment.charge, order);
  }

  return ctx.ok({ success: true });
});

// Invoke via tracer with flag evaluator
const { trace } = new TestTracer(opContext, { flagEvaluator: myFlagEvaluator });
await trace('process-order', { env: myEnv }, processOrder, order);
```

## Performance Characteristics

### Op Creation

- **OpContext setup**: One allocation for metadata at defineOpContext() call
- **Schema compilation**: Done once at op context definition time
- **Deps binding**: Plain object reference, zero per-call allocation

### Op Invocation

- **Buffer creation**: One SpanBuffer per span with `_callsiteMetadata` reference
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
- **[Op Context Pattern](./01l_op_context_pattern.md)**: Defines `defineOpContext()`, `defineOp()` API, and Tracer usage
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

- `_callsiteMetadata`: The caller's OpMetadata (for row 0's git_sha/package_name/package_file)
- `metadata`: The Op's OpMetadata (for rows 1+ git_sha/package_name/package_file)
- `spanName`: The contextual name provided by caller
- `lineNumber_values`: Int32Array for line numbers per row (NOT a lineNumber property)
