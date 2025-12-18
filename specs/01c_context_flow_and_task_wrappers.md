# Context Flow and Op Wrappers

## Overview

The context flow system manages how trace context, feature flags, and user-extensible properties flow through the
application. It provides:

1. **Hierarchical context creation** from request → op → child span
2. **Op wrapper pattern** that creates span-aware contexts
3. **Performance optimization** through single allocation and direct references
4. **Type-safe context destructuring** with automatic span correlation
5. **User-extensible context** via `.ctx<Extra>()` for custom properties like env bindings

## Design Rationale: From ctx Parameter to Destructured Context

### Problem with Traditional ctx Parameter

An alternative approach would pass a `ctx` object through every function:

```typescript
// Alternative approach (rejected): ctx drilling problem
const createUser = op(async (ctx, userData) => {
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
// Destructure what you need
const request = op(async ({ span, log, tag }, url: string, opts: RequestOpts) => {
  tag.method(opts.method);
  log.info('Making request');
  const res = await fetch(url, opts);
  tag.status(res.status);
  return res;
});

// Caller controls span name via span()
await span('fetch-user', request, '/users/123', { method: 'GET' });
```

**Benefits**:

1. **No ctx drilling**: Destructure only what you need `{ span, log, tag }`
2. **Span name at call site**: Caller decides contextually meaningful name
3. **Zero allocation for deps**: Plain object references, not bound closures
4. **V8 friendly**: Op is a plain class, deps is a plain object

## Design Philosophy

**Key Insight**: Each span needs its own buffer for proper trace correlation. The op wrapper handles buffer creation,
entry writing, and context setup automatically.

**Context Hierarchy**:

```
TraceContext (created via module.traceContext())
├── traceId: string (system prop)
├── anchorEpochMicros: number (system prop)
├── anchorPerfNow: number (system prop)
├── threadId: bigint (system prop)
├── ff: FeatureFlagEvaluator (system prop)
├── span: RootSpanFn (system prop)
├── env: EnvironmentConfig (Extra - user-defined)
├── requestId: string (Extra - user-defined)
├── userId?: string (Extra - user-defined)
└── ctx.span('create-user', createUserOp, userData) creates:
    ├── Op Context (create-user span buffer)
    ├── tag: TagAPI (writes to create-user buffer)
    ├── log: SpanLogger (logs to create-user buffer)
    ├── span: ChildSpanCreator (creates child spans)
    ├── deps: BoundDeps (references to dep Op instances)
    ├── ff: traceCtx.ff.withBuffer(buffer) → NEW evaluator bound to buffer
    ├── env: SAME from TraceContext (Extra props passed through)
    ├── requestId: SAME from TraceContext (Extra props passed through)
    ├── userId: SAME from TraceContext (Extra props passed through)
    └── span('validate-user', validateOp) creates:
        ├── Child Op Context (validate-user span buffer)
        ├── tag: TagAPI (writes to validate-user buffer)
        ├── log: SpanLogger (logs to validate-user buffer)
        ├── span: ChildSpanCreator (for nested ops)
        ├── deps: SAME deps references
        ├── ff: parentCtx.ff.withBuffer(childBuffer)
        └── Extra props: SAME (env, requestId, userId)
```

**Op Context Properties**:

- `span(name, op, ...args)`: Invoke an op as a child span
- `log`: SpanLogger for info/debug/warn/error
- `tag`: TagAPI for span attributes
- `deps`: Bound dependency ops (can be destructured: `const { retry } = deps`)
- `ff`: Feature flag evaluator bound to current span's buffer
- Extra props from TraceContext (e.g., `env`, `requestId`, `userId` - user-defined via `.ctx<Extra>()`)

## Trace-Level Context Creation

**Purpose**: Create the initial context at request boundaries with user-specific feature flag evaluation and
high-precision time anchor.

### TraceContext Interface

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
interface TraceContext<FF, Extra> {
  // System properties (always present)
  traceId: string;
  anchorEpochMicros: number; // Date.now() * 1000 at trace root
  anchorPerfNow: number; // performance.now() at trace root (browser)
  // OR anchorHrTime: bigint;  // process.hrtime.bigint() at trace root (Node.js)
  threadId: bigint; // 64-bit random ID, generated once per worker/process
  ff: FeatureFlagEvaluator<FF>;
  span: RootSpanFn<FF, Extra>; // Root span creation - entry point for ops
} & Extra; // User-defined properties (e.g., requestId, userId, env)
```

**Key Design**: `requestId` and `userId` are NOT system properties - they are user-defined in `Extra` via
`.ctx<Extra>()`. The `Extra` type comes from the module's `.ctx<Extra>()` declaration and is spread into OpContext when
ops execute.

### Context Creation via Module.traceContext()

```typescript
// Worker-level thread ID (generated ONCE at worker/process startup)
const workerThreadId: bigint = generateRandom64Bit();

// Module creates TraceContext with proper typing
// Internal implementation:
function createTraceContext<FF, Extra>(
  module: Module<any, any, FF, Extra>,
  params: { ff: FeatureFlagEvaluator<FF> } & Extra
): TraceContext<FF, Extra> {
  const epochMs = Date.now();
  const perfNow = performance.now();

  return {
    traceId: generateTraceId(),
    anchorEpochMicros: epochMs * 1000,
    anchorPerfNow: perfNow,
    threadId: workerThreadId,
    ff: params.ff,
    span: (name, op, ...args) => op._invoke(this, null, name, args),
    ...params, // Spread Extra properties (e.g., requestId, userId, env)
  } as TraceContext<FF, Extra>;
}

// Usage at request entry point:
const ctx = appModule.traceContext({
  ff: ffEvaluator,
  env: workerEnv,
  requestId: req.headers.get('x-request-id')!,
  userId: session?.userId,
});

await ctx.span('handle-request', handleRequestOp, req);
```

## Op Definition and the op() Factory

### What is an Op?

An **Op** is a traced operation. It's created via the `op()` factory from a module:

```typescript
const httpModule = defineModule({
  metadata: { packageName: '@my-company/http', packagePath: 'src/index.ts' },
  schema: { status: S.number(), method: S.enum(['GET', 'POST']) },
  deps: { retry: retryModule },
  ff: { premiumApi: ff.boolean() },
}).ctx<{ env: { apiTimeout: number } }>();

const { op } = httpModule;

// Define ops - context destructured in signature
const request = op(async ({ span, log, tag, env }, url: string, opts: RequestOpts) => {
  tag.method(opts.method);
  tag.url(url);
  log.info('Making HTTP request');

  const res = await fetch(url, { ...opts, timeout: env.apiTimeout });
  tag.status(res.status);
  return res;
});
```

### Op Internal Structure

The `Op<Ctx, Args, Result>` type parameters match the function signature order:

```typescript
class Op<Ctx, Args extends unknown[], Result> {
  constructor(
    private module: ModuleContext,
    // fn MUST use the type parameters, not hardcoded types
    private fn: (ctx: Ctx, ...args: Args) => Promise<Result>
  ) {}

  /**
   * Internal invocation - called by span()
   * @param traceCtx - The root trace context
   * @param parentBuffer - Parent span's buffer (null for root)
   * @param spanName - Name decided by caller
   * @param args - User arguments to the op
   */
  async _invoke(
    traceCtx: TraceContext,
    parentBuffer: SpanBuffer | null,
    spanName: string,
    args: Args
  ): Promise<Result> {
    // 1. Create SpanBuffer with this module's schema (unprefixed internally)
    const buffer = createSpanBuffer(
      this.module.compiledSchema,
      this.module.metadata,
      traceCtx.traceId,
      traceCtx.threadId
    );

    // 2. Register with parent's children (RemappedBufferView if prefixed)
    if (parentBuffer) {
      parentBuffer.children.push(this.module.prefix ? new RemappedBufferView(buffer, this.module.prefixMap) : buffer);
      buffer.parent = parentBuffer;
    }

    // 3. Write span-start entry
    buffer.writeSpanStart(spanName, getCurrentLineNumber());

    // 4. Set up context with destructurable properties
    // Built-in properties + Extra from .ctx<Extra>()
    const opCtx = {
      span: createSpanFn(traceCtx, buffer),
      log: new this.module.SpanLogger(buffer),
      tag: new this.module.TagAPI(buffer),
      deps: this.module.boundDeps, // Plain object of Op references
      ff: traceCtx.ff.withBuffer(buffer),
      // Spread Extra properties from TraceContext (e.g., env, requestId, userId)
      ...extractExtraFromTraceContext(traceCtx),
    } as Ctx;

    // 5. Execute user function with try/catch for span-exception
    try {
      const result = await this.fn(opCtx, ...args);
      buffer.writeSpanOk();
      return result;
    } catch (error) {
      buffer.writeSpanException(error);
      throw error;
    }
  }
}
```

### Why This Design

1. **Span name at call site**: `span('retry-attempt', retry, 1)` - caller provides contextually meaningful name
2. **Module captures metadata**: Op knows its source package/file for observability
3. **Zero allocation deps**: `deps` is a plain object, not closures created per call
4. **V8 hidden class friendly**: Op is a simple class with fixed structure
5. **Single Ctx type param**: Op carries full Ctx requirement, contravariance at span() ensures compatibility

## OpContext Interface

The context passed to op functions combines built-in properties with user-extensible `Extra`:

```typescript
type OpContext<Schema, Deps, FF, Extra> = {
  // Invoke another op as a child span - supports multiple overloads
  span: SpanFn<OpContext<Schema, Deps, FF, Extra>>;

  // Log messages (info/debug/warn/error)
  log: LogAPI;

  // Span attributes (chainable)
  tag: TagAPI<Schema>;

  // Dependencies - can be destructured!
  deps: BoundDeps<Deps>;

  // Feature flags (logs access to current span)
  ff: FeatureFlagEvaluator<FF>;
} & Extra; // User-extensible properties via .ctx<Extra>()
```

**Key Design**: The `Extra` type is spread directly into the context, so properties like `env` become top-level and can
be destructured: `({ env, log, tag }) => ...`.

### Destructuring Pattern

Ops destructure the context in their signature, taking only what they need:

```typescript
// Full destructuring
const processUser = op(async ({ span, log, tag, deps, ff, env }, user: User) => {
  // ...
});

// Partial - only what's needed
const validateEmail = op(async ({ log, tag }, email: string) => {
  log.debug('Validating email');
  tag.email(email);
  // ...
});

// With deps destructuring
const fetchWithRetry = op(async ({ span, deps }, url: string) => {
  const { retry, auth } = deps; // Destructure deps too!

  try {
    return await span('fetch', request, url, { method: 'GET' });
  } catch (e) {
    await span('retry', retry, 1);
    throw e;
  }
});
```

## Child Span Creation via span()

The `span()` function is how ops call other ops:

```typescript
const createUser = op(async ({ span, log, tag }, userData: UserData) => {
  tag.userId(userData.id);

  // Validate via child span
  const valid = await span('validate', validateUser, userData);
  if (!valid.success) {
    return { success: false, error: 'validation_failed' };
  }

  // Save via child span
  const saved = await span('save', saveUser, userData);

  log.info('User created');
  return { success: true, user: saved };
});
```

### How span() Works

1. **Name provided by caller**: The first argument is the span name - caller decides contextually
2. **Op provided by caller**: Second argument is the Op instance to invoke
3. **Args passed through**: Remaining arguments go to the op function
4. **Buffer linking**: Child buffer registered with parent's children array
5. **RemappedBufferView**: If op has prefix, view maps prefixed columns for Arrow

### Example: Multiple Child Spans

```typescript
const processOrder = op(async ({ span, log, tag, deps }, order: Order) => {
  tag.orderId(order.id);

  // Multiple child spans with contextual names
  const validated = await span('validate-order', deps.validation, order);
  const inventory = await span('check-inventory', deps.inventory, order.items);
  const payment = await span('process-payment', deps.payments, order.total);

  // Even same op can have different names based on context
  await span('notify-customer', deps.notify, order.customerId, 'order_confirmed');
  await span('notify-warehouse', deps.notify, order.warehouseId, 'prepare_shipment');

  return { validated, inventory, payment };
});
```

## Root Invocation

At the application entry point, use `module.traceContext()` then `.span()`:

```typescript
// Wire dependencies at composition time
const appRoot = appModule.use({
  http: httpModule.prefix('http').use({
    retry: retryModule.prefix('http_retry').use(),
  }),
});

// Request handler
app.post('/users', async (req, res) => {
  // Create trace context via module - type-safe Extra properties
  const ctx = appModule.traceContext({
    ff: ffEvaluator,
    env: workerEnv,
    requestId: req.id,
    userId: req.user?.id,
  });

  // Root invocation via span()
  const result = await ctx.span('create-user', createUser, req.body);

  if (result.success) {
    res.json(result.data);
  } else {
    res.status(400).json({ error: result.error });
  }
});
```

### Alternative: Pre-Wired Module Root

For pre-wired modules, use the bound module's span method:

```typescript
// Pre-wire at app startup
const httpRoot = httpModule.prefix('http').use({
  retry: retryModule.prefix('http_retry').use(),
});

// Create trace context and invoke
const ctx = httpModule.traceContext({ ff: ffEvaluator, env: workerEnv });
const result = await ctx.span('GET', GET, 'https://example.com');
```

## Thread ID and Distributed Span Identification

The `threadId` in TraceContext enables collision-resistant span identification:

**Problem**: Simple incrementing span IDs (`span_id++`) collide across workers.

**Solution**: Separate columns for `thread_id` + `span_id`:

- `thread_id`: 64-bit random BigInt, generated once per worker at startup
- `span_id`: 32-bit incrementing counter, assigned per span creation

```
Worker A: parentBuffer = { threadId: 0xAAA..., spanId: 1 }
    │
    └─► pmap() dispatches to Thread B
        │
        ▼
Worker B: childBuffer = { threadId: 0xBBB..., spanId: 1, parent: parentBuffer }
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
const createUser = op(async ({ span, log, tag, ff }, userData: UserData) => {
  // Destructure flags - first access logs ff-access
  const { advancedValidation, betaFeatures } = ff;

  if (advancedValidation) {
    await span('advanced-validate', advancedValidate, userData);
    advancedValidation.track(); // Log ff-usage
  }

  if (betaFeatures) {
    log.info('Beta features enabled');
    betaFeatures.track({ feature: 'new_ui' });
  }

  return { success: true };
});
```

## Usage Examples

### Basic Op Usage

```typescript
const { op } = userModule;

const createUser = op(async ({ span, log, tag }, userData: UserData) => {
  // Tag span attributes
  tag.userId(userData.id).operation('INSERT');

  // Log messages
  log.info('Creating new user');

  // Call child ops via span()
  const validated = await span('validate', validateUser, userData);
  if (!validated.success) {
    log.warn('Validation failed');
    return { success: false, error: validated.error };
  }

  const user = await span('save', saveUser, userData);
  log.info('User created successfully');

  return { success: true, user };
});
```

### Using Dependencies

```typescript
const { op } = httpModule;

const GET = op(async ({ span, log, tag, deps }, url: string) => {
  // Destructure dependencies
  const { retry, auth } = deps;

  tag.method('GET').url(url);
  log.info('Starting GET request');

  // Get auth token via child span
  const token = await span('get-token', auth.getToken);

  const headers = { Authorization: `Bearer ${token}` };

  try {
    const res = await fetch(url, { headers });
    tag.status(res.status);
    return res;
  } catch (e) {
    log.error('Request failed, retrying');
    await span('retry', retry, 1);
    throw e;
  }
});
```

### Feature Flags and Environment

```typescript
const processOrder = op(async ({ span, log, tag, deps, ff, env }, order: Order) => {
  const { premiumProcessing, newPaymentFlow } = ff;

  tag.orderId(order.id).total(order.total);

  if (premiumProcessing) {
    await span('premium-validate', deps.premiumValidation, order);
    premiumProcessing.track();
  }

  const paymentProvider = env.paymentProvider;

  if (newPaymentFlow) {
    await span('new-payment', deps.newPayment, order, paymentProvider);
    newPaymentFlow.track({ provider: paymentProvider });
  } else {
    await span('legacy-payment', deps.legacyPayment, order);
  }

  return { success: true };
});
```

## Performance Characteristics

### Op Creation

- **Module setup**: One allocation for Op instance at module load
- **Schema compilation**: Done once at module definition time
- **Deps binding**: Plain object reference, zero per-call allocation

### Op Invocation

- **Buffer creation**: One SpanBuffer per span (pooled/reused)
- **Context creation**: One OpContext object per invocation
- **No closure allocation**: deps is shared object reference

### Memory Usage

- **Shared references**: Module context and deps shared across all ops
- **Direct buffer access**: TagAPI/SpanLogger hold buffer reference directly
- **Buffer management**: Self-tuning capacity per module

## Integration Points

This context flow system integrates with:

- **[Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md)**: Provides the module
  setup and SpanLogger/TagAPI class generation
- **[Module Builder Pattern](./01l_module_builder_pattern.md)**: Defines `defineModule()` and `op()` API
- **[Span Scope Attributes](./01i_span_scope_attributes.md)**: Provides span-level attribute scoping
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Shows how libraries define ops
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Defines the SpanBuffer structure

## Line Number System

The `lineNumber` column allows linking trace entries back to source code:

- **Storage**: Uint16 column (max 65535 lines per file)
- **Injection**: TypeScript transformer injects `.line(N)` calls at compile time
- **Overhead**: Zero runtime overhead - just a method call with literal

```typescript
// Source code:
log.info('Processing user');
await span('validate', validateUser, userData);

// Transformed:
log.info('Processing user').line(42);
await span('validate', validateUser, userData).line(43);
```
