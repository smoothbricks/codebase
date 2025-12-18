# Context Flow and Task Wrappers

## Overview

The context flow system manages how trace context, feature flags, and environment variables flow through the
application. It provides:

1. **Hierarchical context creation** from request → task → child span
2. **Task wrapper pattern** that creates span-aware contexts
3. **Performance optimization** through single allocation and self-reference
4. **Type-safe context enhancement** with automatic span correlation

## Design Philosophy

**Key Insight**: Each level of the call hierarchy needs its own context instance to ensure proper trace correlation.
Feature flag and environment variable access must be logged to the correct span buffer.

**Context Hierarchy**:

```
Request Context
├── ff: FeatureFlagEvaluator (no buffer - can evaluate but not log)
├── env: EnvironmentConfig (plain object)
└── task('create-user') creates:
    ├── Span Context (create-user span buffer)
    ├── tag: TagAPI (new instance, writes to create-user buffer)
    ├── log: SpanLogger (new instance, logs to create-user buffer)
    ├── ff: parentCtx.ff.withBuffer(buffer) → NEW evaluator, same context, bound to buffer
    ├── env: SAME EnvironmentConfig (passed through)
    └── ctx.span('validate-user') creates:
        ├── Child Span Context (validate-user span buffer)
        ├── tag: TagAPI (new instance, writes to validate-user buffer)
        ├── log: SpanLogger (new instance, logs to validate-user buffer)
        ├── ff: parentCtx.ff.withBuffer(childBuffer) → NEW evaluator, same context
        └── env: SAME EnvironmentConfig (passed through)
    └── ctx.span('process-user', { additionalContext: { userId } }) creates:
        ├── Child Span Context (process-user span buffer)
        ├── ff: parentCtx.ff.forContext({ userId }).withBuffer(childBuffer)
        │       → NEW evaluator with MERGED context (adds userId)
        └── ...
```

**FF Evaluator Methods**:

- `forContext(additional)`: Creates new evaluator with merged evaluation context AND fresh cache
- `withBuffer(buffer)`: Creates new evaluator bound to span buffer (fresh cache)
- Both return NEW instances (immutable pattern, each span gets fresh cache)

**Context-Level forContext() Method**:

The `RequestContext` and `SpanContext` also have a `forContext()` method that creates a child context with a new FF
evaluator. This is the recommended pattern for creating child contexts with different scope values:

```typescript
// Create child context with scope-bound FF evaluator
const userCtx = ctx.forContext({ userId: 'user-123' });
// Equivalent to:
// - child.ff = ctx.ff.forContext({ userId: 'user-123' })
// - child.scope = { ...ctx.scope, userId: 'user-123' }
```

## Request-Level Context Creation

**Purpose**: Create the initial context at request boundaries with user-specific feature flag evaluation and
high-precision time anchor.

### RequestContext Interface (Flattened - No Nested Objects)

```typescript
interface RequestContext {
  traceId: string;
  requestId: string;
  userId?: string;

  // Time anchor - flat primitives, not nested object
  // See 01b_columnar_buffer_architecture.md for full timestamp design details
  anchorEpochMicros: number; // Date.now() * 1000 at trace root
  anchorPerfNow: number; // performance.now() at trace root (browser)
  // OR anchorHrTime: bigint;  // process.hrtime.bigint() at trace root (Node.js)

  // Worker/Thread ID for distributed span identification
  // See 01b_columnar_buffer_architecture.md "Distributed Span ID Design" section
  threadId: bigint; // 64-bit random ID, generated once per worker/process

  ff: FeatureFlagEvaluator;
  env: Env;
  scope: EvaluationContext; // Scope values for logging + FF evaluation

  /**
   * Create a child context with additional scope values.
   * The child context gets a NEW FF evaluator bound to the merged scope.
   * Uses prototype-based creation for V8 hidden class optimization.
   */
  forContext(additional: Partial<EvaluationContext>): RequestContext;
}
```

**Why Flattened**:

- Better V8 hidden class optimization
- One less pointer chase per timestamp read
- Simpler serialization if context crosses boundaries
- Matches "flat deferred structure" design principle

### Thread ID and Distributed Span Identification

The `threadId` in RequestContext enables collision-resistant span identification in distributed tracing scenarios:

**Problem**: Simple incrementing span IDs (`span_id++`) collide when the same trace spans multiple workers/processes.

**Solution**: Separate columns for `thread_id` + `span_id`:

- `thread_id`: 64-bit random BigInt, generated once per worker at startup
- `span_id`: 32-bit incrementing counter, assigned per span creation

> **A span represents a unit of work within a single thread of execution.** This justifies having both `thread_id` and
> `span_id` as separate concepts.

**Flow**:

```
Worker Startup
    │
    ├─► Generate workerThreadId: bigint (ONCE, crypto.getRandomValues())
    │
    ▼
createRequestContext()
    │
    ├─► ctx.threadId = workerThreadId
    │
    ▼
task('my-task', fn) execution
    │
    ├─► buffer.threadId = ctx.threadId
    ├─► buffer.spanId = nextSpanId++
    │
    ▼
ctx.span('child-span') creation
    │
    ├─► childBuffer.threadId = ctx.threadId (SAME thread)
    ├─► childBuffer.spanId = nextSpanId++ (NEW span ID)
    ├─► childBuffer.parent = parentBuffer (for parent ID derivation)
```

**Cross-Thread Scenario (pmap/workers)**: When work is distributed to another thread, the child span gets THAT thread's
`threadId`:

```
Thread A: parentBuffer = { threadId: 0xAAA..., spanId: 1 }
    │
    └─► pmap() dispatches to Thread B
        │
        ▼
Thread B: childBuffer = { threadId: 0xBBB..., spanId: 1, parent: parentBuffer }
          Arrow output: parent_thread_id=0xAAA..., parent_span_id=1
```

**Benefits**:

- No BigInt operations in hot path (spanId is just `i++`)
- Cross-thread parent references naturally supported
- Collision resistance via 64-bit random thread ID
- Separate columns allow efficient querying by thread_id alone

### High-Precision Timestamp Design

The context captures a single time anchor at creation, enabling sub-millisecond precision for all spans in the trace:

```typescript
// Browser: ONE Date.now() + ONE performance.now() captured at trace root
// Node.js: ONE Date.now() + ONE process.hrtime.bigint() captured at trace root
// All subsequent timestamps use delta calculation from anchor

function getTimestamp(ctx: RequestContext): bigint {
  // Returns nanoseconds since epoch with sub-millisecond precision
  // Browser: ~5μs resolution from performance.now()
  // Node.js: nanosecond precision from hrtime
  return ctx.anchorEpochNanos + BigInt(Math.round((performance.now() - ctx.anchorPerfNow) * 1_000_000));
}
```

**Benefits**:

- No `Date.now()` calls per span - only high-resolution timer deltas
- Sub-millisecond precision for performance analysis
- All spans in trace share same anchor (comparable timestamps)
- DST/NTP safe - anchor per trace, traces are short-lived
- Safe numeric conversion (Float64 SAFE until year 2255)

**Precision Guarantees**:

- **Storage**: Nanoseconds in `BigInt64Array` (hot path)
- **Safe Range**: Float64 can exactly represent integers up to `Number.MAX_SAFE_INTEGER` (2^53 - 1)
  - Maximum safe value: 9,007,199,254,740,991 microseconds
  - Equivalent date: ~September 2255
- **BigInt Conversion**: `Number(hrtime / 1000n)` is SAFE for all practical timestamps

**Date.now() Usage Guidelines**:

- ❌ **NEVER** use `Date.now()` for trace timestamps (use anchored `getTimestampMicros()`)
- ✅ **OK** for scheduling: `const nextFlush = Date.now() + intervalMs`
- ✅ **OK** for file naming: `traces-${Date.now()}.parquet`
- ✅ **OK** for system metadata: Capacity tuning events, diagnostics

### Context Creation

```typescript
// Worker-level thread ID (generated ONCE at worker/process startup)
// See 01b_columnar_buffer_architecture.md "Distributed Span ID Design" for rationale
const workerThreadId: bigint = generateRandom64Bit(); // crypto.getRandomValues() → BigInt

// Create context at request boundary
function createRequestContext(params: { requestId: string; userId?: string }): RequestContext {
  const epochMs = Date.now();
  const perfNow = performance.now(); // Or process.hrtime.bigint() in Node.js

  return {
    requestId: params.requestId,
    userId: params.userId,
    traceId: generateTraceId(),

    // Capture time anchor ONCE at trace root
    anchorEpochMicros: epochMs * 1000,
    anchorPerfNow: perfNow,

    // Thread ID for distributed span identification
    // All spans created in this worker share the same threadId
    // Combined with spanId (incrementing counter), this forms a collision-resistant span ID
    threadId: workerThreadId,

    // Feature flag evaluator with user context
    // Buffer is null - will be bound when task creates a span via withBuffer()
    ff: new FeatureFlagEvaluator(
      featureFlags,
      { userId: params.userId, requestId: params.requestId },
      new DatabaseFlagEvaluator(),
      null // No buffer yet - bound in task wrapper
    ),

    // Environment config (just a plain object)
    env: environmentConfig,
  };
}

// Usage in request handler
app.post('/users', async (req, res) => {
  const ctx = createRequestContext({
    requestId: req.id,
    userId: req.user?.id,
  });

  // Context flows through all operations
  // All timestamps derived from ctx.anchorEpochMicros + delta
  // FF evaluator will be bound to span buffers in task wrappers
  const result = await createUser(ctx, req.body);

  if (result.success) {
    res.json(result.data);
  } else {
    res.status(400).json({ error: result.error });
  }
});
```

**Why This Design**:

- **User context**: Feature flags can be user-specific from the start
- **Request correlation**: All operations share the same request ID and trace ID
- **Deferred buffer binding**: FF evaluator created without buffer, bound later per span
- **Environment sharing**: Static config shared across all operations
- **Type safety**: Full TypeScript inference for the context object

## Module Context and SpanLogger Generation

**See**: [Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md) for the complete
specification of how modules are set up and SpanLogger classes are generated.

The module context system provides:

- **Schema compilation** for both standard and library modules
- **SpanLogger class generation** with typed attribute methods
- **Factory patterns** for clean library integration
- **Zero-runtime-overhead** through build-time code generation

**Quick Example**:

```typescript
// Module setup (build/startup time)
const userModule = defineModule({
  metadata: { gitSha: 'abc123...', packageName: '@mycompany/user-service', packagePath: 'src/services/user.ts' },
  schema: dbAttributes,
});

const { task } = userModule;

// Generated TagAPI provides typed methods
export const createUser = task('create-user', async (ctx, userData) => {
  ctx.tag.userId(userData.id).operation('INSERT'); // ✅ TypeScript knows these methods
  return ctx.ok(user);
});
```

## Task Wrapper Implementation

**Purpose**: Create span-aware contexts that log feature flag and environment access to the correct span buffer.

**Key Design Decision**: Each span creates its own buffer and a new `SpanLogger` instance. This design:

- Gives the logger a direct reference to its buffer (avoiding traceid+spanid lookups on every log statement)
- Keeps each span's logs neatly sorted together in the final Arrow output
- Enables zero-copy conversion since each span buffer can be directly sliced to Arrow vectors
- Each span has its own buffer, so traceId and spanId TypedArrays are not needed - they're constant per buffer

```typescript
function createTaskWrapper(moduleContext: ModuleContext, compiledTagOps: CompiledTagOperations) {
  return function task(spanName: string, fn: TaskFunction) {
    // Do expensive work ONCE at task definition time
    const taskModuleContext: TaskModuleContext = {
      module: moduleContext, // Shared reference
      spanName: spanName, // Raw string - dictionary built during Arrow conversion
      lineNumber: getCurrentLineNumber(), // Build tool injected
      SpanLogger: compiledTagOps.SpanLogger, // Per-module SpanLogger class with compiled prototype
    };

    // Return lightweight wrapper
    return (...args) => {
      const [originalCtx, ...restArgs] = args;

      // Create new SpanBuffer for this task execution
      // threadId comes from context (set at worker startup)
      // spanId is assigned by createSpanBuffer (simple i++)
      const buffer = createSpanBuffer(
        compiledTagOps.schema,
        taskModuleContext,
        originalCtx.traceId,
        originalCtx.threadId // 64-bit thread ID from context
      );

      // Create enhanced context once (single allocation)
      const enhancedCtx = {
        ...originalCtx,
        log: new taskModuleContext.SpanLogger(buffer), // SpanLogger class from module context
      };

      // Bind FF evaluator to this span's buffer using withBuffer()
      // Context stays the same (inherited from request), only buffer changes
      enhancedCtx.ff = originalCtx.ff.withBuffer(buffer);

      // Environment variables are just passed through (no evaluator needed)
      enhancedCtx.env = originalCtx.env;

      return fn(enhancedCtx, ...restArgs);
    };
  };
}
```

**Why This Implementation**:

- **Single allocation**: Only one object creation per task execution
- **withBuffer() pattern**: FF evaluator bound to span buffer, context inherited from parent
- **Shared evaluator backend**: Reuse the same flag evaluation logic (only buffer changes)
- **Fresh cache**: Each span gets fresh FF cache (via `withBuffer()` creating new instance)
- **Environment pass-through**: No overhead for environment variable access
- **Unified logging interface**: Single `SpanLogger` class handles all entry types

**V8 Memory Layout Optimization**: Depending on V8's memory layout and GC behavior, we could optimize by either:

- **Option A**: Store buffers, spanId, traceId directly in Context, create new Logger and FeatureFlag instances with
  Context reference
- **Option B**: Logger and FF instances store their own buffer/spanId/traceId references directly

This is a performance tuning decision that should be benchmarked. Option A keeps Context as the single source of truth.
Option B reduces indirection (one less pointer chase per buffer access).

## Span Context API Design

### Separate Tag and Log APIs

The span context provides separate APIs for span attributes (`ctx.tag`) and log messages (`ctx.log`):

```typescript
// TagAPI handles span attributes (chainable)
class TagAPI {
  constructor(buffer) {
    this.buffer = buffer;
  }

  // Callable for object-based API: ctx.tag({ userId: "123" })
  (attributes) {
    this._writeTagEntry();
    this._writeAttributes(attributes);
    return this;
  }

  // Attribute methods compiled onto prototype at module context creation time
  userId(value) {
    this._writeTagEntry();
    this.buffer.attr_userId[this.buffer.writeIndex] = value;
    return this;
  }
  httpStatus(value) {
    this._writeTagEntry();
    this.buffer.attr_httpStatus[this.buffer.writeIndex] = value;
    return this;
  }
}

// SpanLogger handles log messages
class SpanLogger {
  constructor(buffer) {
    this.buffer = buffer;
  }

  info(message, attributes?) {
    this._writeLogEntry('info', message);
    if (attributes) this._writeAttributes(attributes);
    return this;
  }

  scope(attributes) {
    this._prefillRemainingCapacity(attributes);
    return this;
  }

  with(attributes) {
    this._writeAttributes(attributes);
    return this;
  }
}
```

### Design Benefits

- **Cleaner API**: `ctx.tag.userId()` is cleaner than `ctx.log.tag.userId()`
- **OpenTelemetry alignment**: Mirrors `Span.setAttribute()` pattern
- **Zero allocation**: Only TagAPI and SpanLogger instances allocated per span
- **Runtime class generation**: Classes built with `new Function` for typed methods
- **Per-span buffers**: Each span gets its own buffer (traceId/spanId constant per buffer)
- **Direct buffer reference**: APIs have reference to buffer, avoiding lookups
- **Sorted output**: Each span's entries together in Arrow output
- **Prototype compilation**: Attribute methods on prototype, not runtime
- **Fluent chaining**: All methods return `this` for seamless chaining

## Library Integration Pattern

**See**: [Library Integration Pattern](./01e_library_integration_pattern.md) for the complete specification of how
third-party libraries can provide traced operations with clean APIs while avoiding attribute name conflicts.

The core task wrapper pattern described in this document provides the foundation that libraries build upon to create
their own traced operations.

## Child Span Creation

**Purpose**: Create nested spans with their own context and buffer while maintaining parent-child relationships. Child
spans can optionally provide additional evaluation context for feature flags (e.g., adding userId when processing a
specific user in a batch job).

### Basic Child Span (Context Inherited)

```typescript
function createChildSpan(parentCtx: SpanContext, spanName: string, childFn: SpanFunction) {
  return async () => {
    // Create child buffer linked to parent
    // Child uses same threadId as parent (same worker)
    // spanId is assigned fresh by createSpanBuffer
    const childBuffer = createSpanBuffer(
      parentCtx.buffer.task.module.schema,
      {
        ...parentCtx.buffer.task,
        spanName: spanName, // Raw string - dictionary built during Arrow conversion
        lineNumber: getCurrentLineNumber(),
      },
      parentCtx.traceId,
      parentCtx.threadId, // Same thread ID as parent
      parentCtx.buffer // Parent buffer for tree linking
    );

    // parent-child relationship is set by createSpanBuffer
    // childBuffer.parent = parentCtx.buffer (set in createSpanBuffer)
    // parentCtx.buffer.children.push(childBuffer) (set in createSpanBuffer)

    // Create child context
    const childEnhancedCtx = {
      ...parentCtx,
      buffer: childBuffer,
      log: new parentCtx.buffer.task.SpanLogger(childBuffer),
    };

    // Bind FF evaluator to child buffer (context inherited from parent)
    childEnhancedCtx.ff = parentCtx.ff.withBuffer(childBuffer);

    // Environment variables passed through unchanged
    childEnhancedCtx.env = parentCtx.env;

    return await childFn(childEnhancedCtx);
  };
}
```

### Child Span with Additional FF Context

When the child span operates in a different evaluation context (e.g., processing a specific user):

```typescript
interface SpanOptions {
  // Additional context for feature flag evaluation
  additionalContext?: Partial<EvaluationContext>;
}

function createChildSpanWithContext(
  parentCtx: SpanContext,
  spanName: string,
  options: SpanOptions,
  childFn: SpanFunction
) {
  return async () => {
    // Create child buffer linked to parent
    const childBuffer = createSpanBuffer(parentCtx.buffer.task.module.schema, {
      ...parentCtx.buffer.task,
      spanName: spanName, // Raw string - dictionary built during Arrow conversion
      lineNumber: getCurrentLineNumber(),
    });

    // Link parent-child relationship
    childBuffer.parent = parentCtx.buffer;
    parentCtx.buffer.children.push(childBuffer);

    // Create child context
    const childEnhancedCtx = {
      ...parentCtx,
      buffer: childBuffer,
      log: new parentCtx.buffer.task.SpanLogger(childBuffer),
    };

    // Create FF evaluator with:
    // 1. Additional context merged with parent context (if provided)
    // 2. Bound to child buffer for logging
    childEnhancedCtx.ff = options.additionalContext
      ? parentCtx.ff.forContext(options.additionalContext).withBuffer(childBuffer)
      : parentCtx.ff.withBuffer(childBuffer);

    // Environment variables passed through unchanged
    childEnhancedCtx.env = parentCtx.env;

    return await childFn(childEnhancedCtx);
  };
}
```

### Alternative: Context-Level forContext() (Recommended)

Instead of passing `additionalContext` to `span()`, use `ctx.forContext()` to create a child context first:

```typescript
// RECOMMENDED: Use forContext() at context level
const userCtx = ctx.forContext({ userId: user.id, userPlan: user.plan });
await userCtx.span('processUser', async (childCtx) => {
  // childCtx.ff already has userId and userPlan in evaluation context
  const { premiumFeatures } = childCtx.ff;
  // ...
});

// This is cleaner than:
await ctx.span(
  'processUser',
  { additionalContext: { userId: user.id, userPlan: user.plan } },
  async (childCtx) => { ... }
);
```

**V8 Optimization**: The `forContext()` method uses prototype-based context creation to maintain stable hidden classes:

```typescript
class SpanContext {
  forContext(additional: Partial<EvaluationContext>): SpanContext {
    // Use Object.create for prototype chain - maintains V8 hidden class
    const child = Object.create(Object.getPrototypeOf(this));
    Object.assign(child, this);

    // Override scope and ff with merged values
    child.scope = { ...this.scope, ...additional };
    child.ff = this.ff.forContext(additional);

    return child;
  }
}
```

### Usage: Context Changes in Child Spans

```typescript
// Batch processing example - parent has no userId, children add it
export const processBatch = task('process-batch', async (ctx, users: User[]) => {
  // Parent span: no specific user
  const { batchProcessing } = ctx.ff; // Evaluated without userId

  for (const user of users) {
    // Child span: add userId to FF evaluation context
    await ctx.span(
      'process-user',
      { additionalContext: { userId: user.id, userPlan: user.plan } },
      async (childCtx) => {
        // childCtx.ff evaluates flags WITH userId and userPlan!
        const { premiumFeatures } = childCtx.ff;

        if (premiumFeatures) {
          await enablePremiumForUser(user);
          premiumFeatures.track();
        }
      }
    );
  }

  return ctx.ok({ processed: users.length });
});
```

**Why This Design**:

- **Tree structure**: Child buffers linked to parents for efficient traversal
- **Context isolation**: Each span gets its own context and buffer
- **Feature flag correlation**: Flag access logged to the correct span's buffer
- **Context inheritance**: Child FF evaluator inherits parent's evaluation context
- **Context extension**: Child can add/override evaluation context via `forContext()`
- **Fresh cache**: Each span's FF evaluator has its own cache (deduplication per span)
- **Environment sharing**: Static config shared across all spans

## Usage Examples

### Basic Task Usage

```typescript
export const createUser = task('create-user', async (ctx, userData: UserData) => {
  // Feature flag access - destructure to check AND get handle
  // First access logs ff-access entry to this span's buffer
  const { advancedValidation } = ctx.ff;

  if (advancedValidation) {
    // advancedValidation is truthy wrapper here (type-narrowed)
    const result = await performAdvancedValidation(userData);
    // Track usage without repeating flag name
    advancedValidation.track({
      result: result.success ? 'success' : 'failure',
    });
  }

  // Environment access (just property access, no logging)
  const region = ctx.env.awsRegion;
  const maxConnections = ctx.env.maxConnections;

  // Span attributes (via ctx.tag - directly on context)
  ctx.tag.requestId(ctx.requestId).userId(userData.id).operation('INSERT');

  // Log messages (via ctx.log)
  ctx.log.info('Creating new user').with({ userId: userData.id, email: userData.email });

  const user = await db.createUser(userData);
  return ctx.ok(user).with({ userId: user.id, operation: 'CREATE' });
});
```

### Nested Span Usage (Context Inherited)

```typescript
export const processOrder = task('process-order', async (ctx, order: Order) => {
  ctx.tag.orderId(order.id).amount(order.total);

  // Child span - inherits parent's FF evaluation context
  const validation = await ctx.span('validate-order', async (childCtx) => {
    childCtx.tag.itemCount(order.items.length);

    // Feature flag access logged to CHILD span's buffer
    // Destructure for deduplication within this span
    const { strictValidation } = childCtx.ff;

    if (strictValidation) {
      strictValidation.track();
    }

    if (order.items.length === 0) {
      return childCtx.err('EMPTY_ORDER').with({ orderId: order.id });
    }

    return childCtx.ok({ valid: true });
  });

  if (!validation.success) {
    return ctx.err('VALIDATION_FAILED').with({ orderId: order.id, validationStep: 'order_validation' });
  }

  // Another child span with initial tag attributes
  const payment = await ctx
    .span('process-payment')
    .with({ paymentMethod: order.paymentMethod, amount: order.total })
    .run(async (childCtx) => {
      childCtx.tag.paymentMethod(order.paymentMethod);

      // Environment access in child span
      const paymentProvider = childCtx.env.paymentProvider;

      return await processPayment(order, paymentProvider);
    });

  return ctx
    .ok({ orderId: order.id, paymentId: payment.data.id })
    .with({ orderId: order.id, paymentMethod: order.paymentMethod, amount: order.total });
});
```

### Child Span with Different FF Evaluation Context

```typescript
// Batch processing - add user-specific context for child spans
export const processBatch = task('process-batch', async (ctx, users: User[]) => {
  // Parent span: no specific user in FF evaluation context
  const { batchProcessing } = ctx.ff; // Evaluated without userId

  if (batchProcessing) {
    batchProcessing.track();
  }

  for (const user of users) {
    // Child span: ADD userId and userPlan to FF evaluation context
    await ctx.span(
      'process-user',
      { additionalContext: { userId: user.id, userPlan: user.plan } },
      async (childCtx) => {
        // childCtx.ff evaluates flags WITH userId and userPlan!
        // The evaluator was created via: parentCtx.ff.forContext({...}).withBuffer(childBuffer)
        const { premiumFeatures, betaAccess } = childCtx.ff;

        if (premiumFeatures) {
          await enablePremiumForUser(user);
          premiumFeatures.track();
        }

        if (betaAccess) {
          await enrollInBeta(user);
          betaAccess.track();
        }

        return childCtx.ok({ userId: user.id });
      }
    );
  }

  return ctx.ok({ processed: users.length });
});
```

### Deduplication Across Spans

```typescript
export const demonstrateDeduplication = task('demo', async (ctx) => {
  // First access in parent span - logs ff-access
  const { featureA } = ctx.ff;
  // Second access in parent span - NO log (cached)
  const { featureA: sameFlag } = ctx.ff;

  await ctx.span('child-span', async (childCtx) => {
    // First access in CHILD span - logs ff-access (fresh cache)
    const { featureA: childFlag } = childCtx.ff;

    // Same flag, same child span - NO log (cached in child)
    const { featureA: childFlagAgain } = childCtx.ff;

    if (childFlag) {
      // track() always logs ff-usage (not deduplicated)
      childFlag.track();
      childFlag.track(); // Also logged
    }
  });

  if (featureA) {
    featureA.track(); // Logged to parent span
  }

  return ctx.ok({});
});
```

## Context Flow Benefits

1. **Trace Correlation**: Every feature flag and environment access is correlated with the specific operation
2. **Performance Optimization**: Single allocation per context creation
3. **Type Safety**: Full TypeScript inference throughout the context hierarchy
4. **Granular Analytics**: See which features are used in which parts of the request flow
5. **Debugging**: When a span fails, see exactly which flags/env vars were accessed
6. **Memory Efficiency**: Shared module context and environment config

## Performance Characteristics

### Context Creation

- **Request context**: One allocation per request
- **Task context**: One allocation per task execution (optimized)
- **Child span context**: One allocation per nested span

### Memory Usage

- **Shared references**: Module context and environment config shared
- **Feature flag evaluators**: Lightweight wrappers around shared backend
- **Buffer management**: Self-tuning capacity per module

### Runtime Overhead

- **Feature flag access**: Direct property access for sync flags
- **Environment access**: Plain property lookup (zero overhead)
- **Tag operations**: <0.1ms per operation (TypedArray writes)

This context flow design ensures that every piece of configuration access is properly attributed to the correct span
while maintaining excellent performance characteristics.

## Integration Points

This context flow system integrates with:

- **[Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md)**: Provides the module
  setup and SpanLogger class generation that task wrappers use
- **[Span Scope Attributes](./01i_span_scope_attributes.md)**: Provides span-level attribute scoping that works with the
  context hierarchy
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Details how the `ctx.tag`, `ctx.info`, etc. APIs
  are generated at runtime
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Shows how libraries create their own task
  wrappers using this foundation
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Defines the SpanBuffer structure that
  contexts write to

## Span Scope Attributes

**See**: [Span Scope Attributes](./01i_span_scope_attributes.md) for the complete specification of how to set span-level
attributes that automatically propagate to all subsequent log entries and child spans.

The scope attributes system provides:

- **Zero-runtime-cost** attribute inclusion through lazy column getters
- **Separate Scope class** that stores values apart from buffer columns
- **Hierarchical inheritance** from parent to child spans via Scope.\_getScopeValues()
- **SIMD pre-filling** of child buffers with TypedArray.fill()
- **Middleware integration** for request-level context setup
- **Clean business logic** that focuses on domain concerns rather than logging boilerplate
- **Readable scope values** via compiled getters
- **Zero indirection** with column properties as direct lazy getters on SpanBuffer

**Quick Example**:

```typescript
// Set scope once at middleware level using property assignment
ctx.scope.requestId = req.id;
ctx.scope.userId = req.user?.id;

// All subsequent operations automatically include these attributes
ctx.log.info('Processing order'); // ← Includes requestId, userId
ctx.tag.step('validation'); // ← Includes requestId, userId + step

// Child spans inherit and can READ/extend scope
await ctx.span('payment', async (childCtx) => {
  // Can read inherited scope values
  console.log('Processing for user:', childCtx.scope.userId);

  // Extend scope for this span
  childCtx.scope.paymentMethod = 'stripe';
  childCtx.log.info('Processing payment'); // ← Includes requestId, userId, paymentMethod
});
```

## Line Number System

The `lineNumber` system column allows linking trace entries back to source code locations.

### Design

- **Storage**: Uint16 column (max 65535 lines per file)
- **Injection**: TypeScript transformer injects `.line(N)` calls at compile time
- **Overhead**: Zero runtime overhead - just a method call with literal number
- **Default**: Value of 0 means "line number not set"

### API

The `.line()` fluent method is available on:

- **Logging methods**: `ctx.log.info('msg').line(42)`
- **Span creation**: `await ctx.span('name', fn).line(42)`
- **Results**: `ctx.ok(value).line(42)` and `ctx.err('CODE', details).line(42)`

### TypeScript Transformer Approach

The line numbers are injected at compile time by a TypeScript transformer:

```typescript
// Source code (what you write):
ctx.log.info('Processing user');
await ctx.span('validate', async (childCtx) => { ... });

// Transformed code (after compilation):
ctx.log.info('Processing user').line(42);
await ctx.span('validate', async (childCtx) => { ... }).line(43);
```

**Benefits**:

- **Zero runtime cost**: No stack trace parsing or Error.captureStackTrace
- **Accurate line numbers**: Compiler knows exact source location
- **Source map compatible**: Works with source maps for debugging
- **Optional**: If transformer not used, line numbers default to 0

### Implementation Notes

The `lineNumber` is stored in the `attr_lineNumber` system column:

```typescript
// In systemSchema.ts
lineNumber: (S.number(), // Uint16 storage (0-65535)
  // In generated code
  (fluentSpan.line = (lineNumber) => {
    writeToColumn(childBuffer, 'attr_lineNumber', lineNumber, 0);
    return resultPromise;
  }));
```

### Usage Example

```typescript
const createUser = task('create-user', async (ctx, userData) => {
  // Scoped attributes propagate to all entries
  ctx.scope({ userId: userData.id });

  // Line numbers injected by transformer
  ctx.log.info('Starting user creation').line(15);

  const result = await ctx
    .span('validate', async (childCtx) => {
      childCtx.log.debug('Validating email').line(19);
      return childCtx.ok({ valid: true });
    })
    .line(18);

  return ctx.ok(user).line(24);
});
```
