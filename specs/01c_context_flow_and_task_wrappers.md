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
        ├── ff: parentCtx.ff.withContext({ userId }).withBuffer(childBuffer)
        │       → NEW evaluator with MERGED context (adds userId)
        └── ...
```

**FF Evaluator Methods**:

- `withBuffer(buffer)`: Creates new evaluator bound to span buffer (fresh cache)
- `withContext(additional)`: Creates new evaluator with merged evaluation context
- Both return NEW instances (immutable pattern, each span gets fresh cache)

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

  ff: FeatureFlagEvaluator;
  env: Env;
}
```

**Why Flattened**:

- Better V8 hidden class optimization
- One less pointer chase per timestamp read
- Simpler serialization if context crosses boundaries
- Matches "flat deferred structure" design principle

### High-Precision Timestamp Design

The context captures a single time anchor at creation, enabling sub-millisecond precision for all spans in the trace:

```typescript
// Browser: ONE Date.now() + ONE performance.now() captured at trace root
// Node.js: ONE Date.now() + ONE process.hrtime.bigint() captured at trace root
// All subsequent timestamps use delta calculation from anchor

function getTimestamp(ctx: RequestContext): number {
  // Returns microseconds since epoch with sub-millisecond precision
  // Browser: ~5μs resolution from performance.now()
  // Node.js: nanosecond precision from hrtime, stored as microseconds
  return ctx.anchorEpochMicros + (performance.now() - ctx.anchorPerfNow) * 1000;
}
```

**Benefits**:

- No `Date.now()` calls per span - only high-resolution timer deltas
- Sub-millisecond precision for performance analysis
- All spans in trace share same anchor (comparable timestamps)
- DST/NTP safe - anchor per trace, traces are short-lived

### Context Creation

```typescript
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
const { task } = createModuleContext({
  moduleMetadata: { gitSha: 'abc123...', filePath: 'src/services/user.ts' },
  tagAttributes: dbAttributes,
});

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
      spanNameId: internString(spanName),
      lineNumber: getCurrentLineNumber(), // Build tool injected
      SpanLogger: compiledTagOps.SpanLogger, // Per-module SpanLogger class with compiled prototype
    };

    // Return lightweight wrapper
    return (...args) => {
      const [originalCtx, ...restArgs] = args;

      // Create new SpanBuffer for this task execution
      const buffer = createSpanBuffer(compiledTagOps.schema, taskModuleContext);

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
function createChildSpan(parentCtx: SpanContext, label: string, childFn: SpanFunction) {
  return async () => {
    // Create child buffer linked to parent
    const childBuffer = createSpanBuffer(parentCtx.buffer.task.module.schema, {
      ...parentCtx.buffer.task,
      spanNameId: internString(label),
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
  label: string,
  options: SpanOptions,
  childFn: SpanFunction
) {
  return async () => {
    // Create child buffer linked to parent
    const childBuffer = createSpanBuffer(parentCtx.buffer.task.module.schema, {
      ...parentCtx.buffer.task,
      spanNameId: internString(label),
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
      ? parentCtx.ff.withContext(options.additionalContext).withBuffer(childBuffer)
      : parentCtx.ff.withBuffer(childBuffer);

    // Environment variables passed through unchanged
    childEnhancedCtx.env = parentCtx.env;

    return await childFn(childEnhancedCtx);
  };
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
          premiumFeatures.track({ outcome: 'enabled' });
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
- **Context extension**: Child can add/override evaluation context via `withContext()`
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
      outcome: result.success ? 'success' : 'failure',
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
      strictValidation.track({ outcome: 'order_validation' });
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
    batchProcessing.track({ outcome: 'batch_started' });
  }

  for (const user of users) {
    // Child span: ADD userId and userPlan to FF evaluation context
    await ctx.span(
      'process-user',
      { additionalContext: { userId: user.id, userPlan: user.plan } },
      async (childCtx) => {
        // childCtx.ff evaluates flags WITH userId and userPlan!
        // The evaluator was created via: parentCtx.ff.withContext({...}).withBuffer(childBuffer)
        const { premiumFeatures, betaAccess } = childCtx.ff;

        if (premiumFeatures) {
          await enablePremiumForUser(user);
          premiumFeatures.track({ outcome: 'enabled' });
        }

        if (betaAccess) {
          await enrollInBeta(user);
          betaAccess.track({ outcome: 'enrolled' });
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
      childFlag.track({ outcome: 'used-in-child' });
      childFlag.track({ outcome: 'used-again' }); // Also logged
    }
  });

  if (featureA) {
    featureA.track({ outcome: 'used-in-parent' }); // Logged to parent span
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

- **Zero-runtime-cost** attribute inclusion through lazy columns
- **Hierarchical inheritance** from parent to child spans
- **Middleware integration** for request-level context setup
- **Clean business logic** that focuses on domain concerns rather than logging boilerplate
- **Readable scope values** via compiled getters
- **Zero indirection** with LazyColumn as direct properties on SpanBuffer

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
