# Context Flow and Task Wrappers

## Overview

The context flow system manages how trace context, feature flags, and environment variables flow through the application. It provides:

1. **Hierarchical context creation** from request → task → child span
2. **Task wrapper pattern** that creates span-aware contexts
3. **Performance optimization** through single allocation and self-reference
4. **Type-safe context enhancement** with automatic span correlation

## Design Philosophy

**Key Insight**: Each level of the call hierarchy needs its own context instance to ensure proper trace correlation. Feature flag and environment variable access must be logged to the correct span buffer.

**Context Hierarchy**:
```
Request Context
├── ff: FeatureFlagEvaluator (logs to request buffer)
├── env: EnvironmentConfig (plain object)
└── task('create-user') creates:
    ├── Span Context (create-user span buffer)
    ├── ff: NEW FeatureFlagEvaluator (logs to create-user span)
    ├── env: SAME EnvironmentConfig (passed through)
    └── ctx.span('validate-user') creates:
        ├── Child Span Context (validate-user span buffer)
        ├── ff: NEW FeatureFlagEvaluator (logs to validate-user span)
        └── env: SAME EnvironmentConfig (passed through)
```

## Request-Level Context Creation

**Purpose**: Create the initial context at request boundaries with user-specific feature flag evaluation.

```typescript
// Create context at request boundary
function createRequestContext(params: { 
  requestId: string, 
  userId?: string 
}): Context {
  return {
    ...params,
    traceId: generateTraceId(),
    
    // Feature flag evaluator with user context
    ff: new FeatureFlagEvaluator(
      featureFlags,
      { userId: params.userId, requestId: params.requestId },
      new DatabaseFlagEvaluator()
    ),
    
    // Environment config (just a plain object)
    env: environmentConfig
  };
}

// Usage in request handler
app.post('/users', async (req, res) => {
  const ctx = createRequestContext({ 
    requestId: req.id,
    userId: req.user?.id 
  });
  
  // Context flows through all operations
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
- **Environment sharing**: Static config shared across all operations
- **Type safety**: Full TypeScript inference for the context object

## Module Context and SpanLogger Generation

**See**: [Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md) for the complete specification of how modules are set up and SpanLogger classes are generated.

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
  tagAttributes: dbAttributes
});

// Generated SpanLogger provides typed methods
export const createUser = task('create-user', async (ctx, userData) => {
  ctx.log.tag.userId(userData.id).operation('INSERT'); // ✅ TypeScript knows these methods
  return ctx.ok(user);
});
```

## Task Wrapper Implementation

**Purpose**: Create span-aware contexts that log feature flag and environment access to the correct span buffer.

```typescript
function createTaskWrapper(
  moduleContext: ModuleContext, 
  compiledTagOps: CompiledTagOperations
) {
  return function task(spanName: string, fn: TaskFunction) {
    // Do expensive work ONCE at task definition time
    const taskModuleContext: TaskModuleContext = {
      module: moduleContext,  // Shared reference
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
        log: new taskModuleContext.SpanLogger(buffer) // SpanLogger class from module context
      };
      
      // Create feature flag evaluator with span context for logging
      enhancedCtx.ff = new FeatureFlagEvaluator(
        originalCtx.ff.schema,
        enhancedCtx, // Self-reference for span logging
        originalCtx.ff.evaluator
      );
      
      // Environment variables are just passed through (no evaluator needed)
      enhancedCtx.env = originalCtx.env;
      
      return fn(enhancedCtx, ...restArgs);
    };
  };
}
```

**Why This Implementation**:
- **Single allocation**: Only one object creation per task execution
- **Self-reference**: Feature flag evaluator references the same context it's attached to
- **Shared evaluator backend**: Reuse the same flag evaluation logic
- **Environment pass-through**: No overhead for environment variable access
- **Unified logging interface**: Single `SpanLogger` class handles all entry types

## SpanLogger Zero-Allocation Design

### Self-Reference Pattern

The `SpanLogger` uses a clever self-reference pattern to achieve zero allocation:

```typescript
class SpanLogger {
  constructor(buffer) {
    this.buffer = buffer;
    this.tag = this; // Key insight: tag points to same instance!
  }
  
  // Fluent methods return this for zero-allocation chaining
  info(message) { 
    this._writeLogEntry('info', message);
    return this;
  }
  
  with(attributes) {
    this._writeAttributes(attributes);
    return this;
  }
  
  // Tag methods compiled onto prototype at task creation time
  userId(value) { this.buffer.writeUserId(value); return this; }
  httpStatus(value) { this.buffer.writeHttpStatus(value); return this; }
}
```

### Design Benefits

- **Zero allocation**: Only the SpanLogger instance itself is allocated
- **Prototype compilation**: Tag methods added to prototype at task creation time, not runtime
- **Fluent chaining**: All methods return `this` for seamless chaining
- **API clarity**: `ctx.log.tag.userId()` vs `ctx.log.info()` provides clear separation

## Library Integration Pattern

**See**: [Library Integration Pattern](./01e_library_integration_pattern.md) for the complete specification of how third-party libraries can provide traced operations with clean APIs while avoiding attribute name conflicts.

The core task wrapper pattern described in this document provides the foundation that libraries build upon to create their own traced operations.

## Child Span Creation

**Purpose**: Create nested spans with their own context and buffer while maintaining parent-child relationships.

```typescript
function createChildSpan(
  parentBuffer: SpanBuffer, 
  label: string, 
  childFn: SpanFunction,
  attributes?: Record<string, any>,
  message?: string
) {
  return async (childCtx) => {
    // Create child buffer linked to parent
    const childBuffer = createSpanBuffer(parentBuffer.task.module.schema, {
      ...parentBuffer.task,
      spanNameId: internString(label),
      lineNumber: getCurrentLineNumber(),
    });
    
    // Link parent-child relationship
    childBuffer.parent = parentBuffer;
    parentBuffer.children.push(childBuffer);
    
    // Create child context (same pattern as task wrapper)
    const childEnhancedCtx = {
      ...childCtx,
      log: new parentBuffer.task.SpanLogger(childBuffer) // SpanLogger class from module context
    };
    
    // Child gets NEW feature flag evaluator that logs to child buffer
    childEnhancedCtx.ff = new FeatureFlagEvaluator(
      childCtx.ff.schema,
      childEnhancedCtx, // Logs to child buffer
      childCtx.ff.evaluator
    );
    
    // Environment variables passed through unchanged
    childEnhancedCtx.env = childCtx.env;
    
    return await childFn(childEnhancedCtx);
  };
}
```

**Why This Design**:
- **Tree structure**: Child buffers linked to parents for efficient traversal
- **Context isolation**: Each span gets its own context and buffer
- **Feature flag correlation**: Flag access logged to the correct span
- **Environment sharing**: Static config shared across all spans

## Usage Examples

### Basic Task Usage

```typescript
export const createUser = task('create-user', async ({ log, ff, env }, userData: UserData) => {
  // Feature flag access (automatically logged to this span)
  if (ff.advancedValidation) {
    const result = await performAdvancedValidation(userData);
    ff.trackUsage('advancedValidation', {
      action: 'validation_performed',
      outcome: result.success ? 'success' : 'failure'
    });
  }
  
  // Environment access (just property access, no logging)
  const region = env.awsRegion;
  const maxConnections = env.maxConnections;
  
  // Tag operations (logged to this span's buffer)
  log.tag.requestId(ctx.requestId);
  log.tag.userId(userData.id);
  log.tag.operation('INSERT');
  
  log.info("Creating new user")
    .with({ userId: userData.id, email: userData.email });
  
  const user = await db.createUser(userData);
  return log.ok(user)
    .with({ userId: user.id, operation: 'CREATE' })
    .message("User created successfully");
});
```

### Nested Span Usage

```typescript
export const processOrder = task('process-order', async (ctx, order: Order) => {
  ctx.tag.orderId(order.id);
  ctx.tag.amount(order.total);
  
  // Child span with its own context and initial attributes
  const validation = await ctx.span('validate-order', async (childCtx) => {
    childCtx.tag.itemCount(order.items.length);
    
    // Feature flag access logged to child span
    if (childCtx.ff.strictValidation) {
      childCtx.ff.trackUsage('strictValidation', { action: 'order_validation' });
    }
    
    if (order.items.length === 0) {
      return childCtx.err('EMPTY_ORDER')
        .with({ orderId: order.id })
        .message("Order must contain at least one item");
    }
    
    return childCtx.ok({ valid: true });
  });
  
  if (!validation.success) {
    return ctx.err('VALIDATION_FAILED')
      .with({ orderId: order.id, validationStep: 'order_validation' })
      .message("Order validation failed");
  }
  
  // Another child span with initial context
  const payment = await ctx.span('process-payment')
    .message("Processing payment for order")
    .with({ 
      paymentMethod: order.paymentMethod,
      amount: order.total 
    })
    .run(async (childCtx) => {
      childCtx.tag.paymentMethod(order.paymentMethod);
      
      // Environment access in child span
      const paymentProvider = childCtx.env.paymentProvider;
      
      return await processPayment(order, paymentProvider);
    });
  
  return ctx.ok({ orderId: order.id, paymentId: payment.data.id })
    .with({ orderId: order.id, paymentMethod: order.paymentMethod, amount: order.total })
    .message("Order processed successfully");
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

This context flow design ensures that every piece of configuration access is properly attributed to the correct span while maintaining excellent performance characteristics.

## Integration Points

This context flow system integrates with:

- **[Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md)**: Provides the module setup and SpanLogger class generation that task wrappers use
- **[Span Scope Attributes](./01i_span_scope_attributes.md)**: Provides span-level attribute scoping that works with the context hierarchy
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Details how the `ctx.tag`, `ctx.info`, etc. APIs are generated at runtime
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Shows how libraries create their own task wrappers using this foundation
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Defines the SpanBuffer structure that contexts write to 

## Span Scope Attributes

**See**: [Span Scope Attributes](./01i_span_scope_attributes.md) for the complete specification of how to set span-level attributes that automatically propagate to all subsequent log entries and child spans.

The scope attributes system provides:
- **Zero-runtime-cost** attribute inclusion through buffer pre-filling
- **Hierarchical inheritance** from parent to child spans  
- **Middleware integration** for request-level context setup
- **Clean business logic** that focuses on domain concerns rather than logging boilerplate

**Quick Example**:
```typescript
// Set scope once at middleware level
ctx.log.scope({ requestId: req.id, userId: req.user?.id });

// All subsequent operations automatically include these attributes
ctx.log.info("Processing order");        // ← Includes requestId, userId
ctx.log.tag.step('validation');          // ← Includes requestId, userId + step

// Child spans inherit and can extend scope
await ctx.span('payment', async (childCtx) => {
  childCtx.log.scope({ paymentMethod: 'stripe' });
  childCtx.log.info("Processing payment"); // ← Includes requestId, userId, paymentMethod
});
```
