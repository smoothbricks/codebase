# Span Scope Attributes

## Overview

Span scope attributes allow setting attributes at the span level that automatically propagate to all subsequent log entries and child spans within that scope. This eliminates repetitive attribute setting, ensures consistency, and provides zero-runtime-cost attribute inclusion.

## Design Philosophy

**Key Insight**: Many attributes are contextual to an entire span scope (requestId, userId, orderId) and should be set once at the span level rather than repeated on every log entry. This is particularly valuable in middleware and top-level request handling.

**Scope Hierarchy**:
```
Request Middleware: scope({ requestId, userId, endpoint, method })
├── Business Logic: scope({ orderId, orderAmount })
├── ├── All log entries include: requestId, userId, endpoint, method, orderId, orderAmount
├── └── Child Span: scope({ validationStep })
├──     └── All child entries include: requestId, userId, endpoint, method, orderId, orderAmount, validationStep
```

**Performance Optimization**: Scoped attributes are pre-filled into the remaining buffer capacity, meaning subsequent log operations have zero additional overhead for including these attributes.

## SpanLogger Scope API

### Core Implementation

```typescript
class SpanLogger {
  constructor(buffer, scopedAttributes = {}) {
    this.buffer = buffer;
    this.scoped = scopedAttributes; // Attributes scoped to this span
    this.tag = this;
  }
  
  // Set scope-level attributes that apply to all subsequent operations
  scope(attributes) {
    // Merge with existing scoped attributes
    this.scoped = { ...this.scoped, ...attributes };
    
    // Pre-fill remaining buffer capacity with these attributes
    this._prefillRemainingCapacity(attributes);
    
    return this;
  }
  
  // Internal method to pre-fill buffer arrays with scoped attributes
  _prefillRemainingCapacity(newScoped) {
    const startIndex = this.buffer.writeIndex;
    const capacity = this.buffer.capacity;
    
    // Pre-fill the remaining buffer space with scoped attributes
    for (const [attrName, value] of Object.entries(newScoped)) {
      const columnName = `attr_${attrName}`;
      const column = this.buffer[columnName];
      
      if (column) {
        const processedValue = this._processValue(value, attrName);
        const bitPos = this._getAttributeBitPosition(attrName);
        
        // Fill from current write position to end of buffer
        for (let i = startIndex; i < capacity; i++) {
          column[i] = processedValue;
          // Set null bitmap bit to indicate this attribute has a value
          this.buffer.nullBitmap[i] |= (1 << bitPos);
        }
      }
    }
  }
  
  // All log operations benefit from pre-filled scoped attributes
  info(message) {
    const index = this.buffer.writeIndex++;
    this.buffer.timestamps[index] = performance.now();
    this.buffer.operations[index] = OPERATION_INFO;
    
    // Scoped attributes are already pre-filled!
    // Just write the message-specific data
    this._writeMessage(index, message);
    
    return this;
  }
  
  // Tag operations can add to scoped attributes
  tag = {
    userId: (value) => {
      const index = this.buffer.writeIndex++;
      this.buffer.timestamps[index] = performance.now();
      this.buffer.operations[index] = OPERATION_TAG;
      
      // Scoped attributes already present, just add this specific tag
      this.buffer.attr_userId[index] = this._processValue(value, 'userId');
      this.buffer.nullBitmap[index] |= (1 << this._getAttributeBitPosition('userId'));
      
      return this;
    }
    // ... other tag methods
  };
}
```

### Scope Inheritance Mechanisms

#### Task Wrapper Integration

```typescript
function createTaskWrapper(moduleContext, compiledTagOps) {
  return function task(spanName, fn) {
    return (...args) => {
      const [originalCtx, ...restArgs] = args;
      
      // Create buffer (existing code)
      const buffer = createSpanBuffer(compiledTagOps.schema, taskModuleContext);
      
      // Inherit scoped attributes from parent context
      const inheritedScoped = originalCtx.log?.scoped || {};
      
      // Create enhanced context with inherited scoped attributes
      const enhancedCtx = {
        ...originalCtx,
        log: new taskModuleContext.SpanLogger(buffer, inheritedScoped)
      };
      
      // Pre-fill buffer with inherited scoped attributes
      if (Object.keys(inheritedScoped).length > 0) {
        enhancedCtx.log._prefillRemainingCapacity(inheritedScoped);
      }
      
      // Rest of existing implementation...
      return fn(enhancedCtx, ...restArgs);
    };
  };
}
```

#### Child Span Inheritance

```typescript
// In TaskContext.span method
async function createChildSpan(parentCtx, spanName, childFn) {
  // Create child buffer (existing code)
  const childBuffer = createSpanBuffer(/*...*/);
  
  // Child inherits parent's scoped attributes
  const childCtx = {
    ...parentCtx,
    log: new SpanLogger(childBuffer, parentCtx.log.scoped) // Inherit scoped attributes
  };
  
  // Pre-fill child buffer with inherited scoped attributes
  if (Object.keys(parentCtx.log.scoped).length > 0) {
    childCtx.log._prefillRemainingCapacity(parentCtx.log.scoped);
  }
  
  return childFn(childCtx);
}
```

#### Buffer Overflow Handling

```typescript
function createNextBuffer(buffer) {
  const nextBuffer = createEmptySpanBuffer(/*...*/);
  
  // Carry forward scoped attributes to overflow buffer
  if (buffer.log && buffer.log.scoped) {
    const scopedCount = Object.keys(buffer.log.scoped).length;
    if (scopedCount > 0) {
      // Create new SpanLogger with inherited scoped attributes
      nextBuffer.log = new SpanLogger(nextBuffer, buffer.log.scoped);
      // Pre-fill entire new buffer with scoped attributes
      nextBuffer.log._prefillRemainingCapacity(buffer.log.scoped);
    }
  }
  
  return nextBuffer;
}
```

## Usage Patterns

### Middleware Pattern

The most powerful use case is setting up request-level scope in middleware that flows through all business logic:

```typescript
// Express middleware sets up request-level scope
app.use((req, res, next) => {
  const ctx = createRequestContext({ 
    requestId: req.id, 
    userId: req.user?.id 
  });
  
  // Set scope attributes once at middleware level
  ctx.log.scope({
    requestId: req.id,
    userId: req.user?.id,
    endpoint: req.path,
    method: req.method,
    userAgent: req.get('User-Agent'),
    ip: req.ip
  });
  
  req.ctx = ctx;
  next();
});

// Business logic focuses on domain concerns
export const createUser = task('create-user', async (ctx, userData) => {
  // Add business-specific scope attributes
  ctx.log.scope({
    operation: 'CREATE_USER',
    email: userData.email // Masked in the background process for privacy (as defined in schema)
  });
  
  // All subsequent operations include middleware + business scope attributes
  ctx.log.info("Starting user creation");
  // ↑ Includes: requestId, userId, endpoint, method, userAgent, ip, operation, email
  
  // Feature flag access (automatically includes scope attributes)
  if (ctx.ff.advancedValidation) {
    ctx.log.info("Using advanced validation");
    // ↑ Also includes all scope attributes
  }
  
  // Child span inherits all scope attributes
  const validation = await ctx.span('validate-email', async (childCtx) => {
    // Child adds validation-specific scope
    childCtx.log.scope({
      validationStep: 'email_uniqueness'
    });
    
    childCtx.log.info("Checking email uniqueness");
    // ↑ Includes: requestId, userId, endpoint, method, userAgent, ip, operation, email, validationStep
    
    if (existingUser) {
      return childCtx.err('EMAIL_EXISTS');
      // ↑ Error also includes all scope attributes
    }
    
    return childCtx.ok({ unique: true });
  });
  
  if (!validation.success) {
    return ctx.err('VALIDATION_FAILED', validation.error);
  }
  
  const user = await db.createUser(userData);
  return ctx.ok(user);
});
```

### Multi-Level Scoping

Demonstrates how scoped attributes layer naturally in complex business flows:

```typescript
export const processOrder = task('process-order', async (ctx, order) => {
  // Order-level scope
  ctx.log.scope({
    orderId: order.id,
    orderAmount: order.total,
    customerTier: order.customer.tier
  });
  
  ctx.log.info("Order processing started");
  
  // Payment processing with additional scope
  const payment = await ctx.span('process-payment', async (paymentCtx) => {
    paymentCtx.log.scope({
      paymentMethod: order.paymentMethod,
      paymentProvider: 'stripe'
    });
    
    paymentCtx.log.info("Initiating payment");
    // ↑ Includes: orderId, orderAmount, customerTier, paymentMethod, paymentProvider
    
    // Fraud check with even more specific scope
    const fraudCheck = await paymentCtx.span('fraud-check', async (fraudCtx) => {
      fraudCtx.log.scope({
        riskScore: calculateRiskScore(order),
        fraudModel: 'v2.1'
      });
      
      fraudCtx.log.info("Running fraud detection");
      // ↑ Includes all parent scope + riskScore, fraudModel
      
      return fraudCtx.ok({ riskLevel: 'low' });
    });
    
    return paymentCtx.ok({ charged: true });
  });
  
  return ctx.ok({ processed: true });
});
```

### Library Integration

Third-party libraries can use scoped attributes to provide clean APIs while ensuring traceability:

```typescript
// HTTP library sets up request-specific scope
export const get = task('http-get', async (ctx, url, options = {}) => {
  // Scope all HTTP operations with request metadata
  ctx.log.scope({
    http_method: 'GET',
    http_url: url,
    http_timeout: options.timeout || 30000
  });
  
  const startTime = performance.now();
  ctx.log.info("HTTP request initiated");
  
  try {
    const response = await fetch(url, { method: 'GET', ...options });
    
    // Add response-specific scope
    ctx.log.scope({
      http_status: response.status,
      http_duration: performance.now() - startTime
    });
    
    ctx.log.info("HTTP request completed");
    return ctx.ok(response);
    
  } catch (error) {
    ctx.log.scope({
      http_error: error.message,
      http_duration: performance.now() - startTime
    });
    
    ctx.log.info("HTTP request failed");
    return ctx.err('HTTP_ERROR', error);
  }
});
```

## Performance Characteristics

### Memory Pre-filling Strategy

The key performance optimization is pre-filling buffer arrays when scoped attributes are set:

```typescript
// When ctx.log.scope({ userId: "user123", requestId: "req456" }) is called:

// 1. Current buffer state:
//    writeIndex = 5, capacity = 64
//    attr_userId = [val1, val2, val3, val4, val5, 0, 0, 0, ...] (59 zeros)
//    attr_requestId = [val1, val2, val3, val4, val5, 0, 0, 0, ...] (59 zeros)

// 2. After scope() call:
//    attr_userId = [val1, val2, val3, val4, val5, "user123", "user123", ...] (59 copies of "user123")
//    attr_requestId = [val1, val2, val3, val4, val5, "req456", "req456", ...] (59 copies of "req456")

// 3. Subsequent log operations just increment writeIndex:
//    ctx.log.info("message") → writeIndex = 6, userId and requestId are already there!
```

### Runtime Overhead Analysis

- **Scope Setting**: O(n × m) where n = remaining capacity, m = number of scoped attributes
- **Log Operations**: O(1) - no additional work for scoped attributes
- **Memory Usage**: No additional allocation, uses existing buffer capacity
- **CPU Cache**: Pre-filling improves cache locality for subsequent operations

### Comparison with Repetitive Tagging

```typescript
// WITHOUT scope (current approach) - O(m) per log operation
ctx.log.tag.userId("user123").requestId("req456").info("Step 1");
ctx.log.tag.userId("user123").requestId("req456").info("Step 2");
ctx.log.tag.userId("user123").requestId("req456").info("Step 3");
// Total: 6 attribute writes + 3 log operations = 9 operations

// WITH scope - O(1) per log operation after initial setup
ctx.log.scope({ userId: "user123", requestId: "req456" }); // One-time setup
ctx.log.info("Step 1"); // userId, requestId already present
ctx.log.info("Step 2"); // userId, requestId already present  
ctx.log.info("Step 3"); // userId, requestId already present
// Total: 1 setup + 3 log operations = 4 operations (56% reduction)
```

## Integration with Existing Systems

### Compatibility with Tag Operations

Scoped attributes work seamlessly with existing tag operations:

```typescript
// Set scope once
ctx.log.scope({ requestId: ctx.requestId, userId: order.userId });

// All subsequent operations include scoped attributes automatically
ctx.log.info("Processing order");              // ← Includes requestId, userId
ctx.log.tag.step('validation');                // ← Includes requestId, userId + step
ctx.log.err('VALIDATION_FAILED', error);       // ← Includes requestId, userId + error
ctx.log.ok({ processed: true });               // ← Includes requestId, userId + result
```

### Feature Flag Integration

Scoped attributes are automatically included in feature flag usage tracking:

```typescript
ctx.log.scope({ userId: order.userId, orderId: order.id });

// Feature flag access includes scoped attributes
if (ctx.ff.advancedValidation) {
  // Feature flag usage automatically includes userId, orderId in its trace entry
  ctx.ff.trackUsage('advancedValidation', { action: 'validation_enabled' });
}
```

### Arrow/Parquet Output

Scoped attributes are handled efficiently during background processing:

```typescript
// Arrow conversion recognizes pre-filled values
const createArrowVectors = (spanBuffer) => {
  return {
    // Standard columns
    timestamp: arrow.Float64Vector.from(spanBuffer.timestamps.slice(0, spanBuffer.writeIndex)),
    
    // Scoped attributes are efficiently converted (many duplicate values compress well)
    user_id: arrow.Utf8Vector.from(spanBuffer.attr_userId.slice(0, spanBuffer.writeIndex)),
    request_id: arrow.Utf8Vector.from(spanBuffer.attr_requestId.slice(0, spanBuffer.writeIndex)),
  };
};

// Parquet compression handles repeated scoped values very efficiently
// Example: 1000 log entries with same userId compresses to ~12 bytes in Parquet
```

## Benefits Summary

1. **Zero Runtime Overhead**: Scoped attributes are pre-filled once, not written repeatedly
2. **Consistency**: Impossible to forget important contextual attributes
3. **Clean Code**: Business logic focuses on domain concerns, not logging boilerplate
4. **Hierarchical Context**: Child spans automatically inherit parent context
5. **Memory Efficient**: Pre-filling leverages existing buffer capacity
6. **Compression Friendly**: Repeated scoped values compress extremely well in Parquet
7. **Type Safe**: Full TypeScript inference for scoped attribute names and types
8. **Middleware Integration**: Perfect fit for request-level context setup

This scope-based approach transforms logging from a repetitive, error-prone task into a clean, consistent, and performant operation that scales naturally with complex request flows.

## Integration Points

This span scope attributes system integrates with:

- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Provides the foundational context creation and inheritance mechanisms
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Defines the SpanBuffer structure that gets pre-filled with scoped attributes
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Shows how the `ctx.log.scope()` API is generated at runtime
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Demonstrates how libraries can use scoped attributes for clean traced operations
``` 
