# Span Scope Attributes

## Overview

Span scope attributes allow setting attributes at the span level that automatically propagate to all subsequent log
entries and child spans within that scope. This eliminates repetitive attribute setting, ensures consistency, and
provides zero-runtime-cost attribute inclusion.

Unlike `ctx.tag` which writes to row 0 only, `ctx.scope` sets values that appear on EVERY row when converted to Arrow.

**Key difference from tag**:

- `ctx.tag.userId('123')` → writes to row 0 only
- `ctx.scope.userId = '123'` → appears on ALL rows (0, 1, 2, 3, ...) in Arrow output

## Design Philosophy

**Key Insight**: Many attributes are contextual to an entire span scope (requestId, userId, orderId) and should be set
once at the span level rather than repeated on every log entry. This is particularly valuable in middleware and
top-level request handling.

**Scope Hierarchy**:

```
Request Middleware: scope({ requestId, userId, endpoint, method })
├── Business Logic: scope({ orderId, orderAmount })
│   ├── All log entries include: requestId, userId, endpoint, method, orderId, orderAmount
│   └── Child Span: scope({ validationStep })
│       └── All child entries include: requestId, userId, endpoint, method, orderId, orderAmount, validationStep
```

**Performance Optimization**: Scoped attributes use lazy columns - TypedArrays are NOT allocated until actually needed
(direct write or Arrow conversion), meaning scope-only attributes have zero hot-path allocation overhead.

## Implementation: Lazy Scope with Zero Hot-Path Allocation

The critical insight: scope values should NOT force TypedArray allocation. We use lazy columns that store a scope value
separately from the actual array data.

### LazyColumn Class

```typescript
class LazyColumn<T extends TypedArray | string[]> {
  private _values: T | null = null;
  private _nullBitmap: Uint8Array | null = null;
  private _scopeValue: number | string | boolean | null = null;

  readonly capacity: number;
  readonly ArrayType: TypedArrayConstructor | ArrayConstructor;

  constructor(capacity: number, ArrayType: TypedArrayConstructor | ArrayConstructor) {
    this.capacity = capacity;
    this.ArrayType = ArrayType;
  }

  // For black-box testing
  get isInitialized(): boolean {
    return this._values !== null;
  }

  get hasScopeValue(): boolean {
    return this._scopeValue !== null;
  }

  // Get scope value - for ctx.scope getters to read
  get scopeValue(): number | string | boolean | null {
    return this._scopeValue;
  }

  // Set scope value - NO ALLOCATION
  setScope(value: number | string | boolean): void {
    if (this._scopeValue !== null && this._values === null) {
      // Second scope call with different value - must allocate NOW
      // to preserve the old scope value in rows 0..writeIndex
      this._allocateAndFillWithScope();
    }
    this._scopeValue = value;
  }

  private _allocateAndFillWithScope(): void {
    this._values = new this.ArrayType(this.capacity) as T;
    if (this._scopeValue !== null) {
      (this._values as any).fill(this._scopeValue);
    }
  }

  // Get null bitmap - allocate lazily
  get nullBitmap(): Uint8Array {
    if (!this._nullBitmap) {
      this._nullBitmap = new Uint8Array(Math.ceil(this.capacity / 8));
    }
    return this._nullBitmap;
  }

  // Hot path - allocate values on first direct write
  get values(): T {
    if (!this._values) {
      this._allocateAndFillWithScope();
    }
    return this._values!;
  }

  // Set a value at a specific index (hot path)
  set(index: number, value: number | string | boolean): void {
    this.values[index] = value as any;
    // Set null bitmap bit (value is present)
    const byteIndex = Math.floor(index / 8);
    const bitOffset = index % 8;
    this.nullBitmap[byteIndex] |= 1 << bitOffset;
  }

  // Cold path - Arrow conversion
  getValuesForArrow(writeIndex: number): T | null {
    if (this._values) {
      // Was written to directly - zero-copy subarray
      return (this._values as any).subarray(0, writeIndex) as T;
    }
    if (this._scopeValue !== null) {
      // Never written directly, but has scope value
      // Allocate NOW (cold path) and fill
      const arr = new this.ArrayType(writeIndex) as T;
      (arr as any).fill(this._scopeValue);
      return arr;
    }
    // No data, no scope - null column
    return null;
  }

  // Get null bitmap for Arrow conversion (cold path)
  getNullBitmapForArrow(writeIndex: number): Uint8Array | null {
    if (this._nullBitmap) {
      // Return only the bytes needed for writeIndex rows
      const neededBytes = Math.ceil(writeIndex / 8);
      return this._nullBitmap.subarray(0, neededBytes);
    }
    if (this._scopeValue !== null) {
      // Scope value means all rows have values - create full bitmap
      const neededBytes = Math.ceil(writeIndex / 8);
      const bitmap = new Uint8Array(neededBytes);
      bitmap.fill(0xff); // All bits set = all values present
      // Clear extra bits in last byte
      const extraBits = writeIndex % 8;
      if (extraBits > 0) {
        bitmap[neededBytes - 1] &= (1 << extraBits) - 1;
      }
      return bitmap;
    }
    // No data, no scope - null column (no bitmap needed)
    return null;
  }
}
```

### Allocation Triggers

TypedArray allocation happens:

1. **Direct write** (e.g., `ctx.tag.userId()`) - accesses `.values`, triggers allocation + scope fill
2. **Second scope call** - must preserve old scope value, triggers allocation + fill
3. **Cold path Arrow conversion** - if still lazy but has scope value, allocate and fill

First `ctx.scope.X = value` assignment = just sets `_scopeValue`, NO allocation.

## API Design

### ctx.scope as Compiled Class with Getters/Setters

The `ctx.scope` API is a `new Function()` compiled class (like ctx.tag) that provides:

- **Setters**: `ctx.scope.userId = '123'` → sets scope value on the LazyColumn
- **Getters**: `ctx.scope.userId` → returns the scope value (or undefined)

This allows user code to:

1. Set scope values: `ctx.scope.userId = user.id`
2. Read scope values: `const id = ctx.scope.userId`
3. Check if set: `if (ctx.scope.userId !== undefined)`

```typescript
// Generated at module creation time (cold path)
class GeneratedScope {
  constructor(private buffer: SpanBuffer) {}

  get userId(): string | undefined {
    return this.buffer.attr_userId.scopeValue;
  }

  set userId(value: string) {
    this.buffer.attr_userId.setScope(value);
  }

  get requestId(): string | undefined {
    return this.buffer.attr_requestId.scopeValue;
  }

  set requestId(value: string) {
    this.buffer.attr_requestId.setScope(value);
  }

  // ... generated for each schema attribute
}
```

### ctx.scope vs ctx.tag

```typescript
// ctx.tag - writes to row 0 only, NOT inherited
ctx.tag.userId('123'); // Allocates column, writes to row 0

// ctx.scope - sets scope value, inherited to children, appears on ALL rows
ctx.scope.userId = '123'; // Just sets _scopeValue, NO allocation
ctx.scope.requestId = 'req-1'; // Just sets _scopeValue, NO allocation

// Reading scope values
const currentUserId = ctx.scope.userId; // Returns '123' or undefined
if (ctx.scope.requestId !== undefined) {
  console.log('Request ID is set:', ctx.scope.requestId);
}
```

Both tag and scope support method chaining for setting multiple values:

```typescript
// Method chaining still works for setting
ctx.tag.status(200).method('POST');

// For scope, use separate assignments or Object.assign pattern
ctx.scope.userId = '123';
ctx.scope.requestId = 'req-1';
ctx.scope.orderId = 'ord-456';
```

### Scope Inheritance

Child spans inherit parent's scope values:

```typescript
// Parent span
ctx.scope.userId = 'u1';
ctx.scope.requestId = 'r1';

await ctx.span('child-operation', async (childCtx) => {
  // childCtx has NEW LazyColumns, each with inherited _scopeValue
  // No TypedArrays allocated yet

  // Can read inherited scope values
  console.log(childCtx.scope.userId); // 'u1' (inherited from parent)
  console.log(childCtx.scope.requestId); // 'r1' (inherited from parent)

  childCtx.scope.orderId = 'ord-1'; // Add to scope, still no allocation

  childCtx.log.info('processing'); // Only allocates attr_logMessage
  // attr_userId, attr_requestId, attr_orderId still lazy

  // At Arrow conversion: all three columns materialized with scope values
});
```

### Implementation of Scope Inheritance

```typescript
function createChildSpanBuffer(parentBuffer: SpanBuffer, schema: TagAttributeSchema): SpanBuffer {
  const childBuffer = createEmptySpanBuffer(/* ... */);

  // Copy parent's scope values to child's LazyColumns
  // LazyColumn instances are direct properties on the buffer (no nested Record)
  for (const fieldName of Object.keys(schema.fields)) {
    const columnName = `attr_${fieldName}` as keyof SpanBuffer;
    const parentColumn = parentBuffer[columnName] as LazyColumn<any>;
    const childColumn = childBuffer[columnName] as LazyColumn<any>;

    if (parentColumn.hasScopeValue) {
      // Access internal scope value for inheritance
      childColumn.setScope(parentColumn.scopeValue);
    }
  }

  return childBuffer;
}
```

### SpanBuffer with Direct LazyColumn Properties

Each SpanBuffer has LazyColumn instances as **direct properties** (no nested Record) for zero indirection:

```typescript
interface SpanBuffer {
  // Core columns - always allocated (used for every entry)
  timestamps: Float64Array;
  operations: Uint8Array;

  // Attribute columns - DIRECT PROPERTIES on buffer (no nested Record!)
  // Each LazyColumn handles: values, null bitmap, AND scope value
  // This design provides zero indirection for hot path access
  attr_userId: LazyColumn<Uint32Array>;
  attr_requestId: LazyColumn<Uint32Array>;
  attr_orderId: LazyColumn<Uint32Array>;
  attr_logMessage: LazyColumn<string[]>;
  // ... one direct property per schema attribute

  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer;
  task: TaskContext;

  // Buffer management
  writeIndex: number;
  capacity: number;
  next?: SpanBuffer;

  spanId: number;
  traceId: string;
}

// Access pattern - DIRECT property access, no indirection:
buffer.attr_userId.set(idx, value); // ✅ Direct - zero indirection
buffer.attr_userId.setScope(value); // ✅ Direct - zero indirection
const scope = buffer.attr_userId.scopeValue; // ✅ Direct read

// NOT this pattern (extra indirection):
// buffer.columns.attr_userId.set(idx, value);  // ❌ Nested - one extra lookup
```

## Hot Path Behavior

```typescript
ctx.scope.userId = 'u1'; // _scopeValue = 'u1', NO allocation
ctx.scope.requestId = 'r1'; // _scopeValue = 'r1', NO allocation
ctx.log.info('step 1'); // Allocates attr_logMessage ONLY
ctx.log.info('step 2'); // Writes to attr_logMessage
ctx.log.info('step 3'); // Writes to attr_logMessage

// buffer.attr_userId: _values = null, _scopeValue = 'u1'
// buffer.attr_requestId: _values = null, _scopeValue = 'r1'
// buffer.attr_logMessage: _values = ['step 1', 'step 2', 'step 3', ...]
```

## Cold Path: Arrow Conversion

```typescript
function convertToArrow(buffer: SpanBuffer, schema: TagAttributeSchema): ArrowData {
  const columns: Record<string, TypedArray | string[]> = {};
  const nullBitmaps: Record<string, Uint8Array> = {};

  // Iterate over schema fields - LazyColumns are direct properties on buffer
  for (const fieldName of Object.keys(schema.fields)) {
    const columnName = `attr_${fieldName}`;
    const lazyColumn = buffer[columnName as keyof SpanBuffer] as LazyColumn<any>;

    const arrowData = lazyColumn.getValuesForArrow(buffer.writeIndex);
    if (arrowData !== null) {
      columns[columnName] = arrowData;
      // Get null bitmap from the SAME LazyColumn object
      const nullBitmap = lazyColumn.getNullBitmapForArrow(buffer.writeIndex);
      if (nullBitmap) {
        nullBitmaps[columnName] = nullBitmap;
      }
    }
  }

  // Zero-copy for directly written columns
  // Allocate + fill for scope-only columns
  return buildArrowData(columns, nullBitmaps);
}
```

**Result in Arrow table**:

```
| row | timestamp | logMessage | userId | requestId |
|-----|-----------|------------|--------|-----------|
| 0   | 1000      | null       | u1     | r1        |  <- span-start
| 1   | 2000      | null       | u1     | r1        |  <- span-end
| 2   | 1100      | 'step 1'   | u1     | r1        |  <- event
| 3   | 1200      | 'step 2'   | u1     | r1        |  <- event
| 4   | 1300      | 'step 3'   | u1     | r1        |  <- event
```

Scope values appear on EVERY row - but the TypedArray was only allocated in the cold path.

## Edge Case: Double Scope

Rare case where scope is set twice for same attribute:

```typescript
ctx.scope.userId = 'u1'; // _scopeValue = 'u1', no allocation
ctx.log.info('msg1'); // Row 2, only touches logMessage
ctx.log.info('msg2'); // Row 3, only touches logMessage
ctx.scope.userId = 'u2'; // SECOND scope assignment!
// Must allocate NOW to preserve 'u1' in rows 0-3
// Fill 0..writeIndex with 'u1'
// Set _scopeValue = 'u2' for remainder
ctx.log.info('msg3'); // Row 4, only touches logMessage
```

**Arrow output**:

```
| row | logMessage | userId |
|-----|------------|--------|
| 0   | null       | u1     |  <- rows 0-3 have old value
| 1   | null       | u1     |
| 2   | 'msg1'     | u1     |
| 3   | 'msg2'     | u1     |
| 4   | 'msg3'     | u2     |  <- row 4+ have new value
```

## Usage Patterns

### Middleware Pattern

The most powerful use case is setting up request-level scope in middleware that flows through all business logic:

```typescript
// Express middleware sets up request-level scope
app.use((req, res, next) => {
  const ctx = createRequestContext({
    requestId: req.id,
    userId: req.user?.id,
  });

  // Set scope attributes once at middleware level (using setters)
  ctx.scope.requestId = req.id;
  ctx.scope.userId = req.user?.id;
  ctx.scope.endpoint = req.path;
  ctx.scope.method = req.method;
  ctx.scope.userAgent = req.get('User-Agent');
  ctx.scope.ip = req.ip;

  req.ctx = ctx;
  next();
});

// Business logic focuses on domain concerns
export const createUser = task('create-user', async (ctx, userData) => {
  // Add business-specific scope attributes
  ctx.scope.operation = 'CREATE_USER';
  ctx.scope.email = userData.email; // Masked in background process for privacy (as defined in schema)

  // All subsequent operations include middleware + business scope attributes
  ctx.log.info('Starting user creation');
  // ↑ Includes: requestId, userId, endpoint, method, userAgent, ip, operation, email

  // Feature flag access (automatically includes scope attributes)
  if (ctx.ff.advancedValidation) {
    ctx.log.info('Using advanced validation');
    // ↑ Also includes all scope attributes
  }

  // Child span inherits all scope attributes
  const validation = await ctx.span('validate-email', async (childCtx) => {
    // Child adds validation-specific scope
    childCtx.scope.validationStep = 'email_uniqueness';

    // Can also READ inherited scope values
    console.log('Processing for user:', childCtx.scope.userId);

    childCtx.log.info('Checking email uniqueness');
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
  ctx.scope.orderId = order.id;
  ctx.scope.orderAmount = order.total;
  ctx.scope.customerTier = order.customer.tier;

  ctx.log.info('Order processing started');

  // Payment processing with additional scope
  const payment = await ctx.span('process-payment', async (paymentCtx) => {
    paymentCtx.scope.paymentMethod = order.paymentMethod;
    paymentCtx.scope.paymentProvider = 'stripe';

    paymentCtx.log.info('Initiating payment');
    // ↑ Includes: orderId, orderAmount, customerTier, paymentMethod, paymentProvider

    // Fraud check with even more specific scope
    const fraudCheck = await paymentCtx.span('fraud-check', async (fraudCtx) => {
      fraudCtx.scope.riskScore = calculateRiskScore(order);
      fraudCtx.scope.fraudModel = 'v2.1';

      fraudCtx.log.info('Running fraud detection');
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
  ctx.scope.http_method = 'GET';
  ctx.scope.http_url = url;
  ctx.scope.http_timeout = options.timeout || 30000;

  const startTime = performance.now();
  ctx.log.info('HTTP request initiated');

  try {
    const response = await fetch(url, { method: 'GET', ...options });

    // Add response-specific scope
    ctx.scope.http_status = response.status;
    ctx.scope.http_duration = performance.now() - startTime;

    ctx.log.info('HTTP request completed');
    return ctx.ok(response);
  } catch (error) {
    ctx.scope.http_error = error.message;
    ctx.scope.http_duration = performance.now() - startTime;

    ctx.log.info('HTTP request failed');
    return ctx.err('HTTP_ERROR', error);
  }
});
```

## Performance Characteristics

### Summary Table

| Operation                         | Allocation      | Cost                                      |
| --------------------------------- | --------------- | ----------------------------------------- |
| First `scope.X = value`           | None            | O(1) - just set \_scopeValue              |
| Second `scope.X = value`          | Yes             | O(n) - allocate + fill to writeIndex      |
| `scope.X` (getter)                | None            | O(1) - just return \_scopeValue           |
| `tag.X(value)`                    | Yes             | O(n) - allocate + fill with scope + write |
| `log.info(msg)`                   | Only logMessage | O(1) - single column write                |
| Arrow conversion (scope column)   | Yes if lazy     | O(n) - allocate + fill                    |
| Arrow conversion (written column) | None            | O(1) - zero-copy subarray                 |

### Comparison with Repetitive Tagging

```typescript
// WITHOUT scope - O(m) per log operation
ctx.tag.userId('user123').requestId('req456');
ctx.log.info('Step 1');
ctx.tag.userId('user123').requestId('req456');
ctx.log.info('Step 2');
ctx.tag.userId('user123').requestId('req456');
ctx.log.info('Step 3');
// Total: Multiple allocations, 6 attribute writes + 3 log operations

// WITH scope - O(1) per log operation after initial setup
ctx.scope.userId = 'user123'; // One-time setup, NO allocation
ctx.scope.requestId = 'req456'; // One-time setup, NO allocation
ctx.log.info('Step 1'); // userId, requestId already scoped
ctx.log.info('Step 2'); // userId, requestId already scoped
ctx.log.info('Step 3'); // userId, requestId already scoped
// Total: 0 allocations during hot path, scope columns filled at Arrow conversion
```

### Comparison with ctx.tag

| Aspect                | ctx.tag               | ctx.scope                       |
| --------------------- | --------------------- | ------------------------------- |
| Writes to row 0       | Yes                   | No (unless also using tag)      |
| Appears on all rows   | No (row 0 only)       | Yes (at Arrow conversion)       |
| Allocates on call     | Yes                   | No (first assignment)           |
| Inherited by children | No                    | Yes                             |
| Can read value        | No                    | Yes (via getter)                |
| Use case              | Span-level attributes | Request context flowing through |

## Integration with Existing Systems

### Compatibility with Tag Operations

Scoped attributes work seamlessly with existing tag operations:

```typescript
// Set scope once
ctx.scope.requestId = ctx.requestId;
ctx.scope.userId = order.userId;

// All subsequent operations include scoped attributes automatically
ctx.log.info('Processing order'); // ← Includes requestId, userId
ctx.tag.step('validation'); // ← Includes requestId, userId + step
ctx.log.error('VALIDATION_FAILED', error); // ← Includes requestId, userId + error
ctx.ok({ processed: true }); // ← Includes requestId, userId + result
```

### Feature Flag Integration

Scoped attributes are automatically included in feature flag usage tracking:

```typescript
ctx.scope.userId = order.userId;
ctx.scope.orderId = order.id;

// Feature flag access includes scoped attributes
if (ctx.ff.advancedValidation) {
  // Feature flag usage automatically includes userId, orderId in its trace entry
  ctx.ff.trackUsage('advancedValidation', { action: 'validation_enabled' });
}
```

### Arrow/Parquet Output

Scoped attributes are handled efficiently during background processing:

```typescript
// Arrow conversion recognizes lazy columns (direct properties on buffer)
const createArrowVectors = (spanBuffer) => {
  return {
    // Standard columns - zero-copy
    timestamp: arrow.Float64Vector.from(spanBuffer.timestamps.slice(0, spanBuffer.writeIndex)),

    // Scoped attributes - filled at conversion time (many duplicate values compress well)
    // LazyColumns are DIRECT properties on buffer (no nested .columns)
    user_id: arrow.Utf8Vector.from(spanBuffer.attr_userId.getValuesForArrow(spanBuffer.writeIndex)),
    request_id: arrow.Utf8Vector.from(spanBuffer.attr_requestId.getValuesForArrow(spanBuffer.writeIndex)),
  };
};

// Parquet compression handles repeated scoped values very efficiently
// Example: 1000 log entries with same userId compresses to ~12 bytes in Parquet
```

## Benefits Summary

1. **Zero Hot-Path Allocation**: Scoped attributes don't allocate TypedArrays until Arrow conversion
2. **Consistency**: Impossible to forget important contextual attributes
3. **Clean Code**: Business logic focuses on domain concerns, not logging boilerplate
4. **Hierarchical Context**: Child spans automatically inherit parent context
5. **Memory Efficient**: Lazy columns defer allocation to cold path
6. **Compression Friendly**: Repeated scoped values compress extremely well in Parquet
7. **Type Safe**: Full TypeScript inference for scoped attribute names and types
8. **Middleware Integration**: Perfect fit for request-level context setup
9. **Readable Scope**: Can read scope values via getters (e.g., `ctx.scope.userId`)
10. **Zero Indirection**: LazyColumn properties are direct on SpanBuffer (no nested Record)

This scope-based approach transforms logging from a repetitive, error-prone task into a clean, consistent, and
performant operation that scales naturally with complex request flows.

## Integration Points

This span scope attributes system integrates with:

- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Provides the foundational context
  creation and inheritance mechanisms
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: LazyColumn implementation for deferred
  allocation
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Shows how the `ctx.scope` API is generated at
  runtime
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Demonstrates how libraries can use scoped
  attributes for clean traced operations
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Cold-path materialization of scope columns
