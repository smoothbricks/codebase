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

## Implementation: Separate Scope and Column Storage

The critical insight: scope values should be SEPARATE from column storage. We use a generated Scope class to hold
values, and pre-fill child buffer columns via `TypedArray.fill()` for SIMD optimization.

### Scope Class (Generated at Module Creation)

The Scope class is generated via `new Function()` and contains ONLY schema attributes (NOT system columns):

```typescript
// Generated once at module creation time (cold path)
function generateScopeClass(schema: TagAttributeSchema): typeof GeneratedScope {
  const fields = Object.keys(schema.fields)
    .map(
      (key) => `
    _${key} = undefined;
    get ${key}() { return this._${key}; }
    set ${key}(value) { this._${key} = value; }
  `
    )
    .join('\n');

  const getScopeValuesBody = Object.keys(schema.fields)
    .map((k) => `${k}: this._${k}`)
    .join(', ');

  const classCode = `
    return class GeneratedScope {
      ${fields}
      _getScopeValues() {
        return { ${getScopeValuesBody} };
      }
    };
  `;

  return new Function(classCode)();
}

// Example generated class for { userId: utf8, requestId: utf8, orderId: utf8 }
class GeneratedScope {
  _userId = undefined;
  get userId() {
    return this._userId;
  }
  set userId(value) {
    this._userId = value;
  }

  _requestId = undefined;
  get requestId() {
    return this._requestId;
  }
  set requestId(value) {
    this._requestId = value;
  }

  _orderId = undefined;
  get orderId() {
    return this._orderId;
  }
  set orderId(value) {
    this._orderId = value;
  }

  _getScopeValues() {
    return {
      userId: this._userId,
      requestId: this._requestId,
      orderId: this._orderId,
    };
  }
}
```

### Column Storage with Lazy Getters (Generated via `new Function()`)

The actual implementation does NOT use a LazyColumn class. Instead, SpanBuffer is generated via `new Function()` with:

1. **Direct properties** for each column: `attr_X_nulls` and `attr_X_values`
2. **Lazy getters** that allocate shared ArrayBuffer on first access
3. **Symbol-based storage** to ensure each buffer instance has its own arrays

```typescript
// Generated SpanBuffer class (via new Function() at module creation time)
// Each attribute gets TWO getters sharing ONE ArrayBuffer

// Example for userId attribute:
// Symbol for private storage (per-instance)
const attr_userId_sym = Symbol('attr_userId');

// Allocator function (called by getters on first access)
function allocate_attr_userId(self) {
  if (self[attr_userId_sym]) return self[attr_userId_sym];

  const capacity = self._alignedCapacity;
  const nullBitmapSize = Math.ceil(capacity / 8);

  // Align null bitmap end to cache line for optimal values array placement
  const alignedNullOffset = Math.ceil(nullBitmapSize / 4) * 4; // For Uint32Array

  const totalSize = alignedNullOffset + capacity * 4; // Uint32Array = 4 bytes/element

  // ONE ArrayBuffer for both nulls and values
  const buffer = new ArrayBuffer(totalSize);

  const storage = {
    buffer: buffer,
    nulls: new Uint8Array(buffer, 0, nullBitmapSize),
    values: new Uint32Array(buffer, alignedNullOffset, capacity),
  };

  self[attr_userId_sym] = storage;
  return storage;
}

// Generated class:
class GeneratedColumnBuffer {
  constructor(requestedCapacity) {
    const alignedCapacity = getCacheAlignedCapacity(requestedCapacity);
    this._alignedCapacity = alignedCapacity;
    this.timestamps = new BigInt64Array(alignedCapacity);
    this.operations = new Uint8Array(alignedCapacity);
    this.writeIndex = 0;
    this.capacity = requestedCapacity;
  }

  // Lazy getters for userId column (allocated on first access)
  get attr_userId_nulls() {
    return allocate_attr_userId(this).nulls;
  }
  get attr_userId_values() {
    return allocate_attr_userId(this).values;
  }
  get attr_userId() {
    return allocate_attr_userId(this).values;
  } // Shorthand

  // ... same pattern for all other schema attributes

  // Helper to check if column is allocated (without triggering allocation)
  getColumnIfAllocated(columnName) {
    const sym = columnSymbols[columnName];
    return sym ? this[sym]?.values : undefined;
  }

  getNullsIfAllocated(columnName) {
    const sym = columnSymbols[columnName];
    return sym ? this[sym]?.nulls : undefined;
  }
}
```

**Key Benefits of This Design**:

1. **Shared ArrayBuffer**: `attr_X_nulls` and `attr_X_values` use the SAME underlying ArrayBuffer, partitioned with
   cache-aligned offsets
2. **Zero Indirection**: Direct property access (`buffer.attr_userId_values[i]`) with no extra lookups
3. **Lazy Allocation**: Unused columns never allocate memory (hot path optimization)
4. **Per-Instance Storage**: Symbol-keyed storage ensures each buffer has its own arrays (no shared closure bugs)
5. **V8 Optimization**: `new Function()` generates concrete classes that V8 can optimize with hidden classes

### Allocation Triggers

TypedArray allocation happens when lazy getters are first accessed:

1. **Direct write** (e.g., `ctx.tag.userId('value')`) - accesses `buffer.attr_userId_values`, triggers lazy getter →
   allocates shared ArrayBuffer
2. **Scope pre-fill** (child span creation) - accesses column getters to fill with parent scope values → allocates if
   not yet allocated
3. **Null bitmap write** - accessing `buffer.attr_userId_nulls` triggers same allocator as values (both use shared
   ArrayBuffer)

First `ctx.scope.X = value` assignment = just sets property on Scope object, NO buffer allocation.

**Important**: The lazy getters ensure allocation happens at most ONCE per column per buffer instance. Subsequent
accesses return the already-allocated arrays.

## API Design

### ctx.scope as Compiled Class with Getters/Setters

The `ctx.scope` API is a `new Function()` compiled class that provides:

- **Setters**: `ctx.scope.userId = '123'` → sets scope value on the Scope instance
- **Getters**: `ctx.scope.userId` → returns the scope value (or undefined)

This allows user code to:

1. Set scope values: `ctx.scope.userId = user.id`
2. Read scope values: `const id = ctx.scope.userId`
3. Check if set: `if (ctx.scope.userId !== undefined)`

The Scope class is separate from the buffer - it ONLY stores schema attributes:

```typescript
// Generated at module creation time (cold path) - see implementation above
// Each TraceContext has its own Scope instance:
class TraceContext {
  scope: GeneratedScope;
  buffer: SpanBuffer;
  tag: GeneratedTag;
  log: GeneratedLog;
  // ...

  constructor(buffer: SpanBuffer, scopeClass: typeof GeneratedScope) {
    this.buffer = buffer;
    this.scope = new scopeClass(); // New Scope instance per context
    // ...
  }
}
```

### ctx.scope vs ctx.tag

```typescript
// ctx.tag - writes to row 0 only, NOT inherited
ctx.tag.userId('123'); // Allocates column, writes to row 0

// ctx.scope - sets scope value, inherited to children, pre-fills child columns
ctx.scope.userId = '123'; // Just sets Scope property, NO allocation
ctx.scope.requestId = 'req-1'; // Just sets Scope property, NO allocation

// Reading scope values
const currentUserId = ctx.scope.userId; // Returns '123' or undefined
if (ctx.scope.requestId !== undefined) {
  console.log('Request ID is set:', ctx.scope.requestId);
}
```

Both tag and scope use separate assignments for multiple values:

```typescript
// tag - method chaining for fluent API
ctx.tag.status(200).method('POST');

// scope - property assignments
ctx.scope.userId = '123';
ctx.scope.requestId = 'req-1';
ctx.scope.orderId = 'ord-456';
```

### Scope Inheritance with Pre-filling

Child spans inherit parent's scope values and PRE-FILL buffer columns for SIMD optimization:

```typescript
// Parent span
ctx.scope.userId = 'u1';
ctx.scope.requestId = 'r1';

await ctx.span('child-operation', async (childCtx) => {
  // childCtx has:
  // 1. NEW Scope instance with copied values from parent
  // 2. NEW buffer columns that can be PRE-FILLED with scope values

  // Can read inherited scope values
  console.log(childCtx.scope.userId); // 'u1' (inherited from parent)
  console.log(childCtx.scope.requestId); // 'r1' (inherited from parent)

  childCtx.scope.orderId = 'ord-1'; // Add to scope - no column allocation yet

  childCtx.log.info('processing'); // Allocates attr_logMessage column
  // attr_userId, attr_requestId columns pre-filled from parent scope
  // attr_orderId not pre-filled yet (set after child creation)

  // At Arrow conversion: userId and requestId use pre-filled arrays (zero-copy)
  // orderId needs to be filled on-demand
});
```

### Implementation of Scope Inheritance with Pre-filling

Key insight: Pre-fill child buffer columns when creating child span, enabling SIMD optimization:

```typescript
function createChildSpanContext(
  parentCtx: TraceContext,
  spanName: string,
  scopeClass: typeof GeneratedScope,
): TraceContext {
  const childBuffer = createEmptySpanBuffer(/* ... */);

  // Create child scope and copy parent's scope values
  const childScope = new scopeClass();
  const parentScopeValues = parentCtx.scope._getScopeValues();

  // Pre-fill child buffer columns with parent scope values (SIMD optimized)
  // This happens BEFORE user code runs, so columns are ready for writing
  for (const [fieldName, value] of Object.entries(parentScopeValues)) {
    if (value !== undefined) {
      // Copy to child scope
      childScope[fieldName] = value;

      // PRE-FILL child buffer column with TypedArray.fill() (SIMD optimized)
      // Accessing the lazy getter triggers allocation of the shared ArrayBuffer
      const valuesArray = childBuffer[`attr_${fieldName}_values`];
      const nullsArray = childBuffer[`attr_${fieldName}_nulls`];

      // Pre-fill values from row 0 to buffer capacity
      valuesArray.fill(value);

      // Mark all rows as valid in null bitmap (0xFF = all bits set)
      const fullBytes = Math.floor(childBuffer.capacity / 8);
      nullsArray.fill(0xff, 0, fullBytes);
      // Handle remaining bits in last byte
      const remainingBits = childBuffer.capacity % 8;
      if (remainingBits > 0) {
        nullsArray[fullBytes] = (1 << remainingBits) - 1;
      }
    }
  }

  return new TraceContext(childBuffer, childScope, scopeClass);
}
  }

  return new TraceContext(childBuffer, childScope, scopeClass);
}
```

**Pre-filling Benefits**:

1. **SIMD Optimization**: `TypedArray.fill()` uses highly optimized SIMD instructions
2. **Zero-Copy on Arrow Conversion**: Pre-filled columns are already materialized, no cold-path allocation
3. **Cache-Friendly**: Contiguous memory writes improve cache utilization
4. **Predictable Performance**: No deferred allocation surprises during Arrow conversion

## Hot Path Behavior

### Parent Span (No Pre-filling)

```typescript
ctx.scope.userId = 'u1'; // Sets Scope property, NO buffer allocation
ctx.scope.requestId = 'r1'; // Sets Scope property, NO buffer allocation
ctx.log.info('step 1'); // Triggers attr_logMessage lazy getter → allocates ArrayBuffer
// Scope values applied at write time (userId, requestId columns allocated too)
ctx.log.info('step 2'); // Writes to already-allocated attr_logMessage
ctx.log.info('step 3'); // Writes to already-allocated attr_logMessage

// Scope: { userId: 'u1', requestId: 'r1' }
// buffer[attr_userId_sym]: allocated during first log.info (scope applied)
// buffer[attr_requestId_sym]: allocated during first log.info (scope applied)
// buffer[attr_logMessage_sym]: allocated during first log.info
```

### Child Span (Pre-filled from Parent)

```typescript
await ctx.span('child', async (childCtx) => {
  // Child span creation:
  // 1. New Scope instance with parent values copied
  // 2. SpanLogger.scope() called to pre-fill buffer with parent scope values
  //    - Accesses attr_userId_values getter → triggers allocation
  //    - Uses TypedArray.fill() to pre-fill entire capacity
  //    - Same for attr_requestId

  // Child buffer state after creation:
  // childCtx.buffer[attr_userId_sym]: allocated and pre-filled with 'u1'
  // childCtx.buffer[attr_requestId_sym]: allocated and pre-filled with 'r1'

  childCtx.scope.orderId = 'ord-1'; // Sets Scope property only, NO allocation yet

  childCtx.log.info('step 1'); // Writes to pre-filled userId/requestId columns
  // Allocates attr_logMessage
  // orderId column allocated now (scope applied at write)
  childCtx.log.info('step 2'); // Writes to all allocated columns

  // Final state:
  // Scope: { userId: 'u1', requestId: 'r1', orderId: 'ord-1' }
  // childCtx.buffer[attr_userId_sym]: pre-filled ArrayBuffer (SIMD optimized)
  // childCtx.buffer[attr_requestId_sym]: pre-filled ArrayBuffer (SIMD optimized)
  // childCtx.buffer[attr_orderId_sym]: allocated on first log.info (scope applied)
  // childCtx.buffer[attr_logMessage_sym]: allocated on first log.info
});
```

## Cold Path: Arrow Conversion

```typescript
function convertToArrow(buffer: SpanBuffer, scope: GeneratedScope, schema: TagAttributeSchema): ArrowData {
  const columns: Record<string, TypedArray> = {};
  const nullBitmaps: Record<string, Uint8Array> = {};

  // Iterate over schema fields
  const scopeValues = scope._getScopeValues();

  for (const fieldName of Object.keys(schema.fields)) {
    const columnName = `attr_${fieldName}`;

    // Check if column was allocated (without triggering allocation)
    const values = buffer.getColumnIfAllocated(columnName);
    const nulls = buffer.getNullsIfAllocated(columnName);

    if (values) {
      // Column was allocated - use zero-copy subarray
      columns[columnName] = values.subarray(0, buffer.writeIndex);

      if (nulls) {
        const neededBytes = Math.ceil(buffer.writeIndex / 8);
        nullBitmaps[columnName] = nulls.subarray(0, neededBytes);
      }
    } else if (scopeValues[fieldName] !== undefined) {
      // Column NOT allocated but scope has value
      // Allocate and fill NOW (cold path - only happens for parent spans with scope-only columns)
      const value = scopeValues[fieldName];
      const ArrayType = getArrayTypeForField(schema.fields[fieldName]);
      const arr = new ArrayType(buffer.writeIndex);
      arr.fill(value);
      columns[columnName] = arr;

      // Create full null bitmap (all values present)
      const neededBytes = Math.ceil(buffer.writeIndex / 8);
      const bitmap = new Uint8Array(neededBytes);
      bitmap.fill(0xff);
      const extraBits = buffer.writeIndex % 8;
      if (extraBits > 0) {
        bitmap[neededBytes - 1] &= (1 << extraBits) - 1;
      }
      nullBitmaps[columnName] = bitmap;
    }
    // If neither allocated nor scope value: column omitted (zero cost)
  }

  // Zero-copy for pre-filled or directly written columns
  // Allocate + fill for scope-only columns (parent span case - rare)
  return buildArrowData(columns, nullBitmaps);
}
```

**Result in Arrow table** (child span with pre-filled columns):

```
| row | timestamp | logMessage | userId | requestId | orderId |
|-----|-----------|------------|--------|-----------|---------|
| 0   | 1000      | null       | u1     | r1        | ord-1   |  <- span-start
| 1   | 2000      | null       | u1     | r1        | ord-1   |  <- span-end
| 2   | 1100      | 'step 1'   | u1     | r1        | ord-1   |  <- event
| 3   | 1200      | 'step 2'   | u1     | r1        | ord-1   |  <- event
```

**Performance characteristics**:

- **userId, requestId**: Pre-filled during child span creation (SIMD optimized) → zero-copy subarray
- **orderId**: Set after child creation → allocate and fill at Arrow conversion (cold path)
- **logMessage**: Written directly → zero-copy subarray

## Scope Value Changes

Changing a scope value is straightforward - it just updates the Scope object:

```typescript
ctx.scope.userId = 'u1'; // Sets Scope property
ctx.log.info('msg1'); // Row 2
ctx.log.info('msg2'); // Row 3
ctx.scope.userId = 'u2'; // Updates Scope property
ctx.log.info('msg3'); // Row 4
```

**Arrow output behavior**:

Since scope values are read at Arrow conversion time, ALL rows will have the LATEST scope value:

```
| row | logMessage | userId |
|-----|------------|--------|
| 0   | null       | u2     |  <- Latest scope value
| 1   | null       | u2     |
| 2   | 'msg1'     | u2     |
| 3   | 'msg2'     | u2     |
| 4   | 'msg3'     | u2     |
```

**Important**: If you need different values per row, use `ctx.tag` which writes to row 0 immediately, or write directly
to the column. Scope is designed for values that apply to the ENTIRE span.

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

| Operation                            | Allocation      | Cost                                   |
| ------------------------------------ | --------------- | -------------------------------------- |
| First `scope.X = value` (parent)     | None            | O(1) - just set Scope property         |
| Second `scope.X = value` (parent)    | None            | O(1) - just update Scope property      |
| `scope.X` (getter)                   | None            | O(1) - just return Scope property      |
| Child span creation with scope       | Yes (pre-fill)  | O(n\*m) - SIMD fill for m scope values |
| `tag.X(value)`                       | Yes             | O(n) - allocate + write                |
| `log.info(msg)` (parent)             | Only logMessage | O(1) - single column write             |
| `log.info(msg)` (child, pre-filled)  | Only logMessage | O(1) - write to pre-filled columns     |
| Arrow conversion (pre-filled column) | None            | O(1) - zero-copy subarray              |
| Arrow conversion (scope-only column) | Yes             | O(n) - allocate + fill                 |
| Arrow conversion (written column)    | None            | O(1) - zero-copy subarray              |

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
ctx.scope.userId = 'user123'; // One-time setup, NO allocation (parent span)
ctx.scope.requestId = 'req456'; // One-time setup, NO allocation (parent span)
ctx.log.info('Step 1'); // userId, requestId filled at Arrow conversion
ctx.log.info('Step 2'); // userId, requestId filled at Arrow conversion
ctx.log.info('Step 3'); // userId, requestId filled at Arrow conversion
// Total: 0 allocations during hot path, scope columns filled at Arrow conversion (cold path)

// WITH scope - child spans get pre-filled columns (SIMD optimized)
await ctx.span('child', async (childCtx) => {
  // Child buffer PRE-FILLED with userId, requestId during span creation (SIMD)
  childCtx.log.info('Step 1'); // Writes to pre-filled columns (already allocated)
  childCtx.log.info('Step 2'); // Writes to pre-filled columns (already allocated)
  childCtx.log.info('Step 3'); // Writes to pre-filled columns (already allocated)
  // Total: One-time SIMD pre-fill at span creation, zero allocation during hot path
});
```

### Comparison with ctx.tag

| Aspect                 | ctx.tag               | ctx.scope                                       |
| ---------------------- | --------------------- | ----------------------------------------------- |
| Writes to row 0        | Yes                   | No (unless also using tag)                      |
| Appears on all rows    | No (row 0 only)       | Yes (at Arrow conversion)                       |
| Allocates on call      | Yes                   | No (just sets Scope property)                   |
| Pre-fills child buffer | No                    | Yes (SIMD optimized)                            |
| Inherited by children  | No                    | Yes                                             |
| Can read value         | No                    | Yes (via getter)                                |
| Use case               | Span-level attributes | Request context flowing through                 |
| Performance (parent)   | Immediate allocation  | Deferred to Arrow conversion                    |
| Performance (child)    | Not applicable        | Pre-filled via SIMD, zero-copy on Arrow convert |

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
  ctx.ff.track('advancedValidation');
}
```

### Arrow/Parquet Output

Scoped attributes are handled efficiently during background processing:

```typescript
// Arrow conversion uses buffer helper methods and scope values
const createArrowVectors = (spanBuffer, scope) => {
  const scopeValues = scope._getScopeValues();

  return {
    // Standard columns - zero-copy
    timestamp: arrow.Float64Vector.from(spanBuffer.timestamps.subarray(0, spanBuffer.writeIndex)),

    // Scoped attributes - check if allocated, fill from scope if not
    // Column properties are DIRECT on buffer via lazy getters (no nested .columns)
    user_id: arrow.Utf8Vector.from(
      spanBuffer.getColumnIfAllocated('attr_userId')?.subarray(0, spanBuffer.writeIndex) ??
        fillArray(new Uint32Array(spanBuffer.writeIndex), scopeValues.userId)
    ),
    request_id: arrow.Utf8Vector.from(
      spanBuffer.getColumnIfAllocated('attr_requestId')?.subarray(0, spanBuffer.writeIndex) ??
        fillArray(new Uint32Array(spanBuffer.writeIndex), scopeValues.requestId)
    ),
  };
};

// Parquet compression handles repeated scoped values very efficiently
// Example: 1000 log entries with same userId compresses to ~12 bytes in Parquet
```

## Benefits Summary

1. **Separate Scope Storage**: Scope values stored in dedicated Scope class, not mixed with buffer columns
2. **Zero Parent Allocation**: Parent span scope sets properties only, NO buffer allocation
3. **SIMD Pre-filling**: Child buffers pre-filled with `TypedArray.fill()` for optimal performance
4. **Consistency**: Impossible to forget important contextual attributes
5. **Clean Code**: Business logic focuses on domain concerns, not logging boilerplate
6. **Hierarchical Context**: Child spans automatically inherit parent context via pre-filling
7. **Predictable Performance**: Pre-filling eliminates deferred allocation surprises
8. **Compression Friendly**: Repeated scoped values compress extremely well in Parquet
9. **Type Safe**: Full TypeScript inference for scoped attribute names and types
10. **Middleware Integration**: Perfect fit for request-level context setup
11. **Readable Scope**: Can read scope values via getters (e.g., `ctx.scope.userId`)
12. **Zero Indirection**: Column properties are direct on SpanBuffer via lazy getters (no nested Record)
13. **Cache-Friendly**: Pre-filling creates contiguous memory writes for better cache utilization

This scope-based approach transforms logging from a repetitive, error-prone task into a clean, consistent, and
performant operation that scales naturally with complex request flows. The separation of scope storage from column
storage enables SIMD optimization for child span creation while maintaining zero allocation for parent spans.

## Integration Points

This span scope attributes system integrates with:

- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Provides the foundational context
  creation and inheritance mechanisms
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Lazy getter implementation for deferred
  column allocation
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Shows how the `ctx.scope` API is generated at
  runtime
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Demonstrates how libraries can use scoped
  attributes for clean traced operations
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Cold-path materialization of scope columns
