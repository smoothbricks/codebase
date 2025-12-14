# Span Scope Attributes

## Overview

Scope attributes are span-level values that automatically propagate to all rows in the span and inherit to child spans.
Unlike `ctx.tag` which writes to row 0 only, `ctx.scope` sets values that appear on EVERY row when converted to Arrow.

**Key difference from tag**:

- `ctx.tag.userId('123')` → writes to row 0 only
- `ctx.scope.userId('123')` → appears on ALL rows (0, 1, 2, 3, ...) in Arrow output

## Design: Lazy Scope with Zero Hot-Path Allocation

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

  // For black-box testing
  get isInitialized(): boolean {
    return this._values !== null;
  }

  get hasScopeValue(): boolean {
    return this._scopeValue !== null;
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
    this._values = new this.ArrayType(this.capacity);
    if (this._scopeValue !== null) {
      this._values.fill(this._scopeValue);
    }
  }

  // Hot path - allocate on first direct write
  get values(): T {
    if (!this._values) {
      this._allocateAndFillWithScope();
    }
    return this._values;
  }

  // Cold path - Arrow conversion
  getValuesForArrow(writeIndex: number): T | null {
    if (this._values) {
      // Was written to directly - zero-copy
      return this._values.subarray(0, writeIndex);
    }
    if (this._scopeValue !== null) {
      // Never written directly, but has scope value
      // Allocate NOW (cold path) and fill
      const arr = new this.ArrayType(writeIndex);
      arr.fill(this._scopeValue);
      return arr;
    }
    // No data, no scope - null column
    return null;
  }
}
```

### Allocation Triggers

TypedArray allocation happens:

1. **Direct write** (e.g., `ctx.tag.userId()`) - accesses `.values`, triggers allocation + scope fill
2. **Second scope call** - must preserve old scope value, triggers allocation + fill
3. **Cold path Arrow conversion** - if still lazy but has scope value, allocate and fill

First `ctx.scope.X()` call = just sets `_scopeValue`, NO allocation.

## API Design

### ctx.scope vs ctx.tag

```typescript
// ctx.tag - writes to row 0 only, NOT inherited
ctx.tag.userId('123'); // Allocates column, writes to row 0

// ctx.scope - sets scope value, inherited to children, appears on ALL rows
ctx.scope.userId('123'); // Just sets _scopeValue, NO allocation
ctx.scope.requestId('req-1'); // Just sets _scopeValue, NO allocation
```

Both are chainable:

```typescript
ctx.scope.userId('123').requestId('req-1').orderId('ord-456');
ctx.tag.status(200).method('POST');
```

### Scope Inheritance

Child spans inherit parent's scope object:

```typescript
// Parent span
ctx.scope.userId('u1').requestId('r1');

await ctx.span('child-operation', async (childCtx) => {
  // childCtx has NEW LazyColumns, each with inherited _scopeValue
  // No TypedArrays allocated yet

  childCtx.scope.orderId('ord-1'); // Add to scope, still no allocation

  childCtx.log.info('processing'); // Only allocates attr_logMessage
  // attr_userId, attr_requestId, attr_orderId still lazy

  // At Arrow conversion: all three columns materialized with scope values
});
```

### Implementation of Scope Inheritance

```typescript
function createChildSpanBuffer(parentBuffer: SpanBuffer): SpanBuffer {
  const childBuffer = createEmptySpanBuffer(/* ... */);

  // Copy parent's scope values to child's LazyColumns
  for (const [columnName, parentColumn] of Object.entries(parentBuffer.columns)) {
    if (parentColumn.hasScopeValue) {
      childBuffer.columns[columnName].setScope(parentColumn._scopeValue);
    }
  }

  return childBuffer;
}
```

## Hot Path Behavior

```typescript
ctx.scope.userId('u1'); // _scopeValue = 'u1', NO allocation
ctx.scope.requestId('r1'); // _scopeValue = 'r1', NO allocation
ctx.log.info('step 1'); // Allocates attr_logMessage ONLY
ctx.log.info('step 2'); // Writes to attr_logMessage
ctx.log.info('step 3'); // Writes to attr_logMessage

// attr_userId: _values = null, _scopeValue = 'u1'
// attr_requestId: _values = null, _scopeValue = 'r1'
// attr_logMessage: _values = ['step 1', 'step 2', 'step 3', ...]
```

## Cold Path: Arrow Conversion

```typescript
function convertToArrow(buffer: SpanBuffer): ArrowData {
  const columns: Record<string, TypedArray | string[]> = {};

  for (const [name, lazyColumn] of Object.entries(buffer.columns)) {
    const arrowData = lazyColumn.getValuesForArrow(buffer.writeIndex);
    if (arrowData !== null) {
      columns[name] = arrowData;
    }
  }

  // Zero-copy for directly written columns
  // Allocate + fill for scope-only columns
  return buildArrowData(columns);
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
ctx.scope.userId('u1'); // _scopeValue = 'u1', no allocation
ctx.log.info('msg1'); // Row 2, only touches logMessage
ctx.log.info('msg2'); // Row 3, only touches logMessage
ctx.scope.userId('u2'); // SECOND scope call!
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

## Performance Characteristics

| Operation                         | Allocation      | Cost                                      |
| --------------------------------- | --------------- | ----------------------------------------- |
| First `scope.X(value)`            | None            | O(1) - just set \_scopeValue              |
| Second `scope.X(value)`           | Yes             | O(n) - allocate + fill to writeIndex      |
| `tag.X(value)`                    | Yes             | O(n) - allocate + fill with scope + write |
| `log.info(msg)`                   | Only logMessage | O(1) - single column write                |
| Arrow conversion (scope column)   | Yes if lazy     | O(n) - allocate + fill                    |
| Arrow conversion (written column) | None            | O(1) - zero-copy subarray                 |

## Comparison with ctx.tag

| Aspect                | ctx.tag               | ctx.scope                       |
| --------------------- | --------------------- | ------------------------------- |
| Writes to row 0       | Yes                   | No (unless also using tag)      |
| Appears on all rows   | No (row 0 only)       | Yes (at Arrow conversion)       |
| Allocates on call     | Yes                   | No (first call)                 |
| Inherited by children | No                    | Yes                             |
| Use case              | Span-level attributes | Request context flowing through |

## Usage Patterns

### Request Middleware

```typescript
app.use((req, res, next) => {
  const ctx = createRequestContext({ requestId: req.id });

  // Set scope once - flows through ALL operations
  ctx.scope.requestId(req.id).userId(req.user?.id).endpoint(req.path).method(req.method);

  req.ctx = ctx;
  next();
});
```

### Business Logic

```typescript
const processOrder = task('process-order', async (ctx, order) => {
  // Add business-specific scope (inherits request scope)
  ctx.scope.orderId(order.id).customerId(order.customerId);

  // All log entries and child spans get these values automatically
  ctx.log.info('Processing order');

  await ctx.span('validate', async (childCtx) => {
    // childCtx inherits: requestId, userId, endpoint, method, orderId, customerId
    childCtx.log.info('Validating'); // All scope values in Arrow output
  });

  return ctx.ok({ processed: true });
});
```

## Integration Points

- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: LazyColumn implementation
- **[Context Flow](./01c_context_flow_and_task_wrappers.md)**: Scope inheritance in task/span creation
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Cold-path materialization
