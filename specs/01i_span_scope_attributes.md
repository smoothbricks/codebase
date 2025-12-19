# Span Scope Attributes

## Overview

Span scope attributes allow setting attributes at the span level that automatically propagate to all rows in the span
and are inherited by child spans. This eliminates repetitive attribute setting, ensures consistency, and provides
zero-hot-path allocation.

**Key differences from direct writes**:

- `tag.userId('123')` → writes to row 0 only (span-start)
- `ctx.ok().userId('123')` → writes to row 1 only (span-end)
- `setScope({ userId: '123' })` → default for ALL rows in Arrow output
- **Direct writes win**: tag wins on row 0, ok/err wins on row 1, scope fills the rest

**Example - why scope on all rows matters**:

```
Row 0 (span-start): "looked up orderId X"     ← orderId from scope
Row 1 (span-err):   "orderId X lookup failed" ← orderId from scope (correlation preserved!)
```

Without scope, you'd need to repeat `orderId` on every log entry for correlation.

## Design Philosophy

**Key Insight**: Many attributes are contextual to an entire span (requestId, userId, orderId) and should be set once at
the span level rather than repeated on every log entry. This is particularly valuable in middleware and request
handling.

**Scope Hierarchy**:

```
Request Middleware: setScope({ requestId, userId, endpoint, method })
├── Business Logic: setScope({ orderId, orderAmount })
│   ├── All log entries include: requestId, userId, endpoint, method, orderId, orderAmount
│   └── Child Span: setScope({ validationStep })
│       └── All child entries include: requestId, userId, endpoint, method, orderId, orderAmount, validationStep
```

**Performance Optimization**: Scope values are stored in a plain object on the buffer. Column allocation and filling
happens at Arrow conversion time (cold path) using `TypedArray.fill()` for SIMD optimization.

## Implementation: Plain Object Storage

**No runtime code generation needed for scope.** Scope is stored as a plain object on the buffer and filled into columns
at Arrow conversion time.

### Why No Codegen for Scope?

Scope is **never in the hot path**:

| Operation                     | When             | Hot Path? |
| ----------------------------- | ---------------- | --------- |
| `ctx.setScope({ userId: x })` | User code        | No        |
| `ctx.scope.userId`            | User code (rare) | No        |
| Child inherits parent scope   | Span creation    | No        |
| Scope values → Arrow columns  | Arrow conversion | No        |

Since scope is only accessed during span setup and Arrow conversion (both cold paths), V8 hidden class optimization
doesn't apply. A plain object is simpler and works just as well.

### Scope Storage

```typescript
interface SpanBuffer {
  // ... existing fields (timestamps, entryTypes, etc.)

  // Scope values - immutable object, replaced on each setScope call
  scopeValues: Readonly<Record<string, unknown>>;
}

// Created with empty frozen scope
function createSpanBuffer(capacity: number): SpanBuffer {
  return {
    // ... other fields
    scopeValues: Object.freeze({}),
  };
}
```

### setScope API - Immutable Merge Semantics

`setScope` creates a **new immutable object** by merging values into the existing scope. Use `null` to clear a value.
The existing scope object is never mutated.

```typescript
function setScope(ctx: SpanContext, values: Record<string, unknown>): void {
  const current = ctx.buffer.scopeValues;
  const next: Record<string, unknown> = { ...current };

  for (const [key, value] of Object.entries(values)) {
    if (value === null) {
      delete next[key];
    } else {
      next[key] = value;
    }
  }

  ctx.buffer.scopeValues = Object.freeze(next); // Replace with new frozen object
}
```

**Examples**:

```typescript
ctx.setScope({ orderId: 'x' }); // scope = { orderId: 'x' } (new object)
ctx.setScope({ userId: 'u1' }); // scope = { orderId: 'x', userId: 'u1' } (new object)
ctx.setScope({ userId: 'u2' }); // scope = { orderId: 'x', userId: 'u2' } (new object)
ctx.setScope({ userId: null }); // scope = { orderId: 'x' } (new object, userId removed)
```

**Why immutable?** See [Immutable Scope Semantics](#immutable-scope-semantics) below for the async safety benefits.

## API Design

### Context API

```typescript
interface SpanContext<Schema> {
  // Set scope values (merge semantics, null to clear)
  setScope(values: Partial<SchemaValues<Schema> | null>): void;

  // Read-only view of current scope
  readonly scope: Readonly<SchemaValues<Schema>>;

  // Tag writes to row 0 only
  tag: TagMethods<Schema>;

  // Logging
  log: LogMethods;

  // Results
  ok<T>(value: T): Result<T>;
  err(code: string, details?: unknown): Result<never>;
}
```

### Usage Example

```typescript
const handleRequest = op(async (ctx, req) => {
  // Set scoped attributes - these appear on ALL rows
  ctx.setScope({
    userId: req.user.id,
    requestId: req.id,
    endpoint: req.path,
  });

  // Tag writes to row 0 only - specific to span-start
  ctx.tag.triggerSource('webhook');
  ctx.tag.webhookId('wh_abc123');

  // All log entries will include scope values
  ctx.log.info('Processing request');
  // Row: userId, requestId, endpoint (from scope)

  // Read scope if needed
  console.log(ctx.scope.userId); // 'user-123'

  return ctx.ok({ done: true });
});
```

### setScope vs tag

```typescript
const handleRequest = op(async (ctx, req) => {
  // SCOPE: Context for ALL rows - correlation, identification
  ctx.setScope({
    requestId: req.id, // Every row needs this for filtering
    userId: req.user?.id, // Identify who this request is for
    tenantId: req.tenant?.id, // Multi-tenant context
  });

  // TAG: Specific to row 0 (span-start) - what triggered this?
  ctx.tag.clientIp(req.ip); // Entry point detail
  ctx.tag.userAgent(req.headers['user-agent']); // Not needed on every row
  ctx.tag.contentLength(req.headers['content-length']); // Request metadata

  ctx.log.info('Request started');
  // Row 0 (span-start): requestId, userId, tenantId, clientIp, userAgent, contentLength
  // Row 1+ (log entries): requestId, userId, tenantId (scope only)
});
```

## Scope Inheritance

Child spans inherit parent's scope by **reference** (safe because scope objects are immutable):

```typescript
function createChildSpanBuffer(parent: SpanBuffer): SpanBuffer {
  return {
    // ... other fields
    scopeValues: parent.scopeValues, // Same reference - safe because immutable!
  };
}
```

**No copy needed!** Since scope objects are never mutated, sharing the reference is safe. When either parent or child
calls `setScope`, they get a NEW object - the shared reference is unaffected.

**Example**:

```typescript
const parentOp = op(async (ctx) => {
  ctx.setScope({ requestId: 'req-1', userId: 'u1' });
  // ctx.scopeValues = Object A: { requestId: 'req-1', userId: 'u1' }

  await ctx.span('child-operation', async (childCtx) => {
    // childCtx.scopeValues = Object A (same reference as parent)

    // Child adds more scope - creates NEW object
    childCtx.setScope({ orderId: 'ord-1' });
    // childCtx.scopeValues = Object B: { requestId: 'req-1', userId: 'u1', orderId: 'ord-1' }
    // Parent still references Object A

    // Child overrides - creates another NEW object
    childCtx.setScope({ userId: 'u2' });
    // childCtx.scopeValues = Object C: { requestId: 'req-1', userId: 'u2', orderId: 'ord-1' }
  });

  // Parent scope still references Object A: { requestId: 'req-1', userId: 'u1' }
});
```

## Immutable Scope Semantics

Scope objects are **immutable** - `setScope` always creates a new frozen object rather than mutating the existing one.
This provides **snapshot semantics** that are critical for async code safety.

### Why Immutable?

Consider this async scenario with mutable scope (BAD):

```typescript
// ❌ MUTABLE (hypothetical) - Race condition!
ctx.span('query', async (ctx) => {
  ctx.setScope({ status: 'pending' }); // Mutates shared object

  const childPromise = ctx.span('validate', async (childCtx) => {
    // Child holds reference to parent's mutable object
    await someAsyncWork();
    console.log(childCtx.scope.status); // 'pending' or 'completed'?? RACE CONDITION!
  });

  ctx.setScope({ status: 'completed' }); // Mutates same object - affects child!
  await childPromise;
});
```

With immutable scope (GOOD):

```typescript
// ✅ IMMUTABLE - Snapshot semantics, no race condition
ctx.span('query', async (ctx) => {
  ctx.setScope({ status: 'pending' });
  // ctx.scopeValues = Object A: { status: 'pending' }

  const childPromise = ctx.span('validate', async (childCtx) => {
    // childCtx.scopeValues = Object A (reference to parent's current object)
    await someAsyncWork();
    console.log(childCtx.scope.status); // Always 'pending' ✅
    // Child still references Object A, which is frozen/immutable
  });

  ctx.setScope({ status: 'completed' });
  // ctx.scopeValues = Object B: { status: 'pending', status: 'completed' } (NEW object!)
  // Object A still exists, child still references it

  await childPromise;
});
```

### Benefits of Immutable Scope

1. **Snapshot semantics**: Child span's scope is "frozen" at the moment it was created
2. **No race conditions**: Parent mutations don't affect running child spans
3. **No copy on inherit**: Child just gets the reference (zero cost)
4. **Predictable behavior**: Scope values are deterministic, not timing-dependent
5. **GC handles cleanup**: Old scope objects are collected when no longer referenced

## Direct Writes vs Scope

Scope provides default values for all rows, but **direct writes always win** on any row:

- **Row 0 (span-start)**: `tag.X()` wins over scope
- **Row 1 (span-end)**: `ctx.ok().X()` / `ctx.err().X()` wins over scope
- **Rows 2+ (log entries)**: `ctx.log.info().X()` wins over scope for that specific row

```typescript
const processOrder = op(async (ctx, order) => {
  // Scope: default for all rows
  ctx.setScope({ status: 'processing', orderId: order.id });

  // Tag: specific to row 0 (span-start)
  ctx.tag.status('started'); // Overrides scope on row 0 only

  // Log entries: fluent attributes override scope for that row only
  ctx.log.info('Step 1'); // status = 'processing' (from scope)
  ctx.log.info('Step 2').status('validating'); // status = 'validating' (direct write wins)
  ctx.log.info('Step 3'); // status = 'processing' (from scope)

  // ok/err: specific to row 1 (span-end)
  return ctx.ok({ done: true }).status('completed'); // Overrides scope on row 1 only
});
```

**Arrow output**:

```
| row | entryType   | status       | orderId |
|-----|-------------|--------------|---------|
| 0   | span-start  | 'started'    | ord-1   |  ← tag wins on row 0
| 1   | span-ok     | 'completed'  | ord-1   |  ← ok() wins on row 1
| 2   | info        | 'processing' | ord-1   |  ← scope value (no direct write)
| 3   | info        | 'validating' | ord-1   |  ← direct write wins on this row
| 4   | info        | 'processing' | ord-1   |  ← scope value (no direct write)
```

**Key principle**: Any direct write to a column for a specific row wins over scope for that row. Scope only fills cells
that have no direct write (null in the column).

**Semantic meaning**:

- **tag (row 0)**: "Entry point state - what triggered this span"
- **ok/err (row 1)**: "Exit state - what was the outcome"
- **log fluent attributes**: "Specific state at this log entry"
- **scope**: "Default context for rows without direct writes"

## Arrow Conversion: Fill Optimization

At Arrow conversion time (cold path), scope values fill any NULL cells in columns. Direct writes (tag, ok/err, log
fluent attributes) are preserved - scope only fills cells that weren't written to.

```typescript
// Simplified logic for each column during Arrow conversion:
for (let row = 0; row < writeIndex; row++) {
  if (columnHasDirectWrite(row)) {
    // Direct write exists - use it (already in column)
    continue;
  } else if (scopeValue !== undefined) {
    // No direct write, but scope has value - fill with scope
    column[row] = scopeValue;
    setNullBit(nullBitmap, row);
  }
  // else: no direct write and no scope - remains null
}
```

The actual implementation checks the null bitmap to detect direct writes:

```typescript
// For each row in buffer:
const hasDirectWrite = (nullBitmap[row >>> 3] & (1 << (row & 7))) !== 0;
if (!hasDirectWrite && scopeValue !== undefined) {
  column[row] = encodedScopeValue;
  nullBitmap[row >>> 3] |= 1 << (row & 7);
}
```

**Key points**:

1. **Direct writes always win**: Any row with a direct write (non-null in column) keeps its value
2. **Scope fills nulls**: Scope value fills any row that has no direct write
3. **Row-level granularity**: Each row is checked independently
4. **Cold path only**: This happens during Arrow conversion, not hot path
5. **SIMD where possible**: When large ranges have no direct writes, `TypedArray.fill()` can be used

## Scope Value Changes

Each `setScope` call creates a new immutable object. Since columns are filled at Arrow conversion, ALL rows get the
**latest** scope value (the final object at span completion):

```typescript
const processOp = op(async (ctx) => {
  ctx.setScope({ phase: 'init' }); // Object A
  ctx.log.info('msg1');
  ctx.log.info('msg2');
  ctx.setScope({ phase: 'processing' }); // Object B (replaces A)
  ctx.log.info('msg3');
  return ctx.ok('done');
  // At Arrow conversion, ctx.scopeValues = Object B
});
```

**Arrow output**:

```
| row | entryType   | logMessage | phase        |
|-----|-------------|------------|--------------|
| 0   | span-start  | null       | 'processing' |  ← Latest scope value (Object B)
| 1   | info        | 'msg1'     | 'processing' |  ← Latest scope value (Object B)
| 2   | info        | 'msg2'     | 'processing' |  ← Latest scope value (Object B)
| 3   | info        | 'msg3'     | 'processing' |  ← Latest scope value (Object B)
| 4   | span-ok     | null       | 'processing' |  ← Latest scope value (Object B)
```

**Important**: Scope is designed for values that apply to the ENTIRE span. If you need different values per row, use
`tag` (row 0 only) or write directly to columns.

**Note**: While scope changes within a span affect all rows (latest value wins), child spans created BEFORE a scope
change retain their snapshot of the parent's scope at creation time (see
[Immutable Scope Semantics](#immutable-scope-semantics)).

## Real-World Usage Patterns

### E-commerce Order Processing

```typescript
const fulfillOrder = op(async (ctx, orderId: string) => {
  const order = await loadOrder(orderId);

  // SCOPE: Context for all rows - enables filtering by orderId across entire trace
  ctx.setScope({
    orderId,
    customerId: order.customerId,
    orderTotal: order.total,
    fulfillmentCenter: 'FC-WEST-1',
  });

  // TAG: What triggered this span (row 0 only)
  ctx.tag.triggerSource('webhook');
  ctx.tag.webhookId('wh_abc123');

  await ctx.span('validate-inventory', async (childCtx) => {
    // Child inherits: orderId, customerId, orderTotal, fulfillmentCenter

    childCtx.setScope({ warehouseId: 'WH-42' });

    childCtx.tag.skuCount(order.items.length); // Row 0 of child span

    childCtx.log.info('Checking stock levels');
    // Row: orderId, customerId, fulfillmentCenter, warehouseId

    if (insufficientStock) {
      return childCtx.err('INSUFFICIENT_STOCK');
      // span-err row: orderId, customerId, fulfillmentCenter, warehouseId
    }
    return childCtx.ok({ reserved: true });
  });

  return ctx.ok({ fulfilled: true });
});
```

### API Gateway / Request Handler

```typescript
const handleRequest = op(async (ctx, req: Request) => {
  // SCOPE: Request context flows through EVERYTHING
  ctx.setScope({
    requestId: req.headers['x-request-id'] ?? crypto.randomUUID(),
    userId: req.user?.id,
    tenantId: req.tenant?.id,
    endpoint: req.path,
    httpMethod: req.method,
  });

  // TAG: Entry point details (row 0 only)
  ctx.tag.clientIp(req.ip);
  ctx.tag.userAgent(req.headers['user-agent']);
  ctx.tag.contentLength(req.headers['content-length']);

  // All downstream operations include request context
  const result = await processRequest(ctx, req);

  return ctx.ok({
    statusCode: result.status,
    responseTime: performance.now() - startTime,
  });
});
```

### Payment Processing

```typescript
const processPayment = op(async (ctx, paymentRequest: PaymentRequest) => {
  // SCOPE: Non-sensitive context for correlation
  ctx.setScope({
    merchantId: paymentRequest.merchantId,
    transactionId: crypto.randomUUID(),
    currency: paymentRequest.currency,
    paymentMethod: paymentRequest.method,
  });

  // TAG: Specific to this attempt
  ctx.tag.amount(paymentRequest.amount);
  ctx.tag.cardLast4(paymentRequest.card.last4);

  const fraudResult = await ctx.span('fraud-check', async (childCtx) => {
    childCtx.setScope({
      fraudModel: 'v2.3',
      riskThreshold: 0.7,
    });

    childCtx.tag.riskScore(calculateRisk()); // Tag on child span-start

    childCtx.log.info('Risk evaluation complete');
    // Row: merchantId, transactionId, currency, paymentMethod, fraudModel, riskThreshold

    return childCtx.ok({ approved: true });
  });

  return ctx.ok({ charged: true });
});
```

### Background Job Processing

```typescript
const processJob = op(async (ctx, job: QueueJob) => {
  // SCOPE: Job context for all processing
  ctx.setScope({
    jobId: job.id,
    jobType: job.type,
    queueName: job.queue,
    attempt: job.attemptNumber,
    correlationId: job.correlationId, // Links back to original request
  });

  // TAG: Queue infrastructure details (row 0)
  ctx.tag.messageId(job.messageId);
  ctx.tag.enqueuedAt(job.timestamp);

  ctx.log.info('Job processing started');
  // Row: jobId, jobType, queueName, attempt, correlationId

  for (const item of job.items) {
    await ctx.span('process-item', async (itemCtx) => {
      itemCtx.setScope({ itemId: item.id });

      itemCtx.log.debug('Processing item');
      // Row: jobId, jobType, queueName, attempt, correlationId, itemId
    });
  }

  return ctx.ok({ processed: job.items.length });
});
```

## Performance Characteristics

### Summary Table

| Operation                    | Allocation | Cost                              |
| ---------------------------- | ---------- | --------------------------------- |
| `setScope({ X: value })`     | New object | O(m) - spread m existing props    |
| `ctx.scope.X`                | None       | O(1) - object property read       |
| Child span creation          | None       | O(1) - reference copy (immutable) |
| Arrow conversion (scope col) | TypedArray | O(n) - SIMD fill n rows           |
| Arrow conversion (tag col)   | None       | O(1) - already allocated          |

**Note**: While `setScope` allocates a new object, this is negligible - scope is cold path and typical spans call
`setScope` only 1-3 times. The benefit (async safety, zero-cost child inheritance) far outweighs the small allocation.

### Comparison: Scope vs Repetitive Tagging

```typescript
// WITHOUT scope - O(m) per log operation, m allocations
const withoutScope = op(async (ctx) => {
  ctx.tag.userId('user123').requestId('req456');
  ctx.log.info('Step 1');
  ctx.tag.userId('user123').requestId('req456');
  ctx.log.info('Step 2');
  ctx.tag.userId('user123').requestId('req456');
  ctx.log.info('Step 3');
  // Total: Multiple column writes per log entry
});

// WITH scope - O(1) per log operation, 1 small object allocation
const withScope = op(async (ctx) => {
  ctx.setScope({ userId: 'user123', requestId: 'req456' }); // One small object
  ctx.log.info('Step 1'); // No scope overhead
  ctx.log.info('Step 2'); // No scope overhead
  ctx.log.info('Step 3'); // No scope overhead
  // Total: Scope columns filled at Arrow conversion (SIMD)
});
```

### Comparison: tag vs ok/err vs log fluent vs setScope

| Aspect                | tag                | ok/err             | log fluent (e.g. `.info().X()`) | setScope                    |
| --------------------- | ------------------ | ------------------ | ------------------------------- | --------------------------- |
| Writes to row         | 0 (span-start)     | 1 (span-end)       | That specific log row           | Default for all rows        |
| Appears on all rows   | No                 | No                 | No                              | Yes (where no direct write) |
| Allocates on call     | Yes (column write) | Yes (column write) | Yes (column write)              | Yes (new immutable object)  |
| Inherited by children | No                 | No                 | No                              | Yes (zero-cost reference)   |
| Async safe            | N/A                | N/A                | N/A                             | Yes (snapshot semantics)    |
| API style             | Method chain       | Method chain       | Method chain                    | Object parameter            |
| Use case              | Entry point state  | Exit/result state  | Per-log-entry state             | Request context propagation |
| Collision with scope  | Wins on row 0      | Wins on row 1      | Wins on that row                | Fills nulls only            |

## Benefits Summary

1. **Immutable Objects**: `setScope` creates new frozen objects - async safe, no race conditions
2. **Zero-Cost Inheritance**: Child spans share parent's scope reference (no copy needed)
3. **Snapshot Semantics**: Child span's scope is frozen at creation time
4. **Direct Writes Win**: Any direct write (tag, ok/err, log fluent) overrides scope for that row
5. **Consistency**: Impossible to forget important contextual attributes
6. **Clean Code**: Business logic focuses on domain concerns, not logging boilerplate
7. **Hierarchical Context**: Child spans automatically inherit parent scope
8. **Compression Friendly**: Repeated scope values compress extremely well in Parquet
9. **Type Safe**: Full TypeScript inference for scope attribute names and types
10. **Merge Semantics**: `setScope` merges, `null` clears - intuitive API
11. **CSP Compatible**: No `new Function()` needed for scope

## Integration Points

This span scope attributes system integrates with:

- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Provides the foundational context
  creation and inheritance mechanisms
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Lazy column allocation for tag columns
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Demonstrates how libraries can use scoped
  attributes for clean traced operations
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Cold-path materialization of scope columns with SIMD fill
