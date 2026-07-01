#!/usr/bin/env bun
/**
 * Example: End-to-end workflow
 *
 * Brings several features together:
 * - Scoped attributes (`ctx.setScope`) inherited by child spans
 * - Synchronous and asynchronous feature flags (`ctx.ff.x?.value` / `await ctx.ff.get('x')`)
 * - Multi-level child spans via `ctx.span(name, op, ...args)`
 * - `FlushScheduler` converting registered buffers to Arrow tables
 *
 * Run it:
 *   bun run examples/complete-example.ts
 */

import {
  type AnySpanBuffer,
  createTraceRoot,
  defineCodeError,
  defineFeatureFlags,
  defineLogSchema,
  defineOpContext,
  FlushScheduler,
  InMemoryFlagEvaluator,
  JsBufferStrategy,
  S,
  StdioTracer,
} from '../src/node.js';

const orderSchema = defineLogSchema({
  operation: S.enum(['CREATE_ORDER', 'UPDATE_ORDER', 'CANCEL_ORDER', 'PROCESS_PAYMENT']),
  httpMethod: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  orderId: S.category(),
  userId: S.category(),
  requestId: S.category(),
  sqlQuery: S.text(),
  orderAmount: S.number(),
  itemCount: S.number(),
  isValid: S.boolean(),
  httpStatus: S.number(),
});

const featureFlags = defineFeatureFlags({
  advancedValidation: S.boolean().default(false).sync(),
  experimentalPaymentFlow: S.boolean().default(false).async(),
});

const opContext = defineOpContext({
  logSchema: orderSchema,
  flags: featureFlags.schema,
  ctx: {
    requestId: undefined as string | undefined,
    userId: undefined as string | undefined,
  },
});
const { defineOp } = opContext;

// A single error code per op keeps the op's inferred error type homogeneous.
const ORDER_FAILED = defineCodeError('ORDER_FAILED')<{ stage: 'validation' | 'payment'; reason: string }>();
const EMPTY_ORDER = defineCodeError('EMPTY_ORDER')<{ reason: string }>();
const AMOUNT_TOO_LARGE = defineCodeError('AMOUNT_TOO_LARGE')<{ amount: number; maxAmount: number }>();

interface OrderData {
  userId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
}

// Validation op — demonstrates a flag-guarded nested span and scope override.
const validateOrder = defineOp('validate-order', async (ctx, orderData: OrderData) => {
  if (ctx.ff.advancedValidation?.value) {
    return ctx.span('advanced-validation', async (childCtx) => {
      // Child inherits parent scope (userId, requestId, ...) and can add/override its own.
      childCtx.setScope({ operation: 'UPDATE_ORDER', sqlQuery: 'SELECT * FROM products WHERE id IN (...)' });
      childCtx.log.info('Running advanced validation');
      if (orderData.items.length === 0) {
        return childCtx.err(EMPTY_ORDER({ reason: 'no items' }));
      }
      return childCtx.ok({ validated: true });
    });
  }

  ctx.log.info('Running basic validation');
  if (orderData.items.length === 0) {
    return ctx.err(EMPTY_ORDER({ reason: 'no items' }));
  }
  return ctx.ok({ validated: true });
});

// Payment op — demonstrates an async feature flag.
const processPayment = defineOp('process-payment', async (ctx, orderId: string, amount: number) => {
  ctx.setScope({ orderId, orderAmount: amount, operation: 'PROCESS_PAYMENT' });
  ctx.log.info('Payment processing started');

  if (amount > 10_000) {
    ctx.setScope({ httpStatus: 400 });
    return ctx.err(AMOUNT_TOO_LARGE({ amount, maxAmount: 10_000 }));
  }

  const experimental = await ctx.ff.get('experimentalPaymentFlow');
  if (experimental?.value) {
    // Run the experimental flow as a child span for its tracing; the op's own result is below.
    await ctx.span('experimental-payment', async (childCtx) => {
      childCtx.setScope({ httpStatus: 200, httpMethod: 'POST' });
      childCtx.log.info('Processing payment with experimental flow');
      return childCtx.ok({ ok: true });
    });
  }

  ctx.setScope({ httpStatus: 200 });
  return ctx.ok({ paymentId: `PAY-${Date.now()}`, status: 'success' });
});

// Root op — sets request-level scope, then composes the sub-ops as child spans.
let rootBuffer: AnySpanBuffer | undefined;
const createOrder = defineOp('create-order', async (ctx, orderData: OrderData) => {
  rootBuffer = ctx.buffer;

  ctx.setScope({
    userId: orderData.userId,
    requestId: ctx.requestId,
    httpMethod: 'POST',
    operation: 'CREATE_ORDER',
  });
  ctx.log.info('Order creation started');

  const orderAmount = orderData.items.reduce((sum, item) => sum + item.price * item.quantity, 0);
  const itemCount = orderData.items.reduce((sum, item) => sum + item.quantity, 0);
  ctx.setScope({ orderAmount, itemCount });

  const validation = await ctx.span('validate-order', validateOrder, orderData);
  if (!validation.success) {
    ctx.log.error('Order validation failed');
    return ctx.err(ORDER_FAILED({ stage: 'validation', reason: validation.error.code }));
  }

  const orderId = `ORD-${Date.now()}`;
  ctx.setScope({ orderId, isValid: true });

  const payment = await ctx.span('process-payment', processPayment, orderId, orderAmount);
  if (!payment.success) {
    ctx.log.error('Payment processing failed');
    return ctx.err(ORDER_FAILED({ stage: 'payment', reason: payment.error.code }));
  }

  ctx.log.info('Order created successfully');
  return ctx.ok({ orderId, userId: orderData.userId, amount: orderAmount, status: 'created' });
});

const { trace } = new StdioTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
  flagEvaluator: new InMemoryFlagEvaluator(featureFlags.schema, {
    advancedValidation: true,
    experimentalPaymentFlow: false,
  }),
});

const scheduler = new FlushScheduler(
  (table, metadata) => {
    console.log('\n📊 Flushing trace data:');
    console.log(`   Rows: ${metadata.totalRows}, Buffers: ${metadata.totalBuffers}, Reason: ${metadata.flushReason}`);
    console.log(`   Arrow columns: ${table.names.join(', ')}`);
  },
  { maxFlushInterval: 5000, capacityThreshold: 0.8, idleDetection: true, idleTimeout: 2000 },
);

async function main(): Promise<void> {
  scheduler.start();

  const result = await trace('create-order', { requestId: 'req-12345', userId: 'user-789' }, createOrder, {
    userId: 'user-789',
    items: [
      { productId: 'prod-1', quantity: 2, price: 29.99 },
      { productId: 'prod-2', quantity: 1, price: 49.99 },
    ],
  });

  if (result.success) {
    console.log(`\n✅ Order ${result.value.orderId} created — $${result.value.amount.toFixed(2)}`);
  } else {
    console.log(`\n❌ Order failed: ${result.error.code}`);
  }

  // Register the captured root buffer and flush it through the scheduler.
  if (rootBuffer) {
    scheduler.register(rootBuffer);
  }
  await scheduler.flush();
  scheduler.stop();
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
