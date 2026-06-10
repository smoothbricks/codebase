#!/usr/bin/env bun
/**
 * Example: Fluent tag-chaining showcase
 *
 * Demonstrates:
 * - Heavy fluent chaining: `ctx.tag.orderId(id).amount(n).currency('USD').status('processing')`
 * - Bulk attributes with `ctx.tag.with({...})` then continued chaining
 * - Feature-flag-guarded child spans (`ctx.ff.fraudDetection?.value`)
 * - User context (`requestId` / `userId`) supplied per-trace via overrides
 *
 * Run it:
 *   bun run examples/chaining-showcase.ts
 */

import {
  createTraceRoot,
  defineCodeError,
  defineFeatureFlags,
  defineLogSchema,
  defineOpContext,
  InMemoryFlagEvaluator,
  JsBufferStrategy,
  S,
  StdioTracer,
} from '../src/node.js';

const orderSchema = defineLogSchema({
  requestId: S.category(),
  userId: S.category(),
  orderId: S.category(),
  amount: S.number(),
  currency: S.enum(['USD', 'EUR', 'GBP', 'JPY']),
  paymentMethod: S.enum(['card', 'paypal', 'bank_transfer']),
  status: S.enum(['pending', 'processing', 'completed', 'failed']),
  duration: S.number(),
  httpStatus: S.number(),
  operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
});

const featureFlags = defineFeatureFlags({
  fraudDetection: S.boolean().default(true).sync(),
  fastCheckout: S.boolean().default(false).sync(),
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

const INSUFFICIENT_FUNDS = defineCodeError('INSUFFICIENT_FUNDS')<{ amount: number; limit: number }>();
const PAYMENT_FAILED = defineCodeError('PAYMENT_FAILED')<{ reason: string }>();

type Currency = 'USD' | 'EUR' | 'GBP' | 'JPY';
type PaymentMethod = 'card' | 'paypal' | 'bank_transfer';
interface Order {
  id: string;
  total: number;
  currency: Currency;
  paymentMethod: PaymentMethod;
}

// 1. Simple chaining.
const validateOrder = defineOp('validate-order', async (ctx, orderId: string) => {
  ctx.tag
    .requestId(ctx.requestId ?? 'req-unknown')
    .userId(ctx.userId ?? 'guest')
    .orderId(orderId)
    .operation('SELECT')
    .status('pending');
  return ctx.ok({ valid: true });
});

// 2. Chaining across a child span, with a typed error path.
const processPayment = defineOp('process-payment', async (ctx, order: Order) => {
  ctx.tag.orderId(order.id).amount(order.total).currency(order.currency).paymentMethod(order.paymentMethod).status('processing');

  const payment = await ctx.span('call-payment-gateway', async (childCtx) => {
    const start = performance.now();
    childCtx.tag.operation('INSERT').orderId(order.id).amount(order.total).status('processing');

    const succeeded = order.total < 10_000; // simulate a limit
    const duration = performance.now() - start;

    if (!succeeded) {
      childCtx.tag.duration(duration).httpStatus(402).status('failed');
      return childCtx.err(INSUFFICIENT_FUNDS({ amount: order.total, limit: 10_000 }));
    }

    childCtx.tag.duration(duration).httpStatus(200).status('completed');
    return childCtx.ok({ transactionId: 'txn-123', duration });
  });

  if (!payment.success) {
    ctx.tag.status('failed').httpStatus(500);
    return ctx.err(PAYMENT_FAILED({ reason: payment.error.code }));
  }

  ctx.tag.status('completed').httpStatus(201).duration(payment.value.duration);
  return ctx.ok({ orderId: order.id, transactionId: payment.value.transactionId });
});

// 3. Mixing with() and chaining + a flag-guarded child span.
const createOrder = defineOp('create-order', async (ctx, order: Order) => {
  ctx.tag
    .with({ requestId: ctx.requestId ?? 'req-unknown', userId: ctx.userId ?? 'guest', operation: 'INSERT' })
    .orderId(order.id)
    .amount(order.total)
    .currency(order.currency)
    .paymentMethod(order.paymentMethod)
    .status('pending');

  if (ctx.ff.fraudDetection?.value) {
    await ctx.span('fraud-check', async (childCtx) => {
      childCtx.tag.orderId(order.id).operation('SELECT').status('pending');
      return childCtx.ok({ safe: true });
    });
  }

  ctx.tag.status('completed').httpStatus(201).duration(25.5);
  return ctx.ok({ created: true, orderId: order.id });
});

const { trace } = new StdioTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
  flagEvaluator: new InMemoryFlagEvaluator(featureFlags.schema, { fraudDetection: true, fastCheckout: true }),
});

async function main(): Promise<void> {
  const overrides = { requestId: `req-${Date.now()}`, userId: 'user-456' };

  console.log('=== 1. Simple chaining ===');
  console.log(await trace('validate-order', overrides, validateOrder, 'order-789'));

  console.log('\n=== 2. Payment with child span ===');
  console.log(
    await trace('process-payment', overrides, processPayment, {
      id: 'order-789',
      total: 149.99,
      currency: 'USD',
      paymentMethod: 'card',
    }),
  );

  console.log('\n=== 3. with() + chaining + flag-guarded child span ===');
  console.log(
    await trace('create-order', overrides, createOrder, {
      id: 'order-999',
      total: 299.99,
      currency: 'EUR',
      paymentMethod: 'paypal',
    }),
  );
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
