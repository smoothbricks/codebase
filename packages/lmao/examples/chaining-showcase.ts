/**
 * Method Chaining Showcase
 * 
 * This example demonstrates the fluent method chaining API for tag attributes.
 * Each tag method returns the tag object, allowing natural, readable chaining.
 */

import { S } from '../src/lib/schema/builder.js';
import { defineTagAttributes } from '../src/lib/schema/defineTagAttributes.js';
import { defineFeatureFlags } from '../src/lib/schema/defineFeatureFlags.js';
import { InMemoryFlagEvaluator } from '../src/lib/schema/evaluator.js';
import { createRequestContext, createModuleContext } from '../src/lib/lmao.js';

// Define comprehensive tag attributes
const orderAttributes = defineTagAttributes({
  requestId: S.string(),
  userId: S.string(),
  orderId: S.string(),
  amount: S.number(),
  currency: S.string(),
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

const environmentConfig = {
  awsRegion: 'us-east-1',
  paymentGateway: 'stripe',
  environment: 'production',
};

const flagEvaluator = new InMemoryFlagEvaluator({
  fraudDetection: true,
  fastCheckout: true,
});

// Create module context
const { task } = createModuleContext({
  moduleMetadata: {
    gitSha: 'abc123def456',
    filePath: 'src/services/order.ts',
    moduleName: 'OrderService',
  },
  tagAttributes: orderAttributes,
});

// Example 1: Simple chaining
const validateOrder = task('validate-order', async (ctx, orderId: string) => {
  // Clean, readable chaining
  ctx.log.tag
    .requestId(ctx.requestId)
    .userId(ctx.userId || 'guest')
    .orderId(orderId)
    .operation('SELECT')
    .status('pending');

  return ctx.ok({ valid: true });
});

// Example 2: Real-world pattern from requirements
interface Order {
  id: string;
  total: number;
  currency: string;
  paymentMethod: 'card' | 'paypal' | 'bank_transfer';
}

const processPayment = task('process-payment', async (ctx, order: Order) => {
  // Example: ctx.log.tag.orderId(order.id).amount(order.total)
  ctx.log.tag
    .orderId(order.id)
    .amount(order.total)
    .currency(order.currency)
    .paymentMethod(order.paymentMethod)
    .status('processing');

  // Simulate payment processing with child span
  const payment = await ctx.span('call-payment-gateway', async (childCtx) => {
    const startTime = Date.now();

    // Chaining in child spans
    childCtx.log.tag
      .operation('INSERT')
      .orderId(order.id)
      .amount(order.total)
      .status('processing');

    // Simulate API call
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Simulate potential payment failure (e.g., insufficient funds)
    const paymentSucceeded = order.total < 10000; // Fail for large amounts

    const duration = Date.now() - startTime;

    if (!paymentSucceeded) {
      childCtx.log.tag
        .duration(duration)
        .httpStatus(402)
        .status('failed');

      return childCtx.err('INSUFFICIENT_FUNDS', { amount: order.total, limit: 10000 });
    }

    // Chain more after processing
    childCtx.log.tag
      .duration(duration)
      .httpStatus(200)
      .status('completed');

    return childCtx.ok({ transactionId: 'txn-123', duration });
  });

  if (!payment.success) {
    ctx.log.tag.status('failed').httpStatus(500);
    return ctx.err('PAYMENT_FAILED', payment.error);
  }

  // Final status update with chaining
  ctx.log.tag
    .status('completed')
    .httpStatus(201)
    .duration(payment.value.duration);

  return ctx.ok({
    orderId: order.id,
    transactionId: payment.value.transactionId,
    status: 'completed',
  });
});

// Example 3: Mixing with() and chaining
const createOrder = task('create-order', async (ctx, orderData: Partial<Order>) => {
  // Start with bulk setting
  ctx.log.tag
    .with({
      requestId: ctx.requestId,
      userId: ctx.userId || 'guest',
      operation: 'INSERT',
    })
    // Then chain specific values
    .orderId(orderData.id!)
    .amount(orderData.total!)
    .currency(orderData.currency!)
    .paymentMethod(orderData.paymentMethod!)
    .status('pending');

  // Feature flags work alongside chaining
  if (ctx.ff.fraudDetection) {
    await ctx.span('fraud-check', async (childCtx) => {
      childCtx.log.tag
        .orderId(orderData.id!)
        .operation('SELECT')
        .status('pending');

      // Fraud check logic...
      return childCtx.ok({ safe: true });
    });
  }

  // More chaining after async operations
  ctx.log.tag
    .status('completed')
    .httpStatus(201)
    .duration(25.5);

  return ctx.ok({ created: true, orderId: orderData.id });
});

// Run examples
async function runExamples() {
  const requestCtx = createRequestContext(
    {
      requestId: 'req-' + Date.now(),
      userId: 'user-456',
    },
    featureFlags,
    flagEvaluator,
    environmentConfig
  );

  console.log('\n=== Example 1: Simple Chaining ===');
  console.log('Writing to columnar buffers...');
  const validation = await validateOrder(requestCtx, 'order-789');
  console.log('✅ Result:', validation);

  console.log('\n=== Example 2: Real-world Pattern (orderId/amount) ===');
  console.log('Each chained call writes to a separate row in Arrow columns...');
  const payment = await processPayment(requestCtx, {
    id: 'order-789',
    total: 149.99,
    currency: 'USD',
    paymentMethod: 'card',
  });
  console.log('✅ Result:', payment);

  console.log('\n=== Example 3: Mixing with() and Chaining ===');
  console.log('Bulk attributes written, then individual columns appended...');
  const creation = await createOrder(requestCtx, {
    id: 'order-999',
    total: 299.99,
    currency: 'EUR',
    paymentMethod: 'paypal',
  });
  console.log('✅ Result:', creation);

  console.log('\n💾 All data stored in Arrow columnar format:');
  console.log('   - Each attribute has its own typed column (array)');
  console.log('   - Operations tracked with entry type codes');
  console.log('   - Timestamps stored as Float64 arrays');
  console.log('   - Child spans create tree structure with parent references');
  console.log('   - Zero-copy serialization ready for Parquet export\n');
}

console.log('🔗 Method Chaining Showcase - Columnar Buffer Storage\n');
runExamples().catch(console.error);
