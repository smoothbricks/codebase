/**
 * Complete LMAO Example - Demonstrates All Features
 *
 * This example shows:
 * - Request context with feature flags and environment
 * - Module context with tag attributes
 * - Task wrappers with typed logging
 * - Scoped attributes inheritance (spec 01i)
 * - Child spans
 * - FlushScheduler with Arrow conversion
 * - String type system (enum/category/text)
 */

import {
  categoryInterner,
  createModuleContext,
  createRequestContext,
  defineFeatureFlags,
  defineTagAttributes,
  FlushScheduler,
  InMemoryFlagEvaluator,
  moduleIdInterner,
  S,
  spanNameInterner,
  textStringStorage,
} from '../src/index.js';

// ====================
// 1. Define Tag Attributes
// ====================

// Using the three string types per specs/01a_trace_schema_system.md:
// - S.enum(): Known values at compile time → Uint8Array (1 byte)
// - S.category(): Values that repeat → Uint32Array with string interning
// - S.text(): Unique values → Uint32Array without interning

const orderSchema = defineTagAttributes({
  // Enum: Known operations at compile time
  operation: S.enum(['CREATE_ORDER', 'UPDATE_ORDER', 'CANCEL_ORDER', 'PROCESS_PAYMENT']),

  // Enum: HTTP methods
  httpMethod: S.enum(['GET', 'POST', 'PUT', 'DELETE']),

  // Category: IDs that repeat across operations
  orderId: S.category(),
  userId: S.category(),
  requestId: S.category(),

  // Text: Unique values that rarely repeat
  errorMessage: S.text(),
  sqlQuery: S.text(),

  // Numbers and booleans
  orderAmount: S.number(),
  itemCount: S.number(),
  isValid: S.boolean(),
  httpStatus: S.number(),
});

// ====================
// 2. Define Feature Flags
// ====================

const featureFlags = defineFeatureFlags({
  // Sync flags (synchronous access)
  advancedValidation: S.boolean().default(false).sync(),
  maxRetries: S.number().default(3).sync(),

  // Async flags (requires await)
  experimentalPaymentFlow: S.boolean().default(false).async(),
});

// ====================
// 3. Setup Environment
// ====================

const environmentConfig = {
  awsRegion: 'us-east-1',
  paymentProvider: 'stripe',
  databaseUrl: 'postgresql://localhost:5432/orders',
};

// ====================
// 4. Create Module Context
// ====================

const orderModule = createModuleContext({
  moduleMetadata: {
    gitSha: 'abc123def456',
    filePath: 'src/services/order-service.ts',
    moduleName: 'order-service',
  },
  tagAttributes: orderSchema,
});

// ====================
// 5. Setup FlushScheduler
// ====================

const scheduler = new FlushScheduler(
  // Flush handler - receives Arrow table
  (table, metadata) => {
    console.log('\n📊 Flushing trace data:');
    console.log(`   Rows: ${metadata.totalRows}`);
    console.log(`   Buffers: ${metadata.totalBuffers}`);
    console.log(`   Reason: ${metadata.flushReason}`);
    console.log(`   Arrow table columns: ${table.schema.fields.map((f) => f.name).join(', ')}`);

    // In production, you would:
    // - Write to ClickHouse
    // - Write to Parquet file
    // - Send to analytics service
    // - etc.
  },
  categoryInterner,
  textStringStorage,
  moduleIdInterner,
  spanNameInterner,
  {
    maxFlushInterval: 5000, // Flush every 5 seconds
    capacityThreshold: 0.8, // Flush when 80% full
    idleDetection: true, // Flush when idle
    idleTimeout: 2000, // 2 seconds of idle
  },
);

// Start scheduler
scheduler.start();

// ====================
// 6. Define Business Logic Tasks
// ====================

// Main order creation task
const createOrder = orderModule.task(
  'create-order',
  async (
    ctx,
    orderData: {
      userId: string;
      items: Array<{ productId: string; quantity: number; price: number }>;
    },
  ) => {
    // Set request-level scoped attributes
    // Per specs/01i - these propagate to all child operations
    ctx.log.scope({
      userId: orderData.userId,
      requestId: ctx.requestId,
      httpMethod: 'POST',
      operation: 'CREATE_ORDER',
    });

    ctx.log.info('Order creation started');

    // Calculate totals
    const orderAmount = orderData.items.reduce((sum, item) => sum + item.price * item.quantity, 0);
    const itemCount = orderData.items.reduce((sum, item) => sum + item.quantity, 0);

    // Add order-specific scope
    ctx.log.scope({
      orderAmount,
      itemCount,
    });

    // Validate order
    const validationResult = await validateOrder(ctx, orderData);

    if (!validationResult.success) {
      ctx.log.error('Order validation failed');
      return ctx.err('VALIDATION_FAILED', validationResult.error);
    }

    ctx.log.info('Order validated successfully');

    // Create order in database
    const orderId = `ORD-${Date.now()}`;

    ctx.log.scope({
      orderId,
      isValid: true,
    });

    // Process payment
    const paymentResult = await processPayment(ctx, orderId, orderAmount);

    if (!paymentResult.success) {
      ctx.log.error('Payment processing failed');
      return ctx.err('PAYMENT_FAILED', paymentResult.error);
    }

    ctx.log.info('Order created successfully');

    return ctx.ok({
      orderId,
      userId: orderData.userId,
      amount: orderAmount,
      status: 'created',
    });
  },
);

// Validation task - demonstrates child span
const validateOrder = orderModule.task(
  'validate-order',
  async (
    ctx,
    orderData: {
      userId: string;
      items: Array<{ productId: string; quantity: number; price: number }>;
    },
  ) => {
    // Check advanced validation feature flag
    if (ctx.ff.advancedValidation) {
      ctx.log.info('Using advanced validation');

      return await ctx.span('advanced-validation', async (childCtx) => {
        // Child span inherits parent's scoped attributes (userId, requestId, etc.)
        childCtx.log.info('Running advanced validation checks');

        // Add validation-specific scope
        childCtx.log.scope({
          operation: 'UPDATE_ORDER', // Can override parent scope
          sqlQuery: 'SELECT * FROM products WHERE id IN (...)',
        });

        // Simulate validation
        if (orderData.items.length === 0) {
          return childCtx.err('EMPTY_ORDER', 'Order has no items');
        }

        return childCtx.ok({ validated: true });
      });
    }

    // Basic validation
    ctx.log.info('Using basic validation');

    if (orderData.items.length === 0) {
      return ctx.err('EMPTY_ORDER', 'Order has no items');
    }

    return ctx.ok({ validated: true });
  },
);

// Payment processing task - demonstrates async feature flags
const processPayment = orderModule.task('process-payment', async (ctx, orderId: string, amount: number) => {
  ctx.log.scope({
    orderId,
    orderAmount: amount,
    operation: 'PROCESS_PAYMENT',
  });

  ctx.log.info('Payment processing started');

  // Check experimental payment flow (async)
  const useExperimentalFlow = await ctx.ff.get('experimentalPaymentFlow');

  if (useExperimentalFlow) {
    ctx.log.info('Using experimental payment flow');

    return await ctx.span('experimental-payment', async (childCtx) => {
      childCtx.log.info('Processing payment with new flow');

      // Simulate payment
      const paymentId = `PAY-${Date.now()}`;

      childCtx.log.scope({
        httpStatus: 200,
        httpMethod: 'POST',
      });

      return childCtx.ok({ paymentId, status: 'success' });
    });
  }

  // Standard payment flow
  ctx.log.info('Using standard payment flow');

  // Simulate payment
  const paymentId = `PAY-${Date.now()}`;

  ctx.log.scope({
    httpStatus: 200,
  });

  return ctx.ok({ paymentId, status: 'success' });
});

// ====================
// 7. Run Example
// ====================

async function main() {
  console.log('🚀 LMAO Complete Example\n');
  console.log('This example demonstrates:');
  console.log('- Request context with feature flags');
  console.log('- Scoped attributes inheritance');
  console.log('- Child spans');
  console.log('- FlushScheduler with Arrow conversion');
  console.log('- String type system (enum/category/text)\n');

  // Create flag evaluator
  const flagEvaluator = new InMemoryFlagEvaluator({
    advancedValidation: true,
    maxRetries: 5,
    experimentalPaymentFlow: false,
  });

  // Create request context
  const requestCtx = createRequestContext(
    {
      requestId: 'req-12345',
      userId: 'user-789',
    },
    featureFlags,
    flagEvaluator,
    environmentConfig,
  );

  console.log('📦 Processing order...\n');

  // Execute task
  const result = await createOrder(requestCtx, {
    userId: 'user-789',
    items: [
      { productId: 'prod-1', quantity: 2, price: 29.99 },
      { productId: 'prod-2', quantity: 1, price: 49.99 },
    ],
  });

  if (result.success) {
    console.log('\n✅ Order created successfully!');
    console.log('   Order ID:', result.value.orderId);
    console.log('   Amount: $' + result.value.amount.toFixed(2));
  } else {
    console.log('\n❌ Order creation failed:');
    console.log('   Error:', result.error);
  }

  // Manually flush to see the data
  console.log('\n🔄 Manually flushing trace data...');
  await scheduler.flush();

  // Stop scheduler
  scheduler.stop();

  console.log('\n✨ Example complete!\n');
}

// Run if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`.replace(/\\/g, '/')) {
  main().catch(console.error);
}
