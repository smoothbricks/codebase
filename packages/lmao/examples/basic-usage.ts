/**
 * Example: Basic LMAO Integration Pattern with Method Chaining
 *
 * This example demonstrates:
 * - Creating trace context via module.traceContext() (per spec 01l)
 * - Using the defineModule().ctx<Extra>().make() builder pattern
 * - Defining tag attributes for structured logging
 * - Using op() for typed span context
 * - Accessing feature flags and environment config
 * - METHOD CHAINING: ctx.tag.userId(id).requestId(req).operation('INSERT')
 * - Chaining with with(): ctx.tag.with({...}).operation('SELECT')
 * - Creating child spans with chained tags
 *
 * Key Feature: All tag methods return the tag object for fluent chaining!
 */

import { defineFeatureFlags, defineLogSchema, defineModule, InMemoryFlagEvaluator, S } from '../src/index.js';

// 1. Define tag attributes for your domain
// Using the three string types per specs/01a_trace_schema_system.md:
// - S.enum: Known values at compile time (Uint8Array, 1 byte)
// - S.category: Values that often repeat (Uint32Array with string interning)
// - S.text: Unique values (no dictionary overhead)
const dbAttributes = defineLogSchema({
  requestId: S.category(), // Category: request IDs repeat within traces
  userId: S.category(), // Category: user IDs repeat across operations
  duration: S.number(),
  httpStatus: S.number(),
  operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']), // Enum: known DB operations
  query: S.text(), // Text: SQL queries are mostly unique
  region: S.category(), // Category: AWS regions have limited cardinality
});

// 2. Define feature flags
const featureFlags = defineFeatureFlags({
  advancedValidation: S.boolean().default(false).sync(),
  maxRetries: S.number().default(3).sync(),
  experimentalFeature: S.boolean().default(false).async(),
});

// 3. Define environment config type
interface EnvConfig {
  awsRegion: string;
  maxConnections: number;
  databaseUrl: string;
  debug: boolean;
}

// 4. Define Extra context properties
interface ExtraContext {
  env: EnvConfig;
  requestId: string;
  userId: string;
}

// 5. Create feature flag evaluator (would be LaunchDarkly, database, etc. in production)
const flagEvaluator = new InMemoryFlagEvaluator({
  advancedValidation: true,
  maxRetries: 5,
  experimentalFeature: false,
});

// 6. Create module with defineModule().ctx<Extra>().make() pattern
// Per spec 01l - this is the ONLY way to create modules
const userModule = defineModule({
  metadata: {
    gitSha: 'abc123def456',
    packageName: '@example/user-service',
    packagePath: 'src/services/user.ts',
  },
  logSchema: dbAttributes,
  ff: featureFlags,
})
  .ctx<ExtraContext>({
    env: null!, // Required - will be provided at traceContext() time
    requestId: null!, // Required
    userId: null!, // Required
  })
  .make({ ffEvaluator: flagEvaluator });

// 7. Define ops with typed context
interface UserData {
  email: string;
  name: string;
}

// Op function using module.op()
const createUser = userModule.op('create-user', async (ctx, userData: UserData) => {
  // Feature flag access (sync flags are properties)
  if (ctx.ff.advancedValidation) {
    ctx.log.info('Using advanced validation');
  }

  // Environment access (from Extra context)
  const region = ctx.env.awsRegion;
  const _maxConnections = ctx.env.maxConnections;

  // METHOD CHAINING: Each tag method returns the tag object for chaining
  ctx.tag.requestId(ctx.requestId).userId(userData.email).operation('INSERT').region(region);

  // Can also chain with with() method
  ctx.tag
    .with({
      httpStatus: 200,
      duration: 5.0,
    })
    .query('BEGIN TRANSACTION');

  // Child span for validation - uses inline closure pattern
  const validation = await ctx.span('validate-user', async (childCtx) => {
    // Chaining works in child spans too
    childCtx.tag.operation('SELECT').query('SELECT COUNT(*) FROM users WHERE email = ?').duration(12.5).httpStatus(200);

    // Simulate validation
    const existingUser = false;

    if (existingUser) {
      return childCtx.err('USER_EXISTS', { email: userData.email });
    }

    return childCtx.ok({ valid: true });
  });

  if (!validation.success) {
    return ctx.err('VALIDATION_FAILED', { reason: 'child failed' });
  }

  // Simulate database operation with chained tags
  ctx.tag.operation('INSERT').query('INSERT INTO users (email, name) VALUES (?, ?)').duration(50.3).httpStatus(201);

  return ctx.ok({
    id: 'user-123',
    ...userData,
    createdAt: new Date().toISOString(),
  });
});

// 7. Use in request handler
async function handleRequest() {
  // Method 1: Use createRequestContext (backward-compatible)
  const requestCtx = createRequestContext(
    {
      requestId: `req-${Date.now()}`,
      userId: 'user-456',
    },
    featureFlags,
    flagEvaluator,
    environmentConfig,
  );

  console.log('\n📊 Request context created (via createRequestContext):', {
    requestId: requestCtx.requestId,
    traceId: requestCtx.traceId,
  });

  // Execute task - all data is written to Arrow columnar buffers in memory
  const result = await createUser(requestCtx, {
    email: 'john@example.com',
    name: 'John Doe',
  });

  if (result.success) {
    console.log('✅ User created successfully:', result.value);
    console.log('\n💾 All trace data written to Arrow columnar buffers in memory');
    console.log('   - Tag attributes stored in typed columns (Utf8, Float64, etc.)');
    console.log('   - Feature flag accesses tracked in separate columns');
    console.log('   - Message logs stored with timestamps');
    console.log('   - Tree structure maintained via parent/child buffer references\n');
  } else {
    console.error('❌ Failed to create user:', result.error);
  }
}

// Run example
console.log('LMAO Integration Example - Columnar Buffer Storage\n');
console.log('Running with module.traceContext()...');
handleRequest().catch(console.error);
