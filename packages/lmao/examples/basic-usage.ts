/**
 * Example: Basic LMAO Integration Pattern with Method Chaining
 * 
 * This example demonstrates:
 * - Creating request context with feature flags and environment
 * - Defining tag attributes for structured logging
 * - Using task wrappers with typed span context
 * - Accessing feature flags and environment config
 * - METHOD CHAINING: ctx.log.tag.userId(id).requestId(req).operation('INSERT')
 * - Chaining with with(): ctx.log.tag.with({...}).operation('SELECT')
 * - Creating child spans with chained tags
 * 
 * Key Feature: All tag methods return the tag object for fluent chaining!
 */

import { S } from '../src/lib/schema/builder.js';
import { defineTagAttributes } from '../src/lib/schema/defineTagAttributes.js';
import { defineFeatureFlags } from '../src/lib/schema/defineFeatureFlags.js';
import { InMemoryFlagEvaluator } from '../src/lib/schema/evaluator.js';
import { createRequestContext, createModuleContext } from '../src/lib/lmao.js';

// 1. Define tag attributes for your domain
// Using the three string types per specs/01a_trace_schema_system.md:
// - S.enum: Known values at compile time (Uint8Array, 1 byte)
// - S.category: Values that often repeat (Uint32Array with string interning)
// - S.text: Unique values (no dictionary overhead)
const dbAttributes = defineTagAttributes({
  requestId: S.category(),              // Category: request IDs repeat within traces
  userId: S.category(),                 // Category: user IDs repeat across operations
  duration: S.number(),
  httpStatus: S.number(),
  operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),  // Enum: known DB operations
  query: S.text(),                      // Text: SQL queries are mostly unique
  region: S.category(),                 // Category: AWS regions have limited cardinality
});

// 2. Define feature flags
const featureFlags = defineFeatureFlags({
  advancedValidation: S.boolean().default(false).sync(),
  maxRetries: S.number().default(3).sync(),
  experimentalFeature: S.boolean().default(false).async(),
});

// 3. Define environment config (just a plain object)
const environmentConfig = {
  awsRegion: 'us-east-1',
  maxConnections: 100,
  databaseUrl: 'postgresql://localhost:5432/mydb',
  debug: true,
};

// 4. Create feature flag evaluator (would be LaunchDarkly, database, etc. in production)
const flagEvaluator = new InMemoryFlagEvaluator({
  advancedValidation: true,
  maxRetries: 5,
  experimentalFeature: false,
});

// 5. Create module context with tag attributes
// Note: Using the internal createModuleContext API directly
// In production, you'd typically use createLibraryModule for better ergonomics
const { task } = createModuleContext({
  moduleMetadata: {
    gitSha: 'abc123def456',
    filePath: 'src/services/user.ts',
    moduleName: 'UserService',
  },
  tagAttributes: dbAttributes,
});

// 6. Define tasks with typed context
interface UserData {
  email: string;
  name: string;
}

const createUser = task('create-user', async (ctx, userData: UserData) => {
  // Feature flag access (sync flags are properties)
  if (ctx.ff.advancedValidation) {
    ctx.log.info('Using advanced validation');
    
    // Track feature flag usage for analytics
    ctx.ff.trackUsage('advancedValidation', {
      action: 'validation_performed',
      outcome: 'success',
    });
  }

  // Environment access (just plain property access)
  const region = ctx.env.awsRegion;
  const maxConnections = ctx.env.maxConnections;

  // METHOD CHAINING: Each tag method returns the tag object for chaining
  ctx.log.tag
    .requestId(ctx.requestId)
    .userId(userData.email)
    .operation('INSERT')
    .region(region);

  // Can also chain with with() method
  ctx.log.tag
    .with({
      httpStatus: 200,
      duration: 5.0,
    })
    .query('BEGIN TRANSACTION');

  // Child span for validation with chained tags
  const validation = await ctx.span('validate-user', async (childCtx) => {
    // Chaining works in child spans too
    childCtx.log.tag
      .operation('SELECT')
      .query('SELECT COUNT(*) FROM users WHERE email = ?')
      .duration(12.5)
      .httpStatus(200);

    // Simulate validation
    const existingUser = false;

    if (existingUser) {
      return childCtx.err('USER_EXISTS', { email: userData.email });
    }
    
    return childCtx.ok({ valid: true });
  });

  if (!validation.success) {
    return ctx.err('VALIDATION_FAILED', validation.error);
  }

  // Simulate database operation with chained tags
  ctx.log.tag
    .operation('INSERT')
    .query('INSERT INTO users (email, name) VALUES (?, ?)')
    .duration(50.3)
    .httpStatus(201);

  return ctx.ok({ 
    id: 'user-123', 
    ...userData,
    createdAt: new Date().toISOString(),
  });
});

// 7. Use in request handler
async function handleRequest() {
  // Create request context at request boundary
  const requestCtx = createRequestContext(
    { 
      requestId: 'req-' + Date.now(),
      userId: 'user-456',
    },
    featureFlags,
    flagEvaluator,
    environmentConfig
  );

  console.log('\n📊 Request context created:', {
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
console.log('🚀 LMAO Integration Example - Columnar Buffer Storage\n');
handleRequest().catch(console.error);
