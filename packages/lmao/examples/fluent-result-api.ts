/**
 * Example: Fluent Result API with Span Lifecycle
 *
 * Demonstrates the new fluent API for ctx.ok() and ctx.err()
 * with automatic span lifecycle tracking (span-start, span-ok, span-err, span-exception)
 */

import {
  createModuleContext,
  createRequestContext,
  defineFeatureFlags,
  defineTagAttributes,
  InMemoryFlagEvaluator,
  S,
} from '../src/index.js';

// Define schema for user management operations
// Note: resultMessage/exceptionMessage are not needed - use the unified .message() API
// which writes to the system 'message' column
const userSchema = defineTagAttributes({
  userId: S.category(),
  email: S.category(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  duration: S.number(),
  errorCode: S.category(),
});

const featureFlags = defineFeatureFlags({
  enableUserValidation: S.boolean().default(true).sync(),
  enableEmailNotifications: S.boolean().default(false).sync(),
});

// Mock evaluator
const mockEvaluator = new InMemoryFlagEvaluator({
  enableUserValidation: true,
  enableEmailNotifications: false,
});

// Create module context
const userModule = createModuleContext({
  moduleMetadata: {
    gitSha: 'abc123',
    filePath: '/src/modules/user.ts',
    moduleName: 'UserModule',
  },
  tagAttributes: userSchema,
});

// Example 1: Success with fluent API
const createUser = userModule.task('createUser', async (ctx, email: string, name: string) => {
  // Span-start entry is automatically written at task start

  const startTime = Date.now();

  // Simulate user creation
  const userId = `user_${Math.random().toString(36).substr(2, 9)}`;

  // Log during operation
  ctx.tag.userId(userId).email(email).operation('CREATE');

  ctx.log.info(`Creating user with email: ${email}`);

  const duration = Date.now() - startTime;

  // Fluent result with attributes and message
  // Writes span-ok entry with all attributes
  return ctx
    .ok({ userId, email, name })
    .with({ userId, email, operation: 'CREATE', duration })
    .message('User created successfully');
});

// Example 2: Error with fluent API
const validateEmail = userModule.task('validateEmail', async (ctx, email: string) => {
  // Span-start entry is automatically written

  if (!email.includes('@')) {
    // Fluent error result
    // Writes span-err entry with error code and attributes
    return ctx
      .err('INVALID_EMAIL', { field: 'email', reason: 'Missing @ symbol' })
      .with({ email, operation: 'READ' })
      .message('Email validation failed');
  }

  return ctx.ok({ valid: true }).with({ email, operation: 'READ' }).message('Email is valid');
});

// Example 3: Exception handling
const updateUser = userModule.task('updateUser', async (ctx, userId: string, updates: Record<string, unknown>) => {
  // Span-start entry is automatically written

  // Simulate exception
  if (userId === 'invalid') {
    throw new Error('User not found in database');
    // Span-exception entry is automatically written with stack trace
  }

  return ctx.ok({ updated: true }).with({ userId, operation: 'UPDATE' }).message('User updated successfully');
});

// Example 4: Child spans with lifecycle tracking
const processUserRegistration = userModule.task('processUserRegistration', async (ctx, email: string, name: string) => {
  // Parent span-start

  // Child span 1: Validate email
  const validationResult = await ctx.span('validateEmail', async (childCtx) => {
    // Child span-start
    const isValid = email.includes('@') && email.includes('.');

    if (!isValid) {
      // Child span-err
      return childCtx
        .err('INVALID_EMAIL', { email })
        .with({ email, operation: 'READ' })
        .message('Email format is invalid');
    }

    // Child span-ok
    return childCtx.ok({ valid: true }).with({ email, operation: 'READ' }).message('Email validation passed');
  });

  if (!validationResult.success) {
    // Parent span-err
    return ctx
      .err('VALIDATION_FAILED', validationResult.error)
      .with({ email, operation: 'CREATE' })
      .message('User registration failed validation');
  }

  // Child span 2: Create user
  const createResult = await ctx.span('createUser', async (childCtx) => {
    // Child span-start
    const userId = `user_${Math.random().toString(36).substr(2, 9)}`;

    // Child span-ok
    return childCtx
      .ok({ userId, email, name })
      .with({ userId, email, operation: 'CREATE' })
      .message('User created in database');
  });

  // Parent span-ok
  return ctx.ok(createResult).with({ email, operation: 'CREATE' }).message('User registration completed successfully');
});

// Run examples
async function main() {
  const requestCtx = createRequestContext({ requestId: 'req_001', userId: 'admin' }, featureFlags, mockEvaluator, {
    environment: 'development',
  });

  console.log('=== Example 1: Success with Fluent API ===');
  const user = await createUser(requestCtx, 'john@example.com', 'John Doe');
  console.log('Result:', user);
  console.log();

  console.log('=== Example 2: Validation Error ===');
  const validResult = await validateEmail(requestCtx, 'invalid-email');
  console.log('Result:', validResult);
  console.log();

  console.log('=== Example 3: Exception Handling ===');
  try {
    await updateUser(requestCtx, 'invalid', { name: 'New Name' });
  } catch (error) {
    console.log('Caught exception:', (error as Error).message);
  }
  console.log();

  console.log('=== Example 4: Nested Spans with Lifecycle ===');
  const registration = await processUserRegistration(requestCtx, 'jane@example.com', 'Jane Smith');
  console.log('Result:', registration);
}

main().catch(console.error);
