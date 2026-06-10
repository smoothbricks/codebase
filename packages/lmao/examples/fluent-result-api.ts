#!/usr/bin/env bun
/**
 * Example: Fluent result API and span lifecycle
 *
 * Demonstrates:
 * - `ctx.ok(value).with({...}).message('...')` — writes the span-ok entry
 * - `ctx.err(CODE({...})).with({...}).message('...')` — writes the span-err entry
 * - Typed error codes via `defineCodeError`
 * - Reading results: `result.success ? result.value : result.error.code`
 * - Exceptions thrown inside an op become span-exception entries
 * - Child spans with their own ok/err lifecycle
 *
 * Run it:
 *   bun run examples/fluent-result-api.ts
 */

import {
  createTraceRoot,
  defineCodeError,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  S,
  StdioTracer,
} from '../src/node.js';

const userSchema = defineLogSchema({
  userId: S.category(),
  email: S.category(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  duration: S.number(),
});

const { defineOp } = defineOpContext({ logSchema: userSchema });

// Typed error codes — the payload shape is checked at the call site.
const INVALID_EMAIL = defineCodeError('INVALID_EMAIL')<{ email: string; reason: string }>();
const VALIDATION_FAILED = defineCodeError('VALIDATION_FAILED')<{ email: string }>();

// 1. Success with fluent attributes + message.
const createUser = defineOp('createUser', async (ctx, email: string, name: string) => {
  const start = performance.now();
  const userId = `user_${Math.random().toString(36).slice(2, 11)}`;

  ctx.tag.userId(userId).email(email).operation('CREATE');
  ctx.log.info('Creating user {{email}}').email(email);

  return ctx
    .ok({ userId, email, name })
    .with({ duration: performance.now() - start })
    .message('User created successfully');
});

// 2. Error with a typed code.
const validateEmail = defineOp('validateEmail', async (ctx, email: string) => {
  ctx.tag.email(email).operation('READ');

  if (!email.includes('@')) {
    return ctx
      .err(INVALID_EMAIL({ email, reason: 'missing @ symbol' }))
      .message('Email validation failed');
  }
  return ctx.ok({ valid: true }).message('Email is valid');
});

// 3. Exception handling — a thrown error is recorded as a span-exception and re-thrown.
const updateUser = defineOp('updateUser', async (ctx, userId: string) => {
  ctx.tag.userId(userId).operation('UPDATE');
  if (userId === 'invalid') {
    throw new Error('User not found in database');
  }
  return ctx.ok({ updated: true }).message('User updated');
});

// 4. Parent op composing child spans, each with its own lifecycle.
const registerUser = defineOp('registerUser', async (ctx, email: string, name: string) => {
  const validation = await ctx.span('validate-email', async (childCtx) => {
    childCtx.tag.email(email).operation('READ');
    if (!email.includes('@') || !email.includes('.')) {
      return childCtx.err(INVALID_EMAIL({ email, reason: 'invalid format' })).message('Email format invalid');
    }
    return childCtx.ok({ valid: true }).message('Email valid');
  });

  if (!validation.success) {
    return ctx.err(VALIDATION_FAILED({ email })).message('Registration failed validation');
  }

  const created = await ctx.span('create-user', async (childCtx) => {
    const userId = `user_${Math.random().toString(36).slice(2, 11)}`;
    childCtx.tag.userId(userId).email(email).operation('CREATE');
    return childCtx.ok({ userId, email, name }).message('User created in database');
  });

  return ctx.ok(created.success ? created.value : { email }).message('Registration completed');
});

const { trace } = new StdioTracer(defineOpContext({ logSchema: userSchema }), {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});

async function main(): Promise<void> {
  console.log('=== 1. Success ===');
  const created = await trace('createUser', createUser, 'john@example.com', 'John Doe');
  console.log(created.success ? created.value : created.error);

  console.log('\n=== 2. Validation error ===');
  const invalid = await trace('validateEmail', validateEmail, 'not-an-email');
  if (!invalid.success) {
    console.log('error code:', invalid.error.code, '| email:', invalid.error.email);
  }

  console.log('\n=== 3. Exception ===');
  // A thrown error is recorded as a span-exception entry and then re-thrown.
  try {
    await trace('updateUser', updateUser, 'invalid');
  } catch (error) {
    console.log('caught exception:', error instanceof Error ? error.message : String(error));
  }

  console.log('\n=== 4. Nested spans ===');
  const registration = await trace('registerUser', registerUser, 'jane@example.com', 'Jane Smith');
  console.log(registration.success ? registration.value : registration.error);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
