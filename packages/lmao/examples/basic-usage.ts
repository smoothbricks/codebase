#!/usr/bin/env bun
/**
 * Example: Basic LMAO usage with fluent tag chaining
 *
 * Demonstrates:
 * - Defining a typed log schema with the three string strategies (enum / category / text)
 * - Feature flags via `defineFeatureFlags` + `InMemoryFlagEvaluator`
 * - User context (`ctx:`) carried into every span and supplied per-trace via overrides
 * - `defineOpContext` -> `defineOp` (the primary API)
 * - Fluent tag chaining: `ctx.tag.userId(id).operation('INSERT')` and `ctx.tag.with({...})`
 * - Child spans via `ctx.span(name, fn)`
 *
 * Run it:
 *   bun run examples/basic-usage.ts
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

// 1. Define tag attributes for your domain.
//    - S.enum:     known values at compile time (1 byte)
//    - S.category: values that repeat (dictionary-encoded)
//    - S.text:     mostly-unique values (no dictionary)
const dbSchema = defineLogSchema({
  requestId: S.category(),
  userId: S.category(),
  duration: S.number(),
  httpStatus: S.number(),
  operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
  query: S.text(),
  region: S.category(),
});

// 2. Define feature flags (sync flags are read as `ctx.ff.flag?.value`).
const featureFlags = defineFeatureFlags({
  advancedValidation: S.boolean().default(false).sync(),
  maxRetries: S.number().default(3).sync(),
});

// 3. User context shape carried alongside every span.
interface EnvConfig {
  awsRegion: string;
  maxConnections: number;
  databaseUrl: string;
}

// 4. Bundle schema + flags + user context into an op context.
const opContext = defineOpContext({
  logSchema: dbSchema,
  flags: featureFlags.schema,
  ctx: {
    requestId: undefined as string | undefined,
    env: undefined as EnvConfig | undefined,
  },
});
const { defineOp } = opContext;

// 5. Error codes are typed factories.
const USER_EXISTS = defineCodeError('USER_EXISTS')<{ email: string }>();
const VALIDATION_FAILED = defineCodeError('VALIDATION_FAILED')<{ reason: string }>();

interface UserData {
  email: string;
  name: string;
}

// 6. Define an op. The body receives a typed SpanContext.
const createUser = defineOp('create-user', async (ctx, userData: UserData) => {
  // Feature flag access (sync): the wrapper is undefined if no evaluator is wired.
  if (ctx.ff.advancedValidation?.value) {
    ctx.log.info('Using advanced validation');
  }

  const region = ctx.env?.awsRegion ?? 'us-east-1';

  // Fluent tag chaining — every setter returns the tag writer.
  ctx.tag.requestId(ctx.requestId ?? 'req-unknown').userId(userData.email).operation('INSERT').region(region);

  // `with()` sets several attributes at once, then chaining continues.
  ctx.tag.with({ httpStatus: 200, duration: 5 }).query('BEGIN TRANSACTION');

  // Child span for validation — inherits scope, gets its own buffer.
  const validation = await ctx.span('validate-user', async (childCtx) => {
    childCtx.tag.operation('SELECT').query('SELECT COUNT(*) FROM users WHERE email = ?').duration(12.5).httpStatus(200);

    const existingUser = false;
    if (existingUser) {
      return childCtx.err(USER_EXISTS({ email: userData.email }));
    }
    return childCtx.ok({ valid: true });
  });

  if (!validation.success) {
    return ctx.err(VALIDATION_FAILED({ reason: 'validation child span failed' }));
  }

  ctx.tag.operation('INSERT').query('INSERT INTO users (email, name) VALUES (?, ?)').duration(50.3).httpStatus(201);

  return ctx.ok({ id: 'user-123', ...userData });
});

// 7. Build a tracer and run. StdioTracer prints the span tree to the console.
const flagEvaluator = new InMemoryFlagEvaluator(featureFlags.schema, {
  advancedValidation: true,
  maxRetries: 5,
});

const { trace } = new StdioTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
  flagEvaluator,
});

async function main(): Promise<void> {
  const result = await trace(
    'create-user',
    {
      requestId: `req-${Date.now()}`,
      env: { awsRegion: 'us-east-1', maxConnections: 100, databaseUrl: 'postgres://localhost:5432/app' },
    },
    createUser,
    { email: 'john@example.com', name: 'John Doe' },
  );

  if (result.success) {
    console.log('✅ User created:', result.value);
  } else {
    console.error('❌ Failed:', result.error.code, result.error);
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
