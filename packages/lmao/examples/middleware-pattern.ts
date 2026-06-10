#!/usr/bin/env bun
/**
 * Example: Middleware / request-scope pattern
 *
 * Demonstrates how to set request-level attributes once at the request boundary and have
 * every business operation inherit them — eliminating repetitive context plumbing:
 * - The request boundary (`runRequest`) sets request scope ONCE via `ctx.setScope(...)`
 * - Business ops are invoked as child spans (`ctx.span`), inheriting the request scope
 * - Business ops only add their own domain attributes (operation, resourceId, httpStatus)
 *
 * Run it:
 *   bun run examples/middleware-pattern.ts
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

const apiSchema = defineLogSchema({
  // Request metadata (set once at the request boundary).
  requestId: S.category(),
  userId: S.category(),
  endpoint: S.category(),
  httpMethod: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
  userAgent: S.text(),
  ipAddress: S.category(),
  // Business attributes.
  operation: S.enum(['CREATE_USER', 'UPDATE_USER', 'DELETE_USER', 'GET_USER']),
  resourceId: S.category(),
  httpStatus: S.number(),
});

const featureFlags = defineFeatureFlags({
  rateLimiting: S.boolean().default(true).sync(),
  detailedLogging: S.boolean().default(false).sync(),
});

const opContext = defineOpContext({ logSchema: apiSchema, flags: featureFlags.schema });
const { defineOp } = opContext;

const INVALID_EMAIL = defineCodeError('INVALID_EMAIL')<{ reason: string }>();
const USER_NOT_FOUND = defineCodeError('USER_NOT_FOUND')<{ userId: string }>();
const VALIDATION_FAILED = defineCodeError('VALIDATION_FAILED')<{ reason: string }>();

interface RequestMeta {
  requestId: string;
  userId: string;
  endpoint: string;
  httpMethod: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  userAgent: string;
  ipAddress: string;
}

// The request-level scope, set once at the boundary and inherited by every child span.
function requestScope(req: RequestMeta): Partial<{
  requestId: string;
  userId: string;
  endpoint: string;
  httpMethod: RequestMeta['httpMethod'];
  userAgent: string;
  ipAddress: string;
}> {
  return {
    requestId: req.requestId,
    userId: req.userId,
    endpoint: req.endpoint,
    httpMethod: req.httpMethod,
    userAgent: req.userAgent,
    ipAddress: req.ipAddress,
  };
}

// ── Business ops — focused; they only add domain attributes ──────────────────
interface UserData {
  email: string;
  name: string;
}

const createUser = defineOp('create-user', async (ctx, userData: UserData) => {
  ctx.setScope({ operation: 'CREATE_USER', resourceId: userData.email });
  ctx.log.info('Creating new user'); // inherits requestId/userId/endpoint/... from request scope

  if (!userData.email.includes('@')) {
    return ctx.err(INVALID_EMAIL({ reason: 'missing @' }));
  }
  if (ctx.ff.detailedLogging?.value) {
    ctx.log.info('Detailed logging enabled — checking for duplicate email');
  }

  const userId = `user-${Date.now()}`;
  ctx.setScope({ resourceId: userId, httpStatus: 201 });
  ctx.log.info('User created');
  return ctx.ok({ id: userId, email: userData.email, name: userData.name });
});

const getUser = defineOp('get-user', async (ctx, userId: string) => {
  ctx.setScope({ operation: 'GET_USER', resourceId: userId });
  ctx.log.info('Fetching user');

  if (userId === 'not-found') {
    ctx.setScope({ httpStatus: 404 });
    return ctx.err(USER_NOT_FOUND({ userId }));
  }
  ctx.setScope({ httpStatus: 200 });
  return ctx.ok({ id: userId, email: 'user@example.com', name: 'John Doe' });
});

const updateUser = defineOp('update-user', async (ctx, userId: string, updates: { name?: string; email?: string }) => {
  ctx.setScope({ operation: 'UPDATE_USER', resourceId: userId });
  ctx.log.info('Updating user');

  const validation = await ctx.span('validate-updates', async (childCtx) => {
    childCtx.log.info('Validating update data'); // inherits request + parent scope
    if (updates.email && !updates.email.includes('@')) {
      return childCtx.err(INVALID_EMAIL({ reason: 'missing @' }));
    }
    return childCtx.ok({ validated: true });
  });

  if (!validation.success) {
    return ctx.err(VALIDATION_FAILED({ reason: validation.error.code }));
  }

  ctx.setScope({ httpStatus: 200 });
  ctx.log.info('User updated');
  return ctx.ok({ id: userId, ...updates });
});

// ── Request boundary ("middleware") ──────────────────────────────────────────
const { trace } = new StdioTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
  flagEvaluator: new InMemoryFlagEvaluator(featureFlags.schema, { rateLimiting: true, detailedLogging: true }),
});

async function main(): Promise<void> {
  const baseReq = { userId: 'admin-123', userAgent: 'Mozilla/5.0', ipAddress: '192.168.1.100' };

  console.log('🌐 POST /api/users');
  const created = await trace('POST /api/users', (ctx) => {
    ctx.setScope(requestScope({ ...baseReq, requestId: 'req-001', endpoint: '/api/users', httpMethod: 'POST' }));
    return ctx.span('create-user', createUser, { email: 'newuser@example.com', name: 'New User' });
  });
  console.log(created.success ? `✅ created ${created.value.id}` : `❌ ${created.error.code}`);

  console.log('\n🌐 GET /api/users/123');
  const fetched = await trace('GET /api/users/123', (ctx) => {
    ctx.setScope(requestScope({ ...baseReq, requestId: 'req-002', endpoint: '/api/users/123', httpMethod: 'GET' }));
    return ctx.span('get-user', getUser, 'user-123');
  });
  console.log(fetched.success ? `✅ fetched ${fetched.value.id}` : `❌ ${fetched.error.code}`);

  console.log('\n🌐 PUT /api/users/123');
  const updated = await trace('PUT /api/users/123', (ctx) => {
    ctx.setScope(requestScope({ ...baseReq, requestId: 'req-003', endpoint: '/api/users/123', httpMethod: 'PUT' }));
    return ctx.span('update-user', updateUser, 'user-123', { name: 'Updated Name' });
  });
  console.log(updated.success ? `✅ updated ${updated.value.id}` : `❌ ${updated.error.code}`);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
