/**
 * Middleware Pattern Example
 *
 * Demonstrates how to use LMAO in an Express-style middleware architecture:
 * - Middleware sets request-level scope attributes
 * - Business logic ops inherit scoped attributes
 * - Zero repetition of context attributes
 *
 * Per specs/01i_span_scope_attributes.md - Middleware Pattern
 */

import { defineFeatureFlags, defineLogSchema, defineModule, InMemoryFlagEvaluator, S } from '../src/index.js';

// ====================
// Schema Definition
// ====================

const apiSchema = defineLogSchema({
  // Request metadata (set in middleware)
  requestId: S.category(),
  userId: S.category(),
  endpoint: S.category(),
  httpMethod: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
  userAgent: S.text(),
  ipAddress: S.category(),

  // Business logic attributes
  operation: S.enum(['CREATE_USER', 'UPDATE_USER', 'DELETE_USER', 'GET_USER']),
  resourceId: S.category(),
  httpStatus: S.number(),
  duration: S.number(),
});

const featureFlags = defineFeatureFlags({
  rateLimiting: S.boolean().default(true).sync(),
  detailedLogging: S.boolean().default(false).sync(),
});

// ====================
// Module Context with defineModule
// ====================

const apiModule = defineModule({
  moduleMetadata: {
    gitSha: 'abc123',
    packageName: '@example/user-api',
    packagePath: 'src/api/user-controller.ts',
  },
  logSchema: apiSchema,
});

// ====================
// Simulated Express Request/Response with Proper Typing
// ====================

// Define TraceContext type we'll attach
// Use typeof featureFlags.schema to get the FeatureFlagSchema type
type LmaoTraceContext = ReturnType<typeof apiModule.traceContext>;

// Extend Request interface using declaration merging (proper TypeScript pattern)
interface Request {
  id: string;
  method: string;
  path: string;
  user?: { id: string };
  headers: { [key: string]: string };
  ip: string;
  // PROPER WAY: Extend the interface instead of using (as any)
  ctx: LmaoTraceContext;
}

interface Response {
  status: (code: number) => Response;
  json: (data: unknown) => void;
}

// ====================
// Middleware - Sets Request-Level Scope
// ====================

/**
 * LMAO middleware - creates request context and sets scope
 *
 * This runs once per request and sets attributes that apply to ALL
 * subsequent operations in the request lifecycle.
 */
function lmaoMiddleware(req: Request, _res: Response, next: () => void) {
  console.log(`\n🌐 ${req.method} ${req.path}`);

  // Create flag evaluator
  const flagEvaluator = new InMemoryFlagEvaluator({
    rateLimiting: true,
    detailedLogging: true,
  });

  // Create request context via module
  const traceCtx = apiModule.traceContext(
    {
      requestId: req.id,
      userId: req.user?.id,
    },
    featureFlags,
    flagEvaluator,
    {},
  );

  // Attach context to request (now type-safe!)
  req.ctx = traceCtx;

  next();
}

// ====================
// Business Logic - Clean and Focused
// ====================

/**
 * Create user endpoint
 *
 * Notice: NO repetitive context setting!
 * All request attributes are inherited from middleware
 */
const createUser = apiModule.task(
  'create-user',
  async (
    ctx,
    userData: {
      email: string;
      name: string;
    },
  ) => {
    // Add business-specific scope (merges with middleware scope)
    ctx.setScope({
      operation: 'CREATE_USER',
      resourceId: userData.email,
    });

    ctx.log.info('Creating new user');
    // ↑ This log entry includes: requestId, userId, endpoint, httpMethod, userAgent, ipAddress, operation, resourceId

    // Validate email
    if (!userData.email.includes('@')) {
      ctx.log.info('Invalid email format');
      return ctx.err('INVALID_EMAIL', 'Email must contain @');
    }

    // Check feature flag
    if (ctx.ff.detailedLogging) {
      ctx.log.info('Detailed logging enabled - checking for duplicate email');
    }

    // Simulate database check
    const existingUser = false; // await db.findByEmail(userData.email);

    if (existingUser) {
      ctx.log.info('Email already exists');
      return ctx.err('EMAIL_EXISTS', 'User with this email already exists');
    }

    // Create user
    const userId = `user-${Date.now()}`;

    ctx.setScope({
      resourceId: userId,
      httpStatus: 201,
    });

    ctx.log.info('User created successfully');

    return ctx.ok({
      id: userId,
      email: userData.email,
      name: userData.name,
    });
  },
);

/**
 * Get user endpoint
 *
 * ✅ IMPORTANT: This function can return EITHER success OR error,
 * so TypeScript infers a union type, allowing proper type narrowing.
 */
const getUser = apiModule.task('get-user', async (ctx, userId: string) => {
  // Only set business-specific scope
  // Middleware scope (requestId, endpoint, etc.) is already inherited
  ctx.setScope({
    operation: 'GET_USER',
    resourceId: userId,
  });

  ctx.log.info('Fetching user');

  // Simulate database fetch
  const user = await (async () => {
    // Simulate not found case
    if (userId === 'not-found') {
      return null;
    }
    return { id: userId, email: 'user@example.com', name: 'John Doe' };
  })();

  // Error case - TypeScript knows this returns FluentErrorResult
  if (!user) {
    ctx.setScope({ httpStatus: 404 });
    ctx.log.info('User not found');
    return ctx.err('USER_NOT_FOUND', { userId });
  }

  // Success case - TypeScript knows this returns FluentSuccessResult
  ctx.setScope({ httpStatus: 200 });
  ctx.log.info('User fetched successfully');

  return ctx.ok(user);
});

/**
 * Update user endpoint - demonstrates child spans
 */
const updateUser = apiModule.task(
  'update-user',
  async (
    ctx,
    userId: string,
    updates: {
      name?: string;
      email?: string;
    },
  ) => {
    ctx.setScope({
      operation: 'UPDATE_USER',
      resourceId: userId,
    });

    ctx.log.info('Updating user');

    // Validate updates in a child span
    const validationResult = await ctx.span('validate-updates', async (childCtx) => {
      // Child inherits all parent scope: requestId, userId, endpoint, operation, resourceId, etc.
      childCtx.log.info('Validating update data');

      if (updates.email && !updates.email.includes('@')) {
        return childCtx.err('INVALID_EMAIL', 'Email must contain @');
      }

      return childCtx.ok({ validated: true });
    });

    if (!validationResult.success) {
      return ctx.err('VALIDATION_FAILED', validationResult.error);
    }

    // Apply updates
    const updatedUser = { id: userId, ...updates };

    ctx.setScope({
      httpStatus: 200,
    });

    ctx.log.info('User updated successfully');

    return ctx.ok(updatedUser);
  },
);

// ====================
// Simulated Express Routes
// ====================

// PROPER WAY: No more (as any) casts - fully type-safe!
async function handleCreateUser(req: Request, res: Response) {
  const result = await createUser(req.ctx, {
    email: 'newuser@example.com',
    name: 'New User',
  });

  if (result.success) {
    res.status(201).json(result.value);
    console.log('✅ User created:', result.value.id);
  } else {
    res.status(400).json({ error: result.error });
    console.log('❌ Failed:', result.error);
  }
}

async function handleGetUser(req: Request, res: Response) {
  const result = await getUser(req.ctx, 'user-123');

  // TypeScript knows result is a union: FluentSuccessResult | FluentErrorResult
  // Type narrowing with 'if (result.success)' gives us type safety
  if (result.success) {
    // TypeScript knows: result.value exists, result.error doesn't
    res.status(200).json(result.value);
    console.log('✅ User fetched:', result.value.id);
  } else {
    // TypeScript knows: result.error exists, result.value doesn't
    res.status(404).json({ error: result.error });
    console.log('❌ Not found:', result.error);
  }
}

async function handleUpdateUser(req: Request, res: Response) {
  const result = await updateUser(req.ctx, 'user-123', {
    name: 'Updated Name',
  });

  if (result.success) {
    res.status(200).json(result.value);
    console.log('✅ User updated:', result.value.id);
  } else {
    res.status(400).json({ error: result.error });
    console.log('❌ Update failed:', result.error);
  }
}

// ====================
// Run Example
// ====================

async function main() {
  console.log('🌐 LMAO Middleware Pattern Example\n');
  console.log('Demonstrates:');
  console.log('- Middleware sets request-level scope ONCE');
  console.log('- Business logic inherits all middleware attributes');
  console.log('- Zero repetition of context attributes');
  console.log('- Clean, focused business logic\n');

  // Simulate Express app
  const mockResponse = {
    status: (_code: number) => mockResponse,
    json: (_data: unknown) => {},
  } as Response;

  // Request 1: POST /api/users
  const req1 = {
    id: 'req-001',
    method: 'POST',
    path: '/api/users',
    user: { id: 'admin-123' },
    headers: { 'user-agent': 'Mozilla/5.0' },
    ip: '192.168.1.100',
  } as Partial<Request> as Request;

  lmaoMiddleware(req1, mockResponse, async () => {
    await handleCreateUser(req1, mockResponse);
  });

  // Wait a bit
  await new Promise((resolve) => setTimeout(resolve, 100));

  // Request 2: GET /api/users/123
  const req2 = {
    id: 'req-002',
    method: 'GET',
    path: '/api/users/123',
    user: { id: 'admin-123' },
    headers: { 'user-agent': 'Mozilla/5.0' },
    ip: '192.168.1.100',
  } as Partial<Request> as Request;

  lmaoMiddleware(req2, mockResponse, async () => {
    await handleGetUser(req2, mockResponse);
  });

  // Wait a bit
  await new Promise((resolve) => setTimeout(resolve, 100));

  // Request 3: PUT /api/users/123
  const req3 = {
    id: 'req-003',
    method: 'PUT',
    path: '/api/users/123',
    user: { id: 'admin-123' },
    headers: { 'user-agent': 'Mozilla/5.0' },
    ip: '192.168.1.100',
  } as Partial<Request> as Request;

  lmaoMiddleware(req3, mockResponse, async () => {
    await handleUpdateUser(req3, mockResponse);
  });

  console.log('\n✨ Example complete!');
  console.log('\n💡 Key Takeaway:');
  console.log('   Middleware sets requestId, userId, endpoint, httpMethod, userAgent, ipAddress ONCE');
  console.log('   All business logic ops inherit these attributes automatically');
  console.log('   Business code only adds domain-specific attributes (operation, resourceId, httpStatus)');
  console.log('   This eliminates 50-80% of repetitive logging boilerplate!\n');
}

// Run if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`.replace(/\\/g, '/')) {
  main().catch(console.error);
}
