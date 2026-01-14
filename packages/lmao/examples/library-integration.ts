/**
 * Example: IDEAL Library Integration Pattern
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Libraries define clean schemas without prefixes
 * - Libraries declare dependencies via `deps: { otherLib: otherModule }`
 * - Dependencies are automatically wired through use()
 * - Operations access dependencies through typed `ctx.deps`
 * - Zero runtime overhead through compile-time optimization
 *
 * This example demonstrates:
 * 1. Clean library definitions with dependency declarations
 * 2. Automatic dependency wiring via use()
 * 3. Type-safe dependency access through ctx.deps
 * 4. Simple composition without complex prefixing
 *
 * Key Pattern: Libraries declare deps and use() wires them automatically!
 */

import { defineLogSchema, defineModule, S } from '../src/index.js';

// ============================================================================
// CACHE LIBRARY - Foundation for other libraries
// ============================================================================

const cacheSchema = defineLogSchema({
  operation: S.enum(['GET', 'SET', 'DELETE', 'EXISTS']),
  key: S.category(),
  hit: S.boolean(),
  ttl: S.number(),
});

const cacheModule = defineModule({
  metadata: {
    gitSha: 'cache-lib-v1.0.0',
    packageName: '@acme/cache',
    packagePath: 'src/redis.ts',
  },
  logSchema: cacheSchema,
})
  .ctx({})
  .make();

// Cache operations - these will be accessible via ctx.deps.cache
const _cacheGet = cacheModule.op('cache-get', async (ctx, key: string) => {
  ctx.tag.operation('GET').key(key);

  // Mock: simulate cache lookup
  const hit = Math.random() > 0.3; // 70% hit rate
  const value = hit ? { data: 'cached-value' } : null;

  ctx.tag.hit(hit);
  return ctx.ok({ value, hit });
});

const _cacheSet = cacheModule.op('cache-set', async (ctx, key: string, _value: unknown, ttl = 3600) => {
  ctx.tag
    .operation('SET')
    .key(key)
    .ttl(ttl as number);

  // Mock: simulate cache write
  return ctx.ok({ success: true });
});

// ============================================================================
// HTTP LIBRARY - Declares dependency on cache
// ============================================================================

const httpSchema = defineLogSchema({
  status: S.number(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
  url: S.text(),
  duration: S.number(),
});

const httpModule = defineModule({
  metadata: {
    gitSha: 'http-lib-v1.0.0',
    packageName: '@acme/http-client',
    packagePath: 'src/index.ts',
  },
  logSchema: httpSchema,
  deps: { cache: cacheModule }, // IDEAL: Declare dependency here!
})
  .ctx({})
  .make();

// HTTP operation - accesses cache via typed ctx.deps
const httpRequest = httpModule.op('http-request', async (ctx, opts: { method: string; url: string }) => {
  const startTime = performance.now();

  // Access wired cache dependency (currently requires type assertion due to type system limitation)
  const { cache } = ctx.deps as {
    cache: {
      get: (key: string) => Promise<{ hit: boolean; value?: unknown }>;
      set: (key: string, value: unknown, ttl?: number) => Promise<{ success: boolean }>;
    };
  };

  // Check cache first using wired dependency
  const cacheKey = `http:${opts.method}:${opts.url}`;
  const cached = await cache.get(cacheKey);

  if (cached.hit) {
    ctx.tag
      .method(opts.method)
      .url(opts.url)
      .status(200)
      .duration(performance.now() - startTime);
    return ctx.ok(cached.value);
  }

  // Cache miss - make HTTP request
  ctx.tag.method(opts.method).url(opts.url);

  // Simulate HTTP request
  try {
    // Mock: simulate successful response
    const response = {
      status: opts.method === 'POST' ? 201 : 200,
      data: { success: true },
    };

    const duration = performance.now() - startTime;
    ctx.tag.status(response.status).duration(duration);

    // Cache successful response
    await cache.set(cacheKey, response, 300);

    return ctx.ok(response);
  } catch (error) {
    const duration = performance.now() - startTime;
    ctx.tag.status(500).duration(duration);
    return ctx.err('HTTP_ERROR', error);
  }
});

// ============================================================================
// DATABASE LIBRARY - Independent of other libraries
// ============================================================================

const dbSchema = defineLogSchema({
  query: S.text(),
  duration: S.number(), // Same name as HTTP library - will be prefixed
  table: S.category(),
  operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
  rowsAffected: S.number(),
});

const dbModule = defineModule({
  metadata: {
    gitSha: 'db-lib-v1.0.0',
    packageName: '@acme/database',
    packagePath: 'src/client.ts',
  },
  logSchema: dbSchema,
})
  .ctx({})
  .make();

const dbQuery = dbModule.op('db-query', async (ctx, sql: string) => {
  ctx.tag.query(sql).operation('SELECT').table('users').rowsAffected(1).duration(50);
  return ctx.ok({ rows: [{ id: 1, name: 'John' }], rowCount: 1 });
});

// ============================================================================
// APPLICATION COMPOSITION - Automatic Dependency Wiring
// ============================================================================

const appSchema = defineLogSchema({
  userId: S.category(),
  requestId: S.category(),
  businessMetric: S.number(),
});

const appModule = defineModule({
  metadata: {
    gitSha: 'app-v1.0.0',
    packageName: '@example/user-service',
    packagePath: 'src/app.ts',
  },
  logSchema: appSchema,
})
  .ctx({})
  .make();

// IDEAL: Simple wiring - HTTP automatically gets its cache dependency!
const wiredApp = appModule.use({
  http: httpModule, // HTTP library + its cache dependency auto-wired
  db: dbModule, // DB library (no deps)
  cache: cacheModule, // Direct cache access for app
});

/**
 * User data interface
 */
interface UserData {
  id: string;
  email: string;
  name: string;
}

// Application operation using wired libraries
const getUserProfile = wiredApp.op('get-user-profile', async (ctx, userId: string) => {
  // Set app-specific attributes
  ctx.tag.userId(userId).requestId('req-123');

  // Use DB to fetch user (current type system requires assertion - should be typed!)
  ctx.log.info('Fetching user from database');
  const dbResult = await (ctx.deps as any).db.span('db-query', dbQuery, `SELECT * FROM users WHERE id = '${userId}'`);

  if (!dbResult.success) {
    return ctx.err('DB_QUERY_FAILED', 'Database error');
  }

  const user = dbResult.value.rows[0] as unknown as UserData;

  // HTTP library uses its internal cache automatically!
  ctx.log.info('Enriching user data via HTTP (with automatic caching)');
  const enrichResult = await (ctx.deps as any).http.span('http-request', httpRequest, {
    method: 'GET',
    url: `https://api.example.com/users/${userId}/profile`,
  });

  if (!enrichResult.success) {
    ctx.log.warn('Failed to enrich user data, continuing with basic data');
  }

  ctx.tag.businessMetric(enrichResult.success ? 1 : 0.5);
  return ctx.ok(user);
});

// ============================================================================
// RUN EXAMPLE
// ============================================================================

async function _runExample() {
  console.log('\n🚀 IDEAL Library Integration Example\n');

  // Create app context
  const traceCtx = wiredApp.traceContext({
    requestId: 'req-123',
    userId: 'user-456',
  });

  console.log('📊 Request Context:', {
    requestId: traceCtx.requestId,
    traceId: traceCtx.traceId,
  });

  // Execute operation using wired libraries
  console.log('\n🔄 Executing getUserProfile...\n');
  const result = await traceCtx.span('get-user-profile', getUserProfile, 'user-123');

  if (result.success) {
    console.log('✅ Success:', result.value);
  } else {
    console.log('❌ Error:', result.error);
  }

  console.log('\n💡 IDEAL Design Principles:');
  console.log('- Libraries declare deps: deps: { cache: cacheModule }');
  console.log('- use() wires dependencies automatically');
  console.log('- ctx.deps provides type-safe access');
  console.log('- No type assertions or complex prefixing needed');
  console.log('- Zero runtime overhead for dependency resolution');
}

// Uncomment to run: runExample().catch(console.error);
