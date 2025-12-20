#!/usr/bin/env bun

// Simple test runner for the library integration example
import { defineLogSchema, defineModule, S } from './packages/lmao/src/index.js';

// ============================================================================
// CACHE LIBRARY (defined first so HTTP can reference it)
// ============================================================================

const cacheModule = defineModule({
  metadata: {
    gitSha: 'cache-lib-v1.0.0',
    packageName: '@acme/cache',
    packagePath: 'src/redis.ts',
  },
  logSchema: {
    operation: S.enum(['GET', 'SET', 'DELETE', 'EXISTS']),
    key: S.category(),
    hit: S.boolean(),
    ttl: S.number(),
  },
})
  .ctx({})
  .make();

const cacheGet = cacheModule.op('cache-get', async (ctx, key: string) => {
  console.log('🔍 cacheGet Op - traceId from ctx._traceCtx:', (ctx as any)._traceCtx?.traceId);
  console.log('🔍 cacheGet Op - buffer trace_id:', (ctx as any)._buffer?.trace_id);

  ctx.tag.operation('GET').key(key);

  // Mock: simulate cache lookup
  const hit = Math.random() > 0.3; // 70% hit rate
  const value = hit ? { data: 'cached-value' } : null;

  ctx.tag.hit(hit);
  return ctx.ok({ value, hit });
});

const cacheSet = cacheModule.op('cache-set', async (ctx, key: string, value: unknown, ttl = 3600) => {
  ctx.tag
    .operation('SET')
    .key(key)
    .ttl(ttl as number);
  return ctx.ok({ success: true });
});

// ============================================================================
// HTTP LIBRARY (now can reference cacheModule in deps)
// ============================================================================

const httpModule = defineModule({
  metadata: {
    gitSha: 'http-lib-v1.0.0',
    packageName: '@acme/http-client',
    packagePath: 'src/index.ts',
  },
  logSchema: {
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
    url: S.text(),
    duration: S.number(),
  },
  deps: { cache: cacheModule }, // HTTP library declares dependency on cache
})
  .ctx({})
  .make();

// HTTP operation - accesses cache via typed ctx.deps
const httpRequest = httpModule.op('http-request', async (ctx, opts: { method: string; url: string }) => {
  const startTime = performance.now();

  console.log('🔍 httpRequest Op - traceId from ctx._traceCtx:', (ctx as any)._traceCtx?.traceId);
  console.log('🔍 httpRequest Op - buffer trace_id:', (ctx as any)._buffer?.trace_id);

  // Access wired cache dependency
  const { cache } = ctx.deps;

  // Check cache first using wired dependency
  const cacheKey = `http:${opts.method}:${opts.url}`;
  const cached = await cache.span('cache-get', cacheGet, cacheKey);

  if (cached.success && cached.value.hit) {
    ctx.tag
      .method(opts.method)
      .url(opts.url)
      .status(200)
      .duration(performance.now() - startTime);
    return ctx.ok(cached.value.value);
  }

  // Cache miss - make HTTP request
  ctx.tag.method(opts.method).url(opts.url);

  try {
    // Mock HTTP request
    const response = {
      status: opts.method === 'POST' ? 201 : 200,
      data: { success: true },
    };

    const duration = performance.now() - startTime;
    ctx.tag.status(response.status).duration(duration);

    // Cache successful response
    await cache.span('cache-set', cacheSet, cacheKey, response, 300);

    return ctx.ok(response);
  } catch (error) {
    const duration = performance.now() - startTime;
    ctx.tag.status(500).duration(duration);
    return ctx.err('HTTP_ERROR', error);
  }
});

// ============================================================================
// DB LIBRARY (for completeness)
// ============================================================================

const dbModule = defineModule({
  metadata: {
    gitSha: 'db-lib-v1.0.0',
    packageName: '@acme/database',
    packagePath: 'src/client.ts',
  },
  logSchema: {
    query: S.text(),
    duration: S.number(),
    table: S.category(),
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    rowsAffected: S.number(),
  },
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

// Demonstrate multiple extends: build app schema by extending multiple times
const appSchema = defineLogSchema({
  ...httpModule.logSchema.fields,
  ...cacheModule.logSchema.fields,
  ...dbModule.logSchema.fields,
  userId: S.category(),
  requestId: S.category(),
  businessMetric: S.number(),
});

// Extend with schemas from wired modules via multiple extends
// This demonstrates reusing module schemas - each extend adds fields from one module
const appSchemaExtended = appSchema;
// .extend() // Reuse HTTP schema fields
// .extend(cacheModule.logSchema.fields) // Reuse cache schema fields
// .extend(dbModule.logSchema.fields); // Reuse DB schema fields

const appModule = defineModule({
  metadata: {
    gitSha: 'app-v1.0.0',
    packageName: '@example/user-service',
    packagePath: 'src/app.ts',
  },
  logSchema: appSchemaExtended, // Use extended schema with all module fields
})
  .ctx({})
  .make();

// IDEAL: Simple wiring - HTTP automatically gets its cache dependency!
const wiredApp = appModule.use({
  http: httpModule, // HTTP library + its cache dep auto-wired
  db: dbModule, // DB library (no deps)
  cache: cacheModule, // Direct cache access for app
});

// ============================================================================
// RUN EXAMPLE
// ============================================================================

async function runExample() {
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

  // Application operation using wired libraries
  const getUserProfile = wiredApp.op('get-user-profile', async (ctx, userId: string) => {
    console.log('🔍 getUserProfile Op - traceId from ctx._traceCtx:', (ctx as any)._traceCtx?.traceId);
    console.log('🔍 getUserProfile Op - buffer trace_id:', (ctx as any)._buffer?.trace_id);

    // Set app-specific attributes
    ctx.tag.userId(userId).requestId('req-123');

    // Use DB to fetch user
    ctx.log.info('Fetching user from database');
    const dbResult = await ctx.span('db-query', dbQuery, `SELECT * FROM users WHERE id = '${userId}'`);

    if (!dbResult.success) {
      return ctx.err('DB_QUERY_FAILED', 'Database error');
    }

    const user = dbResult.value.rows[0];

    // HTTP library uses its internal cache automatically!
    ctx.log.info('Enriching user data via HTTP (with automatic caching)');
    const enrichResult = await ctx.span('http-request', httpRequest, {
      method: 'GET',
      url: `https://api.example.com/users/${userId}/profile`,
    });

    if (!enrichResult.success) {
      ctx.log.warn('Failed to enrich user data, continuing with basic data');
    }

    ctx.tag.businessMetric(enrichResult.success ? 1 : 0.5);
    return ctx.ok(user);
  });

  const result = await traceCtx.span('get-user-profile', getUserProfile, 'user-123');

  if (result.success) {
    console.log('✅ Success:', result.value);
  } else {
    console.log('❌ Error: Operation failed');
  }

  console.log('\n💡 IDEAL Design Principles:');
  console.log('- Libraries declare deps: deps: { cache: cacheModule }');
  console.log('- use() wires dependencies automatically');
  console.log('- ctx.deps provides type-safe access');
  console.log('- No type assertions or complex prefixing needed');
  console.log('- Zero runtime overhead for dependency resolution');
}

runExample().catch(console.error);
