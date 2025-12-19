/**
 * Library Integration Pattern - Complete Example
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Libraries define clean schemas without prefixes
 * - Libraries provide traced operations
 * - Prefixing happens at composition time to avoid conflicts
 * - Zero hot path overhead through compile-time optimization
 *
 * This example demonstrates:
 * 1. Creating library modules with clean schemas
 * 2. Defining traced operations within libraries
 * 3. Composing multiple libraries with prefixes
 * 4. Using library operations in application code
 */

import {
  createLibraryModule,
  defineFeatureFlags,
  defineLogSchema,
  defineModule,
  InMemoryFlagEvaluator,
  S,
} from '../src/index.js';

// ============================================================================
// 1. HTTP TRACING LIBRARY
// ============================================================================

/**
 * HTTP library with clean, unprefixed schema
 * Library author writes domain-focused code
 */
const httpSchema = defineLogSchema({
  status: S.number(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
  url: S.text(),
  duration: S.number(),
});

/**
 * HTTP request options
 */
interface RequestOptions {
  method: string;
  url: string;
  body?: unknown;
}

/**
 * Mock HTTP response
 */
interface HttpResponse {
  status: number;
  data: unknown;
}

/**
 * Create HTTP tracing library
 * Provides traced HTTP operations
 */
function createHttpLibrary(prefix = 'http') {
  const module = createLibraryModule({
    gitSha: 'http-lib-v1.2.3',
    packageName: '@acme/http-client',
    packagePath: 'src/index.ts',
    schema: httpSchema,
  });

  // Define library operations
  const operations = {
    /**
     * Traced HTTP request operation
     * Uses clean API: ctx.tag.status(200), ctx.tag.method('POST')
     * But writes to prefixed columns: http_status, http_method
     */
    request: module.task('http-request', async (ctx, opts: RequestOptions) => {
      const startTime = performance.now();

      // Clean, unprefixed API - library doesn't worry about conflicts
      ctx.tag.method(opts.method).url(opts.url);

      // Simulate HTTP request
      try {
        // Mock: simulate successful response
        const response: HttpResponse = {
          status: opts.method === 'POST' ? 201 : 200,
          data: { success: true },
        };

        const duration = performance.now() - startTime;
        ctx.tag.status(response.status).duration(duration);

        return ctx.ok(response);
      } catch (error) {
        const duration = performance.now() - startTime;
        ctx.tag.status(0).duration(duration);
        return ctx.err('HTTP_ERROR', error);
      }
    }),
  };

  return { module, operations, prefix };
}

// ============================================================================
// 2. DATABASE TRACING LIBRARY
// ============================================================================

/**
 * Database library with clean schema
 * Notice: Both HTTP and DB have 'duration' field - no conflict when prefixed
 */
const dbSchema = defineLogSchema({
  query: S.text(),
  duration: S.number(), // Same name as HTTP library - will be prefixed
  table: S.category(),
  operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
  rowsAffected: S.number(),
});

/**
 * Create database tracing library
 */
function createDatabaseLibrary(prefix = 'db') {
  const module = createLibraryModule({
    gitSha: 'db-lib-v2.0.1',
    packageName: '@acme/database',
    packagePath: 'src/client.ts',
    schema: dbSchema,
  });

  const operations = {
    /**
     * Traced database query operation
     */
    query: module.task('db-query', async (ctx, sql: string) => {
      const startTime = performance.now();

      // Extract table name from SQL (simplified)
      const tableName = sql.match(/FROM\s+(\w+)/i)?.[1] || 'unknown';
      const operation = sql.trim().split(' ')[0].toUpperCase();

      // Clean API: ctx.tag.query(), ctx.tag.table(), etc.
      ctx.tag
        .query(sql)
        .table(tableName)
        .operation(operation as 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE');

      try {
        // Mock: simulate query execution
        const result = {
          rows: operation === 'SELECT' ? [{ id: 1, name: 'Test' }] : [],
          rowCount: operation === 'SELECT' ? 1 : operation === 'INSERT' ? 1 : 0,
        };

        const duration = performance.now() - startTime;
        ctx.tag.duration(duration).rowsAffected(result.rowCount);

        return ctx.ok(result);
      } catch (error) {
        const duration = performance.now() - startTime;
        ctx.tag.duration(duration);
        return ctx.err('DB_ERROR', error);
      }
    }),
  };

  return { module, operations, prefix };
}

// ============================================================================
// 3. CACHE TRACING LIBRARY
// ============================================================================

/**
 * Cache library schema
 */
const cacheSchema = defineLogSchema({
  operation: S.enum(['GET', 'SET', 'DELETE', 'EXISTS']),
  key: S.category(),
  hit: S.boolean(),
  ttl: S.number(),
});

/**
 * Create cache tracing library
 */
function createCacheLibrary(prefix = 'cache') {
  const module = createLibraryModule({
    gitSha: 'cache-lib-v1.0.0',
    packageName: '@acme/cache',
    packagePath: 'src/redis.ts',
    schema: cacheSchema,
  });

  const operations = {
    get: module.task('cache-get', async (ctx, key: string) => {
      ctx.tag.operation('GET').key(key);

      // Mock: simulate cache lookup
      const hit = Math.random() > 0.3; // 70% hit rate
      const value = hit ? { data: 'cached-value' } : null;

      ctx.tag.hit(hit);

      return ctx.ok({ value, hit });
    }),

    set: module.task('cache-set', async (ctx, key: string, _value: unknown, ttl = 3600) => {
      ctx.tag
        .operation('SET')
        .key(key)
        .ttl(ttl as number);

      // Mock: simulate cache write
      return ctx.ok({ success: true });
    }),
  };

  return { module, operations, prefix };
}

// ============================================================================
// 4. APPLICATION COMPOSITION
// ============================================================================

/**
 * Application composes multiple libraries
 * Each library is prefixed to avoid attribute name conflicts
 */

// Create library instances with prefixes
const httpLib = createHttpLibrary('http');
const dbLib = createDatabaseLibrary('db');
const cacheLib = createCacheLibrary('redis');

// Define application-specific attributes
const appSchema = defineLogSchema({
  userId: S.category(),
  requestId: S.category(),
  businessMetric: S.number(),
});

// Define feature flags
const appFlags = defineFeatureFlags({
  useCache: S.boolean().default(true).sync(),
  debugMode: S.boolean().default(false).sync(),
});

// Compose all schemas together
const composedSchema = {
  ...httpLib.module.schema,
  ...dbLib.module.schema,
  ...cacheLib.module.schema,
  ...appSchema,
};

// Create application module with composed schema using defineModule
const appModule = defineModule({
  moduleMetadata: {
    gitSha: 'app-v1.0.0',
    packageName: '@example/user-service',
    packagePath: 'src/services/user.ts',
  },
  logSchema: composedSchema,
});

// ============================================================================
// 5. APPLICATION BUSINESS LOGIC
// ============================================================================

/**
 * User data interface
 */
interface UserData {
  id: string;
  email: string;
  name: string;
}

/**
 * Application op that uses multiple libraries
 * Demonstrates how libraries work together seamlessly
 */
const getUserProfile = appModule.task('get-user-profile', async (ctx, userId: string) => {
  // Set application-specific attributes
  ctx.tag.userId(userId).requestId(ctx.requestId);

  // Check feature flag
  if (ctx.ff.useCache) {
    ctx.log.info('Cache enabled - checking cache first');

    // Use cache library (writes to redis_* columns)
    const cacheKey = `user:${userId}`;
    const cached = await cacheLib.operations.get(ctx, cacheKey);

    if (cached.success && cached.value.hit) {
      ctx.log.info('Cache hit');
      ctx.tag.businessMetric(1); // Track cache hits as metric
      return ctx.ok(cached.value.value);
    }
  }

  // Cache miss - fetch from database (writes to db_* columns)
  ctx.log.info('Fetching from database');
  const dbResult = await dbLib.operations.query(ctx, `SELECT * FROM users WHERE id = '${userId}'`);

  if (!dbResult.success) {
    return ctx.err('DB_QUERY_FAILED', dbResult.error);
  }

  const user = dbResult.value.rows[0] as unknown as UserData;

  // Make HTTP call to enrich user data (writes to http_* columns)
  ctx.log.info('Enriching user data from external API');
  const enrichResult = await httpLib.operations.request(ctx, {
    method: 'GET',
    url: `https://api.example.com/users/${userId}/profile`,
  });

  if (!enrichResult.success) {
    ctx.log.warn('Failed to enrich user data, continuing with basic data');
  }

  // Cache the result
  if (ctx.ff.useCache) {
    await cacheLib.operations.set(ctx, `user:${userId}`, user, 3600);
  }

  ctx.tag.businessMetric(0.5); // Track partial success

  return ctx.ok(user);
});

// ============================================================================
// 6. RUN EXAMPLE
// ============================================================================

async function runExample() {
  console.log('\n🚀 Library Integration Pattern Example\n');
  console.log('='.repeat(70));

  // Create request context via module
  const flagEvaluator = new InMemoryFlagEvaluator({
    useCache: true,
    debugMode: false,
  });

  const traceCtx = appModule.traceContext(
    {
      requestId: `req-${Date.now()}`,
      userId: 'admin',
    },
    appFlags,
    flagEvaluator,
    { environment: 'production' },
  );

  console.log('\n📊 Request Context:');
  console.log(`   Request ID: ${traceCtx.requestId}`);
  console.log(`   Trace ID: ${traceCtx.traceId}`);
  console.log(`   Cache Enabled: ${traceCtx.ff.useCache}`);

  // Execute business logic that uses multiple libraries
  console.log('\n🔄 Executing getUserProfile op...\n');
  const result = await getUserProfile(traceCtx, 'user-123');

  if (result.success) {
    console.log('✅ Success! User profile retrieved:', result.value);
  } else {
    console.log('❌ Error:', result.error);
  }

  console.log(`\n${'='.repeat(70)}`);
  console.log('\n💡 Key Points Demonstrated:\n');
  console.log('1. ✅ HTTP library writes to: http_status, http_method, http_url, http_duration');
  console.log('2. ✅ DB library writes to: db_query, db_duration, db_table, db_operation, db_rowsAffected');
  console.log('3. ✅ Cache library writes to: redis_operation, redis_key, redis_hit, redis_ttl');
  console.log('4. ✅ App writes to: userId, requestId, businessMetric');
  console.log('5. ✅ NO CONFLICTS - Each library has its own namespace');
  console.log('6. ✅ Libraries use CLEAN API - no prefixes in library code');
  console.log('7. ✅ Zero hot path overhead - all mapping done at module creation time\n');

  console.log('📈 All trace data is written to Arrow columnar buffers:');
  console.log('   - System columns: timestamp, trace_id, span_id, entry_type, etc.');
  console.log('   - HTTP columns: http_status, http_method, http_url, http_duration');
  console.log('   - DB columns: db_query, db_duration, db_table, db_operation');
  console.log('   - Cache columns: redis_operation, redis_key, redis_hit, redis_ttl');
  console.log('   - App columns: userId, requestId, businessMetric\n');
}

// Run the example
runExample().catch(console.error);
