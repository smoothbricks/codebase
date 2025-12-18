/**
 * Tests for nested library tasks with prefix remapping
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Libraries define clean schemas (status, method)
 * - Prefixes applied at composition time (http_status, http_method)
 * - Library code writes to clean names, stored in prefixed columns
 *
 * Per specs/01k_tree_walker_and_arrow_conversion.md:
 * - Tree traversal visits all buffers including children
 * - Arrow conversion uses shared dictionaries across all buffers
 */

import { describe, expect, it } from 'bun:test';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { moduleContextFactory } from '../library.js';
import { createModuleContext, createRequestContext } from '../lmao.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';
import type { SpanBuffer } from '../types.js';

// Test feature flags schema (empty for simplicity)
const testFlags = defineFeatureFlags({
  testFlag: S.boolean().default(true).sync(),
});

// Mock feature flag evaluator
const mockEvaluator = new InMemoryFlagEvaluator({
  testFlag: true,
});

describe('Nested Library Tasks', () => {
  describe('4-level nesting WITHOUT library prefixes (regular module contexts)', () => {
    /**
     * Test scenario: App → Module1.task → Module2.task → Module3.task → Module4.task
     * All modules use the same schema (no prefixing)
     * Verifies tree structure and Arrow conversion
     */
    it('should create proper parent-child hierarchy with 4 levels of nesting', async () => {
      // Shared schema for all modules
      const sharedSchema = defineTagAttributes({
        userId: S.category(),
        operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
        depth: S.number(),
      });

      // Create 4 module contexts with the same schema
      const module1 = createModuleContext({
        moduleMetadata: {
          gitSha: 'test-sha',
          packageName: '@test/module1',
          packagePath: 'src/module1.ts',
        },
        tagAttributes: sharedSchema,
      });

      const module2 = createModuleContext({
        moduleMetadata: {
          gitSha: 'test-sha',
          packageName: '@test/module2',
          packagePath: 'src/module2.ts',
        },
        tagAttributes: sharedSchema,
      });

      const module3 = createModuleContext({
        moduleMetadata: {
          gitSha: 'test-sha',
          packageName: '@test/module3',
          packagePath: 'src/module3.ts',
        },
        tagAttributes: sharedSchema,
      });

      const module4 = createModuleContext({
        moduleMetadata: {
          gitSha: 'test-sha',
          packageName: '@test/module4',
          packagePath: 'src/module4.ts',
        },
        tagAttributes: sharedSchema,
      });

      // Capture buffers for verification
      let rootBuffer: SpanBuffer | undefined;
      let level2Buffer: SpanBuffer | undefined;
      let level3Buffer: SpanBuffer | undefined;
      let level4Buffer: SpanBuffer | undefined;

      // Level 4 task (deepest)
      const level4Task = module4.task('level4-task', async (ctx) => {
        level4Buffer = ctx.buffer;
        ctx.tag.depth(4);
        ctx.tag.operation('DELETE');
        return ctx.ok('level4-done');
      });

      // Level 3 task
      const level3Task = module3.task('level3-task', async (ctx) => {
        level3Buffer = ctx.buffer;
        ctx.tag.depth(3);
        ctx.tag.operation('UPDATE');

        // Call level 4 task
        await level4Task(ctx);

        return ctx.ok('level3-done');
      });

      // Level 2 task
      const level2Task = module2.task('level2-task', async (ctx) => {
        level2Buffer = ctx.buffer;
        ctx.tag.depth(2);
        ctx.tag.operation('READ');

        // Call level 3 task
        await level3Task(ctx);

        return ctx.ok('level2-done');
      });

      // Root task (level 1)
      const rootTask = module1.task('root-task', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.userId('user-123');
        ctx.tag.depth(1);
        ctx.tag.operation('CREATE');

        // Call level 2 task
        await level2Task(ctx);

        return ctx.ok('root-done');
      });

      // Execute the nested task chain
      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      const result = await rootTask(requestCtx);

      // Verify result
      expect(result.success).toBe(true);

      // Verify all buffers were captured
      expect(rootBuffer).toBeDefined();
      expect(level2Buffer).toBeDefined();
      expect(level3Buffer).toBeDefined();
      expect(level4Buffer).toBeDefined();

      // Verify parent-child relationships
      // Root has no parent
      expect(rootBuffer?.parent).toBeUndefined();

      // Level 2's parent is root
      expect(level2Buffer?.parent).toBe(rootBuffer);

      // Level 3's parent is level 2
      expect(level3Buffer?.parent).toBe(level2Buffer);

      // Level 4's parent is level 3
      expect(level4Buffer?.parent).toBe(level3Buffer);

      // Verify children array
      expect(rootBuffer?.children).toContain(level2Buffer);
      expect(level2Buffer?.children).toContain(level3Buffer);
      expect(level3Buffer?.children).toContain(level4Buffer);
      expect(level4Buffer?.children.length).toBe(0);

      // Verify all share the same traceId
      const traceId = rootBuffer?.traceId;
      expect(level2Buffer?.traceId).toBe(traceId);
      expect(level3Buffer?.traceId).toBe(traceId);
      expect(level4Buffer?.traceId).toBe(traceId);
    });

    it('should convert 4-level nested tree to Arrow table with correct parent-child relationships', async () => {
      const sharedSchema = defineTagAttributes({
        level: S.number(),
      });

      const module1 = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@test/m1', packagePath: 'm1.ts' },
        tagAttributes: sharedSchema,
      });
      const module2 = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@test/m2', packagePath: 'm2.ts' },
        tagAttributes: sharedSchema,
      });
      const module3 = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@test/m3', packagePath: 'm3.ts' },
        tagAttributes: sharedSchema,
      });
      const module4 = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@test/m4', packagePath: 'm4.ts' },
        tagAttributes: sharedSchema,
      });

      let rootBuffer: SpanBuffer | undefined;

      const level4Task = module4.task('level4', async (ctx) => {
        ctx.tag.level(4);
        return ctx.ok('done');
      });

      const level3Task = module3.task('level3', async (ctx) => {
        ctx.tag.level(3);
        await level4Task(ctx);
        return ctx.ok('done');
      });

      const level2Task = module2.task('level2', async (ctx) => {
        ctx.tag.level(2);
        await level3Task(ctx);
        return ctx.ok('done');
      });

      const rootTask = module1.task('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.level(1);
        await level2Task(ctx);
        return ctx.ok('done');
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      await rootTask(requestCtx);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Should have rows for all 4 levels (span-start + span-ok for each = 8 rows)
      expect(table.numRows).toBeGreaterThanOrEqual(8);

      // Extract all rows for verification
      const rows = Array.from({ length: table.numRows }, (_, i) => table.get(i)?.toJSON());

      // Find span-start entries for each level
      const spanStarts = rows.filter((r) => r?.entry_type === 'span-start');

      // Should have 4 span-start entries (one per level)
      expect(spanStarts.length).toBe(4);

      // Verify parent-child relationships in Arrow output
      const rootSpan = spanStarts.find((r) => r?.message === 'root');
      const level2Span = spanStarts.find((r) => r?.message === 'level2');
      const level3Span = spanStarts.find((r) => r?.message === 'level3');
      const level4Span = spanStarts.find((r) => r?.message === 'level4');

      expect(rootSpan).toBeDefined();
      expect(level2Span).toBeDefined();
      expect(level3Span).toBeDefined();
      expect(level4Span).toBeDefined();

      // Root has no parent
      expect(rootSpan?.parent_span_id).toBeNull();

      // Level 2's parent is root
      expect(level2Span?.parent_span_id).toBe(rootSpan?.span_id);

      // Level 3's parent is level 2
      expect(level3Span?.parent_span_id).toBe(level2Span?.span_id);

      // Level 4's parent is level 3
      expect(level4Span?.parent_span_id).toBe(level3Span?.span_id);

      // All have same trace_id
      expect(level2Span?.trace_id).toBe(rootSpan?.trace_id);
      expect(level3Span?.trace_id).toBe(rootSpan?.trace_id);
      expect(level4Span?.trace_id).toBe(rootSpan?.trace_id);
    });
  });

  describe('4-level nesting WITH library prefixes (moduleContextFactory)', () => {
    /**
     * Test scenario: App → httpLib.task (prefix: 'http') → dbLib.task (prefix: 'db') →
     *                cacheLib.task (prefix: 'cache') → authLib.task (prefix: 'auth')
     *
     * Per specs/01e_library_integration_pattern.md:
     * - Each library has its own schema with unprefixed columns
     * - Library code writes to unprefixed columns directly (ctx.tag.status())
     * - Columns stored with prefix (http_status, db_status, etc.)
     *
     * Note: moduleContextFactory returns erased types for the outer API but provides
     * properly typed context inside task callbacks via the TaskFunction generic.
     */
    it('should allow libraries to write using clean names while storing with prefixes', async () => {
      // Define schemas as consts for type inference
      const httpSchema = { status: S.number(), method: S.enum(['GET', 'POST', 'PUT', 'DELETE']) } as const;
      const dbSchema = { query: S.text(), rowCount: S.number() } as const;
      const cacheSchema = { key: S.category(), hit: S.boolean() } as const;
      const authSchema = { userId: S.category(), role: S.category() } as const;

      // HTTP library with prefix 'http'
      const httpLib = moduleContextFactory(
        'http',
        { gitSha: 'sha', packageName: '@lib/http', packagePath: 'http.ts' },
        httpSchema,
      );

      // DB library with prefix 'db'
      const dbLib = moduleContextFactory(
        'db',
        { gitSha: 'sha', packageName: '@lib/db', packagePath: 'db.ts' },
        dbSchema,
      );

      // Cache library with prefix 'cache'
      const cacheLib = moduleContextFactory(
        'cache',
        { gitSha: 'sha', packageName: '@lib/cache', packagePath: 'cache.ts' },
        cacheSchema,
      );

      // Auth library with prefix 'auth'
      const authLib = moduleContextFactory(
        'auth',
        { gitSha: 'sha', packageName: '@lib/auth', packagePath: 'auth.ts' },
        authSchema,
      );

      let rootBuffer: SpanBuffer | undefined;

      // Auth task (deepest - level 4) - uses typed ctx inside callback
      const authTask = authLib.task('auth-check', async (ctx) => {
        // Library writes using clean names via .with() for bulk assignment
        ctx.tag.with({ userId: 'user-456', role: 'admin' });
        return ctx.ok({ authorized: true });
      });

      // Cache task (level 3)
      const cacheTask = cacheLib.task('cache-lookup', async (ctx) => {
        ctx.tag.with({ key: 'session:user-456', hit: true });

        // Call auth task
        await authTask(ctx);

        return ctx.ok({ cached: true });
      });

      // DB task (level 2)
      const dbTask = dbLib.task('db-query', async (ctx) => {
        ctx.tag.with({ query: 'SELECT * FROM users', rowCount: 1 });

        // Call cache task
        await cacheTask(ctx);

        return ctx.ok({ rows: [] });
      });

      // HTTP task (level 1 - root)
      const httpTask = httpLib.task('http-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.with({ status: 200, method: 'GET' });

        // Call db task
        await dbTask(ctx);

        return ctx.ok({ response: 'ok' });
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      const result = await httpTask(requestCtx);

      expect(result.success).toBe(true);
      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Should have rows for all 4 levels
      expect(table.numRows).toBeGreaterThanOrEqual(8);

      // Verify prefixed columns exist in schema
      const fieldNames = table.schema.fields.map((f) => f.name);

      // HTTP library columns should be prefixed
      expect(fieldNames).toContain('http_status');
      expect(fieldNames).toContain('http_method');

      // DB library columns should be prefixed
      expect(fieldNames).toContain('db_query');
      expect(fieldNames).toContain('db_rowCount');

      // Cache library columns should be prefixed
      expect(fieldNames).toContain('cache_key');
      expect(fieldNames).toContain('cache_hit');

      // Auth library columns should be prefixed
      expect(fieldNames).toContain('auth_userId');
      expect(fieldNames).toContain('auth_role');
    });

    it('should maintain correct tree structure with 4 levels of library tasks', async () => {
      const httpLib = moduleContextFactory(
        'http',
        { gitSha: 'sha', packageName: '@lib/http', packagePath: 'http.ts' },
        { status: S.number() },
      );

      const dbLib = moduleContextFactory(
        'db',
        { gitSha: 'sha', packageName: '@lib/db', packagePath: 'db.ts' },
        { query: S.text() },
      );

      const cacheLib = moduleContextFactory(
        'cache',
        { gitSha: 'sha', packageName: '@lib/cache', packagePath: 'cache.ts' },
        { key: S.category() },
      );

      const authLib = moduleContextFactory(
        'auth',
        { gitSha: 'sha', packageName: '@lib/auth', packagePath: 'auth.ts' },
        { userId: S.category() },
      );

      let httpBuffer: SpanBuffer | undefined;
      let dbBuffer: SpanBuffer | undefined;
      let cacheBuffer: SpanBuffer | undefined;
      let authBuffer: SpanBuffer | undefined;

      const authTask = authLib.task('auth', async (ctx) => {
        authBuffer = ctx.buffer;
        ctx.tag.with({ userId: 'user-1' });
        return ctx.ok('done');
      });

      const cacheTask = cacheLib.task('cache', async (ctx) => {
        cacheBuffer = ctx.buffer;
        ctx.tag.with({ key: 'key-1' });
        await authTask(ctx);
        return ctx.ok('done');
      });

      const dbTask = dbLib.task('db', async (ctx) => {
        dbBuffer = ctx.buffer;
        ctx.tag.with({ query: 'SELECT 1' });
        await cacheTask(ctx);
        return ctx.ok('done');
      });

      const httpTask = httpLib.task('http', async (ctx) => {
        httpBuffer = ctx.buffer;
        ctx.tag.with({ status: 200 });
        await dbTask(ctx);
        return ctx.ok('done');
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      await httpTask(requestCtx);

      // Verify buffers captured
      expect(httpBuffer).toBeDefined();
      expect(dbBuffer).toBeDefined();
      expect(cacheBuffer).toBeDefined();
      expect(authBuffer).toBeDefined();

      // Verify parent-child relationships
      expect(httpBuffer?.parent).toBeUndefined(); // Root
      expect(dbBuffer?.parent).toBe(httpBuffer);
      expect(cacheBuffer?.parent).toBe(dbBuffer);
      expect(authBuffer?.parent).toBe(cacheBuffer);

      // Verify children arrays
      expect(httpBuffer?.children).toContain(dbBuffer);
      expect(dbBuffer?.children).toContain(cacheBuffer);
      expect(cacheBuffer?.children).toContain(authBuffer);
      expect(authBuffer?.children.length).toBe(0);

      // Verify all share same traceId
      expect(dbBuffer?.traceId).toBe(httpBuffer?.traceId);
      expect(cacheBuffer?.traceId).toBe(httpBuffer?.traceId);
      expect(authBuffer?.traceId).toBe(httpBuffer?.traceId);
    });
  });

  describe('Mixed nesting (some with prefix, some without)', () => {
    /**
     * Test scenario: App → regularModule.task → httpLib.task (prefix) →
     *                regularModule2.task → dbLib.task (prefix)
     */
    it('should handle mixed nesting with both prefixed and non-prefixed modules', async () => {
      // Regular module (no prefix)
      const regularSchema = defineTagAttributes({
        requestId: S.category(),
        step: S.category(),
      });

      const regularModule = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@app/handler', packagePath: 'handler.ts' },
        tagAttributes: regularSchema,
      });

      const regularModule2 = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@app/processor', packagePath: 'processor.ts' },
        tagAttributes: regularSchema,
      });

      // Library modules (with prefix)
      const httpLib = moduleContextFactory(
        'http',
        { gitSha: 'sha', packageName: '@lib/http', packagePath: 'http.ts' },
        { status: S.number(), url: S.text() },
      );

      const dbLib = moduleContextFactory(
        'db',
        { gitSha: 'sha', packageName: '@lib/db', packagePath: 'db.ts' },
        { query: S.text(), table: S.category() },
      );

      let rootBuffer: SpanBuffer | undefined;

      // DB task (deepest - level 4, prefixed)
      const dbTask = dbLib.task('db-query', async (ctx) => {
        ctx.tag.with({ query: 'INSERT INTO logs', table: 'logs' });
        return ctx.ok('inserted');
      });

      // Regular module 2 task (level 3, no prefix)
      const processorTask = regularModule2.task('process', async (ctx) => {
        ctx.tag.step('validation');
        await dbTask(ctx);
        return ctx.ok('processed');
      });

      // HTTP library task (level 2, prefixed)
      const httpTask = httpLib.task('fetch', async (ctx) => {
        ctx.tag.with({ status: 200, url: '/api/process' });
        await processorTask(ctx);
        return ctx.ok('fetched');
      });

      // Root task (level 1, no prefix)
      const rootTask = regularModule.task('handle-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.requestId('req-123');
        ctx.tag.step('start');
        await httpTask(ctx);
        return ctx.ok('handled');
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      const result = await rootTask(requestCtx);

      expect(result.success).toBe(true);
      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Verify both prefixed and non-prefixed columns exist
      const fieldNames = table.schema.fields.map((f) => f.name);

      // Non-prefixed columns from regular modules
      expect(fieldNames).toContain('requestId');
      expect(fieldNames).toContain('step');

      // Prefixed columns from libraries
      expect(fieldNames).toContain('http_status');
      expect(fieldNames).toContain('http_url');
      expect(fieldNames).toContain('db_query');
      expect(fieldNames).toContain('db_table');

      // Extract rows
      const rows = Array.from({ length: table.numRows }, (_, i) => table.get(i)?.toJSON());
      const spanStarts = rows.filter((r) => r?.entry_type === 'span-start');

      // Should have 4 span-start entries
      expect(spanStarts.length).toBe(4);

      // Verify correct nesting order
      const handleRequest = spanStarts.find((r) => r?.message === 'handle-request');
      const fetch = spanStarts.find((r) => r?.message === 'fetch');
      const process = spanStarts.find((r) => r?.message === 'process');
      const dbQuery = spanStarts.find((r) => r?.message === 'db-query');

      expect(handleRequest?.parent_span_id).toBeNull();
      expect(fetch?.parent_span_id).toBe(handleRequest?.span_id);
      expect(process?.parent_span_id).toBe(fetch?.span_id);
      expect(dbQuery?.parent_span_id).toBe(process?.span_id);
    });
  });

  describe('Column isolation test (same column name with different prefixes)', () => {
    /**
     * Per specs/01e_library_integration_pattern.md:
     * - Multiple libraries can define the same attribute name
     * - Prefixes resolve to different columns (http_status, db_status)
     * - No collision, correct data in Arrow output
     */
    it('should isolate columns with same name using different prefixes', async () => {
      // Both libraries have a "status" column, but with different prefixes
      const httpLib = moduleContextFactory(
        'http',
        { gitSha: 'sha', packageName: '@lib/http', packagePath: 'http.ts' },
        { status: S.number() }, // http_status
      );

      const dbLib = moduleContextFactory(
        'db',
        { gitSha: 'sha', packageName: '@lib/db', packagePath: 'db.ts' },
        { status: S.category() }, // db_status - same name, different type!
      );

      const processLib = moduleContextFactory(
        'process',
        { gitSha: 'sha', packageName: '@lib/process', packagePath: 'process.ts' },
        { status: S.enum(['running', 'stopped', 'failed']) }, // process_status - same name, enum type
      );

      let rootBuffer: SpanBuffer | undefined;

      const processTask = processLib.task('run-process', async (ctx) => {
        ctx.tag.with({ status: 'running' });
        return ctx.ok('done');
      });

      const dbTask = dbLib.task('db-connect', async (ctx) => {
        ctx.tag.with({ status: 'connected' });
        await processTask(ctx);
        return ctx.ok('done');
      });

      const httpTask = httpLib.task('http-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.with({ status: 200 });
        await dbTask(ctx);
        return ctx.ok('done');
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      await httpTask(requestCtx);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Verify all three prefixed status columns exist
      const fieldNames = table.schema.fields.map((f) => f.name);
      expect(fieldNames).toContain('http_status');
      expect(fieldNames).toContain('db_status');
      expect(fieldNames).toContain('process_status');

      // Extract rows
      const rows = Array.from({ length: table.numRows }, (_, i) => table.get(i)?.toJSON());

      // Find span-start rows for each task
      const httpSpan = rows.find((r) => r?.entry_type === 'span-start' && r?.message === 'http-request');
      const dbSpan = rows.find((r) => r?.entry_type === 'span-start' && r?.message === 'db-connect');
      const processSpan = rows.find((r) => r?.entry_type === 'span-start' && r?.message === 'run-process');

      // HTTP task wrote to http_status (number: 200)
      expect(httpSpan?.http_status).toBe(200);

      // DB task wrote to db_status (string: 'connected')
      expect(dbSpan?.db_status).toBe('connected');

      // Process task wrote to process_status (enum: 'running')
      expect(processSpan?.process_status).toBe('running');

      // Verify no cross-contamination - each task only wrote to its own column
      // Other status columns should be null for each span
      expect(httpSpan?.db_status).toBeNull();
      expect(httpSpan?.process_status).toBeNull();

      expect(dbSpan?.http_status).toBeNull();
      expect(dbSpan?.process_status).toBeNull();

      expect(processSpan?.http_status).toBeNull();
      expect(processSpan?.db_status).toBeNull();
    });
  });

  describe('Tree traversal verification (walkSpanTree)', () => {
    /**
     * Per specs/01k_tree_walker_and_arrow_conversion.md:
     * - walkSpanTree visits all buffers including children
     * - Visits overflow chains (buffer.next)
     * - Depth-first pre-order traversal
     */
    it('should visit all buffers in tree during Arrow conversion', async () => {
      const schema = defineTagAttributes({
        nodeId: S.category(),
      });

      const module = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@test/tree', packagePath: 'tree.ts' },
        tagAttributes: schema,
      });

      let rootBuffer: SpanBuffer | undefined;

      // Create a tree with multiple children at each level
      const leafTask = module.task('leaf', async (ctx) => {
        ctx.tag.nodeId('leaf');
        return ctx.ok('done');
      });

      const child2Task = module.task('child2', async (ctx) => {
        ctx.tag.nodeId('child2');
        await leafTask(ctx); // child2 -> leaf
        return ctx.ok('done');
      });

      const child1Task = module.task('child1', async (ctx) => {
        ctx.tag.nodeId('child1');
        await leafTask(ctx); // child1 -> leaf (another leaf)
        return ctx.ok('done');
      });

      const rootTask = module.task('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.nodeId('root');
        await child1Task(ctx);
        await child2Task(ctx);
        return ctx.ok('done');
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      await rootTask(requestCtx);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Should have all nodes: root, child1, child2, leaf, leaf (2 leaves)
      // Each span has span-start + span-ok = 2 rows
      // Total: 5 spans * 2 = 10 rows
      expect(table.numRows).toBe(10);

      // Extract rows
      const rows = Array.from({ length: table.numRows }, (_, i) => table.get(i)?.toJSON());
      const spanStarts = rows.filter((r) => r?.entry_type === 'span-start');

      // Should have 5 span-start entries
      expect(spanStarts.length).toBe(5);

      // Verify all nodes are present
      const nodeIds = spanStarts.map((r) => r?.nodeId);
      expect(nodeIds).toContain('root');
      expect(nodeIds).toContain('child1');
      expect(nodeIds).toContain('child2');
      // Should have 2 leaf nodes
      expect(nodeIds.filter((id) => id === 'leaf').length).toBe(2);
    });

    it('should correctly traverse tree with proper parent-child order', async () => {
      const schema = defineTagAttributes({
        order: S.number(),
      });

      const module = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@test/order', packagePath: 'order.ts' },
        tagAttributes: schema,
      });

      let rootBuffer: SpanBuffer | undefined;
      let order = 0;

      const deepTask = module.task('deep', async (ctx) => {
        ctx.tag.order(++order);
        return ctx.ok('done');
      });

      const middleTask = module.task('middle', async (ctx) => {
        ctx.tag.order(++order);
        await deepTask(ctx);
        return ctx.ok('done');
      });

      const rootTask = module.task('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.order(++order);
        await middleTask(ctx);
        return ctx.ok('done');
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      await rootTask(requestCtx);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Extract span-start rows in table order
      const rows = Array.from({ length: table.numRows }, (_, i) => table.get(i)?.toJSON());
      const spanStarts = rows.filter((r) => r?.entry_type === 'span-start');

      // Per specs/01k - depth-first pre-order: parent before children
      // Order should be: root(1), middle(2), deep(3)
      expect(spanStarts[0]?.order).toBe(1); // root
      expect(spanStarts[0]?.message).toBe('root');

      expect(spanStarts[1]?.order).toBe(2); // middle
      expect(spanStarts[1]?.message).toBe('middle');

      expect(spanStarts[2]?.order).toBe(3); // deep
      expect(spanStarts[2]?.message).toBe('deep');
    });
  });

  describe('Scope inheritance across nested library tasks', () => {
    /**
     * Per specs/01i_span_scope_attributes.md:
     * - Scoped attributes propagate to child spans
     * - Child spans inherit parent's scoped attributes
     */
    it('should propagate scoped attributes through 4 levels of nesting', async () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
        userId: S.category(),
        level: S.number(),
      });

      const module = createModuleContext({
        moduleMetadata: { gitSha: 'sha', packageName: '@test/scope', packagePath: 'scope.ts' },
        tagAttributes: schema,
      });

      let rootBuffer: SpanBuffer | undefined;

      const level4Task = module.task('level4', async (ctx) => {
        ctx.tag.level(4);
        // Log a message - should include inherited scoped attributes
        ctx.log.info('at level 4');
        return ctx.ok('done');
      });

      const level3Task = module.task('level3', async (ctx) => {
        ctx.tag.level(3);
        ctx.log.info('at level 3');
        await level4Task(ctx);
        return ctx.ok('done');
      });

      const level2Task = module.task('level2', async (ctx) => {
        // Add more scoped attributes
        ctx.scope({ userId: 'user-456' });
        ctx.tag.level(2);
        ctx.log.info('at level 2');
        await level3Task(ctx);
        return ctx.ok('done');
      });

      const rootTask = module.task('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        // Set scoped attributes at root
        ctx.scope({ requestId: 'req-abc' });
        ctx.tag.level(1);
        ctx.log.info('at root');
        await level2Task(ctx);
        return ctx.ok('done');
      });

      const requestCtx = createRequestContext({ requestId: 'req-1' }, testFlags, mockEvaluator, {});
      await rootTask(requestCtx);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Extract info log entries
      const rows = Array.from({ length: table.numRows }, (_, i) => table.get(i)?.toJSON());
      const infoLogs = rows.filter((r) => r?.entry_type === 'info');

      // All info logs should have requestId (scoped at root)
      for (const log of infoLogs) {
        expect(log?.requestId).toBe('req-abc');
      }

      // Logs at level 2 and below should also have userId
      const level2Log = infoLogs.find((r) => r?.message === 'at level 2');
      const level3Log = infoLogs.find((r) => r?.message === 'at level 3');
      const level4Log = infoLogs.find((r) => r?.message === 'at level 4');

      expect(level2Log?.userId).toBe('user-456');
      expect(level3Log?.userId).toBe('user-456');
      expect(level4Log?.userId).toBe('user-456');
    });
  });
});
