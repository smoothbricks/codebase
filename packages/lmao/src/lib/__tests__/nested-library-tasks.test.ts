/**
 * Tests for nested library tasks with prefix remapping
 *
 * Per specs/lmao/01e_library_integration_pattern.md:
 * - Libraries define clean schemas (status, method)
 * - Prefixes applied at composition time (http_status, http_method)
 * - Library code writes to clean names, stored in prefixed columns
 *
 * Per specs/lmao/01k_tree_walker_and_arrow_conversion.md:
 * - Tree traversal visits all buffers including children
 * - Arrow conversion uses shared dictionaries across all buffers
 *
 * ## Migration Note
 * The old `createTrace()` function has been removed from `defineOpContext()`.
 * Tests now use the Tracer class directly with the context from `defineOpContext()`.
 * For nested ops with different schemas, use a combined schema or inline functions.
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import './test-helpers.js';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../types.js';
import { createTestTracerOptions } from './test-helpers.js';

function extractRows(
  table: ReturnType<typeof convertSpanTreeToArrowTable>,
  columns: readonly string[],
): Array<Record<string, unknown>> {
  return Array.from({ length: table.numRows }, (_, rowIndex) => {
    const row: Record<string, unknown> = {};
    for (const columnName of columns) {
      const column = table.getChild(columnName);
      if (column) {
        row[columnName] = column.get(rowIndex) as unknown;
      }
    }
    return row;
  });
}

describe('Nested Library Tasks', () => {
  describe('4-level nesting WITHOUT library prefixes (regular module contexts)', () => {
    /**
     * Test scenario: App -> Module1.op -> Module2.op -> Module3.op -> Module4.op
     * All modules use the same schema (no prefixing)
     * Verifies tree structure and Arrow conversion
     */
    it('should create proper parent-child hierarchy with 4 levels of nesting', async () => {
      // Shared schema for all modules
      const sharedSchema = defineLogSchema({
        userId: S.category(),
        operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
        depth: S.number(),
      });

      // Create op context factory with the shared schema
      const opContext = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp } = opContext;
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      // Capture buffers for verification
      let rootBuffer: AnySpanBuffer | undefined;
      let level2Buffer: AnySpanBuffer | undefined;
      let level3Buffer: AnySpanBuffer | undefined;
      let level4Buffer: AnySpanBuffer | undefined;

      // Level 4 op (deepest)
      const level4Op = defineOp('level4-op', async (ctx) => {
        level4Buffer = ctx.buffer;
        ctx.tag.depth(4);
        ctx.tag.operation('DELETE');
        return ctx.ok('level4-done');
      });

      // Level 3 op
      const level3Op = defineOp('level3-op', async (ctx) => {
        level3Buffer = ctx.buffer;
        ctx.tag.depth(3);
        ctx.tag.operation('UPDATE');

        // Call level 4 op
        await ctx.span('level4-op', level4Op);

        return ctx.ok('level3-done');
      });

      // Level 2 op
      const level2Op = defineOp('level2-op', async (ctx) => {
        level2Buffer = ctx.buffer;
        ctx.tag.depth(2);
        ctx.tag.operation('READ');

        // Call level 3 op
        await ctx.span('level3-op', level3Op);

        return ctx.ok('level2-done');
      });

      // Root op (level 1)
      const rootOp = defineOp('root-op', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.userId('user-123');
        ctx.tag.depth(1);
        ctx.tag.operation('CREATE');

        // Call level 2 op
        await ctx.span('level2-op', level2Op);

        return ctx.ok('root-done');
      });

      // Execute the nested op chain
      const result = await trace('root-op', rootOp);

      // Verify result
      expect(result.success).toBe(true);

      // Verify all buffers were captured
      expect(rootBuffer).toBeDefined();
      expect(level2Buffer).toBeDefined();
      expect(level3Buffer).toBeDefined();
      expect(level4Buffer).toBeDefined();

      // Verify parent-child relationships
      // Root has no parent
      expect(rootBuffer?._parent).toBeUndefined();

      // Level 2's parent is root
      expect(level2Buffer?._parent).toBe(rootBuffer);

      // Level 3's parent is level 2
      expect(level3Buffer?._parent).toBe(level2Buffer);

      // Level 4's parent is level 3
      expect(level4Buffer?._parent).toBe(level3Buffer);

      // Verify children array
      expect(rootBuffer?._children).toContain(level2Buffer);
      expect(level2Buffer?._children).toContain(level3Buffer);
      expect(level3Buffer?._children).toContain(level4Buffer);
      expect(level4Buffer?._children.length).toBe(0);

      // Verify all share the same traceId
      const traceId = rootBuffer?.trace_id;
      expect(level2Buffer?.trace_id).toBe(traceId);
      expect(level3Buffer?.trace_id).toBe(traceId);
      expect(level4Buffer?.trace_id).toBe(traceId);
    });

    it('should convert 4-level nested tree to Arrow table with correct parent-child relationships', async () => {
      const sharedSchema = defineLogSchema({
        level: S.number(),
      });

      const opContext = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp } = opContext;
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      let rootBuffer: AnySpanBuffer | undefined;

      const level4Op = defineOp('level4', async (ctx) => {
        ctx.tag.level(4);
        return ctx.ok('done');
      });

      const level3Op = defineOp('level3', async (ctx) => {
        ctx.tag.level(3);
        await ctx.span('level4', level4Op);
        return ctx.ok('done');
      });

      const level2Op = defineOp('level2', async (ctx) => {
        ctx.tag.level(2);
        await ctx.span('level3', level3Op);
        return ctx.ok('done');
      });

      const rootOp = defineOp('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.level(1);
        await ctx.span('level2', level2Op);
        return ctx.ok('done');
      });

      await trace('root', rootOp);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Should have rows for all 4 levels (span-start + span-ok for each = 8 rows)
      expect(table.numRows).toBeGreaterThanOrEqual(8);

      // Extract all rows for verification
      const rows = extractRows(table, [
        'entry_type',
        'message',
        'span_id',
        'parent_span_id',
        'trace_id',
        'http_status',
        'db_status',
        'process_status',
      ]);

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

      // Root has no parent (null in Arrow)
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

  describe('4-level nesting WITH library prefixes (separate defineOpContext per library)', () => {
    /**
     * Test scenario: App -> HTTP lib op -> DB lib op -> Cache lib op -> Auth lib op
     *
     * Per specs/lmao/01e_library_integration_pattern.md:
     * - Each library defines its OWN schema with clean names (status, query, key, userId)
     * - Libraries are composed via deps with .prefix() for namespacing
     * - Effective schema has prefixed columns (http_status, db_query, cache_key, auth_userId)
     * - Library code writes to UNPREFIXED names, stored in PREFIXED columns via remapping
     *
     * This is the CORRECT pattern - 4 separate defineOpContext calls, composed via deps.
     */

    // =====================================
    // LIBRARY DEFINITIONS (separate packages in real code)
    // =====================================

    // Auth library - deepest level (level 4)
    const authSchema = defineLogSchema({
      userId: S.category(),
      role: S.category(),
    });
    const authContext = defineOpContext({ logSchema: authSchema });
    const authOps = authContext.defineOps({
      checkAuth: async (ctx) => {
        // Library writes to unprefixed names
        ctx.tag.userId('user-456').role('admin');
        return ctx.ok({ authorized: true });
      },
    });

    // Cache library - level 3 (depends on auth)
    const cacheSchema = defineLogSchema({
      key: S.category(),
      hit: S.boolean(),
    });
    const cacheContext = defineOpContext({
      logSchema: cacheSchema,
      deps: { auth: authOps.prefix('auth') },
    });
    const cacheOps = cacheContext.defineOps({
      lookup: async (ctx) => {
        ctx.tag.key('session:user-456').hit(true);
        // Call auth library via deps

        await ctx.span('auth-check', ctx.deps.auth.checkAuth);
        return ctx.ok({ cached: true });
      },
    });

    // DB library - level 2 (depends on cache)
    const dbSchema = defineLogSchema({
      query: S.text(),
      rowCount: S.number(),
    });
    const dbContext = defineOpContext({
      logSchema: dbSchema,
      deps: { cache: cacheOps.prefix('cache') },
    });
    const dbOps = dbContext.defineOps({
      execute: async (ctx) => {
        ctx.tag.query('SELECT * FROM users').rowCount(1);
        // Call cache library via deps

        await ctx.span('cache-lookup', ctx.deps.cache.lookup);
        return ctx.ok({ rows: [] });
      },
    });

    // HTTP library - level 1 (depends on db)
    const httpSchema = defineLogSchema({
      status: S.number(),
      method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    });
    const httpContext = defineOpContext({
      logSchema: httpSchema,
      deps: { db: dbOps.prefix('db') },
    });
    const httpOps = httpContext.defineOps({
      request: async (ctx) => {
        ctx.tag.status(200).method('GET');
        // Call DB library via deps

        await ctx.span('db-query', ctx.deps.db.execute);
        return ctx.ok({ response: 'ok' });
      },
    });

    // =====================================
    // APP (root level, composes all libraries)
    // =====================================
    const appSchema = defineLogSchema({
      requestId: S.category(),
    });
    const appContext = defineOpContext({
      logSchema: appSchema,
      deps: {
        http: httpOps.prefix('http'),
        db: dbOps.prefix('db'),
        cache: cacheOps.prefix('cache'),
        auth: authOps.prefix('auth'),
      },
    });

    it('should compose 4 separate library contexts with prefixed columns', async () => {
      const { defineOp } = appContext;

      const { trace } = new TestTracer(appContext, { ...createTestTracerOptions() });

      let rootBuffer: AnySpanBuffer | undefined;

      const appOp = defineOp('handle-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.requestId('req-123');
        // Call HTTP library via deps

        await ctx.span('http-request', ctx.deps.http.request);
        return ctx.ok({ handled: true });
      });

      const result = await trace('handle-request', appOp);

      expect(result.success).toBe(true);
      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Should have rows for all 5 levels (app + 4 libraries)
      // Each span has span-start + span-ok = 2 rows minimum
      expect(table.numRows).toBeGreaterThanOrEqual(10);

      // Verify prefixed columns exist in schema
      const fieldNames = table.schema.fields.map((f) => f.name);

      // App column
      expect(fieldNames).toContain('requestId');

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
      const { defineOp } = appContext;

      const { trace } = new TestTracer(appContext, { ...createTestTracerOptions() });

      let appBuffer: AnySpanBuffer | undefined;

      const appOp = defineOp('handle-request', async (ctx) => {
        appBuffer = ctx.buffer;
        ctx.tag.requestId('req-123');
        await ctx.span('http-request', ctx.deps.http.request);
        return ctx.ok({ handled: true });
      });

      await trace('handle-request', appOp);

      // Verify root buffer captured
      expect(appBuffer).toBeDefined();
      if (!appBuffer) throw new Error('appBuffer is undefined');

      // Walk tree to collect all buffers
      const allBuffers: AnySpanBuffer[] = [];
      const collectBuffers = (buf: AnySpanBuffer) => {
        allBuffers.push(buf);
        for (const child of buf._children) {
          collectBuffers(child);
        }
      };
      collectBuffers(appBuffer);

      // Should have 5 buffers: app, http, db, cache, auth
      expect(allBuffers.length).toBe(5);

      // Verify parent-child chain
      const httpBuffer = allBuffers.find((b) => b._children.length > 0 && b !== appBuffer);
      expect(httpBuffer).toBeDefined();
      expect(httpBuffer?._parent).toBe(appBuffer);

      // All should share same traceId
      for (const buf of allBuffers) {
        expect(buf.trace_id).toBe(appBuffer.trace_id);
      }
    });
  });

  describe('Mixed nesting (some with prefix, some without)', () => {
    /**
     * Test scenario: App -> regularModule.op -> httpModule.op (prefixed) ->
     *                regularModule2.op -> dbModule.op (prefixed)
     */
    it('should handle mixed nesting with both prefixed and non-prefixed modules', async () => {
      // Combined schema with both prefixed and non-prefixed columns
      const combinedSchema = defineLogSchema({
        // Regular module columns (no prefix)
        requestId: S.category(),
        step: S.category(),
        // HTTP library columns (prefixed)
        http_status: S.number(),
        http_url: S.text(),
        // DB library columns (prefixed)
        db_query: S.text(),
        db_table: S.category(),
      });

      const opContext = defineOpContext({
        logSchema: combinedSchema,
      });

      const { defineOp } = opContext;
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      let rootBuffer: AnySpanBuffer | undefined;

      // DB op (deepest - level 4, prefixed)
      const dbOp = defineOp('db-query', async (ctx) => {
        ctx.tag.db_query('INSERT INTO logs').db_table('logs');
        return ctx.ok('inserted');
      });

      // Regular module 2 op (level 3, no prefix)
      const processorOp = defineOp('process', async (ctx) => {
        ctx.tag.step('validation');
        await ctx.span('db-query', dbOp);
        return ctx.ok('processed');
      });

      // HTTP library op (level 2, prefixed)
      const httpOp = defineOp('fetch', async (ctx) => {
        ctx.tag.http_status(200).http_url('/api/process');
        await ctx.span('process', processorOp);
        return ctx.ok('fetched');
      });

      // Root op (level 1, no prefix)
      const rootOp = defineOp('handle-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.requestId('req-123');
        ctx.tag.step('start');
        await ctx.span('fetch', httpOp);
        return ctx.ok('handled');
      });

      const result = await trace('handle-request', rootOp);

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
      const rows = extractRows(table, ['entry_type', 'message', 'span_id', 'parent_span_id', 'trace_id']);
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
     * Per specs/lmao/01e_library_integration_pattern.md:
     * - Multiple libraries can define the same attribute name
     * - Prefixes resolve to different columns (http_status, db_status)
     * - No collision, correct data in Arrow output
     */
    it('should isolate columns with same name using different prefixes', async () => {
      // All libraries have a "status" column concept, but with different prefixes
      const combinedSchema = defineLogSchema({
        http_status: S.number(),
        db_status: S.category(),
        process_status: S.enum(['running', 'stopped', 'failed']),
      });

      const opContext = defineOpContext({
        logSchema: combinedSchema,
      });

      const { defineOp } = opContext;
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      let rootBuffer: AnySpanBuffer | undefined;

      const processOp = defineOp('run-process', async (ctx) => {
        ctx.tag.process_status('running');
        return ctx.ok('done');
      });

      const dbOp = defineOp('db-connect', async (ctx) => {
        ctx.tag.db_status('connected');
        await ctx.span('run-process', processOp);
        return ctx.ok('done');
      });

      const httpOp = defineOp('http-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.http_status(200);
        await ctx.span('db-connect', dbOp);
        return ctx.ok('done');
      });

      await trace('http-request', httpOp);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Verify all three prefixed status columns exist
      const fieldNames = table.schema.fields.map((f) => f.name);
      expect(fieldNames).toContain('http_status');
      expect(fieldNames).toContain('db_status');
      expect(fieldNames).toContain('process_status');

      const rows = extractRows(table, ['http_status', 'db_status', 'process_status']);

      const httpRow = rows.find((r) => r.http_status === 200);
      const dbRow = rows.find((r) => r.db_status === 'connected');
      const processRow = rows.find((r) => r.process_status === 'running');

      expect(httpRow).toBeDefined();
      expect(dbRow).toBeDefined();
      expect(processRow).toBeDefined();

      // Verify no cross-contamination - each task only wrote to its own status column.
      expect(httpRow?.db_status).toBeNull();
      expect(httpRow?.process_status).toBeNull();

      expect(dbRow?.http_status).toBeNull();
      expect(dbRow?.process_status).toBeNull();

      expect(processRow?.http_status).toBeNull();
      expect(processRow?.db_status).toBeNull();
    });
  });

  describe('Tree traversal verification (walkSpanTree)', () => {
    /**
     * Per specs/lmao/01k_tree_walker_and_arrow_conversion.md:
     * - walkSpanTree visits all buffers including children
     * - Visits overflow chains (buffer._overflow)
     * - Depth-first pre-order traversal
     */
    it('should visit all buffers in tree during Arrow conversion', async () => {
      const schema = defineLogSchema({
        nodeId: S.category(),
      });

      const opContext = defineOpContext({
        logSchema: schema,
      });

      const { defineOp } = opContext;
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      let rootBuffer: AnySpanBuffer | undefined;

      // Create a tree with multiple children at each level
      const leafOp = defineOp('leaf', async (ctx) => {
        ctx.tag.nodeId('leaf');
        return ctx.ok('done');
      });

      const child2Op = defineOp('child2', async (ctx) => {
        ctx.tag.nodeId('child2');
        await ctx.span('leaf', leafOp); // child2 -> leaf
        return ctx.ok('done');
      });

      const child1Op = defineOp('child1', async (ctx) => {
        ctx.tag.nodeId('child1');
        await ctx.span('leaf', leafOp); // child1 -> leaf (another leaf)
        return ctx.ok('done');
      });

      const rootOp = defineOp('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.nodeId('root');
        await ctx.span('child1', child1Op);
        await ctx.span('child2', child2Op);
        return ctx.ok('done');
      });

      await trace('root', rootOp);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Should have all nodes: root, child1, child2, leaf, leaf (2 leaves)
      // Each span has span-start + span-ok = 2 rows
      // Total: 5 spans * 2 = 10 rows
      expect(table.numRows).toBe(10);

      // Extract rows
      const rows = extractRows(table, ['entry_type', 'message', 'nodeId']);
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
      const schema = defineLogSchema({
        order: S.number(),
      });

      const opContext = defineOpContext({
        logSchema: schema,
      });

      const { defineOp } = opContext;
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      let rootBuffer: AnySpanBuffer | undefined;
      let order = 0;

      const deepOp = defineOp('deep', async (ctx) => {
        ctx.tag.order(++order);
        return ctx.ok('done');
      });

      const middleOp = defineOp('middle', async (ctx) => {
        ctx.tag.order(++order);
        await ctx.span('deep', deepOp);
        return ctx.ok('done');
      });

      const rootOp = defineOp('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.order(++order);
        await ctx.span('middle', middleOp);
        return ctx.ok('done');
      });

      await trace('root', rootOp);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Extract span-start rows in table order
      const rows = extractRows(table, ['entry_type', 'message', 'order']);
      const spanStarts = rows.filter((r) => r?.entry_type === 'span-start');

      // Per specs/lmao/01k - depth-first pre-order: parent before children
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
     * Per specs/lmao/01i_span_scope_attributes.md:
     * - Scoped attributes propagate to child spans
     * - Child spans inherit parent's scoped attributes
     */
    it('should propagate scoped attributes through 4 levels of nesting', async () => {
      const schema = defineLogSchema({
        requestId: S.category(),
        userId: S.category(),
        level: S.number(),
      });

      const opContext = defineOpContext({
        logSchema: schema,
      });

      const { defineOp } = opContext;
      const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });

      let rootBuffer: AnySpanBuffer | undefined;

      const level4Op = defineOp('level4', async (ctx) => {
        ctx.tag.level(4);
        // Log a message - should include inherited scoped attributes
        ctx.log.info('at level 4');
        return ctx.ok('done');
      });

      const level3Op = defineOp('level3', async (ctx) => {
        ctx.tag.level(3);
        ctx.log.info('at level 3');
        await ctx.span('level4', level4Op);
        return ctx.ok('done');
      });

      const level2Op = defineOp('level2', async (ctx) => {
        // Add more scoped attributes
        ctx.setScope({ userId: 'user-456' });
        ctx.tag.level(2);
        ctx.log.info('at level 2');
        await ctx.span('level3', level3Op);
        return ctx.ok('done');
      });

      const rootOp = defineOp('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        // Set scoped attributes at root
        ctx.setScope({ requestId: 'req-abc' });
        ctx.tag.level(1);
        ctx.log.info('at root');
        await ctx.span('level2', level2Op);
        return ctx.ok('done');
      });

      await trace('root', rootOp);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Extract info log entries
      const rows = extractRows(table, ['entry_type', 'message', 'requestId', 'userId', 'scopeValue']);
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
