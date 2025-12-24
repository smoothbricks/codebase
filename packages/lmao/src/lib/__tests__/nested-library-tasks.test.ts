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
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import type { AnySpanBuffer } from '../types.js';

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

      // Create 4 op context factories with the same schema
      const { defineOp: defineOp1, createTrace: createTrace1 } = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp: defineOp2 } = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp: defineOp3 } = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp: defineOp4 } = defineOpContext({
        logSchema: sharedSchema,
      });

      // Capture buffers for verification
      let rootBuffer: AnySpanBuffer | undefined;
      let level2Buffer: AnySpanBuffer | undefined;
      let level3Buffer: AnySpanBuffer | undefined;
      let level4Buffer: AnySpanBuffer | undefined;

      // Level 4 op (deepest)
      const level4Op = defineOp4('level4-op', async (ctx) => {
        level4Buffer = ctx.buffer;
        ctx.tag.depth(4);
        ctx.tag.operation('DELETE');
        return ctx.ok('level4-done');
      });

      // Level 3 op
      const level3Op = defineOp3('level3-op', async (ctx) => {
        level3Buffer = ctx.buffer;
        ctx.tag.depth(3);
        ctx.tag.operation('UPDATE');

        // Call level 4 op
        await ctx.span('level4-op', level4Op);

        return ctx.ok('level3-done');
      });

      // Level 2 op
      const level2Op = defineOp2('level2-op', async (ctx) => {
        level2Buffer = ctx.buffer;
        ctx.tag.depth(2);
        ctx.tag.operation('READ');

        // Call level 3 op
        await ctx.span('level3-op', level3Op);

        return ctx.ok('level2-done');
      });

      // Root op (level 1)
      const rootOp = defineOp1('root-op', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.userId('user-123');
        ctx.tag.depth(1);
        ctx.tag.operation('CREATE');

        // Call level 2 op
        await ctx.span('level2-op', level2Op);

        return ctx.ok('root-done');
      });

      // Execute the nested op chain
      const traceCtx = createTrace1({});
      const result = await traceCtx.span('root-op', rootOp);

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

      const { defineOp: defineOp1, createTrace: createTrace1 } = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp: defineOp2 } = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp: defineOp3 } = defineOpContext({
        logSchema: sharedSchema,
      });

      const { defineOp: defineOp4 } = defineOpContext({
        logSchema: sharedSchema,
      });

      let rootBuffer: AnySpanBuffer | undefined;

      const level4Op = defineOp4('level4', async (ctx) => {
        ctx.tag.level(4);
        return ctx.ok('done');
      });

      const level3Op = defineOp3('level3', async (ctx) => {
        ctx.tag.level(3);
        await ctx.span('level4', level4Op);
        return ctx.ok('done');
      });

      const level2Op = defineOp2('level2', async (ctx) => {
        ctx.tag.level(2);
        await ctx.span('level3', level3Op);
        return ctx.ok('done');
      });

      const rootOp = defineOp1('root', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.level(1);
        await ctx.span('level2', level2Op);
        return ctx.ok('done');
      });

      const traceCtx = createTrace1({});
      await traceCtx.span('root', rootOp);

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
      // Root op's span is a child of the trace root (via trace.span())
      // So it HAS a parent_span_id (the trace root's span_id)
      expect(rootSpan?.parent_span_id).toBeDefined();
      expect(typeof rootSpan?.parent_span_id).toBe('number');

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

  describe('4-level nesting WITH library prefixes (defineOpContext with prefixed columns)', () => {
    /**
     * Test scenario: App -> httpModule.op (prefix: 'http') -> dbModule.op (prefix: 'db') ->
     *                cacheModule.op (prefix: 'cache') -> authModule.op (prefix: 'auth')
     *
     * Per specs/01e_library_integration_pattern.md:
     * - Each library has its own schema with prefixed column names
     * - Library code writes to prefixed columns (ctx.tag.http_status())
     * - Columns stored with prefix (http_status, db_status, etc.)
     *
     * Uses defineOpContext() pattern with prefixed column names in schema.
     */
    it('should allow libraries to write using prefixed column names', async () => {
      // Define schemas with prefixed column names for each library
      const httpSchema = defineLogSchema({
        http_status: S.number(),
        http_method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
      });
      const dbSchema = defineLogSchema({
        db_query: S.text(),
        db_rowCount: S.number(),
      });
      const cacheSchema = defineLogSchema({
        cache_key: S.category(),
        cache_hit: S.boolean(),
      });
      const authSchema = defineLogSchema({
        auth_userId: S.category(),
        auth_role: S.category(),
      });

      // HTTP library context
      const { defineOp: defineHttpOp, createTrace: createHttpTrace } = defineOpContext({
        logSchema: httpSchema,
      });

      // DB library context
      const { defineOp: defineDbOp } = defineOpContext({
        logSchema: dbSchema,
      });

      // Cache library context
      const { defineOp: defineCacheOp } = defineOpContext({
        logSchema: cacheSchema,
      });

      // Auth library context
      const { defineOp: defineAuthOp } = defineOpContext({
        logSchema: authSchema,
      });

      let rootBuffer: AnySpanBuffer | undefined;

      // Auth op (deepest - level 4)
      const authOp = defineAuthOp('auth-check', async (ctx) => {
        ctx.tag.auth_userId('user-456').auth_role('admin');
        return ctx.ok({ authorized: true });
      });

      // Cache op (level 3)
      const cacheOp = defineCacheOp('cache-lookup', async (ctx) => {
        ctx.tag.cache_key('session:user-456').cache_hit(true);
        await ctx.span('auth-check', authOp);
        return ctx.ok({ cached: true });
      });

      // DB op (level 2)
      const dbOp = defineDbOp('db-query', async (ctx) => {
        ctx.tag.db_query('SELECT * FROM users').db_rowCount(1);
        await ctx.span('cache-lookup', cacheOp);
        return ctx.ok({ rows: [] });
      });

      // HTTP op (level 1 - root)
      const httpOp = defineHttpOp('http-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.http_status(200).http_method('GET');
        await ctx.span('db-query', dbOp);
        return ctx.ok({ response: 'ok' });
      });

      const traceCtx = createHttpTrace({});
      const result = await traceCtx.span('http-request', httpOp);

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
      const { defineOp: defineHttpOp, createTrace: createHttpTrace } = defineOpContext({
        logSchema: defineLogSchema({ http_status: S.number() }),
      });

      const { defineOp: defineDbOp } = defineOpContext({
        logSchema: defineLogSchema({ db_query: S.text() }),
      });

      const { defineOp: defineCacheOp } = defineOpContext({
        logSchema: defineLogSchema({ cache_key: S.category() }),
      });

      const { defineOp: defineAuthOp } = defineOpContext({
        logSchema: defineLogSchema({ auth_userId: S.category() }),
      });

      let httpBuffer: AnySpanBuffer | undefined;
      let dbBuffer: AnySpanBuffer | undefined;
      let cacheBuffer: AnySpanBuffer | undefined;
      let authBuffer: AnySpanBuffer | undefined;

      const authOp = defineAuthOp('auth', async (ctx) => {
        authBuffer = ctx.buffer;
        ctx.tag.auth_userId('user-1');
        return ctx.ok('done');
      });

      const cacheOp = defineCacheOp('cache', async (ctx) => {
        cacheBuffer = ctx.buffer;
        ctx.tag.cache_key('key-1');
        await ctx.span('auth', authOp);
        return ctx.ok('done');
      });

      const dbOp = defineDbOp('db', async (ctx) => {
        dbBuffer = ctx.buffer;
        ctx.tag.db_query('SELECT 1');
        await ctx.span('cache', cacheOp);
        return ctx.ok('done');
      });

      const httpOp = defineHttpOp('http', async (ctx) => {
        httpBuffer = ctx.buffer;
        ctx.tag.http_status(200);
        await ctx.span('db', dbOp);
        return ctx.ok('done');
      });

      const traceCtx = createHttpTrace({});
      await traceCtx.span('http', httpOp);

      // Verify buffers captured
      expect(httpBuffer).toBeDefined();
      expect(dbBuffer).toBeDefined();
      expect(cacheBuffer).toBeDefined();
      expect(authBuffer).toBeDefined();

      // Verify parent-child relationships
      expect(httpBuffer?._parent).toBeUndefined(); // Root
      expect(dbBuffer?._parent).toBe(httpBuffer);
      expect(cacheBuffer?._parent).toBe(dbBuffer);
      expect(authBuffer?._parent).toBe(cacheBuffer);

      // Verify children arrays
      expect(httpBuffer?._children).toContain(dbBuffer);
      expect(dbBuffer?._children).toContain(cacheBuffer);
      expect(cacheBuffer?._children).toContain(authBuffer);
      expect(authBuffer?._children.length).toBe(0);

      // Verify all share same traceId
      expect(dbBuffer?.trace_id).toBe(httpBuffer?.trace_id);
      expect(cacheBuffer?.trace_id).toBe(httpBuffer?.trace_id);
      expect(authBuffer?.trace_id).toBe(httpBuffer?.trace_id);
    });
  });

  describe('Mixed nesting (some with prefix, some without)', () => {
    /**
     * Test scenario: App -> regularModule.op -> httpModule.op (prefixed) ->
     *                regularModule2.op -> dbModule.op (prefixed)
     */
    it('should handle mixed nesting with both prefixed and non-prefixed modules', async () => {
      // Regular module schema (no prefix)
      const regularSchema = defineLogSchema({
        requestId: S.category(),
        step: S.category(),
      });

      // Library schemas (with prefix in column names)
      const httpSchema = defineLogSchema({
        http_status: S.number(),
        http_url: S.text(),
      });
      const dbSchema = defineLogSchema({
        db_query: S.text(),
        db_table: S.category(),
      });

      const { defineOp: defineRegularOp, createTrace: createRegularTrace } = defineOpContext({
        logSchema: regularSchema,
      });

      const { defineOp: defineRegularOp2 } = defineOpContext({
        logSchema: regularSchema,
      });

      const { defineOp: defineHttpOp } = defineOpContext({
        logSchema: httpSchema,
      });

      const { defineOp: defineDbOp } = defineOpContext({
        logSchema: dbSchema,
      });

      let rootBuffer: AnySpanBuffer | undefined;

      // DB op (deepest - level 4, prefixed)
      const dbOp = defineDbOp('db-query', async (ctx) => {
        ctx.tag.db_query('INSERT INTO logs').db_table('logs');
        return ctx.ok('inserted');
      });

      // Regular module 2 op (level 3, no prefix)
      const processorOp = defineRegularOp2('process', async (ctx) => {
        ctx.tag.step('validation');
        await ctx.span('db-query', dbOp);
        return ctx.ok('processed');
      });

      // HTTP library op (level 2, prefixed)
      const httpOp = defineHttpOp('fetch', async (ctx) => {
        ctx.tag.http_status(200).http_url('/api/process');
        await ctx.span('process', processorOp);
        return ctx.ok('fetched');
      });

      // Root op (level 1, no prefix)
      const rootOp = defineRegularOp('handle-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.requestId('req-123');
        ctx.tag.step('start');
        await ctx.span('fetch', httpOp);
        return ctx.ok('handled');
      });

      const traceCtx = createRegularTrace({});
      const result = await traceCtx.span('handle-request', rootOp);

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
      // All libraries have a "status" column concept, but with different prefixes
      const { defineOp: defineHttpOp, createTrace: createHttpTrace } = defineOpContext({
        logSchema: defineLogSchema({ http_status: S.number() }),
      });

      const { defineOp: defineDbOp } = defineOpContext({
        logSchema: defineLogSchema({ db_status: S.category() }),
      });

      const { defineOp: defineProcessOp } = defineOpContext({
        logSchema: defineLogSchema({ process_status: S.enum(['running', 'stopped', 'failed']) }),
      });

      let rootBuffer: AnySpanBuffer | undefined;

      const processOp = defineProcessOp('run-process', async (ctx) => {
        ctx.tag.process_status('running');
        return ctx.ok('done');
      });

      const dbOp = defineDbOp('db-connect', async (ctx) => {
        ctx.tag.db_status('connected');
        await ctx.span('run-process', processOp);
        return ctx.ok('done');
      });

      const httpOp = defineHttpOp('http-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        ctx.tag.http_status(200);
        await ctx.span('db-connect', dbOp);
        return ctx.ok('done');
      });

      const traceCtx = createHttpTrace({});
      await traceCtx.span('http-request', httpOp);

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
     * - Visits overflow chains (buffer._overflow)
     * - Depth-first pre-order traversal
     */
    it('should visit all buffers in tree during Arrow conversion', async () => {
      const schema = defineLogSchema({
        nodeId: S.category(),
      });

      const { defineOp, createTrace } = defineOpContext({
        logSchema: schema,
      });

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

      const traceCtx = createTrace({});
      await traceCtx.span('root', rootOp);

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
      const schema = defineLogSchema({
        order: S.number(),
      });

      const { defineOp, createTrace } = defineOpContext({
        logSchema: schema,
      });

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

      const traceCtx = createTrace({});
      await traceCtx.span('root', rootOp);

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
      const schema = defineLogSchema({
        requestId: S.category(),
        userId: S.category(),
        level: S.number(),
      });

      const { defineOp, createTrace } = defineOpContext({
        logSchema: schema,
      });

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

      const traceCtx = createTrace({});
      await traceCtx.span('root', rootOp);

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
