/**
 * Tests for RemappedBufferView - the view that maps prefixed column names
 * to unprefixed column names for tree traversal during Arrow conversion.
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Libraries write to unprefixed columns (status, method)
 * - Arrow conversion accesses via prefixed names (http_status, http_method)
 * - RemappedBufferView bridges this gap for tree traversal
 */

import { describe, expect, it } from 'bun:test';
import { createPrefixMapping, generateRemappedBufferViewClass, moduleContextFactory } from '../library.js';
import { createModuleContext, createRequestContext } from '../lmao.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import { InMemoryFlagEvaluator } from '../schema/evaluator.js';
import type { TraceId } from '../traceId.js';
import type { SpanBuffer } from '../types.js';

describe('generateRemappedBufferViewClass', () => {
  describe('class generation', () => {
    it('should generate a valid JavaScript class', () => {
      // Mapping is prefixed → unprefixed (e.g., 'http_status' → 'status')
      // This allows Arrow conversion to access via prefixed names while buffer has unprefixed columns
      const mapping = { http_status: 'status', http_method: 'method' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      expect(typeof ViewClass).toBe('function');
      expect(ViewClass.prototype).toBeDefined();
    });

    it('should create instances with the wrapped buffer', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      // Create a mock buffer
      const mockBuffer = {
        children: [],
        next: undefined,
        writeIndex: 5,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: ['test'],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 123n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: { name: 'test' },
        getColumnIfAllocated: (name: string) => mockBuffer[`${name}_values` as keyof typeof mockBuffer],
        getNullsIfAllocated: (name: string) => mockBuffer[`${name}_nulls` as keyof typeof mockBuffer],
      } as unknown as SpanBuffer;

      const view = new ViewClass(mockBuffer);
      expect(view).toBeDefined();
    });

    it('should cache generated classes by mapping key', () => {
      const mapping = { http_status: 'status' };

      const ViewClass1 = generateRemappedBufferViewClass(mapping);
      const ViewClass2 = generateRemappedBufferViewClass(mapping);

      // Should return cached class for same mapping
      expect(ViewClass1).toBe(ViewClass2);
    });

    it('should generate different classes for different mappings', () => {
      const mapping1 = { http_status: 'status' };
      const mapping2 = { db_query: 'query' };

      const ViewClass1 = generateRemappedBufferViewClass(mapping1);
      const ViewClass2 = generateRemappedBufferViewClass(mapping2);

      expect(ViewClass1).not.toBe(ViewClass2);
    });
  });

  describe('pass-through properties', () => {
    const createMockBuffer = (): SpanBuffer => {
      const buffer = {
        children: [{ spanId: 2 }, { spanId: 3 }],
        next: { spanId: 10 },
        writeIndex: 7,
        timestamps: new BigInt64Array([1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n]),
        operations: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]),
        message_values: ['msg1', 'msg2'],
        message_nulls: new Uint8Array([0b11]),
        lineNumber_values: new Float64Array([10, 20]),
        lineNumber_nulls: new Uint8Array([0b11]),
        errorCode_values: ['ERR001'],
        errorCode_nulls: new Uint8Array([0b01]),
        exceptionStack_values: ['stack trace'],
        exceptionStack_nulls: new Uint8Array([0b01]),
        ffValue_values: ['true'],
        ffValue_nulls: new Uint8Array([0b01]),
        traceId: 'abc123def456789012345678901234ab' as TraceId,
        threadId: 99n,
        spanId: 42,
        parentSpanId: 41,
        _identity: new Uint8Array(32),
        task: { name: 'test-task', module: { name: 'test-module' } },
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;
      return buffer;
    };

    it('should pass through tree traversal properties unchanged', () => {
      // Mapping direction: prefixed → unprefixed
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view.children).toBe(buffer.children);
      expect(view.next).toBe(buffer.next);
    });

    it('should pass through writeIndex', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view.writeIndex).toBe(7);
    });

    it('should pass through system columns', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view.timestamps).toBe(buffer.timestamps);
      expect(view.operations).toBe(buffer.operations);
      expect(view.message_values).toBe(buffer.message_values);
      expect(view.message_nulls).toBe(buffer.message_nulls);
      expect(view.lineNumber_values).toBe(buffer.lineNumber_values);
      expect(view.lineNumber_nulls).toBe(buffer.lineNumber_nulls);
      expect(view.errorCode_values).toBe(buffer.errorCode_values);
      expect(view.errorCode_nulls).toBe(buffer.errorCode_nulls);
      expect(view.exceptionStack_values).toBe(buffer.exceptionStack_values);
      expect(view.exceptionStack_nulls).toBe(buffer.exceptionStack_nulls);
      expect(view.ffValue_values).toBe(buffer.ffValue_values);
      expect(view.ffValue_nulls).toBe(buffer.ffValue_nulls);
    });

    it('should pass through identity properties', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view.traceId).toBe(buffer.traceId);
      expect(view.threadId).toBe(buffer.threadId);
      expect(view.spanId).toBe(buffer.spanId);
      expect(view.parentSpanId).toBe(buffer.parentSpanId);
      expect(view._identity).toBe(buffer._identity);
    });

    it('should pass through task metadata', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view.task).toBe(buffer.task);
    });
  });

  describe('column remapping', () => {
    it('should remap prefixed column names to unprefixed in getColumnIfAllocated', () => {
      // Mapping: prefixed → unprefixed. View receives 'http_status', returns 'status' column.
      const mapping = { http_status: 'status', http_method: 'method' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      const statusValues = new Float64Array([200, 404, 500]);
      const methodValues = ['GET', 'POST', 'DELETE'];

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 3,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        status_values: statusValues, // unprefixed column
        method_values: methodValues, // unprefixed column
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      // Access via PREFIXED name, should return UNPREFIXED column
      expect(view.getColumnIfAllocated('http_status')).toBe(statusValues);
      expect(view.getColumnIfAllocated('http_method')).toBe(methodValues);
    });

    it('should remap prefixed column names to unprefixed in getNullsIfAllocated', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      const statusNulls = new Uint8Array([0b111]);

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 3,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        status_nulls: statusNulls,
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      // Access via PREFIXED name, should return UNPREFIXED null bitmap
      expect(view.getNullsIfAllocated('http_status')).toBe(statusNulls);
    });

    it('should pass through unmapped column names', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      const userIdValues = ['user1', 'user2'];

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 2,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        userId_values: userIdValues,
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      // Non-mapped columns pass through unchanged
      expect(view.getColumnIfAllocated('userId')).toBe(userIdValues);
    });

    it('should return undefined for non-existent columns', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 0,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        getColumnIfAllocated() {
          return undefined;
        },
        getNullsIfAllocated() {
          return undefined;
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      // Columns that don't exist return undefined (correct for Arrow conversion)
      expect(view.getColumnIfAllocated('http_status')).toBeUndefined();
      expect(view.getColumnIfAllocated('nonexistent')).toBeUndefined();
      expect(view.getNullsIfAllocated('http_status')).toBeUndefined();
    });
  });

  describe('integration with createPrefixMapping', () => {
    it('should work with inverted schema-derived prefix mapping', () => {
      const schema = defineTagAttributes({
        status: S.number(),
        method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
        url: S.text(),
      });

      // createPrefixMapping returns unprefixed → prefixed
      const prefixMapping = createPrefixMapping(schema, 'http');

      expect(prefixMapping).toEqual({
        status: 'http_status',
        method: 'http_method',
        url: 'http_url',
      });

      // For RemappedBufferView, we need the inverse: prefixed → unprefixed
      const invertedMapping: Record<string, string> = {};
      for (const [unprefixed, prefixed] of Object.entries(prefixMapping)) {
        invertedMapping[prefixed] = unprefixed;
      }

      const ViewClass = generateRemappedBufferViewClass(invertedMapping);

      const statusValues = new Float64Array([200]);
      const methodValues = new Uint8Array([0]); // enum index

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        status_values: statusValues,
        method_values: methodValues,
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      // Prefixed access returns unprefixed columns
      expect(view.getColumnIfAllocated('http_status')).toBe(statusValues);
      expect(view.getColumnIfAllocated('http_method')).toBe(methodValues);
    });
  });
});

describe('nested tasks with library modules - 4+ levels deep', () => {
  // Setup common schemas and evaluator
  const appSchema = defineTagAttributes({
    requestId: S.category(),
    userId: S.category(),
  });

  const httpSchema = defineTagAttributes({
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  });

  const dbSchema = defineTagAttributes({
    query: S.text(),
    table: S.category(),
  });

  const cacheSchema = defineTagAttributes({
    key: S.category(),
    hit: S.boolean(),
  });

  const featureFlags = defineFeatureFlags({
    testFlag: S.boolean().default(false).sync(),
  });

  const flagEvaluator = new InMemoryFlagEvaluator({ testFlag: true });
  const env = { region: 'us-east-1' };

  describe('without library module constructors', () => {
    it('should handle 4-level nested tasks with correct buffer hierarchy', async () => {
      // All using app schema directly (no library prefixing)
      const appModule = createModuleContext({
        moduleMetadata: {
          gitSha: 'test',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        tagAttributes: appSchema,
      });

      const buffers: { level: number; spanId: number; parentSpanId: number }[] = [];

      const level4Task = appModule.task('level4-task', async (ctx) => {
        buffers.push({
          level: 4,
          spanId: ctx.buffer.spanId,
          parentSpanId: ctx.buffer.parentSpanId,
        });
        ctx.tag.requestId('req-level4');
        return ctx.ok({ level: 4 });
      });

      const level3Task = appModule.task('level3-task', async (ctx) => {
        buffers.push({
          level: 3,
          spanId: ctx.buffer.spanId,
          parentSpanId: ctx.buffer.parentSpanId,
        });
        ctx.tag.requestId('req-level3');

        const result = await ctx.span('nested-level4', async (childCtx) => {
          return level4Task(childCtx);
        });
        return ctx.ok({ level: 3, child: result });
      });

      const level2Task = appModule.task('level2-task', async (ctx) => {
        buffers.push({
          level: 2,
          spanId: ctx.buffer.spanId,
          parentSpanId: ctx.buffer.parentSpanId,
        });
        ctx.tag.userId('user-level2');

        const result = await ctx.span('nested-level3', async (childCtx) => {
          return level3Task(childCtx);
        });
        return ctx.ok({ level: 2, child: result });
      });

      const level1Task = appModule.task('level1-task', async (ctx) => {
        buffers.push({
          level: 1,
          spanId: ctx.buffer.spanId,
          parentSpanId: ctx.buffer.parentSpanId,
        });
        ctx.tag.requestId('req-root').userId('user-root');

        const result = await ctx.span('nested-level2', async (childCtx) => {
          return level2Task(childCtx);
        });
        return ctx.ok({ level: 1, child: result });
      });

      const reqCtx = createRequestContext({ requestId: 'test' }, featureFlags, flagEvaluator, env);

      const result = await level1Task(reqCtx);
      expect(result.success).toBe(true);

      // Verify buffer hierarchy - all 4 levels executed
      expect(buffers).toHaveLength(4);
      expect(buffers[0].level).toBe(1);
      expect(buffers[1].level).toBe(2);
      expect(buffers[2].level).toBe(3);
      expect(buffers[3].level).toBe(4);

      // Root has no parent (parentSpanId = 0)
      expect(buffers[0].parentSpanId).toBe(0);

      // Verify all spanIds are unique and non-zero
      const spanIds = buffers.map((b) => b.spanId);
      expect(new Set(spanIds).size).toBe(4); // All unique
      for (const spanId of spanIds) {
        expect(spanId).toBeGreaterThan(0);
      }

      // Verify each non-root level has a valid parent spanId
      for (let i = 1; i < buffers.length; i++) {
        expect(buffers[i].parentSpanId).toBeGreaterThan(0);
      }
    });

    it('should propagate traceId through all nested levels', async () => {
      const appModule = createModuleContext({
        moduleMetadata: {
          gitSha: 'test',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        tagAttributes: appSchema,
      });

      const traceIds: string[] = [];

      const level4Task = appModule.task('level4', async (ctx) => {
        traceIds.push(ctx.buffer.traceId);
        return ctx.ok({});
      });

      const level3Task = appModule.task('level3', async (ctx) => {
        traceIds.push(ctx.buffer.traceId);
        return ctx.span('to-level4', () => level4Task(ctx));
      });

      const level2Task = appModule.task('level2', async (ctx) => {
        traceIds.push(ctx.buffer.traceId);
        return ctx.span('to-level3', () => level3Task(ctx));
      });

      const level1Task = appModule.task('level1', async (ctx) => {
        traceIds.push(ctx.buffer.traceId);
        return ctx.span('to-level2', () => level2Task(ctx));
      });

      const reqCtx = createRequestContext({ requestId: 'test' }, featureFlags, flagEvaluator, env);

      await level1Task(reqCtx);

      // All levels should have the same traceId
      expect(traceIds).toHaveLength(4);
      const rootTraceId = traceIds[0];
      expect(rootTraceId).toMatch(/^[a-f0-9]{32}$/);
      for (const tid of traceIds) {
        expect(tid).toBe(rootTraceId);
      }
    });
  });

  describe('with library module constructors (prefixed schemas)', () => {
    it('should handle 4-level deep nesting with mixed prefixed libraries', async () => {
      // Level 1: App module (no prefix)
      const appModule = createModuleContext({
        moduleMetadata: {
          gitSha: 'test',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        tagAttributes: appSchema,
      });

      // Level 2: HTTP library (http_ prefix)
      const httpModule = moduleContextFactory(
        'http',
        {
          gitSha: 'test',
          packageName: '@test/http',
          packagePath: 'src/http.ts',
        },
        httpSchema,
      );

      // Level 3: DB library (db_ prefix)
      const dbModule = moduleContextFactory(
        'db',
        {
          gitSha: 'test',
          packageName: '@test/db',
          packagePath: 'src/db.ts',
        },
        dbSchema,
      );

      // Level 4: Cache library (cache_ prefix)
      const cacheModule = moduleContextFactory(
        'cache',
        {
          gitSha: 'test',
          packageName: '@test/cache',
          packagePath: 'src/cache.ts',
        },
        cacheSchema,
      );

      const results: { level: number; module: string }[] = [];

      // Level 4: Cache operation (deepest)
      const cacheTask = cacheModule.task('cache-lookup', async (ctx) => {
        results.push({ level: 4, module: 'cache' });
        // Library writes with clean names (remapped to cache_key, cache_hit)
        ctx.tag.with({ key: 'user:123', hit: true });
        return ctx.ok({ cached: true });
      });

      // Level 3: DB operation
      const dbTask = dbModule.task('db-query', async (ctx) => {
        results.push({ level: 3, module: 'db' });
        // Library writes with clean names (remapped to db_query, db_table)
        ctx.tag.with({ query: 'SELECT * FROM users', table: 'users' });
        // Call cache task directly (library tasks get RequestContext, not SpanContext)
        const cacheResult = await cacheTask(ctx);
        return ctx.ok({ query: 'done', cache: cacheResult });
      });

      // Level 2: HTTP handler
      const httpTask = httpModule.task('http-request', async (ctx) => {
        results.push({ level: 2, module: 'http' });
        // Library writes with clean names (remapped to http_status, http_method)
        ctx.tag.with({ status: 200, method: 'GET' });
        // Call DB task directly
        const dbResult = await dbTask(ctx);
        return ctx.ok({ status: 200, data: dbResult });
      });

      // Level 1: App entry point
      const appTask = appModule.task('app-handler', async (ctx) => {
        results.push({ level: 1, module: 'app' });
        // App writes with clean names (no prefix needed)
        ctx.tag.requestId('req-123').userId('user-456');
        // Call HTTP library task via span (app module has full SpanContext)
        const httpResult = await ctx.span('process-request', () => httpTask(ctx));
        return ctx.ok({ success: true, result: httpResult });
      });

      const reqCtx = createRequestContext({ requestId: 'test' }, featureFlags, flagEvaluator, env);

      const result = await appTask(reqCtx);
      expect(result.success).toBe(true);

      // Verify all levels executed in order
      expect(results).toHaveLength(4);
      expect(results[0]).toEqual({ level: 1, module: 'app' });
      expect(results[1]).toEqual({ level: 2, module: 'http' });
      expect(results[2]).toEqual({ level: 3, module: 'db' });
      expect(results[3]).toEqual({ level: 4, module: 'cache' });
    });

    it('should maintain correct prefix mappings at each level', () => {
      // Verify that moduleContextFactory creates correct prefix mappings
      const httpModule = moduleContextFactory(
        'http',
        { gitSha: 'test', packageName: '@test/http', packagePath: 'src/http.ts' },
        httpSchema,
      );

      const dbModule = moduleContextFactory(
        'db',
        { gitSha: 'test', packageName: '@test/db', packagePath: 'src/db.ts' },
        dbSchema,
      );

      // Verify prefix mappings
      expect(httpModule.prefixMapping).toEqual({
        status: 'http_status',
        method: 'http_method',
      });

      expect(dbModule.prefixMapping).toEqual({
        query: 'db_query',
        table: 'db_table',
      });

      // Verify clean schemas are preserved
      expect(httpModule.cleanSchema.status).toBeDefined();
      expect(httpModule.cleanSchema.method).toBeDefined();

      expect(dbModule.cleanSchema.query).toBeDefined();
      expect(dbModule.cleanSchema.table).toBeDefined();
    });

    it('should handle 5-level deep nesting with alternating library types', async () => {
      // Create modules for 5 levels
      const modules = [
        createModuleContext({
          moduleMetadata: { gitSha: 'test', packageName: '@test/app', packagePath: 'src/app.ts' },
          tagAttributes: appSchema,
        }),
        moduleContextFactory(
          'http',
          { gitSha: 'test', packageName: '@test/http', packagePath: 'src/http.ts' },
          httpSchema,
        ),
        moduleContextFactory('db', { gitSha: 'test', packageName: '@test/db', packagePath: 'src/db.ts' }, dbSchema),
        moduleContextFactory(
          'cache',
          { gitSha: 'test', packageName: '@test/cache', packagePath: 'src/cache.ts' },
          cacheSchema,
        ),
        moduleContextFactory(
          'http2',
          { gitSha: 'test', packageName: '@test/http2', packagePath: 'src/http2.ts' },
          httpSchema,
        ),
      ];

      const executionOrder: number[] = [];

      // Build nested task chain - use with() for type-erased contexts
      // Library tasks call each other directly (no ctx.span - that's only on full SpanContext)
      const level5Task = modules[4].task('level5', async (ctx) => {
        executionOrder.push(5);
        ctx.tag.with({ status: 201, method: 'POST' });
        return ctx.ok({ level: 5 });
      });

      const level4Task = modules[3].task('level4', async (ctx) => {
        executionOrder.push(4);
        ctx.tag.with({ key: 'final', hit: false });
        return level5Task(ctx);
      });

      const level3Task = modules[2].task('level3', async (ctx) => {
        executionOrder.push(3);
        ctx.tag.with({ query: 'UPDATE', table: 'orders' });
        return level4Task(ctx);
      });

      const level2Task = modules[1].task('level2', async (ctx) => {
        executionOrder.push(2);
        ctx.tag.with({ status: 200, method: 'GET' });
        return level3Task(ctx);
      });

      const level1Task = modules[0].task('level1', async (ctx) => {
        executionOrder.push(1);
        ctx.tag.with({ requestId: 'req', userId: 'usr' });
        return ctx.span('to-2', () => level2Task(ctx));
      });

      const reqCtx = createRequestContext({ requestId: 'test' }, featureFlags, flagEvaluator, env);

      const result = await level1Task(reqCtx);
      expect(result.success).toBe(true);

      // Verify execution order
      expect(executionOrder).toEqual([1, 2, 3, 4, 5]);
    });
  });

  describe('buffer tree structure with RemappedBufferView', () => {
    it('should allow tree traversal through RemappedBufferView instances', () => {
      // Simulate what Arrow conversion would see
      // Mapping: prefixed → unprefixed (view.getColumnIfAllocated('http_status') → buffer.getColumnIfAllocated('status'))
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      // Create child buffer mock
      const childBuffer = {
        children: [],
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: ['child-span'],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 2,
        parentSpanId: 1,
        _identity: new Uint8Array(32),
        task: { name: 'child-task' },
        status_values: new Float64Array([404]),
        status_nulls: new Uint8Array([0b01]),
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      // Wrap child in RemappedBufferView
      const childView = new ViewClass(childBuffer);

      // Create parent buffer with child as RemappedBufferView
      const parentBuffer = {
        children: [childView], // Parent sees RemappedBufferView, not raw buffer
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: ['parent-span'],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: { name: 'parent-task' },
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      // Simulate tree traversal (as Arrow conversion would do)
      expect(parentBuffer.children).toHaveLength(1);

      const child = parentBuffer.children[0] as SpanBuffer;

      // Access via prefixed name through the view
      expect(child.getColumnIfAllocated('http_status')).toEqual(new Float64Array([404]));

      // System columns pass through unchanged
      expect(child.spanId).toBe(2);
      expect(child.parentSpanId).toBe(1);
      expect(child.traceId).toBe('12345678901234567890123456789012' as TraceId);
    });

    it('should handle nested RemappedBufferView in children array', () => {
      // Different prefixes for different libraries
      // Mapping: prefixed → unprefixed
      const httpMapping = { http_status: 'status' };
      const dbMapping = { db_query: 'query' };

      const HttpViewClass = generateRemappedBufferViewClass(httpMapping);
      const DbViewClass = generateRemappedBufferViewClass(dbMapping);

      // Grandchild: DB library buffer
      const grandchildBuffer = {
        children: [],
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: ['db-query'],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 3,
        parentSpanId: 2,
        _identity: new Uint8Array(32),
        task: { name: 'db-task' },
        query_values: ['SELECT * FROM users'],
        query_nulls: new Uint8Array([0b01]),
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const grandchildView = new DbViewClass(grandchildBuffer);

      // Child: HTTP library buffer with DB grandchild
      const childBuffer = {
        children: [grandchildView],
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: ['http-request'],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 2,
        parentSpanId: 1,
        _identity: new Uint8Array(32),
        task: { name: 'http-task' },
        status_values: new Float64Array([200]),
        status_nulls: new Uint8Array([0b01]),
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const childView = new HttpViewClass(childBuffer);

      // Root: App buffer with HTTP child
      const rootBuffer = {
        children: [childView],
        writeIndex: 1,
        spanId: 1,
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
      } as unknown as SpanBuffer;

      // Traverse the tree
      const httpChild = rootBuffer.children[0] as SpanBuffer;
      const dbGrandchild = httpChild.children[0] as SpanBuffer;

      // Each level returns correct prefixed columns
      expect(httpChild.getColumnIfAllocated('http_status')).toEqual(new Float64Array([200]));
      expect(dbGrandchild.getColumnIfAllocated('db_query')).toEqual(['SELECT * FROM users']);
    });
  });

  describe('edge cases', () => {
    it('should handle empty prefix mapping', () => {
      const ViewClass = generateRemappedBufferViewClass({});

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        status_values: new Float64Array([200]),
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      // No remapping, pass through directly
      expect(view.getColumnIfAllocated('status')).toEqual(new Float64Array([200]));
    });

    it('should handle special characters in column names', () => {
      // Mapping: prefixed → unprefixed
      const mapping = { prefix_field_with_underscores: 'field_with_underscores' };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        field_with_underscores_values: ['test'],
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      expect(view.getColumnIfAllocated('prefix_field_with_underscores')).toEqual(['test']);
    });

    it('should handle large number of columns in mapping', () => {
      // Mapping: prefixed → unprefixed
      const mapping: Record<string, string> = {};
      for (let i = 0; i < 100; i++) {
        mapping[`prefix_field${i}`] = `field${i}`;
      }

      const ViewClass = generateRemappedBufferViewClass(mapping);

      const buffer = {
        children: [],
        next: undefined,
        writeIndex: 1,
        timestamps: new BigInt64Array(8),
        operations: new Uint8Array(8),
        message_values: [],
        message_nulls: new Uint8Array(1),
        traceId: '12345678901234567890123456789012' as TraceId,
        threadId: 1n,
        spanId: 1,
        parentSpanId: 0,
        _identity: new Uint8Array(32),
        task: {},
        field50_values: ['value50'],
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      const view = new ViewClass(buffer);

      expect(view.getColumnIfAllocated('prefix_field50')).toEqual(['value50']);
    });
  });
});
