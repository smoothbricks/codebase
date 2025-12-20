/**
 * Integration tests for RemappedBufferView registration with prefixed modules
 *
 * Per specs/01e_library_integration_pattern.md and 01c_context_flow_and_op_wrappers.md:
 * - RemappedBufferView class is generated once when prefix() is called (cold path)
 * - Op._invoke() creates instances when registering child spans with parent buffers
 * - Root spans don't create RemappedBufferView (no parent to register with)
 */

import { describe, expect, it } from 'bun:test';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineModule } from '../defineModule.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import type { SpanBuffer } from '../types.js';

describe('RemappedBufferView Integration', () => {
  describe('prefix() stores remappedViewClass on ModuleContext', () => {
    it('should store remappedViewClass when prefix is applied', () => {
      const httpModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/http',
          packagePath: 'src/http.ts',
        },
        logSchema: defineLogSchema({
          status: S.number(),
          method: S.enum(['GET', 'POST']),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const prefixedHttp = httpModule.prefix('http');

      // Check that remappedViewClass is stored on the module's ModuleContext
      expect(prefixedHttp._module.remappedViewClass).toBeDefined();
      expect(typeof prefixedHttp._module.remappedViewClass).toBe('function');
    });

    it('should have undefined remappedViewClass for unprefixed modules', () => {
      const httpModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/http',
          packagePath: 'src/http.ts',
        },
        logSchema: defineLogSchema({
          status: S.number(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      // Unprefixed module should have undefined remappedViewClass
      expect(httpModule._module.remappedViewClass).toBeUndefined();
    });
  });

  describe('prefixed module ops create RemappedBufferView when registering with parent', () => {
    it('should wrap child buffer in RemappedBufferView when module has prefix', async () => {
      const appModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        logSchema: defineLogSchema({
          userId: S.category(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const httpModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/http',
          packagePath: 'src/http.ts',
        },
        logSchema: defineLogSchema({
          status: S.number(),
          method: S.enum(['GET', 'POST']),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const prefixedHttp = httpModule.prefix('http');

      let rootBuffer: SpanBuffer | undefined;
      let childBuffer: SpanBuffer | undefined;

      const httpOp = prefixedHttp.op('http-request', async (ctx) => {
        childBuffer = ctx.buffer;
        // Note: Currently prefixed modules use prefixed method names
        // RemappedSpanLogger generation (clean names → prefixed columns) can be added later
        ctx.tag.http_status(200).http_method('GET');
        return ctx.ok({ success: true });
      });

      const appOp = appModule.op('app-handler', async (ctx) => {
        rootBuffer = ctx.buffer;
        await ctx.span('http-request', httpOp);
        return ctx.ok({ done: true });
      });

      const traceCtx = appModule.traceContext({});
      await traceCtx.span('app-handler', appOp);

      expect(rootBuffer).toBeDefined();
      expect(childBuffer).toBeDefined();
      expect(rootBuffer?._children).toHaveLength(1);

      // Child should be wrapped in RemappedBufferView
      const child = rootBuffer?._children[0];
      expect(child).toBeDefined();

      // RemappedBufferView should have _buffer property pointing to actual buffer
      const view = child as unknown as { _buffer?: SpanBuffer };
      expect(view._buffer).toBe(childBuffer);
    });

    it('should push raw buffer when module has no prefix', async () => {
      const appModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        logSchema: defineLogSchema({
          userId: S.category(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const dbModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/db',
          packagePath: 'src/db.ts',
        },
        logSchema: defineLogSchema({
          query: S.text(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      let rootBuffer: SpanBuffer | undefined;
      let childBuffer: SpanBuffer | undefined;

      const dbOp = dbModule.op('db-query', async (ctx) => {
        childBuffer = ctx.buffer;
        ctx.tag.query('SELECT * FROM users');
        return ctx.ok({ rows: [] });
      });

      const appOp = appModule.op('app-handler', async (ctx) => {
        rootBuffer = ctx.buffer;
        await ctx.span('db-query', dbOp);
        return ctx.ok({ done: true });
      });

      const traceCtx = appModule.traceContext({});
      await traceCtx.span('app-handler', appOp);

      expect(rootBuffer).toBeDefined();
      expect(childBuffer).toBeDefined();
      expect(rootBuffer?._children).toHaveLength(1);

      // Child should be raw buffer (not wrapped)
      const child = rootBuffer?._children[0];
      expect(child).toBe(childBuffer);
      // Should not have _buffer property (that's only on RemappedBufferView)
      expect((child as unknown as { _buffer?: SpanBuffer })._buffer).toBeUndefined();
    });
  });

  describe('root spans from prefixed modules do not create RemappedBufferView', () => {
    it('should not create RemappedBufferView for root spans', async () => {
      const httpModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/http',
          packagePath: 'src/http.ts',
        },
        logSchema: defineLogSchema({
          status: S.number(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const prefixedHttp = httpModule.prefix('http');

      let rootBuffer: SpanBuffer | undefined;

      const httpOp = prefixedHttp.op('http-request', async (ctx) => {
        rootBuffer = ctx.buffer;
        // Note: Currently prefixed modules use prefixed method names
        ctx.tag.http_status(200);
        return ctx.ok({ success: true });
      });

      const traceCtx = prefixedHttp.traceContext({});
      await traceCtx.span('http-request', httpOp);

      expect(rootBuffer).toBeDefined();
      // Root buffer should not be wrapped (no parent to register with)
      expect(rootBuffer?._parent).toBeUndefined();
      // Root buffer should not have _buffer property
      expect((rootBuffer as unknown as { _buffer?: SpanBuffer })._buffer).toBeUndefined();
    });
  });

  describe('Arrow conversion can access prefixed columns through RemappedBufferView', () => {
    it('should allow Arrow conversion to access prefixed columns', async () => {
      const appModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        logSchema: defineLogSchema({
          userId: S.category(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const httpModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/http',
          packagePath: 'src/http.ts',
        },
        logSchema: defineLogSchema({
          status: S.number(),
          method: S.enum(['GET', 'POST']),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const prefixedHttp = httpModule.prefix('http');

      let rootBuffer: SpanBuffer | undefined;

      const httpOp = prefixedHttp.op('http-request', async (ctx) => {
        // Note: Currently prefixed modules use prefixed method names
        ctx.tag.http_status(200).http_method('GET');
        return ctx.ok({ success: true });
      });

      const appOp = appModule.op('app-handler', async (ctx) => {
        rootBuffer = ctx.buffer;
        await ctx.span('http-request', httpOp);
        return ctx.ok({ done: true });
      });

      const traceCtx = appModule.traceContext({});
      await traceCtx.span('app-handler', appOp);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Should have rows
      expect(table.numRows).toBeGreaterThan(0);

      // Verify prefixed columns exist in schema
      const fieldNames = table.schema.fields.map((f) => f.name);
      expect(fieldNames).toContain('http_status');
      expect(fieldNames).toContain('http_method');
    });
  });

  describe('nested prefixed modules', () => {
    it('should handle nested prefixed modules (http → db → cache)', async () => {
      const appModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        logSchema: defineLogSchema({
          userId: S.category(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const httpModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/http',
          packagePath: 'src/http.ts',
        },
        logSchema: defineLogSchema({
          status: S.number(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const dbModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/db',
          packagePath: 'src/db.ts',
        },
        logSchema: defineLogSchema({
          query: S.text(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const cacheModule = defineModule({
        metadata: {
          gitSha: 'test-sha',
          packageName: '@test/cache',
          packagePath: 'src/cache.ts',
        },
        logSchema: defineLogSchema({
          key: S.category(),
        }),
      })
        .ctx<Record<string, never>>({})
        .make();

      const prefixedHttp = httpModule.prefix('http');
      const prefixedDb = dbModule.prefix('db');
      const prefixedCache = cacheModule.prefix('cache');

      let rootBuffer: SpanBuffer | undefined;

      const cacheOp = prefixedCache.op('cache-get', async (ctx) => {
        // Note: Currently prefixed modules use prefixed method names
        ctx.tag.cache_key('user:123');
        return ctx.ok({ value: 'cached' });
      });

      const dbOp = prefixedDb.op('db-query', async (ctx) => {
        // Note: Currently prefixed modules use prefixed method names
        ctx.tag.db_query('SELECT * FROM users');
        await ctx.span('cache-get', cacheOp);
        return ctx.ok({ rows: [] });
      });

      const httpOp = prefixedHttp.op('http-request', async (ctx) => {
        // Note: Currently prefixed modules use prefixed method names
        ctx.tag.http_status(200);
        await ctx.span('db-query', dbOp);
        return ctx.ok({ success: true });
      });

      const appOp = appModule.op('app-handler', async (ctx) => {
        rootBuffer = ctx.buffer;
        await ctx.span('http-request', httpOp);
        return ctx.ok({ done: true });
      });

      const traceCtx = appModule.traceContext({});
      await traceCtx.span('app-handler', appOp);

      expect(rootBuffer).toBeDefined();
      if (!rootBuffer) throw new Error('rootBuffer is undefined');

      // Verify tree structure: app → http → db → cache
      expect(rootBuffer._children).toHaveLength(1);
      const httpChild = rootBuffer._children[0];
      expect(httpChild._children).toHaveLength(1);
      const dbChild = httpChild._children[0];
      expect(dbChild._children).toHaveLength(1);

      // Convert to Arrow table
      const table = convertSpanTreeToArrowTable(rootBuffer);

      // Verify all prefixed columns exist
      const fieldNames = table.schema.fields.map((f) => f.name);
      expect(fieldNames).toContain('http_status');
      expect(fieldNames).toContain('db_query');
      expect(fieldNames).toContain('cache_key');
    });
  });
});
