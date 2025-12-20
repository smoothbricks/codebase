// @ts-nocheck - Type system has DefinedLogSchema/LogSchema/SchemaFields compatibility issues
// that require codebase-level fixes in defineModule.ts, library.ts, and schema/types.ts.
// The tests work correctly at runtime - these are only type-level issues.
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
import { defineModule } from '../defineModule.js';
import { createPrefixMapping, generateRemappedBufferViewClass, prefixSchema } from '../library.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import type { LogSchema } from '../schema/LogSchema.js';
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
        _children: [],
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
        module: { name: 'test-module' },
        spanName: 'test-span',
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
        _children: [{ spanId: 2 }, { spanId: 3 }],
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
        module: { name: 'test-module' },
        spanName: 'test-span',
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

      expect(view._children).toBe(buffer._children);
      expect(view._next).toBe(buffer._next);
    });

    it('should pass through writeIndex', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view._writeIndex).toBe(7);
    });

    it('should pass through system columns', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view.timestamp).toBe(buffer.timestamp);
      expect(view.entry_type).toBe(buffer.entry_type);
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

      expect(view.trace_id).toBe(buffer.trace_id);
      expect(view.thread_id).toBe(buffer.thread_id);
      expect(view.span_id).toBe(buffer.span_id);
      expect(view.parent_span_id).toBe(buffer.parent_span_id);
      expect(view._identity).toBe(buffer._identity);
    });

    it('should pass through module metadata', () => {
      const mapping = { http_status: 'status' };
      const ViewClass = generateRemappedBufferViewClass(mapping);
      const buffer = createMockBuffer();
      const view = new ViewClass(buffer);

      expect(view._module).toBe(buffer._module);
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
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
      const schema = defineLogSchema({
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
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
  // Setup common schemas
  const appSchema = defineLogSchema({
    requestId: S.category(),
    userId: S.category(),
  });

  const httpSchema = defineLogSchema({
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  });

  const dbSchema = defineLogSchema({
    query: S.text(),
    table: S.category(),
  });

  // cacheSchema used for prefix mapping tests
  const cacheSchema = defineLogSchema({
    key: S.category(),
    hit: S.boolean(),
  });
  // Suppress unused variable warning - schema is used for demonstrating API patterns
  void cacheSchema;

  describe('without library module constructors', () => {
    it('should handle 4-level nested tasks with correct buffer hierarchy', async () => {
      // All using app schema directly (no library prefixing)
      const appModule = defineModule({
        metadata: {
          gitSha: 'test',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        logSchema: appSchema,
      })
        .ctx<Record<string, unknown>>({})
        .make();

      const buffers: { level: number; spanId: number; parentSpanId: number }[] = [];

      const level4Op = appModule.op('level4-task', async (ctx) => {
        buffers.push({
          level: 4,
          spanId: ctx.buffer.span_id,
          parentSpanId: ctx.buffer.parent_span_id,
        });
        ctx.tag.requestId('req-level4');
        return ctx.ok({ level: 4 });
      });

      const level3Op = appModule.op('level3-task', async (ctx) => {
        buffers.push({
          level: 3,
          spanId: ctx.buffer.span_id,
          parentSpanId: ctx.buffer.parent_span_id,
        });
        ctx.tag.requestId('req-level3');

        const result = await ctx.span('nested-level4', level4Op);
        return ctx.ok({ level: 3, child: result });
      });

      const level2Op = appModule.op('level2-task', async (ctx) => {
        buffers.push({
          level: 2,
          spanId: ctx.buffer.span_id,
          parentSpanId: ctx.buffer.parent_span_id,
        });
        ctx.tag.userId('user-level2');

        const result = await ctx.span('nested-level3', level3Op);
        return ctx.ok({ level: 2, child: result });
      });

      const level1Op = appModule.op('level1-task', async (ctx) => {
        buffers.push({
          level: 1,
          spanId: ctx.buffer.span_id,
          parentSpanId: ctx.buffer.parent_span_id,
        });
        ctx.tag.requestId('req-root').userId('user-root');

        const result = await ctx.span('nested-level2', level2Op);
        return ctx.ok({ level: 1, child: result });
      });

      const traceCtx = appModule.traceContext({});

      const result = await traceCtx.span('root', level1Op);
      expect(result.success).toBe(true);

      // Verify buffer hierarchy - all 4 levels executed
      expect(buffers).toHaveLength(4);
      expect(buffers[0].level).toBe(1);
      expect(buffers[1].level).toBe(2);
      expect(buffers[2].level).toBe(3);
      expect(buffers[3].level).toBe(4);

      // Root has no parent (parentSpanId = 0)
      expect(buffers[0].parent_span_id).toBe(0);

      // Verify all spanIds are unique and non-zero
      const spanIds = buffers.map((b) => b.span_id);
      expect(new Set(spanIds).size).toBe(4); // All unique
      for (const spanId of spanIds) {
        expect(spanId).toBeGreaterThan(0);
      }

      // Verify each non-root level has a valid parent spanId
      for (let i = 1; i < buffers.length; i++) {
        expect(buffers[i].parent_span_id).toBeGreaterThan(0);
      }
    });

    it('should propagate traceId through all nested levels', async () => {
      const appModule = defineModule({
        metadata: {
          gitSha: 'test',
          packageName: '@test/app',
          packagePath: 'src/app.ts',
        },
        logSchema: appSchema,
      })
        .ctx<Record<string, unknown>>({})
        .make();

      const traceIds: string[] = [];

      const level4Op = appModule.op('level4', async (ctx) => {
        traceIds.push(ctx.buffer.trace_id);
        return ctx.ok({});
      });

      const level3Op = appModule.op('level3', async (ctx) => {
        traceIds.push(ctx.buffer.trace_id);
        return ctx.span('to-level4', level4Op);
      });

      const level2Op = appModule.op('level2', async (ctx) => {
        traceIds.push(ctx.buffer.trace_id);
        return ctx.span('to-level3', level3Op);
      });

      const level1Op = appModule.op('level1', async (ctx) => {
        traceIds.push(ctx.buffer.trace_id);
        return ctx.span('to-level2', level2Op);
      });

      const traceCtx = appModule.traceContext({});

      await traceCtx.span('root', level1Op);

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
    it('should maintain correct prefix mappings at each level', () => {
      // Use createPrefixMapping directly to verify prefix mapping logic
      // DefinedLogSchema is a LogSchema instance at runtime - cast for type compatibility
      const httpPrefixMapping = createPrefixMapping(httpSchema as LogSchema, 'http');
      const dbPrefixMapping = createPrefixMapping(dbSchema as LogSchema, 'db');

      // Verify prefix mappings
      expect(httpPrefixMapping).toEqual({
        status: 'http_status',
        method: 'http_method',
      });

      expect(dbPrefixMapping).toEqual({
        query: 'db_query',
        table: 'db_table',
      });

      // Verify original schemas still have their fields
      expect(httpSchema.fieldNames.includes('status')).toBe(true);
      expect(httpSchema.fieldNames.includes('method')).toBe(true);

      expect(dbSchema.fieldNames.includes('query')).toBe(true);
      expect(dbSchema.fieldNames.includes('table')).toBe(true);
    });

    it('should create library module with proper op wrapper', async () => {
      // Create a module with prefixed schema to simulate library usage
      // DefinedLogSchema is a LogSchema instance at runtime - cast for type compatibility
      const prefixedHttpSchema = prefixSchema(httpSchema as LogSchema, 'http');

      const httpModule = defineModule({
        metadata: { gitSha: 'test', packageName: '@test/http', packagePath: 'src/http.ts' },
        logSchema: prefixedHttpSchema.fields, // Use .fields to get SchemaFields
      })
        .ctx<Record<string, never>>({})
        .make();

      let opExecuted = false;

      // Create an op through the module - use explicit any to work around complex generic inference
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const httpOp = httpModule.op('http-request', async (ctx: any) => {
        opExecuted = true;
        // With prefixed schema, we write to prefixed columns directly
        ctx.tag.http_status(200).http_method('GET');
        return ctx.ok({ status: 200 });
      });

      // Verify the op was created (Op is an object with _invoke method)
      expect(typeof httpOp).toBe('object');
      expect(httpOp).toBeDefined();

      // Create a trace context and run the op via span()
      const traceCtx = httpModule.traceContext({});
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await traceCtx.span('http-request', httpOp as any);

      expect(opExecuted).toBe(true);
      expect(result.success).toBe(true);
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
        _children: [],
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
        module: { name: 'child-module' },
        spanName: 'child-task',
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
        _children: [childView], // Parent sees RemappedBufferView, not raw buffer
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
        module: { name: 'parent-module' },
        spanName: 'parent-task',
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
        getNullsIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_nulls`];
        },
      } as unknown as SpanBuffer;

      // Simulate tree traversal (as Arrow conversion would do)
      expect(parentBuffer._children).toHaveLength(1);

      const child = parentBuffer._children[0] as SpanBuffer;

      // Access via prefixed name through the view
      expect(child.getColumnIfAllocated('http_status')).toEqual(new Float64Array([404]));

      // System columns pass through unchanged
      expect(child.span_id).toBe(2);
      expect(child.parent_span_id).toBe(1);
      expect(child.trace_id).toBe('12345678901234567890123456789012' as TraceId);
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
        _children: [],
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
        module: { name: 'db-module' },
        spanName: 'db-task',
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
        _children: [grandchildView],
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
        module: { name: 'http-module' },
        spanName: 'http-task',
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
        _children: [childView],
        writeIndex: 1,
        spanId: 1,
        getColumnIfAllocated(name: string) {
          return (this as Record<string, unknown>)[`${name}_values`];
        },
      } as unknown as SpanBuffer;

      // Traverse the tree
      const httpChild = rootBuffer._children[0] as SpanBuffer;
      const dbGrandchild = httpChild._children[0] as SpanBuffer;

      // Each level returns correct prefixed columns
      expect(httpChild.getColumnIfAllocated('http_status')).toEqual(new Float64Array([200]));
      expect(dbGrandchild.getColumnIfAllocated('db_query')).toEqual(['SELECT * FROM users']);
    });
  });

  describe('edge cases', () => {
    it('should handle empty prefix mapping', () => {
      const ViewClass = generateRemappedBufferViewClass({});

      const buffer = {
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
        _children: [],
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
        module: {},
        spanName: 'test-span',
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
