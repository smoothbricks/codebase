/**
 * Integration tests for WASM-backed trace flow.
 *
 * These tests verify the full trace lifecycle works with WASM memory backing:
 * - Span start/end with timestamps
 * - Tag writes to WASM columns
 * - Nested spans with parent/child linkage
 * - Error handling (ctx.err() and exceptions)
 * - Log entries (info/debug/warn/error)
 * - Memory reuse via freelist
 */

import { afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { convertToArrowTable } from '../../convertToArrow.js';
import { defineOpContext } from '../../defineOpContext.js';
import { JsBufferStrategy } from '../../JsBufferStrategy.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_ERR,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_START,
  ENTRY_TYPE_WARN,
} from '../../schema/systemSchema.js';
import { createTraceRoot as createNodeTraceRoot } from '../../traceRoot.node.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { WasmBufferStrategy } from '../WasmBufferStrategy.js';
import { createWasmTraceRoot } from '../wasmTraceRoot.js';

/**
 * Type for accessing WASM-specific internal buffer properties.
 * WASM buffers use different property names than standard SpanBuffer.
 * We cast to this type to access WASM-specific properties in tests.
 */
interface WasmBufferInternals {
  // System columns (WASM uses _message array internally)
  _message: string[];
  // Schema columns (WASM uses name_values without underscore prefix)
  userId_values?: string[];
  latency_values?: Float64Array;
  operation_values?: Uint8Array;
  // System schema columns
  exception_stack_values?: string[];
  error_code_values?: string[];
}

/** Cast buffer to access WASM-specific internals */
function asWasm<T>(buffer: T): T & WasmBufferInternals {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test helper narrows to the generated WASM-only fields asserted below.
  return buffer as T & WasmBufferInternals;
}

describe('WASM Integration Tests', () => {
  // Test schema with various field types
  const schema = defineLogSchema({
    userId: S.category(),
    latency: S.number(),
    operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
    success: S.boolean(),
  });

  // Define op context - env is optional (undefined by default, not null-sentinel)
  const opContext = defineOpContext({
    logSchema: schema,
    ctx: { env: undefined as { region: string } | undefined },
  });

  let strategy: WasmBufferStrategy<typeof schema>;
  let tracer: TestTracer<typeof opContext>;

  beforeAll(async () => {
    strategy = await WasmBufferStrategy.create({
      capacity: 64,
      initialPages: 16,
      maxPages: 16,
    });

    tracer = new TestTracer(opContext, {
      bufferStrategy: strategy,
      createTraceRoot: (traceId, lifecycle) => createWasmTraceRoot(strategy.allocator, traceId, lifecycle),
    });
  });

  afterEach(() => {
    // Release all WASM memory for buffers
    for (const buffer of tracer.rootBuffers) {
      strategy.releaseBuffer(buffer);
    }
    // Clear tracer state
    tracer.clear();
    // Reset allocator for next test
    strategy.reset();

    // Create fresh tracer for next test
    tracer = new TestTracer(opContext, {
      bufferStrategy: strategy,
      createTraceRoot: (traceId, lifecycle) => createWasmTraceRoot(strategy.allocator, traceId, lifecycle),
    });
  });

  describe('Simple trace', () => {
    it('creates a simple trace with WASM backing', async () => {
      await tracer.trace('test-op', async (ctx) => {
        return ctx.ok('done');
      });

      expect(tracer.rootBuffers).toHaveLength(1);
      const buffer = tracer.rootBuffers[0];

      // Verify entry types
      expect(buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // Verify timestamps are valid
      expect(buffer.timestamp[0]).toBeGreaterThan(0n);
      expect(buffer.timestamp[1]).toBeGreaterThanOrEqual(buffer.timestamp[0]);

      // Verify span name is set
      expect(buffer.message_values[0]).toBe('test-op');
    });

    it('writes trace_id correctly', async () => {
      await tracer.trace('trace-id-test', async (ctx) => {
        return ctx.ok('done');
      });

      const buffer = tracer.rootBuffers[0];
      // trace_id should be a valid W3C format (32 hex chars)
      expect(buffer.trace_id).toMatch(/^[a-f0-9]{32}$/);
    });

    it('writes span_id correctly', async () => {
      await tracer.trace('span-id-test', async (ctx) => {
        return ctx.ok('done');
      });

      const buffer = tracer.rootBuffers[0];
      // span_id should be a positive number
      expect(buffer.span_id).toBeGreaterThan(0);
    });
  });

  describe('Tags', () => {
    it('writes tags to WASM columns', async () => {
      await tracer.trace('tag-test', async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.latency(42.5);
        ctx.tag.operation('READ');
        return ctx.ok('done');
      });

      const buffer = asWasm(tracer.rootBuffers[0]);

      // Verify tag values were written (row 0 is span-start)
      // Note: Tags write to row 0 by default for initial span attributes
      expect(buffer.userId_values?.[0]).toBe('user-123');
      expect(buffer.latency_values?.[0]).toBe(42.5);
      // Enum values are stored as uint8 indices
      expect(buffer.operation_values?.[0]).toBe(1); // 'READ' is index 1 in ['CREATE', 'READ', 'UPDATE', 'DELETE']
    });

    it('chains multiple tags fluently', async () => {
      await tracer.trace('chain-test', async (ctx) => {
        ctx.tag.userId('user-456').latency(100.25).operation('CREATE');
        return ctx.ok('done');
      });

      const buffer = asWasm(tracer.rootBuffers[0]);
      expect(buffer.userId_values?.[0]).toBe('user-456');
      expect(buffer.latency_values?.[0]).toBe(100.25);
      expect(buffer.operation_values?.[0]).toBe(0); // 'CREATE' is index 0
    });
  });

  describe('Nested spans', () => {
    it('creates nested spans with correct linkage', async () => {
      await tracer.trace('parent', async (ctx) => {
        await ctx.span('child', async (childCtx) => {
          return childCtx.ok('child done');
        });
        return ctx.ok('parent done');
      });

      const parent = tracer.rootBuffers[0];
      expect(parent._children).toHaveLength(1);

      const child = parent._children[0];
      // Child should share trace_id with parent
      expect(child.trace_id).toBe(parent.trace_id);
      // Child's parent_span_id should be parent's span_id
      expect(child.parent_span_id).toBe(parent.span_id);
      // Child should have its own span_id
      expect(child.span_id).not.toBe(parent.span_id);
    });

    it('creates deeply nested spans', async () => {
      await tracer.trace('level-0', async (ctx) => {
        await ctx.span('level-1', async (ctx1) => {
          await ctx1.span('level-2', async (ctx2) => {
            return ctx2.ok('deepest');
          });
          return ctx1.ok('middle');
        });
        return ctx.ok('root');
      });

      const root = tracer.rootBuffers[0];
      expect(root._children).toHaveLength(1);

      const level1 = root._children[0];
      expect(level1._children).toHaveLength(1);
      expect(level1.parent_span_id).toBe(root.span_id);

      const level2 = level1._children[0];
      expect(level2.parent_span_id).toBe(level1.span_id);
      expect(level2.trace_id).toBe(root.trace_id);
    });

    it('creates sibling spans correctly', async () => {
      await tracer.trace('parent', async (ctx) => {
        await ctx.span('child-1', async (c1) => c1.ok('first'));
        await ctx.span('child-2', async (c2) => c2.ok('second'));
        return ctx.ok('done');
      });

      const parent = tracer.rootBuffers[0];
      expect(parent._children).toHaveLength(2);

      const [child1, child2] = parent._children;
      expect(child1.message_values[0]).toBe('child-1');
      expect(child2.message_values[0]).toBe('child-2');
      expect(child1.parent_span_id).toBe(parent.span_id);
      expect(child2.parent_span_id).toBe(parent.span_id);
      // Siblings should have different span IDs
      expect(child1.span_id).not.toBe(child2.span_id);
    });
  });

  describe('Error handling', () => {
    it('handles errors correctly with span-exception', async () => {
      try {
        await tracer.trace('error-op', async (_ctx) => {
          throw new Error('test error');
        });
      } catch (_e) {
        // Expected
      }

      const buffer = asWasm(tracer.rootBuffers[0]);
      expect(buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_EXCEPTION);

      // Verify error message was written - use message_values getter
      const messageValues = buffer.message_values;
      const messages = messageValues.slice(0, buffer._writeIndex).filter((m: string) => m !== undefined);
      expect(messages).toContain('test error');
    });

    it('captures exception stack trace', async () => {
      try {
        await tracer.trace('stack-trace-op', async (_ctx) => {
          throw new Error('stack test');
        });
      } catch (_e) {
        // Expected
      }

      const buffer = asWasm(tracer.rootBuffers[0]);
      // Stack trace should be written
      expect(buffer.exception_stack_values?.[1]).toContain('stack test');
      expect(buffer.exception_stack_values?.[1]).toContain('at'); // Stack trace has 'at' for call frames
    });
  });

  describe('ctx.err()', () => {
    it('writes span-err for ctx.err()', async () => {
      await tracer.trace('err-op', async (ctx) => {
        return ctx.err('something went wrong');
      });

      const buffer = tracer.rootBuffers[0];
      expect(buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_ERR);
    });

    it('writes error code for typed errors', async () => {
      await tracer.trace('typed-err-op', async (ctx) => {
        return ctx.err({ code: 'VALIDATION_ERROR', message: 'Invalid input' });
      });

      const buffer = asWasm(tracer.rootBuffers[0]);
      expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_ERR);
      // Error code should be written to error_code column
      expect(buffer.error_code_values?.[1]).toBe('VALIDATION_ERROR');
    });
  });

  describe('Log entries', () => {
    it('writes log entries with correct entry types', async () => {
      await tracer.trace('log-op', async (ctx) => {
        ctx.log.info('info message');
        ctx.log.debug('debug message');
        ctx.log.warn('warn message');
        return ctx.ok('done');
      });

      const buffer = asWasm(tracer.rootBuffers[0]);
      // Should have: span-start (0), span-ok (1), log entries (2+)
      // _writeIndex tells us how many rows were written
      expect(buffer._writeIndex).toBeGreaterThan(2);

      // Row 0 = span-start, Row 1 = span-ok (written at end)
      expect(buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);

      // Log entries are written at indices 2+
      const entryTypes = Array.from(buffer.entry_type.slice(0, buffer._writeIndex));

      // Use entry type constants from systemSchema
      expect(entryTypes).toContain(ENTRY_TYPE_INFO);
      expect(entryTypes).toContain(ENTRY_TYPE_DEBUG);
      expect(entryTypes).toContain(ENTRY_TYPE_WARN);

      // Messages are written via message_values getter (SpanLogger uses this)
      // Access via the getter since that's what SpanLogger writes to
      const messageValues = buffer.message_values;
      const messages = messageValues.slice(0, buffer._writeIndex).filter((m: string) => m !== undefined);
      expect(messages).toContain('info message');
      expect(messages).toContain('debug message');
      expect(messages).toContain('warn message');
    });

    it('writes error log entries', async () => {
      await tracer.trace('error-log-op', async (ctx) => {
        ctx.log.error('error message');
        return ctx.ok('done');
      });

      const buffer = asWasm(tracer.rootBuffers[0]);
      // Use entry type constant from systemSchema
      const entryTypes = Array.from(buffer.entry_type.slice(0, buffer._writeIndex));
      expect(entryTypes).toContain(ENTRY_TYPE_ERROR);
    });
  });

  describe('Identity ownership', () => {
    it('keeps the root trace owned by the root when a child is released and recycled', async () => {
      await tracer.trace('owned-root', async (ctx) => {
        await ctx.span('released-child', async (child) => child.ok('child done'));
        return ctx.ok('root done');
      });

      const root = tracer.rootBuffers[0];
      const child = root._children[0];
      const rootTraceId = root.trace_id;
      const rootSpanId = root.span_id;
      expect(child.trace_id).toBe(rootTraceId);
      expect(child.span_id).not.toBe(rootSpanId);
      expect(child.parent_span_id).toBe(rootSpanId);
      expect(child._identity).not.toEqual(root._identity);

      strategy.releaseBuffer(child);
      await tracer.trace('recycled-child-identity', async (ctx) => ctx.ok('done'));

      expect(root.trace_id).toBe(rootTraceId);
      expect(root.span_id).toBe(rootSpanId);
      expect(root.message_values[0]).toBe('owned-root');
      expect(tracer.rootBuffers[1].trace_id).not.toBe(rootTraceId);
    });

    it('preserves logical span identity and linkage across overflow segments', async () => {
      await tracer.trace('overflow-root', async (ctx) => {
        for (let i = 0; i < 64; i++) {
          ctx.log.info(`root-log-${i}`);
        }
        await ctx.span('overflow-child', async (child) => {
          for (let i = 0; i < 64; i++) {
            child.log.info(`child-log-${i}`);
          }
          return child.ok('child done');
        });
        return ctx.ok('root done');
      });

      const root = tracer.rootBuffers[0];
      const rootOverflow = root._overflow;
      if (!rootOverflow) throw new Error('root logs did not create an overflow segment');
      expect(rootOverflow.trace_id).toBe(root.trace_id);
      expect(rootOverflow.span_id).toBe(root.span_id);
      expect(rootOverflow.parent_span_id).toBe(root.parent_span_id);
      expect(rootOverflow._identity).toEqual(root._identity);

      const child = root._children[0];
      const childOverflow = child._overflow;
      if (!childOverflow) throw new Error('child logs did not create an overflow segment');
      expect(child.trace_id).toBe(root.trace_id);
      expect(child.span_id).not.toBe(root.span_id);
      expect(child.parent_span_id).toBe(root.span_id);
      expect(childOverflow.trace_id).toBe(child.trace_id);
      expect(childOverflow.span_id).toBe(child.span_id);
      expect(childOverflow.parent_span_id).toBe(child.parent_span_id);
      expect(childOverflow._identity).toEqual(child._identity);
    });
  });

  describe('Released memory ownership', () => {
    it('keeps two new traces independent after repeated tree release', async () => {
      await tracer.trace('released-tree', async (ctx) => {
        await ctx.span('released-child', async (child) => {
          child.tag.latency(1);
          return child.ok('done');
        });
        return ctx.ok('done');
      });

      const released = tracer.rootBuffers[0];
      strategy.releaseBuffer(released);
      strategy.releaseBuffer(released);

      await tracer.trace('first-new-owner', async (ctx) => {
        ctx.tag.latency(11.5);
        return ctx.ok('done');
      });
      await tracer.trace('second-new-owner', async (ctx) => {
        ctx.tag.latency(22.5);
        return ctx.ok('done');
      });

      const firstTable = strategy.toArrowTable(tracer.rootBuffers[1]);
      const secondTable = strategy.toArrowTable(tracer.rootBuffers[2]);
      expect(firstTable.getChild('message')?.get(0)).toBe('first-new-owner');
      expect(firstTable.getChild('latency')?.get(0)).toBe(11.5);
      expect(secondTable.getChild('message')?.get(0)).toBe('second-new-owner');
      expect(secondTable.getChild('latency')?.get(0)).toBe(22.5);
    });

    it('keeps single-buffer Arrow output isolated from recycled WASM memory', async () => {
      await tracer.trace('arrow-first', async (ctx) => {
        ctx.tag.latency(91.25).success(true).operation('READ');
        return ctx.ok('done');
      });

      const firstBuffer = tracer.rootBuffers[0];
      const firstTable = convertToArrowTable(firstBuffer);
      expect(firstTable.numRows).toBe(2);
      expect(firstTable.getChild('message')?.get(0)).toBe('arrow-first');
      expect(firstTable.getChild('latency')?.get(0)).toBe(91.25);
      expect(firstTable.getChild('success')?.get(0)).toBe(true);
      expect(firstTable.getChild('operation')?.get(0)).toBe('READ');

      strategy.releaseBuffer(firstBuffer);
      await tracer.trace('arrow-second', async (ctx) => {
        ctx.tag.latency(-17.5).success(false).operation('DELETE');
        return ctx.ok('done');
      });

      const secondTable = convertToArrowTable(tracer.rootBuffers[1]);
      expect(secondTable.getChild('latency')?.get(0)).toBe(-17.5);
      expect(secondTable.getChild('success')?.get(0)).toBe(false);
      expect(secondTable.getChild('operation')?.get(0)).toBe('DELETE');
      expect(firstTable.getChild('message')?.get(0)).toBe('arrow-first');
      expect(firstTable.getChild('latency')?.get(0)).toBe(91.25);
      expect(firstTable.getChild('success')?.get(0)).toBe(true);
      expect(firstTable.getChild('operation')?.get(0)).toBe('READ');
    });

    it('keeps span-tree Arrow output isolated from recycled WASM memory', async () => {
      await tracer.trace('tree-first', async (ctx) => {
        ctx.tag.latency(1.25);
        await ctx.span('child-first', async (child) => {
          child.tag.latency(2.5);
          return child.ok('done');
        });
        return ctx.ok('done');
      });

      const firstBuffer = tracer.rootBuffers[0];
      const firstTable = strategy.toArrowTable(firstBuffer);
      expect(firstTable.numRows).toBe(4);
      expect(firstTable.getChild('message')?.get(0)).toBe('tree-first');
      expect(firstTable.getChild('message')?.get(2)).toBe('child-first');
      expect(firstTable.getChild('latency')?.get(0)).toBe(1.25);
      expect(firstTable.getChild('latency')?.get(2)).toBe(2.5);

      strategy.releaseBuffer(firstBuffer);
      await tracer.trace('tree-second', async (ctx) => {
        ctx.tag.latency(101.25);
        await ctx.span('child-second', async (child) => {
          child.tag.latency(202.5);
          return child.ok('done');
        });
        return ctx.ok('done');
      });

      const secondTable = strategy.toArrowTable(tracer.rootBuffers[1]);
      expect(secondTable.getChild('latency')?.get(0)).toBe(101.25);
      expect(secondTable.getChild('latency')?.get(2)).toBe(202.5);
      expect(firstTable.getChild('message')?.get(0)).toBe('tree-first');
      expect(firstTable.getChild('message')?.get(2)).toBe('child-first');
      expect(firstTable.getChild('latency')?.get(0)).toBe(1.25);
      expect(firstTable.getChild('latency')?.get(2)).toBe(2.5);
    });
  });

  describe('Memory reuse', () => {
    it('tracks allocation stats', async () => {
      const statsBefore = strategy.getStats();

      await tracer.trace('alloc-test', async (ctx) => ctx.ok('done'));

      const statsAfter = strategy.getStats();
      // Should have allocated memory for the buffer
      expect(statsAfter.allocCount).toBeGreaterThan(statsBefore.allocCount);
    });

    it('reuses WASM memory across traces', async () => {
      // Run multiple traces
      for (let i = 0; i < 5; i++) {
        await tracer.trace(`trace-${i}`, async (ctx) => ctx.ok('done'));
      }

      const statsBeforeClear = strategy.getStats();
      const allocCountBefore = statsBeforeClear.allocCount;

      // Release all buffers
      for (const buffer of tracer.rootBuffers) {
        strategy.releaseBuffer(buffer);
      }

      const statsAfterFree = strategy.getStats();
      // Free count should have increased after releasing buffers
      expect(statsAfterFree.freeCount).toBeGreaterThan(0);

      // Clear tracer (this will reset the allocator)
      tracer.clear();

      // Run more traces - should reuse freed memory
      for (let i = 0; i < 5; i++) {
        await tracer.trace(`trace-reuse-${i}`, async (ctx) => ctx.ok('done'));
      }

      const statsAfterReuse = strategy.getStats();
      // Alloc count should not have more than doubled (some memory reused from freelist)
      // Note: Exact reuse depends on freelist implementation and allocation patterns
      expect(statsAfterReuse.allocCount).toBeLessThanOrEqual(allocCountBefore * 2);
    });

    it('releases entire span tree', async () => {
      // Create a trace with nested spans
      await tracer.trace('parent', async (ctx) => {
        await ctx.span('child-1', async (c1) => {
          await c1.span('grandchild', async (gc) => gc.ok('deep'));
          return c1.ok('c1');
        });
        await ctx.span('child-2', async (c2) => c2.ok('c2'));
        return ctx.ok('parent');
      });

      const freeCountBefore = strategy.getStats().freeCount;

      // Release the root buffer (should release entire tree)
      strategy.releaseBuffer(tracer.rootBuffers[0]);

      const freeCountAfter = strategy.getStats().freeCount;
      // Should have freed memory for all 4 spans (parent + 2 children + 1 grandchild)
      expect(freeCountAfter).toBeGreaterThan(freeCountBefore);
    });
  });

  describe('Sync vs Async traces', () => {
    it('handles sync functions correctly', async () => {
      // Note: trace() always returns a Promise, but the function can be sync
      const result = await tracer.trace('sync-op', (ctx) => {
        ctx.tag.userId('sync-user');
        return ctx.ok({ sync: true });
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.sync).toBe(true);
      }

      const buffer = tracer.rootBuffers[0];
      expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    });

    it('handles async functions correctly', async () => {
      const result = await tracer.trace('async-op', async (ctx) => {
        await new Promise((resolve) => setTimeout(resolve, 1));
        ctx.tag.userId('async-user');
        return ctx.ok({ async: true });
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.async).toBe(true);
      }

      const buffer = tracer.rootBuffers[0];
      expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
      // Timestamps should reflect the delay
      expect(buffer.timestamp[1]).toBeGreaterThan(buffer.timestamp[0]);
    });
  });

  describe('JS/WASM timestamp output parity', () => {
    it('emits identical timestamped lifecycle rows across overflow and child spans', async () => {
      const jsSchema = defineLogSchema({
        userId: S.category(),
        latency: S.number(),
        operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
        success: S.boolean(),
      });
      const jsOpContext = defineOpContext({
        logSchema: jsSchema,
        ctx: { env: undefined as { region: string } | undefined },
      });
      const jsStrategy = new JsBufferStrategy<typeof jsSchema>();
      const jsTracer = new TestTracer(jsOpContext, {
        bufferStrategy: jsStrategy,
        createTraceRoot: createNodeTraceRoot,
      });
      const originalDateNow = Date.now;
      const originalHrtime = process.hrtime.bigint;
      const originalPerformanceNow = performance.now;
      const epochMillis = 1_700_000_000_000;
      const nodeAnchor = 5_000_000n;
      const performanceAnchor = 1_000;

      try {
        Date.now = () => epochMillis;
        let nodeTick = 0n;
        process.hrtime.bigint = () => nodeAnchor + nodeTick++ * 125_000n;
        await jsTracer.trace('parity-root', async (root) => {
          for (let i = 0; i < 70; i++) root.log.info(`root-${i}`);
          await root.span('parity-child', async (child) => {
            for (let i = 0; i < 70; i++) child.log.info(`child-${i}`);
            return child.ok('child done');
          });
          return root.ok('root done');
        });

        let performanceTick = 0;
        Object.defineProperty(performance, 'now', {
          configurable: true,
          value: () => performanceAnchor + performanceTick++ * 0.125,
        });
        await tracer.trace('parity-root', async (root) => {
          for (let i = 0; i < 70; i++) root.log.info(`root-${i}`);
          await root.span('parity-child', async (child) => {
            for (let i = 0; i < 70; i++) child.log.info(`child-${i}`);
            return child.ok('child done');
          });
          return root.ok('root done');
        });

        const jsTable = jsStrategy.toArrowTable(jsTracer.rootBuffers[0]);
        const wasmTable = strategy.toArrowTable(tracer.rootBuffers[0]);
        expect(wasmTable.numRows).toBe(jsTable.numRows);
        for (const columnName of ['entry_type', 'message', 'timestamp'] as const) {
          const jsColumn = jsTable.getChild(columnName);
          const wasmColumn = wasmTable.getChild(columnName);
          if (!jsColumn || !wasmColumn) throw new Error(`missing parity column: ${columnName}`);
          const jsValues = Array.from({ length: jsTable.numRows }, (_, row) => jsColumn.get(row));
          const wasmValues = Array.from({ length: wasmTable.numRows }, (_, row) => wasmColumn.get(row));
          expect(wasmValues).toEqual(jsValues);
        }
      } finally {
        Date.now = originalDateNow;
        process.hrtime.bigint = originalHrtime;
        Object.defineProperty(performance, 'now', { configurable: true, value: originalPerformanceNow });
      }
    });
  });

  describe('Multiple traces', () => {
    it('maintains separate buffers for each trace', async () => {
      await tracer.trace('trace-1', async (ctx) => {
        ctx.tag.userId('user-1');
        return ctx.ok('one');
      });

      await tracer.trace('trace-2', async (ctx) => {
        ctx.tag.userId('user-2');
        return ctx.ok('two');
      });

      expect(tracer.rootBuffers).toHaveLength(2);

      const buffer1 = asWasm(tracer.rootBuffers[0]);
      const buffer2 = asWasm(tracer.rootBuffers[1]);

      // Different trace IDs
      expect(buffer1.trace_id).not.toBe(buffer2.trace_id);

      // Different span names
      expect(buffer1.message_values[0]).toBe('trace-1');
      expect(buffer2.message_values[0]).toBe('trace-2');

      // Different user IDs
      expect(buffer1.userId_values?.[0]).toBe('user-1');
      expect(buffer2.userId_values?.[0]).toBe('user-2');
    });
  });
});
