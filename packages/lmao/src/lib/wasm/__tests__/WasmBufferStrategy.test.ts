/**
 * Tests for WasmBufferStrategy
 */

import { afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { getColumnValue, getRawTimestamp } from '../../__tests__/arrow-test-helpers.js';
import { convertToLeasedArrowTable } from '../../convertToArrow.js';
import { createOpMetadata } from '../../opContext/defineOp.js';
import { resolveMessage } from '../../resolveMessage.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import type { TracerLifecycleHooks } from '../../traceRoot.js';
import { iterateSpanChildren, iterateSpanTree, NO_NODE } from '../../traceTopology.js';
import { WasmBufferStrategy } from '../WasmBufferStrategy.js';
import { createWasmAllocator } from '../wasmAllocator.js';
import type { WasmSpanBufferInstance } from '../wasmSpanBuffer.js';
import { createWasmTraceRoot } from '../wasmTraceRoot.js';

describe('WasmBufferStrategy', () => {
  // Simple schema for testing
  const testSchema = defineLogSchema({
    userId: S.category(),
    latency: S.number(),
    success: S.boolean(),
    operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  });

  // Test metadata for buffer creation
  const testMetadata = createOpMetadata('test-op', 'test-package', 'test.ts', 'abc123', 1);

  // Mock tracer lifecycle hooks for WasmTraceRoot
  let strategy: WasmBufferStrategy<typeof testSchema>;
  let mockTracer: TracerLifecycleHooks<typeof testSchema>;

  beforeAll(async () => {
    strategy = await WasmBufferStrategy.create<typeof testSchema>({
      capacity: 64,
      initialPages: 16, // 1MB
      maxPages: 16,
    });

    // Mock tracer lifecycle hooks for WasmTraceRoot
    // Must be created after strategy so we can reference it
    mockTracer = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      getFlagEvaluatorForContext: () => undefined,
      bufferStrategy: strategy,
    };
  });

  afterEach(() => {
    // Reset allocator between tests to ensure clean state
    strategy.reset();
  });

  describe('create()', () => {
    it('creates a strategy with default options', async () => {
      const s = await WasmBufferStrategy.create();
      expect(s).toBeDefined();
      expect(s.allocator).toBeDefined();
    });

    it('creates a strategy with custom options', async () => {
      const s = await WasmBufferStrategy.create({
        capacity: 128,
        initialPages: 32,
      });
      expect(s).toBeDefined();
      expect(s.allocator.capacity).toBe(128);
    });

    it('accepts a pre-created allocator', async () => {
      const allocator = await createWasmAllocator({ capacity: 32 });
      const s = await WasmBufferStrategy.create({ allocator });
      expect(s.allocator).toBe(allocator);
    });
  });

  describe('createSpanBuffer()', () => {
    it('creates a root span buffer', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const buffer = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);
      // Caller must set span name via message() - simulating writeSpanStart behavior
      buffer.message(0, 'test-span');
      expect('message_nulls' in buffer).toBe(false);

      expect(buffer).toBeDefined();
      expect(buffer._capacity).toBe(64);
      expect(resolveMessage(buffer, 0)).toBe('test-span');
      expect(String(buffer.trace_id)).toBe('test-trace-id');
    });

    it('creates buffer with custom capacity', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const buffer = strategy.createSpanBuffer(
        testSchema,
        traceRoot,
        testMetadata,
        32, // Custom capacity
      );

      expect(buffer._capacity).toBe(32);
    });

    it('allocates WASM memory for buffer', async () => {
      const statsBefore = strategy.getStats();

      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);
      strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      const statsAfter = strategy.getStats();
      expect(statsAfter.allocCount).toBeGreaterThan(statsBefore.allocCount);
    });
  });

  describe('createChildSpanBuffer()', () => {
    it('creates a child span linked to parent', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const parent = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      const child = strategy.createChildSpanBuffer(parent, testMetadata, testMetadata);
      // Caller must set span name via message() - simulating writeSpanStart behavior
      child.message(0, 'child-span');
      expect('message_nulls' in child).toBe(false);

      expect(child).toBeDefined();
      expect(resolveMessage(child, 0)).toBe('child-span');
      expect(child.trace_id).toBe(parent.trace_id);
      expect(child.parent_span_id).toBe(parent.span_id);
      expect(child._parent).toBe(parent);

      const children = iterateSpanChildren(parent);
      const registered = children.next();
      if (registered.done) throw new Error('topology did not register the child span');
      expect(registered.value).toBe(child);
      expect(children.next().done).toBe(true);
    });

    it('inherits capacity from parent by default', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const parent = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      const child = strategy.createChildSpanBuffer(parent, testMetadata, testMetadata);

      expect(child._capacity).toBe(parent._capacity);
    });

    it('allows custom capacity for child', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const parent = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      const child = strategy.createChildSpanBuffer(
        parent,
        testMetadata,
        testMetadata,
        32, // Custom capacity
      );

      expect(child._capacity).toBe(32);
    });
  });

  describe('createOverflowBuffer()', () => {
    it('creates overflow buffer linked to parent', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const buffer = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      const overflow = strategy.createOverflowBuffer(buffer);

      expect(overflow).toBeDefined();
      expect(overflow._capacity).toBe(buffer._capacity);
      expect(overflow.trace_id).toBe(buffer.trace_id);
      expect(overflow.span_id).toBe(buffer.span_id);
      expect(overflow.parent_span_id).toBe(buffer.parent_span_id);
      expect(overflow._nodeIndex).toBe(buffer._nodeIndex);
      expect(overflow._topologyGeneration).toBe(buffer._topologyGeneration);
      expect(traceRoot._topology.count).toBe(1);
      expect(buffer._overflow).toBe(overflow);
      expect(Array.from(iterateSpanTree(buffer))).toEqual([buffer, overflow]);
    });
  });

  describe('trace topology', () => {
    it('registers many siblings in insertion order while growing every topology lane', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'many-siblings-trace', mockTracer);
      const root = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);
      const siblingCount = 257;
      const legacyChildLane = ['_', 'children'].join('');
      expect(Reflect.has(root, legacyChildLane)).toBe(false);

      for (let index = 0; index < siblingCount; index++) {
        const child = strategy.createChildSpanBuffer(root, testMetadata, testMetadata);
        child.message(0, `child-${index}`);
        expect('message_nulls' in child).toBe(false);
        expect(child._nodeIndex).toBe(index + 1);
      }

      const topology = traceRoot._topology;
      expect(topology.count).toBe(siblingCount + 1);
      expect(topology.buffers.length).toBeGreaterThanOrEqual(siblingCount + 1);
      expect(topology.firstChild.length).toBe(topology.buffers.length);
      expect(topology.lastChild.length).toBe(topology.buffers.length);
      expect(topology.nextSibling.length).toBe(topology.buffers.length);
      expect(topology.firstChild[root._nodeIndex]).toBe(1);
      expect(topology.lastChild[root._nodeIndex]).toBe(siblingCount);
      expect(topology.nextSibling[siblingCount]).toBe(NO_NODE);

      let observed = 0;
      for (const child of iterateSpanChildren(root)) {
        expect(child._nodeIndex).toBe(observed + 1);
        expect(resolveMessage(child, 0)).toBe(`child-${observed}`);
        expect(Reflect.has(child, legacyChildLane)).toBe(false);
        observed++;
      }
      expect(observed).toBe(siblingCount);
    });
  });

  describe('releaseBuffer()', () => {
    it('frees WASM memory for a single buffer', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const buffer = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      const statsBefore = strategy.getStats();
      strategy.releaseBuffer(buffer);
      const statsAfter = strategy.getStats();

      expect(statsAfter.freeCount).toBeGreaterThan(statsBefore.freeCount);
    });

    it('frees entire span tree recursively', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const parent = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      strategy.createChildSpanBuffer(parent, testMetadata, testMetadata);

      strategy.createChildSpanBuffer(parent, testMetadata, testMetadata);

      const statsBefore = strategy.getStats();
      strategy.releaseBuffer(parent);
      const statsAfter = strategy.getStats();

      // Should have freed 3 buffers worth of memory
      expect(statsAfter.freeCount).toBeGreaterThan(statsBefore.freeCount);
    });

    it('frees overflow chain', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      const buffer = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      strategy.createOverflowBuffer(buffer);

      const statsBefore = strategy.getStats();
      strategy.releaseBuffer(buffer);
      const statsAfter = strategy.getStats();

      // Should have freed both buffers
      expect(statsAfter.freeCount).toBeGreaterThan(statsBefore.freeCount);
    });

    it('releases and resets chains deeper than recursive walkers can traverse', async () => {
      const deepSchema = defineLogSchema({});
      const deepStrategy = await WasmBufferStrategy.create<typeof deepSchema>({
        capacity: 8,
        initialPages: 16,
        maxPages: 256,
      });
      const deepTracer: TracerLifecycleHooks<typeof deepSchema> = {
        onTraceStart: () => {},
        onTraceEnd: () => {},
        onSpanStart: () => {},
        onSpanEnd: () => {},
        onStatsWillResetFor: () => {},
        getFlagEvaluatorForContext: () => undefined,
        bufferStrategy: deepStrategy,
      };
      const depth = 20_000;
      const buildChain = (traceId: string) => {
        const traceRoot = createWasmTraceRoot(deepStrategy.allocator, traceId, deepTracer);
        const root = deepStrategy.createSpanBuffer(deepSchema, traceRoot, testMetadata);
        let leaf = root;
        for (let level = 0; level < depth; level++) {
          leaf = deepStrategy.createChildSpanBuffer(leaf, testMetadata, testMetadata);
        }
        return { leaf, root, topology: traceRoot._topology };
      };

      const released = buildChain('deep-release-trace');
      const releaseGeneration = released.topology.generation;
      expect(released.topology.count).toBe(depth + 1);
      deepStrategy.releaseBuffer(released.root);
      expect(released.topology.count).toBe(0);
      expect(released.topology.root).toBe(NO_NODE);
      expect(released.topology.generation).toBe(releaseGeneration + 1);
      expect(() => released.topology.assertLive(released.leaf)).toThrow(/stale/);
      expect(() => deepStrategy.releaseBuffer(released.root)).toThrow(/stale/);

      const reset = buildChain('deep-reset-trace');
      const resetGeneration = reset.topology.generation;
      expect(reset.topology.count).toBe(depth + 1);
      deepStrategy.reset();
      expect(reset.topology.count).toBe(0);
      expect(reset.topology.root).toBe(NO_NODE);
      expect(reset.topology.generation).toBe(resetGeneration + 1);
      expect(() => reset.topology.assertLive(reset.leaf)).toThrow(/stale/);
      expect(() => iterateSpanChildren(reset.root).next()).toThrow(/stale/);
    });
  });

  describe('getStats()', () => {
    it('returns allocator statistics', async () => {
      const stats = strategy.getStats();

      expect(stats).toHaveProperty('allocCount');
      expect(stats).toHaveProperty('freeCount');
      expect(stats).toHaveProperty('bumpPtr');
      expect(stats).toHaveProperty('capacity');
      expect(typeof stats.allocCount).toBe('number');
      expect(typeof stats.freeCount).toBe('number');
      expect(typeof stats.bumpPtr).toBe('number');
      expect(typeof stats.capacity).toBe('number');
    });
  });

  describe('reset()', () => {
    it('resets allocator state', async () => {
      const firstTraceRoot = createWasmTraceRoot(strategy.allocator, 'first-test-trace-id', mockTracer);
      const secondTraceRoot = createWasmTraceRoot(strategy.allocator, 'second-test-trace-id', mockTracer);

      strategy.createSpanBuffer(testSchema, firstTraceRoot, testMetadata);
      strategy.createSpanBuffer(testSchema, secondTraceRoot, testMetadata);

      const statsBeforeReset = strategy.getStats();
      expect(statsBeforeReset.allocCount).toBeGreaterThan(0);

      strategy.reset();

      const statsAfterReset = strategy.getStats();
      expect(statsAfterReset.allocCount).toBe(0);
      expect(statsAfterReset.freeCount).toBe(0);
    });

    it('invalidates root, child, and overflow generations before offsets are reused', async () => {
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);
      const root = strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);
      const child = strategy.createChildSpanBuffer(root, testMetadata, testMetadata);
      const overflow = strategy.createOverflowBuffer(root);
      root.timestamp[0] = 11n;
      child.timestamp[0] = 22n;
      overflow.timestamp[0] = 33n;
      const topologyGeneration = traceRoot._topology.generation;

      strategy.reset();

      expect(() => root.timestamp).toThrow(/generation .* released/);
      expect(() => child.timestamp).toThrow(/generation .* released/);
      expect(() => overflow.timestamp).toThrow(/generation .* released/);
      expect(() => traceRoot._topology.assertLive(root)).toThrow(/stale/);
      expect(() => traceRoot._topology.assertLive(child)).toThrow(/stale/);
      expect(() => traceRoot._topology.assertLive(overflow)).toThrow(/stale/);
      expect(() => iterateSpanChildren(root).next()).toThrow(/stale/);
      expect(traceRoot._topology.generation).toBe(topologyGeneration + 1);
      expect(traceRoot._topology.count).toBe(0);
      expect(traceRoot._topology.root).toBe(NO_NODE);
      expect(strategy.getStats()).toEqual({ allocCount: 0, freeCount: 0, bumpPtr: 192, capacity: 64 });

      expect(() => strategy.releaseBuffer(root)).toThrow(/stale/);
      expect(strategy.getStats()).toEqual({ allocCount: 0, freeCount: 0, bumpPtr: 192, capacity: 64 });
    });
  });

  describe('leased Arrow conversion', () => {
    it('survives a WASM memory epoch change, releases safely, and reuses freed slabs', async () => {
      const leasedStrategy = await WasmBufferStrategy.create<typeof testSchema>({
        capacity: 8,
        initialPages: 1,
        maxPages: 18,
      });
      const leasedTracer: TracerLifecycleHooks<typeof testSchema> = {
        onTraceStart: () => {},
        onTraceEnd: () => {},
        onSpanStart: () => {},
        onSpanEnd: () => {},
        onStatsWillResetFor: () => {},
        getFlagEvaluatorForContext: () => undefined,
        bufferStrategy: leasedStrategy,
      };
      const traceRoot = createWasmTraceRoot(leasedStrategy.allocator, 'leased-wasm', leasedTracer);
      const root = leasedStrategy.createSpanBuffer(testSchema, traceRoot, testMetadata, 8) as WasmSpanBufferInstance<
        typeof testSchema
      >;
      root.timestamp[0] = 9_001n;
      {
        const entryTypes = root.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = 8;
      }
      root.message(0, 'leased-wasm-row');
      expect('message_nulls' in root).toBe(false);
      root.latency(0, 17.5);
      root.success(0, true);
      root.operation(0, 1);
      root._writeIndex = 1;

      const f64Pointer = root._familyPtrs.f64;
      const topologyGeneration = traceRoot._topology.generation;
      const memoryVersion = leasedStrategy.allocator.memoryVersion;
      const memoryBuffer = leasedStrategy.allocator.memory.buffer;
      const lease = convertToLeasedArrowTable(root);
      expect(lease.released).toBe(false);
      expect(getColumnValue(lease.table, 'latency', 0)).toBe(17.5);
      expect(getColumnValue(lease.table, 'success', 0)).toBe(true);
      expect(getColumnValue(lease.table, 'operation', 0)).toBe('READ');

      const bytesToGrow = memoryBuffer.byteLength - leasedStrategy.getStats().bumpPtr + 8;
      const growth = leasedStrategy.allocator.allocExact(bytesToGrow, 8);
      expect(growth).toBeGreaterThan(0);
      expect(leasedStrategy.allocator.memory.buffer).not.toBe(memoryBuffer);
      expect(leasedStrategy.allocator.memoryVersion).toBeGreaterThan(memoryVersion);
      expect(getRawTimestamp(lease.table, 0)).toBe(9_001n);
      expect(getColumnValue(lease.table, 'latency', 0)).toBe(17.5);
      expect(getColumnValue(lease.table, 'message', 0)).toBe('leased-wasm-row');
      expect(traceRoot._topology.generation).toBe(topologyGeneration);

      leasedStrategy.releaseBuffer(root);
      expect(traceRoot._topology.generation).toBe(topologyGeneration);
      expect(() => traceRoot._topology.assertLive(root)).toThrow(/stale/);
      expect(root.timestamp[0]).toBe(9_001n);
      expect(getColumnValue(lease.table, 'latency', 0)).toBe(17.5);
      lease.release();
      expect(lease.released).toBe(true);
      expect(traceRoot._topology.generation).toBe(topologyGeneration + 1);
      expect(() => root.timestamp).toThrow(/generation .* released/);
      expect(() => lease.release()).not.toThrow();

      const nextTraceRoot = createWasmTraceRoot(leasedStrategy.allocator, 'leased-wasm-next', leasedTracer);
      const next = leasedStrategy.createSpanBuffer(
        testSchema,
        nextTraceRoot,
        testMetadata,
        8,
      ) as WasmSpanBufferInstance<typeof testSchema>;
      expect(next._familyPtrs.f64).toBe(f64Pointer);
      leasedStrategy.releaseBuffer(next);
    });
  });
});
