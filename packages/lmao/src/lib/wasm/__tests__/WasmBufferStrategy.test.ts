/**
 * Tests for WasmBufferStrategy
 */

import { afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { createOpMetadata } from '../../opContext/defineOp.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import type { TracerLifecycleHooks } from '../../traceRoot.js';
import { WasmBufferStrategy } from '../WasmBufferStrategy.js';
import { createWasmAllocator } from '../wasmAllocator.js';
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
  let mockTracer: TracerLifecycleHooks;

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

      expect(buffer).toBeDefined();
      expect(buffer._capacity).toBe(64);
      expect(buffer.message_values[0]).toBe('test-span');
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

      expect(child).toBeDefined();
      expect(child.message_values[0]).toBe('child-span');
      expect(child.trace_id).toBe(parent.trace_id);
      expect(child.parent_span_id).toBe(parent.span_id);
      // Note: parent._children is NOT populated by the strategy - SpanContext handles that.
      // The strategy only sets up the parent-child relationship via child._parent.
      expect(child._parent).toBe(parent);
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
      // Note: Currently overflow allocates its own identity block with new span_id
      // TODO: Overflow should share identity with parent buffer
      expect(overflow.trace_id).toBe(buffer.trace_id);
      expect(buffer._overflow).toBe(overflow);
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
      const traceRoot = createWasmTraceRoot(strategy.allocator, 'test-trace-id', mockTracer);

      // Create some buffers
      strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);
      strategy.createSpanBuffer(testSchema, traceRoot, testMetadata);

      const statsBeforeReset = strategy.getStats();
      expect(statsBeforeReset.allocCount).toBeGreaterThan(0);

      strategy.reset();

      const statsAfterReset = strategy.getStats();
      expect(statsAfterReset.allocCount).toBe(0);
      expect(statsAfterReset.freeCount).toBe(0);
    });
  });
});
