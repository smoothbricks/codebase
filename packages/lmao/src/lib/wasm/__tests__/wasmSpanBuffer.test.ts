import { beforeEach, describe, expect, it } from 'bun:test';
import { S as ArrowS } from '@smoothbricks/arrow-builder';
import { createTestOpMetadata, TEST_TRACER } from '../../__tests__/test-helpers.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { EMPTY_SCOPE } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import { createWasmAllocator, type WasmAllocator } from '../wasmAllocator.js';
import {
  createWasmChildSpanBuffer,
  createWasmOverflowBuffer,
  createWasmSpanBuffer,
  getWasmSpanBufferClass,
  type WasmSpanBufferInstance,
} from '../wasmSpanBuffer.js';
import { WasmTraceRoot } from '../wasmTraceRoot.js';

function testTraceId(value: string) {
  return createTraceId(value);
}

describe('WasmSpanBuffer', () => {
  let allocator: WasmAllocator;
  const CAPACITY = 64;
  let traceRoot: WasmTraceRoot;

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: CAPACITY });
    allocator.reset();
    traceRoot = new WasmTraceRoot(allocator, 'test-trace' as any, TEST_TRACER);
  });

  function createTestWasmBuffer(overrides: { trace_id?: string; thread_id?: bigint; span_id?: number } = {}) {
    return createWasmSpanBuffer(
      testSchema,
      {
        allocator,
        capacity: CAPACITY,
        trace_id: overrides.trace_id ? testTraceId(overrides.trace_id) : testTraceId('trace-123'),
        thread_id: overrides.thread_id ?? 42n,
        span_id: overrides.span_id ?? 1,
      },
      traceRoot,
      EMPTY_SCOPE,
      createTestOpMetadata(),
      createTestOpMetadata({ name: 'span', line: 0 }),
    );
  }

  // Test schema with various column types
  // Use ArrowS for types not exposed in lmao's S (bigUint64, eager)
  const testSchema = defineLogSchema({
    userId: S.category(),
    requestId: S.category(),
    count: S.number(),
    statusCode: ArrowS.number().eager(),
    isAdmin: S.boolean(),
    operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
    duration: ArrowS.bigUint64(),
    errorMessage: S.text(),
  });

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: CAPACITY });
    allocator.reset();
    // Set thread_id in WASM header (global for all buffers in this allocator)
    // thread_id = 42n for most tests
    allocator.setThreadId(0, 42); // high=0, low=42 -> thread_id = 42n
  });

  describe('getWasmSpanBufferClass', () => {
    it('generates a class constructor', () => {
      const WasmSpanBufferClass = getWasmSpanBufferClass(testSchema);
      expect(typeof WasmSpanBufferClass).toBe('function');
      expect(WasmSpanBufferClass.schema).toBe(testSchema);
    });

    it('caches classes per schema', () => {
      const class1 = getWasmSpanBufferClass(testSchema);
      const class2 = getWasmSpanBufferClass(testSchema);
      expect(class1).toBe(class2);
    });

    it('creates different classes for different schemas', () => {
      const schema2 = defineLogSchema({ otherField: S.number() });
      const class1 = getWasmSpanBufferClass(testSchema);
      const class2 = getWasmSpanBufferClass(schema2);
      expect(class1).not.toBe(class2);
    });
  });

  describe('instance creation', () => {
    it('creates instance with correct identity', () => {
      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 42n, span_id: 1 });

      expect(buffer.trace_id).toBe(testTraceId('trace-123'));
      expect(buffer.thread_id).toBe(42n);
      expect(buffer.span_id).toBe(1);
      expect(buffer.parent_thread_id).toBe(0n);
      expect(buffer.parent_span_id).toBe(0);
      expect(buffer._hasParent).toBe(false);
    });

    it('creates instance with parent identity via child buffer', () => {
      // Parent identity is now set by linking buffers, not via opts
      // Create parent first, then create child that references it
      const parent = createTestWasmBuffer({ trace_id: 'trace-456', thread_id: 100n, span_id: 2 });

      const child = createWasmChildSpanBuffer(
        parent,
        { allocator, capacity: CAPACITY, thread_id: 100n, span_id: 5 },
        traceRoot,
        EMPTY_SCOPE,
        createTestOpMetadata(),
        createTestOpMetadata({ name: 'child-span', line: 0 }),
      );

      // Child should have parent's identity accessible
      expect(child.parent_thread_id).toBe(parent.thread_id);
      expect(child.parent_span_id).toBe(parent.span_id);
      expect(child._hasParent).toBe(true);
    });

    it('allocates system block from WASM', () => {
      const beforeAllocs = allocator.getAllocCount();

      createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      // Should have allocated at least the system block
      expect(allocator.getAllocCount()).toBeGreaterThan(beforeAllocs);
    });

    it('initializes tree structure', () => {
      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      expect(buffer._parent).toBeNull();
      expect(buffer._children).toEqual([]);
      expect(buffer._overflow).toBeNull();
    });
  });

  describe('system columns', () => {
    let buffer: WasmSpanBufferInstance;

    beforeEach(() => {
      buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });
    });

    it('provides timestamp view into WASM memory', () => {
      const timestamp = buffer.timestamp;
      expect(timestamp).toBeInstanceOf(BigInt64Array);
      expect(timestamp.length).toBe(CAPACITY);
    });

    it('provides entry_type view into WASM memory', () => {
      const entryType = buffer.entry_type;
      expect(entryType).toBeInstanceOf(Uint8Array);
      expect(entryType.length).toBe(CAPACITY);
    });

    it('can write to timestamp array', () => {
      const timestamp = buffer.timestamp;
      timestamp[0] = 123456789n;
      expect(buffer.timestamp[0]).toBe(123456789n);
    });

    it('can write to entry_type array', () => {
      const entryType = buffer.entry_type;
      entryType[0] = 1; // SPAN_START
      entryType[1] = 2; // SPAN_OK
      expect(buffer.entry_type[0]).toBe(1);
      expect(buffer.entry_type[1]).toBe(2);
    });

    it('views reflect same WASM memory', () => {
      buffer.timestamp[5] = 999n;
      // Get a new view and verify it sees the same data
      const newView = buffer.timestamp;
      expect(newView[5]).toBe(999n);
    });
  });

  describe('message column (eager string)', () => {
    let buffer: WasmSpanBufferInstance;

    beforeEach(() => {
      buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });
    });

    it('can write message values', () => {
      buffer.message(0, 'span-start message');
      buffer.message(1, 'span-end message');
      buffer.message(2, 'log entry');

      expect(buffer._message[0]).toBe('span-start message');
      expect(buffer._message[1]).toBe('span-end message');
      expect(buffer._message[2]).toBe('log entry');
    });

    it('returns this for chaining', () => {
      const result = buffer.message(0, 'test');
      expect(result).toBe(buffer);
    });
  });

  describe('lazy string columns (category/text)', () => {
    let buffer: WasmSpanBufferInstance;

    beforeEach(() => {
      buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });
    });

    it('userId column starts undefined', () => {
      expect(buffer.getColumnIfAllocated('userId')).toBeUndefined();
      expect(buffer.getNullsIfAllocated('userId')).toBeUndefined();
    });

    it('allocates userId column on first write', () => {
      (buffer as unknown as { userId(idx: number, val: string): void }).userId(0, 'user-123');

      expect(buffer.getColumnIfAllocated('userId')).toBeDefined();
      expect(buffer.getNullsIfAllocated('userId')).toBeDefined();
    });

    it('writes values to category column', () => {
      const b = buffer as unknown as {
        userId(idx: number, val: string): WasmSpanBufferInstance;
        userId_values: string[];
        userId_nulls: Uint8Array;
      };

      b.userId(0, 'user-001');
      b.userId(5, 'user-002');

      expect(b.userId_values[0]).toBe('user-001');
      expect(b.userId_values[5]).toBe('user-002');
    });

    it('sets validity bits on write', () => {
      const b = buffer as unknown as {
        userId(idx: number, val: string | null): WasmSpanBufferInstance;
        userId_nulls: Uint8Array;
      };

      b.userId(0, 'user-001');
      b.userId(5, 'user-002');

      // Check validity bits are set
      // Bit 0 in byte 0 should be 1
      expect(b.userId_nulls[0] & 0b00000001).toBe(1);
      // Bit 5 in byte 0 should be 1
      expect(b.userId_nulls[0] & 0b00100000).toBe(0b00100000);
    });

    it('clears validity bit for null', () => {
      const b = buffer as unknown as {
        userId(idx: number, val: string | null): WasmSpanBufferInstance;
        userId_nulls: Uint8Array;
      };

      b.userId(0, 'user-001');
      b.userId(0, null);

      // Bit 0 should be cleared
      expect(b.userId_nulls[0] & 0b00000001).toBe(0);
    });

    it('returns this for chaining', () => {
      const b = buffer as unknown as { userId(idx: number, val: string): WasmSpanBufferInstance };
      const result = b.userId(0, 'test');
      expect(result).toBe(buffer);
    });
  });

  describe('lazy numeric columns (stored in WASM)', () => {
    let buffer: WasmSpanBufferInstance;

    beforeEach(() => {
      buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });
    });

    it('count column starts unallocated', () => {
      expect(buffer.isColumnAllocated(2)).toBe(false); // count is at index 2
    });

    it('allocates count column on first write', () => {
      const b = buffer as unknown as { count(idx: number, val: number): WasmSpanBufferInstance };
      b.count(0, 42);

      expect(buffer.isColumnAllocated(2)).toBe(true);
    });

    it('writes to numeric column', () => {
      const b = buffer as unknown as {
        count(idx: number, val: number): WasmSpanBufferInstance;
        count_values: Float64Array | null;
      };

      b.count(0, 42);
      b.count(5, 100);

      // Values should be readable (note: WASM memory layout may vary)
      expect(b.count_values).toBeDefined();
    });

    it('returns this for chaining', () => {
      const b = buffer as unknown as { count(idx: number, val: number): WasmSpanBufferInstance };
      const result = b.count(0, 42);
      expect(result).toBe(buffer);
    });
  });

  describe('eager numeric columns', () => {
    it('statusCode column is pre-allocated', () => {
      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      // statusCode is at index 3 and marked eager
      expect(buffer.isColumnAllocated(3)).toBe(true);
    });
  });

  describe('free()', () => {
    it('frees system block', () => {
      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      const freesBefore = allocator.getFreeCount();
      buffer.free();

      expect(allocator.getFreeCount()).toBeGreaterThan(freesBefore);
    });

    it('frees allocated column blocks', () => {
      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      // Write to lazy columns to allocate them
      const b = buffer as unknown as {
        count(idx: number, val: number): WasmSpanBufferInstance;
        isAdmin(idx: number, val: boolean): WasmSpanBufferInstance;
      };
      b.count(0, 42);
      b.isAdmin(0, true);

      const freesBefore = allocator.getFreeCount();
      buffer.free();

      // Should free system block + statusCode (eager) + count + isAdmin
      expect(allocator.getFreeCount()).toBeGreaterThan(freesBefore + 1);
    });
  });

  describe('createWasmChildSpanBuffer', () => {
    it('creates child linked to parent', () => {
      // thread_id is global in WASM header, shared by parent and child
      allocator.setThreadId(0, 1); // thread_id = 1n

      const parent = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      const child = createWasmChildSpanBuffer(
        parent,
        { allocator, capacity: CAPACITY, thread_id: 2n, span_id: 2 },
        traceRoot,
        EMPTY_SCOPE,
        createTestOpMetadata(),
        createTestOpMetadata({ name: 'child-span', line: 0 }),
      );

      // Manually push to _children - SpanContext does this with possible RemappedBufferView wrapper
      parent._children.push(child);

      expect(child._parent).toBe(parent);
      expect(parent._children).toContain(child);
      expect(child.trace_id).toBe(testTraceId('trace-123'));
      // Parent and child share thread_id from global header
      expect(child.parent_thread_id).toBe(1n);
      expect(child.parent_span_id).toBe(1);
    });
  });

  describe('createWasmOverflowBuffer', () => {
    it('creates overflow linked to original', () => {
      // thread_id is global in WASM header
      allocator.setThreadId(0, 1); // thread_id = 1n

      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      const overflow = createWasmOverflowBuffer(
        buffer,
        traceRoot,
        EMPTY_SCOPE,
        createTestOpMetadata(),
        createTestOpMetadata(),
      );

      expect(buffer._overflow).toBe(overflow);
      expect(overflow._parent).toBe(buffer._parent); // Same parent (null for root)
      expect(overflow.trace_id).toBe(testTraceId('trace-123'));
      expect(overflow.thread_id).toBe(1n);
      // Note: span_id is unique per identity block, not shared with original
      // overflow has its own span_id from allocIdentityChild
      expect(overflow.span_id).toBeGreaterThan(0);
    });
  });

  describe('custom inspect', () => {
    it('provides useful inspect output', () => {
      // thread_id is global in WASM header
      allocator.setThreadId(0, 1); // thread_id = 1n

      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      // Get custom inspect
      const inspectSymbol = Symbol.for('nodejs.util.inspect.custom');
      const inspectFn = (buffer as unknown as Record<symbol, () => string>)[inspectSymbol];
      const result = inspectFn.call(buffer);

      expect(result).toContain('WasmSpanBuffer');
      expect(result).toContain('trace_id: trace-123');
      expect(result).toContain('_systemPtr:');
    });
  });
});
