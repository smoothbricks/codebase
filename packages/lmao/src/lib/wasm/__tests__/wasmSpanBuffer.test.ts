import { beforeEach, describe, expect, it } from 'bun:test';
import { S as ArrowS, Nanoseconds } from '@smoothbricks/arrow-builder';
import fc from 'fast-check';
import { defineOpContext } from '../../defineOpContext.js';
import { resolveEagerColumns, type EagerColumnDescriptor } from '../../physicalLayoutPlan.js';
import { createTestOpMetadata, createTestTraceRoot, TEST_TRACER } from '../../__tests__/test-helpers.js';
import { resolveMessage } from '../../resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_TAG,
} from '../../runtimeHint.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { createSpanBuffer, EMPTY_SCOPE } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import { createWasmAllocator, type WasmAllocator } from '../wasmAllocator.js';
import { getWasmPhysicalLayout } from '../wasmPhysicalLayout.js';
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

function getMethod<TArgs extends unknown[], TResult>(target: object, name: string): (...args: TArgs) => TResult {
  const value = Reflect.get(target, name);
  if (typeof value !== 'function') {
    throw new Error(`expected method '${name}' to exist on test buffer`);
  }
  return (...args: TArgs) => value.call(target, ...args);
}

function getColumn<T>(target: object, name: string): T {
  return Reflect.get(target, name);
}

function eagerDescriptorBytes(descriptor: EagerColumnDescriptor): Uint8Array {
  const bytes = new Uint8Array(descriptor.words.length * Uint32Array.BYTES_PER_ELEMENT);
  const view = new DataView(bytes.buffer);
  for (let index = 0; index < descriptor.words.length; index++) {
    view.setUint32(index * Uint32Array.BYTES_PER_ELEMENT, descriptor.words[index], true);
  }
  return bytes;
}

describe('WasmSpanBuffer', () => {
  let allocator: WasmAllocator;
  const CAPACITY = 64;
  let traceRoot: WasmTraceRoot;

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: CAPACITY });
    allocator.reset();
    allocator.setThreadId(0, 42); // high=0, low=42 -> thread_id = 42n
    traceRoot = new WasmTraceRoot(allocator, testTraceId('test-trace'), TEST_TRACER);
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
  const testEagerColumns = resolveEagerColumns(testSchema);

  const compilerEagerSchema = defineLogSchema({
    provenNumber: S.number(),
    lazyNumber: S.number(),
    provenString: S.category(),
    lazyString: S.category(),
  });
  const compilerEagerContext = defineOpContext({ logSchema: compilerEagerSchema });
  const compilerEagerOp = compilerEagerContext.defineOp(
    'compiler-eager-wasm',
    (ctx) => ctx.ok(null),
    undefined,
    {
      runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_TAG | RUNTIME_HINT_RESULT | 8,
      eagerColumns: ['provenString', 'provenNumber', 'provenString'],
    },
  );

  function createCompilerEagerWasmBuffer() {
    return createWasmSpanBuffer(
      compilerEagerSchema,
      {
        allocator,
        capacity: CAPACITY,
        trace_id: testTraceId('compiler-eager-trace'),
        thread_id: 42n,
        span_id: 1,
      },
      traceRoot,
      EMPTY_SCOPE,
      compilerEagerOp.metadata,
      compilerEagerOp.metadata,
    );
  }

  describe('getWasmSpanBufferClass', () => {
    it('generates a class constructor', () => {
      const WasmSpanBufferClass = getWasmSpanBufferClass(testSchema, 'mixed', testEagerColumns);
      expect(typeof WasmSpanBufferClass).toBe('function');
      expect(WasmSpanBufferClass.schema).toBe(testSchema);
    });

    it('caches classes per schema', () => {
      const class1 = getWasmSpanBufferClass(testSchema, 'mixed', testEagerColumns);
      const class2 = getWasmSpanBufferClass(testSchema, 'mixed', testEagerColumns);
      expect(class1).toBe(class2);
    });

    it('creates different classes for different schemas', () => {
      const schema2 = defineLogSchema({ otherField: S.number() });
      const class1 = getWasmSpanBufferClass(testSchema, 'mixed', testEagerColumns);
      const class2 = getWasmSpanBufferClass(schema2, 'mixed', resolveEagerColumns(schema2));
      expect(class1).not.toBe(class2);
    });
  });

  describe('exact physical layout', () => {
    it('produces aligned, exact, non-overlapping family descriptors for arbitrary capacities', () => {
      fc.assert(
        fc.property(fc.integer({ min: 1, max: 512 }), (capacity) => {
          const layout = getWasmPhysicalLayout(testSchema, capacity, 'mixed', testEagerColumns);
          expect(layout).toBe(getWasmPhysicalLayout(testSchema, capacity, 'mixed', testEagerColumns));
          expect(Object.isFrozen(layout)).toBe(true);
          expect(layout.system.timestampOffset).toBe(0);
          expect(layout.system.entryTypeOffset).toBe(capacity * 8);
          const logHeaderOffset = (capacity * 9 + 3) & ~3;
          expect(layout.messageLayoutFamily).toBe('mixed');
          expect(layout.system.logHeaderOffset).toBe(logHeaderOffset);
          expect(layout.system.byteLength).toBe(logHeaderOffset + capacity * Uint32Array.BYTES_PER_ELEMENT);

          for (const family of ['u8', 'u32', 'f64'] as const) {
            const slab = layout.slabs[family];
            const columns = layout.columns.filter((column) => column.family === family);
            expect(slab === null).toBe(columns.length === 0);
            if (slab === null) continue;

            const ranges: Array<readonly [number, number]> = [];
            for (const column of columns) {
              expect(column.valueOffset % column.alignment).toBe(0);
              expect(column.nullByteLength).toBe(Math.ceil(capacity / 8));
              expect(column.valueLength).toBe(capacity * column.byteWidth);
              expect(column.nullOffset + column.nullByteLength).toBeLessThanOrEqual(column.valueOffset);
              expect(column.valueOffset + column.valueLength).toBeLessThanOrEqual(slab.byteLength);

              const columnRanges = [
                [column.nullOffset, column.nullOffset + column.nullByteLength],
                [column.valueOffset, column.valueOffset + column.valueLength],
              ] as const;
              for (const [start, end] of columnRanges) {
                for (const [otherStart, otherEnd] of ranges) {
                  expect(end <= otherStart || start >= otherEnd).toBe(true);
                }
                ranges.push([start, end]);
              }
            }
          }
        }),
      );
    });

    it('allocates one exact slab per numeric family and no blocks on numeric writes', () => {
      const allocsBefore = allocator.getAllocCount();
      const buffer = createTestWasmBuffer();
      const allocatedSlabs = Object.values(buffer._layout.slabs).filter((slab) => slab !== null).length;
      expect(allocator.getAllocCount() - allocsBefore).toBe(2 + allocatedSlabs); // identity + system + families

      const ranges: Array<readonly [number, number]> = [
        [buffer._systemPtr, buffer._systemPtr + buffer._layout.system.byteLength],
      ];
      for (const family of ['u8', 'u32', 'f64'] as const) {
        const slab = buffer._layout.slabs[family];
        if (slab === null) continue;
        const start = buffer._familyPtrs[family];
        const end = start + slab.byteLength;
        expect(start % slab.alignment).toBe(0);
        for (const [otherStart, otherEnd] of ranges) {
          expect(end <= otherStart || start >= otherEnd).toBe(true);
        }
        ranges.push([start, end]);
      }

      const count = getMethod<[idx: number, val: number], WasmSpanBufferInstance>(buffer, 'count');
      const isAdmin = getMethod<[idx: number, val: boolean], WasmSpanBufferInstance>(buffer, 'isAdmin');
      count(3, 42);
      isAdmin(4, true);
      expect(allocator.getAllocCount() - allocsBefore).toBe(2 + allocatedSlabs);
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

    it('reuses canonical typed views while memory is stable', () => {
      const timestamp = buffer.timestamp;
      const entryType = buffer.entry_type;
      const identity = buffer._identity;
      const countValues = getColumn<Float64Array>(buffer, 'count_values');
      const countNulls = getColumn<Uint8Array>(buffer, 'count_nulls');
      const count = getMethod<[idx: number, val: number], WasmSpanBufferInstance>(buffer, 'count');

      count(5, 999);

      expect(buffer.timestamp).toBe(timestamp);
      expect(buffer.entry_type).toBe(entryType);
      expect(buffer._identity).toBe(identity);
      expect(getColumn<Float64Array>(buffer, 'count_values')).toBe(countValues);
      expect(getColumn<Uint8Array>(buffer, 'count_nulls')).toBe(countNulls);
      expect(countValues[5]).toBe(999);
    });

    it('rebuilds every canonical view once after memory growth and preserves data', () => {
      buffer.timestamp[3] = 123n;
      const count = getMethod<[idx: number, val: number], WasmSpanBufferInstance>(buffer, 'count');
      count(4, 456);
      const timestampBefore = buffer.timestamp;
      const identityBefore = buffer._identity;
      const countBefore = getColumn<Float64Array>(buffer, 'count_values');
      const versionBefore = buffer._descriptor.memoryVersion;

      const growthAllocation = allocator.allocExact(allocator.memory.buffer.byteLength, 8);
      expect(growthAllocation).toBeGreaterThan(0);
      const timestampAfter = buffer.timestamp;
      const identityAfter = buffer._identity;
      const countAfter = getColumn<Float64Array>(buffer, 'count_values');

      expect(buffer._descriptor.memoryVersion).toBeGreaterThan(versionBefore);
      expect(timestampAfter).not.toBe(timestampBefore);
      expect(identityAfter).not.toBe(identityBefore);
      expect(countAfter).not.toBe(countBefore);
      expect(buffer.timestamp).toBe(timestampAfter);
      expect(buffer._identity).toBe(identityAfter);
      expect(getColumn<Float64Array>(buffer, 'count_values')).toBe(countAfter);
      expect(timestampAfter[3]).toBe(123n);
      expect(countAfter[4]).toBe(456);
    });

    it('exposes _spanStartTime from row 0', () => {
      buffer.timestamp[0] = 123n;
      expect(buffer._spanStartTime).toBe(Nanoseconds.unsafe(123n));
    });

    it('exposes _lastLoggedTime across overflow chain', () => {
      buffer._writeIndex = 4;
      buffer.timestamp[0] = 100n;
      buffer.timestamp[1] = 200n;
      buffer.timestamp[2] = 300n;
      buffer.timestamp[3] = 400n;

      const overflow = createWasmOverflowBuffer(
        buffer,
        traceRoot,
        EMPTY_SCOPE,
        createTestOpMetadata(),
        createTestOpMetadata({ name: 'overflow', line: 0 }),
      );
      overflow._writeIndex = 2;
      overflow.timestamp[0] = 500n;
      overflow.timestamp[1] = 600n;

      expect(buffer._lastLoggedTime).toBe(Nanoseconds.unsafe(600n));
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
      expect(resolveMessage(buffer, 0)).toBe('span-start message');
      expect(resolveMessage(buffer, 1)).toBe('span-end message');
      expect(resolveMessage(buffer, 2)).toBe('log entry');
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
      const userId = getMethod<[idx: number, val: string], void>(buffer, 'userId');
      userId(0, 'user-123');

      expect(buffer.getColumnIfAllocated('userId')).toBeDefined();
      expect(buffer.getNullsIfAllocated('userId')).toBeDefined();
    });

    it('writes values to category column', () => {
      const userId = getMethod<[idx: number, val: string], WasmSpanBufferInstance>(buffer, 'userId');

      userId(0, 'user-001');
      userId(5, 'user-002');

      const userIdValues = getColumn<string[]>(buffer, 'userId_values');

      expect(userIdValues[0]).toBe('user-001');
      expect(userIdValues[5]).toBe('user-002');
    });

    it('sets validity bits on write', () => {
      const userId = getMethod<[idx: number, val: string | null], WasmSpanBufferInstance>(buffer, 'userId');

      userId(0, 'user-001');
      userId(5, 'user-002');

      const userIdNulls = getColumn<Uint8Array>(buffer, 'userId_nulls');

      // Check validity bits are set
      // Bit 0 in byte 0 should be 1
      expect(userIdNulls[0] & 0b00000001).toBe(1);
      // Bit 5 in byte 0 should be 1
      expect(userIdNulls[0] & 0b00100000).toBe(0b00100000);
    });

    it('clears validity bit for null', () => {
      const userId = getMethod<[idx: number, val: string | null], WasmSpanBufferInstance>(buffer, 'userId');

      userId(0, 'user-001');
      userId(0, null);

      const userIdNulls = getColumn<Uint8Array>(buffer, 'userId_nulls');

      // Bit 0 should be cleared
      expect(userIdNulls[0] & 0b00000001).toBe(0);
    });

    it('returns this for chaining', () => {
      const userId = getMethod<[idx: number, val: string], WasmSpanBufferInstance>(buffer, 'userId');
      const result = userId(0, 'test');
      expect(result).toBe(buffer);
    });
  });

  describe('lazy numeric columns (stored in WASM)', () => {
    let buffer: WasmSpanBufferInstance;

    beforeEach(() => {
      buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });
    });

    it('reserves numeric bytes in the family slab without materializing the lazy lane', () => {
      const countIndex = testSchema._columnNames.indexOf('count');
      if (countIndex < 0) throw new Error('missing count schema field');
      expect(buffer._layout.columns.some((column) => column.name === 'count')).toBe(true);
      expect(buffer.isColumnAllocated(countIndex)).toBe(false);
      expect(buffer.getColumnIfAllocated('count')).toBeUndefined();
      expect(Reflect.get(buffer, '_count_values')).toBeUndefined();
      expect(Reflect.get(buffer, '_count_nulls')).toBeUndefined();
    });

    it('writes without allocating another block', () => {
      const count = getMethod<[idx: number, val: number], WasmSpanBufferInstance>(buffer, 'count');
      const allocsBefore = allocator.getAllocCount();
      count(0, 42);
      expect(allocator.getAllocCount()).toBe(allocsBefore);
      expect(buffer.getColumnIfAllocated('count')).toBeInstanceOf(Float64Array);
      expect(buffer.getNullsIfAllocated('count')).toBeInstanceOf(Uint8Array);
    });

    it('writes to numeric column', () => {
      const count = getMethod<[idx: number, val: number], WasmSpanBufferInstance>(buffer, 'count');

      count(0, 42);
      count(5, 100);

      const countValues = getColumn<Float64Array | null>(buffer, 'count_values');

      // Values should be readable (note: WASM memory layout may vary)
      expect(countValues).toBeDefined();
    });

    it('returns this for chaining', () => {
      const count = getMethod<[idx: number, val: number], WasmSpanBufferInstance>(buffer, 'count');
      const result = count(0, 42);
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

  describe('compiler-proven eager columns', () => {
    it('shares exact frozen descriptor bytes and eager storage across root, child, and overflow', () => {
      const descriptor = compilerEagerOp.callsitePlan.eagerColumns;
      const planSchema = compilerEagerOp.callsitePlan.schema;
      const expectedWords = new Array<number>(Math.ceil(planSchema._columnNames.length / 32)).fill(0);
      for (const name of ['provenNumber', 'provenString']) {
        const ordinal = planSchema._columnNames.indexOf(name);
        if (ordinal < 0) throw new Error(`missing compiler eager schema field ${name}`);
        expectedWords[ordinal >>> 5] = (expectedWords[ordinal >>> 5] | (1 << (ordinal & 31))) >>> 0;
      }
      while (expectedWords.length > 0 && expectedWords[expectedWords.length - 1] === 0) expectedWords.pop();

      expect(descriptor.names).toEqual(['provenNumber', 'provenString']);
      expect(descriptor.words).toEqual(expectedWords);
      expect(eagerDescriptorBytes(descriptor).byteLength).toBe(descriptor.words.length * 4);
      expect(Object.isFrozen(descriptor)).toBe(true);
      expect(Object.isFrozen(descriptor.names)).toBe(true);
      expect(Object.isFrozen(descriptor.words)).toBe(true);
      expect(compilerEagerOp.callsitePlan.wasmLayout.eagerColumns).toBe(descriptor);

      const root = createCompilerEagerWasmBuffer();
      const child = createWasmChildSpanBuffer(
        root,
        { allocator, capacity: CAPACITY, thread_id: 42n, span_id: 2 },
        traceRoot,
        EMPTY_SCOPE,
        compilerEagerOp.metadata,
        compilerEagerOp.metadata,
      );
      const overflow = createWasmOverflowBuffer(
        child,
        traceRoot,
        EMPTY_SCOPE,
        compilerEagerOp.metadata,
        compilerEagerOp.metadata,
      );

      for (const buffer of [root, child, overflow]) {
        expect(buffer._layout.eagerColumns).toBe(descriptor);
        for (const name of ['provenNumber', 'provenString']) {
          expect(Object.hasOwn(buffer, `_${name}_values`)).toBe(true);
          expect(Object.hasOwn(buffer, `_${name}_nulls`)).toBe(true);
          expect(Reflect.get(buffer, `_${name}_values`)).toBeDefined();
          expect(Reflect.get(buffer, `_${name}_nulls`)).toBeInstanceOf(Uint8Array);
        }
        for (const name of ['lazyNumber', 'lazyString']) {
          expect(Reflect.get(buffer, `_${name}_values`)).toBeUndefined();
          expect(Reflect.get(buffer, `_${name}_nulls`)).toBeUndefined();
          expect(buffer.getColumnIfAllocated(name)).toBeUndefined();
          expect(buffer.getNullsIfAllocated(name)).toBeUndefined();
        }
      }
      expect(child._parent).toBe(root);
      expect(overflow._parent).toBe(root);
      expect(overflow._identityPtr).toBe(child._identityPtr);
      expect(overflow._descriptor.layout).toBe(child._descriptor.layout);

      root.provenNumber(0, 73);
      root.provenNumber(1, null);
      root.provenString(2, 'wasm-value');
      overflow.provenNumber(3, null);

      const jsBuffer = createSpanBuffer(
        compilerEagerSchema,
        createTestTraceRoot('compiler-eager-js'),
        compilerEagerOp.metadata,
        CAPACITY,
        compilerEagerOp.callsitePlan.SpanBufferClass,
      );
      jsBuffer.provenNumber(0, 73);
      jsBuffer.provenNumber(1, null);
      jsBuffer.provenString(2, 'wasm-value');

      for (const name of ['provenNumber', 'provenString']) {
        const wasmValues = root.getColumnIfAllocated(name);
        const jsValues = jsBuffer.getColumnIfAllocated(name);
        const wasmNulls = root.getNullsIfAllocated(name);
        const jsNulls = jsBuffer.getNullsIfAllocated(name);
        if (!wasmValues || !jsValues || !wasmNulls || !jsNulls) {
          throw new Error(`missing eager JS/WASM parity storage for ${name}`);
        }
        for (const row of [0, 1, 2, 3]) expect(wasmValues[row]).toEqual(jsValues[row]);
        expect([...wasmNulls]).toEqual([...jsNulls]);
      }

      const lazyString = getMethod<[index: number, value: string], WasmSpanBufferInstance>(root, 'lazyString');
      lazyString(4, 'first lazy write');
      expect(root.getColumnIfAllocated('lazyString')?.[4]).toBe('first lazy write');
      const lazyStringNulls = root.getNullsIfAllocated('lazyString');
      if (!lazyStringNulls) throw new Error('missing lazy string null bitmap after first write');
      expect(lazyStringNulls[0] & 0b00010000).toBe(0b00010000);

      const schemaEager = createTestWasmBuffer();
      expect(schemaEager.getColumnIfAllocated('statusCode')).toBeDefined();
      expect(schemaEager.getNullsIfAllocated('statusCode')).toBeUndefined();
    });
  });

  describe('free()', () => {
    it('releases each owned exact slab and identity once', () => {
      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });
      const ownedSlabs = Object.values(buffer._layout.slabs).filter((slab) => slab !== null).length;
      const freesBefore = allocator.getFreeCount();

      buffer.free();

      expect(allocator.getFreeCount() - freesBefore).toBe(2 + ownedSlabs); // identity + system + families
      expect(buffer._descriptor.state).toBe('freed');
    });

    it('is idempotent and rejects every stale accessor and writer', () => {
      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });
      const count = getMethod<[idx: number, val: number], WasmSpanBufferInstance>(buffer, 'count');
      buffer.free();
      const freesAfterFirstRelease = allocator.getFreeCount();

      buffer.free();

      expect(allocator.getFreeCount()).toBe(freesAfterFirstRelease);
      expect(() => buffer.timestamp).toThrow(/generation .* released/);
      expect(() => buffer.entry_type).toThrow(/generation .* released/);
      expect(() => buffer._identity).toThrow(/generation .* released/);
      expect(() => count(0, 42)).toThrow(/generation .* released/);
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
      expect(child._identityPtr).not.toBe(parent._identityPtr);
      expect(child._identityOwner).toBe(true);
      expect(child._descriptor.kind).toBe('child');
      expect(child._descriptor.parent).toBe(parent._descriptor);
      expect(child._descriptor.generation).toBeGreaterThan(parent._descriptor.generation);
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
      expect(overflow.span_id).toBe(buffer.span_id);
      expect(overflow._identityPtr).toBe(buffer._identityPtr);
      expect(overflow._identityOwner).toBe(false);
      expect(overflow._descriptor.kind).toBe('overflow');
      expect(overflow._descriptor.parent).toBeUndefined();
      expect(buffer._descriptor.overflow).toBe(overflow._descriptor);
      expect(overflow._descriptor.generation).toBeGreaterThan(buffer._descriptor.generation);
    });
  });

  describe('custom inspect', () => {
    it('provides useful inspect output', () => {
      // thread_id is global in WASM header
      allocator.setThreadId(0, 1); // thread_id = 1n

      const buffer = createTestWasmBuffer({ trace_id: 'trace-123', thread_id: 1n, span_id: 1 });

      // Get custom inspect
      const inspectSymbol = Symbol.for('nodejs.util.inspect.custom');
      const inspectFn = Reflect.get(buffer, inspectSymbol);
      if (typeof inspectFn !== 'function') {
        throw new Error('expected custom inspect function on WasmSpanBuffer');
      }
      const result = inspectFn.call(buffer);

      expect(result).toContain('WasmSpanBuffer');
      expect(result).toContain('trace_id: trace-123');
      expect(result).toContain('_systemPtr:');
    });
  });
});
