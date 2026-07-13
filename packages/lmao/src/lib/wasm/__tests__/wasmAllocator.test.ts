import { beforeEach, describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { createWasmAllocator, type WasmAllocator } from '../wasmAllocator.js';

const CAPACITY = 64;
const SYSTEM_BYTE_LENGTH = CAPACITY * 9;
const TRACE_ROOT_BYTE_LENGTH = 16;

describe('WasmAllocator exact slabs', () => {
  let allocator: WasmAllocator;

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: CAPACITY });
    allocator.reset();
  });

  it('exposes stable canonical allocator views until memory grows', () => {
    expect(allocator.u8).toBe(allocator.u8);
    expect(allocator.u32).toBe(allocator.u32);
    expect(allocator.i64).toBe(allocator.i64);
    expect(allocator.f64).toBe(allocator.f64);
    const versionBefore = allocator.memoryVersion;
    const u8Before = allocator.u8;

    const allocation = allocator.allocExact(allocator.memory.buffer.byteLength, 8);
    expect(allocation).toBeGreaterThan(0);

    expect(allocator.refreshViews()).toBeGreaterThan(versionBefore);
    expect(allocator.u8).not.toBe(u8Before);
    expect(allocator.u8).toBe(allocator.u8);
  });

  it('allocates arbitrary exact extents at aligned non-overlapping offsets', () => {
    fc.assert(
      fc.property(
        fc.array(
          fc.record({
            byteLength: fc.integer({ min: 1, max: 2048 }),
            alignmentPower: fc.integer({ min: 0, max: 7 }),
          }),
          { minLength: 1, maxLength: 100 },
        ),
        (requests) => {
          allocator.reset();
          const live: Array<readonly [number, number]> = [];
          for (const { byteLength, alignmentPower } of requests) {
            const alignment = 1 << alignmentPower;
            const offset = allocator.allocExact(byteLength, alignment);
            expect(offset).toBeGreaterThanOrEqual(192);
            expect(offset % alignment).toBe(0);
            for (const [otherOffset, otherLength] of live) {
              expect(offset + byteLength <= otherOffset || offset >= otherOffset + otherLength).toBe(true);
            }
            live.push([offset, byteLength]);
          }
          expect(allocator.getAllocCount()).toBe(requests.length);
        },
      ),
    );
  });

  it('reuses a matching exact descriptor once and makes repeated release idempotent', () => {
    const offset = allocator.allocExact(257, 64);
    allocator.freeExact(offset, 257, 64);
    const freeCount = allocator.getFreeCount();

    allocator.freeExact(offset, 257, 64);

    expect(allocator.getFreeCount()).toBe(freeCount);
    const recycled = allocator.allocExact(257, 64);
    expect(recycled).toBe(offset);
    expect(allocator.allocExact(257, 64)).not.toBe(recycled);
  });

  it('reset invalidates allocation counters and reuses the exact header boundary', () => {
    allocator.allocExact(17, 8);
    allocator.allocExact(65, 16);
    expect(allocator.getAllocCount()).toBe(2);

    allocator.reset();

    expect(allocator.getBumpPtr()).toBe(192);
    expect(allocator.getAllocCount()).toBe(0);
    expect(allocator.getFreeCount()).toBe(0);
    expect(allocator.allocExact(1, 1)).toBe(192);
  });

  it('drives span lifecycle over an exact system slab', () => {
    const system = allocator.allocExact(SYSTEM_BYTE_LENGTH, 8);
    const identity = allocator.allocIdentityChild();
    const traceRoot = allocator.allocExact(TRACE_ROOT_BYTE_LENGTH, 8);
    allocator.initTraceRoot(traceRoot);

    allocator.spanStart(system, identity, traceRoot, CAPACITY);
    expect(allocator.readEntryType(system, 0, CAPACITY)).toBe(1);
    expect(allocator.readEntryType(system, 1, CAPACITY)).toBe(4);
    expect(allocator.readWriteIndex(identity)).toBe(2);

    const row = allocator.writeLogEntry(system, identity, traceRoot, 5, CAPACITY);
    expect(row).toBe(2);
    expect(allocator.readTimestamp(system, row)).toBeGreaterThan(0n);

    allocator.spanEndOk(system, traceRoot, CAPACITY);
    expect(allocator.readEntryType(system, 1, CAPACITY)).toBe(2);
  });

  it('preserves root identity ownership and exact trace bytes', () => {
    const traceIdBytes = new TextEncoder().encode('exact-root-trace');
    const packed = allocator.allocIdentityRootForJsWrite(traceIdBytes.length);
    const identity = Number(packed >> 32n);
    const traceIdOffset = Number(packed & 0xffffffffn);
    allocator.u8.set(traceIdBytes, traceIdOffset);

    expect(allocator.readIdentityTraceIdLen(identity)).toBe(traceIdBytes.length);
    expect(allocator.getIdentityTraceIdPtr(identity)).toBe(traceIdOffset);
    expect(new TextDecoder().decode(allocator.u8.subarray(traceIdOffset, traceIdOffset + traceIdBytes.length))).toBe(
      'exact-root-trace',
    );
  });

  it('grows memory for a single exact slab and keeps its address valid', async () => {
    const alloc = await createWasmAllocator({ initialPages: 17, capacity: CAPACITY });
    const initialByteLength = alloc.memory.buffer.byteLength;
    const offset = alloc.allocExact(initialByteLength, 8);
    expect(offset).toBeGreaterThan(0);
    expect(alloc.memory.buffer.byteLength).toBeGreaterThan(initialByteLength);

    alloc.u8[offset] = 0xa5;
    expect(alloc.u8[offset]).toBe(0xa5);
  });
});
