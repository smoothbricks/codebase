import { beforeEach, describe, expect, it } from 'bun:test';
import { createWasmAllocator, SizeClass, type WasmAllocator } from '../wasmAllocator.js';

describe('WasmAllocator', () => {
  let allocator: WasmAllocator;

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: 64 });
  });

  describe('instantiation', () => {
    it('creates allocator with default options', async () => {
      const alloc = await createWasmAllocator();
      expect(alloc).toBeDefined();
      expect(alloc.memory).toBeInstanceOf(WebAssembly.Memory);
      expect(alloc.u8).toBeInstanceOf(Uint8Array);
      expect(alloc.u32).toBeInstanceOf(Uint32Array);
      expect(alloc.i64).toBeInstanceOf(BigInt64Array);
      expect(alloc.f64).toBeInstanceOf(Float64Array);
    });

    it('creates allocator with custom capacity', async () => {
      const alloc = await createWasmAllocator({ capacity: 128 });
      expect(alloc.capacity).toBe(128);
    });
  });

  describe('init', () => {
    it('initializes freelists to empty', async () => {
      // Create fresh allocator
      const alloc = await createWasmAllocator({ capacity: 64 });

      // All freelists should be empty after init
      for (const tier of [8, 16, 32, 64, 128, 256, 512]) {
        for (const sc of [SizeClass.SpanSystem, SizeClass.Col1B, SizeClass.Col4B, SizeClass.Col8B]) {
          const len = alloc.getFreelistLen(sc, tier);
          expect(len).toBe(0);
        }
      }
    });

    it('stores default capacity', () => {
      expect(allocator.capacity).toBe(64);
    });

    it('can create allocator with different default capacity', async () => {
      const alloc128 = await createWasmAllocator({ capacity: 128 });
      expect(alloc128.capacity).toBe(128);
    });
  });

  describe('allocSpanSystem', () => {
    it('returns valid offset greater than header size (192 bytes)', () => {
      const offset = allocator.allocSpanSystem();
      expect(offset).toBeGreaterThanOrEqual(192);
    });

    it('returns different offsets for multiple allocations', () => {
      const offset1 = allocator.allocSpanSystem();
      const offset2 = allocator.allocSpanSystem();
      expect(offset1).not.toBe(offset2);
    });

    it('returns 8-byte aligned offsets', () => {
      const offset = allocator.allocSpanSystem();
      expect(offset % 8).toBe(0);
    });

    it('increments alloc count', () => {
      const before = allocator.getAllocCount();
      allocator.allocSpanSystem();
      expect(allocator.getAllocCount()).toBe(before + 1);
    });
  });

  describe('freelist reuse', () => {
    it('reuses freed SpanSystem offset', () => {
      const offset1 = allocator.allocSpanSystem();
      allocator.freeSpanSystem(offset1);
      const offset2 = allocator.allocSpanSystem();
      expect(offset2).toBe(offset1);
    });

    it('reuses freed 1B column offset', () => {
      const offset1 = allocator.alloc1B();
      allocator.free1B(offset1);
      const offset2 = allocator.alloc1B();
      expect(offset2).toBe(offset1);
    });

    it('reuses freed 4B column offset', () => {
      const offset1 = allocator.alloc4B();
      allocator.free4B(offset1);
      const offset2 = allocator.alloc4B();
      expect(offset2).toBe(offset1);
    });

    it('reuses freed 8B column offset', () => {
      const offset1 = allocator.alloc8B();
      allocator.free8B(offset1);
      const offset2 = allocator.alloc8B();
      expect(offset2).toBe(offset1);
    });

    it('reuses freed identity block offset', () => {
      const offset1 = allocator.allocIdentityChild();
      allocator.freeIdentity(offset1);
      const offset2 = allocator.allocIdentityChild();
      expect(offset2).toBe(offset1);
    });

    it('increments free count', () => {
      const offset = allocator.allocSpanSystem();
      const before = allocator.getFreeCount();
      allocator.freeSpanSystem(offset);
      expect(allocator.getFreeCount()).toBe(before + 1);
    });
  });

  describe('reset', () => {
    it('clears freelists and resets bump ptr to header', () => {
      // Allocate some blocks
      allocator.allocSpanSystem();
      allocator.alloc1B();
      allocator.alloc4B();
      allocator.alloc8B();

      const bumpBefore = allocator.getBumpPtr();
      expect(bumpBefore).toBeGreaterThan(192);

      // Reset
      allocator.reset();

      // Bump ptr should be back at header (192 bytes)
      expect(allocator.getBumpPtr()).toBe(192);
      expect(allocator.getAllocCount()).toBe(0);
      expect(allocator.getFreeCount()).toBe(0);
    });
  });

  describe('span lifecycle', () => {
    it('spanStart writes entry_type=1 at row 0', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);

      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);

      expect(allocator.readEntryType(systemPtr, 0)).toBe(1); // SPAN_START
    });

    it('spanStart sets entry_type=4 (SPAN_EXCEPTION) at row 1 as placeholder', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);

      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);

      expect(allocator.readEntryType(systemPtr, 1)).toBe(4); // SPAN_EXCEPTION placeholder
    });

    it('spanStart sets writeIndex to 2', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);

      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);

      expect(allocator.readWriteIndex(identityPtr)).toBe(2);
    });

    it('spanEndOk writes entry_type=2 at row 1', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);

      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);
      allocator.spanEndOk(systemPtr, traceRootPtr);

      expect(allocator.readEntryType(systemPtr, 1)).toBe(2); // SPAN_OK
    });

    it('spanEndErr writes entry_type=3 at row 1', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);

      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);
      allocator.spanEndErr(systemPtr, traceRootPtr);

      expect(allocator.readEntryType(systemPtr, 1)).toBe(3); // SPAN_ERR
    });

    it('spanStart writes timestamp at row 0', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);

      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);

      const timestamp = allocator.readTimestamp(systemPtr, 0);
      expect(timestamp).toBeGreaterThan(0n);
    });

    it('spanEndOk writes timestamp at row 1', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);

      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);
      allocator.spanEndOk(systemPtr, traceRootPtr);

      const timestamp = allocator.readTimestamp(systemPtr, 1);
      expect(timestamp).toBeGreaterThan(0n);
    });
  });

  describe('writeLogEntry', () => {
    it('writes log entry and returns row index', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);
      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);

      // _writeIndex starts at 2 after spanStart
      const rowIdx = allocator.writeLogEntry(systemPtr, identityPtr, traceRootPtr, 6); // INFO = 6

      expect(rowIdx).toBe(2);
      expect(allocator.readEntryType(systemPtr, 2)).toBe(6);
      expect(allocator.readWriteIndex(identityPtr)).toBe(3);
    });

    it('increments writeIndex for each log entry', () => {
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();
      const traceRootPtr = allocator.alloc8B();
      allocator.initTraceRoot(traceRootPtr);
      allocator.spanStart(systemPtr, identityPtr, traceRootPtr);

      const idx1 = allocator.writeLogEntry(systemPtr, identityPtr, traceRootPtr, 6);
      const idx2 = allocator.writeLogEntry(systemPtr, identityPtr, traceRootPtr, 7);
      const idx3 = allocator.writeLogEntry(systemPtr, identityPtr, traceRootPtr, 8);

      expect(idx1).toBe(2);
      expect(idx2).toBe(3);
      expect(idx3).toBe(4);
      expect(allocator.readWriteIndex(identityPtr)).toBe(5);
    });
  });

  describe('column writes', () => {
    it('writeColF64 allocates and writes value', () => {
      let colOffset = 0;
      colOffset = allocator.writeColF64(colOffset, 0, 3.14);

      expect(colOffset).toBeGreaterThan(0);
      expect(allocator.readColF64(colOffset, 0)).toBeCloseTo(3.14);
      expect(allocator.readColIsValid(colOffset, 0)).toBe(1);
    });

    it('writeColF64 writes to existing column', () => {
      let colOffset = allocator.writeColF64(0, 0, 1.0);
      colOffset = allocator.writeColF64(colOffset, 1, 2.0);
      allocator.writeColF64(colOffset, 2, 3.0);

      expect(allocator.readColF64(colOffset, 0)).toBeCloseTo(1.0);
      expect(allocator.readColF64(colOffset, 1)).toBeCloseTo(2.0);
      expect(allocator.readColF64(colOffset, 2)).toBeCloseTo(3.0);
    });

    it('writeColU32 allocates and writes value', () => {
      let colOffset = 0;
      colOffset = allocator.writeColU32(colOffset, 0, 42);

      expect(colOffset).toBeGreaterThan(0);
      // Read via u32 view
      const nullBitmapSize = Math.ceil(64 / 8); // capacity / 8
      const valueOffset = (colOffset + nullBitmapSize) / 4; // convert to u32 index
      expect(allocator.u32[valueOffset]).toBe(42);
    });

    it('writeColU8 allocates and writes value', () => {
      let colOffset = 0;
      colOffset = allocator.writeColU8(colOffset, 0, 255);

      expect(colOffset).toBeGreaterThan(0);
      // Read via u8 view
      const nullBitmapSize = Math.ceil(64 / 8); // capacity / 8
      expect(allocator.u8[colOffset + nullBitmapSize]).toBe(255);
    });

    it('writeColF64 sets null bit (validity)', () => {
      const colOffset = allocator.writeColF64(0, 5, 99.9);

      // Check that row 5 is marked valid
      expect(allocator.readColIsValid(colOffset, 5)).toBe(1);
      // Check that row 0 is not valid (we didn't write to it)
      expect(allocator.readColIsValid(colOffset, 0)).toBe(0);
    });
  });

  describe('TraceRoot', () => {
    it('initTraceRoot sets wall clock and monotonic time', () => {
      const traceRootPtr = allocator.alloc8B(); // 8B column is large enough for 16 bytes
      const before = Date.now();

      allocator.initTraceRoot(traceRootPtr);

      // Read wall clock (first 8 bytes as i64 nanoseconds)
      const wallClockNanos = allocator.i64[traceRootPtr / 8];
      const wallClockMs = Number(wallClockNanos / 1_000_000n);

      expect(wallClockMs).toBeGreaterThanOrEqual(before);
      expect(wallClockMs).toBeLessThan(before + 1000); // Within 1 second

      // Read monotonic time (next 8 bytes as f64 milliseconds)
      const monotonicMs = allocator.f64[(traceRootPtr + 8) / 8];
      expect(monotonicMs).toBeGreaterThan(0);
    });
  });

  describe('identity block', () => {
    it('allocIdentityChild returns valid offset', () => {
      const identityPtr = allocator.allocIdentityChild();
      expect(identityPtr).toBeGreaterThanOrEqual(192);
    });

    it('allocIdentityChild returns unique span_id for each allocation', () => {
      const id1 = allocator.allocIdentityChild();
      const id2 = allocator.allocIdentityChild();

      const spanId1 = allocator.readIdentitySpanId(id1);
      const spanId2 = allocator.readIdentitySpanId(id2);

      expect(spanId1).toBeGreaterThan(0);
      expect(spanId2).toBeGreaterThan(spanId1);
    });

    it('allocIdentityRoot stores trace_id', () => {
      // Allocate a scratch buffer first to use as temp storage for trace_id
      // We can't use getBumpPtr() directly since alloc_identity_root will overwrite it
      const scratchPtr = allocator.alloc8B(); // Use an 8B block as scratch space

      const traceId = 'test-trace-id-123';
      const traceIdBytes = new TextEncoder().encode(traceId);
      allocator.u8.set(traceIdBytes, scratchPtr);

      const identityPtr = allocator.allocIdentityRoot(scratchPtr, traceIdBytes.length);

      expect(allocator.readIdentityTraceIdLen(identityPtr)).toBe(traceIdBytes.length);

      // Read back trace_id
      const storedTraceIdPtr = allocator.getIdentityTraceIdPtr(identityPtr);
      const storedBytes = allocator.u8.slice(storedTraceIdPtr, storedTraceIdPtr + traceIdBytes.length);
      const storedTraceId = new TextDecoder().decode(storedBytes);
      expect(storedTraceId).toBe(traceId);

      // Clean up scratch buffer
      allocator.free8B(scratchPtr);
    });
  });

  describe('block sizes', () => {
    it('reports correct span system size for capacity 64', () => {
      // capacity × 9 bytes (timestamp + entry_type, writeIndex moved to identity)
      // 64 × 9 = 576, aligned to 8 = 576
      expect(allocator.getSpanSystemSize()).toBe(576);
    });

    it('reports correct 1B column size for capacity 64', () => {
      // ceil(capacity/8) + capacity × 1
      // ceil(64/8) + 64 = 8 + 64 = 72
      expect(allocator.getCol1BSize()).toBe(72);
    });

    it('reports correct 4B column size for capacity 64', () => {
      // ceil(capacity/8) + capacity × 4
      // 8 + 256 = 264
      expect(allocator.getCol4BSize()).toBe(264);
    });

    it('reports correct 8B column size for capacity 64', () => {
      // ceil(capacity/8) + capacity × 8
      // 8 + 512 = 520
      expect(allocator.getCol8BSize()).toBe(520);
    });
  });

  describe('freelist statistics', () => {
    it('getFreelistLen returns 0 for empty freelist', () => {
      // Fresh allocator, no frees yet
      expect(allocator.getFreelistLen(SizeClass.SpanSystem)).toBe(0);
      expect(allocator.getFreelistLen(SizeClass.Col1B)).toBe(0);
      expect(allocator.getFreelistLen(SizeClass.Col4B)).toBe(0);
      expect(allocator.getFreelistLen(SizeClass.Col8B)).toBe(0);
    });

    it('getFreelistLen increments on free at max tier', async () => {
      // Use max tier (512) to avoid buddy split/merge behavior
      const alloc = await createWasmAllocator({ capacity: 512 });

      // Allocate 2 blocks at max tier (bump allocates directly, no split)
      const offset1 = alloc.allocSpanSystem(512);
      const offset2 = alloc.allocSpanSystem(512);

      expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(0);

      alloc.freeSpanSystem(offset1, 512);
      expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(1);

      alloc.freeSpanSystem(offset2, 512);
      // No merge at max tier, both should be in freelist
      expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(2);
    });

    it('getFreelistReuseCount increments on alloc from freelist', async () => {
      // Use max tier (512) to avoid buddy split/merge behavior
      const alloc = await createWasmAllocator({ capacity: 512 });

      // Allocate 2 blocks at max tier
      const offset1 = alloc.allocSpanSystem(512);
      const offset2 = alloc.allocSpanSystem(512);

      // Free both (no merge at max tier)
      alloc.freeSpanSystem(offset1, 512);
      alloc.freeSpanSystem(offset2, 512);

      expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(2);
      expect(alloc.getFreelistReuseCount(SizeClass.SpanSystem, 512)).toBe(0);

      // Alloc from freelist - should increment reuse count
      alloc.allocSpanSystem(512);
      expect(alloc.getFreelistReuseCount(SizeClass.SpanSystem, 512)).toBe(1);

      alloc.allocSpanSystem(512);
      // Freelist empty now, so reuse count stays at previous HEAD value
      expect(alloc.getFreelistReuseCount(SizeClass.SpanSystem, 512)).toBe(0); // empty freelist returns 0
    });

    it('cascading stats accumulate correctly', async () => {
      // Use fresh allocator to avoid interference from other tests
      const freshAlloc = await createWasmAllocator({ capacity: 64 });

      // Allocate 6 blocks - keep every other one to prevent merge on free
      const offsets: number[] = [];
      const keepers: number[] = [];
      for (let i = 0; i < 12; i++) {
        const offset = freshAlloc.alloc8B();
        if (i % 2 === 0) {
          offsets.push(offset);
        } else {
          keepers.push(offset);
        }
      }

      // Free the 6 non-adjacent blocks (they can't merge because keepers are between them)
      for (const offset of offsets) {
        freshAlloc.free8B(offset);
      }

      expect(freshAlloc.getFreelistLen(SizeClass.Col8B)).toBe(6);

      // Allocate 4 back
      freshAlloc.alloc8B();
      freshAlloc.alloc8B();
      freshAlloc.alloc8B();
      freshAlloc.alloc8B();

      expect(freshAlloc.getFreelistLen(SizeClass.Col8B)).toBe(2); // 6 - 4 = 2 remaining
      expect(freshAlloc.getFreelistReuseCount(SizeClass.Col8B)).toBe(4); // 4 reuses
    });
  });

  describe('buddy allocation', () => {
    it('splits larger tier block when current tier freelist is empty', async () => {
      // Create allocator
      const alloc = await createWasmAllocator({ capacity: 32 });

      // Allocate at capacity 64 (tier 3) and free it
      const largeOffset = alloc.allocSpanSystem(64);
      alloc.freeSpanSystem(largeOffset, 64);

      // Freelist for tier 2 is empty, but tier 3 has a block
      // Buddy split should occur when allocating at capacity 32
      const offset1 = alloc.allocSpanSystem(32);
      expect(offset1).toBe(largeOffset); // First half of split block

      // Second half should now be in freelist at tier 2 (capacity 32)
      expect(alloc.getFreelistLen(SizeClass.SpanSystem, 32)).toBe(1);

      // Split count should be 1
      expect(alloc.getFreelistSplitCount(SizeClass.SpanSystem, 32)).toBe(1);

      // Allocate again - should get second half from freelist
      const offset2 = alloc.allocSpanSystem(32);
      expect(offset2).toBe(largeOffset + alloc.getSpanSystemSize(32));
    });

    describe('buddy merge', () => {
      it('basic freelist push/pop at max tier works correctly', async () => {
        // Use max tier (512) to avoid split behavior
        const alloc = await createWasmAllocator({ capacity: 512 });

        // Allocate at max capacity (tier 6) - bump allocates directly
        const offset = alloc.allocSpanSystem(512);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(0); // Just allocated, freelist empty

        alloc.freeSpanSystem(offset, 512);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(1); // Freed, freelist has 1

        // Allocate again - should get same offset from freelist
        const offset2 = alloc.allocSpanSystem(512);
        expect(offset2).toBe(offset);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(0); // Popped, freelist empty
      });

      it('free at tier 6 (max) pushes correctly', async () => {
        const alloc = await createWasmAllocator({ capacity: 512 });

        // Allocate at max capacity (tier 6)
        const offset = alloc.allocSpanSystem(512);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(0);

        // Free at capacity 512 (tier 6) - should NOT try to merge (no higher tier)
        alloc.freeSpanSystem(offset, 512);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(1);
      });

      it('adjacent blocks cascade merge to max tier', async () => {
        const alloc = await createWasmAllocator({ capacity: 32 });

        // Allocate two tier-3 blocks
        const block1 = alloc.allocSpanSystem(64);
        const block2 = alloc.allocSpanSystem(64);

        // Verify they're adjacent
        expect(block2).toBe(block1 + alloc.getSpanSystemSize(64));

        // Free both - should cascade merge all the way to max tier
        alloc.freeSpanSystem(block1, 64);
        alloc.freeSpanSystem(block2, 64);

        // After cascade, only max tier should have the block
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 64)).toBe(0);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(1);
      });

      it('merges adjacent blocks when both are freed (cascading)', async () => {
        // Create allocator
        const alloc = await createWasmAllocator({ capacity: 32 });

        // Allocate two capacity-32 blocks (which splits all the way from max tier)
        const child1 = alloc.allocSpanSystem(32);
        const child2 = alloc.allocSpanSystem(32);

        // Verify they're adjacent
        expect(child2).toBe(child1 + alloc.getSpanSystemSize(32));

        // Free both - they should merge and cascade up to max tier
        alloc.freeSpanSystem(child2, 32);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 32)).toBe(1); // child2 in tier-2

        alloc.freeSpanSystem(child1, 32);

        // After merge cascade, tier-2 should be empty
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 32)).toBe(0);

        // The merge cascades all the way to max tier (512) because there are
        // sibling blocks at each tier from the original split
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(1);
      });

      it('merges regardless of free order', async () => {
        // Create allocator
        const alloc = await createWasmAllocator({ capacity: 32 });

        // Allocate two capacity-32 blocks
        const child1 = alloc.allocSpanSystem(32);
        const child2 = alloc.allocSpanSystem(32);

        // Free in opposite order (child1 first instead of child2)
        alloc.freeSpanSystem(child1, 32);
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 32)).toBe(1);

        alloc.freeSpanSystem(child2, 32);

        // After merge cascade, tier-2 should be empty
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 32)).toBe(0);

        // Cascades to max tier
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(1);
      });

      it('cascading merge: all the way to max tier', async () => {
        // Create allocator
        const alloc = await createWasmAllocator({ capacity: 16 });

        // Allocate 4 blocks at capacity 16 (splits from max tier)
        const blockSize16 = alloc.getSpanSystemSize(16);

        const blocks = [
          alloc.allocSpanSystem(16),
          alloc.allocSpanSystem(16),
          alloc.allocSpanSystem(16),
          alloc.allocSpanSystem(16),
        ];

        // Verify offsets are sequential
        expect(blocks[1]).toBe(blocks[0] + blockSize16);
        expect(blocks[2]).toBe(blocks[0] + blockSize16 * 2);
        expect(blocks[3]).toBe(blocks[0] + blockSize16 * 3);

        // Free all 4 blocks - should cascade merge all the way to max tier
        for (const offset of blocks) {
          alloc.freeSpanSystem(offset, 16);
        }

        // After full cascade, should end up at max tier (512)
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(1);
      });

      it('partial merge when not all siblings freed', async () => {
        const alloc = await createWasmAllocator({ capacity: 16 });

        // Allocate 4 blocks at capacity 16
        const blocks = [
          alloc.allocSpanSystem(16),
          alloc.allocSpanSystem(16),
          alloc.allocSpanSystem(16),
          alloc.allocSpanSystem(16),
        ];

        // Free only first two - should merge to tier-2 (capacity 32) but stop there
        // because blocks[2] and [3] are still allocated
        alloc.freeSpanSystem(blocks[0], 16);
        alloc.freeSpanSystem(blocks[1], 16);

        // Tier-1 (16) should be empty (merged)
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 16)).toBe(0);

        // Since blocks[2] and [3] are still allocated, merge should stop
        // somewhere in the middle, not reach max tier
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(0);
      });

      it('merge count accumulates correctly', async () => {
        const alloc = await createWasmAllocator({ capacity: 32 });

        // Allocate and free at max tier to get a clean baseline
        const block1 = alloc.allocSpanSystem(512);
        const block2 = alloc.allocSpanSystem(512);
        alloc.freeSpanSystem(block1, 512);
        alloc.freeSpanSystem(block2, 512);

        // Now tier-6 has 2 blocks (no merge possible at max tier)
        expect(alloc.getFreelistLen(SizeClass.SpanSystem, 512)).toBe(2);

        // Merge count at max tier should still be 0 (no merges at max tier)
        expect(alloc.getFreelistMergeCount(SizeClass.SpanSystem, 512)).toBe(0);
      });
    });
  });

  describe('memory growth', () => {
    it('grows memory when bump allocation exceeds current size', async () => {
      // Start with minimal memory (17 pages = ~1MB)
      const alloc = await createWasmAllocator({ initialPages: 17, capacity: 512 });

      const initialBump = alloc.getBumpPtr();
      const blockSize = alloc.getSpanSystemSize(); // 512 * 9 = 4608 bytes per block

      // Allocate enough blocks to exceed initial memory
      // 17 pages = 17 * 64KB = 1,114,112 bytes
      // Each block is ~4608 bytes, so ~242 blocks to fill initial memory
      const blocksToAllocate = 300; // More than fits in initial memory

      const offsets: number[] = [];
      for (let i = 0; i < blocksToAllocate; i++) {
        const offset = alloc.allocSpanSystem();
        expect(offset).toBeGreaterThan(0); // Should not fail
        offsets.push(offset);
      }

      // Bump pointer should have grown well beyond initial memory
      const finalBump = alloc.getBumpPtr();
      const totalAllocated = finalBump - initialBump;

      // We should have allocated roughly blocksToAllocate * blockSize bytes
      expect(totalAllocated).toBeGreaterThan(blocksToAllocate * blockSize * 0.9);

      // Memory should have grown (check that last offset is valid by writing to it)
      const lastOffset = offsets[offsets.length - 1];
      expect(lastOffset).toBeGreaterThan(17 * 65536); // Beyond initial 17 pages
    });
  });
});
