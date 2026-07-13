import { beforeEach, describe, expect, it } from 'bun:test';
import { createWasmAllocator, type WasmAllocator } from '../wasmAllocator.js';

/**
 * Exact slabs may be smaller than the allocator's intrusive free-list metadata.
 * Releasing such a slab must reserve bookkeeping storage internally rather than
 * overwrite an adjacent live slab.
 */
describe('sub-metadata exact slab safety', () => {
  let allocator: WasmAllocator;

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: 64 });
    allocator.reset();
  });

  function churnAndAssertNoCorruption(byteLength: 9 | 18) {
    const batch = 8;
    const rounds = 25;
    let nextMarker = 1;
    const live = new Map<number, number>();

    function allocAndMark(): number {
      const offset = allocator.allocExact(byteLength, 1);
      expect(offset).toBeGreaterThan(0);
      const marker = nextMarker++ & 0xff;
      allocator.u8[offset + byteLength - 1] = marker;
      live.set(offset, marker);
      return offset;
    }

    function assertAllLiveIntact(context: string) {
      for (const [offset, marker] of live) {
        expect(allocator.u8[offset + byteLength - 1], `exact slab at ${offset} corrupted (${context})`).toBe(marker);
      }
    }

    for (let i = 0; i < batch; i++) allocAndMark();
    assertAllLiveIntact('after initial batch');

    for (let round = 0; round < rounds; round++) {
      const offsets = [...live.keys()];
      for (let i = 0; i < offsets.length; i += 2) {
        allocator.freeExact(offsets[i], byteLength, 1);
        live.delete(offsets[i]);
      }
      assertAllLiveIntact(`round ${round} after frees`);
      while (live.size < batch) allocAndMark();
      assertAllLiveIntact(`round ${round} after refill`);
    }
  }

  it('9-byte exact alloc/free/realloc churn never corrupts a live neighbor', () => {
    churnAndAssertNoCorruption(9);
  });

  it('18-byte exact alloc/free/realloc churn never corrupts a live neighbor', () => {
    churnAndAssertNoCorruption(18);
  });

  it('never returns an address that still has a live exact owner', () => {
    const byteLength = 9;
    const live = new Set<number>();
    let allocs = 0;
    let frees = 0;

    for (let i = 0; i < 300; i++) {
      const shouldFree = live.size > 0 && (i % 3 === 0 || live.size > 40);
      if (shouldFree) {
        const victim = [...live][i % live.size];
        allocator.freeExact(victim, byteLength, 1);
        live.delete(victim);
        frees++;
      } else {
        const offset = allocator.allocExact(byteLength, 1);
        expect(offset).toBeGreaterThan(0);
        expect(live.has(offset), `allocator returned live exact offset ${offset}`).toBe(false);
        live.add(offset);
        allocs++;
      }
    }

    expect(allocs).toBeGreaterThan(0);
    expect(frees).toBeGreaterThan(0);
  });
});
