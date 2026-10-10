import { describe, expect, it } from 'bun:test';
import { type Entry, type Store, sweep } from '../gc.js';

function memoryStore(initial: readonly Entry[]): Store & { readonly entries: Entry[] } {
  const entries = [...initial];
  return {
    entries,
    async list() {
      return [...entries];
    },
    async clearVisited(entry) {
      const i = entries.findIndex((e) => e.path === entry.path);
      entries[i] = { ...entry, atimeMs: entry.mtimeMs };
    },
    async evict(entry) {
      entries.splice(
        entries.findIndex((e) => e.path === entry.path),
        1,
      );
    },
  };
}

const entry = (path: string, mtimeMs: number, read: boolean, size = 100): Entry => ({
  path,
  mtimeMs,
  atimeMs: read ? mtimeMs + 1 : mtimeMs,
  size,
});

describe('SIEVE sweep', () => {
  it('does nothing under the cap', async () => {
    const store = memoryStore([entry('a', 1, false), entry('b', 2, true)]);
    const out = await sweep(store, 1000, 0);
    expect(out).toMatchObject({
      deleted: 0,
      cleared: 0,
      usedAfter: 200,
      hand: 0,
    });
    expect(store.entries.map((e) => e.path)).toEqual(['a', 'b']);
  });

  it('evicts the oldest unvisited entries first and spares visited ones by clearing them', async () => {
    // a: old, never read; b: old, read; c: newer, never read; d: newest, read
    const store = memoryStore([entry('a', 1, false), entry('b', 2, true), entry('c', 3, false), entry('d', 4, true)]);
    // 400 over a 250 cap: a (unvisited) goes, b (visited) is cleared and kept,
    // c (unvisited) goes, which lands at 200; d is never reached.
    const out = await sweep(store, 250, 0);
    expect(store.entries.map((e) => e.path)).toEqual(['b', 'd']);
    expect(out).toMatchObject({
      deleted: 2,
      cleared: 1,
      usedAfter: 200,
      hand: 3,
    });
  });

  it('a read entry survives one sweep under pressure, and a second sweep evicts it if still unread', async () => {
    const store = memoryStore([entry('a', 1, true), entry('b', 2, false), entry('c', 3, false)]);
    const first = await sweep(store, 200, 0);
    // a was visited: cleared and skipped; b evicted, which reaches the cap; c untouched
    expect(store.entries.map((e) => e.path)).toEqual(['a', 'c']);
    expect(first).toMatchObject({ deleted: 1, cleared: 1, hand: 2 });
    // nothing read a since. From the hand the ring is c then a: both unvisited now, both go.
    const second = await sweep(store, 50, first.hand);
    expect(store.entries).toEqual([]);
    expect(second.deleted).toBe(2);
  });

  it('a hand past every entry wraps to the start', async () => {
    const store = memoryStore([entry('a', 1, false), entry('b', 2, false)]);
    const out = await sweep(store, 100, 999);
    expect(store.entries.map((e) => e.path)).toEqual(['b']);
    expect(out.hand).toBe(1);
  });

  it('clears every visited bit rather than looping forever when all entries are visited', async () => {
    const store = memoryStore([entry('a', 1, true), entry('b', 2, true)]);
    const out = await sweep(store, 100, 0);
    expect(out.cleared).toBe(2);
    expect(out.deleted).toBe(1);
    expect(store.entries).toHaveLength(1);
  });
});

it('never counts a previously evicted entry twice on the second circuit', async () => {
  const store = memoryStore([entry('a', 1, false), entry('b', 2, true)]);
  const out = await sweep(store, 1, 0);
  expect(out).toMatchObject({ usedAfter: 0, deleted: 2, cleared: 1 });
  expect(store.entries).toEqual([]);
});

it('never evicts or clears the protected just-published archive', async () => {
  const store = memoryStore([entry('a', 1, true), entry('b', 2, true), entry('c', 3, true), entry('fresh', 4, false)]);
  const out = await sweep(store, 300, 0, 'fresh');
  expect(store.entries.map((e) => e.path)).toContain('fresh');
  expect(store.entries.find((e) => e.path === 'fresh')).toEqual(entry('fresh', 4, false));
  expect(store.entries).toHaveLength(3);
  expect(out).toMatchObject({ usedBefore: 400, usedAfter: 300, deleted: 1, cleared: 3 });
});
