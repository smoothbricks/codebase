import { afterEach, expect, test } from 'bun:test';
import { chmod, mkdtemp, readdir, readFile, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openRetention, type Retention } from '../gc.js';
import { type CacheStore, entryPath, openCacheStore } from '../store.js';

// specs/cowshed/03_caches.md, "Nx accepted-task artifact service": retention is filesystem SIEVE, each successful
// commit restores `capBytes` before replying, and only opening the store lists it.

const roots: string[] = [];
const stores: CacheStore[] = [];

afterEach(async () => {
  await Promise.all(stores.splice(0).map((store) => store.close()));
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function scratchStore(): Promise<CacheStore> {
  const root = await mkdtemp(join(tmpdir(), 'nx-cache-retention-'));
  roots.push(root);
  const opened = await openCacheStore(join(root, 'entries'));
  if (!opened.ok) throw new Error('scratch store is locked');
  stores.push(opened.store);
  return opened.store;
}

async function retentionOf(store: CacheStore, capBytes: number): Promise<Retention> {
  const opened = await openRetention(store, capBytes);
  if (!opened.ok) throw new Error(opened.reason);
  return opened.retention;
}

/** Publish as the server does: the archive exists under its key, written at `second`, before it is admitted. */
async function archive(store: CacheStore, key: string, size: number, second: number): Promise<void> {
  const path = entryPath(store, key);
  await writeFile(path, new Uint8Array(size));
  await utimes(path, second, second);
}

/** What a GET does to the inode: atime past mtime. */
async function visit(store: CacheStore, key: string, second: number): Promise<void> {
  await utimes(entryPath(store, key), second + 1, second);
}

async function published(store: CacheStore): Promise<string[]> {
  return (await readdir(store.directory)).filter((name) => !name.startsWith('.')).sort();
}

async function hand(store: CacheStore): Promise<string> {
  return (await readFile(join(store.directory, '..', 'sieve.hand'), 'utf8')).trim();
}

test('publications that leave the store under its cap list nothing and examine nothing', async () => {
  const store = await scratchStore();
  for (let n = 0; n < 50; n++) await archive(store, `old${n}`, 100, 1 + n);
  const retention = await retentionOf(store, 100_000);
  for (let n = 0; n < 20; n++) {
    await archive(store, `new${n}`, 100, 100 + n);
    const admitted = await retention.admit(`new${n}`);
    expect(admitted).toMatchObject({
      ok: true,
      listed: 0,
      outcome: { usedBefore: 5100 + 100 * n, usedAfter: 5100 + 100 * n, examined: 0, deleted: 0, cleared: 0 },
    });
  }
});

test('a full store examines only the archives each publication evicts or spares, oldest from the hand first', async () => {
  const store = await scratchStore();
  for (let n = 0; n < 50; n++) await archive(store, `old${n}`, 100, 1 + n);
  // old0 and old2 were read; every other archive was not.
  await visit(store, 'old0', 1);
  await visit(store, 'old2', 3);
  const retention = await retentionOf(store, 5000);

  await archive(store, 'a', 100, 100);
  // The hand starts at old0: spared (cleared); old1 goes, which is enough.
  expect(await retention.admit('a')).toMatchObject({
    ok: true,
    listed: 0,
    outcome: { examined: 2, cleared: 1, deleted: 1 },
  });
  expect(await published(store)).not.toContain('old1');
  expect(await hand(store)).toBe('2000');

  await archive(store, 'b', 250, 101);
  // From the hand: old2 spared, then old3, old4 and old5 free 300 for b's 250.
  expect(await retention.admit('b')).toMatchObject({
    ok: true,
    listed: 0,
    outcome: { examined: 4, cleared: 1, deleted: 3 },
  });
  const kept = await published(store);
  expect(kept).toContain('old0');
  expect(kept).toContain('old2');
  expect(kept).toContain('a');
  expect(kept).toContain('b');
  expect(['old3', 'old4', 'old5'].filter((key) => kept.includes(key))).toEqual([]);
  expect(await hand(store)).toBe('6000');
});

test('a held ring evicts exactly what a store listed again before every publication evicts', async () => {
  // The same history of publications and reads against two stores: one retention held throughout, one reopened
  // (a restart: one listing, the persisted hand) before each publication.
  for (const seed of [1, 7, 42, 1009]) {
    let state = seed;
    const random = (bound: number) => {
      state = (state * 1103515245 + 12345) % 2147483648;
      return state % bound;
    };
    const held = await scratchStore();
    const restarted = await scratchStore();
    const cap = 4000;
    const heldRetention = await retentionOf(held, cap);
    const keys: { key: string; second: number }[] = [];
    for (let step = 0; step < 120; step++) {
      const key = `k${step}`;
      // Seconds repeat, so insertion order is decided by path among archives of one mtime.
      const second = 10 + Math.floor(step / 3);
      const size = 50 + random(900);
      for (let reads = random(4); reads > 0 && keys.length > 0; reads--) {
        const read = keys[random(keys.length)];
        if (read === undefined || !(await published(held)).includes(read.key)) continue;
        await visit(held, read.key, read.second);
        await visit(restarted, read.key, read.second);
      }
      const restartedRetention = await retentionOf(restarted, cap);
      await archive(held, key, size, second);
      await archive(restarted, key, size, second);
      keys.push({ key, second });
      const [a, b] = [await heldRetention.admit(key), await restartedRetention.admit(key)];
      if (!a.ok || !b.ok) throw new Error('admission failed');
      expect(a.listed).toBe(0);
      expect(a.outcome).toEqual(b.outcome);
      expect(a.outcome.usedAfter).toBeLessThanOrEqual(cap);
      expect(await published(held)).toEqual(await published(restarted));
      expect(await published(held)).toContain(key);
      expect(await hand(held)).toBe(await hand(restarted));
    }
  }
});

test('reopening a store rebuilds the ring from one listing and continues from the persisted hand', async () => {
  const store = await scratchStore();
  for (let n = 0; n < 10; n++) await archive(store, `old${n}`, 100, 1 + n);
  const first = await retentionOf(store, 1000);
  await archive(store, 'a', 300, 100);
  expect(await first.admit('a')).toMatchObject({ ok: true, outcome: { deleted: 3, hand: 3000 } });

  const reopened = await openRetention(store, 1000);
  expect(reopened).toMatchObject({ ok: true, opened: { usedBefore: 1000, usedAfter: 1000, deleted: 0, hand: 3000 } });
  if (!reopened.ok) throw new Error(reopened.reason);
  await archive(store, 'b', 100, 101);
  expect(await reopened.retention.admit('b')).toMatchObject({
    ok: true,
    listed: 0,
    outcome: { deleted: 1, hand: 4000 },
  });
  expect(await published(store)).not.toContain('old3');
});

test('a sweep that fails is reported, and the next publication lists the store again and restores the cap', async () => {
  const store = await scratchStore();
  for (let n = 0; n < 10; n++) await archive(store, `old${n}`, 100, 1 + n);
  const retention = await retentionOf(store, 1000);
  await archive(store, 'a', 100, 100);
  // The directory refuses the unlink an eviction needs.
  await chmod(store.directory, 0o555);
  const failed = await retention.admit('a').finally(() => chmod(store.directory, 0o755));
  expect(failed.ok).toBe(false);
  expect(await published(store)).toHaveLength(11);

  await archive(store, 'b', 100, 101);
  expect(await retention.admit('b')).toMatchObject({
    ok: true,
    listed: 12,
    outcome: { usedBefore: 1200, usedAfter: 1000, deleted: 2 },
  });
  const kept = await published(store);
  expect(kept).toContain('b');
  expect(kept).toHaveLength(10);
});

test('an archive removed behind the ring is dropped from the count, never counted as an eviction', async () => {
  const store = await scratchStore();
  for (let n = 0; n < 10; n++) await archive(store, `old${n}`, 100, 1 + n);
  const retention = await retentionOf(store, 1000);
  await rm(entryPath(store, 'old0'));
  await archive(store, 'a', 100, 100);
  expect(await retention.admit('a')).toMatchObject({
    ok: true,
    listed: 0,
    outcome: { usedBefore: 1100, usedAfter: 1000, examined: 1, missing: 1, deleted: 0 },
  });
  expect(await published(store)).toHaveLength(10);
});
