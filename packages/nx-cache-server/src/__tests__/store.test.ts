import { expect, test } from 'bun:test';
import { mkdtemp, readFile, rm, stat, unlink, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type CacheStore, openCacheStore, publishEntry, readEntry, stageEntry } from '../store.js';

test('an opened archive remains whole when eviction unlinks its name', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-read-'));
  let store: CacheStore | undefined;
  try {
    const result = await openCacheStore(join(root, 'entries'));
    if (!result.ok) throw new Error('Store was locked.');
    store = result.store;
    const body = new Uint8Array(1024 * 1024).fill(27);
    await writeFile(join(store.directory, 'pinned'), body);
    const entry = await readEntry(store, 'pinned');
    expect(entry.kind).toBe('found');
    if (entry.kind !== 'found') throw new Error('Archive was not opened.');
    await unlink(join(store.directory, 'pinned'));
    expect(new Uint8Array(await new Response(entry.body).arrayBuffer())).toEqual(body);
    expect((await readEntry(store, 'pinned')).kind).toBe('missing');
  } finally {
    await store?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('GET explicitly sets the filesystem visited bit after it has been cleared', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-visited-'));
  let store: CacheStore | undefined;
  try {
    const result = await openCacheStore(join(root, 'entries'));
    if (!result.ok) throw new Error('Store was locked.');
    store = result.store;
    const path = join(store.directory, 'visited');
    await writeFile(path, 'whole archive');
    await utimes(path, 100, 100);
    const entry = await readEntry(store, 'visited');
    expect(entry.kind).toBe('found');
    if (entry.kind !== 'found') throw new Error('Archive was not opened.');
    await new Response(entry.body).arrayBuffer();
    const observed = await stat(path);
    expect(observed.mtimeMs).toBe(100000);
    expect(observed.atimeMs).toBeGreaterThan(observed.mtimeMs);
  } finally {
    await store?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('a complete stage is invisible until atomic publication', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-stage-'));
  let store: CacheStore | undefined;
  try {
    const result = await openCacheStore(join(root, 'entries'));
    if (!result.ok) throw new Error('Store was locked.');
    store = result.store;
    const staged = await stageEntry(store, 'complete', new Blob(['whole archive']).stream(), 13);
    expect(staged).toMatchObject({ kind: 'ready', key: 'complete', bytes: 13 });
    if (staged.kind !== 'ready') throw new Error('Archive was not staged.');
    expect((await readEntry(store, 'complete')).kind).toBe('missing');
    expect(await publishEntry(store, staged)).toEqual({
      kind: 'stored',
      bytes: 13,
      stagingRetained: false,
    });
    const opened = await readEntry(store, 'complete');
    if (opened.kind !== 'found') throw new Error('Archive was not published.');
    expect(await new Response(opened.body).text()).toBe('whole archive');
  } finally {
    await store?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('a second open of a live store is refused without reclaiming its staging', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-lease-'));
  const directory = join(root, 'entries');
  try {
    const first = await openCacheStore(directory);
    if (!first.ok) throw new Error('First open was refused.');
    try {
      const inProgress = join(first.store.staging, 'in-progress.1');
      await writeFile(inProgress, 'partial');
      const second = await openCacheStore(directory);
      expect(second).toMatchObject({ ok: false, kind: 'store-locked', path: join(directory, '.owner.lock') });
      expect(await readFile(inProgress, 'utf8')).toBe('partial');
    } finally {
      await first.store.close();
    }
    const reopened = await openCacheStore(directory);
    if (!reopened.ok) throw new Error('Store stayed locked after close.');
    expect(reopened.store.reclaimedStagingFiles).toBe(1);
    await reopened.store.close();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
