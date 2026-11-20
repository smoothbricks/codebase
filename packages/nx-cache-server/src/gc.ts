/// <reference types="bun" />
// SIEVE eviction over the entry store, with the filesystem as the only state.
//
// An entry is written once (its mtime). readEntry explicitly marks atime past
// mtime even on noatime mounts: `atime > mtime` is the visited bit. Clearing it is
// setting atime back to mtime. Insertion order is mtime, and the hand is one
// mtime cursor persisted beside the store. A sweep walks the ring from the hand:
// a visited entry is cleared and skipped, an unvisited one is evicted, until the
// store is under its cap. Scan resistance is the point — a burst of archives no
// gate ever reads again cannot flush the entries every gate reads, which LRU
// would let it do.
//
// The ring is held in memory between sweeps. What it holds never changes for a published archive: its
// path, its mtime and its length. One listing builds it when the store is opened; after that a
// publication adds its one archive and a sweep reads and writes only the archives it examines, from
// the hand until the store is under its cap. The visited bit is never copied: a sweep reads it from
// the inode it is about to judge. The store lease (`store.ts`) is what makes a held ring true: no
// other process links or unlinks an archive while it is held.
import { lstat, readdir, readFile, rename, rm, utimes } from 'node:fs/promises';
import { join } from 'node:path';
import { logLine, logValue } from './log.js';
import { type CacheStore, describe, entryPath, errnoOf, isCacheKey, openCacheStore } from './store.js';

/** What stays true of an archive for as long as it is published. */
export interface Archive {
  readonly path: string;
  readonly mtimeMs: number;
  readonly size: number;
}

/** An archive as one listing saw it. */
export interface Entry extends Archive {
  readonly atimeMs: number;
}

/** The store's archives in insertion order, their summed length, and the mtime cursor of the last eviction. */
export interface Ring {
  readonly archives: Archive[];
  used: number;
  hand: number;
}

export interface SweepOutcome {
  readonly usedBefore: number;
  readonly usedAfter: number;
  readonly deleted: number;
  readonly cleared: number;
  readonly hand: number;
  /** Archives whose visited bit this sweep read: the whole of its per-archive filesystem work. */
  readonly examined: number;
  /** Examined archives that were already gone from the directory; dropped from the ring, never counted deleted. */
  readonly missing: number;
}

export interface Store {
  list(): Promise<Entry[]>;
  /** The archive's visited bit now, or `undefined` when the archive is gone. */
  visited(archive: Archive): Promise<boolean | undefined>;
  clearVisited(archive: Archive): Promise<void>;
  evict(archive: Archive): Promise<void>;
}

export function insertionOrder<A extends Archive>(entries: readonly A[]): A[] {
  return [...entries].sort((a, b) => a.mtimeMs - b.mtimeMs || (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}

/** The first position in insertion order that is not before `(mtimeMs, path)`. */
function position(archives: readonly Archive[], mtimeMs: number, path: string): number {
  let low = 0;
  let high = archives.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const held = archives[middle];
    if (held !== undefined && (held.mtimeMs < mtimeMs || (held.mtimeMs === mtimeMs && held.path < path))) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
}

/** The one listing: every archive the store holds, in insertion order. */
export async function loadRing(store: Store, hand: number): Promise<Ring> {
  const archives: Archive[] = insertionOrder(await store.list());
  return { archives, used: archives.reduce((sum, e) => sum + e.size, 0), hand };
}

/** Account one published archive at its place in insertion order. */
export function addArchive(ring: Ring, archive: Archive): void {
  ring.archives.splice(position(ring.archives, archive.mtimeMs, archive.path), 0, archive);
  ring.used += archive.size;
}

/**
 * One SIEVE sweep over a held ring. It walks from the hand, wrapping once, until the ring is under the
 * cap: a visited archive is cleared and spared, an unvisited one is evicted. Archives spared by that
 * circuit are evicted in the same order only when a full circuit did not free enough.
 */
export async function sweepRing(
  store: Store,
  ring: Ring,
  capBytes: number,
  protectedPath?: string,
): Promise<SweepOutcome> {
  const { archives } = ring;
  const usedBefore = ring.used;
  let deleted = 0;
  let cleared = 0;
  let examined = 0;
  let missing = 0;
  if (ring.used > capBytes) {
    const spared: Archive[] = [];
    let index = position(archives, ring.hand, '');
    for (let left = archives.length; left > 0 && ring.used > capBytes; left--) {
      if (index >= archives.length) index = 0;
      const archive = archives[index];
      if (archive === undefined) break;
      if (archive.path === protectedPath) {
        index++;
        continue;
      }
      examined++;
      const visited = await store.visited(archive);
      if (visited) {
        await store.clearVisited(archive);
        cleared++;
        spared.push(archive);
        index++;
        continue;
      }
      if (visited === undefined) {
        missing++;
      } else {
        await store.evict(archive);
        deleted++;
        ring.hand = archive.mtimeMs;
      }
      archives.splice(index, 1);
      ring.used -= archive.size;
    }
    for (const archive of spared) {
      if (ring.used <= capBytes) break;
      await store.evict(archive);
      archives.splice(position(archives, archive.mtimeMs, archive.path), 1);
      ring.used -= archive.size;
      deleted++;
      ring.hand = archive.mtimeMs;
    }
  }
  return { usedBefore, usedAfter: ring.used, deleted, cleared, hand: ring.hand, examined, missing };
}

/** One SIEVE sweep of a listed store; `hand` is the mtime cursor from the previous sweep (0 = start). */
export async function sweep(
  store: Store,
  capBytes: number,
  hand: number,
  protectedPath?: string,
): Promise<SweepOutcome> {
  return sweepRing(store, await loadRing(store, hand), capBytes, protectedPath);
}

export function directoryStore(entries: string): Store {
  return {
    async list() {
      const names = await readdir(entries);
      const out: Entry[] = [];
      for (const name of names) {
        if (!isCacheKey(name)) continue;
        const path = join(entries, name);
        const s = await lstat(path);
        if (s.isFile())
          out.push({
            path,
            mtimeMs: s.mtimeMs,
            atimeMs: s.atimeMs,
            size: s.size,
          });
      }
      return out;
    },
    async visited(archive) {
      try {
        const s = await lstat(archive.path);
        return s.atimeMs > s.mtimeMs;
      } catch (cause) {
        if (errnoOf(cause) === 'ENOENT') return undefined;
        throw cause;
      }
    },
    async clearVisited(archive) {
      await utimes(archive.path, archive.mtimeMs / 1000, archive.mtimeMs / 1000);
    },
    async evict(archive) {
      await rm(archive.path, { force: true });
    },
  };
}

export type SweepResult =
  | { readonly ok: true; readonly outcome: SweepOutcome; readonly listed: number }
  | { readonly ok: false; readonly reason: string };

/** A store's retention for as long as its lease is held. */
export interface Retention {
  /** Account the archive just published at `key` and restore the cap; that archive survives the sweep. */
  admit(key: string): Promise<SweepResult>;
}

export type RetentionOpenResult =
  | { readonly ok: true; readonly retention: Retention; readonly opened: SweepOutcome }
  | { readonly ok: false; readonly reason: string };

/**
 * List the store once, sweep it to its cap, and hold its ring for the publications that follow. The
 * caller holds the store lease and serializes `admit`. A sweep that fails discards the held ring, so
 * the next one lists the store again instead of trusting a ring a half-done sweep left. Only ENOENT
 * means an absent hand; an unreadable or corrupt one is a failure.
 */
export async function openRetention(store: CacheStore, capBytes: number): Promise<RetentionOpenResult> {
  const files = directoryStore(store.directory);
  const handFile = join(store.directory, '..', 'sieve.hand');
  const temporary = `${handFile}.tmp`;
  let ring: Ring | undefined;

  async function readHand(): Promise<number> {
    let text: string;
    try {
      text = (await readFile(handFile, 'utf8')).trim();
    } catch (cause) {
      if (errnoOf(cause) === 'ENOENT') return 0;
      throw cause;
    }
    const hand = Number(text);
    if (text === '' || !Number.isFinite(hand) || hand < 0) throw new Error(`Invalid SIEVE cursor in ${handFile}.`);
    return hand;
  }

  async function restore(protectedKey?: string): Promise<SweepResult> {
    try {
      const protectedPath = protectedKey === undefined ? undefined : entryPath(store, protectedKey);
      let listed = 0;
      let held = ring;
      // Discarded until this sweep completes: a failure below leaves no ring to trust.
      ring = undefined;
      const fresh = held === undefined;
      if (held === undefined) {
        held = await loadRing(files, await readHand());
        listed = held.archives.length;
      } else if (protectedPath !== undefined) {
        const s = await lstat(protectedPath);
        addArchive(held, { path: protectedPath, mtimeMs: s.mtimeMs, size: s.size });
      }
      const handBefore = held.hand;
      const outcome = await sweepRing(files, held, capBytes, protectedPath);
      if (fresh || outcome.hand !== handBefore) {
        await Bun.write(temporary, `${outcome.hand}\n`);
        await rename(temporary, handFile);
      }
      ring = held;
      logLine(
        `ev=gc used_before=${outcome.usedBefore} used_after=${outcome.usedAfter} cap=${capBytes} deleted=${outcome.deleted} cleared=${outcome.cleared} hand=${outcome.hand} listed=${listed} examined=${outcome.examined} missing=${outcome.missing}`,
      );
      return { ok: true, outcome, listed };
    } catch (cause) {
      const reason = describe(cause);
      logLine(`ev=gc.failed reason=${logValue(reason)}`);
      return { ok: false, reason };
    }
  }

  const opened = await restore();
  return opened.ok ? { ok: true, retention: { admit: restore }, opened: opened.outcome } : opened;
}

if (import.meta.main) {
  const [entries, rawCap] = Bun.argv.slice(2);
  const cap = Number(rawCap);
  if (entries === undefined || !Number.isSafeInteger(cap) || cap < 1) {
    console.error('usage: gc.ts <entries-dir> <positive-cap-bytes>');
    process.exit(2);
  }
  const opened = await openCacheStore(entries);
  if (!opened.ok) {
    console.error(`nx-cache-gc refused: ${opened.path} is held by a running cache server (EWOULDBLOCK)`);
    process.exit(1);
  }
  const result = await openRetention(opened.store, cap).finally(() => opened.store.close());
  if (!result.ok) {
    console.error(`nx-cache-gc failed: ${result.reason}`);
    process.exit(1);
  }
}
