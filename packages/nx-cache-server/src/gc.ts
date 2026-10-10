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
import { lstat, readdir, readFile, rename, rm, utimes } from 'node:fs/promises';
import { join } from 'node:path';
import { logLine, logValue } from './log.js';
import { type CacheStore, describe, entryPath, errnoOf, isCacheKey, openCacheStore } from './store.js';

export interface Entry {
  readonly path: string;
  readonly mtimeMs: number;
  readonly atimeMs: number;
  readonly size: number;
}

export interface SweepOutcome {
  readonly usedBefore: number;
  readonly usedAfter: number;
  readonly deleted: number;
  readonly cleared: number;
  readonly hand: number;
}

export interface Store {
  list(): Promise<Entry[]>;
  clearVisited(entry: Entry): Promise<void>;
  evict(entry: Entry): Promise<void>;
}

export function insertionOrder(entries: readonly Entry[]): Entry[] {
  return [...entries].sort((a, b) => a.mtimeMs - b.mtimeMs || (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}

/** One SIEVE sweep; `hand` is the mtime cursor from the previous sweep (0 = start). */
export async function sweep(
  store: Store,
  capBytes: number,
  hand: number,
  protectedPath?: string,
): Promise<SweepOutcome> {
  const ring = insertionOrder(await store.list());
  const usedBefore = ring.reduce((sum, e) => sum + e.size, 0);
  let used = usedBefore;
  let deleted = 0;
  let cleared = 0;
  let cursor = hand;
  if (used > capBytes && ring.length > 0) {
    // Walk from the hand, wrapping once; a full circuit clears every visited bit.
    const start = ring.findIndex((e) => e.mtimeMs >= hand);
    const order = start < 0 ? ring : [...ring.slice(start), ...ring.slice(0, start)];
    for (let round = 0; round < 2 && used > capBytes; round++) {
      for (const entry of order) {
        if (used <= capBytes) break;
        if (entry.path === protectedPath) continue;
        const visited = entry.atimeMs > entry.mtimeMs;
        // The second circuit visits only entries spared by the first, never already-deleted entries.
        if (round === 1 && !visited) continue;
        if (round === 0 && visited) {
          await store.clearVisited(entry);
          cleared++;
        } else {
          await store.evict(entry);
          used -= entry.size;
          deleted++;
          cursor = entry.mtimeMs;
        }
      }
    }
  }
  return { usedBefore, usedAfter: used, deleted, cleared, hand: cursor };
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
    async clearVisited(entry) {
      await utimes(entry.path, entry.mtimeMs / 1000, entry.mtimeMs / 1000);
    },
    async evict(entry) {
      await rm(entry.path, { force: true });
    },
  };
}

export type SweepResult =
  | { readonly ok: true; readonly outcome: SweepOutcome }
  | { readonly ok: false; readonly reason: string };

/**
 * Caller holds the store lease and serializes sweeps; only ENOENT means an absent hand, not
 * unreadable/corrupt state. `protectedKey` names an archive that must survive this sweep.
 */
export async function sweepDirectory(store: CacheStore, capBytes: number, protectedKey?: string): Promise<SweepResult> {
  const entries = store.directory;
  const handFile = join(entries, '..', 'sieve.hand');
  const temporary = `${handFile}.tmp`;
  try {
    let hand = 0;
    try {
      const text = (await readFile(handFile, 'utf8')).trim();
      hand = Number(text);
      if (text === '' || !Number.isFinite(hand) || hand < 0) {
        return { ok: false, reason: `Invalid SIEVE cursor in ${handFile}.` };
      }
    } catch (cause) {
      if (errnoOf(cause) !== 'ENOENT') return { ok: false, reason: describe(cause) };
    }
    const protectedPath = protectedKey === undefined ? undefined : entryPath(store, protectedKey);
    const outcome = await sweep(directoryStore(entries), capBytes, hand, protectedPath);
    await Bun.write(temporary, `${outcome.hand}\n`);
    await rename(temporary, handFile);
    logLine(
      `ev=gc used_before=${outcome.usedBefore} used_after=${outcome.usedAfter} cap=${capBytes} deleted=${outcome.deleted} cleared=${outcome.cleared} hand=${outcome.hand}`,
    );
    return { ok: true, outcome };
  } catch (cause) {
    const reason = describe(cause);
    logLine(`ev=gc.failed reason=${logValue(reason)}`);
    return { ok: false, reason };
  }
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
  const result = await sweepDirectory(opened.store, cap).finally(() => opened.store.close());
  if (!result.ok) {
    console.error(`nx-cache-gc failed: ${result.reason}`);
    process.exit(1);
  }
}
