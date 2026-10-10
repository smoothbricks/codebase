/// <reference types="bun" />

/**
 * The cache directory: one file per key, and a write that cannot replace one.
 *
 * `link(2)` is the whole design. A staged upload becomes an entry by hard-linking it to
 * `<directory>/<key>`, which the kernel either performs or refuses with `EEXIST` — atomically, with no
 * window in which a reader sees a partial file and no window in which two writers of the same key both
 * believe they won. `rename(2)` would be wrong here: it replaces silently, so the loser of a race
 * would overwrite the winner's artifact and both would be told they stored it.
 *
 * Staging lives at `<directory>/.staging` for two reasons: it is on the same filesystem, which
 * `link(2)` requires, and the leading dot puts it outside the cache-key charset, so no request can
 * name it. Opening the store reclaims whatever a crash left there — a nonzero count in the startup log
 * is how an operator learns a previous process died mid-upload.
 *
 * One process owns one directory at a time. `openCacheStore` takes a persistent kernel lock on
 * `<directory>/.owner.lock` before it touches staging, so a second open is refused with `store-locked`
 * and reclaims nothing until the first store is closed or its process dies.
 */

import { link, mkdir, open, readdir, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { logLine, logValue } from './log.js';
import { tryAcquireRootLease } from './root-lease.js';

const STAGING = '.staging';

/**
 * The cache-key charset. nx sends a task hash and nothing else — twenty decimal digits today
 * (`.nx/cache/10018087211778381691`) — and this accepts any hex, decimal or base64url rendering of
 * one. What it rejects is what makes `join(directory, key)` safe by construction: no `/`, no `\`, and
 * no `.`, so no key can name a parent directory, an absolute path, or the staging directory.
 */
const CACHE_KEY = /^[A-Za-z0-9_-]{1,128}$/;

export interface CacheStore {
  readonly directory: string;
  readonly staging: string;
  /** Staged uploads a previous process left behind, removed while opening. */
  readonly reclaimedStagingFiles: number;
  /** Releases the root lease. Call only after in-flight requests and publication have drained. */
  close(): Promise<void>;
}

export type CacheStoreOpenResult =
  | { readonly ok: true; readonly store: CacheStore }
  | { readonly ok: false; readonly kind: 'store-locked'; readonly path: string };

/** A whole, verified staged upload; only publication makes it visible to GET. */
export interface CompleteUpload {
  readonly kind: 'ready';
  readonly key: string;
  readonly stagedPath: string;
  readonly bytes: number;
}

export type StageOutcome =
  | CompleteUpload
  | { readonly kind: 'incomplete'; readonly bytes: number; readonly stagingRetained: boolean }
  | { readonly kind: 'failed'; readonly reason: string; readonly stagingRetained: boolean };

export type StoreOutcome =
  | { readonly kind: 'stored'; readonly bytes: number; readonly stagingRetained: boolean }
  | { readonly kind: 'exists'; readonly stagingRetained: boolean }
  | { readonly kind: 'failed'; readonly reason: string; readonly stagingRetained: boolean };

/** The common conflict outcome carries no per-request data. */
const ALREADY_STORED: StoreOutcome = { kind: 'exists', stagingRetained: false };

export function isCacheKey(candidate: string): boolean {
  return CACHE_KEY.test(candidate);
}

export function entryPath(store: CacheStore, key: string): string {
  return join(store.directory, key);
}

export async function openCacheStore(directory: string): Promise<CacheStoreOpenResult> {
  await mkdir(directory, { recursive: true });
  const lockPath = join(directory, '.owner.lock');
  const lease = await tryAcquireRootLease(lockPath);
  if (!lease) return { ok: false, kind: 'store-locked', path: lockPath };
  try {
    const staging = join(directory, STAGING);
    await mkdir(staging, { recursive: true });
    const abandoned = await readdir(staging);
    for (const name of abandoned) {
      await unlink(join(staging, name));
    }
    return {
      ok: true,
      store: { directory, staging, reclaimedStagingFiles: abandoned.length, close: () => lease.close() },
    };
  } catch (failure) {
    await lease.close();
    throw failure;
  }
}

export type ReadOutcome =
  | { readonly kind: 'found'; readonly bytes: number; readonly body: ReadableStream<Uint8Array> }
  | { readonly kind: 'missing' }
  | { readonly kind: 'failed'; readonly reason: string };

const MISSING: ReadOutcome = { kind: 'missing' };

/** Pin an opened inode before answering: a concurrent sweep may unlink its path, not truncate GET. */
export async function readEntry(store: CacheStore, key: string): Promise<ReadOutcome> {
  try {
    const file = await open(entryPath(store, key), 'r');
    try {
      const stats = await file.stat();
      if (!stats.isFile() || stats.size === 0) {
        await file.close();
        return MISSING;
      }
      // APFS can leave atime unchanged on a real read. Mark SIEVE's visited bit explicitly on the
      // pinned inode, and only when it is clear; the original modification time remains the cursor.
      if (stats.atimeMs <= stats.mtimeMs) {
        await file.utimes((stats.mtimeMs + 1) / 1000, stats.mtimeMs / 1000);
      }
      return {
        kind: 'found',
        bytes: stats.size,
        body: Readable.toWeb(file.createReadStream({ autoClose: true })),
      };
    } catch (cause) {
      await file.close();
      return { kind: 'failed', reason: describe(cause) };
    }
  } catch (cause) {
    return errnoOf(cause) === 'ENOENT' ? MISSING : { kind: 'failed', reason: describe(cause) };
  }
}

/**
 * Stage the body and verify its length. Publication is a separate atomic operation.
 *
 * WHY the length is checked before the link and not after: an entry is permanent, so publishing a
 * truncated one poisons that task hash forever — every later build fetches an artifact nx cannot
 * extract, and never-replace means nobody can correct it. A body that ends short of `declaredBytes`
 * is therefore discarded while it is still only a staged file.
 */
export async function stageEntry(
  store: CacheStore,
  key: string,
  body: ReadableStream<Uint8Array> | null,
  declaredBytes: number,
): Promise<StageOutcome> {
  const staged = join(store.staging, `${key}.${crypto.randomUUID()}`);
  let bytes: number;
  try {
    // `Response` is the streaming form of `Bun.write`: the body reaches the disk in chunks instead of
    // being buffered whole in this process.
    bytes = await Bun.write(staged, new Response(body));
  } catch (cause) {
    const stagingRetained = !(await discard(staged));
    return { kind: 'failed', reason: describe(cause), stagingRetained };
  }

  if (bytes !== declaredBytes) {
    const stagingRetained = !(await discard(staged));
    return { kind: 'incomplete', bytes, stagingRetained };
  }

  return { kind: 'ready', key, stagedPath: staged, bytes };
}

/** Publish by never-replace hard link. The server serializes this operation with its SIEVE sweep. */
export async function publishEntry(store: CacheStore, upload: CompleteUpload): Promise<StoreOutcome> {
  try {
    await link(upload.stagedPath, entryPath(store, upload.key));
  } catch (cause) {
    const stagingRetained = !(await discard(upload.stagedPath));
    if (errnoOf(cause) === 'EEXIST') {
      return stagingRetained ? { kind: 'exists', stagingRetained } : ALREADY_STORED;
    }
    return { kind: 'failed', reason: describe(cause), stagingRetained };
  }
  const stagingRetained = !(await discard(upload.stagedPath));
  return { kind: 'stored', bytes: upload.bytes, stagingRetained };
}

/**
 * Drop a staged upload once it has been published or abandoned. A staging file that outlives its
 * request is leaked disk, so a failure to remove one is reported rather than dropped; `ENOENT` is not
 * a failure, it is the file already being gone.
 */
async function discard(staged: string): Promise<boolean> {
  try {
    await unlink(staged);
    return true;
  } catch (cause) {
    if (errnoOf(cause) === 'ENOENT') return true;
    logLine(`ev=staging.leaked path=${logValue(staged)} reason=${logValue(describe(cause))}`);
    return false;
  }
}

/** Read an errno off a thrown value without asserting its type. */
export function errnoOf(cause: unknown): string | undefined {
  if (typeof cause !== 'object' || cause === null || !('code' in cause)) return undefined;
  const { code } = cause;
  return typeof code === 'string' ? code : undefined;
}

export function describe(cause: unknown): string {
  if (cause instanceof Error) {
    const errno = errnoOf(cause);
    return errno === undefined ? cause.message : `${errno}: ${cause.message}`;
  }
  return String(cause);
}
