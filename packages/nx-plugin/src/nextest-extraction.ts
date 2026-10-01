import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createReadStream } from 'node:fs';
import { mkdir, mkdtemp, open, readdir, readFile, rename, rm, stat, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

/**
 * The helper a per-crate runner calls before `nextest run`, as the root
 * `node_modules` links it. Spelled through the package link rather than this
 * file's real location: under an isolated install that location is a store
 * path outside the workspace, which would differ per machine in a command Nx
 * hashes.
 */
export const NEXTEST_EXTRACT_BIN = 'node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-nextest-extract.js';

/**
 * Where nextest puts things inside an extracted archive, relative to the
 * extraction directory (nextest-runner's `reuse_build` constants). A run given
 * these three paths reuses that build exactly as `--archive-file` would after
 * unpacking it.
 */
export const EXTRACTED_TARGET_DIR = 'target';
export const EXTRACTED_BINARIES_METADATA = 'target/nextest/binaries-metadata.json';
export const EXTRACTED_CARGO_METADATA = 'target/nextest/cargo-metadata.json';

/** Unpacks `archive` into the empty, existing directory `destination`. */
export type NextestArchiveExtractor = (archive: string, destination: string) => Promise<void>;

export type NextestExtraction =
  | { kind: 'reused'; key: string; directory: string }
  | { kind: 'extracted'; key: string; directory: string; elapsedMs: number }
  | { kind: 'awaited'; key: string; directory: string; elapsedMs: number; holder: string };

/** What a caller is told while it does not hold the extraction itself. */
export type NextestExtractionEvent =
  | { kind: 'waiting'; holder: string }
  | { kind: 'stale-lock'; holder: string; ageMs: number };

/**
 * How a lock proves its holder is alive: it rewrites the lock's mtime every
 * `heartbeatMs`, and a lock older than `staleAfterMs` belongs to nobody. A pid
 * cannot answer that — a holder SIGKILLed at its bound leaves a pid the system
 * may already have handed to something else.
 */
export interface NextestExtractionTiming {
  heartbeatMs: number;
  staleAfterMs: number;
  pollMs: number;
}

const DEFAULT_TIMING: NextestExtractionTiming = { heartbeatMs: 1_000, staleAfterMs: 10_000, pollMs: 100 };

/** The SHA-256 of the archive's bytes: the only identity an extraction is ever looked up by. */
export async function nextestArchiveKey(archive: string): Promise<string> {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(archive, { highWaterMark: 1 << 20 })) {
    hash.update(chunk);
  }
  return hash.digest('hex');
}

/**
 * One archive path's extractions live beside it, each under the hash of the
 * bytes it came from; the lock and staging directories of a key carry that
 * key as their prefix, so cleanup can tell them from another key's.
 */
export interface NextestExtractionPaths {
  root: string;
  directory: string;
  lock: string;
  stagingPrefix: string;
}

export function nextestExtractionPaths(archive: string, key: string): NextestExtractionPaths {
  const root = `${archive}.extracted`;
  return {
    root,
    directory: join(root, key),
    lock: join(root, `${key}.lock`),
    stagingPrefix: join(root, `${key}.staging-`),
  };
}

/**
 * The directory holding `archive`'s extraction, extracting it first if no
 * caller has yet.
 *
 * Correctness does not rest on the lock. An extraction lands under the hash of
 * the bytes it came from by renaming a private staging directory into place,
 * so a directory under a key is a complete extraction of exactly those bytes
 * or it does not exist: concurrent extractors cannot corrupt it, the loser of
 * a race discards its own copy, and an archive rewritten mid-extraction is
 * caught by hashing it again before publishing. The lock only stops every
 * runner of one Nx run from unpacking the same bytes side by side, which is the
 * cost it exists to remove.
 *
 * Publishing a key removes every other key's extraction of this archive path,
 * including stale locks and staging left by a holder that died. Leftovers of
 * the CURRENT key are not touched: a holder that took over a stale lock may
 * still be extracting into one.
 */
export async function ensureNextestArchiveExtracted(
  archive: string,
  extract: NextestArchiveExtractor,
  onEvent: (event: NextestExtractionEvent) => void,
  timing: NextestExtractionTiming = DEFAULT_TIMING,
): Promise<NextestExtraction> {
  const started = performance.now();
  const key = await nextestArchiveKey(archive);
  const paths = nextestExtractionPaths(archive, key);
  if (await isDirectory(paths.directory)) {
    return { kind: 'reused', key, directory: paths.directory };
  }
  await mkdir(paths.root, { recursive: true });
  let holder: string | undefined;
  const settled = (): NextestExtraction =>
    holder === undefined
      ? { kind: 'reused', key, directory: paths.directory }
      : { kind: 'awaited', key, directory: paths.directory, elapsedMs: performance.now() - started, holder };
  for (;;) {
    const lock = await acquireLock(paths.lock, timing.heartbeatMs);
    if (lock !== undefined) {
      try {
        if (await isDirectory(paths.directory)) {
          return settled();
        }
        const published = await extractAndPublish(archive, key, paths, extract);
        if (published) {
          await removeOtherKeys(paths.root, key);
          return { kind: 'extracted', key, directory: paths.directory, elapsedMs: performance.now() - started };
        }
        return settled();
      } finally {
        await lock.release();
      }
    }
    if (await isDirectory(paths.directory)) {
      return settled();
    }
    const current = await readLock(paths.lock);
    if (current === undefined) {
      // Released between our attempt and this read: try again at once.
      continue;
    }
    if (current.ageMs > timing.staleAfterMs) {
      onEvent({ kind: 'stale-lock', holder: current.holder, ageMs: current.ageMs });
      await rm(paths.lock, { force: true });
      continue;
    }
    if (holder === undefined) {
      holder = current.holder;
      onEvent({ kind: 'waiting', holder });
    }
    await sleep(timing.pollMs);
  }
}

/**
 * Extract into a private staging directory and rename it to `paths.directory`.
 * False when another extractor published the same key first, which leaves a
 * complete extraction of the same bytes in place.
 */
async function extractAndPublish(
  archive: string,
  key: string,
  paths: NextestExtractionPaths,
  extract: NextestArchiveExtractor,
): Promise<boolean> {
  const staging = await mkdtemp(paths.stagingPrefix);
  try {
    await extract(archive, staging);
    for (const file of [EXTRACTED_BINARIES_METADATA, EXTRACTED_CARGO_METADATA]) {
      if (!(await statOrUndefined(join(staging, file)))?.isFile()) {
        throw new Error(`extracting ${archive} produced no ${file}; a reuse run cannot read this layout`);
      }
    }
    const after = await nextestArchiveKey(archive);
    if (after !== key) {
      throw new Error(
        `${archive} changed while it was being extracted (sha256 ${key} before, ${after} after); ` +
          'rerun once whatever is rewriting it has finished',
      );
    }
    await rename(staging, paths.directory);
    return true;
  } catch (error) {
    await rm(staging, { recursive: true, force: true });
    const code = errorCode(error);
    if ((code === 'ENOTEMPTY' || code === 'EEXIST') && (await isDirectory(paths.directory))) {
      return false;
    }
    throw error;
  }
}

async function removeOtherKeys(root: string, key: string): Promise<void> {
  for (const entry of await readdir(root)) {
    if (entry !== key && !entry.startsWith(`${key}.`)) {
      await rm(join(root, entry), { recursive: true, force: true });
    }
  }
}

interface HeldLock {
  release(): Promise<void>;
}

/** Create the lock exclusively, or report it held. A held lock is refreshed until released. */
async function acquireLock(path: string, heartbeatMs: number): Promise<HeldLock | undefined> {
  const token = `${process.pid} ${randomUUID()}`;
  try {
    await writeFile(path, `${token}\n`, { flag: 'wx' });
  } catch (error) {
    if (errorCode(error) === 'EEXIST') {
      return undefined;
    }
    throw error;
  }
  const heartbeat = setInterval(() => {
    const now = new Date();
    // ENOENT: a waiter judged this lock stale and removed it. The extraction
    // carries on; publishing is atomic whoever else extracts beside it.
    utimes(path, now, now).catch((error: unknown) => {
      if (errorCode(error) !== 'ENOENT') throw error;
    });
  }, heartbeatMs);
  return {
    async release() {
      clearInterval(heartbeat);
      // Remove it only while it is still ours: a waiter that took it over owns it now.
      const content = await readFile(path, 'utf8').catch((error: unknown) => {
        if (errorCode(error) === 'ENOENT') return undefined;
        throw error;
      });
      if (content?.trim() === token) {
        await rm(path, { force: true });
      }
    },
  };
}

async function readLock(path: string): Promise<{ holder: string; ageMs: number } | undefined> {
  try {
    const handle = await open(path, 'r');
    try {
      const [content, metadata] = await Promise.all([handle.readFile('utf8'), handle.stat()]);
      const pid = content.trim().split(' ')[0];
      return {
        // Empty while its creator is between creating and writing it.
        holder: pid === undefined || pid.length === 0 ? 'a process that has not yet named itself' : `pid ${pid}`,
        ageMs: Date.now() - metadata.mtimeMs,
      };
    } finally {
      await handle.close();
    }
  } catch (error) {
    if (errorCode(error) === 'ENOENT') {
      return undefined;
    }
    throw error;
  }
}

/**
 * nextest's own extractor: `nextest list --list-type binaries-only` unpacks the
 * archive and executes nothing from it.
 *
 * `list` loads nextest configuration after extracting, and a repository's
 * `.config/nextest.toml` may name test groups only the plugin's tool config
 * defines, so it is given an empty config of its own instead: unpacking bytes
 * does not depend on how tests are scheduled. `--workspace-remap .` because
 * nextest insists the workspace root exist, and the archive's own is the
 * producing tree's path.
 */
export const extractWithNextest: NextestArchiveExtractor = async (archive, destination) => {
  const config = join(destination, 'nextest-extract.toml');
  await writeFile(config, '');
  try {
    const args = [
      '--frozen',
      'nextest',
      'list',
      '--archive-file',
      archive,
      '--extract-to',
      destination,
      '--workspace-remap',
      '.',
      '--list-type',
      'binaries-only',
      '--user-config-file',
      'none',
      '--config-file',
      config,
    ];
    // The binary list on stdout is not wanted; nextest's progress and errors are on stderr.
    const child = spawn('cargo', args, { stdio: ['ignore', 'ignore', 'inherit'] });
    // Rejects with the spawn error when cargo cannot be started at all.
    const [code, signal]: unknown[] = await once(child, 'exit');
    if (code !== 0) {
      throw new Error(
        `cargo ${args.join(' ')} ${code === null ? `died of ${String(signal)}` : `exited with status ${String(code)}`}`,
      );
    }
  } finally {
    await rm(config, { force: true });
  }
};

async function isDirectory(path: string): Promise<boolean> {
  return (await statOrUndefined(path))?.isDirectory() ?? false;
}

async function statOrUndefined(path: string) {
  try {
    return await stat(path);
  } catch (error) {
    if (errorCode(error) === 'ENOENT') {
      return undefined;
    }
    throw error;
  }
}

function errorCode(error: unknown): unknown {
  return error instanceof Error && 'code' in error ? error.code : undefined;
}
