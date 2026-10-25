import { type ChildProcess, spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, openSync, writeSync } from 'node:fs';
import { mkdir, mkdtemp, open, readdir, readFile, rename, rm, stat, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

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

/**
 * The file beside an archive that names it: the hash of the Nx task that produced the archive, which that task writes
 * there (`printf '%s\n' "$NX_TASK_HASH"`) and Nx restores with the archive as one more output.
 */
export const NEXTEST_ARCHIVE_KEY_SUFFIX = '.key';

/**
 * The only identity an extraction is ever looked up by: the task hash in `<archive>.key`. Equal task hashes mean equal
 * inputs, so one extraction serves every archive that hash names, restored or rebuilt, and nothing reads the archive's
 * bytes to tell. An archive with no key was not made by the task, and is refused rather than trusted.
 */
export async function nextestArchiveKey(archive: string): Promise<string> {
  const file = `${archive}${NEXTEST_ARCHIVE_KEY_SUFFIX}`;
  const text = await readFile(file, 'utf8').catch((error: unknown) => {
    if (errorCode(error) === 'ENOENT') {
      throw new Error(
        `${archive} has no ${file}, so no Nx task names it; build the archive through its cargo-test-archive target`,
      );
    }
    throw error;
  });
  const key = text.trim();
  if (!/^\w+$/.test(key)) {
    throw new Error(`${file} names no task hash: it holds ${JSON.stringify(text)}`);
  }
  return key;
}

/**
 * An archive's key and the state of its file: a rewrite under the same key, an Nx rerun of the same inputs or a
 * restore racing an extraction, changes the file's identity, size or modification time.
 */
interface ArchiveIdentity {
  readonly key: string;
  readonly file: string;
}

async function archiveIdentity(archive: string): Promise<ArchiveIdentity> {
  const key = await nextestArchiveKey(archive);
  const file = await stat(archive);
  return { key, file: `device ${file.dev} inode ${file.ino}, ${file.size} bytes, modified ${file.mtimeMs}` };
}

/**
 * The name an archive's extraction is published under: its task hash, and a digest of its file's device, inode, size
 * and modification time. The task hash names the inputs the binaries came from; the file part names the run of that
 * task that wrote them, because Nx runs a task again under the same hash on a cache miss and `nextest archive` is not
 * bit-stable, so an extraction of one run must never serve another. Every runner of one Nx run reads one file, so they
 * share one extraction.
 */
export async function nextestExtractionKey(archive: string): Promise<string> {
  return extractionKey(await archiveIdentity(archive));
}

function extractionKey(identity: ArchiveIdentity): string {
  return `${identity.key}-${createHash('sha256').update(identity.file).digest('hex').slice(0, 16)}`;
}

/**
 * The directory, inside the Cargo target directory, that holds a workspace's
 * archive extractions. An extraction is task-keyed per-checkout tool state
 * of the same kind as the rest of the target directory: rebuilt from an Nx
 * output, reused by the next run, never an Nx output itself. It therefore lives
 * wherever Cargo's build tree lives, which a checkout that keeps its build state
 * on its own volume already keeps off its source tree.
 */
export const NEXTEST_EXTRACTION_DIRECTORY = 'nextest-extracted';

/**
 * A workspace's extractions live under one root, each under the key of the
 * archive it came from (nextestExtractionKey); the lock and staging directories of a key carry that key
 * as their prefix, so cleanup can tell them from another key's.
 */
export interface NextestExtractionPaths {
  root: string;
  directory: string;
  lock: string;
  stagingPrefix: string;
}

export function nextestExtractionPaths(root: string, key: string): NextestExtractionPaths {
  return {
    root,
    directory: join(root, key),
    lock: join(root, `${key}.lock`),
    stagingPrefix: join(root, `${key}.staging-`),
  };
}

/**
 * The extraction root of the Cargo workspace in `cwd`:
 * `<target_directory>/nextest-extracted`, with the target directory Cargo itself
 * reports, so a `build.target-dir` in the repository's Cargo config is honoured
 * exactly as every build of the workspace honours it.
 */
export async function nextestExtractionRoot(cwd: string): Promise<string> {
  const args = ['--frozen', 'metadata', '--no-deps', '--format-version', '1'];
  const child = spawn('cargo', args, { cwd, stdio: ['ignore', 'pipe', 'inherit'] });
  const chunks: Buffer[] = [];
  child.stdout.on('data', (chunk: Buffer) => chunks.push(chunk));
  // 'close', not 'exit': stdout may still hold unread metadata when the process exits.
  const { code, signal } = await childSettled(child, 'close');
  if (code !== 0) {
    throw new Error(
      `cargo ${args.join(' ')} ${code === null ? `died of ${String(signal)}` : `exited with status ${String(code)}`}`,
    );
  }
  return join(cargoTargetDirectory(Buffer.concat(chunks).toString('utf8')), NEXTEST_EXTRACTION_DIRECTORY);
}

/** `target_directory` from `cargo metadata --format-version 1` output. */
function cargoTargetDirectory(metadata: string): string {
  const parsed: unknown = JSON.parse(metadata);
  const directory =
    typeof parsed === 'object' && parsed !== null && 'target_directory' in parsed ? parsed.target_directory : undefined;
  if (typeof directory !== 'string' || directory.length === 0) {
    throw new Error('cargo metadata named no target_directory');
  }
  return directory;
}

/**
 * The directory under `root` holding `archive`'s extraction, extracting it
 * first if no caller has yet.
 *
 * Correctness does not rest on the lock. An extraction lands under its archive's
 * key (nextestExtractionKey) by renaming a private staging directory into place,
 * so a directory under a key is a complete extraction of that archive file
 * or it does not exist: concurrent extractors cannot corrupt it, the loser of
 * a race discards its own copy, and an archive rewritten mid-extraction is
 * caught by reading its key and its file's state again before publishing. The lock only stops every
 * runner of one Nx run from unpacking the same archive side by side, which is the
 * cost it exists to remove.
 *
 * Publishing a key removes every other key's extraction under `root`,
 * including stale locks and staging left by a holder that died. Leftovers of
 * the CURRENT key are not touched: a holder that took over a stale lock may
 * still be extracting into one.
 */
export async function ensureNextestArchiveExtracted(
  archive: string,
  root: string,
  extract: NextestArchiveExtractor,
  onEvent: (event: NextestExtractionEvent) => void,
  timing: NextestExtractionTiming = DEFAULT_TIMING,
): Promise<NextestExtraction> {
  const started = performance.now();
  const identity = await archiveIdentity(archive);
  const key = extractionKey(identity);
  const paths = nextestExtractionPaths(root, key);
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
        const published = await extractAndPublish(archive, identity, paths, extract);
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
 * complete extraction of the same archive in place.
 */
async function extractAndPublish(
  archive: string,
  before: ArchiveIdentity,
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
    const after = await archiveIdentity(archive);
    if (after.key !== before.key || after.file !== before.file) {
      throw new Error(
        `${archive} changed while it was being extracted (task ${before.key}, ${before.file} before; ` +
          `task ${after.key}, ${after.file} after); rerun once whatever is rewriting it has finished`,
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
    const fd = openSync(path, 'wx');
    try {
      writeSync(fd, `${token}\n`);
    } finally {
      closeSync(fd);
    }
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
      const content = await handle.readFile('utf8');
      const metadata = await handle.stat();
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
    const { code, signal } = await childSettled(child, 'exit');
    if (code !== 0) {
      throw new Error(
        `cargo ${args.join(' ')} ${code === null ? `died of ${String(signal)}` : `exited with status ${String(code)}`}`,
      );
    }
  } finally {
    await rm(config, { force: true });
  }
};

/** The exit `child` reports on `event`, or its spawn error when it cannot be started at all. */
function childSettled(
  child: ChildProcess,
  event: 'close' | 'exit',
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  // A promise settles once, so whichever of 'error' and `event` comes second changes nothing; the listener stays so a
  // late 'error' is never an unhandled one.
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the cargo metadata read (--frozen --no-deps, no package-cache lock) and the nextest archive extraction it settles last as long as their work; the operator's interrupt or the enclosing runner's bound ends a hung one
  return new Promise((resolve, reject) => {
    child.once('error', reject);
    if (event === 'close') child.once('close', (code, signal) => resolve({ code, signal }));
    else child.once('exit', (code, signal) => resolve({ code, signal }));
  });
}

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
