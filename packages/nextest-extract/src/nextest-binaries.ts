import { type ChildProcess, spawn } from 'node:child_process';
import { closeSync, openSync, writeSync } from 'node:fs';
import { access, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { isAbsolute, join, relative } from 'node:path';
import { stripVTControlCharacters } from 'node:util';

/**
 * The workspace's test binaries, run in place (nx-plugin README, "cargo-test-build"). The `cargo-test-build` target
 * compiles them with `cargo nextest list --list-type binaries-only --message-format json` (an mbx-cached build) and
 * keeps nextest's reuse-build metadata and its own task hash as its declared outputs, relative to the Cargo workspace
 * root. Every runner executes `cargo nextest run --binaries-metadata … --cargo-metadata …` against the binaries the
 * build left in the Cargo target directory: no archive, no extraction and no copy, which an archive only needs when
 * the binaries are built on one platform and run on another (the cross lanes).
 */
export const NEXTEST_BINARIES_METADATA = '.cache/nextest/binaries-metadata.json';
export const NEXTEST_CARGO_METADATA = '.cache/nextest/cargo-metadata.json';
/**
 * The build's key: the hash of the `cargo-test-build` task that wrote the metadata beside it. A binary's path is cargo's
 * `-C metadata` hash, which ignores source content, so the metadata of an edited tree names the paths an older build
 * left: the key, not the paths, says which build a target directory holds.
 */
export const NEXTEST_BUILD_KEY = '.cache/nextest/build.key';

/** Where a target directory records the key of the build it holds, and the lock its fixtures check it under. */
const TARGET_BUILD_KEY = 'nextest-build.key';
const TARGET_BUILD_LOCK = 'nextest-build.lock';
const LOCK_POLL_MS = 200;

/**
 * The arguments after `cargo --frozen` of the build of the workspace's test binaries, without its tool config: the
 * `cargo-test-build` target's command, every runner's fixture, and every script that runs the binaries itself build
 * the same units, so what a fixture materializes is what the metadata names.
 */
export const NEXTEST_BINARIES_BUILD_ARGS = [
  'nextest',
  'list',
  '--workspace',
  '--list-type',
  'binaries-only',
  '--message-format',
  'json',
  '--user-config-file',
  'none',
] as const;

/**
 * A file the binaries metadata names, by its path relative to the build's target directory: a test binary, or a
 * non-test binary (bin-exe, cdylib) a test runs or loads.
 */
export interface TestBinary {
  readonly id: string;
  readonly relativePath: string;
}

export interface BinariesMetadata {
  /** The target directory of the checkout that ran the build, which a run in another checkout remaps from. */
  readonly targetDirectory: string;
  readonly binaries: readonly TestBinary[];
}

function field(value: unknown, key: string): unknown {
  return typeof value === 'object' && value !== null && key in value ? Reflect.get(value, key) : undefined;
}

function within(targetDirectory: string, id: string, path: string): TestBinary {
  const relativePath = isAbsolute(path) ? relative(targetDirectory, path) : path;
  if (relativePath === '' || relativePath.startsWith('..') || isAbsolute(relativePath)) {
    throw new Error(`the binaries metadata puts ${id} at ${path}, outside its target directory ${targetDirectory}`);
  }
  return { id, relativePath };
}

/** The metadata `cargo nextest list --list-type binaries-only --message-format json` wrote; refuses another shape. */
export function readBinariesMetadata(text: string): BinariesMetadata {
  const parsed: unknown = JSON.parse(text);
  const build = field(parsed, 'rust-build-meta');
  const targetDirectory = field(build, 'target-directory');
  const binaries = field(parsed, 'rust-binaries');
  const nonTest = field(build, 'non-test-binaries') ?? {};
  if (
    typeof targetDirectory !== 'string' ||
    typeof binaries !== 'object' ||
    binaries === null ||
    typeof nonTest !== 'object' ||
    nonTest === null
  ) {
    throw new Error('the binaries metadata names no rust-build-meta target-directory or rust-binaries');
  }
  const tests = Object.values(binaries).map((binary: unknown) => {
    const id = field(binary, 'binary-id');
    const path = field(binary, 'binary-path');
    if (typeof id !== 'string' || typeof path !== 'string') {
      throw new Error(`the binaries metadata has a binary with no binary-id or binary-path: ${JSON.stringify(binary)}`);
    }
    return within(targetDirectory, id, path);
  });
  // Relative to the target directory, grouped by the package that built them.
  const others = Object.values(nonTest).flatMap((entries: unknown) => {
    if (!Array.isArray(entries)) throw new Error('the binaries metadata has a non-test-binaries entry that is no list');
    return entries.map((entry: unknown) => {
      const name = field(entry, 'name');
      const kind = field(entry, 'kind');
      const path = field(entry, 'path');
      if (typeof name !== 'string' || typeof kind !== 'string' || typeof path !== 'string') {
        throw new Error(
          `the binaries metadata has a non-test binary with no name, kind or path: ${JSON.stringify(entry)}`,
        );
      }
      return within(targetDirectory, `${name} (${kind})`, path);
    });
  });
  return { targetDirectory, binaries: [...tests, ...others] };
}

/** The binaries of `metadata` that `targetDirectory` does not hold. */
export async function missingBinaries(metadata: BinariesMetadata, targetDirectory: string): Promise<TestBinary[]> {
  const present = await Promise.all(
    metadata.binaries.map((binary) =>
      access(join(targetDirectory, binary.relativePath)).then(
        () => true,
        (error: unknown) => {
          if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
          throw error;
        },
      ),
    ),
  );
  return metadata.binaries.filter((_binary, index) => !present[index]);
}

/** The exit `child` reports on `event`, or its spawn error when it cannot be started at all. */
function childSettled(
  child: ChildProcess,
  event: 'close' | 'exit',
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a cargo metadata read or the build that materializes the binaries lasts as long as its work; the enclosing runner's fixture budget ends a hung one
  return new Promise((resolve, reject) => {
    child.once('error', reject);
    if (event === 'close') child.once('close', (code, signal) => resolve({ code, signal }));
    else child.once('exit', (code, signal) => resolve({ code, signal }));
  });
}

/**
 * The Cargo target directory of the workspace in `cwd`, as `cargo metadata` reports it, so a `build.target-dir` in the
 * repository's Cargo config is honoured exactly as every build of the workspace honours it.
 */
export async function cargoTargetDirectory(cwd: string): Promise<string> {
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
  const directory = field(JSON.parse(Buffer.concat(chunks).toString('utf8')), 'target_directory');
  if (typeof directory !== 'string' || directory.length === 0)
    throw new Error('cargo metadata named no target_directory');
  return directory;
}

/** What one materializing build reported: its wall time and mbx's summary line, when it printed one. */
export interface Materialization {
  readonly elapsedMs: number;
  /** mbx's `mbx[cache]: …` summary, null when the build ran without mbx. */
  readonly mbx: string | null;
  /** Compilations mbx could not serve: its `misses` count, 0 without mbx. */
  readonly misses: number;
  /** The crates cargo named while building, in its order. */
  readonly crates: readonly string[];
}

/** Reads the materializing build's stderr: mbx's cache summary and the crates cargo named. */
export function readMaterialization(stderr: string, elapsedMs: number): Materialization {
  const plain = stripVTControlCharacters(stderr);
  const mbx = /mbx\[cache\]: [^\n]*/.exec(plain)?.[0] ?? null;
  const misses = Number(/(\d+) misses/.exec(mbx ?? '')?.[1] ?? '0');
  const crates = [...plain.matchAll(/^\s*Compiling (\S+) v/gm)].map((match) => match[1] ?? '');
  return { elapsedMs, mbx, misses, crates };
}

/**
 * Runs the build that materializes the binaries (`command`, the cargo-test-build target's own `cargo nextest list`),
 * passing its stderr through and discarding the metadata it prints, which the runner already holds.
 */
export async function materializeBinaries(command: readonly string[], cwd: string): Promise<Materialization> {
  const [program, ...args] = command;
  if (program === undefined) throw new Error('no build command to materialize the test binaries with');
  const started = performance.now();
  const child = spawn(program, args, { cwd, stdio: ['ignore', 'ignore', 'pipe'] });
  const chunks: Buffer[] = [];
  child.stderr.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    process.stderr.write(chunk);
  });
  const { code, signal } = await childSettled(child, 'close');
  if (code !== 0) {
    throw new Error(
      `${command.join(' ')} ${code === null ? `died of ${String(signal)}` : `exited with status ${String(code)}`}`,
    );
  }
  return readMaterialization(Buffer.concat(chunks).toString('utf8'), performance.now() - started);
}

/** The outcome of ensuring a run's binaries: this build already in place, or built here first. */
export type BinariesReadiness =
  | { readonly kind: 'present'; readonly key: string; readonly count: number; readonly targetDirectory: string }
  | {
      readonly kind: 'materialized';
      readonly key: string;
      /** The build the target directory held before, null when it recorded none. */
      readonly held: string | null;
      readonly count: number;
      readonly absent: number;
      readonly targetDirectory: string;
      readonly build: Materialization;
    };

async function readKey(path: string): Promise<string | null> {
  try {
    const key = (await readFile(path, 'utf8')).trim();
    return key === '' ? null : key;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  }
}

/** Records that `targetDirectory` holds the build `key` names, replacing the record whole. */
export async function recordBuildKey(targetDirectory: string, key: string): Promise<void> {
  const record = join(targetDirectory, TARGET_BUILD_KEY);
  const staged = `${record}.${process.pid}`;
  await writeFile(staged, `${key}\n`);
  await rename(staged, record);
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: a process exists that this one may not signal, as across sandboxes.
    return error instanceof Error && 'code' in error && error.code === 'EPERM';
  }
}

/**
 * Runs `body` holding the target directory's fixture lock, so the fixtures of one checkout's runners rebuild once
 * between them rather than each queueing a redundant pass on Cargo's own lock. A holder that died leaves a lock this
 * takes over. The lock is an economy, not a guarantee: two fixtures that both build are serialized by Cargo and record
 * the same key, so a lost race costs one extra freshness pass and nothing else.
 * A target directory that does not exist yet holds no build, and this never creates it (a managed build volume links it
 * into place): its fixtures build without the lock, and Cargo serializes them.
 */
async function withTargetLock<T>(targetDirectory: string, body: () => Promise<T>): Promise<T> {
  const lock = join(targetDirectory, TARGET_BUILD_LOCK);
  const mine = String(process.pid);
  for (;;) {
    try {
      // Synchronous by necessity: the native helper's runtime lowers no fs.promises.writeFile flag.
      const fd = openSync(lock, 'wx');
      try {
        writeSync(fd, mine);
      } finally {
        closeSync(fd);
      }
      break;
    } catch (error) {
      const code = error instanceof Error && 'code' in error ? error.code : undefined;
      if (code === 'ENOENT') return body();
      if (code !== 'EEXIST') throw error;
    }
    const holder = Number(await readKey(lock));
    if (Number.isInteger(holder) && holder > 0 && !alive(holder)) {
      await rm(lock, { force: true });
      continue;
    }
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the wait lasts while a live fixture of this checkout builds; the runner's fixture budget ends a hung one
    await new Promise((resolve) => setTimeout(resolve, LOCK_POLL_MS));
  }
  try {
    return await body();
  } finally {
    if ((await readKey(lock)) === mine) await rm(lock, { force: true });
  }
}

/**
 * Ensures this checkout's target directory holds the build `keyFile` names, with every binary `metadataFile` names: a
 * read and one stat per binary when it does, else `materialize` (the runner's fixture), the same stats again, refusing
 * what is still absent, and the key recorded. A target holding another build is rebuilt even when every path exists.
 */
export async function ensureNextestBinaries(
  metadataFile: string,
  keyFile: string,
  cwd: string,
  materialize: () => Promise<Materialization>,
  targetDirectory: (cwd: string) => Promise<string> = cargoTargetDirectory,
): Promise<BinariesReadiness> {
  const metadata = readBinariesMetadata(await readFile(metadataFile, 'utf8'));
  const key = await readKey(keyFile);
  if (key === null) throw new Error(`${keyFile} names no build: run the test build, whose task writes it`);
  const target = await targetDirectory(cwd);
  const count = metadata.binaries.length;
  return withTargetLock(target, async () => {
    const held = await readKey(join(target, TARGET_BUILD_KEY));
    const absent = await missingBinaries(metadata, target);
    if (held === key && absent.length === 0) return { kind: 'present', key, count, targetDirectory: target };
    const build = await materialize();
    const still = await missingBinaries(metadata, target);
    if (still.length > 0) {
      throw new Error(
        `${still.length} test binaries are still absent from ${target} after building them, first ${still[0]?.relativePath}: ` +
          `the build does not produce what ${metadataFile} names`,
      );
    }
    await recordBuildKey(target, key);
    return { kind: 'materialized', key, held, count, absent: absent.length, targetDirectory: target, build };
  });
}

/**
 * What a fixture says about one ensure, apart from the run's own output: how long it took and, when it built, what
 * mbx served, loudly when mbx had to compile.
 */
export function describeReadiness(readiness: BinariesReadiness, elapsedMs: number): string[] {
  const took = `${(elapsedMs / 1000).toFixed(1)}s`;
  if (readiness.kind === 'present') {
    return [
      `build ${readiness.key}: ${readiness.count} test binaries present in ${readiness.targetDirectory}; checked in ${took}`,
    ];
  }
  const { build } = readiness;
  const why =
    readiness.held === readiness.key
      ? `${readiness.absent} of its ${readiness.count} test binaries were absent`
      : `the target held ${readiness.held === null ? 'no recorded build' : `build ${readiness.held}`}`;
  const lines = [
    `build ${readiness.key}: ${why} in ${readiness.targetDirectory}; built it in ${took} (${build.mbx ?? 'no mbx summary'})`,
  ];
  if (build.misses > 0) {
    lines.push(
      `CACHE MISS: mbx compiled ${build.misses} units building the test binaries; the crates cargo built: ` +
        build.crates.join(', '),
    );
  }
  return lines;
}

/**
 * For a script that runs the workspace's test binaries itself, what a runner's fixture does for a runner: ensures
 * this checkout holds the test build, building it with the test build's command under `toolConfigFile` (nextest's
 * `--tool-config-file` value) when it does not, and returns the target directory a run remaps the build's onto.
 */
export async function ensureInPlace(cwd: string, toolConfigFile: string, say: (line: string) => void): Promise<string> {
  const started = performance.now();
  const readiness = await ensureNextestBinaries(
    join(cwd, NEXTEST_BINARIES_METADATA),
    join(cwd, NEXTEST_BUILD_KEY),
    cwd,
    () =>
      materializeBinaries(
        ['cargo', '--frozen', ...NEXTEST_BINARIES_BUILD_ARGS, '--tool-config-file', toolConfigFile],
        cwd,
      ),
  );
  for (const line of describeReadiness(readiness, performance.now() - started)) say(line);
  return readiness.targetDirectory;
}
