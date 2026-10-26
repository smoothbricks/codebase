import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  type BigIntStats,
  closeSync,
  lstatSync,
  openSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  readSync,
  realpathSync,
} from 'node:fs';
import { isAbsolute, join, relative } from 'node:path';
import type { Task } from 'nx/src/config/task-graph';
import type { expandOutputs, HashInputs } from 'nx/src/native';
import { CARGO_TOOLCHAIN_PIN_INPUTS, cargoRuntimeInput, cargoToolchainPathsInput } from './cargo-toolchain-policy.js';
import type {
  EnsureBuiltOutputReceipt,
  EnsureBuiltProbeFailure,
  EnsureBuiltProducerFileInput,
  EnsureBuiltProducerReceipt,
  EnsureBuiltReceipt,
} from './ensure-built.js';
import {
  type CensusRead,
  censusStart,
  directoryMembers,
  movedNodes,
  type NodeWitness,
  recordedStamp,
  type StampFields,
} from './ensure-built-witness.js';

export type ReceiptEvidence<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly detail: string };
export interface ProviderInputProof {
  readonly kind: 'exact-file-inputs' | 'conservative-provider-source';
  readonly files: readonly string[];
  readonly inputs: Readonly<Record<string, HashInputs>>;
  readonly runtimeSource: { readonly kind: 'bounded' } | { readonly kind: 'opaque'; readonly detail: string };
}
export interface ProducerSourceWitness {
  readonly head: string;
  readonly headTree: string;
  readonly files: readonly EnsureBuiltProducerFileInput[];
  readonly nodes: readonly NodeWitness[];
  readonly outputs: readonly string[];
}
/**
 * Git as receipts read it: the checkout and nothing of the operator's home. The producer hooks run in whichever process
 * Nx runs plugins in, a workspace's sandboxed daemon included, which adopts a host client's HOME; git with that HOME
 * reads the operator's global configuration, the sandbox denies it, and git dies (specs/cowshed/04_sandbox.md, the
 * runtime-input contract). A receipt's tree, HEAD and tracked-file status are read under the repository's own
 * configuration alone, so a host run and a sandboxed run agree on them.
 */
export function receiptGit(root: string, args: string[]): ReceiptEvidence<string> {
  const env: NodeJS.ProcessEnv = { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' };
  for (const key of [
    'GIT_DIR',
    'GIT_COMMON_DIR',
    'GIT_WORK_TREE',
    'GIT_INDEX_FILE',
    'GIT_OBJECT_DIRECTORY',
    'GIT_ALTERNATE_OBJECT_DIRECTORIES',
    'GIT_NAMESPACE',
  ])
    delete env[key];
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git query: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
  const child = spawnSync('git', ['-C', root, ...args], { env, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  return child.status === 0 && child.error === undefined
    ? { ok: true, value: child.stdout }
    : {
        ok: false,
        detail: `git ${args.join(' ')}: ${child.error?.message ?? child.stderr.trim()} (exit ${child.status}, signal ${child.signal})`,
      };
}
/** NUL-delimited paths preserve whitespace, rename sources and index-only changes. */
export function dirtyProviderPaths(status: string): string[] {
  const entries = status.split('\0');
  const paths: string[] = [];
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    if (entry === '') continue;
    paths.push(entry.slice(3));
    if (entry[0] === 'R' || entry[0] === 'C' || entry[1] === 'R' || entry[1] === 'C') {
      index += 1;
      if (entries[index] !== undefined) paths.push(entries[index]);
    }
  }
  return paths;
}
function graphConfiguration(path: string): boolean {
  const name = path.slice(path.lastIndexOf('/') + 1);
  return [
    'nx.json',
    'project.json',
    'package.json',
    'bun.lock',
    'bun.lockb',
    'package-lock.json',
    'pnpm-lock.yaml',
    'yarn.lock',
  ].includes(name);
}
let readBuffer: Buffer | undefined;
function fileReadBuffer(): Buffer {
  if (readBuffer === undefined) readBuffer = Buffer.allocUnsafe(1 << 20);
  return readBuffer;
}
function hashFile(path: string, size: number, algorithms: readonly ('sha1' | 'sha256')[], blob: boolean): string[] {
  const hashes = algorithms.map((algorithm) => createHash(algorithm));
  if (blob) for (const hash of hashes) hash.update(`blob ${size}\0`);
  const buffer = fileReadBuffer();
  const descriptor = openSync(path, 'r');
  try {
    for (let offset = 0; offset < size; offset += buffer.length) {
      const length = Math.min(buffer.length, size - offset);
      if (readSync(descriptor, buffer, 0, length, offset) !== length)
        throw new Error(`File moved while reading ${path}`);
      for (const hash of hashes) hash.update(buffer.subarray(0, length));
    }
  } finally {
    closeSync(descriptor);
  }
  return hashes.map((hash) => hash.digest('hex'));
}
function sourceFileDigests(
  path: string,
  size: number,
  algorithm: 'sha1' | 'sha256',
): { readonly gitBlob: string; readonly sha256: string } {
  const blob = createHash(algorithm).update(`blob ${size}\0`);
  const raw = createHash('sha256');
  const buffer = fileReadBuffer();
  const descriptor = openSync(path, 'r');
  try {
    for (let offset = 0; offset < size; offset += buffer.length) {
      const length = Math.min(buffer.length, size - offset);
      if (readSync(descriptor, buffer, 0, length, offset) !== length)
        throw new Error(`Source moved while reading ${path}`);
      const bytes = buffer.subarray(0, length);
      blob.update(bytes);
      raw.update(bytes);
    }
  } finally {
    closeSync(descriptor);
  }
  return { gitBlob: blob.digest('hex'), sha256: raw.digest('hex') };
}
/**
 * One independently read node; absolute paths also serve tool/package closures. A census records the node's stamp as
 * of its `start` (recordedStamp); a re-read against a record records none. `known` is what a census of the same capture
 * already read of the node ({@link CensusRead}): its stat, listing and start stand for this read, so a capture stats a
 * node once; the stability check after execution reads it again.
 */
export function receiptNode(
  path: string,
  name: string,
  census?: { readonly start: bigint; readonly observed: NodeWitness[] },
  recorded?: (path: string, stats: StampFields) => string | undefined,
  known?: CensusRead,
): EnsureBuiltOutputReceipt {
  const stats = known?.stats ?? lstatSync(path, { bigint: true });
  census?.observed.push({ path, stamp: recordedStamp(stats, known?.start ?? census.start) });
  const mode = Number(stats.mode & 0o7777n);
  if (stats.isFile())
    return {
      path: name,
      type: 'file',
      mode,
      sha256: recorded?.(path, stats) ?? hashFile(path, Number(stats.size), ['sha256'], false)[0],
    };
  if (stats.isSymbolicLink()) return { path: name, type: 'symlink', mode, linkText: readlinkSync(path) };
  if (stats.isDirectory()) {
    const entries = known?.entries ?? directoryMembers(path);
    return {
      path: name,
      type: 'directory',
      mode,
      entries,
      sha256: createHash('sha256').update(JSON.stringify(entries)).digest('hex'),
    };
  }
  throw new Error(`Unsupported receipt node ${path}`);
}
/**
 * Whether a tracked source node whose stamp does not prove it (its ctime alone moved, or it was read in the tick it
 * changed in) still holds the bytes or link text `files` recorded for it. A node that is gone, or no longer a file, does not.
 */
function recordedSource(root: string, files: readonly EnsureBuiltProducerFileInput[]): (path: string) => boolean {
  const recorded = new Map(files.map((file) => [join(root, file.path), file.sha256]));
  return (path) => {
    try {
      return (
        recorded.get(path) ===
        createHash('sha256')
          .update(lstatSync(path).isSymbolicLink() ? readlinkSync(path) : readFileSync(path))
          .digest('hex')
      );
    } catch (error) {
      if (error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'EISDIR'))
        return false;
      throw error;
    }
  };
}
/**
 * One tracked file as it stands against HEAD's entry for it (`head.mode`, `head.oid`): the witness a receipt binds it
 * by and the stamp it was read at, or how it differs.
 */
function trackedSource(
  root: string,
  file: string,
  head: { readonly mode: string; readonly oid: string; readonly algorithm: 'sha1' | 'sha256' },
  start: bigint,
): ReceiptEvidence<{ readonly node: NodeWitness; readonly input: EnsureBuiltProducerFileInput }> {
  const path = join(root, file);
  let stats: BigIntStats;
  try {
    stats = lstatSync(path, { bigint: true });
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT')
      return { ok: false, detail: `Tracked source is missing: ${file}` };
    throw error;
  }
  const actualMode = stats.isSymbolicLink() ? '120000' : (stats.mode & 0o111n) !== 0n ? '100755' : '100644';
  if (actualMode !== head.mode) return { ok: false, detail: `Tracked source mode differs from HEAD: ${file}` };
  let gitBlob: string;
  let sha256: string;
  if (stats.isFile()) {
    const digests = sourceFileDigests(path, Number(stats.size), head.algorithm);
    gitBlob = digests.gitBlob;
    sha256 = digests.sha256;
  } else if (stats.isSymbolicLink()) {
    const text = readlinkSync(path);
    gitBlob = createHash(head.algorithm)
      .update(`blob ${Buffer.byteLength(text)}\0`)
      .update(text)
      .digest('hex');
    sha256 = createHash('sha256').update(text).digest('hex');
  } else return { ok: false, detail: `Unbound tracked source node: ${file}` };
  if (gitBlob !== head.oid) return { ok: false, detail: `Tracked source bytes differ from HEAD: ${file}` };
  return {
    ok: true,
    value: {
      node: { path, stamp: recordedStamp(stats, start) },
      input: { path: file, mode: Number(stats.mode & 0o7777n), gitBlob, sha256 },
    },
  };
}
/**
 * Capture every tracked source byte at pre. Unrelated untracked files are not source basis. A tracked public output is
 * source while it holds HEAD's bytes, so a later task can read it as a FILE input; one that differs from HEAD is the
 * run's to write, and no input binds to it.
 */
export function captureProducerSource(
  root: string,
  outputs: readonly string[],
): ReceiptEvidence<ProducerSourceWitness> {
  try {
    const revision = receiptGit(root, ['rev-parse', '--verify', 'HEAD']);
    if (!revision.ok) return revision;
    const head = revision.value.trim();
    const tree = receiptGit(root, ['rev-parse', '--verify', `${head}^{tree}`]);
    if (!tree.ok) return tree;
    const listed = receiptGit(root, ['ls-tree', '-r', '-z', '--full-tree', head]);
    if (!listed.ok) return listed;
    const format = receiptGit(root, ['rev-parse', '--show-object-format']);
    if (!format.ok) return format;
    const algorithm = format.value.trim();
    if (algorithm !== 'sha1' && algorithm !== 'sha256')
      return { ok: false, detail: `Unproved Git object format ${algorithm}` };
    const excluded = new Set(outputs.filter((path) => !graphConfiguration(path)));
    const nodes: NodeWitness[] = [];
    const files: EnsureBuiltProducerFileInput[] = [];
    const start = censusStart();
    for (const entry of listed.value.split('\0')) {
      if (entry === '') continue;
      const tab = entry.indexOf('\t');
      const [mode, kind, oid] = entry.slice(0, tab).split(' ');
      if (tab < 0 || oid === undefined || mode === undefined)
        return { ok: false, detail: `Unproved HEAD tree entry ${entry}` };
      if (kind !== 'blob') return { ok: false, detail: `Unbound tracked source namespace ${entry}` };
      const file = entry.slice(tab + 1);
      const witnessed = trackedSource(root, file, { mode, oid, algorithm }, start);
      if (!witnessed.ok) {
        if (excluded.has(file)) continue;
        return witnessed;
      }
      // A public output's stamp is the run's to move (a cache restore or a regeneration does), so only its bytes are
      // held.
      if (!excluded.has(file)) nodes.push(witnessed.value.node);
      files.push(witnessed.value.input);
    }
    const status = receiptGit(root, ['status', '--porcelain=v1', '-z', '--untracked-files=no']);
    if (!status.ok) return status;
    const dirty = dirtyProviderPaths(status.value).filter((path) => !excluded.has(path));
    if (dirty.length !== 0)
      return { ok: false, detail: `Tracked source/index differs from ${head}: ${dirty.join(', ')}` };
    const moved = movedNodes(nodes, recordedSource(root, files));
    if (moved !== null) return { ok: false, detail: `Tracked source moved during producer pre capture: ${moved}` };
    return { ok: true, value: { head, headTree: tree.value.trim(), files, nodes, outputs } };
  } catch (error) {
    if (error instanceof Error) return { ok: false, detail: `Cannot prove tracked producer tree: ${error.message}` };
    throw error;
  }
}
export function producerSourceUnchanged(root: string, before: ProducerSourceWitness): ReceiptEvidence<true> {
  const revision = receiptGit(root, ['rev-parse', '--verify', 'HEAD']);
  if (!revision.ok) return revision;
  const status = receiptGit(root, ['status', '--porcelain=v1', '-z', '--untracked-files=no']);
  if (!status.ok) return status;
  const outputs = new Set(before.outputs.filter((path) => !graphConfiguration(path)));
  const tree = receiptGit(root, ['rev-parse', '--verify', `${revision.value.trim()}^{tree}`]);
  if (!tree.ok) return tree;
  const changed = 'Producer HEAD/tracked source nodes changed across actual task execution';
  if (tree.value.trim() !== before.headTree)
    return { ok: false, detail: `${changed}: HEAD's tree is ${tree.value.trim()}, not ${before.headTree}` };
  const dirty = dirtyProviderPaths(status.value).filter((path) => !outputs.has(path));
  if (dirty.length !== 0) return { ok: false, detail: `${changed}: ${dirty.join(', ')} differ from HEAD` };
  const holds = recordedSource(root, before.files);
  const moved = movedNodes(before.nodes, holds);
  if (moved !== null) return { ok: false, detail: `${changed}: ${moved}` };
  const rewritten = before.files.filter(({ path }) => outputs.has(path) && !holds(join(root, path)));
  if (rewritten.length !== 0)
    return {
      ok: false,
      detail: `${changed}: tracked public output ${rewritten.map(({ path }) => path).join(', ')} no longer holds HEAD's bytes`,
    };
  return { ok: true, value: true };
}
/** Enumerate exact public nodes; never exempt a broad directory of tracked source. */
export function producerOutputPaths(root: string, tasks: readonly Task[], expand: typeof expandOutputs): string[] {
  const paths = new Set<string>();
  function visit(path: string): void {
    if (isAbsolute(path) || path.split('/').includes('..')) throw new Error(`Output escapes provider: ${path}`);
    if (paths.has(path)) return;
    paths.add(path);
    let stats: ReturnType<typeof lstatSync>;
    try {
      stats = lstatSync(join(root, path));
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return;
      throw error;
    }
    if (stats.isDirectory()) for (const name of readdirSync(join(root, path))) visit(join(path, name));
  }
  for (const task of tasks) for (const path of expand(root, task.outputs)) visit(path);
  return [...paths].sort();
}
export function providerReceipt(
  root: string,
  taskId: string,
  tasks: readonly Task[],
  proof: ProviderInputProof,
  expand: typeof expandOutputs,
  source: ProducerSourceWitness,
  producer: Omit<EnsureBuiltProducerReceipt, 'outputSha256'>,
): { readonly disposition: 'hit'; readonly receipt: EnsureBuiltReceipt } | EnsureBuiltProbeFailure {
  const failed = (detail: string): EnsureBuiltProbeFailure => ({
    disposition: 'failed',
    reason: { kind: 'provider-proof', taskId, workspaceRoot: root, detail },
    exitCode: 1,
    signal: null,
  });
  if (proof.runtimeSource.kind === 'opaque') return failed(proof.runtimeSource.detail);
  const stable = producerSourceUnchanged(root, source);
  if (!stable.ok) return failed(stable.detail);
  const sourceFiles = new Map(source.files.map((file) => [file.path, file]));
  for (const file of proof.files) {
    const record = sourceFiles.get(file);
    if (record === undefined) return failed(`Actual Nx FILE input is not bound to provider HEAD: ${file}`);
    if (!lstatSync(join(root, file)).isFile()) return failed(`Unbound linked/non-file Nx source: ${file}`);
  }
  const observed: NodeWitness[] = [];
  const census = { start: censusStart(), observed };
  const parents = new Set<string>();
  const outputs = new Map<string, EnsureBuiltOutputReceipt>();
  function visit(path: string): void {
    if (outputs.has(path)) return;
    const absolute = join(root, path);
    let node = receiptNode(absolute, path, census);
    if (node.type === 'directory') {
      for (const entry of node.entries) visit(join(path, entry));
      const children = node.entries.map((entry) => outputs.get(join(path, entry)));
      node = { ...node, sha256: createHash('sha256').update(JSON.stringify(children)).digest('hex') };
    }
    outputs.set(path, node);
  }
  try {
    const receipts: EnsureBuiltReceipt['tasks'][number][] = [];
    for (const task of tasks) {
      if (task.hash === undefined) return failed(`Actual completion supplied no hash for ${task.id}`);
      const entries = task.outputs.length === 0 ? [] : expand(root, task.outputs);
      if (task.outputs.length !== 0 && entries.length === 0) return failed(`No public outputs for ${task.id}`);
      for (const entry of entries) {
        if (isAbsolute(entry) || entry.split('/').includes('..')) return failed(`Output escapes provider: ${entry}`);
        let parent = root;
        for (const segment of entry.split('/').slice(0, -1)) {
          parent = join(parent, segment);
          const stats = lstatSync(parent, { bigint: true });
          observed.push({ path: parent, stamp: recordedStamp(stats, census.start) });
          parents.add(parent);
          if (!stats.isDirectory()) return failed(`Symlinked public output parent ${parent}`);
        }
        visit(entry);
      }
      receipts.push({
        id: task.id,
        hash: task.hash,
        outputs: [...outputs]
          .filter(([path]) => entries.some((entry) => path === entry || path.startsWith(`${entry}/`)))
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([, node]) => node),
      });
    }
    const final = producerSourceUnchanged(root, source);
    if (!final.ok) return failed(final.detail);
    // An output whose stamp does not prove it (its ctime alone moved, or it was read in the tick it changed in, as a
    // task's last writes are) is re-read against its record. A parent directory binds only the real directory the path
    // runs through, which the device, inode and mode its kept stamp still show.
    const moved = movedNodes(observed, (path) => {
      const record = outputs.get(relative(root, path));
      if (record === undefined) return parents.has(path);
      const now = receiptNode(path, record.path);
      return record.type === 'directory' && now.type === 'directory'
        ? record.mode === now.mode && JSON.stringify(record.entries) === JSON.stringify(now.entries)
        : JSON.stringify(record) === JSON.stringify(now);
    });
    if (moved !== null) return failed(`Public outputs changed during independent census: ${moved}`);
    const outputSha256 = createHash('sha256').update(JSON.stringify(receipts)).digest('hex');
    return {
      disposition: 'hit',
      receipt: {
        workspaceRoot: realpathSync(root),
        head: source.head,
        headTree: source.headTree,
        fileInputProvenance: proof.kind,
        runtimeEnvironmentProvenance: 'nx-task-hash-only',
        producer: { ...producer, outputSha256 },
        tasks: receipts,
      },
    };
  } catch (error) {
    if (error instanceof Error) return failed(`Cannot prove public product census: ${error.message}`);
    throw error;
  }
}
/** Admit only the recipes emitted by the owning generator: the version recipes, the declared-programs one, and declared toolchain pins. */
export function boundedReceiptRuntimes(
  inputs: Readonly<Record<string, HashInputs>>,
  directories: readonly string[],
  files: readonly string[],
  installationProved: boolean,
): ProviderInputProof['runtimeSource'] {
  const runtimes = Object.values(inputs).flatMap((input) => input.runtime);
  if (runtimes.length === 0) return { kind: 'bounded' };
  if (!installationProved) return { kind: 'opaque', detail: 'Runtime input plugin/module source closure is unproved' };
  const pins = CARGO_TOOLCHAIN_PIN_INPUTS.map((path) => path.replace('{workspaceRoot}/', ''));
  if (!pins.some((pin) => files.includes(pin)))
    return { kind: 'opaque', detail: 'Version runtime input has no declared toolchain FILE pin' };
  const allowed = new Set([
    ...directories.flatMap((directory) =>
      [
        'rustc -vV && cargo -V',
        'rustc -vV && cargo -V && cargo clippy -V',
        'rustc -vV && cargo -V && cargo clippy -V && cargo fmt --version',
        'rustc -vV && cargo -V && cargo nextest --version',
      ].map((command) => cargoRuntimeInput(directory, command).runtime),
    ),
    cargoToolchainPathsInput().runtime,
  ]);
  const opaque = runtimes.filter((runtime) => !allowed.has(runtime));
  return opaque.length === 0
    ? { kind: 'bounded' }
    : { kind: 'opaque', detail: `Unbounded runtime source closure: ${opaque.join('; ')}` };
}
