import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import type { BigIntStats } from 'node:fs';
import { closeSync, lstatSync, openSync, readdirSync, readlinkSync, readSync, realpathSync } from 'node:fs';
import { isAbsolute, join, relative } from 'node:path';
import type { Task } from 'nx/src/config/task-graph';
import type { expandOutputs, HashInputs } from 'nx/src/native';
import { CARGO_TOOLCHAIN_PIN_INPUTS, cargoRuntimeInput } from './cargo-toolchain-policy.js';
import type { EnsureBuiltOutputReceipt, EnsureBuiltProbeFailure, EnsureBuiltReceipt } from './ensure-built.js';
import { type NodeWitness, nodeStamp, nodesUnchanged } from './ensure-built-witness.js';

type Evidence<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly detail: string };

export interface ProviderInputProof {
  readonly kind: 'exact-file-inputs' | 'conservative-provider-source';
  readonly files: readonly string[];
  readonly inputs: Readonly<Record<string, HashInputs>>;
  readonly runtimeSource: { readonly kind: 'bounded' } | { readonly kind: 'opaque'; readonly detail: string };
}

function git(root: string, args: string[]): Evidence<string> {
  // Provenance belongs to this provider, not a Git directory/index/object
  // namespace an outer build lent the process. Nx's hashing environment is
  // unchanged; only these read-only evidence children bind their own Git state.
  const env = { ...process.env };
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
  const child = spawnSync('git', ['-C', root, ...args], { env, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  return child.status === 0 && child.error === undefined
    ? { ok: true, value: child.stdout }
    : {
        ok: false,
        detail: `git ${args.join(' ')}: ${child.error?.message ?? child.stderr.trim()} (exit ${child.status}, signal ${child.signal})`,
      };
}

/** NUL-delimited Git paths preserve whitespace, rename sources and index-only changes. */
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

export function providerReceipt(
  workspaceRoot: string,
  taskId: string,
  tasks: readonly Task[],
  proof: ProviderInputProof,
  expand: typeof expandOutputs,
): { readonly disposition: 'hit'; readonly receipt: EnsureBuiltReceipt } | EnsureBuiltProbeFailure {
  const root = realpathSync(workspaceRoot);
  const failed = (detail: string): EnsureBuiltProbeFailure => ({
    disposition: 'failed',
    reason: { kind: 'provider-proof', taskId, workspaceRoot: root, detail },
    exitCode: 1,
    signal: null,
  });
  if (proof.runtimeSource.kind === 'opaque') return failed(proof.runtimeSource.detail);
  const revision = git(root, ['rev-parse', '--verify', 'HEAD']);
  if (!revision.ok) return failed(revision.detail);
  const head = revision.value.trim();
  const tree = git(root, ['rev-parse', '--verify', `${head}^{tree}`]);
  if (!tree.ok) return failed(tree.detail);
  const status = git(root, ['status', '--porcelain=v1', '-z', '--untracked-files=all']);
  if (!status.ok) return failed(status.detail);
  const listed = git(root, ['ls-tree', '-r', '-z', '--full-tree', head]);
  if (!listed.ok) return failed(listed.detail);
  const format = git(root, ['rev-parse', '--show-object-format']);
  if (!format.ok) return failed(format.detail);
  const algorithm = format.value.trim();
  if (algorithm !== 'sha1' && algorithm !== 'sha256') return failed(`Unproved Git object format ${algorithm}`);
  const tracked = new Map<string, { mode: string; oid: string }>();
  for (const entry of listed.value.split('\0')) {
    if (entry === '') continue;
    const tab = entry.indexOf('\t');
    const [mode, kind, oid] = entry.slice(0, tab).split(' ');
    if (tab < 0 || oid === undefined) return failed(`Unproved HEAD tree entry ${entry}`);
    if (kind !== 'blob') continue;
    tracked.set(entry.slice(tab + 1), { mode, oid });
  }
  const observed: NodeWitness[] = [];
  const buffer = Buffer.allocUnsafe(1 << 20);
  const outputNodes = new Map<string, EnsureBuiltOutputReceipt>();
  function observe(path: string): BigIntStats {
    const stats = lstatSync(path, { bigint: true });
    observed.push({ path, stamp: nodeStamp(stats) });
    return stats;
  }
  function fileDigest(path: string, size: number, algorithm: 'sha1' | 'sha256', gitBlob: boolean): string {
    const hash = createHash(algorithm);
    if (gitBlob) hash.update(`blob ${size}\0`);
    const fd = openSync(path, 'r');
    try {
      for (let offset = 0; offset < size; offset += buffer.length) {
        const count = Math.min(buffer.length, size - offset);
        if (readSync(fd, buffer, 0, count, offset) !== count) throw new Error(`File moved while reading ${path}`);
        hash.update(buffer.subarray(0, count));
      }
    } finally {
      closeSync(fd);
    }
    return hash.digest('hex');
  }
  function visit(path: string): EnsureBuiltOutputReceipt {
    const rel = relative(root, path);
    if (rel === '..' || rel.startsWith('../') || isAbsolute(rel)) throw new Error(`Output escapes provider: ${path}`);
    const previous = outputNodes.get(rel);
    if (previous !== undefined) return previous;
    const stats = observe(path);
    const mode = Number(stats.mode & 0o7777n);
    let node: EnsureBuiltOutputReceipt;
    if (stats.isFile()) {
      node = { path: rel, type: 'file', mode, sha256: fileDigest(path, Number(stats.size), 'sha256', false) };
    } else if (stats.isSymbolicLink()) {
      node = { path: rel, type: 'symlink', mode, linkText: readlinkSync(path) };
    } else if (stats.isDirectory()) {
      const children = readdirSync(path)
        .sort()
        .map((name) => visit(join(path, name)));
      const entries = children.map(({ path: child }) => relative(rel, child));
      node = {
        path: rel,
        type: 'directory',
        mode,
        entries,
        sha256: createHash('sha256').update(JSON.stringify(children)).digest('hex'),
      };
    } else {
      throw new Error(`Unsupported public output node ${path}`);
    }
    outputNodes.set(rel, node);
    return node;
  }
  try {
    const receipts: EnsureBuiltReceipt['tasks'][number][] = [];
    for (const task of tasks) {
      if (task.hash === undefined) return failed(`No current hash for ${task.id}`);
      const entries = task.outputs.length === 0 ? [] : expand(root, task.outputs);
      if (task.outputs.length > 0 && entries.length === 0) return failed(`No public outputs for ${task.id}`);
      const selected = new Set<string>();
      for (const entry of entries) {
        if (isAbsolute(entry) || entry.split('/').includes('..'))
          return failed(`Invalid public output ${entry} for ${task.id}`);
        // Root-to-entry parent directories must be real; do not follow a
        // parent link into an unrelated tree and sign its bytes.
        let parent = root;
        for (const segment of entry.split('/').slice(0, -1)) {
          parent = join(parent, segment);
          if (!observe(parent).isDirectory()) return failed(`Symlinked public output parent ${parent}`);
        }
        visit(join(root, entry));
        for (const path of outputNodes.keys()) {
          if (path === entry || path.startsWith(`${entry}/`)) selected.add(path);
        }
      }
      receipts.push({
        id: task.id,
        hash: task.hash,
        outputs: [...selected].sort().map((path) => {
          const node = outputNodes.get(path);
          if (node === undefined) throw new Error(`Lost output census node ${path}`);
          return node;
        }),
      });
    }
    const fileInputs = new Set(proof.files);
    const deletedSource = status.value
      .split('\0')
      .some((entry) => (entry[0] === 'D' || entry[1] === 'D') && !outputNodes.has(entry.slice(3)));
    const effectiveKind = deletedSource ? 'conservative-provider-source' : proof.kind;
    const dirty = dirtyProviderPaths(status.value).filter(
      (path) =>
        (!outputNodes.has(path) || fileInputs.has(path)) &&
        (effectiveKind === 'conservative-provider-source' || fileInputs.has(path)),
    );
    if (dirty.length > 0) return failed(`${effectiveKind}: provider source differs from ${head}: ${dirty.join(', ')}`);
    for (const file of fileInputs) {
      const expected = tracked.get(file);
      if (expected === undefined) return failed(`Nx file input is not in provider HEAD tree: ${file}`);
      const path = join(root, file);
      const stats = observe(path);
      const mode = stats.isSymbolicLink() ? '120000' : (stats.mode & 0o111n) !== 0n ? '100755' : '100644';
      if (mode !== expected.mode) return failed(`Nx file input mode differs from provider HEAD: ${file}`);
      if (!stats.isFile()) return failed(`Unproved linked or non-file input source closure: ${file}`);
      const oid = fileDigest(path, Number(stats.size), algorithm, true);
      if (oid !== expected.oid) return failed(`Nx file input bytes differ from provider HEAD: ${file}`);
    }
    const finalHead = git(root, ['rev-parse', '--verify', 'HEAD']);
    const finalStatus = git(root, ['status', '--porcelain=v1', '-z', '--untracked-files=all']);
    if (!finalHead.ok) return failed(finalHead.detail);
    if (!finalStatus.ok) return failed(finalStatus.detail);
    const newlyDirty = dirtyProviderPaths(finalStatus.value).some(
      (path) =>
        (!outputNodes.has(path) || fileInputs.has(path)) &&
        (effectiveKind === 'conservative-provider-source' || fileInputs.has(path)),
    );
    if (finalHead.value.trim() !== head || newlyDirty || !nodesUnchanged(observed)) {
      return failed('Provider source, revision or public output nodes moved during receipt capture');
    }
    return {
      disposition: 'hit',
      receipt: {
        workspaceRoot: root,
        head,
        headTree: tree.value.trim(),
        tasks: receipts,
        fileInputProvenance: effectiveKind,
        runtimeEnvironmentProvenance: 'nx-task-hash-only',
      },
    };
  } catch (error) {
    if (error instanceof Error) return failed(`Cannot prove provider source/public outputs: ${error.message}`);
    throw error;
  }
}

/** Admit only recipes emitted by the same owning toolchain generator.
 * No shell parsing or keyword approximation confers file-source provenance.
 */
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
  const allowed = new Set(
    directories.flatMap((directory) =>
      [
        'rustc -vV && cargo -V',
        'rustc -vV && cargo -V && cargo clippy -V',
        'rustc -vV && cargo -V && cargo clippy -V && cargo fmt --version',
        'rustc -vV && cargo -V && cargo nextest --version',
      ].map((command) => cargoRuntimeInput(directory, command).runtime),
    ),
  );
  const opaque = runtimes.filter((runtime) => !allowed.has(runtime));
  return opaque.length === 0
    ? { kind: 'bounded' }
    : { kind: 'opaque', detail: `Unbounded runtime source closure: ${opaque.join('; ')}` };
}
