import { createHash, randomUUID } from 'node:crypto';
import type { BigIntStats } from 'node:fs';
import {
  closeSync,
  lstatSync,
  openSync,
  readdirSync,
  readFileSync,
  readSync,
  realpathSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import type { NxJsonConfiguration } from 'nx/src/config/nx-json';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { expandOutputs, FileData, WorkspaceContext } from 'nx/src/native';
import type { PackageJson, readModulePackageJsonWithoutFallbacks } from 'nx/src/utils/package-json';
import typia from 'typia';
import type { GraphInputContractProof } from './graph-input-contract.js';

interface InstalledPackageManifest {
  readonly path: string;
  readonly packageJson: PackageJson;
}

/** The stat fields a node's identity is read from. */
export type StampFields = Pick<BigIntStats, 'dev' | 'ino' | 'mode' | 'size' | 'mtimeNs' | 'ctimeNs'>;

/** The stamp a node reads as now: device, inode, mode, size, mtime and ctime. What a recorded stamp is compared with. */
export function nodeStamp(stats: StampFields): string {
  return `${stats.dev}:${stats.ino}:${stats.mode}:${stats.size}:${stats.mtimeNs}:${stats.ctimeNs}`;
}

/** The file type and permission bits a {@link nodeStamp} records: what a copy of the node keeps of its stamp. */
export function stampMode(stamp: string): string {
  return stamp.split(':')[2];
}

/**
 * How an observed stamp compares with the recorded one: `same`; `ctime` when device, inode, mode, size and mtime are
 * kept and only the ctime moved (a same-mode chmod, which Bun's bin linking does to a shared store file on any
 * checkout's install) or was recorded `unproven` ({@link recordedStamp}); otherwise `moved`. A function of the two
 * stamps alone, so no filesystem or clock decides it.
 */
export function stampMove(recorded: string, observed: string): 'same' | 'ctime' | 'moved' {
  if (observed === recorded) return 'same';
  // nodeStamp ends with the ctime: everything before it is the identity a same-mode chmod keeps.
  const kept = observed.slice(0, observed.lastIndexOf(':')) === recorded.slice(0, recorded.lastIndexOf(':'));
  return kept ? 'ctime' : 'moved';
}

/**
 * How long after a node's last change a stamp read of it proves nothing about the next change: git's racily clean
 * rule (git's Documentation/technical/racy-git.txt), which the Nx archive repair already applies to Nx's own file
 * hashes (patches/README.md). A filesystem dates a change by the kernel's clock. On Linux, on a filesystem without
 * fine-grained timestamps such as ZFS, that is the coarse clock, which moves once a jiffy: 1 ms at CONFIG_HZ=1000, as
 * on CI's runner, and 10 ms at CONFIG_HZ=100, the slowest. A same-size rewrite in the tick a stamp was read in leaves
 * every field of it as it was. Node can read neither the jiffy nor a filesystem's granularity, and a shorter window
 * reopens the gap on a slower kernel, so this is the slowest tick, and no option.
 */
export const SAME_TICK_NS = 10_000_000n;

/** The time a census starts, read before its first stat: what {@link unproven} judges each node's change by. */
export function censusStart(): bigint {
  return BigInt(Date.now()) * 1_000_000n;
}

/**
 * Whether a census that started at `start` read a node within {@link SAME_TICK_NS} of its last change (the later of
 * its ctime and mtime, so a node dated ahead counts). Its stamp is then unproven, not unchanged: a rewrite in the same
 * tick could leave it as it is.
 */
export function unproven(stats: StampFields, start: bigint): boolean {
  return start - (stats.ctimeNs > stats.mtimeNs ? stats.ctimeNs : stats.mtimeNs) < SAME_TICK_NS;
}

/**
 * The stamp a census that started at `start` records for a later reader: its {@link nodeStamp}, or, when that is
 * {@link unproven}, the same with its ctime recorded as `unproven`. No stamp read later equals that, so the reader
 * re-reads the node rather than trust the stamp, as it does a node whose ctime alone moved ({@link stampMove}), or
 * counts it moved when it holds nothing to re-read it against. Only such a node costs a re-read.
 */
export function recordedStamp(stats: StampFields, start: bigint): string {
  return unproven(stats, start)
    ? `${stats.dev}:${stats.ino}:${stats.mode}:${stats.size}:${stats.mtimeNs}:unproven`
    : nodeStamp(stats);
}

export interface NodeWitness {
  readonly path: string;
  readonly stamp: string;
}

export interface WorkspaceSnapshot {
  readonly files: readonly FileData[];
  readonly nodes: readonly NodeWitness[];
  readonly stable: boolean;
}

export interface GraphWitness {
  readonly version: 2;
  readonly workspaceRoot: string;
  readonly environment: string;
  readonly installation: string;
  /** The census behind `installation` read these files in the tick they changed in: {@link GraphInstallationCensus}. */
  readonly installationUnproven: readonly string[];
  readonly snapshot: WorkspaceSnapshot;
  readonly projectGraph: ProjectGraph;
}

/** An absent, unreadable or malformed optional witness never authorizes reuse. */
export function readGraphWitness(path: string): GraphWitness | null {
  try {
    return typia.json.isParse<GraphWitness>(readFileSync(path, 'utf8'));
  } catch (error) {
    if (error instanceof SyntaxError || systemError(error)) return null;
    throw error;
  }
}

export function writeWitness(path: string, value: GraphWitness | ArtifactWitness | GraphInstallationCensus): void {
  const temporary = `${path}.${randomUUID()}`;
  try {
    writeFileSync(temporary, JSON.stringify(value), { flag: 'wx', mode: 0o600 });
    renameSync(temporary, path);
  } finally {
    rmSync(temporary, { force: true });
  }
}

/** Capture exact node identity and rehash new/replaced/rewritten nodes.
 * Nx's Unix archive uses whole-second mtimes, so allFileData alone cannot
 * prove an immediate same-second rewrite. An unbound archive hash is never
 * admitted: on the first capture every file is rehashed through Nx itself.
 * The nodes are recorded for later captures and readers (recordedStamp), so a
 * node this capture read in the tick it changed in is rehashed by the next.
 */
export function workspaceSnapshot(
  workspaceRoot: string,
  context: WorkspaceContext,
  previous: WorkspaceSnapshot | undefined,
): WorkspaceSnapshot {
  const start = censusStart();
  const files = context.allFileData();
  const prior = new Map(previous?.nodes.map((node) => [node.path, node.stamp]));
  const nodes: NodeWitness[] = [];
  // What this capture read of each node it records unproven: its own stability check compares what it read.
  const read = new Map<string, string>();
  const changed: string[] = [];
  let complete = true;
  function record(path: string, stats: BigIntStats): string {
    const stamp = nodeStamp(stats);
    if (unproven(stats, start)) read.set(path, stamp);
    nodes.push({ path, stamp: read.has(path) ? recordedStamp(stats, start) : stamp });
    return stamp;
  }
  for (const { file } of files) {
    const path = join(workspaceRoot, file);
    const stats = lstatSync(path, { bigint: true });
    let moved = prior.get(path) !== record(path, stats);
    if (stats.isSymbolicLink()) {
      const physical = realpathSync(path);
      const targetStats = lstatSync(physical, { bigint: true });
      moved ||= prior.get(physical) !== record(physical, targetStats);
      // A file table does not prove the membership of an external directory.
      if (!targetStats.isFile()) complete = false;
    }
    if (moved) changed.push(file);
  }
  if (changed.length > 0) context.incrementalUpdate(changed, []);
  const asRead = read.size === 0 ? nodes : nodes.map(({ path, stamp }) => ({ path, stamp: read.get(path) ?? stamp }));
  return { files: context.allFileData(), nodes, stable: complete && nodesUnchanged(asRead) };
}

export function nodesUnchanged(nodes: readonly NodeWitness[]): boolean {
  for (const node of nodes) {
    try {
      if (nodeStamp(lstatSync(node.path, { bigint: true })) !== node.stamp) return false;
    } catch (error) {
      if (systemError(error)) return false;
      throw error;
    }
  }
  return true;
}

/**
 * The witnessed nodes whose stamp is no longer the one read, named for a refusal (the first eight in witness order, a
 * gone one with its error code, and how many more), or null when none moved. A node whose device, inode, mode, size
 * and mtime are unchanged and only its ctime moved (a same-mode chmod, which Bun's bin linking does to a shared store
 * file on any checkout's install), or whose ctime was recorded unproven ({@link recordedStamp}), asks for a comparison,
 * not a verdict: `same` re-reads it, and one it finds as recorded has not changed. Without `same` every move counts.
 */
export function movedNodes(
  nodes: readonly NodeWitness[],
  same: (path: string) => boolean = () => false,
): string | null {
  const moved: string[] = [];
  for (const node of nodes) {
    try {
      const move = stampMove(node.stamp, nodeStamp(lstatSync(node.path, { bigint: true })));
      if (move === 'same') continue;
      if (move === 'moved' || !same(node.path)) moved.push(node.path);
    } catch (error) {
      if (!systemError(error)) throw error;
      moved.push(`${node.path} (${error.code})`);
    }
  }
  if (moved.length === 0) return null;
  return `${moved.slice(0, 8).join(', ')}${moved.length > 8 ? ` and ${moved.length - 8} more` : ''}`;
}

/** Membership and content are exact. Outputs do not exempt manifests or
 * newly created/deleted files; this graph witness has no output exclusions.
 */
export function snapshotsEqual(left: WorkspaceSnapshot, right: WorkspaceSnapshot): boolean {
  if (!left.stable || !right.stable || left.files.length !== right.files.length) return false;
  const hashes = new Map(left.files.map(({ file, hash }) => [file, hash]));
  return right.files.every(({ file, hash }) => hashes.get(file) === hash);
}

export function reusableGraph(
  witness: GraphWitness | null,
  workspaceRoot: string,
  environment: string,
  installation: string | null,
  snapshot: WorkspaceSnapshot,
): ProjectGraph | null {
  return witness !== null &&
    witness.version === 2 &&
    witness.workspaceRoot === workspaceRoot &&
    installation !== null &&
    witness.installation === installation &&
    witness.environment === environment &&
    snapshotsEqual(witness.snapshot, snapshot)
    ? witness.projectGraph
    : null;
}

/** What {@link graphInstallationIdentity} binds: resolution edges `[from, name, physical manifest]`, node stamps as
 * read and file hashes, read once. `unproven` names the files this census read in the tick they changed in
 * ({@link unproven}); it is not part of the identity, only what a later census that reuses these hashes must re-read.
 */
export interface GraphInstallationCensus {
  readonly resolutions: readonly (readonly [string, string, string | null])[];
  readonly nodes: readonly NodeWitness[];
  readonly files: readonly { readonly path: string; readonly sha256: string }[];
  readonly unproven: readonly string[];
}

/** Plugins with opaque external graph inputs cannot borrow this proof.
 * For the repository plugin and Nx, bind the entire owned module/file tree
 * and its dependency/optional/peer resolution closure, including each alias,
 * physical target and package manifest. SHA-256 binds every regular file's
 * bytes, including manifest metadata; pre/post node stamps still reject a
 * tree that moved while reading. A missing optional dependency is recorded too.
 */
export function graphInstallationIdentity(
  workspaceRoot: string,
  nxJson: NxJsonConfiguration,
  resolvePackage: typeof readModulePackageJsonWithoutFallbacks,
  expand: typeof expandOutputs,
  localInputs: GraphInputContractProof | null,
  onUnproved?: (detail: string) => void,
): string | null {
  const census = provenInstallationCensus(workspaceRoot, nxJson, resolvePackage, expand, localInputs, onUnproved);
  return census === null ? null : installationIdentityOf(census, localInputs);
}

/** The identity {@link graphInstallationIdentity} names for a proven census: what it read, never what it reused. */
export function installationIdentityOf(
  { resolutions, nodes, files }: GraphInstallationCensus,
  localInputs: GraphInputContractProof | null,
): string {
  return JSON.stringify({ resolutions, nodes, files, localInputs: localInputs?.identity ?? null });
}

/** The census behind a recorded identity ({@link installationIdentityOf}) and its unproven files, or `undefined` for
 * one that does not parse: a hash source for {@link graphInstallationCensus}, never a proof.
 */
export function recordedInstallationCensus(
  identity: string,
  unproven: readonly string[],
): GraphInstallationCensus | undefined {
  try {
    const recorded = typia.json.isParse<Omit<GraphInstallationCensus, 'unproven'>>(identity);
    return recorded === null
      ? undefined
      : { resolutions: recorded.resolutions, nodes: recorded.nodes, files: recorded.files, unproven };
  } catch (error) {
    if (error instanceof SyntaxError) return undefined;
    throw error;
  }
}

/** The census a caller recorded at `path`, `unproven` included: a hash source for {@link graphInstallationCensus}, never a
 * proof. An absent, unreadable or malformed record is no hash source, and the next census reads every file.
 */
export function readInstallationCensus(path: string): GraphInstallationCensus | undefined {
  try {
    return typia.json.isParse<GraphInstallationCensus>(readFileSync(path, 'utf8')) ?? undefined;
  } catch (error) {
    if (error instanceof SyntaxError || systemError(error)) return undefined;
    throw error;
  }
}

/**
 * The hash `census` read of the file at `path`, while `stats` is the stamp it read the file at and it did not read the
 * file in the tick it changed in ({@link unproven}); otherwise `undefined`, and the caller reads the bytes. A second
 * reader of a file the census holds takes this instead of reading it again: a write landing between two reads could
 * make them disagree, and a write since the census moves the stamp, so it is read again.
 */
export function censusFileHashes(
  census: GraphInstallationCensus,
): (path: string, stats: StampFields) => string | undefined {
  const unprovenFiles = new Set(census.unproven);
  const stamps = new Map(census.nodes.map(({ path, stamp }) => [path, stamp]));
  const hashes = new Map(census.files.map(({ path, sha256 }) => [path, sha256]));
  return (path, stats) =>
    unprovenFiles.has(path) || stamps.get(path) !== nodeStamp(stats) ? undefined : hashes.get(path);
}

/**
 * A {@link graphInstallationCensus} that no node moved during: every node is read again after the census, and one
 * whose stamp moved fails it, unless only its ctime moved and its bytes are the ones hashed. `previous` is what the
 * census may take hashes from. `null`, with `onUnproved` told why, for a closure that cannot be read or that moved.
 */
export function provenInstallationCensus(
  workspaceRoot: string,
  nxJson: NxJsonConfiguration,
  resolvePackage: typeof readModulePackageJsonWithoutFallbacks,
  expand: typeof expandOutputs,
  localInputs: GraphInputContractProof | null,
  onUnproved?: (detail: string) => void,
  previous?: GraphInstallationCensus,
): GraphInstallationCensus | null {
  const census = graphInstallationCensus(
    workspaceRoot,
    nxJson,
    resolvePackage,
    expand,
    localInputs,
    onUnproved,
    previous,
  );
  if (census === null) return null;
  try {
    // A file whose ctime alone moved is re-hashed: Bun's bin linking on any checkout's install re-chmods a shared
    // store file to the mode it already has (movedNodes).
    const hashes = new Map(census.files.map(({ path, sha256 }) => [path, sha256]));
    const moved = movedNodes(
      census.nodes,
      (path) => hashes.get(path) === createHash('sha256').update(readFileSync(path)).digest('hex'),
    );
    if (moved !== null) {
      onUnproved?.(`Installed module node changed during inspection: ${moved}`);
      return null;
    }
  } catch (error) {
    if (systemError(error) && ['EACCES', 'EPERM', 'EIO'].includes(error.code)) throw error;
    if (error instanceof Error) {
      onUnproved?.(error.message);
      return null;
    }
    throw error;
  }
  return census;
}

/**
 * One read of the installation closure {@link graphInstallationIdentity} binds, without its final check that no node
 * moved while it was read: what a caller that needs the closure, not a proof of it, reads. `null`, with `onUnproved`
 * told why, for a closure that cannot be read.
 *
 * Every census resolves every edge, lists every directory and stats every node; only file bytes are reused. A file
 * takes its hash from `previous` when its stamp (device, inode, mode, size, and mtime and ctime in nanoseconds) is
 * the one `previous` read and `previous` did not read it in the tick it changed in. A write moves a file's ctime to
 * the tick it happens in, so a file written since `previous` stat-ed it, even between that stat and its read, one
 * whose stamp `previous` lacks, and one `previous` recorded unproven are read again; `previous` need not be proven.
 * The key is the stamp, not a cheaper generation (a resolution, a manifest, an install marker): a rebuild of a
 * developer-linked package, this plugin's own `dist`, rewrites its files in place and moves none of those, so a census
 * keyed on them would describe a tree that is no longer there.
 */
export function graphInstallationCensus(
  workspaceRoot: string,
  nxJson: NxJsonConfiguration,
  resolvePackage: typeof readModulePackageJsonWithoutFallbacks,
  expand: typeof expandOutputs,
  localInputs: GraphInputContractProof | null,
  onUnproved?: (detail: string) => void,
  previous?: GraphInstallationCensus,
): GraphInstallationCensus | null {
  const plugins = (nxJson.plugins ?? []).map((plugin) => (typeof plugin === 'string' ? plugin : plugin.plugin));
  const unproved = plugins.filter(
    (plugin) => plugin !== '@smoothbricks/nx-plugin' && !localInputs?.plugins.includes(plugin),
  );
  if (unproved.length > 0) {
    onUnproved?.(`Unproved configured graph plugins: ${unproved.join(', ')}`);
    return null;
  }
  const start = censusStart();
  const nodes: NodeWitness[] = [];
  const files: { path: string; sha256: string }[] = [];
  const unprovenFiles: string[] = [];
  // The stamps `previous` read its files at, those it read in their change tick left out, and the hashes it read.
  const recordedUnproven = new Set(previous?.unproven);
  const recordedStamps = new Map<string, string>();
  for (const { path, stamp } of previous?.nodes ?? []) if (!recordedUnproven.has(path)) recordedStamps.set(path, stamp);
  const recordedHashes = new Map(previous?.files.map(({ path, sha256 }) => [path, sha256]));
  const buffer = Buffer.allocUnsafe(64 * 1024);
  const resolutions: [string, string, string | null][] = [];
  const visited = new Set<string>();
  const declaredDirectoryLinks = new Set<string>();
  const expandedRoots = new Set<string>();
  function walk(path: string, selected?: ReadonlySet<string>): void {
    const stats = lstatSync(path, { bigint: true });
    const stamp = nodeStamp(stats);
    nodes.push({ path, stamp });
    if (stats.isSymbolicLink()) {
      const physical = realpathSync(path);
      if (lstatSync(physical).isDirectory() && !declaredDirectoryLinks.has(path)) {
        throw new Error(`Unproved directory link in graph package: ${path}`);
      }
      walk(physical);
    } else if (stats.isDirectory()) {
      const visible = expandedRoots.has(path) ? undefined : selected;
      for (const name of readdirSync(path).sort()) {
        const child = join(path, name);
        if (
          name !== 'node_modules' &&
          name !== '.git' &&
          name !== '.nx' &&
          (visible === undefined || visible.has(child))
        )
          walk(child, visible);
      }
    } else if (!stats.isFile()) {
      throw new Error(`Unproved graph package node: ${path}`);
    } else {
      if (unproven(stats, start)) unprovenFiles.push(path);
      const recorded = recordedStamps.get(path) === stamp ? recordedHashes.get(path) : undefined;
      files.push({ path, sha256: recorded ?? fileHash(path) });
    }
  }
  function fileHash(path: string): string {
    const hash = createHash('sha256');
    const fd = openSync(path, 'r');
    try {
      for (let count = readSync(fd, buffer); count > 0; count = readSync(fd, buffer)) {
        hash.update(buffer.subarray(0, count));
      }
    } finally {
      closeSync(fd);
    }
    return hash.digest('hex');
  }
  function installedManifest(name: string, from: string): InstalledPackageManifest {
    try {
      return resolvePackage(name, [from]);
    } catch (original) {
      if (systemError(original) && ['EACCES', 'EPERM', 'EIO'].includes(original.code)) throw original;
      // A package may expose only subpaths: its dependency declaration still owns its installed manifest and files.
      for (const directory of createRequire(join(from, 'package.json')).resolve.paths(name) ?? []) {
        const path = join(directory, name, 'package.json');
        try {
          const packageJson = typia.json.assertParse<PackageJson>(readFileSync(path, 'utf8'));
          return { path, packageJson };
        } catch (error) {
          if (systemError(error) && ['ENOENT', 'ENOTDIR'].includes(error.code)) continue;
          throw error;
        }
      }
      throw original;
    }
  }
  function visit(name: string, from: string, optional: boolean): void {
    let resolved: InstalledPackageManifest;
    try {
      resolved = installedManifest(name, from);
    } catch (error) {
      if (optional && systemError(error) && error.code === 'MODULE_NOT_FOUND') {
        resolutions.push([from, name, null]);
        return;
      }
      throw error;
    }
    const physical = realpathSync(resolved.path);
    resolutions.push([from, name, physical]);
    if (visited.has(physical)) return;
    visited.add(physical);
    const packageRoot = dirname(physical);
    const manifest = resolved.packageJson;
    // Explicit publication owns this tree; unknown declarations keep the conservative full census.
    if (manifest.files !== undefined && manifest.files.length > 0) {
      const patterns = manifest.files.map((pattern) => {
        const negative = pattern.startsWith('!');
        const relativePattern = (negative ? pattern.slice(1) : pattern).replace(/^\/+/u, '') || '.';
        return negative ? `!${relativePattern}` : relativePattern;
      });
      const declared = new Set<string>();
      const literal = (pattern: string) => !/[*?[\]{}]/u.test(pattern);
      const validate = (entry: string) => {
        if (isAbsolute(entry) || entry.split(/[\\/]/u).includes('..'))
          throw new Error(`Published graph package path escapes its owner: ${entry}`);
      };
      const addDeclaration = (entry: string) => {
        validate(entry);
        if (literal(entry)) {
          const absolute = resolve(packageRoot, entry);
          declared.add(absolute);
          declaredDirectoryLinks.add(absolute);
        }
      };
      for (const pattern of patterns) {
        validate(pattern.startsWith('!') ? pattern.slice(1) : pattern);
        if (!pattern.startsWith('!')) addDeclaration(pattern);
      }
      const exported = (value: unknown): void => {
        if (typeof value === 'string') {
          if (value.startsWith('./')) {
            patterns.push(value);
            addDeclaration(value);
          }
        } else if (Array.isArray(value)) {
          for (const entry of value) exported(entry);
        } else if (typeof value === 'object' && value !== null) {
          for (const entry of Object.values(value)) exported(entry);
        }
      };
      exported(manifest.exports);
      const selected = new Set<string>([packageRoot]);
      const select = (entry: string) => {
        validate(entry);
        for (let path = resolve(packageRoot, entry); path !== packageRoot; path = dirname(path)) selected.add(path);
      };
      select('package.json');
      for (const path of declared) select(relative(packageRoot, path));
      for (const path of expand(packageRoot, patterns)) {
        select(path);
        expandedRoots.add(resolve(packageRoot, path));
      }
      walk(packageRoot, selected);
    } else {
      walk(packageRoot);
    }
    const dependencies = { ...manifest.peerDependencies, ...manifest.dependencies, ...manifest.optionalDependencies };
    for (const dependency of Object.keys(dependencies).sort()) {
      visit(
        dependency,
        packageRoot,
        manifest.optionalDependencies?.[dependency] !== undefined ||
          manifest.peerDependenciesMeta?.[dependency]?.optional === true,
      );
    }
  }
  try {
    for (const name of ['nx', ...plugins.filter((plugin) => plugin === '@smoothbricks/nx-plugin')])
      visit(name, workspaceRoot, false);
    for (const input of localInputs?.packages ?? []) visit(input.name, input.from, false);
    visit('typescript', workspaceRoot, true);
    return { resolutions, nodes, files, unproven: unprovenFiles };
  } catch (error) {
    if (systemError(error) && ['EACCES', 'EPERM', 'EIO'].includes(error.code)) throw error;
    // Installation uncertainty disables only graph reuse. Nx's authoritative
    // RPC remains responsible for diagnosing invalid plugin installations.
    if (error instanceof Error) {
      onUnproved?.(error.message);
      return null;
    }
    throw error;
  }
}

export interface ArtifactWitness {
  readonly version: 1;
  readonly workspaceRoot: string;
  readonly claims: string;
  readonly nodes: readonly (NodeWitness & { readonly taskId: string })[];
}

export function readArtifactWitness(path: string): ArtifactWitness | null {
  try {
    return typia.json.isParse<ArtifactWitness>(readFileSync(path, 'utf8'));
  } catch (error) {
    if (error instanceof SyntaxError || systemError(error)) return null;
    throw error;
  }
}

function systemError(error: unknown): error is Error & { code: string } {
  return error instanceof Error && 'code' in error && typeof error.code === 'string';
}
