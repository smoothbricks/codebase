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

/** Identity of a previously read node, not a timestamp freshness threshold. */
export function nodeStamp(stats: BigIntStats): string {
  return `${stats.dev}:${stats.ino}:${stats.mode}:${stats.size}:${stats.mtimeNs}:${stats.ctimeNs}`;
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
  readonly version: 1;
  readonly workspaceRoot: string;
  readonly environment: string;
  readonly installation: string;
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

export function writeWitness(path: string, value: GraphWitness | ArtifactWitness): void {
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
 */
export function workspaceSnapshot(
  workspaceRoot: string,
  context: WorkspaceContext,
  previous: WorkspaceSnapshot | undefined,
): WorkspaceSnapshot {
  const files = context.allFileData();
  const prior = new Map(previous?.nodes.map((node) => [node.path, node.stamp]));
  const nodes: NodeWitness[] = [];
  const changed: string[] = [];
  let complete = true;
  for (const { file } of files) {
    const path = join(workspaceRoot, file);
    const stats = lstatSync(path, { bigint: true });
    const node = { path, stamp: nodeStamp(stats) };
    nodes.push(node);
    let moved = prior.get(path) !== node.stamp;
    if (stats.isSymbolicLink()) {
      const physical = realpathSync(path);
      const targetStats = lstatSync(physical, { bigint: true });
      const target = { path: physical, stamp: nodeStamp(targetStats) };
      nodes.push(target);
      moved ||= prior.get(physical) !== target.stamp;
      // A file table does not prove the membership of an external directory.
      if (!targetStats.isFile()) complete = false;
    }
    if (moved) changed.push(file);
  }
  if (changed.length > 0) context.incrementalUpdate(changed, []);
  return { files: context.allFileData(), nodes, stable: complete && nodesUnchanged(nodes) };
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
 * gone one with its error code, and how many more), or null when none moved.
 */
export function movedNodes(nodes: readonly NodeWitness[]): string | null {
  const moved: string[] = [];
  for (const node of nodes) {
    try {
      if (nodeStamp(lstatSync(node.path, { bigint: true })) !== node.stamp) moved.push(node.path);
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
    witness.version === 1 &&
    witness.workspaceRoot === workspaceRoot &&
    installation !== null &&
    witness.installation === installation &&
    witness.environment === environment &&
    snapshotsEqual(witness.snapshot, snapshot)
    ? witness.projectGraph
    : null;
}

/** What {@link graphInstallationIdentity} binds: resolution edges `[from, name, physical manifest]`, node stamps and
 * file hashes, read once.
 */
export interface GraphInstallationCensus {
  readonly resolutions: readonly (readonly [string, string, string | null])[];
  readonly nodes: readonly NodeWitness[];
  readonly files: readonly { readonly path: string; readonly sha256: string }[];
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
  const census = graphInstallationCensus(workspaceRoot, nxJson, resolvePackage, expand, localInputs, onUnproved);
  if (census === null) return null;
  try {
    for (const node of census.nodes) {
      if (nodeStamp(lstatSync(node.path, { bigint: true })) !== node.stamp) {
        onUnproved?.(`Installed module node changed during inspection: ${node.path}`);
        return null;
      }
    }
  } catch (error) {
    if (systemError(error) && ['EACCES', 'EPERM', 'EIO'].includes(error.code)) throw error;
    if (error instanceof Error) {
      onUnproved?.(error.message);
      return null;
    }
    throw error;
  }
  return JSON.stringify({ ...census, localInputs: localInputs?.identity ?? null });
}

/** One read of the installation closure {@link graphInstallationIdentity} binds, without its final check that no node
 * moved while it was read: what a caller that needs the closure, not a proof of it, reads. `null`, with `onUnproved`
 * told why, for a closure that cannot be read.
 */
export function graphInstallationCensus(
  workspaceRoot: string,
  nxJson: NxJsonConfiguration,
  resolvePackage: typeof readModulePackageJsonWithoutFallbacks,
  expand: typeof expandOutputs,
  localInputs: GraphInputContractProof | null,
  onUnproved?: (detail: string) => void,
): GraphInstallationCensus | null {
  const plugins = (nxJson.plugins ?? []).map((plugin) => (typeof plugin === 'string' ? plugin : plugin.plugin));
  const unproved = plugins.filter(
    (plugin) => plugin !== '@smoothbricks/nx-plugin' && !localInputs?.plugins.includes(plugin),
  );
  if (unproved.length > 0) {
    onUnproved?.(`Unproved configured graph plugins: ${unproved.join(', ')}`);
    return null;
  }
  const nodes: NodeWitness[] = [];
  const files: { path: string; sha256: string }[] = [];
  const buffer = Buffer.allocUnsafe(64 * 1024);
  const resolutions: [string, string, string | null][] = [];
  const visited = new Set<string>();
  const declaredDirectoryLinks = new Set<string>();
  const expandedRoots = new Set<string>();
  function walk(path: string, selected?: ReadonlySet<string>): void {
    const stats = lstatSync(path, { bigint: true });
    nodes.push({ path, stamp: nodeStamp(stats) });
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
      const hash = createHash('sha256');
      const fd = openSync(path, 'r');
      try {
        for (let count = readSync(fd, buffer); count > 0; count = readSync(fd, buffer)) {
          hash.update(buffer.subarray(0, count));
        }
      } finally {
        closeSync(fd);
      }
      files.push({ path, sha256: hash.digest('hex') });
    }
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
    return { resolutions, nodes, files };
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
