import { randomUUID } from 'node:crypto';
import type { BigIntStats } from 'node:fs';
import {
  accessSync,
  constants,
  lstatSync,
  readdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import type { NxJsonConfiguration } from 'nx/src/config/nx-json';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { FileData, WorkspaceContext } from 'nx/src/native';
import type { readModulePackageJsonWithoutFallbacks } from 'nx/src/utils/package-json';
import typia from 'typia';

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

/** Plugins with opaque external graph inputs cannot borrow this proof.
 * For the repository plugin and Nx, bind the entire owned module/file tree
 * and its dependency/optional/peer resolution closure, including each alias,
 * physical target and package manifest. The manifest's node stamp binds its
 * name, version, exports and declared dependency membership to the bytes the
 * awaited graph consumed. A missing optional dependency is recorded too.
 */
export function graphInstallationIdentity(
  workspaceRoot: string,
  nxJson: NxJsonConfiguration,
  resolvePackage: typeof readModulePackageJsonWithoutFallbacks,
): string | null {
  const plugins = (nxJson.plugins ?? []).map((plugin) => (typeof plugin === 'string' ? plugin : plugin.plugin));
  if (plugins.some((plugin) => plugin !== '@smoothbricks/nx-plugin')) return null;
  const nodes: NodeWitness[] = [];
  const resolutions: [string, string, string | null][] = [];
  const visited = new Set<string>();
  const declaredDirectoryLinks = new Set<string>();
  function walk(path: string): void {
    const stats = lstatSync(path, { bigint: true });
    nodes.push({ path, stamp: nodeStamp(stats) });
    if (stats.isSymbolicLink()) {
      const physical = realpathSync(path);
      if (lstatSync(physical).isDirectory() && !declaredDirectoryLinks.has(path)) {
        throw new Error(`Unproved directory link in graph package: ${path}`);
      }
      walk(physical);
    } else if (stats.isDirectory()) {
      for (const name of readdirSync(path).sort()) {
        if (name !== 'node_modules' && name !== '.git' && name !== '.nx') walk(join(path, name));
      }
    } else if (!stats.isFile()) {
      throw new Error(`Unproved graph package node: ${path}`);
    } else {
      accessSync(path, constants.R_OK);
    }
  }
  function visit(name: string, from: string, optional: boolean): void {
    let resolved: ReturnType<typeof readModulePackageJsonWithoutFallbacks>;
    try {
      resolved = resolvePackage(name, [from]);
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
    // A package-owned published dist lane may live on its build volume. Its
    // link node and every physical target node remain part of this proof.
    if (manifest.files?.includes('dist')) declaredDirectoryLinks.add(join(packageRoot, 'dist'));
    walk(packageRoot);
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
    for (const name of ['nx', ...plugins]) visit(name, workspaceRoot, false);
    visit('typescript', workspaceRoot, true);
    return nodesUnchanged(nodes) ? JSON.stringify({ resolutions, nodes }) : null;
  } catch (error) {
    if (systemError(error) && ['EACCES', 'EPERM', 'EIO'].includes(error.code)) throw error;
    // Installation uncertainty disables only graph reuse. Nx's authoritative
    // RPC remains responsible for diagnosing invalid plugin installations.
    if (error instanceof Error) return null;
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
