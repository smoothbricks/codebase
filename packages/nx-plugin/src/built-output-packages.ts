import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import type {
  CreateDependenciesContext,
  CreateNodesContextV2,
  ProjectGraphExternalNode,
  RawProjectGraphDependency,
} from 'nx/src/devkit-exports.js';
import { hashArray } from 'nx/src/hasher/file-hasher.js';
import { readBunLockFile } from 'nx/src/plugins/js/lock-file/bun-parser.js';
import { getLockFileDependencies, getLockFileName, getLockFileNodes } from 'nx/src/plugins/js/lock-file/lock-file.js';
import { jsPluginConfig } from 'nx/src/plugins/js/utils/config.js';
import { detectPackageManager, type PackageManager } from 'nx/src/utils/package-manager.js';
import { nxVersion } from 'nx/src/utils/versions.js';

import { isRecord } from './type-guards.js';

/**
 * Built-output packages: lockfile package nodes that carry no version identity for the packages a workspace keys by
 * what its tasks run.
 *
 * Nx keys a task on an npm package through the package's external node, hashing the integrity the lockfile records for
 * it (`hash_external`). A `^` input adds the node of every package a dependency project declares or imports, from any
 * of its files, so a version bump of one package re-keys every task whose dependency closure reaches an importer of it.
 * That is wrong for a package the workspace already keys by its bytes where a task runs them, as a consumer keys the Nx
 * plugin per executor and module closure. The plugin's `builtOutputPackages` option names such packages.
 *
 * The workspace turns Nx's own lockfile analysis off (`pluginsConfig["@nx/js"].analyzeLockfile: false`): Nx merges its
 * default plugin's nodes after every configured plugin's, so they would replace these. This plugin then publishes the
 * nodes and package edges Nx's own parser makes (`getLockFileNodes`, `getLockFileDependencies`), every node of a named
 * package with a `data.hash` that names the package alone. Only that hash differs from Nx's graph: names, versions and
 * edges stay, so a task that reaches a named package still keys each of its dependencies by its own integrity.
 */

/** The `data.hash` of a named package's nodes: the package's name, the one identity Nx's graph keys it by. */
export function builtOutputHash(packageName: string): string {
  return `built-output:${packageName}`;
}

/** One read of the lockfile and Nx's parse of it. */
interface LockfileRead {
  readonly packageManager: PackageManager;
  readonly lockFile: string;
  readonly contents: string;
  readonly hash: string;
  readonly nodes: Record<string, ProjectGraphExternalNode>;
  readonly keyMap: Map<string, unknown>;
}

/**
 * The read whose nodes createNodes published last. An install can rewrite the lockfile between the two hooks, so
 * createDependencies links the nodes the graph holds, as Nx's own lockfile plugin does.
 */
let published: LockfileRead | undefined;

/** The packages the option names, or null when it names none. */
function namedPackages(options: unknown): ReadonlySet<string> | null {
  if (!isRecord(options) || options.builtOutputPackages === undefined) return null;
  const names: unknown = options.builtOutputPackages;
  if (!Array.isArray(names) || !names.every((name): name is string => typeof name === 'string' && name.length > 0)) {
    throw new Error('builtOutputPackages must be a list of package names');
  }
  return names.length === 0 ? null : new Set(names);
}

/** The workspace's lockfile, as Nx's own lockfile plugin reads it, and Nx's parse of it. */
function readLockfile(context: CreateNodesContextV2): LockfileRead {
  const packageManager = detectPackageManager(context.workspaceRoot);
  const lockFile = getLockFileName(packageManager);
  const path = join(context.workspaceRoot, lockFile);
  if (!existsSync(path)) throw new Error(`builtOutputPackages names packages, but ${path} does not exist`);
  const contents = packageManager === 'bun' ? readBunLockFile(path) : readFileSync(path, 'utf-8');
  const hash = hashArray([nxVersion, contents]);
  if (published?.hash === hash) return published;
  const { nodes, keyMap } = getLockFileNodes(packageManager, lockFile, contents, hash, context);
  return { packageManager, lockFile, contents, hash, nodes, keyMap };
}

/**
 * The lockfile's package nodes, each of a named package hashed by {@link builtOutputHash}; none when the option names
 * no package. Refuses while Nx's own nodes would replace them, and for a named package the lockfile installs nowhere.
 */
export function builtOutputLockfileNodes(
  options: unknown,
  context: CreateNodesContextV2,
): Record<string, ProjectGraphExternalNode> {
  const packages = namedPackages(options);
  if (packages === null) return {};
  if (jsPluginConfig(context.nxJsonConfiguration).analyzeLockfile) {
    throw new Error(
      `builtOutputPackages (${[...packages].join(', ')}) needs nx.json pluginsConfig["@nx/js"].analyzeLockfile ` +
        "set to false: Nx merges its own lockfile nodes after this plugin's, and they would key each of these " +
        'packages by its version',
    );
  }
  const read = readLockfile(context);
  published = read;
  const nodes: Record<string, ProjectGraphExternalNode> = {};
  const found = new Set<string>();
  for (const [name, node] of Object.entries(read.nodes)) {
    const packageName = node.data.packageName;
    if (!packages.has(packageName)) {
      nodes[name] = node;
      continue;
    }
    found.add(packageName);
    nodes[name] = { ...node, data: { ...node.data, hash: builtOutputHash(packageName) } };
  }
  const absent = [...packages].filter((name) => !found.has(name));
  if (absent.length > 0) {
    throw new Error(`builtOutputPackages names ${absent.join(', ')}, which ${read.lockFile} installs nowhere`);
  }
  return nodes;
}

/** The package edges Nx's parser reads from the lockfile whose nodes the graph holds; none when no package is named. */
export function builtOutputLockfileDependencies(
  options: unknown,
  context: CreateDependenciesContext,
): RawProjectGraphDependency[] {
  if (namedPackages(options) === null) return [];
  const read = published ?? readLockfile(context);
  return getLockFileDependencies(read.packageManager, read.lockFile, read.contents, read.hash, context, read.keyMap);
}
