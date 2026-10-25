import { existsSync } from 'node:fs';
import { join, posix } from 'node:path';

import {
  type CreateDependenciesContext,
  DependencyType,
  type ProjectGraphExternalNode,
  type ProjectGraphProjectNode,
  type RawProjectGraphDependency,
  readJsonFile,
} from 'nx/src/devkit-exports.js';
import { TargetProjectLocator } from 'nx/src/plugins/js/project-graph/build-dependencies/target-project-locator.js';

/**
 * Every bun lock-scope node's name starts with this; the rest is the root of
 * the project whose manifest it stands for, `.` for the workspace root.
 *
 * A lock-scope node is the part of bun.lock one manifest reaches: what the
 * package can load. Its edges go to the npm nodes the manifest's dependencies
 * resolve to; a member's also go to the node of each workspace member it
 * depends on and to the root's, since a member's `node_modules` lookup ends in
 * the root's. The root's node has no member edges: every member reaches it, so
 * one would key every member on the packages of whichever members the root
 * manifest lists, which a member loads only through its own declaration (or an
 * import Nx turns into a project edge, which a `^` input follows). Nx's
 * lockfile parser links each npm node to the packages it depends on, so a
 * target naming a node in its `externalDependencies` is keyed on every package
 * of that closure by the hash bun.lock records for it, and on nothing else in
 * the lockfile. That replaces bun.lock as a file input: a lockfile edit re-keys
 * the tasks that can load what changed.
 *
 * The nodes carry no content of their own; Nx hashes a node's version and the
 * nodes it reaches.
 */
export const BUN_LOCK_NODE_PREFIX = 'bun-lock:';

/** The lock-scope node of the project at `projectRoot`. */
export function bunLockNodeName(projectRoot: string): string {
  return `${BUN_LOCK_NODE_PREFIX}${posix.normalize(projectRoot === '' ? '.' : projectRoot)}`;
}

/** The node of the workspace root manifest, which every member's node reaches. */
export const BUN_ROOT_LOCK_NODE = bunLockNodeName('.');

/** The input that keys a target on the bun.lock entries its project's manifest reaches. */
export function bunLockInput(projectRoot: string): { externalDependencies: string[] } {
  return { externalDependencies: [bunLockNodeName(projectRoot)] };
}

/** The node itself, published by its project; {@link bunLockDependencies} gives it its edges. */
export function bunLockNode(projectRoot: string): ProjectGraphExternalNode {
  const name = bunLockNodeName(projectRoot);
  return { type: 'bun', name, data: { packageName: name, version: '0.0.0' } };
}

/** The manifest sections Nx reads a project's package dependencies from, in the order it lets them override. */
const DEPENDENCY_SECTIONS = ['optionalDependencies', 'peerDependencies', 'devDependencies', 'dependencies'] as const;

/**
 * The edges of every published bun lock-scope node. A manifest's dependencies
 * are resolved exactly as Nx resolves a project's own package edges
 * (`buildExplicitPackageJsonDependencies`, one `TargetProjectLocator` over
 * the same projects and lockfile nodes): an installed package becomes an edge
 * to its npm node, a workspace member (outside the root manifest) an edge to
 * that member's node, and a package that resolves to no node is skipped
 * exactly as Nx skips it.
 */
export function bunLockDependencies(context: CreateDependenciesContext): RawProjectGraphDependency[] {
  const externalNodes = context.externalNodes ?? {};
  const nodes: Record<string, ProjectGraphProjectNode> = {};
  for (const [name, data] of Object.entries(context.projects)) {
    nodes[name] = { name, type: data.projectType === 'application' ? 'app' : 'lib', data };
  }
  const locator = new TargetProjectLocator(nodes, externalNodes);
  const nodeOfMember = (member: string): string | null => {
    const root = context.projects[member]?.root;
    return root === undefined ? null : bunLockNodeName(root);
  };
  const dependencies: RawProjectGraphDependency[] = [];
  for (const project of Object.values(context.projects).sort((left, right) =>
    left.root < right.root ? -1 : left.root > right.root ? 1 : 0,
  )) {
    const source = bunLockNodeName(project.root);
    if (externalNodes[source] === undefined) continue;
    const manifestPath = posix.join(project.root, 'package.json');
    if (!existsSync(join(context.workspaceRoot, manifestPath))) continue;
    const manifest = readJsonFile<Record<string, unknown>>(join(context.workspaceRoot, manifestPath));
    const specifiers = new Map<string, string>();
    for (const section of DEPENDENCY_SECTIONS) {
      const declared = manifest[section];
      if (typeof declared !== 'object' || declared === null) continue;
      for (const [name, specifier] of Object.entries(declared)) {
        if (typeof specifier === 'string') specifiers.set(name, specifier);
      }
    }
    const targets = new Set<string>(source === BUN_ROOT_LOCK_NODE ? [] : [BUN_ROOT_LOCK_NODE]);
    for (const [name, specifier] of specifiers) {
      const member = locator.findDependencyInWorkspaceProjects(manifestPath, name, specifier);
      if (member !== null && source === BUN_ROOT_LOCK_NODE) continue;
      const target = member === null ? locator.findNpmProjectFromImport(name, manifestPath) : nodeOfMember(member);
      if (target !== null && target !== source && externalNodes[target] !== undefined) targets.add(target);
    }
    for (const target of [...targets].sort()) {
      dependencies.push({ source, target, type: DependencyType.static });
    }
  }
  return dependencies;
}
