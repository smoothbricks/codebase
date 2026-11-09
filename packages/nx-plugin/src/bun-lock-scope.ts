import { existsSync } from 'node:fs';
import { join, posix } from 'node:path';

import {
  type CreateDependenciesContext,
  DependencyType,
  type ProjectGraph,
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
 * resolve to and to the node of each workspace member it depends on, the
 * root's manifest included. Nx's lockfile parser links each npm node to the
 * packages it depends on, so a target naming a node in its
 * `externalDependencies` is keyed on every package of that closure by the hash
 * bun.lock records for it, and on nothing else in the lockfile. That replaces
 * bun.lock as a file input: a lockfile edit re-keys the tasks that can load
 * what changed.
 *
 * A member's node has no edge to the root's. Bun's isolated linker links into
 * a directory's `node_modules` exactly the packages its manifest declares, but
 * Node's lookup from a member continues into the root's `node_modules`, so a
 * member can load any package the root declares. Two rules keep the member's
 * own closure the whole of what it loads: a source import of a package outside
 * that closure refuses the graph ({@link undeclaredImports}), and a tool a
 * target runs names each package it loads by name from a directory by the
 * scope that declares it ({@link bunLoadInput}). The root's own tooling (Nx
 * plugins and generators, release and mutation tooling) then keys only the
 * tasks that load it.
 *
 * The nodes carry no content of their own; Nx hashes a node's version and the
 * nodes it reaches.
 */
export const BUN_LOCK_NODE_PREFIX = 'bun-lock:';

/** The lock-scope node of the project at `projectRoot`. */
export function bunLockNodeName(projectRoot: string): string {
  return `${BUN_LOCK_NODE_PREFIX}${posix.normalize(projectRoot === '' ? '.' : projectRoot)}`;
}

/** The node of the workspace root manifest. */
export const BUN_ROOT_LOCK_NODE = bunLockNodeName('.');

/**
 * The named input every `package.json` project defines as its own lock-scope
 * input. `externalDependencies` takes no `{projectRoot}` token, so a
 * workspace-wide named input or target default names this to key each
 * project's targets on its own manifest's closure.
 */
export const BUN_LOCK_SCOPE_NAMED_INPUT = 'bunLockScope';
/** The input that keys a target on the bun.lock entries its project's manifest reaches. */
export function bunLockInput(projectRoot: string): { externalDependencies: string[] } {
  return { externalDependencies: [bunLockNodeName(projectRoot)] };
}

/** The node itself, published by its project; {@link bunLockDependencies} gives it its edges. */
export function bunLockNode(projectRoot: string): ProjectGraphExternalNode {
  const name = bunLockNodeName(projectRoot);
  return { type: 'bun', name, data: { packageName: name, version: '0.0.0' } };
}

/**
 * Every bun runtime-scope node's name starts with this; the rest is a project
 * root. It is the part of bun.lock a dependent loads through that package: the
 * closure of its `dependencies`, `optionalDependencies` and
 * `peerDependencies`. A package's devDependencies are installed for its own
 * targets only, so a dependent's lock-scope node reaches the runtime node of
 * each member it declares, never that member's own node.
 */
export const BUN_LOCK_RUNTIME_NODE_PREFIX = 'bun-lock-runtime:';

/** The runtime-scope node of the project at `projectRoot`. */
export function bunLockRuntimeNodeName(projectRoot: string): string {
  return `${BUN_LOCK_RUNTIME_NODE_PREFIX}${posix.normalize(projectRoot === '' ? '.' : projectRoot)}`;
}

/** The runtime-scope node itself, published by its project beside its lock-scope node. */
export function bunLockRuntimeNode(projectRoot: string): ProjectGraphExternalNode {
  const name = bunLockRuntimeNodeName(projectRoot);
  return { type: 'bun', name, data: { packageName: name, version: '0.0.0' } };
}

/** The manifest sections Nx reads a project's package dependencies from, in the order it lets them override. */
const DEPENDENCY_SECTIONS = ['optionalDependencies', 'peerDependencies', 'devDependencies', 'dependencies'] as const;
/** The sections a dependent loads through a package: all but its devDependencies. */
const RUNTIME_SECTIONS = ['optionalDependencies', 'peerDependencies', 'dependencies'] as const;

/** Each package a manifest declares in `sections`, with the specifier of the section Nx lets win. */
function declaredSpecifiers(
  manifest: Readonly<Record<string, unknown>>,
  sections: readonly string[] = DEPENDENCY_SECTIONS,
): Map<string, string> {
  const specifiers = new Map<string, string>();
  for (const section of sections) {
    const declared = manifest[section];
    if (typeof declared !== 'object' || declared === null) continue;
    for (const [name, specifier] of Object.entries(declared)) {
      if (typeof specifier === 'string') specifiers.set(name, specifier);
    }
  }
  return specifiers;
}

/**
 * The edges of every published bun lock-scope and runtime-scope node. A
 * manifest's dependencies are resolved exactly as Nx resolves a project's own
 * package edges (`buildExplicitPackageJsonDependencies`, one
 * `TargetProjectLocator` over the same projects and lockfile nodes): an
 * installed package becomes an edge to its npm node, a workspace member an
 * edge to that member's runtime node, and a package that resolves to no node
 * is skipped exactly as Nx skips it. The lock-scope node takes every section,
 * the runtime node all but devDependencies.
 */
export function bunLockDependencies(context: CreateDependenciesContext): RawProjectGraphDependency[] {
  const externalNodes = context.externalNodes ?? {};
  const nodes: Record<string, ProjectGraphProjectNode> = {};
  for (const [name, data] of Object.entries(context.projects)) {
    nodes[name] = { name, type: data.projectType === 'application' ? 'app' : 'lib', data };
  }
  const locator = new TargetProjectLocator(nodes, externalNodes);
  const dependencies: RawProjectGraphDependency[] = [];
  for (const project of Object.values(context.projects).sort((left, right) =>
    left.root < right.root ? -1 : left.root > right.root ? 1 : 0,
  )) {
    const manifestPath = posix.join(project.root, 'package.json');
    if (!existsSync(join(context.workspaceRoot, manifestPath))) continue;
    const manifest = readJsonFile<Record<string, unknown>>(join(context.workspaceRoot, manifestPath));
    for (const [source, sections] of [
      [bunLockNodeName(project.root), DEPENDENCY_SECTIONS],
      [bunLockRuntimeNodeName(project.root), RUNTIME_SECTIONS],
    ] as const) {
      if (externalNodes[source] === undefined) continue;
      const targets = new Set<string>();
      for (const [name, specifier] of declaredSpecifiers(manifest, sections)) {
        const member = locator.findDependencyInWorkspaceProjects(manifestPath, name, specifier);
        const memberRoot = member === null ? undefined : context.projects[member]?.root;
        const target =
          member === null
            ? locator.findNpmProjectFromImport(name, manifestPath)
            : memberRoot === undefined
              ? null
              : bunLockRuntimeNodeName(memberRoot);
        if (target !== null && target !== source && externalNodes[target] !== undefined) targets.add(target);
      }
      for (const target of [...targets].sort()) {
        dependencies.push({ source, target, type: DependencyType.static });
      }
    }
  }
  return dependencies;
}

/** A package a module loads by name, and the workspace-relative directory of that module. */
export interface PackageLoad {
  readonly directory: string;
  readonly name: string;
}

/** The package a bare specifier names: its scope and name, without the subpath. */
export function specifierPackage(specifier: string): string {
  const parts = specifier.split('/');
  return (specifier.startsWith('@') ? parts.slice(0, 2) : parts.slice(0, 1)).join('/');
}

/** One manifest at or above a directory: its project root and the packages it declares. */
export interface ManifestDeclarations {
  readonly root: string;
  readonly declared: ReadonlySet<string>;
}

/** The manifests at and above the workspace-relative `directory`, nearest first, ending with the root's. */
export function manifestsAbove(workspaceRoot: string, directory: string): ManifestDeclarations[] {
  const manifests: ManifestDeclarations[] = [];
  for (let current = posix.normalize(directory === '' ? '.' : directory); ; current = posix.dirname(current)) {
    const path = join(workspaceRoot, current, 'package.json');
    if (existsSync(path)) {
      manifests.push({ root: current, declared: new Set(declaredSpecifiers(readJsonFile(path)).keys()) });
    }
    if (current === '.') return manifests;
  }
}

/**
 * The `externalDependencies` entry that keys package `name` as a process loads
 * it by name from a directory, given the manifests at and above it, nearest
 * first; `null` when none declares it, so nothing on that path loads it. Node's
 * lookup walks the directory's ancestors' `node_modules`, and bun's isolated
 * linker links into each exactly what its manifest declares, so the nearest
 * declaring manifest decides what loads. A member's declaration is keyed by its
 * lock-scope node. The root's is named by itself, and Nx keys that package's
 * node and what it reaches: the root's node would key the whole of the root's
 * tooling.
 */
export function bunLoadScope(manifests: readonly ManifestDeclarations[], name: string): string | null {
  const declaring = manifests.find((manifest) => manifest.declared.has(name));
  if (declaring === undefined) return null;
  return declaring.root === '.' ? name : bunLockNodeName(declaring.root);
}

/** The lock scope of each package load, from the manifests above its directory; sorted, each once. */
export function bunLoadScopes(workspaceRoot: string, loads: readonly PackageLoad[]): string[] {
  const manifests = new Map<string, ManifestDeclarations[]>();
  const scopes = new Set<string>();
  for (const { directory, name } of loads) {
    let above = manifests.get(directory);
    if (above === undefined) {
      above = manifestsAbove(workspaceRoot, directory);
      manifests.set(directory, above);
    }
    const scope = bunLoadScope(above, name);
    if (scope !== null) scopes.add(scope);
  }
  return [...scopes].sort();
}

/** A target's one `externalDependencies` input: its project's lock-scope node first, then each other scope once, sorted. */
export function bunScopeInput(
  projectRoot: string,
  ...scopes: readonly (readonly string[])[]
): { externalDependencies: string[] } {
  const own = bunLockNodeName(projectRoot);
  return { externalDependencies: [own, ...[...new Set(scopes.flat())].filter((scope) => scope !== own).sort()] };
}

/** The input keying every package of `names` a process loads by name from the directory `manifests` stand above. */
export function bunLoadInput(
  manifests: readonly ManifestDeclarations[],
  names: Iterable<string>,
): { externalDependencies: string[] } {
  const scopes = new Set<string>();
  for (const name of names) {
    const scope = bunLoadScope(manifests, name);
    if (scope !== null) scopes.add(scope);
  }
  return { externalDependencies: [...scopes].sort() };
}

/**
 * The packages a TypeScript compile in a directory loads by name beyond its
 * sources' imports: every `@types` package a manifest at or above it declares
 * (TypeScript includes each ancestor's `node_modules/@types` unless the config
 * lists `types`, a narrower set), and the `tslib` an `importHelpers` compile
 * resolves.
 */
export function typescriptLoads(manifests: readonly ManifestDeclarations[]): string[] {
  const names = new Set(['tslib']);
  for (const manifest of manifests) {
    for (const name of manifest.declared) {
      if (name.startsWith('@types/')) names.add(name);
    }
  }
  return [...names].sort();
}

/** A project source import of a package its own lock-scope node does not reach. */
export interface UndeclaredImport {
  readonly project: string;
  readonly root: string;
  readonly packageName: string;
}

/**
 * The npm packages each workspace member's sources import (Nx's own `static`
 * and `dynamic` edges) that its lock-scope node does not reach. Node loads such
 * a package through an ancestor's `node_modules`, the root's, and the
 * project's targets, keyed on the member's own closure, would miss it. The
 * root's project is exempt: its node reaches everything its manifest declares.
 * So is a `@types` package: TypeScript loads it by ancestry, not Node, and
 * every TypeScript target keys the `@types` the manifests above it declare
 * ({@link typescriptLoads}).
 */
export function undeclaredImports(graph: ProjectGraph): UndeclaredImport[] {
  const externalNodes = graph.externalNodes ?? {};
  const findings: UndeclaredImport[] = [];
  for (const [project, node] of Object.entries(graph.nodes).sort(([left], [right]) => (left < right ? -1 : 1))) {
    const scope = bunLockNodeName(node.data.root);
    if (scope === BUN_ROOT_LOCK_NODE || externalNodes[scope] === undefined) continue;
    const reached = new Set<string>();
    for (let pending = [scope]; pending.length > 0; ) {
      const next: string[] = [];
      for (const name of pending) {
        if (reached.has(name)) continue;
        reached.add(name);
        for (const edge of graph.dependencies[name] ?? []) next.push(edge.target);
      }
      pending = next;
    }
    const missing = new Set<string>();
    for (const edge of graph.dependencies[project] ?? []) {
      if (edge.type === DependencyType.implicit) continue;
      const external = externalNodes[edge.target];
      if (external === undefined || external.type !== 'npm' || reached.has(edge.target)) continue;
      if (external.data.packageName.startsWith('@types/')) continue;
      missing.add(external.data.packageName);
    }
    for (const packageName of [...missing].sort()) findings.push({ project, root: node.data.root, packageName });
  }
  return findings;
}

export class UndeclaredImportError extends Error {
  constructor(readonly findings: readonly UndeclaredImport[]) {
    super(
      'Workspace members import packages their package.json does not declare. Node loads each through the root' +
        "'s node_modules, and no lock-scope node of the member keys it; declare each in the member's package.json:\n" +
        findings.map(({ project, root, packageName }) => `  ${project} (${root}): ${packageName}`).join('\n'),
    );
    this.name = 'UndeclaredImportError';
  }
}

export function refuseUndeclaredImports(graph: ProjectGraph): void {
  const findings = undeclaredImports(graph);
  if (findings.length > 0) throw new UndeclaredImportError(findings);
}
