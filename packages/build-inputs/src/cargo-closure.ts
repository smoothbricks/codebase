/**
 * What Cargo resolves for a build, read from `cargo metadata`: the one computation behind every statement of the part
 * of `Cargo.lock` a Cargo build reads. The nx-plugin keys a Cargo task on it (its `cargo-lock:` nodes) and a build's
 * input record states it beside the outputs (`cargoLockInputs`), so the key that restores an output and the record
 * that judges it cannot disagree about a lockfile entry.
 *
 * Cargo opens the whole lockfile and compiles only what the selected packages reach: an entry of another crate's
 * closure gives the artifact no byte. So neither statement holds the file; both hold the closure.
 */
import { dirname, sep } from 'node:path';

/** The fields of `cargo metadata --format-version 1` this module and the nx-plugin's resolution cache read. */
export interface CargoMetadata {
  packages: {
    id: string;
    name: string;
    version: string;
    source: string | null;
    manifest_path: string;
    targets: { src_path: string }[];
    /** Added by the nx-plugin's cache after Cargo identifies each local package's governing workspace. */
    governing_manifest_path?: string;
  }[];
  resolve: { nodes: { id: string; deps: { pkg: string; dep_kinds: { kind: CargoDependencyKind }[] }[] }[] } | null;
  workspace_members: string[];
  workspace_root: string;
}

/**
 * Narrowed field by field, not by typia: graph inference imports this module, and a workspace that loads the plugin
 * from its sources runs them without typia's compile-time transform.
 */
export function parseCargoMetadata(json: string): CargoMetadata {
  const member = (value: unknown, key: string): unknown =>
    typeof value === 'object' && value !== null && key in value ? Reflect.get(value, key) : undefined;
  const text = (value: unknown, field: string): string => {
    if (typeof value !== 'string') throw new Error(`cargo metadata reported no valid ${field}`);
    return value;
  };
  const list = (value: unknown, field: string): unknown[] => {
    if (!Array.isArray(value)) throw new Error(`cargo metadata reported no valid ${field}`);
    return value;
  };
  const document: unknown = JSON.parse(json);
  const resolveGraph = member(document, 'resolve');
  return {
    packages: list(member(document, 'packages'), 'packages').map((pkg) => ({
      id: text(member(pkg, 'id'), 'package id'),
      name: text(member(pkg, 'name'), 'package name'),
      version: text(member(pkg, 'version'), 'package version'),
      source: member(pkg, 'source') === null ? null : text(member(pkg, 'source'), 'package source'),
      manifest_path: text(member(pkg, 'manifest_path'), 'package manifest_path'),
      targets: list(member(pkg, 'targets'), 'package targets').map((target) => ({
        src_path: text(member(target, 'src_path'), 'target src_path'),
      })),
      ...(member(pkg, 'governing_manifest_path') === undefined
        ? {}
        : { governing_manifest_path: text(member(pkg, 'governing_manifest_path'), 'governing manifest') }),
    })),
    resolve:
      resolveGraph === null
        ? null
        : {
            nodes: list(member(resolveGraph, 'nodes'), 'resolve').map((node) => ({
              id: text(member(node, 'id'), 'resolve node id'),
              deps: list(member(node, 'deps'), 'resolve node deps').map((dependency) => ({
                pkg: text(member(dependency, 'pkg'), 'resolve edge'),
                dep_kinds: dependencyKinds(member(dependency, 'dep_kinds')),
              })),
            })),
          },
    workspace_members: list(member(document, 'workspace_members'), 'workspace_members').map((id) =>
      text(id, 'workspace member'),
    ),
    workspace_root: text(member(document, 'workspace_root'), 'workspace_root'),
  };
}

/** How a package declares a dependency: `[dependencies]` (null), `[dev-dependencies]` or `[build-dependencies]`. */
type CargoDependencyKind = null | 'dev' | 'build';

/** A resolve edge's `dep_kinds`. An edge Cargo reports with no kind is refused: read as dev-only, it would drop a dependency. */
function dependencyKinds(value: unknown): { kind: CargoDependencyKind }[] {
  if (!Array.isArray(value) || value.length === 0)
    throw new Error('cargo metadata reported no valid resolve edge kinds');
  return value.map((entry: unknown) => {
    const kind = typeof entry === 'object' && entry !== null ? Reflect.get(entry, 'kind') : undefined;
    if (kind !== null && kind !== 'dev' && kind !== 'build')
      throw new Error('cargo metadata reported no valid resolve edge kind');
    return { kind };
  });
}

/** One resolved dependency of a package: the package it resolved to, and whether every declaration of it is a dev one. */
export interface CargoEdge {
  readonly pkg: string;
  /** Cargo builds this dependency for the dependent's tests, examples and benches only. */
  readonly devOnly: boolean;
}

/** Every package Cargo builds for a selection, by package id, each with the dependencies Cargo builds for it. */
export type CargoClosure = ReadonlyMap<string, readonly string[]>;

/** A package `Cargo.lock` pins (registry, git, alternate registry) as the lockfile names it. */
export interface PinnedCargoPackage {
  readonly name: string;
  readonly version: string;
  /** The lockfile's `source`, which for a git dependency names the pinned commit that the package id alone does not. */
  readonly source: string;
}

/** The part of a Cargo resolve that decides which lockfile entries a selection reaches. Every path is canonical. */
export interface CargoLockResolve {
  /** The canonical Cargo workspace root, whose `Cargo.lock` the resolve read. */
  readonly root: string;
  /** The mutable (path) packages. */
  readonly local: readonly {
    readonly id: string;
    readonly name: string;
    readonly version: string;
    readonly manifest: string;
  }[];
  /** Every package `Cargo.lock` pins, by package id. */
  readonly pinned: ReadonlyMap<string, PinnedCargoPackage>;
  /** Every dependency edge by package id, or null when Cargo reported no resolution. */
  readonly edges: ReadonlyMap<string, readonly CargoEdge[]> | null;
}

/** Every dependency edge of `metadata` by package id, or null when Cargo reported no resolution. */
export function cargoEdges(metadata: CargoMetadata): Map<string, CargoEdge[]> | null {
  return metadata.resolve === null
    ? null
    : new Map(
        metadata.resolve.nodes.map((node) => [
          node.id,
          node.deps.map((dep) => ({ pkg: dep.pkg, devOnly: dep.dep_kinds.every(({ kind }) => kind === 'dev') })),
        ]),
      );
}

/** Every package of `metadata` that `Cargo.lock` pins, by package id. */
export function pinnedPackages(metadata: CargoMetadata): Map<string, PinnedCargoPackage> {
  return new Map(
    metadata.packages.flatMap((pkg) =>
      pkg.source === null ? [] : [[pkg.id, { name: pkg.name, version: pkg.version, source: pkg.source }] as const],
    ),
  );
}

/** The resolve of `metadata`, whose paths the caller has made canonical. */
export function cargoLockResolve(metadata: CargoMetadata): CargoLockResolve {
  return {
    root: metadata.workspace_root,
    local: metadata.packages
      .filter((pkg) => pkg.source === null)
      .map((pkg) => ({ id: pkg.id, name: pkg.name, version: pkg.version, manifest: pkg.manifest_path })),
    pinned: pinnedPackages(metadata),
    edges: cargoEdges(metadata),
  };
}

/** The local packages whose directory is `directory` (canonical) or lies under it. */
export function localPackagesUnder<Package extends { readonly manifest: string }>(
  local: readonly Package[],
  directory: string,
): Package[] {
  return local.filter((pkg) => {
    const at = dirname(pkg.manifest);
    return at === directory || at.startsWith(`${directory}${sep}`);
  });
}

/**
 * The packages Cargo compiles to build, test or run the `selected` ones and to build the `built` ones, each with the
 * dependencies Cargo builds for it. A selected package's targets reach every dependency kind: its library and binaries
 * the normal ones, its build script the build ones, its tests, examples and benches the dev ones. Below the selection,
 * and for a package named only as `built` (an artifact's own crates, which no test of it compiled), Cargo builds a
 * package for its library and build script alone, so its dev-dependencies never compile in and are not part of the
 * closure.
 */
export function buildClosure(
  cargo: Pick<CargoLockResolve, 'edges'>,
  selected: readonly string[],
  built: readonly string[] = [],
): CargoClosure {
  if (cargo.edges === null) throw new Error('cargo metadata reported no dependency resolution');
  const roots = new Set(selected);
  const reached = new Map<string, readonly string[]>();
  // A cursor over a growing list, not pop and a spread push: the record writer also runs compiled by scriptc, which
  // lowers neither on these values ("Array.prototype method is not supported yet").
  const pending = [...built, ...selected];
  for (let at = 0; at < pending.length; at += 1) {
    const id = pending[at];
    if (id === undefined || reached.has(id)) continue;
    const edges: string[] = [];
    for (const edge of cargo.edges.get(id) ?? []) {
      if (!roots.has(id) && edge.devOnly) continue;
      edges.push(edge.pkg);
      pending.push(edge.pkg);
    }
    reached.set(id, edges);
  }
  return reached;
}

/**
 * `closure` as `Cargo.lock` spells it: each package by the full form the lockfile gives a dependency
 * (`name version (source)`, a path package `name version`), to the packages its built dependencies resolved to, both
 * sorted. That is everything the lockfile decides for the closure, in terms a reader of the lockfile alone can check:
 * the entry is there, and its dependency list still resolves to each recorded package. A registry package's checksum
 * is left out as the task key leaves it out: a published version's bytes never change, and every `--locked` build
 * verifies them against the lockfile.
 */
export function lockClosure(cargo: CargoLockResolve, closure: CargoClosure): Record<string, string[]> {
  const spelled = new Map<string, string>();
  for (const pkg of cargo.local) spelled.set(pkg.id, `${pkg.name} ${pkg.version}`);
  for (const [id, pkg] of cargo.pinned) spelled.set(id, `${pkg.name} ${pkg.version} (${pkg.source})`);
  const spell = (id: string): string => {
    const spelling = spelled.get(id);
    if (spelling === undefined) throw new Error(`cargo metadata resolved ${id} and reported no package of that id`);
    return spelling;
  };
  const rows = new Map<string, Set<string>>();
  for (const [id, built] of closure) {
    const row = rows.get(spell(id)) ?? new Set<string>();
    for (const dependency of built) row.add(spell(dependency));
    rows.set(spell(id), row);
  }
  return Object.fromEntries(
    [...rows]
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([spelling, dependencies]) => [spelling, [...dependencies].sort()]),
  );
}
