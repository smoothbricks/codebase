#!/usr/bin/env bun
// The land's build-volume pick (specs/cowshed/16_build_volumes.md, "Picking a land's build volume"). Before the gate,
// land-host.sh prices the two images the gate could build on: the workspace's own build volume and main's latest seed,
// the one image `cowshed reseed <ws> --from main` would clone. Nx cacheable work is not priced: the localhost Nx cache
// serves both, and a seed's gate entries reached it when its land published them. What differs is Cargo: a unit is
// rebuilt when its sources differ from the tree its target directory was built at. The price of an image is the
// compile seconds of the workspace units the gate tree makes dirty in it, closed over the units that depend on them.
//
//   land-pick.ts price CHECKOUT BUILT_TREE RESEED_PREVIEW_JSON
//
// prints the pick (`seed` or `own`) on stdout and the priced report on stderr. Pure decisions are exported for tests.

import { spawnSync } from 'node:child_process';
import { lstatSync, readdirSync, readFileSync, readlinkSync, statSync } from 'node:fs';
import { basename, dirname, join, relative } from 'node:path';

/** Paths whose change makes Cargo's configuration differ for every unit: their effect on unit identity is not computed. */
const CONFIG_FILES = ['Cargo.toml', 'rust-toolchain', 'rust-toolchain.toml'];
const NESTED_CONFIG = /(^|\/)\.cargo\/config(\.toml)?$/;

/** A workspace member as the gate tree's `cargo metadata` resolves it. */
export interface Member {
  readonly name: string;
  /** The member's manifest directory, relative to the workspace root; '' is the root package. */
  readonly dir: string;
  /** Members that depend on this one directly, through any dependency kind. */
  readonly dependents: readonly string[];
  /** Every registry package (name@version) this member reaches, through members and registry packages alike. */
  readonly registry: ReadonlySet<string>;
}

/**
 * How far a change reaches inside a member: its library and every unit above it, or some of its test-only units,
 * named by Cargo's unit file name (test-integration-test-<target>) or by a prefix ending in '*' for all of a kind.
 */
export type Reach = { readonly kind: 'package' } | { readonly kind: 'units'; readonly units: ReadonlySet<string> };

/** The members an image would rebuild for the gate tree, or every member when Cargo's configuration changed. */
export type Dirty =
  | { readonly kind: 'members'; readonly members: ReadonlyMap<string, Reach> }
  | { readonly kind: 'all'; readonly because: string };

/** The unit kind each test-only directory of a member builds, by Cargo's unit file name prefix. */
const TEST_ONLY_UNITS: ReadonlyMap<string, string> = new Map([
  ['tests', 'test-integration-test-'],
  ['benches', 'test-bench-'],
  ['examples', 'example-'],
]);

/**
 * The test-only units a path inside a member rebuilds: dir/<target>.rs and dir/<target>/main.rs are that target's
 * own; any other file under the directory (a shared tests/common/mod.rs) may be any of its targets'. Undefined for a
 * path outside the test-only directories.
 */
export function testOnlyUnit(inner: string): string | undefined {
  const [dir = '', target, file, ...deeper] = inner.split('/');
  const prefix = TEST_ONLY_UNITS.get(dir);
  if (prefix === undefined || target === undefined) return undefined;
  if (file === undefined && target.endsWith('.rs')) return prefix + target.slice(0, -'.rs'.length);
  if (file === 'main.rs' && deeper.length === 0) return prefix + target;
  return `${prefix}*`;
}

/** The member owning a path: the one with the longest manifest directory that contains it. */
export function owningMember(members: readonly Member[], path: string): Member | undefined {
  let owner: Member | undefined;
  for (const member of members) {
    const inside = member.dir === '' || path === member.dir || path.startsWith(`${member.dir}/`);
    if (inside && (owner === undefined || member.dir.length > owner.dir.length)) owner = member;
  }
  return owner;
}

/** The text a configuration file means: its lines without blank lines and whole-line comments. */
export function configMeaning(text: string | undefined): string | undefined {
  if (text === undefined) return undefined;
  return text
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '' && !line.startsWith('#'))
    .join('\n');
}

/** Cargo.lock's package entries, keyed name@version, each mapped to its whole entry text. */
export function lockEntries(lock: string | undefined): ReadonlyMap<string, string> {
  const entries = new Map<string, string>();
  if (lock === undefined) return entries;
  for (const block of lock.split('[[package]]').slice(1)) {
    const name = /^name = "([^"]+)"$/m.exec(block)?.[1];
    const version = /^version = "([^"]+)"$/m.exec(block)?.[1];
    if (name !== undefined && version !== undefined) entries.set(`${name}@${version}`, block.trim());
  }
  return entries;
}

/** The registry packages whose locked entry the gate tree has and the built tree had otherwise, or not at all. */
export function changedLockEntries(
  before: ReadonlyMap<string, string>,
  after: ReadonlyMap<string, string>,
): Set<string> {
  const changed = new Set<string>();
  for (const [key, entry] of after) if (before.get(key) !== entry) changed.add(key);
  return changed;
}

/**
 * The dirty members of an image built at one tree, for the gate tree: each changed path's owner (its tests only, for
 * a path under its tests/, benches/ or examples/), each member reaching a registry package whose locked entry changed,
 * and every member depending on a dirty package, transitively. A configuration change dirties every member.
 */
export function dirtyMembers(
  members: readonly Member[],
  changedPaths: readonly string[],
  changedRegistry: ReadonlySet<string>,
  configChanged: (path: string) => boolean,
): Dirty {
  for (const path of changedPaths) {
    if ((CONFIG_FILES.includes(path) || NESTED_CONFIG.test(path)) && configChanged(path)) {
      return { kind: 'all', because: path };
    }
  }
  const byName = new Map(members.map((member) => [member.name, member]));
  const reach = new Map<string, Reach>();
  const pending: Member[] = [];
  const reachPackage = (member: Member): void => {
    if (reach.get(member.name)?.kind === 'package') return;
    reach.set(member.name, { kind: 'package' });
    pending.push(member);
  };
  for (const path of changedPaths) {
    if (path === 'Cargo.lock') continue;
    const owner = owningMember(members, path);
    if (owner === undefined) continue;
    const inner = owner.dir === '' ? path : relative(owner.dir, path);
    const unit = testOnlyUnit(inner);
    if (unit !== undefined) {
      const known = reach.get(owner.name);
      if (known === undefined) reach.set(owner.name, { kind: 'units', units: new Set([unit]) });
      else if (known.kind === 'units') reach.set(owner.name, { kind: 'units', units: new Set([...known.units, unit]) });
    } else {
      reachPackage(owner);
    }
  }
  for (const member of members) {
    for (const key of member.registry) {
      if (changedRegistry.has(key)) {
        reachPackage(member);
        break;
      }
    }
  }
  for (let member = pending.pop(); member !== undefined; member = pending.pop()) {
    for (const name of member.dependents) {
      const dependent = byName.get(name);
      if (dependent !== undefined) reachPackage(dependent);
    }
  }
  return { kind: 'members', members: reach };
}

/** One unit's compile seconds, from its newest fingerprint generation in a target directory. */
export interface UnitTime {
  /** Cargo's unit file name: lib-x, test-lib-x, test-integration-test-y, bin-z, build-script-build-script-build... */
  readonly unit: string;
  readonly seconds: number;
}

/** An image's price: the compile seconds of its dirty units, and the dirty members no fingerprint priced. */
export interface Price {
  readonly seconds: number;
  readonly units: number;
  readonly dirty: readonly string[];
  readonly unpriced: readonly string[];
}

export function price(
  dirty: Dirty,
  members: readonly Member[],
  times: ReadonlyMap<string, readonly UnitTime[]>,
): Price {
  const reach: ReadonlyMap<string, Reach> =
    dirty.kind === 'all'
      ? new Map(members.map((member) => [member.name, { kind: 'package' } as const]))
      : dirty.members;
  let seconds = 0;
  let units = 0;
  const unpriced: string[] = [];
  for (const [name, extent] of reach) {
    const all = times.get(name) ?? [];
    const counted =
      extent.kind === 'package'
        ? all
        : all.filter((time) =>
            [...extent.units].some((unit) =>
              unit.endsWith('*') ? time.unit.startsWith(unit.slice(0, -1)) : time.unit === unit,
            ),
          );
    if (counted.length === 0) unpriced.push(name);
    for (const time of counted) {
      seconds += time.seconds;
      units += 1;
    }
  }
  return { seconds, units, dirty: [...reach.keys()].sort(), unpriced: unpriced.sort() };
}

/** Which image the gate builds on, and what decided it. */
export type Pick =
  | { readonly pick: 'seed' | 'own'; readonly basis: 'cargo' }
  | { readonly pick: 'seed'; readonly basis: 'space' };

/**
 * The cheaper image wins; a tie keeps the workspace's own volume, so a land never replaces a volume for nothing.
 * When no fingerprint priced any unit (no Cargo target, or a Cargo-less project), space decides and main's seed wins:
 * unpriced workspace-only build state retains private blocks.
 */
export function pick(own: Price, seed: Price, timedUnits: number): Pick {
  if (timedUnits === 0) return { pick: 'seed', basis: 'space' };
  return { pick: seed.seconds < own.seconds ? 'seed' : 'own', basis: 'cargo' };
}

/** The reseed preview's seed id (`cowshed --json reseed <ws> --from main --dry-run`). */
export function previewSeed(preview: string): string | undefined {
  let envelope: unknown;
  try {
    envelope = JSON.parse(preview);
  } catch {
    return undefined;
  }
  if (typeof envelope !== 'object' || envelope === null || !('result' in envelope)) return undefined;
  const result: unknown = envelope.result;
  if (typeof result !== 'object' || result === null || !('seed' in result)) return undefined;
  return typeof result.seed === 'string' && /^[0-9a-f]{32}$/.test(result.seed) ? result.seed : undefined;
}

// ---- the shell: everything below reads the world and hands plain values to the decisions above ----

function fail(message: string): never {
  process.stderr.write(`land-pick: ${message}\n`);
  process.exit(1);
}

function run(command: string, args: readonly string[], cwd: string): string {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8', maxBuffer: 1 << 28 });
  if (result.error !== undefined) fail(`${command} ${args.join(' ')}: ${result.error.message}`);
  if (result.status !== 0) fail(`${command} ${args.join(' ')} exited ${result.status}: ${result.stderr}`);
  return result.stdout;
}

/** A file of a tree, or undefined when the tree has no such path. */
function treeFile(checkout: string, tree: string, path: string): string | undefined {
  const result = spawnSync('git', ['-C', checkout, 'cat-file', 'blob', `${tree}:${path}`], { encoding: 'utf8' });
  return result.status === 0 ? result.stdout : undefined;
}

/** The fields of `cargo metadata --format-version 1` the pick reads; Cargo documents the format as stable. */
interface CargoMetadata {
  readonly version: 1;
  readonly workspace_root: string;
  readonly workspace_members: readonly string[];
  readonly packages: readonly { readonly id: string; readonly name: string; readonly manifest_path: string }[];
  readonly resolve: {
    readonly nodes: readonly { readonly id: string; readonly deps: readonly { readonly pkg: string }[] }[];
  };
}

/** The gate tree's workspace members from `cargo metadata`, with their dependents and reachable registry packages. */
function readMembers(checkout: string): Member[] {
  const parsed: unknown = JSON.parse(run('cargo', ['metadata', '--format-version', '1', '--frozen'], checkout));
  if (typeof parsed !== 'object' || parsed === null || !('version' in parsed) || parsed.version !== 1) {
    fail('cargo metadata is not format version 1');
  }
  const metadata = parsed as CargoMetadata;
  const memberIds = new Set(metadata.workspace_members);
  const nameOf = new Map(metadata.packages.map((pkg) => [pkg.id, pkg.name]));
  const dirOf = new Map(
    metadata.packages.map((pkg) => [pkg.id, relative(metadata.workspace_root, dirname(pkg.manifest_path))]),
  );
  const depsOf = new Map(metadata.resolve.nodes.map((node) => [node.id, node.deps.map((dep) => dep.pkg)]));
  const reachCache = new Map<string, Set<string>>();
  const reachable = (id: string): Set<string> => {
    const known = reachCache.get(id);
    if (known !== undefined) return known;
    const found = new Set<string>();
    reachCache.set(id, found);
    for (const dep of depsOf.get(id) ?? []) {
      // A registry package id ends in #name@version, the key Cargo.lock entries are compared by.
      if (!memberIds.has(dep)) found.add(dep.slice(dep.lastIndexOf('#') + 1));
      for (const key of reachable(dep)) found.add(key);
    }
    return found;
  };
  const dependents = new Map<string, string[]>();
  for (const id of memberIds) {
    for (const dep of depsOf.get(id) ?? []) {
      if (memberIds.has(dep)) dependents.set(dep, [...(dependents.get(dep) ?? []), nameOf.get(id) ?? id]);
    }
  }
  return [...memberIds].map((id) => ({
    name: nameOf.get(id) ?? id,
    dir: dirOf.get(id) ?? '',
    dependents: dependents.get(id) ?? [],
    registry: reachable(id),
  }));
}

/**
 * Each member's units and compile seconds, read from the fingerprints of the checkout's host dev/test target
 * (target/debug/build/<package>/<unit hash>/fingerprint). Cargo writes invoked.timestamp before it starts the
 * compiler and the unit's <unit>.json fingerprint once the unit is built, so their modification times bound the
 * unit's compile; its dep-info is written at the start and says nothing. Of a unit's generations the one whose hash
 * file was used last wins: every build that uses a unit stamps that file (-Zmtime-on-use), and nothing else.
 */
function readUnitTimes(checkout: string, members: readonly Member[]): Map<string, UnitTime[]> {
  const build = join(checkout, 'target', 'debug', 'build');
  const times = new Map<string, UnitTime[]>();
  for (const member of members) {
    const newest = new Map<string, { readonly used: number; readonly seconds: number }>();
    let generations: string[];
    try {
      generations = readdirSync(join(build, member.name));
    } catch {
      continue;
    }
    for (const generation of generations) {
      const fingerprint = join(build, member.name, generation, 'fingerprint');
      let files: string[];
      try {
        files = readdirSync(fingerprint);
      } catch {
        continue;
      }
      const invoked = files.includes('invoked.timestamp')
        ? statSync(join(fingerprint, 'invoked.timestamp')).mtimeMs
        : undefined;
      if (invoked === undefined) continue;
      for (const file of files) {
        if (!files.includes(`${file}.json`)) continue;
        const used = statSync(join(fingerprint, file)).mtimeMs;
        const seconds = (statSync(join(fingerprint, `${file}.json`)).mtimeMs - invoked) / 1000;
        const known = newest.get(file);
        if (seconds >= 0 && (known === undefined || used > known.used)) newest.set(file, { used, seconds });
      }
    }
    times.set(
      member.name,
      [...newest].map(([unit, time]) => ({ unit, seconds: time.seconds })),
    );
  }
  return times;
}

/** Where a checkout's build volume sits: `.cowshed/build` names <mount root>/.build/<owner>/<repo>/<volume id>. */
function storeBuildDirectory(checkout: string): string {
  const link = readlinkSync(join(checkout, '.cowshed', 'build'));
  const repo = dirname(link);
  const owner = dirname(repo);
  // 01_storage.md, "Directory layout": /private/cowshed/store/<owner>/<repo>/build/<id>.dmg.json is each sidecar.
  return join('/private/cowshed/store', basename(owner), basename(repo), 'build');
}

function sidecarTree(path: string): string {
  const sidecar: unknown = JSON.parse(readFileSync(path, 'utf8'));
  if (typeof sidecar !== 'object' || sidecar === null || !('tree' in sidecar) || typeof sidecar.tree !== 'string') {
    fail(`${path} records no tree`);
  }
  return sidecar.tree;
}

function priceImage(
  checkout: string,
  builtTree: string,
  gateTree: string,
  members: readonly Member[],
  times: ReadonlyMap<string, readonly UnitTime[]>,
): Price & { readonly because?: string } {
  const changed = run('git', ['-C', checkout, 'diff-tree', '-r', '-z', '--name-only', builtTree, gateTree], checkout)
    .split('\0')
    .filter((path) => path !== '');
  const registry = changedLockEntries(
    lockEntries(treeFile(checkout, builtTree, 'Cargo.lock')),
    lockEntries(treeFile(checkout, gateTree, 'Cargo.lock')),
  );
  const dirty = dirtyMembers(
    members,
    changed,
    registry,
    (path) => configMeaning(treeFile(checkout, builtTree, path)) !== configMeaning(treeFile(checkout, gateTree, path)),
  );
  const priced = price(dirty, members, times);
  return dirty.kind === 'all' ? { ...priced, because: dirty.because } : priced;
}

if (import.meta.main) {
  const [verb, checkout, builtTree, preview] = process.argv.slice(2);
  if (verb !== 'price' || checkout === undefined || builtTree === undefined || preview === undefined) {
    fail('usage: land-pick.ts price CHECKOUT BUILT_TREE RESEED_PREVIEW_JSON');
  }
  const seedId = previewSeed(preview);
  if (seedId === undefined) fail(`the reseed preview names no seed: ${preview}`);
  const seedSidecar = join(storeBuildDirectory(checkout), `${seedId}.dmg.json`);
  if (!lstatSync(seedSidecar).isFile()) fail(`${seedSidecar} is not a file`);
  const seedTree = sidecarTree(seedSidecar);
  const gateTree = run('git', ['-C', checkout, 'rev-parse', 'HEAD^{tree}'], checkout).trim();
  const members = readMembers(checkout);
  const times = readUnitTimes(checkout, members);
  let timedUnits = 0;
  for (const units of times.values()) timedUnits += units.length;
  const own = priceImage(checkout, builtTree, gateTree, members, times);
  const seed = priceImage(checkout, seedTree, gateTree, members, times);
  const verdict = pick(own, seed, timedUnits);
  process.stderr.write(
    `${JSON.stringify({ ...verdict, gateTree, timedUnits, own: { tree: builtTree, ...own }, seed: { id: seedId, tree: seedTree, ...seed } })}\n`,
  );
  process.stdout.write(`${verdict.pick}\n`);
}
