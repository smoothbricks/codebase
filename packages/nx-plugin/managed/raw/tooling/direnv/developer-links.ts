/**
 * The links a developer points out of this checkout, kept across every
 * install smoo runs.
 *
 * `bun link <name>`, or a hand-made `ln -s`, replaces a dependency's entry in
 * a node_modules tree with a link to a local checkout of that package: how a
 * consumer runs against a build that is not published yet. Every `bun
 * install` re-points every declared dependency at the lockfile's version —
 * an install with nothing to do included, and before the root `preinstall`
 * script runs (measured on Bun 1.4.2), so no lifecycle script ever sees the
 * link. So each install smoo runs (shell entry's, a CI runner's
 * frozen-lockfile one, `smoo monorepo update`'s) records these links before
 * it installs and puts exactly those back afterwards. A plain `bun install`
 * still relinks an undeclared link: only a wrapper around bun itself could
 * intercept that. A declared one (below) comes back on its own.
 *
 * A developer link is a symlink directly inside a node_modules tree bun
 * installs (the root's or a workspace member's, scoped names included) whose
 * target lies outside the project root. The target is resolved against the
 * link's own directory and no further. Everything bun links there resolves
 * inside the root: workspace members by relative path, packages into
 * node_modules/.bun. Following the links further would misclassify: under
 * `globalStore`, each .bun entry is itself a link into the shared install
 * cache, outside every checkout.
 *
 * Nothing is recorded across installs, so the state is the filesystem itself:
 * unlinking a package means removing its link, and the next install puts the
 * lockfile's version there. A link whose target no longer exists is removed
 * before the install, with a warning, so that install puts the lockfile's
 * version in its place.
 *
 * An install that exists to replace links says which: `{ relink }` names the
 * packages (`name` or `@scope/name`) it is meant to relink, in the root's
 * tree and in every member's. When the install resolves, what it left at
 * those names stays (a `bun link` run against providers a developer
 * registered anew) and every other developer link is put back as before.
 * When it rejects, every link is put back, the named ones included, so a
 * failed relink changes nothing. A named package with no developer link when
 * the install starts has nothing to put back, and a named link that dangled
 * is removed first, as for any install.
 *
 * A repository can also DECLARE such links, so every checkout of it on a
 * developer machine gets them without anyone running `bun link`: the root
 * package.json's `smoo.developerLinks` maps a package name to a relative
 * directory. Contained targets belong to this checkout; genuine sibling
 * targets are anchored at the project's main checkout (see `linkAnchor`).
 * `applyDeclaredLinks` makes it so: in every node_modules tree that has the
 * package installed, the entry becomes a link to that directory, unless a
 * developer already pointed it at a checkout of their own. A declared
 * directory this machine does not have leaves the installed version, said so.
 * CI never applies them: it builds against the published packages the
 * lockfile names (setup-environment.ts `--links`).
 *
 * Declarations are applied at the end of every install. smoo's root script
 * policy makes the root's `postinstall` run this file, and bun runs the
 * root's lifecycle scripts after it has re-pointed the dependencies, an
 * install with nothing to do included. Without it a plain `bun install` left
 * the published package in place until the next shell entry, and a process
 * started in that gap kept what it loaded: an Nx daemon ran the published
 * plugin's sync generator for the rest of its life. Shell entry's `--links`
 * task covers a tree that lost a link without an install, such as a clone
 * made while it was missing. The postinstall takes no install lock: an
 * install smoo runs holds that lock while bun runs the script, so waiting
 * for it there would never end.
 *
 * Like secret-references.ts, this is a managed raw script: shell entry imports
 * it before anything is installed, and `smoo monorepo update` loads the copy
 * the nx-plugin ships. Because both use the one implementation, the shell and
 * the command keep the same links.
 */

import {
  type Dirent,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
} from 'node:fs';
import path from 'node:path';
import type { KeepDeveloperLinksOptions, PackageDependencyClosure } from '@smoothbricks/nx-plugin/managed-assets';

interface DeveloperLink {
  /** Where the link is, relative to the project root. */
  readonly path: string;
  /** The link's own text, restored byte for byte. */
  readonly target: string;
}

interface DeveloperLinks {
  readonly live: readonly DeveloperLink[];
  /** Links whose target no longer exists. */
  readonly dangling: readonly DeveloperLink[];
}

/**
 * Runs `install` while preserving the developer links captured under `root`.
 * On rejection every captured live link is restored, less links that already
 * dangled; a resolved install keeps its selected `relink` replacements.
 * This is not a transaction over new entries created by the install. Relink
 * intent reports the actual final link state on either outcome.
 */
export async function keepDeveloperLinks<T>(
  root: string,
  install: () => Promise<T>,
  options?: KeepDeveloperLinksOptions,
): Promise<T> {
  const { live, dangling } = findDeveloperLinks(root);
  for (const link of dangling) {
    rmSync(path.join(root, link.path), { force: true });
    console.error(`! removed developer link ${link.path}: its target ${resolvedTarget(root, link)} no longer exists`);
  }
  // Only an install that resolved leaves the links it was meant to replace as
  // it made them; a rejected one puts every captured live link back.
  let replaced: readonly string[] | undefined;
  try {
    const result = await install();
    replaced = options?.relink;
    return result;
  } finally {
    for (const link of live) {
      if (!replaced?.length || !replaced.includes(packageName(link))) {
        restoreLink(root, link);
      }
    }
    if (options !== undefined) {
      reportDeveloperLinks(root);
    } else {
      describeDeveloperLinks(root, live);
    }
  }
}

/**
 * One stderr line naming each linked package and the checkout it points at,
 * so a shell running a local build says so. Shell entry prints it on every
 * entry that installs nothing. The entry that installs gets the same line
 * from `keepDeveloperLinks`, but devenv's direnv integration runs that entry
 * where its output is not shown. A dangling link is left for the next
 * install to remove.
 */
export function reportDeveloperLinks(root: string): void {
  describeDeveloperLinks(root, findDeveloperLinks(root).live);
}

/** The package a link stands for in its tree: `name` or `@scope/name`. */
function packageName(link: DeveloperLink): string {
  return link.path.slice(link.path.lastIndexOf('node_modules/') + 'node_modules/'.length);
}

/** A package linked from many members is named once, with its count. */
function describeDeveloperLinks(root: string, links: readonly DeveloperLink[]): void {
  if (links.length === 0) {
    return;
  }
  const counts = new Map<string, number>();
  for (const link of links) {
    const named = `${packageName(link)} -> ${resolvedTarget(root, link)}`;
    counts.set(named, (counts.get(named) ?? 0) + 1);
  }
  const entries = [...counts].map(([named, count]) => (count === 1 ? named : `${named} (${count} links)`));
  console.error(`linked to local checkouts: ${entries.join(', ')}`);
}

function findDeveloperLinks(root: string): DeveloperLinks {
  const live: DeveloperLink[] = [];
  const dangling: DeveloperLink[] = [];
  for (const tree of nodeModulesTrees(root)) {
    for (const entry of packageLinks(root, tree)) {
      const link: DeveloperLink = { path: entry, target: readlinkSync(path.join(root, entry)) };
      const target = resolvedTarget(root, link);
      const relative = path.relative(root, target);
      if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
        (statSync(target, { throwIfNoEntry: false }) === undefined ? dangling : live).push(link);
      }
    }
  }
  return { live, dangling };
}

/**
 * The node_modules trees the last install wrote, relative to the root: the
 * root's and each workspace member's, as bun.lock names them. The lockfile,
 * not the manifest's workspace globs, because it is bun's own record of the
 * members it installed. A missing or unreadable lockfile leaves the root's
 * tree alone; the install that follows reports the lockfile in its own words.
 */
function nodeModulesTrees(root: string): string[] {
  let lock: unknown;
  try {
    lock = Bun.JSONC.parse(readFileSync(path.join(root, 'bun.lock'), 'utf8'));
  } catch {
    return ['node_modules'];
  }
  const workspaces = typeof lock === 'object' && lock !== null ? Reflect.get(lock, 'workspaces') : undefined;
  const members = typeof workspaces === 'object' && workspaces !== null ? Object.keys(workspaces) : [];
  return [...new Set(['', ...members])].map((member) => path.join(member, 'node_modules'));
}

/** Every symlink a package name could be in `tree`: `name` and `@scope/name`. */
function packageLinks(root: string, tree: string): string[] {
  const links: string[] = [];
  for (const entry of directoryEntries(path.join(root, tree))) {
    if (entry.name.startsWith('.')) {
      continue;
    }
    if (entry.isSymbolicLink()) {
      links.push(path.join(tree, entry.name));
    } else if (entry.name.startsWith('@') && entry.isDirectory()) {
      for (const scoped of directoryEntries(path.join(root, tree, entry.name))) {
        if (scoped.isSymbolicLink()) {
          links.push(path.join(tree, entry.name, scoped.name));
        }
      }
    }
  }
  return links;
}

function directoryEntries(directory: string): Dirent[] {
  try {
    return readdirSync(directory, { withFileTypes: true });
  } catch (error) {
    if (error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'ENOTDIR')) {
      return [];
    }
    throw error;
  }
}

function resolvedTarget(root: string, link: DeveloperLink): string {
  return path.resolve(root, path.dirname(link.path), link.target);
}

/** Puts `link` back unless the install left it as it was. */
function restoreLink(root: string, link: DeveloperLink): void {
  const at = path.join(root, link.path);
  let current: string | null;
  try {
    current = readlinkSync(at);
  } catch {
    // Nothing there, or no link: either way the install replaced it.
    current = null;
  }
  if (current === link.target) {
    return;
  }
  // What the install put there: a link into node_modules/.bun, or, under the
  // hoisted linker, a copied package directory. Removing a link never
  // follows it. The parent is created again for a dependency the install
  // dropped along with its scope directory.
  rmSync(at, { recursive: true, force: true });
  mkdirSync(path.dirname(at), { recursive: true });
  symlinkSync(link.target, at);
}

/** One tree entry a declaration wants pointed at its package's local checkout. */
export interface DeclaredLinkChange {
  /** Where the entry is, relative to the project root. */
  readonly path: string;
  /** The checkout directory it should link to, absolute. */
  readonly target: string;
}

/** A declaration this machine cannot honour: its checkout directory is not there to link to. */
export interface UnavailableDeclaredLink {
  readonly name: string;
  readonly target: string;
  readonly reason: string;
}

export interface DeclaredLinksPlan {
  readonly changes: readonly DeclaredLinkChange[];
  readonly unavailable: readonly UnavailableDeclaredLink[];
}

/**
 * What `smoo.developerLinks` asks of the trees under `root` that they do not
 * already hold. An entry needs a change when the package is installed there
 * (as the store link or directory bun made) and is not already a link to the
 * declared directory. An entry a developer pointed at another checkout of
 * their own stays theirs, and a tree without the package gains nothing: a
 * declaration links what the manifests depend on, never adds a dependency.
 */
export function planDeclaredLinks(root: string): DeclaredLinksPlan {
  const declared = readDeclaredLinks(root);
  if (declared.length === 0) {
    return { changes: [], unavailable: [] };
  }
  const anchor = linkAnchor(root);
  const trees = nodeModulesTrees(root);
  const changes: DeclaredLinkChange[] = [];
  const unavailable: UnavailableDeclaredLink[] = [];
  for (const [name, relative] of declared) {
    const target = declaredTarget(root, anchor, relative);
    const reason = unavailableReason(target);
    if (reason !== null) {
      unavailable.push({ name, target, reason });
      continue;
    }
    for (const tree of trees) {
      const entry = path.join(tree, name);
      const at = path.join(root, entry);
      const stats = lstatSync(at, { throwIfNoEntry: false });
      if (stats === undefined) {
        continue;
      }
      if (stats.isSymbolicLink()) {
        const current = resolvedTarget(root, { path: entry, target: readlinkSync(at) });
        const inside = path.relative(root, current);
        const foreign = inside === '..' || inside.startsWith(`..${path.sep}`) || path.isAbsolute(inside);
        // A cloned declaration's former main anchor is not a developer's independent choice.
        if (current === target || (foreign && current !== path.resolve(anchor, relative))) continue;
      }
      changes.push({ path: entry, target });
    }
  }
  return { changes, unavailable };
}

/**
 * Makes `plan`'s changes and says what it linked, once per package, and
 * which declarations this machine cannot honour.
 */
export function applyDeclaredLinks(root: string, plan: DeclaredLinksPlan): void {
  for (const { name, target, reason } of plan.unavailable) {
    console.error(`! smoo.developerLinks ${name}: ${target} ${reason}; the installed version stays`);
  }
  const counts = new Map<string, number>();
  for (const change of plan.changes) {
    const at = path.join(root, change.path);
    rmSync(at, { recursive: true, force: true });
    symlinkSync(change.target, at);
    const named = `${packageName(change)} -> ${change.target}`;
    counts.set(named, (counts.get(named) ?? 0) + 1);
  }
  if (counts.size > 0) {
    const entries = [...counts].map(([named, count]) => (count === 1 ? named : `${named} (${count} links)`));
    console.error(`linked declared packages to local checkouts: ${entries.join(', ')}`);
  }
}

/** Host-local binding belongs to activation, never to a portable cached package output. */
export function materializePackageDependencies(root: string, product: string, check = false): boolean {
  const packageRoot = realpathSync(product);
  const relativeRoot = path.relative(realpathSync(root), packageRoot);
  const owned =
    relativeRoot === '' ||
    (!relativeRoot.startsWith(`..${path.sep}`) && relativeRoot !== '..' && !path.isAbsolute(relativeRoot));
  const metadataPath = path.join(packageRoot, 'dependency-closure.json');
  if (!existsSync(metadataPath)) return true;
  const metadata = readDependencyClosure(metadataPath);
  const identity: unknown = JSON.parse(readFileSync(path.join(packageRoot, 'package.json'), 'utf8'));
  if (typeof identity !== 'object' || identity === null || Reflect.get(identity, 'name') !== metadata.packageName) {
    throw new Error(`${metadataPath}: package identity does not match its portable dependency closure`);
  }
  if (metadata.platform !== process.platform || metadata.arch !== process.arch) {
    throw new Error(`${metadataPath}: rebuild this declared product for ${process.platform}/${process.arch}`);
  }
  const cache = Bun.spawnSync(['bun', 'pm', 'cache'], { cwd: root, stdout: 'pipe', stderr: 'pipe' });
  if (cache.exitCode !== 0)
    throw new Error(`cannot resolve the installed Bun store: ${new TextDecoder().decode(cache.stderr)}`);
  const storeRoot = realpathSync(new TextDecoder().decode(cache.stdout).trim());
  for (const dependency of metadata.dependencies) {
    if (!dependency.direct) continue;
    const target = path.join(storeRoot, dependency.storePath);
    if (realpathSync(target) !== target)
      throw new Error(`${metadataPath}: dependency path redirects outside its immutable identity`);
    const installed: unknown = JSON.parse(readFileSync(path.join(target, 'package.json'), 'utf8'));
    if (
      typeof installed !== 'object' ||
      installed === null ||
      Reflect.get(installed, 'name') !== dependency.name ||
      Reflect.get(installed, 'version') !== dependency.version
    )
      throw new Error(`${metadataPath}: installed ${dependency.name} does not match its declared content identity`);
    const link = path.join(packageRoot, 'node_modules', dependency.name);
    const state = lstatSync(link, { throwIfNoEntry: false });
    if (state?.isSymbolicLink() && path.resolve(path.dirname(link), readlinkSync(link)) === target) continue;
    if (check) return false;
    if (!owned)
      throw new Error(
        `local product ${packageRoot} needs binding by its owning checkout; run its declared activation target`,
      );
    if (state && !state.isSymbolicLink()) throw new Error(`refuse to replace non-link dependency ${link}`);
    mkdirSync(path.dirname(link), { recursive: true });
    if (state) rmSync(link);
    symlinkSync(target, link, 'dir');
  }
  return true;
}

export function materializeDeclaredPackages(root: string, check = false): boolean {
  const anchor = linkAnchor(root);
  const targets = new Set(readDeclaredLinks(root).map(([, relative]) => declaredTarget(root, anchor, relative)));
  for (const target of targets) {
    if (unavailableReason(target) === null && !materializePackageDependencies(root, target, check)) return false;
  }
  return true;
}

/** Bootstrap runs before workspace validators are installed; retain the existing explicit boundary validation. */
function readDependencyClosure(file: string): PackageDependencyClosure {
  const value: unknown = JSON.parse(readFileSync(file, 'utf8'));
  if (typeof value !== 'object' || value === null) throw new Error(`${file}: expected a package dependency closure`);
  const version = Reflect.get(value, 'version');
  const packageName = Reflect.get(value, 'packageName');
  const platform = Reflect.get(value, 'platform');
  const arch = Reflect.get(value, 'arch');
  const records = Reflect.get(value, 'dependencies');
  if (
    version !== 1 ||
    typeof packageName !== 'string' ||
    !PACKAGE_NAME.test(packageName) ||
    typeof platform !== 'string' ||
    typeof arch !== 'string' ||
    !Array.isArray(records)
  ) {
    throw new Error(`${file}: invalid portable package identity`);
  }
  const relativePath = (value: unknown): string => {
    if (
      typeof value !== 'string' ||
      value.length === 0 ||
      path.isAbsolute(value) ||
      value.split(/[\\/]/).some((part) => part === '' || part === '.' || part === '..')
    ) {
      throw new Error(`${file}: dependency identity must be a contained Bun-store-relative path`);
    }
    return value;
  };
  const dependencies = records.map((record: unknown) => {
    if (typeof record !== 'object' || record === null) throw new Error(`${file}: invalid package dependency`);
    const name = Reflect.get(record, 'name');
    const version = Reflect.get(record, 'version');
    const storePath = Reflect.get(record, 'storePath');
    const bindingPaths = Reflect.get(record, 'bindingPaths');
    const direct = Reflect.get(record, 'direct');
    if (
      typeof name !== 'string' ||
      !PACKAGE_NAME.test(name) ||
      typeof version !== 'string' ||
      typeof direct !== 'boolean' ||
      !Array.isArray(bindingPaths)
    )
      throw new Error(`${file}: invalid dependency identity`);
    return { name, version, storePath: relativePath(storePath), bindingPaths: bindingPaths.map(relativePath), direct };
  });
  return { version, packageName, platform, arch, dependencies };
}

/** Why `target` cannot be linked to, or null when it is a directory this process can reach. */
function unavailableReason(target: string): string | null {
  try {
    return statSync(target).isDirectory() ? null : 'is not a directory';
  } catch (error) {
    if (error instanceof Error && 'code' in error) {
      if (error.code === 'ENOENT' || error.code === 'ENOTDIR') {
        return 'does not exist on this machine';
      }
      if (error.code === 'EACCES' || error.code === 'EPERM') {
        return 'cannot be read here';
      }
    }
    throw error;
  }
}

/** A package name as npm spells one: `name` or `@scope/name`, one segment each. */
const PACKAGE_NAME = /^(?:@[^/@\s.][^/\s]*\/)?[^/@\s.][^/\s]*$/;

/**
 * `smoo.developerLinks` from the root manifest: each package name with its
 * checkout directory, relative. Hand-validated, like secret-references.ts
 * validates `smoo.secrets`: this runs before anything is installed. The shape
 * is `PackageSmooConfig.developerLinks` in the nx-plugin's workspace-manifest.ts.
 */
function readDeclaredLinks(root: string): [string, string][] {
  const manifestPath = path.join(root, 'package.json');
  const manifest: unknown = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const smoo = typeof manifest === 'object' && manifest !== null ? Reflect.get(manifest, 'smoo') : undefined;
  const declared = typeof smoo === 'object' && smoo !== null ? Reflect.get(smoo, 'developerLinks') : undefined;
  if (declared === undefined) {
    return [];
  }
  if (typeof declared !== 'object' || declared === null || Array.isArray(declared)) {
    throw new Error(`${manifestPath}: smoo.developerLinks must map package names to checkout directories`);
  }
  return Object.entries(declared).map(([name, directory]) => {
    if (!PACKAGE_NAME.test(name)) {
      throw new Error(`${manifestPath}: smoo.developerLinks key ${JSON.stringify(name)} is not a package name`);
    }
    if (typeof directory !== 'string' || directory.length === 0 || path.isAbsolute(directory)) {
      throw new Error(
        `${manifestPath}: smoo.developerLinks ${name} must be a directory relative to the main checkout, not ${JSON.stringify(directory)}`,
      );
    }
    return [name, directory];
  });
}

/** Project-contained source products follow their checkout; siblings retain the shared main anchor. */
function declaredTarget(root: string, anchor: string, relative: string): string {
  const shared = path.resolve(anchor, relative);
  const inside = path.relative(anchor, shared);
  if (inside === '..' || inside.startsWith(`..${path.sep}`) || path.isAbsolute(inside)) return shared;
  const candidate = path.resolve(root, inside);
  const canonicalRoot = realpathSync(root);
  let existing = candidate;
  while (true) {
    try {
      const physical = path.relative(canonicalRoot, realpathSync(existing));
      if (physical === '..' || physical.startsWith(`..${path.sep}`) || path.isAbsolute(physical)) {
        throw new Error(`contained developer product escapes its owning checkout: ${candidate}`);
      }
      return candidate;
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
      const parent = path.dirname(existing);
      if (parent === existing) throw error;
      existing = parent;
    }
  }
}

/**
 * The stable anchor for genuine sibling directories is the project's main checkout.
 * A cowshed marker names it without reading main's tree. Contained source products
 * are projected into the current checkout by `declaredTarget`, with physical
 * containment checked against symlink escapes.
 */
function linkAnchor(root: string): string {
  let marker: unknown;
  try {
    marker = JSON.parse(readFileSync(path.join(root, '.cowshed', 'workspace.json'), 'utf8'));
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return root;
    }
    throw error;
  }
  const projectRoot = typeof marker === 'object' && marker !== null ? Reflect.get(marker, 'projectRoot') : undefined;
  return typeof projectRoot === 'string' && path.isAbsolute(projectRoot) ? projectRoot : root;
}

/**
 * The root package's `postinstall`, `bun tooling/direnv/developer-links.ts`.
 * Bun runs it in the root it installed, so that root is the working
 * directory. A CI runner links nothing, as for setup-environment.ts `--links`.
 */
if (import.meta.main && process.env.GITHUB_ACTIONS !== 'true') {
  const root = process.cwd();
  applyDeclaredLinks(root, planDeclaredLinks(root));
}
