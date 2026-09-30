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
 * still relinks them: only a wrapper around bun itself could intercept that.
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
 * Like secret-references.ts, this is a managed raw script: shell entry imports
 * it before anything is installed, and `smoo monorepo update` loads the copy
 * the nx-plugin ships. Because both use the one implementation, the shell and
 * the command keep the same links.
 */

import {
  type Dirent,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  rmSync,
  symlinkSync,
} from 'node:fs';
import path from 'node:path';

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
 * Runs `install` with every developer link under `root` kept. The links in
 * place when it starts are exactly the links in place when it settles,
 * whether it resolved or rejected, less those that already dangled. It then
 * names them as `reportDeveloperLinks` does.
 */
export async function keepDeveloperLinks<T>(root: string, install: () => Promise<T>): Promise<T> {
  const { live, dangling } = findDeveloperLinks(root);
  for (const link of dangling) {
    rmSync(path.join(root, link.path), { force: true });
    console.error(`! removed developer link ${link.path}: its target ${resolvedTarget(root, link)} no longer exists`);
  }
  try {
    return await install();
  } finally {
    for (const link of live) {
      restoreLink(root, link);
    }
    describeDeveloperLinks(root, live);
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

/** A package linked from many members is named once, with its count. */
function describeDeveloperLinks(root: string, links: readonly DeveloperLink[]): void {
  if (links.length === 0) {
    return;
  }
  const counts = new Map<string, number>();
  for (const link of links) {
    const name = link.path.slice(link.path.lastIndexOf('node_modules/') + 'node_modules/'.length);
    const named = `${name} -> ${resolvedTarget(root, link)}`;
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
        (existsSync(target) ? live : dangling).push(link);
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
