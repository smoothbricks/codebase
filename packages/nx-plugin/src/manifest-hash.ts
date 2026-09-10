import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { basename, posix, relative, resolve, sep } from 'node:path';

import { parseJson } from 'nx/src/devkit-exports.js';
import { parse as parseToml, stringify as stringifyToml } from 'smol-toml';
import typia from 'typia';

import { isNonSourceDirectory } from './source-directories.js';

/**
 * A release rewrites `version` in every package manifest it publishes and in
 * the lockfile entry that mirrors it. Those files are inputs of the validation
 * targets, so the whole gate set misses its cache for a change that alters no
 * code — the publish run cannot reuse what CI just filled. Hashing the
 * manifests with only their own version removed keeps every other byte
 * load-bearing, so a `biome` or `typescript` bump still invalidates while a
 * version bump no longer does.
 *
 * This treatment belongs to checks that READ source, never to a target that
 * produces a shipped artifact. Rust embeds the version at compile time through
 * `env!("CARGO_PKG_VERSION")`, so a version-insensitive build hash would let a
 * post-bump build hit a pre-bump artifact and ship a binary that reports the
 * previous version — silent, and worse than the cache miss it saves. `build`,
 * `pack` and the cargo targets keep hashing the raw manifests.
 *
 * Nx discards a failing `runtime` input: the command's error goes nowhere, the
 * task runs, and the hash silently loses what that command contributed. With
 * the manifests excluded from the fileset that is a stale hit on a real
 * dependency change, invisible even under `NX_VERBOSE_LOGGING`. So nothing
 * here throws on a manifest it cannot read: every stripper falls back to the
 * raw text, which over-invalidates instead of under-invalidating.
 */
const DIGEST_TAG = 'versionless-manifests-v1';

/** The manifests a release rewrites for the workspace as a whole. */
const WORKSPACE_MANIFESTS = ['package.json', 'bun.lock'];

const PACKAGE_MANIFEST = 'package.json';
const CARGO_MANIFEST = 'Cargo.toml';
const LOCKFILE = 'bun.lock';

/** A table whose own `version` a release rewrites. Every other field rides along untouched. */
interface VersionedTable {
  version?: unknown;
  [field: string]: unknown;
}

interface CargoManifest {
  package?: VersionedTable;
  workspace?: {
    package?: VersionedTable;
    [field: string]: unknown;
  };
  [table: string]: unknown;
}

interface Lockfile {
  workspaces?: Record<string, VersionedTable>;
  [field: string]: unknown;
}

const isVersionedTable = typia.createIs<VersionedTable>();
const isCargoManifest = typia.createIs<CargoManifest>();
const isLockfile = typia.createIs<Lockfile>();

/**
 * A package manifest without its own `version`, serialised canonically.
 *
 * Everything else survives, so a dependency range, an export map or a script
 * still reaches the digest. Key order does not: a manifest an installer
 * rewrote in a different order describes the same package.
 */
export function stripPackageJsonVersion(text: string): string {
  const parsed = parsedOrNull(text, parseJson);
  if (!isVersionedTable(parsed)) return text;
  return canonicalJson(withoutVersion(parsed));
}

/**
 * A crate manifest without the version the crate declares for itself.
 *
 * `[package].version` and the `[workspace.package].version` that members
 * inherit are the crate's own. A `version` under a dependency table names a
 * DIFFERENT crate's version and stays in the digest — dropping it would let a
 * genuinely different dependency serve a stale result.
 */
export function stripCargoTomlVersion(text: string): string {
  const parsed = parsedOrNull(text, parseToml);
  if (!isCargoManifest(parsed)) return text;
  // Assigning keys that already exist leaves them in document order, so the
  // serialised form differs from the original in the version alone.
  const stripped: CargoManifest = { ...parsed };
  if (parsed.package !== undefined) {
    stripped.package = withoutVersion(parsed.package);
  }
  if (parsed.workspace?.package !== undefined) {
    stripped.workspace = { ...parsed.workspace, package: withoutVersion(parsed.workspace.package) };
  }
  try {
    return stringifyToml(stripped);
  } catch {
    return text;
  }
}

/**
 * A lockfile without the versions its workspace members declare.
 *
 * The release writes each member's new version back into the lockfile, so the
 * lockfile alone would miss every validation task even with the package
 * manifests handled. Resolved external dependencies are untouched: they are
 * the reason the lockfile is an input at all.
 */
export function stripLockfileWorkspaceVersions(text: string): string {
  const parsed = parsedOrNull(text, parseJson);
  if (!isLockfile(parsed)) return text;
  const { workspaces } = parsed;
  if (workspaces === undefined) return canonicalJson(parsed);
  return canonicalJson({
    ...parsed,
    workspaces: Object.fromEntries(Object.entries(workspaces).map(([path, member]) => [path, withoutVersion(member)])),
  });
}

/**
 * The digest of one project's manifests with their own versions removed.
 *
 * Covers the project's `package.json` and every `Cargo.toml` under it, which
 * is exactly what the validation targets exclude from their filesets. A
 * missing `package.json` is not an error — a Rust-only project has none, and
 * hashing what exists keeps the digest defined. Paths are part of the digest,
 * so moving a crate is a change even when no manifest body differs.
 *
 * A crate that pins a workspace sibling by `path` + `version` still carries
 * that sibling's version, so a release does invalidate it. That is the honest
 * answer: the dependency the digest describes really did change.
 */
export async function hashVersionlessManifests(projectRoot: string, workspaceRoot: string): Promise<string> {
  const root = resolve(workspaceRoot, projectRoot);
  const manifests = await projectManifests(root);
  return digest(
    await Promise.all(
      manifests.map(async (path) => ({
        path: relative(root, path).split(sep).join(posix.sep),
        content: stripVersions(path, await readFile(path, 'utf8')),
      })),
    ),
  );
}

/**
 * The digest of the workspace-wide manifests with member versions removed.
 *
 * Every project's validation tasks share this one, so the command that
 * produces it carries no project argument and Nx runs it once for the graph
 * rather than once per project.
 */
export async function hashVersionlessWorkspaceManifests(workspaceRoot: string): Promise<string> {
  const entries: { path: string; content: string }[] = [];
  for (const name of WORKSPACE_MANIFESTS) {
    // A workspace without a bun lockfile is ordinary; an unreadable one is not.
    // Only absence is tolerated, so a permission fault surfaces as a failure
    // instead of hashing as "no lockfile".
    const content = await readFile(resolve(workspaceRoot, name), 'utf8').catch((error: unknown) => {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
      throw error;
    });
    if (content !== null) entries.push({ path: name, content: stripVersions(name, content) });
  }
  return digest(entries);
}

function stripVersions(path: string, content: string): string {
  const name = basename(path);
  if (name === PACKAGE_MANIFEST) return stripPackageJsonVersion(content);
  if (name === CARGO_MANIFEST) return stripCargoTomlVersion(content);
  if (name === LOCKFILE) return stripLockfileWorkspaceVersions(content);
  return content;
}

function digest(entries: { path: string; content: string }[]): string {
  const hash = createHash('sha256');
  hash.update(`${DIGEST_TAG}\0`);
  const ordered = [...entries].sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));
  for (const entry of ordered) {
    hash.update(`${entry.path}\0${entry.content}\0`);
  }
  return hash.digest('hex');
}

async function projectManifests(root: string): Promise<string[]> {
  const found: string[] = [];
  async function walk(directory: string): Promise<void> {
    // No `catch`: an unreadable directory would otherwise hash as an empty
    // manifest set, which is the silent-staleness failure this module exists
    // to avoid. Symlinked directories are not entries `isDirectory` reports,
    // so the walk cannot loop and matches what Nx keeps in its file map.
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = `${directory}${sep}${entry.name}`;
      if (entry.isDirectory()) {
        // The exclusions every source walk in this plugin uses, so the digest
        // covers the same files Nx keeps in its file map.
        if (!isNonSourceDirectory(entry.name)) await walk(path);
      } else if (entry.name === CARGO_MANIFEST || (entry.name === PACKAGE_MANIFEST && directory === root)) {
        found.push(path);
      }
    }
  }
  await walk(root);
  return found;
}

/**
 * The parsed manifest, or null when it cannot be read.
 *
 * Names the fail-safe direction all three strippers share: an unreadable
 * manifest is hashed raw, because a stripper that threw would take the
 * manifests out of the hash altogether and leave a stale cache hit behind.
 */
function parsedOrNull(text: string, parse: (text: string) => unknown): unknown {
  try {
    return parse(text);
  } catch {
    return null;
  }
}

function withoutVersion(table: VersionedTable): VersionedTable {
  const { version: _version, ...rest } = table;
  return rest;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (isVersionedTable(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalize(value[key])]),
    );
  }
  return value;
}
