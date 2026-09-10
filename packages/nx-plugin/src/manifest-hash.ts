import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { posix, relative, resolve, sep } from 'node:path';

import { parse as parseToml, stringify as stringifyToml } from 'smol-toml';
import typia from 'typia';

import { isNonSourceDirectory } from './source-directories.js';

/**
 * A release rewrites `version` in every manifest it publishes. Those manifests
 * are inputs of the validation targets, so a publish run misses the cache for
 * the whole gate set over a change that alters no code.
 *
 * Nx hashes JSON manifests with the version removed natively, through a `json`
 * input with `excludeFields` — no process, and the file hashing stays in the
 * native hasher. TOML has no such input, so crate manifests are the one
 * remaining case that needs a command, and this module is only that case. A
 * `runtime` input costs a process spawn per project per graph computation
 * (measured: Nx re-runs an identical command string 2-3 times rather than
 * memoizing it), so the plugin declares this one ONLY for projects that
 * actually carry a `Cargo.toml`.
 *
 * This treatment belongs to checks that READ source, never to a target that
 * produces a shipped artifact. A crate embeds its version at compile time
 * through `env!("CARGO_PKG_VERSION")`, so a version-insensitive build hash
 * would let a post-bump build hit a pre-bump artifact and ship a binary
 * reporting the previous version — silent, and worse than the cache miss it
 * saves.
 *
 * Nx discards a failing `runtime` input without failing the task, invisibly
 * even under `NX_VERBOSE_LOGGING`: excluding a manifest from a fileset with no
 * digest replacing it serves stale results in silence. So nothing here throws
 * on a manifest it cannot read — an unparseable manifest is hashed raw, which
 * over-invalidates instead of under-invalidating.
 */
const DIGEST_TAG = 'versionless-crate-manifests-v1';

const CARGO_MANIFEST = 'Cargo.toml';

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

const isCargoManifest = typia.createIs<CargoManifest>();

/**
 * A crate manifest without the version the crate declares for itself.
 *
 * `[package].version` and the `[workspace.package].version` that members
 * inherit are the crate's own. A `version` under a dependency table names a
 * DIFFERENT crate's version and stays in the digest — dropping it would let a
 * genuinely different dependency serve a stale result.
 */
export function stripCargoTomlVersion(text: string): string {
  let parsed: unknown;
  try {
    parsed = parseToml(text);
  } catch {
    // Hashing the raw text over-invalidates; dropping the manifest would leave
    // a stale hit behind, and Nx would report neither.
    return text;
  }
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
 * The digest of every crate manifest under a project, with the versions the
 * crates declare for themselves removed.
 *
 * This is exactly what the validation targets exclude from their filesets.
 * Paths are part of the digest, so moving a crate is a change even when no
 * manifest body differs. A project with no crate manifest never declares this
 * input at all, so the empty digest is not a case that reaches Nx.
 *
 * A crate that pins a workspace sibling by `path` + `version` still carries
 * that sibling's version, so a release does invalidate it. That is the honest
 * answer: the dependency the digest describes really did change.
 */
export async function hashVersionlessCrateManifests(projectRoot: string, workspaceRoot: string): Promise<string> {
  const root = resolve(workspaceRoot, projectRoot);
  const manifests = await crateManifests(root);
  const entries = await Promise.all(
    manifests.map(async (path) => ({
      path: relative(root, path).split(sep).join(posix.sep),
      content: stripCargoTomlVersion(await readFile(path, 'utf8')),
    })),
  );
  const hash = createHash('sha256');
  hash.update(`${DIGEST_TAG}\0`);
  for (const entry of entries.sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0))) {
    hash.update(`${entry.path}\0${entry.content}\0`);
  }
  return hash.digest('hex');
}

async function crateManifests(root: string): Promise<string[]> {
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
      } else if (entry.name === CARGO_MANIFEST) {
        found.push(path);
      }
    }
  }
  await walk(root);
  return found;
}

function withoutVersion(table: VersionedTable): VersionedTable {
  const { version: _version, ...rest } = table;
  return rest;
}
