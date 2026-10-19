/**
 * A fixture's own copy of a workspace's Nx installation, and the check that nothing wrote into it.
 *
 * The graph witness binds node stamps (ctime included) of every file in the closure of Nx, this plugin and
 * TypeScript (README "Ancestor directories bind..."). Linked from a workspace, that closure is the host's: on CI
 * its files are hardlinks in the shared Bun store, and any other task that installs, restores or cleans changes their
 * link count, so their ctime, so a fixture's installation identity, between two calls of one test. Where this plugin
 * is a developer link, the closure is also a live checkout that its own author edits and rebuilds while a consumer's
 * test reads it. A fixture must not take an input it does not own.
 *
 * So the closure is copied, exactly as the witness's own census resolves it from the workspace that names it: each
 * package once, with the census's own exclusions (`node_modules`, `.git`, `.nx`), each dependency edge relinked beside
 * its copy as Bun's isolated layout does, the top-level packages' bins in `.bin`. Every copy is a new inode, cloned
 * where the volume allows and written otherwise, so no link count is shared with anything outside its owner.
 *
 * A test file makes one copy, on first use, shared by its fixtures; retiring it fails, naming the changed nodes, if
 * any fixture wrote into it.
 */
import { constants } from 'node:fs';
import { cp, mkdir, readFile, rm, symlink } from 'node:fs/promises';
import { basename, dirname, join, relative } from 'node:path';
import { expandOutputs } from 'nx/src/native';
import { readModulePackageJson } from 'nx/src/utils/package-json';
import typia from 'typia';
import {
  type GraphInstallationCensus,
  graphInstallationCensus,
  graphInstallationIdentity,
} from './ensure-built-witness.js';

/** The configuration the census resolves: Nx, this plugin and TypeScript. */
const CENSUS_NX_JSON = { plugins: ['@smoothbricks/nx-plugin'] };

/** An installation made under `root`, and its identity when the copy was made. */
interface OwnedInstallation {
  readonly root: string;
  readonly identity: string;
}

/** The graph witness's installation identity resolved from `root`; an unproved installation fails loudly. */
function installationIdentity(root: string): string {
  let unproved = 'no detail';
  const identity = graphInstallationIdentity(
    root,
    CENSUS_NX_JSON,
    readModulePackageJson,
    expandOutputs,
    null,
    (detail) => {
      unproved = detail;
    },
  );
  if (identity === null) throw new Error(`the Nx installation resolved from ${root} is unproved: ${unproved}`);
  return identity;
}

/** Copy the installation closure `censusRoot` resolves into `root/node_modules`; `root` is an empty directory the
 * caller owns.
 *
 * Only the closure is read from `censusRoot`, never its proof: that installation is the host's, whose shared store
 * other tasks relink while the census reads it, which changes node stamps and would make every test that copies it
 * fail for the host's work rather than its own. The copy, which the fixture owns, is what gets proved.
 */
async function makeOwnedInstallation(censusRoot: string, root: string): Promise<OwnedInstallation> {
  let unread = 'no detail';
  const census = graphInstallationCensus(
    censusRoot,
    CENSUS_NX_JSON,
    readModulePackageJson,
    expandOutputs,
    null,
    (detail) => {
      unread = detail;
    },
  );
  if (census === null) throw new Error(`the Nx installation closure of ${censusRoot} cannot be read: ${unread}`);
  const { resolutions } = census;
  const modules = join(root, 'node_modules');
  // Each source package root's copy, inside a store directory whose node_modules also holds its dependency links.
  const copies = new Map<
    string,
    { readonly store: string; readonly copy: string; readonly bin: string | Record<string, string> | undefined }
  >();
  for (const [, , manifest] of resolutions) {
    if (manifest === null || copies.has(dirname(manifest))) continue;
    const { name, bin } = typia.json.assertParse<{ name: string; bin?: string | Record<string, string> }>(
      await readFile(manifest, 'utf8'),
    );
    const store = join(modules, '.store', String(copies.size), 'node_modules');
    copies.set(dirname(manifest), { store, copy: join(store, name), bin });
  }
  const copyOf = (source: string) => {
    const copied = copies.get(source);
    if (copied === undefined) throw new Error(`${source} resolves dependencies but is not in the census`);
    return copied;
  };
  await Promise.all(
    [...copies].map(([source, { copy }]) =>
      cp(source, copy, {
        recursive: true,
        verbatimSymlinks: true,
        mode: constants.COPYFILE_FICLONE,
        filter: (path) => path === source || !['node_modules', '.git', '.nx'].includes(basename(path)),
      }),
    ),
  );
  for (const [from, name, manifest] of resolutions) {
    if (manifest === null) continue;
    const link = join(from === censusRoot ? modules : copyOf(from).store, name);
    await mkdir(dirname(link), { recursive: true });
    await symlink(relative(dirname(link), copyOf(dirname(manifest)).copy), link, 'dir');
    if (from !== censusRoot) continue;
    const { bin } = copyOf(dirname(manifest));
    const commands = typeof bin === 'string' ? { [basename(name)]: bin } : (bin ?? {});
    for (const [command, path] of Object.entries(commands)) {
      await mkdir(join(modules, '.bin'), { recursive: true });
      await symlink(relative(join(modules, '.bin'), join(link, path)), join(modules, '.bin', command));
    }
  }
  return { root, identity: installationIdentity(root) };
}

/**
 * Remove an installation, failing, with the changed nodes named, if its identity is no longer the one it was made
 * with: the proof that its sharers only read it.
 */
async function retireOwnedInstallation({ root, identity }: OwnedInstallation): Promise<void> {
  try {
    const retired = installationIdentity(root);
    if (retired === identity) return;
    const entries = (census: string) => {
      const { nodes, files } = typia.json.assertParse<GraphInstallationCensus>(census);
      return new Set([
        ...nodes.map(({ path, stamp }) => `${path} ${stamp}`),
        ...files.map(({ path, sha256 }) => `${path} ${sha256}`),
      ]);
    };
    const made = entries(identity);
    const now = entries(retired);
    const changed = [
      ...[...made].filter((entry) => !now.has(entry)).map((entry) => `- ${entry}`),
      ...[...now].filter((entry) => !made.has(entry)).map((entry) => `+ ${entry}`),
    ];
    throw new Error(`a fixture changed the installation it shares, in ${root}:\n${changed.slice(0, 20).join('\n')}`);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

/** One test file's installation: made on first use and shared by its fixtures, retired by the file's `afterAll`. */
export interface TestFileInstallation {
  /** The `node_modules` a fixture workspace links. */
  readonly nodeModules: () => Promise<string>;
  /** Fails if a fixture wrote into the copy; a file that never used it has nothing to retire. */
  readonly retire: () => Promise<void>;
}

/**
 * The installation one test file owns: a copy of the closure `censusRoot` (the workspace whose Nx, plugin and
 * TypeScript the fixtures stand for) resolves, made in the empty directory `makeRoot` creates on first use. Call once
 * per file, at module scope: Bun runs many test files in one process, and each must own its copy.
 */
export function ownedInstallation(censusRoot: string, makeRoot: () => Promise<string>): TestFileInstallation {
  let made: Promise<OwnedInstallation> | undefined;
  return {
    nodeModules: async () => {
      made ??= makeRoot().then((root) => makeOwnedInstallation(censusRoot, root));
      return join((await made).root, 'node_modules');
    },
    retire: async () => {
      if (made !== undefined) await retireOwnedInstallation(await made);
    },
  };
}
