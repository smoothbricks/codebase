/**
 * A fixture's own copy of a workspace's Nx installation, kept between runs, and the check that nothing wrote into it.
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
 * The copy is kept between runs, in the workspace's own tool cache, because making it is most of what a test file
 * costs: about 21,000 files, which clonefile cannot share across the volume between Bun's global store and any
 * directory a cowshed job may write (specs/cowshed/03_caches.md "Clone-materializing caches"). It is trusted only as
 * its census proves it: on first use in a run, the census of the host closure and the proven census of the copy are
 * compared package by package, by what each holds (resolution edges, node types and modes, file bytes), and a package
 * whose copy no longer holds what the host's holds is copied again. Both censuses take the hashes of files whose
 * stamps have not moved from the ones recorded beside the copy, so a run that changes nothing reads no file bytes.
 *
 * The copy lies beneath `censusRoot`'s `node_modules`, so a census of it that resolves any package outside the copy
 * proves nothing about it, and the copy is made again, whole, in a directory of its own (`installation-<uuid>`, named
 * in the record beside it). A package in the copy that requires a dependency it does not
 * declare still reaches the host's top-level `node_modules` the same way; the census binds declared edges only.
 *
 * A test file uses its copy, on first use, for all its fixtures; retiring it fails, naming the changed nodes, if any
 * fixture wrote into it.
 */
import { randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { cp, mkdir, readdir, readFile, realpath, rename, rm, symlink, writeFile } from 'node:fs/promises';
import { basename, dirname, join, relative } from 'node:path';
import { expandOutputs } from 'nx/src/native';
import { readModulePackageJson } from 'nx/src/utils/package-json';
import typia from 'typia';
import {
  type GraphInstallationCensus,
  graphInstallationCensus,
  installationIdentityOf,
  provenInstallationCensus,
  stampMode,
} from './ensure-built-witness.js';

/** The configuration the census resolves: Nx, this plugin and TypeScript. */
const CENSUS_NX_JSON = { plugins: ['@smoothbricks/nx-plugin'] };

/**
 * Where a workspace keeps its test files' copies: its tool cache, `node_modules/.cache`. Nothing sweeps it. A cowshed
 * fork clones it with the build volume and main adopts it at land (specs/cowshed/16_build_volumes.md); it goes cold
 * only when `node_modules` is reinstalled or the build volume reseeded.
 */
const KEPT = join('node_modules', '.cache', 'smoothbricks-nx-plugin', 'owned-installations');

/**
 * What is recorded beside a kept copy: where it is, the host closure's last census and the copy's last proven census.
 * Only a hash source for the next censuses; never a proof that the copy is current.
 */
interface KeptCensuses {
  readonly version: 2;
  /** The directory, beside this record, that holds the copy. */
  readonly installation: string;
  readonly host: GraphInstallationCensus;
  readonly copy: GraphInstallationCensus;
}

/** An installation kept under `root`, and its proven census when this run took it. */
interface OwnedInstallation {
  readonly root: string;
  readonly census: GraphInstallationCensus;
}

/** Each package root of the closure, where the copy keeps it, and the bins its manifest declares. */
interface Placement {
  /** `node_modules/.store/<index>/node_modules`, numbered in resolution order: also where its dependencies link. */
  readonly store: string;
  /** `<store>/<name>`: the same for host and copy. */
  readonly place: string;
  readonly bin: string | Record<string, string> | undefined;
}

/** What a census says the closure holds, named by place in the copy: its resolution edges, and each package's nodes. */
interface Holdings {
  readonly edges: string;
  readonly packages: ReadonlyMap<string, string>;
}

/** The proven installation census resolved from `root`, taking unmoved files' hashes from `previous`. */
function provenCensus(
  root: string,
  previous: GraphInstallationCensus | undefined,
  onUnproved: (detail: string) => void,
): GraphInstallationCensus | null {
  return provenInstallationCensus(
    root,
    CENSUS_NX_JSON,
    readModulePackageJson,
    expandOutputs,
    null,
    onUnproved,
    previous,
  );
}

/**
 * The proven census of the copy under `root`, or `null`, with `onUnproved` told why, for one that is unproved or
 * that resolves a package outside the copy. The copy lies in `censusRoot`'s `node_modules/.cache`, so where that is a
 * real directory, as on a fresh CI checkout, Node's ancestor lookup from the copy reaches the host's own
 * `node_modules`: a copy that is missing, or lacks a link, would otherwise be proved by a census of host packages.
 * Node also keeps every resolution for the life of the process, so once a census has resolved a host package from a
 * copy's directory, no copy made there again can be proved in that process: a copy made whole goes to a new directory.
 */
function copyCensus(
  root: string,
  previous: GraphInstallationCensus | undefined,
  onUnproved: (detail: string) => void,
): GraphInstallationCensus | null {
  const census = provenCensus(root, previous, onUnproved);
  const escaped = census?.resolutions.find(([, , manifest]) => manifest !== null && !manifest.startsWith(`${root}/`));
  if (escaped === undefined) return census;
  const [from, name, manifest] = escaped;
  onUnproved(`it resolves ${name} from ${from} to ${manifest}, outside the copy`);
  return null;
}

/** Where the copy keeps each package root the census of `censusRoot` resolves. */
async function placements({ resolutions }: GraphInstallationCensus): Promise<Map<string, Placement>> {
  const placed = new Map<string, Placement>();
  for (const [, , manifest] of resolutions) {
    if (manifest === null || placed.has(dirname(manifest))) continue;
    const { name, bin } = typia.json.assertParse<{ name: string; bin?: string | Record<string, string> }>(
      await readFile(manifest, 'utf8'),
    );
    const store = join('node_modules', '.store', String(placed.size), 'node_modules');
    placed.set(dirname(manifest), { store, place: join(store, name), bin });
  }
  return placed;
}

/**
 * What `census` holds, read from `root` with each package root in `places` named by its place in the copy. A node
 * outside every package root keeps its path, in the place `''` with the root's own edges.
 */
function holdings(census: GraphInstallationCensus, root: string, places: ReadonlyMap<string, string>): Holdings {
  const named = (path: string): readonly [string, string] => {
    for (let at = path; ; at = dirname(at)) {
      const place = at === root ? '' : places.get(at);
      if (place !== undefined) return [place, relative(at, path)];
      if (dirname(at) === at) return ['', path];
    }
  };
  const packages = new Map<string, unknown[]>();
  const hold = (path: string, value: string) => {
    const [place, inner] = named(path);
    const held = packages.get(place);
    if (held === undefined) packages.set(place, [[inner, value]]);
    else held.push([inner, value]);
  };
  for (const { path, stamp } of census.nodes) hold(path, stampMode(stamp));
  for (const { path, sha256 } of census.files) hold(path, sha256);
  return {
    edges: JSON.stringify(
      census.resolutions.map(([from, name, manifest]) => [
        named(from),
        name,
        manifest === null ? null : named(manifest),
      ]),
    ),
    packages: new Map([...packages].map(([place, held]) => [place, JSON.stringify(held)])),
  };
}

/**
 * Each package of `packages` (the first eight) with the first entry in which what the copy holds is not what the host
 * closure holds: a path within the package and its mode or SHA-256, on each side.
 */
function differences(packages: readonly (readonly [string, Placement])[], host: Holdings, copy: Holdings): string {
  const entries = (held: string | undefined) => typia.json.assertParse<[string, string][]>(held ?? '[]');
  const entry = (held: [string, string] | undefined) =>
    held === undefined ? 'nothing' : `${held[0] || '.'} ${held[1]}`;
  const named = packages.slice(0, 8).map(([source, { place }]) => {
    const wanted = entries(host.packages.get(place));
    const found = entries(copy.packages.get(place));
    let at = 0;
    while (at < Math.max(wanted.length, found.length) && entry(wanted[at]) === entry(found[at])) at += 1;
    return `${source} (host: ${entry(wanted[at])}; copy: ${entry(found[at])})`;
  });
  return `${named.join(', ')}${packages.length > 8 ? ` and ${packages.length - 8} more` : ''}`;
}

async function readKept(path: string): Promise<KeptCensuses | null> {
  try {
    return typia.json.isParse<KeptCensuses>(await readFile(path, 'utf8'));
  } catch (error) {
    if (error instanceof SyntaxError || (error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
      return null;
    }
    throw error;
  }
}

async function writeKept(path: string, kept: KeptCensuses): Promise<void> {
  const temporary = `${path}.${randomUUID()}`;
  try {
    await writeFile(temporary, JSON.stringify(kept), { flag: 'wx', mode: 0o600 });
    await rename(temporary, path);
  } finally {
    await rm(temporary, { force: true });
  }
}

/** Copy the host package root `source` to its place under `root`, with the census's own exclusions. */
function copyPackage(source: string, root: string, { place }: Placement): Promise<void> {
  return cp(source, join(root, place), {
    recursive: true,
    verbatimSymlinks: true,
    mode: constants.COPYFILE_FICLONE,
    filter: (path) => path === source || !['node_modules', '.git', '.nx'].includes(basename(path)),
  });
}

/** Link every resolution edge of `host` beside its copy under `root`, and the top-level packages' bins in `.bin`. */
async function linkCopies(
  censusRoot: string,
  root: string,
  { resolutions }: GraphInstallationCensus,
  placed: ReadonlyMap<string, Placement>,
): Promise<void> {
  const placementOf = (source: string) => {
    const placement = placed.get(source);
    if (placement === undefined) throw new Error(`${source} resolves dependencies but is not in the census`);
    return placement;
  };
  const modules = join(root, 'node_modules');
  for (const [from, name, manifest] of resolutions) {
    if (manifest === null) continue;
    const link = join(from === censusRoot ? modules : join(root, placementOf(from).store), name);
    const { place, bin } = placementOf(dirname(manifest));
    await mkdir(dirname(link), { recursive: true });
    await symlink(relative(dirname(link), join(root, place)), link, 'dir');
    if (from !== censusRoot) continue;
    const commands = typeof bin === 'string' ? { [basename(name)]: bin } : (bin ?? {});
    for (const [command, path] of Object.entries(commands)) {
      await mkdir(join(modules, '.bin'), { recursive: true });
      await symlink(relative(join(modules, '.bin'), join(link, path)), join(modules, '.bin', command));
    }
  }
}

/**
 * Remove whatever sits in the copy's `node_modules` beside its store, bins and top-level links: a census binds none
 * of it, and a run starts with the layout alone, as a fresh copy would.
 */
async function clearLeftovers(
  censusRoot: string,
  root: string,
  { resolutions }: GraphInstallationCensus,
): Promise<void> {
  const own = new Set(['.store', '.bin']);
  for (const [from, name, manifest] of resolutions) {
    if (from === censusRoot && manifest !== null) own.add(name.split('/')[0]);
  }
  const modules = join(root, 'node_modules');
  for (const entry of await readdir(modules)) {
    if (!own.has(entry)) await rm(join(modules, entry), { recursive: true, force: true });
  }
}

/**
 * The copy of the closure `censusRoot` resolves kept as `name`, proved to hold what the host closure holds, copying
 * again each package whose copy does not, and every package when the resolution edges moved or the copy is unproved.
 * Says so on stderr when a copy that was there fails its proof: a closure that keeps changing is itself worth knowing.
 *
 * Only the closure is read from `censusRoot`, never its proof: that installation is the host's, whose shared store
 * other tasks relink while the census reads it, which changes node stamps and would make every test that copies it
 * fail for the host's work rather than its own. The copy, which the fixture owns, is what gets proved.
 */
async function keptInstallation(censusRoot: string, name: string): Promise<OwnedInstallation> {
  await mkdir(join(censusRoot, KEPT, name), { recursive: true });
  const kept = await realpath(join(censusRoot, KEPT, name));
  const record = join(kept, 'censuses.json');
  const recorded = await readKept(record);
  let unread = 'no detail';
  const host = graphInstallationCensus(
    censusRoot,
    CENSUS_NX_JSON,
    readModulePackageJson,
    expandOutputs,
    null,
    (detail) => {
      unread = detail;
    },
    recorded?.host,
  );
  if (host === null) throw new Error(`the Nx installation closure of ${censusRoot} cannot be read: ${unread}`);
  const placed = await placements(host);
  const wanted = holdings(host, censusRoot, new Map([...placed].map(([source, { place }]) => [source, place])));
  const placesIn = (root: string) => new Map([...placed.values()].map(({ place }) => [join(root, place), place]));
  // Only a recorded copy is censused: a census of none would resolve the host's packages through the ancestor lookup.
  const current = recorded === null ? null : join(kept, recorded.installation);
  let unproved = 'no detail';
  const found =
    current === null
      ? null
      : copyCensus(current, recorded?.copy, (detail) => {
          unproved = detail;
        });
  const held = current === null || found === null ? null : holdings(found, current, placesIn(current));
  const stale =
    held === null || held.edges !== wanted.edges || held.packages.get('') !== wanted.packages.get('')
      ? null
      : [...placed].filter(([, { place }]) => held.packages.get(place) !== wanted.packages.get(place));
  if (current !== null && stale !== null) {
    await clearLeftovers(censusRoot, current, host);
    if (stale.length === 0 && found !== null) {
      await writeKept(record, { version: 2, installation: basename(current), host, copy: found });
      return { root: current, census: found };
    }
  }
  if (current !== null) {
    const why =
      held === null
        ? `its census is unproved (${unproved})`
        : stale === null
          ? 'its resolution edges are not the host closure’s'
          : `${stale.length} of ${placed.size} packages no longer hold what the host closure holds: ${differences(
              stale,
              wanted,
              held,
            )}`;
    process.stderr.write(`kept Nx installation ${current} failed its proof, copying again: ${why}\n`);
  }
  // A copy made whole goes to a directory no census in this process has resolved from (copyCensus).
  const root = current !== null && stale !== null ? current : join(kept, `installation-${randomUUID()}`);
  if (root === current && stale !== null) {
    for (const [, { place }] of stale) await rm(join(root, place), { recursive: true, force: true });
    await Promise.all(stale.map(([source, placement]) => copyPackage(source, root, placement)));
  } else {
    for (const entry of await readdir(kept)) {
      if (entry !== 'censuses.json') await rm(join(kept, entry), { recursive: true, force: true });
    }
    await Promise.all([...placed].map(([source, placement]) => copyPackage(source, root, placement)));
    await linkCopies(censusRoot, root, host, placed);
  }
  const census = copyCensus(root, root === current ? (found ?? undefined) : undefined, (detail) => {
    unproved = detail;
  });
  if (census === null) throw new Error(`the Nx installation copied to ${root} is unproved: ${unproved}`);
  const copied = holdings(census, root, placesIn(root));
  const moved = [...placed].filter(([, { place }]) => copied.packages.get(place) !== wanted.packages.get(place));
  if (copied.edges !== wanted.edges || moved.length > 0) {
    // The host's own installs relink its shared store while it is read: the copy is proved as made, and the next run
    // compares it with the host closure again.
    process.stderr.write(
      `the Nx installation closure of ${censusRoot} moved while ${root} copied it: ${differences(moved, wanted, copied)}\n`,
    );
  }
  await writeKept(record, { version: 2, installation: basename(root), host, copy: census });
  return { root, census };
}

/**
 * End a test file's use of its installation, failing, with the changed nodes named, if its identity is no longer the
 * one it was taken with: the proof that its sharers only read it. A write moves a node's stamp, which the identity
 * binds, so the census that proves it reads again only the files whose stamps moved. The copy stays for the next run,
 * which proves it against the host closure before using it.
 */
function retireOwnedInstallation({ root, census }: OwnedInstallation): void {
  let unproved = 'no detail';
  const retired = provenCensus(root, census, (detail) => {
    unproved = detail;
  });
  if (retired === null) throw new Error(`a fixture left the installation it shares unproved, in ${root}: ${unproved}`);
  if (installationIdentityOf(retired, null) === installationIdentityOf(census, null)) return;
  const entries = ({ nodes, files }: GraphInstallationCensus) =>
    new Set([
      ...nodes.map(({ path, stamp }) => `${path} ${stamp}`),
      ...files.map(({ path, sha256 }) => `${path} ${sha256}`),
    ]);
  const taken = entries(census);
  const now = entries(retired);
  const changed = [
    ...[...taken].filter((entry) => !now.has(entry)).map((entry) => `- ${entry}`),
    ...[...now].filter((entry) => !taken.has(entry)).map((entry) => `+ ${entry}`),
  ];
  throw new Error(`a fixture changed the installation it shares, in ${root}:\n${changed.slice(0, 20).join('\n')}`);
}

/** One test file's installation: taken on first use and shared by its fixtures, retired by the file's `afterAll`. */
export interface TestFileInstallation {
  /** The `node_modules` a fixture workspace links. */
  readonly nodeModules: () => Promise<string>;
  /** Fails if a fixture wrote into the copy; a file that never used it has nothing to retire. */
  readonly retire: () => Promise<void>;
}

/**
 * The installation one test file owns: the copy of the closure `censusRoot` (the workspace whose Nx, plugin and
 * TypeScript the fixtures stand for) resolves, kept as `name` in `censusRoot`'s tool cache and proved on first use.
 * Call once per file, at module scope, with a name no other file uses: Bun runs many test files in one process, and
 * each must own its copy. Two runs of one file at once share it, and a copy one of them makes again fails the other.
 */
export function ownedInstallation(censusRoot: string, name: string): TestFileInstallation {
  let taken: Promise<OwnedInstallation> | undefined;
  return {
    nodeModules: async () => {
      taken ??= keptInstallation(censusRoot, name);
      return join((await taken).root, 'node_modules');
    },
    retire: async () => {
      if (taken !== undefined) retireOwnedInstallation(await taken);
    },
  };
}
