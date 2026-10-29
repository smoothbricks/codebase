/**
 * Real-Cargo retention fixtures: a churned workspace whose superseded unit generations a pass must reclaim, and the
 * helpers that build it, inspect its target directory and run passes over it. This package's tests and nx-plugin's
 * tests of the retention hook inside a real Nx run share them.
 */
import { expect } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { lstat, mkdir, mkdtemp, readdir, readFile, readlink, realpath, rm, utimes, writeFile } from 'node:fs/promises';
import { delimiter, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import typia from 'typia';
import {
  RECENT_USE_MS,
  RETENTION_RECEIPT,
  type RetentionError,
  type RetentionOptions,
  type RetentionOutcome,
  type RetentionReport,
  type RetentionStart,
  retainCargoWorkspace,
} from '../cargo-retention.js';
import { nodeLocks } from '../locks-node.js';
import { outsideTreesTmpdir } from './outside-trees.js';

/**
 * The compiled modules a real Node imports, resolved through this package's published exports, the one authority
 * for where the ordinary emit lands. A hand-joined dist path goes stale when the emit moves, and then imports
 * whatever an older build left behind or nothing at all.
 */
export const RETENTION_MODULE_URL = import.meta.resolve('@smoothbricks/cargo-retain');
export const NODE_LOCKS_MODULE_URL = import.meta.resolve('@smoothbricks/cargo-retain/locks-node');
export const LAUNCH_NODE_MODULE_URL = import.meta.resolve('@smoothbricks/cargo-retain/launch-node');

/** This package's root, resolved through its published `./package.json` export. */
export const PACKAGE_DIR = dirname(fileURLToPath(import.meta.resolve('@smoothbricks/cargo-retain/package.json')));

/** The command `package.json#bin` declares; the native build of the CLI carries the same name. */
export const CLI_NAME = 'smoo-nx-cargo-retain';

interface DeclaredBin {
  readonly bin: Readonly<Record<typeof CLI_NAME, string>>;
}
const validateDeclaredBin = typia.createValidate<DeclaredBin>();

/**
 * The executable `package.json#bin` declares for {@link CLI_NAME} in the package rooted at `packageDir` (this checkout or
 * an unpacked tarball), as an absolute path: what npm links, where a hand-joined emit path would bypass the declaration.
 */
export async function declaredCli(packageDir: string): Promise<string> {
  const manifest = join(packageDir, 'package.json');
  const checked = validateDeclaredBin(JSON.parse(await readFile(manifest, 'utf8')));
  if (!checked.success) throw new Error(`${manifest}: no bin.${CLI_NAME}: ${JSON.stringify(checked.errors)}`);
  return join(packageDir, checked.data.bin[CLI_NAME]);
}

/** Three days past the newest stamp a fixture build can carry: drops only the recency root. */
export const DAY3 = 3 * RECENT_USE_MS;
export const RETENTION_LOCK = '.smoo-cargo-retention.lock';
export const TRASH = '.smoo-retention-trash';

export function lines(...rows: string[]): string {
  return `${rows.join('\n')}\n`;
}

export async function put(file: string, text: string): Promise<void> {
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, text);
}

export async function exists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch {
    return false;
  }
}

async function isDir(path: string): Promise<boolean> {
  try {
    return (await lstat(path)).isDirectory();
  } catch {
    return false;
  }
}

/** Scope environment variables: the retention pass spawns its own cargo and perl from process.env. */
export async function withEnv(vars: Record<string, string | undefined>, run: () => Promise<void>): Promise<void> {
  const saved = new Map<string, string | undefined>();
  for (const [key, value] of Object.entries(vars)) {
    saved.set(key, process.env[key]);
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    await run();
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

export function scopedTo(targetDir: string, run: () => Promise<void>): Promise<void> {
  return withEnv({ CARGO_TARGET_DIR: targetDir, CARGO_BUILD_DIR: undefined, CARGO_BUILD_TARGET: undefined }, run);
}

/**
 * The environment fixture cargo runs under: the job's own, without its wrappers and
 * without any `CARGO_UNSTABLE_*` switch. Each fixture's `.cargo/config.toml` states the
 * unstable features it tests; cowshed exports `CARGO_UNSTABLE_MTIME_ON_USE=true` into
 * every job, which would otherwise turn mtime-on-use on for the fixture that tests it off.
 * Nor `CI`: Cargo turns incremental compilation off for workspace crates whenever it is set
 * (03_caches.md, "No CI in a dev environment"), and a CI runner sets it, so the incremental
 * cases would find no incremental directories to keep or supersede.
 * Nor mbx: the fixtures are plain Cargo layouts, and in an mbx project's shell `cargo` is
 * mbx's Cargo shim and the canonical mbx environment (`MBX_*`, specs/cowshed/11_shell.md)
 * would put their units in mbx's managed target views and incremental state. So no `MBX_*`
 * variable passes, and the toolchain's own `cargo` ({@link PLAIN_CARGO}) leads PATH.
 */
export function cargoEnv(): NodeJS.ProcessEnv {
  const env = Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) => !name.startsWith('CARGO_UNSTABLE_') && !name.startsWith('MBX_') && name !== 'CI',
    ),
  );
  return {
    ...env,
    PATH: `${dirname(PLAIN_CARGO)}${delimiter}${process.env.PATH ?? ''}`,
    RUSTC_WRAPPER: '',
    RUSTC_WORKSPACE_WRAPPER: '',
    CARGO_NET_OFFLINE: 'true',
  };
}

/**
 * The toolchain's own `cargo`, beside the `rustc` that names its sysroot: never an mbx project's Cargo shim, which
 * stands first on its shell's PATH as `cargo`.
 */
export const PLAIN_CARGO = join(
  execFileSync('rustc', ['--print', 'sysroot'], { encoding: 'utf8' }).trim(),
  'bin',
  'cargo',
);

export interface CargoRun {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

async function cargo(cwd: string, args: string[], env: NodeJS.ProcessEnv = cargoEnv()): Promise<CargoRun> {
  const child = Bun.spawn([PLAIN_CARGO, ...args], { cwd, env, stdout: 'pipe', stderr: 'pipe' });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code, stdout, stderr };
}

export async function cargoOk(cwd: string, args: string[], env?: NodeJS.ProcessEnv): Promise<CargoRun> {
  const run = await cargo(cwd, args, env);
  if (run.code !== 0) throw new Error(`cargo ${args.join(' ')} failed in ${cwd}\n${run.stdout}${run.stderr}`);
  return run;
}

export function relock(workspace: string): void {
  execFileSync(PLAIN_CARGO, ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
    stdio: 'pipe',
    env: cargoEnv(),
  });
}

export async function build(workspace: string, extra: string[] = []): Promise<CargoRun> {
  return cargoOk(workspace, ['build', '--workspace', '--locked', '--offline', ...extra]);
}

export interface Artifact {
  readonly name: string;
  readonly packageId: string;
  readonly fresh: boolean;
}

interface CargoArtifactMessage {
  readonly reason: 'compiler-artifact';
  readonly package_id: string;
  readonly target: { readonly name: string };
  readonly fresh: boolean;
}
const validateArtifact = typia.createValidate<CargoArtifactMessage>();
const validateReport = typia.createValidateEquals<RetentionReport>();

export function parseReport(text: string): RetentionReport {
  const checked = validateReport(JSON.parse(text));
  if (!checked.success) throw new Error(`invalid retention report: ${JSON.stringify(checked.errors)}`);
  return checked.data;
}
export function artifacts(run: CargoRun): Artifact[] {
  const found: Artifact[] = [];
  for (const line of run.stdout.split('\n')) {
    if (!line.startsWith('{')) continue;
    const message: unknown = JSON.parse(line);
    if (
      typeof message !== 'object' ||
      message === null ||
      !('reason' in message) ||
      message.reason !== 'compiler-artifact'
    )
      continue;
    const checked = validateArtifact(message);
    if (!checked.success) throw new Error(`invalid Cargo artifact: ${JSON.stringify(checked.errors)}`);
    found.push({
      name: checked.data.target.name,
      packageId: checked.data.package_id,
      fresh: checked.data.fresh,
    });
  }
  return found;
}

export async function buildJson(workspace: string, extra: string[] = []): Promise<Artifact[]> {
  const run = await cargoOk(workspace, [
    'build',
    '--workspace',
    '--locked',
    '--offline',
    '--message-format=json',
    ...extra,
  ]);
  return artifacts(run);
}

/** The warmth oracle: the next real build compiles nothing at all. */
export async function expectFresh(workspace: string, extra: string[] = []): Promise<void> {
  const run = await cargoOk(
    workspace,
    ['build', '--workspace', '--locked', '--offline', '--message-format=json', ...extra],
    cargoEnv(),
  );
  const built = artifacts(run);
  expect(built.length).toBeGreaterThan(0);
  expect(
    built.filter((a) => !a.fresh).map((a) => `${a.packageId} ${a.name}`),
    run.stderr,
  ).toEqual([]);
}

/** Path, kind and a content hash for every entry under root, links not followed. */
export async function snapshot(root: string, ignore: ReadonlySet<string> = new Set()): Promise<string[]> {
  const rows: string[] = [];
  const walk = async (dir: string): Promise<void> => {
    for (const name of (await readdir(dir)).sort()) {
      const path = join(dir, name);
      const rel = path.slice(root.length + 1);
      if (ignore.has(rel)) continue;
      const stats = await lstat(path);
      if (stats.isSymbolicLink()) rows.push(`${rel} -> ${await readlink(path)}`);
      else if (stats.isDirectory()) {
        rows.push(`${rel}/`);
        await walk(path);
      } else {
        const hash = new Bun.CryptoHasher('sha256').update(await readFile(path)).digest('hex');
        rows.push(`${rel} ${stats.size} ${hash}`);
      }
    }
  };
  await walk(root);
  return rows;
}

/** Every generation directory Cargo made for one package, in any profile under the target root. */
export async function metasOf(targetDir: string, pkg: string): Promise<string[]> {
  const found: string[] = [];
  const scan = async (profile: string): Promise<void> => {
    const pkgDir = join(profile, 'build', pkg);
    if (!(await isDir(pkgDir))) return;
    for (const name of (await readdir(pkgDir)).sort()) found.push(join(pkgDir, name));
  };
  for (const name of (await readdir(targetDir)).sort()) {
    const dir = join(targetDir, name);
    if (!(await isDir(dir))) continue;
    if (await isDir(join(dir, 'build'))) await scan(dir);
    else {
      for (const inner of (await readdir(dir)).sort()) {
        const profile = join(dir, inner);
        if (await isDir(join(profile, 'build'))) await scan(profile);
      }
    }
  }
  return found.sort();
}

export function only(paths: string[], what: string): string {
  const first = paths[0];
  if (paths.length !== 1 || first === undefined) {
    throw new Error(`expected exactly one ${what}, found ${JSON.stringify(paths)}`);
  }
  return first;
}

/** Age a generation's Cargo stamps so real-now passes see it as long unused. */
export async function backdate(metas: string[]): Promise<void> {
  const past = new Date(Date.now() - DAY3);
  for (const meta of metas) {
    const fingerprints = join(meta, 'fingerprint');
    for (const name of await readdir(fingerprints)) await utimes(join(fingerprints, name), past, past);
    const runStamp = join(meta, 'run', 'invoked.timestamp');
    if (await exists(runStamp)) await utimes(runStamp, past, past);
  }
}

/**
 * Ages every file under `trees` to before `backdate`'s stamps, so the units built from them stay fresh under Cargo's
 * mtime freshness, as a checkout's sources and build state come from one tree. `target` keeps its own times.
 */
export async function backdateSources(trees: string[], target: string): Promise<void> {
  const past = new Date(Date.now() - DAY3 - RECENT_USE_MS);
  const age = async (path: string): Promise<void> => {
    if (path === target) return;
    const stats = await lstat(path);
    if (stats.isDirectory()) for (const name of await readdir(path)) await age(join(path, name));
    else if (stats.isFile()) await utimes(path, past, past);
  };
  for (const tree of trees) await age(tree);
}

/** The residue of a compile Cargo never finished: it empties the hash file before compiling a dirty unit. */
export async function emptyHash(meta: string): Promise<void> {
  const fingerprints = join(meta, 'fingerprint');
  for (const name of await readdir(fingerprints)) {
    if (
      !name.endsWith('.json') &&
      name !== 'invoked.timestamp' &&
      !name.startsWith('dep-') &&
      !name.startsWith('output-')
    ) {
      await writeFile(join(fingerprints, name), '');
    }
  }
}
export type PlannedOutcome = Extract<RetentionOutcome, { kind: 'planned' | 'reclaimed' }>;

export async function retain(root: string, options: RetentionOptions): Promise<RetentionReport> {
  const result = await retainCargoWorkspace(root, options, nodeLocks);
  if (!result.ok) throw new Error(`retention error: ${JSON.stringify(result.error)}`);
  return result.value;
}

function plannedIn(outcome: RetentionOutcome): PlannedOutcome {
  if (outcome.kind !== 'planned' && outcome.kind !== 'reclaimed') {
    throw new Error(`expected planned/reclaimed, got ${JSON.stringify(outcome)}`);
  }
  return outcome;
}

export function planned(report: RetentionReport): PlannedOutcome {
  return plannedIn(report.outcome);
}

/** What the pass of one nested root did, or a failure naming the nested roots the report has. */
export function nestedOf(report: RetentionReport, root: string): RetentionOutcome {
  const found = report.nestedRoots.find((nested) => nested.root === root);
  if (found === undefined) {
    throw new Error(`no nested root ${root} in ${JSON.stringify(report.nestedRoots.map((nested) => nested.root))}`);
  }
  return found.outcome;
}

export function nestedPlanned(report: RetentionReport, root: string): PlannedOutcome {
  return plannedIn(nestedOf(report, root));
}

export function refused(report: RetentionReport): { profile: string; reason: string } {
  const outcome = report.outcome;
  if (outcome.kind !== 'refused') throw new Error(`expected refused, got ${JSON.stringify(outcome)}`);
  return outcome;
}

/** The unit generations an outcome reclaims (or would). */
export function reclaimPaths(outcome: PlannedOutcome): string[] {
  return outcome.reclaim
    .filter((entry) => entry.reason === 'superseded-generation')
    .map((entry) => entry.path)
    .sort();
}

/** The `incremental/<crate>-<id>` directories an outcome reclaims (or would). */
export function incrementalReclaimPaths(outcome: PlannedOutcome): string[] {
  return outcome.reclaim
    .filter((entry) => entry.reason === 'superseded-incremental')
    .map((entry) => entry.path)
    .sort();
}

/** Each directory under a profile's `incremental/`, by name, with its inode: a reused one keeps it. */
export async function incrementalDirs(incremental: string): Promise<Map<string, number>> {
  const dirs = new Map<string, number>();
  for (const name of await readdir(incremental)) dirs.set(name, (await lstat(join(incremental, name))).ino);
  return dirs;
}

export function keepReason(outcome: PlannedOutcome, path: string): string | undefined {
  return outcome.retained.find((entry) => entry.path === path)?.reason;
}

export function future(): number {
  return Date.now() + DAY3;
}

export function fixtureConfig(mtimeOnUse: boolean): string {
  return mtimeOnUse ? lines('[unstable]', 'mtime-on-use = true') : '';
}

export function extManifest(version: string): string {
  return lines('[package]', 'name = "ret-ext"', `version = "${version}"`, 'edition = "2021"', '', '[workspace]');
}

export interface Churn {
  readonly root: string;
  readonly ws: string;
  readonly target: string;
  readonly ext: string;
}

export async function writeMid(ws: string, leafExtra: boolean, extVersion: string): Promise<void> {
  const leaf = leafExtra ? 'ret-leaf = { path = "../leaf", features = ["extra"] }' : 'ret-leaf = { path = "../leaf" }';
  await put(
    join(ws, 'crates/mid/Cargo.toml'),
    lines(
      '[package]',
      'name = "ret-mid"',
      'version = "0.1.0"',
      'edition = "2021"',
      '',
      '[dependencies]',
      leaf,
      `ret-ext = { path = "../../../ext", version = "${extVersion}" }`,
    ),
  );
}

/**
 * leaf <- mid <- app in one workspace, ext a path dependency in its own workspace.
 * Toggling leaf's feature from mid churns the mid and app generations; nothing here
 * is a registry unit, and ext stands in for one: not a member, kept only by reachability.
 */
export async function churnFixture(root: string, mtimeOnUse: boolean): Promise<Churn> {
  const ws = join(root, 'ws');
  const ext = join(root, 'ext');
  const target = join(ws, 'target');
  await put(join(ws, '.cargo/config.toml'), fixtureConfig(mtimeOnUse));
  await put(
    join(ws, 'Cargo.toml'),
    lines('[workspace]', 'members = ["crates/leaf", "crates/mid", "crates/app"]', 'resolver = "2"'),
  );
  await put(
    join(ws, 'crates/leaf/Cargo.toml'),
    lines('[package]', 'name = "ret-leaf"', 'version = "0.1.0"', 'edition = "2021"', '', '[features]', 'extra = []'),
  );
  await put(
    join(ws, 'crates/leaf/src/lib.rs'),
    lines('pub fn base() -> u8 { 1 }', '', '#[cfg(feature = "extra")]', 'pub fn extra() -> u8 { 2 }'),
  );
  await put(join(ext, 'Cargo.toml'), extManifest('0.1.0'));
  await put(join(ext, 'src/lib.rs'), lines('pub fn value() -> u8 { 10 }'));
  await put(
    join(ws, 'crates/app/Cargo.toml'),
    lines(
      '[package]',
      'name = "ret-app"',
      'version = "0.1.0"',
      'edition = "2021"',
      '',
      '[dependencies]',
      'ret-mid = { path = "../mid" }',
    ),
  );
  await put(
    join(ws, 'crates/app/src/lib.rs'),
    lines(
      'pub fn total() -> u8 { ret_mid::total() + 2 }',
      '',
      '#[cfg(test)]',
      'mod tests {',
      '    #[test]',
      '    fn totals() {',
      '        assert_eq!(super::total(), 13);',
      '    }',
      '}',
    ),
  );
  await put(join(ws, 'crates/app/src/main.rs'), lines('fn main() { println!("{}", ret_app::total()); }'));
  await writeMid(ws, false, '0.1');
  await put(join(ws, 'crates/mid/src/lib.rs'), lines('pub fn total() -> u8 { ret_leaf::base() + ret_ext::value() }'));
  relock(ws);
  return { root, ws, target, ext };
}

export interface Generations {
  /** The generation-1 mid and app generations the feature toggle superseded. */
  readonly superseded: string[];
  readonly leafOld: string;
  readonly extMeta: string;
}

export async function churnGenerations(fx: Churn): Promise<Generations> {
  await build(fx.ws);
  const mid1 = await metasOf(fx.target, 'ret-mid');
  const app1 = await metasOf(fx.target, 'ret-app');
  const leaf1 = await metasOf(fx.target, 'ret-leaf');
  const ext1 = await metasOf(fx.target, 'ret-ext');
  if (app1.length === 0) throw new Error('the first build made no ret-app generation');
  const generations: Generations = {
    superseded: [...mid1, ...app1].sort(),
    leafOld: only(leaf1, 'ret-leaf generation'),
    extMeta: only(ext1, 'ret-ext generation'),
  };
  only(mid1, 'ret-mid generation');
  await writeMid(fx.ws, true, '0.1');
  relock(fx.ws);
  await build(fx.ws);
  const mid2 = (await metasOf(fx.target, 'ret-mid')).filter((path) => !mid1.includes(path));
  if (mid2.length === 0) throw new Error('the feature toggle made no new ret-mid generation');
  return generations;
}

export interface ProcMacro {
  readonly ws: string;
  readonly pm: string;
  readonly target: string;
  /** The host triple: built for explicitly, target units go to `<triple>/<profile>` and the proc macro to the host profile. */
  readonly host: string;
}

/** `ret-app` in a one-member workspace, using the proc macro `ret-pm` that lives in a workspace of its own. */
export async function procMacroFixture(root: string): Promise<ProcMacro> {
  const version = execFileSync('rustc', ['-vV'], { encoding: 'utf8' });
  const host = /^host: (.+)$/m.exec(version)?.[1];
  if (host === undefined) throw new Error('rustc -vV named no host triple');
  const ws = join(root, 'ws');
  const pm = join(root, 'pm');
  await put(join(ws, '.cargo/config.toml'), fixtureConfig(false));
  await put(join(ws, 'Cargo.toml'), lines('[workspace]', 'members = ["crates/app"]', 'resolver = "2"'));
  await put(
    join(ws, 'crates/app/Cargo.toml'),
    lines(
      '[package]',
      'name = "ret-app"',
      'version = "0.1.0"',
      'edition = "2021"',
      '',
      '[dependencies]',
      'ret-pm = { path = "../../../pm" }',
    ),
  );
  await put(join(ws, 'crates/app/src/lib.rs'), lines('ret_pm::answer!();'));
  await put(
    join(pm, 'Cargo.toml'),
    lines(
      '[package]',
      'name = "ret-pm"',
      'version = "0.1.0"',
      'edition = "2021"',
      '',
      '[lib]',
      'proc-macro = true',
      '',
      '[workspace]',
    ),
  );
  await put(
    join(pm, 'src/lib.rs'),
    lines(
      'extern crate proc_macro;',
      'use proc_macro::TokenStream;',
      '',
      '#[proc_macro]',
      'pub fn answer(_input: TokenStream) -> TokenStream {',
      '    "pub fn answer() -> u8 { 42 }".parse().unwrap()',
      '}',
    ),
  );
  relock(ws);
  return { ws, pm, target: join(ws, 'target'), host };
}

/** A running Cargo build's hold on a profile: the shared `.cargo-lock` flock(2), kept until the returned release. */
export async function holdProfile(lock: string): Promise<() => Promise<void>> {
  const child = Bun.spawn(
    [
      'perl',
      '-e',
      'use Fcntl qw(:flock); open(my $fh, "<", $ARGV[0]) or die "$!"; flock($fh, LOCK_SH) or die "$!"; $| = 1; print "held\n"; 1 while defined(<STDIN>);',
      lock,
    ],
    { stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' },
  );
  const said = await child.stdout.getReader().read();
  const line = said.value === undefined ? '' : new TextDecoder().decode(said.value);
  if (line !== 'held\n')
    throw new Error(`perl did not take ${lock}: ${line}${await new Response(child.stderr).text()}`);
  return async () => {
    await child.stdin.end();
    await child.exited;
  };
}

export async function inFixture(run: (root: string) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-retention-')));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

/** Labels each assertion of a walk step with the case it proves, so a failing check names that case. */
export function labelled(label: string) {
  return <T>(actual: T, detail?: string) => expect(actual, detail === undefined ? label : `${label}: ${detail}`);
}

export async function clearReceipt(target: string): Promise<void> {
  const receipt = join(target, RETENTION_RECEIPT);
  if (await exists(receipt)) await rm(receipt);
}

export async function clearLock(target: string): Promise<void> {
  const lock = join(target, RETENTION_LOCK);
  if (await exists(lock)) await rm(lock);
}

export async function midAndApp(target: string): Promise<string[]> {
  return [...(await metasOf(target, 'ret-mid')), ...(await metasOf(target, 'ret-app'))].sort();
}

/**
 * The walk's next churn: flip the leaf feature mid depends on and build. Returns the mid and app
 * generations that build supersedes, the set the following retention pass must reclaim.
 */
export async function toggleFeature(fx: Churn): Promise<string[]> {
  const extra = (await readFile(join(fx.ws, 'crates/mid/Cargo.toml'), 'utf8')).includes('features = ["extra"]');
  const superseded = await midAndApp(fx.target);
  if (superseded.length === 0) throw new Error('the walk left no mid or app generation to supersede');
  await writeMid(fx.ws, !extra, '0.1');
  relock(fx.ws);
  await build(fx.ws);
  const created = (await metasOf(fx.target, 'ret-mid')).filter((path) => !superseded.includes(path));
  if (created.length === 0) throw new Error('the feature toggle made no new ret-mid generation');
  return superseded;
}

interface ScanWork {
  readonly report: RetentionReport;
  readonly reads: readonly string[];
  readonly stats: readonly string[];
  readonly asyncFingerprintCalls: readonly string[];
  readonly children: readonly string[];
}
export const validateScanWork = typia.createValidateEquals<ScanWork>();

interface PurgeWork {
  readonly report: RetentionReport;
  readonly nativeTrees: readonly string[];
  readonly asyncFiles: readonly string[];
}
export const validatePurgeWork = typia.createValidateEquals<PurgeWork>();
export type RetentionRun =
  | { readonly ok: true; readonly value: RetentionReport }
  | { readonly ok: false; readonly error: RetentionError };
const validateRetentionRun = typia.createValidateEquals<RetentionRun>();

/**
 * One `retainCargoWorkspace` in a real Node, as Nx runs it, with the perl lock helper unable to start. `denied` throws
 * EPERM from `spawn`, which is what Node does for an exec a sandbox refuses (it emits 'error' only for the failures it
 * calls run-time); `missing` runs it where `env` holds no perl. macOS holds its locks in-process, so the platform is
 * faked once the module is imported to take the perl route there too.
 */
export function retainWithoutPerl(
  node: string,
  workspace: string,
  apply: boolean,
  perl: 'denied' | 'missing',
  env: NodeJS.ProcessEnv,
): RetentionRun {
  const source = lines(
    'import cp from "node:child_process";',
    'import { syncBuiltinESMExports } from "node:module";',
    'const spawn = cp.spawn;',
    'cp.spawn = function(command, ...args) {',
    '  if (process.argv[1] === "denied" && command === "perl") {',
    '    throw Object.assign(new Error("spawn EPERM"), { errno: -1, code: "EPERM", syscall: "spawn" });',
    '  }',
    '  return spawn.call(this, command, ...args);',
    '};',
    'syncBuiltinESMExports();',
    `const { retainCargoWorkspace } = await import(${JSON.stringify(RETENTION_MODULE_URL)});`,
    `const { nodeLocks } = await import(${JSON.stringify(NODE_LOCKS_MODULE_URL)});`,
    'Object.defineProperty(process, "platform", { value: "linux" });',
    `const result = await retainCargoWorkspace(${JSON.stringify(workspace)}, { apply: process.argv[2] === "apply", now: ${future()} }, nodeLocks);`,
    'console.log(JSON.stringify(result));',
  );
  const child = spawnSync(node, ['--input-type=module', '-e', source, perl, apply ? 'apply' : 'plan'], {
    cwd: workspace,
    env,
    encoding: 'utf8',
  });
  expect(child.status, child.stderr).toBe(0);
  const checked = validateRetentionRun(JSON.parse(child.stdout));
  if (!checked.success) throw new Error(JSON.stringify(checked.errors));
  return checked.data;
}

/**
 * A Node preload that fails what a sandbox refusing the lock would: `spawn` of each of `commands` throws EPERM, as Node
 * does for a refused exec, and, where `locks`, so does the `open(2)` carrying macOS's O_EXLOCK bit (0x20).
 */
export function denyPreload(commands: readonly string[], locks: boolean): string {
  return lines(
    'import cp from "node:child_process";',
    'import fsp from "node:fs/promises";',
    'import { syncBuiltinESMExports } from "node:module";',
    'const denied = (message, syscall) => Object.assign(new Error(message), { errno: -1, code: "EPERM", syscall });',
    `const commands = ${JSON.stringify(commands)};`,
    'const spawn = cp.spawn;',
    'cp.spawn = function(command, ...args) {',
    '  if (commands.includes(command)) throw denied("spawn EPERM", "spawn");',
    '  return spawn.call(this, command, ...args);',
    '};',
    'const open = fsp.open;',
    'fsp.open = function(path, flags, ...rest) {',
    `  if (${String(locks)} && process.platform === "darwin" && typeof flags === "number" && (flags & 0x20) !== 0) {`,
    '    return Promise.reject(denied("EPERM: operation not permitted, open " + path, "open"));',
    '  }',
    '  return open.call(this, path, flags, ...rest);',
    '};',
    'syncBuiltinESMExports();',
  );
}

/**
 * A Node preload for the pass the Nx hook starts detached (`node-cli.js`, from `launch-node.ts`), which no caller
 * awaits: as that process ends it says `exit <code>` into the FIFO `exited`. With `gate`, every process that loads it
 * holds its first recursive removal under `gate.under`, the target directory, whichever way the pass frees a
 * generation: from its trash, or in place on a volume under pressure. It says its pid into the FIFO `gate.entered`, then
 * waits until the test writes `gate.release`; a pass that ends before reaching one says its exit into `gate.entered`
 * instead, so no reader waits on a process that is gone.
 */
export function detachedPassPreload(
  exited: string,
  gate?: { readonly entered: string; readonly release: string; readonly under: string },
): string {
  return lines(
    'import fs from "node:fs";',
    'import { syncBuiltinESMExports } from "node:module";',
    `const exited = ${JSON.stringify(exited)};`,
    `const gate = ${JSON.stringify(gate ?? null)};`,
    'let held = false;',
    'if (gate !== null) {',
    '  const rmSync = fs.rmSync;',
    '  fs.rmSync = function(path, ...args) {',
    '    if (!held && String(path).startsWith(gate.under + "/")) {',
    '      held = true;',
    '      fs.writeFileSync(gate.entered, process.pid + "\\n");',
    '      fs.readFileSync(gate.release);',
    '    }',
    '    return rmSync.call(this, path, ...args);',
    '  };',
    '  syncBuiltinESMExports();',
    '}',
    'if (process.argv[1]?.endsWith("/node-cli.js")) {',
    '  process.on("exit", (code) => fs.writeFileSync(gate !== null && !held ? gate.entered : exited, "exit " + code + "\\n"));',
    '}',
  );
}

export type RetentionStartRun =
  | { readonly ok: true; readonly value: RetentionStart }
  | { readonly ok: false; readonly error: RetentionError };
const validateRetentionStartRun = typia.createValidateEquals<RetentionStartRun>();

/** What a real Node's `startRetentionPass` printed as JSON, checked against the result type with no unknown field. */
export function parseRetentionStart(text: string): RetentionStartRun {
  const checked = validateRetentionStartRun(JSON.parse(text));
  if (!checked.success) throw new Error(`invalid retention start: ${JSON.stringify(checked.errors)}`);
  return checked.data;
}
