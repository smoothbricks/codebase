import { afterAll, describe, expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import {
  cp,
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  readlink,
  realpath,
  rename,
  rm,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import typia from 'typia';
import { useFixtureCargoHome } from './__tests__/fixture-cargo-home.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { outsideTreesTmpdir } from './__tests__/outside-trees.js';
import { testFileInstallation } from './__tests__/owned-installation.js';
import {
  AUTOMATIC_INTERVAL_MS,
  fingerprintIdentity,
  RECENT_USE_MS,
  RETENTION_RECEIPT,
  type RetentionError,
  type RetentionOptions,
  type RetentionOutcome,
  type RetentionReport,
  readCargoFingerprint,
  retainCargoWorkspace,
} from './cargo-retention.js';
import { EXTRACTED_BINARIES_METADATA, EXTRACTED_CARGO_METADATA, EXTRACTED_TARGET_DIR } from './nextest-extraction.js';

useFixtureCargoHome();

/** The Nx and compiled plugin under test, one copy this file's Nx fixture shares. */
const installation = testFileInstallation();
afterAll(() => installation.retire());

/** Three days past the newest stamp a fixture build can carry: drops only the recency root. */
const DAY3 = 3 * RECENT_USE_MS;
const RETENTION_LOCK = '.smoo-cargo-retention.lock';
const TRASH = '.smoo-retention-trash';

function lines(...rows: string[]): string {
  return `${rows.join('\n')}\n`;
}

async function put(file: string, text: string): Promise<void> {
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, text);
}

async function exists(path: string): Promise<boolean> {
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
async function withEnv(vars: Record<string, string | undefined>, run: () => Promise<void>): Promise<void> {
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

function scopedTo(targetDir: string, run: () => Promise<void>): Promise<void> {
  return withEnv({ CARGO_TARGET_DIR: targetDir, CARGO_BUILD_DIR: undefined, CARGO_BUILD_TARGET: undefined }, run);
}

function cargoEnv(): NodeJS.ProcessEnv {
  return { ...process.env, RUSTC_WRAPPER: '', RUSTC_WORKSPACE_WRAPPER: '', CARGO_NET_OFFLINE: 'true' };
}

interface CargoRun {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

async function cargo(cwd: string, args: string[], env: NodeJS.ProcessEnv = cargoEnv()): Promise<CargoRun> {
  const child = Bun.spawn(['cargo', ...args], { cwd, env, stdout: 'pipe', stderr: 'pipe' });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code, stdout, stderr };
}

async function cargoOk(cwd: string, args: string[], env?: NodeJS.ProcessEnv): Promise<CargoRun> {
  const run = await cargo(cwd, args, env);
  if (run.code !== 0) throw new Error(`cargo ${args.join(' ')} failed in ${cwd}\n${run.stdout}${run.stderr}`);
  return run;
}

function relock(workspace: string): void {
  execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
    stdio: 'pipe',
    env: cargoEnv(),
  });
}

async function build(workspace: string, extra: string[] = []): Promise<CargoRun> {
  return cargoOk(workspace, ['build', '--workspace', '--locked', '--offline', ...extra]);
}

interface Artifact {
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

function parseReport(text: string): RetentionReport {
  const checked = validateReport(JSON.parse(text));
  if (!checked.success) throw new Error(`invalid retention report: ${JSON.stringify(checked.errors)}`);
  return checked.data;
}
function artifacts(run: CargoRun): Artifact[] {
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

async function buildJson(workspace: string, extra: string[] = []): Promise<Artifact[]> {
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
async function expectFresh(workspace: string, extra: string[] = []): Promise<void> {
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
async function snapshot(root: string, ignore: ReadonlySet<string> = new Set()): Promise<string[]> {
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
async function metasOf(targetDir: string, pkg: string): Promise<string[]> {
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

function only(paths: string[], what: string): string {
  const first = paths[0];
  if (paths.length !== 1 || first === undefined) {
    throw new Error(`expected exactly one ${what}, found ${JSON.stringify(paths)}`);
  }
  return first;
}

/** Age a generation's Cargo stamps so real-now passes see it as long unused. */
async function backdate(metas: string[]): Promise<void> {
  const past = new Date(Date.now() - DAY3);
  for (const meta of metas) {
    const fingerprints = join(meta, 'fingerprint');
    for (const name of await readdir(fingerprints)) await utimes(join(fingerprints, name), past, past);
    const runStamp = join(meta, 'run', 'invoked.timestamp');
    if (await exists(runStamp)) await utimes(runStamp, past, past);
  }
}

/** The residue of a compile Cargo never finished: it empties the hash file before compiling a dirty unit. */
async function emptyHash(meta: string): Promise<void> {
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
type PlannedOutcome = Extract<RetentionOutcome, { kind: 'planned' | 'reclaimed' }>;

async function retain(root: string, options: RetentionOptions): Promise<RetentionReport> {
  const result = await retainCargoWorkspace(root, options);
  if (!result.ok) throw new Error(`retention error: ${JSON.stringify(result.error)}`);
  return result.value;
}

function planned(report: RetentionReport): PlannedOutcome {
  const outcome = report.outcome;
  if (outcome.kind !== 'planned' && outcome.kind !== 'reclaimed') {
    throw new Error(`expected planned/reclaimed, got ${JSON.stringify(outcome)}`);
  }
  return outcome;
}

function refused(report: RetentionReport): { profile: string; reason: string } {
  const outcome = report.outcome;
  if (outcome.kind !== 'refused') throw new Error(`expected refused, got ${JSON.stringify(outcome)}`);
  return outcome;
}

function reclaimPaths(outcome: PlannedOutcome): string[] {
  return outcome.reclaim.map((entry) => entry.path).sort();
}

function keepReason(outcome: PlannedOutcome, path: string): string | undefined {
  return outcome.retained.find((entry) => entry.path === path)?.reason;
}

function future(): Date {
  return new Date(Date.now() + DAY3);
}

function fixtureConfig(mtimeOnUse: boolean): string {
  return mtimeOnUse
    ? lines('[build]', 'fingerprint = "content"', '', '[unstable]', 'checksum-freshness = true', 'mtime-on-use = true')
    : lines('[build]', 'fingerprint = "content"', '', '[unstable]', 'checksum-freshness = true');
}

function extManifest(version: string): string {
  return lines('[package]', 'name = "ret-ext"', `version = "${version}"`, 'edition = "2021"', '', '[workspace]');
}

interface Churn {
  readonly root: string;
  readonly ws: string;
  readonly target: string;
  readonly ext: string;
}

async function writeMid(ws: string, leafExtra: boolean, extVersion: string): Promise<void> {
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
async function churnFixture(root: string, mtimeOnUse: boolean): Promise<Churn> {
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

interface Generations {
  /** The generation-1 mid and app generations the feature toggle superseded. */
  readonly superseded: string[];
  readonly leafOld: string;
  readonly extMeta: string;
}

async function churnGenerations(fx: Churn): Promise<Generations> {
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

async function inFixture(run: (root: string) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-retention-')));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

/** Labels each assertion of a walk step with the case it proves, so a failing check names that case. */
function labelled(label: string) {
  return <T>(actual: T, detail?: string) => expect(actual, detail === undefined ? label : `${label}: ${detail}`);
}

async function clearReceipt(target: string): Promise<void> {
  const receipt = join(target, RETENTION_RECEIPT);
  if (await exists(receipt)) await rm(receipt);
}

async function clearLock(target: string): Promise<void> {
  const lock = join(target, RETENTION_LOCK);
  if (await exists(lock)) await rm(lock);
}

async function midAndApp(target: string): Promise<string[]> {
  return [...(await metasOf(target, 'ret-mid')), ...(await metasOf(target, 'ret-app'))].sort();
}

/**
 * The walk's next churn: flip the leaf feature mid depends on and build. Returns the mid and app
 * generations that build supersedes, the set the following retention pass must reclaim.
 */
async function toggleFeature(fx: Churn): Promise<string[]> {
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
const validateScanWork = typia.createValidateEquals<ScanWork>();
type RetentionRun =
  | { readonly ok: true; readonly value: RetentionReport }
  | { readonly ok: false; readonly error: RetentionError };
const validateRetentionRun = typia.createValidateEquals<RetentionRun>();

/**
 * One `retainCargoWorkspace` in a real Node, as Nx runs it, with the perl lock helper unable to start. `denied` throws
 * EPERM from `spawn`, which is what Node does for an exec a sandbox refuses (it emits 'error' only for the failures it
 * calls run-time); `missing` runs it where `env` holds no perl. macOS holds its locks in-process, so the platform is
 * faked once the module is imported to take the perl route there too.
 */
function retainWithoutPerl(
  node: string,
  workspace: string,
  apply: boolean,
  perl: 'denied' | 'missing',
  env: NodeJS.ProcessEnv,
): RetentionRun {
  const compiled = join(import.meta.dir, '..', 'dist', 'cargo-retention.js');
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
    `const { retainCargoWorkspace } = await import(${JSON.stringify(compiled)});`,
    'Object.defineProperty(process, "platform", { value: "linux" });',
    `const result = await retainCargoWorkspace(${JSON.stringify(workspace)}, { apply: process.argv[2] === "apply", now: new Date(${future().getTime()}) });`,
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
function denyPreload(commands: readonly string[], locks: boolean): string {
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
/** Title of the reclaim case for one mtime-on-use setting; the walk's first pass and the mtime-on-use case share it. */
const RECLAIM_TITLE = (mtimeOnUse: boolean): string =>
  `reclaims exactly the superseded generations and keeps the next build Fresh (mtime-on-use ${String(mtimeOnUse)})`;

/** Case: the u64 fingerprint digits parse and identify exactly. Pure values, so no fixture. */
function keepsExactDigitsInTheParser(): void {
  const check = labelled('keeps exact u64 fingerprint digits distinct, parser and end-to-end');
  const base = '{"rustc":8522741132697106019,"target":17066949999414752808,"deps":[],"local":[]}';
  const shifted = '{"rustc":8522741132697106019,"target":17066949999414752809,"deps":[],"local":[]}';
  const a = readCargoFingerprint(base);
  const b = readCargoFingerprint(shifted);
  if (!a.ok || !b.ok) throw new Error('fingerprints unreadable');
  check(fingerprintIdentity('ret-leaf', 'lib-ret_leaf', a.value), 'neighbouring exact digits').not.toBe(
    fingerprintIdentity('ret-leaf', 'lib-ret_leaf', b.value),
  );
  check(readCargoFingerprint('{').ok, 'a truncated fingerprint').toBe(false);
  check(readCargoFingerprint('{"rustc":1.5,"deps":[]}').ok, 'a fractional integer field').toBe(false);
}

/** Case: refuses the whole pass over one unreadable fingerprint and changes nothing. */
async function refusesOverUnreadableFingerprint(fx: Churn): Promise<void> {
  const check = labelled('refuses the whole pass over one unreadable fingerprint and changes nothing');
  const meta = only((await metasOf(fx.target, 'ret-mid')).slice(0, 1), 'a ret-mid generation to corrupt');
  const fingerprints = join(meta, 'fingerprint');
  const jsonName = (await readdir(fingerprints)).find((name) => name.endsWith('.json'));
  if (jsonName === undefined) throw new Error(`no fingerprint json under ${fingerprints}`);
  const jsonPath = join(fingerprints, jsonName);
  const original = await readFile(jsonPath, 'utf8');
  await writeFile(jsonPath, '{');
  const buildDir = join(fx.target, 'debug', 'build');
  const before = await snapshot(buildDir);
  const report = await retain(fx.ws, { apply: true, now: future() });
  check(refused(report).reason).toContain('fingerprint');
  check(await snapshot(buildDir)).toEqual(before);
  await writeFile(jsonPath, original);
}
/** Case: refuses the pass, changing nothing, when a lock cannot be attempted at all. */
async function refusesWithoutPerl(fx: Churn): Promise<void> {
  const check = labelled('refuses the pass, changing nothing, when a lock cannot be attempted at all');
  const node = Bun.which('node');
  if (node === null) throw new Error('node is not on PATH');
  const bin = join(fx.root, 'bin');
  await mkdir(bin);
  for (const tool of ['cargo', 'rustc']) {
    const real = Bun.which(tool);
    if (real === null) throw new Error(`${tool} is not on PATH`);
    await symlink(real, join(bin, tool));
  }
  const buildDir = join(fx.target, 'debug', 'build');
  const before = await snapshot(buildDir);
  const refusedBecause = (run: RetentionRun, lock: string, cause: string): void => {
    if (!run.ok) throw new Error(`the pass failed instead of being refused: ${JSON.stringify(run.error)}`);
    check(refused(run.value).reason, cause).toBe(`cannot attempt ${lock}: ${cause}`);
    check(run.value.completed, cause).toBeUndefined();
  };
  for (const apply of [false, true]) {
    const lock = apply ? 'the retention lock' : "Cargo's locks";
    // A sandbox that refuses the exec: Node throws EPERM from spawn() rather than emitting 'error'.
    refusedBecause(retainWithoutPerl(node, fx.ws, apply, 'denied', process.env), lock, 'cannot run perl: spawn EPERM');
    // No perl on PATH at all.
    refusedBecause(
      retainWithoutPerl(node, fx.ws, apply, 'missing', { ...process.env, PATH: bin }),
      lock,
      'cannot run perl: spawn perl ENOENT',
    );
  }
  check(await exists(join(fx.target, RETENTION_LOCK)), 'no lock file').toBe(false);
  // A lock file that cannot be opened (a symlink that loops, where it belongs) is no lock held elsewhere, on either
  // route: open(2) fails with ELOOP. A directory would do on Linux only, where O_CREAT refuses it.
  await symlink(RETENTION_LOCK, join(fx.target, RETENTION_LOCK));
  const unopenable = await retain(fx.ws, { apply: true, now: future() });
  check(refused(unopenable).reason).toMatch(
    /^cannot attempt the retention lock: .*(ELOOP|too many (levels of )?symbolic links)/i,
  );
  // A preview takes no retention lock of its own.
  check(planned(await retain(fx.ws, { apply: false, now: future() })).kind).toBe('planned');
  await rm(join(fx.target, RETENTION_LOCK));
  check(await snapshot(buildDir)).toEqual(before);
  check(await exists(join(fx.target, RETENTION_RECEIPT)), 'no receipt').toBe(false);
  check(await exists(join(fx.target, 'debug', TRASH)), 'no trash').toBe(false);
}

/** Case: refuses a legacy-layout profile instead of reporting a clean zero. */
async function refusesLegacyLayout(fx: Churn): Promise<void> {
  const check = labelled('refuses a legacy-layout profile instead of reporting a clean zero');
  const leaf = only((await metasOf(fx.target, 'ret-leaf')).slice(0, 1), 'a ret-leaf generation');
  const fingerprints = join(leaf, 'fingerprint');
  const jsonName = (await readdir(fingerprints)).find((name) => name.endsWith('.json'));
  if (jsonName === undefined) throw new Error(`no fingerprint json under ${fingerprints}`);
  const legacy = join(fx.target, 'debug', '.fingerprint');
  await mkdir(legacy, { recursive: true });
  await cp(join(fingerprints, jsonName), join(legacy, jsonName));
  const buildDir = join(fx.target, 'debug', 'build');
  const before = await snapshot(buildDir);
  const report = await retain(fx.ws, { apply: true, now: future() });
  check(refused(report).reason).toContain('legacy');
  check(await snapshot(buildDir)).toEqual(before);
  await rm(legacy, { recursive: true });
}

/** Case: refuses a profile with Cargo locks but no build directory (split build-dir shape). */
async function refusesSplitBuildDir(fx: Churn): Promise<void> {
  const check = labelled('refuses a profile with Cargo locks but no build directory (split build-dir shape)');
  const split = join(fx.target, 'split-profile');
  await put(join(split, '.cargo-lock'), '');
  await put(join(split, '.cargo-build-lock'), '');
  const buildDir = join(fx.target, 'debug', 'build');
  const before = await snapshot(buildDir);
  const report = await retain(fx.ws, { apply: false, now: future() });
  check(refused(report).reason).toBe('no build directory');
  check(await snapshot(buildDir)).toEqual(before);
  await rm(split, { recursive: true });
}

/** Case: never follows a symlinked fingerprint file; a link inside a real generation refuses the pass. */
async function refusesSymlinkedFingerprint(fx: Churn): Promise<void> {
  const check = labelled('never follows a symlinked generation directory or a symlinked fingerprint file');
  const meta = only((await metasOf(fx.target, 'ret-leaf')).slice(0, 1), 'a ret-leaf generation');
  const fingerprints = join(meta, 'fingerprint');
  const kind = (await readdir(fingerprints)).find((name) => name.endsWith('.json'))?.slice(0, -'.json'.length);
  if (kind === undefined) throw new Error(`no fingerprint kind under ${fingerprints}`);
  const hashPath = join(fingerprints, kind);
  const stash = join(fx.root, 'hash-stash');
  await rename(hashPath, stash);
  await symlink(stash, hashPath, 'file');
  const buildDir = join(fx.target, 'debug', 'build');
  const before = await snapshot(buildDir);
  const report = await retain(fx.ws, { apply: true, now: future() });
  check(refused(report).reason).toContain('fingerprint');
  check(await snapshot(buildDir)).toEqual(before);
  check(await readFile(stash, 'utf8')).toMatch(/^[0-9a-f]{16}$/);
  await rm(hashPath);
  await rename(stash, hashPath);
}

/** Case: the plan writes nothing and names exactly the superseded generations; at real now they stay recent. */
async function plansAndKeepsRecent(fx: Churn, gen: Generations, label: string): Promise<PlannedOutcome> {
  const check = labelled(label);
  await clearReceipt(fx.target);
  await clearLock(fx.target);
  const beforePlan = await snapshot(fx.target);
  const planReport = await retain(fx.ws, { apply: false, now: future() });
  const plan = planned(planReport);
  check(plan.kind).toBe('planned');
  check(planReport.targetDir).toBe(await realpath(fx.target));
  check(await snapshot(fx.target)).toEqual(beforePlan);
  check(await exists(join(fx.target, RETENTION_RECEIPT)), 'plan writes no receipt').toBe(false);
  check(await exists(join(fx.target, RETENTION_LOCK)), 'plan takes no lock').toBe(false);
  check(reclaimPaths(plan)).toEqual(gen.superseded);
  check(plan.reclaimedBytes).toBeGreaterThan(0);
  check(keepReason(plan, gen.leafOld)).toBe('newest-of-identity');
  check(keepReason(plan, gen.extMeta)).toBe('dependency-of-kept');
  // At real now the same generations are inside the recent window: nothing goes.
  const recent = planned(await retain(fx.ws, { apply: true }));
  check(recent.reclaim).toEqual([]);
  check(recent.reclaimedBytes).toBe(0);
  for (const path of gen.superseded) check(await exists(path), path).toBe(true);
  await expectFresh(fx.ws);
  return plan;
}

/** Case: apply reclaims exactly the superseded generations and keeps the next build Fresh; a repeat is a no-op. */
async function reclaimsSupersededAndRepeats(
  fx: Churn,
  gen: Generations,
  plan: PlannedOutcome,
  label: string,
): Promise<void> {
  const check = labelled(label);
  const incremental = join(fx.target, 'debug', 'incremental');
  const incrementalBefore = await snapshot(incremental);
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(applied.kind).toBe('reclaimed');
  check(reclaimPaths(applied)).toEqual(gen.superseded);
  check(applied.reclaimedBytes).toBeGreaterThanOrEqual(plan.reclaimedBytes);
  check(applied.reclaimedBytes).toBeGreaterThan(0);
  for (const path of gen.superseded) check(await exists(path), path).toBe(false);
  check(await exists(gen.leafOld)).toBe(true);
  check(await exists(gen.extMeta)).toBe(true);
  check(await snapshot(incremental)).toEqual(incrementalBefore);
  await expectFresh(fx.ws);
  check(execFileSync(join(fx.target, 'debug', 'ret-app'), { encoding: 'utf8' })).toBe('13\n');
  // A repeated pass is a no-op.
  const settled = await snapshot(fx.target, new Set([RETENTION_RECEIPT]));
  const second = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(second.reclaim).toEqual([]);
  check(second.reclaimedBytes).toBe(0);
  check(await snapshot(fx.target, new Set([RETENTION_RECEIPT]))).toEqual(settled);
}

/** Case: a no-op pass reads current fingerprints once without async dispatch or duplicate type stats. */
async function noOpPassReadsFingerprintsOnce(fx: Churn): Promise<void> {
  const check = labelled('a no-op pass reads current fingerprints once without async dispatch or duplicate type stats');
  await clearReceipt(fx.target);
  planned(await retain(fx.ws, { apply: true, now: future() }));
  const module = join(import.meta.dir, '..', 'dist', 'cargo-retention.js');
  const source = lines(
    'import fs from "node:fs";',
    'import promises from "node:fs/promises";',
    'import cp from "node:child_process";',
    'import { syncBuiltinESMExports } from "node:module";',
    `const target = ${JSON.stringify(fx.target)};`,
    'const reads = [], stats = [], asyncFingerprintCalls = [], children = [];',
    'const spawn = cp.spawn; cp.spawn = function(command, ...args) { children.push(command); return spawn.call(this, command, ...args); };',
    'for (const [name, rows] of [["readFileSync", reads], ["lstatSync", stats]]) {',
    '  const original = fs[name];',
    '  fs[name] = function(path, ...args) {',
    '    if (String(path).startsWith(target + "/") && String(path).includes("/fingerprint")) rows.push(String(path));',
    '    return original.call(this, path, ...args);',
    '  };',
    '}',
    'for (const name of ["readFile", "lstat", "readdir"]) {',
    '  const original = promises[name];',
    '  promises[name] = function(path, ...args) {',
    '    if (String(path).startsWith(target + "/") && String(path).includes("/fingerprint")) asyncFingerprintCalls.push(String(path));',
    '    return original.call(this, path, ...args);',
    // Load the product only after installing forwarding I/O counters; static imports run before them.
    '  };',
    '}',
    'syncBuiltinESMExports();',
    `const { retainCargoWorkspace } = await import(${JSON.stringify(module)});`,
    `const result = await retainCargoWorkspace(${JSON.stringify(fx.ws)}, { apply: process.argv[1] === 'automatic', automatic: process.argv[1] === 'automatic', now: new Date(${future().getTime()}) });`,
    'if (!result.ok) throw new Error(JSON.stringify(result.error));',
    'console.log(JSON.stringify({ report: result.value, reads, stats, asyncFingerprintCalls, children }));',
  );
  const child = spawnSync('node', ['--input-type=module', '-e', source], {
    cwd: fx.ws,
    env: { ...process.env },
    encoding: 'utf8',
  });
  check(child.status, child.stderr).toBe(0);
  const checked = validateScanWork(JSON.parse(child.stdout));
  if (!checked.success) throw new Error(JSON.stringify(checked.errors));
  const { report, reads, stats, asyncFingerprintCalls } = checked.data;
  check(planned(report).reclaim).toEqual([]);
  check(reads.length).toBeGreaterThan(0);
  check(new Set(reads).size).toBe(reads.length);
  check(new Set(stats).size).toBe(stats.length);
  check(asyncFingerprintCalls).toEqual([]);
  check(reads.filter((path) => !path.includes('/fingerprint/'))).toEqual([]);
  check(stats.filter((path) => path.endsWith('.json'))).toEqual([]);
  const jsonCount = reads.filter((path) => path.endsWith('.json')).length;
  check(reads.length).toBe(2 * jsonCount);
  const hashPaths = reads.filter((path) => !path.endsWith('.json'));
  for (const path of hashPaths)
    check(
      stats.filter((stat) => stat === path),
      path,
    ).toHaveLength(1);
  const automatic = spawnSync('node', ['--input-type=module', '-e', source, 'automatic'], {
    cwd: fx.ws,
    env: { ...process.env },
    encoding: 'utf8',
  });
  check(automatic.status, automatic.stderr).toBe(0);
  const notDue = validateScanWork(JSON.parse(automatic.stdout));
  if (!notDue.success) throw new Error(JSON.stringify(notDue.errors));
  check(notDue.data.report.outcome.kind).toBe('not-due');
  check(notDue.data.reads).toEqual([]);
  check(notDue.data.stats).toEqual([]);
  check(notDue.data.asyncFingerprintCalls).toEqual([]);
  check(notDue.data.children).not.toContain('perl');
  await expectFresh(fx.ws);
}

/** Case: retains foreign entries and fingerprint-less directories while reclaiming around them. */
async function keepsForeignEntries(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled('retains foreign entries and fingerprint-less directories while reclaiming around them');
  const loose = join(fx.target, 'debug', 'build', 'loose.txt');
  const notHex = join(fx.target, 'debug', 'build', 'ret-mid', 'not-hex');
  const emptyHex = join(fx.target, 'debug', 'build', 'ret-leaf', '0123456789abcdef');
  await put(loose, 'loose\n');
  await put(join(notHex, 'junk.txt'), 'junk\n');
  await mkdir(emptyHex, { recursive: true });
  await put(join(fx.target, 'keep.txt'), 'keep\n');
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(reclaimPaths(applied)).toEqual([...superseded]);
  check(keepReason(applied, loose)).toBe('not-a-unit');
  check(keepReason(applied, notHex)).toBe('not-a-unit');
  check(keepReason(applied, emptyHex)).toBe('no-fingerprint');
  check(await readFile(loose, 'utf8')).toBe('loose\n');
  check(await readFile(join(notHex, 'junk.txt'), 'utf8')).toBe('junk\n');
  check(await readFile(join(fx.target, 'keep.txt'), 'utf8')).toBe('keep\n');
  await expectFresh(fx.ws);
  // The foreign entries have served their assertions; removing them keeps the walk's generation listings exact.
  await rm(loose);
  await rm(notHex, { recursive: true });
  await rm(emptyHex, { recursive: true });
  await rm(join(fx.target, 'keep.txt'));
}

/** Case: never follows a symlinked generation directory; the link is retained and its target untouched. */
async function keepsSymlinkedGeneration(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled('never follows a symlinked generation directory or a symlinked fingerprint file');
  const outside = join(fx.root, 'outside');
  await put(join(outside, 'sentinel.txt'), 'sentinel\n');
  const link = join(fx.target, 'debug', 'build', 'ret-mid', 'fedcba9876543210');
  await symlink(outside, link, 'dir');
  const outsideBefore = await snapshot(outside);
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(reclaimPaths(applied)).toEqual([...superseded]);
  const retainedLink = applied.retained.find((entry) => entry.path === link);
  check(retainedLink?.reason).toBe('not-a-unit');
  check(retainedLink?.detail).toBe('symbolic link');
  check((await lstat(link)).isSymbolicLink()).toBe(true);
  check(await snapshot(outside)).toEqual(outsideBefore);
  await expectFresh(fx.ws);
  await rm(link);
}

/** Case: deletes only proven trash from an interrupted pass and retains the unproven. */
async function reclaimsOnlyProvenTrash(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled('deletes only proven trash from an interrupted pass and retains the unproven');
  const provenVictim = superseded[0];
  const mismatchedVictim = superseded[1];
  if (provenVictim === undefined || mismatchedVictim === undefined) {
    throw new Error('the churn left fewer than two superseded generations');
  }
  const trash = join(fx.target, 'debug', TRASH);
  const passId = randomUUID();
  const passDir = join(trash, passId);
  await mkdir(passDir, { recursive: true });
  const entryOf = async (from: string, inoDelta: bigint) => {
    const stats = await lstat(from, { bigint: true });
    return { from, dev: stats.dev.toString(), ino: (stats.ino + inoDelta).toString() };
  };
  const manifest = {
    version: 1,
    entries: [await entryOf(provenVictim, 0n), await entryOf(mismatchedVictim, 9n)],
  };
  await writeFile(join(trash, `${passId}.json`), JSON.stringify(manifest));
  await rename(provenVictim, join(passDir, '0'));
  await rename(mismatchedVictim, join(passDir, '1'));
  await put(join(passDir, '2', 'sentinel.txt'), 'unlisted\n');
  await put(join(trash, 'stray.txt'), 'stray\n');
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(reclaimPaths(applied)).toContain(provenVictim);
  check(await exists(join(passDir, '0'))).toBe(false);
  check(keepReason(applied, join(passDir, '1'))).toBe('unproven-trash-entry');
  check(keepReason(applied, join(passDir, '2'))).toBe('unproven-trash-entry');
  check(keepReason(applied, join(trash, 'stray.txt'))).toBe('unproven-trash-entry');
  check(await exists(join(passDir, '1'))).toBe(true);
  check(await readFile(join(passDir, '2', 'sentinel.txt'), 'utf8')).toBe('unlisted\n');
  check(await readFile(join(trash, 'stray.txt'), 'utf8')).toBe('stray\n');
  await expectFresh(fx.ws);
  await rm(trash, { recursive: true });
}

/** Case: leaves a nested target directory byte-identical and reports it as a nested root. */
async function nestedRootStaysByteIdentical(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled('leaves a nested target directory byte-identical and reports it as a nested root');
  const nestedWs = join(fx.root, 'nested');
  await put(
    join(nestedWs, 'Cargo.toml'),
    lines('[package]', 'name = "ret-nested"', 'version = "0.1.0"', 'edition = "2021"', '', '[workspace]'),
  );
  await put(join(nestedWs, 'src/lib.rs'), lines('pub fn nested() -> u8 { 7 }'));
  relock(nestedWs);
  const nestedTarget = join(fx.target, 'nested-root');
  await scopedTo(nestedTarget, async () => {
    await build(nestedWs);
  });
  const nestedBefore = await snapshot(nestedTarget);
  const report = await retain(fx.ws, { apply: true, now: future() });
  const applied = planned(report);
  check(report.nestedRoots, JSON.stringify(await readdir(nestedTarget))).toEqual([
    join(report.targetDir, 'nested-root'),
  ]);
  check(reclaimPaths(applied)).toEqual([...superseded]);
  check(await snapshot(nestedTarget)).toEqual(nestedBefore);
  await scopedTo(nestedTarget, async () => {
    await expectFresh(nestedWs);
  });
  await expectFresh(fx.ws);
  await rm(nestedTarget, { recursive: true });
}

/** Case: the CLI plans read-only, applies with a receipt, and exits 3 on a refused pass. */
async function cliPlansReadOnlyAndApplies(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled('the CLI plans read-only, applies with a receipt, and exits 3 on a refused pass');
  await clearReceipt(fx.target);
  const bin = join(import.meta.dir, '..', 'dist', 'bin', 'smoo-nx-cargo-retain.js');
  const cli = async (args: string[]): Promise<CargoRun> => {
    const child = Bun.spawn(['node', bin, ...args], { cwd: fx.ws, env: cargoEnv(), stdout: 'pipe', stderr: 'pipe' });
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    return { code, stdout, stderr };
  };
  const before = await snapshot(fx.target);
  const planRun = await cli([fx.ws]);
  check(planRun.code, planRun.stderr).toBe(0);
  const planReport = parseReport(planRun.stdout);
  check(planned(planReport).kind).toBe('planned');
  check(await snapshot(fx.target)).toEqual(before);
  check(await exists(join(fx.target, RETENTION_RECEIPT))).toBe(false);
  await backdate([...superseded]);
  const applyRun = await cli(['--apply', fx.ws]);
  check(applyRun.code, applyRun.stderr).toBe(0);
  const applyReport = parseReport(applyRun.stdout);
  check(reclaimPaths(planned(applyReport))).toEqual([...superseded]);
  const receipt = parseReport(await readFile(join(fx.target, RETENTION_RECEIPT), 'utf8'));
  check(receipt).toEqual(applyReport);
  for (const path of superseded) check(await exists(path), path).toBe(false);
  await expectFresh(fx.ws);
  const legacy = join(fx.target, 'debug', '.fingerprint');
  await mkdir(legacy, { recursive: true });
  const refusedRun = await cli([fx.ws]);
  check(refusedRun.code, refusedRun.stderr).toBe(3);
  check(refused(parseReport(refusedRun.stdout)).reason).toContain('legacy');
  await rm(legacy, { recursive: true });
}

/** Case: retains an incomplete (empty-hash) superseded generation while reclaiming complete ones. */
async function keepsIncompleteGeneration(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled('retains an incomplete (empty-hash) superseded generation while reclaiming complete ones');
  const midMetas = await metasOf(fx.target, 'ret-mid');
  const husk = superseded.find((path) => midMetas.includes(path));
  if (husk === undefined) throw new Error('no superseded ret-mid generation to leave incomplete');
  await emptyHash(husk);
  await backdate([husk]);
  const rest = superseded.filter((path) => path !== husk);
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(reclaimPaths(applied)).toEqual(rest);
  check(keepReason(applied, husk)).toBe('incomplete-fingerprint');
  check(await exists(husk)).toBe(true);
  await expectFresh(fx.ws);
  await rm(husk, { recursive: true });
}

/** Case: a half-written current generation nothing kept depends on stays the root and its older generation is reclaimed. */
async function keepsHalfWrittenCurrentRoot(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled(
    'a half-written current generation nothing kept depends on stays the root and its older generation is reclaimed',
  );
  let currentBin: string | undefined;
  for (const meta of await metasOf(fx.target, 'ret-app')) {
    if (superseded.includes(meta)) continue;
    const names = await readdir(join(meta, 'fingerprint'));
    if (names.some((name) => name.startsWith('bin-') && name.endsWith('.json'))) currentBin = meta;
  }
  if (currentBin === undefined) throw new Error('no current bin generation for ret-app');
  await emptyHash(currentBin);
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(keepReason(applied, currentBin)).toBe('newest-of-identity');
  check(reclaimPaths(applied)).toEqual([...superseded]);
  check(await exists(currentBin)).toBe(true);
  const rebuilt = await buildJson(fx.ws);
  const compiled = rebuilt.filter((artifact) => !artifact.fresh);
  check(compiled.length).toBeGreaterThan(0);
  check(compiled.filter((artifact) => !artifact.packageId.includes('ret-app'))).toEqual([]);
  await expectFresh(fx.ws);
}

/** Case: a copied bin generation whose exact target digit differs is its own identity: kept, not collapsed. */
async function keepsCopyWithExactDigits(fx: Churn): Promise<void> {
  const check = labelled('keeps exact u64 fingerprint digits distinct, parser and end-to-end');
  let binMeta: string | undefined;
  let binJson = '';
  for (const meta of await metasOf(fx.target, 'ret-app')) {
    const names = await readdir(join(meta, 'fingerprint'));
    const found = names.find((name) => name.startsWith('bin-') && name.endsWith('.json'));
    if (found !== undefined) {
      binMeta = meta;
      binJson = found;
    }
  }
  if (binMeta === undefined) throw new Error('no bin generation for ret-app');
  const name = binMeta.slice(binMeta.lastIndexOf('/') + 1);
  const flipped = (name.startsWith('0') ? '1' : '0') + name.slice(1);
  const copy = join(dirname(binMeta), flipped);
  await cp(binMeta, copy, { recursive: true });
  const copyJson = join(copy, 'fingerprint', binJson);
  const text = await readFile(copyJson, 'utf8');
  const match = /"target":(\d+)/.exec(text);
  if (match === null || match[1] === undefined) throw new Error(`no target digits in ${text}`);
  const digits = BigInt(match[1]);
  const neighbor = Number(digits) === Number(digits + 1n) ? digits + 1n : digits - 1n;
  await writeFile(copyJson, text.replace(`"target":${match[1]}`, `"target":${neighbor.toString()}`));
  await backdate([copy]);
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(keepReason(applied, copy)).toBe('newest-of-identity');
  check(keepReason(applied, binMeta)).toBe('newest-of-identity');
  check(await exists(copy)).toBe(true);
  await expectFresh(fx.ws);
  await rm(copy, { recursive: true });
}

/** Case: invents no churn for source edits, reverts, or whole cold profiles. */
async function sourceEditsInventNoChurn(fx: Churn): Promise<void> {
  const check = labelled('invents no churn for source edits, reverts, or whole cold profiles');
  await build(fx.ws);
  await build(fx.ws, ['--release']);
  const leafSource = join(fx.ws, 'crates/leaf/src/lib.rs');
  // A source edit rehashes fingerprints inside the same generations.
  await put(
    leafSource,
    lines('pub fn base() -> u8 { 5 }', '', '#[cfg(feature = "extra")]', 'pub fn extra() -> u8 { 2 }'),
  );
  await build(fx.ws);
  const afterEdit = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(afterEdit.reclaim).toEqual([]);
  await expectFresh(fx.ws);
  // Reverting to the exact earlier content reuses the same generations again.
  await put(
    leafSource,
    lines('pub fn base() -> u8 { 1 }', '', '#[cfg(feature = "extra")]', 'pub fn extra() -> u8 { 2 }'),
  );
  await build(fx.ws);
  // Cargo also sees the source revert in release; establish its fresh baseline before retention.
  await build(fx.ws, ['--release']);
  const afterRevert = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(afterRevert.reclaim).toEqual([]);
  await expectFresh(fx.ws);
  await expectFresh(fx.ws, ['--release']);
  // The release profile has served its assertions; removing it returns the target to the debug walk's layout.
  await rm(join(fx.target, 'release'), { recursive: true });
}

/** Case: a half-written library keeps dangling generations and adds no recompilation to Cargo recovery. */
async function halfWrittenLibraryRecovers(fx: Churn, superseded: readonly string[]): Promise<void> {
  const title = 'a half-written library keeps dangling generations and adds no recompilation to Cargo recovery';
  const midMetas = await metasOf(fx.target, 'ret-mid');
  const older = superseded.find((path) => midMetas.includes(path));
  if (older === undefined) throw new Error('no superseded ret-mid generation');
  const current = only(
    midMetas.filter((path) => !superseded.includes(path)),
    'current ret-mid generation',
  );
  const recoveries: string[][][] = [];
  for (const apply of [false, true]) {
    const check = labelled(`${title} (${apply ? 'retained' : 'control'})`);
    await emptyHash(current);
    if (apply) {
      const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
      check(keepReason(applied, current)).toBe('newest-of-identity');
      check(keepReason(applied, older)).toBe('dangling-dependency');
      check(reclaimPaths(applied)).toEqual(superseded.filter((path) => path !== older));
      check(await exists(older)).toBe(true);
    }
    // Cargo discovers the incomplete library first, then its binary's changed
    // dependency. Compare both builds with an untouched control, not a warm-up.
    const first = (await buildJson(fx.ws)).filter((artifact) => !artifact.fresh);
    const second = (await buildJson(fx.ws)).filter((artifact) => !artifact.fresh);
    check(first.some((artifact) => artifact.name === 'ret_mid')).toBe(true);
    check([...first, ...second].filter((artifact) => ['ret_leaf', 'ret_ext'].includes(artifact.name))).toEqual([]);
    recoveries.push([first, second].map((artifacts) => artifacts.map((artifact) => artifact.name).sort()));
    await expectFresh(fx.ws);
  }
  labelled(title)(recoveries[1]).toEqual(recoveries[0]);
  await rm(older, { recursive: true });
}

/** Case: automatic cadence uses only successful matching receipts; due and manual runs read the current graph. */
async function automaticCadence(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled(
    'automatic cadence uses only successful matching receipts and due/manual runs read the current graph',
  );
  await clearReceipt(fx.target);
  const now = new Date();
  const first = await retain(fx.ws, { apply: true, automatic: true, now });
  check(planned(first).reclaim).toEqual([]);
  if (first.completed === undefined) throw new Error('first successful apply has no completion');
  const receiptPath = join(fx.target, RETENTION_RECEIPT);
  const before = await readFile(receiptPath, 'utf8');
  const json = join(superseded[0] ?? '', 'fingerprint');
  const name = (await readdir(json)).find((entry) => entry.endsWith('.json'));
  if (name === undefined) throw new Error('fixture has no fingerprint JSON');
  const path = join(json, name);
  const original = await readFile(path, 'utf8');
  await writeFile(path, '{');
  const within = new Date(now.getTime() + 1);
  check((await retain(fx.ws, { apply: true, automatic: true, now: within })).outcome.kind).toBe('not-due');
  check(await readFile(receiptPath, 'utf8')).toBe(before);
  check(refused(await retain(fx.ws, { apply: false, now: within })).reason).toContain('fingerprint');
  const forced = await retain(fx.ws, { apply: true, now: within });
  check(forced.outcome.kind).toBe('refused');
  check(forced.completed).toBeUndefined();
  const invalidClock = await retainCargoWorkspace(fx.ws, { apply: true, automatic: true, now: new Date(Number.NaN) });
  check(invalidClock.ok).toBe(false);
  const due = new Date(now.getTime() + AUTOMATIC_INTERVAL_MS);
  const failed = await retain(fx.ws, { apply: true, automatic: true, now: due });
  check(failed.outcome.kind).toBe('refused');
  check(failed.completed).toBeUndefined();
  await writeFile(path, original);
  const recovered = await retain(fx.ws, { apply: true, automatic: true, now: due });
  check(reclaimPaths(planned(recovered))).toEqual([...superseded]);
  check(recovered.completed?.at).toBe(due.getTime());
  // Invalid/foreign/backward/busy evidence cannot suppress a real current pass.
  const invalidReceipts = [
    '{',
    JSON.stringify({ ...first, workspaceRoot: fx.root }),
    JSON.stringify({ ...first, targetDir: fx.root }),
    JSON.stringify({ ...first, completed: { ...first.completed, at: now.getTime() + 1 } }),
    JSON.stringify({ ...first, completed: { ...first.completed, configuration: 'foreign' } }),
    JSON.stringify({ ...first, outcome: { kind: 'busy', lock: 'held' } }),
    JSON.stringify({ ...first, outcome: { kind: 'refused', profile: 'debug', reason: 'unreadable' } }),
  ];
  for (const [index, invalid] of invalidReceipts.entries()) {
    await writeFile(receiptPath, invalid);
    check((await retain(fx.ws, { apply: true, automatic: true, now })).outcome.kind, `invalid receipt ${index}`).toBe(
      'reclaimed',
    );
  }
  await writeFile(receiptPath, before);
  await put(join(fx.ws, '.cargo/config.toml'), fixtureConfig(true));
  check((await retain(fx.ws, { apply: true, automatic: true, now })).outcome.kind).toBe('reclaimed');
  await expectFresh(fx.ws);
}

/** Case: reclaims an old dependency version no kept unit reaches, and keeps the reached one. */
async function reclaimsUnreachedDependencyVersion(fx: Churn): Promise<void> {
  const check = labelled('reclaims an old dependency version no kept unit reaches, and keeps the reached one');
  await build(fx.ws);
  const extOld = only(await metasOf(fx.target, 'ret-ext'), 'ret-ext 0.1 generation');
  await put(join(fx.ext, 'Cargo.toml'), extManifest('0.2.0'));
  await writeMid(fx.ws, false, '0.2');
  relock(fx.ws);
  await build(fx.ws);
  const extAll = await metasOf(fx.target, 'ret-ext');
  const extNew = only(
    extAll.filter((path) => path !== extOld),
    'ret-ext 0.2 generation',
  );
  const plan = planned(await retain(fx.ws, { apply: false, now: future() }));
  check(reclaimPaths(plan)).toContain(extOld);
  check(keepReason(plan, extNew)).toBe('dependency-of-kept');
  planned(await retain(fx.ws, { apply: true, now: future() }));
  check(await exists(extOld)).toBe(false);
  check(await exists(extNew)).toBe(true);
  await expectFresh(fx.ws);
}

/** Case: keeps an older generation still referenced by a kept unit while reclaiming its unreferenced successor. */
async function keepsReferencedOlderDependency(fx: Churn): Promise<void> {
  const check = labelled(
    'keeps an older generation still referenced by a kept unit while reclaiming its unreferenced successor',
  );
  const extA = only(await metasOf(fx.target, 'ret-ext'), 'first ret-ext generation');
  // ret-ext gains a dependency; built alone it makes a newer generation of the same
  // identity, while the workspace's newest units still reference the first one.
  const ext2 = join(fx.root, 'ext2');
  await put(
    join(ext2, 'Cargo.toml'),
    lines('[package]', 'name = "ret-ext2"', 'version = "0.1.0"', 'edition = "2021"', '', '[workspace]'),
  );
  await put(join(ext2, 'src/lib.rs'), lines('pub fn value() -> u8 { 100 }'));
  await put(
    join(fx.ext, 'Cargo.toml'),
    lines(
      '[package]',
      'name = "ret-ext"',
      'version = "0.2.0"',
      'edition = "2021"',
      '',
      '[dependencies]',
      'ret-ext2 = { path = "../ext2" }',
      '',
      '[workspace]',
    ),
  );
  await put(join(fx.ext, 'src/lib.rs'), lines('pub fn value() -> u8 { ret_ext2::value() + 10 }'));
  relock(fx.ws);
  relock(fx.ext);
  await cargoOk(fx.ext, ['build', '--locked', '--offline']);
  const extB = only(
    (await metasOf(fx.target, 'ret-ext')).filter((path) => path !== extA),
    'second ret-ext generation',
  );
  const ext2Meta = only(await metasOf(fx.target, 'ret-ext2'), 'ret-ext2 generation');
  const plan = planned(await retain(fx.ws, { apply: false, now: future() }));
  check(keepReason(plan, extA)).toBe('dependency-of-kept');
  check(reclaimPaths(plan)).toContain(extB);
  check(reclaimPaths(plan)).toContain(ext2Meta);
  planned(await retain(fx.ws, { apply: true, now: future() }));
  check(await exists(extA)).toBe(true);
  check(await exists(extB)).toBe(false);
  // Cargo's own dirtiness from the manifest change rebuilds the ext chain and its
  // workspace dependents, but the cleaner cost the build nothing else: leaf stays Fresh.
  const rebuilt = await buildJson(fx.ws);
  check(rebuilt.filter((a) => a.packageId.includes('ret-leaf') && !a.fresh)).toEqual([]);
  check(rebuilt.filter((a) => a.packageId.includes('ret-ext') && !a.fresh).length).toBeGreaterThan(0);
  await expectFresh(fx.ws);
}

describe('cargo retention against real Cargo target layouts', () => {
  it('walks the retention cases over one churned Cargo target, asserting after every step (mtime-on-use false)', async () => {
    keepsExactDigitsInTheParser();
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        // Refusals first: each asserts its refusal and puts back the bytes it touched.
        await refusesOverUnreadableFingerprint(fx);
        await refusesWithoutPerl(fx);
        await refusesLegacyLayout(fx);
        await refusesSplitBuildDir(fx);
        await refusesSymlinkedFingerprint(fx);
        // The first plan and reclaim use the generations churnGenerations made.
        const plan = await plansAndKeepsRecent(fx, gen, RECLAIM_TITLE(false));
        await reclaimsSupersededAndRepeats(fx, gen, plan, RECLAIM_TITLE(false));
        // Each later step flips the feature first, so the generations it supersedes are exactly the ones that flip made stale.
        await toggleFeature(fx);
        await noOpPassReadsFingerprintsOnce(fx);
        await keepsForeignEntries(fx, await toggleFeature(fx));
        await keepsSymlinkedGeneration(fx, await toggleFeature(fx));
        await reclaimsOnlyProvenTrash(fx, await toggleFeature(fx));
        await nestedRootStaysByteIdentical(fx, await toggleFeature(fx));
        await cliPlansReadOnlyAndApplies(fx, await toggleFeature(fx));
        await keepsIncompleteGeneration(fx, await toggleFeature(fx));
        await keepsHalfWrittenCurrentRoot(fx, await toggleFeature(fx));
        // These need no superseded generation: the step above left the target clean.
        await keepsCopyWithExactDigits(fx);
        await sourceEditsInventNoChurn(fx);
        await halfWrittenLibraryRecovers(fx, await toggleFeature(fx));
        // Last, because its mtime-on-use config change persists to the end of the walk.
        await automaticCadence(fx, await toggleFeature(fx));
      });
    });
  });

  it(RECLAIM_TITLE(true), async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, true);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const plan = await plansAndKeepsRecent(fx, gen, RECLAIM_TITLE(true));
        await reclaimsSupersededAndRepeats(fx, gen, plan, RECLAIM_TITLE(true));
      });
    });
  });

  it('reclaims an old dependency version no kept unit reaches, and keeps an older generation still referenced by a kept unit', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await reclaimsUnreachedDependencyVersion(fx);
        await keepsReferencedOlderDependency(fx);
      });
    });
  });

  it('keeps a proc macro only a cross build uses, whose unit lives in the host profile', async () => {
    await inFixture(async (root) => {
      const version = execFileSync('rustc', ['-vV'], { encoding: 'utf8' });
      const host = /^host: (.+)$/m.exec(version)?.[1];
      if (host === undefined) throw new Error('rustc -vV named no host triple');
      const ws = join(root, 'ws');
      const pm = join(root, 'pm');
      const target = join(ws, 'target');
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
      await scopedTo(target, async () => {
        await build(ws, ['--target', host]);
        const pmMetas = await metasOf(target, 'ret-pm');
        expect(pmMetas.length).toBeGreaterThan(0);
        const plan = planned(await retain(ws, { apply: false, now: future() }));
        for (const pmMeta of pmMetas) expect(keepReason(plan, pmMeta)).toBe('dependency-of-kept');
        planned(await retain(ws, { apply: true, now: future() }));
        for (const pmMeta of pmMetas) expect(await exists(pmMeta)).toBe(true);
        await expectFresh(ws, ['--target', host]);
        const appMeta = only(await metasOf(target, 'ret-app'), 'cross app generation');
        const fingerprint = join(appMeta, 'fingerprint', 'lib-ret_app.json');
        const original = await readFile(fingerprint, 'utf8');
        for (const foreign of [
          original.replace('"dep_info":"', '"dep_info":"foreign/'),
          original.replace(/"local":\[.*?\],"rustflags"/, '"local":[],"rustflags"'),
        ]) {
          expect(foreign).not.toBe(original);
          await writeFile(fingerprint, foreign);
          const before = await snapshot(target);
          const denied = await retain(ws, { apply: false, now: future() });
          expect(refused(denied).reason).toMatch(/unproven|no fingerprint local path/);
          expect(await snapshot(target)).toEqual(before);
        }
      });
    });
  });

  it('reports busy without waiting or changing anything while a real Cargo holds the profile', async () => {
    await inFixture(async (root) => {
      const ws = join(root, 'ws');
      const target = join(ws, 'target');
      await put(join(ws, '.cargo/config.toml'), fixtureConfig(false));
      await put(
        join(ws, 'Cargo.toml'),
        lines('[package]', 'name = "ret-held"', 'version = "0.1.0"', 'edition = "2021"', '', '[workspace]'),
      );
      await put(join(ws, 'src/lib.rs'), lines('pub fn held() -> u8 { 1 }'));
      await put(
        join(ws, 'build.rs'),
        lines(
          'fn main() {',
          '    let ready = std::env::var("RET_HELD_READY").unwrap();',
          '    let release = std::env::var("RET_HELD_RELEASE").unwrap();',
          '    std::fs::write(&ready, "ready").unwrap();',
          '    let _ = std::fs::read(&release).unwrap();',
          '}',
        ),
      );
      relock(ws);
      await scopedTo(target, async () => {
        const readyFifo = join(root, 'ready.fifo');
        const releaseFifo = join(root, 'release.fifo');
        execFileSync('mkfifo', [readyFifo, releaseFifo]);
        const env = { ...cargoEnv(), RET_HELD_READY: readyFifo, RET_HELD_RELEASE: releaseFifo };
        const child = Bun.spawn(['cargo', 'build', '--locked', '--offline', '--jobs', '1'], {
          cwd: ws,
          env,
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const stderrText = new Response(child.stderr).text();
        void new Response(child.stdout).text();
        let exited = false;
        const ended = child.exited.then((code) => {
          exited = true;
          return code;
        });
        try {
          const said = await Promise.race([
            readFile(readyFifo, 'utf8'),
            ended.then(async (code) => {
              throw new Error(`cargo ended ${code} before its build script held: ${await stderrText}`);
            }),
          ]);
          expect(said).toBe('ready');
          const buildDir = join(target, 'debug', 'build');
          const before = await snapshot(buildDir);
          const report = await retain(ws, { apply: true });
          expect(exited).toBe(false);
          if (report.outcome.kind !== 'busy') {
            throw new Error(`expected busy while cargo is held, got ${JSON.stringify(report.outcome)}`);
          }
          expect(report.outcome.lock).toContain('.cargo');
          expect(await snapshot(buildDir)).toEqual(before);
          expect(await exists(join(target, 'debug', TRASH))).toBe(false);
          await writeFile(releaseFifo, 'go\n');
          const code = await ended;
          if (code !== 0) throw new Error(`the held cargo build failed: ${await stderrText}`);
          await expectFresh(ws);
        } finally {
          child.kill();
          await ended;
        }
      });
    });
  });

  it('accepts a real pre-created Cargo target without a backup tag, but refuses a foreign root or forged tag', async () => {
    await inFixture(async (root) => {
      const empty = await retainCargoWorkspace(root, { apply: false });
      if (empty.ok) throw new Error('an empty directory produced a report');
      expect(empty.error.kind).toBe('cargo-metadata');
      const fx = await churnFixture(root, false);
      await mkdir(fx.target, { recursive: true });
      await scopedTo(fx.target, async () => {
        const foreign = await retainCargoWorkspace(fx.ws, { apply: false });
        if (foreign.ok) throw new Error('an unbuilt target produced a report');
        expect(foreign.error.kind).toBe('target-not-cargo');
        const gen = await churnGenerations(fx);
        expect(await exists(join(fx.target, 'CACHEDIR.TAG'))).toBe(false);
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(reclaimPaths(applied)).toEqual(gen.superseded);
        await expectFresh(fx.ws);
        await writeFile(join(fx.target, 'CACHEDIR.TAG'), 'bogus\n');
        const forged = await retainCargoWorkspace(fx.ws, { apply: false, now: future() });
        if (forged.ok) throw new Error('a forged CACHEDIR.TAG produced a report');
        expect(forged.error.kind).toBe('target-not-cargo');
      });
    });
  });

  it('a real Nx run reclaims after executed Cargo producer tasks, not after a noop, a fetch or a cache replay', async () => {
    await withNxFixture('cargo-retention-hook', async ({ workspace, socketDirectory }) => {
      await withEnv(
        {
          CARGO_TARGET_DIR: undefined,
          CARGO_BUILD_DIR: undefined,
          CARGO_BUILD_TARGET: undefined,
          RUSTC_WRAPPER: '',
          RUSTC_WORKSPACE_WRAPPER: '',
        },
        async () => {
          const git = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
          expect(git.exitCode, git.stderr.toString()).toBe(0);
          await symlink(await installation.nodeModules(), join(workspace, 'node_modules'), 'dir');
          await put(
            join(workspace, 'nx.json'),
            JSON.stringify({ cacheDirectory: '.nx/cache', plugins: ['@smoothbricks/nx-plugin'] }),
          );
          const fx = await churnFixture(workspace, false);
          await put(
            join(fx.ws, 'project.json'),
            JSON.stringify({
              name: 'ws',
              targets: {
                'noop-only': { executor: 'nx:noop', cache: false },
                'fetch-only': {
                  executor: 'nx:run-commands',
                  cache: false,
                  options: { cwd: 'ws', command: 'cargo fetch --locked --offline' },
                },
                'build-cargo-fixture': {
                  executor: 'nx:run-commands',
                  cache: true,
                  metadata: { cargoProducer: true },
                  inputs: ['{projectRoot}/marker.txt'],
                  outputs: ['{projectRoot}/result.txt'],
                  options: {
                    cwd: 'ws',
                    command: 'cargo build --workspace --locked --offline && cp marker.txt result.txt',
                  },
                },
                'build-fails': {
                  executor: 'nx:run-commands',
                  cache: false,
                  metadata: { cargoProducer: true },
                  options: { cwd: 'ws', command: 'cargo build --workspace --locked --offline && exit 3' },
                },
                'build-uncached': {
                  executor: 'nx:run-commands',
                  cache: false,
                  metadata: { cargoProducer: true },
                  options: { cwd: 'ws', command: 'cargo build --workspace --locked --offline' },
                },
              },
            }),
          );
          await put(join(fx.ws, 'marker.txt'), 'one\n');
          const gen = await churnGenerations(fx);
          await backdate(gen.superseded);
          const env = fixtureNxEnv(workspace, socketDirectory);
          const runNx = async (target: string, preload?: string) => {
            const nx = join(workspace, 'node_modules', '.bin', 'nx');
            // A preload is a Node `--import`: it patches the process the hook runs in, which here is Node itself.
            const argv =
              preload === undefined
                ? [process.execPath, nx, 'run', target, '--outputStyle=static']
                : ['node', '--import', pathToFileURL(preload).href, nx, 'run', target, '--outputStyle=static'];
            const child = Bun.spawn(argv, { cwd: workspace, env, stdout: 'pipe', stderr: 'pipe' });
            const [code, stdout, stderr] = await Promise.all([
              child.exited,
              new Response(child.stdout).text(),
              new Response(child.stderr).text(),
            ]);
            return { code, log: stdout + stderr };
          };
          const receiptPath = join(fx.target, RETENTION_RECEIPT);
          const noop = await runNx('ws:noop-only');
          expect(noop.code, noop.log).toBe(0);
          expect(await exists(receiptPath)).toBe(false);
          const fetched = await runNx('ws:fetch-only');
          expect(fetched.code, fetched.log).toBe(0);
          expect(await exists(receiptPath)).toBe(false);
          for (const path of gen.superseded) expect(await exists(path)).toBe(true);
          const built = await runNx('ws:build-cargo-fixture');
          expect(built.code, built.log).toBe(0);
          const receipt = parseReport(await readFile(receiptPath, 'utf8'));
          expect(reclaimPaths(planned(receipt))).toEqual(gen.superseded);
          for (const path of gen.superseded) expect(await exists(path)).toBe(false);
          await expectFresh(fx.ws);
          // A second toggle leaves backdated superseded generations again; a cache replay must not touch them.
          const current = [...(await metasOf(fx.target, 'ret-mid')), ...(await metasOf(fx.target, 'ret-app'))].sort();
          await writeMid(fx.ws, false, '0.1');
          relock(fx.ws);
          await build(fx.ws);
          if (current.length === 0) throw new Error('the second toggle left no generation to supersede');
          await backdate(current);
          const receiptBefore = await readFile(receiptPath, 'utf8');
          const replayed = await runNx('ws:build-cargo-fixture');
          expect(replayed.code, replayed.log).toBe(0);
          expect(replayed.log).toContain('local cache');
          expect(await readFile(receiptPath, 'utf8')).toBe(receiptBefore);
          for (const path of current) expect(await exists(path)).toBe(true);
          const notDue = await runNx('ws:build-fails');
          expect(notDue.code, notDue.log).not.toBe(0);
          expect(notDue.log).toContain('automatic maintenance not due');
          expect(await readFile(receiptPath, 'utf8')).toBe(receiptBefore);
          for (const path of current) expect(await exists(path)).toBe(true);
          if (receipt.completed === undefined) throw new Error('successful hook did not record completion');
          // Advance eligibility through the receipt clock, without sleeping or changing Cargo's stamps.
          await writeFile(
            receiptPath,
            JSON.stringify({ ...receipt, completed: { ...receipt.completed, at: Date.now() - AUTOMATIC_INTERVAL_MS } }),
          );
          const failed = await runNx('ws:build-fails');
          expect(failed.code, failed.log).not.toBe(0);
          const receipt2 = parseReport(await readFile(receiptPath, 'utf8'));
          expect(reclaimPaths(planned(receipt2))).toEqual(current);
          for (const path of current) expect(await exists(path)).toBe(false);
          await expectFresh(fx.ws);
          // A sandbox that refuses the lock by every route its platform has: the run still succeeds, and the hook says
          // why it skipped retention. The EPERM is thrown in a real Node, which is what throws it from spawn().
          if (receipt2.completed === undefined) throw new Error('second successful hook did not record completion');
          const preload = join(workspace, 'deny-locks.mjs');
          await put(preload, denyPreload(['perl'], true));
          await writeFile(
            receiptPath,
            JSON.stringify({
              ...receipt2,
              completed: { ...receipt2.completed, at: Date.now() - AUTOMATIC_INTERVAL_MS },
            }),
          );
          const dueReceipt = await readFile(receiptPath, 'utf8');
          const skipped = await runNx('ws:build-uncached', preload);
          expect(skipped.code, skipped.log).toBe(0);
          expect(skipped.log).toMatch(/Cargo retention .*: refused, .*: cannot attempt .*EPERM/);
          expect(await readFile(receiptPath, 'utf8')).toBe(dueReceipt);
          // Whatever else escapes a pass is reported as well, never rethrown: here cargo itself cannot be spawned.
          await put(preload, denyPreload(['cargo'], false));
          const escaped = await runNx('ws:build-uncached', preload);
          expect(escaped.code, escaped.log).toBe(0);
          expect(escaped.log).toMatch(/Cargo retention for .*: unexpected failure: .*EPERM/s);
          expect(await readFile(receiptPath, 'utf8')).toBe(dueReceipt);
        },
      );
    });
  });

  it('a cache-restored nextest archive still runs green after retention reclaims generations', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const archive = join(fx.ws, 'archive.tar.zst');
        const archived = spawnSync(
          'cargo',
          ['--frozen', 'nextest', 'archive', '--archive-file', archive, '--workspace', '--user-config-file', 'none'],
          { cwd: fx.ws, env: cargoEnv(), encoding: 'utf8' },
        );
        expect({ status: archived.status, stderr: archived.status === 0 ? '' : archived.stderr }).toEqual({
          status: 0,
          stderr: '',
        });
        // The bytes an Nx cache restore would hand back, in a fresh place.
        const restored = join(root, 'restored.tar.zst');
        await cp(archive, restored);
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(reclaimPaths(applied)).toEqual(expect.arrayContaining(gen.superseded));
        const extracted = spawnSync(
          'node',
          [join(import.meta.dir, '..', 'dist', 'bin', 'smoo-nx-nextest-extract.js'), restored],
          { cwd: fx.ws, env: cargoEnv(), encoding: 'utf8' },
        );
        expect(extracted.status, extracted.stderr).toBe(0);
        const extractionDirectory = extracted.stdout.trim();
        const ran = spawnSync(
          'cargo',
          [
            '--frozen',
            'nextest',
            'run',
            '--binaries-metadata',
            join(extractionDirectory, EXTRACTED_BINARIES_METADATA),
            '--cargo-metadata',
            join(extractionDirectory, EXTRACTED_CARGO_METADATA),
            '--target-dir-remap',
            join(extractionDirectory, EXTRACTED_TARGET_DIR),
            '--workspace-remap',
            '.',
            '--user-config-file',
            'none',
            '--color',
            'never',
          ],
          { cwd: fx.ws, env: cargoEnv(), encoding: 'utf8' },
        );
        expect({ status: ran.status, passed: (ran.stderr + ran.stdout).includes('1 passed') }).toEqual({
          status: 0,
          passed: true,
        });
      });
    });
  });
});
