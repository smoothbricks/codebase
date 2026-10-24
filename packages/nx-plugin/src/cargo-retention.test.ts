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
  type RetentionOptions,
  type RetentionOutcome,
  type RetentionReport,
  readCargoFingerprint,
  retainCargoWorkspace,
} from './cargo-retention.js';
import { EXTRACTED_BINARIES_METADATA, EXTRACTED_CARGO_METADATA, EXTRACTED_TARGET_DIR } from './nextest-extraction.js';

useFixtureCargoHome();

/** The Nx and compiled plugin under test, one copy this file's Nx fixture shares. */
const installation = testFileInstallation('cargo-retention-installation');
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

interface ScanWork {
  readonly report: RetentionReport;
  readonly reads: readonly string[];
  readonly stats: readonly string[];
  readonly asyncFingerprintCalls: readonly string[];
  readonly children: readonly string[];
}
const validateScanWork = typia.createValidateEquals<ScanWork>();
describe('cargo retention against real Cargo target layouts', () => {
  it('a no-op pass reads current fingerprints once without async dispatch or duplicate type stats', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await churnGenerations(fx);
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
        expect(child.status, child.stderr).toBe(0);
        const checked = validateScanWork(JSON.parse(child.stdout));
        if (!checked.success) throw new Error(JSON.stringify(checked.errors));
        const { report, reads, stats, asyncFingerprintCalls } = checked.data;
        expect(planned(report).reclaim).toEqual([]);
        expect(reads.length).toBeGreaterThan(0);
        expect(new Set(reads).size).toBe(reads.length);
        expect(new Set(stats).size).toBe(stats.length);
        expect(asyncFingerprintCalls).toEqual([]);
        expect(reads.filter((path) => !path.includes('/fingerprint/'))).toEqual([]);
        expect(stats.filter((path) => path.endsWith('.json'))).toEqual([]);
        const jsonCount = reads.filter((path) => path.endsWith('.json')).length;
        expect(reads.length).toBe(2 * jsonCount);
        const hashPaths = reads.filter((path) => !path.endsWith('.json'));
        for (const path of hashPaths) expect(stats.filter((stat) => stat === path)).toHaveLength(1);
        const automatic = spawnSync('node', ['--input-type=module', '-e', source, 'automatic'], {
          cwd: fx.ws,
          env: { ...process.env },
          encoding: 'utf8',
        });
        expect(automatic.status, automatic.stderr).toBe(0);
        const notDue = validateScanWork(JSON.parse(automatic.stdout));
        if (!notDue.success) throw new Error(JSON.stringify(notDue.errors));
        expect(notDue.data.report.outcome.kind).toBe('not-due');
        expect(notDue.data.reads).toEqual([]);
        expect(notDue.data.stats).toEqual([]);
        expect(notDue.data.asyncFingerprintCalls).toEqual([]);
        expect(notDue.data.children).not.toContain('perl');
        await expectFresh(fx.ws);
      });
    });
  });
  it('automatic cadence uses only successful matching receipts and due/manual runs read the current graph', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const now = new Date();
        const first = await retain(fx.ws, { apply: true, automatic: true, now });
        expect(planned(first).reclaim).toEqual([]);
        if (first.completed === undefined) throw new Error('first successful apply has no completion');
        const receiptPath = join(fx.target, RETENTION_RECEIPT);
        const before = await readFile(receiptPath, 'utf8');
        const json = join(gen.superseded[0] ?? '', 'fingerprint');
        const name = (await readdir(json)).find((name) => name.endsWith('.json'));
        if (name === undefined) throw new Error('fixture has no fingerprint JSON');
        const path = join(json, name);
        const original = await readFile(path, 'utf8');
        await writeFile(path, '{');
        const within = new Date(now.getTime() + 1);
        expect((await retain(fx.ws, { apply: true, automatic: true, now: within })).outcome.kind).toBe('not-due');
        expect(await readFile(receiptPath, 'utf8')).toBe(before);
        expect(refused(await retain(fx.ws, { apply: false, now: within })).reason).toContain('fingerprint');
        const forced = await retain(fx.ws, { apply: true, now: within });
        expect(forced.outcome.kind).toBe('refused');
        expect(forced.completed).toBeUndefined();
        const invalidClock = await retainCargoWorkspace(fx.ws, {
          apply: true,
          automatic: true,
          now: new Date(Number.NaN),
        });
        expect(invalidClock.ok).toBe(false);
        const due = new Date(now.getTime() + AUTOMATIC_INTERVAL_MS);
        const failed = await retain(fx.ws, { apply: true, automatic: true, now: due });
        expect(failed.outcome.kind).toBe('refused');
        expect(failed.completed).toBeUndefined();
        await writeFile(path, original);
        const recovered = await retain(fx.ws, { apply: true, automatic: true, now: due });
        expect(reclaimPaths(planned(recovered))).toEqual(gen.superseded);
        expect(recovered.completed?.at).toBe(due.getTime());
        // Invalid/foreign/backward/busy evidence cannot suppress a real current pass.
        for (const invalid of [
          '{',
          JSON.stringify({ ...first, workspaceRoot: root }),
          JSON.stringify({ ...first, targetDir: root }),
          JSON.stringify({ ...first, completed: { ...first.completed, at: now.getTime() + 1 } }),
          JSON.stringify({ ...first, completed: { ...first.completed, configuration: 'foreign' } }),
          JSON.stringify({ ...first, outcome: { kind: 'busy', lock: 'held' } }),
          JSON.stringify({ ...first, outcome: { kind: 'refused', profile: 'debug', reason: 'unreadable' } }),
        ]) {
          await writeFile(receiptPath, invalid);
          expect((await retain(fx.ws, { apply: true, automatic: true, now })).outcome.kind).toBe('reclaimed');
        }
        await writeFile(receiptPath, before);
        await put(join(fx.ws, '.cargo/config.toml'), fixtureConfig(true));
        expect((await retain(fx.ws, { apply: true, automatic: true, now })).outcome.kind).toBe('reclaimed');
        await expectFresh(fx.ws);
      });
    });
  });
  for (const mtimeOnUse of [false, true]) {
    it(
      'reclaims exactly the superseded generations and keeps the next build Fresh (mtime-on-use ' +
        String(mtimeOnUse) +
        ')',
      async () => {
        await inFixture(async (root) => {
          const fx = await churnFixture(root, mtimeOnUse);
          await scopedTo(fx.target, async () => {
            const gen = await churnGenerations(fx);
            const incremental = join(fx.target, 'debug', 'incremental');
            // A plan creates nothing and names exactly the superseded generations.
            const beforePlan = await snapshot(fx.target);
            const planReport = await retain(fx.ws, { apply: false, now: future() });
            const plan = planned(planReport);
            expect(plan.kind).toBe('planned');
            expect(planReport.targetDir).toBe(await realpath(fx.target));
            expect(await snapshot(fx.target)).toEqual(beforePlan);
            expect(await exists(join(fx.target, RETENTION_RECEIPT))).toBe(false);
            expect(await exists(join(fx.target, RETENTION_LOCK))).toBe(false);
            expect(reclaimPaths(plan)).toEqual(gen.superseded);
            expect(plan.reclaimedBytes).toBeGreaterThan(0);
            expect(keepReason(plan, gen.leafOld)).toBe('newest-of-identity');
            expect(keepReason(plan, gen.extMeta)).toBe('dependency-of-kept');
            // At real now the same generations are inside the recent window: nothing goes.
            const recent = planned(await retain(fx.ws, { apply: true }));
            expect(recent.reclaim).toEqual([]);
            expect(recent.reclaimedBytes).toBe(0);
            for (const path of gen.superseded) expect(await exists(path)).toBe(true);
            await expectFresh(fx.ws);
            // Outside the window, applying the plan leaves the current build warm.
            const incrementalBefore = await snapshot(incremental);
            const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
            expect(applied.kind).toBe('reclaimed');
            expect(reclaimPaths(applied)).toEqual(gen.superseded);
            expect(applied.reclaimedBytes).toBeGreaterThanOrEqual(plan.reclaimedBytes);
            expect(applied.reclaimedBytes).toBeGreaterThan(0);
            for (const path of gen.superseded) expect(await exists(path)).toBe(false);
            expect(await exists(gen.leafOld)).toBe(true);
            expect(await exists(gen.extMeta)).toBe(true);
            expect(await snapshot(incremental)).toEqual(incrementalBefore);
            await expectFresh(fx.ws);
            expect(execFileSync(join(fx.target, 'debug', 'ret-app'), { encoding: 'utf8' })).toBe('13\n');
            // A repeated pass is a no-op.
            const settled = await snapshot(fx.target, new Set([RETENTION_RECEIPT]));
            const second = planned(await retain(fx.ws, { apply: true, now: future() }));
            expect(second.reclaim).toEqual([]);
            expect(second.reclaimedBytes).toBe(0);
            expect(await snapshot(fx.target, new Set([RETENTION_RECEIPT]))).toEqual(settled);
          });
        });
      },
    );
  }

  it('reclaims an old dependency version no kept unit reaches, and keeps the reached one', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
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
        expect(reclaimPaths(plan)).toContain(extOld);
        expect(keepReason(plan, extNew)).toBe('dependency-of-kept');
        planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(await exists(extOld)).toBe(false);
        expect(await exists(extNew)).toBe(true);
        await expectFresh(fx.ws);
      });
    });
  });

  it('keeps an older generation still referenced by a kept unit while reclaiming its unreferenced successor', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await build(fx.ws);
        const extA = only(await metasOf(fx.target, 'ret-ext'), 'first ret-ext generation');
        // ret-ext gains a dependency; built alone it makes a newer generation of the same
        // identity, while the workspace's newest units still reference the first one.
        const ext2 = join(root, 'ext2');
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
            'version = "0.1.0"',
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
        expect(keepReason(plan, extA)).toBe('dependency-of-kept');
        expect(reclaimPaths(plan)).toContain(extB);
        expect(reclaimPaths(plan)).toContain(ext2Meta);
        planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(await exists(extA)).toBe(true);
        expect(await exists(extB)).toBe(false);
        // Cargo's own dirtiness from the manifest change rebuilds the ext chain and its
        // workspace dependents, but the cleaner cost the build nothing else: leaf stays Fresh.
        const rebuilt = await buildJson(fx.ws);
        expect(rebuilt.filter((a) => a.packageId.includes('ret-leaf') && !a.fresh)).toEqual([]);
        expect(rebuilt.filter((a) => a.packageId.includes('ret-ext') && !a.fresh).length).toBeGreaterThan(0);
        await expectFresh(fx.ws);
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

  it('refuses the whole pass over one unreadable fingerprint and changes nothing', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await churnGenerations(fx);
        const meta = only((await metasOf(fx.target, 'ret-mid')).slice(0, 1), 'a ret-mid generation to corrupt');
        const fingerprints = join(meta, 'fingerprint');
        const jsonName = (await readdir(fingerprints)).find((name) => name.endsWith('.json'));
        if (jsonName === undefined) throw new Error(`no fingerprint json under ${fingerprints}`);
        await writeFile(join(fingerprints, jsonName), '{');
        const buildDir = join(fx.target, 'debug', 'build');
        const before = await snapshot(buildDir);
        const report = await retain(fx.ws, { apply: true, now: future() });
        expect(refused(report).reason).toContain('fingerprint');
        expect(await snapshot(buildDir)).toEqual(before);
      });
    });
  });

  it('retains foreign entries and fingerprint-less directories while reclaiming around them', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const loose = join(fx.target, 'debug', 'build', 'loose.txt');
        const notHex = join(fx.target, 'debug', 'build', 'ret-mid', 'not-hex');
        const emptyHex = join(fx.target, 'debug', 'build', 'ret-leaf', '0123456789abcdef');
        await put(loose, 'loose\n');
        await put(join(notHex, 'junk.txt'), 'junk\n');
        await mkdir(emptyHex, { recursive: true });
        await put(join(fx.target, 'keep.txt'), 'keep\n');
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(reclaimPaths(applied)).toEqual(gen.superseded);
        expect(keepReason(applied, loose)).toBe('not-a-unit');
        expect(keepReason(applied, notHex)).toBe('not-a-unit');
        expect(keepReason(applied, emptyHex)).toBe('no-fingerprint');
        expect(await readFile(loose, 'utf8')).toBe('loose\n');
        expect(await readFile(join(notHex, 'junk.txt'), 'utf8')).toBe('junk\n');
        expect(await readFile(join(fx.target, 'keep.txt'), 'utf8')).toBe('keep\n');
        await expectFresh(fx.ws);
      });
    });
  });

  it('never follows a symlinked generation directory or a symlinked fingerprint file', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const outside = join(root, 'outside');
        await put(join(outside, 'sentinel.txt'), 'sentinel\n');
        const link = join(fx.target, 'debug', 'build', 'ret-mid', 'fedcba9876543210');
        await symlink(outside, link, 'dir');
        const outsideBefore = await snapshot(outside);
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(reclaimPaths(applied)).toEqual(gen.superseded);
        const retainedLink = applied.retained.find((entry) => entry.path === link);
        expect(retainedLink?.reason).toBe('not-a-unit');
        expect(retainedLink?.detail).toBe('symbolic link');
        expect((await lstat(link)).isSymbolicLink()).toBe(true);
        expect(await snapshot(outside)).toEqual(outsideBefore);
        await expectFresh(fx.ws);
        // A symlinked fingerprint hash file inside a real generation refuses the pass.
        const meta = only((await metasOf(fx.target, 'ret-leaf')).slice(0, 1), 'a ret-leaf generation');
        const fingerprints = join(meta, 'fingerprint');
        const kind = (await readdir(fingerprints)).find((name) => name.endsWith('.json'))?.slice(0, -'.json'.length);
        if (kind === undefined) throw new Error(`no fingerprint kind under ${fingerprints}`);
        const hashPath = join(fingerprints, kind);
        const stash = join(root, 'hash-stash');
        await rename(hashPath, stash);
        await symlink(stash, hashPath, 'file');
        const before = await snapshot(join(fx.target, 'debug', 'build'));
        const report = await retain(fx.ws, { apply: true, now: future() });
        expect(refused(report).reason).toContain('fingerprint');
        expect(await snapshot(join(fx.target, 'debug', 'build'))).toEqual(before);
        expect(await readFile(stash, 'utf8')).toMatch(/^[0-9a-f]{16}$/);
      });
    });
  });

  it('refuses a legacy-layout profile instead of reporting a clean zero', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await build(fx.ws);
        const meta = only(await metasOf(fx.target, 'ret-leaf'), 'ret-leaf generation');
        const fingerprints = join(meta, 'fingerprint');
        const jsonName = (await readdir(fingerprints)).find((name) => name.endsWith('.json'));
        if (jsonName === undefined) throw new Error(`no fingerprint json under ${fingerprints}`);
        const legacy = join(fx.target, 'debug', '.fingerprint');
        await mkdir(legacy, { recursive: true });
        await cp(join(fingerprints, jsonName), join(legacy, jsonName));
        const before = await snapshot(join(fx.target, 'debug', 'build'));
        const report = await retain(fx.ws, { apply: true, now: future() });
        expect(refused(report).reason).toContain('legacy');
        expect(await snapshot(join(fx.target, 'debug', 'build'))).toEqual(before);
      });
    });
  });

  it('refuses a profile with Cargo locks but no build directory (split build-dir shape)', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await build(fx.ws);
        const split = join(fx.target, 'split-profile');
        await put(join(split, '.cargo-lock'), '');
        await put(join(split, '.cargo-build-lock'), '');
        const before = await snapshot(join(fx.target, 'debug', 'build'));
        const report = await retain(fx.ws, { apply: false, now: future() });
        expect(refused(report).reason).toBe('no build directory');
        expect(await snapshot(join(fx.target, 'debug', 'build'))).toEqual(before);
      });
    });
  });

  it('leaves a nested target directory byte-identical and reports it as a nested root', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      const nestedWs = join(root, 'nested');
      await put(
        join(nestedWs, 'Cargo.toml'),
        lines('[package]', 'name = "ret-nested"', 'version = "0.1.0"', 'edition = "2021"', '', '[workspace]'),
      );
      await put(join(nestedWs, 'src/lib.rs'), lines('pub fn nested() -> u8 { 7 }'));
      relock(nestedWs);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const nestedTarget = join(fx.target, 'nested-root');
        await scopedTo(nestedTarget, async () => {
          await build(nestedWs);
        });
        const nestedBefore = await snapshot(nestedTarget);
        const report = await retain(fx.ws, { apply: true, now: future() });
        const applied = planned(report);
        expect(report.nestedRoots, JSON.stringify(await readdir(nestedTarget))).toEqual([
          join(report.targetDir, 'nested-root'),
        ]);
        expect(reclaimPaths(applied)).toEqual(gen.superseded);
        expect(await snapshot(nestedTarget)).toEqual(nestedBefore);
        await scopedTo(nestedTarget, async () => {
          await expectFresh(nestedWs);
        });
        await expectFresh(fx.ws);
      });
    });
  });

  it('deletes only proven trash from an interrupted pass and retains the unproven', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const provenVictim = gen.superseded[0];
        const mismatchedVictim = gen.superseded[1];
        if (provenVictim === undefined || mismatchedVictim === undefined) {
          throw new Error('the churn left fewer than two superseded generations');
        }
        const profile = join(fx.target, 'debug');
        const trash = join(profile, TRASH);
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
        expect(reclaimPaths(applied)).toContain(provenVictim);
        expect(await exists(join(passDir, '0'))).toBe(false);
        expect(keepReason(applied, join(passDir, '1'))).toBe('unproven-trash-entry');
        expect(keepReason(applied, join(passDir, '2'))).toBe('unproven-trash-entry');
        expect(keepReason(applied, join(trash, 'stray.txt'))).toBe('unproven-trash-entry');
        expect(await exists(join(passDir, '1'))).toBe(true);
        expect(await readFile(join(passDir, '2', 'sentinel.txt'), 'utf8')).toBe('unlisted\n');
        expect(await readFile(join(trash, 'stray.txt'), 'utf8')).toBe('stray\n');
        await expectFresh(fx.ws);
      });
    });
  });

  it('deletes nothing when the flock helper (perl) is unavailable', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await build(fx.ws);
        const bin = join(root, 'bin');
        await mkdir(bin);
        for (const tool of ['cargo', 'rustc']) {
          const real = Bun.which(tool);
          if (real === null) throw new Error(`${tool} is not on PATH`);
          await symlink(real, join(bin, tool));
        }
        const before = await snapshot(join(fx.target, 'debug', 'build'));
        await withEnv({ PATH: bin }, async () => {
          for (const apply of [false, true]) {
            const result = await retainCargoWorkspace(fx.ws, { apply, now: future() });
            if (result.ok) throw new Error(`retention ran without perl: ${JSON.stringify(result.value)}`);
            expect(result.error.kind).toBe('lock-helper');
          }
        });
        expect(await snapshot(join(fx.target, 'debug', 'build'))).toEqual(before);
      });
    });
  });

  it('keeps exact u64 fingerprint digits distinct, parser and end-to-end', async () => {
    const base = '{"rustc":8522741132697106019,"target":17066949999414752808,"deps":[],"local":[]}';
    const shifted = '{"rustc":8522741132697106019,"target":17066949999414752809,"deps":[],"local":[]}';
    expect(Number(17066949999414752808n)).toBe(Number(17066949999414752809n));
    const a = readCargoFingerprint(base);
    const b = readCargoFingerprint(shifted);
    if (!a.ok || !b.ok) throw new Error('fingerprints unreadable');
    expect(fingerprintIdentity('ret-leaf', 'lib-ret_leaf', a.value)).not.toBe(
      fingerprintIdentity('ret-leaf', 'lib-ret_leaf', b.value),
    );
    expect(readCargoFingerprint('{').ok).toBe(false);
    expect(readCargoFingerprint('{"rustc":1.5,"deps":[]}').ok).toBe(false);
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        await build(fx.ws);
        const appMetas = await metasOf(fx.target, 'ret-app');
        let binMeta: string | undefined;
        let binJson = '';
        for (const meta of appMetas) {
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
        expect(Number(neighbor)).toBe(Number(digits));
        await writeFile(copyJson, text.replace(`"target":${match[1]}`, `"target":${neighbor.toString()}`));
        await backdate([copy]);
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(keepReason(applied, copy)).toBe('newest-of-identity');
        expect(keepReason(applied, binMeta)).toBe('newest-of-identity');
        expect(await exists(copy)).toBe(true);
        await expectFresh(fx.ws);
      });
    });
  });

  it('invents no churn for source edits, reverts, or whole cold profiles', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
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
        expect(afterEdit.reclaim).toEqual([]);
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
        expect(afterRevert.reclaim).toEqual([]);
        await expectFresh(fx.ws);
        await expectFresh(fx.ws, ['--release']);
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

  it('the CLI plans read-only, applies with a receipt, and exits 3 on a refused pass', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const bin = join(import.meta.dir, '..', 'dist', 'bin', 'smoo-nx-cargo-retain.js');
        const cli = async (args: string[]): Promise<CargoRun> => {
          const child = Bun.spawn(['node', bin, ...args], {
            cwd: fx.ws,
            env: cargoEnv(),
            stdout: 'pipe',
            stderr: 'pipe',
          });
          const [code, stdout, stderr] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
          ]);
          return { code, stdout, stderr };
        };
        const before = await snapshot(fx.target);
        const planRun = await cli([fx.ws]);
        expect(planRun.code, planRun.stderr).toBe(0);
        const planReport = parseReport(planRun.stdout);
        expect(planned(planReport).kind).toBe('planned');
        expect(await snapshot(fx.target)).toEqual(before);
        expect(await exists(join(fx.target, RETENTION_RECEIPT))).toBe(false);
        await backdate(gen.superseded);
        const applyRun = await cli(['--apply', fx.ws]);
        expect(applyRun.code, applyRun.stderr).toBe(0);
        const applyReport = parseReport(applyRun.stdout);
        expect(reclaimPaths(planned(applyReport))).toEqual(gen.superseded);
        const receipt = parseReport(await readFile(join(fx.target, RETENTION_RECEIPT), 'utf8'));
        expect(receipt).toEqual(applyReport);
        for (const path of gen.superseded) expect(await exists(path)).toBe(false);
        await expectFresh(fx.ws);
        const legacy = join(fx.target, 'debug', '.fingerprint');
        await mkdir(legacy, { recursive: true });
        const refusedRun = await cli([fx.ws]);
        expect(refusedRun.code, refusedRun.stderr).toBe(3);
        expect(refused(parseReport(refusedRun.stdout)).reason).toContain('legacy');
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
              },
            }),
          );
          await put(join(fx.ws, 'marker.txt'), 'one\n');
          const gen = await churnGenerations(fx);
          await backdate(gen.superseded);
          const env = fixtureNxEnv(workspace, socketDirectory);
          const runNx = async (target: string) => {
            const child = Bun.spawn(
              [process.execPath, join(workspace, 'node_modules', '.bin', 'nx'), 'run', target, '--outputStyle=static'],
              { cwd: workspace, env, stdout: 'pipe', stderr: 'pipe' },
            );
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
        },
      );
    });
  });

  it('retains an incomplete (empty-hash) superseded generation while reclaiming complete ones', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const midMetas = await metasOf(fx.target, 'ret-mid');
        const husk = gen.superseded.find((path) => midMetas.includes(path));
        if (husk === undefined) throw new Error('no superseded ret-mid generation to leave incomplete');
        await emptyHash(husk);
        await backdate([husk]);
        const rest = gen.superseded.filter((path) => path !== husk);
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(reclaimPaths(applied)).toEqual(rest);
        expect(keepReason(applied, husk)).toBe('incomplete-fingerprint');
        expect(await exists(husk)).toBe(true);
        await expectFresh(fx.ws);
      });
    });
  });

  it('a half-written current generation nothing kept depends on stays the root and its older generation is reclaimed', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        let currentBin: string | undefined;
        for (const meta of await metasOf(fx.target, 'ret-app')) {
          if (gen.superseded.includes(meta)) continue;
          const names = await readdir(join(meta, 'fingerprint'));
          if (names.some((name) => name.startsWith('bin-') && name.endsWith('.json'))) currentBin = meta;
        }
        if (currentBin === undefined) throw new Error('no current bin generation for ret-app');
        await emptyHash(currentBin);
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(keepReason(applied, currentBin)).toBe('newest-of-identity');
        expect(reclaimPaths(applied)).toEqual(gen.superseded);
        expect(await exists(currentBin)).toBe(true);
        const rebuilt = await buildJson(fx.ws);
        const compiled = rebuilt.filter((a) => !a.fresh);
        expect(compiled.length).toBeGreaterThan(0);
        expect(compiled.filter((a) => !a.packageId.includes('ret-app'))).toEqual([]);
        await expectFresh(fx.ws);
      });
    });
  });

  it('a half-written library keeps dangling generations and adds no recompilation to Cargo recovery', async () => {
    await inFixture(async (root) => {
      const recoveries: string[][][] = [];
      for (const apply of [false, true]) {
        const fx = await churnFixture(join(root, apply ? 'retained' : 'control'), false);
        await scopedTo(fx.target, async () => {
          const gen = await churnGenerations(fx);
          const midMetas = await metasOf(fx.target, 'ret-mid');
          const older = gen.superseded.find((path) => midMetas.includes(path));
          const current = only(
            midMetas.filter((path) => !gen.superseded.includes(path)),
            'current ret-mid generation',
          );
          if (older === undefined) throw new Error('no superseded ret-mid generation');
          await emptyHash(current);
          if (apply) {
            const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
            expect(keepReason(applied, current)).toBe('newest-of-identity');
            expect(keepReason(applied, older)).toBe('dangling-dependency');
            expect(reclaimPaths(applied)).toEqual(gen.superseded.filter((path) => path !== older));
            expect(await exists(older)).toBe(true);
          }
          // Cargo discovers the incomplete library first, then its binary's changed
          // dependency. Compare both builds with an untouched control, not a warm-up.
          const first = (await buildJson(fx.ws)).filter((artifact) => !artifact.fresh);
          const second = (await buildJson(fx.ws)).filter((artifact) => !artifact.fresh);
          expect(first.some((artifact) => artifact.name === 'ret_mid')).toBe(true);
          expect([...first, ...second].filter((artifact) => ['ret_leaf', 'ret_ext'].includes(artifact.name))).toEqual(
            [],
          );
          recoveries.push([first, second].map((artifacts) => artifacts.map((artifact) => artifact.name).sort()));
          await expectFresh(fx.ws);
        });
      }
      expect(recoveries[1]).toEqual(recoveries[0]);
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
