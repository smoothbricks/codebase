import { describe, expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { chmod, cp, lstat, mkdir, readdir, readFile, realpath, rename, rm, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import {
  AUTOMATIC_INTERVAL_MS,
  describeRetention,
  exactIdentity,
  fingerprintIdentity,
  parseExactJson,
  RECENT_USE_MS,
  RETENTION_RECEIPT,
  readCargoFingerprint,
  refusedAnywhere,
  retainCargoWorkspace,
} from './cargo-retention.js';
import {
  artifacts,
  backdate,
  build,
  buildJson,
  type CargoRun,
  type Churn,
  cargoEnv,
  cargoOk,
  churnFixture,
  churnGenerations,
  clearLock,
  clearReceipt,
  DAY3,
  emptyHash,
  exists,
  expectFresh,
  extManifest,
  fixtureConfig,
  future,
  type Generations,
  holdProfile,
  inFixture,
  keepReason,
  labelled,
  lines,
  metasOf,
  midAndApp,
  nestedOf,
  nestedPlanned,
  only,
  type PlannedOutcome,
  type ProcMacro,
  parseReport,
  planned,
  procMacroFixture,
  put,
  RETENTION_LOCK,
  type RetentionRun,
  reclaimPaths,
  refused,
  relock,
  retain,
  retainWithoutPerl,
  scopedTo,
  snapshot,
  TRASH,
  toggleFeature,
  validateScanWork,
  withEnv,
  writeMid,
} from './testing/cargo-fixture.js';
import { useFixtureCargoHome } from './testing/fixture-cargo-home.js';

useFixtureCargoHome();

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

/** Case: a toolchain is no part of a unit's identity, so the same unit built by a newer compiler supersedes the older one's. */
function identityIgnoresToolchain(): void {
  const check = labelled('a newer toolchain supersedes an older one: identity ignores rustc and the --sysroot flag');
  const identity = (rustc: string, rustflags: string, features = '[]'): string => {
    const parsed = readCargoFingerprint(
      `{"rustc":${rustc},"features":${JSON.stringify(features)},"target":17066949999414752808,"profile":1,"rustflags":${rustflags},"deps":[],"local":[]}`,
    );
    if (!parsed.ok) throw new Error(parsed.error);
    return fingerprintIdentity('ret-leaf', 'lib-ret_leaf', parsed.value);
  };
  const base = identity('8522741132697106019', '["--sysroot","/nix/store/a-rust","-C","target-cpu=native"]');
  check(
    identity('4410052297246879569', '["--sysroot","/nix/store/b-rust","-C","target-cpu=native"]'),
    'another compiler and sysroot',
  ).toBe(base);
  check(identity('4410052297246879569', '["-C","target-cpu=native"]'), 'a compiler run without the sysroot flag').toBe(
    base,
  );
  check(
    identity('4410052297246879569', '["--sysroot=/nix/store/c-rust","-C","target-cpu=native"]'),
    'the joined sysroot spelling',
  ).toBe(base);
  check(identity('8522741132697106019', '["-C","target-cpu=generic"]'), 'another flag').not.toBe(base);
  check(
    identity('8522741132697106019', '["--sysroot","/nix/store/a-rust","-C","target-cpu=native"]', '["extra"]'),
    'another feature set',
  ).not.toBe(base);
}

/** Case: the trash manifest proves a generation by device and inode digits, so a number a double rounds proves nothing. */
function exactIdentityRefusesWhatADoubleCannotHold(): void {
  const check = labelled('exactIdentity refuses a device or inode a double cannot hold');
  check(exactIdentity({ dev: 16777232, ino: 4294967296 }), 'ordinary numbers').toEqual({
    dev: '16777232',
    ino: '4294967296',
  });
  check(exactIdentity({ dev: 1, ino: Number.MAX_SAFE_INTEGER }), 'the largest exact inode').toEqual({
    dev: '1',
    ino: '9007199254740991',
  });
  check(exactIdentity({ dev: 1, ino: Number.MAX_SAFE_INTEGER + 1 }), 'an inode 2^53 or more').toBeUndefined();
  check(exactIdentity({ dev: 2 ** 60, ino: 1 }), 'a device 2^53 or more').toBeUndefined();
  check(exactIdentity({ dev: 1, ino: Number.NaN }), 'not a number').toBeUndefined();
}

/** What `JSON.parse` makes of `text` with every integer a bigint from its source digits: the oracle for the exact reader. */
function oracleParse(text: string): unknown {
  return JSON.parse(text, (_key: string, value: unknown, context?: { readonly source?: string }) =>
    typeof value === 'number' && context?.source !== undefined ? BigInt(context.source) : value,
  );
}

/** Case: the exact JSON reader keeps u64 digits, refuses a non-integer and malformed text, and keeps `__proto__` data. Pure values. */
function exactJsonReaderIsExactAndStrict(): void {
  const check = labelled('the exact JSON reader keeps u64 digits and refuses what is not an integer or not JSON');
  const read = (text: string): unknown => {
    const parsed = parseExactJson(text);
    if (!parsed.ok) throw new Error(`${text}: ${parsed.error}`);
    return parsed.value;
  };
  check(read('18446744073709551615'), 'the largest u64').toBe(18446744073709551615n);
  check(read('[-0, 0, -17, 9007199254740993]'), 'a double would round the last').toEqual([
    0n,
    0n,
    -17n,
    9007199254740993n,
  ]);
  check(read(' {"a" : [true,false,null,"x\\u0041\\n"] , "b":{}} '), 'every JSON form').toEqual({
    a: [true, false, null, 'xA\n'],
    b: {},
  });
  check(read('{"a":1,"a":2}'), 'a later duplicate name wins, as in JSON.parse').toEqual({ a: 2n });
  const proto = read('{"__proto__":{"polluted":1}}');
  check(Object.getPrototypeOf(proto), '__proto__ stays data').toBe(Object.prototype);
  check(Object.hasOwn(Object(proto), '__proto__'), '__proto__ is an own property').toBe(true);
  for (const bad of [
    '',
    '{',
    '[',
    '[1,]',
    '[1 2]',
    '{"a":}',
    '{"a" 1}',
    '{a:1}',
    '1.5',
    '1e3',
    '01',
    '-',
    '"a',
    '"\\q"',
    '"tab\there"',
    'nul',
    '{"a":1}x',
  ]) {
    check(parseExactJson(bad).ok, JSON.stringify(bad)).toBe(false);
  }
  check(parseExactJson(`${'['.repeat(65)}${']'.repeat(65)}`).ok, 'nesting past the limit').toBe(false);
  check(parseExactJson(`${'['.repeat(60)}${']'.repeat(60)}`).ok, 'nesting inside the limit').toBe(true);
}

/** Case: the exact reader reads every fingerprint Cargo wrote as `JSON.parse` with its source-text reviver does. */
async function exactReaderAgreesWithJsonParse(fx: Churn): Promise<void> {
  const check = labelled('the exact JSON reader agrees with JSON.parse on every real fingerprint');
  let compared = 0;
  for (const pkg of ['ret-leaf', 'ret-mid', 'ret-app', 'ret-ext']) {
    for (const meta of await metasOf(fx.target, pkg)) {
      const fingerprints = join(meta, 'fingerprint');
      for (const name of (await readdir(fingerprints)).filter((file) => file.endsWith('.json'))) {
        const text = await readFile(join(fingerprints, name), 'utf8');
        const parsed = parseExactJson(text);
        check(parsed.ok, `${pkg} ${name} reads`).toBe(true);
        if (parsed.ok) check(parsed.value, `${pkg} ${name}`).toEqual(oracleParse(text));
        compared += 1;
      }
    }
  }
  check(compared, 'fingerprints compared').toBeGreaterThan(3);
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
  // Earlier steps applied and left the lock file; a refused attempt must not create it again.
  await clearLock(fx.target);
  const bin = join(fx.root, 'bin');
  await mkdir(bin);
  for (const tool of ['cargo', 'rustc']) {
    const real = Bun.which(tool);
    if (real === null) throw new Error(`${tool} is not on PATH`);
    await symlink(real, join(bin, tool));
  }
  const buildDir = join(fx.target, 'debug', 'build');
  const before = await snapshot(buildDir);
  // Earlier walk steps may have left a receipt or trash; a refused pass changes neither.
  const receiptPath = join(fx.target, RETENTION_RECEIPT);
  const receiptBefore = (await exists(receiptPath)) ? await readFile(receiptPath, 'utf8') : null;
  const trashBefore = await exists(join(fx.target, 'debug', TRASH));
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
  check((await exists(receiptPath)) ? await readFile(receiptPath, 'utf8') : null, 'receipt unchanged').toBe(
    receiptBefore,
  );
  check(await exists(join(fx.target, 'debug', TRASH)), 'trash unchanged').toBe(trashBefore);
}

/** A profile whose only unit records are a legacy layout's: Cargo's locks, an empty `build/`, `.fingerprint` and `deps`. */
async function plantLegacyOnlyProfile(fx: Churn): Promise<string> {
  const profile = join(fx.target, 'legacy-profile');
  await put(join(profile, '.cargo-lock'), '');
  await put(join(profile, '.cargo-build-lock'), '');
  await mkdir(join(profile, 'build'), { recursive: true });
  await put(join(profile, '.fingerprint', 'legacy-unit-0123456789abcdef', 'lib-legacy-unit.json'), '{}\n');
  await mkdir(join(profile, 'deps'));
  return profile;
}

/** Case: refuses a profile that holds only an old layout's records instead of reporting a clean zero. */
async function refusesLegacyOnlyProfile(fx: Churn): Promise<void> {
  const check = labelled('refuses a legacy-layout profile instead of reporting a clean zero');
  const profile = await plantLegacyOnlyProfile(fx);
  const buildDir = join(fx.target, 'debug', 'build');
  const before = await snapshot(buildDir);
  const report = await retain(fx.ws, { apply: true, now: future() });
  check(refused(report).reason).toContain('legacy');
  check(refused(report).profile, 'names the profile').toBe(profile);
  check(await snapshot(buildDir)).toEqual(before);
  await rm(profile, { recursive: true });
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

/** Case: a profile that still holds an old layout's records reclaims its new-layout generations and reports the old ones retained. */
async function reclaimsAroundLegacyEntries(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled(
    'reclaims the new-layout generations of a profile that also holds the legacy layout, and reports it',
  );
  const profile = join(fx.target, 'debug');
  const records = join(profile, '.fingerprint');
  const outputs = join(profile, 'deps');
  const script = join(profile, 'build', 'legacy-build-0123456789abcdef');
  await put(
    join(records, 'legacy-unit-0123456789abcdef', 'lib-legacy-unit.json'),
    '{"rustc":1,"deps":[],"local":[]}\n',
  );
  await put(join(outputs, 'liblegacy_unit-0123456789abcdef.rlib'), 'legacy\n');
  await put(join(script, 'output'), 'legacy\n');
  const legacy = [records, outputs, script];
  const before = await Promise.all(legacy.map((path) => snapshot(path)));
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(reclaimPaths(applied)).toEqual([...superseded]);
  for (const path of legacy) check(keepReason(applied, path), path).toBe('legacy-layout');
  check(
    applied.retained.filter((entry) => entry.path.startsWith(`${script}/`)),
    'one entry per legacy build-script directory',
  ).toEqual([]);
  check(await Promise.all(legacy.map((path) => snapshot(path)))).toEqual(before);
  await expectFresh(fx.ws);
  for (const path of legacy) await rm(path, { recursive: true });
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

/** Case: keeps a nested target root holding no unit of a workspace member whole, and reports it as a nested root. */
async function keepsForeignNestedRoot(fx: Churn, superseded: readonly string[]): Promise<void> {
  const check = labelled('keeps a nested target root holding no unit of a workspace member whole and reports it');
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
  const nestedRoot = join(report.targetDir, 'nested-root');
  check(
    report.nestedRoots.map((nested) => nested.root),
    JSON.stringify(await readdir(nestedTarget)),
  ).toEqual([nestedRoot]);
  check(reclaimPaths(applied)).toEqual([...superseded]);
  // Nothing proves the root is this workspace's, so the selection never runs over it.
  const foreign = nestedPlanned(report, nestedRoot);
  check(foreign.reclaim).toEqual([]);
  check(keepReason(foreign, nestedRoot)).toBe('foreign-root');
  check(await snapshot(nestedTarget)).toEqual(nestedBefore);
  await scopedTo(nestedTarget, async () => {
    await expectFresh(nestedWs);
  });
  await expectFresh(fx.ws);
  await rm(nestedTarget, { recursive: true });
}

/**
 * Case: reclaims a nested target root's superseded generations under its own locks and keeps its newest. The root holds
 * the same workspace, as `cargo clippy --target-dir target/<name>` leaves it; the flip supersedes generations in both
 * roots, and one pass reclaims each root's own and none across.
 */
async function reclaimsNestedRoot(fx: Churn): Promise<void> {
  const check = labelled(
    "reclaims a nested target root's superseded generations under its own locks and keeps its newest",
  );
  const nested = join(fx.target, 'nested-root');
  const buildNested = (): Promise<void> =>
    scopedTo(nested, async () => {
      await build(fx.ws);
    });
  await buildNested();
  const leafOld = only(await metasOf(nested, 'ret-leaf'), 'nested ret-leaf generation');
  const extMeta = only(await metasOf(nested, 'ret-ext'), 'nested ret-ext generation');
  // The flip supersedes every mid and app generation built so far, the nested root's among them once it is rebuilt.
  const stale = await toggleFeature(fx);
  await buildNested();
  const nestedStale = stale.filter((path) => path.startsWith(`${nested}/`));
  const ownStale = stale.filter((path) => !path.startsWith(`${nested}/`));
  if (nestedStale.length === 0 || ownStale.length === 0)
    throw new Error('the flip left a root with nothing superseded');
  const incremental = join(nested, 'debug', 'incremental');
  const incrementalBefore = await snapshot(incremental);
  // A plan writes nothing and names each root's own superseded generations.
  const beforePlan = await snapshot(nested);
  const planReport = await retain(fx.ws, { apply: false, now: future() });
  check(reclaimPaths(planned(planReport))).toEqual(ownStale);
  const nestedPlan = nestedPlanned(planReport, nested);
  check(nestedPlan.kind).toBe('planned');
  check(reclaimPaths(nestedPlan)).toEqual(nestedStale);
  check(nestedPlan.reclaimedBytes).toBeGreaterThan(0);
  check(keepReason(nestedPlan, leafOld)).toBe('newest-of-identity');
  check(keepReason(nestedPlan, extMeta)).toBe('dependency-of-kept');
  check(describeRetention(planReport)).toContain(`nested ${nested}: would reclaim`);
  check(await snapshot(nested)).toEqual(beforePlan);
  // Applying reclaims both roots' superseded generations, keeps their newest, and leaves every profile in place.
  const report = await retain(fx.ws, { apply: true, now: future() });
  check(reclaimPaths(planned(report))).toEqual(ownStale);
  const applied = nestedPlanned(report, nested);
  check(applied.kind).toBe('reclaimed');
  check(reclaimPaths(applied)).toEqual(nestedStale);
  check(report.completed, 'every root reclaimed').toBeDefined();
  for (const path of stale) check(await exists(path), path).toBe(false);
  for (const path of [leafOld, extMeta, ...(await midAndApp(fx.target))]) check(await exists(path), path).toBe(true);
  for (const lock of ['.cargo-lock', '.cargo-build-lock']) {
    check(await exists(join(nested, 'debug', lock)), lock).toBe(true);
  }
  check(await snapshot(incremental)).toEqual(incrementalBefore);
  await scopedTo(nested, async () => {
    await expectFresh(fx.ws);
  });
  await expectFresh(fx.ws);
  // A repeated pass is a no-op in every root.
  const second = await retain(fx.ws, { apply: true, now: future() });
  check(planned(second).reclaim).toEqual([]);
  check(nestedPlanned(second, nested).reclaim).toEqual([]);
  await rm(nested, { recursive: true });
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
  const legacy = await plantLegacyOnlyProfile(fx);
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

/** Case: passes a cross-built nested root's host and cross profiles as one root, and skips it while one is locked. */
async function passesCrossBuiltNestedRoot(fx: ProcMacro): Promise<void> {
  const check = labelled(
    "passes a cross-built nested root's host and cross profiles as one root and skips it while one is locked",
  );
  const nested = join(fx.target, 'nested-root');
  const crossLock = join(nested, fx.host, 'debug', '.cargo-lock');
  const buildNested = (): Promise<void> =>
    scopedTo(nested, async () => {
      await build(fx.ws, ['--target', fx.host]);
    });
  await buildNested();
  const appOld = await metasOf(nested, 'ret-app');
  const pmOld = await metasOf(nested, 'ret-pm');
  // A new proc macro version supersedes the host generation of the proc macro and the app built on it.
  const pmManifest = join(fx.pm, 'Cargo.toml');
  await writeFile(pmManifest, (await readFile(pmManifest, 'utf8')).replace('version = "0.1.0"', 'version = "0.1.1"'));
  relock(fx.ws);
  await buildNested();
  const appNew = (await metasOf(nested, 'ret-app')).filter((path) => !appOld.includes(path));
  const pmNew = (await metasOf(nested, 'ret-pm')).filter((path) => !pmOld.includes(path));
  if (appNew.length === 0 || pmNew.length === 0) throw new Error('the proc macro version bump made no new generation');
  // A Cargo build holds the cross profile, a directory the target's own scan never reaches.
  const release = await holdProfile(crossLock);
  try {
    const before = await snapshot(nested);
    const locked = await retain(fx.ws, { apply: true, now: future() });
    check(await snapshot(nested)).toEqual(before);
    check(nestedOf(locked, nested)).toEqual({ kind: 'busy', lock: crossLock });
    check(planned(locked).kind).toBe('reclaimed');
    check(locked.completed, 'a busy root completes nothing').toBeUndefined();
  } finally {
    await release();
  }
  // Released, both profiles are one root: the superseded app and the proc macro only it used go, and the host-profile
  // proc macro the kept cross-profile app uses stays.
  const report = await retain(fx.ws, { apply: true, now: future() });
  const applied = nestedPlanned(report, nested);
  check(reclaimPaths(applied)).toEqual([...appOld, ...pmOld].sort());
  for (const path of appNew) check(keepReason(applied, path), path).toBe('newest-of-identity');
  for (const path of pmNew) check(keepReason(applied, path), path).toBe('dependency-of-kept');
  check(report.completed, 'every root reclaimed').toBeDefined();
  for (const path of [...appOld, ...pmOld]) check(await exists(path), path).toBe(false);
  for (const path of [...appNew, ...pmNew]) check(await exists(path), path).toBe(true);
  for (const profile of [join(nested, 'debug'), join(nested, fx.host, 'debug')]) {
    for (const lock of ['.cargo-lock', '.cargo-build-lock']) {
      check(await exists(join(profile, lock)), `${profile} ${lock}`).toBe(true);
    }
  }
  await scopedTo(nested, async () => {
    await expectFresh(fx.ws, ['--target', fx.host]);
  });
  // A profile of the nested root that it cannot read refuses that root alone: the target directory's pass stands.
  const [unit] = appNew;
  if (unit === undefined) throw new Error('no new ret-app generation');
  const fingerprints = join(unit, 'fingerprint');
  const jsonName = (await readdir(fingerprints)).find((name) => name.endsWith('.json'));
  if (jsonName === undefined) throw new Error(`no fingerprint json under ${fingerprints}`);
  await writeFile(join(fingerprints, jsonName), '{');
  const refusal = await retain(fx.ws, { apply: true, now: future() });
  check(planned(refusal).kind).toBe('reclaimed');
  check(nestedOf(refusal, nested)).toMatchObject({ kind: 'refused', profile: join(nested, fx.host, 'debug') });
  check(refusedAnywhere(refusal)).toBe(true);
  check(refusal.completed, 'a refused root completes nothing').toBeUndefined();
}

/** What Cargo hashes of a toolchain: the compiler's version text, and the flags (a sysroot among them) it is run with. */
interface Toolchain {
  readonly env: Record<string, string>;
}

/** The real compiler under a script that adds a line of its own to `-vV` and a sysroot path of its own to its flags. */
async function toolchain(root: string, name: string): Promise<Toolchain> {
  const real = Bun.which('rustc');
  if (real === null) throw new Error('rustc is not on PATH');
  const sysroot = execFileSync(real, ['--print', 'sysroot'], { encoding: 'utf8' }).trim();
  const directory = join(root, `toolchain-${name}`);
  await mkdir(directory, { recursive: true });
  const quoted = `'${real.replaceAll("'", "'\\''")}'`;
  const compiler = join(directory, 'rustc');
  await writeFile(
    compiler,
    lines(
      '#!/bin/sh',
      `if [ "$1" = "-vV" ]; then ${quoted} -vV; echo "toolchain: ${name}"; exit 0; fi`,
      `exec ${quoted} "$@"`,
    ),
  );
  await chmod(compiler, 0o755);
  const link = join(directory, 'sysroot');
  await symlink(sysroot, link, 'dir');
  return { env: { RUSTC: compiler, RUSTFLAGS: `--sysroot ${link}` } };
}

/** Case: a newer toolchain supersedes an older one's generations, ordered by last use and never while both are recent. */
async function supersedesOlderToolchain(fx: Churn, older: Toolchain, newer: Toolchain): Promise<void> {
  const check = labelled('a newer toolchain supersedes an older one, ordered by last use, never while both are recent');
  const everything = async (): Promise<string[]> =>
    (await Promise.all(['ret-leaf', 'ret-mid', 'ret-app', 'ret-ext'].map((pkg) => metasOf(fx.target, pkg))))
      .flat()
      .sort();
  const buildWith = (toolchain: Toolchain): Promise<void> =>
    withEnv(toolchain.env, async () => {
      await build(fx.ws);
    });
  await buildWith(older);
  const olderGenerations = await everything();
  await buildWith(newer);
  const newerGenerations = (await everything()).filter((path) => !olderGenerations.includes(path));
  if (olderGenerations.length === 0 || newerGenerations.length === 0) {
    throw new Error('a toolchain made no generations of its own');
  }
  // Both compilers were used a moment ago, so both are roots and nothing goes.
  const recent = planned(await retain(fx.ws, { apply: true }));
  check(recent.reclaim, 'two toolchains in recent use').toEqual([]);
  // Outside the recency window the older one's generations, its dependency units included, are superseded.
  const plan = planned(await retain(fx.ws, { apply: false, now: future() }));
  check(reclaimPaths(plan), 'the older toolchain').toEqual(olderGenerations);
  for (const path of newerGenerations) {
    check(keepReason(plan, path), path).toMatch(/^(newest-of-identity|dependency-of-kept)$/);
  }
  const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
  check(reclaimPaths(applied)).toEqual(olderGenerations);
  for (const path of olderGenerations) check(await exists(path), path).toBe(false);
  for (const path of newerGenerations) check(await exists(path), path).toBe(true);
  await withEnv(newer.env, async () => {
    await expectFresh(fx.ws);
  });
  // The order is last use, not which compiler is which: built again, the older one is the newer.
  await buildWith(older);
  const reversed = planned(await retain(fx.ws, { apply: false, now: future() }));
  check(reclaimPaths(reversed), 'the toolchain used last wins').toEqual(newerGenerations);
}

/** The newest mtime among a generation's hash files: what `-Zmtime-on-use` stamps on every use of its units. */
async function hashMtime(meta: string): Promise<number> {
  const fingerprints = join(meta, 'fingerprint');
  const hashes = (await readdir(fingerprints)).filter(
    (name) => !name.endsWith('.json') && name !== 'invoked.timestamp' && !/^(dep|output)-/.test(name),
  );
  if (hashes.length === 0) throw new Error(`no hash file under ${fingerprints}`);
  const times = await Promise.all(hashes.map(async (name) => (await lstat(join(fingerprints, name))).mtimeMs));
  return Math.max(...times);
}

/** Case: a pass says whether Cargo recorded use, and a no-op build stamps exactly the units it used, when asked to. */
async function reportsRecencyFromStampedUse(fx: Churn, stamping: boolean): Promise<void> {
  const check = labelled(
    `reports recency from stamped use, and a no-op build stamps the units it used (mtime-on-use ${String(stamping)})`,
  );
  const unknown = { kind: 'recency-unknown', reason: 'mtime-on-use off' } as const;
  await build(fx.ws);
  check(planned(await retain(fx.ws, { apply: false })).recency, 'compiled and never reused').toEqual(unknown);
  const used = (await Promise.all(['ret-mid', 'ret-leaf', 'ret-ext'].map((pkg) => metasOf(fx.target, pkg)))).flat();
  const unused = await metasOf(fx.target, 'ret-app');
  // A hash file with no compile stamp beside it has nothing to be newer than: that is no evidence either.
  const stamps = [...used, ...unused].map((meta) => join(meta, 'fingerprint', 'invoked.timestamp'));
  for (const stamp of stamps) await rm(stamp);
  check(planned(await retain(fx.ws, { apply: false })).recency, 'no compile stamps').toEqual(unknown);
  for (const stamp of stamps) await put(stamp, '');
  await backdate([...used, ...unused]);
  const aged = Date.now() - DAY3;
  // `ret-mid` and what it builds on: a no-op build of them, and nothing of `ret-app`.
  const run = await cargoOk(
    fx.ws,
    ['build', '-p', 'ret-mid', '--locked', '--offline', '--message-format=json'],
    cargoEnv(),
  );
  check(
    artifacts(run).filter((artifact) => !artifact.fresh),
    'the no-op build compiled nothing',
  ).toEqual([]);
  const stamped = async (meta: string): Promise<boolean> => (await hashMtime(meta)) > aged + RECENT_USE_MS;
  for (const meta of used) check(await stamped(meta), `${meta}, used`).toBe(stamping);
  for (const meta of unused) check(await stamped(meta), `${meta}, not used`).toBe(false);
  check(planned(await retain(fx.ws, { apply: false })).recency, 'after the reuse').toEqual(
    stamping ? { kind: 'use-stamped' } : unknown,
  );
}

describe('cargo retention against real Cargo target layouts', () => {
  it('walks the retention cases over one churned Cargo target, asserting after every step (mtime-on-use false)', async () => {
    keepsExactDigitsInTheParser();
    identityIgnoresToolchain();
    exactIdentityRefusesWhatADoubleCannotHold();
    exactJsonReaderIsExactAndStrict();
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        await exactReaderAgreesWithJsonParse(fx);
        // Refusals first: each asserts its refusal and puts back the bytes it touched.
        await refusesOverUnreadableFingerprint(fx);
        await refusesWithoutPerl(fx);
        await refusesLegacyOnlyProfile(fx);
        await refusesSplitBuildDir(fx);
        await refusesSymlinkedFingerprint(fx);
        // The first plan and reclaim use the generations churnGenerations made.
        const plan = await plansAndKeepsRecent(fx, gen, RECLAIM_TITLE(false));
        await reclaimsSupersededAndRepeats(fx, gen, plan, RECLAIM_TITLE(false));
        // Each later step flips the feature first, so the generations it supersedes are exactly the ones that flip made stale.
        await toggleFeature(fx);
        await noOpPassReadsFingerprintsOnce(fx);
        await keepsForeignEntries(fx, await toggleFeature(fx));
        await reclaimsAroundLegacyEntries(fx, await toggleFeature(fx));
        await keepsSymlinkedGeneration(fx, await toggleFeature(fx));
        await reclaimsOnlyProvenTrash(fx, await toggleFeature(fx));
        await keepsForeignNestedRoot(fx, await toggleFeature(fx));
        await reclaimsNestedRoot(fx);
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

  it('a newer toolchain supersedes an older one, ordered by last use, never while both are recent', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      const older = await toolchain(root, 'a');
      const newer = await toolchain(root, 'b');
      await scopedTo(fx.target, async () => {
        await supersedesOlderToolchain(fx, older, newer);
      });
    });
  });

  it('reports recency-unknown until Cargo stamps a reused unit, and a no-op build stamps exactly the units it used', async () => {
    for (const stamping of [false, true]) {
      await inFixture(async (root) => {
        const fx = await churnFixture(root, stamping);
        await scopedTo(fx.target, async () => {
          await reportsRecencyFromStampedUse(fx, stamping);
        });
      });
    }
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
      const { ws, target, host } = await procMacroFixture(root);
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

  it("passes a cross-built nested root's host and cross profiles as one root and skips it while one is locked", async () => {
    await inFixture(async (root) => {
      const fx = await procMacroFixture(root);
      await scopedTo(fx.target, async () => {
        await passesCrossBuiltNestedRoot(fx);
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
});
