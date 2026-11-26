import { afterAll, afterEach, beforeAll, expect, it } from 'bun:test';
import { type ChildProcess, spawn } from 'node:child_process';
import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { delimiter, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { countedCargo } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, nxFixtureRoot } from './__tests__/fixture-nx-env.js';
import { testFileInstallation } from './__tests__/owned-installation.js';
import { SERIAL_TEST_GROUP_PLATFORM } from './cargo-workspace.js';
import type { NxTaskContext } from './executors/bounded-exec/executor.js';
import type { BoundedExecOptions, NativeBoundedExecOptions } from './executors/bounded-exec/schema.js';
import {
  type BoundedExecRecord,
  JUNIT_FILE,
  readRecord,
  taskDirectory,
  writeRecord,
} from './executors/bounded-exec/verdict.js';
import { createNodesV2 } from './index.js';
import {
  descendantsDiagnostic,
  fixtureSocketDirectory,
  publishFixtureSocketDirectory,
  retireFixtureSocketDirectory,
} from './testing.js';

useFixtureCargoHome();

const GROUP_TARGET = 'cargo-test-group-serial-probe';
const GROUP_ID = `partition-fixture:${GROUP_TARGET}`;
const GROUP_HASH = 'serial-probe-hash';

const ordinary = Array.from({ length: 13 }, (_, index) => `ordinary_${index}`);
// The class the plugin's nextest.toml singles out with an override: the
// compile-fail tests carry a raised slow-timeout, so they are lifted out of the
// ordinary hash.
const exceptional = [
  ...Array.from({ length: 6 }, (_, index) => `compile_fail_partition_${index}`),
  ...Array.from({ length: 6 }, (_, index) => `partition_${index}_fail_to_compile`),
];
const serial = Array.from({ length: 4 }, (_, index) => `serial_probe_${index}`);
const expected = [...ordinary, ...exceptional, ...serial];

/** The one Cargo fixture and test build every case reads, built once in beforeAll. */
interface SharedFixture {
  readonly root: string;
  readonly targets: Readonly<Record<string, TargetConfiguration>>;
}

/** A child this test started and has not yet seen close, with the command line that names it. */
interface OpenChild {
  readonly command: string;
  readonly closed: Promise<unknown>;
  readonly graceful: boolean;
}

/** Set first in beforeAll, so afterAll can keep or remove a root even when beforeAll fails part way. */
let fixtureRoot: string | undefined;
/** Set last in beforeAll: the fixture every case reads. */
let shared: SharedFixture | undefined;
const openChildren = new Map<ChildProcess, OpenChild>();
/** Set once teardown begins: a body still running then may not start a child, nor keep the result of one. */
let ending = false;
/** The current setup or case body, so teardown waits for it before another case or removal. */
let bodyDone: Promise<void> = Promise.resolve();
/** A failed barrier keeps the fixture, even if a later cleanup attempt succeeds. */
let barrierFailure: string | undefined;
/** The Nx installation the fixture links; it must leave it as it was made. */
const installation = testFileInstallation();

beforeAll(
  caseBody(async () => {
    const root = await nxFixtureRoot('smoo-nextest-partitions-');
    fixtureRoot = root;
    await writeFile(join(root, 'package.json'), '{"name":"partition-fixture","private":true}\n');
    await writeFile(join(root, '.gitignore'), 'node_modules/\ntarget/\n.nx/\n*.log\n');
    await writeFile(
      join(root, 'Cargo.toml'),
      '[workspace]\nresolver = "2"\nmembers = ["feature-enabler", "shared"]\n\n[features]\nalternate = []\n\n[dependencies]\nshared = { path = "shared" }\n\n[package]\nname = "partition-probe"\nversion = "0.1.0"\nedition = "2021"\n\n[package.metadata.smoothbricks.test]\nshards = 3\n',
    );
    await mkdir(join(root, '.cargo'));
    await writeFile(
      join(root, '.cargo/config.toml'),
      '[unstable]\nfeature-unification = true\n[resolver]\nfeature-unification = "workspace"\n',
    );
    for (const name of ['feature-enabler', 'shared']) await mkdir(join(root, name, 'src'), { recursive: true });
    await writeFile(
      join(root, 'shared/Cargo.toml'),
      '[package]\nname="shared"\nversion="0.1.0"\nedition="2021"\n[features]\nextra=[]\n',
    );
    await writeFile(join(root, 'shared/src/lib.rs'), '#[cfg(feature="extra")] pub const ENABLED: bool = true;\n');
    await writeFile(
      join(root, 'feature-enabler/Cargo.toml'),
      '[package]\nname="feature-enabler"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nshared={path="../shared",features=["extra"]}\n',
    );
    await writeFile(join(root, 'feature-enabler/src/lib.rs'), '');
    await mkdir(join(root, '.config'));
    await writeFile(
      join(root, '.config/nextest.toml'),
      [
        '[test-groups.serial-probe]',
        'max-threads = 1',
        '',
        '[[profile.default.overrides]]',
        "filter = 'test(/serial_probe_/)'",
        `platform = '${SERIAL_TEST_GROUP_PLATFORM}'`,
        "test-group = 'serial-probe'",
        '',
      ].join('\n'),
    );
    await mkdir(join(root, 'src'));
    await writeFile(
      join(root, 'src/lib.rs'),
      `#[cfg(test)] mod tests {
    use std::io::Write;
    fn record(name: &str) {
        let mut file = std::fs::OpenOptions::new().create(true).append(true)
            .open(std::env::var("PARTITION_EXECUTIONS").unwrap()).unwrap();
        file.write_all(format!("{name}\\n").as_bytes()).unwrap();
    }
    fn fail_when_forced() {
        if std::env::var_os("PARTITION_FAIL_SERIAL").is_some() {
            panic!("forced failure");
        }
    }
${expected.map((name) => `    #[test] fn ${name}() { record("${name}");${serial.includes(name) ? ' fail_when_forced();' : ''} }`).join('\n')}
    #[test] #[ignore] fn workspace_feature_and_ignored_inventory() { assert!(shared::ENABLED); }
}
`,
    );
    await writeFile(
      join(root, 'src/main.rs'),
      'fn main() { println!("{}", if cfg!(feature="alternate") {"second"} else {"first"}); }\n',
    );
    await mkdir(join(root, 'tests'));
    await writeFile(
      join(root, 'tests/fixture_bin.rs'),
      'use std::process::Command;\n#[test] fn fixture_binary_is_available() { let output = Command::new(std::env::var_os("CARGO_BIN_EXE_partition-probe").unwrap()).output().unwrap(); assert!(output.status.success()); assert_eq!(output.stdout, if cfg!(feature="alternate") { b"second\\n".as_slice() } else { b"first\\n".as_slice() }); }\n',
    );
    await symlink(await installation.nodeModules(), join(root, 'node_modules'), 'dir');
    await run(root, ['git', 'init', '--quiet']);
    await run(root, ['cargo', 'generate-lockfile', '--offline']);
    const [, infer] = createNodesV2;
    const result = await infer(['package.json'], undefined, { workspaceRoot: root, nxJsonConfiguration: {} });
    const targets = result[0]?.[1].projects?.['.']?.targets;
    if (targets === undefined) throw new Error('Cargo fixture inferred no targets');
    expect(targets['cargo-test-build']).toBeUndefined();
    const list = async (selection: string[]) => {
      const result = await runTracked(root, [
        'cargo',
        '--frozen',
        'nextest',
        'list',
        ...selection,
        '-E',
        'package(partition-probe)',
        '--run-ignored',
        'only',
        '-T',
        'oneline',
        '--user-config-file',
        'none',
        '--color',
        'never',
      ]);
      expect(result.code, result.output).toBe(0);
      return result.stdout.split('\n').filter((line) => line.startsWith('partition-probe '));
    };
    const before = await list(['--workspace']);
    expect(before, 'nextest stdout must contain the ignored test in the workspace inventory').toEqual([
      'partition-probe tests::workspace_feature_and_ignored_inventory',
    ]);
    expect(await list(['-p', 'partition-probe'])).toEqual(before);
    await mkdir(await publishFixtureSocketDirectory(root), { recursive: true });
    shared = { root, targets };
  }),
);

/**
 * One case's body, run as its own promise. `ending` is cleared for this case, and afterEach waits for the
 * promise before the next case may start, so a body the deadline abandons cannot leak children into it.
 */
function caseBody(body: () => Promise<void>): () => Promise<void> {
  return async () => {
    ending = false;
    bodyDone = body();
    await bodyDone;
  };
}

/**
 * The safety barrier. It runs after every case, and once more before the fixture is removed.
 *
 * A case's deadline abandons its body mid-`await`, so nothing it started may outlive the case. In order:
 * `ending` makes every later `runTracked` refuse to spawn, and makes a captured-only child still running print its
 * capture instead of returning it. The barrier then ends the children it started, and only those: a plain child's
 * process group is killed, and a graceful driver gets SIGTERM so its bounded executor relays the stop to what it runs.
 * Only once every child's exit has settled and the body has settled does the barrier report. A child whose exit was
 * not confirmed is returned by name, not hidden. What of this process's own tree still runs is printed to stderr as a
 * diagnostic, never signalled and never a failure, since ownership is the supervisor's process tree.
 */
async function settle(): Promise<string[]> {
  ending = true;
  const open = [...openChildren];
  openChildren.clear();
  for (const [child, entry] of open) {
    if (entry.graceful) child.kill('SIGTERM');
    else killProcessGroup(child);
  }
  // Each close either resolved, so its child ended, or rejected, so its exit was never confirmed and it is named.
  const exits = await Promise.all(
    open.map(async ([, entry]) => {
      try {
        await entry.closed;
        return null;
      } catch (error) {
        return `${entry.command}: ${String(error)}`;
      }
    }),
  );
  await Promise.allSettled([bodyDone]);
  const left = fixtureRoot === undefined ? null : await descendantsDiagnostic(`the fixture ${fixtureRoot}`);
  if (left !== null) process.stderr.write(`${left}\n`);
  const problems: string[] = [];
  const unconfirmed = exits.filter((line): line is string => line !== null);
  if (unconfirmed.length > 0) {
    problems.push(`child processes whose exit was not confirmed:\n${unconfirmed.join('\n')}`);
  }
  if (problems.length > 0) barrierFailure = problems.join('\n');
  return problems;
}

afterEach(async () => {
  const problems = await settle();
  if (problems.length > 0) throw new Error(`test ended with ${problems.join('\n')}`);
});

/**
 * Removes the one fixture root, after the same barrier every case runs. If the barrier fails, the root is kept
 * and the error names it, so a child whose exit was not confirmed still has its directory. What the census
 * finds never keeps the root: it is printed as a diagnostic only.
 */
afterAll(async () => {
  if (fixtureRoot === undefined) return;
  const root = fixtureRoot;
  try {
    await settle();
  } catch (error) {
    throw new Error(`kept fixture ${root}: the barrier failed: ${String(error)}`);
  }
  if (barrierFailure !== undefined) throw new Error(`kept fixture ${root}:\n${barrierFailure}`);
  fixtureRoot = undefined;
  shared = undefined;
  await retireFixtureSocketDirectory(root);
  await rm(root, { recursive: true, force: true });
});

// After the fixture's own teardown, so every process that could read the installation has ended.
afterAll(() => installation.retire());

function fixture(): SharedFixture {
  if (shared === undefined) {
    throw new Error('the shared Cargo fixture was not built: its beforeAll did not complete');
  }
  return shared;
}

function groupContext(root: string): NxTaskContext {
  return {
    root,
    projectName: 'partition-fixture',
    targetName: GROUP_TARGET,
    taskGraph: { tasks: { [GROUP_ID]: { hash: GROUP_HASH } } },
  };
}

/**
 * The group's verdict directory in the fixture's own workspace data, the same place the overlay sends the real
 * executor. readRecord proves the two agree: a record written anywhere else leaves the verdict null.
 */
function groupDirectory(root: string): string {
  return taskDirectory(join(root, '.nx/workspace-data'), GROUP_ID);
}

/**
 * The group's options as the plan infers them, run from the fixture root: the inferred cwd is the cargo root, which
 * is the fixture root.
 */
function groupOptions(targets: Readonly<Record<string, TargetConfiguration>>, root: string): NativeBoundedExecOptions {
  const options = nativeOptions(targets, GROUP_TARGET);
  expect(options.parallelism).toBe(1);
  return {
    ...options,
    cwd: root,
  };
}

/**
 * Real nextest evaluation smoke: ask `cargo nextest list` to evaluate each inferred piece's rendered
 * `-E` filterset and `--partition` flag against the compiled fixture in parallel, and assert the lists
 * are pairwise disjoint and cover the fixture's runnable tests exactly once in the expected class.
 * Exact cover across all input shapes is proven by `specs/cowshed/laws/nextest_pieces.bend` and bound
 * in `nextest-pieces-laws.test.ts`.
 */
async function listsEveryTestOnce(): Promise<void> {
  const { root, targets } = fixture();
  // macOS gives the serial probes their own runner; off macOS the ordinary pieces own them.
  // Deterministic per-platform inference is covered separately in index.test.ts.
  const hasGroup = targets[GROUP_TARGET] !== undefined;
  const listTarget = async (targetName: string): Promise<string[]> => {
    const { cargoArgs, selectionArgs = [] } = nativeOptions(targets, targetName).nativeTest;
    const result = await runTracked(root, [
      'cargo',
      '--frozen',
      'nextest',
      'list',
      ...cargoArgs,
      ...selectionArgs,
      '-T',
      'oneline',
      '--user-config-file',
      'none',
      '--color',
      'never',
    ]);
    expect(result.code, `${targetName}\n${result.output}`).toBe(0);
    return result.stdout
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.startsWith('partition-probe'));
  };

  const ordinaryNames = [1, 2, 3].map((index) => `cargo-test-partition-probe-shard${index}`);
  const exceptionalNames = [1, 2, 3].map((index) => `cargo-test-partition-probe-exceptions-shard${index}`);
  const [ordinaryLists, exceptionalLists, groupList] = await Promise.all([
    Promise.all(ordinaryNames.map(listTarget)),
    Promise.all(exceptionalNames.map(listTarget)),
    hasGroup ? listTarget(GROUP_TARGET) : Promise.resolve<string[]>([]),
  ]);

  const toLibTest = (name: string): string => `partition-probe tests::${name}`;
  const expectedOrdinary = [
    ...(hasGroup ? ordinary : [...ordinary, ...serial]).map(toLibTest),
    'partition-probe::fixture_bin fixture_binary_is_available',
  ].sort();
  const expectedExceptional = exceptional.map(toLibTest).sort();
  const expectedSerial = hasGroup ? serial.map(toLibTest).sort() : [];

  const observedOrdinary = ordinaryLists.flat();
  const observedExceptional = exceptionalLists.flat();
  expect([...observedOrdinary].sort()).toEqual(expectedOrdinary);
  expect([...observedExceptional].sort()).toEqual(expectedExceptional);
  if (hasGroup) {
    expect([...groupList].sort()).toEqual(expectedSerial);
    expect(groupOptions(targets, root).nativeTest.selectionArgs).not.toContain('--partition');
  }
  const allListed = [...observedOrdinary, ...observedExceptional, ...groupList];
  expect(new Set(allListed).size).toBe(allListed.length);
}

/**
 * The real default executor, run for real on this fixture's group in a driver process. Its verdict is typed
 * passed and returned to its caller.
 */
async function groupSucceeds(source: Pick<SharedFixture, 'root' | 'targets'> = fixture()): Promise<BoundedExecRecord> {
  const { root, targets } = source;
  expect(groupOptions(targets, root).nativeTest.runArgs).toContain('--no-fail-fast');
  const directory = groupDirectory(root);
  // The probes read PARTITION_EXECUTIONS and PARTITION_FAIL_SERIAL from the environment. Each run sets them on the
  // driver's spawn; the runner inherits them, since bounded-exec merges process.env into every runner. The command
  // stays the byte-identical one the plan generates.
  const successLog = join(root, 'group-success.log');
  await rm(successLog, { force: true });
  const success = await runGroupExecutor(root, groupOptions(targets, root), groupContext(root), {
    PARTITION_EXECUTIONS: successLog,
  });
  expect(success.code, success.output).toBe(0);
  const passed = await readRecord(directory);
  if (passed === null) throw new Error('a group run that passed wrote no verdict');
  expect(passed).toMatchObject({ task: GROUP_ID, hash: GROUP_HASH, verdict: { outcome: 'passed' } });
  // Budgeted runs reserve only the group's one runnable test; unbudgeted hosts have no receipt.
  if (passed.cpu !== undefined) {
    expect(passed.cpu).toMatchObject({ tokens: 1, want: 1 });
  }
  expect((await readFile(successLog, 'utf8')).trim().split('\n').sort()).toEqual([...serial].sort());
  await expect(readdir(join(root, '.cache/nextest'))).rejects.toMatchObject({ code: 'ENOENT' });
  expect(await readdir(join(root, 'target'))).not.toContain('nextest-extracted');
  return passed;
}

/**
 * Continuation past failure and stale-verdict replacement: seeds a stale passing verdict and JUnit report,
 * then runs the serial group with PARTITION_FAIL_SERIAL set. The group must replace the stale verdict and
 * report, continue past each failing probe via `--no-fail-fast`, and report all four. Empty-selection
 * command/verdict decisions (`--no-tests=fail`, exit 4 -> `failed`) are covered purely in `nextest-timings.test.ts`.
 */
async function continuesPastFailuresAndReplacesVerdict(): Promise<void> {
  const { root, targets } = fixture();
  const directory = groupDirectory(root);
  await mkdir(directory, { recursive: true });
  await writeRecord(directory, { task: GROUP_ID, hash: GROUP_HASH, verdict: { outcome: 'passed' }, peakBytes: null });
  await writeFile(join(directory, JUNIT_FILE), 'stale report\n');
  const failureLog = join(root, 'group-failure.log');
  await rm(failureLog, { force: true });
  const failed = await runGroupExecutor(root, groupOptions(targets, root), groupContext(root), {
    PARTITION_EXECUTIONS: failureLog,
    PARTITION_FAIL_SERIAL: '1',
  });
  expect(failed.code, failed.output).toBe(2);
  const failedRecord = await readRecord(directory);
  if (failedRecord === null) throw new Error('a group run that failed wrote no verdict');
  if (failedRecord.verdict.outcome !== 'failed') {
    throw new Error(`a failing group wrote verdict ${failedRecord.verdict.outcome}, not failed`);
  }
  expect(failedRecord).toMatchObject({ task: GROUP_ID, hash: GROUP_HASH, verdict: { outcome: 'failed' } });
  expect(failedRecord.verdict.tests).toHaveLength(serial.length);
  for (const name of serial) {
    expect(
      failedRecord.verdict.tests.some((test) => test.includes(name)),
      name,
    ).toBe(true);
  }
  await expect(readFile(join(directory, JUNIT_FILE), 'utf8')).resolves.not.toBe('stale report\n');
  expect((await readFile(failureLog, 'utf8')).trim().split('\n').sort()).toEqual([...serial].sort());
}

it('lists every built test once across the rendered piece selectors and partitions', caseBody(listsEveryTestOnce));

//#region smoo/cowshed!n/native-test-verdicts
it(
  'keeps metadata private to concurrent invocations of the same suite',
  caseBody(async () => {
    const { root, targets } = fixture();
    const options = nativeOptions(targets, 'cargo-test-partition-probe-shard1');
    const runs = await Promise.all(
      [1, 2].map(() =>
        runGroupExecutor(
          root,
          {
            ...options,
            command:
              'test -s "$BOUNDED_EXEC_FIXTURE_DIR/binaries-metadata.json" && test -s "$BOUNDED_EXEC_FIXTURE_DIR/cargo-metadata.json" && printf "fixture path: %s\n" "$BOUNDED_EXEC_FIXTURE_DIR"',
          },
          { root },
        ),
      ),
    );
    const paths = new Set<string>();
    for (const result of runs) {
      expect(result.code, result.output).toBe(0);
      const path = /^fixture path: (.+)$/m.exec(result.output)?.[1];
      if (path === undefined) throw new Error('the custom native command did not receive its fixture directory');
      paths.add(path);
      await expect(readdir(path)).rejects.toMatchObject({ code: 'ENOENT' });
    }
    expect(paths.size).toBe(2);
  }),
);
it(
  'a prepared fixture still executes its own binary after a concurrent Cargo build selects different bytes',
  caseBody(async () => {
    const { root, targets } = fixture();
    const options = nativeOptions(targets, 'cargo-test-partition-probe-shard1');
    const binary = (file: 'binaries-metadata.json' | 'alternate.json') =>
      `node -e 'const m=require(process.env.BOUNDED_EXEC_FIXTURE_DIR+"/${file}")["rust-build-meta"]; process.stdout.write(require("node:path").join(m["target-directory"],Object.values(m["non-test-binaries"]).flat().find(x=>x.name==="partition-probe").path))'`;
    // Prepare the second selection while the first consumer is still alive. Cargo itself reports both executable paths.
    const command =
      `set -e; first="$(${binary('binaries-metadata.json')})"; test "$("$first")" = first; ` +
      `cargo --frozen nextest list -p partition-probe --features alternate --list-type binaries-only --message-format json --user-config-file none > "$BOUNDED_EXEC_FIXTURE_DIR/alternate.json"; ` +
      `alternate="$(${binary('alternate.json')})"; test "$("$alternate")" = second; printf 'owned binary: %s\\n' "$first"; "$first"`;
    const result = await runGroupExecutor(root, { ...options, command }, { root });
    expect(result.code, result.output).toBe(0);
    expect(result.stdout.split('\n')).toContain('first');
    const path = /^owned binary: (.+)$/m.exec(result.stdout)?.[1];
    if (path === undefined) throw new Error('the consumer printed no prepared binary');
    await expect(readFile(path)).rejects.toMatchObject({ code: 'ENOENT' });
  }),
);
it(
  'starts cargo metadata once across sibling native test tasks on unchanged inputs',
  caseBody(async () => {
    const { root, targets } = fixture();
    const countedRoot = await mkdtemp(join(root, 'counted-metadata-'));
    const cargo = await countedCargo(countedRoot);
    try {
      const overlay = { PATH: `${cargo.bin}${delimiter}${process.env.PATH ?? ''}` };
      const checkMetadata = 'test -s "$BOUNDED_EXEC_FIXTURE_DIR/cargo-metadata.json"';
      const shard1 = { ...nativeOptions(targets, 'cargo-test-partition-probe-shard1'), command: checkMetadata };
      const shard2 = { ...nativeOptions(targets, 'cargo-test-partition-probe-shard2'), command: checkMetadata };

      // One real cold flight populates bounded-exec's shared fixture store; sibling tasks reuse it
      // (key purity and input invalidation are proven by cache_key.bend `key_is_pure` and
      // `native-fixture-metadata.test.ts`).
      const first = await runGroupExecutor(root, shard1, { root }, overlay);
      expect(first.code, first.output).toBe(0);
      expect((await cargo.runs()).spawned).toBe(1);

      const sibling = await runGroupExecutor(root, shard2, { root }, overlay);
      expect(sibling.code, sibling.output).toBe(0);
      expect((await cargo.runs()).spawned).toBe(1);
    } finally {
      await cargo.release();
      await rm(countedRoot, { recursive: true, force: true });
    }
  }),
);
//#endregion smoo/cowshed!n/native-test-verdicts

// Serial groups are a macOS capability, not a simulated Darwin plan to execute on another host.
// Every host still checks all fixture tests above through its real inferred partition.
if (process.platform === 'darwin') {
  it(
    'runs the serial group through the real default executor and keeps its passing verdict',
    caseBody(async () => {
      await groupSucceeds();
    }),
  );
  it(
    'runs the serial group past failing probes, replacing the previous verdict and reporting all four',
    caseBody(continuesPastFailuresAndReplacesVerdict),
  );
}

function nativeOptions(targets: Readonly<Record<string, TargetConfiguration>>, name: string): NativeBoundedExecOptions {
  const options = targets[name]?.options;
  const recipe: unknown = options?.nativeTest;
  if (typeof recipe !== 'object' || recipe === null || !('cargoArgs' in recipe) || !Array.isArray(recipe.cargoArgs)) {
    throw new Error(`${name} inferred no native test recipe`);
  }
  return {
    ...options,
    nativeTest: {
      cargoArgs: recipe.cargoArgs.filter((word): word is string => typeof word === 'string'),
      ...('selectionArgs' in recipe && Array.isArray(recipe.selectionArgs)
        ? { selectionArgs: recipe.selectionArgs.filter((word): word is string => typeof word === 'string') }
        : {}),
      ...('runArgs' in recipe && Array.isArray(recipe.runArgs)
        ? { runArgs: recipe.runArgs.filter((word): word is string => typeof word === 'string') }
        : {}),
    },
  };
}

function killProcessGroup(child: ChildProcess): void {
  if (child.pid === undefined) return;
  try {
    process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
  }
}

/** A child that ran to its close: its exit code and what it printed to stdout and stderr together. */
interface Tracked {
  readonly code: number | null;
  readonly output: string;
  readonly stdout: string;
}

async function runTracked(
  root: string,
  args: string[],
  options: { overlay?: Record<string, string>; env?: NodeJS.ProcessEnv; graceful?: boolean; tee?: boolean } = {},
): Promise<Tracked> {
  if (ending) throw new Error(`teardown has begun; not starting ${args.join(' ')}`);
  const [program, ...rest] = args;
  if (program === undefined) throw new Error('run needs a program');
  const child = spawn(program, rest, {
    cwd: root,
    env: { ...(options.env ?? process.env), ...options.overlay },
    detached: process.platform !== 'win32',
  });
  child.stdin?.end();
  const output: Buffer[] = [];
  const stdout: Buffer[] = [];
  const tee = options.tee ?? false;
  child.stdout?.on('data', (chunk: Buffer) => {
    output.push(chunk);
    stdout.push(chunk);
    if (tee) process.stdout.write(chunk);
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    output.push(chunk);
    if (tee) process.stderr.write(chunk);
  });
  const closed = new Promise<number | null>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', resolve);
  });
  openChildren.set(child, { command: args.join(' '), closed, graceful: options.graceful ?? false });
  let code: number | null;
  try {
    code = await closed;
  } finally {
    openChildren.delete(child);
    // Teardown abandoned this case, so no caller will print the capture. A captured-only child prints it here; a teed
    // child already printed it live.
    if (ending && !tee) process.stdout.write(Buffer.concat(output).toString());
  }
  if (ending) {
    throw new Error(
      `teardown began while ${args.join(' ')} ran; its output is printed above and its result is discarded`,
    );
  }
  return { code, stdout: Buffer.concat(stdout).toString(), output: Buffer.concat(output).toString() };
}

async function run(root: string, args: string[], overlay: Record<string, string> = {}): Promise<void> {
  const { code, output } = await runTracked(root, args, { overlay });
  expect(code, `${args.join(' ')}\n${output}`).toBe(0);
}

/**
 * The built production executor, in a driver process the barrier stops with SIGTERM so it relays the stop.
 * The test target already depends on build: do not compile the TypeScript project again in every driver.
 */
const EXECUTOR_MODULE = fileURLToPath(new URL('../dist/executors/bounded-exec/executor.js', import.meta.url));

/**
 * Exit 0 when the executor reports success, 2 when its run fails. A crash in the driver
 * exits 1, with the stack in `output`. `process.exitCode`, not `process.exit`, so stdout and stderr flush.
 * Its stdout and stderr are teed live as they arrive, and the same text is returned in `output` for assertions.
 * `environment` carries a run's probe variables onto the driver's spawn. The workspace data key comes last, so
 * no caller's entry can override the fixture's own workspace data.
 */
async function runGroupExecutor(
  root: string,
  options: BoundedExecOptions,
  context: NxTaskContext,
  environment: Record<string, string> = {},
): Promise<Tracked> {
  const script = `import boundedExecExecutor from ${JSON.stringify(EXECUTOR_MODULE)};
console.log('driver ready: calling the default executor');
const result = await boundedExecExecutor(${JSON.stringify(options)}, ${JSON.stringify(context)});
console.log('driver done: default executor success=' + result.success);
process.exitCode = result.success ? 0 : 2;`;
  const result = await runTracked(root, ['node', '--input-type=module', '-e', script], {
    graceful: true,
    tee: true,
    env: { ...fixtureNxEnv(root, fixtureSocketDirectory(root)), TMPDIR: fixtureSocketDirectory(root) },
    overlay: { ...environment, NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx/workspace-data') },
  });
  return result;
}
