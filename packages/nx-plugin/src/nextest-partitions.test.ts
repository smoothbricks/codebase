import { afterAll, afterEach, beforeAll, expect, it } from 'bun:test';
import { type ChildProcess, spawn } from 'node:child_process';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { useFixtureCargoHome } from './__tests__/fixture-cargo-home.js';
import type { NxTaskContext } from './executors/bounded-exec/executor.js';
import { hostLeaseProcesses } from './executors/bounded-exec/ram-temp.js';
import {
  type BoundedExecRecord,
  JUNIT_FILE,
  readRecord,
  taskDirectory,
  writeRecord,
} from './executors/bounded-exec/verdict.js';
import { NEXTEST_GROUP_PER_TEST_MS, selectedRunnableTestCount } from './executors/nextest-group/executor.js';
import type { NextestGroupOptions } from './executors/nextest-group/schema.js';
import { createNodesV2ForPlatform } from './index.js';
import { NEXTEST_EXTRACTION_DIRECTORY, nextestArchiveKey } from './nextest-extraction.js';

useFixtureCargoHome();

const GROUP_TARGET = 'cargo-test-group-serial-probe';
const GROUP_ID = `partition-fixture:${GROUP_TARGET}`;
const GROUP_HASH = 'serial-probe-hash';
// The filter the plugin's count stage carries; the empty-selection refusal replaces exactly this text.
const SERIAL_FILTER_ARG = "-E '(test(/serial_probe_/))'";

const ordinary = Array.from({ length: 13 }, (_, index) => `ordinary_${index}`);
// The classes the plugin's nextest.toml singles out with an override: a
// compile-fail test and the real-APFS lifecycle test each carry a raised
// slow-timeout, so each is lifted out of the ordinary hash.
const exceptional = [
  ...Array.from({ length: 6 }, (_, index) => `compile_fail_partition_${index}`),
  ...Array.from({ length: 6 }, (_, index) => `partition_${index}_fail_to_compile`),
  'real_apfs_partition_substrate_lifecycle',
];
const serial = Array.from({ length: 4 }, (_, index) => `serial_probe_${index}`);
const expected = [...ordinary, ...exceptional, ...serial];

/** The one Cargo fixture and archive every case reads, built once in beforeAll. */
interface SharedFixture {
  readonly root: string;
  readonly targets: Readonly<Record<string, TargetConfiguration>>;
  readonly listCommand: string;
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

beforeAll(
  caseBody(async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-nextest-partitions-')));
    fixtureRoot = root;
    await writeFile(join(root, 'package.json'), '{"name":"partition-fixture","private":true}\n');
    await writeFile(join(root, '.gitignore'), 'node_modules/\ntarget/\n.nx/\n*.log\n');
    await writeFile(
      join(root, 'Cargo.toml'),
      '[workspace]\nresolver = "2"\n\n[package]\nname = "partition-probe"\nversion = "0.1.0"\nedition = "2021"\n\n[package.metadata.smoothbricks.test]\nshards = 3\n',
    );
    await mkdir(join(root, '.config'));
    await writeFile(
      join(root, '.config/nextest.toml'),
      [
        '[test-groups.serial-probe]',
        'max-threads = 1',
        '',
        '[[profile.default.overrides]]',
        "filter = 'test(/serial_probe_/)'",
        `platform = 'cfg(target_os="macos")'`,
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
}
`,
    );
    await symlink(join(import.meta.dir, '../../../node_modules'), join(root, 'node_modules'), 'dir');
    await run(root, ['git', 'init', '--quiet']);
    await run(root, ['cargo', 'generate-lockfile', '--offline']);
    const [, infer] = createNodesV2ForPlatform('darwin', 'arm64');
    const result = await infer(['package.json'], undefined, { workspaceRoot: root, nxJsonConfiguration: {} });
    const targets = result[0]?.[1].projects?.['.']?.targets;
    if (targets === undefined) throw new Error('Cargo fixture inferred no targets');
    const listCommand: unknown = targets[GROUP_TARGET]?.options?.listCommand;
    if (typeof listCommand !== 'string') throw new Error(`${GROUP_TARGET} inferred no listCommand`);
    // The archive is the one real prerequisite: every runner executes its binaries.
    await run(root, ['sh', '-c', command(targets, 'cargo-test-archive')]);
    shared = { root, targets, listCommand };
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
 * capture instead of returning it; each plain child's process
 * group is killed; then the lease owner ends every process still working in the fixture, a driver first,
 * with SIGTERM so its bounded executor relays the stop and releases its lease. Only once every child's exit has
 * settled and the body has settled does the barrier report. A child whose exit was not confirmed, or any
 * process left working, is returned by name, not hidden.
 */
async function settle(): Promise<string[]> {
  ending = true;
  const open = [...openChildren];
  openChildren.clear();
  for (const [child, entry] of open) {
    if (!entry.graceful) killProcessGroup(child);
  }
  let leftovers: string[];
  try {
    leftovers = fixtureRoot === undefined ? [] : await hostLeaseProcesses.stopWorkingIn(fixtureRoot);
  } catch (error) {
    barrierFailure = `the lease owner could not end its processes: ${String(error)}`;
    throw error;
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
  const problems: string[] = [];
  const unconfirmed = exits.filter((line): line is string => line !== null);
  if (unconfirmed.length > 0) {
    problems.push(`child processes whose exit was not confirmed:\n${unconfirmed.join('\n')}`);
  }
  if (leftovers.length > 0) problems.push(`processes left working in the fixture:\n${leftovers.join('\n')}`);
  if (problems.length > 0) barrierFailure = problems.join('\n');
  return problems;
}

afterEach(async () => {
  const problems = await settle();
  if (problems.length > 0) throw new Error(`test ended with ${problems.join('\n')}`);
});

/**
 * Removes the one fixture root, after the same barrier every case runs. If the barrier cannot account for
 * every process, or the lease owner errors, the root is kept and the error names it, so the processes it
 * names still have their directory.
 */
afterAll(async () => {
  if (fixtureRoot === undefined) return;
  const root = fixtureRoot;
  try {
    await settle();
  } catch (error) {
    throw new Error(`kept fixture ${root}: the lease owner could not end its processes: ${String(error)}`);
  }
  if (barrierFailure !== undefined) throw new Error(`kept fixture ${root}:\n${barrierFailure}`);
  fixtureRoot = undefined;
  shared = undefined;
  await rm(root, { recursive: true, force: true });
});

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

/** The empty-selection listing: the plugin's own count stage with its serial filter replaced by `none()`. */
function emptySelectionListing(listCommand: string): string {
  const emptied = listCommand.replace(SERIAL_FILTER_ARG, "-E 'none()'");
  if (emptied === listCommand) throw new Error(`the serial listing carries no ${SERIAL_FILTER_ARG} to empty`);
  return emptied;
}

/**
 * Every archived test runs exactly once, in the runner that owns its class, and the serial group lists exactly
 * its probes. The runners are independent, so they run side by side exactly as Nx fans them out.
 */
async function executesEveryTestOnce(): Promise<void> {
  const { root, targets, listCommand } = fixture();
  // One log per runner kind, so the proof is exact selection and not only coverage: the ordinary shards ran
  // every ordinary test once and nothing else, the exceptional shards did the same for theirs, and the serial
  // group ran its class once in one unpartitioned runner.
  const serialLog = join(root, 'serial.log');
  const partitions = [
    { kind: 'shard', log: join(root, 'ordinary.log'), tests: ordinary },
    { kind: 'exceptions-shard', log: join(root, 'exceptional.log'), tests: exceptional },
  ];
  await Promise.all([
    ...partitions.flatMap(({ kind, log }) =>
      [1, 2, 3].map((index) =>
        run(root, ['sh', '-c', command(targets, `cargo-test-partition-probe-${kind}${index}`)], {
          PARTITION_EXECUTIONS: log,
        }),
      ),
    ),
    run(root, ['sh', '-c', command(targets, GROUP_TARGET)], { PARTITION_EXECUTIONS: serialLog }),
  ]);
  for (const { log, tests } of [...partitions, { log: serialLog, tests: serial }]) {
    const observed = (await readFile(log, 'utf8')).trim().split('\n').sort();
    expect(observed).toEqual([...tests].sort());
  }
  // The six runners shared one extraction, in Cargo's build tree: the archive's directory holds only the
  // archive, the declared Nx output.
  expect(await readdir(join(root, 'target', NEXTEST_EXTRACTION_DIRECTORY))).toEqual([
    await nextestArchiveKey(join(root, '.cache/nextest/archive.tar.zst')),
  ]);
  expect(await readdir(join(root, '.cache/nextest'))).toEqual(['archive.tar.zst']);
  // The serial group is one unpartitioned runner. Its count stage lists exactly the probes, and the parser its
  // executor sizes the bound with counts the same class the run executed.
  expect(command(targets, GROUP_TARGET)).not.toContain('--partition');
  const listing = join(root, 'serial-listing.json');
  await run(root, ['sh', '-c', `${listCommand} > '${listing}'`]);
  expect(selectedRunnableTestCount(await readFile(listing, 'utf8'))).toEqual({ ok: true, count: serial.length });
}

/**
 * The real default group executor, run for real on this fixture in a driver process. Its verdict is typed
 * passed and returned to its caller: each refusal establishes its own seed, independent of every other case.
 */
async function groupSucceeds(): Promise<BoundedExecRecord> {
  const { root, targets, listCommand } = fixture();
  const directory = groupDirectory(root);
  const realCommand = command(targets, GROUP_TARGET);
  // Main insists on --no-fail-fast: ordinary failures must not stop the remaining tests. Checked before any run.
  expect(realCommand).toContain('--no-fail-fast');
  expect(realCommand).not.toContain('max-fail');
  // The probes read PARTITION_EXECUTIONS and PARTITION_FAIL_SERIAL from the environment. Each run sets them on the
  // driver's spawn; the runner inherits them, since bounded-exec merges process.env into every runner. The command
  // stays the byte-identical one the plan generates.
  const successLog = join(root, 'group-success.log');
  await rm(successLog, { force: true });
  const success = await runGroupExecutor(root, { command: realCommand, listCommand, cwd: root }, groupContext(root), {
    PARTITION_EXECUTIONS: successLog,
  });
  expect(success.code, success.output).toBe(0);
  // The exact receipt line, not a prefix: the per-test share times the serial count.
  expect(success.output).toContain(
    `nextest-group: ${serial.length} selected tests, bounded at ${serial.length * NEXTEST_GROUP_PER_TEST_MS}ms`,
  );
  const passed = await readRecord(directory);
  if (passed === null) throw new Error('a group run that passed wrote no verdict');
  expect(passed).toMatchObject({ task: GROUP_ID, hash: GROUP_HASH, verdict: { outcome: 'passed' } });
  // Budgeted runs reserve only the group's one runnable test; unbudgeted hosts have no receipt.
  if (passed.cpu !== undefined) {
    expect(passed.cpu).toMatchObject({ tokens: 1, want: 1 });
  }
  expect((await readFile(successLog, 'utf8')).trim().split('\n').sort()).toEqual([...serial].sort());
  return passed;
}

/**
 * Refuses one listing before any probe runs. The case first establishes its own passing verdict, then seeds
 * the stale verdict and report whose removal it observes. A timeout in another case cannot remove this
 * precondition. The marker log proves the refused run started no probe.
 */
async function refuses(listCommand: string, reason: string): Promise<void> {
  const { root, targets } = fixture();
  const directory = groupDirectory(root);
  await writeRecord(directory, await groupSucceeds());
  await writeFile(join(directory, JUNIT_FILE), 'stale report\n');
  const refusalLog = join(root, 'group-refused.log');
  const refused = await runGroupExecutor(
    root,
    { command: command(targets, GROUP_TARGET), listCommand, cwd: root },
    groupContext(root),
    { PARTITION_EXECUTIONS: refusalLog },
  );
  // The driver exits 2 on a refusal; the refusal reason is on its stderr, captured in `output`.
  expect(refused.code, refused.output).toBe(2);
  expect(refused.output).toContain(reason);
  expect(await readRecord(directory)).toBeNull();
  await expect(readFile(join(directory, JUNIT_FILE), 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
  await expect(readFile(refusalLog, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
}

/**
 * Continuation past failure: each serial probe records, then fails through `fail_when_forced` because
 * PARTITION_FAIL_SERIAL is set. The group must still run and report all four. This is the observed
 * continuation, not a mutant count, and it says nothing about serialization: the override is macOS-only.
 */
async function continuesPastFailures(): Promise<void> {
  const { root, targets, listCommand } = fixture();
  const directory = groupDirectory(root);
  const failureLog = join(root, 'group-failure.log');
  const failed = await runGroupExecutor(
    root,
    { command: command(targets, GROUP_TARGET), listCommand, cwd: root },
    groupContext(root),
    { PARTITION_EXECUTIONS: failureLog, PARTITION_FAIL_SERIAL: '1' },
  );
  expect(failed.code, failed.output).toBe(2);
  const failedRecord = await readRecord(directory);
  if (failedRecord === null) throw new Error('a group run that failed wrote no verdict');
  if (failedRecord.verdict.outcome !== 'failed') {
    throw new Error(`a failing group wrote verdict ${failedRecord.verdict.outcome}, not failed`);
  }
  expect(failedRecord.verdict.tests).toHaveLength(serial.length);
  for (const name of serial) {
    expect(
      failedRecord.verdict.tests.some((test) => test.includes(name)),
      name,
    ).toBe(true);
  }
  expect((await readFile(failureLog, 'utf8')).trim().split('\n').sort()).toEqual([...serial].sort());
}

it(
  'executes every archived test once across ordinary, exceptional and serial-group runners',
  caseBody(executesEveryTestOnce),
);
it(
  'runs the serial group through the real default executor and keeps its passing verdict',
  caseBody(async () => {
    await groupSucceeds();
  }),
);
it(
  'refuses a failed listing before any run, clearing the previous verdict and report',
  caseBody(() => refuses('exit 7', 'count stage failed')),
);
it(
  'refuses an empty selection before any run, clearing the previous verdict and report',
  caseBody(() => refuses(emptySelectionListing(fixture().listCommand), 'selects no runnable test')),
);
it(
  'refuses a malformed listing before any run, clearing the previous verdict and report',
  caseBody(() => refuses('echo not-json', 'printed no JSON listing')),
);
it('runs the serial group past failing probes and reports all four', caseBody(continuesPastFailures));

function command(targets: Readonly<Record<string, TargetConfiguration>>, name: string): string {
  const single: unknown = targets[name]?.options?.command;
  if (typeof single !== 'string') throw new Error(`${name} inferred no executable command`);
  return single;
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
}

async function runTracked(
  root: string,
  args: string[],
  options: { overlay?: Record<string, string>; graceful?: boolean; tee?: boolean } = {},
): Promise<Tracked> {
  if (ending) throw new Error(`teardown has begun; not starting ${args.join(' ')}`);
  const [program, ...rest] = args;
  if (program === undefined) throw new Error('run needs a program');
  const child = spawn(program, rest, {
    cwd: root,
    env: { ...process.env, ...options.overlay },
    detached: process.platform !== 'win32',
  });
  child.stdin?.end();
  const output: Buffer[] = [];
  const tee = options.tee ?? false;
  child.stdout?.on('data', (chunk: Buffer) => {
    output.push(chunk);
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
  return { code, output: Buffer.concat(output).toString() };
}

async function run(root: string, args: string[], overlay: Record<string, string> = {}): Promise<void> {
  const { code, output } = await runTracked(root, args, { overlay });
  expect(code, `${args.join(' ')}\n${output}`).toBe(0);
}

/** The real default group executor, run in a driver process so its runner group can be ended by the lease owner. */
const EXECUTOR_MODULE = fileURLToPath(new URL('./executors/nextest-group/executor.ts', import.meta.url));
// The driver runs outside this package's bunfig; use the same native transform preload as its test runner.
const VALIDATION_PRELOAD = fileURLToPath(import.meta.resolve('@smoothbricks/validation/bun/preload'));

/**
 * Exit 0 when the executor reports success, 2 when it refuses or its run fails. A crash in the driver
 * exits 1, with the stack in `output`. `process.exitCode`, not `process.exit`, so stdout and stderr flush.
 * Its stdout and stderr are teed live as they arrive, and the same text is returned in `output` for assertions.
 * `environment` carries a run's probe variables onto the driver's spawn. The workspace data key comes last, so
 * no caller's entry can override the fixture's own workspace data.
 */
async function runGroupExecutor(
  root: string,
  options: NextestGroupOptions,
  context: NxTaskContext,
  environment: Record<string, string> = {},
): Promise<Tracked> {
  const script = `import nextestGroupExecutor from ${JSON.stringify(EXECUTOR_MODULE)};
console.log('driver ready: calling the default group executor');
const result = await nextestGroupExecutor(${JSON.stringify(options)}, ${JSON.stringify(context)});
console.log('driver done: default group executor success=' + result.success);
process.exitCode = result.success ? 0 : 2;`;
  const result = await runTracked(root, [process.execPath, '--preload', VALIDATION_PRELOAD, '-e', script], {
    graceful: true,
    tee: true,
    overlay: { ...environment, NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx/workspace-data') },
  });
  return result;
}
