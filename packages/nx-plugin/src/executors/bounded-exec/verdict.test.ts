import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import fc from 'fast-check';

import { onProgressClock } from '../../__tests__/progress-clock.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { measuredTimings } from '../../nextest-timings.js';
import {
  type BoundedExecContext,
  createProcessTreeKiller,
  type ProcessTreeKiller,
  runBoundedExec,
} from './executor.js';
import type { BoundedExecBounds } from './schema.js';
import {
  type BoundedExecVerdict,
  describeVerdict,
  failedTasksOfRun,
  JUNIT_FILE,
  judgeOperationalFailure,
  judgeRun,
  junitCaseTimes,
  junitFailures,
  readRecord,
  taskDirectory,
  VERDICT_FILE,
  withJunitReport,
  writeRecord,
} from './verdict.js';

const roots: string[] = [];
const ownedProcessGroups = new Set<number>();
const originalStdoutWrite = process.stdout.write;
const originalStderrWrite = process.stderr.write;

afterEach(async () => {
  process.stdout.write = originalStdoutWrite;
  process.stderr.write = originalStderrWrite;
  for (const pid of ownedProcessGroups) {
    try {
      process.kill(-pid, 'SIGKILL');
    } catch (cause) {
      if (!(cause instanceof Error && 'code' in cause && cause.code === 'ESRCH')) {
        throw new Error(`cleanup owned abort-report process group pgid=${pid} failed`, { cause });
      }
    } finally {
      ownedProcessGroups.delete(pid);
    }
  }
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function workspace(): Promise<{ root: string; data: string; context: BoundedExecContext }> {
  const root = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-verdict-'));
  roots.push(root);
  const data = join(root, '.nx', 'workspace-data');
  return {
    root,
    data,
    context: { root, record: { task: { id: 'app:test', hash: '42' }, directory: taskDirectory(data, 'app:test') } },
  };
}

/**
 * Run `command` as task app:test and answer the verdict bounded-exec recorded for it. `links` name
 * files of the workspace that link to the given targets.
 */
async function verdictOf(
  command: string,
  bounds: BoundedExecBounds & { killAfterMs?: number },
  files: Record<string, string> = {},
  links: Record<string, string> = {},
  prepareKiller?: (root: string) => Promise<ProcessTreeKiller>,
): Promise<BoundedExecVerdict | undefined> {
  const { root, context } = await workspace();
  for (const [name, contents] of Object.entries(files)) {
    await writeFile(join(root, name), contents);
  }
  for (const [name, target] of Object.entries(links)) {
    await mkdir(dirname(join(root, name)), { recursive: true });
    await symlink(target, join(root, name));
  }
  process.stdout.write = () => true;
  process.stderr.write = () => true;
  const killer = (await prepareKiller?.(root)) ?? createProcessTreeKiller();
  await runBoundedExec({ command, killAfterMs: 0, ...bounds }, context, killer, null);
  const record = await readRecord(context.record?.directory ?? '');
  expect(record?.task).toBe('app:test');
  expect(record?.hash).toBe('42');
  return record?.verdict;
}

// The fixtures run in child processes and must really meet their runner's per-test bound: the verdict is about which
// real bound fired. The slow test never finishes, so nothing can beat that bound; the parent's own required ceiling is
// the leg's backstop, so it cannot end the run first however long `bun test` takes to start on a loaded host.
const SLOW_TEST = `import { test } from 'bun:test';\ntest('slow', () => new Promise(() => {}));\n`;
const FAILING_TEST = `import { expect, test } from 'bun:test';\ntest('wrong', () => { expect(1).toBe(2); });\n`;

describe('bounded-exec verdicts', () => {
  it('records a bun run whose only failure is a per-test timeout as a bound', async () => {
    expect(
      await verdictOf(
        'bun test --timeout=100 ./slow.test.ts',
        { timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        { 'slow.test.ts': SLOW_TEST },
      ),
    ).toEqual({
      outcome: 'bound',
      bound: 'test',
      tests: ['slow'],
    });
  });

  it('records a path-pinned bun run whose only failure is a per-test timeout as a bound', async () => {
    expect(
      await verdictOf(
        '.runtime/bun test --timeout=100 ./slow.test.ts',
        { timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        { 'slow.test.ts': SLOW_TEST },
        { '.runtime/bun': process.execPath },
      ),
    ).toEqual({ outcome: 'bound', bound: 'test', tests: ['slow'] });
  });

  it('records an assertion as a failure even beside a timeout', async () => {
    expect(
      await verdictOf(
        'bun test --timeout=100 ./slow.test.ts ./failing.test.ts',
        { timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        { 'slow.test.ts': SLOW_TEST, 'failing.test.ts': FAILING_TEST },
      ),
    ).toEqual({ outcome: 'failed', exitCode: 1, tests: ['wrong'] });
  });

  it('records the total bound as a bound and the progress bound as a wedge', async () => {
    // Each command holds on its stdin, which bounded-exec keeps open and never writes: it never exits, so only the
    // bound under test can end it.
    expect(await verdictOf('node -e "process.stdin.resume()"', { timeoutMs: 200 })).toMatchObject({
      outcome: 'bound',
      bound: 'total',
      limitMs: 200,
    });
    expect(await verdictOf('node -e "process.stdin.resume()"', { idleTimeoutMs: 100 })).toMatchObject({
      outcome: 'wedged',
      idleMs: 100,
      tests: [],
    });
  });

  it('records a runner that warns of a slow test but finishes none as a wedge naming that test', async () => {
    // nextest's own lines: one test passes, the other only ever says SLOW. A bound on silence would never fire.
    const runner = [
      "console.error('        PASS [   0.010s] (1/2) fake tests::quick');",
      "setInterval(() => console.error('        SLOW [>  0.050s] (───) fake tests::hangs'), 50);",
      '',
    ].join('\n');
    const { root, context } = await workspace();
    await writeFile(join(root, 'runner.js'), runner);
    // The window ends only once the runner has said SLOW, however long node takes to start.
    await onProgressClock(
      (chunk) => chunk.includes('SLOW [>'),
      400,
      () =>
        runBoundedExec(
          { command: 'node runner.js', idleTimeoutMs: 400, killAfterMs: 0 },
          context,
          createProcessTreeKiller(),
          null,
        ),
    );
    expect((await readRecord(context.record?.directory ?? ''))?.verdict).toMatchObject({
      outcome: 'wedged',
      idleMs: 400,
      tests: ['fake tests::hangs'],
    });
  });

  it('names the tests nextest aborted at the progress bound beside those it said were running, once each', () => {
    const report = { timedOut: [], cancelled: ['nt tests::hangs', 'nt tests::young'], failed: [] };
    expect(
      judgeRun({
        exitCode: 100,
        elapsedMs: 900,
        expiry: { kind: 'idle', limitMs: 400, tests: ['nt tests::hangs'] },
        report,
        noSpace: null,
        lockWaits: [],
      }),
    ).toEqual({
      outcome: 'wedged',
      idleMs: 400,
      elapsedMs: 900,
      tests: ['nt tests::hangs', 'nt tests::young'],
      lockWaits: [],
    });
  });

  it('copies the lock waits the snapshot named into the wedged verdict', () => {
    const cache = '/Users/dev/.cargo/.package-cache';
    const lockWaits = [{ path: cache, waiters: [94769], holders: [{ pid: 51234, command: 'cargo' }] }];
    expect(
      judgeRun({
        exitCode: 143,
        elapsedMs: 180_900,
        expiry: { kind: 'idle', limitMs: 180_000, tests: [] },
        report: null,
        noSpace: null,
        lockWaits,
      }),
    ).toEqual({ outcome: 'wedged', idleMs: 180_000, elapsedMs: 180_900, tests: [], lockWaits });
  });

  it('names the process outside the run that holds the file a wedged run waits on', () => {
    const lockWaits = [
      { path: '/Users/dev/.cargo/.package-cache', waiters: [94769], holders: [{ pid: 51234, command: 'cargo' }] },
    ];
    expect(describeVerdict({ outcome: 'wedged', idleMs: 180_000, elapsedMs: 200_000, tests: [], lockWaits })).toBe(
      'wedged: no test finished for 180000ms after 200000ms; the runner named no test still running; ' +
        'waiting on a lock: /Users/dev/.cargo/.package-cache held open by pid 51234 (cargo)',
    );
  });

  it('reads a wedged record written before lock waits were named as naming none, and refuses a malformed one', async () => {
    const { root } = await workspace();
    const directory = join(root, 'record');
    await mkdir(directory, { recursive: true });
    const verdict = { outcome: 'wedged', idleMs: 400, elapsedMs: 900, tests: ['t'] } as const;
    const write = (value: unknown) =>
      writeFile(join(directory, VERDICT_FILE), JSON.stringify({ task: 'app:test', hash: '42', verdict: value }));
    await write(verdict);
    expect((await readRecord(directory))?.verdict).toEqual({ ...verdict, lockWaits: [] });
    const named = [{ path: '/fixture/cache', waiters: [7], holders: [{ pid: 9, command: 'cargo' }] }];
    await write({ ...verdict, lockWaits: named });
    expect((await readRecord(directory))?.verdict).toEqual({ ...verdict, lockWaits: named });
    await write({ ...verdict, lockWaits: 'cargo' });
    await expect(readRecord(directory)).rejects.toThrow('not a bounded-exec verdict');
  });

  it('records a failed run whose output said ENOSPC as out of space, ahead of the tests and the bound that followed', () => {
    // From a downstream land gate: a 1 KB write into the task's RAM lease failed, and two tests failed on it.
    const noSpace = {
      line: 'ENOSPC: no space left on device, write',
      volume: {
        kind: 'measured',
        lease: '/private/var/folders/cowshed/502/ram3/37738-AadRCN',
        mountpoint: '/private/var/folders/cowshed/502/ram3',
        capacityBytes: 8 * 1024 ** 3,
        freeBytes: 4096,
      },
    } as const;
    const report = { timedOut: [], cancelled: [], failed: ['lsp-differential hover'] };
    const ran = { exitCode: 1, elapsedMs: 11_580, expiry: null, report, noSpace, lockWaits: [] };
    expect(judgeRun(ran)).toEqual({ outcome: 'no-space', exitCode: 1, tests: ['lsp-differential hover'], ...noSpace });
    // A full disk that then hung the run is still the disk.
    const idle = { kind: 'idle', limitMs: 400, tests: ['nt tests::writes'] } as const;
    expect(judgeRun({ ...ran, exitCode: 143, expiry: idle, report: null })).toEqual({
      outcome: 'no-space',
      exitCode: 143,
      tests: [],
      ...noSpace,
    });
    // A passing run that printed it, as a test of ENOSPC handling does, passed.
    expect(judgeRun({ ...ran, exitCode: 0, report: null })).toEqual({ outcome: 'passed' });
    expect(describeVerdict(judgeRun(ran))).toBe(
      'ENOSPC (no space left on device): the command printed "ENOSPC: no space left on device, write"; its TMPDIR ' +
        '/private/var/folders/cowshed/502/ram3/37738-AadRCN is on the RAM temp volume /private/var/folders/cowshed/502/ram3, which had 0.0 MiB free ' +
        'of 8192.0 MiB when that line arrived; the tests that then failed: lsp-differential hover',
    );
  });

  it('says the lease was lost from its volume when the ENOSPC line arrived, and reads each recorded kind back', async () => {
    // From a land gate: the RAM volume was detached under a running shard, a write through TMPDIR made the lease
    // directory again on the host's Data volume, and its statfs read 2778060.4 MiB free of the 8192.0 MiB volume.
    const line = 'ENOSPC: no space left on device, write';
    const lost = {
      kind: 'lost',
      lease: '/private/var/folders/cowshed/502/ram3/33054-jdLjKN',
      mountpoint: '/private/var/folders/cowshed/502/ram3',
      detail: 'no marked volume is mounted at /private/var/folders/cowshed/502/ram3 any more',
    } as const;
    const verdict = { outcome: 'no-space', exitCode: 1, tests: [], line, volume: lost } as const;
    expect(describeVerdict(verdict)).toBe(
      `ENOSPC (no space left on device): the command printed "${line}"; its TMPDIR ${lost.lease} was no longer on ` +
        `the RAM temp volume at ${lost.mountpoint} when that line arrived, so no free space of that volume was read: ` +
        lost.detail,
    );

    const { root } = await workspace();
    const directory = join(root, 'record');
    await mkdir(directory, { recursive: true });
    const write = (volume: unknown) =>
      writeFile(
        join(directory, VERDICT_FILE),
        JSON.stringify({ task: 'app:test', hash: '42', verdict: { ...verdict, volume } }),
      );
    await write(lost);
    expect((await readRecord(directory))?.verdict).toEqual(verdict);
    const unread = { ...lost, kind: 'unread', detail: 'EIO' } as const;
    await write(unread);
    expect((await readRecord(directory))?.verdict).toEqual({ ...verdict, volume: unread });
    // A record written before the kinds were named reads as the measurement it recorded.
    const measured = { lease: lost.lease, mountpoint: lost.mountpoint, capacityBytes: 8 * 1024 ** 3, freeBytes: 4096 };
    await write(measured);
    expect((await readRecord(directory))?.verdict).toEqual({ ...verdict, volume: { kind: 'measured', ...measured } });
    await write({ kind: 'lost', lease: lost.lease, mountpoint: lost.mountpoint });
    await expect(readRecord(directory)).rejects.toThrow('not a bounded-exec verdict');
  });

  it('records a failing command with no report as a failure, and a passing one as passed', async () => {
    // How an exit is classified, not how soon it comes: the required ceiling is the leg's backstop, so it cannot end a
    // `node` still starting on a loaded host (a land gate measured 5243 ms for `node -e ""` against 5000 ms).
    expect(await verdictOf('node -e "process.exit(3)"', { timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS })).toEqual({
      outcome: 'failed',
      exitCode: 3,
      tests: [],
    });
    expect(await verdictOf('node -e ""', { timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS })).toEqual({ outcome: 'passed' });
  });

  it("reads nextest's typed timeout from its JUnit report", () => {
    // A cargo-nextest 0.9.143 report: one test terminated at its slow-timeout, one assertion.
    const report = `<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="3" failures="2">
    <testsuite name="nt" tests="3" failures="2">
        <testcase name="t::ok" classname="nt" time="0.095"/>
        <testcase name="t::assert" classname="nt" time="0.095">
            <failure message="thread &apos;t::assert&apos; panicked at src/lib.rs:4:27" type="test failure with exit code 101">assertion failed</failure>
            <system-out>
test t::assert ... FAILED
</system-out>
        </testcase>
        <testcase name="t::slow" classname="nt" time="1.003">
            <failure type="test timeout"/>
            <system-out>
running 1 test
</system-out>
        </testcase>
    </testsuite>
</testsuites>`;
    expect(junitFailures(report)).toEqual({ timedOut: ['nt t::slow'], cancelled: [], failed: ['nt t::assert'] });
  });

  it("keeps a crate's tests of at least the floor from nextest's JUnit report, one timing entry per name", () => {
    // nextest writes the binary id as the case's classname and the test path as its name.
    const report = `<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="6" time="60.0">
<testsuite name="cowshed-cli::command_dispatch" tests="3">
<testcase name="macos::directory_walk" classname="cowshed-cli::command_dispatch" time="41.250"/>
<testcase name="macos::quick" classname="cowshed-cli::command_dispatch" time="0.300"/>
<testcase name="shared::name" classname="cowshed-cli::command_dispatch" time="2.000"><failure type="test failure"><![CDATA[<testcase name="in::output" time="99"/>]]></failure></testcase>
</testsuite>
<testsuite name="cowshed-cli" tests="1"><testcase name="shared::name" classname="cowshed-cli" time="1.500"/></testsuite>
<testsuite name="cowshed-clippy" tests="1"><testcase name="other::crate" classname="cowshed-clippy" time="9.000"/></testsuite>
<testsuite name="cowshed-core" tests="1"><testcase name="core::slow" classname="cowshed-core" time="50.000"/></testsuite>
</testsuites>
`;
    expect(measuredTimings(junitCaseTimes(report), 'cowshed-cli', 3000)).toEqual({
      'macos::directory_walk': 41250,
      'shared::name': 3500,
    });
  });

  it("reads nextest's abort of a test its command was told to stop as neither a failure nor a timeout", () => {
    // cargo-nextest 0.9.143, SIGTERM delivered to the run: the tests still running are reported as `test abort`.
    // A test that dies of any other signal (here SIGSEGV) is a crash of the test itself.
    const abort = (name: string, signal: string) =>
      `<testcase name="${name}" classname="nt" time="0.85"><failure message="process aborted with signal ${signal}" type="test abort">process aborted with signal ${signal}</failure><system-out>\nrunning 1 test\n</system-out></testcase>`;
    const report = `<testsuites name="nextest-run" tests="4" failures="3"><testsuite name="nt" tests="4" failures="3">
<testcase name="t::ok" classname="nt" time="0.1"/>
${abort('t::cut', '15 (SIGTERM)')}
${abort('t::killed', '9 (SIGKILL)')}
${abort('t::crash', '11 (SIGSEGV)')}
</testsuite></testsuites>`;
    expect(junitFailures(report)).toEqual({
      timedOut: [],
      cancelled: ['nt t::cut', 'nt t::killed'],
      failed: ['nt t::crash'],
    });
  });

  it('records a run its ceiling stopped as a bound when the only failures are the tests the stop aborted', () => {
    const total = { kind: 'total', limitMs: 120_000 } as const;
    const cut = { timedOut: [], cancelled: ['nt t::cut'], failed: [] };
    expect(
      judgeRun({ exitCode: 143, elapsedMs: 120_850, expiry: total, report: cut, noSpace: null, lockWaits: [] }),
    ).toEqual({
      outcome: 'bound',
      bound: 'total',
      limitMs: 120_000,
      elapsedMs: 120_850,
    });
    // An assertion that failed before the ceiling is still a failure, and an abort nobody asked for is one too.
    expect(
      judgeRun({
        exitCode: 143,
        elapsedMs: 120_850,
        expiry: total,
        report: { ...cut, failed: ['nt t::assert'] },
        noSpace: null,
        lockWaits: [],
      }),
    ).toEqual({ outcome: 'failed', exitCode: 143, tests: ['nt t::assert'] });
    expect(
      judgeRun({ exitCode: 143, elapsedMs: 850, expiry: null, report: cut, noSpace: null, lockWaits: [] }),
    ).toEqual({
      outcome: 'failed',
      exitCode: 143,
      tests: ['nt t::cut'],
    });
  });

  it('records a test aborted with no total bound to explain it as a failure, whatever the exit or the other tests say', () => {
    const cancelled = ['nt t::cut'];
    // Exit 0 with an abort in the report: the abort is still not nothing.
    expect(
      judgeRun({
        exitCode: 0,
        elapsedMs: 850,
        expiry: null,
        report: { timedOut: [], cancelled, failed: [] },
        noSpace: null,
        lockWaits: [],
      }),
    ).toEqual({
      outcome: 'failed',
      exitCode: 0,
      tests: cancelled,
    });
    // A per-test timeout beside an abort nobody asked for is not a per-test bound: the abort failed the run.
    expect(
      judgeRun({
        exitCode: 100,
        elapsedMs: 850,
        expiry: null,
        report: { timedOut: ['nt t::slow'], cancelled, failed: [] },
        noSpace: null,
        lockWaits: [],
      }),
    ).toEqual({ outcome: 'failed', exitCode: 100, tests: cancelled });
    // With the total bound fired, the same report is the bound's own casualties.
    expect(
      judgeRun({
        exitCode: 143,
        elapsedMs: 120_850,
        expiry: { kind: 'total', limitMs: 120_000 },
        report: { timedOut: ['nt t::slow'], cancelled, failed: [] },
        noSpace: null,
        lockWaits: [],
      }),
    ).toMatchObject({ outcome: 'bound', bound: 'total' });
  });

  it('keeps CPU operational failures ordinary even beside test timeouts or a successful command exit', () => {
    expect(
      judgeOperationalFailure(0, {
        failed: ['nt t::assert'],
        timedOut: ['nt t::slow'],
        cancelled: ['nt t::cut'],
      }),
    ).toEqual({
      outcome: 'failed',
      exitCode: 1,
      tests: ['nt t::assert', 'nt t::slow', 'nt t::cut'],
    });
    expect(judgeOperationalFailure(143, { failed: [], timedOut: ['nt t::slow'], cancelled: [] })).toEqual({
      outcome: 'failed',
      exitCode: 143,
      tests: ['nt t::slow'],
    });
    expect(judgeOperationalFailure(0, null)).toEqual({ outcome: 'failed', exitCode: 1, tests: [] });
  });

  it('records a runner that reports its own abort at the total ceiling as a bound, end to end', async () => {
    // A stand-in for nextest: on SIGTERM it writes the JUnit report with the test it was running aborted, exits 143.
    const runner = `const fs = require('node:fs');
process.on('SIGTERM', () => {
  fs.writeFileSync(process.env.BOUNDED_EXEC_JUNIT, '<testsuites name="nextest-run" tests="2" failures="1"><testsuite name="nt" tests="2" failures="1"><testcase name="t::ok" classname="nt"/><testcase name="t::cut" classname="nt"><failure message="process aborted with signal 15 (SIGTERM)" type="test abort">process aborted with signal 15 (SIGTERM)</failure></testcase></testsuite></testsuites>');
  process.exit(143);
});
fs.writeFileSync('runner-ready.fifo', 'ready pid=' + process.pid + '\\n');
process.stdin.resume();
`;
    let armed: Promise<void> | undefined;
    let report = '';
    const verdict = await verdictOf(
      'exec node runner.js',
      // The total bound is the subject; the grace after it is the leg's backstop, so the run ends by the runner's own
      // exit on SIGTERM, never by a SIGKILL a loaded host's slow handler would have drawn.
      { timeoutMs: 300, killAfterMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      { 'runner.js': runner },
      {},
      async (root) => {
        const ready = join(root, 'runner-ready.fifo');
        report = join(taskDirectory(join(root, '.nx', 'workspace-data'), 'app:test'), JUNIT_FILE);
        expect(spawnSync('mkfifo', [ready]).status).toBe(0);
        const real = createProcessTreeKiller();
        return {
          async kill(pid, signal) {
            ownedProcessGroups.add(pid);
            armed ??= readFile(ready, 'utf8').then((text) => {
              // Bare `ps` through PATH: NixOS has no /bin/ps, and a failed spawn leaves stdout null.
              const observed = spawnSync('ps', ['-o', 'pid=,pgid=,stat=', '-p', String(pid)], {
                encoding: 'utf8',
              });
              const observedStdout = observed.stdout ?? '';
              const detail = `abort-report handler pid=${pid} signal=${signal} ready=${JSON.stringify(text)} state=${observedStdout.trim()} status=${observed.status} stderr=${observed.stderr} error=${observed.error}`;
              expect(text, detail).toBe(`ready pid=${pid}\n`);
              expect(observed.status, detail).toBe(0);
              const [seenPid, pgid, state] = observedStdout.trim().split(/\s+/);
              expect(Number(seenPid), detail).toBe(pid);
              expect(Number(pgid), detail).toBe(pid);
              expect(state !== undefined && !state.startsWith('Z'), detail).toBe(true);
            });
            await armed;
            await real.kill(pid, signal);
          },
        };
      },
    );
    expect(armed, 'abort-report handler readiness must precede the total-bound signal').toBeDefined();
    await armed;
    expect(verdict).toMatchObject({ outcome: 'bound', bound: 'total', limitMs: 300 });
    expect(junitFailures(await readFile(report, 'utf8')), `runner abort report ${report} must name nt t::cut`).toEqual({
      timedOut: [],
      cancelled: ['nt t::cut'],
      failed: [],
    });
  });

  it('asks the one runner in a command for a report, and leaves two runners alone', async () => {
    const { context } = await workspace();
    const directory = context.record?.directory ?? '';
    await mkdir(directory, { recursive: true });
    const report = join(directory, JUNIT_FILE);
    expect(await withJunitReport('bun test --timeout=0 src', directory)).toBe(
      `bun test --reporter=junit --reporter-outfile='${report}' --timeout=0 src`,
    );
    const nextest = await withJunitReport('extracted="$(x)" && cargo --frozen nextest run -E all', directory);
    expect(nextest).toBe(
      `extracted="$(x)" && cargo --frozen nextest run --tool-config-file 'smoo-report:${join(directory, 'nextest-report.toml')}' -E all`,
    );
    expect(await readFile(join(directory, 'nextest-report.toml'), 'utf8')).toBe(
      `[profile.default.junit]\npath = ${JSON.stringify(report)}\n`,
    );
    expect(await withJunitReport('bun test a && bun test b', directory)).toBe('bun test a && bun test b');
    expect(await withJunitReport('bun scripts/test-shard.ts 1', directory)).toBe('bun scripts/test-shard.ts 1');
  });

  it('finds bun by its basename, keeps the binary the target chose, and leaves lookalikes alone', async () => {
    const { context } = await workspace();
    const directory = context.record?.directory ?? '';
    await mkdir(directory, { recursive: true });
    const flags = `--reporter=junit --reporter-outfile='${join(directory, JUNIT_FILE)}'`;
    expect(await withJunitReport('../bun-runtime/.runtime/bun test --timeout=0 --shard=1/4', directory)).toBe(
      `../bun-runtime/.runtime/bun test ${flags} --timeout=0 --shard=1/4`,
    );
    expect(
      await withJunitReport(
        'BUN_EXE=../bun-runtime/.runtime/bun ../bun-runtime/.runtime/bun test --timeout=0 tests',
        directory,
      ),
    ).toBe(`BUN_EXE=../bun-runtime/.runtime/bun ../bun-runtime/.runtime/bun test ${flags} --timeout=0 tests`);
    expect(await withJunitReport('cd pkg && $RUNTIME/bun test src', directory)).toBe(
      `cd pkg && $RUNTIME/bun test ${flags} src`,
    );
    expect(await withJunitReport('../x/.runtime/bun test a && $HOME/.bun/bin/bun test b', directory)).toBe(
      '../x/.runtime/bun test a && $HOME/.bun/bin/bun test b',
    );
    // The basename must be exactly `bun`, and an assignment is not a command word.
    expect(await withJunitReport('../x/debug-bun test src', directory)).toBe('../x/debug-bun test src');
    expect(await withJunitReport('../x/bun-test test src', directory)).toBe('../x/bun-test test src');
    expect(await withJunitReport('BUN_EXE=../x/bun test', directory)).toBe('BUN_EXE=../x/bun test');
  });

  it('calls a failed run bound only when every failed task has a bound verdict for its hash', async () => {
    const { data } = await workspace();
    const record = async (task: string, hash: string, verdict: BoundedExecVerdict) => {
      const directory = taskDirectory(data, task);
      await mkdir(directory, { recursive: true });
      await writeRecord(directory, { task, hash, verdict });
    };
    await record('a:test-1', '1', { outcome: 'bound', bound: 'total', limitMs: 120_000, elapsedMs: 120_004 });
    await record('b:cargo-test', '2', { outcome: 'bound', bound: 'test', tests: ['b t::slow'] });
    await record('c:test', 'old', { outcome: 'bound', bound: 'test', tests: ['c slow'] });
    const run = (tasks: { taskId: string; hash: string; status: number }[]) => ({ run: {}, tasks });

    const bound = await failedTasksOfRun(
      run([
        { taskId: 'a:test-1', hash: '1', status: 1 },
        { taskId: 'b:cargo-test', hash: '2', status: 1 },
        { taskId: 'c:lint', hash: '3', status: 0 },
      ]),
      data,
    );
    expect(bound.map((task) => [task.task, task.bound])).toEqual([
      ['a:test-1', true],
      ['b:cargo-test', true],
    ]);

    const mixed = await failedTasksOfRun(
      run([
        { taskId: 'a:test-1', hash: '1', status: 1 },
        { taskId: 'c:test', hash: 'new', status: 1 },
        { taskId: 'd:lint', hash: '4', status: 1 },
      ]),
      data,
    );
    expect(mixed).toEqual([
      expect.objectContaining({ task: 'a:test-1', bound: true }),
      { task: 'c:test', bound: false, reason: "its verdict is from hash old, not this run's new" },
      { task: 'd:lint', bound: false, reason: 'no bounded-exec verdict' },
    ]);
  });

  it('judges a run that exits inside its total bound by its exit alone, however far the injected clock moved', async () => {
    // The clock is faked: it moves once, when the command reports, by a generated amount short of the total bound, and
    // then the command exits. Below the bound the verdict is a function of the exit, never of how long a loaded host
    // took to get there (a land gate measured 5243 ms for `node -e ""` under a 5000 ms bound). The step is at least
    // 1 ms: Bun 1.4.3's `advanceTimersByTime(0)` moves the fake clock 1 ms, and a run that never moves it is the case
    // above. The command prints its report through `printf`, so the line naming the command never matches it.
    const reported = 'bounded-exec-property-reported';
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 600_000 }).chain((timeoutMs) =>
          fc.record({
            timeoutMs: fc.constant(timeoutMs),
            elapsedMs: fc.integer({ min: 1, max: timeoutMs - 1 }),
            killAfterMs: fc.integer({ min: 0, max: timeoutMs }),
            exitCode: fc.integer({ min: 0, max: 125 }),
          }),
        ),
        async ({ timeoutMs, elapsedMs, killAfterMs, exitCode }) => {
          const { context } = await workspace();
          const { movedMs } = await onProgressClock(
            (chunk) => chunk.includes(reported),
            elapsedMs,
            () =>
              runBoundedExec(
                {
                  command: `printf '%s-%s\\n' bounded-exec property-reported; exit ${exitCode}`,
                  timeoutMs,
                  killAfterMs,
                },
                context,
                createProcessTreeKiller(),
                null,
              ),
          );
          expect(movedMs).toBe(elapsedMs);
          expect((await readRecord(context.record?.directory ?? ''))?.verdict).toEqual(
            exitCode === 0 ? { outcome: 'passed' } : { outcome: 'failed', exitCode, tests: [] },
          );
        },
      ),
      { numRuns: 40 },
    );
  });
});
