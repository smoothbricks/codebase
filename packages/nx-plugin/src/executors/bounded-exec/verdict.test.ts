import { afterEach, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { type BoundedExecContext, createProcessTreeKiller, runBoundedExec } from './executor.js';
import {
  type BoundedExecVerdict,
  failedTasksOfRun,
  JUNIT_FILE,
  judgeRun,
  junitFailures,
  readRecord,
  taskDirectory,
  withJunitReport,
  writeRecord,
} from './verdict.js';

const roots: string[] = [];
const originalStdoutWrite = process.stdout.write;
const originalStderrWrite = process.stderr.write;

afterEach(async () => {
  process.stdout.write = originalStdoutWrite;
  process.stderr.write = originalStderrWrite;
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
  bounds: { timeoutMs: number; idleTimeoutMs?: number; killAfterMs?: number },
  files: Record<string, string> = {},
  links: Record<string, string> = {},
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
  await runBoundedExec({ command, killAfterMs: 0, ...bounds }, context, createProcessTreeKiller(), null, null);
  const record = await readRecord(context.record?.directory ?? '');
  expect(record?.task).toBe('app:test');
  expect(record?.hash).toBe('42');
  return record?.verdict;
}

// The fixtures run in child processes and must really outlive their runner's bound: the verdict
// is about which real bound fired, so no fake clock can stand in for it.
const SLOW_TEST = `import { test } from 'bun:test';\ntest('slow', async () => { await new Promise((r) => setTimeout(r, 5000)); });\n`;
const FAILING_TEST = `import { expect, test } from 'bun:test';\ntest('wrong', () => { expect(1).toBe(2); });\n`;

describe('bounded-exec verdicts', () => {
  it('records a bun run whose only failure is a per-test timeout as a bound', async () => {
    expect(
      await verdictOf('bun test --timeout=100 ./slow.test.ts', { timeoutMs: 60_000 }, { 'slow.test.ts': SLOW_TEST }),
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
        { timeoutMs: 60_000 },
        { 'slow.test.ts': SLOW_TEST },
        { '.runtime/bun': process.execPath },
      ),
    ).toEqual({ outcome: 'bound', bound: 'test', tests: ['slow'] });
  });

  it('records an assertion as a failure even beside a timeout', async () => {
    expect(
      await verdictOf(
        'bun test --timeout=100 ./slow.test.ts ./failing.test.ts',
        { timeoutMs: 60_000 },
        { 'slow.test.ts': SLOW_TEST, 'failing.test.ts': FAILING_TEST },
      ),
    ).toEqual({ outcome: 'failed', exitCode: 1, tests: ['wrong'] });
  });

  it('records the total bound as a bound and the idle bound as a wedge', async () => {
    expect(await verdictOf('node -e "setInterval(() => console.log(1), 10)"', { timeoutMs: 200 })).toMatchObject({
      outcome: 'bound',
      bound: 'total',
      limitMs: 200,
    });
    expect(
      await verdictOf('node -e "setTimeout(() => {}, 30000)"', { timeoutMs: 30_000, idleTimeoutMs: 100 }),
    ).toMatchObject({
      outcome: 'wedged',
      idleMs: 100,
    });
  });

  it('records a failing command with no report as a failure, and a passing one as passed', async () => {
    expect(await verdictOf('node -e "process.exit(3)"', { timeoutMs: 5_000 })).toEqual({
      outcome: 'failed',
      exitCode: 3,
      tests: [],
    });
    expect(await verdictOf('node -e ""', { timeoutMs: 5_000 })).toEqual({ outcome: 'passed' });
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
    expect(judgeRun({ exitCode: 143, elapsedMs: 120_850, expiry: total, report: cut })).toEqual({
      outcome: 'bound',
      bound: 'total',
      limitMs: 120_000,
      elapsedMs: 120_850,
    });
    // An assertion that failed before the ceiling is still a failure, and an abort nobody asked for is one too.
    expect(
      judgeRun({ exitCode: 143, elapsedMs: 120_850, expiry: total, report: { ...cut, failed: ['nt t::assert'] } }),
    ).toEqual({ outcome: 'failed', exitCode: 143, tests: ['nt t::assert'] });
    expect(judgeRun({ exitCode: 143, elapsedMs: 850, expiry: null, report: cut })).toEqual({
      outcome: 'failed',
      exitCode: 143,
      tests: ['nt t::cut'],
    });
  });

  it('records a test aborted with no total bound to explain it as a failure, whatever the exit or the other tests say', () => {
    const cancelled = ['nt t::cut'];
    // Exit 0 with an abort in the report: the abort is still not nothing.
    expect(
      judgeRun({ exitCode: 0, elapsedMs: 850, expiry: null, report: { timedOut: [], cancelled, failed: [] } }),
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
      }),
    ).toEqual({ outcome: 'failed', exitCode: 100, tests: cancelled });
    // With the total bound fired, the same report is the bound's own casualties.
    expect(
      judgeRun({
        exitCode: 143,
        elapsedMs: 120_850,
        expiry: { kind: 'total', limitMs: 120_000 },
        report: { timedOut: ['nt t::slow'], cancelled, failed: [] },
      }),
    ).toMatchObject({ outcome: 'bound', bound: 'total' });
  });

  it('records a runner that reports its own abort at the total ceiling as a bound, end to end', async () => {
    // A stand-in for nextest: on SIGTERM it writes the JUnit report with the test it was running aborted, exits 143.
    const runner = `const fs = require('node:fs');
process.on('SIGTERM', () => {
  fs.writeFileSync(process.env.BOUNDED_EXEC_JUNIT, '<testsuites name="nextest-run" tests="2" failures="1"><testsuite name="nt" tests="2" failures="1"><testcase name="t::ok" classname="nt"/><testcase name="t::cut" classname="nt"><failure message="process aborted with signal 15 (SIGTERM)" type="test abort">process aborted with signal 15 (SIGTERM)</failure></testcase></testsuite></testsuites>');
  process.exit(143);
});
setInterval(() => console.log('running'), 10);
`;
    expect(
      await verdictOf('node runner.js', { timeoutMs: 300, killAfterMs: 5_000 }, { 'runner.js': runner }),
    ).toMatchObject({ outcome: 'bound', bound: 'total', limitMs: 300 });
  });

  it('asks the one runner in a command for a report, and leaves two runners alone', async () => {
    const { context } = await workspace();
    const directory = context.record?.directory ?? '';
    await mkdir(directory, { recursive: true });
    const report = join(directory, JUNIT_FILE);
    expect(await withJunitReport('bun test --timeout=30000 src', directory)).toBe(
      `bun test --reporter=junit --reporter-outfile='${report}' --timeout=30000 src`,
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
    expect(await withJunitReport('../bun-runtime/.runtime/bun test --timeout=30000 --shard=1/4', directory)).toBe(
      `../bun-runtime/.runtime/bun test ${flags} --timeout=30000 --shard=1/4`,
    );
    expect(
      await withJunitReport(
        'BUN_EXE=../bun-runtime/.runtime/bun ../bun-runtime/.runtime/bun test --timeout=30000 tests',
        directory,
      ),
    ).toBe(`BUN_EXE=../bun-runtime/.runtime/bun ../bun-runtime/.runtime/bun test ${flags} --timeout=30000 tests`);
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
});
