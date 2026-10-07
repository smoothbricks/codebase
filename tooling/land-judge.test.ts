import { describe, expect, it } from 'bun:test';
import {
  declaredTests,
  fullRun,
  judgeCoverage,
  judgeRun,
  judgeTask,
  panickedTest,
  parseOutcome,
  parseReport,
  skippedGroups,
} from './land-judge.ts';

/** nextest's JUnit root as the runner writes it: `tests` run, `failures` among them. */
function nextestReport(tests: number, failures: number): string {
  return `<?xml version="1.0" encoding="UTF-8"?>\n<testsuites name="nextest-run" tests="${tests}" skipped="0" failures="${failures}" errors="0" time="1.2">\n</testsuites>\n`;
}

function bunReport(tests: number, failures: number): string {
  return `<?xml version="1.0" encoding="UTF-8"?>\n<testsuites name="bun test" tests="${tests}" assertions="2" failures="${failures}" skipped="1" time="0.14">\n</testsuites>\n`;
}

const TASK = 'cowshed:cargo-test-cowshed-core-shard1';
const shard = 'cowshed-core apfs::tests::real_apfs_attach';

/** A bounded-exec `verdict.json` for `TASK`. */
const recordOf = (verdict: unknown) => ({ task: TASK, hash: '1', verdict });
const perTestBound = (...tests: string[]) => recordOf({ outcome: 'bound', bound: 'test', tests });
const totalBound = recordOf({ outcome: 'bound', bound: 'total', limitMs: 120000, elapsedMs: 120400 });

describe('parseReport', () => {
  it('reads the runner, the tests it ran and the tests that failed', () => {
    expect(parseReport(nextestReport(303, 2))).toEqual({ runner: 'nextest', tests: 303, failed: 2 });
    expect(parseReport(bunReport(4, 2))).toEqual({ runner: 'bun', tests: 4, failed: 2 });
  });

  it('adds nextest errors to its failures', () => {
    const xml = '<testsuites name="nextest-run" tests="9" failures="1" errors="2"></testsuites>';
    expect(parseReport(xml)?.failed).toBe(3);
  });

  it('refuses a report no known runner wrote, or one that does not count its failures', () => {
    expect(parseReport('<testsuites name="mocha" tests="1" failures="0"></testsuites>')).toBeNull();
    expect(parseReport('<testsuites name="nextest-run" tests="1"></testsuites>')).toBeNull();
    expect(parseReport('not xml')).toBeNull();
  });
});

describe('panickedTest', () => {
  const failed = (type: string, text: string) =>
    `<testsuites name="nextest-run" tests="2" failures="1"><testcase name="fine"/><testcase name="slow"><failure type="${type}">${text}</failure><system-err>${text}</system-err></testcase></testsuites>`;

  it("finds a timed-out test that panicked in a thread before it hung, in nextest's own words", () => {
    const text =
      'thread &apos;&lt;unnamed&gt;&apos; (608397262) panicked at src/lib.rs:18:41:\nassertion `left == right` failed';
    expect(panickedTest(failed('test timeout', text))).toBe('slow');
  });

  it('finds a failed assertion with no panic line, and bun’s AssertionError', () => {
    expect(panickedTest(failed('test timeout', 'assertion failed: ready'))).toBe('slow');
    expect(panickedTest(failed('AssertionError', 'expect(received).toBe(expected)'))).toBe('slow');
  });

  it('finds nothing in a timeout that printed nothing, in output that merely says a word, or in a passing test', () => {
    expect(panickedTest(failed('test timeout', ''))).toBeNull();
    expect(panickedTest(failed('test timeout', 'waiting for the attach to settle'))).toBeNull();
    expect(
      panickedTest(
        '<testsuites name="nextest-run" tests="1" failures="0"><testcase name="ok"><system-out>panicked at</system-out></testcase></testsuites>',
      ),
    ).toBeNull();
  });
});

describe('declaredTests', () => {
  it("reads nextest's start line, whatever colour it carries", () => {
    const output =
      '\u001B[1;32m    Starting\u001B[0m 303 tests across 12 binaries (5 skipped)\n     Summary 2 tests run';
    expect(declaredTests(output, 'nextest')).toBe(303);
    expect(declaredTests('    Starting 1 test across 1 binary (102 binaries skipped)', 'nextest')).toBe(1);
  });

  it("reads bun's summary line", () => {
    expect(declaredTests(' 1 pass\n 2 fail\nRan 4 tests across 1 file. [142.00ms]', 'bun')).toBe(4);
  });

  it('takes the last run when the output holds more than one, and none when the runner never said', () => {
    expect(declaredTests('Starting 5 tests across 1 binary\nStarting 9 tests across 1 binary', 'nextest')).toBe(9);
    expect(declaredTests('Compiling cowshed-core', 'nextest')).toBeNull();
    expect(declaredTests('Ran 4 tests across 1 file.', 'nextest')).toBeNull();
  });
});

describe('parseOutcome', () => {
  // The closing summary of a real gate run on a loaded host, as Nx printed it.
  const real = `
> nx run cowshed:cargo-test-cowshed-cli

     SIGTERM [   0.331s] (236/279) cowshed-core::landing content_that_reached_the_target_by_squash_or_rewrite_is_landed_without_being_an_ancestor
────────────

 NX   Running targets lint, test, build for 27 projects and 128 tasks they depend on failed

Tasks not run because their dependencies failed or --nx-bail=true:

- nx-plugin:test
- cowshed:test
- @smoothbricks/codebase:cargo-test

Failed tasks:

- cowshed:cargo-test-cowshed-cli
- nx-plugin:test-shard4
- cowshed:cargo-test-cowshed-core-shard5

Output of 163 successful tasks were not shown. Run with --verbose or --output-style=static to see it.


 NX   Nx detected 2 flaky tasks

  cowshed:cargo-test-cowshed-core-shard1
`;

  it('reads the failed and the skipped tasks out of the closing summary, and stops each list where it ends', () => {
    expect(parseOutcome(real)).toEqual({
      failed: ['cowshed:cargo-test-cowshed-cli', 'nx-plugin:test-shard4', 'cowshed:cargo-test-cowshed-core-shard5'],
      skipped: ['nx-plugin:test', 'cowshed:test', '@smoothbricks/codebase:cargo-test'],
      stopped: [],
    });
  });

  it('reads a summary that Nx coloured, and names the stopped tasks', () => {
    const esc = String.fromCharCode(27);
    const coloured = `${esc}[2mTasks stopped before they finished:${esc}[22m\n\n${esc}[2m-${esc}[22m a:test\n\n${esc}[2mFailed tasks:${esc}[22m\n\n${esc}[2m-${esc}[22m b:test\n`;
    expect(parseOutcome(coloured)).toEqual({ failed: ['b:test'], skipped: [], stopped: ['a:test'] });
  });

  it('takes the last summary, and finds nothing in a run that printed none', () => {
    expect(parseOutcome('Failed tasks:\n\n- a:x\n\nFailed tasks:\n\n- b:y\n').failed).toEqual(['b:y']);
    expect(parseOutcome('nx: the daemon died')).toEqual({ failed: [], skipped: [], stopped: [] });
    expect(parseOutcome('Failed tasks:\n\nsomething else').failed).toEqual([]);
  });
});

describe('judgeTask', () => {
  it('calls a per-test timeout, and a command that outlived its ceiling, bounds', () => {
    expect(judgeTask(TASK, perTestBound(shard), nextestReport(5, 1))).toEqual({
      task: TASK,
      bound: true,
      why: `per-test timeout only: ${shard}`,
    });
    expect(judgeTask(TASK, totalBound, null)).toEqual({ task: TASK, bound: true, why: 'outlived its 120000ms bound' });
  });

  it('refuses a planted assertion failure, a wedge, a record of another task and no record', () => {
    const planted = recordOf({ outcome: 'failed', exitCode: 100, tests: ['pkg asserts'] });
    expect(judgeTask(TASK, planted, nextestReport(5, 1))).toEqual({
      task: TASK,
      bound: false,
      why: 'failed: pkg asserts',
    });
    const wedged = recordOf({ outcome: 'wedged', idleMs: 60000, elapsedMs: 61000 });
    expect(judgeTask(TASK, wedged, null).bound).toBe(false);
    expect(judgeTask('other:test', perTestBound(shard), null)).toEqual({
      task: 'other:test',
      bound: false,
      why: 'no bounded-exec verdict',
    });
    expect(judgeTask(TASK, null, null).bound).toBe(false);
    expect(judgeTask(TASK, recordOf({ outcome: 'failed', exitCode: 2, tests: [] }), null).why).toBe(
      'exited 2 with no report naming a timeout',
    );
  });

  it('refuses a timeout behind a panic: the test failed on its own and then hung', () => {
    const report = `<testsuites name="nextest-run" tests="1" failures="1"><testcase name="${shard}"><failure type="test timeout">thread panicked at src/lib.rs:1:1</failure></testcase></testsuites>`;
    expect(judgeTask(TASK, perTestBound(shard), report)).toEqual({
      task: TASK,
      bound: false,
      why: `${shard} panicked or failed an assertion before it timed out`,
    });
  });
});

describe('judgeRun', () => {
  const recorded = (task: string) => ({ record: task === TASK ? perTestBound(shard) : null, report: null });

  it('lands only a run whose every failed task is a bound, in smoo-nx-bound-failures’ format', () => {
    expect(judgeRun({ failed: [TASK], skipped: [], stopped: [] }, recorded)).toEqual({
      ok: true,
      lines: [`bound  ${TASK}: per-test timeout only: ${shard}`],
    });
  });

  it('stops on one task that failed on more than a bound, a task Nx stopped, and a run that named no failure', () => {
    const mixed = judgeRun({ failed: [TASK, 'lmao:lint'], skipped: [], stopped: [] }, recorded);
    expect(mixed.ok).toBe(false);
    expect(mixed.lines).toEqual([
      `bound  ${TASK}: per-test timeout only: ${shard}`,
      'failed lmao:lint: no bounded-exec verdict',
    ]);
    expect(judgeRun({ failed: [TASK], skipped: [], stopped: ['a:test'] }, recorded).ok).toBe(false);
    expect(judgeRun({ failed: [], skipped: [], stopped: [] }, recorded)).toEqual({
      ok: false,
      lines: ['failed: the run named no failed task'],
    });
  });
});

describe('judgeCoverage', () => {
  const started = 'Starting 5 tests across 1 binary';

  it('accepts a run whose every started test is in its report and whose only failures are per-test timeouts', () => {
    expect(judgeCoverage(TASK, perTestBound(shard), nextestReport(5, 1), started)).toEqual({
      covered: true,
      tests: 5,
      timedOut: [shard],
    });
  });

  it('refuses a run nextest cut short at the first failed test', () => {
    expect(judgeCoverage(TASK, perTestBound(shard), nextestReport(2, 1), started)).toEqual({
      covered: false,
      why: 'nextest started 5 tests and its report accounts for 2',
    });
  });

  it('refuses a run whose output never says how many tests were started', () => {
    expect(judgeCoverage(TASK, perTestBound(shard), nextestReport(5, 1), 'killed').covered).toBe(false);
  });

  it('refuses a command that outlived its ceiling: the tests it never reached are unknown', () => {
    expect(judgeCoverage(TASK, totalBound, nextestReport(5, 0), started)).toEqual({
      covered: false,
      why: 'it outlived its 120000ms ceiling even run alone, so the tests it never reached are unknown',
    });
  });

  it('refuses a planted assertion failure, whatever else the run shows', () => {
    const assertion = recordOf({ outcome: 'failed', exitCode: 100, tests: ['pkg asserts'] });
    expect(judgeCoverage(TASK, assertion, nextestReport(5, 1), started)).toEqual({
      covered: false,
      why: 'failed: pkg asserts',
    });
  });

  it('refuses a bound whose report also holds a failure the bound does not name', () => {
    expect(judgeCoverage(TASK, perTestBound(shard), nextestReport(5, 2), started)).toEqual({
      covered: false,
      why: 'its report holds 2 failed tests and the verdict names 1 per-test timeouts',
    });
  });

  it('refuses a timeout behind a panic even when every test ran', () => {
    const report = `<testsuites name="nextest-run" tests="5" failures="1"><testcase name="${shard}"><failure type="test timeout">thread panicked at src/lib.rs:1:1</failure></testcase></testsuites>`;
    expect(judgeCoverage(TASK, perTestBound(shard), report, started).covered).toBe(false);
  });

  it('refuses a missing record, a missing report and a report of an unknown runner', () => {
    expect(judgeCoverage(TASK, null, nextestReport(5, 1), started).covered).toBe(false);
    expect(judgeCoverage(TASK, perTestBound(shard), null, started).covered).toBe(false);
    expect(
      judgeCoverage(TASK, perTestBound(shard), '<testsuites name="mocha" tests="5" failures="1"/>', started).covered,
    ).toBe(false);
  });

  it('accounts for bun the same way, from its own summary', () => {
    expect(judgeCoverage(TASK, perTestBound('times out'), bunReport(4, 1), 'Ran 4 tests across 1 file.')).toEqual({
      covered: true,
      tests: 4,
      timedOut: ['times out'],
    });
  });
});

describe('fullRun', () => {
  it('has nextest run every test, and bun as it is', () => {
    expect(fullRun(TASK, perTestBound(shard), nextestReport(2, 1))).toEqual({
      args: ['--args=--no-fail-fast', '--forwardAllArgs=false'],
    });
    expect(fullRun(TASK, perTestBound('times out'), bunReport(4, 1))).toEqual({ args: [] });
  });

  it('runs a task killed at its ceiling as configured, since it left no report to name its runner', () => {
    expect(fullRun(TASK, totalBound, null)).toEqual({ args: [] });
    expect(fullRun(TASK, totalBound, nextestReport(2, 0))).toEqual({
      args: ['--args=--no-fail-fast', '--forwardAllArgs=false'],
    });
  });

  it('has no re-run for a per-test bound it cannot account for, or for a failure that is no bound', () => {
    expect(fullRun(TASK, perTestBound(shard), null)).toHaveProperty('why');
    expect(fullRun(TASK, recordOf({ outcome: 'failed', exitCode: 1, tests: ['x'] }), null)).toEqual({
      why: 'failed: x',
    });
  });
});

/** A task graph over `dependencies`, each id `project:target`. */
function graphOf(dependencies: Record<string, string[]>) {
  const tasks: Record<string, { target: { project: string; target: string } }> = {};
  for (const id of new Set([...Object.keys(dependencies), ...Object.values(dependencies).flat()])) {
    const [project = '', target = ''] = id.split(/:(.*)/s);
    tasks[id] = { target: { project, target } };
  }
  return { tasks, dependencies };
}

describe('skippedGroups', () => {
  const graph = graphOf({
    'cowshed:cargo-test-shard1': [],
    'cowshed:cargo-test': ['cowshed:cargo-test-shard1'],
    'cowshed:napi-test': ['cowshed:cargo-test'],
    'cowshed:test': ['cowshed:napi-test', 'cowshed:cargo-test'],
    'lmao:test': [],
  });

  it('orders the tasks a failed task kept Nx from running, behind-first', () => {
    expect(skippedGroups(graph, ['cowshed:test', 'cowshed:napi-test', 'cowshed:cargo-test'])).toEqual([
      { target: 'cargo-test', projects: ['cowshed'] },
      { target: 'napi-test', projects: ['cowshed'] },
      { target: 'test', projects: ['cowshed'] },
    ]);
  });

  it('groups one level by target, and plans nothing for tasks that all ran', () => {
    const wide = graphOf({ 'a:x': ['a:fails'], 'b:x': ['a:fails'], 'b:y': ['a:fails'], 'a:fails': [] });
    expect(skippedGroups(wide, ['a:x', 'b:x', 'b:y'])).toEqual([
      { target: 'x', projects: ['a', 'b'] },
      { target: 'y', projects: ['b'] },
    ]);
    expect(skippedGroups(graph, [])).toEqual([]);
  });

  it('refuses a task the graph does not hold, and a graph with a cycle', () => {
    expect(() => skippedGroups(graph, ['nobody:test'])).toThrow('does not hold');
    expect(() => skippedGroups(graphOf({ 'a:x': ['a:y'], 'a:y': ['a:x'] }), ['a:x', 'a:y'])).toThrow('cycle');
  });
});
