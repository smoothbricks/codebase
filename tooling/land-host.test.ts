import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import type { Call } from './land-host.plan.ts';

// Behavioural tests of tooling/land-host.sh: the script runs for real, against real git repositories for main and the
// workspace and the real tooling/land-judge.ts. Three things are stand-ins on PATH, because they are this machine's
// operator tooling and the gate itself: `cowshed` (a workspace is a git clone of main; rebase and land and their
// fences are git operations), `direnv` (runs the command it is given) and `bun nx` (land-host.fake-nx.ts plays back
// the calls a scenario scripts: what the run prints, with Nx's closing summary, and the verdict and report that
// bounded-exec leaves for each task). A scenario names every nx call the land may make, in order: a call it did not
// script fails the land.

const tooling = import.meta.dir;
const repo = dirname(tooling);
const bun = process.execPath;

const TASK = 'cowshed:cargo-test-cowshed-core-shard1';
const GATE = ['run-many', '-t', 'lint', 'test', 'build', '--nx-bail=false'];
const TIMEOUT = 'cowshed-core apfs::tests::real_apfs_attach';
/** The tasks Nx does not run behind the failed shard: the aggregate over the shards, and the `test` over it. */
const BEHIND_TASKS = ['cowshed:cargo-test', 'cowshed:test'];

interface Testcase {
  readonly name: string;
  /** Present for a failed test: the JUnit failure type and the text the test printed. */
  readonly failure?: { readonly type: string; readonly text?: string };
}

/** nextest's JUnit report of `cases` plus `passed` passing tests. */
function nextestReport(passed: number, cases: readonly Testcase[]): string {
  const failures = cases.filter((one) => one.failure !== undefined).length;
  const total = passed + cases.length;
  const body = [
    ...Array.from({ length: passed }, (_, at) => `<testcase name="passes_${at}" classname="cowshed-core" time="0.1"/>`),
    ...cases.map(({ name, failure }) =>
      failure === undefined
        ? `<testcase name="${name}" classname="cowshed-core" time="0.1"/>`
        : `<testcase name="${name}" classname="cowshed-core" time="1.0"><failure type="${failure.type}">${failure.text ?? ''}</failure><system-err>${failure.text ?? ''}</system-err></testcase>`,
    ),
  ];
  return `<?xml version="1.0" encoding="UTF-8"?>\n<testsuites name="nextest-run" tests="${total}" skipped="0" failures="${failures}" errors="0">\n<testsuite name="cowshed-core" tests="${total}">\n${body.join('\n')}\n</testsuite>\n</testsuites>\n`;
}

const timedOut = (name = TIMEOUT): Testcase => ({ name, failure: { type: 'test timeout' } });
const asserted = (name: string): Testcase => ({ name, failure: { type: 'test failure with exit code 101' } });

/** What bounded-exec leaves for one run of one task: the Nx task and its `verdict.json`. */
interface Recorded {
  readonly task: string;
  readonly record: unknown;
}

/** bounded-exec's verdict for a run of `task`, as it records it. */
function verdictOf(task: string, verdict: unknown): Recorded {
  return { task, record: { task, hash: '1', verdict } };
}

const perTestBound = (...tests: string[]): Recorded => verdictOf(TASK, { outcome: 'bound', bound: 'test', tests });
const totalBound = (): Recorded =>
  verdictOf(TASK, { outcome: 'bound', bound: 'total', limitMs: 120000, elapsedMs: 120400 });
const assertionFailure = (...tests: string[]): Recorded => verdictOf(TASK, { outcome: 'failed', exitCode: 100, tests });

/** Nx's closing summary of a failed run, as `--outputStyle=static-failures-only` prints it. */
function summary(failed: readonly string[], skipped: readonly string[] = [], stopped: readonly string[] = []): string {
  const list = (header: string, tasks: readonly string[]) =>
    tasks.length === 0 ? '' : `${header}\n\n${tasks.map((task) => `- ${task}`).join('\n')}\n\n`;
  return [
    ' NX   Running targets lint, test, build for 27 projects and 128 tasks they depend on failed',
    '',
    `${list('Tasks not run because their dependencies failed or --nx-bail=true:', skipped)}${list('Tasks stopped before they finished:', stopped)}${list('Failed tasks:', failed)}Output of 163 successful tasks were not shown. Run with --verbose or --output-style=static to see it.`,
  ].join('\n');
}

interface FailedRun {
  /** The tasks Nx did not run behind the failed one. */
  readonly skipped?: readonly string[];
  /** How many tests the runner says it started, in the output of the failed task. */
  readonly started?: number;
}

/** A run that failed on `verdict`, which `report` is the JUnit report of (none when the command was killed first). */
function failedRun(
  args: readonly string[],
  verdict: Recorded,
  report: string | undefined,
  { skipped = [], started }: FailedRun = {},
): Call {
  const taskOutput = started === undefined ? '' : `    Starting ${started} tests across 1 binary\n`;
  return {
    args,
    exit: 1,
    output: `${taskOutput}${summary([verdict.task], skipped)}`,
    records: [{ ...verdict, report }],
  };
}

const passingRun = (args: readonly string[]): Call => ({ args, exit: 0 });

/** The graph of the gate: the failed shard, the aggregate over it, and the `test` behind that. */
const graph = {
  tasks: {
    tasks: {
      'cowshed:build': { target: { project: 'cowshed', target: 'build' } },
      [TASK]: { target: { project: 'cowshed', target: 'cargo-test-cowshed-core-shard1' } },
      'cowshed:cargo-test': { target: { project: 'cowshed', target: 'cargo-test' } },
      'cowshed:test': { target: { project: 'cowshed', target: 'test' } },
    },
    dependencies: {
      'cowshed:build': [],
      [TASK]: ['cowshed:build'],
      'cowshed:cargo-test': [TASK],
      'cowshed:test': ['cowshed:cargo-test'],
    },
  },
};

const DAEMON_STOP: Call = { args: ['daemon', '--stop'], exit: 0 };
const FULL_RERUN = ['run', TASK, '--args=--no-fail-fast', '--forwardAllArgs=false'];
const ALONE = ['run', TASK];
const GRAPH = [...GATE.slice(0, -1), '--graph=*'];
const behind = (target: string): readonly string[] => [
  'run-many',
  '-t',
  target,
  '-p',
  'cowshed',
  '--excludeTaskDependencies',
  '--nx-bail=false',
];

/** The gate failing twice on `shard` with nextest's fail-fast cut of its report, then the plan's graph and `rerun`. */
function boundTwice(shard: Recorded, cutShort: string | undefined, rerun: Call): Call[] {
  const gate = failedRun(GATE, shard, cutShort, { skipped: BEHIND_TASKS });
  return [DAEMON_STOP, gate, gate, { args: GRAPH, exit: 0, graph }, rerun];
}

/** The full re-run of the shard, which times out on one test and ran all five. */
const completeRerun = failedRun(FULL_RERUN, perTestBound(TIMEOUT), nextestReport(4, [timedOut()]), { started: 5 });

const stubs: Record<string, string> = {
  direnv: `#!/bin/sh
case $1 in
allow) exit 0 ;;
status) echo 'Found RC allowed 0'; exit 0 ;;
exec) shift 2; exec "$@" ;;
esac
exit 64
`,
  bun: `#!/bin/sh
case $1 in
nx) shift; exec "$FIX_BUN" "$FIX_FAKE_NX" "$@" ;;
*) exec "$FIX_BUN" "$@" ;;
esac
`,
  cowshed: `#!/bin/sh
[ "$1" = --json ] && shift
cmd=$1
shift
fence() {
  printf '{"ok":false,"error":{"code":"conflict","fence":{"reason":"%s","observed":"%s"}}}\\n' "$1" "$2"
  exit 1
}
case $cmd in
path) if [ "$1" = main ]; then echo "$FIX_MAIN"; else echo "$FIX_WS"; fi ;;
rebase)
  shift
  [ "$1" = --expected-onto-head ] || exit 64
  now=$(git -C "$FIX_MAIN" rev-parse HEAD)
  [ "$2" = "$now" ] || fence ontoMoved "$now"
  git -C "$FIX_WS" fetch -q "$FIX_MAIN" main
  git -C "$FIX_WS" rebase -q FETCH_HEAD >&2
  echo '{"ok":true}'
  ;;
land)
  shift
  while [ "$#" -gt 0 ]; do
    case $1 in
    --expected-target-head) target=$2; shift 2 ;;
    --expected-source-head) source=$2; shift 2 ;;
    *) shift ;;
    esac
  done
  if [ -f "$FIX_STATE/move-main" ]; then
    rm "$FIX_STATE/move-main"
    (cd "$FIX_MAIN" && echo moved >moved.txt && git add moved.txt && git commit -q -m 'chore: main moves')
  fi
  now=$(git -C "$FIX_MAIN" rev-parse HEAD)
  [ "$target" = "$now" ] || fence targetMoved "$now"
  head=$(git -C "$FIX_WS" rev-parse HEAD)
  [ "$source" = "$head" ] || fence sourceMoved "$head"
  git -C "$FIX_MAIN" fetch -q "$FIX_WS" HEAD
  git -C "$FIX_MAIN" merge -q --ff-only FETCH_HEAD
  echo '{"ok":true}'
  ;;
*) exit 64 ;;
esac
`,
};

function git(directory: string, ...args: string[]): string {
  const run = spawnSync('git', ['-C', directory, ...args], { encoding: 'utf8' });
  if (run.status !== 0) {
    throw new Error(`git ${args.join(' ')} in ${directory}: ${run.stderr}`);
  }
  return run.stdout.trim();
}

interface LandRun {
  readonly status: number;
  /** What the script said on stderr. */
  readonly log: string;
  /** The nx calls it made, in order. */
  readonly nx: readonly string[];
}

interface Land {
  readonly main: string;
  readonly ws: string;
  /** Runs tooling/land-host.sh over the nx calls `plan` scripts; `moveMain` has main gain a commit as the land begins. */
  land(plan: readonly Call[], moveMain?: boolean): LandRun;
}

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) {
    rmSync(root, { recursive: true, force: true });
  }
});

/** A main repository carrying the land tooling, and a workspace one commit ahead of it. */
function fixture(): Land {
  const root = mkdtempSync(join(tmpdir(), 'land-host-'));
  roots.push(root);
  const main = join(root, 'main');
  const ws = join(root, 'ws');
  const state = join(root, 'state');
  const bin = join(root, 'bin');
  mkdirSync(join(main, 'tooling'), { recursive: true });
  mkdirSync(state);
  mkdirSync(bin);
  for (const file of ['land-host.sh', 'land-judge.ts']) {
    cpSync(join(tooling, file), join(main, 'tooling', file));
  }
  symlinkSync(join(repo, 'node_modules'), join(main, 'node_modules'));
  writeFileSync(join(main, 'nx.json'), '{}\n');
  writeFileSync(join(main, '.envrc'), '# approved\n');
  writeFileSync(join(main, '.gitignore'), 'node_modules\n.nx\n');
  git(main, 'init', '-q', '-b', 'main');
  git(main, 'config', 'user.name', 'land test');
  git(main, 'config', 'user.email', 'land@test.invalid');
  git(main, 'add', '.');
  git(main, 'commit', '-q', '-m', 'chore: start');
  git(root, 'clone', '-q', main, ws);
  git(ws, 'config', 'user.name', 'land test');
  git(ws, 'config', 'user.email', 'land@test.invalid');
  git(ws, 'checkout', '-q', '-b', 'work');
  writeFileSync(join(ws, 'change.txt'), 'the change\n');
  git(ws, 'add', 'change.txt');
  git(ws, 'commit', '-q', '-m', 'feat(tooling): the change');
  for (const [name, text] of Object.entries(stubs)) {
    writeFileSync(join(bin, name), text);
    chmodSync(join(bin, name), 0o755);
  }
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('NX_')));
  return {
    main,
    ws,
    land(plan, moveMain = false) {
      writeFileSync(join(state, 'plan.json'), JSON.stringify(plan));
      if (moveMain) {
        writeFileSync(join(state, 'move-main'), '');
      }
      const run = spawnSync('sh', [join(main, 'tooling/land-host.sh'), 'ws'], {
        cwd: root,
        encoding: 'utf8',
        env: {
          ...env,
          PATH: `${bin}:${process.env.PATH}`,
          FIX_BUN: bun,
          FIX_FAKE_NX: join(tooling, 'land-host.fake-nx.ts'),
          FIX_STATE: state,
          FIX_MAIN: main,
          FIX_WS: ws,
        },
      });
      const nxLog = join(state, 'nx.log');
      return {
        status: run.status ?? -1,
        log: run.stderr,
        nx: existsSync(nxLog) ? readFileSync(nxLog, 'utf8').trimEnd().split('\n') : [],
      };
    },
  };
}

/** The ledger of `land`'s main, or of its workspace, `null` when the file does not exist. */
function ledgerOf(land: Land, at: 'main' | 'ws' = 'main'): string | null {
  const path = join(land[at], 'agent-todo/land-ledger.md');
  return existsSync(path) ? readFileSync(path, 'utf8') : null;
}

describe('land-host.sh', () => {
  it('rebases, stops the daemon, gates every project without bailing, and fast-forwards main to the gated commit', () => {
    const land = fixture();
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, passingRun(GATE)]);
    expect(run.log).toContain('gating');
    expect(run.status).toBe(0);
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
    expect(ledgerOf(land)).toBeNull();
  });

  it('blocks a planted assertion failure at once: no re-run, nothing lands', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([
      DAEMON_STOP,
      failedRun(GATE, assertionFailure(TIMEOUT), nextestReport(4, [asserted(TIMEOUT)])),
    ]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain(`failed ${TASK}: failed: ${TIMEOUT}`);
    expect(run.log).toContain('failed on more than a wall-clock bound; the land stops');
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('blocks an assertion failure hiding behind a bound on the gate that follows it', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([
      DAEMON_STOP,
      failedRun(GATE, perTestBound(TIMEOUT), nextestReport(4, [timedOut()])),
      failedRun(GATE, assertionFailure('planted'), nextestReport(4, [asserted('planted')])),
    ]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain('the re-run of');
    expect(run.log).toContain('failed on more than a wall-clock bound; the land stops');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('blocks a failed task that left no bounded-exec verdict', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, { args: GATE, exit: 1, output: summary(['lmao:lint']) }]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain('failed lmao:lint: no bounded-exec verdict');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('does not judge a failed task by the record of the run before it', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([
      DAEMON_STOP,
      failedRun(GATE, perTestBound(TIMEOUT), nextestReport(4, [timedOut()])),
      { args: GATE, exit: 1, output: summary([TASK]) },
    ]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain(`failed ${TASK}: no bounded-exec verdict`);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('blocks a gate that failed without naming a task', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, { args: GATE, exit: 1, output: 'nx: the daemon died' }]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain('failed: the run named no failed task');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('blocks a gate that left a task stopped, even beside a bound', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([
      DAEMON_STOP,
      {
        args: GATE,
        exit: 1,
        output: summary([TASK], [], ['cowshed:launcher-test']),
        records: [{ ...perTestBound(TIMEOUT), report: nextestReport(4, [timedOut()]) }],
      },
    ]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain('failed cowshed:launcher-test: Nx stopped it before it finished');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('blocks a test that panicked and then hung until its bound, which nextest reports as a timeout', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const hung: Testcase = {
      name: TIMEOUT,
      failure: { type: 'test timeout', text: 'thread &apos;&lt;unnamed&gt;&apos; panicked at src/lib.rs:11:41:\nboom' },
    };
    const run = land.land([DAEMON_STOP, failedRun(GATE, perTestBound(TIMEOUT), nextestReport(4, [hung]))]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain(`failed ${TASK}: ${TIMEOUT} panicked or failed an assertion before it timed out`);
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('lands when the re-run passes, with nothing to ledger', () => {
    const land = fixture();
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const run = land.land([
      DAEMON_STOP,
      failedRun(GATE, perTestBound(TIMEOUT), nextestReport(4, [timedOut()])),
      passingRun(GATE),
    ]);
    expect(run.status).toBe(0);
    expect(run.log).toContain('re-running it once');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
    expect(ledgerOf(land)).toBeNull();
  });

  it('lands a per-test bound that fails twice once every test ran, and ledgers it in the file and the message', () => {
    const land = fixture();
    const run = land.land([
      ...boundTwice(perTestBound(TIMEOUT), nextestReport(1, [timedOut()]), completeRerun),
      passingRun(behind('cargo-test')),
      passingRun(behind('test')),
    ]);
    expect(run.log).toContain(`covered ${TASK}: 5 tests ran, 1 timed out on their per-test bound: ${TIMEOUT}`);
    expect(run.status).toBe(0);
    expect(run.nx.map((call) => call.replace(/--graph=.*/, '--graph=FILE'))).toEqual([
      'daemon --stop',
      GATE.join(' '),
      GATE.join(' '),
      `${GRAPH.slice(0, -1).join(' ')} --graph=FILE`,
      FULL_RERUN.join(' '),
      behind('cargo-test').join(' '),
      behind('test').join(' '),
    ]);
    const line = `- ws · ${TASK} · 5 tests ran, 1 timed out on their per-test bound: ${TIMEOUT}`;
    expect(ledgerOf(land)).toContain(line);
    expect(git(land.main, 'log', '-1', '--format=%s')).toBe("docs(tooling): ledger ws's bound-only land");
    expect(git(land.main, 'log', '-1', '--format=%B')).toContain(line);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(git(land.ws, 'rev-parse', 'HEAD'));
    expect(git(land.main, 'diff', '--name-only', 'HEAD~1')).toBe('agent-todo/land-ledger.md');
  });

  it('blocks a bound whose full re-run still holds a test nextest never ran', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const cut = failedRun(FULL_RERUN, perTestBound(TIMEOUT), nextestReport(1, [timedOut()]), { started: 5 });
    const run = land.land(boundTwice(perTestBound(TIMEOUT), nextestReport(1, [timedOut()]), cut));
    expect(run.status).not.toBe(0);
    expect(run.log).toContain(`blocked ${TASK}: nextest started 5 tests and its report accounts for 2`);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
    expect(ledgerOf(land, 'ws')).toBeNull();
  });

  it('blocks an assertion that only the full re-run reached', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const full = failedRun(
      FULL_RERUN,
      assertionFailure('behind_the_timeout'),
      nextestReport(3, [timedOut(), asserted('behind_the_timeout')]),
      { started: 5 },
    );
    const run = land.land(boundTwice(perTestBound(TIMEOUT), nextestReport(1, [timedOut()]), full));
    expect(run.status).not.toBe(0);
    expect(run.log).toContain('failed on more than a wall-clock bound when every test ran; the land stops');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('gives a task that outlived its ceiling one run alone, and lands when it passes then', () => {
    const land = fixture();
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const run = land.land([
      ...boundTwice(totalBound(), undefined, passingRun(ALONE)),
      passingRun(behind('cargo-test')),
      passingRun(behind('test')),
    ]);
    expect(run.log).toContain(`${TASK} passed with every test running; nothing of it is tolerated`);
    expect(run.status).toBe(0);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
    expect(ledgerOf(land)).toBeNull();
  });

  it('blocks a task that outlives its ceiling even alone: the tests it never reached are unknown', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land(boundTwice(totalBound(), undefined, failedRun(ALONE, totalBound(), undefined)));
    expect(run.status).not.toBe(0);
    expect(run.log).toContain(`blocked ${TASK}: it outlived its 120000ms ceiling even run alone`);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('runs the tasks behind a tolerated failure and blocks when one of them fails on more than a bound', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([
      ...boundTwice(perTestBound(TIMEOUT), nextestReport(1, [timedOut()]), completeRerun),
      {
        args: behind('cargo-test'),
        exit: 1,
        output: summary(['cowshed:cargo-test']),
        records: [
          {
            task: 'cowshed:cargo-test',
            record: { task: 'cowshed:cargo-test', hash: '2', verdict: { outcome: 'failed', exitCode: 1, tests: [] } },
          },
        ],
      },
    ]);
    expect(run.status).not.toBe(0);
    expect(run.log).toContain('running cargo-test for cowshed, which a failed task kept Nx from running');
    expect(run.log).toContain('failed on more than a wall-clock bound; the land stops');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
    expect(ledgerOf(land, 'ws')).toBeNull();
  });

  it('rebases and gates again when main moved while it gated, and lands the new gate', () => {
    const land = fixture();
    const run = land.land([DAEMON_STOP, passingRun(GATE), DAEMON_STOP, passingRun(GATE)], true);
    expect(run.log).toContain('main moved while the gate ran');
    expect(run.status).toBe(0);
    expect(run.nx).toHaveLength(4);
    expect(existsSync(join(land.main, 'moved.txt'))).toBe(true);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(git(land.ws, 'rev-parse', 'HEAD'));
  });

  it('refuses to land main itself', () => {
    const land = fixture();
    const run = spawnSync('sh', [join(land.main, 'tooling/land-host.sh'), 'main'], { encoding: 'utf8' });
    expect(run.status).toBe(64);
    expect(run.stderr).toContain('main is not a workspace to land');
  });
});
