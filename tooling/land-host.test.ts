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
import { judgeHistory, PRE_REWRITE_MAIN } from './land-history.ts';
import type { Call } from './land-host.plan.ts';

// Behavioural tests of tooling/land-host.sh: the script runs for real, against real git repositories for main and the
// workspace and the real ancestry guard and failure reporter. Three things are stand-ins on PATH, because they are this machine's
// operator tooling and the gate itself: `cowshed` (a workspace is a git clone of main; rebase and land and their
// fences are git operations), `direnv` (runs the command it is given) and `bun nx` (land-host.fake-nx.ts plays back
// the calls a scenario scripts: its exact output and exit status). A scenario names every nx call the land may make,
// in order: a call it did not
// script fails the land.

const tooling = import.meta.dir;
const repo = dirname(tooling);
const bun = process.execPath;

const TASK = 'cowshed:cargo-test-cowshed-core-shard1';
// The landing gate is never CPU-limited, so the wrapper asks the kernel for the core count
// rather than carrying a constant. The fixture does the same, which means it asserts that the
// wrapper reads the host - not that it passes some number this file happens to know.
const CORES = String(spawnSync('sysctl', ['-n', 'hw.ncpu'], { encoding: 'utf8' }).stdout).trim();
const GATE = ['run-many', '-t', 'gate', `--parallel=${CORES}`, '--nx-bail=false', '--outputStyle=static-failures-only'];
const DAEMON_STOP: Call = { args: ['daemon', '--stop'], exit: 0 };
const passingRun = (args: readonly string[]): Call => ({ args, exit: 0 });

/** The task's actual output followed by Nx's final list; repeated entries must be shown only once. */
function failedRun(message: string, exit = 1): Call {
  return {
    args: GATE,
    exit,
    output: [
      '> nx run ' + TASK,
      '',
      message,
      '',
      message,
      '',
      ' NX   Running target gate failed',
      '',
      'Failed tasks:',
      '',
      '- ' + TASK,
      '- ' + TASK,
      '',
    ].join('\n'),
  };
}

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

// Like the release Git fixtures: no user hooks/prompts and no fsync for repositories deleted after each test.
const GIT_FIXTURE_ENV: Record<string, string> = {
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_CONFIG_SYSTEM: '/dev/null',
  GIT_TERMINAL_PROMPT: '0',
  GIT_OPTIONAL_LOCKS: '0',
  GIT_CONFIG_COUNT: '1',
  GIT_CONFIG_KEY_0: 'core.fsync',
  GIT_CONFIG_VALUE_0: 'none',
  GIT_AUTHOR_NAME: 'land test',
  GIT_AUTHOR_EMAIL: 'land@test.invalid',
  GIT_COMMITTER_NAME: 'land test',
  GIT_COMMITTER_EMAIL: 'land@test.invalid',
};

function git(directory: string, ...args: string[]): string {
  const run = spawnSync('git', ['-C', directory, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...GIT_FIXTURE_ENV },
  });
  if (run.status !== 0) {
    throw new Error(
      `git ${args.join(' ')} in ${directory}: ${run.error?.message ?? (run.stderr || 'terminated by ' + run.signal)}`,
    );
  }
  return run.stdout.trim();
}

interface LandRun {
  readonly status: number;
  /** What the script said on stderr. */
  readonly log: string;
  /** The nx calls it made, in order. */
  readonly nx: readonly string[];
  /** Arguments received by the planted unavailable replay tool, when this fixture uses it. */
  readonly replayArgs?: readonly string[];
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
function fixture(replayFails = false): Land {
  const root = mkdtempSync(join(tmpdir(), 'land-host-'));
  roots.push(root);
  const main = join(root, 'main');
  const ws = join(root, 'ws');
  const state = join(root, 'state');
  const bin = join(root, 'bin');
  mkdirSync(join(main, 'tooling'), { recursive: true });
  mkdirSync(state);
  mkdirSync(bin);
  for (const file of ['land-host.sh', 'land-history.ts', 'land-summary.ts']) {
    cpSync(join(tooling, file), join(main, 'tooling', file));
  }
  symlinkSync(join(repo, 'node_modules'), join(main, 'node_modules'));
  writeFileSync(join(main, 'nx.json'), '{}\n');
  writeFileSync(join(main, '.envrc'), '# approved\n');
  writeFileSync(join(main, '.gitignore'), 'node_modules\n.nx\n');
  git(main, 'init', '-q', '-b', 'main');
  git(main, 'add', '.');
  git(main, 'commit', '-q', '-m', 'chore: start');
  git(root, 'clone', '-q', main, ws);
  git(ws, 'checkout', '-q', '-b', 'work');
  writeFileSync(join(ws, 'change.txt'), 'the change\n');
  git(ws, 'add', 'change.txt');
  git(ws, 'commit', '-q', '-m', 'feat(tooling): the change');
  for (const [name, text] of Object.entries(stubs)) {
    writeFileSync(join(bin, name), text);
    chmodSync(join(bin, name), 0o755);
  }
  if (replayFails) {
    const timeout = join(bin, 'timeout');
    writeFileSync(timeout, '#!/bin/sh\nprintf \'%s\\n\' "$@" >"$FIX_STATE/replay.args"\nexit 73\n');
    chmodSync(timeout, 0o755);
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
          ...GIT_FIXTURE_ENV,
          PATH: `${bin}:${process.env.PATH}`,
          FIX_BUN: bun,
          FIX_FAKE_NX: join(tooling, 'land-host.fake-nx.ts'),
          FIX_STATE: state,
          FIX_MAIN: main,
          FIX_WS: ws,
        },
      });
      // The production failure log outlives the script; this fixture owns and removes its retained logs.
      for (const match of run.stderr.matchAll(/complete (?:gate )?output: (.+)/g)) {
        if (match[1] !== undefined) roots.push(match[1]);
      }
      const nxLog = join(state, 'nx.log');
      const replayLog = join(state, 'replay.args');
      return {
        status: run.status ?? -1,
        log: run.stderr,
        nx: existsSync(nxLog) ? readFileSync(nxLog, 'utf8').trimEnd().split('\n') : [],
        replayArgs: existsSync(replayLog) ? readFileSync(replayLog, 'utf8').trimEnd().split('\n') : undefined,
      };
    },
  };
}

describe('land-host.sh', () => {
  it('refuses old ancestry and accepts only an explicit onto rebase without replaying the old range', () => {
    const land = fixture();
    const oldMain = git(land.main, 'rev-parse', 'HEAD');
    const sourceBefore = git(land.ws, 'rev-parse', 'HEAD');
    const newMain = git(land.main, 'commit-tree', 'HEAD^{tree}', '-m', 'chore: rewritten base');
    git(land.main, 'update-ref', 'refs/heads/main', newMain, oldMain);
    expect(PRE_REWRITE_MAIN).toBe('e9e596563bfaa8b337893bb7df9937f9f65d06ea');
    expect(judgeHistory(land.ws, newMain, oldMain)).toEqual({
      kind: 'refused',
      why: 'source ancestry contains pre-rewrite main ' + oldMain,
      remedy: 'git rebase --onto ' + newMain + ' ' + oldMain.slice(0, 9),
    });
    expect(git(land.ws, 'rev-parse', 'HEAD')).toBe(sourceBefore);
    git(land.ws, 'fetch', '-q', land.main, newMain);
    git(land.ws, 'rebase', '--onto', newMain, oldMain);
    expect(judgeHistory(land.ws, newMain, oldMain)).toEqual({ kind: 'clean' });
    expect(judgeHistory(join(land.ws, 'missing'), newMain, oldMain).kind).toBe('unavailable');
    const run = land.land([DAEMON_STOP, passingRun(GATE)]);
    expect(run.status, run.log).toBe(0);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(git(land.ws, 'rev-parse', 'HEAD'));
  });

  it('rebases, stops the daemon, gates once without bailing, and fast-forwards exactly the gated commit', () => {
    const land = fixture();
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, passingRun(GATE)]);
    expect(run.log).toContain('gating');
    expect(run.status, run.log).toBe(0);
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
    expect(existsSync(join(land.ws, 'agent-todo/land-ledger.md'))).toBe(false);
  });

  it('captures directly to one file and replays exactly the final 64 KiB without changing the gate verdict', () => {
    const land = fixture();
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const suffix = '\ncapture-suffix\n';
    const run = land.land([
      DAEMON_STOP,
      {
        ...passingRun(GATE),
        requireFileOutput: true,
        output: `capture-prefix\n${'x'.repeat(70_000)}${suffix.slice(0, -1)}`,
      },
    ]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).not.toContain('capture-prefix');
    expect(run.log).toContain('capture-suffix');
    expect(run.log.match(/x{1000,}/)?.[0]?.length).toBe(65_536 - suffix.length);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
  });

  it('reports the complete captured failure even when its summary precedes the replay suffix', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const message = 'error: failure before the replay suffix';
    const failed = failedRun(message, 7);
    const run = land.land([
      DAEMON_STOP,
      { ...failed, requireFileOutput: true, output: `${failed.output}\n${'x'.repeat(70_000)}\ncapture-suffix` },
    ]);
    expect(run.status, run.log).toBe(7);
    expect(run.log).toContain('failed ' + TASK + '\n' + message);
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('refuses to land when bounded replay fails after a successful gate', () => {
    const land = fixture(true);
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, { ...passingRun(GATE), requireFileOutput: true }]);
    expect(run.status, run.log).toBe(1);
    expect(run.log).toContain('could not replay the gate output');
    expect(run.replayArgs?.slice(0, -1)).toEqual(['--kill-after=1s', '10s', 'tail', '-c', '65536']);
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  for (const message of [
    'error: expect(received).toEqual(expected)\n\nExpected: []\nReceived: ["orphan"]\n    at fixture.test.ts:206:43',
    'error: daemon to answer PING: no event within 30000 ms',
    'TIMEOUT [30.000s] test_case',
    'bounded-exec: outlived its 120000ms bound',
    'bounded-exec: wedged, no test finished for 180000ms',
  ]) {
    it('stops immediately on ' + message.split('\n')[0] + ', without rerunning or committing a ledger', () => {
      const land = fixture();
      const before = git(land.main, 'rev-parse', 'HEAD');
      const gated = git(land.ws, 'rev-parse', 'HEAD');
      const run = land.land([DAEMON_STOP, failedRun(message, 7)]);
      expect(run.status, run.log).toBe(7);
      expect(run.log).toContain('failed ' + TASK + '\n' + message);
      expect(run.log.split(message)).toHaveLength(2);
      expect(run.log).toContain('gate exited 7; the land stops');
      expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
      expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
      expect(git(land.ws, 'rev-parse', 'HEAD')).toBe(gated);
      expect(existsSync(join(land.ws, 'agent-todo/land-ledger.md'))).toBe(false);
      const output = /complete output: (.+)/.exec(run.log)?.[1];
      expect(output).toBeDefined();
      if (output !== undefined) expect(readFileSync(output, 'utf8')).toContain(message);
    });
  }

  it('stops on a failed gate with no named task, preserving its exact infrastructure error', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, { args: GATE, exit: 1, output: 'nx: the daemon died' }]);
    expect(run.status, run.log).toBe(1);
    expect(run.log).toContain('Gate failed without a task-level failure summary:\nnx: the daemon died');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('stops if the daemon cannot be stopped, without starting the gate', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([{ args: ['daemon', '--stop'], exit: 1, output: 'daemon stop refused' }]);
    expect(run.status, run.log).toBe(1);
    expect(run.nx).toEqual(['daemon --stop']);
    expect(run.log).toContain('could not stop');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('keeps the failing gate status and raw diagnostics when the reporter is unavailable', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    rmSync(join(land.main, 'tooling/land-summary.ts'));
    const failed = failedRun('error: original failure', 9);
    const run = land.land([
      DAEMON_STOP,
      { ...failed, requireFileOutput: true, output: `${failed.output}\n${'x'.repeat(70_000)}\ncapture-suffix` },
    ]);
    expect(run.status, run.log).toBe(9);
    expect(run.log).toContain('failure summary unavailable');
    expect(run.log).not.toContain('error: original failure');
    expect(run.log).toContain('capture-suffix');
    expect(run.log.match(/x{1000,}/)?.[0]?.length).toBe(65_536 - '\ncapture-suffix\n'.length);
    const output = /complete gate output: (.+)/.exec(run.log)?.[1];
    expect(output).toBeDefined();
    if (output !== undefined) expect(readFileSync(output, 'utf8')).toContain('error: original failure');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('rebases and gates again when main moved while it gated, and lands the new gate', () => {
    const land = fixture();
    const run = land.land([DAEMON_STOP, passingRun(GATE), DAEMON_STOP, passingRun(GATE)], true);
    expect(run.log).toContain('main moved while the gate ran');
    expect(run.status, run.log).toBe(0);
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
