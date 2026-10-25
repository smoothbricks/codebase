import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
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
// workspace and the real ancestry guard, failure reporter and attribution (land-attribution.ts). Four things are
// stand-ins on PATH, because they are this machine's operator tooling and the gate itself: `cowshed` (a workspace is a
// git clone of main; rebase and land and their fences are git operations; the gate must run as
// `exec <ws> --class gate --host --`, at the workspace root, where the job names Nx's cache directory in
// NX_CACHE_DIRECTORY), `direnv` (runs the command it is given) and `bun nx` (land-host.fake-nx.ts plays back the calls
// a scenario scripts: its exact output, exit status and run summary rows). A scenario names every nx call the land may
// make, in order: a call it did not script fails the land. node runs the publisher, recording what land-publish.ts gave
// it and refusing when FIX_PUBLISH says so, and the workspace's tooling/land-inputs.mts, recording the tasks it was asked
// about and printing the inputs the fixture keeps in inputs.json.

const tooling = import.meta.dir;
const repo = dirname(tooling);
// The cowshed bridge that publishes the gate's Nx results: land-publish.ts reads it from the checkout it lands.
const BRIDGE = 'packages/cowshed/crates/cowshed-core/src/build_volume';
const bun = process.execPath;

const TASK = 'cowshed:cargo-test-cowshed-core-shard1';
// The landing gate is never CPU-limited, so the wrapper asks the kernel for the core count
// rather than carrying a constant. The fixture does the same, which means it asserts that the
// wrapper reads the host - not that it passes some number this file happens to know.
const CORES = String(spawnSync('sysctl', ['-n', 'hw.ncpu'], { encoding: 'utf8' }).stdout).trim();
const GATE = ['run-many', '-t', 'gate', `--parallel=${CORES}`, '--nx-bail=false', '--outputStyle=static-failures-only'];
const DAEMON_STOP: Call = { args: ['daemon', '--stop'], exit: 0 };
/** The build a successful land runs in main, so main's built outputs match the sources it lands. */
const MAIN_BUILD = ['run-many', '-t', 'build', '--outputStyle=static-failures-only'];
const passingRun = (args: readonly string[]): Call => ({ args, exit: 0 });
/** A whole gate that passed: its run summary carries the root aggregate's row, the hash a land publishes. */
const GATE_PASSED: Call = {
  ...passingRun(GATE),
  run: [{ taskId: '@smoothbricks/codebase:gate', hash: 'gate-hash', status: 0 }],
};
/** The files Nx hashes for each task and the tasks it waits for: the workspace's one commit writes change.txt. */
const INPUTS = { 'lib:lint': ['change.txt', 'lib/src/index.ts'], 'lib:test': ['lib/src/index.ts'] };

/** The task's actual output followed by Nx's final list; repeated entries must be shown only once. */
function failedRun(message: string, exit = 1): Call {
  return {
    args: GATE,
    exit,
    output: [
      `> nx run ${TASK}`,
      '',
      message,
      '',
      message,
      '',
      ' NX   Running target gate failed',
      '',
      'Failed tasks:',
      '',
      `- ${TASK}`,
      `- ${TASK}`,
      '',
    ].join('\n'),
  };
}

const stubs: Record<string, string> = {
  node: `#!/bin/sh
case $1 in
*/land-inputs.mts)
  # The workspace's tooling/land-inputs.mts: the input files of the failed tasks it names.
  printf '%s\\n' "$@" >"$FIX_STATE/inputs.args"
  exec cat "$FIX_STATE/inputs.json"
  ;;
esac
cat >"$FIX_STATE/publish.program"
printf '%s\\n' "$@" >"$FIX_STATE/publish.args"
[ "$FIX_PUBLISH" = refuse ] && { echo 'the publish was refused' >&2; exit 1; }
echo '{"published":1,"existing":0,"skipped":0}'
`,
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
  # A land's rebase carries at gate class (05_gateway.md, "A land is a gate job"): refuse one that does not ask.
  [ "$1" = --gate ] || exit 64
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
exec)
  [ -n "$1" ] && [ "$2" = --class ] && [ "$3" = gate ] && [ "$4" = --host ] && [ "$5" = -- ] || exit 64
  shift 5
  # The job names the directory its Nx writes the run summary into, as the workspace supervisor does.
  export NX_CACHE_DIRECTORY="$FIX_WS/.nx/cache"
  cd "$FIX_WS" && exec "$@"
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
      `git ${args.join(' ')} in ${directory}: ${run.error?.message ?? (run.stderr || `terminated by ${run.signal}`)}`,
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
  /** Where the fake tools record what they were given: the publisher's program and arguments. */
  readonly state: string;
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
function fixture(replayFails = false, publishFails = false): Land {
  const root = mkdtempSync(join(tmpdir(), 'land-host-'));
  roots.push(root);
  const main = join(root, 'main');
  const ws = join(root, 'ws');
  const state = join(root, 'state');
  const bin = join(root, 'bin');
  mkdirSync(join(main, 'tooling'), { recursive: true });
  mkdirSync(state);
  mkdirSync(bin);
  mkdirSync(join(state, 'tmp'));
  writeFileSync(join(state, 'inputs.json'), JSON.stringify(INPUTS));
  for (const file of [
    'land-host.sh',
    'land-attribution.ts',
    'land-decide.ts',
    'land-history.ts',
    'land-publish.ts',
    'land-summary.ts',
  ]) {
    cpSync(join(tooling, file), join(main, 'tooling', file));
  }
  symlinkSync(join(repo, 'node_modules'), join(main, 'node_modules'));
  // The publisher reads the real bridge from the checkout it lands, at the path BRIDGE names.
  mkdirSync(join(main, BRIDGE), { recursive: true });
  for (const file of ['cache_adapter.js', 'nx_publish.js']) {
    cpSync(join(repo, BRIDGE, file), join(main, BRIDGE, file));
  }
  writeFileSync(join(main, 'nx.json'), '{}\n');
  // The land cross-lints the gated tree for Linux after the gate; the fixture's check passes.
  writeFileSync(join(main, 'package.json'), '{"scripts":{"check:linux":"echo linux cross check passed"}}\n');
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
    state,
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
          FIX_PUBLISH: publishFails ? 'refuse' : 'accept',
          FIX_MAIN: main,
          FIX_WS: ws,
          // The land's temporary files, so a case can prove none is left behind.
          TMPDIR: join(state, 'tmp'),
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
  it('refuses old ancestry, accepts an explicit onto rebase, then gates once and lands exactly the gated commit', () => {
    const land = fixture();
    const oldMain = git(land.main, 'rev-parse', 'HEAD');
    const sourceBefore = git(land.ws, 'rev-parse', 'HEAD');
    const newMain = git(land.main, 'commit-tree', 'HEAD^{tree}', '-m', 'chore: rewritten base');
    git(land.main, 'update-ref', 'refs/heads/main', newMain, oldMain);
    expect(PRE_REWRITE_MAIN).toBe('e9e596563bfaa8b337893bb7df9937f9f65d06ea');
    expect(judgeHistory(land.ws, newMain, oldMain)).toEqual({
      kind: 'refused',
      why: `source ancestry contains pre-rewrite main ${oldMain}`,
      remedy: `git rebase --onto ${newMain} ${oldMain.slice(0, 9)}`,
    });
    expect(git(land.ws, 'rev-parse', 'HEAD')).toBe(sourceBefore);
    git(land.ws, 'fetch', '-q', land.main, newMain);
    git(land.ws, 'rebase', '--onto', newMain, oldMain);
    expect(judgeHistory(land.ws, newMain, oldMain)).toEqual({ kind: 'clean' });
    expect(judgeHistory(join(land.ws, 'missing'), newMain, oldMain).kind).toBe('unavailable');

    // The land rebases, stops the daemon, gates once without bailing, captures directly to one file and replays
    // exactly its final 64 KiB, fast-forwards main to exactly the gated commit, then builds main's outputs.
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const suffix = '\ncapture-suffix\n';
    const run = land.land([
      DAEMON_STOP,
      {
        ...passingRun(GATE),
        requireFileOutput: true,
        output: `capture-prefix\n${'x'.repeat(70_000)}${suffix.slice(0, -1)}`,
      },
      passingRun(MAIN_BUILD),
    ]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain('gating');
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' '), MAIN_BUILD.join(' ')]);
    expect(run.log).toContain("building main's outputs");
    expect(run.log).not.toContain('capture-prefix');
    expect(run.log).toContain('capture-suffix');
    expect(run.log.match(/x{1000,}/)?.[0]?.length).toBe(65_536 - suffix.length);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
    expect(existsSync(join(land.ws, 'agent-todo/land-ledger.md'))).toBe(false);
  });

  // Which diagnostics the summary keeps is land-summary.test.ts's; this land proves what the script does with them.
  it('stops immediately on a failed gate, reporting its complete captured failure once, without rerunning or committing a ledger', () => {
    const land = fixture();
    const before = git(land.main, 'rev-parse', 'HEAD');
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const message =
      'error: expect(received).toEqual(expected)\n\nExpected: []\nReceived: ["orphan"]\n    at fixture.test.ts:206:43';
    const failed = failedRun(message, 7);
    // The summary precedes more than the 64 KiB replay suffix of the captured output.
    const run = land.land([
      DAEMON_STOP,
      { ...failed, requireFileOutput: true, output: `${failed.output}\n${'x'.repeat(70_000)}\ncapture-suffix` },
    ]);
    expect(run.status, run.log).toBe(7);
    expect(run.log).toContain(`failed ${TASK}\n${message}`);
    expect(run.log.split(message)).toHaveLength(2);
    expect(run.log).toContain('gate exited 7; the land stops');
    // This gate left no run summary, so no failed task is read and no commit named; the verdict stands.
    expect(run.log).toContain('could not read which tasks the gate failed');
    expect(run.nx).toEqual(['daemon --stop', GATE.join(' ')]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
    expect(git(land.ws, 'rev-parse', 'HEAD')).toBe(gated);
    expect(existsSync(join(land.ws, 'agent-todo/land-ledger.md'))).toBe(false);
    const output = /complete output: (.+)/.exec(run.log)?.[1];
    expect(output).toBeDefined();
    if (output !== undefined) expect(readFileSync(output, 'utf8')).toContain(message);
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

  it('rebases and gates again when main moved while it gated, lands the new gate, and keeps it when the build fails', () => {
    const land = fixture();
    const run = land.land(
      [DAEMON_STOP, passingRun(GATE), DAEMON_STOP, passingRun(GATE), { args: MAIN_BUILD, exit: 1 }],
      true,
    );
    expect(run.log).toContain('main moved while the gate ran');
    expect(run.status, run.log).toBe(0);
    expect(run.nx).toHaveLength(5);
    expect(run.log).toContain("main's build exited 1; the land stands");
    expect(existsSync(join(land.main, 'moved.txt'))).toBe(true);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(git(land.ws, 'rev-parse', 'HEAD'));
  });

  it('publishes the gate task results once the Linux cross check passes, and lands', () => {
    const land = fixture();
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, GATE_PASSED, passingRun(MAIN_BUILD)]);
    expect(run.status, run.log).toBe(0);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
    // The publisher gets the cowshed bridge as one program, then the checkout and the gate's succeeded task hashes.
    const bridge = join(repo, BRIDGE);
    const program = `${readFileSync(join(bridge, 'cache_adapter.js'), 'utf8')}\n${readFileSync(join(bridge, 'nx_publish.js'), 'utf8')}`;
    expect(readFileSync(join(land.state, 'publish.program'), 'utf8')).toBe(program);
    expect(readFileSync(join(land.state, 'publish.args'), 'utf8')).toBe(`-\n${land.ws}\n["gate-hash"]\n`);
  });

  it('lands even when the publish is refused, and says so', () => {
    const land = fixture(false, true);
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, GATE_PASSED, passingRun(MAIN_BUILD)]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain('the publish was refused');
    expect(run.log).toContain('failed (status 1); the land goes on');
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
  });

  it('lands without publishing when the gate passes without a readable run summary, and leaves no temporary file', () => {
    const land = fixture();
    const gated = git(land.ws, 'rev-parse', 'HEAD');
    const run = land.land([DAEMON_STOP, passingRun(GATE), passingRun(MAIN_BUILD)]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain("could not read the gate's task hashes");
    expect(run.log).toContain('the land goes on without publishing: the cache never blocks a land');
    expect(existsSync(join(land.state, 'publish.args'))).toBe(false);
    expect(readdirSync(join(land.state, 'tmp')).filter((name) => name.startsWith('land-'))).toEqual([]);
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(gated);
  });

  it('names, for each task a red gate failed, the commits of the land that change its inputs, and keeps the verdict', () => {
    const land = fixture();
    const change = git(land.ws, 'rev-parse', 'HEAD');
    const before = git(land.main, 'rev-parse', 'HEAD');
    const run = land.land([
      DAEMON_STOP,
      {
        ...failedRun('error: lib lint', 3),
        run: [
          { taskId: 'app:lint', hash: 'app-lint', status: 0 },
          { taskId: 'lib:lint', hash: 'lib-lint', status: 1 },
          { taskId: 'lib:test', hash: 'lib-test', status: 1 },
        ],
      },
    ]);
    expect(run.status, run.log).toBe(3);
    expect(run.log).toContain(
      `land-host: lib:lint failed; 1 of the land's 1 commits change its inputs: ${change.slice(0, 10)} feat(tooling): the change\n`,
    );
    expect(run.log).toContain(
      "land-host: lib:test failed; none of the land's 1 commits changes a file Nx hashes for it or for a task it waits for (a commit that only deletes files is not seen)",
    );
    expect(run.log.indexOf('land-host: lib:lint failed')).toBeLessThan(
      run.log.indexOf('gate exited 3; the land stops'),
    );
    // Nx is asked for the input files of the failed tasks alone, by the workspace's own land-inputs.mts.
    expect(readFileSync(join(land.state, 'inputs.args'), 'utf8')).toBe(
      `${join(land.ws, 'tooling/land-inputs.mts')}\ngate\nlib:lint\nlib:test\n`,
    );
    expect(git(land.main, 'rev-parse', 'HEAD')).toBe(before);
  });

  it('names no commit from a run summary an earlier invocation wrote', () => {
    const land = fixture();
    mkdirSync(join(land.ws, '.nx/cache'), { recursive: true });
    writeFileSync(
      join(land.ws, '.nx/cache/run.json'),
      JSON.stringify({
        run: { command: 'nx run-many -t gate', startTime: '2026-01-01T00:00:00.000Z', endTime: '', inner: false },
        tasks: [{ taskId: 'lib:lint', hash: 'lib-lint', status: 1 }],
      }),
    );
    const run = land.land([DAEMON_STOP, failedRun('error: no summary written')]);
    expect(run.status, run.log).toBe(1);
    expect(run.log).toContain(
      'the run summary is of an invocation that began at 2026-01-01T00:00:00.000Z, before this gate',
    );
    expect(run.log).toContain('could not read which tasks the gate failed');
    expect(run.log).not.toContain('land-host: lib:lint failed');
  });

  it('refuses to land main itself', () => {
    const land = fixture();
    const run = spawnSync('sh', [join(land.main, 'tooling/land-host.sh'), 'main'], { encoding: 'utf8' });
    expect(run.status).toBe(64);
    expect(run.stderr).toContain('main is not a workspace to land');
  });
});
