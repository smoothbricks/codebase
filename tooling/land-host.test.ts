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
import type { Call } from './land-host.plan.ts';

// Behavioural tests of tooling/land-host.sh, the declared `[land] gate` (specs/cowshed/02_workspaces.md "A declared
// land", step 4): the script runs for real, against real git repositories for main and the workspace and the real
// failure reporter and attribution (land-attribution.ts). Four things are stand-ins on PATH, because they are this
// machine's operator tooling and the gate itself: `cowshed` (a workspace is a git clone of main, already rebased as
// cowshed's route leaves it; it answers `path`, logs every call and refuses any other, so a gate that asked for a
// rebase or a land fails; the gate must run as `exec <ws> --class gate --host --`, at the workspace root, where the job
// names Nx's cache directory in NX_CACHE_DIRECTORY), `direnv` (runs the command it is given) and `bun nx`
// (land-host.fake-nx.ts plays back the calls a scenario scripts: its exact output, exit status and run summary rows). A
// scenario names every nx call the gate may make, in order: a call it did not script fails the gate. node runs the
// publisher, recording what land-publish.ts gave it and refusing when FIX_PUBLISH says so, and the workspace's
// tooling/land-inputs.mts, recording the tasks it was asked about and printing the inputs the fixture keeps in
// inputs.json.

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
const passingRun = (args: readonly string[]): Call => ({ args, exit: 0 });
/** The shipping seam after the Linux cross check: the host production CLI, then cowshed:release-check on its bytes. */
const SHIPPING_PRODUCER = ['run', 'cowshed:cli-native:production', '--outputStyle=static-failures-only'];
const SHIPPING_CHECK = ['run', 'cowshed:release-check', '--outputStyle=static-failures-only'];
const SHIPPING_PRODUCER_PASSED: Call = {
  ...passingRun(SHIPPING_PRODUCER),
  run: [{ taskId: 'cowshed:cli-native:production', hash: 'shipping-producer-hash', status: 0 }],
};
const SHIPPING_CHECK_PASSED: Call = {
  ...passingRun(SHIPPING_CHECK),
  run: [{ taskId: 'cowshed:release-check', hash: 'shipping-check-hash', status: 0 }],
};
const SHIPPING = [SHIPPING_PRODUCER_PASSED, SHIPPING_CHECK_PASSED];
const SHIPPING_NX = SHIPPING.map((call) => call.args.join(' '));

/** The Linux cross check as an Nx run of its own: its run summary names the task the pre-push hook asks Nx for. */
const CROSS = ['run-many', '-t', 'cargo-lint-cross'];
const CROSS_PASSED: Call = {
  ...passingRun(CROSS),
  run: [{ taskId: 'cowshed:cargo-lint-cross', hash: 'cross-hash', status: 0 }],
};
/** A whole gate that passed: its run summary carries the root aggregate's row, the hash a land publishes. */
const GATE_PASSED: Call = {
  ...passingRun(GATE),
  run: [{ taskId: '@smoothbricks/codebase:gate', hash: 'gate-hash', status: 0 }],
};
/** The files Nx hashes for each task and the tasks it waits for: the workspace's one commit writes change.txt. */
const INPUTS = { 'lib:lint': ['change.txt', 'lib/src/index.ts'], 'lib:test': ['lib/src/index.ts'] };
/** The diff-scoped wiring check the gate runs first, against the commit the workspace was rebased onto; `*` is any. */
const WIRING = (base: string) => [
  'run',
  '@smoothbricks/codebase:wiring',
  `--args=--base ${base}`,
  '--outputStyle=static-failures-only',
];

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
printf '%s\\n' "$@" >>"$FIX_STATE/publish.args"
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
esac
exec "$FIX_BUN" "$@"
`,
  cowshed: `#!/bin/sh
# Every call is logged as its subcommand and first argument: the gate's route is cowshed's, so a gate asks for no
# rebase and no land, and anything but 'path' and 'exec' is refused.
printf '%s %s\\n' "$1" "$2" >>"$FIX_STATE/cowshed.log"
cmd=$1
shift
case $cmd in
path) if [ "$1" = main ]; then echo "$FIX_MAIN"; else echo "$FIX_WS"; fi ;;
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
// A commit's detached maintenance can remove objects/maintenance.lock while the next local clone copies it.
// These ephemeral repositories own no background Git work, matching the native Git fixtures.
const GIT_FIXTURE_ENV: Record<string, string> = {
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_CONFIG_SYSTEM: '/dev/null',
  GIT_TERMINAL_PROMPT: '0',
  GIT_OPTIONAL_LOCKS: '0',
  GIT_CONFIG_COUNT: '3',
  GIT_CONFIG_KEY_0: 'core.fsync',
  GIT_CONFIG_VALUE_0: 'none',
  GIT_CONFIG_KEY_1: 'gc.auto',
  GIT_CONFIG_VALUE_1: '0',
  GIT_CONFIG_KEY_2: 'maintenance.auto',
  GIT_CONFIG_VALUE_2: 'false',
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

interface GateRun {
  readonly status: number;
  /** What the script said on stderr. */
  readonly log: string;
  /** The nx calls it made, in order. */
  readonly nx: readonly string[];
  /** The cowshed calls it made, in order, each as its subcommand and first argument. */
  readonly cowshed: readonly string[];
  /** Arguments received by the planted unavailable replay tool, when this fixture uses it. */
  readonly replayArgs?: readonly string[];
}

interface GateFixture {
  readonly main: string;
  readonly ws: string;
  /** Where the fake tools record what they were given: the publisher's program and arguments. */
  readonly state: string;
  /** Runs tooling/land-host.sh with `args` (`--gate ws` unless given) over the nx calls `plan` scripts. */
  gate(plan: readonly Call[], args?: readonly string[]): GateRun;
}

interface FixtureOptions {
  /** The bounded replay tool fails. */
  readonly replayFails?: boolean;
  /** The publisher refuses. */
  readonly publishFails?: boolean;
  /** The Linux cross check exits 5. */
  readonly linuxFails?: boolean;
  /** The Linux cross check is an Nx run of its own, which the scenario scripts. */
  readonly crossRuns?: boolean;
}

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) {
    rmSync(root, { recursive: true, force: true });
  }
});

/** A main repository carrying the gate tooling, and a workspace one commit ahead of it, as cowshed's rebase leaves it. */
function fixture({
  replayFails = false,
  publishFails = false,
  linuxFails = false,
  crossRuns = false,
}: FixtureOptions = {}): GateFixture {
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
    'land-publish.ts',
    'land-cargo-env.ts',
  ]) {
    cpSync(join(tooling, file), join(main, 'tooling', file));
  }
  symlinkSync(join(repo, 'node_modules'), join(main, 'node_modules'));
  // The publisher reads the real bridge from the checkout it lands, at the path BRIDGE names.
  mkdirSync(join(main, BRIDGE), { recursive: true });
  for (const file of ['cache_adapter.js', 'nx_publish.js']) {
    cpSync(join(repo, BRIDGE, file), join(main, BRIDGE, file));
  }
  // The failure reporter is main's native build of the failure-summary package (tooling:test depends on its build).
  mkdirSync(join(main, 'packages'), { recursive: true });
  symlinkSync(join(repo, 'packages/failure-summary'), join(main, 'packages/failure-summary'));
  writeFileSync(join(main, 'nx.json'), '{}\n');
  // The gate cross-lints the gated tree for Linux after the gate; the fixture's check passes unless it is told to fail.
  const linux = linuxFails
    ? 'echo linux cross check failed; exit 5'
    : crossRuns
      ? 'bun nx run-many -t cargo-lint-cross'
      : 'echo linux cross check passed';
  writeFileSync(join(main, 'package.json'), `${JSON.stringify({ scripts: { 'check:linux': linux } })}\n`);
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
    gate(plan, args = ['--gate', 'ws']) {
      writeFileSync(join(state, 'plan.json'), JSON.stringify(plan));
      const run = spawnSync('sh', [join(main, 'tooling/land-host.sh'), ...args], {
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
          // The gate's temporary files, so a case can prove none is left behind.
          TMPDIR: join(state, 'tmp'),
        },
      });
      // The production failure log outlives the script; this fixture owns and removes its retained logs.
      for (const match of run.stderr.matchAll(/complete (?:gate )?output: (.+)/g)) {
        if (match[1] !== undefined) roots.push(match[1]);
      }
      const nxLog = join(state, 'nx.log');
      const replayLog = join(state, 'replay.args');
      const cowshedLog = join(state, 'cowshed.log');
      return {
        status: run.status ?? -1,
        log: run.stderr,
        nx: existsSync(nxLog) ? readFileSync(nxLog, 'utf8').trimEnd().split('\n') : [],
        replayArgs: existsSync(replayLog) ? readFileSync(replayLog, 'utf8').trimEnd().split('\n') : undefined,
        cowshed: existsSync(cowshedLog) ? readFileSync(cowshedLog, 'utf8').trimEnd().split('\n') : [],
      };
    },
  };
}

describe('land-host.sh --gate', () => {
  it('gates once, replays exactly its final 64 KiB, and moves neither main nor the workspace', () => {
    const gate = fixture();
    const base = git(gate.main, 'rev-parse', 'HEAD');
    const gated = git(gate.ws, 'rev-parse', 'HEAD');
    // The gate stops the daemon, checks wiring, gates once without bailing, captures directly to one file and replays
    // exactly its final 64 KiB. The rebase before it and the fast-forward after it are cowshed's route, never this
    // script's: its cowshed calls are paths and gate jobs, and the stand-in refuses any other.
    const suffix = '\ncapture-suffix\n';
    const run = gate.gate([
      DAEMON_STOP,
      passingRun(WIRING(base)),
      {
        ...passingRun(GATE),
        requireFileOutput: true,
        output: `capture-prefix\n${'x'.repeat(70_000)}${suffix.slice(0, -1)}`,
      },
      ...SHIPPING,
    ]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain('gating');
    expect(run.nx).toEqual(['daemon --stop', WIRING(base).join(' '), GATE.join(' '), ...SHIPPING_NX]);
    expect(new Set(run.cowshed.map((call) => call.split(' ')[0]))).toEqual(new Set(['path', 'exec']));
    expect(run.log).not.toContain('capture-prefix');
    expect(run.log).toContain('capture-suffix');
    expect(run.log.match(/x{1000,}/)?.[0]?.length).toBe(65_536 - suffix.length);
    expect(git(gate.main, 'rev-parse', 'HEAD')).toBe(base);
    expect(git(gate.ws, 'rev-parse', 'HEAD')).toBe(gated);
    expect(existsSync(join(gate.ws, 'agent-todo/land-ledger.md'))).toBe(false);
  });

  it('names the commit the workspace was rebased onto, not the main that gained a commit since', () => {
    const gate = fixture();
    const base = git(gate.main, 'rev-parse', 'HEAD');
    // A doc commit reaches main after the rebase: the fast-forward will refuse it, and cowshed rebases and gates again.
    writeFileSync(join(gate.main, 'doc.md'), 'doc\n');
    git(gate.main, 'add', 'doc.md');
    git(gate.main, 'commit', '-q', '-m', 'docs: a doc commit lands');
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING(base)), passingRun(GATE), ...SHIPPING]);
    expect(run.status, run.log).toBe(0);
    expect(run.nx).toEqual(['daemon --stop', WIRING(base).join(' '), GATE.join(' '), ...SHIPPING_NX]);
  });

  it('names the build-script environment the shed recorded and the gate shell does not give, and passes anyway', () => {
    const gate = fixture();
    const base = git(gate.main, 'rev-parse', 'HEAD');
    const fingerprint = join(gate.ws, 'target/debug/build/ring/0123456789abcdef/fingerprint');
    mkdirSync(fingerprint, { recursive: true });
    writeFileSync(
      join(fingerprint, 'run-build-script-build-script-build.json'),
      JSON.stringify({ deps: [], local: [{ RerunIfEnvChanged: { var: 'SMOO_LAND_PROBE', val: '/usr/bin/clang' } }] }),
    );
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING(base)), passingRun(GATE), ...SHIPPING]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain(
      'cargo-build-script-env: SMOO_LAND_PROBE: 1 build-script run(s) of ring recorded "/usr/bin/clang"; Cargo now sees unset',
    );
    expect(run.nx).toEqual(['daemon --stop', WIRING(base).join(' '), GATE.join(' '), ...SHIPPING_NX]);
  });

  // Which diagnostics the summary keeps is the failure-summary package's; this gate proves what the script does with them.
  it('exits with a failed gate status, reporting its complete captured failure once, without rerunning or committing a ledger', () => {
    const gate = fixture();
    const before = git(gate.main, 'rev-parse', 'HEAD');
    const gated = git(gate.ws, 'rev-parse', 'HEAD');
    const message =
      'error: expect(received).toEqual(expected)\n\nExpected: []\nReceived: ["orphan"]\n    at fixture.test.ts:206:43';
    const failed = failedRun(message, 7);
    // The summary precedes more than the 64 KiB replay suffix of the captured output.
    const run = gate.gate([
      DAEMON_STOP,
      passingRun(WIRING(before)),
      { ...failed, requireFileOutput: true, output: `${failed.output}\n${'x'.repeat(70_000)}\ncapture-suffix` },
    ]);
    expect(run.status, run.log).toBe(7);
    expect(run.log).toContain(`failed ${TASK}\n${message}`);
    expect(run.log.split(message)).toHaveLength(2);
    expect(run.log).toContain('gate exited 7; the land stops');
    // This gate left no run summary, so no failed task is read and no commit named; the verdict stands.
    expect(run.log).toContain('could not read which tasks the gate failed');
    expect(run.nx).toEqual(['daemon --stop', WIRING(before).join(' '), GATE.join(' ')]);
    expect(git(gate.main, 'rev-parse', 'HEAD')).toBe(before);
    expect(git(gate.ws, 'rev-parse', 'HEAD')).toBe(gated);
    expect(existsSync(join(gate.ws, 'agent-todo/land-ledger.md'))).toBe(false);
    const output = /complete output: (.+)/.exec(run.log)?.[1];
    expect(output).toBeDefined();
    if (output !== undefined) expect(readFileSync(output, 'utf8')).toContain(message);
  });

  it('fails when bounded replay fails after a successful gate', () => {
    const gate = fixture({ replayFails: true });
    const before = git(gate.main, 'rev-parse', 'HEAD');
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING(before)), { ...passingRun(GATE), requireFileOutput: true }]);
    expect(run.status, run.log).toBe(1);
    expect(run.log).toContain('could not replay the gate output');
    expect(run.replayArgs?.slice(0, -1)).toEqual(['--kill-after=1s', '10s', 'tail', '-c', '65536']);
    expect(run.nx).toEqual(['daemon --stop', WIRING(before).join(' '), GATE.join(' ')]);
  });

  it('stops before the gate when the wiring check refuses what the rebased tree adds', () => {
    const gate = fixture();
    const before = git(gate.main, 'rev-parse', 'HEAD');
    const refusal = 'error[WIRING] packages/w/src/lone.ts:1 export lone: no non-test module imports it';
    const run = gate.gate([DAEMON_STOP, { args: WIRING(before), exit: 1, output: refusal }]);
    expect(run.status, run.log).toBe(1);
    expect(run.nx).toEqual(['daemon --stop', WIRING(before).join(' ')]);
    expect(run.log).toContain(refusal);
    expect(run.log).toContain('the wiring check exited 1; the land stops');
  });

  it('stops if the daemon cannot be stopped, without starting the gate', () => {
    const gate = fixture();
    const run = gate.gate([{ args: ['daemon', '--stop'], exit: 1, output: 'daemon stop refused' }]);
    expect(run.status, run.log).toBe(1);
    expect(run.nx).toEqual(['daemon --stop']);
    expect(run.log).toContain('could not stop');
  });

  it('keeps the failing gate status and raw diagnostics when the reporter is unavailable', () => {
    const gate = fixture();
    rmSync(join(gate.main, 'packages/failure-summary'));
    const failed = failedRun('error: original failure', 9);
    const run = gate.gate([
      DAEMON_STOP,
      passingRun(WIRING('*')),
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
  });

  it.each([
    { phase: 'host production producer', calls: [{ args: SHIPPING_PRODUCER, exit: 17 }], status: 17 },
    { phase: 'shipping guard', calls: [SHIPPING_PRODUCER_PASSED, { args: SHIPPING_CHECK, exit: 19 }], status: 19 },
  ])('stops before publishing when the $phase fails', ({ calls, status }) => {
    const gate = fixture({ crossRuns: true });
    const base = git(gate.main, 'rev-parse', 'HEAD');
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING(base)), GATE_PASSED, CROSS_PASSED, ...calls]);
    expect(run.status, run.log).toBe(status);
    expect(run.log).toContain('the land stops');
    expect(run.nx).toEqual([
      'daemon --stop',
      WIRING(base).join(' '),
      GATE.join(' '),
      CROSS.join(' '),
      ...calls.map((call) => call.args.join(' ')),
    ]);
    expect(existsSync(join(gate.state, 'publish.args'))).toBe(false);
    expect(readdirSync(join(gate.state, 'tmp')).filter((name) => name.startsWith('land-'))).toEqual([]);
  });

  it('exits with the Linux cross check status when it fails after a passing gate, and publishes nothing', () => {
    const gate = fixture({ linuxFails: true });
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING('*')), GATE_PASSED]);
    expect(run.status, run.log).toBe(5);
    expect(existsSync(join(gate.state, 'publish.args'))).toBe(false);
  });

  it('publishes the gate task results once the Linux cross check passes', () => {
    const gate = fixture();
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING('*')), GATE_PASSED, ...SHIPPING]);
    expect(run.status, run.log).toBe(0);
    // The publisher gets the cowshed bridge as one program, then the checkout and the gate's succeeded task hashes.
    const bridge = join(repo, BRIDGE);
    const program = `${readFileSync(join(bridge, 'cache_adapter.js'), 'utf8')}\n${readFileSync(join(bridge, 'nx_publish.js'), 'utf8')}`;
    expect(readFileSync(join(gate.state, 'publish.program'), 'utf8')).toBe(program);
    expect(readFileSync(join(gate.state, 'publish.args'), 'utf8')).toBe(`-\n${gate.ws}\n["gate-hash"]\n`);
  });

  it("publishes the Linux cross check's own task results beside the gate's, so main finds cargo-lint-cross cached", () => {
    const gate = fixture({ crossRuns: true });
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING('*')), GATE_PASSED, CROSS_PASSED, ...SHIPPING]);
    expect(run.status, run.log).toBe(0);
    expect(run.nx.slice(-3)).toEqual([CROSS.join(' '), ...SHIPPING_NX]);
    // One publisher run per list: the gate's hashes, then the cross check's.
    expect(readFileSync(join(gate.state, 'publish.args'), 'utf8')).toBe(
      `-\n${gate.ws}\n["gate-hash"]\n-\n${gate.ws}\n["cross-hash"]\n`,
    );
  });

  it('passes when the publish is refused, and says so', () => {
    const gate = fixture({ publishFails: true });
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING('*')), GATE_PASSED, ...SHIPPING]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain('the publish was refused');
    expect(run.log).toContain('failed (status 1); the land goes on');
  });

  it('passes without publishing when the gate passes without a readable run summary, and leaves no temporary file', () => {
    const gate = fixture();
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING('*')), passingRun(GATE), ...SHIPPING]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain("could not read the gate's task hashes");
    expect(run.log).toContain('the land goes on without publishing: the cache never blocks a land');
    expect(existsSync(join(gate.state, 'publish.args'))).toBe(false);
    expect(readdirSync(join(gate.state, 'tmp')).filter((name) => name.startsWith('land-'))).toEqual([]);
  });

  it('names, for each task a red gate failed, the commits of the land that change its inputs, and keeps the verdict', () => {
    const gate = fixture();
    const change = git(gate.ws, 'rev-parse', 'HEAD');
    const before = git(gate.main, 'rev-parse', 'HEAD');
    const run = gate.gate([
      DAEMON_STOP,
      passingRun(WIRING(before)),
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
      `land-host: lib:lint failed; 1 of the land\'s 1 commits change its inputs: ${change.slice(0, 10)} feat(tooling): the change\n`,
    );
    expect(run.log).toContain(
      "land-host: lib:test failed; none of the land's 1 commits changes a file Nx hashes for it or for a task it waits for (a commit that only deletes files is not seen)",
    );
    expect(run.log.indexOf('land-host: lib:lint failed')).toBeLessThan(
      run.log.indexOf('gate exited 3; the land stops'),
    );
    // Nx is asked for the input files of the failed tasks alone, by the workspace's own land-inputs.mts.
    expect(readFileSync(join(gate.state, 'inputs.args'), 'utf8')).toBe(
      `${join(gate.ws, 'tooling/land-inputs.mts')}\ngate\nlib:lint\nlib:test\n`,
    );
  });

  it('publishes nothing from a run summary an earlier invocation wrote, and passes', () => {
    const gate = fixture();
    mkdirSync(join(gate.ws, '.nx/cache'), { recursive: true });
    writeFileSync(
      join(gate.ws, '.nx/cache/run.json'),
      JSON.stringify({
        run: { command: 'nx run-many -t gate', startTime: '2026-01-01T00:00:00.000Z', endTime: '', inner: false },
        tasks: [{ taskId: '@smoothbricks/codebase:gate', hash: 'stale-hash', status: 0 }],
      }),
    );
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING('*')), passingRun(GATE), ...SHIPPING]);
    expect(run.status, run.log).toBe(0);
    expect(run.log).toContain(
      'the run summary is of an invocation that began at 2026-01-01T00:00:00.000Z, before this gate',
    );
    expect(run.log).toContain('the land goes on without publishing: the cache never blocks a land');
    expect(existsSync(join(gate.state, 'publish.args'))).toBe(false);
  });

  it('names no commit from a run summary an earlier invocation wrote', () => {
    const gate = fixture();
    mkdirSync(join(gate.ws, '.nx/cache'), { recursive: true });
    writeFileSync(
      join(gate.ws, '.nx/cache/run.json'),
      JSON.stringify({
        run: { command: 'nx run-many -t gate', startTime: '2026-01-01T00:00:00.000Z', endTime: '', inner: false },
        tasks: [{ taskId: 'lib:lint', hash: 'lib-lint', status: 1 }],
      }),
    );
    const run = gate.gate([DAEMON_STOP, passingRun(WIRING('*')), failedRun('error: no summary written')]);
    expect(run.status, run.log).toBe(1);
    expect(run.log).toContain(
      'the run summary is of an invocation that began at 2026-01-01T00:00:00.000Z, before this gate',
    );
    expect(run.log).toContain('could not read which tasks the gate failed');
    expect(run.log).not.toContain('land-host: lib:lint failed');
  });

  it('refuses main itself, and any call that is not --gate <workspace>', () => {
    const gate = fixture();
    const main = gate.gate([], ['--gate', 'main']);
    expect(main.status, main.log).toBe(64);
    expect(main.log).toContain('main is not a workspace to land');
    for (const args of [[], ['ws'], ['--gate'], ['--gate', 'ws', 'extra']]) {
      const run = gate.gate([], args);
      expect(run.status, run.log).toBe(64);
      expect(run.log).toContain('usage: land-host.sh --gate <workspace>');
    }
    expect(main.nx).toEqual([]);
  });
});
