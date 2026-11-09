import { afterEach, describe, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  cliTarget,
  cowshedChanged,
  installReadiness,
  isCommit,
  landStatusLine,
  promptLine,
  readinessLine,
  rerunCommand,
  setupCommand,
  setupOutcome,
  setupPrompt,
  stageDirectory,
  staleStages,
  type Wanted,
} from './land-install.ts';

// The decisions of the post-land install are plain calls, one table each. land-install.sh then runs for real over a
// real git repository for main, with stand-ins on PATH because they are this machine's tooling, the build and the
// cowshed it drives: 'uname' (the host), 'bun nx' (the Nx calls: the affected set in main, the cache server bundle and
// the production build in the shed, which write the staged binary below), 'cowshed' (fork, path, exec, rebase, lock
// and the installed gateway status over a directory of sheds, each logged in cowshed.log, and land-turn, which records
// a land that has to wait for a turn another holds) and the production CLI itself, whose doctor, gateway status and
// setup answer what a scenario sets. The install holds no land turn, so a land that asks for its turn while the build
// runs takes it at once.

const HEAD = 'a1'.repeat(20);

describe('cliTarget', () => {
  it('names the release CLI target and dist/bin directory of a macOS host by architecture', () => {
    expect(cliTarget('Darwin', 'arm64')).toEqual({ target: 'cli-record-arm64-macos', platform: 'darwin-arm64' });
    expect(cliTarget('Darwin', 'x86_64')).toEqual({ target: 'cli-record-x64-macos', platform: 'darwin-x64' });
  });

  it('has none for another architecture or another system', () => {
    expect(cliTarget('Darwin', 'riscv64')).toBeUndefined();
    expect(cliTarget('Linux', 'x86_64')).toBeUndefined();
    expect(cliTarget('Linux', 'arm64')).toBeUndefined();
  });
});

describe('cowshedChanged', () => {
  const cases: readonly (readonly [string, string, string, Wanted])[] = [
    ['cowshed itself is affected', '["cowshed","tooling"]', 'packages/cowshed/crates/a.rs', 'yes'],
    [
      'a dependency changed, so cowshed is affected',
      '["nx-cache-server","cowshed"]',
      'packages/nx-cache-server/a.ts',
      'yes',
    ],
    ['cowshed is not affected', '["tooling","@smoothbricks/codebase"]', 'tooling/land-host.sh', 'no'],
    ['an empty affected set', '[]', '', 'no'],
    [
      'the root Cargo.lock, which Nx attributes to the root project alone',
      '["@smoothbricks/codebase"]',
      'Cargo.lock\ntooling/a.ts',
      'yes',
    ],
    ['a Cargo.lock of a project is not the root one', '["tooling"]', 'packages/x/Cargo.lock', 'no'],
    ['no output from nx', '', 'tooling/a.ts', 'unknown'],
    ['output that is not JSON', 'NX Failed to process the project graph', 'tooling/a.ts', 'unknown'],
    ['JSON that is not a list', '{"cowshed":true}', 'tooling/a.ts', 'unknown'],
    ['a list of other things', '[1,2]', 'tooling/a.ts', 'unknown'],
  ];
  for (const [name, affected, changed, expected] of cases) {
    it(name, () => {
      expect(cowshedChanged(affected, changed)).toBe(expected);
    });
  }
});

describe('stage names', () => {
  it('accepts only a full commit id', () => {
    expect(isCommit(HEAD)).toBe(true);
    expect(isCommit(HEAD.slice(0, 39))).toBe(false);
    expect(isCommit('A1'.repeat(20))).toBe(false);
    expect(isCommit(`${HEAD}.partial`)).toBe(false);
    expect(isCommit('../..')).toBe(false);
  });

  it('keeps a land stage under the repository cache, named for the land, and refuses another name', () => {
    expect(stageDirectory('/repo', HEAD)).toBe(`/repo/.cache/cowshed-release/${HEAD}`);
    expect(stageDirectory('/repo', '../escape')).toBeUndefined();
  });

  it('names the stages past the newest ones, never an entry that is not a stage', () => {
    const newest = ['1', '2', '3', '4', '5'].map((digit) => digit.repeat(40));
    const listing = [newest[0], 'notes', newest[1], `${newest[2]}.partial`, newest[2], newest[3], newest[4]];
    expect(
      staleStages(
        listing.filter((name): name is string => name !== undefined),
        3,
      ),
    ).toEqual([newest[3], newest[4]]);
    expect(staleStages(newest.slice(0, 2), 3)).toEqual([]);
  });

  it("names the setup command run in main's devenv, quoting only what a shell needs", () => {
    expect(setupCommand('/repo', '/repo/.cache/cowshed-release/a/dist/bin/darwin-arm64/cowshed')).toBe(
      'direnv exec /repo /repo/.cache/cowshed-release/a/dist/bin/darwin-arm64/cowshed setup',
    );
    expect(setupCommand('/my repo', "/my repo/it's/cowshed")).toBe(
      "direnv exec '/my repo' '/my repo/it'\\''s/cowshed' setup",
    );
  });
});

describe('rerunCommand', () => {
  const before = 'b'.repeat(40);
  const cases: readonly (readonly [string, string, string])[] = [
    [
      "runs the install script in main's devenv for the two commits",
      '/repo',
      `direnv exec /repo sh /repo/tooling/land-install.sh ${before} ${HEAD}`,
    ],
    [
      'quotes a checkout path a shell would split',
      '/my repo',
      `direnv exec '/my repo' sh '/my repo/tooling/land-install.sh' ${before} ${HEAD}`,
    ],
    [
      'quotes a quote in the path',
      "/it's",
      `direnv exec '/it'\\''s' sh '/it'\\''s/tooling/land-install.sh' ${before} ${HEAD}`,
    ],
  ];
  for (const [name, main, expected] of cases) {
    it(name, () => {
      expect(rerunCommand(main, before, HEAD)).toBe(expected);
    });
  }
});

interface Finding {
  readonly code: string;
  readonly severity: string;
  readonly message: string;
  readonly hint: string;
}

function finding(code: string, severity: string, hint = ''): Finding {
  return { code, severity, message: `${code} message`, hint };
}

function doctor(...findings: readonly Finding[]): string {
  const healthy = !findings.some((entry) => entry.severity === 'error');
  return JSON.stringify({ ok: true, result: { healthy, findings } });
}

describe('setupPrompt', () => {
  it('needs no prompt on a host whose only errors are not storage work', () => {
    const report = doctor(
      finding('host-volume', 'info'),
      finding('gateway', 'info'),
      finding('gateway-stale-binary', 'warning', 'cowshed setup'),
      finding('project-checks-skipped', 'error'),
    );
    expect(setupPrompt(report)).toEqual({ kind: 'none' });
  });

  it('needs a prompt when the storage LaunchDaemon needs repair, as this host reported it', () => {
    const report = doctor(
      finding('host-volume', 'info'),
      finding('host-mount-service', 'error', 'cowshed setup'),
      finding('project-checks-skipped', 'error'),
    );
    expect(setupPrompt(report)).toEqual({
      kind: 'needed',
      reasons: ['host-mount-service: host-mount-service message'],
    });
  });

  const storage = [
    'host-storage',
    'host-volume-absent',
    'mount',
    'marker',
    'host-mount-root',
    'host-filevault',
    'host-fstab',
    'mount-stubs',
    'host-mount-service',
  ];
  for (const code of storage) {
    it(`needs a prompt for the storage finding ${code}`, () => {
      expect(setupPrompt(doctor(finding(code, 'error')))).toEqual({
        kind: 'needed',
        reasons: [`${code}: ${code} message`],
      });
    });
  }

  it('names every reason, and catches a storage error of a code it does not know by its remedy', () => {
    const report = doctor(finding('host-fstab', 'error'), finding('host-future-work', 'error', 'cowshed setup'));
    expect(setupPrompt(report)).toEqual({
      kind: 'needed',
      reasons: ['host-fstab: host-fstab message', 'host-future-work: host-future-work message'],
    });
  });

  it('does not count a storage finding below error severity', () => {
    expect(setupPrompt(doctor(finding('mount', 'warning'), finding('host-volume', 'info')))).toEqual({ kind: 'none' });
  });

  const unreadable: readonly (readonly [string, string])[] = [
    ['empty output', ''],
    ['output that is not JSON', 'cowshed: doctor crashed'],
    ['an error envelope', JSON.stringify({ ok: false, error: { code: 'environment-missing', message: 'no gateway' } })],
    ['an envelope without a result', JSON.stringify({ ok: true })],
    ['a result without findings', JSON.stringify({ ok: true, result: { healthy: true } })],
    [
      'a finding without a code',
      JSON.stringify({ ok: true, result: { findings: [{ severity: 'error', message: 'm' }] } }),
    ],
  ];
  for (const [name, report] of unreadable) {
    it(`is unknown for ${name}, never a clean host`, () => {
      expect(setupPrompt(report).kind).toBe('unknown');
    });
  }

  it('prints the verdict word first, then its detail', () => {
    expect(promptLine({ kind: 'none' })).toBe('none');
    expect(promptLine({ kind: 'needed', reasons: ['a: one', 'b: two'] })).toBe('needed a: one; b: two');
    expect(promptLine({ kind: 'unknown', why: 'no report' })).toBe('unknown no report');
  });
});

function status(fields: Record<string, unknown>): string {
  return JSON.stringify({
    ok: true,
    result: { installed: true, running: true, socket: '/s', cliVersion: '0.1.0', activeWorkspaces: 0, ...fields },
  });
}

const ticket = (number: number, workspace: string, pid: number) => ({
  number,
  pid,
  workspace,
  askedUnixMs: 1,
  command: 'land',
});

describe('installReadiness', () => {
  it('is ready when nothing holds a land turn', () => {
    expect(installReadiness(status({}))).toEqual({ kind: 'ready' });
  });

  it('is ready when no daemon answers: an install that restarts nothing checks nothing', () => {
    const turns = [{ repo: 'gadgets', tickets: [ticket(1, 'ws', 5)] }];
    expect(installReadiness(status({ running: false, landTurns: turns }))).toEqual({ kind: 'ready' });
  });

  it('is busy while an install holds the install slot, which setup would wait for with no deadline', () => {
    expect(installReadiness(status({ pendingInstall: true }))).toEqual({
      kind: 'busy',
      blockers: ['another install holds the install slot'],
    });
  });

  it('is busy for a held install slot even when no daemon answers', () => {
    expect(installReadiness(status({ running: false, pendingInstall: true }))).toEqual({
      kind: 'busy',
      blockers: ['another install holds the install slot'],
    });
  });

  it('names the install slot beside the work of a running gateway', () => {
    const turns = [{ repo: 'gadgets', tickets: [ticket(4, 'gadget-land', 99)] }];
    expect(installReadiness(status({ pendingInstall: true, landTurns: turns }))).toEqual({
      kind: 'busy',
      blockers: [
        'another install holds the install slot',
        'the land of gadget-land holds the gadgets land turn (ticket 4, pid 99)',
      ],
    });
  });
  it('names the holder of each repository land turn and not a land queued behind it', () => {
    const turns = [
      { repo: 'gadgets', tickets: [ticket(4, 'gadget-land', 99), ticket(5, 'queued', 100)] },
      { repo: 'smoothbricks', tickets: [], abandoned: [] },
      { repo: 'widgets', tickets: [ticket(2, 'wg', 7)] },
    ];
    expect(installReadiness(status({ landTurns: turns }))).toEqual({
      kind: 'busy',
      blockers: [
        'the land of gadget-land holds the gadgets land turn (ticket 4, pid 99)',
        'the land of wg holds the widgets land turn (ticket 2, pid 7)',
      ],
    });
  });

  it('refuses to guess when the land turns could not be read', () => {
    expect(installReadiness(status({ landTurnsError: 'permission denied' }))).toEqual({
      kind: 'busy',
      blockers: ['land turns unreadable: permission denied'],
    });
  });

  const unreadable: readonly (readonly [string, string])[] = [
    ['empty output', ''],
    ['an error envelope', JSON.stringify({ ok: false, error: { code: 'internal', message: 'boom' } })],
    ['a status without a running state', JSON.stringify({ ok: true, result: { installed: true } })],
    ['a land queue without tickets', status({ landTurns: [{ repo: 'gadgets' }] })],
    ['a ticket without a pid', status({ landTurns: [{ repo: 'gadgets', tickets: [{ number: 1, workspace: 'w' }] }] })],
    ['a pending install that is not a boolean', status({ pendingInstall: 'yes' })],
  ];
  for (const [name, text] of unreadable) {
    it(`is unknown for ${name}, never ready`, () => {
      expect(installReadiness(text).kind).toBe('unknown');
    });
  }

  it('prints the verdict word first, then its detail', () => {
    expect(readinessLine({ kind: 'ready' })).toBe('ready');
    expect(readinessLine({ kind: 'busy', blockers: ['gate job 3 at /w', 'gate job 4 at /x'] })).toBe(
      'busy gate job 3 at /w; gate job 4 at /x',
    );
    expect(readinessLine({ kind: 'unknown', why: 'no status' })).toBe('unknown no status');
  });
});

describe('setupOutcome', () => {
  it('is installed by exit status 0 alone', () => {
    expect(setupOutcome(0, '')).toEqual({ kind: 'installed' });
    expect(setupOutcome(0, 'not json')).toEqual({ kind: 'installed' });
  });

  const typedErrors = [
    {
      name: 'a refusal when the typed error is a conflict',
      status: 4,
      error: { code: 'conflict', message: 'the install would restart the gateway', hint: 'rerun' },
      kind: 'refused',
    },
    {
      name: 'a failure for any other typed error, such as a dismissed dialog',
      status: 6,
      error: { code: 'sandbox-denied', message: 'administrator authorization was declined', hint: 'cowshed setup' },
      kind: 'failed',
    },
  ] as const;
  for (const { name, status, error, kind } of typedErrors) {
    it(`is ${name}`, () => {
      expect(setupOutcome(status, JSON.stringify({ ok: false, error }))).toEqual({
        kind,
        why: `setup exited ${status}: ${error.code}: ${error.message}`,
      });
    });
  }

  it('says so when a failure printed no error envelope', () => {
    expect(setupOutcome(70, '')).toEqual({ kind: 'failed', why: 'setup exited 70 without an error envelope' });
    expect(setupOutcome(6, JSON.stringify({ ok: false }))).toEqual({
      kind: 'failed',
      why: 'setup exited 6 without an error envelope',
    });
  });
});

describe('landStatusLine', () => {
  const when = '2026-10-25T20:00:00Z';
  const command = `/repo/.cache/cowshed-release/${HEAD}/dist/bin/darwin-arm64/cowshed setup`;
  const cases: readonly (readonly [string, Parameters<typeof landStatusLine>[2], string])[] = [
    ['installed', { kind: 'installed' }, 'installed'],
    ['skipped', { kind: 'skipped', why: 'no staged build' }, 'skipped: no staged build'],
    ['failed', { kind: 'failed', why: 'setup exited 4: conflict: x' }, 'failed: setup exited 4: conflict: x'],
    [
      'attended, with the exact command to run',
      { kind: 'attended', why: 'host-mount-service: repair', command },
      `attended: host-mount-service: repair; run ${command}`,
    ],
    [
      'unverified, with the exact command to run',
      { kind: 'unverified', why: 'no report', command },
      `unverified: no report; run ${command}`,
    ],
    [
      'deferred, with the exact command to rerun',
      { kind: 'deferred', why: 'gate job 3 at /w; gate job 4 at /x', command },
      `deferred: gate job 3 at /w; gate job 4 at /x; rerun ${command}`,
    ],
  ];
  for (const [name, outcome, text] of cases) {
    it(`writes ${name} as one line`, () => {
      expect(landStatusLine(when, HEAD, outcome)).toBe(`${when} ${HEAD} install ${text}`);
    });
  }

  it('keeps a multi-line reason on one line', () => {
    const line = landStatusLine(when, HEAD, { kind: 'failed', why: 'first\n  second\tthird\n' });
    expect(line).toBe(`${when} ${HEAD} install failed: first second third`);
  });
});

// ---- land-install.sh over a real repository ----

const tooling = import.meta.dir;
const bun = process.execPath;

const PATH_STUBS: Record<string, string> = {
  direnv: `#!/bin/sh
[ "$1" = exec ] || exit 64
echo "$*" >>"$FIX_STATE/direnv.log"
shift 2
exec "$@"
`,
  uname: `#!/bin/sh
case $1 in
-s) echo Darwin ;;
-m) echo "$FIX_MACHINE" ;;
*) exit 64 ;;
esac
`,
  bun: `#!/bin/sh
case $1 in
nx) shift; exec "$FIX_NX" "$@" ;;
*) exec "$FIX_BUN" "$@" ;;
esac
`,
  // The installed cowshed. Its sheds are directories of the state directory, cloned from the project main names, and
  // every shed verb logs to cowshed.log. A rebase fetches its revision, refuses a shed with uncommitted work by name,
  // and fast-forwards a shed that stands behind the revision: onto a commit the shed is already past it moves nothing.
  cowshed: `#!/bin/sh
project=
while [ "$#" -gt 0 ]; do
  case $1 in
  --project) project=$2; shift 2 ;;
  -q | --quiet | --json) shift ;;
  *) break ;;
  esac
done
verb=$1
shift
sheds=$FIX_STATE/sheds
case $verb in
land-turn)
  [ "$2" = --workspace ] && [ "$4" = -- ] || exit 64
  echo "$1 $3" >>"$FIX_STATE/turns.log"
  workspace=$3
  shift 4
  # A land that asks while another holds the turn queues behind it: the stub records the wait and runs it at once.
  if [ -f "$FIX_STATE/turn-held" ]; then
    echo "$workspace waited" >>"$FIX_STATE/waits.log"
    exec "$@"
  fi
  echo "$workspace" >"$FIX_STATE/turn-held"
  status=0
  "$@" || status=$?
  rm -f "$FIX_STATE/turn-held"
  exit $status
  ;;
fork)
  [ -n "$project" ] && [ "$2" = --ref ] || exit 64
  echo "fork $1 --ref $3" >>"$FIX_STATE/cowshed.log"
  if [ -f "$FIX_STATE/fork-fails" ]; then
    echo "cowshed: the fork is refused" >&2
    exit 5
  fi
  [ ! -e "$sheds/$1" ] || { echo "cowshed: workspace $1 exists" >&2; exit 4; }
  git clone -q "$project" "$sheds/$1" && git -C "$sheds/$1" checkout -q "$3" || exit 1
  echo "$sheds/$1"
  ;;
path)
  [ -n "$project" ] || exit 64
  echo "path $1" >>"$FIX_STATE/cowshed.log"
  [ -d "$sheds/$1" ] || { echo "cowshed: workspace $1 does not exist" >&2; exit 3; }
  echo "$sheds/$1"
  ;;
exec)
  [ -n "$project" ] && [ "$2" = --host ] && [ "$3" = -- ] || exit 64
  echo "exec $*" >>"$FIX_STATE/cowshed.log"
  workspace=$1
  shift 3
  [ -d "$sheds/$workspace" ] || { echo "cowshed: workspace $workspace does not exist" >&2; exit 3; }
  cd "$sheds/$workspace" && exec "$@"
  ;;
rebase)
  [ -n "$project" ] && [ "$2" = --onto ] || exit 64
  echo "rebase $1 --onto $3" >>"$FIX_STATE/cowshed.log"
  [ -d "$sheds/$1" ] || { echo "cowshed: workspace $1 does not exist" >&2; exit 3; }
  if [ -f "$FIX_STATE/rebase-refuses" ]; then
    echo "cowshed: the replay onto $3 conflicts" >&2
    exit 4
  fi
  named=$(git -C "$sheds/$1" status --porcelain | cut -c4-)
  if [ -n "$named" ]; then
    echo "cowshed: workspace $1 has uncommitted work: $(echo $named)" >&2
    exit 4
  fi
  git -C "$sheds/$1" fetch -q "$project" "$3" || exit 1
  if git -C "$sheds/$1" merge-base --is-ancestor HEAD "$3"; then git -C "$sheds/$1" checkout -q "$3"; fi
  git -C "$sheds/$1" rev-parse HEAD
  ;;
lock)
  [ "$2" = -- ] || exit 64
  lock=$1
  shift 2
  [ -d "$(dirname "$lock")" ] || { echo "cowshed: $(dirname "$lock") does not exist" >&2; exit 1; }
  # The real lock queues a second holder behind the first, and says so; the stub does the same with a directory.
  queued=
  until mkdir "$lock.held" 2>/dev/null; do
    [ -n "$queued" ] || echo "lock $lock queued" >>"$FIX_STATE/cowshed.log"
    queued=1
    sleep 0.05
  done
  echo "lock $lock held" >>"$FIX_STATE/cowshed.log"
  stages=$(dirname "$lock")/cowshed-release
  before=$(ls -A "$stages" 2>/dev/null | wc -l | tr -d ' ')
  status=0
  "$@" || status=$?
  after=$(ls -A "$stages" 2>/dev/null | wc -l | tr -d ' ')
  echo "lock $lock [stages $before -> $after]" >>"$FIX_STATE/cowshed.log"
  rmdir "$lock.held"
  exit $status
  ;;
gateway)
  [ "$1" = status ] || exit 64
  cwd=elsewhere
  [ "$(pwd)" = / ] && cwd=/
  echo "gateway status [cwd $cwd]" >>"$FIX_STATE/cowshed.log"
  if [ -f "$FIX_STATE/installed-status.json" ]; then cat "$FIX_STATE/installed-status.json"; else cat "$FIX_STATE/status.json"; fi
  ;;
*) exit 64 ;;
esac
`,
};

// The Nx calls write into their working directory, so the build is in the shed and the affected set in main.
const NX_STUB = `#!/bin/sh
echo "$* [cwd $(basename "$PWD")]" >>"$FIX_STATE/nx.log"
case $1 in
show) cat "$FIX_STATE/affected.json" ;;
run)
  case $2 in
  nx-cache-server:bundle)
    [ ! -f "$FIX_STATE/bundle-fails" ] || exit 1
    mkdir -p packages/nx-cache-server/dist/server
    echo "server bundle" >packages/nx-cache-server/dist/server/nx-cache-server.js
    ;;
  cowshed:cli-record-*-macos:production)
    [ ! -f "$FIX_STATE/build-fails" ] || exit 1
    # A land that asks for its turn while the build runs.
    [ ! -f "$FIX_STATE/race" ] || cowshed land-turn smoothbricks --workspace racing-land -- true
    # A build that runs until the test says so, which keeps the release lock held while another install asks for it.
    if [ -f "$FIX_STATE/hold-build" ]; then
      while [ ! -f "$FIX_STATE/release" ]; do sleep 0.05; done
    fi
    arch=$(echo "$2" | cut -d- -f3)
    mkdir -p "packages/cowshed/dist/bin/darwin-$arch"
    cp "$FIX_STATE/staged.sh" "packages/cowshed/dist/bin/darwin-$arch/cowshed"
    chmod +x "packages/cowshed/dist/bin/darwin-$arch/cowshed"
    echo "release record" >"packages/cowshed/dist/bin/darwin-$arch/cowshed.build"
    ;;
  *) exit 97 ;;
  esac
  ;;
*) exit 97 ;;
esac
`;

const STAGED_STUB = `#!/bin/sh
cwd=elsewhere
[ "$(pwd)" = / ] && cwd=/
echo "$* [cwd $cwd]" >>"$FIX_STATE/staged.log"
[ "$1" = --json ] && shift
case $1 in
doctor) cat "$FIX_STATE/doctor.json"; exit "$(cat "$FIX_STATE/doctor.exit")" ;;
gateway)
  n=0
  [ ! -f "$FIX_STATE/status.count" ] || n=$(cat "$FIX_STATE/status.count")
  echo $((n + 1)) >"$FIX_STATE/status.count"
  if [ -f "$FIX_STATE/status.$n.json" ]; then cat "$FIX_STATE/status.$n.json"; else cat "$FIX_STATE/status.json"; fi
  ;;
setup) cat "$FIX_STATE/setup.json"; exit "$(cat "$FIX_STATE/setup.exit")" ;;
*) exit 64 ;;
esac
`;

const GIT_ENV: Record<string, string> = {
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_CONFIG_SYSTEM: '/dev/null',
  GIT_TERMINAL_PROMPT: '0',
  GIT_OPTIONAL_LOCKS: '0',
  GIT_CONFIG_COUNT: '1',
  GIT_CONFIG_KEY_0: 'core.fsync',
  GIT_CONFIG_VALUE_0: 'none',
  GIT_AUTHOR_NAME: 'land install test',
  GIT_AUTHOR_EMAIL: 'land@test.invalid',
  GIT_COMMITTER_NAME: 'land install test',
  GIT_COMMITTER_EMAIL: 'land@test.invalid',
};

function git(directory: string, ...args: string[]): string {
  const run = spawnSync('git', ['-C', directory, ...args], { encoding: 'utf8', env: { ...process.env, ...GIT_ENV } });
  if (run.status !== 0) {
    throw new Error(`git ${args.join(' ')} in ${directory}: ${run.stderr || `terminated by ${run.signal}`}`);
  }
  return run.stdout.trim();
}

interface Run {
  readonly status: number;
  /** The land-status lines. */
  readonly stdout: string;
  readonly stderr: string;
}

interface Install {
  readonly main: string;
  readonly before: string;
  readonly head: string;
  /** The stage directory of this land. */
  readonly stage: string;
  /** The name of the stable build shed. */
  readonly shed: string;
  /** The directory holding the sheds the cowshed stand-in forked. */
  readonly sheds: string;
  /** Replaces what a stand-in answers: a file of the state directory. */
  set(name: string, text: string): void;
  /** What a stand-in logged, one entry per call. */
  log(name: string): readonly string[];
  run(options?: { readonly machine?: string; readonly args?: readonly string[] }): Run;
  /** The same run, started and not waited for, so another install can be started while it works. */
  start(options?: { readonly machine?: string; readonly args?: readonly string[] }): Promise<Run>;
}

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) {
    rmSync(root, { recursive: true, force: true });
  }
});

const COWSHED_CHANGE = 'packages/cowshed/crates/cowshed-cli/src/main.rs';

/** A main repository carrying the install tooling, one commit past the commit before it that touched the given files. */
function fixture(changed: readonly string[] = [COWSHED_CHANGE]): Install {
  const root = mkdtempSync(join(tmpdir(), 'land-install-'));
  roots.push(root);
  const main = join(root, 'main');
  const state = join(root, 'state');
  const sheds = join(state, 'sheds');
  const bin = join(root, 'bin');
  mkdirSync(join(main, 'tooling'), { recursive: true });
  mkdirSync(sheds, { recursive: true });
  mkdirSync(bin);
  for (const file of ['land-install.sh', 'land-install.ts']) {
    cpSync(join(tooling, file), join(main, 'tooling', file));
  }
  writeFileSync(join(main, '.gitignore'), '.cache\npackages/*/dist\n');
  mkdirSync(join(main, 'packages/cowshed/nix/mbx'), { recursive: true });
  writeFileSync(join(main, 'packages/cowshed/nix/mbx/flake.nix'), '{ outputs = _: { }; }\n');
  writeFileSync(join(main, 'packages/cowshed/nix/mbx/flake.lock'), '{}\n');
  git(main, 'init', '-q', '-b', 'main');
  git(main, 'add', '.');
  git(main, 'commit', '-q', '-m', 'chore: start');
  const before = git(main, 'rev-parse', 'HEAD');
  for (const file of changed) {
    mkdirSync(dirname(join(main, file)), { recursive: true });
    writeFileSync(join(main, file), 'change\n');
  }
  git(main, 'add', '.');
  git(main, 'commit', '-q', '-m', 'feat(cowshed): the change');
  const head = git(main, 'rev-parse', 'HEAD');
  for (const [name, text] of Object.entries(PATH_STUBS)) {
    writeFileSync(join(bin, name), text);
    chmodSync(join(bin, name), 0o755);
  }
  const nx = join(root, 'nx');
  writeFileSync(nx, NX_STUB);
  chmodSync(nx, 0o755);
  const set = (name: string, text: string): void => writeFileSync(join(state, name), text);
  set('staged.sh', STAGED_STUB);
  set('affected.json', '["cowshed"]');
  set('doctor.json', doctor(finding('host-volume', 'info'), finding('project-checks-skipped', 'error')));
  set('doctor.exit', '5');
  set('status.json', status({}));
  set('setup.json', JSON.stringify({ ok: true, result: {} }));
  set('setup.exit', '0');
  const script = join(main, 'tooling/land-install.sh');
  const environment = (machine: string | undefined): NodeJS.ProcessEnv => ({
    ...process.env,
    ...GIT_ENV,
    PATH: `${bin}:${process.env.PATH}`,
    FIX_BUN: bun,
    FIX_NX: nx,
    FIX_STATE: state,
    FIX_MACHINE: machine ?? 'arm64',
  });
  return {
    main,
    before,
    head,
    stage: join(main, '.cache', 'cowshed-release', head),
    shed: 'land-install',
    sheds,
    set,
    log(name) {
      const path = join(state, name);
      return existsSync(path) ? readFileSync(path, 'utf8').trimEnd().split('\n') : [];
    },
    run(options = {}) {
      const run = spawnSync('sh', [script, ...(options.args ?? [before, head])], {
        cwd: main,
        encoding: 'utf8',
        env: environment(options.machine),
      });
      return { status: run.status ?? -1, stdout: run.stdout, stderr: run.stderr };
    },
    start(options = {}) {
      const { promise, resolve } = Promise.withResolvers<Run>();
      const child = spawn('sh', [script, ...(options.args ?? [before, head])], {
        cwd: main,
        env: environment(options.machine),
      });
      let stdout = '';
      let stderr = '';
      child.stdout.setEncoding('utf8').on('data', (chunk: string) => {
        stdout += chunk;
      });
      child.stderr.setEncoding('utf8').on('data', (chunk: string) => {
        stderr += chunk;
      });
      child.on('close', (code) => resolve({ status: code ?? -1, stdout, stderr }));
      return promise;
    },
  };
}

const NX_SHOW = (install: Install): string =>
  `show projects --affected --base=${install.before} --head=${install.head} --json [cwd main]`;
const NX_BUNDLE = (install: Install): string =>
  `run nx-cache-server:bundle --outputStyle=static-failures-only [cwd ${install.shed}]`;

/** What the install asks the installed cowshed, in the order a build in the shed asks it. */
function cowshedCalls(install: Install) {
  const hostExec = `exec ${install.shed} --host -- direnv exec . bun nx run`;
  const lock = join(install.main, '.cache', 'cowshed-release.lock');
  return {
    status: 'gateway status [cwd /]',
    probe: `path ${install.shed}`,
    fork: `fork ${install.shed} --ref ${install.head}`,
    repoint: (head: string) => `rebase ${install.shed} --onto ${head}`,
    path: `path ${install.shed}`,
    bundle: `${hostExec} nx-cache-server:bundle --outputStyle=static-failures-only`,
    production: `${hostExec} cowshed:cli-record-arm64-macos:production --outputStyle=static-failures-only`,
    held: `lock ${lock} held`,
    queued: `lock ${lock} queued`,
    lock: (stages: string) => `lock ${lock} [stages ${stages}]`,
  };
}

/** Lands one more commit on main and returns it: the head of a later install. */
function landAnother(install: Install): string {
  writeFileSync(join(install.main, 'later.txt'), 'later\n');
  git(install.main, 'add', '.');
  git(install.main, 'commit', '-q', '-m', 'feat(cowshed): a later change');
  return git(install.main, 'rev-parse', 'HEAD');
}

/** The command that reruns this land's install, as a deferral names it. */
const RERUN = (install: Install): string =>
  `direnv exec ${install.main} sh ${install.main}/tooling/land-install.sh ${install.before} ${install.head}`;

/** The one status line a run wrote, split into its timestamp, commit and the outcome text. */
function onlyLine(stdout: string): { readonly timestamp: string; readonly commit: string; readonly text: string } {
  const lines = stdout.split('\n').filter((line) => line !== '');
  expect(lines).toHaveLength(1);
  const match = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ) ([0-9a-f]{40}) install (.*)$/.exec(lines[0] ?? '');
  expect(match).not.toBeNull();
  return { timestamp: match?.[1] ?? '', commit: match?.[2] ?? '', text: match?.[3] ?? '' };
}

const BUSY_STATUS = status({ landTurns: [{ repo: 'gadgets', tickets: [ticket(4, 'gadget-land', 99)] }] });
const BUSY_TEXT = 'the land of gadget-land holds the gadgets land turn (ticket 4, pid 99)';

describe('land-install.sh', () => {
  it('does nothing for a land that changed neither cowshed nor a project it depends on', () => {
    const install = fixture(['tooling/change.ts']);
    install.set('affected.json', '["tooling","@smoothbricks/codebase"]');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(run.stdout).toBe('');
    expect(install.log('nx.log')).toEqual([NX_SHOW(install)]);
    expect(install.log('cowshed.log')).toEqual([]);
    expect(install.log('turns.log')).toEqual([]);
    expect(existsSync(join(install.main, '.cache'))).toBe(false);
  });

  it('the first install creates the shed, builds holding no turn, stages, then runs the staged setup once', () => {
    const install = fixture();
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const line = onlyLine(run.stdout);
    expect(line.commit).toBe(install.head);
    expect(line.text).toBe('installed');
    // No land turn is taken for any of it, so no land waits for it.
    expect(install.log('turns.log')).toEqual([]);
    expect(install.log('waits.log')).toEqual([]);
    // The build is the shed's: main stands still, and the shed stays for the next install.
    expect(install.log('nx.log')).toEqual([
      NX_SHOW(install),
      NX_BUNDLE(install),
      `run cowshed:cli-record-arm64-macos:production --outputStyle=static-failures-only [cwd ${install.shed}]`,
    ]);
    expect(existsSync(join(install.main, 'packages/cowshed/dist'))).toBe(false);
    expect(readdirSync(install.sheds)).toEqual([install.shed]);
    expect(git(join(install.sheds, install.shed), 'rev-parse', 'HEAD')).toBe(install.head);
    // Readiness is read before the build; the stage is assembled inside the lock, which found it empty.
    const calls = cowshedCalls(install);
    expect(install.log('cowshed.log')).toEqual([
      calls.status,
      calls.held,
      calls.probe,
      calls.fork,
      calls.path,
      calls.bundle,
      calls.production,
      calls.lock('0 -> 1'),
    ]);
    expect(install.log('staged.log')).toEqual([
      '--json doctor [cwd /]',
      '--json gateway status [cwd /]',
      '--json setup [cwd elsewhere]',
    ]);
    // The stage is a distribution, so setup finds the Nx cache server where it looks for a package's dependency.
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(statSync(binary).mode & 0o111).not.toBe(0);
    // Setup judges the install by the release record beside the binary, so the stage carries it.
    expect(readFileSync(`${binary}.build`, 'utf8')).toBe('release record\n');
    expect(
      readFileSync(
        join(install.stage, 'node_modules/@smoothbricks/nx-cache-server/dist/server/nx-cache-server.js'),
        'utf8',
      ),
    ).toBe('server bundle\n');
    // And setup finds the mbx flake it pins where a package ships it, above its binary.
    expect(readFileSync(join(install.stage, 'nix/mbx/flake.nix'), 'utf8')).toBe('{ outputs = _: { }; }\n');
    expect(readdirSync(dirname(install.stage))).toEqual([install.head]);
    // Nx and setup run in main's devenv, whatever environment started the script, and the build in the shed's; the
    // doctor and status reads need none.
    expect(install.log('direnv.log')).toEqual([
      `exec ${install.main} bun nx show projects --affected --base=${install.before} --head=${install.head} --json`,
      'exec . bun nx run nx-cache-server:bundle --outputStyle=static-failures-only',
      'exec . bun nx run cowshed:cli-record-arm64-macos:production --outputStyle=static-failures-only',
      `exec ${install.main} ${binary} --json setup`,
    ]);
  });

  it('a land that asks for its turn during the install build takes it at once', () => {
    const install = fixture();
    // The production build asks for the repository's land turn, as a land queued behind main would.
    install.set('race', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(install.log('waits.log')).toEqual([]);
    expect(onlyLine(run.stdout).text).toBe('installed');
    expect(install.log('turns.log')).toEqual(['smoothbricks racing-land']);
  });

  it('builds the x64 CLI on an Intel host', () => {
    const install = fixture();
    const run = install.run({ machine: 'x86_64' });
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('installed');
    expect(install.log('nx.log')[2]).toBe(
      `run cowshed:cli-record-x64-macos:production --outputStyle=static-failures-only [cwd ${install.shed}]`,
    );
    expect(install.log('cowshed.log')).toContain(
      `exec ${install.shed} --host -- direnv exec . bun nx run cowshed:cli-record-x64-macos:production --outputStyle=static-failures-only`,
    );
    expect(existsSync(join(install.stage, 'dist/bin/darwin-x64/cowshed'))).toBe(true);
  });

  it('installs for a land that changed only the root Cargo.lock, which Nx attributes to the root project', () => {
    const install = fixture(['Cargo.lock']);
    install.set('affected.json', '["@smoothbricks/codebase"]');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('installed');
  });

  it('installs when nx cannot say what the land affected, and says so', () => {
    const install = fixture();
    rmSync(join(install.main, '..', 'state', 'affected.json'));
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(run.stderr).toContain(`cannot tell whether ${install.head} changed cowshed`);
    expect(onlyLine(run.stdout).text).toBe('installed');
  });

  it('prints the exact command and runs nothing when setup would need an administrator', () => {
    const install = fixture();
    install.set(
      'doctor.json',
      doctor(
        finding('host-volume', 'info'),
        finding('host-mount-service', 'error', 'cowshed setup'),
        finding('project-checks-skipped', 'error'),
      ),
    );
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(onlyLine(run.stdout).text).toBe(
      `attended: host-mount-service: host-mount-service message; run direnv exec ${install.main} ${binary} setup`,
    );
    expect(install.log('staged.log')).toEqual(['--json doctor [cwd /]']);
  });

  it('defers before building when a land holds its turn, builds nothing and names the command to rerun', () => {
    const install = fixture();
    install.set('installed-status.json', BUSY_STATUS);
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe(`deferred: ${BUSY_TEXT}; rerun ${RERUN(install)}`);
    expect(install.log('cowshed.log')).toEqual([cowshedCalls(install).status]);
    expect(install.log('nx.log')).toEqual([NX_SHOW(install)]);
    expect(install.log('staged.log')).toEqual([]);
    expect(readdirSync(install.sheds)).toEqual([]);
    expect(existsSync(join(install.main, '.cache'))).toBe(false);
  });

  it('defers before building while another install holds the install slot', () => {
    const install = fixture();
    install.set('installed-status.json', status({ pendingInstall: true }));
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe(`deferred: another install holds the install slot; rerun ${RERUN(install)}`);
    expect(install.log('cowshed.log')).toEqual([cowshedCalls(install).status]);
  });

  it('is unverified, and builds nothing, when the installed gateway status cannot be read', () => {
    const install = fixture();
    install.set('installed-status.json', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe(
      `unverified: cowshed gateway status printed no JSON envelope (gateway status exited 0); run ${RERUN(install)}`,
    );
    expect(install.log('cowshed.log')).toEqual([cowshedCalls(install).status]);
    expect(install.log('nx.log')).toEqual([NX_SHOW(install)]);
    expect(install.log('staged.log')).toEqual([]);
  });

  it('defers work that began during the build, with the command to run, and runs no setup', () => {
    const install = fixture();
    // Ready before the build; a land holds its turn by the time setup would run.
    install.set('installed-status.json', status({}));
    install.set('status.json', BUSY_STATUS);
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(onlyLine(run.stdout).text).toBe(`deferred: ${BUSY_TEXT}; rerun direnv exec ${install.main} ${binary} setup`);
    expect(install.log('staged.log')).toEqual(['--json doctor [cwd /]', '--json gateway status [cwd /]']);
  });

  it('runs nothing unattended when doctor cannot be read', () => {
    const install = fixture();
    install.set('doctor.json', 'cowshed: doctor crashed');
    install.set('doctor.exit', '70');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(onlyLine(run.stdout).text).toBe(
      `unverified: cowshed doctor printed no JSON envelope (doctor exited 70); run direnv exec ${install.main} ${binary} setup`,
    );
    expect(install.log('staged.log')).toEqual(['--json doctor [cwd /]']);
  });

  it("runs nothing when the staged build's gateway status cannot be read", () => {
    const install = fixture();
    install.set('installed-status.json', status({}));
    install.set('status.json', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toStartWith(
      'unverified: cowshed gateway status printed no JSON envelope (gateway status exited 0); run ',
    );
    expect(install.log('staged.log')).toHaveLength(2);
  });

  it('records a refusal as failed when no work holds the host any more', () => {
    const install = fixture();
    install.set(
      'setup.json',
      JSON.stringify({
        ok: false,
        error: { code: 'conflict', message: 'the build lacks an installed commit', hint: 'rebase' },
      }),
    );
    install.set('setup.exit', '4');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('failed: setup exited 4: conflict: the build lacks an installed commit');
    expect(install.log('staged.log')).toHaveLength(4);
  });

  it('records a failure that is not a refusal as failed, and reads no second status', () => {
    const install = fixture();
    install.set(
      'setup.json',
      JSON.stringify({
        ok: false,
        error: { code: 'sandbox-denied', message: 'administrator authorization was declined' },
      }),
    );
    install.set('setup.exit', '6');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe(
      'failed: setup exited 6: sandbox-denied: administrator authorization was declined',
    );
    expect(install.log('staged.log')).toHaveLength(3);
  });

  it('defers a refusal that a fresh status explains by work that began after the first read', () => {
    const install = fixture();
    install.set('status.1.json', BUSY_STATUS);
    install.set(
      'setup.json',
      JSON.stringify({
        ok: false,
        error: { code: 'conflict', message: 'this install would restart the gateway', hint: 'rerun' },
      }),
    );
    install.set('setup.exit', '4');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(onlyLine(run.stdout).text).toBe(`deferred: ${BUSY_TEXT}; rerun direnv exec ${install.main} ${binary} setup`);
    expect(install.log('staged.log').map((entry) => entry.split(' ').slice(0, 2).join(' '))).toEqual([
      '--json doctor',
      '--json gateway',
      '--json setup',
      '--json gateway',
    ]);
  });

  it('defers before setup while another install holds the install slot, and runs no setup', () => {
    const install = fixture();
    install.set('installed-status.json', status({}));
    install.set('status.json', status({ pendingInstall: true }));
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(onlyLine(run.stdout).text).toBe(
      `deferred: another install holds the install slot; rerun direnv exec ${install.main} ${binary} setup`,
    );
    expect(install.log('staged.log')).toHaveLength(2);
  });

  it('removes the partial stages a crashed run left, whatever land they were for, under the stage lock', () => {
    const install = fixture();
    const root = dirname(install.stage);
    for (const name of [`${install.head}.partial`, `${'9'.repeat(40)}.partial`]) {
      mkdirSync(join(root, name), { recursive: true });
      writeFileSync(join(root, name, 'half'), 'a copy that was never renamed\n');
    }
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('installed');
    expect(readdirSync(root)).toEqual([install.head]);
    // The lock found the two partials and left the one stage: they were removed while it was held.
    expect(install.log('cowshed.log')).toContain(cowshedCalls(install).lock('2 -> 1'));
  });

  it('records a failed production build, stages nothing, runs no setup and keeps the shed', () => {
    const install = fixture();
    install.set('build-fails', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('failed: the production build of cowshed:cli-record-arm64-macos exited 1');
    expect(existsSync(join(install.main, '.cache', 'cowshed-release'))).toBe(false);
    expect(install.log('staged.log')).toEqual([]);
    const calls = cowshedCalls(install);
    expect(install.log('cowshed.log')).toEqual([
      calls.status,
      calls.held,
      calls.probe,
      calls.fork,
      calls.path,
      calls.bundle,
      calls.production,
      calls.lock('0 -> 0'),
    ]);
    expect(readdirSync(install.sheds)).toEqual([install.shed]);
  });

  it('records a failed cache server bundle before building the CLI, and keeps the shed', () => {
    const install = fixture();
    install.set('bundle-fails', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('failed: the production build of cowshed:cli-record-arm64-macos exited 1');
    expect(install.log('nx.log')).toEqual([NX_SHOW(install), NX_BUNDLE(install)]);
    const calls = cowshedCalls(install);
    expect(install.log('cowshed.log')).toEqual([
      calls.status,
      calls.held,
      calls.probe,
      calls.fork,
      calls.path,
      calls.bundle,
      calls.lock('0 -> 0'),
    ]);
    expect(readdirSync(install.sheds)).toEqual([install.shed]);
  });

  it('records a failed fork, builds nothing and leaves no shed', () => {
    const install = fixture();
    install.set('fork-fails', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe(`failed: cowshed fork ${install.shed} --ref ${install.head} exited 5`);
    const calls = cowshedCalls(install);
    expect(install.log('cowshed.log')).toEqual([
      calls.status,
      calls.held,
      calls.probe,
      calls.fork,
      calls.lock('0 -> 0'),
    ]);
    expect(install.log('nx.log')).toEqual([NX_SHOW(install)]);
    expect(readdirSync(install.sheds)).toEqual([]);
  });

  it('re-points the shed to the next head and builds in the same warm tree, forking and removing nothing', () => {
    const install = fixture();
    const first = install.run();
    expect(first.status, first.stderr).toBe(0);
    expect(onlyLine(first.stdout).text).toBe('installed');
    const shed = join(install.sheds, install.shed);
    // A file of the shed's ignored build state, which only the same tree still holds.
    mkdirSync(join(shed, '.cache'), { recursive: true });
    writeFileSync(join(shed, '.cache', 'warm'), 'the first build\n');
    const asked = install.log('cowshed.log').length;
    const second = landAnother(install);
    const run = install.run({ args: [install.head, second] });
    expect(run.status, run.stderr).toBe(0);
    const line = onlyLine(run.stdout);
    expect(line.commit).toBe(second);
    expect(line.text).toBe('installed');
    const calls = cowshedCalls(install);
    expect(install.log('cowshed.log').slice(asked)).toEqual([
      calls.status,
      calls.held,
      calls.probe,
      calls.repoint(second),
      calls.path,
      calls.bundle,
      calls.production,
      calls.lock('1 -> 2'),
    ]);
    expect(git(shed, 'rev-parse', 'HEAD')).toBe(second);
    expect(readFileSync(join(shed, '.cache', 'warm'), 'utf8')).toBe('the first build\n');
    expect(readdirSync(install.sheds)).toEqual([install.shed]);
    expect(readdirSync(dirname(install.stage)).sort()).toEqual([install.head, second].sort());
  });

  it('refuses a shed with uncommitted work by name, records failed and builds nothing', () => {
    const install = fixture();
    const first = install.run();
    expect(first.status, first.stderr).toBe(0);
    const shed = join(install.sheds, install.shed);
    writeFileSync(join(shed, 'stray.txt'), 'left by hand\n');
    const asked = install.log('cowshed.log').length;
    const built = install.log('nx.log').length;
    const second = landAnother(install);
    const run = install.run({ args: [install.head, second] });
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe(
      `failed: the shed ${install.shed} cannot be re-pointed to ${second} (cowshed rebase exited 4): ` +
        `cowshed: workspace ${install.shed} has uncommitted work: stray.txt`,
    );
    const calls = cowshedCalls(install);
    expect(install.log('cowshed.log').slice(asked)).toEqual([
      calls.status,
      calls.held,
      calls.probe,
      calls.repoint(second),
      calls.lock('1 -> 1'),
    ]);
    expect(install.log('nx.log').slice(built)).toEqual([
      `show projects --affected --base=${install.head} --head=${second} --json [cwd main]`,
    ]);
    expect(git(shed, 'rev-parse', 'HEAD')).toBe(install.head);
    expect(existsSync(join(shed, 'stray.txt'))).toBe(true);
  });

  it('records failed, with the refusal, when the re-point is refused, and builds nothing', () => {
    const install = fixture();
    const first = install.run();
    expect(first.status, first.stderr).toBe(0);
    install.set('rebase-refuses', '');
    const built = install.log('nx.log').length;
    const second = landAnother(install);
    const run = install.run({ args: [install.head, second] });
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe(
      `failed: the shed ${install.shed} cannot be re-pointed to ${second} (cowshed rebase exited 4): ` +
        `cowshed: the replay onto ${second} conflicts`,
    );
    expect(install.log('nx.log')).toHaveLength(built + 1);
    expect(existsSync(join(install.main, '.cache', 'cowshed-release', second))).toBe(false);
  });

  it('records failed for a head older than the commit the shed stands at, which a rebase never moves back to', () => {
    const install = fixture();
    const second = landAnother(install);
    // The newer land's install ran first.
    const newer = install.run({ args: [install.head, second] });
    expect(newer.status, newer.stderr).toBe(0);
    const run = install.run();
    expect(onlyLine(run.stdout).text).toBe(
      `failed: the shed ${install.shed} stands at ${second} after cowshed rebase --onto ${install.head}: ` +
        'it is never moved back to an older commit',
    );
    expect(existsSync(install.stage)).toBe(false);
  });

  /** Polls a stand-in's log until a state shows in it; a state that never comes fails the test rather than hanging it. */
  async function until(condition: () => boolean): Promise<void> {
    const deadline = Date.now() + 30_000;
    while (!condition()) {
      if (Date.now() > deadline) throw new Error('the awaited state never came');
      await Bun.sleep(25);
    }
  }

  it('two overlapping installs never move the shed under a build: the second waits in the release lock', async () => {
    const install = fixture();
    const second = landAnother(install);
    // Both production builds wait for 'release', so the first still builds when the second asks for the lock.
    install.set('hold-build', '');
    const first = install.start();
    await until(() => install.log('nx.log').some((entry) => entry.includes(':production')));
    const later = install.start({ args: [install.head, second] });
    const calls = cowshedCalls(install);
    await until(() => install.log('cowshed.log').includes(calls.queued));
    expect(install.log('cowshed.log').some((entry) => entry.startsWith('rebase'))).toBe(false);
    install.set('release', '');
    const [a, b] = await Promise.all([first, later]);
    expect(a.status, a.stderr).toBe(0);
    expect(b.status, b.stderr).toBe(0);
    expect(onlyLine(a.stdout).text).toBe('installed');
    expect(onlyLine(b.stdout).text).toBe('installed');
    // The second re-points the shed only after the first has staged and let go of the lock.
    expect(install.log('cowshed.log')).toEqual([
      calls.status,
      calls.held,
      calls.probe,
      calls.fork,
      calls.path,
      calls.bundle,
      calls.production,
      calls.status,
      calls.queued,
      calls.lock('0 -> 1'),
      calls.held,
      calls.probe,
      calls.repoint(second),
      calls.path,
      calls.bundle,
      calls.production,
      calls.lock('1 -> 2'),
    ]);
    expect(git(join(install.sheds, install.shed), 'rev-parse', 'HEAD')).toBe(second);
  });

  it('an install that waited in the lock for the same head finds its stage whole and builds nothing', async () => {
    const install = fixture();
    install.set('hold-build', '');
    const first = install.start();
    await until(() => install.log('nx.log').some((entry) => entry.includes(':production')));
    const again = install.start();
    const calls = cowshedCalls(install);
    await until(() => install.log('cowshed.log').includes(calls.queued));
    install.set('release', '');
    const [a, b] = await Promise.all([first, again]);
    expect(onlyLine(a.stdout).text).toBe('installed');
    expect(onlyLine(b.stdout).text).toBe('installed');
    expect(install.log('cowshed.log').slice(-3)).toEqual([calls.lock('0 -> 1'), calls.held, calls.lock('1 -> 1')]);
    expect(install.log('nx.log').filter((entry) => entry.includes(':production'))).toHaveLength(1);
  });

  it('reuses a stage that is already whole, forking and building nothing', () => {
    const install = fixture();
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    mkdirSync(dirname(binary), { recursive: true });
    writeFileSync(binary, STAGED_STUB);
    chmodSync(binary, 0o755);
    // A stage is whole only with the release record setup judges the binary by.
    writeFileSync(`${binary}.build`, 'staged earlier\n');
    const server = join(install.stage, 'node_modules/@smoothbricks/nx-cache-server/dist/server/nx-cache-server.js');
    mkdirSync(dirname(server), { recursive: true });
    writeFileSync(server, 'staged earlier\n');
    // A stage is whole only with the mbx flake its setup pins: one staged without it is built again.
    mkdirSync(join(install.stage, 'nix/mbx'), { recursive: true });
    writeFileSync(join(install.stage, 'nix/mbx/flake.nix'), 'staged earlier\n');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('installed');
    expect(install.log('nx.log')).toEqual([NX_SHOW(install)]);
    expect(install.log('cowshed.log')).toEqual([]);
    expect(install.log('turns.log')).toEqual([]);
  });

  it('keeps the newest three stages and nothing else of its own, and never touches a foreign entry', () => {
    const install = fixture();
    const root = dirname(install.stage);
    const old = ['1', '2', '3', '4'].map((digit) => digit.repeat(40));
    const now = Date.now() / 1000;
    old.forEach((name, index) => {
      mkdirSync(join(root, name), { recursive: true });
      utimesSync(join(root, name), now - 400 + index * 100, now - 400 + index * 100);
    });
    mkdirSync(join(root, 'notes'));
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(readdirSync(root).sort()).toEqual([install.head, old[2], old[3], 'notes'].sort());
  });

  it('records a host it has no release CLI for, and builds nothing', () => {
    const install = fixture();
    const run = install.run({ machine: 'riscv64' });
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('skipped: no release CLI of cowshed installs on Darwin riscv64');
    expect(install.log('cowshed.log')).toEqual([]);
    expect(install.log('turns.log')).toEqual([]);
  });

  it('refuses arguments that are not commits before doing anything', () => {
    const install = fixture();
    const run = install.run({ args: ['main', install.head] });
    expect(run.status).toBe(64);
    expect(run.stdout).toBe('');
    expect(run.stderr).toContain('is not a full commit id');
    expect(install.log('nx.log')).toEqual([]);
  });

  it('takes no repository name: the old three arguments are a usage error', () => {
    const install = fixture();
    const run = install.run({ args: ['smoothbricks', install.before, install.head] });
    expect(run.status).toBe(64);
    expect(run.stdout).toBe('');
    expect(run.stderr).toContain('usage: land-install.sh <before> <head>');
    expect(install.log('nx.log')).toEqual([]);
  });
});
