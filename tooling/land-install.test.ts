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
  setupCommand,
  setupOutcome,
  setupPrompt,
  stageDirectory,
  staleStages,
  type Wanted,
} from './land-install.ts';

// The decisions of the post-land install are plain calls, one table each. land-install.sh then runs for real over a
// real git repository for main, with four stand-ins on PATH because they are this machine's tooling and the build:
// 'uname' (the host), 'bun nx' (the Nx calls: the affected set, the cache server bundle, the production build, which
// writes the staged binary below), 'cowshed land-turn' (a file that says whether the turn is held while a command
// runs) and the production CLI itself, whose doctor, gateway status and setup answer what a scenario sets. Each
// stand-in logs the turn state it ran under, so the tests prove the build ran holding the turn and every cowshed
// call after it ran without one.

const HEAD = 'a1'.repeat(20);

describe('cliTarget', () => {
  it('names the release CLI target and dist/bin directory of a macOS host by architecture', () => {
    expect(cliTarget('Darwin', 'arm64')).toEqual({ target: 'cli-arm64-macos', platform: 'darwin-arm64' });
    expect(cliTarget('Darwin', 'x86_64')).toEqual({ target: 'cli-x64-macos', platform: 'darwin-x64' });
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
  cowshed: `#!/bin/sh
[ "$1" = land-turn ] && [ "$3" = --workspace ] && [ "$5" = -- ] || exit 64
echo "$2 $4" >>"$FIX_STATE/turns.log"
shift 5
touch "$FIX_STATE/turn-held"
status=0
"$@" || status=$?
rm -f "$FIX_STATE/turn-held"
exit $status
`,
};

const NX_STUB = `#!/bin/sh
turn=free
[ -f "$FIX_STATE/turn-held" ] && turn=held
echo "$* [turn $turn]" >>"$FIX_STATE/nx.log"
case $1 in
show) cat "$FIX_STATE/affected.json" ;;
run)
  case $2 in
  nx-cache-server:bundle)
    [ ! -f "$FIX_STATE/bundle-fails" ] || exit 1
    mkdir -p packages/nx-cache-server/dist/server
    echo "server bundle" >packages/nx-cache-server/dist/server/nx-cache-server.js
    ;;
  cowshed:cli-*-macos:production)
    [ ! -f "$FIX_STATE/build-fails" ] || exit 1
    arch=$(echo "$2" | cut -d- -f2)
    mkdir -p "packages/cowshed/dist/bin/darwin-$arch"
    cp "$FIX_STATE/staged.sh" "packages/cowshed/dist/bin/darwin-$arch/cowshed"
    chmod +x "packages/cowshed/dist/bin/darwin-$arch/cowshed"
    ;;
  *) exit 97 ;;
  esac
  ;;
*) exit 97 ;;
esac
`;

const STAGED_STUB = `#!/bin/sh
turn=free
[ -f "$FIX_STATE/turn-held" ] && turn=held
cwd=elsewhere
[ "$(pwd)" = / ] && cwd=/
echo "$* [turn $turn] [cwd $cwd]" >>"$FIX_STATE/staged.log"
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
  /** Replaces what a stand-in answers: a file of the state directory. */
  set(name: string, text: string): void;
  /** What a stand-in logged, one entry per call. */
  log(name: string): readonly string[];
  run(options?: { readonly machine?: string; readonly args?: readonly string[] }): Run;
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
  const bin = join(root, 'bin');
  mkdirSync(join(main, 'tooling'), { recursive: true });
  mkdirSync(state);
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
  return {
    main,
    before,
    head,
    stage: join(main, '.cache', 'cowshed-release', head),
    set,
    log(name) {
      const path = join(state, name);
      return existsSync(path) ? readFileSync(path, 'utf8').trimEnd().split('\n') : [];
    },
    run(options = {}) {
      const run = spawnSync(
        'sh',
        [join(main, 'tooling/land-install.sh'), ...(options.args ?? ['smoothbricks', before, head])],
        {
          cwd: main,
          encoding: 'utf8',
          env: {
            ...process.env,
            ...GIT_ENV,
            PATH: `${bin}:${process.env.PATH}`,
            FIX_BUN: bun,
            FIX_NX: nx,
            FIX_STATE: state,
            FIX_MACHINE: options.machine ?? 'arm64',
          },
        },
      );
      return { status: run.status ?? -1, stdout: run.stdout, stderr: run.stderr };
    },
  };
}

const NX_SHOW = (install: Install): string =>
  `show projects --affected --base=${install.before} --head=${install.head} --json [turn free]`;
const NX_BUNDLE = 'run nx-cache-server:bundle --outputStyle=static-failures-only [turn held]';
const NX_PRODUCTION = 'run cowshed:cli-arm64-macos:production --outputStyle=static-failures-only [turn held]';

/** The one status line a run wrote, split into its timestamp, commit and the outcome text. */
function onlyLine(stdout: string): { readonly timestamp: string; readonly commit: string; readonly text: string } {
  const lines = stdout.split('\n').filter((line) => line !== '');
  expect(lines).toHaveLength(1);
  const match = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ) ([0-9a-f]{40}) install (.*)$/.exec(lines[0] ?? '');
  expect(match).not.toBeNull();
  return { timestamp: match?.[1] ?? '', commit: match?.[2] ?? '', text: match?.[3] ?? '' };
}

describe('land-install.sh', () => {
  it('does nothing for a land that changed neither cowshed nor a project it depends on', () => {
    const install = fixture(['tooling/change.ts']);
    install.set('affected.json', '["tooling","@smoothbricks/codebase"]');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(run.stdout).toBe('');
    expect(install.log('nx.log')).toEqual([NX_SHOW(install)]);
    expect(install.log('turns.log')).toEqual([]);
    expect(existsSync(join(install.main, '.cache'))).toBe(false);
  });

  it('builds holding a land turn, stages the distribution, then runs the staged setup once with none held', () => {
    const install = fixture();
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const line = onlyLine(run.stdout);
    expect(line.commit).toBe(install.head);
    expect(line.text).toBe('installed');
    expect(install.log('turns.log')).toEqual(['smoothbricks post-land-install']);
    expect(install.log('nx.log')).toEqual([NX_SHOW(install), NX_BUNDLE, NX_PRODUCTION]);
    expect(install.log('staged.log')).toEqual([
      '--json doctor [turn free] [cwd /]',
      '--json gateway status [turn free] [cwd /]',
      '--json setup [turn free] [cwd elsewhere]',
    ]);
    // The stage is a distribution, so setup finds the Nx cache server where it looks for a package's dependency.
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(statSync(binary).mode & 0o111).not.toBe(0);
    expect(
      readFileSync(
        join(install.stage, 'node_modules/@smoothbricks/nx-cache-server/dist/server/nx-cache-server.js'),
        'utf8',
      ),
    ).toBe('server bundle\n');
    // And setup finds the mbx flake it pins where a package ships it, above its binary.
    expect(readFileSync(join(install.stage, 'nix/mbx/flake.nix'), 'utf8')).toBe('{ outputs = _: { }; }\n');
    expect(readdirSync(dirname(install.stage))).toEqual([install.head]);
    // Nx and setup run in main's devenv, whatever environment started the script; the doctor and status reads need none.
    expect(install.log('direnv.log')).toEqual([
      `exec ${install.main} bun nx show projects --affected --base=${install.before} --head=${install.head} --json`,
      `exec ${install.main} bun nx run nx-cache-server:bundle --outputStyle=static-failures-only`,
      `exec ${install.main} bun nx run cowshed:cli-arm64-macos:production --outputStyle=static-failures-only`,
      `exec ${install.main} ${binary} --json setup`,
    ]);
  });

  it('builds the x64 CLI on an Intel host', () => {
    const install = fixture();
    const run = install.run({ machine: 'x86_64' });
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('installed');
    expect(install.log('nx.log')[2]).toBe(
      'run cowshed:cli-x64-macos:production --outputStyle=static-failures-only [turn held]',
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
    expect(install.log('staged.log')).toEqual(['--json doctor [turn free] [cwd /]']);
  });

  it('defers with the work that holds the host and the command to rerun, and runs no setup', () => {
    const install = fixture();
    install.set(
      'status.json',
      status({
        landTurns: [{ repo: 'gadgets', tickets: [ticket(4, 'gadget-land', 99)] }],
      }),
    );
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(onlyLine(run.stdout).text).toBe(
      `deferred: the land of gadget-land holds the gadgets land turn (ticket 4, pid 99); rerun direnv exec ${install.main} ${binary} setup`,
    );
    expect(install.log('staged.log')).toEqual([
      '--json doctor [turn free] [cwd /]',
      '--json gateway status [turn free] [cwd /]',
    ]);
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
    expect(install.log('staged.log')).toEqual(['--json doctor [turn free] [cwd /]']);
  });

  it('runs nothing when gateway status cannot be read', () => {
    const install = fixture();
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
    install.set(
      'status.1.json',
      status({
        landTurns: [{ repo: 'gadgets', tickets: [ticket(4, 'gadget-land', 99)] }],
      }),
    );
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
    expect(onlyLine(run.stdout).text).toBe(
      `deferred: the land of gadget-land holds the gadgets land turn (ticket 4, pid 99); rerun direnv exec ${install.main} ${binary} setup`,
    );
    expect(install.log('staged.log').map((entry) => entry.split(' ').slice(0, 2).join(' '))).toEqual([
      '--json doctor',
      '--json gateway',
      '--json setup',
      '--json gateway',
    ]);
  });

  it('defers while another install holds the install slot and runs no setup', () => {
    const install = fixture();
    install.set('status.json', status({ pendingInstall: true }));
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    expect(onlyLine(run.stdout).text).toBe(
      `deferred: another install holds the install slot; rerun direnv exec ${install.main} ${binary} setup`,
    );
    expect(install.log('staged.log')).toHaveLength(2);
  });

  it('removes the partial stages a crashed run left, whatever land they were for', () => {
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
  });

  it('records a failed production build, stages nothing and runs no cowshed', () => {
    const install = fixture();
    install.set('build-fails', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('failed: the production build of cowshed:cli-arm64-macos exited 1');
    expect(existsSync(join(install.main, '.cache'))).toBe(false);
    expect(install.log('staged.log')).toEqual([]);
  });

  it('records a failed cache server bundle before building the CLI', () => {
    const install = fixture();
    install.set('bundle-fails', '');
    const run = install.run();
    expect(run.status, run.stderr).toBe(0);
    expect(onlyLine(run.stdout).text).toBe('failed: the production build of cowshed:cli-arm64-macos exited 1');
    expect(install.log('nx.log')).toEqual([NX_SHOW(install), NX_BUNDLE]);
  });

  it('reuses a stage that is already whole, taking the turn but building nothing', () => {
    const install = fixture();
    const binary = join(install.stage, 'dist/bin/darwin-arm64/cowshed');
    mkdirSync(dirname(binary), { recursive: true });
    writeFileSync(binary, STAGED_STUB);
    chmodSync(binary, 0o755);
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
    expect(install.log('turns.log')).toEqual(['smoothbricks post-land-install']);
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
    expect(install.log('turns.log')).toEqual([]);
  });

  it('refuses arguments that are not commits before doing anything', () => {
    const install = fixture();
    const run = install.run({ args: ['smoothbricks', 'main', install.head] });
    expect(run.status).toBe(64);
    expect(run.stdout).toBe('');
    expect(run.stderr).toContain('is not a full commit id');
    expect(install.log('nx.log')).toEqual([]);
  });
});
