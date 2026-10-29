/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { existsSync, readFileSync, realpathSync, symlinkSync } from 'node:fs';
import { chmod, copyFile, mkdir, mkdtemp, rm, utimes, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DECLARED_LINKS_POSTINSTALL } from '../../package-policy.js';

/**
 * What shell entry does, measured against the real script.
 *
 * `tooling/direnv/setup-environment.ts` is what a direnv reload runs. These
 * build a repository the way smoo manages one and run the script in it the way
 * the managed devenv prologue does, counting what actually ran: provider
 * commands for declared secrets, `bun install` (through the root `prepare`
 * lifecycle it triggers), `uv sync` (through a recording `uv` on PATH) and
 * `cargo fetch` (through a recording `cargo` on PATH).
 * The prologue's own first statements, which ask the planner whether the declared links are refused, are the text
 * devenv.smoo.nix declares, run in bash as devenv runs them (`prologue`): once as the pass that captures the
 * environment, once as the entry proper.
 * A shell entry runs an install that replaces developer links only for the Nx
 * bindings (`relink: ['nx']`), and the Nx cases below measure that through the
 * real script. The other replacement cases call `keepDeveloperLinks` directly,
 * in a repository that an entry already installed.
 */

export const MANAGED = resolve(
  dirname(createRequire(import.meta.url).resolve('@smoothbricks/nx-plugin/package.json')),
  'managed',
  'raw',
);

/** What shell entry loads from `tooling/direnv`: the script and the modules it imports. */
export const MANAGED_SCRIPTS = [
  'setup-environment.ts',
  'cargo-fetch-bound.ts',
  'cargo-fetch-run.ts',
  'developer-links.ts',
  'installed-bun-links.ts',
  'linked-snapshots.ts',
  'secret-references.ts',
  'uv-relocation.ts',
] as const;

export interface ShellEntry {
  readonly exitCode: number;
  readonly stderr: string;
}

export type EntryOptions = {
  /** devenv's UV_PROJECT_ENVIRONMENT; present only in a shell that enables uv. */
  readonly uvProjectEnvironment?: string;
  /** The interpreter the managed module passes for a uv project. */
  readonly python?: string;
  /**
   * Runs as a GitHub Actions job whose secret store injects these variables: the strict CI
   * install, which runs no provider command and so needs every declared secret supplied.
   */
  readonly ciSecrets?: Readonly<Record<string, string>>;
  /** devenv's smoo:cargo-fetch task: `--cargo`, and its `status`, `--cargo --check`. */
  readonly cargo?: boolean;
  /** The fetch's deadline, SMOO_CARGO_FETCH_DEADLINE_MS: a step is given a shorter one than its default, never a longer. */
  readonly deadlineMs?: number;
  /** Variables the entry's environment holds besides the fixture's own: what the step must override or pass on. */
  readonly env?: Readonly<Record<string, string>>;
  /** devenv's smoo:developer-links task: `--links`, and its `status`, `--links --check`. */
  readonly links?: boolean;
  readonly check?: boolean;
  /** The prologue's question about the declared links, asked after the tasks: `--links --refusal`. */
  readonly refusal?: boolean;
} & (
  | {
      /** The CARGO_HOME this entry runs with, instead of the repository's own. */
      readonly cargoHome?: string;
      readonly home?: never;
    }
  | {
      /**
       * A private HOME and neither CARGO_HOME nor XDG_CACHE_HOME in the environment, so Cargo's
       * home and smoo's cache both fall back under it, as on a developer machine.
       */
      readonly home: string;
      readonly cargoHome?: never;
    }
);

/** A shell entry still running, with its output piped for `finished`. */
export type ShellProcess = Bun.Subprocess<'ignore', 'pipe', 'pipe'>;

export interface Repository {
  readonly root: string;
  /** devenv's DEVENV_STATE for this checkout. */
  readonly state: string;
  /** Runs the real setup-environment.ts exactly as the managed devenv shell does. */
  readonly enterShell: (options?: EntryOptions) => Promise<ShellEntry>;
  /** The same entry, returned while it runs, for a test that races or kills it. */
  readonly startShell: (options?: EntryOptions) => ShellProcess;
  /** The managed enterShell prologue's first statements, run as devenv runs them: its entry proper, or its capture pass. */
  readonly prologue: (options?: PrologueOptions) => Promise<ShellEntry>;
  /** Times something recorded in the ledger: a provider command's variable, `install`, or `uv`. */
  readonly count: (name: string) => number;
  /** The argument vectors the recording `uv` received, one per run. */
  readonly uvRuns: () => string[][];
  /** The argument vectors the recording `cargo` received, one per run. */
  readonly cargoRuns: () => string[][];
  /** The git low-speed limits the recording `cargo` saw, as `limit time` per run, `unset` for one it did not see. */
  readonly cargoLowSpeed: () => string[][];
  /** The CARGO_HOME every shell entry of this repository runs with. */
  readonly cargoHome: string;
  /** The XDG_CACHE_HOME every shell entry of this repository runs with: private, so a stamp has one place to be. */
  readonly xdgCache: string;
  /** The fixture's ledgers: what the recording `uv`, `cargo` and `git` write, and the pipes of a `cargo` that stalls. */
  readonly ledgers: string;
  /**
   * Each `git config` invocation that writes, as a `begin <pid>` and an `end <pid>` line
   * around the write, in the order they happened across every shell entry.
   */
  readonly gitConfigWrites: () => string[];
}

export interface RepositoryOptions {
  readonly prepare?: string;
  readonly workspaces?: readonly string[];
  /** The root manifest's dependencies. */
  readonly dependencies?: Readonly<Record<string, string>>;
  /** Extra files, relative to the repository root. */
  readonly files?: Readonly<Record<string, string>>;
  /** How long each `git config` write lingers before it runs, to widen a race between entries. */
  readonly gitWriteSeconds?: number;
  /** The root manifest's `smoo.developerLinks`. */
  readonly developerLinks?: Readonly<Record<string, string>>;
}

/**
 * A managed repository on disk: the managed direnv scripts and envrc, the git
 * hooks they link, a `.npmrc` that interpolates one declared variable, and a
 * fake TypeScript API package so the script's post-install pin check finds
 * what it looks for. Nothing here is a stub of the code under test — only of
 * the repository it runs in and of the `uv` and `cargo` binaries its shell provides.
 */
export async function withManagedRepository(
  options: RepositoryOptions,
  run: (repository: Repository) => Promise<void>,
): Promise<void> {
  const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-shell-entry-')));
  const root = join(scratch, 'checkout');
  const ledgers = join(scratch, 'ledgers');
  const bin = join(scratch, 'bin');
  try {
    await mkdir(join(root, 'tooling', 'direnv'), { recursive: true });
    await mkdir(join(root, 'tooling', 'git-hooks'), { recursive: true });
    await mkdir(ledgers);
    await mkdir(bin);
    for (const name of MANAGED_SCRIPTS) {
      await copyFile(join(MANAGED, 'tooling', 'direnv', name), join(root, 'tooling', 'direnv', name));
    }
    await copyFile(join(MANAGED, 'envrc'), join(root, '.envrc'));
    for (const hook of ['pre-commit', 'post-commit', 'commit-msg', 'pre-push']) {
      await writeFile(join(root, 'tooling', 'git-hooks', `${hook}.sh`), '#!/usr/bin/env bash\nexit 0\n');
    }
    await writeFile(join(root, 'tooling', 'workspace.gitconfig'), '[smoo]\n\tcheckout = original\n');
    const record = (name: string) => `printf 'x\\n' >> ${JSON.stringify(join(ledgers, name))}`;
    await writeFile(
      join(root, 'package.json'),
      JSON.stringify({
        name: 'fixture',
        version: '0.0.0',
        private: true,
        ...(options.workspaces === undefined ? {} : { workspaces: options.workspaces }),
        ...(options.dependencies === undefined ? {} : { dependencies: options.dependencies }),
        scripts: { postinstall: DECLARED_LINKS_POSTINSTALL, prepare: options.prepare ?? record('install') },
        smoo: {
          secrets: {
            SMOO_NPM_TOKEN: { command: counter('SMOO_NPM_TOKEN', ledgers) },
            SMOO_TOKEN: { command: counter('SMOO_TOKEN', ledgers) },
          },
          ...(options.developerLinks === undefined ? {} : { developerLinks: options.developerLinks }),
        },
      }),
    );
    await writeFile(
      join(root, '.npmrc'),
      '@acme:registry=https://npm.example.net\n//npm.example.net/:_authToken=${SMOO_NPM_TOKEN}\n',
    );
    for (const [file, content] of Object.entries(options.files ?? {})) {
      await mkdir(dirname(join(root, file)), { recursive: true });
      await writeFile(join(root, file), content);
    }
    // The post-install TypeScript pin is not what this measures, and an
    // install with no dependencies leaves nothing for it to find.
    const api = join(root, 'node_modules', '.bun', 'typescript@6.0.3', 'node_modules', 'typescript');
    await mkdir(api, { recursive: true });
    await writeFile(
      join(api, 'package.json'),
      JSON.stringify({ name: 'typescript', version: '6.0.3', main: 'index.js' }),
    );
    await writeFile(join(api, 'index.js'), "module.exports = { version: '6.0.3', readConfigFile: () => ({}) };\n");
    // uv as a Python shell provides it, reduced to what setup-environment.ts
    // can observe: it records its argv, and leaves an environment behind as
    // uv does. `venv` records whether it was asked for a relocatable one;
    // `sync` creates a plain environment where there is none, and installs
    // every workspace member editable, as the absolute path of its sources.
    await writeFile(
      join(bin, 'uv'),
      [
        '#!/usr/bin/env bash',
        `printf '%s\\n' "$*" >> ${JSON.stringify(join(ledgers, 'uv'))}`,
        'if [ "$1" = venv ]; then',
        '  environment="${@: -1}"',
        '  mkdir -p "$environment/bin"',
        '  relocatable=false',
        '  for arg in "$@"; do [ "$arg" = --relocatable ] && relocatable=true; done',
        '  printf "home = /nix/store/python/bin\\nrelocatable = %s\\n" "$relocatable" > "$environment/pyvenv.cfg"',
        '  exit 0',
        'fi',
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"',
        '[ -f "$UV_PROJECT_ENVIRONMENT/pyvenv.cfg" ] || printf "home = /nix/store/python/bin\\n" > "$UV_PROJECT_ENVIRONMENT/pyvenv.cfg"',
        'site="$UV_PROJECT_ENVIRONMENT/lib/python3.14/site-packages"',
        'mkdir -p "$site"',
        'printf "import _virtualenv\\n" > "$site/_virtualenv.pth"',
        'for member in "$PWD"/python/*/src; do',
        '  [ -d "$member" ] && printf "%s\\n" "$member" > "$site/$(basename "$(dirname "$member")").pth"',
        'done',
        'exit 0',
        '',
      ].join('\n'),
    );
    await chmod(join(bin, 'uv'), 0o755);
    // cargo reduced to what setup-environment.ts can observe: it records its argv and the git low-speed limits it was
    // given and, while the checkout holds `cargo-fails`, refuses the way a fetch without a registry does. It never
    // answers, as a fetch whose git child stalls on an established connection does, in one of two shapes: while the
    // checkout holds `cargo-stalls` it is a tree that reports through pipes a case makes, and while it holds
    // `cargo-hangs` it is one process that reports nothing. A fetch that succeeds leaves what Cargo itself writes in its
    // home — the package-cache lock and the registry — and nothing else, creating the home if it was not there.
    await writeFile(
      join(bin, 'cargo'),
      [
        '#!/usr/bin/env bash',
        'set -e',
        `printf '%s\\n' "$*" >> ${JSON.stringify(join(ledgers, 'cargo'))}`,
        `printf '%s %s\\n' "\${GIT_HTTP_LOW_SPEED_LIMIT-unset}" "\${GIT_HTTP_LOW_SPEED_TIME-unset}" >> ${JSON.stringify(join(ledgers, 'cargo-env'))}`,
        'if [ -f cargo-fails ]; then',
        "  echo 'error: failed to download from `https://index.crates.io/config.json`' >&2",
        '  exit 101',
        'fi',
        'if [ -f cargo-hangs ]; then',
        // One process, so a kill landing at any instant leaves nothing behind: a member forked while a group signal goes
        // out can outlive it, and a case that asserts nothing about the stand-in must not leave one.
        '  exec tail -f /dev/null',
        'fi',
        'if [ -f cargo-stalls ]; then',
        // Every process of this tree holds the write end of the lifetime pipe, so its end of file is the death of
        // the last one, and `up` is said once the tree stands. A case that reads neither finds two plain files.
        `  exec 3>${JSON.stringify(join(ledgers, 'stall-lifetime'))}`,
        '  tail -f /dev/null &',
        `  echo up > ${JSON.stringify(join(ledgers, 'stall-up'))}`,
        '  wait',
        'fi',
        'home="${CARGO_HOME:-$HOME/.cargo}"',
        'mkdir -p "$home/registry/cache"',
        ': >> "$home/.package-cache"',
        'exit 0',
        '',
      ].join('\n'),
    );
    await chmod(join(bin, 'cargo'), 0o755);
    // git as found on PATH, recording each `git config` that writes around the real run. A
    // write is any `git config` without a read option.
    const realGit = Bun.which('git');
    if (realGit === null) throw new Error('git must be on PATH');
    const writes = JSON.stringify(join(ledgers, 'git-config-writes'));
    await writeFile(
      join(bin, 'git'),
      [
        '#!/usr/bin/env bash',
        'write=',
        'if [ "$1" = config ]; then',
        '  write=1',
        '  for arg in "$@"; do',
        '    case "$arg" in --get | --get-all | --get-regexp | --get-urlmatch | --list | -l) write= ;; esac',
        '  done',
        'fi',
        `if [ -n "$write" ]; then printf 'begin %s\\n' "$$" >> ${writes}; sleep ${options.gitWriteSeconds ?? 0}; fi`,
        `${JSON.stringify(realGit)} "$@"`,
        'status=$?',
        `if [ -n "$write" ]; then printf 'end %s\\n' "$$" >> ${writes}; fi`,
        'exit $status',
        '',
      ].join('\n'),
    );
    await chmod(join(bin, 'git'), 0o755);
    await git(root, ['init', '--quiet']);
    await run(repository(root, ledgers, bin));
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

export function repository(root: string, ledgers: string, bin: string): Repository {
  const state = join(root, 'tooling', 'direnv', '.devenv', 'state');
  const lines = (name: string) => {
    const path = join(ledgers, name);
    return existsSync(path)
      ? readFileSync(path, 'utf8')
          .split('\n')
          .filter((line) => line.length > 0)
      : [];
  };
  return {
    root,
    state,
    enterShell: async (options = {}) => finished(startShell(root, state, bin, options)),
    startShell: (options = {}) => startShell(root, state, bin, options),
    prologue: async (options = {}) => finished(startPrologue(root, state, bin, options)),
    count: (name) => lines(name).length,
    uvRuns: () => lines('uv').map((line) => line.split(' ')),
    cargoRuns: () => lines('cargo').map((line) => line.split(' ')),
    cargoLowSpeed: () => lines('cargo-env').map((line) => line.split(' ')),
    cargoHome: join(dirname(bin), 'cargo-home'),
    xdgCache: join(dirname(bin), 'xdg-cache'),
    ledgers,
    gitConfigWrites: () => lines('git-config-writes'),
  };
}

/** A declared provider command that records each run and prints a value. */
export function counter(name: string, ledgers: string): [string, ...string[]] {
  return [
    process.execPath,
    '-e',
    `const fs = require('node:fs');fs.appendFileSync(${JSON.stringify(join(ledgers, name))}, '1\\n');` +
      `process.stdout.write(${JSON.stringify(`${name}-value`)})`,
  ];
}

export async function git(cwd: string, args: readonly string[]): Promise<string> {
  const proc = Bun.spawn({ cmd: ['git', ...args], cwd, stdout: 'pipe', stderr: 'pipe', stdin: 'ignore' });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (exitCode !== 0) throw new Error(`git ${args.join(' ')} failed: ${stderr}`);
  return stdout;
}

/**
 * One shell entry: `bun "$DEVENV_ROOT/setup-environment.ts"`, which is
 * verbatim what the managed devenv `enterShell` runs. The environment carries
 * only what a developer machine has, so neither the CI branch nor the cowshed
 * branch can decide this run, except the CI branch when `ciSecrets` is set.
 * Its temporary directory is the fixture's own root: `bun install` leaves a
 * file there on every run, and it goes with the fixture.
 */
export function startShell(root: string, state: string, bin: string, options: EntryOptions): ShellProcess {
  return Bun.spawn({
    cmd: [
      'bun',
      join(root, 'tooling', 'direnv', 'setup-environment.ts'),
      ...(options.python === undefined ? [] : ['--python', options.python]),
      ...(options.cargo === true ? ['--cargo'] : []),
      ...(options.links === true ? ['--links'] : []),
      ...(options.check === true ? ['--check'] : []),
      ...(options.refusal === true ? ['--refusal'] : []),
    ],
    cwd: root,
    env: entryEnvironment(root, state, bin, options),
    stdout: 'pipe',
    stderr: 'pipe',
    stdin: 'ignore',
  });
}

/** What a developer machine's shell entry carries: only what it has, and the fixture's own roots and caches. */
function entryEnvironment(root: string, state: string, bin: string, options: EntryOptions) {
  return {
    ...options.env,
    PATH: `${bin}:${process.env.PATH ?? ''}`,
    TMPDIR: dirname(bin),
    ...(options.home === undefined
      ? {
          HOME: process.env.HOME,
          CARGO_HOME: options.cargoHome ?? join(dirname(bin), 'cargo-home'),
          XDG_CACHE_HOME: join(dirname(bin), 'xdg-cache'),
        }
      : { HOME: options.home }),
    DEVENV_ROOT: join(root, 'tooling', 'direnv'),
    DEVENV_STATE: state,
    ...(options.uvProjectEnvironment === undefined ? {} : { UV_PROJECT_ENVIRONMENT: options.uvProjectEnvironment }),
    ...(options.deadlineMs === undefined ? {} : { SMOO_CARGO_FETCH_DEADLINE_MS: String(options.deadlineMs) }),
    ...(options.ciSecrets === undefined ? {} : { GITHUB_ACTIONS: 'true', ...options.ciSecrets }),
  };
}

export interface PrologueOptions {
  /** devenv's first pass over the enterShell script: it captures the environment before any task runs (DEVENV_SKIP_TASKS). */
  readonly capture?: boolean;
  /** A directory that goes first on PATH: a stand-in for a program the pass must not run. */
  readonly ahead?: string;
}

/**
 * The managed prologue's first statements (devenv.smoo.nix, `lib.mkBefore`) in bash, from the workspace root, as devenv
 * evaluates them: whatever stands between the move to the root and the PATH writer, which is where the prologue asks
 * about the declared links. The environment is a shell entry's, so the pass sees what the script would.
 */
export function startPrologue(root: string, state: string, bin: string, options: PrologueOptions): ShellProcess {
  const environment = entryEnvironment(root, state, bin, {});
  return Bun.spawn({
    cmd: ['bash', '-c', prologueEntrySteps()],
    cwd: root,
    env: {
      ...environment,
      PATH: `${options.ahead === undefined ? '' : `${options.ahead}:`}${environment.PATH}`,
      ...(options.capture === true ? { DEVENV_SKIP_TASKS: '1' } : {}),
    },
    stdout: 'pipe',
    stderr: 'pipe',
    stdin: 'ignore',
  });
}

/** The shell text of those statements; a Nix indented string spells a literal `${` as `''${`. */
function prologueEntrySteps(): string {
  const lines = readFileSync(join(MANAGED, 'tooling', 'direnv', 'devenv.smoo.nix'), 'utf8').split('\n');
  const first = lines.findIndex((line) => line.trim() === 'cd "$DEVENV_ROOT/../.."');
  const last = lines.findIndex((line, index) => index > first && line.trim().startsWith('PATH="$('));
  if (first < 0 || last < 0) {
    throw new Error(
      'devenv.smoo.nix no longer opens its prologue with the move to the workspace root and the PATH writer',
    );
  }
  return lines.slice(first, last).join('\n').replaceAll("''${", '${');
}

/** The smoo:developer-links task as devenv.smoo.nix declares it: what devenv runs as its status and, when that is not zero, as its exec. */
export function developerLinksTask(): { readonly exec: string; readonly status: string } {
  const text = readFileSync(join(MANAGED, 'tooling', 'direnv', 'devenv.smoo.nix'), 'utf8');
  const task = text.match(/"smoo:developer-links" = \{([\s\S]*?)\n {4}\};/)?.[1] ?? '';
  const exec = task.match(/^\s*exec = "(.*)";$/m)?.[1];
  const status = task.match(/^\s*status = "(.*)";$/m)?.[1];
  if (exec === undefined || status === undefined) {
    throw new Error('devenv.smoo.nix declares no smoo:developer-links task with an exec and a status');
  }
  return { exec, status };
}

export async function finished(proc: ShellProcess): Promise<ShellEntry> {
  const [stderr, exitCode] = await Promise.all([
    remainingText(proc.stderr),
    (async () => {
      await new Response(proc.stdout).text();
      return proc.exited;
    })(),
  ]);
  return { exitCode, stderr };
}

/**
 * The rest of a stream as text. A reader, not a Response: waitForInstallLock may have read and released the start of
 * an entry's stderr, and a Response refuses a body that was already read from.
 */
async function remainingText(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = '';
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) return text + decoder.decode();
      text += decoder.decode(chunk.value, { stream: true });
    }
  } finally {
    reader.releaseLock();
  }
}

/** Another shell entry's install, reduced to its native lock and a readiness pipe. */
export async function withInstallLockHeld(
  root: string,
  run: (holder: Bun.Subprocess<'pipe', 'pipe', 'inherit'>) => Promise<void>,
): Promise<void> {
  const holder = Bun.spawn({
    cmd: [
      'bun',
      '-e',
      [
        "const { dlopen, FFIType } = require('bun:ffi');",
        "const { openSync } = require('node:fs');",
        "const libc = dlopen(process.platform === 'darwin' ? '/usr/lib/libSystem.B.dylib' : 'libc.so.6', {",
        '  flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },',
        '});',
        `if (libc.symbols.flock(openSync(${JSON.stringify(join(root, 'node_modules', '.smoo-install.lock'))}, 'a'), 2) !== 0) process.exit(1);`,
        "process.stdout.write('held\\n');",
        'process.stdin.resume();',
      ].join('\n'),
    ],
    stdout: 'pipe',
    stderr: 'inherit',
    stdin: 'pipe',
  });
  try {
    const reader = holder.stdout.getReader();
    let ready = '';
    try {
      while (!ready.endsWith('\n')) {
        const chunk = await reader.read();
        if (chunk.done) throw new Error('install-lock holder exited before readiness');
        ready += new TextDecoder().decode(chunk.value);
      }
    } finally {
      reader.releaseLock();
    }
    if (ready !== 'held\n') throw new Error(`unexpected install-lock readiness: ${ready}`);
    await run(holder);
  } finally {
    if (holder.exitCode === null) holder.kill('SIGKILL');
    await holder.exited;
  }
}

export const INSTALL_LOCK_WAIT = "setup-environment: waiting for another shell entry's install";

/** Consume through the entry's first lock-contention event, leaving the rest for finished. */
export async function waitForInstallLock(entry: ShellProcess): Promise<string> {
  const reader = entry.stderr.getReader();
  let stderr = '';
  try {
    while (!stderr.includes(INSTALL_LOCK_WAIT)) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`shell entry exited without waiting for the held install lock: ${stderr}`);
      stderr += new TextDecoder().decode(chunk.value);
    }
    return stderr;
  } finally {
    reader.releaseLock();
  }
}

/** Change a file the way an editor or a pull does: new bytes, a newer mtime. */
export async function edit(path: string, content: string): Promise<void> {
  await writeFile(path, content);
  const later = new Date(Date.now() + 5_000);
  await utimes(path, later, later);
}

/** Puts a link at `entry`, relative to `root`, as `ln -sfn` does: whatever was there is replaced. */
export async function linkEntry(root: string, entry: string, target: string): Promise<void> {
  await rm(join(root, entry), { recursive: true, force: true });
  await mkdir(dirname(join(root, entry)), { recursive: true });
  symlinkSync(target, join(root, entry));
}

export const HEALTHY: ShellEntry = { exitCode: 0, stderr: '' };
