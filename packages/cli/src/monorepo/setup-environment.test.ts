/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { existsSync, lstatSync, readdirSync, readFileSync, readlinkSync, realpathSync, symlinkSync } from 'node:fs';
import { chmod, copyFile, cp, mkdir, mkdtemp, rm, utimes, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { keepDeveloperLinks } from './developer-links.js';
import { DECLARED_LINKS_POSTINSTALL } from './package-policy.js';

/**
 * What shell entry does, measured against the real script.
 *
 * `tooling/direnv/setup-environment.ts` is what a direnv reload runs. These
 * build a repository the way smoo manages one and run the script in it the way
 * the managed devenv prologue does, counting what actually ran: provider
 * commands for declared secrets, `bun install` (through the root `prepare`
 * lifecycle it triggers), `uv sync` (through a recording `uv` on PATH) and
 * `cargo fetch` (through a recording `cargo` on PATH).
 * A shell entry runs an install that replaces developer links only for the Nx
 * bindings (`relink: ['nx']`), and the Nx cases below measure that through the
 * real script. The other replacement cases call `keepDeveloperLinks` directly,
 * in a repository that an entry already installed.
 */

const MANAGED = resolve(
  dirname(createRequire(import.meta.url).resolve('@smoothbricks/nx-plugin/package.json')),
  'managed',
  'raw',
);

interface ShellEntry {
  readonly exitCode: number;
  readonly stderr: string;
}

type EntryOptions = {
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
  /** devenv's smoo:developer-links task: `--links`, and its `status`, `--links --check`. */
  readonly links?: boolean;
  readonly check?: boolean;
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
type ShellProcess = Bun.Subprocess<'ignore', 'pipe', 'pipe'>;

interface Repository {
  readonly root: string;
  /** devenv's DEVENV_STATE for this checkout. */
  readonly state: string;
  /** Runs the real setup-environment.ts exactly as the managed devenv shell does. */
  readonly enterShell: (options?: EntryOptions) => Promise<ShellEntry>;
  /** The same entry, returned while it runs, for a test that races or kills it. */
  readonly startShell: (options?: EntryOptions) => ShellProcess;
  /** Times something recorded in the ledger: a provider command's variable, `install`, or `uv`. */
  readonly count: (name: string) => number;
  /** The argument vectors the recording `uv` received, one per run. */
  readonly uvRuns: () => string[][];
  /** The argument vectors the recording `cargo` received, one per run. */
  readonly cargoRuns: () => string[][];
  /** The CARGO_HOME every shell entry of this repository runs with. */
  readonly cargoHome: string;
  /** The XDG_CACHE_HOME every shell entry of this repository runs with: private, so a stamp has one place to be. */
  readonly xdgCache: string;
  /**
   * Each `git config` invocation that writes, as a `begin <pid>` and an `end <pid>` line
   * around the write, in the order they happened across every shell entry.
   */
  readonly gitConfigWrites: () => string[];
}

interface RepositoryOptions {
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
async function withManagedRepository(
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
    for (const name of ['setup-environment.ts', 'developer-links.ts', 'linked-snapshots.ts', 'secret-references.ts']) {
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
    // cargo reduced to what setup-environment.ts can observe: it records its argv and,
    // while the checkout holds `cargo-fails`, refuses the way a fetch without a registry does.
    // A fetch that succeeds leaves what Cargo itself writes in its home — the package-cache
    // lock and the registry — and nothing else, creating the home if it was not there.
    await writeFile(
      join(bin, 'cargo'),
      [
        '#!/usr/bin/env bash',
        'set -e',
        `printf '%s\\n' "$*" >> ${JSON.stringify(join(ledgers, 'cargo'))}`,
        'if [ -f cargo-fails ]; then',
        "  echo 'error: failed to download from `https://index.crates.io/config.json`' >&2",
        '  exit 101',
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

function repository(root: string, ledgers: string, bin: string): Repository {
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
    count: (name) => lines(name).length,
    uvRuns: () => lines('uv').map((line) => line.split(' ')),
    cargoRuns: () => lines('cargo').map((line) => line.split(' ')),
    cargoHome: join(dirname(bin), 'cargo-home'),
    xdgCache: join(dirname(bin), 'xdg-cache'),
    gitConfigWrites: () => lines('git-config-writes'),
  };
}

/** A declared provider command that records each run and prints a value. */
function counter(name: string, ledgers: string): [string, ...string[]] {
  return [
    process.execPath,
    '-e',
    `const fs = require('node:fs');fs.appendFileSync(${JSON.stringify(join(ledgers, name))}, '1\\n');` +
      `process.stdout.write(${JSON.stringify(`${name}-value`)})`,
  ];
}

async function git(cwd: string, args: readonly string[]): Promise<string> {
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
 */
function startShell(root: string, state: string, bin: string, options: EntryOptions): ShellProcess {
  return Bun.spawn({
    cmd: [
      'bun',
      join(root, 'tooling', 'direnv', 'setup-environment.ts'),
      ...(options.python === undefined ? [] : ['--python', options.python]),
      ...(options.cargo === true ? ['--cargo'] : []),
      ...(options.links === true ? ['--links'] : []),
      ...(options.check === true ? ['--check'] : []),
    ],
    cwd: root,
    env: {
      PATH: `${bin}:${process.env.PATH ?? ''}`,
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
      ...(options.ciSecrets === undefined ? {} : { GITHUB_ACTIONS: 'true', ...options.ciSecrets }),
    },
    stdout: 'pipe',
    stderr: 'pipe',
    stdin: 'ignore',
  });
}

async function finished(proc: ShellProcess): Promise<ShellEntry> {
  const [stderr, exitCode] = await Promise.all([
    new Response(proc.stderr).text(),
    (async () => {
      await new Response(proc.stdout).text();
      return proc.exited;
    })(),
  ]);
  return { exitCode, stderr };
}

/** Change a file the way an editor or a pull does: new bytes, a newer mtime. */
async function edit(path: string, content: string): Promise<void> {
  await writeFile(path, content);
  const later = new Date(Date.now() + 5_000);
  await utimes(path, later, later);
}

/** Puts a link at `entry`, relative to `root`, as `ln -sfn` does: whatever was there is replaced. */
async function linkEntry(root: string, entry: string, target: string): Promise<void> {
  await rm(join(root, entry), { recursive: true, force: true });
  await mkdir(dirname(join(root, entry)), { recursive: true });
  symlinkSync(target, join(root, entry));
}

const HEALTHY: ShellEntry = { exitCode: 0, stderr: '' };

describe('what shell entry costs, in provider invocations', () => {
  it('runs no provider for a registry credential, and a shell secret only for the entry that installs', async () => {
    await withManagedRepository({}, async ({ enterShell: enter, count }) => {
      for (let entry = 0; entry < 3; entry += 1) {
        expect({ entry, ...(await enter()) }).toEqual({ entry, ...HEALTHY });
      }

      // The whole point, as a count: the registry credential's provider is
      // never asked, so there is no credential prompt on any reload.
      expect(count('SMOO_NPM_TOKEN')).toBe(0);
      // A shell secret exists for the install that inherits it, so it resolves
      // when an install runs — the first entry — and never on an entry that
      // installs nothing.
      expect(count('install')).toBe(1);
      expect(count('SMOO_TOKEN')).toBe(1);
    });
  });

  it('names the exact grouped command when the install it ran fails', async () => {
    // A failing root prepare script fails `bun install` locally without
    // needing a registry: the degraded path is what prints the deferrals.
    await withManagedRepository({ prepare: 'exit 1' }, async ({ enterShell: enter, count }) => {
      const { exitCode, stderr } = await enter();

      // A local failure must not take the shell down with it.
      expect(exitCode).toBe(0);
      expect(stderr).toContain('SMOO_NPM_TOKEN (registry)');
      expect(stderr).toContain('smoo secrets run registry bun install');
      expect(count('SMOO_NPM_TOKEN')).toBe(0);
    });
  });
});

describe('what shell entry installs', () => {
  const MEMBER = 'packages/member/package.json';

  it('installs only when a lockfile, a workspace manifest or bunfig.toml changed since the last install', async () => {
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: { [MEMBER]: JSON.stringify({ name: 'member', version: '0.0.0' }), 'bunfig.toml': '' },
      },
      async ({ root, enterShell: enter, count }) => {
        expect(await enter()).toEqual(HEALTHY);
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(1);

        await edit(join(root, MEMBER), JSON.stringify({ name: 'member', version: '0.0.1' }));
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(2);

        await edit(join(root, 'bunfig.toml'), '[install]\nexact = true\n');
        expect(await enter()).toEqual(HEALTHY);
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(3);
      },
    );
  });

  it('reinstalls receipts missing lock digests and repairs digest drift through the same installer', async () => {
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: {
          'packages/member/package.json':
            '{"name":"member","version":"0.0.0","dependencies":{"dep":"file:../../vendor/dep"}}',
          'vendor/dep/package.json': '{"name":"dep","version":"0.0.0"}',
        },
      },
      async ({ root, enterShell: enter, count }) => {
        expect(await enter()).toEqual(HEALTHY);
        const path = join(root, 'node_modules/.smoo-install');
        const stamp = JSON.parse(readFileSync(path, 'utf8'));
        await writeFile(path, JSON.stringify({ inputs: stamp.inputs }));
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(2);
        const recorded = JSON.parse(readFileSync(path, 'utf8'));
        expect(recorded.lockfiles['bun.lock']).toBe(
          new Bun.CryptoHasher('sha256').update(readFileSync(join(root, 'bun.lock'))).digest('hex'),
        );
        await writeFile(path, JSON.stringify({ ...recorded, lockfiles: {} }));
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(3);
      },
    );
  });

  it('retries an install that failed instead of recording it as done', async () => {
    await withManagedRepository(
      { prepare: `printf 'x\\n' >> attempts; exit 1` },
      async ({ root, enterShell: enter }) => {
        await enter();
        await enter();
        expect(readFileSync(join(root, 'attempts'), 'utf8')).toBe('x\nx\n');
      },
    );
  });

  // An unfrozen entry install re-resolved a consumer's ranges and migrated its lockfile format: a bun.lock rewrite of
  // thousands of lines that nobody asked for. Shell entry installs what bun.lock records, or fails saying why.
  it('never rewrites bun.lock, failing the install of a manifest the lockfile does not record', async () => {
    const vendored = (name: string) => JSON.stringify({ name, version: '1.0.0' });
    const member = (...dependencies: string[]) =>
      JSON.stringify({
        name: 'member',
        version: '0.0.0',
        dependencies: Object.fromEntries(dependencies.map((name) => [name, `file:../../vendor/${name}`])),
      });
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: {
          'vendor/dep/package.json': vendored('dep'),
          'vendor/dep2/package.json': vendored('dep2'),
          [MEMBER]: member('dep'),
        },
      },
      async ({ root, enterShell: enter }) => {
        const lock = Bun.spawnSync(['bun', 'install', '--no-summary'], { cwd: root, stdout: 'ignore', stderr: 'pipe' });
        expect(lock.exitCode, lock.stderr.toString()).toBe(0);
        const locked = readFileSync(join(root, 'bun.lock'), 'utf8');
        expect(await enter()).toEqual(HEALTHY);

        await edit(join(root, MEMBER), member('dep', 'dep2'));
        const { exitCode, stderr } = await enter();
        // A failed install degrades the shell; it does not take it down.
        expect(exitCode).toBe(0);
        expect(stderr).toContain('bun install --frozen-lockfile');
        expect(readFileSync(join(root, 'bun.lock'), 'utf8')).toBe(locked);
        expect(existsSync(join(root, 'node_modules', 'dep2'))).toBe(false);
      },
    );
  });

  it('installs once when two shell entries start together', async () => {
    // A shell pool warms two shells at once in one checkout. The second must
    // wait for the first install and then find nothing left to do, not run a
    // second install into the same node_modules. The prepare script's real
    // second holds the first install open so the two entries overlap; the
    // race is between processes, so there is no clock to fake.
    await withManagedRepository({ prepare: `sleep 1; printf 'x\\n' >> installed` }, async ({ root, startShell }) => {
      const entries = await Promise.all([finished(startShell()), finished(startShell())]);
      expect(entries.map((entry) => entry.exitCode)).toEqual([0, 0]);
      expect(entries.map((entry) => entry.stderr).join('')).toBe(
        `setup-environment: waiting for another shell entry's install in ${root}\n`,
      );
      expect(readFileSync(join(root, 'installed'), 'utf8')).toBe('x\n');
    });
  });

  it('lets the next entry install at once after the entry holding the install was killed', async () => {
    await withManagedRepository(
      {
        // The held install parks in its prepare script, recording the pid
        // that becomes `sleep`, so the test can kill both halves of it.
        prepare: `if [ -f hold ]; then echo $$ > held; exec sleep 30; fi; printf 'x\\n' >> installed`,
        files: { hold: '' },
      },
      async ({ root, startShell, enterShell: enter }) => {
        const holder = startShell();
        const held = join(root, 'held');
        // The holder is another process; the only signal that it is inside
        // the install is the pid file its prepare script writes.
        for (let waited = 0; !existsSync(held) || readFileSync(held, 'utf8') === ''; waited += 50) {
          expect(waited).toBeLessThan(20_000);
          await Bun.sleep(50);
        }
        holder.kill('SIGKILL');
        await holder.exited;
        process.kill(Number(readFileSync(held, 'utf8').trim()), 'SIGKILL');
        await rm(join(root, 'hold'));

        const started = performance.now();
        expect(await enter()).toEqual(HEALTHY);
        expect(performance.now() - started).toBeLessThan(10_000);
        expect(readFileSync(join(root, 'installed'), 'utf8')).toBe('x\n');
      },
    );
  });

  it('enters a read-only checkout whose installs are current without writing it', async () => {
    // A sandboxed job may be handed a read-only view of the checkout. Its shell
    // entry has nothing to install, so it must neither install nor need the
    // write access an install would.
    await withManagedRepository({}, async ({ root, enterShell: enter, count }) => {
      expect(await enter()).toEqual(HEALTHY);
      expect(Bun.spawnSync(['chmod', '-R', 'a-w', root]).exitCode).toBe(0);
      try {
        expect(await enter()).toEqual(HEALTHY);
      } finally {
        expect(Bun.spawnSync(['chmod', '-R', 'u+w', root]).exitCode).toBe(0);
      }
      expect(count('install')).toBe(1);
    });
  });
});

describe('Bun installation receipt link admission', () => {
  // Only this fixture's private cache is touched. The managed setup runs a real Bun install;
  // its prepare script plants the on-disk isolated topology the receipt must admit or refuse.
  const fixture =
    "import { appendFileSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';\nimport { dirname, join, relative } from 'node:path';\nconst root = process.cwd();\nconst mode = readFileSync('integrity-mode', 'utf8');\nconst store = join(root, 'node_modules', '.bun');\nconst cache = join(dirname(root), 'private-bun-cache');\nconst parentStore = join(cache, 'fixture-parent@1.0.0');\nconst parent = join(parentStore, 'node_modules', '@fixture', 'parent');\nconst child = join(cache, 'fixture-child@1.0.0', 'node_modules', '@fixture', 'child');\nconst storeAlias = join(store, '@fixture+parent@1.0.0');\nconst childAlias = join(parentStore, 'node_modules', '@fixture', 'child');\nconst hoistAlias = join(store, 'node_modules', '@fixture', 'child');\nrmSync(storeAlias, { force: true });\nrmSync(join(store, 'node_modules', '@fixture'), { recursive: true, force: true });\nrmSync(cache, { recursive: true, force: true });\nmkdirSync(parent, { recursive: true });\nmkdirSync(child, { recursive: true });\nmkdirSync(dirname(hoistAlias), { recursive: true });\nwriteFileSync(join(parent, 'package.json'), JSON.stringify({\n  name: '@fixture/parent', version: '1.0.0', dependencies: { '@fixture/child': '1.0.0' },\n  optionalDependencies: { '@fixture/omitted': '1.0.0' },\n}));\nif (mode !== 'missing-manifest') {\n  writeFileSync(join(child, 'package.json'), mode === 'malformed-manifest' ? '{' : JSON.stringify({ name: '@fixture/child', version: '1.0.0' }));\n}\nsymlinkSync(mode === 'broken-store' ? join(cache, 'missing-parent') : parentStore, storeAlias);\nconst childTarget = mode === 'broken-transitive' ? join(cache, 'missing-child') : child;\nsymlinkSync(relative(dirname(childAlias), childTarget), childAlias);\nif (mode === 'healthy' || mode === 'broken-hoist') {\n  symlinkSync(mode === 'broken-hoist' ? join(cache, 'missing-child') : child, hoistAlias);\n}\nappendFileSync('integrity-installs', 'x\\n');\n";
  const files = { 'integrity-fixture.mjs': fixture, 'integrity-mode': 'healthy' };
  const options = { prepare: 'bun integrity-fixture.mjs', files };
  const CI_SECRETS = { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' };

  for (const ci of [false, true]) {
    it(`${ci ? 'CI' : 'local'} refuses each broken dependency topology before writing an install receipt`, async () => {
      await withManagedRepository(options, async ({ root, enterShell: enter }) => {
        const defects = ['broken-store', 'broken-transitive', 'broken-hoist', 'missing-manifest', 'malformed-manifest'];
        expect(defects).toHaveLength(5);
        for (const defect of defects) {
          const label = `${ci ? 'CI' : 'local'} refuses ${defect} before writing an install receipt`;
          // The prepare script rebuilds its topology; retain the unrelated TypeScript API pin.
          await writeFile(join(root, 'integrity-installs'), '');
          await writeFile(join(root, 'integrity-mode'), 'healthy');
          expect(await enter(), label).toEqual(HEALTHY);
          const receipt = join(root, 'node_modules', '.smoo-install');
          await rm(receipt);
          await writeFile(join(root, 'integrity-mode'), defect);
          const refused = await enter(ci ? { ciSecrets: CI_SECRETS } : {});
          const cache = join(dirname(root), 'private-bun-cache');
          const storeAlias = join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0');
          const actualPath =
            defect === 'broken-store'
              ? storeAlias
              : defect === 'broken-hoist'
                ? join(root, 'node_modules', '.bun', 'node_modules', '@fixture', 'child')
                : join(storeAlias, 'node_modules', '@fixture', 'child');
          const expectedTarget =
            defect === 'broken-store'
              ? join(cache, 'missing-parent')
              : defect === 'broken-transitive' || defect === 'broken-hoist'
                ? join(cache, 'missing-child')
                : join(cache, 'fixture-child@1.0.0', 'node_modules', '@fixture', 'child');
          expect(refused.exitCode, label).toBe(ci ? 1 : 0);
          expect(refused.stderr, label).toContain('Bun install integrity:');
          expect(refused.stderr, label).toContain(actualPath);
          expect(refused.stderr, label).toContain(`expected target ${expectedTarget}`);
          expect(refused.stderr, label).toContain(ci ? '--- ERROR:' : '--- WARNING:');
          if (!ci) expect(refused.stderr, label).toContain('The shell is loaded WITHOUT installed dependencies');
          expect(refused.stderr, label).not.toContain('! Failed to install dependencies with frozen lockfile');
          expect(existsSync(receipt), label).toBe(false);
          // The guard is outside CI's frozen-command catch: a refusal cannot trigger another install.
          expect(readFileSync(join(root, 'integrity-installs'), 'utf8'), label).toBe('x\nx\n');
        }
      });
    });
  }

  it('records complete transitive cache links after the normal CI frozen install', async () => {
    await withManagedRepository(options, async ({ root, enterShell: enter }) => {
      expect(await enter()).toEqual(HEALTHY);
      const receipt = join(root, 'node_modules', '.smoo-install');
      await rm(receipt);
      const installed = await enter({ ciSecrets: CI_SECRETS });
      expect(installed.exitCode).toBe(0);
      expect(installed.stderr).not.toContain('Bun install integrity:');
      expect(installed.stderr).not.toContain('git diff after install:');
      expect(existsSync(receipt)).toBe(true);
      expect(readFileSync(join(root, 'integrity-installs'), 'utf8')).toBe('x\nx\n');
    });
  });

  it('records complete scoped/transitive cache links and leaves unchanged entry installed', async () => {
    // An empty dependency graph need not produce bun.lock. Install a real local package so
    // this oracle checks the genuine lock digest without changing the install or receipt contract.
    await withManagedRepository(
      {
        ...options,
        dependencies: { 'fixture-lock-anchor': 'file:vendor/lock-anchor' },
        files: {
          ...files,
          'vendor/lock-anchor/package.json': JSON.stringify({ name: 'fixture-lock-anchor', version: '1.0.0' }),
        },
      },
      async ({ root, enterShell: enter }) => {
        expect(await enter()).toEqual(HEALTHY);
        const receipt = join(root, 'node_modules', '.smoo-install');
        const recorded = readFileSync(receipt);
        expect(JSON.parse(recorded.toString('utf8'))).toEqual({
          inputs: expect.stringMatching(/^[0-9a-f]{64}$/),
          lockfiles: {
            'bun.lock': new Bun.CryptoHasher('sha256').update(readFileSync(join(root, 'bun.lock'))).digest('hex'),
          },
        });
        expect(await enter()).toEqual(HEALTHY);
        expect(readFileSync(receipt)).toEqual(recorded);
        expect(readFileSync(join(root, 'integrity-installs'), 'utf8')).toBe('x\n');
      },
    );
  });
});

describe('what a CI install reports and records', () => {
  const MEMBER = 'packages/member/package.json';
  // A CI runner runs no provider command: the job's secret store injects every declared secret.
  const CI_SECRETS = { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' };
  const member = (...dependencies: string[]) =>
    JSON.stringify({
      name: 'member',
      version: '0.0.0',
      dependencies: Object.fromEntries(dependencies.map((name) => [name, `file:../../vendor/${name}`])),
    });
  const vendored = (name: string) => JSON.stringify({ name, version: '1.0.0' });
  // Two dependencies that live in the checkout, so locking, the frozen miss and the
  // fallback install all run without a registry.
  const FILES = {
    '.gitignore': 'node_modules\ntooling/direnv/.devenv\n',
    'vendor/dep/package.json': vendored('dep'),
    'vendor/dep2/package.json': vendored('dep2'),
    [MEMBER]: member('dep'),
  };

  /** Locks the checkout with a real `bun install` and stages that state, as a pull request commits it. */
  async function stageLockedCheckout(root: string): Promise<void> {
    const install = Bun.spawn({
      cmd: ['bun', 'install'],
      cwd: root,
      // Not the CI entry's secrets: a developer who exports them must not change what gets staged.
      env: Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('SMOO_'))),
      stdout: 'ignore',
      stderr: 'pipe',
      stdin: 'ignore',
    });
    const [stderr, exitCode] = await Promise.all([new Response(install.stderr).text(), install.exited]);
    if (exitCode !== 0) throw new Error(`bun install failed: ${stderr}`);
    await git(root, ['add', '-A']);
  }

  /**
   * Then stages a member manifest that asks for one more dependency than the
   * lockfile records: the pull request that changed a manifest and not bun.lock.
   * The install that follows rewrites bun.lock in the working tree, which is what
   * `git diff` shows against the staged state.
   */
  async function stageStaleLock(root: string): Promise<void> {
    await stageLockedCheckout(root);
    await writeFile(join(root, MEMBER), member('dep', 'dep2'));
    await git(root, ['add', MEMBER]);
  }

  // devenv reports a failed shell entry as "Shell environment capture failed:"
  // followed by the shell's stderr and nothing else; the stdout it captured is
  // discarded. `finished` mirrors that, so these read exactly what devenv would show.
  it('prints the lockfile diff on stderr and still fails the entry', async () => {
    await withManagedRepository({ workspaces: ['packages/*'], files: FILES }, async ({ root, enterShell: enter }) => {
      await stageStaleLock(root);
      const { exitCode, stderr } = await enter({ ciSecrets: CI_SECRETS });

      expect(exitCode).toBe(1);
      expect(stderr).toContain('git diff after install:');
      expect(stderr).toContain('+        "dep2": "file:../../vendor/dep2",');
      // The failed install records nothing, so devenv's task has no stamp to find current.
      expect(existsSync(join(root, 'node_modules', '.smoo-install'))).toBe(false);
    });
  });

  it('prints the whole diff when it is larger than a pipe holds and the reader is slow to drain it', async () => {
    // The entry exits straight after printing, and a write still queued then is cut short:
    // a diff that ends mid-hunk cannot be applied or checked. Four megabytes is well past
    // what a pipe buffers, so the reader's pause decides whether the tail survives.
    const bytes = 4_000_000;
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: { ...FILES, 'tracked.txt': '' },
        // Only the entry under test writes: it alone has the injected secret in its environment.
        prepare: `if [ -n "$SMOO_TOKEN" ]; then head -c ${bytes} /dev/zero | tr '\\0' x > tracked.txt; printf '\\nEND\\n' >> tracked.txt; fi`,
      },
      async ({ root, startShell }) => {
        await stageStaleLock(root);
        const entry = startShell({ ciSecrets: CI_SECRETS });
        // The install's prepare script writes the file, so once it is whole the entry is about to
        // print its diff. Leave the output unread for a while from there: the write cannot finish
        // into a full pipe.
        const tracked = join(root, 'tracked.txt');
        for (let waited = 0; !existsSync(tracked) || !readFileSync(tracked, 'utf8').endsWith('END\n'); waited += 50) {
          expect(waited).toBeLessThan(20_000);
          await Bun.sleep(50);
        }
        await Bun.sleep(1_000);
        const { exitCode, stderr } = await finished(entry);

        expect(exitCode).toBe(1);
        expect(stderr).toContain('+END\n');
        expect(stderr.length).toBeGreaterThan(bytes);
      },
    );
  });

  it('prints no diff when the frozen install succeeds', async () => {
    await withManagedRepository({ workspaces: ['packages/*'], files: FILES }, async ({ root, enterShell: enter }) => {
      await stageLockedCheckout(root);
      const { exitCode, stderr } = await enter({ ciSecrets: CI_SECRETS });

      expect(exitCode).toBe(0);
      expect(stderr).not.toContain('git diff after install:');
    });
  });

  it('stamps node_modules, the file devenv’s smoo:install task watches, so a checkout without one installs again', async () => {
    // A host runner keeps devenv's task state on a shared bind and its checkout does not outlive the job:
    // with no stamp the next job's checkout, same lockfile, was found current, installed nothing and
    // had no `ttsc`. The stamp is what devenv sees go missing with node_modules.
    await withManagedRepository({ workspaces: ['packages/*'], files: FILES }, async ({ root, enterShell: enter }) => {
      await stageLockedCheckout(root);
      await rm(join(root, 'node_modules'), { recursive: true, force: true });
      const { exitCode } = await enter({ ciSecrets: CI_SECRETS });

      expect(exitCode).toBe(0);
      const stamp = JSON.parse(readFileSync(join(root, 'node_modules', '.smoo-install'), 'utf8'));
      expect(stamp).toEqual({
        inputs: expect.stringMatching(/^[0-9a-f]{64}$/),
        lockfiles: {
          'bun.lock': new Bun.CryptoHasher('sha256').update(readFileSync(join(root, 'bun.lock'))).digest('hex'),
        },
      });
      expect(readFileSync(join(MANAGED, 'tooling', 'direnv', 'devenv.smoo.nix'), 'utf8')).toContain(
        '"../../node_modules/.smoo-install"',
      );
    });
  });
});

describe('what shell entry keeps linked', () => {
  // Three workspace members, so the install links two of them into the third's
  // node_modules with no registry involved: `@fixture/lib` under a scope,
  // `util` without one. Bun relinks both on every install, as it does a
  // registry package.
  const APP = 'packages/app';
  const LINKED = `${APP}/node_modules/@fixture/lib`;
  const UNLINKED = `${APP}/node_modules/util`;
  const WORKSPACE = {
    workspaces: ['packages/*'],
    files: {
      'bunfig.toml': '[install]\nlinker = "isolated"\n',
      [`${APP}/package.json`]: JSON.stringify({
        name: 'app',
        version: '0.0.0',
        dependencies: { '@fixture/lib': 'workspace:*', util: 'workspace:*' },
      }),
      'packages/lib/package.json': JSON.stringify({ name: '@fixture/lib', version: '0.0.0' }),
      'packages/util/package.json': JSON.stringify({ name: 'util', version: '0.0.0' }),
    },
  };

  /** Moves the install digest, so the next entry installs. */
  const touchManifest = (root: string) =>
    edit(join(root, 'packages/util/package.json'), JSON.stringify({ name: 'util', version: '0.0.1' }));

  it('preserves external developer targets while an install refreshes declared dependencies', async () => {
    await withManagedRepository(WORKSPACE, async ({ root, enterShell: enter, count }) => {
      expect(await enter()).toEqual(HEALTHY);
      expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
      // A local build of @fixture/lib beside the checkout, linked the way `bun
      // link` or `ln -s` does.
      const local = join(dirname(root), 'local-lib');
      await mkdir(local);
      await writeFile(join(local, 'package.json'), JSON.stringify({ name: '@fixture/lib', version: '9.9.9' }));
      await rm(join(root, LINKED));
      symlinkSync(local, join(root, LINKED));

      expect((await enter()).exitCode).toBe(0);
      expect(count('install')).toBe(1);

      await touchManifest(root);
      expect((await enter()).exitCode).toBe(0);
      expect(count('install')).toBe(2);
      expect(readlinkSync(join(root, LINKED))).toBe(local);
      expect(readlinkSync(join(root, UNLINKED))).toBe('../../util');
    });
  });

  it('replaces a dangling developer link with the installed lockfile dependency', async () => {
    await withManagedRepository(WORKSPACE, async ({ root, enterShell: enter, count }) => {
      expect(await enter()).toEqual(HEALTHY);
      expect(count('install')).toBe(1);
      const stamp = join(root, 'node_modules', '.smoo-install');
      const stampBytes = readFileSync(stamp);
      const gone = join(dirname(root), 'deleted-lib');
      await rm(join(root, LINKED));
      symlinkSync(gone, join(root, LINKED));

      expect(readFileSync(stamp)).toEqual(stampBytes);
      expect((await enter()).exitCode).toBe(0);
      expect(count('install')).toBe(2);
      expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');

      expect((await enter()).exitCode).toBe(0);
      expect(count('install')).toBe(2);
    });
  });

  describe('as smoo.developerLinks declares them', () => {
    /** A local checkout of a package: a directory holding its manifest. */
    async function checkout(at: string, name: string): Promise<string> {
      await mkdir(at, { recursive: true });
      await writeFile(join(at, 'package.json'), JSON.stringify({ name, version: '9.9.9' }));
      return at;
    }

    it('resolves declarations at this checkout even when a cowshed marker names another root', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../local-lib' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          // The marker cannot change the root against which declarations resolve.
          const main = join(dirname(root), 'projects', 'main');
          await mkdir(main, { recursive: true });
          await mkdir(join(root, '.cowshed'), { recursive: true });
          await writeFile(join(root, '.cowshed', 'workspace.json'), JSON.stringify({ projectRoot: main }));
          const local = await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');

          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          const linked = await enter({ links: true });
          expect(linked.exitCode).toBe(0);
          expect(linked.stderr).toContain(`linked declared packages to local checkouts: @fixture/lib -> ${local}`);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
          expect(readlinkSync(join(root, UNLINKED))).toBe('../../util');
          // The root depends on neither, so its tree gains nothing.
          expect(existsSync(join(root, 'node_modules', '@fixture', 'lib'))).toBe(false);
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);

          // An install that reinstalls the package keeps the link.
          await touchManifest(root);
          expect((await enter()).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
        },
      );
    });

    it('keeps an inherited absolute sibling link even when the declaration cannot resolve at this checkout depth', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../provider/lib' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          const sibling = await checkout(join(dirname(root), 'donor-side/provider/lib'), '@fixture/lib');
          await rm(join(root, LINKED));
          symlinkSync(sibling, join(root, LINKED));
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(sibling);
          await touchManifest(root);
          expect((await enter()).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(sibling);
        },
      );
    });

    it('selects contained package directories in this checkout, not the main checkout', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '.cache/owned-package' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
          const main = join(dirname(root), 'projects', 'main');
          await checkout(join(main, '.cache', 'owned-package'), '@fixture/lib');
          const legacy = await checkout(join(main, '.cache', 'package'), '@fixture/lib');
          await mkdir(join(root, '.cowshed'), { recursive: true });
          await writeFile(join(root, '.cowshed', 'workspace.json'), JSON.stringify({ projectRoot: main }));

          await rm(join(root, LINKED));
          symlinkSync(legacy, join(root, LINKED));
          // A missing local directory refuses an inherited Main binding, without rewriting it.
          const missing = await enter({ links: true, check: true });
          expect(missing.exitCode).toBe(1);
          expect(missing.stderr).toContain('restore it in this checkout, never borrow the main checkout');
          expect(readlinkSync(join(root, LINKED))).toBe(legacy);
          const local = await checkout(join(root, '.cache', 'owned-package'), '@fixture/lib');
          // The inherited Main payload need not share the declaration's new relative path.
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(relative(dirname(join(root, LINKED)), local));
          expect(readFileSync(join(legacy, 'package.json'), 'utf8')).toContain('@fixture/lib');
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
          // The explicit declaration is authoritative: an external binding to an independent checkout is
          // repointed to the declared local product.
          const independent = await checkout(join(dirname(root), 'independent-lib'), '@fixture/lib');
          await rm(join(root, LINKED));
          symlinkSync(independent, join(root, LINKED));
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(relative(dirname(join(root, LINKED)), local));
        },
      );
    });

    it('refuses a contained declaration whose existing ancestor escapes through a symlink', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '.cache/owned-package' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          const foreign = join(dirname(root), 'foreign-packages');
          await mkdir(foreign);
          symlinkSync(foreign, join(root, '.cache'));
          // The final component does not exist: the existing ancestor still proves the escape.
          const refused = await enter({ links: true });
          expect(refused.exitCode).toBe(1);
          expect(refused.stderr).toContain('escapes its owning checkout');
          expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
        },
      );
    });

    it("keeps a developer's own link and diagnoses an absent declared checkout on application", async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../absent-lib', util: '../declared-util' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          await checkout(join(dirname(root), 'declared-util'), 'util');
          const mine = await checkout(join(dirname(root), 'my-util'), 'util');
          await rm(join(root, UNLINKED));
          symlinkSync(mine, join(root, UNLINKED));
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
          const entered = await enter({ links: true });
          expect(entered.exitCode).toBe(0);
          expect(entered.stderr).toContain(
            `! smoo.developerLinks @fixture/lib: ${join(dirname(root), 'absent-lib')} does not exist on this machine; the installed version stays`,
          );
          expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
        },
      );
    });

    it('replaces an old declared binding when the declaration changes to another checkout', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../lib-a' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          const a = await checkout(join(dirname(root), 'lib-a'), '@fixture/lib');
          const b = await checkout(join(dirname(root), 'lib-b'), '@fixture/lib');
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(a);

          const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
          manifest.smoo.developerLinks['@fixture/lib'] = '../lib-b';
          await writeFile(join(root, 'package.json'), JSON.stringify(manifest));
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(b);
        },
      );
    });

    it('keeps an absent declared checkout as an unavailable finding, leaving the existing absolute binding bytes unchanged', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../absent-lib' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          const sibling = await checkout(join(dirname(root), 'donor-side/lib'), '@fixture/lib');
          await rm(join(root, LINKED));
          symlinkSync(sibling, join(root, LINKED));
          const check = await enter({ links: true, check: true });
          expect(check.exitCode).toBe(1);
          expect(check.stderr).toContain('does not exist on this machine');
          const applied = await enter({ links: true });
          expect(applied.exitCode).toBe(0);
          expect(applied.stderr).toContain('does not exist on this machine');
          expect(readlinkSync(join(root, LINKED))).toBe(sibling);
        },
      );
    });

    it('reports a present checkout without a package.json as unavailable, never as an unhandled ENOENT', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../bare-lib' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          await mkdir(join(dirname(root), 'bare-lib'), { recursive: true });
          const entered = await enter({ links: true });
          expect(entered.exitCode).toBe(0);
          expect(entered.stderr).toContain('has no package.json here');
          expect(entered.stderr).not.toContain('ENOENT');
          const check = await enter({ links: true, check: true });
          expect(check.exitCode).toBe(1);
          expect(check.stderr).toContain('has no package.json here');
          expect(check.stderr).not.toContain('ENOENT');
        },
      );
    });

    it('links nothing on a CI runner, which builds against the published packages', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../local-lib' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
          const runner = { SMOO_NPM_TOKEN: 'npm', SMOO_TOKEN: 'token' };
          expect(await enter({ links: true, check: true, ciSecrets: runner })).toEqual(HEALTHY);
          expect(await enter({ links: true, ciSecrets: runner })).toEqual(HEALTHY);
          expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
        },
      );
    });

    it('refuses a declaration whose directory is absolute', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '/opt/lib' } },
        async ({ enterShell: enter }) => {
          // The root postinstall reads the same declaration, so the install shell entry runs refuses it too.
          expect((await enter()).stderr).toContain(
            'smoo.developerLinks @fixture/lib must be a directory relative to the main checkout, not "/opt/lib"',
          );
          const refused = await enter({ links: true, check: true });
          expect(refused.exitCode).toBe(1);
          expect(refused.stderr).toContain(
            'smoo.developerLinks @fixture/lib must be a directory relative to this checkout, not "/opt/lib"',
          );
        },
      );
    });

    it('reconciles a changed managed declaration without reinstalling and preserves a manual retarget', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../first-lib', util: '../first-util' } },
        async ({ root, enterShell: enter, count }) => {
          const first = await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
          await checkout(join(dirname(root), 'first-util'), 'util');
          expect((await enter()).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(first);
          const mine = await checkout(join(dirname(root), 'my-util'), 'util');
          await rm(join(root, UNLINKED));
          symlinkSync(mine, join(root, UNLINKED));
          const next = await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
          await checkout(join(dirname(root), 'next-util'), 'util');
          const manifestPath = join(root, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          manifest.smoo.developerLinks = { '@fixture/lib': '../next-lib', util: '../next-util' };
          await writeFile(manifestPath, JSON.stringify(manifest));
          const installs = count('install');

          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(next);
          expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(count('install')).toBe(installs);
          expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
        },
      );
    });

    it('keeps changed managed declarations after an install instead of restoring the previous target', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../first-lib', util: '../declared-util' } },
        async ({ root, enterShell: enter }) => {
          await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
          await checkout(join(dirname(root), 'declared-util'), 'util');
          expect((await enter()).exitCode).toBe(0);
          const mine = await checkout(join(dirname(root), 'my-util'), 'util');
          await rm(join(root, UNLINKED));
          symlinkSync(mine, join(root, UNLINKED));
          const next = await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
          const manifestPath = join(root, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          manifest.smoo.developerLinks['@fixture/lib'] = '../next-lib';
          await writeFile(manifestPath, JSON.stringify(manifest));

          expect((await enter()).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(next);
          expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
        },
      );
    });

    it('does not claim a pre-existing manual link just because it matches a declaration', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../first-lib' } },
        async ({ root, enterShell: enter }) => {
          expect((await enter()).exitCode).toBe(0);
          const first = await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
          await rm(join(root, LINKED));
          symlinkSync(first, join(root, LINKED));
          expect((await enter({ links: true })).exitCode).toBe(0);
          await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
          const manifestPath = join(root, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          manifest.smoo.developerLinks['@fixture/lib'] = '../next-lib';
          await writeFile(manifestPath, JSON.stringify(manifest));
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(first);
        },
      );
    });

    it('restores managed provenance with the link when an install fails after its postinstall', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../first-lib' } },
        async ({ root, enterShell: enter }) => {
          const first = await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
          expect((await enter()).exitCode).toBe(0);
          const next = await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
          const manifestPath = join(root, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          manifest.smoo.developerLinks['@fixture/lib'] = '../next-lib';
          await writeFile(manifestPath, JSON.stringify(manifest));
          await expect(
            keepDeveloperLinks(root, async () => {
              await Bun.$`bun install --no-summary`.cwd(root).quiet();
              throw new Error('fixture install rejected after postinstall');
            }),
          ).rejects.toThrow('fixture install rejected after postinstall');
          expect(readlinkSync(join(root, LINKED))).toBe(first);
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(next);
        },
      );
    });

    it('carries managed provenance into a copied checkout for sibling and contained declarations', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../first-lib', util: '.cache/first-util' } },
        async (original) => {
          const first = await checkout(join(dirname(original.root), 'first-lib'), '@fixture/lib');
          const firstUtil = await checkout(join(original.root, '.cache', 'first-util'), 'util');
          expect((await original.enterShell()).exitCode).toBe(0);
          const clone = join(dirname(original.root), 'clone');
          await cp(original.root, clone, { recursive: true, verbatimSymlinks: true });
          await mkdir(join(clone, '.cowshed'), { recursive: true });
          await writeFile(join(clone, '.cowshed', 'workspace.json'), JSON.stringify({ projectRoot: original.root }));
          const next = await checkout(join(dirname(original.root), 'next-lib'), '@fixture/lib');
          const nextUtil = await checkout(join(clone, '.cache', 'next-util'), 'util');
          const manifestPath = join(clone, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          manifest.smoo.developerLinks = { '@fixture/lib': '../next-lib', util: '.cache/next-util' };
          await writeFile(manifestPath, JSON.stringify(manifest));
          const copied = repository(
            clone,
            join(dirname(original.root), 'ledgers'),
            join(dirname(original.root), 'bin'),
          );
          expect((await copied.enterShell({ links: true, check: true })).exitCode).toBe(1);
          expect((await copied.enterShell({ links: true })).exitCode).toBe(0);
          expect(realpathSync(join(clone, LINKED))).toBe(next);
          expect(realpathSync(join(clone, UNLINKED))).toBe(nextUtil);
          expect((await copied.enterShell({ links: true, check: true })).exitCode).toBe(0);
          expect(realpathSync(join(original.root, LINKED))).toBe(first);
          expect(realpathSync(join(original.root, UNLINKED))).toBe(firstUtil);
        },
      );
    });

    it('keeps the declared link across a plain bun install, through the root postinstall', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../local-lib' } },
        async ({ root, enterShell: enter }) => {
          const local = await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
          expect((await enter()).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
          // `bun install` as a developer types it: no shell entry and no install lock.
          const install = () =>
            Bun.spawnSync({
              cmd: ['bun', 'install'],
              cwd: root,
              env: { PATH: process.env.PATH, HOME: process.env.HOME, SMOO_NPM_TOKEN: 'npm' },
              stdout: 'pipe',
              stderr: 'pipe',
            }).exitCode;

          // Bun re-points a link nothing declares at its lockfile version, and the postinstall puts the declared one back.
          const mine = await checkout(join(dirname(root), 'my-util'), 'util');
          await rm(join(root, UNLINKED));
          symlinkSync(mine, join(root, UNLINKED));
          expect(install()).toBe(0);
          expect(readlinkSync(join(root, UNLINKED))).toBe('../../util');
          expect(readlinkSync(join(root, LINKED))).toBe(local);

          // An install with nothing to do runs the postinstall as well: the entry holds the lockfile's version and ends linked.
          await rm(join(root, LINKED));
          symlinkSync('../../../lib', join(root, LINKED));
          expect(install()).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
        },
      );
    });

    it('removes an undeclared managed link at the next install and preserves a manual retarget', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../local-lib', util: '../declared-util' } },
        async ({ root, enterShell: enter }) => {
          await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
          await checkout(join(dirname(root), 'declared-util'), 'util');
          expect((await enter()).exitCode).toBe(0);
          const mine = await checkout(join(dirname(root), 'my-util'), 'util');
          await rm(join(root, UNLINKED));
          symlinkSync(mine, join(root, UNLINKED));
          const manifestPath = join(root, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          delete manifest.smoo.developerLinks;
          await writeFile(manifestPath, JSON.stringify(manifest));

          const entered = await enter();
          expect(entered.exitCode).toBe(0);
          expect(entered.stderr).toContain(
            `! removed developer link ${LINKED}: smoo.developerLinks no longer declares its package`,
          );
          expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
          expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
          expect(JSON.parse(readFileSync(join(root, 'node_modules', '.smoo-declared-links'), 'utf8'))).toEqual([]);
        },
      );
    });

    it('keeps an undeclared unrecorded manual link even when it matched the old declaration', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../local-lib' } },
        async ({ root, enterShell: enter }) => {
          expect((await enter()).exitCode).toBe(0);
          const local = await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
          await rm(join(root, LINKED));
          symlinkSync(local, join(root, LINKED));
          expect((await enter({ links: true })).exitCode).toBe(0);
          const manifestPath = join(root, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          delete manifest.smoo.developerLinks;
          await writeFile(manifestPath, JSON.stringify(manifest));

          expect((await enter()).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
        },
      );
    });

    it('restores an undeclared managed link and its receipt when the install fails', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../local-lib' } },
        async ({ root, enterShell: enter }) => {
          const local = await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
          expect((await enter()).exitCode).toBe(0);
          const receiptPath = join(root, 'node_modules', '.smoo-declared-links');
          const receipt = readFileSync(receiptPath, 'utf8');
          const manifestPath = join(root, 'package.json');
          const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
          delete manifest.smoo.developerLinks;
          await writeFile(manifestPath, JSON.stringify(manifest));
          await expect(
            keepDeveloperLinks(root, async () => {
              await Bun.$`bun install --no-summary`.cwd(root).quiet();
              throw new Error('fixture undeclared install failed');
            }),
          ).rejects.toThrow('fixture undeclared install failed');
          expect(readlinkSync(join(root, LINKED))).toBe(local);
          expect(readFileSync(receiptPath, 'utf8')).toBe(receipt);

          const next = await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
          manifest.smoo.developerLinks = { '@fixture/lib': '../next-lib' };
          await writeFile(manifestPath, JSON.stringify(manifest));
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(next);
        },
      );
    });
  });

  describe('across an install meant to replace some of them', () => {
    // `app` and a second member, `web`, declare both packages. The root declares neither, so a developer links
    // `@fixture/lib` there by hand: three trees hold it and two hold `util`, and a name selects links in all of them.
    const APP_LIB = LINKED;
    const APP_UTIL = UNLINKED;
    const WEB = 'packages/web';
    const WEB_LIB = `${WEB}/node_modules/@fixture/lib`;
    const WEB_UTIL = `${WEB}/node_modules/util`;
    const ROOT_LIB = 'node_modules/@fixture/lib';
    const WITH_WEB = {
      ...WORKSPACE,
      files: {
        ...WORKSPACE.files,
        [`${WEB}/package.json`]: JSON.stringify({
          name: 'web',
          version: '0.0.0',
          dependencies: { '@fixture/lib': 'workspace:*', util: 'workspace:*' },
        }),
      },
    };
    /** What an install puts at each declared dependency: the lockfile's version. */
    const LOCKFILE: Readonly<Record<string, string>> = {
      [APP_LIB]: '../../../lib',
      [APP_UTIL]: '../../util',
      [WEB_LIB]: '../../../lib',
      [WEB_UTIL]: '../../util',
    };

    interface Developer {
      readonly root: string;
      /** The local checkouts the developer linked to. */
      readonly lib: string;
      readonly util: string;
      /** Every link's text, as the developer wrote it. */
      readonly original: Readonly<Record<string, string>>;
      /** Every link's text now. */
      readonly texts: () => Record<string, string>;
    }

    /** An installed workspace in which a developer has linked both packages, in every tree that has them. */
    async function withDeveloperLinks(run: (developer: Developer) => Promise<void>): Promise<void> {
      await withManagedRepository(WITH_WEB, async ({ root, enterShell: enter }) => {
        expect(await enter()).toEqual(HEALTHY);
        const lib = join(dirname(root), 'local-lib');
        const util = join(dirname(root), 'local-util');
        await mkdir(lib);
        await mkdir(util);
        const original = {
          [ROOT_LIB]: lib,
          [APP_LIB]: lib,
          [WEB_LIB]: lib,
          // Relative to its own directory: a link put back must keep its text, not only its target.
          [APP_UTIL]: '../../../../local-util',
          [WEB_UTIL]: util,
        };
        for (const [entry, target] of Object.entries(original)) {
          await linkEntry(root, entry, target);
        }
        const texts = () =>
          Object.fromEntries(
            Object.keys(original).map((entry): [string, string] => [entry, readlinkSync(join(root, entry))]),
          );
        await run({ root, lib, util, original, texts });
      });
    }

    /** A package-manager replacement of declared entries, followed by selected local targets. */
    function replaceDeclaredLinks(root: string, linked: Readonly<Record<string, string>>): () => Promise<void> {
      return async () => {
        for (const [entry, lockfile] of Object.entries(LOCKFILE)) {
          await linkEntry(root, entry, lockfile);
        }
        for (const [entry, checkout] of Object.entries(linked)) {
          await linkEntry(root, entry, checkout);
        }
      };
    }

    it('keeps the scoped package an install relinked in every tree, and every other link as it was', async () => {
      await withDeveloperLinks(async ({ root, original, texts }) => {
        const provider = join(dirname(root), 'provider-lib');
        await mkdir(provider);
        const relinked = { [ROOT_LIB]: provider, [APP_LIB]: provider, [WEB_LIB]: provider };

        await keepDeveloperLinks(root, replaceDeclaredLinks(root, relinked), { relink: ['@fixture/lib'] });

        expect(texts()).toEqual({ ...original, ...relinked });
      });
    });

    it('keeps the unscoped package an install relinked, and puts back a scoped one sharing its basename', async () => {
      await withDeveloperLinks(async ({ root, original, texts }) => {
        const provider = join(dirname(root), 'provider-util');
        await mkdir(provider);
        const relinked = { [APP_UTIL]: provider, [WEB_UTIL]: provider };

        // Every install re-points `@fixture/lib` at the lockfile too. `lib` is the basename of that package, not
        // its name, so naming it selects nothing and the scoped links are put back.
        await keepDeveloperLinks(root, replaceDeclaredLinks(root, relinked), { relink: ['util', 'lib'] });

        expect(texts()).toEqual({ ...original, ...relinked });
      });
    });

    it.skipIf(process.platform === 'win32' || process.getuid?.() === 0)(
      'retains developer links when their target metadata is denied',
      async () => {
        await withDeveloperLinks(async ({ root, texts }) => {
          const blocked = join(dirname(root), 'blocked-provider');
          const provider = join(blocked, 'package');
          await mkdir(provider, { recursive: true });
          await linkEntry(root, APP_LIB, provider);
          const before = texts();
          await chmod(blocked, 0);
          try {
            await expect(
              keepDeveloperLinks(root, async () => {
                await Bun.$`bun install --no-summary`.cwd(root).quiet();
              }),
            ).rejects.toHaveProperty('code', 'EACCES');
            expect(texts()).toEqual(before);
          } finally {
            await chmod(blocked, 0o700);
          }
        });
      },
    );

    it('puts every link back, the named packages included, when the install fails after relinking', async () => {
      await withDeveloperLinks(async ({ root, original, texts }) => {
        const provider = join(dirname(root), 'provider');
        await mkdir(provider);
        const relink = replaceDeclaredLinks(root, {
          [ROOT_LIB]: provider,
          [APP_LIB]: provider,
          [WEB_LIB]: provider,
          [APP_UTIL]: provider,
          [WEB_UTIL]: provider,
        });

        const failure = new Error('package-manager relink failed');
        await expect(
          keepDeveloperLinks(
            root,
            async () => {
              await relink();
              throw failure;
            },
            { relink: ['@fixture/lib', 'util'] },
          ),
        ).rejects.toBe(failure);
        expect(texts()).toEqual(original);
      });
    });

    it('restores captured links without hiding a new external entry left by a failed relink', async () => {
      await withDeveloperLinks(async ({ root, original, texts }) => {
        const provider = join(dirname(root), 'new-provider');
        await mkdir(provider);
        const created = 'node_modules/@fixture/new';
        const failure = new Error('package-manager partial relink failed');
        await expect(
          keepDeveloperLinks(
            root,
            async () => {
              await linkEntry(root, created, provider);
              await linkEntry(root, APP_LIB, provider);
              throw failure;
            },
            { relink: ['@fixture/lib', '@fixture/new'] },
          ),
        ).rejects.toBe(failure);
        expect(texts()).toEqual(original);
        expect(readlinkSync(join(root, created))).toBe(provider);
      });
    });
  });

  describe('Nx bindings a developer pointed outside the checkout', () => {
    // `app` and the root both depend on the workspace member `nx`, so bun binds each `nx` entry to that member by
    // relative path, as it does `util`. A developer's `nx` entries point at a retired provider outside the checkout.
    const NX = `${APP}/node_modules/nx`;
    const NX_BINDING = '../../nx';
    const ROOT_NX = 'node_modules/nx';
    const ROOT_NX_BINDING = '../packages/nx';
    /** Each Nx entry, with the target a fresh install binds it to. */
    const NX_BINDINGS: Readonly<Record<string, string>> = { [ROOT_NX]: ROOT_NX_BINDING, [NX]: NX_BINDING };
    const WITH_NX = {
      ...WORKSPACE,
      dependencies: { nx: 'workspace:*' },
      files: {
        ...WORKSPACE.files,
        [`${APP}/package.json`]: JSON.stringify({
          name: 'app',
          version: '0.0.0',
          dependencies: { '@fixture/lib': 'workspace:*', nx: 'workspace:*', util: 'workspace:*' },
        }),
        'packages/nx/package.json': JSON.stringify({ name: 'nx', version: '0.0.0' }),
      },
    };
    const RUNNER = { SMOO_NPM_TOKEN: 'npm', SMOO_TOKEN: 'token' };

    /** A checkout a developer keeps outside this one: a directory holding its manifest. */
    async function providerCheckout(at: string, name: string): Promise<string> {
      await mkdir(at, { recursive: true });
      await writeFile(join(at, 'package.json'), JSON.stringify({ name, version: '9.9.9' }));
      return at;
    }

    /** Each Nx entry, root and app, bound by relative path to its member, as a fresh install leaves it. */
    function expectNxBindings(root: string): void {
      for (const [entry, binding] of Object.entries(NX_BINDINGS)) {
        expect(readlinkSync(join(root, entry))).toBe(binding);
      }
    }

    it.each(Object.keys(NX_BINDINGS))(
      'the shell link task repairs an external %s without changed install inputs',
      async (entry) => {
        await withManagedRepository(WITH_NX, async ({ root, enterShell: enter, count }) => {
          expect((await enter()).exitCode).toBe(0);
          expect(count('install')).toBe(1);
          const stamp = readFileSync(join(root, 'node_modules', '.smoo-install'), 'utf8');
          const retired = await providerCheckout(join(dirname(root), 'retired-nx'), 'nx');
          const local = await providerCheckout(join(dirname(root), 'local-lib'), '@fixture/lib');
          await linkEntry(root, entry, retired);
          await linkEntry(root, LINKED, local);

          // devenv's install inputs did not change; only the link task's status runs.
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          expect(count('install')).toBe(1);
          expect(readlinkSync(join(root, entry))).toBe(retired);
          expect(readFileSync(join(root, 'node_modules', '.smoo-install'), 'utf8')).toBe(stamp);

          const repaired = await Promise.all([enter({ links: true }), enter({ links: true })]);
          expect(repaired.map(({ exitCode }) => exitCode)).toEqual([0, 0]);
          expect(count('install')).toBe(2);
          expectNxBindings(root);
          expect(readlinkSync(join(root, LINKED))).toBe(local);

          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(count('install')).toBe(2);
          expectNxBindings(root);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
        });
      },
    );

    it('reports a failed shell link repair and keeps its status stale', async () => {
      await withManagedRepository(
        {
          ...WITH_NX,
          prepare: 'test ! -f refuse-install || { echo "fixture-install-refused" >&2; exit 19; }',
        },
        async ({ root, enterShell: enter }) => {
          expect((await enter()).exitCode).toBe(0);
          const stamp = readFileSync(join(root, 'node_modules', '.smoo-install'), 'utf8');
          const retired = await providerCheckout(join(dirname(root), 'retired-nx'), 'nx');
          const local = await providerCheckout(join(dirname(root), 'local-lib'), '@fixture/lib');
          for (const entry of Object.keys(NX_BINDINGS)) await linkEntry(root, entry, retired);
          await linkEntry(root, LINKED, local);
          await writeFile(join(root, 'refuse-install'), '');

          const failed = await enter({ links: true });
          expect(failed.exitCode).toBe(1);
          expect(failed.stderr).toContain('fixture-install-refused');
          for (const entry of Object.keys(NX_BINDINGS)) expect(readlinkSync(join(root, entry))).toBe(retired);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
          expect(readFileSync(join(root, 'node_modules', '.smoo-install'), 'utf8')).toBe(stamp);
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
        },
      );
    });

    it('replaces external Nx entries that a matching install stamp would keep', async () => {
      await withManagedRepository(WITH_NX, async ({ root, enterShell: enter, count }) => {
        expect((await enter()).exitCode).toBe(0);
        expect(count('install')).toBe(1);
        expectNxBindings(root);

        // Nothing dangles and the stamp still matches: only the entry can see that Nx left the checkout.
        const retired = await providerCheckout(join(dirname(root), 'retired-nx'), 'nx');
        const local = await providerCheckout(join(dirname(root), 'local-lib'), '@fixture/lib');
        for (const entry of Object.keys(NX_BINDINGS)) await linkEntry(root, entry, retired);
        await linkEntry(root, LINKED, local);

        expect((await enter()).exitCode).toBe(0);
        expect(count('install')).toBe(2);
        expectNxBindings(root);
        expect(readlinkSync(join(root, LINKED))).toBe(local);

        // The bindings are current now, so a hot second entry installs nothing.
        expect((await enter()).exitCode).toBe(0);
        expect(count('install')).toBe(2);
        expectNxBindings(root);
      });
    });

    it("replaces external Nx entries on a CI runner's frozen install", async () => {
      await withManagedRepository(WITH_NX, async ({ root, enterShell: enter }) => {
        // The first entry writes the lockfile that the frozen install must match.
        expect((await enter()).exitCode).toBe(0);
        const retired = await providerCheckout(join(dirname(root), 'retired-nx'), 'nx');
        const local = await providerCheckout(join(dirname(root), 'local-lib'), '@fixture/lib');
        for (const entry of Object.keys(NX_BINDINGS)) await linkEntry(root, entry, retired);
        await linkEntry(root, LINKED, local);

        expect((await enter({ ciSecrets: RUNNER })).exitCode).toBe(0);
        expectNxBindings(root);
        expect(readlinkSync(join(root, LINKED))).toBe(local);
      });
    });

    it('refuses a declared Nx link and leaves both Nx bindings where the install put them', async () => {
      await withManagedRepository(WITH_NX, async ({ root, enterShell: enter }) => {
        expect(await enter()).toEqual(HEALTHY);
        expectNxBindings(root);

        // A declared Nx checkout would repoint Bun's binding in postinstall, so smoo refuses the declaration by name.
        const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
        await edit(
          join(root, 'package.json'),
          JSON.stringify({ ...manifest, smoo: { ...manifest.smoo, developerLinks: { nx: '.cache/retired-nx' } } }),
        );

        const checked = await enter({ links: true, check: true });
        expect(checked.exitCode).toBe(1);
        expect(checked.stderr).toContain('smoo.developerLinks.nx');
        expect(checked.stderr).toContain('smoo monorepo update');
        expectNxBindings(root);

        const linked = await enter({ links: true });
        expect(linked.exitCode).toBe(1);
        expect(linked.stderr).toContain('smoo monorepo update');
        expectNxBindings(root);

        // The ordinary entry's install reads the same declaration in postinstall; it must not repoint Nx either.
        await enter();
        expectNxBindings(root);
      });
    });

    it("keeps captured Nx and sibling links when a CI runner's install fails", async () => {
      await withManagedRepository(WITH_NX, async ({ root, enterShell: enter }) => {
        // The first entry writes the lockfile and stamp that a healthy install leaves.
        expect(await enter()).toEqual(HEALTHY);
        const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
        await edit(
          join(root, 'package.json'),
          JSON.stringify({ ...manifest, scripts: { ...manifest.scripts, prepare: 'exit 1' } }),
        );
        const retired = await providerCheckout(join(dirname(root), 'retired-nx'), 'nx');
        const local = await providerCheckout(join(dirname(root), 'local-lib'), '@fixture/lib');
        for (const entry of Object.keys(NX_BINDINGS)) await linkEntry(root, entry, retired);
        await linkEntry(root, LINKED, local);

        // Frozen install fails on prepare, its existing fallback fails too, and the entry reports the refusal.
        const failed = await enter({ ciSecrets: RUNNER });
        expect(failed.exitCode).toBe(1);
        expect(failed.stderr).toContain('! Failed to install dependencies with frozen lockfile');
        expect(failed.stderr).toContain('--- ERROR: setup-environment.ts failed while running: bun install');
        expect(failed.stderr).toContain('exit code: 1');
        for (const entry of Object.keys(NX_BINDINGS)) expect(readlinkSync(join(root, entry))).toBe(retired);
        expect(readlinkSync(join(root, LINKED))).toBe(local);
      });
    });
  });
});

describe('what shell entry syncs for a uv project', () => {
  const PYTHON = '/nix/store/python3-env/bin/python3';
  const UV_PROJECT = {
    'pyproject.toml': '[project]\nname = "workspace"\n\n[tool.uv.workspace]\nmembers = ["python/*"]\n',
    'python/tool/pyproject.toml': '[project]\nname = "tool"\n',
    'python/tool/src/tool/__init__.py': '',
    'uv.lock': 'version = 1\n',
  };

  it('creates a relocatable environment where devenv names it and syncs it once per input change', async () => {
    await withManagedRepository({ files: UV_PROJECT }, async ({ root, state, enterShell: enter, uvRuns }) => {
      const venv = join(state, 'venv');
      const uv = { uvProjectEnvironment: venv, python: PYTHON };
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(await enter(uv)).toEqual(HEALTHY);
      const sync = ['sync', '--python', PYTHON, '--all-packages', '--all-groups'];
      expect(uvRuns()).toEqual([['venv', '--relocatable', '--python', PYTHON, venv], sync]);

      await edit(join(root, 'python/tool/pyproject.toml'), '[project]\nname = "tool"\ndependencies = ["attrs"]\n');
      expect(await enter(uv)).toEqual(HEALTHY);
      await edit(join(root, 'uv.lock'), 'version = 1\nrevision = 2\n');
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(uvRuns().slice(2)).toEqual([sync, sync]);
    });
  });

  it('syncs nothing for a shell that does not enable uv, even under an outer shell’s environment', async () => {
    await withManagedRepository({ files: UV_PROJECT }, async ({ state, enterShell: enter, uvRuns }) => {
      // Entered from another repository's uv shell: its variable is inherited,
      // but this shell's module passed no interpreter.
      expect(await enter({ uvProjectEnvironment: join(state, 'outer-venv') })).toEqual(HEALTHY);
      expect(uvRuns()).toEqual([]);
    });
  });

  it('enters a copied checkout installed, its workspace members importing from the copy', async () => {
    await withManagedRepository({ files: UV_PROJECT }, async (original) => {
      expect(await original.enterShell({ uvProjectEnvironment: join(original.state, 'venv'), python: PYTHON })).toEqual(
        HEALTHY,
      );
      expect(original.count('install')).toBe(1);
      expect(original.uvRuns()).toHaveLength(2);

      // A copy-on-write clone: every byte, ignored state included, at another path.
      const clone = join(dirname(original.root), 'clone');
      await cp(original.root, clone, { recursive: true, verbatimSymlinks: true });
      const venv = join(clone, 'tooling', 'direnv', '.devenv', 'state', 'venv');
      const copied = repository(clone, join(dirname(original.root), 'ledgers'), join(dirname(original.root), 'bin'));

      expect(await copied.enterShell({ uvProjectEnvironment: venv, python: PYTHON })).toEqual(HEALTHY);
      expect(copied.uvRuns()).toHaveLength(2);
      expect(copied.count('install')).toBe(1);
      // Python's site resolves a relative .pth line against site-packages.
      const site = join(venv, 'lib', 'python3.14', 'site-packages');
      expect(resolve(site, readFileSync(join(site, 'tool.pth'), 'utf8').trim())).toBe(join(clone, 'python/tool/src'));
      expect(readFileSync(join(site, '_virtualenv.pth'), 'utf8')).toBe('import _virtualenv\n');
    });
  });

  it('replaces an environment that is not relocatable', async () => {
    await withManagedRepository({ files: UV_PROJECT }, async ({ state, enterShell: enter, uvRuns }) => {
      const venv = join(state, 'venv');
      await mkdir(venv, { recursive: true });
      await writeFile(join(venv, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
      await writeFile(join(venv, 'built-by-an-older-shell'), '');
      expect(await enter({ uvProjectEnvironment: venv, python: PYTHON })).toEqual(HEALTHY);
      expect(existsSync(join(venv, 'built-by-an-older-shell'))).toBe(false);
      expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8')).toContain('relocatable = true');
      expect(uvRuns().map(([verb]) => verb)).toEqual(['venv', 'sync']);
    });
  });
});

describe('what shell entry fetches for a Cargo workspace', () => {
  // The Nx plugin resolves a Cargo workspace wherever a project's Cargo.toml declares
  // `[workspace]`: here the root and one member. Another member's plain package and the
  // root workspace's crate are not workspaces of their own.
  const CARGO_PROJECT = {
    'Cargo.toml': '[workspace]\nmembers = ["crates/*"]\n',
    'Cargo.lock': 'version = 4\n',
    'crates/core/Cargo.toml': '[package]\nname = "core"\n',
    'packages/native/package.json': JSON.stringify({ name: 'native', version: '0.0.0' }),
    'packages/native/Cargo.toml': '[workspace]\nmembers = ["."]\n\n[package]\nname = "native"\n',
    'packages/native/Cargo.lock': 'version = 4\n',
    'packages/plain/package.json': JSON.stringify({ name: 'plain', version: '0.0.0' }),
    'packages/plain/Cargo.toml': '[package]\nname = "plain"\n',
  };
  const fetch = (manifest: string) => ['fetch', '--locked', '--manifest-path', manifest];
  /** What Cargo itself writes at the root of CARGO_HOME: cowshed's STATE_FILES, the exact files it grants a sandbox. */
  const CARGO_STATE_FILES = ['.package-cache', '.package-cache-mutate', '.global-cache', '.global-cache-journal'];
  /** The stamps shell entry left in the private XDG cache: smoo's only record of a fetch. */
  const stamps = (xdgCache: string) => {
    const directory = join(xdgCache, 'smoo', 'cargo-fetched');
    return existsSync(directory) ? readdirSync(directory) : [];
  };

  // Shell entry's installs never fetch: devenv's smoo:cargo-fetch task runs `--cargo`, gated by `--cargo --check`.
  const CARGO = { cargo: true } as const;

  it('installs without fetching; the cargo task fetches each workspace once per change to what the fetch reads', async () => {
    await withManagedRepository(
      { workspaces: ['packages/*'], files: CARGO_PROJECT },
      async ({ root, xdgCache, enterShell: enter, cargoRuns }) => {
        expect(await enter()).toEqual(HEALTHY);
        expect(cargoRuns()).toEqual([]);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns()).toEqual([fetch('Cargo.toml'), fetch('packages/native/Cargo.toml')]);
        expect(stamps(xdgCache)).toHaveLength(2);

        await edit(join(root, 'Cargo.lock'), 'version = 4\n\n[[package]]\nname = "arrow-ipc"\n');
        expect(await enter(CARGO)).toEqual(HEALTHY);
        // Adding Cargo configuration changes what the fetch reads as much as editing it does.
        await mkdir(join(root, 'packages/native/.cargo'));
        await edit(join(root, 'packages/native/.cargo/config.toml'), '[net]\noffline = false\n');
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns().slice(2)).toEqual([fetch('Cargo.toml'), fetch('packages/native/Cargo.toml')]);
      },
    );
  });

  it('reports through --check whether CARGO_HOME holds every workspace, so a fresh one fetches', async () => {
    await withManagedRepository({ files: CARGO_PROJECT }, async (original) => {
      expect((await original.enterShell({ cargo: true, check: true })).exitCode).toBe(1);
      expect(await original.enterShell(CARGO)).toEqual(HEALTHY);
      expect(await original.enterShell({ cargo: true, check: true })).toEqual(HEALTHY);

      // A copy of the checkout sharing CARGO_HOME is fetched already.
      const clone = join(dirname(original.root), 'clone');
      await cp(original.root, clone, { recursive: true, verbatimSymlinks: true });
      const copied = repository(clone, join(dirname(original.root), 'ledgers'), join(dirname(original.root), 'bin'));
      expect(await copied.enterShell({ cargo: true, check: true })).toEqual(HEALTHY);

      await rm(original.cargoHome, { recursive: true, force: true });
      expect((await original.enterShell({ cargo: true, check: true })).exitCode).toBe(1);
      expect(await original.enterShell(CARGO)).toEqual(HEALTHY);
      expect(original.cargoRuns()).toEqual([fetch('Cargo.toml'), fetch('Cargo.toml')]);
    });
  });

  it('fetches again into a recreated CARGO_HOME and into another one, each keeping its own stamp', async () => {
    await withManagedRepository({ files: CARGO_PROJECT }, async ({ cargoHome, enterShell: enter, cargoRuns }) => {
      expect(await enter(CARGO)).toEqual(HEALTHY);
      // Another CARGO_HOME holds none of what the first one fetched.
      expect(await enter({ ...CARGO, cargoHome: `${cargoHome}-other` })).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(2);
      // Neither fetch spoiled the other's stamp.
      expect(await enter(CARGO)).toEqual(HEALTHY);
      expect(await enter({ ...CARGO, cargoHome: `${cargoHome}-other` })).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(2);

      // The same path, deleted and created again: a stamp outside CARGO_HOME survives it, so what
      // it promises must not.
      await rm(cargoHome, { recursive: true });
      await mkdir(cargoHome);
      expect(await enter(CARGO)).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(3);
      expect(await enter(CARGO)).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(3);
    });
  });

  it('keeps its stamp in the private XDG cache, entering a CARGO_HOME that is writable only where Cargo writes', async () => {
    await withManagedRepository(
      { files: CARGO_PROJECT },
      async ({ cargoHome, xdgCache, enterShell: enter, cargoRuns }) => {
        // CARGO_HOME as a cowshed sandbox grants it: Cargo's state files and its `registry` and
        // `git` caches are writable, and nothing else — no entry may be created at its root.
        await mkdir(join(cargoHome, 'registry'), { recursive: true });
        await mkdir(join(cargoHome, 'git'));
        for (const file of CARGO_STATE_FILES) {
          await writeFile(join(cargoHome, file), '');
        }
        await chmod(cargoHome, 0o555);
        try {
          // The fixture is only a model of the grant when the root really refuses a new entry.
          await expect(writeFile(join(cargoHome, 'smoo-fetched'), '')).rejects.toThrow('EACCES');

          expect(await enter(CARGO)).toEqual(HEALTHY);
          expect(await enter(CARGO)).toEqual(HEALTHY);
          expect(cargoRuns()).toEqual([fetch('Cargo.toml')]);
          expect(readdirSync(cargoHome).sort()).toEqual([...CARGO_STATE_FILES, 'git', 'registry'].sort());
          expect(stamps(xdgCache)).toHaveLength(1);
        } finally {
          await chmod(cargoHome, 0o755);
        }
      },
    );
  });

  it('falls back to ~/.cache/smoo without XDG_CACHE_HOME, leaving ~/.cargo to Cargo', async () => {
    await withManagedRepository({ files: CARGO_PROJECT }, async ({ root, enterShell: enter, cargoRuns }) => {
      const home = join(dirname(root), 'home');
      await mkdir(home);
      expect(await enter({ ...CARGO, home })).toEqual(HEALTHY);
      expect(await enter({ ...CARGO, home })).toEqual(HEALTHY);
      expect(cargoRuns()).toEqual([fetch('Cargo.toml')]);
      expect(readdirSync(join(home, '.cargo')).sort()).toEqual(['.package-cache', 'registry']);
      expect(readdirSync(join(home, '.cache', 'smoo'))).toEqual(['cargo-fetched']);
      expect(readdirSync(join(home, '.cache', 'smoo', 'cargo-fetched'))).toHaveLength(1);
    });
  });

  it("loads the shell when the fetch fails, naming Cargo's cause, and fetches again on the next entry", async () => {
    await withManagedRepository(
      { files: { ...CARGO_PROJECT, 'cargo-fails': '' } },
      async ({ root, xdgCache, enterShell: enter, cargoRuns }) => {
        const failed = await enter(CARGO);
        expect(failed.exitCode).toBe(0);
        expect(failed.stderr).toContain('cargo fetch --locked --manifest-path Cargo.toml');
        expect(failed.stderr).toContain('failed to download from `https://index.crates.io/config.json`');
        expect((await enter(CARGO)).exitCode).toBe(0);
        expect(cargoRuns()).toEqual([fetch('Cargo.toml'), fetch('Cargo.toml')]);
        // A fetch that failed promised nothing: no stamp, not even the directory for one.
        expect(existsSync(join(xdgCache, 'smoo'))).toBe(false);

        await rm(join(root, 'cargo-fails'));
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns()).toHaveLength(3);
        expect(stamps(xdgCache)).toHaveLength(1);
      },
    );
  });

  it('fails a CI fetch that fails', async () => {
    await withManagedRepository(
      { files: { ...CARGO_PROJECT, 'cargo-fails': '' } },
      async ({ xdgCache, enterShell: enter, cargoRuns }) => {
        const entry = await enter({
          cargo: true,
          ciSecrets: { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' },
        });
        expect(entry.exitCode).toBe(1);
        expect(entry.stderr).toContain('failed to download from `https://index.crates.io/config.json`');
        expect(cargoRuns()).toEqual([fetch('Cargo.toml')]);
        expect(existsSync(join(xdgCache, 'smoo'))).toBe(false);
      },
    );
  });
});

describe('the workspace git config', () => {
  it('resolves in a copy of the checkout at another path that cannot read the original', async () => {
    await withManagedRepository({}, async (original) => {
      expect(await original.enterShell()).toEqual(HEALTHY);

      // A copy-on-write clone carries .git/config verbatim. In a sandbox the
      // checkout it was copied from is unreadable, and git refuses to run at
      // all when an include it names exists but cannot be read.
      const scratch = dirname(original.root);
      const clone = join(scratch, 'clone');
      await cp(original.root, clone, { recursive: true, verbatimSymlinks: true });
      await writeFile(join(clone, 'tooling', 'workspace.gitconfig'), '[smoo]\n\tcheckout = clone\n');
      const originalConfig = join(original.root, 'tooling', 'workspace.gitconfig');
      await chmod(originalConfig, 0o000);
      try {
        const copied = repository(clone, join(scratch, 'ledgers'), join(scratch, 'bin'));
        expect(await copied.enterShell()).toEqual(HEALTHY);
        // Each checkout reads its own workspace config through the one include.
        for (const [root, checkout] of [
          [clone, 'clone'],
          [original.root, 'original'],
        ]) {
          await chmod(originalConfig, 0o644);
          expect(await git(root, ['config', '--get-all', 'include.path'])).toBe('../tooling/workspace.gitconfig\n');
          expect(await git(root, ['config', 'smoo.checkout'])).toBe(`${checkout}\n`);
        }
      } finally {
        await chmod(originalConfig, 0o644);
      }
    });
  });

  it('keeps the shared primary include unchanged when a real linked worktree enters twice', async () => {
    await withManagedRepository({}, async (original) => {
      expect(await original.enterShell()).toEqual(HEALTHY);
      const config = join(original.root, '.git', 'config');
      const before = readFileSync(config, 'utf8');
      const writes = original.gitConfigWrites().length;
      await git(original.root, ['add', 'package.json', '.npmrc', '.envrc', 'tooling']);
      await git(original.root, [
        '-c',
        'user.name=smoo-fixture',
        '-c',
        'user.email=smoo-fixture@example.invalid',
        '-c',
        'commit.gpgsign=false',
        'commit',
        '--quiet',
        '-m',
        'seed managed checkout',
      ]);
      const scratch = dirname(original.root);
      const linked = join(scratch, 'linked');
      await git(original.root, ['worktree', 'add', '--quiet', '--detach', linked, 'HEAD']);
      await cp(join(original.root, 'node_modules'), join(linked, 'node_modules'), {
        recursive: true,
        verbatimSymlinks: true,
      });
      await writeFile(join(linked, 'tooling', 'workspace.gitconfig'), '[smoo]\n\tcheckout = linked\n');
      const entry = repository(linked, join(scratch, 'ledgers'), join(scratch, 'bin'));
      expect(await entry.enterShell()).toEqual(HEALTHY);
      expect(await entry.enterShell()).toEqual(HEALTHY);
      expect(readFileSync(config, 'utf8')).toBe(before);
      expect(original.gitConfigWrites()).toHaveLength(writes);
      await chmod(join(linked, 'tooling', 'workspace.gitconfig'), 0o000);
      try {
        expect(await git(original.root, ['config', '--get-all', 'include.path'])).toBe(
          '../tooling/workspace.gitconfig\n',
        );
        expect(await git(original.root, ['config', 'smoo.checkout'])).toBe('original\n');
        expect(await git(linked, ['config', 'smoo.checkout'])).toBe('original\n');
      } finally {
        await chmod(join(linked, 'tooling', 'workspace.gitconfig'), 0o644);
      }
    });
  });

  it('is written by one shell entry at a time when two start on a clean config', async () => {
    // Two shells entering one checkout at once — a pool warming a spare beside a command's own
    // activation — must not both write .git/config: git takes config.lock with O_EXCL, and
    // the entry that loses fails with "could not lock config file". Each write lingers so
    // the entries overlap; the race is between processes, so there is no clock to fake.
    await withManagedRepository({ gitWriteSeconds: 0.3 }, async ({ root, startShell, gitConfigWrites }) => {
      const entries = await Promise.all([finished(startShell()), finished(startShell())]);
      expect(entries.map((entry) => entry.exitCode)).toEqual([0, 0]);
      const writes = gitConfigWrites();
      expect(writes.length).toBeGreaterThan(0);
      for (const [index, line] of writes.entries()) {
        expect(line.startsWith(index % 2 === 0 ? 'begin ' : 'end ')).toBe(true);
      }
      expect(await git(root, ['config', 'merge.smoo-newer-pins.driver'])).toBe(
        'bash tooling/direnv/merge-newer-pins.sh %O %A %B %P\n',
      );
      expect(await git(root, ['config', '--get-all', 'include.path'])).toBe('../tooling/workspace.gitconfig\n');
    });
  });

  it('is not written by a shell entry that finds it already correct', async () => {
    await withManagedRepository({}, async ({ enterShell: enter, gitConfigWrites }) => {
      expect(await enter()).toEqual(HEALTHY);
      const first = gitConfigWrites().length;
      expect(first).toBeGreaterThan(0);
      expect(await enter()).toEqual(HEALTHY);
      expect(gitConfigWrites()).toHaveLength(first);
    });
  });

  it('loads the shell when its hooks cannot be written, naming the path and errno, and links them once they can', async () => {
    // A sandboxed job may not write .git/hooks: a cowshed workspace denies it. A hooks
    // directory without write permission is the same refusal outside a sandbox.
    await withManagedRepository({}, async ({ root, enterShell: enter }) => {
      const hooks = join(root, '.git', 'hooks');
      await chmod(hooks, 0o555);
      try {
        const refused = await enter();
        expect(refused.exitCode).toBe(0);
        expect(refused.stderr).toContain('--- WARNING: setup-environment.ts failed:');
        expect(refused.stderr).toContain('EACCES');
        expect(refused.stderr).toContain(join(hooks, 'pre-commit'));
        expect(refused.stderr).toContain(
          'Git hooks and repository config were not applied; the shell is loaded without them.',
        );
      } finally {
        await chmod(hooks, 0o755);
      }
      expect(await enter()).toEqual(HEALTHY);
      expect(readlinkSync(join(hooks, 'pre-commit'))).toBe('../../tooling/git-hooks/pre-commit.sh');
      await chmod(hooks, 0o555);
      try {
        expect(await enter()).toEqual(HEALTHY);
      } finally {
        await chmod(hooks, 0o755);
      }
    });
  });
});

describe('a shell direnv keeps loaded', () => {
  const MEMBER = 'packages/member/package.json';
  // Stands in for `use devenv` and the managed prologue: devenv's two
  // variables, then the script the prologue runs.
  const USE_DEVENV = [
    'export DEVENV_ROOT="$PWD"',
    'export DEVENV_STATE="$PWD/.devenv/state"',
    'bun "$DEVENV_ROOT/setup-environment.ts" >&2',
  ].join('\n');

  /**
   * direnv in `root` with a private home, so no host direnv config or approval leaks in, and a
   * `devenv` on PATH whose `direnvrc` defines `use_devenv` as `useDevenv`: the managed .envrc
   * drives devenv through exactly that pair.
   */
  async function isolatedDirenv(root: string, useDevenv: string) {
    const home = join(dirname(root), 'home');
    const devenvBin = join(dirname(root), 'devenv-bin');
    await mkdir(devenvBin, { recursive: true });
    await writeFile(
      join(devenvBin, 'devenv'),
      ['#!/bin/sh', '[ "$1" = direnvrc ] || exit 64', "cat <<'RC'", 'use_devenv() {', useDevenv, '}', 'RC', ''].join(
        '\n',
      ),
    );
    await chmod(join(devenvBin, 'devenv'), 0o755);
    const base: Record<string, string> = {
      PATH: `${devenvBin}:${process.env.PATH ?? ''}`,
      HOME: home,
      XDG_CONFIG_HOME: join(home, '.config'),
      XDG_DATA_HOME: join(home, '.local', 'share'),
      XDG_CACHE_HOME: join(home, '.cache'),
      DIRENV_LOG_FORMAT: '',
    };
    const direnv = async (args: readonly string[], env: Record<string, string>) => {
      const proc = Bun.spawn({ cmd: ['direnv', ...args], cwd: root, env, stdout: 'pipe', stderr: 'pipe' });
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);
      expect({ args, exitCode, stderr: exitCode === 0 ? '' : stderr }).toEqual({ args, exitCode: 0, stderr: '' });
      return stdout;
    };
    return { base, direnv };
  }

  it('re-enters when a lockfile changes, and not for any other edit', async () => {
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: {
          [MEMBER]: JSON.stringify({ name: 'member', version: '0.0.0' }),
          'README.md': 'fixture\n',
        },
      },
      async ({ root }) => {
        const { base, direnv } = await isolatedDirenv(root, USE_DEVENV);
        await direnv(['allow', root], base);
        // The load a shell pool keeps: every variable the export sets.
        const loaded: Record<string, string> = { ...base };
        const exported: unknown = JSON.parse(await direnv(['export', 'json'], base));
        for (const [name, value] of Object.entries(exported ?? {})) {
          if (typeof value === 'string') loaded[name] = value;
        }

        expect(await direnv(['export', 'json'], loaded)).toBe('');
        await edit(join(root, 'README.md'), 'edited\n');
        await edit(join(root, MEMBER), JSON.stringify({ name: 'member', version: '0.0.1' }));
        expect(await direnv(['export', 'json'], loaded)).toBe('');
        await edit(join(root, 'Cargo.lock'), 'version = 4\n');
        expect(await direnv(['export', 'json'], loaded)).not.toBe('');
      },
    );
  });

  it("is built from the checkout alone, never from a parent directory's .envrc", async () => {
    await withManagedRepository({}, async ({ root }) => {
      const { base, direnv } = await isolatedDirenv(root, 'export DEVENV_ROOT="$PWD"');
      // An approved .envrc one directory up, as a checkout under a projects folder has.
      const parent = join(dirname(root), '.envrc');
      await writeFile(parent, 'export SMOO_PARENT_ENVRC=loaded\n');
      await direnv(['allow', parent], base);
      await direnv(['allow', root], base);

      const exported: unknown = JSON.parse(await direnv(['export', 'json'], base));
      const variables = new Map(Object.entries(exported ?? {}));
      expect(variables.get('DEVENV_ROOT')).toBe(join(root, 'tooling', 'direnv'));
      expect(variables.has('SMOO_PARENT_ENVRC')).toBe(false);
    });
  });

  it('loads the workspace environment, then .envrc-local, from the checkout root after devenv', async () => {
    await withManagedRepository(
      {
        files: {
          '.cowshed/env': 'export SMOO_LOADED="cowshed"\n',
          '.envrc-local': 'export SMOO_LOADED="$SMOO_LOADED local"\n',
        },
      },
      async ({ root }) => {
        // The export devenv replays leaves the shell where its hook last changed directory.
        const { base, direnv } = await isolatedDirenv(root, 'export DEVENV_ROOT="$PWD"\ncd /');
        await direnv(['allow', root], base);

        const exported: unknown = JSON.parse(await direnv(['export', 'json'], base));
        expect(new Map(Object.entries(exported ?? {})).get('SMOO_LOADED')).toBe('cowshed local');
      },
    );
  });
});

/**
 * What an explicit missing-payload repair does, measured against the real
 * "setup-environment.ts --repair-missing-payload" mode.
 *
 * Every case owns a private HTTP registry serving a real published tar: real
 * bytes, real sha512 integrity, a real "bun install" resolving it into
 * bun.lock under a private BUN_INSTALL_CACHE_DIR. Each then breaks one
 * installed file physically and requests the repair through the checkout's
 * installed alias. Nothing here stubs the repair: the fetch, the locked
 * integrity comparison, the filesystem and the whole-directory exchange are
 * the real ones, and the atomic-failure case reaches its cause through real
 * file permissions rather than any fault flag. A successful repair reports its
 * stdout JSON protocol; the filesystem digests around each run are the proof
 * that the report is true.
 */

interface PublishedRegistry {
  readonly origin: string;
  /** Serves tar bytes that hash differently from the integrity bun.lock records. */
  readonly tamper: (tampered: boolean) => void;
  readonly close: () => void;
}

const PUBLISHED_PACKAGE = JSON.stringify({ name: '@acme/payload', version: '1.0.0', main: 'index.js' });
const PUBLISHED_INDEX = "module.exports = 'published index';\n";
const PUBLISHED_MIDDLE = "module.exports = 'published middle';\n";
const PUBLISHED_INNER = "module.exports = 'published inner';\n";
const PUBLISHED_FILES: Readonly<Record<string, string>> = {
  'package.json': PUBLISHED_PACKAGE,
  'index.js': PUBLISHED_INDEX,
  'lib/middle.js': PUBLISHED_MIDDLE,
  'lib/deep/inner.js': PUBLISHED_INNER,
};

/** The published package served over real HTTP, with its real tar and integrity. */
async function publishedRegistry(): Promise<PublishedRegistry> {
  const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-published-')));
  const stage = join(scratch, 'stage', 'package');
  for (const [file, content] of Object.entries(PUBLISHED_FILES)) {
    await mkdir(dirname(join(stage, file)), { recursive: true });
    await writeFile(join(stage, file), content);
  }
  const tgz = join(scratch, 'payload-1.0.0.tgz');
  const packed = Bun.spawnSync(['tar', '-czf', tgz, '-C', join(scratch, 'stage'), 'package']);
  if (packed.exitCode !== 0) {
    throw new Error('tar failed: ' + new TextDecoder().decode(packed.stderr));
  }
  const real = new Uint8Array(readFileSync(tgz));
  const integrity = 'sha512-' + new Bun.CryptoHasher('sha512').update(real).digest('base64');
  const tampered = Uint8Array.from(real);
  const last = tampered.length - 1;
  tampered[last] = (tampered[last] ?? 0) ^ 0xff;
  let served = real;
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const url = new URL(request.url);
      const path = url.pathname;
      if (path === '/@acme%2fpayload' || path === '/@acme/payload') {
        return Response.json({
          name: '@acme/payload',
          'dist-tags': { latest: '1.0.0' },
          versions: {
            '1.0.0': {
              name: '@acme/payload',
              version: '1.0.0',
              dist: {
                tarball: url.origin + '/tarball/payload-1.0.0.tgz',
                integrity,
              },
            },
          },
        });
      }
      if (path === '/tarball/payload-1.0.0.tgz') {
        return new Response(served, { headers: { 'content-type': 'application/octet-stream' } });
      }
      return new Response('not found', { status: 404 });
    },
  });
  return {
    origin: 'http://127.0.0.1:' + server.port,
    tamper: (on: boolean) => {
      served = on ? tampered : real;
    },
    close: () => {
      server.stop(true);
      void rm(scratch, { recursive: true, force: true });
    },
  };
}

/** Every byte and link under dir, keyed relative to it. */
function packageState(dir: string): Record<string, string> {
  const state: Record<string, string> = {};
  const walk = (current: string): void => {
    for (const entry of readdirSync(current).sort()) {
      const path = join(current, entry);
      const key = relative(dir, path);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink()) state[key] = 'link -> ' + readlinkSync(path);
      else if (stat.isDirectory()) walk(path);
      else state[key] = readFileSync(path, 'utf8');
    }
  };
  walk(dir);
  return state;
}

/** The link's target, or null where the path is a real directory or absent. */
function linkTarget(path: string): string | null {
  try {
    return readlinkSync(path);
  } catch {
    return null;
  }
}

/** The real script's run, with the stdout its repair protocol reports on. */
async function setupResult(proc: ShellProcess): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { exitCode, stdout, stderr };
}

/** The reported protocol fields of one repair run, read at the seam. */
function protocol(stdout: string): Record<string, unknown> {
  const report: unknown = JSON.parse(stdout);
  return Object.fromEntries(Object.entries(report ?? {}));
}

/** One run of the real script, with the private Bun install cache this fixture owns. */
function setupRun(root: string, cargoHome: string, args: readonly string[]): ShellProcess {
  const scratch = dirname(cargoHome);
  return Bun.spawn({
    cmd: ['bun', join(root, 'tooling', 'direnv', 'setup-environment.ts'), ...args],
    cwd: root,
    env: {
      PATH: join(scratch, 'bin') + ':' + (process.env['PATH'] ?? ''),
      HOME: process.env['HOME'],
      CARGO_HOME: cargoHome,
      XDG_CACHE_HOME: join(scratch, 'xdg-cache'),
      // The private cache lives inside this checkout: the repair contract
      // requires physically checkout-owned package payload.
      BUN_INSTALL_CACHE_DIR: join(root, '.bun-install-cache'),
      DEVENV_ROOT: join(root, 'tooling', 'direnv'),
      DEVENV_STATE: join(root, 'tooling', 'direnv', '.devenv', 'state'),
    },
    stdout: 'pipe',
    stderr: 'pipe',
    stdin: 'ignore',
  });
}

describe('explicit missing-payload repair', () => {
  const alias = join('node_modules', '@acme', 'payload');
  const missing = join(alias, 'lib', 'middle.js');

  async function withInstalledPayload(
    registry: PublishedRegistry,
    options: RepositoryOptions,
    run: (repository: Repository) => Promise<void>,
    beforeInstall?: (repository: Repository) => Promise<void>,
  ): Promise<void> {
    await withManagedRepository(
      {
        ...options,
        dependencies: { '@acme/payload': '1.0.0', ...options.dependencies },
        files: {
          '.npmrc': '@acme:registry=' + registry.origin + '\n',
          // The repo's own proven install declaration, verbatim: isolated linker
          // plus the global store, whose root is this fixture's private
          // BUN_INSTALL_CACHE_DIR. That is what produces the measured topology —
          // node_modules/.bun/<name>@<ver> absolute store links into
          // <cache>/links/<name>@<ver>-<hash> and relative alias symlinks above
          // them — and nothing reaches any consumer or host cache.
          'bunfig.toml': '[install]\nlinker = "isolated"\nglobalStore = true\n',
          ...options.files,
        },
      },
      async (repository) => {
        if (beforeInstall !== undefined) await beforeInstall(repository);
        // A healthy install reports the declared developer links it created on
        // one line, each entry naming its package and resolved absolute target;
        // a fixture without declarations reports nothing.
        const links = Object.entries(options.developerLinks ?? {});
        const report =
          links.length === 0
            ? ''
            : 'linked to local checkouts: ' +
              links.map(([name, to]) => name + ' -> ' + resolve(repository.root, to)).join(', ') +
              '\n';
        expect(await finished(setupRun(repository.root, repository.cargoHome, []))).toEqual({
          exitCode: 0,
          stderr: report,
        });
        await run(repository);
      },
    );
  }

  it('restores a physically missing published file to its locked published bytes', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        await rm(installed);
        const broken = packageState(join(root, alias));
        expect(Object.hasOwn(broken, 'lib/middle.js')).toBe(false);
        const receipt = join(root, 'node_modules', '.smoo-install');
        const stamped = readFileSync(receipt, 'utf8');
        const locked = readFileSync(join(root, 'bun.lock'), 'utf8');
        const repaired = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
        expect(repaired.exitCode).toBe(0);
        // Missing-only: exactly the named file returns, at its locked published
        // bytes, and every other byte and link of the package is untouched.
        expect(packageState(join(root, alias))).toEqual({
          ...broken,
          'lib/middle.js': PUBLISHED_MIDDLE,
        });
        expect(readFileSync(join(root, 'bun.lock'), 'utf8')).toBe(locked);
        expect(readFileSync(receipt, 'utf8')).toBe(stamped);
        const report = protocol(repaired.stdout);
        expect(report['operation']).toBe('repair-missing-payload');
        expect(report['package']).toBe('@acme/payload@1.0.0');
        expect(report['requestedPath']).toBe(installed);
        expect(report['publishedComplete']).toBe(true);
        expect(report['lockedHashesUnchanged']).toBe(true);
        expect(report['restoredFiles']).toBe(1);
        expect(report['publishedFiles']).toBe(4);
      });
    } finally {
      registry.close();
    }
  });

  it('refuses an existing published-payload mismatch and changes nothing', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        await rm(installed);
        await edit(join(root, alias, 'index.js'), "module.exports = 'local edit';\n");
        const broken = packageState(join(root, alias));
        const receipt = join(root, 'node_modules', '.smoo-install');
        const stamped = readFileSync(receipt, 'utf8');
        const locked = readFileSync(join(root, 'bun.lock'), 'utf8');
        const refused = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
        expect(refused.exitCode).not.toBe(0);
        expect(refused.stderr).toContain('index.js');
        // The mismatch is refused, never silently repaired: package, stamp and
        // locks are all unchanged, and the named file stays missing.
        expect(packageState(join(root, alias))).toEqual(broken);
        expect(readFileSync(join(root, 'bun.lock'), 'utf8')).toBe(locked);
        expect(readFileSync(receipt, 'utf8')).toBe(stamped);
      });
    } finally {
      registry.close();
    }
  });

  it('refuses a tar whose bytes fail the locked integrity and changes nothing', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        await rm(installed);
        const broken = packageState(join(root, alias));
        const receipt = join(root, 'node_modules', '.smoo-install');
        const stamped = readFileSync(receipt, 'utf8');
        const locked = readFileSync(join(root, 'bun.lock'), 'utf8');
        registry.tamper(true);
        const refused = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
        expect(refused.exitCode).not.toBe(0);
        expect(refused.stderr).toContain('integrity');
        expect(packageState(join(root, alias))).toEqual(broken);
        expect(readFileSync(join(root, 'bun.lock'), 'utf8')).toBe(locked);
        expect(readFileSync(receipt, 'utf8')).toBe(stamped);
      });
    } finally {
      registry.close();
    }
  });

  it('repeats a healthy request idempotently, installing nothing', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome, count }) => {
        const installed = join(root, missing);
        await rm(installed);
        const repaired = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
        expect(repaired.exitCode).toBe(0);
        const whole = packageState(join(root, alias));
        const receipt = join(root, 'node_modules', '.smoo-install');
        const stamped = readFileSync(receipt, 'utf8');
        const installs = count('install');
        const again = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
        expect(again.exitCode).toBe(0);
        expect(protocol(again.stdout)['operation']).toBe('repair-missing-payload');
        expect(packageState(join(root, alias))).toEqual(whole);
        expect(readFileSync(receipt, 'utf8')).toBe(stamped);
        expect(count('install')).toBe(installs);
      });
    } finally {
      registry.close();
    }
  });

  it('retains alias links, package metadata and developer links across a repair', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(
        registry,
        {
          // A declaration links what the manifests depend on and never adds a
          // dependency, so the linked name is an installed package first.
          dependencies: { '@acme/notes': 'file:vendor/notes' },
          files: {
            'vendor/notes/package.json': JSON.stringify({ name: '@acme/notes', version: '1.0.0' }),
          },
          developerLinks: { '@acme/notes': '../notes-target' },
        },
        async ({ root, cargoHome }) => {
          const aliasPath = join(root, alias);
          const notes = join(root, 'node_modules', '@acme', 'notes');
          // Each oracle names itself in its failure, because the runner's
          // source frames are unreliable.
          expect(lstatSync(aliasPath).isSymbolicLink() ? 'alias-link' : 'alias-not-a-link').toBe('alias-link');
          const beforeAlias = linkTarget(aliasPath);
          const beforeNotes = linkTarget(notes);
          expect(existsSync(notes) ? 'notes-link' : 'notes-absent').toBe('notes-link');
          const metadata = readFileSync(join(aliasPath, 'package.json'), 'utf8');
          await rm(join(root, missing));
          const repaired = await setupResult(
            setupRun(root, cargoHome, ['--repair-missing-payload', join(root, missing)]),
          );
          expect(repaired.exitCode).toBe(0);
          expect(lstatSync(aliasPath).isSymbolicLink() ? 'alias-link' : 'alias-not-a-link').toBe('alias-link');
          expect(linkTarget(aliasPath)).toBe(beforeAlias);
          expect(linkTarget(notes)).toBe(beforeNotes);
          expect(readFileSync(join(aliasPath, 'package.json'), 'utf8')).toBe(metadata);
        },
        async ({ root }) => {
          const target = join(dirname(root), 'notes-target');
          await mkdir(target, { recursive: true });
          // The external target is a real package directory, as every existing
          // developer-target fixture provides.
          await writeFile(join(target, 'package.json'), JSON.stringify({ name: '@acme/notes', version: '9.9.9' }));
          await writeFile(join(target, 'README.md'), 'developer notes\n');
        },
      );
    } finally {
      registry.close();
    }
  });

  it('refuses a repair with no prior success witness and mints no fresh proof', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        const receipt = join(root, 'node_modules', '.smoo-install');
        await rm(receipt);
        await rm(installed);
        const broken = packageState(join(root, alias));
        const locked = readFileSync(join(root, 'bun.lock'), 'utf8');
        const refused = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
        expect(refused.exitCode).not.toBe(0);
        // Refused rather than minting fresh full-install proof: no witness
        // appears, and the package stays exactly as it was asked.
        expect(existsSync(receipt)).toBe(false);
        expect(packageState(join(root, alias))).toEqual(broken);
        expect(readFileSync(join(root, 'bun.lock'), 'utf8')).toBe(locked);
      });
    } finally {
      registry.close();
    }
  });

  it('preserves a stale witness and its stale inputs exactly', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        const receipt = join(root, 'node_modules', '.smoo-install');
        // The install inputs drift after the witness was recorded. The repair is
        // not an install: the stale inputs and the witness's recorded digests
        // stay exactly as they are, while the missing file is restored.
        await edit(join(root, 'package.json'), readFileSync(join(root, 'package.json'), 'utf8') + '\n');
        const witness = readFileSync(receipt, 'utf8');
        const stale = readFileSync(join(root, 'package.json'), 'utf8');
        await rm(installed);
        const repaired = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
        expect(repaired.exitCode).toBe(0);
        expect(readFileSync(installed, 'utf8')).toBe(PUBLISHED_MIDDLE);
        expect(readFileSync(receipt, 'utf8')).toBe(witness);
        expect(readFileSync(join(root, 'package.json'), 'utf8')).toBe(stale);
      });
    } finally {
      registry.close();
    }
  });

  it('a failed atomic exchange leaves the live package unchanged and reports the cause', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        await rm(installed);
        const broken = packageState(join(root, alias));
        const receipt = join(root, 'node_modules', '.smoo-install');
        const stamped = readFileSync(receipt, 'utf8');
        const locked = readFileSync(join(root, 'bun.lock'), 'utf8');
        // A real permission failure at the package's own parent: the exchange
        // cannot commit, and no fault flag or production seam is involved.
        const parent = dirname(realpathSync(join(root, alias)));
        // The installer's real copy runs through this private PATH wrapper: it
        // invokes the real cp unchanged, propagates its exit code, and only then
        // turns the resolved package parent read-only, so the exchange itself
        // fails on the real permission boundary. No sleeps, no fault flag and no
        // bound changes.
        const realCp = Bun.which('cp');
        const realChmod = Bun.which('chmod');
        if (realCp === null || realChmod === null) throw new Error('cp and chmod must be on PATH');
        await writeFile(
          join(dirname(cargoHome), 'bin', 'cp'),
          [
            '#!/usr/bin/env bash',
            JSON.stringify(realCp) + ' "$@"',
            'status=$?',
            JSON.stringify(realChmod) + ' 555 ' + JSON.stringify(parent),
            'exit $status',
            '',
          ].join('\n'),
        );
        await chmod(join(dirname(cargoHome), 'bin', 'cp'), 0o755);
        try {
          const failed = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
          expect(failed.exitCode).not.toBe(0);
          expect(failed.stderr).toContain('Atomic package exchange failed');
          expect(packageState(join(root, alias))).toEqual(broken);
          expect(readFileSync(join(root, 'bun.lock'), 'utf8')).toBe(locked);
          expect(readFileSync(receipt, 'utf8')).toBe(stamped);
        } finally {
          await chmod(parent, 0o755);
        }
      });
    } finally {
      registry.close();
    }
  });
});

describe('the project root a shell entry owns', () => {
  /**
   * A fresh git checkout holding the full managed direnv directory, so the real script and its
   * imports run. `manifest` writes the repository's package.json; without it, the checkout has none.
   */
  async function freshCheckout(scratch: string, manifest: boolean): Promise<string> {
    const checkout = join(scratch, 'checkout');
    await mkdir(join(checkout, 'tooling', 'direnv'), { recursive: true });
    await cp(join(MANAGED, 'tooling', 'direnv'), join(checkout, 'tooling', 'direnv'), { recursive: true });
    await copyFile(join(MANAGED, 'envrc'), join(checkout, '.envrc'));
    if (manifest) {
      await writeFile(join(checkout, 'package.json'), '{"name":"fixture","version":"0.0.0","private":true}\n');
    }
    expect(Bun.spawnSync(['git', 'init', '--quiet'], { cwd: checkout }).exitCode).toBe(0);
    return checkout;
  }

  function startOwned(
    checkout: string,
    cwd: string,
    devenvRoot: string | undefined,
    args: string[] = [],
  ): ShellProcess {
    return Bun.spawn({
      cmd: ['bun', join(checkout, 'tooling', 'direnv', 'setup-environment.ts'), ...args],
      cwd,
      env: {
        PATH: process.env.PATH ?? '',
        HOME: process.env.HOME,
        ...(devenvRoot === undefined ? {} : { DEVENV_ROOT: devenvRoot }),
      },
      stdout: 'pipe',
      stderr: 'pipe',
      stdin: 'ignore',
    });
  }

  it('refuses a DEVENV_ROOT that names another checkout, naming both roots, with no ENOENT', async () => {
    const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-owning-root-')));
    try {
      const checkout = await freshCheckout(scratch, true);
      const foreign = join(scratch, 'foreign', 'tooling', 'direnv');
      await mkdir(foreign, { recursive: true });

      const entry = await finished(startOwned(checkout, checkout, foreign));

      expect(entry.exitCode).toBe(1);
      expect(entry.stderr).toContain(`DEVENV_ROOT names ${resolve(foreign, '../..')}`);
      expect(entry.stderr).toContain(`this checkout's root is ${checkout}`);
      expect(entry.stderr).not.toContain('ENOENT');
    } finally {
      await rm(scratch, { recursive: true, force: true });
    }
  });

  it('accepts a DEVENV_ROOT that names its own checkout when run from a nested directory', async () => {
    const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-owned-root-')));
    try {
      const checkout = await freshCheckout(scratch, true);
      const nested = join(checkout, 'packages', 'member');
      await mkdir(nested, { recursive: true });

      const entry = await finished(startOwned(checkout, nested, join(checkout, 'tooling', 'direnv')));

      expect(entry.stderr).not.toContain('DEVENV_ROOT names');
      expect(entry.stderr).not.toContain('cannot locate the project root');
    } finally {
      await rm(scratch, { recursive: true, force: true });
    }
  });

  it('reports an unavailable root with a hint and exit 1 when the checkout has no manifest', async () => {
    const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-no-manifest-')));
    try {
      const checkout = await freshCheckout(scratch, false);

      const entry = await finished(startOwned(checkout, checkout, undefined));

      expect(entry.exitCode).toBe(1);
      expect(entry.stderr).toContain('cannot locate the project root');
      expect(entry.stderr).toContain('has no package.json');
      expect(entry.stderr).not.toContain('ENOENT');
    } finally {
      await rm(scratch, { recursive: true, force: true });
    }
  });
});
