import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { chmod, copyFile, cp, mkdir, mkdtemp, rm, utimes, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

/**
 * What shell entry does, measured against the real script.
 *
 * `tooling/direnv/setup-environment.ts` is what a direnv reload runs. These
 * build a repository the way smoo manages one and run the script in it the way
 * the managed devenv prologue does, counting what actually ran: provider
 * commands for declared secrets, `bun install` (through the root `prepare`
 * lifecycle it triggers) and `uv sync` (through a recording `uv` on PATH).
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

interface EntryOptions {
  /** devenv's UV_PROJECT_ENVIRONMENT; present only in a shell that enables uv. */
  readonly uvProjectEnvironment?: string;
  /** The interpreter the managed module passes for a uv project. */
  readonly python?: string;
}

/** A shell entry still running, with its output piped for `finished`. */
type ShellProcess = Bun.Subprocess<'ignore', 'pipe', 'pipe'>;

interface Repository {
  readonly root: string;
  /** devenv's DEVENV_STATE for this checkout. */
  readonly state: string;
  /** Runs the real setup-environment.ts exactly as the managed devenv shell does. */
  readonly enterShell: (options?: EntryOptions) => Promise<ShellEntry>;
  /** The same entry, returned while it runs, for a test that races or kills it. */
  readonly startShell: () => ShellProcess;
  /** Times something recorded in the ledger: a provider command's variable, `install`, or `uv`. */
  readonly count: (name: string) => number;
  /** The argument vectors the recording `uv` received, one per run. */
  readonly uvRuns: () => string[][];
  /**
   * Each `git config` invocation that writes, as a `begin <pid>` and an `end <pid>` line
   * around the write, in the order they happened across every shell entry.
   */
  readonly gitConfigWrites: () => string[];
}

interface RepositoryOptions {
  readonly prepare?: string;
  readonly workspaces?: readonly string[];
  /** Extra files, relative to the repository root. */
  readonly files?: Readonly<Record<string, string>>;
  /** How long each `git config` write lingers before it runs, to widen a race between entries. */
  readonly gitWriteSeconds?: number;
}

/**
 * A managed repository on disk: the managed direnv scripts and envrc, the git
 * hooks they link, a `.npmrc` that interpolates one declared variable, and a
 * fake TypeScript API package so the script's post-install pin check finds
 * what it looks for. Nothing here is a stub of the code under test — only of
 * the repository it runs in and of the `uv` binary a Python shell provides.
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
    for (const name of ['setup-environment.ts', 'secret-references.ts']) {
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
        scripts: { prepare: options.prepare ?? record('install') },
        smoo: {
          secrets: {
            SMOO_NPM_TOKEN: { command: counter('SMOO_NPM_TOKEN', ledgers) },
            SMOO_TOKEN: { command: counter('SMOO_TOKEN', ledgers) },
          },
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
    startShell: () => startShell(root, state, bin, {}),
    count: (name) => lines(name).length,
    uvRuns: () => lines('uv').map((line) => line.split(' ')),
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
 * branch can decide this run.
 */
function startShell(root: string, state: string, bin: string, options: EntryOptions): ShellProcess {
  return Bun.spawn({
    cmd: [
      'bun',
      join(root, 'tooling', 'direnv', 'setup-environment.ts'),
      ...(options.python === undefined ? [] : ['--python', options.python]),
    ],
    cwd: root,
    env: {
      PATH: `${bin}:${process.env['PATH'] ?? ''}`,
      HOME: process.env['HOME'],
      DEVENV_ROOT: join(root, 'tooling', 'direnv'),
      DEVENV_STATE: state,
      ...(options.uvProjectEnvironment === undefined ? {} : { UV_PROJECT_ENVIRONMENT: options.uvProjectEnvironment }),
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

  it('records every install input where the managed envrc watches them', async () => {
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: {
          [MEMBER]: JSON.stringify({ name: 'member', version: '0.0.0' }),
          'pyproject.toml': '[tool.uv.workspace]\nmembers = ["python/*"]\n',
          'python/tool/pyproject.toml': '[project]\nname = "tool"\n',
        },
      },
      async ({ root, state, enterShell: enter }) => {
        expect(await enter()).toEqual(HEALTHY);
        const recorded = readFileSync(join(state, 'install-inputs'), 'utf8').trimEnd().split('\n');
        expect(recorded).toEqual(
          expect.arrayContaining(
            [
              'package.json',
              'bun.lock',
              'bunfig.toml',
              MEMBER,
              'pyproject.toml',
              'uv.lock',
              'python/tool/pyproject.toml',
            ].map((file) => join(root, file)),
          ),
        );
      },
    );
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
});

describe('a shell direnv keeps loaded', () => {
  const MEMBER = 'packages/member/package.json';
  // Stands in for `use devenv` and the managed prologue: devenv's two
  // variables, then the script the prologue runs.
  const ENVRC_SH = [
    'export DEVENV_ROOT="$PWD"',
    'export DEVENV_STATE="$PWD/.devenv/state"',
    'bun "$DEVENV_ROOT/setup-environment.ts" >&2',
    '',
  ].join('\n');

  it('re-enters exactly when an install input changes', async () => {
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: {
          [MEMBER]: JSON.stringify({ name: 'member', version: '0.0.0' }),
          'tooling/direnv/envrc.sh': ENVRC_SH,
          'README.md': 'fixture\n',
        },
      },
      async ({ root }) => {
        const home = join(dirname(root), 'home');
        const base: Record<string, string> = {
          PATH: process.env['PATH'] ?? '',
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

        await direnv(['allow', root], base);
        // The load a shell pool keeps: every variable the export sets.
        const loaded: Record<string, string> = { ...base };
        const exported: unknown = JSON.parse(await direnv(['export', 'json'], base));
        for (const [name, value] of Object.entries(exported ?? {})) {
          if (typeof value === 'string') loaded[name] = value;
        }

        expect(await direnv(['export', 'json'], loaded)).toBe('');
        await edit(join(root, 'README.md'), 'edited\n');
        expect(await direnv(['export', 'json'], loaded)).toBe('');
        await edit(join(root, MEMBER), JSON.stringify({ name: 'member', version: '0.0.1' }));
        expect(await direnv(['export', 'json'], loaded)).not.toBe('');
      },
    );
  });
});
