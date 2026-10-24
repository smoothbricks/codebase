/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { readFileSync, readlinkSync } from 'node:fs';
import { chmod, cp, mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { edit, finished, git, HEALTHY, repository, withManagedRepository } from './__tests__/helpers/shell-entry.js';

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
      TMPDIR: dirname(root),
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
