/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync, realpathSync, watch } from 'node:fs';
import { copyFile, cp, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import {
  edit,
  finished,
  git,
  HEALTHY,
  MANAGED,
  type ShellProcess,
  withManagedRepository,
} from './__tests__/helpers/shell-entry.js';

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

  it('repairs a restored tree that kept its stamp but lost the declared native compiler binary', async () => {
    await withManagedRepository(
      {
        dependencies: { '@typescript/native': 'file:./vendor/native' },
        files: {
          'vendor/native/package.json': JSON.stringify({ name: '@typescript/native', version: '7.0.2' }),
          'vendor/native/bin/tsc': '#!/usr/bin/env node\n',
        },
      },
      async ({ root, enterShell: enter, count }) => {
        const bin = join(root, 'node_modules', '@typescript', 'native', 'bin', 'tsc');
        expect(await enter()).toEqual(HEALTHY);
        expect(existsSync(bin)).toBe(true);
        expect(count('install')).toBe(1);

        // A pruned cache restore: the stamp and the TypeScript API package survive, the binary does not.
        await rm(bin);
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(2);
        expect(existsSync(bin)).toBe(true);
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
        // The holder is another process; the only signal that it is inside the install is the pid file its prepare
        // script writes. A watch on the checkout wakes on that write and the file is checked once the watch is armed,
        // so a write before it is seen; the holder's exit before writing it fails the wait. No clock decides it.
        const written = Promise.withResolvers<void>();
        const pidWritten = () => existsSync(held) && readFileSync(held, 'utf8').endsWith('\n');
        const watcher = watch(root, () => {
          if (pidWritten()) written.resolve();
        });
        void holder.exited.then(() => {
          if (pidWritten()) written.resolve();
          else written.reject(new Error(`the holder exited before it wrote ${held}`));
        });
        if (pidWritten()) written.resolve();
        try {
          await written.promise;
        } finally {
          watcher.close();
        }
        holder.kill('SIGKILL');
        await holder.exited;
        process.kill(Number(readFileSync(held, 'utf8').trim()), 'SIGKILL');
        await rm(join(root, 'hold'));

        // The entry prints its wait line the moment it finds the install lock held, and HEALTHY is empty stderr, so
        // this proves the dead holder's lock was free at the entry's first attempt: the install ran at once.
        expect(await enter()).toEqual(HEALTHY);
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
        TMPDIR: dirname(checkout),
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
