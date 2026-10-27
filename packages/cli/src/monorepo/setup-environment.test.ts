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
  waitForInstallLock,
  withInstallLockHeld,
  withManagedRepository,
} from './__tests__/helpers/shell-entry.js';

const MEMBER = 'packages/member/package.json';

/** A workspace member that depends on packages vendored in the checkout, so an install needs no registry. */
const member = (dependencies: readonly string[], version = '0.0.0') =>
  JSON.stringify({
    name: 'member',
    version,
    dependencies: Object.fromEntries(dependencies.map((name) => [name, `file:../../vendor/${name}`])),
  });
const vendored = (name: string) => JSON.stringify({ name, version: '1.0.0' });

/**
 * A root `prepare` that a case turns into a failing install with a file. It records each install it lets through
 * in the fixture's `install` ledger, which sits beside the checkout; while `install-fails` is in the checkout it
 * fails the install instead, noting the attempt in `attempts`.
 */
const PREPARE = `if [ -f install-fails ]; then printf 'x\\n' >> attempts; exit 1; fi; printf 'x\\n' >> ../ledgers/install`;

describe('what shell entry costs, in provider invocations', () => {
  it('runs no provider for a registry credential, and a shell secret only for the entry that installs, and names the exact grouped command when that install fails', async () => {
    await withManagedRepository({ prepare: PREPARE }, async ({ root, enterShell: enter, count }) => {
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

      // A failing root prepare script fails `bun install` locally without
      // needing a registry: the degraded path is what prints the deferrals.
      await writeFile(join(root, 'install-fails'), '');
      await edit(join(root, 'bunfig.toml'), '[install]\nexact = true\n');
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
  // The stamp's cases on one checkout, one change after another, each from the state the one before left. The
  // failures come last: a failed install leaves the tree behind its manifests.
  it('installs only on a changed input, repairs a lost native binary and a receipt without lock digests, retries a failed install and never rewrites bun.lock', async () => {
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        dependencies: { '@typescript/native': 'file:./vendor/native' },
        prepare: PREPARE,
        files: {
          [MEMBER]: member(['dep']),
          'bunfig.toml': '',
          'vendor/dep/package.json': vendored('dep'),
          'vendor/dep2/package.json': vendored('dep2'),
          'vendor/native/package.json': JSON.stringify({ name: '@typescript/native', version: '7.0.2' }),
          'vendor/native/bin/tsc': '#!/usr/bin/env node\n',
        },
      },
      async ({ root, enterShell: enter, count }) => {
        const stampPath = join(root, 'node_modules', '.smoo-install');
        const lockPath = join(root, 'bun.lock');
        const bin = join(root, 'node_modules', '@typescript', 'native', 'bin', 'tsc');

        // Installs only when a lockfile, a workspace manifest or bunfig.toml changed since the last install.
        expect(await enter()).toEqual(HEALTHY);
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(1);

        await edit(join(root, MEMBER), member(['dep'], '0.0.1'));
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(2);

        await edit(join(root, 'bunfig.toml'), '[install]\nexact = true\n');
        expect(await enter()).toEqual(HEALTHY);
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(3);

        // A pruned cache restore: the stamp and the TypeScript API package survive, the declared native compiler
        // binary does not.
        expect(existsSync(bin)).toBe(true);
        await rm(bin);
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(4);
        expect(existsSync(bin)).toBe(true);

        // A receipt missing its lock digests reinstalls, and so does one whose digests drifted: through the same
        // installer.
        const stamp = JSON.parse(readFileSync(stampPath, 'utf8'));
        await writeFile(stampPath, JSON.stringify({ inputs: stamp.inputs }));
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(5);
        const recorded = JSON.parse(readFileSync(stampPath, 'utf8'));
        expect(recorded.lockfiles['bun.lock']).toBe(
          new Bun.CryptoHasher('sha256').update(readFileSync(lockPath)).digest('hex'),
        );
        await writeFile(stampPath, JSON.stringify({ ...recorded, lockfiles: {} }));
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(6);

        // An install that failed is attempted again by the next entry, not recorded as done.
        await writeFile(join(root, 'install-fails'), '');
        await edit(join(root, MEMBER), member(['dep'], '0.0.2'));
        await enter();
        await enter();
        expect(readFileSync(join(root, 'attempts'), 'utf8')).toBe('x\nx\n');
        await rm(join(root, 'install-fails'));

        // An unfrozen entry install re-resolved a consumer's ranges and migrated its lockfile format: a bun.lock
        // rewrite of thousands of lines that nobody asked for. Shell entry installs what bun.lock records, or fails
        // saying why.
        const locked = readFileSync(lockPath, 'utf8');
        await edit(join(root, MEMBER), member(['dep', 'dep2'], '0.0.2'));
        const { exitCode, stderr } = await enter();
        // A failed install degrades the shell; it does not take it down.
        expect(exitCode).toBe(0);
        expect(stderr).toContain('bun install --frozen-lockfile');
        expect(readFileSync(lockPath, 'utf8')).toBe(locked);
        expect(existsSync(join(root, 'node_modules', 'dep2'))).toBe(false);
      },
    );
  });

  it('waits for the node_modules lock and installs once when two entries are released together', async () => {
    await withManagedRepository({}, async ({ root, startShell, count }) => {
      await withInstallLockHeld(root, async (holder) => {
        const entries = [startShell(), startShell()];
        try {
          // Each process has attempted the held lock before it is released. Both then
          // contend for the same install, and the second must recheck the first's stamp.
          const waits = await Promise.all(entries.map(waitForInstallLock));
          expect(waits).toEqual(
            entries.map(() => `setup-environment: waiting for another shell entry's install in ${root}\n`),
          );
          expect(count('install')).toBe(0);
          holder.stdin.end();
          expect(await holder.exited).toBe(0);
          const entered = await Promise.all(entries.map(finished));
          expect(entered).toEqual([HEALTHY, HEALTHY]);
          expect(count('install')).toBe(1);
        } finally {
          for (const entry of entries) {
            if (entry.exitCode === null) entry.kill('SIGKILL');
          }
          await Promise.all(entries.map((entry) => entry.exited));
        }
      });
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
  // A CI runner runs no provider command: the job's secret store injects every declared secret.
  const CI_SECRETS = { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' };
  // Two dependencies that live in the checkout, so locking, the frozen miss and the
  // fallback install all run without a registry.
  const FILES = {
    '.gitignore': 'node_modules\ntooling/direnv/.devenv\n',
    'vendor/dep/package.json': vendored('dep'),
    'vendor/dep2/package.json': vendored('dep2'),
    [MEMBER]: member(['dep']),
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
    await writeFile(join(root, MEMBER), member(['dep', 'dep2']));
    await git(root, ['add', MEMBER]);
  }

  // devenv reports a failed shell entry as "Shell environment capture failed:"
  // followed by the shell's stderr and nothing else; the stdout it captured is
  // discarded. `finished` mirrors that, so these read exactly what devenv would show.
  it('prints the whole lockfile diff on stderr however slowly it is read and fails the entry, then stamps node_modules and prints none once bun.lock records the manifest', async () => {
    // The entry exits straight after printing, and a write still queued then is cut short:
    // a diff that ends mid-hunk cannot be applied or checked. Four megabytes is well past
    // what a pipe buffers, so the reader's pause decides whether the tail survives.
    const bytes = 4_000_000;
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        files: { ...FILES, 'tracked.txt': '' },
        // Only the entries under test write: they alone have the injected secret in their environment.
        prepare: `if [ -n "$SMOO_TOKEN" ]; then head -c ${bytes} /dev/zero | tr '\\0' x > tracked.txt; printf '\\nEND\\n' >> tracked.txt; fi`,
      },
      async ({ root, startShell, enterShell: enter }) => {
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
        const stale = await finished(entry);

        expect(stale.exitCode).toBe(1);
        expect(stale.stderr).toContain('git diff after install:');
        expect(stale.stderr).toContain('+        "dep2": "file:../../vendor/dep2",');
        expect(stale.stderr).toContain('+END\n');
        expect(stale.stderr.length).toBeGreaterThan(bytes);
        // The failed install records nothing, so devenv's task has no stamp to find current.
        expect(existsSync(join(root, 'node_modules', '.smoo-install'))).toBe(false);

        // The pull request commits the lockfile that install wrote. A host runner keeps devenv's task state on a
        // shared bind and its checkout does not outlive the job: with no stamp the next job's checkout, same
        // lockfile, was found current, installed nothing and had no `ttsc`. The stamp is what devenv sees go
        // missing with node_modules, so a checkout without one installs again.
        await git(root, ['add', 'bun.lock']);
        await rm(join(root, 'node_modules'), { recursive: true, force: true });
        const { exitCode, stderr } = await enter({ ciSecrets: CI_SECRETS });

        expect(exitCode).toBe(0);
        expect(stderr).not.toContain('git diff after install:');
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
      },
    );
  });
});

describe('the project root a shell entry owns', () => {
  /**
   * A fresh git checkout holding the full managed direnv directory, so the real script and its
   * imports run. It has no manifest until a case writes the repository's package.json.
   */
  async function freshCheckout(scratch: string): Promise<string> {
    const checkout = join(scratch, 'checkout');
    await mkdir(join(checkout, 'tooling', 'direnv'), { recursive: true });
    await cp(join(MANAGED, 'tooling', 'direnv'), join(checkout, 'tooling', 'direnv'), { recursive: true });
    await copyFile(join(MANAGED, 'envrc'), join(checkout, '.envrc'));
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

  it('reports an unavailable root until the checkout has a manifest, then refuses a DEVENV_ROOT that names another checkout and accepts its own from a nested directory', async () => {
    const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-owning-root-')));
    try {
      const checkout = await freshCheckout(scratch);

      // No manifest: an unavailable root, with a hint and exit 1.
      const bare = await finished(startOwned(checkout, checkout, undefined));
      expect(bare.exitCode).toBe(1);
      expect(bare.stderr).toContain('cannot locate the project root');
      expect(bare.stderr).toContain('has no package.json');
      expect(bare.stderr).not.toContain('ENOENT');

      await writeFile(join(checkout, 'package.json'), '{"name":"fixture","version":"0.0.0","private":true}\n');

      // A DEVENV_ROOT that names another checkout is refused, naming both roots, with no ENOENT.
      const foreign = join(scratch, 'foreign', 'tooling', 'direnv');
      await mkdir(foreign, { recursive: true });
      const refused = await finished(startOwned(checkout, checkout, foreign));
      expect(refused.exitCode).toBe(1);
      expect(refused.stderr).toContain(`DEVENV_ROOT names ${resolve(foreign, '../..')}`);
      expect(refused.stderr).toContain(`this checkout's root is ${checkout}`);
      expect(refused.stderr).not.toContain('ENOENT');

      // One that names its own checkout is accepted when run from a nested directory.
      const nested = join(checkout, 'packages', 'member');
      await mkdir(nested, { recursive: true });
      const owned = await finished(startOwned(checkout, nested, join(checkout, 'tooling', 'direnv')));
      expect(owned.stderr).not.toContain('DEVENV_ROOT names');
      expect(owned.stderr).not.toContain('cannot locate the project root');
    } finally {
      await rm(scratch, { recursive: true, force: true });
    }
  });
});
