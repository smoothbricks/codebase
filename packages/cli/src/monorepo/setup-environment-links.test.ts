/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { readFileSync, readlinkSync, symlinkSync } from 'node:fs';
import { chmod, mkdir, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { edit, finished, HEALTHY, linkEntry, withManagedRepository } from './__tests__/helpers/shell-entry.js';
import { keepDeveloperLinks } from './developer-links.js';

// What a declaration and an install do to the trees, called directly, is developer-links-declared.test.ts. These
// entries run what only the real shell entry, the root postinstall and a real `bun install` can show.
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
  const RUNNER = { SMOO_NPM_TOKEN: 'npm', SMOO_TOKEN: 'token' };

  /** Moves the install digest, so the next entry installs. */
  const touchManifest = (root: string) =>
    edit(join(root, 'packages/util/package.json'), JSON.stringify({ name: 'util', version: '0.0.1' }));

  /** A local checkout of a package: a directory holding its manifest. */
  async function checkout(at: string, name: string): Promise<string> {
    await mkdir(at, { recursive: true });
    await writeFile(join(at, 'package.json'), JSON.stringify({ name, version: '9.9.9' }));
    return at;
  }

  /** Rewrites the root manifest's `smoo.developerLinks`, or drops it. */
  async function declare(root: string, developerLinks: Readonly<Record<string, string>> | undefined): Promise<void> {
    const manifestPath = join(root, 'package.json');
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    if (developerLinks === undefined) delete manifest.smoo.developerLinks;
    else manifest.smoo.developerLinks = developerLinks;
    await writeFile(manifestPath, JSON.stringify(manifest));
  }

  it('preserves external developer targets while an install refreshes declared dependencies, and replaces one that dangles', async () => {
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

      // A link whose checkout is gone: the stamp still matches, so only the link can say the install is stale.
      const stamp = join(root, 'node_modules', '.smoo-install');
      const stampBytes = readFileSync(stamp);
      const gone = join(dirname(root), 'deleted-lib');
      await rm(join(root, LINKED));
      symlinkSync(gone, join(root, LINKED));

      expect(readFileSync(stamp)).toEqual(stampBytes);
      expect((await enter()).exitCode).toBe(0);
      expect(count('install')).toBe(3);
      expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');

      expect((await enter()).exitCode).toBe(0);
      expect(count('install')).toBe(3);
    });
  });

  it.skipIf(process.getuid?.() === 0)(
    'keeps a developer link whose target it may not read, says so, and installs around it',
    async () => {
      await withManagedRepository(WORKSPACE, async ({ root, enterShell: enter, count }) => {
        expect(await enter()).toEqual(HEALTHY);
        // A local build behind a directory this process may not search: its stat fails EACCES, as a cowshed sandbox
        // that grants no read of it fails EPERM. The link is not wrong, only unreadable here.
        const fence = join(dirname(root), 'fenced');
        const local = join(fence, 'local-lib');
        await mkdir(local, { recursive: true });
        await writeFile(join(local, 'package.json'), JSON.stringify({ name: '@fixture/lib', version: '9.9.9' }));
        await rm(join(root, LINKED));
        symlinkSync(local, join(root, LINKED));
        await chmod(fence, 0);
        try {
          const said = `! developer link ${LINKED} -> ${local}: its target cannot be read here (EACCES); kept as it is\n`;
          const entered = await enter();
          expect(entered.exitCode, entered.stderr).toBe(0);
          expect(entered.stderr).toContain(said);
          expect(count('install')).toBe(1);
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);

          await touchManifest(root);
          const installed = await enter();
          expect(installed.exitCode, installed.stderr).toBe(0);
          expect(installed.stderr).toContain(said);
          expect(count('install')).toBe(2);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
          // The install recorded its stamp: the next entry installs nothing.
          expect((await enter()).exitCode).toBe(0);
          expect(count('install')).toBe(2);
        } finally {
          await chmod(fence, 0o755);
        }
      });
    },
  );

  describe('as smoo.developerLinks declares them', () => {
    it('the --links task links without an install lock or a reinstall, says what it cannot link, and links nothing on CI', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../absent-lib' } },
        async ({ root, startShell, enterShell: enter, count }) => {
          expect(await finished(startShell())).toEqual(HEALTHY);
          // Another entry's install, reduced to its lock: a process holding flock(2) on the checkout's install lock
          // until its stdin closes or it is killed. It says `held` once it has it.
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
            const said = holder.stdout.getReader();
            const first = await said.read();
            expect(new TextDecoder().decode(first.value)).toBe('held\n');
            said.releaseLock();

            // The entry prints its wait line the moment it finds the lock held and then polls until the holder
            // dies, so reading its stderr up to that line or its exit decides the case without a clock.
            const entry = startShell({ links: true });
            const waited = "setup-environment: waiting for another shell entry's install";
            let stderr = '';
            for await (const chunk of entry.stderr) {
              stderr += new TextDecoder().decode(chunk);
              if (stderr.includes(waited)) entry.kill('SIGKILL');
            }
            expect(stderr).not.toContain(waited);
            expect(await entry.exited).toBe(0);
            expect(stderr).toContain(
              `! smoo.developerLinks @fixture/lib: ${join(dirname(root), 'absent-lib')} does not exist on this machine; the installed version stays`,
            );
            expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
          } finally {
            holder.kill('SIGKILL');
            await holder.exited;
          }

          // An absent declared checkout is current for the status, which says nothing about it.
          expect(await enter({ links: true, check: true })).toEqual(HEALTHY);

          // A declared checkout that is present is stale until linked, and linking it installs nothing.
          const local = await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
          await declare(root, { '@fixture/lib': '../local-lib' });
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          // A CI runner builds against the published packages, so it links nothing.
          expect(await enter({ links: true, check: true, ciSecrets: RUNNER })).toEqual(HEALTHY);
          expect(await enter({ links: true, ciSecrets: RUNNER })).toEqual(HEALTHY);
          expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
          const linked = await enter({ links: true });
          expect(linked.exitCode).toBe(0);
          expect(linked.stderr).toContain(`linked declared packages to local checkouts: @fixture/lib -> ${local}`);
          expect(readlinkSync(join(root, LINKED))).toBe(local);
          expect(count('install')).toBe(1);
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);

          // A declaration smoo refuses fails the status with its reason.
          await declare(root, { '@fixture/lib': '/opt/lib' });
          const refused = await enter({ links: true, check: true });
          expect(refused.exitCode).toBe(1);
          expect(refused.stderr).toContain(
            'smoo.developerLinks @fixture/lib must be a directory relative to the main checkout, not "/opt/lib"',
          );
        },
      );
    });

    it('restores managed provenance with the link when an install fails after its postinstall', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../first-lib' } },
        async ({ root, enterShell: enter }) => {
          const first = await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
          expect((await enter()).exitCode).toBe(0);
          const receiptPath = join(root, 'node_modules', '.smoo-declared-links');
          const firstReceipt = readFileSync(receiptPath, 'utf8');

          // A changed declaration: the postinstall links the new checkout before the install fails.
          const next = await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
          await declare(root, { '@fixture/lib': '../next-lib' });
          await expect(
            keepDeveloperLinks(root, async () => {
              await Bun.$`bun install --no-summary`.cwd(root).quiet();
              throw new Error('fixture install rejected after postinstall');
            }),
          ).rejects.toThrow('fixture install rejected after postinstall');
          expect(readlinkSync(join(root, LINKED))).toBe(first);
          expect(readFileSync(receiptPath, 'utf8')).toBe(firstReceipt);
          expect((await enter({ links: true, check: true })).exitCode).toBe(1);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(next);

          // A dropped declaration: the install would remove the managed link, and fails.
          const nextReceipt = readFileSync(receiptPath, 'utf8');
          await declare(root, undefined);
          await expect(
            keepDeveloperLinks(root, async () => {
              await Bun.$`bun install --no-summary`.cwd(root).quiet();
              throw new Error('fixture undeclared install failed');
            }),
          ).rejects.toThrow('fixture undeclared install failed');
          expect(readlinkSync(join(root, LINKED))).toBe(next);
          expect(readFileSync(receiptPath, 'utf8')).toBe(nextReceipt);

          // The restored receipt still proves the link managed, so a new declaration replaces it.
          const third = await checkout(join(dirname(root), 'third-lib'), '@fixture/lib');
          await declare(root, { '@fixture/lib': '../third-lib' });
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(third);
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
          const install = () => {
            const run = Bun.spawnSync({
              cmd: ['bun', 'install'],
              cwd: root,
              env: { PATH: process.env.PATH, HOME: process.env.HOME, TMPDIR: dirname(root), SMOO_NPM_TOKEN: 'npm' },
              stdout: 'pipe',
              stderr: 'pipe',
            });
            return { exitCode: run.exitCode, stderr: run.stderr.toString() };
          };

          // Bun re-points a link nothing declares at its lockfile version, and the postinstall puts the declared one back.
          const mine = await checkout(join(dirname(root), 'my-util'), 'util');
          await rm(join(root, UNLINKED));
          symlinkSync(mine, join(root, UNLINKED));
          expect(install().exitCode).toBe(0);
          expect(readlinkSync(join(root, UNLINKED))).toBe('../../util');
          expect(readlinkSync(join(root, LINKED))).toBe(local);

          // An install with nothing to do runs the postinstall as well: the entry holds the lockfile's version and ends linked.
          await rm(join(root, LINKED));
          symlinkSync('../../../lib', join(root, LINKED));
          expect(install().exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(local);

          // The postinstall reads the same declaration as the --links task, so it refuses what that task refuses.
          await declare(root, { '@fixture/lib': '/opt/lib' });
          expect(install().stderr).toContain(
            'smoo.developerLinks @fixture/lib must be a directory relative to the main checkout, not "/opt/lib"',
          );
        },
      );
    });
  });

  describe('Nx bindings a developer pointed outside the checkout', () => {
    // `app` and the root both depend on the workspace member `nx`, so bun binds each `nx` entry to that member by
    // relative path, as it does `util`. A developer's `nx` entries point at a retired provider outside the checkout.
    const NX = `${APP}/node_modules/nx`;
    const ROOT_NX = 'node_modules/nx';
    /** Each Nx entry, with the target a fresh install binds it to. */
    const NX_BINDINGS: Readonly<Record<string, string>> = { [ROOT_NX]: '../packages/nx', [NX]: '../../nx' };
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
      // The install records itself in the fixture's ledger beside the checkout, and refuses while the checkout holds
      // `refuse-install`.
      prepare:
        'test ! -f refuse-install || { echo "fixture-install-refused" >&2; exit 19; }; printf \'x\\n\' >> ../ledgers/install',
    };

    /** Each Nx entry, root and app, bound by relative path to its member, as a fresh install leaves it. */
    function expectNxBindings(root: string): void {
      for (const [entry, binding] of Object.entries(NX_BINDINGS)) {
        expect(readlinkSync(join(root, entry))).toBe(binding);
      }
    }

    // Whether an external Nx entry in the root's tree or a member's makes the install stale is
    // developer-links.property.test.ts; this sequence runs each entry that acts on it.
    it('every entry replaces external Nx entries and keeps sibling links, and a failed install keeps both', async () => {
      await withManagedRepository(WITH_NX, async ({ root, enterShell: enter, count }) => {
        const stampPath = join(root, 'node_modules', '.smoo-install');
        expect(await enter()).toEqual(HEALTHY);
        expect(count('install')).toBe(1);
        expectNxBindings(root);
        const retired = await checkout(join(dirname(root), 'retired-nx'), 'nx');
        const local = await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
        await linkEntry(root, LINKED, local);

        // The shell link task repairs an external Nx entry without changed install inputs: devenv's install inputs did
        // not change, so only the link task's status runs.
        await linkEntry(root, NX, retired);
        let stamp = readFileSync(stampPath, 'utf8');
        expect((await enter({ links: true, check: true })).exitCode).toBe(1);
        expect(count('install')).toBe(1);
        expect(readlinkSync(join(root, NX))).toBe(retired);
        expect(readFileSync(stampPath, 'utf8')).toBe(stamp);

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

        // An ordinary entry replaces external Nx entries that a matching install stamp would keep: nothing dangles and
        // the stamp still matches, so only the entry can see that Nx left the checkout.
        for (const entry of Object.keys(NX_BINDINGS)) await linkEntry(root, entry, retired);
        expect((await enter()).exitCode).toBe(0);
        expect(count('install')).toBe(3);
        expectNxBindings(root);
        expect(readlinkSync(join(root, LINKED))).toBe(local);
        // The bindings are current now, so a hot second entry installs nothing.
        expect((await enter()).exitCode).toBe(0);
        expect(count('install')).toBe(3);
        expectNxBindings(root);

        // A failed shell link repair is reported, and its status stays stale.
        for (const entry of Object.keys(NX_BINDINGS)) await linkEntry(root, entry, retired);
        stamp = readFileSync(stampPath, 'utf8');
        await writeFile(join(root, 'refuse-install'), '');
        const failed = await enter({ links: true });
        expect(failed.exitCode).toBe(1);
        expect(failed.stderr).toContain('fixture-install-refused');
        for (const entry of Object.keys(NX_BINDINGS)) expect(readlinkSync(join(root, entry))).toBe(retired);
        expect(readlinkSync(join(root, LINKED))).toBe(local);
        expect(readFileSync(stampPath, 'utf8')).toBe(stamp);
        expect((await enter({ links: true, check: true })).exitCode).toBe(1);

        // A CI runner's install that fails keeps the captured Nx and sibling links: the frozen install fails on
        // prepare, its fallback fails too, and the entry reports the refusal.
        const failedOnRunner = await enter({ ciSecrets: RUNNER });
        expect(failedOnRunner.exitCode).toBe(1);
        expect(failedOnRunner.stderr).toContain('! Failed to install dependencies with frozen lockfile');
        expect(failedOnRunner.stderr).toContain('--- ERROR: setup-environment.ts failed while running: bun install');
        expect(failedOnRunner.stderr).toContain('exit code: 19');
        for (const entry of Object.keys(NX_BINDINGS)) expect(readlinkSync(join(root, entry))).toBe(retired);
        expect(readlinkSync(join(root, LINKED))).toBe(local);

        // A CI runner's frozen install that succeeds replaces them.
        await rm(join(root, 'refuse-install'));
        expect((await enter({ ciSecrets: RUNNER })).exitCode).toBe(0);
        expectNxBindings(root);
        expect(readlinkSync(join(root, LINKED))).toBe(local);
      });
    });
  });
});
