/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync, readlinkSync, realpathSync, symlinkSync } from 'node:fs';
import { chmod, cp, mkdir, rm, writeFile } from 'node:fs/promises';
import { dirname, join, relative } from 'node:path';
import {
  edit,
  finished,
  HEALTHY,
  linkEntry,
  repository,
  withManagedRepository,
} from './__tests__/helpers/shell-entry.js';
import { keepDeveloperLinks } from './developer-links.js';

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
    /** A local checkout of a package: a directory holding its manifest. */
    async function checkout(at: string, name: string): Promise<string> {
      await mkdir(at, { recursive: true });
      await writeFile(join(at, 'package.json'), JSON.stringify({ name, version: '9.9.9' }));
      return at;
    }

    it('links each installed declared package beside the main checkout a cowshed marker names', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../local-lib' } },
        async ({ root, enterShell: enter }) => {
          expect(await enter()).toEqual(HEALTHY);
          // Genuine sibling declarations share the main anchor without reading its tree.
          const main = join(dirname(root), 'projects', 'main');
          await mkdir(main, { recursive: true });
          await mkdir(join(root, '.cowshed'), { recursive: true });
          await writeFile(join(root, '.cowshed', 'workspace.json'), JSON.stringify({ projectRoot: main }));
          const local = await checkout(join(dirname(root), 'projects', 'local-lib'), '@fixture/lib');

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
          // A genuinely unrelated checkout remains the developer's independent choice.
          const independent = await checkout(join(dirname(root), 'independent-lib'), '@fixture/lib');
          await rm(join(root, LINKED));
          symlinkSync(independent, join(root, LINKED));
          expect((await enter({ links: true, check: true })).exitCode).toBe(0);
          expect((await enter({ links: true })).exitCode).toBe(0);
          expect(readlinkSync(join(root, LINKED))).toBe(independent);
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
          expect(check.exitCode).toBe(0);
          expect(check.stderr).toBe('');
          const applied = await enter({ links: true });
          expect(applied.exitCode).toBe(0);
          expect(applied.stderr).toContain('does not exist on this machine');
          expect(readlinkSync(join(root, LINKED))).toBe(sibling);
        },
      );
    });

    it('says an unavailable declaration without waiting for the install lock another entry holds', async () => {
      await withManagedRepository(
        { ...WORKSPACE, developerLinks: { '@fixture/lib': '../absent-lib' } },
        async ({ root, startShell }) => {
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
          expect(check.exitCode).toBe(0);
          expect(check.stderr).toBe('');
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
            'smoo.developerLinks @fixture/lib must be a directory relative to the main checkout, not "/opt/lib"',
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
              env: { PATH: process.env.PATH, HOME: process.env.HOME, TMPDIR: dirname(root), SMOO_NPM_TOKEN: 'npm' },
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
      'retains developer links when their target metadata is denied, and installs around them',
      async () => {
        await withDeveloperLinks(async ({ root, texts }) => {
          const blocked = join(dirname(root), 'blocked-provider');
          const provider = join(blocked, 'package');
          await mkdir(provider, { recursive: true });
          await linkEntry(root, APP_LIB, provider);
          const before = texts();
          await chmod(blocked, 0);
          try {
            // An unreadable checkout is not a deleted one, nor a reason to fail: the install runs and the link is put
            // back as made.
            await keepDeveloperLinks(root, async () => {
              await Bun.$`bun install --no-summary`.cwd(root).quiet();
            });
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
