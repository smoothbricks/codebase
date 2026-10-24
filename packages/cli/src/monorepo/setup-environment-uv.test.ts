/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { existsSync, lstatSync, readFileSync, realpathSync } from 'node:fs';
import { chmod, cp, mkdir, symlink, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { edit, HEALTHY, repository, withManagedRepository } from './__tests__/helpers/shell-entry.js';

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

  it('replaces a stamped environment whose launcher and .pth still name the old absolute home', async () => {
    await withManagedRepository({ files: UV_PROJECT }, async ({ root, state, enterShell: enter, uvRuns }) => {
      const venv = join(state, 'venv');
      const uv = { uvProjectEnvironment: venv, python: PYTHON };
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(uvRuns()).toHaveLength(2);

      // A copied legacy environment under a matching stamp: relocatable nowhere, its launcher
      // naming the old absolute home, and an absolute .pth line the sync already relativized.
      await writeFile(join(venv, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
      await mkdir(join(venv, 'bin'), { recursive: true });
      await writeFile(join(venv, 'bin', 'pytest'), `#!${venv}/bin/python\n`);
      const site = join(venv, 'lib', 'python3.14', 'site-packages');
      await writeFile(join(site, 'tool.pth'), `${join(root, 'python/tool/src')}\n`);

      expect(await enter(uv)).toEqual(HEALTHY);
      expect(uvRuns().map(([verb]) => verb)).toEqual(['venv', 'sync', 'venv', 'sync']);
      expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8')).toContain('relocatable = true');
      expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false);
      expect(readFileSync(join(site, 'tool.pth'), 'utf8')).not.toContain(root);

      expect(await enter(uv)).toEqual(HEALTHY);
      expect(uvRuns()).toHaveLength(4);
    });
  });

  it('repairs each stamped escape independently, with a valid config throughout', async () => {
    const cases: ReadonlyArray<{
      name: string;
      tamper: (root: string, venv: string, site: string) => Promise<void>;
      repaired: (root: string, venv: string, site: string) => void;
    }> = [
      {
        name: 'a launcher naming a foreign old root',
        tamper: async (root, venv) => {
          const foreign = join(dirname(root), 'foreign-venv');
          await mkdir(join(foreign, 'bin'), { recursive: true });
          await writeFile(join(foreign, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
          await writeFile(join(foreign, 'bin', 'python'), '');
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(join(venv, 'bin', 'pytest'), `#!${join(foreign, 'bin', 'python')}\n`);
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
      {
        name: 'a launcher naming this environment absolutely',
        tamper: async (_root, venv) => {
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(join(venv, 'bin', 'pytest'), `#!${join(venv, 'bin', 'python')}\n`);
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
      {
        name: 'an absolute .pth line',
        tamper: async (root, _venv, site) => {
          await writeFile(join(site, 'tool.pth'), `${join(root, 'python/tool/src')}\n`);
        },
        repaired: (root, _venv, site) => expect(readFileSync(join(site, 'tool.pth'), 'utf8')).not.toContain(root),
      },
    ];
    for (const testCase of cases) {
      await withManagedRepository({ files: UV_PROJECT }, async ({ root, state, enterShell: enter, uvRuns }) => {
        const venv = join(state, 'venv');
        const uv = { uvProjectEnvironment: venv, python: PYTHON };
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        const site = join(venv, 'lib', 'python3.14', 'site-packages');
        await testCase.tamper(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        // A replacement, not a repeated sync over the escape: the venv verb runs again.
        expect(
          uvRuns().map(([verb]) => verb),
          testCase.name,
        ).toEqual(['venv', 'sync', 'venv', 'sync']);
        expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8'), testCase.name).toContain('relocatable = true');
        testCase.repaired(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        expect(uvRuns(), testCase.name).toHaveLength(4);
      });
    }
  });

  it('repairs checkout-naming launchers independently, with a valid config throughout', async () => {
    const cases: ReadonlyArray<{
      name: string;
      tamper: (root: string, venv: string, site: string) => Promise<void>;
      repaired: (root: string, venv: string, site: string) => void;
    }> = [
      {
        name: 'a launcher naming an old checkout devenv profile interpreter',
        tamper: async (root, venv) => {
          const profile = join(dirname(root), 'old-checkout', '.devenv', 'profile', 'bin');
          await mkdir(profile, { recursive: true });
          await writeFile(join(profile, 'python'), '');
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(join(venv, 'bin', 'pytest'), `#!${join(profile, 'python')}\n`);
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
      {
        name: 'a shell wrapper execing an old venv',
        tamper: async (root, venv) => {
          const foreign = join(dirname(root), 'foreign-venv');
          await mkdir(join(foreign, 'bin'), { recursive: true });
          await writeFile(join(foreign, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
          await writeFile(join(foreign, 'bin', 'python'), '');
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(join(venv, 'bin', 'pytest'), `#!/bin/sh\nexec "${join(foreign, 'bin', 'python')}" "$@"\n`);
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
      {
        name: 'a whitespace-shebang launcher naming an old venv',
        tamper: async (root, venv) => {
          const foreign = join(dirname(root), 'foreign-venv');
          await mkdir(join(foreign, 'bin'), { recursive: true });
          await writeFile(join(foreign, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
          await writeFile(join(foreign, 'bin', 'python'), '');
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(join(venv, 'bin', 'pytest'), `#! ${join(foreign, 'bin', 'python')}\n`);
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
    ];
    for (const testCase of cases) {
      await withManagedRepository({ files: UV_PROJECT }, async ({ root, state, enterShell: enter, uvRuns }) => {
        const venv = join(state, 'venv');
        const uv = { uvProjectEnvironment: venv, python: PYTHON };
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        const site = join(venv, 'lib', 'python3.14', 'site-packages');
        await testCase.tamper(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        expect(
          uvRuns().map(([verb]) => verb),
          testCase.name,
        ).toEqual(['venv', 'sync', 'venv', 'sync']);
        expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8'), testCase.name).toContain('relocatable = true');
        testCase.repaired(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        expect(uvRuns(), testCase.name).toHaveLength(4);
      });
    }
  });

  it('repairs spaced-path and interpreter-symlink escapes independently, with a valid config throughout', async () => {
    const cases: ReadonlyArray<{
      name: string;
      tamper: (root: string, venv: string, site: string) => Promise<void>;
      repaired: (root: string, venv: string, site: string) => void;
    }> = [
      {
        name: 'a shell wrapper execing a spaced old venv path, and an env -S launcher naming it',
        tamper: async (root, venv) => {
          const foreign = join(dirname(root), 'old checkout', 'foreign venv');
          await mkdir(join(foreign, 'bin'), { recursive: true });
          await writeFile(join(foreign, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
          await writeFile(join(foreign, 'bin', 'python'), '');
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(join(venv, 'bin', 'pytest'), `#!/bin/sh\nexec "${join(foreign, 'bin', 'python')}" "$@"\n`);
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
      {
        name: 'an env -S launcher naming a spaced old venv path',
        tamper: async (root, venv) => {
          const foreign = join(dirname(root), 'old checkout', 'foreign venv');
          await mkdir(join(foreign, 'bin'), { recursive: true });
          await writeFile(join(foreign, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
          await writeFile(join(foreign, 'bin', 'python'), '');
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(join(venv, 'bin', 'pytest'), `#!/usr/bin/env -S "${join(foreign, 'bin', 'python')}"\n`);
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
    ];
    for (const testCase of cases) {
      await withManagedRepository({ files: UV_PROJECT }, async ({ root, state, enterShell: enter, uvRuns }) => {
        const venv = join(state, 'venv');
        const uv = { uvProjectEnvironment: venv, python: PYTHON };
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        const site = join(venv, 'lib', 'python3.14', 'site-packages');
        await testCase.tamper(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        expect(
          uvRuns().map(([verb]) => verb),
          testCase.name,
        ).toEqual(['venv', 'sync', 'venv', 'sync']);
        expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8'), testCase.name).toContain('relocatable = true');
        testCase.repaired(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        expect(uvRuns(), testCase.name).toHaveLength(4);
      });
    }
  });

  it('repairs polyglot and interpreter-symlink escapes independently, with a valid config throughout', async () => {
    const cases: ReadonlyArray<{
      name: string;
      tamper: (root: string, venv: string, site: string) => Promise<void>;
      repaired: (root: string, venv: string, site: string) => void;
    }> = [
      {
        name: 'a relative interpreter alias resolving into an old checkout profile',
        tamper: async (root, venv) => {
          const profile = join(dirname(root), 'old-checkout', '.devenv', 'profile', 'bin');
          await mkdir(profile, { recursive: true });
          await writeFile(join(profile, 'python'), '');
          await symlink(relative(join(venv, 'bin'), join(profile, 'python')), join(venv, 'bin', 'python'));
        },
        repaired: (_root, venv) =>
          expect(lstatSync(join(venv, 'bin', 'python'), { throwIfNoEntry: false })).toBeUndefined(),
      },
      {
        name: 'a polyglot wrapper execing a spaced old venv path',
        tamper: async (root, venv) => {
          const foreign = join(dirname(root), 'old checkout', 'foreign venv');
          await mkdir(join(foreign, 'bin'), { recursive: true });
          await writeFile(join(foreign, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
          await writeFile(join(foreign, 'bin', 'python'), '');
          await mkdir(join(venv, 'bin'), { recursive: true });
          await writeFile(
            join(venv, 'bin', 'pytest'),
            `#!/bin/sh\n'''exec' "${join(foreign, 'bin', 'python')}" "$0" "$@"\n' '''\n`,
          );
        },
        repaired: (_root, venv) => expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false),
      },
      {
        name: 'the environment interpreter is a symlink into an old checkout profile',
        tamper: async (root, venv) => {
          const profile = join(dirname(root), 'old-checkout', '.devenv', 'profile', 'bin');
          await mkdir(profile, { recursive: true });
          await writeFile(join(profile, 'python'), '');
          await symlink(join(profile, 'python'), join(venv, 'bin', 'python'));
        },
        repaired: (_root, venv) =>
          expect(lstatSync(join(venv, 'bin', 'python'), { throwIfNoEntry: false })).toBeUndefined(),
      },
    ];
    for (const testCase of cases) {
      await withManagedRepository({ files: UV_PROJECT }, async ({ root, state, enterShell: enter, uvRuns }) => {
        const venv = join(state, 'venv');
        const uv = { uvProjectEnvironment: venv, python: PYTHON };
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        const site = join(venv, 'lib', 'python3.14', 'site-packages');
        await testCase.tamper(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        expect(
          uvRuns().map(([verb]) => verb),
          testCase.name,
        ).toEqual(['venv', 'sync', 'venv', 'sync']);
        expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8'), testCase.name).toContain('relocatable = true');
        testCase.repaired(root, venv, site);
        expect(await enter(uv), testCase.name).toEqual(HEALTHY);
        expect(uvRuns(), testCase.name).toHaveLength(4);
      });
    }
  });

  it('admits a constructed relocatable layout — relative interpreter aliases, a store interpreter, a self-locating wrapper — without another sync', async () => {
    await withManagedRepository({ files: UV_PROJECT }, async ({ root, state, enterShell: enter, uvRuns }) => {
      const venv = join(state, 'venv');
      const uv = { uvProjectEnvironment: venv, python: PYTHON };
      expect(await enter(uv)).toEqual(HEALTHY);

      expect(uvRuns()).toHaveLength(2);
      const store = join(dirname(root), 'store', 'bin');
      await mkdir(store, { recursive: true });
      await writeFile(join(store, 'python'), '');
      await symlink(join(store, 'python'), join(venv, 'bin', 'python'));
      await symlink('python', join(venv, 'bin', 'python3'));
      await symlink('python', join(venv, 'bin', 'python3.14'));
      await writeFile(join(venv, 'bin', 'pytest'), '#!/bin/sh\nexec "$(dirname "$0")/python" "$@"\n');
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(uvRuns()).toHaveLength(2);
    });
  });
  it('enters a real uv environment and its clone installed, the member importing from the clone', async () => {
    const realUv = Bun.which('uv');
    const pythonWhich = Bun.which('python3');
    if (realUv === null || pythonWhich === null) throw new Error('real uv and python3 must be on PATH');
    // The interpreter devenv passes is the store path the PATH python resolves to, not the
    // profile wrapper itself.
    const realPython = realpathSync(pythonWhich);
    const REAL_UV_PROJECT = {
      'pyproject.toml':
        '[project]\nname = "workspace"\nversion = "0.1.0"\nrequires-python = ">=3.12"\n[tool.uv.workspace]\nmembers = ["python/*"]\n[tool.uv]\npackage = false\n',
      'python/tool/pyproject.toml':
        '[project]\nname = "tool"\nversion = "0.1.0"\nrequires-python = ">=3.12"\n[project.scripts]\ntool = "tool:main"\n[build-system]\nrequires = ["uv_build>=0.7.0"]\nbuild-backend = "uv_build"\n',
      'python/tool/src/tool/__init__.py': 'def main():\n    print("tool")\n',
    };
    await withManagedRepository({ files: REAL_UV_PROJECT }, async (original) => {
      // The fixture's recording uv becomes a logging wrapper over the real one: the managed
      // entry, not a swapped layout, is what produces and then admits the environment.
      const wrapper = join(dirname(original.root), 'bin', 'uv');
      await writeFile(
        wrapper,
        `#!/usr/bin/env bash\nprintf '%s\\n' "$*" >> ${JSON.stringify(join(dirname(original.root), 'ledgers', 'uv'))}\nexec ${JSON.stringify(realUv)} "$@"\n`,
      );
      await chmod(wrapper, 0o755);
      const venv = join(original.state, 'venv');
      const uv = { uvProjectEnvironment: venv, python: realPython };
      expect(await original.enterShell(uv)).toEqual(HEALTHY);
      expect(await original.enterShell(uv)).toEqual(HEALTHY);
      const settledRuns = original.uvRuns().length;
      expect(await original.enterShell(uv)).toEqual(HEALTHY);
      expect(original.uvRuns()).toHaveLength(settledRuns);
      expect(original.uvRuns().map(([verb]) => verb)).toEqual(['venv', 'sync']);

      const imported = Bun.spawnSync([join(venv, 'bin', 'python'), '-c', 'import tool; print(tool.__file__)'], {
        stdout: 'pipe',
        stderr: 'pipe',
      });
      expect(imported.exitCode).toBe(0);
      expect(imported.stdout.toString()).toContain(join(original.root, 'python', 'tool', 'src'));
      const script = Bun.spawnSync([join(venv, 'bin', 'tool')], { stdout: 'pipe', stderr: 'pipe' });
      expect(script.exitCode).toBe(0);
      expect(script.stdout.toString()).toBe('tool\n');

      const clone = join(dirname(original.root), 'clone');
      await cp(original.root, clone, { recursive: true, verbatimSymlinks: true });
      const cloneVenv = join(clone, 'tooling', 'direnv', '.devenv', 'state', 'venv');
      const copied = repository(clone, join(dirname(original.root), 'ledgers'), join(dirname(original.root), 'bin'));
      expect(await copied.enterShell({ uvProjectEnvironment: cloneVenv, python: realPython })).toEqual(HEALTHY);
      expect(copied.uvRuns()).toHaveLength(settledRuns);
      expect(copied.count('install')).toBe(1);
      const cloneImported = Bun.spawnSync(
        [join(cloneVenv, 'bin', 'python'), '-c', 'import tool; print(tool.__file__)'],
        { stdout: 'pipe', stderr: 'pipe' },
      );
      expect(cloneImported.exitCode).toBe(0);
      expect(cloneImported.stdout.toString()).toContain(join(clone, 'python', 'tool', 'src'));
      const cloneScript = Bun.spawnSync([join(cloneVenv, 'bin', 'tool')], { stdout: 'pipe', stderr: 'pipe' });
      expect(cloneScript.exitCode).toBe(0);
      expect(cloneScript.stdout.toString()).toBe('tool\n');
    });
  });
});
