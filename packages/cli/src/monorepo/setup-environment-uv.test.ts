import { describe, expect, it } from 'bun:test';
import { existsSync, lstatSync, readFileSync, realpathSync } from 'node:fs';
import { chmod, cp, mkdir, rm, symlink, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { edit, HEALTHY, repository, withManagedRepository } from './__tests__/helpers/shell-entry.js';

/**
 * What shell entry does for a uv project, through the real script with a recording `uv` on PATH, and with the real
 * `uv`. Which environments the witness admits and how each `.pth` line is rewritten is measured on the filesystem
 * alone in uv-relocation.test.ts; what is measured here is the installer's wiring of them: a shell that enables uv
 * creates the relocatable environment devenv names and syncs it once per changed input, a shell that does not syncs
 * nothing, a copy of the checkout enters installed, and an environment the witness refuses is removed and created again
 * before the sync.
 */
describe('what shell entry syncs for a uv project', () => {
  const PYTHON = '/nix/store/python3-env/bin/python3';
  const UV_PROJECT = {
    'pyproject.toml': '[project]\nname = "workspace"\n\n[tool.uv.workspace]\nmembers = ["python/*"]\n',
    'python/tool/pyproject.toml': '[project]\nname = "tool"\n',
    'python/tool/src/tool/__init__.py': '',
    'uv.lock': 'version = 1\n',
  };

  it('creates a relocatable environment, syncs it once per input change, syncs nothing without uv, enters a copy installed and replaces an environment that is not relocatable', async () => {
    await withManagedRepository({ files: UV_PROJECT }, async (original) => {
      const { root, state, enterShell: enter, uvRuns } = original;
      const venv = join(state, 'venv');
      const uv = { uvProjectEnvironment: venv, python: PYTHON };
      const create = ['venv', '--relocatable', '--python', PYTHON, venv];
      const sync = ['sync', '--python', PYTHON, '--all-packages', '--all-groups'];

      // Entered from another repository's uv shell, whose variable is inherited: this shell's module passed no
      // interpreter, so nothing syncs.
      expect(await enter({ uvProjectEnvironment: join(state, 'outer-venv') })).toEqual(HEALTHY);
      expect(uvRuns()).toEqual([]);

      // The environment is created relocatable where devenv names it, and synced once.
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(original.count('install')).toBe(1);
      expect(uvRuns()).toEqual([create, sync]);

      // Then once per changed input, a member's manifest and the lockfile, and not for an entry that changes neither.
      await edit(join(root, 'python/tool/pyproject.toml'), '[project]\nname = "tool"\ndependencies = ["attrs"]\n');
      expect(await enter(uv)).toEqual(HEALTHY);
      await edit(join(root, 'uv.lock'), 'version = 1\nrevision = 2\n');
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(await enter(uv)).toEqual(HEALTHY);
      const settled = [create, sync, sync, sync];
      expect(uvRuns()).toEqual(settled);

      // A copy-on-write clone: every byte, ignored state included, at another path. Its entry installs and syncs
      // nothing, and the member's .pth line resolves to the copy.
      const clone = join(dirname(root), 'clone');
      await cp(root, clone, { recursive: true, verbatimSymlinks: true });
      const cloneVenv = join(clone, 'tooling', 'direnv', '.devenv', 'state', 'venv');
      const copied = repository(clone, original.ledgers, join(dirname(root), 'bin'));
      expect(await copied.enterShell({ uvProjectEnvironment: cloneVenv, python: PYTHON })).toEqual(HEALTHY);
      expect(copied.uvRuns()).toEqual(settled);
      expect(copied.count('install')).toBe(1);
      // Python's site resolves a relative .pth line against site-packages.
      const cloneSite = join(cloneVenv, 'lib', 'python3.14', 'site-packages');
      expect(resolve(cloneSite, readFileSync(join(cloneSite, 'tool.pth'), 'utf8').trim())).toBe(
        join(clone, 'python/tool/src'),
      );
      expect(readFileSync(join(cloneSite, '_virtualenv.pth'), 'utf8')).toBe('import _virtualenv\n');

      // A stamped environment under a valid config whose launcher, interpreter link and .pth line still name this
      // environment or an old checkout absolutely. The stamp cannot witness any of them, so the entry removes the
      // environment and creates it again, relocatable, before the sync, instead of syncing over the escape and
      // stamping it.
      const site = join(venv, 'lib', 'python3.14', 'site-packages');
      const profile = join(dirname(root), 'old-checkout', '.devenv', 'profile', 'bin', 'python');
      await mkdir(dirname(profile), { recursive: true });
      await writeFile(profile, '');
      await symlink(profile, join(venv, 'bin', 'python'));
      await writeFile(join(venv, 'bin', 'pytest'), `#!${join(venv, 'bin', 'python')}\n`);
      await writeFile(join(site, 'tool.pth'), `${join(root, 'python/tool/src')}\n`);
      expect(await enter(uv)).toEqual(HEALTHY);
      const replaced = [...settled, create, sync];
      expect(uvRuns()).toEqual(replaced);
      expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8')).toContain('relocatable = true');
      expect(existsSync(join(venv, 'bin', 'pytest'))).toBe(false);
      expect(lstatSync(join(venv, 'bin', 'python'), { throwIfNoEntry: false })).toBeUndefined();
      expect(resolve(site, readFileSync(join(site, 'tool.pth'), 'utf8').trim())).toBe(join(root, 'python/tool/src'));
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(uvRuns()).toEqual(replaced);

      // An environment an older shell built: no stamp, and a config that does not say relocatable.
      await rm(venv, { recursive: true });
      await mkdir(venv, { recursive: true });
      await writeFile(join(venv, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
      await writeFile(join(venv, 'built-by-an-older-shell'), '');
      expect(await enter(uv)).toEqual(HEALTHY);
      const replacedAgain = [...replaced, create, sync];
      expect(uvRuns()).toEqual(replacedAgain);
      expect(existsSync(join(venv, 'built-by-an-older-shell'))).toBe(false);
      expect(readFileSync(join(venv, 'pyvenv.cfg'), 'utf8')).toContain('relocatable = true');
      expect(await enter(uv)).toEqual(HEALTHY);
      expect(uvRuns()).toEqual(replacedAgain);
    });
  });

  it('enters a real uv environment and its clone installed, the member importing from the clone', async () => {
    const realUv = Bun.which('uv');
    const pythonWhich = Bun.which('python3');
    if (realUv === null || pythonWhich === null) throw new Error('real uv and python3 must be on PATH');
    // The interpreter devenv passes is the store path the PATH python resolves to, not the
    // profile wrapper itself.
    const realPython = realpathSync(pythonWhich);
    // uv's own cache is not what this measures: the fixture's private XDG_CACHE_HOME would otherwise
    // hand uv an empty cache, and every run would fetch the uv_build backend from the index again.
    const uvCache = Bun.spawnSync([realUv, 'cache', 'dir'], { stdout: 'pipe', stderr: 'pipe' });
    if (uvCache.exitCode !== 0) throw new Error(`uv cache dir failed: ${uvCache.stderr.toString()}`);
    const env = { UV_CACHE_DIR: uvCache.stdout.toString().trim() };
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
      const uv = { uvProjectEnvironment: venv, python: realPython, env };
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
      expect(await copied.enterShell({ uvProjectEnvironment: cloneVenv, python: realPython, env })).toEqual(HEALTHY);
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
