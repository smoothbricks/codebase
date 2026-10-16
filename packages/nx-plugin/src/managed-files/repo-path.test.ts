import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { managedAssetsRoot } from '../managed-assets.js';

interface RepoPathFixture {
  readonly root: string;
  readonly helper: string;
  readonly home: string;
  readonly venv: string;
  readonly installed: string;
  readonly tools: string;
  readonly entries: string[];
}

async function executable(path: string, marker: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `#!/bin/sh\nprintf '%s\\n' '${marker}'\n`, { mode: 0o755 });
}

async function withRepoPath(run: (fixture: RepoPathFixture) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-repo-path-'));
  try {
    const helper = join(root, 'tooling/direnv/repo-path');
    await mkdir(dirname(helper), { recursive: true });
    await writeFile(helper, await readFile(join(managedAssetsRoot, 'raw/tooling/direnv/repo-path')), { mode: 0o755 });
    const tools = join(root, 'system-bin');
    await mkdir(tools);
    // Resolve the script's tools before scrubbing PATH; /usr/bin:/bin is not
    // a coreutils installation on NixOS.
    for (const tool of ['bash', 'dirname']) {
      const resolved = Bun.which(tool);
      if (resolved === null) throw new Error(`${tool} must be on PATH`);
      await symlink(resolved, join(tools, tool));
    }
    const home = join(root, 'private home');
    const venv = join(root, 'active venv');
    const installed = join(home, 'Library/Application Support/dev.cowshed/bin');
    const entries = [
      join(root, 'tooling/direnv/.devenv/profile/bin'),
      join(root, 'tooling/git-hooks'),
      join(root, 'tooling'),
      join(root, 'tooling/node_modules/.bin'),
      join(root, 'node_modules/.bin'),
    ];
    await executable(join(entries[0], 'python3'), 'profile-python');
    await executable(join(entries[0], 'cargo'), 'profile-cargo');
    await executable(join(venv, 'bin/python3'), 'venv-python');
    await executable(join(venv, 'bin/cargo'), 'venv-cargo');
    await executable(join(installed, 'cargo'), 'installed-cargo');
    await run({ root, helper, home, venv, installed, tools, entries });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function helper(fixture: RepoPathFixture, args: readonly string[], virtualEnv?: string, path = fixture.tools) {
  const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper, ...args], {
    encoding: 'utf8',
    env: { HOME: fixture.home, PATH: path, ...(virtualEnv === undefined ? {} : { VIRTUAL_ENV: virtualEnv }) },
  });
  expect(result.status).toBe(0);
  expect(result.stderr).toBe('');
  return result.stdout;
}

async function expectModes(fixture: RepoPathFixture, entries: string[], virtualEnv?: string): Promise<void> {
  expect(helper(fixture, ['--print'], virtualEnv)).toBe(`${entries.join('\n')}\n`);
  const githubPath = join(fixture.root, 'github-path');
  await writeFile(githubPath, 'existing-entry\n');
  const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper, '--github-path'], {
    encoding: 'utf8',
    env: {
      HOME: fixture.home,
      PATH: fixture.tools,
      GITHUB_PATH: githubPath,
      ...(virtualEnv === undefined ? {} : { VIRTUAL_ENV: virtualEnv }),
    },
  });
  expect(result.status).toBe(0);
  expect(result.stderr).toBe('');
  expect(await readFile(githubPath, 'utf8')).toBe(`existing-entry\n${[...entries].reverse().join('\n')}\n`);
}

describe('managed repo-path', () => {
  it.each([undefined, ''])('preserves the exact legacy ordering without an active venv (%p)', async (virtualEnv) => {
    await withRepoPath(async (fixture) => {
      await expectModes(fixture, fixture.entries, virtualEnv);
      expect(helper(fixture, [], virtualEnv)).toBe(
        `${[fixture.installed, ...fixture.entries].join(':')}:${fixture.tools}\n`,
      );
    });
  });

  it.each([false, true])(
    'keeps active Python ahead of the profile and installed Cargo first (served=%p)',
    async (served) => {
      await withRepoPath(async (fixture) => {
        if (served) {
          await mkdir(join(fixture.root, '.cowshed'), { recursive: true });
          await writeFile(
            join(fixture.root, '.cowshed/env'),
            `export COWSHED_RUNTIME_LINK=fixture\nexport PATH=${JSON.stringify(fixture.installed)}:$PATH\n`,
          );
        }
        const entries = [join(fixture.venv, 'bin'), ...fixture.entries];
        await expectModes(fixture, entries, fixture.venv);
        const incoming = `${join(fixture.venv, 'bin')}:${fixture.tools}`;
        const path = helper(fixture, [], fixture.venv, incoming).trimEnd();
        expect(path).toBe(`${[fixture.installed, ...entries].join(':')}:${incoming}`);
        const selected = spawnSync(
          join(fixture.tools, 'bash'),
          ['-c', 'command -v python3; python3; command -v cargo; cargo'],
          {
            encoding: 'utf8',
            env: { HOME: fixture.home, PATH: path },
          },
        );
        expect(selected.status).toBe(0);
        expect(selected.stderr).toBe('');
        expect(selected.stdout).toBe(
          `${join(fixture.venv, 'bin/python3')}\nvenv-python\n${join(fixture.installed, 'cargo')}\ninstalled-cargo\n`,
        );
      });
    },
  );
});
