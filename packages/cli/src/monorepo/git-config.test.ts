import { describe, expect, it } from 'bun:test';
import { realpathSync } from 'node:fs';
import { chmod, mkdir, mkdtemp, readlink, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { applyWorkspaceGitConfig, postCommitHookFileForTest } from './git-config.js';

const FOREIGN = `#!/bin/sh
# >>> git-backup post-commit >>>
command -v git-backup >/dev/null 2>&1 && git-backup agent kick >/dev/null 2>&1 || true
# <<< git-backup post-commit <<<
`;

describe('post-commit hook file', () => {
  it('creates an executable shell file that calls the managed script', () => {
    const created = postCommitHookFileForTest('');

    expect(created.startsWith('#!/usr/bin/env bash\n')).toBe(true);
    expect(created).toContain('tooling/git-hooks/post-commit.sh');
    expect(created.endsWith('\n')).toBe(true);
  });

  it('keeps another tool block, which is why this slot is not a symlink', () => {
    const installed = postCommitHookFileForTest(FOREIGN);

    expect(installed).toContain('git-backup agent kick');
    expect(installed.startsWith('#!/bin/sh\n')).toBe(true);
    expect(installed.indexOf('git-backup agent kick')).toBeLessThan(installed.indexOf('# >>> smoo post-commit >>>'));
  });

  it('replaces its own block instead of repeating it', () => {
    const once = postCommitHookFileForTest(FOREIGN);

    expect(postCommitHookFileForTest(once)).toBe(once);
    expect(once.split('# >>> smoo post-commit >>>')).toHaveLength(2);
  });

  it('replaces a block whose end marker was lost', () => {
    const truncated = `${FOREIGN}# >>> smoo post-commit >>>\n"$(git rev-parse --show-toplevel)/tooling/git-hooks/post-commit.sh"\n`;

    const repaired = postCommitHookFileForTest(truncated);

    expect(repaired).toBe(postCommitHookFileForTest(FOREIGN));
    expect(repaired.split('post-commit.sh"')).toHaveLength(2);
  });
});

describe('applyWorkspaceGitConfig', () => {
  // Shell entries managed before the direnv setup stopped importing workspace packages load this
  // export with `await import(...)` as their last bootstrap step, and a throw from it exits the
  // whole shell. It runs here the way they ran it: a process of its own that imports it the same
  // way, with none of this test run's cowshed environment.
  it('returns, naming the path and errno, when the hooks cannot be written', async () => {
    const root = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-git-config-')));
    const hooks = join(root, '.git', 'hooks');
    try {
      await mkdir(join(root, 'tooling', 'git-hooks'), { recursive: true });
      for (const hook of ['pre-commit', 'post-commit', 'commit-msg', 'pre-push']) {
        await writeFile(join(root, 'tooling', 'git-hooks', `${hook}.sh`), '#!/usr/bin/env bash\nexit 0\n');
      }
      expect(Bun.spawnSync(['git', 'init', '--quiet'], { cwd: root }).exitCode).toBe(0);
      await chmod(hooks, 0o555);
      const entry = Bun.spawnSync(
        [
          process.execPath,
          '-e',
          `const { applyWorkspaceGitConfig } = await import(${JSON.stringify(join(import.meta.dir, 'git-config.ts'))});` +
            `await applyWorkspaceGitConfig(${JSON.stringify(root)});`,
        ],
        { cwd: root, env: { PATH: process.env.PATH, HOME: process.env.HOME } },
      );
      await chmod(hooks, 0o755);
      const stderr = entry.stderr.toString();
      expect(entry.exitCode).toBe(0);
      expect(stderr).toContain('EACCES');
      expect(stderr).toContain(join(hooks, 'pre-commit'));
      expect(stderr).toContain('Git hooks and repository config were not applied');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('repository config convergence', () => {
  // The managed setup and this one share one convention: `include.path` names the workspace file
  // relative to the config's own directory, so a clone that copies `.git/config` names its own copy.
  it('writes include.path relative to the config directory and leaves a second run unchanged', async () => {
    const root = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-git-config-convergence-')));
    try {
      await mkdir(join(root, 'tooling', 'git-hooks'), { recursive: true });
      for (const hook of ['pre-commit', 'post-commit', 'commit-msg', 'pre-push']) {
        await writeFile(join(root, 'tooling', 'git-hooks', `${hook}.sh`), '#!/usr/bin/env bash\nexit 0\n');
      }
      await writeFile(join(root, 'tooling', 'workspace.gitconfig'), '[smoo]\n\tcheckout = test\n');
      expect(Bun.spawnSync(['git', 'init', '--quiet'], { cwd: root }).exitCode).toBe(0);

      await applyWorkspaceGitConfig(root);
      const first = Bun.spawnSync(['git', 'config', '--local', '--get', 'include.path'], { cwd: root });
      expect(first.stdout.toString().trim()).toBe('../tooling/workspace.gitconfig');

      const config = await Bun.file(join(root, '.git', 'config')).text();
      await applyWorkspaceGitConfig(root);
      expect(await Bun.file(join(root, '.git', 'config')).text()).toBe(config);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

const HOOK_NAMES = ['pre-commit', 'post-commit', 'commit-msg', 'pre-push'];

/** A checkout's tracked hook sources and the workspace config they include. */
async function workspaceSources(root: string): Promise<void> {
  await mkdir(join(root, 'tooling', 'git-hooks'), { recursive: true });
  for (const hook of HOOK_NAMES) {
    await writeFile(join(root, 'tooling', 'git-hooks', `${hook}.sh`), '#!/usr/bin/env bash\nexit 0\n');
  }
  await writeFile(join(root, 'tooling', 'workspace.gitconfig'), '[smoo]\n\tcheckout = test\n');
}

describe('read-only repository config and hooks', () => {
  // A checkout that is already converged needs no write, so a sandboxed job that denies writes to
  // its config and hooks must see the shell entry stay silent. The sandbox is the real seatbelt.
  it.skipIf(process.platform !== 'darwin')(
    'applies silently to an already converged, write-denied repository',
    async () => {
      const root = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-git-config-readonly-')));
      const config = join(root, '.git', 'config');
      const hooks = join(root, '.git', 'hooks');
      try {
        await workspaceSources(root);
        expect(Bun.spawnSync(['git', 'init', '--quiet'], { cwd: root }).exitCode).toBe(0);
        await applyWorkspaceGitConfig(root);
        const converged = await Bun.file(config).text();
        await chmod(config, 0o444);
        await chmod(hooks, 0o555);
        try {
          const profile = `(version 1)(allow default)(deny file-write* (literal ${JSON.stringify(config)}) (subpath ${JSON.stringify(hooks)}))`;
          const entry = Bun.spawnSync(
            [
              '/usr/bin/sandbox-exec',
              '-p',
              profile,
              process.execPath,
              '-e',
              `import { applyWorkspaceGitConfig } from ${JSON.stringify(join(import.meta.dir, 'git-config.ts'))};` +
                `await applyWorkspaceGitConfig(${JSON.stringify(root)});`,
            ],
            { cwd: root, env: { PATH: process.env.PATH, HOME: process.env.HOME } },
          );
          expect(entry.exitCode).toBe(0);
          expect(entry.stderr.toString()).toBe('');
          expect(await Bun.file(config).text()).toBe(converged);
        } finally {
          await chmod(hooks, 0o755);
          await chmod(config, 0o644);
        }
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    },
  );
});

describe('linked worktree repository config', () => {
  // A linked worktree's shell entry writes the primary repository's config, spelled relative to that
  // config, and a second entry leaves those bytes unchanged. Nothing names the linked worktree.
  it('wires the primary config to its own workspace file and converges on a second entry', async () => {
    const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-git-config-worktree-')));
    const primary = join(scratch, 'primary');
    const linked = join(scratch, 'linked');
    try {
      await mkdir(primary);
      await workspaceSources(primary);
      const git = (cwd: string, args: string[]): string => {
        const result = Bun.spawnSync(
          ['git', '-c', 'user.name=smoo', '-c', 'user.email=smoo@example.invalid', ...args],
          { cwd },
        );
        expect(result.exitCode).toBe(0);
        return result.stdout.toString();
      };
      git(primary, ['init', '--quiet']);
      git(primary, ['add', 'tooling']);
      git(primary, ['commit', '--quiet', '-m', 'workspace sources']);
      git(primary, ['worktree', 'add', '--quiet', '--detach', linked]);

      await applyWorkspaceGitConfig(linked);
      const hooks = join(primary, '.git', 'hooks');
      const config = await Bun.file(join(primary, '.git', 'config')).text();
      const postCommit = await Bun.file(join(hooks, 'post-commit')).text();
      const preCommit = await readlink(join(hooks, 'pre-commit'));
      expect(preCommit).toBe('../../tooling/git-hooks/pre-commit.sh');
      await applyWorkspaceGitConfig(linked);
      expect(await Bun.file(join(primary, '.git', 'config')).text()).toBe(config);
      expect(await Bun.file(join(hooks, 'post-commit')).text()).toBe(postCommit);
      expect(await readlink(join(hooks, 'pre-commit'))).toBe(preCommit);
      expect(git(primary, ['config', '--local', '--get', 'include.path']).trim()).toBe(
        '../tooling/workspace.gitconfig',
      );
      expect(config).not.toContain(linked);
    } finally {
      await rm(scratch, { recursive: true, force: true });
    }
  });
});
