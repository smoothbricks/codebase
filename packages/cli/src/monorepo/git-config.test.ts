import { describe, expect, it } from 'bun:test';
import { realpathSync } from 'node:fs';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { postCommitHookFileForTest } from './git-config.js';

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
