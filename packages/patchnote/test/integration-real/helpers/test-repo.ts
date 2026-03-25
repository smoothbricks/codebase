/**
 * Test helper for creating isolated temporary repos from fixtures.
 *
 * Copies fixture files (excluding .git and node_modules) into a temp directory,
 * initializes a fresh git repo, and optionally runs `bun install`.
 */

import { existsSync } from 'node:fs';
import { cp, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { execa } from 'execa';

/**
 * Isolated temporary repo returned by `createTestRepo`.
 */
export interface TestRepo {
  /** Absolute path to the temp directory */
  path: string;
  /** Remove the temp directory */
  cleanup: () => Promise<void>;
}

/**
 * Create an isolated temp repo by copying a fixture and initializing git.
 *
 * @param fixtureName - Name of the fixture directory under `test-repos/`
 * @param opts.installDeps - If true, run `bun install` and commit the result
 */
export async function createTestRepo(fixtureName: string, opts: { installDeps?: boolean } = {}): Promise<TestRepo> {
  // Resolve fixture path: from test/integration-real/helpers/ -> Glider/test-repos/
  // helpers -> integration-real -> test -> patchnote -> packages -> smoothbricks -> Glider
  const fixtureDir = join(import.meta.dir, '..', '..', '..', '..', '..', '..', 'test-repos', fixtureName);

  if (!existsSync(fixtureDir)) {
    throw new Error(
      `Fixture directory not found: ${fixtureDir}\n` + `Expected fixture "${fixtureName}" at test-repos/${fixtureName}`,
    );
  }

  const tempDir = await mkdtemp(join(tmpdir(), 'patchnote-integration-'));

  // Copy fixture files (skip .git and node_modules)
  await cp(fixtureDir, tempDir, {
    recursive: true,
    filter: (src) => {
      const name = basename(src);
      return name !== 'node_modules' && name !== '.git';
    },
  });

  // Initialize a fresh git repository
  await execa('git', ['init', '-b', 'main'], { cwd: tempDir });
  await execa('git', ['config', 'user.email', 'test@test.com'], { cwd: tempDir });
  await execa('git', ['config', 'user.name', 'Test'], { cwd: tempDir });
  await execa('git', ['add', '-A'], { cwd: tempDir });
  await execa('git', ['commit', '-m', 'initial commit'], { cwd: tempDir });

  if (opts.installDeps) {
    await execa('bun', ['install'], { cwd: tempDir });
    await execa('git', ['add', '-A'], { cwd: tempDir });
    // Only commit if bun install produced changes (lock file may already be up to date)
    const { stdout } = await execa('git', ['status', '--porcelain'], { cwd: tempDir });
    if (stdout.trim().length > 0) {
      await execa('git', ['commit', '-m', 'install dependencies'], { cwd: tempDir });
    }
  }

  return {
    path: tempDir,
    cleanup: async () => {
      await rm(tempDir, { recursive: true, force: true });
    },
  };
}
