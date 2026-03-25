/**
 * Integration tests: real lock file refresh against fixture repos.
 * SLOW -- requires network access for `bun install`.
 *
 * Tests that refreshLockFile correctly detects changes and only
 * touches the lock file (not package.json).
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { execa } from 'execa';
import { refreshLockFile } from '../../src/updaters/bun.js';
import { createTestRepo, type TestRepo } from './helpers/test-repo.js';

describe('refreshLockFile - real repo', () => {
  let repo: TestRepo;

  beforeEach(async () => {
    repo = await createTestRepo('patchnote-test-minimal', { installDeps: true });
  }, 120_000);

  afterEach(async () => {
    await repo.cleanup();
  });

  test('does not modify package.json', async () => {
    const before = await readFile(join(repo.path, 'package.json'), 'utf-8');

    const result = await refreshLockFile(repo.path, { packageManager: 'bun' });
    expect(result.error).toBeUndefined();

    const after = await readFile(join(repo.path, 'package.json'), 'utf-8');
    expect(after).toBe(before);
  }, 120_000);

  test('if changed, only lock file appears in git diff', async () => {
    const result = await refreshLockFile(repo.path, { packageManager: 'bun' });
    expect(result.error).toBeUndefined();

    if (result.changed) {
      const { stdout } = await execa('git', ['diff', '--name-only'], { cwd: repo.path });
      const changedFiles = stdout.split('\n').filter(Boolean);

      // Only lock files should change -- never package.json
      for (const file of changedFiles) {
        expect(file === 'bun.lock' || file === 'bun.lockb').toBe(true);
      }
      expect(changedFiles.length).toBeGreaterThan(0);
    }
  }, 120_000);

  test('running twice produces no additional changes', async () => {
    // First refresh
    await refreshLockFile(repo.path, { packageManager: 'bun' });
    // Commit the result
    await execa('git', ['add', '-A'], { cwd: repo.path });
    await execa('git', ['commit', '-m', 'refresh lock', '--allow-empty'], { cwd: repo.path });

    // Second refresh should find nothing new
    const result = await refreshLockFile(repo.path, { packageManager: 'bun' });
    expect(result.error).toBeUndefined();
    expect(result.changed).toBe(false);
  }, 120_000);
});
