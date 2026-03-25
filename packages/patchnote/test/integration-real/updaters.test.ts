/**
 * Integration tests: real bun update against fixture repos.
 * SLOW -- requires network access for `bun update` and `bun install`.
 *
 * Tests that updateNpmDependencies and parsePackageJsonDiff produce
 * consistent, well-structured results against real package registries.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { execa } from 'execa';
import { getChangedFiles } from '../../src/git.js';
import { parsePackageJsonDiff, updateNpmDependencies } from '../../src/updaters/bun.js';
import { createTestRepo, type TestRepo } from './helpers/test-repo.js';

describe('updateNpmDependencies - real repo', () => {
  let repo: TestRepo;

  beforeEach(async () => {
    repo = await createTestRepo('patchnote-test-minimal', { installDeps: true });
  }, 120_000);

  afterEach(async () => {
    await repo.cleanup();
  });

  test('finds updates for pinned older versions and returns valid structure', async () => {
    const result = await updateNpmDependencies(repo.path, { recursive: true });

    expect(result.success).toBe(true);
    expect(result.ecosystem).toBe('npm');
    // The fixture has pinned older versions, so updates must be found
    expect(result.updates.length).toBeGreaterThan(0);

    for (const update of result.updates) {
      expect(update.name.length).toBeGreaterThan(0);
      expect(update.fromVersion).toMatch(/^\d+\.\d+/);
      expect(update.toVersion).toMatch(/^\d+\.\d+/);
      expect(update.fromVersion).not.toBe(update.toVersion);
      expect(['major', 'minor', 'patch', 'unknown']).toContain(update.updateType);
      expect(update.ecosystem).toBe('npm');
    }
  }, 120_000);

  test('parsePackageJsonDiff agrees with updateNpmDependencies on updated packages', async () => {
    const result = await updateNpmDependencies(repo.path, { recursive: true });
    expect(result.success).toBe(true);
    expect(result.updates.length).toBeGreaterThan(0);

    const { stdout: diff } = await execa('git', ['diff'], { cwd: repo.path });
    const parsed = parsePackageJsonDiff(diff);

    expect(parsed.length).toBeGreaterThan(0);

    // Every package parsePackageJsonDiff finds should also appear in updateNpmDependencies result
    const updateNames = new Set(result.updates.map((u) => u.name));
    for (const p of parsed) {
      expect(updateNames.has(p.name)).toBe(true);
    }

    // Verify parsed entries have from→to version changes (not same version)
    for (const p of parsed) {
      expect(p.fromVersion).not.toBe(p.toVersion);
    }
  }, 120_000);

  test('only package.json and bun.lock are modified after update', async () => {
    await updateNpmDependencies(repo.path, { recursive: true });

    const changed = await getChangedFiles(repo.path);
    for (const file of changed) {
      expect(file === 'package.json' || file === 'bun.lock' || file === 'bun.lockb').toBe(true);
    }
  }, 120_000);
});
