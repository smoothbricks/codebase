/**
 * Tests for consumer-workspace packages-dir resolution.
 *
 * The resolver must work when @smoothbricks/lmao is an installed dependency
 * (Bun isolated store) of a foreign workspace — the regression that left
 * consumer repos' .trace-results.db silently stale: resolution anchored on the
 * library's own file location instead of the test process cwd.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { findWorkspacePackagesDir } from '../workspace-packages-dir.js';

const roots: string[] = [];

function makeTree(paths: readonly string[], files: readonly string[]): string {
  const root = mkdtempSync(join(tmpdir(), 'lmao-wpd-'));
  roots.push(root);
  for (const p of paths) mkdirSync(join(root, p), { recursive: true });
  for (const f of files) writeFileSync(join(root, f), '');
  return root;
}

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

describe('findWorkspacePackagesDir', () => {
  it('finds the packages dir from a nested package cwd via the root lockfile', () => {
    const root = makeTree(['packages/some-app/src'], ['bun.lock']);
    const result = findWorkspacePackagesDir(join(root, 'packages', 'some-app', 'src'), {});
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.packagesDir).toBe(join(root, 'packages'));
      expect(result.source).toBe('workspace-root');
    }
  });

  it('resolves in a foreign consumer workspace regardless of where the library is installed', () => {
    // Consumer repo whose lmao copy lives under the Bun isolated store — the
    // resolver must anchor on cwd, never on any path related to the store.
    const root = makeTree(
      ['packages/consumer-pkg', 'node_modules/.bun/@smoothbricks+lmao@0.0.0+cafebabe/node_modules/@smoothbricks/lmao'],
      ['bun.lock'],
    );
    const result = findWorkspacePackagesDir(join(root, 'packages', 'consumer-pkg'), {});
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.packagesDir).toBe(join(root, 'packages'));
  });

  it('walks past a nested lockfile whose root has no packages dir', () => {
    const root = makeTree(['packages/host/examples/demo'], ['bun.lock', 'packages/host/examples/demo/bun.lock']);
    const result = findWorkspacePackagesDir(join(root, 'packages', 'host', 'examples', 'demo'), {});
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.packagesDir).toBe(join(root, 'packages'));
  });

  it('accepts bun.lockb as a workspace marker', () => {
    const root = makeTree(['packages/p'], ['bun.lockb']);
    const result = findWorkspacePackagesDir(join(root, 'packages', 'p'), {});
    expect(result.ok).toBe(true);
  });

  it('honors an absolute LMAO_PACKAGES_DIR override', () => {
    const root = makeTree(['custom-layout/libs'], []);
    const override = join(root, 'custom-layout', 'libs');
    const result = findWorkspacePackagesDir(root, { LMAO_PACKAGES_DIR: override });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.packagesDir).toBe(override);
      expect(result.source).toBe('env');
    }
  });

  it('resolves a relative LMAO_PACKAGES_DIR against the start dir', () => {
    const root = makeTree(['libs'], []);
    const result = findWorkspacePackagesDir(root, { LMAO_PACKAGES_DIR: 'libs' });
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.packagesDir).toBe(join(root, 'libs'));
  });

  it('reports a nonexistent override as not found, with the searched path', () => {
    const root = makeTree([], []);
    const missing = join(root, 'nope');
    const result = findWorkspacePackagesDir(root, { LMAO_PACKAGES_DIR: missing });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.searched).toEqual([missing]);
  });
});
