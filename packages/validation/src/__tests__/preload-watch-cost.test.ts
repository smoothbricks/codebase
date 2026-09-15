import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const preload = join(packageRoot, 'src', 'bun', 'preload.ts');
const multiDirectoryEntry = join('test-fixtures', 'watch-cost', 'scripts', 'deploy.ts');

/**
 * Well under the cost of one project's worth of `fs.watch` registrations under
 * Bun on macOS, and far above the load itself: measured 2.36 s through the
 * polling seam against 204.87 s through ttsc's own watch fallback for this
 * eight-directory fixture. The child is killed at the same bound so a
 * regression fails in seconds instead of minutes.
 */
const BUDGET_MS = 30_000;

describe('ttsc directory watching', () => {
  it('loads a multi-directory project without paying per-directory watch registration', () => {
    const started = performance.now();
    const result = spawnSync(process.execPath, ['--preload', preload, multiDirectoryEntry], {
      cwd: packageRoot,
      encoding: 'utf8',
      timeout: BUDGET_MS,
    });
    const elapsedMs = performance.now() - started;

    if (result.status !== 0) console.error(result.stderr);
    expect(result.status).toBe(0);
    // Nine discriminating validators: the transform still reaches every module
    // the entry point imports, and its own callsite.
    expect(JSON.parse(result.stdout)).toEqual({ all: true, count: 9 });
    expect(elapsedMs).toBeLessThan(BUDGET_MS);
  }, 120_000);
});
