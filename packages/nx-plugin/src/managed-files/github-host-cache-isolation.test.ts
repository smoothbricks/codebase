import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';

const action = readFileSync(
  new URL('../../managed/templates/github/actions/setup-devenv/action.yml', import.meta.url),
  'utf8',
);
const assignments = action.split('\n').filter((line) => line.trimStart().startsWith('scope=$('));

function scope(overrides: Record<string, string> = {}): string {
  const assignment = assignments[0];
  if (!assignment) throw new Error('The setup action must scope its mutable host caches');
  const result = Bun.spawnSync(['bash', '-ceu', `${assignment}\nprintf '%s' "$scope"`], {
    env: {
      ...process.env,
      GITHUB_WORKFLOW: 'CI',
      GITHUB_REF: 'refs/pull/10/merge',
      GITHUB_JOB: 'main',
      RUNNER_OS: 'Linux',
      RUNNER_ARCH: 'X64',
      ...overrides,
    },
  });
  expect(result.exitCode).toBe(0);
  return result.stdout.toString();
}

describe('host CI mutable cache isolation', () => {
  test('Nx and devenv use the same exact workflow/ref/job/platform identity', () => {
    expect(assignments).toHaveLength(2);
    expect(assignments[0]).toBe(assignments[1]);
    expect(action).toContain('/${lane:-default}/$scope');
    expect(action).toContain('/${lane:-default}/$DEVENV_STATE_JOB/$scope');
    expect(action).toContain('NX_CACHE_DIRECTORY=$NX_CACHE_ROOT/cache');
    expect(action).toContain('NX_WORKSPACE_DATA_DIRECTORY=$NX_CACHE_ROOT/workspace-data');
  });

  test('repeat runs stay warm but distinct PRs, jobs and platforms cannot share graph state', () => {
    const original = scope();
    expect(original).toMatch(/^[a-f0-9]{40}$/);
    expect(scope({ GITHUB_RUN_ID: 'another-run', GITHUB_RUN_ATTEMPT: '2' })).toBe(original);
    const identities: Record<string, string>[] = [
      { GITHUB_WORKFLOW: 'Publish' },
      { GITHUB_REF: 'refs/pull/11/merge' },
      { GITHUB_REF: 'refs/heads/main' },
      { GITHUB_JOB: 'e2e-deployment' },
      { RUNNER_OS: 'macOS' },
      { RUNNER_ARCH: 'ARM64' },
    ];
    for (const other of identities) {
      expect(scope(other)).not.toBe(original);
    }
  });

  test('similar-looking refs do not collide or become filesystem paths', () => {
    expect(scope({ GITHUB_REF: 'refs/heads/feature/a' })).not.toBe(scope({ GITHUB_REF: 'refs/heads/feature-a' }));
    expect(scope({ GITHUB_REF: 'refs/heads/Feature' })).not.toBe(scope({ GITHUB_REF: 'refs/heads/feature' }));
    expect(scope({ GITHUB_REF: 'refs/heads/../../other' })).toMatch(/^[a-f0-9]{40}$/);
  });
});
