import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { CI_CONCURRENCY_GROUP } from './ci-workflow.js';

const action = readFileSync(
  new URL('../../managed/templates/github/actions/setup-devenv/action.yml', import.meta.url),
  'utf8',
);
const assignments = action.split('\n').filter((line) => line.trimStart().startsWith('scope=$('));
/** The steps that scope a mutable host cache, each from its `- name:` line to the next step's. */
const scopedSteps = action.split(/\n(?= {4}- name: )/).filter((step) => step.includes('scope=$('));
/** What CI's concurrency group is named after: a pull request's number, or the ref for pushes. */
const concurrencyIdentity = CI_CONCURRENCY_GROUP.slice('CI-'.length);

function scope(overrides: Record<string, string> = {}): string {
  const assignment = assignments[0];
  if (!assignment) throw new Error('The setup action must scope its mutable host caches');
  const result = Bun.spawnSync(['bash', '-ceu', `${assignment}\nprintf '%s' "$scope"`], {
    env: {
      ...process.env,
      GITHUB_WORKFLOW: 'CI',
      GITHUB_REF: 'refs/pull/10/merge',
      SCOPE_REF: '10',
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
  test('Nx and devenv use the same exact workflow/pull request or ref/job/platform identity', () => {
    expect(assignments).toHaveLength(2);
    expect(assignments[0]).toBe(assignments[1]);
    expect(action).toContain('/${lane:-default}/$scope');
    expect(action).toContain('/${lane:-default}/$DEVENV_STATE_JOB/$scope');
    expect(action).toContain('NX_CACHE_DIRECTORY=$NX_CACHE_ROOT/cache');
    expect(action).toContain('NX_WORKSPACE_DATA_DIRECTORY=$NX_CACHE_ROOT/workspace-data');
  });

  // A merged close carries the base branch's ref, so hashing github.ref gave two
  // pull requests' cleanups one scope while they ran in different concurrency
  // groups: two writers on one workspace-data, one .devenv and one bind target.
  test('both scopes hash what the CI concurrency group is named after, never the ref alone', () => {
    expect(scopedSteps).toHaveLength(2);
    for (const step of scopedSteps) {
      expect(step).toContain(`\n        SCOPE_REF: ${concurrencyIdentity}\n`);
    }
    for (const assignment of assignments) {
      expect(assignment).toContain('"$SCOPE_REF"');
      expect(assignment).not.toContain('GITHUB_REF');
    }
  });

  test('pull requests merged into one base branch keep their own scopes', () => {
    const mergedClose = { GITHUB_REF: 'refs/heads/main' };
    expect(scope({ ...mergedClose, SCOPE_REF: '101' })).not.toBe(scope({ ...mergedClose, SCOPE_REF: '102' }));
    expect(scope({ ...mergedClose, SCOPE_REF: '10' })).toBe(scope());
  });

  test('repeat runs stay warm but distinct PRs, jobs and platforms cannot share graph state', () => {
    const original = scope();
    expect(original).toMatch(/^[a-f0-9]{40}$/);
    expect(scope({ GITHUB_RUN_ID: 'another-run', GITHUB_RUN_ATTEMPT: '2' })).toBe(original);
    const identities: Record<string, string>[] = [
      { GITHUB_WORKFLOW: 'Publish' },
      { SCOPE_REF: '11' },
      { SCOPE_REF: 'refs/heads/main' },
      { GITHUB_JOB: 'e2e-deployment' },
      { RUNNER_OS: 'macOS' },
      { RUNNER_ARCH: 'ARM64' },
    ];
    for (const other of identities) {
      expect(scope(other)).not.toBe(original);
    }
  });

  test('similar-looking refs do not collide or become filesystem paths', () => {
    expect(scope({ SCOPE_REF: 'refs/heads/feature/a' })).not.toBe(scope({ SCOPE_REF: 'refs/heads/feature-a' }));
    expect(scope({ SCOPE_REF: 'refs/heads/Feature' })).not.toBe(scope({ SCOPE_REF: 'refs/heads/feature' }));
    expect(scope({ SCOPE_REF: 'refs/heads/../../other' })).toMatch(/^[a-f0-9]{40}$/);
  });
});
