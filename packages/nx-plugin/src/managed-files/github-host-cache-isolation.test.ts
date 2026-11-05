import { describe, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setupDevenvSteps } from '../__tests__/setup-devenv-steps.js';
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
  const assignment = assignments[1];
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
      CACHE_SCOPE: overrides.CACHE_SCOPE || overrides.GITHUB_JOB || 'main',
    },
  });
  expect(result.exitCode).toBe(0);
  return result.stdout.toString();
}

/** Execute all three managed host steps, recording only the privileged filesystem boundary. */
function mutablePaths(cacheScope = ''): string[] {
  const bind = mkdtempSync(join(tmpdir(), 'setup-matrix-paths-'));
  try {
    const githubEnv = join(bind, 'github-env');
    let workdir = '';
    let stdout = '';
    const stubs = [
      'mkdir() { printf "mkdir %s\n" "$*"; }',
      'rm() { printf "rm %s\n" "$*"; }',
      'ln() { printf "ln %s\n" "$*"; }',
      'mountpoint() { return 1; }',
      'sudo() { printf "sudo %s\n" "$*"; }',
    ].join('\n');
    for (const step of setupDevenvSteps().filter((candidate) =>
      String(candidate.get('name')).includes('on the shared bind'),
    )) {
      const result = Bun.spawnSync(
        ['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', `${stubs}\n${String(step.get('run'))}`],
        {
          cwd: bind,
          env: {
            ...process.env,
            NX_CACHE_BIND: join(bind, 'nx'),
            NX_CACHE_REPO: 'example/app',
            DEVENV_STATE_REPO: 'example/app',
            DEVENV_STATE_LANE: 'CI',
            DEVENV_STATE_JOB: 'main',
            CARGO_TARGET_BIND: join(bind, 'cargo-target'),
            CARGO_TARGET_REPO: 'example/app',
            CARGO_TARGET_LANE: 'CI',
            CACHE_SCOPE: cacheScope || 'main',
            SCOPE_REF: '10',
            BASE_REF: '',
            RECORDS_WORKFLOW: '',
            RECORDS_JOB: '',
            GITHUB_WORKFLOW: 'CI',
            GITHUB_JOB: 'main',
            GITHUB_WORKSPACE: bind,
            GITHUB_ENV: githubEnv,
            RUNNER_OS: 'Linux',
            RUNNER_ARCH: 'X64',
            DEVENV_WORKDIR: workdir,
          },
        },
      );
      expect(result.exitCode, result.stderr.toString()).toBe(0);
      stdout += result.stdout.toString();
      workdir = /^DEVENV_WORKDIR=(.+)$/m.exec(readFileSync(githubEnv, 'utf8'))?.[1] ?? '';
    }
    const exported = readFileSync(githubEnv, 'utf8');
    const required = (pattern: RegExp, text: string): string => {
      const value = pattern.exec(text)?.[1];
      if (value === undefined) throw new Error(`managed step omitted ${pattern}: ${text}`);
      return value.replaceAll(bind, '<bind>');
    };
    return [
      required(/^NX_CACHE_DIRECTORY=(.+)$/m, exported),
      required(/^NX_WORKSPACE_DATA_DIRECTORY=(.+)$/m, exported),
      required(/^ln -s (.+) tooling\/direnv\/\.devenv$/m, stdout),
      required(/^ln -s (.+) tooling\/direnv\/\.direnv$/m, stdout),
      required(/^DEVENV_WORKDIR=(.+)$/m, exported),
      required(/^cargo target: (.+) \(bound at /m, stdout),
      required(/^cargo target: .+ \(bound at (.+)\)$/m, stdout),
    ];
  } finally {
    rmSync(bind, { recursive: true, force: true });
  }
}

describe('host CI mutable cache isolation', () => {
  test('Nx, devenv and the Cargo target use the same exact workflow/pull request or ref/job/platform identity', () => {
    expect(assignments).toHaveLength(3);
    for (const step of scopedSteps) {
      expect(step).toContain('CACHE_SCOPE: ${{ inputs.cache-scope || github.job }}');
      expect(step).not.toMatch(/\bGITHUB_(?:WORKFLOW|JOB)=/);
    }
    expect(assignments[1]?.trim()).toBe(assignments[2]?.trim());
    expect(action).toContain('/${lane:-default}/$scope');
    expect(action).toContain('/${lane:-default}/$DEVENV_STATE_JOB/$scope');
    expect(action).toContain('NX_CACHE_DIRECTORY=$NX_CACHE_ROOT/cache');
    expect(action).toContain('NX_WORKSPACE_DATA_DIRECTORY=$NX_CACHE_ROOT/workspace-data');
    expect(action).toContain('root="$CARGO_TARGET_BIND/$CARGO_TARGET_REPO/${lane:-default}/$scope"');
  });

  // A merged close carries the base branch's ref, so hashing github.ref gave two
  // pull requests' cleanups one scope while they ran in different concurrency
  // groups: two writers on one workspace-data, one .devenv and one bind target.
  test('every scope hashes what the CI concurrency group is named after, never the ref alone', () => {
    expect(scopedSteps).toHaveLength(3);
    for (const step of scopedSteps) {
      expect(step).toContain(`\n        SCOPE_REF: ${concurrencyIdentity}\n`);
    }
    expect(action).toContain('NX_CACHE_ROOT=$(scope_root "$GITHUB_WORKFLOW" "$CACHE_SCOPE" "$SCOPE_REF")');
    for (const assignment of assignments.slice(1)) {
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

  test('two matrix identities isolate every mutable path and repeated runs keep them warm', () => {
    const first = mutablePaths('test-first');
    const second = mutablePaths('test-second');
    expect(first).toHaveLength(7);
    for (const [index, path] of first.entries()) expect(path).not.toBe(second[index]);
    expect(mutablePaths('test-first')).toEqual(first);
    expect(scope({ CACHE_SCOPE: 'test/first' })).not.toBe(scope({ CACHE_SCOPE: 'test-first' }));
    expect(scope({ CACHE_SCOPE: '../../other' })).toMatch(/^[a-f0-9]{40}$/);
  });
  test('unsafe cache identities refuse before a mutable path or environment write', () => {
    for (const step of scopedSteps) {
      const run = step.slice(step.indexOf('      run: |\n') + '      run: |\n'.length);
      for (const identity of ['test\nNODE_OPTIONS=bad', 'test\rbreak', 'test with space', '$(command)']) {
        const result = Bun.spawnSync(['bash', '-ceu', run], { env: { CACHE_SCOPE: identity } });
        expect(result.exitCode).toBe(2);
        expect(result.stderr.toString()).toContain('cache-scope must be a single-line job or matrix identity');
      }
    }
  });

  test('blank scope preserves the exact nonmatrix Nx, devenv, work and Cargo paths', () => {
    const identity = ['CI', '10', 'main', 'Linux', 'X64'].join('\0');
    const hash = createHash('sha1')
      .update(`blob ${Buffer.byteLength(identity)}\0${identity}`)
      .digest('hex');
    expect(scope()).toBe(hash);
    expect(mutablePaths()).toEqual([
      `<bind>/nx/example/app/ci/${hash}/cache`,
      `<bind>/nx/example/app/ci/${hash}/workspace-data`,
      `/var/cache/ci/devenv/example/app/ci/main/${hash}/.devenv`,
      `/var/cache/ci/devenv/example/app/ci/main/${hash}/.direnv`,
      `/work/example/app/ci/main/${hash}`,
      `<bind>/cargo-target/example/app/ci/${hash}`,
      `/work/example/app/ci/main/${hash}/target`,
    ]);
    expect(mutablePaths('main')).toEqual(mutablePaths());
  });
});
