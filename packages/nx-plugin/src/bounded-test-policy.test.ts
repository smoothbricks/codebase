import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  applyBoundedTestTargetPolicy,
  applyWorkspaceBoundedTestTargetPolicy,
  BOUNDED_TEST_EXECUTOR,
  BOUNDED_TEST_KILL_AFTER_MS,
  BOUNDED_TEST_PER_TEST_TIMEOUT_MS,
  BOUNDED_TEST_TIMEOUT_MS,
  type BoundedTestPolicyPackageJson,
  type BoundedTestPolicyProjectJson,
  boundedTestScriptAlias,
  checkBoundedTestTargetPolicy,
  checkWorkspaceBoundedTestTargetPolicy,
  checkWorkspaceCargoTestReachabilityPolicy,
  ensureBunTestTimeoutFlag,
  resolveTestCommand,
} from './bounded-test-policy.js';
import type { ResolvedProjectTargets } from './package-target-policy.js';

const TIMEOUT_FLAG = `--timeout=${BOUNDED_TEST_PER_TEST_TIMEOUT_MS}`;
const BOUNDED_POLICY_MESSAGE = `nx.targets.test must use ${BOUNDED_TEST_EXECUTOR} or delegate through no-op targets to bounded test execution`;

describe('bounded test target policy', () => {
  it('preserves an existing nx:run-commands test command', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: 'bun test --old' },
      nx: {
        targets: {
          test: {
            executor: 'nx:run-commands',
            dependsOn: ['typecheck-tests'],
            options: { command: 'bun test --coverage', cwd: 'packages/example' },
          },
        },
      },
    };

    applyBoundedTestTargetPolicy(packageJson, { projectName: 'example' });

    expect(packageJson.nx?.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      dependsOn: ['typecheck-tests'],
      options: {
        command: `bun test ${TIMEOUT_FLAG} --coverage`,
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
    expect(packageJson.scripts?.test).toBe('nx run example:test --outputStyle=stream');
  });

  it('uses a direct test script when no target command exists', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: 'bun test --pass-with-no-tests' },
    };

    applyBoundedTestTargetPolicy(packageJson, { projectName: '@scope/example' });

    expect(packageJson.nx?.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      options: {
        command: `bun test ${TIMEOUT_FLAG} --pass-with-no-tests`,
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
    expect(packageJson.scripts?.test).toBe('nx run @scope/example:test --outputStyle=stream');
  });

  it('normalizes project.json targets while reading package test scripts', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: 'bun test --script' },
      nx: {
        targets: {
          test: {
            executor: 'nx:run-commands',
            options: { command: 'bun test --package-target' },
          },
        },
      },
    };
    const projectJson: BoundedTestPolicyProjectJson = {
      targets: {
        test: {
          executor: 'nx:run-commands',
          dependsOn: ['typecheck-tests'],
          options: { command: 'bun test --project-target', cwd: 'packages/example' },
        },
      },
    };

    applyBoundedTestTargetPolicy(packageJson, { projectName: 'example', projectJson });

    expect(projectJson.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      dependsOn: ['typecheck-tests'],
      options: {
        command: `bun test ${TIMEOUT_FLAG} --project-target`,
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
    expect(packageJson.nx?.targets?.test).toEqual({
      executor: 'nx:run-commands',
      options: { command: 'bun test --package-target' },
    });
    expect(packageJson.scripts?.test).toBe('nx run example:test --outputStyle=stream');
  });

  it('creates project.json test targets from package test scripts', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: 'bun test --script' },
    };
    const projectJson: BoundedTestPolicyProjectJson = {
      targets: {},
    };

    applyBoundedTestTargetPolicy(packageJson, { projectName: 'example', projectJson });

    expect(projectJson.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      options: {
        command: `bun test ${TIMEOUT_FLAG} --script`,
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
    expect(packageJson.nx).toBeUndefined();
    expect(packageJson.scripts?.test).toBe('nx run example:test --outputStyle=stream');
  });

  it('requires bounded targets in project.json when project config exists', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: boundedTestScriptAlias('example') },
      nx: {
        targets: {
          test: {
            executor: BOUNDED_TEST_EXECUTOR,
            options: {
              command: 'bun test --package-target',
              cwd: '{projectRoot}',
              timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
              killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
            },
          },
        },
      },
    };
    const projectJson: BoundedTestPolicyProjectJson = { targets: {} };

    expect(checkBoundedTestTargetPolicy(packageJson, { projectName: 'example', projectJson })).toBe(false);
  });

  it('accepts an executorless aggregate whose resolved execution target is bounded', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      nx: { targets: { test: { dependsOn: ['napi-test'] } } },
    };
    const resolvedProject = resolvedAggregateProject();

    expect(checkBoundedTestTargetPolicy(packageJson, { projectName: 'cowshed', resolvedProject })).toBe(true);
  });

  it('rejects aggregate bypass scripts, unbounded leaves, and dependency cycles', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      nx: { targets: { test: { dependsOn: ['napi-test'] } } },
    };
    const unboundedLeaf = resolvedAggregateProject();
    unboundedLeaf.targetExecutors = new Map([
      ['test', 'nx:noop'],
      ['napi-test', 'nx:run-commands'],
      ['build', 'nx:run-commands'],
    ]);
    expect(checkBoundedTestTargetPolicy(packageJson, { projectName: 'cowshed', resolvedProject: unboundedLeaf })).toBe(
      false,
    );

    packageJson.scripts = { test: 'bun test' };
    expect(
      checkBoundedTestTargetPolicy(packageJson, {
        projectName: 'cowshed',
        resolvedProject: resolvedAggregateProject(),
      }),
    ).toBe(false);

    delete packageJson.scripts;
    const cycle = resolvedAggregateProject();
    cycle.targetExecutors = new Map([
      ['test', 'nx:noop'],
      ['napi-test', 'nx:noop'],
    ]);
    cycle.targetDependencies = new Map([
      ['test', ['napi-test']],
      ['napi-test', ['test']],
    ]);
    expect(checkBoundedTestTargetPolicy(packageJson, { projectName: 'cowshed', resolvedProject: cycle })).toBe(false);
  });

  it('does not preserve recursive nx run aliases as commands', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: boundedTestScriptAlias('example') },
    };

    expect(resolveTestCommand(packageJson)).toBe(`bun test ${TIMEOUT_FLAG}`);
  });

  it('idempotently appends --timeout to bare bun test commands', () => {
    expect(ensureBunTestTimeoutFlag('bun test')).toBe(`bun test ${TIMEOUT_FLAG}`);
    expect(ensureBunTestTimeoutFlag('bun test --pass-with-no-tests')).toBe(
      `bun test ${TIMEOUT_FLAG} --pass-with-no-tests`,
    );
    expect(ensureBunTestTimeoutFlag(`bun test ${TIMEOUT_FLAG}`)).toBe(`bun test ${TIMEOUT_FLAG}`);
    expect(ensureBunTestTimeoutFlag('bun test --timeout=10')).toBe(`bun test ${TIMEOUT_FLAG}`);
    expect(ensureBunTestTimeoutFlag('bun test --timeout 10 --bail')).toBe(`bun test ${TIMEOUT_FLAG} --bail`);
    // Non-bun-test commands are left alone (vitest, custom runners, etc.).
    expect(ensureBunTestTimeoutFlag('vitest run')).toBe('vitest run');
    expect(ensureBunTestTimeoutFlag('bun run test')).toBe('bun run test');
  });

  it('checks and fixes workspace package test targets', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-policy-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test --pass-with-no-tests' },
        nx: { name: 'app' },
      });

      expect(checkWorkspaceBoundedTestTargetPolicy(root)).toEqual([
        {
          path: join(root, 'packages/app/package.json'),
          message: BOUNDED_POLICY_MESSAGE,
        },
      ]);

      expect(applyWorkspaceBoundedTestTargetPolicy(root)).toBe(true);

      const app = JSON.parse(await readFile(join(root, 'packages/app/package.json'), 'utf8'));
      expect(app.scripts.test).toBe('nx run app:test --outputStyle=stream');
      expect(app.nx.targets.test).toEqual({
        executor: BOUNDED_TEST_EXECUTOR,
        options: {
          command: `bun test ${TIMEOUT_FLAG} --pass-with-no-tests`,
          cwd: '{projectRoot}',
          timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
          killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
        },
      });
      expect(checkWorkspaceBoundedTestTargetPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('leaves multi-command and non-bun test targets for the check to report', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-policy-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      const corpusTarget = {
        executor: 'nx:run-commands',
        options: {
          commands: ['bun test test/', 'cd go && go test ./...'],
          parallel: false,
          cwd: 'packages/schema',
        },
      };
      const cargoTarget = {
        executor: 'nx:run-commands',
        options: { command: 'cargo test --workspace', cwd: 'packages/native' },
      };
      await writeJson(join(root, 'packages/schema/package.json'), {
        name: '@scope/schema',
        nx: { name: 'schema', targets: { test: corpusTarget } },
      });
      await writeJson(join(root, 'packages/native/package.json'), {
        name: '@scope/native',
        nx: { name: 'native', targets: { test: cargoTarget } },
      });

      expect(applyWorkspaceBoundedTestTargetPolicy(root)).toBe(false);

      const schema = JSON.parse(await readFile(join(root, 'packages/schema/package.json'), 'utf8'));
      expect(schema.nx.targets.test).toEqual(corpusTarget);
      const native = JSON.parse(await readFile(join(root, 'packages/native/package.json'), 'utf8'));
      expect(native.nx.targets.test).toEqual(cargoTarget);
      // Still reported: apply refusing to flatten does not silently bless them.
      expect(checkWorkspaceBoundedTestTargetPolicy(root).length).toBe(2);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('preserves a resolved bounded aggregate during workspace updates', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-policy-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      const packagePath = join(root, 'packages/cowshed/package.json');
      await writeJson(packagePath, {
        name: '@scope/cowshed',
        nx: { name: 'cowshed', targets: { test: { dependsOn: ['napi-test'] } } },
      });
      const options = { resolvedTargetsByProject: new Map([['cowshed', resolvedAggregateProject()]]) };

      expect(checkWorkspaceBoundedTestTargetPolicy(root, options)).toEqual([]);
      expect(applyWorkspaceBoundedTestTargetPolicy(root, options)).toBe(false);
      expect(JSON.parse(await readFile(packagePath, 'utf8'))).toEqual({
        name: '@scope/cowshed',
        nx: { name: 'cowshed', targets: { test: { dependsOn: ['napi-test'] } } },
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('checks and fixes workspace project.json test targets', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-policy-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test --pass-with-no-tests' },
      });
      await writeJson(join(root, 'packages/app/project.json'), {
        name: 'app',
        targets: {
          test: {
            executor: 'nx:run-commands',
            options: { command: 'bun test --project', cwd: 'packages/app' },
          },
        },
      });

      expect(checkWorkspaceBoundedTestTargetPolicy(root)).toEqual([
        {
          path: join(root, 'packages/app/project.json'),
          message: BOUNDED_POLICY_MESSAGE.replace('nx.targets', 'targets'),
        },
      ]);

      expect(applyWorkspaceBoundedTestTargetPolicy(root)).toBe(true);

      const appPackage = JSON.parse(await readFile(join(root, 'packages/app/package.json'), 'utf8'));
      const appProject = JSON.parse(await readFile(join(root, 'packages/app/project.json'), 'utf8'));
      expect(appPackage.scripts.test).toBe('nx run app:test --outputStyle=stream');
      expect(appPackage.nx).toBeUndefined();
      expect(appProject.targets.test).toEqual({
        executor: BOUNDED_TEST_EXECUTOR,
        options: {
          command: `bun test ${TIMEOUT_FLAG} --project`,
          cwd: '{projectRoot}',
          timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
          killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
        },
      });
      expect(checkWorkspaceBoundedTestTargetPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('cargo-test reachability policy', () => {
  it('flags a cargo workspace whose test aggregate never reaches cargo-test', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cargo-reach-'));
    try {
      await writeJson(join(root, 'package.json'), { name: '@scope/root', private: true, workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/rusty/package.json'), { name: 'rusty', nx: { name: 'rusty' } });

      // The shape that shipped: nx.json targetDefaults replaced the inferred
      // dependsOn: [cargo-test] with [^build, build], so `nx test rusty` was a
      // no-op that ran nothing and reported success.
      const clobbered: ResolvedProjectTargets = {
        root: 'packages/rusty',
        targets: new Set(['test', 'cargo-test', 'build']),
        targetDependencies: new Map([['test', ['^build', 'build']]]),
        targetExecutors: new Map([
          ['test', 'nx:noop'],
          ['cargo-test', 'nx:noop'],
          ['build', 'nx:run-commands'],
        ]),
        // cargo-test is the runner for a single-crate workspace.
        targetOptions: new Map([['cargo-test', { command: 'cargo --frozen test --workspace' }]]),
      };
      const issues = checkWorkspaceCargoTestReachabilityPolicy(root, {
        resolvedTargetsByProject: new Map([['rusty', clobbered]]),
      });
      expect(issues).toHaveLength(1);
      expect(issues[0]?.message).toContain('cargo-test');

      // Transitive reachability counts: cowshed reaches it through napi-test.
      const transitive: ResolvedProjectTargets = {
        ...clobbered,
        targets: new Set(['test', 'napi-test', 'cargo-test', 'build']),
        targetDependencies: new Map([
          ['test', ['napi-test']],
          ['napi-test', ['cargo-test']],
        ]),
      };
      expect(
        checkWorkspaceCargoTestReachabilityPolicy(root, {
          resolvedTargetsByProject: new Map([['rusty', transitive]]),
        }),
      ).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('flags a cargo-test that reaches only compile targets, never a runner', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cargo-reach-noop-'));
    try {
      await writeJson(join(root, 'package.json'), { name: '@scope/root', private: true, workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/rusty/package.json'), { name: 'rusty', nx: { name: 'rusty' } });

      // The shape a partial package-local override produces: declaring only
      // dependsOn on cargo-test replaces the WHOLE inferred target, so the
      // executor becomes nx:noop with empty options. `test` still reaches
      // cargo-test, and cargo-test still reaches real work — but the only cargo
      // invocation is `--no-run`, so no test is ever executed and the target is
      // green. Reachability alone cannot see this.
      const compileOnly: ResolvedProjectTargets = {
        root: 'packages/rusty',
        targets: new Set(['test', 'cargo-test', 'cargo-test-compile', 'cargo-wasm']),
        targetDependencies: new Map([
          ['test', ['cargo-test']],
          ['cargo-test', ['cargo-test-compile', 'cargo-wasm']],
        ]),
        targetExecutors: new Map([
          ['test', 'nx:noop'],
          ['cargo-test', 'nx:noop'],
          ['cargo-test-compile', 'nx:run-commands'],
          ['cargo-wasm', 'nx:run-commands'],
        ]),
        targetOptions: new Map([
          ['cargo-test-compile', { command: 'cargo --frozen test --workspace --no-run' }],
          ['cargo-wasm', { command: 'just wasm' }],
        ]),
      };
      const issues = checkWorkspaceCargoTestReachabilityPolicy(root, {
        resolvedTargetsByProject: new Map([['rusty', compileOnly]]),
      });
      expect(issues).toHaveLength(1);
      expect(issues[0]?.message).toContain('RUNS tests');

      // Restoring a real runner clears it.
      const withRunner: ResolvedProjectTargets = {
        ...compileOnly,
        targets: new Set([...compileOnly.targets, 'cargo-test-rusty-core']),
        targetDependencies: new Map([
          ['test', ['cargo-test']],
          ['cargo-test', ['cargo-test-rusty-core']],
          ['cargo-test-rusty-core', ['cargo-test-compile']],
        ]),
        targetOptions: new Map([
          ...compileOnly.targetOptions,
          ['cargo-test-rusty-core', { command: 'cargo --frozen nextest run --workspace --package rusty-core' }],
        ]),
      };
      expect(
        checkWorkspaceCargoTestReachabilityPolicy(root, {
          resolvedTargetsByProject: new Map([['rusty', withRunner]]),
        }),
      ).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('ignores projects with no cargo-test target', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cargo-reach-ts-'));
    try {
      await writeJson(join(root, 'package.json'), { name: '@scope/root', private: true, workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/tsonly/package.json'), { name: 'tsonly', nx: { name: 'tsonly' } });
      const tsOnly: ResolvedProjectTargets = {
        root: 'packages/tsonly',
        targets: new Set(['test', 'build']),
        targetDependencies: new Map([['test', ['build']]]),
        targetExecutors: new Map([['test', BOUNDED_TEST_EXECUTOR]]),
        targetOptions: new Map(),
      };
      expect(
        checkWorkspaceCargoTestReachabilityPolicy(root, {
          resolvedTargetsByProject: new Map([['tsonly', tsOnly]]),
        }),
      ).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

function resolvedAggregateProject(): ResolvedProjectTargets {
  return {
    root: 'packages/cowshed',
    targets: new Set(['test', 'napi-test', 'build']),
    targetDependencies: new Map([
      ['test', ['napi-test']],
      ['napi-test', ['build']],
    ]),
    targetExecutors: new Map([
      ['test', 'nx:noop'],
      ['napi-test', BOUNDED_TEST_EXECUTOR],
      ['build', 'nx:run-commands'],
    ]),
    targetOptions: new Map([
      [
        'napi-test',
        {
          command: `bun test ${TIMEOUT_FLAG} src/native.test.ts`,
          cwd: 'packages/cowshed',
          timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
          killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
        },
      ],
      ['build', { command: 'bun run build' }],
    ]),
  };
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
