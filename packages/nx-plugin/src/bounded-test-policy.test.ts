import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  applyBoundedTestTargetPolicy,
  applyWorkspaceBoundedTestTargetPolicy,
  BOUNDED_TEST_EXECUTOR,
  BOUNDED_TEST_KILL_AFTER_MS,
  BOUNDED_TEST_TIMEOUT_MS,
  type BoundedTestPolicyPackageJson,
  boundedTestScriptAlias,
  checkWorkspaceBoundedTestTargetPolicy,
  resolveTestCommand,
} from './bounded-test-policy.js';

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
        command: 'bun test --coverage',
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
    expect(packageJson.scripts?.test).toBe('nx run example:test --tui=false --outputStyle=stream');
  });

  it('uses a direct test script when no target command exists', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: 'bun test --pass-with-no-tests' },
    };

    applyBoundedTestTargetPolicy(packageJson, { projectName: '@scope/example' });

    expect(packageJson.nx?.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      options: {
        command: 'bun test --pass-with-no-tests',
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
    expect(packageJson.scripts?.test).toBe('nx run @scope/example:test --tui=false --outputStyle=stream');
  });

  it('does not preserve recursive nx run aliases as commands', () => {
    const packageJson: BoundedTestPolicyPackageJson = {
      scripts: { test: boundedTestScriptAlias('example') },
    };

    expect(resolveTestCommand(packageJson)).toBe('bun test');
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
          message: `nx.targets.test must use ${BOUNDED_TEST_EXECUTOR} with bounded test policy`,
        },
      ]);

      expect(applyWorkspaceBoundedTestTargetPolicy(root)).toBe(true);

      const app = JSON.parse(await readFile(join(root, 'packages/app/package.json'), 'utf8'));
      expect(app.scripts.test).toBe('nx run app:test --tui=false --outputStyle=stream');
      expect(app.nx.targets.test).toEqual({
        executor: BOUNDED_TEST_EXECUTOR,
        options: {
          command: 'bun test --pass-with-no-tests',
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

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
