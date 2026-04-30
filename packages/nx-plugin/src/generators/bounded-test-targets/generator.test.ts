import { beforeEach, describe, expect, it } from 'bun:test';
import { addProjectConfiguration, readJson, type Tree } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import {
  BOUNDED_TEST_EXECUTOR,
  BOUNDED_TEST_KILL_AFTER_MS,
  BOUNDED_TEST_TIMEOUT_MS,
} from '../../bounded-test-policy.js';
import generator from './generator.js';

describe('bounded-test-targets generator', () => {
  let tree: Tree;

  beforeEach(() => {
    tree = createTreeWithEmptyWorkspace();
  });

  it('normalizes test target from existing run-commands target', async () => {
    addProject(tree, 'example', 'packages/example');
    writeJsonFile(tree, 'packages/example/package.json', {
      name: '@scope/example',
      scripts: { test: 'bun test --script' },
      nx: {
        name: 'example',
        targets: {
          test: {
            executor: 'nx:run-commands',
            dependsOn: ['typecheck-tests', '^build'],
            options: { command: 'bun test --target', cwd: 'packages/example' },
          },
        },
      },
    });

    await generator(tree, { project: '@scope/example' });

    const packageJson = readJson(tree, 'packages/example/package.json');
    expect(packageJson.scripts?.test).toBe('nx run example:test --outputStyle=stream');
    expect(packageJson.nx?.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      dependsOn: ['typecheck-tests', '^build'],
      options: {
        command: 'bun test --target',
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
  });

  it('normalizes test target from direct script', async () => {
    addProject(tree, 'example', 'packages/example');
    writeJsonFile(tree, 'packages/example/package.json', {
      name: '@scope/example',
      scripts: { test: 'bun test --script' },
      nx: { name: 'example' },
    });

    await generator(tree, { project: 'packages/example' });

    const packageJson = readJson(tree, 'packages/example/package.json');
    expect(packageJson.scripts?.test).toBe('nx run example:test --outputStyle=stream');
    expect(packageJson.nx?.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      options: {
        command: 'bun test --script',
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
  });

  it('normalizes project.json test target and rewrites package script alias', async () => {
    addProject(tree, 'example', 'packages/example', { keepProjectJson: true });
    writeJsonFile(tree, 'packages/example/package.json', {
      name: '@scope/example',
      scripts: { test: 'bun test --script' },
      nx: {
        targets: {
          test: {
            executor: 'nx:run-commands',
            options: { command: 'bun test --package-target' },
          },
        },
      },
    });
    writeJsonFile(tree, 'packages/example/project.json', {
      name: 'example',
      targets: {
        test: {
          executor: 'nx:run-commands',
          dependsOn: ['typecheck-tests'],
          options: { command: 'bun test --project-target', cwd: 'packages/example' },
        },
      },
    });

    await generator(tree, { project: 'example' });

    const packageJson = readJson(tree, 'packages/example/package.json');
    const projectJson = readJson(tree, 'packages/example/project.json');
    expect(packageJson.scripts?.test).toBe('nx run example:test --outputStyle=stream');
    expect(packageJson.nx?.targets?.test).toEqual({
      executor: 'nx:run-commands',
      options: { command: 'bun test --package-target' },
    });
    expect(projectJson.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      dependsOn: ['typecheck-tests'],
      options: {
        command: 'bun test --project-target',
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
  });
});

function addProject(tree: Tree, name: string, root: string, options: { keepProjectJson?: boolean } = {}): void {
  addProjectConfiguration(tree, name, {
    root,
    sourceRoot: `${root}/src`,
    projectType: 'library',
    targets: {},
  });
  if (!options.keepProjectJson && tree.exists(`${root}/project.json`)) {
    tree.delete(`${root}/project.json`);
  }
}

function writeJsonFile(tree: Tree, filePath: string, value: unknown): void {
  tree.write(filePath, `${JSON.stringify(value, null, 2)}\n`);
}
