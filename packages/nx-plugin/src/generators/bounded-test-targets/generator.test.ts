import { beforeEach, describe, expect, it } from 'bun:test';
import { addProjectConfiguration, readJson, type Tree } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import { BOUNDED_TEST_EXECUTOR } from '../../bounded-test-policy.js';
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
    expect(packageJson.scripts?.test).toBe('nx run example:test --tui=false --outputStyle=stream');
    expect(packageJson.nx?.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      dependsOn: ['typecheck-tests', '^build'],
      options: {
        command: 'bun test --target',
        cwd: '{projectRoot}',
        timeoutMs: 600000,
        killAfterMs: 10000,
      },
    });
  });

  it('normalizes test target from direct script', async () => {
    addProject(tree, 'example', 'packages/example');
    writeJsonFile(tree, 'packages/example/package.json', {
      name: '@scope/example',
      scripts: { test: 'bun test --script' },
    });

    await generator(tree, { project: 'packages/example' });

    const packageJson = readJson(tree, 'packages/example/package.json');
    expect(packageJson.scripts?.test).toBe('nx run example:test --tui=false --outputStyle=stream');
    expect(packageJson.nx?.targets?.test).toEqual({
      executor: BOUNDED_TEST_EXECUTOR,
      options: {
        command: 'bun test --script',
        cwd: '{projectRoot}',
        timeoutMs: 600000,
        killAfterMs: 10000,
      },
    });
  });
});

function addProject(tree: Tree, name: string, root: string): void {
  addProjectConfiguration(tree, name, {
    root,
    sourceRoot: `${root}/src`,
    projectType: 'library',
    targets: {},
  });
}

function writeJsonFile(tree: Tree, filePath: string, value: unknown): void {
  tree.write(filePath, `${JSON.stringify(value, null, 2)}\n`);
}
