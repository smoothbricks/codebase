import { describe, expect, it } from 'bun:test';
import {
  formatProjectTargetLines,
  projectNamesWithTarget,
  projectRootFromNxProjectJson,
  projectTargetsFromNxProjects,
  targetDependenciesFromNxProjectJson,
  targetNamesFromNxProjectJson,
  targetNamesFromProjects,
  targetOutputsFromNxProjectJson,
} from './index.js';

describe('Nx helper output formatting', () => {
  it('formats project target pairs like the old root helper script', () => {
    expect(
      formatProjectTargetLines([
        { project: 'web', targets: ['test', 'build'] },
        { project: 'cli', targets: ['lint'] },
      ]),
    ).toBe(['cli:lint', 'web:build', 'web:test'].join('\n'));
  });

  it('lists projects that define the requested target', () => {
    expect(
      projectNamesWithTarget(
        [
          { project: 'web', targets: ['build', 'test'] },
          { project: 'cli', targets: ['build', 'lint'] },
          { project: 'docs', targets: ['serve'] },
        ],
        'build',
      ),
    ).toEqual(['cli', 'web']);
  });

  it('extracts sorted target names from Nx project JSON', () => {
    expect(targetNamesFromNxProjectJson({ targets: { test: {}, build: {}, lint: {} } })).toEqual([
      'build',
      'lint',
      'test',
    ]);
  });

  it('extracts resolved roots, output declarations, and same-project target dependencies', () => {
    const metadata = {
      root: 'packages/native',
      targets: {
        'build-macos': {
          outputs: ['{projectRoot}/dist/*.dmg'],
          dependsOn: ['^build', 'compile-macos', { target: 'package-macos', projects: 'self' }],
        },
        test: { outputs: [] },
      },
    };

    expect(projectRootFromNxProjectJson(metadata)).toBe('packages/native');
    expect(targetOutputsFromNxProjectJson(metadata)).toEqual(
      new Map([
        ['build-macos', ['{projectRoot}/dist/*.dmg']],
        ['test', []],
      ]),
    );
    expect(targetDependenciesFromNxProjectJson(metadata)).toEqual(
      new Map([['build-macos', ['^build', 'compile-macos', 'package-macos']]]),
    );
  });

  it('omits cross-project dependencies from same-project target closures', () => {
    expect(
      targetDependenciesFromNxProjectJson({
        targets: {
          'rust-wasm': {
            dependsOn: [
              'prepare',
              { target: 'cargo-wasm', projects: ['lmao'] },
              { target: 'build', projects: 'dependencies' },
            ],
          },
        },
      }),
    ).toEqual(new Map([['rust-wasm', ['prepare']]]));
  });

  it('treats missing target metadata as an empty project', () => {
    expect(targetNamesFromNxProjectJson({ name: 'cli' })).toEqual([]);
  });

  it('projects every resolved graph node without per-project CLI parsing', () => {
    const projects = {
      web: {
        root: 'packages/web',
        targets: {
          test: {
            executor: '@smoothbricks/nx-plugin:bounded-exec',
            cache: true,
            dependsOn: ['build'],
            inputs: ['default', { runtime: 'echo version' }],
            options: { command: 'bun test', timeoutMs: 120_000 },
          },
          build: { outputs: ['{projectRoot}/dist/'] },
        },
      },
      cli: { root: 'packages/cli', targets: { lint: {} } },
    };

    expect(targetNamesFromProjects(projects).sort()).toEqual(['build', 'lint', 'test']);
    expect(projectTargetsFromNxProjects(projects)).toEqual([
      {
        project: 'cli',
        root: 'packages/cli',
        targets: ['lint'],
        targetCache: new Map(),
        targetDependencies: new Map(),
        localTargetDependencies: new Map([['lint', []]]),
        targetExecutors: new Map(),
        targetInputs: new Map(),
        targetOptions: new Map(),
        targetOutputs: new Map(),
        targetScripts: new Map(),
      },
      {
        project: 'web',
        root: 'packages/web',
        targets: ['build', 'test'],
        targetCache: new Map([['test', true]]),
        targetDependencies: new Map([['test', ['build']]]),
        localTargetDependencies: new Map([
          ['test', ['build']],
          ['build', []],
        ]),
        targetExecutors: new Map([['test', '@smoothbricks/nx-plugin:bounded-exec']]),
        // Object inputs carry no fileset a policy can read, so only the
        // strings survive the projection.
        targetInputs: new Map([['test', ['default']]]),
        targetOptions: new Map([['test', { command: 'bun test', timeoutMs: 120_000 }]]),
        targetOutputs: new Map([['build', ['{projectRoot}/dist/']]]),
        targetScripts: new Map(),
      },
    ]);
  });
});
