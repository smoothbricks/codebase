import { describe, expect, it } from 'bun:test';
import { join } from 'node:path';
import {
  formatProjectTargetLines,
  nxCacheDirectories,
  nxResetCommand,
  projectNamesWithTarget,
  projectRootFromNxProjectJson,
  projectTargetsFromNxProjects,
  targetDependenciesFromNxProjectJson,
  targetNamesFromNxProjectJson,
  targetNamesFromProjects,
  targetOutputsFromNxProjectJson,
} from './index.js';

describe('Nx helper command construction', () => {
  it('builds explicit nx reset invocation', () => {
    expect(nxResetCommand()).toEqual({ command: 'nx', args: ['reset'] });
  });

  it('selects only local Nx cache directories', () => {
    expect(nxCacheDirectories('/repo')).toEqual([
      join('/repo', '.nx/cache'),
      join('/repo', '.nx/workspace-data'),
      join('/repo', 'node_modules/.cache/nx'),
    ]);
  });
});

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
            dependsOn: ['build'],
            options: { command: 'bun test', timeoutMs: 120_000 },
          },
          build: { outputs: ['{projectRoot}/dist'] },
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
        buildDependsOn: undefined,
        targetDependencies: new Map(),
        targetExecutors: new Map(),
        targetOptions: new Map(),
        targetOutputs: new Map(),
        targetScripts: new Map(),
      },
      {
        project: 'web',
        root: 'packages/web',
        targets: ['build', 'test'],
        buildDependsOn: undefined,
        targetDependencies: new Map([['test', ['build']]]),
        targetExecutors: new Map([['test', '@smoothbricks/nx-plugin:bounded-exec']]),
        targetOptions: new Map([['test', { command: 'bun test', timeoutMs: 120_000 }]]),
        targetOutputs: new Map([['build', ['{projectRoot}/dist']]]),
        targetScripts: new Map(),
      },
    ]);
  });
});
