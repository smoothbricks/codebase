import { describe, expect, it } from 'bun:test';
import { parseNxProjectJsonText } from '../lib/json.js';
import {
  formatProjectTargetLines,
  interpolateTaskOutput,
  type ProjectTargets,
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
          build: {
            outputs: ['{projectRoot}/{options.out}/'],
            options: { out: 'dist/dev' },
            defaultConfiguration: 'production',
            configurations: { production: { out: 'dist/prod', cache: { type: 'filesystem' } } },
          },
        },
      },
      cli: { root: 'packages/cli', targets: { lint: {} } },
    };

    expect(targetNamesFromProjects(projects).sort()).toEqual(['build', 'lint', 'test']);
    expect(parseNxProjectJsonText(JSON.stringify(projects.web))).toEqual(projects.web);
    expect(projectTargetsFromNxProjects(projects)).toEqual([
      {
        project: 'cli',
        root: 'packages/cli',
        targets: ['lint'],
        targetCache: new Map(),
        targetConfigurations: new Map(),
        targetDefaultConfigurations: new Map(),
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
        targetConfigurations: new Map([['build', { production: { out: 'dist/prod', cache: { type: 'filesystem' } } }]]),
        targetDefaultConfigurations: new Map([['build', 'production']]),
        targetDependencies: new Map([['test', ['build']]]),
        localTargetDependencies: new Map([
          ['test', ['build']],
          ['build', []],
        ]),
        targetExecutors: new Map([['test', '@smoothbricks/nx-plugin:bounded-exec']]),
        targetInputs: new Map([['test', ['default', { runtime: 'echo version' }]]]),
        targetOptions: new Map([
          ['test', { command: 'bun test', timeoutMs: 120_000 }],
          ['build', { out: 'dist/dev' }],
        ]),
        targetOutputs: new Map([['build', ['{projectRoot}/{options.out}/']]]),
        targetScripts: new Map(),
      },
    ]);
  });
});

const web: ProjectTargets = { project: 'web', root: 'packages/web', targets: ['bundle'] };

describe('Nx task output interpolation', () => {
  it('names the task whose output Nx refuses and leaves the next interpolation intact', () => {
    // Nx refuses a `{workspaceRoot}` that does not lead the declaration.
    expect(() => interpolateTaskOutput('{projectRoot}/{workspaceRoot}/dist', web, 'bundle', undefined)).toThrow(
      'web:bundle: Output',
    );
    expect(interpolateTaskOutput('{projectRoot}/dist/', web, 'bundle', undefined)).toBe('packages/web/dist');
  });

  it('refuses a project without its resolved root', () => {
    expect(() =>
      interpolateTaskOutput('{projectRoot}/dist/', { project: 'orphan', targets: [] }, 'build', undefined),
    ).toThrow('Nx project orphan is missing its resolved root.');
  });
});
