import { describe, expect, it } from 'bun:test';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { BuildStateOutputError, refuseOutputsUnderBuildState } from './build-state-output-policy.js';

const workspaceRoot = '/checkout';
const buildStatePaths = [
  { checkout: 'target', volume: 'target' },
  { checkout: '.nx/cache', volume: 'nx/cache' },
  { checkout: '.nx/workspace-data', volume: 'nx/workspace-data' },
  { checkout: 'packages/nested/out/cargo', volume: 'cargo/nested' },
  { checkout: '.codegraph', volume: 'codegraph' },
];

function graph(target: TargetConfiguration, root = 'packages/example'): ProjectGraph {
  return {
    nodes: { example: { name: 'example', type: 'lib', data: { root, targets: { archive: target } } } },
    dependencies: { example: [] },
  };
}

function validate(outputs: string[]): void {
  refuseOutputsUnderBuildState(graph({ outputs }), buildStatePaths, workspaceRoot);
}

describe('build-state Nx output policy', () => {
  it('refuses an output at or under every contributed build-state path', () => {
    for (const { checkout } of buildStatePaths) {
      for (const output of [checkout, `${checkout}/artifact`, `${checkout}/**/*`]) {
        expect(() => validate([`{workspaceRoot}/${output}`])).toThrow(BuildStateOutputError);
      }
    }
  });

  it('refuses ancestors whose restore can remove fixed build-state links', () => {
    for (const output of ['{workspaceRoot}', '{workspaceRoot}/.nx', '{workspaceRoot}/packages/nested/out']) {
      expect(() => validate([output])).toThrow(BuildStateOutputError);
    }
  });

  it('refuses glob prefixes that can cover build state, including an unbounded glob', () => {
    for (const output of ['{workspaceRoot}/**/*', '{workspaceRoot}/packages/*/out', '{workspaceRoot}/target-*']) {
      expect(() => validate([output])).toThrow(BuildStateOutputError);
    }
  });

  it('resolves absolute outputs against the supplied workspace root', () => {
    for (const output of ['/checkout/target/archive', '/checkout/packages/nested', '/checkout/**/archive', '/']) {
      expect(() => validate([output])).toThrow(BuildStateOutputError);
    }
    expect(() => validate(['/elsewhere/target/archive'])).not.toThrow();
  });

  it('allows disjoint source-tree archive and compiler outputs without prefix false positives', () => {
    expect(() =>
      validate([
        '{workspaceRoot}/.cache/nextest/archive.tar.zst',
        '{projectRoot}/dist/**/*',
        '{workspaceRoot}/targeted/artifact',
        '{workspaceRoot}/packages/nested/out/cargo-docs',
        '{workspaceRoot}/.nx-other/**/*',
      ]),
    ).not.toThrow();
  });

  it('uses stock Nx interpolation for project, options and every configuration', () => {
    const resolved = graph({
      outputs: ['{options.outputPath}'],
      options: { outputPath: 'dist/example' },
      configurations: {
        unsafe: { outputPath: 'packages/nested/out/cargo/archive' },
        safe: { outputPath: 'dist/safe' },
      },
    });
    try {
      refuseOutputsUnderBuildState(resolved, buildStatePaths, workspaceRoot);
      throw new Error('unsafe configured output was accepted');
    } catch (error) {
      expect(error).toBeInstanceOf(BuildStateOutputError);
      if (!(error instanceof BuildStateOutputError)) throw error;
      expect(error.findings).toEqual([
        {
          project: 'example',
          target: 'archive',
          configuration: 'unsafe',
          output: 'packages/nested/out/cargo/archive',
          buildStatePath: 'packages/nested/out/cargo',
        },
      ]);
      expect(error.message).toContain('example:archive:unsafe');
    }
    expect(() =>
      refuseOutputsUnderBuildState(
        graph({ outputs: ['{projectRoot}/artifact'] }, 'target'),
        buildStatePaths,
        workspaceRoot,
      ),
    ).toThrow(BuildStateOutputError);
  });

  it('checks stock Nx outputPath fallback and the resolved graph override rather than plugin declarations', () => {
    expect(() =>
      refuseOutputsUnderBuildState(
        graph({ options: { outputPath: 'target/archive' } }),
        buildStatePaths,
        workspaceRoot,
      ),
    ).toThrow(BuildStateOutputError);
    const resolved = graph({ outputs: ['{workspaceRoot}/.cache/nextest/archive.tar.zst'] });
    const project = resolved.nodes.example;
    if (!project) throw new Error('fixture project missing');
    project.data.targets = { archive: { outputs: ['{workspaceRoot}/target/overridden'] } };
    expect(() => refuseOutputsUnderBuildState(resolved, buildStatePaths, workspaceRoot)).toThrow(BuildStateOutputError);
  });
});
