import { describe, expect, it } from 'bun:test';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import {
  BuildStateOutputError,
  type DeclaredOutput,
  OutputDeclarationError,
  OutputOverlapError,
  refuseOutputsUnderBuildState,
  refuseOverlappingOutputs,
} from './build-state-output-policy.js';

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

type Projects = Record<string, { readonly root: string; readonly targets: Record<string, TargetConfiguration> }>;

function projects(spec: Projects): ProjectGraph {
  const nodes: ProjectGraph['nodes'] = {};
  const dependencies: ProjectGraph['dependencies'] = {};
  for (const [name, { root, targets }] of Object.entries(spec)) {
    nodes[name] = { name, type: 'lib', data: { root, targets } };
    dependencies[name] = [];
  }
  return { nodes, dependencies };
}

/** The refusal as `task output <> task output` lines; [] when every output has one producer. */
function overlaps(spec: Projects): string[] {
  try {
    refuseOverlappingOutputs(projects(spec), workspaceRoot);
    return [];
  } catch (error) {
    if (!(error instanceof OutputOverlapError)) throw error;
    const task = ({ project, target, configuration }: DeclaredOutput) =>
      `${project}:${target}${configuration ? `:${configuration}` : ''}`;
    return error.overlaps.map(({ left, right }) => `${task(left)} ${left.output} <> ${task(right)} ${right.output}`);
  }
}

const run = (outputs: string[], extra: Partial<TargetConfiguration> = {}): TargetConfiguration => ({
  executor: 'nx:run-commands',
  outputs,
  options: { command: 'true' },
  ...extra,
});

describe('one producing target per Nx output', () => {
  it('refuses an output at, under or above another target output, in every configuration and project', () => {
    expect(
      overlaps({
        llm: {
          root: 'packages/llm',
          targets: {
            'tsc-js': run(['{projectRoot}/dist']),
            'nx-worker-bundle': run(['{projectRoot}/dist/nx/worker-entry.js']),
          },
        },
        facts: {
          root: 'packages/facts',
          targets: { 'tsc-js': run(['{projectRoot}/dist-types']), 'build-types': run(['{projectRoot}/dist-types/']) },
        },
        ttsc: { root: 'packages/ttsc', targets: { build: run(['{projectRoot}/dist']) } },
        bun: {
          root: 'packages/bun',
          targets: {
            'test-script-compiler': run(['{options.daemon}'], {
              options: { command: 'true', daemon: 'packages/ttsc/dist/native-script-daemon' },
            }),
          },
        },
        app: {
          root: 'packages/app',
          targets: {
            build: run(['{projectRoot}/dist']),
            bundle: run(['{options.out}'], {
              options: { command: 'true', out: 'packages/app/bundle' },
              configurations: { production: { out: 'packages/app/dist/bundle.js' } },
            }),
          },
        },
        rt: {
          root: 'packages/rt',
          targets: {
            native: run(['{projectRoot}/dist/native']),
            darwin: run(['{projectRoot}/dist/native/darwin/**']),
            types: run(['{projectRoot}/**/*.d.ts']),
            wasm: run(['{projectRoot}/out/*.wasm']),
            engine: run(['{projectRoot}/out/engine.wasm']),
          },
        },
        mix: {
          root: 'packages/mix',
          targets: { b: run(['{projectRoot}/b']), split: run(['{projectRoot}/{b/c,d}/**']) },
        },
      }),
    ).toEqual([
      'llm:tsc-js packages/llm/dist <> llm:nx-worker-bundle packages/llm/dist/nx/worker-entry.js',
      'facts:tsc-js packages/facts/dist-types <> facts:build-types packages/facts/dist-types',
      'ttsc:build packages/ttsc/dist <> bun:test-script-compiler packages/ttsc/dist/native-script-daemon',
      'app:build packages/app/dist <> app:bundle:production packages/app/dist/bundle.js',
      'rt:native packages/rt/dist/native <> rt:darwin packages/rt/dist/native/darwin/**',
      'rt:native packages/rt/dist/native <> rt:types packages/rt/**/*.d.ts',
      'rt:types packages/rt/**/*.d.ts <> rt:engine packages/rt/out/engine.wasm',
      'rt:wasm packages/rt/out/*.wasm <> rt:engine packages/rt/out/engine.wasm',
      'mix:b packages/mix/b <> mix:split packages/mix/{b/c,d}/**',
    ]);
  });

  it('passes disjoint outputs, two patterns, a negation and a pattern that cannot reach inside a path', () => {
    expect(
      overlaps({
        llm: {
          root: 'packages/llm',
          targets: {
            'tsc-js': run(['{projectRoot}/dist']),
            'nx-worker-bundle': run(['{projectRoot}/dist-nx-bundle/worker-entry.js']),
            'script-prelude': run([
              '{projectRoot}/dist-prelude/script-prelude.json',
              '{projectRoot}/dist-prelude/x.json',
            ]),
          },
        },
        rt: {
          root: 'packages/rt',
          targets: {
            'tsc-js': run(['{projectRoot}/dist/**/*.{js,d.ts}{,.map}', '{projectRoot}/dist/.build.tsbuildinfo']),
            build: run(['{projectRoot}/dist/*.wasm', '!{projectRoot}/dist']),
            darwin: run(['{projectRoot}/dist/native/darwin-arm64/**']),
            native: run(['{projectRoot}/out/native']),
            wasm: run(['{projectRoot}/*/wasm/*.wasm']),
            docs: run(['{projectRoot}/{doc/api,doc/guide}/*.md']),
          },
        },
      }),
    ).toEqual([]);
  });

  it('names the target whose output Nx refuses to interpolate, and reads the next graph whole', () => {
    // Nx's own error names only the output.
    let refusal: unknown;
    try {
      refuseOverlappingOutputs(graph({ outputs: ['{projectRoot}/{workspaceRoot}/archive'] }), workspaceRoot);
    } catch (error) {
      refusal = error;
    }
    expect(refusal).toBeInstanceOf(OutputDeclarationError);
    expect(refusal instanceof Error ? refusal.message : refusal).toStartWith(
      "example:archive: Output '{projectRoot}/{workspaceRoot}/archive' is invalid.",
    );
    expect(
      overlaps({
        app: {
          root: 'packages/app',
          targets: { build: run(['{projectRoot}/dist']), bundle: run(['{projectRoot}/dist/a.js']) },
        },
      }),
    ).toEqual(['app:build packages/app/dist <> app:bundle packages/app/dist/a.js']);
  });
});
