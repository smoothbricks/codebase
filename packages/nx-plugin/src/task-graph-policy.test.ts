import { expect, it } from 'bun:test';
import type { ProjectGraph, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { BuildTestDependencyError, buildTestDependencies, refuseBuildTestDependencies } from './task-graph-policy.js';

const run = (extra: TargetConfiguration = {}): TargetConfiguration => ({ executor: 'nx:noop', outputs: [], ...extra });
function graph(
  targets: Record<string, TargetConfiguration>,
  dependencyTargets: Record<string, TargetConfiguration> = {},
): ProjectGraph {
  return {
    nodes: {
      app: { name: 'app', type: 'lib', data: { root: 'app', targets } },
      dep: { name: 'dep', type: 'lib', data: { root: 'dep', targets: dependencyTargets } },
    },
    dependencies: { app: [{ source: 'app', target: 'dep', type: 'static' }], dep: [] },
  };
}

const cases: { name: string; graph: ProjectGraph; edges: string[] }[] = [
  {
    name: 'foreign test archive is test work',
    graph: graph({
      build: run({ dependsOn: ['cargo-cross-test-archive-aarch64-unknown-linux-gnu'] }),
      'cargo-cross-test-archive-aarch64-unknown-linux-gnu': run(),
    }),
    edges: ['app:build -> app:cargo-cross-test-archive-aarch64-unknown-linux-gnu'],
  },
  {
    name: 'direct inferred archive',
    graph: graph({ build: run({ dependsOn: ['cargo-test-build'] }), 'cargo-test-build': run() }),
    edges: ['app:build -> app:cargo-test-build'],
  },
  {
    name: 'transitive declared edge',
    graph: graph({
      build: run({ dependsOn: ['prepare'] }),
      prepare: run({ dependsOn: ['test-shard1'] }),
      'test-shard1': run(),
    }),
    edges: ['app:prepare -> app:test-shard1'],
  },
  {
    name: 'wildcard output reaches suffix test',
    graph: graph({
      build: run({ dependsOn: ['*-js'] }),
      'bundle-js': run({ dependsOn: ['contract-test'] }),
      'contract-test': run(),
    }),
    edges: ['app:bundle-js -> app:contract-test', 'app:bundle-js -> app:contract-test'],
  },
  {
    name: 'caret uses project dependency',
    graph: graph({ build: run({ dependsOn: ['^test'] }) }, { test: run() }),
    edges: ['app:build -> dep:test'],
  },
  {
    name: 'object project selection',
    graph: graph({ build: run({ dependsOn: [{ projects: ['dep'], target: 'test-*' }] }) }, { 'test-shard2': run() }),
    edges: ['app:build -> dep:test-shard2'],
  },
  {
    name: 'output producer without reserved name',
    graph: graph({ pack: run({ outputs: ['{projectRoot}/dist/'], dependsOn: ['test'] }), test: run() }),
    edges: ['app:pack -> app:test'],
  },
  {
    name: 'inferred semantic kind without test name',
    graph: graph({
      build: run({ dependsOn: ['compile-checks'] }),
      'compile-checks': run({ metadata: { smooTaskKind: 'test' }, outputs: ['{projectRoot}/checks/'] }),
    }),
    edges: ['app:build -> app:compile-checks'],
  },
  {
    name: 'test consumer builds its own prerequisites',
    graph: graph({ test: run({ dependsOn: ['build'] }), build: run({ dependsOn: ['bundle-js'] }), 'bundle-js': run() }),
    edges: [],
  },
  { name: 'absent optional wildcard', graph: graph({ build: run({ dependsOn: ['missing-*'] }) }), edges: [] },
  {
    name: 'unreachable test work',
    graph: graph({ build: run(), test: run({ dependsOn: ['cargo-test-build'] }), 'cargo-test-build': run() }),
    edges: [],
  },
  {
    name: 'cycle cannot hide test edge',
    graph: graph({
      build: run({ dependsOn: ['prepare'] }),
      prepare: run({ dependsOn: ['build', 'test'] }),
      test: run(),
    }),
    edges: ['app:prepare -> app:test'],
  },
  {
    name: 'continuous test dependency',
    graph: graph({ build: run({ dependsOn: ['test-watch'] }), 'test-watch': run({ continuous: true }) }),
    edges: ['app:build -> app:test-watch'],
  },
];

it('decides build/test reachability using Nx dependency semantics', () => {
  for (const entry of cases) {
    const findings = buildTestDependencies(entry.graph, '/checkout');
    expect(
      findings.map(({ source, test }) => `${source} -> ${test}`),
      entry.name,
    ).toEqual(entry.edges);
    if (entry.edges.length === 0) {
      expect(() => refuseBuildTestDependencies(entry.graph, '/checkout'), entry.name).not.toThrow();
    } else {
      let refusal: unknown;
      try {
        refuseBuildTestDependencies(entry.graph, '/checkout');
      } catch (error) {
        refusal = error;
      }
      expect(refusal, entry.name).toBeInstanceOf(BuildTestDependencyError);
      if (!(refusal instanceof BuildTestDependencyError)) throw new Error('missing typed refusal');
      expect(refusal.findings, entry.name).toEqual(findings);
      expect(refusal.message, entry.name).toContain(entry.edges[0] ?? '');
    }
  }
});

it('retains the build configuration and Nx propagates it to prerequisites', () => {
  const input = graph({
    build: run({ dependsOn: ['prepare'], configurations: { production: {} } }),
    prepare: run({ dependsOn: ['test'], configurations: { production: {} } }),
    test: run(),
  });
  expect(buildTestDependencies(input, '/checkout')).toEqual([
    { build: 'app:build', source: 'app:prepare', test: 'app:test' },
    { build: 'app:build:production', source: 'app:prepare:production', test: 'app:test' },
  ]);
});

it('checks a named default configuration once, not as a second unconfigured producer', () => {
  const input = graph({
    build: run({ dependsOn: ['test'], defaultConfiguration: 'production', configurations: { production: {} } }),
    test: run(),
  });
  expect(buildTestDependencies(input, '/checkout')).toEqual([
    { build: 'app:build:production', source: 'app:build:production', test: 'app:test' },
  ]);
});

it('uses Nx implicit project edges and exclusions without mutating the supplied graph', () => {
  for (const excluded of [false, true]) {
    const input = graph({ build: run({ dependsOn: ['^test'] }) }, { test: run() });
    const app = input.nodes.app;
    if (!app) throw new Error('fixture app is absent');
    app.data.implicitDependencies = [excluded ? '!dep' : 'dep'];
    if (!excluded) input.dependencies.app = [];
    const before = structuredClone(input);
    expect(buildTestDependencies(input, '/checkout')).toEqual(
      excluded ? [] : [{ build: 'app:build', source: 'app:build', test: 'dep:test' }],
    );
    expect(input).toEqual(before);
  }
});
