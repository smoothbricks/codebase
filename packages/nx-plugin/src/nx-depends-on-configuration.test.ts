import { expect, it } from 'bun:test';
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { TargetConfiguration } from 'nx/src/config/workspace-json-project-json';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph.js';

/**
 * Guards the dependsOn-configuration hunk of `patches/nx@23.2.1.patch`. Stock Nx 23.2.1 runs every dependency in the
 * configuration the run asked for, so a graph run in the default configuration could not hold a production build:
 * the land gate built the host's production CLI and addon in a second, sequential Nx run. A dependsOn entry now names
 * the configuration its dependency runs in, and that dependency passes it on to its own dependencies; an entry that
 * names none behaves as before. Drop the hunk once the installed Nx has the field; retain this regression.
 */

function project(
  name: string,
  targets: Record<string, { dependsOn: TargetConfiguration['dependsOn']; production?: true }>,
): ProjectGraphProjectNode {
  return {
    name,
    type: 'lib',
    data: {
      root: name,
      targets: Object.fromEntries(
        Object.entries(targets).map(([target, { dependsOn, production }]) => [
          target,
          { executor: 'nx:noop', dependsOn, ...(production ? { configurations: { production: {} } } : {}) },
        ]),
      ),
    },
  };
}

/**
 * `gate` asks for `pkg:cli` in production and for `pkg:test`, whose own `cli` dependency names no configuration, as
 * the root gate asks for the shipping CLI beside tests that run the development one.
 */
const graph: ProjectGraph = {
  nodes: {
    root: project('root', {
      gate: {
        dependsOn: [
          { projects: ['pkg'], target: 'cli', configuration: 'production' },
          { projects: ['pkg'], target: 'test' },
        ],
      },
    }),
    pkg: project('pkg', {
      cli: { dependsOn: ['compile'], production: true },
      compile: { dependsOn: [], production: true },
      test: { dependsOn: ['cli'] },
    }),
  },
  dependencies: { root: [], pkg: [] },
};

it('runs a dependency in the configuration its entry names, and passes it on', () => {
  const tasks = createTaskGraph(graph, {}, ['root'], ['gate'], undefined, {});
  expect(tasks.dependencies['root:gate']).toEqual(['pkg:cli:production', 'pkg:test']);
  expect(tasks.dependencies['pkg:cli:production']).toEqual(['pkg:compile:production']);
});

it('keeps the run configuration for an entry that names none, beside a named one in the same graph', () => {
  const tasks = createTaskGraph(graph, {}, ['root'], ['gate'], undefined, {});
  expect(tasks.dependencies['pkg:test']).toEqual(['pkg:cli']);
  expect(tasks.dependencies['pkg:cli']).toEqual(['pkg:compile']);
  expect(Object.keys(tasks.tasks).sort()).toEqual([
    'pkg:cli',
    'pkg:cli:production',
    'pkg:compile',
    'pkg:compile:production',
    'pkg:test',
    'root:gate',
  ]);
});

/** Every edge of a task graph with each task's dependencies sorted: the shape two orders of one request must share. */
function edges(graph: ProjectGraph, gate: NonNullable<TargetConfiguration['dependsOn']>): Record<string, string[]> {
  const withGate: ProjectGraph = {
    ...graph,
    nodes: { ...graph.nodes, root: project('root', { gate: { dependsOn: gate } }) },
    dependencies: { ...graph.dependencies, root: [] },
  };
  const tasks = createTaskGraph(withGate, {}, ['root'], ['gate'], undefined, {});
  return Object.fromEntries(
    Object.entries(tasks.dependencies)
      .map(([task, dependencies]): [string, string[]] => [task, [...dependencies].sort()])
      .sort(([left], [right]) => (left < right ? -1 : 1)),
  );
}

it('expands a task without the named configuration under each configuration that reaches it, whatever the order', () => {
  // `tool` has no production configuration, so the production `cli` and the development `cli` share it. Under stock
  // ordering its `compile` dependency was whichever configuration reached `tool` first.
  const shared: ProjectGraph = {
    nodes: {
      pkg: project('pkg', {
        cli: { dependsOn: ['tool'], production: true },
        tool: { dependsOn: ['compile'] },
        compile: { dependsOn: [], production: true },
        test: { dependsOn: ['cli'] },
      }),
    },
    dependencies: { pkg: [] },
  };
  const test = { projects: ['pkg'], target: 'test' };
  const shipping = { projects: ['pkg'], target: 'cli', configuration: 'production' };
  const developmentFirst = edges(shared, [test, shipping]);
  expect(edges(shared, [shipping, test])).toEqual(developmentFirst);
  expect(developmentFirst['pkg:tool']).toEqual(['pkg:compile', 'pkg:compile:production']);
});

it("resolves a production parent's ^ dependency through a project without the target in production, whatever the order", () => {
  // `mid` has no `build`, so `app:build`'s `^build` is a dummy task for it; the production parent's dummy is its own.
  const chain: ProjectGraph = {
    nodes: {
      app: project('app', { build: { dependsOn: ['^build'], production: true } }),
      mid: project('mid', {}),
      lib: project('lib', { build: { dependsOn: [], production: true } }),
    },
    dependencies: {
      app: [{ source: 'app', target: 'mid', type: 'static' }],
      mid: [{ source: 'mid', target: 'lib', type: 'static' }],
      lib: [],
    },
  };
  const development = { projects: ['app'], target: 'build' };
  const production = { projects: ['app'], target: 'build', configuration: 'production' };
  const developmentFirst = edges(chain, [development, production]);
  expect(edges(chain, [production, development])).toEqual(developmentFirst);
  expect(developmentFirst['app:build']).toEqual(['lib:build']);
  expect(developmentFirst['app:build:production']).toEqual(['lib:build:production']);
});
