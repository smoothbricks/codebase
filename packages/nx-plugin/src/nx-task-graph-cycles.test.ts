import { expect, it } from 'bun:test';
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph';
import { findCycles } from 'nx/src/tasks-runner/task-graph-utils';

/**
 * Guards the task-graph hunk of `patches/nx@23.2.1.patch`. A target that depends on `^build` through a project with
 * no `build` target of its own gets a dummy task for that project, and Nx flattens each dummy into the real tasks
 * behind it, except a dummy it believes sits in a cycle, which contributes nothing. Nx 23.2.1 believed that of every
 * task on the depth-first path that first reached a cycle, and only of cycles that traversal reached before another
 * had marked their tasks visited. Both depend on the order of the graph's keys, so the same task got different
 * dependencies, and, through `dependentTasksOutputFiles`, a different hash, according to which other tasks the run
 * asked for: a task requested alone and the same task requested through its aggregate disagreed.
 *
 * `findCycles` now answers with the tasks that lie on a cycle, whatever the order. After dummy normalization,
 * graph construction retains only the original requests and their real regular or continuous dependencies:
 * a producer disconnected by a suppressed dummy cycle must not become an unrelated executable root.
 * Drop each carried hunk once the installed Nx contains that repair; retain its regression.
 */

function project(name: string, targets: Record<string, string[]>): ProjectGraphProjectNode {
  return {
    name,
    type: 'lib',
    data: {
      root: name,
      targets: Object.fromEntries(
        Object.entries(targets).map(([target, dependsOn]) => [target, { executor: 'nx:noop', dependsOn }]),
      ),
    },
  };
}

/**
 * `app` depends on `mid`, which has no `build`, and `mid` and `core` depend on each other: `app`'s `^build` is a
 * dummy task for `mid`, and `core:build` sits in a cycle with its own dummy for `mid`. `agg` asks for `^build` too, so
 * its dummy reaches the cycle before `inner`'s does, as an aggregate's does in a real workspace.
 */
const graph: ProjectGraph = {
  nodes: {
    app: project('app', { agg: ['^build', 'inner'], inner: ['^build'] }),
    mid: project('mid', {}),
    core: project('core', { build: ['^build'] }),
  },
  dependencies: {
    app: [{ source: 'app', target: 'mid', type: 'static' }],
    mid: [{ source: 'mid', target: 'core', type: 'static' }],
    core: [{ source: 'core', target: 'mid', type: 'static' }],
  },
};

it('gives a task the same dependencies whether it is requested alone or through its aggregate', () => {
  const alone = createTaskGraph(graph, {}, ['app'], ['inner'], undefined, {});
  const through = createTaskGraph(graph, {}, ['app'], ['agg'], undefined, {});
  expect(alone.dependencies['app:inner']).toEqual(['core:build']);
  expect(through.dependencies['app:inner']).toEqual(['core:build']);
});

it('finds the tasks on a cycle, not the tasks that lead to one, in any order of the graph', () => {
  expect(findCycles({ dependencies: { a: ['b'], b: [] } })).toBeNull();
  expect(findCycles({ dependencies: { top: ['x'], x: ['y'], y: ['z'], z: ['y'] } })).toEqual(new Set(['y', 'z']));
  expect(findCycles({ dependencies: { a: ['a'], b: [] } })).toEqual(new Set(['a']));
  expect(findCycles({ dependencies: { a: [], b: [] }, continuousDependencies: { a: ['b'], b: ['a'] } })).toEqual(
    new Set(['a', 'b']),
  );
  // `d` reaches the cycle through `c`, which a traversal that began elsewhere has already marked visited.
  const edges: Record<string, string[]> = { a: ['b'], b: ['c'], c: ['b'], d: ['c'] };
  for (const keys of [
    ['a', 'b', 'c', 'd'],
    ['d', 'c', 'b', 'a'],
    ['c', 'a', 'd', 'b'],
  ]) {
    const dependencies = Object.fromEntries(keys.map((key) => [key, edges[key] ?? []]));
    expect(findCycles({ dependencies })).toEqual(new Set(['b', 'c']));
  }
});

/**
 * The dummy route through mid cycles back to app. Normalization removes that
 * route, but graph expansion already discovered leaf:build behind it.
 * A disconnected task is not a dependency and must not become a new root.
 */
const dummyCycleGraph: ProjectGraph = {
  nodes: {
    app: project('app', { inner: ['^build'] }),
    mid: project('mid', {}),
    leaf: project('leaf', { build: [] }),
  },
  dependencies: {
    app: [{ source: 'app', target: 'mid', type: 'static' }],
    mid: [
      { source: 'mid', target: 'app', type: 'static' },
      { source: 'mid', target: 'leaf', type: 'static' },
    ],
    leaf: [],
  },
};

it('does not schedule a producer disconnected by dummy-cycle normalization', () => {
  const tasks = createTaskGraph(dummyCycleGraph, {}, ['app'], ['inner'], undefined, {});
  expect(tasks.dependencies['app:inner']).toEqual([]);
  expect(Object.keys(tasks.tasks)).toEqual(['app:inner']);
  expect(tasks.roots).toEqual(['app:inner']);
});

it('keeps a disconnected producer when the caller explicitly requests it', () => {
  const tasks = createTaskGraph(dummyCycleGraph, {}, ['app', 'leaf'], ['inner', 'build'], undefined, {});
  expect(Object.keys(tasks.tasks).sort()).toEqual(['app:inner', 'leaf:build']);
  expect(tasks.roots.sort()).toEqual(['app:inner', 'leaf:build']);
});

it('keeps real regular and continuous producer edges through the same project cycle', () => {
  const connected: ProjectGraph = {
    ...dummyCycleGraph,
    nodes: {
      ...dummyCycleGraph.nodes,
      app: project('app', { inner: ['^build', 'leaf:build', 'leaf:serve'] }),
      leaf: {
        name: 'leaf',
        type: 'lib',
        data: {
          root: 'leaf',
          targets: {
            build: { executor: 'nx:noop' },
            serve: { executor: 'nx:noop', continuous: true },
          },
        },
      },
    },
  };
  const tasks = createTaskGraph(connected, {}, ['app'], ['inner'], undefined, {});
  expect(Object.keys(tasks.tasks).sort()).toEqual(['app:inner', 'leaf:build', 'leaf:serve']);
  expect(tasks.dependencies['app:inner']).toEqual(['leaf:build']);
  expect(tasks.continuousDependencies['app:inner']).toEqual(['leaf:serve']);
});

it('does not conceal a real cycle between requested task dependencies', () => {
  const cyclic: ProjectGraph = {
    nodes: {
      app: project('app', { inner: ['loop:build'] }),
      loop: project('loop', { build: ['app:inner'] }),
    },
    dependencies: { app: [], loop: [] },
  };
  const tasks = createTaskGraph(cyclic, {}, ['app'], ['inner'], undefined, {});
  expect(Object.keys(tasks.tasks).sort()).toEqual(['app:inner', 'loop:build']);
  expect(findCycles(tasks)).toEqual(new Set(['app:inner', 'loop:build']));
});
