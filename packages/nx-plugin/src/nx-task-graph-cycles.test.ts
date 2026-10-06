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
 * `findCycles` now answers with the tasks that lie on a cycle, whatever the order. Drop the hunk, and keep this test,
 * once the Nx version in use contains that fix.
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
