# Patches

Patches this repository applies to its dependencies. `bun install` applies the root `package.json`'s
`patchedDependencies`. `nx@23.2.1.patch` is the exception: `tooling/patched-nx.ts` applies it to the registry tarball
and packs the result as an immutable GitHub release, which the root `overrides.nx` installs. The Patched Nx workflow
builds that release twice, byte for byte, and verifies the published asset against `bun.lock`.

## Requested task closure after dummy normalization

The `create-task-graph` hunk retains only the caller's initial tasks and the tasks reachable from them through the
normalized regular or continuous dependency edges. Expansion may discover a real producer behind a dummy cycle that
normalization later removes. Leaving that producer in `tasks` makes it an unrelated new root, which Nx executes even
though no requested task depends on it. On a model-command graph, fourteen such tasks pulled in complete role and native
builds; no dependency of the nineteen requested-closure tasks was changed.

The selection happens at Nx's graph-construction owner, not in a consumer cache probe or launcher. Explicitly requested
tasks, genuine producer edges, continuous-only dependencies and real task cycles remain intact. The existing
`nx-task-graph-cycles.test.ts` guards both this closure rule and cycle membership against the package extracted from the
actual tarball produced by `@smoothbricks/codebase:patched-nx`. Every test shard and test typecheck depends on that
producer and hashes its output bytes, so a source gate tests the current patch before its public release is installed.
The installed dependency and its frozen lock remain unchanged until the served release is pinned. Drop each carried hunk
only when the installed upstream version contains its respective repair, retaining the regressions.

## Upstream PR draft: Nx `findCycles`

Status: drafted, not opened. Written against nrwl/nx `master` at `a37b5ca4c6630bb9ea5df9ac0546edc3a186bc2f`
(2026-10-06), whose `findCycles`, `filterDummyTasks` and `getNonDummyDeps` have the logic of Nx 23.2.1's. This
repository carries the fix as the `task-graph-utils` hunk of `nx@23.2.1.patch`;
`packages/nx-plugin/src/nx-task-graph-cycles.test.ts` guards it here. Drop that patch hunk once the installed Nx
contains the fix, retaining the regression.

### Title

```text
fix(core): give a task the same dependencies whichever other tasks a run asks for
```

### Description

Follows the template of `.github/PULL_REQUEST_TEMPLATE.md`.

```markdown
## Current Behavior

When a target depends on `^build` through a project that has no `build` target, Nx makes a dummy task for that project
and later flattens every dummy into the real tasks behind it (`filterDummyTasks`), except a dummy that `findCycles`
reports as being in a cycle, which contributes nothing. #28793 added `findCycles` so the flattening ends when there are
several cycles. It has two defects, and both make its answer depend on the order of the graph's keys:

- it reports every task on the depth-first path that reached a cycle, including tasks that only _lead to_ the cycle and
  are not on it, so a dummy task in front of a cycle loses its dependencies;
- a traversal returns at the first cycle it finds, before it has looked at the rest of that task's dependencies, and
  everything it entered stays marked visited, so a later traversal stops at those tasks and a cycle through them is
  never reported (`a -> b`, `b -> a, e`, `e -> b` reports `a` and `b`, not `e`).

So the same task gets different dependencies depending on which other tasks the run asks for. In the reproduction below,
`app:inner` depends on nothing under `nx run app:inner` and on `core:build` under `nx run app:agg`, where `app:agg` is
an aggregate that also runs `app:inner`; and `app:agg` itself lacks the `core:build` it reaches through the same
project. Dependencies feed `dependentTasksOutputFiles`, so such runs also hash a task differently: measured on a
workspace of 7 projects that import each other, one task hashed 6788173338685328009 requested alone and
16936296670297497910 through `run-many`, the only difference being two `dependentTasksOutputFiles` nodes for a
dependency the lone request had lost, and a pre-run of one never hit the cache of the other. A dropped edge is also a
missing ordering: in the existing spec "app1:build -> app2 <-> app3:build" `app1:compile` depends on `app3` through
`app2`, yet runs with no dependency on `app3:compile`.

## Expected Behavior

`findCycles` returns the tasks that lie on a cycle: every task of a strongly connected component of more than one task,
and every task that depends on itself. That is a property of the graph, so a dummy task that merely leads to a cycle is
still flattened, and the answer no longer depends on the order of the keys or on the other tasks of the run. It is
Tarjan's algorithm: linear in the graph, iterative (no recursion, no copied paths), and `findCycle`, which wants one
cycle as a path, is unchanged.

One existing expectation changes: in "app1:build -> app2 <-> app3:build" `app1:compile` now depends on `app3:compile`,
as `lib2:build` depends on `lib4:build` through `lib3` in the neighbouring specs. The cycle there is `app3:compile` <->
its own dummy for `app2`; the dummy `app1:compile` reaches is in front of it, not on it.

## Related Issue(s)

Follow-up to #28793 (fixes #28788). No issue filed for this.
```

### Minimal reproduction

Three projects: `app` imports `mid`, `mid` and `core` import each other, `mid` has no `build` target. `app` has an
`inner` target and an `agg` target that depends on `^build` and on `inner`; `inner` depends on `^build`.

```ts
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph';

const project = (name: string, targets: Record<string, string[]>) => ({
  name,
  type: 'lib' as const,
  data: {
    root: name,
    targets: Object.fromEntries(
      Object.entries(targets).map(([target, dependsOn]) => [target, { executor: 'nx:noop', dependsOn }])
    ),
  },
});

const graph = {
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
} as any;

const alone = createTaskGraph(graph, {}, ['app'], ['inner'], undefined, {});
const through = createTaskGraph(graph, {}, ['app'], ['agg'], undefined, {});
console.log('nx run app:inner  app:inner ->', JSON.stringify(alone.dependencies['app:inner']));
console.log('nx run app:agg    app:inner ->', JSON.stringify(through.dependencies['app:inner']));
console.log('nx run app:agg    app:agg   ->', JSON.stringify(through.dependencies['app:agg']));
```

Run against the registry's `nx@23.2.1` and against a build of `master` at the commit above, both print:

```text
nx run app:inner  app:inner -> []
nx run app:agg    app:inner -> ["core:build"]
nx run app:agg    app:agg   -> ["app:inner"]
```

and with the patch below, all three print `["core:build"]`, `["core:build"]` and `["core:build", "app:inner"]`.

What `filterDummyTasks` is handed (`dummy(x)` is `mid`'s dummy `build` task made for `x`, such as
`mid:appinner__build__nx_dummy_task__` for `app:inner`):

```text
nx run app:inner   app:inner -> dummy(app:inner) -> core:build -> dummy(core:build) -> core:build
nx run app:agg     app:agg -> dummy(app:agg), app:inner;  dummy(app:agg) -> core:build, app:inner
                   core:build -> dummy(core:build) -> core:build;  app:inner -> dummy(app:inner) -> core:build
```

The only cycle is `core:build` <-> `dummy(core:build)`. Starting from `app:inner`, the depth-first path is the whole
chain, so `findCycles` also reports `app:inner` and `dummy(app:inner)`, and `dummy(app:inner)` is dropped with
`core:build` behind it. Starting from `app:agg`, the traversal returns at the cycle through `dummy(app:agg)`, so
`app:agg` loses `core:build` through that dummy; it searches `app:inner` only afterwards, finds `core:build` visited,
and never flags `dummy(app:inner)`, which is flattened. Fixed, `findCycles` reports only `core:build` and
`dummy(core:build)`, in both.

### Patch

Against `packages/nx/src/tasks-runner/task-graph-utils.ts` (formatted with Prettier 3,
`--single-quote --trailing-comma es5`, which the unchanged files satisfy):

```diff
diff --git a/packages/nx/src/tasks-runner/task-graph-utils.ts b/packages/nx/src/tasks-runner/task-graph-utils.ts
index 51d1a22..26b859f 100644
--- a/packages/nx/src/tasks-runner/task-graph-utils.ts
+++ b/packages/nx/src/tasks-runner/task-graph-utils.ts
@@ -48,22 +48,76 @@ export function findCycle(graph: {

 /**
  * This function finds all cycles in the graph.
- * @returns a list of unique task ids in all cycles found, or null if no cycle is found.
+ * @returns the unique task ids that lie on a cycle (every task of a strongly
+ * connected component of more than one task, and every task that depends on
+ * itself), or null if there is none. A task that merely leads to a cycle is not
+ * on it. The result is a property of the graph, not of the order of its keys.
  */
 export function findCycles(graph: {
   dependencies: Record<string, string[]>;
   continuousDependencies?: Record<string, string[]>;
 }): Set<string> | null {
-  const visited = {};
+  const edges = (id: string): string[] => [
+    ...(graph.dependencies[id] ?? []),
+    ...(graph.continuousDependencies?.[id] ?? []),
+  ];
+
+  // Tarjan's strongly connected components, iterative so that a long chain of
+  // tasks cannot overflow the call stack.
+  const visits = new Map<
+    string,
+    { index: number; lowlink: number; onStack: boolean }
+  >();
+  const stack: string[] = [];
   const cycles = new Set<string>();
-  for (const t of Object.keys(graph.dependencies)) {
-    visited[t] = false;
-  }

-  for (const t of Object.keys(graph.dependencies)) {
-    const cycle = _findCycle(graph, t, visited, [t]);
-    if (cycle) {
-      cycle.forEach((t) => cycles.add(t));
+  const enter = (id: string) => {
+    const visit = { index: visits.size, lowlink: visits.size, onStack: true };
+    visits.set(id, visit);
+    stack.push(id);
+    return { id, visit, edges: edges(id), next: 0 };
+  };
+
+  for (const root of Object.keys(graph.dependencies)) {
+    if (visits.has(root)) {
+      continue;
+    }
+    const frames = [enter(root)];
+    while (frames.length > 0) {
+      const frame = frames[frames.length - 1];
+      if (frame.next < frame.edges.length) {
+        const dep = frame.edges[frame.next++];
+        const seen = visits.get(dep);
+        if (dep === frame.id) {
+          cycles.add(dep);
+        } else if (!seen) {
+          frames.push(enter(dep));
+        } else if (seen.onStack) {
+          frame.visit.lowlink = Math.min(frame.visit.lowlink, seen.index);
+        }
+        continue;
+      }
+
+      frames.pop();
+      const parent = frames[frames.length - 1];
+      if (parent) {
+        parent.visit.lowlink = Math.min(
+          parent.visit.lowlink,
+          frame.visit.lowlink
+        );
+      }
+      if (frame.visit.lowlink === frame.visit.index) {
+        const component: string[] = [];
+        let member: string;
+        do {
+          member = stack.pop();
+          visits.get(member).onStack = false;
+          component.push(member);
+        } while (member !== frame.id);
+        if (component.length > 1) {
+          component.forEach((id) => cycles.add(id));
+        }
+      }
     }
   }

```

Tests, with the one changed expectation:

```diff
diff --git a/packages/nx/src/tasks-runner/create-task-graph.spec.ts b/packages/nx/src/tasks-runner/create-task-graph.spec.ts
index 12f3abd..2333f75 100644
--- a/packages/nx/src/tasks-runner/create-task-graph.spec.ts
+++ b/packages/nx/src/tasks-runner/create-task-graph.spec.ts
@@ -2496,7 +2496,7 @@ describe('createTaskGraph', () => {
       }
     );
     expect(taskGraph).toEqual({
-      roots: ['app1:compile', 'app3:compile'],
+      roots: ['app3:compile'],
       tasks: {
         'app1:compile': {
           id: 'app1:compile',
@@ -2530,7 +2530,7 @@ describe('createTaskGraph', () => {
         },
       },
       dependencies: {
-        'app1:compile': [],
+        'app1:compile': ['app3:compile'],
         'app3:compile': [],
       },
       continuousDependencies: {
@@ -2540,6 +2540,79 @@ describe('createTaskGraph', () => {
     });
   });

+  it('should give a task the same dependencies whichever other tasks are requested (app:inner -> mid <-> core:build)', () => {
+    projectGraph = {
+      nodes: {
+        app: {
+          name: 'app',
+          type: 'app',
+          data: {
+            root: 'app-root',
+            targets: {
+              aggregate: {
+                executor: 'nx:run-commands',
+              },
+              inner: {
+                executor: 'nx:run-commands',
+              },
+            },
+          },
+        },
+        mid: {
+          name: 'mid',
+          type: 'lib',
+          data: {
+            root: 'mid-root',
+            targets: {},
+          },
+        },
+        core: {
+          name: 'core',
+          type: 'lib',
+          data: {
+            root: 'core-root',
+            targets: {
+              build: {
+                executor: 'nx:run-commands',
+              },
+            },
+          },
+        },
+      },
+      dependencies: {
+        app: [{ source: 'app', target: 'mid', type: 'static' }],
+        mid: [{ source: 'mid', target: 'core', type: 'static' }],
+        core: [{ source: 'core', target: 'mid', type: 'static' }],
+      },
+    };
+    const targetDefaults = {
+      aggregate: [{ target: 'build', dependencies: true }, { target: 'inner' }],
+      inner: [{ target: 'build', dependencies: true }],
+      build: [{ target: 'build', dependencies: true }],
+    };
+
+    const alone = createTaskGraph(
+      projectGraph,
+      targetDefaults,
+      ['app'],
+      ['inner'],
+      undefined,
+      { __overrides_unparsed__: [] }
+    );
+    const throughAggregate = createTaskGraph(
+      projectGraph,
+      targetDefaults,
+      ['app'],
+      ['aggregate'],
+      undefined,
+      { __overrides_unparsed__: [] }
+    );
+
+    // `app` reaches `core:build` through `mid`, which has no `build` target.
+    expect(alone.dependencies['app:inner']).toEqual(['core:build']);
+    expect(throughAggregate.dependencies['app:inner']).toEqual(['core:build']);
+  });
+
   it('should not conflate dependencies of dummy tasks', () => {
     projectGraph = {
       nodes: {
diff --git a/packages/nx/src/tasks-runner/task-graph-utils.spec.ts b/packages/nx/src/tasks-runner/task-graph-utils.spec.ts
index accf2bc..c893d2a 100644
--- a/packages/nx/src/tasks-runner/task-graph-utils.spec.ts
+++ b/packages/nx/src/tasks-runner/task-graph-utils.spec.ts
@@ -160,6 +160,81 @@ describe('task graph utils', () => {
         })
       ).toEqual(null);
     });
+
+    it('should not return tasks that only lead to a cycle', () => {
+      expect(
+        findCycles({
+          dependencies: {
+            top: ['x'],
+            x: ['y'],
+            y: ['z'],
+            z: ['y'],
+          },
+        })
+      ).toEqual(new Set(['y', 'z']));
+    });
+
+    it('should return a task that depends on itself', () => {
+      expect(findCycles({ dependencies: { a: ['a'], b: [] } })).toEqual(
+        new Set(['a'])
+      );
+    });
+
+    it('should return a cycle behind a task that was visited before', () => {
+      // The search from `a` stops at the cycle `a` <-> `b` before it looks at
+      // `b`'s other dependency `e`, and the search from `e` finds `b` visited.
+      // `e` is on a cycle with `b` too.
+      expect(
+        findCycles({
+          dependencies: {
+            a: ['b'],
+            b: ['a', 'e'],
+            e: ['b'],
+          },
+        })
+      ).toEqual(new Set(['a', 'b', 'e']));
+    });
+
+    it('should return a continuous cycle', () => {
+      expect(
+        findCycles({
+          dependencies: { a: [], b: [] },
+          continuousDependencies: { a: ['b'], b: ['a'] },
+        })
+      ).toEqual(new Set(['a', 'b']));
+    });
+
+    it('should return the same tasks whatever the order of the graph', () => {
+      const edges: Record<string, string[]> = {
+        a: ['b'],
+        b: ['c'],
+        c: ['b'],
+        d: ['c'],
+      };
+      for (const keys of [
+        ['a', 'b', 'c', 'd'],
+        ['d', 'c', 'b', 'a'],
+        ['c', 'a', 'd', 'b'],
+        ['b', 'd', 'a', 'c'],
+      ]) {
+        const dependencies = Object.fromEntries(
+          keys.map((key) => [key, edges[key]])
+        );
+        expect(findCycles({ dependencies })).toEqual(new Set(['b', 'c']));
+      }
+    });
+
+    it('should not overflow the stack on a long chain of tasks', () => {
+      const dependencies: Record<string, string[]> = {};
+      const length = 50_000;
+      for (let i = 0; i < length; i++) {
+        dependencies[`t${i}`] = [`t${i + 1}`];
+      }
+      dependencies[`t${length}`] = [`t${length - 1}`];
+      expect(findCycles({ dependencies })).toEqual(
+        new Set([`t${length - 1}`, `t${length}`])
+      );
+    });
   });

   describe('makeAcyclic', () => {
```

### How it was checked

Not under Nx's own vitest run: the two spec files and their sources were compiled with `tsc` from a sparse checkout of
`master`, with the native binding stubbed (`validateOutputs` a no-op), and run with `bun test`.

- Without the patch, six tests fail: the four `findCycles` tests that are not guards (`only lead to a cycle`,
  `behind a task that was visited before`, `whatever the order of the graph`, `long chain`), the new `createTaskGraph`
  test, and the changed `app1:build -> app2 <-> app3:build` expectation.
- With it, 72 tests pass and two fail, `assertTaskGraphDoesNotContainInvalidTargets`'s inline snapshots of a thrown
  error, which fail the same way without the patch (Bun and vitest serialise an `Error` differently).
- Every other `createTaskGraph` cycle spec, and the `findCycles` specs that were already there, pass unchanged.

Run `nx test nx --testFile=packages/nx/src/tasks-runner/create-task-graph.spec.ts` and
`--testFile=packages/nx/src/tasks-runner/task-graph-utils.spec.ts` on a real checkout before opening the PR.

### After the PR is opened

Move the `task graph` entry of `NOT_YET_UPSTREAM` in `tooling/patched-nx.ts` to `UPSTREAM` with the PR number. That only
changes the release notes; the tag is the sha256 of the packed tar, and the patch does not change.
