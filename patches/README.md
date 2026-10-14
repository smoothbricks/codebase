# Patches

Patches this repository applies to its dependencies. `bun install` applies the root `package.json`'s
`patchedDependencies`, including `nx@23.2.1.patch`. The root `overrides.nx` pins the registry version `23.2.1`;
`bun.lock` is written by Bun and records that version, registry integrity and patch path. The root `overrides["@nx/js"]`
likewise pins `23.2.1`, the version its patch applies to.

## Flechette factory declarations

All Flechette consumers use the 2.7 series. `@uwdata/flechette@2.7.0.patch` is the reviewed declaration repair shared
with strict external consumers: the upstream source declares the generic row factory as private and readonly, and its
owner-generated `table.d.ts` preserves that field. Runtime factory logic is unchanged. Do not cast tables or relax a
consumer's `skipLibCheck` to hide a factory declaration mismatch. Apply this patch through root `patchedDependencies`
and ordinary `bun install`; rebuild lmao's declarations from the installed product. Retire the patch only when the
upstream declaration preserves the same contract.

Flechette 2.7 builds Date64 metadata from numeric millisecond inputs. The Compact fixture seeds only that schema with a
number; its physical payload remains `BigInt64Array`, with the same i64 round-trip assertions.

## Activating a patch change

Prepare Nx changes with `bun patch nx@23.2.1`, then use the registry-index regeneration flow:

```sh
bun patch nx@23.2.1
python3 tooling/nx_patch.py --check
# Edit the Bun-prepared node_modules/nx package only after the no-op check passes.
python3 tooling/nx_patch.py
rm -rf node_modules/nx # Retire Bun's prepared copy before restoring its isolated dependency links.
bun install
```

The no-op check must reproduce the current patch byte-identically before editing. The script verifies the pristine
registry archive against `bun.lock`, indexes that package in an isolated temporary Git root, and diffs the prepared
package against that index. It preserves full deletion hunks and package-relative paths, excludes only Bun's install
markers and dependency directory, and writes the root patch and its byte-identical managed consumer copy. Its temporary
root is retired on success or failure. `tooling/test_nx_patch.py` guards deletions, additions, installer metadata and
archive/package identity. The generator supports Python 3.9 and newer. Extraction explicitly uses `filter="data"` on
Python 3.12+; older interpreters omit only that unsupported keyword. Every version verifies the archive's lock-selected
SHA-512 before extracting it.

Do not use `bun patch --commit` for this carried patch with Bun 1.4.2: it emits absolute registry-cache paths and
discards the bodies of deleted-file hunks, and includes a `.bun-tag-*` installer marker. Nor hand-edit the generated
patch. Retire the Bun-prepared `node_modules/nx` copy after regeneration: even `bun install --force` leaves that
dependency-less copy in place. Ordinary `bun install` then restores the isolated package and owns activation and all
lock changes; commit them with the patch. Nx is never developer-linked and needs no release publication, generated
tarball or package-activation target. Bun's isolated global store deliberately keeps patched entries project-local under
`node_modules/.bun`; their installed patch marker and cached identity distinguish the patched bytes. Do not replace that
package with a writable generated-product link or edit an installed package without first preparing it. The
installed-product regression runs `node --check` on every patched JavaScript file as well as checking the five-minute
inactivity policy.

The nx-plugin managed files distribute the same Nx patch to consumers. The root monorepo policy maintains
`overrides.nx = "23.2.1"` and `patchedDependencies["nx@23.2.1"] = "patches/nx@23.2.1.patch"`. Run `smoo monorepo update`
and ordinary `bun install` in a consumer to adopt it. Tests import the actual installed Nx package, so a source gate
covers the currently maintained patch without publishing anything. Drop a carried hunk only when the installed upstream
version contains its repair, retaining its regression test.

### Known Bun 1.4.2 deletion-patch bug

Bun 1.4.2 (`744846f84`) generates a malformed deletion section: only a `diff --git` header naming an absolute cache
path, `deleted file mode` and `index`, without `---`, `+++ /dev/null`, the deletion hunk or the removed lines. Full
regeneration therefore corrupts the existing task-history deletions. A fresh registry cache reproduces the same defect;
it is not repaired by clearing the cache.

Minimal reproduction, in a disposable directory:

```sh
mkdir nx-delete-repro
cd nx-delete-repro
printf '{"private":true,"dependencies":{"nx":"23.2.1"}}\n' > package.json
bun install --cache-dir ../nx-registry-cache
bun patch nx@23.2.1 --cache-dir ../nx-registry-cache
rm node_modules/nx/dist/src/daemon/message-types/task-history.js
bun patch --commit nx@23.2.1 --cache-dir ../nx-registry-cache
cat patches/nx@23.2.1.patch
```

The resulting patch has only three lines and cannot carry the deletion's contents. Generate new changed-file sections
with Bun against the pinned registry package, then carry those sections verbatim into the maintained patch while
retaining every existing section byte-identically. Do not rewrite hunk coordinates or adopt the malformed deletion
headers. Exclude the prepared package's `.bun-tag-*` metadata from patch generation. Keep the managed consumer copy
identical and install normally as above. If patch preparation left `node_modules/nx` as an editable directory, remove
that prepared copy before ordinary installation so Nx resolves from Bun's isolated installed store again, with its
dependency links intact.

## Dependency-graph resolution cache identity

The cached dependency edges of an unchanged file are reusable only while its resolution context is unchanged.
`createProjectFileMapCache` records project roots and JavaScript package-resolution metadata: package name, version,
exports, main entry and package-manager workspace membership. `shouldRecomputeWholeGraph` compares that snapshot,
including added or removed projects. Enumeration order is retained because Nx's package-name map is last-writer-wins and
wildcard export lookup uses the first matching entry. Unrelated target and tag changes do not invalidate this dependency
cache. Added TypeScript path aliases also invalidate it, not only changes to previously cached aliases.

A legacy file-map cache without the snapshot is recomputed once through normal graph construction. Nothing deletes,
resets or redirects the task database or cache entries. This fixes a missing-input class where a package's inferred
identity changed while an importing manifest's bytes stayed identical, so live resolution found a dependency but the
persisted graph continued to omit it.

`packages/nx-plugin/src/nx-project-resolution-cache.test.ts` uses the installed Nx cache writer, invalidation decision
and target locator. It proves the missing-edge transition with unchanged file hashes, every resolution field, membership
and collision order, legacy recovery, alias additions, and retention across unrelated target/tag edits. Drop these
carried hunks when the pinned upstream version includes the repair; retain the regression.

## Plugin-load error causes

Both configured and default plugin-load aggregation include each error's message, its complete `cause` chain and every
nested `errors` entry. Local-plugin resolution first reads project configurations, so a duplicate project name can fail
before any plugin runs. The non-verbose diagnostic must retain the duplicate name and both project roots through
`LoadPluginError` and `ProjectConfigurationsError`, not stop at their wrapper messages.

Serialized worker errors retain their `cause` and `errors` when normalized to an `Error`. Formatting follows those same
fields on native and serialized errors; it reports a circular reference without dropping sibling errors, and shared
errors in separate branches remain visible in each branch. It does not dump project configuration objects or require a
stack trace to explain a configuration failure.

`packages/nx-plugin/src/nx-plugin-load-errors.test.ts` drives the actual installed Nx in a fixture containing two
same-named projects and a local plugin, then native, serialized and circular plugin-load failures. Drop this hunk only
when the installed upstream version preserves these diagnostics, retaining the regressions.

## Event-driven daemon validity

The daemon-validity hunks adapt [nrwl/nx PR #37113](https://github.com/nrwl/nx/pull/37113)
(`5812d81d9b00818c00b4a2022f04c998bd1fa381`, addressing [#37112](https://github.com/nrwl/nx/issues/37112)) to the pinned
Nx 23.2.1 distribution. Root lockfile changes restart the daemon only after the native workspace context has hashed the
watcher batch and dropped same-byte rewrites. The startup lockfile baseline is captured before watch/scan. Nested
lockfiles are not workspace lockfiles. The owner-record watcher retires a displaced daemon without waiting for a poll.

Nx 23.2.1 has neither the upstream `ChangeBatch` API nor its `rescan` event. This adaptation checks validity once per
request, including requests on an existing connection, and retains a 5000ms unref'd safety interval for missed events
and socket loss. There is no 20ms validity loop. Each check preserves fresh installed-version resolution and the
daemon's captured socket inode/path: a package-link swap, replaced ownership record or replaced socket cannot leave the
old daemon serving requests or deleting its successor's socket. Root lockfile names as well as bytes enter the fallback
hash, so a missed rename between lockfile formats is visible. A termination latch prevents duplicate replacement spawns
and serving queued requests during asynchronous restart.

The installed-version owner reads the literal `node_modules/<package>/package.json` through each `getNxRequirePaths()`
directory, in order, so the filesystem follows the current link on every read. Node's module resolver can retain stale
realpaths across package-link swaps even after `Module._pathCache` is cleared. Only non-node_modules/PnP layouts fall
back to the existing detached, cache-shielded resolver. Invalid metadata does not fall through to a different installed
copy.

The existing ignore-file content snapshot, watch-before-scan ordering, exclusive daemon claim and five-minute inactivity
policy remain unchanged. This follows `specs/cowshed/04_sandbox.md`'s fixture ownership and one-state-per- checkout
contract and `packages/nx-plugin/README.md`'s runtime-state exclusions and daemon policy. `nx-daemon-validity.test.ts`
exercises the installed product's lockfiles, metadata, ownership and socket lifecycle; the existing claim, ignore-file
and idle-policy regressions remain required. The opt-in `nx-daemon-idle-cpu.test.ts` measures an owned idle daemon using
the kernel's cumulative CPU time, without a profiler or writes during the 30-second window. Run it through the host-only
process-census lane:

```sh
direnv exec <shed> nx run nx-plugin:test-shard1 \
  --command="NX_DAEMON_CPU_PROOF=true bun test --timeout=120000 src/nx-daemon-idle-cpu.test.ts" \
  --forwardAllArgs=false
```

Drop these hunks only when the installed upstream version has the event-driven checks, retaining behavioral and idle CPU
evidence. This is a carry of the existing upstream PR, not a new upstream issue.

## Canonical daemon workspace roots

The daemon and plugin-worker message guard compares canonical path strings. A redundant trailing separator does not make
a workspace foreign to itself, but a different workspace remains refused. The normalizer trims only past the path's
root: POSIX `/`, Windows drive roots and UNC share roots remain absolute, Windows case/separator rules remain intact,
and POSIX case and backslash filename components retain their meaning.

`nx-daemon-root-normalization.test.ts` consumes the actual Bun-installed patched Nx. It checks both platform rules and
drives an owned daemon RPC with the same root's trailing separator, then a genuinely foreign root, and verifies the
daemon remains available to its own workspace. No caller root override, shared-socket allowance or namespace workaround
replaces the guard. Drop this hunk only when upstream contains the same normalization, retaining the regression.

## The installed Nx answers, never `nx@latest`

Stock Nx answers the Nx Console and AI-agent status every `nx run` asks its daemon for by installing `nx@latest` from
the registry into a temporary directory, unless the daemon's environment says `NX_USE_LOCAL=true`; the daemon takes that
environment from whichever client spoke last. Agents, cowshed jobs, test fixtures and library clients (smoo, cowshed's
task hashing, `ensureBuilt`) run without the variable. The `latest-nx` hunk makes the daemon resolve its own installed
Nx instead, whatever a client sends, so it starts no package manager and asks no registry. The `bin/nx` hunk defaults
`NX_USE_LOCAL` to `true` for every command and the processes it starts, so `nx configure-ai-agents`, `nx init` and
`nx migrate` use the installed CLI; `NX_USE_LOCAL=false` restores the latest one for one command.

`nx-use-local.test.ts` starts a fixture daemon from a library client without the variable and then runs the bare
`node_modules/.bin/nx` entry against it, with every package manager on `PATH` replaced by one that records its run. The
daemon must take the branch that pulled `nx@latest`, pull nothing, and start no package manager.

## Run information after a pre-start failure

Stock Nx's discrete worker reports a hash failure through `endTasks` even when `startTasks` never ran. Its
`StoreRunInformationLifeCycle` dereferenced the absent timing entry and threw a `TypeError` over the original error. The
run-information hunk creates that entry only when absent, retaining the failed result and its completion time. An
execution start stays absent unless `startTasks` or the task's own `startTime` supplied one; no start is fabricated.

The composed lifecycle must also complete `endCommand`: native task history requires a hash and an execution start;
legacy history stringifies the start. Both exclude results lacking either, rather than inventing a history row or
crashing after the run information was saved. Genuine database failures for valid execution rows still propagate.
Optional timing diagnostics retain an unstarted result with a `null` duration; profiling emits no executed interval
without a real start. Supplied timestamps, including zero, and normal started tasks retain their timing.

`packages/nx-plugin/src/nx-run-information.test.ts` exercises the actual installed lifecycle with pre-start failures,
absent timestamps, supplied task timestamps and normal started/cache-hit controls.
`packages/nx-plugin/src/nx-failure-life-cycles.test.ts` composes it with native/legacy history and both optional timing
reporters, checks actual SQLite/CSV/profile output and includes a genuine native database-error control. Drop these
hunks only when the installed upstream package handles the same ordering, retaining the regressions.

## Requested task closure after dummy normalization

The `create-task-graph` hunk retains only the caller's initial tasks and the tasks reachable from them through the
normalized regular or continuous dependency edges. Expansion may discover a real producer behind a dummy cycle that
normalization later removes. Leaving that producer in `tasks` makes it an unrelated new root, which Nx executes even
though no requested task depends on it. On a model-command graph, fourteen such tasks pulled in complete role and native
builds; no dependency of the nineteen requested-closure tasks was changed.

The selection happens at Nx's graph-construction owner, not in a consumer cache probe or launcher. Explicitly requested
tasks, genuine producer edges, continuous-only dependencies and real task cycles remain intact. The existing
`nx-task-graph-cycles.test.ts` guards both this closure rule and cycle membership against the actual Bun-installed
patched package. Ordinary installation applies the current patch before test typechecking or execution; no publication
or generated product substitutes for the lock-selected implementation. Drop each carried hunk only when the installed
upstream version contains its respective repair, retaining the regressions.

## Upstream PR draft: Nx `findCycles`

Status: drafted, not opened. Written against nrwl/nx `master` at `a37b5ca4c6630bb9ea5df9ac0546edc3a186bc2f`, whose
`findCycles`, `filterDummyTasks` and `getNonDummyDeps` have the logic of Nx 23.2.1's. This repository carries the fix as
the `task-graph-utils` hunk of `nx@23.2.1.patch`; `packages/nx-plugin/src/nx-task-graph-cycles.test.ts` guards it here.
Drop that patch hunk once the installed Nx contains the fix, retaining the regression.

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

Update the draft's status with the upstream PR number. Retain the carried patch and its regressions until the installed
registry version contains the repair.
