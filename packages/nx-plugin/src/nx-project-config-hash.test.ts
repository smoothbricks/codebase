import { describe, expect, it } from 'bun:test';
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { Task, TaskGraph } from 'nx/src/config/task-graph';
import type { TargetConfiguration } from 'nx/src/config/workspace-json-project-json';
import { hashArray } from 'nx/src/hasher/file-hasher.js';
import { NativeTaskHasherImpl } from 'nx/src/hasher/native-task-hasher-impl';
import type { HashDetails } from 'nx/src/native';

/**
 * Guards the project-configuration hunk of `patches/nx@24.0.3.patch`. Nx 24.0.3 adds a `<project>:ProjectConfiguration`
 * node to a task's key for its own project and every project its `^` inputs reach (`hash_planner.rs`
 * `gather_self_inputs`), and `hash_project_config.rs` hashes the executor, outputs, options, configurations and
 * parallelism of every target of that project: one target's edit re-keyed every task of the project. The patched
 * `NativeTaskHasherImpl` replaces each such node after the native hash: the task's own project hashes its root, tags,
 * named inputs and the task's own target; a project its `^` inputs reach hashes its root, tags and named inputs. The
 * task's value folds its details again, in key order. The installed class runs on an instance whose native planner and
 * hasher are faked at their published methods, each native hash call (`hashPlans`, `hashPlansUpfront`, `hashPlansFor`)
 * answering the same details: a native node's value never reaches the key.
 */

type Targets = Record<string, TargetConfiguration>;

function project(name: string, targets: Targets, namedInputs: Record<string, string[]> = {}): ProjectGraphProjectNode {
  return { name, type: 'lib', data: { root: `packages/${name}`, tags: ['scope:test'], namedInputs, targets } };
}

/** `app` runs `build` and has a sibling `refresh`; `lib` is a project `app:build`'s `^` inputs reach. */
function workspace(): ProjectGraph {
  return {
    nodes: {
      app: project(
        'app',
        {
          build: {
            executor: 'nx:run-commands',
            outputs: ['{projectRoot}/dist'],
            options: { command: 'bun build.ts' },
            configurations: { production: { command: 'bun build.ts --minify' } },
          },
          refresh: { executor: 'nx:run-commands', options: { command: 'bash refresh.sh' } },
        },
        { production: ['{projectRoot}/src/**/*'] },
      ),
      lib: project('lib', { build: { executor: 'nx:run-commands', options: { command: 'tsc' } } }),
    },
    dependencies: { app: [{ source: 'app', target: 'lib', type: 'static' }], lib: [] },
  };
}

const TASK: Task = {
  id: 'app:build',
  target: { project: 'app', target: 'build' },
  overrides: {},
  outputs: ['packages/app/dist'],
  cache: true,
};
const TASK_GRAPH: TaskGraph = {
  roots: [TASK.id],
  tasks: { [TASK.id]: TASK },
  dependencies: { [TASK.id]: [] },
  continuousDependencies: { [TASK.id]: [] },
};

/** What the native hasher answers for `app:build`: values fixed, whatever the graph says. */
function nativeDetails(): HashDetails {
  return {
    value: 'native',
    details: {
      'app:ProjectConfiguration': '11',
      'app:TsConfig': '12',
      'lib:ProjectConfiguration': '13',
      'ProjectFileSet:app': '14',
    },
    inputs: { files: [], runtime: [], environment: [], depOutputs: [], external: [] },
  };
}

type Path = 'hashPlans' | 'hashPlansUpfront' | 'hashPlansFor';

/** `app:build`'s hash through one native hash call of the installed class, over `graph`. */
async function hashed(graph: ProjectGraph, path: Path = 'hashPlans'): Promise<HashDetails> {
  const native = Object.assign(Object.create(NativeTaskHasherImpl.prototype), {
    projectGraph: graph,
    upfrontPlans: null,
    planner: { getPlansReference: () => 'plans', getPlans: () => ({}) },
    hasher: {
      hashPlans: () => ({ [TASK.id]: nativeDetails() }),
      hashPlansUpfront: () => (path === 'hashPlansUpfront' ? { [TASK.id]: nativeDetails() } : {}),
      hashPlansFor: () => ({ [TASK.id]: nativeDetails() }),
    },
  });
  const envs = { [TASK.id]: {} };
  if (path === 'hashPlansUpfront') {
    // The up-front pass answers by task id, for the tasks it did not defer.
    const upfront: Record<string, HashDetails> = await NativeTaskHasherImpl.prototype.hashTasksUpfront.call(
      native,
      [TASK],
      TASK_GRAPH,
      envs,
    );
    const result = upfront[TASK.id];
    if (result === undefined) throw new Error('the up-front pass deferred the task');
    return result;
  }
  if (path === 'hashPlansFor') {
    // The up-front pass deferred the task; the later hash reuses its plans through `hashPlansFor`.
    await NativeTaskHasherImpl.prototype.hashTasksUpfront.call(native, [TASK], TASK_GRAPH, envs);
  }
  const [result] = await NativeTaskHasherImpl.prototype.hashTasks.call(native, [TASK], TASK_GRAPH, envs, '/', false);
  return result;
}

function edited(edit: (graph: ProjectGraph) => void): ProjectGraph {
  const graph = workspace();
  edit(graph);
  return graph;
}

const app = (graph: ProjectGraph) => graph.nodes.app.data;
const lib = (graph: ProjectGraph) => graph.nodes.lib.data;

describe("a task's ProjectConfiguration node", () => {
  it("keys the task's own target, its project's root, tags and named inputs, and no sibling target", async () => {
    const base = (await hashed(workspace())).value;
    const rows: { edit: string; graph: ProjectGraph; rekeys: boolean }[] = [
      {
        edit: "a sibling target's command",
        graph: edited((g) => {
          app(g).targets!.refresh.options = { command: 'bash refresh.sh --all' };
        }),
        rekeys: false,
      },
      { edit: 'a sibling target removed', graph: edited((g) => void delete app(g).targets!.refresh), rekeys: false },
      {
        edit: "a reached project's target",
        graph: edited((g) => {
          lib(g).targets!.build.options = { command: 'tsc -b' };
        }),
        rekeys: false,
      },
      {
        edit: "the task's own command",
        graph: edited((g) => {
          app(g).targets!.build.options = { command: 'bun build.ts --sourcemap' };
        }),
        rekeys: true,
      },
      {
        edit: "the task's own executor",
        graph: edited((g) => {
          app(g).targets!.build.executor = 'nx:noop';
        }),
        rekeys: true,
      },
      {
        edit: "the task's own outputs",
        graph: edited((g) => {
          app(g).targets!.build.outputs = ['{projectRoot}/out'];
        }),
        rekeys: true,
      },
      {
        edit: "the task's own configurations",
        graph: edited((g) => {
          app(g).targets!.build.configurations = {};
        }),
        rekeys: true,
      },
      {
        edit: "the task's own parallelism",
        graph: edited((g) => {
          app(g).targets!.build.parallelism = false;
        }),
        rekeys: true,
      },
      {
        edit: "its project's named inputs",
        graph: edited((g) => {
          app(g).namedInputs = { production: ['{projectRoot}/lib/**/*'] };
        }),
        rekeys: true,
      },
      {
        edit: "its project's tags",
        graph: edited((g) => {
          app(g).tags = ['scope:other'];
        }),
        rekeys: true,
      },
      {
        edit: "a reached project's named inputs",
        graph: edited((g) => {
          lib(g).namedInputs = { production: ['{projectRoot}/src/**/*'] };
        }),
        rekeys: true,
      },
    ];
    const verdicts = await Promise.all(
      rows.map(async ({ edit, graph }) => ({ edit, rekeys: (await hashed(graph)).value !== base })),
    );
    expect(verdicts).toEqual(rows.map(({ edit, rekeys }) => ({ edit, rekeys })));
  });

  it('replaces only ProjectConfiguration nodes and folds the details in key order', async () => {
    const result = await hashed(workspace());
    const keys = Object.keys(result.details).sort();
    expect(keys).toEqual(Object.keys(nativeDetails().details).sort());
    expect(result.details['app:TsConfig']).toBe('12');
    expect(result.details['ProjectFileSet:app']).toBe('14');
    expect(result.details['app:ProjectConfiguration']).not.toBe('11');
    expect(result.details['lib:ProjectConfiguration']).not.toBe('13');
    expect(result.value).toBe(hashArray(keys.map((key) => result.details[key] ?? '')));
  });

  it('keys a task alike through every native hash call', async () => {
    const graph = workspace();
    const values = await Promise.all(
      (['hashPlans', 'hashPlansUpfront', 'hashPlansFor'] as const).map(
        async (path) => (await hashed(graph, path)).value,
      ),
    );
    expect(new Set(values).size).toBe(1);
  });
});
