// The gate's cache keys, bound to their model (specs/cowshed/laws/cache_key.bend; 03_caches.md "Nx cache" and "What a
// land re-runs"). The model is proven. This driver runs it on the task graph Nx builds for this repository: for each
// Rust runner, the model's `a_viol_of`, `u_viol_of` and `b_viol_of` judge the key Nx resolves for it against the
// producers its run reads, and a violation is a red naming the runner and the producer.
// `__tests__/cache-key-model.ts` is the model emitted by bend-emit (its header names the command); it is never edited.
//
// What the binding takes from Nx: a producer's `dependsOn` edges and outputs, its executor and command (which give its
// language and the files its tool writes), and the `dependentTasksOutputFiles` selectors of the runner's key. What it
// holds by hand is `RUNTIME_READS`: the producers a runner's run reads, each with the file that reads them. A key is
// one for every platform (nx.json's targetDefaults), so a read the tests of any platform make is a read on all of them.
import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { posix } from 'node:path';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { Task } from 'nx/src/config/task-graph';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph.js';
import typia from 'typia';
import * as model from './__tests__/cache-key-model.js';
import { REPOSITORY, runInRepositoryNx } from './__tests__/repository-nx.js';
import { packageNameFromCargoTestTarget } from './cargo-workspace.js';
import { isRecord } from './type-guards.js';

const BOUNDED_EXEC = '@smoothbricks/nx-plugin:bounded-exec';
const TYPESCRIPT_EMIT = '@smoothbricks/nx-plugin:typescript-emit';
/** The Linux addon's producer on every host (index.ts, the napi targets): it writes the one `.node` file its `output` names. */
const CARGO_ZIGBUILD = '@smoothbricks/nx-plugin:cargo-zigbuild';

/**
 * A producer a Rust runner's run reads at run time and whose bytes reach its verdict (the model's `reads`): the target
 * that writes it, the files of it the run reads, the crates whose tests read them, and the file that reads them. A
 * producer the runner keys through the sources it is built from, not through its output (the test archive
 * `cargo-test-build` writes, which the runner keys by the Cargo closure that built it), is not listed: the key names it.
 */
interface RuntimeRead {
  readonly crates: readonly string[];
  readonly producer: { readonly project: string; readonly target: string };
  readonly glob: string;
  readonly evidence: string;
}

const RUNTIME_READS: readonly RuntimeRead[] = [
  {
    // The test takes NAPI_DEBUG_ADDON away, so `addonSearch` names the dev addon `.cache/native-dev`, never `.cache/native-debug`.
    crates: ['cowshed-core', 'cowshed-cli'],
    producer: { project: 'cowshed', target: 'napi-native' },
    glob: '**/*.node',
    evidence:
      'cowshed-core/tests/support/napi_client.rs:30-55 runs Node on the package, whose loadAddon loads the dev addon',
    // project_runtime.rs:68 builds napi_client only on macOS; the one key that serves Linux too still covers it.
  },
  {
    crates: ['cowshed-core', 'cowshed-cli'],
    producer: { project: 'cowshed', target: 'tsc-js' },
    glob: 'packages/cowshed/dist/ts/**/*.js',
    evidence: 'cowshed-core/tests/support/napi_client.rs:30-37 canonicalizes packages/cowshed/dist/ts/index.js',
  },
  {
    crates: ['cowshed-core'],
    producer: { project: 'cowshed', target: 'capability-probe' },
    glob: '**/capability-probe/diagnostics.json',
    evidence: 'cowshed-core/tests/public_api_contracts.rs:1501 CAPABILITY_VERDICT_FILE',
  },
];

/** The text a target runs: its `command`, or its `commands` in order. */
function commandOf(target: TargetConfiguration): string {
  const options: unknown = target.options;
  if (!isRecord(options)) return '';
  if (typeof options.command === 'string') return options.command;
  return Array.isArray(options.commands)
    ? options.commands.filter((command): command is string => typeof command === 'string').join(' && ')
    : '';
}

/** What a target builds its outputs from, read off its executor and its command. */
function languageOf(target: TargetConfiguration): model.Lang {
  const command = commandOf(target);
  if (target.executor === TYPESCRIPT_EMIT || /^(tsc|bun build)\b/.test(command)) return { $: 'Ts' };
  if (target.executor === CARGO_ZIGBUILD || /\b(cargo|napi build|just wasm)\b/.test(command)) return { $: 'Rust' };
  return { $: 'Other' };
}

/**
 * The files a producer writes, enough for a glob to decide on. A declared output with an extension is a file. One
 * without is a directory holding what its tool emits there: JavaScript and declarations from a TypeScript compiler,
 * the addon from `napi build` (a `--bin` build writes an executable), one file without an extension from any other
 * tool, which only `**` matches. A cargo-zigbuild producer writes the one file its `output` option names; Nx resolves
 * its declared `{options.output}/../` to no output on a task graph, so the option is read directly.
 */
function writtenFiles(task: Task, target: TargetConfiguration): string[] {
  if (target.executor === CARGO_ZIGBUILD && isRecord(target.options) && typeof target.options.output === 'string') {
    return [target.options.output.replaceAll('{projectRoot}', task.projectRoot ?? '')];
  }
  const command = commandOf(target);
  let emitted = ['output'];
  if (target.executor === TYPESCRIPT_EMIT || /^tsc\b/.test(command)) emitted = ['index.js', 'index.d.ts'];
  else if (/\bnapi build\b/.test(command) && !/--bin\b/.test(command)) emitted = ['addon.node'];
  return task.outputs.flatMap((output) =>
    /\.[A-Za-z0-9]+$/.test(posix.basename(output)) ? [output] : emitted.map((file) => `${output}/${file}`),
  );
}

interface Selector {
  readonly glob: string;
  readonly transitive: boolean;
}

type NamedInputs = Readonly<Record<string, readonly unknown[]>>;

/** The named input an input of a target refers to, if it does. */
function namedReference(input: unknown): string | undefined {
  if (typeof input === 'string') return input.replace(/^\^/, '');
  return isRecord(input) && typeof input.input === 'string' ? input.input : undefined;
}

/** The `dependentTasksOutputFiles` inputs of a target, named inputs expanded: what its key hashes of its dependencies. */
function selectorsOf(
  inputs: readonly unknown[],
  named: NamedInputs,
  seen: ReadonlySet<string> = new Set(),
): Selector[] {
  return inputs.flatMap((input) => {
    if (isRecord(input) && typeof input.dependentTasksOutputFiles === 'string') {
      return [{ glob: input.dependentTasksOutputFiles, transitive: input.transitive === true }];
    }
    const name = namedReference(input);
    const expansion = name === undefined || seen.has(name) ? undefined : named[name];
    return name === undefined || expansion === undefined ? [] : selectorsOf(expansion, named, new Set([...seen, name]));
  });
}

function list<T>(items: readonly T[]): model.List<T> {
  return [...items].reverse().reduce<model.List<T>>((tail, head) => ({ $: 'Con', head, tail }), model.NIL);
}

function array<T>(items: model.List<T>): T[] {
  const out: T[] = [];
  for (let at = items; at.$ === 'Con'; at = at.tail) out.push(at.head);
  return out;
}

/** One Rust runner of the graph: a target whose key the model judges. */
interface Runner {
  readonly project: string;
  readonly target: string;
  readonly id: string;
  /** The Cargo package its tests run, when its name gives it. */
  readonly crate: string | null;
}

/** A runner's key as the model judges it, with the names the model's numbers stand for. */
interface Judged {
  readonly runner: Runner;
  /** The task each model producer (by number) stands for. */
  readonly names: readonly string[];
  /** The files each producer writes. */
  readonly files: readonly (readonly string[])[];
  readonly selectors: readonly Selector[];
  /** Law (a): TypeScript producers whose output the key names and the run does not read. */
  readonly keyedTs: readonly string[];
  /** Law (a') beyond (a): producers of another language whose output the key names and the run does not read. */
  readonly keyedOther: readonly string[];
  /** Law (b): producers whose output the run reads and the key does not name. */
  readonly unkeyed: readonly string[];

  /** The file that reads each producer the run reads, by task id. */
  readonly evidence: Readonly<Record<string, string>>;
}

function rustRunners(graph: ProjectGraph): Runner[] {
  return Object.values(graph.nodes).flatMap((node) =>
    Object.entries(node.data.targets ?? {}).flatMap(([target, configuration]) =>
      configuration.executor === BOUNDED_EXEC && /^cargo\b/.test(commandOf(configuration))
        ? [{ project: node.name, target, id: `${node.name}:${target}`, crate: packageNameFromCargoTestTarget(target) }]
        : [],
    ),
  );
}

/**
 * The model's three checks on one runner. The graph the model gets is Nx's own task graph of the runner and of the
 * producers its run reads (which it may not wait for): each task carries its language, whether it writes files and its
 * `dependsOn` edges. The runner alone carries the producers it reads and the selectors of its key, each selector as one
 * single-producer selector per producer whose files its glob matches.
 */
function judge(graph: ProjectGraph, runner: Runner, named: NamedInputs, reads: readonly RuntimeRead[]): Judged {
  const present = reads.filter(
    ({ producer }) => graph.nodes[producer.project]?.data.targets?.[producer.target] !== undefined,
  );
  const closures = [
    createTaskGraph(graph, {}, [runner.project], [runner.target], undefined, {}),
    ...present.map(({ producer }) => createTaskGraph(graph, {}, [producer.project], [producer.target], undefined, {})),
  ];
  const tasks = new Map<string, Task>();
  const waitsFor = new Map<string, string[]>();
  for (const closure of closures) {
    for (const [id, task] of Object.entries(closure.tasks)) {
      tasks.set(id, task);
      waitsFor.set(id, closure.dependencies[id]?.map((edge) => edge.id) ?? []);
    }
  }
  const names = [...tasks.keys()];
  const numberOf = (id: string): number => names.indexOf(id);
  const configurationOf = (task: Task): TargetConfiguration => {
    const found = graph.nodes[task.target.project]?.data.targets?.[task.target.target];
    if (found === undefined) throw new Error(`${task.id} names no target of the graph`);
    return found;
  };
  const taskOf = (id: string): Task => {
    const task = tasks.get(id);
    if (task === undefined) throw new Error(`${id} is no task of the graph`);
    return task;
  };
  const files = names.map((id) => writtenFiles(taskOf(id), configurationOf(taskOf(id))));
  const matching = (glob: string, ids: Iterable<string>): number[] =>
    [...ids].map(numberOf).filter((index) => files[index]?.some((file) => new Bun.Glob(glob).match(file)) === true);
  const selectors = selectorsOf(configurationOf(taskOf(runner.id)).inputs ?? [], {
    ...named,
    ...graph.nodes[runner.project]?.data.namedInputs,
  });
  const evidence: Record<string, string> = {};
  const readNumbers = present.flatMap(({ glob, producer, evidence: file }, index) => {
    const read = matching(glob, Object.keys(closures[index + 1]?.tasks ?? {}));
    if (read.length === 0) {
      throw new Error(`${producer.project}:${producer.target} writes no file matching ${glob}, which ${file}`);
    }
    for (const number of read) evidence[names[number] ?? ''] = file;
    return read;
  });
  const targets: model.Tgt[] = names.map((id, index) => ({
    $: 'Tgt',
    id: index,
    lang: languageOf(configurationOf(taskOf(id))),
    out: files[index]?.length ? { $: 'OData' } : { $: 'ONone' },
    srcs: model.NIL,
    runs: model.NIL,
    reads: id === runner.id ? list([...new Set(readNumbers)]) : model.NIL,
    deps: list((waitsFor.get(id) ?? []).map(numberOf)),
    sels:
      id === runner.id
        ? list(
            selectors.flatMap(({ glob, transitive }) =>
              matching(glob, names).map(
                (producer): model.Sel => ({
                  $: 'Sel',
                  glob: { $: 'GOf', p: producer },
                  tr: transitive,
                }),
              ),
            ),
          )
        : model.NIL,
  }));
  const self = targets[numberOf(runner.id)];
  if (self === undefined) throw new Error(`${runner.id} has no model target`);
  const whole = list(targets);
  const producers = (violations: model.List<model.Viol>): string[] =>
    array(violations).map(({ p }) => names[p] ?? `#${p}`);
  const keyedTs = new Set(producers(model.a_viol_of(whole, self)));
  return {
    runner,
    names,
    files,
    selectors,
    keyedTs: [...keyedTs],
    keyedOther: producers(model.u_viol_of(whole, self)).filter((producer) => !keyedTs.has(producer)),
    unkeyed: producers(model.b_viol_of(whole, self)),
    evidence,
  };
}

const parseProjectGraph = typia.json.createAssertParse<ProjectGraph>();

// The scripts below run in a child process, the repository's own Nx (see runInRepositoryNx): their imports are the
// child's, so they cannot be static imports of this module.
const GRAPH_SCRIPT = `const { createProjectGraphAsync } = await import('nx/src/project-graph/project-graph.js');
const graph = await createProjectGraphAsync({ exitOnError: false });
process.stdout.write(JSON.stringify({ nodes: graph.nodes, dependencies: graph.dependencies }));`;

/** What Nx hashes for each runner, in a process of its own: the key a gate looks up. */
function hashScript(runners: readonly Runner[]): string {
  const pairs = JSON.stringify(runners.map(({ project, target }) => [project, target]));
  return `const { createProjectGraphAsync } = await import('nx/src/project-graph/project-graph.js');
const { createTaskGraph } = await import('nx/src/tasks-runner/create-task-graph.js');
const { createTaskHasher } = await import('nx/src/hasher/create-task-hasher.js');
const { readNxJson } = await import('nx/src/config/nx-json.js');
const graph = await createProjectGraphAsync({ exitOnError: false });
const runners = ${pairs};
const taskGraph = createTaskGraph(graph, {}, [...new Set(runners.map(([project]) => project))], [...new Set(runners.map(([, target]) => target))], undefined, {});
const tasks = runners.map(([project, target]) => taskGraph.tasks[project + ':' + target]);
const hashes = await createTaskHasher(graph, readNxJson(process.cwd())).hashTasks(tasks, taskGraph, process.env, process.cwd());
process.stdout.write(JSON.stringify(Object.fromEntries(tasks.map((task, index) => [task.id, hashes[index].value]))));`;
}

let repository: Promise<readonly Judged[]> | undefined;

/** Every Rust runner of this repository's graph, judged; computed once for the file because it costs a child process. */
function judgedRunners(): Promise<readonly Judged[]> {
  repository ??= (async () => {
    const graph = parseProjectGraph(await runInRepositoryNx(GRAPH_SCRIPT));
    const nx: unknown = JSON.parse(await readFile(`${REPOSITORY}/nx.json`, 'utf8'));
    const declared = isRecord(nx) && isRecord(nx.namedInputs) ? nx.namedInputs : {};
    const named: NamedInputs = Object.fromEntries(
      Object.entries(declared).flatMap(([name, inputs]) => (Array.isArray(inputs) ? [[name, inputs]] : [])),
    );
    return rustRunners(graph).map((runner) =>
      judge(
        graph,
        runner,
        named,
        RUNTIME_READS.filter(({ crates }) => runner.crate !== null && crates.includes(runner.crate)),
      ),
    );
  })();
  return repository;
}

/** The selector of a runner's key that names `producer`'s files, as the report says it. */
function keyedBy(judged: Judged, producer: string): string {
  const files = judged.files[judged.names.indexOf(producer)] ?? [];
  const selector = judged.selectors.find(({ glob }) => files.some((file) => new Bun.Glob(glob).match(file)));
  return selector === undefined ? 'no selector' : `${selector.glob}${selector.transitive ? ', transitive' : ''}`;
}

describe('the cache keys of the Rust runners of this repository, judged by specs/cowshed/laws/cache_key.bend', () => {
  it('judges runners whose keys name producers: the check is not vacuous', async () => {
    const judged = await judgedRunners();
    expect(judged.length).toBeGreaterThan(40);
    // The runners whose tests run Node on the built package read a producer's output, so the judge has one to look for.
    const readers = judged.filter(({ runner }) => runner.crate === 'cowshed-core' || runner.crate === 'cowshed-cli');
    expect(readers.length).toBeGreaterThan(0);
    expect(readers.every(({ evidence, selectors }) => selectors.length > 0 && Object.keys(evidence).length > 0)).toBe(
      true,
    );
  });

  it('(a) keys no TypeScript build output a Rust runner does not read', async () => {
    const keyed = (await judgedRunners()).flatMap((entry) =>
      entry.keyedTs.map((p) => `${entry.runner.id} keys ${p} (${keyedBy(entry, p)}) and its run does not read it`),
    );
    expect(keyed).toEqual([]);
  });

  it("(a') keys no output of another language a Rust runner does not read", async () => {
    const keyed = (await judgedRunners()).flatMap((entry) =>
      entry.keyedOther.map((p) => `${entry.runner.id} keys ${p} (${keyedBy(entry, p)}) and its run does not read it`),
    );
    expect(keyed).toEqual([]);
  });

  it('(b) keys every output the tests of a crate read: some shard of the crate runs the test that reads it', async () => {
    const judged = await judgedRunners();
    const crates = [...new Set(judged.flatMap(({ runner }) => (runner.crate === null ? [] : [runner.crate])))];
    const missing = crates.flatMap((crate) => {
      const shards = judged.filter(({ runner }) => runner.crate === crate);
      const unkeyed = shards[0]?.unkeyed.filter((p) => shards.every((shard) => shard.unkeyed.includes(p))) ?? [];
      return unkeyed.map(
        (p) =>
          `${crate}: a test reads ${p} (${shards[0]?.evidence[p]}) and none of its ${shards.length} runners keys it`,
      );
    });
    expect(missing).toEqual([]);
  });

  it('(c) hashes every runner the same in two cold computations of one tree', async () => {
    const script = hashScript((await judgedRunners()).map(({ runner }) => runner));
    const first = await runInRepositoryNx(script);
    expect(await runInRepositoryNx(script)).toBe(first);
    const hashes: unknown = JSON.parse(first);
    expect(isRecord(hashes) ? new Set(Object.values(hashes)).size : 0).toBeGreaterThan(1);
  });
});

describe('the judge, on a graph that plants each violation', () => {
  const lineage = { dependentTasksOutputFiles: '**/dist/lineage/**/*.js', transitive: true };
  const addon = { dependentTasksOutputFiles: '**/*.node', transitive: true };
  const owner = (inputs: TargetConfiguration['inputs']): ProjectGraph => ({
    nodes: {
      owner: {
        name: 'owner',
        type: 'lib',
        data: {
          root: 'owner',
          targets: {
            'lineage-js': {
              executor: 'nx:run-commands',
              options: { command: 'tsc --build tsconfig.lineage.json' },
              outputs: ['{projectRoot}/dist/lineage'],
            },
            'napi-native': {
              executor: 'nx:run-commands',
              options: { command: 'napi build --platform --no-js' },
              outputs: ['{projectRoot}/.cache/native-dev/'],
            },
            'cargo-test-owner': {
              executor: BOUNDED_EXEC,
              options: { command: 'cargo --frozen nextest run' },
              dependsOn: ['napi-native', 'lineage-js'],
              inputs,
            },
          },
        },
      },
    },
    dependencies: { owner: [] },
  });
  const runner: Runner = { project: 'owner', target: 'cargo-test-owner', id: 'owner:cargo-test-owner', crate: 'owner' };
  const readsAddon: RuntimeRead = {
    crates: ['owner'],
    producer: { project: 'owner', target: 'napi-native' },
    glob: '**/*.node',
    evidence: 'a test of the fixture loads the addon',
  };

  it('names the TypeScript output a Rust runner keys without reading it', () => {
    const judged = judge(owner([addon, lineage]), runner, {}, [readsAddon]);
    expect([judged.keyedTs, judged.keyedOther, judged.unkeyed]).toEqual([['owner:lineage-js'], [], []]);
  });

  it('is silent when the runner keys only the addon it reads', () => {
    const judged = judge(owner([addon]), runner, {}, [readsAddon]);
    expect([judged.keyedTs, judged.keyedOther, judged.unkeyed]).toEqual([[], [], []]);
  });

  it('names an addon the runner keys without reading it, as another language', () => {
    const judged = judge(owner([addon]), runner, {}, []);
    expect([judged.keyedTs, judged.keyedOther, judged.unkeyed]).toEqual([[], ['owner:napi-native'], []]);
  });

  it('names an addon the runner reads and its key leaves out', () => {
    const judged = judge(owner([lineage]), runner, {}, [readsAddon]);
    expect(judged.unkeyed).toEqual(['owner:napi-native']);
  });

  it('finds the addon a cargo-zigbuild producer writes, as the Linux host builds it', () => {
    const linux = owner([addon]);
    const targets = linux.nodes.owner?.data.targets ?? {};
    targets['napi-x64-linux'] = {
      executor: CARGO_ZIGBUILD,
      options: { output: '{projectRoot}/.cache/native-dev/owner.linux-x64-gnu.node' },
      outputs: ['{options.output}/../'],
    };
    targets['napi-native'] = { executor: 'nx:noop', outputs: [], dependsOn: ['napi-x64-linux'] };
    const judged = judge(linux, runner, {}, [readsAddon]);
    expect([judged.keyedTs, judged.keyedOther, judged.unkeyed]).toEqual([[], [], []]);
  });
});

describe('the cache-key model, on its enumerated corpus and its seeded draws', () => {
  const draws = <A>(seed: bigint, count: number, draw: (rng: model.Rng) => A): A[] => {
    const rng = new model.Rng(seed);
    return Array.from({ length: count }, () => draw(rng));
  };
  const failures = <A extends unknown[]>(
    name: string,
    rows: readonly A[],
    law: (...args: A) => [boolean, string],
  ): string[] =>
    rows.flatMap((row, index) => {
      const [holds, shown] = law(...row);
      return holds ? [] : [`${name} ${index}: ${shown}`];
    });
  const length = model.PROP_LEN_KEY_COMPLETE;
  const graph = (rng: model.Rng): [model.List<model.Tgt>] => [model.gen_ListTgt(rng, length)];
  const inputs = (rng: model.Rng): [model.List<model.Item>, model.List<model.Row>, model.List<model.Row>] => [
    model.gen_ListItem(rng, length),
    model.gen_ListRow(rng, length),
    model.gen_ListRow(rng, length),
  ];
  const checkouts = (
    rng: model.Rng,
  ): [model.List<model.Tgt>, model.Tgt, model.List<model.Row>, model.List<model.Row>] => [
    model.gen_ListTgt(rng, length),
    model.gen_Tgt(rng, length),
    model.gen_ListRow(rng, length),
    model.gen_ListRow(rng, length),
  ];

  it('holds the three laws on every graph the inference builds', () => {
    expect([
      ...failures(
        'rust_key_has_no_ts_output',
        [
          ...model.corpus_rust_key_has_no_ts_output(),
          ...draws(model.SEED_RUST_KEY_HAS_NO_TS_OUTPUT, model.PROPS_RUST_KEY_HAS_NO_TS_OUTPUT, graph),
        ],
        model.law_rust_key_has_no_ts_output,
      ),
      ...failures(
        'key_has_only_reads',
        [
          ...model.corpus_key_has_only_reads(),
          ...draws(model.SEED_KEY_HAS_ONLY_READS, model.PROPS_KEY_HAS_ONLY_READS, graph),
        ],
        model.law_key_has_only_reads,
      ),
      ...failures(
        'key_complete',
        [...model.corpus_key_complete(), ...draws(model.SEED_KEY_COMPLETE, model.PROPS_KEY_COMPLETE, graph)],
        model.law_key_complete,
      ),
    ]).toEqual([]);
  });

  it('holds the key a function of its input bytes, and moving with them', () => {
    expect([
      ...failures('key_is_pure', draws(model.SEED_KEY_IS_PURE, model.PROPS_KEY_IS_PURE, inputs), model.law_key_is_pure),
      ...failures(
        'key_moves_with_input',
        draws(model.SEED_KEY_MOVES_WITH_INPUT, model.PROPS_KEY_MOVES_WITH_INPUT, inputs),
        model.law_key_moves_with_input,
      ),
      ...failures(
        'unchanged_tree_runs_nothing',
        draws(model.SEED_UNCHANGED_TREE_RUNS_NOTHING, model.PROPS_UNCHANGED_TREE_RUNS_NOTHING, checkouts),
        model.law_unchanged_tree_runs_nothing,
      ),
      ...failures(
        'changed_input_changes_key',
        draws(model.SEED_CHANGED_INPUT_CHANGES_KEY, model.PROPS_CHANGED_INPUT_CHANGES_KEY, checkouts),
        model.law_changed_input_changes_key,
      ),
      ...failures('key_can_differ', model.corpus_key_can_differ(), model.law_key_can_differ),
    ]).toEqual([]);
  });

  it('holds the plugin observer refuted on its three-target graph, and the definedness companions', () => {
    expect([
      ...failures('observer_key_refuted', model.corpus_observer_key_refuted(), model.law_observer_key_refuted),
      ...failures(
        'observer_key_sound_when_read',
        model.corpus_observer_key_sound_when_read(),
        model.law_observer_key_sound_when_read,
      ),
      ...failures(
        'inferred_runner_keys_what_it_reads',
        model.corpus_inferred_runner_keys_what_it_reads(),
        model.law_inferred_runner_keys_what_it_reads,
      ),
      ...failures('key_complete_can_fail', model.corpus_key_complete_can_fail(), model.law_key_complete_can_fail),
    ]).toEqual([]);
  });
});
