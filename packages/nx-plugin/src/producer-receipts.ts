import { createHash, randomUUID } from 'node:crypto';
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { delimiter, dirname, join, resolve } from 'node:path';
import type { NxJsonConfiguration } from 'nx/src/config/nx-json';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import type { Task, TaskGraph } from 'nx/src/config/task-graph';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api';
import typia from 'typia';
import {
  createWorkspaceNxRequire,
  type EnsureBuiltProducerReceipt,
  type EnsureBuiltProducerTool,
  type EnsureBuiltReceipt,
  parseTargetSelector,
  providerInputProof,
  selectorTaskId,
} from './ensure-built.js';
import {
  captureProducerSource,
  type ProducerSourceWitness,
  producerOutputPaths,
  providerReceipt,
  type ReceiptEvidence,
  receiptNode,
} from './ensure-built-receipt.js';
import { graphInstallationIdentity, movedNodes, type NodeWitness, workspaceSnapshot } from './ensure-built-witness.js';
import { graphInputContractIdentity } from './graph-input-contract.js';

export interface ProducerReceiptsOptions {
  readonly producerReceipts?: { readonly targets: readonly string[] };
}
interface ToolResolution {
  readonly name: string;
  readonly from: string;
  readonly target: string | null;
}
interface Installation {
  readonly nodes: readonly NodeWitness[];
  readonly resolutions: readonly (readonly [string, string, string | null])[];
  readonly localInputs: string | null;
}
interface TargetBasis {
  readonly graph: TaskGraph;
  readonly source: ProducerSourceWitness;
}
interface RunBasis {
  readonly projectGraph: ProjectGraph;
  readonly nxJson: NxJsonConfiguration;
  readonly targets: ReadonlyMap<string, TargetBasis>;
  readonly tools: readonly EnsureBuiltProducerTool[];
  readonly toolNodes: readonly NodeWitness[];
}
type RunFrame = ReceiptEvidence<RunBasis>;
interface PublishedReceipt {
  readonly version: 1;
  readonly disposition: 'published';
  readonly receipt: EnsureBuiltReceipt;
  readonly projectGraph: ProjectGraph;
  readonly taskGraph: TaskGraph;
  readonly nxJson: NxJsonConfiguration;
}
interface RefusedReceipt {
  readonly version: 1;
  readonly disposition: 'refused';
  readonly target: string;
  readonly detail: string;
}
type ProducerRecord = PublishedReceipt | RefusedReceipt;
const runs = new Map<string, RunFrame>();
function sha(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
function configured(options: ProducerReceiptsOptions | undefined): readonly string[] {
  return options?.producerReceipts?.targets ?? [];
}
/**
 * Where a record of `disposition` lives. A refusal has its own file beside the receipt, so a run that could not prove
 * its products never replaces the receipt an earlier run published: readers validate that receipt on its own evidence.
 */
function recordPath(root: string, target: string, disposition: ProducerRecord['disposition']): string {
  const load = createWorkspaceNxRequire(root);
  return join(
    load('nx/src/utils/cache-directory').workspaceDataDirectoryForWorkspace(root),
    `producer-${disposition === 'published' ? 'receipt' : 'refusal'}-${sha(target)}.json`,
  );
}
function publish(root: string, target: string, record: ProducerRecord): ReceiptEvidence<true> {
  try {
    const path = recordPath(root, target, record.disposition);
    mkdirSync(dirname(path), { recursive: true });
    const temporary = `${path}.${randomUUID()}`;
    try {
      writeFileSync(temporary, JSON.stringify(record), { flag: 'wx', mode: 0o600 });
      renameSync(temporary, path);
    } finally {
      rmSync(temporary, { force: true });
    }
    // A receipt answers the refusal of any run before it.
    if (record.disposition === 'published') rmSync(recordPath(root, target, 'refused'), { force: true });
    return { ok: true, value: true };
  } catch (error) {
    if (error instanceof Error)
      return { ok: false, detail: `Cannot publish producer ${record.disposition} record: ${error.message}` };
    throw error;
  }
}
function same(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}
/**
 * Whether a tool node whose ctime alone moved still reads as `tools` recorded it: the same bytes, link text,
 * membership and mode. Bun's global store is shared by every checkout, and linking a package's bin on any of their
 * installs re-chmods the store file to the mode it already has; that moves the ctime of a node these tools witness
 * without changing it, and is no reason to refuse the run.
 */
function recordedTool(tools: readonly EnsureBuiltProducerTool[]): (path: string) => boolean {
  const recorded = new Map(tools.flatMap((tool) => tool.nodes.map((node) => [node.path, node] as const)));
  return (path) => {
    const node = recorded.get(path);
    return node !== undefined && same(receiptNode(path, path, []), node);
  };
}
function missing(paths: readonly string[]): boolean {
  return paths.every((path) => {
    try {
      lstatSync(path);
      return false;
    } catch (error) {
      if (error instanceof Error && 'code' in error && ['ENOENT', 'ENOTDIR'].includes(String(error.code))) return true;
      throw error;
    }
  });
}
function defaultOverrides(task: Task): boolean {
  return Object.entries(task.overrides).every(
    ([name, value]) => name === '__overrides_unparsed__' && Array.isArray(value) && value.length === 0,
  );
}
function targetGraph(root: string, target: string, projectGraph: ProjectGraph): TaskGraph {
  const selector = parseTargetSelector(target);
  if (selector === null) throw new Error(`Invalid declared producer target ${target}`);
  return createWorkspaceNxRequire(root)('nx/src/tasks-runner/create-task-graph').createTaskGraph(
    projectGraph,
    {},
    [selector.project],
    [selector.target],
    selector.configuration,
    {},
    false,
  );
}
/** Tool resolution is captured in producer scope; readers inspect its recorded filesystem, not their own environment. */
function toolsFor(
  root: string,
  nxJson: NxJsonConfiguration,
  projectGraph: ProjectGraph,
  bases: readonly TargetBasis[],
): ReceiptEvidence<{ readonly tools: readonly EnsureBuiltProducerTool[]; readonly nodes: readonly NodeWitness[] }> {
  try {
    const load = createWorkspaceNxRequire(root);
    const data = load('nx/src/utils/cache-directory').workspaceDataDirectoryForWorkspace(root);
    const snapshot = workspaceSnapshot(root, new (load('nx/src/native').WorkspaceContext)(root, data), undefined);
    const local = graphInputContractIdentity(root, nxJson, snapshot);
    // A task's npm external inputs (AllExternalDependencies, and the `npm:` nodes of its project's dependency closure)
    // are Nx's lockfile identities, bound by the receipt's hashDetails and the committed tree's bun.lock. They add no
    // installed closure: resolved from the task's project, a transitive or uninstalled-optional node is unresolvable
    // under Bun's isolated linker, and the installed closure of every lockfile package measured 46 MB of evidence and
    // 10 s of census. The tools that compute the graph and hashes are still bound below.
    let needsCargo = false;
    for (const basis of bases) {
      const proof = providerInputProof(root, nxJson, projectGraph, basis.graph, load);
      if (proof.runtimeSource.kind === 'opaque') return { ok: false, detail: proof.runtimeSource.detail };
      needsCargo ||= Object.values(proof.inputs).some((input) => input.runtime.length > 0);
    }
    let installationMiss = 'unproved installed module/resolution closure';
    const identity = graphInstallationIdentity(
      root,
      nxJson,
      load('nx/src/utils/package-json').readModulePackageJson,
      load('nx/src/native').expandOutputs,
      local ?? { plugins: [], packages: [], identity: '[]' },
      (detail) => {
        installationMiss = detail;
      },
    );
    if (identity === null)
      return { ok: false, detail: `Producer installed module/resolution closure is unproved: ${installationMiss}` };
    const installation = typia.json.isParse<Installation>(identity);
    if (installation === null) return { ok: false, detail: 'Invalid installed module closure evidence' };
    const observed: NodeWitness[] = [];
    const paths = new Set(installation.nodes.map(({ path }) => path));
    const absent = new Set<string>();
    function namespace(path: string): void {
      let stats: ReturnType<typeof lstatSync>;
      try {
        stats = lstatSync(path);
      } catch (error) {
        if (error instanceof Error && 'code' in error && ['ENOENT', 'ENOTDIR'].includes(String(error.code))) {
          absent.add(path);
          return;
        }
        throw error;
      }
      paths.add(path);
      if (stats.isSymbolicLink()) paths.add(realpathSync(path));
      if (stats.isDirectory()) for (const name of readdirSync(path)) paths.add(join(path, name));
    }
    const resolutions: ToolResolution[] = [];
    for (const [from, name, target] of installation.resolutions) {
      resolutions.push({ from, name, target });
      for (const directory of createRequire(join(from, 'package.json')).resolve.paths(name) ?? []) {
        namespace(directory);
        const candidate = join(directory, name);
        if (existsSync(candidate)) namespace(candidate);
        else absent.add(candidate);
      }
    }
    const nodes = [...paths].sort().map((path) => receiptNode(path, path, observed));
    const module: EnsureBuiltProducerTool = {
      kind: 'module',
      name: 'installed-nx-plugin-closure',
      from: root,
      path: root,
      physicalPath: realpathSync(root),
      nodes,
      missing: [...absent].sort(),
      resolutions,
      sha256: sha(nodes),
    };
    const tools: EnsureBuiltProducerTool[] = [module];
    for (const name of ['nx-runtime', 'bun', 'sh', 'git', ...(needsCargo ? ['rustc', 'cargo'] : [])]) {
      const candidates =
        name === 'nx-runtime'
          ? [process.execPath]
          : (process.env.PATH ?? '').split(delimiter).map((directory) => resolve(directory || root, name));
      const skipped: string[] = [];
      let path: string | undefined;
      for (const candidate of candidates) {
        if (
          (existsSync(candidate) && lstatSync(candidate).isFile()) ||
          (existsSync(candidate) && lstatSync(candidate).isSymbolicLink())
        ) {
          path = candidate;
          break;
        }
        skipped.push(candidate);
      }
      if (path === undefined) return { ok: false, detail: `Unbound producer executable ${name}` };
      const physicalPath = realpathSync(path);
      const entries = [...new Set([path, physicalPath])].map((entry) => receiptNode(entry, entry, observed));
      tools.push({
        kind: 'executable',
        name,
        from: root,
        path,
        physicalPath,
        nodes: entries,
        missing: skipped,
        resolutions: [],
        sha256: sha(entries),
      });
    }
    const capture = movedNodes(observed, recordedTool(tools));
    if (capture !== null)
      return { ok: false, detail: `Producer tool/module namespace moved during capture: ${capture}` };
    if (!missing(module.missing))
      return { ok: false, detail: 'Producer tool/module namespace moved during capture: a missing candidate appeared' };
    return { ok: true, value: { tools, nodes: observed } };
  } catch (error) {
    if (error instanceof Error) return { ok: false, detail: `Cannot prove producer tool closure: ${error.message}` };
    throw error;
  }
}
function verifyTools(tools: readonly EnsureBuiltProducerTool[]): ReceiptEvidence<readonly NodeWitness[]> {
  try {
    const observed: NodeWitness[] = [];
    for (const tool of tools) {
      if (realpathSync(tool.path) !== tool.physicalPath || !missing(tool.missing))
        return { ok: false, detail: `Producer tool alias/namespace changed: ${tool.name}` };
      const nodes = tool.nodes.map((node) => receiptNode(node.path, node.path, observed));
      if (!same(nodes, tool.nodes) || sha(nodes) !== tool.sha256)
        return { ok: false, detail: `Producer tool/module bytes changed: ${tool.name}` };
    }
    const moved = movedNodes(observed, recordedTool(tools));
    return moved === null
      ? { ok: true, value: observed }
      : { ok: false, detail: `Producer tool nodes changed during readonly validation: ${moved}` };
  } catch (error) {
    if (error instanceof Error) return { ok: false, detail: `Cannot validate producer tools: ${error.message}` };
    throw error;
  }
}
export function completedProducerTasks(
  graph: TaskGraph,
  results: PostTasksExecutionContext['taskResults'],
): ReceiptEvidence<readonly Task[]> {
  const tasks: Task[] = [];
  for (const planned of Object.values(graph.tasks)) {
    const result = results[planned.id];
    if (
      result === undefined ||
      result.code !== 0 ||
      !['success', 'local-cache', 'local-cache-kept-existing', 'remote-cache'].includes(result.status)
    )
      return { ok: false, detail: `No actual successful completion for ${planned.id}` };
    if (
      result.task.hash === undefined ||
      result.task.hashDetails === undefined ||
      !defaultOverrides(result.task) ||
      !same(result.task.outputs, planned.outputs) ||
      !same(result.task.target, planned.target)
    )
      return { ok: false, detail: `Unbound actual hash/default target/outputs for ${planned.id}` };
    tasks.push(result.task);
  }
  return { ok: true, value: tasks };
}
export function producerEnvironmentIdentity(inputs: EnsureBuiltProducerReceipt['inputs']): string {
  return sha(
    inputs.map(({ taskId, expanded, hashDetails }) => ({
      taskId,
      runtime: expanded.runtime,
      environment: expanded.environment,
      identity: hashDetails,
    })),
  );
}
export async function preTasksExecution(
  options: ProducerReceiptsOptions | undefined,
  context: PreTasksExecutionContext,
): Promise<void> {
  const targets = configured(options);
  if (targets.length === 0) return;
  try {
    const load = createWorkspaceNxRequire(context.workspaceRoot);
    const projectGraph = await load('nx/src/project-graph/project-graph').createProjectGraphAsync();
    const bases = new Map<string, TargetBasis>();
    for (const target of targets) {
      const graph = targetGraph(context.workspaceRoot, target, projectGraph);
      const outputs = producerOutputPaths(
        context.workspaceRoot,
        Object.values(graph.tasks),
        load('nx/src/native').expandOutputs,
      );
      const source = captureProducerSource(context.workspaceRoot, outputs);
      if (!source.ok) {
        runs.set(context.id, source);
        return;
      }
      bases.set(target, { graph, source: source.value });
    }
    const tools = toolsFor(context.workspaceRoot, context.nxJsonConfiguration, projectGraph, [...bases.values()]);
    runs.set(
      context.id,
      tools.ok
        ? {
            ok: true,
            value: {
              projectGraph,
              nxJson: context.nxJsonConfiguration,
              targets: bases,
              tools: tools.value.tools,
              toolNodes: tools.value.nodes,
            },
          }
        : tools,
    );
  } catch (error) {
    if (error instanceof Error) runs.set(context.id, { ok: false, detail: error.message });
    else throw error;
  }
}
export async function postTasksExecution(
  options: ProducerReceiptsOptions | undefined,
  context: PostTasksExecutionContext,
): Promise<void> {
  const frame = runs.get(context.id);
  runs.delete(context.id);
  for (const target of configured(options)) {
    const selected = parseTargetSelector(target);
    if (selected === null) continue;
    const targetBasis = frame?.ok === true ? frame.value.targets.get(target) : undefined;
    const plannedRoot =
      targetBasis === undefined
        ? undefined
        : Object.values(targetBasis.graph.tasks).find(
            (task) => task.target.project === selected.project && task.target.target === selected.target,
          );
    const rootResult =
      plannedRoot === undefined
        ? Object.values(context.taskResults).find(
            ({ task }) =>
              task.target.project === selected.project &&
              task.target.target === selected.target &&
              (selected.configuration === undefined || task.target.configuration === selected.configuration),
          )
        : context.taskResults[plannedRoot.id];
    if (rootResult === undefined) continue;
    const refuse = (detail: string) => {
      const saved = publish(context.workspaceRoot, target, { version: 1, disposition: 'refused', target, detail });
      if (!saved.ok) process.stderr.write(`producerReceipts: ${target}: ${saved.detail}\n`);
      process.stderr.write(`producerReceipts: ${target}: ${detail}\n`);
    };
    if (frame === undefined || !frame.ok) {
      refuse(frame === undefined ? 'No bound producer pre-execution frame' : frame.detail);
      continue;
    }
    const basis = frame.value.targets.get(target);
    if (basis === undefined) {
      refuse('No declared target pre-execution basis');
      continue;
    }
    const completed = completedProducerTasks(basis.graph, context.taskResults);
    if (!completed.ok) {
      refuse(completed.detail);
      continue;
    }
    const tasks = completed.value;
    let detail: string | undefined;
    const execution = movedNodes(frame.value.toolNodes, recordedTool(frame.value.tools));
    if (execution !== null) {
      refuse(`Producer tool/module nodes changed across actual execution: ${execution}`);
      continue;
    }
    const load = createWorkspaceNxRequire(context.workspaceRoot);
    const graph: TaskGraph = { ...basis.graph, tasks: Object.fromEntries(tasks.map((task) => [task.id, task])) };
    const proof = providerInputProof(
      context.workspaceRoot,
      context.nxJsonConfiguration,
      frame.value.projectGraph,
      graph,
      load,
    );
    if (proof.runtimeSource.kind === 'opaque') {
      refuse(proof.runtimeSource.detail);
      continue;
    }
    const files = new Map(basis.source.files.map((file) => [file.path, file]));
    const inputs: EnsureBuiltProducerReceipt['inputs'][number][] = [];
    for (const task of tasks) {
      const expanded = proof.inputs[task.id];
      const hashDetails = task.hashDetails;
      if (expanded === undefined || hashDetails === undefined || expanded.files.some((path) => !files.has(path))) {
        detail = `Actual FILE/hash evidence is unbound for ${task.id}`;
        break;
      }
      const declared =
        frame.value.projectGraph.nodes[task.target.project]?.data.targets?.[task.target.target]?.inputs ?? [];
      inputs.push({
        taskId: task.id,
        overrides: task.overrides,
        declared,
        expanded,
        hashDetails,
        files: expanded.files.map((path) => {
          const file = files.get(path);
          if (file === undefined) throw new Error(`Lost HEAD-bound input ${path}`);
          return file;
        }),
      });
    }
    if (detail !== undefined) {
      refuse(detail);
      continue;
    }
    const producer: Omit<EnsureBuiltProducerReceipt, 'outputSha256'> = {
      version: 1,
      target,
      argv: [...context.argv],
      trackedTreeClean: true,
      environmentIdentity: producerEnvironmentIdentity(inputs),
      inputs,
      tools: frame.value.tools,
      wallTimeMs: context.endTime - context.startTime,
    };
    if (!Number.isFinite(producer.wallTimeMs) || producer.wallTimeMs < 0) {
      refuse('Invalid actual Nx execution timing');
      continue;
    }
    const result = providerReceipt(
      context.workspaceRoot,
      selectorTaskId(selected),
      tasks,
      proof,
      load('nx/src/native').expandOutputs,
      basis.source,
      producer,
    );
    if (result.disposition !== 'hit') {
      refuse(result.reason.detail);
      continue;
    }
    const census = movedNodes(frame.value.toolNodes, recordedTool(frame.value.tools));
    if (census !== null) {
      refuse(`Producer tools changed during post-execution product census: ${census}`);
      continue;
    }
    const publication = publish(context.workspaceRoot, target, {
      version: 1,
      disposition: 'published',
      receipt: result.receipt,
      projectGraph: frame.value.projectGraph,
      taskGraph: graph,
      nxJson: frame.value.nxJson,
    });
    if (!publication.ok) process.stderr.write(`producerReceipts: ${target}: ${publication.detail}\n`);
  }
}
/**
 * Readonly consumers accept actual producer completion evidence, never their own environment/cache rows. A receipt
 * that does not validate is a miss whose detail also carries the refusal of the producer's latest run, when it left one.
 */
export function readProducerReceipt(root: string, target: string): ReceiptEvidence<EnsureBuiltReceipt> {
  const published = validatedReceipt(root, target);
  if (published.ok) return published;
  const refusal = latestRefusal(root, target);
  return refusal === null
    ? published
    : { ok: false, detail: `${published.detail}; the producer's latest run refused: ${refusal}` };
}
/** The detail of the refusal a producer run left after the last receipt, or null when none did. */
function latestRefusal(root: string, target: string): string | null {
  try {
    const record = typia.json.isParse<RefusedReceipt>(readFileSync(recordPath(root, target, 'refused'), 'utf8'));
    return record === null || record.version !== 1 ? 'corrupt producer refusal record' : record.detail;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    if (error instanceof Error) return `unreadable producer refusal record: ${error.message}`;
    throw error;
  }
}
function validatedReceipt(root: string, target: string): ReceiptEvidence<EnsureBuiltReceipt> {
  try {
    const record = typia.json.isParse<PublishedReceipt>(readFileSync(recordPath(root, target, 'published'), 'utf8'));
    if (record === null || record.version !== 1)
      return { ok: false, detail: 'Missing/corrupt producer completion contract' };
    const receipt = record.receipt;
    if (
      receipt.workspaceRoot !== realpathSync(root) ||
      receipt.producer.version !== 1 ||
      receipt.producer.target !== target ||
      !receipt.producer.trackedTreeClean
    )
      return { ok: false, detail: 'Producer receipt target/provider identity differs' };
    if (receipt.producer.environmentIdentity !== producerEnvironmentIdentity(receipt.producer.inputs))
      return { ok: false, detail: 'Corrupt declared producer environment identity' };
    if (
      receipt.producer.inputs.length !== receipt.tasks.length ||
      new Set(receipt.producer.inputs.map(({ taskId }) => taskId)).size !== receipt.tasks.length
    )
      return { ok: false, detail: 'Producer input/task membership differs' };
    const expected = targetGraph(root, target, record.projectGraph);
    if (
      !same(Object.keys(expected.tasks).sort(), Object.keys(record.taskGraph.tasks).sort()) ||
      Object.values(record.taskGraph.tasks).some(
        (task) =>
          !defaultOverrides(task) ||
          !same(task.target, expected.tasks[task.id]?.target) ||
          !same(task.outputs, expected.tasks[task.id]?.outputs),
      )
    )
      return { ok: false, detail: 'Producer target/default overrides or configuration differ' };
    const source = captureProducerSource(
      root,
      receipt.tasks.flatMap(({ outputs }) => outputs.map(({ path }) => path)),
    );
    if (!source.ok) return source;
    if (source.value.headTree !== receipt.headTree)
      return { ok: false, detail: 'Current committed tree differs from producer basis tree' };
    const tools = verifyTools(receipt.producer.tools);
    if (!tools.ok) return tools;
    const load = createWorkspaceNxRequire(root);
    const proof = providerInputProof(root, record.nxJson, record.projectGraph, record.taskGraph, load);
    const files = new Map(source.value.files.map((file) => [file.path, file]));
    for (const task of receipt.tasks) {
      const input = receipt.producer.inputs.find(({ taskId }) => taskId === task.id);
      if (
        input === undefined ||
        !same(input.expanded, proof.inputs[task.id]) ||
        !same(input.hashDetails, record.taskGraph.tasks[task.id]?.hashDetails) ||
        !same(input.overrides, record.taskGraph.tasks[task.id]?.overrides) ||
        input.files.some((file) => !same(files.get(file.path), file))
      )
        return { ok: false, detail: `Declared FILE/input identity differs for ${task.id}` };
    }
    const census = providerReceipt(
      root,
      target,
      Object.values(record.taskGraph.tasks),
      proof,
      load('nx/src/native').expandOutputs,
      source.value,
      receipt.producer,
    );
    if (
      census.disposition !== 'hit' ||
      !same(census.receipt.tasks, receipt.tasks) ||
      census.receipt.producer.outputSha256 !== receipt.producer.outputSha256
    )
      return {
        ok: false,
        detail:
          census.disposition === 'hit'
            ? 'Public product bytes differ from actual producer receipt'
            : census.reason.detail,
      };
    const moved = movedNodes(tools.value, recordedTool(receipt.producer.tools));
    if (moved !== null)
      return { ok: false, detail: `Producer tool namespace changed during independent product validation: ${moved}` };
    return { ok: true, value: receipt };
  } catch (error) {
    if (error instanceof Error)
      return { ok: false, detail: `Cannot validate producer completion receipt: ${error.message}` };
    throw error;
  }
}
