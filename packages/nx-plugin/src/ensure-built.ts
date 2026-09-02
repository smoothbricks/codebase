import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join, resolve } from 'node:path';

import type { NxJsonConfiguration } from 'nx/src/config/nx-json';
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { Task } from 'nx/src/config/task-graph';
import type { TaskResults } from 'nx/src/tasks-runner/life-cycle';
import type { NxArgs } from 'nx/src/utils/command-line-utils';

/**
 * A `project:target` or `project:target:configuration` selector.
 *
 * Configuration belongs in the selector rather than in a separate option
 * because it is part of the cache key: `app:build` and `app:build:production`
 * are two different tasks with two different hashes.
 */
export interface TargetSelector {
  readonly project: string;
  readonly target: string;
  readonly configuration: string | undefined;
}

/**
 * Why the target could not be replayed from what is already on disk.
 *
 * Every variant names the task that ended the probe: in a dependency graph
 * "something changed" is unactionable, and the useful question is always which
 * of the twelve dependencies moved.
 */
export type MissReason =
  | { readonly kind: 'uncacheable'; readonly taskId: string }
  | { readonly kind: 'unhashable'; readonly taskId: string }
  | { readonly kind: 'not-cached'; readonly taskId: string }
  | { readonly kind: 'cached-failure'; readonly taskId: string; readonly code: number }
  | { readonly kind: 'stale-outputs'; readonly taskId: string }
  | { readonly kind: 'no-daemon'; readonly taskId: string };

/**
 * `hit` means nothing ran: every task in the graph was a recorded success and
 * every recorded output is still byte-identical on disk, so the artifacts are
 * what a fresh run would produce.
 *
 * `built` means the work was handed to Nx — which may have restored outputs
 * from the local cache rather than recompiled anything. The line `hit` draws is
 * "did this process have to do work", which is what a CLI wrapper needs in
 * order to decide whether to stay quiet.
 *
 * `failed` is an operational outcome, not an exception: the target ran and did
 * not succeed. A caller re-raises `signal` when there is one and exits with
 * `exitCode` otherwise, so a Ctrl-C during a build stays a Ctrl-C.
 */
export type EnsureBuiltResult =
  | { readonly disposition: 'hit' }
  | { readonly disposition: 'built'; readonly reason: MissReason }
  | {
      readonly disposition: 'failed';
      readonly reason: MissReason;
      readonly exitCode: number;
      readonly signal: NodeJS.Signals | null;
    };

export interface EnsureBuiltOptions {
  /** `project:target` or `project:target:configuration`. */
  readonly target: string;
  /**
   * Nx workspace root. Required rather than discovered: a wrapper script knows
   * its own checkout, and inferring from the caller's directory would silently
   * bind whichever workspace they happen to be standing in.
   */
  readonly cwd: string;
  /**
   * How the run that happens on a miss reports progress. `stream` is the
   * default and the only style offered: this facility fronts multi-minute
   * compiles, and Nx's buffered styles emit nothing until a task finishes, so a
   * silent terminal would read as a hang.
   */
  readonly onMiss?: 'stream';
}

/** A full hit carries no payload, so it is the same value every time. */
const HIT: EnsureBuiltResult = Object.freeze({ disposition: 'hit' });

const NO_TASK_RESULTS: TaskResults = Object.freeze({});

export function parseTargetSelector(spec: string): TargetSelector | null {
  const parts = spec.split(':');
  if (parts.length < 2 || parts.length > 3) {
    return null;
  }
  for (const part of parts) {
    if (part.length === 0) {
      return null;
    }
  }
  return { project: parts[0], target: parts[1], configuration: parts[2] };
}

export function describeMiss(reason: MissReason): string {
  switch (reason.kind) {
    case 'uncacheable':
      return `${reason.taskId} is not cacheable, so it must run`;
    case 'unhashable':
      return `${reason.taskId} did not produce a task hash`;
    case 'not-cached':
      return `${reason.taskId} has no cached result for its current inputs`;
    case 'cached-failure':
      return `${reason.taskId} last failed with exit code ${reason.code}`;
    case 'stale-outputs':
      return `${reason.taskId} outputs are missing or modified on disk`;
    case 'no-daemon':
      return `the Nx daemon is disabled, so ${reason.taskId} cannot be checked without running it`;
  }
}

/**
 * First reason the task set cannot be replayed, considering only recorded
 * results — no filesystem access. `null` means every task is a recorded
 * success, and the on-disk check is worth paying for.
 *
 * A cached *failure* counts as a miss even though Nx replays one when
 * `NX_CACHE_FAILURES` is set: this facility exists to hand the process over to
 * a binary, and replaying a failed build would hand over a stale one.
 */
export function firstCacheMiss(
  tasks: readonly Task[],
  cachedCodeByHash: ReadonlyMap<string, number>,
): MissReason | null {
  for (const task of tasks) {
    if (!task.cache) {
      return { kind: 'uncacheable', taskId: task.id };
    }
    // A custom hasher is outside Nx's type guarantees and may return an empty
    // value. Without a hash there is no cache key to query.
    if (!task.hash) {
      return { kind: 'unhashable', taskId: task.id };
    }
    const code = cachedCodeByHash.get(task.hash);
    if (code === undefined) {
      return { kind: 'not-cached', taskId: task.id };
    }
    if (code !== 0) {
      return { kind: 'cached-failure', taskId: task.id, code };
    }
  }
  return null;
}

/**
 * First task whose recorded outputs are no longer the bytes on disk.
 *
 * `matches[index]` is the daemon's verdict for `tasks[index]`. A short or
 * ragged array counts as stale rather than as a hit: a missing verdict is not a
 * positive one, and being wrong here means exec'ing a binary that was deleted.
 */
export function firstStaleOutputs(tasks: readonly Task[], matches: readonly boolean[]): MissReason | null {
  for (let index = 0; index < tasks.length; index += 1) {
    if (matches[index] !== true) {
      return { kind: 'stale-outputs', taskId: tasks[index].id };
    }
  }
  return null;
}

/**
 * Make sure an Nx target's outputs are on disk, doing as little as possible
 * when they already are.
 *
 * "Already built" is the hot path, because a checkout-local CLI wrapper pays it
 * on every invocation. So it never spawns the `nx` CLI: the project graph, the
 * task hashes and the on-disk output verification are daemon round-trips, the
 * cache lookup is a local SQLite read, and Nx's task runner is only invoked
 * once something is known to need running.
 *
 * With the daemon disabled there is no probe to make — hashing would have to
 * build the project graph in this process, and no service holds the recorded
 * output hashes to check the working tree against — so the target is handed to
 * the workspace's own `nx` CLI unconditionally.
 */
export async function ensureBuilt(options: EnsureBuiltOptions): Promise<EnsureBuiltResult> {
  const selector = parseTargetSelector(options.target);
  if (!selector) {
    throw new Error(`ensureBuilt: '${options.target}' is not a project:target[:configuration] selector`);
  }
  // Resolved before the chdir below, so a relative `cwd` still means what the
  // caller meant.
  const workspaceRoot = resolve(options.cwd);
  const callerCwd = process.cwd();
  const callerEnv = { ...process.env };
  try {
    // `cd <root> && nx run ...` is the invocation whose hashes are in the
    // cache. Everything below — plugin hooks, `runtime` inputs, and the hasher,
    // which keys against `process.cwd()` — has to see the same directory or the
    // probe keys differently from the CLI and never hits.
    process.chdir(workspaceRoot);
    return await ensureBuiltInWorkspace(workspaceRoot, selector, options.onMiss ?? 'stream');
  } finally {
    // Nx configures itself through the environment and this function runs
    // in-process, so without this the caller — and anything it goes on to exec
    // — would inherit NX_WORKSPACE_ROOT_PATH, the stream/prefix flags, and
    // whatever a plugin's preTasksExecution hook injected. The wrapper this
    // replaces got that isolation for free by running Nx in a child.
    process.chdir(callerCwd);
    for (const key of Object.keys(process.env)) {
      if (!(key in callerEnv)) {
        delete process.env[key];
      }
    }
    for (const [key, value] of Object.entries(callerEnv)) {
      if (process.env[key] !== value) {
        process.env[key] = value;
      }
    }
  }
}

async function ensureBuiltInWorkspace(
  workspaceRoot: string,
  selector: TargetSelector,
  onMiss: 'stream',
): Promise<EnsureBuiltResult> {
  // Every Nx import in this file is dynamic, and every one of them runs after
  // bindWorkspaceRoot, because Nx resolves its workspace root once at module
  // load; see bindWorkspaceRoot. Repeat imports of the same specifier are
  // module-cache lookups, so each function names exactly what it needs.
  await bindWorkspaceRoot(workspaceRoot);
  const { daemonClient } = await import('nx/src/daemon/client/client');
  if (!daemonClient.enabled()) {
    return runViaCli(workspaceRoot, selector);
  }

  const [{ readNxJson }, { splitArgsIntoNxArgsAndOverrides }, { setEnvVarsBasedOnArgs }, { hashArray }, hooks] =
    await Promise.all([
      import('nx/src/config/nx-json'),
      import('nx/src/utils/command-line-utils'),
      import('nx/src/tasks-runner/run-command'),
      import('nx/src/native'),
      import('nx/src/project-graph/plugins/tasks-execution-hooks'),
    ]);

  const nxJson = readNxJson();
  // Reproduce `nx run <selector> --outputStyle=<style>` exactly. Task hashes
  // are derived from these arguments, so anything hand-rolled here instead of
  // routed through Nx's own argument normalization would key the cache
  // differently from the CLI and turn every probe into a miss.
  const { nxArgs, overrides } = splitArgsIntoNxArgsAndOverrides(
    { targets: [selector.target], configuration: selector.configuration, outputStyle: onMiss },
    'run-one',
    { printWarnings: false },
    nxJson,
  );
  const loadDotEnvFiles = process.env.NX_LOAD_DOT_ENV_FILES !== 'false';
  // Sets NX_LOAD_DOT_ENV_FILES and the stream/prefix flags, which the hasher
  // reads through each task's environment. It has to happen before hashing or
  // the probe keys differently from the CLI.
  setEnvVarsBasedOnArgs(nxArgs, loadDotEnvFiles);

  const runId = hashArray([...process.argv, Date.now().toString()]);
  const startTime = Date.now();
  // Plugin `preTasksExecution` hooks inject environment variables, and declared
  // `env` inputs hash against them. Skipping the hook would not merely cost
  // hits: the run below would then execute with a different environment than
  // the CLI does, record a hash the CLI never looks up, and thrash the cache.
  await hooks.runPreTasksExecution({
    id: runId,
    workspaceRoot,
    nxJsonConfiguration: nxJson,
    argv: process.argv,
  });

  performance.mark('ensureBuilt:probe:start');
  const reason = await probe(nxJson, nxArgs, overrides, selector);
  performance.measure('ensureBuilt:probe', 'ensureBuilt:probe:start');
  const outcome =
    reason === null
      ? { result: HIT, taskResults: NO_TASK_RESULTS }
      : await runTarget(nxJson, nxArgs, overrides, selector, reason);
  await hooks.runPostTasksExecution({
    id: runId,
    taskResults: outcome.taskResults,
    workspaceRoot,
    nxJsonConfiguration: nxJson,
    argv: process.argv,
    startTime,
    endTime: Date.now(),
  });
  return outcome.result;
}

/**
 * Nx computes `workspaceRoot` once, at module load, from
 * `NX_WORKSPACE_ROOT_PATH` or the current directory — and its daemon client is
 * a module-level singleton that reads `nx.json` from that root. So the root has
 * to be bound before the first Nx module loads, which is why this module
 * imports Nx dynamically throughout. A static import at the top of the file
 * would bind the caller's launch directory instead.
 */
async function bindWorkspaceRoot(workspaceRoot: string): Promise<void> {
  process.env.NX_WORKSPACE_ROOT_PATH = workspaceRoot;
  if (process.env.NX_PERF_LOGGING === 'true') {
    // Installs Nx's PerformanceObserver, which reports every `performance
    // .measure` this module and Nx itself record. Importing it unconditionally
    // would add an observer to hot paths that never want one.
    await import('nx/src/utils/perf-logging');
  }
  const { workspaceRoot: boundRoot } = await import('nx/src/utils/workspace-root');
  if (boundRoot !== workspaceRoot) {
    // One process cannot serve two workspaces: the constant and the daemon
    // client singleton are already bound. Say so rather than silently probing
    // the wrong checkout.
    throw new Error(
      `ensureBuilt: Nx is already bound to workspace root ${boundRoot}, cannot switch to ${workspaceRoot}`,
    );
  }
}

/** `null` when the target is already built; otherwise the first reason it is not. */
async function probe(
  nxJson: NxJsonConfiguration,
  nxArgs: NxArgs,
  overrides: Record<string, unknown>,
  selector: TargetSelector,
): Promise<MissReason | null> {
  const [
    { daemonClient },
    { createTaskGraph },
    { DaemonBasedTaskHasher },
    { getTaskDetails, hashTasks },
    { getTaskSpecificEnv },
    { getCache },
    { getRunnerOptions },
  ] = await Promise.all([
    import('nx/src/daemon/client/client'),
    import('nx/src/tasks-runner/create-task-graph'),
    import('nx/src/hasher/task-hasher'),
    import('nx/src/hasher/hash-task'),
    import('nx/src/tasks-runner/task-env'),
    import('nx/src/tasks-runner/cache'),
    import('nx/src/tasks-runner/run-command'),
  ]);

  performance.mark('ensureBuilt:graph:start');
  const { projectGraph } = await daemonClient.getProjectGraphAndSourceMaps();
  requireProject(projectGraph, selector.project);
  const taskGraph = createTaskGraph(
    projectGraph,
    {},
    [selector.project],
    [selector.target],
    selector.configuration,
    overrides,
    false,
  );
  const tasks = Object.values(taskGraph.tasks);
  performance.measure('ensureBuilt:graph', 'ensureBuilt:graph:start');

  // `isCloudDefault: false` because these options are only read here by the
  // hasher, which looks at `selectivelyHashTsConfig`; the cloud credentials the
  // flag would add belong to the cloud runner, and the run path calls Nx's own
  // `getRunner` to obtain them.
  const runnerOptions = getRunnerOptions('default', nxJson, nxArgs, false);
  const hasher = new DaemonBasedTaskHasher(daemonClient, runnerOptions);
  performance.mark('ensureBuilt:hash:start');
  // Every task, each against its own environment — per-project and per-target
  // `.env` files and custom hashers that read env participate in the hash, so
  // one shared environment would compute keys the CLI never wrote.
  //
  // `hashTasksThatDoNotDependOnOutputsOfOtherTasks` is the wrong helper here
  // even though it is what Nx's runner warms up with: it deliberately skips
  // tasks whose inputs include another task's outputs, leaving them unhashed
  // for the orchestrator to hash once their dependencies finish. In a probe,
  // "already built" means those outputs are already final, so hashing them now
  // is both possible and exactly what the cache was keyed on. Skipping them
  // instead would make every graph that uses `dependentTasksOutputFiles` — the
  // normal shape for a multi-package build — permanently miss.
  const perTaskEnvs: Record<string, NodeJS.ProcessEnv> = {};
  for (const task of tasks) {
    perTaskEnvs[task.id] = getTaskSpecificEnv(task, projectGraph);
  }
  await hashTasks(hasher, projectGraph, taskGraph, perTaskEnvs, getTaskDetails(), tasks);
  performance.measure('ensureBuilt:hash', 'ensureBuilt:hash:start');

  // Nx's own factory, but deliberately never `init()`ed: that is what makes
  // this a local-only, side-effect-free question. `init()` attaches the remote
  // cache — a network round-trip, and a download is precisely the work this
  // probe exists to detect — and asserts that the cache directory matches the
  // database. Neither matters here: a hit is only declared when the outputs on
  // disk already match, so the cache directory is never read.
  performance.mark('ensureBuilt:cache:start');
  const cache = getCache(runnerOptions);
  const cachedResults = await cache.getBatch(tasks.filter((task) => task.cache && task.hash));
  const cachedCodeByHash = new Map<string, number>();
  for (const [hash, result] of cachedResults) {
    cachedCodeByHash.set(hash, result.code);
  }
  const cacheMiss = firstCacheMiss(tasks, cachedCodeByHash);
  performance.measure('ensureBuilt:cache', 'ensureBuilt:cache:start');
  if (cacheMiss) {
    return cacheMiss;
  }

  // A cache record only proves the task once succeeded with these inputs. The
  // artifact this facility is about — the binary that is about to be exec'd —
  // lives in the working tree, where anything may have deleted or rewritten it
  // since. Only the daemon can settle that, because it holds the recorded
  // output hashes.
  const withOutputs: Task[] = [];
  const outputEntries: { outputs: string[]; hash: string }[] = [];
  for (const task of tasks) {
    const hash = task.hash;
    if (task.outputs.length === 0 || hash === undefined) {
      continue;
    }
    withOutputs.push(task);
    outputEntries.push({ outputs: task.outputs, hash });
  }
  performance.mark('ensureBuilt:outputs:start');
  const matches = await daemonClient.outputsHashesMatchBatch(outputEntries);
  performance.measure('ensureBuilt:outputs', 'ensureBuilt:outputs:start');
  return firstStaleOutputs(withOutputs, matches);
}

interface CompletedRun {
  readonly result: EnsureBuiltResult;
  readonly taskResults: TaskResults;
}

async function runTarget(
  nxJson: NxJsonConfiguration,
  nxArgs: NxArgs,
  overrides: Record<string, unknown>,
  selector: TargetSelector,
  reason: MissReason,
): Promise<CompletedRun> {
  const [{ runCommandForTasks }, { createProjectGraphAsync }, { signalToCode }] = await Promise.all([
    import('nx/src/tasks-runner/run-command'),
    import('nx/src/project-graph/project-graph'),
    import('nx/src/utils/exit-codes'),
  ]);
  // `runCommandForTasks` derives its own task graph and hashes, so the probe's
  // work is not reusable. That duplication is one daemon round-trip on a path
  // that is about to run a build.
  const projectGraph = await createProjectGraphAsync();
  const { taskResults, completed } = await runCommandForTasks(
    [requireProject(projectGraph, selector.project)],
    projectGraph,
    { nxJson },
    nxArgs,
    overrides,
    selector.project,
    {},
    { excludeTaskDependencies: false, loadDotEnvFiles: process.env.NX_LOAD_DOT_ENV_FILES !== 'false' },
  );
  if (!completed) {
    // Nx's own encoding of an interrupted run: `runCommand` reports
    // `signalToCode('SIGINT')` here. The runner does not surface which signal
    // actually arrived, so there is none to forward — only the code Nx's CLI
    // would have exited with.
    return {
      result: { disposition: 'failed', reason, exitCode: signalToCode('SIGINT'), signal: null },
      taskResults,
    };
  }
  for (const taskResult of Object.values(taskResults)) {
    if (taskResult.status === 'failure' || taskResult.status === 'skipped') {
      return { result: { disposition: 'failed', reason, exitCode: 1, signal: null }, taskResults };
    }
  }
  return { result: { disposition: 'built', reason }, taskResults };
}

/**
 * The daemon-disabled path: hand the target to the workspace's own `nx`, with
 * stdio inherited.
 *
 * Inheriting rather than piping is the point. The wrapper this facility
 * replaces piped Nx's output so it could read a cache marker out of the log and
 * stay silent on a hit — a text heuristic over ANSI-coloured, format-unstable
 * output. Without a daemon there is nothing to be silent about: no cheap probe
 * exists, so the run is unconditional, the output is the run's own, and the
 * child's exit status is forwarded verbatim.
 */
async function runViaCli(workspaceRoot: string, selector: TargetSelector): Promise<EnsureBuiltResult> {
  const nxCli = join(workspaceRoot, 'node_modules', '.bin', 'nx');
  if (!existsSync(nxCli)) {
    throw new Error(`ensureBuilt: the Nx daemon is disabled and ${nxCli} does not exist`);
  }
  const reason: MissReason = { kind: 'no-daemon', taskId: `${selector.project}:${selector.target}` };
  const targetSpec = selector.configuration
    ? `${selector.project}:${selector.target}:${selector.configuration}`
    : `${selector.project}:${selector.target}`;
  const { signalToCode } = await import('nx/src/utils/exit-codes');
  const child = spawn(nxCli, ['run', targetSpec, '--outputStyle=stream'], {
    cwd: workspaceRoot,
    stdio: 'inherit',
  });
  // `Promise.withResolvers` would read better but needs lib es2024; this
  // package inherits lib es2022 from tsconfig.base.json.
  const exit = await new Promise<ChildExit>((settle, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => settle({ code, signal }));
  });
  return cliExitOutcome(reason, exit, signalToCode);
}

export interface ChildExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

/**
 * How a child `nx`'s exit becomes a result.
 *
 * A signal is reported as a signal, not flattened into a code, so the caller
 * can re-raise it: a build stopped by Ctrl-C should leave the wrapper looking
 * killed by Ctrl-C to whatever is watching, not merely unsuccessful. The
 * numeric code comes from Nx's own `signalToCode` so it matches what `nx run`
 * would have exited with.
 */
export function cliExitOutcome(
  reason: MissReason,
  exit: ChildExit,
  signalToCode: (signal: NodeJS.Signals | null) => number,
): EnsureBuiltResult {
  if (exit.signal !== null) {
    return { disposition: 'failed', reason, exitCode: signalToCode(exit.signal), signal: exit.signal };
  }
  if (exit.code !== 0) {
    return { disposition: 'failed', reason, exitCode: exit.code ?? 1, signal: null };
  }
  return { disposition: 'built', reason };
}

function requireProject(projectGraph: ProjectGraph, project: string): ProjectGraphProjectNode {
  const node = projectGraph.nodes[project];
  if (!node) {
    throw new Error(`ensureBuilt: no project named '${project}' in this workspace`);
  }
  return node;
}
