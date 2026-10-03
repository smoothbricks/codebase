import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import type { BigIntStats } from 'node:fs';
import { closeSync, existsSync, lstatSync, openSync, readdirSync, readlinkSync, readSync, realpathSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, isAbsolute, join, resolve } from 'node:path';

import type * as NxConfiguration from 'nx/src/config/nx-json';
import type { NxJsonConfiguration } from 'nx/src/config/nx-json';
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { Task, TaskGraph } from 'nx/src/config/task-graph';
import type * as NxDaemonClient from 'nx/src/daemon/client/client';
import type * as NxHashTask from 'nx/src/hasher/hash-task';
import type * as NxTaskHasher from 'nx/src/hasher/task-hasher';
import type * as NxNative from 'nx/src/native';
import type * as NxExecutionHooks from 'nx/src/project-graph/plugins/tasks-execution-hooks';
import type * as NxCache from 'nx/src/tasks-runner/cache';
import type * as NxCreateTaskGraph from 'nx/src/tasks-runner/create-task-graph';
import type { TaskResults } from 'nx/src/tasks-runner/life-cycle';
import type * as NxRunCommand from 'nx/src/tasks-runner/run-command';
import type * as NxTaskEnv from 'nx/src/tasks-runner/task-env';
import type * as NxTasksRunnerUtils from 'nx/src/tasks-runner/utils';
import type * as NxCacheDirectory from 'nx/src/utils/cache-directory';
import type * as NxCommandLineUtils from 'nx/src/utils/command-line-utils';
import type { NxArgs } from 'nx/src/utils/command-line-utils';
import type * as NxPerfLogging from 'nx/src/utils/perf-logging';
import type * as NxWorkspaceRoot from 'nx/src/utils/workspace-root';

interface NxRuntimeModules {
  readonly 'nx/src/config/nx-json': typeof NxConfiguration;
  readonly 'nx/src/daemon/client/client': typeof NxDaemonClient;
  readonly 'nx/src/hasher/hash-task': typeof NxHashTask;
  readonly 'nx/src/hasher/task-hasher': typeof NxTaskHasher;
  readonly 'nx/src/native': typeof NxNative;
  readonly 'nx/src/project-graph/plugins/tasks-execution-hooks': typeof NxExecutionHooks;
  readonly 'nx/src/tasks-runner/cache': typeof NxCache;
  readonly 'nx/src/tasks-runner/create-task-graph': typeof NxCreateTaskGraph;
  readonly 'nx/src/tasks-runner/run-command': typeof NxRunCommand;
  readonly 'nx/src/tasks-runner/task-env': typeof NxTaskEnv;
  readonly 'nx/src/tasks-runner/utils': typeof NxTasksRunnerUtils;
  readonly 'nx/src/utils/cache-directory': typeof NxCacheDirectory;
  readonly 'nx/src/utils/command-line-utils': typeof NxCommandLineUtils;
  readonly 'nx/src/utils/perf-logging': typeof NxPerfLogging;
  readonly 'nx/src/utils/workspace-root': typeof NxWorkspaceRoot;
}

type WorkspaceNxRequire = <Specifier extends keyof NxRuntimeModules>(
  specifier: Specifier,
) => NxRuntimeModules[Specifier];

function createWorkspaceNxRequire(workspaceRoot: string): WorkspaceNxRequire {
  const requireFromWorkspace = createRequire(join(workspaceRoot, 'package.json'));
  return function requireNx<Specifier extends keyof NxRuntimeModules>(
    specifier: Specifier,
  ): NxRuntimeModules[Specifier] {
    return requireFromWorkspace(specifier);
  };
}

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
  | { readonly kind: 'stale-inputs'; readonly taskId: string }
  | { readonly kind: 'cache-disabled'; readonly taskId: string }
  | { readonly kind: 'no-daemon'; readonly taskId: string };

/**
 * `hit` means nothing ran: every task in the graph was a recorded success and
 * every recorded output is still byte-identical on disk, so the artifacts are
 * what a fresh run would produce.
 *
 * `built` means the work was handed to Nx — which may have restored outputs
 * from the local cache rather than recompiled anything. The line `hit` draws is
 * "did this process have to do work", which is what a CLI wrapper needs in
 * order to decide whether to stay quiet. A run writes only to stderr: stdout
 * belongs to whatever the caller goes on to run.
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
    case 'stale-inputs':
      return `${reason.taskId} inputs changed since the Nx daemon last looked`;
    case 'cache-disabled':
      return `the Nx cache is disabled, so ${reason.taskId} must run`;
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
 * The entries the daemon does not vouch for.
 *
 * `verdicts[index]` is the daemon's verdict for `entries[index]`. A short or
 * ragged array leaves the remainder unvouched rather than cleared: a missing
 * verdict is not a positive one, and being wrong here means exec'ing a binary
 * that was deleted.
 */
export function unvouched<Entry>(entries: readonly Entry[], verdicts: readonly boolean[]): Entry[] {
  return entries.filter((_, index) => verdicts[index] !== true);
}

/**
 * Make sure an Nx target's outputs are on disk, doing as little as possible
 * when they already are.
 *
 * "Already built" is the hot path, because a checkout-local CLI wrapper pays it
 * on every invocation. So it never spawns the `nx` CLI: a native filesystem
 * snapshot refreshes the daemon's inputs, the task hashes and on-disk output
 * verification are daemon round-trips, and the cache lookup is a local SQLite
 * read. Only a miss starts the workspace's `nx` CLI, to run the target.
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
  // Whether the environment this process was launched with belongs to the
  // workspace asked for, decided before the chdir and the rebinding below. The
  // caller's workspace is the one its own Nx would bind: `NX_WORKSPACE_ROOT_PATH`
  // when set, else the nearest `nx.json` above where it stands (none, when it
  // stands in no workspace).
  const callerRoot = callerEnv.NX_WORKSPACE_ROOT_PATH
    ? resolve(callerCwd, callerEnv.NX_WORKSPACE_ROOT_PATH)
    : findNxWorkspaceRoot(callerCwd);
  const callerOwnsRoot = callerRoot !== null && isSameDirectory(callerRoot, workspaceRoot);
  try {
    // `cd <root> && nx run ...` is the invocation whose hashes are in the
    // cache. Everything below — plugin hooks, `runtime` inputs, and the hasher,
    // which keys against `process.cwd()` — has to see the same directory or the
    // probe keys differently from the CLI and never hits.
    process.chdir(workspaceRoot);
    return await ensureBuiltInWorkspace(workspaceRoot, selector, options.onMiss ?? 'stream', callerOwnsRoot);
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

/**
 * Nx's own overrides for where one workspace keeps its daemon socket, daemon
 * record, task database and cache. Each names the state of a single
 * workspace, and Nx takes it literally: a daemon spawned for another root
 * with `NX_SOCKET_DIR` inherited listens on, and on stop deletes, the socket
 * directory of the workspace that exported it; with
 * `NX_WORKSPACE_DATA_DIRECTORY` it reads and overwrites that workspace's
 * daemon record and task database; and the cache has to move with the
 * database or a hit names artifacts the run never wrote.
 */
const WORKSPACE_STATE_ENV_KEYS = [
  'NX_SOCKET_DIR',
  'NX_DAEMON_SOCKET_DIR',
  'NX_WORKSPACE_DATA_DIRECTORY',
  'NX_CACHE_DIRECTORY',
] as const;

/**
 * The nearest directory at or above `from` holding an `nx.json`, or null.
 * The marker this wrapper roots a workspace at when `--workspace-root` is
 * not given.
 */
export function findNxWorkspaceRoot(from: string): string | null {
  let directory = from;
  for (;;) {
    if (existsSync(join(directory, 'nx.json'))) {
      return directory;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      return null;
    }
    directory = parent;
  }
}

function isSameDirectory(left: string, right: string): boolean {
  try {
    return realpathSync(left) === realpathSync(right);
  } catch {
    // A path that cannot be resolved cannot be shown to be the same
    // workspace, and "not the same" is the isolating answer.
    return false;
  }
}

/**
 * Point every Nx this call starts — the daemon client loaded below and the
 * `nx` CLI a miss spawns — at `workspaceRoot`, and give it that workspace's
 * state locations. A caller that was set up for this same root keeps its
 * overrides: they are the owner's cache and sandbox boundary. A caller set up
 * for another root, or for none, passes none of them on, so Nx falls back to
 * the per-root defaults. Nothing is reset or bypassed: the root's own daemon
 * and cache are used as they stand. `ensureBuilt` restores the caller's
 * environment when it returns.
 */
function isolateWorkspaceEnvironment(workspaceRoot: string, callerOwnsRoot: boolean): void {
  process.env.NX_WORKSPACE_ROOT_PATH = workspaceRoot;
  if (callerOwnsRoot) {
    return;
  }
  for (const key of WORKSPACE_STATE_ENV_KEYS) {
    delete process.env[key];
  }
}

async function ensureBuiltInWorkspace(
  workspaceRoot: string,
  selector: TargetSelector,
  onMiss: 'stream',
  callerOwnsRoot: boolean,
): Promise<EnsureBuiltResult> {
  isolateWorkspaceEnvironment(workspaceRoot, callerOwnsRoot);
  // The environment has precedence over nx.json. Avoid loading Nx at all on
  // this explicit fallback path, which also lets a deliberately minimal
  // checkout-local CLI stand in for Nx.
  if (process.env.NX_DAEMON === 'false') {
    return runViaCli(workspaceRoot, selector, { kind: 'no-daemon', taskId: selectorTaskId(selector) });
  }
  // Nx rejects daemon access when the client package's version differs from
  // the workspace's running daemon. Resolve every runtime module from the
  // target checkout rather than this package's own dependency: a reusable
  // wrapper must keep using the daemon after the workspace takes an Nx patch.
  const requireNx = createWorkspaceNxRequire(workspaceRoot);
  bindWorkspaceRoot(workspaceRoot, requireNx);
  const { daemonClient } = requireNx('nx/src/daemon/client/client');
  if (!daemonClient.enabled()) {
    return runViaCli(workspaceRoot, selector, { kind: 'no-daemon', taskId: selectorTaskId(selector) });
  }

  // An outer Nx task exports its own cache-bypass setting to the process it
  // launches. That setting describes the outer task, not the nested target:
  // inheriting it makes the inner runner refuse cache writes forever. Outside
  // an Nx task child, however, the same variables are the caller's explicit
  // request to force this target to rebuild and must remain authoritative.
  if (process.env.NX_TASK_TARGET_PROJECT !== undefined || process.env.NX_TASK_TARGET_TARGET !== undefined) {
    delete process.env.NX_SKIP_NX_CACHE;
    delete process.env.NX_DISABLE_NX_CACHE;
  }

  const { readNxJson } = requireNx('nx/src/config/nx-json');
  const { splitArgsIntoNxArgsAndOverrides } = requireNx('nx/src/utils/command-line-utils');
  const { setEnvVarsBasedOnArgs } = requireNx('nx/src/tasks-runner/run-command');
  const { createTaskGraph } = requireNx('nx/src/tasks-runner/create-task-graph');
  const hooks = requireNx('nx/src/project-graph/plugins/tasks-execution-hooks');

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

  const inputsChanged = await refreshWorkspaceContext(workspaceRoot, tasks, requireNx);

  const runId = randomUUID();
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
  let reason: MissReason | null;
  if (process.env.NX_SKIP_NX_CACHE === 'true' || process.env.NX_DISABLE_NX_CACHE === 'true') {
    reason = { kind: 'cache-disabled', taskId: selectorTaskId(selector) };
  } else if (inputsChanged) {
    reason = { kind: 'stale-inputs', taskId: selectorTaskId(selector) };
  } else {
    reason = await probe(workspaceRoot, nxJson, nxArgs, projectGraph, taskGraph, tasks, requireNx);
  }
  performance.measure('ensureBuilt:probe', 'ensureBuilt:probe:start');
  // The CLI runs the hooks around its own run, under its own id. This process
  // ran none of the tasks, so it reports none for the id it opened.
  const result = reason === null ? HIT : await runViaCli(workspaceRoot, selector, reason);
  await hooks.runPostTasksExecution({
    id: runId,
    taskResults: NO_TASK_RESULTS,
    workspaceRoot,
    nxJsonConfiguration: nxJson,
    argv: process.argv,
    startTime,
    endTime: Date.now(),
  });
  return result;
}

function selectorTaskId(selector: TargetSelector): string {
  return selector.configuration === undefined
    ? `${selector.project}:${selector.target}`
    : `${selector.project}:${selector.target}:${selector.configuration}`;
}

/**
 * Nx computes `workspaceRoot` once, at module load, from
 * `NX_WORKSPACE_ROOT_PATH` or the current directory — and its daemon client is
 * a module-level singleton that reads `nx.json` from that root. So the root has
 * to be bound before the first Nx module loads, which is why this module
 * imports Nx dynamically throughout. A static import at the top of the file
 * would bind the caller's launch directory instead. `isolateWorkspaceEnvironment`
 * has already set `NX_WORKSPACE_ROOT_PATH`; this loads Nx and checks it took.
 */
function bindWorkspaceRoot(workspaceRoot: string, requireNx: WorkspaceNxRequire): void {
  if (process.env.NX_PERF_LOGGING === 'true') {
    // Installs Nx's PerformanceObserver, which reports every `performance
    // .measure` this module and Nx itself record. Loading it unconditionally
    // would add an observer to hot paths that never want one.
    requireNx('nx/src/utils/perf-logging');
  }
  const { workspaceRoot: boundRoot } = requireNx('nx/src/utils/workspace-root');
  if (boundRoot !== workspaceRoot) {
    // One process cannot serve two workspaces: the constant and the daemon
    // client singleton are already bound. Say so rather than silently probing
    // the wrong checkout.
    throw new Error(
      `ensureBuilt: Nx is already bound to workspace root ${boundRoot}, cannot switch to ${workspaceRoot}`,
    );
  }
}

/**
 * The daemon drains delivered watcher events before serving its graph, but an
 * OS event can still be in flight after a write has completed. Its graph and
 * hashes can then agree with each other while both describe yesterday's files.
 * Take Nx's native, ignore-aware disk snapshot and compare it with the
 * daemon's file table; its metadata cache reuses unchanged file hashes.
 *
 * Any difference is handed to the daemon so its next graph is current, but
 * that only schedules a recomputation, and a probe meanwhile would hash
 * against the file map it already has. So the caller does not probe: if any
 * differing path is not a declared output of a task in the graph, this
 * returns true and the target is run, which lets Nx's own runner wait for the
 * recomputation. Output paths are exempt because a build's own writes are the
 * commonest thing to reach here before the watcher does, and treating them as
 * a miss would make every hit after a build replay the cached log.
 */
async function refreshWorkspaceContext(
  workspaceRoot: string,
  tasks: Task[],
  requireNx: WorkspaceNxRequire,
): Promise<boolean> {
  const { daemonClient } = requireNx('nx/src/daemon/client/client');
  const { WorkspaceContext, matchOutputPaths } = requireNx('nx/src/native');
  const { workspaceDataDirectoryForWorkspace } = requireNx('nx/src/utils/cache-directory');
  performance.mark('ensureBuilt:inputs:start');
  const previousFiles = await daemonClient.getWorkspaceContextFileData();
  const previousHashes = new Map<string, string>();
  for (const { file, hash } of previousFiles) {
    previousHashes.set(file, hash);
  }
  // A context is a snapshot, not a watcher. Do not reuse one across calls.
  const context = new WorkspaceContext(workspaceRoot, workspaceDataDirectoryForWorkspace(workspaceRoot));
  const createdFiles: string[] = [];
  const updatedFiles: string[] = [];
  for (const { file, hash } of context.allFileData()) {
    const previousHash = previousHashes.get(file);
    if (previousHash === undefined) {
      createdFiles.push(file);
    } else if (previousHash !== hash) {
      updatedFiles.push(file);
    }
    previousHashes.delete(file);
  }
  const deletedFiles = [...previousHashes.keys()];
  const changed = [...createdFiles, ...updatedFiles, ...deletedFiles];
  let inputsChanged = false;
  if (changed.length > 0) {
    await daemonClient.updateWorkspaceContext(createdFiles, updatedFiles, deletedFiles);
    // The same matcher the task runner uses to collect outputs, over the union
    // of every task's declared outputs. A negation declared by one task then
    // also excludes another task's output, which errs toward a miss.
    const outputs = tasks.flatMap((task) => task.outputs);
    inputsChanged = matchOutputPaths(outputs, changed).includes(false);
  }
  performance.measure('ensureBuilt:inputs', 'ensureBuilt:inputs:start');
  return inputsChanged;
}

/** `null` when the target is already built; otherwise the first reason it is not. */
async function probe(
  workspaceRoot: string,
  nxJson: NxJsonConfiguration,
  nxArgs: NxArgs,
  projectGraph: ProjectGraph,
  taskGraph: TaskGraph,
  tasks: Task[],
  requireNx: WorkspaceNxRequire,
): Promise<MissReason | null> {
  const { daemonClient } = requireNx('nx/src/daemon/client/client');
  const { DaemonBasedTaskHasher } = requireNx('nx/src/hasher/task-hasher');
  const { getTaskDetails, hashTasks } = requireNx('nx/src/hasher/hash-task');
  const { getTaskSpecificEnv } = requireNx('nx/src/tasks-runner/task-env');
  const { getCache } = requireNx('nx/src/tasks-runner/cache');
  const { getRunnerOptions } = requireNx('nx/src/tasks-runner/run-command');
  const { getExecutorNameForTask } = requireNx('nx/src/tasks-runner/utils');

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

  // An `nx:noop` task has no command: Nx completes it without spawning
  // anything, and normalizes a command-less target that only names dependencies
  // to one. A cache record would vouch for no work — the tasks it aggregates are
  // in this graph and answer for themselves — so none is required. Demanding one
  // made every call through an uncacheable aggregate hand the whole graph to Nx,
  // which replays each dependency's cached log. Outputs a noop declares are
  // still verified below, with every other task's.
  const recordedTasks = tasks.filter((task) => getExecutorNameForTask(task, projectGraph) !== 'nx:noop');

  // Nx's own factory, but deliberately never `init()`ed: that is what makes
  // this a local-only, side-effect-free question. `init()` attaches the remote
  // cache — a network round-trip, and a download is precisely the work this
  // probe exists to detect — and asserts that the cache directory matches the
  // database. Neither matters here: the directory is read only to compare
  // outputs against, and an artifact missing from it is a miss.
  performance.mark('ensureBuilt:cache:start');
  const cache = getCache(runnerOptions);
  const cachedResults = await cache.getBatch(recordedTasks.filter((task) => task.cache && task.hash));
  const cachedCodeByHash = new Map<string, number>();
  for (const [hash, result] of cachedResults) {
    cachedCodeByHash.set(hash, result.code);
  }
  const cacheMiss = firstCacheMiss(recordedTasks, cachedCodeByHash);
  performance.measure('ensureBuilt:cache', 'ensureBuilt:cache:start');
  if (cacheMiss) {
    return cacheMiss;
  }

  // A cache record only proves the task once succeeded with these inputs. The
  // artifact this facility is about — the binary that is about to be exec'd —
  // lives in the working tree, where anything may have deleted or rewritten it
  // since. The daemon answers that cheaply from the output hashes it recorded
  // when Nx last wrote them.
  const claims: OutputClaim[] = [];
  for (const task of tasks) {
    if (task.outputs.length > 0 && task.hash !== undefined) {
      claims.push({ taskId: task.id, outputs: task.outputs, hash: task.hash });
    }
  }
  performance.mark('ensureBuilt:outputs:start');
  const verdicts = await daemonClient.outputsHashesMatchBatch(claims.map(({ outputs, hash }) => ({ outputs, hash })));
  performance.measure('ensureBuilt:outputs', 'ensureBuilt:outputs:start');
  const doubted = unvouched(claims, verdicts);
  if (doubted.length === 0) {
    return null;
  }

  // Its records are lossy, though, and a lost record is not a changed output.
  // They live in memory, so a restart drops every one. They are kept per
  // collapsed directory (Nx tracks at most three paths per level), so a write
  // anywhere under `packages/` voids a task whose outputs span four projects.
  // And a write the daemon processes more than 2 s after a record erases it,
  // which a daemon busy hashing does to a restore's own writes — so each
  // restore guarantees the next one. What settles the question is what a hit
  // would restore: Nx's own local artifact for this exact hash. A working tree
  // that already holds every entry of it is what the restore would leave.
  // Anything else — no artifact, a difference, an error while reading — stays
  // a miss.
  performance.mark('ensureBuilt:artifacts:start');
  const { expandOutputs } = requireNx('nx/src/native');
  const comparison: ArtifactComparison = {
    expandOutputs,
    left: Buffer.allocUnsafe(COMPARE_CHUNK_BYTES),
    right: Buffer.allocUnsafe(COMPARE_CHUNK_BYTES),
    observed: [],
    realDirectories: new Set(),
  };
  for (const claim of doubted) {
    // `recordedTasks` excludes `nx:noop`, so a noop's declared outputs have no
    // artifact here and stay a miss.
    const artifact = cachedResults.get(claim.hash);
    if (artifact === undefined || !outputsMatchArtifact(comparison, workspaceRoot, artifact.outputsPath, claim)) {
      return { kind: 'stale-outputs', taskId: claim.taskId };
    }
  }
  // A write that landed while the bytes were being compared would otherwise be
  // recorded as current.
  const movedBeforeRecord = firstMovedClaim(comparison.observed);
  if (movedBeforeRecord !== null) {
    return { kind: 'stale-outputs', taskId: movedBeforeRecord };
  }
  // Re-arm the daemon the way Nx's runner does after restoring outputs, so the
  // next call is back on the cheap path.
  await daemonClient.recordOutputsHashBatch(doubted.map(({ outputs, hash }) => ({ outputs, hash })));
  const movedDuringRecord = firstMovedClaim(comparison.observed);
  performance.measure('ensureBuilt:artifacts', 'ensureBuilt:artifacts:start');
  return movedDuringRecord === null ? null : { kind: 'stale-outputs', taskId: movedDuringRecord };
}

/** A task's declared outputs, resolved to workspace-relative paths, and the hash they were cached under. */
interface OutputClaim {
  readonly taskId: string;
  readonly outputs: string[];
  readonly hash: string;
}

/** A working-tree node as it was when compared, so a later write to it can be noticed. */
interface ObservedNode {
  readonly taskId: string;
  readonly path: string;
  readonly stats: BigIntStats;
}

interface ArtifactComparison {
  readonly expandOutputs: typeof NxNative.expandOutputs;
  readonly left: Buffer;
  readonly right: Buffer;
  readonly observed: ObservedNode[];
  /** Workspace directories already proven real (not symlinks) on the way to an entry. */
  readonly realDirectories: Set<string>;
}

/** Read size for byte comparison: large enough to stream a binary quickly, small enough never to load one whole. */
const COMPARE_CHUNK_BYTES = 1 << 20;

/**
 * Whether restoring `claim` from the artifact would leave the working tree as
 * it is.
 *
 * The entries are the ones Nx's restore copies: `expandOutputs` over the
 * artifact root with the task's outputs, globs and negations included. That
 * walk is bounded by the artifact, which holds nothing but outputs, and it
 * needs no reading of the pattern here. A restore replaces each entry whole
 * and touches nothing else, so a working-tree file that matches a glob but is
 * absent from the artifact survives it and is no difference. An output path
 * Nx would refuse to restore (absolute, or climbing out with `..`), an
 * artifact holding no entry at all, and system errors while reading are all
 * misses.
 */
function outputsMatchArtifact(
  comparison: ArtifactComparison,
  workspaceRoot: string,
  artifactRoot: string,
  claim: OutputClaim,
): boolean {
  if (claim.outputs.some((output) => isAbsolute(output) || output.split('/').includes('..'))) {
    return false;
  }
  try {
    const entries = comparison.expandOutputs(artifactRoot, claim.outputs);
    return (
      entries.length > 0 &&
      entries.every(
        (entry) =>
          throughRealDirectories(comparison.realDirectories, workspaceRoot, entry) &&
          sameTree(comparison, claim.taskId, join(workspaceRoot, entry), join(artifactRoot, entry)),
      )
    );
  } catch (error) {
    if (isSystemError(error)) {
      return false;
    }
    throw error;
  }
}

/**
 * Whether every directory between the workspace root and `entry` is a real
 * directory. A restore realizes a symlinked parent as a directory, so reading
 * an entry through one would compare bytes the restore does not leave there —
 * and would read the filesystem behind a link.
 */
function throughRealDirectories(realDirectories: Set<string>, workspaceRoot: string, entry: string): boolean {
  let directory = workspaceRoot;
  const segments = entry.split('/');
  for (const segment of segments.slice(0, -1)) {
    directory = join(directory, segment);
    if (realDirectories.has(directory)) {
      continue;
    }
    if (lstatSync(directory, { throwIfNoEntry: false })?.isDirectory() !== true) {
      return false;
    }
    realDirectories.add(directory);
  }
  return true;
}

/**
 * Whether `actual` is `expected` as a restore would recreate it: same node
 * type; a symlink by its text, never followed, because the link is what Nx
 * stores and restores; a file by permission bits and bytes; a directory by its
 * entry names and each entry in turn. Every working-tree node compared is
 * observed, so a write during the comparison can be caught afterwards.
 */
function sameTree(comparison: ArtifactComparison, taskId: string, actual: string, expected: string): boolean {
  const actualStats = lstatSync(actual, { bigint: true, throwIfNoEntry: false });
  const expectedStats = lstatSync(expected, { bigint: true, throwIfNoEntry: false });
  if (actualStats === undefined || expectedStats === undefined) {
    return false;
  }
  comparison.observed.push({ taskId, path: actual, stats: actualStats });
  if (expectedStats.isSymbolicLink()) {
    return actualStats.isSymbolicLink() && readlinkSync(actual) === readlinkSync(expected);
  }
  if (expectedStats.isFile()) {
    return (
      actualStats.isFile() &&
      actualStats.size === expectedStats.size &&
      (actualStats.mode & 0o7777n) === (expectedStats.mode & 0o7777n) &&
      sameBytes(comparison, actual, expected, Number(expectedStats.size))
    );
  }
  if (expectedStats.isDirectory()) {
    if (!actualStats.isDirectory()) {
      return false;
    }
    const names = readdirSync(actual).sort();
    const expectedNames = readdirSync(expected).sort();
    if (names.length !== expectedNames.length) {
      return false;
    }
    for (let index = 0; index < names.length; index += 1) {
      if (names[index] !== expectedNames[index]) {
        return false;
      }
    }
    return names.every((name) => sameTree(comparison, taskId, join(actual, name), join(expected, name)));
  }
  return false;
}

/** Byte equality of two files already known to be `size` bytes, streamed through the comparison's buffers. */
function sameBytes(comparison: ArtifactComparison, actual: string, expected: string, size: number): boolean {
  const actualFd = openSync(actual, 'r');
  try {
    const expectedFd = openSync(expected, 'r');
    try {
      for (let offset = 0; offset < size; offset += COMPARE_CHUNK_BYTES) {
        const length = Math.min(COMPARE_CHUNK_BYTES, size - offset);
        // A short read means the file changed length since it was sized.
        if (
          readSync(actualFd, comparison.left, 0, length, offset) !== length ||
          readSync(expectedFd, comparison.right, 0, length, offset) !== length ||
          comparison.left.compare(comparison.right, 0, length, 0, length) !== 0
        ) {
          return false;
        }
      }
      return true;
    } finally {
      closeSync(expectedFd);
    }
  } finally {
    closeSync(actualFd);
  }
}

/**
 * The task owning the first observed node that is no longer the node it was
 * compared as: replaced (device, inode), rewritten (size, mtime) or otherwise
 * touched (mode, ctime).
 */
function firstMovedClaim(observed: readonly ObservedNode[]): string | null {
  for (const { taskId, path, stats } of observed) {
    let current: BigIntStats | undefined;
    try {
      current = lstatSync(path, { bigint: true, throwIfNoEntry: false });
    } catch (error) {
      if (isSystemError(error)) {
        return taskId;
      }
      throw error;
    }
    if (
      current === undefined ||
      current.dev !== stats.dev ||
      current.ino !== stats.ino ||
      current.mode !== stats.mode ||
      current.size !== stats.size ||
      current.mtimeNs !== stats.mtimeNs ||
      current.ctimeNs !== stats.ctimeNs
    ) {
      return taskId;
    }
  }
  return null;
}

/** An operating-system or native-binding failure, as opposed to a programming error. */
function isSystemError(error: unknown): boolean {
  return error instanceof Error && 'code' in error && typeof error.code === 'string';
}

/**
 * Run the target through the workspace's own `nx`, the invocation whose hashes
 * the probe reproduces, with the child's stdout on this process's stderr.
 *
 * A child, because Nx does not only write through `process.stdout`: its Rust
 * pseudo-terminal and the forked executors it starts with inherited stdio
 * write task output straight to file descriptor 1, and Node cannot point that
 * descriptor elsewhere in its own process. In a child it is simply stderr, so
 * the stdout of a wrapper that goes on to exec a binary carries nothing but
 * that binary's output (`<cli> list | jq` reads only data).
 *
 * Inheriting rather than piping is the other half. The wrapper this facility
 * replaces piped Nx's output so it could read a cache marker out of the log and
 * stay silent on a hit — a text heuristic over ANSI-coloured, format-unstable
 * output. The probe already decided this is no hit, so the output is the run's
 * own, and the child's exit status is forwarded verbatim.
 */
async function runViaCli(
  workspaceRoot: string,
  selector: TargetSelector,
  reason: MissReason,
): Promise<EnsureBuiltResult> {
  const nxCli = join(workspaceRoot, 'node_modules', '.bin', 'nx');
  if (!existsSync(nxCli)) {
    throw new Error(`ensureBuilt: ${describeMiss(reason)}, and ${nxCli} does not exist`);
  }
  const child = spawn(nxCli, ['run', selectorTaskId(selector), '--outputStyle=stream'], {
    cwd: workspaceRoot,
    stdio: ['inherit', 2, 'inherit'],
  });
  // `Promise.withResolvers` would read better but needs lib es2024; this
  // package inherits lib es2022 from tsconfig.base.json.
  const exit = await new Promise<ChildExit>((settle, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => settle({ code, signal }));
  });
  return cliExitOutcome(reason, exit, nxSignalToCode);
}

function nxSignalToCode(signal: NodeJS.Signals | null): number {
  switch (signal) {
    case 'SIGHUP':
      return 129;
    case 'SIGINT':
      return 130;
    case 'SIGQUIT':
      return 131;
    case 'SIGTERM':
      return 143;
    default:
      return 128;
  }
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
