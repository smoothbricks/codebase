import { join, relative, resolve } from 'node:path';
import {
  describeRetentionError,
  describeRetentionStart,
  type Result,
  type RetentionError,
  type RetentionStart,
  refusedAnywhere,
  startRetentionPass,
} from '@smoothbricks/cargo-retain';
import { launchNodeCli } from '@smoothbricks/cargo-retain/launch-node';
import { nodeLocks } from '@smoothbricks/cargo-retain/locks-node';
import { logger, type ProjectGraph, readCachedProjectGraph } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';

function describeCause(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export type TaskStatus = PostTasksExecutionContext['taskResults'][string]['status'];

/** Whether a task with this status ran its command, and so may have built into a Cargo target directory. */
export function ran(status: TaskStatus): boolean {
  switch (status) {
    case 'success':
    case 'failure':
    case 'stopped':
      return true;
    case 'skipped':
    case 'local-cache-kept-existing':
    case 'local-cache':
    case 'remote-cache':
      return false;
  }
}

/**
 * The metadata the plugin puts on every target it infers that builds, checks or tests into a Cargo
 * target directory. Only an executed task carrying it starts a retention pass: a fetch, a lint of
 * another language or a cache-restored replay wrote nothing there.
 */
export const CARGO_PRODUCER_METADATA = { cargoProducer: true } as const;

/**
 * Whether the run's environment is an mbx project's canonical one, which names the shared store beside mbx-managed
 * target views (specs/cowshed/11_shell.md, "Canonical mbx environment"). Such a project's targets are mbx's to collect,
 * so the plugin retains and purges nothing in them (specs/cowshed/16_build_volumes.md, "Migration" step 4).
 */
export function mbxManagesTargets(environment: Readonly<NodeJS.ProcessEnv>): boolean {
  return Boolean(environment.MBX_CACHE_DIR) && environment.MBX_TARGET_VIEWS === '1';
}

/**
 * Sorted, deduplicated candidate Cargo roots for the executed Cargo producer tasks in `taskResults`.
 * Only tasks whose status executed their command ({@link ran}) and whose graph target carries
 * {@link CARGO_PRODUCER_METADATA} qualify; a `nativeTest` target resolves its string `cwd` against `workspaceRoot`,
 * and every other producer resolves `task.projectRoot ?? project.data.root`.
 */
export function retentionRoots(
  taskResults: PostTasksExecutionContext['taskResults'],
  graph: ProjectGraph,
  workspaceRoot: string,
): string[] {
  const roots = new Set<string>();
  for (const { status, task } of Object.values(taskResults)) {
    if (!ran(status)) continue;
    const project = graph.nodes[task.target.project];
    const target = project?.data.targets?.[task.target.target];
    if (project === undefined || target?.metadata?.cargoProducer !== true) continue;
    const cwd: unknown = target.options?.nativeTest === undefined ? undefined : target.options.cwd;
    roots.add(
      typeof cwd === 'string'
        ? resolve(workspaceRoot, cwd)
        : join(workspaceRoot, task.projectRoot ?? project.data.root),
    );
  }
  return [...roots].sort();
}

/**
 * Whether `root` already lies under one of the Cargo workspace roots whose pass answered in this run (`done`).
 * Checked against `done` rather than pre-pruned from {@link retentionRoots} because a member crate's enclosing
 * workspace root is discovered only when `startRetentionPass` answers, and a failed or thrown pass must not suppress
 * a nested root.
 */
export function isCoveredByWorkspace(root: string, done: readonly string[]): boolean {
  return done.some((workspaceRoot) => !relative(workspaceRoot, root).startsWith('..'));
}

export type RetentionPassOutcome =
  | { readonly kind: 'result'; readonly result: Result<RetentionStart, RetentionError> }
  | { readonly kind: 'thrown'; readonly error: unknown };

export interface RetentionReportLog {
  readonly level: 'info' | 'warn';
  readonly message: string;
  /** Cargo workspace root the pass answered for; recorded in `done` so later nested or sibling roots skip. */
  readonly completedWorkspaceRoot?: string;
}

/**
 * Map a single workspace's pass outcome (a typed {@link Result} from `startRetentionPass` or an escaped throw) to
 * the log line and level the hook emits. Maintenance never fails the run it follows: every refusal, error and escaped
 * throw becomes a log entry.
 */
export function reportRetentionOutcome(root: string, outcome: RetentionPassOutcome): RetentionReportLog {
  if (outcome.kind === 'thrown') {
    const detail =
      outcome.error instanceof Error ? (outcome.error.stack ?? outcome.error.message) : String(outcome.error);
    return { level: 'warn', message: `Cargo retention for ${root}: unexpected failure: ${detail}` };
  }
  if (!outcome.result.ok) {
    return { level: 'warn', message: `Cargo retention for ${root}: ${describeRetentionError(outcome.result.error)}` };
  }
  const start = outcome.result.value;
  const completedWorkspaceRoot = start.kind === 'answered' ? start.report.workspaceRoot : start.workspaceRoot;
  const message = describeRetentionStart(start);
  const level = start.kind === 'answered' && refusedAnywhere(start.report) ? 'warn' : 'info';
  return { level, message, completedWorkspaceRoot };
}

/**
 * After a run that executed a Cargo producer task ({@link CARGO_PRODUCER_METADATA}), check whether
 * each Cargo workspace is due for its daily maintenance, and start each due pass in a process of its own
 * (`startRetentionPass`). Nx awaits this hook, inside its daemon when one serves the run, so the pass never runs here:
 * its unlinks are synchronous, and one freeing a large trash on a loaded volume held a daemon, and every client waiting
 * on it, for more than thirty minutes. Here only the gate answers (not due, busy, refused); the started pass reads the
 * current graph under Cargo's locks and records its outcome in the target's receipt, and neither this process nor its
 * exit waits on it. A successful receipt schedules only.
 * Maintenance never fails the run it follows: every outcome, a refused or busy pass included, is only reported. In an
 * mbx project ({@link mbxManagesTargets}) it does nothing.
 */
export async function postTasksExecution(_options: unknown, context: PostTasksExecutionContext): Promise<void> {
  if (mbxManagesTargets(process.env)) return;
  if (!Object.values(context.taskResults).some(({ status }) => ran(status))) return;
  let graph: ProjectGraph;
  try {
    graph = readCachedProjectGraph();
  } catch (error) {
    logger.warn(
      `Cargo retention: cannot read the project graph that names Cargo producer tasks: ${describeCause(error)}`,
    );
    return;
  }
  const done: string[] = [];
  for (const root of retentionRoots(context.taskResults, graph, context.workspaceRoot)) {
    if (isCoveredByWorkspace(root, done)) continue;
    const started = performance.now();
    const cpu = process.cpuUsage();
    let outcome: RetentionPassOutcome;
    try {
      outcome = { kind: 'result', result: await startRetentionPass(root, Date.now(), nodeLocks, launchNodeCli) };
    } catch (error) {
      // Maintenance never fails the run it follows: say what escaped, then go on to the next workspace.
      outcome = { kind: 'thrown', error };
    } finally {
      performance.measure('cargoRetention:total', { start: started });
      const used = process.cpuUsage(cpu);
      performance.measure('cargoRetention:cpu', { start: 0, duration: (used.user + used.system) / 1000 });
    }
    const report = reportRetentionOutcome(root, outcome);
    if (report.completedWorkspaceRoot !== undefined) done.push(report.completedWorkspaceRoot);
    if (report.level === 'warn') logger.warn(report.message);
    else logger.info(report.message);
  }
}
