import { join, relative } from 'node:path';
import {
  describeRetention,
  describeRetentionError,
  type Result,
  type RetentionError,
  type RetentionReport,
  refusedAnywhere,
  retainCargoWorkspace,
} from '@smoothbricks/cargo-retain';
import { nodeLocks } from '@smoothbricks/cargo-retain/locks-node';
import { logger, type ProjectGraph, readCachedProjectGraph } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';

function describeCause(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

type TaskStatus = PostTasksExecutionContext['taskResults'][string]['status'];

/** Whether a task with this status ran its command, and so may have built into a Cargo target directory. */
function ran(status: TaskStatus): boolean {
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
 * After a run that executed a Cargo producer task ({@link CARGO_PRODUCER_METADATA}), check whether
 * each Cargo workspace is due for its daily maintenance. A successful receipt schedules only;
 * every due pass reads the current graph under Cargo's locks and is awaited by this Nx process.
 * Maintenance never fails the run it follows: every outcome, a refused or busy pass included, is only reported.
 */
export async function postTasksExecution(_options: unknown, context: PostTasksExecutionContext): Promise<void> {
  const executed = Object.values(context.taskResults).filter(({ status }) => ran(status));
  if (executed.length === 0) return;
  let graph: ProjectGraph;
  try {
    graph = readCachedProjectGraph();
  } catch (error) {
    logger.warn(
      `Cargo retention: cannot read the project graph that names Cargo producer tasks: ${describeCause(error)}`,
    );
    return;
  }
  const roots = new Set<string>();
  for (const { task } of executed) {
    const project = graph.nodes[task.target.project];
    if (project?.data.targets?.[task.target.target]?.metadata?.cargoProducer !== true) continue;
    roots.add(join(context.workspaceRoot, task.projectRoot ?? project.data.root));
  }
  const done: string[] = [];
  for (const root of [...roots].sort()) {
    if (done.some((workspaceRoot) => !relative(workspaceRoot, root).startsWith('..'))) continue;
    const started = performance.now();
    const cpu = process.cpuUsage();
    let result: Result<RetentionReport, RetentionError>;
    try {
      result = await retainCargoWorkspace(root, { apply: true, automatic: true, now: Date.now() }, nodeLocks);
    } catch (error) {
      // Maintenance never fails the run it follows: say what escaped, then go on to the next workspace.
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      logger.warn(`Cargo retention for ${root}: unexpected failure: ${detail}`);
      continue;
    } finally {
      performance.measure('cargoRetention:total', { start: started });
      const used = process.cpuUsage(cpu);
      performance.measure('cargoRetention:cpu', { start: 0, duration: (used.user + used.system) / 1000 });
    }
    if (!result.ok) {
      logger.warn(`Cargo retention for ${root}: ${describeRetentionError(result.error)}`);
      continue;
    }
    done.push(result.value.workspaceRoot);
    const line = describeRetention(result.value);
    if (refusedAnywhere(result.value)) logger.warn(line);
    else logger.info(line);
  }
}
