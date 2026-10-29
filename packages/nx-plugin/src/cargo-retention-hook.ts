import { join, relative } from 'node:path';
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
 * Whether the run's environment is an mbx project's canonical one, which names the shared store beside mbx-managed
 * target views (specs/cowshed/11_shell.md, "Canonical mbx environment"). Such a project's targets are mbx's to collect,
 * so the plugin retains and purges nothing in them (specs/cowshed/16_build_volumes.md, "Migration" step 4).
 */
export function mbxManagesTargets(environment: Readonly<NodeJS.ProcessEnv>): boolean {
  return Boolean(environment.MBX_CACHE_DIR) && environment.MBX_TARGET_VIEWS === '1';
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
    let result: Result<RetentionStart, RetentionError>;
    try {
      result = await startRetentionPass(root, Date.now(), nodeLocks, launchNodeCli);
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
    const start = result.value;
    done.push(start.kind === 'answered' ? start.report.workspaceRoot : start.workspaceRoot);
    const line = describeRetentionStart(start);
    if (start.kind === 'answered' && refusedAnywhere(start.report)) logger.warn(line);
    else logger.info(line);
  }
}
