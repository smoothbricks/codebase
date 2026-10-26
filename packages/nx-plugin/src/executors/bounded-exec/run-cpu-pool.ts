import { availableParallelism } from 'node:os';
import { logger } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import { type CpuPool, createCpuPool, readInheritedCpuPool } from './cpu-pool.js';
import { CPU_TOKENS_ENV, parseInheritedTokens } from './runner-sizing.js';

/**
 * The CPU pool of an Nx run (specs/cowshed/05_gateway.md, "Host scheduling", "Parallelism inside a job", "Every Nx run
 * budgets itself").
 *
 * Every runner sizes itself to the whole host unless something sizes it: under `NX_PARALLEL=100%` Nx starts every
 * target side by side and each one's runner takes the whole machine. The run therefore serves its own pool, as a
 * granted executor serves its children, and every task inherits it: `bounded-exec` takes from it as from any parent
 * pool, FIFO, the head taking `min(want, free)` once one token is free, and a Cargo that Nx's own `nx:run-commands`
 * starts reads its jobserver fifo through the `CARGO_MAKEFLAGS` the pool exports, so the runners of one run together
 * stay within its count.
 *
 * A run that inherits a pool (an enclosing executor's) takes from that one and serves none. Otherwise its pool holds
 * one token per core, or the inherited positive `BOUNDED_EXEC_CPU_TOKENS` when that is fewer: inside a cowshed job
 * that count is the job's own, so the job's `--want` bounds the run.
 */

/** The pools this process serves, by Nx run id, until that run's `postTasksExecution`. */
const pools = new Map<string, CpuPool>();

/**
 * Serve `context`'s run a pool of `min(inherited count, cores)` tokens and publish it in `env`, which every task
 * inherits, unless the run inherits a pool. A pool that cannot be made is said, and the run's tasks are then sized only
 * by the ceilings they inherit.
 */
export async function serveRunCpuPool(
  context: Pick<PreTasksExecutionContext, 'id'>,
  env: NodeJS.ProcessEnv = process.env,
  cores: number = availableParallelism(),
): Promise<void> {
  if (readInheritedCpuPool(env) !== null) {
    return;
  }
  const inherited = parseInheritedTokens(env[CPU_TOKENS_ENV]);
  // The pool lives until the run's `postTasksExecution` closes it, or with this process.
  const made = await createCpuPool(
    inherited === null ? cores : Math.min(inherited, cores),
    new Promise<void>(() => {}),
  );
  if (!made.ok) {
    logger.warn(
      `bounded-exec: this run's CPU pool failed (${made.error}); its tasks are sized only by the ceilings they inherit`,
    );
    return;
  }
  pools.set(context.id, made.pool);
  for (const [name, value] of Object.entries(made.pool.environment)) {
    env[name] = value;
  }
}

/** Close the pool `context`'s run was served, if it was served one. */
export async function closeRunCpuPool(context: Pick<PostTasksExecutionContext, 'id'>): Promise<void> {
  const pool = pools.get(context.id);
  if (pool === undefined) {
    return;
  }
  pools.delete(context.id);
  const closed = await pool.close();
  if (!closed.ok) {
    logger.warn(`bounded-exec: this run's CPU pool did not close cleanly: ${closed.error}`);
  }
}
