import { lstat } from 'node:fs/promises';
import { availableParallelism } from 'node:os';
import { logger } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import { type CpuPool, createCpuPool, readInheritedCpuPool } from './cpu-pool.js';
import { GATEWAY_SOCKET } from './gateway-lease.js';

/**
 * The CPU budget of an Nx run on a host without a gateway (specs/cowshed/05_gateway.md, "Host CPU budget", "A run
 * without a gateway").
 *
 * Every runner sizes itself to the whole host unless something budgets it, and without a gateway nothing did: under
 * `NX_PARALLEL=100%` Nx starts every target side by side and each one's runner takes the whole machine. The run
 * therefore serves its own pool of one token per core, as a gateway-granted executor serves its children, and every
 * task inherits it: `bounded-exec` takes from it as from any parent pool, FIFO, the head taking `min(want, free)` once
 * one token is free, and a Cargo that Nx's own `nx:run-commands` starts reads its jobserver fifo through the
 * `CARGO_MAKEFLAGS` the pool exports, so the runners of one run together stay within the cores.
 *
 * The run serves it only when nothing else budgets it: it inherits no pool, and the gateway's control socket does
 * not exist. Only `ENOENT` proves the gateway absent; a socket that exists but does not answer may be a live,
 * saturated gateway, and a run budgeting itself beside it would count the host twice.
 */

/** The pools this process serves, by Nx run id, until that run's `postTasksExecution`. */
const pools = new Map<string, CpuPool>();

/**
 * Serve `context`'s run a pool of `cores` tokens and publish it in `env`, which every task inherits, when the run
 * inherits no pool and no gateway exists at `gateway`. A pool that cannot be made is said and leaves the run
 * unbudgeted, as a run without a gateway was before.
 */
export async function serveRunCpuPool(
  context: Pick<PreTasksExecutionContext, 'id'>,
  env: NodeJS.ProcessEnv = process.env,
  gateway: string = GATEWAY_SOCKET,
  cores: number = availableParallelism(),
): Promise<void> {
  if (readInheritedCpuPool(env) !== null || !(await absent(gateway))) {
    return;
  }
  // The pool lives until the run's `postTasksExecution` closes it, or with this process.
  const made = await createCpuPool(cores, new Promise<void>(() => {}), { begin() {}, end() {} });
  if (!made.ok) {
    logger.warn(
      `bounded-exec: no gateway at ${gateway}, and this run's CPU pool failed (${made.error}); its tasks run unbudgeted`,
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

async function absent(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return false;
  } catch (error) {
    const code = error instanceof Error && 'code' in error ? error.code : undefined;
    return code === 'ENOENT' || code === 'ENOTDIR';
  }
}
