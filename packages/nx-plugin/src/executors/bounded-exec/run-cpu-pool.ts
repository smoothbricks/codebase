import { availableParallelism } from 'node:os';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import { logger } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import { type CloseOutcome, type CpuPool, readInheritedCpuPool } from './cpu-pool.js';
import type { CpuPoolThreadData, CpuPoolThreadReport } from './cpu-pool-thread.js';
import { mbxPool } from './mbx-pool.js';
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
 *
 * The pool runs on a thread of its own (`cpu-pool-thread.ts`). The process that serves it, the Nx daemon when installed
 * plugins load in it or the Nx CLI without one, hashes the run's tasks and builds its project graph in synchronous calls
 * on its main thread. In a gate on 2026-10-28 one `hashPlans` ran for 14.2 s, and sixteen runners queued on a pool on
 * that thread failed `SILENT` together while it was alive (05_gateway.md, "Deadlines"). The thread's event loop runs the
 * pool alone; this module starts it, publishes the environment it reports and closes it.
 */

/** The pools this process serves, by Nx run id, until that run's `postTasksExecution`. */
const pools = new Map<string, CpuPool>();

/**
 * The pool thread's module, in the form this one was loaded in: compiled JavaScript when Nx loads the plugin, its
 * TypeScript source under Bun. A Bun worker starts without its parent's preloads, so from source the thread takes the
 * transform preload this package's source needs, the one its bunfig names; compiled, it needs none.
 */
const FROM_SOURCE = import.meta.url.endsWith('.ts');
const THREAD_ENTRY = new URL(FROM_SOURCE ? './cpu-pool-thread.ts' : './cpu-pool-thread.js', import.meta.url);
const THREAD_PRELOAD: { readonly preload?: string[] } = FROM_SOURCE
  ? { preload: [fileURLToPath(import.meta.resolve('@smoothbricks/validation/bun/preload'))] }
  : {};

type ServedPool = { ok: true; pool: CpuPool } | { ok: false; error: string };

/** Where a pool thread is, and who waits on it: its start, its close, or nobody. */
type ThreadState =
  | { readonly kind: 'starting'; readonly served: (served: ServedPool) => void }
  | { readonly kind: 'serving' }
  | { readonly kind: 'closing'; readonly closed: (outcome: CloseOutcome) => void }
  | { readonly kind: 'ended'; readonly outcome: CloseOutcome };

/**
 * The count of the pool a run is served, or null when it is served none. A run that inherits a pool (an enclosing
 * executor's) takes from that one; with mbx, its pool is the host's one ledger, and a run pool beside it would count
 * every leaf twice. Otherwise one token per core, or the inherited positive count when that is fewer.
 */
export function runPoolTokens(env: Readonly<NodeJS.ProcessEnv>, cores: number): number | null {
  if (readInheritedCpuPool(env) !== null || mbxPool(env) !== null) {
    return null;
  }
  const inherited = parseInheritedTokens(env[CPU_TOKENS_ENV]);
  return inherited === null ? cores : Math.min(inherited, cores);
}

/**
 * Serve `context`'s run a pool of `runPoolTokens` and publish it in `env`, which every task inherits, unless the run
 * is served none. A pool that cannot be made is said, and the run's tasks are then sized only by the ceilings they
 * inherit.
 */
export async function serveRunCpuPool(
  context: Pick<PreTasksExecutionContext, 'id'>,
  env: NodeJS.ProcessEnv = process.env,
  cores: number = availableParallelism(),
): Promise<void> {
  const tokens = runPoolTokens(env, cores);
  if (tokens === null) {
    return;
  }
  const served = await servePoolThread(tokens);
  if (!served.ok) {
    logger.warn(
      `bounded-exec: this run's CPU pool failed (${served.error}); its tasks are sized only by the ceilings they inherit`,
    );
    return;
  }
  pools.set(context.id, served.pool);
  for (const [name, value] of Object.entries(served.pool.environment)) {
    env[name] = value;
  }
}

/**
 * Start a pool of `tokens` on a thread of its own, and wait until it serves or says why it cannot. The first `close()`
 * closes the pool and ends the thread; every call returns that close's outcome. A thread that throws or exits before it
 * has said how its pool closed fails whichever wait is open, a later close returns that failure, and a thread lost while
 * its pool serves is said at once.
 */
function servePoolThread(tokens: number): Promise<ServedPool> {
  const thread = new Worker(THREAD_ENTRY, { workerData: { tokens } satisfies CpuPoolThreadData, ...THREAD_PRELOAD });
  return new Promise<ServedPool>((served) => {
    let state: ThreadState = { kind: 'starting', served };
    let closing: Promise<CloseOutcome> | undefined;
    // The pool is done with its thread once it has said how it ended: nothing of the pool's is left to run there.
    const finish = (outcome: CloseOutcome): ThreadState => {
      const was = state;
      state = { kind: 'ended', outcome };
      void thread.terminate();
      return was;
    };
    const lose = (why: string): void => {
      if (state.kind === 'ended') {
        // The exit that finishing the thread caused.
        return;
      }
      const error = `the CPU pool's thread ${why}`;
      const was = finish({ ok: false, error });
      if (was.kind === 'starting') {
        was.served({ ok: false, error });
      } else if (was.kind === 'closing') {
        was.closed({ ok: false, error });
      } else {
        logger.warn(`bounded-exec: this run's CPU pool stopped serving: ${error}`);
      }
    };
    const close = (): Promise<CloseOutcome> => {
      closing ??=
        state.kind === 'ended'
          ? Promise.resolve(state.outcome)
          : new Promise<CloseOutcome>((closed) => {
              state = { kind: 'closing', closed };
              thread.postMessage(null);
            });
      return closing;
    };
    thread.on('message', (report: CpuPoolThreadReport) => {
      const now = state;
      if (now.kind === 'starting' && report.kind === 'serving') {
        state = { kind: 'serving' };
        now.served({ ok: true, pool: { environment: report.environment, close } });
      } else if (now.kind === 'starting' && report.kind === 'failed') {
        finish({ ok: false, error: report.error });
        now.served({ ok: false, error: report.error });
      } else if (now.kind === 'closing' && report.kind === 'closed') {
        finish(report.outcome);
        now.closed(report.outcome);
      } else {
        lose(`said ${report.kind} while ${now.kind}`);
      }
    });
    thread.on('error', (error) => lose(`failed: ${error instanceof Error ? error.message : String(error)}`));
    thread.on('exit', (code) => lose(`exited with code ${code}`));
  });
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
