import { parentPort, workerData } from 'node:worker_threads';
import { type CloseOutcome, createCpuPool } from './cpu-pool.js';

/**
 * The thread an Nx run's CPU pool runs on (`run-cpu-pool.ts`; specs/cowshed/05_gateway.md, "Every Nx run budgets
 * itself"). The process that serves a run's pool, the Nx daemon when installed plugins load in it or the Nx CLI without
 * one, hashes tasks and builds the project graph in synchronous calls on its main thread. A pool there writes no
 * heartbeat while one runs, and every runner queued on it fails `SILENT` while it still works. Here the pool's socket,
 * jobserver fifo and heartbeat share an event loop that runs nothing else, so the heartbeat proves that the actor which
 * admits and grants is turning.
 *
 * The thread is started with the pool's count. It says once that the pool serves, with the environment a task inherits,
 * or why it cannot; the host's one message is its close, after which the thread says how the pool closed.
 */

/** What the thread is started with. */
export interface CpuPoolThreadData {
  readonly tokens: number;
}

/** What the thread says, in order: the pool serves or failed; after the host's close, how the pool closed. */
export type CpuPoolThreadReport =
  | { readonly kind: 'serving'; readonly environment: Record<string, string> }
  | { readonly kind: 'failed'; readonly error: string }
  | { readonly kind: 'closed'; readonly outcome: CloseOutcome };

const host = parentPort;
if (host === null) {
  throw new Error('cpu-pool-thread runs as the worker thread run-cpu-pool starts, never as a module of its own');
}
const report = (message: CpuPoolThreadReport): void => host.postMessage(message);
const data: CpuPoolThreadData = workerData;
// The pool lives until its host closes it, or with the thread.
const made = await createCpuPool(data.tokens, new Promise<void>(() => {}));
if (made.ok) {
  const { pool } = made;
  host.once('message', () => {
    void pool.close().then((outcome) => report({ kind: 'closed', outcome }));
  });
  report({ kind: 'serving', environment: pool.environment });
} else {
  report({ kind: 'failed', error: made.error });
}
