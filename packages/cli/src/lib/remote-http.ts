/**
 * One request to a remote HTTP service (GitHub, Forgejo, the npm registry), bounded by the measured `remote-http`
 * bound (specs/cowshed/05_gateway.md, "Deadlines"). The bound covers the whole request: the connect, the answer's head
 * and its whole body, which `remoteRequest` reads before it answers. A `gh` command that makes such a request is the
 * same wait, through `remoteChild`.
 *
 * Past the bound the wait ends on this side only (the fetch is aborted, the `gh` client this process started is sent
 * SIGTERM; the service is never touched) and answers a `RemoteHttpTimeout` naming the op, the peer, the bound, the time
 * waited and how far the request got. The failure is terminal: nothing retries around it and nothing raises the bound
 * to pass. The cli does not depend on lmao, so this is the request half of lmao's `deadline()` for these two waits,
 * with the same injected clock (specs/lmao/01r_transient_errors_and_retry.md, "TypeScript: deadlines").
 */
import type { ChildProcess } from 'node:child_process';

/** specs/cowshed/05_gateway.md "Deadlines", the `remote-http` row: the slowest measured kind's p99 × 20. */
export const REMOTE_HTTP_BOUND_MS = 17_000;

/** Monotonic milliseconds and the one timer a wait arms; `after` answers its cancel. A test fires its timer itself. */
export interface RemoteHttpTimers {
  now(): number;
  after(ms: number, fire: () => void): () => void;
}

export const SYSTEM_TIMERS: RemoteHttpTimers = {
  now: () => performance.now(),
  after(ms, fire) {
    const timer = setTimeout(fire, ms);
    return () => clearTimeout(timer);
  },
};

/** How far a request got: nothing, a `gh` client started, or the answer's head arrived and its body did not finish. */
export type RemoteProgress = 'nothing' | 'spawned' | 'head';

/** A remote request that got no whole answer within the `remote-http` bound: code `TIMEOUT`. */
export interface RemoteHttpTimeout {
  readonly code: 'TIMEOUT';
  readonly op: string;
  /** The service's origin, or the `gh` client's executable. */
  readonly peer: string;
  readonly boundMs: number;
  readonly elapsedMs: number;
  readonly last: RemoteProgress;
}

export type RemoteResult<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly error: RemoteHttpTimeout };

/** A whole answer: its status and its body, read to its end within the bound. */
export interface RemoteAnswer {
  readonly status: number;
  readonly ok: boolean;
  readonly body: string;
}

/** The one line a `RemoteHttpTimeout` prints. */
export function describeRemoteHttpTimeout(timeout: RemoteHttpTimeout): string {
  return `${timeout.code}: ${timeout.op}: ${timeout.peer} gave no whole answer within the remote-http bound of ${timeout.boundMs} ms (waited ${timeout.elapsedMs} ms; last progress: ${timeout.last})`;
}

/** A `RemoteHttpTimeout` raised where the caller's convention is to throw every failure. */
export class RemoteHttpTimeoutError extends Error {
  constructor(readonly timeout: RemoteHttpTimeout) {
    super(describeRemoteHttpTimeout(timeout));
    this.name = 'RemoteHttpTimeoutError';
  }
}

/** Arm the bound for one wait; `expired` says whether it fired, and `timeout` is the failure it answers. */
function arm(op: string, peer: string, timers: RemoteHttpTimers, onExpiry: () => void) {
  const started = timers.now();
  let fired = false;
  const cancel = timers.after(REMOTE_HTTP_BOUND_MS, () => {
    fired = true;
    onExpiry();
  });
  return {
    cancel,
    expired: () => fired,
    timeout: (last: RemoteProgress): RemoteHttpTimeout => ({
      code: 'TIMEOUT',
      op,
      peer,
      boundMs: REMOTE_HTTP_BOUND_MS,
      elapsedMs: Math.max(0, timers.now() - started),
      last,
    }),
  };
}

/**
 * Send one request and read its whole answer within the `remote-http` bound. A transport failure rejects as `fetch`
 * raised it, so each caller keeps its own reading of one; only the bound's expiry is answered here.
 */
export async function remoteRequest(
  op: string,
  url: string,
  init: Omit<RequestInit, 'signal'>,
  timers: RemoteHttpTimers = SYSTEM_TIMERS,
): Promise<RemoteResult<RemoteAnswer>> {
  const controller = new AbortController();
  const bound = arm(op, new URL(url).origin, timers, () => controller.abort());
  let last: RemoteProgress = 'nothing';
  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    last = 'head';
    const body = await response.text();
    return { ok: true, value: { status: response.status, ok: response.ok, body } };
  } catch (error) {
    if (bound.expired()) return { ok: false, error: bound.timeout(last) };
    throw error;
  } finally {
    bound.cancel();
  }
}

/**
 * Wait for `child`, a `gh` client making one request, to close within the `remote-http` bound. Past it the client is
 * sent SIGTERM and the wait answers at once; a client that cannot start rejects with its spawn error.
 */
export function remoteChild(
  op: string,
  child: ChildProcess,
  timers: RemoteHttpTimers = SYSTEM_TIMERS,
): Promise<RemoteResult<{ code: number | null; signal: NodeJS.Signals | null }>> {
  const { promise, resolve, reject } =
    Promise.withResolvers<RemoteResult<{ code: number | null; signal: NodeJS.Signals | null }>>();
  let last: RemoteProgress = 'nothing';
  const bound = arm(op, child.spawnfile, timers, () => {
    child.kill('SIGTERM');
    resolve({ ok: false, error: bound.timeout(last) });
  });
  child.once('spawn', () => {
    last = 'spawned';
  });
  child.once('error', (error) => {
    bound.cancel();
    reject(error);
  });
  child.once('close', (code, signal) => {
    bound.cancel();
    resolve({ ok: true, value: { code, signal } });
  });
  return promise;
}
