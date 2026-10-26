/**
 * Deadlines for waits on another process: lmao-core's `deadline.rs` values in TypeScript, and the shell that bounds a
 * wait with them (specs/lmao/01r_transient_errors_and_retry.md, "TypeScript deadlines"; smoothbricks
 * specs/cowshed/05_gateway.md, "Deadlines").
 *
 * A wait on a peer is one of two shapes, and the shape decides its bound:
 *
 * - A **request** (a control call, a handshake, reaping a child that already closed its pipes, a system-service call)
 *   answers in the time its {@link LatencyClass} fixes. {@link deadline} gives its body one monotonic budget from the
 *   request to the answer; past it the wait answers `TIMEOUT`.
 * - A **long wait** (a job's end, a held lease, a queue, a stream) lasts as long as its work and has no total ceiling;
 *   its holder must show progress. {@link idle} answers `SILENT` once the holder has shown none for the idle bound.
 *
 * `PEER_LOST` is a peer that ended the connection before answering, so the outcome is unknown. All three are terminal:
 * nothing retries around them and nothing raises a bound to pass; the error is the diagnosis. A peer absent at connect
 * is `Blocked.service(peer)`, whose fallback the caller states. Each failure comes back as an `Err`, so an Op that
 * returns it with `ctx.err(result.error)` ends its span `span-err` with the failure's code.
 *
 * @example
 * ```typescript
 * const answer = await deadline('status', `gateway ${socket}`, CONTROL, async (wait) => {
 *   const response = await fetch(url, { signal: wait.signal });
 *   wait.progress(CONNECTED);
 *   return response.ok ? new Ok(await response.text()) : new Err(HTTP_ERROR({ status: response.status }));
 * });
 * if (answer.isErr()) return ctx.err(answer.error);
 * ```
 */

import { Err, type Result } from '../result.js';
import { CodeError } from './CodeError.js';

//#region smoo/lmao!n/op-retry.deadlines
/** How long a request across a process boundary takes when its peer is well (Rust `LatencyClass`). */
export type LatencyClass = 'control' | 'handshake' | 'reap' | 'ffi';

/** A class and the bound its owner's spec table records for it; no call site picks a number. */
export interface Bound {
  readonly class: LatencyClass;
  readonly withinMs: number;
}

/**
 * The last thing a waiter saw its peer do (Rust `Progress`). A failure names it, so the reader knows whether the peer
 * never answered the connect, took the request and went silent, or stopped mid-answer.
 */
export type Progress =
  | { readonly kind: 'nothing' }
  | { readonly kind: 'connected' }
  | { readonly kind: 'sent'; readonly bytes: number }
  | { readonly kind: 'received'; readonly bytes: number }
  | { readonly kind: 'heartbeat'; readonly seq: number }
  | { readonly kind: 'signalled'; readonly signal: number }
  /** A state the holder reported (`queued`, `running`, `draining`). */
  | { readonly kind: 'state'; readonly state: string };

export const NOTHING: Progress = { kind: 'nothing' };
export const CONNECTED: Progress = { kind: 'connected' };

/** `sent` with `bytes` more: the counter a stream's writes advance. */
export function sent(last: Progress, bytes: number): Progress {
  return { kind: 'sent', bytes: last.kind === 'sent' ? last.bytes + bytes : bytes };
}

/** `received` with `bytes` more: the counter a stream's reads advance. */
export function received(last: Progress, bytes: number): Progress {
  return { kind: 'received', bytes: last.kind === 'received' ? last.bytes + bytes : bytes };
}

export function describeProgress(progress: Progress): string {
  switch (progress.kind) {
    case 'nothing':
      return 'nothing';
    case 'connected':
      return 'connected';
    case 'sent':
      return `sent ${progress.bytes} bytes`;
    case 'received':
      return `received ${progress.bytes} bytes`;
    case 'heartbeat':
      return `heartbeat ${progress.seq}`;
    case 'signalled':
      return `signalled ${progress.signal}`;
    case 'state':
      return progress.state;
  }
}

/**
 * `ms` as Rust's `Duration` `{:?}` prints it (`5s`, `1.2s`, `500ms`, `750µs`, `0ns`): the largest unit that is at
 * least 1, its fraction exact to the nanosecond with trailing zeros dropped. Every failure line prints its durations
 * this way, so a TS failure reads as the Rust one does.
 */
export function formatDuration(ms: number): string {
  const nanos = Math.max(0, Math.round(ms * 1_000_000));
  const [unit, scale, digits] =
    nanos >= 1_000_000_000
      ? (['s', 1_000_000_000, 9] as const)
      : nanos >= 1_000_000
        ? (['ms', 1_000_000, 6] as const)
        : nanos >= 1_000
          ? (['µs', 1_000, 3] as const)
          : (['ns', 1, 0] as const);
  const whole = Math.floor(nanos / scale);
  const fraction = nanos % scale;
  return fraction === 0
    ? `${whole}${unit}`
    : `${whole}.${String(fraction).padStart(digits, '0').replace(/0+$/, '')}${unit}`;
}

/** Monotonic milliseconds, injected so a test moves time itself. */
export interface Clock {
  now(): number;
}

/** The clock and the one timer the shell arms; `after` answers its cancel. A test fires its timers itself. */
export interface Timers extends Clock {
  after(ms: number, fire: () => void): () => void;
}

export const SYSTEM_TIMERS: Timers = {
  now: () => performance.now(),
  after(ms, fire) {
    const timer = setTimeout(fire, ms);
    return () => clearTimeout(timer);
  },
};

/** A request that got no answer within its bound: code `TIMEOUT`. */
export interface Expired {
  readonly op: string;
  readonly peer: string;
  readonly class: LatencyClass;
  readonly boundMs: number;
  readonly elapsedMs: number;
  readonly last: Progress;
}

/** A long wait whose holder showed no progress for its idle bound: code `SILENT`. */
export interface Silent {
  readonly op: string;
  readonly holder: string;
  readonly idleMs: number;
  readonly silentForMs: number;
  readonly elapsedMs: number;
  readonly last: Progress;
}

/** A peer that ended the connection before answering: code `PEER_LOST`. */
export interface PeerLost {
  readonly op: string;
  readonly peer: string;
  readonly elapsedMs: number;
  readonly last: Progress;
}

/**
 * A request past its class's bound: code `TIMEOUT`, terminal. Its message is the one line lmao-core's `Expired` prints,
 * and an `Err`'s `isErr(TimeoutError)` (or `instanceof`) discriminates it.
 */
export class TimeoutError extends CodeError<'TIMEOUT', Expired> {
  static readonly _tag = 'TIMEOUT';

  constructor(data: Expired) {
    super('TIMEOUT', data);
    this.name = 'TimeoutError';
    this.message = `TIMEOUT: ${data.op} on ${data.peer}: no answer within its ${data.class} bound of ${formatDuration(data.boundMs)}; waited ${formatDuration(data.elapsedMs)}, last progress: ${describeProgress(data.last)}`;
  }
}

/** A long wait whose holder showed no progress for its silence bound: code `SILENT`, terminal. */
export class SilentError extends CodeError<'SILENT', Silent> {
  static readonly _tag = 'SILENT';

  constructor(data: Silent) {
    super('SILENT', data);
    this.name = 'SilentError';
    this.message = `SILENT: ${data.op}: ${data.holder} showed no progress for ${formatDuration(data.silentForMs)} (idle bound ${formatDuration(data.idleMs)}); waited ${formatDuration(data.elapsedMs)} in all, last progress: ${describeProgress(data.last)}`;
  }
}

/**
 * A peer that ended the connection before answering, so the outcome is unknown: code `PEER_LOST`, terminal. Its message
 * is the line cowshed-os's `WaitError::PeerLost` prints.
 */
export class PeerLostError extends CodeError<'PEER_LOST', PeerLost> {
  static readonly _tag = 'PEER_LOST';

  constructor(data: PeerLost) {
    super('PEER_LOST', data);
    this.name = 'PeerLostError';
    this.message = `PEER_LOST: ${data.op} on ${data.peer}: the peer ended the connection after ${formatDuration(data.elapsedMs)}; last progress: ${describeProgress(data.last)}`;
  }
}

export type WaitError = TimeoutError | SilentError | PeerLostError;

/** One request's budget: started once, before the connect, and never restarted by a retry or a fallback. */
export class Deadline {
  private constructor(
    readonly op: string,
    readonly bound: Bound,
    private readonly startedAt: number,
  ) {}

  static start(op: string, bound: Bound, clock: Clock): Deadline {
    return new Deadline(op, bound, clock.now());
  }

  elapsed(clock: Clock): number {
    return Math.max(0, clock.now() - this.startedAt);
  }

  /** What is left of the budget; `null` once it is spent. */
  remaining(clock: Clock): number | null {
    const left = this.bound.withinMs - this.elapsed(clock);
    return left > 0 ? left : null;
  }

  expired(peer: string, last: Progress, clock: Clock): TimeoutError {
    return new TimeoutError({
      op: this.op,
      peer,
      class: this.bound.class,
      boundMs: this.bound.withinMs,
      elapsedMs: this.elapsed(clock),
      last,
    });
  }
}

/** The silence of a long wait's holder, measured from its last progress. */
export class Idle {
  private lastAt: number;
  private lastProgress: Progress = NOTHING;

  private constructor(
    readonly op: string,
    private idleMs: number,
    private readonly startedAt: number,
  ) {
    this.lastAt = startedAt;
  }

  static start(op: string, idleMs: number, clock: Clock): Idle {
    return new Idle(op, idleMs, clock.now());
  }

  get last(): Progress {
    return this.lastProgress;
  }

  /** The whole wait so far, progress included. */
  elapsed(clock: Clock): number {
    return Math.max(0, clock.now() - this.startedAt);
  }

  /** Allow the holder `idleMs` of silence from here on, keeping the wait's start and its last progress. */
  setSilence(idleMs: number): void {
    this.idleMs = idleMs;
  }

  /** The holder showed `progress`: the silence starts over. */
  observe(progress: Progress, clock: Clock): void {
    this.lastAt = clock.now();
    this.lastProgress = progress;
  }

  /** How much more silence the holder is allowed; `null` once it has been silent too long. */
  remaining(clock: Clock): number | null {
    const left = this.idleMs - Math.max(0, clock.now() - this.lastAt);
    return left > 0 ? left : null;
  }

  silent(holder: string, clock: Clock): SilentError {
    const now = clock.now();
    return new SilentError({
      op: this.op,
      holder,
      idleMs: this.idleMs,
      silentForMs: Math.max(0, now - this.lastAt),
      elapsedMs: Math.max(0, now - this.startedAt),
      last: this.lastProgress,
    });
  }
}

/** What a bounded wait's body is given. */
export interface Wait {
  /**
   * Aborted once the bound passes, with the wait's failure as its reason. The body ends its own side of the wait on it
   * (aborts its fetch, destroys its socket, stops reading); the peer is never ended.
   */
  readonly signal: AbortSignal;
  /** The peer did something: a failure names it, and an idle wait's silence starts over. */
  progress(progress: Progress): void;
  /** The peer ended the connection before answering: `PEER_LOST`, naming the time waited and the last progress. */
  lost(): PeerLostError;
}

/** What a long wait's body is given: a {@link Wait} whose holder's silence bound can change once it shows its cadence. */
export interface IdleWait extends Wait {
  /**
   * Allow the holder `idleMs` of silence from here on, keeping the wait's start and its last progress: a holder that has
   * just shown it heartbeats is held to the heartbeat's bound.
   */
  setSilence(idleMs: number): void;
}

/**
 * Run `body`, a request to `peer`, within `bound`. Its own result passes through; past the bound the wait answers
 * `TIMEOUT` and aborts `wait.signal`. A body that has not started by then is never started.
 */
export function deadline<T, E>(
  op: string,
  peer: string,
  bound: Bound,
  body: (wait: Wait) => Promise<Result<T, E>>,
  timers: Timers = SYSTEM_TIMERS,
): Promise<Result<T, E | TimeoutError>> {
  const budget = Deadline.start(op, bound, timers);
  let last = NOTHING;
  return bounded(
    timers,
    {
      progress(progress) {
        last = progress;
      },
      lost: () => new PeerLostError({ op, peer, elapsedMs: budget.elapsed(timers), last }),
      remaining: () => budget.remaining(timers),
      failure: () => budget.expired(peer, last, timers),
    },
    body,
  );
}

/**
 * Run `body`, a long wait on `holder`, for as long as the holder shows progress (`wait.progress`) at least every
 * `idleMs`. Its own result passes through; once the holder has been silent for `idleMs` the wait answers `SILENT` and
 * aborts `wait.signal`. There is no total ceiling.
 */
export function idle<T, E>(
  op: string,
  holder: string,
  idleMs: number,
  body: (wait: IdleWait) => Promise<Result<T, E>>,
  timers: Timers = SYSTEM_TIMERS,
): Promise<Result<T, E | SilentError>> {
  const silence = Idle.start(op, idleMs, timers);
  return bounded(
    timers,
    {
      progress: (progress) => silence.observe(progress, timers),
      lost: () => new PeerLostError({ op, peer: holder, elapsedMs: silence.elapsed(timers), last: silence.last }),
      remaining: () => silence.remaining(timers),
      failure: () => silence.silent(holder, timers),
    },
    (wait, rearm) =>
      body({
        ...wait,
        setSilence(ms) {
          silence.setSilence(ms);
          rearm();
        },
      }),
  );
}

/** The value a bounded wait is measured by: the deadline's budget or the idle wait's silence. */
interface Measure<F> {
  progress(progress: Progress): void;
  lost(): PeerLostError;
  remaining(): number | null;
  failure(): F;
}

function bounded<T, E, F>(
  timers: Timers,
  measure: Measure<F>,
  body: (wait: Wait, rearm: () => void) => Promise<Result<T, E>>,
): Promise<Result<T, E | F>> {
  const controller = new AbortController();
  const wait: Wait = {
    signal: controller.signal,
    progress: (progress) => measure.progress(progress),
    lost: () => measure.lost(),
  };
  return new Promise<Result<T, E | F>>((resolve, reject) => {
    let settled = false;
    let cancel = () => {};
    // One timer at a time, armed for what the measure has left: progress moves an idle wait's end without re-arming.
    const check = () => {
      if (settled) return;
      const left = measure.remaining();
      if (left !== null) {
        cancel = timers.after(left, check);
        return;
      }
      settled = true;
      const failure = measure.failure();
      controller.abort(failure);
      resolve(new Err<E | F>(failure));
    };
    check();
    if (settled) return;
    // A shorter bound than the armed timer's moves the end earlier: arm again for what the measure now has left.
    const rearm = () => {
      cancel();
      check();
    };
    const outcome = body(wait, rearm);
    outcome.then(
      (result) => {
        if (settled) return;
        settled = true;
        cancel();
        resolve(result);
      },
      (error: unknown) => {
        if (!settled) {
          settled = true;
          cancel();
          reject(error);
          return;
        }
        // Past the bound, the body's own abort (the signal's reason, or an AbortError caused by it) is its expected end.
        // Any other rejection is a bug, and it stays an unhandled rejection rather than vanishing behind the answer
        // already given.
        const reason: unknown = controller.signal.reason;
        if (error === reason || (error instanceof Error && error.cause === reason)) return;
        throw error;
      },
    );
  });
}
//#endregion smoo/lmao!n/op-retry.deadlines
