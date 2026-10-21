/**
 * The bound on `cargo fetch --locked`, as decisions over facts. Pure: no clock, no process, no environment and no file
 * of its own. cargo-fetch-run.ts is the shell that measures time, spawns, signals and reads the environment, and asks
 * here what to do; setup-environment.ts reports what comes back.
 *
 * While `cargo fetch` runs it holds Cargo's global package-cache lock, `$CARGO_HOME/.package-cache`, and every other
 * cargo that shares the CARGO_HOME waits behind it. Git has no low-speed limit unless one is configured, so the fetch of
 * a git dependency whose connection stalls holds that lock for as long as the connection stays up. A fetch therefore has
 * two bounds:
 *
 * - git drops a transfer that has all but stopped for GIT_HTTP_LOW_SPEED_TIME_S seconds (`boundedFetchEnv`). Cargo runs
 *   the `git` executable for git dependencies unless `net.git-fetch-with-cli = false`, so this reaches the HTTP(S)
 *   transfers of its fetches and not an ssh or git-protocol one, nor Cargo's own git library; and
 * - the whole fetch, with every process it started, is killed when its deadline passes (`judgeBound`), which bounds
 *   whatever the first does not reach.
 *
 * This file is a managed raw script like secret-references.ts: shell entry loads it before anything is installed, so
 * it imports nothing.
 */

/**
 * The longest one `cargo fetch --locked` may run. It is the bound the Nx plugin puts on each Cargo child of project-graph
 * resolution (CARGO_RESOLVE_BOUND_MS), for the same reason: a Cargo wait without a bound stops every Cargo behind it.
 */
export const CARGO_FETCH_DEADLINE_MS = 300_000;

/**
 * Names a deadline shorter than CARGO_FETCH_DEADLINE_MS, in whole milliseconds. It only shortens: a fetch that needs
 * longer is failing, and the cure is its network, not a longer hold on the lock.
 */
export const CARGO_FETCH_DEADLINE_ENV = 'SMOO_CARGO_FETCH_DEADLINE_MS';

/**
 * Git's low-speed abort, which git leaves off: a transfer that moves under this many bytes a second for
 * GIT_HTTP_LOW_SPEED_TIME_S seconds is dropped. One byte a second is the least git accepts (it ignores 0), so only a
 * transfer that has all but stopped is dropped, and one that is slow but moving is not. A server that prepares a pack
 * in silence sends only a five-byte keepalive every five seconds (`uploadpack.keepAlive`), which is under the limit
 * too: that silence counts as the stall it looks like once the window has passed.
 */
export const GIT_HTTP_LOW_SPEED_LIMIT = 1;

/**
 * The window of the limit, in seconds. It has to outlast the pack a server may prepare in silence, since Cargo runs
 * `git fetch --quiet` and the server then sends nothing else until the pack starts. A stall is dropped this long after
 * its last byte, and up to five seconds later, because libcurl averages the speed over five seconds.
 */
export const GIT_HTTP_LOW_SPEED_TIME_S = 60;

export type FetchDeadline =
  | { readonly kind: 'deadline'; readonly ms: number }
  | { readonly kind: 'refused'; readonly message: string };

const WHOLE_MILLISECONDS = /^[1-9][0-9]*$/;

/** The deadline `raw`, the value of CARGO_FETCH_DEADLINE_ENV, asks for, or why it asks for none. Unset is the default. */
export function fetchDeadline(raw: string | undefined): FetchDeadline {
  if (raw === undefined) return { kind: 'deadline', ms: CARGO_FETCH_DEADLINE_MS };
  const ms = WHOLE_MILLISECONDS.test(raw) ? Number(raw) : Number.NaN;
  if (!Number.isSafeInteger(ms)) {
    return {
      kind: 'refused',
      message: `${CARGO_FETCH_DEADLINE_ENV}=${JSON.stringify(raw)} is not a positive whole number of milliseconds`,
    };
  }
  if (ms > CARGO_FETCH_DEADLINE_MS) {
    return {
      kind: 'refused',
      message: `${CARGO_FETCH_DEADLINE_ENV}=${raw} would lengthen the ${CARGO_FETCH_DEADLINE_MS} ms deadline; it only shortens it`,
    };
  }
  return { kind: 'deadline', ms };
}

/**
 * The environment the fetch runs in: `env` with git's low-speed limit set. Cargo runs `git fetch` for git dependencies
 * and git reads these variables ahead of its configuration, so a limit the caller or a git config already holds gives
 * way to this one: the bound is the step's, not the ambient environment's. The variables are carried as own properties
 * (`Object.fromEntries`, a spread): assigning by name would lose one called `__proto__`.
 */
export function boundedFetchEnv(env: Readonly<Record<string, string | undefined>>): Record<string, string> {
  const carried = Object.entries(env).filter((entry): entry is [string, string] => entry[1] !== undefined);
  return {
    ...Object.fromEntries(carried),
    GIT_HTTP_LOW_SPEED_LIMIT: String(GIT_HTTP_LOW_SPEED_LIMIT),
    GIT_HTTP_LOW_SPEED_TIME: String(GIT_HTTP_LOW_SPEED_TIME_S),
  };
}

/** What the shell knows about the command when it asks for a verdict. */
export interface BoundFacts {
  /** The deadline, in milliseconds after the command started. */
  readonly deadlineMs: number;
  /** Milliseconds since the command started, read by the shell from its clock. */
  readonly elapsedMs: number;
  /** The command has exited and closed its output. */
  readonly finished: boolean;
}

export type BoundVerdict =
  | { readonly kind: 'finished' }
  /** Ask again after this many milliseconds, or when the command finishes. */
  | { readonly kind: 'wait'; readonly forMs: number }
  /** Kill the command and everything it started. */
  | { readonly kind: 'kill' };

/**
 * What to do about a command now. A finished command is left alone however late it finished. A running one is waited
 * for until its deadline and killed from then on. Time that is not a number before a finite deadline is a command past
 * it: no input makes the wait unbounded.
 */
export function judgeBound({ deadlineMs, elapsedMs, finished }: BoundFacts): BoundVerdict {
  if (finished) return { kind: 'finished' };
  const forMs = deadlineMs - elapsedMs;
  if (forMs > 0 && Number.isFinite(forMs)) return { kind: 'wait', forMs };
  return { kind: 'kill' };
}

/** What the failure of a command that outlived its deadline is told. */
export interface DeadlineFacts {
  /** The command as a person would type it. */
  readonly command: string;
  readonly deadlineMs: number;
  readonly elapsedMs: number;
  /** Cargo's package-cache lock file, which the command held while it ran. */
  readonly lockPath: string;
}

/** The failure of a command that outlived its deadline, naming the command and the lock it held. */
export function describeDeadline({ command, deadlineMs, elapsedMs, lockPath }: DeadlineFacts): string {
  return [
    `${command} was still running ${elapsedMs} ms after it started, past its ${deadlineMs} ms deadline, and was`,
    `killed with every process it started. While it runs it holds Cargo's package-cache lock (${lockPath}), which makes`,
    'every other cargo that shares this CARGO_HOME wait behind it, so a stalled fetch is ended rather than waited out.',
    "Cargo's output up to the kill follows.",
  ].join('\n');
}
