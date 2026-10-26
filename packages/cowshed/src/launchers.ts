/// <reference types="node" />

import { type ChildProcess, type StdioOptions, spawn } from 'node:child_process';
import { Readable, Writable } from 'node:stream';
import type * as Api from './api.generated.js';
import { launcherPath, packageRootFromModule } from './launcher.js';
import { CowshedError, type ProtocolViolation } from './types.js';
import * as V from './validators.generated.js';

/**
 * Typed launchers for the verbs whose holder is the process that runs them (07_api.md,
 * "Launchers"): each spawns the verb through the package's `bin/cowshed`, maps the descriptors
 * the verb's argv names to pipes it owns, and decodes the verb's own lines with the generated
 * validators. The CLI is the one implementation; a launcher types it.
 */

/** How a launched cowshed process ended: its exit code, or the signal that ended it. */
export type LauncherStatus =
  | { readonly kind: 'exited'; readonly code: number }
  | { readonly kind: 'signaled'; readonly signal: NodeJS.Signals };

/**
 * A launched process's end: its real status, the first protocol violation its descriptors
 * carried, and a descriptor that could not be read or written, if any. It never rejects.
 */
export type LauncherExit = {
  readonly status: LauncherStatus;
  readonly violation?: ProtocolViolation;
  readonly failure?: string;
};

/** One decoded line: the verb's event, or the violation that ends decoding. */
export type Decoded<T> = { readonly event: T } | { readonly violation: ProtocolViolation };

/**
 * A verb's descriptor protocol, decoded line by line without I/O. A line that is not JSON or
 * fails the generated validator, a line after one the verb sends last, and a last line with no
 * newline at end of file are violations; after the first, nothing more is decoded. End of file
 * after a whole line is never a violation: the verb defines those ends.
 */
export class LineDecoder<T extends { readonly kind: string }> {
  readonly #descriptor: ProtocolViolation['descriptor'];
  readonly #parse: (json: string) => T;
  readonly #last: ReadonlySet<string>;
  #pending: Uint8Array = new Uint8Array(0);
  #lastSeen: string | undefined;
  #violated = false;

  /** The descriptor this decodes. */
  get descriptor(): ProtocolViolation['descriptor'] {
    return this.#descriptor;
  }

  constructor(descriptor: ProtocolViolation['descriptor'], parse: (json: string) => T, last: ReadonlySet<string>) {
    this.#descriptor = descriptor;
    this.#parse = parse;
    this.#last = last;
  }

  push(chunk: Uint8Array): Decoded<T>[] {
    if (this.#violated) {
      return [];
    }
    const bytes = new Uint8Array(this.#pending.length + chunk.length);
    bytes.set(this.#pending);
    bytes.set(chunk, this.#pending.length);
    const decoded: Decoded<T>[] = [];
    let start = 0;
    for (let newline = bytes.indexOf(10, start); newline !== -1; newline = bytes.indexOf(10, start)) {
      const line = bytes.subarray(start, newline);
      start = newline + 1;
      const one = this.#line(line);
      decoded.push(one);
      if ('violation' in one) {
        this.#violated = true;
        this.#pending = new Uint8Array(0);
        return decoded;
      }
    }
    this.#pending = bytes.slice(start);
    return decoded;
  }

  end(): Decoded<T>[] {
    if (this.#violated || this.#pending.length === 0) {
      return [];
    }
    this.#violated = true;
    return [this.#violation(this.#pending, 'a line ends at end of file without its newline')];
  }

  #line(line: Uint8Array): Decoded<T> {
    if (this.#lastSeen !== undefined) {
      return this.#violation(line, `a line after ${this.#lastSeen}, which the verb sends last`);
    }
    let event: T;
    try {
      event = this.#parse(new TextDecoder('utf-8', { fatal: true }).decode(line));
    } catch (error) {
      return this.#violation(
        line,
        `a line that is not the verb's event: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    if (this.#last.has(event.kind)) {
      this.#lastSeen = event.kind;
    }
    return { event };
  }

  #violation(bytes: Uint8Array, reason: string): Decoded<T> {
    return { violation: { descriptor: this.#descriptor, bytes: bytes.slice(), reason } };
  }
}

/** The lock waiter's last lines (`lock` --state-fd). */
export const LOCK_LAST: ReadonlySet<Api.LockWaiterEvent['kind']> = new Set(['failed', 'signalled']);
/** The holder's last line (`hold`). */
export const HOLD_LAST: ReadonlySet<Api.HoldEvent['kind']> = new Set(['failed']);
/** The handle descriptor carries exactly one admission, so every record is its last. */
export const ADMISSION_LAST: ReadonlySet<Api.JobSubtreeAdmission['kind']> = new Set(['admitted', 'refused']);

/**
 * Whether a lock waiter takes the owner-cancellation request now, from what its state channel
 * has said so far: only once it said `queued`, and before it said anything that ends the wait.
 * cowshed watches SIGUSR1 only from the moment it queues.
 */
export function canCancelOwner(said: ReadonlySet<Api.LockWaiterEvent['kind']>): boolean {
  const ending: readonly Api.LockWaiterEvent['kind'][] = ['acquired', 'failed', 'signalled', 'commandFailed'];
  return said.has('queued') && !ending.some((kind) => said.has(kind));
}

/**
 * One descriptor's events, read and decoded to its end whatever the consumer does, so the verb
 * never writes into a closed pipe and a violation found after the consumer left is still kept.
 * The `integrity` error the iterator throws carries the violation.
 */
export class DescriptorEvents<T extends { readonly kind: string }> implements AsyncIterableIterator<T> {
  readonly #queue: T[] = [];
  readonly #said = new Set<T['kind']>();
  readonly #onEvent: (said: ReadonlySet<T['kind']>) => void;
  readonly #onEnd: () => void;
  #waiter: PromiseWithResolvers<IteratorResult<T>> | undefined;
  #ended = false;
  #abandoned = false;
  #error: CowshedError | undefined;
  violation: ProtocolViolation | undefined;
  failure: string | undefined;
  readonly drained: Promise<void>;

  constructor(
    stream: Readable,
    decoder: LineDecoder<T>,
    hooks: {
      onEvent?: (said: ReadonlySet<T['kind']>) => void;
      /** The descriptor can no longer be trusted or read: a protocol violation, or a read failure. */
      onLost?: () => void;
      onAbandon?: () => void;
    },
  ) {
    this.#onEvent = hooks.onEvent ?? (() => {});
    this.#onEnd = hooks.onAbandon ?? (() => {});
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the launched verb holds its event descriptor close-on-exec (lock.rs, hold.rs, subtree.rs), so no process it starts keeps it and the descriptor closes when the verb ends
    const { promise, resolve } = Promise.withResolvers<void>();
    this.drained = promise;
    const take = (decoded: Decoded<T>[]) => {
      for (const one of decoded) {
        if ('violation' in one) {
          this.violation ??= one.violation;
          this.#error ??= new CowshedError(
            'integrity',
            `cowshed's ${one.violation.descriptor} descriptor broke its protocol: ${one.violation.reason}`,
            'report the bytes this error carries; the run itself ended as its exit says',
            { violation: one.violation },
          );
          hooks.onLost?.();
        } else {
          this.#said.add(one.event.kind);
          this.#onEvent(this.#said);
          if (!this.#abandoned) {
            this.#queue.push(one.event);
          }
        }
      }
      this.#wake();
    };
    stream.on('data', (chunk: Buffer) => take(decoder.push(chunk)));
    stream.once('error', (error: Error) => {
      const failure = `cowshed's ${decoder.descriptor} descriptor could not be read: ${error.message}`;
      this.failure ??= failure;
      this.#error ??= new CowshedError('internal', failure, 'the run itself ended as its exit says');
      hooks.onLost?.();
    });
    stream.once('close', () => {
      take(decoder.end());
      this.#ended = true;
      this.#wake();
      resolve();
    });
  }

  /** The kinds the descriptor has said so far, whether or not the consumer read them. */
  get said(): ReadonlySet<T['kind']> {
    return this.#said;
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  next(): Promise<IteratorResult<T>> {
    const event = this.#queue.shift();
    if (event !== undefined) {
      return Promise.resolve({ done: false, value: event });
    }
    if (this.#error !== undefined) {
      const error = this.#error;
      return Promise.reject(error);
    }
    if (this.#ended || this.#abandoned) {
      return Promise.resolve({ done: true, value: undefined });
    }
    this.#waiter = Promise.withResolvers<IteratorResult<T>>();
    return this.#waiter.promise;
  }

  return(): Promise<IteratorResult<T>> {
    if (!this.#abandoned) {
      this.#abandoned = true;
      this.#queue.length = 0;
      this.#onEnd();
    }
    this.#wake();
    return Promise.resolve({ done: true, value: undefined });
  }

  #wake(): void {
    const waiter = this.#waiter;
    if (waiter === undefined) {
      return;
    }
    this.#waiter = undefined;
    this.next().then(waiter.resolve, waiter.reject);
  }
}

/** Start the verb through the package launcher; resolves once the process runs. */
async function launch(argv: readonly string[], stdio: StdioOptions): Promise<ChildProcess> {
  const launcher = launcherPath(packageRootFromModule(import.meta.url));
  const child = spawn(launcher, [...argv], { stdio });
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: Node emits 'spawn' or 'error' once the exec of the launcher returns; no other process is awaited
  const { promise, resolve, reject } = Promise.withResolvers<ChildProcess>();
  child.once('spawn', () => resolve(child));
  child.once('error', (error) =>
    reject(
      new CowshedError(
        'environment-missing',
        `cannot start ${launcher}: ${error.message}`,
        "build or install the package's cowshed binary (06_cli.md, Launcher)",
      ),
    ),
  );
  return promise;
}

/** The process's real status once it has exited. */
function status(child: ChildProcess): Promise<LauncherStatus> {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a launched cowshed verb (a lock wait, a hold, a run) lasts as long as its work; the operator's interrupt or the enclosing runner's bound ends a hung one
  return new Promise((resolve, reject) => {
    child.once('exit', (code: number | null, signal: NodeJS.Signals | null) => {
      if (signal !== null) {
        resolve({ kind: 'signaled', signal });
      } else if (code !== null) {
        resolve({ kind: 'exited', code });
      } else {
        reject(new CowshedError('internal', 'Node reported an exit with neither a code nor a signal', 'report this'));
      }
    });
  });
}

/** The pipe Node opened for descriptor `index`, as the direction the verb uses it. */
function readable(child: ChildProcess, index: number): Readable {
  const stream = child.stdio[index];
  if (stream instanceof Readable) {
    return stream;
  }
  throw new CowshedError('internal', `descriptor ${index} of the launched verb is not a readable pipe`, 'report this');
}

function writable(child: ChildProcess, index: number): Writable {
  const stream = child.stdio[index];
  if (stream instanceof Writable) {
    return stream;
  }
  throw new CowshedError('internal', `descriptor ${index} of the launched verb is not a writable pipe`, 'report this');
}

/** The run's end: its status once it exited and every descriptor it owns was read to its end. */
async function ended(
  child: ChildProcess,
  descriptors: readonly {
    readonly drained: Promise<void>;
    readonly violation?: ProtocolViolation;
    readonly failure?: string;
  }[],
  failures: () => readonly string[],
): Promise<LauncherExit> {
  const exited = await status(child);
  await Promise.all(descriptors.map((descriptor) => descriptor.drained));
  const violation = descriptors.find((descriptor) => descriptor.violation !== undefined)?.violation;
  const failure = [
    ...descriptors.flatMap((descriptor) => (descriptor.failure === undefined ? [] : [descriptor.failure])),
    ...failures(),
  ].join('; ');
  return {
    status: exited,
    ...(violation === undefined ? {} : { violation }),
    ...(failure === '' ? {} : { failure }),
  };
}

/** `lock <path> [--owned-pid <pid>…] -- <argv…>`. */
export type SpawnLockOptions = {
  readonly path: string;
  readonly ownedPids?: readonly number[];
  readonly argv: readonly [string, ...string[]];
};

/** A running `cowshed lock`. No member reaches the process except the verb's own requests. */
export interface LockRun {
  /** The waiter's `LockWaiterEvent` lines (`--state-fd`). */
  readonly state: AsyncIterableIterator<Api.LockWaiterEvent>;
  /**
   * The `--owned-pid` request: SIGUSR1 while queued. Answered on `state` with `notSignalled`
   * or `signalled`. Throws `usage`, sending nothing, unless the waiter has said `queued` and
   * nothing that ends the wait.
   */
  cancelOwner(): void;
  /** INT, HUP or TERM to cowshed, forwarded to the command once it holds the lock. */
  forward(signal: 'INT' | 'HUP' | 'TERM'): void;
  readonly exit: Promise<LauncherExit>;
}

export async function spawnLock(options: SpawnLockOptions): Promise<LockRun> {
  const owned = (options.ownedPids ?? []).flatMap((pid) => ['--owned-pid', String(pid)]);
  const child = await launch(
    ['lock', options.path, '--state-fd', '3', ...owned, '--', ...options.argv],
    ['inherit', 'inherit', 'inherit', 'pipe'],
  );
  const forward = (signal: 'INT' | 'HUP' | 'TERM') => {
    child.kill(`SIG${signal}`);
  };
  const state = new DescriptorEvents<Api.LockWaiterEvent>(
    readable(child, 3),
    new LineDecoder('state', V.parseLockWaiterEvent, LOCK_LAST),
    {
      onLost: () => forward('TERM'),
    },
  );
  return {
    state,
    cancelOwner() {
      if (!canCancelOwner(state.said)) {
        throw new CowshedError(
          'usage',
          'cancelOwner is the request of a waiter that is queued, and this one is not',
          'wait for its queued event, before anything that ends the wait',
        );
      }
      child.kill('SIGUSR1');
    },
    forward,
    exit: ended(child, [state], () => []),
  };
}

/** `hold -- <argv…>`. */
export type SpawnHoldOptions = { readonly argv: readonly [string, ...string[]] };

/** The `hold` control lines (`--control-fd`). */
export interface HoldControl {
  /** `signal <SIG>` to the leader's whole group; answered on `state`. */
  signal(signal: Api.HoldSignal): void;
  /** `release`: cowshed reaps the leader once it has exited. */
  release(): void;
  /**
   * End the control descriptor. Before a `release` this is the controller end of file that tears
   * the held group down (SIGTERM, a grace, then SIGKILL, each answered on `state`); after one it
   * only closes the channel. Idempotent.
   */
  close(): void;
}

/** A running `cowshed hold`: the group is reached only through `control`. */
export interface HoldRun {
  readonly control: HoldControl;
  /** The holder's `HoldEvent` lines (`--state-fd`). */
  readonly state: AsyncIterableIterator<Api.HoldEvent>;
  readonly exit: Promise<LauncherExit>;
}

export async function spawnHold(options: SpawnHoldOptions): Promise<HoldRun> {
  const child = await launch(
    ['hold', '--control-fd', '3', '--state-fd', '4', '--', ...options.argv],
    ['inherit', 'inherit', 'inherit', 'pipe', 'pipe'],
  );
  const pipe = writable(child, 3);
  const failures: string[] = [];
  pipe.once('error', (error: Error) =>
    failures.push(`cowshed's control descriptor could not be written: ${error.message}`),
  );
  let released = false;
  let closed = false;
  const write = (line: string) => {
    if (closed) {
      throw new CowshedError('usage', 'the hold control descriptor is closed', 'open a new hold');
    }
    pipe.write(`${line}\n`);
  };
  const control: HoldControl = {
    signal: (signal) => write(`signal ${signal}`),
    release: () => {
      write('release');
      released = true;
    },
    close: () => {
      if (!closed) {
        closed = true;
        pipe.end();
      }
    },
  };
  // A held group nobody can observe any more is torn down, never left held.
  const unobserved = () => {
    if (!released) {
      control.close();
    }
  };
  const state = new DescriptorEvents<Api.HoldEvent>(
    readable(child, 4),
    new LineDecoder('state', V.parseHoldEvent, HOLD_LAST),
    {
      onLost: unobserved,
      onAbandon: unobserved,
    },
  );
  return { control, state, exit: ended(child, [state], () => failures) };
}

/** What `subtree-root`'s handle descriptor carried. */
export type SubtreeAdmissionOutcome =
  | { readonly kind: 'record'; readonly admission: Api.JobSubtreeAdmission }
  /**
   * Not exactly one well-formed record: the verb's loud admission or transport failure, or a
   * malformed, truncated or second record. Never a refusal: the command may already run admitted.
   */
  | { readonly kind: 'unknown'; readonly violation?: ProtocolViolation };

/** `subtree-root -- <argv…>`. */
export type SpawnSubtreeRootOptions = { readonly argv: readonly [string, ...string[]] };

/** A running `cowshed subtree-root`, which execs its command in its own life. */
export interface SubtreeRootRun {
  /** The one `JobSubtreeAdmission` the verb writes to `--handle-fd`. */
  readonly admission: Promise<SubtreeAdmissionOutcome>;
  readonly exit: Promise<LauncherExit>;
}

export async function spawnSubtreeRoot(options: SpawnSubtreeRootOptions): Promise<SubtreeRootRun> {
  const child = await launch(
    ['subtree-root', '--handle-fd', '3', '--', ...options.argv],
    ['inherit', 'inherit', 'inherit', 'pipe'],
  );
  const handle = new DescriptorEvents<Api.JobSubtreeAdmission>(
    readable(child, 3),
    new LineDecoder('handle', V.parseJobSubtreeAdmission, ADMISSION_LAST),
    {},
  );
  const admission = (async (): Promise<SubtreeAdmissionOutcome> => {
    const records: Api.JobSubtreeAdmission[] = [];
    try {
      for await (const record of handle) {
        records.push(record);
      }
    } catch {
      // The violation is the outcome, carried below.
    }
    const [record] = records;
    if (handle.violation !== undefined || record === undefined) {
      return handle.violation === undefined ? { kind: 'unknown' } : { kind: 'unknown', violation: handle.violation };
    }
    return { kind: 'record', admission: record };
  })();
  return { admission, exit: ended(child, [handle], () => []) };
}

/** `land-turn <repo> [--workspace] [--outside-lock] [--gateway-socket] -- <argv…>`. */
export type SpawnLandTurnOptions = {
  readonly repo: string;
  readonly workspace?: string;
  readonly outsideLock?: string;
  /** The gateway whose queue to join; absent asks the host gateway. */
  readonly gatewaySocket?: string;
  readonly argv: readonly [string, ...string[]];
};

/** A running `cowshed land-turn`: the turn is held until its child ends. */
export interface LandTurnRun {
  readonly exit: Promise<LauncherExit>;
}

export async function spawnLandTurn(options: SpawnLandTurnOptions): Promise<LandTurnRun> {
  const child = await launch(
    [
      'land-turn',
      options.repo,
      ...(options.workspace === undefined ? [] : ['--workspace', options.workspace]),
      ...(options.outsideLock === undefined ? [] : ['--outside-lock', options.outsideLock]),
      ...(options.gatewaySocket === undefined ? [] : ['--gateway-socket', options.gatewaySocket]),
      '--',
      ...options.argv,
    ],
    'inherit',
  );
  return { exit: ended(child, [], () => []) };
}

/** The message of `doc-commit`: given (`-m`), or read from a file (`-F`, `-` for stdin). */
export type DocCommitMessage =
  | { readonly message: string; readonly messageFile?: never }
  | { readonly messageFile: string; readonly message?: never };

/** The edit of `doc-commit`: a `git apply` patch or an ICM program file (`-` for stdin). */
export type DocCommitEdit =
  | { readonly patch: string; readonly icm?: never }
  | { readonly icm: string; readonly patch?: never };

/** `doc-commit (-m | -F) (--patch | --icm) [--gateway-socket]`, in main's checkout `project`. */
export type SpawnDocCommitOptions = {
  readonly project?: string;
  /** The gateway whose ref turn to take; absent asks the host gateway. */
  readonly gatewaySocket?: string;
} & DocCommitMessage &
  DocCommitEdit;

/** A running `cowshed doc-commit`: the ref turn is held until it ends; its stdout is the commit. */
export interface DocCommitRun {
  readonly exit: Promise<LauncherExit>;
}

export async function spawnDocCommit(options: SpawnDocCommitOptions): Promise<DocCommitRun> {
  const child = await launch(
    [
      ...(options.project === undefined ? [] : ['--project', options.project]),
      'doc-commit',
      ...(options.message === undefined ? ['-F', options.messageFile] : ['-m', options.message]),
      ...(options.patch === undefined ? ['--icm', options.icm] : ['--patch', options.patch]),
      ...(options.gatewaySocket === undefined ? [] : ['--gateway-socket', options.gatewaySocket]),
    ],
    'inherit',
  );
  return { exit: ended(child, [], () => []) };
}
