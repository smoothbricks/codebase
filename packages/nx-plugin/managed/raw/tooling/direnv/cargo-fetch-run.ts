/**
 * The imperative shell around cargo-fetch-bound.ts: it spawns the command in a process group of its own, reads its
 * output, keeps the clock, and kills the group when cargo-fetch-bound.ts says the deadline has passed. Every decision
 * is that file's; this one only measures and acts.
 *
 * The command runs detached (`setsid`), so a signal that ends this process no longer reaches it the way a terminal's
 * would. The shell therefore ends the group itself when this process is ending (a SIGINT, SIGTERM or SIGHUP, or exit),
 * and a fetch never outlives the step that started it. Like secret-references.ts this is a managed raw script that
 * shell entry loads before anything is installed, so it imports only the runtime and its sibling.
 */
import { constants } from 'node:os';
import { judgeBound } from './cargo-fetch-bound.ts';

/** The shell's two uses of time, so a caller can move it by hand. */
export interface Clock {
  /** Milliseconds on a scale that only moves forward; its differences are the elapsed time. */
  now(): number;
  /** Calls `wake` after `ms` milliseconds unless the returned function cancels it first. */
  after(ms: number, wake: () => void): () => void;
}

export const systemClock: Clock = {
  now: () => performance.now(),
  after: (ms, wake) => {
    const timer = setTimeout(wake, ms);
    return () => clearTimeout(timer);
  },
};

export interface BoundedCommand {
  readonly argv: readonly string[];
  readonly cwd: string;
  readonly env: Readonly<Record<string, string>>;
  readonly deadlineMs: number;
  /** Copy the output to this process's own as it arrives, besides capturing it. */
  readonly echo: boolean;
}

export type BoundedOutcome =
  /** The command ended on its own; a signal that ended it is reported as a shell does, 128 plus its number. */
  | { readonly kind: 'exited'; readonly exitCode: number; readonly stdout: Uint8Array; readonly stderr: Uint8Array }
  /**
   * The deadline passed with the command still running: it and everything it started were killed. The output is what
   * was read before the kill; a member that left the process group is beyond the kill's reach.
   */
  | { readonly kind: 'expired'; readonly elapsedMs: number; readonly stdout: Uint8Array; readonly stderr: Uint8Array };

/** The signals that end this process and so must end the command with it. */
const TEARDOWN_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP'] as const;

/**
 * Runs `command` and returns how it ended. Failure to start it rejects; so does a failure to read its output, after
 * the group is killed. Whatever way this returns or throws, the group is never left running: it is killed on every
 * path but the command ending itself.
 */
export async function runBounded(command: BoundedCommand, clock: Clock = systemClock): Promise<BoundedOutcome> {
  let group: number | undefined;
  const killGroup = (): void => {
    if (group !== undefined) signalGroup(group);
  };
  // The hooks come before the spawn: a signal that arrives from the spawn on runs its handler after this synchronous
  // body, by which time the group is known.
  const release = endWithThisProcess(killGroup);
  try {
    const child = Bun.spawn({
      cmd: [...command.argv],
      cwd: command.cwd,
      env: { ...command.env },
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
      detached: true,
    });
    group = child.pid;
    const stdout: Uint8Array[] = [];
    const stderr: Uint8Array[] = [];
    const stdoutReader = child.stdout.getReader();
    const stderrReader = child.stderr.getReader();
    let finished = false;
    const done = Promise.all([
      child.exited,
      pump(stdoutReader, stdout, command.echo ? (chunk) => process.stdout.write(chunk) : null),
      pump(stderrReader, stderr, command.echo ? (chunk) => process.stderr.write(chunk) : null),
    ]).then(() => {
      finished = true;
    });
    const started = clock.now();
    for (;;) {
      const elapsedMs = clock.now() - started;
      const verdict = judgeBound({ deadlineMs: command.deadlineMs, elapsedMs, finished });
      switch (verdict.kind) {
        case 'finished':
          return {
            kind: 'exited',
            exitCode: statusOf(child),
            stdout: Buffer.concat(stdout),
            stderr: Buffer.concat(stderr),
          };
        case 'kill':
          killGroup();
          await child.exited;
          return { kind: 'expired', elapsedMs, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) };
        case 'wait':
          await untilFinishedOrAfter(done, clock, verdict.forMs);
      }
    }
  } catch (error) {
    killGroup();
    throw error;
  } finally {
    release();
  }
}

/** Resolves when `done` does, or `ms` after now on `clock`, whichever is first, leaving no timer behind. */
async function untilFinishedOrAfter(done: Promise<void>, clock: Clock, ms: number): Promise<void> {
  const { promise: elapsed, resolve } = Promise.withResolvers<void>();
  const cancel = clock.after(ms, resolve);
  try {
    await Promise.race([done, elapsed]);
  } finally {
    cancel();
  }
}

/** What the shell asks of the reader of a piped stream: the part every typing of the runtime's readers shares. */
interface PipeReader {
  read(): Promise<{ readonly done: boolean; readonly value?: Uint8Array }>;
}

/** Copies a stream to `sink`, and to `echo` when there is one, until it ends. */
async function pump(reader: PipeReader, sink: Uint8Array[], echo: ((chunk: Uint8Array) => void) | null): Promise<void> {
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return;
    if (value !== undefined) {
      sink.push(value);
      echo?.(value);
    }
  }
}

/** The exit status of a command that has exited: its own, or a shell's 128 plus the number of the signal that ended it. */
function statusOf(child: {
  readonly exitCode: number | null;
  readonly signalCode: NodeJS.Signals | number | null;
}): number {
  if (child.exitCode !== null) return child.exitCode;
  const signal = child.signalCode;
  if (typeof signal === 'number') return 128 + signal;
  // invariant: a command that has exited has an exit code or the signal that ended it.
  if (signal === null) throw new Error('an exited command reports neither an exit code nor a signal');
  return 128 + constants.signals[signal];
}

/**
 * SIGKILL to every process of the group led by `pgid`. A group that is already gone is not an error. Nothing is asked
 * to stop first: a fetch past its deadline is stalled, a member that ignores SIGTERM would go on holding the
 * package-cache lock, and a kill costs a fetch nothing it relies on (the kernel drops the lock, and Cargo's caches
 * tolerate being killed).
 */
function signalGroup(pgid: number): void {
  try {
    process.kill(-pgid, 'SIGKILL');
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
  }
}

/**
 * Has `end` run when this process is ending: on exit, and on a SIGINT, SIGTERM or SIGHUP, which also gives the signal
 * back to the process afterwards so it still ends by that signal. A signal somebody already handles stays theirs.
 * Returns the function that takes the hooks away.
 */
function endWithThisProcess(end: () => void): () => void {
  const hooks: Array<readonly [NodeJS.Signals, () => void]> = [];
  const release = (): void => {
    process.removeListener('exit', end);
    for (const [signal, handler] of hooks) process.removeListener(signal, handler);
    hooks.length = 0;
  };
  process.on('exit', end);
  for (const signal of TEARDOWN_SIGNALS) {
    if (process.listenerCount(signal) !== 0) continue;
    const handler = (): void => {
      end();
      release();
      process.kill(process.pid, signal);
    };
    process.on(signal, handler);
    hooks.push([signal, handler]);
  }
  return release;
}
