/**
 * Ending a process group: a decision over facts, and the shell that carries it out.
 *
 * A group signal reaches the members that exist when it goes out, and macOS does not hold a fork back while it does, so
 * a member forked in that moment survives the signal, and so does one that member forks next. A kill that must leave
 * nothing behind is therefore repeated while the kernel still lists members of the group. `judgeReap` decides whether
 * to, over facts, with no clock, process or environment of its own. The rest of this file measures and acts: one probe,
 * one signal, and the one loop, driven synchronously by a caller that can wait for nothing else and asynchronously by
 * one that can.
 *
 * The managed raw script `tooling/direnv/cargo-fetch-run.ts` runs before anything is installed and so cannot import
 * this file; the `cargo-fetch-bound.ts` beside it holds the same decision.
 */

/**
 * How long a kill goes on: the group is signalled at most `rounds` times, and `pauseMs` milliseconds are waited after
 * each.
 */
export interface Patience {
  readonly rounds: number;
  readonly pauseMs: number;
}

/**
 * For a kill on the way out of this process, which can wait for nothing and should hold the exit up for as little as it
 * can: signals a millisecond apart, thirty at most. Rounds with no pause do not do it: a fork already under way when
 * the first signal goes out joins the group after they have all run.
 */
export const TEARDOWN_PATIENCE: Patience = { rounds: 30, pauseMs: 1 };

/**
 * For the kill at a deadline, whose outcome is told: signals 5 ms apart for up to two seconds. A host under load can
 * take that long to finish exiting what it was sent, and the group lists a member until it has.
 */
export const DEADLINE_PATIENCE: Patience = { rounds: 400, pauseMs: 5 };

/** What the shell knows of the group when it asks whether to signal it again. */
export interface ReapFacts {
  /** Signals sent to the group so far. */
  readonly round: number;
  readonly patience: Patience;
  /** The kernel still lists a member of the group. */
  readonly members: boolean;
}

export type ReapVerdict =
  /** Nothing is left in the group. */
  | { readonly kind: 'empty' }
  /** Signal the group again. */
  | { readonly kind: 'signal' }
  /** The patience is spent and the group still lists members. */
  | { readonly kind: 'members-left' };

/**
 * Whether to signal a group again: while it lists members, until it has none or the patience is spent. Patience that is
 * not a positive finite number of rounds ahead is spent, so no input makes the signalling unbounded.
 */
export function judgeReap({ round, patience, members }: ReapFacts): ReapVerdict {
  if (!members) return { kind: 'empty' };
  const roundsLeft = patience.rounds - round;
  return roundsLeft > 0 && Number.isFinite(roundsLeft) ? { kind: 'signal' } : { kind: 'members-left' };
}

/**
 * What is told of a process group that a kill waiting on the event loop could not empty. That wait reaps the group's
 * leader when it is this process's child, so the leader is not what is still listed. A process that left the group is
 * beyond a group kill, and one that exited and has not been reaped yet is listed like a live one.
 */
export function describeMembersLeft(group: number): string {
  return [
    `process group ${group} still had members when the kill gave up: one that left the group is beyond a group kill,`,
    'and one that exited and has not been reaped yet looks the same.',
  ].join(' ');
}

/** What a kill that blocks this process has to tell. */
export interface TeardownFacts {
  /** The command as a person would type it. */
  readonly command: string;
  /** The process group the command ran in, which the kernel still listed when the kill gave up. */
  readonly group: number;
}

/**
 * What is told of a process group that a kill blocking this process (`killProcessGroupNow`) left listed. That kill
 * waits for nothing, not even for the group's leader to be reaped, and the kernel lists the leader as a member of its
 * own group until then however dead it is. So what is left is listed, and nothing says that a process in it runs.
 */
export function describeTeardown({ command, group }: TeardownFacts): string {
  return [
    `While ending ${command}, process group ${group} was still listed when the kill gave up;`,
    'whether a process in it still runs is unconfirmed, because its leader is listed until this process reaps it,',
    'which it cannot do while it waits for the kill.',
  ].join(' ');
}

/** The errno code of a failed system call, when it has one. */
function errorCode(error: unknown): string | undefined {
  return error instanceof Error && 'code' in error && typeof error.code === 'string' ? error.code : undefined;
}

/**
 * Whether the kernel lists a member of the group led by `pgid`. ESRCH proves the group is empty. EPERM proves a member
 * exists that this process may not signal, such as one that exited and has not been reaped, which macOS reports that
 * way. Any other answer proves neither and is a failure, not a verdict. A group outlives its reaped leader for as long
 * as one member does, and no process can take its id meanwhile. Windows has no process groups, so nothing is left to
 * name there.
 */
export function processGroupHasMembers(pgid: number): boolean {
  if (process.platform === 'win32') return false;
  try {
    process.kill(-pgid, 0);
    return true;
  } catch (error) {
    const code = errorCode(error);
    if (code === 'ESRCH') return false;
    if (code === 'EPERM') return true;
    throw error;
  }
}

/**
 * `signal` to every process of the group led by `pgid`, which on POSIX is a command spawned detached: only the group
 * is named, since the leader may already be reaped, and its pid reused by an unrelated process, while the group lives
 * on in its other members. A group that is already gone is not an error, nor is one with no member left that can be
 * signalled; any other failure is. Windows has no process groups, so the process itself is signalled.
 */
export function signalProcessGroup(pgid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(process.platform === 'win32' ? pgid : -pgid, signal);
  } catch (error) {
    const code = errorCode(error);
    if (code !== 'ESRCH' && code !== 'EPERM') throw error;
  }
}

/**
 * The rounds of a kill: the group is signalled first, whatever the kernel lists, and each later round asks `judgeReap`.
 * Each time it yields, the caller signals the group and waits `patience.pauseMs`. It returns whether members were left
 * when the patience ran out. The loop is written once; the callers differ only in how they wait.
 */
function* killRounds(pgid: number, patience: Patience): Generator<void, boolean, void> {
  yield;
  for (let round = 1; ; round += 1) {
    const verdict = judgeReap({ round, patience, members: processGroupHasMembers(pgid) });
    if (verdict.kind === 'empty') return false;
    if (verdict.kind === 'members-left') return true;
    yield;
  }
}

function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Kills the group led by `pgid` and keeps killing it while it has members, blocking this process between the signals,
 * for a caller that can wait for nothing else: an exit hook, a signal handler, a failure path. Until this process reaps
 * the group's leader the kernel lists the leader as a member of its own group, so a group that has lost everything but
 * its leader is reported as having members. Returns whether members were left when the patience ran out, which only
 * `describeTeardown` may tell, since it cannot say that anything but the leader is.
 */
export function killProcessGroupNow(pgid: number, patience: Patience): boolean {
  const kill = killRounds(pgid, patience);
  for (let step = kill.next(); ; step = kill.next()) {
    if (step.done) return step.value;
    signalProcessGroup(pgid, 'SIGKILL');
    sleepSync(patience.pauseMs);
  }
}

/**
 * Waits `ms` of host time without blocking this thread, so that the event loop serves everything else meanwhile and
 * reaps the group's leader. Two waits, and whichever ends first ends the pause: a timer, which keeps the process alive
 * while it waits, and `Atomics.waitAsync`, which does not (a process with nothing else pending exits under it, as
 * measured on Node 26 and Bun 1.4.3) but is not a timer. A timer alone is its caller's clock, and a test that fakes the
 * clock stops it for good, while what a kill waits for is the kernel.
 */
function pause(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    const wait = Atomics.waitAsync(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
    const end = (): void => {
      clearTimeout(timer);
      resolve();
    };
    if (wait.async) void wait.value.then(end);
    else end();
  });
}

/**
 * Kills the group led by `pgid` and keeps killing it while it has members, pausing between the signals without
 * blocking (`pause`), so that the event loop reaps the leader meanwhile. `signal` sends each SIGKILL; it is
 * `signalProcessGroup` unless the caller owns how a group is signalled. Returns whether members were left when the
 * patience ran out, which `describeMembersLeft` tells.
 */
export async function killProcessGroup(
  pgid: number,
  patience: Patience,
  signal: (pgid: number) => Promise<void> | void = (group) => signalProcessGroup(group, 'SIGKILL'),
): Promise<boolean> {
  const kill = killRounds(pgid, patience);
  for (let step = kill.next(); ; step = kill.next()) {
    if (step.done) return step.value;
    await signal(pgid);
    await pause(patience.pauseMs);
  }
}
