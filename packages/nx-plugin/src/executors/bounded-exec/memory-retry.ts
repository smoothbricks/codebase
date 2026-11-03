/**
 * A run again after the kernel killed the command for memory (specs/cowshed/05_gateway.md, "Host scheduling"). Only
 * when a pool lent the run its tokens and a SIGKILL that no bound of this executor and no signal it sent caused ended
 * it: the command runs once more, alone in its run's pool, so what it was killed beside is not there to be killed with
 * it. This decides whether a command runs a second time and how its two outputs join, never what it runs: the key
 * leaves it out (README, "Bounded exec key").
 */

/** How one run of the command ended. */
export interface RunOutcome {
  success: boolean;
  terminalOutput: string;
  /**
   * The command died of a SIGKILL that no bound of this executor and no signal it sent caused: the kernel's OOM killer,
   * which sends it to a leaf in a run's cgroup. Set on the last run of the command only when it was killed again.
   */
  killedForMemory?: boolean;
}

/** What a run read of how its command ended and what this executor did to it. */
export interface Ending {
  /** This executor sent the command's process group a signal. */
  readonly sentSignal: boolean;
  /** A bound of this executor ended the run. */
  readonly bounded: boolean;
  readonly exitSignal: NodeJS.Signals | null;
  /** The exit status, or the status a signal folds to. */
  readonly code: number;
}

/**
 * A SIGKILL this executor did not send, to a command that no bound of this executor ended, came from outside it. In a
 * run's cgroup the one that sends it to a leaf is the kernel's OOM killer. The exit code alone says nothing of who sent
 * it: a bound's own force-kill ends in 137 too, and is why `sentSignal` is read first.
 */
export function killedFromOutside(ending: Ending): boolean {
  return !ending.sentSignal && !ending.bounded && (ending.exitSignal === 'SIGKILL' || ending.code === 137);
}

/**
 * Run the command; when the kernel killed it for memory and a pool lent it tokens, run it once more alone, and join the
 * two runs' output with a note naming the task.
 */
export async function runUntilNotKilledForMemory(
  run: (alone: boolean) => Promise<RunOutcome>,
  pooled: boolean,
  task: string | undefined,
): Promise<RunOutcome> {
  const first = await run(false);
  if (first.killedForMemory !== true || !pooled) {
    return first;
  }
  const note = `cowshed: the kernel killed the command for memory (a SIGKILL this executor did not send${
    task === undefined ? '' : `: ${task}`
  }); running it once more, alone in its run's pool\n`;
  process.stderr.write(note);
  const again = await run(true);
  return {
    success: again.success,
    terminalOutput: `${first.terminalOutput}${note}${again.terminalOutput}`,
    ...(again.killedForMemory === true ? { killedForMemory: true } : {}),
  };
}
