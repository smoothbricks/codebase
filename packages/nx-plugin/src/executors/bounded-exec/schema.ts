/**
 * A command and the bounds that end it. At least one bound is required: a run with neither would block Nx forever.
 * A test target names only `idleTimeoutMs` (`bounded-test-policy.ts`); `timeoutMs` is for commands that report no
 * tests, whose only progress is exiting.
 */
export type BoundedExecOptions = BoundedExecCommand & BoundedExecBounds;

export type BoundedExecBounds =
  | { timeoutMs: number; idleTimeoutMs?: number }
  | { timeoutMs?: undefined; idleTimeoutMs: number };

export interface BoundedExecCommand {
  command: string;
  cwd?: string;
  env?: Record<string, string>;
  /** Absolute wall-clock ceiling: a backstop against an unbounded run, not a performance budget. */
  timeoutMs?: number;
  /**
   * Kill the process tree when no test has finished for this long (`progress.ts`). Absent means no progress bound.
   *
   * This is the bound that catches a hang. A hung test never finishes, whatever its runner keeps printing about it,
   * and the tests still running when it fires are named in the verdict. Unlike `timeoutMs` it does not scale with
   * the size of the suite or the load on the machine: a machine ten times slower still finishes tests, just further
   * apart, and each gap is bounded by the slowest single test rather than by the total. A command that reports no
   * tests makes progress only by exiting.
   */
  idleTimeoutMs?: number;
  killAfterMs?: number;
  /**
   * The most threads or processes the command runs at once, which is how many CPU tokens it asks
   * its live parent pool for. Absent, the runner in the command decides:
   * nextest and cargo one per core, `bun test --parallel=N` N, anything else one. The actual grant
   * reaches the command as `BOUNDED_EXEC_CPU_TOKENS`, with its sub-pool in `BOUNDED_EXEC_CPU_POOL`.
   */
  parallelism?: number;
  forwardAllArgs?: boolean;
  args?: string | string[];
  __unparsed__?: string[];
}
