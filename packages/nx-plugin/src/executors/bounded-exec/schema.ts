export interface BoundedExecOptions {
  command: string;
  cwd?: string;
  env?: Record<string, string>;
  timeoutMs: number;
  /**
   * Kill the process tree when it has produced no stdout or stderr for this
   * long. Absent means no progress bound.
   *
   * This is the bound that catches a hang. A wedged process — a flock deadlock
   * between two cargo writers, a transformer waiting on a lock nobody will
   * release, a runner that ignores its own per-test timeout — emits nothing at
   * all, so silence is the signal. Unlike `timeoutMs` it does not scale with
   * machine speed or load: a machine ten times slower still prints progress,
   * just further apart, and each gap is bounded by the slowest single unit of
   * work rather than by the total.
   */
  idleTimeoutMs?: number;
  killAfterMs?: number;
  /**
   * The most threads or processes the command runs at once, which is how many CPU tokens it asks
   * the cowshed host budget for. Absent, the runner in the command decides: nextest and cargo one
   * per core, `bun test --parallel=N` N, anything else one. The grant reaches the command as
   * `BOUNDED_EXEC_CPU_TOKENS`.
   */
  parallelism?: number;
  forwardAllArgs?: boolean;
  args?: string | string[];
  __unparsed__?: string[];
}
