import type { tags } from 'typia';

/**
 * The CPU a run's tree used while its idle bound counted down. A test that runs a compiler is a build inside the test,
 * which is a defect and not a test (specs/cowshed/08_testing.md, "Integration tests (real substrate)"): the pre-signal
 * snapshot names the processes whose CPU time rose between its two readings of the tree, so the expiry fails as that and
 * not as an anonymous hang. CPU never restarts the idle bound, which counts only finished tests and the progress a
 * running test reports (progress-channel.ts); this says what ran.
 */

/** What a rising process makes of the expiry, said in the same words by the snapshot and by the verdict. */
export const CPU_RISING_CLASS = "a process in the test's tree was using CPU: likely a build inside the test";

/**
 * The least gain that names a rise: 10% of a core over the snapshot's five seconds between its readings. procinfo reads
 * CPU time to the kernel's clock tick (10 ms on Linux) or finer, so one bar holds every reading.
 */
export const CPU_RISE_MS = 500;

/** The longest command a rise keeps: a compiler's argument list runs to kilobytes, and its name leads it. */
const COMMAND_CHARS = 160;

/** A process whose CPU time rose between the snapshot's two readings. */
export interface CpuRise {
  readonly pid: number & tags.Type<'uint32'> & tags.Minimum<1>;
  readonly command: string;
  readonly gainedMs: number & tags.Minimum<0>;
}

/** One process of a reading of the tree, as far as CPU is concerned. */
export interface CpuSample {
  readonly pid: number;
  /** When it started, in microseconds since the epoch (procinfo's `startedUs`): with its pid, one process. */
  readonly startedUs: number | null;
  readonly command: string;
  /** The CPU time it has used, user and system, in milliseconds; null when it was not read. */
  readonly cpuMs: number | null;
}

export interface CpuChange {
  /** Largest gain first. */
  readonly rising: readonly CpuRise[];
  /** Processes in both readings that one of them read no CPU time or no start for: not counted idle. */
  readonly unread: readonly number[];
  /** How many processes had a CPU time read in both readings, so were compared. */
  readonly compared: number;
}

/**
 * The processes present in both readings whose CPU time rose by at least {@link CPU_RISE_MS}. A pid in both is one
 * process only when both readings name the same start: a reused pid is a new process, compared with nothing.
 */
export function cpuChange(before: readonly CpuSample[], after: readonly CpuSample[]): CpuChange {
  const earlier = new Map(before.map((sample) => [sample.pid, sample]));
  const rising: CpuRise[] = [];
  const unread: number[] = [];
  let compared = 0;
  for (const sample of after) {
    const first = earlier.get(sample.pid);
    if (first === undefined) continue;
    if (first.startedUs === null || sample.startedUs === null || first.cpuMs === null || sample.cpuMs === null) {
      unread.push(sample.pid);
      continue;
    }
    if (first.startedUs !== sample.startedUs) continue;
    compared += 1;
    const gainedMs = sample.cpuMs - first.cpuMs;
    if (gainedMs < CPU_RISE_MS) continue;
    const command =
      sample.command.length > COMMAND_CHARS ? `${sample.command.slice(0, COMMAND_CHARS)}…` : sample.command;
    rising.push({ pid: sample.pid, command, gainedMs });
  }
  rising.sort((a, b) => b.gainedMs - a.gainedMs || a.pid - b.pid);
  return { rising, unread, compared };
}

export function describeCpuRise(rise: CpuRise): string {
  return `pid ${rise.pid} (${rise.command}) gained ${(rise.gainedMs / 1_000).toFixed(1)} s of CPU`;
}

/** The largest gains, named, and how many more there were: a parallel build runs many processes. */
export function describeCpuRises(rises: readonly CpuRise[], named = 3): string {
  const shown = rises.slice(0, named).map(describeCpuRise);
  const more = rises.length - shown.length;
  return `${shown.join('; ')}${more > 0 ? `; and ${more} more` : ''}`;
}
