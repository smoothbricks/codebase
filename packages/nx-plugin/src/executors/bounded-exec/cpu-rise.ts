import type { tags } from 'typia';

/**
 * The CPU a run's tree used while its idle bound counted down. A test that runs a compiler is a build inside the test,
 * which is a defect and not a test (specs/cowshed/08_testing.md, "Integration tests (real substrate)"): the pre-signal
 * snapshot names the processes whose CPU time rose between its two process censuses, so the expiry fails as that and
 * not as an anonymous hang. CPU never restarts the idle bound, which counts only finished tests; this says what ran.
 */

/** What a rising process makes of the expiry, said in the same words by the snapshot and by the verdict. */
export const CPU_RISING_CLASS = "a process in the test's tree was using CPU: likely a build inside the test";

/**
 * The least CPU time a process gains to be named. ps counts CPU time in whole seconds, so one counted second can be a
 * boundary that an idle process crossed on a few milliseconds of work; two counted seconds are more than one second.
 */
export const CPU_RISE_MIN_MS = 2_000;

/** The longest command a rise keeps: a compiler's argument list runs to kilobytes, and its name leads it. */
const COMMAND_CHARS = 160;

/** A process whose CPU time rose between the snapshot's two censuses. */
export interface CpuRise {
  readonly pid: number & tags.Type<'uint32'> & tags.Minimum<1>;
  readonly command: string;
  readonly gainedMs: number & tags.Minimum<0>;
}

/** One process of a census, as far as CPU is concerned. */
export interface CpuSample {
  readonly pid: number;
  readonly started: string;
  readonly command: string;
  /** Null when ps printed a CPU time that does not parse. */
  readonly cpuMs: number | null;
}

export interface CpuChange {
  /** Largest gain first. */
  readonly rising: readonly CpuRise[];
  /** Processes in both censuses that one of them gave no readable CPU time for: not counted idle. */
  readonly unread: readonly number[];
}

/**
 * A ps CPU time (`cputime=`) in milliseconds: `[[D-]HH:]MM:SS` with an optional fraction, as Linux and macOS print it.
 * Null for anything else, such as the `-` ps prints for a process it may not read.
 */
export function parseCpuTimeMs(text: string): number | null {
  const match = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)$/.exec(text);
  if (match === null) return null;
  const [, days, hours, minutes, seconds] = match;
  if (minutes === undefined || seconds === undefined) return null;
  const wholeMinutes = ((Number(days ?? 0) * 24 + Number(hours ?? 0)) * 60 + Number(minutes)) * 60;
  return Math.round((wholeMinutes + Number(seconds)) * 1_000);
}

/** The processes present in both censuses (same pid and start) whose CPU time rose by at least {@link CPU_RISE_MIN_MS}. */
export function cpuChange(before: readonly CpuSample[], after: readonly CpuSample[]): CpuChange {
  const earlier = new Map(before.map((sample) => [`${sample.pid} ${sample.started}`, sample]));
  const rising: CpuRise[] = [];
  const unread: number[] = [];
  for (const sample of after) {
    const first = earlier.get(`${sample.pid} ${sample.started}`);
    if (first === undefined) continue;
    if (first.cpuMs === null || sample.cpuMs === null) {
      unread.push(sample.pid);
      continue;
    }
    const gainedMs = sample.cpuMs - first.cpuMs;
    if (gainedMs < CPU_RISE_MIN_MS) continue;
    const command =
      sample.command.length > COMMAND_CHARS ? `${sample.command.slice(0, COMMAND_CHARS)}…` : sample.command;
    rising.push({ pid: sample.pid, command, gainedMs });
  }
  rising.sort((a, b) => b.gainedMs - a.gainedMs || a.pid - b.pid);
  return { rising, unread };
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
