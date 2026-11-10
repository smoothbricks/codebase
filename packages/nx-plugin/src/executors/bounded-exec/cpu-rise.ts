import type { tags } from 'typia';

/**
 * The CPU a run's tree used while its idle bound counted down. A test that runs a compiler is a build inside the test,
 * which is a defect and not a test (specs/cowshed/08_testing.md, "Integration tests (real substrate)"): the pre-signal
 * snapshot names the processes whose CPU time rose between its two process censuses, so the expiry fails as that and
 * not as an anonymous hang. CPU never restarts the idle bound, which counts only finished tests and the progress a
 * running test reports (progress-channel.ts); this says what ran.
 */

/** What a rising process makes of the expiry, said in the same words by the snapshot and by the verdict. */
export const CPU_RISING_CLASS = "a process in the test's tree was using CPU: likely a build inside the test";

/**
 * The least gain that names a rise, whatever resolution ps prints CPU time at: 10% of a core over the snapshot's five
 * seconds between censuses.
 */
export const CPU_RISE_FLOOR_MS = 500;

/**
 * What a printed CPU time resolves to, in milliseconds: the value of its last digit. Whole seconds are 1,000, a
 * fraction of n digits is 10^(3-n); a finer fraction is kept to the millisecond, so none is below 1.
 */
export type CpuResolutionMs = 1_000 | 100 | 10 | 1;

/** A CPU time ps printed, with the resolution it was printed at. */
export interface CpuTime {
  readonly ms: number;
  readonly resolutionMs: CpuResolutionMs;
}

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
  readonly cpu: CpuTime | null;
}

export interface CpuChange {
  /** Largest gain first. */
  readonly rising: readonly CpuRise[];
  /** Processes in both censuses that one of them gave no readable CPU time for: not counted idle. */
  readonly unread: readonly number[];
}

function resolutionOfFraction(digits: number): CpuResolutionMs {
  switch (digits) {
    case 0:
      return 1_000;
    case 1:
      return 100;
    case 2:
      return 10;
    default:
      return 1;
  }
}

/**
 * A ps CPU time (`cputime=`): `[[D-]HH:]MM:SS` with an optional fraction, as Linux prints it (whole seconds) and macOS
 * does (centiseconds). Null for anything else, such as the `-` ps prints for a process it may not read.
 */
export function parseCpuTime(text: string): CpuTime | null {
  const match = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.(\d+))?)$/.exec(text);
  if (match === null) return null;
  const [, days, hours, minutes, seconds, fraction] = match;
  if (minutes === undefined || seconds === undefined) return null;
  const wholeMinutes = ((Number(days ?? 0) * 24 + Number(hours ?? 0)) * 60 + Number(minutes)) * 60;
  return {
    ms: Math.round((wholeMinutes + Number(seconds)) * 1_000),
    resolutionMs: resolutionOfFraction(fraction?.length ?? 0),
  };
}

/**
 * The least gain between two readings of one process that names a rise: twice the coarser of their resolutions, and
 * never under {@link CPU_RISE_FLOOR_MS}. A process that crossed a boundary of the last printed digit on a few
 * milliseconds of work gains one resolution, so two are more than one. That is 2,000 ms between whole-second readings
 * and 500 ms between centisecond ones.
 */
export function cpuRiseThresholdMs(before: CpuTime, after: CpuTime): number {
  return Math.max(CPU_RISE_FLOOR_MS, 2 * Math.max(before.resolutionMs, after.resolutionMs));
}

/** Each process present in both censuses (same pid and start), as its earlier and its later sample. */
function paired(before: readonly CpuSample[], after: readonly CpuSample[]): (readonly [CpuSample, CpuSample])[] {
  const earlier = new Map(before.map((sample) => [`${sample.pid} ${sample.started}`, sample]));
  const pairs: (readonly [CpuSample, CpuSample])[] = [];
  for (const sample of after) {
    const first = earlier.get(`${sample.pid} ${sample.started}`);
    if (first !== undefined) pairs.push([first, sample]);
  }
  return pairs;
}

/**
 * The processes present in both censuses (same pid and start) whose CPU time rose by at least
 * {@link cpuRiseThresholdMs}.
 */
export function cpuChange(before: readonly CpuSample[], after: readonly CpuSample[]): CpuChange {
  const rising: CpuRise[] = [];
  const unread: number[] = [];
  for (const [first, sample] of paired(before, after)) {
    if (first.cpu === null || sample.cpu === null) {
      unread.push(sample.pid);
      continue;
    }
    const gainedMs = sample.cpu.ms - first.cpu.ms;
    if (gainedMs < cpuRiseThresholdMs(first.cpu, sample.cpu)) continue;
    const command =
      sample.command.length > COMMAND_CHARS ? `${sample.command.slice(0, COMMAND_CHARS)}…` : sample.command;
    rising.push({ pid: sample.pid, command, gainedMs });
  }
  rising.sort((a, b) => b.gainedMs - a.gainedMs || a.pid - b.pid);
  return { rising, unread };
}

/**
 * The gains {@link cpuChange} held these censuses to, each distinct one once, smallest first: one per resolution of the
 * readings compared, and none when no process had a readable CPU time in both.
 */
export function cpuRiseThresholdsMs(before: readonly CpuSample[], after: readonly CpuSample[]): readonly number[] {
  const thresholds = new Set<number>();
  for (const [first, sample] of paired(before, after)) {
    if (first.cpu !== null && sample.cpu !== null) thresholds.add(cpuRiseThresholdMs(first.cpu, sample.cpu));
  }
  return [...thresholds].sort((a, b) => a - b);
}

/** The gains of {@link cpuRiseThresholdsMs} in seconds, as the snapshot says them: `2 s`, or `0.5 s or 2 s`. */
export function describeCpuThresholds(thresholdsMs: readonly number[]): string {
  return thresholdsMs.map((ms) => `${ms / 1_000} s`).join(' or ');
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
