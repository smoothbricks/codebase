import { describe, expect, it } from 'bun:test';
import {
  type CpuSample,
  cpuChange,
  cpuRiseThresholdsMs,
  describeCpuRises,
  describeCpuThresholds,
  parseCpuTime,
} from './cpu-rise.js';

const STARTED = 'Thu Oct 15 12:34:56 2026';
const sample = (pid: number, cpu: string, command = `proc ${pid}`, started = STARTED): CpuSample => ({
  pid,
  started,
  command,
  cpu: parseCpuTime(cpu),
});

describe('parseCpuTime', () => {
  it('reads the CPU time ps prints on Linux and macOS, with and without hours, days and a fraction', () => {
    expect(parseCpuTime('00:00')?.ms).toBe(0);
    expect(parseCpuTime('59:27')?.ms).toBe(3_567_000);
    expect(parseCpuTime('15:16:20')?.ms).toBe(54_980_000);
    expect(parseCpuTime('2-01:00:05')?.ms).toBe(176_405_000);
    expect(parseCpuTime('0:00.04')?.ms).toBe(40);
    expect(parseCpuTime('1:02:03.45')?.ms).toBe(3_723_450);
  });

  it('carries the resolution it was printed at: whole seconds, or the last digit of its fraction', () => {
    expect(parseCpuTime('59:27')).toEqual({ ms: 3_567_000, resolutionMs: 1_000 });
    expect(parseCpuTime('0:03.6')).toEqual({ ms: 3_600, resolutionMs: 100 });
    expect(parseCpuTime('0:03.63')).toEqual({ ms: 3_630, resolutionMs: 10 });
    expect(parseCpuTime('0:03.633')).toEqual({ ms: 3_633, resolutionMs: 1 });
    // Finer than a millisecond is kept to the millisecond it is read in.
    expect(parseCpuTime('0:03.6334')).toEqual({ ms: 3_633, resolutionMs: 1 });
  });

  it('reads nothing from what ps prints for a process it may not read', () => {
    for (const text of ['-', '?', '', '12', '1:2:3:4', '00:00s']) expect(parseCpuTime(text)).toBeNull();
  });
});

describe('cpuChange', () => {
  it('names the processes that gained at least two counted seconds, largest first, and no idle one', () => {
    const before = [sample(1, '00:10'), sample(2, '00:10'), sample(3, '00:10'), sample(4, '00:10')];
    const after = [sample(1, '00:10'), sample(2, '00:15'), sample(3, '00:12'), sample(4, '00:11')];
    expect(cpuChange(before, after)).toEqual({
      rising: [
        { pid: 2, command: 'proc 2', gainedMs: 5_000 },
        { pid: 3, command: 'proc 3', gainedMs: 2_000 },
      ],
      unread: [],
    });
  });

  it('breaks a tie by pid', () => {
    const before = [sample(9, '00:00'), sample(5, '00:00')];
    const after = [sample(9, '00:04'), sample(5, '00:04')];
    expect(cpuChange(before, after).rising.map((rise) => rise.pid)).toEqual([5, 9]);
  });

  it('compares a process only with itself: a reused pid is a new process, and a new one has no earlier sample', () => {
    const before = [sample(1, '10:00')];
    const after = [sample(1, '00:30', 'reused', 'Thu Oct 15 13:00:00 2026'), sample(2, '00:30')];
    expect(cpuChange(before, after)).toEqual({ rising: [], unread: [] });
  });

  it('says a process whose CPU time one census did not give, and does not count it idle', () => {
    expect(cpuChange([sample(1, '00:10'), sample(2, '00:10')], [sample(1, '-'), sample(2, '00:20')])).toEqual({
      rising: [{ pid: 2, command: 'proc 2', gainedMs: 10_000 }],
      unread: [1],
    });
  });

  it('keeps the start of a command and ends it where the argument list runs on', () => {
    const command = `rustc ${'--cfg x '.repeat(100)}`;
    const [rise] = cpuChange([sample(1, '00:00', command)], [sample(1, '00:09', command)]).rising;
    expect(rise?.command.startsWith('rustc --cfg x')).toBe(true);
    expect(rise?.command.endsWith('…')).toBe(true);
    expect(rise?.command.length).toBeLessThanOrEqual(161);
  });

  it('names a process that gained 1.2 s between two centisecond readings, as macOS ps prints them, and none under 0.5 s', () => {
    const before = [sample(1, '0:03.63'), sample(2, '0:03.63'), sample(3, '0:03.63')];
    const after = [sample(1, '0:04.83'), sample(2, '0:03.98'), sample(3, '0:04.13')];
    expect(cpuChange(before, after)).toEqual({
      rising: [
        { pid: 1, command: 'proc 1', gainedMs: 1_200 },
        { pid: 3, command: 'proc 3', gainedMs: 500 },
      ],
      unread: [],
    });
  });

  it('does not name a process that gained one counted second between two whole-second readings', () => {
    expect(cpuChange([sample(1, '00:10')], [sample(1, '00:11')])).toEqual({ rising: [], unread: [] });
  });

  it('holds a pair of readings to its coarser resolution: whole seconds against centiseconds is two counted seconds', () => {
    const before = [sample(1, '00:10'), sample(2, '00:10')];
    const after = [sample(1, '0:11.50'), sample(2, '0:12.00')];
    expect(cpuChange(before, after).rising).toEqual([{ pid: 2, command: 'proc 2', gainedMs: 2_000 }]);
  });
});

describe('cpuRiseThresholdsMs', () => {
  it('is 2 s between whole-second readings and 0.5 s between centisecond ones', () => {
    expect(cpuRiseThresholdsMs([sample(1, '00:10')], [sample(1, '00:11')])).toEqual([2_000]);
    expect(cpuRiseThresholdsMs([sample(1, '0:03.63')], [sample(1, '0:04.83')])).toEqual([500]);
  });

  it('holds each pair of readings to its coarser resolution and says each bar once, smallest first', () => {
    const before = [sample(1, '00:10'), sample(2, '0:03.63'), sample(3, '00:10'), sample(4, '0:03.63')];
    const after = [sample(1, '00:11'), sample(2, '0:04.83'), sample(3, '0:11.50'), sample(4, '0:04.00')];
    expect(cpuRiseThresholdsMs(before, after)).toEqual([500, 2_000]);
  });

  it('names no bar when no process has a readable CPU time in both censuses', () => {
    expect(cpuRiseThresholdsMs([sample(1, '-'), sample(2, '00:10')], [sample(1, '00:11'), sample(3, '00:10')])).toEqual(
      [],
    );
  });
});

describe('describeCpuThresholds', () => {
  it('says each bar in seconds', () => {
    expect(describeCpuThresholds([2_000])).toBe('2 s');
    expect(describeCpuThresholds([500])).toBe('0.5 s');
    expect(describeCpuThresholds([500, 2_000])).toBe('0.5 s or 2 s');
  });
});

describe('describeCpuRises', () => {
  const rises = [1, 2, 3, 4, 5].map((pid) => ({ pid, command: `cc ${pid}`, gainedMs: 10_000 - pid * 1_000 }));

  it('names the three largest gains and counts the rest', () => {
    expect(describeCpuRises(rises)).toBe(
      'pid 1 (cc 1) gained 9.0 s of CPU; pid 2 (cc 2) gained 8.0 s of CPU; pid 3 (cc 3) gained 7.0 s of CPU; and 2 more',
    );
  });

  it('names every gain when there are no more than three', () => {
    expect(describeCpuRises(rises.slice(0, 2))).toBe(
      'pid 1 (cc 1) gained 9.0 s of CPU; pid 2 (cc 2) gained 8.0 s of CPU',
    );
  });
});
