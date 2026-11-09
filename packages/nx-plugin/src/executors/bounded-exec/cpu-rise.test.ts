import { describe, expect, it } from 'bun:test';
import { CPU_RISE_MIN_MS, type CpuSample, cpuChange, describeCpuRises, parseCpuTimeMs } from './cpu-rise.js';

const STARTED = 'Thu Oct 15 12:34:56 2026';
const sample = (pid: number, cpu: string, command = `proc ${pid}`, started = STARTED): CpuSample => ({
  pid,
  started,
  command,
  cpuMs: parseCpuTimeMs(cpu),
});

describe('parseCpuTimeMs', () => {
  it('reads the CPU time ps prints on Linux and macOS, with and without hours, days and a fraction', () => {
    expect(parseCpuTimeMs('00:00')).toBe(0);
    expect(parseCpuTimeMs('59:27')).toBe(3_567_000);
    expect(parseCpuTimeMs('15:16:20')).toBe(54_980_000);
    expect(parseCpuTimeMs('2-01:00:05')).toBe(176_405_000);
    expect(parseCpuTimeMs('0:00.04')).toBe(40);
    expect(parseCpuTimeMs('1:02:03.45')).toBe(3_723_450);
  });

  it('reads nothing from what ps prints for a process it may not read', () => {
    for (const text of ['-', '?', '', '12', '1:2:3:4', '00:00s']) expect(parseCpuTimeMs(text)).toBeNull();
  });
});

describe('cpuChange', () => {
  it('names the processes that gained at least two counted seconds, largest first, and no idle one', () => {
    const before = [sample(1, '00:10'), sample(2, '00:10'), sample(3, '00:10'), sample(4, '00:10')];
    const after = [sample(1, '00:10'), sample(2, '00:15'), sample(3, '00:12'), sample(4, '00:11')];
    expect(cpuChange(before, after)).toEqual({
      rising: [
        { pid: 2, command: 'proc 2', gainedMs: 5_000 },
        { pid: 3, command: 'proc 3', gainedMs: CPU_RISE_MIN_MS },
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
