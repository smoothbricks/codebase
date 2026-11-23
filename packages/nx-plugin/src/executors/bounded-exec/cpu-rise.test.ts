import { describe, expect, it } from 'bun:test';
import { type CpuSample, cpuChange, describeCpuRises } from './cpu-rise.js';

const STARTED_US = 1_792_000_000_000_000;
const sample = (pid: number, cpuMs: number | null, command = `proc ${pid}`, startedUs = STARTED_US): CpuSample => ({
  pid,
  startedUs,
  command,
  cpuMs,
});

describe('cpuChange', () => {
  it('names the processes that gained at least 0.5 s, largest first, and no idle one', () => {
    const before = [sample(1, 10_000), sample(2, 10_000), sample(3, 10_000), sample(4, 10_000)];
    const after = [sample(1, 10_000), sample(2, 11_200), sample(3, 10_500), sample(4, 10_499)];
    expect(cpuChange(before, after)).toEqual({
      rising: [
        { pid: 2, command: 'proc 2', gainedMs: 1_200 },
        { pid: 3, command: 'proc 3', gainedMs: 500 },
      ],
      unread: [],
      compared: 4,
    });
  });

  it('breaks a tie by pid', () => {
    const before = [sample(9, 0), sample(5, 0)];
    const after = [sample(9, 4_000), sample(5, 4_000)];
    expect(cpuChange(before, after).rising.map((rise) => rise.pid)).toEqual([5, 9]);
  });

  it('compares a process only with itself: a reused pid is a new process, and a new one has no earlier sample', () => {
    const before = [sample(1, 600_000)];
    const after = [sample(1, 30_000, 'reused', STARTED_US + 1), sample(2, 30_000)];
    expect(cpuChange(before, after)).toEqual({ rising: [], unread: [], compared: 0 });
  });

  it('says a process whose CPU time or start one reading did not give, and does not count it idle', () => {
    const before = [sample(1, 10_000), sample(2, 10_000), sample(3, 10_000)];
    const after = [sample(1, null), sample(2, 20_000), { ...sample(3, 20_000), startedUs: null }];
    expect(cpuChange(before, after)).toEqual({
      rising: [{ pid: 2, command: 'proc 2', gainedMs: 10_000 }],
      unread: [1, 3],
      compared: 1,
    });
  });

  it('keeps the start of a command and ends it where the argument list runs on', () => {
    const command = `rustc ${'--cfg x '.repeat(100)}`;
    const [rise] = cpuChange([sample(1, 0, command)], [sample(1, 9_000, command)]).rising;
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
