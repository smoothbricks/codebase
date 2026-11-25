import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { fewestThreadsWithin, suiteMinimum, suiteWall, wholeSuiteMinimum } from './suite-minimum.js';

const IDLE_MS = 180_000;
const SEED = 20261124;
const seconds = (...walls: number[]): number[] => walls.map((wall) => wall * 1000);

//#region smoo/cowshed!n/a-minimum-for-a-whole-suite
describe('the minimum a whole-suite test leaf names', () => {
  it('is no minimum for a leaf with no recorded wall', () => {
    expect(suiteMinimum([], 18, IDLE_MS)).toBe(1);
  });

  it('is one where a single thread already keeps a test finishing within the bound', () => {
    expect(suiteMinimum(seconds(60, 45, 30, 5), 18, IDLE_MS)).toBe(1);
  });

  it('is the count at which shorter tests finish beside a test longer than the bound', () => {
    // One case of 511 s and others that together outlast it: on one thread nothing finishes for 511 s; on two the
    // others finish beside it, the longest first, never more than 180 s apart.
    const walls = seconds(511, 156, 99, 67, 66, 39, 38, 38, 36, 29, 28, 17, 11, 9, 7, 3);
    expect(suiteMinimum(walls, 18, IDLE_MS)).toBe(2);
  });

  it('grows while the gap after the short tests run out is still longer than the bound', () => {
    // Two threads finish the short tests at 100 s and 200 s, then wait 400 s for the long one; three threads cannot
    // close that gap either, since it is the long test's own tail: the leaf asks for all it wants.
    expect(suiteMinimum(seconds(600, 100, 100), 4, IDLE_MS)).toBe(3);
  });

  it('never names more than the leaf asks for or than it has tests', () => {
    expect(suiteMinimum(seconds(900, 900, 900, 900, 900), 2, IDLE_MS)).toBe(2);
    expect(suiteMinimum(seconds(900), 18, IDLE_MS)).toBe(1);
  });

  it('answers the same for the walls in any order', () => {
    const walls = seconds(511, 156, 99, 67, 66, 39);
    expect(suiteMinimum([...walls].reverse(), 18, IDLE_MS)).toBe(suiteMinimum(walls, 18, IDLE_MS));
  });
});

describe('the LPT wall of a suite', () => {
  it('is zero for a leaf with no recorded wall', () => {
    expect(suiteWall([], 1)).toBe(0);
    expect(suiteWall([], 18)).toBe(0);
  });

  it('schedules the walls longest first onto the thread that frees first', () => {
    expect(suiteWall(seconds(60, 45, 30, 5), 1)).toBe(140_000);
    expect(suiteWall(seconds(60, 45, 30, 30), 2)).toBe(90_000);
    expect(suiteWall(seconds(900, 1, 1, 1), 1)).toBe(903_000);
    expect(suiteWall(seconds(900, 1, 1, 1), 2)).toBe(900_000);
    expect(suiteWall(seconds(900, 1, 1, 1), 18)).toBe(900_000);
  });

  it('answers the same for the walls in any order', () => {
    const walls = seconds(511, 156, 99, 67, 66, 39);
    expect(suiteWall(walls, 2)).toBe(511_000);
    expect(suiteWall([...walls].reverse(), 2)).toBe(suiteWall(walls, 2));
  });
});

describe('the fewest threads to finish a suite within a target wall', () => {
  it('is one for a leaf with no recorded wall', () => {
    expect(fewestThreadsWithin([], 18, 180_000)).toBe(1);
    expect(fewestThreadsWithin([], 1, 0)).toBe(1);
  });

  it('names the fewest threads whose LPT wall meets the target for 6,000 tests of 300 ms', () => {
    const walls = new Array<number>(6_000).fill(300);
    expect(fewestThreadsWithin(walls, 18, 180_000)).toBe(10);
    expect(fewestThreadsWithin(walls, 18, 360_000)).toBe(5);
    expect(fewestThreadsWithin(walls, 18, 90_000)).toBe(18);
  });

  it('is one when a single thread already finishes the whole suite within the target', () => {
    const underTarget = [...seconds(60, 45, 30), ...new Array<number>(1_000).fill(5)];
    const sixtySeconds = [...seconds(25, 15, 10, 5), ...new Array<number>(1_000).fill(5)];
    expect(fewestThreadsWithin(underTarget, 18, 180_000)).toBe(1);
    expect(fewestThreadsWithin(sixtySeconds, 18, 180_000)).toBe(1);
  });

  it('names the fewest threads at the least wall when one test outlasts the target', () => {
    // One 900 s test beside three 1 s tests: 1 thread takes 903 s, 2 threads take 900 s, and 3..18 threads still take
    // 900 s, so 2 is the fewest threads at the least wall, never want.
    expect(fewestThreadsWithin(seconds(900, 1, 1, 1), 18, 300_000)).toBe(2);
  });

  it('never names more than the leaf asks for or than it has tests', () => {
    expect(fewestThreadsWithin(seconds(900, 900, 900, 900, 900), 2, 180_000)).toBe(2);
    expect(fewestThreadsWithin(seconds(900, 900), 18, 180_000)).toBe(2);
    expect(fewestThreadsWithin(seconds(900), 18, 180_000)).toBe(1);
  });

  it('answers the same for the walls in any order', () => {
    const walls = seconds(511, 156, 99, 67, 66, 39);
    const reversed = [...walls].reverse();
    expect(fewestThreadsWithin(walls, 18, 520_000)).toBe(2);
    for (const targetMs of [300_000, 520_000, 1_000_000]) {
      expect(fewestThreadsWithin(reversed, 18, targetMs)).toBe(fewestThreadsWithin(walls, 18, targetMs));
    }
  });

  it('is non-increasing in the target, meets the target or the least wall, and stays within 1..min(want, n)', () => {
    fc.assert(
      fc.property(
        fc.array(fc.integer({ min: 0, max: 1_000_000 }), { minLength: 1, maxLength: 60 }),
        fc.integer({ min: 1, max: 24 }),
        fc.integer({ min: 0, max: 5_000_000 }),
        fc.integer({ min: 0, max: 5_000_000 }),
        (walls, want, targetA, targetB) => {
          const t1 = Math.min(targetA, targetB);
          const t2 = Math.max(targetA, targetB);
          const most = Math.min(want, walls.length);
          let leastWall = Number.POSITIVE_INFINITY;
          for (let threads = 1; threads <= most; threads++) {
            const wall = suiteWall(walls, threads);
            if (wall < leastWall) leastWall = wall;
          }
          const r1 = fewestThreadsWithin(walls, want, t1);
          const r2 = fewestThreadsWithin(walls, want, t2);
          expect(r2).toBeLessThanOrEqual(r1);
          expect(r1).toBeGreaterThanOrEqual(1);
          expect(r1).toBeLessThanOrEqual(most);
          expect(r2).toBeGreaterThanOrEqual(1);
          expect(r2).toBeLessThanOrEqual(most);
          expect(suiteWall(walls, r1)).toBeLessThanOrEqual(Math.max(t1, leastWall));
          expect(suiteWall(walls, r2)).toBeLessThanOrEqual(Math.max(t2, leastWall));
        },
      ),
      { seed: SEED, numRuns: 200 },
    );
  });
});

describe('wholeSuiteMinimum floor composition', () => {
  it('returns 1 for empty walls or want <= 1, with or without bounds', () => {
    expect(wholeSuiteMinimum([], 18, IDLE_MS)).toBe(1);
    expect(wholeSuiteMinimum([], 18, IDLE_MS, 30_000)).toBe(1);
    expect(wholeSuiteMinimum([], 18, undefined, 30_000)).toBe(1);
    expect(wholeSuiteMinimum(seconds(200, 200, 200), 1, IDLE_MS, 30_000)).toBe(1);
    expect(wholeSuiteMinimum(seconds(200, 200, 200), 0, IDLE_MS, 30_000)).toBe(1);
    expect(wholeSuiteMinimum(seconds(200, 200, 200), -2, IDLE_MS, 30_000)).toBe(1);
  });

  it('omits liveness when idleMs is undefined and omits the wall floor when agingMs is omitted or null', () => {
    const minigrafWalls = new Array<number>(955).fill(1_079_000 / 955);
    expect(wholeSuiteMinimum(minigrafWalls, 18, undefined, undefined)).toBe(1);
    expect(wholeSuiteMinimum(minigrafWalls, 18, undefined, null)).toBe(1);
    expect(wholeSuiteMinimum(minigrafWalls, 18, undefined, 30_000)).toBe(12);
    expect(wholeSuiteMinimum(minigrafWalls, 18, IDLE_MS)).toBe(1);
    expect(wholeSuiteMinimum(minigrafWalls, 18, IDLE_MS, null)).toBe(1);
    expect(wholeSuiteMinimum(minigrafWalls, 18, IDLE_MS, 30_000)).toBe(12);
    // Without idleMs, a long test that would raise liveness still contributes only 1 to the liveness side.
    expect(wholeSuiteMinimum(seconds(200, 50, 50), 18, undefined, 100_000)).toBe(1);
  });

  it('takes the larger of liveness and wall floor and stays capped by min(want, walls.length)', () => {
    // Liveness dominates: one 200 s test beside two 50 s tests needs 2 threads for 180 s idle, while 1 thread is
    // within 100 s of the 2-thread wall (300 s vs 200 s).
    expect(wholeSuiteMinimum(seconds(200, 50, 50), 18, IDLE_MS, 100_000)).toBe(2);
    // Wall floor dominates: 6,000 tests of 300 ms satisfy 180 s idle on 1 thread, while aging 30 s over 18 threads
    // (100.2 s + 30 s = 130.2 s) requires 14 threads.
    const manyShort = new Array<number>(6_000).fill(300);
    expect(wholeSuiteMinimum(manyShort, 18, IDLE_MS)).toBe(1);
    expect(wholeSuiteMinimum(manyShort, 18, IDLE_MS, 30_000)).toBe(14);
    // Never exceeds want or walls.length.
    expect(wholeSuiteMinimum(seconds(900, 900), 18, IDLE_MS, 0)).toBe(2);
    expect(wholeSuiteMinimum(seconds(900, 900, 900, 900), 2, IDLE_MS, 0)).toBe(2);
  });

  it('refuses invalid idleMs or agingMs rather than silently dropping them', () => {
    const walls = seconds(60, 60, 60);
    for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      expect(() => wholeSuiteMinimum(walls, 18, IDLE_MS, bad)).toThrow(RangeError);
      expect(() => wholeSuiteMinimum([], 18, IDLE_MS, bad)).toThrow(RangeError);
      expect(() => wholeSuiteMinimum(walls, 18, bad, 30_000)).toThrow(RangeError);
      expect(() => wholeSuiteMinimum([], 18, bad, 30_000)).toThrow(RangeError);
    }
  });
});
//#endregion smoo/cowshed!n/a-minimum-for-a-whole-suite
