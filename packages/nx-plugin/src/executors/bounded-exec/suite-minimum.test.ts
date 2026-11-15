import { describe, expect, it } from 'bun:test';
import { suiteMinimum } from './suite-minimum.js';

const IDLE_MS = 180_000;
const seconds = (...walls: number[]): number[] => walls.map((wall) => wall * 1000);

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
