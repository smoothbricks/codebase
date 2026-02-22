import { describe, expect, it } from 'bun:test';
import { allocateProportional } from '../allocate.js';
import { Amount } from '../types.js';

const usdAmount = (value: bigint) => Amount<'USD'>(value);

describe('allocateProportional', () => {
  it('distributes equally with remainder to first', () => {
    // 100 / 3 = 33 each, remainder 1 -> first gets extra
    const parts = allocateProportional(Amount<'USD'>(100n), [1n, 1n, 1n]);
    expect(parts).toEqual([usdAmount(34n), usdAmount(33n), usdAmount(33n)]);
  });

  it('distributes evenly when possible', () => {
    const parts = allocateProportional(Amount<'USD'>(10n), [1n, 1n]);
    expect(parts).toEqual([usdAmount(5n), usdAmount(5n)]);
  });

  it('distributes 7 across 3 equal weights', () => {
    // 7 / 3 = 2 each, remainder 1 -> first gets extra
    const parts = allocateProportional(Amount<'USD'>(7n), [1n, 1n, 1n]);
    expect(parts).toEqual([usdAmount(3n), usdAmount(2n), usdAmount(2n)]);
  });

  it('sum invariant holds for equal weights', () => {
    const total = Amount<'USD'>(100n);
    const parts = allocateProportional(total, [1n, 1n, 1n]);
    const sum = parts.reduce((s, p) => usdAmount(s + p), usdAmount(0n));
    expect(sum).toBe(usdAmount(100n));
  });

  it('single weight returns total', () => {
    const parts = allocateProportional(Amount<'USD'>(100n), [1n]);
    expect(parts).toEqual([usdAmount(100n)]);
  });

  it('throws on zero total weight', () => {
    expect(() => allocateProportional(Amount<'USD'>(100n), [0n, 0n])).toThrow();
  });

  it('throws on empty weights array', () => {
    expect(() => allocateProportional(Amount<'USD'>(100n), [])).toThrow();
  });

  it('handles large values correctly', () => {
    const parts = allocateProportional(Amount<'USD'>(1000000000n), [3n, 5n, 2n]);
    expect(parts).toEqual([usdAmount(300000000n), usdAmount(500000000n), usdAmount(200000000n)]);
  });

  it('handles unequal weights with remainder', () => {
    // total=100, weights=[1,2], totalWeight=3
    // base: [33, 66], distributed 99, remainder 1
    // remainder goes to index with largest fractional part
    // weight 1: 100 * 1 / 3 = 33.333... -> frac 0.333
    // weight 2: 100 * 2 / 3 = 66.666... -> frac 0.666
    // largest fractional: index 1 (weight 2)
    const parts = allocateProportional(Amount<'USD'>(100n), [1n, 2n]);
    expect(parts).toEqual([usdAmount(33n), usdAmount(67n)]);
  });

  it('sum invariant holds for unequal weights', () => {
    const total = Amount<'USD'>(100n);
    const parts = allocateProportional(total, [1n, 2n]);
    const sum = parts.reduce((s, p) => usdAmount(s + p), usdAmount(0n));
    expect(sum).toBe(usdAmount(100n));
  });

  it('sum invariant holds for 7 / 3', () => {
    const total = Amount<'USD'>(7n);
    const parts = allocateProportional(total, [1n, 1n, 1n]);
    const sum = parts.reduce((s, p) => usdAmount(s + p), usdAmount(0n));
    expect(sum).toBe(usdAmount(7n));
  });

  it('sum invariant holds for large values', () => {
    const total = Amount<'USD'>(1000000000n);
    const parts = allocateProportional(total, [3n, 5n, 2n]);
    const sum = parts.reduce((s, p) => usdAmount(s + p), usdAmount(0n));
    expect(sum).toBe(usdAmount(1000000000n));
  });

  it('handles weights with zero entries mixed in', () => {
    // weight of 0 should get 0 allocation
    const parts = allocateProportional(Amount<'USD'>(100n), [1n, 0n, 1n]);
    expect(parts[1]).toBe(usdAmount(0n));
    const sum = parts.reduce((s, p) => usdAmount(s + p), usdAmount(0n));
    expect(sum).toBe(usdAmount(100n));
  });
});
