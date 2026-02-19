import { describe, expect, it } from 'bun:test';
import { assertBalanced, isBalanced } from '../assertions.js';
import { Amount } from '../types.js';

describe('assertBalanced', () => {
  it('does not throw for balanced legs (100 + -100)', () => {
    expect(() => assertBalanced([Amount<'USD'>(100n), Amount<'USD'>(-100n)])).not.toThrow();
  });

  it('does not throw for three balanced legs', () => {
    expect(() => assertBalanced([Amount<'USD'>(100n), Amount<'USD'>(-50n), Amount<'USD'>(-50n)])).not.toThrow();
  });

  it('throws for imbalanced legs with descriptive message', () => {
    expect(() => assertBalanced([Amount<'USD'>(100n), Amount<'USD'>(-99n)])).toThrow('1');
  });

  it('does not throw for empty array', () => {
    expect(() => assertBalanced([])).not.toThrow();
  });

  it('does not throw for single zero', () => {
    expect(() => assertBalanced([Amount<'USD'>(0n)])).not.toThrow();
  });

  it('throws for single non-zero', () => {
    expect(() => assertBalanced([Amount<'USD'>(100n)])).toThrow();
  });
});

describe('isBalanced', () => {
  it('returns true for balanced legs', () => {
    expect(isBalanced([Amount<'USD'>(100n), Amount<'USD'>(-100n)])).toBe(true);
  });

  it('returns false for imbalanced legs', () => {
    expect(isBalanced([Amount<'USD'>(100n), Amount<'USD'>(-99n)])).toBe(false);
  });

  it('returns true for empty array', () => {
    expect(isBalanced([])).toBe(true);
  });

  it('returns true for single zero', () => {
    expect(isBalanced([Amount<'USD'>(0n)])).toBe(true);
  });
});
