import { describe, expect, it } from 'bun:test';
import { add, divideWithRemainder, multiply, subtract } from '../amount.js';
import { Amount } from '../types.js';

const usdAmount = (value: bigint) => Amount<'USD'>(value);

describe('add', () => {
  it('adds two amounts of same currency', () => {
    const result = add(Amount<'USD'>(100n), Amount<'USD'>(250n));
    expect(result).toBe(usdAmount(350n));
  });

  it('handles negative amounts', () => {
    const result = add(Amount<'USD'>(-100n), Amount<'USD'>(50n));
    expect(result).toBe(usdAmount(-50n));
  });

  it('handles zero', () => {
    const result = add(Amount<'USD'>(0n), Amount<'USD'>(100n));
    expect(result).toBe(usdAmount(100n));
  });
});

describe('subtract', () => {
  it('subtracts two amounts of same currency', () => {
    const result = subtract(Amount<'USD'>(500n), Amount<'USD'>(200n));
    expect(result).toBe(usdAmount(300n));
  });

  it('produces negative result when subtracting larger from smaller', () => {
    const result = subtract(Amount<'USD'>(100n), Amount<'USD'>(200n));
    expect(result).toBe(usdAmount(-100n));
  });
});

describe('multiply', () => {
  it('multiplies amount by scalar', () => {
    const result = multiply(Amount<'USD'>(100n), 3n);
    expect(result).toBe(usdAmount(300n));
  });

  it('handles zero scalar', () => {
    const result = multiply(Amount<'USD'>(100n), 0n);
    expect(result).toBe(usdAmount(0n));
  });

  it('handles large values without overflow (i64 headroom)', () => {
    // $92 quadrillion in cents -- must not overflow
    const result = multiply(Amount<'USD'>(9_200_000_000_000_000n), 1000n);
    expect(result).toBe(usdAmount(9_200_000_000_000_000_000n));
  });
});

describe('divideWithRemainder', () => {
  it('divides evenly', () => {
    const { quotient, remainder } = divideWithRemainder(Amount<'USD'>(100n), 100n);
    expect(quotient).toBe(usdAmount(1n));
    expect(remainder).toBe(usdAmount(0n));
  });

  it('returns correct quotient and remainder', () => {
    const { quotient, remainder } = divideWithRemainder(Amount<'USD'>(7n), 2n);
    expect(quotient).toBe(usdAmount(3n));
    expect(remainder).toBe(usdAmount(1n));
  });

  it('handles 10 / 3', () => {
    const { quotient, remainder } = divideWithRemainder(Amount<'USD'>(10n), 3n);
    expect(quotient).toBe(usdAmount(3n));
    expect(remainder).toBe(usdAmount(1n));
  });

  it('guarantees quotient * divisor + remainder === original', () => {
    const cases: [bigint, bigint][] = [
      [7n, 2n],
      [10n, 3n],
      [100n, 100n],
      [1000n, 7n],
      [999n, 13n],
      [1n, 1n],
    ];

    for (const [value, divisor] of cases) {
      const { quotient, remainder } = divideWithRemainder(Amount<'USD'>(value), divisor);
      expect(quotient * divisor + remainder).toBe(usdAmount(value));
    }
  });

  it('guarantees invariant for negative amounts', () => {
    const { quotient, remainder } = divideWithRemainder(Amount<'USD'>(-7n), 2n);
    // BigInt division truncates toward zero: -7n / 2n === -3n
    expect(quotient * 2n + remainder).toBe(usdAmount(-7n));
  });

  it('throws on zero divisor', () => {
    expect(() => divideWithRemainder(Amount<'USD'>(100n), 0n)).toThrow();
  });
});
