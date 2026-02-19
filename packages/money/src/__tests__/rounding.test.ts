import { describe, expect, it } from 'bun:test';
import { getCurrency } from '../currency-registry.js';
import { RoundingMode, roundBasisToAmount } from '../rounding.js';
import { Amount, Basis } from '../types.js';

describe('RoundingMode', () => {
  it('has Floor, Ceil, HalfEven, HalfUp values', () => {
    expect(RoundingMode.Floor).toBe('floor');
    expect(RoundingMode.Ceil).toBe('ceil');
    expect(RoundingMode.HalfEven).toBe('half-even');
    expect(RoundingMode.HalfUp).toBe('half-up');
  });
});

describe('roundBasisToAmount', () => {
  const usd = getCurrency('USD');
  const jpy = getCurrency('JPY');
  const bhd = getCurrency('BHD');

  describe('USD (decimals=2, basisDecimals=10, scaleDiff=8)', () => {
    it('Floor: 15000000000n -> 150n (exact, no rounding needed)', () => {
      // 15000000000 / 10^8 = 150 exactly
      const result = roundBasisToAmount(Basis<'USD'>(15000000000n), usd, RoundingMode.Floor);
      expect(result).toBe(Amount<'USD'>(150n));
    });

    it('Floor: rounds toward negative infinity', () => {
      // 15050000000 / 10^8 = 150.5 -> floor -> 150
      const result = roundBasisToAmount(Basis<'USD'>(15050000000n), usd, RoundingMode.Floor);
      expect(result).toBe(Amount<'USD'>(150n));
    });

    it('Ceil: rounds toward positive infinity', () => {
      // 15050000000 / 10^8 = 150.5 -> ceil -> 151
      const result = roundBasisToAmount(Basis<'USD'>(15050000000n), usd, RoundingMode.Ceil);
      expect(result).toBe(Amount<'USD'>(151n));
    });

    it('Ceil: exact value does not round up', () => {
      // 15000000000 / 10^8 = 150 exactly -> ceil -> 150
      const result = roundBasisToAmount(Basis<'USD'>(15000000000n), usd, RoundingMode.Ceil);
      expect(result).toBe(Amount<'USD'>(150n));
    });

    it('HalfUp: 0.5 rounds up', () => {
      // 15050000000 / 10^8 = 150.5 -> half-up -> 151
      const result = roundBasisToAmount(Basis<'USD'>(15050000000n), usd, RoundingMode.HalfUp);
      expect(result).toBe(Amount<'USD'>(151n));
    });

    it('HalfUp: below 0.5 rounds down', () => {
      // 15049999999 / 10^8 = 150.49999999 -> half-up -> 150
      const result = roundBasisToAmount(Basis<'USD'>(15049999999n), usd, RoundingMode.HalfUp);
      expect(result).toBe(Amount<'USD'>(150n));
    });

    it('HalfEven (bankers rounding): 0.5 rounds to even (150 is even)', () => {
      // 15050000000 / 10^8 = 150.5 -> half-even -> 150 (even)
      const result = roundBasisToAmount(Basis<'USD'>(15050000000n), usd, RoundingMode.HalfEven);
      expect(result).toBe(Amount<'USD'>(150n));
    });

    it('HalfEven (bankers rounding): 0.5 rounds to even (151 is odd, round up to 152... no, 151.5 rounds to 152)', () => {
      // 15150000000 / 10^8 = 151.5 -> half-even -> 152 (even)
      const result = roundBasisToAmount(Basis<'USD'>(15150000000n), usd, RoundingMode.HalfEven);
      expect(result).toBe(Amount<'USD'>(152n));
    });
  });

  describe('negative values', () => {
    it('Floor with negative: rounds toward negative infinity (more negative)', () => {
      // -15050000000 / 10^8 = -150.5 -> floor -> -151
      const result = roundBasisToAmount(Basis<'USD'>(-15050000000n), usd, RoundingMode.Floor);
      expect(result).toBe(Amount<'USD'>(-151n));
    });

    it('Ceil with negative: rounds toward positive infinity (less negative)', () => {
      // -15050000000 / 10^8 = -150.5 -> ceil -> -150
      const result = roundBasisToAmount(Basis<'USD'>(-15050000000n), usd, RoundingMode.Ceil);
      expect(result).toBe(Amount<'USD'>(-150n));
    });

    it('HalfUp with negative: 0.5 rounds away from zero (more negative)', () => {
      // -15050000000 / 10^8 = -150.5 -> half-up -> -151
      const result = roundBasisToAmount(Basis<'USD'>(-15050000000n), usd, RoundingMode.HalfUp);
      expect(result).toBe(Amount<'USD'>(-151n));
    });

    it('HalfEven with negative: 0.5 rounds to even', () => {
      // -15050000000 / 10^8 = -150.5 -> half-even -> -150 (even)
      const result = roundBasisToAmount(Basis<'USD'>(-15050000000n), usd, RoundingMode.HalfEven);
      expect(result).toBe(Amount<'USD'>(-150n));
    });
  });

  describe('JPY (decimals=0, basisDecimals=8, scaleDiff=8)', () => {
    it('Floor: Basis(150000000n) -> Amount(1n) (1.5 -> 1)', () => {
      // 150000000 / 10^8 = 1.5 -> floor -> 1
      const result = roundBasisToAmount(Basis<'JPY'>(150000000n), jpy, RoundingMode.Floor);
      expect(result).toBe(Amount<'JPY'>(1n));
    });

    it('Ceil: Basis(150000000n) -> Amount(2n) (1.5 -> 2)', () => {
      const result = roundBasisToAmount(Basis<'JPY'>(150000000n), jpy, RoundingMode.Ceil);
      expect(result).toBe(Amount<'JPY'>(2n));
    });

    it('exact value: Basis(100000000n) -> Amount(1n)', () => {
      const result = roundBasisToAmount(Basis<'JPY'>(100000000n), jpy, RoundingMode.Floor);
      expect(result).toBe(Amount<'JPY'>(1n));
    });
  });

  describe('BHD (decimals=3, basisDecimals=10, scaleDiff=7)', () => {
    it('correct scale difference', () => {
      // 12345000000 / 10^7 = 1234.5 -> floor -> 1234
      const result = roundBasisToAmount(Basis<'BHD'>(12345000000n), bhd, RoundingMode.Floor);
      expect(result).toBe(Amount<'BHD'>(1234n));
    });

    it('exact value preserves', () => {
      // 12340000000 / 10^7 = 1234 exactly
      const result = roundBasisToAmount(Basis<'BHD'>(12340000000n), bhd, RoundingMode.Floor);
      expect(result).toBe(Amount<'BHD'>(1234n));
    });
  });

  describe('zero', () => {
    it('Basis(0n) -> Amount(0n) for any mode', () => {
      expect(roundBasisToAmount(Basis<'USD'>(0n), usd, RoundingMode.Floor)).toBe(Amount<'USD'>(0n));
      expect(roundBasisToAmount(Basis<'USD'>(0n), usd, RoundingMode.Ceil)).toBe(Amount<'USD'>(0n));
      expect(roundBasisToAmount(Basis<'USD'>(0n), usd, RoundingMode.HalfEven)).toBe(Amount<'USD'>(0n));
      expect(roundBasisToAmount(Basis<'USD'>(0n), usd, RoundingMode.HalfUp)).toBe(Amount<'USD'>(0n));
    });
  });
});
