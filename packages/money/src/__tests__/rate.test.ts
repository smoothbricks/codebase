import { describe, expect, it } from 'bun:test';
import { getCurrency } from '../currency-registry.js';
import { applyRate } from '../rate.js';
import { RoundingMode, roundBasisToAmount } from '../rounding.js';
import { Amount, Basis, Rate, Unit } from '../types.js';

describe('applyRate', () => {
  it('multiplies rate by quantity in Basis precision', () => {
    const rate = Rate<'USD'>(Basis<'USD'>(166667n), Unit('GB-s'));
    const result = applyRate(rate, 1000n);
    expect(result).toBe(Basis<'USD'>(166667000n));
  });

  it('stays in Basis precision (no rounding yet)', () => {
    const rate = Rate<'USD'>(Basis<'USD'>(1n), Unit('request'));
    const result = applyRate(rate, 3n);
    expect(result).toBe(Basis<'USD'>(3n));
  });

  it('zero quantity produces Basis(0n)', () => {
    const rate = Rate<'USD'>(Basis<'USD'>(166667n), Unit('GB-s'));
    const result = applyRate(rate, 0n);
    expect(result).toBe(Basis<'USD'>(0n));
  });

  it('works with large quantities without overflow', () => {
    const rate = Rate<'USD'>(Basis<'USD'>(1000000000n), Unit('byte'));
    const result = applyRate(rate, 1000000n);
    expect(result).toBe(Basis<'USD'>(1000000000000000n));
  });

  describe('combined workflow: applyRate -> roundBasisToAmount -> Amount', () => {
    it('rate * quantity -> round -> settled amount', () => {
      const usd = getCurrency('USD');
      // Rate: $0.00166667 per GB-s in basis precision
      // basisDecimals=10, so 0.00166667 = 16666700n in Basis
      const rate = Rate<'USD'>(Basis<'USD'>(16666700n), Unit('GB-s'));
      const quantity = 1000n; // 1000 GB-s

      // Step 1: applyRate
      const basisTotal = applyRate(rate, quantity);
      expect(basisTotal).toBe(Basis<'USD'>(16666700000n));

      // Step 2: roundBasisToAmount
      // 16666700000 / 10^8 = 166.667 -> HalfUp -> 167
      const settled = roundBasisToAmount(basisTotal, usd, RoundingMode.HalfUp);
      expect(settled).toBe(Amount<'USD'>(167n)); // $1.67
    });
  });
});
