import { describe, expect, it } from 'bun:test';
import { getCurrency } from '../currency-registry.js';
import { formatAmount, formatAmountDisplay, formatBasis, parseAmount, parseBasis } from '../format.js';
import { Amount, Basis } from '../types.js';

describe('parseAmount', () => {
  it('USD: "1.50" -> Amount(150n)', () => {
    const result = parseAmount<'USD'>('1.50', getCurrency('USD'));
    expect(result).toBe(Amount<'USD'>(150n));
  });

  it('JPY: "100" -> Amount(100n) (0 decimals)', () => {
    const result = parseAmount<'JPY'>('100', getCurrency('JPY'));
    expect(result).toBe(Amount<'JPY'>(100n));
  });

  it('BHD: "1.234" -> Amount(1234n) (3 decimals)', () => {
    const result = parseAmount<'BHD'>('1.234', getCurrency('BHD'));
    expect(result).toBe(Amount<'BHD'>(1234n));
  });

  it('BTC: "0.00000001" -> Amount(1n) (8 decimals)', () => {
    const result = parseAmount<'BTC'>('0.00000001', getCurrency('BTC'));
    expect(result).toBe(Amount<'BTC'>(1n));
  });

  it('handles whole numbers for currencies with decimals', () => {
    // "100" for USD -> Amount(10000n)
    const result = parseAmount<'USD'>('100', getCurrency('USD'));
    expect(result).toBe(Amount<'USD'>(10000n));
  });

  it('handles negative values', () => {
    const result = parseAmount<'USD'>('-1.50', getCurrency('USD'));
    expect(result).toBe(Amount<'USD'>(-150n));
  });

  it('throws on invalid string (letters)', () => {
    expect(() => parseAmount<'USD'>('abc', getCurrency('USD'))).toThrow();
  });

  it('throws on empty string', () => {
    expect(() => parseAmount<'USD'>('', getCurrency('USD'))).toThrow();
  });

  it('handles zero', () => {
    const result = parseAmount<'USD'>('0.00', getCurrency('USD'));
    expect(result).toBe(Amount<'USD'>(0n));
  });
});

describe('formatAmount', () => {
  it('USD: Amount(150n) -> "1.50"', () => {
    const result = formatAmount(Amount<'USD'>(150n), getCurrency('USD'));
    expect(result).toBe('1.50');
  });

  it('JPY: Amount(100n) -> "100"', () => {
    const result = formatAmount(Amount<'JPY'>(100n), getCurrency('JPY'));
    expect(result).toBe('100');
  });

  it('BHD: Amount(1234n) -> "1.234"', () => {
    const result = formatAmount(Amount<'BHD'>(1234n), getCurrency('BHD'));
    expect(result).toBe('1.234');
  });

  it('BTC: Amount(1n) -> "0.00000001"', () => {
    const result = formatAmount(Amount<'BTC'>(1n), getCurrency('BTC'));
    expect(result).toBe('0.00000001');
  });

  it('negative: Amount(-150n) -> "-1.50"', () => {
    const result = formatAmount(Amount<'USD'>(-150n), getCurrency('USD'));
    expect(result).toBe('-1.50');
  });

  it('zero: Amount(0n) -> "0.00"', () => {
    const result = formatAmount(Amount<'USD'>(0n), getCurrency('USD'));
    expect(result).toBe('0.00');
  });

  it('JPY zero: Amount(0n) -> "0"', () => {
    const result = formatAmount(Amount<'JPY'>(0n), getCurrency('JPY'));
    expect(result).toBe('0');
  });
});

describe('parseAmount/formatAmount round-trip', () => {
  const cases: Array<{ currency: string; original: string }> = [
    { currency: 'USD', original: '1.50' },
    { currency: 'USD', original: '0.01' },
    { currency: 'USD', original: '999999.99' },
    { currency: 'JPY', original: '100' },
    { currency: 'JPY', original: '1' },
    { currency: 'BHD', original: '1.234' },
    { currency: 'BTC', original: '0.00000001' },
    { currency: 'BTC', original: '21000000.00000000' },
    { currency: 'USD', original: '-42.50' },
  ];

  for (const { currency, original } of cases) {
    it(`round-trips ${currency}: "${original}"`, () => {
      const curr = getCurrency(currency);
      const parsed = parseAmount(original, curr);
      const formatted = formatAmount(parsed, curr);
      expect(formatted).toBe(original);
    });
  }
});

describe('parseBasis', () => {
  it('USD: "1.5000000000" -> Basis(15000000000n)', () => {
    const result = parseBasis<'USD'>('1.5000000000', getCurrency('USD'));
    expect(result).toBe(Basis<'USD'>(15000000000n));
  });

  it('handles whole number', () => {
    const result = parseBasis<'USD'>('1', getCurrency('USD'));
    expect(result).toBe(Basis<'USD'>(10000000000n));
  });
});

describe('formatBasis', () => {
  it('USD: Basis(15000000000n) -> "1.5000000000"', () => {
    const result = formatBasis(Basis<'USD'>(15000000000n), getCurrency('USD'));
    expect(result).toBe('1.5000000000');
  });

  it('zero: Basis(0n) -> "0.0000000000" (USD)', () => {
    const result = formatBasis(Basis<'USD'>(0n), getCurrency('USD'));
    expect(result).toBe('0.0000000000');
  });
});

describe('parseBasis/formatBasis round-trip', () => {
  it('round-trips USD basis', () => {
    const usd = getCurrency('USD');
    const original = '1.5000000000';
    const parsed = parseBasis(original, usd);
    const formatted = formatBasis(parsed, usd);
    expect(formatted).toBe(original);
  });
});

describe('formatAmountDisplay', () => {
  it('formats USD with currency symbol (en-US)', () => {
    const result = formatAmountDisplay(Amount<'USD'>(150n), getCurrency('USD'), 'en-US');
    expect(result).toContain('1.50');
    expect(result).toContain('$');
  });

  it('formats JPY without decimal places', () => {
    const result = formatAmountDisplay(Amount<'JPY'>(100n), getCurrency('JPY'), 'en-US');
    expect(result).toContain('100');
    expect(result).toContain('¥');
  });

  it('formats negative amounts', () => {
    const result = formatAmountDisplay(Amount<'USD'>(-150n), getCurrency('USD'), 'en-US');
    // Intl.NumberFormat may use minus sign or parentheses depending on locale
    expect(result).toContain('1.50');
  });

  it('formats zero correctly', () => {
    const result = formatAmountDisplay(Amount<'USD'>(0n), getCurrency('USD'), 'en-US');
    expect(result).toContain('0.00');
  });

  it('handles large amounts within safe integer range', () => {
    // $1,000,000,000.00 = 100_000_000_000 cents
    const result = formatAmountDisplay(Amount<'USD'>(100_000_000_000n), getCurrency('USD'), 'en-US');
    expect(result).toContain('1,000,000,000.00');
  });
});
