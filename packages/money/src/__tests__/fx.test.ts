import { describe, expect, it } from 'bun:test';
import { getCurrency } from '../currency-registry.js';
import { convertFx } from '../fx.js';
import { RoundingMode } from '../rounding.js';
import { Amount, Basis } from '../types.js';

describe('convertFx', () => {
  const usd = getCurrency('USD');
  const eur = getCurrency('EUR');

  it('converts USD to EUR with explicit remainder', () => {
    // 100 USD cents * rate 9200000000 (0.92 EUR in basis)
    // = 920000000000 in EUR basis
    // 920000000000 / 10^8 = 9200 -> Amount(9200n) = 92.00 EUR
    const result = convertFx(Amount<'USD'>(100n), Basis<'EUR'>(9200000000n), usd, eur, RoundingMode.HalfUp);
    expect(result.result).toBe(Amount<'EUR'>(9200n));
    expect(result.remainder).toBe(Basis<'EUR'>(0n));
  });

  it('captures remainder when rounding occurs', () => {
    // 100 USD cents * rate 9233333333 (~ 0.9233333333 EUR)
    // = 923333333300 in EUR basis
    // 923333333300 / 10^8 = 9233.33333300 -> HalfUp -> 9233
    // remainder = 923333333300 - 9233 * 10^8 = 923333333300 - 923300000000 = 33333300
    const result = convertFx(Amount<'USD'>(100n), Basis<'EUR'>(9233333333n), usd, eur, RoundingMode.HalfUp);
    expect(result.result).toBe(Amount<'EUR'>(9233n));
    expect(result.remainder).toBe(Basis<'EUR'>(33333300n));
  });

  it('includes complete audit trail', () => {
    const fromAmount = Amount<'USD'>(100n);
    const rate = Basis<'EUR'>(9200000000n);
    const mode = RoundingMode.HalfUp;

    const result = convertFx(fromAmount, rate, usd, eur, mode);
    expect(result.audit.fromAmount).toBe(fromAmount);
    expect(result.audit.rate).toBe(rate);
    expect(result.audit.mode).toBe(mode);
  });

  it('handles zero amount', () => {
    const result = convertFx(Amount<'USD'>(0n), Basis<'EUR'>(9200000000n), usd, eur, RoundingMode.HalfUp);
    expect(result.result).toBe(Amount<'EUR'>(0n));
    expect(result.remainder).toBe(Basis<'EUR'>(0n));
  });

  it('handles negative amount (refund/reversal)', () => {
    // -100 USD cents * rate 9200000000 = -920000000000
    // -920000000000 / 10^8 = -9200 -> Amount(-9200n)
    const result = convertFx(Amount<'USD'>(-100n), Basis<'EUR'>(9200000000n), usd, eur, RoundingMode.HalfUp);
    expect(result.result).toBe(Amount<'EUR'>(-9200n));
    expect(result.remainder).toBe(Basis<'EUR'>(0n));
  });

  it('negative amount with remainder rounds correctly', () => {
    // -100 * 9233333333 = -923333333300
    // -923333333300 / 10^8 = -9233.333333 -> HalfUp -> -9233
    // remainder = -923333333300 - (-9233 * 10^8) = -923333333300 + 923300000000 = -33333300
    const result = convertFx(Amount<'USD'>(-100n), Basis<'EUR'>(9233333333n), usd, eur, RoundingMode.HalfUp);
    expect(result.result).toBe(Amount<'EUR'>(-9233n));
    expect(result.remainder).toBe(Basis<'EUR'>(-33333300n));
  });

  it('uses Floor rounding mode', () => {
    // 100 * 9233333333 = 923333333300
    // Floor: 923333333300 / 10^8 = 9233 (truncate toward -inf, same as truncation for positive)
    const result = convertFx(Amount<'USD'>(100n), Basis<'EUR'>(9233333333n), usd, eur, RoundingMode.Floor);
    expect(result.result).toBe(Amount<'EUR'>(9233n));
    expect(result.audit.mode).toBe(RoundingMode.Floor);
  });

  it('handles zero rate', () => {
    const result = convertFx(Amount<'USD'>(100n), Basis<'EUR'>(0n), usd, eur, RoundingMode.HalfUp);
    expect(result.result).toBe(Amount<'EUR'>(0n));
    expect(result.remainder).toBe(Basis<'EUR'>(0n));
  });

  it('converts across different precision currencies (JPY -> EUR)', () => {
    const jpy = getCurrency('JPY');
    // JPY has decimals=0, basisDecimals=8
    // EUR has decimals=2, basisDecimals=10
    // 100 JPY * rate Basis<'EUR'>(6500000000n) (0.65 EUR in basis)
    // = 650000000000 in EUR basis
    // 650000000000 / 10^8 = 6500 -> Amount(6500n) = 65.00 EUR
    const result = convertFx(Amount<'JPY'>(100n), Basis<'EUR'>(6500000000n), jpy, eur, RoundingMode.HalfUp);
    expect(result.result).toBe(Amount<'EUR'>(6500n));
    expect(result.remainder).toBe(Basis<'EUR'>(0n));
  });
});
