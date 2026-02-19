import { describe, expect, it } from 'bun:test';
import { basisAdd, basisMultiply, basisSubtract, toBasis } from '../basis.js';
import { getCurrency } from '../currency-registry.js';
import { Amount, Basis } from '../types.js';

describe('basisAdd', () => {
  it('adds two basis values of same currency', () => {
    const result = basisAdd(Basis<'USD'>(1_000_000_000n), Basis<'USD'>(2_000_000_000n));
    expect(result).toBe(3_000_000_000n);
  });
});

describe('basisSubtract', () => {
  it('subtracts two basis values of same currency', () => {
    const result = basisSubtract(Basis<'USD'>(3_000_000_000n), Basis<'USD'>(1_000_000_000n));
    expect(result).toBe(2_000_000_000n);
  });

  it('handles negative results', () => {
    const result = basisSubtract(Basis<'USD'>(1_000_000_000n), Basis<'USD'>(3_000_000_000n));
    expect(result).toBe(-2_000_000_000n);
  });
});

describe('basisMultiply', () => {
  it('multiplies basis by scalar', () => {
    const result = basisMultiply(Basis<'USD'>(1_000_000_000n), 5n);
    expect(result).toBe(5_000_000_000n);
  });
});

describe('toBasis', () => {
  it('converts USD Amount to Basis (scale up by 10^(10-2) = 10^8)', () => {
    const usd = getCurrency('USD');
    // 100 cents ($1.00) at basis scale 10^10 = 10,000,000,000
    const result = toBasis(Amount<'USD'>(100n), usd);
    expect(result).toBe(10_000_000_000n);
  });

  it('converts JPY Amount to Basis (scale up by 10^(8-0) = 10^8)', () => {
    const jpy = getCurrency('JPY');
    // 1 yen at basis scale 10^8 = 100,000,000
    const result = toBasis(Amount<'JPY'>(1n), jpy);
    expect(result).toBe(100_000_000n);
  });

  it('converts BTC Amount to Basis (scale up by 10^(10-8) = 10^2)', () => {
    const btc = getCurrency('BTC');
    // 1 satoshi at basis scale 10^10 = 100
    const result = toBasis(Amount<'BTC'>(1n), btc);
    expect(result).toBe(100n);
  });

  it('converts zero amount to zero basis', () => {
    const usd = getCurrency('USD');
    const result = toBasis(Amount<'USD'>(0n), usd);
    expect(result).toBe(0n);
  });

  it('handles large amounts', () => {
    const usd = getCurrency('USD');
    // $1,000,000.00 = 100,000,000 cents
    const result = toBasis(Amount<'USD'>(100_000_000n), usd);
    // 100_000_000 * 10^8 = 10_000_000_000_000_000
    expect(result).toBe(10_000_000_000_000_000n);
  });
});
