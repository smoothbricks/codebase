import { describe, expect, it } from 'bun:test';
import { amountScale, basisScale, getCurrency, hasCurrency, registerCurrency } from '../currency-registry.js';

describe('getCurrency', () => {
  it('returns USD with correct decimals and basisDecimals', () => {
    const usd = getCurrency('USD');
    expect(usd.code).toBe('USD');
    expect(usd.decimals).toBe(2);
    expect(usd.basisDecimals).toBe(10);
    expect(usd.symbol).toBe('$');
    expect(usd.type).toBe('fiat');
  });

  it('returns JPY with 0 decimals', () => {
    const jpy = getCurrency('JPY');
    expect(jpy.code).toBe('JPY');
    expect(jpy.decimals).toBe(0);
  });

  it('returns BTC with 8 decimals', () => {
    const btc = getCurrency('BTC');
    expect(btc.code).toBe('BTC');
    expect(btc.decimals).toBe(8);
    expect(btc.type).toBe('crypto');
  });

  it('returns BHD with 3 decimals', () => {
    const bhd = getCurrency('BHD');
    expect(bhd.code).toBe('BHD');
    expect(bhd.decimals).toBe(3);
  });

  it('throws for unknown currency code', () => {
    expect(() => getCurrency('INVALID')).toThrow('Unknown currency code: INVALID');
  });
});

describe('hasCurrency', () => {
  it('returns true for registered currencies', () => {
    expect(hasCurrency('USD')).toBe(true);
    expect(hasCurrency('EUR')).toBe(true);
    expect(hasCurrency('BTC')).toBe(true);
  });

  it('returns false for unregistered currencies', () => {
    expect(hasCurrency('XYZ')).toBe(false);
  });
});

describe('amountScale', () => {
  it('returns 100n for USD (decimals=2)', () => {
    expect(amountScale(getCurrency('USD'))).toBe(100n);
  });

  it('returns 1n for JPY (decimals=0)', () => {
    expect(amountScale(getCurrency('JPY'))).toBe(1n);
  });

  it('returns 100000000n for BTC (decimals=8)', () => {
    expect(amountScale(getCurrency('BTC'))).toBe(100_000_000n);
  });
});

describe('basisScale', () => {
  it('returns 10000000000n for USD (basisDecimals=10)', () => {
    expect(basisScale(getCurrency('USD'))).toBe(10_000_000_000n);
  });

  it('returns 100000000n for JPY (basisDecimals=8)', () => {
    expect(basisScale(getCurrency('JPY'))).toBe(100_000_000n);
  });
});

describe('registerCurrency', () => {
  it('adds a custom currency that getCurrency can find', () => {
    registerCurrency({
      code: 'TEST',
      symbol: 'T',
      decimals: 4,
      basisDecimals: 12,
      type: 'crypto',
    });

    const test = getCurrency('TEST');
    expect(test.code).toBe('TEST');
    expect(test.decimals).toBe(4);
    expect(test.basisDecimals).toBe(12);
    expect(test.type).toBe('crypto');
    expect(hasCurrency('TEST')).toBe(true);
  });
});
