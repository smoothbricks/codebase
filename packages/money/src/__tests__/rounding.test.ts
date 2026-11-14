import { describe, it, expect as typedExpect } from 'bun:test';

/** Assertions compare runtime values: an amount's brand is a static fact the API tests check by compiling. */
const expect = (value: unknown) => typedExpect(value);

import { amount } from '../amount.js';
import { applyRate, basis, toBasis } from '../basis.js';
import { type Currency, currencyByCode } from '../currencies.js';
import { convertFx, fxRate } from '../fx.js';
import { divideRounded, fromMinor, minor, RoundingMode, roundBasis, toMinor } from '../rounding.js';
import { rate, unit } from '../types.js';

function listed<C extends string>(code: C): Currency<C> {
  const found = currencyByCode(code);
  if (!found.ok) throw new Error(code);
  return { ...found.value, code };
}

//#region smoo/money!n/rounding
describe('the rounding step', () => {
  it('rounds by each named mode and returns the remainder', () => {
    const table: [bigint, Record<RoundingMode, bigint>][] = [
      [15n, { floor: 1n, ceil: 2n, 'toward-zero': 1n, 'half-up': 2n, 'half-even': 2n }],
      [25n, { floor: 2n, ceil: 3n, 'toward-zero': 2n, 'half-up': 3n, 'half-even': 2n }],
      [14n, { floor: 1n, ceil: 2n, 'toward-zero': 1n, 'half-up': 1n, 'half-even': 1n }],
      [-15n, { floor: -2n, ceil: -1n, 'toward-zero': -1n, 'half-up': -2n, 'half-even': -2n }],
      [-25n, { floor: -3n, ceil: -2n, 'toward-zero': -2n, 'half-up': -3n, 'half-even': -2n }],
      [-16n, { floor: -2n, ceil: -1n, 'toward-zero': -1n, 'half-up': -2n, 'half-even': -2n }],
      [20n, { floor: 2n, ceil: 2n, 'toward-zero': 2n, 'half-up': 2n, 'half-even': 2n }],
    ];
    for (const [n, expected] of table) {
      for (const mode of Object.values(RoundingMode)) {
        const { quotient, remainder } = divideRounded(n, 1, mode);
        expect([n, mode, quotient]).toEqual([n, mode, expected[mode]]);
        expect(quotient * 10n + remainder).toBe(n);
      }
    }
  });

  it('settles to minor units with the remainder kept, and back exactly', () => {
    const usd = listed('USD');
    const { minor: cents, remainder } = toMinor(amount<'USD'>(1_234_567), usd, RoundingMode.HalfEven);
    expect([cents, remainder]).toEqual([123, 4_567]);
    expect(fromMinor(cents, usd)).toBe(1_230_000);
    expect(toMinor(amount<'USD'>(1_235_000), usd, RoundingMode.HalfEven).minor).toBe(124);
    expect(toMinor(amount<'USD'>(-1_235_000), usd, RoundingMode.TowardZero)).toEqual({
      minor: -123,
      remainder: -5_000,
    });
    const btc = listed('BTC');
    expect(fromMinor(minor<'BTC'>(1), btc)).toBe(1_000);
  });

  it('rounds basis to an amount once, the remainder in basis units', () => {
    const perToken = rate(basis<'USD'>(4_200), unit('token'));
    const total = applyRate(perToken, 3);
    expect(total).toBe(12_600n);
    expect(roundBasis(total, RoundingMode.HalfUp)).toEqual({ amount: 1, remainder: 2_600n });
    expect(toBasis(amount<'USD'>(7))).toBe(70_000n);
  });
});
//#endregion smoo/money!n/rounding

//#region smoo/money!n/fx
describe('FX', () => {
  it('turns 1.00 USD at 0.92 into 0.92 EUR', () => {
    const converted = convertFx(
      amount<'USD'>(1_000_000),
      fxRate('USD', 'EUR', 92, 2),
      listed('USD'),
      listed('EUR'),
      RoundingMode.HalfEven,
    );
    expect(converted.result).toBe(920_000);
    expect(converted.remainder).toEqual({ units: 0n, scale: 8 });
  });

  it('rounds once and keeps the exact residual', () => {
    const converted = convertFx(
      amount<'USD'>(333_333),
      fxRate('USD', 'EUR', 15, 1),
      listed('USD'),
      listed('EUR'),
      RoundingMode.HalfEven,
    );
    expect(converted.result).toBe(500_000);
    expect(converted.remainder).toEqual({ units: -5n, scale: 7 });
    expect(converted.audit.mode).toBe('half-even');
    const toBtc = convertFx(
      amount<'USD'>(1_000_000),
      fxRate('USD', 'BTC', 1, 5),
      listed('USD'),
      listed('BTC'),
      RoundingMode.Floor,
    );
    expect(toBtc.result).toBe(1_000_000);
  });
});
//#endregion smoo/money!n/fx
