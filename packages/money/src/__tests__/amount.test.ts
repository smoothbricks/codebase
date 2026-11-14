import { describe, it, expect as typedExpect } from 'bun:test';

/** Assertions compare runtime values: an amount's brand is a static fact the API tests check by compiling. */
const expect = (value: unknown) => typedExpect(value);

import { add, amount, compare, divideWithRemainder, isZero, multiply, negate, subtract, sum } from '../amount.js';
import { defineCurrencies } from '../currencies.js';
import { money } from '../money.js';
import type { Amount } from '../types.js';

const MAX = Number.MAX_SAFE_INTEGER;

//#region smoo/money!n/amount-runtime
describe('an amount', () => {
  it('is a number while it is a safe integer and a bigint beyond, one spelling each', () => {
    expect(amount<'USD'>(5n)).toBe(5);
    expect(amount<'USD'>(2n ** 60n)).toBe(2n ** 60n);
    expect(amount<'USD'>(2 ** 60)).toBe(2n ** 60n);
    expect(() => amount<'USD'>(1.5)).toThrow(TypeError);
  });

  it('takes the number path and widens to bigint exactly past 2^53, and back', () => {
    const max = amount<'USD'>(MAX);
    const past = add(max, amount<'USD'>(1));
    expect(past).toBe(2n ** 53n);
    expect(subtract(past, amount<'USD'>(1))).toBe(MAX);
    expect(negate(past)).toBe(-(2n ** 53n));
    expect(negate(amount<'USD'>(0))).toBe(0);
    expect(multiply(amount<'USD'>(3_000_000_000), 4_000_000)).toBe(12_000_000_000_000_000n);
    expect(multiply(amount<'USD'>(-3), 0)).toBe(0);
    expect(sum([max, amount<'USD'>(MAX), amount<'USD'>(-MAX), amount<'USD'>(2n ** 70n)])).toBe(BigInt(MAX) + 2n ** 70n);
    expect(sum([amount<'USD'>(1), amount<'USD'>(2)])).toBe(3);
  });

  it('divides with a remainder that rebuilds the amount', () => {
    const { quotient, remainder } = divideWithRemainder(amount<'USD'>(-7), 2);
    expect([quotient, remainder]).toEqual([-3, -1]);
    expect(() => divideWithRemainder(amount<'USD'>(1), 0)).toThrow(RangeError);
  });

  it('compares across its two spellings', () => {
    expect(compare(amount<'USD'>(MAX), amount<'USD'>(2n ** 60n))).toBe(-1);
    expect(compare(amount<'USD'>(2n ** 60n), amount<'USD'>(MAX))).toBe(1);
    expect(compare(amount<'USD'>(4), amount<'USD'>(4n))).toBe(0);
    expect(isZero(amount<'USD'>(0n))).toBe(true);
  });
});
//#endregion smoo/money!n/amount-runtime

//#region smoo/money!n/compile-time-currency
describe('compile-time currency', () => {
  it('does not add amounts of two currencies, or of a union of currencies', () => {
    const usd = amount<'USD'>(1);
    const eur = amount<'EUR'>(1);
    // @ts-expect-error amounts of two currencies
    add(usd, eur);
    const pick = (usdFirst: boolean): Amount<'USD'> | Amount<'EUR'> => (usdFirst ? usd : eur);
    const either = pick(true);
    // @ts-expect-error an amount of a union of currencies
    add(either, either);
    // @ts-expect-error an amount is not assignable to another currency's
    const wrong: Amount<'EUR'> = usd;
    expect(wrong).toBe(1);
  });

  it('narrows a value read from data by its currency', () => {
    const accepted = defineCurrencies(['USD', 'EUR']);
    const read = money(accepted, 'USD', 1_500_000);
    if (!read.ok) throw new Error('refused');
    const value = read.value;
    if (value.currency === 'USD') {
      const usd: Amount<'USD'> = value.amount;
      expect(add(usd, usd)).toBe(3_000_000);
    } else {
      // @ts-expect-error not narrowed to USD here
      const usd: Amount<'USD'> = value.amount;
      expect(usd).toBeUndefined();
    }
  });
});
//#endregion smoo/money!n/compile-time-currency
