import { describe, it, expect as typedExpect } from 'bun:test';

/** Assertions compare runtime values: an amount's brand is a static fact the API tests check by compiling. */
const expect = (value: unknown) => typedExpect(value);

import { allocateProportional } from '../allocate.js';
import { amount } from '../amount.js';
import { type Currency, currencyByCode, defineCurrencies } from '../currencies.js';
import {
  addMoney,
  checkBalanced,
  compareMoney,
  money,
  moneyFromWire,
  moneyOf,
  moneyToWire,
  sumMoney,
} from '../money.js';

const accepted = defineCurrencies(['USD', 'EUR', 'ETH']);

function usdOf(): Currency<'USD'> {
  const found = accepted.get('USD');
  if (!found.ok) throw new Error('USD');
  return { ...found.value, code: 'USD' };
}

//#region smoo/money!n/checked-construction
describe('money from data', () => {
  it('accepts a currency the registry accepts and an integer a stored cell holds', () => {
    expect(money(accepted, 'USD', 1_500_000)).toEqual({ ok: true, value: { currency: 'USD', amount: 1_500_000 } });
    expect(money(accepted, 'ETH', 10n ** 30n)).toEqual({ ok: true, value: { currency: 'ETH', amount: 10n ** 30n } });
  });

  it('refuses by name an unaccepted currency, a non-integer and an amount no narrow cell holds', () => {
    expect(money(accepted, 'BTC', 1)).toEqual({
      ok: false,
      error: { code: 'UNKNOWN_CURRENCY', op: 'money', currency: 'BTC' },
    });
    expect(money(accepted, 'USD', 1.5)).toEqual({
      ok: false,
      error: { code: 'INVALID_TEXT', op: 'money', text: '1.5' },
    });
    const precise = 123_456_789_012_345_678_901n;
    expect(money(accepted, 'ETH', precise)).toEqual({
      ok: false,
      error: { code: 'OUT_OF_BACKING', op: 'money', currency: 'ETH', amount: precise.toString() },
    });
  });

  it('crosses the wire as a code and exact decimal text', () => {
    const value = moneyFromWire(accepted, { currency: 'EUR', amount: '-12.345' });
    expect(value).toEqual({ ok: true, value: { currency: 'EUR', amount: -12_345_000 } });
    if (!value.ok) throw new Error('refused');
    expect(moneyToWire(accepted, value.value)).toEqual({ ok: true, value: { currency: 'EUR', amount: '-12.345' } });
    expect(moneyFromWire(accepted, { currency: 'EUR', amount: '1.0000001' }).ok).toBe(false);
  });

  it('returns a currency mismatch and an imbalance as values', () => {
    const usd = moneyOf(usdOf(), amount<'USD'>(5));
    const eur = { currency: 'EUR' as const, amount: amount<'EUR'>(5) };
    expect(addMoney<'USD' | 'EUR'>(usd, eur)).toEqual({
      ok: false,
      error: { code: 'CURRENCY_MISMATCH', op: 'addMoney', left: 'USD', right: 'EUR' },
    });
    expect(addMoney(usd, usd)).toEqual({ ok: true, value: { currency: 'USD', amount: 10 } });
    expect(sumMoney<'USD' | 'EUR'>('USD', [usd, usd, eur]).ok).toBe(false);
    expect(sumMoney('USD', [usd, usd])).toEqual({ ok: true, value: { currency: 'USD', amount: 10 } });
    expect(compareMoney(usd, usd)).toEqual({ ok: true, value: 0 });
    expect(checkBalanced(usdOf(), [amount<'USD'>(5), amount<'USD'>(-5)])).toEqual({ ok: true, value: undefined });
    expect(checkBalanced(usdOf(), [amount<'USD'>(5), amount<'USD'>(-4)])).toEqual({
      ok: false,
      error: { code: 'UNBALANCED', op: 'checkBalanced', currency: 'USD', imbalance: '1' },
    });
  });
});
//#endregion smoo/money!n/checked-construction

//#region smoo/money!n/arithmetic
describe('proportional allocation', () => {
  it('sums to the total, ties to the earlier weight', () => {
    expect(allocateProportional(amount<'USD'>(100), [1, 1, 1])).toEqual([34, 33, 33]);
    expect(allocateProportional(amount<'USD'>(-100), [1, 1, 1])).toEqual([-34, -33, -33]);
    const total = amount<'USD'>(2n ** 60n + 7n);
    const parts = allocateProportional(total, [3, 5, 0, 11]);
    expect(parts.reduce<bigint>((a, b) => a + BigInt(b), 0n)).toBe(2n ** 60n + 7n);
    expect(parts[2]).toBe(0);
    expect(() => allocateProportional(amount<'USD'>(1), [0, 0])).toThrow(RangeError);
    expect(currencyByCode('USD').ok).toBe(true);
  });
});
//#endregion smoo/money!n/arithmetic
