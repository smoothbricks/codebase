import { describe, it, expect as typedExpect } from 'bun:test';

/** Assertions compare runtime values: an amount's brand is a static fact the API tests check by compiling. */
const expect = (value: unknown) => typedExpect(value);

import { amount } from '../amount.js';
import { type Currency, currencyByCode } from '../currencies.js';
import { formatAmount, formatAmountDisplay, identityText, parseAmount } from '../format.js';

function listed<C extends string>(code: C): Currency<C> {
  const found = currencyByCode(code);
  if (!found.ok) throw new Error(code);
  return { ...found.value, code };
}

//#region smoo/money!n/identity
describe('text', () => {
  it('formats exactly: the minor digits always, every nonzero digit to the runtime scale', () => {
    expect(formatAmount(amount<'USD'>(1_500_000), listed('USD'))).toBe('1.50');
    expect(formatAmount(amount<'USD'>(42), listed('USD'))).toBe('0.000042');
    expect(formatAmount(amount<'USD'>(-1_000), listed('USD'))).toBe('-0.001');
    expect(formatAmount(amount<'JPY'>(100_000_000), listed('JPY'))).toBe('100');
    expect(formatAmount(amount<'BHD'>(1_234_000), listed('BHD'))).toBe('1.234');
    expect(formatAmount(amount<'BTC'>(10n ** 11n), listed('BTC'))).toBe('1.00000000');
    expect(formatAmount(amount<'ETH'>(123n * 10n ** 18n + 1n), listed('ETH'))).toBe('123.000000000000000001');
  });

  it('parses its own text back, and refuses other text and digits past the runtime scale', () => {
    const usd = listed('USD');
    for (const value of [0, 42, 1_500_000, -1_000, 2n ** 60n]) {
      expect(parseAmount(formatAmount(amount<'USD'>(value), usd), usd)).toEqual({
        ok: true,
        value: amount<'USD'>(value),
      });
    }
    expect(parseAmount('1.5000000', usd)).toEqual({ ok: true, value: 1_500_000 });
    for (const text of ['1.', '01', '1e3', '', ' 1', '+1', '1,5']) {
      expect(parseAmount(text, usd)).toEqual({ ok: false, error: { code: 'INVALID_TEXT', op: 'parseAmount', text } });
    }
    expect(parseAmount('0.0000001', usd)).toEqual({
      ok: false,
      error: { code: 'FINER_THAN_RUNTIME', op: 'parseAmount', currency: 'USD', text: '0.0000001', runtimeDecimals: 6 },
    });
  });

  it("writes an amount's identity without any trailing zero, the same after a raised runtime scale", () => {
    expect(identityText(amount<'USD'>(1_500_000), listed('USD'))).toBe('1.5');
    expect(identityText(amount<'USD'>(-2_000_000), listed('USD'))).toBe('-2');
    const btc = listed('BTC');
    const raised = { ...btc, runtimeDecimals: 14 };
    expect(identityText(amount<'BTC'>(123_000), btc)).toBe(identityText(amount<'BTC'>(123_000_000), raised));
  });

  it('displays for a person through Intl, exactly, a crypto asset with its code', () => {
    expect(formatAmountDisplay(amount<'USD'>(1_500_000), listed('USD'), 'en-US')).toBe('$1.50');
    expect(formatAmountDisplay(amount<'USD'>(-5_000), listed('USD'), 'en-US')).toBe('-$0.01');
    expect(formatAmountDisplay(amount<'JPY'>(1_234_500_000), listed('JPY'), 'en-US')).toBe('¥1,235');
    expect(formatAmountDisplay(amount<'BTC'>(123_456_789_012n), listed('BTC'), 'en-US')).toBe('1.23456789 BTC');
    expect(formatAmountDisplay(amount<'USD'>(2n ** 70n), listed('USD'), 'en-US')).toBe('$1,180,591,620,717,411.30');
  });
});
//#endregion smoo/money!n/identity
