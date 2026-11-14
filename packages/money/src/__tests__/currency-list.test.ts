import { describe, it, expect as typedExpect } from 'bun:test';

/** Assertions compare runtime values: an amount's brand is a static fact the API tests check by compiling. */
const expect = (value: unknown) => typedExpect(value);

import { type Currency, currencyByCode, currencyById, defineCurrencies } from '../currencies.js';
import { CURRENCY_LIST } from '../currency-list.generated.js';
import {
  CRYPTO_CURRENCIES,
  fiatEntry,
  type ListViolation,
  type PublishedCurrency,
  parseIso4217,
  publishCurrencyList,
} from '../publish.js';

function listed(code: string): Currency {
  const found = currencyByCode(code);
  if (!found.ok) throw new Error(`${code} is not listed`);
  return found.value;
}

//#region smoo/money!n/currency-list
describe('the canonical currency list', () => {
  it('holds fiat by its ISO numeric code at six decimals, and crypto at its own scales', () => {
    expect(listed('USD')).toMatchObject({
      id: 840,
      type: 'fiat',
      minorDecimals: 2,
      baseDecimals: 6,
      runtimeDecimals: 6,
      wide: false,
      policy: 'base',
    });
    expect(listed('JPY')).toMatchObject({ id: 392, minorDecimals: 0, baseDecimals: 6 });
    expect(listed('BHD')).toMatchObject({ id: 48, minorDecimals: 3, baseDecimals: 6 });
    expect(listed('BTC')).toMatchObject({
      id: 1000,
      type: 'crypto',
      minorDecimals: 8,
      baseDecimals: 11,
      policy: 'base',
    });
    expect(listed('USDC')).toMatchObject({ id: 1001, minorDecimals: 6, baseDecimals: 6, wide: false });
    expect(listed('ETH')).toMatchObject({
      id: 1002,
      minorDecimals: 18,
      baseDecimals: 18,
      wide: true,
      policy: 'minimal',
    });
    expect(currencyById(840)).toEqual({ ok: true, value: listed('USD') });
    expect(currencyByCode('XAU')).toEqual({
      ok: false,
      error: { code: 'UNKNOWN_CURRENCY', op: 'currencyByCode', currency: 'XAU' },
    });
  });

  it('is its own inputs published again: no entry drifts from the rule that made it', () => {
    const again = publishCurrencyList(CURRENCY_LIST, CURRENCY_LIST);
    expect(again).toEqual({ ok: true, value: [...CURRENCY_LIST] });
    for (const crypto of CRYPTO_CURRENCIES) expect(listed(crypto.code)).toEqual(crypto);
  });

  it('reads ISO list one: one entry per code, an entry without numeric minor units skipped', () => {
    const xml = `<?xml version="1.0"?><ISO_4217 Pblshd="2026-09-17"><CcyTbl>
      <CcyNtry><CtryNm>A</CtryNm><CcyNm>Euro</CcyNm><Ccy>EUR</Ccy><CcyNbr>978</CcyNbr><CcyMnrUnts>2</CcyMnrUnts></CcyNtry>
      <CcyNtry><CtryNm>B</CtryNm><CcyNm>Euro</CcyNm><Ccy>EUR</Ccy><CcyNbr>978</CcyNbr><CcyMnrUnts>2</CcyMnrUnts></CcyNtry>
      <CcyNtry><CtryNm>C</CtryNm><CcyNm IsFund="true">Gold</CcyNm><Ccy>XAU</Ccy><CcyNbr>959</CcyNbr><CcyMnrUnts>N.A.</CcyMnrUnts></CcyNtry>
      <CcyNtry><CtryNm>D</CtryNm><CcyNm>Bahraini Dinar</CcyNm><Ccy>BHD</Ccy><CcyNbr>048</CcyNbr><CcyMnrUnts>3</CcyMnrUnts></CcyNtry>
    </CcyTbl></ISO_4217>`;
    expect(parseIso4217(xml)).toEqual({
      ok: true,
      value: {
        published: '2026-09-17',
        currencies: [
          { id: 978, code: 'EUR', name: 'Euro', minorDecimals: 2 },
          { id: 48, code: 'BHD', name: 'Bahraini Dinar', minorDecimals: 3 },
        ],
      },
    });
  });

  it('keeps an entry the next ISO list withdraws, and takes a raised runtime scale', () => {
    const usd = fiatEntry({ id: 840, code: 'USD', name: 'US Dollar', minorDecimals: 2 });
    const eur = fiatEntry({ id: 978, code: 'EUR', name: 'Euro', minorDecimals: 2 });
    const next = publishCurrencyList([usd, eur], [{ ...usd, runtimeDecimals: 8 }]);
    expect(next).toEqual({
      ok: true,
      value: [
        { ...usd, runtimeDecimals: 8 },
        { ...eur, withdrawn: true },
      ],
    });
  });

  it('refuses every change the append-only rule forbids, by name', () => {
    const btc = CRYPTO_CURRENCIES[0];
    const refused = (change: Partial<PublishedCurrency>, extra: PublishedCurrency[] = []): ListViolation['code'][] => {
      const result = publishCurrencyList([btc], [{ ...btc, ...change }, ...extra]);
      return result.ok ? [] : result.error.map((violation) => violation.code);
    };
    expect(refused({ code: 'XBT' })).toEqual(['CODE_CHANGED']);
    expect(refused({ type: 'fiat' })).toEqual(['TYPE_CHANGED']);
    expect(refused({ baseDecimals: 8, runtimeDecimals: 11 })).toEqual(['BASE_CHANGED']);
    expect(refused({ policy: 'minimal' })).toEqual(['POLICY_CHANGED']);
    expect(refused({ runtimeDecimals: 10 })).toEqual(['RUNTIME_LOWERED', 'SCALE_OUT_OF_RANGE']);
    expect(refused({}, [{ ...btc, id: 1003 }])).toEqual(['DUPLICATE_CODE']);
    expect(refused({}, [{ ...btc, id: 0x100_0000, code: 'BIG' }])).toEqual(['ID_OUT_OF_RANGE']);
    expect(refused({ runtimeDecimals: 43 })).toEqual(['SCALE_OUT_OF_RANGE']);
    const eth = CRYPTO_CURRENCIES[2];
    const narrowed = publishCurrencyList([eth], [{ ...eth, wide: false }]);
    expect(narrowed.ok ? [] : narrowed.error.map((violation) => violation.code)).toEqual(['WIDE_TURNED_OFF']);
  });
});
//#endregion smoo/money!n/currency-list

//#region smoo/money!n/currency-subset
describe('an accepted subset', () => {
  const accepted = defineCurrencies(['USD', 'BTC']);

  it('answers its own currencies and refuses the rest of the list', () => {
    expect(accepted.currencies.map((currency) => currency.code)).toEqual(['USD', 'BTC']);
    expect(accepted.get('USD')).toEqual({ ok: true, value: listed('USD') });
    expect(accepted.byId(1000)).toEqual({ ok: true, value: listed('BTC') });
    expect(accepted.get('EUR')).toEqual({
      ok: false,
      error: { code: 'UNKNOWN_CURRENCY', op: 'registry.get', currency: 'EUR' },
    });
    expect(accepted.byId(978).ok).toBe(false);
    expect(accepted.accepts('USD')).toBe(true);
    expect(accepted.accepts('EUR')).toBe(false);
  });

  it('refuses at definition a code the list lacks and a code named twice', () => {
    // @ts-expect-error a code the list does not hold
    expect(() => defineCurrencies(['ZZZ'])).toThrow(TypeError);
    expect(() => defineCurrencies(['USD', 'USD'])).toThrow(TypeError);
  });
});
//#endregion smoo/money!n/currency-subset
