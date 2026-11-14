import { describe, it, expect as typedExpect } from 'bun:test';

/** Assertions compare runtime values: an amount's brand is a static fact the API tests check by compiling. */
const expect = (value: unknown) => typedExpect(value);

import { amount } from '../amount.js';
import { cellCurrencyId, combineVariantSums, decodeCell, encodeCells, int64FromWords, int64ToWords } from '../cell.js';
import { type Currency, type CurrencyRegistry, currencyByCode, defineCurrencies } from '../currencies.js';
import type { MoneyViolation } from '../errors.js';
import { err, ok } from '../result.js';
import type { Amount } from '../types.js';

function listed<C extends string>(code: C): Currency<C> {
  const found = currencyByCode(code);
  if (!found.ok) throw new Error(code);
  return { ...found.value, code };
}

const offsetCell = (id: number, offset: number) => id | ((offset & 63) << 24);
const accepted = defineCurrencies(['USD', 'BTC', 'ETH']);

/** A registry whose BTC was raised to 14 decimals: the list's own entries otherwise. */
function raisedBtc(): CurrencyRegistry<'BTC'> {
  const btc = { ...listed('BTC'), runtimeDecimals: 14 };
  const refusal = (currency: string | number): MoneyViolation => ({ code: 'UNKNOWN_CURRENCY', op: 'test', currency });
  return {
    currencies: [btc],
    accepts: (code): code is 'BTC' => code === 'BTC',
    get: (code) => (code === 'BTC' ? ok(btc) : err(refusal(code))),
    byId: (id) => (id === btc.id ? ok(btc) : err(refusal(id))),
  };
}

/** One amount's stored form, as the single money field of its row. */
function encodeOne<C extends string>(currency: Currency<C>, value: Amount<C>) {
  const stored = encodeCells(currency, [value]);
  return stored.ok ? ok({ cell: stored.value.cell, value: stored.value.values[0] }) : stored;
}

function roundTrips<R extends string, C extends string>(
  registry: CurrencyRegistry<R>,
  currency: Currency<C>,
  value: Amount<C>,
) {
  const stored = encodeOne(currency, value);
  if (!stored.ok) throw new Error(`${currency.code} ${value} refused`);
  expect(decodeCell(registry, stored.value.cell, stored.value.value)).toEqual({
    ok: true,
    value: { currency: currency.code, amount: value },
  });
  return stored.value;
}

//#region smoo/money!n/storage-cell
describe('the variant cell', () => {
  it('writes a base-scale value with the plain currency id as its cell', () => {
    expect(encodeOne(listed('USD'), amount<'USD'>(1_500_000))).toEqual(ok({ cell: 840, value: 1_500_000 }));
    expect(encodeOne(listed('BTC'), amount<'BTC'>(2_100_000_000_000_000_000n))).toEqual(
      ok({ cell: 1000, value: 2_100_000_000_000_000_000n }),
    );
    expect(encodeOne(listed('ETH'), amount<'ETH'>(1_234_567_890_123_456_789n))).toEqual(
      ok({ cell: 1002, value: 1_234_567_890_123_456_789n }),
    );
    expect(encodeOne(listed('ETH'), amount<'ETH'>(0))).toEqual(ok({ cell: 1002, value: 0 }));
  });

  it('under base, moves a round value too large for 64 bits to the largest coarser scale that holds it', () => {
    const usd = listed('USD');
    const stored = roundTrips(accepted, usd, amount<'USD'>(10n ** 19n));
    expect(stored).toEqual({ cell: offsetCell(840, -1), value: 10n ** 18n });
  });

  it('under minimal, writes the smallest mantissa: 200 ETH is 2 at scale -2, half an ETH 5 at scale 1', () => {
    const eth = listed('ETH');
    expect(roundTrips(accepted, eth, amount<'ETH'>(200n * 10n ** 18n))).toEqual({
      cell: offsetCell(1002, -20),
      value: 2,
    });
    expect(roundTrips(accepted, eth, amount<'ETH'>(5n * 10n ** 17n))).toEqual({
      cell: offsetCell(1002, -17),
      value: 5,
    });
    expect(roundTrips(accepted, eth, amount<'ETH'>(-(10n ** 33n)))).toEqual({
      cell: offsetCell(1002, -32),
      value: -10,
    });
  });

  it("chooses one variant for a row's amounts that share a currency cell", () => {
    const eth = listed('ETH');
    expect(encodeCells(eth, [amount<'ETH'>(200n * 10n ** 18n), amount<'ETH'>(5n * 10n ** 17n)])).toEqual(
      ok({ cell: offsetCell(1002, -17), values: [2_000, 5] }),
    );
    expect(encodeCells(listed('USD'), [amount<'USD'>(1_500_000), amount<'USD'>(42)])).toEqual(
      ok({ cell: 840, values: [1_500_000, 42] }),
    );
    expect(encodeCells(listed('USD'), [amount<'USD'>(10n ** 19n), amount<'USD'>(1)]).ok).toBe(false);
    expect(encodeCells(eth, [amount<'ETH'>(0), amount<'ETH'>(0)])).toEqual(ok({ cell: 1002, values: [0, 0] }));
  });

  it('refuses a value no narrow variant holds, naming it: the wide variant is not built', () => {
    const precise = 123_456_789_012_345_678_901n;
    expect(encodeOne(listed('ETH'), amount<'ETH'>(precise))).toEqual(
      err({ code: 'OUT_OF_BACKING', op: 'encodeCells', currency: 'ETH', amount: precise.toString() }),
    );
    expect(decodeCell(accepted, 1002 | 0x4000_0000, 1)).toEqual(
      err({
        code: 'INVALID_CELL',
        op: 'decodeCell',
        cell: 1002 | 0x4000_0000,
        reason: 'the wide variant is not built',
      }),
    );
  });

  it('subdivides without a migration: an old cell keeps its meaning after a raised runtime scale', () => {
    const registry = raisedBtc();
    const raised = registry.currencies[0];
    expect(decodeCell(registry, 1000, 1)).toEqual(ok({ currency: 'BTC', amount: 1_000 }));
    expect(roundTrips(registry, raised, amount<'BTC'>(1_000))).toEqual({ cell: 1000, value: 1 });
    expect(roundTrips(registry, raised, amount<'BTC'>(123_456_789_012_345))).toEqual({
      cell: offsetCell(1000, 3),
      value: 123_456_789_012_345,
    });
    expect(roundTrips(registry, raised, amount<'BTC'>(1_230))).toEqual({ cell: offsetCell(1000, 2), value: 123 });
  });

  it('refuses a cell it cannot read', () => {
    const refusal = (cell: number) => decodeCell(accepted, cell, 1);
    expect(refusal(978)).toEqual(err({ code: 'UNKNOWN_CURRENCY', op: 'registry.byId', currency: 978 }));
    expect(refusal(offsetCell(840, 1)).ok).toBe(false);
    expect(refusal(-1).ok).toBe(false);
    expect(cellCurrencyId(offsetCell(1002, -20))).toBe(1002);
  });

  it('round-trips every value of a run, one cell per amount', () => {
    let seed = 0x2545f491;
    const next = () => {
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;
      return (seed >>> 0) / 4294967296;
    };
    const digits = (n: number) => {
      let v = 0n;
      for (let i = 0; i < n; i++) v = v * 10n + BigInt(Math.floor(next() * 10));
      return v;
    };
    for (let i = 0; i < 2000; i++) {
      const sign = next() < 0.5 ? -1n : 1n;
      roundTrips(accepted, listed('USD'), amount<'USD'>(sign * digits(1 + Math.floor(next() * 18))));
      roundTrips(accepted, listed('BTC'), amount<'BTC'>(sign * digits(1 + Math.floor(next() * 18))));
      const eth = sign * digits(1 + Math.floor(next() * 4)) * 10n ** BigInt(Math.floor(next() * 22));
      roundTrips(accepted, listed('ETH'), amount<'ETH'>(eth));
    }
  });
});
//#endregion smoo/money!n/storage-cell

//#region smoo/money!n/reducer-sums
describe('combining variant sums', () => {
  it("adds each currency's variant groups at its runtime scale, exactly", () => {
    const totals = combineVariantSums(accepted, [
      [840, 1_500_000],
      [1002, 7],
      [offsetCell(1002, -20), 3],
      [offsetCell(1002, -17), -1],
      [840, 2n ** 60n],
    ]);
    expect(totals).toEqual(
      ok([
        { currency: 'USD', amount: 2n ** 60n + 1_500_000n },
        { currency: 'ETH', amount: 300n * 10n ** 18n - 10n ** 17n + 7n },
      ]),
    );
  });
});
//#endregion smoo/money!n/reducer-sums

//#region smoo/money!n/storage-cell
describe('64-bit cells as words', () => {
  it('reads a number while safe and a bigint beyond, and writes both back', () => {
    const words = new Uint32Array(2);
    const cells = new BigInt64Array(words.buffer);
    const values = [
      0,
      -1,
      1_500_000,
      2 ** 53 - 1,
      -(2 ** 53 - 1),
      -(2n ** 53n),
      2n ** 53n,
      -(2n ** 63n),
      2n ** 63n - 1n,
    ];
    for (const value of values) {
      int64ToWords(value, words, 0);
      expect(cells[0]).toBe(BigInt(value));
      expect(int64FromWords(words[0], words[1] | 0)).toBe(value);
    }
  });
});
//#endregion smoo/money!n/storage-cell
