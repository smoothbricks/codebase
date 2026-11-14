import { describe, expect, it } from 'bun:test';
import { composeColumns, defineFields, MONEY_EXTENSION, S, validateField } from '../index.js';

//#region smoo/money!n/storage-columns
describe('money columns', () => {
  it('declares a currency cell as UInt32 and an amount cell as Int64 read as a number while safe', () => {
    const currency = S.currency();
    const total = S.money('currency');
    expect(currency.physical).toEqual({ type: 'int', bits: 32, signed: false });
    expect(total.physical).toEqual({ type: 'int', bits: 64, signed: true });
    expect(validateField('currency', currency, 840)).toEqual({ ok: true, value: 840 });
    expect(validateField('currency', currency, 0x8000_0000).ok).toBe(false);
    expect(validateField('total', total, 1.5).ok).toBe(false);
    expect(validateField('total', total, 2n ** 63n).ok).toBe(false);
    expect(total.codec.encode(total.params, 1_500_000)).toBe(1_500_000n);
    expect(total.codec.decode(total.params, 1_500_000n)).toBe(1_500_000);
    expect(total.codec.decode(total.params, 2n ** 60n)).toBe(2n ** 60n);
    expect(total.codec.decode(total.params, -(2n ** 53n) + 1n)).toBe(-(2 ** 53) + 1);
  });

  it('refuses a money field whose currency field is absent, of another kind or optional', () => {
    expect(() => defineFields('ledger', { amount: S.money('currency') })).toThrow(TypeError);
    expect(() => defineFields('ledger', { currency: S.u32(), amount: S.money('currency') })).toThrow(TypeError);
    expect(() => defineFields('ledger', { currency: S.optional(S.currency()), amount: S.money('currency') })).toThrow(
      TypeError,
    );
    expect(() => S.money('')).toThrow(TypeError);
  });

  it("writes the money extension naming its currency column, after the composer's mapping", () => {
    const invoice = defineFields('invoice', {
      currency: S.currency(),
      subtotal: S.money('currency'),
      total: S.money('currency'),
    });
    const composed = composeColumns([invoice.prefix('inv')]);
    if (!composed.ok) throw new Error('refused');
    const extensions = Object.fromEntries(composed.value.columns.map((column) => [column.name, column.extension]));
    expect(extensions).toEqual({
      inv_currency: null,
      inv_subtotal: { name: MONEY_EXTENSION, metadata: '{"currency":"inv_currency"}' },
      inv_total: { name: MONEY_EXTENSION, metadata: '{"currency":"inv_currency"}' },
    });
  });

  it('refuses a money column whose currency field has no column, and shared money columns of two currency columns', () => {
    const ledger = defineFields('ledger', { currency: S.currency(), amount: S.money('currency') });
    const dropped = composeColumns([ledger.mapColumns({ currency: null })]);
    expect(dropped.ok ? [] : dropped.error.map((refusal) => refusal.code)).toEqual(['money-without-currency']);
    expect(composeColumns([ledger.mapColumns({ currency: null, amount: null })]).ok).toBe(true);
    const other = defineFields('payout', { currency: S.currency(), amount: S.money('currency') });
    const shared = composeColumns([
      ledger.mapColumns({ amount: 'amount', currency: 'ledger_currency' }),
      other.mapColumns({ amount: 'amount', currency: 'payout_currency' }),
    ]);
    expect(shared.ok ? [] : shared.error.map((refusal) => refusal.code)).toEqual(['money-currency-columns-differ']);
  });
});
//#endregion smoo/money!n/storage-columns
