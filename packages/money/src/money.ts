/**
 * Money values whose currency is data (specs/money/00-money.md §3): the checked door from data, and the operations
 * that return a currency mismatch or an imbalance as a value.
 */

import { addIntegers, sumIntegers } from './amount.js';
import { encodeCells } from './cell.js';
import type { Currency, CurrencyRegistry } from './currencies.js';
import type { MoneyViolation } from './errors.js';
import { formatAmount, parseAmount } from './format.js';
import { canonical, isSafe, toBig } from './integer.js';
import { err, ok, type Result } from './result.js';
import { type Amount, brandAmount, brandMoney, type Money, moneyOfCode } from './types.js';

/** A money value of a currency known in the type. */
export function moneyOf<C extends string>(currency: Currency<C>, amount: Amount<C>): Money<C> {
  return brandMoney(currency.code, amount);
}

/**
 * The one door from data: a currency the registry accepts and an integer of its runtime units that a stored cell can
 * hold (spec §8). A non-integer number is refused as invalid text of the value.
 */
export function money<Code extends string>(
  registry: CurrencyRegistry<Code>,
  code: string,
  value: number | bigint,
): Result<Money<Code>, MoneyViolation> {
  const found = registry.get(code);
  if (!found.ok) return err({ ...found.error, op: 'money' });
  if (typeof value === 'number' && !Number.isInteger(value)) {
    return err({ code: 'INVALID_TEXT', op: 'money', text: String(value) });
  }
  const amount = brandAmount<Code>(typeof value === 'number' && isSafe(value) ? value : canonical(toBig(value)));
  const stored = encodeCells(found.value, [amount]);
  if (!stored.ok) return err({ ...stored.error, op: 'money' });
  return ok(brandMoney(found.value.code, amount));
}

/** A money value from its wire form: a currency code and exact decimal text. */
export function moneyFromWire<Code extends string>(
  registry: CurrencyRegistry<Code>,
  wire: { readonly currency: string; readonly amount: string },
): Result<Money<Code>, MoneyViolation> {
  const found = registry.get(wire.currency);
  if (!found.ok) return err({ ...found.error, op: 'moneyFromWire' });
  const parsed = parseAmount(wire.amount, found.value);
  if (!parsed.ok) return err({ ...parsed.error, op: 'moneyFromWire' });
  return money(registry, wire.currency, parsed.value);
}

/** The wire form of a money value: its code and its exact decimal text. */
export function moneyToWire<Code extends string>(
  registry: CurrencyRegistry<Code>,
  value: Money<Code>,
): Result<{ readonly currency: string; readonly amount: string }, MoneyViolation> {
  const found = registry.get(value.currency);
  if (!found.ok) return err({ ...found.error, op: 'moneyToWire' });
  return ok({ currency: value.currency, amount: formatAmount(brandAmount(value.amount), found.value) });
}

function mismatch(op: string, left: string, right: string): Result<never, MoneyViolation> {
  return err({ code: 'CURRENCY_MISMATCH', op, left, right });
}

/** The sum of two money values of one currency. */
export function addMoney<Code extends string>(a: Money<Code>, b: Money<Code>): Result<Money<Code>, MoneyViolation> {
  if (a.currency !== b.currency) return mismatch('addMoney', a.currency, b.currency);
  return ok(moneyOfCode<Code>(a.currency, addIntegers(a.amount, b.amount)));
}

/** The sum of money values, all of `currency`. */
export function sumMoney<Code extends string>(
  currency: Code,
  values: readonly Money<Code>[],
): Result<Money<Code>, MoneyViolation> {
  const amounts: (number | bigint)[] = [];
  for (const value of values) {
    if (value.currency !== currency) return mismatch('sumMoney', currency, value.currency);
    amounts.push(value.amount);
  }
  return ok(brandMoney(currency, brandAmount(sumIntegers(amounts))));
}

/** -1, 0 or 1 as `a` is below, equal to or above `b`, of one currency. */
export function compareMoney<Code extends string>(a: Money<Code>, b: Money<Code>): Result<-1 | 0 | 1, MoneyViolation> {
  if (a.currency !== b.currency) return mismatch('compareMoney', a.currency, b.currency);
  return ok(a.amount < b.amount ? -1 : a.amount > b.amount ? 1 : 0);
}

/** Whether signed legs of one currency sum to zero; the imbalance is the refusal. */
export function checkBalanced<C extends string>(
  currency: Currency<C>,
  legs: readonly Amount<C>[],
): Result<void, MoneyViolation> {
  const total = sumIntegers(legs);
  return total === 0
    ? ok(undefined)
    : err({ code: 'UNBALANCED', op: 'checkBalanced', currency: currency.code, imbalance: String(total) });
}
