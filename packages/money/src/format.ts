/**
 * Exact decimal text (specs/money/00-money.md §3, §5): one formatter and its parser, driven by the currency list.
 */

import type { Currency } from './currencies.js';
import type { MoneyViolation } from './errors.js';
import { canonical, toBig } from './integer.js';
import { err, ok, type Result } from './result.js';
import { divideRounded, RoundingMode } from './rounding.js';
import { type Amount, brandAmount } from './types.js';

/** `value / 10^scale` as decimal text with at least `minFraction` fraction digits and no other trailing zero. */
export function decimalText(value: number | bigint, scale: number, minFraction: number): string {
  const n = toBig(value);
  const negative = n < 0n;
  const digits = (negative ? -n : n).toString().padStart(scale + 1, '0');
  const whole = digits.slice(0, digits.length - scale);
  let fraction = digits.slice(digits.length - scale);
  let end = fraction.length;
  while (end > minFraction && fraction[end - 1] === '0') end--;
  fraction = fraction.slice(0, end);
  return `${negative ? '-' : ''}${whole}${fraction.length > 0 ? `.${fraction}` : ''}`;
}

/** The exact amount: at least the currency's minor digits, and every nonzero digit to its runtime scale. */
export function formatAmount<C extends string>(amount: Amount<C>, currency: Currency<C>): string {
  return decimalText(amount, currency.runtimeDecimals, currency.minorDecimals);
}

/**
 * The amount's identity text (spec §5): every trailing fraction zero removed, so it does not depend on any scale.
 * Equality, dedup keys and content hashes use it with the currency id.
 */
export function identityText<C extends string>(amount: Amount<C>, currency: Currency<C>): string {
  return decimalText(amount, currency.runtimeDecimals, 0);
}

const DECIMAL = /^(-?)(0|[1-9][0-9]*)(?:\.([0-9]+))?$/;

/** Exact decimal text of C into an amount; refuses other text and a nonzero digit past the runtime scale. */
export function parseAmount<C extends string>(text: string, currency: Currency<C>): Result<Amount<C>, MoneyViolation> {
  const match = DECIMAL.exec(text);
  if (match === null) return err({ code: 'INVALID_TEXT', op: 'parseAmount', text });
  const [, sign, whole, given = ''] = match;
  const scale = currency.runtimeDecimals;
  const extra = given.slice(scale);
  if (/[1-9]/.test(extra)) {
    return err({
      code: 'FINER_THAN_RUNTIME',
      op: 'parseAmount',
      currency: currency.code,
      text,
      runtimeDecimals: scale,
    });
  }
  const n = BigInt(whole + given.slice(0, scale).padEnd(scale, '0'));
  return ok(brandAmount<C>(canonical(sign === '-' ? -n : n)));
}

/**
 * The amount for a person: rounded to the minor unit by `mode` and formatted by `Intl` for the locale. A currency
 * `Intl` does not know (a crypto asset) is formatted as a number followed by its code. Exact: the digits never pass
 * through a float.
 */
export function formatAmountDisplay<C extends string>(
  amount: Amount<C>,
  currency: Currency<C>,
  locale?: string,
  mode: RoundingMode = RoundingMode.HalfUp,
): string {
  const scale = currency.minorDecimals;
  const minorUnits = divideRounded(toBig(amount), currency.runtimeDecimals - scale, mode).quotient;
  const negative = minorUnits < 0n;
  const magnitude = negative ? -minorUnits : minorUnits;
  const whole = magnitude / 10n ** BigInt(scale);
  const fraction = (magnitude % 10n ** BigInt(scale)).toString().padStart(scale, '0');
  const fiat = currency.type === 'fiat';
  const format = new Intl.NumberFormat(locale, {
    ...(fiat ? { style: 'currency', currency: currency.code } : { style: 'decimal' }),
    minimumFractionDigits: scale,
    maximumFractionDigits: scale,
  });
  const signed: number | bigint = negative ? (whole === 0n ? -0 : -whole) : whole;
  const text = format
    .formatToParts(signed)
    .map((part) => (part.type === 'fraction' ? fraction : part.value))
    .join('');
  return fiat ? text : `${text} ${currency.code}`;
}
