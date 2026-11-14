/**
 * Amount arithmetic (specs/money/00-money.md §4): exact, number while safe, widening to bigint exactly when not.
 */

import { canonical, integer, isSafe, pow10, toBig } from './integer.js';
import { type Amount, brandAmount } from './types.js';

/**
 * An amount of a currency known in the type, from an integer of its runtime units.
 * @throws TypeError for a number that is not an integer (a programmer error; data goes through `money`).
 */
export function amount<C extends string>(value: number | bigint): Amount<C> {
  return brandAmount<C>(integer(value, 'amount'));
}

/** The sum of two integers in their one spelling. Used by the currency-as-data operations. */
export function addIntegers(a: number | bigint, b: number | bigint): number | bigint {
  if (typeof a === 'number' && typeof b === 'number') {
    const s = a + b;
    if (isSafe(s)) return s;
  }
  return canonical(toBig(a) + toBig(b));
}

export function add<C extends string>(a: Amount<C>, b: Amount<C>): Amount<C> {
  return brandAmount<C>(addIntegers(a, b));
}

export function subtract<C extends string>(a: Amount<C>, b: Amount<C>): Amount<C> {
  if (typeof a === 'number' && typeof b === 'number') {
    const s = a - b;
    if (isSafe(s)) return brandAmount<C>(s);
  }
  return brandAmount<C>(canonical(toBig(a) - toBig(b)));
}

export function negate<C extends string>(a: Amount<C>): Amount<C> {
  return brandAmount<C>(typeof a === 'number' ? 0 - a : canonical(-a));
}

/** The amount times an integer factor. */
export function multiply<C extends string>(a: Amount<C>, factor: number | bigint): Amount<C> {
  const f = integer(factor, 'multiply');
  if (typeof a === 'number' && typeof f === 'number') {
    const p = a * f;
    if (isSafe(p)) return brandAmount<C>(p === 0 ? 0 : p);
  }
  return brandAmount<C>(canonical(toBig(a) * toBig(f)));
}

/**
 * Quotient and remainder of an integer division, truncating toward zero: `quotient × divisor + remainder = amount`.
 * @throws RangeError for a zero divisor (a programmer error).
 */
export function divideWithRemainder<C extends string>(
  a: Amount<C>,
  divisor: number | bigint,
): { readonly quotient: Amount<C>; readonly remainder: Amount<C> } {
  const d = toBig(integer(divisor, 'divideWithRemainder'));
  if (d === 0n) throw new RangeError('divideWithRemainder: division by zero');
  const n = toBig(a);
  return { quotient: brandAmount<C>(canonical(n / d)), remainder: brandAmount<C>(canonical(n % d)) };
}

/** -1, 0 or 1 as `a` is below, equal to or above `b`. */
export function compare<C extends string>(a: Amount<C>, b: Amount<C>): -1 | 0 | 1 {
  return a < b ? -1 : a > b ? 1 : 0;
}

export function isZero<C extends string>(a: Amount<C>): boolean {
  return a === 0;
}

/** The exact sum: a number accumulator for the values that fit, a bigint for the rest. */
export function sum<C extends string>(values: readonly Amount<C>[]): Amount<C> {
  return brandAmount<C>(sumIntegers(values));
}

export function sumIntegers(values: readonly (number | bigint)[]): number | bigint {
  let acc = 0;
  let side = 0n;
  for (const v of values) {
    if (typeof v === 'number') {
      const s = acc + v;
      if (isSafe(s)) {
        acc = s;
        continue;
      }
      side += BigInt(acc);
      acc = v;
      continue;
    }
    side += v;
  }
  return side === 0n ? acc : canonical(side + BigInt(acc));
}

/** The integer as a bigint, for code that leaves the money operations (a wire, a column). */
export function toBigInt<C extends string>(a: Amount<C>): bigint {
  return toBig(a);
}

/** The integer times 10^k, exactly. */
export function scaleUp(value: number | bigint, k: number): number | bigint {
  return k === 0 ? value : canonical(toBig(value) * pow10(k));
}
