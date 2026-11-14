/**
 * FX conversion (specs/money/00-money.md §7): a rate per whole unit, one rounding, the remainder and the audit returned.
 */

import type { Currency } from './currencies.js';
import { canonical, integer, pow10, toBig } from './integer.js';
import { divideRounded, type RoundingMode } from './rounding.js';
import { type Amount, brandAmount } from './types.js';

/** `basis / 10^scale` units of `To` per one unit of `From`. */
export interface FxRate<From extends string, To extends string> {
  readonly from: From;
  readonly to: To;
  readonly basis: bigint;
  readonly scale: number;
}

/**
 * A rate: `basis / 10^scale` units of `to` per one unit of `from` (0.92 is basis 92, scale 2).
 * @throws TypeError for a non-integer basis or a scale that is not a non-negative integer (programmer errors).
 */
export function fxRate<From extends string, To extends string>(
  from: From,
  to: To,
  basis: number | bigint,
  scale: number,
): FxRate<From, To> {
  if (!Number.isInteger(scale) || scale < 0)
    throw new TypeError(`fxRate: scale ${scale} is not a non-negative integer`);
  return { from, to, basis: toBig(integer(basis, 'fxRate')), scale };
}

/** An exact decimal residual: `units / 10^scale` of the target currency. */
export interface Residual {
  readonly units: bigint;
  readonly scale: number;
}

export interface FxResult<From extends string, To extends string> {
  readonly result: Amount<To>;
  /** What rounding left out, exactly: `amount × rate = result + remainder`. */
  readonly remainder: Residual;
  readonly audit: { readonly amount: Amount<From>; readonly rate: FxRate<From, To>; readonly mode: RoundingMode };
}

/** Convert at a rate: one exact multiplication, one rounding to `to`'s runtime scale. */
export function convertFx<From extends string, To extends string>(
  amount: Amount<From>,
  rate: FxRate<From, To>,
  from: Currency<From>,
  to: Currency<To>,
  mode: RoundingMode,
): FxResult<From, To> {
  const product = toBig(amount) * rate.basis;
  const scale = from.runtimeDecimals + rate.scale;
  const audit = { amount, rate, mode };
  if (scale <= to.runtimeDecimals) {
    const result = brandAmount<To>(canonical(product * pow10(to.runtimeDecimals - scale)));
    return { result, remainder: { units: 0n, scale: to.runtimeDecimals }, audit };
  }
  const { quotient, remainder } = divideRounded(product, scale - to.runtimeDecimals, mode);
  return { result: brandAmount<To>(canonical(quotient)), remainder: { units: remainder, scale }, audit };
}
