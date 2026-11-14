/**
 * The one rounding step (specs/money/00-money.md §6): a named mode, and the remainder returned beside the value.
 */

import { scaleUp } from './amount.js';
import type { Currency } from './currencies.js';
import { canonical, integer, pow10, toBig } from './integer.js';
import { type Amount, type Basis, brandAmount, brandBasis, brandMinor, type Minor } from './types.js';

export const RoundingMode = {
  /** Toward negative infinity. */
  Floor: 'floor',
  /** Toward positive infinity. */
  Ceil: 'ceil',
  /** Truncation. */
  TowardZero: 'toward-zero',
  /** To nearest; a tie goes away from zero. */
  HalfUp: 'half-up',
  /** To nearest; a tie goes to the even neighbour. */
  HalfEven: 'half-even',
} as const;

export type RoundingMode = (typeof RoundingMode)[keyof typeof RoundingMode];

/** `n / 10^k` rounded by `mode`, with `n = quotient × 10^k + remainder` exactly. */
export function divideRounded(n: bigint, k: number, mode: RoundingMode): { quotient: bigint; remainder: bigint } {
  if (k === 0) return { quotient: n, remainder: 0n };
  const d = pow10(k);
  let q = n / d;
  const r = n - q * d;
  if (r !== 0n) {
    const twice = (r < 0n ? -r : r) * 2n;
    let away: boolean;
    switch (mode) {
      case 'floor':
        away = n < 0n;
        break;
      case 'ceil':
        away = n > 0n;
        break;
      case 'toward-zero':
        away = false;
        break;
      case 'half-up':
        away = twice >= d;
        break;
      case 'half-even':
        away = twice > d || (twice === d && (q & 1n) !== 0n);
        break;
      default: {
        const unknown: never = mode;
        throw new TypeError(`unknown rounding mode ${String(unknown)}`);
      }
    }
    if (away) q += n < 0n ? -1n : 1n;
  }
  return { quotient: q, remainder: n - q * d };
}

/** A minor-unit amount from a payment rail or a document, in its one spelling. */
export function minor<C extends string>(value: number | bigint): Minor<C> {
  return brandMinor<C>(integer(value, 'minor'));
}

/**
 * The amount in C's minor units, rounded by `mode`; the remainder is in runtime units, so
 * `fromMinor(minor) + remainder = amount`.
 */
export function toMinor<C extends string>(
  amount: Amount<C>,
  currency: Currency<C>,
  mode: RoundingMode,
): { readonly minor: Minor<C>; readonly remainder: Amount<C> } {
  const { quotient, remainder } = divideRounded(toBig(amount), currency.runtimeDecimals - currency.minorDecimals, mode);
  return { minor: brandMinor<C>(canonical(quotient)), remainder: brandAmount<C>(canonical(remainder)) };
}

/** A minor-unit amount in runtime units: exact. */
export function fromMinor<C extends string>(value: Minor<C>, currency: Currency<C>): Amount<C> {
  return brandAmount<C>(scaleUp(value, currency.runtimeDecimals - currency.minorDecimals));
}

/** A basis value rounded to the runtime scale by `mode`; the remainder stays in basis units. */
export function roundBasis<C extends string>(
  basis: Basis<C>,
  mode: RoundingMode,
): { readonly amount: Amount<C>; readonly remainder: Basis<C> } {
  const { quotient, remainder } = divideRounded(basis, BASIS_EXTRA_DECIMALS, mode);
  return { amount: brandAmount<C>(canonical(quotient)), remainder: brandBasis<C>(remainder) };
}

/** How much finer a basis is than an amount: `basisDecimals = runtimeDecimals + 4`. */
export const BASIS_EXTRA_DECIMALS = 4;
