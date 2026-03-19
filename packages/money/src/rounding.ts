/**
 * Rounding modes and Basis -> Amount conversion with explicit rounding.
 *
 * This is the single boundary where high-precision Basis values become
 * settled Amount values. No implicit truncation -- callers must choose
 * a rounding mode explicitly.
 */

import type { CurrencyDef } from './currency-registry.js';
import { Amount, type Basis } from './types.js';

/**
 * Explicit rounding modes for Basis -> Amount conversion.
 * Matches IEEE 754 / financial standard terminology.
 */
export const RoundingMode = {
  Floor: 'floor',
  Ceil: 'ceil',
  HalfEven: 'half-even',
  HalfUp: 'half-up',
} as const;

export type RoundingMode = (typeof RoundingMode)[keyof typeof RoundingMode];

/**
 * Absolute value for BigInt (not provided by JS stdlib).
 */
function bigAbs(n: bigint): bigint {
  return n < 0n ? -n : n;
}

/**
 * Convert a Basis<C> value to Amount<C> using the specified rounding mode.
 *
 * The scale difference between Basis and Amount precision determines the divisor:
 *   divisor = 10^(basisDecimals - decimals)
 *
 * For USD: basisDecimals=10, decimals=2 -> divisor=10^8
 *   Basis(15000000000n) / 10^8 = 150 -> Amount(150n)
 */
export function roundBasisToAmount<C extends string>(
  basis: Basis<C>,
  currency: CurrencyDef,
  mode: RoundingMode,
): Amount<C> {
  const scaleDiff = BigInt(currency.basisDecimals - currency.decimals);
  const divisor = 10n ** scaleDiff;

  if (divisor === 1n) {
    // No rounding needed when scales are identical
    return Amount<C>(basis as bigint);
  }

  // BigInt division truncates toward zero
  const truncated = basis / divisor;
  const remainder = basis - truncated * divisor;

  // Exact division -- no rounding needed
  if (remainder === 0n) {
    return Amount<C>(truncated);
  }

  const absRemainder = bigAbs(remainder);
  const halfDivisor = divisor / 2n;
  // Whether divisor is even (needed for exact-half detection)
  const divisorIsEven = divisor % 2n === 0n;
  const isNegative = basis < 0n;

  switch (mode) {
    case RoundingMode.Floor: {
      // Floor: toward negative infinity
      if (isNegative && remainder !== 0n) {
        return Amount<C>(truncated - 1n);
      }
      return Amount<C>(truncated);
    }

    case RoundingMode.Ceil: {
      // Ceil: toward positive infinity
      if (!isNegative && remainder !== 0n) {
        return Amount<C>(truncated + 1n);
      }
      return Amount<C>(truncated);
    }

    case RoundingMode.HalfUp: {
      // HalfUp: round away from zero on 0.5
      if (divisorIsEven && absRemainder === halfDivisor) {
        // Exactly half -- round away from zero
        return Amount<C>(isNegative ? truncated - 1n : truncated + 1n);
      }
      if (absRemainder > halfDivisor) {
        return Amount<C>(isNegative ? truncated - 1n : truncated + 1n);
      }
      return Amount<C>(truncated);
    }

    case RoundingMode.HalfEven: {
      // Banker's rounding: on exactly 0.5, round to nearest even
      if (divisorIsEven && absRemainder === halfDivisor) {
        // Exactly half -- round to even
        const absTruncated = bigAbs(truncated);
        if (absTruncated % 2n === 0n) {
          // Already even -- stay
          return Amount<C>(truncated);
        }
        // Odd -- round away from zero to make even
        return Amount<C>(isNegative ? truncated - 1n : truncated + 1n);
      }
      // Not exactly half -- round to nearest
      if (absRemainder > halfDivisor) {
        return Amount<C>(isNegative ? truncated - 1n : truncated + 1n);
      }
      return Amount<C>(truncated);
    }

    default: {
      // Exhaustive check -- should be unreachable
      const _exhaustive: never = mode;
      throw new Error(`Unknown rounding mode: ${_exhaustive}`);
    }
  }
}
