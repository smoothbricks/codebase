/**
 * Basis<C> arithmetic -- high-precision pure functions for rate calculations.
 *
 * Basis values live at a higher precision than Amount values (e.g., 10^10 for USD
 * vs 10^2 for Amount). Math stays in Basis precision to avoid intermediate rounding.
 *
 * Note: fromBasis is NOT provided here -- it requires a rounding mode (Plan 02).
 */

import type { CurrencyDef } from './currency-registry.js';
import type { Amount, Basis } from './types.js';

/**
 * Add two basis values of the same currency.
 */
export function basisAdd<C extends string>(a: Basis<C>, b: Basis<C>): Basis<C> {
  return (a + b) as Basis<C>;
}

/**
 * Subtract b from a (same currency, basis precision).
 */
export function basisSubtract<C extends string>(a: Basis<C>, b: Basis<C>): Basis<C> {
  return (a - b) as Basis<C>;
}

/**
 * Multiply a basis value by a scalar.
 */
export function basisMultiply<C extends string>(basis: Basis<C>, scalar: bigint): Basis<C> {
  return (basis * scalar) as Basis<C>;
}

/**
 * Convert an Amount<C> to Basis<C> by scaling up.
 * Scales by 10^(basisDecimals - decimals).
 *
 * For USD: Amount(100n) -> Basis(10_000_000_000n)
 *   because 100 * 10^(10-2) = 100 * 10^8 = 10,000,000,000
 */
export function toBasis<C extends string>(amount: Amount<C>, currency: CurrencyDef): Basis<C> {
  const scaleFactor = 10n ** BigInt(currency.basisDecimals - currency.decimals);
  return (amount * scaleFactor) as Basis<C>;
}
