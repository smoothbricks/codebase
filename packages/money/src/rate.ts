/**
 * Rate operations -- apply a Rate<C> to a quantity to produce a Basis<C> total.
 *
 * Rate * quantity stays in Basis precision. The consumer is responsible
 * for rounding to Amount via roundBasisToAmount when settling.
 */

import { Basis, type Basis as BasisValue, type Rate } from './types.js';

/**
 * Multiply a rate by a quantity to produce a Basis total.
 *
 * The result stays in Basis precision -- no rounding occurs here.
 * Use roundBasisToAmount to convert to settled Amount values.
 *
 * @example
 * ```ts
 * const rate = Rate<'USD'>(Basis<'USD'>(166667n), Unit('GB-s'));
 * const total = applyRate(rate, 1000n); // Basis<'USD'>(166667000n)
 * const settled = roundBasisToAmount(total, usd, RoundingMode.HalfUp);
 * ```
 */
export function applyRate<C extends string>(rate: Rate<C>, quantity: bigint): BasisValue<C> {
  return Basis<C>(rate.value * quantity);
}
