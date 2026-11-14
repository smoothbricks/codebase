/**
 * Rate arithmetic in basis precision (`runtimeDecimals + 4`), exact; `roundBasis` is the way back to an amount.
 */

import { integer, pow10, toBig } from './integer.js';
import { BASIS_EXTRA_DECIMALS } from './rounding.js';
import { type Amount, type Basis, brandBasis, type Rate } from './types.js';

/** The amount in basis units: exact. */
export function toBasis<C extends string>(amount: Amount<C>): Basis<C> {
  return brandBasis<C>(toBig(amount) * pow10(BASIS_EXTRA_DECIMALS));
}

export function basisAdd<C extends string>(a: Basis<C>, b: Basis<C>): Basis<C> {
  return brandBasis<C>(a + b);
}

export function basisSubtract<C extends string>(a: Basis<C>, b: Basis<C>): Basis<C> {
  return brandBasis<C>(a - b);
}

export function basisMultiply<C extends string>(a: Basis<C>, factor: number | bigint): Basis<C> {
  return brandBasis<C>(a * toBig(integer(factor, 'basisMultiply')));
}

/** A basis value from an integer of basis units. */
export function basis<C extends string>(value: number | bigint): Basis<C> {
  return brandBasis<C>(toBig(integer(value, 'basis')));
}

/** A rate times an integer quantity of its unit: exact, in basis units. */
export function applyRate<C extends string>(rate: Rate<C>, quantity: number | bigint): Basis<C> {
  return brandBasis<C>(rate.value * toBig(integer(quantity, 'applyRate')));
}
