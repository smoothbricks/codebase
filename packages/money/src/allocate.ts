/**
 * Proportional allocation by the largest-remainder (Hamilton) method (specs/money/00-money.md §4): the parts sum to
 * the total exactly, and ties go to the earlier weight.
 */

import { canonical, integer, toBig } from './integer.js';
import { type Amount, brandAmount } from './types.js';

/**
 * Split `total` by non-negative integer weights.
 * @throws RangeError for no weights, a negative weight or weights summing to zero (programmer errors).
 */
export function allocateProportional<C extends string>(
  total: Amount<C>,
  weights: readonly (number | bigint)[],
): Amount<C>[] {
  if (weights.length === 0) throw new RangeError('allocateProportional: no weights');
  const w = weights.map((weight) => toBig(integer(weight, 'allocateProportional')));
  if (w.some((weight) => weight < 0n)) throw new RangeError('allocateProportional: a weight is negative');
  const whole = w.reduce((a, b) => a + b, 0n);
  if (whole === 0n) throw new RangeError('allocateProportional: the weights sum to zero');
  const t = toBig(total);
  const parts = w.map((weight) => (t * weight) / whole);
  const remainders = w.map((weight) => {
    const r = (t * weight) % whole;
    return r < 0n ? -r : r;
  });
  let left = t - parts.reduce((a, b) => a + b, 0n);
  const step = left < 0n ? -1n : 1n;
  const order = parts
    .map((_, i) => i)
    .sort((a, b) => (remainders[b] > remainders[a] ? 1 : remainders[b] < remainders[a] ? -1 : a - b));
  for (const i of order) {
    if (left === 0n) break;
    parts[i] += step;
    left -= step;
  }
  return parts.map((part) => brandAmount<C>(canonical(part)));
}
