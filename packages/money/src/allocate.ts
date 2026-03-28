/**
 * Proportional allocation using the largest remainder method (Hamilton method).
 *
 * Guarantees: sum(parts) === total for all inputs.
 * Remainder cents are distributed to entries with the largest fractional parts,
 * ensuring the most mathematically fair distribution.
 */

import { Amount, type Amount as AmountValue } from './types.js';

/**
 * Distribute a total amount proportionally across weights.
 *
 * Uses the Hamilton/largest-remainder method:
 * 1. Compute each share's floor allocation: floor(total * weight / totalWeight)
 * 2. Distribute remaining units one-at-a-time to entries with largest fractional remainders
 *
 * @throws if weights array is empty or all weights sum to 0n (invariant -- programmer bug)
 */
export function allocateProportional<C extends string>(
  total: AmountValue<C>,
  weights: readonly bigint[],
): AmountValue<C>[] {
  if (weights.length === 0) {
    throw new Error('Cannot allocate to empty weights array');
  }

  const totalWeight = weights.reduce((sum, w) => sum + w, 0n);
  if (totalWeight === 0n) {
    throw new Error('Total weight must not be zero');
  }

  // Step 1: floor allocation for each weight
  const parts: bigint[] = new Array(weights.length);
  const remainders: bigint[] = new Array(weights.length);

  let allocated = 0n;
  for (let i = 0; i < weights.length; i++) {
    // Floor allocation: total * weight / totalWeight (BigInt truncates toward zero)
    const share = (total * weights[i]) / totalWeight;
    parts[i] = share;
    // Remainder is how much the "true" fractional value exceeds the floor
    // remainder_i = (total * weight_i) % totalWeight
    remainders[i] = (total * weights[i]) % totalWeight;
    allocated += share;
  }

  // Step 2: distribute leftover units to entries with largest remainders
  let leftover = total - allocated;

  if (leftover > 0n) {
    // Build index array sorted by remainder descending, then by original index ascending for stability
    const indices = Array.from({ length: weights.length }, (_, i) => i);
    indices.sort((a, b) => {
      const cmp = remainders[b] - remainders[a];
      if (cmp > 0n) return 1;
      if (cmp < 0n) return -1;
      return a - b; // stable: earlier index wins ties
    });

    for (const idx of indices) {
      if (leftover <= 0n) break;
      parts[idx] += 1n;
      leftover -= 1n;
    }
  }

  return parts.map((part) => Amount<C>(part));
}
