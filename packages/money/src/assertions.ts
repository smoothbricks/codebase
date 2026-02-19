/**
 * Journal balance assertions for double-entry accounting.
 *
 * The fundamental invariant: sum(legs) === 0n for every balanced transaction.
 * This is the compile-time + runtime guard that prevents accounting imbalances.
 */

import type { Amount } from './types.js';

/**
 * Assert that journal legs balance to zero.
 *
 * @throws if the sum of all legs is not 0n, with a descriptive message
 *   including the imbalance value (invariant -- programmer/logic bug)
 */
export function assertBalanced<C extends string>(legs: readonly Amount<C>[]): void {
  const sum = legs.reduce((acc, leg) => acc + (leg as bigint), 0n);
  if (sum !== 0n) {
    throw new Error(`Journal legs are not balanced: imbalance of ${sum}`);
  }
}

/**
 * Non-throwing variant -- returns true if legs sum to 0n.
 */
export function isBalanced<C extends string>(legs: readonly Amount<C>[]): boolean {
  const sum = legs.reduce((acc, leg) => acc + (leg as bigint), 0n);
  return sum === 0n;
}
