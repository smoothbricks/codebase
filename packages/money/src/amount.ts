/**
 * Amount<C> arithmetic -- pure functions for same-currency operations.
 *
 * All operations enforce same-currency at compile time via the C generic.
 * No bare `divide` is exported -- use divideWithRemainder to avoid silent truncation.
 */

import type { Amount } from './types.js';

/**
 * Add two amounts of the same currency.
 */
export function add<C extends string>(a: Amount<C>, b: Amount<C>): Amount<C> {
  return (a + b) as Amount<C>;
}

/**
 * Subtract b from a (same currency).
 */
export function subtract<C extends string>(a: Amount<C>, b: Amount<C>): Amount<C> {
  return (a - b) as Amount<C>;
}

/**
 * Multiply an amount by a scalar.
 */
export function multiply<C extends string>(amount: Amount<C>, scalar: bigint): Amount<C> {
  return (amount * scalar) as Amount<C>;
}

/**
 * Divide an amount, returning both quotient and remainder.
 * Guarantees: quotient * divisor + remainder === original
 *
 * @throws if divisor is 0n (invariant -- programmer bug)
 */
export function divideWithRemainder<C extends string>(
  amount: Amount<C>,
  divisor: bigint,
): { quotient: Amount<C>; remainder: Amount<C> } {
  if (divisor === 0n) {
    // invariant throw: division by zero is a programmer bug
    throw new Error('Division by zero');
  }
  const quotient = (amount / divisor) as Amount<C>;
  const remainder = (amount - quotient * divisor) as Amount<C>;
  return { quotient, remainder };
}
