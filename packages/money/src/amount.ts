/**
 * Amount<C> arithmetic -- pure functions for same-currency operations.
 *
 * All operations enforce same-currency at compile time via the C generic.
 * No bare `divide` is exported -- use divideWithRemainder to avoid silent truncation.
 */

import { Amount, type Amount as AmountValue } from './types.js';

/**
 * Add two amounts of the same currency.
 */
export function add<C extends string>(a: AmountValue<C>, b: AmountValue<C>): AmountValue<C> {
  return Amount<C>(a + b);
}

/**
 * Subtract b from a (same currency).
 */
export function subtract<C extends string>(a: AmountValue<C>, b: AmountValue<C>): AmountValue<C> {
  return Amount<C>(a - b);
}

/**
 * Multiply an amount by a scalar.
 */
export function multiply<C extends string>(amount: AmountValue<C>, scalar: bigint): AmountValue<C> {
  return Amount<C>(amount * scalar);
}

/**
 * Divide an amount, returning both quotient and remainder.
 * Guarantees: quotient * divisor + remainder === original
 *
 * @throws if divisor is 0n (invariant -- programmer bug)
 */
export function divideWithRemainder<C extends string>(
  amount: AmountValue<C>,
  divisor: bigint,
): { quotient: AmountValue<C>; remainder: AmountValue<C> } {
  if (divisor === 0n) {
    // invariant throw: division by zero is a programmer bug
    throw new Error('Division by zero');
  }
  const quotient = Amount<C>(amount / divisor);
  const remainder = Amount<C>(amount % divisor);
  return { quotient, remainder };
}
