/**
 * The money value types. A currency is a phantom type parameter, invariant: an operation on two amounts compiles only
 * when both have the same single currency (specs/money/00-money.md §3).
 */

declare const amountBrand: unique symbol;
declare const minorBrand: unique symbol;
declare const basisBrand: unique symbol;
declare const unitBrand: unique symbol;

/** A brand that is invariant in C: a function type is contravariant in its parameter and covariant in its result. */
type Invariant<Key extends symbol, C> = { readonly [K in Key]: (currency: C) => C };

/**
 * An amount: the integer of C at C's `runtimeDecimals`. A `number` while it is a safe integer, a `bigint` beyond, so
 * equal amounts are `===`. Never apply operators to it: `add`, `subtract` and the rest take the number path and
 * widen exactly.
 */
export type Amount<C extends string> = (number | bigint) & Invariant<typeof amountBrand, C>;

/** An amount at C's `minorDecimals`, the display and settlement unit. Made only by `toMinor` and `minor`. */
export type Minor<C extends string> = (number | bigint) & Invariant<typeof minorBrand, C>;

/** A rate-math scalar at C's `basisDecimals` (`runtimeDecimals + 4`). */
export type Basis<C extends string> = bigint & Invariant<typeof basisBrand, C>;

/** The denominator of a rate: per token, per second, per seat. */
export type Unit = string & { readonly [unitBrand]: true };

/** A price: `value` of C per one `per`. */
export interface Rate<C extends string> {
  readonly value: Basis<C>;
  readonly per: Unit;
}

/**
 * A money value: its currency and its amount. Distributive: a `Money` of a union of currencies is a union
 * discriminated by `currency`, so comparing `currency` to a code narrows the amount's type.
 */
export type Money<C extends string> = C extends string ? { readonly currency: C; readonly amount: Amount<C> } : never;

export function brandAmount<C extends string>(value: number | bigint): Amount<C>;
export function brandAmount(value: number | bigint): number | bigint {
  return value;
}

export function brandMinor<C extends string>(value: number | bigint): Minor<C>;
export function brandMinor(value: number | bigint): number | bigint {
  return value;
}

export function brandBasis<C extends string>(value: bigint): Basis<C>;
export function brandBasis(value: bigint): bigint {
  return value;
}

/**
 * A money value rebuilt from a `Money<Code>`'s own currency and a new amount of it. TypeScript reads a generic
 * `Money<Code>`'s currency as `string`, so the currency is taken as data here and the caller keeps it from the same value.
 */
export function moneyOfCode<Code extends string>(currency: string, amount: number | bigint): Money<Code>;
export function moneyOfCode(currency: string, amount: number | bigint): { currency: string; amount: number | bigint } {
  return { currency, amount };
}

export function brandMoney<C extends string>(currency: C, amount: Amount<C>): Money<C>;
export function brandMoney(currency: string, amount: number | bigint): { currency: string; amount: number | bigint } {
  return { currency, amount };
}

/** A rate denominator. */
export function unit(label: string): Unit;
export function unit(label: string): string {
  return label;
}

/** A price: `value` per one `per`. */
export function rate<C extends string>(value: Basis<C>, per: Unit): Rate<C> {
  return { value, per };
}
