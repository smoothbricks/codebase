/**
 * Branded timestamp types for compile-time safety between precision domains.
 *
 * All timestamps are native JS primitives (bigint or number) with phantom type brands.
 * No runtime overhead -- branding is purely a TypeScript construct.
 */

// Inline Brand to avoid circular dependencies.
// Phantom brand pattern for compile-time safety.
declare const __brand: unique symbol;
type Brand<T, B> = T & { readonly [__brand]: B };

function brandBigint<B>(value: bigint): Brand<bigint, B>;
function brandBigint(value: bigint): bigint {
  return value;
}

function brandNumber<B>(value: number): Brand<number, B>;
function brandNumber(value: number): number {
  return value;
}

/** Bigint microseconds since epoch -- canonical timestamp */
export type EpochMicros = Brand<bigint, 'EpochMicros'>;

/** Constructor. Currency-style: `EpochMicros(value)` */
export function EpochMicros(value: bigint): EpochMicros {
  return brandBigint<'EpochMicros'>(value);
}

/** Number milliseconds since epoch -- JS Date.getTime() domain */
export type EpochMillis = Brand<number, 'EpochMillis'>;

/** Constructor */
export function EpochMillis(value: number): EpochMillis {
  return brandNumber<'EpochMillis'>(value);
}
