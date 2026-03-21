/**
 * Branded timestamp types for compile-time safety between precision domains.
 *
 * All timestamps are native JS primitives (bigint or number) with phantom type brands.
 * No runtime overhead -- branding is purely a TypeScript construct.
 */


/** Bigint microseconds since epoch -- canonical timestamp */
export type EpochMicros = Brand<bigint, 'EpochMicros'>;

/** Constructor. Currency-style: `EpochMicros(value)` */
export function EpochMicros(value: bigint): EpochMicros {
  return value as EpochMicros;
}

/** Number milliseconds since epoch -- JS Date.getTime() domain */
export type EpochMillis = Brand<number, 'EpochMillis'>;

/** Constructor */
export function EpochMillis(value: number): EpochMillis {
  return value as EpochMillis;
}
