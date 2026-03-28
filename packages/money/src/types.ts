/**
 * Branded monetary types for compile-time currency safety.
 *
 * All monetary values are native BigInt with phantom type brands.
 * No runtime overhead -- branding is purely a TypeScript construct.
 */

// Unique symbol brands -- never exported, only used as type-level keys
declare const __amountBrand: unique symbol;
declare const __basisBrand: unique symbol;
declare const __unitBrand: unique symbol;

type BigIntBrand<BrandKey extends symbol, Value> = bigint & {
  readonly [Key in BrandKey]: Value;
};

type StringBrand<BrandKey extends symbol, Value> = string & {
  readonly [Key in BrandKey]: Value;
};

function brandBigInt<BrandKey extends symbol, Value>(value: bigint): BigIntBrand<BrandKey, Value>;
function brandBigInt(value: bigint): bigint {
  return value;
}

function brandString<BrandKey extends symbol, Value>(value: string): StringBrand<BrandKey, Value>;
function brandString(value: string): string {
  return value;
}

/**
 * Amount<C> -- per-currency minor units (cents for USD, satoshi for BTC).
 * This is the settled monetary value that appears in ledger entries,
 * invoice totals, and payment amounts.
 *
 * Precision is defined by the currency registry's `decimals` field.
 * i64 in Zig, giving massive headroom (e.g., $92 quadrillion for USD cents).
 */
export type Amount<C extends string = string> = BigIntBrand<typeof __amountBrand, C>;

/**
 * Constructor for Amount<C>. Currency is a phantom type, not stored at runtime.
 */
export function Amount<C extends string>(value: bigint): Amount<C> {
  return brandBigInt<typeof __amountBrand, C>(value);
}

/**
 * Basis<C> -- high-precision monetary scalar for rate calculations.
 * Precision is defined by the currency registry's `basisDecimals` field
 * (e.g., 10 for USD, 18 for ETH).
 *
 * Math stays in Basis precision to avoid intermediate rounding.
 * i128 in Zig for headroom.
 */
export type Basis<C extends string = string> = BigIntBrand<typeof __basisBrand, C>;

/**
 * Constructor for Basis<C>. Currency is a phantom type, not stored at runtime.
 */
export function Basis<C extends string>(value: bigint): Basis<C> {
  return brandBigInt<typeof __basisBrand, C>(value);
}

/**
 * Unit -- branded denominator type (per token, per millisecond, per GB, per seat).
 * Represents the "per what" in a rate.
 */
export type Unit = StringBrand<typeof __unitBrand, true>;

/**
 * Constructor for Unit. Creates a branded string denominator.
 */
export function Unit(label: string): Unit {
  return brandString<typeof __unitBrand, true>(label);
}

/**
 * Rate<C> -- compound type: Basis<C> per Unit. The pricing type.
 * `Rate<C> * quantity -> Basis<C> -> round -> Amount<C>` is the
 * boundary where rounding happens with explicit mode.
 */
export interface Rate<C extends string = string> {
  readonly value: Basis<C>;
  readonly per: Unit;
}

/**
 * Constructor for Rate<C>. Creates a plain object rate value.
 */
export function Rate<C extends string>(value: Basis<C>, per: Unit): Rate<C> {
  return { value, per };
}
