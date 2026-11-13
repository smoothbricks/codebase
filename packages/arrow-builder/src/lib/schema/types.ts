/**
 * Column types of the buffer layer, on the field node (field.ts).
 *
 * A field's `kind` says what its value is. The buffer layer stores a column in one of a few lanes; `StorageLane`
 * names them, and `storageLaneOf` (typeGuards.ts) is the one mapping from a field to its lane.
 */

import type { EnumParams, Field, FieldHints, MaskTransform } from './field.js';

/**
 * How the buffer layer stores a column while rows are written:
 * - enum: Uint8/16/32Array of ordinals
 * - category, text: string[]
 * - number: Float64Array
 * - boolean: Uint8Array (bit-packed)
 * - bigUint64: BigUint64Array
 * - binary: unknown[] (values encoded when the column is written out)
 */
export type StorageLane = 'enum' | 'category' | 'text' | 'number' | 'boolean' | 'bigUint64' | 'binary';

/** An eager column is allocated up front and has no validity bitmap: it is written on every row. */
export type EagerField<F extends Field> = F & { readonly hints: FieldHints & { readonly eager: true } };

/** A lazy column is allocated on first write and has a validity bitmap. `.eager()` gives its eager form. */
export type LazyField<F extends Field> = F & { readonly hints: FieldHints & { readonly eager: false } } & {
  /** Mark this column as eager (always written, no validity bitmap). */
  eager(): EagerField<F>;
};

/** Masking preset names. Applied where a column is written out (cold path), never on a hot-path write. */
export type MaskPreset = 'hash' | 'url' | 'sql' | 'email';

/** A lazy string column that can be masked where it is written out. */
export type MaskableField<F extends Field> = LazyField<F> & {
  /** The same column with a masking transform applied where it is written out. */
  mask(preset: MaskPreset | MaskTransform): MaskableField<F>;
};

/**
 * Pre-computed UTF-8 bytes of an enum's variants, in ordinal order: the enum column's dictionary.
 */
export interface EnumUtf8Precomputed {
  /** UTF-8 bytes for each enum value, in order */
  readonly bytes: readonly Uint8Array[];
  /** Concatenated UTF-8 bytes for all enum values */
  readonly concatenated: Uint8Array;
  /** Arrow-format offsets array (Int32Array, length = values.length + 1) */
  readonly offsets: Int32Array;
}

export type NumberField = Field<number, 'f64', null>;
export type BooleanField = Field<boolean, 'boolean', null>;
export type U64Field = Field<bigint, 'u64', null>;
export type EnumField<T extends string = string> = Field<T, 'enum', EnumParams>;
export type CategoryField = Field<string, 'category', null>;
export type TextField = Field<string, 'text', null>;
export type BinaryField = Field<Uint8Array, 'binary', null>;

export type EagerNumberSchema = EagerField<NumberField>;
export type LazyNumberSchema = LazyField<NumberField>;
export type EagerBooleanSchema = EagerField<BooleanField>;
export type LazyBooleanSchema = LazyField<BooleanField>;
export type EagerU64Schema = EagerField<U64Field>;
export type LazyU64Schema = LazyField<U64Field>;
export type EagerEnumSchema<T extends string = string> = EagerField<EnumField<T>>;
export type LazyEnumSchema<T extends string = string> = LazyField<EnumField<T>>;
export type EagerCategorySchema = EagerField<CategoryField>;
export type LazyCategorySchema = MaskableField<CategoryField>;
export type EagerTextSchema = EagerField<TextField>;
export type LazyTextSchema = MaskableField<TextField>;
export type EagerBinarySchema = EagerField<BinaryField>;
export type LazyBinarySchema = LazyField<BinaryField>;

/**
 * Encodes a binary column's values to bytes where the column is written out (cold path). A field whose kind lowers
 * to a binary column and is not raw bytes has one: its kind's `encode` (`getBinaryEncoder`, typeGuards.ts).
 */
export interface BinaryEncoder {
  /** Encode a single value to bytes. */
  encode(value: unknown): Uint8Array;
}
