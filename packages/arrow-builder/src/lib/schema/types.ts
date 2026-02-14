/**
 * Schema metadata types for arrow-builder
 *
 * These types define the metadata attached to Sury schemas for columnar storage.
 * They enable arrow-builder to determine which TypedArray to create for each schema field.
 */

import type * as Sury from '@sury/sury';
import type { IntType } from '@uwdata/flechette';

/**
 * Schema type marker for columnar storage
 * Determines which TypedArray type to use:
 * - enum: Uint8Array (1 byte, compile-time mapping)
 * - category: Uint32Array (string interning)
 * - text: string[] (no dictionary)
 * - number: Float64Array
 * - boolean: Uint8Array (0/1)
 */
export type SchemaType = 'enum' | 'category' | 'text' | 'number' | 'boolean' | 'bigUint64' | 'binary';

/**
 * Eager brand marker - columns marked as eager are allocated immediately
 * and have no null bitmap (column is always written for every entry).
 */
export interface EagerBrand {
  readonly __eager: true;
}

/**
 * Lazy brand marker - columns marked as lazy are allocated on first write
 * and have a null bitmap for sparse data.
 */
export interface LazyBrand {
  readonly __eager?: never;
}

/**
 * Masking preset names
 * Applied during Arrow conversion (cold path), NOT during hot path writes
 */
export type MaskPreset = 'hash' | 'url' | 'sql' | 'email';

/**
 * Custom masking transform function
 */
export type MaskTransform = (value: string) => string;

/**
 * Pre-computed UTF-8 bytes for enum values
 * Built at schema definition time (cold path) for zero-cost Arrow conversion
 */
export interface EnumUtf8Precomputed {
  /** UTF-8 bytes for each enum value, in order */
  readonly bytes: readonly Uint8Array[];
  /** Concatenated UTF-8 bytes for all enum values */
  readonly concatenated: Uint8Array;
  /** Arrow-format offsets array (Int32Array, length = values.length + 1) */
  readonly offsets: Int32Array;
}

/**
 * Base schema with metadata
 * Using intersection type since we can't extend Sury.Schema directly
 */
export type SchemaWithMetadata<T = unknown> = Sury.Schema<T, unknown> & {
  __schema_type?: SchemaType;
  __enum_values?: readonly string[];
  __enum_utf8?: EnumUtf8Precomputed;
  __eager?: boolean;
};

// =============================================================================
// ENUM SCHEMA TYPES
// =============================================================================

/**
 * Base metadata for enum schema (shared between lazy and eager)
 */
interface EnumMetadataBase {
  __schema_type: 'enum';
  __enum_values: readonly string[];
  /** Pre-computed UTF-8 bytes for zero-cost Arrow conversion */
  __enum_utf8: EnumUtf8Precomputed;
  /** TypedArray constructor for index arrays (determined from enum size at definition time) */
  __index_array_ctor: Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor;
  /** Arrow index type for Dictionary index type */
  __arrow_index_type: IntType;
}

/**
 * Eager enum schema - allocated immediately, no null bitmap
 * Storage: Uint8Array (1 byte) with compile-time mapping
 * Arrow: Dictionary with pre-defined values (UTF-8 pre-computed at definition time)
 */
export type EagerEnumSchema<T extends string = string> = Sury.Schema<T, unknown> & EnumMetadataBase & EagerBrand;

/**
 * Lazy enum schema with chainable eager method
 * Storage: Uint8Array (1 byte) with compile-time mapping
 * Arrow: Dictionary with pre-defined values (UTF-8 pre-computed at definition time)
 */
export type LazyEnumSchema<T extends string = string> = Sury.Schema<T, unknown> &
  EnumMetadataBase &
  LazyBrand & {
    /**
     * Mark this column as eager (always written, no null bitmap).
     * Use for columns written on every entry.
     */
    eager(): EagerEnumSchema<T>;
  };

// =============================================================================
// CATEGORY SCHEMA TYPES
// =============================================================================

/**
 * Base metadata for category schema (shared between lazy and eager)
 */
interface CategoryMetadataBase {
  __schema_type: 'category';
  __mask_transform?: MaskTransform;
}

/**
 * Eager category schema - allocated immediately, no null bitmap
 * Storage: Uint32Array indices with string interning
 * Arrow: Dictionary built dynamically from interned strings
 */
export type EagerCategorySchema = Sury.Schema<string, unknown> & CategoryMetadataBase & EagerBrand;

/**
 * Lazy category schema with chainable mask and eager methods
 * Storage: Uint32Array indices with string interning
 * Arrow: Dictionary built dynamically from interned strings
 */
export type LazyCategorySchema = Sury.Schema<string, unknown> &
  CategoryMetadataBase &
  LazyBrand & {
    /**
     * Apply a masking transform during Arrow conversion (cold path).
     * Returns a new lazy schema with the mask transform attached.
     */
    mask(preset: MaskPreset | MaskTransform): LazyCategorySchema;
    /**
     * Mark this column as eager (always written, no null bitmap).
     * Use for columns written on every entry (like message).
     */
    eager(): EagerCategorySchema;
  };

// =============================================================================
// TEXT SCHEMA TYPES
// =============================================================================

/**
 * Base metadata for text schema (shared between lazy and eager)
 */
interface TextMetadataBase {
  __schema_type: 'text';
  __mask_transform?: MaskTransform;
}

/**
 * Eager text schema - allocated immediately, no null bitmap
 * Storage: Raw strings without interning
 * Arrow: Plain string column (no dictionary overhead)
 */
export type EagerTextSchema = Sury.Schema<string, unknown> & TextMetadataBase & EagerBrand;

/**
 * Lazy text schema with chainable mask and eager methods
 * Storage: Raw strings without interning
 * Arrow: Plain string column (no dictionary overhead)
 */
export type LazyTextSchema = Sury.Schema<string, unknown> &
  TextMetadataBase &
  LazyBrand & {
    /**
     * Apply a masking transform during Arrow conversion (cold path).
     * Returns a new lazy schema with the mask transform attached.
     */
    mask(preset: MaskPreset | MaskTransform): LazyTextSchema;
    /**
     * Mark this column as eager (always written, no null bitmap).
     * Use for columns written on every entry.
     */
    eager(): EagerTextSchema;
  };

// =============================================================================
// NUMBER SCHEMA TYPES
// =============================================================================

/**
 * Base metadata for number schema (shared between lazy and eager)
 */
interface NumberMetadataBase {
  __schema_type: 'number';
}

/**
 * Eager number schema - allocated immediately, no null bitmap
 * Storage: Float64Array
 */
export type EagerNumberSchema = Sury.Schema<number, unknown> & NumberMetadataBase & EagerBrand;

/**
 * Lazy number schema with chainable eager method
 * Storage: Float64Array
 */
export type LazyNumberSchema = Sury.Schema<number, unknown> &
  NumberMetadataBase &
  LazyBrand & {
    /**
     * Mark this column as eager (always written, no null bitmap).
     * Use for columns written on every entry.
     */
    eager(): EagerNumberSchema;
  };

// =============================================================================
// BOOLEAN SCHEMA TYPES
// =============================================================================

/**
 * Base metadata for boolean schema (shared between lazy and eager)
 */
interface BooleanMetadataBase {
  __schema_type: 'boolean';
}

/**
 * Eager boolean schema - allocated immediately, no null bitmap
 * Storage: Uint8Array (0/1)
 */
export type EagerBooleanSchema = Sury.Schema<boolean, unknown> & BooleanMetadataBase & EagerBrand;

/**
 * Lazy boolean schema with chainable eager method
 * Storage: Uint8Array (0/1)
 */
export type LazyBooleanSchema = Sury.Schema<boolean, unknown> &
  BooleanMetadataBase &
  LazyBrand & {
    /**
     * Mark this column as eager (always written, no null bitmap).
     * Use for columns written on every entry.
     */
    eager(): EagerBooleanSchema;
  };

// =============================================================================
// BIGUINT64 SCHEMA TYPES
// =============================================================================

/**
 * Base metadata for bigUint64 schema (shared between lazy and eager)
 */
interface BigUint64MetadataBase {
  __schema_type: 'bigUint64';
}

/**
 * Eager bigUint64 schema - allocated immediately, no null bitmap
 * Storage: BigUint64Array
 */
export type EagerBigUint64Schema = Sury.Schema<bigint, unknown> & BigUint64MetadataBase & EagerBrand;

/**
 * Lazy bigUint64 schema with chainable eager method
 * Storage: BigUint64Array
 */
export type LazyBigUint64Schema = Sury.Schema<bigint, unknown> &
  BigUint64MetadataBase &
  LazyBrand & {
    /**
     * Mark this column as eager (always written, no null bitmap).
     * Use for columns written on every entry.
     */
    eager(): EagerBigUint64Schema;
  };

// =============================================================================
// BINARY SCHEMA TYPES
// =============================================================================

/**
 * Encoder interface for binary columns.
 * Called at flush time (cold path) to encode arbitrary values to bytes.
 *
 * WHY per-value sync: Encoding happens during Arrow conversion (cold path),
 * not during hot-path writes. Sync keeps the interface simple and composable.
 */
export interface BinaryEncoder {
  /** Encode a single value to bytes. Called at flush time (cold path). */
  encode(value: unknown): Uint8Array;
}

/**
 * Base metadata for binary schema (shared between lazy and eager)
 */
interface BinaryMetadataBase {
  __schema_type: 'binary';
  __binary_encoder?: BinaryEncoder;
}

/**
 * Eager binary schema - allocated immediately, no null bitmap
 * Storage: Array (object references, frozen at tag time)
 * Arrow: Binary column (encoded at flush time via BinaryEncoder if present)
 */
export type EagerBinarySchema<T = Uint8Array> = Sury.Schema<T, unknown> & BinaryMetadataBase & EagerBrand;

/**
 * Lazy binary schema with chainable eager method
 * Storage: Array (object references, frozen at tag time)
 * Arrow: Binary column (encoded at flush time via BinaryEncoder if present)
 *
 * T defaults to Uint8Array (raw binary). When an encoder is provided,
 * T will be unknown (the consumer controls the type).
 */
export type LazyBinarySchema<T = Uint8Array> = Sury.Schema<T, unknown> &
  BinaryMetadataBase &
  LazyBrand & {
    /**
     * Mark this column as eager (always written, no null bitmap).
     * Use for columns written on every entry.
     */
    eager(): EagerBinarySchema<T>;
  };
