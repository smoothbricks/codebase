/**
 * Schema metadata types for arrow-builder
 *
 * These types define the metadata attached to Sury schemas for columnar storage.
 * They enable arrow-builder to determine which TypedArray to create for each schema field.
 */

import type * as Sury from '@sury/sury';

/**
 * Schema type marker for columnar storage
 * Determines which TypedArray type to use:
 * - enum: Uint8Array (1 byte, compile-time mapping)
 * - category: Uint32Array (string interning)
 * - text: string[] (no dictionary)
 * - number: Float64Array
 * - boolean: Uint8Array (0/1)
 */
export type SchemaType = 'enum' | 'category' | 'text' | 'number' | 'boolean';

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
};

/**
 * Enum schema with enum values metadata
 * Storage: Uint8Array (1 byte) with compile-time mapping
 * Arrow: Dictionary with pre-defined values (UTF-8 pre-computed at definition time)
 */
export type EnumSchemaWithMetadata<T extends string = string> = Sury.Schema<T, unknown> & {
  __schema_type: 'enum';
  __enum_values: readonly string[];
  /** Pre-computed UTF-8 bytes for zero-cost Arrow conversion */
  __enum_utf8: EnumUtf8Precomputed;
};

/**
 * Base metadata for category schema
 */
interface CategoryMetadata {
  __schema_type: 'category';
  __mask_transform?: MaskTransform;
}

/**
 * Category schema with metadata
 * Storage: Uint32Array indices with string interning
 * Arrow: Dictionary built dynamically from interned strings
 */
export type CategorySchemaWithMetadata = Sury.Schema<string, unknown> & CategoryMetadata;

/**
 * Category schema with chainable mask method
 */
export type CategorySchemaWithMask = CategorySchemaWithMetadata & {
  mask(preset: MaskPreset | MaskTransform): CategorySchemaWithMetadata;
};

/**
 * Base metadata for text schema
 */
interface TextMetadata {
  __schema_type: 'text';
  __mask_transform?: MaskTransform;
}

/**
 * Text schema with metadata
 * Storage: Raw strings without interning
 * Arrow: Plain string column (no dictionary overhead)
 */
export type TextSchemaWithMetadata = Sury.Schema<string, unknown> & TextMetadata;

/**
 * Text schema with chainable mask method
 */
export type TextSchemaWithMask = TextSchemaWithMetadata & {
  mask(preset: MaskPreset | MaskTransform): TextSchemaWithMetadata;
};

/**
 * Number schema with metadata
 * Storage: Float64Array
 */
export type NumberSchemaWithMetadata = Sury.Schema<number, unknown> & {
  __schema_type: 'number';
};

/**
 * Boolean schema with metadata
 * Storage: Uint8Array (0/1)
 */
export type BooleanSchemaWithMetadata = Sury.Schema<boolean, unknown> & {
  __schema_type: 'boolean';
};
