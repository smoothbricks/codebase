/**
 * TypedArray-based ColumnBuffer for zero-copy columnar storage
 *
 * ## Type System
 *
 * - `AnyColumnBuffer`: NO index signatures, accepts any schema (for generic processing)
 * - `ColumnBuffer<T>`: WITH index signatures, typed for specific schema T (T is REQUIRED)
 *
 *
 * ## Column Layout
 *
 * Each attribute column consists of TWO direct properties sharing ONE ArrayBuffer:
 * - X_nulls: Uint8Array for null bitmap (Arrow format: 1=valid, 0=null)
 * - X_values: TypedArray for actual values
 *
 * Both arrays are backed by the SAME ArrayBuffer, partitioned as:
 * [null bitmap bytes | padding to 64-byte cache line | value bytes]
 */

import type { ColumnSchema } from '../schema/ColumnSchema.js';
import type {
  EagerBigUint64Schema,
  EagerBinarySchema,
  EagerBooleanSchema,
  EagerCategorySchema,
  EagerNumberSchema,
  EagerTextSchema,
  LazyBigUint64Schema,
  LazyBinarySchema,
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyNumberSchema,
  LazyTextSchema,
} from '../schema/types.js';

// ============================================================================
// Core Types
// ============================================================================

/**
 * Generic TypedArray union type
 */
export type TypedArray =
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Int8Array
  | Int16Array
  | Int32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array;

/**
 * Column value type - TypedArray or string array for category/text columns
 * - Hot path stores raw JavaScript strings in string[] (zero conversion cost)
 * - Cold path (Arrow conversion) handles UTF-8 encoding and dictionary building
 */
export type ColumnValueType = TypedArray | string[] | unknown[];

/**
 * Default initial buffer capacity.
 *
 * Set to 8 (minimal aligned size).
 */
export const DEFAULT_BUFFER_CAPACITY = 8;

/**
 * Get TypedArray constructor for a schema type
 */
export type TypedArrayConstructor =
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor
  | Uint32ArrayConstructor
  | Int8ArrayConstructor
  | Int16ArrayConstructor
  | Int32ArrayConstructor
  | Float32ArrayConstructor
  | Float64ArrayConstructor;

// ============================================================================
// AnyColumnBuffer - Base type WITHOUT index signatures
// ============================================================================

/**
 * AnyColumnBuffer - Core buffer API for generic processing.
 *
 * This interface has NO index signatures, making it compatible with
 * any ColumnBuffer<T> regardless of schema. Use this type when you need
 * to accept any buffer (e.g., Arrow conversion, generic utilities).
 */
export interface AnyColumnBuffer {
  /** Buffer capacity */
  readonly _capacity: number;

  /** Chain to overflow buffer */
  _overflow?: AnyColumnBuffer;

  /** Runtime column access */
  getColumnIfAllocated(columnName: string): ColumnValueType | undefined;
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;
}

// ============================================================================
// Schema Type Helpers
// ============================================================================

/**
 * Infer the setter value type for a schema field.
 *
 * - enum: accepts number (pre-computed index from enum mapping)
 * - category: accepts string
 * - text: accepts string
 * - number: accepts number
 * - boolean: accepts boolean
 *
 * Matches the structure of ValuesArrayType - handles all known schema types
 * explicitly without using SchemaWithMetadata<infer T> to avoid type inference issues.
 */
export type SetterValueType<S> = S extends { __schema_type: 'enum' }
  ? number
  : S extends LazyCategorySchema | EagerCategorySchema
    ? string
    : S extends LazyTextSchema | EagerTextSchema
      ? string
      : S extends LazyNumberSchema | EagerNumberSchema
        ? number
        : S extends LazyBigUint64Schema | EagerBigUint64Schema
          ? bigint
          : S extends LazyBooleanSchema | EagerBooleanSchema
            ? boolean
            : S extends LazyBinarySchema<infer T> | EagerBinarySchema<infer T>
              ? T
              : unknown;

/**
 * Infer the values array type for a schema field.
 *
 * - enum: Uint8Array (0-255 indices)
 * - category: string[]
 * - text: string[]
 * - number: Float64Array
 * - boolean: Uint8Array (bit-packed)
 */
export type ValuesArrayType<S> = S extends { __schema_type: 'enum' }
  ? Uint8Array
  : S extends LazyCategorySchema | EagerCategorySchema
    ? string[]
    : S extends LazyTextSchema | EagerTextSchema
      ? string[]
      : S extends LazyNumberSchema | EagerNumberSchema
        ? Float64Array
        : S extends LazyBigUint64Schema | EagerBigUint64Schema
          ? BigUint64Array
          : S extends LazyBooleanSchema | EagerBooleanSchema
            ? Uint8Array
            : S extends LazyBinarySchema | EagerBinarySchema
              ? unknown[]
              : string[] | TypedArray;

/**
 * Filter out function properties from a schema object.
 * Only keeps actual schema field definitions.
 */
export type FilterSchemaFields<T> = {
  [K in keyof T as T[K] extends (...args: unknown[]) => unknown ? never : K]: T[K];
};

/**
 * Extract the fields from a schema, handling ColumnSchema wrappers.
 */
export type ExtractFields<S> = S extends ColumnSchema<infer F> ? F : S;

// ============================================================================
// ColumnBuffer<T> - Typed buffer WITH index signatures
// ============================================================================

/**
 * ColumnBuffer<T> - Fully typed buffer for a specific schema.
 *
 * Extends AnyColumnBuffer with:
 * - Schema-specific _values/_nulls properties
 * - Schema-specific setter methods
 * - Index signatures for dynamic access
 *
 * T is REQUIRED - there is no default. Use AnyColumnBuffer for generic processing.
 */
export type ColumnBuffer<Schema> = AnyColumnBuffer & {
  // Override _overflow with typed version
  _overflow?: ColumnBuffer<Schema>;

  // Index signatures for dynamic access
  [key: `${string}_values`]: TypedArray | string[] | unknown[] | undefined;
  [key: `${string}_nulls`]: Uint8Array | undefined;
} & {
  // Schema-specific column data
  readonly [K in keyof FilterSchemaFields<ExtractFields<Schema>> as `${K & string}_values`]: ValuesArrayType<
    FilterSchemaFields<ExtractFields<Schema>>[K]
  >;
} & {
  readonly [K in keyof FilterSchemaFields<ExtractFields<Schema>> as `${K & string}_nulls`]: Uint8Array;
} & {
  // Schema-specific setter methods (return this for method chaining)
  [K in keyof FilterSchemaFields<ExtractFields<Schema>>]: (
    pos: number,
    val: SetterValueType<FilterSchemaFields<ExtractFields<Schema>>[K]>,
  ) => ColumnBuffer<Schema>;
};

/**
 * Exposed view of a ColumnBuffer with typed access to internal arrays.
 */
export type ExposedColumnBuffer<Schema> = ColumnBuffer<Schema> & {
  readonly [K in keyof FilterSchemaFields<ExtractFields<Schema>> & string]: ValuesArrayType<
    FilterSchemaFields<ExtractFields<Schema>>[K]
  >;
};

/**
 * Expose internal column arrays for testing/inspection.
 */
export function expose<S>(buffer: ColumnBuffer<S>): ExposedColumnBuffer<S>;
export function expose<S>(buffer: ColumnBuffer<S>): ColumnBuffer<S> {
  return buffer;
}
