/**
 * TypedArray-based ColumnBuffer for zero-copy columnar storage
 *
 * Uses native TypeScript TypedArrays for efficient memory management
 * per specs/01b_columnar_buffer_architecture.md.
 *
 * Arrow table conversion happens in cold path (background processing).
 *
 * NOTE: This is a GENERIC columnar buffer. Consumer packages
 * extend this type to add domain-specific metadata.
 *
 * ## Naming Conventions
 *
 * - System properties use `_` prefix: `_capacity`, `_next`, `_timestamps`, `_operations`
 * - User columns have NO prefix, just suffixes: `userId_nulls`, `userId_values`, `userId` (alias)
 *
 * This prevents collisions between user-defined field names and system internals.
 *
 * ## Column Layout
 *
 * Each attribute column consists of TWO direct properties sharing ONE ArrayBuffer:
 * - X_nulls: Uint8Array for null bitmap (Arrow format: 1=valid, 0=null)
 * - X_values: TypedArray for actual values
 *
 * Both arrays are backed by the SAME ArrayBuffer, partitioned as:
 * [null bitmap bytes | padding to 64-byte cache line | value bytes]
 *
 * This ensures cache-aligned access while maintaining memory locality.
 *
 * ## Write Position Tracking
 *
 * Write position is tracked by ColumnWriter, NOT by ColumnBuffer.
 * This allows multiple writers to share a buffer or write to different regions.
 */
export interface ColumnBuffer {
  // System columns - prefixed with _ to avoid collision with user columns
  _timestamps: BigInt64Array; // Nanosecond-precision timestamps since Unix epoch
  _operations: Uint8Array; // Operation type: tag, ok, err, etc.

  // User attribute columns are added dynamically at runtime via code generation
  // Each attribute has TWO properties:
  // - X_nulls: Uint8Array for null bitmap
  // - X_values: TypedArray OR string[] for actual values
  //
  // For category/text columns, values are stored as string[] on the hot path
  // For enum/number/boolean columns, values are stored in TypedArray

  // Index signatures for dynamic column properties created at runtime
  [key: `${string}_nulls`]: Uint8Array;
  [key: `${string}_values`]:
    | Int8Array
    | Int16Array
    | Int32Array
    | BigInt64Array
    | Uint8Array
    | Uint16Array
    | Uint32Array
    | BigUint64Array
    | Float32Array
    | Float64Array
    | string[];

  // Buffer management - prefixed with _ to avoid collision with user columns
  _capacity: number; // Logical capacity for bounds checking
  _next?: ColumnBuffer; // Chain to next buffer when overflow
}

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
 *
 * Per new string storage design:
 * - Hot path stores raw JavaScript strings in string[] (zero conversion cost)
 * - Cold path (Arrow conversion) handles UTF-8 encoding and dictionary building
 */
export type ColumnValueType = TypedArray | string[];

/**
 * Default initial buffer capacity.
 *
 * Set to 8 (minimal aligned size) because:
 * 1. Most spans are small: span-start (row 0), span-ok/err (row 1), and 1-3 log events
 *    fit comfortably in 8 rows, covering >80% of spans without overflow.
 * 2. Memory efficiency: Starting at 8 instead of 64 reduces initial allocation by 8×.
 *    For applications with thousands of concurrent spans, this significantly reduces
 *    memory pressure.
 * 3. Multiple of 8: Required for null bitmap byte-alignment (see specs/01b).
 * 4. Self-tuning: Modules that need more capacity will trigger buffer chaining on
 *    first overflow, and the self-tuning system learns to allocate larger initial
 *    capacity for that module.
 *
 * @see specs/01b_columnar_buffer_architecture.md "Initial Capacity: 8 Elements"
 */
export const DEFAULT_BUFFER_CAPACITY = 8;

/**
 * Capacity stats for buffer size tuning
 * Generic stats that any use case can build upon
 */
export interface BufferCapacityStats {
  currentCapacity: number;
  totalWrites: number;
  overflowWrites: number;
  totalBuffersCreated: number;
}

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
// Type-safe buffer types for generated column buffers
// ============================================================================

import type {
  BooleanSchemaWithMetadata,
  CategorySchemaWithMetadata,
  EnumSchemaWithMetadata,
  NumberSchemaWithMetadata,
  SchemaWithMetadata,
  TextSchemaWithMetadata,
} from '../schema/types.js';

/**
 * Infer the setter value type for a schema field.
 *
 * - enum: accepts string (enum value)
 * - category: accepts string
 * - text: accepts string
 * - number: accepts number
 * - boolean: accepts boolean
 */
export type SetterValueType<S> = S extends EnumSchemaWithMetadata<infer T>
  ? T // enum values as string literals
  : S extends CategorySchemaWithMetadata
    ? string
    : S extends TextSchemaWithMetadata
      ? string
      : S extends NumberSchemaWithMetadata
        ? number
        : S extends BooleanSchemaWithMetadata
          ? boolean
          : S extends SchemaWithMetadata<infer T>
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
export type ValuesArrayType<S> = S extends EnumSchemaWithMetadata
  ? Uint8Array
  : S extends CategorySchemaWithMetadata
    ? string[]
    : S extends TextSchemaWithMetadata
      ? string[]
      : S extends NumberSchemaWithMetadata
        ? Float64Array
        : S extends BooleanSchemaWithMetadata
          ? Uint8Array // bit-packed
          : string[] | TypedArray;

/**
 * Filter out function properties from a schema object.
 * Only keeps actual schema field definitions.
 */
type FilterSchemaFields<T> = {
  [K in keyof T as T[K] extends (...args: unknown[]) => unknown ? never : K]: T[K];
};

/**
 * Generate the typed column buffer type for a given schema.
 *
 * For each field in the schema, this creates:
 * - `${field}_nulls`: Uint8Array (null bitmap)
 * - `${field}_values`: TypedArray or string[] based on schema type
 * - `${field}`: alias for `${field}_values` (for lazy columns only)
 * - `${field}(pos, val)`: setter method for fluent writes
 *
 * ## Why This Is a Type Alias (Not an Interface)
 *
 * TypeScript interfaces require "statically known members" - they cannot extend
 * mapped types with computed property names like `${K & string}_values`.
 *
 * Since we need schema-derived properties (e.g., `userId_values`, `status_nulls`),
 * we must use a type alias with mapped types.
 *
 * The tradeoff: `this` return type is only available in interfaces/classes,
 * so setter methods return `TypedColumnBuffer<Schema>` instead of `this`.
 * This slightly limits polymorphism but has no runtime impact.
 */
export type TypedColumnBuffer<Schema> = ColumnBuffer & {
  // Column value arrays (accessed via getters)
  [K in keyof FilterSchemaFields<Schema> as `${K & string}_values`]: ValuesArrayType<FilterSchemaFields<Schema>[K]>;
} & {
  // Column null bitmaps (only for non-eager columns, but we include all for simplicity)
  [K in keyof FilterSchemaFields<Schema> as `${K & string}_nulls`]: Uint8Array;
} & {
  // Alias getters (same as _values)
  [K in keyof FilterSchemaFields<Schema> & string]: ValuesArrayType<FilterSchemaFields<Schema>[K]>;
} & {
  // Setter methods: columnName(position, value) => TypedColumnBuffer<Schema>
  // Note: Can't use `this` because this is a type alias, not an interface
  [K in keyof FilterSchemaFields<Schema> & string]: (
    pos: number,
    val: SetterValueType<FilterSchemaFields<Schema>[K]>,
  ) => TypedColumnBuffer<Schema>;
} & {
  // Runtime inspection methods
  getColumnIfAllocated(columnName: string): ColumnValueType | undefined;
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;
};
