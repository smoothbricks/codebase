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
 * - System properties use `_` prefix: `_capacity`, `_next`
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
  // Generic columnar buffer - no assumptions about table columns
  _capacity: number; // Buffer capacity
  _next?: ColumnBuffer; // Chain to overflow buffer

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
}

/**
 * Narrow buffer interface without dynamic schema access.
 * Prevents compile-time access to schema fields while maintaining system properties.
 */
export interface NarrowColumnBuffer {
  // No system columns - arrow-builder is generic
  _capacity: number;
  _next?: NarrowColumnBuffer;
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

import type { ColumnSchema } from '../schema/ColumnSchema.js';
import type {
  EagerBigUint64Schema,
  EagerBooleanSchema,
  EagerCategorySchema,
  EagerNumberSchema,
  EagerTextSchema,
  LazyBigUint64Schema,
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyNumberSchema,
  LazyTextSchema,
} from '../schema/types.js';

/**
 * Infer the setter value type for a schema field.
 *
 * - enum: accepts number (pre-computed index from enum mapping)
 * - category: accepts string
 * - text: accepts string
 * - number: accepts number
 * - boolean: accepts boolean
 *
 * Note: For enums, the buffer expects numeric indices. String→index conversion
 * happens in higher-level APIs (SpanLogger, TagWriter, ResultWriter).
 *
 * Matches the structure of ValuesArrayType - handles all known schema types
 * explicitly without using SchemaWithMetadata<infer T> to avoid type inference issues.
 */
export type SetterValueType<S> = S extends { __schema_type: 'enum' }
  ? number // enum index (string→index conversion done by SpanLogger/TagWriter)
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
            ? Uint8Array // bit-packed
            : string[] | TypedArray;

/**
 * Filter out function properties from a schema object.
 * Only keeps actual schema field definitions.
 */
export type FilterSchemaFields<T> = {
  [K in keyof T as T[K] extends (...args: unknown[]) => unknown ? never : K]: T[K];
};

/**
 * Type aliases for schema-based property access keys.
 * Enables DRY composition of schema properties across types.
 */
export type ValueKeys<T> = T extends { fields: infer F } ? `${keyof FilterSchemaFields<F>}_values` : never;
export type NullKeys<T> = T extends { fields: infer F } ? `${keyof FilterSchemaFields<F>}_nulls` : never;

/**
 * Typed property access for schema-defined value arrays.
 * Used for composing schema-based access in buffer types.
 */
export type SchemaValueProperties<T> = T extends { fields: infer F }
  ? { [K in ValueKeys<T>]: ValuesArrayType<FilterSchemaFields<F>[Extract<keyof FilterSchemaFields<F>, string>]> }
  : never;

/**
 * Extract the fields from a schema, handling ColumnSchema wrappers.
 */
export type ExtractFields<S> = S extends ColumnSchema<infer F> ? F : S;

/**
 * Buffer data properties (arrays and null bitmaps) without setter methods.
 * Separated from setters to allow different return types for fluent APIs.
 */
export type BufferData<Schema> = ColumnBuffer & {
  // Column value arrays (internal, use expose() to access with type safety)
  [K in keyof FilterSchemaFields<ExtractFields<Schema>> as `${K & string}_values`]: ValuesArrayType<
    FilterSchemaFields<ExtractFields<Schema>>[K]
  >;
} & {
  // Column null bitmaps (internal, use expose() to access with type safety)
  [K in keyof FilterSchemaFields<ExtractFields<Schema>> as `${K & string}_nulls`]: Uint8Array;
};

/**
 * Setter methods layer, parameterized by return type.
 * Allows different fluent API return types.
 */
// ReturnType defaults to unknown for type safety while allowing flexible fluent API return types
export type BufferSetters<Schema, ReturnType = unknown> = {
  [K in keyof FilterSchemaFields<ExtractFields<Schema>>]: (
    pos: number,
    val: SetterValueType<FilterSchemaFields<ExtractFields<Schema>>[K]>,
  ) => ReturnType;
};

/**
 * Runtime inspection methods layer.
 */
export type BufferMethods = {
  getColumnIfAllocated(columnName: string): ColumnValueType | undefined;
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;
};

/**
 * Narrow buffer methods for runtime-only access.
 */
export interface NarrowBufferMethods {
  getColumnIfAllocated(columnName: string): ColumnValueType | undefined;
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;
}

/**
 * Narrow buffer with methods.
 */
export type NarrowBuffer = NarrowColumnBuffer & NarrowBufferMethods;

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
 * ## Why Composable?
 *
 * Breaking into layers allows consumers (like SpanBuffer) to override return types
 * for fluent APIs while reusing the core data and method structures:
 * - BufferData: Arrays and null bitmaps
 * - BufferSetters: Typed setter methods (defaults to any for flexibility)
 * - BufferMethods: Runtime inspection methods
 *
 * The tradeoff: `this` return type is only available in interfaces/classes,
 * so setter methods return the composed type instead of `this`.
 * This slightly limits polymorphism but has no runtime impact.
 */
export type TypedColumnBuffer<Schema> = BufferData<Schema> &
  BufferMethods & {
    // Index signatures for backward compatibility and dynamic access
    [key: `${string}_values`]: TypedArray | string[] | undefined;
    [key: `${string}_nulls`]: Uint8Array | undefined;
    // Index signature for setter methods (for compatibility with generic code)
    [key: string]:
      | ((pos: number, val: unknown) => TypedColumnBuffer<Schema>)
      | TypedArray
      | string[]
      | Uint8Array
      | number
      | BufferMethods[keyof BufferMethods]
      | undefined;
  } & {
    // Setter methods that return this for fluent API
    [K in keyof FilterSchemaFields<ExtractFields<Schema>>]: (
      pos: number,
      val: SetterValueType<FilterSchemaFields<ExtractFields<Schema>>[K]>,
    ) => TypedColumnBuffer<Schema>;
  };

/**
 * Exposed view of a TypedColumnBuffer with typed access to internal arrays.
 * Use `expose()` to cast a buffer to this type for testing/inspection.
 *
 * Provides direct access to:
 * - `${field}`: Values array (alias for `${field}_values`)
 * - `${field}_values`: Values array
 * - `${field}_nulls`: Null bitmap
 */
export type ExposedColumnBuffer<Schema> = TypedColumnBuffer<Schema> & {
  // Alias getters for direct array access (same as _values)
  // These shadow the setter methods for read-only access to underlying arrays
  readonly [K in keyof FilterSchemaFields<ExtractFields<Schema>> & string]: ValuesArrayType<
    FilterSchemaFields<ExtractFields<Schema>>[K]
  >;
};

/**
 * Expose internal column arrays for testing/inspection.
 * Returns the same buffer with a type that allows direct array access.
 *
 * @example
 * ```ts
 * const buffer = createGeneratedColumnBuffer(schema, 10);
 * buffer.status(0, 0);  // Write using setter
 *
 * const exposed = expose(buffer);
 * exposed.status[0];     // Read values array directly
 * exposed.status_nulls;  // Access null bitmap
 * ```
 */
export function expose<S>(buffer: TypedColumnBuffer<S>): ExposedColumnBuffer<S> {
  return buffer as ExposedColumnBuffer<S>;
}
