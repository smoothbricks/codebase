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
 * ## Column Layout
 *
 * Each attribute column consists of TWO direct properties sharing ONE ArrayBuffer:
 * - attr_X_nulls: Uint8Array for null bitmap (Arrow format: 1=valid, 0=null)
 * - attr_X_values: TypedArray for actual values
 *
 * Both arrays are backed by the SAME ArrayBuffer, partitioned as:
 * [null bitmap bytes | padding to 64-byte cache line | value bytes]
 *
 * This ensures cache-aligned access while maintaining memory locality.
 */
export interface ColumnBuffer {
  // Core columns - always present
  timestamps: Float64Array; // Microsecond-precision timestamps (values are Microseconds branded type)
  operations: Uint8Array; // Operation type: tag, ok, err, etc.

  // Attribute columns (generated from schema with attr_ prefix)
  // Each attribute has TWO properties:
  // - attr_X_nulls: Uint8Array for null bitmap
  // - attr_X_values: TypedArray OR string[] for actual values
  //
  // For category/text columns, values are stored as string[] on the hot path
  // For enum/number/boolean columns, values are stored in TypedArray
  [key: `attr_${string}_nulls`]: Uint8Array;
  [key: `attr_${string}_values`]: ColumnValueType;

  // Buffer management
  writeIndex: number; // Current write position (0 to capacity-1)
  capacity: number; // Logical capacity for bounds checking
  next?: ColumnBuffer; // Chain to next buffer when overflow
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
  | Float64Array;

/**
 * Column value type - TypedArray or string array for category/text columns
 *
 * Per new string storage design:
 * - Hot path stores raw JavaScript strings in string[] (zero conversion cost)
 * - Cold path (Arrow conversion) handles UTF-8 encoding and dictionary building
 */
export type ColumnValueType = TypedArray | string[];

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
