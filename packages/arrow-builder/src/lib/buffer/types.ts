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
  [key: `${string}_values`]: Uint8Array | Uint16Array | Uint32Array | Float64Array | string[];

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
