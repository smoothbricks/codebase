/**
 * Create ColumnBuffer with native TypedArrays
 *
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Cache-aligned TypedArrays (64-byte boundaries)
 * - Null bitmap management (one Uint8Array per nullable column per Arrow spec)
 * - Direct TypedArray writes in hot path
 * - Arrow conversion in cold path (background processing)
 *
 * NOTE: This module provides both generic ColumnBuffer creation (for arrow-builder)
 * and SpanBuffer creation (for lmao compatibility). The SpanBuffer functions
 * should eventually be moved to the lmao package.
 */

import type { TagAttributeSchema } from '../schema-types.js';
import type { ColumnBuffer, TypedArray } from './types.js';

/**
 * Get cache-aligned capacity
 *
 * Aligns to 64-byte cache line boundaries for optimal CPU performance.
 * Uses 1-byte element size (worst case) to ensure ALL array types are aligned.
 */
function getCacheAlignedCapacity(elementCount: number): number {
  const CACHE_LINE_SIZE = 64; // Cache line size in bytes
  const totalBytes = elementCount * 1; // Use 1 byte (worst case - Uint8Array)
  const alignedBytes = Math.ceil(totalBytes / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
  return alignedBytes; // Return byte count which becomes element count for Uint8Array
}

/**
 * Helper to get TypedArray constructor for a schema field
 */
function getArrayConstructorForField(
  schema: TagAttributeSchema,
  fieldName: string,
  alignedCapacity: number,
): TypedArray {
  const fieldSchema = schema[fieldName];
  const schemaWithMetadata = fieldSchema as import('../schema-types.js').SchemaWithMetadata;
  const lmaoType = schemaWithMetadata?.__lmao_type;

  // Handle three string types
  if (lmaoType === 'enum') {
    const enumValues = schemaWithMetadata.__lmao_enum_values;
    const enumCount = enumValues?.length ?? 0;

    // Uint8Array can hold 0-255 indices (256 values total)
    if (enumCount === 0 || enumCount <= 256) {
      return new Uint8Array(alignedCapacity);
    }
    if (enumCount <= 65536) {
      return new Uint16Array(alignedCapacity);
    }
    return new Uint32Array(alignedCapacity);
  }

  if (lmaoType === 'category') {
    return new Uint32Array(alignedCapacity);
  }

  if (lmaoType === 'text') {
    return new Uint32Array(alignedCapacity);
  }

  if (lmaoType === 'number') {
    return new Float64Array(alignedCapacity);
  }

  if (lmaoType === 'boolean') {
    return new Uint8Array(alignedCapacity);
  }

  // Default to Uint32Array
  return new Uint32Array(alignedCapacity);
}

/**
 * Create generic ColumnBuffer with lazy column initialization
 *
 * This is the generic buffer creation function that arrow-builder exports.
 * It knows nothing about spans, traces, or any application-specific concepts.
 *
 * Per GitHub review feedback: Uses lazy getters to only allocate memory for
 * columns that are actually written to. This saves memory for sparse columns.
 *
 * ## writeIndex Initialization
 *
 * The buffer is initialized with `writeIndex: 0`, which is correct for a generic
 * columnar buffer. Application-specific consumers (like LMAO's SpanBuffer) may
 * override this after creation to implement fixed row layouts.
 *
 * For example, LMAO uses a fixed row layout where:
 * - Row 0: span-start (reserved)
 * - Row 1: span-end (reserved, pre-initialized)
 * - Row 2+: events
 *
 * The LMAO package's `writeSpanStart()` function sets `writeIndex = 2` after
 * initializing rows 0 and 1, ensuring events are written starting at row 2.
 *
 * @param schema - Tag attribute schema
 * @param requestedCapacity - Requested buffer capacity
 * @returns Generic ColumnBuffer with timestamps, operations, and lazy attribute columns
 */
export function createColumnBuffer(schema: TagAttributeSchema, requestedCapacity = 64): ColumnBuffer {
  // Cache-align capacity for all arrays
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity);
  const bitmapByteLength = Math.ceil(alignedCapacity / 8);

  // Create core columns (always allocated)
  const timestamps = new Float64Array(alignedCapacity);
  const operations = new Uint8Array(alignedCapacity);

  // Storage for lazily-initialized columns
  const lazyColumnStorage: Record<`attr_${string}`, { nulls?: Uint8Array; data?: TypedArray }> = {};

  // Legacy nullBitmaps object for backward compatibility
  // This will be populated lazily via getters
  const nullBitmaps: Record<`attr_${string}`, Uint8Array> = {};

  // Create base buffer object
  const buffer: ColumnBuffer = {
    timestamps,
    operations,
    nullBitmaps,
    writeIndex: 0,
    capacity: requestedCapacity,
    next: undefined,
  };

  // Define lazy getters for each attribute column
  for (const fieldName of Object.keys(schema)) {
    const columnName = `attr_${fieldName}` as `attr_${string}`;

    // Initialize lazy storage for this column
    lazyColumnStorage[columnName] = {};

    // Define getter for the data column (attr_fieldName)
    Object.defineProperty(buffer, columnName, {
      get() {
        // Lazy initialization: only allocate on first access
        const storage = lazyColumnStorage[columnName];
        if (!storage.data) {
          storage.data = getArrayConstructorForField(schema, fieldName, alignedCapacity);
        }
        return storage.data;
      },
      enumerable: true,
      configurable: true,
    });

    // Define getter for null bitmap
    Object.defineProperty(nullBitmaps, columnName, {
      get() {
        // Lazy initialization: only allocate on first access
        const storage = lazyColumnStorage[columnName];
        if (!storage.nulls) {
          storage.nulls = new Uint8Array(bitmapByteLength);
        }
        return storage.nulls;
      },
      enumerable: true,
      configurable: true,
    });
  }

  return buffer;
}

// SpanBuffer creation functions have been moved to @smoothbricks/lmao package
// Import them from there: import { createSpanBuffer, createChildSpanBuffer, createNextBuffer } from '@smoothbricks/lmao';
