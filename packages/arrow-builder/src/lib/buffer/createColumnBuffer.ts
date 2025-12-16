/**
 * Create ColumnBuffer with native TypedArrays
 *
 * Per specs/01b_columnar_buffer_architecture.md and 01b1_buffer_performance_optimizations.md:
 * - Cache-aligned TypedArrays (64-byte boundaries)
 * - Direct properties for zero-indirection access (no lazy getters)
 * - ${name}_nulls and ${name}_values share ONE ArrayBuffer per column
 * - Runtime class generation for optimal V8 performance
 * - Arrow conversion in cold path (background processing)
 *
 * NOTE: This is a GENERIC columnar buffer implementation. Arrow-builder knows
 * nothing about application-specific concepts. Those are added by consumer packages.
 */

import type { TagAttributeSchema } from '../schema-types.js';
import { createGeneratedColumnBuffer } from './columnBufferGenerator.js';
import type { ColumnBuffer } from './types.js';
import { DEFAULT_BUFFER_CAPACITY } from './types.js';

/**
 * Create generic ColumnBuffer using runtime class generation
 *
 * This is the generic buffer creation function that arrow-builder exports.
 * It knows nothing about application-specific concepts.
 *
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - Uses runtime-generated class with direct properties
 * - Zero indirection: ${name}_nulls and ${name}_values are direct properties
 * - Shared ArrayBuffer: nulls and values use same buffer (cache-aligned)
 * - V8 optimizations: hidden class stability, monomorphic access, inline caching
 *
 * ## writeIndex Initialization
 *
 * The buffer is initialized with `writeIndex: 0`, which is correct for a generic
 * columnar buffer. Application-specific consumers may override this after creation
 * to implement fixed row layouts or other domain-specific patterns.
 *
 * ## Column Layout
 *
 * Each attribute column consists of TWO direct properties sharing ONE ArrayBuffer:
 * - X_nulls: Uint8Array for null bitmap (Arrow format: 1=valid, 0=null)
 * - X_values: TypedArray for actual values
 *
 * Both arrays are backed by the SAME ArrayBuffer, partitioned as:
 * [null bitmap bytes | padding to bytesPerElement boundary | value bytes]
 *
 * This ensures properly aligned access while maintaining memory locality.
 *
 * @param schema - Tag attribute schema
 * @param requestedCapacity - Requested buffer capacity
 * @returns Generic ColumnBuffer with timestamps, operations, and direct attribute columns
 */
export function createColumnBuffer(
  schema: TagAttributeSchema,
  requestedCapacity = DEFAULT_BUFFER_CAPACITY,
): ColumnBuffer {
  return createGeneratedColumnBuffer(schema, requestedCapacity);
}

// NOTE: Application-specific buffer types can extend ColumnBuffer in consumer packages
// to add domain-specific metadata and functionality.
