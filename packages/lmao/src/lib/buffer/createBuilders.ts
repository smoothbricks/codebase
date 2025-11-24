/**
 * Create TypedArray columns for attribute columns based on schema
 * 
 * This maps Sury schema types to appropriate TypedArrays per
 * specs/01b_columnar_buffer_architecture.md:
 * - enum → Uint8Array (compile-time mapping)
 * - category → Uint32Array (string interning indices)
 * - text → Uint32Array (raw string indices)
 * - number → Float64Array
 * - boolean → Uint8Array
 */

import * as Sury from '@sury/sury';
import type { TagAttributeSchema, SchemaWithMetadata } from '../schema/types.js';
import type { TypedArray, TypedArrayConstructor } from './types.js';

/**
 * Create TypedArray columns for attribute columns based on schema
 * 
 * Each attribute gets an `attr_` prefix to distinguish it from core columns.
 * Returns a record of TypedArrays with cache-aligned capacity.
 */
export function createAttributeColumns(
  schema: TagAttributeSchema,
  capacity: number = 64
): Record<string, TypedArray> {
  const columns: Record<string, TypedArray> = {};
  
  for (const [fieldName, surySchema] of Object.entries(schema)) {
    const columnName = `attr_${fieldName}`;
    columns[columnName] = createTypedArrayForSchema(surySchema, capacity);
  }
  
  return columns;
}

/**
 * Create appropriate TypedArray for a Sury schema
 * 
 * STRING TYPE SYSTEM (See specs/01a_trace_schema_system.md):
 * - enum: Uint8Array (1 byte with compile-time mapping)
 * - category: Uint32Array (runtime string interning indices)
 * - text: Uint32Array (raw string indices)
 * 
 * OTHER TYPES:
 * - number: Float64Array (full precision)
 * - boolean: Uint8Array (0 or 1)
 * 
 * We attach metadata to schemas in builder.ts to identify the type.
 */
function createTypedArrayForSchema(
  schema: Sury.Schema<unknown, unknown>,
  capacity: number
): TypedArray {
  const schemaWithMetadata = schema as SchemaWithMetadata;
  const lmaoType = schemaWithMetadata.__lmao_type;
  
  // Handle three string types
  if (lmaoType === 'enum') {
    // Enum: Uint8Array for compile-time mapped values (0-255)
    return new Uint8Array(capacity);
  }
  
  if (lmaoType === 'category') {
    // Category: Uint32Array for string interning indices
    return new Uint32Array(capacity);
  }
  
  if (lmaoType === 'text') {
    // Text: Uint32Array for raw string indices (no interning)
    return new Uint32Array(capacity);
  }
  
  // Handle number and boolean types
  if (lmaoType === 'number') {
    // Number: Float64Array for full precision
    return new Float64Array(capacity);
  }
  
  if (lmaoType === 'boolean') {
    // Boolean: Uint8Array (0 or 1)
    return new Uint8Array(capacity);
  }
  
  // Default to Uint32Array for unknown types
  return new Uint32Array(capacity);
}

/**
 * Get TypedArray constructor for a schema type
 * 
 * This examines the Sury schema to determine the appropriate TypedArray constructor.
 */
export function getTypedArrayConstructor(
  schema: Sury.Schema<unknown, unknown>
): TypedArrayConstructor {
  const schemaWithMetadata = schema as SchemaWithMetadata;
  const lmaoType = schemaWithMetadata.__lmao_type;
  
  if (lmaoType === 'enum') return Uint8Array;
  if (lmaoType === 'category') return Uint32Array;
  if (lmaoType === 'text') return Uint32Array;
  
  // Default for unknown types
  return Uint32Array;
}
