/**
 * Create TypedArray columns for attribute columns based on schema
 *
 * This maps Sury schema types to appropriate storage per
 * specs/lmao/01b_columnar_buffer_architecture.md:
 * - enum → Uint8/Uint16/Uint32Array (size based on enum value count)
 * - category → string[] (raw strings, dictionary built at Arrow conversion)
 * - text → string[] (raw strings, no dictionary)
 * - number → Float64Array
 * - boolean → Uint8Array
 */

import type { ColumnSchema } from '../schema/ColumnSchema.js';
import type { Schema } from '../schema/core.js';
import { enumIndexBits } from '../schema/field.js';
import { getEnumValues, storageLaneOf } from '../schema/typeGuards.js';
import { type ColumnValueType, DEFAULT_BUFFER_CAPACITY, type TypedArrayConstructor } from './types.js';

/**
 * Create TypedArray columns for attribute columns based on schema
 *
 * User columns have no prefix - the column name IS the field name.
 * Returns a record of TypedArrays with cache-aligned capacity.
 */
export function createAttributeColumns(
  schema: ColumnSchema,
  capacity = DEFAULT_BUFFER_CAPACITY,
): Record<string, ColumnValueType> {
  const columns: Record<string, ColumnValueType> = {};

  // Use ColumnSchema.fieldEntries() directly
  for (const [fieldName, surySchema] of schema._columns) {
    // User columns have no prefix
    columns[fieldName] = createTypedArrayForSchema(surySchema, capacity);
  }

  return columns;
}

/**
 * Create appropriate TypedArray for a Sury schema
 *
 * STRING TYPE SYSTEM (See specs/lmao/01a_trace_schema_system.md):
 * - enum: Uint8/16/32Array (size based on enum value count: <256=8bit, <65536=16bit, else 32bit)
 * - category: string[] (raw strings, dictionary built at Arrow conversion)
 * - text: string[] (raw strings, no dictionary)
 *
 * OTHER TYPES:
 * - number: Float64Array (full precision)
 * - boolean: Uint8Array (0 or 1)
 *
 * We attach metadata to schemas in builder.ts to identify the type.
 */
function createTypedArrayForSchema(schema: Schema, capacity: number): ColumnValueType {
  switch (storageLaneOf(schema)) {
    case 'enum': {
      // Enum: the index width the declaration fixes
      const bits = enumIndexBits(getEnumValues(schema)?.length ?? 0);
      return bits === 8
        ? new Uint8Array(capacity)
        : bits === 16
          ? new Uint16Array(capacity)
          : new Uint32Array(capacity);
    }
    case 'category':
    case 'text':
      // Raw string array (a dictionary, if any, is built at Arrow conversion)
      return new Array<string>(capacity);
    case 'number':
      return new Float64Array(capacity);
    case 'boolean':
      return new Uint8Array(capacity);
    default:
      // Default to Uint32Array for unknown types
      return new Uint32Array(capacity);
  }
}

/**
 * Get TypedArray constructor for a schema type
 *
 * This examines the schema to determine the appropriate TypedArray constructor.
 */
export function getTypedArrayConstructor(schema: Schema): TypedArrayConstructor {
  const schemaType = storageLaneOf(schema);

  if (schemaType === 'enum') return Uint8Array;
  if (schemaType === 'category') return Uint32Array;
  if (schemaType === 'text') return Uint32Array;
  if (schemaType === 'number') return Float64Array;
  if (schemaType === 'boolean') return Uint8Array;

  // Default for unknown types
  return Uint32Array;
}
