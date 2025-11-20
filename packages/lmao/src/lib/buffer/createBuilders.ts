/**
 * Create Arrow builders for attribute columns based on schema
 * 
 * This maps Sury schema types to appropriate Arrow builders:
 * - string/masked → Utf8Builder
 * - number → Float64Builder
 * - boolean → BoolBuilder
 * - enum/union → Utf8Builder (dictionary encoding can be added later)
 */

import * as arrow from 'apache-arrow';
import * as Sury from '@sury/sury';
import type { TagAttributeSchema } from '../schema/types.js';

/**
 * Create Arrow builders for attribute columns based on schema
 * 
 * This maps Sury schema types to appropriate Arrow builders.
 * Each attribute gets an `attr_` prefix to distinguish it from core columns.
 */
export function createAttributeBuilders(
  schema: TagAttributeSchema,
  capacity: number = 64
): Record<string, arrow.Builder> {
  const builders: Record<string, arrow.Builder> = {};
  
  for (const [fieldName, surySchema] of Object.entries(schema)) {
    const columnName = `attr_${fieldName}`;
    builders[columnName] = createBuilderForSchema(surySchema, capacity);
  }
  
  return builders;
}

/**
 * Create appropriate Arrow builder for a Sury schema
 * 
 * Note: Sury schemas don't expose type info directly at runtime.
 * This is a simplified implementation that defaults to Utf8Builder.
 * 
 * In production, you would either:
 * 1. Add type hints to your schema definitions
 * 2. Use schema introspection if Sury provides it
 * 3. Maintain a type registry alongside schemas
 */
function createBuilderForSchema(
  schema: Sury.Schema<unknown, unknown>,
  capacity: number
): arrow.Builder {
  // For now, use Utf8Builder as default
  // This works for all types with string serialization
  // TODO: Add proper type detection when Sury provides schema introspection
  
  return new arrow.Utf8Builder({
    type: new arrow.Utf8(),
    nullValues: [null, undefined]
  });
}

/**
 * Determine Arrow data type from Sury schema
 * 
 * This examines the Sury schema to determine the appropriate Arrow type.
 * Currently simplified - returns Utf8 for all types.
 * 
 * TODO: Implement proper type detection:
 * - Check if Sury exposes schema metadata
 * - Use type hints from schema definitions
 * - Map to appropriate Arrow types (Float64, Bool, Int32, etc.)
 */
export function getArrowTypeFromSchema(
  schema: Sury.Schema<unknown, unknown>
): arrow.DataType {
  // Simplified implementation - would need schema introspection
  // For now, default to Utf8 for strings
  return new arrow.Utf8();
}
