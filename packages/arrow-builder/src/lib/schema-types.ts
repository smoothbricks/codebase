/**
 * Schema type definitions for arrow-builder
 *
 * Arrow-builder is a low-level columnar buffer engine that works with
 * Sury schemas that have __schema_type metadata attached.
 */

import type * as Sury from '@sury/sury';

// Re-export schema metadata types from the schema module
export type {
  BooleanSchemaWithMetadata,
  CategorySchemaWithMetadata,
  EnumSchemaWithMetadata,
  EnumUtf8Precomputed,
  NumberSchemaWithMetadata,
  SchemaType,
  SchemaWithMetadata,
  TextSchemaWithMetadata,
} from './schema/types.js';

/**
 * Tag attribute schema - maps field names to Sury schemas
 *
 * NOTE: This uses an object type with index signature rather than Record<>
 * because consumer packages may add method properties (validate, parse, etc.)
 * which would conflict with Record<string, Schema>'s strict index signature.
 */
export type TagAttributeSchema = {
  [key: string]: Sury.Schema<unknown, unknown> | ((...args: unknown[]) => unknown);
};

/**
 * Get schema field entries, filtering out methods that may be added by consumer packages.
 *
 * Methods like validate, parse, safeParse, extend are added by consumer packages
 * (e.g., lmao's defineTagAttributes) but should not be treated as schema fields.
 *
 * @param schema - Tag attribute schema (possibly with methods)
 * @returns Array of [fieldName, fieldSchema] tuples, excluding functions
 */
export function getSchemaFields<T extends TagAttributeSchema>(
  schema: T,
): Array<[string, Sury.Schema<unknown, unknown>]> {
  return Object.entries(schema).filter(([_, value]) => typeof value !== 'function') as Array<
    [string, Sury.Schema<unknown, unknown>]
  >;
}
