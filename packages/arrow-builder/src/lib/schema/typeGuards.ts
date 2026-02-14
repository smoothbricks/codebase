/**
 * Type guard functions for schema types
 *
 * These functions provide type-safe runtime validation for schema metadata types.
 */

import type {
  BinaryEncoder,
  EagerEnumSchema,
  EnumUtf8Precomputed,
  LazyCategorySchema,
  LazyEnumSchema,
  LazyTextSchema,
  MaskTransform,
  SchemaType,
  SchemaWithMetadata,
} from './types.js';

/**
 * Valid schema types
 */
const SCHEMA_TYPES: readonly SchemaType[] = ['enum', 'category', 'text', 'number', 'boolean', 'bigUint64', 'binary'];

/**
 * Type guard to check if a value is a SchemaWithMetadata
 * This is used to safely access __schema_type metadata on schemas
 */
export function isSchemaWithMetadata(value: unknown): value is SchemaWithMetadata {
  if (value === null || typeof value !== 'object') {
    return false;
  }

  const obj = value as Record<string, unknown>;

  // __schema_type is optional, but if present must be a valid type
  if ('__schema_type' in obj) {
    return SCHEMA_TYPES.includes(obj.__schema_type as SchemaType);
  }

  // If __schema_type is not present, it's still valid (just unknown type)
  return true;
}

/**
 * Type guard to check if a value is an enum schema (LazyEnumSchema | EagerEnumSchema)
 * This is used to safely access enum values metadata
 */
export function isEnumSchema(value: unknown): value is LazyEnumSchema | EagerEnumSchema {
  if (!isSchemaWithMetadata(value)) {
    return false;
  }

  const obj = value as SchemaWithMetadata;

  return (
    obj.__schema_type === 'enum' &&
    '__enum_values' in obj &&
    Array.isArray((obj as LazyEnumSchema | EagerEnumSchema).__enum_values)
  );
}

/**
 * Get the schema type from a value, returning undefined if not found
 * Safe alternative to casting to access __schema_type
 */
export function getSchemaType(value: unknown): SchemaType | undefined {
  if (isSchemaWithMetadata(value)) {
    return value.__schema_type;
  }
  return undefined;
}

/**
 * Get enum values from a schema, returning undefined if not an enum schema
 * Safe alternative to casting to access __enum_values
 */
export function getEnumValues(value: unknown): readonly string[] | undefined {
  if (isEnumSchema(value)) {
    return value.__enum_values;
  }
  return undefined;
}

/**
 * Get pre-computed UTF-8 bytes for enum values
 * Returns undefined if not an enum schema or if UTF-8 bytes weren't pre-computed
 *
 * Per string storage design: enum UTF-8 bytes are pre-computed at schema definition
 * time (cold path) so Arrow conversion just copies the pre-built dictionary data.
 */
export function getEnumUtf8(value: unknown): EnumUtf8Precomputed | undefined {
  if (isEnumSchema(value) && '__enum_utf8' in value) {
    return (value as LazyEnumSchema | EagerEnumSchema).__enum_utf8;
  }
  return undefined;
}

/**
 * Type guard to check if a schema has a mask transform.
 * Only lazy category/text schemas have mask transforms (mask() returns lazy).
 */
export function hasMaskTransform(value: unknown): value is LazyCategorySchema | LazyTextSchema {
  if (!isSchemaWithMetadata(value)) {
    return false;
  }

  return (
    '__mask_transform' in value && typeof (value as { __mask_transform?: unknown }).__mask_transform === 'function'
  );
}

/**
 * Get mask transform function from a schema
 * Returns undefined if not present
 */
export function getMaskTransform(value: unknown): MaskTransform | undefined {
  if (hasMaskTransform(value)) {
    return (value as LazyCategorySchema | LazyTextSchema).__mask_transform;
  }
  return undefined;
}

/**
 * Get binary encoder from a schema, returning undefined if not a binary schema
 * or if no encoder was provided (raw Uint8Array mode).
 */
export function getBinaryEncoder(value: unknown): BinaryEncoder | undefined {
  if (isSchemaWithMetadata(value) && value.__schema_type === 'binary') {
    return (value as { __binary_encoder?: BinaryEncoder }).__binary_encoder;
  }
  return undefined;
}
