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

function isSchemaType(value: unknown): value is SchemaType {
  return (
    value === 'enum' ||
    value === 'category' ||
    value === 'text' ||
    value === 'number' ||
    value === 'boolean' ||
    value === 'bigUint64' ||
    value === 'binary'
  );
}

function hasReadonlyStringArray<T extends string>(
  value: { [K in T]?: unknown },
  key: T,
): value is { [K in T]: readonly string[] } {
  return Array.isArray(value[key]) && value[key].every((entry) => typeof entry === 'string');
}

function hasMaskTransformProperty(
  value: SchemaWithMetadata & { __mask_transform?: unknown },
): value is SchemaWithMetadata & { __mask_transform: MaskTransform } {
  return typeof value.__mask_transform === 'function';
}

function hasBinaryEncoderProperty(
  value: SchemaWithMetadata & { __binary_encoder?: unknown },
): value is SchemaWithMetadata & { __binary_encoder: BinaryEncoder } {
  return (
    typeof value.__binary_encoder === 'object' && value.__binary_encoder !== null && 'encode' in value.__binary_encoder
  );
}

function hasEnumUtf8(value: { __enum_utf8?: EnumUtf8Precomputed }): value is { __enum_utf8: EnumUtf8Precomputed } {
  return value.__enum_utf8 !== undefined;
}

/**
 * Type guard to check if a value is a SchemaWithMetadata
 * This is used to safely access __schema_type metadata on schemas
 */
/** Narrow unknown to Record<string, unknown> — non-null object with string-keyed access. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isSchemaWithMetadata(value: unknown): value is SchemaWithMetadata {
  if (!isRecord(value)) {
    return false;
  }

  const obj = value;

  // __schema_type is optional, but if present must be a valid type
  if ('__schema_type' in obj) {
    return isSchemaType(obj.__schema_type);
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

  return value.__schema_type === 'enum' && hasReadonlyStringArray(value, '__enum_values');
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
  if (isEnumSchema(value) && hasEnumUtf8(value)) {
    return value.__enum_utf8;
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

  return hasMaskTransformProperty(value);
}

/**
 * Get mask transform function from a schema
 * Returns undefined if not present
 */
export function getMaskTransform(value: unknown): MaskTransform | undefined {
  if (hasMaskTransform(value)) {
    return value.__mask_transform;
  }
  return undefined;
}

/**
 * Get binary encoder from a schema, returning undefined if not a binary schema
 * or if no encoder was provided (raw Uint8Array mode).
 */
export function getBinaryEncoder(value: unknown): BinaryEncoder | undefined {
  if (isSchemaWithMetadata(value) && value.__schema_type === 'binary' && hasBinaryEncoderProperty(value)) {
    return value.__binary_encoder;
  }
  return undefined;
}
