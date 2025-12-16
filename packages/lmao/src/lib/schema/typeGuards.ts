/**
 * Type guard functions for runtime type checking
 *
 * These functions provide type-safe runtime validation for various types used in the system.
 * They follow the TypeScript type predicate pattern: `(value: unknown): value is Type`
 */

import type * as arrow from 'apache-arrow';
import type { EvaluationContext, UsageContext } from './defineFeatureFlags.js';
import type {
  EnumSchemaWithMetadata,
  EnumUtf8Precomputed,
  FeatureFlagDefinition,
  LmaoSchemaType,
  SchemaWithMetadata,
} from './types.js';

/**
 * Type guard to check if a value is an Arrow builder
 * Arrow builders have an `append` method for adding values to columnar storage
 */
export function isArrowBuilder(value: unknown): value is arrow.Builder {
  return (
    value !== null &&
    value !== undefined &&
    typeof value === 'object' &&
    'append' in value &&
    typeof (value as Record<string, unknown>).append === 'function'
  );
}

/**
 * Valid LMAO schema types
 */
const LMAO_SCHEMA_TYPES: readonly LmaoSchemaType[] = ['enum', 'category', 'text', 'number', 'boolean'];

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
    return LMAO_SCHEMA_TYPES.includes(obj.__schema_type as LmaoSchemaType);
  }

  // If __schema_type is not present, it's still valid (just unknown type)
  return true;
}

/**
 * Type guard to check if a value is an EnumSchemaWithMetadata
 * This is used to safely access enum values metadata
 */
export function isEnumSchemaWithMetadata(value: unknown): value is EnumSchemaWithMetadata {
  if (!isSchemaWithMetadata(value)) {
    return false;
  }

  const obj = value as SchemaWithMetadata;

  return (
    obj.__schema_type === 'enum' &&
    '__enum_values' in obj &&
    Array.isArray((obj as EnumSchemaWithMetadata).__enum_values)
  );
}

/**
 * Get the LMAO schema type from a value, returning undefined if not found
 * Safe alternative to casting to access __schema_type
 */
export function getLmaoSchemaType(value: unknown): LmaoSchemaType | undefined {
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
  if (isEnumSchemaWithMetadata(value)) {
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
  if (isEnumSchemaWithMetadata(value) && '__enum_utf8' in value) {
    return (value as EnumSchemaWithMetadata).__enum_utf8;
  }
  return undefined;
}

/**
 * Type guard to check if a value is a FeatureFlagDefinition
 */
export function isFeatureFlagDefinition(value: unknown): value is FeatureFlagDefinition<string | number | boolean> {
  if (value === null || typeof value !== 'object') {
    return false;
  }

  const obj = value as Record<string, unknown>;

  return (
    'schema' in obj &&
    'defaultValue' in obj &&
    'evaluationType' in obj &&
    (obj.evaluationType === 'sync' || obj.evaluationType === 'async')
  );
}

/**
 * Type guard to check if a value is a valid EvaluationContext
 */
export function isEvaluationContext(value: unknown): value is EvaluationContext {
  if (value === null || typeof value !== 'object') {
    return false;
  }

  const obj = value as Record<string, unknown>;

  // EvaluationContext can have various string/number/boolean properties
  // Check that all values are of valid types
  for (const key in obj) {
    const val = obj[key];
    if (val !== undefined && typeof val !== 'string' && typeof val !== 'number' && typeof val !== 'boolean') {
      return false;
    }
  }

  return true;
}

/**
 * Type guard to check if a value is a valid UsageContext
 */
export function isUsageContext(value: unknown): value is UsageContext {
  if (value === null || typeof value !== 'object') {
    return false;
  }

  const obj = value as Record<string, unknown>;

  // Check action property if present
  if ('action' in obj && typeof obj.action !== 'string' && obj.action !== undefined) {
    return false;
  }

  // Check outcome property if present
  if ('outcome' in obj && obj.outcome !== undefined && obj.outcome !== 'success' && obj.outcome !== 'failure') {
    return false;
  }

  // Check value property if present
  if ('value' in obj && typeof obj.value !== 'number' && obj.value !== undefined) {
    return false;
  }

  // Check metadata property if present
  if ('metadata' in obj && obj.metadata !== undefined) {
    if (typeof obj.metadata !== 'object' || obj.metadata === null) {
      return false;
    }

    // Validate metadata values
    const metadata = obj.metadata as Record<string, unknown>;
    for (const key in metadata) {
      const val = metadata[key];
      if (typeof val !== 'string' && typeof val !== 'number' && typeof val !== 'boolean') {
        return false;
      }
    }
  }

  return true;
}

/**
 * Type guard to check if a value is a string
 * Useful for narrowing unknown types in safe contexts
 */
export function isString(value: unknown): value is string {
  return typeof value === 'string';
}

/**
 * Type guard to check if a value is a number
 * Useful for narrowing unknown types in safe contexts
 */
export function isNumber(value: unknown): value is number {
  return typeof value === 'number' && !Number.isNaN(value);
}

/**
 * Type guard to check if a value is a boolean
 * Useful for narrowing unknown types in safe contexts
 */
export function isBoolean(value: unknown): value is boolean {
  return typeof value === 'boolean';
}

/**
 * Type guard to check if a value is a primitive type (string | number | boolean)
 * Useful for validating feature flag values and tag attributes
 */
export function isPrimitive(value: unknown): value is string | number | boolean {
  return isString(value) || isNumber(value) || isBoolean(value);
}

/**
 * Type guard to check if a value is a plain object (not array, not null)
 */
export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}

/**
 * Type guard to check if a value is a valid record with primitive values
 * Useful for validating tag attribute objects
 */
export function isRecordOfPrimitives(value: unknown): value is Record<string, string | number | boolean> {
  if (!isPlainObject(value)) {
    return false;
  }

  for (const key in value) {
    if (!isPrimitive(value[key])) {
      return false;
    }
  }

  return true;
}
