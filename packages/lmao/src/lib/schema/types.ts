/**
 * Schema type definitions for lmao
 *
 * Re-exports core schema types from arrow-builder and adds lmao-specific types
 * for feature flags and type inference.
 */

import type * as Sury from '@sury/sury';

// Re-export schema metadata types from arrow-builder (single source of truth)
export type {
  BooleanSchemaWithMetadata,
  CategorySchemaWithMetadata,
  EnumSchemaWithMetadata,
  EnumUtf8Precomputed,
  MaskPreset,
  MaskTransform,
  NumberSchemaWithMetadata,
  SchemaType,
  SchemaWithMetadata,
  TagAttributeSchema,
  TextSchemaWithMetadata,
} from '@smoothbricks/arrow-builder';

// Re-export Sury's core types for external use
export type { Input, Output, Schema } from '@sury/sury';

// Import schema metadata types for use in InferTagAttributes and local type definitions
import type {
  BooleanSchemaWithMetadata,
  CategorySchemaWithMask,
  CategorySchemaWithMetadata,
  EnumSchemaWithMetadata,
  NumberSchemaWithMetadata,
  TagAttributeSchema,
  TextSchemaWithMask,
  TextSchemaWithMetadata,
} from '@smoothbricks/arrow-builder';
// Import the brand symbol from defineTagAttributes for ExtractOriginalSchema detection
import type { DEFINED_TAG_ATTRIBUTES_BRAND } from './defineTagAttributes.js';

/**
 * Extract the original schema type from DefinedTagAttributes.
 * If T has the brand marker, extract the original schema from it.
 * Otherwise, return T unchanged.
 */
type ExtractOriginalSchema<T extends TagAttributeSchema> = T extends {
  readonly [DEFINED_TAG_ATTRIBUTES_BRAND]?: infer Original extends TagAttributeSchema;
}
  ? Original
  : T;

/**
 * Filter out function keys from a schema type.
 * Methods like validate, parse, safeParse, extend are added by defineTagAttributes
 * but should not be treated as schema fields.
 *
 * Works on the ORIGINAL schema (without index signature pollution).
 */
type SchemaFieldKeys<T extends TagAttributeSchema> = keyof ExtractOriginalSchema<T> extends infer K
  ? K extends string
    ? string extends K // Exclude index signature (where K is exactly `string`)
      ? never
      : ExtractOriginalSchema<T>[K] extends (...args: unknown[]) => unknown // Exclude functions
        ? never
        : K
    : never
  : never;

/**
 * Extract TypeScript output types from tag attribute schema
 * This enables full type inference from Sury schemas
 *
 * IMPORTANT: This type must properly infer from schemas with __schema_type metadata
 *
 * For DefinedTagAttributes, uses the brand marker to extract the original schema
 * type before the index signature was added, preserving type inference.
 *
 * Type resolution order:
 * 1. Check if it's an enum schema -> extract enum type
 * 2. Check if it's a category schema -> string
 * 3. Check if it's a text schema -> string
 * 4. Check if it's a number schema -> number
 * 5. Check if it's a boolean schema -> boolean
 * 6. Fall back to Sury.Output<T[K]>
 *
 * NOTE: Function properties (validate, parse, etc.) are filtered out.
 */
export type InferTagAttributes<T extends TagAttributeSchema> = {
  [K in SchemaFieldKeys<T>]: ExtractOriginalSchema<T>[K] extends EnumSchemaWithMetadata<infer E>
    ? E
    : ExtractOriginalSchema<T>[K] extends CategorySchemaWithMetadata
      ? string
      : ExtractOriginalSchema<T>[K] extends TextSchemaWithMetadata
        ? string
        : ExtractOriginalSchema<T>[K] extends NumberSchemaWithMetadata
          ? number
          : ExtractOriginalSchema<T>[K] extends BooleanSchemaWithMetadata
            ? boolean
            : ExtractOriginalSchema<T>[K] extends Sury.Schema<infer Out, unknown>
              ? Out
              : never;
};

/**
 * Extract TypeScript input types from tag attribute schema
 * Used for validation before transformation
 *
 * NOTE: Function properties (validate, parse, etc.) are filtered out.
 */
export type InferTagAttributesInput<T extends TagAttributeSchema> = {
  [K in SchemaFieldKeys<T>]: T[K] extends Sury.Schema<unknown, infer In> ? In : never;
};

/**
 * Feature flag builder with default value support
 * Returned by S.string(), S.number(), S.boolean(), S.enum() when used for feature flags
 */
export interface FlagBuilder<T> {
  default(value: T): FlagBuilderWithDefault<T>;
}

/**
 * Feature flag builder with sync/async evaluation type support
 */
export interface FlagBuilderWithDefault<T> {
  sync(): FeatureFlagDefinition<T, 'sync'>;
  async(): FeatureFlagDefinition<T, 'async'>;
}

/**
 * Feature flag definition with default value and evaluation type
 */
export interface FeatureFlagDefinition<T, EvalType extends 'sync' | 'async' = 'sync' | 'async'> {
  schema: Sury.Schema<T, unknown>;
  defaultValue: T;
  evaluationType: EvalType;
}

/**
 * Schema that can be used for both tag attributes and feature flags
 * This type is returned by S.string(), S.number(), etc.
 */
export type SchemaOrFlagBuilder<T> = Sury.Schema<T, unknown> & FlagBuilder<T>;

/**
 * Category schema with flag builder, mask method, and eager method.
 * Extends arrow-builder's CategorySchemaWithMask which provides mask() and eager().
 */
export type CategorySchemaOrFlagBuilder = SchemaOrFlagBuilder<string> & CategorySchemaWithMask;

/**
 * Text schema with flag builder, mask method, and eager method.
 * Extends arrow-builder's TextSchemaWithMask which provides mask() and eager().
 */
export type TextSchemaOrFlagBuilder = SchemaOrFlagBuilder<string> & TextSchemaWithMask;

/**
 * Schema builder interface that wraps Sury with custom API
 * Supports both tag attributes and feature flags
 */
export interface SchemaBuilder {
  // Primitive types - return schemas that can also be used as flag builders
  number(): SchemaOrFlagBuilder<number>;
  boolean(): SchemaOrFlagBuilder<boolean>;

  // String types - THREE DISTINCT TYPES per specs/01a_trace_schema_system.md
  // IMPORTANT: Never use generic "string" - always choose enum/category/text

  /**
   * Enum - Known values at compile time
   * Storage: Uint8Array (1 byte) with compile-time mapping
   * Arrow: Dictionary with pre-defined values
   * Use for: Operations, HTTP methods, entry types, status enums
   * Example: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE'])
   */
  enum<T extends readonly string[]>(values: T): SchemaOrFlagBuilder<T[number]>;

  /**
   * Category - Values that often repeat (limited cardinality)
   * Storage: Uint32Array indices with string interning
   * Arrow: Dictionary built dynamically from interned strings
   * Use for: userIds, sessionIds, moduleNames, spanNames, table names
   *
   * Returns a schema with:
   * - .default().sync()/.async() for feature flags
   * - .mask(preset) for masking during Arrow conversion
   */
  category(): CategorySchemaOrFlagBuilder;

  /**
   * Text - Unique values that rarely repeat
   * Storage: Raw strings without interning
   * Arrow: Plain string column (no dictionary overhead)
   * Use for: Unique error messages, URLs, request bodies, masked queries
   *
   * Returns a schema with:
   * - .default().sync()/.async() for feature flags
   * - .mask(preset) for masking during Arrow conversion
   */
  text(): TextSchemaOrFlagBuilder;

  // Optional wrapper
  optional<T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined>;

  // Union types - for multiple schemas
  union<T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T,
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>>;
}

/**
 * Get schema field entries, filtering out methods added by defineTagAttributes
 *
 * Methods like validate, parse, safeParse, extend are added by defineTagAttributes
 * but should not be treated as schema fields for iteration.
 *
 * @param schema - Tag attribute schema (possibly with methods)
 * @returns Array of [fieldName, fieldSchema] tuples, excluding methods
 */
export function getSchemaFields<T extends TagAttributeSchema>(
  schema: T,
): Array<[string, Sury.Schema<unknown, unknown>]> {
  return Object.entries(schema).filter(([_, value]) => typeof value !== 'function') as Array<
    [string, Sury.Schema<unknown, unknown>]
  >;
}
