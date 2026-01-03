/**
 * Schema type definitions for lmao
 *
 * Re-exports core schema types from arrow-builder and adds lmao-specific types
 * for feature flags and type inference.
 */

import type * as Sury from '@sury/sury';

// Re-export schema metadata types from arrow-builder (single source of truth)
export type {
  // Eager schema types (allocated immediately, no null bitmap)
  EagerBooleanSchema,
  EagerCategorySchema,
  EagerEnumSchema,
  EagerNumberSchema,
  EagerTextSchema,
  // Metadata and utility types
  EnumUtf8Precomputed,
  // Lazy schema types (default - allocated on first write)
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyEnumSchema,
  LazyNumberSchema,
  LazyTextSchema,
  MaskPreset,
  MaskTransform,
  SchemaFields,
  SchemaType,
  SchemaWithMetadata,
} from '@smoothbricks/arrow-builder';

// Re-export ColumnSchema and SchemaFields from arrow-builder for external use
export { ColumnSchema, isColumnSchema } from '@smoothbricks/arrow-builder';
// Re-export Sury's core types for external use
export type { Input, Output, Schema } from '@sury/sury';

// Re-export LogSchema for external use
export { isLogSchema, LogSchema } from './LogSchema.js';

// Import schema metadata types for use in InferSchema and local type definitions
import type {
  EagerBooleanSchema,
  EagerCategorySchema,
  EagerEnumSchema,
  EagerNumberSchema,
  EagerTextSchema,
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyEnumSchema,
  LazyNumberSchema,
  LazyTextSchema,
  SchemaFields,
} from '@smoothbricks/arrow-builder';
import type { DEFINED_LOG_SCHEMA_BRAND } from './defineLogSchema.js';
import type { LogSchema } from './LogSchema.js';

/**
 * Extract the original schema fields type from LogSchema or DefinedLogSchema.
 * If T is a LogSchema, extract the fields type.
 * If T has the brand marker, extract the original schema from it.
 * Otherwise, return T unchanged.
 */
type ExtractSchemaFields<T> =
  T extends LogSchema<infer Fields>
    ? Fields
    : T extends {
          readonly [DEFINED_LOG_SCHEMA_BRAND]?: infer Original;
        }
      ? Original extends SchemaFields
        ? Original
        : T extends SchemaFields
          ? T
          : never
      : T extends SchemaFields
        ? T
        : never;

/**
 * Filter out function keys from a schema type.
 * Methods like validate, parse, safeParse, extend are added by defineLogSchema
 * but should not be treated as schema fields for iteration.
 *
 * Works on LogSchema fields or plain schema objects.
 */
type SchemaFieldKeys<T> =
  T extends LogSchema<infer Fields>
    ? keyof Fields
    : ExtractSchemaFields<T> extends infer Fields
      ? keyof Fields extends infer K
        ? K extends string
          ? K
          : never
        : never
      : never;

/**
 * Extract TypeScript output types from log schema
 * This enables full type inference from Sury schemas
 *
 * IMPORTANT: This type must properly infer from schemas with __schema_type metadata
 *
 * Works with LogSchema instances or plain schema objects.
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
export type InferSchema<T extends LogSchema | SchemaFields> = {
  [K in SchemaFieldKeys<T>]: ExtractSchemaFields<T>[K] extends LazyEnumSchema<infer E>
    ? E
    : ExtractSchemaFields<T>[K] extends EagerEnumSchema<infer E2>
      ? E2
      : ExtractSchemaFields<T>[K] extends LazyCategorySchema | EagerCategorySchema
        ? string
        : ExtractSchemaFields<T>[K] extends LazyTextSchema | EagerTextSchema
          ? string
          : ExtractSchemaFields<T>[K] extends LazyNumberSchema | EagerNumberSchema
            ? number
            : ExtractSchemaFields<T>[K] extends LazyBooleanSchema | EagerBooleanSchema
              ? boolean
              : ExtractSchemaFields<T>[K] extends Sury.Schema<infer Out, unknown>
                ? Out
                : never;
};

/**
 * Extract TypeScript input types from log schema
 * Used for validation before transformation
 *
 * NOTE: Function properties (validate, parse, etc.) are filtered out.
 */
export type InferSchemaInput<T extends LogSchema | SchemaFields> = {
  [K in SchemaFieldKeys<T>]: ExtractSchemaFields<T>[K] extends Sury.Schema<unknown, infer In> ? In : never;
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
 * Combines lmao's FlagBuilder with arrow-builder's LazyCategorySchema.
 */
export type CategorySchemaOrFlagBuilder = SchemaOrFlagBuilder<string> & LazyCategorySchema;

/**
 * Text schema with flag builder, mask method, and eager method.
 * Combines lmao's FlagBuilder with arrow-builder's LazyTextSchema.
 */
export type TextSchemaOrFlagBuilder = SchemaOrFlagBuilder<string> & LazyTextSchema;

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
