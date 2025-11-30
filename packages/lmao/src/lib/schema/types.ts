/**
 * Schema type definitions using Sury (ReScript Schema)
 * 
 * Sury provides:
 * - Runtime validation (94,828 ops/ms - fastest in JavaScript)
 * - TypeScript inference
 * - Transformations (e.g., masking)
 * - Standard Schema spec compliance
 */

import type * as Sury from '@sury/sury';

// Re-export Sury's core types for external use
export type { Schema, Output, Input } from '@sury/sury';

/**
 * Tag attribute schema - maps field names to Sury schemas
 * 
 * Example:
 * {
 *   userId: S.string().transform(hashString),
 *   requestId: S.string(),
 *   duration: S.number(),
 *   operation: S.literal('SELECT').or(S.literal('INSERT'))
 * }
 */
export type TagAttributeSchema = Record<string, Sury.Schema<unknown, unknown>>;

/**
 * Extract TypeScript output types from tag attribute schema
 * This enables full type inference from Sury schemas
 * 
 * IMPORTANT: This type must properly infer from schemas with __lmao_type metadata
 * 
 * Type resolution order:
 * 1. Check if it's an enum schema → extract enum type
 * 2. Check if it's a category schema → string
 * 3. Check if it's a text schema → string
 * 4. Check if it's a number schema → number
 * 5. Check if it's a boolean schema → boolean
 * 6. Fall back to Sury.Output<T[K]>
 */
export type InferTagAttributes<T extends TagAttributeSchema> = {
  [K in keyof T]: T[K] extends EnumSchemaWithMetadata<infer E>
    ? E
    : T[K] extends CategorySchemaWithMetadata
    ? string
    : T[K] extends TextSchemaWithMetadata
    ? string
    : T[K] extends NumberSchemaWithMetadata
    ? number
    : T[K] extends BooleanSchemaWithMetadata
    ? boolean
    : T[K] extends Sury.Schema<infer Out, unknown>
    ? Out
    : never;
};

/**
 * Extract TypeScript input types from tag attribute schema
 * Used for validation before transformation
 */
export type InferTagAttributesInput<T extends TagAttributeSchema> = {
  [K in keyof T]: Sury.Input<T[K]>;
};

/**
 * Masking transformations for sensitive data
 * Applied during Arrow table serialization (background processing)
 */
export type MaskType = 'hash' | 'url' | 'sql' | 'email';

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
   * Example: S.category (no arguments needed)
   */
  category(): SchemaOrFlagBuilder<string>;
  
  /**
   * Text - Unique values that rarely repeat
   * Storage: Raw strings without interning
   * Arrow: Plain string column (no dictionary overhead)
   * Use for: Unique error messages, URLs, request bodies, masked queries
   * Example: S.text (no arguments needed)
   */
  text(): SchemaOrFlagBuilder<string>;
  
  // Optional wrapper
  optional<T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined>;
  
  // Union types - for multiple schemas
  union<T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>>;
  
  // String with masking transformation - can be applied to category or text
  masked(type: MaskType): Sury.Schema<string, string>;
}

/**
 * Masking helper function type
 */
export type MaskTransform = (value: string) => string;

/**
 * Schema metadata types - attached to Sury schemas for code generation
 * 
 * These types allow us to access the __lmao_type metadata on schemas
 */
export type LmaoSchemaType = 'enum' | 'category' | 'text' | 'number' | 'boolean';

/**
 * Base schema with LMAO metadata
 * Using intersection type since we can't extend Sury.Schema directly
 */
export type SchemaWithMetadata<T = unknown> = Sury.Schema<T, unknown> & {
  __lmao_type?: LmaoSchemaType;
};

/**
 * Enum schema with enum values metadata
 */
export type EnumSchemaWithMetadata<T extends string = string> = Sury.Schema<T, unknown> & {
  __lmao_type: 'enum';
  __lmao_enum_values: readonly string[];
};

/**
 * Category schema with metadata
 */
export type CategorySchemaWithMetadata = Sury.Schema<string, unknown> & {
  __lmao_type: 'category';
};

/**
 * Text schema with metadata
 */
export type TextSchemaWithMetadata = Sury.Schema<string, unknown> & {
  __lmao_type: 'text';
};

/**
 * Number schema with metadata
 */
export type NumberSchemaWithMetadata = Sury.Schema<number, unknown> & {
  __lmao_type: 'number';
};

/**
 * Boolean schema with metadata
 */
export type BooleanSchemaWithMetadata = Sury.Schema<boolean, unknown> & {
  __lmao_type: 'boolean';
};

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
  schema: T
): Array<[string, Sury.Schema<unknown, unknown>]> {
  return Object.entries(schema).filter(
    ([_, value]) => typeof value !== 'function'
  ) as Array<[string, Sury.Schema<unknown, unknown>]>;
}
