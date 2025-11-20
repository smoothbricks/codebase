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
 */
export type InferTagAttributes<T extends TagAttributeSchema> = {
  [K in keyof T]: Sury.Output<T[K]>;
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
  string(): SchemaOrFlagBuilder<string>;
  number(): SchemaOrFlagBuilder<number>;
  boolean(): SchemaOrFlagBuilder<boolean>;
  
  // Optional wrapper
  optional<T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined>;
  
  // Union types - for multiple schemas
  union<T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>>;
  
  // Enum - for string literal unions (common case)
  enum<T extends readonly string[]>(values: T): SchemaOrFlagBuilder<T[number]>;
  
  // String with masking transformation
  masked(type: MaskType): Sury.Schema<string, string>;
}

/**
 * Masking helper function type
 */
export type MaskTransform = (value: string) => string;
