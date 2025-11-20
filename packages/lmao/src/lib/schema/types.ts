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
 * Schema builder interface that wraps Sury with custom API
 */
export interface SchemaBuilder {
  // Primitive types
  string(): Sury.Schema<string, string>;
  number(): Sury.Schema<number, number>;
  boolean(): Sury.Schema<boolean, boolean>;
  
  // Optional wrapper
  optional<T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined>;
  
  // Union types - for multiple schemas
  union<T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>>;
  
  // Enum - for string literal unions (common case)
  enum<T extends readonly string[]>(values: T): Sury.Schema<T[number], string>;
  
  // String with masking transformation
  masked(type: MaskType): Sury.Schema<string, string>;
}

/**
 * Masking helper function type
 */
export type MaskTransform = (value: string) => string;
