/**
 * Schema type definitions for arrow-builder
 *
 * Arrow-builder is a low-level columnar buffer engine that works with
 * Sury schemas that have __schema_type metadata attached.
 */

import type * as Sury from '@sury/sury';

// Type alias for schema objects (Record<string, Schema>)
export type SchemaFields = Record<string, Sury.Schema<unknown, unknown>>;

// Re-export ColumnSchema for external use (class is both value and type)
export { ColumnSchema, isColumnSchema } from './schema/ColumnSchema.js';

// Re-export schema metadata types from the schema module
export type {
  EagerBooleanSchema,
  // Brands
  EagerBrand,
  EagerCategorySchema,
  EagerEnumSchema,
  EagerNumberSchema,
  EagerTextSchema,
  // Utility types
  EnumUtf8Precomputed,
  // Boolean schemas
  LazyBooleanSchema,
  LazyBrand,
  // Category schemas
  LazyCategorySchema,
  // Enum schemas
  LazyEnumSchema,
  // Number schemas
  LazyNumberSchema,
  // Text schemas
  LazyTextSchema,
  MaskPreset,
  MaskTransform,
  SchemaType,
  SchemaWithMetadata,
} from './schema/types.js';
