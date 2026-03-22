/**
 * Schema type definitions for arrow-builder
 *
 * Arrow-builder is a low-level columnar buffer engine that works with
 * schema objects that have __schema_type metadata attached.
 */

import type { Schema } from './schema/core.js';

// Type alias for schema objects (Record<string, Schema>)
export type SchemaFields = Record<string, Schema>;

// Re-export ColumnSchema for external use (class is both value and type)
export { ColumnSchema, isColumnSchema } from './schema/ColumnSchema.js';

// Re-export schema metadata types from the schema module
export type {
  // Binary encoder interface
  BinaryEncoder,
  EagerBigUint64Schema,
  // Binary schemas
  EagerBinarySchema,
  EagerBooleanSchema,
  // Brands
  EagerBrand,
  EagerCategorySchema,
  EagerEnumSchema,
  EagerNumberSchema,
  EagerTextSchema,
  // Utility types
  EnumUtf8Precomputed,
  LazyBigUint64Schema,
  // Binary schemas
  LazyBinarySchema,
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
