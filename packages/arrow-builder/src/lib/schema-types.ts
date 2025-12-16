/**
 * Schema type definitions for arrow-builder
 *
 * Arrow-builder is a low-level columnar buffer engine that works with
 * generic schemas provided by consumer packages.
 */

import type * as Sury from '@sury/sury';

/**
 * Tag attribute schema - maps field names to Sury schemas
 */
export type TagAttributeSchema = Record<string, Sury.Schema<unknown, unknown>>;

/**
 * Schema type marker for columnar storage
 */
export type SchemaType = 'enum' | 'category' | 'text' | 'number' | 'boolean';

/**
 * Schema with metadata attached
 * Used to determine which TypedArray to create
 */
export interface SchemaWithMetadata {
  __schema_type?: SchemaType;
  __enum_values?: readonly string[];
}
