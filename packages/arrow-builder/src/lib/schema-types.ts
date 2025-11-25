/**
 * Minimal schema type definitions needed by arrow-builder
 * 
 * These types are duplicated from @smoothbricks/lmao to avoid circular dependencies.
 * Arrow-builder is a low-level package that should not depend on lmao at build time.
 * 
 * The full schema system lives in @smoothbricks/lmao.
 */

import type * as Sury from '@sury/sury';

/**
 * Tag attribute schema - maps field names to Sury schemas
 */
export type TagAttributeSchema = Record<string, Sury.Schema<unknown, unknown>>;

/**
 * LMAO schema type marker
 */
export type LmaoSchemaType = 'enum' | 'category' | 'text' | 'number' | 'boolean';

/**
 * Schema with LMAO metadata attached
 * Used to determine which TypedArray to create
 */
export interface SchemaWithMetadata {
  __lmao_type?: LmaoSchemaType;
  __lmao_enum_values?: readonly string[];
}
