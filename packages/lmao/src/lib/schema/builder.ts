/**
 * Schema builder for lmao - extends arrow-builder's schema builder with feature flags and masking
 *
 * This module re-exports the S object from arrow-builder and adds:
 * - Feature flag support via .default().sync()/.async() pattern
 * - Masking transformations for sensitive data
 */

import {
  S as ArrowS,
  type BooleanSchemaWithMetadata,
  type CategorySchemaWithMask,
  type EnumSchemaWithMetadata,
  type NumberSchemaWithMetadata,
  type TextSchemaWithMask,
} from '@smoothbricks/arrow-builder';
import type * as Sury from '@sury/sury';
import type { FeatureFlagDefinition, FlagBuilderWithDefault, SchemaBuilder, SchemaOrFlagBuilder } from './types.js';

/**
 * Create a flag builder that wraps a Sury schema
 * This allows schemas to be used for both tag attributes and feature flags
 */
function createSchemaWithFlagBuilder<T>(schema: Sury.Schema<T, unknown>): SchemaOrFlagBuilder<T> {
  const schemaWithBuilder = schema as SchemaOrFlagBuilder<T>;

  // Add the .default() method for feature flag definitions
  schemaWithBuilder.default = (defaultValue: T): FlagBuilderWithDefault<T> => ({
    sync(): FeatureFlagDefinition<T, 'sync'> {
      return {
        schema,
        defaultValue,
        evaluationType: 'sync' as const,
      };
    },
    async(): FeatureFlagDefinition<T, 'async'> {
      return {
        schema,
        defaultValue,
        evaluationType: 'async' as const,
      };
    },
  });

  return schemaWithBuilder;
}

/**
 * Schema builder that wraps arrow-builder's S with feature flag support
 *
 * Provides a clean API while leveraging Sury's performance:
 * - 94,828 ops/ms validation (fastest in JavaScript)
 * - 14.1 kB bundle size (smallest composable library)
 * - Full TypeScript inference
 * - Runtime transformations
 *
 * STRING TYPE SYSTEM (See specs/01a_trace_schema_system.md):
 * Three distinct string types, each with different storage strategies:
 * - S.enum(['A', 'B', 'C']) - Known values at compile time (Uint8Array, 1 byte)
 * - S.category() - Repeated values (Uint32Array with string interning)
 * - S.text() - Unique values (no dictionary overhead)
 */
const schemaBuilderImpl: SchemaBuilder = {
  /**
   * Create number schema
   *
   * Usage:
   * - S.number() - any number for tag attributes
   * - S.number().default(0).sync() - feature flag
   * - S.refine(S.number(), x => x > 0) - with validation
   */
  number: () => {
    const schema = ArrowS.number();
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<number> & NumberSchemaWithMetadata;
  },

  /**
   * Create boolean schema
   *
   * Usage:
   * - S.boolean() - for tag attributes
   * - S.boolean().default(false).sync() - feature flag
   */
  boolean: () => {
    const schema = ArrowS.boolean();
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<boolean> & BooleanSchemaWithMetadata;
  },

  /**
   * Wrap schema to make it optional
   *
   * Usage:
   * - SchemaBuilder.optional(SchemaBuilder.string()) - string | undefined
   */
  optional: <T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined> => {
    return ArrowS.optional(schema);
  },

  /**
   * Create union of multiple schemas
   *
   * Usage:
   * - SchemaBuilder.union([SchemaBuilder.string(), SchemaBuilder.number()]) - string | number
   */
  union: <T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T,
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>> => {
    return ArrowS.union(schemas);
  },

  /**
   * Enum - Known values at compile time
   *
   * Storage: Uint8Array (1 byte) with compile-time mapping
   * Arrow: Dictionary with pre-defined values (UTF-8 pre-computed at definition time)
   * Use for: Operations, HTTP methods, entry types, status enums
   *
   * Usage:
   * - S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']) - for tag attributes
   * - S.enum(['dev', 'staging', 'prod']).default('dev').sync() - feature flag
   *
   * Generated code maps strings to integers:
   * switch(value) {
   *   case 'CREATE': buffer.attr_operation[idx] = 0; break;
   *   case 'READ': buffer.attr_operation[idx] = 1; break;
   *   ...
   * }
   *
   * UTF-8 bytes are pre-computed at schema definition time (cold path) so
   * Arrow conversion just copies the pre-built dictionary data (zero re-encoding).
   */
  enum: <T extends readonly string[]>(
    values: T,
  ): SchemaOrFlagBuilder<T[number]> & EnumSchemaWithMetadata<T[number]> => {
    const schema = ArrowS.enum(values);
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<T[number]> & EnumSchemaWithMetadata<T[number]>;
  },

  /**
   * Category - Values that often repeat (limited cardinality)
   *
   * Storage: Uint32Array indices with string interning
   * Arrow: Dictionary built dynamically from interned strings
   * Use for: userIds, sessionIds, moduleNames, spanNames, table names
   *
   * Usage:
   * - S.category() - for tag attributes
   * - S.category().mask('hash') - with masking applied during Arrow conversion
   * - S.category().default('default-value').sync() - feature flag
   *
   * Hot path write:
   * buffer.attr_userId[idx] = internString(userId); // Returns Uint32 index
   */
  category: (): SchemaOrFlagBuilder<string> & CategorySchemaWithMask => {
    const schema = ArrowS.category();
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<string> & CategorySchemaWithMask;
  },

  /**
   * Text - Unique values that rarely repeat
   *
   * Storage: Raw strings without interning
   * Arrow: Plain string column (no dictionary overhead)
   * Use for: Unique error messages, URLs, request bodies, masked queries
   *
   * Usage:
   * - S.text() - for tag attributes
   * - S.text().mask('sql') - with masking applied during Arrow conversion
   * - S.text().default('').sync() - feature flag (rare use case)
   *
   * Hot path write:
   * buffer.attr_errorMsg[idx] = rawString; // No interning
   */
  text: (): SchemaOrFlagBuilder<string> & TextSchemaWithMask => {
    const schema = ArrowS.text();
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<string> & TextSchemaWithMask;
  },
};

// Export as S for convenience (matches Sury convention)
// Note: The SchemaBuilder type is exported from types.ts
export const S = schemaBuilderImpl;
