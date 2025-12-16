/**
 * Sury-based schema builder with masking transformations
 *
 * This module provides the S object that wraps Sury's schema API
 * with custom masking transformations for sensitive data.
 * Also supports feature flag definitions with .default().sync()/.async() pattern.
 */

import * as Sury from '@sury/sury';
import type {
  BooleanSchemaWithMetadata,
  CategorySchemaWithMetadata,
  EnumSchemaWithMetadata,
  EnumUtf8Precomputed,
  FeatureFlagDefinition,
  FlagBuilderWithDefault,
  MaskTransform,
  MaskType,
  NumberSchemaWithMetadata,
  SchemaBuilder,
  SchemaOrFlagBuilder,
  TextSchemaWithMetadata,
} from './types.js';

/**
 * Pre-compute UTF-8 bytes for enum values at schema definition time.
 *
 * This is a COLD PATH operation that happens once per schema definition.
 * The pre-computed bytes are stored on the schema metadata and used
 * during Arrow conversion to avoid re-encoding enum strings.
 *
 * @param values - Array of enum string values
 * @returns Pre-computed UTF-8 data ready for Arrow dictionary
 */
function precomputeEnumUtf8(values: readonly string[]): EnumUtf8Precomputed {
  const encoder = new TextEncoder();

  // Encode each value
  const bytes = values.map((v) => encoder.encode(v));

  // Calculate total size and build offsets
  const offsets = new Int32Array(values.length + 1);
  offsets[0] = 0;
  let totalSize = 0;
  for (let i = 0; i < bytes.length; i++) {
    totalSize += bytes[i].length;
    offsets[i + 1] = totalSize;
  }

  // Build concatenated buffer
  const concatenated = new Uint8Array(totalSize);
  let offset = 0;
  for (const b of bytes) {
    concatenated.set(b, offset);
    offset += b.length;
  }

  return { bytes, concatenated, offsets };
}

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
 * Masking functions for sensitive data
 * Applied during Arrow table serialization (background processing)
 */
const maskingTransforms: Record<MaskType, MaskTransform> = {
  /**
   * Hash masking - creates deterministic hash for IDs
   * Maintains referential integrity while hiding actual values
   */
  hash: (value: string): string => {
    // Simple hash for demo - use crypto.subtle.digest in production
    let hash = 0;
    for (let i = 0; i < value.length; i++) {
      hash = (hash << 5) - hash + value.charCodeAt(i);
      hash = hash & hash; // Convert to 32-bit integer
    }
    return `0x${Math.abs(hash).toString(16).padStart(16, '0')}`;
  },

  /**
   * URL masking - hides domain while preserving path structure
   * Useful for HTTP request logging
   */
  url: (value: string): string => {
    try {
      const url = new URL(value);
      return `${url.protocol}//*****${url.pathname}${url.search}`;
    } catch {
      return '*****';
    }
  },

  /**
   * SQL masking - replaces literals with placeholders
   * Preserves query structure for analysis
   */
  sql: (value: string): string => {
    // Replace string literals and numbers with placeholders
    return value.replace(/'[^']*'/g, '?').replace(/\b\d+\b/g, '?');
  },

  /**
   * Email masking - shows first character and domain only
   * Maintains uniqueness while protecting privacy
   */
  email: (value: string): string => {
    const [local, domain] = value.split('@');
    if (!domain) return '*****';
    // Guard against empty local part (e.g., "@domain.com")
    if (!local || local.length === 0) return `*****@${domain}`;
    return `${local[0]}*****@${domain}`;
  },
};

/**
 * Schema builder that wraps Sury with our custom API
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
    // IMPORTANT: Use Object.create to clone with prototype chain intact
    // This avoids mutating the shared Sury.number singleton while preserving
    // the prototype methods needed for Sury.union() and other schema operations
    const schema = createSchemaWithFlagBuilder(
      Object.create(Object.getPrototypeOf(Sury.number), Object.getOwnPropertyDescriptors(Sury.number)),
    );
    // Attach number metadata for code generation by mutating the cloned object
    (schema as unknown as NumberSchemaWithMetadata).__schema_type = 'number';
    return schema as SchemaOrFlagBuilder<number> & NumberSchemaWithMetadata;
  },

  /**
   * Create boolean schema
   *
   * Usage:
   * - S.boolean() - for tag attributes
   * - S.boolean().default(false).sync() - feature flag
   */
  boolean: () => {
    // IMPORTANT: Use Object.create to clone with prototype chain intact
    // This avoids mutating the shared Sury.boolean singleton while preserving
    // the prototype methods needed for Sury.union() and other schema operations
    const schema = createSchemaWithFlagBuilder(
      Object.create(Object.getPrototypeOf(Sury.boolean), Object.getOwnPropertyDescriptors(Sury.boolean)),
    );
    // Attach boolean metadata for code generation by mutating the cloned object
    (schema as unknown as BooleanSchemaWithMetadata).__schema_type = 'boolean';
    return schema as SchemaOrFlagBuilder<boolean> & BooleanSchemaWithMetadata;
  },

  /**
   * Wrap schema to make it optional
   *
   * Usage:
   * - SchemaBuilder.optional(SchemaBuilder.string()) - string | undefined
   */
  optional: <T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined> => {
    return Sury.optional(schema) as Sury.Schema<T | undefined, T | undefined>;
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
    const schemaArray = [...schemas] as [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]];
    return Sury.union(schemaArray) as Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>>;
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
    if (values.length === 0) {
      throw new Error('Enum must have at least one value');
    }
    if (values.length > 256) {
      throw new Error('Enum can have at most 256 values (Uint8Array limit: indices 0-255)');
    }

    // Use refine to validate string is one of the allowed values
    // The callback receives (value, fail) where fail.fail(message) throws validation error
    const schema = Sury.refine(Sury.string, (value, fail): T[number] => {
      if (!values.includes(value)) {
        fail.fail(`Value must be one of: ${values.join(', ')}`);
      }
      return value as T[number];
    }) as Sury.Schema<T[number], string>;

    // Attach enum metadata for code generation by mutating the object
    const schemaWithMetadata = schema as unknown as EnumSchemaWithMetadata<T[number]>;
    schemaWithMetadata.__schema_type = 'enum';
    schemaWithMetadata.__enum_values = values;
    // Pre-compute UTF-8 bytes at schema definition time (cold path)
    // This avoids re-encoding enum strings during Arrow conversion
    schemaWithMetadata.__enum_utf8 = precomputeEnumUtf8(values);

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
   * - S.category().default('default-value').sync() - feature flag
   *
   * Hot path write:
   * buffer.attr_userId[idx] = internString(userId); // Returns Uint32 index
   */
  category: (): SchemaOrFlagBuilder<string> & CategorySchemaWithMetadata => {
    // IMPORTANT: Use Object.create to clone with prototype chain intact
    // This avoids mutating the shared Sury.string singleton while preserving
    // the prototype methods needed for Sury.union() and other schema operations
    const schema = createSchemaWithFlagBuilder(
      Object.create(Object.getPrototypeOf(Sury.string), Object.getOwnPropertyDescriptors(Sury.string)),
    );

    // Attach category metadata for code generation by mutating the cloned object
    (schema as unknown as CategorySchemaWithMetadata).__schema_type = 'category';

    return schema as SchemaOrFlagBuilder<string> & CategorySchemaWithMetadata;
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
   * - S.text().default('').sync() - feature flag (rare use case)
   *
   * Hot path write:
   * buffer.attr_errorMsg[idx] = rawString; // No interning
   */
  text: (): SchemaOrFlagBuilder<string> & TextSchemaWithMetadata => {
    // IMPORTANT: Use Object.create to clone with prototype chain intact
    // This avoids mutating the shared Sury.string singleton while preserving
    // the prototype methods needed for Sury.union() and other schema operations
    const schema = createSchemaWithFlagBuilder(
      Object.create(Object.getPrototypeOf(Sury.string), Object.getOwnPropertyDescriptors(Sury.string)),
    );

    // Attach text metadata for code generation by mutating the cloned object
    (schema as unknown as TextSchemaWithMetadata).__schema_type = 'text';

    return schema as SchemaOrFlagBuilder<string> & TextSchemaWithMetadata;
  },

  /**
   * Create string schema with masking transformation
   *
   * Masking is applied during serialization, not validation.
   * This allows:
   * - Full data in memory for processing
   * - Masked data in logs/traces for privacy
   *
   * Usage:
   * - SchemaBuilder.masked('hash') - for IDs (0x...)
   * - SchemaBuilder.masked('email') - for emails (j*****@example.com)
   * - SchemaBuilder.masked('url') - for URLs (https://*****.com/path)
   * - SchemaBuilder.masked('sql') - for SQL queries (SELECT * FROM users WHERE id = ?)
   */
  masked: (type: MaskType) => {
    return Sury.transform(Sury.string, maskingTransforms[type]);
  },
};

// Export as S for convenience (matches Sury convention)
// Note: The SchemaBuilder type is exported from types.ts
export const S = schemaBuilderImpl;

/**
 * Export masking transforms for custom use
 */
export const mask = maskingTransforms;
