/**
 * Schema builder for lmao - extends arrow-builder's schema builder with feature flags, masking, and binary sugar
 *
 * This module re-exports the S object from arrow-builder and adds:
 * - Feature flag support via .default().sync()/.async() pattern
 * - Masking transformations for sensitive data
 * - S.unknown() and S.object<T>() sugar for msgpack-encoded binary columns
 * - S.binary() passthrough to arrow-builder's S.binary()
 */

import { encode } from '@msgpack/msgpack';
import {
  S as ArrowS,
  type BinaryEncoder,
  type LazyBinarySchema,
  type LazyBooleanSchema,
  type LazyCategorySchema,
  type LazyEnumSchema,
  type LazyNumberSchema,
  type LazyTextSchema,
  type Schema,
} from '@smoothbricks/arrow-builder';
import type { FeatureFlagDefinition, FlagBuilderWithDefault, SchemaBuilder, SchemaOrFlagBuilder } from './types.js';

/**
 * Msgpack encoder singleton -- per-value, synchronous.
 * Encodes at flush time (cold path), not on the hot write path.
 * @msgpack/msgpack already handles most types; functions/symbols become undefined (dropped silently).
 *
 * Both wrap @msgpack/msgpack's encode(). This encoder is private to the schema builder;
 */
const msgpackEncoder: BinaryEncoder = {
  encode(value: unknown): Uint8Array {
    return encode(value) as Uint8Array;
  },
};

/**
 * Create a flag builder that wraps a schema object
 * This allows schemas to be used for both tag attributes and feature flags
 */
function createSchemaWithFlagBuilder<T>(schema: Schema<T>): SchemaOrFlagBuilder<T> {
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
 * STRING TYPE SYSTEM (See specs/lmao/01a_trace_schema_system.md):
 * Three distinct string types, each with different storage strategies:
 * - S.enum(['A', 'B', 'C']) - Known values at compile time (Uint8Array, 1 byte)
 * - S.category() - Repeated values (raw string storage, dictionary built at Arrow conversion)
 * - S.text() - Unique values (no dictionary overhead)
 */
const schemaBuilderImpl: SchemaBuilder = {
  /**
   * Create number schema
   *
   * Usage:
   * - S.number() - any number for tag attributes
   * - S.number().default(0).sync() - feature flag
   */
  number: () => {
    const schema = ArrowS.number();
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<number> & LazyNumberSchema;
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
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<boolean> & LazyBooleanSchema;
  },

  /**
   * Wrap schema to make it optional
   *
   * Usage:
   * - SchemaBuilder.optional(SchemaBuilder.string()) - string | undefined
   */
  optional: <T>(schema: Schema<T>): Schema<T | undefined, T | undefined> => {
    return ArrowS.optional(schema);
  },

  /**
   * Create union of multiple schemas
   *
   * Usage:
   * - SchemaBuilder.union([SchemaBuilder.string(), SchemaBuilder.number()]) - string | number
   */
  union: <T extends readonly [Schema, ...Schema[]]>(schemas: T) => {
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
   */
  enum: <T extends readonly string[]>(values: T): SchemaOrFlagBuilder<T[number]> & LazyEnumSchema<T[number]> => {
    const schema = ArrowS.enum(values);
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<T[number]> & LazyEnumSchema<T[number]>;
  },

  /**
   * Category - Values that often repeat (limited cardinality)
   *
   * Storage: Raw strings (no hot-path interning)
   * Arrow: Dictionary built dynamically from values at Arrow conversion
   * Use for: userIds, sessionIds, moduleNames, spanNames, table names
   */
  category: (): SchemaOrFlagBuilder<string> & LazyCategorySchema => {
    const schema = ArrowS.category();
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<string> & LazyCategorySchema;
  },

  /**
   * Text - Unique values that rarely repeat
   *
   * Storage: Raw strings without interning
   * Arrow: Plain string column (no dictionary overhead)
   * Use for: Unique error messages, URLs, request bodies, masked queries
   */
  text: (): SchemaOrFlagBuilder<string> & LazyTextSchema => {
    const schema = ArrowS.text();
    return createSchemaWithFlagBuilder(schema) as SchemaOrFlagBuilder<string> & LazyTextSchema;
  },

  /**
   * Binary - Raw bytes or encoder-wrapped values
   *
   * Passthrough to arrow-builder's S.binary().
   * Without encoder: accepts Uint8Array (raw binary).
   * With encoder: accepts any value T (encoded to bytes at flush time).
   */
  binary: ((options?: { encoder: BinaryEncoder }): LazyBinarySchema => {
    if (options?.encoder) {
      return ArrowS.binary(options);
    }
    return ArrowS.binary();
  }) as SchemaBuilder['binary'],

  /**
   * Unknown - accepts any value, msgpack-encoded at flush time
   *
   * Sugar for S.binary({ encoder: msgpackEncoder }).
   * Use for: arbitrary objects, request bodies, error context, unknown-shape data.
   * Hot path stores frozen object reference; flush path encodes via @msgpack/msgpack.
   */
  unknown: (): LazyBinarySchema<unknown> => {
    return ArrowS.binary({ encoder: msgpackEncoder });
  },

  /**
   * Object<T> - typed variant of unknown, same underlying binary storage with msgpack encoding
   *
   * TypeScript enforces shape at compile time; no runtime validation on hot path.
   * Sugar for S.binary<T>({ encoder: msgpackEncoder }).
   */
  object: <T extends object>(): LazyBinarySchema<T> => {
    return ArrowS.binary<T>({ encoder: msgpackEncoder });
  },
};

// Export as S for convenience
// Note: The SchemaBuilder type is exported from types.ts
export const S = schemaBuilderImpl;
