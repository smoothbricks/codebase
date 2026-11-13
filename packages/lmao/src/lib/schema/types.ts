/**
 * Schema type definitions for lmao
 *
 * Re-exports the field node and column types from arrow-builder and adds lmao-specific types
 * for feature flags and type inference.
 */

import type { Field, FieldBuilder, LazyField, Schema } from '@smoothbricks/arrow-builder';

// Re-export the field and column types from arrow-builder (single source of truth)
export type {
  BinaryEncoder,
  EagerBinarySchema,
  EagerBooleanSchema,
  EagerCategorySchema,
  EagerEnumSchema,
  EagerNumberSchema,
  EagerTextSchema,
  EnumUtf8Precomputed,
  Field,
  Input,
  LazyBinarySchema,
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyEnumSchema,
  LazyNumberSchema,
  LazyTextSchema,
  MaskPreset,
  MaskTransform,
  Output,
  Schema,
  SchemaFields,
  StorageLane,
} from '@smoothbricks/arrow-builder';
// Re-export ColumnSchema and SchemaFields from arrow-builder for external use
export { ColumnSchema, isColumnSchema } from '@smoothbricks/arrow-builder';

// Re-export LogSchema for external use
export { isLogSchema, LogSchema } from './LogSchema.js';

import type {
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyEnumSchema,
  LazyNumberSchema,
  LazyTextSchema,
  SchemaFields,
} from '@smoothbricks/arrow-builder';
import type { DEFINED_LOG_SCHEMA_BRAND } from './defineLogSchema.js';
import type { LogSchema } from './LogSchema.js';

/**
 * Extract the original schema fields type from LogSchema or DefinedLogSchema.
 * If T is a LogSchema, extract the fields type.
 * If T has the brand marker, extract the original schema from it.
 * Otherwise, return T unchanged.
 */
type ExtractSchemaFields<T> =
  T extends LogSchema<infer Fields>
    ? Fields
    : T extends {
          readonly [DEFINED_LOG_SCHEMA_BRAND]?: infer Original;
        }
      ? Original extends SchemaFields
        ? Original
        : T extends SchemaFields
          ? T
          : never
      : T extends SchemaFields
        ? T
        : never;

/**
 * Filter out function keys from a schema type.
 * Methods like validate, parse, safeParse, extend are added by defineLogSchema
 * but should not be treated as schema fields for iteration.
 *
 * Works on LogSchema fields or plain schema objects.
 */
type SchemaFieldKeys<T> =
  T extends LogSchema<infer Fields>
    ? keyof Fields
    : ExtractSchemaFields<T> extends infer Fields
      ? keyof Fields extends infer K
        ? K extends string
          ? K
          : never
        : never
      : never;

/**
 * Extract TypeScript output types from log schema: each field's value type, with `undefined` for an optional one.
 *
 * Works with LogSchema instances or plain schema objects.
 * NOTE: Function properties (validate, parse, etc.) are filtered out.
 */
export type InferSchema<T extends LogSchema | SchemaFields> = {
  [K in SchemaFieldKeys<T>]: ExtractSchemaFields<T>[K] extends Schema<infer Out> ? Out : never;
};

/**
 * Extract TypeScript input types from log schema
 * Used for validation before transformation
 *
 * NOTE: Function properties (validate, parse, etc.) are filtered out.
 */
export type InferSchemaInput<T extends LogSchema | SchemaFields> = {
  [K in SchemaFieldKeys<T>]: ExtractSchemaFields<T>[K] extends Schema<unknown, infer In> ? In : never;
};

/**
 * Feature flag builder with default value support
 * Returned by S.number(), S.boolean(), S.enum(), S.category(), S.text() when used for feature flags
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
  schema: Schema<T>;
  defaultValue: T;
  evaluationType: EvalType;
}

/**
 * Schema that can be used for both tag attributes and feature flags
 */
export type SchemaOrFlagBuilder<T> = Schema<T> & FlagBuilder<T>;

/** Category field with the flag builder, `.mask()` and `.eager()`. */
export type CategorySchemaOrFlagBuilder = LazyCategorySchema & FlagBuilder<string>;

/** Text field with the flag builder, `.mask()` and `.eager()`. */
export type TextSchemaOrFlagBuilder = LazyTextSchema & FlagBuilder<string>;

/** A field holding any value as MessagePack in a binary column. */
export type MsgpackField<T = unknown> = LazyField<Field<T, 'lmao.msgpack', null>>;

/**
 * lmao's schema builder: arrow-builder's field builder (every shared kind), with the flag builder on the kinds a
 * feature flag can hold, and lmao's own MessagePack kind.
 *
 * String types - THREE DISTINCT TYPES per specs/lmao/01a_trace_schema_system.md.
 * IMPORTANT: Never use generic "string" - always choose enum/category/text
 */
export interface SchemaBuilder extends Omit<FieldBuilder, 'number' | 'boolean' | 'enum' | 'category' | 'text'> {
  number(): LazyNumberSchema & FlagBuilder<number>;
  boolean(): LazyBooleanSchema & FlagBuilder<boolean>;

  /**
   * Enum - Known values at compile time
   * Use for: Operations, HTTP methods, entry types, status enums
   * Example: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE'])
   */
  enum<const T extends readonly string[]>(values: T): LazyEnumSchema<T[number]> & FlagBuilder<T[number]>;

  /**
   * Category - Values that often repeat (limited cardinality)
   * Use for: userIds, sessionIds, moduleNames, spanNames, table names
   */
  category(): CategorySchemaOrFlagBuilder;

  /**
   * Text - Unique values that rarely repeat
   * Use for: Unique error messages, URLs, request bodies, masked queries
   */
  text(): TextSchemaOrFlagBuilder;

  /**
   * Unknown - accepts any value, msgpack-encoded at flush time
   * Use for: arbitrary objects, request bodies, error context, unknown-shape data
   */
  unknown(): MsgpackField;

  /**
   * Object<T> - typed variant of unknown, same storage and encoding
   * TypeScript enforces shape at compile time; no runtime validation on hot path
   */
  object<T extends object>(): MsgpackField<T>;
}
