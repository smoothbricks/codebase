/**
 * Schema builder for arrow-builder
 *
 * Provides the S object that creates Sury schemas with __schema_type metadata.
 * This enables arrow-builder to determine which TypedArray to create for each field.
 *
 * STRING TYPE SYSTEM:
 * - S.enum(['A', 'B', 'C']) - Known values at compile time (Uint8Array, 1 byte)
 * - S.category() - Repeated values (Uint32Array with string interning)
 * - S.text() - Unique values (no dictionary overhead)
 */

import * as Sury from '@sury/sury';
import { uint8, uint16, uint32 } from '@uwdata/flechette';
import { intern } from '../arrow/interner.js';
import type {
  EagerBigUint64Schema,
  EagerBooleanSchema,
  EagerCategorySchema,
  EagerEnumSchema,
  EagerNumberSchema,
  EagerTextSchema,
  EnumUtf8Precomputed,
  LazyBigUint64Schema,
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyEnumSchema,
  LazyNumberSchema,
  LazyTextSchema,
  MaskPreset,
  MaskTransform,
} from './types.js';

/**
 * Masking preset implementations
 * Applied during Arrow table serialization (cold path, NOT hot path)
 */
export const maskingTransforms: Record<MaskPreset, MaskTransform> = {
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
 * Resolve a mask preset name or custom function to a MaskTransform function
 */
function resolveMaskTransform(preset: MaskPreset | MaskTransform): MaskTransform {
  return typeof preset === 'function' ? preset : maskingTransforms[preset];
}

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
  // Use intern() to ensure all enum strings are globally interned for UTF-8 reuse
  const bytes = values.map((v) => intern(v));

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
 * Schema builder interface for arrow-builder
 *
 * Returns plain Sury schemas with __schema_type metadata attached.
 * Consumer packages (like lmao) can extend these schemas with additional functionality.
 */
export interface ArrowSchemaBuilder {
  /**
   * Create number schema
   * Storage: Float64Array
   * @returns Schema with chainable `.eager()` method
   */
  number(): LazyNumberSchema;

  /**
   * Create bigUint64 schema
   * Storage: BigUint64Array
   * @returns Schema with chainable `.eager()` method
   */
  bigUint64(): LazyBigUint64Schema;

  /**
   * Create boolean schema
   * Storage: Uint8Array (0/1)
   * @returns Schema with chainable `.eager()` method
   */
  boolean(): LazyBooleanSchema;

  /**
   * Enum - Known values at compile time
   * Storage: Uint8Array (1 byte) with compile-time mapping
   * Arrow: Dictionary with pre-defined values
   * Use for: Operations, HTTP methods, entry types, status enums
   * @returns Schema with chainable `.eager()` method
   */
  enum<T extends readonly string[]>(values: T): LazyEnumSchema<T[number]>;

  /**
   * Category - Values that often repeat (limited cardinality)
   * Storage: Uint32Array indices with string interning
   * Arrow: Dictionary built dynamically from interned strings
   * Use for: userIds, sessionIds, moduleNames, spanNames
   *
   * Returns a schema with a chainable .mask() method for applying
   * masking transforms during Arrow conversion (cold path).
   */
  category(): LazyCategorySchema;

  /**
   * Text - Unique values that rarely repeat
   * Storage: Raw strings without interning
   * Arrow: Plain string column (no dictionary overhead)
   * Use for: Error messages, URLs, request bodies
   *
   * Returns a schema with a chainable .mask() method for applying
   * masking transforms during Arrow conversion (cold path).
   */
  text(): LazyTextSchema;

  /**
   * Wrap schema to make it optional
   */
  optional<T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined>;

  /**
   * Create union of multiple schemas
   */
  union<T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T,
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>>;
}

/**
 * Schema builder implementation
 */
const schemaBuilderImpl: ArrowSchemaBuilder = {
  number: (): LazyNumberSchema => {
    // Clone to avoid mutating the shared Sury.number singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.number),
      Object.getOwnPropertyDescriptors(Sury.number),
    ) as LazyNumberSchema;
    schema.__schema_type = 'number';

    // Add chainable .eager() method
    schema.eager = (): EagerNumberSchema => {
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as EagerNumberSchema;
      (eagerSchema as { __eager: true }).__eager = true;
      return eagerSchema;
    };

    return schema;
  },

  bigUint64: (): LazyBigUint64Schema => {
    // Clone to avoid mutating the shared Sury.bigint singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.bigint),
      Object.getOwnPropertyDescriptors(Sury.bigint),
    ) as LazyBigUint64Schema;
    schema.__schema_type = 'bigUint64';

    // Add chainable .eager() method
    schema.eager = (): EagerBigUint64Schema => {
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as EagerBigUint64Schema;
      (eagerSchema as { __eager: true }).__eager = true;
      return eagerSchema;
    };

    return schema;
  },

  boolean: (): LazyBooleanSchema => {
    // Clone to avoid mutating the shared Sury.boolean singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.boolean),
      Object.getOwnPropertyDescriptors(Sury.boolean),
    ) as LazyBooleanSchema;
    schema.__schema_type = 'boolean';

    // Add chainable .eager() method
    schema.eager = (): EagerBooleanSchema => {
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as EagerBooleanSchema;
      (eagerSchema as { __eager: true }).__eager = true;
      return eagerSchema;
    };

    return schema;
  },

  enum: <T extends readonly string[]>(values: T): LazyEnumSchema<T[number]> => {
    if (values.length === 0) {
      throw new Error('Enum must have at least one value');
    }

    // Determine index type based on enum size
    const uniqueCount = values.length;
    const indexArrayCtor = uniqueCount <= 255 ? Uint8Array : uniqueCount <= 65535 ? Uint16Array : Uint32Array;
    const arrowIndexType = uniqueCount <= 255 ? uint8() : uniqueCount <= 65535 ? uint16() : uint32();

    // Use refine to validate string is one of the allowed values
    const schema = Sury.refine(Sury.string, (value, fail): T[number] => {
      if (!values.includes(value)) {
        fail.fail(`Value must be one of: ${values.join(', ')}`);
      }
      return value as T[number];
    }) as LazyEnumSchema<T[number]>;

    // Attach enum metadata
    schema.__schema_type = 'enum';
    schema.__enum_values = values;
    schema.__enum_utf8 = precomputeEnumUtf8(values);
    schema.__index_array_ctor = indexArrayCtor;
    schema.__arrow_index_type = arrowIndexType;

    // Add chainable .eager() method
    schema.eager = (): EagerEnumSchema<T[number]> => {
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as EagerEnumSchema<T[number]>;
      (eagerSchema as { __eager: true }).__eager = true;
      return eagerSchema;
    };

    return schema;
  },

  category: (): LazyCategorySchema => {
    // Clone to avoid mutating the shared Sury.string singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.string),
      Object.getOwnPropertyDescriptors(Sury.string),
    ) as LazyCategorySchema;
    schema.__schema_type = 'category';

    // Add chainable .mask() method
    schema.mask = (preset: MaskPreset | MaskTransform): LazyCategorySchema => {
      // Clone to create a new schema with the mask transform
      const maskedSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as LazyCategorySchema;
      maskedSchema.__mask_transform = resolveMaskTransform(preset);
      return maskedSchema;
    };

    // Add chainable .eager() method
    schema.eager = (): EagerCategorySchema => {
      // Clone to create a new schema marked as eager
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as EagerCategorySchema;
      (eagerSchema as { __eager: true }).__eager = true;
      return eagerSchema;
    };

    return schema;
  },

  text: (): LazyTextSchema => {
    // Clone to avoid mutating the shared Sury.string singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.string),
      Object.getOwnPropertyDescriptors(Sury.string),
    ) as LazyTextSchema;
    schema.__schema_type = 'text';

    // Add chainable .mask() method
    schema.mask = (preset: MaskPreset | MaskTransform): LazyTextSchema => {
      // Clone to create a new schema with the mask transform
      const maskedSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as LazyTextSchema;
      maskedSchema.__mask_transform = resolveMaskTransform(preset);
      return maskedSchema;
    };

    // Add chainable .eager() method
    schema.eager = (): EagerTextSchema => {
      // Clone to create a new schema marked as eager
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as EagerTextSchema;
      (eagerSchema as { __eager: true }).__eager = true;
      return eagerSchema;
    };

    return schema;
  },

  optional: <T>(schema: Sury.Schema<T, unknown>): Sury.Schema<T | undefined, T | undefined> => {
    return Sury.optional(schema) as Sury.Schema<T | undefined, T | undefined>;
  },

  union: <T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T,
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>> => {
    const schemaArray = [...schemas] as [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]];
    return Sury.union(schemaArray) as Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>>;
  },
};

/**
 * Schema builder for arrow-builder
 *
 * Creates Sury schemas with __schema_type metadata for columnar storage.
 * Use this to define schemas that arrow-builder can convert to TypedArrays.
 */
export const S = schemaBuilderImpl;
