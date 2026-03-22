/**
 * Schema builder for arrow-builder
 *
 * Provides the S object that creates schema objects with __schema_type metadata.
 * This enables arrow-builder to determine which TypedArray to create for each field.
 *
 * STRING TYPE SYSTEM:
 * - S.enum(['A', 'B', 'C']) - Known values at compile time (Uint8Array, 1 byte)
 * - S.category() - Repeated values (Uint32Array with string interning)
 * - S.text() - Unique values (no dictionary overhead)
 */

import { uint8, uint16, uint32 } from '@uwdata/flechette';
import { intern } from '../arrow/interner.js';
import { type Input, type Output, type Schema, optional as schemaOptional, union as schemaUnion } from './core.js';
import type {
  BinaryEncoder,
  EagerBigUint64Schema,
  EagerBinarySchema,
  EagerBooleanSchema,
  EagerCategorySchema,
  EagerEnumSchema,
  EagerNumberSchema,
  EagerTextSchema,
  EnumUtf8Precomputed,
  LazyBigUint64Schema,
  LazyBinarySchema,
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
 * Create a fresh schema object with an .eager() chain method.
 * Replaces Object.create(Sury.X) cloning — schema objects are now plain
 * objects with __schema_type metadata; no Sury prototype needed.
 */
function withEager<L extends { __schema_type: string }, E>(schema: L): L & { eager(): E } {
  (schema as L & { eager(): E }).eager = (): E => {
    return { ...schema, __eager: true as const } as unknown as E;
  };
  return schema as L & { eager(): E };
}

/**
 * Schema builder interface for arrow-builder
 *
 * Returns schema objects with __schema_type metadata attached.
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
   * Binary - Raw bytes or encoder-wrapped values
   * Storage: Array (object references, frozen at tag time)
   * Arrow: Binary column (encoded at flush time via BinaryEncoder if present)
   * Use for: Arbitrary payloads, serialized objects, msgpack blobs
   *
   * Without encoder: accepts Uint8Array (raw binary)
   * With encoder: accepts any value T (encoded to bytes at flush time)
   *
   * @returns Schema with chainable `.eager()` method
   */
  binary(): LazyBinarySchema<Uint8Array>;
  binary<T = unknown>(options: { encoder: BinaryEncoder }): LazyBinarySchema<T>;

  /**
   * Wrap schema to make it optional
   */
  optional<T>(schema: Schema<T>): Schema<T | undefined, T | undefined>;

  /**
   * Create union of multiple schemas
   */
  union<T extends readonly [Schema, ...Schema[]]>(schemas: T): Schema<Output<T[number]>, Input<T[number]>>;
}

/**
 * Schema builder implementation
 */
const schemaBuilderImpl: ArrowSchemaBuilder = {
  number: (): LazyNumberSchema => {
    return withEager<{ __schema_type: 'number' }, EagerNumberSchema>({
      __schema_type: 'number',
    }) as LazyNumberSchema;
  },

  bigUint64: (): LazyBigUint64Schema => {
    return withEager<{ __schema_type: 'bigUint64' }, EagerBigUint64Schema>({
      __schema_type: 'bigUint64',
    }) as LazyBigUint64Schema;
  },

  boolean: (): LazyBooleanSchema => {
    return withEager<{ __schema_type: 'boolean' }, EagerBooleanSchema>({
      __schema_type: 'boolean',
    }) as LazyBooleanSchema;
  },

  enum: <T extends readonly string[]>(values: T): LazyEnumSchema<T[number]> => {
    if (values.length === 0) {
      throw new Error('Enum must have at least one value');
    }

    // Determine index type based on enum size
    const uniqueCount = values.length;
    const indexArrayCtor = uniqueCount <= 255 ? Uint8Array : uniqueCount <= 65535 ? Uint16Array : Uint32Array;
    const arrowIndexType = uniqueCount <= 255 ? uint8() : uniqueCount <= 65535 ? uint16() : uint32();

    const schema = {
      __schema_type: 'enum' as const,
      __enum_values: values,
      __enum_utf8: precomputeEnumUtf8(values),
      __index_array_ctor: indexArrayCtor,
      __arrow_index_type: arrowIndexType,
    };

    return withEager<typeof schema, EagerEnumSchema<T[number]>>(schema) as LazyEnumSchema<T[number]>;
  },

  category: (): LazyCategorySchema => {
    const schema = { __schema_type: 'category' as const } as LazyCategorySchema;

    // Add chainable .mask() method
    schema.mask = (preset: MaskPreset | MaskTransform): LazyCategorySchema => {
      const maskedSchema = { ...schema, __mask_transform: resolveMaskTransform(preset) } as LazyCategorySchema;
      // Preserve mask/eager chainability
      maskedSchema.mask = schema.mask;
      maskedSchema.eager = (): EagerCategorySchema => {
        return { ...maskedSchema, __eager: true as const } as unknown as EagerCategorySchema;
      };
      return maskedSchema;
    };

    // Add chainable .eager() method
    schema.eager = (): EagerCategorySchema => {
      return { ...schema, __eager: true as const } as unknown as EagerCategorySchema;
    };

    return schema;
  },

  binary: (options?: { encoder: BinaryEncoder }): LazyBinarySchema => {
    const schema = { __schema_type: 'binary' as const } as LazyBinarySchema;

    if (options?.encoder) {
      (schema as LazyBinarySchema & { __binary_encoder: BinaryEncoder }).__binary_encoder = options.encoder;
    }

    schema.eager = (): EagerBinarySchema => {
      return { ...schema, __eager: true as const } as unknown as EagerBinarySchema;
    };

    return schema;
  },

  text: (): LazyTextSchema => {
    const schema = { __schema_type: 'text' as const } as LazyTextSchema;

    // Add chainable .mask() method
    schema.mask = (preset: MaskPreset | MaskTransform): LazyTextSchema => {
      const maskedSchema = { ...schema, __mask_transform: resolveMaskTransform(preset) } as LazyTextSchema;
      maskedSchema.mask = schema.mask;
      maskedSchema.eager = (): EagerTextSchema => {
        return { ...maskedSchema, __eager: true as const } as unknown as EagerTextSchema;
      };
      return maskedSchema;
    };

    // Add chainable .eager() method
    schema.eager = (): EagerTextSchema => {
      return { ...schema, __eager: true as const } as unknown as EagerTextSchema;
    };

    return schema;
  },

  optional: <T>(schema: Schema<T>): Schema<T | undefined, T | undefined> => {
    return schemaOptional(schema);
  },

  union: <T extends readonly [Schema, ...Schema[]]>(schemas: T): Schema<Output<T[number]>, Input<T[number]>> => {
    return schemaUnion(schemas);
  },
};

/**
 * Schema builder for arrow-builder
 *
 * Creates schema objects with __schema_type metadata for columnar storage.
 * Use this to define schemas that arrow-builder can convert to TypedArrays.
 */
export const S = schemaBuilderImpl;
