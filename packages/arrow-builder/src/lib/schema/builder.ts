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

function createLazyNumberSchema(): LazyNumberSchema {
  return {
    __schema_type: 'number',
    eager(): EagerNumberSchema {
      return { __schema_type: 'number', __eager: true };
    },
  };
}

function createLazyBigUint64Schema(): LazyBigUint64Schema {
  return {
    __schema_type: 'bigUint64',
    eager(): EagerBigUint64Schema {
      return { __schema_type: 'bigUint64', __eager: true };
    },
  };
}

function createLazyBooleanSchema(): LazyBooleanSchema {
  return {
    __schema_type: 'boolean',
    eager(): EagerBooleanSchema {
      return { __schema_type: 'boolean', __eager: true };
    },
  };
}

function createLazyEnumSchema<T extends string>(
  values: readonly T[],
  enumUtf8: EnumUtf8Precomputed,
  indexArrayCtor: Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor,
  arrowIndexType: ReturnType<typeof uint8> | ReturnType<typeof uint16> | ReturnType<typeof uint32>,
): LazyEnumSchema<T> {
  return {
    __schema_type: 'enum',
    __enum_values: values,
    __enum_utf8: enumUtf8,
    __index_array_ctor: indexArrayCtor,
    __arrow_index_type: arrowIndexType,
    eager(): EagerEnumSchema<T> {
      return {
        __schema_type: 'enum',
        __enum_values: values,
        __enum_utf8: enumUtf8,
        __index_array_ctor: indexArrayCtor,
        __arrow_index_type: arrowIndexType,
        __eager: true,
      };
    },
  };
}

function createLazyCategorySchema(maskTransform?: MaskTransform): LazyCategorySchema {
  return {
    __schema_type: 'category',
    __mask_transform: maskTransform,
    mask(preset: MaskPreset | MaskTransform): LazyCategorySchema {
      return createLazyCategorySchema(resolveMaskTransform(preset));
    },
    eager(): EagerCategorySchema {
      return {
        __schema_type: 'category',
        __mask_transform: maskTransform,
        __eager: true,
      };
    },
  };
}

function createLazyBinarySchema<T = Uint8Array>(encoder?: BinaryEncoder): LazyBinarySchema<T> {
  return {
    __schema_type: 'binary',
    __binary_encoder: encoder,
    eager(): EagerBinarySchema<T> {
      return {
        __schema_type: 'binary',
        __binary_encoder: encoder,
        __eager: true,
      };
    },
  };
}

function createLazyTextSchema(maskTransform?: MaskTransform): LazyTextSchema {
  return {
    __schema_type: 'text',
    __mask_transform: maskTransform,
    mask(preset: MaskPreset | MaskTransform): LazyTextSchema {
      return createLazyTextSchema(resolveMaskTransform(preset));
    },
    eager(): EagerTextSchema {
      return {
        __schema_type: 'text',
        __mask_transform: maskTransform,
        __eager: true,
      };
    },
  };
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
    return createLazyNumberSchema();
  },

  bigUint64: (): LazyBigUint64Schema => {
    return createLazyBigUint64Schema();
  },

  boolean: (): LazyBooleanSchema => {
    return createLazyBooleanSchema();
  },

  enum: <T extends readonly string[]>(values: T): LazyEnumSchema<T[number]> => {
    if (values.length === 0) {
      throw new Error('Enum must have at least one value');
    }

    // Determine index type based on enum size
    const uniqueCount = values.length;
    const indexArrayCtor = uniqueCount <= 255 ? Uint8Array : uniqueCount <= 65535 ? Uint16Array : Uint32Array;
    const arrowIndexType = uniqueCount <= 255 ? uint8() : uniqueCount <= 65535 ? uint16() : uint32();

    return createLazyEnumSchema(values, precomputeEnumUtf8(values), indexArrayCtor, arrowIndexType);
  },

  category: (): LazyCategorySchema => {
    return createLazyCategorySchema();
  },

  binary: (options?: { encoder: BinaryEncoder }): LazyBinarySchema => {
    return createLazyBinarySchema(options?.encoder);
  },

  text: (): LazyTextSchema => {
    return createLazyTextSchema();
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
