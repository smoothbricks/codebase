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
import type {
  BooleanSchemaWithMetadata,
  CategorySchemaWithMask,
  CategorySchemaWithMetadata,
  EnumSchemaWithMetadata,
  EnumUtf8Precomputed,
  MaskPreset,
  MaskTransform,
  NumberSchemaWithMetadata,
  TextSchemaWithMask,
  TextSchemaWithMetadata,
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
 * Schema builder interface for arrow-builder
 *
 * Returns plain Sury schemas with __schema_type metadata attached.
 * Consumer packages (like lmao) can extend these schemas with additional functionality.
 */
export interface ArrowSchemaBuilder {
  /**
   * Create number schema
   * Storage: Float64Array
   */
  number(): Sury.Schema<number, unknown> & NumberSchemaWithMetadata;

  /**
   * Create boolean schema
   * Storage: Uint8Array (0/1)
   */
  boolean(): Sury.Schema<boolean, unknown> & BooleanSchemaWithMetadata;

  /**
   * Enum - Known values at compile time
   * Storage: Uint8Array (1 byte) with compile-time mapping
   * Arrow: Dictionary with pre-defined values
   * Use for: Operations, HTTP methods, entry types, status enums
   */
  enum<T extends readonly string[]>(values: T): Sury.Schema<T[number], string> & EnumSchemaWithMetadata<T[number]>;

  /**
   * Category - Values that often repeat (limited cardinality)
   * Storage: Uint32Array indices with string interning
   * Arrow: Dictionary built dynamically from interned strings
   * Use for: userIds, sessionIds, moduleNames, spanNames
   *
   * Returns a schema with a chainable .mask() method for applying
   * masking transforms during Arrow conversion (cold path).
   */
  category(): Sury.Schema<string, unknown> & CategorySchemaWithMask;

  /**
   * Text - Unique values that rarely repeat
   * Storage: Raw strings without interning
   * Arrow: Plain string column (no dictionary overhead)
   * Use for: Error messages, URLs, request bodies
   *
   * Returns a schema with a chainable .mask() method for applying
   * masking transforms during Arrow conversion (cold path).
   */
  text(): Sury.Schema<string, unknown> & TextSchemaWithMask;

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
  number: () => {
    // Clone to avoid mutating the shared Sury.number singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.number),
      Object.getOwnPropertyDescriptors(Sury.number),
    ) as Sury.Schema<number, unknown> & NumberSchemaWithMetadata;
    schema.__schema_type = 'number';
    return schema;
  },

  boolean: () => {
    // Clone to avoid mutating the shared Sury.boolean singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.boolean),
      Object.getOwnPropertyDescriptors(Sury.boolean),
    ) as Sury.Schema<boolean, unknown> & BooleanSchemaWithMetadata;
    schema.__schema_type = 'boolean';
    return schema;
  },

  enum: <T extends readonly string[]>(
    values: T,
  ): Sury.Schema<T[number], string> & EnumSchemaWithMetadata<T[number]> => {
    if (values.length === 0) {
      throw new Error('Enum must have at least one value');
    }
    if (values.length > 256) {
      throw new Error('Enum can have at most 256 values (Uint8Array limit: indices 0-255)');
    }

    // Use refine to validate string is one of the allowed values
    const schema = Sury.refine(Sury.string, (value, fail): T[number] => {
      if (!values.includes(value)) {
        fail.fail(`Value must be one of: ${values.join(', ')}`);
      }
      return value as T[number];
    }) as Sury.Schema<T[number], string> & EnumSchemaWithMetadata<T[number]>;

    // Attach enum metadata
    schema.__schema_type = 'enum';
    schema.__enum_values = values;
    schema.__enum_utf8 = precomputeEnumUtf8(values);

    return schema;
  },

  category: () => {
    // Clone to avoid mutating the shared Sury.string singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.string),
      Object.getOwnPropertyDescriptors(Sury.string),
    ) as Sury.Schema<string, unknown> & CategorySchemaWithMask;
    schema.__schema_type = 'category';

    // Add chainable .mask() method
    schema.mask = (preset: MaskPreset | MaskTransform): CategorySchemaWithMetadata => {
      // Clone to create a new schema with the mask transform
      const maskedSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as Sury.Schema<string, unknown> & CategorySchemaWithMetadata;
      maskedSchema.__mask_transform = resolveMaskTransform(preset);
      return maskedSchema;
    };

    // Add chainable .eager() method
    schema.eager = (): CategorySchemaWithMetadata => {
      // Clone to create a new schema marked as eager
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as Sury.Schema<string, unknown> & CategorySchemaWithMetadata;
      eagerSchema.__eager = true;
      return eagerSchema;
    };

    return schema;
  },

  text: () => {
    // Clone to avoid mutating the shared Sury.string singleton
    const schema = Object.create(
      Object.getPrototypeOf(Sury.string),
      Object.getOwnPropertyDescriptors(Sury.string),
    ) as Sury.Schema<string, unknown> & TextSchemaWithMask;
    schema.__schema_type = 'text';

    // Add chainable .mask() method
    schema.mask = (preset: MaskPreset | MaskTransform): TextSchemaWithMetadata => {
      // Clone to create a new schema with the mask transform
      const maskedSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as Sury.Schema<string, unknown> & TextSchemaWithMetadata;
      maskedSchema.__mask_transform = resolveMaskTransform(preset);
      return maskedSchema;
    };

    // Add chainable .eager() method
    schema.eager = (): TextSchemaWithMetadata => {
      // Clone to create a new schema marked as eager
      const eagerSchema = Object.create(
        Object.getPrototypeOf(schema),
        Object.getOwnPropertyDescriptors(schema),
      ) as Sury.Schema<string, unknown> & TextSchemaWithMetadata;
      eagerSchema.__eager = true;
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
