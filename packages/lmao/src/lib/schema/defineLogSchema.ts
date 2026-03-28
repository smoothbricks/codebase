/**
 * Define log schema with runtime validation and type inference
 *
 * This module leverages __schema_type metadata on schema objects for:
 * - Runtime validation via schema-type dispatch
 * - Automatic TypeScript inference
 * - Schema transformations (masking)
 * - Extension composition
 */

import type { SchemaWithMetadata } from '@smoothbricks/arrow-builder';
import { LogSchema } from './LogSchema.js';
import type { InferSchema, SchemaFields } from './types.js';

/**
 * Brand symbol for DefinedLogSchema - used to detect this type in conditionals.
 * Must be exported so types.ts can import it for ExtractOriginalSchema.
 */
export declare const DEFINED_LOG_SCHEMA_BRAND: unique symbol;

/**
 * Define log schema with runtime validation and type inference
 *
 * Example:
 * ```typescript
 * const schema = defineLogSchema({
 *   requestId: S.category(),
 *   userId: S.optional(S.category()),
 *   httpStatus: S.number(),
 *   operation: S.enum(['SELECT', 'INSERT', 'UPDATE'])
 * });
 *
 * // Validate data
 * const result = schema.validate({
 *   requestId: 'req-123',
 *   userId: 'user-456',
 *   httpStatus: 200,
 *   operation: 'SELECT'
 * });
 * ```
 *
 * @param schema - Object mapping field names to schema objects
 * @returns Extended schema with validation and extension methods
 */

/**
 * DefinedLogSchema type - result of defineLogSchema()
 *
 * Includes:
 * - All original schema fields (T)
 * - Extension methods (extend)
 * - Validation methods (validate, parse, safeParse)
 * - LogSchema methods (fieldEntries, fieldCount, fieldNames, fields)
 * - Brand marker for type detection in InferSchema
 */
export type DefinedLogSchema<T extends SchemaFields> = LogSchema<T> & {
  /** Brand marker - never actually set, only for type discrimination */
  readonly [DEFINED_LOG_SCHEMA_BRAND]?: T;
};

type SafeParseResult<T extends SchemaFields> =
  | { success: true; value: InferSchema<T> }
  | { success: false; error: Error };

/**
 * Options for defineLogSchema
 */
export interface DefineLogSchemaOptions {
  /**
   * Skip validation of reserved names.
   * Only used internally for defining system schema columns.
   * @internal
   */
  _skipReservedNameValidation?: boolean;
}

/**
 * LogSchema with validation methods added by defineLogSchema.
 * These methods are added via Object.assign at runtime.
 */
export type ValidatedLogSchema<T extends SchemaFields> = LogSchema<T> & {
  /** Validate data and throw on error */
  validate(data: unknown): InferSchema<T>;
  /** Validate data and return null on error */
  parse(data: unknown): InferSchema<T> | null;
  /** Safe parse with detailed error information */
  safeParse(data: unknown): SafeParseResult<T>;
};

/**
 * Extended schema shape covering optional, refine, union, and key wrappers.
 * Avoids repetitive inline casts throughout validateField.
 *
 * define additional schema types (e.g. 'key') beyond the columnar SchemaType union.
 */
interface ExtendedSchema extends Omit<SchemaWithMetadata, '__schema_type'> {
  __schema_type?: string;
  __optional?: boolean;
  __inner?: SchemaWithMetadata;
  __refiner?: (value: unknown, fail: { fail(msg: string): never }) => unknown;
  __union?: SchemaWithMetadata[];
  __key_name?: string;
  __key_validator?: (value: string) => string;
}

/**
 * Validate a single field value against its schema metadata.
 * Uses __schema_type for type dispatch — no Sury dependency.
 *
 * Handles all schema compositions: optional, refine, union, key.
 * Throws on unknown __schema_type to prevent silent acceptance of invalid data.
 */
function validateField(key: string, value: unknown, schema: SchemaWithMetadata): unknown {
  const s = schema as ExtendedSchema;

  // Handle optional schemas (created via S.optional())
  if (s.__optional) {
    if (value === undefined || value === null) return undefined;
    // Value is present — recursively validate against the inner schema
    if (s.__inner) {
      return validateField(key, value, s.__inner);
    }
    // Optional with no inner schema — accept the value as-is
    // (shouldn't happen with well-formed schemas, but defensive)
    return value;
  }

  if (value === undefined || value === null) {
    throw new Error(`Field "${key}": expected a value, got ${value === null ? 'null' : 'undefined'}`);
  }

  // Handle refine() schemas — invoke the stored refiner function
  if (s.__refiner) {
    try {
      return s.__refiner(value, {
        fail(msg: string): never {
          throw new Error(`Field "${key}": refinement failed — ${msg}`);
        },
      });
    } catch (err) {
      if (err instanceof Error && err.message.startsWith(`Field "${key}":`)) throw err;
      throw new Error(`Field "${key}": refinement failed — ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // Handle union() schemas — try each member, accept if any matches
  if (s.__union) {
    const errors: string[] = [];
    for (const member of s.__union) {
      try {
        return validateField(key, value, member);
      } catch (err) {
        errors.push(err instanceof Error ? err.message : String(err));
      }
    }
    throw new Error(
      `Field "${key}": value does not match any union member. Tried ${s.__union.length}: ${errors.join('; ')}`,
    );
  }

  switch (s.__schema_type) {
    case 'number':
      if (typeof value !== 'number') throw new Error(`Field "${key}": expected number, got ${typeof value}`);
      return value;
    case 'boolean':
      if (typeof value !== 'boolean') throw new Error(`Field "${key}": expected boolean, got ${typeof value}`);
      return value;
    case 'text':
    case 'category':
      if (typeof value !== 'string') throw new Error(`Field "${key}": expected string, got ${typeof value}`);
      return value;
    case 'enum':
      if (typeof value !== 'string') throw new Error(`Field "${key}": expected string, got ${typeof value}`);
      if (s.__enum_values && !s.__enum_values.includes(value)) {
        throw new Error(`Field "${key}": expected one of [${s.__enum_values.join(', ')}], got "${value}"`);
      }
      return value;
    case 'bigUint64':
      if (typeof value !== 'bigint') throw new Error(`Field "${key}": expected bigint, got ${typeof value}`);
      return value;
    case 'binary':
      // Binary accepts any value (raw bytes or encoder-wrapped)
      return value;
    case 'key':
      // Key schema — validate the runtime value using the stored validator
      if (typeof value !== 'string') throw new Error(`Field "${key}": expected string key value, got ${typeof value}`);
      if (s.__key_validator) {
        return s.__key_validator(value);
      }
      return value;
    default:
      throw new Error(
        `Field "${key}": unknown schema type "${s.__schema_type ?? '<none>'}". ` +
          'All schema types must be explicitly handled in validateField.',
      );
  }
}

/**
 * Validate an object against a set of schema columns.
 */
function validateObject<T extends SchemaFields>(
  data: unknown,
  columns: Iterable<readonly [string, SchemaWithMetadata]>,
): InferSchema<T> {
  if (!isUnknownRecord(data)) {
    throw new Error(`Expected object, got ${data === null ? 'null' : typeof data}`);
  }
  const result: Record<string, unknown> = {};
  for (const [key, schema] of columns) {
    result[key] = validateField(key, data[key], schema);
  }
  assertValidatedObject<T>(result, columns);
  return result;
}

function isUnknownRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function assertValidatedObject<T extends SchemaFields>(
  value: Record<string, unknown>,
  columns: Iterable<readonly [string, SchemaWithMetadata]>,
): asserts value is InferSchema<T> {
  for (const [key] of columns) {
    if (!(key in value)) {
      throw new Error(`Missing validated field "${key}"`);
    }
  }
}

export function defineLogSchema<T extends SchemaFields>(
  schema: T,
  options?: DefineLogSchemaOptions,
): ValidatedLogSchema<T> {
  // Wrap user input into LogSchema first
  const logSchema = schema instanceof LogSchema ? schema : new LogSchema(schema);

  // Assert attribute names don't use reserved names
  if (!options?._skipReservedNameValidation) {
    LogSchema.assertUserFieldNames(logSchema._columnNames);
  }

  // Add validation methods directly to logSchema (preserves LogSchema instance)
  // Use Object.assign to mutate logSchema in place, preserving prototype chain
  const result: ValidatedLogSchema<T> = Object.assign(logSchema, {
    /**
     * Validate data and throw on error
     *
     * @throws Error if validation fails
     */
    validate: (data: unknown): InferSchema<T> => {
      return validateObject<T>(data, logSchema._columns);
    },

    /**
     * Validate data and return null on error
     *
     * @returns Validated data or null if invalid
     */
    parse: (data: unknown): InferSchema<T> | null => {
      try {
        return validateObject<T>(data, logSchema._columns);
      } catch {
        return null;
      }
    },

    /**
     * Safe parse with detailed error information
     *
     * @returns Result object with success flag
     */
    safeParse: (data: unknown) => {
      try {
        return { success: true as const, value: validateObject<T>(data, logSchema._columns) };
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        return { success: false as const, error };
      }
    },
  });

  // Verify result is still a LogSchema instance (Object.assign returns the mutated target)
  if (!(result instanceof LogSchema)) {
    throw new Error('defineLogSchema: Object.assign broke LogSchema instance');
  }

  // Return logSchema (still a LogSchema instance with added methods)
  return result;
}
