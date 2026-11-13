/**
 * Define log schema with runtime validation and type inference
 *
 * A log schema is a record of field nodes. Validation is each field's own: its kind judges the value, so a kind
 * defined outside this package validates exactly like a shared one.
 */

import { validateField } from '@smoothbricks/arrow-builder';
import { LogSchema } from './LogSchema.js';
import type { Field, InferSchema, SchemaFields } from './types.js';

/**
 * Brand symbol for DefinedLogSchema - used to detect this type in conditionals.
 * Must be exported so types.ts can import it for ExtractOriginalSchema.
 */
export declare const DEFINED_LOG_SCHEMA_BRAND: unique symbol;

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
   * The library whose fields these are: the name a field's identity is written under. A schema that only traces
   * needs none.
   */
  library?: string;
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
 * Validate an object against a set of schema columns.
 *
 * @throws Error naming the field's path, what its kind expected and what was observed.
 */
function validateObject<T extends SchemaFields>(
  data: unknown,
  columns: Iterable<readonly [string, Field]>,
): InferSchema<T> {
  if (!isUnknownRecord(data)) {
    throw new Error(`Expected object, got ${data === null ? 'null' : typeof data}`);
  }
  const result: Record<string, unknown> = {};
  for (const [key, field] of columns) {
    const judged = validateField(key, field, data[key]);
    if (!judged.ok) {
      const { path, expected, observed } = judged.error;
      throw new Error(`Field "${path}": expected ${expected}, got ${observed}`);
    }
    result[key] = judged.value;
  }
  assertValidatedObject<T>(result, columns);
  return result;
}

function isUnknownRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function assertValidatedObject<T extends SchemaFields>(
  value: Record<string, unknown>,
  columns: Iterable<readonly [string, Field]>,
): asserts value is InferSchema<T> {
  for (const [key] of columns) {
    if (!(key in value)) {
      throw new Error(`Missing validated field "${key}"`);
    }
  }
}

//#region smoo/lmao!n/schema-log-schema
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
 * @param schema - Object mapping field names to field nodes
 * @returns Extended schema with validation and extension methods
 */
export function defineLogSchema<T extends SchemaFields>(
  schema: T,
  options?: DefineLogSchemaOptions,
): ValidatedLogSchema<T> {
  // Wrap user input into LogSchema first
  const logSchema = schema instanceof LogSchema ? schema : new LogSchema(schema, options?.library);

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
//#endregion smoo/lmao!n/schema-log-schema
