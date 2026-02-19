/**
 * Define log schema with runtime validation and type inference
 *
 * This module leverages Sury for:
 * - Ultra-fast runtime validation (94,828 ops/ms)
 * - Automatic TypeScript inference
 * - Schema transformations (masking)
 * - Extension composition
 */

import * as S from '@sury/sury';
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
 * This wraps Sury's object schema to provide:
 * - Runtime validation via Sury
 * - Type inference from schema
 * - Extension capabilities
 * - Reserved name validation
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
 *
 * // Extend schema
 * const extended = schema.extend({
 *   duration: S.number()
 * });
 * ```
 *
 * @param schema - Object mapping field names to Sury schemas
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
 *
 * The brand marker allows InferSchema to detect when it receives a
 * DefinedLogSchema and extract the original schema type T for inference.
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

  // Convert to Sury object schema for validation using builder pattern
  // Use LogSchema.fieldEntries() directly
  const objectSchema = S.object((s) => {
    const output: Record<string, unknown> = {};
    for (const [key, surySchema] of logSchema._columns) {
      output[key] = s.field(key, surySchema);
    }
    return output as InferSchema<T>;
  });

  // Add validation methods directly to logSchema (preserves LogSchema instance)
  // Use Object.assign to mutate logSchema in place, preserving prototype chain
  const result = Object.assign(logSchema, {
    /**
     * Validate data and throw on error
     *
     * @throws Error if validation fails
     */
    validate: (data: unknown): InferSchema<T> => {
      return S.parseOrThrow(data, objectSchema);
    },

    /**
     * Validate data and return null on error
     *
     * @returns Validated data or null if invalid
     */
    parse: (data: unknown): InferSchema<T> | null => {
      const result = S.safe(() => S.parseOrThrow(data, objectSchema));
      return result.success ? result.value : null;
    },

    /**
     * Safe parse with detailed error information
     *
     * @returns Result object with success flag
     */
    safeParse: (data: unknown) => {
      const result = S.safe(() => S.parseOrThrow(data, objectSchema));
      if (result.success) {
        return { success: true as const, value: result.value };
      }
      // Convert Sury error to standard Error
      const error = result.error instanceof Error ? result.error : new Error(String(result.error));
      return { success: false as const, error };
    },
  });

  // Verify result is still a LogSchema instance (Object.assign returns the mutated target)
  if (!(result instanceof LogSchema)) {
    throw new Error('defineLogSchema: Object.assign broke LogSchema instance');
  }

  // Return logSchema (still a LogSchema instance with added methods)
  return result as ValidatedLogSchema<T>;
}
