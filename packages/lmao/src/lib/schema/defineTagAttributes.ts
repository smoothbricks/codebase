/**
 * Define tag attributes with runtime validation and type inference
 *
 * This module leverages Sury for:
 * - Ultra-fast runtime validation (94,828 ops/ms)
 * - Automatic TypeScript inference
 * - Schema transformations (masking)
 * - Extension composition
 */

import * as S from '@sury/sury';
import type { ExtendedSchema } from './extend.js';
import { createExtendedSchema } from './extend.js';
import { getSchemaFields, type InferTagAttributes, type TagAttributeSchema } from './types.js';

/**
 * Brand symbol for DefinedTagAttributes - used to detect this type in conditionals.
 * Must be exported so types.ts can import it for ExtractOriginalSchema.
 */
export declare const DEFINED_TAG_ATTRIBUTES_BRAND: unique symbol;

/**
 * Reserved method names that cannot be used as attribute names
 * These conflict with the fluent API methods in SpanLogger
 */
export const RESERVED_NAMES = new Set([
  'with',
  'message',
  'tag',
  'info',
  'debug',
  'warn',
  'error',
  'ok',
  'err',
  'span',
]);

/**
 * Validate that attribute names don't conflict with reserved names
 * and don't start with underscore (reserved for system properties)
 *
 * @throws Error if any attribute name is reserved or starts with _
 */
export function validateAttributeNames(schema: TagAttributeSchema): void {
  for (const name of Object.keys(schema)) {
    if (name.startsWith('_')) {
      throw new Error(`Field name '${name}' cannot start with '_' - this prefix is reserved for system properties`);
    }
    if (RESERVED_NAMES.has(name)) {
      throw new Error(
        `Attribute name '${name}' is reserved and cannot be used. ` +
          `Reserved names: ${Array.from(RESERVED_NAMES).join(', ')}`,
      );
    }
  }
}

/**
 * Define tag attributes with runtime validation and type inference
 *
 * This wraps Sury's object schema to provide:
 * - Runtime validation via Sury
 * - Type inference from schema
 * - Extension capabilities
 * - Reserved name validation
 *
 * Example:
 * ```typescript
 * const attrs = defineTagAttributes({
 *   requestId: S.category(),
 *   userId: S.optional(S.category()),
 *   httpStatus: S.number(),
 *   operation: S.enum(['SELECT', 'INSERT', 'UPDATE'])
 * });
 *
 * // Validate data
 * const result = attrs.validate({
 *   requestId: 'req-123',
 *   userId: 'user-456',
 *   httpStatus: 200,
 *   operation: 'SELECT'
 * });
 *
 * // Extend schema
 * const extended = attrs.extend({
 *   duration: S.number()
 * });
 * ```
 *
 * @param schema - Object mapping field names to Sury schemas
 * @returns Extended schema with validation and extension methods
 */

/**
 * DefinedTagAttributes type - result of defineTagAttributes()
 *
 * Includes:
 * - All original schema fields (T)
 * - Extension methods (extend)
 * - Validation methods (validate, parse, safeParse)
 * - Index signature for TagAttributeSchema compatibility
 * - Brand marker for type detection in InferTagAttributes
 *
 * The brand marker allows InferTagAttributes to detect when it receives a
 * DefinedTagAttributes and extract the original schema type T for inference.
 */
export type DefinedTagAttributes<T extends TagAttributeSchema> = T &
  ExtendedSchema<T> & {
    validate: (data: unknown) => InferTagAttributes<T>;
    parse: (data: unknown) => InferTagAttributes<T> | null;
    safeParse: (data: unknown) => { success: true; value: InferTagAttributes<T> } | { success: false; error: Error };
    /** Brand marker - never actually set, only for type discrimination */
    readonly [DEFINED_TAG_ATTRIBUTES_BRAND]?: T;
  } & {
    // Index signature for assignability to TagAttributeSchema
    [key: string]: import('@sury/sury').Schema<unknown, unknown> | ((...args: unknown[]) => unknown);
  };

/**
 * Options for defineTagAttributes
 */
export interface DefineTagAttributesOptions {
  /**
   * Skip validation of reserved names.
   * Only used internally for defining system schema columns.
   * @internal
   */
  _skipReservedNameValidation?: boolean;
}

export function defineTagAttributes<T extends TagAttributeSchema>(
  schema: T,
  options?: DefineTagAttributesOptions,
): DefinedTagAttributes<T> {
  // Validate attribute names don't conflict with reserved names (unless skipped for system schema)
  if (!options?._skipReservedNameValidation) {
    validateAttributeNames(schema);
  }

  // Convert to Sury object schema for validation using builder pattern
  // Use getSchemaFields to filter out methods (validate, parse, etc.)
  const objectSchema = S.object((s) => {
    const output: Record<string, unknown> = {};
    for (const [key, surySchema] of getSchemaFields(schema)) {
      output[key] = s.field(key, surySchema);
    }
    return output as InferTagAttributes<T>;
  });

  // Create extendable schema with validation methods
  // Pass skip option to createExtendedSchema so it doesn't re-validate
  const extendedSchema = createExtendedSchema(schema, options);

  // Add validation methods - cast to any to bypass type checking since we know the structure is correct
  return Object.assign(extendedSchema, {
    /**
     * Validate data and throw on error
     *
     * @throws Error if validation fails
     */
    validate: (data: unknown): InferTagAttributes<T> => {
      return S.parseOrThrow(data, objectSchema);
    },

    /**
     * Validate data and return null on error
     *
     * @returns Validated data or null if invalid
     */
    parse: (data: unknown): InferTagAttributes<T> | null => {
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
  }) as DefinedTagAttributes<T>;
}
