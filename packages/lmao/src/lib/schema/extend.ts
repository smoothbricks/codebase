/**
 * Schema extension utilities for composing tag attribute schemas
 *
 * This enables building complex schemas from smaller, reusable pieces:
 * - Base attributes (requestId, userId, etc.)
 * - HTTP-specific attributes (httpStatus, httpMethod, etc.)
 * - Database-specific attributes (dbQuery, dbDuration, etc.)
 * - Custom domain attributes
 */

import { validateAttributeNames } from './defineTagAttributes.js';
import type { TagAttributeSchema } from './types.js';

/**
 * Merge two tag attribute schemas with conflict detection
 *
 * This performs shallow merge and validates:
 * - No conflicting field names
 * - No reserved names in extension
 *
 * Example:
 * ```typescript
 * const base = { requestId: S.string() };
 * const httpAttrs = { httpStatus: S.number(), httpMethod: S.string() };
 * const combined = extendSchema(base, httpAttrs);
 * // Result: { requestId, httpStatus, httpMethod }
 * ```
 *
 * @param base - Base tag attribute schema
 * @param extension - Additional attributes to add
 * @returns Merged schema
 * @throws Error if field names conflict
 */
export function extendSchema<T extends TagAttributeSchema, U extends TagAttributeSchema>(base: T, extension: U): T & U {
  // Check for field name conflicts
  const baseKeys = new Set(Object.keys(base));
  for (const key of Object.keys(extension)) {
    if (baseKeys.has(key)) {
      throw new Error(
        `Schema conflict: attribute '${key}' already exists in base schema. ` +
          `Base schema has: ${Object.keys(base).join(', ')}`,
      );
    }
  }

  // Validate extension doesn't use reserved names
  validateAttributeNames(extension);

  // Perform shallow merge
  return { ...base, ...extension };
}

/**
 * Extended schema interface with fluent .extend() method
 *
 * This allows chaining multiple extensions:
 * ```typescript
 * const schema = createExtendableSchema(baseAttrs)
 *   .extend(httpAttrs)
 *   .extend(dbAttrs)
 *   .extend(customAttrs);
 * ```
 */
export interface ExtendedSchema<T extends TagAttributeSchema> {
  /**
   * Extend this schema with additional attributes
   *
   * @param extension - Additional attributes to add
   * @returns New extended schema with chaining support
   */
  extend<U extends TagAttributeSchema>(extension: U): ExtendedSchema<T & U>;
}

/**
 * Type helper to extract the raw schema from ExtendedSchema
 * The ExtendedSchema spreads the schema fields, so we can use it directly as TagAttributeSchema
 */
export type UnwrapExtendedSchema<T> = T extends ExtendedSchema<infer S> ? S : T extends TagAttributeSchema ? T : never;

/**
 * Type helper to convert any object to TagAttributeSchema by picking only schema properties
 * This strips out methods like 'extend', 'validate', etc.
 */
export type AsTagAttributeSchema<T> = {
  [K in keyof T as T[K] extends Function ? never : K]: T[K];
};

/**
 * Create an extendable schema wrapper
 *
 * This provides a fluent API for schema composition while maintaining
 * the underlying Sury schema objects.
 *
 * Example:
 * ```typescript
 * const base = createExtendableSchema({
 *   requestId: S.string(),
 *   userId: S.optional(S.masked('hash'))
 * });
 *
 * const withHttp = base.extend({
 *   httpStatus: S.number(),
 *   httpMethod: S.enum(['GET', 'POST', 'PUT', 'DELETE'])
 * });
 *
 * const withDb = withHttp.extend({
 *   dbQuery: S.masked('sql'),
 *   dbDuration: S.number()
 * });
 * ```
 *
 * @param schema - Tag attribute schema to make extendable
 * @returns Schema with .extend() method for chaining
 */
export function createExtendedSchema<T extends TagAttributeSchema>(schema: T): ExtendedSchema<T> {
  // Validate initial schema doesn't use reserved names
  validateAttributeNames(schema);

  // Create proxy object that contains schema fields and .extend() method
  // This is safe because we're adding a method to a schema object
  const proxy = { ...schema } as T & ExtendedSchema<T>;

  // Attach .extend() method that returns new extended schema
  proxy.extend = <U extends TagAttributeSchema>(extension: U): ExtendedSchema<T & U> => {
    const merged = extendSchema(schema, extension);
    // Recursively create extended schema from merged result
    return createExtendedSchema(merged);
  };

  return proxy;
}
