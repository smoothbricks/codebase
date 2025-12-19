/**
 * Schema extension utilities for composing tag attribute schemas
 *
 * This enables building complex schemas from smaller, reusable pieces:
 * - Base attributes (requestId, userId, etc.)
 * - HTTP-specific attributes (httpStatus, httpMethod, etc.)
 * - Database-specific attributes (dbQuery, dbDuration, etc.)
 * - Custom domain attributes
 */

import { LogSchema } from './LogSchema.js';
import type { Schema, SchemaFields } from './types.js';

/**
 * Merge two log schemas with conflict detection
 *
 * This performs shallow merge and validates:
 * - No conflicting field names
 * - No reserved names in extension
 *
 * Example:
 * ```typescript
 * const base = { requestId: S.category() };
 * const httpAttrs = { httpStatus: S.number(), httpMethod: S.enum(['GET', 'POST']) };
 * const combined = extendSchema(base, httpAttrs);
 * // Result: { requestId, httpStatus, httpMethod }
 * ```
 *
 * @param base - Base log schema (LogSchema or plain object)
 * @param extension - Additional attributes to add
 * @returns Merged schema
 * @throws Error if field names conflict
 */
export function extendSchema<T extends LogSchema | SchemaFields, U extends SchemaFields>(
  base: T,
  extension: U,
): T extends LogSchema<infer B> ? LogSchema<B & U> : T & U {
  // Extract base fields (from LogSchema.fields or plain object)
  const baseFields = base instanceof LogSchema ? base.fields : base;
  const baseKeys = new Set(Object.keys(baseFields));

  // Check for field name conflicts
  for (const key of Object.keys(extension)) {
    if (baseKeys.has(key)) {
      throw new Error(
        `Schema conflict: attribute '${key}' already exists in base schema. ` +
          `Base schema has: ${Object.keys(baseFields).join(', ')}`,
      );
    }
  }

  // Assert extension doesn't use reserved names
  LogSchema.assertUserFieldNames(Object.keys(extension));

  // Perform shallow merge
  const merged = { ...baseFields, ...extension };

  // If base was LogSchema, return LogSchema; otherwise return plain object
  if (base instanceof LogSchema) {
    return new LogSchema(merged) as unknown as T extends LogSchema<infer B> ? LogSchema<B & U> : T & U;
  }
  return merged as unknown as T extends LogSchema<infer B> ? LogSchema<B & U> : T & U;
}
