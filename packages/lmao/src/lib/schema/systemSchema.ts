/**
 * System schema - base columns that all modules inherit
 *
 * Per specs/01h_entry_types_and_logging_primitives.md and 01f_arrow_table_structure.md:
 * - All system columns use attr_ prefix to prevent conflicts with SpanBuffer internals
 * - These columns are available in all traces regardless of user schema
 */

import { S } from './builder.js';
import { defineTagAttributes } from './defineTagAttributes.js';

/**
 * Base system schema that all modules inherit
 *
 * These columns support:
 * - Span lifecycle (spanName)
 * - Logging (logMessage)
 * - Error handling (errorCode, exceptionMessage, exceptionStack)
 * - Result messages (resultMessage)
 * - Feature flags (ffName, ffValue, action, outcome, contextUserId, contextRequestId)
 */
export const systemSchema = defineTagAttributes({
  // Span lifecycle
  spanName: S.category(),

  // Logging (text type - messages are unique)
  logMessage: S.text(),

  // Error handling
  errorCode: S.category(),
  exceptionMessage: S.text(),
  exceptionStack: S.text(),

  // Result messages
  resultMessage: S.text(),

  // Feature flags
  ffName: S.category(),
  ffValue: S.category(), // Can be boolean, string, or number as string
  action: S.category(),
  outcome: S.category(),

  // Feature flag evaluation context
  contextUserId: S.category(),
  contextRequestId: S.category(),
});

/**
 * Merge system schema with user schema
 * System columns are always included, user schema extends them
 *
 * Warns if user schema conflicts with system schema fields.
 */
export function mergeWithSystemSchema<T extends Record<string, unknown>>(userSchema: T): typeof systemSchema & T {
  // Check for conflicts between user schema and system schema
  const systemKeys = new Set(Object.keys(systemSchema));
  const userKeys = Object.keys(userSchema);

  const conflictingKeys = userKeys.filter((key) => {
    // Only check if it's a schema field (not a method like validate/parse/extend)
    const value = userSchema[key];
    return systemKeys.has(key) && typeof value !== 'function';
  });

  if (conflictingKeys.length > 0) {
    console.warn(
      `⚠️  User schema conflicts with system schema fields: ${conflictingKeys.join(', ')}\n` +
        '   User definitions will override system schema. This may cause unexpected behavior.\n' +
        `   System fields: ${Array.from(systemKeys).join(', ')}`,
    );
  }

  return {
    ...systemSchema,
    ...userSchema,
  } as typeof systemSchema & T;
}
