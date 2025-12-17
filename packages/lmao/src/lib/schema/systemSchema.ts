/**
 * System schema - base columns that all modules inherit
 *
 * Per specs/01h_entry_types_and_logging_primitives.md and 01f_arrow_table_structure.md:
 * - These columns are available in all traces regardless of user schema
 * - The `label` column is UNIFIED for span names, log message templates, and feature flag names
 */

import { S } from './builder.js';
import { defineTagAttributes } from './defineTagAttributes.js';

/**
 * Base system schema that all modules inherit
 *
 * These columns support:
 * - Unified label (span name, log message template, or feature flag name based on entry type)
 * - Source location (lineNumber)
 * - Error handling (errorCode, exceptionMessage, exceptionStack)
 * - Result messages (resultMessage)
 * - Feature flags (ffValue, action, outcome, contextUserId, contextRequestId)
 */
export const systemSchema = defineTagAttributes({
  /**
   * Unified label column - serves different purposes based on entry type.
   *
   * Per specs/01h_entry_types_and_logging_primitives.md "The `label` Column":
   * - span-start, span-ok, span-err, span-exception: Span name (e.g., 'create-user')
   * - info, debug, warn, error, trace: Log message TEMPLATE (e.g., 'User ${userId} created')
   * - ff-access, ff-usage: Feature flag name (e.g., 'darkMode')
   * - tag: Span name (same as span lifecycle entries)
   *
   * Uses S.category() because:
   * - Span names repeat across many traces
   * - Log templates repeat (only values differ)
   * - Flag names repeat across evaluations
   * - String interning provides excellent compression
   *
   * NOTE: Log messages are FORMAT STRINGS, NOT interpolated! The template is stored
   * verbatim, and values go in their own typed columns for queryability.
   */
  label: S.category(),

  /**
   * Source code line number for this entry.
   *
   * Per specs/01c_context_flow_and_task_wrappers.md "Line Number System":
   * - Uint16 column (max 65535 lines per file)
   * - TypeScript transformer injects line numbers at compile time
   * - No runtime overhead - just a method call with literal number
   * - Value of 0 means "line number not set"
   *
   * Used for:
   * - Linking trace entries back to source code
   * - Debugging and error analysis
   * - IDE integration for "jump to line"
   */
  lineNumber: S.number(),

  // Error handling
  errorCode: S.category(),
  exceptionMessage: S.text(),
  exceptionStack: S.text(),

  // Result messages
  resultMessage: S.text(),

  // Feature flags (ffName is now part of unified `label` column)
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
      `!  User schema conflicts with system schema fields: ${conflictingKeys.join(', ')}\n` +
        '   User definitions will override system schema. This may cause unexpected behavior.\n' +
        `   System fields: ${Array.from(systemKeys).join(', ')}`,
    );
  }

  return {
    ...systemSchema,
    ...userSchema,
  } as typeof systemSchema & T;
}
