/**
 * System schema - base columns that all modules inherit
 *
 * Per specs/01h_entry_types_and_logging_primitives.md and 01f_arrow_table_structure.md:
 * - These columns are available in all traces regardless of user schema
 * - The `message` column is UNIFIED for span names, log message templates, exception messages, result messages, and feature flag names
 */

import type {
  CategorySchemaWithMetadata,
  NumberSchemaWithMetadata,
  TextSchemaWithMetadata,
} from '@smoothbricks/arrow-builder';
import { S as ArrowS } from '@smoothbricks/arrow-builder';
import { type DefinedTagAttributes, defineTagAttributes } from './defineTagAttributes.js';
import type { TagAttributeSchema } from './types.js';

/**
 * System schema field type - explicit type to avoid Sury's internal brand symbol leaking.
 */
interface SystemSchemaFieldTypes {
  message: CategorySchemaWithMetadata;
  lineNumber: NumberSchemaWithMetadata;
  errorCode: CategorySchemaWithMetadata;
  exceptionStack: TextSchemaWithMetadata;
  ffValue: CategorySchemaWithMetadata;
}

// Use arrow-builder's S directly (not lmao's wrapped version) to get clean types
// that don't include the flag builder methods - this avoids type inference issues.
const messageSchema: CategorySchemaWithMetadata = ArrowS.category().eager();
const lineNumberSchema: NumberSchemaWithMetadata = ArrowS.number();
const errorCodeSchema: CategorySchemaWithMetadata = ArrowS.category();
const exceptionStackSchema: TextSchemaWithMetadata = ArrowS.text();
const ffValueSchema: CategorySchemaWithMetadata = ArrowS.category();

/**
 * Raw system schema fields object.
 * Using explicit type annotation to ensure proper type inference.
 */
const systemSchemaFields: SystemSchemaFieldTypes & TagAttributeSchema = {
  /**
   * Unified message column - serves different purposes based on entry type.
   *
   * Per specs/01h_entry_types_and_logging_primitives.md:
   * - span-start, span-ok, span-err: Span name (e.g., 'create-user')
   * - span-exception: Exception message (e.g., 'Connection timeout')
   * - info, debug, warn, error, trace: Log message TEMPLATE (e.g., 'User ${userId} created')
   * - ff-access, ff-usage: Feature flag name (e.g., 'darkMode')
   * - tag: Span name (same as span lifecycle entries)
   *
   * Uses S.category().eager() because:
   * - Message is ALWAYS written for every entry type (span name, log message, etc.)
   * - Eager allocation eliminates null bitmap overhead (never null)
   * - Span names repeat across many traces
   * - Log templates repeat (only values differ)
   * - Flag names repeat across evaluations
   * - Exception messages often repeat (same error types)
   * - String interning provides excellent compression
   *
   * NOTE: Log messages are FORMAT STRINGS, NOT interpolated! The template is stored
   * verbatim, and values go in their own typed columns for queryability.
   */
  message: messageSchema,

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
  lineNumber: lineNumberSchema,

  // Error handling
  errorCode: errorCodeSchema,
  exceptionStack: exceptionStackSchema,

  // Feature flags (ffName is now part of unified `message` column)
  ffValue: ffValueSchema, // Can be boolean, string, or number as string
};

/**
 * Base system schema that all modules inherit.
 * Wrapped with defineTagAttributes for validation and extension support.
 */
export const systemSchema: DefinedTagAttributes<SystemSchemaFieldTypes & TagAttributeSchema> = defineTagAttributes(
  systemSchemaFields,
  // Skip reserved name validation for system schema - `message` is a system column
  { _skipReservedNameValidation: true },
);

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
