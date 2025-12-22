/**
 * System schema - base columns that all modules inherit
 *
 * Per specs/01h_entry_types_and_logging_primitives.md and 01f_arrow_table_structure.md:
 * - These columns are available in all traces regardless of user schema
 * - The `message` column is UNIFIED for span names, log message templates, exception messages, result messages, and feature flag names
 */

import type {
  EagerCategorySchema,
  LazyBigUint64Schema,
  LazyCategorySchema,
  LazyNumberSchema,
  LazyTextSchema,
} from '@smoothbricks/arrow-builder';
import { S as ArrowS } from '@smoothbricks/arrow-builder';
import { type DefinedLogSchema, defineLogSchema } from './defineLogSchema.js';
import { LogSchema, type Schema } from './types.js';

/**
 * System schema field type - explicit type to avoid Sury's internal brand symbol leaking.
 */
interface SystemSchemaFieldTypes extends Record<string, Schema<unknown>> {
  message: EagerCategorySchema;
  line: LazyNumberSchema;
  error_code: LazyCategorySchema;
  exception_stack: LazyTextSchema;
  ff_value: LazyCategorySchema;
  uint64_value: LazyBigUint64Schema;
}

// Use arrow-builder's S directly (not lmao's wrapped version) to get clean types
// that don't include the flag builder methods - this avoids type inference issues.
const messageSchema: EagerCategorySchema = ArrowS.category().eager();
const lineSchema: LazyNumberSchema = ArrowS.number();
const errorCodeSchema: LazyCategorySchema = ArrowS.category();
const exceptionStackSchema: LazyTextSchema = ArrowS.text();
const ffValueSchema: LazyCategorySchema = ArrowS.category();
const uint64ValueSchema: LazyBigUint64Schema = ArrowS.bigUint64();

/**
 * Raw system schema fields object.
 * Using explicit type annotation to ensure proper type inference.
 */
/**
 * System schema field names - used to filter out system fields when processing user schema
 * These fields are handled separately in Arrow conversion and should not be included
 * in the user attribute columns.
 */
export const SYSTEM_SCHEMA_FIELD_NAMES = new Set([
  'message',
  'line',
  'error_code',
  'exception_stack',
  'ff_value',
  'uint64_value',
]);

/**
 * Reserved system column names - these correspond to Arrow table columns and cannot be
 * used as user schema field names because they conflict with SpanBuffer properties.
 */
export const RESERVED_SYSTEM_COLUMN_NAMES = new Set([
  // Arrow table columns (exact names)
  'trace_id',
  'thread_id',
  'span_id',
  'parent_thread_id',
  'parent_span_id',
  'timestamp',
  'entry_type',
]);

const systemSchemaFields: SystemSchemaFieldTypes = {
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
   * Per specs/01c_context_flow_and_op_wrappers.md "Line Number System":
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
  line: lineSchema,

  // Error handling
  error_code: errorCodeSchema,
  exception_stack: exceptionStackSchema,

  // Feature flags (ffName is now part of unified `message` column)
  ff_value: ffValueSchema, // Can be boolean, string, or number as string

  /**
   * uint64 value column for metrics and user .uint64() API.
   *
   * Per specs/01f_arrow_table_structure.md:
   * - Metric values (counts, durations in nanoseconds)
   * - User large integers via .uint64() fluent method
   * - Sparse data (only rows that need uint64)
   *
   * Uses S.bigUint64() (lazy BigUint64Array) because:
   * - Most trace rows don't have a uint64 value
   * - Only metrics rows and entries where .uint64() is called
   * - Zero overhead for rows that don't use it
   */
  uint64_value: uint64ValueSchema,
};

/**
 * Base system schema that all modules inherit.
 * Wrapped with defineLogSchema for validation and extension support.
 */
export const systemSchema: DefinedLogSchema<SystemSchemaFieldTypes> = defineLogSchema(
  systemSchemaFields,
  // Skip reserved name validation for system schema - `message` is a system column
  { _skipReservedNameValidation: true },
);

/**
 * Merge system schema with user schema
 * System columns are always included, user schema extends them
 *
 * Warns if user schema conflicts with system schema fields or reserved column names.
 */
export function mergeWithSystemSchema<T extends Record<string, unknown>>(userSchema: T): SystemSchemaFieldTypes & T {
  // Check for conflicts between user schema and system schema
  // Use fieldNames to get only actual field names, not ColumnSchema properties or methods
  const systemKeys = new Set(systemSchema.fieldNames);
  const reservedKeys = RESERVED_SYSTEM_COLUMN_NAMES;
  const userKeys = userSchema instanceof LogSchema ? userSchema.fieldNames : Object.keys(userSchema);

  const conflictingSystemKeys = userKeys.filter((key) => {
    // Only check if it's a schema field (not a method like validate/parse/extend)
    const value = userSchema[key];
    return systemKeys.has(key) && typeof value !== 'function';
  });

  const conflictingReservedKeys = userKeys.filter((key) => {
    const value = userSchema[key];
    return reservedKeys.has(key) && typeof value !== 'function';
  });

  if (conflictingSystemKeys.length > 0) {
    console.warn(
      `!  User schema conflicts with system schema fields: ${conflictingSystemKeys.join(', ')}\n` +
        '   User definitions will override system schema. This may cause unexpected behavior.\n' +
        `   System fields: ${Array.from(systemKeys).join(', ')}`,
    );
  }

  if (conflictingReservedKeys.length > 0) {
    throw new Error(
      `User schema cannot include reserved system column names: ${conflictingReservedKeys.join(', ')}\n` +
        '   These names conflict with SpanBuffer properties that correspond to Arrow table columns.\n' +
        `   Reserved names: ${Array.from(reservedKeys).join(', ')}`,
    );
  }

  // Spread .fields to get the actual schema field definitions
  // (spreading systemSchema directly would spread class instance properties like _fieldNames, not schema fields)
  return {
    ...systemSchema.fields,
    ...userSchema,
  } as SystemSchemaFieldTypes & T;
}

// =============================================================================
// Entry Type Constants
// =============================================================================
// Per specs/01h_entry_types_and_logging_primitives.md
// Stored in Uint8Array for 1-byte storage per entry.
// Ordered by frequency/importance:
// 1-4: Span lifecycle (most common - every span has start + end)
// 5-9: Logging levels (ordered by verbosity: trace < debug < info < warn < error)
// 10-11: Feature flags (least common)
// =============================================================================

/** Span start event - written at row 0 when a span begins */
export const ENTRY_TYPE_SPAN_START = 1;

/** Span success completion - written to row 1 via ctx.ok() */
export const ENTRY_TYPE_SPAN_OK = 2;

/** Span error completion - written to row 1 via ctx.err() */
export const ENTRY_TYPE_SPAN_ERR = 3;

/** Span exception - written to row 1 when an unhandled exception occurs */
export const ENTRY_TYPE_SPAN_EXCEPTION = 4;

/** Trace log level entry - via ctx.log.trace() (most verbose) */
export const ENTRY_TYPE_TRACE = 5;

/** Debug log level entry - via ctx.log.debug() */
export const ENTRY_TYPE_DEBUG = 6;

/** Info log level entry - via ctx.log.info() */
export const ENTRY_TYPE_INFO = 7;

/** Warning log level entry - via ctx.log.warn() */
export const ENTRY_TYPE_WARN = 8;

/** Error log level entry - via ctx.log.error() */
export const ENTRY_TYPE_ERROR = 9;

/** Feature flag access event - logged when a flag value is first read in a span */
export const ENTRY_TYPE_FF_ACCESS = 10;

/** Feature flag usage event - logged when a flag influences a code path decision */
export const ENTRY_TYPE_FF_USAGE = 11;

// =============================================================================
// Period Markers
// =============================================================================

/** Period start marker - written at the beginning of each metrics collection period */
export const ENTRY_TYPE_PERIOD_START = 12;

// =============================================================================
// Op Metrics (per specs/01n_op_and_buffer_metrics.md)
// =============================================================================

/** Op invocations counter - total calls to this operation */
export const ENTRY_TYPE_OP_INVOCATIONS = 13;

/** Op errors counter - calls that returned err() */
export const ENTRY_TYPE_OP_ERRORS = 14;

/** Op exceptions counter - calls that threw unhandled exceptions */
export const ENTRY_TYPE_OP_EXCEPTIONS = 15;

/** Op duration total - sum of all invocation durations (ms) */
export const ENTRY_TYPE_OP_DURATION_TOTAL = 16;

/** Op duration ok - sum of successful invocation durations (ms) */
export const ENTRY_TYPE_OP_DURATION_OK = 17;

/** Op duration err - sum of error invocation durations (ms) */
export const ENTRY_TYPE_OP_DURATION_ERR = 18;

/** Op duration min - minimum invocation duration (ms) */
export const ENTRY_TYPE_OP_DURATION_MIN = 19;

/** Op duration max - maximum invocation duration (ms) */
export const ENTRY_TYPE_OP_DURATION_MAX = 20;

// =============================================================================
// Buffer Metrics (per specs/01n_op_and_buffer_metrics.md)
// =============================================================================

/** Buffer writes counter - total writes to primary buffers */
export const ENTRY_TYPE_BUFFER_WRITES = 21;

/** Buffer overflow writes counter - writes to overflow buffers */
export const ENTRY_TYPE_BUFFER_OVERFLOW_WRITES = 22;

/** Buffer created counter - total buffers allocated */
export const ENTRY_TYPE_BUFFER_CREATED = 23;

/** Buffer overflows counter - times a buffer overflowed to next */
export const ENTRY_TYPE_BUFFER_OVERFLOWS = 24;

/**
 * Human-readable names for entry types, indexed by entry type code.
 *
 * Used for Arrow dictionary encoding and display purposes.
 * Index 0 is unused (entry types start at 1).
 *
 * @example
 * ```typescript
 * const entryType = buffer.entry_type[index];
 * const typeName = ENTRY_TYPE_NAMES[entryType]; // e.g., 'span-ok'
 * ```
 */
export const ENTRY_TYPE_NAMES = [
  '', // 0 - unused
  'span-start', // 1
  'span-ok', // 2
  'span-err', // 3
  'span-exception', // 4
  'trace', // 5 (most verbose)
  'debug', // 6
  'info', // 7
  'warn', // 8
  'error', // 9
  'ff-access', // 10
  'ff-usage', // 11
  'period-start', // 12
  'op-invocations', // 13
  'op-errors', // 14
  'op-exceptions', // 15
  'op-duration-total', // 16
  'op-duration-ok', // 17
  'op-duration-err', // 18
  'op-duration-min', // 19
  'op-duration-max', // 20
  'buffer-writes', // 21
  'buffer-overflow-writes', // 22
  'buffer-created', // 23
  'buffer-overflows', // 24
] as const;
