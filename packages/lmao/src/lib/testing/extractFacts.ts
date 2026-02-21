/**
 * Extract facts from a SpanBuffer tree.
 *
 * This is the bridge between LMAO's buffer system and the trace-testing
 * fact system. It walks the buffer tree and produces strongly-typed facts.
 *
 * @module testing/extractFacts
 */

import type { LogSchema } from '../schema/LogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_FF_ACCESS,
  ENTRY_TYPE_FF_USAGE,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_ERR,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_WARN,
} from '../schema/systemSchema.js';
import type { SpanBuffer } from '../types.js';
import {
  createFactArray,
  type FactArray,
  ffFact,
  type LogLevel,
  logFact,
  metricFact,
  scopeFact,
  spanErr,
  spanException,
  spanOk,
  spanStarted,
  type TraceFact,
  tagFact,
} from './facts.js';

type DynamicRowBuffer = Record<string, unknown>;

function getOptionalArray<T>(buffer: unknown, key: string): T[] | undefined {
  if (typeof buffer !== 'object' || buffer === null) {
    return undefined;
  }
  const value = (buffer as Record<string, unknown>)[key];
  return Array.isArray(value) ? (value as T[]) : undefined;
}

/**
 * Options for fact extraction.
 */
export interface ExtractFactsOptions {
  /**
   * Include scope facts (values propagated through span tree).
   * @default true
   */
  includeScope?: boolean;

  /**
   * Include tag facts from each row.
   * @default true
   */
  includeTags?: boolean;

  /**
   * Include log message facts.
   * @default true
   */
  includeLogs?: boolean;

  /**
   * Include feature flag facts.
   * @default true
   */
  includeFF?: boolean;

  /**
   * Include timing metrics (duration_ns).
   * @default false
   */
  includeMetrics?: boolean;

  /**
   * Schema field names to extract as tags.
   * If not specified, all non-null fields are extracted.
   */
  tagFields?: string[];
}

const DEFAULT_OPTIONS: Required<ExtractFactsOptions> = {
  includeScope: true,
  includeTags: true,
  includeLogs: true,
  includeFF: true,
  includeMetrics: false,
  tagFields: [],
};

/**
 * Extract facts from a SpanBuffer tree.
 *
 * Walks the buffer tree depth-first and produces facts for:
 * - Span lifecycle (started, ok, err, exception)
 * - Log entries (info, warn, error, debug)
 * - Tag values (from row 0 of each span)
 * - Scope values (propagated through tree)
 * - Feature flag accesses
 *
 * @param rootBuffer - The root SpanBuffer to extract from
 * @param options - Extraction options
 * @returns FactArray with all extracted facts
 *
 * @example
 * ```typescript
 * const tracer = new TestTracer(opContext);
 * await tracer.trace('my-op', myOp);
 *
 * const facts = extractFacts(tracer.rootBuffers[0]);
 *
 * expect(facts.has(spanOk('my-op'))).toBe(true);
 * expect(facts.hasMatch('log:error: *')).toBe(false);
 * ```
 */
export function extractFacts<T extends LogSchema>(
  rootBuffer: SpanBuffer<T>,
  options: ExtractFactsOptions = {},
): FactArray {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  const facts: TraceFact[] = [];

  walkBuffer(rootBuffer, facts, opts);

  return createFactArray(facts);
}

/**
 * Walk a buffer and its children, extracting facts.
 */
function walkBuffer<T extends LogSchema>(
  buffer: SpanBuffer<T>,
  facts: TraceFact[],
  opts: Required<ExtractFactsOptions>,
): void {
  // Span name is in message_values[0] (written by writeSpanStart)
  const spanName = buffer.message_values[0];
  const writeIndex = buffer._writeIndex;

  if (writeIndex === 0) {
    // Empty buffer - no facts to extract
    return;
  }

  // Always emit span:started as first fact for this span
  facts.push(spanStarted(spanName));

  // Extract scope facts (from buffer's scope values)
  if (opts.includeScope && buffer._scopeValues) {
    for (const [key, value] of Object.entries(buffer._scopeValues)) {
      if (value !== undefined && value !== null) {
        facts.push(scopeFact(key, String(value)));
      }
    }
  }

  // Extract tag facts from row 0 (ctx.tag writes here)
  // Also check row 1 (completion row - .with() on ok()/err())
  // And rows 2+ (log rows - .with() on log.*)
  if (opts.includeTags) {
    for (let row = 0; row < writeIndex; row++) {
      extractTagFacts(buffer, row, facts, opts);
    }
  }

  // Buffer layout per specs/lmao/01h_entry_types_and_logging_primitives.md:
  // - Row 0: span-start (tags overwrite this row's attribute columns)
  // - Row 1: span-ok/err/exception (completion status)
  // - Row 2+: log entries (info/debug/warn/error), ff entries
  const entryTypes = buffer.entry_type;
  const messages = getOptionalArray<string | undefined>(buffer, 'message_values');

  // Process log/ff entries from row 2 onwards
  for (let row = 2; row < writeIndex; row++) {
    const entryType = entryTypes[row];

    switch (entryType) {
      case ENTRY_TYPE_INFO:
      case ENTRY_TYPE_DEBUG:
      case ENTRY_TYPE_WARN:
      case ENTRY_TYPE_ERROR: {
        if (opts.includeLogs && messages) {
          const level = entryTypeToLogLevel(entryType);
          const message = messages[row] ?? '';
          facts.push(logFact(level, message));
        }
        break;
      }

      case ENTRY_TYPE_FF_ACCESS:
      case ENTRY_TYPE_FF_USAGE: {
        if (opts.includeFF) {
          extractFFfacts(buffer, row, facts);
        }
        break;
      }
    }
  }

  // Recurse into children BEFORE completing this span
  // This maintains the natural execution order in facts
  for (const child of buffer._children) {
    walkBuffer(child as SpanBuffer<T>, facts, opts);
  }

  // Completion status is ALWAYS at row 1 (not last row)
  // Row 1 is pre-initialized as span-exception, overwritten by ok()/err()
  if (writeIndex >= 2) {
    const completionEntryType = entryTypes[1];

    switch (completionEntryType) {
      case ENTRY_TYPE_SPAN_OK:
        facts.push(spanOk(spanName));
        break;

      case ENTRY_TYPE_SPAN_ERR: {
        const errorCode = getErrorCode(buffer, 1);
        facts.push(spanErr(spanName, errorCode));
        break;
      }

      case ENTRY_TYPE_SPAN_EXCEPTION: {
        const message = getExceptionMessage(buffer, 1);
        facts.push(spanException(spanName, message));
        break;
      }
    }
  }

  // Extract duration metric if requested
  if (opts.includeMetrics) {
    const duration = getSpanDuration(buffer);
    if (duration !== undefined) {
      facts.push(metricFact(`${spanName}:duration_ns`, Number(duration)));
    }
  }
}

/**
 * Extract tag facts from a specific row.
 */
function extractTagFacts<T extends LogSchema>(
  buffer: SpanBuffer<T>,
  row: number,
  facts: TraceFact[],
  opts: Required<ExtractFactsOptions>,
): void {
  const schema = buffer._logSchema;
  const fields = opts.tagFields.length > 0 ? opts.tagFields : Object.keys(schema.fields);

  for (const fieldName of fields) {
    // Skip system fields
    if (fieldName.startsWith('_')) continue;

    const nullsKey = `${fieldName}_nulls`;
    const valuesKey = `${fieldName}_values`;

    const dynamicBuffer = buffer as DynamicRowBuffer;
    const nulls = dynamicBuffer[nullsKey] as Uint8Array | undefined;
    const values = dynamicBuffer[valuesKey] as unknown[] | undefined;

    if (!nulls || !values) continue;

    // Check if value is non-null at this row
    if (!isNull(nulls, row)) {
      const value = values[row];
      if (value !== undefined) {
        facts.push(tagFact(fieldName, String(value)));
      }
    }
  }
}

/**
 * Extract feature flag facts from a row.
 */
function extractFFfacts<T extends LogSchema>(buffer: SpanBuffer<T>, row: number, facts: TraceFact[]): void {
  // ff_name and ff_value are system schema fields
  const ffNameValues = getOptionalArray<string>(buffer, 'ff_name_values');
  const ffValueValues = getOptionalArray<string>(buffer, 'ff_value_values');

  if (ffNameValues && ffValueValues) {
    const name = ffNameValues[row];
    const value = ffValueValues[row];
    if (name !== undefined && value !== undefined) {
      facts.push(ffFact(name, value));
    }
  }
}

/**
 * Check if a bit is unset in a null bitmap (Arrow format: 1 = valid, 0 = null).
 */
function isNull(nulls: Uint8Array, index: number): boolean {
  const byteIndex = index >> 3;
  const bitIndex = index & 7;
  return (nulls[byteIndex] & (1 << bitIndex)) === 0;
}

/**
 * Convert entry type number to log level string.
 */
function entryTypeToLogLevel(entryType: number): LogLevel {
  switch (entryType) {
    case ENTRY_TYPE_DEBUG:
      return 'debug';
    case ENTRY_TYPE_INFO:
      return 'info';
    case ENTRY_TYPE_WARN:
      return 'warn';
    case ENTRY_TYPE_ERROR:
      return 'error';
    default:
      return 'info';
  }
}

/**
 * Get error code from a span-err row.
 */
function getErrorCode<T extends LogSchema>(buffer: SpanBuffer<T>, row: number): string {
  const errorCodes = getOptionalArray<string>(buffer, 'error_code_values');
  return errorCodes?.[row] ?? 'UNKNOWN';
}

/**
 * Get exception message from a span-exception row.
 */
function getExceptionMessage<T extends LogSchema>(buffer: SpanBuffer<T>, row: number): string {
  const messages = getOptionalArray<string>(buffer, 'message_values');
  return messages?.[row] ?? 'Unknown exception';
}

/**
 * Calculate span duration from timestamps.
 */
function getSpanDuration<T extends LogSchema>(buffer: SpanBuffer<T>): bigint | undefined {
  const timestamps = buffer.timestamp;
  const writeIndex = buffer._writeIndex;

  if (writeIndex < 2) return undefined;

  // Duration = last timestamp - first timestamp
  const startNanos = timestamps[0];
  const endNanos = timestamps[writeIndex - 1];

  return endNanos - startNanos;
}
