/**
 * Runtime code generation for SpanLogger classes
 *
 * Per specs/lmao/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
 * - SpanLogger extends ColumnWriter from arrow-builder via codegen extension
 * - Handles log entries (rows 2+) - tag writing is in TagWriter (row 0)
 * - Uses _nextRow() to advance position, then fluent setters write at _writeIndex
 * - Compile-time enum mapping for 1-byte storage
 * - Direct buffer writes without intermediate objects
 *
 * Row layout in SpanBuffer:
 * - Row 0: tag attributes (written by TagWriter)
 * - Row 1: result (ok/err) entry
 * - Rows 2+: log entries (info/debug/warn/error)
 *
 * SpanLogger starts with _writeIndex = 1, so first _nextRow() makes it 2.
 */

import {
  type ColumnEntry,
  type ColumnWriter,
  type ColumnWriterExtension,
  getColumnWriterClass,
} from '@smoothbricks/arrow-builder';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_FF_ACCESS,
  ENTRY_TYPE_FF_USAGE,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from '../schema/systemSchema.js';
import { getEnumValues, getSchemaType } from '../schema/typeGuards.js';
import type { InferSchema, LogSchema } from '../schema/types.js';

import type { AnySpanBuffer, SpanBuffer } from '../types.js';

// =============================================================================
// SINGLETON HELPERS OBJECT
// =============================================================================
// Created once at module load, closed over by all generated SpanLogger classes.
// This avoids recreating the helpers object for each schema.

/**
 * Helper functions injected into generated SpanLogger code.
 * Singleton - same object shared by all generated SpanLogger classes.
 */
export const SPAN_LOGGER_HELPERS: {
  setNullBit: (bitmap: Uint8Array, idx: number) => void;
  fillNullBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number) => void;
  fillBooleanBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number, value: boolean) => void;
} = {
  setNullBit: (bitmap: Uint8Array, idx: number) => {
    bitmap[idx >>> 3] |= 1 << (idx & 7);
  },
  fillNullBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number) => {
    // Fill null bitmap for range [startIdx, endIdx)
    const startByte = startIdx >>> 3;
    const endByte = (endIdx - 1) >>> 3;

    if (startByte === endByte) {
      // All bits in same byte
      for (let i = startIdx; i < endIdx; i++) {
        bitmap[i >>> 3] |= 1 << (i & 7);
      }
    } else {
      // Handle first partial byte
      const startBit = startIdx & 7;
      if (startBit !== 0) {
        bitmap[startByte] |= 0xff << startBit;
      } else {
        bitmap[startByte] = 0xff;
      }

      // Fill middle bytes
      for (let b = startByte + 1; b < endByte; b++) {
        bitmap[b] = 0xff;
      }

      // Handle last partial byte
      const endBit = endIdx & 7;
      if (endBit !== 0) {
        bitmap[endByte] |= (1 << endBit) - 1;
      } else {
        bitmap[endByte] = 0xff;
      }
    }
  },
  fillBooleanBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number, value: boolean) => {
    // Fill boolean bitmap for range with value
    if (value) {
      // Same as fillNullBitmapRange - set bits to 1
      const startByte = startIdx >>> 3;
      const endByte = (endIdx - 1) >>> 3;

      if (startByte === endByte) {
        for (let i = startIdx; i < endIdx; i++) {
          bitmap[i >>> 3] |= 1 << (i & 7);
        }
      } else {
        const startBit = startIdx & 7;
        if (startBit !== 0) {
          bitmap[startByte] |= 0xff << startBit;
        } else {
          bitmap[startByte] = 0xff;
        }

        for (let b = startByte + 1; b < endByte; b++) {
          bitmap[b] = 0xff;
        }

        const endBit = endIdx & 7;
        if (endBit !== 0) {
          bitmap[endByte] |= (1 << endBit) - 1;
        } else {
          bitmap[endByte] = 0xff;
        }
      }
    } else {
      // Clear bits to 0
      const startByte = startIdx >>> 3;
      const endByte = (endIdx - 1) >>> 3;

      if (startByte === endByte) {
        for (let i = startIdx; i < endIdx; i++) {
          bitmap[i >>> 3] &= ~(1 << (i & 7));
        }
      } else {
        const startBit = startIdx & 7;
        if (startBit !== 0) {
          bitmap[startByte] &= ~(0xff << startBit);
        } else {
          bitmap[startByte] = 0;
        }

        for (let b = startByte + 1; b < endByte; b++) {
          bitmap[b] = 0;
        }

        const endBit = endIdx & 7;
        if (endBit !== 0) {
          bitmap[endByte] &= ~((1 << endBit) - 1);
        } else {
          bitmap[endByte] = 0;
        }
      }
    }
  },
};

/**
 * Base SpanLogger interface - core logging methods.
 * Extended by generated implementations with schema-specific methods.
 */
export interface BaseSpanLogger<T extends LogSchema> {
  /** Log info-level message */
  info(message: string): FluentLogEntry<T>;
  /** Log debug-level message */
  debug(message: string): FluentLogEntry<T>;
  /** Log warning message */
  warn(message: string): FluentLogEntry<T>;
  /** Log error message */
  error(message: string): FluentLogEntry<T>;
}

/**
 * Fluent interface returned by log methods (info, debug, warn, error, trace).
 * Includes system columns (line) plus all schema fields from T.
 * Also includes logging methods for continued chaining (e.g., ctx.log.info('x').info('y'))
 */
export type FluentLogEntry<T extends LogSchema> = {
  /**
   * Set the source code line number for this log entry.
   * Injected by the LMAO transformer.
   */
  line(lineNumber: number): FluentLogEntry<T>;
  /** Set error code for this entry */
  error_code(code: string): FluentLogEntry<T>;
  /** Set exception stack for this entry */
  exception_stack(stack: string): FluentLogEntry<T>;
  /** Set feature flag value for this entry */
  ff_value(value: string): FluentLogEntry<T>;
  /** Set uint64 value for this entry */
  uint64_value(value: bigint): FluentLogEntry<T>;

  // Logging methods for continued chaining
  info(message: string): FluentLogEntry<T>;
  debug(message: string): FluentLogEntry<T>;
  warn(message: string): FluentLogEntry<T>;
  error(message: string): FluentLogEntry<T>;
  trace(message: string): FluentLogEntry<T>;
} & {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => FluentLogEntry<T>;
};

/**
 * Complete SpanLogger implementation type (runtime-generated class).
 *
 * Includes:
 * - Core methods (info/debug/warn/error) from BaseSpanLogger
 * - Schema-specific methods from ColumnWriter<T>
 * - Internal methods (_setScope, _writeEntry, etc.)
 *
 * This is the FULL type returned by createSpanLogger().
 * Users see SpanLogger<T> which hides internal methods.
 */
export type SpanLoggerImpl<T extends LogSchema> = ColumnWriter<T> & {
  info(message: string): FluentLogEntry<T>;
  debug(message: string): FluentLogEntry<T>;
  warn(message: string): FluentLogEntry<T>;
  error(message: string): FluentLogEntry<T>;
  trace(message: string): FluentLogEntry<T>;
  readonly scope: Readonly<Record<string, unknown>>;
  _setScope(attributes: Partial<InferSchema<T>>): void;
};

/**
 * Generate enum value mapping code
 * Creates a switch-case statement for compile-time enum mapping
 */
function generateEnumMapping(fieldName: string, enumValues: readonly string[]): string {
  const cases = enumValues.map((value, index) => `    case ${JSON.stringify(value)}: return ${index};`).join('\n');

  return `
  function getEnumIndex_${fieldName}(value) {
    switch(value) {
${cases}
      default: return 0;
    }
  }`;
}

/**
 * Generate override fluent setters for enum fields.
 * These convert string values to numeric indices before writing to the buffer.
 * The buffer expects numeric indices (after the enum map removal change).
 */
function generateEnumFluentSetters(enumFieldNames: Set<string>): string {
  const setters: string[] = [];

  for (const fieldName of enumFieldNames) {
    setters.push(`
    /**
     * Override fluent setter for ${fieldName} to convert string → index.
     * Buffer expects numeric index, not string value.
     */
    ${fieldName}(value) {
      const idx = getEnumIndex_${fieldName}(value);
      this._buffer.${fieldName}(this._writeIndex, idx);
      return this;
    }`);
  }

  return setters.join('\n');
}

/**
 * Generate _setScope() method code - IMMUTABLE scope semantics
 *
 * Per specs/lmao/01i_span_scope_attributes.md:
 * - Creates NEW frozen object (never mutates existing)
 * - Merge semantics: new values merge with existing
 * - null clears a key, undefined is ignored
 * - Child spans inherit parent scope by reference (safe because immutable)
 * - Scope filling happens at Arrow conversion time, NOT during span execution
 */
function generateSetScopeMethod(): string {
  // Immutable scope: create new frozen object with merge semantics and null clearing
  // No buffer pre-filling - scope values are filled at Arrow conversion time via SIMD
  return `
    _setScope(attributes) {
      const current = this._buffer._scopeValues || {};
      const next = { ...current };
      
      for (const key of Object.keys(attributes)) {
        const value = attributes[key];
        if (value === null) {
          delete next[key];
        } else if (value !== undefined) {
          next[key] = value;
        }
      }
      
      this._buffer._scopeValues = Object.freeze(next);
    }`;
}

/**
 * Generate _prefillScopedAttributesOn(buffer) method - UNROLLED per-column with BULK null bitmap fill
 *
 * Takes buffer as argument instead of using this._buffer. This avoids the convoluted
 * pattern of temporarily switching this._buffer for prefill then switching back.
 * Called by _checkOverflow after getting overflow buffer.
 */
function generatePrefillScopedAttributesMethod(
  schemaFields: readonly ColumnEntry[],
  enumFieldNames: Set<string>,
): string {
  const columnFills = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = fieldName;
    const lmaoType = getSchemaType(fieldSchema);

    // Binary columns are not scope-fillable (object payloads can't be bulk-filled)
    if (lmaoType === 'binary') {
      return '';
    }

    // Boolean uses bit-packed storage - bulk fill using helper
    if (lmaoType === 'boolean') {
      return `
      {
        const scopeValue = buffer._scopeValues?.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined && buffer._${columnName}_values !== undefined) {
          helpers.fillBooleanBitmapRange(buffer.${columnName}_values, startIdx, endIdx, scopeValue);
          helpers.fillNullBitmapRange(buffer.${columnName}_nulls, startIdx, endIdx);
        }
      }`;
    }

    // Value processing based on type
    let valueExpr = 'scopeValue';
    if (lmaoType === 'enum' && enumFieldNames.has(fieldName)) {
      valueExpr = `getEnumIndex_${fieldName}(scopeValue)`;
    }

    // For string arrays (category/text), use manual loop instead of fill()
    if (lmaoType === 'category' || lmaoType === 'text') {
      return `
      {
        const scopeValue = buffer._scopeValues?.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined && buffer._${columnName}_values !== undefined) {
          const values = buffer.${columnName}_values;
          for (let i = startIdx; i < endIdx; i++) {
            values[i] = scopeValue;
          }
          helpers.fillNullBitmapRange(buffer.${columnName}_nulls, startIdx, endIdx);
        }
      }`;
    }

    return `
      {
        const scopeValue = buffer._scopeValues?.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined && buffer._${columnName}_values !== undefined) {
          buffer.${columnName}_values.fill(${valueExpr}, startIdx, endIdx);
          helpers.fillNullBitmapRange(buffer.${columnName}_nulls, startIdx, endIdx);
        }
      }`;
  });

  // Initial buffer: rows 0-1 reserved for span-start/end, data starts at _writeIndex=2
  // Overflow buffer: no reserved rows, data starts at _writeIndex=0
  // Use buffer._writeIndex as startIdx to handle both cases correctly
  return `
    _prefillScopedAttributesOn(buffer) {
      const startIdx = buffer._writeIndex;
      const endIdx = buffer._capacity;
      ${columnFills.join('\n')}
    }`;
}

/**
 * Build the extension for SpanLogger that extends ColumnWriter
 */
function buildSpanLoggerExtension(schema: LogSchema): ColumnWriterExtension {
  const schemaFields = schema._columns;

  // Collect enum mappings
  const enumMappings: string[] = [];
  const enumFieldNames = new Set<string>();

  for (const [fieldName, fieldSchema] of schemaFields) {
    const lmaoType = getSchemaType(fieldSchema);
    const enumValues = getEnumValues(fieldSchema);

    if (lmaoType === 'enum' && enumValues) {
      enumMappings.push(generateEnumMapping(fieldName, enumValues));
      enumFieldNames.add(fieldName);
    }
  }

  // Generate methods
  const setScopeMethod = generateSetScopeMethod();
  const prefillMethod = generatePrefillScopedAttributesMethod(schemaFields, enumFieldNames);
  const enumFluentSetters = generateEnumFluentSetters(enumFieldNames);

  return {
    constructorParams: '',

    // Entry type constants (inlined from lmao.ts)
    classPreamble: `
  const ENTRY_TYPE_INFO = ${ENTRY_TYPE_INFO};
  const ENTRY_TYPE_DEBUG = ${ENTRY_TYPE_DEBUG};
  const ENTRY_TYPE_WARN = ${ENTRY_TYPE_WARN};
  const ENTRY_TYPE_ERROR = ${ENTRY_TYPE_ERROR};
  const ENTRY_TYPE_TRACE = ${ENTRY_TYPE_TRACE};
  const ENTRY_TYPE_FF_ACCESS = ${ENTRY_TYPE_FF_ACCESS};
  const ENTRY_TYPE_FF_USAGE = ${ENTRY_TYPE_FF_USAGE};

  ${enumMappings.join('\n')}
`,

    // SpanLogger starts at writeIndex 1, so first nextRow() makes it 2
    // (Rows 0 and 1 are reserved for span-start and span-end)
    // Sync buffer's writeIndex - needed for Arrow conversion
    constructorCode: `
      this._writeIndex = 1;
      this._buffer._writeIndex = 2;
`,

    // Check for overflow before writing, switch to overflow buffer if needed.
    // Overflow when _writeIndex >= capacity (buffer is full, no room for this write).
    // Buffer.getOrCreateOverflow() handles: stats notification, capacity tuning, buffer creation.
    // We just need to switch this._buffer and prefill scope on the new buffer.
    methods:
      `_checkOverflow() {
      if (this._buffer._writeIndex >= this._buffer._capacity) {
        this._buffer = this._buffer.getOrCreateOverflow();
        this._prefillScopedAttributesOn(this._buffer);
      }
    }

    ` +
      // Write an info log entry.
      // writeLogEntry bumps buffer._writeIndex, writes timestamp+entry_type, returns idx.
      // We sync this._writeIndex so fluent setters write at correct row.
      `info(message) {
      this._checkOverflow();
      const idx = this._buffer._traceRoot.writeLogEntry(this._buffer, ENTRY_TYPE_INFO);
      this._writeIndex = idx;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer.constructor.stats.totalWrites++;
      return this;
    }

    ` +
      // Write a debug log entry.
      `debug(message) {
      this._checkOverflow();
      const idx = this._buffer._traceRoot.writeLogEntry(this._buffer, ENTRY_TYPE_DEBUG);
      this._writeIndex = idx;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer.constructor.stats.totalWrites++;
      return this;
    }

    ` +
      // Write a warn log entry.
      `warn(message) {
      this._checkOverflow();
      const idx = this._buffer._traceRoot.writeLogEntry(this._buffer, ENTRY_TYPE_WARN);
      this._writeIndex = idx;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer.constructor.stats.totalWrites++;
      return this;
    }

    ` +
      // Write an error log entry.
      `error(message) {
      this._checkOverflow();
      const idx = this._buffer._traceRoot.writeLogEntry(this._buffer, ENTRY_TYPE_ERROR);
      this._writeIndex = idx;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer.constructor.stats.totalWrites++;
      return this;
    }

    ` +
      // Write a trace log entry.
      `trace(message) {
      this._checkOverflow();
      const idx = this._buffer._traceRoot.writeLogEntry(this._buffer, ENTRY_TYPE_TRACE);
      this._writeIndex = idx;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer.constructor.stats.totalWrites++;
      return this;
    }

    ` +
      // Write a feature flag access entry (internal method, not on public type).
      // Called by FeatureFlagEvaluator to log flag access.
      `ffAccess(flagName, value) {
      this._checkOverflow();
      const idx = this._buffer._traceRoot.writeLogEntry(this._buffer, ENTRY_TYPE_FF_ACCESS);
      this._writeIndex = idx;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = flagName;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      if (this._buffer.ff_value_values) {
        const strValue = value === null || value === undefined ? 'null' : String(value);
        this._buffer.ff_value_values[idx] = strValue;
        if (this._buffer.ff_value_nulls) {
          helpers.setNullBit(this._buffer.ff_value_nulls, idx);
        }
      }
      this._buffer.constructor.stats.totalWrites++;
    }

    ` +
      // Write a feature flag usage entry (internal method, not on public type).
      // Called by FeatureFlagEvaluator to log flag usage.
      `ffUsage(flagName, context) {
      this._checkOverflow();
      const idx = this._buffer._traceRoot.writeLogEntry(this._buffer, ENTRY_TYPE_FF_USAGE);
      this._writeIndex = idx;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = flagName;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      // Context attributes can be written to user schema columns if provided
      // For now, just log the flag name - context can be added later if needed
      this._buffer.constructor.stats.totalWrites++;
    }

    ` +
      // Set the source code line number for the current log entry.
      // Injected by the LMAO transformer after info/debug/warn/error/trace calls.
      // Writes to _writeIndex (the current row).
      `line(lineNumber) {
      const idx = this._writeIndex;
      if (this._buffer.line_values) {
        this._buffer.line_values[idx] = lineNumber;
        if (this._buffer.line_nulls) {
          helpers.setNullBit(this._buffer.line_nulls, idx);
        }
      }
      return this;
    }

    error_code(code) {
      const idx = this._writeIndex;
      if (this._buffer.error_code_values) {
        this._buffer.error_code_values[idx] = code;
        if (this._buffer.error_code_nulls) {
          helpers.setNullBit(this._buffer.error_code_nulls, idx);
        }
      }
      return this;
    }

    exception_stack(stack) {
      const idx = this._writeIndex;
      if (this._buffer.exception_stack_values) {
        this._buffer.exception_stack_values[idx] = stack;
        if (this._buffer.exception_stack_nulls) {
          helpers.setNullBit(this._buffer.exception_stack_nulls, idx);
        }
      }
      return this;
    }

    ff_value(value) {
      const idx = this._writeIndex;
      if (this._buffer.ff_value_values) {
        this._buffer.ff_value_values[idx] = value;
        if (this._buffer.ff_value_nulls) {
          helpers.setNullBit(this._buffer.ff_value_nulls, idx);
        }
      }
      return this;
    }

    uint64_value(value) {
      const idx = this._writeIndex;
      if (this._buffer.uint64_value_values) {
        this._buffer.uint64_value_values[idx] = value;
        if (this._buffer.uint64_value_nulls) {
          helpers.setNullBit(this._buffer.uint64_value_nulls, idx);
        }
      }
      return this;
    }

    ` +
      // Get the scope values from buffer directly.
      `get scope() {
      return this._buffer._scopeValues;
    }

    ${setScopeMethod}

    ${prefillMethod}

    ` +
      // Override fluent setters for enum fields to convert string → index
      `${enumFluentSetters}
`,

    // Use singleton helpers object (created once at module load)
    dependencies: {
      helpers: SPAN_LOGGER_HELPERS,
    },
  };
}

/**
 * Cache for generated SpanLogger classes per schema.
 */
const spanLoggerClassCache = new WeakMap<LogSchema, new (buffer: AnySpanBuffer) => SpanLoggerImpl<LogSchema>>();

/**
 * Create SpanLogger class constructor from schema.
 * This is the cold-path function called at module creation time.
 *
 * SpanLogger extends ColumnWriter and adds:
 * - info(), debug(), warn(), error() methods for log entries
 * - _setScope() for setting scoped attributes
 * - _getNextBuffer() override for SpanBuffer creation on overflow
 *
 * Note: Fluent attribute setters are inherited from ColumnWriter and write at _writeIndex.
 */
export function createSpanLoggerClass<T extends LogSchema>(
  schema: T,
): new (
  buffer: AnySpanBuffer,
) => SpanLoggerImpl<T> {
  // Check cache first
  let SpanLoggerClass = spanLoggerClassCache.get(schema);

  if (!SpanLoggerClass) {
    const extension = buildSpanLoggerExtension(schema);

    // Use arrow-builder's getColumnWriterClass with our extension
    const WriterClass = getColumnWriterClass(schema, extension);

    SpanLoggerClass = WriterClass as unknown as new (buffer: AnySpanBuffer) => SpanLoggerImpl<LogSchema>;

    spanLoggerClassCache.set(schema, SpanLoggerClass);
  }

  return SpanLoggerClass as new (
    buffer: AnySpanBuffer,
  ) => SpanLoggerImpl<T>;
}

/**
 * Create a SpanLogger instance for the given buffer.
 *
 * @param schema - Tag attribute schema
 * @param buffer - SpanBuffer to write to
 */
export function createSpanLogger<T extends LogSchema>(schema: T, buffer: SpanBuffer<T>): SpanLoggerImpl<T> {
  const SpanLoggerClass = createSpanLoggerClass(schema);
  return new SpanLoggerClass(buffer);
}
