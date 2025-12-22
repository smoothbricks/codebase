/**
 * Runtime code generation for SpanLogger classes
 *
 * Per specs/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
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

import { type ColumnWriter, type ColumnWriterExtension, getColumnWriterClass } from '@smoothbricks/arrow-builder';
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
import { createNextBuffer as createNextSpanBuffer } from '../spanBuffer.js';
// Import timestamp function - will be injected as dependency
import { getTimestampNanos } from '../timestamp.js';
import type { SpanBuffer } from '../types.js';

/**
 * SpanLogger interface with logging methods and schema-specific attribute setters.
 *
 * Extends ColumnWriter<T> which provides:
 * - _buffer, _writeIndex, _nextRow(), _getNextBuffer()
 * - Fluent setter methods for each schema field
 *
 * SpanLogger adds:
 * - info/debug/warn/error logging methods
 * - Scope management (_getScope, _setScope)
 */
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

export type BaseSpanLogger<T extends LogSchema> = ColumnWriter<T> & {
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
 * Per specs/01i_span_scope_attributes.md:
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
 * Generate _prefillScopedAttributes() method - UNROLLED per-column with BULK null bitmap fill
 */
function generatePrefillScopedAttributesMethod(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  const columnFills = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = fieldName;
    const lmaoType = getSchemaType(fieldSchema);

    // Boolean uses bit-packed storage - bulk fill using helper
    if (lmaoType === 'boolean') {
      return `
      {
        const scopeValue = this._buffer._scopeValues?.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          helpers.fillBooleanBitmapRange(this._buffer.${columnName}_values, startIdx, endIdx, scopeValue);
          helpers.fillNullBitmapRange(this._buffer.${columnName}_nulls, startIdx, endIdx);
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
        const scopeValue = this._buffer._scopeValues?.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          const values = this._buffer.${columnName}_values;
          for (let i = startIdx; i < endIdx; i++) {
            values[i] = scopeValue;
          }
          helpers.fillNullBitmapRange(this._buffer.${columnName}_nulls, startIdx, endIdx);
        }
      }`;
    }

    return `
      {
        const scopeValue = this._buffer._scopeValues?.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          this._buffer.${columnName}_values.fill(${valueExpr}, startIdx, endIdx);
          helpers.fillNullBitmapRange(this._buffer.${columnName}_nulls, startIdx, endIdx);
        }
      }`;
  });

  return `
    _prefillScopedAttributes() {
      const startIdx = this._writeIndex + 1;
      const endIdx = this._buffer._capacity;
      ${columnFills.join('\n')}
    }`;
}

/**
 * Build the extension for SpanLogger that extends ColumnWriter
 */
function buildSpanLoggerExtension(schema: LogSchema): ColumnWriterExtension {
  const schemaFields = Array.from(schema.fieldEntries());

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
    constructorParams: 'createNextBuffer',

    // Entry type constants (inlined from lmao.ts)
    preamble: `
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
      this._createNextBuffer = createNextBuffer;
      this._inOverflow = false;
`,

    // Override _getNextBuffer to create new SpanBuffer on overflow.
    // Called by _nextRow() when buffer is full.
    // Track overflow and check if capacity should be tuned (per specs/01b2_buffer_self_tuning.md)
    // This happens on overflow to adapt quickly to workload changes
    // Link the chain
    // Pre-fill new buffer with scoped attributes
    methods:
      `
    _getNextBuffer() {
      const oldBuffer = this._buffer;
      oldBuffer._module.sb_overflows++;
      this._inOverflow = true;
      const nextBuffer = this._createNextBuffer(oldBuffer);
      oldBuffer._next = nextBuffer;
      this._buffer = nextBuffer;
      this._prefillScopedAttributes();
      this._buffer = oldBuffer;
      return nextBuffer;
    }

    ` +
      // Override _nextRow to sync buffer's writeIndex for Arrow conversion.
      // Check overflow BEFORE incrementing
      // Sync buffer writeIndex for new buffer
      // Sync buffer's writeIndex - Arrow conversion uses this
      `_nextRow() {
      if (this._writeIndex >= this._buffer._capacity - 1) {
        this._buffer = this._getNextBuffer();
        this._writeIndex = -1;
        this._buffer._writeIndex = 0;
      }
      this._writeIndex++;
      this._buffer._writeIndex = this._writeIndex + 1;
      return this;
    }

    ` +
      // Write an info log entry.
      // Advances to next row, writes system columns, returns this for fluent chaining.
      // Write system columns
      // Write message to unified message column (log message template)
      // For eager columns (like message), there's no null bitmap - the column is always present
      // NOTE: Scope values are NOT written here - they are filled at Arrow conversion time
      // Track write for capacity tuning (per specs/01b2_buffer_self_tuning.md)
      `info(message) {
      this._nextRow();
      const idx = this._writeIndex;
      this._buffer._timestamps[idx] = helpers.getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_INFO;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer._module.sb_totalWrites++;
      if (this._inOverflow) {
        this._buffer._module.sb_overflowWrites++;
      }
      return this;
    }

    ` +
      // Write a debug log entry.
      `debug(message) {
      this._nextRow();
      const idx = this._writeIndex;
      this._buffer._timestamps[idx] = helpers.getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_DEBUG;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer._module.sb_totalWrites++;
      if (this._inOverflow) {
        this._buffer._module.sb_overflowWrites++;
      }
      return this;
    }

    ` +
      // Write a warn log entry.
      `warn(message) {
      this._nextRow();
      const idx = this._writeIndex;
      this._buffer._timestamps[idx] = helpers.getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_WARN;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer._module.sb_totalWrites++;
      if (this._inOverflow) {
        this._buffer._module.sb_overflowWrites++;
      }
      return this;
    }

    ` +
      // Write an error log entry.
      `error(message) {
      this._nextRow();
      const idx = this._writeIndex;
      this._buffer._timestamps[idx] = helpers.getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_ERROR;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer._module.sb_totalWrites++;
      if (this._inOverflow) {
        this._buffer._module.sb_overflowWrites++;
      }
      return this;
    }

    ` +
      // Write a trace log entry.
      `trace(message) {
      this._nextRow();
      const idx = this._writeIndex;
      this._buffer._timestamps[idx] = helpers.getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_TRACE;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = message;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      this._buffer._module.sb_totalWrites++;
      if (this._inOverflow) {
        this._buffer._module.sb_overflowWrites++;
      }
      return this;
    }

    ` +
      // Write a feature flag access entry (internal method, not on public type).
      // Called by FeatureFlagEvaluator to log flag access.
      `ffAccess(flagName, value) {
      this._nextRow();
      const idx = this._writeIndex;
      this._buffer._timestamps[idx] = helpers.getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_FF_ACCESS;
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
      this._buffer._module.sb_totalWrites++;
      if (this._inOverflow) {
        this._buffer._module.sb_overflowWrites++;
      }
    }

    ` +
      // Write a feature flag usage entry (internal method, not on public type).
      // Called by FeatureFlagEvaluator to log flag usage.
      `ffUsage(flagName, context) {
      this._nextRow();
      const idx = this._writeIndex;
      this._buffer._timestamps[idx] = helpers.getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_FF_USAGE;
      if (this._buffer.message_values) {
        this._buffer.message_values[idx] = flagName;
        if (this._buffer.message_nulls) {
          helpers.setNullBit(this._buffer.message_nulls, idx);
        }
      }
      // Context attributes can be written to user schema columns if provided
      // For now, just log the flag name - context can be added later if needed
      this._buffer._module.sb_totalWrites++;
      if (this._inOverflow) {
        this._buffer._module.sb_overflowWrites++;
      }
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

    dependencies: {
      helpers: {
        getTimestampNanos,
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
      },
    },
  };
}

/**
 * Cache for generated SpanLogger classes per schema.
 */
const spanLoggerClassCache = new WeakMap<
  LogSchema,
  new (
    buffer: SpanBuffer,
    createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
  ) => BaseSpanLogger<LogSchema>
>();

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
  buffer: SpanBuffer,
  createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
) => BaseSpanLogger<T> {
  // Check cache first
  let SpanLoggerClass = spanLoggerClassCache.get(schema);

  if (!SpanLoggerClass) {
    const extension = buildSpanLoggerExtension(schema);

    // Use arrow-builder's getColumnWriterClass with our extension
    const WriterClass = getColumnWriterClass(schema, extension);

    SpanLoggerClass = WriterClass as unknown as new (
      buffer: SpanBuffer,
      createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
    ) => BaseSpanLogger<LogSchema>;

    spanLoggerClassCache.set(schema, SpanLoggerClass);
  }

  return SpanLoggerClass as new (
    buffer: SpanBuffer,
    createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
  ) => BaseSpanLogger<T>;
}

/**
 * Create a SpanLogger instance for the given buffer.
 *
 * @param schema - Tag attribute schema
 * @param buffer - SpanBuffer to write to
 * @param createNextBuffer - Function to create next buffer on overflow (defaults to createNextSpanBuffer)
 */
export function createSpanLogger<T extends LogSchema>(
  schema: T,
  buffer: SpanBuffer<T>,
  createNextBuffer: (buffer: SpanBuffer<T>) => SpanBuffer<T> = createNextSpanBuffer as unknown as (
    buffer: SpanBuffer<T>,
  ) => SpanBuffer<T>,
): BaseSpanLogger<T> {
  const SpanLoggerClass = createSpanLoggerClass(schema);
  // Type assertion needed because SpanLoggerClass constructor expects SpanBuffer
  // (non-generic) but we pass SpanBuffer<T>. This is safe because at runtime
  // SpanBuffer<T> IS a SpanBuffer - the generic is only for compile-time typing.
  return new SpanLoggerClass(
    buffer as unknown as SpanBuffer,
    createNextBuffer as unknown as (buffer: SpanBuffer) => SpanBuffer,
  );
}
