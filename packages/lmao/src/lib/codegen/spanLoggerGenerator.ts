/**
 * Runtime code generation for SpanLogger classes
 *
 * Per specs/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
 * - SpanLogger extends ColumnWriter from arrow-builder via codegen extension
 * - Handles log entries (rows 2+) - tag writing is in TagWriter (row 0)
 * - Uses nextRow() to advance position, then fluent setters write at _writeIndex
 * - Compile-time enum mapping for 1-byte storage
 * - Direct buffer writes without intermediate objects
 *
 * Row layout in SpanBuffer:
 * - Row 0: tag attributes (written by TagWriter)
 * - Row 1: result (ok/err) entry
 * - Rows 2+: log entries (info/debug/warn/error)
 *
 * SpanLogger starts with _writeIndex = 1, so first nextRow() makes it 2.
 */

import { type ColumnWriter, type ColumnWriterExtension, getColumnWriterClass } from '@smoothbricks/arrow-builder';
import { getEnumValues, getLmaoSchemaType } from '../schema/typeGuards.js';
import type { InferTagAttributes, TagAttributeSchema } from '../schema/types.js';
import { getSchemaFields } from '../schema/types.js';
import { createNextBuffer as createNextSpanBuffer } from '../spanBuffer.js';
import type { SpanBuffer } from '../types.js';
import type { GeneratedScope } from './scopeGenerator.js';

/**
 * Entry type constants for operation tracking
 */
const ENTRY_TYPE_INFO = 9;
const ENTRY_TYPE_DEBUG = 10;
const ENTRY_TYPE_WARN = 11;
const ENTRY_TYPE_ERROR = 12;

/**
 * SpanLogger interface with logging methods and schema-specific attribute setters.
 *
 * Extends ColumnWriter<T> which provides:
 * - _buffer, _writeIndex, nextRow(), _getNextBuffer()
 * - Fluent setter methods for each schema field
 *
 * SpanLogger adds:
 * - info/debug/warn/error logging methods
 * - Scope management (_getScope, _setScope)
 */
export type BaseSpanLogger<T extends TagAttributeSchema> = ColumnWriter<T> & {
  info(message: string): BaseSpanLogger<T>;
  debug(message: string): BaseSpanLogger<T>;
  warn(message: string): BaseSpanLogger<T>;
  error(message: string): BaseSpanLogger<T>;
  _getScope(): GeneratedScope;
  _setScope(attributes: Partial<InferTagAttributes<T>>): void;
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
 * Generate _setScope() method code - UNROLLED per-column with BULK null bitmap fill
 * Uses TypedArray.fill() for values and fillNullBitmapRange helper for null bitmaps
 */
function generateSetScopeMethod(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  const scopeUpdates = schemaFields.map(([fieldName]) => {
    return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        this._scope.${fieldName} = attributes.${fieldName};
      }`;
  });

  const columnFills = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = fieldName;
    const lmaoType = getLmaoSchemaType(fieldSchema);

    // Boolean uses bit-packed storage - bulk fill using helper
    if (lmaoType === 'boolean') {
      return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        helpers.fillBooleanBitmapRange(this._buffer.${columnName}_values, startIdx, endIdx, attributes.${fieldName});
        helpers.fillNullBitmapRange(this._buffer.${columnName}_nulls, startIdx, endIdx);
      }`;
    }

    // Value processing based on type
    let valueExpr = `attributes.${fieldName}`;
    if (lmaoType === 'enum' && enumFieldNames.has(fieldName)) {
      valueExpr = `getEnumIndex_${fieldName}(attributes.${fieldName})`;
    }

    // For string arrays (category/text), use manual loop instead of fill()
    if (lmaoType === 'category' || lmaoType === 'text') {
      return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        // Fill string array with manual loop
        const values = this._buffer.${columnName}_values;
        for (let i = startIdx; i < endIdx; i++) {
          values[i] = ${valueExpr};
        }

        // Bulk fill null bitmap using helper
        helpers.fillNullBitmapRange(this._buffer.${columnName}_nulls, startIdx, endIdx);
      }`;
    }

    return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        // Fill values with SIMD-friendly TypedArray.fill()
        this._buffer.${columnName}_values.fill(${valueExpr}, startIdx, endIdx);

        // Bulk fill null bitmap using helper
        helpers.fillNullBitmapRange(this._buffer.${columnName}_nulls, startIdx, endIdx);
      }`;
  });

  return `
    _setScope(attributes) {
      // Update the Scope instance (stores raw values, not interned)
      ${scopeUpdates.join('\n')}

      // Pre-fill remaining buffer capacity with scoped attributes
      const startIdx = this._writeIndex + 1;
      const endIdx = this._buffer._capacity;

      ${columnFills.join('\n')}
    }`;
}

/**
 * Generate _prefillScopedAttributes() method - UNROLLED per-column with BULK null bitmap fill
 */
function generatePrefillScopedAttributesMethod(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  const columnFills = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = fieldName;
    const lmaoType = getLmaoSchemaType(fieldSchema);

    // Boolean uses bit-packed storage - bulk fill using helper
    if (lmaoType === 'boolean') {
      return `
      {
        const scopeValue = this._scope.${fieldName};
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
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          // Fill string array with manual loop
          const values = this._buffer.${columnName}_values;
          for (let i = startIdx; i < endIdx; i++) {
            values[i] = scopeValue;
          }

          // Bulk fill null bitmap using helper
          helpers.fillNullBitmapRange(this._buffer.${columnName}_nulls, startIdx, endIdx);
        }
      }`;
    }

    return `
      {
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          // Fill values with SIMD-friendly TypedArray.fill()
          this._buffer.${columnName}_values.fill(${valueExpr}, startIdx, endIdx);

          // Bulk fill null bitmap using helper
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
 * Generate scope writes for logging methods - writes scoped attributes at current _writeIndex
 */
function generateScopeWritesForLogEntry(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  return schemaFields
    .map(([fieldName, fieldSchema]) => {
      const columnName = fieldName;
      const lmaoType = getLmaoSchemaType(fieldSchema);

      // Boolean uses bit-packed storage
      if (lmaoType === 'boolean') {
        return `
      {
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          const idx = this._writeIndex;
          const byteIndex = idx >>> 3;
          const bitOffset = idx & 7;
          if (scopeValue) {
            this._buffer.${columnName}_values[byteIndex] |= (1 << bitOffset);
          } else {
            this._buffer.${columnName}_values[byteIndex] &= ~(1 << bitOffset);
          }
          helpers.setNullBit(this._buffer.${columnName}_nulls, idx);
        }
      }`;
      }

      // Value processing based on type
      let valueExpr = 'scopeValue';
      if (lmaoType === 'enum' && enumFieldNames.has(fieldName)) {
        valueExpr = `getEnumIndex_${fieldName}(scopeValue)`;
      }

      return `
      {
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          const idx = this._writeIndex;
          this._buffer.${columnName}_values[idx] = ${valueExpr};
          helpers.setNullBit(this._buffer.${columnName}_nulls, idx);
        }
      }`;
    })
    .join('\n');
}

/**
 * Build the extension for SpanLogger that extends ColumnWriter
 */
function buildSpanLoggerExtension(schema: TagAttributeSchema): ColumnWriterExtension {
  const schemaFields = getSchemaFields(schema);

  // Collect enum mappings
  const enumMappings: string[] = [];
  const enumFieldNames = new Set<string>();

  for (const [fieldName, fieldSchema] of schemaFields) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const enumValues = getEnumValues(fieldSchema);

    if (lmaoType === 'enum' && enumValues) {
      enumMappings.push(generateEnumMapping(fieldName, enumValues));
      enumFieldNames.add(fieldName);
    }
  }

  // Generate methods
  const setScopeMethod = generateSetScopeMethod(schemaFields, enumFieldNames);
  const prefillMethod = generatePrefillScopedAttributesMethod(schemaFields, enumFieldNames);
  const scopeWrites = generateScopeWritesForLogEntry(schemaFields, enumFieldNames);

  return {
    constructorParams: 'scope, createNextBuffer',

    preamble: `
  // Inline getTimestampNanos for performance (zero function call overhead)
  function getTimestampNanos() {
    const epochMicros = Math.round((performance.timeOrigin + performance.now()) * 1000);
    return BigInt(epochMicros) * 1000n;
  }

  // Entry type constants
  const ENTRY_TYPE_INFO = ${ENTRY_TYPE_INFO};
  const ENTRY_TYPE_DEBUG = ${ENTRY_TYPE_DEBUG};
  const ENTRY_TYPE_WARN = ${ENTRY_TYPE_WARN};
  const ENTRY_TYPE_ERROR = ${ENTRY_TYPE_ERROR};

  ${enumMappings.join('\n')}
`,

    constructorCode: `
      // SpanLogger starts at writeIndex 1, so first nextRow() makes it 2
      // Row 0 = tag, Row 1 = result, Rows 2+ = log entries
      this._writeIndex = 1;
      this._scope = scope;
      this._createNextBuffer = createNextBuffer;
`,

    methods: `
    /**
     * Override _getNextBuffer to create new SpanBuffer on overflow.
     * Called by nextRow() when buffer is full.
     */
    _getNextBuffer() {
      const oldBuffer = this._buffer;
      const nextBuffer = this._createNextBuffer(oldBuffer);
      
      // Link the chain
      oldBuffer._next = nextBuffer;
      
      // Pre-fill new buffer with scoped attributes
      this._buffer = nextBuffer;
      this._prefillScopedAttributes();
      this._buffer = oldBuffer;
      
      return nextBuffer;
    }

    /**
     * Write an info log entry.
     * Advances to next row, writes system columns, returns this for fluent chaining.
     */
    info(message) {
      this.nextRow();
      const idx = this._writeIndex;

      // Write system columns
      this._buffer._timestamps[idx] = getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_INFO;

      // Write message to logMessage column
      if (this._buffer.logMessage_values) {
        this._buffer.logMessage_values[idx] = message;
        helpers.setNullBit(this._buffer.logMessage_nulls, idx);
      }

      // Apply scoped attributes
      ${scopeWrites}

      return this;
    }

    /**
     * Write a debug log entry.
     */
    debug(message) {
      this.nextRow();
      const idx = this._writeIndex;

      this._buffer._timestamps[idx] = getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_DEBUG;

      if (this._buffer.logMessage_values) {
        this._buffer.logMessage_values[idx] = message;
        helpers.setNullBit(this._buffer.logMessage_nulls, idx);
      }

      ${scopeWrites}

      return this;
    }

    /**
     * Write a warn log entry.
     */
    warn(message) {
      this.nextRow();
      const idx = this._writeIndex;

      this._buffer._timestamps[idx] = getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_WARN;

      if (this._buffer.logMessage_values) {
        this._buffer.logMessage_values[idx] = message;
        helpers.setNullBit(this._buffer.logMessage_nulls, idx);
      }

      ${scopeWrites}

      return this;
    }

    /**
     * Write an error log entry.
     */
    error(message) {
      this.nextRow();
      const idx = this._writeIndex;

      this._buffer._timestamps[idx] = getTimestampNanos();
      this._buffer._operations[idx] = ENTRY_TYPE_ERROR;

      if (this._buffer.logMessage_values) {
        this._buffer.logMessage_values[idx] = message;
        helpers.setNullBit(this._buffer.logMessage_nulls, idx);
      }

      ${scopeWrites}

      return this;
    }

    /**
     * Get the Scope instance directly.
     */
    _getScope() {
      return this._scope;
    }

    ${setScopeMethod}

    ${prefillMethod}
`,

    dependencies: {
      helpers: {
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
  TagAttributeSchema,
  new (
    buffer: SpanBuffer,
    scope: GeneratedScope,
    createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
  ) => BaseSpanLogger<TagAttributeSchema>
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
export function createSpanLoggerClass<T extends TagAttributeSchema>(
  schema: T,
): new (
  buffer: SpanBuffer,
  scope: GeneratedScope,
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
      scope: GeneratedScope,
      createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
    ) => BaseSpanLogger<TagAttributeSchema>;

    spanLoggerClassCache.set(schema, SpanLoggerClass);
  }

  return SpanLoggerClass as new (
    buffer: SpanBuffer,
    scope: GeneratedScope,
    createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
  ) => BaseSpanLogger<T>;
}

/**
 * Create a SpanLogger instance for the given buffer and scope.
 *
 * @param schema - Tag attribute schema
 * @param buffer - SpanBuffer to write to
 * @param scope - Scope instance for scoped attributes
 * @param createNextBuffer - Function to create next buffer on overflow (defaults to createNextSpanBuffer)
 */
export function createSpanLogger<T extends TagAttributeSchema>(
  schema: T,
  buffer: SpanBuffer,
  scope: GeneratedScope,
  createNextBuffer: (buffer: SpanBuffer) => SpanBuffer = createNextSpanBuffer,
): BaseSpanLogger<T> {
  const SpanLoggerClass = createSpanLoggerClass(schema);
  return new SpanLoggerClass(buffer, scope, createNextBuffer);
}

// Re-export types that may be needed by consumers
export type { GeneratedScope } from './scopeGenerator.js';

// ============================================================================
// Backward compatibility types
// These are kept for library.ts until it's updated for the new design
// ============================================================================

/**
 * @deprecated Use raw strings instead - category/text columns store strings directly
 */
export interface StringInterner {
  intern(str: string): number;
  getString(idx: number): string | undefined;
  getStrings(): readonly string[];
  size(): number;
}

/**
 * @deprecated Use raw strings instead - text columns store strings directly
 */
export interface TextStorage {
  store(str: string): number;
  getString(idx: number): string | undefined;
  getStrings(): readonly string[];
}

/**
 * @deprecated Use createNextBuffer directly via ColumnWriter overflow handling.
 * Kept for backward compatibility with library.ts.
 */
export type GetBufferWithSpaceFn = (buffer: SpanBuffer) => { buffer: SpanBuffer; didOverflow: boolean };
