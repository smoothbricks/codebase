/**
 * Runtime code generation for SpanLogger classes
 *
 * Per specs/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
 * - Uses new Function() to generate optimized classes at module creation time (cold path)
 * - Methods added to prototype for zero-overhead access (hot path)
 * - Compile-time enum mapping for 1-byte storage
 * - Direct buffer writes without intermediate objects
 */

import type { Microseconds } from '@smoothbricks/arrow-builder';
import { getEnumValues, getLmaoSchemaType } from '../schema/typeGuards.js';
import type { InferTagAttributes, TagAttributeSchema } from '../schema/types.js';
import { getSchemaFields } from '../schema/types.js';
import type { SpanBuffer } from '../types.js';
import type { GeneratedScope } from './scopeGenerator.js';

/**
 * String interner interface for category columns
 */
export interface StringInterner {
  intern(str: string): number;
  getString(idx: number): string | undefined;
  getStrings(): readonly string[];
  size(): number;
}

/**
 * Text storage interface for text columns
 */
export interface TextStorage {
  store(str: string): number;
  getString(idx: number): string | undefined;
  getStrings(): readonly string[];
}

/**
 * Buffer with space result
 */
export interface BufferWithSpace {
  buffer: SpanBuffer;
  didOverflow: boolean;
}

/**
 * Get buffer with space function type
 */
export type GetBufferWithSpaceFn = (buffer: SpanBuffer) => BufferWithSpace;

/**
 * Chainable tag API type (same as in lmao.ts)
 */
export type ChainableTagAPI<T extends TagAttributeSchema> = {
  with(attributes: Partial<InferTagAttributes<T>>): ChainableTagAPI<T>;
} & {
  [K in keyof InferTagAttributes<T>]: (value: InferTagAttributes<T>[K]) => ChainableTagAPI<T>;
};

/**
 * Base SpanLogger interface with core methods
 */
export interface BaseSpanLogger<T extends TagAttributeSchema> {
  readonly tag: ChainableTagAPI<T>;
  message(level: 'info' | 'debug' | 'warn' | 'error', message: string): void;
  info(message: string): void;
  debug(message: string): void;
  warn(message: string): void;
  error(message: string): void;
  scope(attributes: Partial<InferTagAttributes<T>>): void;
  _getScope(): GeneratedScope;
}

/**
 * Entry type constants for operation tracking
 * Note: span-start entry type is set at buffer creation time,
 * and ctx.tag.* writes to row 0 (overwrite semantics)
 */
const ENTRY_TYPE_INFO = 9;
const ENTRY_TYPE_DEBUG = 10;
const ENTRY_TYPE_WARN = 11;
const ENTRY_TYPE_ERROR = 12;

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
 * Generate write function for a single attribute
 * Handles enum/category/text types with appropriate storage
 *
 * IMPORTANT: ctx.tag.* methods ALWAYS write to row 0 (span-start row)
 * with overwrite semantics (last write wins, like Datadog)
 */
function generateAttributeWriter(fieldName: string, schema: unknown, hasEnumMapping: boolean): string {
  const columnName = `attr_${fieldName}`;
  const lmaoType = getLmaoSchemaType(schema);

  // For enums, use pre-generated mapping function
  if (lmaoType === 'enum' && hasEnumMapping) {
    return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      const enumIndex = getEnumIndex_${fieldName}(value);
      this._buffer.${columnName}_values[idx] = enumIndex;

      // Mark as non-null (bit 0 for idx 0)
      const nullBitmap = this._buffer.${columnName}_nulls;
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }

      return this;
    }`;
  }

  // For categories, write raw string (no interning on hot path)
  if (lmaoType === 'category') {
    return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      this._buffer.${columnName}_values[idx] = value;

      // Mark as non-null (bit 0 for idx 0)
      const nullBitmap = this._buffer.${columnName}_nulls;
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }

      return this;
    }`;
  }

  // For text, write raw string (no interning on hot path)
  if (lmaoType === 'text') {
    return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      const nullBitmap = this._buffer.${columnName}_nulls;

      // Handle null/undefined: clear null bitmap bit and return early
      if (value === null || value === undefined) {
        if (nullBitmap) {
          nullBitmap[0] &= ~1;  // Clear bit 0
        }
        return this;
      }

      // Store the text value (raw string, no interning)
      this._buffer.${columnName}_values[idx] = value;

      // Mark as non-null (bit 0 for idx 0)
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }

      return this;
    }`;
  }

  // Boolean: bit-packed storage (8 values per byte)
  if (lmaoType === 'boolean') {
    return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      // Bit-packed: bit 0 of byte 0 is index 0
      if (value) {
        this._buffer.${columnName}_values[0] |= 1;
      } else {
        this._buffer.${columnName}_values[0] &= ~1;
      }

      // Mark as non-null (bit 0 for idx 0)
      const nullBitmap = this._buffer.${columnName}_nulls;
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }

      return this;
    }`;
  }

  // Generic writer for other types (number)
  return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      this._buffer.${columnName}_values[idx] = value;

      // Mark as non-null (bit 0 for idx 0)
      const nullBitmap = this._buffer.${columnName}_nulls;
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }

      return this;
    }`;
}

/**
 * Generate with() method code - UNROLLED per-column code
 * No Object.entries at runtime - each column gets explicit code
 */
function generateWithMethod(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  const columnWrites = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = `attr_${fieldName}`;
    const lmaoType = getLmaoSchemaType(fieldSchema);

    // Boolean uses bit-packed storage (8 values per byte)
    if (lmaoType === 'boolean') {
      return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        // Bit-packed boolean write at index 0
        if (attributes.${fieldName}) {
          this._buffer.${columnName}_values[0] |= 1;
        } else {
          this._buffer.${columnName}_values[0] &= ~1;
        }
        this._buffer.${columnName}_nulls[0] |= 1;
      }`;
    }

    // Value processing based on type
    let valueExpr = `attributes.${fieldName}`;
    if (lmaoType === 'enum' && enumFieldNames.has(fieldName)) {
      valueExpr = `getEnumIndex_${fieldName}(attributes.${fieldName})`;
    }
    // category and text: write raw strings (no interning on hot path)

    return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        this._buffer.${columnName}_values[0] = ${valueExpr};
        this._buffer.${columnName}_nulls[0] |= 1;
      }`;
  });

  return `
    with(attributes) {
      ${columnWrites.join('\n')}
      return this;
    }`;
}

/**
 * Generate scope() method code - UNROLLED per-column with BULK null bitmap fill
 * Uses TypedArray.fill() for values and bulk 0xFF writes for null bitmaps
 */
function generateScopeMethod(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  const scopeUpdates = schemaFields.map(([fieldName]) => {
    return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        this._scope.${fieldName} = attributes.${fieldName};
      }`;
  });

  const columnFills = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = `attr_${fieldName}`;
    const lmaoType = getLmaoSchemaType(fieldSchema);

    // Boolean uses bit-packed storage - bulk fill with 0xFF or 0x00
    if (lmaoType === 'boolean') {
      return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        // Bit-packed boolean bulk fill
        const values = this._buffer.${columnName}_values;
        const fillByte = attributes.${fieldName} ? 0xFF : 0x00;
        const startByte = startIdx >>> 3;
        const endByte = (endIdx - 1) >>> 3;

        // Fill full bytes
        for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
          if (byteIdx === startByte && (startIdx & 7) !== 0) {
            // Partial first byte
            const mask = 0xFF << (startIdx & 7);
            if (attributes.${fieldName}) {
              values[byteIdx] |= mask;
            } else {
              values[byteIdx] &= ~mask;
            }
          } else if (byteIdx === endByte && (endIdx & 7) !== 0) {
            // Partial last byte
            const mask = (1 << (endIdx & 7)) - 1;
            if (attributes.${fieldName}) {
              values[byteIdx] |= mask;
            } else {
              values[byteIdx] &= ~mask;
            }
          } else {
            values[byteIdx] = fillByte;
          }
        }

        // Bulk fill null bitmap
        const nullBitmap = this._buffer.${columnName}_nulls;
        for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
          if (byteIdx === startByte && (startIdx & 7) !== 0) {
            nullBitmap[byteIdx] |= (0xFF << (startIdx & 7));
          } else if (byteIdx === endByte && (endIdx & 7) !== 0) {
            nullBitmap[byteIdx] |= ((1 << (endIdx & 7)) - 1);
          } else {
            nullBitmap[byteIdx] = 0xFF;
          }
        }
      }`;
    }

    // Value processing based on type
    let valueExpr = `attributes.${fieldName}`;
    if (lmaoType === 'enum' && enumFieldNames.has(fieldName)) {
      valueExpr = `getEnumIndex_${fieldName}(attributes.${fieldName})`;
    }
    // category and text: write raw strings (no interning on hot path)

    // For string arrays (category/text), use manual loop instead of fill()
    if (lmaoType === 'category' || lmaoType === 'text') {
      return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        // Fill string array with manual loop
        const values = this._buffer.${columnName}_values;
        for (let i = startIdx; i < endIdx; i++) {
          values[i] = ${valueExpr};
        }

        // Bulk fill null bitmap
        const nullBitmap = this._buffer.${columnName}_nulls;
        const startByte = Math.floor(startIdx / 8);
        const endByte = Math.floor((endIdx - 1) / 8);

        for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
          if (byteIdx === startByte && startIdx % 8 !== 0) {
            nullBitmap[byteIdx] |= (0xFF << (startIdx % 8));
          } else if (byteIdx === endByte && endIdx % 8 !== 0) {
            nullBitmap[byteIdx] |= ((1 << (endIdx % 8)) - 1);
          } else {
            nullBitmap[byteIdx] = 0xFF;
          }
        }
      }`;
    }

    return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        // Fill values with SIMD-friendly TypedArray.fill()
        this._buffer.${columnName}_values.fill(${valueExpr}, startIdx, endIdx);

        // Bulk fill null bitmap: full bytes get 0xFF, partial last byte gets individual bits
        const nullBitmap = this._buffer.${columnName}_nulls;
        const startByte = Math.floor(startIdx / 8);
        const endByte = Math.floor((endIdx - 1) / 8);

        // Set full bytes to 0xFF (all bits valid)
        for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
          // For partial first byte, only set bits >= startIdx % 8
          if (byteIdx === startByte && startIdx % 8 !== 0) {
            nullBitmap[byteIdx] |= (0xFF << (startIdx % 8));
          }
          // For partial last byte, only set bits < endIdx % 8 (or all if endIdx % 8 === 0)
          else if (byteIdx === endByte && endIdx % 8 !== 0) {
            nullBitmap[byteIdx] |= ((1 << (endIdx % 8)) - 1);
          }
          // Full byte - set all bits
          else {
            nullBitmap[byteIdx] = 0xFF;
          }
        }
      }`;
  });

  return `
    scope(attributes) {
      // Update the Scope instance (stores raw values, not interned)
      ${scopeUpdates.join('\n')}

      // Pre-fill remaining buffer capacity with scoped attributes
      const startIdx = this._buffer.writeIndex;
      const endIdx = this._buffer.capacity;

      ${columnFills.join('\n')}
    }`;
}

/**
 * Generate _prefillScopedAttributes() method - UNROLLED per-column with BULK null bitmap fill
 */
function generatePrefillScopedAttributesMethod(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  const columnFills = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = `attr_${fieldName}`;
    const lmaoType = getLmaoSchemaType(fieldSchema);

    // Boolean uses bit-packed storage
    if (lmaoType === 'boolean') {
      return `
      {
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          // Bit-packed boolean bulk fill
          const values = this._buffer.${columnName}_values;
          const fillByte = scopeValue ? 0xFF : 0x00;
          const startByte = startIdx >>> 3;
          const endByte = (endIdx - 1) >>> 3;

          for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
            if (byteIdx === startByte && (startIdx & 7) !== 0) {
              const mask = 0xFF << (startIdx & 7);
              if (scopeValue) {
                values[byteIdx] |= mask;
              } else {
                values[byteIdx] &= ~mask;
              }
            } else if (byteIdx === endByte && (endIdx & 7) !== 0) {
              const mask = (1 << (endIdx & 7)) - 1;
              if (scopeValue) {
                values[byteIdx] |= mask;
              } else {
                values[byteIdx] &= ~mask;
              }
            } else {
              values[byteIdx] = fillByte;
            }
          }

          // Bulk fill null bitmap
          const nullBitmap = this._buffer.${columnName}_nulls;
          for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
            if (byteIdx === startByte && (startIdx & 7) !== 0) {
              nullBitmap[byteIdx] |= (0xFF << (startIdx & 7));
            } else if (byteIdx === endByte && (endIdx & 7) !== 0) {
              nullBitmap[byteIdx] |= ((1 << (endIdx & 7)) - 1);
            } else {
              nullBitmap[byteIdx] = 0xFF;
            }
          }
        }
      }`;
    }

    // Value processing based on type
    let valueExpr = 'scopeValue';
    if (lmaoType === 'enum' && enumFieldNames.has(fieldName)) {
      valueExpr = `getEnumIndex_${fieldName}(scopeValue)`;
    }
    // category and text: write raw strings (no interning on hot path)

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

          // Bulk fill null bitmap
          const nullBitmap = this._buffer.${columnName}_nulls;
          const startByte = Math.floor(startIdx / 8);
          const endByte = Math.floor((endIdx - 1) / 8);

          for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
            if (byteIdx === startByte && startIdx % 8 !== 0) {
              nullBitmap[byteIdx] |= (0xFF << (startIdx % 8));
            } else if (byteIdx === endByte && endIdx % 8 !== 0) {
              nullBitmap[byteIdx] |= ((1 << (endIdx % 8)) - 1);
            } else {
              nullBitmap[byteIdx] = 0xFF;
            }
          }
        }
      }`;
    }

    return `
      {
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          // Fill values with SIMD-friendly TypedArray.fill()
          this._buffer.${columnName}_values.fill(${valueExpr}, startIdx, endIdx);

          // Bulk fill null bitmap
          const nullBitmap = this._buffer.${columnName}_nulls;
          const startByte = Math.floor(startIdx / 8);
          const endByte = Math.floor((endIdx - 1) / 8);

          for (let byteIdx = startByte; byteIdx <= endByte; byteIdx++) {
            if (byteIdx === startByte && startIdx % 8 !== 0) {
              nullBitmap[byteIdx] |= (0xFF << (startIdx % 8));
            } else if (byteIdx === endByte && endIdx % 8 !== 0) {
              nullBitmap[byteIdx] |= ((1 << (endIdx % 8)) - 1);
            } else {
              nullBitmap[byteIdx] = 0xFF;
            }
          }
        }
      }`;
  });

  return `
    _prefillScopedAttributes() {
      const startIdx = this._buffer.writeIndex;
      const endIdx = this._buffer.capacity;

      ${columnFills.join('\n')}
    }`;
}

/**
 * Generate _writeMessage() method - UNROLLED per-column scope writes
 */
function generateWriteMessageMethod(schemaFields: [string, unknown][], enumFieldNames: Set<string>): string {
  const scopeWrites = schemaFields.map(([fieldName, fieldSchema]) => {
    const columnName = `attr_${fieldName}`;
    const lmaoType = getLmaoSchemaType(fieldSchema);

    // Boolean uses bit-packed storage
    if (lmaoType === 'boolean') {
      return `
      {
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          const byteIndex = idx >>> 3;
          const bitOffset = idx & 7;
          // Bit-packed boolean write
          if (scopeValue) {
            this._buffer.${columnName}_values[byteIndex] |= (1 << bitOffset);
          } else {
            this._buffer.${columnName}_values[byteIndex] &= ~(1 << bitOffset);
          }
          this._buffer.${columnName}_nulls[byteIndex] |= (1 << bitOffset);
        }
      }`;
    }

    // Value processing based on type
    let valueExpr = 'scopeValue';
    if (lmaoType === 'enum' && enumFieldNames.has(fieldName)) {
      valueExpr = `getEnumIndex_${fieldName}(scopeValue)`;
    }
    // category and text: write raw strings (no interning on hot path)

    return `
      {
        const scopeValue = this._scope.${fieldName};
        if (scopeValue !== null && scopeValue !== undefined) {
          this._buffer.${columnName}_values[idx] = ${valueExpr};
          const byteIndex = idx >>> 3;  // Math.floor(idx / 8)
          const bitOffset = idx & 7;     // idx % 8
          this._buffer.${columnName}_nulls[byteIndex] |= (1 << bitOffset);
        }
      }`;
  });

  return `
    _writeMessage(entryType, message) {
      const result = this._getBufferWithSpace(this._buffer);

      // If overflow occurred, pre-fill new buffer with scoped attributes
      if (result.didOverflow) {
        this._buffer = result.buffer;
        this._prefillScopedAttributes();
      } else {
        this._buffer = result.buffer;
      }

      const idx = this._buffer.writeIndex;

      // Write entry type
      this._buffer.operations[idx] = entryType;

      // Write timestamp using anchor for high precision (microseconds)
      this._buffer.timestamps[idx] = getTimestampMicros(this._anchorEpochMicros, this._anchorPerfNow);

      // Write message to attr_logMessage column (raw string, no interning)
      const messageColumn = this._buffer.attr_logMessage_values;
      if (messageColumn) {
        messageColumn[idx] = message;
        const byteIndex = idx >>> 3;
        const bitOffset = idx & 7;
        this._buffer.attr_logMessage_nulls[byteIndex] |= (1 << bitOffset);
      }

      // Apply scoped attributes - UNROLLED per-column
      ${scopeWrites.join('\n')}

      // Increment write index
      this._buffer.writeIndex++;
    }`;
}

/**
 * Generate complete SpanLogger class code
 * Returns executable JavaScript code as a string
 */
export function generateSpanLoggerClass<T extends TagAttributeSchema>(
  schema: T,
  className = 'GeneratedSpanLogger',
): string {
  // Get schema fields, excluding methods added by defineTagAttributes
  const schemaFields = getSchemaFields(schema);

  // Generate enum mapping functions
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

  // Generate attribute writer methods
  const attributeWriters = schemaFields.map(([fieldName, fieldSchema]) =>
    generateAttributeWriter(fieldName, fieldSchema, enumFieldNames.has(fieldName)),
  );

  // Generate unrolled methods (NO Object.entries at runtime)
  const withMethod = generateWithMethod(schemaFields, enumFieldNames);
  const scopeMethod = generateScopeMethod(schemaFields, enumFieldNames);
  const prefillMethod = generatePrefillScopedAttributesMethod(schemaFields, enumFieldNames);
  const writeMessageMethod = generateWriteMessageMethod(schemaFields, enumFieldNames);

  // Generate the complete class
  const classCode = `
(function() {
  'use strict';

  // Inline getTimestampMicros for performance (zero function call overhead)
  // Uses performance.now() which is browser-safe and provides sub-millisecond precision
  function getTimestampMicros(anchorEpochMicros, anchorPerfNow) {
    return anchorEpochMicros + (performance.now() * 1000 - anchorPerfNow);
  }

  ${enumMappings.join('\n')}

  class ${className} {
    constructor(buffer, getBufferWithSpace, anchorEpochMicros, anchorPerfNow, scopeInstance) {
      this._buffer = buffer;
      this._getBufferWithSpace = getBufferWithSpace;
      this._anchorEpochMicros = anchorEpochMicros;
      this._anchorPerfNow = anchorPerfNow;
      this._scope = scopeInstance;
    }

    // Tag getter - returns chainable API that writes to row 0 (span-start)
    // Note: No overflow check needed - row 0 always exists
    // Note: No writeIndex increment - always writing to same row (overwrite semantics)
    get tag() {
      // Entry type for row 0 is already span-start (set at buffer creation)
      // Just return this for chaining - individual methods write to row 0
      return this;
    }

    // with() method for bulk attribute setting - writes to row 0
    // UNROLLED: Each column gets explicit code, no Object.entries iteration
    ${withMethod}

    // Attribute writer methods (generated from schema)
    ${attributeWriters.join('\n')}

    // Scoped attributes - writes to Scope instance and pre-fills buffer
    // UNROLLED: Each column gets explicit TypedArray.fill() + bulk null bitmap
    ${scopeMethod}

    /**
     * Pre-fill buffer with scoped attributes from current writeIndex to capacity.
     * Called after buffer overflow to propagate scoped attributes to the new buffer.
     *
     * UNROLLED: Each column gets explicit code with bulk null bitmap operations
     */
    ${prefillMethod}

    // Message logging methods
    // UNROLLED: Scope attribute writes are explicit per-column, no Object.entries
    ${writeMessageMethod}
    
    message(level, message) {
      const entryTypeMap = {
        'info': ${ENTRY_TYPE_INFO},
        'debug': ${ENTRY_TYPE_DEBUG},
        'warn': ${ENTRY_TYPE_WARN},
        'error': ${ENTRY_TYPE_ERROR}
      };
      this._writeMessage(entryTypeMap[level] || ${ENTRY_TYPE_INFO}, message);
    }
    
    info(message) {
      this._writeMessage(${ENTRY_TYPE_INFO}, message);
    }
    
    debug(message) {
      this._writeMessage(${ENTRY_TYPE_DEBUG}, message);
    }
    
    warn(message) {
      this._writeMessage(${ENTRY_TYPE_WARN}, message);
    }
    
    error(message) {
      this._writeMessage(${ENTRY_TYPE_ERROR}, message);
    }

    // Get the Scope instance directly
    _getScope() {
      return this._scope;
    }
  }

  return ${className};
})()
`;

  return classCode;
}

/**
 * Create SpanLogger class constructor from schema
 * This is the cold-path function called at module creation time
 */
export function createSpanLoggerClass<T extends TagAttributeSchema>(
  schema: T,
): new (
  buffer: SpanBuffer,
  getBufferWithSpace: GetBufferWithSpaceFn,
  anchorEpochMicros: Microseconds,
  anchorPerfNow: Microseconds,
  scopeInstance: GeneratedScope,
) => BaseSpanLogger<T> {
  const classCode = generateSpanLoggerClass(schema).trim();

  // Use Function constructor to create the class
  // This is safe because we control the code generation
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const GeneratedClass = new Function(`return ${classCode}`)();

  return GeneratedClass as new (
    buffer: SpanBuffer,
    getBufferWithSpace: GetBufferWithSpaceFn,
    anchorEpochMicros: Microseconds,
    anchorPerfNow: Microseconds,
    scopeInstance: GeneratedScope,
  ) => BaseSpanLogger<T>;
}
