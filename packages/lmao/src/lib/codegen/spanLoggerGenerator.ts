/**
 * Runtime code generation for SpanLogger classes
 *
 * Per specs/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
 * - Uses new Function() to generate optimized classes at module creation time (cold path)
 * - Methods added to prototype for zero-overhead access (hot path)
 * - Compile-time enum mapping for 1-byte storage
 * - Direct buffer writes without intermediate objects
 */

import { getEnumValues, getLmaoSchemaType } from '../schema/typeGuards.js';
import type { InferTagAttributes, TagAttributeSchema } from '../schema/types.js';
import { getSchemaFields } from '../schema/types.js';
import type { SpanBuffer } from '../types.js';

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
  getScopedAttributes(): Record<string, unknown>;
}

/**
 * Entry type constants for operation tracking
 * Note: ENTRY_TYPE_TAG (3) is no longer used here - span-start entry type
 * is set at buffer creation time, and ctx.tag.* writes to row 0 (overwrite semantics)
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
      this._buffer.${columnName}[idx] = enumIndex;
      
      // Mark as non-null (bit 0 for idx 0)
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }
      
      return this;
    }`;
  }

  // For categories, use string interning
  if (lmaoType === 'category') {
    return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      this._buffer.${columnName}[idx] = this._categoryInterner.intern(value);
      
      // Mark as non-null (bit 0 for idx 0)
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }
      
      return this;
    }`;
  }

  // For text, use raw storage with null/undefined handling
  if (lmaoType === 'text') {
    return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      
      // Handle null/undefined: clear null bitmap bit and return early
      if (value === null || value === undefined) {
        if (nullBitmap) {
          nullBitmap[0] &= ~1;  // Clear bit 0
        }
        return this;
      }
      
      // Store the text value
      this._buffer.${columnName}[idx] = this._textStorage.store(value);
      
      // Mark as non-null (bit 0 for idx 0)
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }
      
      return this;
    }`;
  }

  // Generic writer for other types (number, boolean)
  return `
    ${fieldName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      this._buffer.${columnName}[idx] = value;
      
      // Mark as non-null (bit 0 for idx 0)
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      if (nullBitmap) {
        nullBitmap[0] |= 1;
      }
      
      return this;
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

  // Create schema type map for runtime type detection in scope()
  const schemaTypeMap = Object.fromEntries(
    schemaFields.map(([fieldName, fieldSchema]) => {
      return [fieldName, getLmaoSchemaType(fieldSchema) || 'unknown'];
    }),
  );

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
  
  // Schema type map for runtime type detection
  const SCHEMA_TYPES = ${JSON.stringify(schemaTypeMap)};
  
  class ${className} {
    constructor(buffer, categoryInterner, textStorage, getBufferWithSpace, anchorEpochMicros, anchorPerfNow, initialScopedAttributes = {}) {
      this._buffer = buffer;
      this._categoryInterner = categoryInterner;
      this._textStorage = textStorage;
      this._getBufferWithSpace = getBufferWithSpace;
      this._anchorEpochMicros = anchorEpochMicros;
      this._anchorPerfNow = anchorPerfNow;
      this._scopedAttributes = initialScopedAttributes;
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
    with(attributes) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      const idx = 0;
      
      for (const [key, value] of Object.entries(attributes)) {
        const columnName = 'attr_' + key;
        const column = this._buffer[columnName];
        if (column && value !== null && value !== undefined) {
          column[idx] = value;
          
          // Mark as non-null (bit 0 for idx 0)
          const nullBitmap = this._buffer.nullBitmaps[columnName];
          if (nullBitmap) {
            nullBitmap[0] |= 1;
          }
        }
      }
      
      return this;
    }
    
    // Attribute writer methods (generated from schema)
    ${attributeWriters.join('\n')}
    
    // Scoped attributes
    scope(attributes) {
      // Store scoped attributes (already interned/processed)
      for (const [key, value] of Object.entries(attributes)) {
        const columnName = 'attr_' + key;
        const column = this._buffer[columnName];
        
        if (column && value !== null && value !== undefined) {
          // Process value based on schema type (not TypedArray type)
          let processedValue = value;
          const fieldType = SCHEMA_TYPES[key];
          
          if (typeof value === 'string') {
            // Category: intern the string
            if (fieldType === 'category') {
              processedValue = this._categoryInterner.intern(value);
            }
            // Text: store without interning
            else if (fieldType === 'text') {
              processedValue = this._textStorage.store(value);
            }
            // Enum: should use the enum method instead, but handle gracefully
            // (this shouldn't happen in normal usage since enums are compile-time)
          }
          
          this._scopedAttributes[key] = processedValue;
        }
      }
      
      // Pre-fill remaining buffer capacity with scoped attributes
      const startIdx = this._buffer.writeIndex;
      const endIdx = this._buffer.capacity;
      
      for (let idx = startIdx; idx < endIdx; idx++) {
        for (const [key, value] of Object.entries(this._scopedAttributes)) {
          const columnName = 'attr_' + key;
          const column = this._buffer[columnName];
          if (column) {
            column[idx] = value;
            
            // Mark as non-null
            const nullBitmap = this._buffer.nullBitmaps[columnName];
            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] |= (1 << bitOffset);
            }
          }
        }
      }
    }
    
    /**
     * Pre-fill buffer with scoped attributes from current writeIndex to capacity.
     * Called after buffer overflow to propagate scoped attributes to the new buffer.
     * 
     * Per specs/01i_span_scope_attributes.md:
     * - Scoped attributes propagate to all entries in a span
     * - When overflow creates a new chained buffer, it must be pre-filled
     */
    _prefillScopedAttributes() {
      const startIdx = this._buffer.writeIndex;
      const endIdx = this._buffer.capacity;
      
      for (let idx = startIdx; idx < endIdx; idx++) {
        for (const [key, value] of Object.entries(this._scopedAttributes)) {
          const columnName = 'attr_' + key;
          const column = this._buffer[columnName];
          if (column) {
            column[idx] = value;
            
            // Mark as non-null
            const nullBitmap = this._buffer.nullBitmaps[columnName];
            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] |= (1 << bitOffset);
            }
          }
        }
      }
    }
    
    // Message logging methods
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
      
      // Write message to attr_logMessage column
      // Named logMessage (not message) to avoid conflict with SpanLogger.message() method
      // Converted to 'message' column in Arrow output per specs/01f_arrow_table_structure.md
      const messageColumn = this._buffer.attr_logMessage;
      if (messageColumn) {
        messageColumn[idx] = this._textStorage.store(message);
        
        // Mark as non-null
        const nullBitmap = this._buffer.nullBitmaps.attr_logMessage;
        if (nullBitmap) {
          const byteIndex = Math.floor(idx / 8);
          const bitOffset = idx % 8;
          nullBitmap[byteIndex] |= (1 << bitOffset);
        }
      }
      
      // Apply scoped attributes
      for (const [key, value] of Object.entries(this._scopedAttributes)) {
        const columnName = 'attr_' + key;
        const column = this._buffer[columnName];
        if (column) {
          column[idx] = value;
          
          // Mark as non-null
          const nullBitmap = this._buffer.nullBitmaps[columnName];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }
      
      // Increment write index
      this._buffer.writeIndex++;
    }
    
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
    
    // Get scoped attributes for inheritance
    getScopedAttributes() {
      return this._scopedAttributes;
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
  categoryInterner: StringInterner,
  textStorage: TextStorage,
  getBufferWithSpace: GetBufferWithSpaceFn,
  anchorEpochMicros: number,
  anchorPerfNow: number,
  initialScopedAttributes?: Record<string, unknown>,
) => BaseSpanLogger<T> {
  const classCode = generateSpanLoggerClass(schema).trim();

  // Use Function constructor to create the class
  // This is safe because we control the code generation
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const GeneratedClass = new Function(`return ${classCode}`)();

  return GeneratedClass as new (
    buffer: SpanBuffer,
    categoryInterner: StringInterner,
    textStorage: TextStorage,
    getBufferWithSpace: GetBufferWithSpaceFn,
    anchorEpochMicros: number,
    anchorPerfNow: number,
    initialScopedAttributes?: Record<string, unknown>,
  ) => BaseSpanLogger<T>;
}
