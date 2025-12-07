/**
 * Runtime code generation for SpanLogger classes
 * 
 * Per specs/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
 * - Uses new Function() to generate optimized classes at module creation time (cold path)
 * - Methods added to prototype for zero-overhead access (hot path)
 * - Compile-time enum mapping for 1-byte storage
 * - Direct buffer writes without intermediate objects
 */

import type { TagAttributeSchema, InferTagAttributes } from '../schema/types.js';
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
 */
const ENTRY_TYPE_TAG = 3;
const ENTRY_TYPE_INFO = 9;
const ENTRY_TYPE_DEBUG = 10;
const ENTRY_TYPE_WARN = 11;
const ENTRY_TYPE_ERROR = 12;

/**
 * Generate enum value mapping code
 * Creates a switch-case statement for compile-time enum mapping
 */
function generateEnumMapping(fieldName: string, enumValues: readonly string[]): string {
  const cases = enumValues.map((value, index) => 
    `    case ${JSON.stringify(value)}: return ${index};`
  ).join('\n');
  
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
 */
function generateAttributeWriter(
  fieldName: string,
  schema: unknown,
  hasEnumMapping: boolean
): string {
  const columnName = `attr_${fieldName}`;
  const schemaWithMetadata = schema as { __lmao_type?: string };
  const lmaoType = schemaWithMetadata.__lmao_type;
  
  // For enums, use pre-generated mapping function
  if (lmaoType === 'enum' && hasEnumMapping) {
    return `
    ${fieldName}(value) {
      const idx = this._currentTagIndex !== null ? this._currentTagIndex : this._buffer.writeIndex;
      const enumIndex = getEnumIndex_${fieldName}(value);
      this._buffer.${columnName}[idx] = enumIndex;
      
      // Mark as non-null
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      if (nullBitmap) {
        const byteIndex = Math.floor(idx / 8);
        const bitOffset = idx % 8;
        nullBitmap[byteIndex] |= (1 << bitOffset);
      }
      
      return this;
    }`;
  }
  
  // For categories, use string interning
  if (lmaoType === 'category') {
    return `
    ${fieldName}(value) {
      const idx = this._currentTagIndex !== null ? this._currentTagIndex : this._buffer.writeIndex;
      this._buffer.${columnName}[idx] = this._categoryInterner.intern(value);
      
      // Mark as non-null
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      if (nullBitmap) {
        const byteIndex = Math.floor(idx / 8);
        const bitOffset = idx % 8;
        nullBitmap[byteIndex] |= (1 << bitOffset);
      }
      
      return this;
    }`;
  }
  
  // For text, use raw storage
  if (lmaoType === 'text') {
    return `
    ${fieldName}(value) {
      const idx = this._currentTagIndex !== null ? this._currentTagIndex : this._buffer.writeIndex;
      this._buffer.${columnName}[idx] = this._textStorage.store(value);
      
      // Mark as non-null
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      if (nullBitmap) {
        const byteIndex = Math.floor(idx / 8);
        const bitOffset = idx % 8;
        nullBitmap[byteIndex] |= (1 << bitOffset);
      }
      
      return this;
    }`;
  }
  
  // Generic writer for other types
  return `
    ${fieldName}(value) {
      const idx = this._currentTagIndex !== null ? this._currentTagIndex : this._buffer.writeIndex;
      this._buffer.${columnName}[idx] = value;
      
      // Mark as non-null
      const nullBitmap = this._buffer.nullBitmaps.${columnName};
      if (nullBitmap) {
        const byteIndex = Math.floor(idx / 8);
        const bitOffset = idx % 8;
        nullBitmap[byteIndex] |= (1 << bitOffset);
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
  className: string = 'GeneratedSpanLogger'
): string {
  // Get schema fields, excluding methods added by defineTagAttributes
  const schemaFields = getSchemaFields(schema);
  
  // Generate enum mapping functions
  const enumMappings: string[] = [];
  const enumFieldNames = new Set<string>();
  
  for (const [fieldName, fieldSchema] of schemaFields) {
    const schemaWithMetadata = fieldSchema as { 
      __lmao_type?: string; 
      __lmao_enum_values?: readonly string[]; 
    };
    
    if (schemaWithMetadata.__lmao_type === 'enum' && schemaWithMetadata.__lmao_enum_values) {
      enumMappings.push(generateEnumMapping(fieldName, schemaWithMetadata.__lmao_enum_values));
      enumFieldNames.add(fieldName);
    }
  }
  
  // Generate attribute writer methods
  const attributeWriters = schemaFields.map(([fieldName, fieldSchema]) =>
    generateAttributeWriter(fieldName, fieldSchema, enumFieldNames.has(fieldName))
  );
  
  // Create schema type map for runtime type detection in scope()
  const schemaTypeMap = Object.fromEntries(
    schemaFields.map(([fieldName, fieldSchema]) => {
      const schemaWithMetadata = fieldSchema as { __lmao_type?: string };
      return [fieldName, schemaWithMetadata.__lmao_type || 'unknown'];
    })
  );
  
  // Generate the complete class
  const classCode = `
(function() {
  'use strict';
  
  ${enumMappings.join('\n')}
  
  // Schema type map for runtime type detection
  const SCHEMA_TYPES = ${JSON.stringify(schemaTypeMap)};
  
  class ${className} {
    constructor(buffer, categoryInterner, textStorage, getBufferWithSpace, initialScopedAttributes = {}) {
      this._buffer = buffer;
      this._categoryInterner = categoryInterner;
      this._textStorage = textStorage;
      this._getBufferWithSpace = getBufferWithSpace;
      this._currentTagIndex = null;
      this._scopedAttributes = initialScopedAttributes;
    }
    
    // Tag getter - creates new entry and returns chainable API
    get tag() {
      // Check for overflow and get buffer with space
      const result = this._getBufferWithSpace(this._buffer);
      this._buffer = result.buffer;
      
      const idx = this._buffer.writeIndex;
      
      // Write entry type for tag
      this._buffer.operations[idx] = ${ENTRY_TYPE_TAG};
      
      // Write timestamp
      this._buffer.timestamps[idx] = Date.now();
      
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
      
      // Set current tag index for chained calls
      this._currentTagIndex = idx;
      
      // Increment write index
      this._buffer.writeIndex++;
      
      return this;
    }
    
    // with() method for bulk attribute setting
    with(attributes) {
      const idx = this._currentTagIndex !== null ? this._currentTagIndex : this._buffer.writeIndex;
      
      for (const [key, value] of Object.entries(attributes)) {
        const columnName = 'attr_' + key;
        const column = this._buffer[columnName];
        if (column && value !== null && value !== undefined) {
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
    
    // Message logging methods
    _writeMessage(entryType, message) {
      const result = this._getBufferWithSpace(this._buffer);
      this._buffer = result.buffer;
      
      const idx = this._buffer.writeIndex;
      
      // Write entry type
      this._buffer.operations[idx] = entryType;
      
      // Write timestamp
      this._buffer.timestamps[idx] = Date.now();
      
      // Write message
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
  schema: T
): new (
  buffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: TextStorage,
  getBufferWithSpace: GetBufferWithSpaceFn,
  initialScopedAttributes?: Record<string, unknown>
) => BaseSpanLogger<T> {
  const classCode = generateSpanLoggerClass(schema).trim();
  
  // Use Function constructor to create the class
  // This is safe because we control the code generation
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const GeneratedClass = new Function('return ' + classCode)();
  
  return GeneratedClass as new (
    buffer: SpanBuffer,
    categoryInterner: StringInterner,
    textStorage: TextStorage,
    getBufferWithSpace: GetBufferWithSpaceFn,
    initialScopedAttributes?: Record<string, unknown>
  ) => BaseSpanLogger<T>;
}
