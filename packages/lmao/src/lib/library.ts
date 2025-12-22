/**
 * Library Prefix/Remapping Utilities
 *
 * WHY: Libraries need to write clean, domain-focused code (ctx.tag.status(200))
 * while avoiding naming conflicts when composed with other libraries.
 *
 * HOW: At composition time (cold path), we generate a remapped SpanLogger class
 * that exposes clean method names but writes to prefixed buffer columns.
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Libraries define clean schemas without prefixes (status, method, url)
 * - Prefixing happens at composition time (http_status, http_method, http_url)
 * - Zero hot path overhead - all remapping done via code generation at module creation
 *
 * WHAT: This module provides:
 * - prefixSchema() - Rename schema fields with prefix
 * - createPrefixMapping() - Build clean→prefixed name mapping
 * - generateRemappedBufferViewClass() - Generate view for Arrow conversion
 * - generateRemappedSpanLoggerClass() - Generate class with clean methods, prefixed writes
 * - createRemappedSpanLoggerClass() - Compile and cache SpanLogger classes
 */

import type { BaseSpanLogger } from './codegen/spanLoggerGenerator.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from './schema/systemSchema.js';
import { getEnumValues, getSchemaType } from './schema/typeGuards.js';
import { LogSchema, type SchemaFields } from './schema/types.js';
import type { SpanBuffer } from './types.js';

/**
 * Prefix a tag attribute schema
 * Renames all fields with a prefix to avoid conflicts
 *
 * Example:
 * - Input: { status: S.number(), method: S.enum(['GET', 'POST']) }
 * - Prefix: 'http'
 * - Output: { http_status: S.number(), http_method: S.enum(['GET', 'POST']) }
 */
export function prefixSchema(
  schema: LogSchema | { fieldEntries(): Iterable<[string, unknown]> },
  prefix: string,
): LogSchema & Record<string, unknown> {
  const prefixedFields: Record<string, unknown> = {};

  // Get schema fields from LogSchema.fieldEntries()
  for (const [fieldName, fieldSchema] of schema.fieldEntries()) {
    const prefixedName = `${prefix}_${fieldName}`;
    prefixedFields[prefixedName] = fieldSchema;
  }

  // Create LogSchema and spread fields directly onto result for direct access
  const logSchema = new LogSchema(prefixedFields as SchemaFields);
  return Object.assign(logSchema, prefixedFields);
}

/**
 * Prefix mapping: maps clean names to prefixed column names
 *
 * WHY: Library authors write ctx.tag.status() but buffer column is http_status
 * HOW: At module creation time, we build this mapping once (cold path)
 *
 * @example
 * // Input: schema = { status: S.number(), method: S.enum([...]) }, prefix = 'http'
 * // Output: { status: 'http_status', method: 'http_method' }
 */
export interface PrefixMapping {
  [cleanName: string]: string; // cleanName -> prefixedColumnName
}

/**
 * Create a mapping from clean field names to prefixed column names
 *
 * WHY: This mapping is used by generateRemappedSpanLoggerClass to create methods
 * that write to prefixed columns but expose clean API names.
 *
 * @param schema - Clean schema (LogSchema instance)
 * @param prefix - Prefix to apply (e.g., 'http')
 * @returns Mapping from clean name to prefixed name
 *
 * @example
 * const schema = defineLogSchema({ status: S.number() });
 * const mapping = createPrefixMapping(schema, 'http');
 * // Returns: { status: 'http_status' }
 */
export function createPrefixMapping<T extends LogSchema>(schema: T, prefix: string): PrefixMapping {
  const mapping: PrefixMapping = {};

  // Use LogSchema.fieldNames directly (no conditional needed)
  for (const fieldName of schema.fieldNames) {
    mapping[fieldName] = `${prefix}_${fieldName}`;
  }

  return mapping;
}

// ============================================================================
// RemappedBufferView - Maps prefixed column names to unprefixed for tree traversal
// ============================================================================

/**
 * Cache for generated RemappedBufferView classes.
 * Key is a stable string representation of the mapping.
 * This avoids regenerating classes for the same mapping.
 */
const remappedBufferViewClassCache = new Map<string, new (buffer: SpanBuffer) => SpanBuffer>();

/**
 * Create a stable cache key from a prefix mapping.
 * Sorts keys to ensure consistent ordering.
 */
function createMappingCacheKey(mapping: Record<string, string>): string {
  const sortedEntries = Object.entries(mapping).sort(([a], [b]) => a.localeCompare(b));
  return JSON.stringify(sortedEntries);
}

/**
 * Generate a RemappedBufferView class for library prefix support.
 *
 * WHY: Libraries write to unprefixed columns (status, method) but Arrow conversion
 * iterates using prefixed names from the root schema (http_status, http_method).
 * RemappedBufferView bridges this gap for tree traversal during cold-path Arrow conversion.
 *
 * HOW: Uses new Function() to generate a class at library composition time (cold path).
 * The generated class wraps a SpanBuffer and remaps getColumnIfAllocated/getNullsIfAllocated
 * calls from prefixed names to unprefixed names.
 *
 * WHAT: Returns a class constructor that:
 * - Passes through tree traversal properties (children, next)
 * - Passes through system columns (timestamps, operations, message, etc.)
 * - Passes through identity properties (traceId, spanId, etc.)
 * - Remaps getColumnIfAllocated() and getNullsIfAllocated() from prefixed→unprefixed
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Hot path: Library writes directly to unprefixed columns (zero overhead)
 * - Cold path: Arrow conversion uses RemappedBufferView to access via prefixed names
 *
 * @param prefixToUnprefixedMapping - Maps prefixed names to unprefixed names
 *   e.g., { 'http_status': 'status', 'http_method': 'method' }
 *   NOTE: The mapping is prefixed→unprefixed (reverse of createPrefixMapping output)
 *
 * @returns Constructor for RemappedBufferView class
 *
 * @example
 * ```typescript
 * // Create mapping (prefixed → unprefixed)
 * const mapping = { 'http_status': 'status', 'http_method': 'method' };
 * const ViewClass = generateRemappedBufferViewClass(mapping);
 *
 * // Wrap library buffer
 * const view = new ViewClass(libraryBuffer);
 *
 * // Access via prefixed name returns unprefixed column
 * view.getColumnIfAllocated('http_status'); // Returns buffer.getColumnIfAllocated('status')
 * ```
 */
export function generateRemappedBufferViewClass(
  prefixToUnprefixedMapping: Record<string, string>,
): new (
  buffer: SpanBuffer,
) => SpanBuffer {
  // Check cache first
  const cacheKey = createMappingCacheKey(prefixToUnprefixedMapping);
  const cached = remappedBufferViewClassCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const mappingCode = JSON.stringify(prefixToUnprefixedMapping);

  // Generate the class code
  // Uses IIFE wrapper for clean scope, similar to generateRemappedSpanLoggerClass
  const code = `(function() {
  'use strict';
  const mapping = ${mappingCode};
  
  class RemappedBufferView {
    constructor(buffer) {
      this._buffer = buffer;
    }
    
    // Tree traversal (pass-through)
    get _children() { return this._buffer._children; }
    get _next() { return this._buffer._next; }

    // Row count
    get _writeIndex() { return this._buffer._writeIndex; }

    // System columns (NOT remapped - same in all buffers)
    get timestamp() { return this._buffer.timestamp; }
    get entry_type() { return this._buffer.entry_type; }
    get message_values() { return this._buffer.message_values; }
    get message_nulls() { return this._buffer.message_nulls; }
    get line_values() { return this._buffer.line_values; }
    get line_nulls() { return this._buffer.line_nulls; }
    get error_code_values() { return this._buffer.error_code_values; }
    get error_code_nulls() { return this._buffer.error_code_nulls; }
    get exception_stack_values() { return this._buffer.exception_stack_values; }
    get exception_stack_nulls() { return this._buffer.exception_stack_nulls; }
    get ff_value_values() { return this._buffer.ff_value_values; }
    get ff_value_nulls() { return this._buffer.ff_value_nulls; }
    
    // Identity (pass-through)
    get trace_id() { return this._buffer.trace_id; }
    get thread_id() { return this._buffer.thread_id; }
    get span_id() { return this._buffer.span_id; }
    get parent_span_id() { return this._buffer.parent_span_id; }
    get parent_thread_id() { return this._buffer.parent_thread_id; }
    get _identity() { return this._buffer._identity; }
    
    // Metadata (pass-through)
    get module() { return this._buffer._module; }
    get spanName() { return this._buffer._spanName; }
    // Remapped column access (for Arrow conversion iteration)
    // Maps prefixed name → unprefixed name before calling underlying buffer
    getColumnIfAllocated(name) {
      const unprefixedName = mapping[name] ?? name;
      return this._buffer.getColumnIfAllocated(unprefixedName);
    }
    
    getNullsIfAllocated(name) {
      const unprefixedName = mapping[name] ?? name;
      return this._buffer.getNullsIfAllocated(unprefixedName);
    }
  }
  
  return RemappedBufferView;
})()`;

  // Compile the class using Function constructor (cold path - happens once per mapping)
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const GeneratedClass = new Function(`return ${code}`)() as new (buffer: SpanBuffer) => SpanBuffer;

  // Cache for future use
  remappedBufferViewClassCache.set(cacheKey, GeneratedClass);

  return GeneratedClass;
}

/**
 * Generate enum value mapping code for remapped class
 * Creates a switch-case statement for compile-time enum mapping
 *
 * WHY: Enums need compile-time mapping to Uint8 values for 1-byte storage
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
 * Generate attribute writer method for remapped SpanLogger
 *
 * WHY: Each method must write to the PREFIXED column while being called by CLEAN name
 * HOW: Method name is cleanName, but buffer access uses prefixedColumnName
 *
 * @param cleanName - Clean method name (e.g., 'status')
 * @param prefixedColumnName - Prefixed column name (e.g., 'http_status')
 * @param schema - Field schema for type-specific handling
 * @param hasEnumMapping - Whether enum mapping function exists
 */
function generateRemappedAttributeWriter(
  cleanName: string,
  prefixedColumnName: string,
  schema: unknown,
  hasEnumMapping: boolean,
): string {
  const columnName = prefixedColumnName;
  const lmaoType = getSchemaType(schema);

  // For enums, use pre-generated mapping function (uses cleanName for function)
  // Generated code: ALWAYS writes to row 0 (span-start) with overwrite semantics
  // Maps clean method name to prefixed column, marks value as non-null (bit 0)
  if (lmaoType === 'enum' && hasEnumMapping) {
    return `
    ${cleanName}(value) {
      const idx = 0;
      const enumIndex = getEnumIndex_${cleanName}(value);
      this._buffer.${columnName}_values[idx] = enumIndex;
      this._buffer.${columnName}_nulls[0] |= 1;
      return this;
    }`;
  }

  // For categories, store strings directly
  // Generated code: writes to row 0, stores string directly, marks non-null
  if (lmaoType === 'category') {
    return `
    ${cleanName}(value) {
      const idx = 0;
      if (value === null || value === undefined) {
        this._buffer.${columnName}_nulls[0] &= ~1;
        return this;
      }
      this._buffer.${columnName}_values[idx] = value;
      this._buffer.${columnName}_nulls[0] |= 1;
      return this;
    }`;
  }

  // For text, store strings directly
  // Generated code: writes to row 0, handles null/undefined by clearing bit and returning early,
  // stores string directly, marks non-null
  if (lmaoType === 'text') {
    return `
    ${cleanName}(value) {
      const idx = 0;
      if (value === null || value === undefined) {
        this._buffer.${columnName}_nulls[0] &= ~1;
        return this;
      }
      this._buffer.${columnName}_values[idx] = value;
      this._buffer.${columnName}_nulls[0] |= 1;
      return this;
    }`;
  }

  // Boolean: bit-packed storage (8 values per byte)
  // Generated code: writes to row 0, bit 0 of byte 0 is index 0,
  // sets or clears value bit, marks non-null
  if (lmaoType === 'boolean') {
    return `
    ${cleanName}(value) {
      if (value) {
        this._buffer.${columnName}_values[0] |= 1;
      } else {
        this._buffer.${columnName}_values[0] &= ~1;
      }
      this._buffer.${columnName}_nulls[0] |= 1;
      return this;
    }`;
  }

  // Generic writer for other types (number)
  // Generated code: writes to row 0, direct value assignment, marks non-null
  return `
    ${cleanName}(value) {
      const idx = 0;
      this._buffer.${columnName}_values[idx] = value;
      this._buffer.${columnName}_nulls[0] |= 1;
      return this;
    }`;
}

/**
 * Generate a remapped SpanLogger class for library prefix support
 *
 * WHY: Library authors write clean code (ctx.tag.status(200)) but buffers use prefixed
 * columns (http_status). This function generates a class at module creation time
 * (cold path) that exposes clean method names but writes to prefixed columns.
 *
 * HOW: Uses new Function() to generate optimized JavaScript code. The generated class
 * has methods named after clean schema fields but writes to prefixed buffer columns.
 *
 * WHAT: Returns executable JavaScript code string for a SpanLogger class with:
 * - Clean method names (status, method, url)
 * - Writes to prefixed columns (http_status, http_method, http_url)
 * - Full support for enum/category/text type handling
 * - Method chaining support
 *
 * @param cleanSchema - Original clean schema (without prefix)
 * @param prefixMapping - Mapping from clean names to prefixed names
 * @param className - Name for the generated class
 * @returns Executable JavaScript code string
 *
 * @example
 * const code = generateRemappedSpanLoggerClass(
 *   { status: S.number(), method: S.enum(['GET', 'POST']) },
 *   { status: 'http_status', method: 'http_method' },
 *   'HttpSpanLogger'
 * );
 * // Generated class has .status() method that writes to http_status
 */
export function generateRemappedSpanLoggerClass<T extends LogSchema>(
  cleanSchema: T,
  prefixMapping: PrefixMapping,
  className = 'RemappedSpanLogger',
): string {
  // Use LogSchema.fieldEntries() to iterate schema fields
  const schemaFields = Array.from(cleanSchema.fieldEntries());

  // Generate enum mapping functions (use cleanName for function names)
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

  // Generate attribute writer methods with remapping
  const attributeWriters = schemaFields.map(([cleanName, fieldSchema]) => {
    const prefixedName = prefixMapping[cleanName] || cleanName;
    return generateRemappedAttributeWriter(cleanName, prefixedName, fieldSchema, enumFieldNames.has(cleanName));
  });

  // Create schema type map for runtime type detection in scope()
  const schemaTypeMap = Object.fromEntries(
    schemaFields.map(([fieldName, fieldSchema]) => {
      return [fieldName, getSchemaType(fieldSchema) || 'unknown'];
    }),
  );

  // Create clean-to-prefixed mapping for with() and scope() methods
  const prefixMappingJson = JSON.stringify(prefixMapping);

  // Entry type constants (imported from lmao.ts at module level, used here for code gen)
  // These values are inlined into the generated code string below

  // Generate the complete class
  // Generated code: IIFE wrapper for clean scope
  const classCode =
    `(function() {
  'use strict';
  ` +
    // Generated code: getTimestampNanos - platform-aware timestamp function
    // Node.js: uses process.hrtime.bigint() for true nanosecond precision
    // Browser: uses performance.timeOrigin + performance.now() for microsecond precision
    `const getTimestampNanos = (typeof process !== 'undefined' && process.hrtime)
    ? (() => {
        const anchorEpochNanos = BigInt(Date.now()) * 1000000n;
        const anchorHrtime = process.hrtime.bigint();
        return () => anchorEpochNanos + (process.hrtime.bigint() - anchorHrtime);
      })()
    : () => {
        const epochMicros = Math.round((performance.timeOrigin + performance.now()) * 1000);
        return BigInt(epochMicros) * 1000n;
      };
  ` +
    // Generated code: enum mapping functions (getEnumIndex_${fieldName})
    enumMappings.join('\n') +
    `
  ` +
    // Generated code: SCHEMA_TYPES - maps clean field names to lmao types for runtime type detection
    `const SCHEMA_TYPES = ${JSON.stringify(schemaTypeMap)};
  ` +
    // Generated code: PREFIX_MAPPING - maps clean names to prefixed column names
    `const PREFIX_MAPPING = ${prefixMappingJson};
  
  class ${className} {
    ` +
    // Generated code: constructor stores buffer, createNextBuffer function, and scoped attributes
    `constructor(buffer, createNextBuffer, initialScopedAttributes = {}) {
      this._buffer = buffer;
      this._createNextBuffer = createNextBuffer;
      this._scopedAttributes = initialScopedAttributes;
    }
    ` +
    // Generated code: tag getter returns this for chainable API (writes to row 0 = span-start)
    `get tag() {
      return this;
    }
    ` +
    // Generated code: with() bulk attribute setting - maps clean names to prefixed columns,
    // stores values directly (strings stored as-is), marks non-null (bit 0)
    `with(attributes) {
      const idx = 0;
      for (const [cleanKey, value] of Object.entries(attributes)) {
        const prefixedKey = PREFIX_MAPPING[cleanKey] || cleanKey;
        const columnName = prefixedKey;
        const column = this._buffer[columnName + '_values'];
        if (column && value !== null && value !== undefined) {
          column[idx] = value;
          const nullBitmap = this._buffer[columnName + '_nulls'];
          if (nullBitmap) {
            nullBitmap[0] |= 1;
          }
        }
      }
      return this;
    }
    ` +
    // Generated code: individual attribute setters (one per schema field)
    attributeWriters.join('\n') +
    `
    ` +
    // Generated code: scope() stores values in _scopedAttributes,
    // then pre-fills remaining buffer capacity so future log entries inherit these values
    `scope(attributes) {
      for (const [cleanKey, value] of Object.entries(attributes)) {
        if (value !== null && value !== undefined) {
          this._scopedAttributes[cleanKey] = value;
        }
      }
      const startIdx = this._buffer._writeIndex;
      const endIdx = this._buffer._capacity;
      for (let idx = startIdx; idx < endIdx; idx++) {
        for (const [cleanKey, value] of Object.entries(this._scopedAttributes)) {
          const prefixedKey = PREFIX_MAPPING[cleanKey] || cleanKey;
          const columnName = prefixedKey;
          const column = this._buffer[columnName + '_values'];
          if (column) {
            column[idx] = value;
            const nullBitmap = this._buffer[columnName + '_nulls'];
            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] |= (1 << bitOffset);
            }
          }
        }
      }
    }
    ` +
    // Generated code: _writeMessage() writes a log entry - ensures buffer has space,
    // writes entry type + timestamp + message, applies scoped attributes
    `_writeMessage(entryType, message) {
      // Check if buffer is full and create next buffer if needed
      if (this._buffer._writeIndex >= this._buffer._capacity) {
        const oldBuffer = this._buffer;
        this._buffer = this._createNextBuffer(oldBuffer);
        oldBuffer._next = this._buffer;
      }
      const idx = this._buffer._writeIndex;
      this._buffer._operations[idx] = entryType;
      this._buffer._timestamps[idx] = getTimestampNanos();
      const messageColumn = this._buffer.message_values;
      if (messageColumn) {
        messageColumn[idx] = message;
        const nullBitmap = this._buffer.message_nulls;
        if (nullBitmap) {
          const byteIndex = Math.floor(idx / 8);
          const bitOffset = idx % 8;
          nullBitmap[byteIndex] |= (1 << bitOffset);
        }
      }
      for (const [cleanKey, value] of Object.entries(this._scopedAttributes)) {
        const prefixedKey = PREFIX_MAPPING[cleanKey] || cleanKey;
        const columnName = prefixedKey;
        const column = this._buffer[columnName + '_values'];
        if (column) {
          column[idx] = value;
          const nullBitmap = this._buffer[columnName + '_nulls'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }
      this._buffer._writeIndex++;
    }
    ` +
    // Generated code: message() generic logging with level string mapping to entry type constants
    `message(level, message) {
      const entryTypeMap = {
        'info': ${ENTRY_TYPE_INFO},
        'debug': ${ENTRY_TYPE_DEBUG},
        'warn': ${ENTRY_TYPE_WARN},
        'error': ${ENTRY_TYPE_ERROR},
        'trace': ${ENTRY_TYPE_TRACE}
      };
      this._writeMessage(entryTypeMap[level] || ${ENTRY_TYPE_INFO}, message);
    }
    ` +
    // Generated code: convenience logging methods - each calls _writeMessage with entry type constant
    `info(message) { this._writeMessage(${ENTRY_TYPE_INFO}, message); }
    debug(message) { this._writeMessage(${ENTRY_TYPE_DEBUG}, message); }
    warn(message) { this._writeMessage(${ENTRY_TYPE_WARN}, message); }
    error(message) { this._writeMessage(${ENTRY_TYPE_ERROR}, message); }
    trace(message) { this._writeMessage(${ENTRY_TYPE_TRACE}, message); }
  }
  return ${className};
})()`;

  return classCode;
}

/**
 * Create a remapped SpanLogger class constructor from clean schema and prefix mapping
 *
 * WHY: This is the cold-path function called at module creation time. It compiles
 * the generated class code once and caches the constructor.
 *
 * @param cleanSchema - Clean schema (without prefix)
 * @param prefixMapping - Mapping from clean names to prefixed names
 * @returns Constructor for the remapped SpanLogger class
 */
export function createRemappedSpanLoggerClass<T extends LogSchema>(
  cleanSchema: T,
  prefixMapping: PrefixMapping,
): new (
  buffer: SpanBuffer,
  createNextBuffer: (buffer: SpanBuffer) => SpanBuffer,
  initialScopedAttributes?: Record<string, unknown>,
) => BaseSpanLogger<T> {
  const classCode = generateRemappedSpanLoggerClass(cleanSchema, prefixMapping).trim();

  // Use Function constructor to create the class (cold path - happens once per schema/prefix combo)
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const GeneratedClass = new Function(`return ${classCode}`)();

  return GeneratedClass;
}
