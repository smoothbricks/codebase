/**
 * Runtime code generation for ColumnWriter classes that write to ColumnBuffer.
 *
 * ColumnWriter provides a fluent API for writing rows to a ColumnBuffer:
 * - `nextRow()` advances to the next row (handles overflow automatically)
 * - Fluent setter methods for each schema column (e.g., `.userId("123").status("ok")`)
 *
 * WHY separate ColumnWriter from ColumnBuffer:
 * - ColumnBuffer owns storage (TypedArrays, null bitmaps)
 * - ColumnWriter owns write position and fluent API
 * - Writers can be extended (via _getNextBuffer) for custom overflow handling
 * - Same buffer can have multiple writers (different write positions)
 *
 * Flow:
 * 1. `nextRow()` checks overflow, handles it, then increments `_writeIndex`
 * 2. Fluent setters write at `_writeIndex` (already incremented by nextRow)
 * 3. Return `this` for chaining: `writer.nextRow().userId("123").status("ok")`
 */

import { getSchemaFields, type SchemaWithMetadata, type TagAttributeSchema } from '../schema-types.js';
import type { ColumnBuffer } from './types.js';

/**
 * Extension options for injecting custom code into generated ColumnWriter classes.
 *
 * WHY: Consumers (like lmao's SpanLogger) need to add domain-specific properties
 * and methods to the generated class. This extension mechanism allows injecting
 * code directly into the generated class for optimal performance.
 */
export interface ColumnWriterExtension {
  /**
   * Additional code to add to the constructor (after _buffer and _writeIndex are set).
   * Has access to `this`, `buffer`, and any constructorParams.
   * @example
   * ```
   * this._scope = scope;
   * this._taskContext = taskContext;
   * ```
   */
  constructorCode?: string;

  /**
   * Additional methods to add to the class body.
   * @example
   * ```
   * info(message) {
   *   this._buffer._operations[this._writeIndex] = 1;
   *   return this;
   * }
   * ```
   */
  methods?: string;

  /**
   * Additional code to add before the class definition.
   * Useful for helper functions, symbols, constants, etc.
   * These can reference dependencies by name (they're in scope).
   * @example
   * ```
   * const ENTRY_TYPE = { INFO: 1, WARN: 2, ERROR: 3 };
   * ```
   */
  preamble?: string;

  /**
   * Constructor parameters beyond buffer.
   * These are added AFTER buffer in the constructor signature.
   * @example "scope, taskContext"
   */
  constructorParams?: string;

  /**
   * Runtime dependencies to inject into the generated class closure.
   * Keys become variable names available in preamble, constructorCode, and methods.
   * Values are the actual runtime objects/functions.
   *
   * NOTE: Dependencies are NOT included in the cache key - they should be
   * stable singleton values (like module-level functions).
   *
   * @example
   * ```
   * dependencies: {
   *   setNullBit: setNullBit,
   *   internString: internString
   * }
   * // Then in methods: "helpers.setNullBit(this._buffer.userId_nulls, idx);"
   * ```
   */
  dependencies?: Record<string, unknown>;
}

/**
 * ColumnWriter with schema-specific fluent setter methods.
 *
 * Each schema field gets a setter method that:
 * 1. Writes the value at _writeIndex
 * 2. Returns `this` for fluent chaining
 *
 * @example
 * ```typescript
 * const writer: ColumnWriter<{ userId: S.category(), count: S.number() }>;
 * writer.nextRow().userId("u123").count(42);
 * ```
 */
export type ColumnWriter<T extends TagAttributeSchema = TagAttributeSchema> = {
  _buffer: ColumnBuffer;
  _writeIndex: number;
  nextRow(): ColumnWriter<T>;
  _getNextBuffer(): ColumnBuffer;
} & {
  [K in keyof T as K extends string ? K : never]: (value: import('@sury/sury').Output<T[K]>) => ColumnWriter<T>;
};

/**
 * Get the setter method body for a schema field type
 */
function getSetterBody(schema: TagAttributeSchema, fieldName: string): string {
  const fieldSchema = schema[fieldName];
  const schemaWithMetadata = fieldSchema as SchemaWithMetadata;
  const schemaType = schemaWithMetadata?.__schema_type;

  const columnName = fieldName;

  if (schemaType === 'boolean') {
    // Bit-packed boolean: set bit in values bitmap
    return `
    const idx = this._writeIndex;
    const byteIdx = idx >>> 3;
    const bitMask = 1 << (idx & 7);
    this._buffer.${columnName}_nulls[byteIdx] |= bitMask;
    if (value) {
      this._buffer.${columnName}_values[byteIdx] |= bitMask;
    } else {
      this._buffer.${columnName}_values[byteIdx] &= ~bitMask;
    }
    return this;`;
  }

  // All other types: call buffer's setter method (handles enum mapping, null bitmap, etc.)
  return `
    this._buffer.${columnName}(this._writeIndex, value);
    return this;`;
}

/**
 * Generate ColumnWriter class code as a string
 *
 * Creates a class with:
 * 1. `_buffer` - reference to current ColumnBuffer
 * 2. `_writeIndex` - current write position
 * 3. `nextRow()` - advance to next row with overflow handling
 * 4. `_getNextBuffer()` - get/create next buffer (override for custom behavior)
 * 5. Fluent setter methods for each schema column
 */
export function generateColumnWriterClass(
  schema: TagAttributeSchema,
  className = 'GeneratedColumnWriter',
  extension?: ColumnWriterExtension,
): string {
  // Use getSchemaFields to filter out methods (validate, parse, etc.)
  const schemaFields = getSchemaFields(schema).map(([name]) => name);

  // Generate setter methods for each schema field
  const setterMethods: string[] = [];

  for (const fieldName of schemaFields) {
    const setterBody = getSetterBody(schema, fieldName);
    setterMethods.push(`    ${fieldName}(value) {${setterBody}
    }`);
  }

  // Build constructor signature with optional extension params
  const constructorSignature = extension?.constructorParams ? `buffer, ${extension.constructorParams}` : 'buffer';

  // Build constructor body
  const constructorBody: string[] = [];
  constructorBody.push('      this._buffer = buffer;');
  constructorBody.push('      this._writeIndex = -1;');

  // Add extension constructor code if provided
  if (extension?.constructorCode) {
    constructorBody.push('');
    constructorBody.push('      // Extension constructor code');
    const extensionLines = extension.constructorCode
      .trim()
      .split('\n')
      .map((line) => `      ${line.trim()}`);
    constructorBody.push(...extensionLines);
  }

  // Build extension methods if provided
  const extensionMethods = extension?.methods
    ? `
    // Extension methods
${extension.methods
  .trim()
  .split('\n')
  .map((line) => `    ${line}`)
  .join('\n')}`
    : '';

  // Build preamble if provided
  const preambleCode = extension?.preamble
    ? `
  // Extension preamble
${extension.preamble}
`
    : '';

  // Generate the complete class code
  const classCode = `
  'use strict';
${preambleCode}
  class ${className} {
    constructor(${constructorSignature}) {
${constructorBody.join('\n')}
    }

    /**
     * Advance to next row. Handles overflow by getting next buffer.
     * Must be called before writing any column values.
     */
    nextRow() {
      // Check overflow BEFORE incrementing
      if (this._writeIndex >= this._buffer._capacity - 1) {
        this._buffer = this._getNextBuffer();
        this._writeIndex = -1;
      }
      this._writeIndex++;
      return this;
    }

    /**
     * Get the next buffer in the chain.
     * Default implementation returns existing _next buffer.
     * Override this method to implement custom buffer creation/pooling.
     */
    _getNextBuffer() {
      if (!this._buffer._next) {
        throw new Error('Buffer overflow: no next buffer available. Override _getNextBuffer() to handle this.');
      }
      return this._buffer._next;
    }

    // Schema column setters
${setterMethods.join('\n\n')}
${extensionMethods}
  }

  return ${className};
`;

  return classCode;
}

/**
 * Cache for generated ColumnWriter classes.
 * Key: JSON-serialized schema + extension (stable across identical configurations)
 * Value: Generated class constructor
 */
const writerClassCache = new Map<string, new (buffer: ColumnBuffer, ...args: unknown[]) => ColumnWriter>();

/**
 * Create a stable cache key from schema and extension options.
 */
function createCacheKey(schema: TagAttributeSchema, extension?: ColumnWriterExtension): string {
  if (!extension) {
    return JSON.stringify(schema);
  }
  // Exclude dependencies from cache key - they should be stable singletons
  const { dependencies: _, ...extensionWithoutDeps } = extension;
  return JSON.stringify({ schema, extension: extensionWithoutDeps });
}

/**
 * Create or retrieve a cached ColumnWriter class for the given schema and extension
 *
 * This is the cold-path function called at module initialization time.
 * The generated class is cached per schema+extension to avoid regenerating for every writer.
 *
 * @param schema - The tag attribute schema defining columns
 * @param extension - Optional extension for injecting constructor code, methods, etc.
 * @returns The generated class constructor with typed setter methods
 */
export function getColumnWriterClass<T extends TagAttributeSchema>(
  schema: T,
  extension?: ColumnWriterExtension,
): new (
  buffer: ColumnBuffer,
  ...args: unknown[]
) => ColumnWriter<T> {
  const cacheKey = createCacheKey(schema, extension);

  let WriterClass = writerClassCache.get(cacheKey);

  if (!WriterClass) {
    const classCode = generateColumnWriterClass(schema, 'GeneratedColumnWriter', extension).trim();

    // Compile with new Function()
    const depNames: string[] = [];
    const depValues: unknown[] = [];

    if (extension?.dependencies && Object.keys(extension.dependencies).length > 0) {
      for (const [name, value] of Object.entries(extension.dependencies)) {
        depNames.push(name);
        depValues.push(value);
      }
    }

    // Create function that takes dependencies as parameters and returns the class
    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    const factory = new Function(...depNames, classCode) as (
      ...args: unknown[]
    ) => new (
      buffer: ColumnBuffer,
      ...args: unknown[]
    ) => ColumnWriter;
    WriterClass = factory(...depValues);

    writerClassCache.set(cacheKey, WriterClass);
  }

  // Cast is safe because the generated class has typed setters matching the schema T
  return WriterClass as unknown as new (
    buffer: ColumnBuffer,
    ...args: unknown[]
  ) => ColumnWriter<T>;
}

/**
 * Create a ColumnWriter instance for the given schema and buffer
 *
 * @param schema - The tag attribute schema defining columns
 * @param buffer - The ColumnBuffer to write to
 * @param extension - Optional extension for injecting constructor code, methods, etc.
 * @param constructorArgs - Additional constructor arguments (passed to extension constructor code)
 * @returns A new ColumnWriter instance with typed setter methods
 */
export function createColumnWriter<T extends TagAttributeSchema>(
  schema: T,
  buffer: ColumnBuffer,
  extension?: ColumnWriterExtension,
  ...constructorArgs: unknown[]
): ColumnWriter<T> {
  const WriterClass = getColumnWriterClass(schema, extension);
  return new WriterClass(buffer, ...constructorArgs);
}
