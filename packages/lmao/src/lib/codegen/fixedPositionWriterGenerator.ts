/**
 * Runtime code generation for TagWriter and ResultWriter classes
 *
 * These classes provide fluent setter methods that write to a fixed position in a SpanBuffer:
 * - TagWriter: writes to position 0 (span-start row) via ctx.tag.userId("123")
 * - ResultWriter: writes to position 1 (result row) via ctx.ok(data).userId("123")
 *
 * Both share the same codegen since they only differ in:
 * 1. The fixed position (0 vs 1)
 * 2. ResultWriter has additional _result/_error properties
 *
 * WHY Fixed Positions:
 * - Row 0: span-start entry (written at span creation, attributes set via ctx.tag)
 * - Row 1: span-end entry (written at ctx.ok()/ctx.err(), attributes set via fluent API)
 *
 * Pattern:
 * - Each setter calls this._buffer.columnName(this._pos, value) and returns this
 * - Fluent chaining: ctx.tag.userId("123").requestId("abc")
 * - Zero allocation: returns same object reference
 */

import { bufferHelpers, type ColumnEntry } from '@smoothbricks/arrow-builder';
import { getEnumValues, getSchemaType } from '../schema/typeGuards.js';
import type { InferSchema, LogSchema } from '../schema/types.js';
import type { AnySpanBuffer } from '../types.js';

/**
 * Extension options for injecting custom code into generated writer classes.
 */
export interface FixedPositionWriterExtension {
  /**
   * Additional code to add to the constructor (after buffer assignment).
   * Has access to `this` and constructorParams.
   */
  constructorCode?: string;

  /**
   * Additional methods to add to the class body.
   */
  methods?: string;

  /**
   * Constructor parameters beyond buffer.
   * Added AFTER buffer in the constructor signature.
   * @example "result, isError"
   */
  constructorParams?: string;
}

/**
 * TagWriter interface - fluent setter API for span-start attributes (row 0)
 */
export type TagWriter<T extends LogSchema> = {
  /**
   * Bulk set multiple attributes at once.
   */
  with(attributes: Partial<InferSchema<T>>): TagWriter<T>;
} & {
  /**
   * Individual attribute setters - each returns `this` for chaining.
   */
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => TagWriter<T>;
};

/**
 * ResultWriter interface - fluent setter API for result attributes (row 1)
 *
 * Includes _result and _error properties for accessing the return value or error.
 */
export type ResultWriter<T extends LogSchema, R = unknown, E = unknown> = {
  /**
   * The successful result value (set by ctx.ok(data))
   */
  readonly _result: R | undefined;

  /**
   * The error value (set by ctx.err(error))
   */
  readonly _error: E | undefined;

  /**
   * Whether this is an error result
   */
  readonly _isError: boolean;

  /**
   * Bulk set multiple attributes at once.
   */
  with(attributes: Partial<InferSchema<T>>): ResultWriter<T, R, E>;
} & {
  /**
   * Individual attribute setters - each returns `this` for chaining.
   */
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => ResultWriter<T, R, E>;
};

/**
 * Generate enum value mapping code.
 * Creates a switch-case statement for compile-time enum mapping.
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
 * Generate setter method code for a single attribute.
 * Calls this._buffer.columnName(this._pos, value) and returns this.
 */
function generateSetterMethod(fieldName: string, schema: unknown, hasEnumMapping: boolean): string {
  const columnName = fieldName;
  const lmaoType = getSchemaType(schema);

  if (lmaoType === 'enum' && hasEnumMapping) {
    return `
    ${fieldName}(value) {
      this._buffer.${columnName}(this._pos, getEnumIndex_${fieldName}(value));
      return this;
    }`;
  }

  // All other types: direct pass-through to buffer setter
  // The buffer's setter handles null bitmap and storage details
  return `
    ${fieldName}(value) {
      this._buffer.${columnName}(this._pos, value);
      return this;
    }`;
}

/**
 * Generate with() method code - bulk attribute setting.
 * UNROLLED per-column code for zero Object.entries overhead.
 */
function generateWithMethod(schemaFields: readonly ColumnEntry[], enumFieldNames: Set<string>): string {
  const columnWrites = schemaFields.map(([fieldName]) => {
    const hasEnumMapping = enumFieldNames.has(fieldName);
    const valueExpr = hasEnumMapping ? `getEnumIndex_${fieldName}(attributes.${fieldName})` : `attributes.${fieldName}`;

    return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        this._buffer.${fieldName}(this._pos, ${valueExpr});
      }`;
  });

  return `
    with(attributes) {
      ${columnWrites.join('\n')}
      return this;
    }`;
}

/**
 * Generate the complete fixed-position writer class code.
 *
 * @param schema - Tag attribute schema defining columns
 * @param position - Fixed position to write to (0 for TagWriter, 1 for ResultWriter)
 * @param className - Name for the generated class
 * @param extension - Optional extension for constructor code, methods, params
 * @returns JavaScript code string for the class
 */
export function generateFixedPositionWriterClass(
  schema: LogSchema,
  position: number,
  className: string,
  extension?: FixedPositionWriterExtension,
): string {
  const schemaFields = schema._columns;

  // Generate enum mapping functions
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

  // Generate setter methods
  const setterMethods = schemaFields.map(([fieldName, fieldSchema]) =>
    generateSetterMethod(fieldName, fieldSchema, enumFieldNames.has(fieldName)),
  );

  // Generate with() method
  const withMethod = generateWithMethod(schemaFields, enumFieldNames);

  // Build constructor signature
  const constructorSignature = extension?.constructorParams ? `buffer, ${extension.constructorParams}` : 'buffer';

  // Build constructor body
  // Generated structure: assigns _buffer and _pos, then runs extension constructor code if provided
  const constructorBody = ['    this._buffer = buffer;', `    this._pos = ${position};`];

  if (extension?.constructorCode) {
    constructorBody.push('');
    // Extension constructor code runs after _buffer/_pos assignment
    const extensionLines = extension.constructorCode
      .trim()
      .split('\n')
      .map((line) => `    ${line.trim()}`);
    constructorBody.push(...extensionLines);
  }

  // Build extension methods (additional methods injected by the caller)
  const extensionMethods = extension?.methods
    ? '\n' +
      extension.methods
        .trim()
        .split('\n')
        .map((line) => `    ${line}`)
        .join('\n')
    : '';

  // Generated class structure:
  // - constructor: assigns _buffer, _pos, then extension code
  // - with(): bulk attribute setting method
  // - Individual setter methods for each schema field
  // - Extension methods (if any)
  const classCode = `
  'use strict';

  ${enumMappings.join('\n')}

  class ${className} {
    constructor(${constructorSignature}) {
${constructorBody.join('\n')}
    }

    ${withMethod}

    ${setterMethods.join('\n')}
${extensionMethods}
  }

  return ${className};
`;

  return classCode;
}

// ============================================================================
// Class Caches
// ============================================================================

/**
 * Cache for TagWriter classes (position 0).
 * WeakMap allows garbage collection when schema is no longer referenced.
 */
const tagWriterClassCache = new WeakMap<LogSchema, new (buffer: AnySpanBuffer) => TagWriter<LogSchema>>();

/**
 * Cache for ResultWriter classes (position 1).
 */
const resultWriterClassCache = new WeakMap<
  LogSchema,
  new (
    buffer: AnySpanBuffer,
    result: unknown,
    isError: boolean,
  ) => ResultWriter<LogSchema>
>();

// ============================================================================
// TagWriter API
// ============================================================================

/**
 * Generate TagWriter class code for a schema.
 * TagWriter writes to position 0 (span-start row).
 */
export function generateTagWriterClass(schema: LogSchema): string {
  return generateFixedPositionWriterClass(schema, 0, 'GeneratedTagWriter');
}

/**
 * Get or create a cached TagWriter class for a schema.
 *
 * @param schema - Tag attribute schema
 * @returns TagWriter class constructor
 */
export function getTagWriterClass<T extends LogSchema>(schema: T): new (buffer: AnySpanBuffer) => TagWriter<T> {
  let WriterClass = tagWriterClassCache.get(schema);

  if (!WriterClass) {
    const classCode = generateTagWriterClass(schema).trim();

    // Compile with new Function()
    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    const factory = new Function('helpers', classCode) as (
      helpers: typeof bufferHelpers,
    ) => new (
      buffer: AnySpanBuffer,
    ) => TagWriter<LogSchema>;

    WriterClass = factory(bufferHelpers);
    tagWriterClassCache.set(schema, WriterClass);
  }

  return WriterClass as new (
    buffer: AnySpanBuffer,
  ) => TagWriter<T>;
}

/**
 * Create a TagWriter instance for a buffer.
 *
 * @param schema - Tag attribute schema
 * @param buffer - SpanBuffer to write to
 * @returns TagWriter instance bound to position 0
 */
export function createTagWriter<T extends LogSchema>(schema: T, buffer: AnySpanBuffer): TagWriter<T> {
  const WriterClass = getTagWriterClass(schema);
  return new WriterClass(buffer);
}

// ============================================================================
// ResultWriter API
// ============================================================================

/**
 * ResultWriter extension - adds _result, _error, _isError properties
 *
 * Constructor sets _isError, _result, and _error based on the isError flag.
 * Result/error values are already accessible via the properties set in constructor.
 */
const resultWriterExtension: FixedPositionWriterExtension = {
  constructorParams: 'resultOrError, isError',
  constructorCode: `
    this._isError = isError;
    if (isError) {
      this._error = resultOrError;
      this._result = undefined;
    } else {
      this._result = resultOrError;
      this._error = undefined;
    }
  `,
};

/**
 * Generate ResultWriter class code for a schema.
 * ResultWriter writes to position 1 (result row).
 */
export function generateResultWriterClass(schema: LogSchema): string {
  return generateFixedPositionWriterClass(schema, 1, 'GeneratedResultWriter', resultWriterExtension);
}

/**
 * Get or create a cached ResultWriter class for a schema.
 *
 * @param schema - Tag attribute schema
 * @returns ResultWriter class constructor
 */
export function getResultWriterClass<T extends LogSchema>(
  schema: T,
): new (
  buffer: AnySpanBuffer,
  resultOrError: unknown,
  isError: boolean,
) => ResultWriter<T> {
  let WriterClass = resultWriterClassCache.get(schema);

  if (!WriterClass) {
    const classCode = generateResultWriterClass(schema).trim();

    // Compile with new Function()
    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    const factory = new Function('helpers', classCode) as (
      helpers: typeof bufferHelpers,
    ) => new (
      buffer: AnySpanBuffer,
      resultOrError: unknown,
      isError: boolean,
    ) => ResultWriter<LogSchema>;

    WriterClass = factory(bufferHelpers);
    resultWriterClassCache.set(schema, WriterClass);
  }

  return WriterClass as new (
    buffer: AnySpanBuffer,
    resultOrError: unknown,
    isError: boolean,
  ) => ResultWriter<T>;
}

/**
 * Create a ResultWriter instance for a buffer.
 *
 * @param schema - Tag attribute schema
 * @param buffer - SpanBuffer to write to
 * @param resultOrError - The result value or error
 * @param isError - Whether this is an error result
 * @returns ResultWriter instance bound to position 1
 */
export function createResultWriter<T extends LogSchema, R = unknown, E = unknown>(
  schema: T,
  buffer: AnySpanBuffer,
  resultOrError: R | E,
  isError: boolean,
): ResultWriter<T, R, E> {
  const WriterClass = getResultWriterClass(schema);
  return new WriterClass(buffer, resultOrError, isError) as ResultWriter<T, R, E>;
}

/**
 * Get or create a cached fixed-position writer class.
 * This is the generic version - prefer getTagWriterClass/getResultWriterClass for standard use.
 *
 * @param schema - Tag attribute schema
 * @param position - Fixed position to write to
 * @param extension - Optional extension for constructor code, methods, params
 * @returns Writer class constructor
 */
export function getFixedPositionWriterClass<T extends LogSchema>(
  schema: T,
  position: number,
  extension?: FixedPositionWriterExtension,
): new (
  buffer: AnySpanBuffer,
  ...args: unknown[]
) => { _buffer: AnySpanBuffer; _pos: number } & Record<string, unknown> {
  // For custom positions/extensions, generate without caching
  // (TagWriter at 0 and ResultWriter at 1 use their own caches)
  const className = `FixedPositionWriter_${position}`;
  const classCode = generateFixedPositionWriterClass(schema, position, className, extension).trim();

  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const factory = new Function('helpers', classCode) as (
    helpers: typeof bufferHelpers,
  ) => new (
    buffer: AnySpanBuffer,
    ...args: unknown[]
  ) => { _buffer: AnySpanBuffer; _pos: number };

  return factory(bufferHelpers);
}
