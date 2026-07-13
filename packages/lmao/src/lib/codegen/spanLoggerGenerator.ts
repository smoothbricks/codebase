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

import { type ColumnEntry, type ColumnWriterExtension, generateColumnWriterClass } from '@smoothbricks/arrow-builder';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_FF_ACCESS,
  ENTRY_TYPE_FF_USAGE,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from '../schema/systemSchema.js';
import type { WriterState } from './fixedPositionWriterGenerator.js';
import {
  resolveEnumLookupDescriptor,
  type EnumLookupDescriptor,
  type SchemaEnumLookupDescriptor,
} from '../enumMetadata.js';
import { getSchemaType } from '../schema/typeGuards.js';
import type { InferSchema, LogSchema } from '../schema/types.js';

import type { MessageLayoutFamily } from '../runtimeHint.js';
import type { AnySpanBuffer } from '../types.js';

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
  normalizeOperationalTemplate: (template: string) => string;
  setNullBit: (bitmap: Uint8Array, idx: number) => void;
  fillNullBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number) => void;
  fillBooleanBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number, value: boolean) => void;
} = {
  normalizeOperationalTemplate: (template: string) => {
    if (template.indexOf('{{') === -1 && template.indexOf('}}') === -1) return template;
    return template.replaceAll('{{', '{').replaceAll('}}', '}');
  },
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
  info(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  /** Log debug-level message */
  debug(message: string): FluentLogEntry<T>;
  /** Log warning message */
  warn(message: string): FluentLogEntry<T>;
  warn(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  /** Log error message */
  error(message: string): FluentLogEntry<T>;
  error(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  /** Log trace-level message */
  trace(message: string): FluentLogEntry<T>;
}

/**
 * Fluent interface returned by log methods (info, debug, warn, error, trace).
 * Includes system columns (line) plus all schema fields from T.
 * Also includes logging methods for continued chaining (e.g., ctx.log.info('x').info('y'))
 */
export type FluentLogEntry<T extends LogSchema> = {
  /** Set multiple attributes for this entry */
  with(attributes: Partial<InferSchema<T>>): FluentLogEntry<T>;
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
  info(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  debug(message: string): FluentLogEntry<T>;
  warn(message: string): FluentLogEntry<T>;
  warn(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  error(message: string): FluentLogEntry<T>;
  error(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
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
export type SpanLoggerImpl<T extends LogSchema> = {
  info(message: string): FluentLogEntry<T>;
  info(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  debug(message: string): FluentLogEntry<T>;
  warn(message: string): FluentLogEntry<T>;
  warn(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  error(message: string): FluentLogEntry<T>;
  error(template: string, fields: Partial<InferSchema<T>>): FluentLogEntry<T>;
  trace(message: string): FluentLogEntry<T>;
  _infoTemplate(vocabularyIndex: number): FluentLogEntry<T>;
  _debugTemplate(vocabularyIndex: number): FluentLogEntry<T>;
  _warnTemplate(vocabularyIndex: number): FluentLogEntry<T>;
  _errorTemplate(vocabularyIndex: number): FluentLogEntry<T>;
  _traceTemplate(vocabularyIndex: number): FluentLogEntry<T>;
  readonly scope: Readonly<Record<string, unknown>>;
  _setScope(attributes: ScopeUpdate<T>): void;
  _prefillScopedAttributesOn(buffer: AnySpanBuffer): void;
} & {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => FluentLogEntry<T>;
};

export type SpanLoggerConstructor<T extends LogSchema> = new (state: WriterState) => SpanLoggerImpl<T>;

export type ScopeUpdate<T extends LogSchema> = {
  [K in keyof InferSchema<T>]?: InferSchema<T>[K] | null;
};


/**
 * Generate override fluent setters for enum fields.
 * These convert string values to numeric indices before writing to the buffer.
 * The buffer expects numeric indices (after the enum map removal change).
 */
function generateEnumFluentSetters(
  enumFields: readonly EnumLookupDescriptor[],
  enumEncoderNames: Readonly<Record<string, string>>,
): string {
  const setters: string[] = [];

  for (const { fieldName } of enumFields) {
    const encoderName = enumEncoderNames[fieldName];
    setters.push(`
    /** Convert the declared enum string to its schema-order index. */
    ${fieldName}(value) {
      const idx = ${encoderName}(value);
      this._buffer.${fieldName}(this._writeIndex, idx);
      return this;
    }`);
  }

  return setters.join('\n');
}

//#region smoo/lmao!n/codegen-spanlogger.scope
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
  enumEncoderNames: Readonly<Record<string, string>>,
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
    const enumEncoderName = enumEncoderNames[fieldName];
    if (lmaoType === 'enum' && enumEncoderName !== undefined) {
      valueExpr = `${enumEncoderName}(scopeValue)`;
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
//#endregion smoo/lmao!n/codegen-spanlogger.scope

//#region smoo/lmao!n/codegen-spanlogger
/**
 * Build the extension for SpanLogger that extends ColumnWriter
 */
function buildSpanLoggerExtension(
  schema: LogSchema,
  messageLayoutFamily: MessageLayoutFamily,
  eagerColumns: readonly string[],
  enumLookup: SchemaEnumLookupDescriptor,
): ColumnWriterExtension {
  const schemaFields = schema._columns;

  const enumEncoderNames = Object.create(null) as Record<string, string>;
  const enumEncoderBindings = enumLookup.ordered.map(({ fieldName }, index) => {
    const encoderName = `encodeEnum${index}`;
    enumEncoderNames[fieldName] = encoderName;
    return `  const ${encoderName} = enumLookup.byField[${JSON.stringify(fieldName)}].encode;`;
  });

  // Generate methods
  const setScopeMethod = generateSetScopeMethod();
  const prefillMethod = generatePrefillScopedAttributesMethod(schemaFields, enumEncoderNames);
  const enumFluentSetters = generateEnumFluentSetters(enumLookup.ordered, enumEncoderNames);

  return {
    preallocatedColumns: eagerColumns,

    // Entry type constants (inlined from lmao.ts)
    classPreamble: `
  const ENTRY_TYPE_INFO = ${ENTRY_TYPE_INFO};
  const ENTRY_TYPE_DEBUG = ${ENTRY_TYPE_DEBUG};
  const ENTRY_TYPE_WARN = ${ENTRY_TYPE_WARN};
  const ENTRY_TYPE_ERROR = ${ENTRY_TYPE_ERROR};
  const ENTRY_TYPE_TRACE = ${ENTRY_TYPE_TRACE};
  const ENTRY_TYPE_FF_ACCESS = ${ENTRY_TYPE_FF_ACCESS};
  const ENTRY_TYPE_FF_USAGE = ${ENTRY_TYPE_FF_USAGE};
${enumEncoderBindings.join('\n')}
`,

    methods:
      // The optional fields argument is the untransformed fallback. JavaScript evaluates
      // the receiver, template, and object initializers before this method runs. Lowered
      // calls use _infoTemplate/_warnTemplate/_errorTemplate plus direct setters instead.
      // Context-owned append advances the shared active buffer and returns the literal row.
      `info(message, fields) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_INFO);
      ${messageLayoutFamily === 'static-only' ? `throw new TypeError('Dynamic log write reached a static-only callsite plan');` : `this._buffer.message_values[idx] = helpers.normalizeOperationalTemplate(message);`}
      if (fields !== undefined) {
        this.with(fields);
      }
      return this;
    }

    _infoTemplate(vocabularyIndex) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_INFO);
      ${messageLayoutFamily === 'dynamic-only' ? `throw new TypeError('Static log write reached a dynamic-only callsite plan');` : `this._buffer._logHeaders[idx] = ((vocabularyIndex << 8) | ENTRY_TYPE_INFO) >>> 0;`}
      return this;
    }

    ` +
      // Write a debug log entry.
      `debug(message) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_DEBUG);
      ${messageLayoutFamily === 'static-only' ? `throw new TypeError('Dynamic log write reached a static-only callsite plan');` : `this._buffer.message_values[idx] = message;`}
      return this;
    }

    _debugTemplate(vocabularyIndex) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_DEBUG);
      ${messageLayoutFamily === 'dynamic-only' ? `throw new TypeError('Static log write reached a dynamic-only callsite plan');` : `this._buffer._logHeaders[idx] = ((vocabularyIndex << 8) | ENTRY_TYPE_DEBUG) >>> 0;`}
      return this;
    }

    ` +
      // Write a warn log entry.
      `warn(message, fields) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_WARN);
      ${messageLayoutFamily === 'static-only' ? `throw new TypeError('Dynamic log write reached a static-only callsite plan');` : `this._buffer.message_values[idx] = helpers.normalizeOperationalTemplate(message);`}
      if (fields !== undefined) {
        this.with(fields);
      }
      return this;
    }

    _warnTemplate(vocabularyIndex) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_WARN);
      ${messageLayoutFamily === 'dynamic-only' ? `throw new TypeError('Static log write reached a dynamic-only callsite plan');` : `this._buffer._logHeaders[idx] = ((vocabularyIndex << 8) | ENTRY_TYPE_WARN) >>> 0;`}
      return this;
    }

    ` +
      // Write an error log entry.
      `error(message, fields) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_ERROR);
      ${messageLayoutFamily === 'static-only' ? `throw new TypeError('Dynamic log write reached a static-only callsite plan');` : `this._buffer.message_values[idx] = helpers.normalizeOperationalTemplate(message);`}
      if (fields !== undefined) {
        this.with(fields);
      }
      return this;
    }

    _errorTemplate(vocabularyIndex) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_ERROR);
      ${messageLayoutFamily === 'dynamic-only' ? `throw new TypeError('Static log write reached a dynamic-only callsite plan');` : `this._buffer._logHeaders[idx] = ((vocabularyIndex << 8) | ENTRY_TYPE_ERROR) >>> 0;`}
      return this;
    }

    ` +
      // Write a trace log entry.
      `trace(message) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_TRACE);
      ${messageLayoutFamily === 'static-only' ? `throw new TypeError('Dynamic log write reached a static-only callsite plan');` : `this._buffer.message_values[idx] = message;`}
      return this;
    }

    _traceTemplate(vocabularyIndex) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_TRACE);
      ${messageLayoutFamily === 'dynamic-only' ? `throw new TypeError('Static log write reached a dynamic-only callsite plan');` : `this._buffer._logHeaders[idx] = ((vocabularyIndex << 8) | ENTRY_TYPE_TRACE) >>> 0;`}
      return this;
    }

    ` +
      // Set multiple attributes on the current log entry.
      `with(attributes) {
      for (const key in attributes) {
        const value = attributes[key];
        if (value === null || value === undefined) {
          continue;
        }
        const setter = this[key];
        if (typeof setter === 'function') {
          setter.call(this, value);
        }
      }
      return this;
    }

    ` +
      // Write a feature flag access entry (internal method, not on public type).
      // Called by FeatureFlagEvaluator to log flag access.
      `ffAccess(flagName, value) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_FF_ACCESS);
      ${messageLayoutFamily === 'static-only' ? `throw new TypeError('Feature flag write reached a static-only callsite plan');` : `this._buffer.message_values[idx] = flagName;`}
      if (this._buffer.ff_value_values) {
        const strValue = value === null || value === undefined ? 'null' : String(value);
        this._buffer.ff_value_values[idx] = strValue;
        if (this._buffer.ff_value_nulls) {
          helpers.setNullBit(this._buffer.ff_value_nulls, idx);
        }
      }
    }

    ` +
      // Write a feature flag usage entry (internal method, not on public type).
      // Called by FeatureFlagEvaluator to log flag usage.
      `ffUsage(flagName, context) {
      const idx = this._state._appendWriterEntry(ENTRY_TYPE_FF_USAGE);
      ${messageLayoutFamily === 'static-only' ? `throw new TypeError('Feature flag write reached a static-only callsite plan');` : `this._buffer.message_values[idx] = flagName;`}
      if (context) {
        this.with(context);
      }
      return this;
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
      enumLookup,
    },
  };
}

/**
 * Cache for generated SpanLogger classes per schema.
 */
const spanLoggerClassCache = new WeakMap<LogSchema, Map<string, unknown>>();

function isSpanLoggerConstructor<T extends LogSchema>(value: unknown): value is SpanLoggerConstructor<T> {
  return typeof value === 'function';
}

function generateStateBoundSpanLoggerClass(
  schema: LogSchema,
  extension: ColumnWriterExtension,
): string {
  const source = generateColumnWriterClass(schema, 'GeneratedSpanLogger', extension);
  const constructorStart = source.indexOf('    constructor(buffer) {');
  const firstSetter = schema._columnNames[0];
  const methodsStart = source.indexOf(
    firstSetter === undefined ? '\n    info(message, fields) {' : `\n    ${firstSetter}(value) {`,
    constructorStart,
  );
  if (constructorStart < 0 || methodsStart < 0) {
    throw new Error('Failed to locate generated SpanLogger class boundaries');
  }
  const stateConstructor = `    constructor(state) {\n      this._state = state;\n    }\n`;
  return (source.slice(0, constructorStart) + stateConstructor + source.slice(methodsStart + 1))
    .replaceAll('this._buffer', 'this._state._buffer')
    .replaceAll('this._writeIndex', '(this._state._buffer._writeIndex - 1)');
}

/**
 * Create the state-only SpanLogger constructor selected for one schema/layout family.
 */
export function createSpanLoggerClass<T extends LogSchema>(
  schema: T,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  eagerColumns: readonly string[] = [],
  enumLookup: SchemaEnumLookupDescriptor = resolveEnumLookupDescriptor(schema),
): SpanLoggerConstructor<T> {
  const cacheKey = `${messageLayoutFamily}:${eagerColumns.join('\u0000')}`;
  let familyClasses = spanLoggerClassCache.get(schema);
  let SpanLoggerClass = familyClasses?.get(cacheKey);

  if (!SpanLoggerClass) {
    const extension = buildSpanLoggerExtension(schema, messageLayoutFamily, eagerColumns, enumLookup);
    const classCode = generateStateBoundSpanLoggerClass(schema, extension).trim();
    SpanLoggerClass = new Function('helpers', 'enumLookup', classCode)(SPAN_LOGGER_HELPERS, enumLookup);

    if (!isSpanLoggerConstructor<LogSchema>(SpanLoggerClass)) {
      throw new Error('Failed to generate SpanLogger constructor');
    }

    familyClasses ??= new Map();
    familyClasses.set(cacheKey, SpanLoggerClass);
    spanLoggerClassCache.set(schema, familyClasses);
  }

  if (!isSpanLoggerConstructor<T>(SpanLoggerClass)) {
    throw new Error('Invalid cached SpanLogger constructor');
  }

  return SpanLoggerClass;
}

/** Create a SpanLogger bound to an existing SpanContext writer state. */
export function createSpanLogger<T extends LogSchema>(schema: T, state: WriterState): SpanLoggerImpl<T> {
  return new (createSpanLoggerClass(schema))(state);
}
//#endregion smoo/lmao!n/codegen-spanlogger
