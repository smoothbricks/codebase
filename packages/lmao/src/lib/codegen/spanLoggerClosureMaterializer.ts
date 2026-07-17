//#region smoo/lmao!n/codegen-spanlogger.closure
/**
 * Closure-composed (no-eval) materializer for state-bound SpanLogger classes.
 *
 * Production workerd forbids code generation from strings (EvalError), so the
 * compiled renderer in spanLoggerGenerator.ts cannot run there. This module
 * assembles the SAME class shape from per-field closures instead of source
 * text, mirroring arrow-builder's closureMaterializers.ts:
 * - one shared prototype per schema/config plan,
 * - methods installed with { value, writable: true, configurable: true, enumerable: false },
 * - accessors installed with { get, configurable: true, enumerable: false },
 * - constructors assign instance properties in the same order (V8 hidden classes).
 *
 * Every closure body mirrors the compiled class member it replaces, including
 * evaluation order (append -> message write -> fields), guard conditions, and
 * thrown error messages that callers observe.
 */

import type { SchemaType } from '@smoothbricks/arrow-builder';
import type { SchemaEnumLookupDescriptor } from '../enumMetadata.js';
import type { MessageLayoutFamily, MessagePhysicalLayout } from '../runtimeHint.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_FF_ACCESS,
  ENTRY_TYPE_FF_USAGE,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from '../schema/systemSchema.js';
import type { VocabularyGeneration } from '../vocabularyRegistry.js';

// ============================================================================
// Plan (pure data) + injected singleton dependencies
// ============================================================================

/** Structural type of SPAN_LOGGER_HELPERS (kept local to avoid an import cycle). */
export interface SpanLoggerRuntimeHelpers {
  normalizeOperationalTemplate: (template: string) => string;
  setNullBit: (bitmap: Uint8Array, idx: number) => void;
  decodeVocabularyMessage: (generation: VocabularyGeneration, denseIndex: number) => string;
  fillNullBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number) => void;
  fillBooleanBitmapRange: (bitmap: Uint8Array, startIdx: number, endIdx: number, value: boolean) => void;
}

/**
 * How a schema column's storage is allocated and guarded (mirrors
 * arrow-builder's ColumnAccessMode for the state-bound writer surface):
 * - eager: allocated in the buffer constructor, no null bitmap
 * - preallocated: allocated in the buffer constructor WITH a null bitmap
 * - lazy: allocated on first public-getter access
 */
export type SpanLoggerColumnMode = 'eager' | 'preallocated' | 'lazy';

/** One schema column of the SpanLogger plan. */
export interface SpanLoggerColumnPlan {
  readonly name: string;
  readonly schemaType: SchemaType | undefined;
  readonly mode: SpanLoggerColumnMode;
}

/**
 * Pure-data description of one generated SpanLogger class, shared by the
 * compiled renderer and this closure materializer. `helpers`/`enumLookup` are
 * the same stable singletons the compiled path injects via `new Function`
 * parameters (extension.dependencies).
 */
export interface SpanLoggerPlan {
  readonly columns: readonly SpanLoggerColumnPlan[];
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly messagePhysicalLayout: MessagePhysicalLayout;
  readonly helpers: SpanLoggerRuntimeHelpers;
  readonly enumLookup: SchemaEnumLookupDescriptor;
}

// ============================================================================
// Structural views (generated members are schema-dynamic, typed unknown)
// ============================================================================

/** Span buffer view with schema-dynamic lanes visible for keyed access. */
interface SpanBufferView {
  [key: string]: unknown;
  _writeIndex: number;
  _capacity: number;
  _scopeValues?: Readonly<Record<string, unknown>>;
  _vocabularyGeneration: VocabularyGeneration;
}

/** The SpanContext-owned writer state every generated method dereferences. */
interface WriterStateView {
  _buffer: SpanBufferView;
  readonly _physicalLayoutPlan: { encodeLocalMessage(vocabularyIndex: number): number };
  _appendWriterEntry(entryType: number): number;
}

/** Structural view of a closure-composed logger instance used by closures. */
interface LoggerInstance {
  [key: string]: unknown;
  _state: WriterStateView;
}

/** Anything a column write can index-assign into (TypedArray or JS Array). */
interface IndexWritable {
  [pos: number]: unknown;
}

function isIndexWritable(value: unknown): value is IndexWritable {
  return typeof value === 'object' && value !== null;
}

function laneOf(value: unknown): IndexWritable {
  if (isIndexWritable(value)) return value;
  throw new TypeError('Expected an indexable column lane');
}

function bitmapOf(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  throw new TypeError('Expected a Uint8Array bitmap lane');
}

/** Lane that supports range fill (TypedArray or JS Array). */
interface RangeFillable {
  fill(value: unknown, start: number, end: number): unknown;
}

function isRangeFillable(value: unknown): value is RangeFillable {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, 'fill') === 'function';
}

function fillableOf(value: unknown): RangeFillable {
  if (isRangeFillable(value)) return value;
  throw new TypeError('Expected a fillable column lane');
}

/** Dynamic `this.with(...)` dispatch (the compiled methods call through `this`). */
function callFluentWith(self: LoggerInstance, attributes: unknown): void {
  const withFn = self.with;
  if (typeof withFn !== 'function') throw new TypeError('SpanLogger.with is not callable');
  withFn.call(self, attributes);
}

const METHOD_DESCRIPTOR = { writable: true, configurable: true, enumerable: false } as const;
const ACCESSOR_DESCRIPTOR = { configurable: true, enumerable: false } as const;

type FluentMethod = (this: LoggerInstance, value: unknown) => LoggerInstance;

// ============================================================================
// Schema column fluent setters (state-bound mirror of the base ColumnWriter
// setters after the `this._buffer` -> `this._state._buffer` and
// `this._writeIndex` -> `(this._state._buffer._writeIndex - 1)` rewrites)
// ============================================================================

function makeStateBoundSetter(column: SpanLoggerColumnPlan): FluentMethod {
  const { name, schemaType, mode } = column;
  // Preallocated setters write through the raw backing properties; eager and
  // lazy setters go through the buffer's public getters (which allocate lazy
  // columns on first access).
  const nullsKey = mode === 'preallocated' ? `_${name}_nulls` : `${name}_nulls`;
  const valuesKey = mode === 'preallocated' ? `_${name}_values` : `${name}_values`;
  const hasNulls = mode !== 'eager';
  const lookupKey = `_${name}_enumLookup`;
  const enumValuesKey = `${name}_enumValues`;

  if (schemaType === 'boolean') {
    if (hasNulls) {
      return function (value) {
        const buf = this._state._buffer;
        const idx = buf._writeIndex - 1;
        const byteIdx = idx >>> 3;
        const bitMask = 1 << (idx & 7);
        bitmapOf(buf[nullsKey])[byteIdx] |= bitMask;
        const values = bitmapOf(buf[valuesKey]);
        if (value) values[byteIdx] |= bitMask;
        else values[byteIdx] &= ~bitMask;
        return this;
      };
    }
    return function (value) {
      const buf = this._state._buffer;
      const idx = buf._writeIndex - 1;
      const byteIdx = idx >>> 3;
      const bitMask = 1 << (idx & 7);
      const values = bitmapOf(buf[valuesKey]);
      if (value) values[byteIdx] |= bitMask;
      else values[byteIdx] &= ~bitMask;
      return this;
    };
  }

  if (schemaType === 'enum') {
    // Base enum setter. Reachable only for enum fields absent from
    // enumLookup.ordered (the fluent enum overrides shadow it otherwise) —
    // the compiled class throws there too because its constructor-built
    // `_<name>_enumLookup` Maps were dropped with the state-bound rewrite.
    const makeEnumWrite = (writeNulls: boolean): FluentMethod =>
      function (value) {
        const buf = this._state._buffer;
        const idx = buf._writeIndex - 1;
        const lookup = this[lookupKey];
        if (!(lookup instanceof Map)) {
          throw new TypeError(`Enum lookup ${lookupKey} is not initialized (buffer has no enumValues).`);
        }
        const enumIndex: unknown = lookup.get(value);
        if (enumIndex === undefined) {
          const enumValues = buf[enumValuesKey];
          throw new Error(
            `Invalid enum value "${value}" for field "${name}". Valid values: ${Array.isArray(enumValues) ? enumValues.join(', ') : ''}`,
          );
        }
        if (writeNulls) bitmapOf(buf[nullsKey])[idx >>> 3] |= 1 << (idx & 7);
        laneOf(buf[valuesKey])[idx] = enumIndex;
        return this;
      };
    return makeEnumWrite(hasNulls);
  }

  if (schemaType === 'binary') {
    // Shallow-freeze binary payload objects (mirrors the compiled binary setter).
    const freezeBinaryValue = (value: unknown): void => {
      if (typeof value === 'object' && value !== null && !(value instanceof Uint8Array)) Object.freeze(value);
    };
    if (hasNulls) {
      return function (value) {
        const buf = this._state._buffer;
        const idx = buf._writeIndex - 1;
        freezeBinaryValue(value);
        bitmapOf(buf[nullsKey])[idx >>> 3] |= 1 << (idx & 7);
        laneOf(buf[valuesKey])[idx] = value;
        return this;
      };
    }
    return function (value) {
      const buf = this._state._buffer;
      const idx = buf._writeIndex - 1;
      freezeBinaryValue(value);
      laneOf(buf[valuesKey])[idx] = value;
      return this;
    };
  }

  if (hasNulls) {
    return function (value) {
      const buf = this._state._buffer;
      const idx = buf._writeIndex - 1;
      bitmapOf(buf[nullsKey])[idx >>> 3] |= 1 << (idx & 7);
      laneOf(buf[valuesKey])[idx] = value;
      return this;
    };
  }
  return function (value) {
    const buf = this._state._buffer;
    const idx = buf._writeIndex - 1;
    laneOf(buf[valuesKey])[idx] = value;
    return this;
  };
}

// ============================================================================
// SpanLogger materializer
// ============================================================================

/**
 * Materialize a state-bound SpanLogger class from a plan without string
 * codegen. Same observable behavior as the compiled renderer. Returned as
 * `unknown` to flow through the same constructor type guard as the compiled
 * factory (isSpanLoggerConstructor).
 */
export function materializeSpanLoggerClass(plan: SpanLoggerPlan): unknown {
  const { helpers, enumLookup } = plan;
  const family = plan.messageLayoutFamily;
  const physical = plan.messagePhysicalLayout;

  class GeneratedSpanLogger {
    [key: string]: unknown;

    declare _state: WriterStateView;

    constructor(state: WriterStateView) {
      this._state = state;
    }
  }

  const proto = GeneratedSpanLogger.prototype;
  const defineMethod = (name: string, value: unknown): void => {
    Object.defineProperty(proto, name, { ...METHOD_DESCRIPTOR, value });
  };

  // --------------------------------------------------------------------------
  // Message writes per layout family / physical layout
  // --------------------------------------------------------------------------

  /** Operational entry (info/warn/error): normalized message + optional fields. */
  const makeOperationalLogMethod = (entryType: number) => {
    if (family === 'static-only') {
      return function (this: LoggerInstance): LoggerInstance {
        this._state._appendWriterEntry(entryType);
        throw new TypeError('Dynamic log write reached a static-only callsite plan');
      };
    }
    return function (this: LoggerInstance, message: string, fields?: Record<string, unknown>): LoggerInstance {
      const state = this._state;
      const idx = state._appendWriterEntry(entryType);
      laneOf(state._buffer.message_values)[idx] = helpers.normalizeOperationalTemplate(message);
      if (fields !== undefined) {
        callFluentWith(this, fields);
      }
      return this;
    };
  };

  /** Plain entry (debug/trace): raw message, no fields argument. */
  const makePlainLogMethod = (entryType: number) => {
    if (family === 'static-only') {
      return function (this: LoggerInstance): LoggerInstance {
        this._state._appendWriterEntry(entryType);
        throw new TypeError('Dynamic log write reached a static-only callsite plan');
      };
    }
    return function (this: LoggerInstance, message: string): LoggerInstance {
      const state = this._state;
      const idx = state._appendWriterEntry(entryType);
      laneOf(state._buffer.message_values)[idx] = message;
      return this;
    };
  };

  /** Static template entry (_infoTemplate & co.) per physical layout. */
  const makeTemplateLogMethod = (entryType: number) => {
    if (family === 'dynamic-only') {
      return function (this: LoggerInstance): LoggerInstance {
        this._state._appendWriterEntry(entryType);
        throw new TypeError('Static log write reached a dynamic-only callsite plan');
      };
    }
    switch (physical) {
      case 'current':
        return function (this: LoggerInstance, vocabularyIndex: number): LoggerInstance {
          const state = this._state;
          const idx = state._appendWriterEntry(entryType);
          const localMessageId = state._physicalLayoutPlan.encodeLocalMessage(vocabularyIndex);
          if (localMessageId === 0) {
            laneOf(state._buffer.message_values)[idx] = helpers.decodeVocabularyMessage(
              state._buffer._vocabularyGeneration,
              vocabularyIndex,
            );
          } else {
            laneOf(state._buffer._messageIds)[idx] = localMessageId;
          }
          return this;
        };
      case 'specialized':
        return function (this: LoggerInstance, vocabularyIndex: number): LoggerInstance {
          const state = this._state;
          const idx = state._appendWriterEntry(entryType);
          laneOf(state._buffer._logHeaders)[idx] = vocabularyIndex + 1;
          return this;
        };
      case 'packed':
        return function (this: LoggerInstance, vocabularyIndex: number): LoggerInstance {
          const state = this._state;
          const idx = state._appendWriterEntry(entryType);
          if (vocabularyIndex > 0x00fffffe) throw new RangeError('Packed message dense index exceeds 0xFFFFFE');
          laneOf(state._buffer._rowHeaders)[idx] = (((vocabularyIndex + 1) << 8) | entryType) >>> 0;
          return this;
        };
    }
  };

  /** Row-lane setter for fixed system columns (line, error_code, ...). */
  const makeRowLaneSetter = (valuesKey: string, nullsKey: string): FluentMethod =>
    function (value) {
      const buf = this._state._buffer;
      const idx = buf._writeIndex - 1;
      const values = buf[valuesKey];
      if (values) {
        laneOf(values)[idx] = value;
        const nulls = buf[nullsKey];
        if (nulls) {
          helpers.setNullBit(bitmapOf(nulls), idx);
        }
      }
      return this;
    };

  // --------------------------------------------------------------------------
  // Prototype assembly — SAME member order as the compiled class body, so
  // name collisions shadow identically (schema setters first, extension
  // methods next, enum fluent overrides last).
  // --------------------------------------------------------------------------

  for (const column of plan.columns) {
    defineMethod(column.name, makeStateBoundSetter(column));
  }

  defineMethod('info', makeOperationalLogMethod(ENTRY_TYPE_INFO));
  defineMethod('_infoTemplate', makeTemplateLogMethod(ENTRY_TYPE_INFO));
  defineMethod('debug', makePlainLogMethod(ENTRY_TYPE_DEBUG));
  defineMethod('_debugTemplate', makeTemplateLogMethod(ENTRY_TYPE_DEBUG));
  defineMethod('warn', makeOperationalLogMethod(ENTRY_TYPE_WARN));
  defineMethod('_warnTemplate', makeTemplateLogMethod(ENTRY_TYPE_WARN));
  defineMethod('error', makeOperationalLogMethod(ENTRY_TYPE_ERROR));
  defineMethod('_errorTemplate', makeTemplateLogMethod(ENTRY_TYPE_ERROR));
  defineMethod('trace', makePlainLogMethod(ENTRY_TYPE_TRACE));
  defineMethod('_traceTemplate', makeTemplateLogMethod(ENTRY_TYPE_TRACE));

  defineMethod('with', function (this: LoggerInstance, attributes: Record<string, unknown>): LoggerInstance {
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
  });

  defineMethod(
    'ffAccess',
    family === 'static-only'
      ? function (this: LoggerInstance): void {
          this._state._appendWriterEntry(ENTRY_TYPE_FF_ACCESS);
          throw new TypeError('Feature flag write reached a static-only callsite plan');
        }
      : function (this: LoggerInstance, flagName: string, value: unknown): void {
          const state = this._state;
          const idx = state._appendWriterEntry(ENTRY_TYPE_FF_ACCESS);
          laneOf(state._buffer.message_values)[idx] = flagName;
          const ffValues = state._buffer.ff_value_values;
          if (ffValues) {
            const strValue = value === null || value === undefined ? 'null' : String(value);
            laneOf(ffValues)[idx] = strValue;
            const ffNulls = state._buffer.ff_value_nulls;
            if (ffNulls) {
              helpers.setNullBit(bitmapOf(ffNulls), idx);
            }
          }
        },
  );

  defineMethod(
    'ffUsage',
    family === 'static-only'
      ? function (this: LoggerInstance): LoggerInstance {
          this._state._appendWriterEntry(ENTRY_TYPE_FF_USAGE);
          throw new TypeError('Feature flag write reached a static-only callsite plan');
        }
      : function (this: LoggerInstance, flagName: string, context: unknown): LoggerInstance {
          const state = this._state;
          const idx = state._appendWriterEntry(ENTRY_TYPE_FF_USAGE);
          laneOf(state._buffer.message_values)[idx] = flagName;
          if (context) {
            callFluentWith(this, context);
          }
          return this;
        },
  );

  defineMethod('line', makeRowLaneSetter('line_values', 'line_nulls'));
  defineMethod('error_code', makeRowLaneSetter('error_code_values', 'error_code_nulls'));
  defineMethod('exception_stack', makeRowLaneSetter('exception_stack_values', 'exception_stack_nulls'));
  defineMethod('ff_value', makeRowLaneSetter('ff_value_values', 'ff_value_nulls'));
  defineMethod('uint64_value', makeRowLaneSetter('uint64_value_values', 'uint64_value_nulls'));

  Object.defineProperty(proto, 'scope', {
    ...ACCESSOR_DESCRIPTOR,
    get(this: LoggerInstance) {
      return this._state._buffer._scopeValues;
    },
  });

  defineMethod('_setScope', function (this: LoggerInstance, attributes: Record<string, unknown>): void {
    const buf = this._state._buffer;
    const current = buf._scopeValues || {};
    const next: Record<string, unknown> = { ...current };

    for (const key of Object.keys(attributes)) {
      const value = attributes[key];
      if (value === null) {
        delete next[key];
      } else if (value !== undefined) {
        next[key] = value;
      }
    }

    buf._scopeValues = Object.freeze(next);
  });

  defineMethod('_prefillScopedAttributesOn', makePrefillScopedAttributes(plan));

  // Fluent enum overrides: convert the declared enum string to its
  // schema-order index (shadow the base enum setters, exactly like the
  // compiled class where they appear last in the class body).
  for (const { fieldName } of enumLookup.ordered) {
    const encode = enumLookup.byField[fieldName].encode;
    defineMethod(fieldName, function (this: LoggerInstance, value: unknown): LoggerInstance {
      const idx = encode(value);
      const buf = this._state._buffer;
      const write = buf[fieldName];
      if (typeof write !== 'function') throw new TypeError(`Span buffer has no ${fieldName} setter method`);
      write.call(buf, buf._writeIndex - 1, idx);
      return this;
    });
  }

  return GeneratedSpanLogger;
}

// ============================================================================
// Scoped attribute prefill (mirrors generatePrefillScopedAttributesMethod)
// ============================================================================

type ColumnPrefill = (buffer: SpanBufferView, startIdx: number, endIdx: number) => void;

function makeColumnPrefill(
  column: SpanLoggerColumnPlan,
  encode: ((value: unknown) => number) | undefined,
  helpers: SpanLoggerRuntimeHelpers,
): ColumnPrefill | undefined {
  const { name, schemaType } = column;

  // Binary columns are not scope-fillable (object payloads can't be bulk-filled)
  if (schemaType === 'binary') return undefined;

  const rawValuesKey = `_${name}_values`;
  const valuesKey = `${name}_values`;
  const nullsKey = `${name}_nulls`;

  // Boolean uses bit-packed storage - bulk fill using helper
  if (schemaType === 'boolean') {
    return (buffer, startIdx, endIdx) => {
      const scopeValue = buffer._scopeValues?.[name];
      if (scopeValue !== null && scopeValue !== undefined && buffer[rawValuesKey] !== undefined) {
        helpers.fillBooleanBitmapRange(bitmapOf(buffer[valuesKey]), startIdx, endIdx, Boolean(scopeValue));
        helpers.fillNullBitmapRange(bitmapOf(buffer[nullsKey]), startIdx, endIdx);
      }
    };
  }

  // For string arrays (category/text), use manual loop instead of fill()
  if (schemaType === 'category' || schemaType === 'text') {
    return (buffer, startIdx, endIdx) => {
      const scopeValue = buffer._scopeValues?.[name];
      if (scopeValue !== null && scopeValue !== undefined && buffer[rawValuesKey] !== undefined) {
        const values = laneOf(buffer[valuesKey]);
        for (let i = startIdx; i < endIdx; i++) {
          values[i] = scopeValue;
        }
        helpers.fillNullBitmapRange(bitmapOf(buffer[nullsKey]), startIdx, endIdx);
      }
    };
  }

  // Enum values are encoded to their schema-order index before filling
  if (schemaType === 'enum' && encode !== undefined) {
    return (buffer, startIdx, endIdx) => {
      const scopeValue = buffer._scopeValues?.[name];
      if (scopeValue !== null && scopeValue !== undefined && buffer[rawValuesKey] !== undefined) {
        fillableOf(buffer[valuesKey]).fill(encode(scopeValue), startIdx, endIdx);
        helpers.fillNullBitmapRange(bitmapOf(buffer[nullsKey]), startIdx, endIdx);
      }
    };
  }

  return (buffer, startIdx, endIdx) => {
    const scopeValue = buffer._scopeValues?.[name];
    if (scopeValue !== null && scopeValue !== undefined && buffer[rawValuesKey] !== undefined) {
      fillableOf(buffer[valuesKey]).fill(scopeValue, startIdx, endIdx);
      helpers.fillNullBitmapRange(bitmapOf(buffer[nullsKey]), startIdx, endIdx);
    }
  };
}

function makePrefillScopedAttributes(plan: SpanLoggerPlan): (this: LoggerInstance, buffer: SpanBufferView) => void {
  const { helpers, enumLookup } = plan;
  const orderedEnumFields = new Set(enumLookup.ordered.map(({ fieldName }) => fieldName));
  const prefills: ColumnPrefill[] = [];

  for (const column of plan.columns) {
    const encode = orderedEnumFields.has(column.name) ? enumLookup.byField[column.name].encode : undefined;
    const prefill = makeColumnPrefill(column, encode, helpers);
    if (prefill !== undefined) prefills.push(prefill);
  }

  // Initial buffer: rows 0-1 reserved for span-start/end, data starts at _writeIndex=2
  // Overflow buffer: no reserved rows, data starts at _writeIndex=0
  // Use buffer._writeIndex as startIdx to handle both cases correctly
  return (buffer) => {
    const startIdx = buffer._writeIndex;
    const endIdx = buffer._capacity;
    for (let i = 0; i < prefills.length; i++) {
      prefills[i](buffer, startIdx, endIdx);
    }
  };
}
//#endregion smoo/lmao!n/codegen-spanlogger.closure
