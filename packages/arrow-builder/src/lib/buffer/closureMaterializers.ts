/**
 * Closure-composed (no-eval) materializers for ColumnBuffer and ColumnWriter
 * classes.
 *
 * Production workerd forbids code generation from strings, so the compiled
 * materializer (`new Function` over rendered class source) cannot run there.
 * This module builds the SAME classes from an accessor plan without any
 * source text: one shared prototype per schema, methods installed with
 * Object.defineProperty as per-field lambdas closing over (storage kind,
 * property keys, enum values). The closures are monomorphic per schema —
 * every instance of a schema shares one prototype and one set of closures —
 * so V8 optimizes the call sites; per-row cost is the closure call, not
 * shape lookup.
 *
 * Behavioral contract: byte-identical buffers to the compiled materializer
 * (asserted by materializerParity.test.ts). Code-string extension fields
 * cannot be honored without eval and are rejected up front.
 */

import type { ColumnAccessorPlan, ColumnPlanEntry, ColumnStorageInfo } from './accessorPlan.js';
import { getAlignedCapacity } from './bufferHelpers.js';
import type { AnyColumnBuffer, ColumnValueType, TypedArray } from './types.js';

// ============================================================================
// Extension compatibility
// ============================================================================

/** Extension fields that carry raw JavaScript source and therefore need eval. */
const CODE_EXTENSION_FIELDS = [
  'classPreamble',
  'constructorPreamble',
  'constructorCode',
  'constructorParams',
  'methods',
] as const;

interface CodeStringExtensionFields {
  classPreamble?: string;
  constructorPreamble?: string;
  constructorCode?: string;
  constructorParams?: string;
  methods?: string;
  closureInit?: ClosureInitHook;
  closureMethods?: ClosureMethodsHook;
}

/** No-eval counterpart of constructorPreamble/constructorParams (see ColumnBufferExtension). */
export type ClosureInitHook = (
  self: Record<string, unknown>,
  requestedCapacity: number,
  ctorArgs: readonly unknown[],
) => void;
/** No-eval counterpart of methods/classPreamble (see ColumnBufferExtension). */
export type ClosureMethodsHook = (prototype: object) => void;

/**
 * Reject extensions the closure-composed materializer cannot honor.
 *
 * `preallocatedColumns` is pure data and fully supported. The code-string
 * fields require `new Function` UNLESS the extension carries their no-eval
 * counterparts: `closureInit` covers constructorPreamble + constructorParams,
 * `closureMethods` covers methods + classPreamble. `constructorCode` has no
 * counterpart — it MUST either run where string codegen is allowed or be
 * pregenerated at build time.
 */
export function assertClosureMaterializable(extension: CodeStringExtensionFields | undefined, what: string): void {
  if (!extension) return;
  const offending = CODE_EXTENSION_FIELDS.filter((field) => {
    const value = extension[field];
    if (typeof value !== 'string' || value.length === 0) return false;
    if ((field === 'constructorPreamble' || field === 'constructorParams') && extension.closureInit) return false;
    if ((field === 'methods' || field === 'classPreamble') && extension.closureMethods) return false;
    return true;
  });
  if (offending.length > 0) {
    throw new Error(
      `${what} extension fields [${offending.join(', ')}] carry code strings, which require string codegen ` +
        '(new Function) — unavailable in this runtime (e.g. workerd). Provide the closureInit/closureMethods ' +
        'no-eval counterparts, pregenerate the class at build time, or drop the code-string extension fields.',
    );
  }
}

// ============================================================================
// Narrowing helpers (instance column slots are schema-dynamic, typed unknown)
// ============================================================================

/** Anything a column setter can index-assign into (TypedArray or JS Array). */
interface IndexWritable {
  [pos: number]: unknown;
}

function isIndexWritable(value: unknown): value is IndexWritable {
  return typeof value === 'object' && value !== null;
}

function indexWritableOf(value: unknown): IndexWritable {
  if (isIndexWritable(value)) return value;
  throw new TypeError('Column values array is not allocated.');
}

function bitmapOf(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  throw new TypeError('Column bitmap is not allocated.');
}

function isTypedArrayColumn(value: unknown): value is TypedArray {
  return ArrayBuffer.isView(value) && !(value instanceof DataView);
}

// ============================================================================
// Storage kinds
// ============================================================================

type ValuesArrayConstructor =
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor
  | Uint32ArrayConstructor
  | Float64ArrayConstructor
  | BigUint64ArrayConstructor;

/** TypedArray constructors by plan name ('Array' is deliberately absent). */
const VALUES_CTORS: Record<string, ValuesArrayConstructor> = {
  Uint8Array,
  Uint16Array,
  Uint32Array,
  Float64Array,
  BigUint64Array,
};

/** Default written by eager setters when null is passed (mirrors getDefaultValueLiteral). */
function eagerDefaultValue(storage: ColumnStorageInfo): unknown {
  const { schemaType, isBitPacked } = storage;
  if (isBitPacked || schemaType === 'boolean') return false;
  if (schemaType === 'enum' || schemaType === 'number') return 0;
  if (schemaType === 'bigUint64') return 0n;
  if (schemaType === 'category' || schemaType === 'text') return '';
  if (schemaType === 'binary') return null;
  return 0;
}

// ============================================================================
// ColumnBuffer materializer
// ============================================================================

/** Structural view of a closure-composed buffer instance used by per-field closures. */
interface BufferInstance {
  [key: string]: unknown;
  _alignedCapacity: number;
}

type BufferInit = (self: BufferInstance, alignedCapacity: number) => void;
type BufferAllocator = (self: BufferInstance) => Uint8Array;
type ValueWriter = (self: BufferInstance, pos: number, val: unknown) => void;

/**
 * Allocate a lazy column's nulls AND values on first access
 * (mirrors generateInlineAllocation).
 */
function makeLazyAllocator(storage: ColumnStorageInfo, rawNulls: string, rawValues: string): BufferAllocator {
  if (storage.isBitPacked) {
    // Bit-packed boolean: nulls and values both use 1 bit per element
    return (self) => {
      const cap = self._alignedCapacity;
      const bitmapSize = (cap + 7) >>> 3;
      const buf = new ArrayBuffer(bitmapSize + bitmapSize);
      const nulls = new Uint8Array(buf, 0, bitmapSize);
      self[rawNulls] = nulls;
      self[rawValues] = new Uint8Array(buf, bitmapSize, bitmapSize);
      return nulls;
    };
  }
  if (storage.constructorName === 'Array') {
    // String array: nulls is bit-packed Uint8Array, values is JS Array (can't share ArrayBuffer)
    return (self) => {
      const cap = self._alignedCapacity;
      const nullSize = (cap + 7) >>> 3;
      const nulls = new Uint8Array(nullSize);
      self[rawNulls] = nulls;
      self[rawValues] = new Array(cap);
      return nulls;
    };
  }
  // TypedArray: shared ArrayBuffer with aligned offset
  const Values = VALUES_CTORS[storage.constructorName];
  const bytesPerElement = storage.bytesPerElement;
  const shift = Math.log2(bytesPerElement);
  return (self) => {
    const cap = self._alignedCapacity;
    const nullSize = (cap + 7) >>> 3;
    const alignedOffset = ((nullSize + bytesPerElement - 1) >>> shift) << shift;
    const buf = new ArrayBuffer(alignedOffset + cap * bytesPerElement);
    const nulls = new Uint8Array(buf, 0, nullSize);
    self[rawNulls] = nulls;
    self[rawValues] = new Values(buf, alignedOffset, cap);
    return nulls;
  };
}

/** Constructor allocation for a nullable preallocated column (mirrors generatePreallocatedAllocation). */
function makePreallocatedInit(storage: ColumnStorageInfo, rawNulls: string, rawValues: string): BufferInit {
  if (storage.isBitPacked) {
    return (self, alignedCapacity) => {
      const bitmapSize = (alignedCapacity + 7) >>> 3;
      const buf = new ArrayBuffer(bitmapSize + bitmapSize);
      self[rawNulls] = new Uint8Array(buf, 0, bitmapSize);
      self[rawValues] = new Uint8Array(buf, bitmapSize, bitmapSize);
    };
  }
  if (storage.constructorName === 'Array') {
    return (self, alignedCapacity) => {
      const nullSize = (alignedCapacity + 7) >>> 3;
      self[rawNulls] = new Uint8Array(nullSize);
      self[rawValues] = new Array(alignedCapacity);
    };
  }
  const Values = VALUES_CTORS[storage.constructorName];
  const bytesPerElement = storage.bytesPerElement;
  const shift = Math.log2(bytesPerElement);
  return (self, alignedCapacity) => {
    const nullSize = (alignedCapacity + 7) >>> 3;
    const alignedOffset = ((nullSize + bytesPerElement - 1) >>> shift) << shift;
    const buf = new ArrayBuffer(alignedOffset + alignedCapacity * bytesPerElement);
    self[rawNulls] = new Uint8Array(buf, 0, nullSize);
    self[rawValues] = new Values(buf, alignedOffset, alignedCapacity);
  };
}

/** Constructor allocation for an eager column (no null bitmap). */
function makeEagerInit(storage: ColumnStorageInfo, rawValues: string): BufferInit {
  if (storage.constructorName === 'Array') {
    return (self, alignedCapacity) => {
      self[rawValues] = new Array(alignedCapacity);
    };
  }
  if (storage.isBitPacked) {
    return (self, alignedCapacity) => {
      self[rawValues] = new Uint8Array((alignedCapacity + 7) >>> 3);
    };
  }
  const Values = VALUES_CTORS[storage.constructorName];
  return (self, alignedCapacity) => {
    self[rawValues] = new Values(alignedCapacity);
  };
}

/** Raw value assignment into an (already allocated) values array (mirrors generateSetterValueAssignment). */
function makeValueWriter(storage: ColumnStorageInfo, rawValues: string): ValueWriter {
  if (storage.isBitPacked) {
    return (self, pos, val) => {
      const values = bitmapOf(self[rawValues]);
      const byteIdx = pos >>> 3;
      const bitMask = 1 << (pos & 7);
      if (val) values[byteIdx] |= bitMask;
      else values[byteIdx] &= ~bitMask;
    };
  }
  return (self, pos, val) => {
    indexWritableOf(self[rawValues])[pos] = val;
  };
}

const METHOD_DESCRIPTOR = { writable: true, configurable: true, enumerable: false } as const;
const ACCESSOR_DESCRIPTOR = { configurable: true, enumerable: false } as const;

/** No-eval extension hooks honored by the closure-composed buffer materializer. */
export interface ClosureExtensionHooks {
  closureInit?: ClosureInitHook;
  closureMethods?: ClosureMethodsHook;
}

/**
 * Materialize a ColumnBuffer class from an accessor plan without string
 * codegen. Same observable behavior as the compiled materializer with the
 * default class name; `hooks` are the no-eval counterparts of the
 * code-string extension fields (closureInit runs like constructorPreamble at
 * the top of the constructor; closureMethods installs like extension methods
 * after the per-column members).
 */
export function materializeColumnBufferClass(
  plan: ColumnAccessorPlan,
  hooks?: ClosureExtensionHooks,
): new (
  requestedCapacity: number,
  ...ctorArgs: unknown[]
) => AnyColumnBuffer {
  const inits: BufferInit[] = [];
  const closureInit = hooks?.closureInit;

  class GeneratedColumnBuffer {
    [key: string]: unknown;

    _alignedCapacity: number;
    _capacity: number;
    _overflow: AnyColumnBuffer | undefined;

    constructor(requestedCapacity: number, ...ctorArgs: unknown[]) {
      if (closureInit) closureInit(this, requestedCapacity, ctorArgs);
      const alignedCapacity = getAlignedCapacity(requestedCapacity);
      this._alignedCapacity = alignedCapacity;
      this._capacity = requestedCapacity;
      this._overflow = undefined;
      for (let i = 0; i < inits.length; i++) inits[i](this, alignedCapacity);
    }

    getColumnIfAllocated(columnName: string): ColumnValueType | undefined {
      const values = this[`_${columnName}_values`];
      if (Array.isArray(values)) return values;
      if (isTypedArrayColumn(values)) return values;
      return undefined;
    }

    getNullsIfAllocated(columnName: string): Uint8Array | undefined {
      const nulls = this[`_${columnName}_nulls`];
      return nulls instanceof Uint8Array ? nulls : undefined;
    }
  }

  const proto = GeneratedColumnBuffer.prototype;

  for (const column of plan.columns) {
    const { name, storage, mode } = column;
    const rawNulls = `_${name}_nulls`;
    const rawValues = `_${name}_values`;
    const enumValues = storage.schemaType === 'enum' ? storage.enumValues : undefined;
    const enumKey = `${name}_enumValues`;
    const writeValue = makeValueWriter(storage, rawValues);

    if (mode === 'eager') {
      const allocateEager = makeEagerInit(storage, rawValues);
      inits.push(
        enumValues
          ? (self, alignedCapacity) => {
              allocateEager(self, alignedCapacity);
              self[enumKey] = enumValues.slice();
            }
          : allocateEager,
      );

      Object.defineProperty(proto, `${name}_values`, {
        ...ACCESSOR_DESCRIPTOR,
        get(this: GeneratedColumnBuffer) {
          return this[rawValues];
        },
      });
      // Eager setter: write default for null. Defined after the accessors,
      // so (like the compiled class body) the method named `name` wins over
      // the alias getter.
      const defaultValue = eagerDefaultValue(storage);
      Object.defineProperty(proto, name, {
        ...METHOD_DESCRIPTOR,
        value: function (this: GeneratedColumnBuffer, pos: number, val: unknown) {
          writeValue(this, pos, val == null ? defaultValue : val);
          return this;
        },
      });
      continue;
    }

    if (mode === 'preallocated') {
      const allocatePreallocated = makePreallocatedInit(storage, rawNulls, rawValues);
      inits.push(
        enumValues
          ? (self, alignedCapacity) => {
              allocatePreallocated(self, alignedCapacity);
              self[enumKey] = enumValues.slice();
            }
          : allocatePreallocated,
      );

      Object.defineProperty(proto, `${name}_nulls`, {
        ...ACCESSOR_DESCRIPTOR,
        get(this: GeneratedColumnBuffer) {
          return this[rawNulls];
        },
      });
      Object.defineProperty(proto, `${name}_values`, {
        ...ACCESSOR_DESCRIPTOR,
        get(this: GeneratedColumnBuffer) {
          return this[rawValues];
        },
      });
      Object.defineProperty(proto, name, {
        ...METHOD_DESCRIPTOR,
        value: function (this: GeneratedColumnBuffer, pos: number, val: unknown) {
          const nulls = bitmapOf(this[rawNulls]);
          if (val == null) {
            nulls[pos >>> 3] &= ~(1 << (pos & 7));
          } else {
            writeValue(this, pos, val);
            nulls[pos >>> 3] |= 1 << (pos & 7);
          }
          return this;
        },
      });
      continue;
    }

    // Lazy column: initialize slots as undefined, allocate on first access.
    const allocate = makeLazyAllocator(storage, rawNulls, rawValues);
    inits.push(
      enumValues
        ? (self) => {
            self[rawNulls] = undefined;
            self[rawValues] = undefined;
            self[enumKey] = enumValues.slice();
          }
        : (self) => {
            self[rawNulls] = undefined;
            self[rawValues] = undefined;
          },
    );

    // _nulls getter: allocates BOTH nulls and values on first access
    Object.defineProperty(proto, `${name}_nulls`, {
      ...ACCESSOR_DESCRIPTOR,
      get(this: GeneratedColumnBuffer) {
        const nulls = this[rawNulls];
        return nulls === undefined ? allocate(this) : nulls;
      },
    });
    // _values getter: triggers allocation if needed, then returns values
    Object.defineProperty(proto, `${name}_values`, {
      ...ACCESSOR_DESCRIPTOR,
      get(this: GeneratedColumnBuffer) {
        if (this[rawValues] === undefined) allocate(this);
        return this[rawValues];
      },
    });
    // Lazy setter: handle null by clearing bit, valid value by setting bit
    Object.defineProperty(proto, name, {
      ...METHOD_DESCRIPTOR,
      value: function (this: GeneratedColumnBuffer, pos: number, val: unknown) {
        let nulls = this[rawNulls];
        if (nulls === undefined) nulls = allocate(this);
        const bitmap = bitmapOf(nulls);
        if (val == null) {
          bitmap[pos >>> 3] &= ~(1 << (pos & 7));
        } else {
          writeValue(this, pos, val);
          bitmap[pos >>> 3] |= 1 << (pos & 7);
        }
        return this;
      },
    });
  }

  // Custom inspect to avoid dumping huge TypedArrays in test output
  Object.defineProperty(proto, Symbol.for('nodejs.util.inspect.custom'), {
    ...METHOD_DESCRIPTOR,
    value: function (this: GeneratedColumnBuffer) {
      return `GeneratedColumnBuffer { _writeIndex: ${this._writeIndex}, _capacity: ${this._capacity}, trace_id: ${this.trace_id ?? 'N/A'} }`;
    },
  });

  // Extension methods install last so same-named members override the
  // generated ones (matching the compiled class-body order).
  hooks?.closureMethods?.(proto);

  return GeneratedColumnBuffer;
}

// ============================================================================
// ColumnWriter materializer
// ============================================================================

/** A buffer with its schema-dynamic column properties visible for keyed access. */
type DynamicColumnBuffer = AnyColumnBuffer & { [key: string]: unknown };

function toDynamicColumnBuffer(buffer: AnyColumnBuffer): DynamicColumnBuffer {
  if (isDynamicColumnBuffer(buffer)) return buffer;
  throw new TypeError('ColumnWriter requires a ColumnBuffer object.');
}

function isDynamicColumnBuffer(buffer: AnyColumnBuffer): buffer is DynamicColumnBuffer {
  return typeof buffer === 'object' && buffer !== null;
}

/** Structural view of a closure-composed writer instance used by per-field closures. */
interface WriterInstance {
  [key: string]: unknown;
  _buffer: DynamicColumnBuffer;
  _writeIndex: number;
}

type WriterInit = (self: WriterInstance) => void;

function enumLookupOf(self: WriterInstance, lookupKey: string): Map<unknown, number> {
  const lookup = self[lookupKey];
  if (lookup instanceof Map) return lookup;
  throw new TypeError(`Enum lookup ${lookupKey} is not initialized (buffer has no enumValues).`);
}

function enumValuesListOf(buffer: DynamicColumnBuffer, enumValuesKey: string): string {
  const values = buffer[enumValuesKey];
  return Array.isArray(values) ? values.join(', ') : '';
}

/** Shallow-freeze binary payload objects (mirrors the compiled binary setter). */
function freezeBinaryValue(value: unknown): void {
  if (typeof value === 'object' && value !== null && !(value instanceof Uint8Array)) Object.freeze(value);
}

type WriterSetter = (this: WriterInstance, value: unknown) => WriterInstance;

/** Build the fluent setter for one schema field (mirrors getSetterBody). */
function makeWriterSetter(column: ColumnPlanEntry): WriterSetter {
  const { name, storage, mode } = column;
  const schemaType = storage.schemaType;
  const lookupKey = `_${name}_enumLookup`;
  const enumValuesKey = `${name}_enumValues`;
  // Preallocated setters write through the raw backing properties; eager and
  // lazy setters go through the buffer's public getters (which allocate lazy
  // columns on first access).
  const nullsKey = mode === 'preallocated' ? `_${name}_nulls` : `${name}_nulls`;
  const valuesKey = mode === 'preallocated' ? `_${name}_values` : `${name}_values`;
  const hasNulls = mode !== 'eager';

  if (schemaType === 'boolean') {
    if (hasNulls) {
      return function (value) {
        const idx = this._writeIndex;
        const byteIdx = idx >>> 3;
        const bitMask = 1 << (idx & 7);
        bitmapOf(this._buffer[nullsKey])[byteIdx] |= bitMask;
        const values = bitmapOf(this._buffer[valuesKey]);
        if (value) values[byteIdx] |= bitMask;
        else values[byteIdx] &= ~bitMask;
        return this;
      };
    }
    return function (value) {
      const idx = this._writeIndex;
      const byteIdx = idx >>> 3;
      const bitMask = 1 << (idx & 7);
      const values = bitmapOf(this._buffer[valuesKey]);
      if (value) values[byteIdx] |= bitMask;
      else values[byteIdx] &= ~bitMask;
      return this;
    };
  }

  if (schemaType === 'enum') {
    if (hasNulls) {
      return function (value) {
        const idx = this._writeIndex;
        const enumIndex = enumLookupOf(this, lookupKey).get(value);
        if (enumIndex === undefined) {
          throw new Error(
            `Invalid enum value "${value}" for field "${name}". Valid values: ${enumValuesListOf(this._buffer, enumValuesKey)}`,
          );
        }
        bitmapOf(this._buffer[nullsKey])[idx >>> 3] |= 1 << (idx & 7);
        indexWritableOf(this._buffer[valuesKey])[idx] = enumIndex;
        return this;
      };
    }
    return function (value) {
      const idx = this._writeIndex;
      const enumIndex = enumLookupOf(this, lookupKey).get(value);
      if (enumIndex === undefined) {
        throw new Error(
          `Invalid enum value "${value}" for field "${name}". Valid values: ${enumValuesListOf(this._buffer, enumValuesKey)}`,
        );
      }
      indexWritableOf(this._buffer[valuesKey])[idx] = enumIndex;
      return this;
    };
  }

  if (schemaType === 'binary') {
    if (hasNulls) {
      return function (value) {
        const idx = this._writeIndex;
        freezeBinaryValue(value);
        bitmapOf(this._buffer[nullsKey])[idx >>> 3] |= 1 << (idx & 7);
        indexWritableOf(this._buffer[valuesKey])[idx] = value;
        return this;
      };
    }
    return function (value) {
      const idx = this._writeIndex;
      freezeBinaryValue(value);
      indexWritableOf(this._buffer[valuesKey])[idx] = value;
      return this;
    };
  }

  if (hasNulls) {
    return function (value) {
      const idx = this._writeIndex;
      bitmapOf(this._buffer[nullsKey])[idx >>> 3] |= 1 << (idx & 7);
      indexWritableOf(this._buffer[valuesKey])[idx] = value;
      return this;
    };
  }
  return function (value) {
    const idx = this._writeIndex;
    indexWritableOf(this._buffer[valuesKey])[idx] = value;
    return this;
  };
}

/**
 * Materialize a ColumnWriter class from an accessor plan without string
 * codegen. Same observable behavior as the compiled materializer with the
 * default class name and no code-string extension. Returned as `unknown` to
 * flow through the same constructor type guard as the compiled factory.
 */
export function materializeColumnWriterClass(plan: ColumnAccessorPlan): unknown {
  const enumInits: WriterInit[] = [];

  class GeneratedColumnWriter {
    [key: string]: unknown;

    _buffer: DynamicColumnBuffer;
    _writeIndex: number;

    constructor(buffer: AnyColumnBuffer) {
      this._buffer = toDynamicColumnBuffer(buffer);
      this._writeIndex = -1;
      for (let i = 0; i < enumInits.length; i++) enumInits[i](this);
    }

    nextRow(): this {
      if (this._writeIndex >= this._buffer._capacity - 1) {
        this._buffer = toDynamicColumnBuffer(this._getNextBuffer());
        this._writeIndex = -1;
      }
      this._writeIndex++;
      return this;
    }

    _getNextBuffer(): AnyColumnBuffer {
      const overflow = this._buffer._overflow;
      if (!overflow) {
        throw new Error('Buffer overflow: no next buffer available. Override _getNextBuffer() to handle this.');
      }
      return overflow;
    }
  }

  const proto = GeneratedColumnWriter.prototype;

  for (const column of plan.columns) {
    if (column.storage.schemaType === 'enum') {
      // O(1) enum lookup via pre-built Map, guarded with an existence check:
      // some buffer implementations (e.g. WasmSpanBuffer) may not have
      // enumValues, and the setter may be overridden.
      const lookupKey = `_${column.name}_enumLookup`;
      const enumValuesKey = `${column.name}_enumValues`;
      enumInits.push((self) => {
        const values = self._buffer[enumValuesKey];
        self[lookupKey] = Array.isArray(values)
          ? new Map(values.map((v: unknown, i: number): [unknown, number] => [v, i]))
          : undefined;
      });
    }

    Object.defineProperty(proto, column.name, {
      ...METHOD_DESCRIPTOR,
      value: makeWriterSetter(column),
    });
  }

  return GeneratedColumnWriter;
}
