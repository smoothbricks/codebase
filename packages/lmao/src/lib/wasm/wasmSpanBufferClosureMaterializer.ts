/**
 * Closure-composed (no-eval) materializer for WASM SpanBuffer classes.
 *
 * workerd refuses code generation from strings inside a request, where
 * WasmSpanBuffer classes are first built, so the compiled renderer in
 * wasmSpanBuffer.ts cannot run there (selection: arrow-builder's
 * codegenCapability.ts). This module builds the SAME schema-specific class
 * shell from the same column metadata without source text:
 * - the constructor assigns instance properties in the compiled order (V8 hidden classes);
 * - `_refreshViews`, `_getWasmColumnValue`, `_getWasmColumnNulls` and the per-column
 *   setter/`_values`/`_nulls` members are installed on the prototype as closures over
 *   (column index, layout index, storage mode, property keys), with the compiled
 *   class-member descriptors: methods `{ writable, configurable }`, accessors
 *   `{ configurable }`, none enumerable.
 *
 * Every closure mirrors the compiled member it replaces, including guard order and
 * thrown error messages. getWasmSpanBufferClass installs the schema-independent
 * prototype members and statics on either shell.
 *
 * Spec: specs/lmao/01b6_buffer_codegen_extension.md#smoo/lmao!n/buffer-codegen.materializer-selection
 */

import type { MessageLayoutFamily, MessagePhysicalLayout } from '../runtimeHint.js';
import {
  WASM_NO_LAYOUT_OFFSET,
  WASM_SPAN_IDENTITY_CHILD,
  WASM_SPAN_IDENTITY_ROOT,
  type WasmAllocator,
} from './wasmAllocator.js';
import type { WasmNumericFamily, WasmPhysicalLayoutDescriptor } from './wasmPhysicalLayout.js';
import type {
  WasmBufferDescriptor,
  WasmColumnMeta,
  WasmSpanBufferInstance,
  WasmSpanBufferOptions,
} from './wasmSpanBuffer.js';

const METHOD_DESCRIPTOR = { writable: true, configurable: true, enumerable: false } as const;
const ACCESSOR_DESCRIPTOR = { configurable: true, enumerable: false } as const;

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

/**
 * The members per-column closures touch. Column storage lives under schema-dynamic
 * keys (`_<name>_values` / `_<name>_nulls`), hence the index signature.
 */
interface WasmClosureInstance {
  [key: string]: unknown;
  _allocator: WasmAllocator;
  _capacity: number;
  _layout: WasmPhysicalLayoutDescriptor;
  _familyPtrs: Readonly<Record<WasmNumericFamily, number>>;
  _columnPtrs: Int32Array;
  _descriptor: WasmBufferDescriptor;
  _systemPtr: number;
  _identityPtr: number;
  _identityOwner: boolean;
  _identitySource: WasmSpanBufferInstance | undefined;
  _viewVersion: number;
  _ensureWasmViews: () => void;
}

type ValuesViewConstructor = Float64ArrayConstructor | BigUint64ArrayConstructor | Uint8ArrayConstructor;

/** Mirrors generateNumericViewBinding's array type choice. */
function valuesViewConstructor(column: WasmColumnMeta): ValuesViewConstructor {
  if (column.schemaType === 'number') return Float64Array;
  if (column.schemaType === 'bigUint64') return BigUint64Array;
  return column.sizeClass === '1b' ? Uint8Array : Float64Array;
}

type NumericViewBinder = (self: WasmClosureInstance, memory: ArrayBuffer) => void;

/** Mirrors generateNumericViewBinding: bind one numeric column's nulls/values views into WASM memory. */
function numericViewBinder(column: WasmColumnMeta, layoutIndex: number): NumericViewBinder {
  const Values = valuesViewConstructor(column);
  const rawNulls = `_${column.name}_nulls`;
  const rawValues = `_${column.name}_values`;
  const columnIndex = column.columnIndex;
  return (self, memory) => {
    const layoutColumn = self._layout.columns[layoutIndex];
    const familyPtr = self._familyPtrs[layoutColumn.family];
    self[rawNulls] = new Uint8Array(memory, familyPtr + layoutColumn.nullOffset, layoutColumn.nullByteLength);
    self[rawValues] = new Values(memory, familyPtr + layoutColumn.valueOffset, self._capacity);
    self._columnPtrs[columnIndex] = familyPtr + layoutColumn.nullOffset;
  };
}

type StringColumnInit = (self: WasmClosureInstance) => void;

/** Mirrors generateColumnInit for one string column. */
function stringColumnInit(column: WasmColumnMeta): StringColumnInit {
  const rawNulls = `_${column.name}_nulls`;
  const rawValues = `_${column.name}_values`;
  if (column.isSchemaEager) {
    return (self) => {
      self[rawValues] = new Array(self._capacity);
    };
  }
  if (column.isPreallocated) {
    return (self) => {
      self[rawValues] = new Array(self._capacity);
      self[rawNulls] = new Uint8Array((self._capacity + 7) >>> 3);
    };
  }
  return (self) => {
    self[rawValues] = undefined;
    self[rawNulls] = undefined;
  };
}

function assertLiveForWrite(self: WasmClosureInstance): void {
  if (self._descriptor.state !== 'live') throw new Error('Cannot write a released WASM buffer');
}

function assertLiveForRead(self: WasmClosureInstance): void {
  if (self._descriptor.state !== 'live') throw new Error('Cannot read a released WASM buffer');
}

/** Mirrors generateStringSetter / generateStringValuesGetter / generateStringNullsGetter. */
function installStringColumn(proto: object, column: WasmColumnMeta): void {
  const { name, isSchemaEager, isPreallocated } = column;
  const rawNulls = `_${name}_nulls`;
  const rawValues = `_${name}_values`;
  const isLazy = !isSchemaEager && !isPreallocated;
  // generateEnsureStringStorage: lazy columns allocate both arrays on first touch.
  const ensureStorage = (self: WasmClosureInstance): void => {
    if (isLazy && self[rawValues] === undefined) {
      self[rawValues] = new Array(self._capacity);
      self[rawNulls] = new Uint8Array((self._capacity + 7) >>> 3);
    }
  };

  Object.defineProperty(proto, name, {
    ...METHOD_DESCRIPTOR,
    value: isSchemaEager
      ? function (this: WasmClosureInstance, idx: number, value: unknown) {
          assertLiveForWrite(this);
          indexWritableOf(this[rawValues])[idx] = value;
          return this;
        }
      : function (this: WasmClosureInstance, idx: number, value: unknown) {
          assertLiveForWrite(this);
          ensureStorage(this);
          const nulls = bitmapOf(this[rawNulls]);
          if (value == null) {
            nulls[idx >>> 3] &= ~(1 << (idx & 7));
          } else {
            indexWritableOf(this[rawValues])[idx] = value;
            nulls[idx >>> 3] |= 1 << (idx & 7);
          }
          return this;
        },
  });
  Object.defineProperty(proto, `${name}_values`, {
    ...ACCESSOR_DESCRIPTOR,
    get(this: WasmClosureInstance) {
      assertLiveForRead(this);
      ensureStorage(this);
      return this[rawValues];
    },
  });
  Object.defineProperty(proto, `${name}_nulls`, {
    ...ACCESSOR_DESCRIPTOR,
    get: isSchemaEager
      ? () => undefined
      : function (this: WasmClosureInstance) {
          assertLiveForRead(this);
          ensureStorage(this);
          return this[rawNulls];
        },
  });
}

/** Mirrors generateNumericSetter / generateNumericValuesGetter / generateNumericNullsGetter. */
function installNumericColumn(proto: object, column: WasmColumnMeta, bindView: NumericViewBinder): void {
  const { name, columnIndex, isSchemaEager, isPreallocated } = column;
  const rawNulls = `_${name}_nulls`;
  const rawValues = `_${name}_values`;
  const isLazy = !isSchemaEager && !isPreallocated;
  // generateEnsureNumericView: lazy columns bind their views on first touch.
  const ensureView = (self: WasmClosureInstance): void => {
    if (isLazy && self._columnPtrs[columnIndex] === -1) bindView(self, self._allocator.memory.buffer);
  };

  Object.defineProperty(proto, name, {
    ...METHOD_DESCRIPTOR,
    value: isSchemaEager
      ? function (this: WasmClosureInstance, idx: number, value: unknown) {
          this._ensureWasmViews();
          ensureView(this);
          indexWritableOf(this[rawValues])[idx] = value;
          return this;
        }
      : function (this: WasmClosureInstance, idx: number, value: unknown) {
          this._ensureWasmViews();
          ensureView(this);
          const nulls = bitmapOf(this[rawNulls]);
          if (value == null) {
            nulls[idx >>> 3] &= ~(1 << (idx & 7));
          } else {
            nulls[idx >>> 3] |= 1 << (idx & 7);
            indexWritableOf(this[rawValues])[idx] = value;
          }
          return this;
        },
  });
  Object.defineProperty(proto, `${name}_values`, {
    ...ACCESSOR_DESCRIPTOR,
    get(this: WasmClosureInstance) {
      this._ensureWasmViews();
      ensureView(this);
      return this[rawValues];
    },
  });
  Object.defineProperty(proto, `${name}_nulls`, {
    ...ACCESSOR_DESCRIPTOR,
    get: isSchemaEager
      ? () => undefined
      : function (this: WasmClosureInstance) {
          this._ensureWasmViews();
          ensureView(this);
          return this[rawNulls];
        },
  });
}

/**
 * Materialize the schema-specific WasmSpanBuffer class shell without string codegen.
 * Returned as `unknown` like the compiled factory's result; getWasmSpanBufferClass
 * narrows it once both materializers have produced a constructor.
 */
export function materializeWasmSpanBufferClass(
  columnMeta: readonly WasmColumnMeta[],
  messageLayoutFamily: MessageLayoutFamily,
  messagePhysicalLayout: MessagePhysicalLayout,
): unknown {
  const columnCount = columnMeta.length;
  const stringInits: StringColumnInit[] = [];
  const viewBinders: { readonly column: WasmColumnMeta; readonly bind: NumericViewBinder }[] = [];
  const valueKeys = Array.from({ length: columnCount }, (): string | undefined => undefined);
  const nullKeys = Array.from({ length: columnCount }, (): string | undefined => undefined);

  for (const column of columnMeta) {
    if (column.name === 'message') continue;
    valueKeys[column.columnIndex] = `_${column.name}_values`;
    if (!column.isSchemaEager) nullKeys[column.columnIndex] = `_${column.name}_nulls`;
    if (column.sizeClass === 'string') stringInits.push(stringColumnInit(column));
    else viewBinders.push({ column, bind: numericViewBinder(column, viewBinders.length) });
  }

  class WasmSpanBuffer {
    [key: string]: unknown;
    declare _allocator: WasmAllocator;
    declare _capacity: number;
    declare _layout: WasmPhysicalLayoutDescriptor;
    declare _familyPtrs: Readonly<Record<WasmNumericFamily, number>>;
    declare _columnPtrs: Int32Array;
    declare _descriptor: WasmBufferDescriptor;
    declare _systemPtr: number;
    declare _identityPtr: number;
    declare _identityOwner: boolean;
    declare _identitySource: WasmSpanBufferInstance | undefined;
    declare _viewVersion: number;
    declare _ensureWasmViews: () => void;
    declare _refreshViews: (version: number) => void;
    declare free: () => void;

    // Mirrors the compiled constructor statement for statement.
    constructor(opts: WasmSpanBufferOptions) {
      this._allocator = opts.allocator;
      this._capacity = opts.capacity;
      this._logSchema = opts.logSchema;
      this._parent = opts._parent ?? null;
      this._nodeIndex = 4294967295;
      this._topologyGeneration = 0;
      this._overflow = null;
      this._overflowWriteIndex = 0;
      this._layout = opts._layout;
      if (this._layout.messageLayoutFamily !== messageLayoutFamily) {
        throw new TypeError('WASM layout descriptor does not match its generated message family');
      }
      if (this._layout.messagePhysicalLayout !== messagePhysicalLayout) {
        throw new TypeError('WASM layout descriptor does not match its generated physical layout');
      }

      this._traceRoot = opts._traceRoot;
      this._scopeValues = opts._scopeValues;
      this._opMetadata = opts._opMetadata;
      this._callsiteMetadata = opts._callsiteMetadata;
      this._vocabularyGeneration = opts._vocabularyGeneration;
      this._statsSealed = false;
      this._statsReservedRows = 2;
      this._columnPtrs = new Int32Array(columnCount).fill(-1);
      if (messageLayoutFamily === 'static-only') {
        this._spanName = undefined;
        this._terminalMessage = undefined;
      } else if (messageLayoutFamily === 'dynamic-only') {
        this._spanName = undefined;
        this._message = new Array(opts.capacity);
      } else {
        this._message = new Array(opts.capacity);
      }
      for (let i = 0; i < stringInits.length; i++) stringInits[i](this);

      const identityMode = opts._identityMode ?? 'root';
      this._identityOwner = identityMode !== 'overflow';
      this._identitySource = identityMode === 'overflow' ? opts._identitySource : undefined;
      const superblock = this._identityOwner ? this._layout.spanSuperblock : this._layout.overflowSuperblock;
      this._allocationByteLength = superblock.byteLength;
      let allocationPtr: number;
      if (identityMode === 'overflow') {
        const identitySource = this._identitySource;
        if (!identitySource) throw new Error('Overflow buffer requires an identity owner');
        allocationPtr = opts.allocator.createOverflowSpan(superblock.byteLength);
        this._allocationPtr = allocationPtr;
        this._identityPtr = identitySource._identityPtr;
      } else {
        // The ITraceRoot contract does not carry the WASM trace-root pointer; WasmTraceRoot does.
        const traceRootPtr: unknown = Reflect.get(opts._traceRoot, '_traceRootPtr');
        if (typeof traceRootPtr !== 'number' || !Number.isInteger(traceRootPtr) || traceRootPtr <= 0) {
          throw new TypeError('WASM span requires a live WasmTraceRoot');
        }
        const traceIdBytes = identityMode === 'root' ? opts._traceRoot._traceIdBytes : undefined;
        allocationPtr = opts.allocator.createAndStartSpan(
          identityMode === 'child' ? WASM_SPAN_IDENTITY_CHILD : WASM_SPAN_IDENTITY_ROOT,
          traceIdBytes?.length ?? 0,
          superblock.byteLength,
          superblock.systemOffset,
          this._layout.system.entryTypeOffset ?? WASM_NO_LAYOUT_OFFSET,
          this._layout.system.rowHeaderOffset ?? WASM_NO_LAYOUT_OFFSET,
          traceRootPtr,
        );
        this._allocationPtr = allocationPtr;
        this._identityPtr = allocationPtr;
        if (allocationPtr !== 0 && traceIdBytes !== undefined) {
          opts.allocator.u8.set(traceIdBytes, this._identityPtr + 9);
        }
      }
      if (allocationPtr === 0) throw new Error('WASM span superblock allocation failed');

      this._systemPtr = allocationPtr + superblock.systemOffset;
      const familyPtrs: Record<WasmNumericFamily, number> = { u8: 0, u32: 0, f64: 0 };
      for (const family of ['u8', 'u32', 'f64'] as const) {
        const familyOffset = superblock.familyOffsets[family];
        if (familyOffset !== 0) familyPtrs[family] = allocationPtr + familyOffset;
      }
      this._familyPtrs = Object.freeze(familyPtrs);
      this._threadId = this._identitySource?.thread_id ?? opts.allocator.readThreadId();
      this._spanId = this._identitySource?.span_id ?? opts.allocator.u32[(this._identityPtr + 4) >>> 2];
      this._viewVersion = 0;
      this._spanStartedAtAllocation = this._identityOwner;

      this._descriptor = {
        generation: opts._generation ?? 0,
        kind: identityMode,
        state: 'live',
        layout: this._layout,
        allocationPtr,
        allocationByteLength: superblock.byteLength,
        systemPtr: this._systemPtr,
        familyPtrs: this._familyPtrs,
        identityPtr: this._identityPtr,
        ownsIdentity: this._identityOwner,
        memoryVersion: 0,
        parent: opts._parent?._descriptor,
      };

      try {
        this._refreshViews(this._allocator.refreshViews());
      } catch (error) {
        this.free();
        throw error;
      }
    }
  }

  const proto = WasmSpanBuffer.prototype;

  // Mirrors generateViewRefresh.
  Object.defineProperty(proto, '_refreshViews', {
    ...METHOD_DESCRIPTOR,
    value: function (this: WasmClosureInstance, version: number) {
      const memory = this._allocator.memory.buffer;
      this._timestampView = new BigInt64Array(memory, this._systemPtr, this._capacity);
      const system = this._layout.system;
      if (messagePhysicalLayout === 'packed') {
        this._rowHeaders = new Uint32Array(memory, this._systemPtr + (system.rowHeaderOffset ?? 0), this._capacity);
      } else {
        this._entryTypeView = new Uint8Array(memory, this._systemPtr + (system.entryTypeOffset ?? 0), this._capacity);
        if (messageLayoutFamily !== 'dynamic-only') {
          if (messagePhysicalLayout === 'current') {
            this._messageIds = new Uint16Array(memory, this._systemPtr + (system.messageIdOffset ?? 0), this._capacity);
          } else {
            this._logHeaders = new Uint32Array(
              memory,
              this._systemPtr + (system.messageDenseIndexOffset ?? 0),
              this._capacity,
            );
          }
        }
      }
      if (this._identityOwner) {
        this._identityView = new Uint8Array(memory, this._identityPtr, 128);
        this._identityData = new DataView(memory, this._identityPtr, 128);
      } else {
        const identitySource = this._identitySource;
        if (!identitySource) throw new Error('Overflow buffer requires an identity owner');
        identitySource._ensureWasmViews();
        this._identityView = identitySource._identityView;
        this._identityData = identitySource._identityData;
      }
      for (let i = 0; i < viewBinders.length; i++) {
        const { column, bind } = viewBinders[i];
        if (column.isSchemaEager || column.isPreallocated || this._columnPtrs[column.columnIndex] !== -1) {
          bind(this, memory);
        }
      }
      this._viewVersion = version;
      this._descriptor.memoryVersion = version;
    },
  });

  // Mirrors generateColumnLookup.
  Object.defineProperty(proto, '_getWasmColumnValue', {
    ...METHOD_DESCRIPTOR,
    value: function (this: WasmClosureInstance, columnIndex: number) {
      const key = valueKeys[columnIndex];
      return key === undefined ? undefined : this[key];
    },
  });
  Object.defineProperty(proto, '_getWasmColumnNulls', {
    ...METHOD_DESCRIPTOR,
    value: function (this: WasmClosureInstance, columnIndex: number) {
      const key = nullKeys[columnIndex];
      return key === undefined ? undefined : this[key];
    },
  });

  // Mirrors generateColumnMethods.
  let viewIndex = 0;
  for (const column of columnMeta) {
    if (column.name === 'message') continue;
    if (column.sizeClass === 'string') installStringColumn(proto, column);
    else installNumericColumn(proto, column, viewBinders[viewIndex++].bind);
  }

  return WasmSpanBuffer;
}
