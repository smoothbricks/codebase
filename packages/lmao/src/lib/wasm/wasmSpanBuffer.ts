/**
 * WASM-backed SpanBuffer class generation.
 *
 * Implements spec 01q: WASM Memory Architecture for SpanBuffer Storage
 *
 * Key differences from JS SpanBuffer:
 * - System columns (timestamp, entry_type) live in WASM memory at _systemPtr
 * - Numeric columns allocated lazily from WASM freelists
 * - String columns (category, text) remain as JS string[] arrays
 * - All TypedArray views created on-demand from WASM memory.buffer
 */

import type { ColumnValueType } from '@smoothbricks/arrow-builder';
import { getSchemaType } from '@smoothbricks/arrow-builder';
import { checkCapacityTuning } from '../capacityTuning.js';
import type { OpMetadata } from '../op.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SpanBufferStats } from '../spanBufferStats.js';
import type { ITraceRoot } from '../traceRoot.js';
import type { SpanBuffer } from '../types.js';
import { getVocabularyGeneration, type VocabularyGeneration } from '../vocabularyRegistry.js';
import type { WasmAllocator } from './wasmAllocator.js';
import {
  getWasmPhysicalLayout,
  type WasmNumericFamily,
  type WasmPhysicalLayoutDescriptor,
} from './wasmPhysicalLayout.js';

// Spec link (88): realizes specs/lmao/01q_wasm_memory_architecture.md#smoo/lmao!n/wasm-mem (SpanBuffer + SpanLogger codegen).
//#region smoo/lmao!n/wasm-mem.spanbuffer

// =============================================================================
// Types
// =============================================================================

/**
 * Options for WASM SpanBuffer instances.
 * Used by createWasmSpanBuffer (root), createWasmChildSpanBuffer (child), and createWasmOverflowBuffer.
 * Internal fields (_traceRoot, _scopeValues, _opMetadata, _callsiteMetadata) are passed separately
 * by the create* functions.
 */
export type WasmBufferState = 'live' | 'freed';
export type WasmIdentityMode = 'root' | 'child' | 'overflow';

export interface WasmBufferDescriptor {
  readonly generation: number;
  readonly kind: WasmIdentityMode;
  state: WasmBufferState;
  readonly layout: WasmPhysicalLayoutDescriptor;
  readonly systemPtr: number;
  readonly familyPtrs: Readonly<Record<WasmNumericFamily, number>>;
  readonly identityPtr: number;
  readonly ownsIdentity: boolean;
  memoryVersion: number;
  readonly parent?: WasmBufferDescriptor;
  overflow?: WasmBufferDescriptor;
}

let nextWasmBufferGeneration = 0;

export interface WasmSpanBufferOptions<T extends LogSchema = LogSchema> {
  allocator: WasmAllocator;
  capacity: number;
  trace_id: string;
  thread_id: bigint;
  span_id: number;
  parent_thread_id?: bigint;
  parent_span_id?: number;
  logSchema: T;
  _traceRoot: ITraceRoot<T>;
  _scopeValues: Readonly<Record<string, unknown>>;
  _opMetadata: OpMetadata;
  _callsiteMetadata: OpMetadata;
  _vocabularyGeneration: VocabularyGeneration;
  _layout: WasmPhysicalLayoutDescriptor;
  _identityMode?: WasmIdentityMode;
  _identityPtr?: number;
  _parent?: WasmSpanBufferInstance<T>;
  _generation?: number;
}

/**
 * Extends the schema-specific SpanBuffer shape with WASM pointer ownership.
 */
export type WasmSpanBufferInstance<T extends LogSchema = LogSchema> = SpanBuffer<T> & {
  // ===========================================================================
  // WASM pointers (internal implementation details)
  // ===========================================================================
  /** Byte offset into WASM memory for system columns (timestamp + entry_type) */
  readonly _systemPtr: number;
  /** Byte offset into WASM memory for identity block (writeIndex, span_id, trace_id) */
  readonly _identityPtr: number;
  /** Array of byte offsets for each column (-1 = not allocated) */
  readonly _columnPtrs: Int32Array;
  /** Reference to the WASM allocator */
  readonly _allocator: WasmAllocator;
  readonly _descriptor: WasmBufferDescriptor;
  readonly _layout: WasmPhysicalLayoutDescriptor;
  readonly _familyPtrs: Readonly<Record<WasmNumericFamily, number>>;
  _viewVersion: number;
  _timestampView: BigInt64Array;
  _entryTypeView: Uint8Array;
  _identityView: Uint8Array;
  _identityData: DataView;
  _refreshViews(version: number): void;
  _ensureWasmViews(): void;
  _getWasmColumnValue(columnIndex: number): ColumnValueType | undefined;
  _getWasmColumnNulls(columnIndex: number): Uint8Array | undefined;
  readonly _identityOwner: boolean;
  _overflowWriteIndex: number;

  // Override tree structure with WASM-typed versions
  _parent?: WasmSpanBufferInstance<T>;
  _children: WasmSpanBufferInstance<T>[];
  _overflow?: WasmSpanBufferInstance<T>;

  // Override message column - WASM stores as JS array (not WASM memory)
  readonly _message: string[];

  // ===========================================================================
  // WASM-specific methods
  // ===========================================================================
  /** Free all WASM memory for this buffer */
  free(): void;

  /** Check if column is allocated */
  isColumnAllocated(columnIndex: number): boolean;

  /** Get column values array if allocated */
  getColumnIfAllocated(columnName: string): ColumnValueType | undefined;

  /** Get column nulls array if allocated */
  getNullsIfAllocated(columnName: string): Uint8Array | undefined;
};

/**
 * Constructor type for generated WASM SpanBuffer classes.
 */
export interface WasmSpanBufferConstructor<T extends LogSchema = LogSchema> {
  new (opts: WasmSpanBufferOptions<T>): WasmSpanBufferInstance<T>;
  readonly schema: T;
  stats: SpanBufferStats;
}

// =============================================================================
// Column Metadata
// =============================================================================

/**
 * Size class for WASM allocation (determines which freelist to use).
 */
type SizeClass = '1b' | '4b' | '8b' | 'string';

/**
 * Metadata for a single column - used during code generation.
 */
interface ColumnMeta {
  /** Column name from schema */
  name: string;
  /** WASM allocation size class */
  sizeClass: SizeClass;
  /** Schema type (enum, category, text, number, etc.) */
  schemaType: string;
  /** Index in the _columnPtrs array */
  columnIndex: number;
  /** Whether this column is eager (always allocated) */
  isEager: boolean;
  /** Enum values if schemaType === 'enum' */
  enumValues?: readonly string[];
}

function getFieldEager(value: unknown): boolean {
  return typeof value === 'object' && value !== null && Reflect.get(value, '__eager') === true;
}

function getFieldEnumValues(value: unknown): readonly string[] | undefined {
  if (typeof value !== 'object' || value === null) {
    return undefined;
  }
  const enumValues = Reflect.get(value, '__enum_values');
  return Array.isArray(enumValues) ? enumValues : undefined;
}


function isWasmSpanBufferConstructor<T extends LogSchema = LogSchema>(
  value: unknown,
  schema?: T,
): value is WasmSpanBufferConstructor<T> {
  return (
    typeof value === 'function' &&
    typeof Reflect.get(value, 'schema') === 'object' &&
    Reflect.get(value, 'schema') !== null &&
    (schema === undefined || Reflect.get(value, 'schema') === schema) &&
    typeof Reflect.get(value, 'stats') === 'object' &&
    Reflect.get(value, 'stats') !== null
  );
}

export function isWasmSpanBufferInstance<T extends LogSchema = LogSchema>(
  value: unknown,
): value is WasmSpanBufferInstance<T> {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, '_systemPtr') === 'number';
}

function getWasmSpanBufferConstructor(instance: { constructor: unknown }): WasmSpanBufferConstructor {
  if (!isWasmSpanBufferConstructor(instance.constructor)) {
    throw new Error('Expected generated WasmSpanBuffer constructor');
  }
  return instance.constructor;
}

function isWasmSpanBufferFactory(value: unknown): value is () => WasmSpanBufferConstructor {
  return typeof value === 'function';
}

/**
 * Build column metadata array from schema.
 * Maps each schema field to its storage characteristics.
 */
function buildColumnMeta(schema: LogSchema): ColumnMeta[] {
  const result: ColumnMeta[] = [];
  const fields = schema.fields;
  let columnIndex = 0;

  for (const name of schema._columnNames) {
    const field = fields[name];
    const schemaType = getSchemaType(field);
    const isEager = getFieldEager(field);
    const enumValues = getFieldEnumValues(field);

    let sizeClass: SizeClass;
    switch (schemaType) {
      case 'enum':
      case 'boolean':
        sizeClass = '1b';
        break;
      case 'number':
      case 'bigUint64':
        sizeClass = '8b';
        break;
      case 'category':
      case 'text':
        sizeClass = 'string';
        break;
      default:
        sizeClass = '8b'; // Default to largest for unknown types
    }

    result.push({
      name,
      sizeClass,
      schemaType: schemaType ?? 'unknown',
      columnIndex,
      isEager,
      enumValues,
    });
    columnIndex++;
  }

  return result;
}

// =============================================================================
// Common WasmSpanBuffer Methods (shared via prototype)
// =============================================================================

function wasmEnsureViews(this: WasmSpanBufferInstance): void {
  assertWasmBufferLive(this);
  const memoryVersion = this._allocator.refreshViews();
  if (memoryVersion !== this._viewVersion) this._refreshViews(memoryVersion);
}

function wasmGetTimestamp(this: WasmSpanBufferInstance): BigInt64Array {
  wasmEnsureViews.call(this);
  return this._timestampView;
}

function wasmGetEntryType(this: WasmSpanBufferInstance): Uint8Array {
  wasmEnsureViews.call(this);
  return this._entryTypeView;
}

function assertWasmBufferLive(buffer: WasmSpanBufferInstance): void {
  if (buffer._descriptor.state !== 'live') {
    const generation = buffer._descriptor.generation;
    throw new Error(`Cannot access released WASM buffer generation ${generation}; generation ${generation} has been released`);
  }
}

function wasmGetWriteIndex(this: WasmSpanBufferInstance): number {
  wasmEnsureViews.call(this);
  return this._identityOwner ? this._identityData.getUint32(0, true) : this._overflowWriteIndex;
}

function wasmSetWriteIndex(this: WasmSpanBufferInstance, value: number): void {
  wasmEnsureViews.call(this);
  if (this._identityOwner) {
    this._identityData.setUint32(0, value, true);
  } else {
    this._overflowWriteIndex = value;
  }
}

function wasmGetTraceId(this: WasmSpanBufferInstance): string {
  wasmEnsureViews.call(this);
  const len = this._identityView[8];
  if (len === 0) {
    let p = this._parent;
    while (p?._parent) p = p._parent;
    return p ? p.trace_id : '';
  }
  return new TextDecoder().decode(this._identityView.subarray(9, 9 + len));
}

function wasmGetSpanStartTime(this: WasmSpanBufferInstance): bigint {
  return this.timestamp[0];
}

function wasmGetLastLoggedTime(this: WasmSpanBufferInstance): bigint | null {
  const chain: WasmSpanBufferInstance[] = [];
  let current: WasmSpanBufferInstance | undefined = this;
  while (current) {
    chain.push(current);
    current = current._overflow;
  }

  for (let i = chain.length - 1; i >= 0; i--) {
    const buffer = chain[i];
    for (let row = buffer._writeIndex - 1; row >= 0; row--) {
      const ts = buffer.timestamp[row];
      if (ts !== 0n) {
        return ts;
      }
    }
  }

  return null;
}

/**
 * Get thread_id from WASM identity block.
 */
function wasmGetThreadId(this: WasmSpanBufferInstance): bigint {
  assertWasmBufferLive(this);
  const high = this._allocator.getThreadIdHigh();
  const low = this._allocator.getThreadIdLow();
  return (BigInt(high) << 32n) | BigInt(low);
}

function wasmGetSpanId(this: WasmSpanBufferInstance): number {
  wasmEnsureViews.call(this);
  return this._identityData.getUint32(4, true);
}

/**
 * Get parent_thread_id.
 */
function wasmGetParentThreadId(this: WasmSpanBufferInstance): bigint {
  return this._parent ? this._parent.thread_id : 0n;
}

/**
 * Get parent_span_id.
 */
function wasmGetParentSpanId(this: WasmSpanBufferInstance): number {
  return this._parent ? this._parent.span_id : 0;
}

/**
 * Check if buffer has parent.
 */
function wasmGetHasParent(this: WasmSpanBufferInstance): boolean {
  return this._parent !== null;
}

/**
 * Message column setter.
 */
function wasmMessage(this: WasmSpanBufferInstance, idx: number, value: string): WasmSpanBufferInstance {
  assertWasmBufferLive(this);
  this._message[idx] = value;
  return this;
}

/**
 * Get message column values.
 */
function wasmGetMessageValues(this: WasmSpanBufferInstance): string[] {
  assertWasmBufferLive(this);
  return this._message;
}

/**
 * Get message column nulls (always undefined for eager column).
 */
function wasmGetMessageNulls(this: WasmSpanBufferInstance): undefined {
  assertWasmBufferLive(this);
  return undefined;
}

/** Free each owned WASM block exactly once. */
function wasmFree(this: WasmSpanBufferInstance): void {
  if (this._descriptor.state === 'freed') return;
  if (this._viewVersion !== 0) this._sealStats();

  for (const family of ['u8', 'u32', 'f64'] as const) {
    const slab = this._layout.slabs[family];
    if (slab !== null) this._allocator.freeExact(this._familyPtrs[family], slab.byteLength, slab.alignment);
  }
  this._allocator.freeExact(this._systemPtr, this._layout.system.byteLength, this._layout.system.alignment);
  if (this._identityOwner) {
    this._allocator.freeIdentity(this._identityPtr);
  }
  this._descriptor.state = 'freed';
}

/**
 * Check if column is allocated.
 */
function wasmIsColumnAllocated(this: WasmSpanBufferInstance, columnIndex: number): boolean {
  assertWasmBufferLive(this);
  wasmEnsureViews.call(this);
  return this._columnPtrs[columnIndex] >= 0 || this._getWasmColumnValue(columnIndex) !== undefined;
}

/** Get a canonical column value view or JS sidecar without accessor reflection. */
function wasmGetColumnIfAllocated(this: WasmSpanBufferInstance, columnName: string): ColumnValueType | undefined {
  assertWasmBufferLive(this);
  const idx = this._logSchema._columnNames.indexOf(columnName);
  if (idx === -1) return undefined;
  wasmEnsureViews.call(this);
  return this._getWasmColumnValue(idx);
}

/** Get a canonical column null-bitmap view without accessor reflection. */
function wasmGetNullsIfAllocated(this: WasmSpanBufferInstance, columnName: string): Uint8Array | undefined {
  assertWasmBufferLive(this);
  const idx = this._logSchema._columnNames.indexOf(columnName);
  if (idx === -1) return undefined;
  wasmEnsureViews.call(this);
  return this._getWasmColumnNulls(idx);
}

/** Get the current WASM memory backing store. */
function wasmGetSystem(this: WasmSpanBufferInstance): ArrayBuffer {
  wasmEnsureViews.call(this);
  return this._allocator.memory.buffer;
}

/** Get the canonical identity view for the current memory version. */
function wasmGetIdentity(this: WasmSpanBufferInstance): Uint8Array {
  wasmEnsureViews.call(this);
  return this._identityView;
}

function wasmSealStats(this: WasmSpanBufferInstance): void {
  if (this._statsSealed) return;
  const completedRows = this._writeIndex - this._statsReservedRows;
  if (completedRows > 0) getWasmSpanBufferConstructor(this).stats.totalWrites += completedRows;
  this._statsSealed = true;
}

function wasmSealStatsChain(this: WasmSpanBufferInstance): void {
  let current: WasmSpanBufferInstance | undefined = this;
  while (current) {
    current._sealStats();
    current = current._overflow;
  }
}

/**
 * Get or create overflow buffer.
 */
function wasmGetOrCreateOverflow(this: WasmSpanBufferInstance): WasmSpanBufferInstance {
  assertWasmBufferLive(this);
  if (this._overflow) return this._overflow;
  this._sealStats();
  const tracer = this._traceRoot.tracer;
  tracer.onStatsWillResetFor(this);
  checkCapacityTuning(getWasmSpanBufferConstructor(this).stats);
  // WasmSpanBufferInstance extends AnySpanBuffer and has all SpanBuffer properties at runtime
  const overflow = tracer.bufferStrategy.createOverflowBuffer(this);
  if (!isWasmSpanBufferInstance(overflow)) {
    throw new Error('Expected overflow buffer to be WASM-backed');
  }
  return overflow;
}

/**
 * Get _stats property.
 */
function wasmGetStats(this: WasmSpanBufferInstance): SpanBufferStats {
  return getWasmSpanBufferConstructor(this).stats;
}

/**
 * Get _columns property.
 */
function wasmGetColumns(this: WasmSpanBufferInstance): LogSchema['_columns'] {
  return getWasmSpanBufferConstructor(this).schema._columns;
}

/**
 * Custom inspect for debugging.
 */
function wasmInspect(this: WasmSpanBufferInstance): string {
  return `WasmSpanBuffer { _writeIndex: ${this._writeIndex}, _capacity: ${this._capacity}, trace_id: ${this.trace_id ?? 'N/A'}, _systemPtr: ${this._systemPtr} }`;
}

// =============================================================================
// Code Generation Helpers
// =============================================================================

/**
 * Generate eager column initialization code for constructor.
 * Eager columns are pre-allocated, lazy columns start as undefined/-1.
 */
function generateColumnInit(columnMeta: ColumnMeta[]): string {
  const lines: string[] = [];
  for (const col of columnMeta) {
    if (col.name === 'message' || col.sizeClass !== 'string') continue;
    if (col.isEager) {
      lines.push(`this._${col.name}_values = new Array(this._capacity);`);
    } else {
      lines.push(`this._${col.name}_values = undefined;`);
      lines.push(`this._${col.name}_nulls = undefined;`);
    }
  }
  return lines.join('\n    ');
}

function generateNumericSetter(col: ColumnMeta): string {
  return `${col.name}(idx, value) {
    this._ensureWasmViews();
    this._${col.name}_nulls[idx >>> 3] |= 1 << (idx & 7);
    this._${col.name}_values[idx] = value;
    return this;
  }`;
}

function generateNumericValuesGetter(col: ColumnMeta): string {
  return `get ${col.name}_values() {
    this._ensureWasmViews();
    return this._${col.name}_values;
  }`;
}

function generateNumericNullsGetter(col: ColumnMeta): string {
  if (col.isEager) {
    return `get ${col.name}_nulls() {
    return undefined;
  }`;
  }
  return `get ${col.name}_nulls() {
    this._ensureWasmViews();
    return this._${col.name}_nulls;
  }`;
}

function generateViewRefresh(columnMeta: ColumnMeta[]): string {
  const lines = [
    'const memory = this._allocator.memory.buffer;',
    'this._timestampView = new BigInt64Array(memory, this._systemPtr, this._capacity);',
    'this._entryTypeView = new Uint8Array(memory, this._systemPtr + this._layout.system.entryTypeOffset, this._capacity);',
    'this._identityView = new Uint8Array(memory, this._identityPtr, 128);',
    'this._identityData = new DataView(memory, this._identityPtr, 128);',
  ];
  let layoutIndex = 0;
  for (const col of columnMeta) {
    if (col.sizeClass === 'string' || col.name === 'message') continue;
    lines.push(`{
      const column = this._layout.columns[${layoutIndex}];
      const familyPtr = this._familyPtrs[column.family];
      this._${col.name}_nulls = new Uint8Array(memory, familyPtr + column.nullOffset, column.nullByteLength);
      this._${col.name}_values = new ${
        col.schemaType === 'number'
          ? 'Float64Array'
          : col.schemaType === 'bigUint64'
            ? 'BigUint64Array'
            : col.sizeClass === '1b'
              ? 'Uint8Array'
              : col.sizeClass === '4b'
                ? 'Uint32Array'
                : 'Float64Array'
      }(memory, familyPtr + column.valueOffset, this._capacity);
      this._columnPtrs[${col.columnIndex}] = familyPtr + column.nullOffset;
    }`);
    layoutIndex++;
  }
  lines.push('this._viewVersion = version;', 'this._descriptor.memoryVersion = version;');
  return lines.join('\n    ');
}

function generateColumnLookup(columnMeta: ColumnMeta[], nulls: boolean): string {
  const cases: string[] = [];
  for (const col of columnMeta) {
    if (col.name === 'message') continue;
    const property = nulls ? `${col.name}_nulls` : `${col.name}_values`;
    cases.push(`case ${col.columnIndex}: return this.${property};`);
  }
  return `switch (columnIndex) { ${cases.join(' ')} default: return undefined; }`;
}

/**
 * Generate setter method for a string column (stored in JS).
 */
function generateStringSetter(col: ColumnMeta): string {
  if (col.isEager) {
    return `${col.name}(idx, value) {
    if (this._descriptor.state !== 'live') throw new Error('Cannot write a released WASM buffer');
    this._${col.name}_values[idx] = value;
    return this;
  }`;
  }

  // Lazy string column: allocate arrays on first write
  return `${col.name}(idx, value) {
    if (this._descriptor.state !== 'live') throw new Error('Cannot write a released WASM buffer');
    if (this._${col.name}_values === undefined) {
      this._${col.name}_values = new Array(this._capacity);
      const nullBitmapSize = Math.ceil(this._capacity / 8);
      this._${col.name}_nulls = new Uint8Array(nullBitmapSize);
    }
    if (value == null) {
      // Clear validity bit
      this._${col.name}_nulls[idx >>> 3] &= ~(1 << (idx & 7));
    } else {
      this._${col.name}_values[idx] = value;
      // Set validity bit
      this._${col.name}_nulls[idx >>> 3] |= (1 << (idx & 7));
    }
    return this;
  }`;
}

/**
 * Generate getter for string column values.
 */
function generateStringValuesGetter(col: ColumnMeta): string {
  if (col.isEager) {
    return `get ${col.name}_values() {
    return this._${col.name}_values;
  }`;
  }

  return `get ${col.name}_values() {
    return this._${col.name}_values;
  }`;
}

/**
 * Generate getter for string column nulls.
 */
function generateStringNullsGetter(col: ColumnMeta): string {
  if (col.isEager) {
    return `get ${col.name}_nulls() {
    return undefined;
  }`;
  }

  return `get ${col.name}_nulls() {
    return this._${col.name}_nulls;
  }`;
}

/**
 * Generate column methods for all schema columns.
 * Note: 'message' column is handled specially by generateMessageMethods() and excluded here.
 */
function generateColumnMethods(columnMeta: ColumnMeta[]): string {
  const methods: string[] = [];

  for (const col of columnMeta) {
    // Skip 'message' - handled specially by generateMessageMethods()
    if (col.name === 'message') {
      continue;
    }

    if (col.sizeClass === 'string') {
      methods.push(generateStringSetter(col));
      methods.push(generateStringValuesGetter(col));
      methods.push(generateStringNullsGetter(col));
    } else {
      methods.push(generateNumericSetter(col));
      methods.push(generateNumericValuesGetter(col));
      methods.push(generateNumericNullsGetter(col));
    }
  }

  return methods.join('\n\n  ');
}

// =============================================================================
// Class Cache and Generation
// =============================================================================

/**
 * Cache for generated WASM SpanBuffer classes.
 * Key is the schema object reference (WeakMap for GC).
 */
const wasmSpanBufferClassCache = new WeakMap<LogSchema, object>();

/**
 * Generate a WASM SpanBuffer class for the given schema.
 *
 * Generates minimal constructor + column methods, then assigns common methods to prototype.
 *
 * @param schema - LogSchema defining the buffer structure
 * @returns WasmSpanBufferConstructor for creating buffer instances
 */
export function getWasmSpanBufferClass<T extends LogSchema>(schema: T): WasmSpanBufferConstructor<T> {
  const cached = wasmSpanBufferClassCache.get(schema);
  if (cached !== undefined && isWasmSpanBufferConstructor(cached, schema)) return cached;

  // Pre-compute column metadata
  const columnMeta = buildColumnMeta(schema);

  // Generate ONLY schema-specific code (constructor + column methods)
  const classCode = `
class WasmSpanBuffer {
  constructor(opts) {
    this._allocator = opts.allocator;
    this._capacity = opts.capacity;
    this._logSchema = opts.logSchema;
    this._parent = opts._parent ?? null;
    this._children = [];
    this._overflow = null;
    this._overflowWriteIndex = 0;

    const identityMode = opts._identityMode ?? 'root';
    this._identityOwner = identityMode !== 'overflow';
    if (identityMode === 'overflow') {
      if (!opts._identityPtr) throw new Error('Overflow buffer requires a nonzero shared identity pointer');
      this._identityPtr = opts._identityPtr;
    } else if (identityMode === 'child') {
      this._identityPtr = opts.allocator.allocIdentityChild();
    } else {
      const traceIdBytes = new TextEncoder().encode(opts.trace_id);
      const packed = opts.allocator.allocIdentityRootForJsWrite(traceIdBytes.length);
      this._identityPtr = Number(packed >> 32n);
      const traceIdOffset = Number(packed & 0xFFFFFFFFn);
      if (this._identityPtr !== 0) opts.allocator.u8.set(traceIdBytes, traceIdOffset);
    }
    if (this._identityPtr === 0) throw new Error('WASM identity allocation failed');

    this._layout = opts._layout;
    this._columnPtrs = new Int32Array(${columnMeta.length}).fill(-1);
    const familyPtrs = { u8: 0, u32: 0, f64: 0 };
    this._systemPtr = opts.allocator.allocExact(this._layout.system.byteLength, this._layout.system.alignment);
    if (this._systemPtr === 0) {
      if (this._identityOwner) opts.allocator.freeIdentity(this._identityPtr);
      throw new Error('WASM exact system-slab allocation failed');
    }
    for (const family of ['u8', 'u32', 'f64']) {
      const slab = this._layout.slabs[family];
      if (slab === null) continue;
      familyPtrs[family] = opts.allocator.allocExact(slab.byteLength, slab.alignment);
      if (familyPtrs[family] === 0) {
        for (const allocatedFamily of ['u8', 'u32', 'f64']) {
          const allocatedSlab = this._layout.slabs[allocatedFamily];
          if (allocatedSlab !== null && familyPtrs[allocatedFamily] !== 0) {
            opts.allocator.freeExact(familyPtrs[allocatedFamily], allocatedSlab.byteLength, allocatedSlab.alignment);
          }
        }
        opts.allocator.freeExact(this._systemPtr, this._layout.system.byteLength, this._layout.system.alignment);
        if (this._identityOwner) opts.allocator.freeIdentity(this._identityPtr);
        throw new Error('WASM exact column-family slab allocation failed');
      }
    }
    this._familyPtrs = Object.freeze(familyPtrs);
    this._viewVersion = 0;

    this._descriptor = {
      generation: opts._generation ?? 0,
      kind: identityMode,
      state: 'live',
      layout: this._layout,
      systemPtr: this._systemPtr,
      familyPtrs: this._familyPtrs,
      identityPtr: this._identityPtr,
      ownsIdentity: this._identityOwner,
      memoryVersion: 0,
      parent: opts._parent?._descriptor,
    };

    this._traceRoot = opts._traceRoot;
    this._scopeValues = opts._scopeValues;
    this._opMetadata = opts._opMetadata;
    this._callsiteMetadata = opts._callsiteMetadata;
    this._vocabularyGeneration = opts._vocabularyGeneration;
    this._statsSealed = false;
    this._statsReservedRows = 2;

    try {
      this._logHeaders = new Uint32Array(opts.capacity);
      this._message = new Array(opts.capacity);
      ${generateColumnInit(columnMeta)}
      this._refreshViews(this._allocator.refreshViews());
    } catch (error) {
      this.free();
      throw error;
    }
  }

  _refreshViews(version) {
    ${generateViewRefresh(columnMeta)}
  }

  _getWasmColumnValue(columnIndex) {
    ${generateColumnLookup(columnMeta, false)}
  }

  _getWasmColumnNulls(columnIndex) {
    ${generateColumnLookup(columnMeta, true)}
  }

  ${generateColumnMethods(columnMeta)}

}

return WasmSpanBuffer;
`;

  // Create the class using Function constructor
  const factory = new Function(classCode);
  if (!isWasmSpanBufferFactory(factory)) {
    throw new Error('Expected WasmSpanBuffer factory function');
  }
  const WasmSpanBufferClass = factory();

  // Assign common methods to prototype (shared by all schemas)
  Object.defineProperty(WasmSpanBufferClass.prototype, 'timestamp', {
    get: wasmGetTimestamp,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'entry_type', {
    get: wasmGetEntryType,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, '_writeIndex', {
    get: wasmGetWriteIndex,
    set: wasmSetWriteIndex,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'trace_id', {
    get: wasmGetTraceId,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'thread_id', {
    get: wasmGetThreadId,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'span_id', {
    get: wasmGetSpanId,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'parent_thread_id', {
    get: wasmGetParentThreadId,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'parent_span_id', {
    get: wasmGetParentSpanId,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, '_hasParent', {
    get: wasmGetHasParent,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'message_values', {
    get: wasmGetMessageValues,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, 'message_nulls', {
    get: wasmGetMessageNulls,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, '_stats', {
    get: wasmGetStats,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, '_columns', {
    get: wasmGetColumns,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, '_spanStartTime', {
    get: wasmGetSpanStartTime,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, '_lastLoggedTime', {
    get: wasmGetLastLoggedTime,
    enumerable: true,
    configurable: true,
  });

  // Assign compatibility getters for AnySpanBuffer interface
  Object.defineProperty(WasmSpanBufferClass.prototype, '_system', {
    get: wasmGetSystem,
    enumerable: true,
    configurable: true,
  });
  Object.defineProperty(WasmSpanBufferClass.prototype, '_identity', {
    get: wasmGetIdentity,
    enumerable: true,
    configurable: true,
  });

  WasmSpanBufferClass.prototype._ensureWasmViews = wasmEnsureViews;
  WasmSpanBufferClass.prototype.message = wasmMessage;
  WasmSpanBufferClass.prototype.free = wasmFree;
  WasmSpanBufferClass.prototype.isColumnAllocated = wasmIsColumnAllocated;
  WasmSpanBufferClass.prototype.getColumnIfAllocated = wasmGetColumnIfAllocated;
  WasmSpanBufferClass.prototype.getNullsIfAllocated = wasmGetNullsIfAllocated;
  WasmSpanBufferClass.prototype.getOrCreateOverflow = wasmGetOrCreateOverflow;
  WasmSpanBufferClass.prototype._sealStats = wasmSealStats;
  WasmSpanBufferClass.prototype._sealStatsChain = wasmSealStatsChain;
  WasmSpanBufferClass.prototype[Symbol.for('nodejs.util.inspect.custom')] = wasmInspect;

  // Add static schema property
  Object.defineProperty(WasmSpanBufferClass, 'schema', {
    value: schema,
    writable: false,
    enumerable: true,
    configurable: false,
  });

  // Add static stats property (required by SpanContext and arrow-builder's ColumnWriter)
  // This matches the structure in spanBuffer.ts
  // IMPORTANT: stats object itself must be writable so totalWrites++ and spansCreated++ can modify it
  const statsObject = {
    capacity: 64, // Default capacity, will be overridden by allocator
    totalWrites: 0,
    spansCreated: 0,
  };
  Object.defineProperty(WasmSpanBufferClass, 'stats', {
    value: statsObject,
    writable: true,
    enumerable: true,
    configurable: true,
  });

  if (!isWasmSpanBufferConstructor(WasmSpanBufferClass, schema)) {
    throw new Error('Generated WasmSpanBuffer constructor lost its schema identity');
  }

  // Cache the class
  wasmSpanBufferClassCache.set(schema, WasmSpanBufferClass);

  return WasmSpanBufferClass;
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a WASM SpanBuffer instance.
 *
 * @param schema - LogSchema defining the buffer structure
 * @param opts - Options for buffer creation
 * @param _traceRoot - Trace root context
 * @param _scopeValues - Scope values for this span
 * @param _opMetadata - Op metadata for attribution
 * @param _callsiteMetadata - Callsite metadata for attribution
 * @returns Schema-specific WASM buffer instance
 */
export function createWasmSpanBuffer<T extends LogSchema>(
  schema: T,
  opts: Omit<
    WasmSpanBufferOptions<T>,
    'logSchema' | '_traceRoot' | '_scopeValues' | '_opMetadata' | '_callsiteMetadata' | '_vocabularyGeneration' | '_layout'
  >,
  _traceRoot: ITraceRoot<T>,
  _scopeValues: Readonly<Record<string, unknown>>,
  _opMetadata: OpMetadata,
  _callsiteMetadata: OpMetadata,
): WasmSpanBufferInstance<T> {
  const WasmSpanBufferClass = getWasmSpanBufferClass(schema);
  return new WasmSpanBufferClass({
    ...opts,
    _identityMode: 'root',
    _generation: ++nextWasmBufferGeneration,
    _layout: getWasmPhysicalLayout(schema, opts.capacity),
    logSchema: schema,
    _traceRoot,
    _scopeValues,
    _opMetadata,
    _callsiteMetadata,
    _vocabularyGeneration: _opMetadata._physicalLayoutPlan?.vocabularyGeneration ?? getVocabularyGeneration(),
  });
}

/**
 * Create a child WASM SpanBuffer linked to a parent.
 *
 * For cross-library calls, the child buffer may use a different schema than the parent.
 * Pass the schema option to use a different schema.
 *
 * @param parent - Parent WASM SpanBuffer
 * @param opts - Options for child buffer creation (schema is optional, defaults to parent's)
 * @param _traceRoot - Trace root context
 * @param _scopeValues - Scope values for this span
 * @param _opMetadata - Op metadata for attribution
 * @param _callsiteMetadata - Callsite metadata for attribution
 * @returns Schema-specific child WASM buffer
 */
export function createWasmChildSpanBuffer<T extends LogSchema>(
  parent: WasmSpanBufferInstance<T>,
  opts: Omit<
    WasmSpanBufferOptions<T>,
    | 'logSchema'
    | 'trace_id'
    | 'parent_thread_id'
    | 'parent_span_id'
    | '_traceRoot'
    | '_scopeValues'
    | '_opMetadata'
    | '_callsiteMetadata'
    | '_vocabularyGeneration'
    | '_layout'
  > & {
    schema?: T;
  },
  _traceRoot: ITraceRoot<T>,
  _scopeValues: Readonly<Record<string, unknown>>,
  _opMetadata: OpMetadata,
  _callsiteMetadata: OpMetadata,
): WasmSpanBufferInstance<T> {
  // Use provided schema (for cross-library calls) or parent's schema
  const childSchema = opts.schema ?? parent._logSchema;
  const WasmSpanBufferClass = getWasmSpanBufferClass(childSchema);

  const child = new WasmSpanBufferClass({
    ...opts,
    logSchema: childSchema,
    trace_id: '',
    parent_thread_id: parent.thread_id,
    parent_span_id: parent.span_id,
    _identityMode: 'child',
    _parent: parent,
    _generation: ++nextWasmBufferGeneration,
    _layout: getWasmPhysicalLayout(childSchema, opts.capacity),
    _traceRoot,
    _scopeValues,
    _opMetadata,
    _callsiteMetadata,
    _vocabularyGeneration: _opMetadata._physicalLayoutPlan?.vocabularyGeneration ?? getVocabularyGeneration(),
  });

  return child;
}

/**
 * Create an overflow WASM SpanBuffer for chaining.
 *
 * @param buffer - The full buffer that needs overflow handling
 * @param _traceRoot - Trace root context
 * @param _scopeValues - Scope values for this span
 * @param _opMetadata - Op metadata for attribution
 * @param _callsiteMetadata - Callsite metadata for attribution
 * @returns Schema-specific overflow WASM buffer
 */
export function createWasmOverflowBuffer<T extends LogSchema>(
  buffer: WasmSpanBufferInstance<T>,
  _traceRoot: ITraceRoot<T>,
  _scopeValues: Readonly<Record<string, unknown>>,
  _opMetadata: OpMetadata,
  _callsiteMetadata: OpMetadata,
): WasmSpanBufferInstance<T> {
  const WasmSpanBufferClass = getWasmSpanBufferClass(buffer._logSchema);

  const overflow = new WasmSpanBufferClass({
    allocator: buffer._allocator,
    capacity: buffer._capacity,
    trace_id: '',
    thread_id: buffer.thread_id,
    span_id: buffer.span_id,
    parent_thread_id: buffer.parent_thread_id,
    parent_span_id: buffer.parent_span_id,
    logSchema: buffer._logSchema,
    _traceRoot,
    _scopeValues,
    _opMetadata,
    _callsiteMetadata,
    _vocabularyGeneration: buffer._vocabularyGeneration,
    _identityMode: 'overflow',
    _identityPtr: buffer._identityPtr,
    _parent: buffer._parent,
    _generation: ++nextWasmBufferGeneration,
    _layout: buffer._layout,
  });
  overflow._statsReservedRows = 0;
  buffer._overflow = overflow;
  buffer._descriptor.overflow = overflow._descriptor;

  return overflow;
}
//#endregion smoo/lmao!n/wasm-mem.spanbuffer
