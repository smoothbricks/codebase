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
import type { AnySpanBuffer } from '../types.js';
import type { WasmAllocator } from './wasmAllocator.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Options for WASM SpanBuffer instances.
 * Used by createWasmSpanBuffer (root), createWasmChildSpanBuffer (child), and createWasmOverflowBuffer.
 * Internal fields (_traceRoot, _scopeValues, _opMetadata, _callsiteMetadata) are passed separately
 * by the create* functions.
 */
export interface WasmSpanBufferOptions {
  allocator: WasmAllocator;
  capacity: number;
  trace_id: string;
  thread_id: bigint;
  span_id: number;
  parent_thread_id?: bigint;
  parent_span_id?: number;
  logSchema: LogSchema;
  _traceRoot: ITraceRoot;
  _scopeValues: Readonly<Record<string, unknown>>;
  _opMetadata: OpMetadata;
  _callsiteMetadata: OpMetadata;
}

/**
 * WASM SpanBuffer instance interface.
 * Extends AnySpanBuffer with WASM-specific pointer properties.
 */
export interface WasmSpanBufferInstance extends AnySpanBuffer {
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

  // Override tree structure with WASM-typed versions
  _parent?: WasmSpanBufferInstance;
  _children: WasmSpanBufferInstance[];
  _overflow?: WasmSpanBufferInstance;

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
}

/**
 * Constructor type for generated WASM SpanBuffer classes.
 */
export interface WasmSpanBufferConstructor {
  new (opts: WasmSpanBufferOptions): WasmSpanBufferInstance;
  readonly schema: LogSchema;
  stats: SpanBufferStats; // Mutable stats shared across all instances
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
    const isEager = (field as { __eager?: boolean }).__eager === true;
    const enumValues = (field as { __enum_values?: readonly string[] }).__enum_values;

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

/**
 * Get timestamp column view from WASM memory.
 * Assigned to prototype after class generation.
 */
function wasmGetTimestamp(this: WasmSpanBufferInstance): BigInt64Array {
  return new BigInt64Array(this._allocator.memory.buffer, this._systemPtr, this._capacity);
}

/**
 * Get entry_type column view from WASM memory.
 * Assigned to prototype after class generation.
 */
function wasmGetEntryType(this: WasmSpanBufferInstance): Uint8Array {
  return new Uint8Array(this._allocator.memory.buffer, this._systemPtr + this._capacity * 8, this._capacity);
}

/**
 * Get writeIndex from WASM identity block.
 */
function wasmGetWriteIndex(this: WasmSpanBufferInstance): number {
  return this._allocator.readWriteIndex(this._identityPtr);
}

/**
 * Set writeIndex in WASM identity block.
 */
function wasmSetWriteIndex(this: WasmSpanBufferInstance, value: number): void {
  new DataView(this._allocator.memory.buffer).setUint32(this._identityPtr, value, true);
}

/**
 * Get trace_id from WASM identity block.
 */
function wasmGetTraceId(this: WasmSpanBufferInstance): string {
  const len = this._allocator.readIdentityTraceIdLen(this._identityPtr);
  if (len === 0) {
    let p = this._parent;
    while (p && p._parent) p = p._parent;
    return p ? p.trace_id : '';
  }
  const traceIdPtr = this._allocator.getIdentityTraceIdPtr(this._identityPtr);
  return new TextDecoder().decode(new Uint8Array(this._allocator.memory.buffer, traceIdPtr, len));
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
  const high = this._allocator.getThreadIdHigh();
  const low = this._allocator.getThreadIdLow();
  return (BigInt(high) << 32n) | BigInt(low);
}

/**
 * Get span_id from WASM identity block.
 */
function wasmGetSpanId(this: WasmSpanBufferInstance): number {
  return this._allocator.readIdentitySpanId(this._identityPtr);
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
  this._message[idx] = value;
  return this;
}

/**
 * Get message column values.
 */
function wasmGetMessageValues(this: WasmSpanBufferInstance): string[] {
  return this._message;
}

/**
 * Get message column nulls (always undefined for eager column).
 */
function wasmGetMessageNulls(this: WasmSpanBufferInstance): undefined {
  return undefined;
}

/**
 * Free all WASM memory for this buffer.
 */
function wasmFree(this: WasmSpanBufferInstance): void {
  // Free identity block
  this._allocator.freeIdentity(this._identityPtr);

  // Free system block
  this._allocator.freeSpanSystem(this._systemPtr);

  // Free column blocks
  const columnMeta = buildColumnMeta(this._logSchema);
  for (const col of columnMeta) {
    if (col.sizeClass !== 'string' && this._columnPtrs[col.columnIndex] >= 0) {
      const freeMethod = `free${col.sizeClass.toUpperCase()}` as 'free1B' | 'free4B' | 'free8B';
      this._allocator[freeMethod](this._columnPtrs[col.columnIndex]);
    }
  }
}

/**
 * Check if column is allocated.
 */
function wasmIsColumnAllocated(this: WasmSpanBufferInstance, columnIndex: number): boolean {
  return this._columnPtrs[columnIndex] >= 0;
}

/**
 * Get column values if allocated.
 */
function wasmGetColumnIfAllocated(this: WasmSpanBufferInstance, columnName: string): ColumnValueType | undefined {
  const idx = this._logSchema._columnNames.indexOf(columnName);
  if (idx === -1) return undefined;
  const getter = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(this), columnName + '_values');
  if (getter && getter.get) {
    return getter.get.call(this) as ColumnValueType | undefined;
  }
  return undefined;
}

/**
 * Get column nulls if allocated.
 */
function wasmGetNullsIfAllocated(this: WasmSpanBufferInstance, columnName: string): Uint8Array | undefined {
  const getter = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(this), columnName + '_nulls');
  if (getter && getter.get) {
    return getter.get.call(this);
  }
  return undefined;
}

/**
 * Get system ArrayBuffer for compatibility with AnySpanBuffer.
 * WASM uses shared memory, so this returns the entire WASM memory buffer.
 */
function wasmGetSystem(this: WasmSpanBufferInstance): ArrayBuffer {
  return this._allocator.memory.buffer;
}

/**
 * Get identity Uint8Array for compatibility with AnySpanBuffer.
 * Returns a view of the identity block (48 bytes) in WASM memory.
 */
function wasmGetIdentity(this: WasmSpanBufferInstance): Uint8Array {
  // Identity block is 48 bytes: writeIndex(4) + reserved(4) + span_id(4) + trace_id_len(1) + trace_id(up to 35)
  return new Uint8Array(this._allocator.memory.buffer, this._identityPtr, 48);
}

/**
 * Get or create overflow buffer.
 */
function wasmGetOrCreateOverflow(this: WasmSpanBufferInstance): WasmSpanBufferInstance {
  if (this._overflow) return this._overflow;
  const tracer = this._traceRoot.tracer;
  tracer.onStatsWillResetFor(this);
  checkCapacityTuning((this.constructor as WasmSpanBufferConstructor).stats);
  // WasmSpanBufferInstance extends AnySpanBuffer and has all SpanBuffer properties at runtime
  const overflow = tracer.bufferStrategy.createOverflowBuffer(this);
  return overflow as WasmSpanBufferInstance;
}

/**
 * Get _stats property.
 */
function wasmGetStats(this: WasmSpanBufferInstance): SpanBufferStats {
  return (this.constructor as WasmSpanBufferConstructor).stats;
}

/**
 * Get _columns property.
 */
function wasmGetColumns(this: WasmSpanBufferInstance): LogSchema['_columns'] {
  return (this.constructor as WasmSpanBufferConstructor).schema._columns;
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
function generateEagerColumnInit(columnMeta: ColumnMeta[]): string {
  const lines: string[] = [];

  for (const col of columnMeta) {
    if (col.name === 'message') continue; // Message handled separately

    if (col.sizeClass === 'string') {
      if (col.isEager) {
        lines.push(`this._${col.name}_values = new Array(this._capacity);`);
      } else {
        lines.push(`this._${col.name}_values = undefined;`);
        lines.push(`this._${col.name}_nulls = undefined;`);
      }
    } else if (col.isEager) {
      lines.push(`this._columnPtrs[${col.columnIndex}] = this._allocator.alloc${col.sizeClass.toUpperCase()}();`);
    }
  }

  return lines.join('\n    ');
}

/**
 * Generate setter method for a numeric column (stored in WASM).
 * Calls WASM exports directly for hot path optimization.
 */
function generateNumericSetter(col: ColumnMeta): string {
  const allocMethod = `alloc${col.sizeClass.toUpperCase()}`;

  // Map to WASM export names
  const writeExportName =
    col.schemaType === 'number' || col.schemaType === 'bigUint64'
      ? 'write_col_f64'
      : col.sizeClass === '1b'
        ? 'write_col_u8'
        : col.sizeClass === '4b'
          ? 'write_col_u32'
          : 'write_col_f64';

  if (col.isEager) {
    // Eager: column is pre-allocated, just write value (call WASM export directly)
    return `${col.name}(idx, value) {
    const ptr = this._columnPtrs[${col.columnIndex}];
    this._allocator.exports.${writeExportName}(ptr, idx, value, this._capacity);
    return this;
  }`;
  }

  // Lazy: allocate on first write (call WASM export directly)
  return `${col.name}(idx, value) {
    let ptr = this._columnPtrs[${col.columnIndex}];
    if (ptr < 0) {
      ptr = this._allocator.${allocMethod}();
      this._columnPtrs[${col.columnIndex}] = ptr;
    }
    this._allocator.exports.${writeExportName}(ptr, idx, value, this._capacity);
    return this;
  }`;
}

/**
 * Generate getter for numeric column values (creates view on demand).
 */
function generateNumericValuesGetter(col: ColumnMeta): string {
  const arrayType =
    col.schemaType === 'number'
      ? 'Float64Array'
      : col.schemaType === 'bigUint64'
        ? 'BigUint64Array'
        : col.sizeClass === '1b'
          ? 'Uint8Array'
          : col.sizeClass === '4b'
            ? 'Uint32Array'
            : 'Float64Array';

  const bytesPerElement = col.sizeClass === '1b' ? 1 : col.sizeClass === '4b' ? 4 : 8;

  return `get ${col.name}_values() {
    const ptr = this._columnPtrs[${col.columnIndex}];
    if (ptr < 0) return ${col.isEager ? 'null' : 'undefined'};
    const nullBitmapSize = Math.ceil(this._capacity / 8);
    const valueOffset = ptr + nullBitmapSize;
    // Align offset for typed array
    const alignedOffset = (valueOffset + ${bytesPerElement - 1}) & ~${bytesPerElement - 1};
    return new ${arrayType}(
      this._allocator.memory.buffer,
      alignedOffset,
      this._capacity
    );
  }`;
}

/**
 * Generate getter for numeric column nulls bitmap.
 */
function generateNumericNullsGetter(col: ColumnMeta): string {
  if (col.isEager) {
    // Eager columns don't have null bitmaps
    return `get ${col.name}_nulls() {
    return undefined;
  }`;
  }

  return `get ${col.name}_nulls() {
    const ptr = this._columnPtrs[${col.columnIndex}];
    if (ptr < 0) return undefined;
    const nullBitmapSize = Math.ceil(this._capacity / 8);
    return new Uint8Array(
      this._allocator.memory.buffer,
      ptr,
      nullBitmapSize
    );
  }`;
}

/**
 * Generate setter method for a string column (stored in JS).
 */
function generateStringSetter(col: ColumnMeta): string {
  if (col.isEager) {
    return `${col.name}(idx, value) {
    this._${col.name}_values[idx] = value;
    return this;
  }`;
  }

  // Lazy string column: allocate arrays on first write
  return `${col.name}(idx, value) {
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
const wasmSpanBufferClassCache = new WeakMap<LogSchema, WasmSpanBufferConstructor>();

/**
 * Generate a WASM SpanBuffer class for the given schema.
 *
 * Generates minimal constructor + column methods, then assigns common methods to prototype.
 *
 * @param schema - LogSchema defining the buffer structure
 * @returns WasmSpanBufferConstructor for creating buffer instances
 */
export function getWasmSpanBufferClass(schema: LogSchema): WasmSpanBufferConstructor {
  const cached = wasmSpanBufferClassCache.get(schema);
  if (cached) {
    return cached;
  }

  // Pre-compute column metadata
  const columnMeta = buildColumnMeta(schema);

  // Generate ONLY schema-specific code (constructor + column methods)
  const classCode = `
class WasmSpanBuffer {
  constructor(opts) {
    // Store allocator and capacity
    this._allocator = opts.allocator;
    this._capacity = opts.capacity;
    this._logSchema = opts.logSchema;

    // Allocate identity block from WASM (root has trace_id, child does not)
    if (opts.trace_id) {
      const traceIdBytes = new TextEncoder().encode(opts.trace_id);
      const packed = opts.allocator.allocIdentityRootForJsWrite(traceIdBytes.length);
      this._identityPtr = Number(packed >> 32n);
      const traceIdOffset = Number(packed & 0xFFFFFFFFn);
      new Uint8Array(opts.allocator.memory.buffer).set(traceIdBytes, traceIdOffset);
    } else {
      this._identityPtr = opts.allocator.allocIdentityChild();
    }

    // Allocate system block from WASM
    this._systemPtr = opts.allocator.allocSpanSystem();

    // Initialize column pointers as unallocated (-1)
    this._columnPtrs = new Int32Array(${columnMeta.length}).fill(-1);

    // Tree structure
    this._parent = null;
    this._children = [];
    this._overflow = null;

    // Assign context properties from opts
    this._traceRoot = opts._traceRoot;
    this._scopeValues = opts._scopeValues;
    this._opMetadata = opts._opMetadata;
    this._callsiteMetadata = opts._callsiteMetadata;

    // Initialize message array
    this._message = new Array(opts.capacity);

    // Initialize eager columns
    ${generateEagerColumnInit(columnMeta)}
  }

  // Schema-specific column methods (ONLY part that differs per schema)
  ${generateColumnMethods(columnMeta)}
}

return WasmSpanBuffer;
`;

  // Create the class using Function constructor
  const factory = new Function(classCode) as () => WasmSpanBufferConstructor;
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

  WasmSpanBufferClass.prototype.message = wasmMessage;
  WasmSpanBufferClass.prototype.free = wasmFree;
  WasmSpanBufferClass.prototype.isColumnAllocated = wasmIsColumnAllocated;
  WasmSpanBufferClass.prototype.getColumnIfAllocated = wasmGetColumnIfAllocated;
  WasmSpanBufferClass.prototype.getNullsIfAllocated = wasmGetNullsIfAllocated;
  WasmSpanBufferClass.prototype.getOrCreateOverflow = wasmGetOrCreateOverflow;
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
 * @returns WasmSpanBufferInstance
 */
export function createWasmSpanBuffer(
  schema: LogSchema,
  opts: Omit<WasmSpanBufferOptions, 'logSchema' | '_traceRoot' | '_scopeValues' | '_opMetadata' | '_callsiteMetadata'>,
  _traceRoot: ITraceRoot,
  _scopeValues: Readonly<Record<string, unknown>>,
  _opMetadata: OpMetadata,
  _callsiteMetadata: OpMetadata,
): WasmSpanBufferInstance {
  const WasmSpanBufferClass = getWasmSpanBufferClass(schema);
  return new WasmSpanBufferClass({
    ...opts,
    logSchema: schema,
    _traceRoot,
    _scopeValues,
    _opMetadata,
    _callsiteMetadata,
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
 * @returns WasmSpanBufferInstance linked to parent
 */
export function createWasmChildSpanBuffer(
  parent: WasmSpanBufferInstance,
  opts: Omit<
    WasmSpanBufferOptions,
    | 'logSchema'
    | 'trace_id'
    | 'parent_thread_id'
    | 'parent_span_id'
    | '_traceRoot'
    | '_scopeValues'
    | '_opMetadata'
    | '_callsiteMetadata'
  > & {
    schema?: LogSchema;
  },
  _traceRoot: ITraceRoot,
  _scopeValues: Readonly<Record<string, unknown>>,
  _opMetadata: OpMetadata,
  _callsiteMetadata: OpMetadata,
): WasmSpanBufferInstance {
  // Use provided schema (for cross-library calls) or parent's schema
  const childSchema = opts.schema ?? parent._logSchema;
  const WasmSpanBufferClass = getWasmSpanBufferClass(childSchema);

  const child = new WasmSpanBufferClass({
    ...opts,
    logSchema: childSchema, // Use the child's schema (may differ from parent)
    trace_id: parent.trace_id,
    parent_thread_id: parent.thread_id,
    parent_span_id: parent.span_id,
    _traceRoot,
    _scopeValues,
    _opMetadata,
    _callsiteMetadata,
  });

  // Link to parent (SpanContext will push to parent._children with possible RemappedBufferView wrapper)
  child._parent = parent;

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
 * @returns WasmSpanBufferInstance linked as overflow
 */
export function createWasmOverflowBuffer(
  buffer: WasmSpanBufferInstance,
  _traceRoot: ITraceRoot,
  _scopeValues: Readonly<Record<string, unknown>>,
  _opMetadata: OpMetadata,
  _callsiteMetadata: OpMetadata,
): WasmSpanBufferInstance {
  const WasmSpanBufferClass = getWasmSpanBufferClass(buffer._logSchema);

  const overflow = new WasmSpanBufferClass({
    allocator: buffer._allocator,
    capacity: buffer._capacity,
    trace_id: buffer.trace_id,
    thread_id: buffer.thread_id,
    span_id: buffer.span_id,
    parent_thread_id: buffer.parent_thread_id,
    parent_span_id: buffer.parent_span_id,
    logSchema: buffer._logSchema,
    _traceRoot,
    _scopeValues,
    _opMetadata,
    _callsiteMetadata,
  });

  // Link as overflow (not child - same logical span)
  overflow._parent = buffer._parent;
  buffer._overflow = overflow;

  return overflow;
}
