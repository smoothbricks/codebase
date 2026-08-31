/**
 * Thin SpanBuffer facade over a ThreadSpanBufferBinding.
 *
 * Overflow lives inside the native row store. Scope is a side table (01i latest
 * value); this view never prefills future rows. Generated loggers still write
 * `*_values[idx] = v` — those lanes are proxies that forward to the binding.
 */

import type { RemapDescriptor } from '../logBinding.js';
import type { OpMetadata } from '../opContext/opTypes.js';
import type { LogSchema } from '../schema/LogSchema.js';
import { ENTRY_TYPE_SPAN_ERR, ENTRY_TYPE_SPAN_EXCEPTION, THREAD_ATTRIBUTE_KINDS } from '../schema/systemSchema.js';
import { getEnumValues, getSchemaType } from '../schema/typeGuards.js';
import type { SpanBufferStats } from '../spanBufferStats.js';
import { getThreadId } from '../threadId.js';
import type { ITraceRoot } from '../traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from '../types.js';
import { getVocabularyGeneration } from '../vocabularyRegistry.js';
import { attributeKindForSchemaType, isThreadSystemColumn, schemaAttributeOrdinals } from './schemaBlob.js';
import { THREAD_SPAN_BUFFER_OK, type ThreadAttributeKind, type ThreadSpanBufferBinding } from './threadSpanBuffer.js';
import type { ThreadSpanBufferRuntime } from './threadSpanBufferHost.js';

export const THREAD_SPAN_VIEW = Symbol('thread-span-view');
const EMPTY_SCOPE: Readonly<Record<string, unknown>> = Object.freeze({});

const KIND_NUMBER = THREAD_ATTRIBUTE_KINDS[0].discriminant;
const KIND_UINT64 = THREAD_ATTRIBUTE_KINDS[1].discriminant;
const KIND_BOOLEAN = THREAD_ATTRIBUTE_KINDS[2].discriminant;
const KIND_TEXT = THREAD_ATTRIBUTE_KINDS[3].discriminant;
const KIND_ENUM = THREAD_ATTRIBUTE_KINDS[4].discriminant;

/**
 * `_laneStore` slots. System lanes take fixed slots; a schema attribute at
 * index `i` takes `LANE_SCHEMA_BASE + 2 * i` for values and `+ 1` for nulls.
 */
const LANE_MESSAGE = 0;
const LANE_ERROR_CODE = 1;
const LANE_EXCEPTION_STACK = 2;
const LANE_FF_VALUE = 3;
const LANE_SCHEMA_BASE = 4;

/** Null-lane sink size: covers any row index a single span can reach. */
const NULL_LANE_BYTES = 8192;

const bits = new DataView(new ArrayBuffer(8));

function f64Bits(value: number): bigint {
  bits.setFloat64(0, value, true);
  return bits.getBigUint64(0, true);
}

/**
 * A write-only indexable sink that forwards `lane[i] = v` into the row store.
 *
 * The target array is never populated: the native store is the only reader of
 * these values, so mirroring them into JS would allocate a second copy of
 * every row for nobody. `length` still tracks so generated loggers that read
 * it back see a coherent lane.
 */
function laneProxy(write: (index: number, value: unknown) => void): unknown[] {
  const target: unknown[] = [];
  return new Proxy(target, {
    set(obj, prop, value) {
      if (prop === 'length') {
        obj.length = Number(value);
        return true;
      }
      const index = typeof prop === 'string' ? Number(prop) : Number.NaN;
      if (Number.isInteger(index) && index >= 0) {
        write(index, value);
        if (index >= obj.length) obj.length = index + 1;
        return true;
      }
      Reflect.set(obj, prop, value);
      return true;
    },
  });
}

export interface ThreadSpanViewArgs<T extends LogSchema = LogSchema> {
  runtime: ThreadSpanBufferRuntime;
  binding: ThreadSpanBufferBinding;
  schema: T;
  traceRoot: ITraceRoot;
  opMetadata: OpMetadata;
  callsiteMetadata: OpMetadata;
  parent?: AnySpanBuffer;
  stats: SpanBufferStats;
}

export class ThreadSpanView {
  readonly [THREAD_SPAN_VIEW] = true;
  readonly runtime: ThreadSpanBufferRuntime;
  readonly binding: ThreadSpanBufferBinding;
  readonly layout: ThreadSpanLayout;
  readonly ordinals: ReadonlyMap<string, number>;
  readonly kinds: ReadonlyMap<string, number>;
  readonly enumVariants: ReadonlyMap<string, readonly string[]>;

  spanId = 0;
  startRow = 0;
  completionRow = 1;
  lastRow = 0;
  pendingLine = 0;
  pendingEntryType: number | undefined;
  opened = false;
  readonly fakeToReal = new Map<number, number>();

  _writeIndex = 0;
  readonly _capacity = Number.MAX_SAFE_INTEGER;
  _overflow: AnySpanBuffer | undefined;
  _statsSealed = false;
  _statsReservedRows = 2;
  _nodeIndex = 0xffffffff;
  _topologyGeneration = 0;
  _parent?: AnySpanBuffer;
  _traceRoot: ITraceRoot;
  _opMetadata: OpMetadata;
  _callsiteMetadata?: OpMetadata;
  _scopeValues: Readonly<Record<string, unknown>> = EMPTY_SCOPE;
  _remapDescriptor?: RemapDescriptor;
  readonly _logSchema: LogSchema;
  readonly _columns: ReadonlyArray<[string, unknown]>;
  readonly _stats: SpanBufferStats;
  readonly _vocabularyGeneration = getVocabularyGeneration();
  readonly _messageLayoutFamily = 'mixed' as const;
  readonly _messagePhysicalLayout = 'current' as const;
  readonly _system = new ArrayBuffer(8);
  readonly _identity = new Uint8Array(12);
  readonly timestamp = new BigInt64Array(2);
  readonly entry_type = new Uint8Array(2);
  /**
   * Lazily materialized write lanes, indexed by `LANE_*` slot.
   *
   * Lanes are proxies whose only job is to forward `lane[i] = v` into the
   * native row store; nothing ever reads them back. Materializing them on
   * demand keeps every view's own-property set identical to this class's
   * declared fields — no span pays a hidden-class transition — and a span
   * that never touches a lane never allocates one.
   */
  readonly _laneStore: unknown[] = [];
  readonly line_values = new Float64Array(1);
  readonly line_nulls = new Uint8Array(1);
  readonly error_code_nulls = new Uint8Array(1);
  readonly retry_attempt_values = new Float64Array(1);
  readonly retry_attempt_nulls = new Uint8Array(1);
  readonly retry_delay_ms_values = new Float64Array(1);
  readonly retry_delay_ms_nulls = new Uint8Array(1);
  readonly exception_stack_nulls = new Uint8Array(1);
  readonly ff_value_nulls = new Uint8Array(1);
  readonly uint64_value_values = new BigUint64Array(1);
  readonly uint64_value_nulls = new Uint8Array(1);
  readonly thread_id: bigint;
  readonly _threadId: bigint;
  parent_span_id = 0;
  parent_thread_id = 0n;
  _hasParent = false;
  _spanName?: string | number;

  get message_values(): (string | undefined)[] {
    const existing = this._laneStore[LANE_MESSAGE];
    if (existing !== undefined) return existing as (string | undefined)[];
    const lane = laneProxy((index, value) => {
      this.commitLog(index, typeof value === 'string' ? value : String(value));
    }) as (string | undefined)[];
    this._laneStore[LANE_MESSAGE] = lane;
    return lane;
  }

  get error_code_values(): unknown[] {
    const existing = this._laneStore[LANE_ERROR_CODE];
    if (existing !== undefined) return existing as unknown[];
    const lane = laneProxy((index, value) => {
      this.writeNamed('error_code', this.physicalRow(index), value);
    });
    this._laneStore[LANE_ERROR_CODE] = lane;
    return lane;
  }

  get exception_stack_values(): unknown[] {
    const existing = this._laneStore[LANE_EXCEPTION_STACK];
    if (existing !== undefined) return existing as unknown[];
    const lane = laneProxy((index, value) => {
      this.writeNamed('exception_stack', this.physicalRow(index), value);
    });
    this._laneStore[LANE_EXCEPTION_STACK] = lane;
    return lane;
  }

  get ff_value_values(): unknown[] {
    const existing = this._laneStore[LANE_FF_VALUE];
    if (existing !== undefined) return existing as unknown[];
    const lane = laneProxy((index, value) => {
      this.writeNamed('ff_value', this.physicalRow(index), value);
    });
    this._laneStore[LANE_FF_VALUE] = lane;
    return lane;
  }

  constructor(args: ThreadSpanViewArgs) {
    this.runtime = args.runtime;
    this.binding = args.binding;
    this._logSchema = args.schema;
    this._columns = args.schema._columns;
    this._traceRoot = args.traceRoot;
    this._opMetadata = args.opMetadata;
    this._callsiteMetadata = args.callsiteMetadata;
    this._parent = args.parent;
    this._stats = args.stats;
    this.thread_id = getThreadId();
    this._threadId = this.thread_id;
    this._hasParent = args.parent !== undefined;
    if (args.parent !== undefined) {
      this.parent_span_id = args.parent.span_id;
      this.parent_thread_id = args.parent.thread_id;
    }
    const layout = threadSpanLayoutFor(args.schema);
    this.layout = layout;
    this.ordinals = layout.ordinals;
    this.kinds = layout.kinds;
    this.enumVariants = layout.enumVariants;
    // Attribute lanes and writer methods live on `layout.ViewClass.prototype`;
    // the constructor deliberately adds nothing beyond this class's declared
    // fields, so every span of a schema shares one hidden class.
  }

  get span_id(): number {
    return this.spanId;
  }

  get trace_id(): string {
    return this._traceRoot.trace_id;
  }

  get _spanStartTime(): bigint {
    return this.timestamp[0] ?? 0n;
  }

  get _lastLoggedTime(): bigint | null {
    return this.timestamp[0] === 0n ? null : this.timestamp[0];
  }

  beginLog(entryType: number): number {
    if (!this.opened) this.openSpan(this._spanName ?? 'span');
    this.pendingEntryType = entryType;
    const fake = this._writeIndex;
    this._writeIndex = fake + 1;
    return fake;
  }

  openSpan(name: string | number): void {
    if (this.opened) return;
    const timestamp = this._traceRoot._timestampNow(this._traceRoot);
    const label = typeof name === 'string' ? name : String(name);
    const nameId = this.runtime.intern(this.binding, label);
    const trace = this.runtime.writeUtf8(this._traceRoot.trace_id);
    const packed = this.binding.openSpan(
      trace.ptr,
      trace.len,
      this.parent_thread_id,
      this.parent_span_id,
      nameId,
      timestamp,
      this.pendingLine,
    );
    if (packed === 0n) throw new Error('thread_span_buffer_open_span failed');
    this.spanId = Number(packed >> 32n);
    this.startRow = Number(packed & 0xffffffffn);
    this.completionRow = this.startRow + 1;
    this.lastRow = this.startRow;
    this._writeIndex = 2;
    this.timestamp[0] = timestamp;
    this.entry_type[0] = 1;
    this.line_values[0] = this.pendingLine;
    this._spanName = name;
    this.opened = true;
    this._stats.spansCreated += 1;
  }

  end(entryType: number): void {
    if (!this.opened) this.openSpan(this._spanName ?? 'span');
    const timestamp = this._traceRoot._timestampNow(this._traceRoot);
    const status =
      entryType === ENTRY_TYPE_SPAN_ERR || entryType === ENTRY_TYPE_SPAN_EXCEPTION
        ? this.binding.endErr(this.spanId, timestamp)
        : this.binding.endOk(this.spanId, timestamp);
    if (status !== THREAD_SPAN_BUFFER_OK) throw new Error('thread_span_buffer_end failed');
    this.timestamp[1] = timestamp;
    this.entry_type[1] = entryType;
  }

  writeNamed(name: string, row: number, value: unknown): this {
    const ordinal = this.ordinals.get(name);
    const kind = this.kinds.get(name);
    if (ordinal === undefined || kind === undefined) return this;
    if (value === null || value === undefined) return this;
    const scalar = this.encodeValue(name, kind, value);
    const status = this.binding.writeAttr(row, ordinal, kind as ThreadAttributeKind, scalar);
    if (status !== THREAD_SPAN_BUFFER_OK) throw new Error(`thread_span_buffer_write_attr failed for ${name}`);
    return this;
  }

  writeTagNamed(name: string, value: unknown): this {
    const ordinal = this.ordinals.get(name);
    const kind = this.kinds.get(name);
    if (ordinal === undefined || kind === undefined) return this;
    if (value === null || value === undefined) return this;
    const scalar = this.encodeValue(name, kind, value);
    const status = this.binding.writeTag(this.spanId, ordinal, kind as ThreadAttributeKind, scalar);
    if (status !== THREAD_SPAN_BUFFER_OK) throw new Error(`thread_span_buffer_write_tag failed for ${name}`);
    return this;
  }

  syncScope(attributes: object): void {
    const next: Record<string, unknown> = { ...this._scopeValues };
    for (const key of Object.keys(attributes)) {
      const value = Reflect.get(attributes, key);
      if (value === null) delete next[key];
      else if (value !== undefined) next[key] = value;
      const ordinal = this.ordinals.get(key);
      if (ordinal === undefined) continue;
      if (value === null) {
        this.binding.setScope(this.spanId, ordinal, 0, 0n);
        continue;
      }
      const kind = this.kinds.get(key);
      if (value === undefined || kind === undefined) continue;
      const scalar = this.encodeValue(key, kind, value);
      this.binding.setScope(this.spanId, ordinal, kind, scalar);
    }
    this._scopeValues = Object.freeze(next);
  }

  line(pos: number, val: number): this {
    if (!this.opened) this.pendingLine = val;
    if (pos === 0) this.line_values[0] = val;
    return this;
  }

  message(pos: number, val: string): this {
    if (pos === 0 && !this.opened) {
      this._spanName = val;
      return this;
    }
    this.message_values[pos] = val;
    return this;
  }

  error_code(_pos: number, val: string): this {
    return this.writeNamed('error_code', this.completionRow, val);
  }

  retry_attempt(_pos: number, val: number): this {
    return this.writeNamed('retry_attempt', this.lastRow, val);
  }

  retry_delay_ms(_pos: number, val: number): this {
    return this.writeNamed('retry_delay_ms', this.lastRow, val);
  }

  exception_stack(_pos: number, val: string): this {
    return this.writeNamed('exception_stack', this.completionRow, val);
  }

  ff_value(_pos: number, val: string): this {
    return this.writeNamed('ff_value', this.lastRow, val);
  }

  uint64_value(_pos: number, val: bigint): this {
    return this.writeNamed('uint64_value', this.lastRow, val);
  }

  getOrCreateOverflow(): AnySpanBuffer {
    return this;
  }

  _sealStats(): void {
    this._statsSealed = true;
  }

  _sealStatsChain(): void {
    this._statsSealed = true;
  }

  isParentOf(other: AnySpanBuffer): boolean {
    return other._parent === this;
  }

  isChildOf(other: AnySpanBuffer): boolean {
    return this._parent === other;
  }

  physicalRow(index: number): number {
    return this.fakeToReal.get(index) ?? index;
  }

  private commitLog(fakeIndex: number, message: string): void {
    const existing = this.fakeToReal.get(fakeIndex);
    if (existing !== undefined) return;
    if (!this.opened) this.openSpan(this._spanName ?? 'span');
    const entryType = this.pendingEntryType ?? 8;
    this.pendingEntryType = undefined;
    const timestamp = this._traceRoot._timestampNow(this._traceRoot);
    // Intern to a u32 and pass the ordinal, rather than re-encoding the same
    // message to UTF-8 on every row. Vocabulary ids are stable per handle, so
    // a repeated message costs one Map lookup and no encode at all.
    const messageOrdinal = this.runtime.intern(this.binding, message);
    const packed = this.binding.appendLog(this.spanId, entryType, messageOrdinal, timestamp, this.pendingLine);
    if (packed === 0n) throw new Error('thread_span_buffer_append_log failed');
    const row = Number(packed & 0xffffffffn);
    this.fakeToReal.set(fakeIndex, row);
    this.lastRow = row;
  }

  private encodeValue(name: string, kind: number, value: unknown): bigint {
    if (kind === KIND_NUMBER) {
      if (typeof value !== 'number') throw new TypeError(`${name} expects number`);
      return f64Bits(value);
    }
    if (kind === KIND_UINT64) {
      if (typeof value !== 'bigint') throw new TypeError(`${name} expects bigint`);
      return value;
    }
    if (kind === KIND_BOOLEAN) {
      if (typeof value !== 'boolean') throw new TypeError(`${name} expects boolean`);
      return value ? 1n : 0n;
    }
    if (kind === KIND_ENUM) {
      const variants = this.enumVariants.get(name);
      if (variants === undefined) throw new TypeError(`${name} is missing enum variants`);
      const index = typeof value === 'number' ? value : variants.indexOf(String(value));
      if (index < 0) throw new TypeError(`${name} has no variant ${String(value)}`);
      return BigInt(index);
    }
    if (kind === KIND_TEXT) {
      if (typeof value !== 'string') throw new TypeError(`${name} expects string`);
      return BigInt(this.runtime.intern(this.binding, value));
    }
    throw new TypeError(`${name} has unsupported attribute kind ${kind}`);
  }
}

export function isThreadSpanView(value: unknown): value is ThreadSpanView {
  return typeof value === 'object' && value !== null && THREAD_SPAN_VIEW in value;
}

export function requireThreadSpanView(value: AnySpanBuffer): ThreadSpanView {
  if (!isThreadSpanView(value)) throw new TypeError('expected ThreadSpanView');
  return value;
}

/**
 * Everything the view needs that depends only on the schema, resolved once.
 *
 * Building this per span was the lane's fixed floor: two Map builds over
 * `_columnNames` plus three `Object.defineProperty` calls per attribute, all
 * recomputing values fixed at schema-definition time.
 */
export interface ThreadSpanLayout {
  readonly ordinals: ReadonlyMap<string, number>;
  readonly kinds: ReadonlyMap<string, number>;
  readonly enumVariants: ReadonlyMap<string, readonly string[]>;
  readonly attributeNames: readonly string[];
  readonly ViewClass: new (args: ThreadSpanViewArgs) => ThreadSpanView;
}

const layouts = new WeakMap<LogSchema, ThreadSpanLayout>();

function buildLayout(schema: LogSchema): ThreadSpanLayout {
  const ordinals = schemaAttributeOrdinals(schema);
  const kinds = new Map<string, number>();
  const enumVariants = new Map<string, readonly string[]>();
  for (const name of schema._columnNames) {
    if (isThreadSystemColumn(name)) continue;
    const type = getSchemaType(schema.fields[name]);
    if (type === undefined) continue;
    const kind = attributeKindForSchemaType(type);
    if (kind === undefined) continue;
    kinds.set(name, kind);
    if (type === 'enum') {
      const variants = getEnumValues(schema.fields[name]);
      if (variants) enumVariants.set(name, variants);
    }
  }

  // One subclass per schema carries the attribute writer methods and the
  // attribute lanes on its prototype. Doing this per span was the lane's fixed
  // floor: three Object.defineProperty calls and two allocations per attribute
  // on an instance that already has forty declared fields.
  class SchemaBoundThreadSpanView extends ThreadSpanView {}
  const descriptors: PropertyDescriptorMap = {};
  const attributeNames = [...ordinals.keys()];
  for (let index = 0; index < attributeNames.length; index++) {
    const name = attributeNames[index] ?? '';
    const valuesSlot = LANE_SCHEMA_BASE + 2 * index;
    const nullsSlot = valuesSlot + 1;
    descriptors[name] = {
      value: function attributeWriter(this: ThreadSpanView, pos: number, val: unknown): ThreadSpanView {
        if (pos === 0) return this.writeTagNamed(name, val);
        if (pos === 1) return this.writeNamed(name, this.completionRow, val);
        return this.writeNamed(name, this.physicalRow(pos), val);
      },
      writable: true,
      configurable: true,
      enumerable: false,
    };
    descriptors[`${name}_values`] = {
      get: function attributeLane(this: ThreadSpanView): unknown[] {
        const existing = this._laneStore[valuesSlot];
        if (existing !== undefined) return existing as unknown[];
        const lane = laneProxy((rowIndex, value) => {
          this.writeNamed(name, this.physicalRow(rowIndex), value);
        });
        this._laneStore[valuesSlot] = lane;
        return lane;
      },
      configurable: true,
      enumerable: false,
    };
    descriptors[`${name}_nulls`] = {
      get: function attributeNulls(this: ThreadSpanView): Uint8Array {
        const existing = this._laneStore[nullsSlot];
        if (existing !== undefined) return existing as Uint8Array;
        // Write-only sink: the native row store holds validity, and nothing
        // in the flush path reads this lane. It exists so generated loggers
        // can keep their unconditional `${name}_nulls[i >>> 3] |= …` store.
        const lane = new Uint8Array(NULL_LANE_BYTES);
        this._laneStore[nullsSlot] = lane;
        return lane;
      },
      configurable: true,
      enumerable: false,
    };
  }
  Object.defineProperties(SchemaBoundThreadSpanView.prototype, descriptors);

  const layout: ThreadSpanLayout = {
    ordinals,
    kinds,
    enumVariants,
    attributeNames,
    ViewClass: SchemaBoundThreadSpanView,
  };
  layouts.set(schema, layout);
  return layout;
}

export function threadSpanLayoutFor(schema: LogSchema): ThreadSpanLayout {
  return layouts.get(schema) ?? buildLayout(schema);
}

export function createThreadSpanView<T extends LogSchema>(args: ThreadSpanViewArgs<T>): SpanBuffer<T> {
  return new (threadSpanLayoutFor(args.schema).ViewClass)(args) as SpanBuffer<T> & ThreadSpanView;
}
