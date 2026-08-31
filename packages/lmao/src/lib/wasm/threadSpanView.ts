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

const bits = new DataView(new ArrayBuffer(8));

function f64Bits(value: number): bigint {
  bits.setFloat64(0, value, true);
  return bits.getBigUint64(0, true);
}

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
        obj[index] = value;
        return true;
      }
      Reflect.set(obj, prop, value);
      return true;
    },
  });
}

export class ThreadSpanView {
  readonly [THREAD_SPAN_VIEW] = true;
  readonly runtime: ThreadSpanBufferRuntime;
  readonly binding: ThreadSpanBufferBinding;
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
  readonly message_values: (string | undefined)[];
  readonly line_values = new Float64Array(1);
  readonly line_nulls = new Uint8Array(1);
  readonly error_code_values: unknown[];
  readonly error_code_nulls = new Uint8Array(1);
  readonly retry_attempt_values = new Float64Array(1);
  readonly retry_attempt_nulls = new Uint8Array(1);
  readonly retry_delay_ms_values = new Float64Array(1);
  readonly retry_delay_ms_nulls = new Uint8Array(1);
  readonly exception_stack_values: unknown[];
  readonly exception_stack_nulls = new Uint8Array(1);
  readonly ff_value_values: unknown[];
  readonly ff_value_nulls = new Uint8Array(1);
  readonly uint64_value_values = new BigUint64Array(1);
  readonly uint64_value_nulls = new Uint8Array(1);
  readonly thread_id: bigint;
  readonly _threadId: bigint;
  parent_span_id = 0;
  parent_thread_id = 0n;
  _hasParent = false;
  _spanName?: string | number;

  constructor(args: {
    runtime: ThreadSpanBufferRuntime;
    binding: ThreadSpanBufferBinding;
    schema: LogSchema;
    traceRoot: ITraceRoot;
    opMetadata: OpMetadata;
    callsiteMetadata: OpMetadata;
    parent?: AnySpanBuffer;
    stats: SpanBufferStats;
  }) {
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
    this.ordinals = schemaAttributeOrdinals(args.schema);
    const kinds = new Map<string, number>();
    const enums = new Map<string, readonly string[]>();
    for (const name of args.schema._columnNames) {
      if (isThreadSystemColumn(name)) continue;
      const type = getSchemaType(args.schema.fields[name]);
      if (type === undefined) continue;
      const kind = attributeKindForSchemaType(type);
      if (kind === undefined) continue;
      kinds.set(name, kind);
      if (type === 'enum') {
        const variants = getEnumValues(args.schema.fields[name]);
        if (variants) enums.set(name, variants);
      }
    }
    this.kinds = kinds;
    this.enumVariants = enums;

    this.message_values = laneProxy((index, value) => {
      this.commitLog(index, typeof value === 'string' ? value : String(value));
    }) as (string | undefined)[];
    this.error_code_values = laneProxy((index, value) => {
      this.writeNamed('error_code', this.physicalRow(index), value);
    });
    this.exception_stack_values = laneProxy((index, value) => {
      this.writeNamed('exception_stack', this.physicalRow(index), value);
    });
    this.ff_value_values = laneProxy((index, value) => {
      this.writeNamed('ff_value', this.physicalRow(index), value);
    });

    for (const name of this.ordinals.keys()) {
      Object.defineProperty(this, `${name}_values`, {
        value: laneProxy((index, value) => {
          this.writeNamed(name, this.physicalRow(index), value);
        }),
        writable: true,
        configurable: true,
        enumerable: false,
      });
      Object.defineProperty(this, `${name}_nulls`, {
        value: new Uint8Array(8192),
        writable: true,
        configurable: true,
        enumerable: false,
      });
      Object.defineProperty(this, name, {
        value: (pos: number, val: unknown) => {
          if (pos === 0) return this.writeTagNamed(name, val);
          if (pos === 1) return this.writeNamed(name, this.completionRow, val);
          return this.writeNamed(name, this.physicalRow(pos), val);
        },
        writable: true,
        configurable: true,
        enumerable: false,
      });
    }
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

  private physicalRow(index: number): number {
    return this.fakeToReal.get(index) ?? index;
  }

  private commitLog(fakeIndex: number, message: string): void {
    const existing = this.fakeToReal.get(fakeIndex);
    if (existing !== undefined) return;
    if (!this.opened) this.openSpan(this._spanName ?? 'span');
    const entryType = this.pendingEntryType ?? 8;
    this.pendingEntryType = undefined;
    const timestamp = this._traceRoot._timestampNow(this._traceRoot);
    const payload = this.runtime.writeUtf8(message);
    const packed = this.binding.appendLogDynamic(
      this.spanId,
      entryType,
      payload.ptr,
      payload.len,
      timestamp,
      this.pendingLine,
    );
    if (packed === 0n) throw new Error('thread_span_buffer_append_log_dynamic failed');
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

export function createThreadSpanView<T extends LogSchema>(args: {
  runtime: ThreadSpanBufferRuntime;
  binding: ThreadSpanBufferBinding;
  schema: T;
  traceRoot: ITraceRoot;
  opMetadata: OpMetadata;
  callsiteMetadata: OpMetadata;
  parent?: AnySpanBuffer;
  stats: SpanBufferStats;
}): SpanBuffer<T> {
  return new ThreadSpanView(args) as SpanBuffer<T> & ThreadSpanView;
}
