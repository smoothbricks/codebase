/**
 * Thin SpanBuffer facade over a ThreadSpanBufferBinding.
 *
 * Overflow lives inside the native row store. Scope is a side table (01i latest
 * value); this view never prefills future rows. Row lifecycle crosses the
 * binding; attribute values are TypedArray stores straight into the store's
 * own cells ({@link ThreadSpanCells}), so no value is copied between languages.
 */

import { Nanoseconds } from '@smoothbricks/arrow-builder';
import type { PhysicalAppenders } from '../lifecycleAppenders.js';
import type { RemapDescriptor } from '../logBinding.js';
import type { OpMetadata } from '../opContext/opTypes.js';
import type { LogSchema } from '../schema/LogSchema.js';
import { THREAD_ATTRIBUTE_KINDS } from '../schema/systemSchema.js';
import { getEnumValues, getSchemaType } from '../schema/typeGuards.js';
import type { SpanBufferStats } from '../spanBufferStats.js';
import type { ThreadSpanBufferProvider } from '../ThreadBufferStrategy.js';
import { getThreadId } from '../threadId.js';
import { createTraceId, type TraceId } from '../traceId.js';
import type { ITraceRoot, TimestampAppendPrimitive } from '../traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from '../types.js';
import { getVocabularyGeneration } from '../vocabularyRegistry.js';
import { attributeKindForSchemaType, schemaAttributeOrdinals, THREAD_SYSTEM_COLUMN_COUNT } from './schemaBlob.js';
import {
  attributeCellStride,
  NO_ROW,
  THREAD_SPAN_BUFFER_OK,
  type ThreadAttributeKind,
  type ThreadSpanBufferBinding,
} from './threadSpanBuffer.js';

// Registered, not unique: a process can hold two copies of this module (src
// and dist through a package boundary), and a view minted by one copy must
// still satisfy the other's guard. The brand is the contract, not the module.
export const THREAD_SPAN_VIEW = Symbol.for('@smoothbricks/lmao/thread-span-view');
const EMPTY_SCOPE: Readonly<Record<string, unknown>> = Object.freeze({});

const KIND_NUMBER = THREAD_ATTRIBUTE_KINDS[0].discriminant;
const KIND_UINT64 = THREAD_ATTRIBUTE_KINDS[1].discriminant;
const KIND_BOOLEAN = THREAD_ATTRIBUTE_KINDS[2].discriminant;
const KIND_TEXT = THREAD_ATTRIBUTE_KINDS[3].discriminant;

/**
 * `_laneStore` slots. System lanes take fixed slots; a schema attribute at
 * index `i` takes `LANE_SCHEMA_BASE + i` for its values lane.
 */
const LANE_MESSAGE = 0;
const LANE_ERROR_CODE = 1;
const LANE_EXCEPTION_STACK = 2;
const LANE_FF_VALUE = 3;
const LANE_MESSAGE_IDS = 4;
const LANE_SCHEMA_BASE = 5;

/** Null-lane sink size: covers any row index a single span can reach. */
const NULL_LANE_BYTES = 8192;

/**
 * The `${name}_nulls` sink every view shares. Validity lives in the row store's
 * cells; this lane exists only so generated loggers can keep their
 * unconditional `${name}_nulls[i >>> 3] |= …` store, and nothing reads it — so
 * one sink serves every span rather than each span allocating its own.
 */
const NULL_SINK = new Uint8Array(NULL_LANE_BYTES);

/**
 * Log rows between forced clock reads.
 *
 * `_timestampNow` is `process.hrtime.bigint()` plus bigint arithmetic, and it
 * is the largest deletable slice of the row path: freezing it moved a 32-row
 * span from 179-190 to 155-163 ns/row over six interleaved pairs, -27 ns/row
 * (14%), while ablating the two `writeNamed` Map lookups or `f64Bits`/
 * `encodeValue` moved nothing out of noise. Log rows therefore ride a cached
 * stamp and span boundaries read fresh.
 *
 * The invariant is `lmao-core`'s `CoarseClock`: rows stamped from the cache
 * share a timestamp, which is sound because row order — not stamp distinctness
 * — is authoritative for ordering, while span start and completion always read
 * fresh, so durations never coarsen. Sixteen is a quarter of the 64-row buffer,
 * the same ratio the Rust writer uses, so staleness stays bounded well inside
 * one buffer.
 */
const LOG_STAMP_REFRESH = 16;

/** The store has not been asked yet: a span opens on its first write. */
const SPAN_UNOPENED = 0;
/** The store opened the span; its rows and its span id are the store's. */
const SPAN_OPEN = 1;
/**
 * The store refused the span, or refused the span it would hang from. It holds
 * no rows and no span id, so it writes nothing for the rest of its life: no
 * cell, and no lifecycle call that would name a span the store has no record of.
 */
const SPAN_REFUSED = 2;
/**
 * The store released the span's rows — a flush after it ended, or a reset —
 * so it holds no record of it any more and the span writes nothing further,
 * exactly as a refused one: its rows were emitted, and the rows it read before
 * are another span's now.
 */
const SPAN_RELEASED = 3;

/** Where a view's span stands with its row store. */
export type ThreadSpanState = typeof SPAN_UNOPENED | typeof SPAN_OPEN | typeof SPAN_REFUSED | typeof SPAN_RELEASED;

const bits = new DataView(new ArrayBuffer(8));

function f64Bits(value: number): bigint {
  bits.setFloat64(0, value, true);
  return bits.getBigUint64(0, true);
}

/** Typed views over one block's attribute cells — four names for the same bytes. */
interface CellViews {
  readonly bytes: Uint8Array;
  readonly f64: Float64Array;
  readonly u64: BigUint64Array;
  readonly u32: Uint32Array;
}

/**
 * Stores attribute values straight into a row store's cells.
 *
 * One per binding, created with it, so no span pays for views. The layout is
 * `lmao-core`'s `AttributeCells`: per field, `capacity` little-endian `u64`
 * value words followed by the validity bitmap. A value store plus one validity
 * bit is the whole write — no call into the store, no second copy of the value.
 * The store reads the same words when it converts to Arrow.
 *
 * It is every view's handle on that one store, so it also keeps the store's
 * one count a view writes: the spans the store refused to open.
 */
export class ThreadSpanCells {
  readonly binding: ThreadSpanBufferBinding;
  /**
   * Spans this store refused to open. Each wrote nothing, and nothing under it
   * was offered to the store, so one count is one subtree missing from it.
   * A host reads the sum through `ThreadBufferStrategy.refusedSpans`.
   */
  refusedSpans = 0;
  private readonly capacity: number;
  private readonly stride: number;
  private readonly blocks: (CellViews | undefined)[] = [];

  constructor(binding: ThreadSpanBufferBinding) {
    this.binding = binding;
    this.capacity = binding.capacity;
    this.stride = attributeCellStride(binding.capacity);
  }

  /**
   * The views over the block holding `row`, derived on first touch and again
   * only when a Wasm memory growth detached them.
   */
  views(row: number): CellViews {
    const block = Math.floor(row / this.capacity);
    const cached = this.blocks[block];
    if (cached !== undefined && cached.bytes.byteLength !== 0) return cached;
    const bytes = this.binding.attributeCells(block);
    // invariant throw: a row the store issued always lives in a block it allocated.
    if (bytes === undefined) throw new Error(`thread span buffer has no attribute cells for block ${block}`);
    const words = bytes.byteLength / 8;
    const views: CellViews = {
      bytes,
      f64: new Float64Array(bytes.buffer, bytes.byteOffset, words),
      u64: new BigUint64Array(bytes.buffer, bytes.byteOffset, words),
      u32: new Uint32Array(bytes.buffer, bytes.byteOffset, words * 2),
    };
    this.blocks[block] = views;
    return views;
  }

  /** Word index of `field`'s value cell for `row`. */
  word(field: number, row: number): number {
    return field * this.stride + (row % this.capacity);
  }

  /** Mark `field` valid at `row` in the block `views` covers. */
  valid(views: CellViews, field: number, row: number): void {
    const local = row % this.capacity;
    views.bytes[(field * this.stride + this.capacity) * 8 + (local >>> 3)] |= 1 << (local & 7);
  }

  storeNumber(field: number, row: number, value: number): void {
    const views = this.views(row);
    views.f64[this.word(field, row)] = value;
    this.valid(views, field, row);
  }

  storeUint64(field: number, row: number, value: bigint): void {
    const views = this.views(row);
    views.u64[this.word(field, row)] = value;
    this.valid(views, field, row);
  }

  /** Boolean, intern-ordinal and enum-index cells: the low half of the word. */
  storeUint32(field: number, row: number, value: number): void {
    const views = this.views(row);
    views.u32[this.word(field, row) * 2] = value;
    this.valid(views, field, row);
  }
}

/**
 * A write-only indexable sink that forwards `lane[i] = v` into the row store.
 *
 * The target array is never populated: the native store is the only reader of
 * these values, so mirroring them into JS would allocate a second copy of
 * every row for nobody. `length` is a high-water mark held in a closure rather
 * than on the target — storing it on the array reshapes indexed storage on
 * every write and cost 39 ns of a 228 ns row, more than the ABI crossing it
 * accompanies, for a number no reader of this lane consults.
 *
 * A Proxy looks like the expensive way to observe an indexed store, and the
 * profiler agrees it is visible (12.8% of thread-lane self time: this trap
 * plus `performProxyObjectSetByValStrict`). It is nonetheless the cheapest
 * mechanism available here. Measured per store, M5 Max / bun 1.4.0, 128-row
 * spans, best-of-five:
 *
 * | shape                                    | ns/store |
 * | ---------------------------------------- | -------- |
 * | this Proxy trap                          |     10.8 |
 * | index accessors (`String(i)` setters)    |  18.2–22 |
 * | plain method call                        |      2.7 |
 * | real array / TypedArray store            |      0.4 |
 *
 * Only two mechanisms in JS can observe `obj[i] = v` at all — a Proxy, or an
 * accessor named `String(i)` — and JSC's sparse-accessor `putByIndex` slow
 * path is worse than its proxy set-by-val path, in every accessor variant
 * tried (prototype table x256 and x4096, own accessors, sealed instances). So
 * a "flat class with real setters" is a measured REGRESSION of ~6.5 ns per
 * store, ~13 ns/row at two lane stores per row; do not re-attempt it.
 *
 * The exit is not a better observer but no observer. Schema attributes already
 * take it: their writer methods store into the row store's cells through
 * {@link ThreadSpanCells}. These lanes remain only where generated code indexes
 * by the span-local write index, which is not the store's row.
 */
function laneProxy<A extends object>(target: A, write: (index: number, value: unknown) => void): A {
  let highWater = 0;
  return new Proxy(target, {
    get(obj, prop, receiver) {
      if (prop === 'length') return highWater;
      return Reflect.get(obj, prop, receiver);
    },
    set(obj, prop, value) {
      if (prop === 'length') {
        highWater = Number(value);
        return true;
      }
      const index = typeof prop === 'string' ? Number(prop) : Number.NaN;
      if (Number.isInteger(index) && index >= 0) {
        write(index, value);
        if (index >= highWater) highWater = index + 1;
        return true;
      }
      Reflect.set(obj, prop, value);
      return true;
    },
  });
}

export interface ThreadSpanViewArgs<T extends LogSchema = LogSchema> {
  /** How the lane reaches this span's store: the view converts through it. */
  provider: ThreadSpanBufferProvider;
  cells: ThreadSpanCells;
  schema: T;
  traceRoot: ITraceRoot;
  opMetadata: OpMetadata;
  callsiteMetadata: OpMetadata;
  parent?: AnySpanBuffer;
  stats: SpanBufferStats;
}

export class ThreadSpanView {
  readonly [THREAD_SPAN_VIEW] = true;
  readonly provider: ThreadSpanBufferProvider;
  readonly cells: ThreadSpanCells;
  readonly binding: ThreadSpanBufferBinding;
  readonly layout: ThreadSpanLayout;
  readonly ordinals: ReadonlyMap<string, number>;
  readonly fields: ReadonlyMap<string, ThreadAttributeField>;

  spanId = 0;
  startRow = NO_ROW;
  completionRow = NO_ROW;
  lastRow = NO_ROW;
  pendingLine = 0;
  pendingEntryType: number | undefined;
  state: ThreadSpanState = SPAN_UNOPENED;
  readonly fakeToReal = new Map<number, number>();
  /**
   * The store's {@link ThreadSpanBufferBinding.rowGeneration} the rows above
   * were read at. A flush moves an open span's rows and a reset releases them,
   * so rows read at an earlier generation are re-read before any write
   * ({@link currentRows}).
   */
  rowGeneration = 0;

  /**
   * Row-stamp cache: the value log rows ride and the reads left before a
   * refresh. Seeded by every boundary read, so a span's first log rows reuse
   * the fresh stamp `openSpan` already paid for.
   */
  _stampCache = 0n;
  _stampReads = 0;

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
  readonly _columns: ReadonlyArray<readonly [string, unknown]>;
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
  declare readonly line_nulls: Uint8Array;
  declare readonly error_code_nulls: Uint8Array;
  readonly retry_attempt_values = new Float64Array(1);
  declare readonly retry_attempt_nulls: Uint8Array;
  readonly retry_delay_ms_values = new Float64Array(1);
  declare readonly retry_delay_ms_nulls: Uint8Array;
  declare readonly exception_stack_nulls: Uint8Array;
  declare readonly ff_value_nulls: Uint8Array;
  readonly uint64_value_values = new BigUint64Array(1);
  declare readonly uint64_value_nulls: Uint8Array;
  readonly thread_id: bigint;
  readonly _threadId: bigint;
  parent_span_id = 0;
  parent_thread_id = 0n;
  _hasParent = false;
  _spanName?: string | number;

  /** Binding-backed lifecycle writers, installed once on this prototype below. */
  declare readonly _appenders: PhysicalAppenders;
  /** Binding-backed log-append primitive; the traceRoot operand is unused on this lane. */
  declare readonly _appendLogEntry: TimestampAppendPrimitive;

  get message_values(): (string | undefined)[] {
    const existing = this._laneStore[LANE_MESSAGE];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- heterogeneous lane store; this slot is only ever written by this getter.
    if (existing !== undefined) return existing as (string | undefined)[];
    const lane = laneProxy<(string | undefined)[]>([], (index, value) => {
      this.commitLog(index, typeof value === 'string' ? value : String(value));
    });
    this._laneStore[LANE_MESSAGE] = lane;
    return lane;
  }

  /**
   * Static-vocabulary message lane.
   *
   * Generated loggers resolve a compile-time template to a local u16 id and
   * store it here instead of a string. Without this lane the write landed on
   * `undefined` and threw, so the lane's best case — a message that never
   * needs encoding — was the one path it could not take.
   *
   * The id indexes the callsite's local dictionary, whose entries are dense
   * vocabulary indices, and the row store speaks that vocabulary directly.
   * So the dense index crosses as an integer: no decode to a string, no
   * intern, no scratch page, nothing for the boundary to copy.
   *
   * Typed as Uint16Array to satisfy the SpanBuffer contract: this lane is a
   * write sink with indexed-set semantics, not storage, and nothing reads it.
   */
  get _messageIds(): Uint16Array {
    const existing = this._laneStore[LANE_MESSAGE_IDS];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- heterogeneous lane store; this slot is only ever written by this getter.
    if (existing !== undefined) return existing as Uint16Array;
    const lane = laneProxy(new Uint16Array(0), (index, value) => {
      this.commitStaticLog(index, this.vocabularyIdFor(Number(value)));
    });
    this._laneStore[LANE_MESSAGE_IDS] = lane;
    return lane;
  }

  /**
   * Wire form of a callsite's local message id.
   *
   * `VocabularyId` is 1..=0x00ffffff, because 0 in a packed header means "this
   * row's message is dynamic" — which is why readers decode with
   * `encodedDenseIndex - 1`. Dense indices are 0-based, so the wire value is
   * one more than the dictionary entry.
   */
  private vocabularyIdFor(localMessageId: number): number {
    const denseIndex = this._opMetadata._physicalLayoutPlan?.localMessageDictionary?.[localMessageId - 1];
    if (denseIndex === undefined) {
      throw new Error(`Missing local message dictionary entry ${localMessageId}`);
    }
    return denseIndex + 1;
  }

  private commitStaticLog(fakeIndex: number, vocabularyId: number): void {
    if (this.fakeToReal.get(fakeIndex) !== undefined) return;
    if (!this.writable()) return;
    const entryType = this.pendingEntryType ?? 8;
    this.pendingEntryType = undefined;
    const timestamp = this.logTimestamp();
    const packed = this.binding.appendLogStatic(this.spanId, entryType, vocabularyId, timestamp, this.pendingLine);
    if (packed === 0n) {
      throw new Error(
        `thread_span_buffer_append_log_static rejected vocabulary id ${vocabularyId} for entry type ${entryType}`,
      );
    }
    const row = Number(packed & 0xffffffffn);
    this.fakeToReal.set(fakeIndex, row);
    this.lastRow = row;
  }

  get error_code_values(): string[] {
    const existing = this._laneStore[LANE_ERROR_CODE];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- heterogeneous lane store; this slot is only ever written by this getter.
    if (existing !== undefined) return existing as string[];
    const lane = laneProxy<string[]>([], (index, value) => {
      this.writeNamed('error_code', this.physicalRow(index), value);
    });
    this._laneStore[LANE_ERROR_CODE] = lane;
    return lane;
  }

  get exception_stack_values(): string[] {
    const existing = this._laneStore[LANE_EXCEPTION_STACK];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- heterogeneous lane store; this slot is only ever written by this getter.
    if (existing !== undefined) return existing as string[];
    const lane = laneProxy<string[]>([], (index, value) => {
      this.writeNamed('exception_stack', this.physicalRow(index), value);
    });
    this._laneStore[LANE_EXCEPTION_STACK] = lane;
    return lane;
  }

  get ff_value_values(): string[] {
    const existing = this._laneStore[LANE_FF_VALUE];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- heterogeneous lane store; this slot is only ever written by this getter.
    if (existing !== undefined) return existing as string[];
    const lane = laneProxy<string[]>([], (index, value) => {
      this.writeNamed('ff_value', this.physicalRow(index), value);
    });
    this._laneStore[LANE_FF_VALUE] = lane;
    return lane;
  }

  constructor(args: ThreadSpanViewArgs) {
    this.provider = args.provider;
    this.cells = args.cells;
    this.binding = args.cells.binding;
    this._logSchema = args.schema;
    this._columns = args.schema._columns;
    this._traceRoot = args.traceRoot;
    this._opMetadata = args.opMetadata;
    this._callsiteMetadata = args.callsiteMetadata;
    this._parent = args.parent;
    this._stats = args.stats;
    this.thread_id = getThreadId();
    this._threadId = this.thread_id;
    // A view with no local parent is its trace's root, which hangs from the trace's remote parent when the trace
    // continues one; the store records whichever parent this names.
    const parent = args.parent ?? args.traceRoot.remoteParent;
    this._hasParent = parent !== undefined;
    if (parent !== undefined) {
      this.parent_span_id = parent.span_id;
      this.parent_thread_id = parent.thread_id;
    }
    const layout = threadSpanLayoutFor(args.schema);
    this.layout = layout;
    this.ordinals = layout.ordinals;
    this.fields = layout.fields;
    // Attribute lanes and writer methods live on `layout.ViewClass.prototype`;
    // the constructor deliberately adds nothing beyond this class's declared
    // fields, so every span of a schema shares one hidden class.
  }

  get span_id(): number {
    return this.spanId;
  }

  get trace_id(): TraceId {
    // Cold accessor (flush/assertions): re-validating the root's string here is
    // cheaper than carrying a second branded copy on every view.
    return createTraceId(this._traceRoot.trace_id);
  }

  get _spanStartTime(): Nanoseconds {
    return Nanoseconds.unsafe(this.timestamp[0] ?? 0n);
  }

  get _lastLoggedTime(): Nanoseconds | null {
    return this.timestamp[0] === 0n ? null : Nanoseconds.unsafe(this.timestamp[0]);
  }

  /**
   * Stamp for a span boundary: always a fresh read, and it seeds the row cache
   * so the log rows that follow reuse it. Durations derive from these two
   * reads, so they never see a cached value.
   */
  boundaryTimestamp(): bigint {
    const timestamp = this._traceRoot._timestampNow(this._traceRoot);
    this._stampCache = timestamp;
    this._stampReads = LOG_STAMP_REFRESH;
    return timestamp;
  }

  /** Stamp for a log row: the cached value, refreshed every {@link LOG_STAMP_REFRESH} rows. */
  logTimestamp(): bigint {
    const reads = this._stampReads;
    if (reads === 0) return this.boundaryTimestamp();
    this._stampReads = reads - 1;
    return this._stampCache;
  }

  beginLog(entryType: number): number {
    if (this.state === SPAN_UNOPENED) this.openSpan(this._spanName ?? 'span');
    this.pendingEntryType = entryType;
    const fake = this._writeIndex;
    this._writeIndex = fake + 1;
    return fake;
  }

  /**
   * Open the span if nothing has yet, and answer whether it holds rows to
   * write: false once its store refused it or released its rows. An open
   * span's rows are the store's current ones when this answers true.
   */
  writable(): boolean {
    if (this.state === SPAN_UNOPENED) this.openSpan(this._spanName ?? 'span');
    else this.currentRows();
    return this.state === SPAN_OPEN;
  }

  /**
   * Re-read this span's rows when the store moved them since they were read.
   * A host flush keeps an open span by moving its start and completion rows to
   * the front of the store, and a reset releases every row, so a row read
   * before either names another span's row after it. The lifecycle pair is read
   * again from the store; every log row the span had was emitted and released
   * by that flush, so what still names one writes nothing; and a span whose
   * rows the store released writes nothing further.
   */
  currentRows(): void {
    if (this.state !== SPAN_OPEN) return;
    const generation = this.binding.rowGeneration;
    if (generation === this.rowGeneration) return;
    this.rowGeneration = generation;
    const start = this.binding.spanStartRow(this.spanId);
    // Before any log row, the last row is the start row, and moves with it.
    this.lastRow = this.lastRow === this.startRow ? start : NO_ROW;
    this.startRow = start;
    this.completionRow = start === NO_ROW ? NO_ROW : start + 1;
    for (const index of this.fakeToReal.keys()) this.fakeToReal.set(index, NO_ROW);
    if (start === NO_ROW) this.state = SPAN_RELEASED;
  }

  openSpan(name: string | number): void {
    if (this.state !== SPAN_UNOPENED) return;
    // Writer indices 0 and 1 are the lifecycle pair whether or not the store
    // opens the span, so a refused span's log rows never alias them.
    this._writeIndex = 2;
    // A child of a refused span has no span to hang from. Offered with parent
    // 0 it would open as a root, which a host store parents on whatever it
    // roots unattributed spans on — so it is not offered at all.
    const parent = this._parent;
    const parentState = parent === undefined ? undefined : requireThreadSpanView(parent).state;
    if (parentState === SPAN_REFUSED || parentState === SPAN_RELEASED) {
      this.state = SPAN_REFUSED;
      return;
    }
    const timestamp = this.boundaryTimestamp();
    const label = typeof name === 'string' ? name : String(name);
    const nameId = this.binding.intern(label);
    const packed = this.binding.openSpan(
      this._traceRoot.trace_id,
      this.parent_thread_id,
      this.parent_span_id,
      nameId,
      timestamp,
      this.pendingLine,
    );
    if (packed === 0n) {
      // A refused open is the store's operational answer — a host that cannot
      // place the span, a store that is full — not a bug in the code being
      // traced, and a trace must not fail what it traces. So the span writes
      // nothing, the body runs on, and the store's refusal is counted where a
      // host reads it.
      this.state = SPAN_REFUSED;
      this.cells.refusedSpans += 1;
      return;
    }
    this.spanId = Number(packed >> 32n);
    this.startRow = Number(packed & 0xffffffffn);
    this.completionRow = this.startRow + 1;
    this.lastRow = this.startRow;
    this.rowGeneration = this.binding.rowGeneration;
    this.timestamp[0] = timestamp;
    this.entry_type[0] = 1;
    this.line_values[0] = this.pendingLine;
    this._spanName = name;
    this.state = SPAN_OPEN;
    this._stats.spansCreated += 1;
  }

  end(entryType: number): void {
    if (!this.writable()) return;
    const timestamp = this.boundaryTimestamp();
    // The tracer's entry type goes through verbatim. Folding EXCEPTION onto
    // the error path recorded a thrown bug as a handled failure, which is the
    // one distinction the completion taxonomy exists to make.
    const status = this.binding.end(this.spanId, entryType, timestamp);
    if (status !== THREAD_SPAN_BUFFER_OK) throw new Error('thread_span_buffer_end failed');
    this.timestamp[1] = timestamp;
    this.entry_type[1] = entryType;
  }

  writeNamed(name: string, row: number, value: unknown): this {
    const field = this.fields.get(name);
    if (field === undefined || value === null || value === undefined) return this;
    this.storeCell(field, row, value);
    return this;
  }

  writeTagNamed(name: string, value: unknown): this {
    const field = this.fields.get(name);
    if (field === undefined || value === null || value === undefined) return this;
    // A tag can be the span's first write (ctx.tag before any log). The row
    // store has no start row for a span it has not opened, so opening here
    // mirrors commitLog's lazy open rather than making order significant.
    if (!this.writable()) return this;
    this.storeCell(field, this.startRow, value);
    return this;
  }

  /**
   * Store one attribute value into the row store's cell for `row`: a TypedArray
   * store and a validity bit, nothing crossing the binding except a warm-miss
   * intern of a text value. A span that holds no rows stores nothing.
   */
  storeCell(field: ThreadAttributeField, row: number, value: unknown): void {
    if (row === NO_ROW) return;
    switch (field.kind) {
      case KIND_NUMBER:
        if (typeof value !== 'number') throw new TypeError(`${field.name} expects number`);
        this.cells.storeNumber(field.index, row, value);
        return;
      case KIND_UINT64:
        if (typeof value !== 'bigint') throw new TypeError(`${field.name} expects bigint`);
        this.cells.storeUint64(field.index, row, value);
        return;
      case KIND_BOOLEAN:
        if (typeof value !== 'boolean') throw new TypeError(`${field.name} expects boolean`);
        this.cells.storeUint32(field.index, row, value ? 1 : 0);
        return;
      default:
        this.cells.storeUint32(field.index, row, this.smallCell(field, value));
    }
  }

  syncScope(attributes: object): void {
    const next: Record<string, unknown> = { ...this._scopeValues };
    // Only a span the store opened has a scope there to set.
    this.currentRows();
    const scoped = this.state === SPAN_OPEN;
    for (const key of Object.keys(attributes)) {
      const value = Reflect.get(attributes, key);
      if (value === null) delete next[key];
      else if (value !== undefined) next[key] = value;
      const field = this.fields.get(key);
      if (!scoped || field === undefined || value === undefined) continue;
      if (value === null) {
        this.binding.setScope(this.spanId, field.ordinal, 0, 0n);
        continue;
      }
      this.binding.setScope(this.spanId, field.ordinal, field.kind, this.encodeValue(field, value));
    }
    this._scopeValues = Object.freeze(next);
  }

  line(pos: number, val: number): this {
    if (this.state === SPAN_UNOPENED) this.pendingLine = val;
    if (pos === 0) this.line_values[0] = val;
    return this;
  }

  message(pos: number, val: string): this {
    if (pos === 0 && this.state === SPAN_UNOPENED) {
      this._spanName = val;
      return this;
    }
    if (pos === 1) {
      this.complete(val);
      return this;
    }
    this.message_values[pos] = val;
    return this;
  }

  // These name a lifecycle row or the last log row directly, so each reads the
  // store's current rows first ({@link currentRows}).
  error_code(_pos: number, val: string): this {
    this.currentRows();
    return this.writeNamed('error_code', this.completionRow, val);
  }

  retry_attempt(_pos: number, val: number): this {
    this.currentRows();
    return this.writeNamed('retry_attempt', this.lastRow, val);
  }

  retry_delay_ms(_pos: number, val: number): this {
    this.currentRows();
    return this.writeNamed('retry_delay_ms', this.lastRow, val);
  }

  exception_stack(_pos: number, val: string): this {
    this.currentRows();
    return this.writeNamed('exception_stack', this.completionRow, val);
  }

  ff_value(_pos: number, val: string): this {
    this.currentRows();
    return this.writeNamed('ff_value', this.lastRow, val);
  }

  uint64_value(_pos: number, val: bigint): this {
    this.currentRows();
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

  /**
   * Attribute storage lives in the native row store; nothing is allocated on
   * the JS heap, so the JS Arrow path sees no columns. The thread lane's
   * conversion is the native `lmao_arrow` flush, never `convertToArrowTable`.
   */
  getColumnIfAllocated(_name: string): undefined {
    return undefined;
  }

  getNullsIfAllocated(_name: string): undefined {
    return undefined;
  }

  copyThreadIdTo(dest: Uint8Array, offset: number): void {
    let bits = this.thread_id;
    for (let i = 0; i < 8; i++) {
      dest[offset + i] = Number(bits & 0xffn);
      bits >>= 8n;
    }
  }

  copyParentThreadIdTo(dest: Uint8Array, offset: number): void {
    let bits = this.parent_thread_id;
    for (let i = 0; i < 8; i++) {
      dest[offset + i] = Number(bits & 0xffn);
      bits >>= 8n;
    }
  }

  isParentOf(other: AnySpanBuffer): boolean {
    return other._parent === this;
  }

  isChildOf(other: AnySpanBuffer): boolean {
    return this._parent === other;
  }

  physicalRow(index: number): number {
    this.currentRows();
    // Fakes 0/1 are the lifecycle pair; they are structural, never entries in
    // the log-row map, which tests and stamp accounting read as logs-only.
    if (index === 0) return this.startRow;
    if (index === 1) return this.completionRow;
    const row = this.fakeToReal.get(index);
    if (row !== undefined) return row;
    // A refused span appended no rows, so it mapped none, and a released one
    // appends none: what their writers store for one lands on NO_ROW, which
    // stores nothing.
    if (this.state === SPAN_REFUSED || this.state === SPAN_RELEASED) return NO_ROW;
    // invariant throw: generated writers store a row's message before its
    // attributes, so an unmapped index is a writer bug — and guessing a row
    // would write another span's cell.
    throw new Error(`log row ${index} has no row in the thread store yet`);
  }

  /**
   * The span's terminal result or error text. It lands on the reserved
   * completion row — the js-heap lane's row-1 contract — never as an appended
   * row, or the two lanes disagree on row count for the same trace.
   */
  private complete(text: string): void {
    if (!this.writable()) return;
    const status = this.binding.setCompletionMessage(this.spanId, text);
    if (status !== THREAD_SPAN_BUFFER_OK) throw new Error('thread_span_buffer_set_completion_message failed');
  }

  private commitLog(fakeIndex: number, message: string): void {
    if (fakeIndex === 1) {
      this.complete(message);
      return;
    }
    if (fakeIndex === 0) {
      // Row 0's message is the span name, written at open.
      if (this.state === SPAN_UNOPENED) this._spanName = message;
      return;
    }
    const existing = this.fakeToReal.get(fakeIndex);
    if (existing !== undefined) return;
    if (!this.writable()) return;
    const entryType = this.pendingEntryType ?? 8;
    this.pendingEntryType = undefined;
    const timestamp = this.logTimestamp();
    // Intern to a u32 and pass the ordinal, rather than re-encoding the same
    // message to UTF-8 on every row. Vocabulary ids are stable per store, so
    // a repeated message costs one Map lookup and no encode at all.
    const messageOrdinal = this.binding.intern(message);
    const packed = this.binding.appendLog(this.spanId, entryType, messageOrdinal, timestamp, this.pendingLine);
    if (packed === 0n) throw new Error('thread_span_buffer_append_log failed');
    const row = Number(packed & 0xffffffffn);
    this.fakeToReal.set(fakeIndex, row);
    this.lastRow = row;
  }

  /** A set_scope value: the same encodings the cells hold, as the ABI's `u64`. */
  private encodeValue(field: ThreadAttributeField, value: unknown): bigint {
    switch (field.kind) {
      case KIND_NUMBER:
        if (typeof value !== 'number') throw new TypeError(`${field.name} expects number`);
        return f64Bits(value);
      case KIND_UINT64:
        if (typeof value !== 'bigint') throw new TypeError(`${field.name} expects bigint`);
        return value;
      case KIND_BOOLEAN:
        if (typeof value !== 'boolean') throw new TypeError(`${field.name} expects boolean`);
        return value ? 1n : 0n;
      default:
        return BigInt(this.smallCell(field, value));
    }
  }

  /** The `u32` a text or enum cell holds: an intern ordinal or a variant index. */
  private smallCell(field: ThreadAttributeField, value: unknown): number {
    if (field.kind === KIND_TEXT) {
      if (typeof value !== 'string') throw new TypeError(`${field.name} expects string`);
      const ordinal = this.binding.intern(value);
      if (ordinal === 0) throw new Error(`thread span buffer refused to intern a value of ${field.name}`);
      return ordinal;
    }
    const index = typeof value === 'number' ? value : field.variants.get(String(value));
    if (index === undefined || !Number.isInteger(index) || index < 0 || index >= field.variants.size) {
      throw new TypeError(`${field.name} has no variant ${String(value)}`);
    }
    return index;
  }
}

export function isThreadSpanView(value: unknown): value is ThreadSpanView {
  return typeof value === 'object' && value !== null && THREAD_SPAN_VIEW in value;
}

/**
 * The thread lane's lifecycle writers cross the row-store binding directly;
 * message layout does not apply because the store owns the row format.
 */
const THREAD_BUFFER_APPENDERS: PhysicalAppenders = Object.freeze({
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    requireThreadSpanView(buffer).openSpan(name);
  },
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    requireThreadSpanView(buffer).end(entryType);
  },
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    return requireThreadSpanView(buffer).beginLog(entryType);
  },
});

const THREAD_APPEND_LOG_ENTRY: TimestampAppendPrimitive = (_traceRoot, buffer, entryType) =>
  requireThreadSpanView(buffer).beginLog(entryType);

// Validity lives in the row store's cells, so every `*_nulls` lane is the one
// shared write-only sink: generated loggers keep their unconditional
// `…_nulls[i >>> 3] |= …` store, and no span allocates a lane nobody reads.
const nullSink = { value: NULL_SINK, writable: false, configurable: true, enumerable: false };
Object.defineProperties(ThreadSpanView.prototype, {
  _appenders: { value: THREAD_BUFFER_APPENDERS },
  _appendLogEntry: { value: THREAD_APPEND_LOG_ENTRY },
  line_nulls: nullSink,
  error_code_nulls: nullSink,
  retry_attempt_nulls: nullSink,
  retry_delay_ms_nulls: nullSink,
  exception_stack_nulls: nullSink,
  ff_value_nulls: nullSink,
  uint64_value_nulls: nullSink,
});

export function requireThreadSpanView(value: AnySpanBuffer): ThreadSpanView {
  if (!isThreadSpanView(value)) throw new TypeError('expected ThreadSpanView');
  return value;
}

/** One schema attribute as the thread lane writes it. */
export interface ThreadAttributeField {
  readonly name: string;
  /** Position among the schema's attributes: which field's cells hold it. */
  readonly index: number;
  /** The store's column ordinal (`SYSTEM_COLUMN_COUNT + index`). */
  readonly ordinal: number;
  readonly kind: ThreadAttributeKind;
  /** Enum variant → index; empty for every other kind. */
  readonly variants: ReadonlyMap<string, number>;
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
  readonly fields: ReadonlyMap<string, ThreadAttributeField>;
  readonly ViewClass: new (args: ThreadSpanViewArgs) => ThreadSpanView;
}

const layouts = new WeakMap<LogSchema, ThreadSpanLayout>();

const NO_VARIANTS: ReadonlyMap<string, number> = new Map();

function buildLayout(schema: LogSchema): ThreadSpanLayout {
  const ordinals = schemaAttributeOrdinals(schema);
  const fields = new Map<string, ThreadAttributeField>();
  for (const [name, ordinal] of ordinals) {
    const type = getSchemaType(schema.fields[name]);
    const kind = type === undefined ? undefined : attributeKindForSchemaType(type);
    if (kind === undefined) continue;
    const enumValues = type === 'enum' ? getEnumValues(schema.fields[name]) : undefined;
    fields.set(name, {
      name,
      index: ordinal - THREAD_SYSTEM_COLUMN_COUNT,
      ordinal,
      kind,
      variants: enumValues === undefined ? NO_VARIANTS : new Map(enumValues.map((variant, index) => [variant, index])),
    });
  }

  // One subclass per schema carries the attribute writer methods and the
  // attribute lanes on its prototype. Doing this per span was the lane's fixed
  // floor: three Object.defineProperty calls and two allocations per attribute
  // on an instance that already has forty declared fields.
  class SchemaBoundThreadSpanView extends ThreadSpanView {}
  const descriptors: PropertyDescriptorMap = {};
  for (const field of fields.values()) {
    const valuesSlot = LANE_SCHEMA_BASE + field.index;
    descriptors[field.name] = {
      value: function attributeWriter(this: ThreadSpanView, pos: number, val: unknown): ThreadSpanView {
        if (val === null || val === undefined) return this;
        // Any attribute can be a span's first write; its rows exist only once
        // the store opened it, and never if the store refused it.
        if (!this.writable()) return this;
        this.storeCell(field, this.physicalRow(pos), val);
        return this;
      },
      writable: true,
      configurable: true,
      enumerable: false,
    };
    descriptors[`${field.name}_values`] = {
      get: function attributeLane(this: ThreadSpanView): unknown[] {
        const existing = this._laneStore[valuesSlot];
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- heterogeneous lane store; this slot is only ever written by this getter.
        if (existing !== undefined) return existing as unknown[];
        const lane = laneProxy<unknown[]>([], (rowIndex, value) => {
          if (value !== null && value !== undefined) this.storeCell(field, this.physicalRow(rowIndex), value);
        });
        this._laneStore[valuesSlot] = lane;
        return lane;
      },
      configurable: true,
      enumerable: false,
    };
    descriptors[`${field.name}_nulls`] = nullSink;
  }
  Object.defineProperties(SchemaBoundThreadSpanView.prototype, descriptors);

  const layout: ThreadSpanLayout = { ordinals, fields, ViewClass: SchemaBoundThreadSpanView };
  layouts.set(schema, layout);
  return layout;
}

export function threadSpanLayoutFor(schema: LogSchema): ThreadSpanLayout {
  return layouts.get(schema) ?? buildLayout(schema);
}

export function createThreadSpanView<T extends LogSchema>(args: ThreadSpanViewArgs<T>): SpanBuffer<T> {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- schema-bound ViewClass carries its generated lanes; the static type cannot name them per schema.
  return new (threadSpanLayoutFor(args.schema).ViewClass)(args) as SpanBuffer<T> & ThreadSpanView;
}
