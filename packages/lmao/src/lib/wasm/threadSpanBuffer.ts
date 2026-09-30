/**
 * The shared per-thread span buffer, as JavaScript sees it.
 *
 * A thread lane writes rows into a native row store (`lmao-core`'s
 * `ThreadSpanBuffer`) that it reaches through a {@link ThreadSpanBufferBinding}.
 * The binding carries the row LIFECYCLE — open, append, end, scope, intern —
 * as scalar calls, because the store owns row allocation, span identity and
 * (on a host-stamped lane) the clock. Attribute VALUES do not cross as calls at
 * all: {@link ThreadSpanBufferBinding.attributeCells} hands out the bytes of a
 * block's attribute cells, and the lane stores into them through TypedArray
 * views. There is one copy of every value, and both languages name it.
 *
 * Providers differ only in how they reach the store: the Wasm provider below
 * talks to `allocator.wasm` through a numeric slot token and views linear
 * memory; a native provider (Bun FFI, or a host runtime that embeds the row
 * store) views memory the store allocated. None of them hands JavaScript a row
 * store pointer or lets it write identity or entry types.
 */

import { isRecord } from '@smoothbricks/validation';
import { decodeVocabularyMessage } from '../resolveMessage.js';
import { THREAD_ATTRIBUTE_KINDS } from '../schema/systemSchema.js';
import { getVocabularyGeneration } from '../vocabularyRegistry.js';

export { THREAD_ATTRIBUTE_KINDS };
export type ThreadAttributeKind = (typeof THREAD_ATTRIBUTE_KINDS)[number]['discriminant'];

/** Numeric token returned by `thread_span_buffer_new`; zero is never a handle. */
export type ThreadSpanBufferHandle = number;

/** Successful status returned by fallible row-write exports. */
export const THREAD_SPAN_BUFFER_OK = 0;

/**
 * `u64` words one attribute field occupies in a block of `capacity` rows: the
 * value cells plus the validity bitmap (`lmao-core` `attribute_cells::stride`).
 */
export function attributeCellStride(capacity: number): number {
  return capacity + Math.ceil(capacity / 64);
}

/**
 * The text of a static vocabulary id as the thread lane hands it to
 * `openSpanStatic` / `appendLogStatic`: one-based, `denseIndex + 1`, because 0
 * in a packed header means "dynamic". A provider whose store holds no copy of
 * this process's vocabulary resolves the id here and interns the text instead.
 */
/**
 * A binding's text-ordinal cache, exact within the store's text epoch.
 *
 * `intern` answers a warm string from the cache and crosses nothing; a miss
 * pays one crossing, once per distinct string per epoch. `revalidate` — called
 * after every reset or retain, the only calls that can reclaim — reads the
 * store's epoch and drops every cached ordinal when the arena was renumbered.
 */
export interface InternCache {
  intern(text: string): number;
  revalidate(): void;
}

export function internCache(internText: (text: string) => number, textEpoch: () => number): InternCache {
  const ordinals = new Map<string, number>();
  let epoch = textEpoch();
  return {
    intern(text) {
      const cached = ordinals.get(text);
      if (cached !== undefined) return cached;
      const ordinal = internText(text);
      if (ordinal !== 0) ordinals.set(text, ordinal);
      return ordinal;
    },
    revalidate() {
      const next = textEpoch();
      if (next === epoch) return;
      ordinals.clear();
      epoch = next;
    },
  };
}

export function threadVocabularyText(vocabularyId: number): string {
  return decodeVocabularyMessage(getVocabularyGeneration(), vocabularyId - 1);
}

/**
 * A writer bound to one row store. Construction is cold; each lifecycle method
 * is one call into the store.
 *
 * Rows live in fixed blocks of {@link capacity} rows: row `r` is local row
 * `r % capacity` of block `Math.floor(r / capacity)`. Every row-producing
 * method returns the packed receipt `(spanId << 32) | row`, and bare `0n` when
 * the store refused.
 *
 * A binding whose host owns span identity and the clock may ignore the trace
 * id, the parent thread id and the timestamps it is handed: it stamps and
 * parents rows itself, and the arguments exist for the lanes where JavaScript
 * owns them.
 */
export interface ThreadSpanBufferBinding {
  /** Rows per block. */
  readonly capacity: number;
  free(): void;
  /**
   * Release every row and span, keeping every block's memory and — until the
   * store reclaims its text — the interned vocabulary.
   */
  reset(): number;
  /**
   * Intern `text`; the ordinal is stable within the store's text epoch, and
   * `0` means refused. A warm string costs one lookup and crosses nothing.
   */
  intern(text: string): number;
  openSpan(
    traceId: string,
    parentThreadId: bigint,
    parentSpanId: number,
    nameOrdinal: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  openSpanStatic(
    traceId: string,
    parentThreadId: bigint,
    parentSpanId: number,
    nameId: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  /** Complete a span with the tracer's own entry type. */
  end(spanId: number, entryType: number, timestamp: bigint): number;
  appendLog(spanId: number, entryType: number, messageOrdinal: number, timestamp: bigint, line: number): bigint;
  appendLogStatic(spanId: number, entryType: number, messageId: number, timestamp: bigint, line: number): bigint;
  setScope(spanId: number, ordinal: number, kind: ThreadAttributeKind | 0, value: bigint): number;
  /** Store the span's terminal message on its reserved completion row. */
  setCompletionMessage(spanId: number, message: string): number;
  /**
   * The bytes of `block`'s attribute cells, laid out as `lmao-core`'s
   * `AttributeCells` documents, or `undefined` before the block exists or when
   * the schema has no attributes. A view may be detached later (Wasm memory
   * growth); a caller holding one re-asks when its `byteLength` is 0.
   */
  attributeCells(block: number): Uint8Array | undefined;
}

/** Raw exports supplied by `allocator.wasm` for the shared-buffer ABI. */
export interface ThreadSpanBufferWasmExports {
  thread_span_buffer_new(threadId: bigint, capacity: number): ThreadSpanBufferHandle;
  thread_span_buffer_new_with_schema(
    threadId: bigint,
    capacity: number,
    fieldsPtr: number,
    fieldsLen: number,
  ): ThreadSpanBufferHandle;
  thread_span_buffer_free(handle: ThreadSpanBufferHandle): void;
  thread_span_buffer_reset(handle: ThreadSpanBufferHandle): number;
  thread_span_buffer_text_epoch(handle: ThreadSpanBufferHandle): number;
  thread_span_buffer_intern(handle: ThreadSpanBufferHandle, ptr: number, len: number): number;
  thread_span_buffer_open_span(
    handle: ThreadSpanBufferHandle,
    tracePtr: number,
    traceLen: number,
    parentThreadId: bigint,
    parentSpanId: number,
    nameOrdinal: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  thread_span_buffer_open_span_static(
    handle: ThreadSpanBufferHandle,
    tracePtr: number,
    traceLen: number,
    parentThreadId: bigint,
    parentSpanId: number,
    nameId: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  thread_span_buffer_open_span_dynamic(
    handle: ThreadSpanBufferHandle,
    tracePtr: number,
    traceLen: number,
    parentThreadId: bigint,
    parentSpanId: number,
    namePtr: number,
    nameLen: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  thread_span_buffer_end(handle: ThreadSpanBufferHandle, spanId: number, entryType: number, timestamp: bigint): number;
  thread_span_buffer_append_log(
    handle: ThreadSpanBufferHandle,
    spanId: number,
    entryType: number,
    messageOrdinal: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  thread_span_buffer_append_log_static(
    handle: ThreadSpanBufferHandle,
    spanId: number,
    entryType: number,
    messageId: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  thread_span_buffer_append_log_dynamic(
    handle: ThreadSpanBufferHandle,
    spanId: number,
    entryType: number,
    messagePtr: number,
    messageLen: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  thread_span_buffer_set_completion_message(
    handle: ThreadSpanBufferHandle,
    spanId: number,
    messagePtr: number,
    messageLen: number,
  ): number;
  thread_span_buffer_set_scope(
    handle: ThreadSpanBufferHandle,
    spanId: number,
    ordinal: number,
    // Kind 0 is the 01i clear sentinel (`setScope({ field: null })`); every
    // value-carrying write uses a real attribute kind.
    kind: ThreadAttributeKind | 0,
    value: bigint,
  ): number;
  /** Linear-memory offset of a block's attribute cells; 0 when there are none. */
  thread_span_buffer_attribute_cells(handle: ThreadSpanBufferHandle, block: number): number;
}

/** Validate the complete batch ABI before wiring it into a WASM instance. */
export function isThreadSpanBufferWasmExports(value: unknown): value is ThreadSpanBufferWasmExports {
  if (!isRecord(value)) return false;
  return (
    typeof Reflect.get(value, 'thread_span_buffer_new') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_new_with_schema') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_reset') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_text_epoch') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_intern') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_open_span') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_open_span_static') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_open_span_dynamic') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_end') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_append_log') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_append_log_static') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_append_log_dynamic') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_set_scope') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_set_completion_message') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_attribute_cells') === 'function'
  );
}

/** Where a Wasm binding encodes strings and finds its cells. */
export interface WasmThreadSpanBufferMemory {
  readonly memory: WebAssembly.Memory;
  /** Encode `text` into the scratch page; the bytes are valid until the next call. */
  writeUtf8(text: string): { ptr: number; len: number };
}

/** A binding over one `allocator.wasm` slot. */
export interface WasmThreadSpanBufferBinding extends ThreadSpanBufferBinding {
  readonly handle: ThreadSpanBufferHandle;
}

/**
 * Bind one slot of `allocator.wasm`. `fieldCount` is the schema's attribute
 * count, which with `capacity` fixes the byte length of a block's cells.
 */
export function bindThreadSpanBuffer(
  value: unknown,
  handle: ThreadSpanBufferHandle,
  capacity: number,
  fieldCount: number,
  scratch: WasmThreadSpanBufferMemory,
): WasmThreadSpanBufferBinding | undefined {
  if (!isThreadSpanBufferWasmExports(value) || handle === 0) return undefined;
  const cellBytes = fieldCount * attributeCellStride(capacity) * 8;
  const interned = internCache(
    (text) => {
      const payload = scratch.writeUtf8(text);
      return value.thread_span_buffer_intern(handle, payload.ptr, payload.len);
    },
    () => value.thread_span_buffer_text_epoch(handle),
  );
  return {
    handle,
    capacity,
    free: () => value.thread_span_buffer_free(handle),
    reset: () => {
      const status = value.thread_span_buffer_reset(handle);
      interned.revalidate();
      return status;
    },
    intern: interned.intern,
    openSpan: (traceId, parentThreadId, parentSpanId, nameOrdinal, timestamp, line) => {
      const trace = scratch.writeUtf8(traceId);
      return value.thread_span_buffer_open_span(
        handle,
        trace.ptr,
        trace.len,
        parentThreadId,
        parentSpanId,
        nameOrdinal,
        timestamp,
        line,
      );
    },
    openSpanStatic: (traceId, parentThreadId, parentSpanId, nameId, timestamp, line) => {
      const trace = scratch.writeUtf8(traceId);
      return value.thread_span_buffer_open_span_static(
        handle,
        trace.ptr,
        trace.len,
        parentThreadId,
        parentSpanId,
        nameId,
        timestamp,
        line,
      );
    },
    end: (spanId, entryType, timestamp) => value.thread_span_buffer_end(handle, spanId, entryType, timestamp),
    appendLog: (spanId, entryType, messageOrdinal, timestamp, line) =>
      value.thread_span_buffer_append_log(handle, spanId, entryType, messageOrdinal, timestamp, line),
    appendLogStatic: (spanId, entryType, messageId, timestamp, line) =>
      value.thread_span_buffer_append_log_static(handle, spanId, entryType, messageId, timestamp, line),
    setScope: (spanId, ordinal, kind, attributeValue) =>
      value.thread_span_buffer_set_scope(handle, spanId, ordinal, kind, attributeValue),
    setCompletionMessage: (spanId, message) => {
      const payload = scratch.writeUtf8(message);
      return value.thread_span_buffer_set_completion_message(handle, spanId, payload.ptr, payload.len);
    },
    attributeCells: (block) => {
      const offset = value.thread_span_buffer_attribute_cells(handle, block);
      return offset === 0 ? undefined : new Uint8Array(scratch.memory.buffer, offset, cellBytes);
    },
  };
}
