/**
 * Opaque-handle ABI for the shared per-thread span buffer.
 *
 * WASM uses a numeric slot token rather than exposing a Rust pointer to JS. The
 * binding owns the token and forwards row writes without recreating a SpanBuffer
 * or an Arrow converter in TypeScript. Dynamic text is addressed in the WASM
 * linear memory; `intern` turns it into a stable handle-local vocabulary id.
 */

import { isRecord } from '@smoothbricks/validation';
import { THREAD_ATTRIBUTE_KINDS } from '../schema/systemSchema.js';

export { THREAD_ATTRIBUTE_KINDS };
export type ThreadAttributeKind = (typeof THREAD_ATTRIBUTE_KINDS)[number]['discriminant'];

/** Numeric token returned by `thread_span_buffer_new`; zero is never a handle. */
export type ThreadSpanBufferHandle = number;

/** Successful status returned by fallible row-write exports. */
export const THREAD_SPAN_BUFFER_OK = 0;

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
  thread_span_buffer_write_attr(
    handle: ThreadSpanBufferHandle,
    row: number,
    ordinal: number,
    kind: ThreadAttributeKind,
    value: bigint,
  ): number;
  thread_span_buffer_write_tag(
    handle: ThreadSpanBufferHandle,
    spanId: number,
    ordinal: number,
    kind: ThreadAttributeKind,
    value: bigint,
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
}

/** A handle-bound writer. Construction is cold; each method is one ABI call. */
export interface ThreadSpanBufferBinding {
  readonly handle: ThreadSpanBufferHandle;
  free(): void;
  /** Release every row and span, keeping this handle's interned vocabulary. */
  reset(): number;
  openSpan(
    tracePtr: number,
    traceLen: number,
    parentThreadId: bigint,
    parentSpanId: number,
    nameOrdinal: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  openSpanStatic(
    tracePtr: number,
    traceLen: number,
    parentThreadId: bigint,
    parentSpanId: number,
    nameId: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  openSpanDynamic(
    tracePtr: number,
    traceLen: number,
    parentThreadId: bigint,
    parentSpanId: number,
    namePtr: number,
    nameLen: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  /** Complete a span with the tracer's own entry type. */
  end(spanId: number, entryType: number, timestamp: bigint): number;
  appendLog(spanId: number, entryType: number, messageOrdinal: number, timestamp: bigint, line: number): bigint;
  appendLogStatic(spanId: number, entryType: number, messageId: number, timestamp: bigint, line: number): bigint;
  appendLogDynamic(
    spanId: number,
    entryType: number,
    messagePtr: number,
    messageLen: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  writeAttr(row: number, ordinal: number, kind: ThreadAttributeKind, value: bigint): number;
  writeTag(spanId: number, ordinal: number, kind: ThreadAttributeKind, value: bigint): number;
  setScope(spanId: number, ordinal: number, kind: ThreadAttributeKind | 0, value: bigint): number;
  intern(ptr: number, len: number): number;
}

/** Validate the complete batch ABI before wiring it into a WASM instance. */
export function isThreadSpanBufferWasmExports(value: unknown): value is ThreadSpanBufferWasmExports {
  if (!isRecord(value)) return false;
  return (
    typeof Reflect.get(value, 'thread_span_buffer_new') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_new_with_schema') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_reset') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_intern') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_open_span') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_open_span_static') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_open_span_dynamic') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_end') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_append_log') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_append_log_static') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_append_log_dynamic') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_write_attr') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_write_tag') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_set_scope') === 'function'
  );
}

/** Bind one opaque handle without copying or adapting the row layout. */
export function bindThreadSpanBuffer(
  value: unknown,
  handle: ThreadSpanBufferHandle,
): ThreadSpanBufferBinding | undefined {
  if (!isThreadSpanBufferWasmExports(value)) return undefined;
  return {
    handle,
    free: () => value.thread_span_buffer_free(handle),
    reset: () => value.thread_span_buffer_reset(handle),
    openSpan: (tracePtr, traceLen, parentThreadId, parentSpanId, nameOrdinal, timestamp, line) =>
      value.thread_span_buffer_open_span(
        handle,
        tracePtr,
        traceLen,
        parentThreadId,
        parentSpanId,
        nameOrdinal,
        timestamp,
        line,
      ),
    openSpanStatic: (tracePtr, traceLen, parentThreadId, parentSpanId, nameId, timestamp, line) =>
      value.thread_span_buffer_open_span_static(
        handle,
        tracePtr,
        traceLen,
        parentThreadId,
        parentSpanId,
        nameId,
        timestamp,
        line,
      ),
    openSpanDynamic: (tracePtr, traceLen, parentThreadId, parentSpanId, namePtr, nameLen, timestamp, line) =>
      value.thread_span_buffer_open_span_dynamic(
        handle,
        tracePtr,
        traceLen,
        parentThreadId,
        parentSpanId,
        namePtr,
        nameLen,
        timestamp,
        line,
      ),
    end: (spanId, entryType, timestamp) => value.thread_span_buffer_end(handle, spanId, entryType, timestamp),
    appendLog: (spanId, entryType, messageOrdinal, timestamp, line) =>
      value.thread_span_buffer_append_log(handle, spanId, entryType, messageOrdinal, timestamp, line),
    appendLogStatic: (spanId, entryType, messageId, timestamp, line) =>
      value.thread_span_buffer_append_log_static(handle, spanId, entryType, messageId, timestamp, line),
    appendLogDynamic: (spanId, entryType, messagePtr, messageLen, timestamp, line) =>
      value.thread_span_buffer_append_log_dynamic(handle, spanId, entryType, messagePtr, messageLen, timestamp, line),
    writeAttr: (row, ordinal, kind, attributeValue) =>
      value.thread_span_buffer_write_attr(handle, row, ordinal, kind, attributeValue),
    writeTag: (spanId, ordinal, kind, attributeValue) =>
      value.thread_span_buffer_write_tag(handle, spanId, ordinal, kind, attributeValue),
    setScope: (spanId, ordinal, kind, attributeValue) =>
      value.thread_span_buffer_set_scope(handle, spanId, ordinal, kind, attributeValue),
    intern: (ptr, len) => value.thread_span_buffer_intern(handle, ptr, len),
  };
}
