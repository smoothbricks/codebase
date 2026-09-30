/**
 * Bun's native provider for the shared per-thread span buffer.
 *
 * The shared library is loaded once when this Bun-only entrypoint is imported.
 * Development resolves the repository-root Cargo release artifact; published
 * consumers supply `LMAO_THREAD_FFI_DYLIB` because npm packages do not ship
 * platform-native build output.
 *
 * The store's attribute cells are native memory: `attributeCells` wraps them
 * with `toArrayBuffer`, which aliases the bytes rather than copying them, so a
 * TypedArray store here is a store into the row store itself.
 */

import { dlopen, FFIType, type Pointer, ptr, suffix, toArrayBuffer } from 'bun:ffi';
import { fileURLToPath } from 'node:url';
import type { LogSchema } from '../lib/schema/LogSchema.js';
import { encodeSchemaBlob, schemaAttributeOrdinals } from '../lib/wasm/schemaBlob.js';
import {
  attributeCellStride,
  type ThreadAttributeKind,
  type ThreadSpanBufferBinding,
  type ThreadSpanBufferHandle,
} from '../lib/wasm/threadSpanBuffer.js';

export type { ThreadAttributeKind, ThreadSpanBufferHandle };

/** A binding over one native row store. */
export interface NativeThreadSpanBufferBinding extends ThreadSpanBufferBinding {
  /** The store's opaque native pointer. */
  readonly handle: Pointer;
  /** Native library path selected at module load. */
  readonly dylibPath: string;
}

const utf8 = new TextEncoder();

const dylibName = suffix === 'dll' ? 'lmao_ffi_dylib.dll' : `liblmao_ffi_dylib.${suffix}`;
const configuredPath = process.env.LMAO_THREAD_FFI_DYLIB;
export const THREAD_SPAN_BUFFER_FFI_DYLIB_PATH =
  configuredPath && configuredPath.length > 0
    ? configuredPath
    : fileURLToPath(new URL(`../../../../target/release/${dylibName}`, import.meta.url));

const nativeSymbols = {
  thread_span_buffer_new: {
    args: [FFIType.u64, FFIType.u64],
    returns: FFIType.ptr,
  },
  thread_span_buffer_new_with_schema: {
    args: [FFIType.u64, FFIType.u64, FFIType.ptr, FFIType.u64],
    returns: FFIType.ptr,
  },
  thread_span_buffer_free: {
    args: [FFIType.ptr],
    returns: FFIType.void,
  },
  thread_span_buffer_reset: {
    args: [FFIType.ptr],
    returns: FFIType.u8,
  },
  thread_span_buffer_intern: {
    args: [FFIType.ptr, FFIType.ptr, FFIType.u64],
    returns: FFIType.u32,
  },
  thread_span_buffer_set_completion_message: {
    args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u64],
    returns: FFIType.u8,
  },
  thread_span_buffer_open_span: {
    args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.u64, FFIType.u32, FFIType.u32, FFIType.i64, FFIType.u32],
    returns: FFIType.u64,
  },
  thread_span_buffer_open_span_static: {
    args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.u64, FFIType.u32, FFIType.u32, FFIType.i64, FFIType.u32],
    returns: FFIType.u64,
  },
  thread_span_buffer_end: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u8, FFIType.i64],
    returns: FFIType.u8,
  },
  thread_span_buffer_append_log: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u8, FFIType.u32, FFIType.i64, FFIType.u32],
    returns: FFIType.u64,
  },
  thread_span_buffer_append_log_static: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u8, FFIType.u32, FFIType.i64, FFIType.u32],
    returns: FFIType.u64,
  },
  thread_span_buffer_set_scope: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u16, FFIType.u8, FFIType.u64],
    returns: FFIType.u8,
  },
  thread_span_buffer_attribute_cells: {
    args: [FFIType.ptr, FFIType.u64, FFIType.ptr],
    returns: FFIType.ptr,
  },
} as const;

function loadNativeLibrary() {
  try {
    return { library: dlopen(THREAD_SPAN_BUFFER_FFI_DYLIB_PATH, nativeSymbols), error: undefined };
  } catch (error) {
    return { library: undefined, error };
  }
}

const nativeLoad = loadNativeLibrary();

/** The module-load error, if the consumer has not supplied a usable dylib. */
export const threadSpanBufferFfiError = nativeLoad.error;
/** Whether the module-load `dlopen` succeeded. */
export const threadSpanBufferFfiAvailable = nativeLoad.library !== undefined;

/**
 * One scratch page for the UTF-8 the ABI borrows for a single call: a warm
 * span open encodes its trace id here instead of allocating an array.
 */
const scratch = new Uint8Array(4096);
const scratchAddress = ptr(scratch);

/** Encode `text` for one call; oversized text takes a one-off array. */
function withUtf8<T>(text: string, call: (address: Pointer, length: bigint) => T): T {
  if (text.length * 3 <= scratch.byteLength) {
    return call(scratchAddress, BigInt(utf8.encodeInto(text, scratch).written));
  }
  const bytes = utf8.encode(text);
  return call(ptr(bytes), BigInt(bytes.byteLength));
}

/** Bind a native row store returned by a constructor. */
function bindNative(handle: Pointer, capacity: number, fieldCount: number): NativeThreadSpanBufferBinding | undefined {
  const library = nativeLoad.library;
  if (library === undefined) return undefined;
  const symbols = library.symbols;
  const cellBytes = fieldCount * attributeCellStride(capacity) * 8;
  const cellLength = new BigUint64Array(1);
  const cellLengthAddress = ptr(cellLength);
  /** Ordinals are stable for the store's life, so a JS-side cache is exact. */
  const interned = new Map<string, number>();
  return {
    handle,
    capacity,
    dylibPath: THREAD_SPAN_BUFFER_FFI_DYLIB_PATH,
    free: () => symbols.thread_span_buffer_free(handle),
    reset: () => symbols.thread_span_buffer_reset(handle),
    intern: (text) => {
      const cached = interned.get(text);
      if (cached !== undefined) return cached;
      const id = withUtf8(text, (address, length) => symbols.thread_span_buffer_intern(handle, address, length));
      if (id !== 0) interned.set(text, id);
      return id;
    },
    openSpan: (traceId, parentThreadId, parentSpanId, nameOrdinal, timestamp, line) =>
      withUtf8(traceId, (address, length) =>
        symbols.thread_span_buffer_open_span(
          handle,
          address,
          length,
          parentThreadId,
          parentSpanId,
          nameOrdinal,
          timestamp,
          line,
        ),
      ),
    openSpanStatic: (traceId, parentThreadId, parentSpanId, nameId, timestamp, line) =>
      withUtf8(traceId, (address, length) =>
        symbols.thread_span_buffer_open_span_static(
          handle,
          address,
          length,
          parentThreadId,
          parentSpanId,
          nameId,
          timestamp,
          line,
        ),
      ),
    end: (spanId, entryType, timestamp) => symbols.thread_span_buffer_end(handle, spanId, entryType, timestamp),
    appendLog: (spanId, entryType, messageOrdinal, timestamp, line) =>
      symbols.thread_span_buffer_append_log(handle, spanId, entryType, messageOrdinal, timestamp, line),
    appendLogStatic: (spanId, entryType, messageId, timestamp, line) =>
      symbols.thread_span_buffer_append_log_static(handle, spanId, entryType, messageId, timestamp, line),
    setScope: (spanId, ordinal, kind, value) =>
      symbols.thread_span_buffer_set_scope(handle, spanId, ordinal, kind, value),
    setCompletionMessage: (spanId, message) =>
      withUtf8(message, (address, length) =>
        symbols.thread_span_buffer_set_completion_message(handle, spanId, address, length),
      ),
    attributeCells: (block) => {
      const cells = symbols.thread_span_buffer_attribute_cells(handle, BigInt(block), cellLengthAddress);
      if (cells === null || cellLength[0] !== BigInt(cellBytes)) return undefined;
      return new Uint8Array(toArrayBuffer(cells, 0, cellBytes));
    },
  };
}

/** Allocate and bind a native row store, laid out for `schema`'s attributes when given. */
export function createThreadSpanBuffer(
  threadId: bigint,
  capacity: number,
  schema?: LogSchema,
): NativeThreadSpanBufferBinding | undefined {
  const library = nativeLoad.library;
  if (library === undefined || !Number.isSafeInteger(capacity) || capacity < 0) return undefined;
  if (schema === undefined) {
    const handle = library.symbols.thread_span_buffer_new(threadId, BigInt(capacity));
    return handle === null || typeof handle === 'bigint' ? undefined : bindNative(handle, capacity, 0);
  }
  const blob = encodeSchemaBlob(schema);
  const handle = library.symbols.thread_span_buffer_new_with_schema(
    threadId,
    BigInt(capacity),
    blob.byteLength === 0 ? null : ptr(blob),
    BigInt(blob.byteLength),
  );
  return handle === null || typeof handle === 'bigint'
    ? undefined
    : bindNative(handle, capacity, schemaAttributeOrdinals(schema).size);
}
