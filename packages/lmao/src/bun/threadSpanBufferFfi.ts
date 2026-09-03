/**
 * Bun's native provider for the shared per-thread span buffer ABI.
 *
 * The shared library is loaded once when this Bun-only entrypoint is imported.
 * Development resolves the repository-root Cargo release artifact; published
 * consumers supply `LMAO_THREAD_FFI_DYLIB` because npm packages do not ship
 * platform-native build output.
 */

import { dlopen, FFIType, type Pointer, ptr, suffix } from 'bun:ffi';
import { fileURLToPath } from 'node:url';
import type {
  ThreadAttributeKind,
  ThreadSpanBufferHandle,
  ThreadSpanBufferBinding as WasmThreadSpanBufferBinding,
} from '../lib/wasm/threadSpanBuffer.js';

export type { ThreadAttributeKind, ThreadSpanBufferHandle };

/** A binding with string helpers layered over the shared low-level ABI. */
export type ThreadSpanBufferBinding = WasmThreadSpanBufferBinding & {
  /** Intern a JS string without re-encoding warmed values. */
  intern(text: string): number;
  /** Explicit string form for callers that do not use the overloaded intern method. */
  internString(text: string): number;
  openSpanText(
    traceId: string,
    parentThreadId: bigint,
    parentSpanId: number,
    nameOrdinal: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  openSpanStaticText(
    traceId: string,
    parentThreadId: bigint,
    parentSpanId: number,
    nameId: number,
    timestamp: bigint,
    line: number,
  ): bigint;
  openSpanDynamicText(
    traceId: string,
    parentThreadId: bigint,
    parentSpanId: number,
    name: string,
    timestamp: bigint,
    line: number,
  ): bigint;
  appendLogDynamicText(spanId: number, entryType: number, message: string, timestamp: bigint, line: number): bigint;
  /** Native library path selected at module load. */
  readonly dylibPath: string;
};

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
  thread_span_buffer_new_with_fields: {
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
  thread_span_buffer_open_span_dynamic: {
    args: [
      FFIType.ptr,
      FFIType.ptr,
      FFIType.u64,
      FFIType.u64,
      FFIType.u32,
      FFIType.ptr,
      FFIType.u64,
      FFIType.i64,
      FFIType.u32,
    ],
    returns: FFIType.u64,
  },
  thread_span_buffer_end: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u8, FFIType.i64],
    returns: FFIType.u8,
  },
  thread_span_buffer_end_ok: {
    args: [FFIType.ptr, FFIType.u32, FFIType.i64],
    returns: FFIType.u8,
  },
  thread_span_buffer_end_err: {
    args: [FFIType.ptr, FFIType.u32, FFIType.i64],
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
  thread_span_buffer_append_log_dynamic: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u8, FFIType.ptr, FFIType.u64, FFIType.i64, FFIType.u32],
    returns: FFIType.u64,
  },
  thread_span_buffer_write_attr: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u16, FFIType.u8, FFIType.u64],
    returns: FFIType.u8,
  },
  thread_span_buffer_write_tag: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u16, FFIType.u8, FFIType.u64],
    returns: FFIType.u8,
  },
  thread_span_buffer_set_scope: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u16, FFIType.u8, FFIType.u64],
    returns: FFIType.u8,
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

const internCaches = new WeakMap<ThreadSpanBufferBinding, Map<string, number>>();

function withUtf8<T>(text: string, callback: (address: Pointer, length: bigint) => T): T {
  const scratch = utf8.encode(text);
  return callback(ptr(scratch), BigInt(scratch.byteLength));
}

function internText(binding: ThreadSpanBufferBinding, text: string): number {
  let cache = internCaches.get(binding);
  if (cache === undefined) {
    cache = new Map<string, number>();
    internCaches.set(binding, cache);
  }
  const cached = cache.get(text);
  if (cached !== undefined) return cached;
  const library = nativeLoad.library;
  if (library === undefined) return 0;
  const id = withUtf8(text, (address, length) =>
    library.symbols.thread_span_buffer_intern(BigInt(binding.handle), address, length),
  );
  if (id !== 0) cache.set(text, id);
  return id;
}

/** Bind an existing native opaque pointer returned by the constructor. */
export function bindThreadSpanBuffer(handle: ThreadSpanBufferHandle): ThreadSpanBufferBinding | undefined {
  const library = nativeLoad.library;
  if (library === undefined || handle === 0) return undefined;

  const binding: ThreadSpanBufferBinding = {
    handle,
    dylibPath: THREAD_SPAN_BUFFER_FFI_DYLIB_PATH,
    free: () => library.symbols.thread_span_buffer_free(BigInt(handle)),
    reset: () => library.symbols.thread_span_buffer_reset(BigInt(handle)),
    openSpan: (tracePtr, traceLen, parentThreadId, parentSpanId, nameOrdinal, timestamp, line) =>
      library.symbols.thread_span_buffer_open_span(
        BigInt(handle),
        BigInt(tracePtr),
        BigInt(traceLen),
        parentThreadId,
        parentSpanId,
        nameOrdinal,
        timestamp,
        line,
      ),
    openSpanStatic: (tracePtr, traceLen, parentThreadId, parentSpanId, nameId, timestamp, line) =>
      library.symbols.thread_span_buffer_open_span_static(
        BigInt(handle),
        BigInt(tracePtr),
        BigInt(traceLen),
        parentThreadId,
        parentSpanId,
        nameId,
        timestamp,
        line,
      ),
    openSpanDynamic: (tracePtr, traceLen, parentThreadId, parentSpanId, namePtr, nameLen, timestamp, line) =>
      library.symbols.thread_span_buffer_open_span_dynamic(
        BigInt(handle),
        BigInt(tracePtr),
        BigInt(traceLen),
        parentThreadId,
        parentSpanId,
        BigInt(namePtr),
        BigInt(nameLen),
        timestamp,
        line,
      ),
    end: (spanId, entryType, timestamp) =>
      library.symbols.thread_span_buffer_end(BigInt(handle), spanId, entryType, timestamp),
    appendLog: (spanId, entryType, messageOrdinal, timestamp, line) =>
      library.symbols.thread_span_buffer_append_log(BigInt(handle), spanId, entryType, messageOrdinal, timestamp, line),
    appendLogStatic: (spanId, entryType, messageId, timestamp, line) =>
      library.symbols.thread_span_buffer_append_log_static(
        BigInt(handle),
        spanId,
        entryType,
        messageId,
        timestamp,
        line,
      ),
    appendLogDynamic: (spanId, entryType, messagePtr, messageLen, timestamp, line) =>
      library.symbols.thread_span_buffer_append_log_dynamic(
        BigInt(handle),
        spanId,
        entryType,
        BigInt(messagePtr),
        BigInt(messageLen),
        timestamp,
        line,
      ),
    writeAttr: (row, ordinal, kind, value) =>
      library.symbols.thread_span_buffer_write_attr(BigInt(handle), row, ordinal, kind, value),
    writeTag: (spanId, ordinal, kind, value) =>
      library.symbols.thread_span_buffer_write_tag(BigInt(handle), spanId, ordinal, kind, value),
    setScope: (spanId, ordinal, kind, value) =>
      library.symbols.thread_span_buffer_set_scope(BigInt(handle), spanId, ordinal, kind, value),
    setCompletionMessage: (spanId, messagePtr, messageLen) =>
      library.symbols.thread_span_buffer_set_completion_message(
        BigInt(handle),
        spanId,
        BigInt(messagePtr),
        BigInt(messageLen),
      ),
    intern: (addressOrText: number | string, length?: number): number => {
      if (typeof addressOrText === 'string') return internText(binding, addressOrText);
      if (length === undefined) return 0;
      return library.symbols.thread_span_buffer_intern(BigInt(handle), BigInt(addressOrText), BigInt(length));
    },
    internString: (text) => internText(binding, text),
    openSpanText: (traceId, parentThreadId, parentSpanId, nameOrdinal, timestamp, line) =>
      withUtf8(traceId, (tracePtr, traceLen) =>
        library.symbols.thread_span_buffer_open_span(
          BigInt(handle),
          tracePtr,
          traceLen,
          parentThreadId,
          parentSpanId,
          nameOrdinal,
          timestamp,
          line,
        ),
      ),
    openSpanStaticText: (traceId, parentThreadId, parentSpanId, nameId, timestamp, line) =>
      withUtf8(traceId, (tracePtr, traceLen) =>
        library.symbols.thread_span_buffer_open_span_static(
          BigInt(handle),
          tracePtr,
          traceLen,
          parentThreadId,
          parentSpanId,
          nameId,
          timestamp,
          line,
        ),
      ),
    openSpanDynamicText: (traceId, parentThreadId, parentSpanId, name, timestamp, line) =>
      withUtf8(traceId, (tracePtr, traceLen) =>
        withUtf8(name, (namePtr, nameLen) =>
          library.symbols.thread_span_buffer_open_span_dynamic(
            BigInt(handle),
            tracePtr,
            traceLen,
            parentThreadId,
            parentSpanId,
            namePtr,
            nameLen,
            timestamp,
            line,
          ),
        ),
      ),
    appendLogDynamicText: (spanId, entryType, message, timestamp, line) =>
      withUtf8(message, (messagePtr, messageLen) =>
        library.symbols.thread_span_buffer_append_log_dynamic(
          BigInt(handle),
          spanId,
          entryType,
          messagePtr,
          messageLen,
          timestamp,
          line,
        ),
      ),
  };
  internCaches.set(binding, new Map<string, number>());
  return binding;
}

/** Allocate and bind a native row store. */
export function createThreadSpanBuffer(threadId: bigint, capacity: number): ThreadSpanBufferBinding | undefined {
  const library = nativeLoad.library;
  if (library === undefined || !Number.isSafeInteger(capacity) || capacity < 0) return undefined;
  const nativeHandle = library.symbols.thread_span_buffer_new(threadId, BigInt(capacity));
  if (nativeHandle === null) return undefined;
  return bindThreadSpanBuffer(Number(nativeHandle));
}
