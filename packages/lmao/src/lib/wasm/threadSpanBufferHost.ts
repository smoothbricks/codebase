/**
 * The Wasm provider of the thread lane: instantiates allocator.wasm and binds
 * its shared per-thread span buffer slots.
 *
 * Scratch pages are grown from the imported memory so intern/open payloads live
 * at offsets the WASM module can read without colliding with the Rust heap.
 */

import { isRecord } from '@smoothbricks/validation';
import type { Table } from '@uwdata/flechette';
import type { LogSchema } from '../schema/LogSchema.js';
import type { ThreadSpanBufferProvider } from '../ThreadBufferStrategy.js';
import { convertThreadViewToArrowTable } from './convertThreadBuffer.js';
import { encodeSchemaBlob, schemaAttributeOrdinals } from './schemaBlob.js';
import {
  bindThreadSpanBuffer,
  isThreadSpanBufferWasmExports,
  type ThreadSpanBufferBinding,
  type ThreadSpanBufferWasmExports,
  type WasmThreadSpanBufferBinding,
} from './threadSpanBuffer.js';
import type { ThreadSpanView } from './threadSpanView.js';
import { getWasmModule } from './wasmAllocator.js';

const WASM_PAGE = 65_536;
const MIN_INITIAL_PAGES = 17;
const DEFAULT_MAX_PAGES = 16384;

export interface ThreadSpanBufferReadExports {
  thread_span_buffer_row_count(handle: number): number;
  thread_span_buffer_materialize_scope(handle: number, startRow: number, rowCount: number): number;
  thread_span_buffer_read_timestamp(handle: number, row: number): bigint;
  thread_span_buffer_read_span_id(handle: number, row: number): number;
  thread_span_buffer_read_header(handle: number, row: number): number;
  thread_span_buffer_read_parent_span_id(handle: number, row: number): number;
  thread_span_buffer_read_parent_thread_id(handle: number, row: number): bigint;
  thread_span_buffer_read_line(handle: number, row: number): number;
  thread_span_buffer_read_trace_id(handle: number, row: number, outPtr: number, outLen: number): number;
  thread_span_buffer_read_message(handle: number, row: number, outPtr: number, outLen: number): number;
  thread_span_buffer_read_attr(handle: number, row: number, ordinal: number, outKind: number, outValue: number): number;
  thread_span_buffer_read_interned(handle: number, ordinal: number, outPtr: number, outLen: number): number;
}

export type ThreadSpanBufferModuleExports = ThreadSpanBufferWasmExports & ThreadSpanBufferReadExports;

/** Reads a Wasm slot back into JavaScript, for the JS-side Arrow conversion. */
export interface ThreadSpanBufferReader {
  rowCount(binding: ThreadSpanBufferBinding): number;
  readTimestamp(binding: ThreadSpanBufferBinding, row: number): bigint;
  readSpanId(binding: ThreadSpanBufferBinding, row: number): number;
  readHeader(binding: ThreadSpanBufferBinding, row: number): number;
  readParentSpanId(binding: ThreadSpanBufferBinding, row: number): number;
  readLine(binding: ThreadSpanBufferBinding, row: number): number;
  readTraceId(binding: ThreadSpanBufferBinding, row: number): string;
  readMessage(binding: ThreadSpanBufferBinding, row: number): string;
  materializeScope(binding: ThreadSpanBufferBinding, startRow: number, rowCount: number): void;
  readAttr(binding: ThreadSpanBufferBinding, row: number, ordinal: number): { kind: number; value: bigint } | undefined;
  readInterned(binding: ThreadSpanBufferBinding, ordinal: number): string;
}

export interface ThreadSpanBufferRuntime extends ThreadSpanBufferProvider, ThreadSpanBufferReader {
  readonly memory: WebAssembly.Memory;
  readonly exports: ThreadSpanBufferModuleExports;
  createBinding(threadId: bigint, capacity: number, schema: LogSchema): WasmThreadSpanBufferBinding;
}

function isThreadSpanBufferModuleExports(value: unknown): value is ThreadSpanBufferModuleExports {
  if (!isThreadSpanBufferWasmExports(value) || !isRecord(value)) return false;
  return (
    typeof Reflect.get(value, 'thread_span_buffer_row_count') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_read_timestamp') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_read_span_id') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_read_header') === 'function' &&
    typeof Reflect.get(value, 'thread_span_buffer_read_attr') === 'function'
  );
}

const utf8 = new TextEncoder();
const utf8Decoder = new TextDecoder();

export async function createThreadSpanBufferRuntime(options?: {
  initialPages?: number;
  maxPages?: number;
  /**
   * Pre-compiled allocator.wasm. Bundled environments (Expo web) cannot
   * resolve the artifact relative to import.meta.url, so the consumer that
   * knows where the artifact ships supplies the compiled module — the same
   * contract createWasmAllocatorSync already offers on the allocator side.
   */
  module?: WebAssembly.Module;
}): Promise<ThreadSpanBufferRuntime> {
  const initialPages = Math.max(options?.initialPages ?? MIN_INITIAL_PAGES, MIN_INITIAL_PAGES);
  const maxPages = Math.max(options?.maxPages ?? DEFAULT_MAX_PAGES, initialPages);
  const memory = new WebAssembly.Memory({ initial: initialPages, maximum: maxPages });
  const module = options?.module ?? (await getWasmModule());
  const instance = await WebAssembly.instantiate(module, {
    env: {
      memory,
      performanceNow: () => performance.now(),
      dateNow: () => Date.now(),
    },
  });
  if (!isThreadSpanBufferModuleExports(instance.exports)) {
    throw new Error('allocator.wasm is missing the ThreadSpanBuffer ABI');
  }
  const exports = instance.exports;

  const grown = memory.grow(1);
  if (grown < 0) throw new Error('failed to grow WASM scratch page');
  let scratchPtr = grown * WASM_PAGE;
  let scratchLen = WASM_PAGE;

  // One view over the scratch page, re-derived only when the page moves.
  // `memory.grow` detaches every view onto `memory.buffer`, so `ensureScratch`
  // is the single place allowed to replace it.
  let scratchView = new Uint8Array(memory.buffer, scratchPtr, scratchLen);

  const ensureScratch = (len: number): void => {
    if (len <= scratchLen && scratchView.byteLength !== 0) return;
    if (len > scratchLen) {
      const pages = Math.ceil(len / WASM_PAGE);
      const next = memory.grow(pages);
      if (next < 0) throw new Error('failed to grow WASM scratch');
      scratchPtr = next * WASM_PAGE;
      scratchLen = pages * WASM_PAGE;
    }
    scratchView = new Uint8Array(memory.buffer, scratchPtr, scratchLen);
  };

  const scratch = {
    memory,
    writeUtf8: (text: string): { ptr: number; len: number } => {
      // Worst case for UTF-8 is 3 bytes per UTF-16 code unit; surrogate pairs
      // are 2 units producing 4 bytes, so the bound holds. Encoding straight
      // into linear memory avoids the intermediate array `encode()` returns.
      ensureScratch(text.length * 3);
      const written = utf8.encodeInto(text, scratchView).written;
      return { ptr: scratchPtr, len: written };
    },
  };

  const readUtf8 = (ptr: number, len: number): string =>
    len === 0 ? '' : utf8Decoder.decode(new Uint8Array(memory.buffer, ptr, len));

  /** Every slot this runtime bound, so a reader can name the slot behind a binding. */
  const handles = new WeakMap<ThreadSpanBufferBinding, number>();
  const handleOf = (binding: ThreadSpanBufferBinding): number => {
    const handle = handles.get(binding);
    // invariant throw: a binding from another provider reached this reader.
    if (handle === undefined) throw new Error('binding was not created by this thread span buffer runtime');
    return handle;
  };

  const createBinding = (threadId: bigint, capacity: number, schema: LogSchema): WasmThreadSpanBufferBinding => {
    const blob = encodeSchemaBlob(schema);
    let handle: number;
    if (blob.length === 0) {
      handle = exports.thread_span_buffer_new(threadId, capacity);
    } else {
      ensureScratch(blob.length);
      new Uint8Array(memory.buffer, scratchPtr, blob.length).set(blob);
      handle = exports.thread_span_buffer_new_with_schema(threadId, capacity, scratchPtr, blob.length);
    }
    if (handle === 0) throw new Error('thread_span_buffer_new rejected capacity or schema');
    const binding = bindThreadSpanBuffer(exports, handle, capacity, schemaAttributeOrdinals(schema).size, scratch);
    if (binding === undefined) throw new Error('failed to bind ThreadSpanBuffer handle');
    handles.set(binding, handle);
    return binding;
  };

  const copyString = (
    reader: (handle: number, row: number, ptr: number, len: number) => number,
    binding: ThreadSpanBufferBinding,
    row: number,
  ): string => {
    const handle = handleOf(binding);
    const needed = reader(handle, row, scratchPtr, scratchLen);
    if (needed === 0) return '';
    if (needed > scratchLen) {
      ensureScratch(needed);
      reader(handle, row, scratchPtr, scratchLen);
    }
    return readUtf8(scratchPtr, needed);
  };

  const runtime: ThreadSpanBufferRuntime = {
    memory,
    exports,
    createBinding,
    toArrowTable: (view: ThreadSpanView): Table => convertThreadViewToArrowTable(runtime, view),
    rowCount: (binding) => exports.thread_span_buffer_row_count(handleOf(binding)),
    readTimestamp: (binding, row) => exports.thread_span_buffer_read_timestamp(handleOf(binding), row),
    readSpanId: (binding, row) => exports.thread_span_buffer_read_span_id(handleOf(binding), row),
    readHeader: (binding, row) => exports.thread_span_buffer_read_header(handleOf(binding), row),
    readParentSpanId: (binding, row) => exports.thread_span_buffer_read_parent_span_id(handleOf(binding), row),
    readLine: (binding, row) => exports.thread_span_buffer_read_line(handleOf(binding), row),
    readTraceId: (binding, row) => copyString(exports.thread_span_buffer_read_trace_id, binding, row),
    readMessage: (binding, row) => copyString(exports.thread_span_buffer_read_message, binding, row),
    materializeScope: (binding, startRow, rowCount) => {
      if (exports.thread_span_buffer_materialize_scope(handleOf(binding), startRow, rowCount) !== 0) {
        throw new Error('thread_span_buffer_materialize_scope failed');
      }
    },
    readAttr: (binding, row, ordinal) => {
      ensureScratch(16);
      const kindPtr = scratchPtr;
      const valuePtr = (scratchPtr + 8) & ~7;
      const status = exports.thread_span_buffer_read_attr(handleOf(binding), row, ordinal, kindPtr, valuePtr);
      if (status !== 0) return undefined;
      const view = new DataView(memory.buffer);
      return { kind: view.getUint8(kindPtr), value: view.getBigUint64(valuePtr, true) };
    },
    readInterned: (binding, ordinal) => {
      const handle = handleOf(binding);
      const needed = exports.thread_span_buffer_read_interned(handle, ordinal, scratchPtr, scratchLen);
      if (needed === 0) return '';
      if (needed > scratchLen) {
        ensureScratch(needed);
        exports.thread_span_buffer_read_interned(handle, ordinal, scratchPtr, scratchLen);
      }
      return readUtf8(scratchPtr, needed);
    },
  };
  return runtime;
}
