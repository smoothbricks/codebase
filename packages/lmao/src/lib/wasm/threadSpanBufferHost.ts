/**
 * Instantiates allocator.wasm and binds the shared per-thread span buffer ABI.
 *
 * Scratch pages are grown from the imported memory so intern/open payloads live
 * at offsets the WASM module can read without colliding with the Rust heap.
 */

import { isRecord } from '@smoothbricks/validation';
import type { LogSchema } from '../schema/LogSchema.js';
import { encodeSchemaBlob } from './schemaBlob.js';
import {
  bindThreadSpanBuffer,
  isThreadSpanBufferWasmExports,
  type ThreadSpanBufferBinding,
  type ThreadSpanBufferWasmExports,
} from './threadSpanBuffer.js';
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

export interface ThreadSpanBufferRuntime {
  readonly memory: WebAssembly.Memory;
  readonly exports: ThreadSpanBufferModuleExports;
  writeUtf8(text: string): { ptr: number; len: number };
  intern(binding: ThreadSpanBufferBinding, text: string): number;
  createBinding(threadId: bigint, capacity: number, schema: LogSchema): ThreadSpanBufferBinding;
  readUtf8(ptr: number, len: number): string;
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
}): Promise<ThreadSpanBufferRuntime> {
  const initialPages = Math.max(options?.initialPages ?? MIN_INITIAL_PAGES, MIN_INITIAL_PAGES);
  const maxPages = Math.max(options?.maxPages ?? DEFAULT_MAX_PAGES, initialPages);
  const memory = new WebAssembly.Memory({ initial: initialPages, maximum: maxPages });
  const module = await getWasmModule();
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

  const ensureScratch = (len: number): void => {
    if (len <= scratchLen) return;
    const pages = Math.ceil(len / WASM_PAGE);
    const next = memory.grow(pages);
    if (next < 0) throw new Error('failed to grow WASM scratch');
    scratchPtr = next * WASM_PAGE;
    scratchLen = pages * WASM_PAGE;
  };

  const writeUtf8 = (text: string): { ptr: number; len: number } => {
    const bytes = utf8.encode(text);
    ensureScratch(bytes.length);
    new Uint8Array(memory.buffer, scratchPtr, bytes.length).set(bytes);
    return { ptr: scratchPtr, len: bytes.length };
  };

  const readUtf8 = (ptr: number, len: number): string => {
    if (len === 0) return '';
    return utf8Decoder.decode(new Uint8Array(memory.buffer, ptr, len));
  };

  const intern = (binding: ThreadSpanBufferBinding, text: string): number => {
    const payload = writeUtf8(text);
    const id = binding.intern(payload.ptr, payload.len);
    if (id === 0) throw new Error('thread span buffer intern failed');
    return id;
  };

  const createBinding = (threadId: bigint, capacity: number, schema: LogSchema): ThreadSpanBufferBinding => {
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
    const binding = bindThreadSpanBuffer(exports, handle);
    if (binding === undefined) throw new Error('failed to bind ThreadSpanBuffer handle');
    return binding;
  };

  const copyString = (
    reader: (handle: number, row: number, ptr: number, len: number) => number,
    binding: ThreadSpanBufferBinding,
    row: number,
  ): string => {
    const needed = reader(binding.handle, row, scratchPtr, scratchLen);
    if (needed === 0) return '';
    if (needed > scratchLen) {
      ensureScratch(needed);
      reader(binding.handle, row, scratchPtr, scratchLen);
    }
    return readUtf8(scratchPtr, needed);
  };

  return {
    memory,
    exports,
    writeUtf8,
    intern,
    createBinding,
    readUtf8,
    rowCount: (binding) => exports.thread_span_buffer_row_count(binding.handle),
    readTimestamp: (binding, row) => exports.thread_span_buffer_read_timestamp(binding.handle, row),
    readSpanId: (binding, row) => exports.thread_span_buffer_read_span_id(binding.handle, row),
    readHeader: (binding, row) => exports.thread_span_buffer_read_header(binding.handle, row),
    readParentSpanId: (binding, row) => exports.thread_span_buffer_read_parent_span_id(binding.handle, row),
    readLine: (binding, row) => exports.thread_span_buffer_read_line(binding.handle, row),
    readTraceId: (binding, row) => copyString(exports.thread_span_buffer_read_trace_id, binding, row),
    readMessage: (binding, row) => copyString(exports.thread_span_buffer_read_message, binding, row),
    materializeScope: (binding, startRow, rowCount) => {
      if (exports.thread_span_buffer_materialize_scope(binding.handle, startRow, rowCount) !== 0) {
        throw new Error('thread_span_buffer_materialize_scope failed');
      }
    },
    readAttr: (binding, row, ordinal) => {
      ensureScratch(16);
      const kindPtr = scratchPtr;
      const valuePtr = (scratchPtr + 8) & ~7;
      const status = exports.thread_span_buffer_read_attr(binding.handle, row, ordinal, kindPtr, valuePtr);
      if (status !== 0) return undefined;
      const view = new DataView(memory.buffer);
      return { kind: view.getUint8(kindPtr), value: view.getBigUint64(valuePtr, true) };
    },
    readInterned: (binding, ordinal) => {
      const needed = exports.thread_span_buffer_read_interned(binding.handle, ordinal, scratchPtr, scratchLen);
      if (needed === 0) return '';
      if (needed > scratchLen) {
        ensureScratch(needed);
        exports.thread_span_buffer_read_interned(binding.handle, ordinal, scratchPtr, scratchLen);
      }
      return readUtf8(scratchPtr, needed);
    },
  };
}
