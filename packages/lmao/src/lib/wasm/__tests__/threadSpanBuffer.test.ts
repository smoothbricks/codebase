import { describe, expect, it } from 'vitest';
import {
  bindThreadSpanBuffer,
  isThreadSpanBufferWasmExports,
  THREAD_SPAN_BUFFER_OK,
  type ThreadSpanBufferWasmExports,
} from '../threadSpanBuffer.js';

describe('thread span buffer ABI', () => {
  it('forwards the opaque handle and preserves every row-write argument', () => {
    const calls: string[] = [];
    const stub: ThreadSpanBufferWasmExports = {
      thread_span_buffer_new: (threadId, capacity) => {
        calls.push(`new:${threadId}:${capacity}`);
        return 17;
      },
      thread_span_buffer_free: (handle) => {
        calls.push(`free:${handle}`);
      },
      thread_span_buffer_open_span: (
        handle,
        tracePtr,
        traceLen,
        parentThreadId,
        parentSpanId,
        nameVocab,
        timestamp,
        line,
      ) => {
        calls.push(
          `open:${handle}:${tracePtr}:${traceLen}:${parentThreadId}:${parentSpanId}:${nameVocab}:${timestamp}:${line}`,
        );
        return (3n << 32n) | 2n;
      },
      thread_span_buffer_open_span_dynamic: (
        handle,
        tracePtr,
        traceLen,
        parentThreadId,
        parentSpanId,
        namePtr,
        nameLen,
        timestamp,
        line,
      ) => {
        calls.push(
          `open-dynamic:${handle}:${tracePtr}:${traceLen}:${parentThreadId}:${parentSpanId}:${namePtr}:${nameLen}:${timestamp}:${line}`,
        );
        return (3n << 32n) | 2n;
      },
      thread_span_buffer_end_ok: (handle, spanId, timestamp) => {
        calls.push(`end-ok:${handle}:${spanId}:${timestamp}`);
        return THREAD_SPAN_BUFFER_OK;
      },
      thread_span_buffer_end_err: (handle, spanId, timestamp) => {
        calls.push(`end-err:${handle}:${spanId}:${timestamp}`);
        return THREAD_SPAN_BUFFER_OK;
      },
      thread_span_buffer_append_log: (handle, spanId, entryType, messageVocab, timestamp, line) => {
        calls.push(`append:${handle}:${spanId}:${entryType}:${messageVocab}:${timestamp}:${line}`);
        return (3n << 32n) | 4n;
      },
      thread_span_buffer_append_log_dynamic: (handle, spanId, entryType, messagePtr, messageLen, timestamp, line) => {
        calls.push(`append-dynamic:${handle}:${spanId}:${entryType}:${messagePtr}:${messageLen}:${timestamp}:${line}`);
        return (3n << 32n) | 4n;
      },
      thread_span_buffer_write_attr: (handle, row, ordinal, kind, value) => {
        calls.push(`attr:${handle}:${row}:${ordinal}:${kind}:${value}`);
        return THREAD_SPAN_BUFFER_OK;
      },
      thread_span_buffer_write_tag: (handle, spanId, ordinal, kind, value) => {
        calls.push(`tag:${handle}:${spanId}:${ordinal}:${kind}:${value}`);
        return THREAD_SPAN_BUFFER_OK;
      },
      thread_span_buffer_set_scope: (handle, spanId, ordinal, kind, value) => {
        calls.push(`scope:${handle}:${spanId}:${ordinal}:${kind}:${value}`);
        return THREAD_SPAN_BUFFER_OK;
      },
      thread_span_buffer_intern: (handle, ptr, len) => {
        calls.push(`intern:${handle}:${ptr}:${len}`);
        return 9;
      },
    };

    expect(isThreadSpanBufferWasmExports(stub)).toBe(true);
    const handle = stub.thread_span_buffer_new(7n, 64);
    const binding = bindThreadSpanBuffer(stub, handle);
    expect(binding).toBeDefined();
    if (!binding) return;

    expect(binding.openSpan(100, 4, 0n, 0, 11, 10n, 8)).toBe((3n << 32n) | 2n);
    expect(binding.openSpanDynamic(100, 4, 7n, 3, 200, 5, 20n, 12)).toBe((3n << 32n) | 2n);
    expect(binding.endOk(3, 30n)).toBe(THREAD_SPAN_BUFFER_OK);
    expect(binding.endErr(3, 40n)).toBe(THREAD_SPAN_BUFFER_OK);
    expect(binding.appendLog(3, 5, 12, 50n, 16)).toBe((3n << 32n) | 4n);
    expect(binding.appendLogDynamic(3, 5, 200, 5, 60n, 20)).toBe((3n << 32n) | 4n);
    expect(binding.writeAttr(2, 12, 2, 99n)).toBe(THREAD_SPAN_BUFFER_OK);
    expect(binding.writeTag(3, 13, 3, 100n)).toBe(THREAD_SPAN_BUFFER_OK);
    expect(binding.setScope(3, 14, 4, 101n)).toBe(THREAD_SPAN_BUFFER_OK);
    expect(binding.intern(200, 5)).toBe(9);
    binding.free();

    expect(calls).toEqual([
      'new:7:64',
      'open:17:100:4:0:0:11:10:8',
      'open-dynamic:17:100:4:7:3:200:5:20:12',
      'end-ok:17:3:30',
      'end-err:17:3:40',
      'append:17:3:5:12:50:16',
      'append-dynamic:17:3:5:200:5:60:20',
      'attr:17:2:12:2:99',
      'tag:17:3:13:3:100',
      'scope:17:3:14:4:101',
      'intern:17:200:5',
      'free:17',
    ]);
  });

  it('rejects an incomplete export object before binding', () => {
    expect(bindThreadSpanBuffer({}, 1)).toBeUndefined();
    expect(isThreadSpanBufferWasmExports({})).toBe(false);
  });
});
