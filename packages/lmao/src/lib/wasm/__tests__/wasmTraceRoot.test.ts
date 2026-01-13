import { beforeEach, describe, expect, it } from 'bun:test';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_ERR,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_START,
  ENTRY_TYPE_WARN,
} from '../../schema/systemSchema.js';
import type { TraceId } from '../../traceId.js';
import type { TracerLifecycleHooks } from '../../traceRoot.js';
import { createWasmAllocator, type WasmAllocator } from '../wasmAllocator.js';
import { createWasmTraceRoot } from '../wasmTraceRoot.js';

/**
 * Mock tracer that implements TracerLifecycleHooks for testing.
 */
const mockTracer: TracerLifecycleHooks = {
  onTraceStart: () => {},
  onTraceEnd: () => {},
  onSpanStart: () => {},
  onSpanEnd: () => {},
  onStatsWillResetFor: () => {},
  bufferStrategy: {
    createChildSpanBuffer: () => ({}) as any,
    createOverflowBuffer: () => ({}) as any,
  },
};

describe('WasmTraceRoot', () => {
  let allocator: WasmAllocator;

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: 64 });
  });

  describe('construction', () => {
    it('allocates TraceRoot memory and initializes timestamps', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);

      expect(traceRoot._traceRootPtr).toBeGreaterThan(0);
      expect(traceRoot.trace_id).toBe('test-trace-id' as TraceId);
      expect(traceRoot.tracer).toBe(mockTracer);
      expect(traceRoot.allocator).toBe(allocator);
    });

    it('initializes anchorEpochNanos to a valid timestamp', () => {
      const before = Date.now();
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const after = Date.now();

      // anchorEpochNanos is in nanoseconds, Date.now() is in milliseconds
      const anchorMs = Number(traceRoot.anchorEpochNanos / 1_000_000n);

      expect(anchorMs).toBeGreaterThanOrEqual(before);
      expect(anchorMs).toBeLessThanOrEqual(after + 1); // +1 for timing tolerance
    });

    it('initializes anchorPerfNow to a valid performance.now() value', () => {
      const before = performance.now();
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const after = performance.now();

      expect(traceRoot.anchorPerfNow).toBeGreaterThanOrEqual(before);
      expect(traceRoot.anchorPerfNow).toBeLessThanOrEqual(after);
    });
  });

  describe('getTimestampNanos', () => {
    it('returns valid nanosecond timestamp', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);

      const timestamp = traceRoot.getTimestampNanos();

      // Should be a positive bigint
      expect(typeof timestamp).toBe('bigint');
      expect(timestamp).toBeGreaterThan(0n);
    });

    it('returns increasing timestamps', async () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);

      const ts1 = traceRoot.getTimestampNanos();
      // Small delay to ensure timestamp increases
      await new Promise((resolve) => setTimeout(resolve, 1));
      const ts2 = traceRoot.getTimestampNanos();

      expect(ts2).toBeGreaterThan(ts1);
    });

    it('returns timestamp close to current time', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);

      const now = Date.now();
      const timestamp = traceRoot.getTimestampNanos();
      const timestampMs = Number(timestamp / 1_000_000n);

      // Should be within 100ms of Date.now()
      expect(Math.abs(timestampMs - now)).toBeLessThan(100);
    });
  });

  describe('writeSpanStartPtr (low-level pointer API)', () => {
    it('sets entry_type[0] = SPAN_START (1)', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);

      expect(allocator.readEntryType(systemPtr, 0)).toBe(ENTRY_TYPE_SPAN_START);
    });

    it('sets entry_type[1] = SPAN_EXCEPTION (4) for crash safety', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);

      expect(allocator.readEntryType(systemPtr, 1)).toBe(ENTRY_TYPE_SPAN_EXCEPTION);
    });

    it('sets _writeIndex to 2', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);

      expect(traceRoot.readWriteIndex(identityPtr)).toBe(2);
    });

    it('writes a valid timestamp at row 0', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);

      const timestamp = allocator.readTimestamp(systemPtr, 0);
      expect(timestamp).toBeGreaterThan(0n);
    });
  });

  describe('writeSpanEndOkPtr (low-level pointer API)', () => {
    it('sets entry_type[1] = SPAN_OK (2)', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      traceRoot.writeSpanEndOkPtr(systemPtr);

      expect(allocator.readEntryType(systemPtr, 1)).toBe(ENTRY_TYPE_SPAN_OK);
    });

    it('writes a valid timestamp at row 1', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      traceRoot.writeSpanEndOkPtr(systemPtr);

      const timestamp = allocator.readTimestamp(systemPtr, 1);
      expect(timestamp).toBeGreaterThan(0n);
    });

    it('writes timestamp >= span start timestamp', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      const startTimestamp = allocator.readTimestamp(systemPtr, 0);

      traceRoot.writeSpanEndOkPtr(systemPtr);
      const endTimestamp = allocator.readTimestamp(systemPtr, 1);

      expect(endTimestamp).toBeGreaterThanOrEqual(startTimestamp);
    });
  });

  describe('writeSpanEndErrPtr (low-level pointer API)', () => {
    it('sets entry_type[1] = SPAN_ERR (3)', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      traceRoot.writeSpanEndErrPtr(systemPtr);

      expect(allocator.readEntryType(systemPtr, 1)).toBe(ENTRY_TYPE_SPAN_ERR);
    });

    it('writes a valid timestamp at row 1', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      traceRoot.writeSpanEndErrPtr(systemPtr);

      const timestamp = allocator.readTimestamp(systemPtr, 1);
      expect(timestamp).toBeGreaterThan(0n);
    });
  });

  describe('writeLogEntryPtr (low-level pointer API)', () => {
    it('returns correct row index starting at 2', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      const rowIdx = traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_INFO);

      expect(rowIdx).toBe(2);
    });

    it('increments write index for each log entry', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);

      const idx1 = traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_INFO);
      const idx2 = traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_DEBUG);
      const idx3 = traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_WARN);

      expect(idx1).toBe(2);
      expect(idx2).toBe(3);
      expect(idx3).toBe(4);
      expect(traceRoot.readWriteIndex(identityPtr)).toBe(5);
    });

    it('writes correct entry type', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_INFO);
      traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_DEBUG);
      traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_WARN);

      expect(allocator.readEntryType(systemPtr, 2)).toBe(ENTRY_TYPE_INFO);
      expect(allocator.readEntryType(systemPtr, 3)).toBe(ENTRY_TYPE_DEBUG);
      expect(allocator.readEntryType(systemPtr, 4)).toBe(ENTRY_TYPE_WARN);
    });

    it('writes valid timestamps for log entries', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_INFO);

      const timestamp = allocator.readTimestamp(systemPtr, 2);
      expect(timestamp).toBeGreaterThan(0n);
    });
  });

  describe('readWriteIndex', () => {
    it('returns 2 after writeSpanStartPtr', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);

      expect(traceRoot.readWriteIndex(identityPtr)).toBe(2);
    });

    it('reflects log entries written', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);
      const systemPtr = allocator.allocSpanSystem();
      const identityPtr = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr, identityPtr);
      expect(traceRoot.readWriteIndex(identityPtr)).toBe(2);

      traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_INFO);
      expect(traceRoot.readWriteIndex(identityPtr)).toBe(3);

      traceRoot.writeLogEntryPtr(systemPtr, identityPtr, ENTRY_TYPE_DEBUG);
      expect(traceRoot.readWriteIndex(identityPtr)).toBe(4);
    });
  });

  describe('free', () => {
    it('returns memory to allocator', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);

      const freeCountBefore = allocator.getFreeCount();
      traceRoot.free();
      const freeCountAfter = allocator.getFreeCount();

      expect(freeCountAfter).toBe(freeCountBefore + 1);
    });

    it('allows memory to be reused after free', () => {
      const traceRoot1 = createWasmTraceRoot(allocator, 'test-trace-id-1', mockTracer);
      const ptr1 = traceRoot1._traceRootPtr;

      traceRoot1.free();

      // Creating a new trace root should reuse the freed memory
      const traceRoot2 = createWasmTraceRoot(allocator, 'test-trace-id-2', mockTracer);
      expect(traceRoot2._traceRootPtr).toBe(ptr1);
    });
  });

  describe('multiple spans with same TraceRoot', () => {
    it('can write to multiple system blocks', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);

      const systemPtr1 = allocator.allocSpanSystem();
      const identityPtr1 = allocator.allocIdentityChild();
      const systemPtr2 = allocator.allocSpanSystem();
      const identityPtr2 = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr1, identityPtr1);
      traceRoot.writeSpanStartPtr(systemPtr2, identityPtr2);

      expect(allocator.readEntryType(systemPtr1, 0)).toBe(ENTRY_TYPE_SPAN_START);
      expect(allocator.readEntryType(systemPtr2, 0)).toBe(ENTRY_TYPE_SPAN_START);
      expect(traceRoot.readWriteIndex(identityPtr1)).toBe(2);
      expect(traceRoot.readWriteIndex(identityPtr2)).toBe(2);
    });

    it('maintains separate state for each span buffer', () => {
      const traceRoot = createWasmTraceRoot(allocator, 'test-trace-id', mockTracer);

      const systemPtr1 = allocator.allocSpanSystem();
      const identityPtr1 = allocator.allocIdentityChild();
      const systemPtr2 = allocator.allocSpanSystem();
      const identityPtr2 = allocator.allocIdentityChild();

      traceRoot.writeSpanStartPtr(systemPtr1, identityPtr1);
      traceRoot.writeSpanStartPtr(systemPtr2, identityPtr2);

      // Write log entries to first span
      traceRoot.writeLogEntryPtr(systemPtr1, identityPtr1, ENTRY_TYPE_INFO);
      traceRoot.writeLogEntryPtr(systemPtr1, identityPtr1, ENTRY_TYPE_DEBUG);

      // Write to second span
      traceRoot.writeLogEntryPtr(systemPtr2, identityPtr2, ENTRY_TYPE_WARN);

      // Complete spans differently
      traceRoot.writeSpanEndOkPtr(systemPtr1);
      traceRoot.writeSpanEndErrPtr(systemPtr2);

      // Verify independent state
      expect(traceRoot.readWriteIndex(identityPtr1)).toBe(4); // 2 + 2 log entries
      expect(traceRoot.readWriteIndex(identityPtr2)).toBe(3); // 2 + 1 log entry
      expect(allocator.readEntryType(systemPtr1, 1)).toBe(ENTRY_TYPE_SPAN_OK);
      expect(allocator.readEntryType(systemPtr2, 1)).toBe(ENTRY_TYPE_SPAN_ERR);
    });
  });

  describe('entry type constants verification', () => {
    it('SPAN_START = 1', () => {
      expect(ENTRY_TYPE_SPAN_START).toBe(1);
    });

    it('SPAN_OK = 2', () => {
      expect(ENTRY_TYPE_SPAN_OK).toBe(2);
    });

    it('SPAN_ERR = 3', () => {
      expect(ENTRY_TYPE_SPAN_ERR).toBe(3);
    });

    it('SPAN_EXCEPTION = 4', () => {
      expect(ENTRY_TYPE_SPAN_EXCEPTION).toBe(4);
    });
  });
});
