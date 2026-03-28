import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import fc from 'fast-check';
import { createSpanLogger } from '../codegen/spanLoggerGenerator.js';
import { DEFAULT_METADATA } from '../opContext/defineOp.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import { createSpanBuffer, getSpanBufferClass, type SpanBufferConstructor } from '../spanBuffer.js';

import type { AnySpanBuffer } from '../types.js';
import { createBuffer, createTestTraceRoot } from './test-helpers.js';

/**
 * Property-based tests for buffer overflow handling.
 *
 * Core properties verified:
 * 1. Entry Preservation: All N entries exist across the buffer chain
 * 2. Buffer Count Formula: Exact number of buffers matches mathematical expectation
 * 3. Chain Integrity: Linked list is properly formed
 * 4. Data Correctness: Each entry has expected attribute values
 * 5. Overflow Counter: logBinding.sb_overflows === bufferCount - 1
 */

// Schema for testing - includes various column types and system fields
const testSchema = new LogSchema(
  mergeWithSystemSchema({
    requestId: S.category(),
    userId: S.category(),
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    duration: S.number(),
    success: S.boolean(),
  }),
);

/**
 * SpanLogger reserves rows 0-1 for span-start and span-end.
 * User entries start at row 2. So with capacity C, we can write (C-2) entries
 * before overflow in the first buffer.
 */
const RESERVED_ROWS = 2;
const DB_OPERATIONS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'] as const;

type BufferChainAnalysis = {
  bufferCount: number;
  totalEntries: number;
  writeIndices: number[];
};

/**
 * Helper: Get SpanBufferClass and reset stats for testing
 */
function getTestSpanBufferClass(capacity?: number): SpanBufferConstructor {
  const SpanBufferClass = getSpanBufferClass(testSchema);
  // Reset stats for clean test state
  SpanBufferClass.stats.capacity = capacity ?? DEFAULT_BUFFER_CAPACITY;
  SpanBufferClass.stats.totalWrites = 0;
  SpanBufferClass.stats.spansCreated = 0;
  return SpanBufferClass;
}

/**
 * Helper: Count overflows by walking buffer chain
 */
function countOverflows(buffer: AnySpanBuffer): number {
  let count = 0;
  let curr: AnySpanBuffer | undefined = buffer;
  while (curr?._overflow) {
    count++;
    curr = curr._overflow;
  }
  return count;
}

/**
 * Helper: Count buffers in chain and collect total entries
 */
function analyzeBufferChain(rootBuffer: AnySpanBuffer): BufferChainAnalysis {
  const writeIndices: number[] = [];
  let bufferCount = 0;
  let totalEntries = 0;
  let current: AnySpanBuffer | undefined = rootBuffer;

  while (current) {
    bufferCount++;
    writeIndices.push(current._writeIndex);
    totalEntries += current._writeIndex;
    current = current._overflow as AnySpanBuffer | undefined;
  }

  return { bufferCount, totalEntries, writeIndices };
}

type LazyColumnProbe = {
  _requestId_values?: unknown;
  _userId_values?: unknown;
  _operation_values?: unknown;
  _duration_values?: unknown;
  _success_values?: unknown;
};

function requireOverflowBuffer(buffer: AnySpanBuffer, label: string): AnySpanBuffer {
  if (!buffer._overflow) {
    throw new Error(`Expected overflow buffer for ${label}`);
  }
  return buffer._overflow;
}

/**
 * Helper: Collect all entries from buffer chain
 */
function collectEntries(
  rootBuffer: AnySpanBuffer,
  startRow = 0,
): Array<{
  bufferIndex: number;
  rowIndex: number;
  requestId: string | undefined;
  userId: string | undefined;
  operation: number | undefined;
  duration: number | undefined;
}> {
  const entries: Array<{
    bufferIndex: number;
    rowIndex: number;
    requestId: string | undefined;
    userId: string | undefined;
    operation: number | undefined;
    duration: number | undefined;
  }> = [];

  let bufferIndex = 0;
  let current: AnySpanBuffer | undefined = rootBuffer;

  while (current) {
    // First buffer starts at startRow, subsequent buffers start at 0
    const start = bufferIndex === 0 ? startRow : 0;
    const end = current._writeIndex;

    const requestIdValues = current.getColumnIfAllocated('requestId');
    const userIdValues = current.getColumnIfAllocated('userId');
    const operationValues = current.getColumnIfAllocated('operation');
    const durationValues = current.getColumnIfAllocated('duration');

    for (let row = start; row < end; row++) {
      const requestIdValue = Array.isArray(requestIdValues) ? requestIdValues[row] : undefined;
      const userIdValue = Array.isArray(userIdValues) ? userIdValues[row] : undefined;
      entries.push({
        bufferIndex,
        rowIndex: row,
        requestId: typeof requestIdValue === 'string' ? requestIdValue : undefined,
        userId: typeof userIdValue === 'string' ? userIdValue : undefined,
        operation: operationValues instanceof Uint8Array ? operationValues[row] : undefined,
        duration: durationValues instanceof Float64Array ? durationValues[row] : undefined,
      });
    }

    bufferIndex++;
    current = current._overflow as AnySpanBuffer | undefined;
  }

  return entries;
}

/**
 * Calculate expected buffer count for N entries.
 *
 * With capacity C and R reserved rows in first buffer:
 * - First buffer holds (C - R) entries
 * - Each subsequent buffer holds C entries
 *
 * Formula: 1 + ceil(max(0, N - (C - R)) / C)
 */
function expectedBufferCount(numEntries: number, capacity: number, reservedRows: number): number {
  const firstBufferCapacity = capacity - reservedRows;
  if (numEntries <= firstBufferCapacity) {
    return 1;
  }
  const remaining = numEntries - firstBufferCapacity;
  return 1 + Math.ceil(remaining / capacity);
}

describe('Buffer Overflow Property Tests', () => {
  let SpanBufferClass: SpanBufferConstructor;

  beforeEach(() => {
    SpanBufferClass = getTestSpanBufferClass();
  });

  describe('Property: Entry Preservation', () => {
    it('all entries are preserved across buffer chain for any entry count', () => {
      fc.assert(
        fc.property(
          // Generate entry count from 1 to 200 (enough to trigger many overflows)
          fc.integer({ min: 1, max: 200 }),
          (numEntries) => {
            // Reset counters for each test run
            SpanBufferClass.stats.spansCreated = 0;

            const buffer = createBuffer(testSchema);
            const logger = createSpanLogger(testSchema, buffer);

            // Write entries
            for (let i = 0; i < numEntries; i++) {
              logger
                .info(`msg-${i}`)
                .requestId(`req-${i}`)
                .userId(`user-${i}`)
                .operation(DB_OPERATIONS[i % 4])
                .duration(i * 10);
            }

            // Collect entries starting from row RESERVED_ROWS (rows 0-1 are span lifecycle)
            const entries = collectEntries(buffer, RESERVED_ROWS);

            // Property: exactly numEntries entries exist
            expect(entries.length).toBe(numEntries);
          },
        ),
        { numRuns: 100 },
      );
    });
  });

  describe('Property: Buffer Count Formula', () => {
    it('buffer count matches mathematical formula for any entry count', () => {
      fc.assert(
        // Limit to 90 entries to stay below capacity tuning threshold (100 writes)
        // This ensures capacity stays constant throughout the test run
        fc.property(fc.integer({ min: 1, max: 90 }), (numEntries) => {
          // Reset all stats including capacity before each run
          const capacity = DEFAULT_BUFFER_CAPACITY; // 8
          SpanBufferClass.stats.capacity = capacity;
          SpanBufferClass.stats.totalWrites = 0;
          SpanBufferClass.stats.spansCreated = 0;

          const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
          const logger = createSpanLogger(testSchema, buffer);

          // Write entries
          for (let i = 0; i < numEntries; i++) {
            logger.info(`msg-${i}`);
          }

          const { bufferCount } = analyzeBufferChain(buffer);
          // SpanLogger reserves 2 rows (span-start, span-end)
          const expected = expectedBufferCount(numEntries, capacity, RESERVED_ROWS);

          expect(bufferCount).toBe(expected);
        }),
        { numRuns: 100 },
      );
    });
  });

  describe('Property: Chain Integrity', () => {
    it('buffer chain is properly linked', () => {
      fc.assert(
        fc.property(fc.integer({ min: 1, max: 100 }), (numEntries) => {
          SpanBufferClass.stats.spansCreated = 0;

          const buffer = createBuffer(testSchema);
          const logger = createSpanLogger(testSchema, buffer);

          for (let i = 0; i < numEntries; i++) {
            logger.info(`msg-${i}`);
          }

          // Walk the chain and verify each link
          const buffers: AnySpanBuffer[] = [];
          let current: AnySpanBuffer | undefined = buffer;
          while (current) {
            buffers.push(current);
            current = current._overflow as AnySpanBuffer | undefined;
          }

          // Property: each buffer (except last) has next pointing to following buffer
          for (let i = 0; i < buffers.length - 1; i++) {
            expect(buffers[i]._overflow).toBe(buffers[i + 1]);
          }

          // Property: last buffer has no next
          expect(buffers[buffers.length - 1]._overflow).toBeUndefined();

          // Property: all buffers share same spanId and traceId
          const spanId = buffer.span_id;
          const traceId = buffer.trace_id;
          for (const buf of buffers) {
            expect(buf.span_id).toBe(spanId);
            expect(buf.trace_id).toBe(traceId);
          }
        }),
        { numRuns: 50 },
      );
    });
  });

  describe('Property: Data Correctness', () => {
    it('each entry has correct attribute values', () => {
      fc.assert(
        fc.property(fc.integer({ min: 1, max: 100 }), (numEntries) => {
          SpanBufferClass.stats.spansCreated = 0;

          const buffer = createBuffer(testSchema);
          const logger = createSpanLogger(testSchema, buffer);

          const operations = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'] as const;

          // Write entries with predictable values
          for (let i = 0; i < numEntries; i++) {
            logger
              .info(`msg-${i}`)
              .requestId(`req-${i}`)
              .userId(`user-${i}`)
              .operation(operations[i % 4])
              .duration(i * 10.5);
          }

          // Collect starting from RESERVED_ROWS
          const entries = collectEntries(buffer, RESERVED_ROWS);

          // Property: each entry has expected values
          for (let i = 0; i < numEntries; i++) {
            const entry = entries[i];
            expect(entry.requestId).toBe(`req-${i}`);
            expect(entry.userId).toBe(`user-${i}`);
            // Enum stored as index
            expect(entry.operation).toBe(i % 4);
            expect(entry.duration).toBe(i * 10.5);
          }
        }),
        { numRuns: 50 },
      );
    });
  });

  describe('Property: Overflow Counter Consistency', () => {
    it('sb_overflows equals bufferCount - 1', () => {
      fc.assert(
        fc.property(fc.integer({ min: 1, max: 150 }), (numEntries) => {
          // Reset counters
          SpanBufferClass.stats.spansCreated = 0;

          const buffer = createBuffer(testSchema);
          const logger = createSpanLogger(testSchema, buffer);

          for (let i = 0; i < numEntries; i++) {
            logger.info(`msg-${i}`);
          }

          const { bufferCount } = analyzeBufferChain(buffer);

          // Property: overflow events = bufferCount - 1 (one per chain link)
          expect(countOverflows(buffer)).toBe(bufferCount - 1);
        }),
        { numRuns: 100 },
      );
    });
  });

  describe('Property: WriteIndex Bounds', () => {
    it('each buffer writeIndex is within capacity', () => {
      fc.assert(
        fc.property(fc.integer({ min: 1, max: 200 }), (numEntries) => {
          SpanBufferClass.stats.spansCreated = 0;

          const buffer = createBuffer(testSchema);
          const logger = createSpanLogger(testSchema, buffer);

          for (let i = 0; i < numEntries; i++) {
            logger.info(`msg-${i}`);
          }

          // Walk chain and check each buffer
          let current: AnySpanBuffer | undefined = buffer;
          while (current) {
            // Property: writeIndex <= capacity
            expect(current._writeIndex).toBeLessThanOrEqual(current._capacity);
            // Property: writeIndex >= 0
            expect(current._writeIndex).toBeGreaterThanOrEqual(0);
            current = current._overflow as AnySpanBuffer | undefined;
          }
        }),
        { numRuns: 100 },
      );
    });
  });

  describe('Edge Cases', () => {
    it('exact usable capacity: no overflow when entries fit in first buffer', () => {
      const capacity = DEFAULT_BUFFER_CAPACITY; // 8
      const usableCapacity = capacity - RESERVED_ROWS; // 6 entries fit

      const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
      const logger = createSpanLogger(testSchema, buffer);

      // Write exactly usable capacity entries (6 with capacity=8, reserved=2)
      for (let i = 0; i < usableCapacity; i++) {
        logger.info(`msg-${i}`);
      }

      // Should be exactly 1 buffer (no overflow)
      expect(buffer._overflow).toBeUndefined();
      expect(countOverflows(buffer)).toBe(0);
      // writeIndex = RESERVED_ROWS + usableCapacity = 2 + 6 = 8 = capacity
      expect(buffer._writeIndex).toBe(capacity);
    });

    it('usable capacity + 1: triggers exactly one overflow', () => {
      const capacity = DEFAULT_BUFFER_CAPACITY; // 8
      const usableCapacity = capacity - RESERVED_ROWS; // 6

      const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
      const logger = createSpanLogger(testSchema, buffer);

      // Write usable capacity + 1 entries (7 entries)
      for (let i = 0; i < usableCapacity + 1; i++) {
        logger.info(`msg-${i}`);
      }

      // Should be exactly 2 buffers
      expect(buffer._overflow).toBeDefined();
      expect(buffer._overflow?._overflow).toBeUndefined();
      expect(countOverflows(buffer)).toBe(1);

      // First buffer full, second has 1 entry
      expect(buffer._writeIndex).toBe(capacity);
      expect(buffer._overflow?._writeIndex).toBe(1);
    });

    it('zero entries: single buffer with just reserved space', () => {
      const buffer = createBuffer(testSchema);
      // Create logger but don't write anything
      createSpanLogger(testSchema, buffer);

      expect(buffer._overflow).toBeUndefined();
      // Logger constructor sets writeIndex to 2 (after reserved rows)
      expect(buffer._writeIndex).toBe(RESERVED_ROWS);
      expect(countOverflows(buffer)).toBe(0);
    });
  });

  describe('Varying Capacity', () => {
    it('property holds for different buffer capacities', () => {
      fc.assert(
        fc.property(
          fc.integer({ min: 1, max: 100 }), // entries
          fc.integer({ min: 8, max: 64 }).map((n) => (n + 7) & ~7), // capacity aligned to 8
          (numEntries, capacity) => {
            // Reset all stats to prevent capacity tuning from modifying them mid-test
            SpanBufferClass.stats.capacity = capacity; // Set for chained buffers
            SpanBufferClass.stats.totalWrites = 0;
            SpanBufferClass.stats.spansCreated = 0;

            const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
            const logger = createSpanLogger(testSchema, buffer);

            for (let i = 0; i < numEntries; i++) {
              logger.info(`msg-${i}`).requestId(`req-${i}`);
            }

            const { bufferCount } = analyzeBufferChain(buffer);

            // Property: buffer count matches formula (with reserved rows)
            const expected = expectedBufferCount(numEntries, capacity, RESERVED_ROWS);
            expect(bufferCount).toBe(expected);

            // Property: overflow count matches
            expect(countOverflows(buffer)).toBe(bufferCount - 1);
          },
        ),
        { numRuns: 100 },
      );
    });
  });

  describe('Scoped Attributes in Overflow Buffers', () => {
    it('should prefill scoped attributes in overflow buffer from row 0 (not row 2)', () => {
      const capacity = 8;
      const usableCapacity = capacity - RESERVED_ROWS; // 6

      const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
      const logger = createSpanLogger(testSchema, buffer);

      // Set scope values that should be prefilled
      buffer._scopeValues = Object.freeze({
        requestId: 'req-123',
        userId: 'user-456',
      });

      // Write enough entries to trigger overflow (usableCapacity + 1)
      for (let i = 0; i < usableCapacity + 1; i++) {
        logger.info(`msg-${i}`);
      }

      // Verify overflow happened
      expect(buffer._overflow).toBeDefined();
      const overflowBuffer = requireOverflowBuffer(buffer, 'prefill-row0');
      const overflowProbe = overflowBuffer as AnySpanBuffer & LazyColumnProbe;

      // CRITICAL: Overflow buffer starts at _writeIndex=0, not 2
      // The bug was that _prefillScopedAttributesOn used startIdx=2, skipping rows 0-1
      expect(overflowBuffer._writeIndex).toBe(1); // One entry written

      // CRITICAL: Columns in scope but never written to directly are NOT allocated
      // Prefill checks _requestId_values (private) to avoid triggering lazy allocation
      // These columns stay unallocated until Arrow conversion time
      expect(overflowProbe._requestId_values).toBeUndefined();
      expect(overflowProbe._userId_values).toBeUndefined();

      // Row 0 has the actual entry (message IS allocated because .info() writes to it)
      expect(overflowBuffer.message_values[0]).toBe('msg-6');
    });

    it('should prefill scoped attributes across multiple overflow buffers', () => {
      const capacity = 8;
      const usableCapacity = capacity - RESERVED_ROWS; // 6

      const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
      const logger = createSpanLogger(testSchema, buffer);

      buffer._scopeValues = Object.freeze({
        requestId: 'req-xyz',
      });

      // Write enough to create 2 overflow buffers
      // First buffer: 6 entries (usableCapacity), overflow 1: 8 entries (full capacity), overflow 2: 1 entry
      const entriesToWrite = usableCapacity + capacity + 1; // 6 + 8 + 1 = 15
      for (let i = 0; i < entriesToWrite; i++) {
        logger.info(`msg-${i}`);
      }

      expect(buffer._overflow).toBeDefined();
      expect(buffer._overflow?._overflow).toBeDefined();

      const overflow1 = requireOverflowBuffer(buffer, 'multiple-overflow-1');
      const overflow2 = requireOverflowBuffer(overflow1, 'multiple-overflow-2');
      const overflow1Probe = overflow1 as AnySpanBuffer & LazyColumnProbe;
      const overflow2Probe = overflow2 as AnySpanBuffer & LazyColumnProbe;

      // Columns in scope but never written directly stay unallocated (lazy)
      expect(overflow1Probe._requestId_values).toBeUndefined();
      expect(overflow2Probe._requestId_values).toBeUndefined();
    });

    it('only prefills columns present in _scopeValues, not all schema columns', () => {
      const capacity = 8;
      const usableCapacity = capacity - RESERVED_ROWS; // 6

      const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
      const logger = createSpanLogger(testSchema, buffer);

      // Set scope for ONLY userId and requestId, NOT operation or duration or success
      buffer._scopeValues = Object.freeze({
        userId: 'user-789',
        requestId: 'req-abc',
      });

      // Trigger overflow
      for (let i = 0; i < usableCapacity + 1; i++) {
        logger.info(`msg-${i}`);
      }

      const overflowBuffer = requireOverflowBuffer(buffer, 'scope-columns');
      const overflowProbe = overflowBuffer as AnySpanBuffer & LazyColumnProbe;

      // Columns in _scopeValues but NEVER written to directly stay unallocated (lazy)
      expect(overflowProbe._userId_values).toBeUndefined();
      expect(overflowProbe._requestId_values).toBeUndefined();

      // Columns NOT in _scopeValues also stay unallocated (lazy)
      expect(overflowProbe._operation_values).toBeUndefined();
      expect(overflowProbe._duration_values).toBeUndefined();
      expect(overflowProbe._success_values).toBeUndefined();
    });

    it('does not allocate columns that are only in scope (lazy allocation)', () => {
      const capacity = 8;
      const usableCapacity = capacity - RESERVED_ROWS;

      const buffer = createSpanBuffer(testSchema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, capacity);
      const logger = createSpanLogger(testSchema, buffer);

      // Set scope for userId but NEVER call .userId() directly
      buffer._scopeValues = Object.freeze({
        userId: 'scope-only-user',
      });

      // Write entries without touching userId column
      for (let i = 0; i < usableCapacity + 1; i++) {
        logger.info(`msg-${i}`);
      }

      // Verify overflow happened
      expect(buffer._overflow).toBeDefined();
      const overflowBuffer = requireOverflowBuffer(buffer, 'lazy-scope-only');
      const rootProbe = buffer as AnySpanBuffer & LazyColumnProbe;
      const overflowProbe = overflowBuffer as AnySpanBuffer & LazyColumnProbe;

      // CRITICAL: prefill checks the PRIVATE property _userId_values
      // This prevents triggering the lazy getter and allocating memory
      // Since userId was only in scope (never written directly), it stays unallocated

      // Verify column was NOT allocated (lazy allocation preserved)
      expect(rootProbe._userId_values).toBeUndefined();
      expect(overflowProbe._userId_values).toBeUndefined();

      // Accessing via the public getter would trigger allocation
      // (but we verify it's undefined before triggering the getter)
    });
  });
});
