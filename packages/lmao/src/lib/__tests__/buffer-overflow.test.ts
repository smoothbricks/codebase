import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import fc from 'fast-check';
import { createSpanLogger } from '../codegen/spanLoggerGenerator.js';
import { ModuleContext } from '../moduleContext.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import { createNextBuffer, createSpanBuffer } from '../spanBuffer.js';
import type { SpanBuffer } from '../types.js';

/**
 * Property-based tests for buffer overflow handling.
 *
 * Core properties verified:
 * 1. Entry Preservation: All N entries exist across the buffer chain
 * 2. Buffer Count Formula: Exact number of buffers matches mathematical expectation
 * 3. Chain Integrity: Linked list is properly formed
 * 4. Data Correctness: Each entry has expected attribute values
 * 5. Overflow Counter: module.sb_overflows === bufferCount - 1
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

/**
 * Helper: Create a ModuleContext for testing
 */
function createTestModuleContext(): ModuleContext {
  return new ModuleContext('test-sha', '@test/overflow', 'src/overflow.ts', testSchema);
}

/**
 * Helper: Count buffers in chain and collect total entries
 */
function analyzeBufferChain(rootBuffer: SpanBuffer): {
  bufferCount: number;
  totalEntries: number;
  writeIndices: number[];
} {
  const writeIndices: number[] = [];
  let bufferCount = 0;
  let totalEntries = 0;
  let current: SpanBuffer | undefined = rootBuffer;

  while (current) {
    bufferCount++;
    writeIndices.push(current._writeIndex);
    totalEntries += current._writeIndex;
    current = current._next as SpanBuffer | undefined;
  }

  return { bufferCount, totalEntries, writeIndices };
}

/**
 * Helper: Collect all entries from buffer chain
 */
function collectEntries(
  rootBuffer: SpanBuffer,
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
  let current: SpanBuffer | undefined = rootBuffer;

  while (current) {
    // First buffer starts at startRow, subsequent buffers start at 0
    const start = bufferIndex === 0 ? startRow : 0;
    const end = current._writeIndex;

    for (let row = start; row < end; row++) {
      entries.push({
        bufferIndex,
        rowIndex: row,
        requestId: (current as unknown as Record<string, unknown[]>).requestId_values?.[row] as string | undefined,
        userId: (current as unknown as Record<string, unknown[]>).userId_values?.[row] as string | undefined,
        operation: (current as unknown as Record<string, unknown[]>).operation_values?.[row] as number | undefined,
        duration: (current as unknown as Record<string, unknown[]>).duration_values?.[row] as number | undefined,
      });
    }

    bufferIndex++;
    current = current._next as SpanBuffer | undefined;
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
  let module: ModuleContext;

  beforeEach(() => {
    module = createTestModuleContext();
    // Reset overflow counters
    module.sb_overflows = 0;
    module.sb_overflowWrites = 0;
    module.sb_totalWrites = 0;
    module.sb_totalCreated = 0;
  });

  describe('Property: Entry Preservation', () => {
    it('all entries are preserved across buffer chain for any entry count', () => {
      fc.assert(
        fc.property(
          // Generate entry count from 1 to 200 (enough to trigger many overflows)
          fc.integer({ min: 1, max: 200 }),
          (numEntries) => {
            // Reset counters for each test run
            module.sb_overflows = 0;
            module.sb_totalCreated = 0;

            const buffer = createSpanBuffer(testSchema, module, 'test-span');
            const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

            // Write entries
            for (let i = 0; i < numEntries; i++) {
              logger
                .info(`msg-${i}`)
                .requestId(`req-${i}`)
                .userId(`user-${i}`)
                .operation(['SELECT', 'INSERT', 'UPDATE', 'DELETE'][i % 4] as 'SELECT')
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
        fc.property(fc.integer({ min: 1, max: 200 }), (numEntries) => {
          module.sb_overflows = 0;
          module.sb_totalCreated = 0;

          const capacity = DEFAULT_BUFFER_CAPACITY; // 8
          const buffer = createSpanBuffer(testSchema, module, 'test-span', undefined, capacity);
          const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

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
          module.sb_overflows = 0;
          module.sb_totalCreated = 0;

          const buffer = createSpanBuffer(testSchema, module, 'test-span');
          const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

          for (let i = 0; i < numEntries; i++) {
            logger.info(`msg-${i}`);
          }

          // Walk the chain and verify each link
          const buffers: SpanBuffer[] = [];
          let current: SpanBuffer | undefined = buffer;
          while (current) {
            buffers.push(current);
            current = current._next as SpanBuffer | undefined;
          }

          // Property: each buffer (except last) has next pointing to following buffer
          for (let i = 0; i < buffers.length - 1; i++) {
            expect(buffers[i]._next).toBe(buffers[i + 1]);
          }

          // Property: last buffer has no next
          expect(buffers[buffers.length - 1]._next).toBeUndefined();

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
          module.sb_overflows = 0;
          module.sb_totalCreated = 0;

          const buffer = createSpanBuffer(testSchema, module, 'test-span');
          const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

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
          module.sb_overflows = 0;
          module.sb_totalCreated = 0;

          const buffer = createSpanBuffer(testSchema, module, 'test-span');
          const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

          for (let i = 0; i < numEntries; i++) {
            logger.info(`msg-${i}`);
          }

          const { bufferCount } = analyzeBufferChain(buffer);

          // Property: overflow events = bufferCount - 1 (one per chain link)
          expect(module.sb_overflows).toBe(bufferCount - 1);
        }),
        { numRuns: 100 },
      );
    });
  });

  describe('Property: WriteIndex Bounds', () => {
    it('each buffer writeIndex is within capacity', () => {
      fc.assert(
        fc.property(fc.integer({ min: 1, max: 200 }), (numEntries) => {
          module.sb_overflows = 0;
          module.sb_totalCreated = 0;

          const buffer = createSpanBuffer(testSchema, module, 'test-span');
          const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

          for (let i = 0; i < numEntries; i++) {
            logger.info(`msg-${i}`);
          }

          // Walk chain and check each buffer
          let current: SpanBuffer | undefined = buffer;
          while (current) {
            // Property: writeIndex <= capacity
            expect(current._writeIndex).toBeLessThanOrEqual(current._capacity);
            // Property: writeIndex >= 0
            expect(current._writeIndex).toBeGreaterThanOrEqual(0);
            current = current._next as SpanBuffer | undefined;
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
      module.sb_overflows = 0;

      const buffer = createSpanBuffer(testSchema, module, 'test-span', undefined, capacity);
      const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

      // Write exactly usable capacity entries (6 with capacity=8, reserved=2)
      for (let i = 0; i < usableCapacity; i++) {
        logger.info(`msg-${i}`);
      }

      // Should be exactly 1 buffer (no overflow)
      expect(buffer._next).toBeUndefined();
      expect(module.sb_overflows).toBe(0);
      // writeIndex = RESERVED_ROWS + usableCapacity = 2 + 6 = 8 = capacity
      expect(buffer._writeIndex).toBe(capacity);
    });

    it('usable capacity + 1: triggers exactly one overflow', () => {
      const capacity = DEFAULT_BUFFER_CAPACITY; // 8
      const usableCapacity = capacity - RESERVED_ROWS; // 6
      module.sb_overflows = 0;

      const buffer = createSpanBuffer(testSchema, module, 'test-span', undefined, capacity);
      const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

      // Write usable capacity + 1 entries (7 entries)
      for (let i = 0; i < usableCapacity + 1; i++) {
        logger.info(`msg-${i}`);
      }

      // Should be exactly 2 buffers
      expect(buffer._next).toBeDefined();
      expect(buffer._next?._next).toBeUndefined();
      expect(module.sb_overflows).toBe(1);

      // First buffer full, second has 1 entry
      expect(buffer._writeIndex).toBe(capacity);
      expect(buffer._next?._writeIndex).toBe(1);
    });

    it('zero entries: single buffer with just reserved space', () => {
      module.sb_overflows = 0;

      const buffer = createSpanBuffer(testSchema, module, 'test-span');
      // Create logger but don't write anything
      createSpanLogger(testSchema, buffer, createNextBuffer);

      expect(buffer._next).toBeUndefined();
      // Logger constructor sets writeIndex to 2 (after reserved rows)
      expect(buffer._writeIndex).toBe(RESERVED_ROWS);
      expect(module.sb_overflows).toBe(0);
    });
  });

  describe('Varying Capacity', () => {
    it('property holds for different buffer capacities', () => {
      fc.assert(
        fc.property(
          fc.integer({ min: 1, max: 100 }), // entries
          fc
            .integer({ min: 8, max: 64 })
            .map((n) => (n + 7) & ~7), // capacity aligned to 8
          (numEntries, capacity) => {
            module.sb_overflows = 0;
            module.sb_totalCreated = 0;
            module.sb_capacity = capacity; // Set for chained buffers

            const buffer = createSpanBuffer(testSchema, module, 'test-span', undefined, capacity);
            const logger = createSpanLogger(testSchema, buffer, createNextBuffer);

            for (let i = 0; i < numEntries; i++) {
              logger.info(`msg-${i}`).requestId(`req-${i}`);
            }

            const { bufferCount } = analyzeBufferChain(buffer);

            // Property: buffer count matches formula (with reserved rows)
            const expected = expectedBufferCount(numEntries, capacity, RESERVED_ROWS);
            expect(bufferCount).toBe(expected);

            // Property: overflow count matches
            expect(module.sb_overflows).toBe(bufferCount - 1);
          },
        ),
        { numRuns: 100 },
      );
    });
  });
});
