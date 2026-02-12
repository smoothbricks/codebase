/**
 * Buffer overflow tests - Op-centric API Integration
 *
 * These tests verify buffer chaining behavior through the PUBLIC API:
 * - defineOpContext / defineOp for op definition
 * - Fluent logging API (ctx.log.info().userId().requestId())
 * - trace.span() for span execution
 * - convertSpanTreeToArrowTable for Arrow output
 *
 * Complements buffer-overflow.test.ts which tests low-level buffer mechanics.
 *
 * Key differences from buffer-overflow.test.ts:
 * - Uses public API only (no internal buffer access like _writeIndex)
 * - Verifies behavior via Arrow output, not buffer internals
 * - Tests natural usage patterns, not precise buffer mechanics
 *
 * Property-based testing with fast-check for comprehensive coverage.
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import './test-helpers.js';
import fc from 'fast-check';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { defineCodeError } from '../result.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

// Error code factory for tests
const VALIDATION_ERROR = defineCodeError('VALIDATION_ERROR')<{ field: string }>();

// biome-ignore lint/suspicious/noExplicitAny: SpanBuffer generic types are complex, using any for test buffer capture
type CapturedBuffer = any;

// Test schema with various column types
const testSchema = defineLogSchema({
  userId: S.category(),
  requestId: S.category(),
  operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  duration: S.number(),
  errorMsg: S.text(),
  index: S.number(),
});

// Create op context factory
const ctx = defineOpContext({
  logSchema: testSchema,
  ctx: {} as Record<string, never>,
});

const { defineOp } = ctx;

/**
 * Helper to create a properly typed tracer for tests
 */
function createTestTracer() {
  return new TestTracer(ctx, { ...createTestTracerOptions() });
}

function getColumnValue<T>(
  table: ReturnType<typeof convertSpanTreeToArrowTable>,
  columnName: string,
  rowIndex: number,
): T {
  const column = table.getChild(columnName);
  if (!column) {
    throw new Error(`column not found: ${columnName}`);
  }
  return column.get(rowIndex) as T;
}

/**
 * Count rows in Arrow table by entry_type
 */
function countRowsByEntryType(table: ReturnType<typeof convertSpanTreeToArrowTable>): Record<string, number> {
  const counts: Record<string, number> = {};
  for (let i = 0; i < table.numRows; i++) {
    const entryType = getColumnValue<string | null>(table, 'entry_type', i);
    if (!entryType) {
      continue;
    }
    counts[entryType] = (counts[entryType] || 0) + 1;
  }
  return counts;
}

/**
 * Extract all rows from Arrow table as JSON
 */
function extractRows(table: ReturnType<typeof convertSpanTreeToArrowTable>): Array<Record<string, unknown>> {
  const columns = [
    'entry_type',
    'message',
    'userId',
    'requestId',
    'operation',
    'duration',
    'index',
    'errorMsg',
    'parent_span_id',
    'span_id',
    'trace_id',
  ] as const;
  return Array.from({ length: table.numRows }, (_, rowIndex) => {
    const row: Record<string, unknown> = {};
    for (const columnName of columns) {
      const column = table.getChild(columnName);
      if (column) {
        row[columnName] = column.get(rowIndex) as unknown;
      }
    }
    return row;
  });
}

describe('Buffer Overflow - Op-centric API Integration', () => {
  describe('Entry Preservation via Arrow Output', () => {
    it('should preserve all logged entries in Arrow output', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 50 }), // Number of log entries
          async (numEntries) => {
            let capturedBuffer: CapturedBuffer;

            const testOp = defineOp('entry-preservation', async (ctx) => {
              capturedBuffer = ctx.buffer;

              // Log entries using the fluent API
              for (let i = 0; i < numEntries; i++) {
                ctx.log
                  .info(`message-${i}`)
                  .userId(`user-${i % 5}`)
                  .requestId(`req-${i % 3}`)
                  .operation(['GET', 'POST', 'PUT', 'DELETE'][i % 4] as 'GET')
                  .index(i);
              }

              return ctx.ok({ logged: numEntries });
            });

            const { trace } = createTestTracer();
            const result = await trace('test', testOp);

            expect(result.success).toBe(true);
            expect(capturedBuffer).toBeDefined();

            // Convert to Arrow and verify
            const table = convertSpanTreeToArrowTable(capturedBuffer!);
            const counts = countRowsByEntryType(table);

            // Should have: 1 span-start + numEntries info + 1 span-ok
            expect(counts['span-start']).toBe(1);
            expect(counts.info).toBe(numEntries);
            expect(counts['span-ok']).toBe(1);
            expect(table.numRows).toBe(2 + numEntries); // span-start + span-ok + entries
          },
        ),
        { numRuns: 20 },
      );
    });
  });

  describe('Buffer Chaining with Large Entry Counts', () => {
    it('should handle overflow transparently for large log volumes', async () => {
      // Log many entries that will definitely cause buffer overflow
      const numEntries = 200; // Way more than default buffer capacity (8)
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('large-volume', async (ctx) => {
        capturedBuffer = ctx.buffer;

        for (let i = 0; i < numEntries; i++) {
          ctx.log
            .info(`log-${i}`)
            .index(i)
            .userId(`user-${i % 10}`);
        }

        return ctx.ok({ count: numEntries });
      });

      const { trace } = createTestTracer();
      const result = await trace('large-test', testOp);

      expect(result.success).toBe(true);
      expect(capturedBuffer).toBeDefined();

      // Convert to Arrow - this walks the entire buffer chain
      const table = convertSpanTreeToArrowTable(capturedBuffer!);
      const counts = countRowsByEntryType(table);

      // All entries should be present
      expect(counts.info).toBe(numEntries);
      expect(table.numRows).toBe(2 + numEntries);

      // Verify some sample data
      const rows = extractRows(table);
      const infoRows = rows.filter((r) => r.entry_type === 'info');

      // First and last entries should be correct
      expect(infoRows[0].message).toBe('log-0');
      expect(infoRows[0].index).toBe(0);
      expect(infoRows[numEntries - 1].message).toBe(`log-${numEntries - 1}`);
      expect(infoRows[numEntries - 1].index).toBe(numEntries - 1);
    });

    it('should preserve order across buffer chain boundaries', async () => {
      const numEntries = 100;
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('order-test', async (ctx) => {
        capturedBuffer = ctx.buffer;

        for (let i = 0; i < numEntries; i++) {
          ctx.log.info(`entry-${i}`).index(i);
        }

        return ctx.ok('done');
      });

      const { trace } = createTestTracer();
      await trace('order', testOp);

      const table = convertSpanTreeToArrowTable(capturedBuffer!);
      const rows = extractRows(table);
      const infoRows = rows.filter((r) => r.entry_type === 'info');

      // Verify strict ordering - each entry should have index matching its position
      for (let i = 0; i < numEntries; i++) {
        expect(infoRows[i].index).toBe(i);
        expect(infoRows[i].message).toBe(`entry-${i}`);
      }
    });
  });

  describe('Data Integrity Through Arrow Conversion', () => {
    it('should preserve attribute values through Arrow conversion', async () => {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 10, max: 30 }), async (numEntries) => {
          let capturedBuffer: CapturedBuffer;

          const testOp = defineOp('data-integrity', async (ctx) => {
            capturedBuffer = ctx.buffer;

            for (let i = 0; i < numEntries; i++) {
              ctx.log
                .info(`msg-${i}`)
                .userId(`user-${i}`)
                .requestId(`req-${i}`)
                .operation(['GET', 'POST', 'PUT', 'DELETE'][i % 4] as 'GET')
                .duration(i * 1.5)
                .index(i);
            }

            return ctx.ok('done');
          });

          const { trace } = createTestTracer();
          await trace('integrity', testOp);

          const table = convertSpanTreeToArrowTable(capturedBuffer!);
          const rows = extractRows(table);
          const infoRows = rows.filter((r) => r.entry_type === 'info');

          expect(infoRows.length).toBe(numEntries);

          // Verify each entry has correct values
          for (let i = 0; i < numEntries; i++) {
            const row = infoRows[i];
            expect(row.message).toBe(`msg-${i}`);
            expect(row.userId).toBe(`user-${i}`);
            expect(row.requestId).toBe(`req-${i}`);
            expect(row.operation).toBe(['GET', 'POST', 'PUT', 'DELETE'][i % 4]);
            expect(row.duration).toBe(i * 1.5);
            expect(row.index).toBe(i);
          }
        }),
        { numRuns: 10 },
      );
    });

    it('should handle all column types correctly across overflow', async () => {
      const numEntries = 50;
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('column-types', async (ctx) => {
        capturedBuffer = ctx.buffer;

        for (let i = 0; i < numEntries; i++) {
          ctx.log
            .info(`text-message-${i}`) // text in message (system column)
            .userId(`category-${i % 5}`) // category
            .operation(['GET', 'POST', 'PUT', 'DELETE'][i % 4] as 'GET') // enum
            .duration(i * Math.PI) // number (float)
            .index(i) // number (int stored as float)
            .errorMsg(`unique-error-${i}-${Date.now()}`); // text (unique values)
        }

        return ctx.ok('done');
      });

      const { trace } = createTestTracer();
      await trace('types', testOp);

      const table = convertSpanTreeToArrowTable(capturedBuffer!);
      const rows = extractRows(table);
      const infoRows = rows.filter((r) => r.entry_type === 'info');

      expect(infoRows.length).toBe(numEntries);

      // Spot check values
      expect(infoRows[0].message).toBe('text-message-0');
      expect(infoRows[0].userId).toBe('category-0');
      expect(infoRows[0].operation).toBe('GET');
      expect(infoRows[0].duration).toBeCloseTo(0, 5);
      expect(infoRows[0].index).toBe(0);
      expect((infoRows[0].errorMsg as string).startsWith('unique-error-0-')).toBe(true);

      expect(infoRows[25].message).toBe('text-message-25');
      expect(infoRows[25].duration).toBeCloseTo(25 * Math.PI, 5);
    });
  });

  describe('Nested Spans with Overflow', () => {
    it('should handle overflow in nested span hierarchies', async () => {
      let rootBuffer: CapturedBuffer;
      let childBuffer: CapturedBuffer;

      const childOp = defineOp('child-op', async (ctx) => {
        childBuffer = ctx.buffer;

        // Log many entries in child span
        for (let i = 0; i < 50; i++) {
          ctx.log.info(`child-log-${i}`).index(i);
        }

        return ctx.ok('child-done');
      });

      const parentOp = defineOp('parent-op', async (ctx) => {
        rootBuffer = ctx.buffer;

        // Log some in parent
        for (let i = 0; i < 20; i++) {
          ctx.log.info(`parent-log-${i}`).index(i);
        }

        // Create child span
        await ctx.span('child', childOp);

        // More logs in parent after child
        for (let i = 20; i < 30; i++) {
          ctx.log.info(`parent-log-${i}`).index(i);
        }

        return ctx.ok('parent-done');
      });

      const { trace } = createTestTracer();
      const result = await trace('parent', parentOp);

      expect(result.success).toBe(true);
      expect(rootBuffer).toBeDefined();
      expect(childBuffer).toBeDefined();

      // Convert entire tree
      const table = convertSpanTreeToArrowTable(rootBuffer!);

      // Count entries
      const counts = countRowsByEntryType(table);

      // Parent: 1 span-start + 30 info + 1 span-ok = 32
      // Child: 1 span-start + 50 info + 1 span-ok = 52
      // Total: 84 rows
      expect(counts['span-start']).toBe(2); // parent + child
      expect(counts['span-ok']).toBe(2); // parent + child
      expect(counts.info).toBe(80); // 30 parent + 50 child
      expect(table.numRows).toBe(84);

      // Verify hierarchy in Arrow output
      const rows = extractRows(table);
      const spanStarts = rows.filter((r) => r.entry_type === 'span-start');

      expect(spanStarts.length).toBe(2);

      const parentSpan = spanStarts.find((r) => r.message === 'parent');
      const childSpan = spanStarts.find((r) => r.message === 'child');

      expect(parentSpan).toBeDefined();
      expect(childSpan).toBeDefined();

      // Parent span IS the root span (created by trace('parent', parentOp))
      // Root spans have no parent, so parent_span_id is null
      expect(parentSpan?.parent_span_id).toBeNull();

      // Child's parent is the parent span
      expect(childSpan?.parent_span_id).toBe(parentSpan?.span_id);

      // Both have same trace_id
      expect(childSpan?.trace_id).toBe(parentSpan?.trace_id);
    });

    it('should handle deeply nested spans with overflow at each level', async () => {
      let level1Buffer: CapturedBuffer;

      const level4Op = defineOp('l4', async (ctx) => {
        for (let i = 0; i < 15; i++) {
          ctx.log.info(`l4-${i}`);
        }
        return ctx.ok('l4');
      });

      const level3Op = defineOp('l3', async (ctx) => {
        for (let i = 0; i < 15; i++) {
          ctx.log.info(`l3-${i}`);
        }
        await ctx.span('level4', level4Op);
        return ctx.ok('l3');
      });

      const level2Op = defineOp('l2', async (ctx) => {
        for (let i = 0; i < 15; i++) {
          ctx.log.info(`l2-${i}`);
        }
        await ctx.span('level3', level3Op);
        return ctx.ok('l2');
      });

      const level1Op = defineOp('l1', async (ctx) => {
        level1Buffer = ctx.buffer;
        for (let i = 0; i < 15; i++) {
          ctx.log.info(`l1-${i}`);
        }
        await ctx.span('level2', level2Op);
        return ctx.ok('l1');
      });

      const { trace } = createTestTracer();
      await trace('level1', level1Op);

      const table = convertSpanTreeToArrowTable(level1Buffer!);
      const counts = countRowsByEntryType(table);

      // 4 levels * (1 span-start + 15 info + 1 span-ok) = 4 * 17 = 68
      expect(counts['span-start']).toBe(4);
      expect(counts['span-ok']).toBe(4);
      expect(counts.info).toBe(60); // 15 * 4 levels
      expect(table.numRows).toBe(68);
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty span (no user logs)', async () => {
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('empty-span', async (ctx) => {
        capturedBuffer = ctx.buffer;
        // No logging, just return
        return ctx.ok('empty');
      });

      const { trace } = createTestTracer();
      await trace('empty', testOp);

      const table = convertSpanTreeToArrowTable(capturedBuffer!);

      // Just span-start and span-ok
      expect(table.numRows).toBe(2);

      const counts = countRowsByEntryType(table);
      expect(counts['span-start']).toBe(1);
      expect(counts['span-ok']).toBe(1);
    });

    it('should handle single log entry', async () => {
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('single-log', async (ctx) => {
        capturedBuffer = ctx.buffer;
        ctx.log.info('single').userId('u1');
        return ctx.ok('done');
      });

      const { trace } = createTestTracer();
      await trace('single', testOp);

      const table = convertSpanTreeToArrowTable(capturedBuffer!);
      expect(table.numRows).toBe(3); // span-start + info + span-ok

      const rows = extractRows(table);
      const infoRow = rows.find((r) => r.entry_type === 'info');
      expect(infoRow?.message).toBe('single');
      expect(infoRow?.userId).toBe('u1');
    });

    it('should handle error result (span-err)', async () => {
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('error-span', async (ctx) => {
        capturedBuffer = ctx.buffer;
        ctx.log.info('before error');
        return ctx.err(VALIDATION_ERROR({ field: 'email' }));
      });

      const { trace } = createTestTracer();
      await trace('error', testOp);

      const table = convertSpanTreeToArrowTable(capturedBuffer!);
      const counts = countRowsByEntryType(table);

      expect(counts['span-start']).toBe(1);
      expect(counts.info).toBe(1);
      expect(counts['span-err']).toBe(1);
      expect(table.numRows).toBe(3);
    });

    it('should handle exception (span-exception)', async () => {
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('exception-span', async (ctx) => {
        capturedBuffer = ctx.buffer;
        ctx.log.info('before throw');
        throw new Error('test exception');
      });

      const { trace } = createTestTracer();

      await expect(trace('exception', testOp)).rejects.toThrow('test exception');

      // Buffer should still have entries
      expect(capturedBuffer).toBeDefined();

      const table = convertSpanTreeToArrowTable(capturedBuffer!);
      const counts = countRowsByEntryType(table);

      expect(counts['span-start']).toBe(1);
      expect(counts.info).toBe(1);
      expect(counts['span-exception']).toBe(1);
      expect(table.numRows).toBe(3);
    });
  });

  describe('Mixed Log Levels', () => {
    it('should preserve all log levels through overflow', async () => {
      let capturedBuffer: CapturedBuffer;

      const testOp = defineOp('mixed-levels', async (ctx) => {
        capturedBuffer = ctx.buffer;

        // Mix of log levels
        for (let i = 0; i < 50; i++) {
          switch (i % 4) {
            case 0:
              ctx.log.info(`info-${i}`);
              break;
            case 1:
              ctx.log.debug(`debug-${i}`);
              break;
            case 2:
              ctx.log.warn(`warn-${i}`);
              break;
            case 3:
              ctx.log.error(`error-${i}`);
              break;
          }
        }

        return ctx.ok('done');
      });

      const { trace } = createTestTracer();
      await trace('mixed', testOp);

      const table = convertSpanTreeToArrowTable(capturedBuffer!);
      const counts = countRowsByEntryType(table);

      // 50 entries divided among 4 levels: 13+13+12+12 = 50
      // info:  i % 4 === 0: indices 0,4,8,12,16,20,24,28,32,36,40,44,48 = 13
      // debug: i % 4 === 1: indices 1,5,9,13,17,21,25,29,33,37,41,45,49 = 13
      // warn:  i % 4 === 2: indices 2,6,10,14,18,22,26,30,34,38,42,46 = 12
      // error: i % 4 === 3: indices 3,7,11,15,19,23,27,31,35,39,43,47 = 12
      expect(counts.info).toBe(13);
      expect(counts.debug).toBe(13);
      expect(counts.warn).toBe(12);
      expect(counts.error).toBe(12);

      expect(counts['span-start']).toBe(1);
      expect(counts['span-ok']).toBe(1);
    });
  });
});
