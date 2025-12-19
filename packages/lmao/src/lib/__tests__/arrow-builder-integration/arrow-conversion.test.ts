/**
 * Tests for Arrow table conversion
 *
 * Per specs/01f_arrow_table_structure.md and 01b_columnar_buffer_architecture.md:
 * - Zero-copy conversion from SpanBuffer to Arrow
 * - Null bitmap handling
 * - String type conversion (enum/category/text)
 * - Buffer chaining
 * - Span tree conversion
 */

import { beforeEach, describe, expect, test } from 'bun:test';
import type { Table } from 'apache-arrow';
import { convertSpanTreeToArrowTable, convertToArrowTable } from '../../convertToArrow.js';
import { S } from '../../schema/builder.js';

import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_FF_ACCESS,
  ENTRY_TYPE_FF_USAGE,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_ERR,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_START,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
  mergeWithSystemSchema,
} from '../../schema/systemSchema.js';
import { createChildSpanBuffer, createNextBuffer, createSpanBuffer } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import { createTestSchema, createTestTaskContext } from '../test-helpers.js';

/**
 * Helper to get raw timestamp value from Arrow table.
 * Arrow JS converts Timestamp<NANOSECOND> to milliseconds in get()/toJSON(),
 * so we access the underlying BigInt64Array directly.
 */
function getRawTimestamp(table: Table, rowIndex: number): bigint {
  const timestampCol = table.getChild('timestamp');
  if (!timestampCol) throw new Error('timestamp column not found');
  const values = timestampCol.data[0]?.values as BigInt64Array;
  return values[rowIndex];
}

describe('Arrow Table Conversion', () => {
  describe('Single buffer conversion', () => {
    test('converts basic span buffer to Arrow table', () => {
      const schema = createTestSchema({
        httpStatus: S.number(),
        userId: S.category(),
      });

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write some test data
      buffer.timestamps[0] = 1000n;
      buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'test-span');
      buffer.httpStatus(0, 200);
      buffer.userId(0, 'user-123');
      buffer.writeIndex = 1;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(1);
      expect(table.numCols).toBeGreaterThan(0);

      // Check that columns exist
      const row0 = table.get(0)?.toJSON();
      // Use helper for raw timestamp (Arrow JS converts to milliseconds in toJSON)
      expect(getRawTimestamp(table, 0)).toBe(1000n);
      expect(row0?.entry_type).toBe('span-start');
      expect(row0?.message).toBe('test-span');
      expect(row0?.httpStatus).toBe(200);
      expect(row0?.userId).toBe('user-123');
    });

    test('handles multiple rows in buffer', () => {
      const schema = createTestSchema({
        level: S.enum(['DEBUG', 'INFO', 'WARN', 'ERROR'] as const),
      });

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-456'));

      // Write multiple rows
      buffer.timestamps[0] = 1000n;
      buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'span-1');
      buffer.level(0, 1); // INFO

      buffer.timestamps[1] = 2000n;
      buffer.operations[1] = ENTRY_TYPE_INFO;
      buffer.message(1, 'Log message');
      buffer.level(1, 0); // DEBUG

      buffer.timestamps[2] = 3000n;
      buffer.operations[2] = ENTRY_TYPE_SPAN_OK;
      buffer.message(2, 'span-1');
      buffer.level(2, 2); // WARN

      buffer.writeIndex = 3;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(3);

      const row0 = table.get(0)?.toJSON();
      expect(getRawTimestamp(table, 0)).toBe(1000n);
      expect(row0?.entry_type).toBe('span-start');
      expect(row0?.level).toBe('INFO');

      const row1 = table.get(1)?.toJSON();
      expect(getRawTimestamp(table, 1)).toBe(2000n);
      expect(row1?.entry_type).toBe('info');
      expect(row1?.level).toBe('DEBUG');

      const row2 = table.get(2)?.toJSON();
      expect(getRawTimestamp(table, 2)).toBe(3000n);
      expect(row2?.entry_type).toBe('span-ok');
      expect(row2?.level).toBe('WARN');
    });

    test('handles null values correctly', () => {
      const schema = createTestSchema({
        optionalField: S.category(),
        requiredField: S.number(),
      });

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-789'));

      // Write row with only requiredField set
      buffer.timestamps[0] = 1000n;
      buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'test-span');
      buffer.requiredField(0, 42);
      // optionalField not set - should be null
      buffer.writeIndex = 1;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(1);

      const row0 = table.get(0)?.toJSON();
      expect(row0?.requiredField).toBe(42);
      expect(row0?.optionalField).toBeNull();
    });
  });

  describe('Buffer chaining', () => {
    test('converts chained buffers to single table', () => {
      const schema = createTestSchema({
        counter: S.number(),
      });

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer1 = createSpanBuffer(schema, taskContext, createTraceId('trace-chain'));

      // Fill first buffer
      buffer1.timestamps[0] = 1000n;
      buffer1.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer1.message(0, 'test-span');
      buffer1.counter(0, 1);
      buffer1.writeIndex = 1;

      // Create chained buffer
      const buffer2 = createNextBuffer(buffer1);
      buffer2.timestamps[0] = 2000n;
      buffer2.operations[0] = ENTRY_TYPE_INFO;
      buffer2.message(0, 'Log in chain');
      buffer2.counter(0, 2);
      buffer2.writeIndex = 1;

      const table = convertToArrowTable(buffer1);

      expect(table.numRows).toBe(2);

      const row0 = table.get(0)?.toJSON();
      expect(getRawTimestamp(table, 0)).toBe(1000n);
      expect(row0?.counter).toBe(1);

      const row1 = table.get(1)?.toJSON();
      expect(getRawTimestamp(table, 1)).toBe(2000n);
      expect(row1?.counter).toBe(2);
    });

    test('handles multiple chained buffers', () => {
      const schema = createTestSchema({
        value: S.number(),
      });

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer1 = createSpanBuffer(schema, taskContext, createTraceId('trace-multi-chain'));

      buffer1.timestamps[0] = 1000n;
      buffer1.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer1.message(0, 'test');
      buffer1.value(0, 1);
      buffer1.writeIndex = 1;

      const buffer2 = createNextBuffer(buffer1);
      buffer2.timestamps[0] = 2000n;
      buffer2.operations[0] = ENTRY_TYPE_INFO;
      buffer2.message(0, 'log-1');
      buffer2.value(0, 2);
      buffer2.writeIndex = 1;

      const buffer3 = createNextBuffer(buffer2);
      buffer3.timestamps[0] = 3000n;
      buffer3.operations[0] = ENTRY_TYPE_SPAN_OK;
      buffer3.message(0, 'test');
      buffer3.value(0, 3);
      buffer3.writeIndex = 1;

      const table = convertToArrowTable(buffer1);

      expect(table.numRows).toBe(3);

      // Verify all values are present
      const values = [table.get(0)?.toJSON()?.value, table.get(1)?.toJSON()?.value, table.get(2)?.toJSON()?.value];
      expect(values).toEqual([1, 2, 3]);
    });
  });

  describe('Span tree conversion', () => {
    test('converts parent and child spans to single table', () => {
      const schema = createTestSchema({
        spanType: S.category(),
      });

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const parentBuffer = createSpanBuffer(schema, taskContext, createTraceId('trace-tree'));

      parentBuffer.timestamps[0] = 1000n;
      parentBuffer.operations[0] = ENTRY_TYPE_SPAN_START;
      parentBuffer.message(0, 'parent-span');
      parentBuffer.spanType(0, 'parent');
      parentBuffer.writeIndex = 1;

      // Create child span and register with parent
      const childTaskContext = createTestTaskContext(schema, { lineNumber: 43 });
      const childBuffer = createChildSpanBuffer(parentBuffer, childTaskContext);
      parentBuffer.children.push(childBuffer); // Explicit registration per spanBuffer.ts

      childBuffer.timestamps[0] = 1500n;
      childBuffer.operations[0] = ENTRY_TYPE_SPAN_START;
      childBuffer.message(0, 'child-span');
      childBuffer.spanType(0, 'child');
      childBuffer.writeIndex = 1;

      // Complete child span
      childBuffer.timestamps[1] = 1800n;
      childBuffer.operations[1] = ENTRY_TYPE_SPAN_OK;
      childBuffer.message(1, 'child-span');
      childBuffer.writeIndex = 2;

      // Complete parent span
      parentBuffer.timestamps[1] = 2000n;
      parentBuffer.operations[1] = ENTRY_TYPE_SPAN_OK;
      parentBuffer.message(1, 'parent-span');
      parentBuffer.writeIndex = 2;

      const table = convertSpanTreeToArrowTable(parentBuffer);

      // Should have 4 rows: parent start, parent end, child start, child end
      // (walkSpanTree visits buffer rows first, then children recursively)
      expect(table.numRows).toBe(4);

      // Verify span hierarchy is preserved
      const rows = [table.get(0)?.toJSON(), table.get(1)?.toJSON(), table.get(2)?.toJSON(), table.get(3)?.toJSON()];

      // All should have same trace_id
      const traceIds = rows.map((r) => r?.trace_id);
      expect(new Set(traceIds).size).toBe(1);

      // Parent and child should have different span_ids
      // Order: parent start (0), parent end (1), child start (2), child end (3)
      const spanIds = rows.map((r) => r?.span_id);
      expect(spanIds[0]).toBe(spanIds[1]); // Parent start and end (both from parent buffer)
      expect(spanIds[2]).toBe(spanIds[3]); // Child start and end (both from child buffer)
      expect(spanIds[0]).not.toBe(spanIds[2]); // Parent != Child
    });

    test('handles multiple sibling child spans', () => {
      const schema = createTestSchema({});

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const parentBuffer = createSpanBuffer(schema, taskContext, createTraceId('trace-siblings'));

      parentBuffer.timestamps[0] = 1000n;
      parentBuffer.operations[0] = ENTRY_TYPE_SPAN_START;
      parentBuffer.message(0, 'parent');
      parentBuffer.writeIndex = 1;

      // Create first child and register with parent
      const child1TaskContext = createTestTaskContext(schema, { lineNumber: 43 });
      const child1Buffer = createChildSpanBuffer(parentBuffer, child1TaskContext);
      parentBuffer.children.push(child1Buffer); // Explicit registration per spanBuffer.ts
      child1Buffer.timestamps[0] = 1100n;
      child1Buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      child1Buffer.message(0, 'child-1');
      child1Buffer.timestamps[1] = 1200n;
      child1Buffer.operations[1] = ENTRY_TYPE_SPAN_OK;
      child1Buffer.message(1, 'child-1');
      child1Buffer.writeIndex = 2;

      // Create second child and register with parent
      const child2TaskContext = createTestTaskContext(schema, { lineNumber: 44 });
      const child2Buffer = createChildSpanBuffer(parentBuffer, child2TaskContext);
      parentBuffer.children.push(child2Buffer); // Explicit registration per spanBuffer.ts
      child2Buffer.timestamps[0] = 1300n;
      child2Buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      child2Buffer.message(0, 'child-2');
      child2Buffer.timestamps[1] = 1400n;
      child2Buffer.operations[1] = ENTRY_TYPE_SPAN_OK;
      child2Buffer.message(1, 'child-2');
      child2Buffer.writeIndex = 2;

      // Complete parent
      parentBuffer.timestamps[1] = 1500n;
      parentBuffer.operations[1] = ENTRY_TYPE_SPAN_OK;
      parentBuffer.message(1, 'parent');
      parentBuffer.writeIndex = 2;

      const table = convertSpanTreeToArrowTable(parentBuffer);

      // Should have 6 rows: parent + 2 children with start/end each
      expect(table.numRows).toBe(6);
    });
  });

  describe('Entry type handling', () => {
    test('correctly converts all entry types', () => {
      const schema = createTestSchema({});

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      // Use capacity of 16 to hold all 11 entry types
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-types'), 16);

      const entryTypes = [
        ENTRY_TYPE_SPAN_START,
        ENTRY_TYPE_SPAN_OK,
        ENTRY_TYPE_SPAN_ERR,
        ENTRY_TYPE_SPAN_EXCEPTION,
        ENTRY_TYPE_TRACE,
        ENTRY_TYPE_DEBUG,
        ENTRY_TYPE_INFO,
        ENTRY_TYPE_WARN,
        ENTRY_TYPE_ERROR,
        ENTRY_TYPE_FF_ACCESS,
        ENTRY_TYPE_FF_USAGE,
      ];

      const expectedNames = [
        'span-start',
        'span-ok',
        'span-err',
        'span-exception',
        'trace',
        'debug',
        'info',
        'warn',
        'error',
        'ff-access',
        'ff-usage',
      ];

      // Write one row for each entry type
      for (let i = 0; i < entryTypes.length; i++) {
        buffer.timestamps[i] = BigInt(1000 + i * 100);
        buffer.operations[i] = entryTypes[i];
        buffer.message(i, `entry-${i}`);
      }
      buffer.writeIndex = entryTypes.length;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(entryTypes.length);

      // Verify each entry type is correctly converted
      for (let i = 0; i < entryTypes.length; i++) {
        const row = table.get(i)?.toJSON();
        expect(row?.entry_type).toBe(expectedNames[i]);
      }
    });
  });

  describe('Buffer metrics', () => {
    test('includes both span entries and buffer metric entries in same table', () => {
      const schema = createTestSchema({});
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write some span entries
      buffer.timestamps[0] = 1000n;
      buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'test-span');
      buffer.writeIndex = 1;

      buffer.timestamps[1] = 2000n;
      buffer.operations[1] = ENTRY_TYPE_INFO;
      buffer.message(1, 'Test log message');
      buffer.writeIndex = 2;

      // Update capacity stats to have meaningful values
      taskContext.module.sb_capacity = 128;
      taskContext.module.sb_totalWrites = 50;
      taskContext.module.sb_overflowWrites = 5;
      taskContext.module.sb_totalCreated = 2;
      taskContext.module.sb_overflows = 1;

      // Convert with modulesToLogStats
      const modulesToLogStats = new Set([taskContext.module]);
      const table = convertSpanTreeToArrowTable(buffer, undefined, modulesToLogStats);

      // Should have span entries (2) + buffer metric entries (5) = 7 rows total
      expect(table.numRows).toBe(7);
      expect(table.batches.length).toBe(2); // One batch for span data, one for buffer metrics

      // Verify span entries
      const row0 = table.get(0)?.toJSON();
      expect(row0?.entry_type).toBe('span-start');
      expect(row0?.message).toBe('test-span');

      const row1 = table.get(1)?.toJSON();
      expect(row1?.entry_type).toBe('info');
      expect(row1?.message).toBe('Test log message');

      // Verify buffer metric entries (5 rows per module)
      // Per spec, buffer metrics use uint64_value column, NOT JSON in message
      const row2 = table.get(2)?.toJSON();
      expect(row2?.entry_type).toBe('period-start');
      expect(row2?.package_name).toBe('@test/package');
      expect(row2?.uint64_value).toBe(0n); // periodStartNs defaults to 0n

      const row3 = table.get(3)?.toJSON();
      expect(row3?.entry_type).toBe('buffer-writes');
      expect(row3?.uint64_value).toBe(50n); // sb_totalWrites

      const row4 = table.get(4)?.toJSON();
      expect(row4?.entry_type).toBe('buffer-overflow-writes');
      expect(row4?.uint64_value).toBe(5n); // sb_overflowWrites

      const row5 = table.get(5)?.toJSON();
      expect(row5?.entry_type).toBe('buffer-created');
      expect(row5?.uint64_value).toBe(2n); // sb_totalCreated

      const row6 = table.get(6)?.toJSON();
      expect(row6?.entry_type).toBe('buffer-overflows');
      expect(row6?.uint64_value).toBe(1n); // sb_overflows
    });

    test('includes only buffer metrics when no span data', () => {
      const schema = createTestSchema({});
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Buffer has no entries (writeIndex = 0)
      buffer.writeIndex = 0;

      // Update capacity stats
      taskContext.module.sb_capacity = 64;
      taskContext.module.sb_totalWrites = 10;
      taskContext.module.sb_overflowWrites = 0;
      taskContext.module.sb_totalCreated = 1;
      taskContext.module.sb_overflows = 0;

      // Convert with modulesToLogStats
      const modulesToLogStats = new Set([taskContext.module]);
      const table = convertSpanTreeToArrowTable(buffer, undefined, modulesToLogStats);

      // Should have only buffer metric entries (5 rows)
      expect(table.numRows).toBe(5);
      expect(table.batches.length).toBe(1); // Only buffer metrics batch

      const row0 = table.get(0)?.toJSON();
      expect(row0?.entry_type).toBe('period-start');
      expect(row0?.uint64_value).toBe(0n);

      const row1 = table.get(1)?.toJSON();
      expect(row1?.entry_type).toBe('buffer-writes');
      expect(row1?.uint64_value).toBe(10n);

      const row2 = table.get(2)?.toJSON();
      expect(row2?.entry_type).toBe('buffer-overflow-writes');
      expect(row2?.uint64_value).toBe(0n);

      const row3 = table.get(3)?.toJSON();
      expect(row3?.entry_type).toBe('buffer-created');
      expect(row3?.uint64_value).toBe(1n);

      const row4 = table.get(4)?.toJSON();
      expect(row4?.entry_type).toBe('buffer-overflows');
      expect(row4?.uint64_value).toBe(0n);
    });
  });
});
