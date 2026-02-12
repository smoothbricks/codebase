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

import { describe, expect, test } from 'bun:test';
import type { Table } from '@uwdata/flechette';
import type { CapacityStatsEntry } from '../../arrow/capacityStats.js';
import { convertSpanTreeToArrowTable, convertToArrowTable } from '../../convertToArrow.js';
import { DEFAULT_METADATA } from '../../opContext/defineOp.js';
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
} from '../../schema/systemSchema.js';
import { createChildSpanBuffer, createOverflowBuffer, createSpanBuffer, getSpanBufferClass } from '../../spanBuffer.js';

import type { SpanBuffer } from '../../types.js';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from '../test-helpers.js';

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

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );

      // Write some test data
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'test-span');
      buffer.httpStatus(0, 200);
      buffer.userId(0, 'user-123');
      buffer._writeIndex = 1;

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

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-456'),
        DEFAULT_METADATA,
        undefined,
      );

      // Write multiple rows
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'span-1');
      buffer.level(0, 1); // INFO

      buffer.timestamp[1] = 2000n;
      buffer.entry_type[1] = ENTRY_TYPE_INFO;
      buffer.message(1, 'Log message');
      buffer.level(1, 0); // DEBUG

      buffer.timestamp[2] = 3000n;
      buffer.entry_type[2] = ENTRY_TYPE_SPAN_OK;
      buffer.message(2, 'span-1');
      buffer.level(2, 2); // WARN

      buffer._writeIndex = 3;

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

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-789'),
        DEFAULT_METADATA,
        undefined,
      );

      // Write row with only requiredField set
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'test-span');
      buffer.requiredField(0, 42);
      // optionalField not set - should be null
      buffer._writeIndex = 1;

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

      const buffer1 = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-chain'),
        DEFAULT_METADATA,
        undefined,
      );

      // Fill first buffer
      buffer1.timestamp[0] = 1000n;
      buffer1.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer1.message(0, 'test-span');
      buffer1.counter(0, 1);
      buffer1._writeIndex = 1;

      // Create chained buffer
      const buffer2 = createOverflowBuffer(buffer1);
      buffer2.timestamp[0] = 2000n;
      buffer2.entry_type[0] = ENTRY_TYPE_INFO;
      buffer2.message(0, 'Log in chain');
      buffer2.counter(0, 2);
      buffer2._writeIndex = 1;

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

      const buffer1 = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-multi-chain'),
        DEFAULT_METADATA,
        undefined,
      );

      buffer1.timestamp[0] = 1000n;
      buffer1.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer1.message(0, 'test');
      buffer1.value(0, 1);
      buffer1._writeIndex = 1;

      const buffer2 = createOverflowBuffer(buffer1);
      buffer2.timestamp[0] = 2000n;
      buffer2.entry_type[0] = ENTRY_TYPE_INFO;
      buffer2.message(0, 'log-1');
      buffer2.value(0, 2);
      buffer2._writeIndex = 1;

      const buffer3 = createOverflowBuffer(buffer2);
      buffer3.timestamp[0] = 3000n;
      buffer3.entry_type[0] = ENTRY_TYPE_SPAN_OK;
      buffer3.message(0, 'test');
      buffer3.value(0, 3);
      buffer3._writeIndex = 1;

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

      const SpanBufferClass = getSpanBufferClass(schema);
      const parentBuffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-tree'),
        DEFAULT_METADATA,
        undefined,
      );

      parentBuffer.timestamp[0] = 1000n;
      parentBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      parentBuffer.message(0, 'parent-span');
      parentBuffer.spanType(0, 'parent');
      parentBuffer._writeIndex = 1;

      // Create child span and register with parent
      const childBuffer = createChildSpanBuffer(
        parentBuffer,
        SpanBufferClass,
        DEFAULT_METADATA,
        DEFAULT_METADATA,
      ) as SpanBuffer<typeof schema>;
      parentBuffer._children.push(childBuffer); // Explicit registration per spanBuffer.ts

      childBuffer.timestamp[0] = 1500n;
      childBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      childBuffer.message(0, 'child-span');
      childBuffer.spanType(0, 'child');
      childBuffer._writeIndex = 1;

      // Complete child span
      childBuffer.timestamp[1] = 1800n;
      childBuffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
      childBuffer.message(1, 'child-span');
      childBuffer._writeIndex = 2;

      // Complete parent span
      parentBuffer.timestamp[1] = 2000n;
      parentBuffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
      parentBuffer.message(1, 'parent-span');
      parentBuffer._writeIndex = 2;

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

      const SpanBufferClass = getSpanBufferClass(schema);
      const parentBuffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-siblings'),
        DEFAULT_METADATA,
        undefined,
      );

      parentBuffer.timestamp[0] = 1000n;
      parentBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      parentBuffer.message(0, 'parent');
      parentBuffer._writeIndex = 1;

      // Create first child and register with parent
      const child1Buffer = createChildSpanBuffer(parentBuffer, SpanBufferClass, DEFAULT_METADATA, DEFAULT_METADATA);
      parentBuffer._children.push(child1Buffer); // Explicit registration per spanBuffer.ts
      child1Buffer.timestamp[0] = 1100n;
      child1Buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      child1Buffer.message(0, 'child-1');
      child1Buffer.timestamp[1] = 1200n;
      child1Buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
      child1Buffer.message(1, 'child-1');
      child1Buffer._writeIndex = 2;

      // Create second child and register with parent
      const child2Buffer = createChildSpanBuffer(parentBuffer, SpanBufferClass, DEFAULT_METADATA, DEFAULT_METADATA);
      parentBuffer._children.push(child2Buffer); // Explicit registration per spanBuffer.ts
      child2Buffer.timestamp[0] = 1300n;
      child2Buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      child2Buffer.message(0, 'child-2');
      child2Buffer.timestamp[1] = 1400n;
      child2Buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
      child2Buffer.message(1, 'child-2');
      child2Buffer._writeIndex = 2;

      // Complete parent
      parentBuffer.timestamp[1] = 1500n;
      parentBuffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
      parentBuffer.message(1, 'parent');
      parentBuffer._writeIndex = 2;

      const table = convertSpanTreeToArrowTable(parentBuffer);

      // Should have 6 rows: parent + 2 children with start/end each
      expect(table.numRows).toBe(6);
    });
  });

  describe('Entry type handling', () => {
    test('correctly converts all entry types', () => {
      const schema = createTestSchema({});

      // Use capacity of 16 to hold all 11 entry types
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-types'), DEFAULT_METADATA, 16);

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
        buffer.timestamp[i] = BigInt(1000 + i * 100);
        buffer.entry_type[i] = entryTypes[i];
        buffer.message(i, `entry-${i}`);
      }
      buffer._writeIndex = entryTypes.length;

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
      const SpanBufferClass = getSpanBufferClass(schema);
      const metadata = createTestOpMetadata({ package_name: '@test/package' });
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-123'), metadata, undefined);

      // Write some span entries
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.message(0, 'test-span');
      buffer._writeIndex = 1;

      buffer.timestamp[1] = 2000n;
      buffer.entry_type[1] = ENTRY_TYPE_INFO;
      buffer.message(1, 'Test log message');
      buffer._writeIndex = 2;

      // Update capacity stats to have meaningful values (utilization-based tuning)
      SpanBufferClass.stats.capacity = 128;
      SpanBufferClass.stats.totalWrites = 50;
      SpanBufferClass.stats.spansCreated = 5;

      // Convert with statsToLog - now requires CapacityStatsEntry[] with both SpanBufferClass and metadata
      const statsToLog: CapacityStatsEntry[] = [{ bufferClass: SpanBufferClass, metadata }];
      const table = convertSpanTreeToArrowTable(buffer, undefined, statsToLog);

      // Should have span entries (2) + buffer metric entries (4) = 6 rows total
      expect(table.numRows).toBe(6);
      expect((table as unknown as { data: unknown[] }).data.length).toBe(2); // One batch for span data, one for buffer metrics

      // Verify span entries
      const row0 = table.get(0)?.toJSON();
      expect(row0?.entry_type).toBe('span-start');
      expect(row0?.message).toBe('test-span');

      const row1 = table.get(1)?.toJSON();
      expect(row1?.entry_type).toBe('info');
      expect(row1?.message).toBe('Test log message');

      // Verify buffer metric entries (4 rows per module for utilization-based tuning)
      // Per spec, buffer metrics use uint64_value column, NOT JSON in message
      const row2 = table.get(2)?.toJSON();
      expect(row2?.entry_type).toBe('period-start');
      expect(row2?.package_name).toBe('@test/package');
      expect(row2?.uint64_value).toBe(0n); // periodStartNs defaults to 0n

      const row3 = table.get(3)?.toJSON();
      expect(row3?.entry_type).toBe('buffer-writes');
      expect(row3?.uint64_value).toBe(50n); // totalWrites

      const row4 = table.get(4)?.toJSON();
      expect(row4?.entry_type).toBe('buffer-spans');
      expect(row4?.uint64_value).toBe(5n); // spansCreated

      const row5 = table.get(5)?.toJSON();
      expect(row5?.entry_type).toBe('buffer-capacity');
      expect(row5?.uint64_value).toBe(128n); // capacity
    });

    test('includes only buffer metrics when no span data', () => {
      const schema = createTestSchema({});
      const SpanBufferClass = getSpanBufferClass(schema);
      const metadata = createTestOpMetadata();
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-123'), metadata, undefined);

      // Buffer has no entries (writeIndex = 0)
      buffer._writeIndex = 0;

      // Update capacity stats (utilization-based tuning)
      SpanBufferClass.stats.capacity = 64;
      SpanBufferClass.stats.totalWrites = 10;
      SpanBufferClass.stats.spansCreated = 2;

      // Convert with statsToLog - now requires CapacityStatsEntry[] with both SpanBufferClass and metadata
      const statsToLog: CapacityStatsEntry[] = [{ bufferClass: SpanBufferClass, metadata }];
      const table = convertSpanTreeToArrowTable(buffer, undefined, statsToLog);

      // Should have only buffer metric entries (4 rows)
      expect(table.numRows).toBe(4);
      expect((table as unknown as { data: unknown[] }).data.length).toBe(1); // Only buffer metrics batch

      const row0 = table.get(0)?.toJSON();
      expect(row0?.entry_type).toBe('period-start');
      expect(row0?.uint64_value).toBe(0n);

      const row1 = table.get(1)?.toJSON();
      expect(row1?.entry_type).toBe('buffer-writes');
      expect(row1?.uint64_value).toBe(10n);

      const row2 = table.get(2)?.toJSON();
      expect(row2?.entry_type).toBe('buffer-spans');
      expect(row2?.uint64_value).toBe(2n); // spansCreated

      const row3 = table.get(3)?.toJSON();
      expect(row3?.entry_type).toBe('buffer-capacity');
      expect(row3?.uint64_value).toBe(64n); // capacity
    });
  });
});
