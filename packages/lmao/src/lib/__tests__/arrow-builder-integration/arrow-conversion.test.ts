/**
 * Tests for Arrow table conversion
 *
 * Per specs/lmao/01f_arrow_table_structure.md and 01b_columnar_buffer_architecture.md:
 * - Zero-copy conversion from SpanBuffer to Arrow
 * - Null bitmap handling
 * - String type conversion (enum/category/text)
 * - Buffer chaining
 * - Span tree conversion
 */

import { describe, expect, test } from 'bun:test';
import type { CapacityStatsEntry } from '../../arrow/capacityStats.js';
import {
  convertSpanTreeToArrowTable,
  convertSpanTreeToLeasedArrowTable,
  convertToArrowTable,
  convertToLeasedArrowTable,
} from '../../convertToArrow.js';
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
import { getColumnValue, getRawTimestamp } from '../arrow-test-helpers.js';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from '../test-helpers.js';

function toBigInt(value: unknown): bigint {
  if (typeof value === 'bigint') {
    return value;
  }
  if (typeof value === 'number') {
    return BigInt(value);
  }
  throw new Error(`Expected numeric Arrow value, received ${typeof value}`);
}

describe('Arrow Table Conversion', () => {
  describe('Single buffer conversion', () => {
    test('converts basic span buffer to Arrow table', () => {
      const schema = createTestSchema({
        httpStatus: S.number(),
        userId: S.category(),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('trace-123'), DEFAULT_METADATA, undefined);

      // Write some test data
      buffer.timestamp[0] = 1000n;
      {
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      buffer.message(0, 'test-span');
      buffer.httpStatus(0, 200);
      buffer.userId(0, 'user-123');
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(1);
      expect(table.numCols).toBeGreaterThan(0);

      // Check that columns exist
      expect(getRawTimestamp(table, 0)).toBe(1000n);
      expect(getColumnValue(table, 'entry_type', 0)).toBe('span-start');
      expect(getColumnValue(table, 'message', 0)).toBe('test-span');
      expect(getColumnValue(table, 'httpStatus', 0)).toBe(200);
      expect(getColumnValue(table, 'userId', 0)).toBe('user-123');
    });

    test('handles multiple rows in buffer', () => {
      const schema = createTestSchema({
        level: S.enum(['DEBUG', 'INFO', 'WARN', 'ERROR'] as const),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('trace-456'), DEFAULT_METADATA, undefined);

      // Write multiple rows
      buffer.timestamp[0] = 1000n;
      {
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      buffer.message(0, 'span-1');
      buffer.level(0, 1); // INFO

      buffer.timestamp[1] = 2000n;
      {
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_INFO;
      }
      buffer.message(1, 'Log message');
      buffer.level(1, 0); // DEBUG

      buffer.timestamp[2] = 3000n;
      {
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[2] = ENTRY_TYPE_SPAN_OK;
      }
      buffer.message(2, 'span-1');
      buffer.level(2, 2); // WARN

      buffer._writeIndex = 3;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(3);

      expect(getRawTimestamp(table, 0)).toBe(1000n);
      expect(getColumnValue(table, 'entry_type', 0)).toBe('span-start');
      expect(getColumnValue(table, 'level', 0)).toBe('INFO');

      expect(getRawTimestamp(table, 1)).toBe(2000n);
      expect(getColumnValue(table, 'entry_type', 1)).toBe('info');
      expect(getColumnValue(table, 'level', 1)).toBe('DEBUG');

      expect(getRawTimestamp(table, 2)).toBe(3000n);
      expect(getColumnValue(table, 'entry_type', 2)).toBe('span-ok');
      expect(getColumnValue(table, 'level', 2)).toBe('WARN');
    });

    test('handles null values correctly', () => {
      const schema = createTestSchema({
        optionalField: S.category(),
        requiredField: S.number(),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('trace-789'), DEFAULT_METADATA, undefined);

      // Write row with only requiredField set
      buffer.timestamp[0] = 1000n;
      {
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      buffer.message(0, 'test-span');
      buffer.requiredField(0, 42);
      // optionalField not set - should be null
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(1);

      expect(getColumnValue(table, 'requiredField', 0)).toBe(42);
      expect(getColumnValue(table, 'optionalField', 0)).toBeUndefined();
    });
  });

  describe('Buffer chaining', () => {
    test('converts chained buffers to single table', () => {
      const schema = createTestSchema({
        counter: S.number(),
      });

      const buffer1 = createSpanBuffer(schema, createTestTraceRoot('trace-chain'), DEFAULT_METADATA, undefined);

      // Fill first buffer
      buffer1.timestamp[0] = 1000n;
      {
        const entryTypes = buffer1.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      buffer1.message(0, 'test-span');
      buffer1.counter(0, 1);
      buffer1._writeIndex = 1;

      // Create chained buffer
      const buffer2 = createOverflowBuffer(buffer1);
      buffer2.timestamp[0] = 2000n;
      {
        const entryTypes = buffer2.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_INFO;
      }
      buffer2.message(0, 'Log in chain');
      buffer2.counter(0, 2);
      buffer2._writeIndex = 1;

      const table = convertToArrowTable(buffer1);

      expect(table.numRows).toBe(2);

      expect(getRawTimestamp(table, 0)).toBe(1000n);
      expect(getColumnValue(table, 'counter', 0)).toBe(1);

      expect(getRawTimestamp(table, 1)).toBe(2000n);
      expect(getColumnValue(table, 'counter', 1)).toBe(2);
    });

    test('handles multiple chained buffers', () => {
      const schema = createTestSchema({
        value: S.number(),
      });

      const buffer1 = createSpanBuffer(schema, createTestTraceRoot('trace-multi-chain'), DEFAULT_METADATA, undefined);

      buffer1.timestamp[0] = 1000n;
      {
        const entryTypes = buffer1.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      buffer1.message(0, 'test');
      buffer1.value(0, 1);
      buffer1._writeIndex = 1;

      const buffer2 = createOverflowBuffer(buffer1);
      buffer2.timestamp[0] = 2000n;
      {
        const entryTypes = buffer2.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_INFO;
      }
      buffer2.message(0, 'log-1');
      buffer2.value(0, 2);
      buffer2._writeIndex = 1;

      const buffer3 = createOverflowBuffer(buffer2);
      buffer3.timestamp[0] = 3000n;
      {
        const entryTypes = buffer3.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_OK;
      }
      buffer3.message(0, 'test');
      buffer3.value(0, 3);
      buffer3._writeIndex = 1;

      const table = convertToArrowTable(buffer1);

      expect(table.numRows).toBe(3);

      // Verify all values are present
      const values = [
        getColumnValue(table, 'value', 0),
        getColumnValue(table, 'value', 1),
        getColumnValue(table, 'value', 2),
      ];
      expect(values).toEqual([1, 2, 3]);
    });
  });

  describe('Span tree conversion', () => {
    test('stores compact identity bytes for child spans', () => {
      const schema = createTestSchema({});
      const SpanBufferClass = getSpanBufferClass(schema);

      const parentBuffer = createSpanBuffer(
        schema,
        createTestTraceRoot('trace-identity-layout'),
        DEFAULT_METADATA,
        undefined,
      );

      const childBuffer = createChildSpanBuffer(parentBuffer, SpanBufferClass, DEFAULT_METADATA, DEFAULT_METADATA);

      // Root identity stores thread_id + span_id + trace_id metadata.
      expect(parentBuffer._identity.byteLength).toBeGreaterThan(12);
      // Child identity stores only thread_id + span_id.
      expect(childBuffer._identity.byteLength).toBe(12);
    });

    test('converts parent and child spans to single table', () => {
      const schema = createTestSchema({
        spanType: S.category(),
      });

      const SpanBufferClass = getSpanBufferClass(schema);
      const parentBuffer = createSpanBuffer(schema, createTestTraceRoot('trace-tree'), DEFAULT_METADATA, undefined);

      parentBuffer.timestamp[0] = 1000n;
      {
        const entryTypes = parentBuffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      parentBuffer.message(0, 'parent-span');
      parentBuffer.spanType(0, 'parent');
      parentBuffer._writeIndex = 1;

      // Create child span; the factory registers it with the parent topology.
      const childBuffer = createChildSpanBuffer(parentBuffer, SpanBufferClass, DEFAULT_METADATA, DEFAULT_METADATA);

      childBuffer.timestamp[0] = 1500n;
      {
        const entryTypes = childBuffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      childBuffer.message(0, 'child-span');
      childBuffer.spanType(0, 'child');
      childBuffer._writeIndex = 1;

      // Complete child span
      childBuffer.timestamp[1] = 1800n;
      {
        const entryTypes = childBuffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_SPAN_OK;
      }
      childBuffer.message(1, 'child-span');
      childBuffer._writeIndex = 2;

      // Complete parent span
      parentBuffer.timestamp[1] = 2000n;
      {
        const entryTypes = parentBuffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_SPAN_OK;
      }
      parentBuffer.message(1, 'parent-span');
      parentBuffer._writeIndex = 2;

      const table = convertSpanTreeToArrowTable(parentBuffer);

      // Should have 4 rows: parent start, parent end, child start, child end
      // (walkSpanTree visits buffer rows first, then children recursively)
      expect(table.numRows).toBe(4);

      // Verify span hierarchy is preserved
      // All should have same trace_id
      const traceIds = [0, 1, 2, 3].map((rowIndex) => getColumnValue(table, 'trace_id', rowIndex));
      expect(new Set(traceIds).size).toBe(1);

      // Parent and child should have different span_ids
      // Order: parent start (0), parent end (1), child start (2), child end (3)
      const spanIds = [0, 1, 2, 3].map((rowIndex) => getColumnValue(table, 'span_id', rowIndex));
      expect(spanIds[0]).toBe(spanIds[1]); // Parent start and end (both from parent buffer)
      expect(spanIds[2]).toBe(spanIds[3]); // Child start and end (both from child buffer)
      expect(spanIds[0]).not.toBe(spanIds[2]); // Parent != Child
    });

    test('handles multiple sibling child spans', () => {
      const schema = createTestSchema({});

      const SpanBufferClass = getSpanBufferClass(schema);
      const parentBuffer = createSpanBuffer(schema, createTestTraceRoot('trace-siblings'), DEFAULT_METADATA, undefined);

      parentBuffer.timestamp[0] = 1000n;
      {
        const entryTypes = parentBuffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      parentBuffer.message(0, 'parent');
      parentBuffer._writeIndex = 1;

      // Create first child; the factory registers it with the parent topology.
      const child1Buffer = createChildSpanBuffer(parentBuffer, SpanBufferClass, DEFAULT_METADATA, DEFAULT_METADATA);
      child1Buffer.timestamp[0] = 1100n;
      {
        const entryTypes = child1Buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      child1Buffer.message(0, 'child-1');
      child1Buffer.timestamp[1] = 1200n;
      {
        const entryTypes = child1Buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_SPAN_OK;
      }
      child1Buffer.message(1, 'child-1');
      child1Buffer._writeIndex = 2;

      // Create second child; the factory preserves sibling insertion order.
      const child2Buffer = createChildSpanBuffer(parentBuffer, SpanBufferClass, DEFAULT_METADATA, DEFAULT_METADATA);
      child2Buffer.timestamp[0] = 1300n;
      {
        const entryTypes = child2Buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      child2Buffer.message(0, 'child-2');
      child2Buffer.timestamp[1] = 1400n;
      {
        const entryTypes = child2Buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_SPAN_OK;
      }
      child2Buffer.message(1, 'child-2');
      child2Buffer._writeIndex = 2;

      // Complete parent
      parentBuffer.timestamp[1] = 1500n;
      {
        const entryTypes = parentBuffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_SPAN_OK;
      }
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
      const buffer = createSpanBuffer(schema, createTestTraceRoot('trace-types'), DEFAULT_METADATA, 16);

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
        {
          const physicalEntryTypes = buffer.entry_type;
          if (physicalEntryTypes === undefined) throw new Error('Expected split entry-type lane');
          physicalEntryTypes[i] = entryTypes[i];
        }
        buffer.message(i, `entry-${i}`);
      }
      buffer._writeIndex = entryTypes.length;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(entryTypes.length);

      // Verify each entry type is correctly converted
      for (let i = 0; i < entryTypes.length; i++) {
        expect(getColumnValue(table, 'entry_type', i)).toBe(expectedNames[i]);
      }
    });
  });

  describe('Buffer metrics', () => {
    test('includes both span entries and buffer metric entries in same table', () => {
      const schema = createTestSchema({});
      const SpanBufferClass = getSpanBufferClass(schema);
      const metadata = createTestOpMetadata({ package_name: '@test/package' });
      const buffer = createSpanBuffer(schema, createTestTraceRoot('trace-123'), metadata, undefined);

      // Write some span entries
      buffer.timestamp[0] = 1000n;
      {
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      buffer.message(0, 'test-span');
      buffer._writeIndex = 1;

      buffer.timestamp[1] = 2000n;
      {
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_INFO;
      }
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

      // Verify span entries
      expect(getColumnValue(table, 'entry_type', 0)).toBe('span-start');
      expect(getColumnValue(table, 'message', 0)).toBe('test-span');

      expect(getColumnValue(table, 'entry_type', 1)).toBe('info');
      expect(getColumnValue(table, 'message', 1)).toBe('Test log message');

      // Verify buffer metric entries (4 rows per module for utilization-based tuning)
      // Per spec, buffer metrics use uint64_value column, NOT JSON in message
      expect(getColumnValue(table, 'entry_type', 2)).toBe('period-start');
      expect(getColumnValue(table, 'package_name', 2)).toBe('@test/package');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 2))).toBe(0n); // periodStartNs defaults to 0n

      expect(getColumnValue(table, 'entry_type', 3)).toBe('buffer-writes');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 3))).toBe(50n); // totalWrites

      expect(getColumnValue(table, 'entry_type', 4)).toBe('buffer-spans');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 4))).toBe(5n); // spansCreated

      expect(getColumnValue(table, 'entry_type', 5)).toBe('buffer-capacity');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 5))).toBe(128n); // capacity
    });

    test('includes only buffer metrics when no span data', () => {
      const schema = createTestSchema({});
      const SpanBufferClass = getSpanBufferClass(schema);
      const metadata = createTestOpMetadata();
      const buffer = createSpanBuffer(schema, createTestTraceRoot('trace-123'), metadata, undefined);

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

      expect(getColumnValue(table, 'entry_type', 0)).toBe('period-start');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 0))).toBe(0n);

      expect(getColumnValue(table, 'entry_type', 1)).toBe('buffer-writes');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 1))).toBe(10n);

      expect(getColumnValue(table, 'entry_type', 2)).toBe('buffer-spans');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 2))).toBe(2n); // spansCreated

      expect(getColumnValue(table, 'entry_type', 3)).toBe('buffer-capacity');
      expect(toBigInt(getColumnValue(table, 'uint64_value', 3))).toBe(64n); // capacity
    });
  });

  describe('leased chunked conversion', () => {
    test('pins source chunks until idempotent release and preserves exact schema, nulls, and tree preorder', () => {
      const schema = createTestSchema({ value: S.number(), label: S.category() });
      const SpanBufferClass = getSpanBufferClass(schema);
      const root = createSpanBuffer(schema, createTestTraceRoot('leased-tree'), DEFAULT_METADATA, 8);
      root.timestamp[0] = 1000n;
      {
        const entryTypes = root.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      root.message(0, 'root-start');
      root.value(0, 11);
      root.label(0, 'root');
      root.timestamp[1] = 1100n;
      {
        const entryTypes = root.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[1] = ENTRY_TYPE_SPAN_OK;
      }
      root._writeIndex = 2;

      const overflow = createOverflowBuffer(root);
      overflow.timestamp[0] = 1200n;
      {
        const entryTypes = overflow.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_INFO;
      }
      overflow.message(0, 'overflow-log');
      overflow.value(0, 22);
      overflow._writeIndex = 1;

      const child = createChildSpanBuffer(root, SpanBufferClass, DEFAULT_METADATA, DEFAULT_METADATA);
      child.timestamp[0] = 1300n;
      {
        const entryTypes = child.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
      }
      child.message(0, 'child-start');
      child.label(0, 'child');
      child.value(0, 0);
      const childValueNulls = child.getNullsIfAllocated('value');
      if (!(childValueNulls instanceof Uint8Array)) throw new Error('Expected child value validity');
      childValueNulls[0] &= ~1;
      child._writeIndex = 1;

      const topology = root._traceRoot._topology;
      const generation = topology.generation;
      const lease = convertSpanTreeToLeasedArrowTable(root);
      expect(lease.released).toBe(false);
      expect(lease.table.names).toEqual([
        'timestamp',
        'trace_id',
        'thread_id',
        'span_id',
        'parent_thread_id',
        'parent_span_id',
        'entry_type',
        'package_name',
        'package_file',
        'git_sha',
        'message',
        'uint64_value',
        'value',
        'label',
      ]);
      const timestampColumn = lease.table.getChild('timestamp');
      if (!timestampColumn) throw new Error('Missing leased timestamp column');
      const rawTimestamps = timestampColumn.data.flatMap((batch) => {
        if (!(batch.values instanceof BigInt64Array)) throw new Error('Expected raw BigInt64Array timestamps');
        return Array.from(batch.values.subarray(0, batch.length));
      });
      expect(rawTimestamps).toEqual([1000n, 1100n, 1200n, 1300n]);
      expect(
        Array.from({ length: lease.table.numRows }, (_, row) => getColumnValue(lease.table, 'message', row)),
      ).toEqual(['root-start', null, 'overflow-log', 'child-start']);
      expect(
        Array.from({ length: lease.table.numRows }, (_, row) => getColumnValue(lease.table, 'value', row)),
      ).toEqual([11, null, 22, null]);
      expect(
        Array.from({ length: lease.table.numRows }, (_, row) => getColumnValue(lease.table, 'label', row)),
      ).toEqual(['root', null, null, 'child']);

      const valueColumn = lease.table.getChild('value');
      if (!valueColumn) throw new Error('Missing leased value column');
      const sourceColumns = [root, overflow, child].map((buffer) => {
        const source = buffer.getColumnIfAllocated('value');
        if (!(source instanceof Float64Array)) throw new Error('Expected source Float64Array');
        return source;
      });
      expect(valueColumn.data).toHaveLength(3);
      expect(valueColumn.data.map((batch) => batch.values.buffer)).toEqual(
        sourceColumns.map((source) => source.buffer),
      );
      expect(topology.generation).toBe(generation);
      expect(() => topology.assertLive(root)).not.toThrow();

      root._traceRoot.tracer.bufferStrategy.releaseBuffer(root);
      expect(topology.generation).toBe(generation);
      expect(() => topology.assertLive(root)).toThrow('stale');
      expect(getColumnValue(lease.table, 'message', 2)).toBe('overflow-log');
      lease.release();
      expect(lease.released).toBe(true);
      expect(topology.generation).toBe(generation + 1);
      expect(() => topology.assertLive(root)).toThrow('stale');
      expect(() => lease.release()).not.toThrow();
      expect(() => lease[Symbol.dispose]()).not.toThrow();
    });

    test('borrows nonpacked entry types but derives packed entry types without aliasing row headers', () => {
      const schema = createTestSchema({ value: S.number() });
      for (const physicalLayout of ['current', 'specialized', 'packed'] as const) {
        const SpanBufferClass = getSpanBufferClass(schema, 'mixed', physicalLayout);
        const root = createSpanBuffer(
          schema,
          createTestTraceRoot(`leased-entry-${physicalLayout}`),
          DEFAULT_METADATA,
          8,
          SpanBufferClass,
        );
        root.timestamp[0] = 1000n;
        root.timestamp[1] = 1100n;
        root.timestamp[2] = 1200n;
        if (physicalLayout !== 'packed') {
          if (root.entry_type === undefined) throw new Error('Expected split entry-type lane');
          {
            const entryTypes = root.entry_type;
            if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
            entryTypes[0] = ENTRY_TYPE_SPAN_START;
          }
          {
            const entryTypes = root.entry_type;
            if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
            entryTypes[1] = ENTRY_TYPE_INFO;
          }
          {
            const entryTypes = root.entry_type;
            if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
            entryTypes[2] = ENTRY_TYPE_SPAN_OK;
          }
        } else {
          if (root._rowHeaders === undefined) throw new Error('Expected packed row-header lane');
          root._rowHeaders[0] = ENTRY_TYPE_SPAN_START;
          root._rowHeaders[1] = ENTRY_TYPE_INFO;
          root._rowHeaders[2] = ENTRY_TYPE_SPAN_OK;
        }
        root.message(0, `${physicalLayout}-root`);
        root.message(1, `${physicalLayout}-raw`);
        root.value(1, 42);
        root._writeIndex = 3;

        const lease = convertToLeasedArrowTable(root);
        expect(Array.from({ length: 3 }, (_, row) => getColumnValue(lease.table, 'entry_type', row))).toEqual([
          'span-start',
          'info',
          'span-ok',
        ]);
        expect(Array.from({ length: 3 }, (_, row) => getColumnValue(lease.table, 'message', row))).toEqual([
          `${physicalLayout}-root`,
          `${physicalLayout}-raw`,
          null,
        ]);
        const entryTypes = lease.table.getChild('entry_type');
        if (!entryTypes) throw new Error('Missing leased entry_type column');
        const values = entryTypes.data[0]?.values;
        if (!(values instanceof Int8Array)) throw new Error('Expected dictionary entry-type indices');
        if (physicalLayout !== 'packed') {
          if (root.entry_type === undefined) throw new Error('Expected split entry-type lane');
          expect(values.buffer).toBe(root.entry_type.buffer);
        } else {
          if (root._rowHeaders === undefined) throw new Error('Expected packed row-header lane');
          expect(values.buffer).not.toBe(root._rowHeaders.buffer);
        }
        lease.release();
      }
    });

    test('keeps the single-buffer Table API semantically identical while exposing leased source reuse', () => {
      const schema = createTestSchema({ value: S.number() });
      const root = createSpanBuffer(schema, createTestTraceRoot('leased-single'), DEFAULT_METADATA, 8);
      root.timestamp[0] = 2000n;
      {
        const entryTypes = root.entry_type;
        if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
        entryTypes[0] = ENTRY_TYPE_INFO;
      }
      root.message(0, 'single');
      root.value(0, 42);
      root._writeIndex = 1;

      const legacy = convertToArrowTable(root);
      const lease = convertToLeasedArrowTable(root);
      expect(lease.table.names).toEqual(legacy.names);
      expect(lease.table.numRows).toBe(legacy.numRows);
      for (const name of ['timestamp', 'entry_type', 'message', 'value']) {
        expect(getColumnValue(lease.table, name, 0)).toEqual(getColumnValue(legacy, name, 0));
      }
      const source = root.getColumnIfAllocated('value');
      if (!(source instanceof Float64Array)) throw new Error('Expected source Float64Array');
      const valueColumn = lease.table.getChild('value');
      if (!valueColumn) throw new Error('Missing leased value column');
      expect(valueColumn.data[0]?.values.buffer).toBe(source.buffer);
      root._traceRoot.tracer.bufferStrategy.releaseBuffer(root);
      lease[Symbol.dispose]();
      expect(lease.released).toBe(true);
    });
  });
});
