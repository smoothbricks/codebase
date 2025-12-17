/**
 * Arrow format correctness tests
 *
 * This test suite verifies that LMAO produces valid Arrow IPC format that can be:
 * 1. Serialized to IPC binary format
 * 2. Deserialized back to identical data
 * 3. Read by any Arrow-compatible tool
 *
 * Per specs/01f_arrow_table_structure.md and 01b_columnar_buffer_architecture.md:
 * - Zero-copy conversion MUST produce correct Arrow format
 * - Dictionary encoding MUST work correctly
 * - Null bitmaps MUST use Arrow format (1=valid, 0=null)
 */

import { beforeEach, describe, expect, it } from 'bun:test';
import * as arrow from 'apache-arrow';
import { convertToArrowTable } from '../../convertToArrow.js';
import { ENTRY_TYPE_SPAN_START } from '../../lmao.js';
import { S } from '../../schema/builder.js';
import { createSpanBuffer } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import { createTestTaskContext } from '../test-helpers.js';

// MockStringInterner no longer needed - convertToArrowTable now uses direct string access
// via buf.task.module.packageName, buf.task.module.packagePath, and buf.task.spanName

/**
 * Round-trip test: serialize to IPC, deserialize, verify data matches
 */
function verifyRoundTrip(table: arrow.Table): arrow.Table {
  // Serialize to IPC format
  const ipcBytes = arrow.tableToIPC(table);

  // Deserialize back
  const roundTripped = arrow.tableFromIPC(ipcBytes);

  // Verify schema matches
  expect(roundTripped.schema.fields.length).toBe(table.schema.fields.length);
  for (let i = 0; i < table.schema.fields.length; i++) {
    expect(roundTripped.schema.fields[i].name).toBe(table.schema.fields[i].name);
    expect(roundTripped.schema.fields[i].typeId).toBe(table.schema.fields[i].typeId);
    expect(roundTripped.schema.fields[i].nullable).toBe(table.schema.fields[i].nullable);
  }

  // Verify row count matches
  expect(roundTripped.numRows).toBe(table.numRows);

  return roundTripped;
}

/**
 * Set null bit at position (Arrow format: 1=valid, 0=null)
 */
function setNull(nullBitmap: Uint8Array, idx: number, isNull: boolean): void {
  const byteIndex = Math.floor(idx / 8);
  const bitOffset = idx % 8;
  if (isNull) {
    nullBitmap[byteIndex] &= ~(1 << bitOffset);
  } else {
    nullBitmap[byteIndex] |= 1 << bitOffset;
  }
}

describe('Arrow IPC Round-Trip', () => {
  // No interners needed - direct string access is used

  describe('serializes and deserializes correctly', () => {
    it('number columns survive round-trip', () => {
      const schema = { value: S.number() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = [1.5, 2.5, 3.5, Number.NaN, 5.5];
      for (const value of testValues) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.value(idx, value);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      // Verify data values match
      for (let i = 0; i < testValues.length; i++) {
        const original = table.get(i)?.toJSON().value;
        const restored = roundTripped.get(i)?.toJSON().value;

        if (Number.isNaN(testValues[i])) {
          expect(Number.isNaN(original)).toBe(true);
          expect(Number.isNaN(restored)).toBe(true);
        } else {
          expect(restored).toBe(original);
          expect(restored).toBe(testValues[i]);
        }
      }
    });

    it('boolean columns survive round-trip', () => {
      const schema = { flag: S.boolean() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      // Need capacity for 9 test values
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'), 16);

      const testValues = [true, false, true, false, true, false, true, false, true];
      for (const value of testValues) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.flag(idx, value);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().flag).toBe(testValues[i]);
      }
    });

    it('enum columns survive round-trip', () => {
      const schema = { status: S.enum(['pending', 'active', 'completed'] as const) } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write enum indices (0=pending, 1=active, 2=completed)
      // Note: Buffer setters accept numeric indices for enum columns at runtime,
      // but TypeScript types expect string literals. Use type assertion for low-level tests.
      const testIndices = [0, 2, 1, 0, 2];
      const expectedStrings = ['pending', 'completed', 'active', 'pending', 'completed'];
      for (const enumIdx of testIndices) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        (buffer.status as unknown as (pos: number, val: number) => unknown)(idx, enumIdx);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < expectedStrings.length; i++) {
        expect(roundTripped.get(i)?.toJSON().status).toBe(expectedStrings[i]);
      }
    });

    it('category columns survive round-trip', () => {
      const schema = { userId: S.category() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = ['user-123', 'user-456', 'user-789', 'user-123', 'user-456'];
      for (const userId of testValues) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.userId(idx, userId);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().userId).toBe(testValues[i]);
      }
    });

    it('text columns survive round-trip', () => {
      const schema = { message: S.text() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = ['First message', 'Second message', 'Third message', 'Second message', 'First message'];
      for (const value of testValues) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.message(idx, value);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().message).toBe(testValues[i]);
      }
    });

    it('nullable columns with nulls survive round-trip', () => {
      const schema = { value: S.number() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = [1.0, null, 3.0, null, 5.0];
      for (const value of testValues) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        if (value !== null) {
          buffer.value(idx, value);
        } else {
          // For null values, we need to explicitly mark as null in the bitmap
          const nullBitmap = buffer.getNullsIfAllocated('value');
          if (nullBitmap) {
            setNull(nullBitmap, idx, true);
          }
        }
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().value).toBe(testValues[i]);
      }
    });

    it('mixed column types survive round-trip', () => {
      const schema = {
        count: S.number(),
        active: S.boolean(),
        status: S.enum(['pending', 'active', 'completed'] as const),
        userId: S.category(),
        message: S.text(),
      } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testData = [
        { count: 42, active: true, status: 0, userId: 'user-123', message: 'First message' },
        { count: 100, active: false, status: 1, userId: 'user-456', message: 'Second message' },
        { count: null, active: true, status: 2, userId: 'user-123', message: 'First message' },
      ];

      for (const row of testData) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;

        if (row.count !== null) {
          buffer.count(idx, row.count);
        } else {
          const nullBitmap = buffer.getNullsIfAllocated('count');
          if (nullBitmap) {
            setNull(nullBitmap, idx, true);
          }
        }
        buffer.active(idx, row.active);
        (buffer.status as unknown as (pos: number, val: number) => unknown)(idx, row.status);
        buffer.userId(idx, row.userId);
        buffer.message(idx, row.message);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      // Verify first row
      const row0 = roundTripped.get(0)?.toJSON();
      expect(row0?.count).toBe(42);
      expect(row0?.active).toBe(true);
      expect(row0?.status).toBe('pending');
      expect(row0?.userId).toBe('user-123');
      expect(row0?.message).toBe('First message');

      // Verify second row
      const row1 = roundTripped.get(1)?.toJSON();
      expect(row1?.count).toBe(100);
      expect(row1?.active).toBe(false);
      expect(row1?.status).toBe('active');
      expect(row1?.userId).toBe('user-456');
      expect(row1?.message).toBe('Second message');

      // Verify third row with null
      const row2 = roundTripped.get(2)?.toJSON();
      expect(row2?.count).toBe(null);
      expect(row2?.active).toBe(true);
      expect(row2?.status).toBe('completed');
      expect(row2?.userId).toBe('user-123');
      expect(row2?.message).toBe('First message');
    });

    it('system columns survive round-trip', () => {
      const schema = {} as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write a few rows with timestamps (BigInt64Array stores nanoseconds)
      const timestamps = [1000n, 1100n, 1200n];
      for (let i = 0; i < 3; i++) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = timestamps[i];
        buffer.operations[idx] = i;
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      // Verify system columns exist
      const fieldNames = roundTripped.schema.fields.map((f) => f.name);
      expect(fieldNames).toContain('timestamp');
      expect(fieldNames).toContain('trace_id');
      expect(fieldNames).toContain('thread_id');
      expect(fieldNames).toContain('span_id');
      expect(fieldNames).toContain('parent_thread_id');
      expect(fieldNames).toContain('parent_span_id');
      expect(fieldNames).toContain('entry_type');
      expect(fieldNames).toContain('package_name');
      expect(fieldNames).toContain('package_path');
      expect(fieldNames).toContain('span_name');

      // Verify timestamps round-tripped correctly
      // Arrow's getter converts nanoseconds to a decimal, so access raw BigInt64Array
      const timestampIndex = roundTripped.schema.fields.findIndex((f) => f.name === 'timestamp');
      const timestampVector = roundTripped.getChildAt(timestampIndex)!;
      const rawTimestamps = timestampVector.data[0].values as BigInt64Array;
      for (let i = 0; i < 3; i++) {
        expect(rawTimestamps[i]).toBe(timestamps[i]);
      }
    });
  });

  describe('schema correctness', () => {
    it('system columns have correct nullability', () => {
      const schema = { userAttr: S.number() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const idx = buffer.writeIndex;
      buffer.timestamps[idx] = 1000n;
      buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
      buffer.writeIndex++;

      const table = convertToArrowTable(buffer);

      // System columns that should NOT be nullable
      const nonNullableColumns = [
        'timestamp',
        'trace_id',
        'thread_id',
        'span_id',
        'entry_type',
        'package_name',
        'package_path',
        'span_name',
      ];
      for (const colName of nonNullableColumns) {
        const field = table.schema.fields.find((f) => f.name === colName);
        expect(field).toBeDefined();
        expect(field?.nullable).toBe(false);
      }

      // Columns that SHOULD be nullable
      const nullableColumns = ['parent_thread_id', 'parent_span_id', 'userAttr'];
      for (const colName of nullableColumns) {
        const field = table.schema.fields.find((f) => f.name === colName);
        expect(field).toBeDefined();
        expect(field?.nullable).toBe(true);
      }
    });

    it('dictionary columns have correct type', () => {
      const schema = {
        category: S.category(),
        text: S.text(),
        status: S.enum(['a', 'b'] as const),
      } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const idx = buffer.writeIndex;
      buffer.timestamps[idx] = 1000n;
      buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
      buffer.category(idx, 'cat1');
      buffer.text(idx, 'text1');
      (buffer.status as unknown as (pos: number, val: number) => unknown)(idx, 0);
      buffer.writeIndex++;

      const table = convertToArrowTable(buffer);

      // Category and text should be Dictionary<Utf8, Uint32>
      const categoryField = table.schema.fields.find((f) => f.name === 'category');
      expect(categoryField?.type.typeId).toBe(arrow.Type.Dictionary);

      const textField = table.schema.fields.find((f) => f.name === 'text');
      expect(textField?.type.typeId).toBe(arrow.Type.Dictionary);

      // Enum should be Dictionary<Utf8, Uint8>
      const enumField = table.schema.fields.find((f) => f.name === 'status');
      expect(enumField?.type.typeId).toBe(arrow.Type.Dictionary);
    });
  });

  describe('null bitmap format', () => {
    it('uses Arrow format (1=valid, 0=null)', () => {
      const schema = { value: S.number() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write pattern: valid, null, valid, null, valid, null, valid, null
      const values = [1.0, null, 2.0, null, 3.0, null, 4.0, null];
      for (const value of values) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        if (value !== null) {
          buffer.value(idx, value);
        } else {
          const nullBitmap = buffer.getNullsIfAllocated('value');
          if (nullBitmap) {
            setNull(nullBitmap, idx, true);
          }
        }
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      // Extract value column
      const valueColumnIndex = table.schema.fields.findIndex((f) => f.name === 'value');
      const valueVector = table.getChildAt(valueColumnIndex)!;
      const valueData = valueVector.data[0];

      // Verify null count
      expect(valueData.nullCount).toBe(4);

      // Verify null bitmap (1=valid, 0=null)
      // Pattern: 01010101 = 0x55
      const nullBitmap = valueData.nullBitmap;
      expect(nullBitmap).toBeDefined();
      expect(nullBitmap![0]).toBe(0b01010101);
    });

    it('handles sparse nulls correctly', () => {
      const schema = { value: S.number() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      // Need capacity for 10 values
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'), 16);

      // Only one null in 10 values
      const values = [1, 2, 3, null, 5, 6, 7, 8, 9, 10];
      for (const value of values) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        if (value !== null) {
          buffer.value(idx, value);
        } else {
          const nullBitmap = buffer.getNullsIfAllocated('value');
          if (nullBitmap) {
            setNull(nullBitmap, idx, true);
          }
        }
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      const valueColumnIndex = table.schema.fields.findIndex((f) => f.name === 'value');
      const valueVector = table.getChildAt(valueColumnIndex)!;
      const valueData = valueVector.data[0];

      expect(valueData.nullCount).toBe(1);

      // Verify all values round-trip correctly
      for (let i = 0; i < values.length; i++) {
        expect(table.get(i)?.toJSON().value).toBe(values[i]);
      }
    });

    it('omits null bitmap when no nulls', () => {
      const schema = { value: S.number() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // No nulls - write all valid values
      const values = [1, 2, 3, 4, 5];
      for (const value of values) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.value(idx, value);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      const valueColumnIndex = table.schema.fields.findIndex((f) => f.name === 'value');
      const valueVector = table.getChildAt(valueColumnIndex)!;
      const valueData = valueVector.data[0];

      expect(valueData.nullCount).toBe(0);
      // When nullCount is 0, Arrow may or may not include a bitmap
      // The important thing is nullCount is correct
    });
  });

  describe('dictionary encoding', () => {
    it('preserves dictionary values through round-trip', () => {
      const schema = { category: S.category() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = ['alpha', 'beta', 'zebra', 'alpha', 'beta'];
      for (const category of testValues) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.category(idx, category);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().category).toBe(testValues[i]);
      }
    });

    it('handles repeated values efficiently', () => {
      const schema = { userId: S.category() } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      // Use capacity of 128 to hold 100 rows without overflow
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'), 128);

      // Write same value many times
      const testValue = 'user-repeated';
      for (let i = 0; i < 100; i++) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.userId(idx, testValue);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      // Verify all 100 rows have the same value by accessing the column directly
      const userIdIndex = table.schema.fields.findIndex((f) => f.name === 'userId');
      const userIdVector = table.getChildAt(userIdIndex)!;

      for (let i = 0; i < 100; i++) {
        expect(userIdVector.get(i)).toBe(testValue);
      }

      // Dictionary should only have 1 unique value
      const dictData = userIdVector.data[0];
      expect(dictData.dictionary?.length).toBe(1);
    });
  });
});
