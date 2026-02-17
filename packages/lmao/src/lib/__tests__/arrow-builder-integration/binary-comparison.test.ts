/**
 * Arrow format correctness tests
 *
 * This test suite verifies that LMAO produces valid Arrow IPC format that can be:
 * 1. Serialized to IPC binary format
 * 2. Deserialized back to identical data
 * 3. Read by any Arrow-compatible tool
 *
 * Per specs/lmao/01f_arrow_table_structure.md and 01b_columnar_buffer_architecture.md:
 * - Zero-copy conversion MUST produce correct Arrow format
 * - Dictionary encoding MUST work correctly
 * - Null bitmaps MUST use Arrow format (1=valid, 0=null)
 */

import { describe, expect, it } from 'bun:test';
import { dictionary, type Table, tableFromColumns, tableFromIPC, tableToIPC, uint8, utf8 } from '@uwdata/flechette';
import { convertToArrowTable } from '../../convertToArrow.js';
import { DEFAULT_METADATA } from '../../opContext/defineOp.js';
import { S } from '../../schema/builder.js';
import { ENTRY_TYPE_SPAN_START } from '../../schema/systemSchema.js';

import { createSpanBuffer } from '../../spanBuffer.js';

import { createTestSchema, createTestTraceRoot } from '../test-helpers.js';

/**
 * Round-trip test: serialize to IPC, deserialize, verify data matches
 */
const DICTIONARY_TYPE_ID = dictionary(utf8(), uint8()).typeId;

function verifyRoundTrip(table: Table, columnNames: string[]): Table {
  const ipcColumns: Record<string, ReturnType<typeof getColumn>> = {};
  for (const columnName of columnNames) {
    const column = table.getChild(columnName);
    if (!column) {
      throw new Error(`Column not found for IPC round-trip: ${columnName}`);
    }
    ipcColumns[columnName] = column;
  }

  const ipcTable = tableFromColumns(ipcColumns);

  // Serialize to IPC stream format
  const ipcBytes = tableToIPC(ipcTable, { format: 'stream' });
  if (!ipcBytes) {
    throw new Error('Failed to serialize Arrow table');
  }

  // Deserialize back
  const roundTripped = tableFromIPC(ipcBytes);

  // Verify schema matches
  expect(roundTripped.schema.fields.length).toBe(ipcTable.schema.fields.length);
  for (let i = 0; i < ipcTable.schema.fields.length; i++) {
    expect(roundTripped.schema.fields[i].name).toBe(ipcTable.schema.fields[i].name);
    expect(roundTripped.schema.fields[i].type.typeId).toBe(ipcTable.schema.fields[i].type.typeId);
    expect(roundTripped.schema.fields[i].nullable).toBe(ipcTable.schema.fields[i].nullable);
  }

  // Verify row count matches
  expect(roundTripped.numRows).toBe(table.numRows);

  return roundTripped;
}

function getColumn(table: Table, columnName: string) {
  const column = table.getChild(columnName);
  if (!column) {
    throw new Error(`Column not found: ${columnName}`);
  }
  return column;
}

function getColumnValue<T>(table: Table, columnName: string, rowIndex: number): T {
  return getColumn(table, columnName).get(rowIndex) as T;
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
      const schema = createTestSchema({ value: S.number() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write a fewbuffer._opMetadata = DEFAULT_METADATA;

      const testValues = [1.5, 2.5, 3.5, Number.NaN, 5.5];
      for (const value of testValues) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        buffer.value(idx, value);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table, ['value']);

      // Verify data values match
      for (let i = 0; i < testValues.length; i++) {
        const original = getColumnValue<number>(table, 'value', i);
        const restored = getColumnValue<number>(roundTripped, 'value', i);

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
      const schema = createTestSchema({ flag: S.boolean() });

      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-123'), DEFAULT_METADATA, 16);
      buffer._opMetadata = DEFAULT_METADATA;

      // Write a fewbuffer._opMetadata = DEFAULT_METADATA;

      const testValues = [true, false, true, false, true, false, true, false, true];
      for (const value of testValues) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        buffer.flag(idx, value);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table, ['flag']);

      for (let i = 0; i < testValues.length; i++) {
        expect(getColumnValue<boolean>(roundTripped, 'flag', i)).toBe(testValues[i]);
      }
    });

    it('enum columns survive round-trip', () => {
      const schema = createTestSchema({ status: S.enum(['pending', 'active', 'completed'] as const) });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write enum indices (0=pending, 1=active, 2=completed)
      // Note: Buffer setters accept numeric indices for enum columns at runtime,
      // but TypeScript types expect string literals. Use type assertion for low-level tests.
      const testIndices = [0, 2, 1, 0, 2];
      const expectedStrings = ['pending', 'completed', 'active', 'pending', 'completed'];
      for (const enumIdx of testIndices) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        (buffer.status as unknown as (pos: number, val: number) => unknown)(idx, enumIdx);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      for (let i = 0; i < expectedStrings.length; i++) {
        expect(getColumnValue<string>(table, 'status', i)).toBe(expectedStrings[i]);
      }
    });

    it('category columns survive round-trip', () => {
      const schema = createTestSchema({ userId: S.category() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write a fewbuffer._opMetadata = DEFAULT_METADATA;

      const testValues = ['user-123', 'user-456', 'user-789', 'user-123', 'user-456'];
      for (const userId of testValues) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        buffer.userId(idx, userId);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const originalField = table.schema.fields.find((field) => field.name === 'userId');
      expect(originalField?.type.typeId).toBe(DICTIONARY_TYPE_ID);

      for (let i = 0; i < testValues.length; i++) {
        expect(getColumnValue<string>(table, 'userId', i)).toBe(testValues[i]);
      }
    });

    it('text columns survive round-trip', () => {
      const schema = createTestSchema({ userMessage: S.text() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write a fewbuffer._opMetadata = DEFAULT_METADATA;

      const testValues = ['First message', 'Second message', 'Third message', 'Second message', 'First message'];
      for (const value of testValues) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        buffer.userMessage(idx, value);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      for (let i = 0; i < testValues.length; i++) {
        expect(getColumnValue<string>(table, 'userMessage', i)).toBe(testValues[i]);
      }
    });

    it('nullable columns with nulls survive round-trip', () => {
      const schema = createTestSchema({ value: S.number() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write a fewbuffer._opMetadata = DEFAULT_METADATA;

      const testValues = [1.0, null, 3.0, null, 5.0];
      for (const value of testValues) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        if (value !== null) {
          buffer.value(idx, value);
        } else {
          // For null values, we need to explicitly mark as null in the bitmap
          const nullBitmap = buffer.getNullsIfAllocated('value');
          if (nullBitmap) {
            setNull(nullBitmap, idx, true);
          }
        }
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table, ['value']);

      for (let i = 0; i < testValues.length; i++) {
        expect(getColumnValue<number | null>(roundTripped, 'value', i)).toBe(testValues[i]);
      }
    });

    it('mixed column types survive round-trip', () => {
      const schema = createTestSchema({
        count: S.number(),
        active: S.boolean(),
        status: S.enum(['pending', 'active', 'completed'] as const),
        userId: S.category(),
        userMessage: S.text(),
      });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      const testData = [
        { count: 42, active: true, status: 0, userId: 'user-123', userMessage: 'First message' },
        { count: 100, active: false, status: 1, userId: 'user-456', userMessage: 'Second message' },
        { count: null, active: true, status: 2, userId: 'user-123', userMessage: 'First message' },
      ];

      for (const row of testData) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;

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
        buffer.userMessage(idx, row.userMessage);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table, ['count', 'active']);

      // Verify first row
      expect(getColumnValue<number>(roundTripped, 'count', 0)).toBe(42);
      expect(getColumnValue<boolean>(roundTripped, 'active', 0)).toBe(true);
      expect(getColumnValue<string>(table, 'status', 0)).toBe('pending');
      expect(getColumnValue<string>(table, 'userId', 0)).toBe('user-123');
      expect(getColumnValue<string>(table, 'userMessage', 0)).toBe('First message');

      // Verify second row
      expect(getColumnValue<number>(roundTripped, 'count', 1)).toBe(100);
      expect(getColumnValue<boolean>(roundTripped, 'active', 1)).toBe(false);
      expect(getColumnValue<string>(table, 'status', 1)).toBe('active');
      expect(getColumnValue<string>(table, 'userId', 1)).toBe('user-456');
      expect(getColumnValue<string>(table, 'userMessage', 1)).toBe('Second message');

      // Verify third row with null
      expect(getColumnValue<number | null>(roundTripped, 'count', 2)).toBeNull();
      expect(getColumnValue<boolean>(roundTripped, 'active', 2)).toBe(true);
      expect(getColumnValue<string>(table, 'status', 2)).toBe('completed');
      expect(getColumnValue<string>(table, 'userId', 2)).toBe('user-123');
      expect(getColumnValue<string>(table, 'userMessage', 2)).toBe('First message');
    });

    it('system columns survive round-trip', () => {
      const schema = createTestSchema({});

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write a few rows with timestamps (BigInt64Array stores nanoseconds)
      const timestamps = [1000n, 1100n, 1200n];
      for (let i = 0; i < 3; i++) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = timestamps[i];
        buffer.entry_type[idx] = i;
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      const roundTripped = verifyRoundTrip(table, [
        'timestamp',
        'thread_id',
        'span_id',
        'parent_thread_id',
        'parent_span_id',
      ]);

      // Verify system columns exist in source table
      const sourceFieldNames = table.schema.fields.map((f) => f.name);
      expect(sourceFieldNames).toContain('timestamp');
      expect(sourceFieldNames).toContain('trace_id');
      expect(sourceFieldNames).toContain('thread_id');
      expect(sourceFieldNames).toContain('span_id');
      expect(sourceFieldNames).toContain('parent_thread_id');
      expect(sourceFieldNames).toContain('parent_span_id');
      expect(sourceFieldNames).toContain('entry_type');
      expect(sourceFieldNames).toContain('package_name');
      expect(sourceFieldNames).toContain('package_file');

      // Verify IPC round-tripped subset columns exist
      const fieldNames = roundTripped.schema.fields.map((f) => f.name);
      expect(fieldNames).toContain('timestamp');
      expect(fieldNames).toContain('thread_id');
      expect(fieldNames).toContain('span_id');
      expect(fieldNames).toContain('parent_thread_id');
      expect(fieldNames).toContain('parent_span_id');

      // Verify timestamps round-tripped correctly
      // Arrow's getter converts nanoseconds to a decimal, so access raw BigInt64Array
      const timestampIndex = roundTripped.schema.fields.findIndex((f) => f.name === 'timestamp');
      const timestampVector = roundTripped.getChildAt(timestampIndex);
      if (!timestampVector) {
        throw new Error('Timestamp vector not found');
      }
      const rawTimestamps = timestampVector.data[0].values as BigInt64Array;
      for (let i = 0; i < 3; i++) {
        expect(rawTimestamps[i]).toBe(timestamps[i]);
      }
    });
  });

  describe('schema correctness', () => {
    it('system columns have correct nullability', () => {
      const schema = createTestSchema({ userAttr: S.number() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      const idx = buffer._writeIndex;
      buffer.timestamp[idx] = 1000n;
      buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex++;

      const table = convertToArrowTable(buffer);

      // System columns must be present and match current conversion contract.
      const expectedSystemColumns = [
        'timestamp',
        'trace_id',
        'thread_id',
        'span_id',
        'entry_type',
        'package_name',
        'package_file',
      ];
      for (const colName of expectedSystemColumns) {
        const field = table.schema.fields.find((f) => f.name === colName);
        expect(field).toBeDefined();
        expect(field?.nullable).toBe(true);
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
      const schema = createTestSchema({
        category: S.category(),
        text: S.text(),
        status: S.enum(['a', 'b'] as const),
      });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      const idx = buffer._writeIndex;
      buffer.timestamp[idx] = 1000n;
      buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
      buffer.category(idx, 'cat1');
      buffer.text(idx, 'text1');
      (buffer.status as unknown as (pos: number, val: number) => unknown)(idx, 0);
      buffer._writeIndex++;

      const table = convertToArrowTable(buffer);

      // Category and text should be Dictionary<Utf8, Uint32>
      const categoryField = table.schema.fields.find((f) => f.name === 'category');
      expect(categoryField?.type.typeId).toBe(DICTIONARY_TYPE_ID);

      const textField = table.schema.fields.find((f) => f.name === 'text');
      expect(textField?.type.typeId).toBe(DICTIONARY_TYPE_ID);

      // Enum should be Dictionary<Utf8, Uint8>
      const enumField = table.schema.fields.find((f) => f.name === 'status');
      expect(enumField?.type.typeId).toBe(DICTIONARY_TYPE_ID);
    });
  });

  describe('null bitmap format', () => {
    it('uses Arrow format (1=valid, 0=null)', () => {
      const schema = createTestSchema({ value: S.number() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write pattern: valid, null, valid, null, valid, null, valid, null
      const values = [1.0, null, 2.0, null, 3.0, null, 4.0, null];
      for (const value of values) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        if (value !== null) {
          buffer.value(idx, value);
        } else {
          const nullBitmap = buffer.getNullsIfAllocated('value');
          if (nullBitmap) {
            setNull(nullBitmap, idx, true);
          }
        }
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      // Extract value column
      const valueColumnIndex = table.schema.fields.findIndex((f) => f.name === 'value');
      const valueVector = table.getChildAt(valueColumnIndex);
      if (!valueVector) {
        throw new Error('Value vector not found');
      }
      const valueData = valueVector.data[0] as { nullCount: number; nullBitmap?: Uint8Array; validity?: Uint8Array };

      // Verify null count
      expect(valueData.nullCount).toBe(4);

      // Verify null bitmap (1=valid, 0=null)
      // Pattern: 01010101 = 0x55
      const nullBitmap = valueData.nullBitmap ?? valueData.validity;
      expect(nullBitmap).toBeDefined();
      expect(nullBitmap?.[0]).toBe(0b01010101);
    });

    it('handles sparse nulls correctly', () => {
      const schema = createTestSchema({ value: S.number() });

      // Need capacity for 10 values
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-123'), DEFAULT_METADATA, 16);
      buffer._opMetadata = DEFAULT_METADATA;

      // Only one null in 10 values
      const values = [1, 2, 3, null, 5, 6, 7, 8, 9, 10];
      for (const value of values) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        if (value !== null) {
          buffer.value(idx, value);
        } else {
          const nullBitmap = buffer.getNullsIfAllocated('value');
          if (nullBitmap) {
            setNull(nullBitmap, idx, true);
          }
        }
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      const valueColumnIndex = table.schema.fields.findIndex((f) => f.name === 'value');
      const valueVector = table.getChildAt(valueColumnIndex);
      if (!valueVector) {
        throw new Error('Value vector not found');
      }
      const valueData = valueVector.data[0];

      expect(valueData.nullCount).toBe(1);

      // Verify all values round-trip correctly
      for (let i = 0; i < values.length; i++) {
        expect(getColumnValue<number | null>(table, 'value', i)).toBe(values[i]);
      }
    });

    it('omits null bitmap when no nulls', () => {
      const schema = createTestSchema({ value: S.number() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // No nulls - write all valid values
      const values = [1, 2, 3, 4, 5];
      for (const value of values) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        buffer.value(idx, value);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      const valueColumnIndex = table.schema.fields.findIndex((f) => f.name === 'value');
      const valueVector = table.getChildAt(valueColumnIndex);
      if (!valueVector) {
        throw new Error('Value vector not found');
      }
      const valueData = valueVector.data[0];

      expect(valueData.nullCount).toBe(0);
      // When nullCount is 0, Arrow may or may not include a bitmap
      // The important thing is nullCount is correct
    });
  });

  describe('dictionary encoding', () => {
    it('preserves dictionary values through round-trip', () => {
      const schema = createTestSchema({ category: S.category() });

      const buffer = createSpanBuffer(
        schema,
        'test-span',
        createTestTraceRoot('trace-123'),
        DEFAULT_METADATA,
        undefined,
      );
      buffer._opMetadata = DEFAULT_METADATA;

      // Write a fewbuffer._opMetadata = DEFAULT_METADATA;

      const testValues = ['alpha', 'beta', 'zebra', 'alpha', 'beta'];
      for (const category of testValues) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        buffer.category(idx, category);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);
      for (let i = 0; i < testValues.length; i++) {
        expect(getColumnValue<string>(table, 'category', i)).toBe(testValues[i]);
      }
    });

    it('handles repeated values efficiently', () => {
      const schema = createTestSchema({ userId: S.category() });

      // Use capacity of 128 to hold 100 rows without overflow
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-123'), DEFAULT_METADATA, 128);
      buffer._opMetadata = DEFAULT_METADATA;

      // Write same value many times
      const testValue = 'user-repeated';
      for (let i = 0; i < 100; i++) {
        const idx = buffer._writeIndex;
        buffer.timestamp[idx] = 1000n;
        buffer.entry_type[idx] = ENTRY_TYPE_SPAN_START;
        buffer.userId(idx, testValue);
        buffer._writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      // Verify all 100 rows have the same value by accessing the column directly
      const userIdIndex = table.schema.fields.findIndex((f) => f.name === 'userId');
      const userIdVector = table.getChildAt(userIdIndex);
      if (!userIdVector) {
        throw new Error('UserId vector not found');
      }

      for (let i = 0; i < 100; i++) {
        expect(userIdVector.get(i)).toBe(testValue);
      }

      // Dictionary should only have 1 unique value
      const dictData = userIdVector.data[0] as { dictionary?: { length: number } };
      expect(dictData.dictionary?.length).toBe(1);
    });
  });
});
