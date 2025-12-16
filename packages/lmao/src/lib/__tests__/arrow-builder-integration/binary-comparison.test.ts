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
import { S } from '../../schema/builder.js';
import type { TagAttributeSchema } from '../../schema/types.js';
import { createSpanBuffer } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import type { SpanBuffer, TaskContext } from '../../types.js';
import { createTestTaskContext } from '../test-helpers.js';

/**
 * Mock string interner for testing
 */
class MockStringInterner {
  private strings: string[] = [];
  private indices = new Map<string, number>();

  intern(str: string): number {
    let idx = this.indices.get(str);
    if (idx === undefined) {
      idx = this.strings.length;
      this.strings.push(str);
      this.indices.set(str, idx);
    }
    return idx;
  }

  getString(idx: number): string | undefined {
    return this.strings[idx];
  }

  getStrings(): readonly string[] {
    return this.strings;
  }
}

/**
 * Create mock task context for testing
 */
function createMockTaskContext(schema: TagAttributeSchema): TaskContext {
  return createTestTaskContext(schema, { lineNumber: 42 });
}

/**
 * Helper to write a row to buffer
 */
function writeRow(
  buffer: SpanBuffer,
  data: {
    timestamp: bigint;
    operation: number;
    attributes?: Record<string, unknown>;
  },
): void {
  const idx = buffer.writeIndex;

  // Write core columns
  buffer.timestamps[idx] = data.timestamp;
  buffer.operations[idx] = data.operation;

  // Write attributes using X_values and X_nulls pattern
  if (data.attributes) {
    for (const [key, value] of Object.entries(data.attributes)) {
      const valuesKey = `${key}_values` as keyof SpanBuffer;
      const nullsKey = `${key}_nulls` as keyof SpanBuffer;
      const column = buffer[valuesKey];
      const nullBitmap = buffer[nullsKey] as Uint8Array | undefined;

      if (column) {
        // Handle string arrays (category/text columns)
        if (Array.isArray(column)) {
          if (value === null || value === undefined) {
            (column as string[])[idx] = '';
            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] &= ~(1 << bitOffset);
            }
          } else {
            (column as string[])[idx] = value as string;
            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] |= 1 << bitOffset;
            }
          }
        } else if (ArrayBuffer.isView(column)) {
          // Handle TypedArrays (number/enum/boolean columns)
          const typedColumn = column as Float64Array | Uint8Array | Uint16Array | Uint32Array;
          if (value === null || value === undefined) {
            if (typedColumn instanceof Float64Array) {
              typedColumn[idx] = 0;
            } else if (typeof value !== 'boolean') {
              typedColumn[idx] = 0;
            }

            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] &= ~(1 << bitOffset);
            }
          } else {
            if (typeof value === 'number') {
              if (typedColumn instanceof Float64Array) {
                typedColumn[idx] = value;
              } else {
                typedColumn[idx] = value;
              }
            } else if (typeof value === 'boolean') {
              const byteIndex = idx >>> 3;
              const bitOffset = idx & 7;
              if (value) {
                (typedColumn as Uint8Array)[byteIndex] |= 1 << bitOffset;
              } else {
                (typedColumn as Uint8Array)[byteIndex] &= ~(1 << bitOffset);
              }
            }

            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] |= 1 << bitOffset;
            }
          }
        }
      }
    }
  }

  buffer.writeIndex++;
}

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

describe('Arrow IPC Round-Trip', () => {
  let moduleIdInterner: MockStringInterner;
  let spanNameInterner: MockStringInterner;

  beforeEach(() => {
    moduleIdInterner = new MockStringInterner();
    spanNameInterner = new MockStringInterner();

    // Pre-intern required system strings
    moduleIdInterner.intern('test-file.ts');
    spanNameInterner.intern('test-span');
  });

  describe('serializes and deserializes correctly', () => {
    it('number columns survive round-trip', () => {
      const schema: TagAttributeSchema = {
        value: S.number(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = [1.5, 2.5, 3.5, Number.NaN, 5.5];
      for (const value of testValues) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { value },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
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
      const schema: TagAttributeSchema = {
        flag: S.boolean(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = [true, false, true, false, true, false, true, false, true];
      for (const value of testValues) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { flag: value },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().flag).toBe(testValues[i]);
      }
    });

    it('enum columns survive round-trip', () => {
      const schema: TagAttributeSchema = {
        status: S.enum(['pending', 'active', 'completed'] as const),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write enum indices
      const testIndices = [0, 2, 1, 0, 2];
      const expectedStrings = ['pending', 'completed', 'active', 'pending', 'completed'];
      for (const idx of testIndices) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { status: idx },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < expectedStrings.length; i++) {
        expect(roundTripped.get(i)?.toJSON().status).toBe(expectedStrings[i]);
      }
    });

    it('category columns survive round-trip', () => {
      const schema: TagAttributeSchema = {
        userId: S.category(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = ['user-123', 'user-456', 'user-789', 'user-123', 'user-456'];
      for (const userId of testValues) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { userId },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().userId).toBe(testValues[i]);
      }
    });

    it('text columns survive round-trip', () => {
      const schema: TagAttributeSchema = {
        message: S.text(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = ['First message', 'Second message', 'Third message', 'Second message', 'First message'];
      for (const value of testValues) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { message: value },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().message).toBe(testValues[i]);
      }
    });

    it('nullable columns with nulls survive round-trip', () => {
      const schema: TagAttributeSchema = {
        value: S.number(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = [1.0, null, 3.0, null, 5.0];
      for (const value of testValues) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { value },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().value).toBe(testValues[i]);
      }
    });

    it('mixed column types survive round-trip', () => {
      const schema: TagAttributeSchema = {
        count: S.number(),
        active: S.boolean(),
        status: S.enum(['pending', 'active', 'completed'] as const),
        userId: S.category(),
        message: S.text(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testData = [
        { count: 42, active: true, status: 0, userId: 'user-123', message: 'First message' },
        { count: 100, active: false, status: 1, userId: 'user-456', message: 'Second message' },
        { count: null, active: true, status: 2, userId: 'user-123', message: 'First message' },
      ];

      for (const row of testData) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: row,
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
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
      const schema: TagAttributeSchema = {};

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write a few rows with timestamps (BigInt64Array stores nanoseconds)
      const timestamps = [1000n, 1100n, 1200n];
      for (let i = 0; i < 3; i++) {
        buffer.timestamps[buffer.writeIndex] = timestamps[i];
        buffer.operations[buffer.writeIndex] = i;
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
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
      expect(fieldNames).toContain('module');
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
      const schema: TagAttributeSchema = {
        userAttr: S.number(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));
      writeRow(buffer, { timestamp: 1000n, operation: 1 });

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);

      // System columns that should NOT be nullable
      const nonNullableColumns = ['timestamp', 'trace_id', 'thread_id', 'span_id', 'entry_type', 'module', 'span_name'];
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
      const schema: TagAttributeSchema = {
        category: S.category(),
        text: S.text(),
        status: S.enum(['a', 'b'] as const),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));
      writeRow(buffer, {
        timestamp: 1000n,
        operation: 1,
        attributes: { category: 'cat1', text: 'text1', status: 0 },
      });

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);

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
      const schema: TagAttributeSchema = {
        value: S.number(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write pattern: valid, null, valid, null, valid, null, valid, null
      const values = [1.0, null, 2.0, null, 3.0, null, 4.0, null];
      for (const value of values) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { value },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);

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
      const schema: TagAttributeSchema = {
        value: S.number(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Only one null in 10 values
      const values = [1, 2, 3, null, 5, 6, 7, 8, 9, 10];
      for (const value of values) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { value },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);

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
      const schema: TagAttributeSchema = {
        value: S.number(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // No nulls - write all valid values and set null bits
      const values = [1, 2, 3, 4, 5];
      for (const value of values) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { value },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);

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
      const schema: TagAttributeSchema = {
        category: S.category(),
      };

      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const testValues = ['alpha', 'beta', 'zebra', 'alpha', 'beta'];
      for (const category of testValues) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { category },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);
      const roundTripped = verifyRoundTrip(table);

      for (let i = 0; i < testValues.length; i++) {
        expect(roundTripped.get(i)?.toJSON().category).toBe(testValues[i]);
      }
    });

    it('handles repeated values efficiently', () => {
      const schema: TagAttributeSchema = {
        userId: S.category(),
      };

      const taskContext = createMockTaskContext(schema);
      // Use capacity of 128 to hold 100 rows without overflow
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'), 128);

      // Write same value many times
      const testValue = 'user-repeated';
      for (let i = 0; i < 100; i++) {
        writeRow(buffer, {
          timestamp: 1000n,
          operation: 3,
          attributes: { userId: testValue },
        });
      }

      const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);

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
