import { describe, expect, it } from 'bun:test';
import { tableFromColumns, tableFromIPC, tableToIPC } from '@uwdata/flechette';
import { convertToArrowTable } from '../../convertToArrow.js';
import { DEFAULT_METADATA } from '../../opContext/defineOp.js';
import { S } from '../../schema/builder.js';
import { ENTRY_TYPE_SPAN_START } from '../../schema/systemSchema.js';
import { createSpanBuffer } from '../../spanBuffer.js';

import { createTestSchema, createTestTraceRoot, createTraceId } from '../test-helpers.js';

function serializeToIpcFile(table: ReturnType<typeof convertToArrowTable>, columnNames: string[]): Uint8Array {
  if (columnNames.length === 0) {
    const ipcBytes = tableToIPC(tableFromColumns({}), { format: 'file' });
    if (!ipcBytes) {
      throw new Error('Failed to serialize empty Arrow table');
    }
    return ipcBytes;
  }

  const ipcColumns: Record<string, NonNullable<ReturnType<typeof table.getChild>>> = {};
  for (const columnName of columnNames) {
    const column = table.getChild(columnName);
    if (!column) {
      throw new Error(`Column not found for IPC file serialization: ${columnName}`);
    }
    ipcColumns[columnName] = column;
  }

  const ipcTable = tableFromColumns(ipcColumns);
  const ipcBytes = tableToIPC(ipcTable, { format: 'file' });
  if (!ipcBytes) {
    throw new Error('Failed to serialize Arrow table');
  }
  return ipcBytes;
}

describe('Arrow Binary Format Compliance', () => {
  describe('IPC Message Structure', () => {
    it('should write correct IPC continuation bytes', () => {
      const schema = createTestSchema({
        numberValue: S.number(),
      });

      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot(traceId), DEFAULT_METADATA, undefined);
      buffer._opMetadata = DEFAULT_METADATA;

      // Donbuffer._opMetadata = DEFAULT_METADATA;

      // Writebuffer.timestamp[0] = BigInt(Date.now()) * 1000000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.numberValue(0, 42);
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const ipcBytes = serializeToIpcFile(table, ['numberValue']);

      // IPC file should start with ARROW1 magic bytes
      const magic = new TextDecoder().decode(ipcBytes.slice(0, 6));
      expect(magic).toBe('ARROW1');
    });
  });

  describe('Endianness', () => {
    it('should preserve number values through round-trip', () => {
      const schema = createTestSchema({
        uint32Value: S.number(),
      });

      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot(traceId), DEFAULT_METADATA, undefined);
      buffer._opMetadata = DEFAULT_METADATA;

      // Write test data using generated methods
      const testValue = 0x12345678;
      buffer.timestamp[0] = BigInt(Date.now()) * 1000000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.uint32Value(0, testValue);
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const ipcBytes = serializeToIpcFile(table, ['uint32Value']);

      // Round-trip test
      const reader = tableFromIPC(ipcBytes);
      expect(reader.numRows).toBe(1);

      // Find our data column (skip system columns)
      for (let i = 0; i < reader.schema.fields.length; i++) {
        const field = reader.schema.fields[i];
        if (field.name === 'uint32Value') {
          const col = reader.getChildAt(i);
          if (col) {
            expect(col.get(0)).toBe(testValue);
          }
          break;
        }
      }
    });
  });

  describe('Null Bitmaps', () => {
    it('should handle null values correctly in round-trip', () => {
      const schema = createTestSchema({
        numberValue: S.number(),
      });

      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot(traceId), DEFAULT_METADATA, undefined);
      buffer._opMetadata = DEFAULT_METADATA;

      // Write data with some nulls
      const testData = [1, null, 3, null, 5];

      for (let i = 0; i < testData.length; i++) {
        const value = testData[i];
        buffer.timestamp[i] = BigInt(Date.now()) * 1000000n;
        buffer.entry_type[i] = ENTRY_TYPE_SPAN_START;

        if (value === null) {
          // Don't call the method for null values, just leave as default (null)
        } else {
          buffer.numberValue(i, value);
        }
      }
      buffer._writeIndex = testData.length;

      const table = convertToArrowTable(buffer);
      const ipcBytes = serializeToIpcFile(table, ['numberValue']);

      // Round-trip test
      const reader = tableFromIPC(ipcBytes);
      expect(reader.numRows).toBe(5);

      // Find our data column and verify null handling
      for (let i = 0; i < reader.schema.fields.length; i++) {
        const field = reader.schema.fields[i];
        if (field.name === 'numberValue') {
          const col = reader.getChildAt(i);
          if (col) {
            expect(col.get(0)).toBe(1);
            expect(col.get(1)).toBe(null);
            expect(col.get(2)).toBe(3);
            expect(col.get(3)).toBe(null);
            expect(col.get(4)).toBe(5);
          }
          break;
        }
      }
    });
  });

  describe('Dictionary Encoding', () => {
    it('should handle enum values correctly in round-trip', () => {
      const schema = createTestSchema({
        enumValue: S.enum(['a', 'b', 'c']),
      });

      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot(traceId), DEFAULT_METADATA, undefined);
      buffer._opMetadata = DEFAULT_METADATA;

      // Write test data using generated methods
      // For enums, we pass the index, not the string value
      const testValues = [0, 1, 2, 0]; // a=0, b=1, c=2, includes repeated value

      for (let i = 0; i < testValues.length; i++) {
        buffer.timestamp[i] = BigInt(Date.now()) * 1000000n;
        buffer.entry_type[i] = ENTRY_TYPE_SPAN_START;
        buffer.enumValue(i, testValues[i]);
      }
      buffer._writeIndex = testValues.length;

      const table = convertToArrowTable(buffer);
      const enumField = table.schema.fields.find((field) => field.name === 'enumValue');
      expect(enumField).toBeDefined();
      expect(enumField?.type.typeId).toBe(-1);

      const enumColumn = table.getChild('enumValue');
      if (!enumColumn) {
        throw new Error('enumValue column not found');
      }
      expect(enumColumn.get(0)).toBe('a');
      expect(enumColumn.get(1)).toBe('b');
      expect(enumColumn.get(2)).toBe('c');
      expect(enumColumn.get(3)).toBe('a');
    });
  });

  describe('Variable Size Binary', () => {
    it('should handle string values correctly in round-trip', () => {
      const schema = createTestSchema({
        textValue: S.text(),
      });

      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot(traceId), DEFAULT_METADATA, undefined);
      buffer._opMetadata = DEFAULT_METADATA;

      // Write test data with various string lengths
      const testValues = ['', 'hello', '', 'world!'];

      for (let i = 0; i < testValues.length; i++) {
        buffer.timestamp[i] = BigInt(Date.now()) * 1000000n;
        buffer.entry_type[i] = ENTRY_TYPE_SPAN_START;
        buffer.textValue(i, testValues[i]);
      }
      buffer._writeIndex = testValues.length;

      const table = convertToArrowTable(buffer);
      const textField = table.schema.fields.find((field) => field.name === 'textValue');
      expect(textField).toBeDefined();
      expect(textField?.type.typeId).toBe(-1);

      const textColumn = table.getChild('textValue');
      if (!textColumn) {
        throw new Error('textValue column not found');
      }
      expect(textColumn.get(0)).toBe('');
      expect(textColumn.get(1)).toBe('hello');
      expect(textColumn.get(2)).toBe('');
      expect(textColumn.get(3)).toBe('world!');
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty buffers correctly', () => {
      const schema = createTestSchema({
        numberValue: S.number(),
      });

      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot(traceId), DEFAULT_METADATA, undefined);
      buffer._opMetadata = DEFAULT_METADATA;

      // Don't write any data

      const table = convertToArrowTable(buffer);
      const ipcBytes = serializeToIpcFile(table, []);

      // Should still produce valid IPC
      expect(ipcBytes.length).toBeGreaterThan(0);

      // Should round-trip
      const reader = tableFromIPC(ipcBytes);
      expect(reader.numRows).toBe(0);
    });

    it('should handle boolean values correctly', () => {
      const schema = createTestSchema({
        boolValue: S.boolean(),
      });

      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot(traceId), DEFAULT_METADATA, undefined);
      buffer._opMetadata = DEFAULT_METADATA;

      // Write test data using generated methods
      const testValues = [true, false, true, false, true];

      for (let i = 0; i < testValues.length; i++) {
        buffer.timestamp[i] = BigInt(Date.now()) * 1000000n;
        buffer.entry_type[i] = ENTRY_TYPE_SPAN_START;
        buffer.boolValue(i, testValues[i]);
      }
      buffer._writeIndex = testValues.length;

      const table = convertToArrowTable(buffer);
      const ipcBytes = serializeToIpcFile(table, ['boolValue']);

      // Round-trip test
      const reader = tableFromIPC(ipcBytes);
      expect(reader.numRows).toBe(5);

      // Find our data column and verify boolean handling
      for (let i = 0; i < reader.schema.fields.length; i++) {
        const field = reader.schema.fields[i];
        if (field.name === 'boolValue') {
          const col = reader.getChildAt(i);
          if (col) {
            expect(col.get(0)).toBe(true);
            expect(col.get(1)).toBe(false);
            expect(col.get(2)).toBe(true);
            expect(col.get(3)).toBe(false);
            expect(col.get(4)).toBe(true);
          }
          break;
        }
      }
    });
  });
});
