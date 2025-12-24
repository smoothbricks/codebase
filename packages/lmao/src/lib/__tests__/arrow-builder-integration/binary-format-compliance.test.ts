import { describe, expect, it } from 'bun:test';
import * as arrow from 'apache-arrow';
import { convertToArrowTable } from '../../convertToArrow';
import { S } from '../../schema/builder';
import { ENTRY_TYPE_SPAN_START } from '../../schema/systemSchema';
import { createSpanBuffer } from '../../spanBuffer';
import { createTraceId } from '../../traceId';
import { createTestLogBinding } from '../test-helpers';

describe('Arrow Binary Format Compliance', () => {
  describe('IPC Message Structure', () => {
    it('should write correct IPC continuation bytes', () => {
      const schema = {
        numberValue: S.number(),
      };

      const module = createTestLogBinding(schema);
      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(module.logSchema, module, 'test-span', traceId);

      // Write some data using generated methods
      buffer.timestamp[0] = BigInt(Date.now()) * 1000000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.numberValue(0, 42);
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const ipcBytes = arrow.tableToIPC(table, 'stream');

      // IPC stream should start with 0xFFFFFFFF continuation marker
      expect(ipcBytes[0]).toBe(0xff);
      expect(ipcBytes[1]).toBe(0xff);
      expect(ipcBytes[2]).toBe(0xff);
      expect(ipcBytes[3]).toBe(0xff);
    });
  });

  describe('Endianness', () => {
    it('should preserve number values through round-trip', () => {
      const schema = {
        uint32Value: S.number(),
      };

      const module = createTestLogBinding(schema);
      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(module.logSchema, module, 'test-span', traceId);

      // Write test data using generated methods
      const testValue = 0x12345678;
      buffer.timestamp[0] = BigInt(Date.now()) * 1000000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer.uint32Value(0, testValue);
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const ipcBytes = arrow.tableToIPC(table, 'stream');

      // Round-trip test
      const reader = arrow.tableFromIPC(ipcBytes);
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
      const schema = {
        numberValue: S.number(),
      };

      const module = createTestLogBinding(schema);
      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(module.logSchema, module, 'test-span', traceId);

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
      const ipcBytes = arrow.tableToIPC(table, 'stream');

      // Round-trip test
      const reader = arrow.tableFromIPC(ipcBytes);
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
      const schema = {
        enumValue: S.enum(['a', 'b', 'c']),
      };

      const module = createTestLogBinding(schema);
      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(module.logSchema, module, 'test-span', traceId);

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
      const ipcBytes = arrow.tableToIPC(table, 'stream');

      // Round-trip test
      const reader = arrow.tableFromIPC(ipcBytes);
      expect(reader.numRows).toBe(4);

      // Find our data column and verify enum handling
      for (let i = 0; i < reader.schema.fields.length; i++) {
        const field = reader.schema.fields[i];
        if (field.name === 'enumValue') {
          const col = reader.getChildAt(i);
          if (col) {
            expect(col.get(0)).toBe('a');
            expect(col.get(1)).toBe('b');
            expect(col.get(2)).toBe('c');
            expect(col.get(3)).toBe('a');
          }
          break;
        }
      }
    });
  });

  describe('Variable Size Binary', () => {
    it('should handle string values correctly in round-trip', () => {
      const schema = {
        textValue: S.text(),
      };

      const module = createTestLogBinding(schema);
      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(module.logSchema, module, 'test-span', traceId);

      // Write test data with various string lengths
      const testValues = ['', 'hello', '', 'world!'];

      for (let i = 0; i < testValues.length; i++) {
        buffer.timestamp[i] = BigInt(Date.now()) * 1000000n;
        buffer.entry_type[i] = ENTRY_TYPE_SPAN_START;
        buffer.textValue(i, testValues[i]);
      }
      buffer._writeIndex = testValues.length;

      const table = convertToArrowTable(buffer);
      const ipcBytes = arrow.tableToIPC(table, 'stream');

      // Round-trip test
      const reader = arrow.tableFromIPC(ipcBytes);
      expect(reader.numRows).toBe(4);

      // Find our data column and verify string handling
      for (let i = 0; i < reader.schema.fields.length; i++) {
        const field = reader.schema.fields[i];
        if (field.name === 'textValue') {
          const col = reader.getChildAt(i);
          if (col) {
            expect(col.get(0)).toBe('');
            expect(col.get(1)).toBe('hello');
            expect(col.get(2)).toBe('');
            expect(col.get(3)).toBe('world!');
          }
          break;
        }
      }
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty buffers correctly', () => {
      const schema = {
        numberValue: S.number(),
      };

      const module = createTestLogBinding(schema);
      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(module.logSchema, module, 'test-span', traceId);

      // Don't write any data

      const table = convertToArrowTable(buffer);
      const ipcBytes = arrow.tableToIPC(table, 'stream');

      // Should still produce valid IPC
      expect(ipcBytes.length).toBeGreaterThan(0);

      // Should round-trip
      const reader = arrow.tableFromIPC(ipcBytes);
      expect(reader.numRows).toBe(0);
    });

    it('should handle boolean values correctly', () => {
      const schema = {
        boolValue: S.boolean(),
      };

      const module = createTestLogBinding(schema);
      const traceId = createTraceId('trace-123');
      const buffer = createSpanBuffer(module.logSchema, module, 'test-span', traceId);

      // Write test data using generated methods
      const testValues = [true, false, true, false, true];

      for (let i = 0; i < testValues.length; i++) {
        buffer.timestamp[i] = BigInt(Date.now()) * 1000000n;
        buffer.entry_type[i] = ENTRY_TYPE_SPAN_START;
        buffer.boolValue(i, testValues[i]);
      }
      buffer._writeIndex = testValues.length;

      const table = convertToArrowTable(buffer);
      const ipcBytes = arrow.tableToIPC(table, 'stream');

      // Round-trip test
      const reader = arrow.tableFromIPC(ipcBytes);
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
