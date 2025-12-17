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
import { convertSpanTreeToArrowTable, convertToArrowTable } from '../../convertToArrow.js';
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
} from '../../lmao.js';
import { S } from '../../schema/builder.js';
import { createChildSpanBuffer, createNextBuffer, createSpanBuffer } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import { createTestTaskContext } from '../test-helpers.js';

// MockStringInterner no longer needed - convertToArrowTable now uses direct string access
// via buf.task.module.packageName, buf.task.module.packagePath, and buf.task.spanName

describe('Arrow Table Conversion', () => {
  // let moduleIdInterner: MockStringInterner;
  // let spanNameInterner: MockStringInterner;

  beforeEach(() => {
    // moduleIdInterner = new MockStringInterner();
    // spanNameInterner = new MockStringInterner();
    // moduleIdInterner.intern('test-file.ts');
    // spanNameInterner.intern('test-span');
  });

  describe('Basic Conversion', () => {
    test('converts empty buffer to empty table', () => {
      const schema = { userId: S.category() } as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const table = convertToArrowTable(buffer);
      expect(table.numRows).toBe(0);
    });

    test('converts single row with number and boolean', () => {
      const schema = {
        count: S.number(),
        active: S.boolean(),
      } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const idx = buffer.writeIndex;
      buffer.timestamps[idx] = 1000n;
      buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
      buffer.count(idx, 42);
      buffer.active(idx, true);
      buffer.writeIndex++;

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(1);
      const row = table.get(0)?.toJSON();
      expect(row?.count).toBe(42);
      expect(row?.active).toBe(true);
    });

    test('converts multiple rows', () => {
      const schema = { value: S.number() } as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      for (let i = 0; i < 5; i++) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = BigInt(1000 + i);
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.value(idx, i * 10);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(5);
      for (let i = 0; i < 5; i++) {
        expect(table.get(i)?.toJSON().value).toBe(i * 10);
      }
    });
  });

  describe('Null Handling', () => {
    test('unwritten positions are null', () => {
      const schema = { value: S.number() } as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write row 0 with value
      buffer.timestamps[0] = 1000n;
      buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer.value(0, 42);

      // Write row 1 without value (skip setter - null)
      buffer.timestamps[1] = 2000n;
      buffer.operations[1] = ENTRY_TYPE_SPAN_START;

      // Write row 2 with value
      buffer.timestamps[2] = 3000n;
      buffer.operations[2] = ENTRY_TYPE_SPAN_START;
      buffer.value(2, 100);

      buffer.writeIndex = 3;

      const table = convertToArrowTable(buffer);

      expect(table.get(0)?.toJSON().value).toBe(42);
      expect(table.get(1)?.toJSON().value).toBe(null);
      expect(table.get(2)?.toJSON().value).toBe(100);
    });

    // TODO: Add test for explicit null via setter once setters support null values
    // test('setter accepts null to clear value', () => { ... });
  });

  describe('String Type Conversion', () => {
    test('enum stored as numeric index, converted to string in Arrow', () => {
      const schema = {
        status: S.enum(['pending', 'active', 'completed'] as const),
      } as const;

      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Write enum values as numeric indices
      const idx0 = buffer.writeIndex;
      buffer.timestamps[idx0] = 1000n;
      buffer.operations[idx0] = ENTRY_TYPE_SPAN_START;
      buffer.status(idx0, 0); // pending
      buffer.writeIndex++;

      const idx1 = buffer.writeIndex;
      buffer.timestamps[idx1] = 2000n;
      buffer.operations[idx1] = ENTRY_TYPE_SPAN_START;
      buffer.status(idx1, 2); // completed
      buffer.writeIndex++;

      const idx2 = buffer.writeIndex;
      buffer.timestamps[idx2] = 3000n;
      buffer.operations[idx2] = ENTRY_TYPE_SPAN_START;
      buffer.status(idx2, 1); // active
      buffer.writeIndex++;

      const table = convertToArrowTable(buffer);

      expect(table.get(0)?.toJSON().status).toBe('pending');
      expect(table.get(1)?.toJSON().status).toBe('completed');
      expect(table.get(2)?.toJSON().status).toBe('active');
    });

    test('category stores strings directly', () => {
      const schema = { userId: S.category() } as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const users = ['user-123', 'user-456', 'user-123'];
      for (let i = 0; i < users.length; i++) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = BigInt(1000 + i);
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.userId(idx, users[i]);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      expect(table.get(0)?.toJSON().userId).toBe('user-123');
      expect(table.get(1)?.toJSON().userId).toBe('user-456');
      expect(table.get(2)?.toJSON().userId).toBe('user-123');
    });

    test('text stores strings directly', () => {
      const schema = { message: S.text() } as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      const messages = ['First', 'Second', 'Third'];
      for (let i = 0; i < messages.length; i++) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = BigInt(1000 + i);
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.message(idx, messages[i]);
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      expect(table.get(0)?.toJSON().message).toBe('First');
      expect(table.get(1)?.toJSON().message).toBe('Second');
      expect(table.get(2)?.toJSON().message).toBe('Third');
    });
  });

  describe('Buffer Chaining', () => {
    test('converts chained buffers', () => {
      const schema = { value: S.number() } as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

      // Fill first buffer
      for (let i = 0; i < 3; i++) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = BigInt(1000 + i);
        buffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        buffer.value(idx, i);
        buffer.writeIndex++;
      }

      // Create and fill chained buffer
      const nextBuffer = createNextBuffer(buffer);
      for (let i = 0; i < 2; i++) {
        const idx = nextBuffer.writeIndex;
        nextBuffer.timestamps[idx] = BigInt(2000 + i);
        nextBuffer.operations[idx] = ENTRY_TYPE_SPAN_START;
        nextBuffer.value(idx, 10 + i);
        nextBuffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      expect(table.numRows).toBe(5);
      expect(table.get(0)?.toJSON().value).toBe(0);
      expect(table.get(1)?.toJSON().value).toBe(1);
      expect(table.get(2)?.toJSON().value).toBe(2);
      expect(table.get(3)?.toJSON().value).toBe(10);
      expect(table.get(4)?.toJSON().value).toBe(11);
    });
  });

  describe('Span Tree Conversion', () => {
    test('converts parent and child spans', () => {
      const schema = { depth: S.number() } as const;
      const parentContext = createTestTaskContext(schema, { lineNumber: 42 });
      const parentBuffer = createSpanBuffer(schema, parentContext, createTraceId('trace-123'));

      // Parent span start
      let idx = parentBuffer.writeIndex;
      parentBuffer.timestamps[idx] = 1000n;
      parentBuffer.operations[idx] = ENTRY_TYPE_SPAN_START;
      parentBuffer.depth(idx, 0);
      parentBuffer.writeIndex++;

      // Create child span
      const childContext = createTestTaskContext(schema, { lineNumber: 43 });
      const childBuffer = createChildSpanBuffer(parentBuffer, childContext);

      idx = childBuffer.writeIndex;
      childBuffer.timestamps[idx] = 2000n;
      childBuffer.operations[idx] = ENTRY_TYPE_SPAN_START;
      childBuffer.depth(idx, 1);
      childBuffer.writeIndex++;

      idx = childBuffer.writeIndex;
      childBuffer.timestamps[idx] = 3000n;
      childBuffer.operations[idx] = ENTRY_TYPE_SPAN_OK;
      childBuffer.depth(idx, 1);
      childBuffer.writeIndex++;

      // Parent span end
      idx = parentBuffer.writeIndex;
      parentBuffer.timestamps[idx] = 4000n;
      parentBuffer.operations[idx] = ENTRY_TYPE_SPAN_OK;
      parentBuffer.depth(idx, 0);
      parentBuffer.writeIndex++;

      const table = convertSpanTreeToArrowTable(parentBuffer);

      expect(table.numRows).toBe(4);

      // Check parent-child relationship
      // Tree walk order: all parent buffer rows first (0,1), then child buffer rows (2,3)
      const rows = [0, 1, 2, 3].map((i) => table.get(i)?.toJSON());

      // Parent rows (0, 1) should have null parent IDs
      expect(rows[0]?.parent_span_id).toBe(null);
      expect(rows[1]?.parent_span_id).toBe(null);

      // Child rows (2, 3) should reference parent
      expect(rows[2]?.parent_span_id).toBe(rows[0]?.span_id);
      expect(rows[3]?.parent_span_id).toBe(rows[0]?.span_id);
    });
  });

  describe('Entry Type Mapping', () => {
    test('maps all entry type codes to strings', () => {
      const schema = {} as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'), 16);

      const entryTypes = [
        { code: ENTRY_TYPE_SPAN_START, name: 'span-start' },
        { code: ENTRY_TYPE_SPAN_OK, name: 'span-ok' },
        { code: ENTRY_TYPE_SPAN_ERR, name: 'span-err' },
        { code: ENTRY_TYPE_SPAN_EXCEPTION, name: 'span-exception' },
        { code: ENTRY_TYPE_TRACE, name: 'trace' },
        { code: ENTRY_TYPE_DEBUG, name: 'debug' },
        { code: ENTRY_TYPE_INFO, name: 'info' },
        { code: ENTRY_TYPE_WARN, name: 'warn' },
        { code: ENTRY_TYPE_ERROR, name: 'error' },
        { code: ENTRY_TYPE_FF_ACCESS, name: 'ff-access' },
        { code: ENTRY_TYPE_FF_USAGE, name: 'ff-usage' },
      ];

      for (const { code } of entryTypes) {
        const idx = buffer.writeIndex;
        buffer.timestamps[idx] = 1000n;
        buffer.operations[idx] = code;
        buffer.writeIndex++;
      }

      const table = convertToArrowTable(buffer);

      for (let i = 0; i < entryTypes.length; i++) {
        expect(table.get(i)?.toJSON().entry_type).toBe(entryTypes[i].name);
      }
    });
  });

  describe('System Columns', () => {
    test('includes all system columns', () => {
      const schema = {} as const;
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-xyz'));

      buffer.timestamps[0] = 1000n;
      buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer.writeIndex = 1;

      const table = convertToArrowTable(buffer);

      const fieldNames = table.schema.fields.map((f) => f.name);
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
    });

    test('trace_id is preserved', () => {
      const schema = {} as const;
      const traceId = createTraceId('my-trace-id');
      const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
      const buffer = createSpanBuffer(schema, taskContext, traceId);

      buffer.timestamps[0] = 1000n;
      buffer.operations[0] = ENTRY_TYPE_SPAN_START;
      buffer.writeIndex = 1;

      const table = convertToArrowTable(buffer);

      expect(table.get(0)?.toJSON().trace_id).toBe(traceId);
    });
  });
});
