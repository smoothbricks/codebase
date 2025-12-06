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

import { describe, test, expect, beforeEach } from 'bun:test';
import { createSpanBuffer, createChildSpanBuffer, createNextBuffer } from '@smoothbricks/lmao';
import { convertToArrowTable, convertSpanTreeToArrowTable } from '../convertToArrow.js';
import type { SpanBuffer, TaskContext, ModuleContext } from '../types.js';
import type { TagAttributeSchema } from '../../schema-types.js';

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
  const moduleContext: ModuleContext = {
    moduleId: 1,
    gitSha: 'test-sha',
    filePath: 'test-file.ts',
    tagAttributes: schema,
    spanBufferCapacityStats: {
      currentCapacity: 64,
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };
  
  return {
    module: moduleContext,
    spanNameId: 1,
    lineNumber: 42,
  };
}

/**
 * Helper to write a row to buffer
 */
function writeRow(
  buffer: SpanBuffer,
  data: {
    timestamp: number;
    operation: number;
    attributes?: Record<string, unknown>;
  }
): void {
  const idx = buffer.writeIndex;
  
  // Write core columns
  buffer.timestamps[idx] = data.timestamp;
  buffer.operations[idx] = data.operation;
  
  // Write attributes
  if (data.attributes) {
    for (const [key, value] of Object.entries(data.attributes)) {
      const columnName = `attr_${key}` as keyof SpanBuffer;
      const column = buffer[columnName];
      const nullBitmap = buffer.nullBitmaps[`attr_${key}` as `attr_${string}`];
      
      if (column && ArrayBuffer.isView(column)) {
        if (value === null || value === undefined) {
          // Write 0 and clear null bit
          if (column instanceof Float64Array) {
            column[idx] = 0;
          } else {
            (column as Uint8Array | Uint16Array | Uint32Array)[idx] = 0;
          }
          
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] &= ~(1 << bitOffset);
          }
        } else {
          // Write value and set null bit
          if (typeof value === 'number') {
            if (column instanceof Float64Array) {
              column[idx] = value;
            } else {
              (column as Uint8Array | Uint16Array | Uint32Array)[idx] = value;
            }
          } else if (typeof value === 'boolean') {
            (column as Uint8Array)[idx] = value ? 1 : 0;
          }
          
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }
    }
  }
  
  buffer.writeIndex++;
}

describe('Arrow Table Conversion', () => {
  let categoryInterner: MockStringInterner;
  let textStorage: MockStringInterner;
  let moduleIdInterner: MockStringInterner;
  let spanNameInterner: MockStringInterner;
  
  beforeEach(() => {
    categoryInterner = new MockStringInterner();
    textStorage = new MockStringInterner();
    moduleIdInterner = new MockStringInterner();
    spanNameInterner = new MockStringInterner();
  });
  
  describe('Basic Conversion', () => {
    test('converts empty buffer to empty table', () => {
      const schema: TagAttributeSchema = {
        userId: {
          __lmao_type: 'category',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      // Intern module and span names
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      expect(table.numRows).toBe(0);
    });
    
    test('converts single row with basic types', () => {
      const schema: TagAttributeSchema = {
        count: {
          __lmao_type: 'number',
        } as any,
        active: {
          __lmao_type: 'boolean',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      // Intern required strings
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Write a row
      writeRow(buffer, {
        timestamp: 1000,
        operation: 3, // tag
        attributes: {
          count: 42,
          active: true,
        },
      });
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      expect(table.numRows).toBe(1);
      
      // Verify data in first batch
      const batch = table.batches[0];
      expect(batch.numRows).toBe(1);
      
      // Check timestamp
      const timestampVector = batch.getChild('timestamp');
      expect(timestampVector?.get(0)).toBeDefined();
      
      // Check entry type
      const entryTypeVector = batch.getChild('entry_type');
      expect(entryTypeVector?.get(0)).toBe('tag');
      
      // Check attributes
      const countVector = batch.getChild('count');
      expect(countVector?.get(0)).toBe(42);
      
      const activeVector = batch.getChild('active');
      expect(activeVector?.get(0)).toBe(true);
    });
    
    test('converts multiple rows', () => {
      const schema: TagAttributeSchema = {
        value: {
          __lmao_type: 'number',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Write multiple rows
      for (let i = 0; i < 5; i++) {
        writeRow(buffer, {
          timestamp: 1000 + i,
          operation: 3,
          attributes: { value: i * 10 },
        });
      }
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      expect(table.numRows).toBe(5);
      
      const batch = table.batches[0];
      const valueVector = batch.getChild('value');
      
      for (let i = 0; i < 5; i++) {
        expect(valueVector?.get(i)).toBe(i * 10);
      }
    });
  });
  
  describe('Null Bitmap Handling', () => {
    test('handles null values correctly', () => {
      const schema: TagAttributeSchema = {
        value: {
          __lmao_type: 'number',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Write rows with mix of null and non-null
      writeRow(buffer, {
        timestamp: 1000,
        operation: 3,
        attributes: { value: 42 },
      });
      
      writeRow(buffer, {
        timestamp: 2000,
        operation: 3,
        attributes: { value: null },
      });
      
      writeRow(buffer, {
        timestamp: 3000,
        operation: 3,
        attributes: { value: 100 },
      });
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const valueVector = batch.getChild('value');
      
      expect(valueVector?.get(0)).toBe(42);
      expect(valueVector?.get(1)).toBe(null);
      expect(valueVector?.get(2)).toBe(100);
    });
    
    test('handles all-null column', () => {
      const schema: TagAttributeSchema = {
        optional: {
          __lmao_type: 'number',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Write rows with all nulls
      for (let i = 0; i < 3; i++) {
        writeRow(buffer, {
          timestamp: 1000 + i,
          operation: 3,
          attributes: { optional: null },
        });
      }
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const optionalVector = batch.getChild('optional');
      
      for (let i = 0; i < 3; i++) {
        expect(optionalVector?.get(i)).toBe(null);
      }
    });
  });
  
  describe('String Type Conversion', () => {
    test('converts enum type to dictionary', () => {
      const schema: TagAttributeSchema = {
        status: {
          __lmao_type: 'enum',
          __lmao_enum_values: ['pending', 'active', 'completed'],
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Write rows with enum values (stored as indices 0, 1, 2)
      writeRow(buffer, {
        timestamp: 1000,
        operation: 3,
        attributes: { status: 0 }, // 'pending'
      });
      
      writeRow(buffer, {
        timestamp: 2000,
        operation: 3,
        attributes: { status: 2 }, // 'completed'
      });
      
      writeRow(buffer, {
        timestamp: 3000,
        operation: 3,
        attributes: { status: 1 }, // 'active'
      });
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const statusVector = batch.getChild('status');
      
      expect(statusVector?.get(0)).toBe('pending');
      expect(statusVector?.get(1)).toBe('completed');
      expect(statusVector?.get(2)).toBe('active');
    });
    
    test('converts category type with string interning', () => {
      const schema: TagAttributeSchema = {
        userId: {
          __lmao_type: 'category',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Intern some user IDs
      const user1Idx = categoryInterner.intern('user-123');
      const user2Idx = categoryInterner.intern('user-456');
      
      // Write rows with category values (stored as interned indices)
      writeRow(buffer, {
        timestamp: 1000,
        operation: 3,
        attributes: { userId: user1Idx },
      });
      
      writeRow(buffer, {
        timestamp: 2000,
        operation: 3,
        attributes: { userId: user2Idx },
      });
      
      writeRow(buffer, {
        timestamp: 3000,
        operation: 3,
        attributes: { userId: user1Idx }, // Repeated
      });
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const userIdVector = batch.getChild('userId');
      
      expect(userIdVector?.get(0)).toBe('user-123');
      expect(userIdVector?.get(1)).toBe('user-456');
      expect(userIdVector?.get(2)).toBe('user-123');
    });
    
    test('converts text type without interning', () => {
      const schema: TagAttributeSchema = {
        message: {
          __lmao_type: 'text',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Store text messages (each gets unique index)
      const msg1Idx = textStorage.intern('First message');
      const msg2Idx = textStorage.intern('Second message');
      const msg3Idx = textStorage.intern('Third message');
      
      writeRow(buffer, {
        timestamp: 1000,
        operation: 3,
        attributes: { message: msg1Idx },
      });
      
      writeRow(buffer, {
        timestamp: 2000,
        operation: 3,
        attributes: { message: msg2Idx },
      });
      
      writeRow(buffer, {
        timestamp: 3000,
        operation: 3,
        attributes: { message: msg3Idx },
      });
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const messageVector = batch.getChild('message');
      
      expect(messageVector?.get(0)).toBe('First message');
      expect(messageVector?.get(1)).toBe('Second message');
      expect(messageVector?.get(2)).toBe('Third message');
    });
  });
  
  describe('Buffer Chaining', () => {
    test('converts chained buffers correctly', () => {
      const schema: TagAttributeSchema = {
        value: {
          __lmao_type: 'number',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      // Fill first buffer
      for (let i = 0; i < 3; i++) {
        writeRow(buffer, {
          timestamp: 1000 + i,
          operation: 3,
          attributes: { value: i },
        });
      }
      
      // Create chained buffer
      const nextBuffer = createNextBuffer(buffer);
      
      // Fill chained buffer
      for (let i = 0; i < 2; i++) {
        writeRow(nextBuffer, {
          timestamp: 2000 + i,
          operation: 3,
          attributes: { value: 10 + i },
        });
      }
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      // Should have all rows from both buffers
      expect(table.numRows).toBe(5);
      
      const batch = table.batches[0];
      const valueVector = batch.getChild('value');
      
      // Check values from first buffer
      expect(valueVector?.get(0)).toBe(0);
      expect(valueVector?.get(1)).toBe(1);
      expect(valueVector?.get(2)).toBe(2);
      
      // Check values from chained buffer
      expect(valueVector?.get(3)).toBe(10);
      expect(valueVector?.get(4)).toBe(11);
    });
    
    test('maintains spanId across chained buffers', () => {
      const schema: TagAttributeSchema = {
        value: {
          __lmao_type: 'number',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      const originalSpanId = buffer.spanId;
      
      writeRow(buffer, {
        timestamp: 1000,
        operation: 3,
        attributes: { value: 1 },
      });
      
      const nextBuffer = createNextBuffer(buffer);
      
      // Chained buffer should have same spanId (continuation)
      expect(nextBuffer.spanId).toBe(originalSpanId);
      
      writeRow(nextBuffer, {
        timestamp: 2000,
        operation: 3,
        attributes: { value: 2 },
      });
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const spanIdVector = batch.getChild('span_id');
      
      // Both rows should have same spanId
      expect(spanIdVector?.get(0)).toBe(BigInt(originalSpanId));
      expect(spanIdVector?.get(1)).toBe(BigInt(originalSpanId));
    });
  });
  
  describe('Span Tree Conversion', () => {
    test('converts parent and child spans', () => {
      const schema: TagAttributeSchema = {
        value: {
          __lmao_type: 'number',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const parentBuffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('parent-span');
      
      // Write to parent
      writeRow(parentBuffer, {
        timestamp: 1000,
        operation: 5, // span-start
        attributes: { value: 1 },
      });
      
      // Create child span
      const childTaskContext = createMockTaskContext(schema);
      const childBuffer = createChildSpanBuffer(parentBuffer, childTaskContext);
      
      // Write to child
      writeRow(childBuffer, {
        timestamp: 2000,
        operation: 5, // span-start
        attributes: { value: 2 },
      });
      
      writeRow(childBuffer, {
        timestamp: 3000,
        operation: 6, // span-ok
        attributes: { value: 3 },
      });
      
      // Close parent
      writeRow(parentBuffer, {
        timestamp: 4000,
        operation: 6, // span-ok
        attributes: { value: 4 },
      });
      
      const table = convertSpanTreeToArrowTable(
        parentBuffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      // Should have rows from both parent and child
      expect(table.numRows).toBe(4);
      
      // Verify parent-child relationship via span IDs
      const batch = table.batches[0];
      const spanIdVector = batch.getChild('span_id');
      const parentSpanIdVector = batch.getChild('parent_span_id');
      
      // Rows 0-1 are from parent span (2 writes)
      // Rows 2-3 are from child span (2 writes)
      const parentSpanId = spanIdVector?.get(0);
      const childSpanId = spanIdVector?.get(2); // Child starts at row 2
      
      // Parent rows should have null parent_span_id
      expect(parentSpanIdVector?.get(0)).toBe(null);
      expect(parentSpanIdVector?.get(1)).toBe(null);
      
      // Child rows' parent_span_id should match parent's span_id
      expect(parentSpanIdVector?.get(2)).toBe(parentSpanId);
      expect(parentSpanIdVector?.get(3)).toBe(parentSpanId);
    });
    
    test('converts deep span hierarchy', () => {
      const schema: TagAttributeSchema = {
        depth: {
          __lmao_type: 'number',
        } as any,
      };
      
      const taskContext = createMockTaskContext(schema);
      const root = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('span');
      
      writeRow(root, {
        timestamp: 1000,
        operation: 5,
        attributes: { depth: 0 },
      });
      
      // Create level 1 child
      const child1Context = createMockTaskContext(schema);
      const child1 = createChildSpanBuffer(root, child1Context);
      
      writeRow(child1, {
        timestamp: 2000,
        operation: 5,
        attributes: { depth: 1 },
      });
      
      // Create level 2 child
      const child2Context = createMockTaskContext(schema);
      const child2 = createChildSpanBuffer(child1, child2Context);
      
      writeRow(child2, {
        timestamp: 3000,
        operation: 5,
        attributes: { depth: 2 },
      });
      
      const table = convertSpanTreeToArrowTable(
        root,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      expect(table.numRows).toBe(3);
      
      const batch = table.batches[0];
      const depthVector = batch.getChild('depth');
      
      expect(depthVector?.get(0)).toBe(0);
      expect(depthVector?.get(1)).toBe(1);
      expect(depthVector?.get(2)).toBe(2);
    });
  });
  
  describe('Entry Type Mapping', () => {
    test('maps all entry type codes correctly', () => {
      const schema: TagAttributeSchema = {};
      const taskContext = createMockTaskContext(schema);
      const buffer = createSpanBuffer(schema, taskContext, 'trace-123');
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      const entryTypes = [
        { code: 1, name: 'ff-access' },
        { code: 2, name: 'ff-usage' },
        { code: 3, name: 'tag' },
        { code: 5, name: 'span-start' },
        { code: 6, name: 'span-ok' },
        { code: 7, name: 'span-err' },
        { code: 8, name: 'span-exception' },
        { code: 9, name: 'info' },
        { code: 10, name: 'debug' },
        { code: 11, name: 'warn' },
        { code: 12, name: 'error' },
      ];
      
      for (const entryType of entryTypes) {
        writeRow(buffer, {
          timestamp: 1000,
          operation: entryType.code,
        });
      }
      
      const table = convertToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const entryTypeVector = batch.getChild('entry_type');
      
      for (let i = 0; i < entryTypes.length; i++) {
        expect(entryTypeVector?.get(i)).toBe(entryTypes[i].name);
      }
    });
  });
  
  describe('TraceId and SpanId', () => {
    test('maintains traceId across all spans', () => {
      const schema: TagAttributeSchema = {};
      const taskContext = createMockTaskContext(schema);
      const traceId = 'trace-xyz-789';
      const buffer = createSpanBuffer(schema, taskContext, traceId);
      
      moduleIdInterner.intern('test-file.ts');
      spanNameInterner.intern('test-span');
      
      writeRow(buffer, {
        timestamp: 1000,
        operation: 5,
      });
      
      const childContext = createMockTaskContext(schema);
      const childBuffer = createChildSpanBuffer(buffer, childContext);
      
      writeRow(childBuffer, {
        timestamp: 2000,
        operation: 5,
      });
      
      const table = convertSpanTreeToArrowTable(
        buffer,
        categoryInterner,
        textStorage,
        moduleIdInterner,
        spanNameInterner
      );
      
      const batch = table.batches[0];
      const traceIdVector = batch.getChild('trace_id');
      
      // Both rows should have same traceId
      expect(traceIdVector?.get(0)).toBe(traceId);
      expect(traceIdVector?.get(1)).toBe(traceId);
    });
  });
});
