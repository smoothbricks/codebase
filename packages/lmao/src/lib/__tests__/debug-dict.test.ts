/**
 * Debug dictionary encoding issue
 */
import { describe, expect, it } from 'bun:test';
import * as arrow from 'apache-arrow';
import { convertToArrowTable } from '../convertToArrow.js';
import { S } from '../schema/builder.js';
import type { TagAttributeSchema } from '../schema/types.js';
import { createSpanBuffer } from '../spanBuffer.js';
import type { SpanBuffer, TaskContext } from '../types.js';
import { createTestTaskContext } from './test-helpers.js';

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

function createMockTaskContext(schema: TagAttributeSchema): TaskContext {
  return createTestTaskContext(schema, { lineNumber: 42 });
}

function writeRow(
  buffer: SpanBuffer,
  data: {
    timestamp: bigint;
    operation: number;
    attributes?: Record<string, unknown>;
  },
): void {
  const idx = buffer.writeIndex;

  buffer.timestamps[idx] = data.timestamp;
  buffer.operations[idx] = data.operation;

  if (data.attributes) {
    for (const [key, value] of Object.entries(data.attributes)) {
      const valuesKey = `attr_${key}_values` as keyof SpanBuffer;
      const nullsKey = `attr_${key}_nulls` as keyof SpanBuffer;
      const column = buffer[valuesKey];
      const nullBitmap = buffer[nullsKey] as Uint8Array | undefined;

      if (column) {
        if (value === null || value === undefined) {
          // Handle nulls
          if (Array.isArray(column)) {
            // String array - no need to set value
          } else if (column instanceof Float64Array) {
            column[idx] = 0;
          } else if (ArrayBuffer.isView(column)) {
            (column as Uint8Array | Uint16Array | Uint32Array)[idx] = 0;
          }

          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] &= ~(1 << bitOffset);
          }
        } else {
          // Handle values
          if (Array.isArray(column)) {
            // String array (category/text)
            if (typeof value === 'string') {
              column[idx] = value;
            }
          } else if (typeof value === 'number') {
            if (column instanceof Float64Array) {
              column[idx] = value;
            } else if (ArrayBuffer.isView(column)) {
              (column as Uint8Array | Uint16Array | Uint32Array)[idx] = value;
            }
          } else if (typeof value === 'boolean') {
            if (ArrayBuffer.isView(column)) {
              (column as Uint8Array)[idx] = value ? 1 : 0;
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

  buffer.writeIndex++;
}

describe('Debug Dictionary', () => {
  it('category and text columns should use separate dictionaries', () => {
    const moduleIdInterner = new MockStringInterner();
    const spanNameInterner = new MockStringInterner();

    moduleIdInterner.intern('test-file.ts');
    spanNameInterner.intern('test-span');

    const schema: TagAttributeSchema = {
      userId: S.category(),
      message: S.text(),
    };

    const taskContext = createMockTaskContext(schema);
    const buffer = createSpanBuffer(schema, taskContext, 'trace-123');

    // Write test data (raw strings - no interning on hot path)
    writeRow(buffer, {
      timestamp: 1000n,
      operation: 3,
      attributes: { userId: 'user-123', message: 'First message' },
    });

    writeRow(buffer, {
      timestamp: 1000n,
      operation: 3,
      attributes: { userId: 'user-456', message: 'Second message' },
    });

    const table = convertToArrowTable(buffer, moduleIdInterner, spanNameInterner);

    console.log(
      'Schema fields:',
      table.schema.fields.map((f) => ({ name: f.name, type: f.type.toString() })),
    );

    // Check vectors
    const userIdIdx = table.schema.fields.findIndex((f) => f.name === 'userId');
    const messageIdx = table.schema.fields.findIndex((f) => f.name === 'message');

    console.log('userId column index:', userIdIdx);
    console.log('message column index:', messageIdx);

    const userIdVector = table.getChildAt(userIdIdx)!;
    const messageVector = table.getChildAt(messageIdx)!;

    console.log('userId vector type:', userIdVector.type.toString());
    console.log('message vector type:', messageVector.type.toString());

    // Check dictionary
    const userIdData = userIdVector.data[0];
    const messageData = messageVector.data[0];

    console.log('userId dictionary:', userIdData.dictionary?.toArray());
    console.log('message dictionary:', messageData.dictionary?.toArray());

    console.log('userId indices:', userIdData.values);
    console.log('message indices:', messageData.values);

    // Check values
    const row0 = table.get(0)?.toJSON();
    console.log('Row 0:', row0);

    expect(row0?.userId).toBe('user-123');
    expect(row0?.message).toBe('First message');

    const row1 = table.get(1)?.toJSON();
    console.log('Row 1:', row1);

    expect(row1?.userId).toBe('user-456');
    expect(row1?.message).toBe('Second message');
  });
});
