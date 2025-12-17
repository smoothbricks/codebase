/**
 * Debug dictionary encoding issue
 */
import { describe, expect, it } from 'bun:test';

import { convertToArrowTable } from '../convertToArrow.js';
import { ENTRY_TYPE_SPAN_START } from '../lmao.js';
import { S } from '../schema/builder.js';
import { createSpanBuffer } from '../spanBuffer.js';
import { createTraceId } from '../traceId.js';
import { createTestTaskContext } from './test-helpers.js';

// MockStringInterner no longer needed - convertToArrowTable now uses direct string access
// via buf.task.module.filePath and buf.task.spanName

describe('Debug Dictionary', () => {
  it('category and text columns should use separate dictionaries', () => {
    // No interners needed - direct string access is used

    const schema = {
      userId: S.category(),
      message: S.text(),
    } as const;

    const taskContext = createTestTaskContext(schema, { lineNumber: 42 });
    const buffer = createSpanBuffer(schema, taskContext, createTraceId('trace-123'));

    // Write first row using buffer setters directly
    const idx0 = buffer.writeIndex;
    buffer.timestamps[idx0] = 1000n;
    buffer.operations[idx0] = ENTRY_TYPE_SPAN_START;
    buffer.userId(idx0, 'user-123');
    buffer.message(idx0, 'First message');
    buffer.writeIndex++;

    // Write second row using buffer setters directly
    const idx1 = buffer.writeIndex;
    buffer.timestamps[idx1] = 1000n;
    buffer.operations[idx1] = ENTRY_TYPE_SPAN_START;
    buffer.userId(idx1, 'user-456');
    buffer.message(idx1, 'Second message');
    buffer.writeIndex++;

    const table = convertToArrowTable(buffer);

    console.log(
      'Schema fields:',
      table.schema.fields.map((f) => ({ name: f.name, type: f.type.toString() })),
    );

    // Check vectors
    const userIdIdx = table.schema.fields.findIndex((f) => f.name === 'userId');
    const messageIdx = table.schema.fields.findIndex((f) => f.name === 'message');

    console.log('userId column index:', userIdIdx);
    console.log('message column index:', messageIdx);

    const userIdVector = table.getChildAt(userIdIdx);
    const messageVector = table.getChildAt(messageIdx);
    if (!userIdVector || !messageVector) {
      throw new Error('Expected userId and message vectors to exist');
    }

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
