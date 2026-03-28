/**
 * Tests for binary column Arrow conversion (S.unknown(), S.object(), S.binary()).
 *
 * Verifies that binary schema columns (raw and msgpack-encoded) are correctly
 * serialized to Arrow Binary columns via convertToArrowTable and convertSpanTreeToArrowTable.
 */

import { describe, expect, it } from 'bun:test';
import { decode } from '@msgpack/msgpack';
import { createColumnWriter } from '@smoothbricks/arrow-builder';
import { convertSpanTreeToArrowTable, convertToArrowTable, createSpanBuffer, S } from '@smoothbricks/lmao';
import { ENTRY_TYPE_INFO, ENTRY_TYPE_SPAN_START } from '../../schema/systemSchema.js';
import { invokeWriterMethod, nextWriterRow, requireBinaryCell, requireColumn } from '../arrow-test-helpers.js';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from '../test-helpers.js';

function isHttpRequest(value: unknown): value is {
  method: string;
  url: string;
  headers: Record<string, string>;
} {
  return (
    typeof value === 'object' &&
    value !== null &&
    'method' in value &&
    'url' in value &&
    'headers' in value &&
    typeof value.method === 'string' &&
    typeof value.url === 'string' &&
    typeof value.headers === 'object' &&
    value.headers !== null
  );
}

describe('Binary Arrow Conversion', () => {
  describe('convertToArrowTable (Path 1: single buffer)', () => {
    it('converts S.unknown() column with mixed values to Arrow Binary', () => {
      const schema = createTestSchema({
        payload: S.unknown(),
        requestId: S.category(),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      // Write row with an object payload
      const row0 = invokeWriterMethod(nextWriterRow(writer), 'payload', { action: 'click', x: 100, y: 200 });
      invokeWriterMethod(row0, 'requestId', 'req-1');
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

      // Write row with a string payload
      const row1 = invokeWriterMethod(nextWriterRow(writer), 'payload', 'simple-string');
      invokeWriterMethod(row1, 'requestId', 'req-2');
      buffer.timestamp[1] = 2000n;
      buffer.entry_type[1] = ENTRY_TYPE_INFO;

      buffer._writeIndex = 2;

      const table = convertToArrowTable(buffer);

      // Verify the payload column exists and is binary
      const payloadCol = requireColumn(table, 'payload');
      expect(payloadCol).toBeDefined();
      expect(payloadCol.length).toBe(2);

      // Row 0: msgpack-decoded object should match original
      const row0Bytes = requireBinaryCell(payloadCol, 0);
      expect(row0Bytes).toBeInstanceOf(Uint8Array);
      const decoded0 = decode(row0Bytes);
      expect(decoded0).toEqual({ action: 'click', x: 100, y: 200 });

      // Row 1: msgpack-decoded string should match original
      const row1Bytes = requireBinaryCell(payloadCol, 1);
      expect(row1Bytes).toBeInstanceOf(Uint8Array);
      const decoded1 = decode(row1Bytes);
      expect(decoded1).toBe('simple-string');
    });

    it('converts S.object<T>() column to Arrow Binary with msgpack encoding', () => {
      interface HttpRequest {
        method: string;
        url: string;
        headers: Record<string, string>;
      }

      const schema = createTestSchema({
        request: S.object<HttpRequest>(),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      const requestData: HttpRequest = {
        method: 'POST',
        url: '/api/users',
        headers: { 'content-type': 'application/json' },
      };
      invokeWriterMethod(nextWriterRow(writer), 'request', requestData);
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const requestCol = requireColumn(table, 'request');
      expect(requestCol).toBeDefined();

      const bytes = requireBinaryCell(requestCol, 0);
      expect(bytes).toBeInstanceOf(Uint8Array);
      const decoded = decode(bytes);
      if (!isHttpRequest(decoded)) {
        throw new Error('Decoded request payload did not match HttpRequest shape');
      }
      expect(decoded.method).toBe('POST');
      expect(decoded.url).toBe('/api/users');
      expect(decoded.headers['content-type']).toBe('application/json');
    });

    it('converts S.binary() column (raw Uint8Array) to Arrow Binary', () => {
      const schema = createTestSchema({
        rawData: S.binary(),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      const rawBytes = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
      invokeWriterMethod(nextWriterRow(writer), 'rawData', rawBytes);
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const rawDataCol = requireColumn(table, 'rawData');
      expect(rawDataCol).toBeDefined();

      const bytes = requireBinaryCell(rawDataCol, 0);
      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes).toEqual(new Uint8Array([0xde, 0xad, 0xbe, 0xef]));
    });

    it('handles null/unset binary columns correctly', () => {
      const schema = createTestSchema({
        payload: S.unknown(),
        requestId: S.category(),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      // Row 0: payload set
      const firstRow = invokeWriterMethod(nextWriterRow(writer), 'payload', { key: 'value' });
      invokeWriterMethod(firstRow, 'requestId', 'req-1');
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

      // Row 1: payload NOT set (null)
      invokeWriterMethod(nextWriterRow(writer), 'requestId', 'req-2');
      buffer.timestamp[1] = 2000n;
      buffer.entry_type[1] = ENTRY_TYPE_INFO;

      buffer._writeIndex = 2;

      const table = convertToArrowTable(buffer);
      const payloadCol = requireColumn(table, 'payload');
      expect(payloadCol).toBeDefined();
      expect(payloadCol.length).toBe(2);

      // Row 0: should have data
      const row0 = requireBinaryCell(payloadCol, 0);
      expect(row0).toBeInstanceOf(Uint8Array);
      expect(decode(row0)).toEqual({ key: 'value' });

      // Row 1: should be null
      const row1 = payloadCol?.at(1);
      expect(row1).toBeNull();
    });

    it('handles binary columns alongside other column types', () => {
      const schema = createTestSchema({
        userId: S.category(),
        httpStatus: S.number(),
        payload: S.unknown(),
        operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      const mixedRow = invokeWriterMethod(nextWriterRow(writer), 'userId', 'user-1');
      invokeWriterMethod(
        invokeWriterMethod(invokeWriterMethod(mixedRow, 'httpStatus', 200), 'payload', { items: [1, 2, 3] }),
        'operation',
        'GET',
      );
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);

      // Verify all column types work together
      expect(table.getChild('userId')?.at(0)).toBe('user-1');
      expect(table.getChild('httpStatus')?.at(0)).toBe(200);
      expect(table.getChild('operation')?.at(0)).toBe('GET');

      const payloadBytes = requireBinaryCell(requireColumn(table, 'payload'), 0);
      expect(decode(payloadBytes)).toEqual({ items: [1, 2, 3] });
    });
  });

  describe('convertSpanTreeToArrowTable (Path 2: shared dicts)', () => {
    it('converts binary columns through the shared-dict tree path', () => {
      const schema = createTestSchema({
        payload: S.unknown(),
        userId: S.category(),
      });

      // Create root buffer
      const rootBuffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const rootWriter = createColumnWriter(schema, rootBuffer);

      const rootRow = invokeWriterMethod(nextWriterRow(rootWriter), 'payload', { level: 'root', data: [1, 2] });
      invokeWriterMethod(rootRow, 'userId', 'user-root');
      rootBuffer.timestamp[0] = 1000n;
      rootBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      rootBuffer._writeIndex = 1;

      // Create child buffer
      const childBuffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const childWriter = createColumnWriter(schema, childBuffer);

      const childRow = invokeWriterMethod(nextWriterRow(childWriter), 'payload', { level: 'child', nested: { a: 1 } });
      invokeWriterMethod(childRow, 'userId', 'user-child');
      childBuffer.timestamp[0] = 2000n;
      childBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      childBuffer._writeIndex = 1;

      // Link parent-child
      rootBuffer._children.push(childBuffer);
      childBuffer._parent = rootBuffer;

      const table = convertSpanTreeToArrowTable(rootBuffer);
      const payloadCol = requireColumn(table, 'payload');
      expect(payloadCol).toBeDefined();
      expect(payloadCol.length).toBe(2);

      // Both rows should have msgpack-encoded binary data
      const row0 = decode(requireBinaryCell(payloadCol, 0));
      const row1 = decode(requireBinaryCell(payloadCol, 1));

      // Values should match what was written (order may vary due to tree walk)
      const payloads = [row0, row1];
      expect(payloads).toContainEqual({ level: 'root', data: [1, 2] });
      expect(payloads).toContainEqual({ level: 'child', nested: { a: 1 } });
    });
  });

  describe('Object.freeze behavioral guarantee', () => {
    it('freezes objects at tag time so mutations after tagging do not affect flushed data', () => {
      // Object.freeze is shallow -- intentional. This test only verifies top-level property mutation.
      const schema = createTestSchema({
        payload: S.unknown(),
      });

      const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      const obj: Record<string, unknown> = { x: 1, y: 2, label: 'original' };

      invokeWriterMethod(nextWriterRow(writer), 'payload', obj);
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      // Mutate AFTER tagging -- Object.freeze should prevent this from affecting the stored reference
      expect(() => {
        obj.x = 999;
      }).toThrow(); // frozen objects throw in strict mode

      const table = convertToArrowTable(buffer);
      const payloadCol = requireColumn(table, 'payload');
      expect(payloadCol).toBeDefined();

      const bytes = requireBinaryCell(payloadCol, 0);
      const decoded = decode(bytes);
      expect(decoded).toEqual({ x: 1, y: 2, label: 'original' });
    });
  });

  describe('mixed binary + dictionary columns through shared-dict path', () => {
    it('builds category dictionaries correctly when binary columns are present', () => {
      const schema = createTestSchema({
        payload: S.unknown(),
        userId: S.category(),
        action: S.text(),
      });

      // Root buffer with multiple rows to exercise dictionary deduplication
      const rootBuffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const rootWriter = createColumnWriter(schema, rootBuffer);

      const rootRow0 = invokeWriterMethod(nextWriterRow(rootWriter), 'payload', { req: 1 });
      invokeWriterMethod(invokeWriterMethod(rootRow0, 'userId', 'user-A'), 'action', 'click');
      rootBuffer.timestamp[0] = 1000n;
      rootBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

      const rootRow1 = invokeWriterMethod(nextWriterRow(rootWriter), 'payload', { req: 2 });
      invokeWriterMethod(invokeWriterMethod(rootRow1, 'userId', 'user-A'), 'action', 'scroll');
      rootBuffer.timestamp[1] = 2000n;
      rootBuffer.entry_type[1] = ENTRY_TYPE_INFO;

      const rootRow2 = invokeWriterMethod(nextWriterRow(rootWriter), 'payload', { req: 3 });
      invokeWriterMethod(invokeWriterMethod(rootRow2, 'userId', 'user-B'), 'action', 'click');
      rootBuffer.timestamp[2] = 3000n;
      rootBuffer.entry_type[2] = ENTRY_TYPE_INFO;

      rootBuffer._writeIndex = 3;

      // Child buffer with overlapping category values
      const childBuffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
      const childWriter = createColumnWriter(schema, childBuffer);

      const childDataRow = invokeWriterMethod(nextWriterRow(childWriter), 'payload', { req: 4 });
      invokeWriterMethod(invokeWriterMethod(childDataRow, 'userId', 'user-B'), 'action', 'submit');
      childBuffer.timestamp[0] = 4000n;
      childBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

      childBuffer._writeIndex = 1;
      rootBuffer._children.push(childBuffer);
      childBuffer._parent = rootBuffer;

      // convertSpanTreeToArrowTable uses the shared-dict path (buildSortedCategoryDictionary)
      const table = convertSpanTreeToArrowTable(rootBuffer);
      expect(table.numRows).toBe(4);

      // Category column should have dictionary-encoded strings (not corrupted by binary column presence)
      const userIdCol = table.getChild('userId');
      expect(userIdCol).toBeDefined();
      expect(userIdCol?.at(0)).toBe('user-A');
      expect(userIdCol?.at(1)).toBe('user-A');
      expect(userIdCol?.at(2)).toBe('user-B');
      expect(userIdCol?.at(3)).toBe('user-B');

      // Text column should also work
      const actionCol = table.getChild('action');
      expect(actionCol).toBeDefined();
      const actions = [actionCol?.at(0), actionCol?.at(1), actionCol?.at(2), actionCol?.at(3)];
      expect(actions).toContain('click');
      expect(actions).toContain('scroll');
      expect(actions).toContain('submit');

      // Binary column should have msgpack-encoded objects
      const payloadCol = requireColumn(table, 'payload');
      expect(payloadCol).toBeDefined();
      const payloads: unknown[] = [];
      for (let i = 0; i < payloadCol.length; i++) {
        const bytes = payloadCol.at(i);
        if (bytes instanceof Uint8Array) {
          payloads.push(decode(bytes));
        }
      }
      expect(payloads).toContainEqual({ req: 1 });
      expect(payloads).toContainEqual({ req: 2 });
      expect(payloads).toContainEqual({ req: 3 });
      expect(payloads).toContainEqual({ req: 4 });
    });
  });

  describe('msgpack encoding correctness', () => {
    it('correctly roundtrips various JavaScript types through msgpack', () => {
      const schema = createTestSchema({
        payload: S.unknown(),
      });

      const testValues = [
        42,
        'hello',
        true,
        null,
        [1, 'two', 3],
        { nested: { deep: { value: 99 } } },
        new Uint8Array([1, 2, 3]),
      ];

      for (const testValue of testValues) {
        const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), createTestOpMetadata());
        const writer = createColumnWriter(schema, buffer);

        invokeWriterMethod(nextWriterRow(writer), 'payload', testValue);
        buffer.timestamp[0] = 1000n;
        buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
        buffer._writeIndex = 1;

        const table = convertToArrowTable(buffer);
        const payloadCol = requireColumn(table, 'payload');
        const bytes = requireBinaryCell(payloadCol, 0);
        const decoded = decode(bytes);

        if (testValue instanceof Uint8Array) {
          // msgpack encodes Uint8Array as bin, decoded as Uint8Array
          expect(decoded).toEqual(testValue);
        } else {
          expect(decoded).toEqual(testValue);
        }
      }
    });
  });
});
