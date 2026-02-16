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
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from '../test-helpers.js';

describe('Binary Arrow Conversion', () => {
  describe('convertToArrowTable (Path 1: single buffer)', () => {
    it('converts S.unknown() column with mixed values to Arrow Binary', () => {
      const schema = createTestSchema({
        payload: S.unknown(),
        requestId: S.category(),
      });

      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      // Write row with an object payload
      (writer.nextRow() as any).payload({ action: 'click', x: 100, y: 200 }).requestId('req-1');
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

      // Write row with a string payload
      (writer.nextRow() as any).payload('simple-string').requestId('req-2');
      buffer.timestamp[1] = 2000n;
      buffer.entry_type[1] = ENTRY_TYPE_INFO;

      buffer._writeIndex = 2;

      const table = convertToArrowTable(buffer);

      // Verify the payload column exists and is binary
      const payloadCol = table.getChild('payload');
      expect(payloadCol).toBeDefined();
      expect(payloadCol!.length).toBe(2);

      // Row 0: msgpack-decoded object should match original
      const row0Bytes = payloadCol!.at(0) as Uint8Array;
      expect(row0Bytes).toBeInstanceOf(Uint8Array);
      const decoded0 = decode(row0Bytes);
      expect(decoded0).toEqual({ action: 'click', x: 100, y: 200 });

      // Row 1: msgpack-decoded string should match original
      const row1Bytes = payloadCol!.at(1) as Uint8Array;
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

      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      const requestData: HttpRequest = {
        method: 'POST',
        url: '/api/users',
        headers: { 'content-type': 'application/json' },
      };
      (writer.nextRow() as any).request(requestData);
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const requestCol = table.getChild('request');
      expect(requestCol).toBeDefined();

      const bytes = requestCol!.at(0) as Uint8Array;
      expect(bytes).toBeInstanceOf(Uint8Array);
      const decoded = decode(bytes) as HttpRequest;
      expect(decoded.method).toBe('POST');
      expect(decoded.url).toBe('/api/users');
      expect(decoded.headers['content-type']).toBe('application/json');
    });

    it('converts S.binary() column (raw Uint8Array) to Arrow Binary', () => {
      const schema = createTestSchema({
        rawData: S.binary(),
      });

      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      const rawBytes = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
      (writer.nextRow() as any).rawData(rawBytes);
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);
      const rawDataCol = table.getChild('rawData');
      expect(rawDataCol).toBeDefined();

      const bytes = rawDataCol!.at(0) as Uint8Array;
      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes).toEqual(new Uint8Array([0xde, 0xad, 0xbe, 0xef]));
    });

    it('handles null/unset binary columns correctly', () => {
      const schema = createTestSchema({
        payload: S.unknown(),
        requestId: S.category(),
      });

      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      // Row 0: payload set
      (writer.nextRow() as any).payload({ key: 'value' }).requestId('req-1');
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

      // Row 1: payload NOT set (null)
      (writer.nextRow() as any).requestId('req-2');
      buffer.timestamp[1] = 2000n;
      buffer.entry_type[1] = ENTRY_TYPE_INFO;

      buffer._writeIndex = 2;

      const table = convertToArrowTable(buffer);
      const payloadCol = table.getChild('payload');
      expect(payloadCol).toBeDefined();
      expect(payloadCol!.length).toBe(2);

      // Row 0: should have data
      const row0 = payloadCol!.at(0) as Uint8Array;
      expect(row0).toBeInstanceOf(Uint8Array);
      expect(decode(row0)).toEqual({ key: 'value' });

      // Row 1: should be null
      const row1 = payloadCol!.at(1);
      expect(row1).toBeNull();
    });

    it('handles binary columns alongside other column types', () => {
      const schema = createTestSchema({
        userId: S.category(),
        httpStatus: S.number(),
        payload: S.unknown(),
        operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
      });

      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      (writer.nextRow() as any)
        .userId('user-1')
        .httpStatus(200)
        .payload({ items: [1, 2, 3] })
        .operation('GET');
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      const table = convertToArrowTable(buffer);

      // Verify all column types work together
      expect(table.getChild('userId')!.at(0)).toBe('user-1');
      expect(table.getChild('httpStatus')!.at(0)).toBe(200);
      expect(table.getChild('operation')!.at(0)).toBe('GET');

      const payloadBytes = table.getChild('payload')!.at(0) as Uint8Array;
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
      const rootBuffer = createSpanBuffer(
        schema,
        'root-span',
        createTestTraceRoot('test-trace'),
        createTestOpMetadata(),
      );
      const rootWriter = createColumnWriter(schema, rootBuffer);

      (rootWriter.nextRow() as any).payload({ level: 'root', data: [1, 2] }).userId('user-root');
      rootBuffer.timestamp[0] = 1000n;
      rootBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      rootBuffer._writeIndex = 1;

      // Create child buffer
      const childBuffer = createSpanBuffer(
        schema,
        'child-span',
        createTestTraceRoot('test-trace'),
        createTestOpMetadata(),
      );
      const childWriter = createColumnWriter(schema, childBuffer);

      (childWriter.nextRow() as any).payload({ level: 'child', nested: { a: 1 } }).userId('user-child');
      childBuffer.timestamp[0] = 2000n;
      childBuffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      childBuffer._writeIndex = 1;

      // Link parent-child
      rootBuffer._children.push(childBuffer);
      childBuffer._parent = rootBuffer;

      const table = convertSpanTreeToArrowTable(rootBuffer);
      const payloadCol = table.getChild('payload');
      expect(payloadCol).toBeDefined();
      expect(payloadCol!.length).toBe(2);

      // Both rows should have msgpack-encoded binary data
      const row0 = decode(payloadCol!.at(0) as Uint8Array);
      const row1 = decode(payloadCol!.at(1) as Uint8Array);

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

      const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('test-trace'), createTestOpMetadata());
      const writer = createColumnWriter(schema, buffer);

      const obj: Record<string, unknown> = { x: 1, y: 2, label: 'original' };

      (writer.nextRow() as any).payload(obj);
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 1;

      // Mutate AFTER tagging -- Object.freeze should prevent this from affecting the stored reference
      expect(() => {
        obj.x = 999;
      }).toThrow(); // frozen objects throw in strict mode

      const table = convertToArrowTable(buffer);
      const payloadCol = table.getChild('payload');
      expect(payloadCol).toBeDefined();

      const bytes = payloadCol!.at(0) as Uint8Array;
      const decoded = decode(bytes) as Record<string, unknown>;
      expect(decoded).toEqual({ x: 1, y: 2, label: 'original' });
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
        const buffer = createSpanBuffer(schema, 'test-span', createTestTraceRoot('test-trace'), createTestOpMetadata());
        const writer = createColumnWriter(schema, buffer);

        (writer.nextRow() as any).payload(testValue);
        buffer.timestamp[0] = 1000n;
        buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
        buffer._writeIndex = 1;

        const table = convertToArrowTable(buffer);
        const payloadCol = table.getChild('payload');
        const bytes = payloadCol!.at(0) as Uint8Array;
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
