import { describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { DEFAULT_METADATA } from '../../opContext/defineOp.js';
import { S } from '../../schema/builder.js';
import { createSpanBuffer, getSpanBufferClass, type SpanBufferConstructor } from '../../spanBuffer.js';

import { createBuffer, createTestSchema, createTestTraceRoot, createTraceId } from '../test-helpers.js';

describe('Buffer Foundation', () => {
  it('creates SpanBuffer with TypedArrays', () => {
    const schema = createTestSchema({
      userId: S.category(), // Category: user IDs repeat
      count: S.number(),
    });
    const buf = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-123'), DEFAULT_METADATA); // Uses DEFAULT_BUFFER_CAPACITY

    // Span identity assertions (unified memory layout)
    expect(typeof buf.span_id).toBe('number');
    expect(buf.span_id).toBeGreaterThan(0);
    expect(buf._hasParent).toBe(false);
    expect(buf.trace_id).toBe(createTraceId('trace-123'));

    // Check TypedArrays are created
    expect(buf.timestamp).toBeInstanceOf(BigInt64Array);
    expect(buf.entry_type).toBeInstanceOf(Uint8Array);

    // Check null bitmaps exist for each attribute (Arrow format: 1 Uint8Array per column)
    expect(buf.userId_nulls).toBeInstanceOf(Uint8Array);
    expect(buf.count_nulls).toBeInstanceOf(Uint8Array);

    // Check attribute columns exist for each schema field (using _values suffix)
    // category columns now use string[] arrays for zero-cost hot path writes
    expect(Array.isArray(buf.userId_values)).toBe(true); // category → string[]
    expect(buf.count_values).toBeInstanceOf(Float64Array); // number → Float64Array

    // Metadata
    expect(buf._children).toBeInstanceOf(Array);
    expect(buf._writeIndex).toBe(0);
    expect(buf._capacity).toBe(DEFAULT_BUFFER_CAPACITY);
  });

  it('creates root SpanBuffer with createSpanBuffer', () => {
    const schema = createTestSchema({
      userId: S.category(),
      count: S.number(),
    });

    const buf = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-999'), DEFAULT_METADATA);

    expect(buf.span_id).toBeGreaterThan(0);
    expect(buf._hasParent).toBe(false);
    expect(buf._parent).toBeUndefined();
    expect(buf._children).toHaveLength(0);
  });

  it('provides access to buffer stats via constructor', () => {
    const schema = createTestSchema({
      userId: S.category(),
      count: S.number(),
    });

    // Get SpanBufferClass to access stats
    const SpanBufferClass = getSpanBufferClass(schema);
    const buf = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-456'), DEFAULT_METADATA, 64);

    // Stats are accessible from constructor
    const bufferClass = buf.constructor as SpanBufferConstructor;
    expect(bufferClass.stats).toBe(SpanBufferClass.stats);
    expect(bufferClass.stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);
  });

  it('handles different schema sizes', () => {
    // Define a larger schema
    const largeSchema = createTestSchema({
      field1: S.category(), // Category string
      field2: S.number(),
      field3: S.boolean(),
      field4: S.text(), // Text string
      field5: S.number(),
    });

    const buf = createSpanBuffer(largeSchema, 'test-span', createTestTraceRoot('trace-789'), DEFAULT_METADATA, 64);

    // Should have TypedArray columns for all 5 attributes (each has _values and _nulls)
    // Note: Columns are lazy-allocated via getters, so Object.keys() won't find them
    // Access them directly to trigger allocation and verify they exist
    expect(Array.isArray(buf.field1_values)).toBe(true); // category (raw strings)
    expect(buf.field1_nulls).toBeInstanceOf(Uint8Array); // null bitmap
    expect(buf.field2_values).toBeInstanceOf(Float64Array); // number
    expect(buf.field2_nulls).toBeInstanceOf(Uint8Array);
    expect(buf.field3_values).toBeInstanceOf(Uint8Array); // boolean
    expect(buf.field3_nulls).toBeInstanceOf(Uint8Array);
    expect(Array.isArray(buf.field4_values)).toBe(true); // text (raw strings)
    expect(buf.field4_nulls).toBeInstanceOf(Uint8Array);
    expect(buf.field5_values).toBeInstanceOf(Float64Array); // number
    expect(buf.field5_nulls).toBeInstanceOf(Uint8Array);
  });

  it('accesses schema from SpanBufferConstructor', () => {
    const schema = createTestSchema({
      userId: S.category(),
      count: S.number(),
    });

    const buf = createSpanBuffer(schema, 'test-span', createTestTraceRoot('trace-123'), DEFAULT_METADATA);
    const SpanBufferClass = buf.constructor as SpanBufferConstructor;

    // Schema is accessible via static property
    expect(SpanBufferClass.schema).toBe(schema);
    expect(SpanBufferClass.schema.fields.userId).toBeDefined();
    expect(SpanBufferClass.schema.fields.count).toBeDefined();
  });

  it('accesses stats from SpanBufferConstructor', () => {
    const schema = createTestSchema({
      userId: S.category(),
      count: S.number(),
    });

    const buf = createBuffer(schema);
    const SpanBufferClass = buf.constructor as SpanBufferConstructor;

    // Stats are accessible via static property (clean names, no sb_ prefix)
    expect(typeof SpanBufferClass.stats.capacity).toBe('number');
    expect(typeof SpanBufferClass.stats.totalWrites).toBe('number');
    expect(typeof SpanBufferClass.stats.overflowWrites).toBe('number');
    expect(typeof SpanBufferClass.stats.totalCreated).toBe('number');
    expect(typeof SpanBufferClass.stats.overflows).toBe('number');
  });
});
