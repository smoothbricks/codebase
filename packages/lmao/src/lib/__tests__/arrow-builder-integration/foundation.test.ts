import { describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { S } from '../../schema/builder.js';
import { createSpanBuffer } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import type { LogBinding } from '../../types.js';
import { createTestLogBinding, createTestSchema } from '../test-helpers.js';

describe('Buffer Foundation', () => {
  // Helper to create a test module context
  function createTestModule(): LogBinding {
    const schema = createTestSchema({
      userId: S.category(), // Category: user IDs repeat
      count: S.number(),
    });

    // Use createTestModuleContext helper which returns LogBinding
    return createTestLogBinding(schema);
  }

  it('creates SpanBuffer with TypedArrays', () => {
    const module = createTestModule();
    const schema = module.logSchema; // LogSchema instance
    const traceId = createTraceId('trace-123');

    const buf = createSpanBuffer(schema, module, 'test-span', traceId); // Uses DEFAULT_BUFFER_CAPACITY

    // Span identity assertions (unified memory layout)
    expect(typeof buf.span_id).toBe('number');
    expect(buf.span_id).toBeGreaterThan(0);
    expect(buf._hasParent).toBe(false);
    expect(buf.trace_id).toBe(traceId);

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
    expect(buf._logBinding).toBe(module);
  });

  it('creates root SpanBuffer with createSpanBuffer', () => {
    const module = createTestModule();
    const schema = module.logSchema; // LogSchema instance

    const buf = createSpanBuffer(schema, module, 'test-span', createTraceId('trace-999'));

    expect(buf.span_id).toBeGreaterThan(0);
    expect(buf._hasParent).toBe(false);
    expect(buf._parent).toBeUndefined();
    expect(buf._children).toHaveLength(0);
  });

  it('tracks buffer creation in capacity stats', () => {
    const module = createTestModule();
    const schema = module.logSchema; // LogSchema instance

    const initialCount = module.sb_totalCreated;

    createSpanBuffer(schema, module, 'test-span', createTraceId('trace-456'), 64);

    expect(module.sb_totalCreated).toBe(initialCount + 1);
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

    // Create module context with larger schema using test helper
    const module = createTestLogBinding(largeSchema);

    const buf = createSpanBuffer(largeSchema, module, 'test-span', createTraceId('trace-789'), 64);

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
});
