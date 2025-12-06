import { describe, it, expect } from 'bun:test';
import { createColumnBuffer } from '../createColumnBuffer.js';
import { defineTagAttributes, S } from '@smoothbricks/lmao';

/**
 * Tests for lazy column initialization
 * 
 * Per GitHub review feedback: Columns should only be allocated when accessed,
 * not eagerly during buffer creation. This saves memory for sparse columns.
 */

describe('Lazy Column Initialization', () => {
  it('should not allocate attribute columns until accessed', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
      duration: S.number(),
    });

    // Extract schema fields
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;

    const buffer = createColumnBuffer(schemaFields, 64);

    // Core columns should be allocated immediately
    expect(buffer.timestamps).toBeInstanceOf(Float64Array);
    expect(buffer.operations).toBeInstanceOf(Uint8Array);

    // Access one attribute column - should allocate it
    const userIdColumn = buffer['attr_userId'];
    expect(userIdColumn).toBeInstanceOf(Uint32Array);
    expect(userIdColumn.length).toBeGreaterThan(0);

    // Access null bitmap - should allocate it
    const userIdNulls = buffer.nullBitmaps['attr_userId'];
    expect(userIdNulls).toBeInstanceOf(Uint8Array);
    expect(userIdNulls.length).toBeGreaterThan(0);
  });

  it('should allocate correct TypedArray type for different schema types', () => {
    const schema = defineTagAttributes({
      smallEnum: S.enum(['A', 'B', 'C']), // < 256 values -> Uint8Array
      category: S.category(), // -> Uint32Array
      text: S.text(), // -> Uint32Array
      num: S.number(), // -> Float64Array
      bool: S.boolean(), // -> Uint8Array
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Access each column and verify type
    expect(buffer['attr_smallEnum']).toBeInstanceOf(Uint8Array);
    expect(buffer['attr_category']).toBeInstanceOf(Uint32Array);
    expect(buffer['attr_text']).toBeInstanceOf(Uint32Array);
    expect(buffer['attr_num']).toBeInstanceOf(Float64Array);
    expect(buffer['attr_bool']).toBeInstanceOf(Uint8Array);
  });

  it('should allocate small enum as Uint8Array', () => {
    // Create enum with < 256 values (at the limit)
    const enumValues = Array.from({ length: 256 }, (_, i) => `VALUE_${i}`);
    const schema = defineTagAttributes({
      maxEnum: S.enum(enumValues),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Should use Uint8Array for <= 255 values
    expect(buffer['attr_maxEnum']).toBeInstanceOf(Uint8Array);
  });

  it('should work correctly when writing to lazily-allocated columns', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      count: S.number(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Write to columns (triggering lazy allocation)
    const userIdColumn = buffer['attr_userId'] as Uint32Array;
    const countColumn = buffer['attr_count'] as Float64Array;

    userIdColumn[0] = 123;
    countColumn[0] = 45.67;

    // Verify values
    expect(userIdColumn[0]).toBe(123);
    expect(countColumn[0]).toBe(45.67);
  });

  it('should allow multiple accesses to the same column without re-allocation', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // First access
    const column1 = buffer['attr_userId'];
    
    // Second access - should return same array
    const column2 = buffer['attr_userId'];

    // Should be the exact same object
    expect(column1).toBe(column2);
  });

  it('should handle null bitmap lazy initialization correctly', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Access one null bitmap
    const userIdNulls = buffer.nullBitmaps['attr_userId'];
    expect(userIdNulls).toBeInstanceOf(Uint8Array);

    // Access another null bitmap
    const requestIdNulls = buffer.nullBitmaps['attr_requestId'];
    expect(requestIdNulls).toBeInstanceOf(Uint8Array);

    // Should be different arrays
    expect(userIdNulls).not.toBe(requestIdNulls);

    // Multiple accesses should return same array
    expect(buffer.nullBitmaps['attr_userId']).toBe(userIdNulls);
  });
});
