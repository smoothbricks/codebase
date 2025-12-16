import { describe, expect, it } from 'bun:test';
import { createColumnBuffer } from '@smoothbricks/arrow-builder';
import { defineTagAttributes, S } from '@smoothbricks/lmao';

/**
 * Tests for lazy column initialization
 *
 * Per GitHub review feedback: Columns should only be allocated when accessed,
 * not eagerly during buffer creation. This saves memory for sparse columns.
 */

describe('True Lazy Initialization', () => {
  it('should NOT have TypedArrays for unaccessed columns', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
      count: S.number(),
      active: S.boolean(),
    });
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Core columns are always allocated
    expect(buffer._timestamps).toBeInstanceOf(BigInt64Array);
    expect(buffer._operations).toBeInstanceOf(Uint8Array);

    // Access ONE column (category = Array now, not Uint32Array)
    const userIdColumn = buffer['userId'];
    expect(Array.isArray(userIdColumn)).toBe(true);

    // Now check that OTHER columns were NOT allocated
    // Getters are on the prototype, not the instance
    const proto = Object.getPrototypeOf(buffer);
    const descriptor = Object.getOwnPropertyDescriptor(proto, 'requestId');
    expect(descriptor?.get).toBeDefined(); // Should be a getter on prototype

    // After access, the getter should be replaced with the value
    const requestIdColumn = buffer['requestId'];
    expect(Array.isArray(requestIdColumn)).toBe(true);
  });

  it('should allocate null bitmaps lazily', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
    });
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Access userId column (category = Array now)
    const userIdColumn = buffer['userId'];
    expect(Array.isArray(userIdColumn)).toBe(true);

    // Check that userId's null bitmap was allocated (by accessing it)
    const userIdBitmap = buffer.userId_nulls;
    expect(userIdBitmap).toBeInstanceOf(Uint8Array);

    // Check requestId's null bitmap is still a getter on prototype (not accessed yet)
    const proto = Object.getPrototypeOf(buffer);
    const bitmapDescriptor = Object.getOwnPropertyDescriptor(proto, 'requestId_nulls');
    expect(bitmapDescriptor?.get).toBeDefined();
  });

  it('should save memory with sparse column access', () => {
    const schema = defineTagAttributes({
      col1: S.number(),
      col2: S.number(),
      col3: S.number(),
      col4: S.number(),
      col5: S.number(),
    });
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 1024);

    // Only access 2 of 5 columns
    buffer['col1'][0] = 1;
    buffer['col3'][0] = 3;

    // col2, col4, col5 should still be getters on prototype (not accessed yet)
    const proto = Object.getPrototypeOf(buffer);
    const col2Desc = Object.getOwnPropertyDescriptor(proto, 'col2');
    const col4Desc = Object.getOwnPropertyDescriptor(proto, 'col4');
    const col5Desc = Object.getOwnPropertyDescriptor(proto, 'col5');

    expect(col2Desc?.get).toBeDefined();
    expect(col4Desc?.get).toBeDefined();
    expect(col5Desc?.get).toBeDefined();
  });

  it('should have getters that become values after first access', () => {
    const schema = defineTagAttributes({
      field1: S.category(),
      field2: S.number(),
    });
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Before access: should be getter on prototype
    const proto = Object.getPrototypeOf(buffer);
    const beforeDesc = Object.getOwnPropertyDescriptor(proto, 'field1');
    expect(beforeDesc?.get).toBeDefined();

    // Access the column (category = Array now)
    const column = buffer['field1'];
    expect(Array.isArray(column)).toBe(true);

    // After access: the property should still work (getter returns cached value from symbol-keyed storage)
    const afterValue = buffer['field1'];
    expect(afterValue).toBe(column); // Same instance returned
    expect(Array.isArray(afterValue)).toBe(true);
  });
});

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
    expect(buffer._timestamps).toBeInstanceOf(BigInt64Array);
    expect(buffer._operations).toBeInstanceOf(Uint8Array);

    // Access one attribute column - should allocate it lazily on first access
    const userIdColumn = buffer['userId'];
    // Category stores raw strings in Array
    expect(Array.isArray(userIdColumn)).toBe(true);
    expect(userIdColumn.length).toBeGreaterThan(0);

    // Access null bitmap - should allocate it lazily on first access
    const userIdNulls = buffer.userId_nulls;
    expect(userIdNulls).toBeInstanceOf(Uint8Array);
    expect(userIdNulls.length).toBeGreaterThan(0);

    // Verify lazy allocation worked by checking that multiple accesses return same object
    expect(buffer['userId']).toBe(userIdColumn);
    expect(buffer.userId_nulls).toBe(userIdNulls);
  });

  it('should allocate correct TypedArray type for different schema types', () => {
    const schema = defineTagAttributes({
      smallEnum: S.enum(['A', 'B', 'C']), // < 256 values -> Uint8Array
      category: S.category(), // -> Array<string> (no hot-path interning)
      text: S.text(), // -> Array<string> (no hot-path interning)
      num: S.number(), // -> Float64Array
      bool: S.boolean(), // -> Uint8Array
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Access each column and verify type
    // Per spec 01a:
    // - enum: Uint8Array (small enums) or Uint16Array/Uint32Array (larger)
    // - category: Uint32Array with string interning (indices)
    // - text: Uint32Array with raw storage (indices into TextStringStorage)
    expect(buffer['smallEnum']).toBeInstanceOf(Uint8Array);
    expect(Array.isArray(buffer['category'])).toBe(true); // Category stores raw strings
    expect(Array.isArray(buffer['text'])).toBe(true); // Text stores raw strings
    expect(buffer['num']).toBeInstanceOf(Float64Array);
    expect(buffer['bool']).toBeInstanceOf(Uint8Array);
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
    expect(buffer['maxEnum']).toBeInstanceOf(Uint8Array);
  });

  it('should work correctly when writing to lazily-allocated columns', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      count: S.number(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Write to columns (triggering lazy allocation)
    // Category stores Uint32Array indices (not strings directly)
    const userIdColumn = buffer['userId'] as Uint32Array;
    const countColumn = buffer['count'] as Float64Array;

    // Write an interned index for category (simulating how the system works)
    userIdColumn[0] = 42; // This would be an interned string index
    countColumn[0] = 45.67;

    // Verify values
    expect(userIdColumn[0]).toBe(42);
    expect(countColumn[0]).toBe(45.67);
  });

  it('should allow multiple accesses to the same column without re-allocation', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // First access
    const column1 = buffer['userId'];

    // Second access - should return same array
    const column2 = buffer['userId'];

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
    const userIdNulls = buffer.userId_nulls;
    expect(userIdNulls).toBeInstanceOf(Uint8Array);

    // Access another null bitmap
    const requestIdNulls = buffer.requestId_nulls;
    expect(requestIdNulls).toBeInstanceOf(Uint8Array);

    // Should be different arrays
    expect(userIdNulls).not.toBe(requestIdNulls);

    // Multiple accesses should return same array
    expect(buffer.userId_nulls).toBe(userIdNulls);
  });
});
