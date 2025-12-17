import { describe, expect, it } from 'bun:test';
import { createColumnBuffer } from '@smoothbricks/arrow-builder';
import { createSpanBuffer, createTagWriter, defineTagAttributes, S } from '@smoothbricks/lmao';
import { createTestTaskContext } from '../test-helpers.js';

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

    // Access ONE column via _values suffix (category = Array now, not Uint32Array)
    // Note: buffer.userId is a setter method, buffer.userId_values is the getter
    const userIdColumn = buffer.userId_values;
    expect(Array.isArray(userIdColumn)).toBe(true);

    // Now check that OTHER columns were NOT allocated
    // Getters are on the prototype, not the instance
    const proto = Object.getPrototypeOf(buffer);
    const descriptor = Object.getOwnPropertyDescriptor(proto, 'requestId_values');
    expect(descriptor?.get).toBeDefined(); // Should be a getter on prototype

    // After access, the getter should be replaced with the value
    const requestIdColumn = buffer.requestId_values;
    expect(Array.isArray(requestIdColumn)).toBe(true);
  });

  it('should allocate null bitmaps lazily', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
    });
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Access userId column via _values suffix (category = Array now)
    const userIdColumn = buffer.userId_values;
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

    // Only access 2 of 5 columns (use _values suffix for the array)
    (buffer.col1_values as Float64Array)[0] = 1;
    (buffer.col3_values as Float64Array)[0] = 3;

    // col2, col4, col5 should still be getters on prototype (not accessed yet)
    // Note: use _values suffix to check the getter (not the setter method)
    const proto = Object.getPrototypeOf(buffer);
    const col2Desc = Object.getOwnPropertyDescriptor(proto, 'col2_values');
    const col4Desc = Object.getOwnPropertyDescriptor(proto, 'col4_values');
    const col5Desc = Object.getOwnPropertyDescriptor(proto, 'col5_values');

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

    // Before access: should be getter on prototype (use _values suffix)
    const proto = Object.getPrototypeOf(buffer);
    const beforeDesc = Object.getOwnPropertyDescriptor(proto, 'field1_values');
    expect(beforeDesc?.get).toBeDefined();

    // Access the column via _values suffix (category = Array now)
    const column = buffer.field1_values;
    expect(Array.isArray(column)).toBe(true);

    // After access: the property should still work (getter returns cached value from symbol-keyed storage)
    const afterValue = buffer.field1_values;
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

    // Access one attribute column via _values suffix - should allocate it lazily on first access
    const userIdColumn = buffer.userId_values as string[];
    // Category stores raw strings in Array
    expect(Array.isArray(userIdColumn)).toBe(true);
    expect(userIdColumn.length).toBeGreaterThan(0);

    // Access null bitmap - should allocate it lazily on first access
    const userIdNulls = buffer.userId_nulls;
    expect(userIdNulls).toBeInstanceOf(Uint8Array);
    expect(userIdNulls.length).toBeGreaterThan(0);

    // Verify lazy allocation worked by checking that multiple accesses return same object
    expect(buffer.userId_values).toBe(userIdColumn);
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

    // Access each column via _values suffix and verify type
    // Per spec 01a:
    // - enum: Uint8Array (small enums) or Uint16Array/Uint32Array (larger)
    // - category: Array<string> (raw strings, no hot-path interning)
    // - text: Array<string> (raw strings)
    expect(buffer.smallEnum_values).toBeInstanceOf(Uint8Array);
    expect(Array.isArray(buffer.category_values)).toBe(true); // Category stores raw strings
    expect(Array.isArray(buffer.text_values)).toBe(true); // Text stores raw strings
    expect(buffer.num_values).toBeInstanceOf(Float64Array);
    expect(buffer.bool_values).toBeInstanceOf(Uint8Array);
  });

  it('should allocate small enum as Uint8Array', () => {
    // Create enum with < 256 values (at the limit)
    const enumValues = Array.from({ length: 256 }, (_, i) => `VALUE_${i}`);
    const schema = defineTagAttributes({
      maxEnum: S.enum(enumValues),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Should use Uint8Array for <= 255 values (use _values suffix)
    expect(buffer.maxEnum_values).toBeInstanceOf(Uint8Array);
  });

  it('should work correctly when writing to lazily-allocated columns', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      count: S.number(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // Write to columns via _values suffix (triggering lazy allocation)
    // Category stores strings in Array (no hot-path interning)
    const userIdColumn = buffer.userId_values as string[];
    const countColumn = buffer.count_values as Float64Array;

    // Write a string value for category (raw strings stored directly)
    userIdColumn[0] = 'user-123';
    countColumn[0] = 45.67;

    // Verify values
    expect(userIdColumn[0]).toBe('user-123');
    expect(countColumn[0]).toBe(45.67);
  });

  it('should allow multiple accesses to the same column without re-allocation', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const buffer = createColumnBuffer(schemaFields, 64);

    // First access (use _values suffix)
    const column1 = buffer.userId_values;

    // Second access - should return same array
    const column2 = buffer.userId_values;

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

describe('SpanBuffer Lazy Column Allocation', () => {
  it('should have eager columns allocated immediately and lazy columns undefined until accessed', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
      count: S.number(),
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
    });

    // Extract schema fields
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const taskContext = createTestTaskContext(schemaFields);
    const buffer = createSpanBuffer(schemaFields, taskContext, undefined, 8);

    // Eager columns (system columns) should be allocated immediately
    expect(buffer._timestamps).toBeDefined();
    expect(buffer._timestamps).toBeInstanceOf(BigInt64Array);
    expect(buffer._operations).toBeDefined();
    expect(buffer._operations).toBeInstanceOf(Uint8Array);
    // Also check public aliases
    expect(buffer.timestamps).toBe(buffer._timestamps);
    expect(buffer.operations).toBe(buffer._operations);

    // Lazy columns (user attributes) should be undefined before access
    expect(buffer.getColumnIfAllocated('userId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('requestId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('count')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('operation')).toBeUndefined();

    // Access one lazy column - should trigger allocation
    const userIdColumn = buffer.userId_values;
    expect(Array.isArray(userIdColumn)).toBe(true);

    // After access, the column should be allocated
    expect(buffer.getColumnIfAllocated('userId')).toBeDefined();
    expect(buffer.getColumnIfAllocated('userId')).toBe(userIdColumn);

    // Other columns should still be undefined
    expect(buffer.getColumnIfAllocated('requestId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('count')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('operation')).toBeUndefined();

    // Access another column
    const countColumn = buffer.count_values;
    expect(countColumn).toBeInstanceOf(Float64Array);

    // Now both accessed columns should be allocated
    expect(buffer.getColumnIfAllocated('userId')).toBeDefined();
    expect(buffer.getColumnIfAllocated('count')).toBeDefined();
    expect(buffer.getColumnIfAllocated('count')).toBe(countColumn);

    // Unaccessed columns should still be undefined
    expect(buffer.getColumnIfAllocated('requestId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('operation')).toBeUndefined();
  });

  it('should allocate lazy columns when TagWriter writes to them', () => {
    const schema = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
      count: S.number(),
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
    });

    // Extract schema fields
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const taskContext = createTestTaskContext(schemaFields);
    const buffer = createSpanBuffer(schemaFields, taskContext, undefined, 8);

    // Lazy columns should be undefined before TagWriter access
    expect(buffer.getColumnIfAllocated('userId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('requestId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('count')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('operation')).toBeUndefined();

    // Create TagWriter and write to one column
    const tagWriter = createTagWriter(schemaFields, buffer);
    tagWriter.userId('user-123');

    // After TagWriter write, the column should be allocated
    expect(buffer.getColumnIfAllocated('userId')).toBeDefined();
    const userIdColumn = buffer.getColumnIfAllocated('userId') as string[];
    expect(Array.isArray(userIdColumn)).toBe(true);
    expect(userIdColumn[0]).toBe('user-123');

    // Other columns should still be undefined
    expect(buffer.getColumnIfAllocated('requestId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('count')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('operation')).toBeUndefined();

    // Write to another column via TagWriter
    tagWriter.count(42);

    // Now both columns should be allocated
    expect(buffer.getColumnIfAllocated('userId')).toBeDefined();
    expect(buffer.getColumnIfAllocated('count')).toBeDefined();
    const countColumn = buffer.getColumnIfAllocated('count') as Float64Array;
    expect(countColumn).toBeInstanceOf(Float64Array);
    expect(countColumn[0]).toBe(42);

    // Unaccessed columns should still be undefined
    expect(buffer.getColumnIfAllocated('requestId')).toBeUndefined();
    expect(buffer.getColumnIfAllocated('operation')).toBeUndefined();

    // Write to enum column via TagWriter
    tagWriter.operation('CREATE');

    // Enum column should now be allocated
    expect(buffer.getColumnIfAllocated('operation')).toBeDefined();
    const operationColumn = buffer.getColumnIfAllocated('operation') as Uint8Array;
    expect(operationColumn).toBeInstanceOf(Uint8Array);
    // Enum values are mapped to indices (CREATE = 0, READ = 1, etc.)
    expect(operationColumn[0]).toBe(0);
  });
});
