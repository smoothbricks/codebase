import { describe, expect, test } from 'bun:test';
import { S } from '../../schema/builder.js';
import { ColumnSchema } from '../../schema/ColumnSchema.js';
import { createGeneratedColumnBuffer } from '../columnBufferGenerator.js';

// Create a schema with all types using the S schema builder
const mockSchema = new ColumnSchema({
  status: S.enum(['pending', 'active', 'completed'] as const),
  userId: S.category(),
  errorMsg: S.text(),
  count: S.number(),
  isActive: S.boolean(),
  timestamp: S.bigUint64(),
});

// Schema with eager column (no null bitmap, allocated in constructor)
const eagerSchema = new ColumnSchema({
  message: S.category().eager(),
  userId: S.category(),
});

function callSetter(buffer: object, setterName: string, pos: number, value: unknown): void {
  const setter = Reflect.get(buffer, setterName);
  if (typeof setter !== 'function') {
    throw new Error(`Missing setter: ${setterName}`);
  }
  Reflect.apply(setter, buffer, [pos, value]);
}

describe('ColumnBuffer setter methods', () => {
  test('should generate setter methods for each column', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);

    // Verify setter methods exist and are functions
    expect(typeof buffer.status).toBe('function');
    expect(typeof buffer.userId).toBe('function');
    expect(typeof buffer.errorMsg).toBe('function');
    expect(typeof buffer.count).toBe('function');
    expect(typeof buffer.isActive).toBe('function');
  });

  test('setter should write value and return this for chaining', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);

    // Write at position 0 - typed setters should work without casting
    const result = buffer.userId(0, 'user123');

    // Should return this for chaining
    expect(result).toBe(buffer);

    // Should have written the value
    expect(buffer.userId_values[0]).toBe('user123');

    // Should have set the null bit (mark as valid)
    expect(buffer.userId_nulls[0] & 1).toBe(1);
  });

  test('enum setter should accept numeric indices directly', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);

    // Buffer now accepts numeric indices directly (string→index conversion
    // happens in the higher-level TagWriter, not in the buffer).
    // Enum setters now accept number type directly.
    buffer.status(0, 0); // pending = 0
    buffer.status(1, 1); // active = 1
    buffer.status(2, 2); // completed = 2
    buffer.status(3, 0); // default to 0

    expect(buffer.status_values[0]).toBe(0);
    expect(buffer.status_values[1]).toBe(1);
    expect(buffer.status_values[2]).toBe(2);
    expect(buffer.status_values[3]).toBe(0);
  });

  test('boolean setter should use bit-packed storage', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 16);

    buffer.isActive(0, true);
    buffer.isActive(1, false);
    buffer.isActive(2, true);
    buffer.isActive(7, true);
    buffer.isActive(8, true); // Second byte

    // First byte: bits 0, 2, 7 set = 0b10000101 = 133
    expect(buffer.isActive_values[0]).toBe(133);
    // Second byte: bit 0 set = 1
    expect(buffer.isActive_values[1]).toBe(1);
  });

  test('number setter should write to Float64Array', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);

    buffer.count(0, 42.5);
    buffer.count(1, 100);

    expect(buffer.count_values[0]).toBe(42.5);
    expect(buffer.count_values[1]).toBe(100);
  });

  test('text setter should write string directly', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);

    buffer.errorMsg(0, 'Something went wrong');
    buffer.errorMsg(1, 'Another error');

    expect(buffer.errorMsg_values[0]).toBe('Something went wrong');
    expect(buffer.errorMsg_values[1]).toBe('Another error');
  });

  test('setters should be chainable', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);

    // Chain multiple setters at the same position
    // Note: status now takes numeric index (1 = 'active'), not string
    buffer.userId(0, 'user1');
    buffer.status(0, 1); // active = 1
    buffer.count(0, 5).isActive(0, true);

    expect(buffer.userId_values[0]).toBe('user1');
    expect(buffer.status_values[0]).toBe(1); // active = 1
    expect(buffer.count_values[0]).toBe(5);
    expect((buffer.isActive_values as Uint8Array)[0] & 1).toBe(1);
  });

  test('_writeIndex should not exist on buffer', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);
    // _writeIndex is tracked by ColumnWriter, not ColumnBuffer
    expect('_writeIndex' in buffer).toBe(false);
  });

  test('null bitmap should mark positions as valid when written', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 16);

    // Write to positions 0, 5, 10
    buffer.userId(0, 'user0');
    buffer.userId(5, 'user5');
    buffer.userId(10, 'user10');

    // Check null bits are set for written positions
    // Position 0: byte 0, bit 0 -> 0b00000001
    // Position 5: byte 0, bit 5 -> 0b00100000
    // Combined: 0b00100001 = 33
    expect(buffer.userId_nulls[0]).toBe(33);

    // Position 10: byte 1, bit 2 -> 0b00000100 = 4
    expect(buffer.userId_nulls[1]).toBe(4);
  });

  test('category setter should write strings directly', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema, 10);

    buffer.userId(0, 'alice');
    buffer.userId(1, 'bob');
    buffer.userId(2, 'alice'); // Duplicate value

    expect(buffer.userId_values[0]).toBe('alice');
    expect(buffer.userId_values[1]).toBe('bob');
    expect(buffer.userId_values[2]).toBe('alice');
  });
});

describe('Eager column support (__eager: true)', () => {
  test('eager column should be allocated in constructor (not lazily)', () => {
    const buffer = createGeneratedColumnBuffer(eagerSchema, 10);

    // Eager column should have values property directly accessible
    // WITHOUT triggering lazy allocation (it's already allocated)
    expect(buffer.message_values).toBeDefined();
    expect(Array.isArray(buffer.message_values)).toBe(true);
    expect((buffer.message_values as string[]).length).toBe(16); // Aligned to 16
  });

  test('eager column should NOT have null bitmap', () => {
    const buffer = createGeneratedColumnBuffer(eagerSchema, 10);

    // Eager columns don't have null bitmap (they're always written)
    // getNullsIfAllocated should return undefined for eager columns
    expect(buffer.getNullsIfAllocated('message')).toBeUndefined();
  });

  test('eager column setter should write without null bit', () => {
    const buffer = createGeneratedColumnBuffer(eagerSchema, 10);

    // Write to eager column - typed setters work without casting
    buffer.message(0, 'test message');
    buffer.message(1, 'another message');

    // Values should be written
    expect(buffer.message_values[0]).toBe('test message');
    expect(buffer.message_values[1]).toBe('another message');
  });

  test('lazy column alongside eager column should still have null bitmap', () => {
    const buffer = createGeneratedColumnBuffer(eagerSchema, 10);

    // Write to lazy column
    buffer.userId(0, 'user1');

    // Lazy column should have null bitmap
    expect(buffer.userId_nulls).toBeDefined();
    expect(buffer.userId_nulls[0] & 1).toBe(1); // null bit set
  });

  test('eager column should work with enum type', () => {
    // Now that S.enum() has .eager() method, we can use it directly
    const eagerEnumSchema = new ColumnSchema({
      status: S.enum(['a', 'b', 'c'] as const).eager(),
    });

    const buffer = createGeneratedColumnBuffer(eagerEnumSchema, 10);

    // Should be allocated
    expect(buffer.status_values).toBeDefined();
    expect(buffer.status_values instanceof Uint8Array).toBe(true);

    // Write and verify using typed setters (enum setters accept numeric indices)
    buffer.status(0, 0); // 'a' = 0
    buffer.status(1, 1); // 'b' = 1
    expect(buffer.status_values[0]).toBe(0);
    expect(buffer.status_values[1]).toBe(1);
  });

  test('eager column should work with number type', () => {
    // S.number() now has .eager() method
    const eagerNumberSchema = new ColumnSchema({
      count: S.number().eager(),
    });

    const buffer = createGeneratedColumnBuffer(eagerNumberSchema, 10);

    // Should be allocated as Float64Array
    expect(buffer.count_values).toBeDefined();
    expect(buffer.count_values instanceof Float64Array).toBe(true);

    // Write and verify using typed setters
    buffer.count(0, 42.5);
    expect(buffer.count_values[0]).toBe(42.5);
  });

  test('eager column should work with boolean type', () => {
    // S.boolean() now has .eager() method
    const eagerBoolSchema = new ColumnSchema({
      active: S.boolean().eager(),
    });

    const buffer = createGeneratedColumnBuffer(eagerBoolSchema, 10);

    // Should be allocated as bit-packed Uint8Array
    expect(buffer.active_values).toBeDefined();
    expect(buffer.active_values instanceof Uint8Array).toBe(true);

    // Write and verify using typed setters
    buffer.active(0, true);
    buffer.active(1, false);
    buffer.active(2, true);
    const values = buffer.active_values as Uint8Array;
    expect(values[0] & 0b101).toBe(0b101); // bits 0 and 2 set
  });
});

describe('Null value support in setters', () => {
  test('lazy column setter handles null by clearing null bit', () => {
    const schema = new ColumnSchema({
      userId: S.category(),
    });

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a value first
    buffer.userId(0, 'user-123');

    // Check null bit is set (value is valid)
    const nulls = buffer.userId_nulls;
    expect(nulls[0] & 1).toBe(1); // bit 0 should be set

    // Now write null to mark as null
    callSetter(buffer, 'userId', 0, null);

    // Check null bit is cleared (value is null)
    expect(nulls[0] & 1).toBe(0); // bit 0 should be cleared
  });

  test('lazy column setter handles undefined by clearing null bit', () => {
    const schema = new ColumnSchema({
      count: S.number(),
    });

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a value first
    buffer.count(0, 42);
    buffer.count(1, 100);

    // Check null bits are set
    const nulls = buffer.count_nulls;
    expect(nulls[0] & 0b11).toBe(0b11); // bits 0 and 1 should be set

    // Write undefined to position 0
    callSetter(buffer, 'count', 0, undefined);

    // Check bit 0 is cleared, bit 1 still set
    expect(nulls[0] & 0b11).toBe(0b10); // bit 0 cleared, bit 1 set
  });

  test('eager column setter writes default value for null', () => {
    const schema = new ColumnSchema({
      message: S.text().eager(),
    });

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a real value
    buffer.message(0, 'hello');
    expect(buffer.message_values[0]).toBe('hello');

    // Write null - should write empty string (default)
    callSetter(buffer, 'message', 0, null);
    expect(buffer.message_values[0]).toBe('');
  });

  test('eager number column setter writes 0 for null', () => {
    const schema = new ColumnSchema({
      count: S.number().eager(),
    });

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a real value
    buffer.count(0, 42);
    expect(buffer.count_values[0]).toBe(42);

    // Write null - should write 0 (default)
    callSetter(buffer, 'count', 0, null);
    expect(buffer.count_values[0]).toBe(0);
  });

  test('lazy enum column handles null correctly', () => {
    const schema = new ColumnSchema({
      status: S.enum(['PENDING', 'ACTIVE', 'DONE']),
    });

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a value
    buffer.status(0, 1); // ACTIVE index
    expect(buffer.status_nulls[0] & 1).toBe(1);
    expect(buffer.status_values[0]).toBe(1);

    // Write null
    callSetter(buffer, 'status', 0, null);
    expect(buffer.status_nulls[0] & 1).toBe(0); // null bit cleared
  });
});

describe('ColumnBuffer comprehensive data type coverage', () => {
  test('should handle all supported enum sizes', () => {
    // Small enum (<256 values) - should use Uint8Array
    const smallEnumSchema = new ColumnSchema({
      smallStatus: S.enum(['A', 'B', 'C'] as const),
    });

    // Medium enum (100 values) - should use Uint8Array
    const mediumEnumSchema = new ColumnSchema({
      mediumStatus: S.enum(Array.from({ length: 100 }, (_, i) => `VAL_${i}`)),
    });

    // Large enum (>256 values) - should use Uint16Array
    const largeEnumSchema = new ColumnSchema({
      largeStatus: S.enum(Array.from({ length: 300 }, (_, i) => `BIG_${i}`)),
    });

    const smallBuffer = createGeneratedColumnBuffer(smallEnumSchema, 10);
    const mediumBuffer = createGeneratedColumnBuffer(mediumEnumSchema, 10);
    const largeBuffer = createGeneratedColumnBuffer(largeEnumSchema, 10);

    // Verify TypedArray types
    expect(smallBuffer.smallStatus_values instanceof Uint8Array).toBe(true);
    expect(mediumBuffer.mediumStatus_values instanceof Uint8Array).toBe(true);
    expect(largeBuffer.largeStatus_values instanceof Uint16Array).toBe(true);
  });

  test('should handle BigInt values in bigUint64 columns', () => {
    const bigIntSchema = new ColumnSchema({
      timestamp: S.bigUint64(),
      largeId: S.bigUint64(),
    });

    const buffer = createGeneratedColumnBuffer(bigIntSchema, 10);

    // Test various BigInt values including edge cases
    const testValues = [
      0n,
      1n,
      1234567890n,
      BigInt('18446744073709551615'), // Max uint64
      BigInt('9223372036854775808'), // 2^63
    ];

    testValues.forEach((value, idx) => {
      buffer.timestamp(idx, value);
      buffer.largeId(idx, value * 2n);
      expect(buffer.timestamp_values[idx]).toBe(value);
      // BigUint64Array wraps around at 2^64, so multiply result wraps too
      expect(buffer.largeId_values[idx]).toBe((value * 2n) & 0xffffffffffffffffn);
    });

    // Verify BigUint64Array type
    expect(buffer.timestamp_values instanceof BigUint64Array).toBe(true);
    expect(buffer.largeId_values instanceof BigUint64Array).toBe(true);
  });

  test('should handle null/undefined correctly across all types', () => {
    const testSchema = new ColumnSchema({
      enumCol: S.enum(['A', 'B'] as const),
      categoryCol: S.category(),
      textCol: S.text(),
      numberCol: S.number(),
      booleanCol: S.boolean(),
      bigIntCol: S.bigUint64(),
    });

    const buffer = createGeneratedColumnBuffer(testSchema, 10);

    // Write real values first
    buffer.enumCol(0, 1);
    buffer.categoryCol(0, 'test-category');
    buffer.textCol(0, 'test-text');
    buffer.numberCol(0, 42.5);
    buffer.booleanCol(0, true);
    buffer.bigIntCol(0, 12345n);

    // Verify null bits are set after writing real values (1 = not null)
    expect(buffer.enumCol_nulls[0] & 1).toBe(1);
    expect(buffer.categoryCol_nulls[0] & 1).toBe(1);
    expect(buffer.textCol_nulls[0] & 1).toBe(1);
    expect(buffer.numberCol_nulls[0] & 1).toBe(1);
    expect(buffer.booleanCol_nulls[0] & 1).toBe(1);
    expect(buffer.bigIntCol_nulls[0] & 1).toBe(1);

    // Write null/undefined to clear bits
    callSetter(buffer, 'enumCol', 0, null);
    callSetter(buffer, 'categoryCol', 0, undefined);
    callSetter(buffer, 'textCol', 0, null);
    callSetter(buffer, 'numberCol', 0, undefined);
    callSetter(buffer, 'booleanCol', 0, null);
    callSetter(buffer, 'bigIntCol', 0, undefined);

    // Verify all null bits are cleared
    expect(buffer.enumCol_nulls[0] & 1).toBe(0);
    expect(buffer.categoryCol_nulls[0] & 0b10).toBe(0);
    expect(buffer.textCol_nulls[0] & 0b100).toBe(0);
    expect(buffer.numberCol_nulls[0] & 0b1000).toBe(0);
    expect(buffer.booleanCol_nulls[0] & 0b100000).toBe(0);
    expect(buffer.bigIntCol_nulls[0] & 0b1000000).toBe(0);
  });

  test('should handle boolean bit packing across multiple bytes', () => {
    const booleanSchema = new ColumnSchema({
      flags: S.boolean(),
    });

    const buffer = createGeneratedColumnBuffer(booleanSchema, 25);

    // Set bits across multiple bytes
    buffer.flags(0, true); // byte 0, bit 0
    buffer.flags(7, true); // byte 0, bit 7
    buffer.flags(8, true); // byte 1, bit 0
    buffer.flags(15, true); // byte 1, bit 7
    buffer.flags(16, true); // byte 2, bit 0
    buffer.flags(23, true); // byte 2, bit 7

    // Verify bit patterns
    expect(buffer.flags_values[0]).toBe(0b10000001); // 129
    expect(buffer.flags_values[1]).toBe(0b10000001); // 129
    expect(buffer.flags_values[2]).toBe(0b10000001); // 129
    expect(buffer.flags_values[3]).toBe(0b00000000); // 0 (buffer only allocated 25, but bits are set at specified positions)
  });

  test('should handle capacity boundaries correctly', () => {
    const testSchema = new ColumnSchema({
      value: S.number(),
    });

    // Test with exactly 16 capacity (aligned to 16)
    const buffer = createGeneratedColumnBuffer(testSchema, 16);

    // Should be able to write to all positions 0-15
    for (let i = 0; i < 16; i++) {
      buffer.value(i, i * 10);
    }

    // Verify all values are written
    for (let i = 0; i < 16; i++) {
      expect(buffer.value_values[i]).toBe(i * 10);
      expect(buffer.value_nulls[i >>> 3] & (1 << (i & 7))).toBe(1 << (i & 7));
    }
  });

  test('should handle method chaining correctly', () => {
    const testSchema = new ColumnSchema({
      text: S.text(),
      number: S.number(),
      flag: S.boolean(),
    });

    const buffer = createGeneratedColumnBuffer(testSchema, 10);

    // Test method chaining
    const result = buffer.text(0, 'first').number(0, 42).flag(0, true);

    expect(result).toBe(buffer);
    expect(buffer.text_values[0]).toBe('first');
    expect(buffer.number_values[0]).toBe(42);
    expect(buffer.flag_nulls[0] & 1).toBe(1);
  });
});
