import { describe, expect, test } from 'bun:test';
import { S } from '../../schema/builder.js';
import { createGeneratedColumnBuffer } from '../columnBufferGenerator.js';

// Create a schema with all types using the S schema builder
const mockSchema = {
  status: S.enum(['pending', 'active', 'completed'] as const),
  userId: S.category(),
  errorMsg: S.text(),
  count: S.number(),
  isActive: S.boolean(),
};

// Schema with eager column (no null bitmap, allocated in constructor)
const eagerSchema = {
  message: S.category().eager(),
  userId: S.category(),
};

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
    // Note: TypeScript now knows _writeIndex doesn't exist, which is correct
    expect((buffer as unknown as Record<string, unknown>)._writeIndex).toBeUndefined();
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
    const eagerEnumSchema = {
      status: S.enum(['a', 'b', 'c'] as const).eager(),
    };

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
    const eagerNumberSchema = {
      count: S.number().eager(),
    };

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
    const eagerBoolSchema = {
      active: S.boolean().eager(),
    };

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
    const schema = {
      userId: S.category(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a value first
    buffer.userId(0, 'user-123');

    // Check null bit is set (value is valid)
    const nulls = buffer.userId_nulls;
    expect(nulls[0] & 1).toBe(1); // bit 0 should be set

    // Now write null to mark as null
    buffer.userId(0, null as unknown as string);

    // Check null bit is cleared (value is null)
    expect(nulls[0] & 1).toBe(0); // bit 0 should be cleared
  });

  test('lazy column setter handles undefined by clearing null bit', () => {
    const schema = {
      count: S.number(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a value first
    buffer.count(0, 42);
    buffer.count(1, 100);

    // Check null bits are set
    const nulls = buffer.count_nulls;
    expect(nulls[0] & 0b11).toBe(0b11); // bits 0 and 1 should be set

    // Write undefined to position 0
    buffer.count(0, undefined as unknown as number);

    // Check bit 0 is cleared, bit 1 still set
    expect(nulls[0] & 0b11).toBe(0b10); // bit 0 cleared, bit 1 set
  });

  test('eager column setter writes default value for null', () => {
    const schema = {
      message: S.text().eager(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a real value
    buffer.message(0, 'hello');
    expect(buffer.message_values[0]).toBe('hello');

    // Write null - should write empty string (default)
    buffer.message(0, null as unknown as string);
    expect(buffer.message_values[0]).toBe('');
  });

  test('eager number column setter writes 0 for null', () => {
    const schema = {
      count: S.number().eager(),
    };

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a real value
    buffer.count(0, 42);
    expect(buffer.count_values[0]).toBe(42);

    // Write null - should write 0 (default)
    buffer.count(0, null as unknown as number);
    expect(buffer.count_values[0]).toBe(0);
  });

  test('lazy enum column handles null correctly', () => {
    const schema = {
      status: S.enum(['PENDING', 'ACTIVE', 'DONE']),
    };

    const buffer = createGeneratedColumnBuffer(schema, 10);

    // Write a value
    buffer.status(0, 1); // ACTIVE index
    expect(buffer.status_nulls[0] & 1).toBe(1);
    expect(buffer.status_values[0]).toBe(1);

    // Write null
    buffer.status(0, null as unknown as number);
    expect(buffer.status_nulls[0] & 1).toBe(0); // null bit cleared
  });
});
