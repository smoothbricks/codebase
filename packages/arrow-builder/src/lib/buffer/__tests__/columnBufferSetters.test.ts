import { describe, expect, test } from 'bun:test';
import { createGeneratedColumnBuffer } from '../columnBufferGenerator.js';

// Create a mock schema with all types
const mockSchema = {
  status: { __schema_type: 'enum', __enum_values: ['pending', 'active', 'completed'] },
  userId: { __schema_type: 'category' },
  errorMsg: { __schema_type: 'text' },
  count: { __schema_type: 'number' },
  isActive: { __schema_type: 'boolean' },
} as const;

describe('ColumnBuffer setter methods', () => {
  test('should generate setter methods for each column', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);

    // Check that setter methods exist (runtime-generated, need cast)
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime-generated methods
    const b = buffer as any;
    expect(typeof b.status).toBe('function');
    expect(typeof b.userId).toBe('function');
    expect(typeof b.errorMsg).toBe('function');
    expect(typeof b.count).toBe('function');
    expect(typeof b.isActive).toBe('function');
  });

  test('setter should write value and return this for chaining', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);

    // Write at position 0
    const result = (buffer as any).userId(0, 'user123');

    // Should return this for chaining
    expect(result).toBe(buffer);

    // Should have written the value
    expect(buffer.userId_values[0]).toBe('user123');

    // Should have set the null bit (mark as valid)
    expect(buffer.userId_nulls[0] & 1).toBe(1);
  });

  test('enum setter should use index mapping', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);

    (buffer as any).status(0, 'pending');
    (buffer as any).status(1, 'active');
    (buffer as any).status(2, 'completed');
    (buffer as any).status(3, 'unknown'); // Should default to 0

    expect(buffer.status_values[0]).toBe(0); // pending = 0
    expect(buffer.status_values[1]).toBe(1); // active = 1
    expect(buffer.status_values[2]).toBe(2); // completed = 2
    expect(buffer.status_values[3]).toBe(0); // unknown defaults to 0
  });

  test('boolean setter should use bit-packed storage', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 16);

    (buffer as any).isActive(0, true);
    (buffer as any).isActive(1, false);
    (buffer as any).isActive(2, true);
    (buffer as any).isActive(7, true);
    (buffer as any).isActive(8, true); // Second byte

    // First byte: bits 0, 2, 7 set = 0b10000101 = 133
    expect(buffer.isActive_values[0]).toBe(133);
    // Second byte: bit 0 set = 1
    expect(buffer.isActive_values[1]).toBe(1);
  });

  test('number setter should write to Float64Array', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);

    (buffer as any).count(0, 42.5);
    (buffer as any).count(1, 100);

    expect(buffer.count_values[0]).toBe(42.5);
    expect(buffer.count_values[1]).toBe(100);
  });

  test('text setter should write string directly', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);

    (buffer as any).errorMsg(0, 'Something went wrong');
    (buffer as any).errorMsg(1, 'Another error');

    expect(buffer.errorMsg_values[0]).toBe('Something went wrong');
    expect(buffer.errorMsg_values[1]).toBe('Another error');
  });

  test('setters should be chainable', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);

    // Chain multiple setters at the same position
    (buffer as any).userId(0, 'user1').status(0, 'active').count(0, 5).isActive(0, true);

    expect(buffer.userId_values[0]).toBe('user1');
    expect(buffer.status_values[0]).toBe(1); // active = 1
    expect(buffer.count_values[0]).toBe(5);
    expect((buffer.isActive_values as Uint8Array)[0] & 1).toBe(1);
  });

  test('_writeIndex should not exist on buffer', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);
    expect((buffer as any)._writeIndex).toBeUndefined();
  });

  test('null bitmap should mark positions as valid when written', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 16);

    // Write to positions 0, 5, 10
    (buffer as any).userId(0, 'user0');
    (buffer as any).userId(5, 'user5');
    (buffer as any).userId(10, 'user10');

    // Check null bits are set for written positions
    // Position 0: byte 0, bit 0 -> 0b00000001
    // Position 5: byte 0, bit 5 -> 0b00100000
    // Combined: 0b00100001 = 33
    const nulls = buffer.userId_nulls as Uint8Array;
    expect(nulls[0]).toBe(33);

    // Position 10: byte 1, bit 2 -> 0b00000100 = 4
    expect(nulls[1]).toBe(4);
  });

  test('category setter should write strings directly', () => {
    const buffer = createGeneratedColumnBuffer(mockSchema as any, 10);

    (buffer as any).userId(0, 'alice');
    (buffer as any).userId(1, 'bob');
    (buffer as any).userId(2, 'alice'); // Duplicate value

    expect(buffer.userId_values[0]).toBe('alice');
    expect(buffer.userId_values[1]).toBe('bob');
    expect(buffer.userId_values[2]).toBe('alice');
  });
});
