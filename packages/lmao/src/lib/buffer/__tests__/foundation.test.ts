import { describe, it, expect } from 'bun:test';
import { getCacheAlignedCapacity } from '../capacity.js';
import { createEmptySpanBuffer } from '../createSpanBuffer.js';

describe('Buffer Foundation', () => {
  it('computes cache-aligned capacities', () => {
    // 1 element of 1 byte -> 64-byte alignment
    expect(getCacheAlignedCapacity(1, 1)).toBe(64);

    // 16 elements of 1 byte -> still 64-byte aligned
    expect(getCacheAlignedCapacity(16, 1)).toBe(64);

    // 65 elements -> rounds up to next 64-byte boundary (128 bytes), capacity 128
    expect(getCacheAlignedCapacity(65, 1)).toBe(128);

    // Larger count around 130 -> 192 bytes, capacity 192
    expect(getCacheAlignedCapacity(130, 1)).toBe(192);
  });

  it('creates empty SpanBuffer with core columns and proper length', () => {
    const buf = createEmptySpanBuffer(1, 16, 4);

    expect(buf.spanId).toBe(1);
    // All core arrays should be the aligned length
    expect(buf.timestamps.length).toBe(64);
    expect(buf.operations.length).toBe(64);
    expect(buf.nullBitmap.length).toBe(64);

    // Metadata
    expect(buf.children).toBeInstanceOf(Array);
    expect(buf.writeIndex).toBe(0);
    expect(buf.capacity).toBe(16);
  });

  it('uses Uint16Array for moderate attribute counts', () => {
    const buf = createEmptySpanBuffer(2, 16, 9);
    // nullBitmap type should be Uint16Array when attributeCount <= 16
    expect(buf.nullBitmap).toBeInstanceOf(Uint16Array);
  });

  it('throws on too many attributes', () => {
    // attributeCount > 32 should throw
    expect(() => createEmptySpanBuffer(3, 8, 33)).toThrow();
  });
});
