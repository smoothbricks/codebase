/**
 * Unit tests for buffer helper functions
 */

import { describe, expect, it } from 'bun:test';
import { fillBooleanBitmapRange, fillNullBitmapRange, getAlignedCapacity, setNullBit } from '../bufferHelpers.js';

describe('getAlignedCapacity', () => {
  it('should return same value for multiples of 8', () => {
    expect(getAlignedCapacity(8)).toBe(8);
    expect(getAlignedCapacity(16)).toBe(16);
    expect(getAlignedCapacity(64)).toBe(64);
  });

  it('should round up non-multiples of 8', () => {
    expect(getAlignedCapacity(1)).toBe(8);
    expect(getAlignedCapacity(7)).toBe(8);
    expect(getAlignedCapacity(9)).toBe(16);
    expect(getAlignedCapacity(15)).toBe(16);
    expect(getAlignedCapacity(17)).toBe(24);
  });

  it('should handle zero', () => {
    expect(getAlignedCapacity(0)).toBe(0);
  });
});

describe('setNullBit', () => {
  it('should set bit 0 in byte 0', () => {
    const bitmap = new Uint8Array(1);
    setNullBit(bitmap, 0);
    expect(bitmap[0]).toBe(0b00000001);
  });

  it('should set bit 7 in byte 0', () => {
    const bitmap = new Uint8Array(1);
    setNullBit(bitmap, 7);
    expect(bitmap[0]).toBe(0b10000000);
  });

  it('should set bit 8 in byte 1', () => {
    const bitmap = new Uint8Array(2);
    setNullBit(bitmap, 8);
    expect(bitmap[0]).toBe(0);
    expect(bitmap[1]).toBe(0b00000001);
  });

  it('should set bit 15 in byte 1', () => {
    const bitmap = new Uint8Array(2);
    setNullBit(bitmap, 15);
    expect(bitmap[0]).toBe(0);
    expect(bitmap[1]).toBe(0b10000000);
  });

  it('should preserve existing bits', () => {
    const bitmap = new Uint8Array(1);
    bitmap[0] = 0b00001010; // bits 1 and 3 already set
    setNullBit(bitmap, 2);
    expect(bitmap[0]).toBe(0b00001110); // bits 1, 2, 3 set
  });
});

describe('fillNullBitmapRange', () => {
  /**
   * Helper to check which bits are set
   */
  function getBitsSet(bitmap: Uint8Array, count: number): boolean[] {
    const result: boolean[] = [];
    for (let i = 0; i < count; i++) {
      const byteIndex = i >>> 3;
      const bitOffset = i & 7;
      result.push((bitmap[byteIndex] & (1 << bitOffset)) !== 0);
    }
    return result;
  }

  it('should do nothing for empty range', () => {
    const bitmap = new Uint8Array(1);
    fillNullBitmapRange(bitmap, 0, 0);
    expect(bitmap[0]).toBe(0);

    fillNullBitmapRange(bitmap, 5, 5);
    expect(bitmap[0]).toBe(0);

    fillNullBitmapRange(bitmap, 5, 3); // endIdx < startIdx
    expect(bitmap[0]).toBe(0);
  });

  it('should fill single bit (indices 0-1)', () => {
    const bitmap = new Uint8Array(1);
    fillNullBitmapRange(bitmap, 0, 1);
    expect(bitmap[0]).toBe(0b00000001);
  });

  it('should fill full byte (indices 0-8)', () => {
    const bitmap = new Uint8Array(1);
    fillNullBitmapRange(bitmap, 0, 8);
    expect(bitmap[0]).toBe(0xff);
  });

  it('should fill partial byte from start (indices 0-5)', () => {
    const bitmap = new Uint8Array(1);
    fillNullBitmapRange(bitmap, 0, 5);
    // Bits 0-4 should be set: 0b00011111
    expect(bitmap[0]).toBe(0b00011111);
  });

  it('should fill partial byte from middle (indices 3-7)', () => {
    const bitmap = new Uint8Array(1);
    fillNullBitmapRange(bitmap, 3, 7);
    // Bits 3-6 should be set: 0b01111000
    expect(bitmap[0]).toBe(0b01111000);
  });

  it('BUG FIX: should fill within single byte when start > 0 (indices 2-6)', () => {
    // This is the critical edge case that was buggy before
    const bitmap = new Uint8Array(1);
    fillNullBitmapRange(bitmap, 2, 6);
    // Bits 2-5 should be set: 0b00111100
    const bits = getBitsSet(bitmap, 8);
    expect(bits[0]).toBe(false);
    expect(bits[1]).toBe(false);
    expect(bits[2]).toBe(true);
    expect(bits[3]).toBe(true);
    expect(bits[4]).toBe(true);
    expect(bits[5]).toBe(true);
    expect(bits[6]).toBe(false);
    expect(bits[7]).toBe(false);
  });

  it('should fill across byte boundary (indices 5-12)', () => {
    const bitmap = new Uint8Array(2);
    fillNullBitmapRange(bitmap, 5, 12);
    const bits = getBitsSet(bitmap, 16);
    // Bits 0-4 should NOT be set
    for (let i = 0; i < 5; i++) {
      expect(bits[i]).toBe(false);
    }
    // Bits 5-11 should be set
    for (let i = 5; i < 12; i++) {
      expect(bits[i]).toBe(true);
    }
    // Bits 12-15 should NOT be set
    for (let i = 12; i < 16; i++) {
      expect(bits[i]).toBe(false);
    }
  });

  it('should fill multiple full bytes (indices 0-24)', () => {
    const bitmap = new Uint8Array(3);
    fillNullBitmapRange(bitmap, 0, 24);
    expect(bitmap[0]).toBe(0xff);
    expect(bitmap[1]).toBe(0xff);
    expect(bitmap[2]).toBe(0xff);
  });

  it('should fill with partial start and end (indices 3-21)', () => {
    const bitmap = new Uint8Array(3);
    fillNullBitmapRange(bitmap, 3, 21);
    const bits = getBitsSet(bitmap, 24);
    // Bits 0-2 should NOT be set
    for (let i = 0; i < 3; i++) {
      expect(bits[i]).toBe(false);
    }
    // Bits 3-20 should be set
    for (let i = 3; i < 21; i++) {
      expect(bits[i]).toBe(true);
    }
    // Bits 21-23 should NOT be set
    for (let i = 21; i < 24; i++) {
      expect(bits[i]).toBe(false);
    }
  });

  it('should preserve existing bits', () => {
    const bitmap = new Uint8Array(1);
    bitmap[0] = 0b00000001; // bit 0 already set
    fillNullBitmapRange(bitmap, 4, 7);
    // Should have bits 0, 4, 5, 6 set
    expect(bitmap[0]).toBe(0b01110001);
  });
});

describe('fillBooleanBitmapRange', () => {
  function getBitsSet(bitmap: Uint8Array, count: number): boolean[] {
    const result: boolean[] = [];
    for (let i = 0; i < count; i++) {
      const byteIndex = i >>> 3;
      const bitOffset = i & 7;
      result.push((bitmap[byteIndex] & (1 << bitOffset)) !== 0);
    }
    return result;
  }

  it('should do nothing for empty range', () => {
    const bitmap = new Uint8Array(1);
    fillBooleanBitmapRange(bitmap, 0, 0, true);
    expect(bitmap[0]).toBe(0);
  });

  it('should set bits when value is true (indices 0-8)', () => {
    const bitmap = new Uint8Array(1);
    fillBooleanBitmapRange(bitmap, 0, 8, true);
    expect(bitmap[0]).toBe(0xff);
  });

  it('should clear bits when value is false (indices 0-8)', () => {
    const bitmap = new Uint8Array(1);
    bitmap[0] = 0xff; // all bits set
    fillBooleanBitmapRange(bitmap, 0, 8, false);
    expect(bitmap[0]).toBe(0x00);
  });

  it('should set partial range when value is true (indices 2-6)', () => {
    const bitmap = new Uint8Array(1);
    fillBooleanBitmapRange(bitmap, 2, 6, true);
    const bits = getBitsSet(bitmap, 8);
    expect(bits[0]).toBe(false);
    expect(bits[1]).toBe(false);
    expect(bits[2]).toBe(true);
    expect(bits[3]).toBe(true);
    expect(bits[4]).toBe(true);
    expect(bits[5]).toBe(true);
    expect(bits[6]).toBe(false);
    expect(bits[7]).toBe(false);
  });

  it('should clear partial range when value is false (indices 2-6)', () => {
    const bitmap = new Uint8Array(1);
    bitmap[0] = 0xff; // all bits set
    fillBooleanBitmapRange(bitmap, 2, 6, false);
    const bits = getBitsSet(bitmap, 8);
    expect(bits[0]).toBe(true);
    expect(bits[1]).toBe(true);
    expect(bits[2]).toBe(false);
    expect(bits[3]).toBe(false);
    expect(bits[4]).toBe(false);
    expect(bits[5]).toBe(false);
    expect(bits[6]).toBe(true);
    expect(bits[7]).toBe(true);
  });

  it('should fill across byte boundary (indices 5-12)', () => {
    const bitmap = new Uint8Array(2);
    fillBooleanBitmapRange(bitmap, 5, 12, true);
    const bits = getBitsSet(bitmap, 16);
    for (let i = 0; i < 5; i++) expect(bits[i]).toBe(false);
    for (let i = 5; i < 12; i++) expect(bits[i]).toBe(true);
    for (let i = 12; i < 16; i++) expect(bits[i]).toBe(false);
  });

  it('should clear across byte boundary (indices 5-12)', () => {
    const bitmap = new Uint8Array(2);
    bitmap[0] = 0xff;
    bitmap[1] = 0xff;
    fillBooleanBitmapRange(bitmap, 5, 12, false);
    const bits = getBitsSet(bitmap, 16);
    for (let i = 0; i < 5; i++) expect(bits[i]).toBe(true);
    for (let i = 5; i < 12; i++) expect(bits[i]).toBe(false);
    for (let i = 12; i < 16; i++) expect(bits[i]).toBe(true);
  });

  it('should fill multiple full bytes (indices 0-24)', () => {
    const bitmap = new Uint8Array(3);
    fillBooleanBitmapRange(bitmap, 0, 24, true);
    expect(bitmap[0]).toBe(0xff);
    expect(bitmap[1]).toBe(0xff);
    expect(bitmap[2]).toBe(0xff);
  });
});
