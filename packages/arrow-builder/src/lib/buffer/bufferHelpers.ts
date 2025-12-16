/**
 * Helper utilities for generated ColumnBuffer classes.
 *
 * These functions are defined once and injected into generated code via
 * the `new Function()` dependency injection mechanism. This avoids:
 * - Regenerating identical code for every schema
 * - Bloating generated code with docstrings
 * - Duplicating logic across generated classes
 */

/**
 * Aligns capacity to a multiple of 8 elements.
 *
 * PRIMARY reason: Null bitmaps use 1 bit per element. Multiple of 8 ensures:
 * - Each buffer's null bitmap ends on a byte boundary
 * - When concatenating buffers for Arrow conversion, bitmaps can be bulk-copied
 * - No bit-shifting needed when merging null bitmaps across buffer chains
 *
 * BONUS: Also provides 64-byte cache line alignment for BigInt64Array
 * (8 elements × 8 bytes = 64 bytes per cache line)
 *
 * @param elementCount - Requested number of elements
 * @returns Capacity rounded up to nearest multiple of 8
 */
export function getAlignedCapacity(elementCount: number): number {
  return Math.ceil(elementCount / 8) * 8;
}

/**
 * Set a single bit in a null bitmap (mark index as non-null).
 *
 * Arrow format: bit N is in byte floor(N/8), LSB ordering (bit 0 is least significant).
 *
 * @param bitmap - Null bitmap Uint8Array
 * @param idx - Index to mark as non-null
 */
export function setNullBit(bitmap: Uint8Array, idx: number): void {
  bitmap[idx >>> 3] |= 1 << (idx & 7);
}

/**
 * Fill a range of bits in a null bitmap (bulk mark indices as non-null).
 *
 * Sets bits [startIdx, endIdx) - exclusive end, like TypedArray.fill().
 * Arrow format: bit N is in byte floor(N/8), LSB ordering.
 *
 * Handles partial bytes at start/end correctly, including the edge case
 * where the entire range fits in a single byte.
 *
 * @param bitmap - Null bitmap Uint8Array
 * @param startIdx - First index to mark as non-null (inclusive)
 * @param endIdx - End index (exclusive)
 */
export function fillNullBitmapRange(bitmap: Uint8Array, startIdx: number, endIdx: number): void {
  if (startIdx >= endIdx) return;

  const startByte = startIdx >>> 3;
  const endByte = (endIdx - 1) >>> 3;

  if (startByte === endByte) {
    // Entire range fits in a single byte - mask both ends
    const startMask = 0xff << (startIdx & 7);
    const endMask = (endIdx & 7) === 0 ? 0xff : (1 << (endIdx & 7)) - 1;
    bitmap[startByte] |= startMask & endMask;
    return;
  }

  // Partial first byte (if not byte-aligned)
  if ((startIdx & 7) !== 0) {
    bitmap[startByte] |= 0xff << (startIdx & 7);
  } else {
    bitmap[startByte] = 0xff;
  }

  // Full bytes in the middle - use fill() for SIMD optimization
  if (endByte > startByte + 1) {
    bitmap.fill(0xff, startByte + 1, endByte);
  }

  // Partial last byte (if not byte-aligned)
  if ((endIdx & 7) !== 0) {
    bitmap[endByte] |= (1 << (endIdx & 7)) - 1;
  } else {
    bitmap[endByte] = 0xff;
  }
}

/**
 * Fill a range of bits in a boolean values bitmap with a specific value.
 *
 * Sets or clears bits [startIdx, endIdx) based on the value parameter.
 * Arrow format: bit N is in byte floor(N/8), LSB ordering.
 *
 * @param bitmap - Boolean values Uint8Array (bit-packed)
 * @param startIdx - First index (inclusive)
 * @param endIdx - End index (exclusive)
 * @param value - Boolean value to fill (true = set bits, false = clear bits)
 */
export function fillBooleanBitmapRange(bitmap: Uint8Array, startIdx: number, endIdx: number, value: boolean): void {
  if (startIdx >= endIdx) return;

  const startByte = startIdx >>> 3;
  const endByte = (endIdx - 1) >>> 3;
  const fillByte = value ? 0xff : 0x00;

  if (startByte === endByte) {
    // Entire range fits in a single byte - mask both ends
    const startMask = 0xff << (startIdx & 7);
    const endMask = (endIdx & 7) === 0 ? 0xff : (1 << (endIdx & 7)) - 1;
    const mask = startMask & endMask;
    if (value) {
      bitmap[startByte] |= mask;
    } else {
      bitmap[startByte] &= ~mask;
    }
    return;
  }

  // Partial first byte
  if ((startIdx & 7) !== 0) {
    const mask = 0xff << (startIdx & 7);
    if (value) {
      bitmap[startByte] |= mask;
    } else {
      bitmap[startByte] &= ~mask;
    }
  } else {
    bitmap[startByte] = fillByte;
  }

  // Full bytes in the middle - use fill() for SIMD optimization
  if (endByte > startByte + 1) {
    bitmap.fill(fillByte, startByte + 1, endByte);
  }

  // Partial last byte
  if ((endIdx & 7) !== 0) {
    const mask = (1 << (endIdx & 7)) - 1;
    if (value) {
      bitmap[endByte] |= mask;
    } else {
      bitmap[endByte] &= ~mask;
    }
  } else {
    bitmap[endByte] = fillByte;
  }
}

/**
 * Helpers object to inject into generated ColumnBuffer code.
 *
 * Usage in columnBufferGenerator.ts:
 * ```typescript
 * const factory = new Function('helpers', `return ${classCode}`);
 * const BufferClass = factory(bufferHelpers);
 * ```
 *
 * Then in generated code:
 * ```javascript
 * const alignedCapacity = helpers.getAlignedCapacity(requestedCapacity);
 * helpers.setNullBit(nullBitmap, idx);
 * helpers.fillNullBitmapRange(nullBitmap, startIdx, endIdx);
 * helpers.fillBooleanBitmapRange(valuesBitmap, startIdx, endIdx, true);
 * ```
 */
export const bufferHelpers = {
  getAlignedCapacity,
  setNullBit,
  fillNullBitmapRange,
  fillBooleanBitmapRange,
} as const;

export type BufferHelpers = typeof bufferHelpers;
