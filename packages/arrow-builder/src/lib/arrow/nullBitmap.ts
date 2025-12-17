/**
 * Null bitmap utilities for Arrow-compatible columnar storage.
 *
 * Arrow format: bit N is in byte floor(N/8), LSB ordering.
 * - 1 = valid (not null)
 * - 0 = null
 */

/**
 * Count null values in a bitmap for the first `length` elements.
 */
export function countNulls(nullBitmap: Uint8Array, length: number): number {
  let count = 0;
  for (let i = 0; i < length; i++) {
    const byteIdx = i >>> 3;
    const bitIdx = i & 7;
    if ((nullBitmap[byteIdx] & (1 << bitIdx)) === 0) {
      count++;
    }
  }
  return count;
}

/**
 * Check if a specific index is valid (not null) in a bitmap.
 */
export function isValid(nullBitmap: Uint8Array, index: number): boolean {
  const byteIdx = index >>> 3;
  const bitIdx = index & 7;
  return (nullBitmap[byteIdx] & (1 << bitIdx)) !== 0;
}

/**
 * Set a bit to 1 (mark as valid/not null).
 */
export function setBit(bitmap: Uint8Array, index: number): void {
  bitmap[index >>> 3] |= 1 << (index & 7);
}

/**
 * Clear a bit to 0 (mark as null).
 */
export function clearBit(bitmap: Uint8Array, index: number): void {
  bitmap[index >>> 3] &= ~(1 << (index & 7));
}

/**
 * Clear a range of bits (set them to 0 = null).
 *
 * PRECONDITION: startBit should be byte-aligned (startBit % 8 === 0)
 * for optimal performance. Works correctly for non-aligned starts but
 * uses slower bit-by-bit operations for the first partial byte.
 */
export function clearBitRange(bitmap: Uint8Array, startBit: number, count: number): void {
  if (count === 0) return;

  const startByte = startBit >>> 3;
  const startBitOffset = startBit & 7;

  if (startBitOffset === 0) {
    // Byte-aligned start - fast path
    const fullBytes = count >>> 3;
    const remainingBits = count & 7;

    if (fullBytes > 0) {
      bitmap.fill(0, startByte, startByte + fullBytes);
    }

    if (remainingBits > 0) {
      const mask = (1 << remainingBits) - 1;
      bitmap[startByte + fullBytes] &= ~mask;
    }
  } else {
    // Non-aligned start - slower path
    for (let i = 0; i < count; i++) {
      clearBit(bitmap, startBit + i);
    }
  }
}

/**
 * Set a range of bits (set them to 1 = valid).
 *
 * PRECONDITION: startBit should be byte-aligned for optimal performance.
 */
export function setBitRange(bitmap: Uint8Array, startBit: number, count: number): void {
  if (count === 0) return;

  const startByte = startBit >>> 3;
  const startBitOffset = startBit & 7;

  if (startBitOffset === 0) {
    // Byte-aligned start - fast path
    const fullBytes = count >>> 3;
    const remainingBits = count & 7;

    if (fullBytes > 0) {
      bitmap.fill(0xff, startByte, startByte + fullBytes);
    }

    if (remainingBits > 0) {
      const mask = (1 << remainingBits) - 1;
      bitmap[startByte + fullBytes] |= mask;
    }
  } else {
    // Non-aligned start - slower path
    for (let i = 0; i < count; i++) {
      setBit(bitmap, startBit + i);
    }
  }
}

/**
 * Copy bits from source to destination bitmap.
 *
 * Both bitmaps use Arrow format (LSB ordering).
 * Handles byte-aligned copies efficiently with bulk operations.
 */
export function copyBits(
  dest: Uint8Array,
  destOffset: number,
  src: Uint8Array,
  srcOffset: number,
  count: number,
): void {
  if (count === 0) return;

  // Fast path: both offsets are byte-aligned
  if ((destOffset & 7) === 0 && (srcOffset & 7) === 0) {
    const destByte = destOffset >>> 3;
    const srcByte = srcOffset >>> 3;
    const fullBytes = count >>> 3;
    const remainingBits = count & 7;

    if (fullBytes > 0) {
      dest.set(src.subarray(srcByte, srcByte + fullBytes), destByte);
    }

    if (remainingBits > 0) {
      const mask = (1 << remainingBits) - 1;
      const srcLastByte = src[srcByte + fullBytes];
      dest[destByte + fullBytes] = (dest[destByte + fullBytes] & ~mask) | (srcLastByte & mask);
    }
  } else {
    // Slow path: bit-by-bit copy
    for (let i = 0; i < count; i++) {
      if (isValid(src, srcOffset + i)) {
        setBit(dest, destOffset + i);
      } else {
        clearBit(dest, destOffset + i);
      }
    }
  }
}

/**
 * Create a new null bitmap with all bits set to valid (1).
 */
export function createValidBitmap(length: number): Uint8Array {
  const bytes = Math.ceil(length / 8);
  const bitmap = new Uint8Array(bytes);
  bitmap.fill(0xff);
  return bitmap;
}

/**
 * Create a new null bitmap with all bits set to null (0).
 */
export function createNullBitmap(length: number): Uint8Array {
  return new Uint8Array(Math.ceil(length / 8));
}
