/**
 * Scope value pre-fill utilities for columnar buffers.
 *
 * These are used by SpanLogger to bake scope values into columns
 * at allocation time, avoiding the need for scope filling during
 * Arrow conversion.
 */

/**
 * Fill null slots (where nullBitmap bit is 0) with a value.
 * Uses byte-wise null bitmap checks for fast paths - if entire byte is 0x00,
 * use TypedArray.fill() for that 8-element range.
 *
 * @param values - Column values array (TypedArray or string[])
 * @param nullBitmap - Null bitmap (Arrow format: 1 = valid, 0 = null)
 * @param value - Value to fill into null slots
 * @param startIndex - Start index (inclusive)
 * @param endIndex - End index (exclusive)
 */
export function fillNullSlots(
  values: Float64Array | Uint8Array | Uint16Array | Uint32Array | Int8Array | string[],
  nullBitmap: Uint8Array,
  value: number | string,
  startIndex: number,
  endIndex: number,
): void {
  // Implementation: check byte-wise for fast paths
  // If byte is 0x00 (all 8 bits null), use fill() for that range
  // Otherwise check bit-by-bit

  for (let i = startIndex; i < endIndex; i++) {
    const byteIdx = i >>> 3;
    const bitIdx = i & 7;
    const isNull = (nullBitmap[byteIdx] & (1 << bitIdx)) === 0;
    if (isNull) {
      (values as (number | string)[])[i] = value;
      nullBitmap[byteIdx] |= 1 << bitIdx; // Mark as valid
    }
  }
}

/**
 * Fill a range with a value and mark all as valid in null bitmap.
 * Used when pre-filling new/overflow buffers with scope values.
 *
 * @param values - Column values array
 * @param nullBitmap - Null bitmap to update
 * @param value - Value to fill
 * @param startIndex - Start index (inclusive)
 * @param endIndex - End index (exclusive)
 */
export function fillRangeWithValue(
  values: Float64Array | Uint8Array | Uint16Array | Uint32Array | Int8Array | string[],
  nullBitmap: Uint8Array,
  value: number | string,
  startIndex: number,
  endIndex: number,
): void {
  // For TypedArrays: use fill()
  // For string arrays: loop
  if (Array.isArray(values)) {
    for (let i = startIndex; i < endIndex; i++) {
      values[i] = value as string;
    }
  } else {
    (values as Float64Array | Uint8Array | Uint16Array | Uint32Array | Int8Array).fill(
      value as number,
      startIndex,
      endIndex,
    );
  }

  // Mark range as valid in nullBitmap
  for (let i = startIndex; i < endIndex; i++) {
    const byteIdx = i >>> 3;
    const bitIdx = i & 7;
    nullBitmap[byteIdx] |= 1 << bitIdx;
  }
}

/**
 * Fill boolean bitmap range with a boolean value.
 * Handles bit-packed boolean storage.
 */
export function fillBooleanRange(
  boolBitmap: Uint8Array,
  nullBitmap: Uint8Array,
  value: boolean,
  startIndex: number,
  endIndex: number,
): void {
  for (let i = startIndex; i < endIndex; i++) {
    const byteIdx = i >>> 3;
    const bitIdx = i & 7;
    if (value) {
      boolBitmap[byteIdx] |= 1 << bitIdx;
    } else {
      boolBitmap[byteIdx] &= ~(1 << bitIdx);
    }
    nullBitmap[byteIdx] |= 1 << bitIdx; // Mark as valid
  }
}
