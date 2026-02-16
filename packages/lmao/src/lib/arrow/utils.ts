/**
 * Utility functions for Arrow conversion
 */

import { copyBits, countNulls } from '@smoothbricks/arrow-builder';
import type { AnySpanBuffer } from '../types.js';

const UTF8_ENCODER = new TextEncoder();

/**
 * Encode an array of strings into a single UTF-8 Uint8Array
 */
export function encodeUtf8Strings(strings: readonly string[]): Uint8Array {
  const encoded = strings.map((s) => UTF8_ENCODER.encode(s));
  const totalLength = encoded.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of encoded) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

/**
 * Calculate UTF-8 byte offsets for an array of strings
 */
export function calculateUtf8Offsets(strings: readonly string[]): Int32Array {
  const offsets = new Int32Array(strings.length + 1);
  offsets[0] = 0;
  for (let i = 0; i < strings.length; i++) {
    const byteLength = UTF8_ENCODER.encode(strings[i]).length;
    offsets[i + 1] = offsets[i] + byteLength;
  }
  return offsets;
}

/**
 * Concatenate Uint8 arrays without type casting.
 */
export function concatenateUint8Arrays(arrays: Uint8Array[]): Uint8Array {
  if (arrays.length === 0) throw new Error('Cannot concatenate empty array list');
  if (arrays.length === 1) return arrays[0];
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

/**
 * Concatenate Float64 arrays without type casting.
 */
export function concatenateFloat64Arrays(arrays: Float64Array[]): Float64Array {
  if (arrays.length === 0) throw new Error('Cannot concatenate empty array list');
  if (arrays.length === 1) return arrays[0];
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Float64Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

/**
 * Concatenate null bitmaps from multiple buffers
 */
export function concatenateNullBitmaps(
  buffers: AnySpanBuffer[],
  columnName: string,
): { nullBitmap: Uint8Array | undefined; nullCount: number } {
  const hasAnyNulls = buffers.some((buf) => buf.getNullsIfAllocated(columnName) !== undefined);

  if (!hasAnyNulls) return { nullBitmap: undefined, nullCount: 0 };

  const totalRows = buffers.reduce((sum, buf) => sum + buf._writeIndex, 0);
  const bitmapBytes = Math.ceil(totalRows / 8);
  const nullBitmap = new Uint8Array(bitmapBytes);
  nullBitmap.fill(0xff); // Default all valid

  // Buffer chains: all buffers except the last are full (writeIndex == capacity).
  // If capacity is a multiple of 8, each buffer starts at a byte boundary.
  let rowOffset = 0;
  let nullCount = 0;

  for (const buf of buffers) {
    const sourceBitmap = buf.getNullsIfAllocated(columnName);
    const rowCount = buf._writeIndex;

    if (sourceBitmap) {
      copyBits(nullBitmap, rowOffset, sourceBitmap, 0, rowCount);
      // Count nulls using countNulls from arrow-builder
      nullCount += countNulls(sourceBitmap, rowCount);
    }
    // If no sourceBitmap, leave as 0xff (all valid)

    rowOffset += rowCount;
  }

  return { nullBitmap, nullCount };
}

/**
 * Get Arrow field name from LMAO field name
 */
export function getArrowFieldName(fieldName: string): string {
  return fieldName;
}

/**
 * Walk a SpanBuffer tree (including overflow chains and children)
 */
export function walkSpanTree(root: AnySpanBuffer, visitor: (buffer: AnySpanBuffer) => void): void {
  let current: AnySpanBuffer | undefined = root;
  while (current) {
    visitor(current);
    current = current._overflow;
  }
  for (const child of root._children) {
    walkSpanTree(child, visitor);
  }
}
