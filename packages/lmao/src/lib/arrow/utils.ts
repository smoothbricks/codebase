/**
 * Utility functions for Arrow conversion
 */

import { countNulls } from '@smoothbricks/arrow-builder';
import type { SpanBuffer } from '../types.js';

/**
 * Encode an array of strings into a single UTF-8 Uint8Array
 */
export function encodeUtf8Strings(strings: readonly string[]): Uint8Array {
  const encoder = new TextEncoder();
  const encoded = strings.map((s) => encoder.encode(s));
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
  const encoder = new TextEncoder();
  const offsets = new Int32Array(strings.length + 1);
  offsets[0] = 0;
  for (let i = 0; i < strings.length; i++) {
    const byteLength = encoder.encode(strings[i]).length;
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
  buffers: SpanBuffer[],
  columnName: string,
): { nullBitmap: Uint8Array | undefined; nullCount: number } {
  const nullsName = `${columnName}_nulls` as const;
  const hasAnyNulls = buffers.some((buf) => buf[nullsName] !== undefined);

  if (!hasAnyNulls) return { nullBitmap: undefined, nullCount: 0 };

  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  const bitmapBytes = Math.ceil(totalRows / 8);
  const nullBitmap = new Uint8Array(bitmapBytes);
  nullBitmap.fill(0xff); // Default all valid

  // Buffer chains: all buffers except the last are full (writeIndex == capacity).
  // If capacity is a multiple of 8, each buffer starts at a byte boundary.
  let rowOffset = 0;
  let nullCount = 0;

  for (const buf of buffers) {
    const sourceBitmap = buf[nullsName];
    const rowCount = buf.writeIndex;

    if (sourceBitmap) {
      const byteOffset = rowOffset >>> 3; // rowOffset / 8
      const fullBytes = rowCount >>> 3;
      const remainingBits = rowCount & 7;

      // Bulk copy full bytes
      if (fullBytes > 0) {
        nullBitmap.set(sourceBitmap.subarray(0, fullBytes), byteOffset);
      }
      // Handle remaining bits in last partial byte
      if (remainingBits > 0) {
        const srcLastByte = sourceBitmap[fullBytes];
        const mask = (1 << remainingBits) - 1;
        nullBitmap[byteOffset + fullBytes] = (nullBitmap[byteOffset + fullBytes] & ~mask) | (srcLastByte & mask);
      }
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
  if (fieldName === 'logMessage') return 'message';
  return fieldName;
}

/**
 * Walk a SpanBuffer tree (including overflow chains and children)
 */
export function walkSpanTree(root: SpanBuffer, visitor: (buffer: SpanBuffer) => void): void {
  let current: SpanBuffer | undefined = root;
  while (current) {
    visitor(current);
    current = current.next as SpanBuffer | undefined;
  }
  for (const child of root.children) {
    walkSpanTree(child, visitor);
  }
}
