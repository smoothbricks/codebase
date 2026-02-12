/**
 * Dictionary building for Arrow conversion
 */

import { type IntType, uint8, uint16, uint32 } from '@uwdata/flechette';
import type { AnySpanBuffer } from '../types.js';
import { concatenateNullBitmaps } from './utils.js';

/**
 * Dictionary build result with consistent shape for V8 optimization.
 * Using a class ensures all instances share the same hidden class.
 */
export class DictionaryBuildResult {
  constructor(
    public readonly dictionary: string[],
    public readonly indices: Uint8Array | Uint16Array | Uint32Array,
    public readonly indexArrayCtor: Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor,
    public readonly arrowIndexType: IntType,
    public readonly nullBitmap: Uint8Array | undefined,
    public readonly nullCount: number,
  ) {}
}

/**
 * Build a sorted category dictionary from buffers
 */
export function buildSortedCategoryDictionary(
  buffers: AnySpanBuffer[],
  columnName: string,
  maskTransform?: (value: string) => string,
): DictionaryBuildResult {
  const totalRows = buffers.reduce((sum, buf) => sum + buf._writeIndex, 0);

  // Build mapping from original value to masked value (for dictionary lookup)
  // and collect unique masked values for the dictionary
  const uniqueMaskedStrings = new Set<string>();
  const originalToMasked = new Map<string, string>();

  for (const buf of buffers) {
    const column = buf.getColumnIfAllocated(columnName);
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf._writeIndex; i++) {
        const value = column[i];
        if (value != null && !originalToMasked.has(value)) {
          const maskedValue = maskTransform ? maskTransform(value) : value;
          originalToMasked.set(value, maskedValue);
          uniqueMaskedStrings.add(maskedValue);
        }
      }
    }
  }

  const dictionary = Array.from(uniqueMaskedStrings).sort();
  const maskedToIndex = new Map(dictionary.map((s, i) => [s, i]));

  // Determine index type constructors based on dictionary size
  const uniqueCount = dictionary.length;
  const indexArrayCtor = uniqueCount <= 255 ? Uint8Array : uniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const arrowIndexType = uniqueCount <= 255 ? uint8() : uniqueCount <= 65535 ? uint16() : uint32();
  const indices = new indexArrayCtor(totalRows);
  const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

  let rowOffset = 0;
  for (const buf of buffers) {
    const column = buf.getColumnIfAllocated(columnName);
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf._writeIndex; i++) {
        const value = column[i];
        if (value != null) {
          const maskedValue = originalToMasked.get(value) ?? value;
          indices[rowOffset + i] = maskedToIndex.get(maskedValue) ?? 0;
        } else {
          indices[rowOffset + i] = 0;
        }
      }
    }
    rowOffset += buf._writeIndex;
  }

  return new DictionaryBuildResult(dictionary, indices, indexArrayCtor, arrowIndexType, nullBitmap, nullCount);
}

/**
 * Build a text dictionary from buffers (not sorted, uses frequency-based optimization)
 */
export function buildTextDictionary(
  buffers: AnySpanBuffer[],
  columnName: string,
  maskTransform?: (value: string) => string,
): DictionaryBuildResult | null {
  const totalRows = buffers.reduce((sum, buf) => sum + buf._writeIndex, 0);

  // Build mapping from original value to masked value and track frequency of masked values
  const frequencyMap = new Map<string, number>();
  const originalToMasked = new Map<string, string>();

  for (const buf of buffers) {
    const column = buf.getColumnIfAllocated(columnName);
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf._writeIndex; i++) {
        const value = column[i];
        if (value != null) {
          let maskedValue: string;
          if (originalToMasked.has(value)) {
            const masked = originalToMasked.get(value);
            if (masked === undefined) {
              throw new Error(`Masked value not found for: ${value}`);
            }
            maskedValue = masked;
          } else {
            maskedValue = maskTransform ? maskTransform(value) : value;
            originalToMasked.set(value, maskedValue);
          }
          frequencyMap.set(maskedValue, (frequencyMap.get(maskedValue) ?? 0) + 1);
        }
      }
    }
  }

  if (frequencyMap.size === 0) {
    return new DictionaryBuildResult([], new Uint32Array(totalRows), Uint32Array, uint32(), undefined, totalRows);
  }

  const dictionary = Array.from(frequencyMap.keys());
  const maskedToIndex = new Map(dictionary.map((s, i) => [s, i]));

  // Determine index type constructors based on dictionary size
  const uniqueCount = dictionary.length;
  const indexArrayCtor = uniqueCount <= 255 ? Uint8Array : uniqueCount <= 65535 ? Uint16Array : Uint32Array;
  const arrowIndexType = uniqueCount <= 255 ? uint8() : uniqueCount <= 65535 ? uint16() : uint32();
  const indices = new indexArrayCtor(totalRows);
  const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, columnName);

  let rowOffset = 0;
  for (const buf of buffers) {
    const column = buf.getColumnIfAllocated(columnName);
    if (column && Array.isArray(column)) {
      for (let i = 0; i < buf._writeIndex; i++) {
        const value = column[i];
        if (value != null) {
          const maskedValue = originalToMasked.get(value) ?? value;
          indices[rowOffset + i] = maskedToIndex.get(maskedValue) ?? 0;
        } else {
          indices[rowOffset + i] = 0;
        }
      }
    }
    rowOffset += buf._writeIndex;
  }

  return new DictionaryBuildResult(dictionary, indices, indexArrayCtor, arrowIndexType, nullBitmap, nullCount);
}
