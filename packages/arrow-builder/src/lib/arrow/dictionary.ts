/**
 * DictionaryBuilder for Arrow string columns.
 *
 * Builds sorted Arrow dictionaries efficiently:
 * 1. Checks global interner for pre-encoded UTF-8 (enums, module names)
 * 2. Falls back to Utf8Encoder with 2nd-occurrence caching
 * 3. Determines index type (uint8/16/32) based on dictionary size
 */

import { getInterned } from './interner.js';
import type { SortedArray } from './sorted.js';
import { defaultUtf8Encoder, type Utf8Encoder, utf8ByteLength } from './utf8.js';

export interface FinalizedDictionary {
  /** Concatenated UTF-8 bytes */
  data: Uint8Array;
  /** Arrow-format offsets (length = uniqueCount + 1) */
  offsets: Int32Array;
  /** Map from string value to sorted index */
  indexMap: Map<string, number>;
  /** Index type based on dictionary size */
  indexType: 'uint8' | 'uint16' | 'uint32';
}

/**
 * Entry with pre-encoded UTF-8 bytes for direct dictionary creation.
 */
export interface PreEncodedEntry {
  str: string;
  utf8: Uint8Array;
}

/**
 * Create a finalized dictionary from pre-sorted, pre-encoded entries.
 * Use sortInPlace() to sort entries before calling this function.
 *
 * @param entries - SortedArray of {str, utf8} pairs (must be sorted and deduplicated)
 */
export function createSortedDictionary(entries: SortedArray<PreEncodedEntry>): FinalizedDictionary {
  const uniqueCount = entries.length;

  // Determine index type based on dictionary size
  const indexType: 'uint8' | 'uint16' | 'uint32' =
    uniqueCount <= 255 ? 'uint8' : uniqueCount <= 65535 ? 'uint16' : 'uint32';

  // Calculate total bytes needed
  let totalBytes = 0;
  for (const { utf8 } of entries) {
    totalBytes += utf8.length;
  }

  // Pre-allocate output buffers
  const data = new Uint8Array(totalBytes);
  const offsets = new Int32Array(uniqueCount + 1);
  const indexMap = new Map<string, number>();

  let offset = 0;
  for (let i = 0; i < entries.length; i++) {
    const { str, utf8 } = entries[i];
    offsets[i] = offset;
    indexMap.set(str, i);
    data.set(utf8, offset);
    offset += utf8.length;
  }
  offsets[uniqueCount] = offset;

  return { data, offsets, indexMap, indexType };
}

/**
 * Create a finalized dictionary from unsorted, pre-encoded entries.
 * For text columns that don't require sorted dictionaries.
 *
 * @param entries - Array of {str, utf8} pairs (assumed deduplicated)
 */
export function createDictionary(entries: readonly PreEncodedEntry[]): FinalizedDictionary {
  const uniqueCount = entries.length;

  // Determine index type based on dictionary size
  const indexType: 'uint8' | 'uint16' | 'uint32' =
    uniqueCount <= 255 ? 'uint8' : uniqueCount <= 65535 ? 'uint16' : 'uint32';

  // Calculate total bytes needed
  let totalBytes = 0;
  for (const { utf8 } of entries) {
    totalBytes += utf8.length;
  }

  // Pre-allocate output buffers
  const data = new Uint8Array(totalBytes);
  const offsets = new Int32Array(uniqueCount + 1);
  const indexMap = new Map<string, number>();

  let offset = 0;
  for (let i = 0; i < entries.length; i++) {
    const { str, utf8 } = entries[i];
    offsets[i] = offset;
    indexMap.set(str, i);
    data.set(utf8, offset);
    offset += utf8.length;
  }
  offsets[uniqueCount] = offset;

  return { data, offsets, indexMap, indexType };
}

interface TrackedEntry {
  /** Pre-encoded UTF-8 bytes (from interner or 2nd occurrence) */
  utf8?: Uint8Array;
  /** Byte length (always known) */
  byteLength: number;
  /** Occurrence count (for 2nd-occurrence caching) */
  count: number;
}

// Static encoder instance (stateless)
const encoder = new TextEncoder();

export class DictionaryBuilder {
  private entries = new Map<string, TrackedEntry>();
  private totalBytes = 0;
  private utf8Encoder: Utf8Encoder;

  constructor(utf8Encoder?: Utf8Encoder) {
    this.utf8Encoder = utf8Encoder ?? defaultUtf8Encoder;
  }

  /**
   * Add a string to the dictionary.
   * Checks interner first, then uses encoder with 2nd-occurrence caching.
   */
  add(str: string): void {
    const existing = this.entries.get(str);
    if (existing) {
      existing.count++;
      // Cache UTF-8 on 2nd occurrence if not already cached
      if (existing.count === 2 && !existing.utf8) {
        existing.utf8 = this.utf8Encoder.encode(str);
      }
      return;
    }

    // Check interner for pre-encoded UTF-8
    const interned = getInterned(str);
    if (interned) {
      this.entries.set(str, {
        utf8: interned,
        byteLength: interned.length,
        count: 1,
      });
      this.totalBytes += interned.length;
      return;
    }

    // First occurrence - just track byte length
    const byteLength = utf8ByteLength(str);
    this.entries.set(str, { byteLength, count: 1 });
    this.totalBytes += byteLength;
  }

  /**
   * Get number of unique strings.
   */
  get size(): number {
    return this.entries.size;
  }

  /**
   * Finalize dictionary into Arrow-compatible format.
   * @param sorted - if true, dictionary values are sorted alphabetically
   */
  finalize(sorted: boolean): FinalizedDictionary {
    const uniqueCount = this.entries.size;

    // Get strings, optionally sorted
    const strings = [...this.entries.keys()];
    if (sorted) strings.sort();

    // Determine index type based on dictionary size
    const indexType: 'uint8' | 'uint16' | 'uint32' =
      uniqueCount <= 255 ? 'uint8' : uniqueCount <= 65535 ? 'uint16' : 'uint32';

    // Pre-allocate output buffers
    const data = new Uint8Array(this.totalBytes);
    const offsets = new Int32Array(uniqueCount + 1);
    const indexMap = new Map<string, number>();

    let offset = 0;
    for (let i = 0; i < strings.length; i++) {
      const str = strings[i];
      const entry = this.entries.get(str);
      if (!entry) continue;

      offsets[i] = offset;
      indexMap.set(str, i);

      if (entry.utf8) {
        // Use cached/interned UTF-8
        data.set(entry.utf8, offset);
        offset += entry.utf8.length;
      } else {
        // Encode directly into output buffer
        const written = encoder.encodeInto(str, data.subarray(offset)).written ?? 0;
        offset += written;
      }
    }
    offsets[uniqueCount] = offset;

    return { data, offsets, indexMap, indexType };
  }

  /**
   * Clear the builder for reuse.
   */
  clear(): void {
    this.entries.clear();
    this.totalBytes = 0;
  }
}
