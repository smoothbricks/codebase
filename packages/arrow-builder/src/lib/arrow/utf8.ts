/**
 * UTF-8 encoding utilities for Arrow string columns.
 */

/**
 * Interface for UTF-8 encoding with optional caching.
 * DictionaryBuilder uses this as fallback for non-interned strings.
 */
export interface Utf8Encoder {
  /** Get UTF-8 byte length without encoding */
  byteLength(str: string): number;

  /** Encode string to UTF-8 */
  encode(str: string): Uint8Array;

  /** Encode directly into buffer at offset, returns bytes written */
  encodeInto(str: string, buffer: Uint8Array, offset: number): number;
}

const encoder = new TextEncoder();

/**
 * Calculate UTF-8 byte length without allocating.
 */
export function utf8ByteLength(str: string): number {
  let bytes = 0;
  for (let i = 0; i < str.length; i++) {
    const c = str.charCodeAt(i);
    if (c < 0x80) bytes += 1;
    else if (c < 0x800) bytes += 2;
    else if (c < 0xd800 || c >= 0xe000) bytes += 3;
    else {
      // Surrogate pair
      i++;
      bytes += 4;
    }
  }
  return bytes;
}

/**
 * Default UTF-8 encoder - no caching, just TextEncoder.
 */
export const defaultUtf8Encoder: Utf8Encoder = {
  byteLength: utf8ByteLength,

  encode(str: string): Uint8Array {
    return encoder.encode(str);
  },

  encodeInto(str: string, buffer: Uint8Array, offset: number): number {
    const result = encoder.encodeInto(str, buffer.subarray(offset));
    return result.written ?? 0;
  },
};
