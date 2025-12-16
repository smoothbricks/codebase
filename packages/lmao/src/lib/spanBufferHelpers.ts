/**
 * Helper utilities for generated SpanBuffer classes.
 *
 * These are injected into generated code via the `dependencies` mechanism
 * to avoid regenerating identical code for every schema.
 */

/**
 * Shared TextEncoder instance for traceId encoding.
 * TextEncoder is stateless and thread-safe, so one instance suffices.
 */
export const textEncoder = new TextEncoder();

/**
 * Write spanId bytes (4 bytes, little-endian) to identity buffer.
 *
 * @param identity - Target Uint8Array (identity bytes)
 * @param offset - Byte offset to write at
 * @param spanId - 32-bit span ID
 */
export function writeSpanId(identity: Uint8Array, offset: number, spanId: number): void {
  identity[offset] = spanId & 0xff;
  identity[offset + 1] = (spanId >> 8) & 0xff;
  identity[offset + 2] = (spanId >> 16) & 0xff;
  identity[offset + 3] = (spanId >> 24) & 0xff;
}

/**
 * Read spanId bytes (4 bytes, little-endian) from identity buffer.
 *
 * @param identity - Source Uint8Array (identity bytes)
 * @param offset - Byte offset to read from
 * @returns 32-bit span ID
 */
export function readSpanId(identity: Uint8Array, offset: number): number {
  return identity[offset] | (identity[offset + 1] << 8) | (identity[offset + 2] << 16) | (identity[offset + 3] << 24);
}

/**
 * Decode traceId string from identity buffer (root spans only).
 *
 * Format: [length byte][traceId bytes...]
 *
 * @param identity - Source Uint8Array (identity bytes)
 * @param offset - Byte offset where length byte starts
 * @returns Decoded traceId string
 */
export function decodeTraceId(identity: Uint8Array, offset: number): string {
  const len = identity[offset];
  let str = '';
  for (let i = 0; i < len; i++) {
    str += String.fromCharCode(identity[offset + 1 + i]);
  }
  return str;
}

/**
 * Helpers object to inject into generated SpanBuffer code.
 */
export const spanBufferHelpers = {
  textEncoder,
  writeSpanId,
  readSpanId,
  decodeTraceId,
} as const;

export type SpanBufferHelpers = typeof spanBufferHelpers;
