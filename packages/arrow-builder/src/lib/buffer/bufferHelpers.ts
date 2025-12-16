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
 * ```
 */
export const bufferHelpers = {
  getAlignedCapacity,
} as const;

export type BufferHelpers = typeof bufferHelpers;
