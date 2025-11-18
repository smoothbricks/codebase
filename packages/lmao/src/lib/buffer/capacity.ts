const CACHE_LINE_SIZE = 64; // bytes

export function getCacheAlignedCapacity(
  elementCount: number,
  bytesPerElement: number
): number {
  const totalBytes = elementCount * bytesPerElement;
  const alignedBytes = Math.ceil(totalBytes / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
  return Math.ceil(alignedBytes / bytesPerElement);
}
