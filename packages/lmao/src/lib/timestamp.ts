import { Nanoseconds } from '@smoothbricks/arrow-builder';

/**
 * Browser timestamp implementation.
 *
 * Uses `performance.timeOrigin + performance.now()` for microsecond precision (~5-20μs).
 * Last 3 digits of nanoseconds are always 000.
 *
 * For Node.js with true nanosecond precision, import from `@smoothbricks/lmao/node`.
 */
export function getTimestampNanos(): Nanoseconds {
  // performance.timeOrigin + performance.now() = epoch milliseconds with sub-ms precision
  // Multiply by 1000 for microseconds (≈1.7e15, safe for Number.MAX_SAFE_INTEGER ≈9e15)
  // Math.floor because BigInt() throws on non-integers
  // Multiply by 1000n for nanoseconds (last 3 digits = 000)
  const epochMicros = Math.floor((performance.timeOrigin + performance.now()) * 1000);
  return (BigInt(epochMicros) * 1000n) as Nanoseconds;
}

// Set Nanoseconds.now to our implementation
Nanoseconds.now = getTimestampNanos;

export { Nanoseconds };
