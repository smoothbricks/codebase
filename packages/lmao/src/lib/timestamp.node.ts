import { Nanoseconds } from '@smoothbricks/arrow-builder';

/**
 * Node.js timestamp implementation.
 *
 * Uses `process.hrtime.bigint()` anchored to epoch for true nanosecond precision.
 *
 * **How it works:**
 * - On module load: capture `Date.now()` and `process.hrtime.bigint()` together
 * - For each call: add hrtime delta to the epoch anchor
 * - Result: absolute epoch time with nanosecond precision
 *
 * **Why anchor once?**
 * - `Date.now()` has millisecond precision and can drift with NTP
 * - `process.hrtime.bigint()` has nanosecond precision but is monotonic (not epoch)
 * - Anchoring once gives both absolute epoch time AND nanosecond precision
 */

// Anchor point: captured once at module load
// Capture hrtime first, then Date.now() - this way Date.now() is the most recent reading
const anchorHrtime = process.hrtime.bigint();
const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;

/**
 * Get current timestamp in nanoseconds since Unix epoch.
 * True nanosecond precision via `process.hrtime.bigint()`.
 */
export function getTimestampNanos(): Nanoseconds {
  return Nanoseconds.unsafe(anchorEpochNanos + (process.hrtime.bigint() - anchorHrtime));
}

// Set Nanoseconds.now to our implementation
Nanoseconds.now = getTimestampNanos;

export { Nanoseconds };
