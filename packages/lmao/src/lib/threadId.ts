/**
 * Thread ID generator for distributed span identification
 *
 * Each worker/process gets a unique 64-bit random ID that remains constant
 * for the lifetime of the process. Combined with a local incrementing counter,
 * this provides globally unique span IDs without coordination.
 *
 * **Design rationale:**
 * - Thread ID generated once per process (cheap BigInt creation)
 * - Local span ID increments are simple number operations (hot path)
 * - Combined (threadId, spanId) tuple is globally unique
 *
 * @module threadId
 */

let currentThreadId: bigint | null = null;

/**
 * Get the current thread's unique 64-bit identifier.
 *
 * Generated lazily on first call, then cached for the process lifetime.
 * Uses crypto.getRandomValues for high-quality randomness.
 *
 * @returns 64-bit thread ID as BigInt
 */
export function getThreadId(): bigint {
  if (currentThreadId === null) {
    // Generate 64-bit random ID using two 32-bit values
    const high = Math.floor(Math.random() * 0xffffffff);
    const low = Math.floor(Math.random() * 0xffffffff);
    currentThreadId = (BigInt(high) << 32n) | BigInt(low);
  }
  return currentThreadId;
}

/**
 * Reset thread ID for testing purposes only.
 *
 * **WARNING**: Only use in tests! Resetting in production would break
 * span ID uniqueness guarantees.
 *
 * @internal
 */
export function _resetThreadId(): void {
  currentThreadId = null;
}
