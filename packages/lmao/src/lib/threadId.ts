/**
 * Thread ID - Process-level unique identifier for distributed span identification
 *
 * Each worker/process gets a unique 64-bit random ID that remains constant
 * for the lifetime of the process. Combined with a local incrementing counter,
 * this provides globally unique span IDs without coordination.
 *
 * The thread ID is stored as raw bytes (Uint8Array) to enable zero-copy
 * writes to SpanBuffer identity sections.
 *
 * ## Module-level = Per-Process/Worker Singleton
 *
 * ES modules are instantiated once per JavaScript realm:
 * - **Node.js main process**: One thread ID per process
 * - **Node.js worker_threads**: Each Worker gets its own module instance → own thread ID
 * - **Browser main thread**: One thread ID
 * - **Web Workers**: Each Worker gets its own module instance → own thread ID
 *
 * This is exactly what we want: each "thread of execution" automatically gets
 * a unique ID without any explicit thread-local storage API.
 *
 * @module threadId
 */

/** Thread ID size in bytes */
export const THREAD_ID_BYTES = 8;

/** Process-level thread ID bytes (lazily initialized) */
let threadIdBytes: Uint8Array | null = null;

/** Cached BigInt representation (lazily computed) */
let threadIdBigInt: bigint | null = null;

/**
 * Ensure thread ID is initialized.
 * Called lazily on first access.
 */
function ensureInitialized(): void {
  if (threadIdBytes !== null) return;

  threadIdBytes = new Uint8Array(THREAD_ID_BYTES);

  // Use Web Crypto API - available in browsers, Node 19+, Deno, Bun, Workers
  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    crypto.getRandomValues(threadIdBytes);
    return;
  }

  // For Node.js < 19, try to use node:crypto
  try {
    const nodeCrypto = require('node:crypto');
    if (nodeCrypto && typeof nodeCrypto.randomFillSync === 'function') {
      nodeCrypto.randomFillSync(threadIdBytes);
      return;
    }
  } catch {
    // node:crypto not available
  }

  throw new Error(
    'crypto.getRandomValues is not available. ' +
      'For React Native, install and import "react-native-get-random-values" before using this library.',
  );
}

/**
 * Copy the process thread ID bytes to a destination buffer.
 * This is the primary hot-path API for SpanBuffer construction.
 *
 * @param dest - Destination Uint8Array
 * @param offset - Byte offset in destination
 */
export function copyThreadIdTo(dest: Uint8Array, offset: number): void {
  ensureInitialized();
  if (!threadIdBytes) {
    throw new Error('ThreadId bytes not initialized');
  }
  dest.set(threadIdBytes, offset);
}

/**
 * Get the process thread ID as a BigInt.
 * Use copyThreadIdTo() for hot-path writes; this is for Arrow conversion.
 *
 * @returns 64-bit thread ID as BigInt
 */
export function getThreadId(): bigint {
  ensureInitialized();
  if (threadIdBigInt === null) {
    if (!threadIdBytes) {
      throw new Error('ThreadId bytes not initialized');
    }
    const b = threadIdBytes;
    // Little-endian conversion to match identity byte layout
    threadIdBigInt =
      BigInt(b[0]) |
      (BigInt(b[1]) << 8n) |
      (BigInt(b[2]) << 16n) |
      (BigInt(b[3]) << 24n) |
      (BigInt(b[4]) << 32n) |
      (BigInt(b[5]) << 40n) |
      (BigInt(b[6]) << 48n) |
      (BigInt(b[7]) << 56n);
  }
  return threadIdBigInt;
}

/**
 * Write the process thread ID to a BigUint64Array at the given index.
 * Avoids BigInt conversion by writing bytes directly to the underlying buffer.
 *
 * @param dest - Destination BigUint64Array
 * @param index - Element index (not byte offset)
 */
export function writeThreadIdToUint64Array(dest: BigUint64Array, index: number): void {
  ensureInitialized();
  if (!threadIdBytes) {
    throw new Error('ThreadId bytes not initialized');
  }
  const byteView = new Uint8Array(dest.buffer, dest.byteOffset + index * 8, 8);
  byteView.set(threadIdBytes);
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
  threadIdBytes = null;
  threadIdBigInt = null;
}
