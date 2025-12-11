/**
 * Platform-specific timestamp utilities
 * 
 * Per vizanto's review feedback and specs/01f_arrow_table_structure.md:
 * - Node.js: Use process.hrtime.bigint() for nanosecond precision
 * - Browser: Use performance.now() + Date.now() for microsecond precision
 * 
 * Timestamps are stored in Float64Array as milliseconds in the hot path,
 * then converted to appropriate Arrow timestamp type during conversion.
 */

/**
 * Detect runtime environment
 */
const isNode =
  typeof process !== 'undefined' && process.versions != null && process.versions.node != null;

/**
 * Span start time tracker for browser relative timestamps
 * Maps span ID to { startTime: Date.now(), startPerf: performance.now() }
 */
const spanStartTimes = new Map<
  number,
  { startTime: number; startPerf: number }
>();

/**
 * Get current timestamp in milliseconds
 * 
 * - Node.js: Uses process.hrtime.bigint() with nanosecond precision
 * - Browser: Uses Date.now() for absolute time
 * 
 * @returns Timestamp in milliseconds (Float64)
 */
export function getCurrentTimestamp(): number {
  if (isNode) {
    // Node.js: Get nanoseconds and convert to milliseconds
    const nanos = process.hrtime.bigint();
    return Number(nanos) / 1_000_000; // Convert nanoseconds to milliseconds
  }

  // Browser: Use Date.now() for millisecond precision
  return Date.now();
}

/**
 * Initialize span timing (browser only)
 * 
 * Captures both Date.now() and performance.now() at span creation
 * to enable high-precision relative timestamps
 * 
 * @param spanId - Span identifier
 */
export function initSpanTiming(spanId: number): void {
  if (!isNode && typeof performance !== 'undefined') {
    spanStartTimes.set(spanId, {
      startTime: Date.now(),
      startPerf: performance.now(),
    });
  }
}

/**
 * Get relative timestamp from span start (browser only)
 * 
 * Uses performance.now() for microsecond precision relative timing
 * 
 * @param spanId - Span identifier
 * @returns Timestamp in milliseconds relative to span start, or absolute time if not initialized
 */
export function getRelativeTimestamp(spanId: number): number {
  if (isNode) {
    // Node.js: Just use hrtime
    return getCurrentTimestamp();
  }

  // Browser: Use performance.now() for high-precision relative time
  if (typeof performance !== 'undefined') {
    const spanStart = spanStartTimes.get(spanId);
    if (spanStart) {
      const elapsedPerf = performance.now() - spanStart.startPerf;
      return spanStart.startTime + elapsedPerf;
    }
  }

  // Fallback to Date.now()
  return Date.now();
}

/**
 * Clean up span timing data
 * 
 * @param spanId - Span identifier
 */
export function cleanupSpanTiming(spanId: number): void {
  spanStartTimes.delete(spanId);
}

/**
 * Get Arrow timestamp type for current platform
 * 
 * - Node.js: TimestampNanosecond (nanosecond precision)
 * - Browser: TimestampMicrosecond (microsecond precision via performance.now())
 * 
 * @returns Arrow timestamp type unit
 */
export function getArrowTimestampUnit(): 'nanosecond' | 'microsecond' | 'millisecond' {
  if (isNode) {
    return 'nanosecond'; // Node.js has nanosecond precision with process.hrtime.bigint()
  }
  
  // Browser: performance.now() has microsecond precision
  if (typeof performance !== 'undefined') {
    return 'microsecond';
  }
  
  // Fallback: Date.now() has millisecond precision
  return 'millisecond';
}

/**
 * Convert milliseconds timestamp to Arrow timestamp value
 * 
 * @param timestampMs - Timestamp in milliseconds
 * @param unit - Target Arrow timestamp unit
 * @returns Timestamp in target unit
 */
export function convertToArrowTimestamp(
  timestampMs: number,
  unit: 'nanosecond' | 'microsecond' | 'millisecond',
): number {
  switch (unit) {
    case 'nanosecond':
      return Math.floor(timestampMs * 1_000_000); // ms to ns
    case 'microsecond':
      return Math.floor(timestampMs * 1_000); // ms to μs
    case 'millisecond':
      return Math.floor(timestampMs); // Already in ms
    default:
      return Math.floor(timestampMs);
  }
}
