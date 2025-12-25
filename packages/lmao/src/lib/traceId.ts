/**
 * TraceId - Branded string type for trace identification
 *
 * A TraceId is a string that uniquely identifies a distributed trace.
 * It's validated to be non-empty and at most 128 characters (ASCII).
 *
 * Common formats:
 * - W3C Trace Context: 32 hex chars (e.g., "0af7651916cd43dd8448eb211c80319c")
 * - OpenTelemetry: same as W3C
 * - Custom: any string up to 128 chars
 *
 * @module traceId
 */

/** Maximum length for a trace ID */
export const MAX_TRACE_ID_LENGTH = 128;

/** Branded type for validated trace IDs */
export type TraceId = string & { readonly __brand: 'TraceId' };

/** Precompiled regex for non-ASCII detection (2x faster than loop) */
const NON_ASCII_REGEX = /[^\x20-\x7E]/;

/**
 * Validate and create a TraceId from a string.
 * @throws Error if validation fails
 */
export function createTraceId(value: string): TraceId {
  if (typeof value !== 'string') {
    throw new Error('TraceId must be a string');
  }
  if (value.length === 0) {
    throw new Error('TraceId cannot be empty');
  }
  if (value.length > MAX_TRACE_ID_LENGTH) {
    throw new Error(`TraceId cannot exceed ${MAX_TRACE_ID_LENGTH} characters`);
  }
  if (NON_ASCII_REGEX.test(value)) {
    throw new Error('TraceId must be ASCII printable characters only');
  }
  return value as TraceId;
}

/**
 * Check if a value is a valid TraceId without throwing.
 */
export function isValidTraceId(value: unknown): value is TraceId {
  return (
    typeof value === 'string' && value.length > 0 && value.length <= MAX_TRACE_ID_LENGTH && !NON_ASCII_REGEX.test(value)
  );
}

/**
 * Generate a new random TraceId (W3C format: 32 hex chars).
 */
export function generateTraceId(): TraceId {
  const bytes = new Uint8Array(16);

  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    crypto.getRandomValues(bytes);
  } else {
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const nodeCrypto = require('node:crypto');
      if (nodeCrypto?.randomFillSync) {
        nodeCrypto.randomFillSync(bytes);
      }
    } catch {
      // Fallback to Math.random (not ideal but works)
      for (let i = 0; i < 16; i++) {
        bytes[i] = Math.floor(Math.random() * 256);
      }
    }
  }

  // Convert to hex string
  let hex = '';
  for (let i = 0; i < 16; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }

  return hex as TraceId;
}

// ============================================================================
// TraceRoot - Per-trace anchor data
// ============================================================================

/**
 * TraceRoot - Per-trace anchoring data for high-precision timestamps
 *
 * Created once at trace creation (createTrace()) and shared by all spans in the trace.
 * Stored on root SpanBuffer and copied by reference to all child spans for O(1) access.
 *
 * **Why per-trace anchoring:**
 * - Each trace has fresh anchor - no long-running drift issues
 * - NTP corrections between traces are isolated
 * - Trace is self-contained unit with consistent time reference
 *
 * **Memory layout:**
 * - Created ONCE per trace (root span creation)
 * - Shared by ALL spans in trace (copied by reference, 8 bytes per span)
 * - Total overhead: ~32 bytes per trace (object header + 2 properties)
 *
 * See specs/01b3_high_precision_timestamps.md for timestamp design.
 */
export interface TraceRoot {
  /**
   * Trace ID for this trace.
   * Stored here for quick access without walking buffer parent chain.
   */
  readonly trace_id: TraceId;

  /**
   * Epoch time in nanoseconds when trace was created.
   * Captured via Date.now() * 1_000_000n at trace root.
   */
  readonly anchorEpochNanos: bigint;

  /**
   * High-resolution timer value when trace was created.
   * Captured via performance.now() (browser) or process.hrtime.bigint() (Node.js).
   */
  readonly anchorPerfNow: number;

  /**
   * Tracer reference for lifecycle hooks and event callbacks.
   *
   * Provides all Tracer lifecycle methods needed by SpanContext and SpanLogger.
   * Uses `unknown` for buffer parameter to avoid circular dependency with types.ts.
   * The Tracer implementation receives AnySpanBuffer at runtime.
   */
  readonly tracer: {
    onTraceStart(buffer: unknown): void;
    onTraceEnd(buffer: unknown): void;
    onSpanStart(buffer: unknown): void;
    onSpanEnd(buffer: unknown): void;
    onStatsWillResetFor(buffer: unknown): void;
  };
}
