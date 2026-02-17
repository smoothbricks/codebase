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
// SpanIdentity - Span identification for external systems
// ============================================================================

/**
 * SpanIdentity - Identifies a specific span for external correlation.
 *
 * distributed tracing headers, error reporting). Contains the minimal fields needed
 * to uniquely identify a span within a trace.
 *
 * **Usage:**
 * ```typescript
 * // Extract from buffer
 * const identity: SpanIdentity = {
 *   trace_id: buffer.trace_id,
 *   span_id: buffer.span_id,
 *   thread_id: buffer.thread_id,
 * };
 *
 * // Use in signals/messages
 * signalReportDown({ service, reporter, span: identity });
 * ```
 *
 * **Fields:**
 * - `trace_id`: 128-bit UUID identifying the entire trace (branded string)
 * - `span_id`: 32-bit counter unique within the trace
 * - `thread_id`: 64-bit identifier for execution thread/context
 *
 * @see specs/lmao/01b4_span_identity.md for full identity design
 */
export interface SpanIdentity {
  /** Trace ID - 128-bit UUID identifying the entire trace */
  readonly trace_id: TraceId;

  /** Span ID - 32-bit counter unique within this trace */
  readonly span_id: number;

  /** Thread ID - 64-bit identifier for execution thread/context */
  readonly thread_id: bigint;
}

/**
 * Extract SpanIdentity from a span buffer.
 *
 * @param buffer - Any object with trace_id, span_id, thread_id properties
 * @returns SpanIdentity for external correlation
 */
export function extractSpanIdentity(buffer: { trace_id: TraceId; span_id: number; thread_id: bigint }): SpanIdentity {
  return {
    trace_id: buffer.trace_id,
    span_id: buffer.span_id,
    thread_id: buffer.thread_id,
  };
}
