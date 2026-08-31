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

//#region smoo/lmao!n/span-identity.trace-id
/** Maximum length for a trace ID */
export const MAX_TRACE_ID_LENGTH = 128;

/** Branded type for validated trace IDs */
export type TraceId = string & { readonly __brand: 'TraceId' };

function brandTraceId(value: string): TraceId;
function brandTraceId(value: string): string {
  return value;
}

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
  return brandTraceId(value);
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
 * Byte → its two lowercase hex digits.
 *
 * `byte.toString(16).padStart(2, '0')` is a radix conversion plus a pad, run 16
 * times per trace id; the JSC sampling profile of the js-heap row path
 * (`benchmarks/_clockProfile.ts`) puts `generateTraceId` + `padStart` at 7.7% of
 * on-CPU even at 50 log rows per span, where the trace id is paid once per span.
 * Two hundred fifty-six interned two-character strings turn the loop into
 * sixteen array loads and one concatenation.
 *
 * Built from an explicit digit table rather than from `toString(16)` so the
 * parity check over all 256 bytes tests the table instead of restating it.
 */
const HEX_DIGITS = '0123456789abcdef';
const HEX_BYTE: readonly string[] = Array.from(
  { length: 256 },
  (_, byte) => `${HEX_DIGITS[byte >>> 4]}${HEX_DIGITS[byte & 0xf]}`,
);

/**
 * Encode 16 already-chosen bytes as a W3C trace id.
 *
 * Split out from [`generateTraceId`] because the encoding and the byte source
 * are separate contracts: the table is verified over the whole 0x00-0xFF domain
 * in all sixteen positions, and reaching it through the generator meant
 * substituting `crypto.getRandomValues` on the host object. That descriptor is
 * not configurable on every engine — Bun on Linux refuses the redefinition with
 * `Attempting to change configurable attribute of unconfigurable property`, so
 * the parity check passed on one platform and threw on the other. It is also
 * the entry point for adopting an incoming distributed-trace id from bytes.
 */
export function traceIdFromBytes(bytes: Uint8Array): TraceId {
  // 16 table loads into one rope; see HEX_BYTE for why the toString(16) loop went.
  const hex =
    HEX_BYTE[bytes[0]] +
    HEX_BYTE[bytes[1]] +
    HEX_BYTE[bytes[2]] +
    HEX_BYTE[bytes[3]] +
    HEX_BYTE[bytes[4]] +
    HEX_BYTE[bytes[5]] +
    HEX_BYTE[bytes[6]] +
    HEX_BYTE[bytes[7]] +
    HEX_BYTE[bytes[8]] +
    HEX_BYTE[bytes[9]] +
    HEX_BYTE[bytes[10]] +
    HEX_BYTE[bytes[11]] +
    HEX_BYTE[bytes[12]] +
    HEX_BYTE[bytes[13]] +
    HEX_BYTE[bytes[14]] +
    HEX_BYTE[bytes[15]];

  return brandTraceId(hex);
}

/**
 * The byte source, bound ONCE at module load.
 *
 * `generateTraceId` runs once per root trace and the JSC sampling profile
 * already puts it at 7.7% of on-CPU, so re-deciding the implementation on every
 * call spent two `typeof` probes and a try/catch frame to reach the same
 * function every time. Every platform this package ships an entrypoint for —
 * `./node`, `./es`, `./wasm`, `./cloudflare` — has WebCrypto, so there is one
 * implementation to bind and no feature test to repeat.
 *
 * The arms this replaces could not run. The package is ESM-only, so
 * `require('node:crypto')` threw `ReferenceError` rather than loading Node's
 * crypto, and the catch then produced a `Math.random` trace id: 128 bits of
 * entropy traded away silently, on the identity every stored trace and every
 * external correlation keys on. A host without WebCrypto now fails loudly at
 * import instead.
 */
const fillRandomBytes: (bytes: Uint8Array) => Uint8Array = crypto.getRandomValues.bind(crypto);

/** Generate a new random TraceId (W3C format: 32 hex chars). */
export function generateTraceId(): TraceId {
  return traceIdFromBytes(fillRandomBytes(new Uint8Array(16)));
}
//#endregion smoo/lmao!n/span-identity.trace-id

// ============================================================================
// SpanIdentity - Span identification for external systems
// ============================================================================
//#region smoo/lmao!n/span-identity.external-correlation

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
//#endregion smoo/lmao!n/span-identity.external-correlation
