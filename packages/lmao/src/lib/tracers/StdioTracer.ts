/**
 * StdioTracer - A tracer that prints spans to stdout/stderr
 *
 * Features:
 * - Color-coded trace IDs based on trace_id for visual separation of concurrent traces
 * - Per-trace indent tracking using Map<TraceId, number> for correct nesting
 * - Human-readable timestamps (ISO 8601) and durations
 * - Tree-style output with ├─ for start and └─ for end
 * - Exceptions written to stderr
 *
 * Useful for development/debugging to see span execution in real-time.
 *
 * @example
 * ```typescript
 * import { createTraceRoot } from '@smoothbricks/lmao/node';
 * import { JsBufferStrategy } from '@smoothbricks/lmao';
 *
 * const ctx = defineOpContext({ logSchema: mySchema });
 * const { trace } = new StdioTracer(ctx, {
 *   bufferStrategy: new JsBufferStrategy(),
 *   createTraceRoot,
 * });
 *
 * await trace('fetch-user', fetchOp);
 *
 * // Output:
 * // [2025-12-25T10:30:45.123Z] [123456] fetch-user
 * // [2025-12-25T10:30:45.125Z] [123456]   ├─ db-query
 * // [2025-12-25T10:30:45.225Z] [123456]   └─ db-query [OK] (100.00ms)
 * // [2025-12-25T10:30:45.226Z] [123456] fetch-user (103.00ms) ========
 * ```
 */

import type { OpContextBinding } from '../opContext/types.js';
import { ENTRY_TYPE_SPAN_ERR, ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_OK } from '../schema/systemSchema.js';
import type { TraceId } from '../traceId.js';
import type { TracerOptions } from '../tracer.js';
import { Tracer } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

// ============================================================================
// Formatting Helpers
// ============================================================================

/**
 * Format nanosecond timestamp to ISO 8601 string with millisecond precision.
 */
function formatTimestamp(nanos: bigint): string {
  const millis = Number(nanos / 1_000_000n);
  return new Date(millis).toISOString();
}

/**
 * Format duration in nanoseconds to human-readable string.
 */
function formatDuration(nanos: bigint): string {
  const n = Number(nanos);
  if (n >= 1_000_000_000) {
    return `${(n / 1_000_000_000).toFixed(2)}s`;
  }
  if (n >= 1_000_000) {
    return `${(n / 1_000_000).toFixed(2)}ms`;
  }
  if (n >= 1_000) {
    return `${(n / 1_000).toFixed(2)}µs`;
  }
  return `${n}ns`;
}

/**
 * ANSI color codes for visual trace separation.
 * Each trace gets a consistent color based on trace_id hash.
 */
const COLORS = [
  '\u001b[31m', // Red
  '\u001b[32m', // Green
  '\u001b[33m', // Yellow
  '\u001b[34m', // Blue
  '\u001b[35m', // Magenta
  '\u001b[36m', // Cyan
  '\u001b[91m', // Bright Red
  '\u001b[92m', // Bright Green
  '\u001b[93m', // Bright Yellow
  '\u001b[94m', // Bright Blue
  '\u001b[95m', // Bright Magenta
  '\u001b[96m', // Bright Cyan
] as const;

const RESET = '\u001b[0m';

/**
 * Get consistent color for a trace_id.
 * Uses modulo arithmetic to map 128-bit trace_id to one of the colors.
 */
function getTraceColor(traceId: TraceId): string {
  // Convert TraceId (string representation) to a number for coloring
  // Use simple hash of the string to pick a color
  let hash = 0;
  const str = String(traceId);
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    // Simple hash function
    hash = (hash << 5) - hash + char;
    // biome-ignore lint: Convert to 32bit integer
    hash = hash & hash;
  }
  const index = Math.abs(hash) % COLORS.length;
  return COLORS[index];
}

/**
 * Get status string from entry type.
 */
function getStatus(entryType: number): string {
  switch (entryType) {
    case ENTRY_TYPE_SPAN_OK:
      return 'OK';
    case ENTRY_TYPE_SPAN_ERR:
      return 'ERR';
    case ENTRY_TYPE_SPAN_EXCEPTION:
      return 'EXCEPTION';
    default:
      return 'UNKNOWN';
  }
}

// ============================================================================
// StdioTracer Options
// ============================================================================

/**
 * Options for StdioTracer
 */
export interface StdioTracerOptions<
  T extends import('../schema/LogSchema.js').LogSchema = import('../schema/LogSchema.js').LogSchema,
> extends TracerOptions<T> {
  /** Output stream (defaults to process.stdout) */
  out?: NodeJS.WriteStream;
  /** Error stream (defaults to process.stderr) */
  err?: NodeJS.WriteStream;
  /** Enable ANSI colors (defaults to true) */
  colorEnabled?: boolean;
}

// ============================================================================
// StdioTracer
// ============================================================================

/**
 * Tracer that prints spans to stdout/stderr with colored, indented output.
 *
 * Features:
 * - Color-coded trace IDs for visual separation of concurrent traces
 * - Per-trace indent tracking (concurrent-safe via Map)
 * - Tree-style output (├─ for start, └─ for end)
 * - Human-readable timestamps and durations
 * - Exceptions go to stderr
 */
export class StdioTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  /**
   * Per-trace indent levels for concurrent-safe nesting.
   * Key is trace_id string representation, value is current indent level.
   */
  private readonly indents = new Map<string, number>();

  private readonly out: NodeJS.WriteStream;
  private readonly err: NodeJS.WriteStream;
  private readonly colorEnabled: boolean;

  constructor(binding: B, options: StdioTracerOptions<B['logBinding']['logSchema']>) {
    super(binding, options);
    this.out = options.out ?? process.stdout;
    this.err = options.err ?? process.stderr;
    this.colorEnabled = options.colorEnabled ?? true;
  }

  // --------------------------------------------------------------------------
  // Indent Management
  // --------------------------------------------------------------------------

  private getIndent(traceId: TraceId): number {
    const key = String(traceId);
    return this.indents.get(key) ?? 0;
  }

  private incrementIndent(traceId: TraceId): void {
    const key = String(traceId);
    this.indents.set(key, this.getIndent(traceId) + 1);
  }

  private decrementIndent(traceId: TraceId): void {
    const key = String(traceId);
    const current = this.getIndent(traceId);
    if (current <= 1) {
      this.indents.delete(key); // Cleanup when trace ends
    } else {
      this.indents.set(key, current - 1);
    }
  }

  // --------------------------------------------------------------------------
  // Color Helpers
  // --------------------------------------------------------------------------

  private color(traceId: TraceId): string {
    return this.colorEnabled ? getTraceColor(traceId) : '';
  }

  private reset(): string {
    return this.colorEnabled ? RESET : '';
  }

  // --------------------------------------------------------------------------
  // Lifecycle Hooks
  // --------------------------------------------------------------------------

  onTraceStart(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    const traceId = rootBuffer.trace_id;
    const name = rootBuffer.message_values[0];
    const ts = formatTimestamp(rootBuffer.timestamp[0]);

    this.out.write(`[${ts}] ${this.color(traceId)}[${traceId}]${this.reset()} ${name}\n`);
    this.incrementIndent(traceId);
  }

  onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    const traceId = rootBuffer.trace_id;
    this.decrementIndent(traceId);

    const name = rootBuffer.message_values[0];
    const startTs = rootBuffer.timestamp[0];
    const endTs = rootBuffer.timestamp[1];
    const duration = formatDuration(endTs - startTs);
    const entryType = rootBuffer.entry_type[1];
    const status = getStatus(entryType);
    const ts = formatTimestamp(endTs);

    const line = `[${ts}] ${this.color(traceId)}[${traceId}]${this.reset()} ${name} [${status}] (${duration}) ${'='.repeat(40)}\n`;

    // Write exceptions to stderr
    if (entryType === ENTRY_TYPE_SPAN_EXCEPTION) {
      this.err.write(line);
    } else {
      this.out.write(line);
    }
  }

  onSpanStart(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    const traceId = childBuffer.trace_id;
    const indent = this.getIndent(traceId);
    const name = childBuffer.message_values[0];
    const ts = formatTimestamp(childBuffer.timestamp[0]);

    this.out.write(`[${ts}] ${this.color(traceId)}[${traceId}]${this.reset()} ${'  '.repeat(indent)}├─ ${name}\n`);
    this.incrementIndent(traceId);
  }

  onSpanEnd(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    const traceId = childBuffer.trace_id;
    this.decrementIndent(traceId);
    const indent = this.getIndent(traceId);

    const name = childBuffer.message_values[0];
    const startTs = childBuffer.timestamp[0];
    const endTs = childBuffer.timestamp[1];
    const duration = formatDuration(endTs - startTs);
    const entryType = childBuffer.entry_type[1];
    const status = getStatus(entryType);
    const ts = formatTimestamp(endTs);

    const line = `[${ts}] ${this.color(traceId)}[${traceId}]${this.reset()} ${'  '.repeat(indent)}└─ ${name} [${status}] (${duration})\n`;

    // Write exceptions to stderr
    if (entryType === ENTRY_TYPE_SPAN_EXCEPTION) {
      this.err.write(line);
    } else {
      this.out.write(line);
    }
  }

  onStatsWillResetFor(_buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    const stats = _buffer._stats;
    // Compute utilization = totalWrites / (spansCreated * usableRowsPerSpan)
    const usableRowsPerSpan = stats.capacity - 2;
    const utilization =
      stats.spansCreated > 0
        ? ((stats.totalWrites / (stats.spansCreated * usableRowsPerSpan)) * 100).toFixed(1)
        : '0.0';
    const traceId = _buffer.trace_id;
    const ts = formatTimestamp(_buffer.timestamp[0]);

    this.out.write(
      `[${ts}] ${this.color(traceId)}[${traceId}]${this.reset()} ` +
        `[CAPACITY] writes=${stats.totalWrites} spans=${stats.spansCreated} utilization=${utilization}% ` +
        `capacity=${stats.capacity}\n`,
    );
  }
}
