/**
 * LogAPI - Simple logging interface for contexts that need structured logging
 * without direct access to a SpanBuffer.
 *
 * Used by:
 * - Other contexts where a simplified logging interface is needed
 *
 * Unlike SpanLogger which is tightly coupled to a SpanBuffer and returns
 * FluentLogEntry for chaining, LogAPI is a simple fire-and-forget interface
 * that can be backed by any logging implementation.
 */

/**
 * Structured logging API.
 *
 * Provides info/warn/error/debug methods with optional structured data.
 * Implementation determines where logs are written (span buffer, console, etc).
 *
 * @example
 * ```typescript
 * function decide(ctx) {
 *   ctx.log.info('Processing order', { orderId: ctx.instanceId });
 *   if (!ctx.state.inventory) {
 *     ctx.log.warn('No inventory data', { state: ctx.state });
 *   }
 * }
 * ```
 */
export interface LogAPI {
  /**
   * Log at info level.
   * @param message - Log message
   * @param data - Optional structured data to include with the log
   */
  info(message: string, data?: Record<string, unknown>): void;

  /**
   * Log at warn level.
   * @param message - Log message
   * @param data - Optional structured data to include with the log
   */
  warn(message: string, data?: Record<string, unknown>): void;

  /**
   * Log at error level.
   * @param message - Log message
   * @param data - Optional structured data to include with the log
   */
  error(message: string, data?: Record<string, unknown>): void;

  /**
   * Log at debug level.
   * @param message - Log message
   * @param data - Optional structured data to include with the log
   */
  debug(message: string, data?: Record<string, unknown>): void;
}

/**
 * No-op LogAPI implementation.
 *
 * Used when tracing is disabled or for testing contexts that don't
 * need actual logging.
 */
export const noopLogAPI: LogAPI = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};
