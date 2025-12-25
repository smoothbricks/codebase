/**
 * ES/Browser entry point for @smoothbricks/lmao/es
 * Uses performance.now() for high-precision timestamps
 * Works in browsers, Deno, Cloudflare Workers, etc.
 */

// Set platform-specific timestamp implementation BEFORE re-exports
// This ensures SPAN_LOGGER_HELPERS.getTimestampNanos is set before any
// SpanLogger class is created
import { setTimestampNanosImpl } from './lib/codegen/spanLoggerGenerator.js';
import { getTimestampNanos } from './lib/timestamp.js';

setTimestampNanosImpl(getTimestampNanos);

// Re-export all main functionality
export * from './index.js';

// Also export the timestamp function and anchor creation for direct use
export { createTimestampAnchor, getTimestampNanos } from './lib/timestamp.js';
