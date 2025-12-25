/**
 * Node.js entry point for @smoothbricks/lmao/node
 * Uses process.hrtime.bigint() for nanosecond-precision timestamps
 */

// Set platform-specific timestamp implementation BEFORE re-exports
// This ensures SPAN_LOGGER_HELPERS.getTimestampNanos is set before any
// SpanLogger class is created
import { setTimestampNanosImpl } from './lib/codegen/spanLoggerGenerator.js';
import { getTimestampNanos } from './lib/timestamp.node.js';

setTimestampNanosImpl(getTimestampNanos);

// Re-export all main functionality
export * from './index.js';

// Also export the timestamp function and anchor creation for direct use
export { createTimestampAnchor, getTimestampNanos } from './lib/timestamp.node.js';
