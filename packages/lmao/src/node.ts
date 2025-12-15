/**
 * Node.js entry point for @smoothbricks/lmao/node
 * Uses process.hrtime.bigint() for nanosecond-precision timestamps
 */

// Re-export all main functionality
export * from './index.js';

// Override with Node.js-specific timestamp implementation
export { createTimeAnchor, getTimestampMicros } from './lib/timestamp.node.js';
