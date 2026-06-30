/**
 * Node.js entry point for @smoothbricks/lmao/node
 * Uses process.hrtime.bigint() for nanosecond-precision timestamps
 */

// Re-export all main functionality
export * from './index.js';

// Export Node.js-specific TraceRoot factory for Tracer construction
//#region smoo/lmao!n/trace-root-timestamps.entry-points #node
export { createTraceRoot } from './lib/traceRoot.node.js';
//#endregion smoo/lmao!n/trace-root-timestamps.entry-points
