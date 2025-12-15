/**
 * ES/Browser entry point for @smoothbricks/lmao/es
 * Uses performance.now() for high-precision timestamps
 * Works in browsers, Deno, Cloudflare Workers, etc.
 */

// Re-export all main functionality
export * from './index.js';

// Override with ES-specific timestamp implementation (performance.now)
export { createTimeAnchor, getTimestampMicros } from './lib/timestamp.js';
