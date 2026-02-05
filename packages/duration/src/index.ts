/**
 * @smoothbricks/duration
 *
 * Duration parsing utilities for human-readable duration strings.
 *
 * @example
 * ```typescript
 * import { parseDuration, addDuration } from '@smoothbricks/duration';
 *
 * // Parse to milliseconds
 * parseDuration('5 minutes');  // 300000
 *
 * // Add to date
 * addDuration(new Date(), '1 hour');  // Date 1 hour from now
 * ```
 *
 * @packageDocumentation
 */

// Convenience wrapper - date + string → Date
export { addDuration } from './add-duration.js';

// Core function - string → milliseconds
export { parseDuration } from './parse-duration.js';
// Types
export type { DurationString, Milliseconds } from './types.js';
