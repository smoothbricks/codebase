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
 * // Add to EpochMicros (canonical decide clock)
 * addDuration(nowMicros(), '1 hour');
 *
 * // Add to Date (display / civil edges)
 * addDuration(new Date(), '1 hour');
 * ```
 *
 * @packageDocumentation
 */

// Clock + string → same clock domain (Date or EpochMicros)
export { addDuration, durationToMicros } from './add-duration.js';

// Core function - string → milliseconds
export { parseDuration } from './parse-duration.js';
// Types
export type { DurationString, Milliseconds } from './types.js';
