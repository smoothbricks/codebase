/**
 * Error types and utilities for LMAO.
 *
 * @example
 * ```typescript
 * import { Code, CodeError, Transient, TransientError, Blocked, exponentialBackoff } from '@smoothbricks/lmao/errors';
 * ```
 */

export * from './Blocked.js';
// Existing exports
export * from './CodeError.js';
export * from './RetriesExhausted.js';

// Phase 19 exports - TransientError uses its own RetryPolicy from retry-policy.ts
// Blocked.ts has its own local RetryPolicy which is different (string-based delay)
// Export retry-policy types with different name to avoid conflict
export {
  exponentialBackoff,
  fixedDelay,
  linearBackoff,
  mergePolicy,
  type RetryPolicy as TransientRetryPolicy,
} from './retry-policy.js';
export { Transient, type TransientConstructor, type TransientConstructorVoid, TransientError } from './Transient.js';
