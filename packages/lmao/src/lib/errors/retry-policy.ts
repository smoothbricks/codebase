/**
 * RetryPolicy - configures retry behavior for transient failures.
 *
 * Carried by TransientError instances. Op class (in LMAO) reads policy
 * from transient errors to handle retry loop.
 *
 * @example
 * ```typescript
 * // Define error with default policy
 * const RATE_LIMITED = Transient<{ retryAfter?: number }>('RATE_LIMITED', exponentialBackoff(5));
 *
 * // Usage with default policy
 * ctx.err(RATE_LIMITED({ retryAfter: 60000 }));
 *
 * // Override policy for this call
 * ctx.err(RATE_LIMITED({ retryAfter: 60000 }, fixedDelay(1, 60000)));
 * ```
 */
export interface RetryPolicy {
  /** Backoff strategy */
  backoff: 'exponential' | 'linear' | 'fixed';
  /** Maximum retry attempts (including initial attempt) */
  maxAttempts: number;
  /** Base delay in milliseconds */
  baseDelayMs: number;
  /** Maximum delay cap in milliseconds (optional, defaults to 30000) */
  maxDelayMs?: number;
  /** Add randomness to delay (default: true) */
  jitter?: boolean;
}

/**
 * Create linear backoff policy.
 * Delay increases linearly: baseDelay * attempt
 *
 * @example linearBackoff(3) → 100ms, 200ms, 300ms
 * @example linearBackoff(5, 200) → 200ms, 400ms, 600ms, 800ms, 1000ms
 */
export function linearBackoff(maxAttempts: number, baseDelayMs = 100): RetryPolicy {
  return {
    backoff: 'linear',
    maxAttempts,
    baseDelayMs,
    jitter: true,
  };
}

/**
 * Create exponential backoff policy.
 * Delay doubles each attempt: baseDelay * 2^(attempt-1)
 *
 * @example exponentialBackoff(3) → 100ms, 200ms, 400ms
 * @example exponentialBackoff(5, 50) → 50ms, 100ms, 200ms, 400ms, 800ms
 */
export function exponentialBackoff(maxAttempts: number, baseDelayMs = 100): RetryPolicy {
  return {
    backoff: 'exponential',
    maxAttempts,
    baseDelayMs,
    jitter: true,
  };
}

/**
 * Create fixed delay policy.
 * Same delay between each attempt.
 *
 * @example fixedDelay(3, 1000) → 1000ms, 1000ms, 1000ms
 * @example fixedDelay(1, 60000) → single retry after 60s (for rate limit Retry-After)
 */
export function fixedDelay(maxAttempts: number, delayMs = 1000): RetryPolicy {
  return {
    backoff: 'fixed',
    maxAttempts,
    baseDelayMs: delayMs,
    jitter: false, // Fixed delay typically doesn't want jitter
  };
}

/**
 * Merge base policy with override.
 * Override fields take precedence.
 */
export function mergePolicy(base: RetryPolicy, override?: Partial<RetryPolicy>): RetryPolicy {
  if (!override) return base;
  return { ...base, ...override };
}
