/**
 * TransientError - typed transient error codes with embedded retry policy.
 *
 * Use the `Transient()` factory to define transient error codes WITH default retry policy.
 * Op class checks `instanceof TransientError` and reads `error.policy` for retry config.
 *
 * @example
 * ```typescript
 * import { Transient, exponentialBackoff, fixedDelay } from '@smoothbricks/lmao/errors/Transient';
 *
 * // Define error with default exponential backoff (5 attempts)
 * const SERVICE_UNAVAILABLE = Transient<{ service: string }>('SERVICE_UNAVAILABLE', exponentialBackoff(5));
 *
 * // Define error with fixed delay for rate limiting
 * const RATE_LIMITED = Transient<{ retryAfter?: number }>('RATE_LIMITED', fixedDelay(3, 5000));
 *
 * // Usage - data only, uses default policy
 * ctx.err(SERVICE_UNAVAILABLE({ service: 'api' }));
 *
 * // Override policy for this specific call (e.g., use Retry-After header)
 * ctx.err(RATE_LIMITED({ retryAfter: 60000 }, fixedDelay(1, 60000)));
 *
 * // Op class detects transient and reads policy
 * if (result.error instanceof TransientError) {
 *   const { maxAttempts, backoff, baseDelayMs } = result.error.policy;
 *   // Execute retry loop based on policy
 * }
 * ```
 */

import type { TaggedErrorConstructor } from '../result.js';
import { CodeError } from './CodeError.js';
import { mergePolicy, type RetryPolicy } from './retry-policy.js';

/**
 * Transient error instance - triggers retry in Op class.
 *
 * Extends CodeError to inherit code/data pattern while adding:
 * - `instanceof TransientError` checks for retry classification
 * - `policy` field carrying merged retry configuration
 *
 * @typeParam C - The error code string literal type (also serves as _tag)
 * @typeParam D - The data payload type
 */
export class TransientError<C extends string, D = void> extends CodeError<C, D> {
  /**
   * Merged retry policy (defaultPolicy + per-call override).
   * Op class reads this to configure retry loop.
   */
  readonly policy: RetryPolicy;

  constructor(code: C, data: D, policy: RetryPolicy) {
    super(code, data);
    this.name = 'TransientError';
    this.policy = policy;
  }

  override toJSON() {
    return {
      ...super.toJSON(),
      policy: this.policy,
    };
  }
}

/**
 * Constructor function for a specific transient error code.
 * Has static `_tag` for isErr() and `defaultPolicy` for introspection.
 */
export interface TransientConstructor<C extends string, D = void> extends TaggedErrorConstructor<TransientError<C, D>> {
  /** Create transient error with data and optional policy override */
  (data: D, policyOverride?: Partial<RetryPolicy>): TransientError<C, D>;
  /** Error code (same as _tag) */
  readonly _tag: C;
  /** Default retry policy for this error type */
  readonly defaultPolicy: RetryPolicy;
}

/**
 * Constructor function for transient error codes with no data.
 */
export interface TransientConstructorVoid<C extends string> extends TaggedErrorConstructor<TransientError<C, void>> {
  /** Create transient error with optional policy override */
  (policyOverride?: Partial<RetryPolicy>): TransientError<C, void>;
  /** Error code (same as _tag) */
  readonly _tag: C;
  /** Default retry policy for this error type */
  readonly defaultPolicy: RetryPolicy;
}

type TransientFactoryReturn<C extends string, D> = [unknown] extends [D]
  ? TransientConstructor<C, D>
  : undefined extends D
    ? TransientConstructorVoid<C>
    : TransientConstructor<C, D>;

/**
 * Define a typed transient error code with default retry policy.
 *
 * Creates errors that extend TransientError, enabling:
 * - `instanceof TransientError` for retry triggering
 * - `result.isErr(CODE)` for specific code discrimination
 * - `error.policy` for retry configuration
 *
 * @param code - The error code string (becomes _tag for discrimination)
 * @param defaultPolicy - Default retry policy for this error type
 * @returns A constructor function that creates TransientError instances
 *
 * @example
 * ```typescript
 * // With typed data and exponential backoff
 * const NETWORK_ERROR = Transient<{ service: string }>('NETWORK_ERROR', exponentialBackoff(5));
 * ctx.err(NETWORK_ERROR({ service: 'payment-api' }));
 *
 * // Without data, linear backoff
 * const TIMEOUT = Transient('TIMEOUT', linearBackoff(3));
 * ctx.err(TIMEOUT());
 *
 * // Override policy per-call
 * ctx.err(NETWORK_ERROR({ service: 'api' }, fixedDelay(1, 30000)));
 *
 * // Op class handles retry
 * if (result.error instanceof TransientError) {
 *   const { policy } = result.error;
 *   // Retry loop using policy.maxAttempts, policy.backoff, etc.
 * }
 * ```
 */
export function Transient<C extends string>(code: C, defaultPolicy: RetryPolicy): TransientConstructorVoid<C>;
export function Transient<D, C extends string = string>(
  code: C,
  defaultPolicy: RetryPolicy,
): TransientFactoryReturn<C, D>;
export function Transient(
  code: string,
  defaultPolicy: RetryPolicy,
): TransientConstructorVoid<string> | TransientConstructor<string, unknown> {
  function createTransientError(policyOverride?: Partial<RetryPolicy>): TransientError<string, void>;
  function createTransientError(data: unknown, policyOverride?: Partial<RetryPolicy>): TransientError<string, unknown>;
  function createTransientError(dataOrOverride?: unknown, maybeOverride?: Partial<RetryPolicy>) {
    const override =
      maybeOverride !== undefined ? maybeOverride : isRetryPolicyLike(dataOrOverride) ? dataOrOverride : undefined;

    const data = maybeOverride !== undefined || override !== undefined ? undefined : dataOrOverride;
    const mergedPolicy = mergePolicy(defaultPolicy, override);
    return new TransientError(code, data, mergedPolicy);
  }

  return Object.assign(createTransientError, {
    _tag: code,
    defaultPolicy,
  });
}

/**
 * Type guard to check if value looks like a RetryPolicy (partial).
 * Used to disambiguate void data + override from actual data.
 */
function isRetryPolicyLike(value: unknown): value is Partial<RetryPolicy> {
  if (typeof value !== 'object' || value === null) return false;
  // Check for RetryPolicy-specific keys — `in` accepts `object`, no cast needed
  return (
    'backoff' in value || 'maxAttempts' in value || 'baseDelayMs' in value || 'maxDelayMs' in value || 'jitter' in value
  );
}

// Re-export policy helpers for convenience
export { exponentialBackoff, fixedDelay, linearBackoff, mergePolicy, type RetryPolicy } from './retry-policy.js';
