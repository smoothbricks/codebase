/**
 * Blocked error - indicates an Op cannot proceed due to a temporary condition.
 *
 * A tagged error type for use with `result.isErr(Blocked)` to discriminate
 * blocked states from other errors. Useful for retry logic and dependency waiting.
 *
 * Supports two configuration styles:
 * - **RetryPolicy** (legacy): Declarative config with `delay`, `backoff`, `maxAttempts` strings
 * - **BlockedConfig** (spec): Closure-based `nextRetry(attempt) => ms` that captures Op context
 *
 * @example
 * ```typescript
 * import { defineOpContext, S, Err } from '@smoothbricks/lmao';
 * import { Blocked } from '@smoothbricks/lmao/errors/Blocked';
 *
 * const { defineOp } = defineOpContext({
 *   logSchema: { service: S.category() },
 *   ctx: { env: null as Env },
 * });
 *
 * const fetchData = defineOp('fetchData', async (ctx, service: string) => {
 *   const health = await checkHealth(service);
 *   if (!health.available) {
 *     // Return Err with Blocked - can chain .with() for tags
 *     return new Err(Blocked.service(service)).with({ service });
 *   }
 *   ctx.tag.service(service);
 *   return ctx.ok(await doFetch(service));
 * });
 *
 * // With closure-based retry (captures HTTP response context)
 * const callApi = defineOp('callApi', async (ctx, url: string) => {
 *   const response = await fetch(url);
 *   if (response.status === 503) {
 *     const retryAfterSec = parseInt(response.headers.get('Retry-After') ?? '5');
 *     return new Err(Blocked.service('payment-api', {
 *       maxAttempts: 5,
 *       nextRetry: (attempt) => {
 *         if (attempt === 1) return retryAfterSec * 1000;
 *         return 5000 * Math.pow(2, attempt - 1);
 *       },
 *     }));
 *   }
 *   return ctx.ok(await response.json());
 * });
 *
 * // Caller can discriminate blocked from other errors
 * const result = await trace('fetch', fetchData, 'payment-api');
 * if (result.isErr(Blocked)) {
 *   // Handle temporary unavailability - retry, queue, etc.
 *   console.log(`Blocked on ${result.error.reason.name}`);
 * }
 * ```
 */

import type { TaggedError } from '../result.js';

/**
 * Reason why an Op is blocked.
 */
export type BlockedReason =
  | { readonly type: 'service'; readonly name: string }
  | { readonly type: 'ended'; readonly target: string }
  | { readonly type: 'index'; readonly indexName: string };

/**
 * Retry policy for blocked errors (legacy declarative style).
 *
 * When not specified, engine uses defaults based on reason type:
 * - `service`: `{ delay: '5s', backoff: 'exponential', maxAttempts: 5 }`
 * - `ended`: `{ maxAttempts: undefined }` (wait indefinitely)
 * - `index`: `{ delay: '1s', backoff: 'linear', maxAttempts: 10 }`
 */
export interface RetryPolicy {
  /** Initial delay before retry (e.g., '2s', '500ms', '1 minute') */
  readonly delay?: string;
  /** Backoff strategy */
  readonly backoff?: 'fixed' | 'exponential' | 'linear';
  /** Maximum retry attempts (undefined = infinite) */
  readonly maxAttempts?: number;
}

/**
 * Engine-level retry configuration for blocked operations (spec style).
 *
 * The nextRetry closure is powerful because it captures the Op's execution context.
 * Each time the Op re-executes, it produces a new Blocked error with a new closure
 * that may reference updated information (e.g., fresh Retry-After headers).
 */
export interface BlockedConfig {
  /** Max canary retries before RetriesExhausted (default: 5) */
  readonly maxAttempts?: number;
  /** Closure returning delay in ms before next retry. Captures Op context. */
  readonly nextRetry?: (attempt: number) => number;
}

/**
 * Type guard: is this a legacy RetryPolicy (has `delay` or `backoff`)?
 */
function isRetryPolicy(config: RetryPolicy | BlockedConfig): config is RetryPolicy {
  return 'delay' in config || 'backoff' in config;
}

export class Blocked extends Error implements TaggedError<'Blocked'> {
  static readonly _tag = 'Blocked' as const;

  get _tag(): 'Blocked' {
    return 'Blocked';
  }

  /** Unified storage for either config style */
  readonly retryConfig: RetryPolicy | BlockedConfig | undefined;

  constructor(
    /** The reason this Op is blocked */
    readonly reason: BlockedReason,
    /** Optional retry configuration (RetryPolicy or BlockedConfig) */
    retryOrConfig?: RetryPolicy | BlockedConfig,
  ) {
    super(
      `Blocked: ${reason.type === 'service' ? reason.name : reason.type === 'ended' ? reason.target : reason.indexName}`,
    );
    this.name = 'Blocked';
    this.retryConfig = retryOrConfig;
  }

  /**
   * Backward-compatible getter: returns RetryPolicy if one was provided.
   * Returns undefined if a BlockedConfig was used instead.
   */
  get retry(): RetryPolicy | undefined {
    if (this.retryConfig && isRetryPolicy(this.retryConfig)) {
      return this.retryConfig;
    }
    return undefined;
  }

  /**
   * Returns BlockedConfig if one was provided.
   * Returns undefined if a legacy RetryPolicy was used instead.
   */
  get blockedConfig(): BlockedConfig | undefined {
    if (!this.retryConfig) return undefined;
    if (isRetryPolicy(this.retryConfig)) return undefined;
    return this.retryConfig;
  }

  /** Create a Blocked error for a service being unavailable */
  static service(name: string, retry?: RetryPolicy): Blocked;
  static service(name: string, config?: BlockedConfig): Blocked;
  static service(name: string, retryOrConfig?: RetryPolicy | BlockedConfig): Blocked {
    return new Blocked({ type: 'service', name }, retryOrConfig);
  }

  /** Create a Blocked error waiting for another execution to end */
  static ended(target: string, retry?: RetryPolicy): Blocked;
  static ended(target: string, config?: BlockedConfig): Blocked;
  static ended(target: string, retryOrConfig?: RetryPolicy | BlockedConfig): Blocked {
    return new Blocked({ type: 'ended', target }, retryOrConfig);
  }

  /** Create a Blocked error for an index being rebuilt */
  static index(indexName: string, retry?: RetryPolicy): Blocked;
  static index(indexName: string, config?: BlockedConfig): Blocked;
  static index(indexName: string, retryOrConfig?: RetryPolicy | BlockedConfig): Blocked {
    return new Blocked({ type: 'index', indexName }, retryOrConfig);
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): {
    _tag: 'Blocked';
    reason: BlockedReason;
    retry?: RetryPolicy;
    blockedConfig?: BlockedConfig;
  } {
    return {
      _tag: this._tag,
      reason: this.reason,
      ...(this.retry ? { retry: this.retry } : {}),
      ...(this.blockedConfig ? { blockedConfig: this.blockedConfig } : {}),
    };
  }

  /** Clean output for JSON.stringify */
  toJSON(): {
    _tag: 'Blocked';
    reason: BlockedReason;
    retry?: RetryPolicy;
    blockedConfig?: Omit<BlockedConfig, 'nextRetry'> & { nextRetry?: string };
  } {
    const config = this.blockedConfig;
    return {
      _tag: this._tag,
      reason: this.reason,
      ...(this.retry ? { retry: this.retry } : {}),
      // Closures aren't serializable - represent as string indicator
      ...(config
        ? {
            blockedConfig: {
              ...(config.maxAttempts != null ? { maxAttempts: config.maxAttempts } : {}),
              ...(config.nextRetry ? { nextRetry: '[closure]' } : {}),
            },
          }
        : {}),
    };
  }
}
