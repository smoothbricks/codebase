/**
 * Blocked error - indicates an Op cannot proceed due to a temporary condition.
 *
 * A tagged error type for use with `result.isErr(Blocked)` to discriminate
 * blocked states from other errors. Useful for retry logic and dependency waiting.
 *
 * Uses BlockedConfig for retry configuration (closure-based nextRetry).
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
 * Convert a blocked reason union into its display name.
 */
export function getBlockedReasonName(reason: BlockedReason): string {
  switch (reason.type) {
    case 'service':
      return reason.name;
    case 'ended':
      return reason.target;
    case 'index':
      return reason.indexName;
  }
}

/**
 * Engine-level retry configuration for blocked operations.
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

type BlockedInspectView = {
  _tag: 'Blocked';
  reason: BlockedReason;
  blockedConfig?: BlockedConfig;
};

type BlockedJsonView = {
  _tag: 'Blocked';
  reason: BlockedReason;
  blockedConfig?: Omit<BlockedConfig, 'nextRetry'> & { nextRetry?: string };
};

export class Blocked extends Error implements TaggedError<'Blocked'> {
  static readonly _tag = 'Blocked' as const;

  get _tag(): 'Blocked' {
    return 'Blocked';
  }

  /** Retry configuration */
  readonly blockedConfig: BlockedConfig | undefined;

  constructor(
    /** The reason this Op is blocked */
    readonly reason: BlockedReason,
    /** Optional retry configuration */
    config?: BlockedConfig,
  ) {
    super(`Blocked: ${getBlockedReasonName(reason)}`);
    this.name = 'Blocked';
    this.blockedConfig = config;
  }

  /** Create a Blocked error for a service being unavailable */
  static service(name: string, config?: BlockedConfig): Blocked {
    return new Blocked({ type: 'service', name }, config);
  }

  /** Create a Blocked error waiting for another execution to end */
  static ended(target: string, config?: BlockedConfig): Blocked {
    return new Blocked({ type: 'ended', target }, config);
  }

  /** Create a Blocked error for an index being rebuilt */
  static index(indexName: string, config?: BlockedConfig): Blocked {
    return new Blocked({ type: 'index', indexName }, config);
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): BlockedInspectView {
    return {
      _tag: this._tag,
      reason: this.reason,
      ...(this.blockedConfig ? { blockedConfig: this.blockedConfig } : {}),
    };
  }

  /** Clean output for JSON.stringify */
  toJSON(): BlockedJsonView {
    const config = this.blockedConfig;
    return {
      _tag: this._tag,
      reason: this.reason,
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
