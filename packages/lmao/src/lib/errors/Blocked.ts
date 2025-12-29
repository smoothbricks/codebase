/**
 * Blocked error - indicates an Op cannot proceed due to a temporary condition.
 *
 * A tagged error type for use with `result.isErr(Blocked)` to discriminate
 * blocked states from other errors. Useful for retry logic and dependency waiting.
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
 * Retry policy for blocked errors.
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

export class Blocked extends Error implements TaggedError<'Blocked'> {
  static readonly _tag = 'Blocked' as const;

  get _tag(): 'Blocked' {
    return 'Blocked';
  }

  constructor(
    /** The reason this Op is blocked */
    readonly reason: BlockedReason,
    /** Optional retry policy override (engine has defaults per reason type) */
    readonly retry?: RetryPolicy,
  ) {
    super(
      `Blocked: ${reason.type === 'service' ? reason.name : reason.type === 'ended' ? reason.target : reason.indexName}`,
    );
    this.name = 'Blocked';
  }

  /** Create a Blocked error for a service being unavailable */
  static service(name: string, retry?: RetryPolicy): Blocked {
    return new Blocked({ type: 'service', name }, retry);
  }

  /** Create a Blocked error waiting for another execution to end */
  static ended(target: string, retry?: RetryPolicy): Blocked {
    return new Blocked({ type: 'ended', target }, retry);
  }

  /** Create a Blocked error for an index being rebuilt */
  static index(indexName: string, retry?: RetryPolicy): Blocked {
    return new Blocked({ type: 'index', indexName }, retry);
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): { _tag: 'Blocked'; reason: BlockedReason; retry?: RetryPolicy } {
    return { _tag: this._tag, reason: this.reason, retry: this.retry };
  }

  /** Clean output for JSON.stringify */
  toJSON(): { _tag: 'Blocked'; reason: BlockedReason; retry?: RetryPolicy } {
    return { _tag: this._tag, reason: this.reason, retry: this.retry };
  }
}
