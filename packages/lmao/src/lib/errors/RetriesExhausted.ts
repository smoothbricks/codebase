/**
 * Retries exhausted error - returned when a Blocked error has exceeded max attempts.
 *
 * When an Op repeatedly returns `Blocked` and the retry config's `maxAttempts`
 * is exhausted, the retry layer converts the final failure to `RetriesExhausted`.
 *
 * Unlike `Blocked`, this represents a permanent failure that callers must handle
 * (e.g., compensate, alert, escalate, return error to caller).
 *
 * @example
 * ```typescript
 * import { defineOpContext, S, Err } from '@smoothbricks/lmao';
 * import { Blocked } from '@smoothbricks/lmao/errors/Blocked';
 * import { RetriesExhausted } from '@smoothbricks/lmao/errors/RetriesExhausted';
 *
 * const { defineOp } = defineOpContext({
 *   logSchema: { service: S.category() },
 *   ctx: { env: null as Env },
 * });
 *
 * // Op that may be blocked by external service
 * const callPaymentApi = defineOp('callPaymentApi', async (ctx, amount: number) => {
 *   const health = await checkHealth('payment-api');
 *   if (!health.available) {
 *     return new Err(Blocked.service('payment-api'));
 *   }
 *   const result = await ctx.deps.http.post('/pay', { amount });
 *   return ctx.ok(result);
 * });
 *
 * // Caller handles permanent failure after retries exhausted
 * const result = await trace('checkout', checkoutOp);
 * if (result.isErr(RetriesExhausted)) {
 *   ctx.log.error(`Payment failed after ${result.error.attempts} attempts`);
 *   return ctx.err('PAYMENT_UNAVAILABLE', { service: result.error.reason });
 * }
 * ```
 */

import type { TaggedError } from '../result.js';
import { type BlockedReason, getBlockedReasonName } from './Blocked.js';

type RetriesExhaustedView = {
  _tag: 'RetriesExhausted';
  reason: BlockedReason;
  attempts: number;
  maxAttempts: number;
};

export class RetriesExhausted extends Error implements TaggedError<'RetriesExhausted'> {
  static readonly _tag = 'RetriesExhausted' as const;

  get _tag(): 'RetriesExhausted' {
    return 'RetriesExhausted';
  }

  constructor(
    /** The original blocked reason */
    readonly reason: BlockedReason,
    /** How many attempts were made */
    readonly attempts: number,
    /** Max attempts that was configured (the limit that was hit) */
    readonly maxAttempts: number,
  ) {
    super(`RetriesExhausted: ${getBlockedReasonName(reason)} after ${attempts} attempts`);
    this.name = 'RetriesExhausted';
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): RetriesExhaustedView {
    return { _tag: this._tag, reason: this.reason, attempts: this.attempts, maxAttempts: this.maxAttempts };
  }

  /** Clean output for JSON.stringify */
  toJSON(): RetriesExhaustedView {
    return { _tag: this._tag, reason: this.reason, attempts: this.attempts, maxAttempts: this.maxAttempts };
  }
}
