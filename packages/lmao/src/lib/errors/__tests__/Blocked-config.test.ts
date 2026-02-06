import { describe, expect, it } from 'bun:test';
import { Blocked, type BlockedConfig, type RetryPolicy } from '../Blocked.js';

describe('Blocked with BlockedConfig', () => {
  describe('Blocked.service', () => {
    it('should work with no config', () => {
      const err = Blocked.service('payment-api');
      expect(err.reason).toEqual({ type: 'service', name: 'payment-api' });
      expect(err.retry).toBeUndefined();
      expect(err.blockedConfig).toBeUndefined();
      expect(err.retryConfig).toBeUndefined();
    });

    it('should work with legacy RetryPolicy (backward compat)', () => {
      const policy: RetryPolicy = { delay: '5s', backoff: 'exponential', maxAttempts: 5 };
      const err = Blocked.service('payment-api', policy);
      expect(err.reason).toEqual({ type: 'service', name: 'payment-api' });
      expect(err.retry).toEqual(policy);
      expect(err.blockedConfig).toBeUndefined();
      expect(err.retryConfig).toBe(policy);
    });

    it('should work with new BlockedConfig (nextRetry closure)', () => {
      const nextRetry = (attempt: number) => 5000 * 2 ** (attempt - 1);
      const config: BlockedConfig = { maxAttempts: 5, nextRetry };
      const err = Blocked.service('payment-api', config);
      expect(err.reason).toEqual({ type: 'service', name: 'payment-api' });
      expect(err.retry).toBeUndefined();
      expect(err.blockedConfig).toEqual(config);
      expect(err.blockedConfig!.nextRetry).toBe(nextRetry);
      expect(err.retryConfig).toBe(config);
    });

    it('should work with BlockedConfig that has only maxAttempts', () => {
      const config: BlockedConfig = { maxAttempts: 3 };
      const err = Blocked.service('payment-api', config);
      // No delay/backoff keys, so not a RetryPolicy
      expect(err.retry).toBeUndefined();
      expect(err.blockedConfig).toEqual(config);
    });

    it('should work with BlockedConfig that has only nextRetry', () => {
      const nextRetry = () => 1000;
      const config: BlockedConfig = { nextRetry };
      const err = Blocked.service('payment-api', config);
      expect(err.retry).toBeUndefined();
      expect(err.blockedConfig).toEqual(config);
      expect(err.blockedConfig!.nextRetry!(1)).toBe(1000);
    });
  });

  describe('Blocked.ended', () => {
    it('should work with BlockedConfig', () => {
      const config: BlockedConfig = { maxAttempts: 10 };
      const err = Blocked.ended('ax-123', config);
      expect(err.reason).toEqual({ type: 'ended', target: 'ax-123' });
      expect(err.blockedConfig).toEqual(config);
      expect(err.retry).toBeUndefined();
    });

    it('should work with legacy RetryPolicy', () => {
      const policy: RetryPolicy = { delay: '1s', maxAttempts: 3 };
      const err = Blocked.ended('ax-123', policy);
      expect(err.retry).toEqual(policy);
      expect(err.blockedConfig).toBeUndefined();
    });
  });

  describe('Blocked.index', () => {
    it('should work with BlockedConfig', () => {
      const nextRetry = (attempt: number) => 1000 * attempt;
      const config: BlockedConfig = { maxAttempts: 5, nextRetry };
      const err = Blocked.index('orders-index', config);
      expect(err.reason).toEqual({ type: 'index', indexName: 'orders-index' });
      expect(err.blockedConfig).toEqual(config);
      expect(err.retry).toBeUndefined();
    });

    it('should work with legacy RetryPolicy', () => {
      const policy: RetryPolicy = { delay: '1s', backoff: 'linear', maxAttempts: 10 };
      const err = Blocked.index('orders-index', policy);
      expect(err.retry).toEqual(policy);
      expect(err.blockedConfig).toBeUndefined();
    });
  });

  describe('.retry getter (backward compat)', () => {
    it('should return RetryPolicy when given RetryPolicy', () => {
      const policy: RetryPolicy = { delay: '2s', backoff: 'fixed' };
      const err = Blocked.service('api', policy);
      expect(err.retry).toBe(policy);
    });

    it('should return undefined when given BlockedConfig', () => {
      const config: BlockedConfig = { nextRetry: () => 1000 };
      const err = Blocked.service('api', config);
      expect(err.retry).toBeUndefined();
    });

    it('should return undefined when no config', () => {
      const err = Blocked.service('api');
      expect(err.retry).toBeUndefined();
    });
  });

  describe('.blockedConfig getter', () => {
    it('should return BlockedConfig when given BlockedConfig', () => {
      const config: BlockedConfig = { maxAttempts: 3, nextRetry: () => 500 };
      const err = Blocked.service('api', config);
      expect(err.blockedConfig).toBe(config);
    });

    it('should return undefined when given RetryPolicy', () => {
      const policy: RetryPolicy = { delay: '5s', backoff: 'exponential' };
      const err = Blocked.service('api', policy);
      expect(err.blockedConfig).toBeUndefined();
    });

    it('should return undefined when no config', () => {
      const err = Blocked.service('api');
      expect(err.blockedConfig).toBeUndefined();
    });
  });

  describe('discrimination: RetryPolicy with only maxAttempts', () => {
    it('should treat { maxAttempts, delay } as RetryPolicy', () => {
      // delay is unique to RetryPolicy
      const config = { maxAttempts: 5, delay: '2s' } as RetryPolicy;
      const err = Blocked.service('api', config);
      expect(err.retry).toBe(config);
      expect(err.blockedConfig).toBeUndefined();
    });

    it('should treat { maxAttempts, backoff } as RetryPolicy', () => {
      // backoff is unique to RetryPolicy
      const config = { maxAttempts: 5, backoff: 'exponential' as const };
      const err = Blocked.service('api', config);
      expect(err.retry).toBe(config);
      expect(err.blockedConfig).toBeUndefined();
    });
  });

  describe('serialization', () => {
    it('should include retry in toJSON for RetryPolicy', () => {
      const policy: RetryPolicy = { delay: '5s', backoff: 'exponential', maxAttempts: 5 };
      const err = Blocked.service('api', policy);
      const json = err.toJSON();
      expect(json).toEqual({
        _tag: 'Blocked',
        reason: { type: 'service', name: 'api' },
        retry: policy,
      });
    });

    it('should include blockedConfig in toJSON for BlockedConfig', () => {
      const config: BlockedConfig = { maxAttempts: 3, nextRetry: () => 1000 };
      const err = Blocked.service('api', config);
      const json = err.toJSON();
      expect(json).toEqual({
        _tag: 'Blocked',
        reason: { type: 'service', name: 'api' },
        blockedConfig: { maxAttempts: 3, nextRetry: '[closure]' },
      });
    });

    it('should omit both in toJSON when no config', () => {
      const err = Blocked.service('api');
      const json = err.toJSON();
      expect(json).toEqual({
        _tag: 'Blocked',
        reason: { type: 'service', name: 'api' },
      });
    });

    it('should include blockedConfig in inspect for BlockedConfig', () => {
      const config: BlockedConfig = { maxAttempts: 3, nextRetry: () => 1000 };
      const err = Blocked.service('api', config);
      const inspected = err[Symbol.for('nodejs.util.inspect.custom')]();
      expect(inspected).toEqual({
        _tag: 'Blocked',
        reason: { type: 'service', name: 'api' },
        blockedConfig: config,
      });
    });

    it('should include retry in inspect for RetryPolicy', () => {
      const policy: RetryPolicy = { delay: '5s' };
      const err = Blocked.service('api', policy);
      const inspected = err[Symbol.for('nodejs.util.inspect.custom')]();
      expect(inspected).toEqual({
        _tag: 'Blocked',
        reason: { type: 'service', name: 'api' },
        retry: policy,
      });
    });
  });

  describe('nextRetry closure captures context', () => {
    it('should use captured Retry-After value on first attempt', () => {
      // Simulates a captured HTTP Retry-After header
      const retryAfterMs = 30000;
      const config: BlockedConfig = {
        maxAttempts: 5,
        nextRetry: (attempt) => {
          if (attempt === 1 && retryAfterMs > 0) return retryAfterMs;
          return 5000 * 2 ** (attempt - 1);
        },
      };

      const err = Blocked.service('payment-api', config);
      const nextRetry = err.blockedConfig!.nextRetry!;

      expect(nextRetry(1)).toBe(30000); // Uses Retry-After
      expect(nextRetry(2)).toBe(10000); // Falls back to exponential
      expect(nextRetry(3)).toBe(20000);
    });
  });

  describe('Error properties', () => {
    it('should be an instance of Error', () => {
      const err = Blocked.service('api');
      expect(err).toBeInstanceOf(Error);
      expect(err).toBeInstanceOf(Blocked);
    });

    it('should have correct message', () => {
      expect(Blocked.service('payment-api').message).toBe('Blocked: payment-api');
      expect(Blocked.ended('ax-123').message).toBe('Blocked: ax-123');
      expect(Blocked.index('orders').message).toBe('Blocked: orders');
    });

    it('should have name "Blocked"', () => {
      expect(Blocked.service('api').name).toBe('Blocked');
    });

    it('should have _tag "Blocked"', () => {
      expect(Blocked.service('api')._tag).toBe('Blocked');
      expect(Blocked._tag).toBe('Blocked');
    });
  });
});
