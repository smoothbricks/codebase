import { describe, expect, it } from 'bun:test';
import { CodeError } from '../CodeError.js';
import { exponentialBackoff, fixedDelay, linearBackoff, Transient, TransientError } from '../Transient.js';

describe('TransientError', () => {
  describe('class', () => {
    it('should extend CodeError', () => {
      const policy = linearBackoff(3);
      const error = new TransientError('TEST_CODE', { foo: 'bar' }, policy);
      expect(error).toBeInstanceOf(CodeError);
      expect(error).toBeInstanceOf(TransientError);
      expect(error).toBeInstanceOf(Error);
    });

    it('should have name TransientError', () => {
      const error = new TransientError('TEST', {}, linearBackoff(3));
      expect(error.name).toBe('TransientError');
    });

    it('should carry policy', () => {
      const policy = exponentialBackoff(5);
      const error = new TransientError('NETWORK', { service: 'api' }, policy);
      expect(error.policy).toBe(policy);
      expect(error.policy.maxAttempts).toBe(5);
      expect(error.policy.backoff).toBe('exponential');
    });

    it('should include policy in toJSON', () => {
      const policy = fixedDelay(2, 1000);
      const error = new TransientError('RATE_LIMITED', { ms: 1000 }, policy);
      const json = error.toJSON();
      expect(json).toEqual({
        code: 'RATE_LIMITED',
        data: { ms: 1000 },
        policy: {
          backoff: 'fixed',
          maxAttempts: 2,
          baseDelayMs: 1000,
          jitter: false,
        },
      });
    });
  });

  describe('Transient() factory', () => {
    it('should create TransientError with default policy', () => {
      const SERVICE_UNAVAILABLE = Transient<{ status: number }>('SERVICE_UNAVAILABLE', exponentialBackoff(5));
      const error = SERVICE_UNAVAILABLE({ status: 503 });

      expect(error).toBeInstanceOf(TransientError);
      expect(error.code).toBe('SERVICE_UNAVAILABLE');
      expect(error.data).toEqual({ status: 503 });
      expect(error.policy.maxAttempts).toBe(5);
      expect(error.policy.backoff).toBe('exponential');
    });

    it('should have static _tag property', () => {
      const NETWORK_ERROR = Transient<{ service: string }>('NETWORK_ERROR', linearBackoff(3));
      expect(NETWORK_ERROR._tag).toBe('NETWORK_ERROR');
    });

    it('should have static defaultPolicy property', () => {
      const policy = exponentialBackoff(5);
      const TIMEOUT = Transient('TIMEOUT', policy);
      expect(TIMEOUT.defaultPolicy).toBe(policy);
    });

    it('should allow policy override per-call', () => {
      const SERVICE_UNAVAILABLE = Transient<{ status: number }>('SERVICE_UNAVAILABLE', exponentialBackoff(5));

      // Use default
      const error1 = SERVICE_UNAVAILABLE({ status: 503 });
      expect(error1.policy.maxAttempts).toBe(5);
      expect(error1.policy.backoff).toBe('exponential');

      // Override maxAttempts
      const error2 = SERVICE_UNAVAILABLE({ status: 503 }, { maxAttempts: 2 });
      expect(error2.policy.maxAttempts).toBe(2);
      expect(error2.policy.backoff).toBe('exponential'); // Unchanged

      // Override entire policy
      const error3 = SERVICE_UNAVAILABLE({ status: 503 }, fixedDelay(1, 60000));
      expect(error3.policy.maxAttempts).toBe(1);
      expect(error3.policy.backoff).toBe('fixed');
      expect(error3.policy.baseDelayMs).toBe(60000);
    });

    it('should work with void data', () => {
      const TIMEOUT = Transient('TIMEOUT', linearBackoff(3));

      // No args - use default policy
      const error1 = TIMEOUT();
      expect(error1.code).toBe('TIMEOUT');
      expect(error1.data).toBeUndefined();
      expect(error1.policy.maxAttempts).toBe(3);

      // Policy override only (void data)
      const error2 = TIMEOUT({ maxAttempts: 1 });
      expect(error2.data).toBeUndefined();
      expect(error2.policy.maxAttempts).toBe(1);
    });

    it('should enable instanceof checks for retry classification', () => {
      const SERVICE_DOWN = Transient<{ service: string }>('SERVICE_DOWN', exponentialBackoff(5));
      const error = SERVICE_DOWN({ service: 'payment' });

      // This is the key use case - Op class uses this check
      expect(error instanceof TransientError).toBe(true);
      expect(error instanceof CodeError).toBe(true);
    });

    it('should distinguish from regular CodeError', async () => {
      const { Code } = await import('../CodeError.js');

      const TRANSIENT_ERR = Transient<{ reason: string }>('SOME_ERROR', linearBackoff(3));
      const REGULAR_ERR = Code<{ reason: string }>('SOME_ERROR');

      const transientError = TRANSIENT_ERR({ reason: 'network' });
      const regularError = REGULAR_ERR({ reason: 'validation' });

      // Same code, different instanceof
      expect(transientError.code).toBe('SOME_ERROR');
      expect(regularError.code).toBe('SOME_ERROR');

      expect(transientError instanceof TransientError).toBe(true);
      expect(regularError instanceof TransientError).toBe(false);
    });
  });
});
