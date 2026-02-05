import { describe, expect, it } from 'bun:test';
import { exponentialBackoff, fixedDelay, linearBackoff, mergePolicy } from '../retry-policy.js';

describe('RetryPolicy helpers', () => {
  describe('linearBackoff', () => {
    it('should create linear policy with defaults', () => {
      const policy = linearBackoff(3);
      expect(policy).toEqual({
        backoff: 'linear',
        maxAttempts: 3,
        baseDelayMs: 100,
        jitter: true,
      });
    });

    it('should accept custom baseDelayMs', () => {
      const policy = linearBackoff(5, 200);
      expect(policy.maxAttempts).toBe(5);
      expect(policy.baseDelayMs).toBe(200);
    });
  });

  describe('exponentialBackoff', () => {
    it('should create exponential policy with defaults', () => {
      const policy = exponentialBackoff(5);
      expect(policy).toEqual({
        backoff: 'exponential',
        maxAttempts: 5,
        baseDelayMs: 100,
        jitter: true,
      });
    });

    it('should accept custom baseDelayMs', () => {
      const policy = exponentialBackoff(3, 50);
      expect(policy.maxAttempts).toBe(3);
      expect(policy.baseDelayMs).toBe(50);
    });
  });

  describe('fixedDelay', () => {
    it('should create fixed policy with defaults', () => {
      const policy = fixedDelay(3);
      expect(policy).toEqual({
        backoff: 'fixed',
        maxAttempts: 3,
        baseDelayMs: 1000,
        jitter: false,
      });
    });

    it('should accept custom delayMs', () => {
      const policy = fixedDelay(1, 60000);
      expect(policy.maxAttempts).toBe(1);
      expect(policy.baseDelayMs).toBe(60000);
    });
  });

  describe('mergePolicy', () => {
    it('should return base when no override', () => {
      const base = linearBackoff(3);
      expect(mergePolicy(base)).toBe(base);
      expect(mergePolicy(base, undefined)).toBe(base);
    });

    it('should merge override fields', () => {
      const base = exponentialBackoff(5);
      const merged = mergePolicy(base, { maxAttempts: 3 });
      expect(merged.maxAttempts).toBe(3);
      expect(merged.backoff).toBe('exponential');
      expect(merged.baseDelayMs).toBe(100);
    });

    it('should override multiple fields', () => {
      const base = linearBackoff(5, 100);
      const merged = mergePolicy(base, { maxAttempts: 2, baseDelayMs: 500, jitter: false });
      expect(merged).toEqual({
        backoff: 'linear',
        maxAttempts: 2,
        baseDelayMs: 500,
        jitter: false,
      });
    });
  });
});
