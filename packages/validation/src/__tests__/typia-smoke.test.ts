import { describe, expect, it } from 'bun:test';
import typia from 'typia';

interface SimplePayload {
  name: string;
  count: number;
}

interface NestedPayload {
  id: string;
  metadata: { tags: string[]; active: boolean };
}

describe('Typia transformer smoke test', () => {
  describe('typia.is<T>()', () => {
    it('accepts a correct payload', () => {
      expect(typia.is<SimplePayload>({ name: 'test', count: 1 })).toBe(true);
    });

    it('rejects wrong field type', () => {
      expect(typia.is<SimplePayload>({ name: 'test', count: 'nope' })).toBe(false);
    });

    it('rejects missing field', () => {
      expect(typia.is<SimplePayload>({ name: 'test' })).toBe(false);
    });

    it('accepts extra fields', () => {
      expect(typia.is<SimplePayload>({ name: 'test', count: 1, extra: true })).toBe(true);
    });

    it('handles nested objects', () => {
      expect(typia.is<NestedPayload>({ id: 'x', metadata: { tags: ['a', 'b'], active: true } })).toBe(true);
    });

    it('rejects nested type mismatch', () => {
      expect(typia.is<NestedPayload>({ id: 'x', metadata: { tags: 'not-array', active: true } })).toBe(false);
    });
  });

  describe('typia.assert<T>()', () => {
    it('returns the value on success', () => {
      const result = typia.assert<SimplePayload>({ name: 'hello', count: 42 });
      expect(result.name).toBe('hello');
      expect(result.count).toBe(42);
    });

    it('throws on invalid input', () => {
      expect(() => typia.assert<SimplePayload>({ name: 123 })).toThrow();
    });
  });

  describe('typia.validate<T>()', () => {
    it('returns success for valid input', () => {
      const result = typia.validate<SimplePayload>({ name: 'ok', count: 0 });
      expect(result.success).toBe(true);
    });

    it('returns errors for invalid input', () => {
      const result = typia.validate<SimplePayload>({ name: 'ok', count: 'bad' });
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.errors.length).toBeGreaterThan(0);
      }
    });
  });
});
