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

const isSimplePayload = typia.createIs<SimplePayload>();
const validateSimplePayload = typia.createValidate<SimplePayload>();
const isNestedPayload = typia.createIs<NestedPayload>();

describe('Typia transformer smoke test', () => {
  describe('typia.createIs<T>()', () => {
    it('accepts a correct payload', () => {
      expect(isSimplePayload({ name: 'test', count: 1 })).toBe(true);
    });

    it('rejects wrong field type', () => {
      expect(isSimplePayload({ name: 'test', count: 'nope' })).toBe(false);
    });

    it('rejects missing field', () => {
      expect(isSimplePayload({ name: 'test' })).toBe(false);
    });

    it('accepts extra fields', () => {
      expect(isSimplePayload({ name: 'test', count: 1, extra: true })).toBe(true);
    });

    it('handles nested objects', () => {
      expect(isNestedPayload({ id: 'x', metadata: { tags: ['a', 'b'], active: true } })).toBe(true);
    });

    it('rejects nested type mismatch', () => {
      expect(isNestedPayload({ id: 'x', metadata: { tags: 'not-array', active: true } })).toBe(false);
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

  describe('typia.createValidate<T>()', () => {
    it('returns success and the payload for valid input', () => {
      const payload = { name: 'ok', count: 0 };
      const result = validateSimplePayload(payload);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toEqual(payload);
      }
    });

    it('returns the field error for invalid input', () => {
      const result = validateSimplePayload({ name: 'ok', count: 'bad' });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.errors).toEqual(
          expect.arrayContaining([expect.objectContaining({ expected: 'number', value: 'bad' })]),
        );
      }
    });
  });
});
