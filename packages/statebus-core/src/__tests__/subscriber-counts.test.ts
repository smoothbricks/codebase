import { describe, expect, it } from 'bun:test';
import { mergeSubscriberCounts } from '../subscriber-counts.js';

type Counts = Readonly<Partial<Record<'first' | 'second', number>>>;

// Exhaust the small count domain, including omitted keys and the terminal zero.
const values: Counts[] = [{}];
for (const first of [undefined, 0, 1, 2]) {
  for (const second of [undefined, 0, 1, 2]) {
    values.push({
      ...(first === undefined ? {} : { first }),
      ...(second === undefined ? {} : { second }),
    });
  }
}

describe('subscriber-count coalescing properties', () => {
  it('has an empty identity and does not mutate either input', () => {
    for (const value of values) {
      const frozen = Object.freeze({ ...value });
      expect(mergeSubscriberCounts(frozen, {})).toEqual(value);
      expect(mergeSubscriberCounts({}, frozen)).toEqual(value);
      expect(frozen).toEqual(value);
    }
  });

  it('keeps the latest count independently for each key, including zero', () => {
    for (const previous of values) {
      for (const next of values) {
        const result = mergeSubscriberCounts(Object.freeze({ ...previous }), Object.freeze({ ...next }));
        for (const key of ['first', 'second'] as const) {
          expect(result[key]).toBe(Object.hasOwn(next, key) ? next[key] : previous[key]);
        }
      }
    }
  });

  it('is associative across arbitrary partitions of the count stream', () => {
    for (const first of values) {
      for (const second of values) {
        for (const third of values) {
          expect(mergeSubscriberCounts(mergeSubscriberCounts(first, second), third)).toEqual(
            mergeSubscriberCounts(first, mergeSubscriberCounts(second, third)),
          );
        }
      }
    }
  });
});
