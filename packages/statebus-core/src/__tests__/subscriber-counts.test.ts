import { describe, expect, it } from 'bun:test';
import { mergeSubscriberCounts, SubscriberCountBatch } from '../subscriber-counts.js';

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

// The mutable accumulator owns only unpublished scratch. take() transfers its
// result to the caller; a later wave must never reuse that published record.
describe('wave-owned subscriber count accumulation', () => {
  it('does not allocate a replacement record for an empty or single-payload wave', () => {
    const batch = new SubscriberCountBatch<'first' | 'second'>();
    expect(batch.take()).toBeUndefined();
    const counts = Object.freeze({ first: 1 });
    batch.append(counts);
    expect(batch.take()).toBe(counts);
    expect(batch.take()).toBeUndefined();
  });

  it('matches the pure reference for every three-payload stream', () => {
    const batch = new SubscriberCountBatch<'first' | 'second'>();
    for (const first of values) {
      for (const second of values) {
        for (const third of values) {
          batch.append(Object.freeze(first));
          batch.append(Object.freeze(second));
          batch.append(Object.freeze(third));
          expect(batch.take()).toEqual(mergeSubscriberCounts(mergeSubscriberCounts(first, second), third));
        }
      }
    }
  });

  it('keeps transferred records unchanged across later waves and resets', () => {
    const batch = new SubscriberCountBatch<'first' | 'second'>();
    batch.append({ first: 1 });
    batch.append({ second: 2 });
    const retained = Object.freeze(batch.take());
    batch.append({ first: 0 });
    batch.append({ second: 0 });
    expect(batch.take()).toEqual({ first: 0, second: 0 });
    expect(retained).toEqual({ first: 1, second: 2 });
    batch.append({ first: 9 });
    batch.clear();
    expect(batch.take()).toBeUndefined();
  });

  it('copies only own keys and treats __proto__ as data, not a prototype mutation', () => {
    const batch = new SubscriberCountBatch<string>();
    batch.append({ first: 1 });
    batch.append({ second: 2 });
    batch.append({ ['__proto__']: 0, constructor: 3 });
    const result = batch.take();
    expect(result).toEqual({ first: 1, second: 2, ['__proto__']: 0, constructor: 3 });
    expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
  });
});
