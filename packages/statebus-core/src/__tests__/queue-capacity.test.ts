import { describe, expect, it } from 'bun:test';
import { ManualStateBus } from '../manual.js';
import { MicrotaskStateBus } from '../microtask.js';

const increment = (payload: number) => ({ topic: 'count', type: 'increment', payload }) as const;
const initialState = { counter: 0, counter1: 0, counter2: 0 };

describe('retained queue capacity', () => {
  it('preserves order across growth and subsequent smaller waves', () => {
    const observed: number[] = [];
    const bus = new ManualStateBus({ initialState, reducers: {} });
    bus.subscribe('count', 'increment', (event) => observed.push(event.payload));
    for (const length of [1, 32, 33, 256, 3, 0, 65]) {
      observed.length = 0;
      for (let i = 0; i < length; i++) expect(bus.publish(increment(i))).toBe(i + 1);
      bus.dispatchEvents();
      expect(observed).toEqual(Array.from({ length }, (_, i) => i));
    }
  });

  it('separates successor waves when both queues grow', () => {
    const observed: number[] = [];
    const bus = new ManualStateBus({ initialState, reducers: {} });
    bus.subscribe('count', 'increment', (event) => {
      observed.push(event.payload);
      if (event.payload === 0) {
        for (let i = 64; i < 128; i++) bus.publish(increment(i));
        bus.dispatchEvents();
      }
    });
    for (let i = 0; i < 64; i++) bus.publish(increment(i));
    bus.dispatchEvents();
    expect(observed).toEqual(Array.from({ length: 128 }, (_, i) => i));
    observed.length = 0;
    bus.publish(increment(128));
    bus.dispatchEvents();
    expect(observed).toEqual([128]);
  });

  it('discards every slot in a partially reduced failed wave', () => {
    const reduced: number[] = [];
    const bus = new ManualStateBus({
      initialState,
      reducers: {
        count: {
          increment: (_state, value) => {
            reduced.push(value);
            if (value === 2) throw new Error('reducer invariant');
          },
        },
      },
    });
    bus.publish(increment(1));
    bus.publish(increment(2));
    bus.publish(increment(3));
    expect(() => bus.dispatchEvents()).toThrow('reducer invariant');
    bus.publish(increment(4));
    bus.dispatchEvents();
    bus.publish(increment(5));
    bus.dispatchEvents();
    expect(reduced).toEqual([1, 2, 4, 5]);
  });

  it('reuses the same scheduler callback across independent batches', () => {
    const queued: (() => void)[] = [];
    const original = globalThis.queueMicrotask;
    globalThis.queueMicrotask = (callback) => {
      queued.push(callback);
    };
    try {
      const bus = new MicrotaskStateBus({ initialState, reducers: {} });
      bus.subscribe('count', 'increment', (event) => {
        if (event.payload === 1) bus.publish(increment(2));
      });
      bus.publish(increment(1));
      expect(queued.length).toBe(1);
      queued[0]();
      expect(queued.length).toBe(1);
      bus.publish(increment(4));
      expect(queued.length).toBe(2);
      expect(queued[1]).toBe(queued[0]);
      queued[1]();
    } finally {
      globalThis.queueMicrotask = original;
    }
  });
});
