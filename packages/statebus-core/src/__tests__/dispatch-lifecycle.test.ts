import { describe, expect, it } from 'bun:test';
import { ManualStateBus } from '../manual.js';
import type { Event, StateBusConfig } from '../types.js';
import { initialTestState } from './test-state.js';

const initialState = initialTestState();

function createBus(reducers: StateBusConfig['reducers'] = {}) {
  return new ManualStateBus({ initialState, reducers });
}

const increment = (payload: number) => ({ topic: 'count', type: 'increment', payload }) as const;

describe('event listener lifecycle', () => {
  it('removes the first registered listener without removing a later listener', () => {
    const bus = createBus();
    const calls: string[] = [];
    const unsubscribe = bus.subscribe('count', 'increment', () => calls.push('first'));
    bus.subscribe('count', 'increment', () => calls.push('second'));
    unsubscribe();
    unsubscribe();
    bus.publish(increment(1));
    bus.dispatchEvents();
    expect(calls).toEqual(['second']);
  });
});

describe('state interest lifecycle', () => {
  it('publishes the final zero and makes cleanup idempotent', () => {
    const bus = createBus();
    const observed: number[] = [];
    bus.subscribe('statebus', 'substateInterest', (event) => {
      const value = event.payload.subscribers.counter;
      if (value !== undefined) observed.push(value);
    });
    const first = bus.substateInterest(['counter']);
    const second = bus.substateInterest(['counter']);
    bus.dispatchEvents();
    first();
    first();
    bus.dispatchEvents();
    expect(bus.substateInterestCount.get('counter')).toBe(1);
    second();
    second();
    bus.dispatchEvents();
    expect(bus.substateInterestCount.has('counter')).toBe(false);
    expect(observed).toEqual([2, 1, 0]);
  });

  it('captures the subscribed keys instead of retaining the caller-owned array', () => {
    const bus = createBus();
    const keys: ('counter' | 'counter1')[] = ['counter'];
    const unsubscribe = bus.substateInterest(keys);
    keys[0] = 'counter1';
    unsubscribe();
    bus.dispatchEvents();
    expect(bus.substateInterestCount.size).toBe(0);
  });

  it('coalesces independent keys and StrictMode churn into their final counts', () => {
    const bus = createBus();
    const observed: Event<'statebus', 'substateInterest'>['payload'][] = [];
    bus.subscribe('statebus', 'substateInterest', (event) => observed.push(event.payload));
    const release = bus.substateInterest(['counter']);
    release();
    bus.substateInterest(['counter']);
    bus.substateInterest(['counter1']);
    bus.dispatchEvents();
    expect(observed.map((payload) => payload.subscribers)).toEqual([{ counter: 1, counter1: 1 }]);
  });

  it('does not mutate the published events while coalescing', () => {
    const bus = createBus();
    const first = Object.freeze({
      topic: 'statebus',
      type: 'substateInterest',
      payload: Object.freeze({ subscribers: Object.freeze({ counter: 1 }) }),
    } as const);
    const second = Object.freeze({
      topic: 'statebus',
      type: 'substateInterest',
      payload: Object.freeze({ subscribers: Object.freeze({ counter1: 2 }) }),
    } as const);
    const observed: Event<'statebus', 'substateInterest'>['payload'][] = [];
    bus.subscribe('statebus', 'substateInterest', (event) => observed.push(event.payload));
    bus.publish(first);
    bus.publish(second);
    bus.dispatchEvents();
    expect(first.payload.subscribers).toEqual({ counter: 1 });
    expect(second.payload.subscribers).toEqual({ counter1: 2 });
    expect(observed.map((payload) => payload.subscribers)).toEqual([{ counter: 1, counter1: 2 }]);
  });

  it('does not redispatch an interest event in a subsequent event wave', () => {
    const bus = createBus();
    let interestCalls = 0;
    let incrementCalls = 0;
    bus.subscribe('statebus', 'substateInterest', () => {
      interestCalls += 1;
      // Bound the regression: the broken implementation otherwise loops forever.
      if (interestCalls < 3) bus.publish(increment(1));
    });
    bus.subscribe('count', 'increment', () => {
      incrementCalls += 1;
    });
    bus.substateInterest(['counter']);
    bus.dispatchEvents();
    expect(interestCalls).toBe(1);
    expect(incrementCalls).toBe(1);
  });
});

describe('dispatch waves', () => {
  it('reduces the complete wave before listeners and defers reentrant dispatch', () => {
    const observed: number[] = [];
    const bus = createBus({ count: { increment: (state, amount) => state.counter.update((value) => value + amount) } });
    bus.subscribe('count', 'increment', (event) => {
      observed.push(bus.state.counter.get());
      if (event.payload === 1) {
        bus.publish(increment(4));
        bus.dispatchEvents();
      }
    });
    bus.publish(increment(1));
    bus.publish(increment(2));
    bus.dispatchEvents();
    expect(observed).toEqual([3, 3, 7]);
  });
});
