import { describe, expect, it } from 'bun:test';
import { ManualStateBus } from '../manual.js';

//*
declare module '@smoothbricks/statebus-core' {
  export interface States {
    counter: number;
  }
  export interface Events {
    count: CountEvents;
  }
  interface CountEvents {
    increment: number;
    decrement: number;
  }
}
//*/

describe('ManualStateBus', () => {
  it('should initialize with correct initial state', () => {
    const bus = new ManualStateBus({
      initialState: { counter: 0, counter1: 0, counter2: 0 },
      reducers: {
        count: {
          increment: () => {},
        },
      },
    });
    expect(bus.state.counter.get()).toBe(0);
  });

  it('should handle events and update state through reducers', () => {
    const bus = new ManualStateBus({
      initialState: { counter: 0, counter1: 0, counter2: 0 },
      reducers: {
        statebus: {
          substateInterest: (state, event) => {
            state.counter.update((v) => v + (event.subscribers.counter ?? 0));
          },
        },
      },
    });

    bus.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers: { counter: 1 } } } as const);
    bus.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers: { counter: 2 } } } as const);
    bus.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers: { counter: 3 } } } as const);
    bus.dispatchEvents();

    expect(bus.state.counter.get()).toBe(6);
  });

  it('should properly handle event listeners', () => {
    const bus = new ManualStateBus({
      initialState: { counter: 0, counter1: 0, counter2: 0 },
      reducers: {},
    });

    let listenerCalled = false;
    bus.subscribe('count', 'increment', () => {
      listenerCalled = true;
    });

    bus.publish({ topic: 'count', type: 'increment', payload: 1 });
    bus.dispatchEvents();

    expect(listenerCalled).toBe(true);
  });
});
