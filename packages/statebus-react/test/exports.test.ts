import { describe, expect, it } from 'bun:test';
import { ManualStateBus } from '../src';

declare module '@smoothbricks/statebus-core' {
  export interface States {
    counter: number;
  }
  export interface Events {
    test: {
      increment: number;
    };
  }
}

describe('StateBus React exports', () => {
  it('should export ManualStateBus for testing', () => {
    const bus = new ManualStateBus({
      initialState: { counter: 0 },
      reducers: {
        test: {
          increment: (state, payload) => {
            state.counter.update((v) => v + payload);
          },
        },
      },
    });

    expect(bus.state.counter.get()).toBe(0);

    // Publish an event - it should be queued
    bus.publish({ topic: 'test', type: 'increment', payload: 5 });

    // State shouldn't change until manual dispatch
    expect(bus.state.counter.get()).toBe(0);

    // Manually dispatch events
    bus.dispatchEvents();

    // Now state should be updated
    expect(bus.state.counter.get()).toBe(5);
  });
});
