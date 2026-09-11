import { describe, expect, it } from 'bun:test';
import { computed, ManualStateBus, viewPropsIdentity } from '../index.js';
import { initialTestState } from './test-state.js';

//*
declare module '@smoothbricks/statebus-core' {
  export interface States {
    counter1: number;
    counter2: number;
  }
  // See test/index.test.ts for the related Events declaration
  export interface CountEvents {
    increment1: number;
    increment2: number;
  }
}
//*/

describe('Computed States', () => {
  it('should properly compute derived state', () => {
    const bus = new ManualStateBus({
      initialState: initialTestState(),
      reducers: {
        count: (state, event) => {
          state.counter.update((v) => v + event.payload);
        },
      },
    });

    const doubledCounter = computed(bus, 'doubledCounter', () => bus.state.counter.get() * 2, undefined);

    expect(doubledCounter.get()).toBe(0);

    bus.publish({ topic: 'count', type: 'increment', payload: 5 });
    bus.dispatchEvents();

    expect(doubledCounter.get()).toBe(10);
  });

  it('should update computed values when dependencies change', () => {
    const bus = new ManualStateBus({
      initialState: initialTestState(),
      reducers: (state, event) => {
        switch (event.type) {
          case 'increment1':
            state.counter1.update((v) => v + event.payload);
            break;
          case 'increment2':
            state.counter2.update((v) => v + event.payload);
            break;
        }
      },
    });

    const sum = computed(bus, 'sum', () => bus.state.counter1.get() + bus.state.counter2.get(), undefined);

    expect(sum.get()).toBe(0);

    bus.publish({ topic: 'count', type: 'increment1', payload: 3 });
    bus.publish({ topic: 'count', type: 'increment2', payload: 2 });
    bus.dispatchEvents();

    expect(sum.get()).toBe(5);
  });
});

describe('computed prop identity properties', () => {
  it('preserves Object.is distinctions for every scalar prop in the finite domain', () => {
    const values = [
      undefined,
      null,
      '',
      '0',
      '-0',
      'NaN',
      'Infinity',
      0,
      -0,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      7,
    ];
    for (const left of values)
      for (const right of values) {
        expect(viewPropsIdentity(left) === viewPropsIdentity(right)).toBe(Object.is(left, right));
        expect(viewPropsIdentity({ value: left }) === viewPropsIdentity({ value: right })).toBe(Object.is(left, right));
      }
  });
  it('ignores object insertion order but distinguishes changed key sets and delimiter-containing keys', () => {
    for (const value of [undefined, null, '', 'a=b,c=d', 7]) {
      expect(viewPropsIdentity({ first: value, second: 'x' })).toBe(viewPropsIdentity({ second: 'x', first: value }));
      expect(viewPropsIdentity({ first: value })).not.toBe(viewPropsIdentity({ first: value, second: undefined }));
      expect(viewPropsIdentity({ 'a=b,c': value })).not.toBe(viewPropsIdentity({ a: value, 'b,c': undefined }));
    }
  });
});
