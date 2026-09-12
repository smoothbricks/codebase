import { describe, expect, it } from 'bun:test';
import { captureViewProps, computed, ManualStateBus, sameViewProps } from '../index.js';
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
        expect(sameViewProps(left, right)).toBe(Object.is(left, right));
        expect(sameViewProps({ value: left }, { value: right })).toBe(Object.is(left, right));
      }
  });
  it('ignores object insertion order but distinguishes changed key sets and delimiter-containing keys', () => {
    for (const value of [undefined, null, '', 'a=b,c=d', 7]) {
      expect(sameViewProps({ first: value, second: 'x' }, { second: 'x', first: value })).toBe(true);
      expect(sameViewProps({ first: value }, { first: value, second: undefined })).toBe(false);
      expect(sameViewProps({ 'a=b,c': value }, { a: value, 'b,c': undefined })).toBe(false);
    }
  });
});

it('captures caller-owned prop values and keeps reserved keys as data', () => {
  const props = { ['__proto__']: 7, value: 'before' };
  const captured = captureViewProps(props);
  props.value = 'after';
  expect(captured.value).toBe('before');
  expect(Object.hasOwn(captured, '__proto__')).toBe(true);
  expect(sameViewProps(captured, props)).toBe(false);
  expect(sameViewProps({}, { missing: undefined })).toBe(false);
  expect(sameViewProps(null, {})).toBe(false);
  expect(captureViewProps(7)).toBe(7);
});
