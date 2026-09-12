import { describe, expect, it } from 'bun:test';
import { MicrotaskStateBus } from '../microtask.js';
import { initialTestState } from './test-state.js';

function createBus() {
  return new MicrotaskStateBus({
    initialState: initialTestState(),
    reducers: { count: { increment: (state, amount) => state.counter.update((value) => value + amount) } },
  });
}

const increment = (payload: number) => ({ topic: 'count', type: 'increment', payload }) as const;

describe('microtask scheduling', () => {
  it('reduces a complete wave before notifying listeners without waiting for a paint', async () => {
    const bus = createBus();
    const observed: number[] = [];
    bus.subscribe('count', 'increment', () => observed.push(bus.state.counter.get()));
    bus.publish(increment(1));
    bus.publish(increment(2));
    expect(bus.state.counter.get()).toBe(0);
    await Promise.resolve();
    expect(observed).toEqual([3, 3]);
    expect(bus.state.counter.get()).toBe(3);
  });

  it('does not duplicate delivery after an explicit manual flush', async () => {
    const bus = createBus();
    let calls = 0;
    bus.subscribe('count', 'increment', () => {
      calls += 1;
    });
    bus.publish(increment(1));
    bus.dispatchEvents();
    await Promise.resolve();
    expect(calls).toBe(1);
    expect(bus.state.counter.get()).toBe(1);
  });

  it('keeps listener publications in the following wave', async () => {
    const bus = createBus();
    const observed: number[] = [];
    bus.subscribe('count', 'increment', (event) => {
      observed.push(bus.state.counter.get());
      if (event.payload === 1) bus.publish(increment(4));
    });
    bus.publish(increment(1));
    bus.publish(increment(2));
    await Promise.resolve();
    expect(observed).toEqual([3, 3, 7]);
  });
});

it('drains listener-published waves without scheduling another empty microtask', async () => {
  class ObservedBus extends MicrotaskStateBus {
    flushes = 0;
    override dispatchEvents() {
      this.flushes += 1;
      super.dispatchEvents();
    }
  }
  const bus = new ObservedBus({
    initialState: initialTestState(),
    reducers: {},
  });
  bus.subscribe('count', 'increment', (event) => {
    if (event.payload < 4) bus.publish(increment(event.payload + 1));
  });
  bus.publish(increment(1));
  await Promise.resolve();
  await Promise.resolve();
  expect(bus.flushes).toBe(1);
});
