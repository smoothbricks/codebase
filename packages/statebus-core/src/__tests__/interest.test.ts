import { describe, expect, it } from 'bun:test';
import {
  mergeStateInterests,
  type StateInterest,
  type StateInterestChange,
  StateInterestRegistry,
  stateInterestKey,
} from '../interest.js';
import { ManualStateBus } from '../manual.js';
import { initialTestState } from './test-state.js';

function createBus() {
  return new ManualStateBus({ initialState: initialTestState(), reducers: {} });
}

const addresses: readonly StateInterest[] = [
  { key: 'records' },
  { key: 'records', id: 7 },
  { key: 'records', id: '7' },
  { key: 'records', id: 'a.b/[x]' },
  { key: 'records.a', id: 'b/[x]' },
];

describe('exact interest identity and count properties', () => {
  it('distinguishes scalar, numeric, string and delimiter-containing addresses', () => {
    expect(new Set(addresses.map(stateInterestKey)).size).toBe(addresses.length);
  });

  it('merges counts associatively with latest values, including zero, without mutating inputs', () => {
    const changes = addresses.flatMap((interest) =>
      [0, 1, 2].map((subscribers) => Object.freeze({ interest, subscribers })),
    );
    for (const first of changes)
      for (const second of changes)
        for (const third of changes) {
          const left = mergeStateInterests(mergeStateInterests([first], [second]), [third]);
          const right = mergeStateInterests([first], mergeStateInterests([second], [third]));
          expect(left).toEqual(right);
          expect(left.find((value) => stateInterestKey(value.interest) === stateInterestKey(third.interest))).toBe(
            third,
          );
        }
  });

  it('keeps counts equal to active leases for every five-step acquire/release sequence', () => {
    for (let sequence = 0; sequence < 1024; sequence += 1) {
      const events: StateInterestChange[] = [];
      const registry = new StateInterestRegistry((changes) => events.push(...changes));
      const leases: { release: () => void; interest: StateInterest; active: boolean }[] = [];
      let choices = sequence;
      for (let step = 0; step < 5; step += 1) {
        const action = choices % 4;
        choices = Math.floor(choices / 4);
        if (action < 2) {
          const interest: StateInterest = { key: 'records', id: action === 0 ? 7 : '7' };
          leases.push({ release: registry.acquire([interest]), interest, active: true });
        } else {
          const lease = action === 2 ? leases[0] : leases.at(-1);
          lease?.release();
          if (lease) lease.active = false;
        }
        for (const id of [7, '7']) {
          expect(registry.count({ key: 'records', id })).toBe(
            leases.filter((lease) => lease.active && lease.interest.id === id).length,
          );
        }
        expect(events.every((event) => event.subscribers >= 0)).toBe(true);
      }
      for (const lease of leases) {
        lease.release();
        lease.release();
      }
      expect(registry.snapshot()).toEqual([]);
    }
  });

  it('refuses an invalid lease atomically and copies mutable address descriptors', () => {
    const bus = createBus();
    expect(() =>
      bus.substateInterest([
        { key: 'records', id: 7 },
        { key: 'records', id: Number.NaN },
      ]),
    ).toThrow(RangeError);
    expect(bus.getStateInterests()).toEqual([]);
    expect(bus.substateInterestCount.size).toBe(0);
    const descriptor: { key: 'records'; id: string | number } = { key: 'records', id: 7 };
    const release = bus.substateInterest([descriptor]);
    descriptor.id = 'changed';
    release();
    expect(bus.getStateInterests()).toEqual([]);
  });
});

describe('StateBus exact interest lifecycle', () => {
  it('coalesces final exact counts per wave and reports terminal zero', () => {
    const bus = createBus();
    const observed: (readonly StateInterestChange[])[] = [];
    bus.subscribe('statebus', 'substateInterest', (event) => observed.push(event.payload.changes ?? []));
    const first = bus.substateInterest([{ key: 'records', id: 7 }]);
    const second = bus.substateInterest([{ key: 'records', id: 7 }]);
    const text = bus.substateInterest([{ key: 'records', id: '7' }]);
    first();
    bus.dispatchEvents();
    expect(observed[0]).toEqual([
      { interest: { key: 'records', id: 7 }, subscribers: 1 },
      { interest: { key: 'records', id: '7' }, subscribers: 1 },
    ]);
    second();
    second();
    text();
    bus.dispatchEvents();
    expect(observed[1]).toEqual([
      { interest: { key: 'records', id: 7 }, subscribers: 0 },
      { interest: { key: 'records', id: '7' }, subscribers: 0 },
    ]);
    expect(bus.getStateInterests()).toEqual([]);
  });

  it('preserves an interested signal through removal/recreation and StrictMode churn', () => {
    const bus = createBus();
    const first = bus.substateInterest([{ key: 'records', id: 7 }]);
    const signal = bus.state.records.get(7);
    signal.set('before');
    expect(bus.state.records.remove(7)).toBe(true);
    expect(signal.get()).toBeUndefined();
    expect(bus.state.records.get(7)).toBe(signal);
    first();
    const remount = bus.substateInterest([{ key: 'records', id: 7 }]);
    bus.dispatchEvents();
    expect(bus.state.records.get(7)).toBe(signal);
    signal.set('after');
    expect(bus.state.records.get(7).get()).toBe('after');
    remount();
    bus.dispatchEvents();
    // Unsubscribing must not evict application data.
    expect(bus.state.records.get(7)).toBe(signal);
    bus.state.records.remove(7);
    expect(bus.state.records.get(7)).not.toBe(signal);
  });

  it('releases removed empty signal storage after the last interested subscriber leaves', () => {
    const bus = createBus();
    const release = bus.substateInterest([{ key: 'records', id: 'empty' }]);
    const signal = bus.state.records.get('empty');
    bus.state.records.remove('empty');
    release();
    bus.dispatchEvents();
    expect(bus.state.records.get('empty')).not.toBe(signal);
  });
});
