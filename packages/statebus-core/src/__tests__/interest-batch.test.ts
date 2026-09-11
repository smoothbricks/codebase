import { describe, expect, it } from 'bun:test';
import {
  type StateInterest,
  StateInterestBatch,
  type StateInterestChange,
  type StateInterestKey,
  StateInterestMap,
  StateInterestRegistry,
  stateInterestKey,
} from '../interest.js';
import { ManualStateBus } from '../manual.js';
import { initialTestState } from './test-state.js';

const addresses: readonly StateInterest[] = [
  { key: 'records' },
  { key: 'records', id: 0 },
  { key: 'records', id: '0' },
  { key: '__proto__', id: '' },
  { key: 'a/b', id: 'c' },
  { key: 'a', id: 'b/c' },
];

describe('structural interest index', () => {
  it('preserves scalar, numeric, string, delimiter and reserved-property identities', () => {
    const map = new StateInterestMap<number | undefined>();
    for (let index = 0; index < addresses.length; index += 1) map.set(addresses[index], index);
    expect(map.size).toBe(addresses.length);
    for (let index = 0; index < addresses.length; index += 1) expect(map.get(addresses[index])).toBe(index);
    map.set(addresses[0], undefined);
    expect(map.has(addresses[0])).toBe(true);
    expect(map.size).toBe(addresses.length);
    for (const address of addresses) {
      expect(map.delete(address)).toBe(true);
      expect(map.delete(address)).toBe(false);
    }
    expect(map.size).toBe(0);
    expect(Array.from(map.values())).toEqual([]);
  });

  it('exposes a nominal wire key rather than accepting arbitrary strings', () => {
    const key: StateInterestKey = stateInterestKey(addresses[0]);
    // @ts-expect-error wire keys must come from the address codec
    const unvalidated: StateInterestKey = 'arbitrary';
    expect(typeof key).toBe('string');
    expect(unvalidated).not.toBe(key);
  });
});

describe('single-pass exact-interest accumulation', () => {
  it('matches the reference over every three-change stream and partition', () => {
    const changes = addresses.flatMap((interest) =>
      [0, 1, 2].map((subscribers) => Object.freeze({ interest, subscribers })),
    );
    const batch = new StateInterestBatch();
    for (const a of changes)
      for (const b of changes)
        for (const c of changes) {
          const expected = Array.from(
            new Map([a, b, c].map((change) => [stateInterestKey(change.interest), change])).values(),
          );
          batch.append([a]);
          batch.append([b, c]);
          expect(batch.take()).toEqual(expected);
          batch.append([a, b]);
          batch.append([c]);
          expect(batch.take()).toEqual(expected);
        }
  });

  it('transfers each output once and never mutates a retained snapshot', () => {
    const batch = new StateInterestBatch();
    const first = Object.freeze([{ interest: addresses[0], subscribers: 1 }]);
    batch.append(first);
    expect(batch.take()).toBe(first);
    batch.append(first);
    batch.append([{ interest: addresses[1], subscribers: 2 }]);
    const retained = Object.freeze(batch.take());
    batch.append([{ interest: addresses[0], subscribers: 0 }]);
    batch.append([{ interest: addresses[1], subscribers: 0 }]);
    expect(batch.take().map((c) => c.subscribers)).toEqual([0, 0]);
    expect(retained).toEqual([first[0], { interest: addresses[1], subscribers: 2 }]);
    expect(batch.take()).toEqual([]);
  });

  it('bounds cached addresses after high-cardinality waves and recovers after clear', () => {
    const batch = new StateInterestBatch(16);
    for (let wave = 0; wave < 8; wave += 1) {
      const changes = Array.from({ length: 128 }, (_, id) => ({
        interest: { key: 'records', id: wave * 128 + id },
        subscribers: id,
      }));
      for (const change of changes) batch.append([change]);
      expect(batch.take()).toEqual(changes);
      expect(batch.retainedAddresses).toBeLessThanOrEqual(16);
    }
    batch.append([{ interest: addresses[0], subscribers: 9 }]);
    batch.clear();
    expect(batch.take()).toEqual([]);
    batch.append([{ interest: addresses[0], subscribers: 0 }]);
    expect(batch.take()[0]?.subscribers).toBe(0);
  });

  it('needs no JSON encoding for runtime counts, lease changes or dispatch', () => {
    const bus = new ManualStateBus({ initialState: initialTestState(), reducers: {} });
    let notifications = 0;
    bus.subscribe('statebus', 'substateInterest', () => {
      notifications += 1;
    });
    const original = JSON.stringify;
    JSON.stringify = () => {
      throw new Error('Encoding on a runtime interest path');
    };
    try {
      const a = bus.substateInterest([{ key: 'records', id: 7 }]);
      const b = bus.substateInterest([{ key: 'records', id: '7' }]);
      bus.state.records.get(7).set('value');
      bus.state.records.remove(7);
      bus.dispatchEvents();
      a();
      b();
      bus.dispatchEvents();
    } finally {
      JSON.stringify = original;
    }
    expect(notifications).toBe(2);
    expect(bus.getStateInterests()).toEqual([]);
  });

  it('preserves final counts for duplicate leases, validates atomically, and keeps outputs owned', () => {
    const published: (readonly StateInterestChange[])[] = [];
    const registry = new StateInterestRegistry((changes) => published.push(Object.freeze(changes)));
    const release = registry.acquire([addresses[0], addresses[0]]);
    expect(registry.count(addresses[0])).toBe(2);
    expect(registry.propertyCounts.get('records')).toBe(2);
    expect(() => registry.acquire([addresses[0], { key: 'records', id: Number.POSITIVE_INFINITY }])).toThrow(
      RangeError,
    );
    expect(registry.count(addresses[0])).toBe(2);
    release();
    release();
    expect(registry.snapshot()).toEqual([]);
    expect(registry.propertyCounts.size).toBe(0);
    expect(published.map((changes) => changes.map((c) => c.subscribers))).toEqual([
      [1, 2],
      [1, 0],
    ]);
  });
});
