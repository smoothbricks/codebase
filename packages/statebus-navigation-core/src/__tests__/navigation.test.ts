import { describe, expect, it } from 'bun:test';
import { ManualStateBus } from '@smoothbricks/statebus-core';
import {
  connectNavigation,
  NAVIGATION_DISPATCHED,
  type NavigationChannel,
  type NavigationDriver,
  type NavigationObservation,
} from '../driver.js';
import { createMemoryNavigation } from '../memory.js';
import { initialNavigationState, type NavigationEvent, type NavigationState, reduceNavigation } from '../model.js';

type Event = NavigationEvent<string, string>;
declare module '@smoothbricks/statebus-core' {
  interface States {
    'navigationTest.location': NavigationState<string, string>;
  }
  interface Events {
    navigationTest: { event: Event };
  }
}

function fixture(driver: NavigationDriver<string, string>) {
  const initial = initialNavigationState<string, string>(driver.current());
  const bus = new ManualStateBus({
    initialState: { 'navigationTest.location': initial },
    reducers: {
      navigationTest: {
        event: (state, event) =>
          state['navigationTest.location'].update((previous) => reduceNavigation(previous, event)),
      },
    },
  });
  const events: Event[] = [];
  bus.subscribe('navigationTest', 'event', (event) => events.push(event.payload));
  const channel: NavigationChannel<string, string> = {
    publish: (event) => {
      bus.publish({ topic: 'navigationTest', type: 'event', payload: event });
    },
    subscribe: (listener) => bus.subscribe('navigationTest', 'event', (event) => listener(event.payload)),
    read: () => bus.state['navigationTest.location'].get(),
  };
  const dispose = connectNavigation({ channel, driver });
  return {
    channel,
    initial,
    events,
    dispose,
    async tick() {
      bus.dispatchEvents();
      await Bun.sleep(0);
      bus.dispatchEvents();
    },
  };
}

describe('navigation production wiring', () => {
  it('drives a guarded deeplink, confirmation, replace, Back and Forward through event streams', async () => {
    const history = createMemoryNavigation({ entries: ['/editor'], equal: (left, right) => left === right });
    const test = fixture(history);
    try {
      test.channel.publish({ type: 'navigationGuardChanged', reason: 'Unsaved changes' });
      test.channel.publish({
        type: 'navigationRequested',
        request: { requestId: 'billing', intent: { kind: 'push', to: '/settings/billing?tab=invoices' } },
      });
      await test.tick();
      expect(history.current()).toBe('/editor');
      expect(test.channel.read().operation.kind).toBe('blocked');
      test.channel.publish({ type: 'navigationConfirmed', requestId: 'billing' });
      await test.tick();
      expect(test.channel.read().location).toBe('/settings/billing?tab=invoices');
      test.channel.publish({ type: 'navigationGuardChanged' });
      test.channel.publish({
        type: 'navigationRequested',
        request: { requestId: 'replace', intent: { kind: 'replace', to: '/settings/billing?tab=plans' } },
      });
      await test.tick();
      test.channel.publish({ type: 'navigationRequested', request: { requestId: 'back', intent: { kind: 'back' } } });
      await test.tick();
      expect(test.channel.read().location).toBe('/editor');
      test.channel.publish({
        type: 'navigationRequested',
        request: { requestId: 'forward', intent: { kind: 'forward' } },
      });
      await test.tick();
      expect(test.channel.read().location).toBe('/settings/billing?tab=plans');
      expect(test.events.reduce(reduceNavigation<string, string>, test.initial)).toEqual(test.channel.read());
    } finally {
      test.dispose();
    }
  });

  it('aborts superseded driver scopes and suppresses rejected/late completions on disposal', async () => {
    const pending = Promise.withResolvers<typeof NAVIGATION_DISPATCHED>();
    const signals: AbortSignal[] = [];
    const observers = new Set<(value: NavigationObservation<string>) => void>();
    const driver: NavigationDriver<string, string> = {
      current: () => '/',
      subscribe(listener) {
        observers.add(listener);
        return () => {
          observers.delete(listener);
        };
      },
      async execute(_request, { signal }) {
        signals.push(signal);
        return pending.promise;
      },
    };
    const test = fixture(driver);
    test.channel.publish({
      type: 'navigationRequested',
      request: { requestId: 'a', intent: { kind: 'push', to: '/a' } },
    });
    await test.tick();
    test.channel.publish({
      type: 'navigationRequested',
      request: { requestId: 'b', intent: { kind: 'push', to: '/b' } },
    });
    await test.tick();
    expect(signals[0]?.aborted).toBe(true);
    expect(signals[1]?.aborted).toBe(false);
    test.dispose();
    test.dispose();
    expect(signals[1]?.aborted).toBe(true);
    expect(observers.size).toBe(0);
    const count = test.events.length;
    pending.reject(new Error('late navigation failure'));
    await test.tick();
    expect(test.events.length).toBe(count);
  });

  it('records an observed browser location even while user-intent navigation is guarded', async () => {
    const listeners = new Set<(value: NavigationObservation<string>) => void>();
    const test = fixture({
      current: () => '/',
      subscribe(listener) {
        listeners.add(listener);
        return () => {
          listeners.delete(listener);
        };
      },
      execute: () => NAVIGATION_DISPATCHED,
    });
    try {
      test.channel.publish({ type: 'navigationGuardChanged', reason: 'dirty' });
      await test.tick();
      for (const listener of listeners) listener({ source: 'history', location: '/actual-back-destination' });
      await test.tick();
      expect(test.channel.read().location).toBe('/actual-back-destination');
    } finally {
      test.dispose();
    }
  });
});
