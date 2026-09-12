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
import {
  initialNavigationState,
  type NavigationEvent,
  type NavigationState,
  navigationRequestId,
  reduceNavigation,
} from '../model.js';

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
        request: {
          requestId: navigationRequestId('billing'),
          intent: { kind: 'push', to: '/settings/billing?tab=invoices' },
        },
      });
      await test.tick();
      expect(history.current()).toBe('/editor');
      expect(test.channel.read().operation.kind).toBe('blocked');
      test.channel.publish({ type: 'navigationGuardChanged', reason: 'Upload still in progress' });
      await test.tick();
      expect(test.channel.read().operation).toEqual({
        kind: 'blocked',
        request: {
          requestId: navigationRequestId('billing'),
          intent: { kind: 'push', to: '/settings/billing?tab=invoices' },
        },
        reason: 'Upload still in progress',
      });
      expect(history.current()).toBe('/editor');
      test.channel.publish({ type: 'navigationConfirmed', requestId: navigationRequestId('billing') });
      await test.tick();
      expect(test.channel.read().location).toBe('/settings/billing?tab=invoices');
      test.channel.publish({ type: 'navigationGuardChanged' });
      test.channel.publish({
        type: 'navigationRequested',
        request: {
          requestId: navigationRequestId('replace'),
          intent: { kind: 'replace', to: '/settings/billing?tab=plans' },
        },
      });
      await test.tick();
      test.channel.publish({
        type: 'navigationRequested',
        request: { requestId: navigationRequestId('back'), intent: { kind: 'back' } },
      });
      await test.tick();
      expect(test.channel.read().location).toBe('/editor');
      test.channel.publish({
        type: 'navigationRequested',
        request: { requestId: navigationRequestId('forward'), intent: { kind: 'forward' } },
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
      request: { requestId: navigationRequestId('a'), intent: { kind: 'push', to: '/a' } },
    });
    await test.tick();
    test.channel.publish({
      type: 'navigationRequested',
      request: { requestId: navigationRequestId('b'), intent: { kind: 'push', to: '/b' } },
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

it('cancels an active asynchronous adapter when a newer request is blocked', async () => {
  const pending = Promise.withResolvers<typeof NAVIGATION_DISPATCHED>();
  let scope: AbortSignal | undefined;
  let starts = 0;
  const test = fixture({
    current: () => '/',
    subscribe: () => () => {},
    execute(_request, { signal }) {
      starts++;
      scope = signal;
      return pending.promise;
    },
  });
  try {
    test.channel.publish({
      type: 'navigationRequested',
      request: { requestId: navigationRequestId('old'), intent: { kind: 'push', to: '/old' } },
    });
    await test.tick();
    test.channel.publish({ type: 'navigationGuardChanged', reason: 'dirty' });
    test.channel.publish({
      type: 'navigationRequested',
      request: { requestId: navigationRequestId('new'), intent: { kind: 'push', to: '/new' } },
    });
    await test.tick();
    expect(scope?.aborted).toBe(true);
    expect(starts).toBe(1);
    pending.resolve(NAVIGATION_DISPATCHED);
    await test.tick();
    expect(test.channel.read().operation.kind).toBe('blocked');
    expect(
      test.events.some((e) => e.type === 'navigationDispatched' && e.requestId === navigationRequestId('old')),
    ).toBe(false);
  } finally {
    test.dispose();
  }
});

it('admits no driver work when a guard or cancellation arrives later in the same wave', async () => {
  let starts = 0;
  const test = fixture({
    current: () => '/',
    subscribe: () => () => {},
    execute() {
      starts++;
      return NAVIGATION_DISPATCHED;
    },
  });
  try {
    const request = { requestId: navigationRequestId('guarded'), intent: { kind: 'push', to: '/next' } } as const;
    test.channel.publish({ type: 'navigationRequested', request });
    test.channel.publish({ type: 'navigationGuardChanged', reason: 'dirty' });
    await test.tick();
    expect(starts).toBe(0);
    expect(test.channel.read().operation.kind).toBe('blocked');
    test.channel.publish({ type: 'navigationConfirmed', requestId: request.requestId });
    test.channel.publish({ type: 'navigationCancelled', requestId: request.requestId });
    await test.tick();
    expect(starts).toBe(0);
    expect(test.channel.read().operation.kind).toBe('idle');
  } finally {
    test.dispose();
  }
});

it('memory history acknowledges no-op navigation and honors aborted operations', async () => {
  const driver = createMemoryNavigation({ entries: ['/'], equal: Object.is });
  const test = fixture(driver);
  try {
    for (const [id, intent] of [
      ['same', { kind: 'navigate', to: '/' }],
      ['outside', { kind: 'back' }],
    ] as const) {
      test.channel.publish({ type: 'navigationRequested', request: { requestId: navigationRequestId(id), intent } });
      await test.tick();
      expect(test.channel.read().operation.kind).toBe('idle');
    }
    const abort = new AbortController();
    abort.abort();
    expect(
      (
        await driver.execute(
          { requestId: navigationRequestId('aborted'), intent: { kind: 'push', to: '/bad' } },
          { signal: abort.signal },
        )
      ).kind,
    ).toBe('failed');
    expect(driver.current()).toBe('/');
  } finally {
    test.dispose();
  }
});

it('observed native history cancels older asynchronous adapter work without hiding the URL fact', async () => {
  const pending = Promise.withResolvers<typeof NAVIGATION_DISPATCHED>();
  let scope: AbortSignal | undefined;
  const test = fixture({
    current: () => '/',
    subscribe: () => () => {},
    execute(_r, { signal }) {
      scope = signal;
      return pending.promise;
    },
  });
  try {
    test.channel.publish({
      type: 'navigationRequested',
      request: { requestId: navigationRequestId('old'), intent: { kind: 'push', to: '/old' } },
    });
    await test.tick();
    test.channel.publish({ type: 'locationObserved', source: 'history', location: '/actual' });
    await test.tick();
    expect(scope?.aborted).toBe(true);
    pending.resolve(NAVIGATION_DISPATCHED);
    await test.tick();
    expect(test.channel.read().location).toBe('/actual');
    expect(test.channel.read().operation.kind).toBe('idle');
  } finally {
    test.dispose();
  }
});
