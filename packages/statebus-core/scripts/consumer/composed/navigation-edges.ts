import assert from 'node:assert/strict';
import { composeLibraries, defineLibrary, ManualScheduler, mountLibrary } from '@smoothbricks/statebus-core';
import { connectBrowserNavigation, createBrowserNavigation } from '@smoothbricks/statebus-navigation-browser';
import {
  connectNavigation,
  createMemoryNavigation,
  initialNavigationState,
  NAVIGATION_DISPATCHED,
  type NavigationChannel,
  type NavigationDriver,
  type NavigationEvent,
  type NavigationLocation,
  type NavigationObservation,
  type NavigationOutcome,
  navigationRequestId,
  reduceNavigation,
} from '@smoothbricks/statebus-navigation-core';
import { createSelectionHook, createStateBusReact } from '@smoothbricks/statebus-react';
import { Window } from 'happy-dom';
import { act, createElement, StrictMode } from 'react';

function channel<Location>(initial: Location) {
  let state = initialNavigationState<string, Location>(initial);
  const listeners = new Set<(event: NavigationEvent<string, Location>) => void>();
  const events: NavigationEvent<string, Location>[] = [];
  const port: NavigationChannel<string, Location> = {
    read: () => state,
    publish(event) {
      events.push(event);
      state = reduceNavigation(state, event);
      for (const listener of listeners) listener(event);
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
  return { port, events, listeners };
}
let passed = 0;
async function test(name: string, run: () => void | Promise<void>) {
  await run();
  passed++;
  console.log(`PASS adjacent audit: ${name}`);
}
const request = (id: string) => ({
  requestId: navigationRequestId(id),
  intent: { kind: 'push' as const, to: `/${id}` },
});

await test('memory driver subscriptions are independent leases even for the same callback', () => {
  const driver = createMemoryNavigation({ entries: ['/'], equal: Object.is });
  let calls = 0;
  const listener = () => {
    calls++;
  };
  const first = driver.subscribe(listener);
  const second = driver.subscribe(listener);
  driver.execute(request('one'), { signal: new AbortController().signal });
  assert.equal(calls, 2);
  first();
  first();
  driver.execute(request('two'), { signal: new AbortController().signal });
  assert.equal(calls, 3);
  second();
  driver.execute(request('three'), { signal: new AbortController().signal });
  assert.equal(calls, 3);
});

await test('memory push after traversal truncates only the forward branch', () => {
  const driver = createMemoryNavigation({ entries: ['/'], equal: Object.is });
  const signal = new AbortController().signal;
  driver.execute(request('one'), { signal });
  driver.execute(request('two'), { signal });
  driver.execute({ requestId: navigationRequestId('back'), intent: { kind: 'back' } }, { signal });
  assert.equal(driver.current(), '/one');
  driver.execute(request('three'), { signal });
  driver.execute({ requestId: navigationRequestId('forward'), intent: { kind: 'forward' } }, { signal });
  assert.equal(driver.current(), '/three');
  driver.execute({ requestId: navigationRequestId('home'), intent: { kind: 'go', delta: -2 } }, { signal });
  assert.equal(driver.current(), '/');
});

await test('failed initial publication unwinds both acquired navigation subscriptions, even if cleanup throws', () => {
  const { port, listeners } = channel('/');
  let locations = 0;
  const errors: unknown[] = [];
  const failure = Error('initial publication');
  const driver: NavigationDriver<string, string> = {
    current: () => '/',
    execute: () => NAVIGATION_DISPATCHED,
    subscribe: () => {
      locations++;
      return () => {
        locations--;
      };
    },
  };
  assert.throws(
    () =>
      connectNavigation({
        driver,
        onError: (cause) => errors.push(cause),
        channel: {
          ...port,
          publish: () => {
            throw failure;
          },
          subscribe: (listener) => {
            const stop = port.subscribe(listener);
            return () => {
              stop();
              throw Error('cleanup');
            };
          },
        },
      }),
    (cause) => cause === failure,
  );
  assert.equal(locations, 0);
  assert.equal(listeners.size, 0);
  assert.equal(errors.length, 1);
});

await test('async driver completion and observation publication failures are handled without rejecting tracked completion', async () => {
  const { port, events } = channel('/');
  const completion = Promise.withResolvers<NavigationOutcome>();
  const tasks: Promise<void>[] = [];
  const errors: unknown[] = [];
  let observation: ((value: NavigationObservation<string>) => void) | undefined;
  const dispose = connectNavigation({
    channel: {
      ...port,
      publish: (event) => {
        if (event.type === 'navigationDispatched' || (event.type === 'locationObserved' && event.source === 'external'))
          throw Error('publication');
        port.publish(event);
      },
    },
    driver: {
      current: () => '/',
      execute: () => completion.promise,
      subscribe: (listener) => {
        observation = listener;
        return () => {
          observation = undefined;
        };
      },
    },
    onError: (cause) => errors.push(cause),
    trackExecution: (task) => tasks.push(task),
  });
  try {
    port.publish({ type: 'navigationRequested', request: request('async') });
    assert.equal(tasks.length, 1);
    observation?.({ location: '/external', source: 'external' });
    completion.resolve(NAVIGATION_DISPATCHED);
    await tasks[0];
    assert.equal(errors.length, 2);
    assert.equal(events.filter((event) => event.type === 'navigationDispatched').length, 0);
  } finally {
    dispose();
  }
});

await test('asynchronous navigation disposal suppresses late acknowledgement but tracks actual settlement', async () => {
  const { port, events, listeners } = channel('/');
  const completion = Promise.withResolvers<NavigationOutcome>();
  const tasks: Promise<void>[] = [];
  let aborted = false;
  let locations = 0;
  const stop = connectNavigation({
    channel: port,
    driver: {
      current: () => '/',
      subscribe: () => {
        locations++;
        return () => {
          locations--;
        };
      },
      execute: (_request, { signal }) => {
        signal.addEventListener('abort', () => {
          aborted = true;
        });
        return completion.promise;
      },
    },
    trackExecution: (task) => tasks.push(task),
  });
  port.publish({ type: 'navigationRequested', request: request('async') });
  stop();
  stop();
  assert.equal(aborted, true);
  assert.equal(listeners.size, 0);
  assert.equal(locations, 0);
  let finished = false;
  void tasks[0].then(() => {
    finished = true;
  });
  await Promise.resolve();
  assert.equal(finished, false);
  completion.resolve(NAVIGATION_DISPATCHED);
  await tasks[0];
  assert.equal(events.filter((event) => event.type === 'navigationDispatched').length, 0);
});

await test('abort-time disposal cannot start a superseding navigation operation', () => {
  const { port } = channel('/');
  const pending = Promise.withResolvers<NavigationOutcome>();
  let calls = 0;
  const dispose = connectNavigation({
    channel: port,
    driver: {
      current: () => '/',
      subscribe: () => () => {},
      execute: (_request, { signal }) => {
        calls++;
        signal.addEventListener('abort', () => dispose());
        return pending.promise;
      },
    },
  });
  port.publish({ type: 'navigationRequested', request: request('one') });
  port.publish({ type: 'navigationRequested', request: request('two') });
  assert.equal(calls, 1);
  pending.resolve(NAVIGATION_DISPATCHED);
  dispose();
});

await test('a newer request from an abort observer is not overwritten by the outer superseded request', async () => {
  const { port } = channel('/');
  const pending = Promise.withResolvers<NavigationOutcome>();
  const calls: string[] = [];
  const dispose = connectNavigation({
    channel: port,
    driver: {
      current: () => '/',
      subscribe: () => () => {},
      execute: (input, { signal }) => {
        calls.push(input.requestId);
        if (input.requestId === navigationRequestId('one'))
          signal.addEventListener('abort', () =>
            port.publish({
              type: 'navigationRequested',
              request: request('newest'),
            }),
          );
        return pending.promise;
      },
    },
  });
  try {
    port.publish({ type: 'navigationRequested', request: request('one') });
    port.publish({ type: 'navigationRequested', request: request('superseded') });
    assert.deepEqual(calls, ['one', 'newest']);
  } finally {
    pending.resolve(NAVIGATION_DISPATCHED);
    dispose();
    await pending.promise;
  }
});

const browser = new Window({ url: 'https://navigation.example.test/' });
const saved = new Map<string, PropertyDescriptor | undefined>();
for (const [key, value] of Object.entries({
  window: browser,
  document: browser.document,
  HTMLElement: browser.HTMLElement,
  Node: browser.Node,
  IS_REACT_ACT_ENVIRONMENT: true,
})) {
  saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
  Object.defineProperty(globalThis, key, { value, configurable: true, writable: true });
}
try {
  const adds = window.addEventListener;
  const removes = window.removeEventListener;
  const registered = new Map<string, Set<EventListenerOrEventListenerObject>>();
  let rejectHash = false;
  Object.defineProperty(window, 'addEventListener', {
    configurable: true,
    value: (
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: boolean | AddEventListenerOptions,
    ) => {
      let set = registered.get(type);
      if (!set) {
        set = new Set();
        registered.set(type, set);
      }
      set.add(listener);
      Reflect.apply(adds, window, [type, listener, options]);
      if (rejectHash && type === 'hashchange') throw Error('hash subscription');
    },
  });
  Object.defineProperty(window, 'removeEventListener', {
    configurable: true,
    value: (type: string, listener: EventListenerOrEventListenerObject, options?: boolean | EventListenerOptions) => {
      registered.get(type)?.delete(listener);
      Reflect.apply(removes, window, [type, listener, options]);
    },
  });
  await test('browser driver rolls back partial native-listener installation and can be subscribed again', () => {
    const driver = createBrowserNavigation({ window });
    rejectHash = true;
    assert.throws(() => driver.subscribe(() => {}), /hash subscription/);
    rejectHash = false;
    assert.equal(registered.get('popstate')?.size, 0);
    assert.equal(registered.get('hashchange')?.size, 0);
    const release = driver.subscribe(() => {});
    assert.equal(registered.get('popstate')?.size, 1);
    release();
    driver.dispose();
    assert.equal(registered.get('popstate')?.size, 0);
  });
  await test('browser connection removes unload and location listeners despite a failing guard cleanup', () => {
    const { port, listeners } = channel<NavigationLocation>({ pathname: '/', search: '', hash: '' });
    port.publish({ type: 'navigationGuardChanged', reason: 'dirty' });
    let subscriptions = 0;
    const errors: unknown[] = [];
    const dispose = connectBrowserNavigation({
      window,
      onError: (cause) => errors.push(cause),
      channel: {
        ...port,
        subscribe: (listener) => {
          const ordinal = ++subscriptions;
          const stop = port.subscribe(listener);
          return () => {
            stop();
            if (ordinal === 2) throw Error('guard cleanup');
          };
        },
      },
    });
    assert.equal(registered.get('beforeunload')?.size, 1);
    dispose();
    dispose();
    for (const key of ['beforeunload', 'popstate', 'hashchange']) assert.equal(registered.get(key)?.size, 0);
    assert.equal(listeners.size, 0);
    assert.equal(errors.length, 1);
  });
  await test('boolean selector props compile, reuse snapshots, and release exact subscriptions under StrictMode', async () => {
    const definition = defineLibrary({
      name: 'boolean-ui',
      requires: [],
      setup(scope) {
        const value = scope.scalar('value', () => 3);
        return { value };
      },
    });
    const mount = mountLibrary(definition, 'ui');
    const composition = composeLibraries(mount);
    const runtime = composition.createRuntime({ scheduler: new ManualScheduler() });
    const App = createStateBusReact(composition);
    const useValue = createSelectionHook(
      'boolean',
      (state, props: { enabled: boolean }) => (props.enabled ? state.read(mount.exports.value) : 0),
      () => [mount.exports.value],
    );
    const { createRoot } = await import('react-dom/client');
    const node = document.createElement('div');
    const root = createRoot(node);
    function View({ enabled }: { enabled: boolean }) {
      return createElement('span', null, useValue({ enabled }));
    }
    const render = (enabled: boolean) =>
      act(() =>
        root.render(
          createElement(StrictMode, null, createElement(App.Provider, { runtime }, createElement(View, { enabled }))),
        ),
      );
    try {
      await render(true);
      runtime.flush();
      assert.equal(node.textContent, '3');
      const interests = runtime.interestSource.snapshot();
      await render(true);
      runtime.flush();
      assert.deepEqual(runtime.interestSource.snapshot(), interests);
      await render(false);
      runtime.flush();
      assert.equal(node.textContent, '0');
      await act(() => root.unmount());
      runtime.flush();
      assert.equal(runtime.interestSource.snapshot().length, 0);
    } finally {
      runtime.dispose();
    }
  });
} finally {
  await browser.happyDOM.close();
  for (const [key, descriptor] of saved) {
    if (descriptor) Object.defineProperty(globalThis, key, descriptor);
    else Reflect.deleteProperty(globalThis, key);
  }
}
console.log(JSON.stringify({ adjacentAuditScenarios: passed, publicExports: true }));
