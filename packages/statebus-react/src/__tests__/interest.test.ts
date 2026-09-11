import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { type ByID, ManualStateBus } from '@smoothbricks/statebus-core';
import { Window } from 'happy-dom';
import { act, createElement, StrictMode } from 'react';
import type { Root } from 'react-dom/client';
import { computedHook, StatebusProvider, useSubstate } from '../react.js';

declare module '@smoothbricks/statebus-core' {
  interface States {
    testRecords: ByID<string>;
  }
}

const saved = new Map<string, PropertyDescriptor | undefined>();
const browser = new Window({ url: 'https://example.test/' });
let createRoot: typeof import('react-dom/client').createRoot;
const roots = new Set<Root>();
beforeAll(async () => {
  const globals = {
    window: browser,
    document: browser.document,
    HTMLElement: browser.HTMLElement,
    Node: browser.Node,
    IS_REACT_ACT_ENVIRONMENT: true,
  };
  for (const [name, value] of Object.entries(globals)) {
    saved.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
  }
  ({ createRoot } = await import('react-dom/client'));
});
afterEach(async () => {
  for (const root of roots) await act(() => root.unmount());
  roots.clear();
});
afterAll(async () => {
  await browser.happyDOM.close();
  for (const [name, descriptor] of saved) {
    if (descriptor) Object.defineProperty(globalThis, name, descriptor);
    else Reflect.deleteProperty(globalThis, name);
  }
});

function createBus() {
  return new ManualStateBus({
    initialState: { counter: 0, testRecords: (id: string | number) => `${typeof id}:${id}` },
    reducers: {},
  });
}
function Screen({ id }: { id: string | number }) {
  const value = useSubstate('testRecords', id);
  return createElement('span', null, value);
}
const useLabel = computedHook(
  'label',
  (states, props: { id: string | number }) => states.testRecords.get(props.id).get(),
  (props) => [{ key: 'testRecords', id: props.id }],
);
function ComputedScreen({ id }: { id: string | number }) {
  return createElement('span', null, useLabel({ id }));
}

function mount() {
  const container = document.createElement('div');
  const root = createRoot(container);
  roots.add(root);
  return { container, root };
}

describe('real React subscription lifecycle in a DOM environment', () => {
  it('coalesces StrictMode churn, retains numeric identity, and cleans up once on unmount', async () => {
    const bus = createBus();
    const { root, container } = mount();
    await act(() =>
      root.render(
        createElement(
          StrictMode,
          null,
          createElement(StatebusProvider, { value: bus }, createElement(Screen, { id: 7 })),
        ),
      ),
    );
    bus.dispatchEvents();
    expect(container.textContent).toBe('number:7');
    expect(bus.getStateInterests()).toEqual([{ interest: { key: 'testRecords', id: 7 }, subscribers: 1 }]);
    await act(() => root.unmount());
    roots.delete(root);
    bus.dispatchEvents();
    expect(bus.getStateInterests()).toEqual([]);
  });

  it('does not resubscribe for equivalent descriptors, but changes interest when the ID or bus changes', async () => {
    const first = createBus();
    const second = createBus();
    const { root } = mount();
    let interestEvents = 0;
    first.subscribe('statebus', 'substateInterest', () => {
      interestEvents += 1;
    });
    const render = (bus: ManualStateBus, id: string | number) =>
      act(() => root.render(createElement(StatebusProvider, { value: bus }, createElement(Screen, { id }))));
    await render(first, 7);
    first.dispatchEvents();
    for (let index = 0; index < 10; index += 1) {
      await render(first, 7);
      first.dispatchEvents();
    }
    expect(interestEvents).toBe(1);
    await render(first, '7');
    first.dispatchEvents();
    expect(first.getStateInterests()).toEqual([{ interest: { key: 'testRecords', id: '7' }, subscribers: 1 }]);
    await render(second, '7');
    first.dispatchEvents();
    second.dispatchEvents();
    expect(first.getStateInterests()).toEqual([]);
    expect(second.getStateInterests()).toEqual([{ interest: { key: 'testRecords', id: '7' }, subscribers: 1 }]);
  });

  it('binds computed-view interest to resource props and keeps the mounted signal live after recreation', async () => {
    const bus = createBus();
    const { root, container } = mount();
    const render = (id: string) =>
      act(() => root.render(createElement(StatebusProvider, { value: bus }, createElement(ComputedScreen, { id }))));
    await render('a');
    bus.dispatchEvents();
    expect(bus.getStateInterests()).toEqual([{ interest: { key: 'testRecords', id: 'a' }, subscribers: 1 }]);
    await act(() => {
      bus.state.testRecords.remove('a');
      bus.state.testRecords.get('a').set('recreated');
    });
    expect(container.textContent).toBe('recreated');
    await render('b');
    bus.dispatchEvents();
    expect(container.textContent).toBe('string:b');
    expect(bus.getStateInterests()).toEqual([{ interest: { key: 'testRecords', id: 'b' }, subscribers: 1 }]);
  });
});
