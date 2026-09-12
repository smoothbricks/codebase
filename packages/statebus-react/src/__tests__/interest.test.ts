import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { type ByID, ManualStateBus } from '@smoothbricks/statebus-core';
import { Window } from 'happy-dom';
import { act, createElement, StrictMode, Suspense, startTransition } from 'react';
import type { Root } from 'react-dom/client';
import { computedHook, eventPublisher, StatebusProvider, useBus, useSubstate } from '../react.js';

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

it('keeps equivalent primitive props and event publisher functions stable across renders', async () => {
  const bus = createBus();
  const { root, container } = mount();
  let computations = 0;
  let interestEvents = 0;
  bus.subscribe('statebus', 'substateInterest', () => {
    interestEvents += 1;
  });
  const useSummary = computedHook(
    'summary',
    (states, props: { id: string; label?: string }) => {
      computations += 1;
      return `${props.label}:${states.testRecords.get(props.id).get()}`;
    },
    (props) => [{ key: 'testRecords', id: props.id }],
  );
  const publishers: ((amount: number) => void)[] = [];
  const rootPublishers: ((amount: number) => void)[] = [];
  function Summary({ input }: { input: { id: string; label?: string } }) {
    const value = useSummary(input);
    const topic = useBus('test');
    const all = useBus();
    publishers.push(topic.increment);
    rootPublishers.push(all.test.increment);
    return createElement('span', null, value);
  }
  const render = (input: { id: string; label?: string }) =>
    act(() => root.render(createElement(StatebusProvider, { value: bus }, createElement(Summary, { input }))));
  await render({ id: 'a', label: 'first' });
  bus.dispatchEvents();
  for (let i = 0; i < 12; i += 1) await render({ label: 'first', id: 'a' });
  bus.dispatchEvents();
  expect(computations).toBe(1);
  expect(interestEvents).toBe(1);
  expect(publishers.every((publish) => publish === publishers[0])).toBe(true);
  expect(rootPublishers.every((publish) => publish === rootPublishers[0])).toBe(true);
  const mutable = { id: 'a', label: 'first' };
  await render(mutable);
  mutable.label = 'changed';
  await render(mutable);
  bus.dispatchEvents();
  expect(container.textContent).toBe('changed:string:a');
  expect(interestEvents).toBe(1); // Unrelated view props must not churn demand.
  await act(() => bus.state.testRecords.get('a').set('updated'));
  expect(container.textContent).toBe('changed:updated');
  const direct = eventPublisher(bus, 'test');
  expect(direct.increment).toBe(direct.increment);
});

it('does not commit interest or overwrite the current computation from an abandoned suspended render', async () => {
  const bus = createBus();
  const { root, container } = mount();
  const pending = new Promise<void>(() => {});
  function SuspendedScreen({ id }: { id: string }) {
    const value = useLabel({ id });
    if (id === 'suspended') throw pending;
    return createElement('span', null, value);
  }
  const screen = (id: string) =>
    createElement(
      StatebusProvider,
      { value: bus },
      createElement(Suspense, { fallback: 'waiting' }, createElement(SuspendedScreen, { id })),
    );
  await act(() => root.render(screen('visible')));
  bus.dispatchEvents();
  await act(() => startTransition(() => root.render(screen('suspended'))));
  bus.dispatchEvents();
  expect(container.textContent).toBe('string:visible');
  expect(bus.getStateInterests()).toEqual([{ interest: { key: 'testRecords', id: 'visible' }, subscribers: 1 }]);
  await act(() => root.render(screen('visible')));
  await act(() => bus.state.testRecords.get('visible').set('still-live'));
  expect(container.textContent).toBe('still-live');
  bus.dispatchEvents();
  expect(bus.getStateInterests()).toEqual([{ interest: { key: 'testRecords', id: 'visible' }, subscribers: 1 }]);
});

it('rebinds publishers to a new runtime rather than emitting to a stale provider', async () => {
  const first = createBus();
  const second = createBus();
  const { root } = mount();
  let publish: ((amount: number) => void) | undefined;
  const seen: number[] = [];
  first.subscribe('test', 'increment', () => seen.push(1));
  second.subscribe('test', 'increment', () => seen.push(2));
  function Publisher() {
    publish = useBus('test').increment;
    return null;
  }
  await act(() => root.render(createElement(StatebusProvider, { value: first }, createElement(Publisher))));
  const old = publish;
  await act(() => root.render(createElement(StatebusProvider, { value: second }, createElement(Publisher))));
  expect(publish).not.toBe(old);
  publish?.(1);
  first.dispatchEvents();
  second.dispatchEvents();
  expect(seen).toEqual([2]);
});
