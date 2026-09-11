import { MicrotaskStateBus } from '@smoothbricks/statebus-core';
import {
  initialNavigationState,
  type NavigationChannel,
  type NavigationEvent,
  type NavigationIntent,
  type NavigationLocation,
  type NavigationRequest,
  type NavigationState,
  navigationRequestId,
  reduceNavigation,
} from '@smoothbricks/statebus-navigation-core';
import { StatebusProvider, useBus, useSubstate } from '@smoothbricks/statebus-react';
import { createElement as h, StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { connectBrowserNavigation, createBrowserNavigation } from '../src/index.js';

type NavEvent = NavigationEvent<string, NavigationLocation>;
type NavState = NavigationState<string, NavigationLocation>;
declare module '@smoothbricks/statebus-core' {
  interface States {
    'browser.location': NavState;
  }
  interface Events {
    browser: { event: NavEvent };
  }
}

const initial = initialNavigationState<string, NavigationLocation>({
  pathname: location.pathname,
  search: location.search,
  hash: location.hash,
});
const bus = new MicrotaskStateBus({
  initialState: { 'browser.location': initial },
  reducers: {
    browser: {
      event: (state, event) => {
        const previous = state['browser.location'].get();
        state['browser.location'].set(reduceNavigation(previous, event));
      },
    },
  },
});
const channel: NavigationChannel<string, NavigationLocation> = {
  publish: (event) => {
    bus.publish({ topic: 'browser', type: 'event', payload: event });
  },
  subscribe: (listener) => bus.subscribe('browser', 'event', (event) => listener(event.payload)),
  read: () => bus.state['browser.location'].get(),
};
const events: NavEvent[] = [];
const stopEvents = channel.subscribe((event) => events.push(event));
const disconnect = connectBrowserNavigation({ channel, window });
const direct = createBrowserNavigation({ window });
let rawObservations = 0;
const stopDirect = direct.subscribe(() => {
  rawObservations++;
});
let nextId = 0;
function request(intent: NavigationIntent<string>): NavigationRequest<string> {
  return { requestId: navigationRequestId(`browser-${++nextId}`), intent };
}
function Screen() {
  const state = useSubstate('browser.location');
  const send = useBus('browser');
  return h(
    'main',
    null,
    h('output', { id: 'path' }, state.location.pathname + state.location.search + state.location.hash),
    h('output', { id: 'operation' }, state.operation.kind),
    h(
      'button',
      {
        id: 'billing',
        type: 'button',
        onClick: () =>
          send.event({
            type: 'navigationRequested',
            request: request({ kind: 'push', to: '/settings/billing?tab=invoices#recent' }),
          }),
      },
      'Billing',
    ),
    h(
      'button',
      {
        id: 'guard',
        type: 'button',
        onClick: () => send.event({ type: 'navigationGuardChanged', reason: 'Unsaved changes' }),
      },
      'Make dirty',
    ),
    h(
      'button',
      { id: 'clear', type: 'button', onClick: () => send.event({ type: 'navigationGuardChanged' }) },
      'Clear guard',
    ),
    h(
      'button',
      {
        id: 'confirm',
        type: 'button',
        disabled: state.operation.kind !== 'blocked',
        onClick: () => {
          if (state.operation.kind === 'blocked')
            send.event({ type: 'navigationConfirmed', requestId: state.operation.request.requestId });
        },
      },
      'Leave anyway',
    ),
    h(
      'button',
      {
        id: 'external',
        type: 'button',
        onClick: () =>
          send.event({
            type: 'navigationRequested',
            request: request({ kind: 'external', href: '/opened', mode: 'new-tab' }),
          }),
      },
      'Open in new tab',
    ),
  );
}
const element = document.getElementById('app');
if (!element) throw new Error('Missing fixture root.');
const root = createRoot(element);
root.render(h(StrictMode, null, h(StatebusProvider, { value: bus }, h(Screen))));

export interface BrowserFixture {
  publish(event: NavEvent): void;
  navigate(intent: NavigationIntent<string>): void;
  state(): NavState;
  eventCount(): number;
  rawCount(): number;
  observeRaw(): void;
  rawExecute(request: NavigationRequest<string>, abort: boolean): ReturnType<typeof direct.execute>;
  disposeRaw(): void;
  dispose(): void;
}
declare global {
  interface Window {
    navigationTest: BrowserFixture;
  }
}
window.navigationTest = {
  publish: channel.publish,
  navigate: (intent) => channel.publish({ type: 'navigationRequested', request: request(intent) }),
  state: channel.read,
  eventCount: () => events.length,
  rawCount: () => rawObservations,
  observeRaw: direct.observe,
  rawExecute(input, abort) {
    const scope = new AbortController();
    if (abort) scope.abort();
    return direct.execute(input, { signal: scope.signal });
  },
  disposeRaw() {
    stopDirect();
    direct.dispose();
  },
  dispose() {
    disconnect();
    stopEvents();
    stopDirect();
    direct.dispose();
    root.unmount();
  },
};
