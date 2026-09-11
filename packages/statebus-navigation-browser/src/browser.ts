import {
  NAVIGATION_CANCELLED,
  NAVIGATION_DISPATCHED,
  type NavigationDriver,
  type NavigationLocation,
  type NavigationObservation,
  type NavigationOutcome,
  type NavigationRequestId,
} from '@smoothbricks/statebus-navigation-core';
import { planBrowserNavigation } from './plan.js';

export interface BrowserNavigationDriver extends NavigationDriver<string, NavigationLocation> {
  /** Notify the bus after a host router writes history itself. History methods are not monkey-patched. */
  observe(): void;
  dispose(): void;
}
const DISPOSED: NavigationOutcome = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'disposed', message: 'The browser navigation driver is disposed.' }),
});
const FAILED: NavigationOutcome = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'driver-failed', message: 'The browser rejected the navigation operation.' }),
});

/** No ambient window access: one composition owns one adapter and its subscription lifetime. */
export function createBrowserNavigation(options: { readonly window: Window }): BrowserNavigationDriver {
  const browser = options.window;
  const listeners = new Set<(observation: NavigationObservation<NavigationLocation>) => void>();
  let disposed = false;
  let listening = false;
  let lastHref = '';
  let notifiedHref = browser.location.href;
  let location: NavigationLocation | undefined;

  function current(): NavigationLocation {
    const href = browser.location.href;
    if (location === undefined || lastHref !== href) {
      lastHref = href;
      location = Object.freeze({
        pathname: browser.location.pathname,
        search: browser.location.search,
        hash: browser.location.hash,
      });
    }
    return location;
  }

  function emit(source: NavigationObservation<NavigationLocation>['source'], requestId?: NavigationRequestId): void {
    if (disposed) return;
    notifiedHref = browser.location.href;
    const observation = { location: current(), source, requestId };
    for (const listener of listeners) listener(observation);
  }
  function onPopState(): void {
    emit('history');
  }
  function onHashChange(): void {
    // Traversing a hash entry can fire both popstate and hashchange for the same URL.
    if (notifiedHref !== browser.location.href) emit('history');
  }
  function stop(): void {
    if (!listening) return;
    listening = false;
    browser.removeEventListener('popstate', onPopState);
    browser.removeEventListener('hashchange', onHashChange);
  }

  return {
    current,
    observe() {
      emit('external');
    },
    subscribe(listener) {
      if (disposed) throw new Error('Cannot subscribe to a disposed browser navigation driver.');
      // Each subscription is its own lease, even when the callback is shared.
      const forward = (observation: NavigationObservation<NavigationLocation>) => listener(observation);
      listeners.add(forward);
      if (!listening) {
        listening = true;
        notifiedHref = browser.location.href;
        browser.addEventListener('popstate', onPopState);
        browser.addEventListener('hashchange', onHashChange);
      }
      let released = false;
      return () => {
        if (released) return;
        released = true;
        listeners.delete(forward);
        if (listeners.size === 0) stop();
      };
    },
    execute(request, { signal }) {
      if (disposed) return DISPOSED;
      if (signal.aborted) return NAVIGATION_CANCELLED;
      const plan = planBrowserNavigation(request.intent, browser.location.href);
      try {
        switch (plan.kind) {
          case 'failed':
            return plan;
          case 'write':
            if (plan.mode === 'replace') browser.history.replaceState(browser.history.state, '', plan.href);
            else browser.history.pushState(null, '', plan.href);
            break;
          case 'unchanged':
            break;
          case 'traverse':
            // Native traversals cannot be vetoed or cancelled. Later observations remain authoritative.
            browser.history.go(plan.delta);
            return NAVIGATION_DISPATCHED;
          case 'external':
            if (plan.mode === 'new-tab') {
              // With noopener a successful open may return null; null is not proof of a popup failure.
              browser.open(plan.href, '_blank', 'noopener,noreferrer');
            } else if (plan.mode === 'replace') browser.location.replace(plan.href);
            else browser.location.assign(plan.href);
            return NAVIGATION_DISPATCHED;
        }
      } catch {
        return FAILED;
      }
      // Read the actual browser URL after the write. No predicted location enters state.
      emit('intent', request.requestId);
      return NAVIGATION_DISPATCHED;
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      stop();
      listeners.clear();
      location = undefined;
      lastHref = '';
      notifiedHref = '';
    },
  };
}
