import {
  connectNavigation,
  type NavigationConnectionOptions,
  type NavigationLocation,
} from '@smoothbricks/statebus-navigation-core';
import { createBrowserNavigation } from './browser.js';

/** Own the browser driver, StateBus channel binding and native unload guard together. */
export function connectBrowserNavigation(
  options: Omit<NavigationConnectionOptions<string, NavigationLocation>, 'driver'> & {
    readonly window: Window;
  },
): () => void {
  const driver = createBrowserNavigation(options);
  let disposed = false;
  let guarded = false;
  const { window: browser, channel } = options;
  let stopNavigation: (() => void) | undefined;
  let stopGuard: (() => void) | undefined;
  function report(cause: unknown): void {
    try {
      if (options.onError) options.onError(cause);
      else console.error('StateBus browser navigation cleanup failure', cause);
    } catch {
      // Cleanup still owns the other subscriptions even when reporting fails.
    }
  }
  function release(stop: (() => void) | undefined): void {
    try {
      stop?.();
    } catch (cause) {
      report(cause);
    }
  }
  function beforeUnload(event: BeforeUnloadEvent): void {
    if (disposed || !channel.read().guard) return;
    event.preventDefault();
    event.returnValue = '';
  }
  function updateGuard(): void {
    if (disposed) return;
    const next = Boolean(channel.read().guard);
    if (guarded === next) return;
    // Mark intent first so rollback removes the listener even if an adapter throws after installation.
    guarded = next;
    if (next) browser.addEventListener('beforeunload', beforeUnload);
    else browser.removeEventListener('beforeunload', beforeUnload);
  }
  function removeGuard(): void {
    if (!guarded) return;
    guarded = false;
    browser.removeEventListener('beforeunload', beforeUnload);
  }
  function dispose(): void {
    if (disposed) return;
    disposed = true;
    release(stopGuard);
    release(stopNavigation);
    release(driver.dispose);
    release(removeGuard);
    stopGuard = undefined;
    stopNavigation = undefined;
  }
  try {
    stopNavigation = connectNavigation({
      channel,
      driver,
      onError: options.onError,
      trackExecution: options.trackExecution,
    });
    stopGuard = channel.subscribe(updateGuard);
    updateGuard();
  } catch (cause) {
    dispose();
    throw cause;
  }
  return dispose;
}
