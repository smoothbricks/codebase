import {
  connectNavigation,
  type NavigationChannel,
  type NavigationLocation,
} from '@smoothbricks/statebus-navigation-core';
import { createBrowserNavigation } from './browser.js';

/** Own the browser driver, StateBus channel binding and native unload guard together. */
export function connectBrowserNavigation(options: {
  readonly window: Window;
  readonly channel: NavigationChannel<string, NavigationLocation>;
}): () => void {
  const driver = createBrowserNavigation(options);
  let disposed = false;
  let guarded = false;
  const { window: browser, channel } = options;
  function beforeUnload(event: BeforeUnloadEvent): void {
    if (!channel.read().guard) return;
    event.preventDefault();
    event.returnValue = '';
  }
  function updateGuard(): void {
    const next = Boolean(channel.read().guard);
    if (guarded === next) return;
    guarded = next;
    if (next) browser.addEventListener('beforeunload', beforeUnload);
    else browser.removeEventListener('beforeunload', beforeUnload);
  }
  const stopNavigation = connectNavigation({ channel, driver });
  let stopGuard: () => void = () => {};
  try {
    stopGuard = channel.subscribe(updateGuard);
    updateGuard();
  } catch (cause) {
    stopGuard();
    if (guarded) browser.removeEventListener('beforeunload', beforeUnload);
    stopNavigation();
    driver.dispose();
    throw cause;
  }
  return () => {
    if (disposed) return;
    disposed = true;
    stopGuard();
    stopNavigation();
    driver.dispose();
    if (guarded) browser.removeEventListener('beforeunload', beforeUnload);
    guarded = false;
  };
}
