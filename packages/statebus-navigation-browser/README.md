# StateBus Browser Navigation

An explicitly owned browser adapter for `@smoothbricks/statebus-navigation-core`. Production dependencies contain no
React, React Router, or StateBus singleton. Importing the package does not read `window` and is safe during SSR.

## Compose once, publish typed intents

```ts
import { connectBrowserNavigation } from '@smoothbricks/statebus-navigation-browser';
import { navigationRequestId } from '@smoothbricks/statebus-navigation-core';

// The composition supplies its existing NavigationChannel<string, NavigationLocation>.
const dispose = connectBrowserNavigation({ window, channel });
channel.publish({
  type: 'navigationRequested',
  request: {
    requestId: navigationRequestId('open-billing-1'),
    intent: { kind: 'push', to: '/settings/billing?tab=invoices#recent' },
  },
});
// Dispose with the application runtime. It is safe to call this more than once.
dispose();
```

The channel reads production reducer state and delivers events after the complete StateBus wave is reduced. Screens
read that state and publish intents. They do not call History or maintain a second location authority. Every logical
request receives a distinct branded `NavigationRequestId`; brands are erased, not allocated wrapper objects.

`navigate` avoids adding an identical URL. `push` adds an entry, `replace` preserves the current entry's history state,
and `back`/`forward`/`go` call native traversal. The pure `planBrowserNavigation` resolves relative URLs, queries and
hashes before the shell executes them. Internal targets must be same-origin HTTP(S). Cross-origin targets require an
explicit `external` intent. Credentials in URLs, unsafe schemes, zero/fractional/out-of-WebIDL-range deltas are rejected
with typed failures. In particular, a delta that would wrap to zero cannot accidentally reload the document.

## Observe facts, not predicted routes

After a same-document write, the adapter reads the actual browser location. It also observes `popstate` and
`hashchange`, suppressing duplicate hash observations following a traversal. `current()` retains the same immutable
location object while the URL is unchanged. Callback functions are created at installation, not per browser event.

`navigationDispatched` acknowledges primitive dispatch, not successful destination loading or a completed payment.
Native traversal can be out of range and has no completion signal in that case. It remains `dispatched` until a real
location observation or explicit cancellation; no timer guesses a URL. An abort prevents work that has not started
and suppresses late adapter outcomes, but cannot undo a History/Location operation already issued.

For integration with a router that writes history itself, use `createBrowserNavigation({ window })` and
`connectNavigation({ channel, driver })`; call `driver.observe()` from the router's committed-location subscription.
The adapter does not patch global `history.pushState`/`replaceState` or claim a React Router binding. Dispose both the
connection and the driver in that lower-level composition. A future Expo implementation uses the platform-neutral
core contracts without importing this browser package.

## Guards and external navigation

The reducer blocks bus-originated intents when `navigationGuardChanged` sets a reason. A matching
`navigationConfirmed` admits that intent; `navigationCancelled` abandons it. A guard arriving later in the same wave
still prevents execution. Browser Back/Forward has already happened at `popstate`: it is not vetoed or rolled back.
The adapter always reports the actual location, even while a guard is active.

`connectBrowserNavigation` installs native `beforeunload` only while a guard is active and removes it when cleared or
disposed. Browser user-activation and platform rules determine whether a dialog is displayed; the reason is not a
custom native-dialog message. This is not guaranteed delivery protection on mobile/process termination.

Explicit external intents support `assign`, `replace` and `new-tab`. New tabs use `noopener,noreferrer`; a null
`window.open` result is not classified as failure because a successful opener-isolated tab can return null. Popup
blocking and transient user-activation policy remain browser-owned. Do not use an animation-frame-delayed publisher
for actions that require transient activation; the Chromium tests exercise microtask publication from an actual click.

## Tests

```sh
nx lint statebus-navigation-browser
nx test statebus-navigation-browser
nx run statebus-navigation-browser:test-browser
```

Fast Bun properties test URL policy without a browser. The separate browser target uses Playwright with real Chromium,
a real MicrotaskStateBus, the production reducer/driver, and a React StrictMode screen. It exercises URL writes,
history, guard confirmation, external Location calls, opener isolation, unload dialogs and subscription disposal. Set
`PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` to a provisioned Chromium, use a detected system installation, or install the
matching Playwright browser. The StateBus browser CI workflow runs this target independently of the fast unit lane.
