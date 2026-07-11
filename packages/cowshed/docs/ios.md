# iOS development with cowshed (Expo, React Native, Xcode)

This guide assumes posture B (a dedicated `dev` uid — see the deployment postures in `specs/cowshed/14_nix.md`): your
GUI session is your personal account, and every build, agent, and bundler runs as `dev`. Under posture A (single
account) none of the brokering below is needed — everything is already one uid. For **macOS desktop** apps (not
simulators) the same dev-uid boundary applies with a simpler shape — see [desktop.md](desktop.md).

The one topology fact everything follows from: **Xcode has no remote mode, and Simulator.app only shows the invoking
user's simulators.** So cowshed splits the simulator estate:

- **Your simulator** (personal session, native Simulator.app, full fidelity) is an _artifact host_: it runs builds you
  chose to install, delivered through a one-way drop directory.
- **Dev-side headless simulators** are the _agent runtime_: `simctl`, XCUITest, screenshots via `simctl io`, streaming
  via idb. Agents never touch your simulator without a grant.

## Expo: the daily loop

Expo splits perfectly along the uid boundary because its dev loop is already client/server:

- **Data plane — works unchanged.** Metro, the JS bundle, Fast Refresh, the dev menu, DevTools — all plain HTTP/WS on
  loopback, and **loopback is shared across uids**. The app in _your_ simulator talks to Metro running as _dev_ with
  neither side noticing.
- **Control plane — brokered.** Only Expo CLI's simulator control (`i` → `xcrun simctl …`) targets the invoking uid's
  CoreSimulator. The in-image `xcrun` wrapper reroutes exactly that.

The flow:

```sh
# dev side (your remote editor terminal / ssh dev@localhost)
$ cowshed exec raven -- npx expo start --port $PORT      # Metro binds inside the workspace's port block

# your side, once per dev-client build — install the artifact:
$ xcrun simctl install booted /Users/Shared/cowshed-drop/conloca-3f2a9c1b/MyApp.app

# your side, once per session — point the app at Metro:
$ xcrun simctl openurl booted "exp+myapp://expo-development-client/?url=http://127.0.0.1:40961"
```

After that, save-file → Fast Refresh is indistinguishable from single-user Expo. Reload (`r`) and the dev menu ride the
Metro websocket to connected clients, so the dev-side CLI drives _your_ simulator for those. DevTools/debugger frontends
are URLs on the same loopback port — open them in your own browser.

**With `--sim` grants, the two "your side" steps disappear.** The workspace's in-image `xcrun` wrapper routes simulator
verbs through the gateway to a small broker in your session, so Expo CLI's `i` Just Works from the dev side:

```sh
$ cowshed grant raven --sim openurl        # deep links / reconnects: freely grantable
$ cowshed grant raven --sim install        # installs: drop-dir artifacts only, human-gated
```

`openurl` only drives code you already installed; `install` is restricted to drop-dir artifacts and the human-gating
rule — an agent can never push a fresh binary into your session unattended. (Why the gate exists: simulator apps execute
_as you_, with only loosely-emulated iOS containment. Installing a build is running that code as yourself.)

**Native rebuilds** (new native module, config-plugin change) are the only recurring handoff:
`cowshed exec raven -- npx expo run:ios` builds the new dev client dev-side, `cowshed sim export raven` drops it, and
your side (or an optional launchd path-watcher on the drop dir) installs it.

Bare React Native and Flutter get the same loop through the same wrapper — they all drive simulators via `xcrun`.

## What runs where

| Task                                            | Where                                                                                 |
| ----------------------------------------------- | ------------------------------------------------------------------------------------- |
| Metro / Expo dev server, builds, tests, signing | dev, over SSH (signing certs in dev's Keychain — see 14_nix.md)                       |
| Agent test loops (XCUITest, screenshots)        | dev-side headless simulators (`--preset simulator` grant)                             |
| Looking at and poking the app                   | your native Simulator.app, fed by the drop dir                                        |
| Stepping through native code (lldb)             | dev-side headless simulator (cross-uid debugger attach is a no)                       |
| Interface Builder, Instruments GUI, previews    | the dev GUI session: fast-user-switch, or Screen Sharing to dev's session as a window |

## Debugging notes

- JS debugging: the Hermes inspector rides Metro — open the DevTools URL in your own browser. Full loop, no brokering.
- Native debugging: lldb runs dev-side against a dev-side headless simulator; `simctl io` screenshots/recordings and idb
  streaming give agents (and you, in a pinch) eyes on it.
- App logs from _your_ simulator: `log stream` on your side, or an lmao-instrumented app streaming traces to a dev-side
  collector over loopback (`cowshed logs` shows them — telemetry.md).

## When it fights you

- A tool only sees dev-local simulators → it spawned `/usr/bin/xcrun` directly, bypassing the wrapper. That's the safe
  default (your session is unreachable that way); use `cowshed sim export` + your side's `simctl`, or fix the tool's
  PATH.
- `cowshed: sim broker unreachable` (exit 5) → you're not logged in, or the broker launch agent isn't loaded — `next:`
  names the `launchctl` kickstart.
- Xcode-heavy day (Interface Builder, Instruments all afternoon)? Just work in the dev session (posture B1) — the
  boundary is still there; you're visiting.
