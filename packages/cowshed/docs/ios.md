# iOS development with cowshed (Expo, React Native, Xcode)

This guide assumes posture B (a dedicated `dev` uid — see the deployment postures in `specs/cowshed/14_nix.md`): your
GUI session is your personal account, and every build, agent, and bundler runs as `dev`. Under posture A (single
account) none of the brokering below is needed — everything is already one uid. For **macOS desktop** apps (not
simulators) the same dev-uid boundary applies with a simpler shape — see [desktop.md](desktop.md).

The one topology fact everything follows from: **Xcode has no remote mode, and Simulator.app only shows the invoking
user's simulators.** So cowshed splits the simulator estate:

- **Your simulator** (personal session, native Simulator.app, full fidelity) is an _artifact host_: it runs only builds
  you explicitly approve and install, delivered through a one-way drop directory.
- **Dev-side headless simulators** are the _agent runtime_: `simctl list/boot/install/launch`, XCUITest, screenshots via
  `simctl io`, and streaming via idb all remain dev-side. Agents never list, boot, or otherwise control personal
  devices.

## Expo: the daily loop

Expo splits perfectly along the uid boundary because its dev loop is already client/server:

- **Data plane — works unchanged.** Metro, the JS bundle, Fast Refresh, the dev menu, DevTools — all plain HTTP/WS on
  loopback, and **loopback is shared across uids**. The app in _your_ simulator talks to Metro running as _dev_ with
  neither side noticing.
- **Control plane — narrowly brokered.** The personal-session broker exposes only `openurl` and `install`. Device
  discovery and lifecycle (`list`, `boot`, and related verbs) stay dev-side against dev-owned headless simulators. The
  in-image `xcrun` wrapper may request a brokered operation, but it cannot approve an installation.

The flow:

```sh
# dev side (remote editor terminal connected to the dedicated development account)
$ cowshed exec raven -- npx expo start --port $PORT      # Metro binds inside the workspace's port block

# your side, once per dev-client build — explicitly approve and install the staged artifact:
$ xcrun simctl install booted <drop-dir>/<owner>/<repo>/MyApp.app

# your side, once per session — point the already-installed app at Metro:
$ xcrun simctl openurl booted "exp+myapp://expo-development-client/?url=http://127.0.0.1:40961"
```

After that, save-file → Fast Refresh is indistinguishable from single-user Expo. Reload (`r`) and the dev menu ride the
Metro websocket to connected clients, so the dev-side CLI drives _your_ simulator for those. DevTools/debugger frontends
are URLs on the same loopback port — open them in your own browser.

`--sim` grants do not remove the personal consent step. They authorize the wrapper to request one of the broker's two
operations; they never grant device discovery or lifecycle control:

```sh
$ cowshed grant raven --sim openurl        # deep links for an already-approved install
$ cowshed grant raven --sim install        # eligible drop-dir artifacts only; still requires personal approval
```

`openurl` only drives code you already installed. An `install` request names one immutable artifact under
`<drop-dir>/<owner>/<repo>/`, where `owner` and `repo` are the separately validated and encoded components of the
primary `repo_id`, and completes only after the human explicitly approves that artifact in the personal session. An
agent cannot approve or automate it. An optional path watcher may notify the human or stage an artifact for review, but
MUST NOT call `simctl install`, invoke broker `install`, launch, or relaunch an app.

**Native rebuilds** (new native module, config-plugin change) are the only recurring handoff:
`cowshed exec raven -- npx expo run:ios` builds the new dev client dev-side and `cowshed sim export raven` stages it.
The personal-side human then reviews and installs it; a watcher may only notify or stage.

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
- App logs from _your_ simulator: `log stream` on your side, or app instrumentation streaming traces to a dev-side
  collector over loopback (`cowshed logs` shows them — telemetry.md).

## When it fights you

- A tool only sees dev-local simulators → it spawned `/usr/bin/xcrun` directly, bypassing the wrapper. That's the safe
  default (your session is unreachable that way); use `cowshed sim export` + your side's `simctl`, or fix the tool's
  PATH.
- `cowshed: sim broker unreachable` (exit 5) → you're not logged in, or the broker launch agent isn't loaded — `next:`
  names the `launchctl` kickstart.
- Xcode-heavy day (Interface Builder, Instruments all afternoon)? Just work in the dev session (posture B1) — the
  boundary is still there; you're visiting.
