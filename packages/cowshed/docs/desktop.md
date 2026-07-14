# macOS desktop apps with cowshed

This guide assumes posture B (a dedicated `dev` uid — `specs/cowshed/14_nix.md`): your GUI session is your personal
account, and builds and agents run as `dev`. Under posture A (single account) there is no boundary to cross — you build
and run as one user. The iOS/simulator story is [ios.md](ios.md); desktop apps are actually **simpler**, because the
runtime is a native process, not a simulator.

One fact drives everything: **a process runs as one uid, and macOS shows its window only in that uid's own GUI
session.** So the question is never "how do I see a dev-run app in my session" (you can't relocate a window across
sessions) — it's "which uid runs this artifact." That splits cleanly into three lanes.

## The three lanes

| Lane                          | Runs as | Appears in                                            | You do this to…                                             |
| ----------------------------- | ------- | ----------------------------------------------------- | ----------------------------------------------------------- |
| **1 — agent / E2E testing**   | dev     | dev's background GUI session                          | let agents drive the app (accessibility APIs / AppleScript) |
| **2 — interactive debugging** | dev     | fast-user-switch or Screen Sharing into dev's session | poke at it yourself, occasionally                           |
| **3 — daily use**             | **you** | your own session, natively                            | actually use the app you built                              |

Lanes 1 and 2 need no cowshed plumbing at all — they are just **dev running dev's build in dev's session**, same uid, no
grant, no broker. Lane 3 is the one that crosses to you, and it crosses through a single human-run verb.

## Lanes 1–2: run and test as dev

Build as usual, then run the built `.app` in dev's session:

```sh
# dev side (remote editor terminal connected to the dedicated development account)
$ cowshed exec myapp -- npm run build           # or xcodebuild, tauri build, …
$ open dist/MyApp.app                            # launches as dev, in dev's session
```

Agents automate it in place through the standard accessibility/AppleScript path used for desktop applications. Lane 1
wants a **persistent background GUI session for dev** (the one already kept alive for simulator reliability — see
ios.md); the app has a real Aqua session to draw into, agents send events and read the accessibility tree, and
screenshots come back as artifacts. Boundary intact: untrusted code never leaves the dev uid.

For lane 2, fast-user-switch into dev (or Screen Sharing to dev's session as a window on your desktop) and debug with
the app right there.

## Lane 3: use it yourself — `app export` → `app promote`

To run the app as **you**, in your own session, it has to be installed for your user. That is a deliberate step, and
it's the consent point:

```sh
# dev side: drop the built app
$ cowshed app export myapp                       # stages MyApp.app under <drop-dir>/<owner>/<repo>/

# your side (personal session, YOU run this):
$ cowshed app promote                            # verify signature, install to ~/Applications, clear quarantine
~/Applications/MyApp.app
```

`cowshed app promote` runs **as you, in your session** — it writes `~/Applications`, which dev cannot touch, so it is
structurally a personal verb: **no agent, and nothing in a sandbox, can invoke it.** It verifies the code signature
(**Developer-ID by default**; an ad-hoc build needs `--force` because ad-hoc trips Gatekeeper), optionally checks the
build came from a landed commit, installs into `~/Applications` (`--system` for `/Applications`), and clears any
quarantine flag. The result is _yours_ — Dock it, make it a login item, use it for months.

Why the asymmetry (no consent for lanes 1–2, explicit consent for lane 3): a dev-run app is **confined** — it has dev's
authority under posture B, not yours. A promoted app runs with **your** full authority (Photos, Keychain, Documents) —
which is exactly what posture B fences off from agents. Lane 3 exits that boundary on purpose, per build, at your
keystroke. That's not a hole; it's you choosing to run software you made, like installing anything else.

## Live iteration while you use it

If the app is **Electron or React-Native-desktop**, the promoted copy can point at dev's dev-server over **loopback**
(shared across uids — the same trick that makes Expo hot-reload work in ios.md): you daily-drive yesterday's promoted
shell while today's JS and hot-reload stream from `cowshed exec myapp -- npm run dev` as dev. **Native SwiftUI/AppKit**
can't do that — there, picking up a new build is another `app promote`.

## Signing

Lane 3 wants a real **Developer-ID** signature so Gatekeeper (and notarization-dependent features) are happy — which is
why dev holds the signing identity (`specs/cowshed/14_nix.md`: signing certs live in dev's Keychain, a separation win).
Lanes 1–2 tolerate ad-hoc (dev's own session can allow its own builds), but for anything you'll actually use, sign it
properly on the dev side and `promote` installs it without a fight. (Verified: `cp` into the drop dir adds no
`com.apple.quarantine` xattr, so a Developer-ID drop opens on first launch with no right-click dance.)

## When it fights you

- **"I want the app running as dev but visible in my session."** Not possible — macOS won't show one uid's window in
  another's session. Pick a lane: test/debug as dev (view via Screen Sharing), or `promote` and run as you.
- **Gatekeeper blocks a promoted app.** It's ad-hoc-signed — `promote` warned you and needed `--force`. Sign with
  Developer-ID on the dev side, or right-click-open once.
- **An agent asks to launch the app in your session.** It can't, by design — there is no agent verb for that. Have it
  `cowshed app export`; you `promote`.
