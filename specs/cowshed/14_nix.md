# Declarative Host Setup

cowshed's host-side setup — cache-subtree symlinks, launchd agents, tool defaults, backup exclusions — is a handful of
imperative mutations of `$HOME`. On nix hosts those mutations belong to the user's configuration, not to `adopt`: this
spec defines the home-manager module that owns them, the detection that switches cowshed from _mutating_ to
_validating_, and the deployment postures (which uid runs all of this). The NixOS sibling for CI runners is
`services.cowshed-runner` (10_ci.md); this file is its workstation counterpart.

## `programs.cowshed` (home-manager)

```nix
programs.cowshed = {
  enable = true;               # puts the cowshed binary in home.packages
  relocations = true;          # cache-subtree symlinks: ~/.cargo/{registry,git}, ~/.cache/zig,
                               #   ~/.gradle/caches (+ other gradle cache dirs), sccache default → ~/.cowshed/caches/…
  gateway.launchd = true;      # dev.cowshed.gateway LaunchAgent (HM manages ~/Library/LaunchAgents on darwin)
  loginAttach = true;          # dev.cowshed.attach LaunchAgent: mount volumes + personal workspaces at login
  goEnvDefaults = true;        # host-side go hygiene that is NOT per-workspace (GOTOOLCHAIN=local guidance; the
                               #   per-workspace GOENV file stays cowshed-written, in-image — 03_caches.md)
  tmutilExclusions = true;     # volume-level TM exclusions for cowshed.store/cowshed.caches (activation script;
                               #   pending the TM default-inclusion verification, kickoff)
};
```

With the module enabled, cowshed's last `~/Library` residue (the launchd plists) becomes a home-manager generation —
declared, rollbackable, never drifting. Combined with the store volume mounted at `~/.cowshed` (01_storage.md), the
Data-volume home footprint is: one empty mountpoint directory, plus HM-managed artifacts.

## Ownership detection: validate, never mutate

When home-manager owns the host setup, `adopt` and `ensure` **validate and refuse to mutate**. Detection is structural,
not configured: HM-created symlinks resolve into `/nix/store` (verification item: confirm across HM's symlink strategies
— `home.file` vs `mkOutOfStoreSymlink`). Per artifact:

| Artifact               | HM-owned (declarative)                                                   | Unowned (imperative fallback)      |
| ---------------------- | ------------------------------------------------------------------------ | ---------------------------------- |
| Cache-subtree symlinks | validate targets; `doctor` → `next: enable programs.cowshed.relocations` | `adopt` writes them (03_caches.md) |
| launchd agents         | validate loaded; `doctor` → `next:` the HM option                        | `adopt` installs plists            |
| TM exclusions          | validate; hint the HM option                                             | `adopt` runs `tmutil`              |

`doctor` findings in declarative mode always name the nix option, never a mutating command — the self-driving contract
(06_cli.md) pointed at configuration instead of side effects.

**What stays imperative always**, on every host: volume creation (`diskutil apfs addVolume` — stateful, hardware-
adjacent) and every per-project/per-workspace artifact (images, grants, tokens, CA keys). Those live inside cowshed's
volumes, not `$HOME`; there is nothing for a dotfile generation to own.

## Deployment postures

Paths are uid-relative — `~/.cowshed` — under every posture; a posture decides _which uid_, never _which path_. A fixed
non-user path (`/opt/cowshed`, `/Users/Shared`) was considered and rejected: cross-uid sharing would force a group-ACL
model through grant files (0600, controller-owned), supervisor and gateway sockets, volume ownership, and Keychain
access. Same-uid-or-nothing.

### Posture A — single account (default)

Everything — cowshed, gateway, workspaces, editors, agents — runs as the interactive user. Seatbelt is the only
boundary. Stated plainly: **main is unsandboxed** (02_workspaces.md), so a main-workspace compromise has the full
authority of the personal account — Photos, Documents, browser sessions, Keychain. This is today's model and the
confinement ceiling 00_overview.md documents.

### Posture B — dedicated `dev` uid (recommended hardening)

The entire cowshed stack belongs to a second macOS account `dev`: both volumes (owned by dev's uid), the gateway and its
Keychain items, the launchd jobs, the home-manager generation, every editor backend and agent. What it buys:

- a **kernel uid boundary outside Seatbelt** that covers exactly the surfaces Seatbelt doesn't — unsandboxed main and
  the controller tooling itself;
- credential and TCC separation: dev's Keychain holds registry/git tokens, the personal account's holds your life; TCC
  prompts (camera, Photos, Desktop) simply don't exist for dev;
- a **fully rebuildable dev identity**: dev's home is an HM generation + the cowshed mountpoint + git remotes — nothing
  precious, `deleteVolume`-and-reprovision is a real recovery path. The symmetry with 10_ci's dedicated runner user on
  NixOS is exact.

Two ways to live in it:

- **B1 — full login as dev** (fast-user-switching): simplest semantics; GUI, editors, everything as dev.
- **B2 — remote-backend session (recommended)**: ONE GUI session (the personal account); ALL workloads as dev. Treat the
  dev uid as a remote dev machine that happens to be localhost — the devcontainer pattern without containers:
  - shells: `ssh dev@localhost` (Remote Login enabled, scoped to dev, key auth) or `sudo -u dev -i` for quick ones;
  - editors: remote backends — VS Code/Cursor Remote-SSH to `dev@localhost`, JetBrains Gateway, Zed remote. The GUI
    client runs as the personal user; the server, LSPs, watchers, terminals, agents, and builds all run as dev.

**Services under posture B**: user LaunchAgents never fire for a uid with no graphical login, so the gateway, autosave,
and attach jobs become **nix-darwin LaunchDaemons with `UserName = dev`** — boot-time, session-independent, declarative:

```nix
services.cowshed = {            # nix-darwin sibling of programs.cowshed
  user = "dev";
  gateway.enable = true;        # LaunchDaemon, UserName = dev
  autosave.enable = true;
  attachAtBoot = true;
};
```

home-manager keeps owning dev's _home_ artifacts (relocations, go defaults, dotfiles); nix-darwin owns the daemons. One
wrinkle, stated honestly: dev's **login keychain is not auto-unlocked** without a login session — the first dev shell
(or an explicit `cowshed unlock`) unlocks it, and the gateway defers Keychain reads until then (mirror cache hits and
unauthenticated upstreams work before unlock; credentialed flows queue a clear exit-5 hint).

### iOS, Xcode, and simulators (posture B)

The facts that shape everything here: **Xcode has no client/server split** — no remote-development mode exists — and
**Simulator.app displays only the invoking user's CoreSimulatorService devices**; there is no cross-uid or remote
attach. Commercial "remote simulators" (Appetize, BrowserStack App Live) are the same trick underneath: run the real
Simulator on the real Mac and stream its screen. So the topology is not negotiable, only its division of labor:

- **Dev-side headless simulators are the agent/CI runtime.** `simctl boot/install/launch`, XCUITest via
  `xcodebuild test`, screenshots and recordings via `simctl io`, streaming/HID via idb where needed. Build, sign, and
  test all run as dev over SSH — the standard headless-CI recipes apply verbatim: signing identities live in **dev's**
  keychain (a separation win — the personal keychain never holds a distribution cert), unlocked by the once-per-boot
  unlock above, with `security set-key-partition-list` so `codesign` never prompts. Simulator builds are ad-hoc signed —
  no provisioning barrier exists for a build crossing uids.
- **The personal-session simulator is an artifact host.** Full-fidelity native Simulator.app, run by the human, for the
  human — fed through the artifact handoff below and driven through the `--sim` grant class (04_sandbox.md,
  05_gateway.md). Operational note: CI folklore holds that simulator workloads are more reliable when the executing uid
  has an Aqua session alive somewhere; a persistent background GUI session for dev (log in once, switch away) is the
  recommended substrate — verification item (kickoff).
- **Irreducibly-GUI tasks** — Interface Builder, Instruments GUI (`xctrace` is the CLI alternative), SwiftUI previews,
  signing wizards, physical-device pairing — are **B1 moments**: fast-user-switch into dev's session, or macOS Screen
  Sharing to dev's session as a window on the personal desktop.

Stated honestly: Xcode is the workload where the uid boundary charges the most rent. Expo/React Native daily driving
stays fully in B2 (`docs/ios.md` is the walkthrough — the dev loop rides shared loopback and needs nothing from the GUI
session); Xcode-heavy days may simply prefer B1.

### Artifact handoff: the drop dir

The one sanctioned path for build products to cross the uid boundary is a **one-way drop directory**:
`/Users/Shared/cowshed-drop/<project_id>/` — dev-owned, world-readable, sticky-bit semantics. Dev writes **immutable
artifacts** (a built `.app`, via `cowshed sim export` — 06_cli.md); the personal side reads. This is the CI-artifact
pattern — an immutable product crosses, never a live tree — and it is explicitly **not** cross-uid file sharing, which
stays rejected below. The personal side may run an optional launchd path-watcher that auto-installs and relaunches new
artifacts into the booted simulator (`simctl install` + `launch`).

The consent rule, and why it exists: **simulator apps execute as the invoking user, with only loosely-emulated iOS
containment** — installing a build into the personal-session simulator is running that code as yourself, which is
precisely the hole posture B closes. So personal-session installs are **human-initiated, per-artifact**; agents cannot
target the personal simulator except through the `--sim` grant class (04_sandbox.md), where `openurl` (driving an
already-consented install) is grantable freely and `install` is additionally bound to drop-dir artifacts and the
human-gating rule. Agent test loops stay on dev-side headless simulators, always.

### Explicitly unsupported (v1)

**Cross-uid FILE access**: personal-account processes reading or writing `/Users/dev` directly — an editor opening dev
files as the personal user, shared group-ACL trees. B2 does not need it (the personal side hosts only GUI frontends
speaking to dev-side backends over SSH/sockets), and half-supporting it would mean permissions, launchd, and socket
semantics that limp. Group-ACL sharing is a roadmap note, nothing more. `cowshed doctor` refuses cowshed volumes owned
by a different uid with a clear error (per-user volume naming, e.g. `cowshed.store.<uid>`, is noted as a future
extension for multi-posture machines) — and recognizes posture B2 as healthy: running via an ssh or `sudo -u dev` shell
is fine and expected.

## Tradeoffs

**Imperative-only host setup rejected.** `adopt` silently symlinking `~/.cargo/registry` on a home-manager host is
exactly the out-of-band drift HM users adopted HM to eliminate; the next `home-manager switch` may fight it. Dual-mode
costs one detection check and keeps both audiences: nix hosts get generation-owned state, everyone else keeps
zero-config adopt.

**Fixed shared path rejected** (see Deployment postures): a group-writable `/opt/cowshed` breaks the 0600
controller-owned grant model and Keychain scoping for no gain — the uid boundary is the point of posture B, not a shared
filesystem.

**Split-session cross-uid file sharing rejected (v1)**: the one configuration that looks convenient — personal GUI
editing dev-owned files in place — reintroduces every cross-uid ACL problem the fixed-path rejection avoided, at the
exact boundary posture B exists to harden. Remote-backend editors make it unnecessary.

**Simulator screen-streaming as the default rejected.** Streaming a dev-session Simulator to the personal desktop (idb
video + HID, or Screen Sharing) was the first design; the artifact handoff beats it — the human gets the native,
full-fidelity Simulator.app with zero latency, and the only thing that crosses the boundary is an immutable build
product. Streaming (idb) remains the dev-side path for agents that need to observe a headless simulator, not the human's
daily loop.
