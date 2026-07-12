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
  linuxConnector = true;       # Linux only: trusted per-attached-workspace netns connector runtime/identity
  goEnvDefaults = true;        # host-side go hygiene that is NOT per-workspace (GOTOOLCHAIN=local guidance; the
                               #   per-workspace GOENV file stays cowshed-written, in-image — 03_caches.md)
  tmutilExclusions = true;     # volume-level TM exclusions for cowshed.store/cowshed.caches (activation script;
                               #   pending the TM default-inclusion verification, kickoff)
};
```

With the module enabled, cowshed's last `~/Library` residue (the launchd plists) becomes a home-manager generation —
declared, rollbackable, never drifting. Combined with the store volume mounted at `~/.cowshed` (01_storage.md), the
Data-volume home footprint is: one empty mountpoint directory, plus HM-managed artifacts.

On Linux, `programs.cowshed.linuxConnector` (and `services.cowshed-runner` in CI) installs the controller-side launcher
and declares the dedicated connector uid/process restrictions and cgroup subtree. The launcher may enter the already
created workspace netns, bind only `127.0.0.1:7644`, and open only the bind-mounted per-workspace gateway socket; it has
no credential-store, policy-file, CA-key, or upstream-network access. Workspace processes cannot signal, ptrace,
inspect, or join that identity/cgroup. The module owns this host security setup, while attach/detach/restore create,
drain, and remove each ephemeral connector instance. This is not a system-wide listener and allocates no Linux
`portBlock`.

## Ownership detection: validate, never mutate

When home-manager owns the host setup, `adopt` and `ensure` **validate and refuse to mutate**. Detection is structural,
not configured: HM-created symlinks resolve into `/nix/store` (verification item: confirm across HM's symlink strategies
— `home.file` vs `mkOutOfStoreSymlink`). Per artifact:

| Artifact                | HM-owned (declarative)                                                   | Unowned (imperative fallback)      |
| ----------------------- | ------------------------------------------------------------------------ | ---------------------------------- |
| Cache-subtree symlinks  | validate targets; `doctor` → `next: enable programs.cowshed.relocations` | `adopt` writes them (03_caches.md) |
| launchd agents          | validate loaded; `doctor` → `next:` the HM option                        | `adopt` installs plists            |
| Linux connector runtime | validate launcher identity/cgroup; `doctor` → `next:` the module option  | `adopt` refuses until installed    |
| TM exclusions           | validate; hint the HM option                                             | `adopt` runs `tmutil`              |

`doctor` findings in declarative mode always name the nix option, never a mutating command — the self-driving contract
(06_cli.md) pointed at configuration instead of side effects.

**Trusted project policy is always declarative/controller-owned.** Repository identity is a stable, machine-independent
`repo_id`, normalized from a chosen remote URL to lowercase `owner/repo`. The binding records that chosen remote and
validation requires its URL to produce the recorded `repo_id`; discovery may propose a binding but never silently mint
one. Multiple bindings may exist with exactly one primary, while a local-only repository requires an explicit `repo_id`.
Trusted policy lives at `~/.cowshed/<owner>/<repo>/policy.json`, with `owner` and `repo` encoded as separate, path-safe
components. Home-manager or the trusted host bootstrap owns that file; `adopt`, `ensure`, workspaces, agents, and
repository content may validate it but never create or rewrite it. Missing policy or an inconsistent binding is a
bootstrap error with a declarative remediation hint, never an imperative fallback derived from a checkout path.

**What stays imperative always**, on every host: volume creation (`diskutil apfs addVolume` — stateful, hardware-
adjacent) and every per-project/per-workspace artifact (images, grants, tokens, CA keys). Those live inside cowshed's
volumes, not `$HOME`; there is nothing for a dotfile generation to own. Trusted repository bindings and policy are the
exception: despite living under the cowshed volume, they remain host-bootstrap-owned and outside workspace authority.

## Deployment postures

Paths are uid-relative under every posture: cowshed state is rooted at `~/.cowshed`, and trusted policy is
`~/.cowshed/<owner>/<repo>/policy.json`. A posture decides _which uid owns that root and its policy_, never derives a
second identity from a machine-local checkout path. Host activation and repository bootstrap MUST run as that owning uid
(or a trusted system service writing on its behalf); a workspace, remote editor, or personal-session broker cannot
bootstrap policy. Changing postures is an explicit reprovision into the new owner's root, not a recursive `chown`,
shared-policy mount, or rediscovery. Multiple repository identities remain separate path-safe owner/repo trees, with the
binding's single primary identity selecting the default policy. Fixed cross-user state was considered and rejected:
sharing would force a group-ACL model through controller-owned policy and grant files (0600), supervisor and gateway
sockets, volume ownership, and Keychain access. Same-uid-or-nothing.

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
  - shells: connect to the dedicated account over localhost SSH (Remote Login enabled, scoped to that account, key auth)
    or use an equivalent explicit uid switch for quick shells;
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

The GUI-toolchain postures below (simulators, then desktop apps) all rest on one boundary fact and one physics limit,
stated once here and reused: **a process runs under exactly one uid, and macOS shows a window only in that uid's own GUI
session.** Everything else is choosing which uid runs a given artifact.

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

The one sanctioned path for build products to cross the uid boundary is a deployment-configured **one-way drop
directory**, written here as `<drop-dir>/<owner>/<repo>/`. The dedicated development account owns it; the personal side
can read it, and sticky-bit semantics prevent consumers from rewriting another producer's artifacts. Dev writes
**immutable artifacts** (a built `.app`, via `cowshed sim export` — 06_cli.md); the personal side reads. This is the
CI-artifact pattern — an immutable product crosses, never a live tree — and it is explicitly **not** cross-uid file
sharing, which stays rejected below. An optional personal-session path watcher may notify the human or stage an artifact
for review, but it MUST NOT install, launch, or relaunch anything.

The consent rule, and why it exists: **simulator apps execute as the invoking user, with only loosely-emulated iOS
containment** — installing a build into the personal-session simulator is running that code as yourself, which is
precisely the hole posture B closes. Personal-session installs are therefore **human-initiated, per-artifact**. The
personal-session broker exposes only `openurl` and `install`: `openurl` may drive an already-consented install under a
workspace grant, while `install` accepts only an immutable drop-dir artifact and completes only after explicit human
approval for that artifact. A watcher can notify or stage but cannot invoke this approval or installation path. Device
`list` and `boot` remain dev-side operations against dev-owned headless simulators and are never brokered into the
personal session. Agent test loops stay on dev-side headless simulators, always.

### macOS desktop apps: three lanes

The uid boundary confines **agents and build-time code execution** — unattended, semi-trusted. A finished app the
**human chooses to run as themselves** is a deliberate boundary exit at a human decision point, like installing from the
App Store — not a violation. Desktop apps you develop therefore have three legitimate lanes, and cowshed serves all
three rather than treating daily use as an escape hatch:

| Lane                          | Runs as               | Appears in                                           | Trigger           | Consent                                                     |
| ----------------------------- | --------------------- | ---------------------------------------------------- | ----------------- | ----------------------------------------------------------- |
| **1 — E2E / agent testing**   | dev                   | dev's background GUI session                         | agent, unattended | none — it's confined (dev running dev's build)              |
| **2 — interactive debugging** | dev                   | fast-user-switch / Screen Sharing into dev's session | human, occasional | none — same uid                                             |
| **3 — daily use of your app** | **the personal user** | your own session, natively                           | human, deliberate | **`app promote`** is the consent — the point, not a warning |

**Lanes 1–2 run the drop-dir build in place, as dev — no broker, no grant, no promotion.** Driving a dev-session app as
dev is just dev running dev's app (like dev-side headless simulators need no `/sim/` grant): agents automate it through
accessibility APIs / AppleScript, and screenshots come out. Lane 1 uses the **dev-side background GUI session** (the
persistent Aqua session recommended for simulator reliability above, now doing double duty). This makes desktop testing
**simpler** than the simulator case: same uid, so no `/sim/`-style cross-session broker is involved at all.

**Lane 3 is `cowshed app export` → `cowshed app promote`:**

- `cowshed app export <ws>` copies the built `.app` to the same one-way drop dir (the Mac-target sibling of
  `cowshed sim export` — 02_workspaces.md/06_cli.md).
- `cowshed app promote [artifact]` is **run by the human, in the personal session** — it writes `~/Applications`, which
  dev cannot, so it is structurally a personal-session verb, **not a sandbox or gateway call and not grantable to any
  agent**. It verifies the code signature (**Developer-ID required by default**; ad-hoc into `~/Applications` needs
  `--force` because ad-hoc trips Gatekeeper), optionally checks the artifact came from a landed/clean commit, copies
  into `~/Applications` (or `/Applications` with escalation), and clears any quarantine xattr. The promoted copy is
  yours — launch it, Dock it, make it a login item, use it for months.

The consent asymmetry, stated plainly: lanes 1–2 need no consent because the app is **confined** (it runs as dev under
posture B); lane 3's consent **is** the point — the app runs as you, with your full authority (Photos, Keychain,
Documents), which is exactly what posture B fences off for agents and what a human deliberately, per-build, chooses to
exit. `promote` is that choice, and no agent can make it.

**Gatekeeper / quarantine** (verified: `cp`/`cp -R` applies no `com.apple.quarantine` xattr — that xattr comes from
downloader apps, not the filesystem copy): a Developer-ID-signed app dropped by dev's copy opens cleanly in the personal
session; an **ad-hoc**-signed app still trips Gatekeeper on first launch (right-click-open or an `spctl` allowance).
This is one more reason lanes 1–2 are smoother (dev's own session can permit its own builds) and lane 3 wants real
Developer-ID signing — which is precisely why dev holds the signing identity.

**Live iteration in lane 3.** An Electron / React-Native-desktop promoted app can still point at dev's dev-server over
**loopback** (shared across uids — the same trick as Expo/Metro in the iOS story): you use yesterday's promoted shell
while today's JS and hot-reload come from dev. Native SwiftUI cannot do this — there, daily-driving a new build is a
re-`promote`.

**The one hard limit** (physics, not policy): an app cannot simultaneously run **as dev** and show its window in the
**personal** session — macOS has no cross-session window display (Screen Sharing streams a whole session, it does not
relocate one window). The three lanes each pick a side cleanly, so this costs nothing: testing and debugging want
run-as-dev; using wants run-as-you, and `promote` is the bridge between them.

### Explicitly unsupported (v1)

**Cross-uid FILE access**: personal-account processes reading or writing the dedicated development account's home
directly — for example, a personal-session editor opening dev-owned files in place or a shared group-ACL tree. B2 does
not need it (the personal side hosts only GUI frontends speaking to dev-side backends over SSH/sockets), and
half-supporting it would mean permissions, launchd, and socket semantics that limp. Group-ACL sharing is a roadmap note,
nothing more. `cowshed doctor` refuses cowshed volumes owned by a different uid with a clear error (per-user volume
naming is noted as a future extension for multi-posture machines) — and recognizes posture B2 as healthy: running via a
remote-backend shell or explicit uid switch is fine and expected.

## Tradeoffs

**Imperative-only host setup rejected.** `adopt` silently symlinking `~/.cargo/registry` on a home-manager host is
exactly the out-of-band drift HM users adopted HM to eliminate; the next `home-manager switch` may fight it. Dual-mode
costs one detection check and keeps both audiences: nix hosts get generation-owned state, everyone else keeps
zero-config adopt.

**Fixed shared path rejected** (see Deployment postures): a group-writable shared state root breaks the 0600
controller-owned policy and grant model and Keychain scoping for no gain — the uid boundary is the point of posture B,
not a shared filesystem. The separately configured one-way artifact drop is not a state root and grants no access back
into cowshed.

**Split-session cross-uid file sharing rejected (v1)**: the one configuration that looks convenient — personal GUI
editing dev-owned files in place — reintroduces every cross-uid ACL problem the fixed-path rejection avoided, at the
exact boundary posture B exists to harden. Remote-backend editors make it unnecessary.

**A personal-session `--app open` grant rejected.** It is tempting to mirror `--sim` with an `--app open` axis that
launches a desktop app in the personal session on an agent's behalf. Rejected: the only personal-session path for a
desktop app is `app promote`, and promote is **human-run by construction** (it writes `~/Applications`, unreachable from
dev or any sandbox). Giving an agent a verb that runs arbitrary code as the personal user would reintroduce exactly the
authority posture B removes — with none of the simulator's loose containment. Dev-session app runs (lanes 1–2) need no
grant at all; personal-session use is promote-only, and the human makes the call.

**Simulator screen-streaming as the default rejected.** Streaming a dev-session Simulator to the personal desktop (idb
video + HID, or Screen Sharing) was the first design; the artifact handoff beats it — the human gets the native,
full-fidelity Simulator.app with zero latency, and the only thing that crosses the boundary is an immutable build
product. Streaming (idb) remains the dev-side path for agents that need to observe a headless simulator, not the human's
daily loop.
