# Declarative Host Setup

cowshed's host-side setup — launchd agents, tool defaults, backup exclusions — is a handful of imperative mutations of
`$HOME`. On nix hosts those mutations belong to the user's configuration, not to `adopt`: this spec defines the
home-manager module that owns them, the detection that switches cowshed from _mutating_ to _validating_, and the
deployment postures (which uid runs all of this). The NixOS sibling for CI runners is `services.cowshed-runner`
(10_ci.md); this file is its workstation counterpart.

## `programs.cowshed` (home-manager)

```nix
programs.cowshed = {
  enable = true;               # puts the cowshed binary in home.packages
  gateway.launchd = true;      # dev.cowshed.gateway LaunchAgent (HM manages ~/Library/LaunchAgents on darwin)
  loginAttach = true;          # dev.cowshed.attach LaunchAgent: mount volumes + personal workspaces at login
  linuxConnector = true;       # Linux only: trusted per-attached-workspace netns connector runtime/identity
};
```

With the module enabled, cowshed's launchd plists become a home-manager generation — declared, rollbackable, never
drifting. Combined with the store volume mounted at `/private/cowshed/store` (01_storage.md), the Data-volume home
footprint is: one empty mountpoint directory, HM-managed artifacts, cowshed's own support and cache directories
(`~/Library/Application Support/dev.cowshed`, `~/Library/Caches/dev.cowshed`: installed service binaries and the
gateway's mirrors), and the layer-3 caches the tools already keep at their own defaults (03_caches.md).

On Linux, `programs.cowshed.linuxConnector` (and `services.cowshed-runner` in CI) installs the controller-side launcher
and declares the dedicated connector uid/process restrictions and cgroup subtree. The launcher may enter the already
created workspace netns, bind only `127.0.0.1:7644`, and open only the bind-mounted per-workspace gateway socket; it has
no credential-store, policy-file, CA-key, or upstream-network access. Workspace processes cannot signal, ptrace,
inspect, or join that identity/cgroup. The module owns this host security setup, while attach/detach create, drain, and
remove each ephemeral connector instance. This is not a system-wide listener and allocates no Linux `portBlock`.

## Ownership detection: validate, never mutate

When home-manager owns the host setup, `adopt` and `setup` **validate and refuse to mutate**. Detection is structural,
not configured: HM-created symlinks resolve into `/nix/store` (verification item: confirm across HM's symlink strategies
— `home.file` vs `mkOutOfStoreSymlink`). Per artifact:

| Artifact                            | HM-owned (declarative)                                                          | Unowned (imperative fallback)               |
| ----------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------- |
| Link into the retired caches volume | a conflict naming the module option that declares it; `setup` never rewrites it | `setup` moves the bytes home (03_caches.md) |
| launchd agents                      | validate loaded; `doctor` → `next:` the HM option                               | `adopt` installs plists                     |
| Linux connector runtime             | validate launcher identity/cgroup; `doctor` → `next:` the module option         | `adopt` refuses until installed             |

`doctor` findings in declarative mode always name the nix option, never a mutating command — the self-driving contract
(06_cli.md) pointed at configuration instead of side effects.

**Trusted project policy is always declarative/controller-owned.** Repository identity is a stable, machine-independent
`repo_id`, normalized from a chosen remote URL to lowercase `owner/repo`. The binding records that chosen remote and
validation requires its URL to produce the recorded `repo_id`; discovery may propose a binding but never silently mint
one. Multiple bindings may exist with exactly one primary, while a local-only repository requires an explicit `repo_id`.
Trusted policy lives at `/private/cowshed/store/<owner>/<repo>/policy.json`, with `owner` and `repo` encoded as
separate, path-safe components. It holds the project's standing grants (04_sandbox.md). Home-manager, the trusted host
bootstrap, and the operator's controller verbs (`cowshed grant --project-wide`) write it; `adopt`, `setup`, workspaces,
agents, and repository content may validate it but never create or rewrite it. Missing policy is the empty policy; an
inconsistent binding is a bootstrap error with a declarative remediation hint, never an imperative fallback derived from
a checkout path.

**What stays imperative always**, on every host: volume creation (`diskutil apfs addVolume` — stateful, hardware-
adjacent) and every per-project/per-workspace artifact (images, grants, tokens, CA keys). Native volume creation is
capability-fenced further: only an explicit, foreground `cowshed setup` or `cowshed adopt` (which runs the same host
setup first) uses provisioning mode and may cause the one-time macOS administrator authorization prompt that creates
`cowshed.store` and the build-cache volume, `cowshed.build-cache`, both inside that one authorization session; which of
them a given host's run creates and which it adopts is 01a_apfs.md's ("Dedicated store volume"; "Dedicated build-cache
volume" — the build-cache volume's provisioning uses the same machinery, and its adoption by UUID is stated there). All
other commands and every launchd/background service use existing-only mode. They may validate filesystem, `diskutil`,
mount, and marker evidence, reclaim a launchd StandardErrorPath stub, and remount already-created cowshed volumes at
their canonical `-nobrowse` paths. A plan that would create a volume, write a marker, or otherwise require authorization
is rejected before executor dispatch with `environment-missing` and `next: cowshed setup`. Per-project/per-workspace
artifacts live inside cowshed's volumes, not `$HOME`; there is nothing for a dotfile generation to own. Trusted
repository bindings and policy are the exception: despite living under the cowshed volume, they remain
host-bootstrap-owned and outside workspace authority.

## mbx

Cowshed's package pins mbx by git revision as a flake input; `cowshed setup` installs that revision beside the installed
cowshed binary, and cowshed invokes it through that link for its own reads and for external-work reservations
(05_gateway.md, "Host scheduling"), never by a PATH lookup: a launchd agent's PATH is whatever shell set it up, and on
this host it led with a project's devenv profile, whose `mbx` is exactly the arbitrary one 05_gateway.md says selects
nothing. The pin is mbx v1.29.0 plus changes that are not upstream yet: one front-codes C and C++ predictions so a large
C++ build's task manifest is written, one admits queued work whenever the pool has free capacity, a nested reservation's
included, so no request waits behind another while capacity is idle, one says each hold on the pool on a descriptor the
caller names (`mbx reserve --grant-fd N`, and `MBX_GRANT_FD` for every admission of a tree), so a runner's clocks run
only while its work holds the pool (05_gateway.md, "Parallelism inside a job"), one accepts the caller's
`MBX_SESSION_ROOT` so session sockets can use the workspace's existing short runtime path instead of its long `TMPDIR`,
one skips every mbx Cargo shim when resolving the real Cargo, so an older project shim cannot loop with the installed
route, and one maps the selected compiler's physical default sysroot into portable keys, so naming a Nix toolchain
directly does not prevent action storage and restoration. Distinct installations remain distinct, and arbitrary sysroot
overrides gain no mapping. Reservation and compiler-admission waits block on kqueue/inotify with domain-qualified,
rate-limited shared rescue scans for crash recovery instead of per-waiter polling. The short session root is the
canonical environment's (11_shell.md, "Canonical mbx environment"), not a new namespace or sandbox grant (04_sandbox.md,
"Fixture roots under /tmp"; 08_testing.md, "Fixtures"; 01c_macos.md, "sun_path is 104 bytes"). Further changes key a Nix
cc-wrapper's environment flags through the same model as argv and its store path, key inputs in the Nix store by their
store path, cache compilations that use a Clang precompiled header (every one a chain loads keyed by its bytes), model
more compiler and rustc flags as written, identify a macOS SDK from its own files while refusing host links a Nix
wrapper shapes, record why a compiled result was not stored, and share build-script output under `mbx exec`. The latest
count a crate's edit against the commit HEAD it last compiled under (16_build_volumes.md, "One Cargo environment on
every path"), refresh the store's recency only for hits a build exposed, predict build scripts per target, map a nested
Cargo's own target, and seed a managed target from one donor checkout only, so no target mixes two lineages of one
crate. It is `smoothbricks/mr-boxington` at rev `687faba594cd44b414d5cea8ba29eb9142a01c64`. No project needs to pin it:
the host's mbx serves every Cargo project, which reaches it through the Cargo shim that `cowshed setup` generates from
it, below (the capability is 15_capabilities.md's, its route 11_shell.md's). The installed mbx is never spelled as a Nix
store path or a host path where a command is spawned; its store path is only what the link resolves to.

Managed-target seeding installs a profile's build-script shim pins before publishing a copied unit and rejects damaged
donors, so cancellation cannot leave a fresh launcher without its executable. For unmeasured Cargo test binaries, the
scheduler estimates at most half its CPU budget, rounded up, separately from one default memory share per binary. These
are cold admission estimates, not a harness thread cap; observations replace them.

The pin exposes `mbx stats --scheduler --json --activity`, a version-2 activity-only report that skips store and
historical-summary walks. Queued records become visible only after the publisher owns their lifetime lock. This producer
and bounded-exec's version-2 consumer cut over in one unit, so a gate never mixes the consumer with an older host schema
(11_shell.md, "Cargo test-runner boundaries").

SmoothBricks' own shell carries no mbx input, package or Cargo shim; its plain toolchain `cargo` sits behind the
setup-owned route.

The host's `mbx` entry is a **symlink to the shipped pinned executable, never a copied inode**, so its realpath is the
pinned build's Nix store path, which names the build. Nothing proves a project's mbx against it, because no project's
mbx runs: the capability's route leads PATH with the shim that runs the installed one, so a project that still pins
another build is admitted with the installed one winning (05_gateway.md, "Pool selection and revision"). The host's
answer is derived at each detection and never stored: with no link, or a dangling one, no project gets mbx, and doctor
and `cowshed setup` report a broken link; with a resolving link and its shim directory provisioned, every Cargo project
does (15_capabilities.md, "mbx detection"). On a home-manager host the link holds declaratively: the module puts
cowshed's package, with its pinned mbx beside the cowshed binary, into the generation, and `setup` validates the
symlink's realpath instead of writing it (Ownership detection, above).

There is nothing else to declare for mbx. It runs no daemon and owns no socket (03_caches.md, "mbx"), so
`programs.cowshed` has no mbx option and neither mode installs an agent for it: home-manager owns the generation that
puts the pin in place, and the cache it serves lives on the build-cache volume under mbx's own collection
(03_caches.md). **The host mbx's settings are the operator's.** Cowshed reads the installed mbx's scheduler settings
with `mbx settings get`, and writes only `target.keep` and `target.evict_first`, appending to what the operator listed
(03_caches.md, "mbx"). One more is recommended above its default: `events_max_size`. At the 16 MiB default a large
workspace's warm-up truncated its event recording, and `mbx explain --last` then answered "no earlier recording" for the
units it had dropped (measured 2026-10-28). for the units it had dropped (measured 2026-10-28).

The imperative install is four steps of every `cowshed setup` that reaches its host services: build the flake the
package ships at `nix/mbx` (its `flake.lock` locks mbx's own nixpkgs exactly as mbx's flake does, so the build is the
store path mbx's own flake realizes at that revision), root the result with `--out-link` at
`~/Library/Application Support/dev.cowshed/nix/mbx`, point `dev.cowshed/bin/mbx` at the rooted executable by an atomic
rename of a fresh link, and run that installed mbx's own `mbx setup`, which generates the Cargo shim and its one-line
`mbx-target` sidecar into `dev.cowshed/mbx-shim/`, beside `bin`, so no project provides either. A link that already
names it is left; one naming another build is retargeted; anything there that is not a link is not cowshed's and fails
the run untouched. A host without `nix` keeps no mbx, which is the non-mbx scheduling path, not a failure
(05_gateway.md, "Host scheduling"). `--uninstall` removes the link, the shim directory and the root and nothing under
the nix store. Enforced by `cowshed-core`'s
`the_host_link_is_made_once_retargeted_to_a_new_pin_and_never_replaces_a_foreign_file` and
`the_link_is_observed_without_following_it`, `cowshed-cli`'s `the_host_mbx_is_one_link_to_the_rooted_executable` and the
setup cells `a_repair_links_the_pinned_mbx_after_storage_and_names_it`,
`a_host_without_nix_keeps_no_mbx_and_still_succeeds` and `a_failed_mbx_install_fails_the_run_after_storage_is_reported`.
Tests that must exist beyond those: after an imperative `cowshed setup` and after a home-manager switch, `mbx` resolves
by PATH to a symlink whose realpath is the pinned revision's executable beside the installed cowshed binary; after an
imperative `cowshed setup`, `dev.cowshed/mbx-shim/` holds mbx's generated `cargo` and `mbx-target`, which run that
executable; and no install in either mode writes an mbx plist, socket or second copy anywhere.

## Deployment postures

Paths are uid-relative under every posture: cowshed state is rooted at `/private/cowshed/store`, and trusted policy is
`/private/cowshed/store/<owner>/<repo>/policy.json`. A posture decides _which uid owns that root and its policy_, never
derives a second identity from a machine-local checkout path. Host activation and repository bootstrap MUST run as that
owning uid (or a trusted system service writing on its behalf); a workspace, remote editor, or personal-session broker
cannot bootstrap policy. Changing postures is an explicit reprovision into the new owner's root, not a recursive
`chown`, shared-policy mount, or rediscovery. Multiple repository identities remain separate path-safe owner/repo trees,
with the binding's single primary identity selecting the default policy. Fixed cross-user state was considered and
rejected: sharing would force a group-ACL model through controller-owned policy and grant files (0600), supervisor and
gateway sockets, volume ownership, and Keychain access. Same-uid-or-nothing.

### Posture A — single account (default)

Everything — cowshed, gateway, workspaces, editors, agents — runs as the interactive user. Seatbelt is the only
boundary. Stated plainly: **main is unsandboxed** (02_workspaces.md), so a main-workspace compromise has the full
authority of the personal account — Photos, Documents, browser sessions, Keychain. This is the default model and the
confinement ceiling 00_overview.md documents.

### Posture B — dedicated `dev` uid (recommended hardening)

The entire cowshed stack belongs to a second macOS account `dev`: the store volume (owned by dev's uid), the gateway and
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

home-manager keeps owning dev's _home_ artifacts (dotfiles); nix-darwin owns the daemons. One wrinkle, stated honestly:
dev's **login keychain is not auto-unlocked** without a login session — the first dev shell (or an explicit
`cowshed unlock`) unlocks it, and the gateway defers Keychain reads until then (mirror cache hits and unauthenticated
upstreams work before unlock; credentialed flows queue a clear exit-5 hint).

These daemons always enter cowshed through the existing-only runtime API. Boot or login remounts already-created volumes
and reclaims a leftover launchd stub; that never opens an administrator authorization dialog. Volume creation still
requires foreground `cowshed adopt`.

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
half-supporting it would mean permissions, launchd, and socket semantics that limp. `cowshed doctor` refuses cowshed
volumes owned by a different uid with a clear error — and recognizes posture B2 as healthy: running via a remote-backend
shell or explicit uid switch is fine and expected.

## Tradeoffs

**Imperative-only host setup rejected.** cowshed writing its own LaunchAgent plists on a home-manager host is exactly
the out-of-band drift HM users adopted HM to eliminate; the next `home-manager switch` may fight it. Dual-mode costs one
detection check and keeps both audiences: nix hosts get generation-owned state, everyone else gets the same agents from
`cowshed gateway start`. Layer-3 caches need neither mode: they stay at the tools' own defaults, where no module and no
command has to place them (03_caches.md).

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
