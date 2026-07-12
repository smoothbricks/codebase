# cowshed — implementation kickoff

Prompt for a fresh session: launch a multi-agent implementation workflow for `packages/cowshed`. Everything below is the
context that session needs; the authoritative design is in the specs, not this file. Nothing in this summary implies
that the specified behavior is already implemented.

## What cowshed is

Warm git workspaces for macOS/APFS and Linux/ZFS: every workspace is a copy-on-write clone, sandboxed by default, with
shared toolchain/package caches and an egress gateway holding all secrets. Rust applications consume `cowshed-core`
directly, Bun/Node applications use `cowshed-napi`, and shell-based agents use the CLI.

## Read first (in order)

1. `specs/cowshed/*.md` (repo root) — the committed design: 00 overview, 01 storage, 02 workspaces, 03 caches, 04
   sandbox, 05 gateway, 06 CLI, 07 API, 08 testing, 09 substrates (ZFS/Linux), 10 CI runner, 11 shell layer, 12 MCP, 13
   telemetry, 14 nix/declarative host setup + deployment postures. Implement from them and resolve any conflict in favor
   of the specs.
2. `packages/cowshed/docs/` — user-facing UX acceptance documents, not evidence of an implementation.
3. `specs/cowshed/prototypes/apfs-workspace-bench/` — benchmark harness, report, and results backing the design numbers.

## Decisions already made (do not relitigate)

- **Substrate**: clonefile'd sparse images (Option B). Shadows rejected (2.34× worse sync writes, base-file GC
  dependency). **ASIF default on Tahoe — measured** (2.1× create, 2.5× write, 5.6× read, 2.3× metadata vs SPARSE;
  clonefile equal; attach ~75 ms slower), SPARSE/hdiutil fallback pre-Tahoe. Attach **branches on the marker's
  `imageFormat`**: `diskutil image attach` for ASIF (hdiutil refuses it), `hdiutil attach` for SPARSE; ASIF volume roots
  are chowned after create (diskutil leaves them root-owned). 01_storage.md.
- **No templates**: main-as-base. `cowshed adopt` turns the existing checkout into an image mounted at its original
  path; `cowshed new` clones main's live image (sync + clonefile, crash-consistent). Safe because secrets never live in
  worktrees (gateway holds them; no `.env` files). The invariant is enforced, not assumed: a built-in scanner gates
  `cowshed adopt` (blocking, full-tree, `--quarantine` escape), gates `cowshed push`/autosave on the content delta, and
  audits in `cowshed doctor` (filename + known-token-shape rules, no entropy heuristics; explicit reasoned waivers
  outside the image; `--secrets-deep` via gitleaks when installed). Spec: 02_workspaces.md.
- **Stable repository identity and trusted policy**: `repo_id` comes from one explicitly chosen Git remote URL,
  normalized to lowercase `owner/repo` after discarding transport, credentials, host, leading slash, optional `.git`,
  query, and fragment. Controller-owned binding metadata records both the chosen URL and `repo_id`; every open
  re-normalizes the recorded URL and conflicts if it no longer matches. A checkout may have multiple candidate
  identities but exactly one primary binding. Discovery may propose candidates, never silently mint or select one;
  local-only repositories require an explicit `repo_id`. Treat the two components as validated path components,
  rejecting empty, `.`, `..`, separators, NUL, and noncanonical forms. The trusted project policy for `acme/widget` is
  `~/.cowshed/acme/widget/policy.json`, outside every workspace and denied to sandboxes.
- **No SQLite / no state store**: registry = readdir over images/datasets; mount truth = the kernel mount table;
  workspace identity = in-image `.cowshed/workspace.json`; grants and repository binding metadata are controller-owned
  and outside the workspace. No image pooling. Per-workspace supervisors and the optional MCP server hold no
  authoritative lifecycle state.
- **Local-only git and coordinator-owned mirrors**: workspace git touches only local paths: the `host` remote and
  gateway-owned, sandbox-read-only bare mirrors on the cache volume. Mirror creation and refresh are
  coordinator/control- plane operations using host-held credentials; workspaces may read mirrors but can never
  configure, update, or write them. Publishing to a real origin is also coordinator work outside every sandbox. There is
  no in-sandbox Git-over-HTTPS, credential helper, `insteadOf` rewrite, SSH proxy, or data-plane Git protocol.
- **Push/autosave direction is host-side**: `cowshed push` and the autosave net are the _host_ fetching from the
  workspace mount (`git fetch <mount> +cowshed/<ws>:…`), never the sandbox running `git push` against agent-controlled
  `.git` config/hooks. Autosave ref namespace: `refs/cowshed/<ws>/wip`.
- **Platform network isolation**: macOS uses one 16-port block per workspace; Seatbelt emits 16 literal outbound allows,
  leaves bind/inbound permissive, and relies on outbound confinement. The host control plane at 7644 never appears in
  workspace configuration. Linux instead has a per-workspace gateway Unix data socket and no TCP egress. A fresh network
  namespace remains solely for private loopback/dev-server isolation; there is no veth, DNAT, or routing plumbing, and
  macOS port blocks do not apply on Linux.
- **Denial evidence, never string-sniffing**: exit/typed 6 is emitted only on authoritative evidence: pre-spawn
  validation, profile-application failure, gateway policy denial, or a verified kernel signal. Child output and bounded
  summaries never establish a denial or synthesize a grant.
- **Layout — two dedicated volumes, zero `~/Library`** (except the launchd plists, which must live there;
  home-manager-owned on nix hosts): `cowshed.store` mounted AT `~/.cowshed` (the dotdir IS the volume — no `store/`
  level; images, grants, quarantine, gateway state, telemetry at the volume root) and `cowshed.caches` NESTED at
  `~/.cowshed/caches` (mirror, git mirrors, layer-3 caches — fully rebuildable, nukeable). Both lazily created,
  space-sharing the container; store mounts first (the other mountpoints live on it) and the volume-root marker
  `.cowshed-volume.json` distinguishes mounted from bare — absent means unmounted, heal before acting. Workspace mounts
  at `~/.cowshed/mnt/<owner>/<repo>/<ws>` (primary `repo_id`, with each component separately validated and encoded;
  nobrowse, owners on, NOT /Volumes). Data-volume home footprint: one empty mountpoint dir. Rationale: Data's local
  snapshots would pin churned image blocks (path-level tmutil exclusion doesn't stop snapshotting); dedicated volumes
  also collapse backup policy to per-volume decisions and separate fsck/corruption domains by rebuildability class.
  Sandbox consequence: ONE `~/.cowshed` subtree deny + carve-backs replaces the enumerated store/sibling-mount denies
  (04_sandbox.md). Spec: 01_storage.md.
- **Declarative host setup + deployment postures (14_nix.md)**: on nix hosts, `programs.cowshed` (home-manager) owns the
  cache-subtree symlinks, launchd agents, go env defaults, and TM exclusions declaratively — `adopt`/`ensure` VALIDATE
  and never mutate when HM owns the host (detection: HM symlinks resolve into /nix/store); imperative mode stays the
  non-nix fallback. Volume creation and per-project artifacts stay imperative always. Postures: A = single-account
  (Seatbelt-only boundary, main unsandboxed); B2 (recommended) = dedicated `dev` uid used as a remote-backend "localhost
  dev machine" (one personal GUI session; shells via `ssh dev@localhost`/`sudo -u dev -i`; editors via
  Remote-SSH/Gateway/Zed-remote; gateway/autosave as nix-darwin LaunchDaemons with `UserName = dev`). Cross-uid FILE
  access is rejected; same-uid-or-nothing. Spec: 14_nix.md.
- **Cache taxonomy (3 layers, discriminated by USE not concurrency safety)**: (1) gateway mirror artifacts on the cache
  volume — download dedupe; (2) clone-materializing caches — the cache is the reflink source (bun materializes
  node_modules by cloning from it; APFS clonefile is same-volume-only) — kept reflink-reachable INSIDE the workspace
  image, inherited from main via CoW; today that means "bun, on APFS", possibly nothing on ZFS (BRT crosses datasets —
  verify); (3) read-at-build caches (cargo registry, sccache, zig global, gradle) — read at build time, outputs written
  elsewhere, so sharing costs nothing — on the shared cache volume, reached via the tools' DEFAULT paths relocated once
  at first adopt. **Relocate only the cache subdirs** (`~/.cargo/registry`, `~/.cargo/git`, `~/.cache/zig`, gradle's
  cache dirs → symlinks) — NOT `~/.cargo` wholesale, which also holds `config.toml`, `credentials.toml`, and `bin/` (on
  PATH); moving those onto a sandbox-writable volume would be a persistence-escape surface. Workspace-keyed state
  (target/, node_modules, DerivedData, .nx) in-image. Env wiring is 0–2 vars total, each only until its verification
  passes; everything else is in-image config files + host paths. Main uses identical wiring. Spec: 03_caches.md.
- **Go: `~/go` is never created.** `GOMODCACHE`/`GOCACHE` on the caches volume (`~/.cowshed/caches/go/{mod,build}` —
  read-at-build, layer 3; go's sum/ziphash verification + 0444 entries make it the strongest-postured shared cache);
  `GOPATH`/`GOBIN` in-image (`go install` binaries are the `~/.cargo/bin` hazard); `GOTOOLCHAIN=local` (toolchain is
  nix/devenv-pinned; `auto` contradicts the declarative env — deliberate opt-in lands downloads in GOMODCACHE, on the
  caches volume). Wired by an in-image `GOENV` file at `.cowshed/cache/go/env` carrying the per-workspace
  `GOPROXY=http://127.0.0.1:<base>/go` (no `,direct`), reached via the `GOENV` export riding `.envrc`/direnv — Go has no
  directory-scoped config, so this is a second load-bearing export beside the token candidate. New gateway endpoint:
  `/go` GOPROXY mirror with sumdb passthrough and private-module credential injection (workspace git is local-only, so
  the proxy IS the private-module path). `~/go` is deny-listed as a misconfiguration tripwire (04_sandbox.md). Specs:
  03_caches.md, 04_sandbox.md, 05_gateway.md.
- **iOS/Xcode topology (posture B)**: Xcode has no remote mode and Simulator.app cannot attach cross-uid, so the
  **personal-session simulator is an artifact host** (human inspection, fed by the one-way drop dir
  `<shared-drop-root>/<owner>/<repo>/`, using the separately validated components of the primary `repo_id`, via
  `cowshed sim export`) and **dev-side headless simulators are the agent/CI runtime** (simctl/XCUITest/idb; the
  "simulator" Seatbelt preset opens CoreSimulator IPC only). The in-image `.cowshed/bin/xcrun` wrapper (passthrough
  except simctl/devicectl; dev-local default) routes personal-session verbs through the gateway's `/sim/` endpoint to a
  session broker — the one cowshed component running as the personal user — under the `sim` grant axis (`openurl` freely
  grantable; `install` drop-dir-bound and human-gated: simulator apps execute AS the invoking user). Expo/RN daily loops
  ride shared loopback unchanged (docs/ios.md). Specs: 14_nix.md, 02/03/04/05/06/07/12/13.
- **macOS desktop apps (posture B) — three lanes**: (1) agent E2E testing and (2) interactive debugging both run the app
  **as dev in dev's own session** (accessibility APIs/AppleScript; same uid → **no broker, no grant** — simpler than
  simulators); (3) daily use runs it **as the personal user** via `cowshed app export` (drop dir) →
  `cowshed app promote` — a **human-run personal-session verb** (writes `~/Applications`, so unreachable from any
  sandbox/agent; Developer-ID signature required by default, clears quarantine). The consent asymmetry: lanes 1–2 need
  none (confined as dev), lane 3's `promote` IS the consent (the app runs with the human's full authority — a deliberate
  boundary exit). The contract defines no `--app open` agent grant: promotion is the only personal-session path and is
  human by construction. `cp` applies no quarantine xattr (verified), so a Developer-ID drop opens cleanly; ad-hoc trips
  Gatekeeper. Electron/RN-desktop promoted apps point at dev's dev-server over loopback for live iteration. Specs:
  14_nix.md, 02/04/05/06; docs/desktop.md.
- **Layered sandbox and grant propagation**: start closed. Egress and simulator policy update immediately at the
  gateway. Filesystem authority belongs to an immutable supervisor launch revision: after an effective filesystem grant
  or revoke, the old supervisor stops admitting work and drains; existing jobs retain the old revision, then the
  controller relaunches under the new revision. Inner per-command profiles may narrow but never widen. Named sessions
  pinned to a stale revision conflict rather than migrate. No-op, egress-only, and simulator-only changes do not
  relaunch.
- **Supervisor, clients, and jobs**: one persistent Unix socket per workspace supports concurrent clients. Its runtime
  directory is mode 0700, socket mode 0600, and connections require peer-credential/uid validation. A disconnect
  detaches only that client's view; jobs continue and clients resume by durable numeric job ID and backing-file offsets.
  Every accepted exec gets a positive workspace-local monotonic `u64` job ID with no reuse. Complete separate streams
  live at `.cowshed/job/<id>/out` and `err`; metadata and deterministic bounded redacted summaries live in control
  messages and Arrow records. A configurable combined output quota defaults to 1 GiB. At the first crossing, stop
  accepting bytes past the boundary, terminate the whole process group, drain both pipes, fsync, and record an
  authoritative `output-limit` terminal state; never silently truncate while a job continues.
- **MCP authority delivery and consent**: coordinator authority is delivered only over an inherited dedicated file
  descriptor or socketpair from the spawner, then validated, closed, and made non-inheritable before any workspace
  process is spawned. It never appears in environment, argv, stderr, workspace files, or ordinary token text. Worker
  connection descriptors are separate 256-bit random, one-use, 30-second-TTL, memory-only capabilities, atomically
  consumed and bound to the intended workspace plus peer/socket identity; restart invalidates them. MCP has no
  interactive consent or elicitation. The coordinator applies policy agent-to-agent. A worker calling a coordinator-only
  tool fails authorization before execution with an error distinct from sandbox denial and domain errors. Simulator
  install remains per-artifact human-gated, and desktop promotion remains a human-run personal-session action; no grant
  bypasses either boundary.
- **Sandbox**: layered, start closed (write: own mount + designated cache subtrees + temp; egress: localhost only),
  widened at runtime through coordinator-owned grants. No VMs/containers; Seatbelt on host is the documented confinement
  ceiling.
- **devenv/Nix in sandboxes**: multi-user daemon Nix is required; store reads use the broad read allow and
  builds/substitution use the daemon Unix socket as an accepted trusted channel. Writable Nix cache/state roots are
  explicit, clone `.envrc` trust is healed at lifecycle boundaries, and exec loads direnv fail-closed without exporting
  secrets.
- **Gateway**: localhost daemon. npm + cargo mirrors (caching/dedupe, Keychain upstream creds) + the `repo mirror`
  control-plane verb (bare read-only git mirrors; no in-sandbox git broker). Granted egress is **intercepted by
  default** — per-workspace CA (private key gateway-side next to the grant file, public cert an in-image trust anchor;
  minted at new/fork, preserved on restore, destroyed with the workspace), TLS terminated, Keychain credential +
  traceparent injected, audited per-request; `--opaque` reverts a host to a pass-through CONNECT tunnel (pinned
  clients), `--impersonate <profile>` gives a browser TLS fingerprint (and suppresses header injection). This
  **supersedes the old "no TLS MITM" line** and the per-authenticated-service endpoint roadmap (/github, /anthropic): a
  granted host needs no bespoke endpoint. Engine = `hyper` + `rustls` + `rcgen` + `tokio`, single-listener dynamic SNI
  (`ResolvesServerCert`), h1+h2 on both legs (SSE/streaming), LRU **leaf** cache, upstream-health gate at the one
  outbound choke point — the Bun/Hero MITM production lessons ported as **normative tests, not code** (05_gateway.md).
  Outbound is an `OutboundConnector` trait: default hyper/rustls, optional `impersonate` cargo feature linking
  libcurl-impersonate (runtime-detected, truly optional; wreq/rquest possible behind the same trait). Per-workspace port
  block + token → egress/repo policy; 7644 = control plane only.
- **Telemetry = lmao Arrow, one substrate**: observability is distributed tracing into Arrow columns via lmao
  (`packages/lmao-rs`), not text logs. W3C trace context minted-or-adopted at every entry (CLI env, Rust `TraceContext`
  on request structs, MCP `_meta`, CI deterministic from `(run_id, attempt)`); the supervisor injects `TRACEPARENT` into
  each job's env; exec records and gateway audit carry trace/span ids. Store telemetry + gateway audit flush as
  day-partitioned Arrow IPC segments under `~/.cowshed/telemetry/` (retention = gc drops segments); exec records are
  in-volume Arrow (travel with checkpoints, not audit-grade). **No on-disk NDJSON** (wire/export encoding only) and no
  telemetry daemon; the one text file is `telemetry/daemon-stderr.log` for pre-tracer-init crashes. lmao-query selectors
  are the 08 trace-assertion surface (golden trace fixtures via injected Clock); `cowshed logs`/`audit`/`trace` read
  them; OTLP is a projection if ever needed. Spec: 13_telemetry.md.
- **Substrates**: `cowshed_core::Substrate` has APFS image and ZFS dataset contracts. Linux enforcement combines
  Landlock, a private-loopback network namespace, and a per-workspace gateway Unix socket, with the same grants,
  revision semantics, and exit taxonomy as macOS.
- **CI runner**: cowshed is a GitHub Actions runner substrate — NixOS module `services.cowshed-runner`, ephemeral
  runners labeled `[self-hosted, cowshed, zfs]`, one workspace per job (`ci-<run_id>`), headless main refreshed by the
  green main-branch job; composite action at `.github/actions/smoothbricks-ci/action.yml` (mode auto|github|cowshed).
  Deletes Nix-restore / bun-install / Nx-cache steps and moves registry/git secrets off GitHub into the host gateway.
  Spec: 10_ci.md.
- **CLI contract ("self-driving")**: stdout = machine-readable only (bare value or `--json` envelope
  `{"ok":…,"result"|"error":…}` — the frozen shape; the docs' `cmd`/`detail` variant is stale); ALL guidance on stderr
  ending with `next:` hints (so `| grep` on stdout never eats agent guidance); cowshed's own exit codes 0/2/3/4/5/6 =
  ok/usage/not-found/conflict/env-missing/ sandbox-denied. `cowshed exec` passes the **child's** code through unchanged;
  cowshed-wrapper failures during exec use **100–105** so they never collide with a child that exits 1–6. `-q`/
  `--quiet` are aliases. Convention over configuration: zero required config, `.cowshed.toml` overrides only.
- **Structured stdin and safe shell optimization**: `ExecRequest` supports no stdin, inline binary/streamed stdin, or a
  workspace-relative file source. File input is opened no-follow beneath the workspace and streamed with backpressure;
  EOF and cancellation are explicit, and job metadata records only source kind and byte count, never content. No shell
  interpolation is involved. Ordinary shell execution remains the correctness path. An optional fast path may use a real
  shell AST parser—never regex or output sniffing—for one top-level simple command with literal, in-workspace,
  same-filesystem, nonexistent clobber targets for `>` and/or `2>`. Append, descriptor duplication, pipelines,
  subshells, expansion, symlinks, noclobber, ambiguity, and every ineligible form fall back unchanged. The supervisor
  may create the target inode and hardlink the matching job stream to it, with inode-aware tailing and quota accounting.
  This is never a security, denial-evidence, authority, quota-correctness, or general correctness dependency.

## State of the tree

- `packages/cowshed/` contains Cargo workspace scaffolding and stub crates only. Treat every behavior in the specs and
  docs as an implementation obligation, not as shipped functionality.
- Toolchain configuration lives in `tooling/direnv/devenv.{yaml,nix}`. Invoke repository commands from `<repo-root>`;
  never embed a workstation-specific absolute path.
- Keep the implementation greenfield: migrate all callers cleanly and leave no compatibility paths.

## Suggested workflow shape

Phase 1 — `cowshed-core` and `cowshed-shell`: substrate lifecycle, repository binding and policy lookup, markers/grants,
env wiring, sandbox profile generation, multi-client supervision, jobs, output quotas, and revision-bound relaunch. Unit
tests cover pure contracts; integration tests use isolated temporary paths. Phase 2 — parallel work on the CLI, gateway,
N-API bindings, MCP server, and escape suite. Phase 3 — adversarial conformance and sandbox review, confirmed fixes,
then targeted verification plus the repository's full Rust checks and a CLI transcript against the docs.

**Resolved experiments** (2026-07-11, harnesses + results in `specs/cowshed/prototypes/apfs-workspace-bench/`; verdicts
folded into the specs):

- ASIF vs SPARSE → **ASIF default on Tahoe** (2.1× create, 2.5× write, 5.6× read, 2.3× metadata; attach ~75 ms slower;
  hdiutil cannot attach ASIF — `diskutil image attach` branch required).
- Attach latency floor → **not flag-reducible** (`-noverify` ~15 ms noise); only a DiskImages2/diskarbitrationd-level
  path could improve it — research item, budgets don't assume it.
- Fork-mid-write clone validity → **10/10 clean** under continuous writer + streaming dd, both formats, `fsck_apfs -q`
  and full `-n`; sync-before-clone is freshness only. Gate removed from `new`/`fork`/`checkpoint`; regression-pinned in
  08_testing.md.
- (a) bunfig relative `install.cache.dir` → **works, resolves to project root** (bun 1.3.14); `BUN_INSTALL_CACHE_DIR`
  retired. `[install] cacheDir` is silently ignored — doctor checks for it.
- Cross-volume bun install cost → **measured 6× slower + full-copy** (0.03 s/480 KiB in-volume vs 0.18 s/59 MB
  cross-volume, 1,579-file tree) — in-image placement confirmed (03_caches.md).
- (d) cargo `[env]` → **reaches all rustc-wrapper invocations** (cargo 1.97); `SCCACHE_NO_DAEMON` rides there, no env
  fallback.
- (e) SBPL semantics → **ranges don't parse; last-match-wins (denies must be emitted last); implicit bind-on-connect
  exempt; `bind(0)` breaks under bind restriction** → the outbound-only 16-single-port-rule model in 04_sandbox.md,
  validated end-to-end.

Open experiments (fold into Phase 1 as tests, don't block on them):

- **Unified-log denial visibility** — confirm Seatbelt violations reach the unified log with pid+operation _from an
  unsandboxed controller context_ (the log store is permission-gated from sandboxed sessions, so this could not be
  tested). Gates the log-correlation enhancement only; the errno signal (EPERM vs ECONNREFUSED, measured) stands alone.
- **Time Machine default-inclusion of additional internal volumes** — decides whether adopt must volume-exclude
  `cowshed.store`/`cowshed.caches` (01_storage.md). The `/private/tmp` probe was inconclusive (that tree is TM-excluded
  by default); test against the real volumes.
- **In-block-only self-connects in practice** — under the outbound-only model an in-sandbox client can reach only its
  own block ports; survey which real tools break by spawning a helper server on a random port and connecting back (test
  runners, HMR side-channels), and whether the PORT convention covers them (04_sandbox.md).
- **devenv eval cost across clone paths** — needs the first real `cowshed new`: does a fresh clone's `.envrc` evaluation
  reuse the in-image `.devenv`/Nix state (cheap) or re-evaluate? Related measurement: warm `direnv exec` on this repo
  costs 1.3–2.9 s (11_shell.md's pool rationale).
- (b) bun registry token via the per-workspace port URL / registry auth (the workspace's identity is primarily its
  data-plane _port_ now, so this decides only whether `COWSHED_GATEWAY_TOKEN` survives as a load-bearing export); (c)
  bun's Linux copyfile backend × OpenZFS BRT (`copy_file_range` — empties layer 2 on ZFS); cargo `trim-paths`
  warm-target survival across mount paths.
- **Declarative host setup (14_nix.md)** — (i) home-manager launchd coverage on darwin: confirm HM-managed LaunchAgents
  (gateway, login-attach/autosave) load and KeepAlive correctly across HM generation switches; (ii) ownership detection:
  HM-created symlinks resolve into `/nix/store` — verify this holds across HM's symlink strategies (home.file vs
  mkOutOfStoreSymlink) as the validate-vs-mutate signal; (iii) posture B2: nix-darwin LaunchDaemons with
  `UserName = dev` for gateway/autosave (a uid that never logs in graphically gets no user LaunchAgents), and the
  Keychain wrinkle — dev's login keychain is not auto-unlocked without a login session; first dev shell (or
  `cowshed unlock`) unlocks it and the gateway defers Keychain reads until then.
- **Go wiring verification** (03/04/05, empirical baseline: devenv-provided go 1.26.3, defaults `GOPATH=~/go` with a 1.1
  GB modcache already grown, `GOENV=~/Library/Application Support/go/env`): (i) `GOENV` coverage across all go
  invocations — including editor-spawned gopls (direnv integration) and unwrapped spawns — and whether any file-based
  mechanism could kill the export (none known: go has no directory-scoped config); (ii) per-exec `GOPROXY` env override
  taking precedence over the `GOENV` file (decides tier-2 go attribution, 13_telemetry.md); (iii) Go-on-macOS platform
  verifier: confirm `crypto/x509` ignores `SSL_CERT_FILE` on darwin so Go-built clients of intercepted hosts need
  `--opaque` (04_sandbox.md gap table; moot for module traffic on the plain-HTTP `/go` mirror); (iv) cite/verify the
  GOMODCACHE concurrent-sharing guarantee across many workspaces + main (0444 entries, internal locking) and confirm
  GOFLAGS `-modcacherw` projects don't break the model.
- **iOS/simulator verification** (14_nix.md, 05_gateway.md `/sim/`): (i) simulator reliability without an Aqua session —
  CI folklore says XCUITest/`simctl` workloads want a live GUI session for the executing uid; confirm what actually
  degrades over pure SSH and whether a persistent background dev session fixes it; (ii) Expo CLI resolves `xcrun` via
  PATH (not an absolute `/usr/bin/xcrun` spawn), so the in-image wrapper intercepts `i`/`shift+i`; (iii) cross-uid
  `simctl install` end-to-end: a dev-built, ad-hoc-signed `.app` from the drop dir installs and launches in the
  personal-session simulator; (iv) the gateway↔session-broker auth mechanism (mutual secret provisioning at posture-B
  setup); (v) the headless-signing recipe (`security set-key-partition-list`, once-per-boot unlock) under the dev uid;
  (vi) idb as the dev-side streaming/HID path for agents observing headless simulators; (vii) the "simulator" Seatbelt
  preset's exact mach-lookup/XPC-socket inventory for CoreSimulator (04_sandbox.md).
- **macOS desktop verification** (14_nix.md): (i) Developer-ID vs ad-hoc Gatekeeper behavior when `cowshed app promote`
  installs into `~/Applications` (confirm ad-hoc needs the override, Developer-ID does not); (ii) quarantine-xattr
  absence on `cp`/`cp -R` into the drop dir — **verified this session** (`cp` adds no `com.apple.quarantine`),
  re-confirm on a real signed `.app` bundle; (iii) an Electron/RN-desktop promoted app pointing at dev's dev-server over
  loopback across uids (live iteration); (iv) driving a dev-session desktop app from a dev-side agent via accessibility
  APIs/AppleScript (the lane-1 mechanism; reuse cowshed's agent-browser Electron path); (v) `app promote` refuses to run
  as anything but the personal user and is unreachable from the sandbox/gateway (structural, but assert it).

Interception + telemetry verification (05_gateway.md, 13_telemetry.md — fold into Phase 2 gateway work):

- **Bun `NODE_EXTRA_CA_CERTS`** — does Bun honor it so the workspace CA trust anchor works for `fetch`/`bun install`
  over an intercepted host? If not, Bun is a documented interception gap (04_sandbox.md trust-anchor table).
- **macOS-native-TLS tool inventory** — enumerate the Security.framework-backed tools (Swift/Xcode, some system
  utilities) that ignore env anchors and therefore need `--opaque` for an intercepted host (04_sandbox.md gaps).
- **curl-impersonate linking** — feasibility of the optional `impersonate` cargo feature: runtime detection of
  `libcurl-impersonate`, build/link on macOS + Linux, profile coverage (05_gateway.md OutboundConnector).
- **Fingerprint vs traceparent injection** — confirm that injecting a header defeats a JA4-family fingerprint, i.e. the
  per-connector suppression on `--impersonate` connections is actually necessary (05_gateway.md trace-context
  carve-out).
- **`BUN_CONFIG_REGISTRY` per-job trace URL** — env-vs-bunfig precedence, subcommand coverage (bun#617), and the
  npmrc-overrides-bunfig bug (bun#20593): decides whether tier-2 exact `bun install` attribution is reliable
  (13_telemetry.md).
- **Audit flush-durability window** — measure the one-batch crash window under the decision-boundary/short-timer flush
  policy; confirm no audit-relevant event class needs a per-event flush (13_telemetry.md).
- **lmao TRACEPARENT adoption** — verify that the TS/Rust tracer adopts `TRACEPARENT` at init and stamps outbound
  `fetch`/HTTP, which promotes first-party traffic to tier-1 attribution (13_telemetry.md).

## Acceptance

- `cowshed adopt && cowshed new x && cowshed exec x -- <build> && cowshed push x && cowshed rm x` works end-to-end on
  each supported host, sandboxed, with caches warm from main.
- Exit code 6 and a grant hint appear only on authoritative sandbox-denial evidence. After a filesystem grant, the prior
  supervisor drains and relaunches at the new revision before retry; egress and simulator grants apply immediately.
- `cowshed ensure` fast path ≤ 25 ms; `cowshed new` ≤ 1 s cold; `cowshed rm` returns instantly.
- Zero host-visible inode growth per workspace beyond the image file + grants file.
- Substrate parity: the acceptance flow above passes on Linux/ZFS with Landlock enforcement (integration leg; can run in
  CI on a ZFS-capable Linux host once the runner exists — bootstrap order is macOS first, then Linux substrate, then the
  runner that uses it.)
