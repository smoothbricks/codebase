# cowshed — implementation kickoff

Prompt for a fresh session: launch a multi-agent implementation workflow for `packages/cowshed` ("ultracode"-style:
Sonnet 5 implementors, fork reviewers, fixers). Everything below is the context that session needs; the authoritative
design is in the specs, not this file.

## What cowshed is

Warm git workspaces for macOS/APFS: every workspace is one copy-on-write cloned disk image (clonefile ≈ 2 ms, attach ≈
235–400 ms), mounted nobrowse, sandboxed by default, with shared toolchain/package caches and an egress gateway holding
all secrets. Consumers: jcode (Rust API, replacing its `.jcode-worktrees` linked worktrees and per-command sandbox
plumbing) and Claude Code / other agents (CLI "warm worktrees").

## Read first (in order)

1. `specs/cowshed/*.md` (repo root) — the committed design: 00 overview, 01 storage, 02 workspaces, 03 caches, 04
   sandbox, 05 gateway, 06 CLI, 07 API, 08 testing, 09 substrates (ZFS/Linux), 10 CI runner, 11 shell layer, 12 MCP.
   Specs are commitments (see AGENTS.md); implement from them, critique them if they conflict with reality.
2. `packages/cowshed/docs/` — user-facing docs written as if cowshed exists; treat as UX acceptance.
3. `/Users/danny/Dev/_fork/jcode/crates/jcode-base/src/sandbox.rs` (+ `shell_supervisor.rs`,
   `jcode-app-core/src/tool/bash.rs`) — the proven Seatbelt generator cowshed-core generalizes: launch-time
   `sandbox-exec` wrapping, deny-default profiles, secret deny rules, dynamic writable-root grants
   (`grant_write_paths`), `SCCACHE_NO_DAEMON=1`, direnv env filtering.
4. `specs/cowshed/prototypes/apfs-workspace-bench/` — benchmark harness + REPORT.md + results backing the design numbers
   (folded into the repo; formerly `~/Dev/apfs-workspace-bench`).

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
- **No SQLite / no state store**: registry = readdir over images; mount truth = kernel mount table; identity = in-image
  `.cowshed/workspace.json`; grants = `<image>.grants.json` OUTSIDE the volume (never writable from inside the sandbox).
  No _image_ pooling — cold ~270 ms is fine (budget: ≤10 s human, minutes for agents). (The per-workspace warm _shell_
  pool in 11_shell.md is a different mechanism and stays.) The only daemons are the host gateway and per-workspace shell
  supervisors (+ optional MCP socket server); none store authoritative state.
- **Local-only git**: a workspace's git touches only local paths — the `host` remote (main's mount, fetch-only for
  rebase) and read-only bare mirrors on the cache volume. Pulling a new upstream repo is a gateway control-plane verb
  `cowshed repo mirror <url>` / `cowshed repo clone` (gateway fetches with Keychain creds into a bare mirror it owns;
  repo-scoped egress grant). The git credential broker is **deleted** — no in-sandbox git-over-HTTPS, no `insteadOf`
  rewrite, no `ssh_config ProxyCommand`. Publishing to a real origin is coordinator work, host-side, outside any
  sandbox.
- **Push/autosave direction is host-side**: `cowshed push` and the autosave net are the _host_ fetching from the
  workspace mount (`git fetch <mount> +cowshed/<ws>:…`), never the sandbox running `git push` against agent-controlled
  `.git` config/hooks. Autosave ref namespace: `refs/cowshed/<ws>/wip`.
- **Per-workspace gateway port BLOCK**: each workspace gets a contiguous 16-port block from a reserved range (default
  40960–49151), recorded in the grant file as `portBlock: {base, size}`. `base` = the gateway's per-workspace data-plane
  listener (host-bound; workspace identity is transport-enforced by which port it reaches, token is the second factor).
  `base+1..size-1` = the workspace's own bindable service ports, so dev servers (vite/astro/metro/`devenv up`) run
  container-style without sibling collisions. **Enforcement shape is measured, not the naive one**: SBPL rejects port
  ranges and can't enumerate the ephemeral range, and restricting `network-bind` breaks `bind(0)` — so isolation rides
  **entirely on outbound** (16 literal single-port allow rules per workspace) with bind/inbound left permissive;
  siblings may bind freely but cannot be _reached_ (EPERM). A second measured invariant: SBPL is **last-match-wins** —
  the secret deny list protects anything only because generation emits denies last (unit-tested ordering, 04/08). macOS
  cooperative mechanism; Linux netns gives private loopback, so blocks are macOS-only. **7644 is the host/control plane
  only and never appears in workspace config.**
- **Denial evidence, never string-sniffing**: exit/typed 6 (sandbox-denied) is emitted only on authoritative evidence —
  pre-spawn validation, profile-application failure, gateway policy denial, or the measured in-band errno signal (denial
  = EPERM(1) vs allowed-but-unserved = ECONNREFUSED(61)). Unified-log correlation by pid is an _enhancement pending
  verification_ from an unsandboxed context (the log store is permission-gated from sandboxed sessions). cowshed never
  parses child stdout/stderr to infer a denial or synthesize a suggested grant (jcode's output-string heuristic is
  explicitly not carried over).
- **Layout — two dedicated volumes, zero `~/Library`** (except the launchd plists, which must live there):
  `cowshed.store` at `~/.cowshed/store` (images, grants, quarantine, gateway state, logs) and `cowshed.caches` at
  `~/.cowshed/caches` (mirror, git mirrors, layer-3 caches — fully rebuildable, nukeable). Both lazily created,
  space-sharing the container. Mounts at `~/.cowshed/mnt/<project>/<ws>` (nobrowse, owners on, NOT /Volumes, nothing
  visible in $HOME). Rationale: Data's local snapshots would pin churned image blocks (path-level tmutil exclusion
  doesn't stop snapshotting); dedicated volumes also collapse backup policy to per-volume decisions and separate
  fsck/corruption domains by rebuildability class. Spec: 01_storage.md.
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
- **Sandbox**: layered, start closed (write: own mount + designated cache subtrees + temp; egress: localhost only),
  widened at runtime via grants (fs read/write paths + egress domains) applied at next exec (Seatbelt regen) and
  immediately at the gateway. Parity with jcode swarm coordinator grants is a hard requirement. No VMs/containers (user
  rejected; VZ caps macOS guests at 2 anyway); Seatbelt on host is the isolation ceiling, documented as confinement.
- **devenv/Nix in sandboxes** (jcode requirement): multi-user (daemon) Nix required; store reads via the broad read
  allow; builds/substitution via the daemon unix socket — an accepted off-gateway trusted channel; `~/.cache/nix` +
  `~/.local/state/nix` in the writable baseline; cowshed auto-trusts each clone's `.envrc` (direnv trust is path-keyed)
  at new/fork/restore and in `ensure` healing; `cowshed exec` wraps commands fail-closed in `direnv export` (jcode's
  `command_with_nearest_envrc`, without secret filtering — no-secrets invariant). See 04_sandbox.md.
- **Gateway**: protocol-aware localhost daemon — npm + cargo mirror with Keychain-held upstream creds, the `repo mirror`
  control-plane verb (bare read-only git mirrors; no in-sandbox git broker), allowlisted CONNECT; per-workspace port
  block + token → per-workspace egress/repo policy; NDJSON audit. No TLS MITM. 7644 = control plane only.
- **Substrates**: `cowshed_core::Substrate` trait with two v1 implementations — APFS images (macOS) and ZFS datasets
  (Linux; snapshot `main@cowshed:<name>` + clone, destroy clone+origin together, minimal root helper for mount ops since
  Linux ignores zfs mount delegation). Auto-detected from the filesystem the project root sits on. Linux sandbox
  enforcement = Landlock + loopback-only netns, same grant files, same exit codes. Spec: 09_substrates.md +
  04_sandbox.md.
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

## State of the tree

- `packages/cowshed/`: Cargo workspace scaffolded (resolver 3, edition 2024), stub crates compile: `cowshed-core`,
  `cowshed-cli` (bin `cowshed`), `cowshed-gateway`, `cowshed-napi` (cdylib), `cowshed-escape-tests`.
- Toolchain: latest stable Rust via rust-overlay in `tooling/direnv/devenv.{yaml,nix}` — verified 1.97.0. **Caution
  (measured): `cargo` is currently NOT on the devenv profile PATH** — the profile ships
  rust-analyzer/cargo-nextest/sccache but not cargo itself; builds today reach the toolchain via the rust-overlay store
  path directly. Fix the devenv profile (or use the store path) before assuming
  `direnv exec /Users/danny/Dev/smoothbricks cargo …` works.
- Repo rules (AGENTS.md): bun only (no npm/npx), fix everything you see, greenfield — no compat layers, search before
  implementing.

## Suggested workflow shape

Phase 1 — `cowshed-core` (one strong implementor; it's the foundation): image/volume lifecycle (hdiutil/diskutil
wrappers, clonefile via libc), marker/grants files, env wiring, Seatbelt profile generation ported+generalized from
jcode, exec supervision. Unit tests for pure parts (profile text, path policy, grant merge); integration tests behind a
flag on real APFS under `/private/tmp` (hdiutil works unsandboxed; CI-skip otherwise). Phase 2 — parallel implementors:
`cowshed-cli` (self-driving contract), `cowshed-gateway` (mirror + broker + CONNECT), `cowshed-napi` (napi-rs async
bindings), `cowshed-escape-tests` (port jcode's escape-test pattern). Phase 3 — fork reviewers per crate (adversarial:
spec conformance, sandbox soundness, exit-code contract), then fixers apply confirmed findings, then a final verify
agent: `cargo check && cargo clippy -- -D warnings && cargo test` (via direnv) + a CLI transcript smoke-run against the
docs' examples.

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
  data-plane _port_ now, so this decides only whether `COWSHED_GATEWAY_TOKEN` survives as the last export); (c) bun's
  Linux copyfile backend × OpenZFS BRT (`copy_file_range` — empties layer 2 on ZFS); cargo `trim-paths` warm-target
  survival across mount paths.

## Acceptance

- `cowshed adopt && cowshed new x && cowshed exec x -- <build> && cowshed push x && cowshed rm x` works end-to-end on
  this machine, sandboxed, with caches warm from main.
- Exit code 6 + stderr grant hint on sandbox denial; `cowshed grant` unblocks without restart.
- `cowshed ensure` fast path ≤ 25 ms; `cowshed new` ≤ 1 s cold; `cowshed rm` returns instantly.
- Zero host-visible inode growth per workspace beyond the image file + grants file.
- Substrate parity: the acceptance flow above passes on Linux/ZFS with Landlock enforcement (integration leg; can run in
  CI on a ZFS-capable Linux host once the runner exists — bootstrap order is macOS first, then Linux substrate, then the
  runner that uses it.)
