# cowshed — implementation kickoff

Prompt for a fresh session: launch a multi-agent implementation workflow for `packages/cowshed`. Everything below is the
context that session needs; the authoritative design is in the specs, not this file. Nothing in this summary implies
that the specified behavior is already implemented.

## What cowshed is

Warm git workspaces for macOS/APFS and Linux/ZFS: every workspace is a copy-on-write clone, sandboxed by default, with
shared toolchain/package caches and an egress gateway holding all secrets. Rust applications consume `cowshed-core`
directly, Bun/Node applications use `cowshed-napi`, and shell-based agents use the CLI.

## Read first (in order)

1. `specs/cowshed/*.md` (repo root) — the committed design: 00 overview, 01 storage, 01a APFS, 02 workspaces, 03 caches,
   04 sandbox, 05 gateway, 06 CLI, 07 API, 08 testing, 09 substrates (ZFS/Linux), 10 CI runner, 11 shell layer, 12 MCP,
   13 telemetry, 14 nix/declarative host setup + deployment postures. Implement from them and resolve any conflict in
   favor of the specs.
2. `packages/cowshed/docs/` — user-facing UX acceptance documents, not evidence of an implementation.
3. `specs/cowshed/prototypes/apfs-workspace-bench/` — benchmark harness, report, and results backing the design numbers.

## Decisions already made (do not relitigate)

- **Substrate**: clonefile'd disk images (Option B). Shadows rejected (2.34× worse sync writes, base-file GC
  dependency). **One format: RAW (UDRW) `.dmg` holding one case-sensitive APFS volume, stored as a sparse file**
  (01a_apfs.md §Images). No fallback, no format field anywhere, macOS 26 is the floor. Historical, pre-cutover ASIF
  measurements: ASIF was measured faster than SPARSE on every row (2.1× create, 2.5× write, 5.6× read, 2.3× metadata;
  clonefile equal) and equal to case-insensitive ASIF within noise; they do not describe RAW. Blank templates are
  created unprivileged: `diskutil image create blank --format RAW --fs None`, attach `--noMount`,
  `newfs_apfs -U/-G … -e`, `fsck_apfs -q`, `hdiutil detach`; attach is `diskutil image attach` (01a_apfs.md §Images,
  §Mounts).
- **No workspace pools**: main-as-base; formatted blank templates serve image minting only. `cowshed adopt` turns the
  existing checkout into an image mounted at its original path; `cowshed fork` descriptor-clones main's live image after
  a flush, with main still mounted (filesystem crash-consistent). Safe because secrets never live in worktrees (gateway
  holds them; no `.env` files). The invariant is enforced, not assumed: a built-in scanner gates `cowshed adopt`
  (blocking, full-tree, `--quarantine` escape), gates `cowshed push`/autosave on the content delta, and audits in
  `cowshed doctor` (filename + known-token-shape rules, no entropy heuristics; explicit reasoned waivers outside the
  image; `--secrets-deep` via gitleaks when installed). Spec: 02_workspaces.md.
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
- **Fetch-only git**: workspace git fetches from the `main` remote, from the local clones the controller-generated
  `.cowshed/git-fetch.inc` rewrites bound repository URLs to, and over HTTPS only through an intercepted egress grant,
  which admits `git-upload-pack` and refuses `git-receive-pack`. Publishing to a real origin is coordinator work outside
  every sandbox. There is no in-sandbox credential helper, SSH proxy, or data-plane git protocol.
- **Push/autosave direction is host-side**: `cowshed push` and the autosave net are the _host_ fetching from the
  workspace mount (`git fetch <mount> +cowshed/<ws>:…`), never the sandbox running `git push` against agent-controlled
  `.git` config/hooks. Autosave ref namespace: `refs/cowshed/<ws>/wip`.
- **Platform network isolation**: macOS uses one port block per workspace (its size recorded with it, 64 for new ones);
  Seatbelt emits one literal outbound allow per block port, leaves bind/inbound permissive, and relies on outbound
  confinement. The host control plane at 7644 never appears in workspace configuration. Linux instead has a
  per-workspace gateway Unix data socket and no TCP egress. A fresh network namespace remains solely for private
  loopback/dev-server isolation; there is no veth, DNAT, or routing plumbing, and macOS port blocks do not apply on
  Linux.
- **Denial evidence, never string-sniffing**: exit/typed 6 is emitted only on authoritative evidence: pre-spawn
  validation, profile-application failure, gateway policy denial, or a verified kernel signal. Child output and bounded
  summaries never establish a denial or synthesize a grant.
- **Layout — one dedicated store volume, separate empty mountpoints.** `cowshed.store` mounts at
  `/private/cowshed/store`, with images, grants, quarantine and telemetry at its root; the volume marker distinguishes
  the mounted store from its bare mountpoint. Layer-3 caches stay at their tools' own HOME defaults (03_caches.md), and
  the gateway's repository mirrors live in `~/Library/Caches/dev.cowshed`. Session mounts use
  `<mount-root>/<owner>/<repo>/<ws>`, with independently validated and encoded primary repository components. Fresh
  macOS setup provisions `/private/var/folders/cowshed/<uid>/mnt` as the default root, outside Spotlight's
  media-reference admission path; existing explicit/legacy roots remain unchanged. The mount root is a plain directory
  on Data, not the store volume. An existing host changes it only at the detached-session boundary. Durable image churn
  stays outside Data snapshots and its fsck domain. Spec: 01_storage.md §Directory layout; 01a_apfs.md §Stable workspace
  mount root and §Dedicated store volume.
- **Declarative host setup + deployment postures (14_nix.md)**: on nix hosts, `programs.cowshed` (home-manager) owns the
  launchd agents and TM exclusions declaratively — `adopt`/`doctor` VALIDATE and never mutate when HM owns the host
  (detection: HM symlinks resolve into /nix/store); imperative mode stays the non-nix fallback. Volume creation and
  per-project artifacts stay imperative always. Postures: A = single-account (Seatbelt-only boundary, main unsandboxed);
  B2 (recommended) = dedicated `dev` uid used as a remote-backend "localhost dev machine" (one personal GUI session;
  shells via `ssh dev@localhost`/`sudo -u dev -i`; editors via Remote-SSH/Gateway/Zed-remote; gateway/autosave as
  nix-darwin LaunchDaemons with `UserName = dev`). Cross-uid FILE access is rejected; same-uid-or-nothing. Spec:
  14_nix.md.
- **Cache taxonomy (3 layers, discriminated by USE not concurrency safety)**: (1) gateway mirror artifacts in cowshed's
  own cache directory (`~/Library/Caches/dev.cowshed/mirror`) — download dedupe; (2) clone-materializing caches — the
  cache is the reflink source (bun materializes node_modules by cloning from it; APFS clonefile is same-volume-only) —
  kept reflink-reachable INSIDE the workspace image, inherited from main via CoW; (3) read-at-build caches (cargo
  registry, sccache, zig global, gradle) — read at build time, outputs written elsewhere, so sharing costs nothing — at
  each tool's own default path in the host HOME, shared into the sandboxes of projects that use the tool; no relocation,
  no symlinks. **Share only the cache subdirs** (`~/.cargo/registry`, `~/.cargo/git`, `~/.cache/zig`,
  `~/.gradle/caches`) — NOT `~/.cargo` wholesale, which also holds `config.toml`, `credentials.toml`, and `bin/` (on
  PATH); granting those writable to a sandbox would be a persistence-escape surface. Workspace-keyed state (target/,
  node_modules, DerivedData, .nx) in-image. Env wiring is 0–2 vars total; everything else is in-image config files +
  host paths. Main uses identical wiring. Spec: 03_caches.md.
- **Go: shared caches at Go's own defaults.** `GOMODCACHE`/`GOCACHE` name the host's `~/go/pkg/mod` and the user cache
  directory's `go-build` (`~/Library/Caches/go-build`) — read-at-build, layer 3; go's sum/ziphash verification + 0444
  entries make it the strongest-postured shared cache — and the host's own `go` uses the same two directories without
  configuration. `GOPATH`/`GOBIN` stay at Go's default under the private HOME, in-image (`go install` binaries are the
  `~/.cargo/bin` hazard). No Go configuration file is written: `GOPROXY`, `GOSUMDB` and `GOTOOLCHAIN` are Go's defaults
  or the project's own setting, and the supervisor sets the two cache variables. Go is not a gateway mirror client:
  `cmd/go` sends credentials only over HTTPS, so both hosts are reached through the proxy variables as opaque tunnels
  under project-standing grants (workspace git reaches a forge only through a grant, fetch-only, so the proxy IS the
  private-module path). Specs: 03_caches.md, 04_sandbox.md, 05_gateway.md.
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
- **Layered sandbox and child invariant**: start closed. Filesystem authority belongs to an immutable supervisor launch
  revision. Every anonymous shell, named session, one-shot, startup hook, and descendant receives a narrower child
  profile before repository-controlled code runs; it denies every mutation beneath `.cowshed/job/**`, and no writable
  protected-artifact/controller-writer FD is inherited. Effective filesystem grant changes drain/relaunch the
  supervisor; named sessions pinned to stale revisions conflict. Egress/simulator changes apply at the gateway. No-op
  and non-filesystem changes do not relaunch.
- **Supervisor, clients, jobs, and stream storage**: one permission-checked persistent Unix socket per workspace
  supports concurrent/reconnecting clients. Every accepted exec gets an unreused positive workspace-local monotonic job
  ID, committed before spawn. Stdout/stderr are separate opaque byte streams under one exact combined quota (default 1
  GiB) with byte count, SHA-256, and bounded redacted summary. Capture starts in bounded memory; small terminal bytes
  live as protected Arrow Binary, while `.cowshed/job/<id>/out|err` appears lazily only after threshold,
  background/replay, or checkpoint promotion. No per-job stream path is promised.
- **Frozen storage and publication DTOs**: `ProtectedOutput::{Inline{data:BinaryData},File{path}}`;
  `OutputStorage::{Captured{artifact},Redirect{source,artifact}}`; `StreamInfo{storage,bytes,sha256,summary}`;
  `ProtectedRecord::{Job(JobArtifactRecord),CheckpointManifest(CheckpointManifestRecord)}`. `BinaryData` is a bounded
  byte newtype whose JSON wire union is exact tagged `utf8|base64`; ordinary `JobInfo` may carry it, while controller
  commitments never carry payload/path/summary fields. `Redirect.source` is mutable shell state; reads resolve its
  independent protected artifact. Classify only an AST-proven redirect with a trusted actual descriptor and exact quota
  accounting; never poll/tail/reopen. `ExecRequest.stdout_copy` / `stderr_copy` are
  `Option<OutputPublication { path, policy: CreateNew|Replace }>` (camelCase in JSON/NAPI). CLI
  `--stdout-copy`/`--stderr-copy` default CreateNew; one `--replace-output` switches all requested copies and is Usage
  without a copy. Redirect snapshots and post-terminal publication use independent clone/reflink/copy, never hardlinks.
- **Checkpoint artifact barrier**: admissions/artifact writes pause; running prefixes promote and files fsync. Append a
  complete `CheckpointManifestRecord { version,repo_id,origin_incarnation,barrier_id,visible_jobs,records_sha256 }`;
  each visible stream is `{storage_kind,bytes,sha256,protected_path}` with exact captured/redirect × inline/file kind
  and path iff file. Clone while held; publish
  `CheckpointCommitment { version,order,repo_id,origin_incarnation,checkpoint_id,barrier_id, manifest_batch_sha256 }`.
  Terminal inline bytes resolve through prefix Job rows; running bytes are named fsynced files.
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
  clients). This **supersedes the old "no TLS MITM" line** and the per-authenticated-service endpoint roadmap (/github,
  /anthropic): a granted host needs no bespoke endpoint. Engine = `hyper` + `rustls` + `rcgen` + `tokio`,
  single-listener dynamic SNI (`ResolvesServerCert`), h1+h2 on both legs (SSE/streaming), LRU **leaf** cache,
  upstream-health gate at the one outbound choke point — the Bun/Hero MITM production lessons ported as **normative
  tests, not code** (05_gateway.md). Outbound is one `UpstreamConnector` over hyper/rustls. Per-workspace port block +
  token → egress/repo policy; 7644 = control plane only.
- **Telemetry and tiered authority = lmao Arrow**: protected Arrow is exact
  `ProtectedRecord::{Job(JobArtifactRecord),CheckpointManifest(CheckpointManifestRecord)}`. Complete batches/sealed
  files are content authority within the origin boundary. Controller continuity is exact
  `ControllerCommitment::{Admission(AdmissionCommitment),Terminal(TerminalCommitment), Checkpoint(CheckpointCommitment),Fork(ForkCommitment),Restore(RestoreCommitment)}`
  with exact fields frozen in 07_api.md/13_telemetry.md. It owns existence/status/order/lineage/count/hash/digests but
  carries no raw payload, inline bytes, protected/redirect path, or summary. `order` is positive, globally unique,
  contiguous, and strictly increasing; admission precedes the sole terminal, checkpoint/fork/restore sources exist, and
  lineage is acyclic. Constructors, Arrow, and JSON run one row/collection validator with `CommitmentPriorContext`. Any
  gap, contradiction, missing/altered content, invalid complete frame, or digest mismatch is `Integrity`; only an
  incomplete trailing frame may be discarded and reported with its `batch_sha256`. W3C context still spans
  CLI/Rust/MCP/CI/job/gateway boundaries. No on-disk NDJSON or telemetry daemon; `daemon-stderr.log` is pre-init only.
- **Substrates**: `cowshed_core::Substrate` has APFS image and ZFS dataset contracts. Linux enforcement combines
  Landlock, a private-loopback network namespace, and a per-workspace gateway Unix socket, with the same grants,
  revision semantics, and exit taxonomy as macOS.
- **CI runner**: cowshed is a GitHub Actions runner substrate — NixOS module `services.cowshed-runner`, ephemeral
  runners labeled `[self-hosted, cowshed, zfs]`, one workspace per job (`ci-<run_id>`), headless main refreshed by the
  green main-branch job; composite action at `.github/actions/smoothbricks-ci/action.yml` (mode auto|github|cowshed).
  Deletes Nix-restore / bun-install / Nx-cache steps and moves registry/git secrets off GitHub into the host gateway.
  Spec: 10_ci.md.
- **CLI/error contract ("self-driving")**: stdout is machine-readable only (bare value or the frozen JSON envelope); all
  guidance is stderr with `next:` hints. Own exits are 0 success, 1 internal, 2 usage, 3 not-found, 4 conflict, 5
  environment-missing, 6 sandbox-denied, and 7 integrity. Child exits pass through unchanged. Exec-wrapper errors are
  100–106 in the same taxonomy order, with integrity at 106. JSON/NAPI/MCP use the shared `integrity` code; MCP maps it
  to -32006. `-q`/`--quiet` are aliases. Configuration remains optional.
- **Structured stdin, redirect evidence, and publication**: `ExecRequest` supports empty, inline binary/streamed, or
  safe workspace-file stdin. It also has separate `stdout_copy`/`stderr_copy` post-terminal publications with explicit
  create/replace policy. Shell text remains opaque by default. A real AST may recognize a narrowly proven simple literal
  `>`/`2>` only when supervisor descriptor interposition preserves exact capture/quota semantics; terminal bytes are
  independently snapshotted into the protected artifact. Ineligible forms use ordinary shell semantics and cowshed
  claims only bytes reaching captured pipes. The old shell-AST hardlink optimization is deleted: no protected inode may
  have a writable hardlink alias.

## Working rules

- Toolchain configuration lives in `tooling/direnv/devenv.{yaml,nix}`. Invoke repository commands from `<repo-root>`;
  never embed a workstation-specific absolute path.
- Keep the implementation greenfield: migrate all callers cleanly and leave no compatibility paths.

## Suggested workflow shape

Phase 1 — `cowshed-core` and `cowshed-shell`: substrate lifecycle, repository binding and policy lookup, markers/grants,
env wiring, sandbox profile generation, multi-client supervision, jobs, output quotas, and revision-bound relaunch. Unit
tests cover pure contracts; integration tests use isolated temporary paths. Phase 2 — parallel work on the CLI, gateway,
N-API bindings, MCP server, and escape suite. Phase 3 — adversarial conformance and sandbox review, confirmed fixes,
then targeted verification plus the repository's full Rust checks and a CLI transcript against the docs.

## Acceptance

- `cowshed adopt && cowshed fork x && cowshed exec x -- <build> && cowshed push x && cowshed rm x` works end-to-end on
  each supported host, sandboxed, with caches warm from main.
- Exit code 6 and a grant hint appear only on authoritative sandbox-denial evidence. After a filesystem grant, the prior
  supervisor drains and relaunches at the new revision before retry; egress and simulator grants apply immediately.
- `cowshed attach` fast path ≤ 25 ms; `cowshed fork` ≤ 1 s cold; `cowshed rm` returns instantly.
- Zero host-visible inode growth per workspace beyond the image file + grants file.
- Substrate parity: the acceptance flow above passes on Linux/ZFS with Landlock enforcement (integration leg; can run in
  CI on a ZFS-capable Linux host once the runner exists — bootstrap order is macOS first, then Linux substrate, then the
  runner that uses it.)
