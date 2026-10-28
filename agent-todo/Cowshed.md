# Cowshed

- Post-reboot proof owed for detached disk-image commands (landed): run
  `real_apfs_a_disk_image_attach_survives_its_callers_kill` (cowshed-core lib) green. It is compile-verified and its
  unit proofs pass, but new `diskutil image attach` calls hang host-wide until the attach pool recovers, so the
  real-APFS run could not complete before the reboot.

- Post-reboot proof owed for fixture-attach-pool (landed): run `COWSHED_TIMING=1` nextest `apfs_native` + `clone_intent`
  real-APFS subset and count `apfs-command attach.*start` lines (one shared attach in the flag table). New
  `diskutil image attach` calls hang host-wide until the attach pool recovers, so this proof could not run before the
  reboot; compile and merge evidence are recorded in the landing.

- Qualify the initial `jobCpuSamplePeriod` (1 s) and observed-CPU leaf coverage threshold (900‰) on real Cargo/Nx/Bun
  gate jobs; report sampler cost, `cpuPct` stability and coverage evidence (specs/cowshed/07_api.md, "Job monitoring"
  and "Process-tree observations").
- Complete the selected Linux ptrace source's production spawn/reaper handoff on a real Linux execution substrate and
  prove fork/exec/exit, retained pidfd identity, burst coverage and PID reuse through controller/N-API
  (specs/cowshed/07_api.md, "Process-tree observations" and "Complete job accounting and observation reconciliation";
  specs/cowshed/11_shell.md, "Complete process tree, compact observations"). Until that cutover, `processes()` and
  `processEvents()` return the typed `EnvironmentMissing`. This macOS drive provides no Linux execution substrate. Keep
  all-targets Linux cross-checks green without treating them as production wiring, Linux behavioral acceptance or
  real-workspace acceptance.

- Make smoke fixture disposal and interrupted-run recovery leave no orphaned project registrations. Keep
  `specs/cowshed/08_testing.md` §Integration tests' owning teardown and retained-failure evidence, and
  `02_workspaces.md` §`cowshed rm`'s verified preservation fences. Cover success, assertion failure, panic, and killed
  runs whose temporary checkout root disappears; an unavailable root must not make its durable registration invisible to
  recovery or require raw store deletion.

- Move the APFS workspace host into `cowshed-apfs` so a core land stops rerunning real-image tests. This follows the
  `cowshed-apfs`/`cowshed-os` split. Until it lands, the host-subject real-APFS tests run as the `real-apfs-host` serial
  group over core's closure (specs/cowshed/08_testing.md §Integration tests). The host is `MacOsApfsExecutionHost`:
  `cowshed-core/src/storage/apfs/native.rs`, `native/{blank_template,build_volumes,re_adopt}.rs` and
  `storage/apfs/extents.rs`. Line numbers are at main 570cd23a4. It is done when those files live in `cowshed-apfs`,
  `cowshed-apfs` has no `cowshed-core` dependency, every real-image test is a `cowshed-apfs` test, and the
  `real-apfs-host` group is gone. Invert in this order:
  1. The host trait and its error. `ApfsExecutionHost` (storage/apfs.rs:400, impl native.rs:4117) and `ApfsStorageError`
     (storage/apfs.rs:736) move down with the host; core's `ApfsSubstrate<H>` keeps consuming the trait. Its argument
     types move with it: `ApfsSubstrateConfig` (storage/apfs.rs:34), `LockMode` (:332), `Labelled` (:385),
     `MarkerExpectation` (:129), `MetadataPolicy` (:116), `PendingAdoption` (:228), `PendingPublicationFact` (:215),
     `PublicationError` (:345), `ResumableClone` (:252). So do the path helpers `layout` (:3308), `canonical_image_path`
     (:3312), `companion_path` (:3274), `COMPANION_SUFFIX` (:3272), `main_aware_mount_point` (:3456),
     `recovery_staging_mount` (:3369), `retired_image_below` (:3413), `split_retired_stem` (:3427), `volume_key`
     (:3469), `workspace_lock_path` (:2121) and `combine_cleanup` (:3209).
  2. Storage records the host reads and writes. These are data, so they move into a `cowshed-apfs` record module that
     core imports, not behind traits: the `storage::lifecycle` value types (lifecycle.rs:18 `Revision` through :879
     `KernelMountFact`, `ExtentCount` :833, `Pin` :118 with its wire `From` impls at native.rs:301 and :310); the
     `storage::recovery` namespaces and records (recovery.rs:16, :17, :19, :20, `LifecycleIntentJournal` :618,
     `ReAdoptSource` :65, `ReAdoptMain` :86, `MissingMainBuild` :125, `ReAdoptBuild` :38, `ReAdoptEmptyBuild` :158);
     `storage::deletion_log` (deletion_log.rs:43, :60, :141); `storage/mod.rs` (`WORKSPACE_MARKER_PATH` :28,
     `PRE_RESTORE_PREFIX` :32, `CheckpointLabel` :37, `discover_session_images` :340, `civil_from_days` :369,
     `verify_no_symlinks` :452, `StorageLayoutError` :504); and `storage::bootstrap::is_reserved_store_namespace`
     (bootstrap.rs:65).
  3. Workspace metadata the host persists. From metadata.rs: `DetachedWorkspaceMetadata` :1104/:1188,
     `GRANTS_SIDECAR_SUFFIX` :1202, `MARKER_VERSION` :15, `SIDECAR_VERSION` :16, `Platform` :798, `PublicationState`
     :1093, `WorkspaceIncarnation` :352, `WorkspaceInfoSnapshot` :1067, `WorkspaceMarker` :695/:789, `WorkspaceName`
     :281, `WorkspaceRole` :662, `sidecar_path` :1211, `append_suffix` :1205, `validate_image_path` :165, `read_json`
     :1226, `write_json` :1234 and `image_from_sidecar_path` :1217. From repository.rs: `RepoId` :35, `OwnedRepoIds`
     :532 and the directory constants :17-:21. `RepoId` already has a leaf home in `cowshed-gateway-types::repo_id`.
  4. Build-volume facts: the data moves down and the policy stays up. From build_volume/mod.rs: `BuildVolumeId` :65,
     `BuildVolumeRole` :110, `BuildVolumeRecord` :126, `BuildVolumeState` :156, `LinkResolution` :473,
     `BuildVolumeLayout` :492, `RECORD_SUFFIX` :49 and `ReleaseClaim` :874. From build_volume/link.rs: `BUILD_LINK` :25,
     `linked` :28 and `point` :43. Nx knowledge stays in core, so `nx::volume_holders` (nx.rs:561 macOS, :460 Linux) and
     `nx::discard_daemon_records` (nx.rs:50) become a capability the host is handed.
  5. Core operations the host calls become capabilities passed in at construction:
     - `copy::copy_until_quiescent_blocking` (copy.rs:162) and `verify_tree_blocking` (:220), called at
       native.rs:4422-4423.
     - `git::is_hidden_path` (git.rs:2853), `ignored_by_blocking` (:2674) and `held_by_repository_blocking` (:2687),
       called at native.rs:6504-6550.
     - `workspace_credentials::{mint_workspace_credentials, validate_private_key, validate_public_workspace_assets}`
       (workspace_credentials.rs:89, :170, :179), imported at native.rs:37.
     - `runtime::build_volumes::Quiet` (runtime/build_volumes.rs:77), used at native/build_volumes.rs:142.
     - The `runtime::volume_usage::VolumeStat` impl (volume_usage.rs:27, impl at native.rs:6642).
  6. Errors. The `CowshedError` uses (native.rs:4801, native/build_volumes.rs:438, and `environment_missing` at
     native.rs:6663) become `ApfsStorageError` variants that core maps.

  Then move the host-subject tests into `cowshed-apfs`: the `storage/apfs/native*` real_apfs tests,
  `tests/apfs_integration`, `tests/apfs_native_macos` and the CLI host-subject dispatch cases. Delete the
  `real-apfs-host` group from `.config/nextest.toml`. Prove the result with two `nx` dry runs: a core-only change leaves
  the `cowshed-apfs` real-image target a cache hit, and a native.rs change runs it.

- Verify that build volumes keep main's source image on the no-clone fragmentation curve: sample its extent count across
  a week of lands with sessions held and compare it with the single-image baseline (`specs/cowshed/16_build_volumes.md`
  "Why two volumes", item 1; `specs/cowshed/01a_apfs.md` "Images", its "Clone cost follows extents, not size" bullet).
  Within the curve, `cowshed defrag main` stays a repair for imports and not a routine operation.

- Verify how rust-analyzer and editors behave when `target` is a symlink into the build volume
  (`specs/cowshed/16_build_volumes.md` "One link per checkout"). The behavior is unverified.

- Replace the workspace supervisor's 5-second probe of its sandboxed Nx daemon (`specs/cowshed/04_sandbox.md`, the
  daemon keeper) with an event-driven exit watch if Nx exposes one.

- Specify build volumes in the ZFS substrate (`specs/cowshed/09_substrates.md` "ZFS substrate", "Mapping"): the dataset
  layout, seeds as snapshots, and how a land adopts the landing dataset while `zfs promote` stays unused.
  `specs/cowshed/16_build_volumes.md` "Substrate" names `<pool>/cowshed/<owner>/<repo>/build/<id>` while 09 roots every
  dataset under `<pool>/cowshed/projects/<owner>/<repo>`; one owner states the layout and the other links to it.

- Build the complete fork/exec observer, per-process usage/blocker and process-event streams, process/job resource
  spans, Linux cgroup totals and accounting samples, charged-memory measurements, and event-source coverage/overhead
  qualification; B13's macOS Census and sampling clock are canonical (specs/cowshed/07_api.md, "Job monitoring" and
  "Process-tree observations").

- Before a nested executor runs its command as asked for want of a parent pool, distinguish a failed connect caused by a
  full listen backlog or local file-descriptor exhaustion from an absent parent pool. `holdLease`
  (`packages/nx-plugin/src/executors/bounded-exec/gateway-lease.ts`) classifies every connect error as an absent
  endpoint, and the executor (`packages/nx-plugin/src/executors/bounded-exec/executor.ts`) then runs the command without
  a grant beside a live pool (`specs/cowshed/05_gateway.md` "Every Nx run budgets itself").

- Measure cold `new`, `fork` and `checkpoint` on RAW images against the ≤ 1 s budgets. The clonefile and attach figures
  behind those budgets come from a benchmark of an earlier image format, and no RAW timing backs them
  (`specs/cowshed/08_testing.md:586` "APFS / Seatbelt (macOS)"; `specs/cowshed/00_overview.md:152` "Tradeoffs";
  `specs/cowshed/09_substrates.md:208` "APFS image substrate (reference)").

- Establish the ZFS/Landlock baseline on a ZFS-capable Linux runner, then confirm or revise its budgets (cold `new` ≤
  250 ms in particular). The figures are placeholders until they are first measured (`specs/cowshed/08_testing.md:606`
  "ZFS / Landlock (Linux) — separate baseline").

- Research whether the private DiskImages2 attach API lowers the APFS attach floor. Budgets must not assume it until
  that research is done (`specs/cowshed/08_testing.md:586` "APFS / Seatbelt (macOS)").

- Implement Linux enforcement of the workspace-relative `denyWrite` and `deny` lists in the Linux workspace filesystem
  ruleset. Both are Seatbelt-only (`specs/cowshed/04_sandbox.md:462` "Project-standing grants").

- Verify whether Go-built binaries on darwin ignore `SSL_CERT_FILE` (`crypto/x509`), which would make Go clients of
  intercepted hosts need `--opaque`. Inventory exactly which macOS-native TLS tools (Security.framework-backed, such as
  Swift/Xcode and some system utilities) ignore the env trust anchors and so need `--opaque`. Verify that Bun honors
  `NODE_EXTRA_CA_CERTS`, so the workspace CA works for `fetch` and `bun install` over an intercepted host; if it does
  not, Bun is an interception gap for the trust-anchor table (`specs/cowshed/04_sandbox.md:578` "Egress-interception
  trust anchors").

- Verify whether OpenZFS block cloning (BRT) lets a pool-shared clone-materializing cache whose copy path uses
  `copy_file_range` (including bun's Linux copyfile backend) reach reflink speed across datasets, so that layer 2 can be
  shared on ZFS (`specs/cowshed/03_caches.md:41` "The three layers").

- Implement the btrfs substrate (subvolume snapshot plus clone) when a host needs one. No btrfs substrate exists
  (`specs/cowshed/09_substrates.md:351` "macOS ZFS, btrfs").

- Decide whether to support group-ACL cross-uid file sharing, and per-user volume naming for machines that use several
  postures. Neither is supported, and doctor refuses volumes owned by another uid (`specs/cowshed/14_nix.md:237`
  "Explicitly unsupported (v1)").

- Verify that the lmao TypeScript and Rust tracers adopt `TRACEPARENT` at init and stamp outbound `fetch`/HTTP requests
  with it, which promotes first-party traffic to tier-1 attribution (`specs/cowshed/13_telemetry.md:35` "Trace context
  propagation"; `specs/cowshed/13_telemetry.md:150` "Attribution tiers").

- Measure the one-batch audit crash window under the decision-boundary/short-timer flush policy, and confirm that no
  audit-relevant event class needs a per-event flush (`specs/cowshed/13_telemetry.md:494` "Tradeoffs").

- Confirm that Seatbelt violations reach the unified log with pid and operation from an unsandboxed controller context
  (the log store is permission-gated from sandboxed sessions, so this could not be tested from one). This gates only the
  log-correlation enhancement; the errno signal (EPERM vs ECONNREFUSED) stands alone (`specs/cowshed/04_sandbox.md:656`
  "Sandbox-denial evidence").

- Survey which real tools break under the outbound-only port model, where an in-sandbox client can reach only its own
  block ports: spawn a helper server on a random port and connect back (test runners, HMR side-channels), and check
  whether the PORT convention covers them (`specs/cowshed/04_sandbox.md:283` "Trusted supervisor and child profiles").

- Measure whether a fresh clone's `.envrc` evaluation reuses the in-image `.devenv`/Nix state (cheap) or re-evaluates. A
  warm `direnv exec` costs seconds per invocation, which is the pool rationale (`specs/cowshed/11_shell.md:12` "Shell
  activation and process reuse").

- Decide whether bun's registry token travels through the per-workspace port URL and registry auth (the workspace
  identity is primarily its data-plane port), which determines whether `COWSHED_GATEWAY_TOKEN` stays a load-bearing
  export, and verify that cargo `trim-paths` keeps a warm `target` valid across mount paths
  (`specs/cowshed/05_gateway.md:200` "npm registry mirror — eligible requests to an admitted registry";
  `specs/cowshed/16_build_volumes.md:787` "One Cargo environment on every path").

- Verify declarative host setup on a real home-manager host: HM-managed LaunchAgents (gateway, login-attach/autosave)
  load and keep alive across generation switches; HM-created symlinks resolve into `/nix/store` under both `home.file`
  and `mkOutOfStoreSymlink`, which is the validate-vs-mutate signal; and under posture B2, nix-darwin LaunchDaemons with
  `UserName = dev` run the gateway and autosave, with the dev login keychain unlocked by the first dev shell or
  `cowshed unlock` (`specs/cowshed/14_nix.md:9` "`programs.cowshed` (home-manager)"; `specs/cowshed/14_nix.md:36`
  "Ownership detection: validate, never mutate"; `specs/cowshed/14_nix.md:93` "Posture B — dedicated `dev` uid
  (recommended hardening)").

- Verify Go wiring on a real host: `GOENV` covers every go invocation, including editor-spawned gopls (direnv
  integration) and unwrapped spawns, and no file-based mechanism can remove the export; and the GOMODCACHE
  concurrent-sharing guarantee (0444 entries, internal locking) holds across many workspaces plus main, including
  projects that set `GOFLAGS=-modcacherw` (`specs/cowshed/03_caches.md:115` "Wiring"; `specs/cowshed/03_caches.md:378`
  "New-package flow").

- Verify iOS and simulator behavior under posture B: simulator reliability without an Aqua session (what degrades over
  pure SSH, and whether a persistent background dev session fixes it); that Expo CLI resolves `xcrun` through `PATH` so
  the in-image wrapper intercepts `i`/`shift+i`; that a dev-built, ad-hoc-signed `.app` from the drop dir installs and
  launches cross-uid in the personal-session simulator via `simctl install`; the gateway-to-session-broker
  authentication mechanism (mutual secret provisioning at posture-B setup); the headless-signing recipe
  (`security set-key-partition-list`, once-per-boot unlock) under the dev uid; idb as the dev-side streaming/HID path
  for agents observing headless simulators; and the exact mach-lookup/XPC-socket inventory of the "simulator" Seatbelt
  preset for CoreSimulator (`specs/cowshed/14_nix.md:137` "iOS, Xcode, and simulators (posture B)";
  `specs/cowshed/05_gateway.md:232` "`/sim/` — personal-session simulator broker (posture B)";
  `specs/cowshed/04_sandbox.md:566` "The "simulator" profile preset").

- Verify macOS desktop behavior: Developer-ID vs ad-hoc Gatekeeper behavior when `cowshed app promote` installs into
  `~/Applications` (ad-hoc needs the override, Developer-ID does not); that `cp`/`cp -R` into the drop dir adds no
  quarantine xattr, re-confirmed on a real signed `.app` bundle; an Electron/RN-desktop promoted app pointing at the dev
  server over loopback across uids; driving a dev-session desktop app from a dev-side agent through accessibility APIs
  or AppleScript; and that `app promote` refuses to run as anything but the personal user and is unreachable from the
  sandbox and gateway (`specs/cowshed/14_nix.md:186` "macOS desktop apps: three lanes").

- Linux controller Git currently selects unconfined `git_command_at` in the non-macOS branch of
  `run_git_at_with_objects` (`packages/cowshed/crates/cowshed-core/src/git.rs:3297–3298`). Implement the Landlock
  controller Git boundary required by `specs/cowshed/04_sandbox.md:341`, "Controller Git is separately confined", and
  refuse controller verbs on platforms that cannot enforce it. Hardened flags alone do not satisfy that confinement
  contract.

## Refused build hold reports no holders

On 2026-10-12, an ordinary fork of `<source-ws>` retained a stale seed because the source build hold was shared, but its
refusal reported an empty holder list. The fork completed; it did not carry the donor's newly warmed Nx results. This is
separate from the lmao task-input repair.

Contract: `specs/cowshed/16_build_volumes.md:329-339`, **Targets and seeds**, “A reseed clones the live volume only
while it has no writer,” step 1 requires an admitted-job refusal to name each process holding the hold. **Which volume a
seed holds** (`16_build_volumes.md:398`) likewise requires a skip to name each holder and preserve the seed. Preserve
that exclusion; investigate why the refused shared hold has no reported owner, rather than forcing a seal or copying
live Nx databases.

Start in `packages/cowshed/crates/cowshed-core/src/storage/apfs/native/build_volumes.rs:122-145`,
`MacOsApfsExecutionHost::claim_capture_source`: a refused claim collects hold owners through
`crate::build_volume::nx::holders` (`src/build_volume/nx.rs:568-569`, macOS), then constructs
`ApfsStorageError::BuildVolumeHeld` (`src/storage/apfs.rs:748-760`, its diagnostic formatter). The current tree's error
wording differs from the installed binary's recorded text below; no matching-binary source diagnosis was performed.

Path placeholders: `<checkout>` is the source checkout; `<store>` is the project's Cowshed store; `<image-id>` is the
affected build volume; `<host cowshed>` is the installed host CLI. The affected image is `<store>/build/<image-id>.dmg`,
and its hold is `<store>/build/<image-id>.dmg.hold`.

Commands exercised from `<checkout>`, with host paths and workspace names redacted:

```sh
taskpolicy -b '<host cowshed>' --project '<checkout>' exec '<source-ws>' -- env -u LMAO_GIT_REVISION nx run-many --projects=lmao --targets=cargo-test-lmao-core,cargo-test-lmao-macros --parallel=1 --outputStyle=static
taskpolicy -b '<host cowshed>' --project '<checkout>' fork '<source-ws>' '<first-proof-ws>'
taskpolicy -b '<host cowshed>' --project '<checkout>' exec '<source-ws>' -- nx daemon --stop
taskpolicy -b direnv exec . nx daemon --stop
taskpolicy -b '<host cowshed>' --project '<checkout>' fork '<source-ws>' '<second-proof-ws>'
```

The first fork reported a seed `1496033 ms` behind and an in-use volume held by the checkout's Nx daemon, PID 43143 in
the first fork's output. The managed daemon-stop attempt emitted `NX kill EPERM` followed by `Daemon Server - Stopped`;
that message was not treated as proof that the process exited. The host-shell stop above was then attempted through Nx's
supported interface. The second fork completed in 39.45 s and included this output with the same substitutions:

```text
cowshed: <source-ws>'s seed stays 1937886 ms behind its build volume, so <second-proof-ws> misses what <source-ws> built since: the build volume could not be sealed: build volume <store>/build/<image-id>.dmg has shared ownership holds on <store>/build/<image-id>.dmg.hold: []; the next fork retries
<second-proof-checkout>
next: cowshed exec <second-proof-ws> -- <cmd>
```

No owner or cause was established for the empty list. No raw process kill, forced unmount, image deletion, cache bypass,
live database copy, or implementation change was attempted for this failure.

## CLI guidance gaps left by the DX pass of 2026-10-28

The DX pass that replaced the fork extent warning with the clone-time rule and grouped the command map changed help,
warning and hint text only, by the owner's ruling of 2026-10-28. These gaps stay, each with its owner and its count:

- **Prose `next:` hints.** About 1,070 of about 1,380 `CowshedError` hints in the crates' production sources (tests
  excluded) are prose remedies, not commands, against `specs/cowshed/06_cli.md` "Self-driving conventions" ("every
  failure's stderr includes the exact command that would resolve it"). The counts are a syntactic scan of each
  constructor's second argument; the largest holders are `cowshed-core/src/runtime/project.rs` (318),
  `cowshed-core/src/git.rs` (112) and `cowshed-core/src/runtime/supervisor.rs` (108). The pass fixed the two the v2
  comparison report named (`workspace <name> does not exist` and `workspace main does not exist` now hint `cowshed ls`).
  The owner ruled out a rewrite sweep and a rendering rule for now. Owner: the cowshed CLI contract
  (`specs/cowshed/06_cli.md`), staffed by the lead.
- **`controller-open` lifecycle spans on `ls` and `path`.** `cowshed ls` and `cowshed path` print
  `cowshed: controller-open connect|project start|done` lines on every call (`cowshed-cli/src/runtime.rs`, the
  `controller-open` spans). `cowshed-os/src/timing.rs` makes lifecycle spans unconditional and reserves
  `COWSHED_TIMING`-gated latency spans for verbs whose stderr belongs to someone else, naming `path`; the controller
  open on these verbs is the lifecycle kind. An env-var gate is a workaround shape (AGENTS.md rule 6). Owner:
  cowshed-cli runtime, staffed by the lead.
- **Per-fork image-allocation warnings.** Every build-volume clone prints
  `cowshed: warning image clone before|after … reason=allocated_absolute` (a 128 GiB `ALLOCATED_WARN`,
  `cowshed-core/src/storage/apfs/native/image_telemetry.rs`); one fork of SmoothBricks main on 2026-10-28 printed six,
  because every build image there is 392 GiB or more allocated. It is the same per-fork noise the extent warning was.
  Owner: the image telemetry in `cowshed-core/src/storage/apfs/native/image_telemetry.rs`, staffed by the lead.
- **No measured source-clone time outside the fork.** Doctor's extent findings are information only, and the clone-time
  warning fires only on the fork that measured it; no record keeps the last measured time, so doctor cannot apply the
  same 4 s decision (`source_clone_is_slow` in `cowshed-core/src/storage/lifecycle.rs`). A record would be a second
  store unless an existing authority (the fork's spans) already answers it. Owner: cowshed doctor, staffed by the lead.
