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

- Specify and implement a supported record-driven forget for corrupt or orphaned project registrations. The current
  `specs/cowshed/06_cli.md` §Commands (`mount main`, `rm`) and `02_workspaces.md` §`cowshed rm` provide remount and
  checkout-based removal, not removal when the recorded checkout and its marker are unavailable. Five smoke
  registrations keep setup at integrity exit 7 while the installed daemon is correct and running. Their supported
  remount/removal attempts refuse missing checkout parents, duplicate image attachments, or a missing workspace marker;
  no store records are deleted manually. Define the ownership and preservation contract before adding the verb; prove
  exact-project removal without damaging an uncertain attachment or another project.
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
