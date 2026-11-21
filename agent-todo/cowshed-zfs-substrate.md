# cowshed: the ZFS substrate

Target: `specs/cowshed/01b_zfs.md`, `specs/cowshed/09_substrates.md` ("ZFS substrate"), `16_build_volumes.md`. This
brief holds what stands between the tree and that target, with the evidence for each gap.

## Landed

- **`cowshed-helper`** (`packages/cowshed/crates/cowshed-helper`): the fixed verb set, containment below one root that
  names its caller (`cowshed:owner`), the property allow-list, the mountpoint rule and the handover of a created
  dataset's root. Proven against real ZFS by `cowshed:zfs-helper-test` (08_testing.md, "Real ZFS"). The macOS arm
  (`src/host/macos.rs`) is written from the `fstatfs` contract and has never been compiled: this host has no Apple
  target installed.

## Gaps, in dependency order

1. **Nothing calls the helper.** No production code spawns `cowshed-helper`; its only caller is its own host proof. The
   client (spawn through the bounded runner, map exit status 2 and `zfs` failures into `ApfsStorageError`) belongs to
   the storage host of gap 3.
2. **The root is still `<pool>/cowshed` in selection and bootstrap.** `storage/bootstrap.rs` builds the root as
   `zfs_name(pool, ZFS_COWSHED_ROOT)` in `plan_zfs`, `SubstrateConfig` holds only a pool, and `.cowshed.toml`
   `[substrate]` accepts only `pool`. The helper and 09_substrates.md already take `<parent>/cowshed`; a host whose only
   writable tree is a delegated or zoned dataset cannot be selected until the configuration names the parent dataset.
   `ZFS_CACHES_CHILD` is still planned where 09 specifies `build-cache`, and the tool is spawned as `/usr/sbin/zfs`, a
   path NixOS does not have (AGENTS.md rule 12: spawn by PATH name).
3. **There is no ZFS storage host.** `ApfsExecutionHost` (`storage/apfs.rs`) has two implementations,
   `MacOsApfsExecutionHost` and `DirectoryExecutionHost`. A `ZfsExecutionHost` follows the directory host's shape (the
   layout's image path stands for each payload, with its sidecar, lock and trash grammar beside it) with a dataset as
   the payload: `clone_image` is snapshot plus clone, `retire_image` is unmount, `canmount=off` and rename into
   `.trash`, `reclaim_image` is destroy of the clone then its origin, `resize` is `refquota`, `list` is `zfs list`
   joined with the mount table.
4. **The project runtime does not exist off macOS.** `runtime/mod.rs` compiles `storage_host`, `build_volumes`,
   `daemon_watch`, `held_bundles`, `job_accounting`, `job_attribution`, `mount_activity`, `process_events`,
   `process_fence` and `process_monitor` for macOS only; `ProjectRuntime::open_native` and `open_directory`
   (`runtime/project.rs`) answer `environment_missing` elsewhere, and `storage/bootstrap/native/linux.rs` answers
   `UnsupportedPlatform` for setup. So `adopt`, `fork`, `exec`, `land`, `rm` and `gc` reach no storage on Linux whatever
   the substrate. This is the largest part of "ZFS on Linux" and is not ZFS work: each gated module needs its Linux
   mechanism (several exist: `job_cgroup`, `host_pressure/linux.rs`, `exec/linux.rs`) behind the interface the macOS one
   already serves.
5. **Platform mechanism still sits inline.** 1,904 `cfg(target_os)` occurrences in 138 files lie outside files named for
   a platform (`rg -c 'cfg\(.*target_os' --glob '*.rs' packages/cowshed/crates`, excluding `linux.rs`, `macos.rs` and
   `cowshed-apfs`). Most gate tests or macOS-only features of gap 4. Real mechanism inline, to move behind
   `linux.rs`/`macos.rs` as `host_pressure/mod.rs` does: `build_volume/nx.rs` (procfs census beside `libc::vinfo_stat`),
   `cowshed-os/src/process.rs` (`KERN_PROCARGS2` parsing beside the Linux `running`), `runtime/job_groups.rs`
   (`proc_pidinfo` identity), `runtime/supervisor.rs` (`observed_command`, `spawn_observed`, the paired
   `cgroup`/`observations` methods), and `cowshed-os/src/{deadline,fsio}.rs`. Nothing checks the rule: there is no lint
   for it.
6. **The helper's mountpoint race** (09_substrates.md, "Privileged helper", Open). Until the helper mounts by
   descriptor, installing it setuid or by sudoers on a host where the caller is not already root-equivalent is an
   escalation.
7. **macOS is unmeasured.** No macOS host with OpenZFS has run any of this: the `com.apple.*` properties, the
   no-`IOMedia` claim, and whether mounting needs the helper there (01b_zfs.md, "Open questions").
