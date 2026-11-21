# cowshed: Linux host and the ZFS substrate

Target: `specs/cowshed/01b_zfs.md`, `09_substrates.md` ("ZFS substrate"), `04_sandbox.md` ("Linux enforcement"). This
brief holds what stands between the tree and that target, with the evidence for each gap.

## Proven

On a NixOS container whose datasets are zoned to its user namespace (kernel 6.18, OpenZFS 2.4.3, musl release build,
systemd user daemon), 2026-11-21: `setup`, `gateway start`, `adopt`, `fork`, `exec`, a fork of a fork, `land`, `rm`,
`rm --abandon`, `gc` and `doctor` run, and the store ends with only `main`. Each workspace is a dataset named for its
place (`main`, `ws/<name>`, `ws/.trash/<name>-<incarnation>`, `build/<id>`); a job runs under Landlock in private user,
network and mount namespaces. Host proofs: `cowshed:zfs-helper-test` and `storage::directory::zfs_tests` (both need
`sudo -n` and `COWSHED_ZFS_TEST_PARENT`).

## Gaps

1. **`01b_zfs.md` describes a design the code does not follow.** A seed is its own `build/<id>` dataset, not a
   `@seed-<n>` snapshot, and freshness is the shared write mark, not `written@`. Mountpoints are set per dataset, since
   a payload moves between its store path and its mountpoint, rather than inherited from `ws` and `build`. The spec's
   "Seed", "Behind" and layout diagram need rewriting to the dataset design.
2. **Tests that are not green on Linux.**
   - Three `git::tests` (`unregistering_one_worktree_spares_a_detached_sibling`,
     `rename_preserves_linked_source_index_wip_and_branch_across_resumed_handoff`,
     `fork_carry_clean_and_ref_preserve_source_bytes_and_index`): `git worktree repair` reads every sibling worktree's
     `.git` pointer, which the controller-git policy gives no rule (`04_sandbox.md`, Open item 9).
   - Eleven `runtime::build_volumes` `directory_*` tests and `land_lease::a_second_taker_is_refused_naming_the_first`
     fail only when run as threads of one process, and
     `exec::profile_file::tests::gc_removes_a_profile_only_when_every_live_supervisor_started_after_its_last_launch`
     failed once in a combined run. Unverified reading: a forked child holds a copy of a lock or listener descriptor
     until its exec.
   - `tests/project_runtime.rs` and `tests/workspace_supervisor.rs` compile on Linux and have not been run; fixtures
     that reach a launch must name the launcher (`sandbox::name_launcher`).
   - About 190 tests in `sandbox.rs`, `runtime/supervisor.rs` and those two files are still macOS-gated and unaudited.
   - `directory_adoption_copies_nothing_from_the_target` needs `sqlite3` on PATH;
     `a_host_daemon_whose_record_is_handed_over_serves_on_across_the_move` asserts a daemon reads unsandboxed, which a
     session under a seccomp filter cannot show.
3. **Sandbox gaps** (`04_sandbox.md`, "Linux enforcement", Open): pathname Unix sockets outside the two masked
   directories are reachable; nothing is write-protected inside the workspace mount (marker, token, CA); `/proc` shows
   host command lines; namespaces and the connector are per launch, not per workspace.
4. **The helper's mountpoint check and the mount are two steps** (`09_substrates.md`, "Privileged helper", Open).
5. **Residue seen in the real run.** `<name>.dmg.lock`, `.intent.lock` and `.land.lock` stay in `sessions/` after a
   workspace is landed or removed; whether macOS leaves the same is unchecked. Messages on a ZFS store still say "apfs"
   and "image" (`apfs staging/unmount`, "image stayed attached as <dataset>").
6. **Host-side PATH lists that still retype FHS directories:** `cowshed-cli/src/capabilities/mbx.rs` (runs `mbx setup`)
   and `declared_land.rs::bootstrap_path`. The owner is `cowshed-core/src/host_path.rs`.
7. **`bootstrap/native/linux.rs::locate_helper`** prefers the helper beside the executable; the runtime's
   `choose_helper` prefers a setuid one on PATH. One rule should serve both.
8. **macOS is compile-checked only.** `cargo check -Zbuild-std --target aarch64-apple-darwin` is clean for every crate;
   nothing was run on a Mac, so every moved `macos.rs`, the ZFS substrate on macOS and the `com.apple.*` properties of
   `01b_zfs.md` are unproven there.
9. **mbx on NixOS** needs the `nix-command` and `flakes` features; without them `setup` exits 5 after everything else
   succeeded.
