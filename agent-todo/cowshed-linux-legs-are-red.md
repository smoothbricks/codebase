# cowshed's Linux test legs are red, and no gate runs them

The SB publish of 2026-11-25 failed in `linux-release-candidate`'s unit tests on the self-hosted NixOS runner, with 21
cowshed tests red. Main re-dispatched that release without cowshed. Every land gate runs macOS only, so none of these
reds was ever seen at a land.

**The real fix is a gate leg.** cowshed's cargo-test targets must also run on Linux, the NixOS runner `publish.yml`
uses, as a leg of the land gate (or a required CI job before a release candidate). Without one, macOS-only assumptions
keep landing unseen, and the publish is the first place they show. Each item below needs a red on that leg.

## Linux portability defects (product code)

- `copy::tests::verification_walk_checks_content_namespace_and_link_targets` (copy.rs:2322, NotFound). Linux
  `copy::native::directory_metadata` (copy/native/linux.rs:105) runs `fs::set_permissions` and `set_times` through a
  symlink. On a symlink whose target is missing (here `link -> changed`) it fails ENOENT; on one whose target exists it
  changes the target's mode and times. macOS copies the link's own metadata (`fcopyfile` on `O_SYMLINK`). Owed: the link
  itself, never its target (`lchown`/`utimensat(AT_SYMLINK_NOFOLLOW)`; Linux has no lchmod for links).
- `git::tests::{fork_carry_clean_and_ref_preserve_source_bytes_and_index, rename_preserves_linked_source_index_wip_and_branch_across_resumed_handoff, unregistering_one_worktree_spares_a_detached_sibling}`
  (git.rs:6418, 8282, 8653). `git worktree repair` fails with "could not open '<worktree>/.git' for writing: Permission
  denied", because a linked worktree's `.git` file is not writable on Linux at the moment cowshed repairs it. Cause not
  yet traced [INFERENCE: a read-only mode or the Linux write fence that cowshed puts on the source or the clone].
- `build_volume::nx::tests::a_host_daemon_whose_record_is_handed_over_serves_on_across_the_move` (nx.rs:1875).
  `procinfo::sandboxed` (procinfo linux.rs:62) counts `NoNewPrivs: 1` as sandboxed, so on a runner whose service sets
  `NoNewPrivileges` (systemd hardening) every process counts as sandboxed, the host's daemons included [INFERENCE: the
  runner's unit file is unread]. The assertion is right; the Linux test for "sandboxed" cannot tell cowshed's sandbox
  from the runner's. Owed: a test of what cowshed's own sandbox sets that the runner does not (its seccomp filter or its
  landlock domain), or the daemon judged against the runner's own baseline.

## Tests that assume the macOS host or a developer's PATH

- `runtime::shell_job::stall_tests` (4; shell_job_stall_tests.rs:88): bind the host program `/usr/bin/false`, which
  NixOS lacks. **Fixed** by a56b880c00 (shed linux-stall-tests): it binds `/bin/sh`, which the fixture's own stand-in
  host already runs.
- `checkout_relocation` (5; tests/checkout_relocation.rs:247, 444, 477, 505): `EnvironmentMissing` "this process has no
  sandbox launcher … named no cowshed-sandbox-exec". On Linux the integration-test binary must name the
  cowshed-sandbox-exec built with it, the way the cowshed binary is its own. The macOS path uses the system
  sandbox-exec, so it never needed one. Owed: the test harness names the built launcher (as other Linux integration
  tests do), not a cfg.
- `workspace_supervisor::{a_shell_less_bun_workspace_runs_its_land_helpers_with_its_bun, a_shed_with_no_node_refuses_its_nx_daemon_with_its_enoent_named_until_one_appears}`
  (workspace_supervisor.rs:9978, 9926): both assume no `node` on the host, and the runner has
  `/run/current-system/sw/bin/node`. The fixture must pin the PATH it hands the supervisor, not inherit the runner's.
- `runtime::build_volumes::tests::directory_adoption_copies_nothing_from_the_target` (build_volumes.rs:8747): needs the
  `sqlite3` shell on PATH and the runner has none. Either devenv provides it to the test, or the test reads the database
  through the crate's own sqlite.
- `cowshed-cli capabilities::mbx::tests::a_cargo_builds_flake_is_written_only_for_nix_and_kept_only_for_a_failed_builds_retry`
  (mbx.rs:1118): the fake installed mbx's script runs `mkdir`, and the PATH the test gives it has none on NixOS ("mkdir:
  command not found", exit 127). The fixture must name its tools by absolute path or carry the PATH.
- `cowshed-helper mountpoint::tests::a_mountpoint_is_permitted_only_in_the_callers_own_tree` (mountpoint.rs:163):
  expects `/` to be owned by uid 0 as the caller sees it. On the runner it is not [INFERENCE: the runner's user
  namespace maps root to the overflow uid]. Read `/`'s owner rather than assume 0.

## macOS-only tests missing their cfg

- `cowshed-cli host::a_host_disk_command_refuses_volume_renames_as_the_verb_does` (tests/host.rs:483): on Linux
  `disk-command` refuses first (exit 104, "runs diskutil or hdiutil, which only macOS has"), as designed. The test is
  `#[cfg(target_os = "macos")]`; a Linux twin asserts that refusal.
- `cowshed-cli launchd::update_failure_cleans_temporary_file_and_preserves_primary_and_cleanup_errors`
  (tests/launchd.rs:733): asserts the LaunchAgents path, and Linux writes the systemd user unit
  (`~/.config/systemd/user`). Either it expects the platform's service directory, or it is macOS-only.

The run's log is on the host as the publish run 45540070896 log (`grep -a '"event":"failed"'`).
