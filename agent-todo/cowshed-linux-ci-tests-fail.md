# cowshed tests fail on Linux CI, so cowshed cannot publish

Publish run 45046770251 (main f7d8922f3f, `projects=bend-emit,cowshed`) failed in `linux-release-candidate`: 13 cowshed
shards (`cargo-test-cowshed-core-shard2..8`, `-exceptions-shard2/4`, `cargo-test-cowshed-cli-shard2/5/7`,
`cargo-test-cowshed-helper`). The same shards pass on macOS land gates, so these are Linux behaviours of cowshed or of
its tests. Until they pass, the cowshed npm package stays at 0.5.3, while the installed and pinned-elsewhere cowshed is
0.5.4.

Failing tests (each failed twice: run and retry):

- `cowshed-cli capabilities::mbx::tests::a_cargo_builds_flake_is_written_only_for_nix_and_kept_only_for_a_failed_builds_retry`
- `cowshed-cli::host a_host_disk_command_refuses_volume_renames_as_the_verb_does`
- `cowshed-cli::launchd update_failure_cleans_temporary_file_and_preserves_primary_and_cleanup_errors`
- `cowshed-core build_volume::nx::tests::a_host_daemon_whose_record_is_handed_over_serves_on_across_the_move`
- `cowshed-core copy::tests::verification_walk_checks_content_namespace_and_link_targets`
- `cowshed-core git::tests::fork_carry_clean_and_ref_preserve_source_bytes_and_index`
- `cowshed-core git::tests::rename_preserves_linked_source_index_wip_and_branch_across_resumed_handoff`
- `cowshed-core git::tests::unregistering_one_worktree_spares_a_detached_sibling`
- `cowshed-core runtime::build_volumes::tests::directory_adoption_copies_nothing_from_the_target`
- `cowshed-core runtime::shell_job::stall_tests::a_silent_evaluation_while_a_build_runs_is_not_ended`
- `cowshed-core runtime::shell_job::stall_tests::a_silent_evaluation_without_a_build_is_ended_and_its_waiter_evaluates`
- `cowshed-core runtime::shell_job::stall_tests::a_waiter_names_the_holders_pid_age_and_last_line`
- `cowshed-core::checkout_relocation a_live_foreign_main_remote_is_never_retargeted`
- `cowshed-core::checkout_relocation a_recorded_owner_lets_cowshed_retarget_a_remote_whose_path_still_resolves`
- `cowshed-core::checkout_relocation a_relocated_project_has_every_recorded_path_repaired`
- `cowshed-core::checkout_relocation inspecting_merge_drivers_reports_without_repairing`
- `cowshed-core::checkout_relocation repairing_a_repaired_project_changes_nothing`
- `cowshed-core::workspace_supervisor a_shed_with_no_node_refuses_its_nx_daemon_with_its_enoent_named_until_one_appears`
- `cowshed-helper::bin/cowshed-helper mountpoint::tests::a_mountpoint_is_permitted_only_in_the_callers_own_tree`

Read a job log with `gh api --allow-escape-sequences repos/smoothbricks/codebase/actions/jobs/<id>/logs`. Done when a
Publish of `cowshed` passes `linux-release-candidate`. Red first: run each failing test on Linux (a CI job or a Linux
shed) and name its cause; fix the code or the test, never skip it on Linux.
