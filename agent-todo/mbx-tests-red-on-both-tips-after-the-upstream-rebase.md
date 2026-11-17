# mbx: 17 tests are red on the fork before and after its rebase onto upstream, and one scheduler test is flaky

Repository: the mbx fork (`github.com/smoothbricks/mr-boxington`). Found 2026-11-17 while the fork's 85 commits were
rebased onto upstream `6caed39` (new main `1056ab0`; the old tip `1f75b2b` is kept as branch `pre-rebase-2026-11-17`).

## What is measured

`cargo test --workspace --no-fail-fast`, the same command on both tips, eight test threads, the `MBX_*` variables a
cowshed hold exports removed and a short temporary directory:

| Tip            | Passed | Red |
| -------------- | ------ | --- |
| old, `1f75b2b` | 2,310  | 18  |
| new, `1056ab0` | 2,388  | 17  |

No test is red on the new tip that is not red on the old. The 17 red on both:

- `tests/build.rs`, 16: `a_build_script_watching_a_bare_repository_keeps_each_checkout_s_commit`,
  `a_build_script_watching_a_directory_of_repositories_keeps_each_checkout_s_commit`,
  `a_build_script_watching_git_metadata_keeps_each_checkout_s_commit`,
  `a_build_script_watching_git_metadata_keeps_each_commit_in_a_shared_target`,
  `a_build_script_with_a_custom_path_restores_across_checkouts`,
  `a_changed_build_script_environment_value_counts_as_an_edit`,
  `a_changed_environment_value_keeps_private_state_without_a_prediction`,
  `a_compilation_removes_the_object_files_its_predecessors_left`,
  `a_crate_with_an_unshareable_native_search_path_keeps_state_across_environment_changes`,
  `a_deleted_source_file_counts_as_an_edit`, `a_restored_build_script_stays_in_every_ci_cache_export`,
  `a_stale_prediction_recovers_instead_of_stranding_the_compilation`,
  `build_script_declared_directory_invalidates_macro_compilations`,
  `build_script_shim_does_not_redirty_its_compilation`, `build_script_without_declared_inputs_uses_the_package_tree`,
  `objects_landing_beside_a_generated_header_still_cross_checkouts`.
- `tests/nextest_scheduling.rs`, 1: `unix::nextest_history_is_per_test_and_cargo_test_keeps_suite_history`.

One scheduler test fails some of the time on both tips with the same text ("timed out waiting for the compile to
queue"): `scheduler::tests::a_waiting_compile_refreshes_its_plan_before_admission`, 15 of 39 runs red on the old tip and
14 of 39 on the new. In the one run that printed more, `pool.admit` answered `None` (`scheduler_tests.rs:4393`), which
`admit` does when `try_admit_state` returns an error and it fails open.

Two things about running the suite at all:

- Under a cowshed hold the suite inherits the hold's `MBX_SCHED_DISABLE`, `MBX_CACHE_DIR` and `MBX_SCHEDULER_TESTS`,
  which its own tests read: 180 and 173 tests go red for that alone.
- Under the cowshed sandbox it cannot run: 154 of 164 `tests/build.rs` cases fail and `mbx_cache_core` waits on its mock
  HTTP server without using CPU.

## What is not established

- Why each of the 17 is red, and which are the host's and which the fork's.
- Which error inside `try_admit_state` makes `admit` fail open in the flaky test.

## What to change

- The suite states the environment it needs and clears what it must not inherit, so it runs the same inside and outside
  a hold.
- Each of the 17 is fixed or named as needing something this host lacks.
- The flaky test waits on the event it means, or the defect it finds is fixed.

## Done when

The fork's suite exits 0 on this host through one documented command, three times in a row.
