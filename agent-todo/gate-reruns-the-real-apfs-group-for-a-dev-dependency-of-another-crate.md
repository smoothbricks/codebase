# The land gate re-runs cowshed's real-APFS test group for a dev-dependency added to another crate

Observed 2026-11-15, land of `bitmosaic-bitmap` (a4d1308183 on 7c433915d2). The commit changes one crate
(`packages/bitmosaic`): source, tests, and a dev-dependency on a JSON crate the workspace already resolves, which adds
one line to `Cargo.lock` (bitmosaic's own dependency list).

The gate (`nx run-many -t gate`) then ran `@smoothbricks/codebase:cargo-test-group-real-apfs-host`, cowshed's tests on
real APFS volumes, instead of restoring it: the task was 5 min 50 s in at the 12th minute of the gate, serial
(`BOUNDED_EXEC_CPU_WANT=1`), its nextest at 0% CPU, with the host at load 30. The gate passed its usual wall (median 5
min, p90 11 min 37 s). In the workspace before the land, `nx affected -t lint test` had put 36 cowshed cargo-test tasks
in the affected set for the same commit.

`packages/nx-plugin` narrows a crate's `Cargo.lock` input to its own closure (`cargo-closure-input.test.ts:228`), so a
line in another crate's dependency list should not change cowshed's task hashes.

## To establish

- Which input of `cargo-test-group-real-apfs-host` changed between 7c433915d2 and a4d1308183: print both hashes' input
  lists and diff them. Candidates: the lock closure not applied to the grouped test targets; a workspace-level input
  (`Cargo.toml` of the workspace, the nextest binaries metadata); or the task never cached at all.
- Whether the group's time is running or waiting: it was at 0% CPU when sampled.

## Done when

- A commit that adds a dev-dependency to one crate leaves the test tasks of crates outside its closure restored from the
  cache, proven by the two hashes being equal.
