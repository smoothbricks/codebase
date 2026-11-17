# cowshed's cargo suites are refused their temp roots inside `cowshed exec`

Found 2026-11-17 while the checks of `9267c0efdf` (a call its caller stops waiting on frees its slot) ran in a shed.

## What was seen

Run sandboxed through `cowshed exec <shed> -- …` (no `--host`), cowshed's ordinary cargo suites fail as their fixtures
are built (reported by the unit's agent; the failing runs' output was not kept):

- `create owned test root under /private/tmp: Operation not permitted`, raised by `TempRoot::new_in`
  (`packages/cowshed/crates/cowshed-core/tests/support/temp_root.rs:41`);
- the same EPERM at `capabilities::test_support::Fixture::new`, which creates `/tmp/cs-cap-<uuid>` itself
  (`packages/cowshed/crates/cowshed-core/src/capabilities/mod.rs:1109`).

The same suites pass on the host lane (`cowshed exec <shed> --host --want 8 -- …`): `cowshed:cargo-test`,
`cowshed:host-napi-test` and `cargo-test-group-real-apfs-host` were granted that lane for the unit and exited 0.

## Why it matters

A unit that changes cowshed can prove itself only on the host lane, one invocation at a time under the host lock, and
its owner learns that from a red run. A consumer workspace's agents run their checks sandboxed by default; the same
default here costs a failed run per agent.

## What the code says

`temp_root.rs` selects the canonical `/tmp` parent on purpose: "Independent sandbox fixtures explicitly select the
canonical `/tmp` parent because their profiles cannot grant protected store paths inherited through TMPDIR." The
`cowshed exec` profile of a shed does not grant writes there. `Fixture::new` in `capabilities/mod.rs` does not go
through `TempRoot` and names its root `cs-cap-<uuid>` without a pid, so the shared-temp sweep's claim rule (`cowshed-os`
`scratch::named_owner`) does not cover a root a killed run leaves.

## Not established

- Which rule of the exec profile refuses the write, and whether a shed's profile is meant to grant a private scratch
  parent that these fixtures could select.
- Which of the three targets are host-only by design (the real-APFS group is) and which should pass sandboxed.

## Done when

- Each cowshed cargo target either passes under plain `cowshed exec` or states that it is host-only where an agent reads
  it before running it: the target refuses to start sandboxed, by name, with the host-lane command.
- `capabilities::test_support::Fixture` takes its root from `TempRoot`, so its name is claimable and its parent is the
  one the other fixtures use.
- A test fails when a fixture creates a root outside the selected parent.
