# A cargo test group re-runs when a crate it does not depend on changes its manifest

Measured in a land gate on 2026-11-14: `@smoothbricks/codebase:cargo-test-group-real-apfs-host` (cowshed-core's
real-APFS tests, minutes on a loaded host) ran again although no cowshed file had changed. The only change in its inputs
was a `[[bench]]` entry added to `packages/columine/crates/columine-vm/Cargo.toml`.

The task's inputs name the manifest of every workspace member (`packages/bitmosaic/…/Cargo.toml`,
`packages/columine/crates/*/Cargo.toml`, `packages/lmao/crates/*/Cargo.toml`, …) beside the sources of its own
dependency closure. A member the tested packages do not depend on reaches the task in one way only: cargo loads the
whole workspace, so a manifest that does not parse fails every cargo task. Its targets, benches, features and
dependencies do not change what `-p <package>` builds under resolver 2.

Change: a cargo test group is keyed on the manifests of the packages in its own dependency closure and on its
`cargo-lock` entries, not on the manifests of the other members. Keep one cheap workspace-load check that every cargo
task depends on, so a broken foreign manifest still fails fast.

Never narrow past a real read: prove it with Nx's own hash. Adding a bench target to columine-vm leaves the hash of
`cargo-test-group-real-apfs-host` unchanged; a change to `cowshed-core`'s manifest or to a dependency of it changes it;
a syntax error in a foreign manifest fails the workspace-load check.
