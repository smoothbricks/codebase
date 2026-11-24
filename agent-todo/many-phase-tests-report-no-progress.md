# Single tests with many heavy phases report no progress under load

A test leaf's no-progress bound is per test: "the longest any single test may run" (180 s,
`packages/nx-plugin/src/test-backstop.ts`; specs/cowshed/08_testing.md, the hang backstop). A test that runs many
independent heavy phases inside one `it` (or one Rust `#[test]`) reports progress only when the whole of it ends, so
under load it passes the bound with nothing hung: on 2026-11-25 sb-batch-1's gate 4 wedged `nx-plugin:test-shard2` and
`test-shard4` this way (a 20-phase ensure-built daemon test, 129-170 s alone at load 68-73; process-group's churn test,
102 s alone at load 75). Both are split now, one `it` per phase over a shared fixture, as are the receipts test and
go-progress-host. The remedy is the same for each item below: one `it` per independent phase, a shared fixture through
`describe` with `beforeAll`/`afterAll` where phases share state, every assertion kept; never a raised bound.

Walls are tonight's trace DB or `packages/nx-plugin/test-timings.json` (file totals) at host load 60-110.

## nx-plugin

- `src/nx-daemon-validity.test.ts:847`: 5 lockfiles × 6 checks, 20 daemon restarts; 32-41 s per lockfile.
- `src/nx-daemon-validity.test.ts:921`: 5 daemon-stop scenarios; the ignored-lockfile phase alone 32 s.
- `src/nx-daemon-validity.test.ts:1100`, `:1184`, `:1276`, `:1417`, `:1538`: 3-6 daemon or `run-many` phases each; the
  file 53 s.
- `src/executors/bounded-exec/ram-temp-host.test.ts:832`: 7 real APFS RAM-volume phases; up to 51 s.
- `src/cargo-source-hash.test.ts:336`: 15 CLI spawns; 20 s.
- `src/cargo-resolve-cache.test.ts:460` (about 40 resolution phases) and `:908` (about 18 graph and hash phases); the
  file 24 s.
- `src/cargo-retention.test.ts:44`: 7 real `nx run` and Cargo phases; the file 10 s.
- `src/nx-daemon-start-never.test.ts:269`: 7 daemon and client phases; the file 5 s.

## cowshed (Rust; no per-test walls recorded yet, so record them first)

- `crates/cowshed-core/tests/apfs_native_macos/macos.rs:5195` (6 APFS resize, defrag, unmount-refusal and rewrite
  phases) and `:4405` (4 mount, opt-out and retire cycles).
- `crates/cowshed-cli/tests/command_dispatch/macos.rs:973` (6 adopt, fork, exec, push, land and rm phases), `:5798` (5
  adopt, seed, land, fork and doctor phases) and `:6745` (5 build-volume ownership and reclaim phases).
- `crates/cowshed-apfs-tests/src/tests.rs:481`: 6 image-lease phases.
- `crates/cowshed-core/tests/devenv_runtime_dir.rs:2092`: 4 nested Go, `cargo fetch` and `bun install` phases.
- `crates/cowshed-core/tests/clone_intent_recovery/macos.rs:440`: 5 land-train phases.
- `crates/cowshed-helper/tests/real_zfs.rs:229`: 6 ZFS phases.
- `crates/cowshed-core/src/storage/apfs/native/build_volumes.rs:1969`: 4 build-volume phases.
