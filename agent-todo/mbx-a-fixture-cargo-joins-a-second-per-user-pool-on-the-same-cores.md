# A fixture's Cargo joins a second, per-user mbx pool on the same cores

Measured on 2026-11-26 on the host.

## What was measured

Two mbx pools schedule the host's 18 cores independently:

- cowshed's shared pool, `/private/var/folders/cowshed/shared/build-cache/mbx/scheduler`, which every job's mbx uses
  through `MBX_CACHE_DIR` (`packages/cowshed/crates/cowshed-core/src/capabilities/mbx.rs:29`);
- mbx's per-user default, `~/Library/Caches/mbx/scheduler`. Its last admissions were at 16:15:02 to 16:15:15, builds
  `FEB45IyGtoli`, `LguaI50mQ08S` and `C1dIW1Bb59Db`, each compiling and linking a crate named `alpha`.

`~/Library/Caches/mbx/incremental/*/checkout.json` names the workspaces of those builds:
`…/smoothbricks-fixtures/nx-plugin/run-58742/native-result-cache…` and
`…/smoothbricks-fixtures/nx-plugin/run-58742/smoo-nextest-partitions-…`, the fixtures of
`packages/nx-plugin/src/native-test-cache.test.ts` and `packages/nx-plugin/src/nextest-partitions.test.ts`.

## Cause

`fixtureNxEnv` (`packages/nx-plugin/src/__tests__/fixture-nx-env.ts:73`) drops every inherited `MBX_*` variable, so a
fixture's Nx does not join the outer job's reservations. The fixture's Cargo is still mbx's shim on PATH, and with no
`MBX_CACHE_DIR` it falls back to the per-user default: its compiles are admitted against a pool that sees 18 free cores
while the shared pool has all 18 leased. The per-user pool also recorded one full build of a downstream workspace on
2026-11-25 at 17:19, so a host shell outside cowshed's environment lands there too.

## What to find

- How a fixture keeps out of the outer job's reservations yet still compiles through the host's one pool: for instance,
  keep `MBX_CACHE_DIR` (or only the scheduler location) while dropping the job's reservation variables.
- Whether mbx outside cowshed's environment on a cowshed host should find the shared pool itself rather than the
  per-user default.

## Done when

A run of both fixture tests and a host-shell Cargo build leave `~/Library/Caches/mbx/scheduler/decisions.jsonl`
untouched, and their compiles appear in the shared pool's log.
