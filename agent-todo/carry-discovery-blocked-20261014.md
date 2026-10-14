# Carry discovery: blocked handoff (2026-10-14)

**Blocked, uncommitted, not landable.** No compiled-Rust verdict, real carry completion, or installed-build receipt.

## Unit and evidence

Base: `c0e3d162fb20d5835ae90bbd29844d24e2a26b01`. Changed paths:

- `packages/cowshed/crates/cowshed-core/src/capabilities/build_state_discovery.rs`: four concurrent Cargo queries per
  activated-shell batch, private result files, manifest-ordered answers, and concurrency regression coverage.
- `specs/cowshed/16_build_volumes.md`: document the bound and existing manifest-fingerprint cache. No new
  service/schema; carry holds, journal identity, and failure fences remain unchanged.

Recovery patch: `local://astra-carry-discovery-source.patch`. Do not reapply it over the edited shed.

- **85 manifests: 6.433s serial → 2.619s changed driver**, same activated environment, byte-identical answers.
- Nine-query driver regression **passed**: concurrent execution, reordered completion, failure reporting, and partial
  final batch. This executed the source driver, not the compiled Rust test.
- Real `rustfmt --edition 2024` and repository Prettier **passed**. Native edits followed two icm formatter refusals
  caused by missing child `HOME`; no icm publication or hook bypass is claimed.

Evidence files: `local://astra-carry-driver-proof.json`, `local://astra-carry-discovery-steady.json`,
`local://astra-carry-metadata-proof.json`, and `local://astra-carry-activation-before.json`. icm refusals:
`artifact://290335`, `artifact://290388`.

Parallel discovery alone is not proved to fix the original 300s carry timeout. No cross-checkout shell-export cache was
added; the current cache-isolation contract forbids replaying another checkout's executable environment.

## Blocker and custody

Fork publication timed out during build-image attachment. The mounted clone still has its inherited main build link, as
expected before `finish_fork`; the normal commit hook then failed with `ENOENT` under `.cache/ttsc/plugins`. No commit
was produced and no Cargo build ran against that inherited volume. Doctor record: `artifact://289969`.

Retain both pending roots, neither contained nor safe to remove:

- `<source-shed>`: mounted source clone holding the two edited paths.
- `<carry-probe-shed>` in the consumer project: scratch fork request; no completed workspace reported.

Exact root identities and custody: `local://carry-discovery-custody.json`.

## Resume only after the lifecycle hold is released

1. Resume existing journals with `cowshed --project <sb-main> --json path <source-shed>` and
   `cowshed --project <consumer-main> --json path <carry-probe-shed>`. If no recoverable intent exists, report it; do
   not create another root.
2. Confirm publication, branch `cowshed/<source-shed>`, and its **own** build-volume link. In that shed's activated
   environment, run `cargo test --frozen -j 4 -p cowshed-core --lib capabilities::build_state_discovery::tests` and
   `cargo build --frozen -j 4 -p cowshed-cli --bin cowshed`.
3. Commit only the two named paths through the normal hook: `perf(cowshed): bound Cargo discovery concurrency`.
4. Run the changed CLI's `--project <consumer-main> --json rebase <carry-probe-shed>` within the existing 300s deadline.
   Retain executable identity, discovery/carry timings, and the complete result. No live queue shed or raised bound.
5. Hand the frozen commit and receipts to the designated SmoothBricks lander for the normal land gate and
   installed-build receipt. Consumers remain parked until that receipt. The lander retires owned roots only after
   containment; never detach.
