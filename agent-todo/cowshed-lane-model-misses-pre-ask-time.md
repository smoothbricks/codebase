# The lane model that chose the decay bounds cannot see a job's time before its first ask

05 §Classes chose the interactive decay bounds (120 s, 240 s) from a lane model of the pool and its calibration of
2026-11-09 (the "round 5" sweep), which is kept outside every repository. Its jobs ask the pool the moment they start,
so a job's age at its first ask is its admission wait alone.

On 2026-11-25 a focused check's rustc first asked 274 s after its job started (activation, cargo resolution) and entered
at `low`; that case decided that the decay counts from a job's first admitted ask. Replayed with that clock (12 configs
× 24 paired seeds, 120/240 s), the model moved gate span +2.3 ± 1.7 s (+0.1 %) and edit-to-result p95 +0.1 s: it shows
the cost and none of the benefit, because it has no pre-ask phase.

To do:

1. Record each focused job's time from start to first ask from the live host (cowshed's job span start against mbx's
   decision rows, `enqueued` with the job's key) over a window of several days, and add it to the calibration as a
   per-kind distribution.
2. Give the model's jobs that pre-ask phase and re-run the clock comparison (the start clock against first admission) at
   120/240 s, reporting edit-to-result p95, the share within 240 s, and gate span.
3. Move the model and its calibration into `packages/cowshed/crates/cowshed-admission-sim`, so the bounds 05 states are
   re-derivable from the repository.
