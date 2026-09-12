# Exact-interest phase diagnostics

```sh
STATEBUS_BENCH_ORDER=baseline-first nx run statebus-core:bench-exact-interest
STATEBUS_BENCH_ORDER=candidate-first nx run statebus-core:bench-exact-interest
STATEBUS_BENCH_ORDER=baseline-first nx run statebus-core:bench-exact-interest --configuration=v8
STATEBUS_BENCH_ORDER=candidate-first nx run statebus-core:bench-exact-interest --configuration=v8
```

Set `STATEBUS_BENCH_COMMIT` to the tested commit. The script records source hashes and environment metadata regardless.
The reference reproduces the prefix-merge/JSON-address algorithm at PR23 head `21215c95`. Prepared inputs are outside
timing. Owned publication output is inside the owned-wave phase. Each variant must produce an identical checksum and
must leave previously retained/frozen output unchanged. Both orderings are retained independently, not averaged away.

Mitata reports individual samples with p50/p95/p99/p99.9 for 1/32/256/1000-address owned waves, warmed scratch updates,
cold setup/growth, warmed exact-count lookup and unchanged view-prop comparison. The setup phase includes output
validation and is not a zero-allocation phase. The warmed scratch phase excludes publication; the owned-wave phase
includes it. High-cardinality waves are processed fully and then clear the bounded address cache rather than retaining
an unlimited interner. Raw JSON is written under `.cache/benchmarks/exact-interest-<engine>-<order>.json`.
The package-contract CI job executes all four engine/order combinations through these same Nx targets and retains the
raw samples with its tarball/consumer evidence. Semantic or retained-output mismatches fail the job. Latency samples
are diagnostic: a shared CI runner is not a dedicated-hardware percentile/noise-regression gate.

These are reproducible diagnostics, not a whole-runtime allocation or browser frame-time certificate. Process memory
snapshots are not per-operation allocation counts, effective-memory peak/retained bounds or GC pause measurements.
Allocation, IC/hidden-class, branch and GC instrumentation unavailable in this runner is explicitly marked unavailable.
The leaf functions declare no tracing Op; separate plugin-on/off instrumentation runs are not represented as measured.
Sub-microsecond results near the timer floor and noisy tail differences must not be advertised as reliable speedups.
The broader LMAO measurement contract retains its separate instrumentation and dedicated-hardware requirements.
