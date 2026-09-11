# Interest publication benchmark

This is a focused Mitata comparison against the subscriber-merge loop at
`8331a29cf68bb1bcedb2e28772e464279f347bbb`. It measures **owned interest flush**, not application rendering or a
zero-allocation state update. Payloads and accumulator are prepared before timing; output-record allocation stays
inside timing. Full checksums and retained-output equality are checked before and after measurement.

Run both positions, rather than interpreting one favorable ordering:

```sh
nx run statebus-core:bench
STATEBUS_BENCH_ORDER=candidate-first nx run statebus-core:bench
nx run statebus-core:bench:v8
STATEBUS_BENCH_ORDER=candidate-first nx run statebus-core:bench:v8
```

Bun is the default runner. The `v8` configuration uses Node with exposed GC because Bun's JavaScriptCore measurements
cannot prove V8-specific behavior. Each run preserves schema-versioned raw individual samples, environment versions,
source hash, checksum, order, and p50/p95/p99/p99.9 in `.cache/benchmarks/`. Set `STATEBUS_BENCH_COMMIT` when recording a
committed build. Sample counts are reported after Mitata's own trimming. No inner batching hides long individual waves.

## September 11 diagnostic results

Linux x86-64 sandbox, Bun 1.4.2 and Node 26.8.2. These are not dedicated-hardware or browser frame-time measurements.
The unchanged single-payload case is near the measurement floor and shows ordering noise; no speedup is claimed there.

| Engine / order | Payloads | Baseline p50 | Candidate p50 | Baseline p99 | Candidate p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Node / baseline first | 32 | 7.71 us | 0.94 us | 26.48 us | 1.87 us |
| Node / candidate first | 32 | 7.62 us | 0.93 us | 40.90 us | 3.21 us |
| Node / baseline first | 256 | 1,989.17 us | 24.54 us | 3,575.28 us | 47.88 us |
| Node / candidate first | 256 | 2,001.79 us | 24.27 us | 2,982.11 us | 134.07 us |
| Bun / baseline first | 256 | 751.94 us | 8.97 us | 1,320.49 us | 35.42 us |
| Bun / candidate first | 256 | 718.42 us | 9.49 us | 1,256.58 us | 37.12 us |

This removes repeated accumulated-prefix copying: a multi-event wave creates one subscriber record and one final event
envelope, rather than one of each per merge. Already-published records are not mutated or pooled. State and event
objects supplied by callers, subscriber-map backing storage, and scheduler/runtime allocations are not erased by this
change.

**Unavailable evidence is not zero:** per-operation allocation bytes, GC pause traces, IC/hidden-class stability,
branch counters, and per-request effective-memory peaks have not been certified by this microbenchmark. The reported
process memory totals include the harness and are not an allocation bound. Startup, subscription setup, queue growth,
React commits, LMAO/Arrow flush and end-to-end browser interaction require their own workloads. This benchmark does not
claim the broader StateBus performance/readiness gate is complete.
