# cowshed process monitoring and complete job accounting

The specs state the process-tree monitoring design as fact. These parts of it have no code yet; the spec text that used
to say so has been removed from `07_api.md`, `11_shell.md` and `13_telemetry.md` in favour of this briefing.

## Spec

- `specs/cowshed/07_api.md:844-1013` "Process-tree observations": complete fork/exec tree observation, per-process
  CPU/RSS/I/O usage and blocker samples, the sparse per-process delta stream with heartbeats, CPU-winning leaf identity.
- `specs/cowshed/07_api.md:1014-1100` "Complete job accounting and observation reconciliation": the per-job cgroup v2 on
  Linux, measured Linux fork/exec event-source selection, macOS exit observation and rusage reconciliation,
  charged-memory current/peak, and explicit `job.unattributed` rows.
- `specs/cowshed/11_shell.md:443-486` "Complete process tree, compact observations".
- `specs/cowshed/13_telemetry.md:85-150` "Process-tree spans": one `process.run` span per owned process, on-change and
  heartbeat rows, job-span host/volume columns, cgroup job-total and charged-memory columns, unattributed-usage rows.
- `specs/cowshed/07_api.md:1577`: cancellation covers attachments and log reads; byte-stream iterables for logs and
  attachment output are backpressured.

## What exists

The supervisor owns durable numeric jobs, protected stdout/stderr journals, offset reads, bounded cursor tails, cursor
attach, detach, and complete-group termination through the process-group ownership ledger. Job resource samples,
terminal persistence, keyed admission/lookup and `progress` subscriptions exist in Rust and through N-API. The ledger
identifies groups for sampling and termination; it does not observe the process tree. Job spans
(`crates/cowshed-core/src/runtime/job_spans.rs`) carry the job's start and terminal rows only.

## Work

1. Process-tree observation: the event source chosen per platform by measurement (07_api.md:844-1013), with typed
   coverage gaps instead of silent incompleteness.
2. Per-process usage and blocker samples, and the sparse delta stream with its heartbeat rule.
3. `process.run` spans parented to the job span, with the compact on-change/heartbeat rows of 13_telemetry.md.
4. Linux per-job cgroup v2 accounting and charged-memory counters; macOS exit observation and rusage reconciliation.
5. `job.unattributed` rows whenever attributed process usage and the independent job total disagree.
6. Backpressured byte-stream iterables for logs and attachment output, with abort plumbing.

## Acceptance

- A fork-heavy job (a shell loop spawning short-lived children) yields one `process.run` span per child with exact exit
  state, or a typed coverage-gap row; never a silently partial tree.
- On Linux the job's cgroup total equals the attributed rows plus the `job.unattributed` row.
- A slow consumer of a log iterable pauses the producer instead of growing memory; aborting it ends the read.
