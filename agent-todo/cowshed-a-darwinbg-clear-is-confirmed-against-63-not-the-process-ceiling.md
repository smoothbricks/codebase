# A darwinbg clear is confirmed against 63, not against the ceiling the process had

Found 2026-11-27 while fixing procinfo's `thread_max_priorities_reads_back_darwin_bg` on a GitHub macOS runner, whose
job tree reads `pth_maxpriority` 20 before any `PRIO_DARWIN_BG`.

## What XNU does (apple-oss-distributions/xnu `osfmk/kern/task_policy.c`)

- `task->max_priority` is 63 (`MAXPRI_USER`) only with no QoS clamp. A task QoS clamp lowers it to `qos_pri[clamp]`
  (:1272-1276): UTILITY gives 20 (`BASEPRI_UTILITY`). BACKGROUND and MAINTENANCE also set darwinbg, so they give 4
  (:946-950, :1250-1252).
- A child inherits its parent's clamp (`task_policy_create`, :746). launchd starts a job whose `ProcessType` is
  `Standard` or omitted under a UTILITY clamp; `Background` gives 4. Confirmed live on this host's launchd jobs.
- `PRIO_DARWIN_BG` forces 4 whatever the clamp (:1250-1252), and clearing it leaves the clamp, so the process returns to
  the clamp's ceiling, not to 63.

## What cowshed assumes

- `cowshed-core/src/runtime/placement/macos.rs` ≈:132-200: `clear_darwinbg` confirms only when every thread reads
  `DARWIN_NORMAL_MAX_PRIORITY` (63); `has_darwinbg` treats any thread at 4 as backgrounded.
- So under a UTILITY clamp (a supervisor whose launchd job omits `ProcessType`, as on macOS CI, or any clamped child in
  a job) every successful clear is `Unconfirmed`: a `job.darwinbg.unconfirmed` warning per process and
  `ClearedDarwinBg { confirmed: false }` (`job_groups.rs` ≈:88, ≈:616-628). Under a BACKGROUND clamp every process reads
  4 for good: `has_darwinbg` says each is backgrounded and no clear can ever be confirmed.
- The same 63 is written in `specs/cowshed/05_gateway.md` ≈:2151-2153, `cowshed-core/tests/workspace_supervisor.rs`
  (≈:14122, 14196, 14245, 14286, 14336) and `cowshed-gateway/tests/land_gate_priority.rs` ≈:629-634.

## What must hold

A clear is confirmed when every thread is back at the ceiling the process had before the set, recorded when it was set;
a process already at 4 before any set is one cowshed did not background. 05_gateway states that rule and why (a clamped
job's ceiling is not 63).

## Red

A job child spawned under `/usr/sbin/taskpolicy -c utility` (as the fixture's program) is set and then cleared: today
the clear is `Unconfirmed`; it must be `Confirmed` at 20.
