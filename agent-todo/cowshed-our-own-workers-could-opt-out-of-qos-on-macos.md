# Our own workers could opt out of QoS on macOS, so a class's nice orders them

Unbuilt. 05_gateway.md §Classes, "On macOS the order between classes is admission, not OS priority", states why a
class's nice orders no QoS-tagged thread and why no lever the supervisor applies from outside orders CPU without
throttling I/O.

One lever is left, and it is cooperative. A thread that calls
`setiopolicy_np(IOPOL_TYPE_DISK, IOPOL_SCOPE_THREAD, IOPOL_IMPORTANT)` loses its QoS (XNU's `iopolicysys_disk` strips
it), keeps disk tier 0, and from then on honours its process's nice. Measured 2026-11-25 on the class-cpu-order harness
(edb4ffaaa4, case D), as interleaved pairs at load 60 to 110: a background child whose worker threads opted out ran at
priority 21 (31 minus its nice of 10), and the gate got CPU 2.7 : 1 over it (range 2.0 to 3.1, against 1.0 : 1 at the
same nice without the opt-out). Its unlink phase stayed at tier 0, with a median 1.9 times its pair's baseline (5 of 6
pairs kept).

The supervisor can't do this to a child it didn't write. Processes we do write can: mbx's workers and shims, our test
runners, the nx-plugin executors, cowshed's own helpers.

To decide before building:

- which of our processes run long enough beside a gate for the order to matter, and whether each one's worker threads
  can opt out at start (a thread created later keeps its QoS);
- whether the opt-out changes anything else those threads rely on: QoS-driven timer coalescing, efficiency-core
  placement, and the I/O tier when a QoS was raising it;
- how a job's class reaches the process (it already gets its nice; the opt-out would be keyed by the same class), and
  how the effect is verified: by the thread's priority and ceiling from `PROC_PIDTHREADINFO`, never by `getpriority` or
  `getiopolicy_np`, which misreport (05 §Classes);
- the red test: a gate and a background process of ours under contention on macOS, whose CPU order is the class's, read
  from the kernel as priority 31 against 21.
