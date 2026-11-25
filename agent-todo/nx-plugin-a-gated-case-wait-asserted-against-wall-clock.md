# A gated case's recorded wait is asserted against a fixed 1,000 ms instead of the bound the leaf applied

`packages/nx-plugin/src/executors/bounded-exec/executor.test.ts`, "records a gated case still waiting when its leaf ends
at the no-progress bound as never started, beside the case that ran" (about line 2371), runs a gated nextest leaf at
`idleTimeoutMs: 1_500` and asserts the waiting case's `waitMs >= 1_000` (about line 2448). On 2026-11-25 at host load
70-90 it read `waitMs` 974 (`held.durationMs` 981, trace `bb7e3871…`): the queued case reaches the gate only once the
held one runs, so how long it waits before the bound ends the leaf is the bound minus that arrival delay, and the delay
grows with load. Nothing was wrong; the assertion measured wall-clock arithmetic.

What the test means is that the case waited out the leaf's bound and never started (05_gateway.md, "A run that ends
early ends its gate first": a case still waiting then never starts and "its result names its whole wait as `waitMs` and
no wall"). Assert that, against the bound the leaf actually applied, read from the run's own record (the verdict's
`idleMs`, or the leaf's span and the case's gate-wait span in the trace), and the case's `durationMs` null; never
against a fixed duration.
