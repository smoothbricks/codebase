# The test trace sink loses a killed run and never prunes

`@smoothbricks/lmao/testing` writes each test run to `<package>/.cache/lmao/trace-results.db`
(`packages/lmao/src/lib/testing/bun-harness.ts`, `vitest-harness.ts`). Two gaps, both measured on 2026-11-14 in a
consumer's largest package.

## 1. A killed run leaves no row

The sink is flushed in the global `afterAll` (`bun-harness.ts:623-626`, `:345-347`). A test process that spins or hangs
and is killed by a runner's idle bound never reaches it, so the run that most needs reading is absent: the database held
the runs before and after the killed one and nothing of it, and the runner could only say "named no test still running".

Design: flush as tests finish, so a killed process leaves its completed tests and the start row of the test that was
running (a span with row 0 and no row 1 names the hang). Bounded cost: one transaction per test file or per N completed
tests, never per row. A span-start row for a test must be durable before its body runs for the hang case; measure that
write against the suite's wall before choosing per-test or batched starts.

Red: a test file whose second test never settles, run under a kill after a bound; reopening the database finds the first
test's completion and the second test's open start row.

## 2. The database grows without bound

Nothing deletes a run: no `DELETE`, no retention option, no `VACUUM` in `testing/` or `sqlite/`. The measured file was
9.95 GB with 34,020,140 rows. A reader has to bound every query by `rowid` to get an answer in seconds, and every shed
that copies the package's state carries it.

Design: a retention bound owned by the harness, applied when a run opens the sink: keep the latest N runs per package
(default small, for example 50) or the runs of the last D days, delete the rest in one transaction, and reclaim space
incrementally (`auto_vacuum = incremental`), never a full `VACUUM` in a test run. The bound is an option of
`initTraceTestRun`'s `sqlite` settings with a stated default.

Red: after N + 1 runs the first run's rows are gone and the file is no larger than after N.
