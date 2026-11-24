# A draining supervisor owes a job whose process is gone

Seen 2026-11-25 in a consumer repository, while its test harness swept the scratch projects its killed host tests left.
The workspace supervisor pid 88030, build `b280695b8db6…`
(`__workspace-supervisor <scratch>/project ax-91842f37e6c9c15b55794038e8a5caa7`), had been up for 23 h after the cowshed
daemon moved to build `4270ad63ca9d…`. Its drain report
`/private/cowshed/store/run/db78bf9309038bdb9f7ab0e8d277cd56.drain` read
`{"pid":88030,"waits":{"for":"jobs","running":[14]}}`. No process had pid 88030 as its parent, so job 14 had no process
left to end. The run that requested the job had died (a killed host test). A land step in that project then waited on
`await_other_build_retired` (`cowshed-core/src/runtime/supervisor_socket.rs:3496`) for `OTHER_BUILD_DRAIN_CEILING`,
which is 1 h. That one wait used up the sweep's 30-minute budget.

11_shell.md "Draining a supervisor of another build" says a drain waits for running jobs and for ends that are owed. A
job whose process has exited has ended, and here every party it was owed to had exited too. So the drain should have
read the job as ended and retired, without waiting out a bound. What to find out: how the supervisor's ledger came to
hold job 14 as running when the job has no process (a missed exit watch, or a job whose spawn never reached the ledger's
end record), and why the drain does not reconcile the ledger with the job's process exit before it waits. Related:
`cowshed-drain-waits-on-endless-jobs.md`, which covers jobs that still run and never end on their own.

Red: a draining supervisor whose ledger holds a running job whose process has exited, with its requester gone, retires
without waiting.
