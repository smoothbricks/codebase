# A silence test names its hung process only if the shell has already exec'd

`packages/cowshed/crates/cowshed-os/src/deadline.rs`, the test
`deadline::tests::a_run_to_log_that_goes_silent_fails_naming_its_hung_process_and_is_ended`.

## What is measured

The test runs `/bin/sh -c "echo started; sleep 600"` under `run_to_log` with an idle bound of 50 ms and asserts that the
silent run's group holds a member whose command is exactly `sleep 600`.

On 2026-11-16 it failed a land gate, once, while the host's one-minute load was above 40 on 18 cores. It panicked at
`deadline.rs:2335` with its own message:

```
the hung process is named: SILENT: run to log: probe command pid 10475 (sleeping) showed no progress for 51.989834ms
(idle bound 50ms); waited 126.884334ms in all, last progress: received 8 bytes; no CPU advanced among its members; its
running members: pid 10479 `/bin/sh -c echo started; sleep 600` (running); pid 10475 `/bin/sh -c echo started; sleep
600` (sleeping);
```

Both members still carry the shell's command line: the shell had forked and the child had not yet exec'd `sleep` when
the bound ran out and the group was read. About twenty minutes later, on the same commits and a quiet host, the gate
passed. The other 108 tests of the group passed in the failing run.

So the report matched what the process table held at that moment. The test asks for more than a 50 ms bound guarantees:
that fork and exec both finish inside it. Whether the report should name a child that has not exec'd in some other way
was not looked at.

## What to change

The test states what it means without depending on the scheduler:

- it asserts the hung process is named by what the group holds at the moment of the report (the shell's line names a
  child that has not exec'd), or
- it starts a process that needs no exec to be the hung one, so the member's command is fixed from the first instant.

The 50 ms bound can stay: it is what keeps the test fast.

## Done when

- The test passes repeatedly while the host runs a build on every core; the count and the load are stated.
- Its assertion still fails if the report names no member of the group.
