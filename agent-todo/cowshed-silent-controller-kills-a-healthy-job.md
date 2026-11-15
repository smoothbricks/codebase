# cowshed: a busy controller ends a healthy job as SILENT

`packages/cowshed` (the exec client's wait on the controller: the "idle bound 15s" of a controller call; the router that
serves `job.logs`).

## What was seen

Three times on 2026-11-15, on a loaded host, a long `cowshed exec` or a land gate ended with exit 5 while the job it ran
was healthy:

- `cowshed: SILENT: controller call: cowshed controller on a socketpair (pid 14430), router busy: job.logs fold for 54254 ms showed no progress for 17.431356625s (idle bound 15s); waited 410.033114666s in all, last progress: answer`
  during a full test run in a lane base, while another cowshed command forked a workspace from that lane base.
- The same line ended a unit's acceptance run after 303 s.
- A land gate was ended after 1075 s at test 1913 of 6113, all passing:
  `… showed no progress for 17.45860325s (idle bound 15s); waited 1070.387477916s in all … nothing was stopped`, then
  `land-host: the check exited 5 after 1075s; the land stops`. Eighteen minutes of gate were thrown away and the land
  had to be run again.

The line says "nothing was stopped", and the job's exit status reached its caller as 5, so the caller stopped.

## What should hold

- A controller that is alive and busy is not a failed job. The client keeps waiting, or reattaches to the job's output
  and status, and the job's own exit status is what the caller receives.
- The bound that names a silent controller is about the controller, so it is reported as such and never becomes the exit
  status of a command that is still running or that passed.
- One job's log stream being served (`job.logs` for 54 s) does not starve another call on the same controller past its
  bound.

## Done when

A test holds a controller busy past the idle bound while a job runs to a passing end, and the caller receives the job's
status.
