# land source-paths fails when a finished job's member sits in a kernel wait

Found 2026-11-26 at the gate of an SB batch whose commits do not touch cowshed (ttsc 0.38.1 and nx-plugin). Owner:
cowshed.

`cowshed-cli::command_dispatch macos::directory_land_leaves_a_stock_detached_landing_daemon_running_and_skips_adoption_substrate_lifecycle`
failed at `tests/command_dispatch/macos.rs:2974` with `land outputs` answering `Conflict`:

```
cowshed: job 3 membership retirement refused; process and volume claims retained: job 3's processes still run 2s
after SIGKILL, every exit queued for them folded: pid 4700 in an uninterruptible kernel wait, nice 0, cpu 169.6ms,
holding [fd 0 /dev/null, fd 6 /, fd 8 /, fd 10 /, fd 11 /dev/null]
cowshed: land source-paths done elapsed=4.995317708s status=err
```

The host's vnode table was full (8,391,264 of 8,388,608, 1,272 steals/s) and `apfsd` sat in an uninterruptible wait, so
a process leaving a finished job can stay in a kernel wait for seconds. The retirement design already says such a member
stays owned and watched (`runtime/live_membership.rs:4`, `runtime/process_monitor.rs:1349-1386`); what fails is the
land, because `source-paths` turns that retained retirement into its own error after its job's work was done.

## What must hold

- A land step whose job finished its work answers with that work. A member still in a kernel wait after SIGKILL is
  retained and watched as the retirement design says, and does not fail the step that used the job.
- A job that did not finish its work still fails the step.

## Red

A `source-paths` job whose work completes but one of whose members stays in an uninterruptible wait past the retirement
grace: the land step reports success and the member is retained.
