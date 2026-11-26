# cowshed exec's background class runs in the macOS background tier, not at nice +10

Found 2026-11-26 while a consumer workspace's test target kept hitting its no-progress bound. Owner: cowshed.

`cowshed exec --help` says background jobs run at nice +10. A test target run in the default background class was at
scheduler priority 4 (`RN`, nice 10): the macOS background tier (darwinbg), which also throttles disk I/O. It got about
3% CPU while the host was 38% idle, and the run hit its 180 s no-progress bound three times. The same run with
`--class gate` was at priority 31.

A process in the background tier is starved by any foreground work, idle cores or not, so an agent's check run in the
default class can make no progress for minutes on a busy host and be killed as wedged.

## What must hold

- The background class does what its help says: a lower CPU priority than gate and interactive work, without the
  background tier's I/O throttling and starvation, so it still progresses when the host has idle cores.
- If the background tier is wanted for some jobs, that is its own class, named and documented as such.

## Red

A background-class job on a host with idle cores and foreground load runs at a priority above the background tier and
gets CPU in proportion to the idle cores.
