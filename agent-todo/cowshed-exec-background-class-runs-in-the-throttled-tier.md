# A land's gate backgrounds every token-unaware test runner, so checks starve on a host with idle cores

Found 2026-11-26 while consumer workspaces' test targets kept hitting their no-progress bound. Owner: cowshed.

A test target run in the default `background` class was at scheduler priority 4 (`RN`, nice 10). That is the macOS
background tier (`PRIO_DARWIN_BG`), which also throttles disk I/O. It got about 3% CPU while the host was 38% idle, and
the run hit its 180 s no-progress bound three times. The same run with `--class gate` was at priority 31.

The class does not cause this. Class placement sets one nice and nothing else: `background` is +10
(specs/cowshed/05_gateway.md, "Classes"; 11_shell.md, "Job admission and class placement": "it never applies
`PRIO_DARWIN_BG`"). A background job read outside any land's gate is at priority 31, nice 10.

The priority 4 comes from "A land's gate takes tokens back" (05_gateway.md). While any land's gate runs, a non-gate job
whose CPU over a 3 s window exceeds 1.5 cores per jobserver token it holds is set `PRIO_DARWIN_BG` until the gate ends.
That rule is the owner's ruling of 2026-11-25.

A runner that reads no jobserver holds only the implicit token, so the rule backgrounds any such job using about two
cores. That covers `bun test`, `ttsc`, a Go plugin, and a non-Cargo leaf running on its mbx grant. The rule does not
look at whether the host has idle cores.

When lands run back to back, such jobs spend most of the day backgrounded. They are starved by any foreground work, idle
cores or not, and bounded-exec's no-progress bound then kills them as wedged. Measured 2026-11-26 during two land gates:
about 30 running nice-10 processes (bun test, ttsc, rustc, nix) were at priority 4. The `gate` class is the one class
never withdrawn from, which is why the same shards passed with `--class gate`.

## Decision needed (the owner's)

The rule is a ruling, so changing it is the owner's decision. Options:

- Set `PRIO_DARWIN_BG` only while the host has no idle cores.
- Drop `PRIO_DARWIN_BG` and keep the token drain, which throttles no I/O.
- Keep the rule and stop bounded-exec's no-progress bound from killing a job while it is backgrounded.

`cowshed exec --help` says an ordinary command runs at nice +10. That is true of placement, but the help says nothing of
the gate-span background tier.

## Red

While a land's gate runs on a host with idle cores, a background-class `bun test` leaf reads its priority and
`PRIO_DARWIN_BG` state from its own threads (`PROC_PIDTHREADINFO`). Today it shows priority 4 with `PRIO_DARWIN_BG` set;
after the change it shows what the owner's decision says it should.
