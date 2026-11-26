# A gate-priority test bounds a job's gate-start by the gateway silence bound

`packages/cowshed/crates/cowshed-core/tests/workspace_supervisor.rs`, module `land_gate_priority`. `recv_event`
(line 12862) waits for the stand-in gateway's next event through `within_silence` (line 12657), which is
`deadline::HEARTBEAT_SILENCE`, 15 s. `gateway_silence_restores_tokens_and_clears_darwinbg_within_silence_bound`
(line 14352) starts a jobserver client, a CPU burner of 3 and a land `GateJob`. It then waits for `land gate-start`
(line 14379) under that bound.

The spec's silence bound (05_gateway.md, "A land's gate takes tokens back") is the longest the supervisor tolerates
without a gateway frame. It is not a bound on how soon the supervisor starts a job and sends `gate-start`. That path
spawns the job and writes the line through `run_watch_loop` (`src/runtime/supervisor/gate_priority.rs:449`), and it
takes as long as the host takes to spawn. The same test uses the test backstop `cowshed_os::test_backstop::NO_PROGRESS`
for its other wait that has no product bound ("darwinbg cleared after gateway silence", line 14423).

On 2026-11-26 the gate of land `bun-devenv` (`/private/tmp/land-bun-devenv.out`, shard3, around 17:31-17:38) failed it
at 50.67 s: `land gate-start did not happen within the silence bound (15s)`. The `supervisor.groups` ENOENT lines that
follow come from the fixture's teardown after the panic, not from the cause. In other gates that week it passed in 15.3
to 35.6 s, and at host load spawning tests in the same gate ran 10-600× slow. Run three times alone in the shed through
`cowshed exec --host`, it passed in 15.84 s, 15.30 s and 15.69 s. No commit of that land touched `packages/cowshed`.

The same `recv_event` bounds every other `gate-start` and `gate-end` wait in the module: lines 13410, 13437, 13552,
13601, 14088, 14126, 14484, 14583, 14629, 14643, 14849 and 14947.

To do: bound the waits for supervisor-sent events by `test_backstop::NO_PROGRESS`, the backstop a wait with no product
bound uses. Keep `HEARTBEAT_SILENCE` only where the test measures the silence rule itself (`bg.wait_tokens_backstop`
after `go_silent`). Red: a fixture whose job spawn is held past 15 s (for example a stand-in that delays its first
`Started`) fails `land gate-start` today, though the product breaks no bound.
