# A gateway that stops reading holds the gate watch's silence

`packages/cowshed/crates/cowshed-core/src/runtime/supervisor/gate_priority.rs`, `run_watch_loop`. The inner
`tokio::select!` has three arms:

- the local command arm, `cmd = commands.recv()`, which hands each `gate-start`/`gate-end` to the gateway with
  `write_json_line(&mut writer, &event).await` in its handler;
- the gateway frame arm;
- `() = &mut silence`.

A selected arm's handler runs to completion before the select polls again. A gateway that keeps the connection open but
stops reading its socket therefore stalls that write once the socket's send buffer is full (about 8 KiB of event lines),
and while the handler waits, the silence deadline is never polled.

05_gateway.md "A land's gate takes tokens back" says a supervisor that hears no state for the silence bound takes the
state to be off and gives every token back. Silence counts from the gateway's frames, so the deadline must stay live
whatever the supervisor's own writes are doing. The span would instead stay on, with tokens withdrawn and
`PRIO_DARWIN_BG` set, for as long as the stalled gateway holds the socket.

Real gate events are rare (one start and one end per land), so it takes a gateway wedged in exactly that state. Seen in
review of the span-off fix (573576618e), not observed in a run.

To do: race the write against the silence deadline inside the command arm (`tokio::select!` over the write and
`&mut silence`; on silence, end the watch as any other silence does: the connection is dropped, so a partly written line
is never read). Alternatively, move the writes to a writer task fed by a channel, so the loop never awaits a write. Red:
a stand-in gateway in `gate_priority`'s tests that accepts the watch, reads the open line and then never reads again,
fed enough `GateStart` events through `commands` to fill the send buffer. Today the span stays on past
`HEARTBEAT_SILENCE` + `HEARTBEAT`.
