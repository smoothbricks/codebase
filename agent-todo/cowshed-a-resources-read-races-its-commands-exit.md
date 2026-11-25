# A resources read races the command's exit in the activation-accounting test

`packages/cowshed/crates/cowshed-core/tests/workspace_supervisor.rs`, the test
`an_activation_and_its_command_each_count_once` (around line 8230):

```rust
let ran = command.burned();
command.expect("ran");
reap(host.exit());

let both = activation + ran;
let running = accounted_us(&handle.resources(job_id).await.unwrap());
```

On 2026-11-25 it failed once in a broad `nx run-many -t lint typecheck test -p cowshed procinfo` at host load 45 to 60,
with the `unwrap` refusing:

```
CowshedError { code: EnvironmentMissing, message: "the processes of job group 51811 could not be read: process 51811 was
read or ended again after its exit", hint: "the job runs on; read its resources again" }
```

It passed five times out of five when run alone in the same shed. The refusal is the product's own answer for a read
that met the leader after its exit (`process_usage.rs`, "was read or ended again after its exit"), and its hint says to
read again. So the test reads the job's resources at a moment the product defines as unreadable, the window between the
command's exit and the supervisor folding it. Nothing makes the test wait for the end of that window.

To do: wait for the event that closes the window (the job's exit as the supervisor publishes it, or its next published
resource sample) before reading, rather than retrying the read. Red: hold the supervisor's fold of the exit (a stand-in
`ProcessReads` that delays the exit's read), so that today's code reads inside the window every time.
