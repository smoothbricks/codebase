# An ungated fold never rebases a tree under a live job

On 2026-11-25 `cowshed land supervisor-tee-wedge --into sb-batch-1 --ungated` rebased the shed's branch (0d66a600f8
became a83bfff45d, now on c429a82660 and 4b05ee03f3) while a host-lane job in that shed was 4 h into a broad
`bun nx run-many -t lint typecheck test cargo-test-group-real-apfs-host -p cowshed @smoothbricks/codebase tooling`. The
run ended exit 1 after 318 min with 22 cargo-test tasks failed, all from the moved tree, none from the unit:

- mbx: `result was not stored: compiler input was modified during compilation` for cowshed-core;
- `error[E0425]: cannot find function directories in crate procinfo` at cowshed-os
  `tests/support/scratch_processes.rs:68`, the old procinfo mixed with the new test support, so every cowshed-core and
  cowshed-cli shard failed to build its test binaries;
- Nx `producerReceipts: … Producer tool/module nodes changed between capture and the end of execution` for
  `lmao:rust-dist`, `cli:build` and `nx-plugin:build`.

Four hours of a saturated host were spent on a result that measured nothing, and its reds looked like the unit's.

Owed: `land <ws> --into <batch>` (ungated, and any other path that moves a workspace's tree) never rewrites the tree of
a workspace that has a live job. It waits for the jobs to end or refuses, naming each live job: its id, its class and
its command. A caller may then end them and retry. The supervisor already serves its live jobs. Red: start a job in a
workspace that blocks until released, run `land <ws> --into <batch> --ungated`, and assert that it refuses (or waits)
naming the job while the workspace's HEAD and tree are unchanged; release the job, and the fold proceeds. The run's log
stays with the host's handoff files (`supervisor-tee-wedge/gate.log`).
