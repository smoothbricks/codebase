# cowshed host scheduling: the kernel schedules, cowshed admits only what the kernel cannot take back

Status: the first scope is landed in the code and the specs. The host CPU ledger is deleted, a job spawns at once at its
class's priority, every job serves its own jobserver, Linux jobs run under class cgroups, and a background start waits
while the host reports memory pressure. What is not built needs the busy-host harness or the memory estimates; "Not
built" and "Open" name each item and what decides it.

Approved by the owner, 2026-10-26: "Need this designed for cowshed where the OS (mac, Linux) does actual resource
scheduling." The specs state the design now, and this brief keeps only what they do not. The research and the incident
table are in `agent-todo/cowshed-os-scheduling-research.md`. Paths are relative to the repository root.

The measuring rule is the owner's ruling of the same day: no gate ever needs a quiet host (the one legitimate quiet host
is a benchmark that needs absolute precision), and a real optimization must be a clear winner on a busy host, because
one that only wins on an idle host fails in reality. Every unit not built ends with the same measurement, run on the
18-core arm64 host while it is in normal use, against the busy-host harness of U0, and lands only if its row of the
harness table is not worse than the previous unit's and its own acceptance holds.

## Landed

Each item names the spec text that states it, which this brief does not repeat, and the code that implements it.

- **Classes and bands (U1).** `specs/cowshed/05_gateway.md` "Classes"; `specs/cowshed/11_shell.md` "Job admission and
  class placement".
  - One class-to-nice table serves both platforms: `class_nice` in
    `packages/cowshed/crates/cowshed-core/src/runtime/placement/mod.rs`.
  - macOS: the supervisor places `setpriority` on the held leader and reads it back
    (`packages/cowshed/crates/cowshed-core/src/runtime/placement/macos.rs`). The child no longer reports its own nice,
    and no crate sets a QoS clamp, `PRIO_DARWIN_BG` or a disk I/O policy.
  - Linux: per-process nice is retired. `packages/cowshed/crates/cowshed-core/src/runtime/placement/linux.rs` writes
    each class cgroup, and `packages/cowshed/crates/cowshed-core/src/runtime/job_cgroup.rs` puts every job's cgroup
    under its class's, directly under the delegated root, with the `cpu`, `memory`, `io` and `pids` controllers enabled;
    a missing controller is a typed error.
  - There are three class cgroups, one per nice of the table: `resident` runs in `background` and `interactive-resident`
    in `interactive`. `io.weight` stays out of scope, since it needs the root-only `io.cost.qos`.
- **Memory pressure (U5, first scope).** `specs/cowshed/05_gateway.md` "Memory pressure".
  - `packages/cowshed/crates/cowshed-core/src/host_pressure/` holds the pure decisions (`waits_for_memory`, `may_start`,
    `job_to_end`) and their limits in its module root, and one reader per platform beside it: PSI and `meminfo` on
    Linux, `kern.memorystatus_vm_pressure_level` and `vm.swapusage` on macOS.
  - A background start waits in `wait_for_memory` (`packages/cowshed/crates/cowshed-core/src/runtime/job_cpu.rs`). At
    Critical the macOS supervisor ends the youngest background or resident job in `end_for_memory`
    (`packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs`). Linux has no such loop: the background class's
    `memory.high`, `memory.max` and `memory.swap.max` bound it in the kernel.
- **Per-job jobserver (U3).** `specs/cowshed/05_gateway.md` "Parallelism inside a job".
  `packages/cowshed/crates/cowshed-core/src/runtime/job_cpu.rs`: the supervisor serves each job a fifo of
  `min(want, cores)`, a nested `cowshed exec` from inside a job of the same workspace reuses its fifo, and the gateway
  serves no pool.
- **The ledger is deleted (U4).** The gateway's CPU budget and jobserver modules with their tests, the gateway-types CPU
  budget module, the control operations that served them, the probation lease state, the supervisor's side of the ledger
  (the job CPU admission trait, its gateway implementation and a spare shell's activation stake) and the install naming
  on the listener-handoff socket are gone; `rg` finds none of them.
- **Status.** `specs/cowshed/05_gateway.md` "Status"; `specs/cowshed/07_api.md` `GatewayStatus`. The CLI reads the
  kernel's numbers itself: `packages/cowshed/crates/cowshed-gateway-types/src/host_status.rs` (`HostReadings`),
  `packages/cowshed/crates/cowshed-core/src/host_readings.rs`, and `read_scheduling` in
  `packages/cowshed/crates/cowshed-cli/src/gateway_service.rs`.
- **Task server.** `specs/cowshed/05_gateway.md` "The task server schedules work units".
  `packages/cowshed/crates/cowshed-gateway/src/task_server.rs` has no CPU fit, and a background unit is held while
  memory pressure holds a background start (`memoryWait` in `task-queue`).
- **Repository ordering.** The gateway keeps no one-gate-per-repository rule, which the design had kept: a gate-class
  job is a nice, not a lock, and the land turn alone serializes a repository's lands (`specs/cowshed/05_gateway.md`
  "Gates and lands", "Land turns").
- **Specs and docs (U7).** `specs/cowshed/05_gateway.md` "Host scheduling" replaces the CPU budget. The other text that
  stated the token budget is under "Consumers the design missed".

## Consumers the design missed

The design did not name these. Each is handled, as `rg` in the shed shows.

- The restart guard: `refuse_restart_while_busy` in `packages/cowshed/crates/cowshed-cli/src/gateway_service.rs` and its
  decision `restart_blockers` in `packages/cowshed/crates/cowshed-cli/src/agent_cycle.rs` read the land turns alone
  (`specs/cowshed/05_gateway.md` "An install refuses while a land runs"). The status lines that read the ledger give way
  to `scheduling_lines`, over `read_scheduling`.
- `tooling/land-install.ts`: `installReadiness` reads the land turns alone, and `tooling/land-install.sh` and
  `tooling/land-host.sh` say the same.
- The generated readers and bindings are regenerated (`cowshed:api-generate`, `cowshed:wire-fixtures`), and no
  CPU-budget type remains in `packages/cowshed/crates/cowshed-core/src/api/reader.generated.rs`,
  `packages/cowshed/src/api.generated.ts`, `packages/cowshed/src/reader.generated.ts`,
  `packages/cowshed/src/validators.generated.ts`, `packages/cowshed/src/wire-contract.test.ts` or
  `packages/cowshed/src/wire-fixtures.json`.
- `bounded-exec` asks no gateway. It takes its tokens from the pool it inherits
  (`packages/nx-plugin/src/executors/bounded-exec/cpu-pool.ts`) and sizes its runner in
  `packages/nx-plugin/src/executors/bounded-exec/runner-sizing.ts`; the old CPU-token module is deleted. Every Nx run
  serves its own pool of `min(inherited count, cores)` tokens
  (`packages/nx-plugin/src/executors/bounded-exec/run-cpu-pool.ts`; `specs/cowshed/05_gateway.md` "Every Nx run budgets
  itself"). The supervisor exports a job's count as `BOUNDED_EXEC_CPU_TOKENS` and its fifo, never a pool endpoint.
- Other text that stated the token budget, or cited the sections it lived in, now states the landed design: `AGENTS.md`
  ("cowshed exec"), `packages/cowshed/docs/README.md`, `packages/cowshed/docs/cli.md`,
  `packages/cowshed/docs/gateway.md`, `packages/cowshed/docs/troubleshooting.md`,
  `packages/cowshed/skills/cowshed/SKILL.md`, `packages/nx-plugin/README.md`, and the specs `specs/cowshed/01a_apfs.md`,
  `specs/cowshed/01c_macos.md`, `specs/cowshed/02_workspaces.md`, `specs/cowshed/03_caches.md`,
  `specs/cowshed/04_sandbox.md`, `specs/cowshed/06_cli.md`, `specs/cowshed/07_api.md`, `specs/cowshed/08_testing.md`,
  `specs/cowshed/11_shell.md` and `specs/cowshed/16_build_volumes.md`. The reproductions in
  `packages/cowshed/docs/optimization-ledger.md` no longer run under `taskpolicy -b`; the CPU-budget item of
  `agent-todo/cowshed-small-spec-gaps.md` is deleted, and the item of `agent-todo/Cowshed.md` that cited the budget is
  restated against the pool.
- Code comments that cited spec sections the landing removed cite the sections under "Host scheduling".

## Not built

The owner's rulings of 2026-10-26 set the first scope and left the measurement units out of it. Order: U0 first, since
every acceptance that needs a number needs it; U5's rest and U6 are independent of each other and follow it; the ledger
rows close.

- **U0, the busy-host harness.** A tooling script, not product code. It starts `B` background `cargo nextest run` jobs
  from `B` sheds of one repository (B = 3 and 6), one `interactive` probe loop (`cowshed exec true` every 2 s, recording
  latency) and, after 60 s, one land gate (`nx run-many -t gate`). It records the gate's wall time, background
  throughput (tests completed per minute, summed), interactive p50 and p95, host load, PSI or the macOS pressure level,
  swap in use and per-class CPU time. Rows: an empty host (B = 0) and, for what the ledger gave, a build of
  `d2dc2aaef9`, which still holds the ledger. Acceptance: it reproduces f5cff758e8's shape, gate wall at least 2× the B
  = 0 run on that build at B = 6; a harness that cannot show the problem cannot show the fix.
- **What the harness must show,** clauses the first scope does not claim, each against that build:
  - Linux, B = 6: the gate's wall time against B = 0 with background throughput above 0 throughout, and swap at 0 under
    `memory.swap.max`. An earlier bound of 1.1× was set for weights of 100 : 10; the owner's bands give interactive,
    gate and background about 9 : 3 : 1 under contention, so the bound is restated from the first run.
  - macOS, B = 6: gate wall at most 1.25× B = 0 (a threshold to restate once the harness exists), interactive p95 at
    most 2× B = 0, no core idle for more than 5 s while any background job has runnable work (sampled from load or
    `cpu.stat`), and the f5cff758e8 reproduction (an idle 17-thread nextest beside a gate) with the gate within 10 % of
    B = 0.
  - Memory: the 3607bc754e reproduction (six acceptance runs of one repository started at once) ends with swap in use at
    most 1 GB (the incident reached 5.9 of 7 GB) and every run completing or ending with a stated `memory` failure; a
    gate started during it meets the macOS bound.
- **U2, the macOS band measurement.** Not needed: the owner fixed the bands, so no choice remains for a measurement to
  make.
- **U5, the rest: memory estimates.** An exec declares `memory: <bytes>`, defaulting per class from the job's own
  history in its artifact (`memory.peak` on Linux, resident peak on macOS; a job with no history takes the class
  default). The gateway holds `budget = physical − reserved_for_host` and defers a `background` or `interactive` start,
  queued and never refused, while the running jobs' estimates plus its own exceed the budget; a `gate` start is never
  deferred by estimate. Estimates decay toward measured usage after a startup window, as Borg's reservation does
  (research §2.2). On Linux the estimates become per-job `memory.high` (estimate × 1.5) and `memory.max` (a share of the
  budget) beneath the class's ceilings; on macOS per-process soft limits wait for the measurement of whether macOS
  enables the over-soft-limit kill (research §2.9). No value is set yet for `reserved_for_host`, the per-class default
  estimate, the budget share or the decay window (Borg's is 300 s, for its own reservation).
- **U6, I/O pools.** A fork and attach pool of depth `k`, chosen by measuring fork latency p95 under B = 6 at k = 1, 2
  and 4 (a fork's first write copies the source's extent map and an attach spends most of its time in `syncAllDisks`:
  `specs/cowshed/01a_apfs.md`); on Linux an `io.latency` target for the gate from its B = 0 store latency p95; an audit
  that no spawn path sets a disk policy below IMPORTANT, a QoS clamp or `PRIO_DARWIN_BG` (a grep finds none today, and
  no test names them). Image cleanup already frees one entry at a time, in cuts of at most 1,024 physical runs
  (`specs/cowshed/05_gateway.md` "Deleting released images"). Acceptance: the 28385f7929 shape (release 55 images at
  once) keeps a concurrent `git status` p95 at most 2× its B = 0 value, and fork p95 under B = 6 at most 2× B = 0.
- **Status fields.** `cowshed gateway status` reports no per-class CPU time (Linux `cpu.stat`, macOS summed rusage) or
  resident and charged memory, no PSI `cpu` or `io`, and neither the memory budget with its deferred starts and
  estimates nor the pool depths and waiters. The last two come with U5's rest and U6; U0 decides whether the others
  belong in status or only in the harness, which records them.
- **The ledger rows.** `packages/cowshed/docs/optimization-ledger.md` gets a row per unit, U0 to U6, once the harness
  exists; each names the host, the date, B and the kernel reading its claim rests on.

## Open

Each item says what decides it.

- **The Linux pressure threshold.** A background start waits while the host's `some avg10` memory pressure is above
  10.00 % (`PSI_SOME_AVG10_LIMIT` in `packages/cowshed/crates/cowshed-core/src/host_pressure/mod.rs`), a starting value
  that no measurement backs. Decides: the `some avg10` a pressured Linux host reaches in the 3607bc754e shape, and the
  level above which a start should wait, read with U0's harness.
- **The background class's memory ceilings.** `memory.high` (half of physical memory) and `memory.max` (three quarters)
  in `packages/cowshed/crates/cowshed-core/src/runtime/placement/linux.rs` bound every background and resident job
  together. They are fixed shares, since no job declares an estimate, and `resident` has the same values because it runs
  in the `background` class cgroup. Decides: U0's swap and throughput rows at B = 6 say whether the shares hold; U5's
  estimates would replace them with a budget.
- **The task server's serialization classes and the disk lease.** The classes `cowshed-new`, `image` and
  `land-publication` and the disk-lifecycle lease are unchanged by the landing (`specs/cowshed/05_gateway.md` "The task
  server schedules work units", "Disk-lifecycle lease"). `[INFERENCE]` They are ordering rules and stay. The sentence of
  "Host scheduling" that lists what cowshed bounds does not name them. Decides: the owner's ruling that they are
  ordering rules and not admission, after which that sentence names them.
- **A test of the Linux values.** `packages/cowshed/crates/cowshed-core/tests/job_cgroup_linux.rs` follows the class
  layout, with every job admitted under its class cgroup, but no test reads back the values the placement writes
  (`cpu.weight.nice`, `memory.low`, `memory.high`, `memory.max`, `memory.swap.max`, `pids.max`), and none drives the
  supervisor's pressure wait or the macOS ending: the test paragraph of `specs/cowshed/05_gateway.md` "Status" keeps
  tests to the pure decisions. Decides: whether pinning cowshed's own writes is worth a test on a Linux host.
- **The `pids.max` bound.** 16 384 per non-gate job is a reasoned figure, far above what a Cargo or nextest tree holds,
  not a measured one, and no unit measures it.
- **macOS E-core placement.** Nice +10 (base priority 21) is non-UI and P-core-eligible, and nice +11 and beyond is
  utility class and E-core-preferred. `[INFERENCE]` The class mapping is verbatim from the XNU sources; the placement
  the performance controller then chooses is not measured, and whether E-core steering would help the gate stays
  unmeasured (research §2.8). Nothing depends on it while the bands are fixed.
- **Overshoot of a pressure-only gate.** `[INFERENCE]` A burst of starts before the reading moves can overshoot, since
  the gate reacts to pressure the host already shows. Decides: the 3607bc754e reproduction on the harness, which also
  decides whether U5's rest is needed.

The research marks more inferences: that macOS thread-group membership cannot be set by a daemon for its children
(§2.1), that the honest form of the opportunistic tier on macOS is lower nice plus a memory ceiling, not a freeze
(§2.6), the P/E core split of an 18-core M-series (§2.8), whether macOS enables the over-soft-limit kill and which API
sums a job tree's resident memory (§2.9), and that the lock inversion of 57b1c81bc0 also holds for Linux `io.latency`
(§2.10).
