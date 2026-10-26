# cowshed host scheduling: the kernel schedules, cowshed admits only what the kernel cannot take back

Status: Approved by the owner, 2026-10-26; implementation under way in the simplest scope (ledger deleted, spawn at
class priority, per-job jobserver, Linux cgroup weights, pressure-gated background starts).

Written 2026-10-26 from the owner's ruling of that day: "Need this designed for cowshed where the OS (mac, Linux) does
actual resource scheduling." The research and the incident table under it are in
`agent-todo/cowshed-os-scheduling-research.md`. Sections 1 to 8 and 10 are in the present tense, as spec text, and
describe the system after the migration of section 9, so that U7 can lift them into `specs/cowshed/`; until the
implementation lands they describe what the code will do, not what it does. They already carry the decisions below,
which amend the draft this brief was made from. Sources are cited inline, and inferences are marked `[INFERENCE]` and
collected under "Open inferences". Spec names without a directory are in `specs/cowshed/`, and code paths are relative
to the repository root. Line numbers are at main 33aaff9cc0.

## Goal

Replace the host-wide CPU token ledger with kernel scheduling. A job that is admitted spawns at once under a class that
the supervisor turns into kernel scheduling parameters: cgroup v2 weights and ceilings on Linux, `setpriority` nice on
macOS. Cowshed keeps admission only for what a kernel cannot take back from a running job without killing it: memory,
the thread count inside one build tree, device-saturating store I/O units, and one gate per repository.

The measuring rule is the owner's ruling of 2026-10-26: no gate ever needs a quiet host (the one legitimate quiet host
is a benchmark that needs absolute precision; `05_gateway.md` "Room for a gate", :1537-1542), and a real optimization
must be a clear winner on a busy host, because one that only wins on an idle host fails in reality. Section 9 builds the
harness that applies the rule; the first scope does not build it (decision 7).

## Decisions

The owner's rulings of 2026-10-26 on this design. The sections below carry them; where the draft this brief was made
from said otherwise, the decision wins.

1. **Scope.** The design is approved and implemented first in the simplest scope: the ledger is deleted, a job spawns at
   once at its class's priority, every job gets a per-job jobserver, Linux jobs run under cgroup weights, and background
   starts are gated on memory pressure.
2. **One interface, two platforms.** Linux and macOS are separate, in separate per-platform files behind one shared
   interface (§2 "Structure").
3. **Bands.** The bands stay the owner's order of 2026-10-17 (`05_gateway.md` "Bands"): interactive before gate before
   background. One class-to-nice table serves both platforms (§2): macOS places it with `setpriority`, Linux writes the
   same nice value to the class cgroup's `cpu.weight.nice`.
4. **Classes are cgroups.** On Linux the class cgroups sit directly under the delegated root (`gate`, `interactive`,
   `background`, `resident`), each carrying its class's `cpu.weight.nice` and memory settings, and every job cgroup
   lives under its class's cgroup, so classes compete as siblings host-wide (§3). Per-workspace grouping, if accounting
   needs it, is read from job metadata and is not a cgroup level.
5. **Task server.** Its resource fit against tokens is deleted with the ledger. Work units start at once, subject only
   to the memory-pressure gate.
6. **Out of scope.** `io.weight`, which needs the root-only `io.cost.qos`, and the gate's `memory.low`. Background keeps
   `memory.high` and `memory.swap.max` = 0.
7. **Non-goals.** The measurement units: the busy-host harness (U0), the macOS band measurement (U2), and every
   acceptance number that needs the harness.
8. **macOS memory.** When `kern.memorystatus_vm_pressure_level` reaches Critical, the supervisor ends the youngest
   `background` (or `resident`) job, and ends the next one on each next pressure reading while Critical persists (§5.2).
   Linux has no such loop: the kernel throttles and bounds (`memory.high`, `memory.swap.max`).

## Spec

The text this design replaces or rewrites (U7):

- `specs/cowshed/05_gateway.md:1222` "Host CPU budget", which holds the token ledger: "Nested runners use a parent-held
  sub-ledger" (:1274), "All at once, then topped up from tokens nobody waits for" (:1384), "A one-token job starts on
  probation" (:1402), "Gate admission: oldest gate job first, ahead of background, never starving it" (:1440), "One gate
  per repository, and a second says whom it waits for" (:1460), "A job lives with its requester" (:1503), "Room for a
  gate: gate-class asks are served first, and nothing else yields to them" (:1518), "A resident holds no token" (:1637)
  and "Bands" (:1722), the one statement of a class's token order and nice (interactive 0, gate +5, background +10).
- `specs/cowshed/05_gateway.md:261` "An install refuses while a land or a gate runs": the restart guard reads a gate
  that "holds or wants tokens".
- `specs/cowshed/05_gateway.md:1944` "The task server schedules work units", built on the ledger ("The CPU budget stays
  the only allocator", :1977).
- `specs/cowshed/07_api.md`: `ExecClass` (:216), the exec request's `class` and `want` (:202-205), `cpuBudget` and
  `CpuBudgetStatus` (:1730-1737), "Complete job accounting and observation reconciliation" (:1431, the Linux per-job
  cgroup this design extends), and the `--class` parity row (:2223).
- `specs/cowshed/08_testing.md:765-770`: "The landing gate is never CPU-limited", with the 40-minute `--parallel=2`
  under `taskpolicy -b` against 4m44s measurement.
- `specs/cowshed/11_shell.md:1303` "Server-owned CPU admission and nice placement", and `REPLY_NICE_REFUSED` (:1328).

## What exists

Admission today is the host ledger (`packages/cowshed/crates/cowshed-gateway/src/cpu_budget.rs`): a job asks for `want`
tokens and is granted them all at once or waits; a running grant cannot be shrunk; every free or returned token goes
first to a waiting gate-class request, and background is limited only by the free tokens, under checkout fairness, with
a one-token floor (`behind_limit`) that guarantees a starved background checkout progress; a grant below its want tops
up from tokens no request waits for; a one-token job starts on probation. Since 023682e1c4 nothing else yields to a
gate: forks, attaches and the released-image cleanup never wait for one, and the gateway keeps no quiet state
(`05_gateway.md` "Room for a gate", :1537-1548). The class fixes a nice, which the supervisor places with `setpriority`
and reads back: `class_nice` (`packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1044`) gives interactive
0, gate +5 and background +10, `place_nice` places it on macOS (:1078), and `report_own_nice`
(`packages/cowshed/crates/cowshed-core/src/runtime/shell_host.rs:507`) reports it back from the child. Every cowshed
launchd agent is `ProcessType=Interactive`, with the measured cost of the clamped types in the comment at
`packages/cowshed/crates/cowshed-cli/src/launchd.rs:372-389`.

On Linux every job already has its own cgroup v2, entered by its first process before exec
(`packages/cowshed/crates/cowshed-core/src/runtime/job_cgroup.rs`, `admit` at :527), under its workspace incarnation's
cgroup (`<delegated>/ws-<incarnation>/job-<id>`, :15-19), with the `cpu`, `memory` and `io` controllers enabled for
accounting (`Controller::ACCOUNTING`, :99-102). Cowshed reads `cpu.stat`, `memory.current`/`memory.peak` and `io.stat`
from it and writes `cgroup.kill` and `cgroup.freeze`. No weight, ceiling or `pids` limit is set anywhere in the crates,
the `pids` controller is not enabled, and nothing reads PSI or the macOS memory pressure level.

The six incidents that show the ledger's failure modes, with their measurements and commits, are in
`agent-todo/cowshed-os-scheduling-research.md`.

## 1. Principle

Cowshed never decides whether runnable work runs. A job that is admitted is spawned at once, under a **class** that the
supervisor translates into kernel scheduling parameters, and from then on the kernel arbitrates CPU time, memory reclaim
and I/O among every runnable thread on the host. Cowshed keeps **admission** for exactly the resources a kernel cannot
reclaim from a running job without killing it or that it cannot see as one unit:

- memory, by estimate and by measured pressure (non-compressible:
  [Borg §6.2](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf));
- parallelism _inside_ one build tree, by the jobserver, so one `cargo`/`nx`/`nextest` does not hand the kernel ten
  times the core count ([make jobserver](https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html);
  [cargo job_queue](https://doc.rust-lang.org/stable/nightly-rustc/cargo/compiler/job_queue/index.html));
- device-saturating I/O units on the store, by bounded work and a depth-limited pool
  ([Ninja pools](https://ninja-build.org/manual.html); commits 28385f7929, 57b1c81bc0);
- repository ordering (one gate per repository), which is a correctness rule, not a resource.

Nothing else waits. There is no host token, no grant, no floor, no top-up, no quiet. The gateway reports what the kernel
measures, not what a ledger counts.

## 2. Classes

The class vocabulary stays: `gate`, `interactive`, `background`, `resident`, `interactive-resident` (`ExecClass`,
07_api.md). A class is a place in one order, interactive before gate before background (the owner's order of 2026-10-17,
05_gateway.md "Bands"), realized per OS by §3 and §4 from one class-to-nice table. A resident is a background process
that is idle between requests and therefore costs nothing while idle (a weight on a sleeping thread is free: weights are
work-conserving, [cgroup-v2 "Resource Distribution Models"](https://docs.kernel.org/admin-guide/cgroup-v2.html)).

| class                  | nice | memory                                                       | I/O     | what it means                                                                                    |
| ---------------------- | ---- | ------------------------------------------------------------ | ------- | ------------------------------------------------------------------------------------------------ |
| `interactive`          | 0    | soft ceiling at its declared estimate                        | default | a person or model waits on it                                                                    |
| `gate`                 | +5   | no ceiling                                                   | default | the run that moves main (08_testing.md:766); it is never CPU-limited: no cap, no clamp, no quota |
| `background`           | +10  | soft ceiling; first to be reclaimed or killed under pressure | default | every other exec, the default                                                                    |
| `resident`             | +10  | as background                                                | default | holds nothing while idle; nothing to release                                                     |
| `interactive-resident` | 0    | as interactive                                               | default | the same resident, at interactive's nice                                                         |

`io.weight` is out of scope (decision 6), so I/O stays at the default policy on both platforms. The nice values are the
owner's bands (decision 3). On macOS they are relative to the supervisor's own nice: raising a priority above it takes a
privilege no supervisor holds, so the order is made by lowering it (05_gateway.md "Bands"), and the scheduler's priority
order decides (§4.4). On Linux the kernel turns them into the classes' weights, and five nice steps are roughly a 3 : 1
weight ratio (each step splits two competing tasks 55 / 45,
[sched-nice-design](https://docs.kernel.org/scheduler/sched-nice-design.html)): with every class active, the interactive
class gets about three times the gate class's share and the gate class about three times the background class's, however
many jobs each holds.

**Structure.** Linux (§3) and macOS (§4) are separate, in separate per-platform files behind one shared interface
(decision 2). There are three areas: job class placement (a class becomes a class cgroup's weight on Linux and a nice on
macOS), the memory-pressure reader (PSI on Linux, `kern.memorystatus_vm_pressure_level` on macOS), and the host-status
reader (`cpu.stat`, PSI and cgroup files on Linux, rusage and the pressure level on macOS). Each area is a directory:
`mod.rs` holds the interface and the pure core, the decisions computed from readings (the start gate of §5.1, the choice
of the job to end in §5.2, the aggregation of §7), and `macos.rs` and `linux.rs` hold only what reads or writes the OS;
one `cfg` selects the platform file, in `mod.rs`. A decision calls a reader and never reads the OS itself.
`packages/cowshed/crates/cowshed-core/src/api/peer_credentials/` (`mod.rs`, `linux.rs`, `macos.rs`) is the pattern in
the tree. The interface of each area is designed before either platform file is written. The macOS nice placement that
`supervisor.rs` holds today (`class_nice`, `place_nice`) moves into the macOS file of the placement area.

## 3. Linux

Cowshed on Linux already runs every job in its own cgroup v2 under a delegated subtree, entered by the job's first
process before exec, with `cpu.stat`, `memory.current`/`memory.peak` and `io.stat` read for accounting and
`cgroup.kill`/`cgroup.freeze` for control (`packages/cowshed/crates/cowshed-core/src/runtime/job_cgroup.rs`; 07_api.md
"Complete job accounting and observation reconciliation"). The design puts **a class cgroup between the delegated root
and every job** (decision 4): `<delegated>/gate`, `<delegated>/interactive`, `<delegated>/background` and
`<delegated>/resident` `[INFERENCE: interactive-resident, which has interactive's nice, goes under interactive]`, each
carrying its class's weight and memory settings. Every job cgroup is created under its class's cgroup and entered by the
job's first process before exec, as today. Weights compare siblings only: "A parent's resource is distributed by adding
up the weights of all active children and giving each the fraction matching the ratio of its weight against the sum"
([cgroup-v2 "Weights"](https://docs.kernel.org/admin-guide/cgroup-v2.html)). With the classes as siblings under one
root, the class order therefore holds across every workspace on the host, where jobs grouped under their workspace's
cgroup (today's `<delegated>/ws-<incarnation>/job-<id>`, `job_cgroup.rs:15-19`) would be ranked only against the jobs of
their own workspace. Per-workspace grouping, if accounting needs it, is read from job metadata and is not a cgroup
level. The accounting controllers (`cpu`, `memory`, `io`) stay enabled in the `cgroup.subtree_control` of the delegated
root and of each class cgroup, so every job cgroup keeps its counters; `pids` is added where `pids.max` is written (not
in the first scope). The delegation is unchanged: the controller writes only inside the subtree it was given
(`systemd-run --scope -p Delegate=yes` or an equivalent delegated directory;
[cgroup-v2 "Delegation"](https://docs.kernel.org/admin-guide/cgroup-v2.html);
[systemd.resource-control(5)](https://www.man7.org/linux/man-pages/man5/systemd.resource-control.5.html)).

Per class cgroup, written once when the class cgroup is created. The first scope writes exactly these rows:

| file              | `gate` | `interactive` | `background`               | `resident`    |
| ----------------- | ------ | ------------- | -------------------------- | ------------- |
| `cpu.weight.nice` | +5     | 0             | +10                        | +10           |
| `cpu.max`         | `max`  | `max`         | `max`                      | `max`         |
| `memory.high`     | `max`  | `max`         | class ceiling (value open) | as background |
| `memory.swap.max` | `max`  | `max`         | 0                          | 0             |

`memory.high` on a class cgroup bounds the class's jobs together, since usage is counted over the cgroup's subtree. The
rest of the draft's table, which was per job cgroup, needs an estimate, a share or a measurement, and is not in the
first scope `[INFERENCE: the decisions name the rows above; the rest waits]`:

| file          | `gate`                     | `interactive`                                     | `background`             | `resident` |
| ------------- | -------------------------- | ------------------------------------------------- | ------------------------ | ---------- |
| `memory.high` | `max`                      | declared estimate × 1.5                           | declared estimate × 1.5  | class's    |
| `memory.max`  | `max`                      | `max`                                             | host budget share (§5.1) | class's    |
| `io.latency`  | target from U6 measurement | —                                                 | —                        | —          |
| `pids.max`    | `max`                      | 4× cores × 64 `[INFERENCE: bound to be measured]` | same                     | same       |

`io.weight` and the gate's `memory.low` are out of scope (decision 6). `io.weight` needs iocost, whose `io.cost.qos`
"exists only on the root cgroup" and whose controller "is disabled by default"
([cgroup-v2 IO](https://docs.kernel.org/admin-guide/cgroup-v2.html)); nothing in this design reserves memory for the
gate.

Why each file, with its source:

- `cpu.weight.nice`: the class's nice from §2's table, written through the kernel's alternative interface for
  `cpu.weight`, which "allows reading and setting weight using the same values used by nice(2)"
  ([cgroup-v2 CPU](https://docs.kernel.org/admin-guide/cgroup-v2.html)); the value read back is "the closest
  approximation of the current weight", and the weights are work-conserving.
- `cpu.max`: never set: a quota strands cycles exactly as tokens did.
- `memory.high`: throttle + reclaim, never OOM; "should be used in scenarios where an external process monitors the
  limited cgroup" ([cgroup-v2 memory](https://docs.kernel.org/admin-guide/cgroup-v2.html)) — the supervisor is that
  process.
- `memory.swap.max`: background work does not earn the right to swap the host (3607bc754e).
- `memory.max` (later): hard limit; OOM kill inside the job's cgroup only, so a runaway background build dies alone.
- `io.latency` (later): work-conserving: throttles peers only once the gate misses its target.
- `pids.max` (later): fork bombs fail with EAGAIN inside the job, not host-wide.

Pressure is read, not inferred: `/proc/pressure/memory`, `/proc/pressure/cpu`, `/proc/pressure/io` host-wide and
`memory.pressure`/`io.pressure` per job ([psi.rst](https://docs.kernel.org/6.0/accounting/psi.html)). The gateway's
status line reports them (§7).

Per-process nice placement is retired on Linux: a per-process nice inside a cgroup only orders threads within that
group, and the group's weight is what the host sees, so the class's nice goes to its class cgroup's `cpu.weight.nice`
(decision 3) and one table serves both platforms. `report_own_nice` and `REPLY_NICE_REFUSED` (11_shell.md) remain as the
macOS mechanism and are not called on Linux.

## 4. macOS

### 4.1 What macOS has

- A per-thread timesharing scheduler with QoS buckets selected by EDF under per-bucket worst-case latencies (FG 0 ms, IN
  37.5, DF 75, UT 150, BG 250) and quanta (10/10/8/6/4/2 ms), with a starvation-avoidance window for low buckets
  ([sched_clutch_edge.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/scheduler/sched_clutch_edge.md)).
  A niced thread is therefore bounded-late, never starved.
- POSIX `nice` via `setpriority(PRIO_PROCESS)`: base priority 31 − nice. The performance controller classes a thread by
  base priority: ≤ 4 background, ≤ 20 utility, ≤ 31 non-UI
  ([sched_prim.c](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched_prim.c);
  [sched.h](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched.h)). The AMP policy "is to run
  utility and bg threads on E-Cores only" by default
  ([sched_amp_common.c](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched_amp_common.c)). So
  nice +10 (pri 21) is non-UI and P-core-eligible; nice +11 and beyond (pri ≤ 20) is utility and E-core-preferred
  `[INFERENCE: the class mapping is verbatim; CLPC's resulting placement is not measured, since U2 is not built]`.
- `PRIO_DARWIN_BG` and the QoS clamps (`taskpolicy -b`, `-c utility|background`): priority to the lowest value **and**
  "disk and network IO are throttled"
  ([setpriority(2)](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setpriority.2.html);
  [taskpolicy(8)](https://manp.gs/mac/8/taskpolicy)).
- Disk I/O policies IMPORTANT/STANDARD/UTILITY/THROTTLE/PASSIVE, where a throttleable I/O issued near a higher one
  "forces [the issuing thread] to a sleep for a short period"
  ([setiopolicy_np(3)](https://manp.gs/mac/3/getiopolicy_np)).
- memorystatus: per-process memory limits set at spawn (`taskpolicy -m`, launchd JetsamProperties), jetsam bands, kills
  of over-soft-limit processes under pressure, and a published pressure level (Normal / Warning / Critical) via
  `kern.memorystatus_vm_pressure_level` and `DISPATCH_SOURCE_TYPE_MEMORYPRESSURE`
  ([memorystatus.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/vm/memorystatus.md);
  [memorystatus_notify.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/vm/memorystatus_notify.md)).

### 4.2 What macOS lacks

- No cgroups: no group weight, no group memory charge, no proportional I/O controller. A job tree is observed by the
  supervisor's process fold (`JobContainment::Observed`, 07_api.md) and its resident memory is summed from per-process
  samples (`JobResourceSample.rss`), never charged by the kernel.
- No kernel-side preemption class that does not also throttle I/O.

### 4.3 The measured constraint

Commit 57b1c81bc0: under IOPOL_THROTTLE, 1,213 of 1,963 metadata reads slept at a 20.1 ms median; an unlink took 53.1 s
against 7.6 s at normal policy, and a probe writer's p50 rose from 0.2 ms to 10.6 ms, because the throttled thread held
the APFS container's work while it slept. `packages/cowshed/crates/cowshed-cli/src/launchd.rs:372-389`: a `Background`
process type made a 0.7 s loop take 1–30 s and `direnv export json` take 537 s at load 80; the `utility` clamp made a
cold SQLite open take 34.5–54.5 s against 2.4–2.7 s. The inversion is structural — the throttle sleeps the holder of a
shared lock — so:

**Rule.** On macOS no cowshed-spawned process that opens a path under the store or a workspace runs under
`PRIO_DARWIN_BG`, a QoS clamp, or any disk policy below IMPORTANT. Class is expressed with
`setpriority(PRIO_PROCESS, pid, nice)` alone, placed and read back as today (11_shell.md). Every cowshed launchd agent
stays `ProcessType=Interactive`.

### 4.4 Per class on macOS

| class         | nice                           | I/O policy          | memory                                                | E-core                   |
| ------------- | ------------------------------ | ------------------- | ----------------------------------------------------- | ------------------------ |
| `interactive` | 0                              | default             | never ended for memory (§5.2)                         | P-eligible               |
| `gate`        | +5                             | IMPORTANT (default) | no ceiling; never ended for memory (§5.2)             | P-eligible               |
| `background`  | +10                            | default             | first to be ended at Critical (§5.2)                  | P-eligible `[INFERENCE]` |
| `resident`    | +10 (`interactive-resident` 0) | default             | as background (`interactive-resident` as interactive) | as its nice              |

The gate's advantage on macOS is thus the Clutch scheduler's: at nice +5 against nice +10 its threads are picked first
by priority (base priority 26 against 21) and the background bucket's progress is bounded to its WCEL; interactive work
at nice 0 (base priority 31) is picked before both. The 08_testing.md measurement (40 min capped at `--parallel=2` under
`taskpolicy -b` vs 4m44s) was a clamp plus a parallelism cap, not nice; the first scope does not measure nice alone
(decision 7).

## 5. What remains admission-controlled, and why

### 5.1 Memory

Memory is the non-compressible resource; the kernel can only reclaim or kill. Admission is kept, in three parts. The
first scope builds part 3, the pressure gate, alone
`[INFERENCE: the scope names pressure-gated starts and no estimates]`; parts 1 and 2 are the budget of the full design.

1. **Estimate at ask.** An exec declares `memory: <bytes>` (default per class from the job's own history in the job
   artifact: `memory.peak` on Linux, resident peak on macOS; a job with no history takes the class default). This is
   Bazel's `--local_resources=memory` applied per job ([Bazel user manual](https://bazel.build/docs/user-manual)) and
   Apollo's token "up to a predefined amount of CPU and memory"
   ([Apollo §2.1](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-boutin_0.pdf)).
2. **Host budget.** The gateway holds `budget = physical − reserved_for_host`; a `background` or `interactive` start is
   deferred — queued, never refused — while `Σ estimates of running jobs + estimate > budget`. A `gate` start is never
   deferred by estimate. Estimates decay toward measured usage after a startup window, as Borg's reservation does
   (initial = request, then "decays slowly towards the actual usage plus a safety margin",
   [Borg §5.5](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf)), so an idle 17-thread nextest holds only what it
   maps.
3. **Pressure.** No `background` job starts while the host reports memory pressure: Linux `/proc/pressure/memory`
   `some avg10` above a threshold (the draft left its value to U5's measurement, a non-goal of the first scope); macOS
   `kern.memorystatus_vm_pressure_level ≥ Warning` (non-compressed available memory under 50% of available,
   [memorystatus_notify.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/vm/memorystatus_notify.md)).
   This is make's `-l` rule for the resource that matters
   ([make options](https://www.gnu.org/software/make/manual/html_node/Options-Summary.html)).

### 5.2 Ceilings and ending

Linux: the kernel throttles and bounds, with no loop of cowshed's: `memory.high` on the background and resident class
cgroups throttles and reclaims, and `memory.swap.max` = 0 keeps them off swap (§3).

macOS, the owner's ruling of 2026-10-26 (decision 8): when `kern.memorystatus_vm_pressure_level` reaches Critical, the
supervisor ends the youngest `background` (or `resident`) job — SIGTERM, the existing grace, SIGKILL, exactly the cancel
path — and reports it `failed`, failure `memory`, naming the level and its resident sum (from the job's resident
sample). While Critical persists it ends the next one on each next pressure reading. A `gate` or `interactive` job is
never ended for memory. Which job to end is a pure decision of the memory-pressure area (§2); the reading is the
platform file's, and the ending is the supervisor's existing cancel path.

The rule in 05_gateway.md "Room for a gate" (:1521-1523) that a killed nextest run cannot resume stands: an ended job is
reported as `failed`, failure `memory`, with the pressure level and the resident sum on its stderr. It is not silently
retried.

### 5.3 Parallelism inside a build

Every job still receives a jobserver fifo (`MAKEFLAGS`, `CARGO_MAKEFLAGS`, `BOUNDED_EXEC_CPU_TOKENS`,
`RUST_TEST_THREADS`, `NEXTEST_TEST_THREADS`), **served by its own supervisor with a constant count**: `cores` for every
class. The fifo bounds the number of compiler and test threads one build tree starts
([make jobserver](https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html); "Cargo also must not spawn
more than N threads of work",
[cargo job_queue](https://doc.rust-lang.org/stable/nightly-rustc/cargo/compiler/job_queue/index.html)). Nested runners
inherit the parent's fifo as they do now ("Nested runners use a parent-held sub-ledger", 05_gateway.md), minus the host
relay: a gate's children read the gate's fifo. A pool is a per-job object, not a host object; two builds on one host
each have `cores` tokens, and the kernel weighs their threads against each other. An ask for fewer tokens (a short
one-token command) gets fewer.

Why `cores` and not fewer for background: a grant of one token to a 20-minute `nx run-many` made it a 21-minute run on
one core beside a mostly idle host ("A one-token job starts on probation", 05_gateway.md). Under the bands, a background
build at `cores` threads and nice +10 gives way to the gate and to interactive work, which hold larger shares from the
moment they are runnable, and takes the cores back the moment they are not.

### 5.4 Store I/O units

Operations that saturate the APFS container or the ext4/xfs journal are **pools with a depth**, not classes: image
cleanup depth 1 with 1,024-extent cuts (28385f7929, measured ~0.1 s per cut); fork and attach of a repository's images
depth `k` set by U6 (a fork's first write copies the source's extent map, 01a_apfs.md:74-76, and an attach spends most
of its time in `syncAllDisks`, 01a_apfs.md:764-775 and 05_gateway.md "Disk-lifecycle lease", :1036). A pool entry waits
for a slot, never for a gate. On Linux, `io.latency` on the gate's cgroup additionally throttles peers at bio issue when
the gate's store latency slips (not in the first scope: its target needs a measurement); on macOS the pool is the only
arbiter.

### 5.5 Repository ordering

One gate-class job per repository at a time stays (05_gateway.md "One gate per repository"): it serializes lands, which
is ordering, not capacity. A second gate waits with cause `RepositoryGate`. No CPU wait cause exists; the only other
waits are the memory budget's and the pools' (§7).

## 6. The incidents under this design

Figures are those of the research table, where the ones no commit message records are marked.

- **f5cff758e8 — background nextest held 17/18 tokens at 0% CPU for 50 min, gate on 1 token.** There are no host tokens.
  The nextest's 17 threads are asleep in the kernel and weigh nothing. The gate starts at nice +5 in the `gate` class
  cgroup with its own `cores`-token fifo and runs at full width at once. Measured target (needs the busy-host harness, a
  non-goal of the first scope): gate wall time beside the idle nextest within 10% of an empty host.
- **fa47c27313 — half the host idle under a static cap with no gate running.** No cap exists. Background builds each run
  `cores` threads; the kernel keeps every core busy. Weights are work-conserving by definition
  ([cgroup-v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)).
- **3607bc754e — six acceptance runs of one repo, load 23 on 18 cores, 5.9/7 GB swap.** CPU: load 23 is fine; the
  classes' weights decide who gets the cores. Memory: in the first scope no further background run starts while the host
  reports memory pressure (§5.1.3), Linux `memory.high` and `memory.swap.max=0` on the background class throttle them
  before the host swaps, and macOS ends the youngest background run at Critical (§5.2); the estimate budget (§5.1.2),
  not in the first scope, would also defer the fourth and later runs at start. No agent has to hear a quiet window.
- **9752d00c7d — 17 asks queued with 9/18 tokens free.** No ledger, no floor, no queue to mis-serve. The only queue is
  the memory gate (the pressure reading of §5.1.3 in the first scope, with the estimate budget of §5.1.2 later), whose
  state is reported in status.
- **28385f7929 — 55 truncators saturated the container.** Depth-1 cleanup pool with bounded cuts, unchanged. On Linux,
  cleanup could additionally run under the gate's `io.latency` target (not in the first scope).
- **57b1c81bc0 — IOPOL_THROTTLE inversion, 50× slower writers.** §4.3 rule: no throttle policy, no clamp, no
  `PRIO_DARWIN_BG` for anything touching the store. Priority is CPU-only nice.
- **2026-10-26 observation — nextest holding 9 tokens at 0 CPU for 12 min, tests waiting on children queued at the
  host.** Children never queue at the host: there is no host queue. A test's child `cargo` reads the test run's own fifo
  (§5.3); if the fifo is exhausted it waits for a sibling test's thread to finish, which is the jobserver's intended
  behavior inside one tree, and the kernel runs whichever threads are runnable.

## 7. Observability

`cowshed gateway status` and `CpuBudgetStatus` are replaced by a **host pressure status**: per class the number of
running jobs, their summed CPU time over the last minute (Linux `cpu.stat`; macOS summed rusage), their summed
resident/charged memory, the host's PSI (`cpu`, `memory`, `io` `some avg10/avg60`) or macOS pressure level and 1-minute
load, the memory budget (`Σ estimates / budget`, with the deferred starts and their estimates), each pool's depth and
waiters, and the per-repository gate holder. Every number is a kernel or rusage reading; none is a count the gateway
invented. A deferred start says why on its `jobserver wait start` line as today's wait cause does:
`{"kind":"memory","estimate":…,"budget":…,"pressure":…}` or `{"kind":"pool","pool":"image-cleanup","depth":1}` or
`{"kind":"repositoryGate",…}`.

## 8. Deleted and kept

**Deleted**, in the crates `cowshed-gateway-types`, `cowshed-gateway` and `cowshed-core` (under
`packages/cowshed/crates/`; locations at main 33aaff9cc0): the host CPU ledger and everything that exists to serve it.

- `cowshed-gateway/src/cpu_budget.rs`: `Ledger` (:245), `behind_limit` (:195), `next_gate` (:634), `next_background`
  (:700), `TopUpOutcome` (:210) with top-up and return handling, checkout-fairness shares and the overdue-head pass
  bound, and the gate's relay pool (header, :25-30). `Ledger` also names three other types that stay:
  `cowshed-gateway/src/disk_lease.rs:141`, `cowshed-gateway/src/task_server.rs:234` and
  `cowshed-core/src/runtime/job_groups.rs:45`.
- `cowshed-gateway-types/src/cpu_budget.rs`: `background_limit` (:503, with `background_share` :516), `JobserverClass`
  on the wire (:54), `CpuWaitCause::Tokens` (:327; `RepositoryGate` stays), the `CpuBudgetStatus` token fields (:377),
  and the `BOUNDED_EXEC_CPU_POOL` host endpoint (`CPU_POOL_ENV`, :34).
- `cowshed-core/src/runtime/job_cpu.rs`: the probation token (`JobCpuAdmission` probation, `probation_stopped` :212,
  `ProbationOutcome` :106).
- The task server's resource fit against tokens (decision 5): the "resource fit (the predicted cost against the free CPU
  tokens, unknown last)" ordering key (05_gateway.md:1956) and the capacity view that folds the ledger (:1977), in
  `cowshed-gateway/src/task_server.rs`. Work units start at once, subject only to the memory-pressure gate.
- Spec text: in 05_gateway.md "Room for a gate", "All at once", "Gate admission" (except repository serialization), "A
  one-token job starts on probation" and "A resident holds no token" (vacuous once nothing holds tokens); in 11_shell.md
  "Server-owned CPU admission" insofar as it describes the grant.
- `taskpolicy -b` guidance in `packages/cowshed/docs/optimization-ledger.md` (:59-66, :113) and
  `packages/cowshed/skills/cowshed/SKILL.md` (:112-114) is rewritten under §4.3.
- Not named by the design, found by search at main 33aaff9cc0 (search again at implementation time). The rest of
  `JobCpuAdmission` (the trait, :48), `GatewayJobCpu` (:69) and `ActivationStake` (:266, a spare shell's activation
  holds one background token while it runs) are the supervisor's side of the ledger, and every supervisor, shell-host
  and test fixture implements or holds them. The restart guard `refuse_restart_while_busy`
  (`packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:1495`), its status lines (`read_cpu_budget` :1473,
  `cpu_budget_lines` :2067) and `tooling/land-install.ts` (`gateJobs`, :237) read the ledger's gate jobs, so the host
  pressure status of §7 keeps the per-repository gate holder they need. The generated readers and bindings regenerate:
  `packages/cowshed/crates/cowshed-core/src/api/reader.generated.rs`,
  `packages/cowshed/src/{api,reader,validators}.generated.ts` and `packages/cowshed/src/wire-contract.test.ts`. The
  `bounded-exec` executor of `packages/nx-plugin` reads `BOUNDED_EXEC_CPU_POOL` and `BOUNDED_EXEC_CPU_TOKENS`
  (`src/executors/bounded-exec/cpu-pool.ts`, `cpu-tokens.ts`, `executor.ts`, `schema.json`, its tests and `README.md`),
  and `cowshed-core/src/runtime/supervisor.rs` exports the pool endpoint to each job (:755).
- Already gone: the quiet waits were removed by 023682e1c4, which the draft's list still named (`floor_open`,
  `GateWork`, `QuietScope`, `CpuWaitCause::Quiet`, the `quiet` control op with `QUIET_OP` and `QuietRequest`, and the "A
  fork or attach waits for its repository's gate" paragraph of 05_gateway.md). Nothing of them remains to delete.

**Kept**: `ExecClass` and class authorization (gate is trusted-route only); the macOS nice placement with read-back and
typed `priority` failure, moved into the macOS file of the placement area (§2); the per-job jobserver fifo and its
environment exports, now with a constant count; the Linux job cgroup and its accounting, now beneath class cgroups that
carry the weights and memory settings; `JobContainment`, process folds and resident sampling on macOS;
`cgroup.kill`/`cgroup.freeze` and the macOS group-leader hold; the image-cleanup pool and bounded release; one gate per
repository; "The landing gate is never CPU-limited" (08_testing.md); "A job lives with its requester".

## 9. Migration, as ordered units, each proven on a busy host

The owner's ruling of 2026-10-26: a real optimization wins without an idle host. The design ends every unit with the
same measurement, run on the 18-core arm64 host while it is in normal use, against the **busy-host harness** of U0, and
lands a unit only if its row of the harness table is not worse than the previous unit's and its own acceptance holds.
The first scope builds no harness (decision 7), so it claims no measured result: each acceptance below marks the clauses
that need one.

The first scope is U1 (the Linux class cgroups), U3, U4, the pressure-gated start and the macOS ending of U5, and U7. U0
and U2 are not built. U6, and the rest of U5 (estimates, host budget, per-job ceilings), are not named by the scope
`[INFERENCE: the status names the pressure gate and no estimates or pools]`.

### U0 — Busy-host harness and baseline (not built)

- Work: a script (tooling, not product) that starts `B` background `cargo nextest run` jobs from `B` sheds of one
  repository (B = 3 and 6), one `interactive` probe loop (`cowshed exec true` every 2 s, recording latency), then after
  60 s one land gate (`nx run-many -t gate`). It records: gate wall time; background throughput (tests completed per
  minute, summed); interactive p50/p95; host load, PSI or pressure level, swap in use, per-class CPU time. Baseline
  rows: the current ledger (B = 3, 6) and an empty host (B = 0).
- Acceptance: the harness reproduces f5cff758e8's shape (gate wall ≥ 2× the B = 0 run under the current ledger with B
  = 6) — a harness that cannot show the problem cannot show the fix.

### U1 — Linux class cgroups

- Work: create the class cgroups under the delegated root with their `cpu.weight.nice` and, for `background` and
  `resident`, `memory.high` and `memory.swap.max` (§3's first table); create every job cgroup under its class's cgroup;
  enable the controllers those files and the job accounting need in the delegated root and the class cgroups; status
  reports PSI. No `io.weight` and no gate `memory.low` (decision 6); the rows of §3's second table wait for a unit that
  measures them. No ledger change yet.
- Acceptance: the diagnostics test `packages/cowshed/crates/cowshed-core/tests/job_cgroup_linux.rs` gains the
  weight/ceiling cases: a job's cgroup is created under its class's cgroup, and each class cgroup carries its
  `cpu.weight.nice` and, for background, `memory.swap.max` 0. Needs the harness, so not claimed in the first scope: the
  draft's bound, gate wall ≤ 1.1× the B = 0 run at B = 6 while background throughput stays > 0 throughout (weights do
  not starve), was set for weights of 100 : 10 and needs re-stating for the bands of decision 3; and `memory.swap.max=0`
  keeping swap at 0 during B = 6.

### U2 — macOS nice bands measured (not built)

- Work: not built. The draft ran the harness at background nice +10, +15 and +20, with the ledger still in place but
  granting `cores` to every job (a one-line constant, so the comparison isolates nice), and chose the band whose gate
  wall was lowest while background throughput stayed above a third of its +10 value. The owner fixed the bands (decision
  3: 0, +5, +10), so no choice remains for a measurement to make; whether E-core steering would help the gate stays
  unmeasured.

### U3 — Per-job jobserver with a constant count

- Work: the supervisor serves each job a fifo of `cores` (or its smaller ask) from its own pool; nested relays read the
  parent's fifo; the host endpoint `BOUNDED_EXEC_CPU_POOL` is no longer published by the gateway.
- Acceptance: the 2026-10-26 observation of §6 reproduced (a nextest whose tests each start a `cargo` child) completes
  with no child waiting on a host queue; the `cowshed-core` `job_cpu` tests are rewritten to the constant pool. Needs
  the harness, so not claimed in the first scope: harness rows unchanged or better.

### U4 — Delete the ledger

- Work: remove §8's list; execs spawn at once under class; `cowshed gateway status` becomes §7. The socket and ledger
  test suites are deleted with the code, not rewritten.
- Acceptance: needs the harness, so not claimed in the first scope: harness B = 6 on macOS: gate wall ≤ 1.25× B = 0 (the
  draft set the threshold from U2's measurement, stated in the commit; with the bands fixed it needs stating when the
  harness exists), interactive p95 ≤ 2× B = 0, no core idle for more than 5 s while any background job has runnable work
  (sampled from load / `cpu.stat`), and the f5cff758e8 reproduction (idle 17-thread nextest + gate) shows the gate
  within 10% of B = 0.

### U5 — Memory admission

- Work, in the first scope: the pressure-gated background start on both platforms (§5.1.3), the macOS ending at Critical
  (§5.2), and the Linux class ceilings of U1. Not in the first scope: estimates from job history, the host budget, the
  exec `memory` field, per-job ceilings, and the measurement of macOS per-process soft limits.
- Acceptance, in the first scope: a `background` start waits while the host reports memory pressure; on macOS at
  Critical the youngest `background` or `resident` job ends `failed` with failure `memory`, naming the level and its
  resident sum, and a `gate` or `interactive` job is never ended. Needs the harness, so not claimed: the 3607bc754e
  reproduction (six acceptance runs of one repo started at once) ends with swap in use ≤ 1 GB (the incident reached 5.9
  of 7 GB) and every run completing or ending with a stated `memory` failure; the gate started during it meets U4's
  bound; status shows the deferred starts with their estimates.

### U6 — I/O pools and the macOS I/O rule (not named in the first scope)

- Work: a fork/attach pool with depth `k` chosen by measuring fork latency p95 under B = 6 at k = 1, 2, 4; a Linux
  `io.latency` target for the gate from its B = 0 store latency p95; an audit (grep, tests) that no spawn path sets a
  disk policy below IMPORTANT, a QoS clamp or `PRIO_DARWIN_BG`.
- Acceptance: the 28385f7929 shape (release 55 images at once) keeps a concurrent `git status` p95 ≤ 2× its B = 0 value;
  fork p95 under B = 6 ≤ 2× B = 0.

### U7 — Specs and docs

- Work: 05_gateway.md (Host CPU budget rewritten to §§1–7 of this document), 11_shell.md, 07_api.md (`CpuBudgetStatus` →
  host pressure status, exec `memory` field), 08_testing.md (the harness as the proof of record), docs/cli.md,
  skills/cowshed/SKILL.md, docs/optimization-ledger.md (every U0–U6 row with its numbers, once the harness exists).
  Other text that states the token budget, found by search at main 33aaff9cc0 (search again at implementation time),
  follows: other paragraphs of 05_gateway.md (for example :261, :304, :375), `AGENTS.md` ("cowshed exec"),
  `packages/cowshed/docs/{README,gateway,troubleshooting}.md`, `packages/nx-plugin/README.md`,
  `specs/cowshed/{01c_macos,03_caches,04_sandbox,06_cli}.md`, and the briefs `agent-todo/cowshed-small-spec-gaps.md`
  (its "CPU budget through Cargo's jobserver" item is written against the ledger) and `agent-todo/Cowshed.md`.
- Acceptance: the ledger rows exist and name the host, date, B and the kernel reading each claim rests on (once the
  harness exists).

Order: U0 comes first, since every unit ends with its measurement. Units U1 and U2 are independent and run in parallel;
U3 precedes U4; U5 and U6 follow U4 and are independent of each other; U7 closes. U0 and U2 are not built in the first
scope.

## 10. What this gives up, said aloud

- The gate no longer has "admission order with no deadline" as its guarantee; it has a share from the moment it is
  runnable: its class's nice (+5). On Linux the gate class has about a third of the interactive class's share and about
  three times the background class's (§2), so interactive work runs ahead of it. On macOS that share is the Clutch
  scheduler's priority order with WCEL-bounded background progress, not a ratio cowshed can set.
- In the first scope memory admission is the pressure gate alone: it reacts to pressure the host already shows, so a
  burst of starts before the reading moves can overshoot `[INFERENCE]`. Where the estimate budget is built (not in the
  first scope), memory admission is estimate-based and can be wrong in both directions: too high defers work the host
  could run; too low lets the host reach Warning before starts stop. Decay toward measured usage and the pressure gate
  bound both errors, and status shows the estimate beside the measurement.
- A background job on macOS can be ended for memory, at Critical, the youngest first. It is told why, in numbers. A
  killed nextest does not resume; under the old ledger it would have been granted the memory and swapped the host
  instead (3607bc754e).
- Nothing in this design needs a privilege cowshed does not already hold: a delegated cgroup subtree on Linux (already
  required), `setpriority` on its own children on macOS (already done), and reads of `/proc/pressure/*`,
  `sysctl kern.memorystatus_vm_pressure_level`, rusage and cgroup files. (`io.weight`, which needed the root-only
  `io.cost.qos`, is out of scope.)

## Open inferences

### Marked `[INFERENCE]`

In the design:

- §3: the `pids.max` bound of 4 × cores × 64 is a guess "to be measured" (later; no unit measures it).
- §4.1 and §4.4: nice +10 (base priority 21) is non-UI and P-core-eligible, and nice +11 and beyond (priority ≤ 20) is
  utility class and E-core-preferred. The class mapping is verbatim from the XNU sources; the placement CLPC then
  chooses is not measured in the first scope (U2 is not built).

In this brief:

- §3: `interactive-resident`, which has interactive's nice, goes under the `interactive` class cgroup (the decision
  names four class cgroups).
- §3: the rows of the draft's per-job table wait; the decisions name the class-cgroup rows only.
- §5.1: the first scope builds the pressure gate and no estimates.
- §9: U6 and the rest of U5 are not named by the scope.
- §10: a burst of starts can overshoot a pressure-only gate.

The research marks more: that macOS thread-group membership is assigned by the kernel and cannot be set by a daemon for
its children (§2.1); that the honest form of the opportunistic tier on macOS is lower nice plus a memory ceiling, not a
freeze (§2.6); the P/E core split of an 18-core M-series (§2.8); whether macOS enables the over-soft-limit kill, and
which API sums a job tree's resident memory (§2.9); and that the lock inversion of 57b1c81bc0 also holds for Linux
`io.latency` (§2.10).

### Not stated by the design

Left open by the text and the decisions; each needs an answer, or an explicit decision not to answer, before the code
that depends on it is final.

1. **Values the first scope needs and no text sets.** The Linux memory-pressure threshold (§5.1.3 left it to a U5
   measurement, which is a non-goal); the background class's `memory.high` (it now bounds all background jobs together,
   so the draft's per-job "declared estimate × 1.5" does not carry over), and whether the `resident` class cgroup
   carries the same value.
2. **The task server beyond its resource fit.** Decision 5 deletes the fit. The task server's serialization classes
   (`cowshed-new`, `image`, `land-publication`, 05_gateway.md:1964) and the disk-lifecycle lease (05_gateway.md:1023),
   which already space disk work, are not named, and §1's "Nothing else waits" does not account for them.
   `[INFERENCE: they are ordering rules and stay.]`
3. **Values for the later budget.** `reserved_for_host` and the per-class default estimate (§5.1); the "host budget
   share" that §3's per-job table gives a background job as `memory.max`, which §5.1 never defines; the decay window of
   an estimate (Borg's is 300 s, for Borg's own reservation).

Beyond the decisions, this brief corrects three slips of the draft it was made from: §5.5 said that no other wait cause
exists, against the memory and pool causes of §7 (it now says what §1 and §7 say); U5 named a "7 GB host" where the
incident's 7 GB is swap (it now says what the incident measured); and §5.4 cited a paragraph of 05_gateway.md that
023682e1c4 deleted (it now cites the sections that state the same facts).
