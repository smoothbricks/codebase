# Work-queue and host-scheduling design: state of the art, mapped onto cowshed's incidents

Status: the research under a design that the owner approved on 2026-10-26 (`agent-todo/cowshed-os-scheduling.md`), which
is being implemented in a reduced scope. The research's recommendation (§4) was the input; where the design's decisions
differ from it, the design governs: the bands of §4 "Add" 1 are the owner's nice values 0 / +5 / +10 rather than weights
of 100 : 10, and `io.weight` and the gate's `memory.low` are out of scope.

Written 2026-10-26 for the owner's question of that day: "What is SOTA in work queue design? I feel like we reinvent
scheduling painfully for a dumb reason." Read-only research; every external claim carries a URL, every inference is
marked `[INFERENCE]`. Spec names without a directory are in `specs/cowshed/`, and code paths are relative to the
repository root. Line numbers are at main 33aaff9cc0.

## 0. The one-paragraph answer

The dumb reason is real and has a name. cowshed's gateway ledger is a **slot reservation system** (GNU-make-jobserver
tokens, granted whole at start, never shrunk, held until exit) that is being asked to do the job of a
**proportional-share scheduler** (give the gate most of the host when it wants it, let background fill the rest). Every
mature system separates the two: _admission_ is used only for resources that cannot be taken back without killing
(memory, disk space, a device, a lock), and _compressible_ resources (CPU time, I/O bandwidth) are left to a kernel
scheduler that arbitrates by weight every few milliseconds and is work-conserving by construction. Borg states this
split verbatim: compressible resources "can be reclaimed from a task by decreasing its quality of service without
killing it", non-compressible ones "generally cannot be reclaimed without killing the task", and the Borglet throttles
the former and kills for the latter ([Borg §6.2](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf)). cowshed
treats CPU as non-compressible (a token is "a runnable thread the holder is using now", 05_gateway.md "A resident holds
no token"), yet the ledger cannot see whether the thread is running; so an idle nextest holding 17 of 18 tokens at 0%
CPU for 50 minutes ([f5cff758e8](#incidents)) is not a bug in the policy, it is the policy. Six of the seven incidents
below are consequences of doing in userspace, without usage information, what the kernel does with it.

## 1. Our system, as the specs state it

- Host-wide CPU token ledger in the gateway; a job asks `want` tokens, is granted "all at once" or waits, and "a running
  grant cannot be shrunk: Cargo's implicit make token and libtest/nextest threads sized from the original grant do not
  return FIFO bytes" (05_gateway.md "Room for a gate").
- Gate class served FIFO first, with a starvation floor for background; beside gate work every background grant is one
  token (05_gateway.md "Gate admission", "Room for a gate"). Commit 023682e1c4 removed that one-token grant after this
  research was written: background is now limited only by the free tokens, and the floor stays (05_gateway.md "Room for
  a gate", :1518-1535).
- Top-ups from tokens nobody waits for; returns on exit (05_gateway.md "All at once").
- A resident holds no token (05_gateway.md).
- Nice bands: interactive 0 / gate +5 / background +10, placed by the supervisor and read back (05_gateway.md "Bands";
  11_shell.md "Server-owned CPU admission and nice placement").
- Quiet waits (fork/attach and image cleanup wait while a gate holds or waits) — removed by 023682e1c4, which landed
  after this research was written.
- The landing gate "is never CPU-limited": 40 min capped at `--parallel=2` under `taskpolicy -b` vs 4m44s uncapped, and
  "the failures were the cap, not the tests" (08_testing.md:766).
- Measured macOS QoS clamp cost, already in the tree: under a launchd `Background` clamp at load 80 a 0.7 s shell loop
  took 1–30 s, a supervisor took 115 s to answer, `direnv export json` took 537 s; under the `utility` clamp a cold 1 GB
  SQLite open took 34.5–54.5 s vs 2.4–2.7 s unclamped (`packages/cowshed/crates/cowshed-cli/src/launchd.rs:372-389`).

### Incidents

| id                      | date          | what was measured                                                                                                                                                                                                                        |
| ----------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| f5cff758e8              | 2026-10-19    | background nextest granted 17/18 tokens sat at 0% CPU for 50 min while a land gate ran on 1 token (35+ min vs usual 10–20†)                                                                                                              |
| fa47c27313              | 2026-10-22    | no gate running, background capped at 9/18; `leak-prevent` held all 9, three sheds waited, the other 9 tokens idled                                                                                                                      |
| 3607bc754e              | 2026-10-26    | agents missed a "quiet window": six acceptance runs of one repo†, load 23 on 18 cores, 5.9 of 7 GB swap in use†                                                                                                                          |
| 9752d00c7d              | 2026-10-26    | one gate job holding 1 token, asking nothing more; 17 background asks queued while 9/18 tokens sat free (floor check bug)                                                                                                                |
| 28385f7929              | 2026-10-25/26 | 55 detached image truncators ran at once (oldest 1h37m); APFS container saturated; `lsof`, `git`, forks sat in uninterruptible waits                                                                                                     |
| 57b1c81bc0              | 2026-10-22    | trash thread under IOPOL_THROTTLE: 1,213 throttled 4 KiB metadata reads at 20.1 ms median; unlink 53.1 s throttled vs 7.6 s normal; probe p50 10.6 ms vs 0.2 ms — a throttled thread held the container's work while its own reads slept |
| 2026-10-26 observation† | 2026-10-26    | a nextest holding 9 tokens at 0 CPU for 12 min (same class as f5cff758e8; per 05_gateway.md "Nested runners", tests waiting on children queued at the host)                                                                              |

† Not recorded in the commit message of its id (the 2026-10-26 observation has no commit): carried from the research as
written. Every other figure in the table is in the message of the commit named, and each id is a commit of this
repository.

## 2. The designs

### 2.1 Linux CFS → EEVDF, nice, cgroup v2 `cpu.weight` / `cpu.max`

**What it does.** The fair-class scheduler distributes CPU time among runnable entities in proportion to weight. Since
6.6 the picker is EEVDF: each task has a _lag_ (how far behind its ideal share it is) and a _virtual deadline_; the
scheduler runs the eligible task (lag ≥ 0) with the earliest deadline, and tasks can request a slice via
`sched_setattr()` ([docs.kernel.org/scheduler/sched-eevdf](https://docs.kernel.org/scheduler/sched-eevdf.html)). Nice is
multiplicative: each nice step is a fixed CPU ratio (two tasks one step apart split 55/45) and nice +19 gets ~1.5%
against nice 0 ([sched-nice-design](https://docs.kernel.org/scheduler/sched-nice-design.html)). cgroup v2 `cpu.weight`
(range 1–10000, default 100) "proportionally distributes CPU cycles to active children"; the doc defines weights as
_work-conserving_: "only the children which are actively using the resource at the moment participate in the
distribution" ([cgroup-v2 "Resource Distribution Models"](https://docs.kernel.org/admin-guide/cgroup-v2.html)).
`cpu.max` is the opposite model, an absolute quota per period — time not used is lost. `cpu.idle=1` makes a whole cgroup
SCHED_IDLE, "very low priority relative to its peers" while its threads keep their relative order
([cgroup-v2 CPU interface](https://docs.kernel.org/admin-guide/cgroup-v2.html)); SCHED_IDLE per task exists since 2.6.23
([sched(7)](https://www.man7.org/linux/man-pages/man7/sched.7.html)).

**Against our incidents.** With gate at weight 100 and background at weight 10 (or `cpu.idle`), f5cff758e8 cannot
happen: the idle nextest is not runnable and consumes nothing; when the gate becomes runnable it gets its weight share
at the next tick. There is no "grant" to shrink because nothing is granted. fa47c27313 cannot happen: weights are
work-conserving. 9752d00c7d cannot happen: there is no floor state to get wrong. 3607bc754e's CPU half (load 23 on 18
cores) is harmless under weights — gate threads win the cores — its memory half is not (see §2.9). Borg found exactly
this: batch tasks "are given tiny scheduler shares relative to the LS tasks", with CFS bandwidth control applied only
"selectively" to greedy LS tasks ([Borg §6.2](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf)).

**Cost on macOS.** No cgroups, so no per-job weight group. Per-process `nice` exists (§2.8) and XNU timeshares per
thread with bounded starvation, but there is no way to say "these 300 threads together are one weight-10 entity"; XNU's
own design doc names this as the flaw of thread-level accounting ("incentivizes creating more threads") that thread
groups fix inside Apple's runtime
([sched_clutch_edge.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/scheduler/sched_clutch_edge.md)).
`[INFERENCE]` thread-group membership is assigned by the kernel/CLPC (coalitions, apps, XPC relationships), not settable
by an arbitrary daemon for its children.

### 2.2 Borg, Omega, Autopilot

**What it does.** Borg has _priority bands_ (monitoring, production, batch, best-effort); "a high-priority task can
obtain resources at the expense of a lower-priority one, even if that involves preempting (killing) the latter", with
production-band tasks forbidden to preempt one another to stop cascades; _quota_ is admission, "not scheduling"; the
scheduler scans pending work "from high to low priority, modulated by a round-robin scheme within a priority"
([Borg §2.5, §3.2](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf)). The utilization story is _resource
reclamation_: the _limit_ is what the user asked, the _reservation_ "is set equal to the resource request (the limit);
after 300 s … it decays slowly towards the actual usage plus a safety margin", non-prod work is scheduled into the gap,
and "about 20% of the workload runs in reclaimed resources in a median cell"
([Borg §5.5](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf)). Segregating prod and non-prod would need 20–30%
more machines (Fig. 5). Omega replaced the monolithic scheduler with shared state and "lock-free optimistic concurrency
control" so several schedulers can place work at once
([Omega, EuroSys 2013](https://research.google.com/pubs/archive/41684.pdf)). Autopilot closes the loop by moving
container limits toward usage automatically
([Autopilot, EuroSys 2020](https://dl.acm.org/doi/pdf/10.1145/3342195.3387524)).

**Against our incidents.** f5cff758e8 and the 2026-10-26 nextest are the pre-reclamation Borg: feasibility by limit, and
"most tasks use much less than their limit". Borg's answer was not a cleverer limit policy; it was to measure usage and
lend the slack. fa47c27313 is Borg's segregation cost. 3607bc754e is handled by appclass + priority: LS work "capable of
temporarily starving batch tasks for several seconds", and memory overcommit resolved by killing non-prod first, never
prod.

**Cost on macOS.** Reclamation needs usage sampling, which exists (`proc_pid_rusage`, `host_statistics`) and a userspace
loop; it does not need cgroups. Preemption-by-kill of background test runs is what 05_gateway.md rejects ("a killed
nextest run cannot resume"). Borg's SIGTERM notice (~80% delivered) is the same compromise.

### 2.3 Kubernetes QoS, requests vs limits, priority/preemption, in-place resize

**What it does.** `requests.cpu` becomes `cpu.weight` (v2) / `cpu.shares` (v1) — a relative weight that matters only
under contention; `limits.cpu` becomes `cpu.max`, a hard quota that throttles
([kubernetes.io QoS task](https://kubernetes.io/docs/tasks/configure-pod-container/quality-service-pod/);
[cgroup mapping explained](https://victoriametrics.com/blog/kubernetes-cpu-go-gomaxprocs/)). QoS classes: Guaranteed
(requests == limits), Burstable, BestEffort; eviction under node pressure goes BestEffort → Burstable → Guaranteed
([pod-qos](https://v1-34.docs.kubernetes.io/docs/concepts/workloads/pods/pod-qos)). PriorityClass preemption evicts
lower-priority pods with their graceful-termination period
([pod-priority-preemption](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/)). In-place
pod resize changes CPU without restart, memory with a per-container restart policy — the exact "no grant shrinks"
problem, solved by making CPU resizable and memory optionally not
([resize-pod-resources](https://kubernetes.io/docs/tasks/configure-pod-container/resize-pod-resources)).

**Against our incidents.** Gate = Guaranteed-ish high PriorityClass, background = BestEffort: f5cff758e8, fa47c27313,
9752d00c7d vanish for the same reason as §2.1 (the node agent sets weights, the kernel runs them). 3607bc754e: memory
eviction order kills BestEffort first — the kernel-visible equivalent of "gate first".

**Cost on macOS.** Same as §2.1: weights need cgroups. Eviction order can be approximated with per-process jetsam limits
(§2.8/2.9).

### 2.4 Bazel and Buck2 local resource scheduling

**What it does.** Bazel runs at most `--jobs` actions, "but also by Bazel's scheduler, which tries to avoid running
concurrent jobs that will use up more resources (RAM or CPU) than are available, based on some (very crude) estimates of
the resource consumption of each job", tunable via `--local_resources=cpu=…,memory=…` (default HOST_CPUS and a fraction
of HOST_RAM) ([Bazel user manual](https://bazel.build/docs/user-manual)). Tests declare `cpu:N` (reserves N slots,
"doubles as a memory reservation"), `exclusive` (nothing else runs), custom `resources:gpu:1`, and `--local_test_jobs`
caps tests separately from build actions
([aspect.build test resources](https://aspect.build/docs/bazel/guides/test-resources)). Buck2 runs `-j` threads (default
cores) and its `LocalResourceInfo` is a _pool of instances_ (e.g. simulators): "the number of concurrently running tests
that require resources of the same type is limited by how many instances are in a list"
([Buck2 local resources](https://buck2.build/docs/rule_authors/local_resources/);
[buck2 build](https://buck2.build/docs/users/commands/build)).

**Against our incidents.** This is cowshed's model (static estimates, counted slots) applied _inside one build_, where
the owner knows the estimates and the whole machine is its own. It does not try to arbitrate between builds; a second
Bazel on the same host gets no protection. So it would have _caused_ f5cff758e8 and the 2026-10-26 nextest (a `cpu:17`
test idle in a syscall still holds 17 slots) and fa47c27313 (static split), and never prevents cross-build incidents.
Its memory estimate is the one part cowshed lacks and needs (§2.9): 3607bc754e's swap would have been refused at start.

**Cost on macOS.** None; it is pure userspace counting. That is also its ceiling.

### 2.5 GNU make jobserver and Ninja pools

**What it does.** The jobserver is a pipe/fifo "pre-loaded with one single-character token for each available job. To
obtain an extra slot you must read a single character … to release a slot you must write a single character back"; each
sub-make has an implicit token for itself
([make: POSIX Jobserver](https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html)). make also has
`-l LOAD`: "no new recipes should be started if there are other recipes running and the load average is at least LOAD"
([make options](https://www.gnu.org/software/make/manual/html_node/Options-Summary.html)) — a 1990s utilization-based
admission rule. Ninja: `pool` with `depth` bounds classes of edges ("No more than 4 links at a time"), "ninja will never
run more concurrent jobs than … `-j`", and since 1.13 it can be a jobserver _client_, with "-l load-average limitations
still enforced" ([Ninja manual](https://ninja-build.org/manual.html)). Cargo: "1 cargo to N rustc", "Cargo also must not
spawn more than N threads of work: the total amount of tokens … must always be limited to N", and the jobserver also
hands rustc extra tokens for codegen threads beyond each rustc's implicit token
([cargo job_queue docs](https://doc.rust-lang.org/stable/nightly-rustc/cargo/compiler/job_queue/index.html)). nextest:
`test-threads` (default num-cpus) and `threads-required` so a heavy test takes several slots; `test-groups` with
`max-threads` bound classes
([nextest threads-required](https://github.com/nextest-rs/nextest/blob/main/site/src/docs/configuration/threads-required.md);
[test-groups](https://github.com/nextest-rs/nextest/blob/main/site/src/docs/configuration/test-groups.md)).

**Against our incidents.** The jobserver's purpose is **thread-count bounding inside one build tree**: keep runnable
compiler threads ≈ cores so the kernel is not handed 10× oversubscription and the build's own memory stays bounded. It
has no notion of priority, usage, or other builds. Lifting it to a host-wide, cross-build, priority-bearing ledger is
the reinvention: f5cff758e8 and the 2026-10-26 nextest are its native failure mode (a token holder blocked in `read(2)`
or waiting on a child still owns the byte); 9752d00c7d is policy state the protocol never had. Ninja pools, however, are
the right model for 28385f7929: "no more than 1 image truncation at a time" is a pool of depth 1, which is what the fix
implemented. make `-l` is the cheapest honest admission signal for _starting_ background work and would have softened
3607bc754e.

**Cost on macOS.** Zero; fifos work (cowshed already does this). `-l` needs `getloadavg(3)`, present on macOS.

### 2.6 Sparrow, Apollo, Hydra/YARN opportunistic containers

**What it does.** Sparrow schedules sub-second tasks with no central state: batch sampling (probe `d·m` workers for an
`m`-task job) and **late binding** ("workers do not reply immediately to probes and instead place a reservation … when
this reservation reaches the front of the queue, the worker sends an RPC … requesting a task"), so a task binds to the
worker that is actually free, not the one that looked free; workers run "a fixed number of slots", keep one queue per
priority, and "Sparrow does not currently support preemption"
([Sparrow, SOSP 2013](https://people.csail.mit.edu/matei/papers/2013/sosp_sparrow.pdf)). Apollo (Microsoft, 20k-server
clusters) uses _tokens_ for capacity ("the right to execute a regular task, consuming up to a predefined amount of CPU
and memory") and adds **opportunistic tasks** that "fill in the slack left by regular tasks" while regular tasks keep
low latency; a wait-time matrix per server estimates when a slot frees
([Apollo, OSDI 2014](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-boutin_0.pdf)). Hydra federates
YARN subclusters and relies on _opportunistic containers_: "just like airlines overbook a flight … opportunistic
containers … ensure that idle resources are fully utilized. Since they are scavengers, they are preempted when
guaranteed work arrives"
([Azure blog on Hydra/YARN](https://azure.microsoft.com/en-us/blog/how-microsoft-drives-exabyte-analytics-on-the-world-s-largest-yarn-cluster);
[Hydra, NSDI 2019](https://www.usenix.org/system/files/nsdi19-curino.pdf)). DRF generalizes fairness to several resource
types (max-min over each user's dominant share)
([DRF, NSDI 2011](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-18.html)).

**Against our incidents.** Every one of these keeps _two tiers_: a guaranteed tier that is admitted by token and an
opportunistic tier that is never admitted, only run in slack and _preempted_ when guaranteed work arrives. cowshed
admits both tiers by token and preempts neither, which is why the gate waits for background to finish (f5cff758e8).
Sparrow's late binding is the cure for 9752d00c7d's class: bind at the moment capacity is real, never from a stale
count. Apollo's token = "CPU _and memory_ up to a bound" is the shape a memory admission should take (§2.9).

**Cost on macOS.** Opportunistic-tier preemption is cheap if the tier's unit is restartable (a cargo build is; a
40-minute nextest run is not, which is 05_gateway.md's objection). The preemption primitive on macOS is `SIGSTOP`, which
the spec rejects because wall-clock timeouts keep running during the freeze; `[INFERENCE]` the honest form on macOS is
lower nice (CPU) plus a memory ceiling, not a freeze.

### 2.7 Work stealing

**What it does.** Blumofe–Leiserson: each worker owns a deque, steals from a random victim when empty; expected time
`T1/P + O(T∞)` with space `O(P·S1)` ([JACM 1999](https://dl.acm.org/doi/10.1145/324133.324234)). Apple's guidance for
Apple silicon: `DispatchQueue.concurrentPerform` "uses a work-stealing algorithm", make iterations ≥ 3× core count so
"threads running on higher efficiency cores" do not finish early and strand the P/E imbalance, and "don't keep threads
active and idle"
([Tuning your code's performance for Apple silicon](https://developer.apple.com/documentation/apple-silicon/tuning-your-code-s-performance-for-apple-silicon)).
XNU's Edge scheduler does the kernel-side version: idle cores steal foreign/runnable threads from other clusters and
"stir-the-pot" swaps long-running threads between P and E so none straggles
([sched_clutch_edge.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/scheduler/sched_clutch_edge.md)).

**Against our incidents.** Work stealing is an intra-process scheduling discipline, not an inter-job one; it does not
address any listed incident directly. It matters for one thing: the host must not be sized by _counting threads_ (a
token per thread) when the runtime itself steals across cores; the right unit is runnable work, which only the kernel
sees.

**Cost on macOS.** Free; it is the native model.

### 2.8 macOS: QoS classes, nice, E-core placement, I/O throttling

**What it does.**

- _QoS classes_ (user-interactive, user-initiated, utility, background) map to scheduling buckets. The Clutch scheduler
  picks the root bucket by EDF with a per-bucket worst-case latency — FG 0, IN 37.5 ms, DF 75 ms, UT 150 ms, BG 250 ms —
  plus a "warp" that lets higher buckets jump the queue briefly (FG 8 ms … BG 0) and a starvation-avoidance window for
  low buckets; quanta are FG 10 ms … BG 2 ms
  ([sched_clutch_edge.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/scheduler/sched_clutch_edge.md)).
  So BG is not starved; it is bounded to run within 250 ms of becoming runnable even under full contention.
- _Priority bands_ (`osfmk/kern/sched.h`): BASEPRI_DEFAULT 31, BASEPRI_UTILITY 20, MAXPRI_THROTTLE 4,
  BASEPRI_USER_INITIATED 37, BASEPRI_FOREGROUND 47
  ([sched.h](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched.h)). `sched_prim.c` classifies
  for the performance controller by `base_pri`: ≤4 BACKGROUND, ≤20 UTILITY, ≤31 NONUI
  ([sched_prim.c](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched_prim.c)). POSIX nice
  subtracts from 31, so nice +10 → 21 (NONUI), nice +11 → 20 (UTILITY), nice +20 → 11 (UTILITY band, above the throttle
  line). `[INFERENCE]` from these constants: cowshed's background nice +10 sits one step above the utility boundary, so
  background work stays P-core-eligible; +11 or more would be classed utility.
- _E-cores_: "The default AMP scheduler policy is to run utility and bg threads on E-Cores only. Run-time policy
  adjustment unlocks ability of utility and bg threads to be scheduled based on run-time conditions"
  (`sched_amp_qos_max_parallelism`,
  [sched_amp_common.c](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched_amp_common.c)); Apple
  documents that "the system is more likely to run background tasks on lower performance cores"
  ([Tuning … Apple silicon](https://developer.apple.com/documentation/apple-silicon/tuning-your-code-s-performance-for-apple-silicon)).
  On an 18-core M-series with 4–6 E-cores `[INFERENCE: core split not measured here]` a utility/background-clamped build
  is confined to those cores — consistent with the 40 min vs 4m44s gate measurement in 08_testing.md.
- _nice vs. PRIO_DARWIN_BG_: `setpriority(PRIO_PROCESS, …)` lowers scheduling priority only; `PRIO_DARWIN_BG` /
  `PRIO_DARWIN_THREAD` sets "scheduling priority … to lowest value, disk and network IO are throttled"
  ([setpriority(2)](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setpriority.2.html)).
  `taskpolicy -b` is PRIO_DARWIN_BG; `-c utility|background|maintenance` is a QoS clamp; `-d/-g` set disk I/O policy;
  `-m` sets a per-process memory limit (MiB), `-j` a jetsam priority, `-P kill|throttle|suspend` a process-control
  action "upon resource exhaustion" ([taskpolicy(8)](https://manp.gs/mac/8/taskpolicy)).
- _I/O policy_: IMPORTANT (default), STANDARD, UTILITY, THROTTLE, PASSIVE. "If a throttleable request occurs within a
  small time window of a request of higher priority, the thread that issued the throttleable I/O is forced to a sleep
  for a short period"; large throttleable reads are split, large writes are not; the effective policy is "the lowest
  priority policy of the current thread and the current process"
  ([setiopolicy_np(3)](https://manp.gs/mac/3/getiopolicy_np)). The sleep is on the _issuing thread_, which is exactly
  57b1c81bc0: the thread slept while holding the APFS container's work.

**Against our incidents.** 57b1c81bc0 is _caused_ by this design when the throttled thread holds a shared lock; the man
page's own notes warn that throttled writes fill the track cache and stall "subsequent higher priority writes". The
launchd.rs measurements (537 s `direnv export`) show the same inversion at the QoS-clamp level: a clamped process also
has its I/O throttled, and the host's unclamped processes then wait on the locks it holds. So on macOS, lowering a
build's class must be CPU-only (nice) and never a clamp or PRIO_DARWIN_BG while it touches shared filesystem state. For
CPU alone, nice + the Clutch bounds give what the ledger tries to give: the gate at nice 0 outranks nice +10 threads,
and BG/UT buckets still progress within 150–250 ms. That is the kernel-side resolution of f5cff758e8 and fa47c27313 — no
token needed for a runnable-vs-blocked decision.

**Cost on macOS.** Per-process only, no group weight; thread-level accounting as noted; the clamp/throttle coupling
makes the "background" QoS class unusable for build work that holds locks (measured). Memory: see §2.9.

### 2.9 Memory admission: the non-preemptible resource

**What it does.** Linux cgroup v2: `memory.min` (hard protection), `memory.low` (best-effort protection: within it "the
cgroup's memory won't be reclaimed unless there is no reclaimable memory available in unprotected cgroups"),
`memory.high` ("throttle limit … processes of the cgroup are throttled and put under heavy reclaim pressure … never
invokes the OOM killer"), `memory.max` (hard limit; "the OOM killer is invoked in the cgroup"), `memory.reclaim` (ask
the kernel to shrink a group), plus `memory.pressure` PSI
([cgroup-v2 memory](https://docs.kernel.org/admin-guide/cgroup-v2.html)). PSI exposes `some`/`full` stall fractions for
cpu, memory and io system-wide and per cgroup ([psi.rst](https://docs.kernel.org/6.0/accounting/psi.html)); Facebook's
senpai "dynamically adjusts [a cgroup's] memory range … using psi memory pressure data" to find the true working set
([senpai](https://github.com/facebookincubator/senpai/blob/main/senpai.py)). Borg: memory is non-compressible; running
out means the Borglet "immediately terminates tasks, from lowest to highest priority"
([Borg §6.2](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf)). Bazel admits by estimated RAM
(`--local_resources=memory=`) ([Bazel user manual](https://bazel.build/docs/user-manual)). macOS: the memorystatus
subsystem tracks `memorystatus_available_pages`, keeps 210 jetsam bands, kills in ascending band order under
`kern.memstat_critical_mb`, and "we'll kill procs over their soft limit if we're under 15% available pages even if the
system is otherwise healthy"
([memorystatus.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/vm/memorystatus.md)); pressure levels
Normal/Warning/Critical are published via `DISPATCH_SOURCE_TYPE_MEMORYPRESSURE` and
`kern.memorystatus_vm_pressure_level`, Warning when non-compressed available memory falls under 50% of available,
Critical near the swap-unthrottle threshold
([memorystatus_notify.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/vm/memorystatus_notify.md)).
Per-process limits are set at spawn (`taskpolicy -m`, launchd JetsamProperties)
([taskpolicy(8)](https://manp.gs/mac/8/taskpolicy)).

**Against our incidents.** 3607bc754e (5.9/7 GB swap) is the only incident where _admission_ was the right tool, and it
is the one resource the ledger does not count. On Linux: background jobs in a slice with `memory.high` (throttle, not
kill) and the gate protected by `memory.low`, and swap pressure shows in `memory.pressure` before it hurts. On macOS: no
per-group accounting; what exists is per-process soft limits (jetsam kills the over-limit background process first under
pressure — `[INFERENCE]` the "over soft limit at <15% available" rule is documented for the subsystem generally; whether
macOS enables it must be measured), the pressure level as a _start_ gate (do not start new background jobs at Warning or
above), and Bazel-style estimates per job class for the start decision.

**Cost on macOS.** Admission by estimate + pressure signal + per-process soft limits is all userspace and cheap; what is
missing is group accounting (a job tree's total RSS must be summed by the gateway from `proc_pid_rusage`-class calls)
`[INFERENCE: API choice]`.

### 2.10 Disk I/O scheduling

**What it does.** Linux cgroup v2 `io` controller: `io.weight` proportional control via iocost (a device cost model with
auto-tuned QoS: "will consider the device saturated if the 95th percentile of read completion latencies is above 75ms or
write 150ms, and adjust the overall IO issue rate"), `io.latency` ("work conserving; so as long as everybody is meeting
their latency target the controller doesn't do anything. Once a group starts missing its target it begins throttling any
peer group that has a higher target"), and `io.max` absolute bps/iops
([cgroup-v2 io](https://docs.kernel.org/admin-guide/cgroup-v2.html)). Writeback attribution needs filesystem support
(ext4, btrfs, xfs, …). macOS has only the tiered throttle policies of §2.8 and no proportional control.

**Against our incidents.** 28385f7929 (55 truncators) on Linux: io.latency on the gate's cgroup would throttle the
cleanup cgroup once gate I/O latency slipped — but only at bio issue; metadata locks inside the filesystem are still
held by whoever is in the critical section
`[INFERENCE: same inversion class as 57b1c81bc0, documented for Linux only indirectly via the writeback section]`. The
robust fix is the one taken: bound the work unit (1,024-extent cuts, measured ~0.1 s) and serialize it (depth-1 pool).
57b1c81bc0 on macOS: the fix is to stop using THROTTLE for lock-holding work, as done.

**Cost on macOS.** No proportional I/O controller; serialization pools and bounded work units are the only tools, and
they are enough for the two measured cases.

## 3. Incidents × designs

Legend: **P** prevents; **C** would cause or reproduce; **–** does not address; **p** partial (reason in notes).
Columns, in the order of the §1 table: f5cff758e8 idle 17/18 tokens; fa47c27313 half host idle; 3607bc754e load 23 +
swap; 9752d00c7d floor bug; 28385f7929 55 truncators; 57b1c81bc0 IOPOL inversion; 2026-10-26 nextest holding 9 tokens at
0 CPU.

| design                                                    | f5cff758e8 | fa47c27313 | 3607bc754e | 9752d00c7d | 28385f7929 | 57b1c81bc0 | 2026-10-26 | notes                                                           |
| --------------------------------------------------------- | ---------- | ---------- | ---------- | ---------- | ---------- | ---------- | ---------- | --------------------------------------------------------------- |
| Linux EEVDF + cgroup `cpu.weight` / `cpu.idle`            | P          | P          | p          | P          | –          | –          | P          | CPU half of 3607 only; no I/O or memory                         |
| cgroup `cpu.max` quota                                    | C          | C          | –          | –          | –          | –          | C          | non-work-conserving; same stranding as tokens                   |
| Borg (priority + reclamation + compressible split)        | P          | P          | P          | P          | –          | –          | P          | reclamation = measured usage lends slack; memory kills non-prod |
| Kubernetes QoS + PriorityClass preemption                 | P          | P          | P          | P          | –          | –          | P          | needs cgroups; preemption kills                                 |
| Bazel/Buck2 local resources                               | C          | C          | p          | –          | –          | –          | C          | memory estimate helps 3607; slots strand like tokens            |
| make jobserver / Cargo / nextest threads                  | C          | C          | –          | –          | –          | –          | C          | correct inside one build; wrong across builds                   |
| make `-l` / Ninja `-l` load admission                     | p          | p          | p          | –          | –          | –          | p          | delays _starts_ by measured load; no priority                   |
| Ninja pools (depth)                                       | –          | –          | –          | –          | P          | –          | –          | depth-1 pool is the shipped fix                                 |
| Sparrow late binding + per-priority queues                | –          | –          | –          | P          | –          | –          | –          | bind on real capacity, never a stale count                      |
| Apollo/Hydra two-tier (tokens + opportunistic, preempted) | P          | P          | p          | P          | –          | –          | P          | preemption of the opportunistic tier                            |
| Work stealing                                             | –          | –          | –          | –          | –          | –          | –          | intra-process only                                              |
| macOS nice (no clamp, no DARWIN_BG)                       | P          | P          | p          | P          | –          | –          | P          | Clutch EDF bounds BG/UT to 150–250 ms; CPU only                 |
| macOS QoS clamp / PRIO_DARWIN_BG / IOPOL_THROTTLE         | p          | p          | –          | –          | C          | C          | p          | measured: 537 s `direnv`, 50× slower unlink; inversion          |
| Linux memory.low/high/max + PSI                           | –          | –          | P          | –          | –          | –          | –          | the non-compressible resource                                   |
| macOS jetsam soft limits + pressure level                 | –          | –          | p          | –          | –          | –          | –          | start gate + kill order; group RSS must be summed in userspace  |
| Linux io.latency / io.weight                              | –          | –          | –          | –          | p          | p          | –          | throttles at issue; lock inversion still possible               |
| Bounded I/O units + depth-1 serialization                 | –          | –          | –          | –          | P          | P          | –          | shipped in 28385f7929 / 57b1c81bc0                              |

## 4. Recommendation

**Keep.**

1. The per-build jobserver fifo (make/Cargo/nextest protocol) as _parallelism bounding inside one build tree_, sized to
   cores. This is what the protocol is for and every build tool speaks it.
2. The class vocabulary (gate / interactive / background / resident) and the rule that a resident holds nothing. Classes
   become kernel weights, not ledger priorities.
3. One gate per repository (ordering), and "the landing gate is never CPU-limited" (08_testing.md) — now literally true:
   the gate runs at nice 0 / top weight with no cap of any kind.
4. Bounded I/O work units with depth-1 serialization for container-saturating operations (28385f7929, 57b1c81bc0). These
   are Ninja pools; name them so.
5. Nice placement with read-back and typed refusal (11_shell.md) — the mechanism is right; only the values and the
   surrounding ledger change.

**Delete.**

1. The host-wide CPU token ledger: grants, all-at-once, top-up, return, starvation floor, half caps, `GateWork`,
   `CpuWaitCause`, probation, `background_limit`/`behind_limit`/`floor_open`. Every one of these is a userspace
   re-derivation of a decision the kernel makes with information the ledger does not have (runnable vs blocked). The
   incidents f5cff758e8, fa47c27313, 9752d00c7d and the 2026-10-26 observation are all ledger-only failure modes.
2. Quiet waits (since removed by 023682e1c4): a fork/attach contends on I/O, and I/O is handled by pools, not by waiting
   for a CPU-class event.
3. `taskpolicy -b`, QoS clamps and IOPOL_THROTTLE for any work that touches the store (measured inversions).

**Add.**

1. Kernel-arbitrated CPU classes. Linux: one cgroup per job under a delegated cowshed subtree, `cpu.weight` gate 100 /
   interactive 100 / background 10 (or `cpu.idle=1` for background), no `cpu.max` anywhere. macOS:
   `setpriority(PRIO_PROCESS)` only — gate 0, interactive 0, background a value chosen by measurement between +10 (stays
   NONUI/P-core-eligible) and +11…+20 (utility class, E-core-preferred by default). Which is better for _gate_ latency
   is an experiment, not a guess: 08_testing.md's 40 min vs 4m44s came from a clamp plus `--parallel=2`, not from nice.
2. Memory admission — the one admission that stays. A job class carries a per-job memory estimate (Bazel-style) and the
   host a budget; background starts are refused or deferred at memory pressure (Linux: PSI `memory some avg10` /
   `memory.pressure`; macOS: `kern.memorystatus_vm_pressure_level` ≥ Warning), and background jobs get a soft ceiling
   (Linux `memory.high`; macOS per-process jetsam soft limit) so the kernel reclaims or kills _them_ first. This handles
   3607bc754e, the incident the token ledger never saw.
3. Load-aware starts for background (make `-l` semantics): do not _start_ a new background build while 1-minute load ≥
   k·cores; this is a start-rate limiter, not a grant, and it needs no state.
4. Measured, not counted, utilization in `gateway status`: per-class CPU time, RSS sum, PSI/pressure level — the system
   must be able to say what it is doing. Today it reports tokens, which 9752d00c7d shows can be wrong while the host
   idles.

**Why this is not just "trust the kernel".** The kernel is trusted for the compressible resources it schedules every
tick (CPU, and on Linux I/O). Admission is kept exactly where every cited system keeps it: non-compressible memory,
device-saturating I/O units, and per-build thread counts. The gate's guarantee changes from "admission order with no
deadline" to "weight share from the moment it is runnable" — which is what the owner asked for on 2026-10-17 ("needs
more tickets than background but not fully starve background", `05_gateway.md` "Gate admission"): a weight ratio _is_
that sentence.

## 5. Sources

- Linux: [sched-eevdf](https://docs.kernel.org/scheduler/sched-eevdf.html),
  [sched-nice-design](https://docs.kernel.org/scheduler/sched-nice-design.html),
  [cgroup-v2](https://docs.kernel.org/admin-guide/cgroup-v2.html),
  [psi](https://docs.kernel.org/6.0/accounting/psi.html),
  [sched(7)](https://www.man7.org/linux/man-pages/man7/sched.7.html),
  [senpai](https://github.com/facebookincubator/senpai/blob/main/senpai.py)
- Google: [Borg](https://john.e-wilkes.com/papers/2015-EuroSys-Borg.pdf),
  [Omega](https://research.google.com/pubs/archive/41684.pdf),
  [Autopilot](https://dl.acm.org/doi/pdf/10.1145/3342195.3387524)
- Kubernetes: [QoS](https://kubernetes.io/docs/tasks/configure-pod-container/quality-service-pod/),
  [pod-qos](https://v1-34.docs.kubernetes.io/docs/concepts/workloads/pods/pod-qos),
  [priority/preemption](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/),
  [in-place resize](https://kubernetes.io/docs/tasks/configure-pod-container/resize-pod-resources)
- Build tools: [Bazel user manual](https://bazel.build/docs/user-manual),
  [Bazel test resources](https://aspect.build/docs/bazel/guides/test-resources),
  [Buck2 local resources](https://buck2.build/docs/rule_authors/local_resources/),
  [Buck2 build](https://buck2.build/docs/users/commands/build),
  [make jobserver](https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html),
  [make options](https://www.gnu.org/software/make/manual/html_node/Options-Summary.html),
  [Ninja manual](https://ninja-build.org/manual.html),
  [cargo job_queue](https://doc.rust-lang.org/stable/nightly-rustc/cargo/compiler/job_queue/index.html),
  [nextest threads-required](https://github.com/nextest-rs/nextest/blob/main/site/src/docs/configuration/threads-required.md),
  [nextest test-groups](https://github.com/nextest-rs/nextest/blob/main/site/src/docs/configuration/test-groups.md)
- Cluster schedulers: [Sparrow](https://people.csail.mit.edu/matei/papers/2013/sosp_sparrow.pdf),
  [Apollo](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-boutin_0.pdf),
  [Hydra](https://www.usenix.org/system/files/nsdi19-curino.pdf),
  [Azure on opportunistic containers](https://azure.microsoft.com/en-us/blog/how-microsoft-drives-exabyte-analytics-on-the-world-s-largest-yarn-cluster),
  [DRF](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-18.html),
  [work stealing](https://dl.acm.org/doi/10.1145/324133.324234)
- macOS/XNU:
  [sched_clutch_edge.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/scheduler/sched_clutch_edge.md),
  [sched_amp_common.c](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched_amp_common.c),
  [sched.h](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched.h),
  [sched_prim.c](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/sched_prim.c),
  [memorystatus.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/vm/memorystatus.md),
  [memorystatus_notify.md](https://github.com/apple-oss-distributions/xnu/blob/main/doc/vm/memorystatus_notify.md),
  [setpriority(2)](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setpriority.2.html),
  [setiopolicy_np(3)](https://manp.gs/mac/3/getiopolicy_np), [taskpolicy(8)](https://manp.gs/mac/8/taskpolicy),
  [Tuning for Apple silicon](https://developer.apple.com/documentation/apple-silicon/tuning-your-code-s-performance-for-apple-silicon)
- cowshed: `specs/cowshed/05_gateway.md`, `specs/cowshed/08_testing.md:766`, `specs/cowshed/11_shell.md`; commits
  f5cff758e8, fa47c27313, 3607bc754e, 9752d00c7d, 28385f7929, 57b1c81bc0;
  `packages/cowshed/crates/cowshed-cli/src/launchd.rs:372-389`
