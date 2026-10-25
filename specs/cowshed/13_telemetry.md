# Telemetry

cowshed's observability is **distributed tracing into Arrow columns**, not a text logfile. Every lifecycle operation,
every job, and every gateway request is a span; spans carry W3C trace context across cowshed's boundaries; and the spans
flush as Arrow record batches that are queryable, assertable, and cheap to retain. The substrate is **lmao**
(`packages/lmao`) — a spans-first tracer with deterministic Arrow encoding, arena-backed buffers, and a tracer-agnostic
query surface (`lmao-query`). There is no OTel collector and no telemetry daemon; OTLP export, if ever wanted, is a
projection from the Arrow store.

## Why lmao, not text logs

Text logs record _that_ things happened. Columns make cowshed's behavior a **dataset**: the same artifact answers
debugging (span waterfalls), security (audit joins), and fleet ops (SLOs from real usage). lmao is the right substrate
specifically because it is trace-first and **deterministic** — with an injected `Clock` and `Entropy` it emits
bit-identical trace bytes for a given `(build, seed, config)` (see `packages/lmao`).

**Dependency honesty**: cowshed does not depend on lmao's crates. One `cowshed-core` module writes lmao's Arrow trace
schema — the system-column prefix, the entry-type dictionary and sealed-segment publication — byte-for-byte aligned with
`lmao-arrow`'s, because that crate cannot be imported without its `lmao-core` runtime. The gateway's request spans and
the supervisor's job spans both use that module; neither keeps a copy. Nothing in cowshed queries traces with
`lmao-query`.

**Job span segments.** The workspace supervisor writes each job's span with the trace context the controller minted or
adopted for it (`JobInfo.trace`): a `span-start` row at admission, the `job.unattributed` row below when its terminal
sample states unattributed usage, and a `span-ok`/`span-err` row at its terminal state, each as its own sealed segment
`job-<order:020>-<writer>.arrow` under `<host-telemetry-root>/<yyyy-mm-dd>/`, sealed like the gateway's (create-new
`0600` temporary, fsync, rename, directory fsync). The rows carry `repo_id`, `workspace_incarnation`, `job_id`,
`grant_revision`, the job's W3C span id and, on the terminal row, its state. The span's lmao address is span 1 of the
thread derived from the durable key `(repo_id, workspace_incarnation, job_id)` (the first eight bytes of a
domain-separated BLAKE3), never the workspace-local `job_id` alone: jobs of two workspaces under one adopted trace
context stay distinct spans. The writes run off the supervisor's actor; a refused write is counted in the supervisor's
trace health, which every supervisor handle reads, in process or over its socket, and never fails or delays the job.
This job span is the anchor `process.run` spans parent from. The terminal row carries the job's `env_hash` (the same
environment identity as the job record's `envHash`, 11_shell.md).

## Trace context propagation

cowshed uses W3C trace context (`traceparent`). Every entry point **mints or adopts**:

- **CLI** — adopts an inbound `TRACEPARENT` from the caller's environment if present (agent harnesses and CI often run
  inside a trace already), else mints a fresh root.
- **Rust API** (`cowshed-core`) — every request struct carries an explicit `TraceContext`; the coordinator propagates
  one per task (07_api.md).
- **MCP** — tool calls carry trace context in `_meta` (12_mcp.md).
- **CI** — derives the trace id **deterministically from `(run_id, attempt)`** (10_ci.md), so a job's trace is findable
  from the GitHub UI with no registry, matching lmao's determinism ethos.

Propagation across cowshed's own boundaries:

- The shell supervisor **injects `TRACEPARENT` into each job's environment** (04_sandbox.md exec pipeline). The
  control-channel `run` message carries the traceparent (11_shell.md protocol), so a job's span parents to the exec that
  launched it — and any lmao-instrumented tool inside the job continues the same trace with no cowshed plumbing.
- **Job spans** carry `repo_id`, `workspace_incarnation`, and workspace-local numeric `job_id` beside standard lmao
  `trace_id` / `thread_id` / `span_id` / `parent_thread_id` / `parent_span_id`, `grant_revision`, and `env_hash`
  (11_shell.md). `job_id` joins the span to job control and protected evidence representation-transparently; it promises
  no per-job file path and never substitutes for `span_id`. The durable key is
  `(repo_id, workspace_incarnation, job_id)` (camelCase `(repoId, workspaceIncarnation,jobId)`), so copied checkpoint
  history remains distinct from jobs submitted in a later incarnation.
- **Deferred work** — `rm` teardown, `gc` completing interrupted cleanup, autosave ticks (02_workspaces.md) — persists
  the originating traceparent in the trash entry / grants sidecar and continues as a **deferred span**; a causal (not
  parental) relation to the originating trace is a span **link**, not a parent.

**lmao TRACEPARENT convention**: a tracer adopts `TRACEPARENT` at init and stamps outbound `fetch`/HTTP requests with
it. This is what upgrades a first-party tool's traffic from tier-2 to tier-1 attribution (below).

## Span taxonomy

- **Lifecycle spans.** `cowshed fork`'s steps (02_workspaces.md) are the canonical waterfall — clone, attach, fsck,
  marker, port-block, CA, grants, direnv-trust, branch — and they map 1:1 onto the 08_testing.md performance budgets, so
  a budget regression is a span that got slower, findable directly.
- **Job spans.** Every `bash`/exec is a span (11_shell.md); its children (an lmao-instrumented build, a dev server)
  parent into it via the injected `TRACEPARENT`.
- **Gateway request spans.** Each mirror fetch and intercepted request is a span (05_gateway.md), workspace-attributed
  by port, with the upstream leg as a child span.
- **CoW-lineage links.** The in-image marker gains `createdTrace` (01_storage.md); `fork` links the child's trace to the
  source workspace's trace, `restore` links to the checkpoint's, `checkpoint` links to the workspace's. The clone graph
  becomes a queryable provenance graph — from any gateway denial you can walk back to which task created this workspace
  from which state of main.
- **Grant-mutation spans.** Each `grant`/`revoke` (04_sandbox.md) records the trace that caused it, so
  `revision × trace` answers "**why** does this workspace hold this grant" — the task, not just the timestamp.
- **The escalation loop as one trace.** The exit-6 negotiation (12_mcp.md) — denial span (with the EPERM evidence,
  04_sandbox.md) → worker→coordinator report → `grant` span (revision bump) → retry exec — is a single trace instead of
  four disconnected log lines across three files.

## Native image observations

The APFS host records `cowshed.image.seed_capture`, `cowshed.image.clone` and `cowshed.image.retire` spans, plus `warn`
rows when an image exceeds a budget. They are unconditional stderr diagnostics **and** Arrow rows in
`<store>/telemetry/<yyyy-mm-dd>/image-<uuid>.arrow`, encoded by the existing `trace_columns` and `trace_segment` owners.
These are host-local traces with unique trace/thread identities, not attributes relayed through `StepReport` into a
caller's trace.

Each capture observes both the source and the destination. An absent destination starts at zero; its delta describes the
newly named clone's allocation, **not** newly allocated unique blocks. A release (a build image's, a clean removal's
source image) compares the image at its name before it with what is left there once it answers: a release commits the
whole image to its deleting directory before it answers, so the image is `absent` at its name after and its delta reads
as everything it held, though a rename frees no extents or allocated bytes: a background deletion frees them later
(05_gateway.md, "Deleting released images"). A release walks no extent map: it reads the allocation alone, and its
extent columns are null with the cause "a release does not walk its image's extent map". Its commit never opens the
image (05_gateway.md), and a released build image's map runs to 880,000 extents, which took 2.5-10.6 s per image to walk
inside `rm` (measured 2026-10-25), before the verb could answer. A recovery retirement compares the canonical file with
the renamed file in trash: a rename frees no extents or allocated bytes. A fork's capture of its source's seed
(16_build_volumes.md, "Fork" step 3) runs under the source's image lock, so it too reads allocation alone: its extent
columns are null with the cause "a capture under its source's image lock does not walk an extent map"; the clone's map
is the seed's, which the seed's own capture walked. Missing files are explicitly `absent`; failed measurements are null
with their cause, never zero. The extent count comes from `storage/apfs/extents.rs::count_extents`, the existing native
`F_LOG2PHYS_EXT` walker. Allocation reuses that module's `allocated_bytes` (`st_blocks * 512`). Neither measures the
mounted filesystem's contents, shared-block exclusivity, or bytes written.

The custom columns are:

- Identity: `operation`, `image_role` (`source`, `destination`, `image`), `image_path_before`, `image_path_after`,
  `image_state_before`, `image_state_after`, `process_id`.
- Layout: `extents_before`, `extents_after`, `extents_delta`, `allocated_bytes_before`, `allocated_bytes_after`,
  `allocated_bytes_delta`. Deltas are signed after-minus-before values.
- Time: `elapsed_us` covers only the operation; `measurement_us` reports the separate cost of reading both boundaries.
  The span's start/end timestamps cover the operation, not counting or sealing.
- I/O: `process_io_write_bytes_before`, `process_io_write_bytes_after`, `process_io_write_bytes_delta`,
  `process_io_scope`, `process_io_error`. The existing macOS process sampler reads
  `proc_pid_rusage(RUSAGE_INFO_V4).ri_diskio_byteswritten` immediately around the operation. Both log and trace carry
  the literal scope: **"process-window, includes concurrent threads, excludes children and later async writes"**. It is
  neither image-exclusive nor an upper bound. The source and destination rows of one capture repeat the same process
  window; do not sum them. `RUSAGE_CHILDREN` cannot see an in-process write.
- Failures: `measurement_error_before`, `measurement_error_after`, `error` (operation failure). A measurement or
  publication failure never changes the operation's result.
- Warning policy: `warning_reason`, `extents_warn`, `extents_delta_warn`, `allocated_bytes_warn`,
  `allocated_bytes_delta_warn`.

Absolute extent counts **at least 100,000** warn independently of growth **at least 10,000**. The measured
435,459-extent, 281 GB image spent 60.8 seconds in first write; these budgets warn at 23% of that extent count or a
single-operation increase of 2.3%. They are early-warning budgets, not a linear time prediction (another observed first
write took 10.6 seconds). Allocation separately warns at **128 GiB absolute** or **8 GiB growth**, approximately 49% and
3.1% of that 281 GB image. A high baseline warns before work begins even when the delta is zero. Shrinking does not
trigger a growth warning. Each warning names the signal and the thresholds.

One lazily started writer belongs to each native host. Its queue admits at most **4,096 operations or 16 MiB of retained
Rust payload capacity, whichever binds first**, including observations the writer has received but not yet sealed. The
operation limit can absorb 4,096 arrivals in one ten-second window, rather than discarding a busy interval after 64
arrivals. The independent byte limit accounts for variable-length image paths and errors: each observation counts its
row/vector storage and every string's reserved capacity, without serializing a second copy. Allocator, channel
bookkeeping and temporary Arrow encoding storage are outside this payload bound. Admission refuses and increments a
failure count rather than waiting for capacity; the diagnostic names the operation or byte limit that refused it.
Draining the channel does not make the interval buffer unbounded. Sealing runs on the ten-second `PROCESS_SEAL_INTERVAL`
alone, regardless of operation count, with a final flush on host drop. A full queue drops detail with a counted refusal;
it never triggers an early seal. A count-triggered seal would make full-drive flush rate grow with activity, recreating
the defect telemetry coalescing fixes.

This is **per writer**, not per partition: a fork's observations share a seal, not one `F_FULLFSYNC` per observation.
Host drop closes the queue and joins the final flush, preserving the last accepted operation. Queue refusal, spawn
failure and seal failure report a cumulative failed-observation count on stderr. A crash may lose the unsealed window,
as with process telemetry. Before/after rows are published together after the operation; live waits remain visible in
the existing lifecycle start diagnostics.

Use the ordinary reader to inspect a trace (the date is the batch's first operation's UTC date):

```sh
python /path/to/lmao-q.py --dir "$STORE/telemetry/$DAY" traces
python /path/to/lmao-q.py --dir "$STORE/telemetry/$DAY" trace <trace-id>
```

For an image's changes over a night, use that reader's existing loader, not a second Arrow decoder:

```sh
python - /path/to/lmao-q.py "$STORE/telemetry/$DAY" "$IMAGE" <<'PY'
import runpy, sys
from pathlib import Path
q = runpy.run_path(sys.argv[1])
rows = q["load_rows"](Path(sys.argv[2]))
rows = rows[rows["message"].str.startswith("cowshed.image.", na=False)]
rows = rows[rows["entry_type"].isin(["span-ok", "span-err"])]
rows = rows[(rows["image_path_before"] == sys.argv[3]) | (rows["image_path_after"] == sys.argv[3])]
print(rows.sort_values("timestamp")[[
    "timestamp", "operation", "image_role", "extents_before", "extents_after", "extents_delta",
    "allocated_bytes_before", "allocated_bytes_after", "allocated_bytes_delta", "elapsed_us",
    "measurement_us", "process_io_write_bytes_delta", "process_io_scope", "warning_reason"
]].to_string(index=False))
PY
```

## Process-tree spans

The workspace supervisor records one `process.run` LMAO span for every process owned by a job, even a short-lived
descendant. Span start/end are its observed birth/exit. Within the job's tree the parent span is the observed ppid edge;
a root process hangs from the job's trace context. No extra parent column restates span parentage. The same
identity-fenced process records reach the controller protocol and N-API through the single declaration in 07_api.md.

Every process span and its rows share exactly this small fixed custom column set:

| Column               | Type         | Meaning                                                                |
| -------------------- | ------------ | ---------------------------------------------------------------------- |
| `proc_pid`           | `u32`        | Observed process PID, fenced by its retained birth identity.           |
| `proc_program`       | `S.category` | Program; repeated values are dictionary-encoded.                       |
| `proc_argv`          | `S.text`     | Deterministic argv display written once, on the span's start row only. |
| `cpu_user_us`        | `u64`        | Cumulative own-process user CPU microseconds.                          |
| `cpu_sys_us`         | `u64`        | Cumulative own-process system CPU microseconds.                        |
| `rss_bytes`          | `u64`        | Current resident bytes.                                                |
| `rss_peak_bytes`     | `u64`        | Peak observed resident bytes.                                          |
| `io_read_bytes`      | `u64`        | Process I/O read bytes.                                                |
| `io_write_bytes`     | `u64`        | Process I/O write bytes.                                               |
| `exit_code`          | `i32`        | Ordinary exit code, set on the terminal row when applicable.           |
| `exit_signal`        | `S.enum`     | Exact terminating signal, set only for a signaled exit.                |
| `blocked_on`         | `S.enum`     | Observed `none                                                         | lock | socket | pipe | child | stdin | disk`; unobserved is null. |
| `blocked_path`       | `S.category` | Observed blocker path, when the blocker has one.                       |
| `blocked_holder_pid` | `u32`        | Evidence-backed lock-holder PID, when known.                           |

Samples become log rows only on a state/blocker transition, an RSS crossing of a 2× step, or a busy/idle CPU flip, plus
one heartbeat row a minute for each unchanged live process. API sampling/subscriber intervals do not set this cadence;
changed-state and terminal rows are immediate. A row sets only the columns that changed; unchanged columns are null, not
repeated payload. The terminal row carries the final usage and exact exit. A blocker transition with no other changed
value still creates a row. Program changes at exec use `proc_program`; argv remains a single start-row display, while
the typed process event retains the exact command arguments. Display escaping is deterministic and does not turn
arbitrary Unix argv bytes into lossy UTF-8 or a JSON string column. A CPU busy/idle flip uses a fixed standard row
kind/template, not a fifteenth custom column. Sparse API deltas distinguish unchanged, SET, and CLEAR; the row's
transition kind preserves clearing semantics without carrying a JSON payload or adding columns.

**Process segments.** The life the job's fold retained at birth-order index `i` is span `i + 2` of the job's lmao
thread, beside the job span (span 1), and every row of it carries its parent's address. The rows of every batch of
events the fold emits are projected and gathered by the job's writer, which seals what it gathered as one segment
`process-<order:020>-<writer>.arrow` beside the job's segments, by the same order and sealing, once 10 seconds have
passed since the first of them; it also seals what it gathered before a job row, before answering a trace-health read,
and when it stops. No count of rows seals early: the interval is the seal rate and the gathered rows are the buffer, and
the two are not interchangeable. Rows past 16,384 gathered in one interval are refused and counted in the trace health,
so a burst loses rows rather than flushing the drive more often because a lot happened. A process fold batch is not a
durability event: each seal is a full-drive flush of the segment and of its directory, and one seal per batch made 26
seals and about 52 `F_FULLFSYNC`s a second host-wide, 5-8 s of flushing per wall second at 0.10-0.16 s each, saturating
the drive under every other process on it (measured 2026-10-21; the same 0.7 s p99 metadata-read latency then showed on
the store disk and on a build-volume image alike). Each seal keeps its durability; rows still gathered when the writer's
process dies are lost, as the gateway's one-batch audit window is ("Batched audit flush accepted"). An event the
projection cannot place is counted in the trace health and adds no row. A row's kind is its lmao entry type and a fixed
message: `span-start` and `span-ok`/`span-err` named `process.run` at the observed birth and exit (`span-ok` only for a
clean exit with status 0; an exit whose status the kernel withheld ends `span-err` with both exit columns null), and
`info` rows `process.exec`, `process.busy`, `process.idle`, `process.usage` (a first read or an RSS step),
`process.heartbeat`, `process.blocked` and `process.blocker-cleared`. A `process.blocked` row is a SET that states
`blocked_on`, `blocked_path` and `blocked_holder_pid` whole, so a null path or holder there is absent, never unchanged;
`process.blocker-cleared` is a CLEAR. A span starts idle and only busy/idle rows change that; a flip with no counter
moved is a row with every column null. The end row states every counter of the final usage. `exit_signal` names the
signal from one dictionary on every host: the macOS and Linux signal names, then Linux's real-time signals by kernel
number as `SIG32`…`SIG64`. The argv display writes an argument of only `[A-Za-z0-9_@%+=:,./-]` bare and any other in
`$'…'` quotes, with `\\`, `\'`, and `\xHH` for each byte of a control character or of an invalid UTF-8 sequence. The
enum columns (`exit_signal`, `blocked_on`) are dictionaries of every declared name keyed by `u16`, as lmao encodes
`S.enum`; `S.category` and `S.text` are `u32`-keyed dictionaries.

The job span, not each process span, carries signed `disk_ws_delta_bytes` and optional `disk_build_delta_bytes`,
`load1_milli: u32` at start and end, and `cores: u16` at those same boundaries. The start row states the host as the
supervisor read it at admission, when no process exists yet to sample; the end row states the job's terminal resource
sample: its host, and each volume's used-bytes change since admission's baseline. Host load converts once to nearest
milliload with the declared checked conversion; non-finite, negative, or overflowing load and an overflowing core count
are typed errors, never truncation, and the row they would have described is refused and counted in the trace health. A
missing build volume leaves its delta null. A volume whose usage is `Unavailable` (07_api.md: unconfigured, an
unsupported platform, or a failed stat) also leaves its delta null, never zero; the job's resource sample carries the
reason. A job that owned no process, or whose terminal sample could not be taken, leaves every end-row host and volume
cell null. All four columns are nullable. Process I/O and volume allocation remain different facts.

Linux job accounting retains the complete cgroup-v2 CPU and storage-I/O counters and separate charged-memory
current/peak; charged memory includes cache/kernel charges and is never written into an RSS column. macOS retains the
leader's own/children rusage reconciliation source. The declared source and named checked unit types are part of the
typed accounting record (07_api.md), not a JSON string.

A discrepancy between attributed process rows and the independent job total emits one `job.unattributed` row on the job
span: an `info` row of that message, sealed just before the job's `span-ok`/`span-err` row, that states the job's
terminal reconciliation (07_api.md, `UsageReconciliation`) whole in six fixed columns, which every other row of the span
leaves null.

| Column                        | Type     | Meaning                                                                                                                                                                                       |
| ----------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `unattributed_cpu_us`         | `i64`    | The independent CPU total less the CPU the observed processes counted as their own, in microseconds; negative where the processes counted more.                                               |
| `unattributed_io_read_bytes`  | `i64`    | The same difference in storage bytes read; null where the source or the observed processes have no bytes to compare.                                                                          |
| `unattributed_io_write_bytes` | `i64`    | The same difference in storage bytes written; null where nothing could be compared.                                                                                                           |
| `unattributed_source`         | `S.enum` | The independent source that counted the totals, by its wire kind: `linuxCgroupV2` or `macOsRusageChildren`.                                                                                   |
| `unattributed_coverage`       | `S.enum` | How completely the observed processes covered the job: `complete`, `eventsLost`, `unobservedBirth`, `unobservedExit`, `uncountedFork`, `unreadImage`, `unreadFinalUsage`, `unreadExitStatus`. |
| `unattributed_gap_pid`        | `u32`    | The observed process a gap's evidence names; null for a complete tree and for lost events.                                                                                                    |

Differences are signed checked units and are never clamped: observation can see work its source has not counted yet, and
the two are read at different instants, so a negative difference is a fact. A comparison no source could make -- macOS
counts no storage bytes for a job -- stays null, never zero. The row is written whenever the coverage names a gap, even
with every difference zero, and whenever a difference is non-zero over a complete tree (counter precision or
sampling-window disagreement); a complete tree that agrees in every comparison its source could make writes none. A job
whose terminal sample could not be taken writes none, and no row is invented for it: the span states a comparison's
absence, not a whole sample's unavailability. The row names no leaf: the leaf stays the sample's own
`JobLeafAttribution` (07_api.md), and nothing infers one from what was missed. Its enum columns are `u16`-keyed
dictionaries of every declared name, the names being the wire kinds. Charged-memory fields
`charged_memory_current_bytes`/`charged_memory_peak_bytes` stay on the job span, never on a process span: its end row
states them as nullable `u64` byte columns from the terminal sample's accounting, the cgroup source's checked
`memory.current`/`memory.peak` pair unconverted. A source with no charged-memory counter (macOS) and a job with no
terminal sample leave both null, never zero and never an RSS column's value. None of these fields grows the
fourteen-column process schema or duplicates process-tree JSON.

The process event-source RED compares expected birth/exec/exit records for bursts of short-lived grandchildren entirely
between coarse polls. Linux compares `CN_PROC` through the owning privileged helper and ptrace
`TRACEFORK`/`TRACEEXEC`/`TRACEEXIT` for coverage and fork-heavy overhead before selecting one; pidfds alone supply
identity and exit observation, not fork events. macOS includes `NOTE_EXIT` beside `NOTE_FORK`/`NOTE_EXEC`; its RED
measures the gap a burst reaped before `proc_listchildpids` reads it leaves, and requires that gap to be stated as
unattributed usage against the leader-own plus reaped-children rusage totals, whose source limits 07_api.md names.

One generated column declaration owns these names, types, enum values, and event-to-row projections. No writer
hand-copies the schema. The tree is span parentage, never JSON; no process name, metric name, or sample number creates a
new column. Program/path categories share dictionaries across repeated observations.

## Attribution tiers

Under interception (05_gateway.md), most granted traffic is request-visible, so attribution sharpens by tier:

1. **Cooperative lmao clients** on the plain-HTTP mirror endpoints or sending `traceparent` on an intercepted host:
   **exact span parentage** — the gateway adopts the inbound context. The in-image `xcrun` wrapper (03_caches.md) is
   first-party code, so `/sim/` broker calls (05_gateway.md) are tier-1 by construction.
2. **Proxy clients carrying the job id** — every job's environment names its job id as the proxy userinfo username
   (`http://job-<job_id>:<token>@…`, 03_caches.md), and the native package managers (bun, cargo, Go) and every other
   proxy-aware client turn that userinfo into `Proxy-Authorization: Basic` on the request or CONNECT. The gateway
   authenticates the password as the workspace token (05_gateway.md) and records the username's job id on the gateway
   span: **job-exact**, whether or not the workspace's jobs overlap. The job id is attribution, never authority: the
   endpoint and the token select and authenticate the workspace, and a process that rewrites its own proxy URL can
   misattribute traffic only among its own workspace's jobs. A child inherits its job's environment, so its traffic is
   the job's.
3. **Everything else** — an opaque tunnel, or an intercepted host whose client sent no `traceparent` and no job userinfo
   (a shell outside any job, whose wiring names `cowshed` as the username, or a client that drops proxy userinfo) — is
   **workspace-exact** (port identity) and **job-attributed at query time** by an interval join (request timestamp
   within a job's start/end, same workspace incarnation) over two controller-owned tables. The resulting gateway span
   links the matching `job_id` to its standard lmao trace/thread/span identity; exact when a workspace's jobs don't
   overlap, heuristic when they do.

Interception enriches tiers 2 and 3 with request detail; an opaque tunnel remains host-only. An intercepted request is
workspace-exact **with request-level detail** (verb, path, bytes) and an outbound-injected `traceparent`, even when the
inbound client is silent.

## Accepted-project Nx cache measurements

The Nx gateway route (05_gateway.md) attributes every accepted-project read and write to the selected project, workspace
incarnation and operation, and a land's publish writes are attributed the same way. These are diagnostic projections;
audit records authorize nothing.

Report the following independently, with unavailable measurements explicitly absent rather than zero:

- ordinary WIP lookups: local and remote hit counts, misses, and bytes and time (a shed's read-only client sends no
  `PUT` and builds no archive);
- a land's publication: the `published`, `existing` and `skipped` counts of `build_volume::nx::Published`, and each
  failed entry's cause on stderr;
- client archive construction, land publication transfer, target download/extraction, local carry/copy bytes and time,
  and Cargo build-volume warmth;
- server complete-entry bytes, staging bytes, configured capacity, eviction count/bytes, interrupted-upload recovery,
  and actual allocated bytes where the filesystem reports them;
- the target's exact landed head and the same successful cacheable task/hash set before and after adoption, every
  hit/miss/exit status, the check timings, and its no-remote control.

A shed's read-only client builds no archive and sends no `PUT` (05_gateway.md, "A shed never uploads"), so its lookups
are its whole Nx cost; a land's publish pays archive construction and `PUT` per stored result, measured there. Task-hash
archives are not a per-file content-addressed store, and transferred bytes are not filesystem allocation. A successful
server-only HTTP smoke proves neither the gateway's route nor a two-workspace adoption.

### Automated Nx proof surfaces

These are process and API fixtures. They are not protected authorized-land, installed-adoption, undeclared-input audit
or normal-client missing-artifact proofs, and they do not measure a stock client against a real land.

- `cowshed-gateway` `tests/gateway/nx_land_publish.rs`, ignored host case
  `host_controller_a_landed_gate_publishes_and_another_shed_restores_it`: shed A's read-only run stores nothing; the
  land's gate publish stores the attributed result once, and a second publish finds it already stored; shed B restores
  it with nothing executed.
- `cowshed-gateway` `tests/gateway/nx_process.rs`, ignored host case: a real `Gateway` is SIGKILLed, the kernel reports
  its server's exit, a fresh server on the same store acquires the lock, and two sessions read through the route.

Live physical-bound eviction and killed-upload proofs remain with the server's own tests. No proof here checks
entry-level stats for the entire store bound.

### Gateway-client outage and cold controls

`cowshed-gateway` `tests/gateway/nx_outage.rs` runs the installed Nx client in fresh fixture workspaces with separate
local caches and task databases. Real gateway cases cover missing service configuration, a missing executable, a dead
backend socket, an offline connector, and a backend's pre-response 503. Each unavailable GET is an audited 404, and the
task executes locally once. A direct native store of one task hash through the gateway to the 503 backend returns
`false` without another task execution and records a failed `nx-backend-unavailable` PUT refused with 403. The ordinary
short-Content-Length case commits a 200 head and visible body prefix before closing the upstream early. It requires a
client-visible short body and a failed `nx-transfer-failed` audit, never a completed read. Direct-client controls run
the same client against the fixture backend with no gateway, so the backend's 503 reaches the client, which reads it as
its own outage (05_gateway.md "The client half"): a GET 503 is a miss, the task executes once and the run succeeds; a
publishing client's PUT 503 after that execution skips the store and keeps the result. Each run prints exactly one
outage line naming the server, the request and the 503, and asks that server nothing more; with no gateway there is no
audit. Through the gateway the same backend stays an audited 404/403, the contrast these controls exist to show.

Run fresh one-task cold controls both without an endpoint and through the gateway. Record each wall-time row, its
cacheable-task count, execution marker, gateway statuses and backend-request count in a dated proof receipt. The rows
are observations under that run's load, not a universal overhead bound or an isolated measurement of compression CPU,
archive allocation, transfer bytes, accepted restores or an installed landing. Keep pass counts and measured timings in
the proof receipt, not in this contract.

The ignored `tests/gateway/nx_stream_kill.rs` host-controller case uses the normally produced server executable and
kills that backend after a real read has committed its 200 head and begun its body. Its required observation is a short
stream and a failed `nx-transfer-failed` audit event, never a completed hit. This is a stream-failure proof surface, not
task-result restoration or cutover evidence.

### Stock Nx client cost control

Governing sections: `specs/cowshed/05_gateway.md`, “`/nx/v1/cache/{hash}` — accepted-project Nx artifacts”;
`specs/cowshed/13_telemetry.md`, “Accepted-project Nx cache measurements”.

Nx 23.2.1's native HTTP cache has a zero-argument constructor configured by `NX_SELF_HOSTED_REMOTE_CACHE_SERVER`.
`DbCache` checks its local cache first. On a local miss, it can retrieve an accepted artifact through HTTP. After an
executed task, it stores the result locally and then calls the remote store operation.

Stock Nx has no GET-only/skip-upload control checked before archive construction; the carried patch's
`ReadOnlyRemoteCache` adds one on the `DbCache` path, answering `store` `false` before any archive is built.
`HttpRemoteCache.store` reads the cached outputs and constructs a complete in-memory gzip tar archive before sending
`PUT`. `skipRemoteCache` disables retrieval too. The public `RemoteCacheV2` type is not an arbitrary provider-injection
hook. Public pre/post task lifecycle hooks can wire environment variables and capture task results, but expose neither a
provider handle nor a prearchive store veto.

Measure a shed's lookups and a land's publish separately: a land's archive construction and attempted `PUT` apart from
accepted GET/restore and subsequent local hits. A dated proof receipt identifies the retained native/client and service
executables, separates payload lengths, filesystem operations and process RSS, and names relay attribution. Client cost
evidence alone does not prove the artifact service, gateway or installed cutover.

Primary sources:

- [Nx self-hosted cache protocol and configuration](https://nx.dev/docs/kb/self-hosted-caching.md)
- [Pinned native HTTP cache constructor/retrieve/store implementation](https://raw.githubusercontent.com/nrwl/nx/23.2.1/packages/nx/src/native/cache/http_remote_cache.rs)
- [Pinned DbCache local-first flow and fixed provider discovery](https://raw.githubusercontent.com/nrwl/nx/23.2.1/packages/nx/src/tasks-runner/cache.ts)
- [Pinned native allocator definition](https://raw.githubusercontent.com/nrwl/nx/23.2.1/packages/nx/src/lib.rs)
- [Supported task lifecycle hooks](https://nx.dev/docs/kb/task-running-lifecycle.md)

## Storage: one authority tier, one audit trail, one Arrow substrate

Cowshed uses Arrow IPC for both, but placement and authority differ:

- **Protected in-volume evidence** — `.cowshed/job/records.arrow` contains allocation/lifecycle batches, terminal exec
  records, checkpoint manifests, bounded summaries, and small terminal stdout/stderr as Arrow Binary. Larger or
  checkpoint-forced streams spill lazily to protected `.cowshed/job/<job_id>/out|err` files. Complete batches and sealed
  files are authoritative captured-content evidence only within their recorded origin incarnation/checkpoint boundary.
  They are not workspace-writable: the supervisor is the sole live writer and the mandatory child profile denies every
  mutation beneath `.cowshed/job/**` before repository-controlled startup (04_sandbox.md/11_shell.md). The workspace
  marker the image carries (`.cowshed/workspace.json`) names the incarnation and its **lineage** — the ancestor
  incarnations the image was cloned from, nearest first, written by the controller when it mints the incarnation — and
  that lineage is what authorizes records an ancestor wrote into a cloned image.
- **Authority is the host inventory.** What workspaces exist, which incarnation each is, which are retired, which
  lineage an image carries, and which port block and grants belong to a workspace are read from the images and mounts
  under `/private/cowshed/store/`, the per-workspace grants files, and the controller lock. No log is replayed for any
  of these decisions; a controller that opens a project reads the inventory once and starts.
- **Controller audit records** — every controller act (workspace introduced/retired, job admission and terminal state,
  checkpoint, fork, restore, each land whose build-volume step completes and returns its actual result, and each cache
  miss a land's output restoration found) is emitted as one typed `ControllerCommitment` record to an audit sink. A land
  outcome records whether the landing volume seeded the target and whether adoption succeeded or skipped, including the
  exact skip reason and its holders. The records never contain inline output, a protected artifact path, a redirect
  source, or any other raw stdout/stderr payload duplication. **Nothing reads them for a decision.** The sink is
  selected when the project opens (`COWSHED_CONTINUITY_AUDIT`): `arrow` (default for the standalone CLI) writes sealed
  per-writer segments under `/private/cowshed/store/telemetry/`, `off` discards, and a supervising runtime injects its
  own sink through `ProjectRuntime::open_existing_with_audit` (routing the same records into its own durable log). A
  sink that refuses a record is a `doctor` finding (`audit-sink`), never a reason to fail the act it describes. Job
  admission, terminal and checkpoint records come from the workspace supervisors, which are processes of their own
  (11_shell.md): each records to the host's default sink, and a controller with a sink of its own reads each
  supervisor's records after the last cursor it forwarded, records them, and acknowledges them by cursor.
- **Arrow audit segments** — one Arrow IPC batch holding one row per record, one to 64 records per segment, at
  `<host-telemetry-root>/<yyyy-mm-dd>/commitment-<order:020>-<writer_uuid>.arrow`, where the UTC partition is an exact
  calendar date, `writer_uuid` is the lowercase hyphenated UUID of the controller process that wrote it, and `order` is
  the segment's first record's place in that writer's own monotone sequence from 1; its rows carry consecutive orders in
  `commitment_order`. A completed segment is mode `0600`, sealed by create-new atomic rename, and never reopened,
  replaced, or shared for append; a crash leaves at most an unsealed dot-prefixed temporary that nothing reads.
  Concurrent controllers never contend: names are unique by writer, so no lock and no global order exist. Partitioning
  by day is a query/retention layout, not shared-file append. This rule does not describe the separately locked
  protected `.cowshed/job/records.arrow` framed stream above.
- **Group commit** — the commitment publisher takes every record already queued when it takes one, up to 64
  (`GROUP_RECORDS`, `cowshed-core` `storage/audit.rs`), and seals them as one segment with one sync; it never waits for
  more, so a group adds no delay, and it grows only while the previous group's sync runs. The group is synced at the
  strongest durability any of its records requires, never less: `fsync(2)` of the segment when it holds only job
  admission and terminal records (11_shell.md, "Durability of a job's own records"), otherwise `F_FULLFSYNC` of the
  segment and of its date directory. Every record of a group is acknowledged only after that sync returns; a failed
  group counts each of its records as refused. Group commit changes how many records one sync carries, not which barrier
  any record gets, and there is no cheaper barrier to give them: `F_BARRIERFSYNC` (`<sys/fcntl.h>`: "fsync + issue
  barrier to drive") orders writes but persists nothing, and Apple's `fsync(2)` page names `F_FULLFSYNC` as the call
  that makes data persistent on the drive. A lone producer, which waits for each acknowledgment before its next record,
  always forms groups of one; grouping comes only from concurrent producers. A process that dies mid-group acknowledged
  none of it, so no caller acted on a reply for any of its records: before the rename they are lost in an unsealed
  temporary nothing reads, after it they are sealed whole but unanswered. Grouping changes when queued records are
  written, not which a crash can take. A lost record stays lost — a job's terminal record is sealed before its
  commitment, so the next supervisor does not re-seal that job — while the inventory and the protected records still
  hold the facts, and nothing reads commitments for a decision.
- **Controller producer capability delivery** — each controller telemetry producer receives a dedicated IPC channel or
  inherited write-only capability/FD. It is close-on-exec/non-inheritable before any workspace child starts and is never
  named by a workspace-readable path or token. Audit records are recorded in the order the controller performed the
  acts, each acknowledged only once it is durable (Group commit, above); the short-timer one-batch crash window applies
  to diagnostic events; gateway decision boundaries retain the flush policy below.
- **The one text-file survivor** — `~/Library/Logs/cowshed/daemon-stderr.log`, the launchd `StandardErrorPath` target,
  exists only for crashes before tracer initialization. It lives off the `/private/cowshed/store` mountpoint so reboot
  remount cannot be masked. `doctor` flags it when non-empty. The gateway, every workspace supervisor and their
  children, and every released image's deletion child, whichever process started it (05_gateway.md, "Deleting released
  images"), write it at once, so each cowshed process writes each line in one `write(2)` (`cowshed_os::eprintln!`;
  clippy refuses `std::eprintln!`/`eprint!` in the cowshed crates). `std` writes a line one formatted piece at a time,
  and pieces of concurrent processes splice: an install's `supervisor-recovery drain` spans and its supervisors'
  "retires after draining" lines interleave mid-word, so no line says which supervisor waited on what. A daemon (the
  gateway, each workspace supervisor) never writes it from the thread that serves: that thread runs the daemon's whole
  runtime, and the file shares the store's APFS container, so one write the container stalls stops every request. A free
  in progress stalls such a write, and every `cowshed fork` and `ls` on the host hangs behind the thread that waits in
  it. So a daemon queues each whole line for one writer thread of its own (`cowshed_os::stderr::detach`), up to 4,096
  lines. A line that finds the queue full is dropped and counted, never waited for, and the writer's next write first
  says `cowshed: <n> stderr line(s) dropped: the log took no writes while the queue was full`. A line the log itself
  refuses (`EIO`, `ENOSPC`, a closed descriptor) is a different loss, counted apart: the next write the log takes says
  `cowshed: <n> stderr line(s) lost: the log refused them: <error>`, naming the error that refused the first of them,
  never a full queue. A note the log refuses is said again, not lost. The process waits for the queue before it exits.

`StreamInfo` is the shared content descriptor:

```
ProtectedOutput = Inline { data: BinaryData } | File { path: WorkspacePath }
OutputStorage   = Captured { artifact: ProtectedOutput }
                | Redirect { source: WorkspacePath, artifact: ProtectedOutput }
StreamInfo      = { storage, bytes, sha256, summary }
```

Inline bytes are bounded by the frozen inline-output limit. Protected Arrow represents them as Binary. Ordinary
`JobInfo` JSON uses the exact `BinaryData` wire union `{encoding:"utf8",data} | {encoding:"base64",data}`, selecting
UTF-8 only for valid bytes and bounding both branches by decoded length. `Redirect.source` is mutable/non-authoritative;
reads resolve its independent protected `artifact`. Controller commitments carry only count/hash—never the storage
union, either inline encoding, or payload/path. Summaries remain bounded diagnostic projections and establish no
outcome.

`JobInfo.argv` uses the separate canonical `CommandArg` union with the same exact tag names, but stricter command
invariants: `utf8` is selected iff the OS bytes are valid UTF-8; base64 must be canonical and must represent non-UTF-8
bytes. Unknown fields/encodings, malformed data, NUL, elements above 128 KiB, aggregate argv above 1 MiB, and empty
argv/`argv[0]` reject before RPC, spawn, or protected evidence mutation. Common UTF-8 arguments therefore remain
readable without base64 or lossy conversion. A script job carries `JobInfo.script` instead (07_api.md, 11_shell.md).

## Protected exec and checkpoint schema

Protected Arrow is the exact tagged/versioned union:

```rust
enum ProtectedRecord {
    Job(JobArtifactRecord),
    CheckpointManifest(CheckpointManifestRecord),
}
struct CheckpointManifestRecord {
    version: u16,
    repo_id: RepoId,
    origin_incarnation: WorkspaceIncarnation,
    barrier_id: u64,
    visible_jobs: Vec<VisibleJobCommitment>,
    records_sha256: Sha256Digest,
}
struct VisibleJobCommitment {
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
    state: JobState,
    stdout: VisibleStreamCommitment,
    stderr: VisibleStreamCommitment,
}
struct VisibleStreamCommitment {
    storage_kind: VisibleStorageKind,
    bytes: u64,
    sha256: Sha256Digest,
    protected_path: Option<WorkspacePath>,
}
```

`barrier_id` is positive and monotonic within `origin_incarnation`. `VisibleStorageKind` is exactly
`captured-inline|captured-file|redirect-inline|redirect-file`; `protected_path` is present exactly for a file kind.
`records_sha256` hashes the bytes of the complete protected-record stream prefix immediately before the manifest batch.
All running memory-only prefixes promote and all files fsync before this record is appended; terminal inline bytes live
in a prior `ProtectedRecord::Job` covered by the prefix digest. Recovery frames retain their `batch_sha256`; recovery
may discard/report only an incomplete trailing frame.

The file is the 8-byte magic `CSARROW1`, then one frame per record: `CSBATCH1`, the payload length as a little-endian
`u64`, that length's bitwise complement as a little-endian `u64`, the payload — an Arrow IPC stream holding exactly one
one-row batch — its SHA-256 (32 bytes), and `CSEND001`. A reader outside cowshed (a query over a workspace's job
history) walks those frames and hands each payload to any Arrow IPC stream reader; only an incomplete trailing frame may
be skipped.

The flat Arrow schema begins `record_kind, record_version, repo_id`. A Job row then uses
`workspace_incarnation, job_id, sequence, state, grant_revision`, followed by the existing
`stdout_storage_kind, stdout_source_path, stdout_inline_bytes, stdout_protected_path, stdout_bytes, stdout_sha256, stdout_summary_version, stdout_summary_text, stdout_summary_truncated`
and equivalent `stderr_*` columns, optional output-limit columns, required `argv: List<Binary>`, which holds a script
job as the two elements `\0script` and the script's JSON, a nullable `failure` naming why a `failed` job failed when no
status of its own says so (`supervisorLost`), then `exit_code` (Int32) or `exit_signal, exit_core_dumped` (Int32,
Boolean) for the status `wait(2)` reported, and `duration_ms` (UInt64). The exit and duration columns are null on a
running record and on a terminal one whose end nothing observed (a job refused before its command ran,
`supervisorLost`). A terminal job's resource sample follows: `resources_sampled_at` (Utf8), `resources_wall_us`
(UInt64), `resources_leader_pid` (UInt32), `resources_members: List<UInt32>`,
`resources_host_start_load1, resources_host_start_cores, resources_host_load1, resources_host_cores` (Float64, UInt16,
Float64, UInt16), `resources_rss_bytes, resources_rss_peak_bytes` (UInt64), the four UInt64 stream tallies
`resources_stdout_bytes, resources_stdout_lines, resources_stderr_bytes, resources_stderr_lines`, then the sample's
`JobAccounting`: `resources_accounting_source` (Utf8, its wire kind: `macOsRusageChildren` or `linuxCgroupV2`),
`resources_accounting_cpu_user_us, resources_accounting_cpu_sys_us` (UInt64), and
`resources_accounting_io_read_bytes, resources_accounting_io_write_bytes` (UInt64), then, appended in version 7,
`resources_volume_workspace` and `resources_volume_build`, each that volume's `VolumeUsage` as its 07_api.md wire JSON
(Utf8). Version 8 appends the fourteen admission columns (07_api.md, "Keyed admission"). Version 9 appends the rest of
the sample: `resources_cpu_pct` (Float64), `resources_leaf` and `resources_accounting_reconciliation` (the
`JobLeafAttribution` and `UsageReconciliation` as their wire JSON, Utf8; a leaf's argv is bounded, 128 KiB per argument
and 1 MiB in all, and `truncated: true` marks one that was cut, so no observed process can make a record unwritable),
and `resources_accounting_charged_current_bytes, resources_accounting_charged_peak_bytes` (UInt64).
`cpuUserMs`/`cpuSysMs` and `wallMs` are derived again from their microseconds, never stored twice. Every resource column
is null on a record without a sample. A sample always carries `resources_volume_workspace`, its accounting source with
CPU totals and reconciliation, its CPU share and its leaf; `resources_volume_build` is null for a sample of a job that
ran with no build volume; the macOS source's two byte columns are null together, a cgroup's are present, and the charged
pair is present exactly for `linuxCgroupV2`; any other null combination is rejected. Records are written and read at
`record_version` 9 only: a store holding a record in an earlier layout is set aside whole and never read (11_shell.md),
and a batch whose layout and version disagree is rejected; each version is its predecessor with columns appended, so an
earlier reader meets a later record as a newer writer's. A CheckpointManifest row instead uses
`origin_incarnation, barrier_id, visible_jobs, records_sha256`, with
`visible_jobs: List<Struct<workspace_incarnation,job_id,state,stdout,stderr>>`. Columns outside the selected variant are
null and validators reject every other null combination. Job recovery validates non-null raw argv elements, the
non-empty first argument, NUL exclusion, the 128 KiB element limit, and the 1 MiB total before allocating OS strings,
and decodes and validates a script record's JSON.

## Controller audit record schema

Controller audit version 2 is the exact tagged/versioned union (the `order` is the writing controller's own sequence):

```rust
enum ControllerCommitment {
    WorkspaceIntroduced(WorkspaceIntroducedCommitment),
    WorkspaceRetired(WorkspaceRetiredCommitment),
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Checkpoint(CheckpointCommitment),
    Fork(ForkCommitment),
    Restore(RestoreCommitment),
    LandAdoption(LandAdoptionCommitment),
    LandOutcome(LandOutcomeCommitment),
}
struct WorkspaceIntroducedCommitment {
    version: u16, order: u64, repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
}
struct WorkspaceRetiredCommitment {
    version: u16, order: u64, repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
}
struct AdmissionCommitment {
    version: u16, order: u64, repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation, job_id: JobId, grant_revision: u64,
}
struct TerminalCommitment {
    version: u16, order: u64, repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation, job_id: JobId, state: JobState, grant_revision: u64,
    stdout_bytes: u64, stdout_sha256: Sha256Digest,
    stderr_bytes: u64, stderr_sha256: Sha256Digest, batch_sha256: Sha256Digest,
}
struct CheckpointCommitment {
    version: u16, order: u64, repo_id: RepoId, origin_incarnation: WorkspaceIncarnation,
    checkpoint_id: String, barrier_id: u64, manifest_batch_sha256: Sha256Digest,
}
struct ForkCommitment {
    version: u16, order: u64, repo_id: RepoId,
    source_incarnation: WorkspaceIncarnation, destination_incarnation: WorkspaceIncarnation,
}
struct RestoreCommitment {
    version: u16, order: u64, repo_id: RepoId, source_checkpoint: String,
    source_incarnation: WorkspaceIncarnation, replaced_incarnation: WorkspaceIncarnation,
    destination_incarnation: WorkspaceIncarnation,
}
struct LandAdoptionCommitment {
    version: u16, order: u64, repo_id: RepoId,
    landing_incarnation: WorkspaceIncarnation, target_incarnation: WorkspaceIncarnation,
    landed_head: GitOid, task: String, task_hash: String, inputs_digest: Sha256Digest,
}
struct LandOutcomeCommitment {
    version: u16, order: u64, repo_id: RepoId,
    landing_incarnation: WorkspaceIncarnation, target_incarnation: WorkspaceIncarnation,
    landed_head: GitOid, seeded: bool, skip: Option<AdoptionSkip>,
}
```

A `LandAdoption` record is one Nx task that missed the cache when a land re-ran its check in the target on the build
volume it adopted (16_build_volumes.md, Land step 7): one record per miss, never one per land. `task` is the Nx task id,
`task_hash` the hash Nx computed in the target, and `inputs_digest` the SHA-256 of the task's hash inputs as
`nx show target inputs` named them (the land report carries the inputs themselves). Why: every such miss is a defect in
the project's Nx configuration (an input that differs between checkouts, an undeclared output, a nondeterministic step),
and the land succeeds regardless, so the land report alone is read once and forgotten. As records, a coordinator queries
every miss across lands and hosts, groups recurrences by `task` and `inputs_digest`, and turns them into fix work. The
landing and target incarnations must differ, and `task` and `task_hash` must be non-empty.

A `LandOutcome` record is the actual build-volume result of one `cowshed land` whose build-volume step completes and
returns that result, written whether or not its seed or adoption happened, before retirement. If the build-volume step
returns an error, the error is returned and no outcome record is written. `seeded` is true exactly when the report's
`seed` is `frozen`; an adopted `sealBusy` outcome keeps the previous seed and records false. `skip` is the report's
complete `AdoptionSkip` when adoption skipped, including lock paths and each holder's pid and command, and is absent
only when adoption succeeded; an ungated lane fold records `skip` `{"kind":"ungated"}` and `seeded` false
(16_build_volumes.md, "Ungated folds"). Landing and target incarnations must differ. It does not replace the per-miss
`LandAdoption` records. A failed audit sink is reported through audit health, never used to undo a land.

The flat controller Arrow columns are exactly `commitment_kind, commitment_version, commitment_order, repo_id` plus
variant-selected
`workspace_incarnation, job_id, grant_revision, state, stdout_bytes, stdout_sha256, stderr_bytes, stderr_sha256, batch_sha256, origin_incarnation, checkpoint_id, barrier_id, manifest_batch_sha256, source_incarnation, destination_incarnation, source_checkpoint, output_limit_bytes, output_crossing_bytes, replaced_incarnation, landed_head, task, task_hash, inputs_digest, seeded, adoption_skip`.
A `landAdoption` or `landOutcome` row carries its landing and target incarnations in `source_incarnation` and
`destination_incarnation`. Both carry `landed_head` (Utf8); `landAdoption` alone carries `task`, `task_hash` (Utf8) and
`inputs_digest` (Binary); `landOutcome` alone carries `seeded` (Boolean) and `adoption_skip` (nullable Utf8, the exact
JSON `AdoptionSkip` value). Non-selected fields are null and the tag controls required fields. Missing columns are null
for every kind that does not require them. A row requiring a column its segment lacks is malformed and refused. Sealed
segments are never rewritten.

`order` is the writing controller's own positive, strictly increasing sequence from 1; across writers a segment is
identified by `(order, writer_uuid)`, and nothing requires a host-global order because nothing replays the segments.
What the records used to prove — which incarnations exist, which retired, which lineage an image carries, which jobs
were admitted — is read from the inventory: the images and mounts, the marker each image carries (incarnation and
`lineage`, the ancestor incarnations nearest first), the grants files, and the protected records inside the image. A
records file may hold frames from its lineage and from no other incarnation; the supervisor refuses anything else as
`Integrity`.

The segment set is never enumerated to decide anything: a writer assigns its own next order and never reads back what it
wrote. A draft is acknowledged exactly once only after its immutable segment is durable.

No commitment variant contains `inline_bytes`, `protected_path`, `source_path`, summary text, or output payload. The
protected records are self-validating — frame digests, the manifest's `records_sha256`, stream hashes — and recovery
types missing/altered content or an invalid complete frame as `Integrity`; an incomplete trailing frame alone is
successful reported recovery with its `batch_sha256` retained for diagnosis. The audit records repeat the counts and
hashes so an after-the-fact query can compare them against what the image holds, but no controller decision performs
that comparison.

## Inspection

Two layers, so cowshed ships no bespoke log reader:

- **`lmao-inspect`** (specs/lmao/04_inspect_cli.md) — a thin, generic binary over `lmao-query`: tail/follow a segment
  directory, filter by column, run selectors and SQL, render human tables, export `--json`/`--ndjson`. Domain-agnostic;
  the same tool inspects any compatible trace store.
- **cowshed CLI domain verbs** (06_cli.md) wrap it with cowshed's paths and vocabulary: `cowshed logs` (controller
  telemetry, `--ws/--kind/--since/--follow`), `cowshed audit` (gateway events, `--denied/--host`), and
  `cowshed trace <trace-id>` (terminal waterfall of a lifecycle op, exec, or land). All follow the stdout contract:
  human tables by default, `--json`/`--ndjson` for machines.

**NDJSON survives only as a stream/export encoding** — the `--ndjson` flags above, and 06_cli.md's stderr progress
events for long `--json` operations, are wire formats on a pipe to a live consumer. Nothing writes NDJSON to disk.

**Latency spans.** Until lifecycle spans flush as Arrow, the CLI's own steps print on stderr: lifecycle verbs
unconditionally (`cowshed: <step> start` / `done elapsed=…`), and every other step — project discovery, host storage
validation, the controller's inventory, binding and recovery passes, each controller route, the gateway reconcile, a
resident answer's resolution and why it declined (06_cli.md), job submission and relay — only under `COWSHED_TIMING=1`,
one `cowshed: timing +<since start> <scope> <step> <elapsed>` line per finished step. `path` and `exec` never print them
otherwise: their stdout answers a script and their stderr is the child's. A workspace supervisor prints the same lines
for the steps of each job it runs — admission record and commitment, sandbox environment, spawn, and the terminal record
and commitment — on its own stderr, which is the daemon's log, when it runs with `COWSHED_TIMING=1`. A controller call
that asks for its steps (07_api.md) also hears each lifecycle step start and end as a step frame, nested under the step
it runs inside, on whichever thread the step runs: an embedder that does not share the controller's stderr records them
as its own spans.

## Querying

- **Capability-scoped**, mirroring the coordinator/worker split (07_api.md, 12_mcp.md): a coordinator queries controller
  audit records and telemetry. A worker queries one workspace's reconciled lifecycle view and reads captured bytes
  representation-transparently from protected in-volume artifacts. Controller rows never serve raw output, and protected
  rows alone never claim cross-incarnation completeness.
- **Integrity joins** can require the protected terminal/manifest batch digests, counts, and stream hashes to match the
  audit records — an after-the-fact query over telemetry, not a gate.
- **`cowshed doctor --bench`** reports real p50/p99 from accumulated lifecycle spans, turning the 08_testing.md budgets
  into SLOs monitored over actual usage.

## What columns buy

- **Real distributions.** Every attach/clonefile/fsck ever run is the dataset; the n=20 benchmark problem dissolves.
- **Query-time correlation** links the numeric `job_id` to standard lmao trace/thread/span identity and replaces runtime
  ID-plumbing through uncooperative tools (the tier-3 interval join, lineage walks, "what could this workspace reach at
  time T vs what it tried").
- **Fleet questions, zero infra** — egress-denial hot spots, mirror hit rates, image-growth trajectories, grant churn,
  per-task cost — as queries over local files.
- **Cheap diagnostic retention** — dictionary + zstd columnar spans are far smaller than NDJSON; gc may drop ordinary
  diagnostic segments. Controller audit segments are telemetry too: retention is the operator's policy, and no reader
  depends on their completeness.
- **Checkpoint evidence diffing** — "diff the job histories of these two forks" is a query; a coordinator can select the
  winning fork of a `land --check` by its trace.

## Tradeoffs

**lmao over OpenTelemetry SDK + collector.** An OTel collector is a daemon and a wire protocol cowshed would have to run
and secure; lmao is an in-process library writing local Arrow, consistent with "no state daemon" (00_overview.md).
cowshed's events are schema-regular (dictionary-friendly workspace/host/decision/kind columns, per-trace-anchored
timestamps), which is the columnar sweet spot. If a team wants Jaeger/Grafana, OTLP export is a **projection** from the
Arrow store, not a second pipeline.

**NDJSON storage rejected.** An earlier draft kept a line-oriented NDJSON audit as the "durable append form" beside the
Arrow segments. That is two files, two schemas, and two rotation/retention policies recording the same events — log-file
explosion for no good reason. One substrate with an honest one-batch crash window beats a shadow format whose only job
is narrowing that window; if a specific event class ever proves too precious for the window, the fix is a per-event
flush of that class, not a second format.

**Batched audit flush accepted.** A per-request flush would be the most durable but throttles the gateway. The one-batch
crash window is the accepted cost, bounded by the decision-boundary/short-timer flush policy above.

**Tiered job authority.** Protected in-volume records and artifacts are not editable convenience projections: the child
profile makes them supervisor-only, and complete batches/sealed files are authoritative captured-content evidence inside
their origin incarnation/checkpoint boundary. They cannot prove that a later job or incarnation was not omitted by
restoring the entire image — and cowshed does not try to: a restore is a controller verb the workspace cannot perform on
itself, so "was something rolled back" is a question for the audit trail after the fact, not a fact any decision depends
on. The compact audit records carry existence/status/order/lineage and the hashes such a query needs, never raw output.
Within the image, disagreement between a frame and its digests is typed `Integrity`; no blanket “outside wins” or “newer
wins” rule exists.
