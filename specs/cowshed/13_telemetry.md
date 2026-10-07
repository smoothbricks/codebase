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

**Dependency honesty**: cowshed does not depend on lmao's crates. The gateway writes lmao's Arrow trace schema itself
(`cowshed-gateway/src/telemetry.rs`), byte-for-byte aligned with `lmao-arrow`'s, because that crate cannot be imported
without its `lmao-core` runtime; nothing in cowshed queries traces with `lmao-query`, and no golden trace fixture
exists.

> **Implementation status — process monitoring:** fork/exec tree observation, per-process resource/blocker events,
> `process.run` spans, their on-change/heartbeat rows, and job-span host/volume columns below are unbuilt. Existing
> gateway trace segments and controller commitments do not implement that process tree. Cgroup job-total and
> charged-memory columns and unattributed-usage reconciliation rows are unbuilt as well.

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

**lmao TRACEPARENT convention** (a verification item until the TS/Rust tracer ships it, kickoff): a tracer adopts
`TRACEPARENT` at init and stamps outbound `fetch`/HTTP requests with it. This is what upgrades a first-party tool's
traffic from tier-2 to tier-1 attribution (below).

## Span taxonomy

- **Lifecycle spans.** `cowshed new`'s steps (02_workspaces.md) are the canonical waterfall — clone, attach, fsck,
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
one coarse heartbeat row per progress tick. A row sets only the columns that changed; unchanged columns are null, not
repeated payload. The terminal row carries the final usage and exact exit. A blocker transition with no other changed
value still creates a row. Program changes at exec use `proc_program`; argv remains a single start-row display, while
the typed process event retains the exact command arguments. Display escaping is deterministic and does not turn
arbitrary Unix argv bytes into lossy UTF-8 or a JSON string column. A CPU busy/idle flip uses a fixed standard row
kind/template, not a fifteenth custom column. Sparse API deltas distinguish unchanged, SET, and CLEAR; the row's
transition kind preserves clearing semantics without carrying a JSON payload or adding columns.

The job span, not each process span, carries signed `disk_ws_delta_bytes` and optional `disk_build_delta_bytes`,
`load1_milli: u32` at start and end, and `cores: u16` at those same boundaries. Host load converts once to nearest
milliload with the declared checked conversion; non-finite, negative, or overflowing load and an overflowing core count
are typed errors, never truncation. A missing build volume leaves its delta null. Process I/O and volume allocation
remain different facts.

Linux job accounting retains the complete cgroup-v2 CPU and storage-I/O counters and separate charged-memory
current/peak; charged memory includes cache/kernel charges and is never written into an RSS column. macOS retains the
leader's own/children rusage reconciliation source. The declared source and named checked unit types are part of the
typed accounting record (07_api.md), not a JSON string.

A discrepancy between attributed process rows and the independent job total emits one `job.unattributed` row with
`unattributed_cpu_us`, optional `unattributed_io_read_bytes` and `unattributed_io_write_bytes`, in signed checked units;
source/coverage is a fixed row kind or enum, not a dynamically named column. Event loss, unavailable evidence and
counter-window/precision disagreement are explicit. Charged-memory fields
`charged_memory_current_bytes`/`charged_memory_peak_bytes` stay on the job span. None of these fields grows the
fourteen-column process schema or duplicates process-tree JSON.

The process event-source RED compares expected birth/exec/exit records for bursts of short-lived grandchildren entirely
between coarse polls. Linux compares `CN_PROC` through the owning privileged helper and ptrace
`TRACEFORK`/`TRACEEXEC`/`TRACEEXIT` for coverage and fork-heavy overhead before selecting one; pidfds alone supply
identity and exit observation, not fork events. macOS includes `NOTE_EXIT` beside `NOTE_FORK`/`NOTE_EXEC`; its RED
measures the gap a burst reaped before `proc_listchildpids` reads it leaves, and requires that gap to be stated as
unattributed usage against the exact leader/children rusage totals (07_api.md).

One generated column declaration owns these names, types, enum values, and event-to-row projections. No writer
hand-copies the schema. The tree is span parentage, never JSON; no process name, metric name, or sample number creates a
new column. Program/path categories share dictionaries across repeated observations.

## Attribution tiers

Under interception (05_gateway.md), most granted traffic is request-visible, so attribution sharpens by tier:

1. **Cooperative lmao clients** on the plain-HTTP mirror endpoints or sending `traceparent` on an intercepted host:
   **exact span parentage** — the gateway adopts the inbound context. The in-image `xcrun` wrapper (03_caches.md) is
   first-party code, so `/sim/` broker calls (05_gateway.md) are tier-1 by construction.
2. **Everything else** — an intercepted host whose client sent no `traceparent`, an `--opaque` tunnel, and the native
   package managers (bun, cargo, Go): none takes a per-job registry URL, because none is wired to a mirror route that
   could carry a `/t/<traceparent>` segment — is **workspace-exact** (port identity) and **job-attributed at query
   time** by an interval join (request timestamp within a job's start/end, same workspace incarnation) over two
   controller-owned tables. The resulting gateway span links the matching `job_id` to its standard lmao
   trace/thread/span identity; exact when a workspace's jobs don't overlap, heuristic when they do.

Interception makes tier 2 richer than a tunnel would: an intercepted request is workspace-exact **with request-level
detail** (verb, path, bytes) and an outbound-injected `traceparent`, even when the inbound client is silent.

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
  checkpoint, fork, restore, and each cache miss a land's adoption check found) is emitted as one typed
  `ControllerCommitment` record to an audit sink. The records carry existence, lifecycle/status, a writer-local order,
  lineage, byte counts, and expected hashes; they never contain inline output, a protected artifact path, a redirect
  source, or any other raw stdout/stderr payload duplication. **Nothing reads them for a decision.** The sink is
  selected when the project opens (`COWSHED_CONTINUITY_AUDIT`): `arrow` (default for the standalone CLI) writes sealed
  per-writer segments under `/private/cowshed/store/telemetry/`, `off` discards, and a supervising runtime injects its
  own sink through `ProjectRuntime::open_existing_with_audit` (routing the same records into its own durable log). A
  sink that refuses a record is a `doctor` finding (`audit-sink`), never a reason to fail the act it describes. Job
  admission, terminal and checkpoint records come from the workspace supervisors, which are processes of their own
  (11_shell.md): each records to the host's default sink, and a controller with a sink of its own reads each
  supervisor's records after the last cursor it forwarded, records them, and acknowledges them by cursor.
- **Arrow audit segments** — one Arrow IPC batch containing one row per segment at
  `<host-telemetry-root>/<yyyy-mm-dd>/commitment-<order:020>-<writer_uuid>.arrow`, where the UTC partition is an exact
  calendar date, `writer_uuid` is the lowercase hyphenated UUID of the controller process that wrote it, and `order` is
  that writer's own monotone sequence from 1. A completed segment is mode `0600`, sealed by create-new atomic rename,
  parent-directory-fsynced, and never reopened, replaced, or shared for append; a crash leaves at most an unsealed
  dot-prefixed temporary that nothing reads. Concurrent controllers never contend: names are unique by writer, so no
  lock and no global order exist. Partitioning by day is a query/retention layout, not shared-file append. This rule
  does not describe the separately locked protected `.cowshed/job/records.arrow` framed stream above.
- **Controller producer capability delivery** — each controller telemetry producer receives a dedicated IPC channel or
  inherited write-only capability/FD. It is close-on-exec/non-inheritable before any workspace child starts and is never
  named by a workspace-readable path or token. Audit records are recorded in the order the controller performed the
  acts, each acknowledged by the sink before the next; the short-timer one-batch crash window applies to diagnostic
  events; gateway decision boundaries retain the flush policy below.
- **The one text-file survivor** — `~/Library/Logs/cowshed/daemon-stderr.log`, the launchd `StandardErrorPath` target,
  exists only for crashes before tracer initialization. It lives off the `/private/cowshed/store` mountpoint so reboot
  remount cannot be masked. `doctor` flags it when non-empty.

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
`supervisorLost`). Records are written at `record_version` 5. Version-4 records carry two more Utf8 columns,
`warm_base, warm_head`, between `failure` and the exit columns; they are read with those columns skipped. Version-3
records, which have every column up to `failure`, and version-2 records, which lack `failure` too, are read as they
were, and a batch whose layout and version disagree is rejected. A CheckpointManifest row instead uses
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
```

A `LandAdoption` record is one Nx task that missed the cache when a land re-ran its check in the target on the build
volume it adopted (16_build_volumes.md, Land step 7): one record per miss, never one per land. `task` is the Nx task id,
`task_hash` the hash Nx computed in the target, and `inputs_digest` the SHA-256 of the task's hash inputs as
`nx show target inputs` named them (the land report carries the inputs themselves). Why: every such miss is a defect in
the project's Nx configuration (an input that differs between checkouts, an undeclared output, a nondeterministic step),
and the land succeeds regardless, so the land report alone is read once and forgotten. As records, a coordinator queries
every miss across lands and hosts, groups recurrences by `task` and `inputs_digest`, and turns them into fix work. The
landing and target incarnations must differ, and `task` and `task_hash` must be non-empty.

The flat controller Arrow columns are exactly `commitment_kind, commitment_version, commitment_order, repo_id` plus
variant-selected
`workspace_incarnation, job_id, grant_revision, state, stdout_bytes, stdout_sha256, stderr_bytes, stderr_sha256, batch_sha256, origin_incarnation, checkpoint_id, barrier_id, manifest_batch_sha256, source_incarnation, destination_incarnation, source_checkpoint, output_limit_bytes, output_crossing_bytes, replaced_incarnation, landed_head, task, task_hash, inputs_digest`.
A `landAdoption` row carries its landing and target incarnations in `source_incarnation` and `destination_incarnation`;
the last four columns (`landed_head`, `task`, `task_hash` as Utf8, `inputs_digest` as Binary) are its alone.
Non-selected fields are null and the tag controls required fields. Segments sealed before `LandAdoption` existed carry
only the first twenty-three columns; they are read as they are, every earlier kind decoding the same, because the four
columns they lack are null for every kind but `landAdoption`. A `landAdoption` row in such a segment is malformed and
refused. Why: sealed segments are never rewritten, and a refresh enumerates every one of them (below), so refusing the
shorter layout would fail every host's history closed. Version 1 segments are intentionally incompatible with this clean
schema cutover; no optional-field or legacy parser path exists for them.

`order` is the writing controller's own positive, strictly increasing sequence from 1; across writers a segment is
identified by `(order, writer_uuid)`, and nothing requires a host-global order because nothing replays the segments.
What the records used to prove — which incarnations exist, which retired, which lineage an image carries, which jobs
were admitted — is read from the inventory: the images and mounts, the marker each image carries (incarnation and
`lineage`, the ancestor incarnations nearest first), the grants files, and the protected records inside the image. A
records file may hold frames from its lineage and from no other incarnation; the supervisor refuses anything else as
`Integrity`.

Immutable publication uses the filesystem lock only to recover the complete segment set and append one create-new
segment. A publisher refreshes under that lock before assigning an order to a draft. A refresh re-enumerates every
sealed name — malformed, duplicate, gapped, or vanished history still fails closed — but opens, decodes, and folds only
segments beyond the store's last folded order: sealed segments are immutable, so history already folded in this process
is never re-read, and a refresh that finds nothing new costs one name enumeration. The publisher refreshes before every
request, so a refresh that finds nothing new must not cost a replay: a full replay per refresh scales every command with
host history times workspace count. If another valid writer advances the global sequence or deliberately wins the
same-order destination, the stale store adopts the recovered context and the publisher retries the unchanged draft at
the new next order with a bounded conflict count. Such a known concurrent advance is an operational conflict, not
integrity failure; malformed, duplicate, gapped, or lineage-invalid history still fails closed as `Integrity`. A draft
must validate both against the baseline-merged context and against replayed history alone; a draft only the opening
baseline could authorize is refused as `Integrity` before any segment is written, because it would seal and then fail
every later recovery. A draft is acknowledged exactly once only after its immutable segment is durable.

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
  ID-plumbing through uncooperative tools (the tier-2 interval join, lineage walks, "what could this workspace reach at
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
