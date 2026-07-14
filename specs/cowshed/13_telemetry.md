# Telemetry

cowshed's observability is **distributed tracing into Arrow columns**, not a text logfile. Every lifecycle operation,
every job, and every gateway request is a span; spans carry W3C trace context across cowshed's boundaries; and the spans
flush as Arrow record batches that are queryable, assertable, and cheap to retain. The substrate is **lmao**
(`packages/lmao-rs` / `packages/lmao`) — a spans-first tracer with deterministic Arrow encoding, arena-backed buffers,
and a tracer-agnostic query surface (`lmao-query`). There is no OTel collector and no telemetry daemon; OTLP export, if
ever wanted, is a projection from the Arrow store.

## Why lmao, not text logs

Text logs record _that_ things happened. Columns make cowshed's behavior a **dataset**: the same artifact answers
debugging (span waterfalls), security (audit joins), testing (trace assertions), and fleet ops (SLOs from real usage).
lmao is the right substrate specifically because it is trace-first and **deterministic** — with an injected `Clock` and
`Entropy` it emits bit-identical trace bytes for a given `(build, seed, config)` (see `packages/lmao-rs`), which is what
makes golden trace fixtures and the assertion surface below possible.

**Dependency honesty**: lmao-rs is today a pinned-API scaffold (hot paths unimplemented; port order
arena→core→arrow→macros→wasm→query). cowshed Phase 1 codes against the pinned `lmao-core`/`lmao-arrow` APIs; the
overhead gates arrive with the port. Sequencing lives in the kickoff.

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
traffic from tier-3 to tier-1 attribution (below).

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

## Attribution tiers

Under interception (05_gateway.md), most granted traffic is request-visible, so attribution sharpens by tier:

1. **Cooperative lmao clients** on the plain-HTTP mirror endpoints or sending `traceparent` on an intercepted host:
   **exact span parentage** — the gateway adopts the inbound context. The in-image `xcrun` wrapper (03_caches.md) is
   first-party code, so `/sim/` broker calls (05_gateway.md) are tier-1 by construction.
2. **`bun install`** — a per-job registry URL segment `http://127.0.0.1:<base>/npm/t/<traceparent>` injected via
   `BUN_CONFIG_REGISTRY` (verification items: env-vs-bunfig precedence, subcommand coverage — bun#617 — and the
   npmrc-overrides-bunfig bug bun#20593). The gateway strips the `/t/…` segment for **exact job attribution** of an
   otherwise-uncooperative native client. **`go`** is the same shape: a real `GOPROXY` env var takes precedence over the
   `GOENV` file (verification item, kickoff), so the supervisor can inject a per-job
   `GOPROXY=http://127.0.0.1:<base>/go/t/<traceparent>` for exact go-fetch attribution. cargo has no per-invocation
   source-replacement env, so cargo mirror traffic falls to tier 3.
3. **Everything else** — an intercepted host whose client sent no `traceparent`, an `--opaque` tunnel, cargo — is
   **workspace-exact** (port identity) and **job-attributed at query time** by an interval join (request timestamp
   within a job's start/end, same workspace incarnation) over two controller-owned tables. The resulting gateway span
   links the matching `job_id` to its standard lmao trace/thread/span identity; exact when a workspace's jobs don't
   overlap, heuristic when they do.

Interception makes tier 3 richer than a tunnel would: an intercepted request is workspace-exact **with request-level
detail** (verb, path, bytes) and an outbound-injected `traceparent`, even when the inbound client is silent.

## Storage: two authority tiers, one Arrow substrate

Cowshed uses Arrow IPC for both tiers, but placement and authority differ:

- **Protected in-volume evidence** — `.cowshed/job/records.arrow` contains allocation/lifecycle batches, terminal exec
  records, checkpoint manifests, bounded summaries, and small terminal stdout/stderr as Arrow Binary. Larger or
  checkpoint-forced streams spill lazily to protected `.cowshed/job/<job_id>/out|err` files. Complete batches and sealed
  files are authoritative captured-content evidence only within their recorded origin incarnation/checkpoint boundary.
  They are not workspace-writable: the supervisor is the sole live writer and the mandatory child profile denies every
  mutation beneath `.cowshed/job/**` before repository-controlled startup (04_sandbox.md/11_shell.md).
- **Controller continuity commitments and telemetry** — lifecycle spans, compact job commitments,
  checkpoint/fork/restore lineage, gateway audit, grant mutations, autosave/gc/doctor, and debug events flush under
  `~/.cowshed/telemetry/`. Controller job commitments own existence, lifecycle/status, ordering, lineage, byte counts,
  and expected hashes. They never contain inline output, a protected artifact path, a redirect source, or any other raw
  stdout/stderr payload duplication.
- **Controller immutable per-writer segments** — all projects on a host share one controller commitment segment
  namespace; it is not partitioned by repository. Authoritative commitments use exactly one Arrow IPC batch containing
  one row per segment at `<host-telemetry-root>/<yyyy-mm-dd>/commitment-<order:020>-<writer_uuid>.arrow`, where the UTC
  partition is an exact calendar date and `writer_uuid` is lowercase hyphenated UUID text owned by that controller
  session. A completed segment is mode `0600`, sealed by create-new atomic rename, parent-directory-fsynced, and never
  reopened, replaced, or shared for append. Recovery no-follow enumerates exact date partitions and commitment names,
  rejects links, malformed or duplicate names/orders, and accepts only one globally contiguous order across every
  repository and date partition. Validation applies each row to the context selected by its `repo_id`; a valid
  foreign-repository row is not corruption. Unrelated telemetry names and unsealed dot-prefixed temporary names are not
  commitment authority. Partitioning by day is a query/retention layout, not shared-file append. This rule does not
  describe the separately locked protected `.cowshed/job/records.arrow` framed stream above.
- **Controller producer capability delivery** — each controller telemetry producer receives a dedicated IPC channel or
  inherited write-only capability/FD. It is close-on-exec/non-inheritable before any workspace child starts and is never
  named by a workspace-readable path or token. Admission and terminal/checkpoint commitments are flushed and atomically
  published before their corresponding operation is acknowledged. The short-timer one-batch crash window applies only to
  non-commitment diagnostic/audit events; gateway decision boundaries retain the flush policy below.
- **The one text-file survivor** — `~/.cowshed/telemetry/daemon-stderr.log`, the launchd `StandardErrorPath` target,
  exists only for crashes before tracer initialization. `doctor` flags it when non-empty.

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
readable without base64 or lossy conversion.

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

The flat Arrow schema begins `record_kind, record_version, repo_id`. A Job row then uses
`workspace_incarnation, job_id, sequence, state, grant_revision`, followed by the existing
`stdout_storage_kind, stdout_source_path, stdout_inline_bytes, stdout_protected_path, stdout_bytes, stdout_sha256, stdout_summary_version, stdout_summary_text, stdout_summary_truncated`
and equivalent `stderr_*` columns, optional output-limit columns, and required `argv: List<Binary>`. A
CheckpointManifest row instead uses `origin_incarnation, barrier_id, visible_jobs, records_sha256`, with
`visible_jobs: List<Struct<workspace_incarnation,job_id,state,stdout,stderr>>`. Columns outside the selected variant are
null and validators reject every other null combination. Job recovery validates non-null raw argv elements, the
non-empty first argument, NUL exclusion, the 128 KiB element limit, and the 1 MiB total before allocating OS strings.

## Controller commitment schema

Controller continuity is the exact tagged/versioned union:

```rust
enum ControllerCommitment {
    WorkspaceIntroduced(WorkspaceIntroducedCommitment),
    WorkspaceRetired(WorkspaceRetiredCommitment),
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Checkpoint(CheckpointCommitment),
    Fork(ForkCommitment),
    Restore(RestoreCommitment),
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
    source_incarnation: WorkspaceIncarnation, destination_incarnation: WorkspaceIncarnation,
}
```

The flat controller Arrow columns are exactly `commitment_kind, commitment_version, commitment_order, repo_id` plus
variant-selected
`workspace_incarnation, job_id, grant_revision, state, stdout_bytes, stdout_sha256, stderr_bytes, stderr_sha256, batch_sha256, origin_incarnation, checkpoint_id, barrier_id, manifest_batch_sha256, source_incarnation, destination_incarnation, source_checkpoint`.
Non-selected fields are null and the tag controls required fields.

`order` is positive and belongs to one host-global, strictly increasing, gap-free sequence across all repositories.
`CommitmentPriorContext` therefore splits into a global `last_order` and a component-safe map from `RepoId` to that
repository's admissions, terminals, checkpoints, and incarnation lineage. Opening a project merges its verified active
and retired storage baseline only into that project's component. A foreign repository receives no authority from the
opening project's baseline: its history must first establish each incarnation through `WorkspaceIntroduced`, `Fork`, or
`Restore`, and `WorkspaceRetired` removes that authority. Identical incarnation bytes in different repositories remain
independent because every lookup is repository-scoped. Admission, terminal, checkpoint, fork source, and restore source
all reject an unknown or retired incarnation in their own repository. Constructors and Arrow/JSON encode/decode invoke
the same row and collection validator; no derive-only path bypasses it.

Immutable publication uses the filesystem lock only to recover the complete segment set and append one create-new
segment. A publisher refreshes under that lock before assigning an order to a draft. If another valid writer advances
the global sequence or deliberately wins the same-order destination, the stale store adopts the recovered context and
the publisher retries the unchanged draft at the new next order with a bounded conflict count. Such a known concurrent
advance is an operational conflict, not integrity failure; malformed, duplicate, gapped, or lineage-invalid history
still fails closed as `Integrity`. A draft is acknowledged exactly once only after its immutable segment is durable.

No commitment variant contains `inline_bytes`, `protected_path`, `source_path`, summary text, or output payload.
`reconcile_commitments` compares protected content counts/hashes plus Job `batch_sha256` and checkpoint
`manifest_batch_sha256`; missing/altered content, invalid complete frames, or order/lineage/digest contradiction is
typed `Integrity`. Neither tier overwrites the other. An incomplete trailing frame alone is successful reported recovery
with its `batch_sha256` retained for diagnosis.

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

## Querying

- **Capability-scoped**, mirroring the coordinator/worker split (07_api.md, 12_mcp.md): a coordinator queries controller
  continuity commitments and telemetry. A worker queries one workspace's reconciled lifecycle view and reads captured
  bytes representation-transparently from protected in-volume artifacts. Controller rows never serve raw output, and
  protected rows alone never claim cross-incarnation completeness.
- **`lmao-query` selectors are the assertion surface** for 08_testing.md. Escape/integration tiers assert over traces
  and commitments with `never`/`count` selectors rather than scraping text. Integrity joins explicitly require the
  protected terminal/manifest batch digests, counts, and stream hashes to match controller commitments.
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
  diagnostic segments. Controller commitment segments are continuity authority and may not be pruned into an order gap.
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
restoring the entire image. Compact controller commitments therefore own existence/status/order/lineage and the hashes
that detect rollback, while never duplicating raw output. Any disagreement is typed `Integrity`; no blanket “outside
wins” or “newer wins” rule exists.
