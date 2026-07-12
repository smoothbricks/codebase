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
- **Job records** carry `repo_id`, `workspace_incarnation`, and the workspace-local numeric `job_id` beside standard
  lmao `trace_id` / `thread_id` / `span_id` / `parent_thread_id` / `parent_span_id`, `grant_revision`, and `env_hash`
  (11_shell.md). `job_id` links the span to job control and `.cowshed/job/<job_id>/…`; it never substitutes for or
  reuses `span_id`. The durable correlation key is `(repo_id, workspace_incarnation, job_id)` (camelCase:
  `(repoId, workspaceIncarnation, jobId)`), so records copied through checkpoints remain distinct from jobs submitted in
  a later incarnation.
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

## Storage: Arrow segments, one substrate

There is **exactly one telemetry storage form**: lmao spans/events flushed as Arrow IPC segments. No parallel text log,
no NDJSON files, no per-command debug logs (Tradeoffs).

- **Controller telemetry + authoritative job state + gateway audit** — lifecycle operations, authoritative job
  transitions (including `output-limit`), gateway audit events, grant mutations, autosave/gc/doctor runs, and
  per-command debug events flush under `~/.cowshed/telemetry/`. The controller gives each producer either a dedicated
  IPC channel or a write-only inherited capability/FD; it is close-on-exec/non-inheritable before any workspace child
  starts and is never named by a workspace-readable path or token.
- **Immutable per-writer segments** — a segment has one exclusively allocated writer. A completed segment is sealed and
  never reopened or shared for append; publication is atomic, and rotation/recovery starts a new segment. Partitioning
  by day is a query/retention layout, not shared-file append. `cowshed gc` drops whole sealed segments.
- **The one text-file survivor**: `~/.cowshed/telemetry/daemon-stderr.log`, the launchd `StandardErrorPath` target — it
  exists for the crashes that happen **before a tracer can initialize** (bad binary, missing volume). It stays tiny
  because nothing else ever writes there; `cowshed doctor` flags it when it is non-empty.
- **Exec convenience projection** — `.cowshed/job/records.arrow` travels with checkpoints but is workspace-writable and
  therefore never authoritative. It may help reproduce a run, but controller queries, job status, denial correlation,
  output-limit state, audit joins, and all security decisions use controller-owned immutable segments. Full retained job
  output remains separate opaque byte files at `.cowshed/job/<job_id>/out` and `err`.
- **Diagnostic summaries** — both the in-volume job record and the store-side job event carry separate `stdout` and
  `stderr` stream structs, each `{path, bytes, summary}`. Their nested summaries are the same deterministic, bounded,
  versioned, RTK/ContextCrawler-style projections emitted on the shell control channel: ordered signature context plus
  fixed head/tail context, with fixed line/byte/match budgets. Configured secrets, tokens, and paths are redacted before
  any summary leaves the supervisor; no unredacted summary field or event exists. The summary `version` identifies the
  complete selection, normalization, and redaction ruleset, and `truncated` records that the projection omitted source
  bytes, so readers can compare like with like.
- **Durability policy.** Audit-relevant events flush on decision boundaries or a short timer, whichever comes first; the
  crash window is **at most one unflushed batch**, stated plainly. That window is the accepted price of a single storage
  substrate (Tradeoffs); events that must never be lost in-window (today: none identified) would warrant a per-event
  flush, not a second format.

## Event schema

One schema family across store segments and in-volume records — schema-regular and dictionary-heavy, which is the
columnar sweet spot. JSON/MCP uses the camelCase rendering of the same fields; Arrow names are canonical snake_case:

| Column                               | Arrow type                       | Notes                                                                                                                                                   |
| ------------------------------------ | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ts`                                 | `Timestamp(ns)`                  | per-trace anchored (lmao 01b3)                                                                                                                          |
| `kind`                               | `Dictionary<u8,Utf8>`            | `lifecycle-step` / `job` / `npm` / `cargo` / `go` / `intercept` / `opaque` / `connect` / `repo-mirror` / `sim` / `grant` / `autosave` / `gc` / `doctor` |
| `repo_id`, `ws`                      | `Dictionary<u16,Utf8>`           | stable repository identity plus workspace name                                                                                                          |
| `workspace_incarnation`              | `FixedSizeBinary(16)` (nullable) | required for jobs; immutable controller-minted incarnation                                                                                              |
| `job_id`                             | `UInt64` (nullable)              | workspace-local job handle; joins control, raw files, and trace; never a span id                                                                        |
| `trace_id`                           | `Dictionary<*,Utf8>`             | standard lmao trace identity; propagated from W3C `traceparent`                                                                                         |
| `thread_id`, `span_id`               | `UInt64`, `UInt32`               | standard lmao thread/span identity                                                                                                                      |
| `parent_thread_id`, `parent_span_id` | `UInt64`, `UInt32` (nullable)    | standard lmao parent identity; causal links use lmao's link column                                                                                      |
| `rev`                                | `UInt64`                         | grant revision in force                                                                                                                                 |
| `host` / `name`                      | `Dictionary<u16,Utf8>`           | egress host or package name                                                                                                                             |
| `method`, `path`, `url`              | `Utf8` (nullable)                | intercepted/mirror requests only                                                                                                                        |
| `status`, `decision`                 | `UInt16`, `Dictionary`           | HTTP status; `allow`/`deny`/`offline`; job state includes `output-limit`                                                                                |
| `bytes`, `duration_ms`               | `UInt64`                         |                                                                                                                                                         |
| `cache`                              | `Dictionary<u8,Utf8>`            | `hit`/`miss`, mirror endpoints only                                                                                                                     |
| `stdout`, `stderr`                   | `Struct` (nullable)              | `{path, bytes, summary:{version, text, truncated}}`; paths reference raw bytes retained before any output-limit trip; summary text is redacted          |
| `attrs`                              | `Map<Utf8,Utf8>`                 | kind-specific tail (exit code, errno, label, …)                                                                                                         |

Summary generation is a pure versioned projection: identical retained stream bytes, versions, and configuration emit
identical nested summary structs. Summaries are diagnostic and lossy; they **never** establish sandbox denial,
exit/signal, output-limit, policy or audit decisions, or build/test success. The combined per-job stdout+stderr capture
quota is configurable and defaults to 1 GiB; crossing it produces the authoritative `output-limit` state after
TERM/grace/KILL and pipe drain. Summary truncation is independent of that terminal state.

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

- **Capability-scoped**, mirroring the coordinator/worker split (07_api.md, 12_mcp.md): a **coordinator** queries the
  controller-owned store; a **worker** queries one-workspace job views whose authoritative state is served by the
  controller/supervisor capability, with the in-volume projection used only as non-authoritative reproduction data.
- **`lmao-query` selectors are the assertion surface** for 08_testing.md. The escape and integration tiers assert over
  traces with `never`/`count` selectors instead of scraping text: `never(gateway.allow ∧ host ∉ grants)`,
  `every(rm → supervisor-stop precedes detach)`, `count(secret-deny-ordering-violation) == 0`. Because lmao is
  deterministic under an injected `Clock`, integration runs emit **golden trace fixtures**, and the crash-recovery
  tables (02_workspaces.md adopt/rm) become schedules with trace assertions rather than hoped-for behavior.
- **`cowshed doctor --bench`** reports real p50/p99 from the store's accumulated lifecycle spans — turning the
  08_testing.md budgets from single-run estimates into SLOs monitored over actual usage (the direct fix for "a p99 from
  20 samples").

## What columns buy

- **Real distributions.** Every attach/clonefile/fsck ever run is the dataset; the n=20 benchmark problem dissolves.
- **Query-time correlation** links the numeric `job_id` to standard lmao trace/thread/span identity and replaces runtime
  ID-plumbing through uncooperative tools (the tier-3 interval join, lineage walks, "what could this workspace reach at
  time T vs what it tried").
- **Fleet questions, zero infra** — egress-denial hot spots, mirror hit rates, image-growth trajectories, grant churn,
  per-task cost — as queries over local files.
- **Cheap retention** — dictionary + zstd columnar audit is an order of magnitude smaller than NDJSON; gc drops
  segments.
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

**In-volume exec records are not status or audit authority.** They travel with checkpoints and aid reproduction, but a
job can rewrite them. Authoritative job lifecycle/quota events and gateway audit events are controller-owned immutable
per-writer segments; grant authority remains in controller-owned grant files. Any disagreement resolves in favor of the
outside-the-workspace source.
