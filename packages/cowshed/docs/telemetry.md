# Telemetry & tracing

cowshed's observability is **distributed tracing into Arrow columns**, not a pile of text logs. Every lifecycle
operation, every job, and every gateway request is a span; spans carry a W3C trace id across cowshed's boundaries; and
they flush as Arrow segments under `/private/cowshed/store/telemetry/`. There is one storage format (lmao's Arrow trace
schema), no NDJSON files on disk, and no telemetry daemon. (Spec: `specs/cowshed/13_telemetry.md`.)

> **Implementation status — process monitoring:** per-process tree observation, resource/blocker events, and the compact
> process/job spans described below are unbuilt. Existing gateway and continuity segments are not that tree. Complete
> cgroup job accounting, charged-memory counters, event-source measurement and reconciliation rows are also unbuilt.

## Why not a logfile

Text logs record _that_ things happened. Columns make cowshed's behavior a **dataset** — the same artifact answers
debugging (span waterfalls), security (audit joins), and fleet ops. "What did this workspace try to reach, and what was
denied?" is one query over the gateway's audit columns, not a grep across rotated files, and columnar audit is an order
of magnitude smaller than the equivalent NDJSON.

## Compact process observations

The supervisor observes the complete owned process tree and exposes typed snapshots/events through the generated
controller and Node-API declarations. Each process has one `process.run` span with birth/exit boundaries and process
tree parentage. Fourteen shared custom columns hold PID, dictionary-encoded program, one start-row argv display, CPU
user/sys microseconds, RSS/current peak, I/O read/write bytes, exit code/signal, and blocker kind/path/holder PID. The
exact declaration is [Process-tree spans](../../../specs/cowshed/13_telemetry.md#process-tree-spans).

Rows record state/blocker changes, RSS 2× steps, and busy/idle transitions, plus a coarse heartbeat per progress tick.
Only changed columns are set; the terminal row carries final usage. Host start/end load and cores and the signed
workspace/build-volume deltas stay on the job span. There are no JSON string columns or new columns per process, metric,
or sample. Missing blocker evidence is not a claim that a process is unblocked.

Job totals have an independent source: Linux uses each job's cgroup v2 CPU, charged-memory and storage-I/O counters;
macOS uses the leader's own/children rusage reconciliation. Charged memory is not RSS. Missed process events do not
erase those totals: an explicit unattributed CPU/I/O row and typed coverage gap say what remains unattributed. An
unknown leaf is never guessed for a command baseline.

Linux pidfd/proc polling does not capture every short-lived fork/exec. A measured implementation compares proc connector
through the privileged helper and ptrace event tracing on the same fork-heavy workload before selecting the event
source; macOS uses fork/exec/exit kqueue events. The burst-between-polls case is the coverage oracle, not just a
long-lived process census.

## Reading it

cowshed has no reader verb for it: each segment is an Arrow IPC stream, readable with any Arrow library. The layout:

```
/private/cowshed/store/telemetry/
  <yyyy-mm-dd>/commitment-*.arrow   # controller commitments: lifecycle and job continuity records
  gateway/<yyyy-mm-dd>/gateway-*.arrow   # gateway egress decisions, one event per decision
  daemon-stderr.log, sccache-stderr.log  # launchd stderr of the two agents, for crashes before tracing starts
```

Those segments are compact continuity records, not a second copy of job stdout/stderr.

## Tiered job authority and writers

Every job is identified durably by `(repo_id, workspace_incarnation, job_id)`. Protected records and canonical stream
artifacts inside the volume are authoritative for content within the incarnation and checkpoint snapshot that created
them. Each stream is `StreamInfo { storage, bytes, sha256, summary }`, where `storage` is `Captured { artifact }` or
`Redirect { source, artifact }`, and the protected `artifact` is `Inline { data: BinaryData }` or
`File { path: WorkspacePath }`. `Redirect.source` is live caller-visible state, never authority. Small terminal content
may remain inline as Arrow Binary; protected spill files under `.cowshed/job/**` are created lazily. In-volume Arrow
keeps this columnar with `storage_kind`, `source_path`, `inline_bytes`, and `protected_path`, without invalid
optional-field combinations.

The supervisor is the sole writer to `.cowshed/job/**`. Before any repository-controlled shell, named session, command,
or descendant starts, its child restriction removes write authority to that subtree. Protected records share one
append-only framed Arrow stream at `.cowshed/job/records.arrow`; the store lock serializes complete, synced frame
publication, and recovery may truncate only an incomplete trailing frame. Sealed spill files are never discarded as
recovery debris. Missing committed content, an invalid complete frame, or a digest mismatch is an explicit integrity
failure.

Authority is the host inventory, not a log. What workspaces exist, which incarnation each is, which are retired, and
which ancestors an image was cloned from are read from the images and mounts under `/private/cowshed/store/`, the marker
each image carries (`.cowshed/workspace.json`: incarnation and `lineage`, nearest ancestor first — written by the
controller when it mints the incarnation, because a fork or restore clones the source image together with the job
records its ancestors wrote, and the lineage is what authorizes those records), the per-workspace grants files, and the
controller lock. A controller opening a project reads the inventory once and starts; per-command cost does not grow with
history.

Controller audit records are telemetry. Every controller act — workspace introduced/retired, job admission and terminal
state, checkpoint, fork, restore — is emitted as one typed record carrying existence, lifecycle/status, a writer-local
order, lineage, grant revision, byte counts, stream SHA-256 digests, and terminal-batch digest, never inline bytes,
spill paths, or duplicated raw payload. Nothing reads them for a decision. The sink is chosen when the project opens
(`COWSHED_CONTINUITY_AUDIT`): `arrow` (the standalone default) writes one sealed Arrow IPC segment per record under
`/private/cowshed/store/telemetry/<yyyy-mm-dd>/commitment-<order:020>-<writer_uuid>.arrow` — private mode, fsync,
create-new rename, directory sync, no lock and no global order because names are unique per writer; `off` writes
nothing; and a runtime that supervises the controller injects its own sink through
`ProjectRuntime::open_existing_with_audit`, routing the same records into its durable log instead of files. A sink that
refuses a record is an `audit-sink` finding in `cowshed doctor`, never a failed act. Rollback and omission are therefore
after-the-fact questions for the audit trail: a restore is a controller verb a workspace cannot perform on itself, so no
decision waits on proving one did not happen.

Checkpoint publication crosses a supervisor barrier that seals complete batches and spill files and writes a manifest
covering every checkpoint-resident job byte. Restoring that snapshot mints a new workspace incarnation; controller
commitments preserve the lineage and detect omission or rollback, while the protected content remains scoped to its
origin snapshot.

stdout and stderr share a configurable capture quota, default 1 GiB, whose accounting includes persisted and in-flight
bytes. A crossing yields TERM/grace/KILL of the process group, pipe drain, and the explicit authoritative `output-limit`
terminal state. cowshed never silently truncates a stream while the job continues. Bounded diagnostic-summary truncation
is independent and cannot establish status or policy.

## Trace propagation (what "distributed" buys you)

cowshed uses W3C `traceparent`. Every entry point **mints or adopts** a trace:

- The **CLI** adopts an inbound `TRACEPARENT` from your environment if present, else mints a root — so if your agent
  harness is already traced, cowshed's spans nest under it.
- **CI** derives the trace id deterministically from `(run_id, attempt)`, so a job's trace is findable straight from the
  GitHub run — no lookup table.
- The **shell supervisor injects `TRACEPARENT` into each job's environment** and records structured stdin metadata:
  source kind, bytes delivered, EOF completion, and an optional normalized workspace-relative file path. Inline binary
  input is never stored in telemetry. The framed stdin channel preserves backpressure and cancellation semantics while
  keeping input separate from shell text.
- **CoW lineage is linked**: a workspace's marker records the trace that created it, and `fork`/`restore`/`checkpoint`
  link back — from a gateway denial you can walk to which task cloned this workspace from which state of main.

The payoff you'll feel most: **the grant-escalation loop is one trace.** A denial (exit 6), the worker asking its
coordinator, the `grant`, and the retry are four events under one trace id — one filter on that id gives the whole
negotiation instead of four disconnected log lines.

## Gateway attribution

Because most granted egress is intercepted (see [gateway.md](gateway.md)), the gateway sees requests and stamps
`traceparent` on the upstream leg. How precisely a request maps back to a _job_:

1. **Exact** — a cooperative (lmao-instrumented) client sends `traceparent`; the gateway adopts it.
2. **Exact for `bun install`** — the supervisor injects a per-job registry URL segment the gateway strips, so native
   `bun install` traffic is job-attributed without bun cooperating (a verification item; see the kickoff).
3. **Workspace-exact, job-by-time** — everything else is attributed to the workspace by its port, and joined to a job by
   timestamp. Exact when a workspace's jobs don't overlap.

## For agents

An in-workspace agent doesn't configure any of this — `TRACEPARENT` is already in its environment. Query surface is
capability-scoped: a coordinator queries continuity commitments, while a worker sees one-workspace job views through the
supervisor/controller capability. Ordinary responses are bounded: they expose lifecycle metadata, typed artifact
handles, hashes, summaries, and may include small `Inline.data` bytes tagged as `utf8` or `base64`. Unbounded bytes
require explicit `JobHandle.logs`, `JobHandle.attach`, or artifact-read streaming and remain separate for stdout and
stderr.
