# Telemetry & tracing

cowshed's observability is **distributed tracing into Arrow columns**, not a pile of text logs. Every lifecycle
operation, every job, and every gateway request is a span; spans carry a W3C trace id across cowshed's boundaries; and
they flush as Arrow segments you query with `cowshed logs` / `cowshed audit` / `cowshed trace`. There is one storage
substrate ([lmao](https://github.com/smoothbricks)), no NDJSON files on disk, and no telemetry daemon. (Spec:
`specs/cowshed/13_telemetry.md`.)

## Why not a logfile

Text logs record _that_ things happened. Columns make cowshed's behavior a **dataset** — the same artifact answers
debugging (span waterfalls), security (audit joins), testing (trace assertions), and fleet ops. Concretely:

- `cowshed doctor --bench` reports real p50/p99 for `attach`/`clonefile`/`fsck` from every run ever recorded, not a
  benchmark guess.
- "What did this workspace try to reach, and what was denied?" is `cowshed audit --ws X` — one query, not a grep across
  rotated files.
- Retention is `cowshed gc` dropping whole day-segment files; columnar audit is an order of magnitude smaller than the
  equivalent NDJSON.

## The three verbs

```sh
cowshed logs  [--ws X] [--kind lifecycle|job|grant|gc|…] [--since 1h] [--follow]   # controller telemetry
cowshed audit [--ws X] [--denied] [--host H] [--follow]                            # gateway egress decisions
cowshed trace <trace-id>                                                           # terminal span waterfall
```

Human tables by default; `--json` (one envelope) or `--ndjson` (one event per line) to pipe into `jq`. **NDJSON only
ever exists on the pipe** — nothing writes it to disk. Under the hood these wrap the generic `lmao-inspect` reader over
the controller-owned Arrow segments in `~/.cowshed/telemetry/`. Workspace-local `.cowshed/job/records.arrow` travels
with checkpoints as reproduction data, but is editable and never authoritative.

## Job authority and writers

Authoritative job lifecycle, exit/signal, output-limit, and denial-correlation events live outside workspaces in
controller-owned telemetry. Every job event carries the durable `(repoId, workspaceIncarnation, jobId)` key; the numeric
`jobId` remains a workspace-local handle, while the full tuple distinguishes copied and restored timelines. At
supervisor launch the controller provides each producer a dedicated IPC channel or a write-only inherited capability/FD;
it is made non-inheritable before any job starts. Each writer owns a separately allocated segment, publishes it
atomically, and never reopens a sealed segment or shares append ownership. Recovery starts a new segment. If controller
telemetry and an in-workspace record disagree, controller telemetry wins.

stdout and stderr remain separate job payload files. They share a configurable capture quota, default 1 GiB, whose
accounting includes persisted and in-flight bytes. A crossing yields TERM/grace/KILL of the process group, pipe drain,
and the explicit authoritative `output-limit` terminal state. cowshed never silently truncates a stream while the job
continues. Bounded diagnostic-summary truncation is independent and cannot establish status or policy.

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
coordinator, the `grant`, and the retry are four events under one trace id — `cowshed trace <id>` shows the whole
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
capability-scoped: a coordinator queries the controller-owned store; a worker sees one-workspace job views through the
supervisor/controller capability. Local records remain non-authoritative even when exposed for reproduction.
