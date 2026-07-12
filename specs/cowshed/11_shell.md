# Warm Shell Layer

`cowshed-shell` is the process-management layer between `cowshed-core` (which owns _what may run where_ — substrate,
mounts, sandbox profiles, grants) and every client that runs commands (CLI, MCP, CI, NAPI). It provides one long-lived
supervisor per workspace holding a pool of warm shells with the composed environment already loaded, a framed stdio
protocol over a Unix socket, job control, and the single exec-record capture that all clients consume.

## Why a pool at all — honest calibration

A fresh workspace is a CoW clone of main, so it inherits main's `.direnv`/`.devenv` caches; whether that makes a fresh
clone's _first_ evaluation cheap is a listed verification item (04_sandbox.md, devenv section — it needs a real
`cowshed new` to test). What is already measured is the steady state: **a warm `direnv exec` on a devenv repo costs
~1.3–2.9 s per invocation** — `direnv export` dominates, dwarfing `sandbox-exec` startup and shell init. That makes the
pool load-bearing, not a nicety: per-command environment composition (04's exec pipeline wraps every command in
fail-closed `direnv export`) is too slow to pay per exec across the dozens of commands an agent or CI job runs. The
pool's value:

- **The per-command residual, measured ~1.5–3 s on a devenv repo** — paid once per warm shell instead of once per
  command.
- **Persistent shell state** — a named session keeps cwd, shell variables, and running jobs across calls, so an agent's
  multi-step task is one shell, not N independent `sandbox-exec` spawns.
- **The framed stdio protocol** — multiplexed, backpressured, language-neutral I/O for concurrent CLI, NAPI, MCP, and CI
  clients, instead of each reinventing pipe plumbing.
- **Job control** — timeouts, auto-backgrounding, re-attach, structured capture.

The spec still states plainly: the pool is not what makes cowshed fast — the CoW clone and warm caches are that thing.
The pool is what keeps the _per-command_ cost from eating those wins.

## Supervisor

One supervisor process per attached workspace, spawned on first exec (or by `cowshed ensure` for warmed workspaces).
Each instance is launched **once per effective filesystem grant revision** under that revision's workspace sandbox
profile, so its whole process tree — every shell it forks, every child those shells spawn — inherits the
Seatbelt/Landlock boundary transparently, with no per-command wrapping (04_sandbox.md).

- Holds K warm shells (default 2, `.cowshed.toml` `[shell] pool`), each an interactive shell that has already sourced
  the workspace environment via `direnv export`. Anonymous shells are reset (`cd` to mount root, environment re-applied
  from the cached snapshot, shell vars cleared) and returned to the pool after each anonymous exec.
- **Named sessions** (`--session <name>`, `WorkspaceHandle::shell(Some(name))`) are persistent shells outside the pool:
  state survives across calls until explicitly closed or the supervisor stops. This is how a coordinator gives one
  subagent a stable shell for a multi-step task.
- Idle timeout: the supervisor exits after `[shell] idle` (default 30 min) with no sessions and no running jobs, freeing
  memory; the next exec respawns it. `cowshed detach`/`cowshed rm` stop it immediately (teardown below).

## Protocol

A persistent Unix socket per supervisor lives at `<runtime dir>/<owner>/<repo>/<workspace>.sock`. The controller derives
`owner` and `repo` from the primary `repo_id`, validates and encodes each as one component, then joins them; it never
places an unsplit identifier into a path. The controller creates the per-user runtime directory mode `0700`; the
supervisor binds the socket mode `0600` and verifies the connecting peer's uid (and platform peer credentials where
available) before accepting any frame. The socket remains bound for the supervisor's lifetime and accepts multiple
concurrent clients. Disconnecting one client detaches only that client's views: it neither stops jobs nor unlinks the
socket. Clients may reconnect, authenticate as the same one-workspace capability, query the durable job id, and resume
status/log/attach operations. The supervisor unlinks the socket only on orderly exit; startup may unlink a stale socket
only after proving that no live supervisor owns it. It never applies the short-lived pattern of unlinking after the
first client.

Framing is deliberately minimal and binary so the NAPI and MCP clients need no parser beyond a length read:

```
frame = channel:u8  length:u32-le  payload:[u8; length]
channel = 0 control(JSON)  1 stdin  2 stdout  3 stderr  4 events(JSON)
```

- **control** — request/response JSON: `open-session`, `run`, `signal`, `resize`, `close`. An accepted `run` response
  always includes the numeric `jobId`, allocated before process creation; a spawn failure is therefore a terminal job,
  not a response with no identity.
- **stdin** — a structured binary source selected by the exec request: empty, inline bytes, a backpressured client
  stream, or a workspace-relative regular file. Inline/stream bytes arrive on channel 1; the workspace-file request
  carries only the normalized relative path and the supervisor opens it inside the sandbox boundary. Bounded queues
  propagate child-pipe backpressure to the source; bytes are never interpolated into shell text.
- **stdout/stderr** — raw bytes, backpressured per client. A slow or disconnected client never blocks capture: capture
  drains the child into the backing files independently, while a live client receives bytes subject to its own bounded
  queue and resumes from a backing-file offset after reconnect. These channels are live transport; retained raw bytes
  are the separate backing files described below.
- **events** — asynchronous JSON notifications: `job-started`, `job-backgrounded`, `job-exited`, `session-closed`. Every
  job event carries the durable `(repoId, workspaceIncarnation, jobId)` key and standard lmao
  `traceId`/`threadId`/`spanId` plus nullable `parentThreadId`/`parentSpanId`. `job-backgrounded` and `job-exited` also
  carry `stdout` and `stderr` `StreamInfo` objects with the current byte count, backing path, and bounded summary. A
  client that only wants completion can ignore the byte channels and watch events.

The protocol is transport for both the CLI (which renders it to the stdout/stderr contract, 06_cli.md) and the MCP
server (which renders it to tool results, 12_mcp.md). One protocol, three front-ends. JSON carries control/result
metadata and summaries only; arbitrary stdout/stderr bytes never enter JSON.

## Job control

Every exec submission is a _job_, foreground or backgrounded. At admission the supervisor allocates a positive `u64`
`jobId`, monotonically increasing within that workspace. Decimal rendering is canonical, with no prefix or zero padding.
The raw streams and in-volume record stream are:

```
.cowshed/job/<jobId>/out
.cowshed/job/<jobId>/err
.cowshed/job/records.arrow
```

`out` and `err` are separate opaque byte streams admitted under the combined quota: no UTF-8 assumption, line rewriting,
redaction, merging, or summary substitution. The supervisor creates each job directory exclusively before spawning the
process and opens both files without following symlinks.

Allocation is exclusive and crash-safe without a counter file or state database. One supervisor is the sole allocator
for an attached workspace. At startup it scans canonical decimal job directories, chooses `max(existing) + 1` (or `1`
when none exist), and reserves that number by creating `.cowshed/job/<jobId>/` exclusively before spawn. An unexpected
`AlreadyExists` advances and retries; malformed names are ignored for allocation and reported by `doctor`. The created
directory is the durable allocation record, so a crash may leave a terminal pre-spawn attempt but cannot cause reuse.
Supervisor replacement and attach repeat the scan before admitting an exec. Fork/checkpoint/restore copies reconcile
above the inherited maximum; no separate high-water file exists.

A controller-minted immutable `workspaceIncarnation` disambiguates histories copied by fork/checkpoint/restore. Each
create, fork destination, and restore result receives a fresh incarnation; inherited records retain the incarnation that
produced them, and the new allocator starts above the inherited maximum. Thus the durable job key is
`(repoId, workspaceIncarnation, jobId)`: `jobId` is the familiar workspace-local handle and path component, while the
full tuple remains unique across checkpoint copies and recycled workspace names.

- **Soft timeout → auto-background.** A foreground command still running at its soft timeout (`--timeout`, default
  `[shell] soft_timeout` = 120 s) is detached: it keeps running, its `out` and `err` files keep growing, and an
  `event: job-backgrounded` fires. The client already has the job id and can walk away — the classic agent pattern of
  "this build is slow, background it and poll." Because the files are in-volume, a later `cowshed checkpoint` captures
  them alongside the filesystem the job produced.
- **`--background`** forces detachment immediately.
- **Hard timeout** (`[shell] hard_timeout`, unset by default, set by CI — 10_ci.md) → SIGTERM, then SIGKILL after a
  grace, drain both pipes to EOF, then mark the job `killed:timeout`. This is how CI bounds a hung step.
- **Combined output quota.** Each job has one configurable quota across stdout and stderr, default **1 GiB**. Accounting
  includes bytes already persisted plus bytes read from either child pipe but still in flight to the backing files. The
  first read whose inclusion would cross the quota atomically trips the limit: the supervisor stops admitting payload
  bytes beyond the boundary, sends SIGTERM to the complete process group, waits the configured grace, SIGKILLs
  stragglers, and continues draining both pipes to EOF so no writer can deadlock teardown. After drain and fsync, the
  authoritative terminal state is `output-limit` (distinct from timeout, signal, and ordinary nonzero exit), with the
  configured limit and observed crossing recorded. This is explicit bounded capture, never silent tail truncation;
  summaries report their own independent projection truncation.
- **Re-attach**: `cowshed job attach` / `Job::attach` re-opens stdio to a running job (replaying from the client's last
  acknowledged backing-file offsets, then live). `cowshed job logs` streams `out` and/or `err` (`--follow` to tail).

### Exec records and output summaries

Every job emits lifecycle records to two projections. The workspace-local `.cowshed/job/records.arrow` remains a
checkpoint-travelling convenience copy. **Authoritative job telemetry is written outside the workspace** by the
controller telemetry writer. At supervisor launch the controller supplies either a dedicated IPC channel or an
inherited, write-only capability/FD for that writer; it is marked close-on-exec/non-inheritable before any workspace
process is spawned and is never represented by a path or token readable inside the workspace. The supervisor cannot
reopen or delegate it, and children never inherit it.

Each writer appends only to its own immutable segment: segment names are exclusively allocated, a segment has one
writer, completed segments are sealed and never reopened or shared for append, and publication is atomic. Recovery may
publish a new segment containing a terminal `supervisor-lost` event; it never edits a sealed segment. Controller
queries, status authority, quota terminal state, denial correlation, and audit joins use these controller-owned
segments, **never** workspace-local records. If the two projections disagree, the controller-owned segment wins.
Completion is a record-batch boundary in both projections.

| Field                                                                        | Example                                                           | Notes                                                                                                                         |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `repo_id`, `job_id`, `workspace_incarnation`                                 | `acme/widget`, `42`, `01J…`                                       | canonical durable key; numeric local handle plus checkpoint/incarnation identity                                              |
| `argv`, `cwd`, `stdin`                                                       | `["bun","test"]`, `.`, `{kind:"stream",bytes:4096,complete:true}` | binary stdin metadata; workspace path is relative when present                                                                |
| `env_hash`, `grant_revision`                                                 | `…`, `7`                                                          | reproduce a run: same env, same authority                                                                                     |
| `trace_id` / `thread_id` / `span_id` / `parent_thread_id` / `parent_span_id` | standard lmao values                                              | `job_id` is not a span id; parent identity is nullable                                                                        |
| `started`, `duration_ms`                                                     | `2026-07-11T12:34:56Z`, `8421`                                    |                                                                                                                               |
| `exit`, `signal`, `state`                                                    | `1`, `null`, `exited`                                             | authoritative process result; `state` also includes `output-limit`                                                            |
| `stdout`, `stderr`                                                           | stream structs                                                    | `{path, bytes, summary}` for `.cowshed/job/42/out` and `err`; paths reference raw bytes retained before any output-limit trip |

Each `StreamInfo.summary` is a deterministic, bounded, versioned **diagnostic summary**, inspired by RTK/ContextCrawler:
retain a small ordered context window around compiler/test/error signatures plus deterministic head/tail context,
normalize only the summary text, redact configured secret/token/path patterns before emission, and apply fixed byte,
line, and match budgets. The summary object is `{version, text, truncated}`: `version` identifies the complete
selection, normalization, and redaction ruleset; `text` is redacted UTF-8; and `truncated` says the bounded projection
omitted source bytes. Identical bytes plus an identical version and configuration produce identical output. Redaction
happens before a summary enters a control event, MCP result, in-volume Arrow record, or store-side Arrow segment; no
unredacted summary variant is retained. `stdout.summary` and `stderr.summary` are always separate and use the same
algorithm and limits.

Summaries are convenience evidence only. They **never** establish a sandbox denial, exit status, signal, output-limit,
policy/audit decision, or build/test success; those come from authoritative structured process, quota, sandbox, CI, and
gateway fields. They may omit decisive text by design. Full bytes retained up to the combined job-output quota remain
exclusively in `out` and `err`; crossing the quota terminates and drains the job into the explicit `output-limit` state.
No stream is silently truncated while the process continues.

The workspace-file source is opened after job-id allocation with an `openat`-style, read-only, no-follow component walk
rooted at the workspace mount. It must canonicalize beneath that mount and resolve to a regular file; absolute paths,
`..` escapes, symlinks (including ancestors), directories, devices, sockets, and replacement races fail closed as a
terminal spawn-stage job. EOF from any source closes child stdin exactly once. Client-stream cancellation closes the
source and child stdin and records incomplete stdin delivery; it does not implicitly kill the job. If the job exits or
is killed first, the supervisor cancels the producer, stops reading the file/client, and releases backpressure waiters.
Every job event and authoritative record carries stdin kind, bytes delivered, completion, and the normalized relative
file source when applicable, plus the existing job and trace identity; inline contents are never telemetry.

Inspect with `cowshed ps <ws> --json`, `cowshed job list <ws> --json`, or generically with `lmao-inspect`
(13_telemetry.md). This is the _only_ capture implementation in cowshed. CLI, MCP job tools, and CI diagnostics read the
same records and raw files; no client re-captures or derives authority from summary text. The standard lmao trace
identity locates the job span, while `job_id` links that trace to the workspace-local raw files and job-control handle.
Raw streams are the byte record admitted under the combined quota; only diagnostic summaries use lossy text projection.
A quota crossing is separately explicit in authoritative terminal state.

**Not authoritative.** Workspace-local records live _inside_ the workspace volume, which is workspace-writable by
construction — a job can rewrite its own `records.arrow`, `out`, and `err`. They are a reproduction/observability aid,
not status or audit authority. Authoritative job lifecycle and quota state live in controller-owned immutable writer
segments; grant authority lives in controller-owned grant files; gateway decisions live in controller-owned audit
segments. Outside-the-workspace sources always win.

## Grant-change propagation

When a coordinator applies an effective filesystem `grant`/`revoke` (04_sandbox.md, 07_api.md), the immutable outer
profile changes. The old supervisor cannot widen or revoke its inherited authority by nesting another profile, so it:

1. stops accepting new exec submissions and marks anonymous pooled shells stale;
2. drains jobs already admitted under the old revision, then closes its pooled shells and exits;
3. is relaunched by the controller under a supervisor profile compiled from the new grant revision, re-applying the
   cached environment snapshot (no Nix re-evaluation — the environment is unchanged, only the sandbox boundary moved);
4. reports the new enforced `grant_revision` on subsequent jobs.

Running jobs launched under the old profile continue under it until they end or are killed. A coordinator needing a hard
cut kills those jobs before the drain completes. No-op mutations and egress-only or simulator-only mutations do not
restart the supervisor because they do not alter its filesystem profile.

**Named sessions refuse to cross revisions.** A named session is a persistent shell pinned to the supervisor profile it
launched with. Once a filesystem grant/revoke advances the revision, a later `run` targeting that stale session is
rejected with `Conflict` naming the enforced and current revisions; it is never silently migrated or resumed. The caller
opens a new named session after relaunch. Anonymous shells need no migration rule because the old pool is destroyed with
the supervisor and a fresh pool is created under the new outer profile.

## Teardown ordering

`cowshed rm` / `cowshed detach` must stop the supervisor **before** unmounting or destroying the substrate, because live
children hold the mount busy and would force `-force` detaches or `zfs destroy` retries. The daemon/long-lived-child
population is large and project-specific — Nx daemon, Gradle/Kotlin daemons, watchman, Metro, `workerd`/miniflare,
Verdaccio, DynamoDB Local, MinIO, Jupyter kernel gateways, LSP servers, DAP adapters, `devenv up` service trees, and
anything a job double-forks — so the supervisor never enumerates by name; it tracks its **process group** and tears down
the whole descendant tree regardless of what those processes are. Order:

1. supervisor SIGTERMs its session/job tree, waits the grace, SIGKILLs stragglers;
2. supervisor exits; only then is its persistent socket unlinked;
3. only then does `cowshed-core` detach/destroy (02_workspaces.md, 09_substrates.md).

The supervisor tracks its descendants (process group) precisely so this is deterministic — another reason the shell
layer is not optional glue but core to correct lifecycle.

## ensure / doctor awareness

- `cowshed ensure` (workspace attached) checks supervisor liveness and restarts it if the socket is dead; it never
  starts one proactively on the ≤25 ms fast path unless a stale socket is found.
- `cowshed doctor` reports orphaned sockets (no live supervisor), stuck jobs (running past hard timeout with a dead
  supervisor), and job directories exceeding budget, each with a `fix:` hint.

## Tradeoffs

**Per-command sandbox wrapping rejected as the default.** Wrapping every command in its own `sandbox-exec` (the fallback
path, still available for one-shot `cowshed exec` with no supervisor) pays the full startup cost per command and cannot
hold persistent shell state. Launch-time supervisor sandboxing pays it once; the cost — grants bind only at shell
relaunch — is surfaced, not hidden.

**A cross-workspace shared supervisor rejected.** One supervisor serving many workspaces would straddle sandbox
boundaries and couple unrelated workspaces' lifecycles. One supervisor per workspace keeps the boundary and teardown
clean while its persistent socket still supports multiple concurrent clients.

**Text-log capture rejected.** Scraping merged terminal text is what forces agents into brittle `grep` parsing.
Structured exec records with separated raw streams and the grant/env context make a failed run reproducible instead of
merely readable (10_ci.md). Records are Arrow, not a line log, for the same reason all cowshed telemetry is
(13_telemetry.md): one substrate, queryable and joinable against the store-side spans — "diff the job histories of these
two forks" is a query, not a diff of text files.

## Shell text and output redirection

`cowshed exec` is the token-efficient, safe shell path for agent harnesses. Shell semantics are normally delegated to
the shell, but cowshed may optimize trivial, very common redirections **only after parsing with a real shell AST
parser**. Regexes, substring checks, token sniffing, and ad-hoc quote handling are forbidden.

The fast path is eligible only for one top-level simple command with a literal, in-workspace, same-filesystem,
nonexistent destination for `>path` and/or `2>path`, using ordinary clobber semantics. It is ineligible for append, fd
duplication, pipelines, lists, subshells, command/parameter/glob expansion, symlink destinations or ancestors,
`noclobber`, existing destinations, cross-filesystem paths, or any syntax/condition not affirmatively proven safe. The
supervisor exclusively creates each eligible destination inode, then may make the corresponding `.cowshed/job/<id>/out`
or `err` path a hardlink to that same inode and tail/account that inode once for streaming, summaries, and the combined
output quota. Retention never unlinks the caller's destination, and quota accounting never double-counts the two names.

Parsing is an optimization only — never an authorization, denial, or correctness dependency. Any parse ambiguity,
ineligible AST, failed canonical containment/same-filesystem/nonexistence check, or inode/link operation failure falls
back before execution to ordinary shell execution plus normal supervisor capture. The fallback must preserve exact shell
behavior. A future structured copy/export API may clone or copy a sealed backing file (using `clonefile` where
supported); it is separate from shell parsing.
