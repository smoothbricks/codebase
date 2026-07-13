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
profile. That outer profile is the authority ceiling for the process tree. Every anonymous shell, named session, and
one-shot command also receives a deterministic inner profile which denies writes beneath `.cowshed/job/**` and applies
request-specific narrowing such as ReadOnly (04_sandbox.md). The inner profile can remove authority but never add it.

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
The protected raw streams and in-volume record stream are:

```
.cowshed/job/<jobId>/out
.cowshed/job/<jobId>/err
.cowshed/job/records.arrow
```

`out` and `err` are separate opaque byte streams admitted under the combined quota: no UTF-8 assumption, line rewriting,
redaction, merging, or summary substitution. The trusted supervisor creates each job directory exclusively and opens
both files without following symlinks before spawning the command. Executed shells receive a child profile that denies
every write mutation beneath `.cowshed/job/**`; no writable artifact descriptor is inheritable. The supervisor is the
only live writer, and completion drains, fsyncs, closes, and seals the backing files before publishing terminal state.

Allocation is exclusive and crash-safe without a counter file or state database. One supervisor is the sole allocator
for an attached workspace. At startup it scans canonical decimal job directories, chooses `max(existing) + 1` (or `1`
when none exist), and reserves that number by creating `.cowshed/job/<jobId>/` exclusively before spawn. An unexpected
`AlreadyExists` advances and retries; malformed names are ignored for allocation and reported by `doctor`. The created
directory is the durable allocation record, so a crash may leave a terminal pre-spawn attempt but cannot cause reuse.
Supervisor replacement and attach repeat the scan before admitting an exec. They validate `records.arrow`, discard only
an incomplete trailing Arrow batch left by an unclean supervisor exit, and never rewrite a complete batch or sealed
backing file. Fork/checkpoint/restore copies reconcile above the inherited maximum; no separate high-water file exists.

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

Every job emits protected lifecycle records to `.cowshed/job/records.arrow`. The file travels with checkpoints and is
authoritative for the complete batches written by the trusted supervisor within the recorded
`workspaceIncarnation`. Executed jobs may read it but cannot write, truncate, rename, unlink, hardlink, or replace it.
Completion is a record-batch boundary. After an unclean supervisor exit, recovery validates the stream and may truncate
only an incomplete trailing batch before appending a new recovery batch; malformed complete data is an integrity error,
not child-authored input to reinterpret.

Cross-incarnation completeness still requires a controller-owned commitment. At supervisor launch the controller
supplies either a dedicated IPC channel or an inherited, write-only capability/FD for its telemetry writer; it is
close-on-exec/non-inheritable before any workspace process is spawned and is never represented by a path or token
readable inside the workspace. The supervisor cannot reopen or delegate it, and children never inherit it. Admission
publishes the durable job key; after terminal artifacts are drained, fsynced, and sealed, the supervisor publishes a
compact commitment containing the terminal state, grant revision, byte counts, incremental SHA-256 stream digests, and
the digest of the terminal record batch. Controller writers use exclusively allocated immutable segments: one writer
per segment, atomic publication, no shared append, and no reopening after seal.

Protected in-volume records and streams are authoritative job evidence for their originating incarnation. Controller
commitments are authoritative for job existence, cross-incarnation ordering, fork/restore lineage, and detection of
later omission, mutation, or rollback. A mismatch is an explicit integrity failure; neither side silently overwrites
the other. Grant authority remains in controller-owned grant files and gateway decisions remain in controller-owned
gateway audit segments.

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
gateway fields. They may omit decisive text by design. Complete bytes observed on the child pipes and admitted up to the
combined job-output quota remain exclusively in the protected `out` and `err` files; crossing the quota terminates and
drains the job into the explicit `output-limit` state. No stream is silently truncated while the process continues.

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
same protected records and raw files; no client re-captures or derives authority from summary text. The standard lmao
trace identity locates the job span, while `job_id` links that trace to the workspace-local raw files and job-control
handle. Raw streams are the byte record admitted under the combined quota; only diagnostic summaries use lossy text
projection. A quota crossing is separately explicit in authoritative terminal state.

**Tiered authority.** Physical placement inside a workspace image does not imply executed-job write authority. The
mandatory child profile excludes `.cowshed/job/**`, the supervisor is its sole live writer, and completed files are
sealed. Those artifacts are authoritative within their originating incarnation. They cannot alone prove that a newer
job or incarnation was not omitted by restoring or deleting the whole image, so compact controller commitments preserve
cross-incarnation continuity and detect rollback. Grant files and gateway audit remain controller-owned. A commitment
mismatch or missing committed artifact is reported as an integrity failure rather than resolved by trusting whichever
copy is newer.

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

**Full per-command authority regeneration rejected.** Rebuilding the complete workspace profile and environment for
every command would pay the expensive setup cost and cannot hold persistent shell state. Launch-time supervisor
sandboxing pays that cost once per grant revision. A small deterministic inner profile is nevertheless mandatory for
every shell: it removes `.cowshed/job/**` write authority and applies request-specific narrowing without regenerating or
widening the outer profile. A one-shot exec uses the same trusted-parent/narrow-child split.

**A cross-workspace shared supervisor rejected.** One supervisor serving many workspaces would straddle sandbox
boundaries and couple unrelated workspaces' lifecycles. One supervisor per workspace keeps the boundary and teardown
clean while its persistent socket still supports multiple concurrent clients.

**Text-log capture rejected.** Scraping merged terminal text is what forces agents into brittle `grep` parsing.
Structured exec records with separated raw streams and the grant/env context make a failed run reproducible instead of
merely readable (10_ci.md). Records are Arrow, not a line log, for the same reason all cowshed telemetry is
(13_telemetry.md): one substrate, queryable and joinable against the store-side spans — "diff the job histories of these
two forks" is a query, not a diff of text files.

## Shell text, redirection, and sealed output export

Shell text is always interpreted by the selected shell. Cowshed does not parse `>`, `2>`, append, pipelines, expansions,
or any other shell syntax to redirect, authorize, or optimize output. Bytes the shell redirects away from the
supervisor's stdout/stderr pipes have ordinary shell semantics and are not part of the captured pipe stream. In
particular, cowshed never aliases a protected job artifact into the writable workspace with a hardlink.

Callers that want a durable workspace-visible copy of a captured stream request `stdout_copy` and/or `stderr_copy`
explicitly in `ExecRequest` (07_api.md). After the command is terminal and the selected backing file has been drained,
fsynced, closed, and sealed, the supervisor materializes a temporary destination from that protected source, fsyncs it,
and atomically renames it according to the requested create/replace policy. The destination is an independent CoW clone
on APFS (`clonefile`) or on ZFS with block cloning enabled; the substrate falls back to an ordinary extent copy when
reflinking is unavailable. It is never a hardlink. Later writes, replacement, or deletion of the exported destination
cannot change the protected source.

Export paths are normalized workspace-relative regular-file paths opened by a no-follow component walk. Absolute paths,
traversal, symlink ancestors, directories, devices, sockets, and replacement races fail closed. Export status and the
`reflink`/`copy` disposition are structured job metadata. Export failure never changes the already-established process
exit or terminal quota state. This operation is deliberately not shell redirection: it publishes after completion and
does not promise that the destination exists or is observable while the command runs.
