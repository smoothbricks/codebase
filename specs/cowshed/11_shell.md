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
Each instance is launched once per effective filesystem grant revision under that revision's workspace sandbox profile.
That outer profile is the authority ceiling and gives the trusted supervisor protected-artifact write access. The
supervisor itself never evaluates `.envrc`, sources shell startup, or runs repository hooks. It installs the
deterministic inner child profile first, then starts a restricted environment-loader child and every anonymous shell,
named session, one-shot, and descendant beneath that restriction. The child profile denies writes beneath
`.cowshed/job/**` and may further narrow for ReadOnly; it never adds authority (04_sandbox.md).

- Holds K warm restricted shells (default 2, `.cowshed.toml` `[shell] pool`). Each receives the environment returned by
  a restricted fail-closed `direnv export` loader; no repository-controlled startup runs in the supervisor. Anonymous
  shells reset cwd/environment/shell variables and return to the pool after each exec.
- **Named sessions** (`--session <name>`, `WorkspaceHandle::shell(Some(name))`) are persistent shells outside the pool:
  state survives across calls until explicitly closed or the supervisor stops. This is how a coordinator gives one
  subagent a stable shell for a multi-step task.

Execution cwd has one representation across `ExecRequest`, supervisor/session state, `JobInfo`, JSON, and Arrow:
`Option<WorkspacePath>`. `None` denotes the workspace mount root; `Some(path)` denotes exactly one validated, normalized
workspace-relative path. Wire `cwd` is required and encodes `None` as JSON `null`; omission rejects, and neither an
empty path nor `.` is admitted as a root sentinel. Execution argv likewise has one byte-exact representation.
`ExecRequest.argv` and `JobInfo.argv` are `Vec<CommandArg>`, where each immutable element owns an `OsString`. On Unix
the shared serde codec emits exactly `{encoding:"utf8",data}` iff the bytes validate as UTF-8 and otherwise canonical
standard base64. It denies unknown fields/encodings and rejects malformed/non-canonical base64, base64 for valid UTF-8,
decoded NUL, arguments above 128 KiB, aggregate argv above 1 MiB, and empty argv/`argv[0]`. Validation precedes RPC,
job/artifact effects, process allocation, and spawn. The supervisor consumes arguments into `OsString` for `plan_exec`;
no `String`, lossy rendering, or alternate supervisor wire shape exists.

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
- **stdout/stderr** — raw bytes, backpressured per client and always separate. Capture begins in a bounded in-memory
  buffer while SHA-256 and the combined quota advance over the exact admitted bytes. A stream promotes lazily to a
  protected file when it exceeds the inline bound or when backgrounding, checkpointing, or replay requires
  filesystem-resident bytes; promotion writes the complete buffered prefix before appending. A slow or disconnected
  client never blocks capture. Reconnect resumes from the representation-transparent stream offset whether the protected
  artifact is terminal inline Arrow Binary or a file.
- **events** — asynchronous JSON notifications: `job-started`, `job-backgrounded`, `job-exited`, `session-closed`. Every
  job event carries the durable `(repoId, workspaceIncarnation, jobId)` key and standard lmao
  `traceId`/`threadId`/`spanId` plus nullable `parentThreadId`/`parentSpanId`. `job-backgrounded` forces any memory-only
  prefix to a protected file before acknowledging detachment. Terminal events carry separate `stdout` and `stderr`
  `StreamInfo` values with storage, byte count, SHA-256, and bounded summary. A client that only wants completion can
  ignore the byte channels and watch events.

The protocol is transport for both the CLI (which renders it to the stdout/stderr contract, 06_cli.md) and the MCP
server (which renders it to tool results, 12_mcp.md). JSON is bounded control/result transport: it may carry a tagged,
bounded inline artifact, but never an unbounded stdout/stderr stream. Controller commitments never carry output payload.

## Job control

Every exec submission is a job. At admission the supervisor allocates a positive `u64`-backed `jobId` in `1..=2^53-1`,
monotonically increasing within that workspace and never reused; exhaustion is typed `Conflict`. Decimal rendering is
canonical, with no prefix or zero padding. The protected record stream is `.cowshed/job/records.arrow`; optional spill
files, created only on promotion, are:

```
.cowshed/job/<jobId>/out
.cowshed/job/<jobId>/err
```

Stdout and stderr are separate opaque byte streams admitted under one combined quota: no UTF-8 assumption, line
rewriting, redaction, merging, or summary substitution. The supervisor begins each stream in bounded memory and creates
neither per-job stream path eagerly. When promotion is required it exclusively creates the job directory and selected
file without following links, writes the buffered prefix, then appends future admitted bytes. Executed shells receive a
child profile that denies every write mutation beneath `.cowshed/job/**`; no writable protected-artifact descriptor is
inheritable and no protected inode may be hardlinked into a workspace-writable path. Completion drains, hashes, fsyncs,
closes, and seals file artifacts, or writes bounded inline bytes into the terminal Job Arrow batch, before publishing
`ControllerCommitment::Terminal(TerminalCommitment)`.

Allocation is exclusive and crash-safe without a counter file or eager job directory. One supervisor is the sole
allocator for an attached workspace. Admission appends a complete `ProtectedRecord::Job(JobArtifactRecord)` batch and
publishes `ControllerCommitment::Admission(AdmissionCommitment)` before process creation, so spawn failure retains
durable identity. At startup the supervisor reconciles the maximum canonical `job_id` across valid complete in-volume
allocation batches, controller commitments for the active lineage, and canonical spill directories, then chooses
`max + 1` (or `1` when none exist). Any disagreement or duplicate durable key is `Integrity`, not a reason to reuse a
number. Open/recovery requires every record's `repo_id` to equal the workspace's bound repository. A record incarnation
may differ from the current marker only when controller fork/restore/checkpoint lineage and commitments admit it as
inherited history; an unknown historical incarnation, or any new allocation under a non-current incarnation, is
`Integrity`. Thus copied histories are intentional but cannot smuggle a foreign repository/timeline. Supervisor
replacement and attach otherwise discard only an incomplete trailing Arrow batch and never rewrite a complete batch or
sealed artifact. Fork/checkpoint/restore copies start above every inherited allocation; no separate high-water file
exists.

Each admission and terminal job batch also records its immutable argv as a required Arrow `List<Binary>`. Recovery
requires the canonical schema, non-null Binary elements, a non-empty first element, no NUL, and the same per-element and
aggregate byte bounds before reconstructing `CommandArg`. This preserves non-UTF-8 Unix argv across crash recovery and
rejects malformed complete batches as `Integrity`; protected storage never downgrades argv to Arrow Utf8.

A controller-minted immutable `workspaceIncarnation` disambiguates histories copied by fork/checkpoint/restore. Each
create, fork destination, and restore result receives a fresh incarnation; inherited records retain the incarnation that
produced them, and the new allocator starts above the inherited maximum. Thus the durable job key is
`(repoId, workspaceIncarnation, jobId)`: `jobId` is the familiar workspace-local handle, while the full tuple remains
unique across checkpoint copies and recycled workspace names.

- **Soft timeout → auto-background.** A foreground command still running at its soft timeout (`--timeout`, default
  `[shell] soft_timeout` = 120 s) is detached. Before acknowledging detachment, the supervisor promotes each
  memory-resident stream prefix to its protected file; the files then keep growing and `job-backgrounded` fires. The
  client already has the job id and can poll or reattach. A later checkpoint uses the barrier/manifest protocol below.
- **`--background`** forces the same detachment and promotion immediately.
- **Hard timeout** (`[shell] hard_timeout`, unset by default, set by CI — 10_ci.md) → SIGTERM, then SIGKILL after a
  grace, drain both pipes to EOF, then mark the job `killed:timeout`.
- **Combined output quota.** Each job has one configurable quota across stdout and stderr, default **1 GiB**. Accounting
  includes protected bytes plus bytes read from either child pipe but still buffered/in flight. The first read whose
  inclusion would cross the quota atomically trips the limit: the supervisor admits no payload beyond the exact
  boundary, sends SIGTERM to the complete process group, waits the configured grace, SIGKILLs stragglers, and drains
  both pipes to EOF without retaining post-boundary payload. After drain and artifact sealing, the authoritative
  terminal state is `output-limit`, with configured limit and observed crossing recorded. Summary truncation remains an
  independent bounded projection.
- **Re-attach**: `cowshed job attach` / `Job::attach` re-opens stdio to a running job from the client's last
  acknowledged stream offsets, representation-transparently across memory/file promotion.
- **Logs**: `cowshed job logs` / `Job::logs` resolves `StreamInfo.storage.artifact`; callers never need to distinguish
  inline Arrow Binary from a protected file and no path is promised.

### Exec records, stream storage, and tiered authority

Every job emits protected lifecycle records to `.cowshed/job/records.arrow`. The stream travels with checkpoints and
complete batches written by the trusted supervisor are authoritative for captured content within their recorded origin
`workspaceIncarnation` and checkpoint-manifest boundary. Executed jobs may read but cannot write, truncate, rename,
unlink, link, or replace protected records or artifacts. Terminal completion is a record-batch boundary. After an
unclean supervisor exit, recovery may discard only an incomplete trailing batch or uncommitted bytes beyond a checkpoint
manifest; malformed or mismatched complete data is `Integrity`, never child-authored input to reinterpret.

The shared DTO is exact:

```rust
enum ProtectedOutput {
    Inline { data: BinaryData },
    File { path: WorkspacePath },
}
enum OutputStorage {
    Captured { artifact: ProtectedOutput },
    Redirect { source: WorkspacePath, artifact: ProtectedOutput },
}
struct StreamInfo {
    storage: OutputStorage,
    bytes: u64,
    sha256: Sha256Digest,
    summary: OutputSummary,
}
```

`BinaryData` is a bounded byte newtype. Arrow stores Binary; JSON/NAPI uses the exact wire union
`{encoding:"utf8",data:"…"} | {encoding:"base64",data:"…"}`, choosing `utf8` iff the bytes validate and bounding both
branches by decoded length. Ordinary `JobInfo` may contain this bounded value; controller commitments never do.
`File.path` is a protected workspace-relative path and exists only after lazy promotion. `Captured.artifact` is the
canonical full-fidelity content. `Redirect.source` names an AST-proven real-shell `>`/`2>` workspace destination written
during execution; it is mutable caller-visible state and never authority. Its `artifact` is an independent protected
post-terminal snapshot of the exact admitted bytes, inline when small or clone/reflink/copied to a sealed protected file
when large.

For queued/running jobs, `StreamInfo` is a bounded current view and its artifact is not sealed content authority yet.
Inline data may be the supervisor's current bounded buffer; background acknowledgement and checkpointing force it to a
protected file, and terminal publication freezes the final union/count/hash. Only complete protected batches and sealed
files carry the scoped content authority described here. Representation-transparent reads always resolve `artifact`,
never `Redirect.source`.

Redirect classification is permitted only for a real shell AST whose simple literal redirection semantics are proven and
only when the supervisor controls the actual writable descriptor and applies the same exact combined-quota boundary as
ordinary captured pipes. Polling, tailing, or reopening the destination path after execution is forbidden: it cannot
prove byte admission or defeat replacement/truncation races. If interposition is unavailable or any eligibility check
fails, cowshed runs ordinary shell semantics and `StreamInfo` describes only bytes that actually reached the captured
pipe; it makes no claim over bytes redirected away by the shell.

After terminal sealing, the publication API remains exactly `ExecRequest.stdout_copy` /
`stderr_copy: Option<OutputPublication>`, `OutputPublication { path, policy }`, and
`PublicationPolicy::{CreateNew, Replace}` (camelCase `stdoutCopy`/`stderrCopy`, `createNew`/`replace` in JSON/NAPI). The
supervisor clonefiles/reflinks or extent-copies into a temporary destination, fsyncs, and atomically renames.
Publication never changes `StreamInfo.storage`; reads and authority never resolve through it. Redirect snapshots and
publication forbid hardlinks.

Cross-incarnation completeness uses this exact tagged/versioned union:

```rust
enum ControllerCommitment {
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Checkpoint(CheckpointCommitment),
    Fork(ForkCommitment),
    Restore(RestoreCommitment),
}
```

Every event struct has `version, order, repo_id`. Admission adds `workspace_incarnation, job_id, grant_revision`;
Terminal adds
`workspace_incarnation, job_id, state, grant_revision, stdout_bytes, stdout_sha256, stderr_bytes, stderr_sha256, batch_sha256`;
Checkpoint adds `origin_incarnation, checkpoint_id, barrier_id, manifest_batch_sha256`; Fork adds
`source_incarnation, destination_incarnation`; Restore adds
`source_checkpoint, source_incarnation, destination_incarnation`.

At launch the controller supplies a dedicated write-only IPC capability that is non-inheritable before any workspace
process starts. `order` is positive/globally monotonic. `CommitmentPriorContext` carries the prior order plus known
admissions, terminals, checkpoints, and incarnation lineage into `validate_commitments`; validation requires admission
before the sole terminal row and existing acyclic fork/restore/checkpoint lineage. Controller writers publish exclusive
immutable segments. No variant contains inline bytes, protected paths, redirect sources, summaries, or raw payload.

Protected in-volume records and artifacts are authoritative captured-content evidence for their origin
incarnation/checkpoint boundary. Controller commitments are authoritative for existence, lifecycle/status, ordering,
fork/restore/checkpoint lineage, and the expected hashes/counts that detect omission, mutation, or rollback. Neither
tier substitutes for the other. A missing committed artifact, unknown commitment, count/hash/terminal-batch/manifest
digest mismatch, or lineage contradiction returns typed `Integrity`, stops status/restore/export from presenting the
record as valid, and preserves both sides for diagnosis; cowshed never overwrites one side or chooses whichever appears
newer. Grant authority remains in controller-owned grant files and gateway decisions remain in controller-owned audit
segments.

Each `StreamInfo.summary` is a deterministic, bounded, versioned **diagnostic summary**, inspired by RTK/ContextCrawler:
retain a small ordered context window around compiler/test/error signatures plus deterministic head/tail context,
normalize only summary text, redact configured secret/token/path patterns before emission, and apply fixed byte, line,
and match budgets. The summary object is `{version, text, truncated}`. Summaries are convenience evidence only: they
never establish denial, exit/signal, output-limit, policy/audit, or build/test success. Full-fidelity admitted bytes are
resolved through the protected artifact; quota crossing remains explicit terminal state.

The protected record stream uses exactly:

```rust
enum ProtectedRecord {
    Job(JobArtifactRecord),
    CheckpointManifest(CheckpointManifestRecord),
}
```

`barrier_id` is positive/monotonic within the origin. `records_sha256` hashes the complete protected-record prefix
before the manifest batch. Each `VisibleJobCommitment` is `{workspace_incarnation,job_id,state,stdout,stderr}`; a stream
is `{storage_kind,bytes,sha256,protected_path}` with storage kind exactly
`captured-inline|captured-file|redirect-inline|redirect-file` and path present iff file. The barrier pauses mutation,
promotes running prefixes, fsyncs files, writes the complete manifest batch, and clones while held. Recovery frames
retain `batch_sha256`; only an incomplete trailing frame may be discarded/reported.

The workspace-file stdin source opens after allocation with a read-only no-follow component walk rooted at the workspace
mount and must resolve to a regular file beneath it. EOF closes child stdin exactly once. Cancellation records
incomplete delivery without implicitly killing the job. `JobInfo.stdin` and job/trace metadata carry stdin kind, bytes
delivered, completion, and normalized relative source where applicable; inline stdin contents never enter telemetry.

Inspect with `cowshed ps <ws> --json`, `cowshed job list <ws> --json`, or `lmao-inspect` (13_telemetry.md). This is the
only capture implementation. CLI, MCP, NAPI, and CI consume the same protected evidence and controller commitments; no
client recaptures output or derives authority from summary text.

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

## Shell text, redirection, and sealed output publication

Shell text is interpreted by the selected shell. Cowshed does not use regexes or output sniffing to reinterpret `>`,
`2>`, append, pipelines, expansions, or other syntax. The optional real-AST `Redirect` classification above is an
evidence-preserving capture path, not the former hardlink optimization: it is available only with a
supervisor-controlled writable descriptor and exact quota accounting, and snapshots independently after terminal state.
Otherwise ordinary shell semantics apply and bytes redirected away from captured pipes are not claimed by the job
handle.

Callers that want a workspace-visible copy independent of shell syntax request `stdout_copy` and/or `stderr_copy` in
`ExecRequest` (07_api.md). Only after terminal state and canonical artifact sealing does the supervisor materialize a
temporary destination from that artifact, fsync it, and atomically rename it according to create/replace policy. APFS
uses `clonefile`, ZFS uses block cloning when available, and other substrates use an extent copy. Hardlinks are
forbidden in every case.

Redirect sources and publication paths are normalized workspace-relative regular-file paths opened with no-follow
component walks. Absolute paths, traversal, symlink ancestors, directories, devices, sockets, and replacement races fail
closed. Publication disposition (`clone`, `reflink`, or `copy`) and failure are structured metadata. Publication failure
does not change the established process/quota state or `StreamInfo`; it is a separate typed operational outcome. The
destination is never promised while the command runs, never used for job reads, and never authoritative.
