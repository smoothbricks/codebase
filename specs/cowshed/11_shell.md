# Warm Shell Layer

`cowshed-shell` is the process-management layer between `cowshed-core` (which owns _what may run where_ — substrate,
mounts, sandbox profiles, grants) and every client that runs commands (CLI, MCP, CI, NAPI). It provides one long-lived
supervisor per workspace holding warm exec hosts with the workspace shell already activated, a framed stdio protocol
over a Unix socket, job control, and the single exec-record capture that all clients consume.

## Shell activation and process reuse

A fresh workspace is a CoW clone of main, including its `.direnv`/`.devenv` caches. These are inputs to canonical
activation, not proof that an exported environment is current. Activation dominates command latency: **a warm
`direnv exec` on a devenv repo costs ~1.3–5 s per invocation**, dwarfing `sandbox-exec` startup and process creation.
Cowshed therefore activates a workspace shell once, reuses the activated process for every command until one of the
inputs direnv recorded for that activation changes, and never replays exported variables in place of activation.

**Exec hosts.** A command whose cwd lies under a workspace-contained `.envrc` runs in a warm exec host: a small cowshed
process started under the executed-child profile, in its own process group, with the sandbox environment
(04_sandbox.md). The host approves the `.envrc` in the workspace's private trust store and evaluates it once with
`direnv export json`, applying the exported diff to its own environment; the repository's entry hooks run exactly as
they do under `direnv exec`. For each command it receives the job's stdin, stdout and stderr descriptors over its
control socket (`SCM_RIGHTS`), forks the argv into a new process group with the requested cwd and the caller's
environment laid over the activated one, and reports the raw `waitpid` status. Nothing a command does — `cd`, `export`,
a changed umask — reaches the host or a later command: every command is a fresh process from the same activated
environment. The host program is staged under `.cowshed/shell-host/`, which every profile denies writes to, so no job
can replace the parent of later commands. A workspace with no shell configuration gets hosts that skip activation and
hold the sandbox environment itself; its argv jobs keep one-shot spawns, its script jobs run in those hosts. A
configured devenv-only project, which has no direnv watch list to judge freshness by, enters `devenv shell` inside each
job's own child and cannot run script jobs.

**Script jobs.** Besides a byte-exact argv, a job can be a script: bash-compatible shell text as a template of literal
`parts` and `values` (`ExecCommand::Script`, 07_api.md). At admission the supervisor binds each value to a shell
variable of its own and puts a reference to it where the value stood, quoted for that position — `"${v}"` in an unquoted
word, `"${v[@]}"` when a list is a whole bare word (one word per element), `${v}` inside `"…"`, `'"${v}"'` inside `'…'`;
`$(…)`, backquotes and process substitution start a fresh unquoted context. A value therefore never reaches the shell as
text: no quoting in a value can end its word, start a command or expand anything. A value inside a comment, inside
`$'…'` or directly after an odd run of backslashes is refused before admission. Rendering does not detect the one
context where the shell reads a variable's content as more than data: arithmetic (`$((…))`, `((…))`, `let`, an array
subscript, an integer variable, an arithmetic `[[ … -eq … ]]` operand), where bash and brush evaluate the content as an
expression, so a value placed there can assign shell variables. Clients refuse those placements before submitting. The
host parses the text with an upstream `brush` parser, so a script that does not parse fails before anything runs: the
job ends `failed` with `failure: scriptSyntax`, status 2 as bash gives, and the parser's diagnostic on its stderr. A
script that parses runs in a child the host forks without exec, the way bash runs a subshell: the child leads the job's
process group and is the only process that creates it (two creations of one group race, and macOS refuses the loser with
`EPERM`), and it reports once the group exists, so the host gives the supervisor the pid only then and the supervisor
never signals a group not yet made. A child that cannot lead its group says why on the job's stderr and exits 126. The
child resets every signal the host handles to its default as an exec would, holds the job's descriptors as 0, 1 and 2,
builds a brush interpreter from the already-activated environment (no activation, no rc files) with job control off,
runs the program and exits with its status. Every external command and descendant the script starts is in the job's
group, so a kill reaches all of them and never the host; a killed script dies by the signal and reports it exactly;
`cd`, `umask`, `ulimit`, `trap` and `exec` end with the child. The interpreter lives only in the host binary (the
`cowshed-shell` crate), not in the supervisor library or its Node addon.

**Process identity and signal ownership.** The process that creates a command's group observes its leader's immutable
birth identity before anything can reap it, including a leader that exits immediately. One-shot commands and activation
hosts are direct children held by a parent-owned reap/signal fence. A job's leader is observed exiting **without
reaping** (`WNOWAIT`) and remains held until the complete job concludes: descendants can outlive the leader and keep its
output pipes open. A later `kill`, quota stop, or retirement therefore still reaches their exact group. Only after both
pipes end and the terminal outcome is established does the parent release and reap the leader. Signals and reaping
serialize on the fence, without holding a blocking lock across an await; a released/reaped id grants no future signal
rights. Birth-observation failure never becomes a ledger claim, but the actual parent's unreaped child remains its own.

A warm command's signals are framed `REQUEST_SIGNAL`s to its actual parent host. The host reports the observed birth
with `REPLY_STARTED`, and its exact typed exit with `REPLY_EXITED`, while keeping the leader unreaped. It continues
serving signals until the supervisor concludes the job and sends `REQUEST_RELEASE`; `REPLY_RELEASED` acknowledges actual
reaping, and only then may that host serve another command. Failed signal delivery is `REPLY_SIGNAL_FAILED`, not silent
success. Signal/release while no command is held, malformed frames, and unknown tags fail closed.

Parent-side output/diagnostic writers are dropped as soon as the command starts, not kept until release: otherwise EOF
would wait for release while release waited for EOF. Every post-fork failure and controller EOF ends the host's own held
command group with TERM, the existing grace, KILL, and reap. The supervisor never force-kills the host before that
command retirement can run, and no stale start reply can authorize signalling another process.

**Freshness is direnv's own.** direnv records every input an evaluation depended on — the `.envrc`, its approval files,
each `source_up`, `use devenv` and `watch_file` target — as `DIRENV_WATCHES` (base64url of zlib-deflated JSON
`[{path, modtime, exists}]`) and reloads when any of them changes. The supervisor decodes that list from each
activation, subscribes the listed paths with the kernel (kqueue `EVFILT_VNODE` on macOS, inotify on Linux; the nearest
existing ancestor for an input recorded as absent), and keeps the exact identity — device, inode, size, mtime and ctime
at the filesystem's timestamp resolution — each had when the activation finished. direnv's whole-second `modtime` is
never compared. Where timestamps are coarse (a scheduler tick on Linux without multigrain timestamps, a second on HFS+)
two same-size writes inside one tick share an identity, and the kernel subscription is what reports them. A kernel event
only sets the pool's dirty flag, however many inputs changed. The next command acts on it, never the event: if the flag
is set, or an in-process stat of the listed paths finds an identity changed (the backstop for coalesced events), every
idle host and the spare are retired and a fresh host activates for that command; a host still executing finishes its
command and is dropped instead of returned. An activation whose own inputs moved while it ran — an input the previous
generation listed changed identity across the evaluation, or a newly listed input's ctime is not earlier than the
evaluation's start — serves the command that paid for it and is never reused. The start is read off the workspace
filesystem's own clock, never the process clock, which runs up to a tick ahead of the stamps a coarse clock gives: the
supervisor stamps a file of its own in the protected host directory, and stamps again until the clock has moved past the
first, so every change before the start, the approval's included, stamps earlier and every change after it stamps no
earlier — as long as the clock runs forward. A wall clock stepped back while the evaluation runs (a VM's time sync, an
NTP step) stamps a change made during it earlier than the start, so the supervisor reads the clock again once the
evaluation's inputs are snapshotted: when that reading is earlier than the start, no newly listed input counts as older
than the evaluation, and the activation is never reused. That catches a step back larger than the evaluation took, and
only that: a smaller step leaves the end reading after the start, which is what a clock that ran forward, was slewed or
stamps coarsely also shows, and the filesystem's stamps are the only clock there is to read (a monotonic clock beside
them drifts from the wall clock by slewing alone). A newly listed input written within such a step after the start can
still count as older, and the shell serves until that input changes again. An input on another filesystem, which may
stamp in whole seconds, must predate the start's second. A change to a path not on the list costs nothing. The
repository decides what else counts as shell input with `watch_file`: lockfiles and devenv inputs whose change must
rerun shell entry.

**One spare and a SIEVE cache.** Hosts are pooled per shell identity: effective sandbox mode, `.envrc` directory, grant
revision, the exact sandbox environment, and the host program. A read-only command never runs in a read-write host, and
no host activated under one grant revision serves another. Each pool keeps one activated spare ahead of demand and a
bounded cache (four hosts) of hosts that are executing or were just returned; a full cache evicts by SIEVE. A command
takes the most recently returned idle host, else the spare — whose replacement starts activating in the background —
else activates a host of its own. Commands never queue for a host. One actor owns each pool's state; nothing locks. A
supervisor that lives only as long as a short-lived controller process keeps no spare, which would end unused with it.

**Activation belongs to the job that waited on it.** A command that activates a host receives the activation's output on
its own stdout and stderr, and until its command starts that host's process group is the job's: a kill reaches the
activation, and a failed activation fails the job with the activation's status and discards the host. A warm command's
streams carry only its own output. A spare's activation output is discarded; its failure only means there is no spare.
An activation whose supervisor goes away — dropped the host, or its process exited — ends with its whole process group,
since nothing can use its result. A host that cannot serve a request — it cannot run `direnv`, say — answers with the
reason and exits; any host that breaks its protocol fails the job waiting on it as a launch that failed, with the reason
on the job's stderr, since its command never ran. The one exception is a host that died by a signal while it activated
for the job: a kill of the job reaches its activation, and a crash is the job's to see, so that death is the job's
status (the reason still goes to its stderr).

The layer's other roles:

- **Sessions** — a named session carries a cwd and an environment overlay across calls; each of its commands runs in a
  warm host like any other and sees the session's overlay, never another command's exports.
- **The framed stdio protocol** — multiplexed, backpressured, language-neutral I/O for concurrent CLI, NAPI, MCP, and CI
  clients, instead of each reinventing pipe plumbing.
- **Job control** — waiting, caller timeouts, backgrounding, re-attach, structured capture.

The CoW clone and warm build caches are distinct from shell reuse. Neither justifies bypassing repository activation or
replaying a supervisor-owned environment in place of its shell entry hooks.

## Supervisor

One supervisor per attached workspace, the workspace's sole job allocator, running as a process of its own:
`cowshed __workspace-supervisor <project-root> <workspace>`, which serves it on the workspace's socket (Protocol,
below). The gateway daemon owns these processes through its supervisor manager, listening at `<store>/run/manager.sock`.
A controller asks the manager to ensure the workspace's supervisor under the incarnation and grant revision it needs,
and the manager answers with the socket once a supervisor serves it there. It starts one with the daemon's own binary,
in a session of its own, when nothing answers; ensures of one workspace run one at a time, so two controllers never
start two allocators. A supervisor therefore outlives the command that first needed it, and the daemon's restarts: the
manager watches every supervisor it started and, when it starts, finds the ones still serving. A supervisor answering
under another incarnation is refused with `Conflict` naming its pid. A supervisor that ends before it serves says why on
a report pipe the manager hands it (named by `COWSHED_SUPERVISOR_REPORT_FD`, close-on-exec in the supervisor so no job
inherits it), and the ensure — so the command that needed the workspace — fails with that error and its code, not with a
pointer to the daemon's log. The supervisor itself never evaluates `.envrc`, sources shell startup, or runs repository
hooks; it reads only the watch list an activation reports. It compiles the deterministic inner child profile first, then
starts every exec host, one-shot command, and descendant beneath that restriction; each command runs in its job's own
process group. The child profile denies writes beneath `.cowshed/job/**` and may further narrow for ReadOnly; it never
adds authority (04_sandbox.md).

- Holds the warm exec hosts above. Host startup and activation run inside the child sandbox; no repository-controlled
  startup runs in the supervisor.
- **Named sessions** (`--session <name>`, `WorkspaceHandle::shell(Some(name))`) keep a cwd, an environment overlay, and
  the set of their background jobs until explicitly closed or the supervisor stops. They hold no process of their own.
  This is how a coordinator gives one subagent a stable working directory and environment for a multi-step task.
- **The warm lane** runs main's warm step (02_workspaces.md "Warm main"). `land` hands main's supervisor the declared
  argv and the landed range; the supervisor answers at once — `started` with the job it admitted, or `queued` behind the
  warm job running — and never makes the caller wait for the build. At most one warm job runs; a land that arrives while
  it runs becomes the one run waiting, and a later one replaces it, keeping the waiting base and taking the new head and
  argv. When the running warm job ends, however it ends, the supervisor starts the waiting run as an ordinary background
  job carrying `COWSHED_LAND_BASE`/`COWSHED_LAND_HEAD` and its range in `JobInfo.warm`. The waiting run is supervisor
  memory: a supervisor that retires first drops it and says so on its stderr.

Execution cwd has one representation across `ExecRequest`, supervisor/session state, `JobInfo`, JSON, and Arrow:
`Option<WorkspacePath>`. `None` denotes the workspace mount root; `Some(path)` denotes exactly one validated, normalized
workspace-relative path. Wire `cwd` is required and encodes `None` as JSON `null`; omission rejects, and neither an
empty path nor `.` is admitted as a root sentinel. Execution argv likewise has one byte-exact representation.
`ExecRequest.command` and `JobInfo`'s command are one `ExecCommand`: an argv or a script (above), flattened on the wire
as exactly one of `argv` and `script`. An argv is `Vec<CommandArg>`, where each immutable element owns an `OsString`. On
Unix the shared serde codec emits exactly `{encoding:"utf8",data}` iff the bytes validate as UTF-8 and otherwise
canonical standard base64. It denies unknown fields/encodings and rejects malformed/non-canonical base64, base64 for
valid UTF-8, decoded NUL, arguments above 128 KiB, aggregate argv above 1 MiB, and empty argv/`argv[0]`. A script's
parts and values are UTF-8 strings without NUL, bounded together at the same 1 MiB, with exactly one more part than
values. Validation precedes RPC, job/artifact effects, process allocation, and spawn. The supervisor consumes arguments
into `OsString` for `plan_exec`; no `String`, lossy rendering, or alternate supervisor wire shape exists.

- It retires itself after 30 minutes with no named session open and no job running, freeing its warm hosts; the next
  command starts another. `cowshed detach`/`cowshed rm` retire it before the substrate changes (teardown below).

## Protocol

Each supervisor listens on a Unix socket at `<store>/run/<digest>.sock`, where `digest` is the first 32 hex digits of
the SHA-256 of `repo_id`, a NUL, and the workspace name. Owner, repository and workspace names together can exceed the
104 bytes a socket path may hold on macOS; the digest keeps every path at 64 and no two workspaces share one. The
serving process creates `<store>/run` mode `0700`, so no other user reaches a socket in it, binds the socket there and
sets it `0600`, and verifies each connecting peer's uid before reading a byte. A persistent no-follow file lease at
`<digest>.sock.lock` serializes binders and stale-socket replacement. The bound listener and lease are one owned value,
held throughout serving or a workspace mutation. A supervisor of another build must finish the other-build drain before
a new build binds; ensure refuses the old build while it still serves, and the lifecycle stop waits for its exit. Stream
refusal is not absence: a live Darwin listener can refuse when its backlog is full. Recovery probes the exact filesystem
socket with a datagram connection; wrong-type or connected means a socket remains attached to that vnode, including one
inherited after its creator exited. Only a proven unattached socket is removed under the lease. New listeners bind
privately in mode `0600` and publish with one exclusive rename. Cleanup removes only the bound listener's own inode
while holding the lease. Retirement releases the listener and lease **before** acknowledging success.

Every call is one connection: the client writes one JSON request frame, then the raw bytes the call carries (inline
stdin, a stdin chunk) as one more frame; the supervisor answers with one JSON response frame, then the raw bytes the
answer carries (a log chunk), and both sides close. A frame is a big-endian `u32` length and that many bytes. A long
call — `wait`, a following log read — therefore holds only its own connection: no call queues behind another, nothing is
multiplexed, and a client that disconnects abandons only its own call, never a job.

- **hello** — the supervisor answers with its build, the authority it serves (repository, workspace, incarnation, grant
  and lifecycle revisions) and its pid. A client reads the build first and refuses any other by name (`Conflict`), so
  two cowshed builds never exchange a call either cannot decode. A build is the Mach-O `LC_UUID` the linker derives from
  the binary's contents (the SHA-256 of the executable on Linux): no number anybody has to remember to bump, so a change
  nobody announced still counts. The manager refuses an ensure from another build the same way, and its refusal carries
  both builds as data — `otherBuild: { daemon, caller }` on the `Conflict` — so a client tells a controller the daemon
  will no longer serve from every other conflict without reading the sentence. The controller client notes the first
  such answer on its connection (`Coordinator::other_build`, 07_api.md); a supervisor of the controller's own build that
  still drains its jobs keeps answering the calls that reach it. A program that links cowshed as a library is another
  build whatever revision it links, so it opens no project in-process: it runs its controller as `cowshed controller`
  (06_cli.md), a process of the host's own build, and speaks the controller protocol to it over the socket it hands that
  process; after an install has started the daemon of a new build, it starts that verb again for its new work.
- **advance** — the supervisor re-reads its workspace's grants and serves under their revision from then on
  (Grant-change propagation, below); answered with the authority it serves afterwards.
- **drain** — the supervisor admits nothing more, lets its running jobs finish, then retires; answered at once with its
  pid. This one request keeps its shape across builds (Supervisor recovery, below).
- **commitments** — the controller commitments the supervisor recorded after a cursor, waiting up to 30 seconds for one
  when there is none; **acknowledgeCommitments** forgets every one through a cursor. Every commitment goes to the
  supervisor process's own sink, the host's default; a controller whose sink is its own reads them here, records them
  into it, and acknowledges them. The supervisor keeps up to 65,536 unacknowledged commitments and then drops the
  oldest, which the next read reports.
- **calls** — `openSession`, `sessionSnapshot`, `closeSession`, `exec`, `warm`, `stdinWrite`, `stdinClose`,
  `streamChunk`, `streamEnd`, `info`, `sealed`, `list`, `kill`, `wait`, `logRead`, `checkpoint`, `quiesce`, `retire`:
  one per supervisor operation, each naming the authority the caller holds. The supervisor fences every call by it
  exactly as it fences an in-process one, so a caller holding a stale incarnation or grant revision is refused, not
  served under the wrong profile. An accepted `exec` answers with the numeric `jobId`, allocated before process
  creation; a spawn failure is therefore a terminal job, not a response with no identity. `warm` answers with the
  `WarmAdmission` above. `info`, `list`, `kill` and `wait` answer the supervisor's own jobs; `sealed` answers a job's
  terminal record from the workspace's records — state, exit, failure, duration, output limit and both streams — for any
  job of the incarnation that has one, including a job an earlier supervisor ran and sealed, and `logRead` reads such a
  job's sealed streams from any offset as it reads its own terminal jobs'.
- **stdin** — empty, inline bytes (the request's raw frame), a workspace-relative regular file the supervisor opens
  inside the sandbox boundary, or a stream: the client forwards its source as `streamChunk` calls in order, each
  answered only once the job's bounded queue took it, and ends it with `streamEnd`, naming the source's error if it
  failed. Bytes are never interpolated into shell text.
- **stdout/stderr** — `logRead` returns the bytes of one stream from an offset, following (waiting for the next bytes)
  when asked. Capture begins in a bounded in-memory buffer while SHA-256 and the combined quota advance over the exact
  admitted bytes; a stream promotes lazily to a protected file when it exceeds the inline bound or when backgrounding,
  checkpointing, or replay requires filesystem-resident bytes. A slow or absent reader never blocks capture, and a
  reader resumes from the representation-transparent offset whether the protected artifact is terminal inline Arrow
  Binary or a file.

The protocol is transport for every client — the controller behind the CLI (06_cli.md), NAPI, and the MCP server
(12_mcp.md). JSON is bounded control/result transport: it may carry a tagged, bounded inline artifact, but never an
unbounded stdout/stderr stream. Controller commitments never carry output payload.

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

Allocation is exclusive and crash-safe without an eager job directory. One supervisor is the sole allocator for an
attached workspace. Admission appends a complete `ProtectedRecord::Job(JobArtifactRecord)` batch and publishes
`ControllerCommitment::Admission(AdmissionCommitment)` before process creation, so spawn failure retains durable
identity. A record's sequence comes from the `records.sequence` counter, published by atomic rename before the record is
appended under the same lock, so a crash may leave a gap but never hands a sequence out twice. At startup the supervisor
reconciles the maximum canonical `job_id` across valid complete in-volume allocation batches, controller commitments for
the active lineage, and canonical spill directories, then chooses `max + 1` (or `1` when none exist). Any disagreement
or duplicate durable key is `Integrity`, not a reason to reuse a number. Open/recovery requires every record's `repo_id`
to equal the workspace's bound repository. A record incarnation may differ from the current marker only when controller
fork/restore/checkpoint lineage and commitments admit it as inherited history; an unknown historical incarnation, or any
new allocation under a non-current incarnation, is `Integrity`. Thus copied histories are intentional but cannot smuggle
a foreign repository/timeline. Supervisor replacement and attach otherwise discard only an incomplete trailing Arrow
batch and never rewrite a complete batch or sealed artifact. Fork/checkpoint/restore copies start above every inherited
allocation; no separate high-water file exists.

**Durability of a job's own records.** A job's records — its admission and terminal batches, its spill files (made
durable when it backgrounds, sealed when it ends), the output copies it publishes, and its admission and terminal
commitments — are written for every exec and survive the death of the process that wrote them, not power loss: each
takes exactly one `fsync(2)` of the file it appends or creates, and no directory sync, before the job is admitted,
backgrounded, answered as ended, or its copy reported published; the sequence counter's rename is not synced at all.
`F_FULLFSYNC` is what survives power loss on macOS, and on a disk image it flushes every dirty block of the whole image
— tens of milliseconds to seconds right after a build — so it stays on lifecycle and authority state only: workspace
creation and removal, landing, checkpoint manifests, grants and policy revisions, and every lifecycle commitment. A
checkpoint's manifest batch is appended after it makes every running prefix durable, so its one `F_FULLFSYNC` carries
those prefixes past power loss with it. Power loss ends every job anyway, and may take any suffix of the records written
since: a torn trailing batch is discarded as above; a counter rolled back behind the log is advanced to the log's
highest sequence at the next open, before anything allocates; and when the next supervisor takes the workspace's socket
— which no other supervisor then holds — it seals every job of the incarnation that has an admission and no terminal
record `failed` with `supervisorLost`, after ending the process groups its predecessor's ledger still names, and seals a
spill file the power took as a stream with no bytes. A job whose terminal record was lost is therefore reported lost,
never as having succeeded.

Each admission and terminal job batch also records its immutable command in a required Arrow `List<Binary>` `argv`
column. An argv job stores its arguments; a script job stores exactly two elements, `\0script` and the script's JSON — a
first element no argv can have. Recovery requires the canonical schema and non-null Binary elements; an argv needs a
non-empty first element, no NUL, and the same per-element and aggregate byte bounds before reconstructing `CommandArg`,
and a script needs its JSON to decode to a valid script. This preserves non-UTF-8 Unix argv across crash recovery and
rejects malformed complete batches as `Integrity`; protected storage never downgrades argv to Arrow Utf8.

Record layouts grow only by trailing columns, and each build reads its own layout and every earlier one. A complete,
intact batch that begins with every column of this build's layout and has more — a job record in it declares a version
above this build's — was written by a newer cowshed: recovery refuses it as `Conflict`, naming the record's layout and
the newest this build reads, and never truncates, rewrites or seals it. Only a batch in no layout at all, or holding
other than one row, is `Integrity`.

A controller-minted immutable `workspaceIncarnation` disambiguates histories copied by fork/checkpoint/restore. Each
create, fork destination, and restore result receives a fresh incarnation; inherited records retain the incarnation that
produced them, and the new allocator starts above the inherited maximum. Thus the durable job key is
`(repoId, workspaceIncarnation, jobId)`: `jobId` is the familiar workspace-local handle, while the full tuple remains
unique across checkpoint copies and recycled workspace names.

- **Foreground commands wait.** A foreground command's output streams to the caller while it runs, and the caller waits
  for its end however long it runs; there is no default timeout. A caller that sets one (`--timeout`) reads the job's
  status when it passes: a job still running is detached and the caller is told it still runs, never given a success
  that stands in for the command's; a job that ended meanwhile is not detached — its output is drained to EOF and its
  exit is the command's, because a finished job has nothing to reattach to. Before acknowledging detachment, the
  supervisor promotes each memory-resident stream prefix to its protected file; the files then keep growing and
  `job-backgrounded` fires. The client already has the job id and can poll or reattach through the API. A later
  checkpoint uses the barrier/manifest protocol below.
- **`--background`** detaches and promotes immediately, and the caller gets the job id.
- **Hard timeout** (`[shell] hard_timeout`, unset by default, set by CI — 10_ci.md) → SIGTERM, then SIGKILL after a
  grace, drain both pipes to EOF, then mark the job `killed:timeout`.
- **Combined output quota.** Each job has one configurable quota across stdout and stderr, default **1 GiB**. Accounting
  includes protected bytes plus bytes read from either child pipe but still buffered/in flight. The first read whose
  inclusion would cross the quota atomically trips the limit: the supervisor admits no payload beyond the exact
  boundary, sends SIGTERM to the complete process group, waits the configured grace, SIGKILLs stragglers, and drains
  both pipes to EOF without retaining post-boundary payload. After drain and artifact sealing, the authoritative
  terminal state is `output-limit`, with configured limit and observed crossing recorded. Summary truncation remains an
  independent bounded projection.
- **Re-attach**: `Job::attach` re-opens stdio to a running job from the client's last acknowledged stream offsets,
  representation-transparently across memory/file promotion.
- **Logs**: `Job::logs` resolves `StreamInfo.storage.artifact`; callers never need to distinguish inline Arrow Binary
  from a protected file and no path is promised.

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
process starts. `order` is the writing controller's own monotone sequence. The records are audit telemetry: each is
validated on its own row and written to the configured sink (sealed Arrow segments by default, `off`, or a host's own
sink), and nothing replays them for a decision. No variant contains inline bytes, protected paths, redirect sources,
summaries, or raw payload.

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

Inspect with `WorkspaceHandle::list_jobs`, `cowshed exec --json` (the final `JobInfo`), or `lmao-inspect`
(13_telemetry.md). This is the only capture implementation. CLI, MCP, NAPI, and CI consume the same protected evidence
and controller commitments; no client recaptures output or derives authority from summary text.

## Grant-change propagation

When a coordinator applies an effective filesystem `grant`/`revoke` (04_sandbox.md, 07_api.md), the revision a command
needs moves past the one its workspace's supervisor serves. The supervisor runs no workspace code, so it needs no
process per revision: each exec host and one-shot command starts under the profile compiled for the revision the
supervisor serves at that moment. So the next command's ensure finds the supervisor serving the older revision of the
same incarnation and tells it to advance; it re-reads the workspace's and the project's grants, compiles the profile for
them, and from then on:

1. starts every new exec host and command under that profile; exec hosts, keyed by grant revision, are never reused
   across revisions;
2. reports the new enforced `grant_revision` on subsequent jobs;
3. leaves running jobs under the profile they started with until they end or are killed. A coordinator needing a hard
   cut kills them.

No-op mutations and egress-only or simulator-only mutations leave the revision, and the supervisor, untouched.

**Named sessions refuse to cross revisions.** A named session is pinned to the supervisor profile it was opened under.
Once a filesystem grant/revoke advances the revision, a later `run` targeting that stale session is rejected with
`Conflict` naming the enforced and current revisions; it is never silently migrated or resumed. The caller opens a new
named session after the advance. Exec hosts need no migration rule: a host is keyed by the grant revision it activated
under, so no host of the old revision serves a command of the new one.

## Supervisor recovery

A supervisor that ends without retiring — killed, crashed — leaves its jobs' processes running and their records
admitted but never sealed. Two things put that right:

- **The group ledger.** A served supervisor keeps `<store>/run/<digest>.groups` beside its socket: each job's process
  group and the leader's immutable birth identity observed by its parent before reaping, naming the supervisor that
  wrote it. Every rewrite carries that original observation; it never samples an old pid again to renew a claim. A
  watcher acts only on its own lost supervisor's ledger, never one a newer supervisor has written. Older protected
  ledgers with a recorded start time remain readable, but new writers record the birth identity.

  Recovery proves that the recorded leader held its id throughout the membership snapshot. Only those proven members are
  signalled, through a kernel identity that cannot target a later process: the pid/version audit token on macOS, a pidfd
  on Linux. It sends TERM, waits the existing grace, proves ownership again, then sends KILL to still-owned members. No
  ledger operation signals a numeric pid or group after an ownership check. A reused live leader is left alone. A reaped
  or never-identified leader with surviving members is unresolved, never signalled or silently dropped. A full
  membership buffer is not absence evidence; inspection and signal failures retain the ledger and report the operational
  error. KILL is followed by bounded membership verification; live survivors or inspection errors retain the ledger
  rather than claiming release. A sealed failed-wait record is not process-release proof, so its still-unaccounted-for
  group remains recorded. Failed inspection of an inherited group likewise retains that entry while recording new
  groups.

  Every replacement supervisor inherits unresolved entries with their original job, group, and lost supervisor. Each
  subsequent ledger rewrite carries them while the group remains unaccounted for, and drops only entries proven
  released. A prior ended marker does not authorize force or erase that evidence. Unresolved groups are diagnosed
  through `doctor`; they do not prevent a new supervisor from serving safe commands.

- **Sealing.** The next supervisor binds the workspace socket first, reconciles the predecessor's groups, then seals
  every admitted but unterminated record of the current incarnation as `failed` with `failure: supervisorLost` and a
  terminal commitment. It preserves the bytes already spilled to protected files, not bytes held only in lost memory.
  The record allocator and recovery use the protected records, not a ledger's completeness: power loss may have taken an
  entry. Unresolved process evidence remains in the inherited ledger independently of the job's terminal record.

**Draining a supervisor of another build.** The manager of a newly started daemon asks every supervisor of another build
— including one that names no build at all — to drain: it admits nothing more, lets its running jobs finish, and
retires, and the next command for its workspace gets a supervisor of the new build. `drain` is the one request whose
shape never changes across builds, so any later daemon can retire any earlier supervisor. A supervisor of the daemon's
own build keeps serving across a daemon restart and retires on its own when idle. A draining supervisor retires the
moment its last job ends, usually before its client has read the end: the client reaches that job from the workspace's
next supervisor by its number, through `sealed` and `logRead`, and reads on from the bytes it already holds.

Until a draining supervisor retires, a command of the new build that reaches it is refused by name: `exec` and every
other verb that would run work there, because they cannot speak its protocol. A lifecycle verb that must change the
workspace's substrate — `detach`, `rm`, `restore`, `resize`, `mv` — does not wait for it, since a drain lasts as long as
its longest job. A lifecycle verb stops admission with `drain` and identifies the serving process from that exact
connection's kernel peer credential, cross-checking the reply's pid. TERM and, after the existing grace, KILL go only
through that non-reusable identity: a macOS audit token or Linux peer pidfd (kernel 6.5 or newer), never a later process
that reused the pid. A host lacking that identity primitive gets a typed refusal, not a raw-pid fallback.

The owner exit watch is registered before sending `drain` or a termination signal. On macOS, registration is followed by
an actual kernel pid-version match, never a guessed numeric-pid or signal-zero probe; on Linux the watch duplicates the
already pinned peer pidfd. A watch registered after the process starts exiting may fail while its descriptors are still
open, so that failure is not death proof. Termination waits on the pre-established watch within the existing grace, then
retains the exclusive replacement listener continuously through the caller's mutation. A listener inherited by another
live process remains attached and is refused even after its original creator dies.

Once the supervisor exits, the verb reconciles its group ledger as above and unlinks the socket unless something serves
it again; unresolved group evidence is retained. The jobs' records remain unterminated until the next supervisor seals
them. `exec` does not replace another build as a side effect: its refusal names the lifecycle step that owns the
cutover.

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

Lost-supervisor groups that cannot be safely identified do not block ordinary supervisor startup or new admission. They
do block destroying or replacing the canonical image: `rm`, checkpoint restore, and adopted-main restore reconcile the
ledger **after** stopping the supervisor while holding the exclusive socket/listener lease across the mutation. Unknown
hello, retirement, or exit outcomes are errors, not proof of absence; no group takeover runs beside a live supervisor.
The adopted-main refusal guard runs before recording any mutation intent, so a clean refusal cannot lock later safe
admission out through intent replay. Neither `--force` nor an ended marker authorizes detaching beneath an
unaccounted-for writer. Ordinary detach and resize use idle-only unmounting without a force fallback. Grant changes
retain their existing contract above: running jobs keep their original profiles; a revision change never claims every
old-authority process died.

## doctor awareness

`cowshed doctor` reports a detached workspace that still has a supervisor (`mount-supervisor`), with the detach and
attach that clear it. It also reports each unresolved lost-job group (`unresolved-job-group`) with its workspace, job,
group id, lost supervisor, reason, and retained ledger path. A readable live unresolved group is a warning, not a
fabricated healthy or dead group; an unreadable ledger is an error. The next ledger writer drops evidence only once the
recorded group is proven released. This read-only diagnosis grants no permission to signal a stale numeric pid,
force-detach a busy image, or erase a ledger.

## Tradeoffs

**Full per-command authority regeneration rejected.** Rebuilding the complete workspace profile and environment for
every command would pay the expensive setup cost and cannot hold persistent shell state. Launch-time supervisor
sandboxing pays that cost once per grant revision. A small deterministic inner profile is nevertheless mandatory for
every shell: it removes `.cowshed/job/**` write authority and applies request-specific narrowing without regenerating or
widening the outer profile. A one-shot exec uses the same trusted-parent/narrow-child split.

**Re-entering the shell per command rejected.** `direnv exec` per job evaluates the whole `.envrc` — for a devenv
project, a Nix evaluation and every entry hook — on every command, which is where a sandboxed command's seconds went.
Whether that evaluation is still valid is a question direnv already answers from its own recorded watch list; asking it
once per activation and watching the answer's inputs costs a few dozen `stat`s per command instead.

**Replaying an exported environment rejected.** Capturing `direnv export`/`print-dev-env` output and applying it to
later processes looks equivalent but is not: it skips entry hooks that do work (installs, generated files), and it has
no truthful invalidation — only direnv knows which inputs its evaluation read. A live activated process whose freshness
is direnv's own watch set keeps both.

**A live bash as the warm shell rejected.** A job owns anonymous pipes whose other ends only the supervisor holds, a
process group the supervisor signals, and an exact wait status. A long-lived bash can accept none of these per command:
it cannot receive descriptors, named FIFOs are reachable by sibling jobs, and its `wait` folds a signal death into
`128+N` and drops the core-dump flag. The exec host does exactly what bash cannot and nothing more; shell text belongs
to a shell interpreter, not to the process that keeps the activation.

**Scripts interpreted inside the warm host rejected.** Running a script in the host's own brush interpreter would save a
fork but break the job contract: with job control off every external command joins the interpreter's own process group
(so a job kill would kill the host), with it on each pipeline gets a group of its own (so `a; b &` escapes a kill), a
command's signal death is folded into exit `128+N` with no way to report the signal, and `umask`, `ulimit`, `trap` and
`exec` act on the host process that later commands share. A fork of the warm host costs about a millisecond and inherits
the activated environment, so a per-script child keeps every property of an argv job without changing the interpreter.

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

Once the protected terminal record is sealed, a refused output copy cannot leave the live job `running`: terminal state,
exit, artifacts, commitment, and session removal advance before the original copy error reaches an early or late
`wait`/`kill` caller; normal process-group bookkeeping still concludes. A refused terminal commitment likewise fails its
callers without replacing sealed state, stream counts/hashes, or quota metadata with the admission's empty values. The
completed in-memory outcome is either a sealed artifact with separate commitment/publication errors, or a genuine
sealing failure; it cannot claim both sealed and unsealed.

A true sealing failure answers `info`, `list`, `wait`, and `kill` with that error rather than presenting a fake empty
success. The captured bytes remain readable through `logRead`; `sealed` reports no terminal record. Every caller is
answered without hanging, and retirement may proceed after the child has ended. The next supervisor uses the existing
lost-job recovery for records that genuinely remained unterminated, preserving the distinction between a child's exit, a
protected terminal record, and its controller commitment.
