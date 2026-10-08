# Rust and N-API integration

This document defines the intended consumer contract. It is UX acceptance for an implementation; it does not claim that
the APIs shown here are already available.

Rust applications use `cowshed-core` directly rather than spawning the CLI on a hot path. Bun and Node applications use
the asynchronous `cowshed-napi` bindings. Shell-only clients use the CLI, and MCP clients use `cowshed-mcp`. Every
frontend is required to preserve the same typed lifecycle, sandbox, job, and error contracts.

```toml
# Cargo.toml
[dependencies]
cowshed-core = { path = "<repo-root>/packages/cowshed/crates/cowshed-core" }
```

## Repository binding

Opening a project resolves its Git root and its controller-owned repository binding. The binding records one explicitly
chosen remote URL and the stable machine-independent `repo_id` derived from it. Supported remote forms normalize to
lowercase `owner/repo`; transport, credentials, host, leading slash, optional `.git`, query, and fragment do not
contribute. Every open re-normalizes the recorded URL and conflicts if it does not equal the recorded `repo_id`.

A checkout may expose several candidate identities, but exactly one binding is primary. Discovery may propose candidates
and must never silently select or mint one. A local-only repository therefore requires an explicit `repo_id`. Its two
components are validated independently; empty components, `.`, `..`, separators, NUL, and noncanonical forms are
rejected. For `repo_id = acme/widget`, trusted project policy is controller-owned at
`~/.cowshed/acme/widget/policy.json`, outside every workspace and denied to sandboxes.

## Authority split

The coordinator is the sole mutating authority for create, fork, grant, revoke, land, mirror refresh, and destroy. A
`WorkspaceHandle` is non-escalating and scoped to one workspace: it can run and observe jobs, checkpoint, and push, but
cannot modify grants, destroy workspaces, refresh mirrors, or reach siblings.

Workspace Git fetches from the main mount, and from the network only through an intercepted egress grant, which admits
fetch and refuses push. Remote publication is coordinator work outside the sandbox.

```rust
use cowshed_core::{Cowshed, CreateOptions, ExecRequest};

let project = Cowshed::open("<project-root>").await?;
let coordinator = project.coordinator()?;
let workspace = coordinator.create("task-raven", CreateOptions::default()).await?;
let worker = coordinator.worker("task-raven")?;

let job = worker
    .exec(ExecRequest::new(["cargo", "test", "-p", "example-core"])
        .cwd_rel("crates/example-core")
        .trace(task_context))
    .await?;

let info = job.wait().await?;
worker.push(Default::default()).await?;
coordinator.destroy("task-raven", cowshed_core::Destroy::IfPushed).await?;
```

The snippet is contract-shaped pseudocode. Exact exported names must follow `specs/cowshed/07_api.md` when implemented.

## Grants and supervisor revisions

A fresh workspace starts from the closed baseline. The coordinator mutates the controller-owned grant snapshot with
compare-and-swap revision semantics; workers may observe grants but cannot mutate them.

- Egress and simulator changes apply immediately because the gateway evaluates current policy per request.
- An effective filesystem grant or revoke changes the supervisor envelope. The old supervisor stops accepting
  submissions and drains; admitted jobs retain the old revision. The controller then relaunches under the new revision
  before accepting the next exec.
- A command that read the grants just before another change landed is answered by the supervisor already serving the
  newer revision, under that revision: it never runs under grants older than the ones it read, and never fails because
  grants moved forward under it.
- Inner command profiles may narrow the revision-bound envelope and can never widen it.
- A named session pinned to a stale revision conflicts instead of silently migrating.
- A no-op, egress-only, or simulator-only mutation does not relaunch the supervisor.

There is no interactive MCP consent or elicitation. A worker reports a denied need to its coordinator, which applies
project policy and decides whether to grant it. Simulator installation remains human-gated per artifact, and desktop
promotion remains a human-run personal-session action; coordinator authority does not bypass either boundary.

## Multi-client jobs and artifact handles

One persistent Unix socket per workspace supervisor accepts concurrent clients. The runtime directory is mode 0700, the
socket is mode 0600, and peer credentials must identify the expected uid. Disconnecting a client detaches only that
view: it does not stop jobs or unlink the socket. Clients reconnect and resume by the durable
`(repo_id, workspace_incarnation, job_id)` identity and a byte offset in the selected artifact.

Every accepted exec receives a positive workspace-local monotonic numeric `u64` job ID, allocated before process
creation and never reused. `stdout` and `stderr` are separate:

```text
StreamInfo { storage, bytes, sha256, summary }
OutputStorage = Captured { artifact } | Redirect { source: WorkspacePath, artifact }
ProtectedOutput = Inline { data: BinaryData } | File { path: WorkspacePath }
```

Small terminal streams may be stored directly as Arrow Binary. Protected files under `.cowshed/job/**` spill lazily when
a stream needs file backing, rather than once per job. Representation-transparent reads always resolve `artifact`;
`Redirect.source` is the live caller-visible destination and is never content authority. The supervisor is the only
writer to the protected subtree. Every executed shell, named session, and descendant receives a child restriction before
repository-controlled startup; complete record batches and sealed spill files are immutable.

Control messages, N-API objects, MCP results, JSON envelopes, and controller audit records never duplicate unbounded raw
output. A bounded `JobInfo` carries lifecycle metadata, `StreamInfo`, byte counts, hashes, redacted summaries, and may
carry small `Inline.data` bytes tagged as `utf8` or `base64`. Larger or live output remains a handle. Full-fidelity
output of any size is available through explicit raw byte streams returned by `JobHandle.logs`, `JobHandle.attach`, or
artifact-read APIs, with stdout and stderr kept separate.

The controller Unix-socket lane keeps JSON strictly control-only. An RPC header may declare top-level camel-case
`binaryLength`; the actor then transfers exactly one separate u32-length-prefixed raw frame, capped at 64 KiB, before
processing another request. Uploads use this for inline/streamed stdin and attachment writes. `job.logs` responses
return JSON metadata `{eof,nextOffset}` plus a separate raw frame; the actor requires
`nextOffset == requestedOffset + binaryLength` with checked arithmetic. JSON-only calls reject binary metadata, and
binary calls reject missing, oversized, unsolicited, or length-mismatched frames. This preserves arbitrary bytes without
base64/JSON-array allocation while the bounded actor/channel supplies backpressure.

Protected in-volume Arrow records and canonical `Inline`/`File` artifacts are captured-content authority for their
originating incarnation/checkpoint snapshot. The controller's audit records carry compact telemetry of job existence,
lifecycle, ordering, fork/restore lineage, terminal state, terminal-batch digest, stream byte counts, and stream hashes;
they store no artifact payload or path and are never read for a decision. A missing artifact or digest mismatch inside
the image is a typed integrity failure, not a last-writer-wins repair.

A configurable combined stdout-plus-stderr quota defaults to 1 GiB and counts persisted plus read-but-not-yet-persisted
bytes. At the first crossing, the supervisor atomically stops accepting payload past the boundary, sends TERM to the
whole process group, waits the grace period, sends KILL if necessary, drains both pipes to EOF, fsyncs, and records an
authoritative `output-limit` terminal state with limit and crossing metadata. It never silently truncates output while
allowing the job to continue.

### Structured stdin

`ExecRequest` stdin is typed as none, inline binary or a streamed byte source, or a workspace-relative file. File input
is not shell text: the supervisor resolves it beneath the workspace, opens it without following symlinks, and streams it
with backpressure. EOF closes the child's stdin; cancellation closes the source and participates in the job's normal
cancellation/termination path. Job metadata records the stdin source kind and byte count, never the input content. No
variant interpolates a filename into a shell command.

### Script jobs

A job is either a byte-exact argv or a script: bash-compatible text given as literal `parts` with `values` between them,
the shape a tagged template produces. From TypeScript:

```ts
await worker.exec({
  script: { parts: ['grep -r ', ' src | wc -l'], values: [{ word: 'needle with spaces' }] },
});
```

A value is never shell text. The supervisor binds each one to a shell variable and puts a reference to it where the
value stood, so `{ word }` stays one word (or one piece of a word) whatever it contains, and `{ words }` becomes one
word per element where it stands alone as a word. Placing a value inside a comment, inside `$'…'`, or right after an
unpaired backslash is a `usage` error at submission. Arithmetic is not detected: in `$((…))`, `((…))`, `let`, an array
subscript or an integer variable the shell evaluates a variable's content as an expression, so keep values out of those
places. The script runs in the workspace's warm exec host with bash defaults (no `errexit`, no `pipefail`); a script
that does not parse ends `failed` with `failure: "scriptSyntax"`, exit code 2, and the parser's message on stderr, and
nothing from it runs. `JobInfo` then carries `script` instead of `argv`. Script jobs need the `cowshed` binary as the
controller (it carries the interpreter); they run under the workspace `.envrc` when it has one and in the plain sandbox
environment when it does not.

### Shell redirects and sealed export

An optional real-shell-AST fast path may recognize only a proven literal `>`/`2>` workspace destination. While the
command runs, the shell writes that live caller-visible path and `OutputStorage::Redirect.source` names it. After the
job is terminal, cowshed independently snapshots the admitted bytes into `Redirect.artifact`: Arrow Binary when small or
a protected clone/reflink/copy file when large. The writable source is never authoritative and never hardlinked to the
protected artifact. Arbitrary or ambiguous shell text keeps ordinary shell semantics; bytes redirected away from the
supervisor's pipes without this proven representation are not in the job handle.

`ExecRequest` also supports separate `stdout_copy` and `stderr_copy` publication destinations. These are materialized
post-terminal from the canonical protected artifact through an independent clone/reflink/copy and atomic rename. They do
not change `StreamInfo.storage` and are never used for reads or authority. Publication failure is a typed operational
error and does not rewrite the already-established process exit or output-limit state. Neither path uses hardlinks.

## Embedding the controller

A program that links `cowshed-core` — a supervising runtime rather than the CLI — does not open a project in-process.
The daemon's manager starts workspace supervisors only for its own build, the `LC_UUID` the linker derived from the
`cowshed` binary, and a program linking cowshed is always another build: an in-process controller has every exec,
session and sandboxed Git step refused with `Conflict` ("the cowshed daemon is build …; this cowshed is build …").

The program runs its controller as the host's own `cowshed` instead. It makes a socketpair, starts
`cowshed --project <root> controller` with one end as its standard input, and connects on the other end:
`Cowshed::connect(descriptor)` yields the same `Cowshed` and `CoordinatorToken` an in-process controller would, and
every call it makes runs in the child. Dropping every handle closes the socket, and the child shuts the project down and
exits 0. The child reconciles the project's gateway sessions before each exec, shell and checked land, as `cowshed exec`
and `cowshed land --check` do, and records controller commitments into the host's default sink
(`COWSHED_CONTINUITY_AUDIT`, [telemetry](telemetry.md)); the embedder keeps neither. A child that cannot open the
project exits with the typed error on its stderr and closes the socket, so the embedder's handshake fails instead of
waiting.

An install that starts the daemon of a new build leaves a running controller on the old one. The new daemon drains the
old build's supervisors — each finishes its running jobs and retires — and refuses every ensure the old controller sends
with `Conflict` carrying `otherBuild: { daemon, caller }`, the two builds as data. The client notes the first such
answer on its connection, `Coordinator::other_build()`, so the embedder learns its controller can start nothing more
without reading the sentence. It starts `cowshed controller` again — the host's `cowshed` is the new build once the
install has run — and sends its new work there. A job the old controller started stays reachable through it while the
draining supervisor still serves. That supervisor retires the moment the job ends, usually before the old controller has
read the end; once a call of the job meets the refusal, the new controller reaches it by its number from the workspace's
next supervisor — `WorkspaceHandle::sealed` answers its terminal record — and reads its output on from the bytes already
held (`JobHandle::logs(stream, offset, follow)`). The old controller exits 0 when its last handle is dropped.

## MCP authority delivery

Coordinator authority is supplied by the trusted spawner over an inherited dedicated file descriptor or socketpair. The
MCP server validates it and immediately closes it or marks it non-inheritable before any workspace process can be
spawned. Coordinator authority never appears in environment variables, argv, stderr, workspace files, or ordinary token
text.

Worker connection descriptors are separate 256-bit random capabilities: one-use, 30-second TTL, memory-only, atomically
consumed, invalid after server restart, and bound to the intended workspace plus peer/socket identity. Presenting worker
authority to a coordinator-only tool fails before execution with an authorization error distinct from sandbox denial and
other domain errors.

## N-API

Node and Bun use the same napi-rs `.node` addon. There is no separate synchronous `bun:ffi` lane: workspace discovery,
attachment, execution, and lifecycle calls are IO-bound and remain Promise-based on both runtimes.

The binding is endpoint-backed. A trusted spawner supplies a connected controller descriptor out of band;
`coordinatorEndpoint` takes ownership, marks it close-on-exec, and permits exactly one handshake attempt. `openProject`
discards coordinator authority before exposing `Project` and `WorkspaceRef`; `connectCoordinator` retains it in a
`Coordinator`, whose `worker(workspace)` returns a non-escalating one-workspace handle. Every handle opened from one
endpoint shares its connection, and calls run concurrently: a pending `job.wait()` or following log read never holds
another call.

```ts
import { coordinatorEndpoint, openProject } from '@smoothbricks/cowshed';

// `endpointFd` is an inherited socket from the trusted controller, never token text.
const endpoint = coordinatorEndpoint(endpointFd);
const project = await openProject(endpoint, '<project-root>');
const workspaces = await project.listWorkspaces();
const main = await project.main();

const current = await main.info();
await main.attach({ browse: false });
const grants = await main.grants();
```

Both runtimes receive the same typed `CowshedError` with stable kebab-case `code`, exact `message`, and actionable
`hint`. Workspace and grant DTOs are serialized directly from cowshed-core and Typia-validated by the TypeScript facade.
Keyed refusal details are native objects generated from that same declaration: `CowshedError.admission` distinguishes
changed request fields, an already-bound stdin reader, and unreadable keyed history. No message parsing or JSON-string
cause conversion is involved.

The addon exposes coordinator lifecycle operations, workspace exec and named sessions, numeric and keyed job lookup,
`status()`, `resources()`, `wait()`, `kill()`, `detach()`, `logs({ stream, offset, follow })`, bounded
`tail(cursor, limits)`, and `progress(everyMs)` as an `AsyncIterable`. An exec's optional `admissionKey` binds its
authored request to one job of the immutable workspace incarnation before spawn; repeating it returns that job, and
`worker.jobByKey(key)` recovers its handle after a lost reply. Dropping a job handle or detaching its view does not kill
the job. `logs` answers one chunk of a stream from `offset` with its `nextOffset` and `eof`; reading again from
`nextOffset` continues where the chunk ended, and `follow` waits for bytes or the stream's end. It is not a bounded
running-command tail.

`resources()` reads the canonical sample directly through the generated `job.resources` adapter, not by decoding a full
status result. Before the job owns a process it reports the typed not-ready conflict; once the job ends it returns the
same frozen sample its terminal record carries.

### Implementation status — monitoring gaps

Core job resource samples and terminal persistence, controller cursor-addressed bounded tails, and keyed admission and
lookup are implemented. Periodic progress samples stream over the controller and through the generated N-API adapter.
Resumable N-API raw-byte streams, full attachment stdio/EOF and `AbortSignal` plumbing remain unbuilt. The Rust core
supports numeric and keyed reattachment and attachment stdin writes; its `JobStdin` still has no explicit close
operation on main, and the addon does not yet expose attachment. One-use worker descriptor connection is also unbuilt.
Fork/exec tree observations, per-process CPU/RSS/I/O and blocker facts, typed process event streams, CPU-winning leaf
identity, and their `process.run`/job spans are also unbuilt. Complete cgroup job totals, separate charged-memory
counters, measured fork/exec/exit observation and explicit unattributed-usage reconciliation are unbuilt as well.

The controller and N-API monitoring surface is generated from the same canonical API declarations, including resource
and process-group samples, workspace/build-volume usage, journal cursors and tails, attach, kill, and progress events.
TypeScript public types and validators are generated projections, never a second hand-maintained field list. Every
declared controller operation reaches the addon as a generated adapter on the handle that binds its authority, with a
generated TypeScript declaration whose argument type picks exactly the request fields the caller names. Of the
monitoring surface above, `progress`, `kill`, `tail` and `listeningPorts` are declared; the rest is generated once it is
declared.

Each `JobResourceSample` carries its own `jobId`. Its start baseline is the first job-owned process, including a cold
shell's activation; progress and terminal accounting include that activation, but never charge the idle time of a
previously warm host. Readiness uses `listeningPorts()` from the owned group rather than a host-wide probe. Attachment
`write()` and `end()` preserve binary input, backpressure, and exactly-once EOF without implicitly cancelling the
command.

On macOS a sample's `accounting` is cumulative leader-own plus reaped-children CPU: each leader's own CPU plus every
child it reaped, in microseconds converted from Mach ticks through the kernel timebase. A cold host's activation counts
up to its end, once, and descendants born and reaped between samples still count. It is not a complete job total: it
misses descendants still running, those exited and not yet reaped, and orphans reparented to another reaper, and an
exited leader's rusage stays fixed at its exit while orphans run on. Its storage bytes are `null`, never zero, because
the children accumulators carry none. A Linux sample's `accounting` is `null` until its cgroup v2 totals exist.

`processes()` returns the identity-fenced tree, including exited descendants and their final usage;
`processEvents(everyMs)` streams birth/exec/change/heartbeat/exit records. Both derive from the same API declaration as
the controller. Low CPU never supplies a guessed blocker; `none` means observed unblocked state, and missing evidence
remains explicit. The span projection uses the compact fixed column set in the telemetry specification, not JSON
payloads or one column per process. No command flavor or argv-derived expectation is required.

Process snapshots carry honest coverage alongside the retained rows. Resource receipts carry independent job-accounting
sources and named checked units, with separate charged memory rather than an RSS substitution. Short-lived descendants
are accounted even if an observer missed their spans; the missing attribution is an explicit row/gap, and an unknown
leaf stays absent instead of polluting a baseline. The same generated records reach the controller and N-API.

The CLI remains the integration point for shell-only consumers. Rust, N-API, CLI, and MCP frontends must agree on
lifecycle, grant propagation, numeric jobs, tiered artifact storage, bounded summaries and control responses, raw
logs/attachment/export behavior, quota termination, and the distinction between authorization, sandbox denial, child
exit, integrity failure, and internal failure.
