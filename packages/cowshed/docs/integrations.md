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

Workspace Git uses local paths only: the main mount and coordinator-created, gateway-owned bare mirrors. Mirrors are
sandbox-read-only and use host-held credentials on the control plane. A workspace may read a mirror but can never
configure, update, or write it. Remote publication is coordinator work outside the sandbox.

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

Control messages, N-API objects, MCP results, JSON envelopes, and controller Arrow commitments never duplicate unbounded
raw output. A bounded `JobInfo` carries lifecycle metadata, `StreamInfo`, byte counts, hashes, redacted summaries, and
may carry small `Inline.data` bytes tagged as `utf8` or `base64`. Larger or live output remains a handle. Full-fidelity
output of any size is available through explicit raw byte streams returned by `cowshed job logs`, `cowshed job attach`,
or artifact-read APIs, with stdout and stderr kept separate.

The controller Unix-socket lane keeps JSON strictly control-only. An RPC header may declare top-level camel-case
`binaryLength`; the actor then transfers exactly one separate u32-length-prefixed raw frame, capped at 64 KiB, before
processing another request. Uploads use this for inline/streamed stdin and attachment writes. `job.logs` responses
return JSON metadata `{eof,nextOffset}` plus a separate raw frame; the actor requires
`nextOffset == requestedOffset + binaryLength` with checked arithmetic. JSON-only calls reject binary metadata, and
binary calls reject missing, oversized, unsolicited, or length-mismatched frames. This preserves arbitrary bytes without
base64/JSON-array allocation while the bounded actor/channel supplies backpressure.

Protected in-volume Arrow records and canonical `Inline`/`File` artifacts are captured-content authority for their
originating incarnation/checkpoint snapshot. Controller Arrow stores compact commitments to job existence, lifecycle,
ordering, fork/restore lineage, terminal state, terminal-batch digest, stream byte counts, and stream hashes. It stores
no artifact payload or path authority. A missing committed artifact or digest mismatch is a typed integrity failure, not
a last-writer-wins repair.

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

The first binding surface is intentionally read-only and endpoint-backed. A trusted spawner supplies a connected
controller descriptor out of band; `coordinatorEndpoint` takes ownership, marks it close-on-exec, and permits exactly
one handshake attempt. `openProject` discards coordinator authority before exposing `Project` and `WorkspaceRef`.

```ts
import { coordinatorEndpoint, openProject } from '@smoothbricks/cowshed';

// `endpointFd` is an inherited socket from the trusted controller, never token text.
const endpoint = coordinatorEndpoint(endpointFd);
const project = await openProject(endpoint, '<project-root>');
const workspaces = await project.listWorkspaces();
const main = await project.main();

const current = await main.info();
const ensured = await main.ensure();
await main.attach({ browse: false });
const grants = await main.grants();
```

Both runtimes receive the same typed `CowshedError` with stable kebab-case `code`, exact `message`, and actionable
`hint`. Workspace, ensure, and grant DTOs are serialized directly from cowshed-core and Typia-validated by the
TypeScript facade.

Coordinator mutation, one-use worker descriptors, jobs, raw-byte streams with backpressure, and `AbortSignal`
cancellation remain the next binding slice. They must preserve the existing core authority boundaries; the addon does
not expose raw RPC, `ProjectRuntime`, or the CLI's in-process `ActorBridge` as a shortcut.

The CLI remains the integration point for shell-only consumers. Rust, N-API, CLI, and MCP frontends must agree on
lifecycle, grant propagation, numeric jobs, tiered artifact storage, bounded summaries and control responses, raw
logs/attachment/export behavior, quota termination, and the distinction between authorization, sandbox denial, child
exit, integrity failure, and internal failure.
