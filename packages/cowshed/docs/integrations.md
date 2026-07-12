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

## Multi-client jobs

One persistent Unix socket per workspace supervisor accepts concurrent clients. The runtime directory is mode 0700, the
socket is mode 0600, and peer credentials must identify the expected uid. Disconnecting a client detaches only that
view: it does not stop jobs or unlink the socket. Clients reconnect and resume by durable job ID and backing-file
offset.

Every accepted exec receives a positive workspace-local monotonic numeric `u64` job ID, allocated before process
creation and never reused. Its durable key is `(repoId, workspaceIncarnation, jobId)`, so copied timelines remain
distinguishable. Complete raw streams remain separate and uncapped by summaries:

```text
.cowshed/job/<id>/out
.cowshed/job/<id>/err
.cowshed/job/records.arrow
```

Control messages, N-API objects, MCP results, and Arrow records carry metadata plus separate `stdout` and `stderr`
stream information: `{ path, bytes, summary: { version, text, truncated } }`. Summaries are deterministic, bounded, and
redacted diagnostics. They never determine exit status, sandbox denial, policy, audit, quota, or build/test success.

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

### Literal-redirection optimization

Ordinary shell parsing and redirection semantics are always the correctness fallback. An optional fast path may use a
real shell AST parser—never regexes or output sniffing—for exactly one top-level simple command with literal,
in-workspace, same-filesystem, nonexistent `>path` and/or `2>path` targets using clobber semantics. It is ineligible for
append, descriptor duplication, pipelines, subshells, expansions, symlinks, noclobber, or any ambiguity.

For an eligible command, the supervisor creates the inode and may hardlink the destination and corresponding job `out`
or `err` to it; tailing and quota accounting must recognize that they share one inode. Ineligible or ambiguous input
runs through ordinary shell execution and normal capture. This optimization is never an authority, denial-evidence,
quota-correctness, or general correctness dependency. Tests must pin shell-equivalent behavior, eligibility and fallback
boundaries, inode-aware tail/accounting, and quota enforcement.

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

The N-API surface mirrors the Rust types asynchronously. It must preserve numeric job IDs, separate raw-byte streams
with backpressure, cancellation through `AbortSignal`, typed terminal state, grant revision, and trace identity. It must
not flatten job output into one string or derive security evidence from text.

```ts
import { openProject, connectWorkspace } from '@smoothbricks/cowshed';

const project = await openProject('<project-root>');
const coordinator = await project.coordinator();
const workspace = await coordinator.createWorkspace(`task-${id}`);
const worker = await connectWorkspace(workerDescriptor);
const job = await worker.exec(['bun', 'test'], { cwdRel: 'packages/example' });
const info = await job.wait();

await worker.push();
await coordinator.destroyWorkspace(workspace.name, { ifPushed: true });
```

The CLI remains the integration point for shell-only consumers. Rust, N-API, CLI, and MCP frontends must agree on
lifecycle, grant propagation, numeric jobs, output storage and summaries, quota termination, and the distinction between
authorization, sandbox denial, child exit, and internal failure.
