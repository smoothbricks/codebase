# Programmatic APIs

Two surfaces over one core: the `cowshed-core` Rust crate (jcode links it directly) and `@smoothbricks/cowshed` NAPI
bindings (Bun/Node). The CLI is a thin third client of the same core — anything the CLI can do, both APIs can do, with
identical semantics and error taxonomy.

## Authority model (frozen)

Three handle types, one rule: **a handle reachable from inside a sandbox must not authorize escalation.** Mutation
authority is not a method on the workspace you happen to hold — it lives on `Coordinator`, which a sandboxed worker can
never obtain.

- **`Project`** — discovery and trusted-local entry point. Resolves a project from a path, enumerates workspaces,
  returns _read-only_ `WorkspaceRef` views, and runs whole-project maintenance (`gc`, `doctor`). It does **not** create,
  destroy, grant, or otherwise mutate a workspace's authority.
- **`Coordinator`** — the **sole** mutation authority: `adopt`, `create`, `fork`, `grant`, `revoke`, `rebase`, `land`,
  `destroy`, `checkpoint`, `restore`, `slot_assign`, and minting worker handles. An unsandboxed controller (jcode's
  swarm coordinator, a top-level agent) holds it; it is obtained by connecting with the coordinator token, never handed
  to a worker.
- **`WorkspaceHandle`** — a worker's non-escalating capability over exactly one workspace: exec, shells, jobs,
  checkpoint, push, and _read-only_ grant inspection. No grant/revoke, no destroy, no cross-workspace access. This is
  the type a subagent receives.

## cowshed-core (Rust)

Design rules: async (tokio), no global state, no interior config lookup — everything reachable from an explicit handle;
all filesystem/mount state derived per call (01_storage.md).

```rust
/// Stateless entry point.
pub struct Cowshed;
impl Cowshed {
    /// Resolve a project from any path inside the repository.
    pub async fn open(path: impl AsRef<Path>) -> Result<Project, CowshedError>;
    /// Bind coordinator authority (unsandboxed controller only) over a resolved project.
    pub async fn coordinator(project: &Project, token: &CoordinatorToken)
        -> Result<Coordinator, CowshedError>;
}

/// Discovery and read-only entry point. Holds no mutation authority.
pub struct Project { /* project_id, git root, cowshed dirs — cheap, Clone */ }
impl Project {
    pub async fn main(&self) -> Result<WorkspaceRef, CowshedError>;
    pub async fn workspace(&self, name: &str) -> Result<WorkspaceRef, CowshedError>;   // read-only view
    pub async fn list(&self) -> Result<Vec<WorkspaceInfo>, CowshedError>;
    pub async fn gc(&self) -> Result<GcReport, CowshedError>;
    pub async fn doctor(&self) -> Result<Vec<Finding>, CowshedError>;
}

/// Read-only view of one workspace: identity, mount state, grant inspection, ensure/attach.
/// Carries no lifecycle or grant mutation — obtain those from `Coordinator`.
pub struct WorkspaceRef { /* name, image path, marker snapshot */ }
impl WorkspaceRef {
    pub fn name(&self) -> &str;
    pub fn mount_path(&self) -> &Path;               // canonical, whether or not attached
    pub async fn info(&self) -> Result<WorkspaceInfo, CowshedError>;
    pub async fn ensure(&self) -> Result<EnsureReport, CowshedError>;   // heal + env snapshot
    pub async fn attach(&self, opts: AttachOptions) -> Result<(), CowshedError>;
    pub async fn detach(&self) -> Result<(), CowshedError>;
    pub async fn grants(&self) -> Result<GrantSet, CowshedError>;       // read-only
    pub async fn exec(&self, req: ExecRequest) -> Result<ExecHandle, CowshedError>;

    /// Sandbox plumbing for embedders that spawn processes themselves (jcode
    /// supervisor): a profile + env snapshot the embedder applies at launch.
    pub async fn sandbox_spec(&self, mode: RunSandboxMode) -> Result<SandboxSpec, CowshedError>;
}
```

### Exec and grants

```rust
pub enum RunSandboxMode { ReadWrite, ReadOnly }

pub struct ExecRequest {
    pub argv: Vec<String>,
    pub cwd: Option<PathBuf>,            // relative to mount; default mount root
    pub mode: RunSandboxMode,            // default ReadWrite
    pub env: HashMap<String, String>,    // filtered through the build-config allowlist
    pub trace: Option<TraceContext>,     // W3C context; propagated into the job env as TRACEPARENT (13_telemetry.md)
    pub stdin: Stdio, pub stdout: Stdio, pub stderr: Stdio,
}
pub struct TraceContext { pub trace_id: [u8; 16], pub span_id: [u8; 8] }  // adopted or minted per request

pub struct ExecHandle {
    pub pid: u32,
    pub grant_revision: u64,             // the snapshot this tree enforces
    /* streamed io */
    pub async fn wait(self) -> Result<ExecOutcome, CowshedError>;
    pub fn kill(&self) -> Result<(), CowshedError>;   // hard revocation = kill + re-exec
}

pub struct GrantSet {
    pub revision: u64,
    pub port_block: PortBlock,           // { base, size }: base = data-plane gateway listener,
                                         //   base+1..size-1 = the workspace's own service ports (04_sandbox.md)
    pub read: Vec<PathBuf>,
    pub write: Vec<PathBuf>,
    pub egress: Vec<EgressRule>,         // { host, ports, mode, impersonate }
    pub repos: Vec<RepoRule>,            // repo-scoped mirror grants (05_gateway.md)
    pub sim: Vec<SimVerb>,               // personal-session simulator broker verbs (04/05/14_nix.md)
}
pub enum SimVerb { OpenUrl, Install }    // closed enum; unknown verbs are usage errors
pub struct PortBlock { pub base: u16, pub size: u16 }   // default size 16, from the reserved range
pub struct EgressRule {                 // 04_sandbox.md / 05_gateway.md
    pub host: String, pub ports: Vec<u16>,
    pub mode: EgressMode,                // default Intercept (per-workspace CA); Opaque = pass-through CONNECT
    pub impersonate: Option<String>,     // outbound TLS fingerprint; suppresses header injection
}
pub enum EgressMode { Intercept, Opaque }
pub struct GrantDelta {
    pub read: Vec<PathBuf>, pub write: Vec<PathBuf>,
    pub egress: Vec<EgressRule>, pub repos: Vec<RepoRule>, pub sim: Vec<SimVerb>,
    pub expected_revision: Option<u64>,  // CAS: reject with Conflict if the on-disk revision differs
}

pub struct SandboxSpec {
    pub grant_revision: u64,
    pub env: Vec<(String, String)>,      // identity + cache wiring (03_caches.md)
    pub fn seatbelt_profile(&self) -> &str;
    pub fn wrap(&self, argv: &[String]) -> Vec<String>;  // sandbox-exec -f … /usr/bin/env …
}
```

`GrantDelta::expected_revision` is the compare-and-swap hook: when set, `grant`/`revoke` refuse
(`CowshedError::Conflict`) if the grant file has moved on since the caller last read it, so two coordinators cannot
silently clobber each other (mutation semantics in 04_sandbox.md). There are **no SSH-key or Docker fields** in the
grant model — the axes are read, write, egress, and repo.

This is the jcode integration point: the swarm coordinator holds a `Coordinator`, hands each worker a `WorkspaceHandle`,
calls `Coordinator::grant` as tasks prove needs (its `grant_write_paths` semantics, extended to reads, egress, and
repos), and either lets cowshed spawn (`exec`) or takes a `SandboxSpec` to wrap its own supervisor launch.

## Capability split: `Coordinator` vs `WorkspaceHandle`

Mutation authority lives only on `Coordinator`; `WorkspaceHandle` is the non-escalating worker capability. Together they
mirror the MCP token model (12_mcp.md) in the type system:

```rust
/// The sole mutation authority over a project's workspaces. jcode's swarm coordinator holds this,
/// obtained from `Cowshed::coordinator` with the coordinator token. The only thing that can adopt,
/// create, destroy, fork, grant, revoke, rebase, land, checkpoint, restore, or assign slots.
pub struct Coordinator { /* Project + controller identity */ }
impl Coordinator {
    pub async fn adopt(&self, opts: AdoptOptions) -> Result<WorkspaceRef, CowshedError>;
    pub async fn create(&self, name: &str, opts: CreateOptions) -> Result<WorkspaceRef, CowshedError>;
    pub async fn fork(&self, src: &str, dst: &str) -> Result<WorkspaceRef, CowshedError>;
    pub async fn grant(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    pub async fn revoke(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    pub async fn rebase(&self, ws: &str, opts: RebaseOptions) -> Result<Oid, CowshedError>;
    pub async fn land(&self, ws: &str, opts: LandOptions) -> Result<Oid, CowshedError>;
    pub async fn checkpoint(&self, ws: &str, label: Option<&str>) -> Result<String, CowshedError>;
    pub async fn restore(&self, ws: &str, label: &str) -> Result<(), CowshedError>;
    pub async fn assign_slot(&self, ws: &str, slot: u32) -> Result<(), CowshedError>;
    pub async fn destroy(&self, ws: &str, opts: RemoveOptions) -> Result<(), CowshedError>;
    /// Hand a worker a capability scoped to exactly one workspace.
    pub fn worker(&self, ws: &str) -> Result<WorkspaceHandle, CowshedError>;
}

/// A worker's capability: it can run and observe work in *its* workspace and hand results
/// back, but it can never widen its own sandbox or touch another workspace. Escalation is
/// the coordinator's job. This is the type a subagent receives; it carries no grant mutation.
pub struct WorkspaceHandle { /* one workspace, worker capability */ }
impl WorkspaceHandle {
    pub fn name(&self) -> &str;
    pub fn mount_path(&self) -> &Path;
    pub async fn exec(&self, req: ExecRequest) -> Result<ExecHandle, CowshedError>;
    pub async fn shell(&self, session: Option<&str>) -> Result<Session, CowshedError>;
    pub async fn jobs(&self) -> Result<Vec<JobInfo>, CowshedError>;
    pub async fn job(&self, id: &JobId) -> Result<Job, CowshedError>;
    pub async fn checkpoint(&self, label: Option<&str>) -> Result<String, CowshedError>;
    pub async fn push(&self, opts: PushOptions) -> Result<PushReport, CowshedError>;
    pub async fn grants(&self) -> Result<GrantSet, CowshedError>;   // read-only: observe, never mutate
    // No grant/revoke, no destroy, no rebase/land, no cross-workspace access.
}
```

The invariant is the same one that keeps grant files outside the volume (01_storage.md, 04): a capability reachable from
inside a sandbox must not authorize escalation. `WorkspaceHandle` is that principle expressed in the type system — a
subagent holding one cannot grant itself anything.

## DTO freeze (single source of truth)

These types are defined **once** in `cowshed-core` and reused verbatim by the CLI (`--json` bodies), NAPI, and MCP — no
adapter redefines a field, and the contract goldens (08_testing.md) pin their shapes: `WorkspaceInfo`, `EnsureReport`,
`GcReport`, `Finding`, `ExecOutcome`, `ExecRecord`, `PushReport`, `JobInfo`,
`GrantSet`/`GrantDelta`/`PortBlock`/`EgressRule`/`RepoRule`/`SimVerb`, `GatewayStatus`, `AuditEvent`, and every
`*Options` type (`AdoptOptions`, `CreateOptions`, `AttachOptions`, `RemoveOptions`, `RebaseOptions`, `LandOptions`,
`PushOptions`). Field sketches elsewhere in this spec are illustrative; the freeze rule — one definition, reused,
versioned together — is the contract. Adding a field is a coordinated change across core + goldens, not a per-adapter
patch.

## Shell client (`cowshed-shell`)

Types for the warm-shell layer (11_shell.md); `WorkspaceHandle::shell`/`jobs` return these.

```rust
pub struct Session { /* supervisor connection, named or anonymous */ }
impl Session {
    pub async fn run(&self, req: ExecRequest) -> Result<ExecHandle, CowshedError>;  // reuses a warm shell
    pub async fn background(&self, req: ExecRequest) -> Result<JobId, CowshedError>;
    pub fn is_named(&self) -> bool;
    pub async fn close(self) -> Result<(), CowshedError>;   // named: persist; anonymous: return to pool
}

pub struct Job { /* id, state, spool paths inside the volume */ }
impl Job {
    pub async fn status(&self) -> Result<JobInfo, CowshedError>;
    pub async fn logs(&self, stream: Stream, follow: bool)
        -> Result<impl futures::Stream<Item = Bytes>, CowshedError>;
    pub async fn attach(&self) -> Result<ExecHandle, CowshedError>;
    pub fn kill(&self) -> Result<(), CowshedError>;
}

/// The single capture record CLI, MCP, and CI all consume (11_shell.md).
pub struct ExecRecord {
    pub argv: Vec<String>, pub cwd: PathBuf, pub env_hash: u64,
    pub grant_revision: u64, pub trace: TraceContext,   // traceId/spanId/parentSpanId (13_telemetry.md)
    pub started: SystemTime, pub duration: Duration,
    pub exit: ExitStatus, pub stdout_spool: PathBuf, pub stderr_spool: PathBuf,
    pub truncated: bool,
}
```

### GatewayClient

```rust
pub struct GatewayClient;  // unix-socket control plane
impl GatewayClient {
    pub async fn status(&self) -> Result<GatewayStatus, CowshedError>;
    pub async fn audit_tail(&self, follow: bool) -> Result<impl Stream<Item = AuditEvent>, CowshedError>;
}
```

### Errors

```rust
#[non_exhaustive]
pub enum CowshedError {
    Usage(String),          // CLI exit 2
    NotFound(String),       // 3
    Conflict(String),       // 4
    Environment(String),    // 5
    SandboxDenied(String),  // 6
    Internal(anyhow::Error) // 1
}
```

Every variant carries a `hint()` — the same actionable next step the CLI prints on stderr.

## cowshed-napi (`@smoothbricks/cowshed`)

napi-rs, async (tokio runtime owned by the addon), Promise-returning. Names follow JS conventions; semantics are 1:1
with cowshed-core.

The authority split holds across the NAPI boundary too — `Project` is read-only discovery, `Coordinator` is the only
mutation surface, `WorkspaceHandle` is the non-escalating worker capability:

```ts
export function openProject(path: string): Promise<Project>;

export interface Project {
  // discovery + read-only
  main(): Promise<WorkspaceRef>;
  workspace(name: string): Promise<WorkspaceRef>;
  listWorkspaces(): Promise<WorkspaceInfo[]>;
}

export interface WorkspaceRef {
  // read-only view; no lifecycle/grant mutation
  readonly name: string;
  readonly mountPath: string;
  ensure(): Promise<EnsureReport>;
  grants(): Promise<GrantSet>; // read-only
  exec(argv: string[], opts?: ExecOptions): Promise<ExecResult>;
}
```

Errors are `Error` instances with `code` set to the taxonomy name (`"conflict"`, `"notFound"`, `"sandboxDenied"`, …) and
`hint` when available — mirroring the CLI's JSON envelope, so a caller can share handling between CLI and NAPI use.

The capability split is preserved across the boundary by _how a caller connects_, not by trust in a flag:
`connectCoordinator(controlSocket, coordinatorToken)` yields a `Coordinator` (with the full mutation surface of the Rust
`Coordinator`, including `fork`/`checkpoint`/`restore`/`grant`); `connectWorkspace(workspaceToken)` yields only a
`WorkspaceHandle`. The workspace token is the in-volume `.cowshed/token` a subagent already has; it cannot mint a
coordinator connection.

```ts
export function connectCoordinator(socket: string, token: string): Promise<Coordinator>;
export function connectWorkspace(token: string): Promise<WorkspaceHandle>;

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(argv: string[], opts?: ExecOptions): Promise<ExecResult>;
  background(argv: string[], opts?: ExecOptions): Promise<JobId>;
  jobs(): Promise<JobInfo[]>;
  jobLogs(id: JobId, opts?: { stderr?: boolean; follow?: boolean; signal?: AbortSignal }): AsyncIterable<Uint8Array>;
  checkpoint(label?: string): Promise<string>;
  push(opts?: PushOptions): Promise<PushReport>;
  grants(): Promise<GrantSet>; // read-only
}
```

### NAPI streaming (frozen)

One boundary answer, no ambiguity:

- **Bytes, not lines.** Each stream (`stdout`, `stderr`) is an independent `AsyncIterable<Uint8Array>` carrying raw
  bytes — the transport never assumes UTF-8, so binary output and partial multibyte sequences pass through intact. The
  `onStdout`/`onStderr` **line callbacks** in `ExecOptions` are convenience sugar layered _over_ the byte iterables
  (buffer, split on `\n`, decode) for the common text case; they are never the only access path.
- **stdout and stderr stay separate** end to end (they are separate channels in the framed protocol, 11_shell.md) — no
  interleaving, no merged stream.
- **Backpressure** propagates: a slow consumer of the async iterable pauses the child's output via the socket buffer
  (11_shell.md), so the addon never buffers unboundedly.
- **Cancellation** is an `AbortSignal` on `ExecOptions`/`jobLogs`; aborting stops iteration and, for a foreground exec,
  kills the process tree.
- **Binary stdin** is supported: `ExecOptions.stdin` accepts a `Uint8Array` or an `AsyncIterable<Uint8Array>`.
- **Truncation** surfaces from the `ExecRecord` (`truncated: boolean`) when a spool hit its size cap (11_shell.md) — the
  stream ends cleanly and the flag tells the caller output was cut.

## Tradeoffs

**CLI-as-API for jcode rejected.** Shelling out serializes every call through argv/JSON and loses typed grants, streamed
exec, and the SandboxSpec handoff the supervisor needs. The crate boundary is the API; the CLI exists for processes that
are not Rust and not Node (and for humans).

**Sync API rejected.** Attach/exec/push are IO-bound with real latencies (hundreds of ms); blocking variants would
immediately be wrapped in `spawn_blocking` by every consumer. Async-only keeps one calling convention, and the CLI is a
trivial `#[tokio::main]` wrapper.

**A single all-powerful handle rejected.** An earlier draft returned a full-authority `Workspace` from `Project` and
relied on convention ("please don't call `grant`") — the exact mistake the grants model exists to prevent. Mutation now
lives _only_ on `Coordinator` (obtained with the coordinator token), `Project` returns read-only `WorkspaceRef`s, and
workers get a `WorkspaceHandle` with no escalation methods. A subagent physically cannot widen its own sandbox — the
compiler and the connection factory enforce it, not documentation.
