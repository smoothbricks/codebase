# Programmatic APIs

Two surfaces over one core: the `cowshed-core` Rust crate and `@smoothbricks/cowshed` NAPI bindings (Bun/Node). The CLI
is a thin third client of the same core — anything the CLI can do is assembled from the same capability-scoped APIs,
with identical semantics and error taxonomy.

## Authority model (frozen)

Four handle types, one rule: **a handle reachable from inside a sandbox must not authorize escalation or select another
workspace.** Authority is attached to explicit capabilities, never inferred from a path or workspace-owned record.

- **`Project`** — discovery only. Resolves trusted repository identity and enumerates names into read-only
  `WorkspaceRef` values. It has no attach, exec, lifecycle, maintenance, repository-mirror, or policy mutation methods.
- **`WorkspaceRef`** — inspection plus safe attachment for one workspace. It exposes identity, mount path, state,
  grants, `ensure`, and `attach`; it cannot detach, exec, open a shell, mutate lifecycle, or mint a worker capability.
- **`WorkspaceHandle`** — a worker's non-escalating capability over exactly one workspace: exec, shell, job control,
  checkpoint subject to controller-configured checkpoint quotas, push, and read-only grant inspection. It has no
  grant/revoke, restore/destroy/rebase/land/gc, repo-mirror, or cross-workspace access.
- **`Coordinator`** — the sole project mutation and cross-workspace authority: adopt/create/fork, grant/revoke,
  restore/destroy/rebase/land, garbage collection, repository mirroring, slot assignment, checkpoint-quota policy, and
  minting one-workspace handles. A trusted unsandboxed controller holds it; workers never do.

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

/// Discovery-only entry point. Holds no attachment, execution, maintenance, or mutation authority.
pub struct Project { /* repo_id, chosen remote binding, git root, cowshed dirs — cheap, Clone */ }
impl Project {
    pub async fn main(&self) -> Result<WorkspaceRef, CowshedError>;
    pub async fn workspace(&self, name: &str) -> Result<WorkspaceRef, CowshedError>;
    pub async fn list(&self) -> Result<Vec<WorkspaceRef>, CowshedError>;
}

/// Read-only view of one workspace: identity, mount state, grant inspection, and safe ensure/attach.
/// Carries no execution, detach, lifecycle, maintenance, repository-mirror, or grant mutation authority.
pub struct WorkspaceRef { /* name, image path, detached controller metadata snapshot */ }
impl WorkspaceRef {
    pub fn name(&self) -> &str;
    pub fn mount_path(&self) -> &Path;               // canonical, whether or not attached
    pub async fn info(&self) -> Result<WorkspaceInfo, CowshedError>;
    pub async fn ensure(&self) -> Result<EnsureReport, CowshedError>;
    pub async fn attach(&self, opts: AttachOptions) -> Result<(), CowshedError>;
    pub async fn grants(&self) -> Result<GrantSet, CowshedError>;       // read-only
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
    pub stdin: StdinSource,
    pub stdout: Stdio, pub stderr: Stdio,
}

/// Binary stdin without shell interpolation. N-API projects Inline as Uint8Array/Buffer,
/// Stream as a backpressured readable, and WorkspaceFile as a relative path object.
pub enum StdinSource {
    Empty,
    Inline(Bytes),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
    WorkspaceFile(PathBuf),
}

pub struct StdinInfo {
    pub kind: StdinKind,                 // empty | inline | stream | workspace-file
    pub bytes: u64,                      // bytes successfully delivered before EOF/cancellation
    pub workspace_path: Option<PathBuf>, // normalized relative path; never host-absolute
    pub complete: bool,                  // true only after clean source EOF reached child stdin
}

pub struct TraceContext { pub trace_id: [u8; 16], pub span_id: [u8; 8] }  // adopted or minted per request

/// Positive, workspace-local monotonic identity allocated for every exec submission.
/// The allocator never reuses a value. Values are capped at 2^53-1 so the same integer is exact
/// in Rust, JSON, and N-API/JavaScript `number`.
#[serde(transparent)]
pub struct JobId(pub u64);

pub enum JobState { Queued, Running, Exited, Signaled, Killed, OutputLimit, Failed }

/// Deterministic, bounded, redacted text projection of a raw stream.
pub struct OutputSummary {
    pub version: u16,
    pub text: String,
    pub truncated: bool,                 // the summary omitted source bytes; not spool truncation
}

pub struct StreamInfo {
    pub path: PathBuf,                   // `.cowshed/job/<id>/out` or `err`, relative to mount
    pub bytes: u64,
    pub summary: OutputSummary,
}

/// The single lifecycle/result DTO reused by core, CLI JSON, N-API, MCP, and Arrow projections.
pub struct JobInfo {
    pub repo_id: String,
    pub workspace_incarnation: [u8; 16],
    pub id: JobId,
    pub state: JobState,
    pub pid: Option<u32>,
    pub grant_revision: u64,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub started: SystemTime,
    pub duration: Option<Duration>,
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub trace: TraceContext,
    pub output_limit: Option<OutputLimitInfo>, // present for the explicit OutputLimit terminal state
    pub stdin: StdinInfo,
}

pub struct OutputLimitInfo {
    pub limit_bytes: u64,                   // configured combined stdout+stderr quota; default 1 GiB
    pub crossing_bytes: u64,                // persisted plus bytes already read and in flight at the crossing
}

/// Control handle for one durable job record; dropping it never kills or deletes the job.
pub struct JobHandle { /* workspace capability + JobId */ }
impl JobHandle {
    pub fn id(&self) -> JobId;
    pub async fn status(&self) -> Result<JobInfo, CowshedError>;
    pub async fn logs(&self, stream: Stream, follow: bool)
        -> Result<impl futures::Stream<Item = Bytes>, CowshedError>; // raw bytes from backing file
    pub async fn attach(&self) -> Result<JobAttachment, CowshedError>;
    pub async fn detach(&self) -> Result<(), CowshedError>;          // job continues
    pub async fn wait(&self) -> Result<JobInfo, CowshedError>;
    pub fn kill(&self) -> Result<(), CowshedError>;                  // complete process tree
}

/// A live attachment is only a view over one durable job's raw backing streams.
pub struct JobAttachment { /* stdin sink + independent stdout/stderr byte streams */ }
impl JobAttachment {
    pub async fn detach(self) -> Result<(), CowshedError>; // closes the client view; job continues
}

pub struct GrantSet {
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port_block: Option<PortBlock>,   // macOS: Some({ base, size }); Linux: None and omitted from JSON/N-API,
                                         // never a zero-sized or otherwise sentinel block
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

This is the controller integration point: a trusted orchestrator holds a `Coordinator`, hands each worker a
`WorkspaceHandle`, calls `Coordinator::grant` as policy allows, and either lets cowshed spawn (`exec`) or takes a
controller-side `SandboxSpec` while launching the supervisor. A worker never receives the latter launch authority.

## Capability split: `Coordinator` vs `WorkspaceHandle`

Mutation authority lives only on `Coordinator`; `WorkspaceHandle` is the non-escalating worker capability. Together they
mirror the MCP token model (12_mcp.md) in the type system:

```rust
/// The sole mutation and cross-workspace authority over a project. It is the only capability that can adopt, create,
/// destroy, fork, grant, revoke, restore, rebase, land, collect garbage, mirror repositories, set checkpoint quotas,
/// or assign slots.
pub struct Coordinator { /* Project + controller identity */ }
impl Coordinator {
    pub async fn adopt(&self, opts: AdoptOptions) -> Result<WorkspaceRef, CowshedError>;
    pub async fn create(&self, name: &str, opts: CreateOptions) -> Result<WorkspaceRef, CowshedError>;
    pub async fn fork(&self, src: &str, dst: &str) -> Result<WorkspaceRef, CowshedError>;
    pub async fn grant(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    pub async fn revoke(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    pub async fn rebase(&self, ws: &str, opts: RebaseOptions) -> Result<Oid, CowshedError>;
    pub async fn land(&self, ws: &str, opts: LandOptions) -> Result<Oid, CowshedError>;
    pub async fn restore(&self, ws: &str, label: &str) -> Result<(), CowshedError>;
    pub async fn assign_slot(&self, ws: &str, slot: u32) -> Result<(), CowshedError>;
    pub async fn destroy(&self, ws: &str, opts: RemoveOptions) -> Result<(), CowshedError>;
    pub async fn gc(&self, opts: GcOptions) -> Result<GcReport, CowshedError>;
    pub async fn repo_mirror(&self, ws: &str, url: &Url) -> Result<MirrorInfo, CowshedError>;
    pub async fn set_checkpoint_quota(&self, ws: &str, quota: CheckpointQuota) -> Result<(), CowshedError>;
    /// Hand a worker a capability scoped to exactly one workspace.
    pub fn worker(&self, ws: &str) -> Result<WorkspaceHandle, CowshedError>;
}

/// A worker's capability: it can run and observe work in *its* workspace and hand results
/// back, but it can never widen its own sandbox or touch another workspace. Escalation is
/// the coordinator's job. This is the type a subagent receives; it carries no grant mutation.
pub struct WorkspaceHandle { /* exactly one workspace, non-escalating worker capability */ }
impl WorkspaceHandle {
    pub fn name(&self) -> &str;
    pub fn mount_path(&self) -> &Path;
    pub async fn exec(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;
    pub async fn shell(&self, session: Option<&str>) -> Result<Session, CowshedError>;
    pub async fn list_jobs(&self) -> Result<Vec<JobInfo>, CowshedError>;
    pub async fn job(&self, id: JobId) -> Result<JobHandle, CowshedError>;
    pub async fn checkpoint(&self, label: Option<&str>) -> Result<String, CowshedError>; // quota-enforced atomically
    pub async fn push(&self, opts: PushOptions) -> Result<PushReport, CowshedError>;
    pub async fn grants(&self) -> Result<GrantSet, CowshedError>;   // read-only: observe, never mutate
    // No grant/revoke, restore/destroy/rebase/land/gc, repo mirror, detach, or cross-workspace access.
}
```

`WorkspaceHandle::checkpoint` is intentionally available to a worker for retry points, but it is not unbounded storage
authority. Admission atomically enforces the coordinator-configured per-workspace checkpoint count and byte quotas;
quota exhaustion is `CowshedError::Conflict` with the current usage and limit, and never triggers implicit pruning or a
coordinator operation. Restore remains coordinator-only.

`JobInfo.state = OutputLimit` is the explicit result when the configurable combined stdout+stderr quota (default 1 GiB)
is crossed. Accounting includes persisted and in-flight bytes; the supervisor terminates the process group, observes the
grace, kills stragglers, drains both pipes, and only then publishes the terminal DTO. No API silently truncates and
continues the job. Summaries remain a separate bounded diagnostic projection.

The shell connection is a reconnectable view over the persistent, permission-checked, multi-client per-workspace
supervisor socket (11_shell.md). Dropping a `Session`, `JobHandle`, or transport never unlinks that socket or stops the
job. Authoritative status comes from controller-owned immutable per-writer telemetry segments delivered through the
controller-provided non-inheritable writer capability/FD or IPC channel; workspace-local `records.arrow` is never
trusted for status, authorization, denial, quota, or audit decisions.

The invariant is the same one that keeps grant files outside the volume (01_storage.md, 04): a capability reachable from
inside a sandbox must not authorize escalation. `WorkspaceHandle` is that principle expressed in the type system — a
subagent holding one cannot grant itself anything.

## DTO freeze (single source of truth)

These types are defined **once** in `cowshed-core` and reused verbatim by the CLI (`--json` bodies), NAPI, and MCP — no
adapter redefines a field, and the contract goldens (08_testing.md) pin their shapes: `WorkspaceInfo`, `EnsureReport`,
`GcReport`, `Finding`, `JobId`, `JobState`, `JobInfo`, `StreamInfo`, `OutputSummary`, `ExecRecord`, `PushReport`,
`GrantSet`/`GrantDelta`/`PortBlock`/`EgressRule`/`RepoRule`/`SimVerb`, `GatewayStatus`, `AuditEvent`, and every
`*Options` type (`AdoptOptions`, `CreateOptions`, `AttachOptions`, `RemoveOptions`, `RebaseOptions`, `LandOptions`,
`PushOptions`). Field sketches elsewhere in this spec are illustrative; the freeze rule — one definition, reused,
versioned together — is the contract. `GrantSet.port_block` is the platform union: macOS always carries a real
`PortBlock`, while Linux carries `None` and its JSON/N-API projection omits `portBlock`. Adapters and consumers must use
that optional shape directly; casts, `null`, zero-sized blocks, and sentinel base values are forbidden. Adding a field
is a coordinated change across core + goldens, not a per-adapter patch. JSON and N-API use the same camel-case
projection: `JobInfo.stdout` and `JobInfo.stderr` are `StreamInfo` objects, each with `path`, `bytes`, and `summary`;
each `summary` has `version`, `text`, and `truncated`. The paths name the in-workspace raw byte files containing every
byte admitted before any output-limit trip. There are no flattened aliases or adapter-specific spellings.

## Shell client (`cowshed-shell`)

Types for the warm-shell layer (11_shell.md); `WorkspaceHandle::shell`/`list_jobs` return these.

```rust
pub struct Session { /* supervisor connection, named or anonymous */ }
impl Session {
    pub async fn run(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;  // allocates a JobId
    pub async fn background(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;
    pub fn is_named(&self) -> bool;
    pub async fn close(self) -> Result<(), CowshedError>;   // named: persist; anonymous: return to pool
}

/// The single capture record CLI, MCP, and CI all consume (11_shell.md).
pub struct ExecRecord {
    pub repo_id: String,
    pub workspace_incarnation: [u8; 16],
    pub job_id: JobId,
    pub argv: Vec<String>, pub cwd: PathBuf, pub env_hash: u64,
    pub grant_revision: u64,
    pub trace: TraceContext,              // standard trace_id/span_id; job_id is a separate join key
    pub started: SystemTime, pub duration: Duration,
    pub exit: ExitStatus,
    pub stdout: StreamInfo, pub stderr: StreamInfo,
    pub stdin: StdinInfo,
}
```

Every foreground and background submission is the same durable job: attachment is a client state, not a second exec
kind. The allocator commits `JobId` before spawn, so even a pre-spawn failure has a queryable terminal `JobInfo`. Raw
stdout and stderr are always captured separately at `.cowshed/job/<id>/out` and `.cowshed/job/<id>/err`; streaming reads
those files (and follows them while running) rather than creating a competing capture path. Summaries are deterministic,
size-bounded, versioned, and redacted. They may be copied into control results and Arrow records for orientation, but
must never determine denial, exit, policy, or build success; those decisions use authoritative status and evidence.

Arrow records carry `job_id` alongside the standard `trace_id`, `span_id`, and parent linkage. `job_id` joins a job to
its trace; it is never packed into, substituted for, or derived from `span_id`.

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
  // inspection + safe attachment only; no exec/detach/lifecycle/grant mutation
  readonly name: string;
  readonly mountPath: string;
  info(): Promise<WorkspaceInfo>;
  ensure(): Promise<EnsureReport>;
  attach(opts?: AttachOptions): Promise<void>;
  grants(): Promise<GrantSet>; // read-only
}
```

Errors are `Error` instances with `code` set to the taxonomy name (`"conflict"`, `"notFound"`, `"sandboxDenied"`, …) and
`hint` when available — mirroring the CLI's JSON envelope, so a caller can share handling between CLI and NAPI use.

The capability split is preserved across the boundary by _how a caller connects, not by a caller-supplied authority
string_: `connectCoordinator` accepts only the opaque receiving endpoint for the controller-created inherited
FD/socketpair and consumes/closes it after binding; coordinator authority never enters JavaScript as token text.
`connectWorkspace(workerDescriptor)` consumes a distinct 256-bit, one-use, 30-second-TTL descriptor minted for exactly
one workspace. The in-volume gateway token is not an N-API or MCP credential.

```ts
export interface CoordinatorEndpoint {
  readonly __opaqueCoordinatorEndpoint: unique symbol;
}
export interface WorkerDescriptor {
  readonly __opaqueWorkerDescriptor: unique symbol;
}
export function connectCoordinator(endpoint: CoordinatorEndpoint): Promise<Coordinator>;
export function connectWorkspace(descriptor: WorkerDescriptor): Promise<WorkspaceHandle>;

/** Positive exact integer, local to one workspace and never reused. */
export type JobId = number;
export type JobStream = 'stdout' | 'stderr';

export interface OutputSummary {
  version: number;
  text: string;
  truncated: boolean;
}

export interface StreamInfo {
  path: string;
  bytes: number;
  summary: OutputSummary;
}

export interface JobInfo {
  repoId: string;
  workspaceIncarnation: string;
  jobId: JobId;
  state: 'queued' | 'running' | 'exited' | 'signaled' | 'killed' | 'outputLimit' | 'failed';
  pid?: number;
  grantRevision: number;
  argv: string[];
  cwd: string;
  started: Date;
  durationMs?: number;
  exit?: ExitStatus;
  stdout: StreamInfo;
  stderr: StreamInfo;
  trace: TraceContext;
  stdin: StdinInfo;
  outputLimit?: { limitBytes: number; crossingBytes: number };
}

export interface JobHandle {
  readonly jobId: JobId;
  status(): Promise<JobInfo>;
  logs(stream: JobStream, opts?: { follow?: boolean; signal?: AbortSignal }): AsyncIterable<Uint8Array>;
  attach(opts?: { signal?: AbortSignal }): Promise<JobAttachment>;
  detach(): Promise<void>;
  kill(): Promise<void>;
  wait(opts?: { signal?: AbortSignal }): Promise<JobInfo>;
}

export interface JobAttachment {
  readonly stdout: AsyncIterable<Uint8Array>;
  readonly stderr: AsyncIterable<Uint8Array>;
  write(chunk: Uint8Array): Promise<void>;
  detach(): Promise<void>; // closes this view; the job continues
}

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(argv: string[], opts?: ExecOptions): Promise<JobHandle>;
  background(argv: string[], opts?: ExecOptions): Promise<JobHandle>;
  listJobs(): Promise<JobInfo[]>;
  job(id: JobId): Promise<JobHandle>;
  checkpoint(label?: string): Promise<string>;
  push(opts?: PushOptions): Promise<PushReport>;
  grants(): Promise<GrantSet>; // read-only
}
```

### NAPI streaming (frozen)

One boundary answer, no ambiguity:

- **Backing files are canonical.** Every exec immediately returns a `JobHandle` with a numeric `jobId`; foreground and
  background differ only in attachment. Full raw stdout and stderr are independently captured in `.cowshed/job/<id>/out`
  and `.cowshed/job/<id>/err`. `JobHandle.logs` and an attachment's stream iterables expose those bytes; `JobInfo`
  exposes stream paths, lengths, and summaries.
- **JSON is control-only.** A JSON serialization of `JobInfo` carries lifecycle/result metadata and deterministic,
  bounded, versioned, redacted `stdout.summary` and `stderr.summary` objects; it never contains raw bytes, base64, byte
  arrays, or a decoded substitute for the backing files. The same summaries appear in Arrow records, and never drive
  denial, exit, policy, or build-success decisions.
- **Bytes, not lines.** Each stream (`stdout`, `stderr`) is an independent `AsyncIterable<Uint8Array>` carrying raw
  bytes — the transport never assumes UTF-8, so binary output and partial multibyte sequences pass through intact. The
  `onStdout`/`onStderr` **line callbacks** in `ExecOptions` are convenience sugar layered _over_ the byte iterables
  (buffer, split on `\n`, decode) for the common text case; they are never the only access path.
- **Cancellation** is an `AbortSignal` on `ExecOptions`, attachments, and `JobHandle.logs`/`wait`; aborting stops the
  client operation. It does not kill the durable job; callers invoke `JobHandle.kill()` explicitly.
- **Binary stdin** is supported without shell interpolation: `ExecOptions.stdin` accepts the `StdinSource` union of
  inline `Uint8Array`, backpressured `AsyncIterable<Uint8Array>`, or a workspace-relative file object. File sources use
  the same canonical no-follow regular-file open as core/MCP/CLI.
- **Stdin lifecycle is observable.** Open occurs after `JobId` allocation; EOF closes child stdin once, cancellation
  closes stdin and records incomplete delivery without implicitly killing the job, and events carry only kind, delivered
  bytes, completion, and optional normalized relative path.

**Structured stdin safety.** `WorkspaceFile` must be relative, canonicalize beneath the workspace mount, and be opened
read-only with no-follow traversal (`openat`-style component walk); symlinks, devices, sockets, directories, escapes,
and post-check replacement fail closed. Inline bytes and streams are opaque binary data. Bounded buffers propagate child
backpressure. Source-open failures remain terminal jobs with trace/job identity because allocation precedes open.

## Tradeoffs

**CLI-as-API rejected.** Shelling out serializes every call through argv/JSON and loses typed grants, streamed exec, and
the capability handoff the supervisor needs. The crate boundary is the API; the CLI exists for processes that are not
Rust and not Node (and for humans).

**Sync API rejected.** Attach/exec/push are IO-bound with real latencies (hundreds of ms); blocking variants would
immediately be wrapped in `spawn_blocking` by every consumer. Async-only keeps one calling convention, and the CLI is a
trivial `#[tokio::main]` wrapper.

**A single all-powerful handle rejected.** An earlier draft returned a full-authority `Workspace` from `Project` and
relied on convention ("please don't call `grant`") — the exact mistake the grants model exists to prevent. Mutation now
lives _only_ on `Coordinator` (obtained with the coordinator token), `Project` returns read-only `WorkspaceRef`s, and
workers get a `WorkspaceHandle` with no escalation methods. A subagent physically cannot widen its own sandbox — the
compiler and the connection factory enforce it, not documentation.

Structured stdin is opened only after `JobId` allocation, so source-open failures are terminal jobs with trace and job
identity. `WorkspaceFile` must be relative, canonicalize beneath the workspace mount, and be opened read-only with
no-follow traversal (`openat`-style component walk); symlinks, devices, sockets, directories, escapes, and post-check
replacement fail closed. Inline bytes and streams are treated as opaque binary data, never shell text. Delivery uses the
framed stdin channel with bounded buffers and child-pipe backpressure. Source EOF closes child stdin exactly once;
client cancellation closes the source and child stdin, records incomplete delivery, and does not implicitly kill the job
unless the caller separately requests cancellation of the job. Job/trace events carry stdin kind, delivered bytes,
completion, and normalized workspace-relative source where applicable, never inline contents.
