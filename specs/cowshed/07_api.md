# Programmatic APIs

Two surfaces over one core: the `cowshed-core` Rust crate and `@smoothbricks/cowshed` NAPI bindings (Bun/Node). The CLI
is a thin third client of the same core — anything the CLI can do is assembled from the same capability-scoped APIs,
with identical semantics and error taxonomy.

> **Implementation status — monitoring and generation:** core jobs expose numeric lookup, leader pid, start and terminal
> duration, protected per-stream output, offset reads, bounded cursor tails, attach resumed at a journal cursor, detach,
> and complete-group termination. Core job resource samples and terminal persistence are implemented; periodic progress
> streams and keyed admission/lookup are not yet complete. Controller request/result codecs, TypeScript types and
> validators, and N-API operation bindings derive from one Rust API declaration. The addon exposes the declared offset
> log reads, the bounded `job.tail` operation and `job.listeningPorts`; async stream backpressure, attachment stdin EOF,
> and abort plumbing remain separate implementation work. Core attachment stdin writes exist, but `JobStdin` has no
> close operation on main yet. Complete fork/exec process-tree observation, per-process CPU/RSS/I/O and blocker facts,
> process event streams, leaf-work identity, and the compact process/job spans in 13_telemetry.md are also unbuilt. The
> ownership ledger identifies groups for safe termination; it does not yet provide these observations. Per-job cgroup-v2
> accounting, measured Linux fork/exec event-source selection, macOS exit observation and rusage reconciliation,
> charged-memory counters, and explicit unattributed-usage rows are also unbuilt.

## Authority model (frozen)

Four handle types, one rule: **a handle reachable from inside a sandbox must not authorize escalation or select another
workspace.** Authority is attached to explicit capabilities, never inferred from a path or workspace-owned record.

- **`Project`** — discovery only. Resolves trusted repository identity and enumerates names into read-only
  `WorkspaceRef` values. It has no attach, exec, lifecycle, maintenance, repository-mirror, or policy mutation methods.
- **`WorkspaceRef`** — inspection plus safe attachment for one workspace. It exposes identity, mount path, state,
  grants, and `attach`; it cannot detach, exec, open a shell, mutate lifecycle, or mint a worker capability.
- **`WorkspaceHandle`** — a worker's non-escalating capability over exactly one workspace: exec, shell, job control,
  checkpoint subject to controller-configured checkpoint quotas, push, and read-only grant inspection. It has no
  grant/revoke, restore/destroy/rebase/land/gc, repo-mirror, or cross-workspace access.
- **`Coordinator`** — the sole project mutation, diagnostics, and cross-workspace authority: adopt/create/fork,
  grant/revoke, restore/destroy/rebase/land, garbage collection, repository mirroring, doctor, slot assignment,
  checkpoint-quota policy, and minting one-workspace handles. A trusted unsandboxed controller holds it; workers never
  do.

`CoordinatorToken` is an affine, opaque, non-serializable proof acquired only by consuming a controller-provided
inherited Unix stream descriptor. Acquisition validates that the descriptor is a connected socket owned by the current
uid, performs the controller's one-use nonce handshake, and binds the resulting token to the returned controller actor
channel and one primary `repoId`. It is neither `Clone` nor `Copy`, has no public constructor or byte/string projection,
and is consumed by `Cowshed::coordinator`. Environment text may identify the inherited descriptor number but is never
itself authority. Missing, reused, wrong-peer, malformed, or cross-project descriptors return a typed `CowshedError`;
there is no fallback token, ambient singleton, filesystem token, or workspace-readable credential.

All capability operations are messages to a single-owner controller/supervisor actor. Handles may clone an immutable
sender, but they never share mutable authority state behind a mutex. `WorkspaceRef::attach` is deliberately safe,
idempotent one-workspace attachment; only `Coordinator::detach` may detach because detachment affects running jobs and
controller fencing.

Every `worker.*`, `job.*`, and `session.*` request is fenced by the immutable
`{ repoId, workspace, workspaceIncarnation }` captured when `Coordinator::worker` mints the handle. `JobHandle`,
`JobStdin`, `JobAttachment`, log polling, and `Session` carry that same typed fence; callers never reconstruct it from
strings or paths. Recreating a workspace does not retarget an old handle: the old handle continues to send its original
incarnation and the controller rejects it as stale before effects.

## cowshed-core (Rust)

Design rules: async (tokio), no global state, no interior config lookup — everything reachable from an explicit handle;
all filesystem/mount state derived per call (01_storage.md).

```rust
/// Explicit client for one authenticated, single-owner controller actor.
pub struct Cowshed { /* sealed actor sender */ }
impl Cowshed {
    /// Consume an inherited, peer-verified controller socket, perform the nonce handshake, and return the client plus
    /// its affine coordinator token. Environment text may identify the fd number but is not authority.
    pub async fn connect(fd: OwnedFd) -> Result<(Cowshed, CoordinatorToken), CowshedError>;
    /// Resolve a project from any path inside the repository through that actor.
    pub async fn open(&self, path: impl AsRef<Path>) -> Result<Project, CowshedError>;
    /// Bind and consume coordinator authority over a project resolved by the same actor.
    pub fn coordinator(&self, project: &Project, token: CoordinatorToken)
        -> Result<Coordinator, CowshedError>;
}

/// Discovery-only entry point. Holds no attachment, execution, maintenance, or mutation authority.
pub struct Project { /* repo_id, chosen remote binding, git root, cowshed dirs — cheap, Clone */ }
impl Project {
    pub async fn main(&self) -> Result<WorkspaceRef, CowshedError>;
    pub async fn workspace(&self, name: &str) -> Result<WorkspaceRef, CowshedError>;
    pub async fn list(&self) -> Result<Vec<WorkspaceRef>, CowshedError>;
    /// Resolve only through authoritative metadata plus an exact active-mount containment match.
    pub async fn workspace_at(&self, path: impl AsRef<Path>) -> Result<WorkspaceRef, CowshedError>;
}
```

`Project::workspace_at` is a coordinator-channel RPC and a host seam, not local marker discovery. Each call re-reads
storage, exact detached metadata, and kernel mount facts, canonicalizes the input, and succeeds only when exactly one
active mount owned by this project contains it. Nested cwd paths resolve; unmounted, detached, ambiguous, cross-project,
inaccessible, and marker-only paths return a typed error without granting a workspace capability.

```rust
/// Read-only view of one workspace: an immutable information/grant snapshot plus safe attach.
/// Carries no execution, detach, lifecycle, maintenance, repository-mirror, or grant mutation authority.
pub struct WorkspaceRef { /* detached WorkspaceInfo + GrantSet snapshot and sealed actor sender */ }
impl WorkspaceRef {
    pub fn name(&self) -> &WorkspaceName;
    pub fn mount_path(&self) -> &Path;                // canonical, whether or not attached
    pub fn info(&self) -> &WorkspaceInfo;             // captured snapshot; no RPC or copy
    pub fn grants(&self) -> &GrantSet;                // captured snapshot; no RPC or copy
    pub fn snapshot(&self) -> (&WorkspaceInfo, &GrantSet);
    pub fn into_info(self) -> WorkspaceInfo;          // consuming, copy-free list projection
    pub fn into_snapshot(self) -> (WorkspaceInfo, GrantSet);
    pub async fn refresh_info(&self) -> Result<WorkspaceInfo, CowshedError>;
    pub async fn refresh_grants(&self) -> Result<GrantSet, CowshedError>;
    /// The volume a job of this incarnation would be granted now (16_build_volumes.md); `None` when
    /// the checkout links none. Coordinator-only; refuses a detached workspace and a recreated name.
    pub async fn build_volume(&self) -> Result<Option<PathBuf>, CowshedError>;
    pub async fn attach(&self, opts: AttachOptions) -> Result<(), CowshedError>;
}
```

### Exec and grants

```rust
pub enum RunSandboxMode { ReadWrite, ReadOnly }
pub const MAX_COMMAND_ARG_BYTES: usize = 128 * 1024;
pub const MAX_ARGV_BYTES: usize = 1024 * 1024;

/// Immutable OS-native argument. From<OsString>/From<String>/From<&str> preserve ownership;
/// as_os_str borrows and into_os_string consumes without a lossy text conversion.
pub struct CommandArg(OsString);


pub struct ExecRequest {
    pub command: ExecCommand,
    pub cwd: Option<PathBuf>,            // relative to mount; default mount root
    pub mode: RunSandboxMode,            // default ReadWrite
    pub env: HashMap<String, String>,    // filtered through the build-config allowlist
    pub trace: Option<TraceContext>,     // W3C context; propagated into the job env as TRACEPARENT (13_telemetry.md)
    pub stdin: StdinSource,
    pub stdout_copy: Option<OutputPublication>,
    pub stderr_copy: Option<OutputPublication>,
    pub admission_key: Option<String>,  // idempotent admission within the exact workspace incarnation
}

pub enum ExecCommand {
    Argv(Vec<CommandArg>),               // byte-exact, executed directly
    Script(ScriptCommand),               // bash-compatible text, run by the warm host (11_shell.md)
}

/// parts.len() == values.len() + 1; no NUL anywhere; parts and values together at most MAX_ARGV_BYTES.
/// ScriptCommand::new validates, and so does deserialization; the fields are private.
pub struct ScriptCommand { parts: Vec<String>, values: Vec<ScriptValue> }
pub enum ScriptValue {
    Word(String),                        // one word, or one piece of a word
    Words(Vec<String>),                  // one word per element as a whole bare word; else joined by spaces
}
pub enum JobFailure {
    ScriptSyntax,                        // the script did not parse; nothing ran
    SupervisorLost,                      // its supervisor ended without seeing it end (11_shell.md)
}

pub struct OutputPublication {
    pub path: WorkspacePath,             // writable caller-visible destination, never artifact authority
    pub policy: PublicationPolicy,       // CreateNew | Replace
}
pub enum PublicationPolicy { CreateNew, Replace }

/// Binary stdin without shell interpolation. N-API projects Inline as Uint8Array/Buffer,
/// Stream as a backpressured readable, and WorkspaceFile as a relative path object.
pub enum StdinSource {
    Empty,
    Inline(Bytes),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
    WorkspaceFile(WorkspacePath),
}

pub struct StdinInfo {
    pub kind: StdinKind,                 // empty | inline | stream | workspace-file
    pub bytes: u64,                      // bytes successfully delivered before EOF/cancellation
    pub workspace_path: Option<WorkspacePath>, // normalized relative path; never host-absolute
    pub complete: bool,                  // true only after clean source EOF reached child stdin
}

pub struct TraceContext { pub trace_id: TraceId, pub span_id: SpanId } // validated non-zero lowercase hex

/// Positive, workspace-local monotonic identity allocated for every exec submission.
/// The allocator never reuses a value. Values are capped at 2^53-1 so the same integer is exact
/// in Rust, JSON, and N-API/JavaScript `number`.
pub struct JobId(u64); // constructed with JobId::new; 1..=2^53-1

pub enum JobState { Queued, Running, Exited, Signaled, Killed, OutputLimit, Failed }

#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ExitStatus {
    Exited { code: i32 },
    Signaled { signal: i32, core_dumped: bool },
}

/// Deterministic, bounded, redacted text projection of a raw stream.
pub struct OutputSummary {
    pub version: u16,
    pub text: String,
    pub truncated: bool,                 // the summary omitted source bytes; not artifact truncation
}

/// Opaque bytes bounded by MAX_INLINE_OUTPUT_BYTES. Arrow uses Binary. JSON/N-API uses the exact
/// wire union `{encoding:"utf8",data:String}|{encoding:"base64",data:String}` and bounds decoded bytes.
/// Serialization chooses utf8 iff the bytes are valid UTF-8; decoding is lossless in both branches.
pub struct BinaryData(Bytes);

#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ProtectedOutput {
    Inline { data: BinaryData },
    File { path: WorkspacePath },
}

#[serde(tag = "kind", rename_all = "camelCase")]
pub enum OutputStorage {
    Captured { artifact: ProtectedOutput },
    Redirect {
        source: WorkspacePath,           // mutable live shell destination; never authority
        artifact: ProtectedOutput,       // independent sealed canonical bytes
    },
}

pub struct StreamInfo {
    pub storage: OutputStorage,
    pub bytes: u64,
    pub sha256: Sha256Digest,
    pub summary: OutputSummary,
}

/// Protected in-volume record schema. `version` is carried by the selected variant as `record_version` in Arrow.
pub struct JobArtifactRecord {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub sequence: u64,
    pub state: JobState,
    pub grant_revision: u64,
    pub command: ExecCommand,
    pub output_limit: Option<OutputLimitInfo>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub failure: Option<JobFailure>,
    pub exit: Option<ExitStatus>,           // terminal records whose end was observed
    pub duration_ms: Option<u64>,           // terminal records whose end was observed
}
#[serde(rename_all = "kebab-case")]
pub enum VisibleStorageKind { CapturedInline, CapturedFile, RedirectInline, RedirectFile }
pub struct VisibleStreamCommitment {
    pub storage_kind: VisibleStorageKind,
    pub bytes: u64,
    pub sha256: Sha256Digest,
    pub protected_path: Option<WorkspacePath>, // present exactly for *File
}
pub struct VisibleJobCommitment {
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub stdout: VisibleStreamCommitment,
    pub stderr: VisibleStreamCommitment,
}
pub struct CheckpointManifestRecord {
    pub version: u16,
    pub repo_id: RepoId,
    pub origin_incarnation: WorkspaceIncarnation,
    pub barrier_id: u64,
    pub visible_jobs: Vec<VisibleJobCommitment>,
    pub records_sha256: Sha256Digest,
}
pub enum ProtectedRecord {
    Job(JobArtifactRecord),
    CheckpointManifest(CheckpointManifestRecord),
}

/// Compact controller audit schema — telemetry, never read for a decision. Every record flattens identity and carries
/// `version` + `order` (the writing controller's own sequence).
pub struct AdmissionCommitment {
    pub version: u16, pub order: u64, pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation, pub job_id: JobId, pub grant_revision: u64,
}
pub struct TerminalCommitment {
    pub version: u16, pub order: u64, pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation, pub job_id: JobId,
    pub state: JobState, pub grant_revision: u64,
    pub stdout_bytes: u64, pub stdout_sha256: Sha256Digest,
    pub stderr_bytes: u64, pub stderr_sha256: Sha256Digest,
    pub batch_sha256: Sha256Digest,
}

pub struct CheckpointCommitment {
    pub version: u16, pub order: u64, pub repo_id: RepoId,
    pub origin_incarnation: WorkspaceIncarnation, pub checkpoint_id: String,
    pub barrier_id: u64, pub manifest_batch_sha256: Sha256Digest,
}
pub struct ForkCommitment {
    pub version: u16, pub order: u64, pub repo_id: RepoId,
    pub source_incarnation: WorkspaceIncarnation, pub destination_incarnation: WorkspaceIncarnation,
}
pub struct RestoreCommitment {
    pub version: u16, pub order: u64, pub repo_id: RepoId, pub source_checkpoint: String,
    pub source_incarnation: WorkspaceIncarnation, pub destination_incarnation: WorkspaceIncarnation,
}
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ControllerCommitment {
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Checkpoint(CheckpointCommitment),
    Fork(ForkCommitment),
    Restore(RestoreCommitment),
}

For queued/running `JobInfo`, `StreamInfo` is a current bounded view; its artifact becomes authoritative only when the
corresponding protected batch/file is complete and sealed. Background acknowledgement and checkpointing force any
memory-only prefix to `File`. Terminal state freezes `storage`, `bytes`, and `sha256`.

/// The single lifecycle/result DTO reused by core, CLI JSON, N-API, MCP, and Arrow projections.
pub struct JobInfo {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub pid: Option<u32>,
    pub grant_revision: u64,
    pub command: ExecCommand,            // flattened as exactly one of argv or script on the wire
    pub cwd: Option<WorkspacePath>,        // None is the workspace mount root
    pub started: UtcTimestamp,
    pub duration_ms: Option<u64>,
    pub resources: Option<JobResourceSample>, // absent before spawn, never a fabricated zero sample
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub trace: TraceContext,
    pub output_limit: Option<OutputLimitInfo>, // present exactly for OutputLimit
    pub stdin: StdinInfo,
    pub failure: Option<JobFailure>,         // a failed job that failed before its command ran
}

pub struct OutputLimitInfo {
    pub limit_bytes: u64,                   // configured combined stdout+stderr quota; default 1 GiB
    pub crossing_bytes: u64,                // persisted plus bytes already read and in flight at the crossing
}

/// Control handle for one durable job record; dropping it never kills or deletes the job.
pub struct JobHandle { /* immutable repo/workspace/incarnation fence + JobId + actor sender */ }
impl JobHandle {
    pub fn id(&self) -> JobId;
    pub async fn status(&self) -> Result<JobInfo, CowshedError>;
    pub async fn resources(&self) -> Result<JobResourceSample, CowshedError>;
    pub async fn listening_ports(&self) -> Result<JobListeningPorts, CowshedError>;
    pub async fn processes(&self) -> Result<JobProcessTree, CowshedError>;
    pub async fn process_events(&self, every_ms: u64) -> Result<JobProcessStream, CowshedError>;
    pub async fn progress(&self, every_ms: u64) -> Result<JobProgressStream, CowshedError>;
    pub async fn tail(&self, cursor: Option<JobJournalCursor>, limits: JobTailLimits)
        -> Result<JobTail, CowshedError>;
    // From `offset` on: a reader holding the first `offset` bytes continues where it stopped.
    pub async fn logs(&self, stream: JobStream, offset: u64, follow: bool)
        -> Result<RawByteStream, CowshedError>; // representation-transparent; always resolves storage.artifact
    pub async fn attach(&self, cursor: Option<JobJournalCursor>) -> Result<JobAttachment, CowshedError>;
    pub async fn detach(&self) -> Result<(), CowshedError>;          // job continues
    pub async fn wait(&self) -> Result<JobInfo, CowshedError>;
    pub async fn kill(&self) -> Result<(), CowshedError>;            // awaits complete process-tree termination
}

/// A live attachment is only a view over one durable job's raw backing streams.
pub struct JobAttachment { /* immutable fence + JobStdin + two independent RawByteStream handles */ }
impl JobAttachment {
    pub fn into_parts(self) -> (JobStdin, RawByteStream, RawByteStream);
    pub async fn detach(self) -> Result<(), CowshedError>; // closes this view; the job continues
}

pub struct JobStdin { /* same immutable fence + JobId + actor sender */ }
impl JobStdin {
    pub async fn write(&self, bytes: Bytes) -> Result<(), CowshedError>;
    pub async fn close(&self) -> Result<(), CowshedError>; // EOF once; the job is not cancelled
}

pub struct RawByteStream { /* bounded receiver; polling task retains the same immutable fence */ }
impl RawByteStream {
    pub async fn next(&mut self) -> Option<Result<Bytes, CowshedError>>;
}

pub struct GrantSet {
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port_block: Option<PortBlock>,   // macOS: Some({ base, size }); Linux: None and omitted from JSON/N-API,
                                         // never a zero-sized or otherwise sentinel block
    pub read: Vec<PathBuf>,
    pub write: Vec<PathBuf>,
    pub deny_write: Vec<PathBuf>,       // paths relative to this workspace, terminal file-write* denies
    pub deny: Vec<PathBuf>,             // paths relative to this workspace, terminal file-read* file-write* denies
    pub egress: Vec<EgressRule>,         // { host, ports, mode }
    pub repos: Vec<RepoRule>,            // repo-scoped mirror grants (05_gateway.md)
    pub sim: Vec<SimVerb>,               // personal-session simulator broker verbs (04/05/14_nix.md)
}
pub enum SimVerb { OpenUrl, Install }    // closed enum; unknown verbs are usage errors
pub struct PortBlock { base: u16, size: u16 } // private; `new` and custom JSON decode enforce a power-of-two size ≥ 2, a base aligned to it, and a checked end; new blocks are 64
pub struct EgressRule {                 // 04_sandbox.md / 05_gateway.md
    pub host: String, pub ports: Vec<u16>,
    pub mode: EgressMode,                // default Intercept (per-workspace CA); Opaque = pass-through CONNECT
}
pub enum EgressMode { Intercept, Opaque }
pub struct GrantDelta {
    pub read: Vec<PathBuf>, pub write: Vec<PathBuf>, pub deny_write: Vec<PathBuf>, pub deny: Vec<PathBuf>,
    pub egress: Vec<EgressRule>, pub repos: Vec<RepoRule>, pub sim: Vec<SimVerb>,
    pub service_ports: Option<u16>,      // macOS: minimum service ports in the block, gateway excluded; grows only
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
grant model — the axes are read, write, egress, and repo, plus macOS port capacity.

`GrantDelta::service_ports` asks for a macOS port block holding at least that many service ports, the gateway listener
at `base` excluded; the block becomes the smallest aligned power-of-two block with `service_ports + 1` ports. Growth is
monotone: a count the block already holds leaves the block, the revision, and the supervisor unchanged, and `revoke`
refuses the field. Linux refuses it because no port block exists there. The block grows in place when it can and
otherwise moves to a larger disjoint block; either way the grant requires an idle workspace and is refused immediately
(never queued) while any of its jobs is active. A disjoint move adds the old block to `GrantSet::retained_port_blocks`:
it stays reserved to the workspace and connectable by its jobs until the workspace retires, while the gateway endpoint
and the advertised base/size follow the current `port_block`. Growth into a block that contains the current one retains
nothing, and a later block that contains a retained block drops that entry without releasing a port; retained blocks are
otherwise released only at retirement. The next exec launches a supervisor, and so a `SandboxSpec`, from the new
snapshot. A block with no free aligned place in the host's reserved range is refused.

This is the controller integration point: a trusted orchestrator holds a `Coordinator`, hands each worker a
`WorkspaceHandle`, calls `Coordinator::grant` as policy allows, and either lets cowshed spawn (`exec`) or takes a
controller-side `SandboxSpec` while launching the supervisor. A worker never receives the latter launch authority.

## Capability split: `Coordinator` vs `WorkspaceHandle`

Mutation authority lives only on `Coordinator`; `WorkspaceHandle` is the non-escalating worker capability. Together they
mirror the MCP token model (12_mcp.md) in the type system:

```rust
pub enum RevisionTarget {
    Branch(BranchName),   // validated `git check-ref-format --branch` domain
    Ref(GitRef),          // validated fully-qualified `refs/...` domain
    Oid(GitOid),          // validated lowercase 40- or 64-hex; never a land destination
}
impl RevisionTarget {
    pub fn parse_cli(value: impl Into<String>) -> Result<RevisionTarget, DtoError>;
}

pub struct RebaseOptions {
    pub onto: Option<RevisionTarget>,    // default: what the workspace lands into; exclusive with `into`
    pub fresh: bool,
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    pub expected_source_head: Option<GitOid>,
    pub expected_onto_head: Option<GitOid>,
}

pub struct LandOptions {
    pub target_branch: Option<String>,   // default: the branch the target has checked out
    pub check: Option<Vec<String>>,
    pub retire: bool,
    pub push_only: bool,
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    pub expected_source_head: Option<GitOid>,
    pub expected_target_head: Option<ExpectedRefHead>,
}

pub struct LandReport {
    pub landed_head: GitOid,
    pub target_branch: String,
    pub previous_target_head: Option<GitOid>,
    pub target_was_checked_out: bool,
    pub retired: bool,
}
```

```rust
/// The sole mutation, diagnostics, and cross-workspace authority over a project. It is the only capability that can
/// adopt, create, destroy, fork, grant, revoke, restore, rebase, land, collect garbage, diagnose controller state,
/// mirror repositories, set checkpoint quotas, or assign slots.
pub struct Coordinator { /* Project + authenticated controller identity */ }
impl Coordinator {
    // The daemon's refusal of this controller's build (`OtherBuild`), once any call on this
    // connection met it; the remedy is a controller of the daemon's build (11_shell.md "hello").
    pub fn other_build(&self) -> watch::Receiver<Option<OtherBuild>>;
    pub async fn adopt(&self, opts: AdoptOptions) -> Result<WorkspaceRef, CowshedError>;
    pub async fn create(&self, name: &str, opts: CreateOptions) -> Result<WorkspaceRef, CowshedError>;
    pub async fn fork(&self, src: &str, dst: &str) -> Result<WorkspaceRef, CowshedError>;
    pub async fn grant(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    pub async fn revoke(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    // The project's standing grants (04: reads and egress every workspace runs under).
    pub async fn project_grants(&self) -> Result<ProjectGrants, CowshedError>;
    pub async fn grant_project(&self, delta: ProjectGrantDelta) -> Result<ProjectGrants, CowshedError>;
    pub async fn revoke_project(&self, delta: ProjectGrantDelta) -> Result<ProjectGrants, CowshedError>;
    // `into`: the lane base the workspace lands into, main when None (02: "Lanes").
    pub async fn rebase(&self, ws: &str, into: Option<&WorkspaceRef>, opts: RebaseOptions) -> Result<GitOid, CowshedError>;
    pub async fn land(&self, ws: &str, into: Option<&WorkspaceRef>, opts: LandOptions) -> Result<LandReport, CowshedError>;
    pub async fn restore(&self, ws: &str, label: &str) -> Result<(), CowshedError>;
    pub async fn detach(&self, ws: &str) -> Result<EmptyResult, CowshedError>;
    pub async fn assign_slot(&self, ws: &str, slot: u32) -> Result<(), CowshedError>;
    pub async fn destroy(&self, ws: &str, opts: RemoveOptions) -> Result<(), CowshedError>;
    pub async fn gc(&self, opts: GcOptions) -> Result<GcReport, CowshedError>;
    // Remove the adopted project end to end (`cowshed rm main --restore --purge`, 02_workspaces.md): every session
    // workspace, listed or never published, under opts' force/abandon; wait for their reclamation; gc; under abandon
    // delete the trash's abandon bundles; restore main. A stale gc plan is `Conflict` with `Retry::GcPlanStale`
    // (`retry_source()`, wire `"retry": {"reason": "gcPlanStale"}`): call again, nothing done is repeated.
    pub async fn remove_project(&self, opts: RemoveProjectOptions) -> Result<RemoveProjectReport, CowshedError>;
    pub async fn repo_mirror(&self, ws: &str, url: &Url) -> Result<MirrorInfo, CowshedError>;
    pub async fn set_checkpoint_quota(&self, ws: &str, quota: CheckpointQuota) -> Result<(), CowshedError>;
    pub async fn doctor(&self) -> Result<DoctorReport, CowshedError>;
    /// Hand a worker a capability scoped to exactly one workspace.
    pub async fn worker(&self, ws: &str) -> Result<WorkspaceHandle, CowshedError>;
}
```

`Coordinator::land` fast-forwards a real `refs/heads/<target_branch>`, not a hidden integration ref, in the target's
repository: main's, or with `into` the lane base's. The target branch must be the one the target's checkout has checked
out; the operation updates it through that checkout so its `HEAD`, index, and working tree all resolve to `landed_head`,
and dirty state causes `Conflict`. Any other branch, or a detached checkout, is refused. All expected values are
revalidated under the target lock immediately before the fast-forward. A mismatch or non-fast-forward retains the source
workspace and leaves the target branch and visible working state unchanged.

`into` is a `WorkspaceRef`, not a name, so it carries the incarnation it was resolved at: a lane base removed and
recreated under the same name since is refused with `Conflict`, and naming the workspace itself, or `into` together with
`RebaseOptions::onto`, with `Usage`. The CLI's `--into <name>` resolves the reference once, when it runs.

```rust
/// A worker's capability: it can run and observe work in *its* workspace and hand results
/// back, but it can never widen its own sandbox or touch another workspace. Escalation is
/// the coordinator's job. This is the type a subagent receives; it carries no grant mutation.
pub struct WorkspaceHandle {
    /* WorkspaceRef snapshot + immutable repo/workspace/incarnation fence + actor sender */
}
impl WorkspaceHandle {
    pub fn name(&self) -> &WorkspaceName;
    pub fn mount_path(&self) -> &Path;
    // The immutable WorkspaceInfo the handle was minted on; its incarnation is the fence every call carries.
    pub fn info(&self) -> &WorkspaceInfo;
    pub async fn exec(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;
    pub async fn shell(&self, session: Option<&str>) -> Result<Session, CowshedError>;
    pub async fn list_jobs(&self) -> Result<Vec<JobInfo>, CowshedError>;
    pub async fn job(&self, id: JobId) -> Result<JobHandle, CowshedError>;
    pub async fn job_by_key(&self, key: &str) -> Result<JobHandle, CowshedError>;
    // An ended job's terminal record and a handle reading its sealed output, also for a job an
    // earlier supervisor of the incarnation ran (11_shell.md "Draining a supervisor of another build").
    pub async fn sealed(&self, id: JobId) -> Result<(SealedJob, JobHandle), CowshedError>;
    pub async fn checkpoint(&self, opts: CheckpointOptions) -> Result<String, CowshedError>; // quota-enforced atomically
    pub async fn push(&self, opts: PushOptions) -> Result<PushReport, CowshedError>;
    pub async fn grants(&self) -> Result<GrantSet, CowshedError>;   // read-only: observe, never mutate
    // No grant/revoke, restore/destroy/rebase/land/gc, repo mirror, detach, or cross-workspace access.
}
```

`CheckpointOptions` carries both the optional validated label and explicit retention intent:

```rust
pub struct CheckpointOptions {
    pub label: Option<String>,
    pub keep: bool,
}
```

`PushOptions` and `PushReport` make preservation and retry safety explicit:

```rust
pub enum ExpectedRefHead { Missing, Oid(GitOid) }

pub struct PushOptions {
    pub branch: Option<BranchName>, // the workspace's local source branch; `cowshed/<ws>` when absent
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    pub expected_source_head: Option<GitOid>,
    pub expected_destination_head: Option<ExpectedRefHead>,
}

pub struct PushReport {
    pub source_head: GitOid,
    pub destination_ref: String,
    pub previous_destination_head: Option<GitOid>,
}
```

The destination is the non-checked-out `refs/cowshed/<ws>/heads/<branch>` ref in the main workspace's standalone
repository/object store, where `<branch>` is the source branch. A push fetches and installs the exact `source_head` (the
fetched source branch tip) through host-side Git in the main repository, reading the workspace mount by path: it needs
no remote on either side and never runs the workspace's Git configuration or hooks. It never checks out or advances the
Git `main` branch and never changes the main workspace index or working tree. Each supplied expectation is checked as
one atomic destination-ref update. An incarnation, source-head, or destination-head mismatch is
`CowshedError::Conflict`, leaves the destination unchanged, and does not retire the source workspace. `Missing` lets a
caller assert first publication rather than accepting an overwrite. After success, the source may be retired only when
all commits that must survive are reachable from the returned durable ref. Remote publication and workflow policy are
not part of this API.

`WorkspaceHandle::checkpoint` is intentionally available to a worker for retry points, but it is not unbounded storage
authority. `Coordinator::set_checkpoint_quota(ws, quota)` owns a cap for exactly that workspace. Before the supervisor
barrier, admission reads authoritative `SubstrateStats`: projected count is that workspace's existing checkpoint count
plus one; projected bytes are all of its existing checkpoint allocated bytes (pinned and unpinned) plus the target
active image's allocated bytes. `pinned_checkpoint_bytes` is an authoritative reporting subset, not an exclusion from
quota. Sibling workspaces consume none of the cap. Exact `<=` boundaries pass; `>` is `CowshedError::Conflict` before
any checkpoint image, fact, metadata, or barrier publication. Only then does cowshed run the 02_workspaces.md barrier,
clone while held, and publish the controller manifest-digest/lineage commitment. Manifest/commitment mismatch is
`CowshedError::Integrity`. Restore remains coordinator-only. On controller startup, restore retryability is derived only
from APFS `PendingPublicationFact`s backed by exact `.restore.json` substrate recovery facts. The coordinator
idempotently republishes the matching `RestoreCommitment`, activates the pending detached metadata, and removes the
fact; no runtime journal or `.restore-fences` capability exists.

`JobInfo.state = OutputLimit` is the explicit result when the configurable combined stdout+stderr quota (default 1 GiB)
is crossed. Accounting includes protected and in-flight bytes; the supervisor admits no payload beyond the exact
boundary, terminates the process group, drains pipes, seals artifacts, and only then publishes terminal state. Summaries
remain a separate bounded diagnostic projection.

The shell connection is a reconnectable view over the persistent, permission-checked, multi-client per-workspace
supervisor socket (11_shell.md). Dropping a `Session`, `JobHandle`, or transport never unlinks that socket or stops the
job. Protected complete in-volume Arrow batches and sealed artifacts are authoritative captured-content evidence within
their origin incarnation/checkpoint boundary. Controller commitments are authoritative for existence/status/order/
lineage and expected counts/hashes, without payload/path duplication. APIs reconcile both and return `Integrity` for a
missing committed artifact, invalid complete batch, or digest/lineage contradiction; neither side silently wins.
Authorization, grant, and gateway audit authority remain controller-owned.

The invariant is the same one that keeps grant files outside the volume (01_storage.md, 04): a capability reachable from
inside a sandbox must not authorize escalation. `WorkspaceHandle` is that principle expressed in the type system — a
subagent holding one cannot grant itself anything.

## DTO freeze (single source of truth)

The canonical API declarations own operation signatures, authority scopes, request/result/event records, units, wire
names, and validation bounds. They generate the controller protocol, N-API adapter signatures, TypeScript public types,
and validators as one surface. There is no separate handwritten N-API request or TypeScript DTO field list. Shared
corpus tests check every generated projection and reject a deliberately changed or omitted field.

Externally projected types are defined **once** in `cowshed-core` and reused verbatim by the CLI (`--json` bodies),
NAPI, and MCP — no adapter redefines a field, and contract goldens (08_testing.md) pin their shapes: `WorkspaceInfo`,
`CheckpointInfo`, `GcReport`, `Finding`, `JobId`, `JobState`, `JobInfo`, `StreamInfo`, `OutputStorage`,
`ProtectedOutput`, `BinaryData`, `OutputSummary`, `OutputPublication`, `PublicationPolicy`, `ControllerCommitment` and
its five event structs, `PushReport`, `LandReport`, `RevisionTarget`, `GrantSet`/`GrantDelta`/`PortBlock`/`EgressRule`/
`RepoRule`/`SimVerb`, `GatewayStatus`, `AuditEvent`, and every `*Options` type (`AdoptOptions`, `CreateOptions`,
`AttachOptions`, `CheckpointOptions`, `RemoveOptions`, `GcOptions`, `RebaseOptions`, `LandOptions`, `PushOptions`).

`JobArtifactRecord`, `ProtectedRecord`, `CheckpointManifestRecord`, and `VisibleJobCommitment` are canonical internal
storage/Arrow contracts, not CLI/N-API/MCP JSON envelopes. Adapters expose only their bounded constituent result types
and payload-free controller commitments; they never reveal protected paths or inline storage records.

Field sketches elsewhere in this spec are illustrative; the freeze rule — one definition, reused, versioned together —
is the contract. `GrantSet.port_block` is the platform union: macOS always `PortBlock`, while Linux carries `None` and
its JSON/N-API projection omits `portBlock`. `GrantSet.retained_port_blocks` (`retainedPortBlocks`, omitted when empty)
is controller-written, macOS-only, and non-empty only after a relocation; no `GrantDelta` field sets it. Adapters and
consumers must use that optional shape directly; casts, `null`, zero-sized blocks, and sentinel base values are
forbidden. Adding a field is a coordinated change across core + goldens, not a per-adapter patch.

JSON and N-API use the same camel-case projection. `StreamInfo` is exactly `{storage,bytes,sha256,summary}`. `storage`
is `{kind:"captured",artifact}` or `{kind:"redirect",source,artifact}`; an artifact is `{kind:"inline",data}` or
`{kind:"file",path}`. Inline `data` is the bounded wire union
`{encoding:"utf8",data:"…"} | {encoding:"base64",data:"…"}`; `sha256` is 64 lowercase hex characters. Ordinary `JobInfo`
JSON may carry this bounded inline data, while controller commitments carry no payload. No path exists for an inline
artifact, and no adapter adds a synthetic one. `Redirect.source` is mutable caller-visible state; readers and authority
always resolve its independent protected `artifact`. `summary` is `{version,text,truncated}`. Field names are
camel-case, but `ErrorCode` values are taxonomy tokens: every adapter uses the same kebab-case `not-found`,
`environment-missing`, and `sandbox-denied` plus the unhyphenated `integrity`; no adapter rewrites an error code.

Public request/result and controller-commitment definitions live in `cowshed_core::api::dto`; the protected
`JobArtifactRecord`/manifest/record envelope and Arrow projections live in `cowshed_core::storage::job_artifact` and
reuse those DTOs. Serde uses `camelCase`, documented enum strings, and omission rather than `null`.

### Job monitoring

`JobInfo.resources` and `SealedJob.resources` carry the latest `JobResourceSample` once the child has spawned, frozen at
terminal publication. They are absent for a queued job or a failure before spawn: no leader or start baseline exists to
sample. Every live resource progress event carries the same type, not a lighter telemetry DTO. `resources()` reads it
without waiting for exit and reports a typed not-ready conflict before spawn; `progress(everyMs)` emits the latest
sample once available, periodic samples even when neither stream advances, then the terminal sample exactly once before
closing. The interval is positive and uses the declaration's bounded duration contract. A slow reader may coalesce
intermediate samples; terminal evidence and journal bytes are never lost.

```rust
pub struct WallMillis(u64);
pub struct WallMicros(u64);
pub struct CpuMillis(u64);
pub struct CpuMicros(u64);
pub struct CpuMicrosDelta(i64);
pub struct ResidentBytes(u64);
pub struct ChargedMemoryBytes(u64);
pub struct StorageIoBytes(u64);
pub struct StorageIoBytesDelta(i64);
pub struct VolumeUsedBytesDelta(i64);
pub struct HostLoad1(f64);
pub struct OneCoreCpuPercent(f64);
pub struct HostCores(u16);
pub struct HostLoadSample { pub load1: HostLoad1, pub cores: HostCores }
pub enum VolumeUnavailable {
    Unconfigured,                     // the supervisor was configured with no volume to stat
    UnsupportedPlatform,              // this platform's substrate has no used-bytes stat yet
    Failed { message: String },       // the stat failed at spawn or at this sample, or its delta is inexact
}
pub enum VolumeUsage { Read { delta_bytes: VolumeUsedBytesDelta }, Unavailable { reason: VolumeUnavailable } }
pub struct JobVolumeUsage {
    pub workspace: VolumeUsage,
    pub build: Option<VolumeUsage>,   // absent when the job runs with no build volume
}
pub struct StreamBytes(u64);
pub struct StreamLines(u64);
pub struct JobStreamWatermark { pub bytes: StreamBytes, pub lines: StreamLines }
pub struct JobResourceSample {
    pub job_id: JobId,                // retained in standalone progress and terminal resource receipts
    pub sampled_at: UtcTimestamp,
    pub wall_ms: WallMillis,
    pub wall_us: WallMicros,          // retained precise duration; wallMs is its display projection
    pub leader_pid: u32,              // observed leader; this sample exists only after spawn
    pub members: Vec<u32>,            // complete current membership of the owned job process group
    pub leaf: JobLeafAttribution,      // the observed CPU-dominant process, or why none is named
    pub cpu_user_ms: CpuMillis,       // derived once from the accounting source's microseconds
    pub cpu_sys_ms: CpuMillis,
    pub cpu_pct: OneCoreCpuPercent,
    pub rss_bytes: ResidentBytes,
    pub rss_peak_bytes: ResidentBytes,
    pub accounting: JobAccounting,
    pub host_start: HostLoadSample,
    pub host: HostLoadSample,          // current sample; end host facts in the terminal sample
    pub volumes: JobVolumeUsage,
    pub stdout: JobStreamWatermark,
    pub stderr: JobStreamWatermark,
}
pub struct JobJournalCursor { pub stdout: u64, pub stderr: u64 }
pub struct JobTailLimits { pub bytes_per_stream: u32, pub lines_per_stream: u32 }
pub struct JobTail {
    pub stdout: BinaryData,
    pub stderr: BinaryData,
    pub next: JobJournalCursor,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}
pub struct JobProgressStream { /* bounded stream of Result<JobResourceSample, CowshedError> */ }
pub struct JobListeningPorts {
    pub job_id: JobId,
    pub sampled_at: UtcTimestamp,
    pub ports: Vec<u16>,              // current TCP LISTEN ports owned by this job's process group
}
```

The sample carries its own `jobId`, so a progress event or result projection cannot lose the command's cowshed identity.
Spawn means the first process owned by the job: its shell activation on a cold host, otherwise its command. While a cold
host activates, the job already has real resource samples; it is not an unobserved queue wait. The wall, host-start, and
volume baselines remain at that first spawn as ownership moves to the command's group. CPU accumulates activation plus
command without charging a warm host's earlier idle time; the current leader and members change to the command's group
when it starts. An activation that fails preserves its observed cost in the terminal result.

`sampledAt` uses the existing RFC3339 timestamp contract; elapsed wall time and CPU durations use monotonic integer
milliseconds. `hostStart` is captured once at job spawn and survives every later sample; `host` is captured at the
sample boundary. CPU user/sys time is cumulative across the complete owned process group, retaining exited and reaped
descendants without double-counting them. `cpuPct` is the user+system delta divided by the elapsed window since the
preceding supervisor sample, multiplied by 100; it is one-core normalized and may exceed 100. Reading resources or
starting a second subscriber does not reset that window. The first zero-length window reports zero share, not a division
by zero. RSS is the simultaneously sampled group sum; peak is its maximum observed sum. `members` retains the complete
current group, never a truncated membership claim; CPU/RSS accounting covers every member. A sample exceeding the
generated frame bound is a typed error, not a silently shortened list. `leaderPid` retains the job's observed leader
after exit; terminal membership may be empty. An incomplete kernel membership read is an operational error, never
evidence of an empty group.

`volumes` compares the used bytes of the workspace volume and the job's build volume at spawn with those at the sample
boundary. Deltas are signed and never clamped: deletion can shrink a volume. They describe volume-wide usage, not
per-process write syscalls or exclusive attribution when commands overlap. Usage is a volume stat, never a recursive
scan or a shared-store/container free-space proxy. Each volume answers for itself: `Read` with its delta, or
`Unavailable` with why, so one volume that cannot be read never fails the sample or hides the other's reading. `build`
is absent when the job runs with no build volume. A stat that fails at spawn or at the sample is that volume's `Failed`,
never absence or zero usage; a platform whose substrate has no used-bytes stat reports `UnsupportedPlatform` (Linux
until the ZFS dataset stat), and a supervisor configured with no volumes reports `Unconfigured`. The supervisor's
configuration carries the workspace volume's mountpoint, refused at construction when nothing is mounted there; each
job's build volume is the one its admission grants. Both baselines are read at admission, immediately before the job's
first process is dispatched, so nothing the job writes precedes them.

Stream watermarks count admitted bytes and newline-delimited lines separately for stdout/stderr; a non-empty trailing
partial line counts as one line. `bytes` is also the next byte cursor. `tail(cursor, limits)` returns a bounded raw
slice after each supplied cursor and its next cursor; an omitted cursor selects the latest bounded tail and is absent on
the wire, never `null`. Limits are positive, with at most 64 KiB per stream. Each byte window is then cut to
`linesPerStream` lines: the first lines after a cursor, the last lines for the latest tail. Truncation is explicit:
admitted bytes lie after a cursor slice or before the latest slice. Both cursors are checked against one admitted
snapshot before either stream is read. Offsets remain representation-transparent through inline/file promotion, and a
cursor beyond admitted bytes is a typed usage error. `attach(cursor)` resumes each stream at its supplied offset;
omission means byte zero. It never starts a process.

All monitoring methods retain the immutable repo/workspace/incarnation fence of the `JobHandle`. The same handle exposes
`kill()` for explicit complete-group cancellation; a monitoring executor maps its own operation key to this exact job
before calling it. Disconnecting a reader, detaching an attachment, or reaching a soft deadline never kills the job.
Sampling failure is a typed operational error with the unavailable metric's cause, not fabricated zeroes; captured
output and the actual exit status remain preserved.

`listeningPorts()` reads the current TCP LISTEN sockets held by the job's identity-proven process-group members,
including a child that binds before its leader exits. It reports both IPv4 and IPv6 local ports, deduplicated; a merely
bound, connected, or host-unrelated socket is not readiness. A port another process owns never satisfies this job's
readiness, even if a host-wide connection probe succeeds. The same process-birth fence used for group sampling protects
socket ownership from PID reuse. Missing ownership evidence is a typed error, never an empty list that pretends
readiness was checked. The query starts no process and preserves the workspace incarnation fence.

Each member's sockets are read by pid and count only if that member's identity (its pidfd on Linux, its pid version on
macOS), read afterwards, shows it still running: a member shown exited holds none, and a failed read of a running member
is an error. Linux matches the socket inodes of `/proc/<pid>/fd` against the `LISTEN` rows of `/proc/<pid>/net/tcp` and
`tcp6`; those tables are the member's own network namespace, which is complete because a job never leaves its
workspace's one private namespace (04_sandbox.md). macOS reads each socket descriptor's `socket_fdinfo`
(`PROC_PIDFDSOCKETINFO`) and keeps TCP sockets in `TSI_S_LISTEN`. A job that has not yet owned a process is a conflict;
an ended job's group answers what its ended group still holds, normally nothing.

Attachment stdin is the same bounded, backpressured raw-byte lane as exec stdin, not text interpolated into the command.
`JobStdin.write` waits until its chunk is admitted to the job's input queue; `close()` sends EOF exactly once and is
idempotent. Writes after EOF are a typed conflict. The N-API attachment projects these as `write()` and `end()`, without
buffering the whole input. Detaching the attachment or closing its output iterator does not kill the job; explicit input
EOF and job cancellation remain different operations.

### Process-tree observations

`processes()` returns `JobProcessTree { jobId, sampledAt, processes, coverage }`, retaining final records of observed
exited descendants as well as current members. The supervisor observes fork, exec and exit events. Linux requires an
event source beside pidfd identity and proc metrics. Periodic proc children polling alone is not a complete tree. A
missed observation records a typed coverage gap and an unattributed-usage row; it never silently claims completeness or
trusts a reused PID.

macOS per-process coverage is best-effort with a measured gap, and its job totals carry their own named source limits
(below). kqueue `NOTE_FORK` on each member coalesces and carries no child PID, and `NOTE_TRACK`/`NOTE_CHILD` are refused
with `ENOTSUP` (xnu
[`filt_procattach`](https://github.com/apple-oss-distributions/xnu/blob/ac9718fb1af618d5ce8678d0dc6e8a58f252216f/bsd/kern/kern_event.c#L1094-L1131),
[`filt_procevent`](https://github.com/apple-oss-distributions/xnu/blob/ac9718fb1af618d5ce8678d0dc6e8a58f252216f/bsd/kern/kern_event.c#L1210-L1212),
and the `NOTE_FORK` comment in `bsd/sys/event.h`; measured on Darwin 25.6: 64 children forked and reaped under a watched
root delivered one `NOTE_FORK` and an empty child list). The supervisor therefore answers each `NOTE_FORK` by reading
the member's children with `proc_listchildpids` and watches each new one for `NOTE_FORK`/`NOTE_EXEC`/`NOTE_EXIT`
(`NOTE_EXITSTATUS` is accepted for any process the supervisor may signal, grandchildren included). A child that forks,
execs and is reaped before that read is missed, and a coalesced `NOTE_FORK` cannot prove how many children it stood for,
so a macOS tree in which a member forked never claims `Complete`. The leader/children rusage source (below) still counts
the missed children's CPU once each parent up to the leader has reaped them, and reconciliation states it as
unattributed usage. An Endpoint Security observer is not used: it needs an Apple entitlement. It is revisited only if
the measured gap on real gates proves large.

The canonical records are:

```rust
pub struct JobProcessTree {
    pub job_id: JobId,
    pub sampled_at: UtcTimestamp,
    pub processes: Vec<JobProcessSample>,
    pub coverage: ProcessCoverage,
}
pub struct JobProcessLeaf { pub pid: u32, pub program: String, pub argv: Vec<CommandArg> }
pub enum ProcessBlockedOn {               // lock detail exists only on a lock: no stale path or holder
    None, Lock { path: String, holder: Option<LockHolder> }, Socket, Pipe, Child, Stdin, Disk,
}
pub struct LockHolder { pub pid: u32, pub job: Option<JobId> }
pub struct JobProcessSample {
    pub pid: u32,
    pub ppid: u32,
    pub program: String,
    pub argv: Vec<CommandArg>,
    pub born_at: UtcTimestamp,
    pub usage: Option<ProcessUsage>,          // absent until its counters are first read, never zeroes
    pub blocked_on: Option<ProcessBlockedOn>, // absent when not observed, not fabricated `none`
    pub exit: Option<ProcessExit>,
}
pub struct ProcessExit { pub status: ExitStatus, pub exited_at: UtcTimestamp }
pub const BUSY_CPU_PERMILLE: u32 = 10;     // busy: own CPU over the last sample window ≥ 1 % of one core
pub struct ProcessUsage {                  // the process's own counters, as last read
    pub cpu_user_us: CpuMicros,
    pub cpu_sys_us: CpuMicros,
    pub busy: bool,
    pub rss_bytes: ResidentBytes,          // zero once it has exited
    pub rss_peak_bytes: ResidentBytes,
    pub io: ProcessStorageIo,
}
pub enum ProcessStorageIo {
    Read { read_bytes: StorageIoBytes, write_bytes: StorageIoBytes },
    Unavailable { reason: ProcessIoUnavailable }, // never zero bytes in their place
}
pub enum ProcessIoUnavailable {
    NotPermitted,                          // the kernel refused this observer the counters
    NotAccounted,                          // the kernel keeps no per-process storage I/O counters
}
pub const PROCESS_HEARTBEAT: Duration = Duration::from_secs(60); // per live process, not per tree
pub enum JobProcessEvent {             // `index`: the record's position in `JobProcessTree.processes`
    Born { index: u32, process: JobProcessSample },
    Exec { index: u32, process: JobProcessSample },
    Changed(JobProcessDelta),          // non-empty sparse change of one process
    Heartbeat { index: u32, process: JobProcessSample }, // one unchanged live process, once a minute
    Exited { index: u32, process: JobProcessSample }, // final own-process usage, exit and exitedAt
}
pub enum JobProcessDelta {            // one changed field; no shape changes nothing
    Usage { index: u32, usage: ProcessUsage },          // usage is never cleared
    Blocker { index: u32, blocked_on: BlockerChange },
}
pub enum BlockerChange { Set(ProcessBlockedOn), Clear }
pub struct JobProcessStream { /* bounded stream of Result<JobProcessEvent, CowshedError> */ }
pub enum ProcessCoverage { Complete, Gap { reason: ProcessCoverageGap } }
pub enum ProcessCoverageGap {      // the first observation the tree is known to lack
    EventsLost,                     // the kernel event source reported dropped events
    UnobservedBirth { pid: u32 },   // a fork, exec or exit named a process whose birth was not observed
    UnobservedExit { pid: u32 },    // a pid was born again while its previous life had no observed exit
    UncountedFork { pid: u32 },     // a member forked without the kernel naming or counting its children
    UnreadImage { pid: u32 },       // a member exec'd and its new image was not read (exited first, or the read failed)
    UnreadFinalUsage { pid: u32 },  // a member exited with no read of its counters after its exit
}
pub struct CpuTotals { pub user_us: CpuMicros, pub sys_us: CpuMicros }
pub struct StorageIoTotals { pub read_bytes: StorageIoBytes, pub write_bytes: StorageIoBytes }
pub struct ChargedMemoryUsage { pub current_bytes: ChargedMemoryBytes, pub peak_bytes: ChargedMemoryBytes }
pub struct UsageReconciliation {
    pub cpu_us: CpuMicrosDelta,
    pub io_read_bytes: Option<StorageIoBytesDelta>,
    pub io_write_bytes: Option<StorageIoBytesDelta>,
    pub coverage: ProcessCoverage,
}
pub enum JobAccounting {
    LinuxCgroupV2 { cpu: CpuTotals, io: StorageIoTotals, charged_memory: ChargedMemoryUsage,
                    reconciliation: UsageReconciliation },
    MacOsRusageChildren { cpu: CpuTotals, io: Option<StorageIoTotals>, reconciliation: UsageReconciliation },
}
```

Birth and parent identity are kernel observations retained internally, not fresh trust in numeric PID values.
Per-process CPU counters are that process's own usage. Complete job counters have an independent, declared source and
reconcile against the retained process counters; they are not a live-members-only sum. Microseconds convert to
milliseconds once, after aggregation. Storage I/O byte counters follow the kernel source's semantics and never convert
operation counts into invented byte totals or stand in for volume-allocation deltas. An exit's status and time are one
`ProcessExit`, present only after exit; the existing `ExitStatus` union prevents an empty or ambiguous code/signal
result. A gap is absorbing and names the first missed observation. `UncountedFork.pid` is the observed forking member,
not an invented child PID or a missing-child count; `UnreadImage.pid` is the member whose new image was not read,
whether it exited before the read or the read failed, not an invented program or argv. A failed read is also returned as
an observation error with its call and errno; the gap keeps the tree from claiming Complete, and does not replace the
error. Both gaps retain their evidence through the generated controller and N-API records. Blocker detail fields are
valid only for their observed blocker kind; an unobserved blocker is absence or an observation error, never an assertion
that the process is unblocked. A lock observation identifies its path and, when kernel evidence resolves it, the holder
PID and that holder's job; no program-name guess supplies it.

`usage` is a process's own counters as last read. It is absent, never zero, for a process whose counters were never
read: one reaped before the sampler reached it, or on macOS one that exited before its first read could be proven its
own. Usage is final only when it was read after the process exited. An exit with no such read is the coverage gap
`UnreadFinalUsage`, and the usage stays the last one observed, never zeroed or reset. Each read is fenced to the
retained life. On macOS the first read is proven by the process's unique id read after it, and every later one by the
start time the read itself carries. On Linux the held pidfd must still name an unreaped process after each read. `busy`
holds when the process's own CPU over the supervisor's last sample window is at least `BUSY_CPU_PERMILLE` thousandths of
one core; that constant is the one declaration consumers read through the generated surface. A first read and a
zero-length window divide nothing and keep the prior judgment, initially idle. Reading usage or subscribing never moves
the window. Storage I/O follows each platform's per-process source: macOS
`ri_diskio_bytesread`/`ri_diskio_byteswritten`, the I/O the process issued to disk; Linux `/proc/<pid>/io`
`read_bytes`/`write_bytes`, reads it caused to be fetched from storage and pages it dirtied for storage, counted when
dirtied rather than at writeback. Reads served from the cache count on neither. `NotPermitted` is a kernel refusal
(Linux: a process made non-dumpable, for example by a set-id exec); `NotAccounted` is a kernel that keeps no per-process
counters.

`processEvents(everyMs)` emits birth/exec transitions, non-empty changed-state records, one heartbeat per minute for
each unchanged live process, and each process's final usage on exit. The sampling/subscriber interval `everyMs` does not
set the heartbeat cadence; changed-state and terminal events are immediate. A blocker transition, resident memory
crossing a power of two (`[2ⁿ⁻¹, 2ⁿ)` is one step), a busy/idle CPU flip, and a process's first usage read produce
change records; a sample that only moves counters does not. A live process whose usage no record restated for
`PROCESS_HEARTBEAT` gets its own heartbeat restating its whole record; a blocker change restates no usage, so it does
not postpone one. A read taken after a process exited is withheld from every record until its `Exited` event carries it,
once, as final usage: from that read the process is no longer live, so no heartbeat restates it, no blocker read of it
is accepted, and a late exec event still shows the usage read before the exit. Nothing about that process follows its
exit. Closing a reader never kills the process. Consumers use these observed facts without declaring or deriving an
expectation from a command's argv. Events name a process by `index`, its record's position in birth-observation order,
never by a pid that another life may reuse. The generated sparse delta changes exactly one field, usage or blocker;
absent is unchanged. A blocker observed afresh, `none` included, is SET, and a blocker no longer observed is CLEAR.
Because lock detail lives inside `Lock`, setting any other blocker leaves no stale path or holder. No delta shape
changes nothing, so neither Rust nor the generated validators admit an empty one.

The `leaf` in `JobResourceSample` is the observed process, live or exited, with the most own CPU (user+system
microseconds), ties broken by birth identity then PID. It does not need a complete tree: it needs the observed processes
to account for most of the job's exact CPU. It is named only when the observed processes' own CPU covers at least a
declared fraction of the accounting source's CPU total; the fraction is supervisor configuration, initially 90%,
measured on real cargo/Nx/Bun gate jobs and reported. Below it, or with no accounted CPU yet, the leaf is not named and
the sample says why. The rule is the same on Linux and macOS; the unattributed remainder is still stated by
reconciliation.

```rust
pub enum JobLeafAttribution {
    Attributed { leaf: JobProcessLeaf, observed_cpu_permille: u16 },
    Unattributed { observed_cpu_permille: u16, required_permille: u16 },
    NoAccountedCpu,
}
```

A consumer skips a leaf-keyed baseline update for a terminal without an attributed leaf and records the reason the
terminal carries; it never picks a guessed observed process to stand in for work the tree missed.

### Complete job accounting and observation reconciliation

On Linux, the job owns a cgroup v2 from its first owned process through terminal accounting. `cpu.stat` accounts CPU
usage for the job and its descendants, `memory.current`/`memory.peak` retain charged memory, and `io.stat` accounts
storage I/O. Descendants born and reaped between polls still contribute. Cgroup placement precedes their execution;
migrating a process after it ran is not complete accounting. These totals reach the controller and N-API through the
same declaration as process events.

The implemented Linux authority and CPU reader are proven by native fixtures in a sudo-delegated CI scope. Placement is
a controller-owned API: no request field, environment variable or workload action selects the cgroup through that API.
This is accounting separation, not an adversarial sandbox boundary. A workload running as the controller's UID can still
open a writable sibling `cgroup.procs` and migrate itself; denying that access remains a stated Linux sandbox gap. The
real-workspace admission, restart lookup and retirement proof remains blocked on a Linux execution substrate. No
production spawn-path hook is installed before a Linux `SpawnSink` exists.

Charged-memory current/peak is separate from `rssBytes`/`rssPeakBytes`: cgroup memory includes anonymous memory,
file/page cache and kernel charges. It is never relabeled as RSS. Named resource types above have private fields,
checked constructors and checked unit conversions, while their generated wire projection preserves the numeric
representation. Non-finite load/share, invalid core counts and numeric overflow are typed errors, never casts or
truncation. The accounting union identifies each total's source; a source that cannot report storage bytes leaves that
optional observation unavailable, never converts block-operation counts or inserts zero. Lifetime CPU share uses the
accounting source's cumulative user+system microseconds divided by `wallUs`; `cpuPct` remains the latest sampler-window
share and is not that lifetime statistic. A zero-duration observation does not manufacture a ratio or enter a usage
baseline. The cgroup's peak counter is read without resetting it, and missing controllers/counters are typed operational
errors, not zero totals.

The Linux per-process event source is chosen by a measured implementation unit comparing proc connector `CN_PROC`
through the owning privileged Linux helper with a ptrace `TRACEFORK`/`TRACEEXEC`/`TRACEEXIT` seam. Both run the same
fork-heavy workload, measuring complete birth/exec/exit coverage and overhead against an unobserved control. Neither
backend is selected by familiarity or assumed overhead. pidfd identity and proc sampling complement the chosen event
source, not replace it. macOS observes the three kqueue event kinds best-effort, as above, and reconciles CPU against
cumulative leader-own plus reaped-children rusage. That covers the activation interval and the command once each, but it
is not a complete job total. Descendants still running, those exited and not yet reaped, and orphans reparented to
another reaper are missing. An exited leader's held zombie keeps the rusage fixed at its exit, so orphans that run on
after it add nothing. These limits are stated, never presented as exact.

Reconciliation retains the difference between independent job totals and attributed process rows in named units.
Unattributed CPU/storage-I/O emits an explicit typed row on the job span; event loss, unavailable comparison evidence,
counter precision and sampling-window disagreement remain stated, not clamped away. A gap does not erase the independent
job totals. Coverage and leaf attribution remain honest so a partial tree cannot contaminate a baseline. The RED
includes bursts of short-lived grandchildren that fork, exec and exit entirely between coarse polls.

Counter semantics: [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html);
[pidfd events](https://man7.org/linux/man-pages/man2/pidfd_open.2.html) report exit/reap, not fork/exec.

The corresponding span layout and event-to-row projection are defined once in 13_telemetry.md, "Process-tree spans". API
records remain typed; no JSON string column carries a process tree or metric payload.

### Keyed admission and restart attachment

`ExecRequest.admissionKey` is optional for ordinary direct callers and mandatory for an executor admitting an idempotent
operation. Cowshed atomically commits the admission key, exact request identity, and allocated job id within the
immutable workspace incarnation before spawn. A repeated exec with the same key and request answers the existing job,
never a second spawn; the same key with different command, cwd, environment, stdin, sandbox mode, session, or
publication arguments is a typed conflict. The key never grants authority or crosses an incarnation.
`WorkspaceHandle.jobByKey(key)` reaches the admitted job even when the original exec reply was lost.

An executor persists its own operation-to-key binding before dispatch, and its operation-to-job binding before
acknowledging admission. After a crash it looks up the original key or job number, resumes progress/tails, and replays
only a recorded terminal. A missing reply is not evidence that the effect did not run. No attach, restart, or missing
in-memory handle causes an unkeyed exec to repeat. The terminal resource sample remains readable from `sealed` after the
supervisor that ran the job retires.

### Frozen wire projections

- `WorkspaceInfo = { repoId, workspace, workspaceIncarnation, role, mount, state, branch?, baseCommit?, createdAt?, checkpoints, snapshotStale }`;
  `state` is `"attached" | "detached"`. `checkpoints` is always an array of
  `CheckpointInfo = { label, revision, pinned }` facts derived from canonical storage. Detached rows without a cached
  marker snapshot omit all three marker-derived optionals but still report checkpoint facts.
  `MountResult = { workspace, mount, baseCommit? }`; lifecycle creation/restoration fills `baseCommit`, while
  attachment/query results may omit it. `EmptyResult` serializes as exactly `{}`.
- `AdoptOptions.repoId` is optional only because a trusted remote binding can supply it; local-only adoption requires
  the explicit value. `CheckpointOptions = { label?, keep }` carries pin intent without granting quota mutation
  authority. `RemoveOptions.restore` selects the reversible `main` adoption rollback; it is not an alias for forced
  retirement.
- `DoctorReport = { healthy, findings }`; `Finding = { code, severity, message, hint, path? }`, and severity is
  `"info" | "warning" | "error"`. `GcCandidate = { identity: Sha256Digest, path, bytes, reason }`, where reason is the
  closed
  `retiredWorkspace | orphanSessionImage | orphanStagingImage | orphanStagingMetadata | orphanStagingMount | orphanMountpoint | expiredCheckpoint`
  enum. `GcReport = { examined, reclaimed, retainedPinned, retainedActive, freedBytes, dryRun, candidates, deferred }`.
  `deferred` contains `{ path, diagnostic }` for each candidate this run left behind; one deferral does not stop later
  candidates. Dry-run candidates are the exact immutable substrate plan and never mutable handles; execution revalidates
  the plan before the first effect.
- `JobId` is a positive integer no greater than `2^53-1`.
  `JobInfo = { repoId, workspaceIncarnation, jobId, state, pid?, grantRevision, argv | script, failure?, cwd, started, durationMs?, resources?, exit?, stdout, stderr, trace, outputLimit?, stdin }`.
  The command is flattened: exactly one of `argv` and `script` is present, and an `ExecRequest` carries the same field.
  `script = { parts: string[], values: ({word:string} | {words:string[]})[] }` obeys the `ScriptCommand` bounds above.
  `failure` is present only on a `failed` job: `"scriptSyntax"` when its script did not parse (exit
  `{kind:"exited",code:2}`, diagnostic on stderr), and `"supervisorLost"` in the terminal record the next supervisor
  seals for a job its lost predecessor never saw end (11_shell.md). Protected Arrow stores a script in the `argv` column
  as the two arguments `\0script` and the script's JSON, a pair no argv can produce. Every element of `argv` is the
  exact tagged `CommandArg` object `{encoding:"utf8",data:String} | {encoding:"base64",data:String}`. Serialization
  selects `utf8` if and only if the Unix argument bytes are valid UTF-8; otherwise it emits canonical standard base64.
  Decoders deny unknown fields and encodings and reject malformed or non-canonical base64, base64 used for valid UTF-8,
  decoded NUL, arguments above 128 KiB, total argv above 1 MiB, an empty vector or `argv[0]`, and a byte representation
  the host platform cannot reproduce exactly. Validation happens before RPC dispatch, process allocation/spawn, and
  protected-artifact effects. Protected Arrow stores `argv` as a required `List<Binary>` and recovery revalidates each
  raw argument and both bounds. `started` is a full RFC3339 string: `Z` and numeric offsets are accepted. A `:60` value
  is normalized to UTC and accepted only when it denotes a published IERS leap instant (for example
  `2016-12-31T18:59:60-05:00`); local `23:59:60` alone is insufficient, and unannounced future leap seconds reject.
  Calendar, clock, fraction, and offset ranges are validated. `exit` is the discriminated union `{kind:"exited",code}`
  or `{kind:"signaled",signal,coreDumped}`; it is absent before a process result exists. `outputLimit` is present iff
  `state == "outputLimit"`. Both serialization and deserialization enforce these state / duration / exit / output-limit
  invariants. `cwd` is required but nullable on the wire: `null` means the workspace mount root and a string means
  exactly one validated, normalized workspace-relative `WorkspacePath`. Decoders reject an omitted `cwd`; neither `""`
  nor `"."` is a root sentinel, and the same `Option<WorkspacePath>` is preserved by Rust, JSON, CLI, N-API, MCP, and
  Arrow.
- `StreamInfo = { storage, bytes, sha256, summary }` with the exact discriminated unions above. JSON decoders reject
  unknown/multiple discriminants, invalid digest hex, inline data over `MAX_INLINE_OUTPUT_BYTES`, a protected file path
  outside `.cowshed/job/**`, or a redirect source outside the writable workspace. Complete output bytes never appear in
  controller commitments; bounded inline output may appear only in protected Arrow Binary and tagged API JSON.
- `StdinInfo = { kind, bytes, workspacePath?, complete }`; kind is `"empty" | "inline" | "stream" | "workspaceFile"`.
- Every non-root cwd, protected artifact path, redirect source, publication path, and workspace stdin path uses the
  validated relative `WorkspacePath` domain type: no empty component, `.`/`..`, prefix, NUL, or symlink-following open.
  Public capability methods convert non-UTF8 host paths into typed `CowshedError::Usage`; they never pass a `Path` to
  `json!` or panic while constructing a controller request.
- `ProtectedRecord` is exactly `Job(JobArtifactRecord) | CheckpointManifest(CheckpointManifestRecord)`. The Job fields
  are `{repoId,workspaceIncarnation,jobId,sequence,state,grantRevision,stdout,stderr}`. Protected Arrow begins
  `record_kind,record_version,repo_id`; Job uses the frozen flat stream columns, while CheckpointManifest uses
  `{origin_incarnation,barrier_id,visible_jobs,records_sha256}`. Variant-invalid null combinations reject.
- `ControllerCommitment` is exactly the tagged Admission/Terminal/Checkpoint/Fork/Restore union defined above. Its Arrow
  prefix is `commitment_kind,commitment_version,commitment_order,repo_id`; it never contains payload/path/summary
  fields.
- `RevisionTarget` projects as an exact one-key object `{branch}`, `{ref}`, or `{oid}` and rejects ambiguous/multi-key
  objects. `RevisionTarget::parse_cli` classifies lowercase 40/64-hex first as `GitOid`, then any `refs/...` input as a
  validated `GitRef`, and otherwise as a validated `BranchName`. Invalid values return `DtoError`; git rev expressions
  such as `HEAD~1` never fall back to another resolver. `ExpectedRefHead` projects as `{missing:true}` or `{oid}`. Oids
  are validated lowercase 40- or 64-hex strings.
- `AdoptOptions = { path?, capacity?, quarantine }`; `CreateOptions = { revision?, fromWorkspace?, browse, slot? }`;
  `AttachOptions = { browse }`; `RemoveOptions = { force }`; `GcOptions = { dryRun }`; `RebaseOptions`, `LandOptions`,
  and `PushOptions` use the expectation fields shown above. All booleans are explicit in JSON; absence never silently
  means authority was granted.
- `GrantSet`, `GrantDelta`, `PortBlock`, `EgressRule`, `RepoRule`, and `SimVerb` reuse the metadata definitions.
  `GrantSet.portBlock` is present on macOS and omitted on Linux; `GrantSet.retainedPortBlocks` is present only after a
  macOS relocation; `GrantDelta.servicePorts` and `GrantDelta.expectedRevision` are optional. `PortBlock` fields are
  private; `new`, `base()`, and `size()` are the public surface, and custom deserialization invokes the same validation
  so size zero, a size that is not a power of two, a base not aligned to its size, overflow, unknown fields, and
  struct-literal forgery fail.
- `PushReport = { sourceHead, destinationRef, previousDestinationHead? }`;
  `LandReport = { landedHead, targetBranch, previousTargetHead?, targetWasCheckedOut, retired }`;
  `MirrorInfo = { url, mirror }`; `CheckpointQuota = { maxCount, maxBytes }`.
- `GatewayStatus = { installed, running, socket, cliVersion, daemonVersion?, activeWorkspaces, drainCause?, healing?, recovering?, staleDaemon? }`;
  `healing = { mounting: { projects } } | "restoringSessions"` says how far the daemon's startup pass has got — the
  projects it still mounts, never 0, then the sessions it restores from them — and is absent once that pass is over
  (05_gateway.md "Startup contract"); `recovering = { supervisors }` counts the workspace supervisors from before the
  daemon started that it is still recovering, never 0 (11_shell.md "Supervisor recovery").
  `AuditEvent = { timestamp, repoId, workspaceIncarnation, workspace, action, decision, reason?, trace }`.

`JsonEnvelope<T>` has only the private-body constructors `success(T)` → `{"ok":true,"result":T}` and
`failure(CowshedError)` → `{"ok":false,"error":{"code","message","hint"}}`. `T` must implement the sealed core-only
`ResultBody`; `()` and adapter-local maps cannot satisfy it, so `result:null` is unrepresentable and no public enum
variant or arbitrary `ok` boolean can bypass the contract. `EmptyResult {}` is the sole empty success and serializes as
`{}`. The discriminant is also validated when decoding. `CowshedError` is the single structured value
`{code,message,hint}` with stable codes `internal`, `usage`, `not-found`, `conflict`, `environment-missing`,
`sandbox-denied`, and `integrity`. `Integrity` covers missing/altered committed content and invalid complete Arrow
batches; discarding an incomplete trailing batch is successful recovery with a structured recovery report.

## Shell client (`cowshed-shell`)

Types for the warm-shell layer (11_shell.md); `WorkspaceHandle::shell`/`list_jobs` return these.

```rust
pub struct Session { /* immutable fence + exact optional session identity + actor sender */ }
impl Session {
    pub async fn run(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;  // allocates a JobId
    pub async fn background(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;
    pub fn is_named(&self) -> bool;
    pub async fn close(self) -> Result<(), CowshedError>;   // forgets the session's cwd/env overlay
}

/// Session uses the protected/controller record DTOs defined in the frozen API block above.
```

`WorkspaceHandle::exec` sends `"session": null`, the canonical direct-exec shape. `Session::run` and
`Session::background` share the same exec helper and send the session's exact optional identity in `worker.exec`;
`Session::close` uses the same identity and immutable repo/workspace/incarnation fence.

`JobArtifactRecord` is the protected content record consumed by CLI, MCP, NAPI, and CI. It is not the richer `JobInfo`:
the latter remains the live/result API projection. Record and audit-record constructors, serializers, and deserializers
run the same per-row validation; the audit records are telemetry, so there is no cross-record sequence validation — the
facts they describe are held by the image inventory and the protected records themselves.

Every foreground and background submission is the same durable job: attachment is a client state, not a second exec
kind. The allocator commits `JobId` before spawn, so even a pre-spawn failure has a queryable terminal `JobInfo`.
Stdout/stderr remain separate. Small terminal streams are protected inline Arrow Binary; lazy protected files exist only
after promotion. `JobHandle.logs` and attachments read them representation-transparently through
`StreamInfo.storage.artifact`. Summaries are deterministic, bounded, versioned, and redacted, and never determine
denial, exit, policy, or build success.

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
pub struct CowshedError {
    pub code: ErrorCode,
    pub message: String,
    pub hint: String,
    /* otherBuild, fence: optional structured sources, read through accessors */
}
impl CowshedError {
    pub fn other_build_source(&self) -> Option<&OtherBuild>;
    pub fn fence_source(&self) -> Option<&FenceRefusal>;
}

pub enum ErrorCode {
    Internal, EnvironmentMissing, Usage, NotFound, Conflict, SandboxDenied, Integrity,
}

/// Why a rebase or land refused at one of its fences, with what the fence observed. `IncarnationMoved` also types
/// every other exact-incarnation refusal, and `SourceMoved` push's source-head refusal. Wire:
/// `"fence": { "reason": "<camelCase variant>", ...camelCase fields }`, absent on every other error; a reason this
/// build does not know decodes as no fence, never as a lost error.
pub enum FenceRefusal {
    IncarnationMoved { workspace: WorkspaceName, observed: WorkspaceIncarnation },
    SourceMoved { observed: GitOid },
    OntoMoved { observed: GitOid },
    TargetMoved { observed: Option<GitOid> },          // None: the target branch does not exist
    NotFastForward { target_head: GitOid },
    TargetNotCheckedOut { checked_out: Option<String> }, // None: a detached HEAD
    SourceDirty { paths: Vec<WorkspacePath>, total: u64 },
    TargetDirty { paths: Vec<WorkspacePath>, total: u64 },
    ReplayConflicted { rolled_back_to: GitOid },
}
```

Every `FenceRefusal` is a `Conflict` that left the source workspace and the target as they were; the observed value is
what a caller would otherwise read back from the repositories to decide its next move. The dirty variants name at most
`MAX_FENCE_PATHS` (64) UTF-8 paths and count all of them in `total`, so the error always fits one frame. `SourceDirty`
from `land` is `rm`'s reading of work; from `rebase` it is the tracked changes git refuses to rebase over.

The code maps to stable CLI exits `1, 5, 2, 3, 4, 6, 7` respectively; exec-wrapper failures map to
`100, 104, 101, 102, 103, 105, 106` for the same variants. `hint` is always the actionable next step printed on CLI
stderr; known operational failures never panic or hide an unstructured `anyhow::Error` inside the public value.

## cowshed-napi (`@smoothbricks/cowshed`)

napi-rs, async (Tokio runtime owned by the addon), Promise-returning. Node and Bun load the same `.node` addon; a
separate `bun:ffi` ABI is deliberately absent because lifecycle calls are IO-bound and Node cannot consume Bun FFI.
Names follow JS conventions; semantics are 1:1 with cowshed-core. The exception is `ErrorCode`: its serialized `code`
value is the global kebab-case taxonomy token in every adapter (`not-found`, `environment-missing`, `sandbox-denied`).
JS class/method/property names may be camelCase; error code values never are.

The authority split holds across the NAPI boundary too — `Project` is read-only discovery, `Coordinator` is the only
mutation surface, `WorkspaceHandle` is the non-escalating worker capability:

```ts
export interface CoordinatorEndpoint {
  readonly __opaqueCoordinatorEndpoint: unique symbol;
}

/** Takes ownership of fd, sets close-on-exec, and permits exactly one connection attempt. */
export function coordinatorEndpoint(fd: number): CoordinatorEndpoint;
export function openProject(endpoint: CoordinatorEndpoint, path: string): Promise<Project>;

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
  attach(opts?: AttachOptions): Promise<void>;
  grants(): Promise<GrantSet>; // read-only
}
```

The capability split is preserved across the boundary by _how a caller connects, not by a caller-supplied authority
string_. `coordinatorEndpoint` wraps only an already inherited controller socket; it does not mint authority. The
read-only `openProject` consumes that endpoint, completes the peer/nonce handshake, opens the explicit project path,
then discards `CoordinatorToken` before returning `Project`. Reuse fails, and dropping an unused endpoint closes its
descriptor. An embedding process gets such a socket by making a socketpair and starting the host's `cowshed controller`
with one end as its standard input (06_cli.md); the other end is what `coordinatorEndpoint` and `Cowshed::connect` take.
That child is the controller: it is the host's own build, so the daemon starts workspace supervisors for it, which it
would refuse to do for a controller running inside the embedding process (11_shell.md, Protocol).

The mutation surface accepts a fresh endpoint plus project path because the handshake identifies the repository while
`Cowshed::open` still requires the checkout path. `connectWorkspace(workerDescriptor)` consumes a distinct 256-bit,
one-use, 30-second-TTL descriptor minted for exactly one workspace. The in-volume gateway token is not an N-API or MCP
credential.

```ts
export interface WorkerDescriptor {
  readonly __opaqueWorkerDescriptor: unique symbol;
}
export function connectCoordinator(endpoint: CoordinatorEndpoint, path: string): Promise<Coordinator>;
export function connectWorkspace(descriptor: WorkerDescriptor): Promise<WorkspaceHandle>;

/** Positive exact integer, local to one workspace and never reused. */
export type JobId = number;
export type JobStream = 'stdout' | 'stderr';

export interface OutputSummary {
  version: number;
  text: string;
  truncated: boolean;
}

export type CommandArg = { encoding: 'utf8'; data: string } | { encoding: 'base64'; data: string };
// Per argument: 128 KiB decoded. Per argv: 1 MiB decoded. NUL and non-canonical tags reject.

export type BinaryData = { encoding: 'utf8'; data: string } | { encoding: 'base64'; data: string };
// Both branches are bounded by decoded byte length; utf8 is emitted iff the bytes are valid UTF-8.
export type ProtectedOutput = { kind: 'inline'; data: BinaryData } | { kind: 'file'; path: string };
export type OutputStorage =
  { kind: 'captured'; artifact: ProtectedOutput } | { kind: 'redirect'; source: string; artifact: ProtectedOutput };

export interface StreamInfo {
  storage: OutputStorage;
  bytes: number;
  sha256: string;
  summary: OutputSummary;
}

export type ScriptValue = { word: string } | { words: string[] };
export interface ScriptCommand {
  parts: string[]; // exactly one more than values
  values: ScriptValue[];
}
export type JobCommand = { argv: CommandArg[]; script?: never } | { script: ScriptCommand; argv?: never };
export type JobFailure = 'scriptSyntax' | 'supervisorLost';

export type JobInfo = JobCommand & {
  repoId: string;
  workspaceIncarnation: string;
  jobId: JobId;
  state: 'queued' | 'running' | 'exited' | 'signaled' | 'killed' | 'outputLimit' | 'failed';
  pid?: number;
  grantRevision: number;
  failure?: JobFailure;
  cwd: string | null;
  started: string; // canonical RFC3339 wire timestamp, not a JavaScript Date
  durationMs?: number;
  resources?: JobResourceSample; // absent before spawn; generated camelCase projection
  exit?: ExitStatus;
  stdout: StreamInfo;
  stderr: StreamInfo;
  trace: TraceContext;
  stdin: StdinInfo;
  outputLimit?: { limitBytes: number; crossingBytes: number };
};

export interface JobHandle {
  readonly id: JobId;
  status(): Promise<JobInfo>;
  resources(): Promise<JobResourceSample>;
  listeningPorts(): Promise<JobListeningPorts>;
  processes(): Promise<JobProcessTree>;
  processEvents(everyMs: number): AsyncIterable<JobProcessEvent>;
  progress(everyMs: number): AsyncIterable<JobResourceSample>;
  tail(cursor: JobJournalCursor | undefined, limits: JobTailLimits): Promise<JobTail>;
  logs(
    stream: JobStream,
    opts?: { offset?: number; follow?: boolean; signal?: AbortSignal }
  ): AsyncIterable<Uint8Array>;
  attach(opts?: { cursor?: JobJournalCursor; signal?: AbortSignal }): Promise<JobAttachment>;
  detach(): Promise<void>;
  kill(): Promise<void>;
  wait(opts?: { signal?: AbortSignal }): Promise<JobInfo>;
}

export interface JobAttachment {
  readonly stdout: AsyncIterable<Uint8Array>;
  readonly stderr: AsyncIterable<Uint8Array>;
  write(chunk: Uint8Array): Promise<void>;
  end(): Promise<void>; // explicit stdin EOF; never implicit job cancellation
  detach(): Promise<void>; // closes this view; the job continues
}

export interface OutputPublication {
  path: string;
  policy: 'createNew' | 'replace';
}

export type ExecCommand = { argv: string[]; script?: never } | { script: ScriptCommand; argv?: never };
export type ExecRequest = ExecCommand & ExecOptions;

export interface ExecOptions {
  cwd?: string;
  mode?: 'readWrite' | 'readOnly';
  env?: Record<string, string>;
  trace?: TraceContext;
  stdin?: Uint8Array | AsyncIterable<Uint8Array> | { workspaceFile: string };
  stdoutCopy?: OutputPublication;
  stderrCopy?: OutputPublication;
  signal?: AbortSignal;
  onStdout?: (line: string) => void;
  onStderr?: (line: string) => void;
  admissionKey?: string;
}

export interface CheckpointOptions {
  label?: string;
  keep?: boolean;
}

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(request: ExecRequest): Promise<JobHandle>;
  background(request: ExecRequest): Promise<JobHandle>;
  listJobs(): Promise<JobInfo[]>;
  job(id: JobId): Promise<JobHandle>;
  jobByKey(key: string): Promise<JobHandle>;
  checkpoint(opts?: CheckpointOptions): Promise<string>;
  push(opts?: PushOptions): Promise<PushReport>;
  grants(): Promise<GrantSet>; // read-only
}
```

`PushOptions` projects to N-API without weakening its CAS shape: `expectedWorkspaceIncarnation` and `expectedSourceHead`
are optional strings, while `expectedDestinationHead` is the discriminated union `{ missing: true } | { oid: string }`.
`PushReport` returns `sourceHead`, `destinationRef`, and optional `previousDestinationHead`. A conflict rejects the
Promise with `code: "conflict"`; it never silently retries with newer values.

### NAPI streaming (frozen)

One boundary answer, no ambiguity:

- **Protected artifacts are canonical.** Every exec immediately returns a `JobHandle` with a numeric `jobId`; foreground
  and background differ only in attachment. Small terminal streams live inline as protected Arrow Binary; a file is
  created lazily on promotion. `JobHandle.logs` and attachment stream iterables resolve `storage.artifact`
  representation-transparently. `Redirect.source` and `stdoutCopy`/`stderrCopy` publication destinations are never used
  for reads or authority.
- **Command arguments are OS bytes, never text-normalized.** Rust moves `OsString` into `CommandArg`; the controller
  request and `JobInfo` reuse its one exact tagged serde shape, and protected Arrow stores a Binary list. Valid UTF-8
  takes the readable `utf8` branch without base64 allocation. Non-UTF-8 Unix bytes take canonical base64 across JSON,
  are restored before supervisor planning, and are consumed into `OsString` for spawn without `String`,
  `to_string_lossy`, replacement characters, or a second wire union. The 128 KiB argument and 1 MiB argv bounds are
  checked before RPC, spawn, and artifact mutation.
- **JSON is bounded control plus tagged inline data.** Ordinary `JobInfo` may carry `BinaryData` as the exact bounded
  `utf8|base64` tagged union for an inline protected artifact. It never embeds an unbounded stream or invents a path.
  Controller commitments never contain either encoding, protected paths, redirect sources, or other output payload.
- **Bytes, not lines.** Each stream (`stdout`, `stderr`) is an independent `AsyncIterable<Uint8Array>` carrying raw
  bytes — the transport never assumes UTF-8. The `onStdout`/`onStderr` line callbacks in `ExecOptions` are convenience
  sugar over byte iterables and never the only access path.
- **The controller wire has one bounded binary lane.** JSON is length-framed control only. A request or response may
  declare top-level camel-case `binaryLength`, followed by exactly one independent u32-length-prefixed raw frame capped
  at 64 KiB. Inline/stream stdin and attachment writes upload frames. `job.logs` downloads return control metadata
  `{eof,nextOffset}` and one frame; the actor requires `nextOffset == requestedOffset + binaryLength` with checked
  arithmetic. JSON-only methods reject binary metadata; binary methods reject missing, oversized, unsolicited, or
  mismatched frames; each answer's frame is checked against its declaration before its bytes are read.
- **One connection carries concurrent calls.** A client holds one controller connection for every handle it opens, and a
  call can wait as long as a job: `job.wait` until the job ends, a follow read of `job.logs` until the next bytes,
  `job.kill` until the job has stopped. So calls are multiplexed by id: the client sends each call under the next id in
  order, and the controller answers each as it completes, not in arrival order, writing an answer and its frame whole.
  The controller's router resolves such a call's job under its own lock and awaits the job on a task of its own; it
  never holds another request, of any client, while a job gets there. A connection holds at most 64 open calls and reads
  no further request past that. A job's output therefore reaches a client while the client waits for the job's end, and
  a status read is answered while a wait is open on the same connection. A frame that breaks the protocol ends the
  connection and fails every call still open on it with that error.
- **A call may ask to hear its lifecycle steps.** A request that sets top-level `steps: true` gets, ahead of its answer,
  one `{id, step}` frame per step start and end its router call reports — the same steps a lifecycle verb prints on
  stderr (13_telemetry.md): `{event: "started", step, parent?, scope, name}` and `{event: "ended", step, error?}`. Step
  ids are unique within the call, a step's `parent` is the step it runs inside and is always reported started first, and
  a step ends before its parent. Frames are written as the steps happen, so a caller can name the step a slow call is in
  while it is still in it; all of a call's step frames precede its answer. A request without `steps` never gets a step
  frame, so a client that never asks reads only answers. The Rust client exposes this as
  `Coordinator::create_reporting`, which sends each step to a channel the caller reads while the create runs.
- **Post-terminal publication is independent.** `ExecOptions.stdoutCopy` / `stderrCopy` project
  `OutputPublication {path,policy}`. They clone/reflink/copy the sealed protected artifact after terminal state, never
  hardlink, never change `StreamInfo.storage`, and report publication failure separately from process state.
- **Cancellation** is an `AbortSignal` on `ExecOptions`, attachments, and `JobHandle.logs`/`wait`; aborting stops the
  client operation, not the durable job. Callers invoke `JobHandle.kill()` explicitly. A completed explicit kill or
  workspace retirement records `JobState::Killed` while retaining the operating system's actual `ExitStatus`. A child
  that handles SIGTERM may report `Exited`, including code zero; a child terminated by the signal reports `Signaled`.
  Both are valid in `JobInfo` and `ExecRecord`; neither may omit the observed exit status.
- **Binary stdin** is supported without shell interpolation: `ExecOptions.stdin` accepts inline `Uint8Array`,
  backpressured `AsyncIterable<Uint8Array>`, or a workspace-relative file object with the canonical no-follow open.
- **Stdin lifecycle is observable.** Open occurs after `JobId` allocation; EOF closes child stdin once, cancellation
  records incomplete delivery without implicitly killing the job, and events carry metadata but never inline contents.

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
