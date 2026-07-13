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
    pub stdout_copy: Option<OutputPublication>,
    pub stderr_copy: Option<OutputPublication>,
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
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
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

/// Compact controller continuity schema. Every event struct flattens identity and carries `version` + `order`.
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
    pub argv: Vec<String>,
    pub cwd: WorkspacePath,
    pub started: UtcTimestamp,
    pub duration_ms: Option<u64>,
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub trace: TraceContext,
    pub output_limit: Option<OutputLimitInfo>, // present exactly for OutputLimit
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
    pub async fn logs(&self, stream: JobStream, follow: bool)
        -> Result<RawByteStream, CowshedError>; // representation-transparent; always resolves storage.artifact
    pub async fn attach(&self) -> Result<JobAttachment, CowshedError>;
    pub async fn detach(&self) -> Result<(), CowshedError>;          // job continues
    pub async fn wait(&self) -> Result<JobInfo, CowshedError>;
    pub async fn kill(&self) -> Result<(), CowshedError>;            // awaits complete process-tree termination
}

/// A live attachment is only a view over one durable job's raw backing streams.
pub struct JobAttachment { /* JobStdin + two independent RawByteStream handles */ }
impl JobAttachment {
    pub fn into_parts(self) -> (JobStdin, RawByteStream, RawByteStream);
    pub async fn detach(self) -> Result<(), CowshedError>; // closes this view; the job continues
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
pub struct PortBlock { base: u16, size: u16 } // private; `new` and custom JSON decode enforce size 16 + checked end
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
pub enum RevisionTarget {
    Branch(BranchName),   // validated `git check-ref-format --branch` domain
    Ref(GitRef),          // validated fully-qualified `refs/...` domain
    Oid(GitOid),          // validated lowercase 40- or 64-hex; never a land destination
}

pub struct RebaseOptions {
    pub onto: Option<RevisionTarget>,    // default Branch("main")
    pub fresh: bool,
    pub expected_workspace_incarnation: Option<[u8; 16]>,
    pub expected_source_head: Option<Oid>,
    pub expected_onto_head: Option<Oid>,
}

pub struct LandOptions {
    pub target_branch: Option<String>,   // default "main"
    pub check: Option<Vec<String>>,
    pub retire: bool,
    pub push_only: bool,
    pub expected_workspace_incarnation: Option<[u8; 16]>,
    pub expected_source_head: Option<Oid>,
    pub expected_target_head: Option<ExpectedRefHead>,
}

pub struct LandReport {
    pub landed_head: Oid,
    pub target_branch: String,
    pub previous_target_head: Option<Oid>,
    pub target_was_checked_out: bool,
    pub retired: bool,
}
```

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
    pub async fn land(&self, ws: &str, opts: LandOptions) -> Result<LandReport, CowshedError>;
    pub async fn restore(&self, ws: &str, label: &str) -> Result<(), CowshedError>;
    pub async fn detach(&self, ws: &str) -> Result<EmptyResult, CowshedError>;
    pub async fn assign_slot(&self, ws: &str, slot: u32) -> Result<(), CowshedError>;
    pub async fn destroy(&self, ws: &str, opts: RemoveOptions) -> Result<(), CowshedError>;
    pub async fn gc(&self, opts: GcOptions) -> Result<GcReport, CowshedError>;
    pub async fn repo_mirror(&self, ws: &str, url: &Url) -> Result<MirrorInfo, CowshedError>;
    pub async fn set_checkpoint_quota(&self, ws: &str, quota: CheckpointQuota) -> Result<(), CowshedError>;
    /// Hand a worker a capability scoped to exactly one workspace.
    pub async fn worker(&self, ws: &str) -> Result<WorkspaceHandle, CowshedError>;
}

`Coordinator::land` fast-forwards a real `refs/heads/<target_branch>`, not a hidden integration ref. When that target
branch is checked out in the main workspace, the operation updates the checked-out branch through the main workspace so
its `HEAD`, index, and working tree all resolve to `landed_head`; dirty state causes `Conflict`. When the target is not
checked out, Cowshed compare-and-swaps the branch ref without disturbing the main workspace's current checkout. A target
checked out by an unmanaged linked worktree is refused rather than leaving that worktree stale. All expected values are
revalidated under the target lock immediately before the fast-forward. A mismatch or non-fast-forward retains the source
workspace and leaves the target branch and visible working state unchanged.

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
pub enum ExpectedRefHead { Missing, Oid(Oid) }

pub struct PushOptions {
    pub branch: Option<String>,
    pub expected_workspace_incarnation: Option<[u8; 16]>,
    pub expected_source_head: Option<Oid>,
    pub expected_destination_head: Option<ExpectedRefHead>,
}

pub struct PushReport {
    pub source_head: Oid,
    pub destination_ref: String,
    pub previous_destination_head: Option<Oid>,
}
```

The destination is the non-checked-out `refs/cowshed/<ws>/heads/<branch>` ref in the main workspace's standalone
repository/object store. A push fetches and installs the exact `source_head`; it never checks out or advances the Git
`main` branch and never changes the main workspace index or working tree. Each supplied expectation is checked as one
atomic destination-ref update. An incarnation, source-head, or destination-head mismatch is `CowshedError::Conflict`,
leaves the destination unchanged, and does not retire the source workspace. `Missing` lets a caller assert first
publication rather than accepting an overwrite. After success, the source may be retired only when all commits that must
survive are reachable from the returned durable ref. Remote publication and workflow policy are not part of this API.

`WorkspaceHandle::checkpoint` is intentionally available to a worker for retry points, but it is not unbounded storage
authority. Admission atomically enforces checkpoint count/byte quotas, then runs the 02_workspaces.md barrier: pause
admissions/artifact writes, promote running memory-only stream prefixes, fsync protected files, append and fsync the
complete manifest batch, clone while held, and publish the controller manifest-digest/lineage commitment. Quota
exhaustion is `CowshedError::Conflict`; manifest/commitment mismatch is `CowshedError::Integrity`. Restore remains
coordinator-only.

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

Externally projected types are defined **once** in `cowshed-core` and reused verbatim by the CLI (`--json` bodies),
NAPI, and MCP — no adapter redefines a field, and contract goldens (08_testing.md) pin their shapes: `WorkspaceInfo`,
`CheckpointInfo`, `EnsureReport`, `GcReport`, `Finding`, `JobId`, `JobState`, `JobInfo`, `StreamInfo`, `OutputStorage`,
`ProtectedOutput`, `BinaryData`, `OutputSummary`, `OutputPublication`, `PublicationPolicy`, `ControllerCommitment` and
its five event structs, `PushReport`, `LandReport`, `RevisionTarget`, `GrantSet`/`GrantDelta`/`PortBlock`/`EgressRule`/
`RepoRule`/`SimVerb`, `GatewayStatus`, `AuditEvent`, and every `*Options` type (`AdoptOptions`, `CreateOptions`,
`AttachOptions`, `CheckpointOptions`, `RemoveOptions`, `GcOptions`, `RebaseOptions`, `LandOptions`, `PushOptions`).

`JobArtifactRecord`, `ProtectedRecord`, `CheckpointManifestRecord`, and `VisibleJobCommitment` are canonical internal
storage/Arrow contracts, not CLI/N-API/MCP JSON envelopes. Adapters expose only their bounded constituent result types
and payload-free controller commitments; they never reveal protected paths or inline storage records.

Field sketches elsewhere in this spec are illustrative; the freeze rule — one definition, reused, versioned together —
is the contract. `GrantSet.port_block` is the platform union: macOS always `PortBlock`, while Linux carries `None` and
its JSON/N-API projection omits `portBlock`. Adapters and consumers must use that optional shape directly; casts,
`null`, zero-sized blocks, and sentinel base values are forbidden. Adding a field is a coordinated change across core +
goldens, not a per-adapter patch.

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

- `WorkspaceInfo = { repoId, workspace, workspaceIncarnation, role, imageFormat, mount, state, branch?, baseCommit?, createdAt?, checkpoints, snapshotStale }`;
  `state` is `"attached" | "detached"`. `checkpoints` is always an array of
  `CheckpointInfo = { label, revision, pinned }` facts derived from canonical storage. Detached rows without a cached
  marker snapshot omit all three marker-derived optionals but still report checkpoint facts.
- `EnsureReport = { workspace, mount, action }`, where action is `"alreadyMounted" | "attached" | "healed"`.
  `MountResult = { workspace, mount, baseCommit? }`; lifecycle creation/restoration fills `baseCommit`, while
  attachment/query results may omit it. `EmptyResult` serializes as exactly `{}`.
- `AdoptOptions.repoId` is optional only because a trusted remote binding can supply it; local-only adoption requires
  the explicit value. `CheckpointOptions = { label?, keep }` carries pin intent without granting quota mutation
  authority. `RemoveOptions.restore` selects the reversible `main` adoption rollback; it is not an alias for forced
  retirement.
- `DoctorReport = { healthy, findings }`; `Finding = { code, severity, message, hint, path? }`, and severity is
  `"info" | "warning" | "error"`. `GcReport = { examined, reclaimed, retainedPinned, freedBytes, dryRun }`.
- `JobId` is a positive integer no greater than `2^53-1`.
  `JobInfo = { repoId, workspaceIncarnation, jobId, state, pid?, grantRevision, argv, cwd, started, durationMs?, exit?, stdout, stderr, trace, outputLimit?, stdin }`.
  `started` is a full RFC3339 string: `Z` and numeric offsets are accepted. A `:60` value is normalized to UTC and
  accepted only when it denotes a published IERS leap instant (for example `2016-12-31T18:59:60-05:00`); local
  `23:59:60` alone is insufficient, and unannounced future leap seconds reject. Calendar, clock, fraction, and offset
  ranges are validated. `exit` is the discriminated union `{kind:"exited",code}` or
  `{kind:"signaled",signal,coreDumped}`; it is absent before a process result exists. `outputLimit` is present iff
  `state == "outputLimit"`. Both serialization and deserialization enforce these state / duration / exit / output-limit
  invariants.
- `StreamInfo = { storage, bytes, sha256, summary }` with the exact discriminated unions above. JSON decoders reject
  unknown/multiple discriminants, invalid digest hex, inline data over `MAX_INLINE_OUTPUT_BYTES`, a protected file path
  outside `.cowshed/job/**`, or a redirect source outside the writable workspace. Complete output bytes never appear in
  controller commitments; bounded inline output may appear only in protected Arrow Binary and tagged API JSON.
- `StdinInfo = { kind, bytes, workspacePath?, complete }`; kind is `"empty" | "inline" | "stream" | "workspaceFile"`.
  Every cwd, protected artifact path, redirect source, publication path, and workspace stdin path uses the validated
  relative `WorkspacePath` domain type: no root, empty component, `.`/`..`, prefix, NUL, or symlink-following open.
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
  objects. Branch and fully-qualified ref values use validated `BranchName` / `GitRef` domains; raw invalid strings
  cannot be serialized. `ExpectedRefHead` projects as `{missing:true}` or `{oid}`. Oids are validated lowercase 40- or
  64-hex strings.
- `AdoptOptions = { path?, capacity?, quarantine, imageFormat? }`;
  `CreateOptions = { revision?, fromWorkspace?, browse, slot? }`; `AttachOptions = { browse }`;
  `RemoveOptions = { force }`; `GcOptions = { dryRun }`; `RebaseOptions`, `LandOptions`, and `PushOptions` use the
  expectation fields shown above. All booleans are explicit in JSON; absence never silently means authority was granted.
- `GrantSet`, `GrantDelta`, `PortBlock`, `EgressRule`, `RepoRule`, and `SimVerb` reuse the metadata definitions.
  `GrantSet.portBlock` is present on macOS and omitted on Linux; `GrantDelta.expectedRevision` is optional. `PortBlock`
  fields are private; `new`, `base()`, and `size()` are the public surface, and custom deserialization invokes the same
  validation so size zero, any size other than 16, overflow, unknown fields, and struct-literal forgery fail.
- `PushReport = { sourceHead, destinationRef, previousDestinationHead? }`;
  `LandReport = { landedHead, targetBranch, previousTargetHead?, targetWasCheckedOut, retired }`;
  `MirrorInfo = { url, mirror }`; `CheckpointQuota = { maxCount, maxBytes }`.
- `GatewayStatus = { running, socket, cacheEntries, cacheBytes, activeWorkspaces }`.
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
pub struct Session { /* supervisor connection, named or anonymous */ }
impl Session {
    pub async fn run(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;  // allocates a JobId
    pub async fn background(&self, req: ExecRequest) -> Result<JobHandle, CowshedError>;
    pub fn is_named(&self) -> bool;
    pub async fn close(self) -> Result<(), CowshedError>;   // named: persist; anonymous: return to pool
}

/// Session uses the protected/controller record DTOs defined in the frozen API block above.
```

`JobArtifactRecord` is the protected content record consumed by CLI, MCP, NAPI, and CI. It is not the richer `JobInfo`:
the latter remains the live/result API projection. Record and commitment constructors, serializers, and deserializers
run the same validation. `CommitmentPriorContext` plus `validate_commitments` enforce positive global order,
admission-before-single-terminal, known checkpoints, and acyclic incarnation lineage across batches.

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
}

pub enum ErrorCode {
    Internal, EnvironmentMissing, Usage, NotFound, Conflict, SandboxDenied, Integrity,
}
```

The code maps to stable CLI exits `1, 5, 2, 3, 4, 6, 7` respectively; exec-wrapper failures map to
`100, 104, 101, 102, 103, 105, 106` for the same variants. `hint` is always the actionable next step printed on CLI
stderr; known operational failures never panic or hide an unstructured `anyhow::Error` inside the public value.

## cowshed-napi (`@smoothbricks/cowshed`)

napi-rs, async (tokio runtime owned by the addon), Promise-returning. Names follow JS conventions; semantics are 1:1
with cowshed-core. The exception is `ErrorCode`: its serialized `code` value is the global kebab-case taxonomy token in
every adapter (`not-found`, `environment-missing`, `sandbox-denied`). JS class/method/property names may be camelCase;
error code values never are.

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

export interface OutputPublication {
  path: string;
  policy: 'createNew' | 'replace';
}

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
}

export interface CheckpointOptions {
  label?: string;
  keep?: boolean;
}

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(argv: string[], opts?: ExecOptions): Promise<JobHandle>;
  background(argv: string[], opts?: ExecOptions): Promise<JobHandle>;
  listJobs(): Promise<JobInfo[]>;
  job(id: JobId): Promise<JobHandle>;
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
  mismatched frames. The actor serializes frames per connection, and bounded channels preserve cancellation and
  backpressure without JSON arrays or base64.
- **Post-terminal publication is independent.** `ExecOptions.stdoutCopy` / `stderrCopy` project
  `OutputPublication {path,policy}`. They clone/reflink/copy the sealed protected artifact after terminal state, never
  hardlink, never change `StreamInfo.storage`, and report publication failure separately from process state.
- **Cancellation** is an `AbortSignal` on `ExecOptions`, attachments, and `JobHandle.logs`/`wait`; aborting stops the
  client operation, not the durable job. Callers invoke `JobHandle.kill()` explicitly.
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
