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
  grants, and `attach`; it cannot detach, exec, open a shell, mutate lifecycle, or mint a worker capability.
- **`WorkspaceHandle`** — a worker's non-escalating capability over exactly one workspace: exec, shell, job control,
  push, and read-only grant inspection. It has no grant/revoke, destroy/rebase/land/gc, repo-mirror, or cross-workspace
  access.
- **`Coordinator`** — the sole project mutation, diagnostics, and cross-workspace authority: adopt/create/fork,
  grant/revoke, destroy/rebase/land, garbage collection, repository mirroring, doctor, slot assignment, and minting
  one-workspace handles. A trusted unsandboxed controller holds it; workers never do.

`CoordinatorToken` is an affine, opaque, non-serializable proof acquired only by consuming a controller-provided
inherited Unix stream descriptor. Acquisition validates that the descriptor is a connected socket owned by the current
uid, performs the controller's one-use nonce handshake, and binds the resulting token to the returned controller actor
channel and one primary `repoId`. It is neither `Clone` nor `Copy`, has no public constructor or byte/string projection,
and is consumed by `Cowshed::coordinator`. Environment text may identify the inherited descriptor number but is never
itself authority. Missing, reused, wrong-peer, malformed, or cross-project descriptors return a typed `CowshedError`;
there is no fallback token, ambient singleton, filesystem token, or workspace-readable credential.

**Controller handshake.** The client's hello is `{version, nonce}`. The controller answers with the nonce and the
version the client sent: `{version, nonce, repoId, pid}` on a project connection, `{version, nonce, host, pid}` on a
host connection. The current version is 2, and a controller accepts 1 and 2. A version 2 answer carries `pid`, the
controller's own process id, and the connection carries heartbeats (05_gateway.md, "Deadlines"). A version 1 answer
carries neither, so a client of an earlier build reads exactly the answer it always read. A version 2 client refuses an
answer of another version, or one without `pid`. A new client is therefore refused at hello by a controller of an
earlier build, while an old client keeps working against a new controller. The deploy order follows: install cowshed
from SB main first, so the host's controller accepts version 2, and only then bump the cowshed pin of a program that
links the client.

All capability operations are messages to a single-owner controller/supervisor actor. Handles may clone an immutable
sender, but they never share mutable authority state behind a mutex. `WorkspaceRef::attach` is deliberately safe,
idempotent one-workspace attachment; only `Coordinator::detach` may detach because detachment affects running jobs and
controller fencing.

Every `worker.*`, `job.*`, and `session.*` request is fenced by the immutable
`{ repoId, workspace, workspaceIncarnation }` captured when `Coordinator::worker` mints the handle. `JobHandle`,
`JobStdin`, `JobAttachment`, log polling, and `Session` carry that same typed fence; callers never reconstruct it from
strings or paths. Recreating a workspace does not retarget an old handle: the old handle continues to send its original
incarnation and the controller rejects it as stale before effects.

The controller validates that fence from the named workspace's authoritative snapshot, never a project-wide listing. A
missing or corrupt sibling session/grants record cannot refuse another workspace's admission, job observation, stdin,
logs, resources, or session operations. Project enumeration still reports its own inventory faults. Mount assurance and
publication recovery on a workspace request use that same scope: held canonical image locks select the exact workspaces
to recover before any sidecar is read. An opening takes those locks without waiting. A selected lock another process
holds is a live lifecycle operation's, such as a fork's of `main`, and that workspace's publication is left to the
holder or the next opening; with every selected lock held the opening recovers nothing, and never falls back to the
store-wide pass. So no opening, `cowshed ls`'s and `cowshed exec`'s included, waits for another process's fork.
Store-wide recovery is an explicit startup/maintenance pass, not a dependency of a live job's supervisor lookup.

A cold `Workspaces`, `Removal` or `Supervisor` opening observes main and its explicitly selected names, filtering
pending and retired records before reading their sidecars. It additionally observes the invocation-origin workspace only
to validate its host-backed marker against authoritative facts; that extra observation grants no publication-recovery
lock or intent replay. Selected identity, marker, project binding and active-versus-trash checks remain mandatory.
`Store`, `Inspect` and explicit project enumeration retain full inventory reads and their diagnostic failures.

A `Supervisor` opening, the one `cowshed __workspace-supervisor` makes before it serves (11_shell.md, "Supervisor"),
observes exactly what a `Workspaces` opening naming its one workspace observes and finishes nothing, as `Inspect` does:
it replays no intent, `main`'s included, completes no publication, reclaims no trash, finishes no identity change and
writes no binding, so it takes no image lock. The command that asked for the supervisor has already opened under its own
scope and finished what was its; a lock held now is a live lifecycle operation's, such as a fork's, which holds its
source's lock, `main`'s included, across the source's `clonefile`s and seed refresh only, or to its end when it is a
git-worktree fork (16_build_volumes.md, "Fork" step 1). Residue left since stays for that operation, or the next
ordinary command, to finish.

The supervisor's start serves a settled mount without waiting either. With the workspace's image lock free it settles
the mount under that lock as every caller does. With the lock held, a workspace already mounted from its own image, its
owners honoured, carrying its marker, and with its build link absent or kept on a mounted volume of its own is served as
it stands; any other waits for the lock, because only that lock authorizes healing, mounting or relinking it. Before it
waits it says once on stderr which processes hold the lock, by PID and command. The wait is the kernel's `flock` with no
deadline of its own: the manager's start bound (11_shell.md, "Supervisor") is its only one. Every other caller also
waits for the lock before it settles a mount, except gateway startup's heal of main, which waits for none and leaves a
main that needs healing to a background wait (05_gateway.md, "Startup", step 3). Measured 2026-10-22: supervisors whose
openings waited for `main.dmg.lock` behind concurrent forks' clones were killed at the manager's start bound as
supervisors that "did not serve". A start that refused instead while its own workspace's lock was held left its caller
to retry by hand once the holder let go.

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
    pub async fn list(&self) -> Result<WorkspaceListing, CowshedError>; // { workspaces: Vec<WorkspaceRef>, unproven }
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

### Held leaders

`ChildFence` (`cowshed_core::runtime::child_fence`) is the held-leader fence of 11_shell.md "Process identity and signal
ownership", the one fence the workspace supervisor holds its job leaders and activation hosts with and `cowshed hold`
gives a caller that is not cowshed (11_shell.md "Held leaders: `cowshed hold`"). Its decisions are the pure `Hold`
transitions; refusals are values.

```rust
pub struct ChildFence { /* pid, birth, Mutex<{ Hold, recorded exit }> */ }
impl ChildFence {
    /// Spawn `command`, consumed, as the leader of a new session and group this process is outside of.
    pub fn spawn(command: std::process::Command) -> io::Result<ChildFence>;
    pub fn pid(&self) -> i32;
    pub fn birth(&self) -> &Birth;
    pub fn hold(&self) -> Hold;
    /// `kill(-pid, signal)` only while the leader is unreaped and its birth was observed.
    pub fn signal(&self, signal: libc::c_int) -> Result<(), NotSignalled>;
    /// The leader's exit, observed without reaping it unless it was released; the exit the fence
    /// recorded once it reaped it.
    pub async fn exited(&self) -> Result<ExitStatus, CowshedError>;
    /// Reap the leader now if it has exited, else as soon as it is seen exiting.
    pub fn release(&self) -> io::Result<()>;
}
pub enum Hold { Held, Released, Reaped }
pub enum NotSignalled { Reaped { pgid }, Unidentified { pgid, reason }, Refused { pgid, error } }
impl NotSignalled { pub fn into_error(self) -> CowshedError; }
```

A process the caller did not spawn is held by its life instead: `job_groups::Process::of_socket_peer(&socket)` names the
peer of a connected Unix socket by its kernel identity (a pidfd on Linux, its pid version on Darwin), and
`Process::signal` reaches that life or nothing, answering whether it still ran.

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
    pub env: HashMap<String, String>,    // execution env; existing reserved/credential rules apply, record projection below
    pub trace: Option<TraceContext>,     // W3C context; propagated into the job env as TRACEPARENT (13_telemetry.md)
    pub stdin: StdinSource,
    pub stdout_copy: Option<OutputPublication>,
    pub stderr_copy: Option<OutputPublication>,
    pub admission_key: Option<AdmissionKey>, // idempotent admission within the exact workspace incarnation

    pub class: ExecClass,                 // Background by default; Interactive for work its requester awaits;
                                          // Gate requires trusted coordinator (05_gateway.md "Classes")
    pub want: Option<NonZeroUsize>,       // the job's jobserver count, min(want, cores); None: cores
                                          // (05_gateway.md "Parallelism inside a job")
    pub scope: ExecScope,                 // Workspace default; HostGate: coordinator route only, any class
}

/// The exec's own class: one row of 05_gateway.md "Classes" (nice, Linux class cgroup, memory-pressure
/// wait). InteractiveResident is Resident at nice 0 and differs from it in nothing else.
pub enum ExecClass { Background, Interactive, Gate, Resident, InteractiveResident }
// wire: "background" | "interactive" | "gate" | "resident" | "interactive-resident"

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
    CpuAdmission,                        // its admission failed before its command ran: jobserver fifo not made,
                                         // memory pressure unreadable; the stderr names the cause
    Priority,                            // its class nice could not be placed; its command never ran
    Memory,                              // ended at macOS Critical memory pressure (05_gateway.md "Memory pressure")
    Unlaunched,                          // anything else kept its command from launching
}

pub struct OutputPublication {
    pub path: WorkspacePath,             // writable caller-visible destination, never artifact authority
    pub policy: PublicationPolicy,       // CreateNew | Replace
}
pub enum PublicationPolicy { CreateNew, Replace }

/// Binary stdin without shell interpolation. Inline crosses N-API as a byte view; Open as
/// `{ kind: "open" }`, with no reader or automatic EOF. Stream is a Rust backpressured reader.
pub enum StdinSource {
    Empty,
    Inline(Bytes),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
    WorkspaceFile(WorkspacePath),
    Open,
}

pub struct StdinInfo {
    pub kind: StdinKind,                 // empty | inline | stream | workspace-file
    pub bytes: u64,                      // last known fully-written byte in the child's stdin pipe
    pub workspace_path: Option<WorkspacePath>, // normalized relative path; never host-absolute
    pub complete: bool,                  // clean EOF sent once all accepted writes drained
}

/// Raw UTF-8 spelling, 1..=4096 bytes; AdmissionKey::new enforces the invariant. Never hashed.
pub struct AdmissionKey(String);

pub struct TraceContext { pub trace_id: TraceId, pub span_id: SpanId } // validated non-zero lowercase hex

/// Positive, workspace-local monotonic identity allocated for every exec submission.
/// The allocator never reuses a value. Values are capped at 2^53-1 so the same integer is exact
/// in Rust, JSON, and N-API/JavaScript `number`.
pub struct JobId(u64); // constructed with JobId::new; 1..=2^53-1

pub enum JobState { Queued, Running, Draining, Exited, Signaled, Killed, OutputLimit, Failed }

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
    pub resources: Option<JobResourceSample>,
    pub containment: Option<JobContainment>,
    pub outlived: Vec<OutlivedProcess>,
    pub outlived_incomplete: bool,
    pub admission: Option<JobAdmission>,   // raw key and authored identity, present on every keyed job record
}
pub struct JobAdmission {
    pub key: AdmissionKey,
    pub cwd: Option<WorkspacePath>,
    pub mode: RunSandboxMode,
    pub env: BTreeMap<String, String>,      // sorted public-name plaintext projection only, for every scope
    pub env_sha256: Sha256Digest,          // full authored non-withheld env identity; wire envSha256
    pub stdin: AdmittedStdin,
    pub session: AdmittedSession,
    pub stdout_copy: Option<OutputPublication>,
    pub stderr_copy: Option<OutputPublication>,
}
pub enum AdmittedStdin {
    Empty,
    Inline { sha256: Sha256Digest, bytes: u64 },
    Stream,                              // a live reader's bytes are not request identity
    WorkspaceFile(WorkspacePath),
}
pub enum AdmittedSession { None, Unnamed(Uuid), Named(String) }

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

pub struct ForkCommitment {
    pub version: u16, pub order: u64, pub repo_id: RepoId,
    pub source_incarnation: WorkspaceIncarnation, pub destination_incarnation: WorkspaceIncarnation,
}

The `ControllerCommitment` union of these event structs is defined in 13_telemetry.md (Controller audit record schema).

For queued/running/draining `JobInfo`, `StreamInfo` is a current bounded view; its artifact becomes authoritative only when the
corresponding protected batch/file is complete and sealed. Background acknowledgement forces any
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
    pub containment: Option<JobContainment>,
    pub outlived: Vec<OutlivedProcess>,        // omitted on the wire when empty
    pub outlived_incomplete: bool,           // defaults false, omitted on the wire when false
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
    pub async fn progress(&self, every: SampleInterval) -> Result<EventStream<JobProgress>, CowshedError>;
    pub async fn tail(&self, cursor: Option<JobJournalCursor>, limits: JobTailLimits)
        -> Result<JobTail, CowshedError>;
    // From `offset` on: a reader holding the first `offset` bytes continues where it stopped.
    // Without follow, drain the currently written bytes and end even while the job lives.
    // Follow waits on supervisor output notifications past that end, until the job is terminal.
    pub async fn logs(&self, stream: JobStream, offset: u64, follow: bool)
        -> Result<RawByteStream, CowshedError>; // representation-transparent; always resolves storage.artifact
    pub async fn attach(&self, cursor: Option<JobJournalCursor>) -> Result<JobAttachment, CowshedError>;
    pub async fn detach(&self) -> Result<(), CowshedError>;          // job continues; its captured prefixes are durable first
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
    pub loopback_any: bool,              // macOS: jobs may connect to any loopback port; omitted from JSON while false
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
    pub loopback_any: bool,              // macOS: grant sets / revoke clears GrantSet::loopback_any; false = no change
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

`GrantDelta::loopback_any` is the workspace's explicit loopback escape (04_sandbox.md, "`--loopback-any` is the
explicit, separately audited escape"): `grant` with it true records `GrantSet::loopback_any`, `revoke` with it true
clears it, and false changes nothing. It changes the Seatbelt profile, so it advances the grant revision like a
filesystem grant. `ProjectGrantDelta` has no such field, and Linux refuses it. The CLI's `grant --loopback-any` and
`revoke --loopback-any` are its wire counterparts, held to the parity table like every other grant flag.

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
    pub resume: Option<RebaseResume>,    // Continue | Abort the rebase this verb stopped at a conflict
}

pub enum RebaseReport {                  // tagged `kind`
    Rebased { oid: GitOid, onto: GitOid },  // `git rebase` left the branch on `onto` at `oid`
    Aborted { oid: GitOid },
}

pub struct LandOptions {
    pub target_branch: Option<String>,   // default: the branch the target has checked out
    pub check: Option<Vec<String>>,
    pub retire: bool,
    pub push_only: bool,
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    pub expected_source_head: Option<GitOid>,
    pub expected_target_head: Option<ExpectedRefHead>,
    pub ref_lock: Option<RefLockPath>,   // absolute; held only for the head fence and fast-forward (02, land step 3)
    pub ref_turn: Option<LandTurnName>,  // the writers' land-turn queue, `<repo>.ref`; the same span; exclusive with ref_lock (05)
    pub mode: LandMode,                  // Ungated: a lane fold by Git alone, rebasing first; refused into main (02, "Lanes")
    pub preflight: bool,                 // land's fences before its check only, mutating nothing; absent when false (02)
}

#[serde(rename_all = "camelCase")]
pub enum LandMode {                      // default Gated, absent on the wire; `"mode": "ungated"` otherwise
    Gated,
    Ungated,
}

#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LandReport {
    Delivered(PushReport),
    Landed(LandedReport),
    Preflighted(LandPreflight),          // the fences a preflight passed read these; the land pins them (02)
}

pub struct LandPreflight {
    pub workspace_incarnation: WorkspaceIncarnation,
    pub source_head: GitOid,
    pub target_branch: String,
    pub target_head: ExpectedRefHead,
}

pub struct LandedReport {
    pub landed_head: GitOid,
    pub target_branch: String,
    pub previous_target_head: Option<GitOid>,
    pub target_was_checked_out: bool,
    pub retired: bool,
    pub build_volume: LandBuildVolume,
    pub folded: Vec<FoldedWorkspace>,    // the sessions the land folded (02: `land` step 5, "Folds")
}

pub struct FoldedWorkspace {
    pub workspace: WorkspaceName,
    pub head: GitOid,
    pub outcome: FoldOutcome,
}

#[serde(tag = "kind", rename_all = "camelCase")]
pub enum FoldOutcome {
    Retired,
    Kept { reason: FoldKept },
}

#[serde(tag = "kind", rename_all = "camelCase")]
pub enum FoldKept {
    Dirty { files: u64 },                 // tracked changes and untracked files no ignore rule covers
    Refused { code: ErrorCode, message: String }, // the retirement's own refusal: a live owner, a holder, a lease
}
```

```rust
/// The sole mutation, diagnostics, and cross-workspace authority over a project. It is the only capability that can
/// adopt, destroy, fork, grant, revoke, rebase, land, collect garbage, diagnose controller state,
/// mirror repositories, or assign slots.
pub struct Coordinator { /* Project + authenticated controller identity */ }
impl Coordinator {
    // The daemon's refusal of this controller's build (`OtherBuild`), once any call on this
    // connection met it; the remedy is a controller of the daemon's build (11_shell.md "hello").
    pub fn other_build(&self) -> watch::Receiver<Option<OtherBuild>>;
    // Main, and, when `opts.re_adopt` replaced it, what the replacement kept (02: "Re-adoption", step 5):
    // `Adopted { workspace: WorkspaceRef, re_adoption: Option<ReAdoption> }`, where
    // `ReAdoption { replacement: WorkspaceIncarnation, retained: Vec<PathBuf> }` holds paths, never
    // instruction text; the CLI renders the removal command from them.
    pub async fn adopt(&self, opts: AdoptOptions) -> Result<Adopted, CowshedError>;
    pub async fn fork(&self, workspace: &str, source: Option<&str>, opts: ForkOptions) -> Result<WorkspaceRef, CowshedError>;
    pub async fn grant(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    pub async fn revoke(&self, ws: &str, delta: GrantDelta) -> Result<GrantSet, CowshedError>;
    // The project's standing grants (04: reads and egress every workspace runs under).
    pub async fn project_grants(&self) -> Result<ProjectGrants, CowshedError>;
    pub async fn grant_project(&self, delta: ProjectGrantDelta) -> Result<ProjectGrants, CowshedError>;
    pub async fn revoke_project(&self, delta: ProjectGrantDelta) -> Result<ProjectGrants, CowshedError>;
    // `into`: the lane base the workspace lands into, main when None (02: "Lanes").
    pub async fn rebase(&self, ws: &str, into: Option<&WorkspaceRef>, opts: RebaseOptions) -> Result<RebaseReport, CowshedError>;
    pub async fn land(&self, ws: &str, into: Option<&WorkspaceRef>, opts: LandOptions) -> Result<LandReport, CowshedError>;
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
    pub async fn doctor(&self) -> Result<DoctorReport, CowshedError>;
    /// Hand a worker a capability scoped to exactly one workspace.
    pub async fn worker(&self, ws: &str) -> Result<WorkspaceHandle, CowshedError>;
}
```

`Coordinator::land` fast-forwards a real `refs/heads/<target_branch>`, not a hidden integration ref, in the target's
repository: main's, or with `into` the lane base's. When the target's checkout has the target branch checked out, the
operation updates it through that checkout so its `HEAD`, index, and working tree all resolve to `landed_head`, and
dirty state causes `Conflict`. When the target branch is not checked out, Cowshed compares-and-swaps
`refs/heads/<target_branch>` to the validated head without disturbing the checkout's current branch, index, or working
tree; a failed compare-and-swap fails the operation and changes nothing. A target branch held by a linked worktree, or a
detached checkout with no `target_branch`, is refused. All expected values are revalidated under the target lock
immediately before the update. A mismatch or non-fast-forward retains the source workspace and leaves the target branch
and visible working state unchanged.

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
    pub async fn job_by_key(&self, key: AdmissionKey) -> Result<Option<JobHandle>, CowshedError>;
    // An ended job's terminal record and a handle reading its sealed output, also for a job an
    // earlier supervisor of the incarnation ran (11_shell.md "Draining a supervisor of another build").
    pub async fn sealed(&self, id: JobId) -> Result<(SealedJob, JobHandle), CowshedError>;
    pub async fn push(&self, opts: PushOptions) -> Result<PushReport, CowshedError>;
    pub async fn grants(&self) -> Result<GrantSet, CowshedError>;   // read-only: observe, never mutate
    // No grant/revoke, destroy/rebase/land/gc, repo mirror, detach, or cross-workspace access.
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

`JobInfo.state = OutputLimit` is the explicit result when the configurable combined stdout+stderr quota (default 1 GiB)
is crossed. Accounting includes protected and in-flight bytes; the supervisor admits no payload beyond the exact
boundary, terminates the process group, drains pipes, seals artifacts, and only then publishes terminal state. Summaries
remain a separate bounded diagnostic projection.

Authorization, grant, and gateway audit authority remain controller-owned.

The invariant is the same one that keeps grant files outside the volume
([01_storage.md, “Grant files live outside the volume”](01_storage.md#grant-files-live-outside-the-volume)): a
capability reachable from inside a sandbox must not authorize escalation. `WorkspaceHandle` is that principle expressed
in the type system — a subagent holding one cannot grant itself anything.

### Captured-content authority

The shell connection is a reconnectable view over the persistent, permission-checked, multi-client per-workspace
supervisor socket (11_shell.md). Dropping a `Session`, `JobHandle`, or transport never unlinks that socket or stops the
job. Protected complete in-volume Arrow batches and sealed artifacts are the authoritative evidence of a job; controller
commitments are audit telemetry that no API reads for a decision (11_shell.md). APIs return `Integrity` for a missing or
altered artifact, an invalid complete batch, or an incarnation outside the marker lineage; nothing silently wins.
Authorization, grant, and gateway audit authority remain controller-owned. A ranged read of a sealed file returns bytes
only from the generation the store fully verified at seal or open: device, inode, length, mode, link count, and
nanosecond modification and change times. The digest pass observes the same opened descriptor before and after hashing
and accepts evidence only when both generations and the commitment agree. Each bounded positioned read checks that
generation before and after reading; a changed generation is `Integrity`, never altered bytes. Generation evidence is
process-local and published beside the immutable terminal record in the validated snapshot, not persisted or accompanied
by a payload cache. Paging B bytes reads O(B) payload bytes after the required seal/open digest pass, without reading or
hashing preceding prefixes.

The artifact sink's `sealed(jobId)` returns `Result<Option<SealedJobReader>>`: one owned projection sharing the
canonical validated terminal entry and workspace root for both streams. `Ok(None)` means the current protected-store
authority proves no terminal record for that id. Logs and tails use that projection directly, without a discarded
terminal DTO, payload copy or inline revalidation. Only an explicit terminal-facts request materializes the unchanged
wire `SealedJob` DTO. A supervisor-held unsealed job answers that fact before any store refresh; a held sealed terminal
still answers its record. A readable current-incarnation admission row likewise proves that its job has no terminal row,
rather than making that known job an unread-history unknown.

A cache miss refreshes under the existing records lock through the same authoritative snapshot that append uses.
Unchanged validated inode/length within the retained budget reads no record payload and re-hashes no sealed stream.
Changed records validate repository/lineage and row invariants; only newly learned terminal spill files are hashed. Read
and recovery failures retain their causes and never become `None`. Snapshot publication advances readable-job presence,
immutable terminal rows, key authority, record identity together.

Any unread layout can hold numeric job ids at or below its valid retained `records.job-floor`. A missing id above that
floor is proven not to come from the unread store and reaches the ordinary missing-job error. With unread history but no
trustworthy retained floor, every missing id stays unknown; a malformed floor retains its integrity failure. At or below
the floor, the sink refuses with the existing `AdmissionRefusal::Unprovable { setAside }`, carried as typed `Conflict`,
not a fabricated absence. An archived opening-incarnation marker never proves numeric absence: the unread store may
retain lineage rows. Key lookup keeps its narrower keyed-layout/incarnation contract below. No earlier payload is
decoded or migrated. A readable sealed record remains answerable despite unrelated unread history. `sealed`, sealed
monitoring, logs and tails preserve that same complete refusal across the controller/socket boundary; none silently
converts it to `NotFound`, empty bytes or a re-execution.

## DTO freeze (single source of truth)

The canonical API declarations own operation signatures, authority scopes, request/result/event records, units, wire
names, and validation bounds. They generate the controller protocol, N-API adapter signatures, TypeScript public types,
and validators as one surface. There is no separate handwritten N-API request or TypeScript DTO field list. Shared
corpus tests check every generated projection and reject a deliberately changed or omitted field.

Error detail projection follows the canonical Serde object, including omitted and flattened sources. The native adapter
copies its actual serialized own properties, not a second generated property list. Optional flattened named causes
retain their optional public members in TypeScript; absence is not a null intersection. Mutually exclusive cold refusals
may share one private boxed cause without changing the flat cpuAdmission/priority/removal fields, unknown-source
decoding, or borrowed source accessors.

The public declaration IR is serializable from its own Rust types. Its serde and projection metadata and normalized Rust
type graph generate their TypeScript model through the same owner emitter; a consumer never restates those fields or
parses Rust declarations with a private mirror. Normalized types retain qualified names, type/lifetime/const arguments,
reference ownership and constant-expression spelling. Unsupported source syntax is refused by name, and a consumer
requiring a literal bound refuses a symbolic expression rather than guessing its value. Each declaration retains its
exact lexical Rust module, including inline and path-included modules, and its item visibility. Non-public ancestor
modules keep a public item from being exported globally. The module bindings derive from those declarations, and
duplicate names refuse collection rather than selecting the first or last source. Full metadata JSON and owner-emitted
TypeScript model/validator goldens pin every field; the generated Equals validators admit actual owner JSON and refuse
omitted, changed and extra facts. Normal owner generation publishes that model as `src/declaration-ir.generated.ts`,
exposed through `@smoothbricks/cowshed/declaration-ir`. Consumers import these public types through their declared
package dependency; no consumer generator produces an input to its own generic schema library's build.

The declaration IR also retains whether the Rust AST derives `Copy` and its static native byte size. The owner computes
sizes from primitives, literal fixed arrays, tuples, nested records and aliases, including native alignment padding.
Native layouts are finalized once after complete declaration collection, with each declaration evaluated once per pass.
Sequential-discriminant enums carry a tag plus their largest payload; a single-data-arm enum reuses spare discriminant
values in that payload when available. Explicit representations or discriminants, symbolic lengths, unresolved types and
recursive layouts have unknown size. These are native calling-convention facts, not VO wire widths. A consumer that
classifies a native helper by value emits a compile-time `size_of` assertion against the computed size, so a different
compiler layout fails loudly instead of silently changing the calling convention.

The public CLI health summary `GatewayStatus` and the daemon snapshot `DaemonStatus` are distinct declarations with
distinct names. API and support collection share the same duplicate-name refusal; neither source shadows the other. This
naming cutover changes no serialized field or status wire envelope. The control socket and gateway reconciliation
fixtures use the daemon snapshot, not the public CLI health summary.

Scalar projection accessor metadata is the closed get/as-str/display contract owned by the declaration. Opaque numeric
and byte-backed string units use their actual Display, never a guessed as_str or consumer formatter.

Externally projected types are defined **once** in `cowshed-core` and reused verbatim by the CLI (`--json` bodies),
NAPI, and MCP — no adapter redefines a field, and contract goldens (08_testing.md) pin their shapes: `WorkspaceInfo`,
`GcReport`, `Finding`, `JobId`, `JobState`, `JobInfo`, `StreamInfo`, `OutputStorage`, `ProtectedOutput`, `BinaryData`,
`OutputSummary`, `OutputPublication`, `PublicationPolicy`, `ControllerCommitment` and its event structs, `PushReport`,
`LandReport`, `RevisionTarget`, `GrantSet`/`GrantDelta`/`PortBlock`/`EgressRule`/ `RepoRule`/`SimVerb`, `GatewayStatus`,
`AuditEvent`, and every `*Options` type (`AdoptOptions`, `ForkOptions`, `AttachOptions`, `RemoveOptions`, `GcOptions`,
`RebaseOptions`, `LandOptions`, `PushOptions`).

`JobArtifactRecord` is a canonical internal storage/Arrow contract, not CLI/N-API/MCP JSON envelopes. Adapters expose
only their bounded constituent result types and payload-free controller commitments; they never reveal protected paths
or inline storage records.

Field sketches elsewhere in this spec are illustrative; the freeze rule — one definition, reused, versioned together —
is the contract. `GrantSet.port_block` is the platform union: macOS always `PortBlock`, while Linux carries `None` and
its JSON/N-API projection omits `portBlock`. `GrantSet.retained_port_blocks` (`retainedPortBlocks`, omitted when empty)
is controller-written, macOS-only, and non-empty only after a relocation; no `GrantDelta` field sets it. Adapters and
consumers must use that optional shape directly; casts, `null`, zero-sized blocks, and sentinel base values are
forbidden. Adding a field is a coordinated change across core + goldens, not a per-adapter patch. Removing or retyping
one is the same coordinated change — the core declaration, every generated projection and the goldens move together in
one unit — because under Reader projection (below) a dropped field is only ever one the reader never declared: a missing
required field, a retyped value and an unknown enum variant still refuse by name, so no adapter can absorb a removal on
its own and no compatibility shim stands in for the declaration moving.

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
`JobArtifactRecord`/record envelope and Arrow projections live in `cowshed_core::storage::job_artifact` and reuse those
DTOs. Serde uses `camelCase`, documented enum strings, and omission rather than `null`.

### Reader projection

Writers are strict and readers project: a field a writer emits that the reader does not declare is dropped, because a
closed record of an older reader cannot represent it, and the writer's own output keeps it. The controller decodes a
request strictly: an unknown field refuses. A client reads an operation result or stream event of a peer controller,
which may be a newer build, by projecting it through the declaration that result was generated from: an object member
its declared record does not name is dropped, at every depth, and the reader's own strict decode then certifies the
projection. A missing required field, a retyped value and an unknown enum variant are not projected; they still refuse
by name: a variant outside the reader's closed set has no arm to read it as. Projection never adds a value, never
retypes one and never selects an enum arm.

This is one generated rule, not a per-DTO exception. `cowshed-api-gen` derives every peer result's reader from its
declaration with the same serde resolution its TypeScript type uses, so the Rust and TypeScript clients drop the same
members. Rust clients read through `operations::read_result` over `api/reader.generated.rs`; TypeScript adapters read
through `read<Result>` in `reader.generated.ts`, which walks the same derived reader table with `src/reader.ts` before
the result's strict `assert*` validator. Every request, a controller routing a call to itself, and every
`assert*`/`parse*` validator stay strict. A result only internal operations answer never crosses a connection, so its
reader drops nothing.

### Nx state clear

`coordinator.clearNxState` is a coordinator-only, JSON-only operation:
`WorkspaceRequest { repoId: RepoId, workspace: WorkspaceName } -> NxStateClearResult`. No worker, workspace-ref or
project-discovery capability serves it. Rust `Coordinator::clear_nx_state` uses it; the CLI's `cache clear` calls it
before asking the gateway to clear the project's store (06_cli.md, "Cache clear").

`NxStateClearResult` is the named core DTO with `workspace`, `volume` (the cleared build volume), `cacheEntries` and
`workspaceDataEntries`: the entries deleted directly in every `.nx/cache` and `.nx/workspace-data` directory the volume
holds. Held jobs and land work, lifecycle/image locks and other Nx task-database holders refuse as `conflict` and delete
nothing; the storage rules are 16_build_volumes.md, "Clearing a checkout's Nx state". The CLI's JSON result is
`CacheClearReport { workspace: NxStateClearResult, store: NxStoreClearResult { repoId, entries } }`.

### Generated surfaces

Canonical declarations in `cowshed-core/src/api/dto.rs`, `operations.rs` and capability authority generate the public
protocol projections; adapters never maintain request/result field copies. Run `bun nx run cowshed:api-generate` from
the repository root. Its declared generator writes `src/api.generated.ts`, `src/validators.generated.ts`,
`src/reader.generated.ts`, `src/native.generated.ts`, `src/declaration-ir.generated.ts`, `src/cli-parity.generated.ts`,
`crates/cowshed-core/src/api/served.generated.rs`, `crates/cowshed-core/src/api/reader.generated.rs` and
`crates/cowshed-napi/src/operations.generated.rs` under `packages/cowshed`. The underlying write command is
`packages/cowshed/.cache/api-generator/cowshed-api-gen write packages/cowshed`; the target builds that canonical
generator first when needed. Generated files are changed only by this generator. Handwritten TypeScript capability
facades call the generated adapters with the generated DTO types; they do not redefine protocol records. The operation
corpus covers the same operation table; CLI/N-API option parity is its own gate (see "CLI and N-API option parity").

### Job monitoring

`JobInfo.resources` and `SealedJob.resources` carry the latest `JobResourceSample` once the child has spawned, frozen at
terminal publication. They are absent for a queued job or a failure before spawn: no leader or start baseline exists to
sample. Every live resource progress event carries the same type, not a lighter telemetry DTO. `resources()` reads it
without waiting for exit and reports a typed not-ready conflict before spawn; `progress(everyMs)` emits the latest
sample once available, periodic samples even when neither stream advances, then the terminal sample exactly once before
closing. The interval is positive and uses the declaration's bounded duration contract. A slow reader may coalesce
intermediate samples; terminal evidence and journal bytes are never lost.

A job without a terminal resource sample leaves its span's end-row host, volume and charged-memory cells null and states
no `job.unattributed` row (13_telemetry.md, "Process-tree spans"). The supervisor does not resample a boundary for the
span or fabricate a zero sample.

`JobHandle::resources()` and the generated N-API `resources()` call use the declared `job.resources` operation directly,
not a `status()` projection. The supervisor's existing reader takes one canonical sample for a live job and returns its
frozen result after terminal publication; adapters neither resample it nor allocate/decode the unrelated `JobInfo`
fields.

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
pub struct OneCoreCpuPermille(u64);  // a process's share of one core over a window, in thousandths
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
preceding supervisor sample, multiplied by 100; it is one-core normalized and may exceed 100. Supervisor samples are its
own boundaries: the job's first owned process, a new leader, the terminal sample, and each tick of its sampling clock,
which runs every `jobCpuSamplePeriod` while any job owns a process. `jobCpuSamplePeriod` is a supervisor policy value,
initially one second. Reading resources or starting a second subscriber does not reset that window: a read measures from
the open window to its own instant and leaves it open. The first zero-length window reports zero share, not a division
by zero; a later window with no whole elapsed microsecond stays open, so its CPU is measured by the next. RSS is the
simultaneously sampled group sum; peak is its maximum observed sum. `members` retains the complete current group, never
a truncated membership claim; CPU/RSS accounting covers every member. A sample exceeding the generated frame bound is a
typed error, not a silently shortened list. `leaderPid` retains the job's observed leader after exit; terminal
membership may be empty. An incomplete kernel membership read is an operational error, never evidence of an empty group.
On Linux a retained pidfd judges each procfs read after it completes: a process proven departed is no current member,
even when its stat read reports `ESRCH`; a read failure for a process still alive remains an operational error.

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

Rust artifact readers reuse `api::journal_tail::tail_journals` with typed `JobStream` range reads, without opening a
controller. The public pure core is the same implementation the live and sealed controller tails use; readers must
return each requested range exactly. Consumers requiring whole lines use `WholeLineWindow` over that same cursor/range
plan: at most one boundary byte accompanies the bounded window, live trailing partial lines are withheld, and ended
unterminated lines are complete. An over-long line advances by the byte bound. This opt-in cut does not change the raw
controller tail or its wire contract.

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

Attachment stdin uses the same bounded raw-byte lane as exec stdin. Explicit `StdinSource::Open` / wire
`{ kind: "open" }` admits no reader and sends no automatic EOF; its metadata kind is `stream`. `JobStdin.write` splits
input into at most 64 KiB raw frames and awaits each complete write to the child's pipe, not merely admission to a
channel. The delivered cursor advances only over those complete writes. The accepted boundary is the already-checked end
retained at acceptance; moving bytes through the queue, lane and pipe does not recompute it.

`close()` waits for every accepted write, including one the pump owns while its channel slot is free, then sends one
EOF. Repeated close is idempotent and subsequent writes return a typed conflict. A full-range replay is acknowledged
without redelivery only when retained bytes prove it identical; mismatched or older, unprovable input is an explicit
refusal. A partial pump failure returns `stdin: { reason: "deliveryUnknown", cursor }`, naming the last known delivered
prefix, with no safe-retry metadata. No later write or close disguises that uncertainty.

The public N-API `job.attach({ cursor? })` returns an input view whose `write(Uint8Array)`, `end()` and `detach()`
project the core's stateful capability methods through the generator, without a second cursor implementation. Each
JavaScript write preserves its view's byte offset and length and snapshots that supplied view once before the
runtime-thread handoff; its bounded frames then await delivery. EOF, detachment and process-group cancellation are
distinct; neither EOF nor a dropped attachment kills the job.

### Process-tree observations

`processes()` returns `JobProcessTree { jobId, sampledAt, processes, coverage }`, retaining final records of observed
exited descendants as well as current members until the job is sealed. The supervisor observes fork, exec and exit
events. Linux requires an event source beside pidfd identity and proc metrics. Periodic proc children polling alone is
not a complete tree. A missed observation records a typed coverage gap and an unattributed-usage row; it never silently
claims completeness or trusts a reused PID.

macOS per-process coverage is best-effort with a coverage gap, and its job totals carry their own named source limits
(below). kqueue `NOTE_FORK` on each member coalesces and carries no child PID, and `NOTE_TRACK`/`NOTE_CHILD` are refused
with `ENOTSUP` (xnu
[`filt_procattach`](https://github.com/apple-oss-distributions/xnu/blob/ac9718fb1af618d5ce8678d0dc6e8a58f252216f/bsd/kern/kern_event.c#L1094-L1131),
[`filt_procevent`](https://github.com/apple-oss-distributions/xnu/blob/ac9718fb1af618d5ce8678d0dc6e8a58f252216f/bsd/kern/kern_event.c#L1210-L1212),
and the `NOTE_FORK` comment in `bsd/sys/event.h`). The supervisor therefore answers each `NOTE_FORK` by reading the
member's children with `proc_listchildpids` and watches each new one for `NOTE_FORK`/`NOTE_EXEC`/`NOTE_EXIT`
(`NOTE_EXITSTATUS` is accepted for any process the supervisor may signal, grandchildren included). A life it refuses
`NOTE_EXITSTATUS` with `EACCES` -- one the supervisor may not signal, such as a root command a member ran through `sudo`
-- is watched through the same one registration path for `NOTE_FORK`/`NOTE_EXEC`/`NOTE_EXIT` alone, which xnu grants for
every process: its exit is joined, and only its status is withheld (`UnreadExitStatus`), never guessed. A child that
forks, execs and is reaped before that read is missed, and a coalesced `NOTE_FORK` cannot prove how many children it
stood for, so a macOS tree in which a member forked never claims `Complete`. The leader/children rusage source (below)
still counts the missed children's CPU once each parent up to the leader has reaped them, and reconciliation states it
as unattributed usage. An Endpoint Security observer is not used: it needs an Apple entitlement.

The canonical records are:

```rust
pub struct JobProcessTree {
    pub job_id: JobId,
    pub sampled_at: UtcTimestamp,
    pub processes: Vec<JobProcessSample>,
    pub coverage: ProcessCoverage,
}
pub struct JobProcessLeaf { pub pid: u32, pub program: String, pub argv: Vec<CommandArg>, pub truncated: bool }
pub enum ProcessBlockedOn {               // lock detail exists only on a lock: no stale path or holder
    None, Lock { path: String, holder: Option<LockHolder> }, Socket, Pipe, Child, Stdin, Disk,
}
pub struct LockHolder { pub pid: u32, pub job: Option<JobId>, pub program: Option<String> }
pub struct JobProcessSample {
    pub pid: u32,
    pub ppid: u32,
    pub program: Arc<str>,                    // shared with every record of the same exec's image
    pub argv: Arc<[CommandArg]>,              // at most 128 KiB per argument and 1 MiB in all
    pub truncated: bool,                      // the exec's argv exceeded those bounds and was cut to them
    pub born_at: UtcTimestamp,
    pub usage: Option<ProcessUsage>,          // absent until its counters are first read, never zeroes
    pub blocked_on: Option<ProcessBlockedOn>, // absent when not observed, not fabricated `none`
    pub exit: Option<ProcessExit>,
}
pub struct ProcessExit { pub status: Option<ExitStatus>, pub exited_at: UtcTimestamp } // status absent only when withheld
pub const BUSY_CPU_PERMILLE: u32 = 10;     // busy: own CPU over the last sample window ≥ 1 % of one core
pub struct ProcessUsage {                  // the process's own counters, as last read
    pub cpu_user_us: CpuMicros,
    pub cpu_sys_us: CpuMicros,
    pub busy: bool,                        // cpu_window's share ≥ BUSY_CPU_PERMILLE; idle while it is absent
    pub cpu_window: Option<ProcessCpuWindow>, // absent until two reads on the job's clock closed one
    pub rss_bytes: ResidentBytes,          // zero once it has exited
    pub rss_peak_bytes: ResidentBytes,
    pub io: ProcessStorageIo,
}
pub struct ProcessCpuWindow {             // the last closed sample window of the process's own CPU
    pub opened_us: WallMicros,             // on the job's clock: since its first owned process spawned, as wallUs
    pub closed_us: WallMicros,             // equal to opened_us for a window under a microsecond
    pub cpu_permille: OneCoreCpuPermille,  // own user+sys CPU over the exact span in thousandths of a core, rounded down
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
    Gap { reason: ProcessCoverageGap }, // emitted once on the first transition from Complete
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
    UnreadExitStatus { pid: u32 },  // a member's exit was observed but the kernel withheld its status
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

An observation keeps every argv within the bounds a record holds (`MAX_COMMAND_ARG_BYTES`, `MAX_ARGV_BYTES`): an
argument past 128 KiB keeps its first 128 KiB, the argument that crosses 1 MiB in all keeps what fits, later ones are
dropped, and `truncated` says so. macOS bounds only a process's whole argv (`KERN_ARGMAX`), so a legal process can hold
an argument no record could; a record written before this field reads it as `false`.

Each exec's image is held once: the life that runs it, a child forked from it until that child's own exec, and every
record and event of them share it, so a record copies no argv. A concluded job's tree and events are released when it is
sealed; afterwards `processes()`/`processEvents()` answer a conflict naming the sealed record. A subscription opened
before the seal still reads to its end.

Birth and parent identity are kernel observations retained internally, not fresh trust in numeric PID values.
Per-process CPU counters are that process's own usage. Complete job counters have an independent, declared source and
reconcile against the retained process counters; they are not a live-members-only sum. Microseconds convert to
milliseconds once, after aggregation. Storage I/O byte counters follow the kernel source's semantics and never convert
operation counts into invented byte totals or stand in for volume-allocation deltas. An exit's status and time are one
`ProcessExit`, present only after exit; the existing `ExitStatus` union prevents an empty or ambiguous code/signal
result. Its status is absent only when the kernel withheld it, and `UnreadExitStatus.pid` names that member; no code
stands in for it. A gap is absorbing and names the first missed observation. `UncountedFork.pid` is the observed forking
member, not an invented child PID or a missing-child count; `UnreadImage.pid` is the member whose new image was not
read, whether it exited before the read or the read failed, not an invented program or argv. A failed read is also
returned as an observation error with its call and errno; the gap keeps the tree from claiming Complete, and does not
replace the error. Both gaps retain their evidence through the generated controller and N-API records. Blocker detail
fields are valid only for their observed blocker kind; an unobserved blocker is absence or an observation error, never
an assertion that the process is unblocked. A lock observation identifies its path and, when kernel evidence resolves
it, the holder PID and that holder's job. Its optional `program` is present only after an identity-fenced image read; no
program-name guess supplies it.

`usage` is a process's own counters as last read. It is absent, never zero, for a process whose counters were never
read: one reaped before the sampler reached it, or on macOS one that exited before its first read could be proven its
own. Usage is final only when it was read after the process exited. An exit with no such read is the coverage gap
`UnreadFinalUsage`, and the usage stays the last one observed, never zeroed or reset. Each read is fenced to the
retained life. On macOS the first read is proven by the process's unique id read after it, and every later one by the
start time the read itself carries. On Linux the held pidfd must still name an unreaped process after each read.
`cpuWindow` is the supervisor's last closed sample window of the process's own CPU: where it opened and closed, in whole
microseconds on the job's clock (since its first owned process spawned, the origin of a resource sample's `wallUs`, on
the supervisor's monotonic clock), and `cpuPermille`, its own user plus system CPU over the window's exact span in
thousandths of one core, rounded down; a process on several cores exceeds 1000. A share is comparable only with its
window, so the window is part of the value. The observer reads a job's root when it registers it and at its exec, before
the supervisor stamps the job's clock when the spawn returns; a read before the clock started opens no window, and the
first read on it opens the first. Windows tile a life from there, each opening where the one before it closed, so a
reader polling the tree tells a window it already saw (the same `closedUs`) from the next one and from one it missed
(`openedUs` past the last `closedUs`). The share does not depend on `sampledAt` or any other second-resolution
timestamp. `busy` holds when that share is at least `BUSY_CPU_PERMILLE`; that constant is the one declaration consumers
read through the generated surface. A read off the clock, the first read on it and a zero-length window divide nothing
and keep the prior window and judgment, initially none and idle. Reading usage or subscribing never moves the window; a
new window alone is not a change record. Storage I/O follows each platform's per-process source: macOS
`ri_diskio_bytesread`/`ri_diskio_byteswritten`, the I/O the process issued to disk; Linux `/proc/<pid>/io`
`read_bytes`/`write_bytes`, reads it caused to be fetched from storage and pages it dirtied for storage, counted when
dirtied rather than at writeback. Reads served from the cache count on neither. `NotPermitted` is a kernel refusal
(Linux: a process made non-dumpable, for example by a set-id exec); `NotAccounted` is a kernel that keeps no per-process
counters.

`processEvents(everyMs)` emits birth/exec transitions, non-empty changed-state records, one heartbeat per minute for
each unchanged live process, and each process's final usage on exit. The first transition from `Complete` to `Gap` emits
one `Gap { reason }`, without an index for a life the observer may never have seen; later gaps emit nothing and retain
that first reason. The sampling/subscriber interval `everyMs` does not set the heartbeat cadence; changed-state,
coverage-gap and terminal events are immediate. A blocker transition, resident memory crossing a power of two
(`[2ⁿ⁻¹, 2ⁿ)` is one step), a busy/idle CPU flip, and a process's first usage read produce change records; a sample that
only moves counters does not. A live process whose usage no record restated for `PROCESS_HEARTBEAT` gets its own
heartbeat restating its whole record; a blocker change restates no usage, so it does not postpone one. A read taken
after a process exited is withheld from every record until its `Exited` event carries it, once, as final usage: from
that read the process is no longer live, so no heartbeat restates it, no blocker read of it is accepted, and a late exec
event still shows the usage read before the exit. Nothing about that process follows its exit. Before terminal
publication, the job enters nonterminal `draining` and ends its owned membership; its stream ends after the last
retained registration ends. Closing a reader never kills the process. Consumers use these observed facts without
declaring or deriving an expectation from a command's argv. Events name a process by `index`, its record's position in
birth-observation order, never by a pid that another life may reuse. The generated sparse delta changes exactly one
field, usage or blocker; absent is unchanged. A blocker observed afresh, `none` included, is SET, and a blocker no
longer observed is CLEAR. Because lock detail lives inside `Lock`, setting any other blocker leaves no stale path or
holder. No delta shape changes nothing, so neither Rust nor the generated validators admit an empty one.

Events preserve each observed life's transition order. A parent's birth precedes the birth of its observed child.

The `leaf` in `JobResourceSample` is the observed process, live or exited, with the most own CPU (user+system
microseconds), ties broken by birth identity then PID. It does not need a complete tree: it needs the observed processes
to account for most of the job's exact CPU. It is named only when the observed processes' own CPU covers at least a
declared fraction of the accounting source's CPU total; the fraction is supervisor configuration, initially 90%. Below
it, or with no accounted CPU yet, the leaf is not named and the sample says why. The rule is the same on Linux and
macOS; the unattributed remainder is still stated by reconciliation.

Native macOS jobs attribute from the observer's retained, identity-fenced process lives, including final usage of exited
processes. The observer publishes the checked CPU sum and leaf candidate with its immutable tree; a resource read loads
that publication once, compares it with the independent job total, and neither resamples those lives nor moves the
observer's sampling window. A refused or failed current-image read excludes that life from both the sum and leaf until
an identity-fenced image read names it again, even when an earlier gap already describes the tree's coverage.

Without an event observer feeding the sampler, the observed processes are the sampler's census: at each sample boundary
it reads every member of the group, its identity, the image it runs (fenced by the identity read after it) and its own
counters, and keeps each life it found, exited ones with their last counters. A census polls, so its coverage is never
`Complete`: it names the first observation it lacks -- a member it found whose birth no leader spawn accounts for
(`UnobservedBirth`), a member that left before a read after its exit (`UnreadFinalUsage`), a member whose image read the
kernel refused (`UnreadImage`) -- and with none, `UncountedFork` of the current leader, which may have forked between
boundaries. A member whose current image is unread -- never read, or refused or failed since a read named one -- is not
an observed process until a read names its image again: its CPU covers nothing and it is never the leaf. Coverage is the
observed CPU's share of the accounted total, never more than the whole; observed CPU beyond the total stays in the
signed reconciliation.

A Linux job's membership and accounting source are its own cgroup v2, `<class>/job-<incarnation>-<id>` (05_gateway.md
"Classes"). Admission creates it before recording or dispatching the job; the controller's open `Placement` descriptor
moves the first process into it before exec (or before a pooled host interprets a script). A supervisor without
delegation refuses admission with `NotDelegated`, naming the unwritable directory, `cgroup.procs`, or
`cgroup.subtree_control`. A cgroup that cannot be created, or whose `cgroup.kill` cannot be opened, likewise refuses the
job; there is no uncontained fallback. Changing sessions, including after the parent exits, does not change membership.

A command the controller runs outside any admitted job has a leaf of its own beside the jobs of its class,
`<class>/run-<incarnation>-<controller pid>-<n>`: discovery's Cargo, which runs while the controller admits a job's
build state and so before any job of that workspace exists, and the Nx daemon retirement of a landing workspace, which
runs after its supervisor has stopped. The controller that holds the delegation allocates it (the first `n` its pid has
not taken), its child enters it before exec, and its membership and CPU are read from it as a job leaf's are
(`cgroup.procs`, `cpu.stat`), so the run's progress bound (05_gateway.md "Deadlines") sees every member wherever it was
reparented. Once the run ends the leaf is killed, waited empty within the reap bound and removed; a leaf whose
controller no longer runs is retired by the next run of its incarnation, and a leaf another run retired first is done,
not a failure. A census of jobs passes run leaves over. A declared land's after-land job, which runs its push and after
commands one after another, gives each its own leaf inside its job cgroup (`after-<log>`, 02_workspaces.md "A declared
land" step 6): the job's cgroup enables no controller for its children, so it holds the job's own process and the leaves
alike, and a command's leftovers are never counted as the next one's.

Terminal retirement signals the cgroup's observed members through pidfds, grants TERM one grace deadline, then writes
`cgroup.kill` if membership remains. The job seals only after `cgroup.events` reads `populated 0`; `poll(POLLPRI)` wakes
that wait, never a periodic timer. Retained member pidfds are then joined with `poll(POLLIN)` too: Linux's `cgroup_exit`
precedes `exit_notify`, so the empty-cgroup notification can precede pidfd readability. Both waits after the kill share
one reap-class deadline (05_gateway.md, "Deadlines", 5 s), and finalization's pidfd join takes the same bound. Past it
retirement fails `TIMEOUT` (`CgroupError::Unreaped`) naming the cgroup and each member still alive with its pid, image
and kernel state: a member in an uninterruptible kernel wait (`D`), which SIGKILL ends only once its call returns, is
named as such and left as it is. The job then keeps its claims and stays nonterminal, and an explicit retry retires it
once that member has exited. The terminal receipt reports kernel containment only after those proofs. `cgroup.kill`
requires Linux 5.14; the supervisor's peer-pidfd protocol sets the overall floor at Linux 6.5 (11_shell.md). Below the
relevant capability floor, missing interfaces or failed peer-pidfd acquisition are operational refusals, never a
downgrade to process groups. Linux acceptance must run in a delegated scope on a real Linux kernel; compilation or a
Darwin run is not evidence of Linux exit behavior.

The actor retains the admitted cgroup even before spawn. After terminal sampling it reads final counters, checks the
directory identity, and removes the empty hierarchy before releasing the claim. This does not depend on the last exec
host's shared descriptor closing; that descriptor then names a removed cgroup and cannot place a later command. A queued
cancellation or refused spawn removes its empty admission by the same path.

Successful retirement and finalization are single-flight and cached for explicit retries and late host Drop; a failed
final counter read cannot consume the signal rows. `Kernel` proves empty membership, not an exhaustive member census:
`cgroup.kill` returns no killed-member list. Every atomic kill of populated membership therefore sets
`outlivedIncomplete`, even when the pre-signal pidfd snapshot supplied rows, because concurrent forks may have been
ended without a row. Empty rows without this marker mean no member was ended by retirement; the marker preserves the
distinction without freezing the job. Metadata gaps do not prevent killing, joining, or sealing proven-empty membership.

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

The Linux authority and CPU reader are proven by native fixtures in a sudo-delegated CI scope. Placement is a
controller-owned API: no request field, environment variable or workload action selects the cgroup through that API.
This is accounting separation, not an adversarial sandbox boundary. A workload running as the controller's UID can still
open a writable sibling `cgroup.procs` and migrate itself.

Charged-memory current/peak is separate from `rssBytes`/`rssPeakBytes`: cgroup memory includes anonymous memory,
file/page cache and kernel charges. It is never relabeled as RSS, and the job span's end row states it in columns of its
own (13_telemetry.md). Named resource types above have private fields, checked constructors and checked unit
conversions, while their generated wire projection preserves the numeric representation. Non-finite load/share, invalid
core counts and numeric overflow are typed errors, never casts or truncation. The accounting union identifies each
total's source; a source that cannot report storage bytes leaves that optional observation unavailable, never converts
block-operation counts or inserts zero. Lifetime CPU share uses the accounting source's cumulative user+system
microseconds divided by `wallUs`; `cpuPct` remains the latest sampler-window share and is not that lifetime statistic. A
zero-duration observation does not manufacture a ratio or enter a usage baseline. The cgroup's peak counter is read
without resetting it, and missing controllers/counters are typed operational errors, not zero totals.

The delegated Linux accounting fixture keeps a root setup/cleanup parent in its non-job controller leaf and runs all
accounting workloads as the ordinary runner. Cgroup delegation grants cgroupfs authority, not mount authority; role
receipts record the actual UID, effective capabilities, mount namespace and kernel rather than inferring privileges. The
setup parent consumes only runner-declared root-only loop-control and finite loop block nodes, validates their kernel
device numbers and mode, and binds its own sparse ext4 image atomically with `LOOP_CONFIGURE`/`AUTOCLEAR`. It selects
the narrow classic ext4 mount operation with command-local `LIBMOUNT_FORCE_MOUNT2=always`. The controller receives the
runner-owned mountpoint, not the device capability; a real denied loop-control open proves that separation. The parent
holds the loop descriptor until kernel-proven unmount; closing it does not itself prove deferred device retirement. A
fresh `IN_CLOSE_WRITE` watch is armed on the exact opened backing inode through `/proc/self/fd`, after mkfs and all
earlier writable opens finish and before `LOOP_CONFIGURE`. The local backing descriptor is dropped after configuration;
no later independent writable image open is allowed. Linux's loop teardown sets `Lo_unbound` before putting its retained
backing file, whose final put emits the close notification
([loop.c](https://github.com/torvalds/linux/blob/v6.18/drivers/block/loop.c),
[file_table.c](https://github.com/torvalds/linux/blob/v6.18/fs/file_table.c)). After proven unmount and owned loop
close, the fixture performs one blocking read of that watch: exactly one complete 16-byte nameless `inotify_event` must
match this watch descriptor, an `IN_CLOSE_WRITE` mask and a zero cookie, then one independent device/backing
verification runs. Overflow, watch loss, rename/unlink, a short or misframed record, or failed delivery is an error,
never completion and never retried. No timed sleep, status retry or new deadline substitutes for the kernel event.
Cleanup observes the target's kernel mount ID and device/backing identity, including when mount reports failure after
acquiring it; an unmount command's status is reconciled with that kernel state before the image can be removed. Explicit
cleanup returns every release and diagnostic-write failure. Destructor fallback attempts the same safe steps without
panicking, then attempts fallible stderr/stdout reporting; delivery is unavailable if both existing channels fail.
Failure controls exercise refused commands before and after mounting, including a setup refusal after actual mount
acquisition.

The fixture compares `memory.current`/`memory.peak` with direct reads and verifies regular-file cache separately from
shmem using `memory.stat`'s `file - shmem`. Direct-I/O workloads require exact independent `io.stat` parity, excluding
cached reads, unrelated jobs and volume-allocation proxies. `/proc/pid/io` includes waited-for children, so the missing
live-process control runs after the complete tree is reaped, alongside retained terminal counters. Allocation and
deletion metadata are flushed at the held/terminal boundaries without timing waits. Each direct-I/O workload
synchronizes its parent directory after unlink, before the child can be reaped and its parent can report held readiness.
CPU comparisons collect each selected child's full `wait4` lifetime; a pre-report sample cannot include its report and
process teardown. Unavailable loop/mount authority remains a failure, never a skip or permission widening. Tmpfs can
measure anonymous/shmem charging only: it does not prove regular-file page-cache or storage-I/O attribution. At the held
boundary, each live-process control requires the directly owned, unreaped parent to be the sole cgroup member and reads
every stated counter. Unavailable membership, unreadable proc records or absent CPU/RSS/I/O keys fail the observation
rather than becoming a zero-valued proxy; both proc I/O byte keys are required. Terminal census zero is accepted only
after kernel terminal evidence and an independently read empty membership.

The Linux per-process event source is chosen by a measurement comparing proc connector `CN_PROC` through the owning
privileged Linux helper with a ptrace `TRACEFORK`/`TRACEEXEC`/`TRACEEXIT` seam. Both run the same fork-heavy workload,
measuring complete birth/exec/exit coverage and overhead against an unobserved control. Neither backend is selected by
familiarity or assumed overhead. pidfd identity and proc sampling complement the chosen event source, not replace it.
macOS observes the three kqueue event kinds best-effort, as above, and reconciles CPU against cumulative leader-own plus
reaped-children rusage. That covers the activation interval and the command once each, but it is not a complete job
total. Descendants still running, those exited and not yet reaped, and orphans reparented to another reaper are missing.
An exited leader's held zombie keeps the rusage fixed at its exit, so orphans that run on after it add nothing. These
limits are stated, never presented as exact. xnu does not keep each `proc_pid_rusage` CPU counter from falling between
two reads (a job leader's own user CPU has read 239 µs, then 222 µs). CPU spent is never unspent, so such a dip is the
platform's, never an inconsistency of the job. The leader source and each process row hold every CPU counter at the
highest value read of it, so a cumulative total never falls and no window counts negative CPU. A total may therefore
exceed the final reading by at most the deepest dip.

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

`ExecRequest.admissionKey` is a raw string of 1 to 4096 UTF-8 bytes, compared byte for byte. It is optional for ordinary
direct callers and mandatory for an executor admitting an idempotent operation. Before spawn, cowshed commits the key,
the authored request identity and the allocated job id in the job's first record, under the lock ordering the workspace
incarnation's records. The identity is the request before a session lends it a cwd or environment: command, cwd, the
full authored environment less withheld credential names, sandbox mode, session, publications, stdin source and
scheduling: scope, class and the CPU want as the request named it, or none. An unnamed want resolves to the background
share of whichever supervisor admits it, which may differ from one supervisor to the next, so the record keeps the named
want (`admission_want`) beside the resolved one (`cpu_want`) and a repeat compares the named one. Measured 2026-10-21 in
a land gate with the resolved want compared: a replay was refused as a want change. The environment binds the key
through the typed envSha256 commitment, not the public plaintext projection recorded in JobAdmission.env. Custom/private
changes therefore remain Env keyConflict after a restart without storing their values. Inline input is identified by its
SHA-256 and length, a workspace file by its path, and a stream only by its kind, never its bytes. Named sessions compare
by name; unnamed sessions carry a distinct UUID, including across supervisor restart. The command remains the record's
own, not a second copy inside admission metadata.

Open remains distinct from Stream on the wire and has no live reader, while its durable stdin identity uses the existing
stream kind. A repeated keyed Open answers the same job before or after termination, starts no reader and sends no EOF.
Only a repeat that actually supplies a Stream reader receives `StdinBound`; `jobByKey` can recover the handle used to
attach and write.

A repeated exec with the same key and request answers the existing job and spawns nothing. A changed request is a typed
`Conflict`, `admission: { reason: "keyConflict", jobId, fields }`. The first stream admission binds its reader; a repeat
carrying another reader returns typed `Usage`, `stdin already bound to job N; attach to write`, with
`admission: { reason: "stdinBound", jobId }`. The refused reader is never polled.

`WorkspaceHandle.job_by_key(key)` reads the admitted job id from durable records even after a lost reply or supervisor
restart. It returns `Result<Option<JobHandle>, CowshedError>`; the TypeScript `jobByKey(key)` returns
`Promise<JobHandle | null>`. Only `AdmissionLookup::Absent`, proven under the current incarnation and store authority,
returns `None` / `null`. A missing workspace or job, stale incarnation or revision, transport failure, unreadable
history, and storage or observation failure remain errors with their original causes, never absence. A keyed store set
aside while it may have served this incarnation makes unreadable keys unprovable, so lookup and exec return typed
`Conflict`, `admission: { reason: "unprovable", setAside }`, never absence or a spawn. A missing archived incarnation
marker leaves its history unprovable; a malformed marker is an integrity failure, never proof of absence. The key grants
no authority and never crosses an incarnation. A successful root JSON `null` is distinct from an omitted `result`, which
remains an invalid response envelope.

Record layout 8 preserves layout 7's resource, accounting and volume columns, then appends the fourteen admission
columns. Layout 9 preserves layout 8 and appends the sample's CPU share, leaf and reconciliation and a cgroup source's
charged memory (13_telemetry.md). Native error causes are object projections generated from the canonical `CowshedError`
declaration, not JSON strings or parsed message text; the TypeScript facade validates the generated type directly.

An executor persists its own operation-to-key binding before dispatch, and its operation-to-job binding before
acknowledging admission. After a crash it looks up the original key or job number, resumes progress/tails, and replays
only a recorded terminal. A missing reply is not evidence that the effect did not run. No attach, restart, or missing
in-memory handle causes an unkeyed exec to repeat. The terminal resource sample remains readable from `sealed` after the
supervisor that ran the job retires.

### Frozen wire projections

- `WorkspaceInfo = { repoId, workspace, workspaceIncarnation, role, mount, state, branch?, baseCommit?, createdAt?, snapshotStale }`;
  `state` is `"attached" | "detached"`. Detached rows without a cached marker snapshot omit all three marker-derived
  optionals. `MountResult = { workspace, mount, baseCommit? }`; lifecycle creation fills `baseCommit`, while
  attachment/query results may omit it. `EmptyResult` serializes as exactly `{}`.
- `UnprovenWorkspace = { workspace, workspaceIncarnation, mount, reason }` names a published workspace whose mount does
  not prove itself the workspace's (01a_apfs.md, "Ownership, identity, and the volume label"). It is neither attached
  nor detached, so it has no `WorkspaceInfo` row; `mount` is the canonical path its row would carry and `reason` is the
  refusal's text. A listing never fails for one: `project.list` answers
  `WorkspaceListView = { workspaces: WorkspaceView[], unproven: UnprovenWorkspace[] }`, `Project::list` the matching
  `WorkspaceListing`, the facade's `listWorkspaces()` `{ workspaces: WorkspaceInfo[], unproven: UnprovenWorkspace[] }`,
  and `ProjectWorkspaces = { repoId, workspaces, unproven? }` (`ls --all --json`) omits `unproven` when it is empty. A
  read of that one workspace still refuses with its refusal. `project.list` answered a bare `WorkspaceView[]` before;
  the object is a breaking wire change, and a client of an older build cannot decode it. A consumer that keys on the
  listed workspaces counts an unproven one as present: it is the project's, and only its mount is in doubt.
- `AdoptOptions.repoId` is optional only because a trusted remote binding can supply it; local-only adoption requires
  the explicit value. `RemoveOptions.restore` selects the reversible `main` adoption rollback; it is not an alias for
  forced retirement.
- `DoctorReport = { healthy, findings }`; `Finding = { code, severity, message, hint, path? }`, and severity is
  `"info" | "warning" | "error"`. `GcCandidate = { identity: Sha256Digest, path, bytes, reason }`, where reason is the
  closed
  `retiredWorkspace | orphanSessionImage | orphanStagingImage | orphanStagingMetadata | orphanStagingMount | orphanMountpoint | abandonedBundle`
  enum. `GcReport = { examined, reclaimed, retainedActive, releasedBytes, dryRun, candidates, deferred }`. `reclaimed`
  counts the candidates the run collected and `releasedBytes` the allocated bytes of the images among them that it
  released for deletion; their background deletions free the space after the report (05_gateway.md, "Deleting released
  images"). `deferred` contains `{ path, diagnostic }` for each candidate this run left behind; one deferral does not
  stop later candidates. Dry-run candidates are the exact immutable substrate plan and never mutable handles; execution
  revalidates the plan under the same sweep options before the first effect. `abandonedBundle` names a regular recovery
  bundle in `sessions/.trash` only when `GcOptions.abandoned` is explicitly true; a default sweep never names or
  reclaims one.
- `JobId` is a positive integer no greater than `2^53-1`.
  `JobInfo = { repoId, workspaceIncarnation, jobId, state, pid?, grantRevision, argv | script, failure?, cwd, started, durationMs?, resources?, containment?, outlived?, outlivedIncomplete?, exit?, stdout, stderr, trace, outputLimit?, stdin }`.
  The command is flattened: exactly one of `argv` and `script` is present, and an `ExecRequest` carries the same field.
  `script = { parts: string[], values: ({word:string} | {words:string[]})[] }` obeys the `ScriptCommand` bounds above.
  `failure` is present only on a `failed` job: `"scriptSyntax"` when its script did not parse (exit
  `{kind:"exited",code:2}`, diagnostic on stderr), `"supervisorLost"` in the terminal record the next supervisor seals
  for a job its lost predecessor never saw end (11_shell.md), `"memory"` for a background or resident job ended at macOS
  Critical memory pressure (05_gateway.md "Memory pressure"), and, for a job whose command never ran, `"cpuAdmission"`
  or `"priority"` (its jobserver fifo or memory-pressure read, or its class nice, failed) or `"unlaunched"` (anything
  else kept the command from launching: a program, `#!` interpreter or `env` name proven absent, a held child refused
  before its exec, a host that could not start it). A never-launched job's exit is the refusal's wrapper code; a host
  process it owned meanwhile does not make its command one that ran, so `failure`, never a `pid` or a sample, says it
  never ran. The one exception keeps its own shape: a cold activation that itself fails ends the job with the
  activation's status and no `failure` (11_shell.md, "Activation belongs to the job that waited on it"). Protected Arrow
  stores a script in the `argv` column as the two arguments `\0script` and the script's JSON, a pair no argv can
  produce. Every element of `argv` is the exact tagged `CommandArg` object
  `{encoding:"utf8",data:String} | {encoding:"base64",data:String}`. Serialization selects `utf8` if and only if the
  Unix argument bytes are valid UTF-8; otherwise it emits canonical standard base64. The controller's request decoder
  denies unknown fields; a result reader projects them (Reader projection). Every decoder denies unknown encodings and
  rejects malformed or non-canonical base64, base64 used for valid UTF-8, decoded NUL, arguments above 128 KiB, total
  argv above 1 MiB, an empty vector or `argv[0]`, and a byte representation the host platform cannot reproduce exactly.
  Validation happens before RPC dispatch, process allocation/spawn, and protected-artifact effects. Protected Arrow
  stores `argv` as a required `List<Binary>` and recovery revalidates each raw argument and both bounds. `started` is a
  full RFC3339 string: `Z` and numeric offsets are accepted. A `:60` value is normalized to UTC and accepted only when
  it denotes a published IERS leap instant (for example `2016-12-31T18:59:60-05:00`); local `23:59:60` alone is
  insufficient, and unannounced future leap seconds reject. Calendar, clock, fraction, and offset ranges are validated.
  `exit` is the discriminated union `{kind:"exited",code}` or `{kind:"signaled",signal,coreDumped}`; it is absent before
  a process result exists. `outputLimit` is present iff `state == "outputLimit"`. Both serialization and deserialization
  enforce these state / duration / exit / output-limit invariants. `cwd` is required but nullable on the wire: `null`
  means the workspace mount root and a string means exactly one validated, normalized workspace-relative
  `WorkspacePath`. Decoders reject an omitted `cwd`; neither `""` nor `"."` is a root sentinel, and the same
  `Option<WorkspacePath>` is preserved by Rust, JSON, CLI, N-API, MCP, and Arrow. `draining` is nonterminal: the direct
  child may have exited, but the actor still holds the process, CPU and volume claims until membership retirement
  succeeds. `containment = {kind:"kernel"} | {kind:"observed",lost:ProcessCoverageGap[]}` records the successful
  membership proof, not a promise at admission. Darwin always reports `observed`; even an empty `lost` cannot prove that
  coalesced fork events missed no life. `outlived` records the live members retirement signalled, including a live
  leader on cancellation or a live subtree root, as `{index?:number,pid:number,image?:string,signal:number}`. A
  successful receipt names processes already ended, never survivors left running. `index` absent means its observed-tree
  index is unknown, not that it had no index; `image` absent means no identity-fenced image read succeeded. No synthetic
  index or empty image fills either gap. `signal` is the last delivered retirement signal. `outlivedIncomplete` defaults
  to false and is omitted when false; true means retirement may have ended members absent from `outlived`. Kernel-proven
  empty membership does not prove exhaustive metadata. An empty `outlived` means the operation ended no processes only
  when this marker is false. A true marker requires containment and, where the DTO carries one, a spawned pid; receipts
  are stated only while draining or terminal. A refused/never-spawned job has no containment, no rows and a false
  marker, never an invented kernel proof. The sealed record retains the receipt unchanged.
  `JobSubtreeHandle = {jobId,index,incarnation}` is opaque: the monitor issues a random 32-lower-hex nonce for the
  authenticated retained root, not a client birth identity. Changing its job, index or nonce cannot select another root.
  `JobSubtreeAdmission = {kind:"admitted",handle} | {kind:"refused",reason}` and
  `JobSubtreeRetirement = {kind:"retired",retirement:JobProcessRetirement} | {kind:"refused",reason}` are closed tagged
  unions; reasons are `foldUnavailable`, `notMember`, `foreignHandle`, `jobEnded` and `spentHandle`. A successful
  retirement consumes the root capability; replay is `spentHandle`, while native failure remains an error and can be
  retried. A naturally ended, previously unspent subtree returns an empty receipt on first retirement. Subtree scope is
  the retained root and its descendants through immutable parent identities, not siblings or an expanded whole-job
  process group. Linux without an observed fold refuses with `foldUnavailable`; no RAM lease substitutes for one. Every
  subtree DTO is closed: its writer emits exactly its fields, and a result reader projects it (Reader projection).
- `StreamInfo = { storage, bytes, sha256, summary }` with the exact discriminated unions above. JSON decoders reject
  unknown/multiple discriminants, invalid digest hex, inline data over `MAX_INLINE_OUTPUT_BYTES`, a protected file path
  outside `.cowshed/job/**`, or a redirect source outside the writable workspace. Complete output bytes never appear in
  controller commitments; bounded inline output may appear only in protected Arrow Binary and tagged API JSON.
- `StdinInfo = { kind, bytes, workspacePath?, complete }`; kind is `"empty" | "inline" | "stream" | "workspaceFile"`.
- Every non-root cwd, protected artifact path, redirect source, publication path, and workspace stdin path uses the
  validated relative `WorkspacePath` domain type: no empty component, `.`/`..`, prefix, NUL, or symlink-following open.
  Public capability methods convert non-UTF8 host paths into typed `CowshedError::Usage`; they never pass a `Path` to
  `json!` or panic while constructing a controller request.
- The protected stream holds `JobArtifactRecord` rows, and `record_kind` has one live value, `job`. The Job fields are
  `{repoId,workspaceIncarnation,jobId,sequence,state,grantRevision,stdout,stderr}`. Protected Arrow begins
  `record_kind,record_version,repo_id`; Job uses the frozen flat stream columns. The protected-record columns
  `origin_incarnation`, `barrier_id`, `visible_jobs` and `records_sha256` are retired and always null; the
  `checkpointManifest` record kind is retired, and recovery passes over a frame of that kind (it names no job). Null
  combinations invalid for a `job` row reject.
- `ControllerCommitment` is the tagged union of event structs defined in 13_telemetry.md. Its Arrow prefix is
  `commitment_kind,commitment_version,commitment_order,repo_id`; it never contains payload/path/summary fields.
- `RevisionTarget` projects as an exact one-key object `{branch}`, `{ref}`, or `{oid}` and rejects ambiguous/multi-key
  objects. `RevisionTarget::parse_cli` classifies lowercase 40/64-hex first as `GitOid`, then any `refs/...` input as a
  validated `GitRef`, and otherwise as a validated `BranchName`. Invalid values return `DtoError`; git rev expressions
  such as `HEAD~1` never fall back to another resolver. `ExpectedRefHead` projects as `{missing:true}` or `{oid}`. Oids
  are validated lowercase 40- or 64-hex strings.
- `AdoptOptions = { path?, repoId?, capacity?, quarantine, reAdopt }`; `reAdopt: false` keeps default adoption
  idempotent, while `true` selects the verified retained-source replacement in 02_workspaces.md §Re-adoption, or, when
  main's image is missing, the plain-checkout recovery in §Missing main image. `coordinator.adopt` answers
  `AdoptView = { workspace: WorkspaceView, reAdoption: ReAdoption | null }`, with
  `ReAdoption = { replacement, retained }` and `reAdoption` non-null exactly when the call re-adopted main; the
  TypeScript `Coordinator.adopt` resolves `Adopted = { workspace: WorkspaceRef, reAdoption: ReAdoption | null }`. The
  value is for callers. A re-adoption that startup recovery finishes answers no caller, so it logs the same paths on
  stderr for the human who reopened cowshed; the two coexist deliberately.
  `ForkOptions = { ref?, clean, browse, slot?, register, gitWorktree }`;
  `AttachOptions = { browse, observedPath?, aggregate }`, `aggregate` marking one attach of `attach` with no name or
  `--all`, which makes no room (02_workspaces.md, "Making room for an attach"); `RemoveOptions = { force }`;
  `GcOptions = { dryRun, abandoned }`; `RebaseOptions`, `LandOptions`, and `PushOptions` use the expectation fields
  shown above. All booleans are explicit in JSON; absence never silently means authority was granted. Both GC options
  default to false on input; only `abandoned: true` authorizes reclaiming abandon bundles.
  `coordinator.fork { repoId, workspace, source?, options: ForkOptions }` defaults an absent source to main. A ref with
  `clean: false` is a usage refusal; the CLI sets clean for `--ref`. Every fork uses the same mint, carries source edits
  and untracked files by default, and gets its own `cowshed/<ws>` branch. `coordinator.create`, `CreateOptions`, and
  `cowshed.create` are not API names; calling `coordinator.create` returns a Usage error. The TypeScript entry point is
  `cowshed.fork(workspace, { source, ... })`.
- `GrantSet`, `GrantDelta`, `PortBlock`, `EgressRule`, `RepoRule`, and `SimVerb` reuse the metadata definitions.
  `GrantSet.portBlock` is present on macOS and omitted on Linux; `GrantSet.retainedPortBlocks` is present only after a
  macOS relocation; `GrantSet.loopbackAny` is present only while granted; `GrantDelta.servicePorts`,
  `GrantDelta.loopbackAny` and `GrantDelta.expectedRevision` are optional. `PortBlock` fields are private; `new`,
  `base()`, and `size()` are the public surface, and custom deserialization invokes the same validation so size zero, a
  size that is not a power of two, a base not aligned to its size, overflow, and struct-literal forgery fail; an unknown
  field fails a request and is dropped by a result reader (Reader projection).
- `HoldEvent` (`cowshed hold --state-fd`, one per line) is
  `{kind:"started",pid,birth:HeldBirth,output:{stdout:HeldDescriptor,stderr:HeldDescriptor}}`,
  `{kind:"exited",status:ExitStatus}`, `{kind:"signalled",signal:HoldSignal}`,
  `{kind:"notSignalled",signal:HoldSignal,reason}`, `{kind:"failed",reason}` (no command runs) or
  `{kind:"commandFailed",pid,reason}` (the holder failed once its leader ran, and released it). `HoldSignal` is the
  closed name set `HUP INT QUIT ABRT KILL USR1 USR2 ALRM TERM STOP CONT TSTP TTIN TTOU WINCH`, as `signal <SIG>` control
  lines spell it. `HeldBirth = {kind:"observed",birth,start} | {kind:"unobserved",reason}`, the leader's rusage birth
  clock and start time as exact decimal strings.
  `HeldDescriptor = {kind:"device",device} | {kind:"inode",inode} | {kind:"closed"} | {kind:"unnamed",fdType}`: `device`
  is lsof's `d` field, `0x` and lowercase hex digits with no leading zero; `inode` is lsof's `i` field as an exact
  decimal string; `fdType` is a Darwin `PROX_FDTYPE_*` neither field names.
- `PushReport = { sourceHead, destinationRef, previousDestinationHead? }`;
  `LandReport = { kind: "delivered", ...PushReport } | { kind: "landed", ...LandedReport } | { kind: "preflighted", ...LandPreflight }`;
  `LandPreflight = { workspaceIncarnation, sourceHead, targetBranch, targetHead: ExpectedRefHead }`;
  `LandedReport = { landedHead, targetBranch, previousTargetHead?, targetWasCheckedOut, retired, buildVolume, folded }`;
  `FoldedWorkspace = { workspace, head, outcome: { kind: "retired" } | { kind: "kept", reason: { kind: "dirty", files } | { kind: "refused", code, message } } }`;
  preservation-only delivery has no landed, build-volume or retirement fields; `MirrorInfo = { url, mirror }`.
- `GatewayStatus = { installed, running, socket, cliVersion, daemonVersion?, activeWorkspaces, drainCause?, healing?, recovering?, staleDaemon?, scheduling?, pendingInstall?, landTurns, landTurnsError? }`;
  `pendingInstall?: true` says an install holds the install slot (`dev.cowshed/install.lock`, read without taking it),
  absent while the slot is free (05_gateway.md "A pending install is visible");
  `healing = { mounting: { projects } } | "restoringSessions"` says how far the daemon's startup pass has got — the
  projects it still mounts, never 0, then the sessions it restores from them — and is absent once that pass is over
  (05_gateway.md "Startup contract"); `recovering = { supervisors }` counts the workspace supervisors from before the
  daemon started that it is still recovering, never 0 (11_shell.md "Supervisor recovery"). `scheduling?: HostScheduling`
  is what the CLI reads on the host for `gateway status` and `doctor` (05_gateway.md "Status"), absent from an install's
  health wait: `HostScheduling = { readings, jobs, imageCleanup }`. `GatewayStatus.scheduling` identifies the active
  scheduling mechanism; with mbx present it reports the expected pinned mbx revision, the canonical shared cache
  directory and pool activity read through mbx's published inspection interface (05_gateway.md, "Status"), never
  gateway-maintained pool counters. `readings: HostReadings = { load, memoryPressure, swap }`, each
  `{ kind: "read", … } | { kind: "unread", error }`, a failed kernel read stated and never zero: `load` reads
  `{ one, five, fifteen }` (`getloadavg`), `memoryPressure` reads
  `{ kind: "darwin", level: "normal" | "warning" | "critical" }` (`kern.memorystatus_vm_pressure_level`) or
  `{ kind: "psi", someAvg10 }` in hundredths of a percent (`/proc/pressure/memory`), and `swap` reads
  `{ usedBytes, totalBytes }` (`vm.swapusage`, `/proc/meminfo`). `jobs: HostJobs = { counts?, unanswered? }`, read from
  each live workspace supervisor's own job list: `counts` is
  `[{ class: ExecClass, state: "queued" | "running" | "draining", jobs }]`, one row per class and state with at least
  one job, and `unanswered` is `[{ socket, error }]` for a supervisor whose list failed, whose jobs are in no count.
  `imageCleanup: ImageCleanup = { waiting, unread? }`: the entries a listing of the store's `.deleting` directories
  finds ("Deleting released images"), and each directory that listing could not read. `landTurns`/`landTurnsError` are
  05_gateway.md "Land turns"'s.
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
    /* otherBuild, fence, admission, stdin: optional structured sources, read through accessors */
}
impl CowshedError {
    pub fn other_build_source(&self) -> Option<&OtherBuild>;
    pub fn fence_source(&self) -> Option<&FenceRefusal>;
    pub fn admission_source(&self) -> Option<&AdmissionRefusal>;
    pub fn stdin_source(&self) -> Option<StdinRefusal>;
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
  workspaceAt(path: string): Promise<WorkspaceRef>;
  workspaceInSlot(slot: number): Promise<WorkspaceRef>; // project.workspaceInSlot
  listWorkspaces(options?: {
    landing?: 'measure' | 'landed';
  }): Promise<{ workspaces: WorkspaceInfo[]; unproven: UnprovenWorkspace[] }>;
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
export type JobFailure = 'scriptSyntax' | 'supervisorLost' | 'cpuAdmission' | 'priority' | 'memory' | 'unlaunched';

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

// The N-API boundary returns one bounded raw chunk, not the Rust stream adapter.
// Empty bytes mark the current end; eof means the stream has closed, not merely caught up.
export type JobLogs = { readonly bytes: Uint8Array; readonly nextOffset: number; readonly eof: boolean };

export interface JobHandle {
  readonly id: JobId;
  status(): Promise<JobInfo>;
  resources(): Promise<JobResourceSample>;
  listeningPorts(): Promise<JobListeningPorts>;
  processes(): Promise<JobProcessTree>;
  processEvents(everyMs: number): AsyncIterable<JobProcessEvent>;
  progress(everyMs: number): AsyncIterable<JobResourceSample>;
  tail(cursor: JobJournalCursor | undefined, limits: JobTailLimits): Promise<JobTail>;
  // Without follow, continue at nextOffset until bytes is empty or eof is true.
  // Follow waits for output notifications at the current end while the job is running.
  logs(args: { stream: JobStream; offset: number; follow: boolean; signal?: AbortSignal }): Promise<JobLogs>;
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

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(request: ExecRequest): Promise<JobHandle>;
  background(request: ExecRequest): Promise<JobHandle>;
  listJobs(): Promise<JobInfo[]>;
  job(id: JobId): Promise<JobHandle>;
  jobByKey(key: string): Promise<JobHandle | null>;
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
  `to_string_lossy`, replacement characters, or a second wire union. `CommandArg::with_wire` validates the argument,
  then lends one public `TaggedBytesRef { encoding, data }`: valid UTF-8 borrows those bytes as `utf8`, and every other
  sequence borrows canonical standard base64. A validation failure is `DtoError` and does not call the closure. Serde
  writes that same projection; it does not encode the bytes a second time. The 128 KiB argument and 1 MiB argv bounds
  are checked before RPC, spawn, and artifact mutation.
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
  never holds another request, of any client, while a job gets there. A connection holds at most 64 open calls; a call
  past that waits, unrouted, until an open one completes, up to 64 waiting calls, and one past both is refused with a
  conflict. The controller never stops reading a connection, so a demand on an open stream is always read. A job's
  output therefore reaches a client while the client waits for the job's end, and a status read is answered while a wait
  is open on the same connection. A frame that breaks the protocol ends the connection and fails every call still open
  on it with that error. The SDK client admits at most that same published limit of non-stream calls onto its connection
  at once. Excess calls wait in FIFO order until an answer frees a slot, and receive their wire id only when sent.
  Stream-lane requests, demands and closes continue while the non-stream queue is full; the client neither opens extra
  connections nor retries or suppresses a controller conflict. A fatal connection error fails queued calls as well as
  calls already sent.
- **A call may ask to hear its lifecycle steps.** A request that sets top-level `steps: true` gets, ahead of its answer,
  one `{id, step}` frame per step start and end its router call reports — the same steps a lifecycle verb prints on
  stderr (13_telemetry.md): `{event: "started", step, parent?, scope, name}` and `{event: "ended", step, error?}`. Step
  ids are unique within the call, a step's `parent` is the step it runs inside and is always reported started first, and
  a step ends before its parent. Frames are written as the steps happen, so a caller can name the step a slow call is in
  while it is still in it; all of a call's step frames precede its answer. A request without `steps` never gets a step
  frame, so a client that never asks reads only answers. The Rust client exposes this as
  `Coordinator::create_reporting`, which sends each step to a channel the caller reads while the create runs.
- **A stream-lane call answers each demand.** An operation declared `stream` answers with events of its declared result,
  then ends. Its request is its first demand; each later one is a `{id, demand: "next"}` frame the caller sends only
  after the event that answered its previous demand, and `{id, demand: "close"}` ends the call at any time. The
  controller answers every demand with exactly one frame: an `{id, event}` frame, or the call's answer -- `{}` once the
  events ended or the caller closed it, or the error that ended them. So an unread stream holds the controller to one
  event and the connection to none, and a producer coalesces or holds what its reader has not asked for. A close that
  crosses the call's end is no error; a second demand while one is unanswered, or a demand naming no open stream, ends
  the connection. Stream-lane calls do not count against the 64 open calls: a connection holds at most 64 open streams
  and refuses one past that, and always reads demands. The Rust client exposes a call as `EventStream<O>`; dropping it
  before its end sends the close. N-API projects it as an `AsyncIterable` generated from the same row: each step of a
  loop sends one demand, and leaving the loop early (`return`) sends the close, which ends the subscription and never
  the job it observes.
- **Post-terminal publication is independent.** `ExecOptions.stdoutCopy` / `stderrCopy` project
  `OutputPublication {path,policy}`. They clone/reflink/copy the sealed protected artifact after terminal state, never
  hardlink, never change `StreamInfo.storage`, and report publication failure separately from process state.
- **Cancellation** is an `AbortSignal` on `ExecOptions`, attachments, and `JobHandle.logs`/`wait`; aborting stops the
  client operation, not the durable job. Callers invoke `JobHandle.kill()` explicitly. A completed explicit kill or
  workspace retirement records `JobState::Killed` while retaining the operating system's actual `ExitStatus`. A child
  that handles SIGTERM may report `Exited`, including code zero; a child terminated by the signal reports `Signaled`.
  Both are valid in `JobInfo` and `ExecRecord`; neither may omit the observed exit status. A job killed before its
  command existed — withdrawn while queued for memory pressure (11_shell.md, "Job admission and class placement") —
  never ran: its `JobInfo` is `Killed` with no `pid` and no `exit`. A killed `JobInfo` with a `pid` and no `exit` is
  refused, so "never ran" and "ran and was killed" never share a projection. Cancellation uses the same membership
  retirement as spontaneous leader exit: snapshot, TERM, existing grace, KILL where necessary, and native exit proof. A
  retirement refusal is an error, not permission to release the claim or publish terminal state: it answers `wait` and a
  following `logs` with the typed error while the job stays held, and on Darwin a refusal that came while members still
  ran re-runs once they have exited (11_shell.md, "Shell activation and process reuse"). Neither quiet output, elapsed
  age nor zero CPU authorizes cancellation.
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

## Task server scheduling and trusted host gates

`Coordinator::exec(worker, request)` is the trusted `coordinator.exec` upload route; the worker handle supplies only the
immutable workspace/incarnation fence. Ordinary `worker.exec` admits Background, Interactive, Resident and
InteractiveResident in Workspace scope. Requesting Gate or HostGate there is typed class-not-permitted before snapshot,
record or spawn. Scope, class and want bind keyed admission just like command/env: changing one under the same key is
keyConflict, never a new spawn. No class has an admission deadline: a background start waits out memory pressure for as
long as it lasts (05_gateway.md, "Memory pressure").

`ExecScope` is Workspace (default) or HostGate. HostGate requires the coordinator route, argv, read-write mode and no
session, and pairs with every class: it names where the command runs, and the class alone decides its placement
(06_cli.md, `--host`). It runs one host one-shot without ExecutedChild or shell activation, never a new wrapper. No
worker gains host authority. The operator gate command explicitly selects its checkout environment. The host child
starts from an empty environment and gets the ordinary request environment under the existing reserved/credential rules,
its job's jobserver fifo and count, PWD and `NX_DAEMON_START=never` (no job's Nx client starts a daemon, 04_sandbox.md),
not the supervisor's environment or authority. CLI HostGate preserves custom operator/project names; ordinary worker
activation and overlays remain unchanged.

The public environment-name policy is **record-only**, for **all scopes**: `JobAdmission.env` stores only
`PATH HOME USER LOGNAME SHELL TMPDIR LANG TERM TZ CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN BUN_INSTALL DEVELOPER_DIR` and
the `LC_*` locale-category family. Filtering occurs before admission records are persisted, never by refusing a request
or stripping its execution environment. Project variables exported by devenv or `.envrc`, custom caller variables and
their values continue to reach actual jobs under the existing runtime rules. `NX_*`, `NODE_*`, `CI`, credentials,
CPU-authority names and every other non-public name are omitted only from this plaintext record projection.
`SSL_CERT_FILE`, `SSL_CERT_DIR`, `NIX_SSL_CERT_FILE` and `SDKROOT` are not public-record names; this does not prevent
their runtime use by a project. There is no private ambient channel or plaintext shadow.

The full authored request environment, minus the existing withheld credentials and before session/activation overlays,
is committed as typed `envSha256` (`Sha256Digest`). Name/value pairs are sorted by name; each UTF-8 component is framed
by its u64 little-endian byte length followed by its bytes, using the existing structured SHA-256 commitment pattern.
Env identity comparisons use this commitment, not the filtered plaintext map. Changing a custom/private value cannot
alias a previous job, including after supervisor restart; changing a withheld credential still does not change request
identity because it cannot reach the command. The existing Arrow record proof covers both projection and commitment. No
unrecorded private map is retained to make a repeat work.

JobInfo and SealedJob expose admitted class/scope and observed nice (absent before placement, and always on Linux, where
the class cgroup places the job). CowshedError carries typed cpuAdmission (a jobserver fifo not made or a memory
pressure not read) and priority (actual pid, requested/observed nice and errno). A failed or absent observation is never
fabricated.

Records now use layout14: layout13's columns remain an exact prefix, followed by the nullable boolean
retirement_outlived_incomplete marker. Layout13 appended the nullable retirement_containment and retirement_outlived
wire-JSON receipt columns (11_shell.md, "Job admission and class placement"). Layout12 appended nullable UInt64
admission_want, the want a keyed request named, null for one that named none and for every unkeyed record. Layout11
appended nullable Binary admission_env_sha256 to layout10, exactly 32 bytes and present exactly for keyed job records.
Earlier histories are retained untouched and set aside, not migrated or re-keyed. On an existing incarnation that may
have served keyed work, new keyed exec is refused as unprovable until the incarnation is renewed. **Operators must renew
existing incarnations before keyed exec after upgrading.** Unkeyed operator CLI exec remains available. This is the
existing strict history-attestation contract, not silent compatibility.

## CLI and API exec parity

`cowshed exec` asks for nothing an embedding process cannot: every scheduling option and class the CLI offers is
available through the Rust API and the NAPI/Bun binding, so an embedding executor never shells out for one. Every flag
is one typed field of the same request, decoded by the same validation (`JobScheduling::new`, `AdmissionSource::admit`,
`validate_host_gate_request`), so the two surfaces refuse the same requests:

| `cowshed exec` flag                                                       | Rust `ExecRequest` (`WorkspaceHandle::exec`, `Coordinator::exec`) | NAPI `ExecRequest` (`worker.exec`, `coordinator.exec`)    |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------- |
| `-- <cmd…>`                                                               | `command: ExecCommand::Argv`                                      | `argv`                                                    |
| `--stdin`                                                                 | `stdin: StdinSource::Stream`                                      | `stdin: { kind: 'open' }` and the job's attachment writes |
| `--stdin-file <rel>`                                                      | `StdinSource::WorkspaceFile`                                      | `stdinWorkspacePath`                                      |
| `--stdin-base64 <data>`                                                   | `StdinSource::Inline`                                             | `stdin: string \| Uint8Array`                             |
| `--ro`                                                                    | `mode: RunSandboxMode::ReadOnly`                                  | `mode: 'readOnly'`                                        |
| `--cwd <rel>`                                                             | `cwd`                                                             | `cwd`                                                     |
| `--session <name>`                                                        | `WorkspaceHandle::shell(Some(name))`, then `Session::run`         | `shell(name)`, then `session.exec`                        |
| `--stdout-copy`, `--stderr-copy`                                          | `stdout_copy`, `stderr_copy`                                      | `stdoutCopy`, `stderrCopy`                                |
| `--replace-output`                                                        | `PublicationPolicy::Replace`                                      | `policy: 'replace'`                                       |
| `--want <n>`                                                              | `want: Option<NonZeroUsize>`                                      | `want`                                                    |
| `--class <background\|interactive\|gate\|resident\|interactive-resident>` | `class: ExecClass`                                                | `class`                                                   |
| `--host`                                                                  | `scope: ExecScope::HostGate`                                      | `scope: 'hostGate'`                                       |
| `--background`                                                            | every exec answers its `JobHandle` at admission                   | the same                                                  |
| `--timeout <dur>`                                                         | the caller's own deadline over `JobHandle::wait`                  | the same, over `wait()`                                   |
| `land --ungated`                                                          | `LandOptions::mode`: `LandMode::Ungated`                          | `mode: 'ungated'`                                         |

The gate class and host scope are the coordinator route's alone on both surfaces (`coordinator.exec`; the CLI is that
route); a worker capability asks for background, interactive and resident work, a resident at either nice, in its own
workspace. No exec request carries a priority.

The API also carries what a one-shot CLI process cannot use, none of which asks the scheduler for anything: script
commands (`ExecCommand::Script`), open stdin written through the job's attachment, the request `env` and `trace`,
`admission_key` for idempotent keyed admission, and reattaching to a job by id or key (`WorkspaceHandle::job`,
`job_by_key`).

One behaviour is the CLI's alone, on purpose: a foreground `cowshed exec` holds its job (11_shell.md, `hold`), so a
client killed by its caller's timeout leaves no orphan (05_gateway.md, "A job lives with its requester"). An API job
outlives the connection that admitted it, because that is what lets an embedding executor reattach to it after its own
restart; an API caller ends a job it no longer wants with `JobHandle::kill`.

## CLI and N-API option parity

Every word the CLI parses has a typed counterpart. "Word" means each leaf verb (`fork`, `cache clear`) and each argument
its clap definition declares: positionals, flags, values and the trailing argv. A counterpart is one of:

- **Wire**: a field of a declared, projected operation's request record (or the request itself for a verb), reached
  through a member of the public TypeScript facade (`Coordinator`, `Project`, `WorkspaceRef`, `WorkspaceHandle`,
  `Session`, `JobHandle`, or a package export such as `connectCoordinator`).
- **Composed**: a composition of facade calls this specification defines and the CLI itself runs: `exec --timeout` and
  `--background` ("CLI and API exec parity"), `revoke --all` (read the grants, revoke every item at their revision),
  `doctor --repair` (`doctor()` then `gc({})`), `path --no-attach` (`Project.path` skips the attach), and `controller`,
  whose peer is `coordinatorEndpoint`.
- **CLI-only**, narrowly: pure terminal presentation (`--json`, `--quiet`, help, version) or a human step a library
  caller cannot provide (`setup`'s administrator authorization dialog, 06_cli.md rule 3). A refusal-only flag is not
  CLI-only: `fork --markdown-only` is `ForkOptions.markdownOnly`, which the controller refuses for every caller.
- **Launcher**: a verb whose holder is the process that runs it — `lock` holds a lock, `hold` holds a command's group
  leader unreaped from outside that group, `subtree-root` execs its command in its own life, `land-turn` keeps a land
  turn until its child ends — so no connection carries it. Its counterpart is a typed launcher of the facade
  ("Launchers" below). A word is either an **input**, the launcher or a property of its options, or a **result**: a
  caller-opened descriptor the verb's argv names (`--state-fd`, `--control-fd`, `--handle-fd`) is a member of the
  launcher's resolved run whose type the facade assertion checks it extends. A descriptor number is never an input: the
  launcher chooses it.

The gate is `cowshed-api-gen`'s `cli_parity` module, run by every `api-generate` and `api-check`. It reads the grammar
from `cli_command()` in `crates/cowshed-cli/src/args.rs` without building the CLI, accepting only the builder vocabulary
that file uses and refusing any other call by name; the addon's `parity_tests` compares that reading with the tree clap
builds. Its `PARITY` table holds one row per word with the counterpart and, for every non-wire row, its reason. A word
without a row, a row naming a word the CLI no longer parses, a wire field the request record does not carry (resolved
through the same declarations the projections are generated from, serde renames and flattening included) and an
operation that is internal or undeclared each refuse generation by name. The facade half is
`src/cli-parity.generated.ts`: one property per wire, composed or launcher word whose type indexes the facade member and
the parameter property that carries it, so a facade that lacks the member or the property fails the package's typecheck;
a launcher result word is the resolved run's member, asserted to extend its stated type (`Extends`). A word carried by
one variant of a tagged union (`credential add --secret-env` is `secret` tagged `env`, its `name`) is checked through
that variant, selected by a tag the union must declare, so a word is never satisfied by a sibling variant's field. Its
header states the counts per class. Every word the CLI parses is wire, composed, launcher or CLI-only: the gate has no
class for a word without a counterpart.

The project-scoped words the facade could not reach are operations of their own: `project.list` carries an optional
`landing` selection (`measure` measures every attached session against main's landing branch with
`cowshed_core::landing::annotate_listing` after the actor answers; `landed` then keeps only what
`landing::retain_fully_landed` proves fully landed). The CLI's project-scoped `ls --landed` asks for `measure` and
applies the same filter itself, because it names every row withheld for want of an answer. `project.workspaceInSlot`
resolves the workspace whose mount is a build slot's stable path (`path --slot`), and `coordinator.rekey` rotates one
keyless workspace's CA identity (`rekey`) and re-derives that workspace's gateway session from the new CA before it
answers, both for the CLI and the addon alike: a held gateway identity never outlives its key until some later exec. The
rotation republishes the grants sidecar at revision + 1 whether its source was the quarantine entry or the live sidecar,
because the gateway installs only a higher revision and never bypasses that fence.

## Host operations

The host's own verbs — whose subject is the host, or a project no controller serves — are rows of the one `operations!`
table with scope `host`, projected like every other row: generated request and result records, validators, served
construction and N-API adapters, onto a `Host` handle that binds no request field. A project-subject host operation
names the project by a path inside its checkout (`project`), resolved as `--project` resolves it.

A host row is served by exactly one route, a `cowshed host` connection, and nothing else serves it: a project connection
refuses a host method before decoding it, the project router refuses one that reaches it, and a host connection refuses
every method that is not a host row. `cowshed host` is the CLI binary serving one connection on the socket it inherits
as standard input, with the controller's frames, handshake version and limits; its handshake answer is
`{version, nonce, host: "host", pid}` ("Controller handshake"), which a project client does not decode, and a project
controller's answer does not decode as a host's. Every operation calls the function its CLI verb calls and answers with
what that function computes; only rendering is the verb's. Guidance goes to `cowshed host`'s stderr; stdout is never
written.

TypeScript reaches it through `openHost(): Promise<Host>`: the addon makes a socketpair, starts the package launcher
(`bin/cowshed host`, 06_cli.md "Launcher") with one end as its standard input and the caller's stdout and stderr, and
connects on the other. `Host.close()` ends the connection and resolves once the process has exited; every call after it
rejects. A child a host operation starts never inherits the socket: its stdin is the request's own bytes or `/dev/null`.

Served rows: `host.listProjects` (`ls --all`, with `ls`'s landing selection), `host.attachAll`, `host.detachAll`,
`host.forgetAbandoned` (`forget --abandon`: the operation is the authorization), `host.mountMain`, `host.gatewayStatus`,
`host.gatewayStart`, `host.gatewayStop` (`purge`), `host.credentialStatus`, `host.credentialRemove`, `host.identityAdd`,
`host.skillInstall`, `host.buildState`, `host.clearNxStore` (`cache clear`'s gateway half), `host.lockHolders`
(`lock-holders`: the `LockObservation` it prints), `host.treeHolders` (`tree-holders`: its `TreeHolder` lines as one
list), `host.fdHolders` (`fd-holders`: the `FdHolders` object it prints, the `--device` and `--inode` values as the
request's `devices` and `inodes`) and `host.retireDescendants` (`retire-descendants`: the handle the verb reads on stdin
is the request's `handle`, and the supervisor asked is the one `COWSHED_SUPERVISOR_SOCKET` names in the `cowshed host`
process, which inherits the environment of the process that called `openHost`; no socket is the typed `foldUnavailable`
refusal, as the verb prints it).

`host.diskCommand` (a stream row) is `disk-command` through the same runner: the host's disk lease, the child's own
session, `deadlineMs` exactly as `--deadline-ms` (an ordinary verb's total admission-and-child budget, 120 s when
absent), and a verb that creates or releases a disk-image device detached, file-backed and never killed, past 120 s
warned about and waited for whatever `deadlineMs` says. The child's standard input is the request's `stdin` bytes,
written whole on a fresh pipe and closed (empty when absent), never the connection's socket. The runner collects the
tool's output and delivers it once the tool has ended, as the verb does: the events are its whole stdout in order, in
chunks of at most one inline frame, then its whole stderr the same way, then `exit` with the code the verb exits with
(its exit code, or 128 plus its signal). A runner failure is the call's error, as the verb's wrapper codes are.

`gatewaySocket` selects the authenticated disk-admission endpoint exactly as `--gateway-socket` does. It is an absolute
control-socket path; a relative path is a usage error. Omitting it selects the host gateway. `host.credentialAdd` (an
upload row) is `credential add`'s enrolment: the origin, the path prefixes, the methods (absent or empty is GET and
HEAD) and one tagged secret source, so no request names two. `env` (`--secret-env`, with `command` as `--secret-command`
when the variable is unset or empty) reads `cowshed host`'s environment, which is the `openHost` caller's, and withholds
the variable from every child of the project; `command` (`--secret-command`) runs the helper with no stdin; `upload`
(`--secret-stdin`) is the call's upload frame, trimmed UTF-8 as the verb's pipe is. A frame beside any other source is
refused as usage before a project is opened. The answer is the project's routes.

### Launchers

The process-holder verbs are reached through typed launchers of the public facade. Each spawns its verb through the
package launcher (`bin/cowshed`, 06_cli.md "Launcher") with Node's `stdio` array: every descriptor the verb's argv names
is a pipe the launcher owns, numbered from 3, and the verb's own lines on it are decoded with the generated validators
of their event DTOs. The CLI is the one implementation; a launcher types it. The process inherits the caller's
environment, working directory and standard streams, as the CLI verb does, and a launcher adds no option of its own.

| launcher                                                  | spawns                                                    | resolves to                                            |
| --------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------ |
| `spawnLock({ path, ownedPids?, argv })`                   | `lock <path> --state-fd 3 [--owned-pid …] -- argv`        | `LockRun { state, cancelOwner(), forward(sig), exit }` |
| `spawnHold({ argv })`                                     | `hold --control-fd 3 --state-fd 4 -- argv`                | `HoldRun { control: HoldControl, state, exit }`        |
| `spawnSubtreeRoot({ argv })`                              | `subtree-root --handle-fd 3 -- argv`                      | `SubtreeRootRun { admission, exit }`                   |
| `spawnLandTurn({ repo, workspace?, outsideLock?, argv })` | `land-turn <repo> [--workspace] [--outside-lock] -- argv` | `LandTurnRun { exit }`                                 |

No run exposes the process or a way to signal it freely; each holder is reached only through its verb's documented
requests. `LockRun.forward` sends INT, HUP or TERM, which cowshed forwards to the command once it holds the lock (before
that, the signal ends the waiter and gives up its place). `LockRun.cancelOwner` is the `--owned-pid` request, SIGUSR1:
it is sent only when the decoder has seen `queued` and none of `acquired`, `failed`, `signalled` or `commandFailed`
(`canCancelOwner`), because cowshed watches SIGUSR1 only from the moment it queues; otherwise it throws `usage` and
sends nothing. `HoldControl` writes the `hold` control lines: `signal(HoldSignal)`, `release()`, and `close()`, which
before a `release` is the controller end of file that tears the held group down (11_shell.md), and after one only closes
the channel; `signal` and `release` after `close` throw `usage`.

`exit` always resolves, once the process has ended and its descriptors were read to their end, with
`LauncherExit { status, violation?, failure? }`: the real status (cowshed's, which is the command's or a wrapper code),
the first protocol violation, and a descriptor that could not be read or written. A spawn that cannot start rejects
`environment-missing`; nothing has started. Operational outcomes are the verb's events: `failed`, `commandFailed`,
`signalled` and `notSignalled` are yielded values, and end of file without a last line is no violation (the verb defines
those ends; `exit.status` says what happened). A violation (`ProtocolViolation { descriptor, bytes, reason }`) is a line
that is not JSON or fails its validator, a line truncated at end of file, a line after one the verb sends last (lock's
`failed` or `signalled`, hold's `failed`), or a second admission record; `LineDecoder` decides it without I/O. `state`
throws `CowshedError { code: 'integrity', violation }` carrying it, and `exit.violation` keeps it. A descriptor that
cannot be read throws `internal` with the failure, which `exit.failure` keeps. After either the launcher ends what it
can no longer observe: `hold` closes the control descriptor unless released, `lock` forwards TERM; `subtree-root` ends
nothing, since the process may already be the command. `admission` resolves `{ kind: 'record', admission }` (typed
refusals included) or `{ kind: 'unknown', violation? }` when the descriptor did not carry exactly one well-formed
record; an unknown admission is never turned into a refusal. A `state` the consumer abandons is still read and validated
to its end; for `hold`, abandoning it before a `release` closes the control descriptor, so a group nobody observes is
torn down.

One verb has no counterpart by name: `gateway run` is the process launchd execs as the daemon. A library call never
becomes that process and no host connection runs it.
