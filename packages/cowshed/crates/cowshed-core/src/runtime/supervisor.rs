use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{CStr, OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt as _};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::api::dto::{
    BinaryData, CommandArg, ExecCommand, ExecRequest, ExitStatus, JobFailure, JobId, JobInfo,
    JobState, OutputLimitInfo, OutputPublication, OutputStorage, OutputSummary, ProtectedOutput,
    SealedJob, Sha256Digest, StdinInfo, StdinKind, StdinSource, StreamInfo, TraceContext, TraceId,
    UtcTimestamp, WarmAdmission, WarmRange, WorkspacePath,
};
use crate::error::{CowshedError, Result};
use crate::exec::{
    ExecError, SandboxExecRequest, SpawnPlan, classify_spawn_error, plan_exec_under,
    prepare_child_descriptors,
};
use crate::fsio::AnchoredDirectory;
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::sandbox::{
    SandboxConfig, SandboxProfileRole, sandbox_runtime_dir, sandbox_runtime_link, seatbelt_profile,
};
use crate::storage::audit::AuditSinkError;
use crate::workspace_environment::{
    GO_ENV, NODE_CA_ENV, PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV, WORKSPACE_TOKEN_ENV,
};
use cowshed_gateway_types::WorkspaceToken;

use crate::runtime::land_warm::{WarmLane, WarmRun, WarmTurn};
use crate::storage::job_artifact::{
    ArtifactConfig, ArtifactError, ArtifactStore, CompletedJobArtifacts, JobEnding, OutputTargets,
    SealedCheckpointManifest, StreamKind,
};

const DEFAULT_ACTOR_CAPACITY: usize = 64;
const DEFAULT_EVENT_CAPACITY: usize = 64;
const PROCESS_IO_CHUNK: usize = 64 * 1024;
const MAX_LOG_READ: usize = 64 * 1024;
const MAX_PENDING_STDIN_BYTES: usize = 256 * 1024;

/// Exact immutable authority carried by every cheap supervisor handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceAuthoritySnapshot {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub grant_revision: u64,
    pub lifecycle_revision: u64,
}

/// Production construction inputs for exactly one mounted workspace incarnation.
#[derive(Clone, Debug)]
pub struct WorkspaceSupervisorConfig {
    pub authority: WorkspaceAuthoritySnapshot,
    /// Every identity the project owns. Kept beside the authority rather than inside it: the
    /// authority is the single pinned identity a workspace acts under, while artifact frames the
    /// workspace already wrote may be stamped with an identity the project has since left behind.
    pub owned_repo_ids: OwnedRepoIds,
    pub workspace_root: PathBuf,
    pub default_cwd: Option<WorkspacePath>,
    pub sandbox: SandboxConfig,
    pub artifacts: ArtifactConfig,
    pub term_grace: Duration,
    pub actor_capacity: usize,
    pub event_capacity: usize,
    /// Environment variable names every spawn withholds from the child.
    ///
    /// Resolved host-side from the approved credential routes for this project: when the gateway
    /// holds a registry credential for an origin, an ambient copy of the same token in the
    /// operator's shell has no business reaching a sandbox. Empty unless a route was enrolled.
    pub credential_env_names: BTreeSet<String>,
    /// The program that starts this workspace's warm exec hosts. `None` runs every command
    /// through one-shot activation, which is what a host process that cannot start exec hosts
    /// (it never called [`super::shell_host::dispatch`]) is left with.
    pub shell_host: Option<super::shell_host::ShellHostProgram>,
    pub shell_pool: super::shell_pool::ShellPoolConfig,
    /// Where the supervisor keeps the process groups of its running jobs for whoever finds it
    /// gone ([`super::job_groups`]); `None` keeps no ledger.
    pub group_ledger: Option<PathBuf>,
}

impl WorkspaceSupervisorConfig {
    pub fn validate(&self) -> Result<()> {
        if self.actor_capacity == 0 || self.event_capacity == 0 {
            return Err(CowshedError::usage(
                "workspace supervisor channel capacities must be positive",
                "configure positive bounded channel capacities",
            ));
        }
        if self.term_grace.is_zero() {
            return Err(CowshedError::usage(
                "workspace supervisor TERM grace must be positive",
                "configure a positive TERM grace interval",
            ));
        }
        if self.workspace_root != self.sandbox.workspace_mount {
            return Err(CowshedError::conflict(
                "sandbox workspace mount does not match supervisor workspace root",
                "reattach the authoritative workspace mount",
            ));
        }
        // The sandbox's profiles are rendered by `SandboxPolicy::render` at start, which refuses
        // a sandbox that cannot compile.
        self.artifacts.validate().map_err(map_artifact_error)
    }
}

impl Default for WorkspaceSupervisorConfig {
    fn default() -> Self {
        let workspace_root = PathBuf::from("/tmp/cowshed-workspace");
        Self {
            authority: WorkspaceAuthoritySnapshot {
                repo_id: RepoId::parse("local/default").expect("static repo id"),
                workspace: WorkspaceName::new("main").expect("static workspace name"),
                workspace_incarnation: WorkspaceIncarnation::new(
                    "00000000000000000000000000000000",
                )
                .expect("static incarnation"),
                grant_revision: 0,
                lifecycle_revision: 0,
            },
            owned_repo_ids: OwnedRepoIds::sole(
                RepoId::parse("local/default").expect("static repo id"),
            ),
            workspace_root: workspace_root.clone(),
            default_cwd: Some(WorkspacePath::new("work").expect("static cwd")),
            sandbox: SandboxConfig {
                home: PathBuf::from("/tmp/cowshed-home"),
                mount_root: PathBuf::from("/tmp/cowshed-mounts"),
                workspace_mount: workspace_root,
                exec_temp_dir: PathBuf::from("/tmp/cowshed-exec"),
                port_block: crate::metadata::PortBlock::new(49_136, 16).expect("static port block"),
                mode: crate::sandbox::RunSandboxMode::ReadWrite,
                grants: crate::sandbox::SandboxGrants::default(),
                allowed_unix_sockets: Vec::new(),
                additional_denies: Vec::new(),
                shed_links: Vec::new(),
                git_worktree_repository: None,
                shared_tool_homes: Vec::new(),
            },
            artifacts: ArtifactConfig::default(),
            term_grace: Duration::from_secs(2),
            actor_capacity: DEFAULT_ACTOR_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            credential_env_names: BTreeSet::new(),
            shell_host: None,
            shell_pool: super::shell_pool::ShellPoolConfig::default(),
            group_ledger: None,
        }
    }
}

/// A named or anonymous session identity. Reopening a closed name gets a new identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionToken {
    authority: WorkspaceAuthoritySnapshot,
    identity: u64,
    name: Option<String>,
}

impl SessionToken {
    /// A token a supervisor served over its socket issued: identity and name as it reported.
    pub(super) fn remote(
        authority: &WorkspaceAuthoritySnapshot,
        identity: u64,
        name: Option<String>,
    ) -> Self {
        Self {
            authority: authority.clone(),
            identity,
            name,
        }
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub const fn identity(&self) -> u64 {
        self.identity
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionSnapshot {
    pub identity: u64,
    pub name: Option<String>,
    pub cwd: Option<WorkspacePath>,
    pub env: BTreeMap<String, String>,
    pub background_jobs: BTreeSet<JobId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogChunk {
    pub bytes: Bytes,
    pub next_offset: u64,
    pub eof: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointBarrier {
    pub checkpoint_id: String,
    pub barrier_id: u64,
    pub manifest_batch_sha256: Sha256Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessSignal {
    Term,
    Kill,
}

/// What a spawn runs, in the form the spawner executes it.
#[derive(Clone, Debug)]
pub enum SpawnCommand {
    Argv(Vec<OsString>),
    /// A script already rendered at admission (`crate::script`); only an exec host runs it.
    Script(crate::script::RenderedScript),
}

/// A workspace's sandbox and the Seatbelt profiles it compiles to, rendered together once — when
/// a supervisor takes an authority, at start and at each grant advance — and shared by every job
/// admitted under that authority. A spawn carries this value, so no job renders a profile, and
/// no spawn can pair a sandbox with a profile rendered from another one.
#[derive(Clone, Debug)]
pub struct SandboxPolicy(std::sync::Arc<RenderedPolicy>);

#[derive(Debug)]
struct RenderedPolicy {
    /// The sandbox the supervisor holds; a job may narrow it, never widen it.
    ceiling: SandboxConfig,
    read_only: SandboxConfig,
    ceiling_child: String,
    read_only_child: String,
}

impl SandboxPolicy {
    pub fn render(ceiling: SandboxConfig) -> Result<Self> {
        let mut read_only = ceiling.clone();
        read_only.mode = crate::sandbox::RunSandboxMode::ReadOnly;
        let render = |sandbox: &SandboxConfig, role| {
            seatbelt_profile(sandbox, role).map_err(map_sandbox_error)
        };
        // The trusted-supervisor role runs nothing, but a sandbox that cannot compile in it is
        // not one this supervisor may hold.
        render(&ceiling, SandboxProfileRole::TrustedSupervisor)?;
        Ok(Self(std::sync::Arc::new(RenderedPolicy {
            ceiling_child: render(&ceiling, SandboxProfileRole::ExecutedChild)?,
            read_only_child: render(&read_only, SandboxProfileRole::ExecutedChild)?,
            ceiling,
            read_only,
        })))
    }

    pub fn ceiling(&self) -> &SandboxConfig {
        &self.0.ceiling
    }

    /// The sandbox and executed-child profile of a job that asked for `mode`.
    pub fn child(&self, mode: crate::api::dto::RunSandboxMode) -> (&SandboxConfig, &str) {
        match mode {
            crate::api::dto::RunSandboxMode::ReadOnly => {
                (&self.0.read_only, &self.0.read_only_child)
            }
            crate::api::dto::RunSandboxMode::ReadWrite => (&self.0.ceiling, &self.0.ceiling_child),
        }
    }

    /// Whether both are the one rendering admission hands every job under one authority.
    pub fn is_same_rendering(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Debug)]
pub struct ProcessSpawnRequest {
    pub authority: WorkspaceAuthoritySnapshot,
    pub job_id: JobId,
    pub command: SpawnCommand,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub devenv_dir: Option<PathBuf>,
    pub policy: SandboxPolicy,
    /// Requested narrowing of the policy's ceiling.
    pub mode: crate::api::dto::RunSandboxMode,
}

#[derive(Debug)]
pub enum ProcessEvent {
    Output {
        job_id: JobId,
        stream: StreamKind,
        bytes: Bytes,
    },
    OutputEof {
        job_id: JobId,
        stream: StreamKind,
    },
    Exited {
        job_id: JobId,
        exit: ExitStatus,
    },
    /// `wait(2)` did not report how the child died. Carries the failure instead of a status so
    /// no consumer can mistake an unreaped child for a terminated one.
    WaitFailed {
        job_id: JobId,
        error: CowshedError,
    },
    StdinReady {
        job_id: JobId,
    },
    StdinPumpWrite {
        job_id: JobId,
        bytes: Bytes,
        reply: oneshot::Sender<Result<()>>,
    },
    StdinPumpClose {
        job_id: JobId,
    },
    StdinPumpFailed {
        job_id: JobId,
        error: CowshedError,
    },
    Escalate {
        job_id: JobId,
    },
    /// A command that runs in a warm exec host started after its job was admitted.
    Started {
        job_id: JobId,
        pid: u32,
    },
    /// A warm-shell job ended before any command started; nothing is left running.
    LaunchFailed {
        job_id: JobId,
        error: CowshedError,
    },
    /// A script job's text did not parse; nothing ran and its stderr carries the diagnostic.
    ScriptSyntax {
        job_id: JobId,
    },
}

pub trait RunningProcess: Send {
    /// `None` until the command's process exists; a warm-shell job reports it with
    /// [`ProcessEvent::Started`].
    fn pid(&self) -> Option<u32>;
    /// `Ok(false)` means the bounded process-input lane is full.
    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool>;
    fn close_stdin(&mut self) -> Result<()>;
    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()>;
    /// Collects the process's exit status if it has already exited, so a later signal to its group
    /// reaches only the members still running. Only a supervisor that is going away asks: its own
    /// wait for the process will never run, and an exited but unreaped leader is exactly what
    /// makes Darwin refuse a group signal with EPERM instead of reporting the group gone.
    fn reap_if_exited(&mut self) {}
}

#[async_trait]
pub trait SpawnSink: Send {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactWrite {
    pub accepted_bytes: usize,
    pub output_limit: Option<OutputLimitInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactSeal {
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub terminal_batch_sha256: Sha256Digest,
    pub output_limit: Option<OutputLimitInfo>,
}

pub trait ArtifactSink: Send {
    fn next_job_id(&self) -> Result<JobId>;
    fn admit(
        &mut self,
        job_id: JobId,
        grant_revision: u64,
        command: &ExecCommand,
        warm: Option<&WarmRange>,
    ) -> Result<()>;
    fn prepare_background(&mut self, job_id: JobId) -> Result<()>;
    fn write(&mut self, job_id: JobId, stream: StreamKind, bytes: &[u8]) -> Result<ArtifactWrite>;
    fn seal(
        &mut self,
        job_id: JobId,
        ending: JobEnding,
        stdout_copy: Option<OutputPublication>,
        stderr_copy: Option<OutputPublication>,
    ) -> Result<ArtifactSeal>;
    fn checkpoint(&mut self) -> Result<CheckpointBarrier>;
    /// The terminal record of a job of this workspace incarnation, whichever supervisor sealed
    /// it; `None` for a job without one.
    fn sealed(&self, job_id: JobId) -> Option<SealedJob>;
}

pub use crate::process::ProcessStatus;
pub use crate::storage::audit::CommitmentDraft;

/// Where a supervisor sends its audit records. Recording is best effort by contract: the act a
/// record describes has already happened, and a sink that cannot write is a `doctor` finding
/// ([`AuditHealth`]), never a reason to fail a job. `Err` here means the publisher itself is
/// gone, which is the project detaching — not a sink fault.
#[async_trait]
pub trait CommitmentSink: Send {
    async fn record(&mut self, draft: CommitmentDraft) -> Result<()>;
}

/// Production artifact adapter. One supervisor actor owns the store and every token.
pub struct ArtifactStoreSink {
    store: ArtifactStore,
    tokens: BTreeMap<JobId, crate::storage::job_artifact::JobArtifactToken>,
}

impl ArtifactStoreSink {
    pub fn open(
        workspace_root: impl Into<PathBuf>,
        owned_repo_ids: &OwnedRepoIds,
        authority: &WorkspaceAuthoritySnapshot,
        config: ArtifactConfig,
    ) -> Result<Self> {
        let store = ArtifactStore::open(
            workspace_root,
            owned_repo_ids.clone(),
            authority.workspace_incarnation.clone(),
            config,
        )
        .map_err(map_artifact_error)?;
        Ok(Self {
            store,
            tokens: BTreeMap::new(),
        })
    }
}

impl ArtifactSink for ArtifactStoreSink {
    fn next_job_id(&self) -> Result<JobId> {
        self.store.next_job_id().map_err(map_artifact_error)
    }

    fn sealed(&self, job_id: JobId) -> Option<SealedJob> {
        self.store.sealed(job_id).map(|record| SealedJob {
            job_id: record.job_id,
            state: record.state,
            exit: record.exit.clone(),
            failure: record.failure,
            duration_ms: record.duration_ms,
            output_limit: record.output_limit.clone(),
            stdout: record.stdout.clone(),
            stderr: record.stderr.clone(),
        })
    }

    fn admit(
        &mut self,
        job_id: JobId,
        grant_revision: u64,
        command: &ExecCommand,
        warm: Option<&WarmRange>,
    ) -> Result<()> {
        let token = self
            .store
            .begin_job(
                job_id,
                grant_revision,
                command,
                warm,
                OutputTargets::default(),
            )
            .map_err(map_artifact_error)?;
        if token.job_id() != job_id || self.tokens.insert(job_id, token).is_some() {
            return Err(CowshedError::integrity(
                "artifact token identity diverged from actor job identity",
                "cowshed doctor --json",
            ));
        }
        Ok(())
    }

    fn prepare_background(&mut self, job_id: JobId) -> Result<()> {
        let (store, tokens) = (&mut self.store, &self.tokens);
        let token = tokens
            .get(&job_id)
            .ok_or_else(|| missing_artifact_token(job_id))?;
        store.prepare_background(token).map_err(map_artifact_error)
    }

    fn write(&mut self, job_id: JobId, stream: StreamKind, bytes: &[u8]) -> Result<ArtifactWrite> {
        let (store, tokens) = (&mut self.store, &self.tokens);
        let token = tokens
            .get(&job_id)
            .ok_or_else(|| missing_artifact_token(job_id))?;
        let outcome = store
            .append(token, stream, bytes)
            .map_err(map_artifact_error)?;
        Ok(ArtifactWrite {
            accepted_bytes: outcome.accepted_bytes,
            output_limit: outcome.output_limit,
        })
    }

    fn seal(
        &mut self,
        job_id: JobId,
        ending: JobEnding,
        stdout_copy: Option<OutputPublication>,
        stderr_copy: Option<OutputPublication>,
    ) -> Result<ArtifactSeal> {
        let token = self.tokens.remove(&job_id).ok_or_else(|| {
            CowshedError::integrity(
                format!("job {} has no live artifact token", job_id.get()),
                "cowshed doctor --json",
            )
        })?;
        let CompletedJobArtifacts {
            sealed,
            stdout_publication,
            stderr_publication,
        } = self
            .store
            .finish_and_publish(token, ending, stdout_copy, stderr_copy)
            .map_err(map_artifact_error)?;
        if let Some(Err(error)) = stdout_publication {
            return Err(map_artifact_error(error));
        }
        if let Some(Err(error)) = stderr_publication {
            return Err(map_artifact_error(error));
        }
        Ok(ArtifactSeal {
            stdout: sealed.record.stdout,
            stderr: sealed.record.stderr,
            terminal_batch_sha256: sealed.terminal_batch_sha256,
            output_limit: sealed.output_limit,
        })
    }

    fn checkpoint(&mut self) -> Result<CheckpointBarrier> {
        let SealedCheckpointManifest {
            record,
            manifest_batch_sha256,
        } = self.store.checkpoint().map_err(map_artifact_error)?;
        Ok(CheckpointBarrier {
            checkpoint_id: String::new(),
            barrier_id: record.barrier_id,
            manifest_batch_sha256,
        })
    }
}

enum CommitmentRequest {
    Record {
        draft: Box<CommitmentDraft>,
        reply: oneshot::Sender<()>,
    },
    Health {
        reply: oneshot::Sender<AuditHealth>,
    },
}

/// What `doctor` reports about the audit sink: which sink, how many records it refused, and the
/// last refusal's message.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuditHealth {
    pub sink: &'static str,
    pub recorded: u64,
    pub failed: u64,
    pub last_failure: Option<String>,
}

/// Dedicated owner of the audit sink: one actor serializes records in the order the controller
/// performed the acts and absorbs sink failures into [`AuditHealth`].
pub struct CommitmentPublisher;

impl CommitmentPublisher {
    pub fn open(
        telemetry_root: impl AsRef<Path>,
        continuity: crate::storage::audit::ContinuityAudit,
        capacity: usize,
    ) -> Result<CommitmentPublisherHandle> {
        let sink = continuity
            .into_sink(telemetry_root.as_ref())
            .map_err(map_audit_error)?;
        Self::start(sink, capacity)
    }

    pub fn start(
        sink: Box<dyn crate::storage::audit::AuditSink>,
        capacity: usize,
    ) -> Result<CommitmentPublisherHandle> {
        if capacity == 0 {
            return Err(CowshedError::usage(
                "commitment publisher capacity must be positive",
                "configure a positive bounded commitment channel",
            ));
        }
        let (sender, mut receiver) = mpsc::channel::<CommitmentRequest>(capacity);
        tokio::spawn(async move {
            let mut sink = sink;
            let mut health = AuditHealth {
                sink: sink.name(),
                ..AuditHealth::default()
            };
            while let Some(request) = receiver.recv().await {
                match request {
                    CommitmentRequest::Record { draft, reply } => {
                        match sink.record(*draft) {
                            Ok(()) => health.recorded = health.recorded.saturating_add(1),
                            Err(error) => {
                                health.failed = health.failed.saturating_add(1);
                                health.last_failure = Some(error.to_string());
                            }
                        }
                        let _ = reply.send(());
                    }
                    CommitmentRequest::Health { reply } => {
                        let _ = reply.send(health.clone());
                    }
                }
            }
        });
        Ok(CommitmentPublisherHandle { sender })
    }
}

#[derive(Clone)]
pub struct CommitmentPublisherHandle {
    sender: mpsc::Sender<CommitmentRequest>,
}

impl std::fmt::Debug for CommitmentPublisherHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommitmentPublisherHandle")
            .finish_non_exhaustive()
    }
}

impl CommitmentPublisherHandle {
    pub async fn health(&self) -> Result<AuditHealth> {
        let (reply, receive) = oneshot::channel();
        self.send(CommitmentRequest::Health { reply }).await?;
        receive.await.map_err(|_| publisher_stopped())
    }

    async fn send(&self, request: CommitmentRequest) -> Result<()> {
        self.sender.send(request).await.map_err(|_| {
            CowshedError::environment_missing(
                "repo commitment publisher is unavailable",
                "reattach the project",
            )
        })
    }
}

fn publisher_stopped() -> CowshedError {
    CowshedError::environment_missing(
        "repo commitment publisher stopped before acknowledging the record",
        "reattach the project",
    )
}

#[async_trait]
impl CommitmentSink for CommitmentPublisherHandle {
    async fn record(&mut self, draft: CommitmentDraft) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        self.send(CommitmentRequest::Record {
            draft: Box::new(draft),
            reply,
        })
        .await?;
        receive.await.map_err(|_| publisher_stopped())
    }
}

const COWSHED_CONFIG_FILE: &str = ".cowshed.toml";
const DEVENV_PROFILE_BIN: &str = ".devenv/profile/bin";
#[derive(Clone, Debug)]
struct DevenvResolutionError {
    message: String,
}

impl DevenvResolutionError {
    fn into_cowshed_error(self) -> CowshedError {
        CowshedError::environment_missing(
            self.message,
            "repair .cowshed.toml or the configured devenv directory, then retry",
        )
    }
}

fn resolve_devenv_dir(
    workspace_mount: &Path,
) -> std::result::Result<Option<PathBuf>, DevenvResolutionError> {
    let config_path = workspace_mount.join(COWSHED_CONFIG_FILE);
    let config = match fs::read_to_string(&config_path) {
        Ok(input) => Some(
            crate::storage::bootstrap::parse_cowshed_config(&input).map_err(|error| {
                DevenvResolutionError {
                    message: format!("invalid {}: {error}", config_path.display()),
                }
            })?,
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(DevenvResolutionError {
                message: format!("cannot read {}: {error}", config_path.display()),
            });
        }
    };

    if let Some(configured) = config.as_ref().and_then(|config| config.devenv()) {
        let devenv_dir = workspace_mount.join(configured.dir());
        let devenv_nix = devenv_dir.join("devenv.nix");
        if !devenv_nix.is_file() {
            return Err(DevenvResolutionError {
                message: format!(
                    "configured devenv directory {} is missing {}",
                    devenv_dir.display(),
                    devenv_nix.display()
                ),
            });
        }
        return Ok(Some(devenv_dir));
    }

    let root_devenv_nix = workspace_mount.join("devenv.nix");
    Ok(root_devenv_nix
        .is_file()
        .then(|| workspace_mount.to_owned()))
}

/// Resolve a shell input without letting discovery cross the workspace boundary.
fn contained_shell_path(workspace_mount: &Path, path: &Path) -> Result<PathBuf> {
    let resolved = fs::canonicalize(path).map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot resolve shell input {}: {error}", path.display()),
            "repair the workspace shell configuration and retry",
        )
    })?;
    if !resolved.starts_with(workspace_mount) {
        return Err(CowshedError::sandbox_denied(
            format!(
                "shell input {} escapes workspace {}",
                path.display(),
                workspace_mount.display()
            ),
            "keep shell configuration inside the workspace",
        ));
    }
    Ok(resolved)
}

fn shell_input_exists(workspace_mount: &Path, path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => contained_shell_path(workspace_mount, path).map(|_| true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(CowshedError::environment_missing(
            format!("cannot inspect shell input {}: {error}", path.display()),
            "repair the workspace shell configuration and retry",
        )),
    }
}

/// Configuration is rooted beside its `.cowshed.toml`, not at the requested command cwd.
fn shell_project(
    workspace_mount: &Path,
    cwd: &Path,
    configured_dir: Option<&Path>,
) -> Result<Option<(PathBuf, PathBuf)>> {
    if let Some(directory) = configured_dir {
        let directory = contained_shell_path(workspace_mount, directory)?;
        return Ok(Some((workspace_mount.to_owned(), directory)));
    }
    for root in cwd
        .ancestors()
        .take_while(|root| root.starts_with(workspace_mount))
    {
        // Validate links before the config parser reads any workspace-controlled path.
        shell_input_exists(workspace_mount, &root.join(COWSHED_CONFIG_FILE))?;
        shell_input_exists(workspace_mount, &root.join("devenv.nix"))?;
        if let Some(directory) =
            resolve_devenv_dir(root).map_err(DevenvResolutionError::into_cowshed_error)?
        {
            let directory = contained_shell_path(workspace_mount, &directory)?;
            contained_shell_path(workspace_mount, &directory.join("devenv.nix"))?;
            return Ok(Some((root.to_owned(), directory)));
        }
    }
    Ok(None)
}

/// How a command in some cwd enters the workspace shell.
enum ShellEntry {
    /// The nearest workspace-contained `.envrc`, loaded by direnv.
    Envrc(PathBuf),
    /// A configured devenv project without an `.envrc`, entered with `devenv shell`.
    Devenv { root: PathBuf, directory: PathBuf },
    /// No shell to enter.
    Bare,
}

struct ShellSelection {
    entry: ShellEntry,
    /// The devenv project whose evaluated profile bootstraps PATH, if any.
    devenv_dir: Option<PathBuf>,
}

/// Select the shell entry for `cwd`: the nearest `.envrc` walking up to the workspace
/// boundary, else a configured devenv project, else none. An unrelated ancestor's `.envrc`
/// outside the workspace is neither authorized nor evaluated.
fn select_shell(
    sandbox: &SandboxConfig,
    cwd: &Path,
    configured_dir: Option<&Path>,
) -> Result<ShellSelection> {
    let project = shell_project(&sandbox.workspace_mount, cwd, configured_dir)?;
    let mut envrc_directory = None;
    for directory in cwd
        .ancestors()
        .take_while(|directory| directory.starts_with(&sandbox.workspace_mount))
    {
        if shell_input_exists(&sandbox.workspace_mount, &directory.join(".envrc"))? {
            envrc_directory = Some(directory.to_path_buf());
            break;
        }
    }
    let devenv_dir = project.as_ref().map(|(_, directory)| directory.clone());
    let entry = match (envrc_directory, project) {
        (Some(directory), _) => ShellEntry::Envrc(directory),
        (None, Some((root, directory))) => ShellEntry::Devenv { root, directory },
        (None, None) => ShellEntry::Bare,
    };
    Ok(ShellSelection { entry, devenv_dir })
}

/// One-shot activation is part of the executed child: it inherits the same sandbox, pipes and
/// process group, and its failure is the job's failure. Only constant scripts are shell code;
/// cwd and the complete original argv remain positional arguments, never interpolated shell
/// code.
fn wrap_one_shot(plan: &mut SpawnPlan, entry: &ShellEntry) {
    let mut activation = match entry {
        // Approval is private to this workspace (DIRENV_CONFIG/XDG_DATA_HOME), never the
        // user's host trust database. No workspace code executes until sandbox-exec.
        ShellEntry::Envrc(directory) => vec![
            OsString::from("/bin/sh"),
            OsString::from("-c"),
            OsString::from(
                r#"directory=$1; shift; direnv allow "$directory/.envrc" && exec direnv exec "$directory" "$@""#,
            ),
            OsString::from("cowshed-direnv"),
            directory.as_os_str().to_owned(),
        ],
        ShellEntry::Devenv { root, directory } => {
            let mut source = OsString::from("path:");
            source.push(directory);
            vec![
                OsString::from("/bin/sh"),
                OsString::from("-c"),
                OsString::from(
                    r#"root=$1; source=$2; cwd=$3; shift 3; cd "$root" && exec devenv --from "$source" shell -- /bin/sh -c 'cd "$1" && shift && exec "$@"' cowshed-command "$cwd" "$@""#,
                ),
                OsString::from("cowshed-devenv"),
                root.as_os_str().to_owned(),
                source,
                plan.cwd.as_os_str().to_owned(),
            ]
        }
        ShellEntry::Bare => return,
    };
    activation.extend(plan.args.drain(3..));
    plan.args.extend(activation);
}

/// Resolve a store-backed profile for bootstrap tool discovery only.
///
/// Canonical activation owns the resulting PATH. These existing profiles only make direnv and
/// devenv reachable before activation; both locations retain the immutable store guard.
fn workspace_profile_bin(workspace_mount: &Path, devenv_dir: &Path) -> Option<PathBuf> {
    [devenv_dir, workspace_mount].into_iter().find_map(|root| {
        let resolved = fs::canonicalize(root.join(DEVENV_PROFILE_BIN)).ok()?;
        resolved.starts_with("/nix/store").then_some(resolved)
    })
}

/// The Nix profiles that belong to the host's user and the host itself rather than to any
/// shell: where `nix profile`, home-manager, nix-darwin and NixOS install tools. The daemon
/// starts workspace supervisors with launchd's (or systemd's) PATH, which names none of them,
/// so `direnv` and `devenv` are looked up here instead of on whatever PATH started the
/// supervisor. Nearest the user first.
fn host_profile_bins(home: &Path, user: Option<&OsStr>) -> Vec<PathBuf> {
    let mut profiles = vec![
        home.join(".nix-profile/bin"),
        home.join(".local/state/nix/profile/bin"),
    ];
    if let Some(user) = user {
        profiles.push(Path::new("/etc/profiles/per-user").join(user).join("bin"));
        profiles.push(
            Path::new("/nix/var/nix/profiles/per-user")
                .join(user)
                .join("profile/bin"),
        );
    }
    profiles.push(PathBuf::from("/run/current-system/sw/bin"));
    profiles.push(PathBuf::from("/nix/var/nix/profiles/default/bin"));
    profiles
}

/// The effective user's login name, from the password database rather than the environment.
fn effective_user_name() -> Option<OsString> {
    let mut entry = std::mem::MaybeUninit::<libc::passwd>::zeroed();
    let mut buffer = vec![0_u8; 4096];
    let mut found = std::ptr::null_mut();
    // SAFETY: `entry` and `buffer` are writable for their declared sizes and outlive the call;
    // on success `found` points into them.
    let status = unsafe {
        libc::getpwuid_r(
            libc::geteuid(),
            entry.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut found,
        )
    };
    if status != 0 || found.is_null() {
        return None;
    }
    // SAFETY: getpwuid_r succeeded, so `pw_name` is a NUL-terminated string inside `buffer`.
    let name = unsafe { std::ffi::CStr::from_ptr((*found).pw_name) };
    Some(OsString::from_vec(name.to_bytes().to_vec()))
}

fn bootstrap_path(sandbox: &SandboxConfig, devenv_dir: Option<&Path>) -> Result<OsString> {
    bootstrap_path_from(
        sandbox,
        devenv_dir,
        &host_profile_bins(&sandbox.home, effective_user_name().as_deref()),
        std::env::var_os("PATH").as_deref(),
    )
}

fn bootstrap_path_from(
    sandbox: &SandboxConfig,
    devenv_dir: Option<&Path>,
    host_profiles: &[PathBuf],
    inherited: Option<&OsStr>,
) -> Result<OsString> {
    let mut paths = vec![sandbox.workspace_mount.join(".cowshed/bin")];
    if let Some(profile) = workspace_profile_bin(
        &sandbox.workspace_mount,
        devenv_dir.unwrap_or(&sandbox.workspace_mount),
    ) {
        paths.push(profile);
    }
    // A host profile is bootstrap authority only when it resolves to immutable store content.
    for profile in host_profiles {
        if let Ok(profile) = fs::canonicalize(profile)
            && profile.starts_with("/nix/store")
            && !paths.contains(&profile)
        {
            paths.push(profile);
        }
    }
    let mut seen = paths.iter().cloned().collect::<BTreeSet<_>>();
    if let Some(path) = developer_directory().map(|directory| directory.join("usr/bin"))
        && seen.insert(path.clone())
    {
        paths.push(path);
    }
    for fixed in ["/usr/bin", "/bin", "/usr/sbin", "/sbin"] {
        let path = PathBuf::from(fixed);
        seen.insert(path.clone());
        paths.push(path);
    }
    if let Some(inherited) = inherited {
        for path in std::env::split_paths(inherited) {
            let admitted = path.is_absolute()
                && [
                    Path::new("/nix/store"),
                    Path::new("/run/current-system"),
                    // Nix per-user profiles. `/etc/profiles/per-user/<user>/bin` is where
                    // nix-darwin and NixOS put a user's installed tools — the same immutable
                    // store-backed class as /nix/store, reached through a stable symlink. Omitting
                    // it made every nix-installed verify command unrunnable inside a workspace
                    // while the identical command worked in the user's own shell.
                    Path::new("/etc/profiles"),
                    Path::new("/etc/static/profiles"),
                    Path::new("/opt"),
                    Path::new("/System"),
                    Path::new("/Library"),
                ]
                .iter()
                .any(|root| path.starts_with(root));
            if admitted && seen.insert(path.clone()) {
                paths.push(path);
            }
        }
    }
    std::env::join_paths(paths)
        .map_err(|error| CowshedError::internal(format!("construct sandbox PATH: {error}")))
}

fn developer_directory() -> Option<PathBuf> {
    let configured = std::env::var_os("DEVELOPER_DIR").map(PathBuf::from);
    configured
        .into_iter()
        .chain([
            PathBuf::from("/Applications/Xcode.app/Contents/Developer"),
            PathBuf::from("/Library/Developer/CommandLineTools"),
        ])
        .find(|path| {
            path.is_absolute()
                && path.is_dir()
                && [
                    Path::new("/Applications"),
                    Path::new("/Library/Developer"),
                    Path::new("/System"),
                ]
                .iter()
                .any(|root| path.starts_with(root))
        })
}

/// The `HTTP_PROXY` value for a workspace's own gateway endpoint.
///
/// The token rides as basic-auth userinfo because that is the only channel a standard client has:
/// curl, libcurl (so cargo), reqwest, and Go all turn proxy userinfo into `Proxy-Authorization:
/// Basic` on the first CONNECT, and none of them can be told to send cowshed's `Bearer` spelling.
/// Without it every CONNECT is rejected, and cargo reads the rejection as a spurious network error
/// and retries its whole ladder before failing.
///
/// This exports no authority the sandbox lacks: it also receives `COWSHED_WORKSPACE_TOKEN`, and
/// the token authenticates against nothing but this workspace's own loopback port. The username is
/// a fixed label the gateway does not compare. The token's alphabet is unpadded base64url, which
/// is userinfo-safe, so the value never needs percent-encoding.
fn gateway_proxy_url(port_base: &str, workspace_token: &WorkspaceToken) -> String {
    format!(
        "http://cowshed:{}@127.0.0.1:{port_base}",
        workspace_token.encode()
    )
}

/// The private XDG roots under which Nix keeps client state, each named the same as its shared
/// directory under `<caches>/nix`: `<root>/<name>/nix` links to `<caches>/nix/<name>`.
///
/// Nix keeps its fetcher cache (URL and lock-hash to store path), tarball cache, git cache and
/// evaluation cache under `$XDG_CACHE_HOME/nix`, and profile and channel state under
/// `$XDG_STATE_HOME/nix`. cowshed hands every child private XDG roots, so a freshly minted
/// workspace would start with an EMPTY nix cache and the sandboxed `devenv print-dev-env`
/// evaluation would re-fetch every flake input the lock names — through the gateway proxy, which
/// admits nothing without an egress grant. The store paths already exist (the host fetched them),
/// only the client-side index is missing. Sharing one directory each on the caches volume, which
/// the executed-child profile carves back read-write, lets every workspace see what any one of
/// them has fetched; nix serialises access to its sqlite indexes itself.
const NIX_CLIENT_DIRECTORIES: [&CStr; 2] = [c"cache", c"state"];

/// Create the private `home`, `config`, `data`, `run` and Nix client (`cache`, `state`) roots
/// under `environment_root`, point each Nix client root's `nix` at its shared directory under
/// `caches`, and create the [`crate::sandbox::DIRECT_TOOL_CACHES`] there: a child granted writes
/// inside one of those cannot create its parent, and nothing on the host is relocated for them.
///
/// A host without the caches root gets no links and the private directories stand (a CI runner
/// or a box before `cowshed setup` has no caches volume, and an environment that cannot be
/// shared must not fail the spawn), a link that already resolves to the shared directory is
/// kept, a stale link is replaced, and a real directory a workspace already owns is left alone.
///
/// Returns the held environment directory, through which the host publishes the rest of the
/// private environment.
fn prepare_private_environment(
    environment_root: &Path,
    caches: &Path,
) -> Result<AnchoredDirectory> {
    // Keep directory capabilities through link preparation: a running child
    // may rename these paths, but cannot redirect a host write through a link.
    let environment =
        AnchoredDirectory::create(environment_root).map_err(private_environment_error)?;
    for name in [c"home", c"config", c"data", c"run"] {
        environment.child(name).map_err(private_environment_error)?;
    }
    let shared_nix = if caches.is_dir() {
        for directory in crate::sandbox::DIRECT_TOOL_CACHES {
            AnchoredDirectory::create(&caches.join(directory))
                .map_err(private_environment_error)?;
        }
        Some(
            AnchoredDirectory::create(caches)
                .and_then(|caches| caches.child(c"nix"))
                .map_err(private_environment_error)?,
        )
    } else {
        None
    };
    for name in NIX_CLIENT_DIRECTORIES {
        let private = environment.child(name).map_err(private_environment_error)?;
        if let Some(shared_nix) = &shared_nix {
            shared_nix.child(name).map_err(private_environment_error)?;
            let target = caches.join("nix").join(OsStr::from_bytes(name.to_bytes()));
            private
                .ensure_symlink(c"nix", &target)
                .map_err(private_environment_error)?;
        }
    }
    Ok(environment)
}

fn private_environment_error(error: io::Error) -> CowshedError {
    let message = format!("cannot safely prepare the sandbox private environment: {error}");
    let hint = "repair the workspace private environment, then retry cowshed exec";
    if error.raw_os_error() == Some(libc::ELOOP)
        || error.kind() == io::ErrorKind::NotADirectory
        || error.kind() == io::ErrorKind::InvalidInput
    {
        CowshedError::integrity(message, hint)
    } else {
        CowshedError::environment_missing(message, hint)
    }
}

/// Points [`sandbox_runtime_link`] at the runtime dir. Host-side, before the child spawns;
/// idempotent.
async fn link_runtime_dir(sandbox: &SandboxConfig, runtime_dir: &Path) -> Result<()> {
    point_runtime_link(&sandbox_runtime_link(sandbox), runtime_dir).await
}

/// Points `link` at `runtime_dir`. A link that already points there is the whole answer: every
/// spawn of a live workspace takes that path, so it reads one link and nothing else. Creating or
/// retargeting the link also sweeps every `cs-*` link beside it whose target is gone - a retired
/// workspace takes its mount with it and leaves the link dangling - so the scan of the shared
/// directory is paid once per link a workspace takes, never once per command.
async fn point_runtime_link(link: &Path, runtime_dir: &Path) -> Result<()> {
    let io = |what: &str, path: &Path, error: std::io::Error| {
        CowshedError::environment_missing(
            format!("cannot {what} {}: {error}", path.display()),
            "reattach the workspace and retry",
        )
    };
    match tokio::fs::read_link(link).await {
        Ok(target) if target == runtime_dir => return Ok(()),
        Ok(_) => {
            tokio::fs::remove_file(link)
                .await
                .map_err(|error| io("replace runtime link", link, error))?;
            tokio::fs::symlink(runtime_dir, link)
                .await
                .map_err(|error| io("create runtime link", link, error))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::symlink(runtime_dir, link)
                .await
                .map_err(|error| io("create runtime link", link, error))?;
        }
        Err(error) => return Err(io("inspect runtime link", link, error)),
    }
    let Some(directory) = link.parent() else {
        return Ok(());
    };
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("cs-") {
            continue;
        }
        let path = entry.path();
        let Ok(target) = tokio::fs::read_link(&path).await else {
            continue;
        };
        if tokio::fs::metadata(&target).await.is_err() {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
    Ok(())
}

/// The caller's variables a child may take: all but the Git configuration channels the managed
/// fetch include must not be bypassed through. What the sandbox owns or withholds is laid over
/// this by [`SandboxEnvironment`].
pub(super) fn caller_environment(
    caller: &BTreeMap<String, String>,
) -> impl Iterator<Item = (&str, &str)> {
    caller
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .filter(|(name, _)| crate::workspace_git_fetch::caller_git_environment_allowed(name))
}

/// The environment every child of this workspace starts from, split by who may change what.
///
/// A one-shot child receives the caller's variables through [`caller_environment`] with these
/// merged over them; a warm exec host starts from exactly [`SandboxEnvironment::base`] and
/// receives the caller's variables per command as [`SandboxEnvironment::overlay`]. Either way a
/// name the sandbox owns or withholds never takes the caller's value.
pub(super) struct SandboxEnvironment {
    /// Set for every child; the caller's value for these names never passes.
    owned: BTreeMap<OsString, OsString>,
    /// Removed from whatever the caller supplied.
    withheld: Vec<&'static str>,
    /// Set unless the caller named the variable itself.
    defaults: BTreeMap<OsString, OsString>,
    /// A line the sandbox appends to the caller's value, or the whole value when the caller
    /// named none, so the sandbox's line is the one that takes effect.
    appended: BTreeMap<OsString, String>,
}

impl SandboxEnvironment {
    fn reserved(&self, name: &str) -> bool {
        self.owned.contains_key(OsStr::new(name)) || self.withheld.contains(&name)
    }

    /// `line` after the caller's own non-empty value, or `line` alone.
    fn append(caller: Option<&str>, line: &str) -> OsString {
        match caller {
            Some(caller) if !caller.is_empty() => format!("{caller}\n{line}").into(),
            _ => line.into(),
        }
    }

    /// The complete environment of a one-shot child, before shell activation.
    fn child(&self, caller: &BTreeMap<String, String>) -> BTreeMap<OsString, OsString> {
        let mut environment: BTreeMap<OsString, OsString> = caller_environment(caller)
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        for name in &self.withheld {
            environment.remove(OsStr::new(name));
        }
        environment.extend(self.owned.clone());
        for (name, value) in &self.defaults {
            environment
                .entry(name.clone())
                .or_insert_with(|| value.clone());
        }
        for (name, line) in &self.appended {
            let caller = name.to_str().and_then(|name| caller.get(name));
            environment.insert(name.clone(), Self::append(caller.map(String::as_str), line));
        }
        environment
    }

    /// The environment a warm exec host is started with and activates from.
    pub(super) fn base(&self) -> BTreeMap<OsString, OsString> {
        let mut environment = self.owned.clone();
        environment.extend(self.defaults.clone());
        for (name, line) in &self.appended {
            environment.insert(name.clone(), Self::append(None, line));
        }
        environment
    }

    /// The caller's variables one command adds over its host's activated environment; applied
    /// over [`Self::base`] they give exactly what [`Self::child`] gives a one-shot child.
    pub(super) fn overlay(&self, caller: &BTreeMap<String, String>) -> Vec<(OsString, OsString)> {
        caller_environment(caller)
            .filter(|(name, _)| !self.reserved(name))
            .map(|(name, value)| {
                let merged = match self.appended.get(OsStr::new(name)) {
                    Some(line) => Self::append(Some(value), line),
                    None => value.into(),
                };
                (name.into(), merged)
            })
            .collect()
    }
}

/// Prepare the workspace's private environment host-side and describe the child environment.
///
/// Shell activation runs inside the sandbox, after this. The PATH here discovers bootstrap
/// tools; activation owns PATH from then on.
pub(super) async fn sandbox_environment(
    sandbox: &SandboxConfig,
    devenv_dir: Option<&Path>,
    caller: &BTreeMap<String, String>,
) -> Result<SandboxEnvironment> {
    let private_root = sandbox.workspace_mount.join(".cowshed");
    // This directory contains controller-published credentials and Git policy. Reject an
    // inherited symlink before any host-side preparation writes through it.
    crate::storage::verify_no_symlinks(&sandbox.workspace_mount, &private_root).map_err(
        |error| {
            CowshedError::integrity(
                format!("unsafe workspace metadata directory: {error}"),
                "repair the workspace metadata directory and reattach",
            )
        },
    )?;
    // direnv approval and tool state use the already-authorized exec-temp
    // carve-back for read-only jobs, not a new workspace-wide write grant.
    let environment_root = match sandbox.mode {
        crate::sandbox::RunSandboxMode::ReadOnly => &sandbox.exec_temp_dir,
        crate::sandbox::RunSandboxMode::ReadWrite => &private_root,
    };
    let private_home = environment_root.join("home");
    let private_config = environment_root.join("config");
    let private_cache = environment_root.join("cache");
    let private_data = environment_root.join("data");
    let private_state = environment_root.join("state");
    let private_runtime = sandbox_runtime_dir(sandbox);
    let environment = prepare_private_environment(
        environment_root,
        Path::new(crate::storage::bootstrap::CACHES_ROOT),
    )?;
    // TMPDIR must exist even when read-write environment state lives elsewhere.
    if sandbox.mode == crate::sandbox::RunSandboxMode::ReadWrite {
        AnchoredDirectory::create(&sandbox.exec_temp_dir).map_err(private_environment_error)?;
    }
    let token_path = sandbox
        .workspace_mount
        .join(crate::workspace_credentials::WORKSPACE_TOKEN_PATH);
    let encoded_token = tokio::fs::read_to_string(&token_path)
        .await
        .map_err(|error| {
            CowshedError::integrity(
                format!(
                    "cannot read workspace token {}: {error}",
                    token_path.display()
                ),
                "reattach the workspace to mint fresh credentials",
            )
        })?;
    let workspace_token = WorkspaceToken::parse(encoded_token.trim()).map_err(|error| {
        CowshedError::integrity(
            format!(
                "workspace token is malformed at {}: {error}",
                token_path.display()
            ),
            "reattach the workspace to mint fresh credentials",
        )
    })?;
    link_runtime_dir(sandbox, &private_runtime).await?;
    // Host-side preparation: adopted bindings are controller metadata, not child-readable
    // files. The Git directory probe runs under the narrower GitDiscovery child profile.
    // Refresh each spawn so revoked grants and relocated checkouts cannot leave stale routes.
    let git_fetch_config = crate::workspace_git_fetch::refresh_git_fetch_config(sandbox).await?;
    // Identity is the workspace's own published file or nothing at all. A workspace minted
    // before capture existed keeps the empty device and fails an authorless commit loudly,
    // rather than silently borrowing whatever the controller's user happens to be.
    let git_identity = crate::git::workspace_git_identity_config(&sandbox.workspace_mount)?;
    let path = bootstrap_path(sandbox, devenv_dir)?;
    let port_base = sandbox.port_block.base().to_string();
    let encoded_token = workspace_token.encode();
    let gateway_http = gateway_proxy_url(&port_base, &workspace_token);
    let runtime_link = sandbox_runtime_link(sandbox);

    // Local services already have a bounded direct-connect capability. Sending
    // them through the external gateway incorrectly requires an egress grant.
    // The sandbox still rejects loopback ports outside this workspace's block.
    let loopback_no_proxy = "localhost,127.0.0.1,::1";
    let mut owned: BTreeMap<OsString, OsString> = BTreeMap::new();
    let mut withheld: Vec<&'static str> = Vec::new();
    let mut own = |name: &str, value: &OsStr| {
        owned.insert(name.into(), value.to_owned());
    };
    own("PATH", &path);
    own("HOME", private_home.as_os_str());
    own("XDG_CONFIG_HOME", private_config.as_os_str());
    own("XDG_CACHE_HOME", private_cache.as_os_str());
    own("XDG_DATA_HOME", private_data.as_os_str());
    own("XDG_STATE_HOME", private_state.as_os_str());
    own("DIRENV_CONFIG", private_config.join("direnv").as_os_str());
    own(
        "GIT_CONFIG_GLOBAL",
        git_identity
            .as_deref()
            .unwrap_or(Path::new("/dev/null"))
            .as_os_str(),
    );
    own("GIT_CONFIG_NOSYSTEM", OsStr::new("1"));
    own("GIT_ATTR_NOSYSTEM", OsStr::new("1"));
    own("TMPDIR", sandbox.exec_temp_dir.as_os_str());
    // devenv resolves its runtime directory as `$XDG_RUNTIME_DIR/devenv-<hash>`, falling
    // back to `/tmp` when the variable is unset, and ignores TMPDIR by design (its runtime
    // dir must rendezvous across invocations that may carry different TMPDIRs). The child
    // gets the short `/tmp/cs-<port>` link: the shed's runtime dir under a name that leaves
    // `sun_path` room for the sockets devenv keeps there.
    own("XDG_RUNTIME_DIR", runtime_link.as_os_str());
    // Nx ignores XDG_RUNTIME_DIR and otherwise falls back to a world-shared
    // directory or a private HOME path longer than Unix sockets permit.
    // Its O_NOFOLLOW admission requires a real leaf below the short alias.
    own("NX_SOCKET_DIR", runtime_link.join("nx").as_os_str());
    // A sandboxed Nx runs without a daemon. A client finds the daemon through the record the
    // daemon writes into the checkout's workspace-data directory, which names its socket (the
    // socket directory decides nothing), and host shells use the same checkout: a daemon started
    // here would become their daemon, computing their project graph and running their runtime
    // inputs inside this sandbox with the host client's environment. Moving the record means
    // moving the workspace-data directory, which holds the task database that indexes the
    // checkout's Nx cache, and a sandbox with a database of its own never hits what the host or
    // main cached. Without a daemon both boundaries share one cache; a hit is copied back (with
    // its timestamps) rather than left in place.
    own("NX_DAEMON", OsStr::new("false"));
    own(GO_ENV, private_cache.join("go/env").as_os_str());
    // Rust routes through sccache in every workspace of a host that pinned one. Cargo's
    // `-C metadata` is path-independent for workspace members (cargo >= 1.97, measured), and the
    // pinned sccache normalizes the residual path-bearing key inputs (cwd, blanket `CARGO_*` env,
    // argument bytes) against the request cwd when the client sets `SCCACHE_BASEDIR_CWD=1` — so
    // name-mounted workspaces share entries with each other, not just successive slot tenants.
    // env-dep values stay unnormalized in the key, so a crate that compiles
    // `env!("CARGO_MANIFEST_DIR")` into its output still fail-closes across paths.
    //
    // The wrapper is the pinned program itself, read through the GC root before every spawn, not
    // a name for `PATH` to resolve: shell activation owns `PATH`, and a repository shell without
    // sccache left a bare `sccache` unresolvable, failing every cargo command at its version
    // probe. A host that pinned none builds without a wrapper — sccache is opt-in — rather than
    // against a daemon that is not there. Neither variable is the caller's: the Seatbelt profile
    // admits exactly the host daemon's socket and denies binding it, so a caller that pointed the
    // wrapper elsewhere would be reaching outside the boundary. rustc-wrapper clients speak to
    // that host-owned daemon, and a client whose daemon is down fails fast instead of spawning a
    // wrong-boundary server inside the sandbox.
    //
    // `CARGO_INCREMENTAL` is deliberately not the sandbox's, so whatever the caller names arrives
    // verbatim and an unnamed one stays unset. Cargo then decides per profile, which is the right
    // decision for both halves of a build at once: workspace crates stay incremental and local,
    // while dependencies are always non-incremental and so reach sccache without anyone forcing
    // anything. Forcing 0 cost every interactive build a full recompile — measured on a one-line
    // edit to a mid-size crate, ~1.7s incremental against ~20-32s with `CARGO_INCREMENTAL=0`.
    match crate::sandbox::sccache_client(&sandbox.home) {
        Some(client) => own("RUSTC_WRAPPER", client.as_os_str()),
        None => withheld.push("RUSTC_WRAPPER"),
    }
    own("SCCACHE_BASEDIR_CWD", OsStr::new("1"));
    own(
        "SCCACHE_SERVER_UDS",
        crate::sandbox::sccache_server_socket().as_os_str(),
    );
    own(
        "SCCACHE_DIR",
        crate::sandbox::sccache_cache_directory().as_os_str(),
    );
    own(PORT_BASE_ENV, OsStr::new(&port_base));
    own(
        PORT_BLOCK_SIZE_ENV,
        OsStr::new(&sandbox.port_block.size().to_string()),
    );
    own(WORKSPACE_TOKEN_ENV, OsStr::new(&encoded_token));
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
        own(name, OsStr::new(&gateway_http));
    }
    own("NO_PROXY", OsStr::new(loopback_no_proxy));
    own("no_proxy", OsStr::new(loopback_no_proxy));
    // The host and every workspace reach a shared tool home through one literal path (cargo
    // fingerprints dependencies by it, Bun's isolated linker writes it into `node_modules`), so
    // the variable names the host path once the tool's caches are shared, and never passes a
    // caller's value through: without shared caches the tool follows the private HOME.
    for (variable, host_path) in sandbox.shared_tool_environment() {
        match host_path {
            Some(host_path) => own(variable, host_path.as_os_str()),
            None => withheld.push(variable),
        }
    }
    // Cargo only honors url.insteadOf through the Git CLI, so every child
    // fetches through it; uv shells out to Git and follows the same include
    // with no extra wiring. The include points at the managed file
    // regenerated above, layered over the isolated GLOBAL set just above —
    // the workspace's own identity file or the empty device, never the
    // user's or the system's configuration. With no mapping the count is
    // pinned to zero so caller-supplied GIT_CONFIG_KEY_* entries cannot
    // smuggle configuration in.
    own(
        crate::workspace_git_fetch::CARGO_NET_GIT_FETCH_WITH_CLI_ENV,
        OsStr::new(crate::workspace_git_fetch::CARGO_NET_GIT_FETCH_WITH_CLI_VALUE),
    );
    match &git_fetch_config {
        Some(path) => {
            for (key, value) in crate::workspace_git_fetch::git_fetch_include_env(path) {
                own(key, value);
            }
        }
        None => {
            own("GIT_CONFIG_COUNT", OsStr::new("0"));
            withheld.extend(["GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"]);
        }
    }
    for key in ["LANG", "LC_ALL", "LC_CTYPE", "TERM", "COLORTERM"] {
        if let Some(value) = std::env::var_os(key) {
            own(key, &value);
        }
    }
    // Mirror, never invent: a workspace shell must see the same toolchain
    // selection as the host shell that adopted it. xcrun and xcode-select
    // resolve the system default inside the sandbox on their own (measured),
    // so an injected Xcode DEVELOPER_DIR adds nothing when the host has none —
    // and it makes CMake resolve Xcode's SDK for a Nix clang whose sysroot is
    // the Nix apple-sdk, which fails on the first header (`uint8_t` unknown in
    // sys/resource.h) while the identical build passes in the host shell.
    // The developer directory still joins PATH above so its tools are found.
    if let Some(directory) = std::env::var_os("DEVELOPER_DIR") {
        own("DEVELOPER_DIR", &directory);
    }
    // Every intercepted HTTPS origin is presented with a leaf this workspace's CA signed, so a
    // client that does not trust that CA cannot reach the registry at all — measured on the
    // pinned Bun as `UNABLE_TO_VERIFY_LEAF_SIGNATURE downloading package manifest`. The
    // certificate is the public half and already travels in the image; only the anchor wiring
    // was missing.
    //
    // NODE_EXTRA_CA_CERTS is additive and belongs to Node and Bun alone, so it can be set
    // without touching SSL_CERT_FILE — which nix and devenv own for the whole toolchain, and
    // which is not ours to redirect. Nothing here relaxes verification: the anchor is added, no
    // check is disabled.
    //
    // A caller that set the variable itself keeps it. Node reads exactly one file, so there is
    // no honest merge; silently replacing an operator's anchor would change what a build was
    // compiled against without saying so, and the alternative — refusing to spawn — is worse
    // for a variable that may be entirely unrelated. It is announced instead.
    let mut defaults = BTreeMap::new();
    let anchor = sandbox
        .workspace_mount
        .join(crate::workspace_credentials::CA_CERTIFICATE_PATH);
    if tokio::fs::try_exists(&anchor).await.unwrap_or(false) {
        defaults.insert(OsString::from(NODE_CA_ENV), anchor.into_os_string());
    }
    if caller.contains_key(NODE_CA_ENV) {
        eprintln!(
            "cowshed: {NODE_CA_ENV} was supplied by the caller; the workspace gateway CA is not \
             being added, so an intercepted HTTPS origin may fail to verify"
        );
    }
    // Registry clients find the gateway's mirror routes, and one-file TLS clients find a bundle
    // that trusts the workspace CA, in the private environment (crate::workspace_clients) —
    // republished before every spawn so a rotated token or a moved endpoint is never served
    // stale. The mirror files need only the endpoint and the token; the bundle needs the CA.
    let anchor = sandbox
        .workspace_mount
        .join(crate::workspace_credentials::CA_CERTIFICATE_PATH);
    let workspace_ca = match tokio::fs::read(&anchor).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(CowshedError::integrity(
                format!("cannot read workspace CA {}: {error}", anchor.display()),
                "reattach the workspace to mint fresh credentials",
            ));
        }
    };
    let system_bundle = match &workspace_ca {
        Some(_) => crate::workspace_clients::system_trust_bundle().map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot read the platform trust bundle {}: {error}",
                    crate::workspace_clients::SYSTEM_TRUST_BUNDLE
                ),
                "restore the platform CA bundle, then retry cowshed exec",
            )
        })?,
        None => Vec::new(),
    };
    let gateway_base = format!("http://127.0.0.1:{port_base}");
    let token = workspace_token.encode();
    crate::workspace_clients::publish_client_wiring(
        &environment,
        &crate::workspace_clients::ClientWiring {
            gateway_http: &gateway_base,
            token: &token,
            workspace_ca: workspace_ca.as_deref(),
            system_bundle: &system_bundle,
            environment: environment_root,
        },
    )
    .map_err(private_environment_error)?;
    let mut appended = BTreeMap::new();
    if workspace_ca.is_some() {
        let bundle = environment_root.join(
            crate::workspace_clients::TRUST_BUNDLE_NAME
                .to_str()
                .expect("the bundle name is ASCII"),
        );
        // Same rule as NODE_EXTRA_CA_CERTS above: a caller's own anchor is kept and announced.
        // The bundle is the platform roots plus the workspace CA, a superset of what the toolchain
        // would otherwise point SSL_CERT_FILE at, so every client that reads it keeps its trust.
        for name in [
            crate::workspace_clients::GIT_CA_ENV,
            crate::workspace_clients::CARGO_CA_ENV,
            crate::workspace_clients::NIX_CA_ENV,
            crate::workspace_clients::SSL_CERT_ENV,
        ] {
            if caller.contains_key(name) {
                eprintln!(
                    "cowshed: {name} was supplied by the caller; the workspace trust bundle is not \
                     being used for it, so an intercepted HTTPS origin may fail to verify"
                );
            }
            defaults.insert(OsString::from(name), bundle.clone().into_os_string());
        }
        // uv ignores SSL_CERT_FILE until told to verify with system certificates; a caller that
        // sets UV_SYSTEM_CERTS itself keeps its choice.
        defaults.insert(
            OsString::from(crate::workspace_clients::UV_SYSTEM_CERTS_ENV),
            OsString::from("true"),
        );
        // A host `nix.conf` that names its own `ssl-cert-file` outranks NIX_SSL_CERT_FILE; only
        // NIX_CONFIG, applied after every config file, outranks it. A caller's NIX_CONFIG keeps
        // its lines, with this one last so it is the setting nix applies.
        appended.insert(
            OsString::from(crate::workspace_clients::NIX_CONFIG_ENV),
            format!("ssl-cert-file = {}", bundle.display()),
        );
    }
    Ok(SandboxEnvironment {
        owned,
        withheld,
        defaults,
        appended,
    })
}

/// Build the sandboxed `Command` for a one-shot child of this workspace.
///
/// Shell activation runs inside this command, after the sandbox and private environment are
/// established. No environment is extracted, merged or rewritten after activation; `env_clear`
/// means nothing is inherited that is not named in `environment`.
fn sandboxed_command(
    plan: &SpawnPlan,
    environment: &BTreeMap<OsString, OsString>,
) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(&plan.program);
    command
        .env_clear()
        .args(&plan.args)
        .current_dir(&plan.cwd)
        .envs(environment)
        .env("PWD", &plan.cwd);
    command
}

/// Spawns a workspace's children. With a shell host program, commands whose cwd sits under a
/// workspace `.envrc` run in warm exec hosts; everything else, and every command when no host
/// program was given, enters the shell one-shot inside its own child.
#[derive(Default)]
pub struct SystemSpawnSink {
    shells: Option<super::shell_job::WorkspaceShells>,
}

impl SystemSpawnSink {
    pub fn with_shell_host(
        program: super::shell_host::ShellHostProgram,
        config: super::shell_pool::ShellPoolConfig,
    ) -> Self {
        Self {
            shells: Some(super::shell_job::WorkspaceShells::new(program, config)),
        }
    }
}

#[async_trait]
impl SpawnSink for SystemSpawnSink {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        let (sandbox, profile) = request.policy.child(request.mode);
        let cwd = crate::exec::contained_cwd(&sandbox.workspace_mount, &request.cwd)
            .map_err(map_exec_error)?;
        let selection = select_shell(sandbox, &cwd, request.devenv_dir.as_deref())?;
        let environment = crate::timing::spanned(
            "spawn",
            "environment",
            sandbox_environment(sandbox, selection.devenv_dir.as_deref(), &request.env),
        )
        .await?;
        let activation = match &selection.entry {
            ShellEntry::Envrc(directory) => Some(Some(directory.clone())),
            ShellEntry::Bare => Some(None),
            ShellEntry::Devenv { .. } => None,
        };
        let pooled = |command, environment| super::shell_job::PooledSpawn {
            job_id: request.job_id,
            command,
            cwd: cwd.clone(),
            profile,
            read_only: sandbox.mode == crate::sandbox::RunSandboxMode::ReadOnly,
            workspace_mount: sandbox.workspace_mount.clone(),
            envrc_directory: activation.clone().flatten(),
            environment,
            caller: &request.env,
            grant_revision: request.authority.grant_revision,
        };
        let argv = match request.command {
            SpawnCommand::Script(script) => {
                let (Some(shells), Some(_)) = (self.shells.as_mut(), &activation) else {
                    return Err(CowshedError::environment_missing(
                        "a script job runs in the workspace's exec host, which needs the cowshed \
                         binary and a workspace .envrc or no shell configuration at all",
                        "run the script through the cowshed CLI; a devenv-only project needs an \
                         .envrc that uses devenv",
                    ));
                };
                return shells.spawn(
                    pooled(super::shell_job::HostCommand::Script(script), environment),
                    events,
                );
            }
            SpawnCommand::Argv(argv) => argv,
        };
        // Warm hosts serve commands under a workspace `.envrc`; a bare workspace and a
        // devenv-only project keep one-shot activation for argv jobs.
        if let (Some(Some(_)), Some(shells)) = (&activation, self.shells.as_mut()) {
            return shells.spawn(
                pooled(super::shell_job::HostCommand::Argv(argv), environment),
                events,
            );
        }
        let mut plan = plan_exec_under(SandboxExecRequest { argv, cwd }, sandbox, profile)
            .map_err(map_exec_error)?;
        wrap_one_shot(&mut plan, &selection.entry);
        let mut command = sandboxed_command(&plan, &environment.child(&request.env));
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        prepare_child_descriptors(command.as_std_mut())
            .map_err(ExecError::from)
            .map_err(map_exec_error)?;
        // SAFETY: `pre_exec` runs in the forked child, between `fork` and `exec`, in a process
        // that was multithreaded at the fork. Only async-signal-safe calls are legal there, and
        // POSIX lists `setpgid` as one; it allocates nothing, takes no lock, and touches no
        // memory this closure captures. Its success is load-bearing rather than decorative:
        // `kill_process_group` signals `-pid`, which is the child's own group only because the
        // child made itself a group leader here, so a failure is returned and fails the spawn
        // instead of leaving a job whose kill would target the wrong processes.
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let mut child = command
            .spawn()
            .map_err(classify_spawn_error)
            .map_err(ExecError::from)
            .map_err(map_exec_error)?;
        let pid = child.id().ok_or_else(|| {
            CowshedError::internal("spawned sandbox process has no process identity")
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CowshedError::internal("spawned process has no stdin pipe"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CowshedError::internal("spawned process has no stdout pipe"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CowshedError::internal("spawned process has no stderr pipe"))?;
        let job_id = request.job_id;
        let (stdin_sender, stdin_receiver) = mpsc::channel(1);
        tokio::spawn(run_system_stdin(
            job_id,
            stdin,
            stdin_receiver,
            events.clone(),
        ));
        tokio::spawn(run_system_output(
            job_id,
            StreamKind::Stdout,
            stdout,
            events.clone(),
        ));
        tokio::spawn(run_system_output(
            job_id,
            StreamKind::Stderr,
            stderr,
            events.clone(),
        ));
        tokio::spawn(async move {
            let event = match process_termination_from_wait(child.wait().await) {
                Ok(exit) => ProcessEvent::Exited { job_id, exit },
                Err(error) => {
                    // The child was never reaped, so it may still be running. Kill the group
                    // before reporting: a job that cannot be observed must not be left alive
                    // behind a terminal record.
                    let _ = kill_process_group(pid, libc::SIGKILL);
                    ProcessEvent::WaitFailed { job_id, error }
                }
            };
            let _ = events.send(event).await;
        });
        Ok(Box::new(SystemRunningProcess {
            pid,
            stdin: StdinLane::new(stdin_sender),
        }))
    }
}

/// Translate a `wait(2)` result into the job's terminal exit status.
///
/// Every branch that cannot name how the child died returns `Err`. A `wait` that fails, or that
/// reports neither an exit code nor a terminating signal, means the child has not been reaped:
/// answering with a synthesized `SIGKILL` would let `finalize_job` seal the artifact and drain
/// the job's waiters with a successful terminal status while the process is still running.
pub(super) fn process_termination_from_wait(
    waited: io::Result<std::process::ExitStatus>,
) -> Result<ExitStatus> {
    let status = waited.map_err(|error| {
        CowshedError::integrity(
            format!("cannot wait for the sandbox process: {error}"),
            "cowshed doctor --json",
        )
    })?;
    match ProcessStatus::from(status) {
        ProcessStatus::Exit(code) => Ok(ExitStatus::Exited { code }),
        ProcessStatus::Signal(signal) => Ok(ExitStatus::Signaled {
            signal,
            core_dumped: status.core_dumped(),
        }),
        ProcessStatus::Unknown => Err(CowshedError::integrity(
            format!("sandbox process reported {}", ProcessStatus::Unknown),
            "cowshed doctor --json",
        )),
    }
}

/// Signal a process group created by `setpgid(0, 0)` in the child.
///
/// SAFETY: `kill` is a plain syscall with no memory operands, so the only precondition is the
/// argument itself. The negation is only a process-group target for a strictly positive pid:
/// `kill(-1, ...)` is "every process the caller may signal" and `kill(0, ...)` is the caller's
/// own group, so both are rejected before negating rather than escaping the sandbox tree. A
/// group whose last member already exited (`ESRCH`) is the intended outcome, not a failure.
pub(super) fn kill_process_group(pid: u32, signal: i32) -> Result<()> {
    let pid = i32::try_from(pid)
        .ok()
        .filter(|pid| *pid > 1)
        .ok_or_else(|| CowshedError::internal("process id is not a signalable process group"))?;
    if unsafe { libc::kill(-pid, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(CowshedError::environment_missing(
            format!("failed to signal sandbox process tree: {error}"),
            "inspect the job and retry",
        ))
    }
}

pub(super) enum SystemStdin {
    Write(Bytes),
    Close,
}

/// The bounded lane from the actor to a job's stdin pump.
pub(super) struct StdinLane {
    sender: mpsc::Sender<SystemStdin>,
    closed: bool,
}

impl StdinLane {
    pub(super) fn new(sender: mpsc::Sender<SystemStdin>) -> Self {
        Self {
            sender,
            closed: false,
        }
    }

    pub(super) fn try_write(&mut self, bytes: Bytes) -> Result<bool> {
        if self.closed {
            return Err(CowshedError::conflict(
                "job stdin is closed",
                "attach before closing stdin",
            ));
        }
        match self.sender.try_send(SystemStdin::Write(bytes)) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(CowshedError::conflict(
                "job stdin is no longer available",
                "inspect the terminal job status",
            )),
        }
    }

    pub(super) fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        match self.sender.try_send(SystemStdin::Close) {
            Ok(()) => {
                self.closed = true;
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(CowshedError::conflict(
                "job stdin still has a pending write",
                "retry stdin close after the pending write is accepted",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.closed = true;
                Ok(())
            }
        }
    }
}

struct SystemRunningProcess {
    pid: u32,
    stdin: StdinLane,
}

impl RunningProcess for SystemRunningProcess {
    fn pid(&self) -> Option<u32> {
        Some(self.pid)
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        self.stdin.try_write(bytes)
    }

    fn close_stdin(&mut self) -> Result<()> {
        self.stdin.close()
    }

    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()> {
        kill_process_group(
            self.pid,
            match signal {
                ProcessSignal::Term => libc::SIGTERM,
                ProcessSignal::Kill => libc::SIGKILL,
            },
        )
    }

    fn reap_if_exited(&mut self) {
        let Ok(pid) = i32::try_from(self.pid) else {
            return;
        };
        let mut status = 0;
        // SAFETY: `WNOHANG` makes `waitpid` return at once, `status` is a valid out-pointer for
        // the call, and `pid` is this process's own child. A still-running child answers 0 and an
        // already-reaped one ECHILD; both leave nothing to collect, which is the point.
        unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    }
}

pub(super) async fn run_system_stdin<W>(
    job_id: JobId,
    mut stdin: W,
    mut receiver: mpsc::Receiver<SystemStdin>,
    events: mpsc::Sender<ProcessEvent>,
) where
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = receiver.recv().await {
        match message {
            SystemStdin::Write(bytes) => {
                if stdin.write_all(&bytes).await.is_err() {
                    break;
                }
                if events
                    .send(ProcessEvent::StdinReady { job_id })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            SystemStdin::Close => {
                let _ = stdin.shutdown().await;
                break;
            }
        }
    }
}

pub(super) async fn run_system_output<R>(
    job_id: JobId,
    stream: StreamKind,
    mut reader: R,
    events: mpsc::Sender<ProcessEvent>,
) where
    R: AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; PROCESS_IO_CHUNK];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                if events
                    .send(ProcessEvent::Output {
                        job_id,
                        stream,
                        bytes: Bytes::copy_from_slice(&buffer[..count]),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
    let _ = events
        .send(ProcessEvent::OutputEof { job_id, stream })
        .await;
}

/// Clone is cheap: immutable authority plus a bounded actor sender.
#[derive(Clone)]
pub struct WorkspaceSupervisorHandle {
    authority: WorkspaceAuthoritySnapshot,
    commands: mpsc::Sender<Command>,
}

impl std::fmt::Debug for WorkspaceSupervisorHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkspaceSupervisorHandle")
            .field("authority", &self.authority)
            .finish_non_exhaustive()
    }
}

impl WorkspaceSupervisorHandle {
    pub(super) fn from_parts(
        authority: WorkspaceAuthoritySnapshot,
        commands: mpsc::Sender<Command>,
    ) -> Self {
        Self {
            authority,
            commands,
        }
    }

    /// The same supervisor, called under `authority`: the actor fences every call by it.
    pub(super) fn with_authority(&self, authority: WorkspaceAuthoritySnapshot) -> Self {
        Self {
            authority,
            commands: self.commands.clone(),
        }
    }

    pub fn snapshot(&self) -> &WorkspaceAuthoritySnapshot {
        &self.authority
    }

    pub async fn advance_authority(
        &self,
        grant_revision: u64,
        lifecycle_revision: u64,
        sandbox: SandboxConfig,
    ) -> Result<Self> {
        let authority = WorkspaceAuthoritySnapshot {
            repo_id: self.authority.repo_id.clone(),
            workspace: self.authority.workspace.clone(),
            workspace_incarnation: self.authority.workspace_incarnation.clone(),
            grant_revision,
            lifecycle_revision,
        };
        self.call(|reply| Command::AdvanceAuthority {
            expected: self.authority.clone(),
            authority: authority.clone(),
            sandbox,
            reply,
        })
        .await?;
        Ok(Self {
            authority,
            commands: self.commands.clone(),
        })
    }

    pub async fn open_session(&self, name: Option<String>) -> Result<SessionToken> {
        self.call(|reply| Command::OpenSession {
            authority: self.authority.clone(),
            name,
            reply,
        })
        .await
    }

    pub async fn session_snapshot(&self, session: &SessionToken) -> Result<SessionSnapshot> {
        self.call(|reply| Command::SessionSnapshot {
            authority: self.authority.clone(),
            session: session.clone(),
            reply,
        })
        .await
    }

    pub async fn close_session(&self, session: SessionToken) -> Result<()> {
        self.call(|reply| Command::CloseSession {
            authority: self.authority.clone(),
            session,
            reply,
        })
        .await
    }

    pub async fn exec(
        &self,
        session: Option<&SessionToken>,
        request: ExecRequest,
    ) -> Result<JobId> {
        self.exec_admitted(session, request, false).await
    }

    pub async fn exec_background(
        &self,
        session: Option<&SessionToken>,
        request: ExecRequest,
    ) -> Result<JobId> {
        self.exec_admitted(session, request, true).await
    }

    async fn exec_admitted(
        &self,
        session: Option<&SessionToken>,
        request: ExecRequest,
        background: bool,
    ) -> Result<JobId> {
        self.call(|reply| Command::Exec {
            authority: self.authority.clone(),
            session: session.cloned(),
            request,
            background,
            reply,
        })
        .await
    }

    /// Hand this workspace's warm step one land's range: it starts now, or waits behind the warm
    /// job running, merged into the one run waiting there. Answered at once, never at the build's
    /// end.
    pub async fn warm(&self, argv: Vec<CommandArg>, range: WarmRange) -> Result<WarmAdmission> {
        self.call(|reply| Command::Warm {
            authority: self.authority.clone(),
            argv,
            range,
            reply,
        })
        .await
    }

    pub async fn stdin_write(&self, job_id: JobId, bytes: Bytes) -> Result<()> {
        if bytes.len() > PROCESS_IO_CHUNK {
            return Err(CowshedError::usage(
                "stdin write exceeds the 64 KiB bounded frame",
                "split stdin into 64 KiB or smaller chunks",
            ));
        }
        self.call(|reply| Command::StdinWrite {
            authority: self.authority.clone(),
            job_id,
            bytes,
            reply,
        })
        .await
    }

    pub async fn stdin_close(&self, job_id: JobId) -> Result<()> {
        self.call(|reply| Command::StdinClose {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    pub async fn info(&self, job_id: JobId) -> Result<JobInfo> {
        self.call(|reply| Command::Info {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    /// The job's terminal record, for any job of this workspace incarnation that has one —
    /// including a job an earlier supervisor ran and sealed, which [`Self::info`] answers only
    /// while that supervisor serves.
    pub async fn sealed(&self, job_id: JobId) -> Result<SealedJob> {
        self.call(|reply| Command::Sealed {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    pub async fn list(&self) -> Result<Vec<JobInfo>> {
        self.call(|reply| Command::List {
            authority: self.authority.clone(),
            reply,
        })
        .await
    }

    pub async fn kill(&self, job_id: JobId) -> Result<()> {
        self.call(|reply| Command::Kill {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    pub async fn wait(&self, job_id: JobId) -> Result<JobInfo> {
        self.call(|reply| Command::Wait {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    pub async fn log_read(
        &self,
        job_id: JobId,
        stream: StreamKind,
        offset: u64,
        follow: bool,
    ) -> Result<LogChunk> {
        self.call(|reply| Command::LogRead {
            authority: self.authority.clone(),
            job_id,
            stream,
            offset,
            follow,
            reply,
        })
        .await
    }

    pub async fn attach_read(
        &self,
        job_id: JobId,
        stream: StreamKind,
        offset: u64,
    ) -> Result<LogChunk> {
        self.log_read(job_id, stream, offset, true).await
    }

    pub async fn checkpoint_barrier(&self, checkpoint_id: String) -> Result<CheckpointBarrier> {
        self.call(|reply| Command::Checkpoint {
            authority: self.authority.clone(),
            checkpoint_id,
            reply,
        })
        .await
    }

    pub async fn quiesce(&self) -> Result<()> {
        self.call(|reply| Command::Quiesce {
            authority: self.authority.clone(),
            reply,
        })
        .await
    }

    /// The authority the supervisor holds now, which a grant advance may have moved past the
    /// one this handle was made with.
    pub async fn current_authority(&self) -> Result<WorkspaceAuthoritySnapshot> {
        self.call(|reply| Command::CurrentAuthority { reply }).await
    }

    /// Whether the supervisor holds nothing a client could come back for: no named session is
    /// open and no job is running. Only the process serving a supervisor asks, to retire it; that
    /// serving host is the macOS native project host.
    #[cfg(target_os = "macos")]
    pub(super) async fn idle(&self) -> Result<bool> {
        self.call(|reply| Command::Idle { reply }).await
    }

    pub async fn retire(&self) -> Result<()> {
        self.call(|reply| Command::Retire {
            authority: self.authority.clone(),
            reply,
        })
        .await
    }

    async fn call<T>(&self, make: impl FnOnce(oneshot::Sender<Result<T>>) -> Command) -> Result<T> {
        let (reply, receive) = oneshot::channel();
        self.commands.send(make(reply)).await.map_err(|_| {
            CowshedError::environment_missing(
                "workspace supervisor actor is unavailable",
                "reattach the workspace",
            )
        })?;
        receive.await.map_err(|_| {
            CowshedError::environment_missing(
                "workspace supervisor stopped before replying",
                "reattach the workspace",
            )
        })?
    }
}

pub struct WorkspaceSupervisor;

impl WorkspaceSupervisor {
    pub fn start<C>(
        config: WorkspaceSupervisorConfig,
        commitments: C,
    ) -> Result<WorkspaceSupervisorHandle>
    where
        C: CommitmentSink + Send + 'static,
    {
        config.validate()?;
        // A supervisor serves exactly the block its sandbox records, so this is where the
        // workspace's `.cowshed/env` is brought up to date with its metadata.
        crate::workspace_credentials::publish_workspace_environment(
            &config.sandbox.workspace_mount,
            &config.sandbox.workspace_mount,
            crate::metadata::Platform::Macos,
            Some(config.sandbox.port_block),
        )
        .map_err(|error| {
            CowshedError::integrity(
                format!("cannot publish the workspace environment: {error}"),
                "reattach the workspace to mint fresh credentials",
            )
        })?;
        let artifacts = ArtifactStoreSink::open(
            config.workspace_root.clone(),
            &config.owned_repo_ids,
            &config.authority,
            config.artifacts.clone(),
        )?;
        let spawner = match config.shell_host.clone() {
            Some(program) => SystemSpawnSink::with_shell_host(program, config.shell_pool),
            None => SystemSpawnSink::default(),
        };
        Self::start_with_sinks(
            config,
            Box::new(spawner),
            Box::new(artifacts),
            Box::new(commitments),
        )
    }

    pub fn start_with_sinks(
        config: WorkspaceSupervisorConfig,
        spawner: Box<dyn SpawnSink>,
        artifacts: Box<dyn ArtifactSink>,
        commitments: Box<dyn CommitmentSink>,
    ) -> Result<WorkspaceSupervisorHandle> {
        config.validate()?;
        let policy = SandboxPolicy::render(config.sandbox)?;
        let next_job_id = artifacts.next_job_id()?;
        let (commands, receiver) = mpsc::channel(config.actor_capacity);
        let (events, event_receiver) = mpsc::channel(config.event_capacity);
        let handle = WorkspaceSupervisorHandle {
            authority: config.authority.clone(),
            commands,
        };
        let actor = SupervisorActor {
            authority: config.authority,
            workspace_root: config.workspace_root,
            default_cwd: config.default_cwd,
            policy,
            credential_env_names: config.credential_env_names,
            group_ledger: config.group_ledger,
            term_grace: config.term_grace,
            next_job_id,
            next_session_id: 1,
            lifecycle: ActorLifecycle::Running,
            commands: receiver,
            events,
            event_receiver,
            spawner,
            artifacts,
            commitments,
            jobs: BTreeMap::new(),
            sessions: BTreeMap::new(),
            named_sessions: BTreeMap::new(),
            quiesce_waiters: Vec::new(),
            retire_waiters: Vec::new(),
            command_lane_closed: false,
            warm: WarmLane::default(),
        };
        tokio::spawn(actor.run());
        Ok(handle)
    }
}

pub(super) enum Command {
    AdvanceAuthority {
        expected: WorkspaceAuthoritySnapshot,
        authority: WorkspaceAuthoritySnapshot,
        sandbox: SandboxConfig,
        reply: oneshot::Sender<Result<()>>,
    },
    OpenSession {
        authority: WorkspaceAuthoritySnapshot,
        name: Option<String>,
        reply: oneshot::Sender<Result<SessionToken>>,
    },
    SessionSnapshot {
        authority: WorkspaceAuthoritySnapshot,
        session: SessionToken,
        reply: oneshot::Sender<Result<SessionSnapshot>>,
    },
    CloseSession {
        authority: WorkspaceAuthoritySnapshot,
        session: SessionToken,
        reply: oneshot::Sender<Result<()>>,
    },
    Exec {
        authority: WorkspaceAuthoritySnapshot,
        session: Option<SessionToken>,
        request: ExecRequest,
        background: bool,
        reply: oneshot::Sender<Result<JobId>>,
    },
    Warm {
        authority: WorkspaceAuthoritySnapshot,
        argv: Vec<CommandArg>,
        range: WarmRange,
        reply: oneshot::Sender<Result<WarmAdmission>>,
    },
    StdinWrite {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        bytes: Bytes,
        reply: oneshot::Sender<Result<()>>,
    },
    StdinClose {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<()>>,
    },
    Info {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<JobInfo>>,
    },
    Sealed {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<SealedJob>>,
    },
    List {
        authority: WorkspaceAuthoritySnapshot,
        reply: oneshot::Sender<Result<Vec<JobInfo>>>,
    },
    Kill {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<()>>,
    },
    Wait {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<JobInfo>>,
    },
    LogRead {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        stream: StreamKind,
        offset: u64,
        follow: bool,
        reply: oneshot::Sender<Result<LogChunk>>,
    },
    Checkpoint {
        authority: WorkspaceAuthoritySnapshot,
        checkpoint_id: String,
        reply: oneshot::Sender<Result<CheckpointBarrier>>,
    },
    Quiesce {
        authority: WorkspaceAuthoritySnapshot,
        reply: oneshot::Sender<Result<()>>,
    },
    Retire {
        authority: WorkspaceAuthoritySnapshot,
        reply: oneshot::Sender<Result<()>>,
    },
    /// The authority the actor holds now; unfenced, because it is how a caller learns it.
    CurrentAuthority {
        reply: oneshot::Sender<Result<WorkspaceAuthoritySnapshot>>,
    },
    /// Whether no named session is open and no job is running.
    #[cfg(target_os = "macos")]
    Idle {
        reply: oneshot::Sender<Result<bool>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActorLifecycle {
    Running,
    Quiescing,
    Retiring,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Why the actor stopped a job before it terminated on its own. Every variant is a distinct
/// diagnosis: `doctor` cannot tell a stdin pump error from a corrupt artifact append if they
/// share a discriminant, and all four of the failure variants land on `JobState::Failed`.
enum KillReason {
    Requested,
    OutputLimit,
    Retire,
    SpawnFailure,
    StdinFailure,
    ArtifactFailure,
    /// `wait(2)` never reported a status, so the child's fate is unknown.
    WaitFailure,
    /// A script did not parse, so nothing ran.
    ScriptSyntax,
}

struct PendingStdin {
    bytes: Bytes,
    reply: oneshot::Sender<Result<()>>,
}

struct PendingLog {
    stream: StreamKind,
    offset: u64,
    reply: oneshot::Sender<Result<LogChunk>>,
}

struct SessionState {
    identity: u64,
    name: Option<String>,
    cwd: Option<WorkspacePath>,
    env: BTreeMap<String, String>,
    background_jobs: BTreeSet<JobId>,
}

struct JobStateRecord {
    info: JobInfo,
    started_at: Instant,
    process: Option<Box<dyn RunningProcess>>,
    artifact_live: bool,
    stdout: VecDeque<Bytes>,
    stderr: VecDeque<Bytes>,
    stdout_len: u64,
    stderr_len: u64,
    stdout_eof: bool,
    stderr_eof: bool,
    exit: Option<ExitStatus>,
    /// Set when `wait(2)` could not name the child's termination. Keeps the job's terminal
    /// record free of a fabricated exit and hands the failure to everyone awaiting the job.
    wait_failure: Option<CowshedError>,
    output_limit: Option<OutputLimitInfo>,
    kill_reason: Option<KillReason>,
    terminal_committed: bool,
    stdout_copy: Option<OutputPublication>,
    stderr_copy: Option<OutputPublication>,
    pending_stdin: VecDeque<PendingStdin>,
    pending_stdin_bytes: usize,
    close_stdin_when_drained: bool,
    close_waiters: Vec<oneshot::Sender<Result<()>>>,
    waiters: Vec<oneshot::Sender<Result<JobInfo>>>,
    kill_waiters: Vec<oneshot::Sender<Result<()>>>,
    log_waiters: Vec<PendingLog>,
    session_identity: Option<u64>,
}

impl JobStateRecord {
    fn terminal(&self) -> bool {
        self.info.state.is_terminal()
    }

    /// The answer to "how did this job end", for every caller that asked to be told.
    ///
    /// A retained wait failure outranks the record: the state and byte counts are true, but
    /// nothing observed the child terminate, so `Ok` would be a wrong-success channel.
    fn terminal_outcome(&self) -> Result<JobInfo> {
        match &self.wait_failure {
            Some(error) => Err(error.clone()),
            None => Ok(self.info.clone()),
        }
    }

    fn stream(&self, stream: StreamKind) -> (&VecDeque<Bytes>, u64, bool) {
        match stream {
            StreamKind::Stdout => (&self.stdout, self.stdout_len, self.stdout_eof),
            StreamKind::Stderr => (&self.stderr, self.stderr_len, self.stderr_eof),
        }
    }
}

struct SupervisorActor {
    authority: WorkspaceAuthoritySnapshot,
    group_ledger: Option<PathBuf>,
    workspace_root: PathBuf,
    default_cwd: Option<WorkspacePath>,
    /// The sandbox of the authority served, rendered once when it was taken.
    policy: SandboxPolicy,
    /// Names withheld from every child; see [`WorkspaceSupervisorConfig::credential_env_names`].
    credential_env_names: BTreeSet<String>,
    term_grace: Duration,
    next_job_id: JobId,
    next_session_id: u64,
    lifecycle: ActorLifecycle,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<ProcessEvent>,
    event_receiver: mpsc::Receiver<ProcessEvent>,
    spawner: Box<dyn SpawnSink>,
    artifacts: Box<dyn ArtifactSink>,
    commitments: Box<dyn CommitmentSink>,
    jobs: BTreeMap<JobId, JobStateRecord>,
    sessions: BTreeMap<u64, SessionState>,
    named_sessions: BTreeMap<String, u64>,
    quiesce_waiters: Vec<oneshot::Sender<Result<()>>>,
    retire_waiters: Vec<oneshot::Sender<Result<()>>>,
    command_lane_closed: bool,
    /// Main's warm step: the one warm job running and the one run waiting behind it.
    warm: WarmLane,
}

/// The actor's run loop ends only once every job is terminal, so an actor dropped with a job still
/// running was torn down with the runtime that hosts it. No later controller knows that job: it
/// would run on unobserved and uncancellable, so its process tree ends here with its record.
///
/// It ends by the protocol every kill follows — SIGTERM, the term grace, then SIGKILL — so a job
/// that stops its own children on SIGTERM (Nx's task runner stops task trees it keeps in their
/// own sessions, beyond this group) gets to. Nothing runs after a drop, so the grace is waited out
/// here, blocking, and only when a job was still running. A host that wants its jobs' cleanup to
/// be asynchronous cancels them before it drops the runtime. Before each signal an exited leader
/// is reaped, since its own wait will never run and Darwin refuses a group holding only unreaped
/// members with EPERM; a group left with no member has nothing to signal. Any other failure to
/// signal is reported, because the job then outlives its supervisor.
impl Drop for SupervisorActor {
    fn drop(&mut self) {
        let grace = self.term_grace;
        let mut running = self
            .jobs
            .iter_mut()
            .filter(|(_, job)| !job.terminal())
            .filter_map(|(id, job)| job.process.as_mut().map(|process| (*id, process)))
            .collect::<Vec<_>>();
        if running.is_empty() {
            return;
        }
        for (id, process) in &mut running {
            process.reap_if_exited();
            report_unsignalled(*id, process.signal_process_tree(ProcessSignal::Term));
        }
        std::thread::sleep(grace);
        for (id, process) in &mut running {
            process.reap_if_exited();
            report_unsignalled(*id, process.signal_process_tree(ProcessSignal::Kill));
        }
    }
}

fn report_unsignalled(job: JobId, signalled: Result<()>) {
    if let Err(error) = signalled {
        eprintln!(
            "cowshed: could not signal job {} of a supervisor that is going away, so it may keep \
             running: {}",
            job.get(),
            error.message
        );
    }
}

impl SupervisorActor {
    async fn run(mut self) {
        loop {
            if self.command_lane_closed && !self.has_running_jobs() {
                break;
            }
            tokio::select! {
                command = self.commands.recv(), if !self.command_lane_closed => {
                    match command {
                        Some(command) => self.handle_command(command).await,
                        None => self.command_lane_closed = true,
                    }
                }
                event = self.event_receiver.recv() => {
                    match event {
                        Some(event) => self.handle_event(event),
                        None => break,
                    }
                }
            }
            self.finish_ready_jobs().await;
            self.advance_warm_lane().await;
            self.finish_lifecycle_waiters();
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::AdvanceAuthority {
                expected,
                authority,
                sandbox,
                reply,
            } => {
                let result = self.advance_authority(expected, authority, sandbox);
                let _ = reply.send(result);
            }
            Command::OpenSession {
                authority,
                name,
                reply,
            } => {
                let result = self.open_session(&authority, name);
                let _ = reply.send(result);
            }
            Command::SessionSnapshot {
                authority,
                session,
                reply,
            } => {
                let result = self.session_snapshot(&authority, &session);
                let _ = reply.send(result);
            }
            Command::CloseSession {
                authority,
                session,
                reply,
            } => {
                let result = self.close_session(&authority, &session);
                let _ = reply.send(result);
            }
            Command::Exec {
                authority,
                session,
                request,
                background,
                reply,
            } => {
                let result = self
                    .admit_exec(authority, session, request, background, None)
                    .await;
                let _ = reply.send(result);
            }
            Command::Warm {
                authority,
                argv,
                range,
                reply,
            } => {
                let result = self.warm(&authority, WarmRun { argv, range }).await;
                let _ = reply.send(result);
            }
            Command::StdinWrite {
                authority,
                job_id,
                bytes,
                reply,
            } => self.stdin_write(&authority, job_id, bytes, reply),
            Command::StdinClose {
                authority,
                job_id,
                reply,
            } => self.stdin_close(&authority, job_id, reply),
            Command::Info {
                authority,
                job_id,
                reply,
            } => {
                let result = self
                    .validate_authority(&authority)
                    .and_then(|()| self.job(job_id).map(|job| job.info.clone()));
                let _ = reply.send(result);
            }
            Command::Sealed {
                authority,
                job_id,
                reply,
            } => {
                let result = self.validate_authority(&authority).and_then(|()| {
                    self.artifacts
                        .sealed(job_id)
                        .ok_or_else(|| unsealed_job(job_id))
                });
                let _ = reply.send(result);
            }
            Command::List { authority, reply } => {
                let result = self.validate_authority(&authority).map(|()| {
                    self.jobs
                        .values()
                        .map(|job| job.info.clone())
                        .collect::<Vec<_>>()
                });
                let _ = reply.send(result);
            }
            Command::Kill {
                authority,
                job_id,
                reply,
            } => self.kill(&authority, job_id, reply),
            Command::Wait {
                authority,
                job_id,
                reply,
            } => self.wait(&authority, job_id, reply),
            Command::LogRead {
                authority,
                job_id,
                stream,
                offset,
                follow,
                reply,
            } => self.log_read(&authority, job_id, stream, offset, follow, reply),
            Command::Checkpoint {
                authority,
                checkpoint_id,
                reply,
            } => {
                let result = self.checkpoint(&authority, checkpoint_id).await;
                let _ = reply.send(result);
            }
            Command::Quiesce { authority, reply } => {
                if let Err(error) = self.validate_authority(&authority) {
                    let _ = reply.send(Err(error));
                } else if self.lifecycle == ActorLifecycle::Retired {
                    let _ = reply.send(Ok(()));
                } else {
                    if self.lifecycle == ActorLifecycle::Running {
                        self.lifecycle = ActorLifecycle::Quiescing;
                    }
                    self.quiesce_waiters.push(reply);
                }
            }
            Command::Retire { authority, reply } => {
                if let Err(error) = self.validate_authority(&authority) {
                    let _ = reply.send(Err(error));
                } else if self.lifecycle == ActorLifecycle::Retired {
                    let _ = reply.send(Ok(()));
                } else {
                    self.lifecycle = ActorLifecycle::Retiring;
                    self.retire_waiters.push(reply);
                    let running = self
                        .jobs
                        .iter()
                        .filter_map(|(id, job)| (!job.terminal()).then_some(*id))
                        .collect::<Vec<_>>();
                    for job_id in running {
                        let _ = self.begin_kill(job_id, KillReason::Retire);
                    }
                }
            }
            Command::CurrentAuthority { reply } => {
                let _ = reply.send(Ok(self.authority.clone()));
            }
            #[cfg(target_os = "macos")]
            Command::Idle { reply } => {
                let idle = self.named_sessions.is_empty()
                    && self.jobs.values().all(JobStateRecord::terminal);
                let _ = reply.send(Ok(idle));
            }
        }
    }

    /// Replace the group ledger with the groups of the jobs still running.
    fn record_groups(&self) {
        let Some(path) = &self.group_ledger else {
            return;
        };
        let groups: Vec<(u64, u32)> = self
            .jobs
            .iter()
            .filter(|(_, job)| !job.terminal_committed)
            .filter_map(|(job_id, job)| Some((job_id.get(), job.info.pid?)))
            .collect();
        if let Err(error) = super::job_groups::record(path, &groups) {
            // The jobs run either way; what is lost is only the means to end them should
            // this supervisor die, which is worth saying where the daemon log shows it.
            eprintln!(
                "cowshed: cannot record the job process groups at {}: {error}",
                path.display()
            );
        }
    }

    fn validate_authority(&self, authority: &WorkspaceAuthoritySnapshot) -> Result<()> {
        if authority == &self.authority {
            Ok(())
        } else {
            Err(CowshedError::conflict(
                "workspace supervisor authority is stale",
                "reattach the workspace and retry with its current incarnation and revisions",
            ))
        }
    }

    fn advance_authority(
        &mut self,
        expected: WorkspaceAuthoritySnapshot,
        authority: WorkspaceAuthoritySnapshot,
        sandbox: SandboxConfig,
    ) -> Result<()> {
        self.validate_authority(&expected)?;
        if authority.repo_id != self.authority.repo_id
            || authority.workspace != self.authority.workspace
            || authority.workspace_incarnation != self.authority.workspace_incarnation
            || authority.grant_revision < self.authority.grant_revision
            || authority.lifecycle_revision < self.authority.lifecycle_revision
        {
            return Err(CowshedError::conflict(
                "authority advancement is not a monotonic revision of this workspace",
                "reattach the authoritative workspace incarnation",
            ));
        }
        if sandbox.workspace_mount != self.workspace_root {
            return Err(CowshedError::conflict(
                "advanced sandbox mount does not match the workspace",
                "reattach the authoritative workspace mount",
            ));
        }
        // Rendered here, once for the new authority; every job admitted under it shares it.
        let policy = SandboxPolicy::render(sandbox)?;
        self.authority = authority;
        self.policy = policy;
        Ok(())
    }

    fn open_session(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        name: Option<String>,
    ) -> Result<SessionToken> {
        self.validate_authority(authority)?;
        if self.lifecycle != ActorLifecycle::Running {
            return Err(retiring_error());
        }
        if let Some(name) = name.as_deref() {
            validate_session_name(name)?;
            if let Some(identity) = self.named_sessions.get(name).copied() {
                return Ok(SessionToken {
                    authority: self.authority.clone(),
                    identity,
                    name: Some(name.to_owned()),
                });
            }
        }
        let identity = self.next_session_id;
        self.next_session_id = self
            .next_session_id
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("session identity allocation exhausted"))?;
        let state = SessionState {
            identity,
            name: name.clone(),
            cwd: self.default_cwd.clone(),
            env: BTreeMap::new(),
            background_jobs: BTreeSet::new(),
        };
        self.sessions.insert(identity, state);
        if let Some(name) = &name {
            self.named_sessions.insert(name.clone(), identity);
        }
        Ok(SessionToken {
            authority: self.authority.clone(),
            identity,
            name,
        })
    }

    fn session_snapshot(
        &self,
        authority: &WorkspaceAuthoritySnapshot,
        token: &SessionToken,
    ) -> Result<SessionSnapshot> {
        self.validate_session(authority, token)?;
        let state = self
            .sessions
            .get(&token.identity)
            .expect("validated session exists");
        Ok(SessionSnapshot {
            identity: state.identity,
            name: state.name.clone(),
            cwd: state.cwd.clone(),
            env: state.env.clone(),
            background_jobs: state.background_jobs.clone(),
        })
    }

    fn close_session(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        token: &SessionToken,
    ) -> Result<()> {
        self.validate_session(authority, token)?;
        let state = self
            .sessions
            .remove(&token.identity)
            .expect("validated session exists");
        if let Some(name) = state.name {
            self.named_sessions.remove(&name);
        }
        Ok(())
    }

    fn validate_session(
        &self,
        authority: &WorkspaceAuthoritySnapshot,
        token: &SessionToken,
    ) -> Result<()> {
        self.validate_authority(authority)?;
        if token.authority != self.authority {
            return Err(CowshedError::conflict(
                "session authority is stale",
                "open a new session on the current workspace authority",
            ));
        }
        let Some(state) = self.sessions.get(&token.identity) else {
            return Err(CowshedError::conflict(
                "session identity is closed or stale",
                "open a new session",
            ));
        };
        if state.name != token.name {
            return Err(CowshedError::conflict(
                "session identity does not match its name",
                "open a new session",
            ));
        }
        Ok(())
    }

    /// Admit and spawn one job. `warm` marks it as a land target's warm step for that landed range.
    async fn admit_exec(
        &mut self,
        authority: WorkspaceAuthoritySnapshot,
        session: Option<SessionToken>,
        request: ExecRequest,
        background: bool,
        warm: Option<WarmRange>,
    ) -> Result<JobId> {
        self.validate_authority(&authority)
            .and_then(|()| {
                if self.lifecycle == ActorLifecycle::Running {
                    Ok(())
                } else {
                    Err(retiring_error())
                }
            })
            .and_then(|()| {
                if let Some(token) = &session {
                    self.validate_session(&authority, token)
                } else {
                    Ok(())
                }
            })?;

        let ExecRequest {
            command,
            cwd,
            mode,
            env,
            trace,
            stdin,
            stdout_copy,
            stderr_copy,
        } = request;
        command.validate().map_err(|error| {
            CowshedError::usage(error.to_string(), "provide a valid bounded command")
        })?;
        // A script is rendered before anything is admitted, so a value that stands where no
        // substitution can mean it is refused like any other malformed request.
        let spawn_command = match &command {
            ExecCommand::Argv(argv) => SpawnCommand::Argv(
                argv.iter()
                    .cloned()
                    .map(CommandArg::into_os_string)
                    .collect(),
            ),
            ExecCommand::Script(script) => match crate::script::render(script) {
                Ok(rendered) => SpawnCommand::Script(rendered),
                Err(error) => {
                    return Err(CowshedError::usage(
                        error.to_string(),
                        "put script values only where a word or a quoted string stands",
                    ));
                }
            },
        };
        let (cwd, mut merged_env, session_identity) = match session.as_ref() {
            Some(token) => {
                let state = self
                    .sessions
                    .get_mut(&token.identity)
                    .expect("validated session exists");
                if let Some(cwd) = cwd {
                    state.cwd = Some(cwd);
                }
                state.env.extend(env);
                // A session is long-lived, so the withheld names must not accumulate in it
                // either: what the session remembers is what a later exec would forward.
                state
                    .env
                    .retain(|name, _| !self.credential_env_names.contains(name));
                (state.cwd.clone(), state.env.clone(), Some(state.identity))
            }
            None => (
                cwd.or_else(|| self.default_cwd.clone()),
                env.into_iter().collect(),
                None,
            ),
        };
        // The gateway holds the credential for these origins. An ambient copy of the same token
        // in the caller's environment would hand the workspace the very bytes the host kept out
        // of it, so it is dropped here — at the one place every job's environment is settled.
        merged_env.retain(|name, _| !self.credential_env_names.contains(name));
        let job_id = self.next_job_id;
        let expected_next = job_id
            .get()
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("job id allocation exhausted"))
            .and_then(|value| {
                JobId::new(value).map_err(|error| CowshedError::internal(error.to_string()))
            })?;
        {
            let _span = crate::timing::span("admit", "record");
            self.artifacts.admit(
                job_id,
                self.authority.grant_revision,
                &command,
                warm.as_ref(),
            )?;
        }
        self.next_job_id = expected_next;
        let admission = crate::timing::spanned(
            "admit",
            "commitment",
            self.commitments.record(CommitmentDraft::Admission {
                repo_id: self.authority.repo_id.clone(),
                workspace_incarnation: self.authority.workspace_incarnation.clone(),
                job_id,
                grant_revision: self.authority.grant_revision,
            }),
        )
        .await;
        if let Err(error) = admission {
            let _ = self
                .artifacts
                .seal(job_id, JobState::Failed.into(), stdout_copy, stderr_copy);
            return Err(error);
        }
        if background && let Err(error) = self.artifacts.prepare_background(job_id) {
            let _ = self
                .artifacts
                .seal(job_id, JobState::Failed.into(), stdout_copy, stderr_copy);
            return Err(error);
        }

        let stdin_info = stdin_info(&stdin);
        // A clock fault is not an invariant, and `started` orders job records and commitments.
        // Stamping the job at the epoch would make every consumer that sorts by it lie, so
        // admission fails here exactly as it does for a seatbelt-profile failure below.
        let started = match utc_now() {
            Ok(started) => started,
            Err(error) => {
                let _ =
                    self.artifacts
                        .seal(job_id, JobState::Failed.into(), stdout_copy, stderr_copy);
                return Err(error);
            }
        };
        let trace = trace.unwrap_or_else(new_trace_context);
        let info = JobInfo {
            repo_id: self.authority.repo_id.clone(),
            workspace_incarnation: self.authority.workspace_incarnation.clone(),
            job_id,
            state: JobState::Running,
            pid: None,
            grant_revision: self.authority.grant_revision,
            command,
            cwd: cwd.clone(),
            started,
            duration_ms: None,
            exit: None,
            stdout: empty_stream(),
            stderr: empty_stream(),
            trace,
            output_limit: None,
            stdin: stdin_info,
            failure: None,
            warm,
        };
        let spawn_span = crate::timing::span("admit", "spawn");
        let spawn = self
            .spawner
            .spawn(
                ProcessSpawnRequest {
                    authority: self.authority.clone(),
                    job_id,
                    command: spawn_command,
                    cwd: cwd
                        .as_ref()
                        .map(WorkspacePath::as_path)
                        .map(Path::to_path_buf)
                        .unwrap_or_default(),
                    env: merged_env,
                    devenv_dir: None,
                    // Rendered when this authority was taken. The request may narrow the
                    // ceiling, never widen it, and narrowing one job alters no other job.
                    policy: self.policy.clone(),
                    mode,
                },
                self.events.clone(),
            )
            .await;
        drop(spawn_span);
        let mut job = JobStateRecord {
            info,
            started_at: Instant::now(),
            process: None,
            artifact_live: true,
            stdout: VecDeque::new(),
            stderr: VecDeque::new(),
            stdout_len: 0,
            stderr_len: 0,
            stdout_eof: false,
            stderr_eof: false,
            exit: None,
            wait_failure: None,
            output_limit: None,
            kill_reason: None,
            terminal_committed: false,
            stdout_copy,
            stderr_copy,
            pending_stdin: VecDeque::new(),
            pending_stdin_bytes: 0,
            close_stdin_when_drained: false,
            close_waiters: Vec::new(),
            waiters: Vec::new(),
            kill_waiters: Vec::new(),
            log_waiters: Vec::new(),
            session_identity,
        };
        match spawn {
            Ok(process) => {
                job.info.pid = process.pid();
                job.process = Some(process);
                let started = job.info.pid.is_some();
                if background
                    && let Some(identity) = session_identity
                    && let Some(session) = self.sessions.get_mut(&identity)
                {
                    session.background_jobs.insert(job_id);
                }
                self.jobs.insert(job_id, job);
                if started {
                    self.record_groups();
                }
                launch_stdin_pump(
                    job_id,
                    stdin,
                    self.workspace_root.clone(),
                    self.events.clone(),
                );
                Ok(job_id)
            }
            Err(error) => {
                job.stdout_eof = true;
                job.stderr_eof = true;
                job.exit = Some(ExitStatus::Exited {
                    code: error.exec_wrapper_exit_code().into(),
                });
                job.kill_reason = Some(KillReason::SpawnFailure);
                self.jobs.insert(job_id, job);
                self.finalize_job(job_id, Some(JobState::Failed)).await;
                Err(error)
            }
        }
    }

    /// One land's warm run: start it when no warm job runs, else fold it into the run waiting
    /// behind the one that does.
    async fn warm(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        run: WarmRun,
    ) -> Result<WarmAdmission> {
        self.validate_authority(authority)?;
        if self.lifecycle != ActorLifecycle::Running {
            return Err(retiring_error());
        }
        match self.warm.admit(run) {
            WarmTurn::Wait(admission) => Ok(admission),
            WarmTurn::Start(run) => {
                let job_id = self.start_warm(&run).await?;
                Ok(WarmAdmission::Started {
                    job_id,
                    range: run.range,
                })
            }
        }
    }

    async fn start_warm(&mut self, run: &WarmRun) -> Result<JobId> {
        let authority = self.authority.clone();
        let job_id = self
            .admit_exec(
                authority,
                None,
                run.request(),
                true,
                Some(run.range.clone()),
            )
            .await?;
        self.warm.started(job_id);
        Ok(job_id)
    }

    /// Once the running warm job has ended, start the run that waited behind it. Nobody waits for
    /// that answer — the lands it covers returned long ago — so a run that cannot start is said
    /// where the supervisor's own failures go.
    async fn advance_warm_lane(&mut self) {
        let Some(running) = self.warm.running() else {
            return;
        };
        if self.jobs.get(&running).is_some_and(|job| !job.terminal()) {
            return;
        }
        let Some(run) = self.warm.ended() else {
            return;
        };
        if self.lifecycle != ActorLifecycle::Running {
            eprintln!(
                "cowshed: main's supervisor is retiring, so the warm run for {} that waited \
                 behind job {} does not start",
                run.range,
                running.get()
            );
            return;
        }
        if let Err(error) = self.start_warm(&run).await {
            eprintln!(
                "cowshed: main's warm run for {} did not start: {}",
                run.range, error.message
            );
        }
    }

    fn stdin_write(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        bytes: Bytes,
        reply: oneshot::Sender<Result<()>>,
    ) {
        if let Err(error) = self.validate_authority(authority) {
            let _ = reply.send(Err(error));
            return;
        }
        let Ok(job) = self.job_mut(job_id) else {
            let _ = reply.send(Err(not_found_job(job_id)));
            return;
        };
        if job.terminal() || job.info.stdin.complete {
            let _ = reply.send(Err(CowshedError::conflict(
                "job stdin is closed",
                "inspect the job status",
            )));
            return;
        }
        let Some(process) = job.process.as_mut() else {
            let _ = reply.send(Err(CowshedError::conflict(
                "job process is unavailable",
                "inspect the terminal job status",
            )));
            return;
        };
        match process.try_write_stdin(bytes.clone()) {
            Ok(true) => {
                job.info.stdin.bytes = job.info.stdin.bytes.saturating_add(byte_count(bytes.len()));
                let _ = reply.send(Ok(()));
            }
            Ok(false) => {
                if job.pending_stdin_bytes.saturating_add(bytes.len()) > MAX_PENDING_STDIN_BYTES {
                    let _ = reply.send(Err(CowshedError::conflict(
                        "job stdin backpressure budget is full",
                        "wait for the pending stdin write to drain",
                    )));
                } else {
                    job.pending_stdin_bytes += bytes.len();
                    job.pending_stdin.push_back(PendingStdin { bytes, reply });
                }
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn stdin_close(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<()>>,
    ) {
        if let Err(error) = self.validate_authority(authority) {
            let _ = reply.send(Err(error));
            return;
        }
        let Ok(job) = self.job_mut(job_id) else {
            let _ = reply.send(Err(not_found_job(job_id)));
            return;
        };
        if job.info.stdin.complete {
            let _ = reply.send(Ok(()));
            return;
        }
        if !job.pending_stdin.is_empty() {
            job.close_stdin_when_drained = true;
            job.close_waiters.push(reply);
            return;
        }
        let result = job
            .process
            .as_mut()
            .ok_or_else(|| {
                CowshedError::conflict("job process is unavailable", "inspect job status")
            })
            .and_then(|process| process.close_stdin());
        if result.is_ok() {
            job.info.stdin.complete = true;
        }
        let _ = reply.send(result);
    }

    fn wait(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<JobInfo>>,
    ) {
        if let Err(error) = self.validate_authority(authority) {
            let _ = reply.send(Err(error));
            return;
        }
        let Ok(job) = self.job_mut(job_id) else {
            let _ = reply.send(Err(not_found_job(job_id)));
            return;
        };
        if job.terminal() {
            let _ = reply.send(job.terminal_outcome());
        } else {
            job.waiters.push(reply);
        }
    }
    fn kill(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<()>>,
    ) {
        if let Err(error) = self.validate_authority(authority) {
            let _ = reply.send(Err(error));
            return;
        }
        if let Err(error) = self.begin_kill(job_id, KillReason::Requested) {
            let _ = reply.send(Err(error));
            return;
        }
        let job = self
            .jobs
            .get_mut(&job_id)
            .expect("begin_kill validated the job");
        if job.terminal_committed {
            let _ = reply.send(job.terminal_outcome().map(|_| ()));
        } else {
            job.kill_waiters.push(reply);
        }
    }

    fn log_read(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        stream: StreamKind,
        offset: u64,
        follow: bool,
        reply: oneshot::Sender<Result<LogChunk>>,
    ) {
        if let Err(error) = self.validate_authority(authority) {
            let _ = reply.send(Err(error));
            return;
        }
        let Ok(job) = self.job_mut(job_id) else {
            // A job an earlier supervisor of this incarnation ran and sealed: its sealed artifact
            // answers exactly as this supervisor's own terminal jobs do.
            let Some(sealed) = self.artifacts.sealed(job_id) else {
                let _ = reply.send(Err(not_found_job(job_id)));
                return;
            };
            let stream = match stream {
                StreamKind::Stdout => sealed.stdout,
                StreamKind::Stderr => sealed.stderr,
            };
            let workspace_root = self.workspace_root.clone();
            tokio::task::spawn_blocking(move || {
                let _ = reply.send(read_sealed_chunk(&workspace_root, &stream, offset));
            });
            return;
        };
        if job.terminal_committed {
            // The sealed artifact is the authoritative copy of both streams, so the actor keeps
            // none. Off the actor thread: this may open and read a protected file, and every
            // other job's output pump waits behind this loop.
            let sealed = match stream {
                StreamKind::Stdout => job.info.stdout.clone(),
                StreamKind::Stderr => job.info.stderr.clone(),
            };
            let workspace_root = self.workspace_root.clone();
            tokio::task::spawn_blocking(move || {
                let _ = reply.send(read_sealed_chunk(&workspace_root, &sealed, offset));
            });
            return;
        }
        match make_log_chunk(job, stream, offset) {
            Ok(Some(chunk)) => {
                let _ = reply.send(Ok(chunk));
            }
            Ok(None) if follow && !job.terminal() => {
                job.log_waiters.push(PendingLog {
                    stream,
                    offset,
                    reply,
                });
            }
            Ok(None) => {
                let (_, len, eof) = job.stream(stream);
                let _ = reply.send(Ok(LogChunk {
                    bytes: Bytes::new(),
                    next_offset: len,
                    eof: eof || job.terminal(),
                }));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    async fn checkpoint(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        checkpoint_id: String,
    ) -> Result<CheckpointBarrier> {
        self.validate_authority(authority)?;
        validate_checkpoint_id(&checkpoint_id)?;
        let mut barrier = self.artifacts.checkpoint()?;
        barrier.checkpoint_id = checkpoint_id.clone();
        self.commitments
            .record(CommitmentDraft::Checkpoint {
                repo_id: self.authority.repo_id.clone(),
                origin_incarnation: self.authority.workspace_incarnation.clone(),
                checkpoint_id,
                barrier_id: barrier.barrier_id,
                manifest_batch_sha256: barrier.manifest_batch_sha256,
            })
            .await?;
        Ok(barrier)
    }

    fn handle_event(&mut self, event: ProcessEvent) {
        match event {
            ProcessEvent::Output {
                job_id,
                stream,
                bytes,
            } => self.process_output(job_id, stream, bytes),
            ProcessEvent::OutputEof { job_id, stream } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    match stream {
                        StreamKind::Stdout => job.stdout_eof = true,
                        StreamKind::Stderr => job.stderr_eof = true,
                    }
                    flush_log_waiters(job);
                }
            }
            ProcessEvent::Exited { job_id, exit } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.exit = Some(exit);
                    release_exited_process(job);
                }
            }
            ProcessEvent::WaitFailed { job_id, error } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                if job.terminal_committed {
                    return;
                }
                // `exit` stays `None`: there is no truthful status to publish. The job seals as
                // `Failed` and every waiter gets the integrity error, so a still-running child
                // can never be read as a completed one.
                job.wait_failure = Some(error);
                job.kill_reason = Some(KillReason::WaitFailure);
                // The child was never reaped, so it may still be running. Kill the group before
                // retiring the handle: an unobservable process must not outlive its record. The
                // spawn sink's own wait task kills too, because it is the one component that is
                // still alive if this actor has already stopped.
                if let Some(process) = job.process.as_mut() {
                    let _ = process.signal_process_tree(ProcessSignal::Kill);
                }
                release_exited_process(job);
            }
            ProcessEvent::StdinReady { job_id } => self.flush_stdin(job_id),
            ProcessEvent::StdinPumpWrite {
                job_id,
                bytes,
                reply,
            } => {
                let authority = self.authority.clone();
                self.stdin_write(&authority, job_id, bytes, reply);
            }
            ProcessEvent::StdinPumpClose { job_id } => {
                let (reply, _receive) = oneshot::channel();
                let authority = self.authority.clone();
                self.stdin_close(&authority, job_id, reply);
            }
            ProcessEvent::StdinPumpFailed { job_id, error: _ } => {
                let _ = self.begin_kill(job_id, KillReason::StdinFailure);
            }
            ProcessEvent::Started { job_id, pid } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.info.pid = Some(pid);
                    self.record_groups();
                }
            }
            ProcessEvent::LaunchFailed { job_id, error } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                if job.terminal_committed {
                    return;
                }
                // The same terminal shape as a spawn that failed synchronously; the job's
                // stderr already carries the reason.
                job.exit = Some(ExitStatus::Exited {
                    code: error.exec_wrapper_exit_code().into(),
                });
                job.kill_reason = Some(KillReason::SpawnFailure);
                release_exited_process(job);
            }
            ProcessEvent::ScriptSyntax { job_id } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                if job.terminal_committed {
                    return;
                }
                // Bash's own status for a script that does not parse.
                job.exit = Some(ExitStatus::Exited { code: 2 });
                job.kill_reason = Some(KillReason::ScriptSyntax);
                release_exited_process(job);
            }
            ProcessEvent::Escalate { job_id } => {
                if let Some(job) = self.jobs.get_mut(&job_id)
                    && !job.terminal()
                    && let Some(process) = job.process.as_mut()
                {
                    let _ = process.signal_process_tree(ProcessSignal::Kill);
                }
            }
        }
    }

    fn process_output(&mut self, job_id: JobId, stream: StreamKind, bytes: Bytes) {
        let Some(job) = self.jobs.get(&job_id) else {
            return;
        };
        if job.terminal_committed || !job.artifact_live {
            return;
        }
        match self.artifacts.write(job_id, stream, &bytes) {
            Ok(admission) => {
                let job = self
                    .jobs
                    .get_mut(&job_id)
                    .expect("artifact write job remains actor-owned");
                if admission.accepted_bytes != 0 {
                    let accepted = bytes.slice(..admission.accepted_bytes);
                    match stream {
                        StreamKind::Stdout => {
                            job.stdout_len += byte_count(accepted.len());
                            job.stdout.push_back(accepted);
                        }
                        StreamKind::Stderr => {
                            job.stderr_len += byte_count(accepted.len());
                            job.stderr.push_back(accepted);
                        }
                    }
                }
                let crossed = admission.output_limit;
                if let Some(limit) = crossed.clone() {
                    job.output_limit = Some(limit);
                }
                flush_log_waiters(job);
                if crossed.is_some() {
                    let _ = self.begin_kill(job_id, KillReason::OutputLimit);
                }
            }
            Err(_) => {
                let _ = self.begin_kill(job_id, KillReason::ArtifactFailure);
            }
        }
    }

    fn flush_stdin(&mut self, job_id: JobId) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        while let Some(pending) = job.pending_stdin.pop_front() {
            let Some(process) = job.process.as_mut() else {
                let _ = pending.reply.send(Err(CowshedError::conflict(
                    "job process is unavailable",
                    "inspect the terminal job status",
                )));
                continue;
            };
            match process.try_write_stdin(pending.bytes.clone()) {
                Ok(true) => {
                    job.pending_stdin_bytes -= pending.bytes.len();
                    job.info.stdin.bytes = job
                        .info
                        .stdin
                        .bytes
                        .saturating_add(byte_count(pending.bytes.len()));
                    let _ = pending.reply.send(Ok(()));
                }
                Ok(false) => {
                    job.pending_stdin.push_front(pending);
                    break;
                }
                Err(error) => {
                    job.pending_stdin_bytes -= pending.bytes.len();
                    let _ = pending.reply.send(Err(error));
                }
            }
        }
        if job.pending_stdin.is_empty() && job.close_stdin_when_drained {
            let result = job
                .process
                .as_mut()
                .map_or(Ok(()), |process| process.close_stdin());
            if result.is_ok() {
                job.info.stdin.complete = true;
                job.close_stdin_when_drained = false;
            }
            for waiter in job.close_waiters.drain(..) {
                let _ = waiter.send(result.clone());
            }
        }
    }

    fn begin_kill(&mut self, job_id: JobId, reason: KillReason) -> Result<()> {
        let grace = self.term_grace;
        let events = self.events.clone();
        let job = self.job_mut(job_id)?;
        if job.terminal() {
            return Ok(());
        }
        let initiate = job.kill_reason.is_none();
        if initiate || reason == KillReason::OutputLimit {
            job.kill_reason = Some(reason);
        }
        if !initiate {
            return Ok(());
        }
        if let Some(process) = job.process.as_mut() {
            process.signal_process_tree(ProcessSignal::Term)?;
        }
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let _ = events.send(ProcessEvent::Escalate { job_id }).await;
        });
        Ok(())
    }

    async fn finish_ready_jobs(&mut self) {
        let ready = self
            .jobs
            .iter()
            .filter_map(|(id, job)| {
                // An unreaped child may hold its pipes open forever, so a wait failure does not
                // wait for EOF. Output already accepted is sealed; anything later is dropped by
                // the `terminal_committed` guard in `process_output`.
                (!job.terminal_committed
                    && (job.wait_failure.is_some()
                        || (job.exit.is_some() && job.stdout_eof && job.stderr_eof)))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for job_id in ready {
            self.finalize_job(job_id, None).await;
        }
    }

    async fn finalize_job(&mut self, job_id: JobId, forced_state: Option<JobState>) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if job.terminal_committed {
            return;
        }
        let state = forced_state.unwrap_or_else(|| match job.kill_reason {
            Some(KillReason::OutputLimit) => JobState::OutputLimit,
            Some(KillReason::Requested | KillReason::Retire) => JobState::Killed,
            Some(
                KillReason::SpawnFailure
                | KillReason::StdinFailure
                | KillReason::ArtifactFailure
                | KillReason::WaitFailure
                | KillReason::ScriptSyntax,
            ) => JobState::Failed,
            None => match job.exit.as_ref().expect("ready terminal job has exit") {
                ExitStatus::Exited { .. } => JobState::Exited,
                ExitStatus::Signaled { .. } => JobState::Signaled,
            },
        });
        if !job.artifact_live {
            return;
        }
        job.artifact_live = false;
        let duration_ms = job
            .started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let ending = JobEnding {
            state,
            exit: job.exit.clone(),
            duration_ms: Some(duration_ms),
        };
        let seal_span = crate::timing::span("seal", "record");
        let sealed = self.artifacts.seal(
            job_id,
            ending,
            job.stdout_copy.take(),
            job.stderr_copy.take(),
        );
        drop(seal_span);
        let seal = match sealed {
            Ok(seal) => seal,
            Err(error) => {
                for waiter in job.waiters.drain(..) {
                    let _ = waiter.send(Err(error.clone()));
                }
                for waiter in job.kill_waiters.drain(..) {
                    let _ = waiter.send(Err(error.clone()));
                }
                return;
            }
        };
        let commitment = crate::timing::spanned(
            "seal",
            "commitment",
            self.commitments.record(CommitmentDraft::Terminal {
                repo_id: self.authority.repo_id.clone(),
                workspace_incarnation: self.authority.workspace_incarnation.clone(),
                job_id,
                state,
                grant_revision: job.info.grant_revision,
                stdout_bytes: seal.stdout.bytes,
                stdout_sha256: seal.stdout.sha256,
                stderr_bytes: seal.stderr.bytes,
                stderr_sha256: seal.stderr.sha256,
                batch_sha256: seal.terminal_batch_sha256,
                output_limit: seal.output_limit.clone(),
            }),
        )
        .await;
        if let Err(error) = commitment {
            for waiter in job.waiters.drain(..) {
                let _ = waiter.send(Err(error.clone()));
            }
            for waiter in job.kill_waiters.drain(..) {
                let _ = waiter.send(Err(error.clone()));
            }
            return;
        }
        job.terminal_committed = true;
        job.info.state = state;
        let ledger_changed = job.info.pid.is_some();
        job.info.duration_ms = Some(duration_ms);
        job.info.exit = job.exit.clone();
        job.info.stdout = seal.stdout;
        job.info.stderr = seal.stderr;
        job.info.output_limit = seal.output_limit;
        job.info.failure =
            (job.kill_reason == Some(KillReason::ScriptSyntax)).then_some(JobFailure::ScriptSyntax);
        job.info.stdin.complete = true;
        if let Some(identity) = job.session_identity
            && let Some(session) = self.sessions.get_mut(&identity)
        {
            session.background_jobs.remove(&job_id);
        }
        let outcome = job.terminal_outcome();
        for waiter in job.waiters.drain(..) {
            let _ = waiter.send(outcome.clone());
        }
        for waiter in job.kill_waiters.drain(..) {
            let _ = waiter.send(outcome.clone().map(|_| ()));
        }
        flush_log_waiters(job);
        // Release the actor's copy of the output. The store already holds every byte, under a
        // committed digest, and its per-job quota is a gigabyte -- so retaining this second copy
        // for the supervisor's lifetime is growth with no closed form, and a workspace that
        // execs often exhausts memory while its disk quota still holds. Ordered after the
        // waiters so a follower that is mid-stream is served from the live deque first.
        job.stdout = VecDeque::new();
        job.stderr = VecDeque::new();
        if ledger_changed {
            self.record_groups();
        }
    }

    fn finish_lifecycle_waiters(&mut self) {
        if self.has_running_jobs() {
            return;
        }
        for waiter in self.quiesce_waiters.drain(..) {
            let _ = waiter.send(Ok(()));
        }
        if self.lifecycle == ActorLifecycle::Retiring {
            self.lifecycle = ActorLifecycle::Retired;
        }
        if self.lifecycle == ActorLifecycle::Retired {
            for waiter in self.retire_waiters.drain(..) {
                let _ = waiter.send(Ok(()));
            }
        }
    }

    fn has_running_jobs(&self) -> bool {
        self.jobs.values().any(|job| !job.terminal())
    }

    fn job(&self, job_id: JobId) -> Result<&JobStateRecord> {
        self.jobs.get(&job_id).ok_or_else(|| not_found_job(job_id))
    }

    fn job_mut(&mut self, job_id: JobId) -> Result<&mut JobStateRecord> {
        self.jobs
            .get_mut(&job_id)
            .ok_or_else(|| not_found_job(job_id))
    }
}

fn launch_stdin_pump(
    job_id: JobId,
    stdin: StdinSource,
    workspace_root: PathBuf,
    events: mpsc::Sender<ProcessEvent>,
) {
    tokio::spawn(async move {
        let result = match stdin {
            StdinSource::Empty => Ok(()),
            StdinSource::Inline(bytes) => pump_one(job_id, bytes, &events).await,
            StdinSource::Stream(reader) => pump_reader(job_id, reader, &events).await,
            StdinSource::WorkspaceFile(path) => {
                match tokio::fs::File::open(workspace_root.join(path.as_path())).await {
                    Ok(reader) => pump_reader(job_id, Box::pin(reader), &events).await,
                    Err(error) => Err(CowshedError::environment_missing(
                        format!("workspace stdin file could not be opened: {error}"),
                        "verify the workspace-relative stdin path",
                    )),
                }
            }
        };
        match result {
            Ok(()) => {
                let _ = events.send(ProcessEvent::StdinPumpClose { job_id }).await;
            }
            Err(error) => {
                let _ = events
                    .send(ProcessEvent::StdinPumpFailed { job_id, error })
                    .await;
            }
        }
    });
}

async fn pump_one(job_id: JobId, bytes: Bytes, events: &mpsc::Sender<ProcessEvent>) -> Result<()> {
    let (reply, receive) = oneshot::channel();
    events
        .send(ProcessEvent::StdinPumpWrite {
            job_id,
            bytes,
            reply,
        })
        .await
        .map_err(|_| CowshedError::environment_missing("stdin pump stopped", "reattach the job"))?;
    receive
        .await
        .map_err(|_| CowshedError::environment_missing("stdin pump stopped", "reattach the job"))?
}

async fn pump_reader(
    job_id: JobId,
    mut reader: std::pin::Pin<Box<dyn AsyncRead + Send>>,
    events: &mpsc::Sender<ProcessEvent>,
) -> Result<()> {
    let mut buffer = vec![0_u8; PROCESS_IO_CHUNK];
    loop {
        let count = reader.read(&mut buffer).await.map_err(|error| {
            CowshedError::environment_missing(
                format!("stdin stream failed: {error}"),
                "retry with a readable stdin source",
            )
        })?;
        if count == 0 {
            return Ok(());
        }
        pump_one(job_id, Bytes::copy_from_slice(&buffer[..count]), events).await?;
    }
}

fn stdin_info(stdin: &StdinSource) -> StdinInfo {
    match stdin {
        StdinSource::Empty => StdinInfo {
            kind: StdinKind::Empty,
            bytes: 0,
            workspace_path: None,
            complete: false,
        },
        StdinSource::Inline(bytes) => StdinInfo {
            kind: StdinKind::Inline,
            bytes: 0,
            workspace_path: None,
            complete: bytes.is_empty(),
        },
        StdinSource::Stream(_) => StdinInfo {
            kind: StdinKind::Stream,
            bytes: 0,
            workspace_path: None,
            complete: false,
        },
        StdinSource::WorkspaceFile(path) => StdinInfo {
            kind: StdinKind::WorkspaceFile,
            bytes: 0,
            workspace_path: Some(path.clone()),
            complete: false,
        },
    }
}

fn empty_stream() -> StreamInfo {
    let data = BinaryData::new(Vec::new()).expect("empty inline data");
    StreamInfo {
        storage: OutputStorage::Captured {
            artifact: ProtectedOutput::Inline { data },
        },
        bytes: 0,
        sha256: Sha256Digest::compute(&[]),
        summary: OutputSummary {
            version: 1,
            text: String::new(),
            truncated: false,
        },
    }
}

fn make_log_chunk(
    job: &JobStateRecord,
    stream: StreamKind,
    offset: u64,
) -> Result<Option<LogChunk>> {
    let (chunks, len, eof) = job.stream(stream);
    if offset > len {
        return Err(CowshedError::conflict(
            "log offset is beyond the captured stream",
            "restart the read at the returned stream length",
        ));
    }
    if offset == len {
        return Ok(None);
    }
    let mut skip = offset;
    let available = usize::try_from(len - offset).unwrap_or(MAX_LOG_READ);
    let mut output = Vec::with_capacity(MAX_LOG_READ.min(available));
    for chunk in chunks {
        let chunk_len = byte_count(chunk.len());
        if skip >= chunk_len {
            skip -= chunk_len;
            continue;
        }
        let start = usize::try_from(skip)
            .map_err(|_| CowshedError::internal("log offset exceeds platform range"))?;
        skip = 0;
        let remaining = MAX_LOG_READ - output.len();
        let take = remaining.min(chunk.len() - start);
        output.extend_from_slice(&chunk[start..start + take]);
        if output.len() == MAX_LOG_READ {
            break;
        }
    }
    let next_offset = offset + byte_count(output.len());
    Ok(Some(LogChunk {
        bytes: Bytes::from(output),
        next_offset,
        eof: (eof || job.terminal()) && next_offset == len,
    }))
}

/// Retire the process handle and release everything that was waiting on its stdin.
fn release_exited_process(job: &mut JobStateRecord) {
    job.process = None;
    for pending in job.pending_stdin.drain(..) {
        let _ = pending.reply.send(Err(CowshedError::conflict(
            "job exited before stdin was accepted",
            "inspect the terminal job status",
        )));
    }
    job.pending_stdin_bytes = 0;
    for waiter in job.close_waiters.drain(..) {
        let _ = waiter.send(Ok(()));
    }
    job.info.stdin.complete = true;
}

/// One bounded chunk of a sealed stream, read out of the artifact store.
///
/// The store's reader verifies bytes and digest as it goes and cannot seek, so an offset is
/// reached by reading and discarding. Paging a whole stream is therefore quadratic in chunk
/// count -- which is the right trade against keeping every terminal job's full output resident
/// forever, and is bounded anyway by the same `MAX_LOG_READ` frame the live path uses. An inline
/// stream, which is every output under the store's inline cap, touches no filesystem at all.
fn read_sealed_chunk(workspace_root: &Path, stream: &StreamInfo, offset: u64) -> Result<LogChunk> {
    let len = stream.bytes;
    if offset > len {
        return Err(CowshedError::conflict(
            "log offset is beyond the captured stream",
            "restart the read at the returned stream length",
        ));
    }
    if offset == len {
        return Ok(LogChunk {
            bytes: Bytes::new(),
            next_offset: len,
            eof: true,
        });
    }
    let mut reader = crate::storage::job_artifact::open_stream_reader(workspace_root, stream)
        .map_err(map_artifact_error)?;
    let mut discard = vec![0_u8; PROCESS_IO_CHUNK];
    let mut skipped = 0_u64;
    while skipped < offset {
        let want = usize::try_from(offset - skipped)
            .unwrap_or(PROCESS_IO_CHUNK)
            .min(PROCESS_IO_CHUNK);
        let read = reader
            .read_chunk(&mut discard[..want])
            .map_err(map_artifact_error)?;
        if read == 0 {
            return Err(CowshedError::integrity(
                "sealed stream ended before the requested log offset",
                "cowshed doctor --json",
            ));
        }
        skipped += byte_count(read);
    }
    let want = usize::try_from(len - offset)
        .unwrap_or(MAX_LOG_READ)
        .min(MAX_LOG_READ);
    let mut output = vec![0_u8; want];
    let mut filled = 0;
    while filled < want {
        let read = reader
            .read_chunk(&mut output[filled..])
            .map_err(map_artifact_error)?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    output.truncate(filled);
    let next_offset = offset + byte_count(filled);
    Ok(LogChunk {
        bytes: Bytes::from(output),
        next_offset,
        eof: next_offset == len,
    })
}

fn flush_log_waiters(job: &mut JobStateRecord) {
    let waiters = std::mem::take(&mut job.log_waiters);
    for waiter in waiters {
        match make_log_chunk(job, waiter.stream, waiter.offset) {
            Ok(Some(chunk)) => {
                let _ = waiter.reply.send(Ok(chunk));
            }
            Ok(None) if !job.terminal() => job.log_waiters.push(waiter),
            Ok(None) => {
                let (_, len, _) = job.stream(waiter.stream);
                let _ = waiter.reply.send(Ok(LogChunk {
                    bytes: Bytes::new(),
                    next_offset: len,
                    eof: true,
                }));
            }
            Err(error) => {
                let _ = waiter.reply.send(Err(error));
            }
        }
    }
}

fn validate_session_name(name: &str) -> Result<()> {
    if (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Ok(())
    } else {
        Err(CowshedError::usage(
            "invalid session name",
            "use 1-64 ASCII letters, digits, dash, underscore, or dot",
        ))
    }
}

/// A checkpoint barrier is published as a commitment, so the commitment id grammar is the only
/// grammar there is. Restating it here let the two drift silently in either direction.
fn validate_checkpoint_id(value: &str) -> Result<()> {
    if crate::api::dto::valid_commitment_id(value) {
        Ok(())
    } else {
        Err(CowshedError::usage(
            "invalid checkpoint commitment id",
            "use a 1-128 character alphanumeric checkpoint id",
        ))
    }
}

fn new_trace_context() -> TraceContext {
    let trace = Uuid::new_v4().simple().to_string();
    let span = Uuid::new_v4().simple().to_string();
    TraceContext {
        trace_id: TraceId::new(trace).expect("UUID simple form is a nonzero trace id"),
        span_id: crate::api::dto::SpanId::new(&span[..16])
            .expect("UUID prefix is a nonzero span id"),
    }
}

/// The current UTC second as the API's timestamp type.
///
/// Uses the crate's total civil-date conversion rather than a third `libc::gmtime_r`: that call
/// needs a `time_t` the seconds may not fit, can return null, and requires `unsafe` twice to read
/// its out-parameter, all to compute what `SystemTime` already holds. `civil_from_days` is total
/// over every `u64` second count, which is exactly why it exists. The one remaining failure is a
/// clock before the epoch, which is a real operational fault, not an invariant.
pub(crate) fn utc_now() -> Result<UtcTimestamp> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CowshedError::internal(format!("system clock is before epoch: {error}")))?
        .as_secs();
    let (year, month, day) = crate::storage::civil_from_days(seconds / 86_400);
    let clock = seconds % 86_400;
    UtcTimestamp::new(format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        clock / 3_600,
        clock % 3_600 / 60,
        clock % 60,
    ))
    .map_err(|error| CowshedError::internal(error.to_string()))
}

fn byte_count(value: usize) -> u64 {
    u64::try_from(value).expect("supported platforms have at most 64-bit usize")
}

pub(super) fn map_exec_error(error: crate::exec::ExecError) -> CowshedError {
    match error {
        crate::exec::ExecError::InvalidRequest { .. } => CowshedError::usage(
            error.to_string(),
            "provide a valid executable and workspace cwd",
        ),
        crate::exec::ExecError::SandboxDenied { .. } => CowshedError::sandbox_denied(
            error.to_string(),
            "request only paths admitted by the workspace grant snapshot",
        ),
        crate::exec::ExecError::WrapperFailure { .. } => CowshedError::environment_missing(
            error.to_string(),
            "verify the macOS sandbox execution environment",
        ),
    }
}

fn map_sandbox_error(error: crate::sandbox::SandboxError) -> CowshedError {
    CowshedError::sandbox_denied(
        error.to_string(),
        "repair the authoritative workspace grant snapshot",
    )
}

fn missing_artifact_token(job_id: JobId) -> CowshedError {
    CowshedError::integrity(
        format!("job {} has no live artifact token", job_id.get()),
        "cowshed doctor --json",
    )
}

/// A store refusal as the caller reports it: records a newer cowshed wrote are a version
/// conflict this build must not touch; anything else is damage to the store.
pub(super) fn map_artifact_error(error: ArtifactError) -> CowshedError {
    match error {
        ArtifactError::NewerLayout { .. } => CowshedError::conflict(
            error.to_string(),
            "run the cowshed that wrote these records; this build is older",
        ),
        error => CowshedError::integrity(error.to_string(), "cowshed doctor --json"),
    }
}

fn map_audit_error(error: AuditSinkError) -> CowshedError {
    match error {
        AuditSinkError::Io { .. } => CowshedError::environment_missing(
            error.to_string(),
            "verify telemetry storage or set COWSHED_CONTINUITY_AUDIT=off",
        ),
        AuditSinkError::Integrity { .. } => {
            CowshedError::integrity(error.to_string(), "cowshed doctor --json")
        }
    }
}

fn not_found_job(job_id: JobId) -> CowshedError {
    CowshedError::not_found(
        format!(
            "job {} does not exist in this workspace incarnation",
            job_id.get()
        ),
        "list jobs on the current workspace",
    )
}

fn unsealed_job(job_id: JobId) -> CowshedError {
    CowshedError::not_found(
        format!(
            "job {} has no terminal record in this workspace incarnation",
            job_id.get()
        ),
        "a running job is answered by its status; list jobs on the current workspace",
    )
}

fn retiring_error() -> CowshedError {
    CowshedError::conflict(
        "workspace supervisor is quiescing or retired",
        "reattach an active workspace before starting work",
    )
}

#[cfg(test)]
mod workspace_toolchain_tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use crate::sandbox::{
        RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole, nix_daemon_socket,
        seatbelt_profile,
    };

    // Only the macOS host-controller test below builds a sandbox; on the Linux
    // cross lint the helper would be dead code.
    #[cfg(target_os = "macos")]
    fn sandbox_at(mount: &Path) -> SandboxConfig {
        SandboxConfig {
            home: mount.parent().expect("root").join("home"),
            mount_root: mount.parent().expect("root").to_path_buf(),
            workspace_mount: mount.to_path_buf(),
            exec_temp_dir: mount.parent().expect("root").join("tmp"),
            port_block: crate::metadata::PortBlock::new(40_960, 16).expect("port block"),
            mode: RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: nix_daemon_socket().into_iter().collect(),
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            shared_tool_homes: Vec::new(),
        }
    }

    fn scratch(test: &str) -> PathBuf {
        let root = std::fs::canonicalize(std::env::temp_dir())
            .expect("temp dir")
            .join(format!(
                "cowshed-toolchain-{test}-{}",
                Uuid::new_v4().simple()
            ));
        std::fs::create_dir_all(&root).expect("scratch root");
        root
    }

    /// A child finds every package manager's route to the gateway, and every one-file TLS client
    /// finds a bundle that trusts both the platform roots and the workspace CA — from the files
    /// and variables the host prepared before the spawn, with no tracked file touched.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_child_finds_its_mirror_clients_and_a_trust_bundle_with_the_workspace_ca() {
        let root = scratch("clients");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::create_dir_all(root.join("home")).expect("home");
        std::fs::write(mount.join(".cowshed/token"), "A".repeat(43)).expect("token");
        std::fs::write(
            mount.join(".cowshed/ca.pem"),
            b"-----BEGIN CERTIFICATE-----\nWORKSPACE\n-----END CERTIFICATE-----\n",
        )
        .expect("workspace CA");
        // The wiring before this one left a token-bearing netrc in the private HOME.
        std::fs::create_dir_all(mount.join(".cowshed/home")).expect("private home");
        std::fs::write(
            mount.join(".cowshed/home/.netrc"),
            "machine 127.0.0.1\nlogin cowshed\npassword stale\n",
        )
        .expect("stale netrc");
        let mut sandbox = sandbox_at(&mount);
        sandbox.port_block = crate::metadata::PortBlock::new(49_104, 16).expect("port block");
        let mut env = BTreeMap::new();
        env.insert(
            "NIX_CONFIG".to_owned(),
            "extra-experimental-features = flakes".to_owned(),
        );
        env.insert("CALLER_ONLY".to_owned(), "kept".to_owned());
        env.insert("HOME".to_owned(), "/caller/home".to_owned());

        let environment = sandbox_environment(&sandbox, None, &env)
            .await
            .expect("environment");
        let child = environment.child(&env);
        // A warm exec host starts from `base` and each command adds `overlay`: the command must
        // see exactly what a one-shot child of the same request sees.
        let mut pooled = environment.base();
        pooled.extend(environment.overlay(&env));
        assert_eq!(
            pooled, child,
            "pooled and one-shot children see one environment"
        );
        let vars: BTreeMap<String, String> = child
            .into_iter()
            .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
            .collect();
        assert_eq!(vars.get("CALLER_ONLY").map(String::as_str), Some("kept"));
        assert_eq!(
            vars.get("HOME").map(PathBuf::from),
            Some(mount.join(".cowshed/home")),
            "the sandbox owns HOME"
        );

        let bundle = mount.join(".cowshed/ca-bundle.pem");
        for name in [
            "GIT_SSL_CAINFO",
            "CARGO_HTTP_CAINFO",
            "NIX_SSL_CERT_FILE",
            "SSL_CERT_FILE",
        ] {
            assert_eq!(
                vars.get(name).map(PathBuf::from),
                Some(bundle.clone()),
                "{name}"
            );
        }
        // uv verifies against its own bundled roots unless told to use the platform's, and then
        // reads SSL_CERT_FILE as that platform bundle.
        assert_eq!(
            vars.get("UV_SYSTEM_CERTS").map(String::as_str),
            Some("true")
        );
        // nix.conf's own `ssl-cert-file` outranks NIX_SSL_CERT_FILE; NIX_CONFIG outranks
        // nix.conf, and a caller's NIX_CONFIG keeps its own lines.
        assert_eq!(
            vars.get("NIX_CONFIG").map(String::as_str),
            Some(
                format!(
                    "extra-experimental-features = flakes\nssl-cert-file = {}",
                    bundle.display()
                )
                .as_str()
            )
        );
        let bundle_bytes = std::fs::read_to_string(&bundle).expect("bundle");
        assert!(bundle_bytes.ends_with("WORKSPACE\n-----END CERTIFICATE-----\n"));
        assert!(
            bundle_bytes.len() > 10_000,
            "platform roots precede the workspace CA"
        );

        let private = mount.join(".cowshed");
        let token = "A".repeat(43);
        assert_eq!(
            std::fs::read_to_string(private.join("config/.bunfig.toml")).expect("bunfig"),
            format!(
                "[install]\nregistry = {{ url = \"http://127.0.0.1:49104/npm/\", token = \"{token}\" }}\n"
            )
        );
        assert_eq!(
            vars.get("XDG_CONFIG_HOME").map(PathBuf::from),
            Some(private.join("config"))
        );
        // Go sends credentials only over HTTPS, so it cannot authenticate to the loopback mirror:
        // it fetches from the public proxy through an opaque tunnel, and no token file exists.
        let go_env = std::fs::read_to_string(private.join("cache/go/env")).expect("go env");
        assert!(
            go_env.contains("GOPROXY=https://proxy.golang.org\n"),
            "{go_env}"
        );
        assert!(!go_env.contains("127.0.0.1"), "{go_env}");
        assert_eq!(
            vars.get("GOENV").map(PathBuf::from),
            Some(private.join("cache/go/env"))
        );
        assert!(
            !private.join("home/.netrc").exists(),
            "the netrc a previous wiring left, token and all, is gone"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Every cargo a workspace runs wraps rustc with the sccache the host pinned: the program
    /// itself, not a name the repository shell's `PATH` may not resolve. A host that pinned none,
    /// or whose pinned store path was collected, builds without a wrapper rather than failing
    /// every cargo at its version probe. Neither the wrapper nor its cwd normalization is the
    /// caller's, in a one-shot child or a warm shell's command, and `CARGO_INCREMENTAL` stays
    /// cargo's to decide.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn cargo_wraps_rustc_with_the_pinned_sccache_or_with_nothing() {
        let root = scratch("sccache-client");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::write(mount.join(".cowshed/token"), "A".repeat(43)).expect("token");
        let mut sandbox = sandbox_at(&mount);
        sandbox.port_block = crate::metadata::PortBlock::new(49_072, 16).expect("port block");
        std::fs::create_dir_all(&sandbox.home).expect("home");
        let caller = BTreeMap::from([
            ("RUSTC_WRAPPER".to_owned(), "/bin/false".to_owned()),
            ("SCCACHE_BASEDIR_CWD".to_owned(), "0".to_owned()),
        ]);
        let wiring = async || {
            let environment = sandbox_environment(&sandbox, None, &caller)
                .await
                .expect("environment");
            let child = environment.child(&caller);
            let mut pooled = environment.base();
            pooled.extend(environment.overlay(&caller));
            assert_eq!(
                pooled, child,
                "pooled and one-shot children see one environment"
            );
            ["RUSTC_WRAPPER", "SCCACHE_BASEDIR_CWD", "CARGO_INCREMENTAL"]
                .map(|name| child.get(OsStr::new(name)).cloned())
        };
        let unwrapped = [None, Some(OsString::from("1")), None];
        assert_eq!(wiring().await, unwrapped, "no sccache pinned");

        let store_path = root.join("store/0000-sccache-cowshed");
        std::fs::create_dir_all(store_path.join("bin")).expect("store path");
        std::fs::write(store_path.join("bin/sccache"), b"").expect("program");
        let gc_root = crate::sandbox::sccache_gc_root(&sandbox.home);
        std::fs::create_dir_all(gc_root.parent().expect("parent")).expect("support directory");
        std::os::unix::fs::symlink(&store_path, &gc_root).expect("gc root");
        assert_eq!(
            wiring().await,
            [
                Some(store_path.join("bin/sccache").into_os_string()),
                Some(OsString::from("1")),
                None,
            ],
            "the pinned program"
        );

        std::fs::remove_dir_all(&store_path).expect("collect the store path");
        assert_eq!(wiring().await, unwrapped, "a collected store path");
        std::fs::remove_dir_all(&root).ok();
        std::fs::remove_file(sandbox_runtime_link(&sandbox)).ok();
    }

    /// Every spawn of a live workspace finds its link already in place, so it must not scan the
    /// directory the links share (on a busy host `/tmp` holds thousands of entries); only taking
    /// a link sweeps the dangling ones a retired workspace left beside it.
    #[tokio::test]
    async fn only_taking_a_runtime_link_sweeps_the_dangling_links_beside_it() {
        let root = scratch("runtime-link");
        let runtime = root.join("run");
        let links = root.join("links");
        std::fs::create_dir_all(&runtime).expect("runtime dir");
        std::fs::create_dir_all(&links).expect("link directory");
        let dangling = links.join("cs-retired");
        std::os::unix::fs::symlink(root.join("retired/run"), &dangling).expect("dangling link");
        let live = links.join("cs-live");
        std::os::unix::fs::symlink(&runtime, &live).expect("live link");

        point_runtime_link(&live, &runtime)
            .await
            .expect("current link");
        assert!(
            dangling.symlink_metadata().is_ok(),
            "a link already in place reads only itself"
        );

        let taken = links.join("cs-taken");
        point_runtime_link(&taken, &runtime)
            .await
            .expect("taken link");
        assert_eq!(std::fs::read_link(&taken).expect("taken link"), runtime);
        assert_eq!(std::fs::read_link(&live).expect("live link"), runtime);
        assert!(
            dangling.symlink_metadata().is_err(),
            "taking a link sweeps the dangling one"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_workspace_without_an_evaluated_profile_is_unchanged() {
        let root = scratch("absent");
        let mount = root.join("workspace");
        std::fs::create_dir_all(&mount).expect("mount");
        let devenv_root = mount.join("tooling/devenv");
        std::fs::create_dir_all(&devenv_root).expect("devenv root");

        assert_eq!(workspace_profile_bin(&mount, &devenv_root), None);

        // A profile that does not resolve into the store is not a profile. This is the substitution
        // guard: a workspace can create any symlink it likes inside its own volume, and only one
        // that lands in the immutable store may go on PATH.
        let profile_state = devenv_root.join(".devenv");
        std::fs::create_dir_all(&profile_state).expect("devenv state");
        let decoy = root.join("decoy/bin");
        std::fs::create_dir_all(&decoy).expect("decoy");
        std::os::unix::fs::symlink(root.join("decoy"), profile_state.join("profile"))
            .expect("decoy link");
        assert_eq!(workspace_profile_bin(&mount, &devenv_root), None);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn devenv_resolution_prefers_config_then_root_then_none() {
        let root = scratch("resolution");
        let mount = root.join("workspace");
        let configured = mount.join("tooling/devenv");
        std::fs::create_dir_all(&configured).expect("configured devenv");
        std::fs::write(mount.join("devenv.nix"), "{}").expect("root devenv");
        std::fs::write(configured.join("devenv.nix"), "{}").expect("configured devenv");
        std::fs::write(
            mount.join(COWSHED_CONFIG_FILE),
            "[devenv]\ndir = \"tooling/devenv\"\n",
        )
        .expect("config");

        assert_eq!(resolve_devenv_dir(&mount).unwrap(), Some(configured));

        std::fs::remove_file(mount.join(COWSHED_CONFIG_FILE)).expect("remove config");
        assert_eq!(resolve_devenv_dir(&mount).unwrap(), Some(mount.clone()));

        std::fs::remove_file(mount.join("devenv.nix")).expect("remove root devenv");
        assert_eq!(resolve_devenv_dir(&mount).unwrap(), None);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn configured_devenv_without_devenv_nix_is_an_error() {
        let root = scratch("configured-missing");
        let mount = root.join("workspace");
        std::fs::create_dir_all(&mount).expect("mount");
        std::fs::write(
            mount.join(COWSHED_CONFIG_FILE),
            "[devenv]\ndir = \"tooling/devenv\"\n",
        )
        .expect("config");

        let error = resolve_devenv_dir(&mount).unwrap_err();
        assert_eq!(
            error.into_cowshed_error().code,
            crate::error::ErrorCode::EnvironmentMissing
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// A supervisor the daemon starts inherits launchd's PATH, which names no Nix profile. The
    /// user's own profile still puts its tools — `direnv` above all, which every activation
    /// runs — on the bootstrap PATH, where the old controller found them only because it
    /// inherited an interactive shell's PATH.
    #[test]
    fn a_supervisor_started_with_launchds_path_still_finds_the_users_profile_tools() {
        // Any store `bin` on this test's PATH stands in for the user's profile generation.
        // Absence fails: cowshed requires Nix, and this repository's own shell is devenv.
        let store_bin = std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
            .filter_map(|entry| std::fs::canonicalize(entry).ok())
            .find(|entry| entry.starts_with("/nix/store") && entry.ends_with("bin"))
            .expect("a /nix/store bin directory on PATH; cowshed requires Nix");
        let root = scratch("launchd-path");
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("home");
        std::os::unix::fs::symlink(
            store_bin.parent().expect("a store path"),
            home.join(".nix-profile"),
        )
        .expect("profile link");
        let sandbox = SandboxConfig {
            home: home.clone(),
            mount_root: root.clone(),
            workspace_mount: root.join("workspace"),
            exec_temp_dir: root.join("tmp"),
            port_block: crate::metadata::PortBlock::new(40_960, 16).expect("port block"),
            mode: crate::sandbox::RunSandboxMode::ReadWrite,
            grants: crate::sandbox::SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            shared_tool_homes: Vec::new(),
        };

        let path = bootstrap_path_from(
            &sandbox,
            None,
            &host_profile_bins(&home, Some(OsStr::new("nobody-in-particular"))),
            Some(OsStr::new("/usr/bin:/bin:/usr/sbin:/sbin")),
        )
        .expect("bootstrap PATH");
        let entries: Vec<PathBuf> = std::env::split_paths(&path).collect();
        assert!(
            entries.contains(&store_bin),
            "the user's profile is on the bootstrap PATH: {entries:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// The end of the mechanism, exercised for real: a workspace whose `devenv` evaluation
    /// materialized a store profile gets that profile's tools on `PATH`, ahead of the inherited
    /// roots, and can actually execute them inside its own Seatbelt sandbox.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_an_evaluated_workspace_profile_leads_path_and_runs_inside_the_sandbox() {
        // A store-resolved profile root, standing in for a devenv-generated one without pinning
        // a generated store path. The daemon profile is the obvious candidate but is absent on a
        // single-user Nix install, so any store `bin` already on PATH serves equally: it is the
        // same immutable-store class, reached through the same canonicalization this test is
        // about.
        //
        // Absence is a FAILURE, not a skip. Nix is a hard requirement of cowshed -- the sandbox
        // admits the daemon socket as a standing grant, this repository's own dev shell is
        // devenv, and a host with no store cannot run a workspace at all. The `return`s that used
        // to sit here made this test report success on exactly the hosts where its subject does
        // not exist, which is the one outcome worse than failing.
        let store_profile = std::fs::canonicalize("/nix/var/nix/profiles/default")
            .ok()
            .filter(|profile| profile.starts_with("/nix/store"))
            .or_else(|| {
                std::env::split_paths(&std::env::var_os("PATH")?)
                    .filter_map(|entry| std::fs::canonicalize(entry).ok())
                    .find(|entry| entry.starts_with("/nix/store") && entry.ends_with("bin"))
                    .and_then(|bin| bin.parent().map(Path::to_path_buf))
            })
            .expect(
                "no /nix/store profile is resolvable on this host; cowshed requires Nix, so this \
                 is a broken environment rather than a test that does not apply",
            );
        let tool = std::fs::read_dir(store_profile.join("bin"))
            .expect("a store profile has a bin directory")
            .find_map(|entry| {
                let path = entry.ok()?.path();
                path.is_file().then_some(path)
            })
            .expect("a store profile's bin directory is not empty");

        let root = scratch("profile");
        let mount = root.join("workspace");
        let devenv_root = mount.join("tooling/devenv");
        let config = sandbox_at(&mount);
        for directory in [
            &devenv_root.join(".devenv"),
            &config.home,
            &config.exec_temp_dir,
        ] {
            std::fs::create_dir_all(directory).expect("directory");
        }
        // Exactly what `devenv shell` leaves behind: an in-image symlink into the store.
        std::os::unix::fs::symlink(&store_profile, devenv_root.join(".devenv/profile"))
            .expect("profile link");

        let profile_bin =
            workspace_profile_bin(&mount, &devenv_root).expect("an evaluated profile is admitted");
        // Resolved all the way through: a profile's `bin` is itself a symlink chain inside the
        // store, and what goes on PATH is the immutable path it finally reaches.
        assert!(profile_bin.starts_with("/nix/store"));
        assert_eq!(
            profile_bin,
            std::fs::canonicalize(store_profile.join("bin")).expect("resolved profile bin")
        );

        let path = bootstrap_path(&config, Some(&devenv_root)).expect("bootstrap PATH");
        let entries: Vec<PathBuf> = std::env::split_paths(&path).collect();
        assert_eq!(
            entries.get(1),
            Some(&profile_bin),
            "the workspace's own toolchain comes before the inherited roots, or an edited \
             devenv.nix loses to the controller's environment"
        );

        // And it is genuinely reachable: the store read grants have to cover the resolved profile,
        // or PATH names a tool the sandbox refuses to exec.
        let profile =
            seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).expect("profile");
        let status = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &profile, "--", "/bin/test", "-x"])
            .arg(&tool)
            .status()
            .expect("sandbox-exec");
        // Asserted here, before the fallback-path mutations below. Deferring it to the end of the
        // function meant any later panic silently discarded the only runtime check in the file.
        assert!(
            status.success(),
            "a tool on the workspace profile must be executable inside the sandbox"
        );

        // Native devenv bindings anchor `.devenv` at the allowed repository root. Workspace paths
        // have no binding, but accepting this fallback keeps a profile evaluated before mounting
        // usable without weakening the same store-path guard.
        std::fs::remove_file(devenv_root.join(".devenv/profile")).expect("nested profile");
        std::fs::create_dir_all(mount.join(".devenv")).expect("root devenv state");
        std::os::unix::fs::symlink(&store_profile, mount.join(".devenv/profile"))
            .expect("root profile link");
        assert_eq!(
            workspace_profile_bin(&mount, &devenv_root),
            Some(profile_bin.clone())
        );

        std::fs::remove_dir_all(&root).ok();
    }
}

#[cfg(test)]
mod lifecycle_commitment_tests {
    use super::*;

    #[tokio::test]
    async fn publisher_records_every_act_and_reports_sink_health() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-lifecycle-publisher-{}",
            Uuid::new_v4().simple()
        ));
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let mut publisher =
            CommitmentPublisher::open(&root, crate::storage::audit::ContinuityAudit::Arrow, 4)
                .unwrap();
        publisher
            .record(CommitmentDraft::WorkspaceIntroduced {
                repo_id: repo_id.clone(),
                workspace_incarnation: incarnation.clone(),
            })
            .await
            .unwrap();
        publisher
            .record(CommitmentDraft::WorkspaceRetired {
                repo_id: repo_id.clone(),
                workspace_incarnation: incarnation.clone(),
            })
            .await
            .unwrap();
        let health = publisher.health().await.unwrap();
        assert_eq!(health.sink, "arrow");
        assert_eq!((health.recorded, health.failed), (2, 0));
        let sealed = std::fs::read_dir(&root)
            .unwrap()
            .flat_map(|date| std::fs::read_dir(date.unwrap().path()).unwrap())
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("commitment-")
            })
            .count();
        assert_eq!(sealed, 2, "one sealed segment per record");
        drop(publisher);

        let mut silent =
            CommitmentPublisher::open(&root, crate::storage::audit::ContinuityAudit::Off, 4)
                .unwrap();
        silent
            .record(CommitmentDraft::WorkspaceIntroduced {
                repo_id,
                workspace_incarnation: incarnation,
            })
            .await
            .unwrap();
        let health = silent.health().await.unwrap();
        assert_eq!((health.sink, health.recorded, health.failed), ("off", 1, 0));
        drop(silent);
        tokio::task::yield_now().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn publish_sealed_job(
        publisher: &mut CommitmentPublisherHandle,
        repo_id: &RepoId,
        incarnation: &WorkspaceIncarnation,
        sealed: &crate::storage::job_artifact::SealedJobArtifacts,
    ) {
        publisher
            .record(CommitmentDraft::Admission {
                repo_id: repo_id.clone(),
                workspace_incarnation: incarnation.clone(),
                job_id: sealed.record.job_id,
                grant_revision: sealed.record.grant_revision,
            })
            .await
            .unwrap();
        publisher
            .record(CommitmentDraft::Terminal {
                repo_id: repo_id.clone(),
                workspace_incarnation: incarnation.clone(),
                job_id: sealed.record.job_id,
                state: sealed.record.state,
                grant_revision: sealed.record.grant_revision,
                stdout_bytes: sealed.record.stdout.bytes,
                stdout_sha256: sealed.record.stdout.sha256,
                stderr_bytes: sealed.record.stderr.bytes,
                stderr_sha256: sealed.record.stderr.sha256,
                batch_sha256: sealed.terminal_batch_sha256,
                output_limit: sealed.output_limit.clone(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    /// Named for what it checks: ancestor incarnations named in the lineage are admitted when a
    /// replacement supervisor opens the store, and an incarnation that was never introduced is an
    /// integrity fault. Restart behaviour of the resident job set is asserted, not assumed.
    async fn lineage_admits_ancestor_records_and_refuses_an_unintroduced_incarnation() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-restored-supervisor-{}",
            Uuid::new_v4().simple()
        ));
        let telemetry = root.join("telemetry");
        let workspace_root = root.join("workspace");
        let unintroduced_root = root.join("unintroduced");
        std::fs::create_dir_all(workspace_root.join(".cowshed")).unwrap();
        std::fs::create_dir_all(&unintroduced_root).unwrap();
        // A supervisor start publishes `.cowshed/env` from the image's token.
        {
            use std::io::Write as _;
            use std::os::unix::fs::OpenOptionsExt as _;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(workspace_root.join(crate::workspace_credentials::WORKSPACE_TOKEN_PATH))
                .unwrap()
                .write_all("A".repeat(43).as_bytes())
                .unwrap();
        }
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let foreign_repo = RepoId::parse("other/repository").unwrap();
        let source = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let destination = WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").unwrap();
        let second_destination =
            WorkspaceIncarnation::new("4198f2c0b7e34dc795f17b238b331c80").unwrap();
        let foreign_only = WorkspaceIncarnation::new("2198f2c0b7e34dc795f17b238b331c80").unwrap();
        let baseline_only = WorkspaceIncarnation::new("3198f2c0b7e34dc795f17b238b331c80").unwrap();
        let mut publisher =
            CommitmentPublisher::open(&telemetry, crate::storage::audit::ContinuityAudit::Arrow, 8)
                .unwrap();
        publisher
            .record(CommitmentDraft::WorkspaceIntroduced {
                repo_id: repo_id.clone(),
                workspace_incarnation: source.clone(),
            })
            .await
            .unwrap();

        let mut artifacts = ArtifactStore::open(
            &workspace_root,
            OwnedRepoIds::sole(repo_id.clone()),
            source.clone(),
            ArtifactConfig::default(),
        )
        .unwrap();
        let first = artifacts
            .begin_job(
                JobId::new(1).unwrap(),
                7,
                &crate::api::dto::ExecCommand::Argv(vec!["true".into()]),
                None,
                OutputTargets::default(),
            )
            .unwrap();
        let first = artifacts.finish(first, JobState::Exited).unwrap();
        publish_sealed_job(&mut publisher, &repo_id, &source, &first).await;
        let checkpoint = artifacts.checkpoint().unwrap();
        publisher
            .record(CommitmentDraft::Checkpoint {
                repo_id: repo_id.clone(),
                origin_incarnation: source.clone(),
                checkpoint_id: "baseline".into(),
                barrier_id: checkpoint.record.barrier_id,
                manifest_batch_sha256: checkpoint.manifest_batch_sha256,
            })
            .await
            .unwrap();

        let later = artifacts
            .begin_job(
                JobId::new(2).unwrap(),
                8,
                &crate::api::dto::ExecCommand::Argv(vec!["true".into()]),
                None,
                OutputTargets::default(),
            )
            .unwrap();
        let later = artifacts.finish(later, JobState::Exited).unwrap();
        publish_sealed_job(&mut publisher, &repo_id, &source, &later).await;
        drop(artifacts);
        publisher
            .record(CommitmentDraft::Restore {
                repo_id: repo_id.clone(),
                source_checkpoint: "baseline".into(),
                source_incarnation: source.clone(),
                replaced_incarnation: source.clone(),
                destination_incarnation: destination.clone(),
            })
            .await
            .unwrap();
        publisher
            .record(CommitmentDraft::Restore {
                repo_id: repo_id.clone(),
                source_checkpoint: "baseline".into(),
                source_incarnation: source.clone(),
                replaced_incarnation: destination.clone(),
                destination_incarnation: second_destination.clone(),
            })
            .await
            .unwrap();
        publisher
            .record(CommitmentDraft::WorkspaceIntroduced {
                repo_id: foreign_repo,
                workspace_incarnation: foreign_only.clone(),
            })
            .await
            .unwrap();

        // The lineage a marker carries after restore → restore: nearest ancestor first. The
        // records file under `workspace_root` was written by `source`, so it opens under any
        // incarnation whose lineage names `source`; nothing names `foreign_only` or
        // `baseline_only`, and a records file from them is an integrity fault.
        let admitted: BTreeSet<WorkspaceIncarnation> =
            BTreeSet::from([destination.clone(), source.clone()]);
        assert!(!admitted.contains(&foreign_only));
        assert!(!admitted.contains(&baseline_only));

        let defaults = WorkspaceSupervisorConfig::default();
        let config = WorkspaceSupervisorConfig {
            authority: WorkspaceAuthoritySnapshot {
                repo_id: repo_id.clone(),
                workspace: WorkspaceName::new("raven").unwrap(),
                workspace_incarnation: second_destination.clone(),
                grant_revision: 8,
                lifecycle_revision: 2,
            },
            owned_repo_ids: OwnedRepoIds::sole(repo_id.clone()),
            workspace_root: workspace_root.clone(),
            default_cwd: None,
            sandbox: SandboxConfig {
                workspace_mount: workspace_root.clone(),
                ..defaults.sandbox
            },
            artifacts: ArtifactConfig {
                historical_incarnations: admitted.clone(),
                ..ArtifactConfig::default()
            },
            term_grace: defaults.term_grace,
            actor_capacity: defaults.actor_capacity,
            event_capacity: defaults.event_capacity,
            credential_env_names: defaults.credential_env_names,
            shell_host: None,
            shell_pool: defaults.shell_pool,
            group_ledger: None,
        };
        // `list()`/`info()` answer from the actor's resident job set, which is this supervisor's
        // own lifetime and deliberately not the durable history: the artifact store holds every
        // sealed record, and the actor releases output at seal rather than accumulating it. So a
        // replacement supervisor starts with an empty resident set even though the store it opened
        // already contains sealed jobs from `source` and `destination`.
        //
        // Asserting only that `list()` is `Ok` could not tell that apart from a supervisor that
        // reloaded history, or from one that returned an error-free empty vector for the wrong
        // reason. These assertions can go red in both directions.
        let sealed_job = JobId::new(1).unwrap();
        let first_supervisor =
            WorkspaceSupervisor::start(config.clone(), publisher.clone()).unwrap();
        assert!(
            first_supervisor.list().await.unwrap().is_empty(),
            "the resident job set is this supervisor's own, not the store's history"
        );
        assert_eq!(
            first_supervisor.info(sealed_job).await.unwrap_err().code,
            crate::error::ErrorCode::NotFound,
            "a job sealed by an ancestor incarnation is not resident in a fresh supervisor"
        );
        drop(first_supervisor);
        tokio::task::yield_now().await;

        // Restarting over the same store is the case the lineage admission exists for: opening
        // must succeed with the ancestor records present, and must answer identically.
        let restarted = WorkspaceSupervisor::start(config, publisher.clone()).unwrap();
        assert!(restarted.list().await.unwrap().is_empty());
        assert_eq!(
            restarted.info(sealed_job).await.unwrap_err().code,
            crate::error::ErrorCode::NotFound
        );
        drop(restarted);

        let mut unintroduced = ArtifactStore::open(
            &unintroduced_root,
            OwnedRepoIds::sole(repo_id.clone()),
            foreign_only.clone(),
            ArtifactConfig::default(),
        )
        .unwrap();
        let token = unintroduced
            .begin_job(
                JobId::new(1).unwrap(),
                1,
                &crate::api::dto::ExecCommand::Argv(vec!["true".into()]),
                None,
                OutputTargets::default(),
            )
            .unwrap();
        unintroduced.finish(token, JobState::Exited).unwrap();
        drop(unintroduced);
        assert!(matches!(
            ArtifactStore::open(
                &unintroduced_root,
                OwnedRepoIds::sole(repo_id),
                destination,
                ArtifactConfig {
                    historical_incarnations: admitted,
                    ..ArtifactConfig::default()
                },
            ),
            Err(ArtifactError::Integrity { .. })
        ));

        drop(publisher);
        tokio::task::yield_now().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod sandbox_environment_tests {
    use super::*;

    fn scratch(label: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("cowshed-{label}-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::canonicalize(root).unwrap()
    }

    #[test]
    fn proxy_url_carries_the_token_as_basic_auth_userinfo() {
        let token = WorkspaceToken::from_bytes([7; 32]);
        let encoded = token.encode();
        assert_eq!(
            gateway_proxy_url("40960", &token),
            format!("http://cowshed:{encoded}@127.0.0.1:40960")
        );
    }

    /// Nix keeps profile and channel state under `$XDG_STATE_HOME/nix` as it keeps its fetcher
    /// cache under `$XDG_CACHE_HOME/nix`. Both private roots link into the caches volume, so
    /// every workspace sees the Nix client state any one of them wrote; a host without the caches
    /// volume keeps plain private roots and fails nothing.
    #[test]
    fn the_private_nix_client_roots_link_to_the_shared_ones() {
        let root = scratch("private-environment");
        let environment = root.join("environment");
        let caches = root.join("caches");
        std::fs::create_dir_all(&caches).unwrap();
        prepare_private_environment(&environment, &caches).unwrap();
        for name in ["cache", "state"] {
            assert_eq!(
                std::fs::read_link(environment.join(name).join("nix")).ok(),
                Some(caches.join("nix").join(name)),
                "{name}/nix"
            );
            assert!(caches.join("nix").join(name).is_dir(), "{name}");
        }

        let bare = root.join("bare");
        prepare_private_environment(&bare, &root.join("no-caches-volume")).unwrap();
        for name in ["home", "config", "cache", "data", "state", "run"] {
            assert!(bare.join(name).is_dir(), "{name}");
        }
        assert!(std::fs::symlink_metadata(bare.join("state/nix")).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Nothing on the host is relocated for the caches a tool's own configuration names (Go's
    /// env file, `TTSC_CACHE_DIR`), and a child granted writes inside one cannot create its
    /// parent: the directories exist before any child runs.
    #[test]
    fn directly_configured_caches_exist_before_a_child_runs() {
        let root = scratch("direct-caches");
        let caches = root.join("caches");
        std::fs::create_dir_all(&caches).unwrap();
        prepare_private_environment(&root.join("environment"), &caches).unwrap();
        for directory in ["go/mod", "go/build", "ttsc"] {
            assert!(caches.join(directory).is_dir(), "{directory}");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    fn built(caller: &[(&str, &str)]) -> BTreeMap<String, String> {
        let caller = caller
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        caller_environment(&caller)
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect()
    }

    /// Whatever the caller names arrives verbatim, including the value nothing in cowshed would
    /// ever choose. The builder has no opinion left to impose.
    #[test]
    fn a_caller_owns_cargo_incremental_outright() {
        for requested in ["1", "0", ""] {
            assert_eq!(
                built(&[("CARGO_INCREMENTAL", requested)]).get("CARGO_INCREMENTAL"),
                Some(&requested.to_owned()),
                "the child must see exactly the CARGO_INCREMENTAL its caller named"
            );
        }
    }

    #[test]
    fn caller_config_parameters_cannot_bypass_the_managed_git_include() {
        let root = scratch("git-environment-injection");
        let injected = [("GIT_CONFIG_PARAMETERS", "'cowshed.injected=unsafe'")];
        let run = |environment: BTreeMap<String, String>| {
            // env_clear drops the ambient GIT_* variables; PATH passes through so
            // git resolves on NixOS runners too, where there is no /usr/bin/git.
            std::process::Command::new("git")
                .args(["config", "--get", "cowshed.injected"])
                .current_dir(&root)
                .env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .envs(environment)
                .output()
                .expect("git config")
        };
        let control = run(injected
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect());
        assert!(
            control.status.success(),
            "negative control must inject a real Git setting"
        );
        let protected = run(built(&injected));
        assert_eq!(
            protected.status.code(),
            Some(1),
            "caller injection must not reach Git"
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    /// The gateway decodes the token to exactly 32 bytes before it authenticates a CONNECT, so
    /// "43 characters from the right alphabet" is not the same predicate. 43 unpadded base64url
    /// symbols carry 258 bits, and the two bits past 32 bytes must be zero; this fixture ends in
    /// `B`, whose low bits are not, so a strict decoder refuses it. The length-and-alphabet check
    /// this file used to carry accepted exactly this string, put it in `HTTP_PROXY`, and left the
    /// rejection to surface as a spurious network error inside the workspace.
    #[test]
    fn a_well_formed_looking_string_is_not_a_token_unless_it_decodes() {
        let non_canonical = "0123456789abcdefghijklmnopqrstuvwxyz-_ABCDB";
        assert_eq!(
            non_canonical.len(),
            WorkspaceToken::from_bytes([0; 32]).encode().len()
        );
        assert!(
            non_canonical
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        );
        assert!(
            WorkspaceToken::parse(non_canonical).is_err(),
            "a 43-character alphabet-valid string that is not 32 encoded bytes must be refused"
        );
    }
}

#[cfg(test)]
mod process_death_tests {
    use super::*;

    /// A `wait(2)` that fails tells us nothing about the child. Reporting it as a signal death
    /// publishes a terminal status the kernel never gave us, and finalizes the job while the
    /// child may still be running.
    #[test]
    fn a_failed_wait_is_not_a_terminal_exit_status() {
        let error = process_termination_from_wait(Err(io::Error::from_raw_os_error(libc::ECHILD)))
            .expect_err("a failed wait must stay an error, not a fabricated signal death");
        assert_eq!(error.code, crate::error::ErrorCode::Integrity);
    }

    /// `wait` succeeding is not the same as `wait` answering. A stopped child is neither exited
    /// nor signalled, which is the other way the old mapping reached a synthesized SIGKILL.
    #[test]
    fn a_wait_status_that_names_neither_exit_nor_signal_is_an_error() {
        // Classic `WIFSTOPPED` wait status: SIGTSTP in the high byte, 0x7f in the low byte.
        let stopped = std::process::ExitStatus::from_raw((libc::SIGTSTP << 8) | 0x7f);
        assert_eq!(ProcessStatus::from(stopped), ProcessStatus::Unknown);
        let error = process_termination_from_wait(Ok(stopped))
            .expect_err("an unreaped child has no terminal exit status");
        assert_eq!(error.code, crate::error::ErrorCode::Integrity);
    }

    #[test]
    fn a_clean_exit_and_a_signal_death_keep_their_status() {
        assert_eq!(
            process_termination_from_wait(Ok(std::process::ExitStatus::from_raw(0))).unwrap(),
            ExitStatus::Exited { code: 0 }
        );
        assert_eq!(
            process_termination_from_wait(Ok(std::process::ExitStatus::from_raw(libc::SIGKILL)))
                .unwrap(),
            ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false,
            }
        );
    }

    /// `kill(-pid)` is a process-group signal only for a strictly positive pid. `kill(-1, ...)`
    /// would signal every process the daemon may signal.
    #[test]
    fn a_pid_that_is_not_a_process_group_is_refused() {
        for pid in [0, 1] {
            assert!(
                kill_process_group(pid, 0).is_err(),
                "pid {pid} must not be negated into a process-group target"
            );
        }
    }
}
