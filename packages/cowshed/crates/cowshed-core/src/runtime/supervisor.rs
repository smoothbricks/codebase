use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::{fs::MetadataExt, process::ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::api::dto::{
    BinaryData, CommandArg, ExecRequest, ExitStatus, JobId, JobInfo, JobState, OutputLimitInfo,
    OutputPublication, OutputStorage, OutputSummary, ProtectedOutput, Sha256Digest, StdinInfo,
    StdinKind, StdinSource, StreamInfo, TraceContext, TraceId, UtcTimestamp, WorkspacePath,
    validate_command_argv,
};
use crate::error::{CowshedError, Result};
use crate::exec::{
    SandboxExecRequest, SpawnFailure, SpawnPlan, classify_spawn_error, plan_exec,
    prepare_child_descriptors,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::sandbox::{SandboxConfig, SandboxProfileRole, seatbelt_profile};
use crate::storage::audit::AuditSinkError;
use cowshed_gateway::WorkspaceToken;

use crate::storage::job_artifact::{
    ArtifactConfig, ArtifactError, ArtifactStore, CompletedJobArtifacts, OutputTargets,
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
        self.artifacts.validate().map_err(map_artifact_error)?;
        seatbelt_profile(&self.sandbox, SandboxProfileRole::TrustedSupervisor)
            .map_err(map_sandbox_error)?;
        seatbelt_profile(&self.sandbox, SandboxProfileRole::ExecutedChild)
            .map_err(map_sandbox_error)?;
        Ok(())
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
                git_worktree_repository: None,
            },
            artifacts: ArtifactConfig::default(),
            term_grace: Duration::from_secs(2),
            actor_capacity: DEFAULT_ACTOR_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
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

#[derive(Clone, Debug)]
pub struct ProcessSpawnRequest {
    pub authority: WorkspaceAuthoritySnapshot,
    pub job_id: JobId,
    pub argv: Vec<OsString>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub devenv_dir: Option<PathBuf>,
    pub sandbox: SandboxConfig,
    pub trusted_supervisor_profile: String,
    pub executed_child_profile: String,
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
}

pub trait RunningProcess: Send {
    fn pid(&self) -> u32;
    /// `Ok(false)` means the bounded process-input lane is full.
    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool>;
    fn close_stdin(&mut self) -> Result<()>;
    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()>;
}

#[async_trait]
pub trait SpawnSink: Send {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>>;

    async fn print_devenv_env(
        &mut self,
        devenv_dir: &Path,
        sandbox: &SandboxConfig,
    ) -> Result<CommandOutput> {
        let _ = sandbox;
        Err(CowshedError::internal(format!(
            "spawn sink cannot evaluate devenv at {}",
            devenv_dir.display()
        )))
    }
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
    fn admit(&mut self, job_id: JobId, grant_revision: u64, argv: &[CommandArg]) -> Result<()>;
    fn prepare_background(&mut self, job_id: JobId) -> Result<()>;
    fn write(&mut self, job_id: JobId, stream: StreamKind, bytes: &[u8]) -> Result<ArtifactWrite>;
    fn seal(
        &mut self,
        job_id: JobId,
        state: JobState,
        stdout_copy: Option<OutputPublication>,
        stderr_copy: Option<OutputPublication>,
    ) -> Result<ArtifactSeal>;
    fn checkpoint(&mut self) -> Result<CheckpointBarrier>;
}

pub use crate::process::{CommandOutput, ProcessStatus};
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

    fn admit(&mut self, job_id: JobId, grant_revision: u64, argv: &[CommandArg]) -> Result<()> {
        let token = self
            .store
            .begin_job(job_id, grant_revision, argv, OutputTargets::default())
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
        state: JobState,
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
            .finish_and_publish(token, state, stdout_copy, stderr_copy)
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
const DEVENV_SNAPSHOT_FILE: &str = ".devenv/cowshed-env.json";
const DEVENV_PRINT_ARGV: [&str; 3] = ["devenv", "print-dev-env", "--json"];
const DEVENV_INPUT_FILES: [&str; 4] = [
    "devenv.nix",
    "devenv.lock",
    "devenv.yaml",
    "devenv.local.nix",
];

#[derive(Clone, Debug)]
struct DevenvResolutionError {
    message: String,
    configured_dir: Option<PathBuf>,
}

impl DevenvResolutionError {
    fn into_cowshed_error(self) -> CowshedError {
        CowshedError::environment_missing(
            self.message,
            "repair .cowshed.toml or the configured devenv directory, then retry",
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DevenvEnvSnapshot {
    vars: BTreeMap<String, String>,
    /// The tracked inputs as they were when these vars were evaluated.
    ///
    /// One algorithm answers "are the devenv inputs still the snapshot's inputs?", at startup and
    /// at every dirty exec. The startup check used to be snapshot-mtime versus source-mtime while
    /// the runtime check used this fingerprint, and the two disagree by construction: an atomic
    /// replace changes inode and ctime without moving mtime forward, so each could call fresh
    /// what the other called stale. A snapshot that cannot say which inputs produced it is not a
    /// snapshot that can be reused, which is why this is required rather than defaulted.
    inputs: DevenvInputFingerprint,
}

/// Tracked devenv inputs, keyed by path relative to the workspace mount so a snapshot survives
/// the clone into a differently-named mount that copy-on-write forks produce.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DevenvInputFingerprint(Vec<(PathBuf, Option<DevenvFileFingerprint>)>);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DevenvFileFingerprint {
    device: u64,
    inode: u64,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Debug, Deserialize)]
struct PrintedDevenvEnvironment {
    variables: BTreeMap<String, PrintedDevenvVariable>,
}

#[derive(Debug, Deserialize)]
struct PrintedDevenvVariable {
    #[serde(rename = "type")]
    kind: String,
    /// devenv 2.2.x emits `{"type": "unknown"}` with NO value field for
    /// shell-special variables (BASHOPTS, BASHPID, ...). Only `exported`
    /// entries are consumed below, so a missing value defaults to Null and
    /// non-exported kinds skip before it is ever read.
    #[serde(default)]
    value: serde_json::Value,
}

/// One long-lived watcher belongs to each mounted-workspace supervisor.
///
/// Source mtimes are reconciled once when the supervisor starts, covering edits made while the
/// daemon was down. After that, filesystem events are the staleness signal: clean execs do no
/// source metadata work and never invoke devenv. A dirty exec fingerprints the tracked inputs so a
/// delayed pre-watch event can reuse the evaluated snapshot; a new revision refreshes it.
struct DevenvEnvironment {
    workspace_mount: PathBuf,
    dirty: Arc<AtomicBool>,
    tracked_paths: Arc<RwLock<BTreeSet<PathBuf>>>,
    evaluated_inputs: Option<DevenvInputFingerprint>,
    resolution: std::result::Result<Option<PathBuf>, DevenvResolutionError>,
    _watcher: RecommendedWatcher,
}

impl DevenvEnvironment {
    fn new(workspace_mount: &Path) -> Result<Self> {
        let resolution = resolve_devenv_dir(workspace_mount);
        let resolved = resolution.as_ref().ok().and_then(|value| value.as_deref());
        let tracked_paths = devenv_tracked_paths(workspace_mount, tracked_devenv_dir(&resolution));
        // Reconcile once against the snapshot's own recorded inputs, covering edits made while
        // the daemon was down. Identical comparison to the dirty-exec path below, so a snapshot
        // is never fresh by one rule and stale by the other.
        let evaluated_inputs = resolved
            .and_then(|dir| parse_devenv_snapshot(&fs::read(dir.join(DEVENV_SNAPSHOT_FILE)).ok()?))
            .map(|snapshot| snapshot.inputs)
            .filter(|persisted| {
                devenv_input_fingerprint(workspace_mount, &tracked_paths)
                    .is_ok_and(|current| current == *persisted)
            });
        let snapshot_is_stale = resolved.is_some() && evaluated_inputs.is_none();
        let tracked_paths = Arc::new(RwLock::new(tracked_paths));
        let dirty = Arc::new(AtomicBool::new(false));
        let callback_paths = Arc::clone(&tracked_paths);
        let callback_dirty = Arc::clone(&dirty);
        let mut watcher: RecommendedWatcher =
            notify::recommended_watcher(move |event: notify::Result<Event>| match event {
                Ok(event) => {
                    let Ok(paths) = callback_paths.read() else {
                        callback_dirty.store(true, Ordering::Release);
                        return;
                    };
                    if event_touches_devenv(&event, &paths) {
                        callback_dirty.store(true, Ordering::Release);
                    }
                }
                Err(_) => callback_dirty.store(true, Ordering::Release),
            })
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot create devenv watcher for {}: {error}",
                        workspace_mount.display()
                    ),
                    "reattach the workspace and retry",
                )
            })?;
        watcher
            .watch(workspace_mount, RecursiveMode::Recursive)
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot watch workspace {} for devenv changes: {error}",
                        workspace_mount.display()
                    ),
                    "reattach the workspace and retry",
                )
            })?;
        if snapshot_is_stale {
            dirty.store(true, Ordering::Release);
        }
        Ok(Self {
            workspace_mount: workspace_mount.to_owned(),
            dirty,
            tracked_paths,
            evaluated_inputs,
            resolution,
            _watcher: watcher,
        })
    }

    async fn environment_for_spawn(
        &mut self,
        spawner: &mut dyn SpawnSink,
        sandbox: &SandboxConfig,
        controller_env: BTreeMap<String, String>,
    ) -> Result<(Option<PathBuf>, BTreeMap<String, String>)> {
        let mut changed = self.dirty.swap(false, Ordering::AcqRel);
        if changed {
            self.resolve_again();
            if self
                .input_fingerprint()
                .is_some_and(|inputs| self.evaluated_inputs.as_ref() == Some(&inputs))
            {
                // FSEvents can deliver a write from before watcher registration after the
                // evaluated snapshot is already current. The fingerprint distinguishes that
                // delayed notification from a new source revision without losing the event.
                changed = false;
            }
        }
        let devenv_dir = self
            .resolution
            .clone()
            .map_err(DevenvResolutionError::into_cowshed_error)?;
        let Some(devenv_dir) = devenv_dir else {
            return Ok((None, controller_env));
        };

        if !changed && let Some(snapshot) = read_devenv_snapshot(&devenv_dir).await {
            return Ok((
                Some(devenv_dir),
                merge_devenv_environment(snapshot.vars, controller_env),
            ));
        }

        let inputs_before = self.input_fingerprint();
        match evaluate_devenv_environment(spawner, sandbox, &devenv_dir).await {
            Ok(vars) => {
                let inputs_after = self.input_fingerprint();
                self.evaluated_inputs = match (inputs_before, inputs_after) {
                    (Some(before), Some(after)) if before == after => Some(after),
                    _ => {
                        // A source changed while devenv was evaluating, so the next process
                        // must refresh again rather than treating this output as authoritative.
                        self.dirty.store(true, Ordering::Release);
                        None
                    }
                };
                // Persisted only when the inputs held still across the evaluation. Without a
                // fingerprint the snapshot cannot claim to describe any particular revision, so
                // the next startup re-evaluates instead of trusting it.
                if let Some(inputs) = self.evaluated_inputs.clone() {
                    write_devenv_snapshot(
                        &devenv_dir,
                        DevenvEnvSnapshot {
                            vars: vars.clone(),
                            inputs,
                        },
                    )
                    .await?;
                }
                Ok((
                    Some(devenv_dir),
                    merge_devenv_environment(vars, controller_env),
                ))
            }
            Err(error) => {
                // A failed refresh is never a clean state and a stale snapshot is never reused.
                self.dirty.store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    fn resolve_again(&mut self) {
        let resolution = resolve_devenv_dir(&self.workspace_mount);
        if let Ok(mut paths) = self.tracked_paths.write() {
            *paths = devenv_tracked_paths(&self.workspace_mount, tracked_devenv_dir(&resolution));
        } else {
            self.dirty.store(true, Ordering::Release);
        }
        self.resolution = resolution;
    }

    fn input_fingerprint(&self) -> Option<DevenvInputFingerprint> {
        let paths = self.tracked_paths.read().ok()?;
        devenv_input_fingerprint(&self.workspace_mount, &paths).ok()
    }
}

fn devenv_input_fingerprint(
    workspace_mount: &Path,
    paths: &BTreeSet<PathBuf>,
) -> io::Result<DevenvInputFingerprint> {
    let mut fingerprints = Vec::with_capacity(paths.len());
    for path in paths {
        let relative = path.strip_prefix(workspace_mount).map_err(|_| {
            io::Error::other(format!(
                "tracked devenv input {} is outside the workspace mount {}",
                path.display(),
                workspace_mount.display()
            ))
        })?;
        let fingerprint = match fs::metadata(path) {
            Ok(metadata) => Some(DevenvFileFingerprint {
                device: metadata.dev(),
                inode: metadata.ino(),
                size: metadata.size(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
                changed_seconds: metadata.ctime(),
                changed_nanoseconds: metadata.ctime_nsec(),
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        fingerprints.push((relative.to_path_buf(), fingerprint));
    }
    Ok(DevenvInputFingerprint(fingerprints))
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
                    configured_dir: None,
                }
            })?,
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(DevenvResolutionError {
                message: format!("cannot read {}: {error}", config_path.display()),
                configured_dir: None,
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
                configured_dir: Some(devenv_dir.clone()),
            });
        }
        return Ok(Some(devenv_dir));
    }

    let root_devenv_nix = workspace_mount.join("devenv.nix");
    Ok(root_devenv_nix
        .is_file()
        .then(|| workspace_mount.to_owned()))
}

fn tracked_devenv_dir(
    resolution: &std::result::Result<Option<PathBuf>, DevenvResolutionError>,
) -> Option<&Path> {
    match resolution {
        Ok(Some(devenv_dir)) => Some(devenv_dir),
        Err(error) => error.configured_dir.as_deref(),
        Ok(None) => None,
    }
}

fn devenv_tracked_paths(workspace_mount: &Path, devenv_dir: Option<&Path>) -> BTreeSet<PathBuf> {
    let devenv_dir = devenv_dir.unwrap_or(workspace_mount);
    std::iter::once(workspace_mount.join(COWSHED_CONFIG_FILE))
        .chain(
            DEVENV_INPUT_FILES
                .into_iter()
                .map(|file| devenv_dir.join(file)),
        )
        .collect()
}

fn event_touches_devenv(event: &Event, tracked_paths: &BTreeSet<PathBuf>) -> bool {
    !matches!(event.kind, notify::EventKind::Access(_))
        && event.paths.iter().any(|path| tracked_paths.contains(path))
}

/// One parser for the snapshot, so the synchronous startup reconciliation and the asynchronous
/// exec path cannot disagree about what a snapshot file means.
fn parse_devenv_snapshot(bytes: &[u8]) -> Option<DevenvEnvSnapshot> {
    serde_json::from_slice(bytes).ok()
}

async fn read_devenv_snapshot(devenv_dir: &Path) -> Option<DevenvEnvSnapshot> {
    let bytes = tokio::fs::read(devenv_dir.join(DEVENV_SNAPSHOT_FILE))
        .await
        .ok()?;
    parse_devenv_snapshot(&bytes)
}

async fn evaluate_devenv_environment(
    spawner: &mut dyn SpawnSink,
    sandbox: &SandboxConfig,
    devenv_dir: &Path,
) -> Result<BTreeMap<String, String>> {
    let output = spawner.print_devenv_env(devenv_dir, sandbox).await?;
    if !output.status.succeeded() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim();
        return Err(CowshedError::environment_missing(
            if detail.is_empty() {
                format!(
                    "devenv print-dev-env --json failed in {} with {}",
                    devenv_dir.display(),
                    output.status
                )
            } else {
                format!(
                    "devenv print-dev-env --json failed in {}: {detail}",
                    devenv_dir.display()
                )
            },
            format!(
                "run devenv print-dev-env --json in {} and repair the reported error",
                devenv_dir.display()
            ),
        ));
    }
    parse_printed_devenv_environment(&output.stdout, devenv_dir)
}

async fn write_devenv_snapshot(devenv_dir: &Path, snapshot: DevenvEnvSnapshot) -> Result<()> {
    let snapshot_path = devenv_dir.join(DEVENV_SNAPSHOT_FILE);
    let parent = snapshot_path
        .parent()
        .expect("devenv snapshot always has a parent");
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot create devenv snapshot directory {}: {error}",
                parent.display()
            ),
            "repair workspace permissions and retry",
        )
    })?;
    let write_path = snapshot_path.clone();
    tokio::task::spawn_blocking(move || crate::metadata::write_json(&write_path, &snapshot))
        .await
        .map_err(|error| {
            CowshedError::internal(format!(
                "devenv snapshot writer failed for {}: {error}",
                snapshot_path.display()
            ))
        })?
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot write devenv environment snapshot {}: {error}",
                    snapshot_path.display()
                ),
                "repair workspace permissions and retry",
            )
        })?;
    Ok(())
}

fn parse_printed_devenv_environment(
    stdout: &[u8],
    devenv_dir: &Path,
) -> Result<BTreeMap<String, String>> {
    let printed: PrintedDevenvEnvironment = serde_json::from_slice(stdout).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "devenv print-dev-env --json returned invalid JSON in {}: {error}",
                devenv_dir.display()
            ),
            "upgrade or repair devenv, then retry",
        )
    })?;
    let mut vars = BTreeMap::new();
    for (name, variable) in printed.variables {
        if variable.kind != "exported" {
            continue;
        }
        let value = variable.value.as_str().ok_or_else(|| {
            CowshedError::environment_missing(
                format!(
                    "devenv exported variable {name:?} has a non-string value in {}",
                    devenv_dir.display()
                ),
                "upgrade or repair devenv, then retry",
            )
        })?;
        vars.insert(name, value.to_owned());
    }
    Ok(vars)
}

fn merge_devenv_environment(
    mut snapshot: BTreeMap<String, String>,
    controller_env: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    // PATH is constructed from admitted roots below; an evaluated shell cannot bypass that policy.
    snapshot.remove("PATH");
    snapshot.extend(controller_env);
    snapshot
}

/// Resolve the evaluated profile produced inside a workspace.
///
/// Cowshed's cwd-based refresh writes state below the configured devenv directory, which is
/// preferred. A user may also have evaluated through devenv's native root binding, whose state
/// lives at the mount root, so that location is admitted as a presence-based fallback. Both paths
/// retain the immutable `/nix/store` canonicalization guard.
fn workspace_profile_bin(workspace_mount: &Path, devenv_dir: &Path) -> Option<PathBuf> {
    [devenv_dir, workspace_mount].into_iter().find_map(|root| {
        let resolved = fs::canonicalize(root.join(DEVENV_PROFILE_BIN)).ok()?;
        resolved.starts_with("/nix/store").then_some(resolved)
    })
}

fn sandbox_path(sandbox: &SandboxConfig, devenv_dir: Option<&Path>) -> Result<OsString> {
    let mut paths = vec![sandbox.workspace_mount.join(".cowshed/bin")];
    if let Some(profile) = devenv_dir
        .and_then(|devenv_dir| workspace_profile_bin(&sandbox.workspace_mount, devenv_dir))
    {
        paths.push(profile);
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
    if let Some(inherited) = std::env::var_os("PATH") {
        for path in std::env::split_paths(&inherited) {
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

/// Point the private HOME's `$CARGO_HOME` registry at the host's download cache.
///
/// `$CARGO_HOME` follows `HOME`, so exporting a private HOME hands cargo an empty registry and
/// every workspace refetches crates the host already has — over the gateway, one CONNECT at a
/// time. Linking is what makes the profile's read grant reachable: Seatbelt matches resolved
/// paths, so these links carry the host registry's authority, which is read-only.
///
/// `src` — where cargo unpacks an archive — is a real directory inside the mount, so a crate the
/// host downloaded but never built still unpacks, per workspace, and copy-on-write hands a warm
/// one to every clone. A host with no registry yet yields no links and an ordinary empty
/// `$CARGO_HOME`; an existing real directory is left alone, because it is a workspace's own
/// registry state and losing it is worse than not sharing.
async fn link_cargo_registry(private_home: &Path, host_home: &Path) -> Result<()> {
    let host_registry = crate::sandbox::host_cargo_registry(host_home);
    let registry = private_home.join(".cargo/registry");
    let unpacked = registry.join("src");
    tokio::fs::create_dir_all(&unpacked)
        .await
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot prepare sandbox cargo registry {}: {error}",
                    unpacked.display()
                ),
                "reattach the workspace and retry",
            )
        })?;
    for directory in crate::sandbox::SHARED_CARGO_REGISTRY_DIRECTORIES {
        let target = host_registry.join(directory);
        if !target.is_dir() {
            continue;
        }
        let link = registry.join(directory);
        match tokio::fs::read_link(&link).await {
            Ok(existing) if existing == target => continue,
            Ok(_) => tokio::fs::remove_file(&link).await.map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot replace stale sandbox cargo link {}: {error}",
                        link.display()
                    ),
                    "reattach the workspace and retry",
                )
            })?,
            Err(_) if link.exists() => continue,
            Err(_) => {}
        }
        tokio::fs::symlink(&target, &link).await.map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot link sandbox cargo cache {}: {error}",
                    link.display()
                ),
                "reattach the workspace and retry",
            )
        })?;
    }
    Ok(())
}

/// Build the sandboxed `Command` for a child of this workspace.
///
/// Every process the supervisor launches goes through here, including the `devenv print-dev-env`
/// evaluation that produces the environment the others consume. That is the point: the evaluation
/// reads workspace-controlled Nix, so running it as a bare host `Command` with the daemon's
/// inherited environment handed workspace content the one path to host credentials, `PATH`, and
/// agent env that no other child has. It is a child of the workspace and it is sandboxed like one.
///
/// Caller `env` is applied first so the policy variables below always win; `env_clear` means
/// nothing is inherited that is not named here.
async fn sandboxed_command(
    plan: &SpawnPlan,
    sandbox: &SandboxConfig,
    devenv_dir: Option<&Path>,
    env: &BTreeMap<String, String>,
) -> Result<tokio::process::Command> {
    let private_root = sandbox.workspace_mount.join(".cowshed");
    let private_home = private_root.join("home");
    let private_config = private_root.join("config");
    let private_cache = private_root.join("cache");
    for directory in [&private_home, &private_config, &private_cache] {
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot prepare sandbox environment directory {}: {error}",
                        directory.display()
                    ),
                    "reattach the workspace and retry",
                )
            })?;
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
    link_cargo_registry(&private_home, &sandbox.home).await?;
    let path = sandbox_path(sandbox, devenv_dir)?;
    let port_base = sandbox.port_block.base().to_string();
    let encoded_token = workspace_token.encode();
    let gateway_http = gateway_proxy_url(&port_base, &workspace_token);

    let mut command = tokio::process::Command::new(&plan.program);
    command
        .env_clear()
        .args(&plan.args)
        .current_dir(&plan.cwd)
        .envs(env)
        .env("PATH", path)
        .env("HOME", &private_home)
        .env("XDG_CONFIG_HOME", &private_config)
        .env("XDG_CACHE_HOME", &private_cache)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("TMPDIR", &sandbox.exec_temp_dir)
        .env("PWD", &plan.cwd)
        .env("GOENV", private_cache.join("go/env"))
        // rustc-wrapper clients speak to the host-owned sccache daemon; the
        // Seatbelt profile admits exactly this socket and denies binding it,
        // so a client whose daemon is down fails fast instead of spawning a
        // wrong-boundary server inside the sandbox.
        .env(
            "SCCACHE_SERVER_UDS",
            crate::sandbox::sccache_server_socket(),
        )
        .env("SCCACHE_DIR", crate::sandbox::sccache_cache_directory())
        .env("COWSHED_PORT_BASE", &port_base)
        .env("COWSHED_WORKSPACE_TOKEN", encoded_token)
        .env("HTTP_PROXY", &gateway_http)
        .env("HTTPS_PROXY", &gateway_http)
        .env("http_proxy", &gateway_http)
        .env("https_proxy", &gateway_http);
    for key in ["LANG", "LC_ALL", "LC_CTYPE", "TERM", "COLORTERM"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    // Rust routes through sccache in EVERY workspace. Cargo's `-C metadata` is
    // path-independent for workspace members (cargo >= 1.97, measured), and the bundled
    // sccache normalizes the residual path-bearing key inputs (cwd, blanket CARGO_* env,
    // argument bytes) against the request cwd when the client sets SCCACHE_BASEDIR_CWD=1 —
    // so name-mounted workspaces share entries with each other, not just successive slot
    // tenants. env-dep values stay unnormalized in the key, so a crate that compiles
    // env!("CARGO_MANIFEST_DIR") into its output still fail-closes across paths.
    // Incremental stays off: sccache refuses incremental compilations, and the shared
    // cache is worth more to a fleet of clones than per-unit local state.
    command
        .env("RUSTC_WRAPPER", "sccache")
        .env("CARGO_INCREMENTAL", "0")
        .env("SCCACHE_BASEDIR_CWD", "1");
    if let Some(directory) = developer_directory() {
        command.env("DEVELOPER_DIR", directory);
    }
    Ok(command)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemSpawnSink;

#[async_trait]
impl SpawnSink for SystemSpawnSink {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        let plan = plan_exec(
            SandboxExecRequest {
                argv: request.argv,
                cwd: request.cwd,
            },
            &request.sandbox,
        )
        .map_err(map_exec_error)?;
        if !plan.args.get(1).is_some_and(|profile| {
            profile.as_encoded_bytes() == request.executed_child_profile.as_bytes()
        }) {
            return Err(CowshedError::integrity(
                "executed-child Seatbelt profile changed between admission and spawn",
                "cowshed doctor --json",
            ));
        }
        if request.trusted_supervisor_profile
            != seatbelt_profile(&request.sandbox, SandboxProfileRole::TrustedSupervisor)
                .map_err(map_sandbox_error)?
        {
            return Err(CowshedError::integrity(
                "trusted-supervisor Seatbelt profile changed between admission and spawn",
                "cowshed doctor --json",
            ));
        }

        let mut command = sandboxed_command(
            &plan,
            &request.sandbox,
            request.devenv_dir.as_deref(),
            &request.env,
        )
        .await?;
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        prepare_child_descriptors(command.as_std_mut()).map_err(map_spawn_failure)?;
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
            .map_err(map_spawn_failure)?;
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
            stdin: stdin_sender,
            stdin_closed: false,
        }))
    }

    async fn print_devenv_env(
        &mut self,
        devenv_dir: &Path,
        sandbox: &SandboxConfig,
    ) -> Result<CommandOutput> {
        let plan = plan_exec(
            SandboxExecRequest {
                argv: DEVENV_PRINT_ARGV.map(OsString::from).to_vec(),
                cwd: devenv_dir.to_path_buf(),
            },
            sandbox,
        )
        .map_err(map_exec_error)?;
        // The evaluation writes `.devenv/` and talks to the nix daemon, both of which the
        // executed-child profile already admits: the daemon socket is a standing grant because
        // building inside a workspace is the point of a workspace.
        let output = sandboxed_command(&plan, sandbox, Some(devenv_dir), &BTreeMap::new())
            .await?
            .output()
            .await
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot run devenv print-dev-env --json in {}: {error}",
                        devenv_dir.display()
                    ),
                    "install devenv or repair the host PATH, then retry",
                )
            })?;
        Ok(CommandOutput::from(output))
    }
}

/// Translate a `wait(2)` result into the job's terminal exit status.
///
/// Every branch that cannot name how the child died returns `Err`. A `wait` that fails, or that
/// reports neither an exit code nor a terminating signal, means the child has not been reaped:
/// answering with a synthesized `SIGKILL` would let `finalize_job` seal the artifact and drain
/// the job's waiters with a successful terminal status while the process is still running.
fn process_termination_from_wait(
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
fn kill_process_group(pid: u32, signal: i32) -> Result<()> {
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

enum SystemStdin {
    Write(Bytes),
    Close,
}

struct SystemRunningProcess {
    pid: u32,
    stdin: mpsc::Sender<SystemStdin>,
    stdin_closed: bool,
}

impl RunningProcess for SystemRunningProcess {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        if self.stdin_closed {
            return Err(CowshedError::conflict(
                "job stdin is closed",
                "attach before closing stdin",
            ));
        }
        match self.stdin.try_send(SystemStdin::Write(bytes)) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(CowshedError::conflict(
                "job stdin is no longer available",
                "inspect the terminal job status",
            )),
        }
    }

    fn close_stdin(&mut self) -> Result<()> {
        if self.stdin_closed {
            return Ok(());
        }
        match self.stdin.try_send(SystemStdin::Close) {
            Ok(()) => {
                self.stdin_closed = true;
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(CowshedError::conflict(
                "job stdin still has a pending write",
                "retry stdin close after the pending write is accepted",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.stdin_closed = true;
                Ok(())
            }
        }
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
}

async fn run_system_stdin(
    job_id: JobId,
    mut stdin: tokio::process::ChildStdin,
    mut receiver: mpsc::Receiver<SystemStdin>,
    events: mpsc::Sender<ProcessEvent>,
) {
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

async fn run_system_output<R>(
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
    pub fn start(
        config: WorkspaceSupervisorConfig,
        commitments: CommitmentPublisherHandle,
    ) -> Result<WorkspaceSupervisorHandle> {
        config.validate()?;
        let artifacts = ArtifactStoreSink::open(
            config.workspace_root.clone(),
            &config.owned_repo_ids,
            &config.authority,
            config.artifacts.clone(),
        )?;
        Self::start_with_sinks(
            config,
            Box::new(SystemSpawnSink),
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
        let devenv = DevenvEnvironment::new(&config.workspace_root)?;
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
            sandbox: config.sandbox,
            devenv,
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
        };
        tokio::spawn(actor.run());
        Ok(handle)
    }
}

enum Command {
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
        !matches!(self.info.state, JobState::Queued | JobState::Running)
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
    workspace_root: PathBuf,
    default_cwd: Option<WorkspacePath>,
    sandbox: SandboxConfig,
    devenv: DevenvEnvironment,
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
                self.admit_exec(authority, session, request, background, reply)
                    .await;
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
        seatbelt_profile(&sandbox, SandboxProfileRole::TrustedSupervisor)
            .map_err(map_sandbox_error)?;
        seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).map_err(map_sandbox_error)?;
        self.authority = authority;
        self.sandbox = sandbox;
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

    async fn admit_exec(
        &mut self,
        authority: WorkspaceAuthoritySnapshot,
        session: Option<SessionToken>,
        request: ExecRequest,
        background: bool,
        reply: oneshot::Sender<Result<JobId>>,
    ) {
        let result = self
            .validate_authority(&authority)
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
            });
        if let Err(error) = result {
            let _ = reply.send(Err(error));
            return;
        }

        let ExecRequest {
            argv,
            cwd,
            mode: _,
            env,
            trace,
            stdin,
            stdout_copy,
            stderr_copy,
        } = request;
        if let Err(error) = validate_command_argv(&argv) {
            let _ = reply.send(Err(CowshedError::usage(
                error.to_string(),
                "provide a valid bounded command argv",
            )));
            return;
        }
        let info_argv = argv.clone();
        let argv_os = request_argv_to_os(argv);
        let (cwd, merged_env, session_identity) = match session.as_ref() {
            Some(token) => {
                let state = self
                    .sessions
                    .get_mut(&token.identity)
                    .expect("validated session exists");
                if let Some(cwd) = cwd {
                    state.cwd = Some(cwd);
                }
                state.env.extend(env);
                (state.cwd.clone(), state.env.clone(), Some(state.identity))
            }
            None => (
                cwd.or_else(|| self.default_cwd.clone()),
                env.into_iter().collect(),
                None,
            ),
        };
        let (devenv_dir, merged_env) = match self
            .devenv
            .environment_for_spawn(&mut *self.spawner, &self.sandbox, merged_env)
            .await
        {
            Ok(environment) => environment,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let job_id = self.next_job_id;
        let expected_next = match job_id
            .get()
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("job id allocation exhausted"))
            .and_then(|value| {
                JobId::new(value).map_err(|error| CowshedError::internal(error.to_string()))
            }) {
            Ok(next) => next,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        if let Err(error) = self
            .artifacts
            .admit(job_id, self.authority.grant_revision, &info_argv)
        {
            let _ = reply.send(Err(error));
            return;
        }
        self.next_job_id = expected_next;
        let admission = self
            .commitments
            .record(CommitmentDraft::Admission {
                repo_id: self.authority.repo_id.clone(),
                workspace_incarnation: self.authority.workspace_incarnation.clone(),
                job_id,
                grant_revision: self.authority.grant_revision,
            })
            .await;
        if let Err(error) = admission {
            let _ = self
                .artifacts
                .seal(job_id, JobState::Failed, stdout_copy, stderr_copy);
            let _ = reply.send(Err(error));
            return;
        }
        if background && let Err(error) = self.artifacts.prepare_background(job_id) {
            let _ = self
                .artifacts
                .seal(job_id, JobState::Failed, stdout_copy, stderr_copy);
            let _ = reply.send(Err(error));
            return;
        }

        let stdin_info = stdin_info(&stdin);
        // A clock fault is not an invariant, and `started` orders job records and commitments.
        // Stamping the job at the epoch would make every consumer that sorts by it lie, so
        // admission fails here exactly as it does for a seatbelt-profile failure below.
        let started = match utc_now() {
            Ok(started) => started,
            Err(error) => {
                let _ = self
                    .artifacts
                    .seal(job_id, JobState::Failed, stdout_copy, stderr_copy);
                let _ = reply.send(Err(error));
                return;
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
            argv: info_argv,
            cwd: cwd.clone(),
            started,
            duration_ms: None,
            exit: None,
            stdout: empty_stream(),
            stderr: empty_stream(),
            trace,
            output_limit: None,
            stdin: stdin_info,
        };
        let trusted_supervisor_profile =
            match seatbelt_profile(&self.sandbox, SandboxProfileRole::TrustedSupervisor)
                .map_err(map_sandbox_error)
            {
                Ok(profile) => profile,
                Err(error) => {
                    let _ = self
                        .artifacts
                        .seal(job_id, JobState::Failed, stdout_copy, stderr_copy);
                    let _ = reply.send(Err(error));
                    return;
                }
            };
        let executed_child_profile =
            match seatbelt_profile(&self.sandbox, SandboxProfileRole::ExecutedChild)
                .map_err(map_sandbox_error)
            {
                Ok(profile) => profile,
                Err(error) => {
                    let _ = self
                        .artifacts
                        .seal(job_id, JobState::Failed, stdout_copy, stderr_copy);
                    let _ = reply.send(Err(error));
                    return;
                }
            };
        let spawn = self
            .spawner
            .spawn(
                ProcessSpawnRequest {
                    authority: self.authority.clone(),
                    job_id,
                    argv: argv_os,
                    cwd: cwd
                        .as_ref()
                        .map(WorkspacePath::as_path)
                        .map(Path::to_path_buf)
                        .unwrap_or_default(),
                    env: merged_env,
                    devenv_dir,
                    sandbox: self.sandbox.clone(),
                    trusted_supervisor_profile,
                    executed_child_profile,
                },
                self.events.clone(),
            )
            .await;
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
                job.info.pid = Some(process.pid());
                job.process = Some(process);
                if background
                    && let Some(identity) = session_identity
                    && let Some(session) = self.sessions.get_mut(&identity)
                {
                    session.background_jobs.insert(job_id);
                }
                self.jobs.insert(job_id, job);
                launch_stdin_pump(
                    job_id,
                    stdin,
                    self.workspace_root.clone(),
                    self.events.clone(),
                );
                let _ = reply.send(Ok(job_id));
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
                let _ = reply.send(Err(error));
            }
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
            let _ = reply.send(Err(not_found_job(job_id)));
            return;
        };
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
                | KillReason::WaitFailure,
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
        let seal = match self.artifacts.seal(
            job_id,
            state,
            job.stdout_copy.take(),
            job.stderr_copy.take(),
        ) {
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
        let commitment = self
            .commitments
            .record(CommitmentDraft::Terminal {
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
            })
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
        job.info.duration_ms = Some(
            job.started_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        job.info.exit = job.exit.clone();
        job.info.stdout = seal.stdout;
        job.info.stderr = seal.stderr;
        job.info.output_limit = seal.output_limit;
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

fn request_argv_to_os(argv: Vec<CommandArg>) -> Vec<OsString> {
    argv.into_iter().map(CommandArg::into_os_string).collect()
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
fn utc_now() -> Result<UtcTimestamp> {
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

fn map_spawn_failure(failure: SpawnFailure) -> CowshedError {
    CowshedError::environment_missing(
        format!(
            "sandbox wrapper failed during {:?}: {}",
            failure.stage, failure.source
        ),
        "verify the macOS sandbox execution environment",
    )
}

fn map_exec_error(error: crate::exec::ExecError) -> CowshedError {
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

fn map_artifact_error(error: ArtifactError) -> CowshedError {
    CowshedError::integrity(error.to_string(), "cowshed doctor --json")
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

fn retiring_error() -> CowshedError {
    CowshedError::conflict(
        "workspace supervisor is quiescing or retired",
        "reattach an active workspace before starting work",
    )
}

#[cfg(test)]
mod workspace_toolchain_tests {
    use super::*;
    use crate::sandbox::{
        RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole, nix_daemon_socket,
        seatbelt_profile,
    };

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
            git_worktree_repository: None,
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

    struct NoSpawn;

    #[async_trait]
    impl SpawnSink for NoSpawn {
        async fn spawn(
            &mut self,
            _request: ProcessSpawnRequest,
            _events: mpsc::Sender<ProcessEvent>,
        ) -> Result<Box<dyn RunningProcess>> {
            panic!("spawn is not used by devenv environment tests")
        }
    }

    /// `print-dev-env` reads workspace-controlled Nix, so it is the one child that must not be
    /// a bare host `Command`. This pins the argv through the same `plan_exec` every job uses: the
    /// program is the Seatbelt wrapper, the profile is the executed-child role, and the cwd is
    /// contained in the workspace mount. Reverting to `Command::new("devenv")` fails it.
    #[test]
    fn the_devenv_evaluation_is_planned_through_seatbelt_like_any_other_child() {
        let root = scratch("devenv-sandboxed");
        let mount = root.join("workspace");
        let devenv_dir = mount.join("tooling/devenv");
        std::fs::create_dir_all(&devenv_dir).expect("devenv dir");
        let sandbox = sandbox_at(&mount);

        let plan = plan_exec(
            SandboxExecRequest {
                argv: DEVENV_PRINT_ARGV.map(OsString::from).to_vec(),
                cwd: devenv_dir.clone(),
            },
            &sandbox,
        )
        .expect("the devenv argv is a plannable sandboxed exec");

        assert_eq!(plan.program, Path::new(crate::exec::SANDBOX_EXEC));
        assert_eq!(
            plan.args.first().map(OsString::as_os_str),
            Some("-p".as_ref())
        );
        assert_eq!(
            plan.args.get(1).and_then(|profile| profile.to_str()),
            seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild)
                .expect("executed-child profile")
                .as_str()
                .into()
        );
        assert!(plan.cwd.starts_with(&mount));
        assert_eq!(
            plan.args.last().and_then(|argument| argument.to_str()),
            Some("--json")
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
        assert!(error.message.contains("tooling/devenv"));
        assert!(error.message.contains("devenv.nix"));

        std::fs::remove_dir_all(&root).ok();
    }

    fn persist_snapshot(
        devenv_dir: &Path,
        vars: BTreeMap<String, String>,
        inputs: &DevenvInputFingerprint,
    ) {
        let path = devenv_dir.join(DEVENV_SNAPSHOT_FILE);
        std::fs::create_dir_all(path.parent().unwrap()).expect("snapshot dir");
        let snapshot = DevenvEnvSnapshot {
            vars,
            inputs: inputs.clone(),
        };
        std::fs::write(path, serde_json::to_vec(&snapshot).unwrap()).expect("snapshot");
    }

    /// Startup reconciliation is the fingerprint comparison, not an mtime race. `.cowshed.toml`
    /// is a tracked input and a missing optional input is a recorded absence, so neither needs a
    /// sleep to observe: changing the config changes the fingerprint by content, and the absent
    /// `devenv.lock`/`devenv.yaml`/`devenv.local.nix` stay absent.
    #[test]
    fn startup_staleness_includes_config_and_ignores_missing_optional_inputs() {
        let root = scratch("staleness");
        let mount = root.join("workspace");
        let devenv_root = mount.join("tooling/devenv");
        std::fs::create_dir_all(&devenv_root).expect("devenv dir");
        std::fs::write(devenv_root.join("devenv.nix"), "{}").expect("devenv.nix");
        std::fs::write(
            mount.join(COWSHED_CONFIG_FILE),
            "[devenv]\ndir = \"tooling/devenv\"\n",
        )
        .expect("config");

        let tracked = devenv_tracked_paths(&mount, Some(&devenv_root));
        assert_eq!(tracked.len(), DEVENV_INPUT_FILES.len() + 1);
        let evaluated = devenv_input_fingerprint(&mount, &tracked).expect("fingerprint");
        persist_snapshot(&devenv_root, BTreeMap::new(), &evaluated);
        assert_eq!(
            DevenvEnvironment::new(&mount)
                .expect("watcher")
                .evaluated_inputs,
            Some(evaluated.clone()),
            "a snapshot whose recorded inputs still hold is reusable at startup"
        );

        std::fs::write(
            mount.join(COWSHED_CONFIG_FILE),
            "[devenv]\ndir = \"tooling/devenv\"\n# changed\n",
        )
        .expect("changed config");
        assert_ne!(
            devenv_input_fingerprint(&mount, &tracked).expect("fingerprint"),
            evaluated
        );
        assert_eq!(
            DevenvEnvironment::new(&mount)
                .expect("watcher")
                .evaluated_inputs,
            None,
            "a changed .cowshed.toml invalidates the snapshot at startup"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// A snapshot with no recorded inputs cannot claim to describe any revision of the sources,
    /// so it is not reusable. This is the case the old mtime rule got wrong in the dangerous
    /// direction: it called such a file fresh whenever it happened to be the newest.
    #[test]
    fn a_snapshot_without_recorded_inputs_is_not_reusable() {
        let root = scratch("snapshot-no-inputs");
        let mount = root.join("workspace");
        let snapshot = mount.join(DEVENV_SNAPSHOT_FILE);
        std::fs::create_dir_all(snapshot.parent().unwrap()).expect("snapshot dir");
        std::fs::write(mount.join("devenv.nix"), "{}").expect("devenv.nix");
        std::fs::write(&snapshot, "{\"vars\":{}}\n").expect("snapshot");

        assert_eq!(parse_devenv_snapshot(b"{\"vars\":{}}\n"), None);
        assert_eq!(
            DevenvEnvironment::new(&mount)
                .expect("watcher")
                .evaluated_inputs,
            None
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn snapshot_is_base_path_is_dropped_and_controller_values_win() {
        let snapshot = BTreeMap::from([
            ("PATH".to_owned(), "/untrusted/bin".to_owned()),
            ("SAME".to_owned(), "snapshot".to_owned()),
            ("SNAPSHOT_ONLY".to_owned(), "yes".to_owned()),
        ]);
        let controller = BTreeMap::from([
            ("SAME".to_owned(), "controller".to_owned()),
            ("GOENV".to_owned(), "/workspace/goenv".to_owned()),
        ]);

        assert_eq!(
            merge_devenv_environment(snapshot, controller),
            BTreeMap::from([
                ("GOENV".to_owned(), "/workspace/goenv".to_owned()),
                ("SAME".to_owned(), "controller".to_owned()),
                ("SNAPSHOT_ONLY".to_owned(), "yes".to_owned()),
            ])
        );
    }

    #[test]
    fn watcher_ignores_read_events_for_devenv_inputs() {
        let path = PathBuf::from("/workspace/devenv.nix");
        let tracked_paths = BTreeSet::from([path.clone()]);
        let event =
            Event::new(notify::EventKind::Access(notify::event::AccessKind::Read)).add_path(path);

        assert!(!event_touches_devenv(&event, &tracked_paths));
    }

    #[test]
    fn watcher_marks_the_workspace_dirty_after_a_devenv_input_changes() {
        let root = scratch("watcher");
        let mount = root.join("workspace");
        std::fs::create_dir_all(&mount).expect("mount");
        std::fs::write(mount.join("devenv.nix"), "{}").expect("devenv.nix");
        let tracked = devenv_tracked_paths(&mount, Some(&mount));
        let evaluated = devenv_input_fingerprint(&mount, &tracked).expect("fingerprint");
        persist_snapshot(&mount, BTreeMap::new(), &evaluated);
        let environment = DevenvEnvironment::new(&mount).expect("watcher");
        assert!(!environment.dirty.load(Ordering::Acquire));

        std::fs::write(mount.join("devenv.nix"), "{ pkgs, ... }: {}").expect("change input");
        for _ in 0..200 {
            if environment.dirty.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            environment.dirty.load(Ordering::Acquire),
            "a relevant filesystem event must dirty the next sandbox process"
        );

        drop(environment);
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn delayed_watcher_event_does_not_refresh_unchanged_devenv_inputs() {
        let root = scratch("watcher-delayed");
        let mount = root.join("workspace");
        std::fs::create_dir_all(&mount).expect("mount");
        std::fs::write(mount.join("devenv.nix"), "{}").expect("devenv.nix");
        let tracked = devenv_tracked_paths(&mount, Some(&mount));
        let evaluated = devenv_input_fingerprint(&mount, &tracked).expect("fingerprint");
        persist_snapshot(
            &mount,
            BTreeMap::from([("FROM_SNAPSHOT".to_owned(), "yes".to_owned())]),
            &evaluated,
        );
        let mut environment = DevenvEnvironment::new(&mount).expect("watcher");

        // macOS FSEvents can deliver the source write that preceded watcher registration late.
        environment.dirty.store(true, Ordering::Release);
        let (_, variables) = environment
            .environment_for_spawn(
                &mut NoSpawn,
                &WorkspaceSupervisorConfig::default().sandbox,
                BTreeMap::new(),
            )
            .await
            .expect("unchanged inputs reuse the evaluated snapshot");

        assert_eq!(
            variables.get("FROM_SNAPSHOT").map(String::as_str),
            Some("yes")
        );

        drop(environment);
        std::fs::remove_dir_all(&root).ok();
    }

    /// The end of the mechanism, exercised for real: a workspace whose `devenv` evaluation
    /// materialized a store profile gets that profile's tools on `PATH`, ahead of the inherited
    /// roots, and can actually execute them inside its own Seatbelt sandbox.
    #[cfg_attr(not(target_os = "macos"), ignore)]
    #[test]
    fn an_evaluated_workspace_profile_leads_path_and_runs_inside_the_sandbox() {
        // Any multi-user Nix host has this profile, and it resolves into the store exactly as a
        // devenv profile does, so it stands in for one without pinning a generated store path.
        let Ok(store_profile) = std::fs::canonicalize("/nix/var/nix/profiles/default") else {
            return;
        };
        if !store_profile.starts_with("/nix/store") {
            return;
        }
        let Some(tool) = std::fs::read_dir(store_profile.join("bin"))
            .ok()
            .and_then(|mut entries| entries.find_map(|entry| entry.ok()))
            .map(|entry| entry.path())
        else {
            return;
        };

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

        let path = sandbox_path(&config, Some(&devenv_root)).expect("sandbox PATH");
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
        assert!(
            status.success(),
            "a tool on the workspace profile must be executable inside the sandbox"
        );
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
    async fn lineage_history_opens_and_restarts_a_replacement_supervisor() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-restored-supervisor-{}",
            Uuid::new_v4().simple()
        ));
        let telemetry = root.join("telemetry");
        let workspace_root = root.join("workspace");
        let unintroduced_root = root.join("unintroduced");
        std::fs::create_dir_all(&workspace_root).unwrap();
        std::fs::create_dir_all(&unintroduced_root).unwrap();
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
                &["true".into()],
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
                &["true".into()],
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
        };
        let first_supervisor =
            WorkspaceSupervisor::start(config.clone(), publisher.clone()).unwrap();
        first_supervisor.list().await.unwrap();
        drop(first_supervisor);
        tokio::task::yield_now().await;
        let restarted = WorkspaceSupervisor::start(config, publisher.clone()).unwrap();
        restarted.list().await.unwrap();
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
                &["true".into()],
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
        root
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

    #[tokio::test]
    async fn cargo_registry_links_the_host_download_cache_and_keeps_unpacking_local() {
        let root = scratch("cargo-registry");
        let host_home = root.join("host");
        let private_home = root.join("mount/.cowshed/home");
        let host_registry = crate::sandbox::host_cargo_registry(&host_home);
        for directory in ["index", "cache", "src"] {
            std::fs::create_dir_all(host_registry.join(directory)).unwrap();
        }

        link_cargo_registry(&private_home, &host_home)
            .await
            .unwrap();

        let registry = private_home.join(".cargo/registry");
        for directory in crate::sandbox::SHARED_CARGO_REGISTRY_DIRECTORIES {
            assert_eq!(
                std::fs::read_link(registry.join(directory)).unwrap(),
                host_registry.join(directory)
            );
        }
        // Unpacking must stay writable inside the mount, so `src` is a real directory and never a
        // link onto the read-only host tree.
        let unpacked = registry.join("src");
        assert!(unpacked.is_dir());
        assert!(std::fs::read_link(&unpacked).is_err());
        std::fs::write(unpacked.join("witness"), b"unpacked").unwrap();

        // Idempotent across execs, and a stale link from an earlier host layout is replaced.
        let stale = registry.join("index");
        std::fs::remove_file(&stale).unwrap();
        std::os::unix::fs::symlink(root.join("elsewhere"), &stale).unwrap();
        link_cargo_registry(&private_home, &host_home)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_link(&stale).unwrap(),
            host_registry.join("index")
        );
        assert_eq!(
            std::fs::read(unpacked.join("witness")).unwrap(),
            b"unpacked"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cargo_registry_yields_an_ordinary_home_without_a_host_cache() {
        let root = scratch("cargo-registry-absent");
        let host_home = root.join("host");
        let private_home = root.join("mount/.cowshed/home");
        std::fs::create_dir_all(&host_home).unwrap();

        link_cargo_registry(&private_home, &host_home)
            .await
            .unwrap();

        let registry = private_home.join(".cargo/registry");
        assert!(registry.join("src").is_dir());
        for directory in crate::sandbox::SHARED_CARGO_REGISTRY_DIRECTORIES {
            assert!(!registry.join(directory).exists());
        }

        // A workspace that built its own registry before the host had one keeps it: losing a real
        // directory of registry state is worse than not sharing the host's.
        let owned = registry.join("index");
        std::fs::create_dir_all(owned.join("index.crates.io-0000000000000000")).unwrap();
        std::fs::create_dir_all(crate::sandbox::host_cargo_registry(&host_home).join("index"))
            .unwrap();
        link_cargo_registry(&private_home, &host_home)
            .await
            .unwrap();
        assert!(std::fs::read_link(&owned).is_err());
        assert!(owned.join("index.crates.io-0000000000000000").is_dir());

        std::fs::remove_dir_all(root).unwrap();
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
