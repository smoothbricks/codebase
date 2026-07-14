use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::io;
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
    AdmissionCommitment, BinaryData, CONTROLLER_COMMITMENT_VERSION, CheckpointCommitment,
    CommandArg, ControllerCommitment, ExecRequest, ExitStatus, JobId, JobInfo, JobState,
    OutputLimitInfo, OutputPublication, OutputStorage, OutputSummary, ProtectedOutput,
    Sha256Digest, StdinInfo, StdinKind, StdinSource, StreamInfo, TraceContext, TraceId,
    UtcTimestamp, WorkspaceIntroducedCommitment, WorkspacePath, WorkspaceRetiredCommitment,
    validate_command_argv,
};
use crate::error::{CowshedError, Result};
use crate::exec::{
    SandboxExecRequest, SpawnFailure, classify_spawn_error, plan_exec, prepare_child_descriptors,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::repository::RepoId;
use crate::sandbox::{SandboxConfig, SandboxProfileRole, seatbelt_profile};
use crate::storage::commitment_store::{CommitmentStore, CommitmentStoreError};
use crate::storage::job_artifact::{
    ArtifactConfig, ArtifactError, ArtifactStore, CompletedJobArtifacts, OutputTargets,
    SealedCheckpointManifest, StreamKind,
};

const DEFAULT_ACTOR_CAPACITY: usize = 64;
const DEFAULT_EVENT_CAPACITY: usize = 64;
const PROCESS_IO_CHUNK: usize = 64 * 1024;
const MAX_LOG_READ: usize = 64 * 1024;
const MAX_PENDING_STDIN_BYTES: usize = 256 * 1024;
const MAX_COMMITMENT_CONFLICT_RETRIES: usize = 8;

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
            workspace_root: workspace_root.clone(),
            default_cwd: Some(WorkspacePath::new("work").expect("static cwd")),
            sandbox: SandboxConfig {
                home: PathBuf::from("/tmp/cowshed-home"),
                workspace_mount: workspace_root,
                exec_temp_dir: PathBuf::from("/tmp/cowshed-exec"),
                port_block: crate::metadata::PortBlock::new(49_152, 16).expect("static port block"),
                mode: crate::sandbox::RunSandboxMode::ReadWrite,
                grants: crate::sandbox::SandboxGrants::default(),
                allowed_unix_sockets: Vec::new(),
                additional_denies: Vec::new(),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputStream {
    Stdout,
    Stderr,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessExit {
    Exited(i32),
    Signaled { signal: i32, core_dumped: bool },
}

#[derive(Clone, Debug)]
pub struct ProcessSpawnRequest {
    pub authority: WorkspaceAuthoritySnapshot,
    pub job_id: JobId,
    pub argv: Vec<OsString>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub sandbox: SandboxConfig,
    pub trusted_supervisor_profile: String,
    pub executed_child_profile: String,
}

#[derive(Debug)]
pub enum ProcessEvent {
    Output {
        job_id: JobId,
        stream: OutputStream,
        bytes: Bytes,
    },
    OutputEof {
        job_id: JobId,
        stream: OutputStream,
    },
    Exited {
        job_id: JobId,
        exit: ProcessExit,
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
    fn write(&mut self, job_id: JobId, stream: OutputStream, bytes: &[u8])
    -> Result<ArtifactWrite>;
    fn seal(
        &mut self,
        job_id: JobId,
        state: JobState,
        stdout_copy: Option<OutputPublication>,
        stderr_copy: Option<OutputPublication>,
    ) -> Result<ArtifactSeal>;
    fn checkpoint(&mut self, barrier_id: u64) -> Result<CheckpointBarrier>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitmentDraft {
    WorkspaceIntroduced {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
    },
    WorkspaceRetired {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
    },
    Admission {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
        job_id: JobId,
        grant_revision: u64,
    },
    Terminal {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
        job_id: JobId,
        state: JobState,
        grant_revision: u64,
        stdout_bytes: u64,
        stdout_sha256: Sha256Digest,
        stderr_bytes: u64,
        stderr_sha256: Sha256Digest,
        batch_sha256: Sha256Digest,
        output_limit: Option<OutputLimitInfo>,
    },
    Checkpoint {
        repo_id: RepoId,
        origin_incarnation: WorkspaceIncarnation,
        checkpoint_id: String,
        barrier_id: u64,
        manifest_batch_sha256: Sha256Digest,
    },
    Fork {
        repo_id: RepoId,
        source_incarnation: WorkspaceIncarnation,
        destination_incarnation: WorkspaceIncarnation,
    },
    Restore {
        repo_id: RepoId,
        source_checkpoint: String,
        source_incarnation: WorkspaceIncarnation,
        destination_incarnation: WorkspaceIncarnation,
    },
}

impl CommitmentDraft {
    pub fn into_commitment(self, order: u64) -> ControllerCommitment {
        match self {
            Self::WorkspaceIntroduced {
                repo_id,
                workspace_incarnation,
            } => ControllerCommitment::WorkspaceIntroduced(WorkspaceIntroducedCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
            }),
            Self::WorkspaceRetired {
                repo_id,
                workspace_incarnation,
            } => ControllerCommitment::WorkspaceRetired(WorkspaceRetiredCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
            }),
            Self::Admission {
                repo_id,
                workspace_incarnation,
                job_id,
                grant_revision,
            } => ControllerCommitment::Admission(AdmissionCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
                job_id,
                grant_revision,
            }),
            Self::Terminal {
                repo_id,
                workspace_incarnation,
                job_id,
                state,
                grant_revision,
                stdout_bytes,
                stdout_sha256,
                stderr_bytes,
                stderr_sha256,
                batch_sha256,
                output_limit,
            } => ControllerCommitment::Terminal(crate::api::dto::TerminalCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
                job_id,
                state,
                grant_revision,
                stdout_bytes,
                stdout_sha256,
                stderr_bytes,
                stderr_sha256,
                batch_sha256,
                output_limit,
            }),
            Self::Checkpoint {
                repo_id,
                origin_incarnation,
                checkpoint_id,
                barrier_id,
                manifest_batch_sha256,
            } => ControllerCommitment::Checkpoint(CheckpointCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                origin_incarnation,
                checkpoint_id,
                barrier_id,
                manifest_batch_sha256,
            }),
            Self::Fork {
                repo_id,
                source_incarnation,
                destination_incarnation,
            } => ControllerCommitment::Fork(crate::api::dto::ForkCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                source_incarnation,
                destination_incarnation,
            }),
            Self::Restore {
                repo_id,
                source_checkpoint,
                source_incarnation,
                destination_incarnation,
            } => ControllerCommitment::Restore(crate::api::dto::RestoreCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                source_checkpoint,
                source_incarnation,
                destination_incarnation,
            }),
        }
    }
}

#[async_trait]
pub trait CommitmentSink: Send {
    /// Atomically allocates the repo-wide order and durably publishes the draft.
    async fn publish(&mut self, draft: CommitmentDraft) -> Result<u64>;
}

/// Production artifact adapter. One supervisor actor owns the store and every token.
pub struct ArtifactStoreSink {
    store: ArtifactStore,
    tokens: BTreeMap<JobId, crate::storage::job_artifact::JobArtifactToken>,
}

impl ArtifactStoreSink {
    pub fn open(
        workspace_root: impl Into<PathBuf>,
        authority: &WorkspaceAuthoritySnapshot,
        config: ArtifactConfig,
    ) -> Result<Self> {
        let store = ArtifactStore::open(
            workspace_root,
            authority.repo_id.clone(),
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

    fn write(
        &mut self,
        job_id: JobId,
        stream: OutputStream,
        bytes: &[u8],
    ) -> Result<ArtifactWrite> {
        let (store, tokens) = (&mut self.store, &self.tokens);
        let token = tokens
            .get(&job_id)
            .ok_or_else(|| missing_artifact_token(job_id))?;
        let stream = match stream {
            OutputStream::Stdout => StreamKind::Stdout,
            OutputStream::Stderr => StreamKind::Stderr,
        };
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

    fn checkpoint(&mut self, barrier_id: u64) -> Result<CheckpointBarrier> {
        let SealedCheckpointManifest {
            record,
            manifest_batch_sha256,
        } = self
            .store
            .checkpoint(barrier_id)
            .map_err(map_artifact_error)?;
        Ok(CheckpointBarrier {
            checkpoint_id: String::new(),
            barrier_id: record.barrier_id,
            manifest_batch_sha256,
        })
    }
}

enum CommitmentRequest {
    Publish {
        draft: CommitmentDraft,
        reply: oneshot::Sender<Result<u64>>,
    },
    EnsureWorkspaceIntroduced {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
        reply: oneshot::Sender<Result<Option<u64>>>,
    },
    EnsureWorkspaceRetired {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
        reply: oneshot::Sender<Result<Option<u64>>>,
    },
}

/// Dedicated controller owner for globally ordered commitment publication.
pub struct CommitmentPublisher;

impl CommitmentPublisher {
    pub fn open(
        telemetry_root: impl AsRef<Path>,
        repo_id: RepoId,
        known_incarnations: impl IntoIterator<Item = WorkspaceIncarnation>,
        capacity: usize,
    ) -> Result<CommitmentPublisherHandle> {
        let store = CommitmentStore::open(telemetry_root, repo_id, known_incarnations)
            .map_err(map_commitment_error)?;
        Self::start(store, capacity)
    }

    pub fn start(store: CommitmentStore, capacity: usize) -> Result<CommitmentPublisherHandle> {
        if capacity == 0 {
            return Err(CowshedError::usage(
                "commitment publisher capacity must be positive",
                "configure a positive bounded commitment channel",
            ));
        }
        let (sender, mut receiver) = mpsc::channel::<CommitmentRequest>(capacity);
        tokio::spawn(async move {
            let mut store = store;
            while let Some(request) = receiver.recv().await {
                match request {
                    CommitmentRequest::Publish { draft, reply } => {
                        let _ = reply.send(publish_draft(&mut store, draft));
                    }
                    CommitmentRequest::EnsureWorkspaceIntroduced {
                        repo_id,
                        workspace_incarnation,
                        reply,
                    } => {
                        let result = store
                            .refresh()
                            .map_err(map_commitment_error)
                            .and_then(|()| {
                                if store
                                    .workspace_is_retired(&repo_id, &workspace_incarnation)
                                {
                                    Err(CowshedError::integrity(
                                        "active storage fact references a retired workspace incarnation",
                                        "cowshed doctor --json",
                                    ))
                                } else if store
                                    .workspace_is_introduced(&repo_id, &workspace_incarnation)
                                {
                                    Ok(None)
                                } else {
                                    publish_draft(
                                        &mut store,
                                        CommitmentDraft::WorkspaceIntroduced {
                                            repo_id,
                                            workspace_incarnation,
                                        },
                                    )
                                    .map(Some)
                                }
                            });
                        let _ = reply.send(result);
                    }
                    CommitmentRequest::EnsureWorkspaceRetired {
                        repo_id,
                        workspace_incarnation,
                        reply,
                    } => {
                        let result = store
                            .refresh()
                            .map_err(map_commitment_error)
                            .and_then(|()| {
                                if store.workspace_is_retired(&repo_id, &workspace_incarnation) {
                                    Ok(None)
                                } else {
                                    publish_draft(
                                        &mut store,
                                        CommitmentDraft::WorkspaceRetired {
                                            repo_id,
                                            workspace_incarnation,
                                        },
                                    )
                                    .map(Some)
                                }
                            });
                        let _ = reply.send(result);
                    }
                }
            }
        });
        Ok(CommitmentPublisherHandle { sender })
    }
}

fn publish_draft(store: &mut CommitmentStore, draft: CommitmentDraft) -> Result<u64> {
    for attempt in 0..=MAX_COMMITMENT_CONFLICT_RETRIES {
        store.refresh().map_err(map_commitment_error)?;
        let order = store.next_order().map_err(map_commitment_error)?;
        match store.publish(draft.clone().into_commitment(order)) {
            Ok(()) => return Ok(order),
            Err(CommitmentStoreError::Conflict { .. })
                if attempt < MAX_COMMITMENT_CONFLICT_RETRIES => {}
            Err(error) => return Err(map_commitment_error(error)),
        }
    }
    unreachable!("bounded conflict loop returns on its final attempt")
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
    pub(crate) async fn ensure_workspace_introduced(
        &mut self,
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
    ) -> Result<Option<u64>> {
        let (reply, receive) = oneshot::channel();
        self.send(CommitmentRequest::EnsureWorkspaceIntroduced {
            repo_id,
            workspace_incarnation,
            reply,
        })
        .await?;
        receive_commitment_reply(receive).await
    }

    pub(crate) async fn ensure_workspace_retired(
        &mut self,
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
    ) -> Result<Option<u64>> {
        let (reply, receive) = oneshot::channel();
        self.send(CommitmentRequest::EnsureWorkspaceRetired {
            repo_id,
            workspace_incarnation,
            reply,
        })
        .await?;
        receive_commitment_reply(receive).await
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

async fn receive_commitment_reply<T>(receive: oneshot::Receiver<Result<T>>) -> Result<T> {
    receive.await.map_err(|_| {
        CowshedError::environment_missing(
            "repo commitment publisher stopped before durable acknowledgement",
            "reattach the project",
        )
    })?
}

#[async_trait]
impl CommitmentSink for CommitmentPublisherHandle {
    async fn publish(&mut self, draft: CommitmentDraft) -> Result<u64> {
        let (reply, receive) = oneshot::channel();
        self.send(CommitmentRequest::Publish { draft, reply })
            .await?;
        receive_commitment_reply(receive).await
    }
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

        let mut command = tokio::process::Command::new(&plan.program);
        command
            .args(&plan.args)
            .current_dir(&plan.cwd)
            .envs(&request.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        prepare_child_descriptors(command.as_std_mut()).map_err(map_spawn_failure)?;
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
            OutputStream::Stdout,
            stdout,
            events.clone(),
        ));
        tokio::spawn(run_system_output(
            job_id,
            OutputStream::Stderr,
            stderr,
            events.clone(),
        ));
        tokio::spawn(async move {
            let exit = match child.wait().await {
                Ok(status) => {
                    if let Some(code) = status.code() {
                        ProcessExit::Exited(code)
                    } else {
                        ProcessExit::Signaled {
                            signal: status.signal().unwrap_or(libc::SIGKILL),
                            core_dumped: status.core_dumped(),
                        }
                    }
                }
                Err(_) => ProcessExit::Signaled {
                    signal: libc::SIGKILL,
                    core_dumped: false,
                },
            };
            let _ = events.send(ProcessEvent::Exited { job_id, exit }).await;
        });
        Ok(Box::new(SystemRunningProcess {
            pid,
            stdin: stdin_sender,
            stdin_closed: false,
        }))
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
        let raw = match signal {
            ProcessSignal::Term => libc::SIGTERM,
            ProcessSignal::Kill => libc::SIGKILL,
        };
        let pid = i32::try_from(self.pid)
            .map_err(|_| CowshedError::internal("process id exceeds platform range"))?;
        let result = unsafe { libc::kill(-pid, raw) };
        if result == 0 {
            Ok(())
        } else {
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
    stream: OutputStream,
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
        stream: OutputStream,
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
        stream: OutputStream,
        offset: u64,
    ) -> Result<LogChunk> {
        self.log_read(job_id, stream, offset, true).await
    }

    pub async fn checkpoint_barrier(
        &self,
        checkpoint_id: String,
        barrier_id: u64,
    ) -> Result<CheckpointBarrier> {
        self.call(|reply| Command::Checkpoint {
            authority: self.authority.clone(),
            checkpoint_id,
            barrier_id,
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
            term_grace: config.term_grace,
            next_job_id,
            next_session_id: 1,
            next_barrier_id: 1,
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
        stream: OutputStream,
        offset: u64,
        follow: bool,
        reply: oneshot::Sender<Result<LogChunk>>,
    },
    Checkpoint {
        authority: WorkspaceAuthoritySnapshot,
        checkpoint_id: String,
        barrier_id: u64,
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
enum KillReason {
    Requested,
    OutputLimit,
    Retire,
    StdinFailure,
}

struct PendingStdin {
    bytes: Bytes,
    reply: oneshot::Sender<Result<()>>,
}

struct PendingLog {
    stream: OutputStream,
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
    exit: Option<ProcessExit>,
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

    fn stream(&self, stream: OutputStream) -> (&VecDeque<Bytes>, u64, bool) {
        match stream {
            OutputStream::Stdout => (&self.stdout, self.stdout_len, self.stdout_eof),
            OutputStream::Stderr => (&self.stderr, self.stderr_len, self.stderr_eof),
        }
    }
}

struct SupervisorActor {
    authority: WorkspaceAuthoritySnapshot,
    workspace_root: PathBuf,
    default_cwd: Option<WorkspacePath>,
    sandbox: SandboxConfig,
    term_grace: Duration,
    next_job_id: JobId,
    next_session_id: u64,
    next_barrier_id: u64,
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
                barrier_id,
                reply,
            } => {
                let result = self.checkpoint(&authority, checkpoint_id, barrier_id).await;
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
            .publish(CommitmentDraft::Admission {
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
        let started = utc_now().unwrap_or_else(|_| {
            UtcTimestamp::new("1970-01-01T00:00:00Z").expect("static timestamp")
        });
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
                job.exit = Some(ProcessExit::Exited(error.exec_wrapper_exit_code().into()));
                job.kill_reason = Some(KillReason::StdinFailure);
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
            let _ = reply.send(Ok(job.info.clone()));
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
            let _ = reply.send(Ok(()));
        } else {
            job.kill_waiters.push(reply);
        }
    }

    fn log_read(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        stream: OutputStream,
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
        barrier_id: u64,
    ) -> Result<CheckpointBarrier> {
        self.validate_authority(authority)?;
        if barrier_id != self.next_barrier_id {
            return Err(CowshedError::conflict(
                "checkpoint barrier id is stale or out of order",
                "retry with the next supervisor barrier id",
            ));
        }
        validate_checkpoint_id(&checkpoint_id)?;
        let mut barrier = self.artifacts.checkpoint(barrier_id)?;
        barrier.checkpoint_id = checkpoint_id.clone();
        self.commitments
            .publish(CommitmentDraft::Checkpoint {
                repo_id: self.authority.repo_id.clone(),
                origin_incarnation: self.authority.workspace_incarnation.clone(),
                checkpoint_id,
                barrier_id,
                manifest_batch_sha256: barrier.manifest_batch_sha256,
            })
            .await?;
        self.next_barrier_id = self
            .next_barrier_id
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("checkpoint barrier allocation exhausted"))?;
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
                        OutputStream::Stdout => job.stdout_eof = true,
                        OutputStream::Stderr => job.stderr_eof = true,
                    }
                    flush_log_waiters(job);
                }
            }
            ProcessEvent::Exited { job_id, exit } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.exit = Some(exit);
                    job.process = None;
                    for pending in job.pending_stdin.drain(..) {
                        let _ = pending.reply.send(Err(CowshedError::conflict(
                            "job exited before stdin was accepted",
                            "inspect the terminal job status",
                        )));
                    }
                    for waiter in job.close_waiters.drain(..) {
                        let _ = waiter.send(Ok(()));
                    }
                    job.info.stdin.complete = true;
                }
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

    fn process_output(&mut self, job_id: JobId, stream: OutputStream, bytes: Bytes) {
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
                        OutputStream::Stdout => {
                            job.stdout_len += byte_count(accepted.len());
                            job.stdout.push_back(accepted);
                        }
                        OutputStream::Stderr => {
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
                let _ = self.begin_kill(job_id, KillReason::StdinFailure);
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
                (!job.terminal_committed && job.exit.is_some() && job.stdout_eof && job.stderr_eof)
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
            Some(KillReason::StdinFailure) => JobState::Failed,
            None => match job.exit.expect("ready terminal job has exit") {
                ProcessExit::Exited(_) => JobState::Exited,
                ProcessExit::Signaled { .. } => JobState::Signaled,
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
            .publish(CommitmentDraft::Terminal {
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
        job.info.exit = match job.exit {
            Some(ProcessExit::Exited(code)) => Some(ExitStatus::Exited { code }),
            Some(ProcessExit::Signaled {
                signal,
                core_dumped,
            }) => Some(ExitStatus::Signaled {
                signal,
                core_dumped,
            }),
            None => None,
        };
        job.info.stdout = seal.stdout;
        job.info.stderr = seal.stderr;
        job.info.output_limit = seal.output_limit;
        job.info.stdin.complete = true;
        if let Some(identity) = job.session_identity
            && let Some(session) = self.sessions.get_mut(&identity)
        {
            session.background_jobs.remove(&job_id);
        }
        let info = job.info.clone();
        for waiter in job.waiters.drain(..) {
            let _ = waiter.send(Ok(info.clone()));
        }
        for waiter in job.kill_waiters.drain(..) {
            let _ = waiter.send(Ok(()));
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
    stream: OutputStream,
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

fn validate_checkpoint_id(value: &str) -> Result<()> {
    if (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
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

fn utc_now() -> Result<UtcTimestamp> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CowshedError::internal(format!("system clock is before epoch: {error}")))?
        .as_secs();
    let timestamp = libc::time_t::try_from(seconds)
        .map_err(|_| CowshedError::internal("system time exceeds platform range"))?;
    let mut broken = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::gmtime_r(&timestamp, broken.as_mut_ptr()) };
    if result.is_null() {
        return Err(CowshedError::internal("failed to convert UTC timestamp"));
    }
    let broken = unsafe { broken.assume_init() };
    UtcTimestamp::new(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        broken.tm_year + 1900,
        broken.tm_mon + 1,
        broken.tm_mday,
        broken.tm_hour,
        broken.tm_min,
        broken.tm_sec,
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

fn map_commitment_error(error: CommitmentStoreError) -> CowshedError {
    match error {
        CommitmentStoreError::Conflict { .. } => CowshedError::conflict(
            error.to_string(),
            "reattach and retry against the current commitment order",
        ),
        CommitmentStoreError::Io { .. } => CowshedError::environment_missing(
            error.to_string(),
            "verify telemetry storage and retry",
        ),
        CommitmentStoreError::Integrity { .. } => {
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
mod lifecycle_commitment_tests {
    use super::*;

    #[tokio::test]
    async fn publisher_recovers_lifecycle_idempotently_before_reclaim() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-lifecycle-publisher-{}",
            Uuid::new_v4().simple()
        ));
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let store = CommitmentStore::open(&root, repo_id.clone(), [incarnation.clone()]).unwrap();
        let mut publisher = CommitmentPublisher::start(store, 4).unwrap();

        assert_eq!(
            publisher
                .ensure_workspace_introduced(repo_id.clone(), incarnation.clone())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            publisher
                .ensure_workspace_introduced(repo_id.clone(), incarnation.clone())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            publisher
                .ensure_workspace_retired(repo_id.clone(), incarnation.clone())
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            publisher
                .ensure_workspace_retired(repo_id.clone(), incarnation)
                .await
                .unwrap(),
            None
        );
        drop(publisher);

        let reopened = CommitmentStore::open(&root, repo_id, []).unwrap();
        assert_eq!(reopened.next_order().unwrap(), 3);
        std::fs::remove_dir_all(root).unwrap();
    }
}
