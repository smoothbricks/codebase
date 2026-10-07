use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::io;
use std::ops::Range;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::api::dto::{
    AdmissionKey, BinaryData, CommandArg, ExecCommand, ExecRequest, ExitStatus, JobFailure, JobId,
    JobInfo, JobJournalCursor, JobListeningPorts, JobState, JobTail, JobTailLimits,
    OutputLimitInfo, OutputPublication, OutputStorage, OutputSummary, ProtectedOutput, SealedJob,
    Sha256Digest, StdinInfo, StdinKind, StdinSource, StreamInfo, TraceContext, TraceId,
    UtcTimestamp, WorkspacePath,
};
use crate::api::resources::{HostLoadSample, JobResourceSample, ResidentBytes};
use crate::error::{AdmissionField, AdmissionRefusal, CowshedError, Result};
use crate::exec::{
    ExecError, SandboxExecRequest, SpawnPlan, classify_spawn_error, plan_exec_under,
    prepare_child_descriptors,
};
use crate::fork_lock::Spawn as _;
use crate::fsio::AnchoredDirectory;
use crate::metadata::{WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::sandbox::{
    SandboxConfig, SandboxProfileRole, sandbox_runtime_dir, sandbox_runtime_link, seatbelt_profile,
    shared_daemon_runtime_dir, shared_daemon_runtime_link,
};
use crate::storage::audit::AuditSinkError;
use crate::workspace_environment::{PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV, WORKSPACE_TOKEN_ENV};
use cowshed_gateway_types::WorkspaceToken;

use crate::runtime::job_groups::Birth;
use crate::runtime::job_resources::{JobSampler, Member, Observation, Sampling, StreamTally};
use crate::runtime::job_spans::{JobSpanEdge, JobSpanPublisher, TraceHealth};
use crate::runtime::nx_daemon::{NxDaemonKeeper, PROBE_INTERVAL, Probe, Verdict};
use crate::runtime::volume_usage::{HostVolume, HostVolumeStat, VolumeBaseline, VolumeMountpoint};
use crate::storage::job_artifact::{
    AdmissionLookup, AdmittedSession, AdmittedStdin, ArtifactConfig, ArtifactError, ArtifactStore,
    CompletedJobArtifacts, JobAdmission, JobEnding, OutputTargets, SealedCheckpointManifest,
    StreamKind,
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
    /// Controller-owned store and mount paths for resolving each Nx keeper start's current
    /// build-volume grant. Repository links cannot supply this authority.
    pub build_volume_layout: Option<crate::build_volume::BuildVolumeLayout>,
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
    /// The host telemetry root the supervisor seals each job's span under
    /// ([`super::job_spans`]); `None` writes no job spans.
    pub telemetry_root: Option<PathBuf>,
    /// Groups a lost predecessor recorded that processes still hold although their leader is
    /// gone ([`super::job_groups::take_lost`]): never signalled, carried in every ledger this
    /// supervisor writes until nothing holds their ids.
    pub inherited_groups: Vec<super::job_groups::UnresolvedGroup>,
    /// What this supervisor names its checkout's volumes, and how; `None` names nothing.
    pub volume_labels: Option<VolumeLabels>,
    /// The workspace volume each job's resource samples stat, mounted at `workspace_root`, beside
    /// the build volume each job's admission grants (07_api.md "Job monitoring"). `None` stats
    /// no volume: every job's volumes are then unconfigured.
    pub workspace_volume: Option<VolumeMountpoint>,
}

/// How a volume's label stood when its supervisor looked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Labelled {
    /// Nothing is mounted at the path: the directory belongs to another volume, never renamed.
    NotMounted,
    /// The file system already reports the label.
    Already,
    /// Renamed to the label.
    Renamed,
}

/// Names the volume mounted at a path: reads the file system's own name for it and renames it
/// through the APFS host (Disk Arbitration) when it differs. Injected, so the supervisor names
/// its checkout's volumes without owning the storage backend.
pub trait VolumeLabeller: Send + Sync + 'static {
    fn ensure_label(&self, mount: &Path, label: &str) -> std::io::Result<Labelled>;
}

/// The labels of one checkout's two volumes (16_build_volumes.md, "Substrate"): the workspace
/// image's, and the build volume's it links. A clone carries its source's label, the blank
/// template's `[cowshed]` or another checkout's, until its supervisor names it here: when the
/// supervisor starts, and whenever a job is admitted on a build volume the checkout's link moved
/// to (a land, a refork). Renaming queues on Disk Arbitration, measured at 10–28 s on a busy host,
/// so it runs in the background and never on a command's path; the outcome goes to stderr.
#[derive(Clone)]
pub struct VolumeLabels {
    pub workspace: String,
    pub build: String,
    pub labeller: Arc<dyn VolumeLabeller>,
}

impl std::fmt::Debug for VolumeLabels {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VolumeLabels")
            .field("workspace", &self.workspace)
            .field("build", &self.build)
            .finish_non_exhaustive()
    }
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
        if let Some(volume) = &self.workspace_volume
            && volume.path() != self.workspace_root
        {
            return Err(CowshedError::usage(
                format!(
                    "the workspace volume {} is not mounted at the supervisor workspace root {}",
                    volume.path().display(),
                    self.workspace_root.display()
                ),
                "configure the volume mounted at the workspace root",
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
                retained_port_blocks: Vec::new(),
                mode: crate::sandbox::RunSandboxMode::ReadWrite,
                grants: crate::sandbox::SandboxGrants::default(),
                allowed_unix_sockets: Vec::new(),
                additional_denies: Vec::new(),
                shed_links: Vec::new(),
                git_worktree_repository: None,
                build_volume_mount: None,
                repository_caches: Vec::new(),
                capabilities: Default::default(),
            },
            build_volume_layout: None,
            artifacts: ArtifactConfig::default(),
            term_grace: Duration::from_secs(2),
            actor_capacity: DEFAULT_ACTOR_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            credential_env_names: BTreeSet::new(),
            shell_host: None,
            shell_pool: super::shell_pool::ShellPoolConfig::default(),
            group_ledger: None,
            telemetry_root: None,
            inherited_groups: Vec::new(),
            volume_labels: None,
            workspace_volume: None,
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

/// A matched sandbox and its executed-child profiles. Jobs share the authority snapshot while
/// its project conventions are unchanged; a spawn refreshes capabilities before choosing its
/// read-write or read-only profile. No spawn pairs a sandbox with another snapshot's profile.
#[derive(Clone, Debug)]
pub struct SandboxPolicy(std::sync::Arc<RenderedPolicy>);

#[derive(Debug)]
struct RenderedPolicy {
    /// The current authority grants plus convention-gated project capabilities.
    ceiling: SandboxConfig,
    read_only: SandboxConfig,
    ceiling_child: String,
    read_only_child: String,
}

impl SandboxPolicy {
    pub fn render(ceiling: SandboxConfig) -> Result<Self> {
        let mut read_only = ceiling.clone();
        read_only.mode = crate::sandbox::RunSandboxMode::ReadOnly;
        let shell_cwd = ceiling
            .capabilities
            .contribution
            .shell
            .as_ref()
            .map_or(ceiling.workspace_mount.as_path(), |shell| {
                shell.directory.as_path()
            });
        read_only.configure_capabilities_for(shell_cwd)?;
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

    fn for_cwd(&self, cwd: &Path) -> Result<Self> {
        let capabilities = {
            let _span = crate::timing::span("spawn", "capability-detection");
            self.ceiling().detect_capabilities_for(cwd)?
        };
        if capabilities == self.ceiling().capabilities {
            Ok(self.clone())
        } else {
            let _span = crate::timing::span("spawn", "capability-profile");
            Self::render(self.ceiling().with_capabilities(capabilities))
        }
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
}

#[derive(Clone, Debug)]
pub struct ProcessSpawnRequest {
    pub authority: WorkspaceAuthoritySnapshot,
    pub job_id: JobId,
    pub command: SpawnCommand,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
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
    /// A cold exec host spawned to activate for a job: the job's first process, which it owns
    /// until its command starts.
    Activating {
        job_id: JobId,
        process: OwnedProcess,
    },
    /// The cold host's activation for the job ended, however it ended: its interval's cost, read
    /// by the host's parent while it still held the host. The host's later work is not the
    /// job's.
    #[cfg(target_os = "macos")]
    ActivationEnded {
        job_id: JobId,
        usage: std::result::Result<
            super::job_accounting::LeaderRusage,
            super::job_accounting::LeaderRusageError,
        >,
    },
    /// A command that runs in a warm exec host started after its job was admitted. The host is
    /// the command's parent and observed its group leader before it could reap it.
    Started {
        job_id: JobId,
        process: OwnedProcess,
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
    /// A signal the job asked for did not reach its process group.
    SignalFailed {
        job_id: JobId,
        error: CowshedError,
    },
}

/// A process a job owns, as its parent observed it right after spawning it: the identity of the
/// group it leads, and when it was spawned.
#[derive(Clone, Debug)]
pub struct OwnedProcess {
    pub birth: Birth,
    /// When the parent saw the spawn return. A command a warm host forks is stamped when the
    /// host's report of its start arrives, the first moment this process knows of it.
    pub spawned: Instant,
    /// Read by the spawning task, not when the supervisor later handles its event.
    pub host: std::result::Result<HostLoadSample, crate::host_load::HostLoadError>,
}

/// A job's running process. The leader is held unreaped until the handle is dropped -- once the
/// job concluded, or when its supervisor goes away -- so the group id names the job's group alone
/// for as long as signals may be sent, descendants that outlive the leader included.
pub trait RunningProcess: Send {
    /// The command's process as its parent observed it before anything could reap it. `None`
    /// until the process exists; a warm-shell job reports it with [`ProcessEvent::Started`].
    fn process(&self) -> Option<&OwnedProcess>;
    /// `Ok(false)` means the bounded process-input lane is full.
    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool>;
    fn close_stdin(&mut self) -> Result<()>;
    /// The leader ended: close the job's stdin for good, so descendants reading it see its end.
    fn end_stdin(&mut self);
    /// Signal the job's process group, only while the process that reaps its leader holds the
    /// leader unreaped, so the group id still names the job's group ([`ChildFence`]).
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
    /// The first refusal of an output copy the job asked for, stdout's before stderr's. The seal
    /// stands regardless: the terminal record is durable before any copy is attempted, so a
    /// refused copy travels beside it instead of replacing it.
    pub publication_failure: Option<CowshedError>,
}

pub trait ArtifactSink: Send {
    fn next_job_id(&self) -> Result<JobId>;
    /// Durably admit `job_id`, under `admission`'s key when it has one, before it spawns.
    fn admit(
        &mut self,
        job_id: JobId,
        grant_revision: u64,
        command: &ExecCommand,
        admission: Option<JobAdmission>,
    ) -> Result<()>;
    /// What this workspace incarnation's admissions say of `key`.
    fn admitted(&self, key: &AdmissionKey) -> AdmissionLookup<'_>;
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
            resources: record.resources.clone(),
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
        admission: Option<JobAdmission>,
    ) -> Result<()> {
        let token = self
            .store
            .begin_job(
                job_id,
                grant_revision,
                command,
                admission,
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

    fn admitted(&self, key: &AdmissionKey) -> AdmissionLookup<'_> {
        self.store.admitted(key)
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
        let publication_failure = [stdout_publication, stderr_publication]
            .into_iter()
            .flatten()
            .find_map(|publication| publication.err())
            .map(map_artifact_error);
        Ok(ArtifactSeal {
            stdout: sealed.record.stdout,
            stderr: sealed.record.stderr,
            terminal_batch_sha256: sealed.terminal_batch_sha256,
            output_limit: sealed.output_limit,
            publication_failure,
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

/// The selected detector supplies the convention; core never discovers a tool by name.
fn shell_directory(sandbox: &SandboxConfig) -> Option<PathBuf> {
    sandbox
        .capabilities
        .contribution
        .shell
        .as_ref()
        .map(|shell| shell.directory.clone())
}

/// Activation runs inside the executed child, with directory and argv passed positionally.
fn wrap_one_shot(plan: &mut SpawnPlan, shell: &crate::capabilities::ShellActivation) {
    let mut activation = vec![
        OsString::from("/bin/sh"),
        OsString::from("-c"),
        OsString::from(shell.script),
        OsString::from(shell.label),
        shell.directory.as_os_str().to_owned(),
    ];
    activation.extend(plan.args.drain(3..));
    plan.args.extend(activation);
}

/// The sandboxed launch of a one-shot child running `argv` in `cwd`: entered through the
/// workspace shell selected for that cwd when there is one.
fn one_shot_plan(
    sandbox: &SandboxConfig,
    profile: &str,
    cwd: PathBuf,
    argv: Vec<OsString>,
) -> Result<SpawnPlan> {
    let mut plan = plan_exec_under(SandboxExecRequest { argv, cwd }, sandbox, profile)
        .map_err(map_exec_error)?;
    if let Some(shell) = &sandbox.capabilities.contribution.shell {
        wrap_one_shot(&mut plan, shell);
    }
    Ok(plan)
}

/// Tool commands are exact private-bin links; no host profile joins PATH.
fn bootstrap_path(sandbox: &SandboxConfig) -> Result<OsString> {
    let tools = match sandbox.mode {
        crate::sandbox::RunSandboxMode::ReadOnly => sandbox.exec_temp_dir.join("tools/bin"),
        crate::sandbox::RunSandboxMode::ReadWrite => {
            sandbox.workspace_mount.join(".cowshed/tools/bin")
        }
    };
    let mut paths = vec![tools];
    paths.push(sandbox.workspace_mount.join(".cowshed/bin"));
    if let Some(path) = developer_directory().map(|directory| directory.join("usr/bin"))
        && !paths.contains(&path)
    {
        paths.push(path);
    }
    for fixed in ["/usr/bin", "/bin", "/usr/sbin", "/sbin"] {
        paths.push(PathBuf::from(fixed));
    }
    std::env::join_paths(paths)
        .map_err(|error| CowshedError::internal(format!("construct sandbox PATH: {error}")))
}

/// The platform's selected developer directory: a fixed system install, never a value from the
/// environment that started this process.
fn developer_directory() -> Option<PathBuf> {
    [
        PathBuf::from("/Applications/Xcode.app/Contents/Developer"),
        PathBuf::from("/Library/Developer/CommandLineTools"),
    ]
    .into_iter()
    .find(|path| path.is_dir())
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

/// Prepare only generic private roots and the cache/daemon paths contributed by active detectors.
///
/// Every shared cache directory is created where the host keeps it before any child runs: a
/// child granted writes inside one cannot create its parent.
fn prepare_private_environment(
    environment_root: &Path,
    runtime: Option<(&Path, &Path)>,
    contribution: &crate::capabilities::CapabilityContribution,
) -> Result<AnchoredDirectory> {
    let environment =
        AnchoredDirectory::create(environment_root).map_err(private_environment_error)?;
    for name in [c"home", c"config", c"cache", c"data", c"state", c"run"] {
        environment.child(name).map_err(private_environment_error)?;
    }
    let bin = environment
        .child(c"tools")
        .and_then(|tools| tools.child(c"bin"))
        .map_err(private_environment_error)?;
    let mut programs = Vec::with_capacity(contribution.bootstrap_programs.len());
    for program in &contribution.bootstrap_programs {
        if program.name.is_empty() || program.name.contains('/') {
            return Err(CowshedError::integrity(
                "capability executable name is invalid",
                "repair the bootstrap detector",
            ));
        }
        let name = std::ffi::CString::new(program.name).map_err(|error| {
            private_environment_error(io::Error::new(io::ErrorKind::InvalidInput, error))
        })?;
        programs.push((name, program.target.as_path()));
    }
    bin.reconcile_links(c".links", &programs)
        .map_err(private_environment_error)?;
    for cache in &contribution.shared_caches {
        AnchoredDirectory::create(&cache.path).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot prepare shared cache {}: {error}",
                    cache.path.display()
                ),
                "cowshed setup",
            )
        })?;
        if let Some(target) = &cache.private_link {
            let relative = target.strip_prefix(environment_root).map_err(|_| {
                CowshedError::integrity(
                    "capability cache link escapes the private environment",
                    "repair the cache detector",
                )
            })?;
            let leaf = relative.file_name().ok_or_else(|| {
                CowshedError::integrity(
                    "capability cache link has no filename",
                    "repair the cache detector",
                )
            })?;
            let leaf = std::ffi::CString::new(leaf.as_bytes()).map_err(|error| {
                private_environment_error(io::Error::new(io::ErrorKind::InvalidInput, error))
            })?;
            let parent = relative.parent().expect("a relative filename has a parent");
            if parent.as_os_str().is_empty() {
                environment
                    .ensure_symlink(&leaf, &cache.path)
                    .map_err(private_environment_error)?;
            } else {
                private_directory(&environment, parent)
                    .and_then(|directory| directory.ensure_symlink(&leaf, &cache.path))
                    .map_err(private_environment_error)?;
            }
        }
    }
    for path in &contribution.daemon_isolation.directories {
        if let Ok(relative) = path.strip_prefix(environment_root) {
            private_directory(&environment, relative).map_err(private_environment_error)?;
        } else if let Some((alias, directory)) = runtime
            && let Ok(relative) = path.strip_prefix(alias)
        {
            let runtime =
                AnchoredDirectory::create(directory).map_err(private_environment_error)?;
            private_directory(&runtime, relative).map_err(private_environment_error)?;
        } else {
            return Err(CowshedError::integrity(
                "capability daemon directory escapes the private environment and shared runtime",
                "repair the daemon detector",
            ));
        }
    }
    Ok(environment)
}

fn private_directory(root: &AnchoredDirectory, relative: &Path) -> io::Result<AnchoredDirectory> {
    let mut components = relative.components();
    let Some(std::path::Component::Normal(first)) = components.next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private directory must be a nonempty relative path",
        ));
    };
    let name = std::ffi::CString::new(first.as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let directory = root.child(&name)?;
    if components.as_path().as_os_str().is_empty() {
        Ok(directory)
    } else {
        private_directory(&directory, components.as_path())
    }
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

/// The caller's variables a child may take: no bypass of managed Git configuration and no
/// agent-harness `CI` in a development checkout. GitHub/Forgejo Actions identifies real CI
/// with `GITHUB_ACTIONS=true`; a bare `CI` changes Cargo unit identities but names no runner.
/// What the sandbox owns or withholds is laid over this by [`SandboxEnvironment`].
pub(super) fn caller_environment(
    caller: &BTreeMap<String, String>,
) -> impl Iterator<Item = (&str, &str)> {
    caller
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .filter(|(name, _)| crate::workspace_git_fetch::caller_git_environment_allowed(name))
        .filter(|(name, _)| {
            *name != "CI"
                || caller
                    .get("GITHUB_ACTIONS")
                    .is_some_and(|value| value == "true")
        })
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

/// Run `argv` in `cwd` exactly as a one-shot read-write job of `sandbox` runs it, and collect its
/// output: the capabilities detected for `cwd`, the executed-child profile, the job environment
/// without a caller's variables, and the workspace shell's activation inside the sandbox.
///
/// Controller-owned discovery asks a project's tools through this rather than through the
/// pre-activation environment: a checkout whose toolchain comes from its dev environment has
/// none on the bootstrap PATH, or a different one, and a job only ever meets the activated one.
pub async fn run_as_job(
    sandbox: &SandboxConfig,
    cwd: &Path,
    argv: Vec<OsString>,
) -> Result<std::process::Output> {
    let mut ceiling = sandbox.clone();
    ceiling.configure_capabilities_for(cwd)?;
    let policy = SandboxPolicy::render(ceiling)?;
    let (sandbox, profile) = policy.child(crate::api::dto::RunSandboxMode::ReadWrite);
    let caller = BTreeMap::new();
    let environment = sandbox_environment(sandbox, &caller).await?;
    let plan = one_shot_plan(sandbox, profile, cwd.to_owned(), argv)?;
    let mut command = sandboxed_command(&plan, &environment.child(&caller));
    prepare_child_descriptors(command.as_std_mut())
        .map_err(ExecError::from)
        .map_err(map_exec_error)?;
    crate::fork_lock::RunAsync::output_locked(&mut command)
        .await
        .map_err(classify_spawn_error)
        .map_err(ExecError::from)
        .map_err(map_exec_error)
}

/// Prepare the workspace's private environment host-side and describe the child environment.
///
/// The description is the whole job contract: every value is derived from the sandbox, the
/// workspace image and the host's fixed install locations, and `caller` is only the request's
/// explicit `env`. Nothing is read from this process's own environment, so a supervisor started
/// from another checkout's shell, a login PATH or a stuffed caller hands its children exactly
/// what one started by launchd does. Shell activation runs inside the sandbox, after this; the
/// PATH here discovers bootstrap tools and the workspace shell owns PATH from then on.
pub(super) async fn sandbox_environment(
    sandbox: &SandboxConfig,
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
    let runtime_link = sandbox_runtime_link(sandbox);
    let shared_runtime = shared_daemon_runtime_dir(sandbox);
    let shared_alias = shared_daemon_runtime_link(sandbox);
    let environment = prepare_private_environment(
        environment_root,
        Some((&shared_alias, &shared_runtime)),
        &sandbox.capabilities.contribution,
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
    if sandbox
        .capabilities
        .contribution
        .daemon_isolation
        .directories
        .iter()
        .any(|path| path.starts_with(&shared_alias))
    {
        point_runtime_link(&shared_alias, &shared_runtime).await?;
    }
    // Host-side preparation: adopted bindings are controller metadata, not child-readable
    // files. The Git directory probe runs under the narrower GitDiscovery child profile.
    // Refresh each spawn so revoked grants and relocated checkouts cannot leave stale routes.
    let git_fetch_config = crate::workspace_git_fetch::refresh_git_fetch_config(sandbox).await?;
    // Identity is the workspace's own published file or nothing at all. A workspace minted
    // before capture existed keeps the empty device and fails an authorless commit loudly,
    // rather than silently borrowing whatever the controller's user happens to be.
    let git_identity = crate::git::workspace_git_identity_config(&sandbox.workspace_mount)?;
    let path = bootstrap_path(sandbox)?;
    let port_base = sandbox.port_block.base().to_string();
    let encoded_token = workspace_token.encode();
    let gateway_http = gateway_proxy_url(&port_base, &workspace_token);

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
    // Short, workspace-owned rendezvous namespace; no host runtime state is inherited.
    own("XDG_RUNTIME_DIR", runtime_link.as_os_str());
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
    // Core Git policy includes only controller-approved routes and never inherits caller config.
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
    let mut defaults = BTreeMap::new();
    // Publish the public workspace CA plus platform roots before any child runs.
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
    crate::workspace_clients::publish_client_wiring(
        &environment,
        &crate::workspace_clients::ClientWiring {
            workspace_ca: workspace_ca.as_deref(),
            system_bundle: &system_bundle,
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
        for name in [
            crate::workspace_clients::GIT_CA_ENV,
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
    }
    for (&name, action) in &sandbox.capabilities.contribution.env {
        match action {
            crate::capabilities::EnvAction::Own(value) => {
                owned.insert(name.into(), value.clone());
            }
            crate::capabilities::EnvAction::Unset => withheld.push(name),
            crate::capabilities::EnvAction::Default(value) => {
                if caller.contains_key(name) {
                    eprintln!(
                        "cowshed: {name} was supplied by the caller; the project capability \
                         default is not being used"
                    );
                }
                defaults.insert(name.into(), value.clone());
            }
            crate::capabilities::EnvAction::Append(value) => {
                appended.insert(name.into(), value.clone());
            }
        }
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
        let cwd =
            crate::exec::contained_cwd(&request.policy.ceiling().workspace_mount, &request.cwd)
                .map_err(map_exec_error)?;
        let policy = request.policy.for_cwd(&cwd)?;
        let (sandbox, profile) = policy.child(request.mode);
        let envrc_directory = shell_directory(sandbox);
        let environment = crate::timing::spanned(
            "spawn",
            "environment",
            sandbox_environment(sandbox, &request.env),
        )
        .await?;
        let pooled = |command, environment| super::shell_job::PooledSpawn {
            job_id: request.job_id,
            command,
            cwd: cwd.clone(),
            profile,
            read_only: sandbox.mode == crate::sandbox::RunSandboxMode::ReadOnly,
            workspace_mount: sandbox.workspace_mount.clone(),
            envrc_directory: envrc_directory.clone(),
            environment,
            caller: &request.env,
            grant_revision: request.authority.grant_revision,
        };
        let argv = match request.command {
            SpawnCommand::Script(script) => {
                let Some(shells) = self.shells.as_mut() else {
                    return Err(CowshedError::environment_missing(
                        "a script job runs in the workspace's exec host, which needs the cowshed \
                         binary",
                        "run the script through the cowshed CLI",
                    ));
                };
                return shells.spawn(
                    pooled(super::shell_job::HostCommand::Script(script), environment),
                    events,
                );
            }
            SpawnCommand::Argv(argv) => argv,
        };
        // Warm hosts serve commands under a workspace `.envrc`; a bare workspace keeps one-shot
        // spawns for argv jobs.
        if let (Some(_), Some(shells)) = (&envrc_directory, self.shells.as_mut()) {
            return shells.spawn(
                pooled(super::shell_job::HostCommand::Argv(argv), environment),
                events,
            );
        }
        let plan = one_shot_plan(sandbox, profile, cwd, argv)?;
        let mut command = sandboxed_command(&plan, &environment.child(&request.env));
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        prepare_child_descriptors(command.as_std_mut())
            .map_err(ExecError::from)
            .map_err(map_exec_error)?;
        // SAFETY: `pre_exec` runs in the forked child, between `fork` and `exec`, in a process
        // that was multithreaded at the fork. Only async-signal-safe calls are legal there, and
        // POSIX lists `setpgid` as one; it allocates nothing, takes no lock, and touches no
        // memory this closure captures. Its success is load-bearing rather than decorative:
        // `ChildFence::signal` signals `-pid`, which is the child's own group only because the
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
        // Through `std`: this process alone reaps the child, inside its fence ([`ChildFence`]).
        let mut child = command
            .into_std()
            .spawn_locked()
            .map_err(classify_spawn_error)
            .map_err(ExecError::from)
            .map_err(map_exec_error)?;
        let fence = std::sync::Arc::new(ChildFence::new(child.id())?);
        let pipe = |error: io::Error| {
            CowshedError::internal(format!("cannot watch the spawned process's pipes: {error}"))
        };
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CowshedError::internal("spawned process has no stdin pipe"))
            .and_then(|stdin| tokio::process::ChildStdin::from_std(stdin).map_err(pipe))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CowshedError::internal("spawned process has no stdout pipe"))
            .and_then(|stdout| tokio::process::ChildStdout::from_std(stdout).map_err(pipe))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CowshedError::internal("spawned process has no stderr pipe"))
            .and_then(|stderr| tokio::process::ChildStderr::from_std(stderr).map_err(pipe))?;
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
        let watcher = std::sync::Arc::clone(&fence);
        tokio::spawn(async move {
            let event = match watcher.exited().await {
                Ok(exit) => ProcessEvent::Exited { job_id, exit },
                Err(error) => {
                    // The child's end was not observed, so it may still be running. Kill the
                    // group before reporting: a job that cannot be observed must not be left
                    // alive behind a terminal record.
                    let _ = watcher.signal(libc::SIGKILL);
                    ProcessEvent::WaitFailed { job_id, error }
                }
            };
            let _ = events.send(event).await;
        });
        Ok(Box::new(SystemRunningProcess {
            fence,
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

/// A child this process spawned and alone reaps, with the fence that makes signalling its group
/// safe. Until the child is reaped its id cannot name another process, and it is reaped only
/// inside the critical section every signal takes, so a signal meets either the unreaped child
/// or the record that it is gone -- never a process that reused its id. Nothing else may reap
/// it: the child is spawned through `std`, whose handle never reaps on its own, not through a
/// Tokio handle whose dropped children Tokio reaps by pid in the background.
///
/// A job's leader is observed exiting without being reaped ([`Self::exited`]) and stays held
/// until the job concludes ([`Self::release`]): descendants that outlive it may hold the job's
/// output open, and a kill must still reach them through the group id the held leader keeps.
pub(super) struct ChildFence {
    pid: i32,
    process: OwnedProcess,
    hold: Mutex<Hold>,
}

/// Where a fenced child is, as far as signalling its group is concerned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Hold {
    /// Unreaped, running or exited: its id names only it, and its group id only its group.
    Held,
    /// Unreaped, and wanted no longer: it is reaped as soon as it is seen exited.
    Released,
    /// Collected: its id may name another process, so no signal follows.
    Reaped,
}

impl ChildFence {
    /// The fence for `pid`, this process's own child, spawned just now and not yet reaped.
    pub(super) fn new(pid: u32) -> Result<Self> {
        let spawned = Instant::now();
        let leader = i32::try_from(pid)
            .ok()
            .filter(|pid| *pid > 1)
            .ok_or_else(|| {
                CowshedError::internal("process id is not a signalable process group")
            })?;
        let host = crate::host_load::read_host_load();
        Ok(Self {
            pid: leader,
            process: OwnedProcess {
                birth: Birth::of(pid),
                spawned,
                host,
            },
            hold: Mutex::new(Hold::Held),
        })
    }

    pub(super) fn birth(&self) -> &Birth {
        &self.process.birth
    }

    pub(super) fn process(&self) -> &OwnedProcess {
        &self.process
    }

    fn hold(&self) -> std::sync::MutexGuard<'_, Hold> {
        self.hold.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Signal the group the child leads, while it is unreaped.
    pub(super) fn signal(&self, signal: i32) -> Result<()> {
        let hold = self.hold();
        if *hold == Hold::Reaped {
            return Err(CowshedError::conflict(
                format!(
                    "process {} has been reaped; processes still holding its group id cannot be \
                     told from another group that took it, so none is signalled",
                    self.pid
                ),
                format!("inspect them with `ps -g {}`", self.pid),
            ));
        }
        if let Birth::Unobserved { reason, .. } = &self.process.birth {
            return Err(CowshedError::environment_missing(
                format!(
                    "process group {} was never identified ({reason}); it is not signalled",
                    self.pid
                ),
                format!("inspect it with `ps -g {}`", self.pid),
            ));
        }
        super::job_groups::signal_unreaped_group(self.pid, signal).map_err(|error| {
            CowshedError::environment_missing(
                format!("failed to signal sandbox process tree: {error}"),
                "inspect the job and retry",
            )
        })
    }

    /// Collect the child inside the fence if it has exited; `None` while it runs.
    fn collect(&self, hold: &mut Hold) -> io::Result<Option<std::process::ExitStatus>> {
        loop {
            let mut status = 0;
            // SAFETY: `pid` is this process's own child and `status` a valid out-pointer.
            let waited = unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) };
            if waited == self.pid {
                *hold = Hold::Reaped;
                return Ok(Some(std::process::ExitStatus::from_raw(status)));
            }
            if waited == 0 {
                return Ok(None);
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ECHILD) {
                // Something else collected it: its id is free, so no signal may follow.
                *hold = Hold::Reaped;
                return Err(error);
            }
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    /// How the child ended, if it has, read without reaping it -- unless it was released.
    fn look(&self) -> io::Result<Option<ExitStatus>> {
        let mut hold = self.hold();
        if *hold == Hold::Reaped {
            return Err(io::Error::other("the child was already reaped"));
        }
        let exit = match super::job_groups::exit_unreaped(self.pid) {
            Ok(exit) => exit,
            Err(error) => {
                if error.raw_os_error() == Some(libc::ECHILD) {
                    // Something else collected it: its id is free, so no signal may follow.
                    *hold = Hold::Reaped;
                }
                return Err(error);
            }
        };
        if exit.is_some() && *hold == Hold::Released {
            self.collect(&mut hold)?;
        }
        Ok(exit)
    }

    /// Wait for the child to exit without reaping it: it stays held until [`Self::release`].
    /// The child-exit notifications are subscribed before the first look, so none falls between
    /// a look and the wait for the next.
    pub(super) async fn exited(&self) -> Result<ExitStatus> {
        let unobserved = |error: io::Error| {
            CowshedError::integrity(
                format!("cannot wait for the sandbox process: {error}"),
                "cowshed doctor --json",
            )
        };
        let mut exits = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
            .map_err(unobserved)?;
        loop {
            if let Some(exit) = self.look().map_err(unobserved)? {
                return Ok(exit);
            }
            if exits.recv().await.is_none() {
                return Err(unobserved(io::Error::other(
                    "child-exit notifications ended",
                )));
            }
        }
    }

    /// Nothing more is wanted of the child: reap it now if it has exited, or as soon as it is
    /// seen exiting.
    pub(super) fn release(&self) -> io::Result<()> {
        let mut hold = self.hold();
        if *hold != Hold::Held {
            return Ok(());
        }
        *hold = Hold::Released;
        self.collect(&mut hold).map(drop)
    }

    /// Reap the child, inside the fence, if it has exited; `None` while it runs.
    pub(super) fn try_reap(&self) -> io::Result<Option<std::process::ExitStatus>> {
        let mut hold = self.hold();
        if *hold == Hold::Reaped {
            return Err(io::Error::other("the child was already reaped"));
        }
        self.collect(&mut hold)
    }

    /// Wait for the child to exit and reap it inside the fence. The child-exit notifications are
    /// subscribed before the first look, so none falls between a look and the wait for the next.
    pub(super) async fn wait(&self) -> io::Result<std::process::ExitStatus> {
        let mut exits = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())?;
        loop {
            if let Some(status) = self.try_reap()? {
                return Ok(status);
            }
            if exits.recv().await.is_none() {
                return Err(io::Error::other("child-exit notifications ended"));
            }
        }
    }
}

pub(super) enum SystemStdin {
    Write(Bytes),
    Close,
}

/// The bounded lane from the actor to a job's stdin pump.
pub(super) struct StdinLane {
    /// `None` once the job's leader ended: the pump then closes the pipe.
    sender: Option<mpsc::Sender<SystemStdin>>,
    closed: bool,
}

impl StdinLane {
    pub(super) fn new(sender: mpsc::Sender<SystemStdin>) -> Self {
        Self {
            sender: Some(sender),
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
        let unavailable = || {
            CowshedError::conflict(
                "job stdin is no longer available",
                "inspect the terminal job status",
            )
        };
        let Some(sender) = &self.sender else {
            return Err(unavailable());
        };
        match sender.try_send(SystemStdin::Write(bytes)) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(unavailable()),
        }
    }

    pub(super) fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let Some(sender) = &self.sender else {
            self.closed = true;
            return Ok(());
        };
        match sender.try_send(SystemStdin::Close) {
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

    /// The job's leader ended: drop the lane, so the pump closes the pipe once it has written
    /// what it already took.
    pub(super) fn end(&mut self) {
        self.sender = None;
        self.closed = true;
    }
}

struct SystemRunningProcess {
    fence: std::sync::Arc<ChildFence>,
    stdin: StdinLane,
}

impl RunningProcess for SystemRunningProcess {
    fn process(&self) -> Option<&OwnedProcess> {
        Some(self.fence.process())
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        self.stdin.try_write(bytes)
    }

    fn close_stdin(&mut self) -> Result<()> {
        self.stdin.close()
    }

    fn end_stdin(&mut self) {
        self.stdin.end();
    }

    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()> {
        self.fence.signal(match signal {
            ProcessSignal::Term => libc::SIGTERM,
            ProcessSignal::Kill => libc::SIGKILL,
        })
    }
}

/// The job concluded, or its supervisor is going away: the leader is reaped now if it exited, or
/// by its watcher as soon as it does.
impl Drop for SystemRunningProcess {
    fn drop(&mut self) {
        if let Err(error) = self.fence.release() {
            eprintln!(
                "cowshed: cannot reap the sandbox process {}: {error}",
                self.fence.birth().pid()
            );
        }
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

    /// How this supervisor's job-span writer has fared, once every span row queued before the
    /// call is sealed or refused. Refused by a supervisor that writes no job spans.
    pub async fn trace_health(&self) -> Result<TraceHealth> {
        self.call(|reply| Command::TraceHealth {
            authority: self.authority.clone(),
            reply,
        })
        .await
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
            sandbox: Box::new(sandbox),
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

    /// Admit `request` with `build_volume`, the mountpoint of the checkout's build volume the
    /// controller resolved for this job (`None` when the checkout links none): the job's
    /// sandbox grants exactly that volume (16_build_volumes.md, "Process lifetime across a
    /// swap").
    pub async fn exec(
        &self,
        session: Option<&SessionToken>,
        build_volume: Option<PathBuf>,
        request: ExecRequest,
    ) -> Result<JobId> {
        self.exec_admitted(session, build_volume, request, false)
            .await
    }

    pub async fn exec_background(
        &self,
        session: Option<&SessionToken>,
        build_volume: Option<PathBuf>,
        request: ExecRequest,
    ) -> Result<JobId> {
        self.exec_admitted(session, build_volume, request, true)
            .await
    }

    async fn exec_admitted(
        &self,
        session: Option<&SessionToken>,
        build_volume: Option<PathBuf>,
        request: ExecRequest,
        background: bool,
    ) -> Result<JobId> {
        self.call(|reply| Command::Exec {
            authority: self.authority.clone(),
            session: session.cloned(),
            build_volume,
            request: Box::new(request),
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

    /// The job's resource sample: observed now while it runs, its terminal sample once it ended.
    /// A job that owns no process yet has none, and says so.
    pub async fn resources(&self, job_id: JobId) -> Result<JobResourceSample> {
        self.call(|reply| Command::Resources {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    /// The job `key` admitted in this workspace incarnation, from its durable records: answered
    /// after the exec's reply was lost, and by a supervisor that did not run the job.
    pub async fn job_by_key(&self, key: AdmissionKey) -> Result<JobId> {
        self.call(|reply| Command::JobByKey {
            authority: self.authority.clone(),
            key,
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

    /// A bounded slice of both streams: after `cursor` when it is named, else their latest
    /// bounded tail. A cursor past a stream's admitted bytes is a usage error.
    pub async fn tail(
        &self,
        job_id: JobId,
        cursor: Option<JobJournalCursor>,
        limits: JobTailLimits,
    ) -> Result<JobTail> {
        self.call(|reply| Command::Tail {
            authority: self.authority.clone(),
            job_id,
            cursor,
            limits,
            reply,
        })
        .await
    }

    /// The TCP ports the job's process group listens on now, read from the kernel; once the job
    /// ended, those its ended group still holds -- none, unless a process it left holds the
    /// group's id, which nothing then proves the job's and is an error.
    pub async fn listening_ports(&self, job_id: JobId) -> Result<JobListeningPorts> {
        self.call(|reply| Command::ListeningPorts {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
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
            fail_if_busy: false,
            reply,
        })
        .await
    }

    /// Close admission atomically only when no job is active; never wait for a workload.
    pub async fn quiesce_if_idle(&self) -> Result<()> {
        self.call(|reply| Command::Quiesce {
            authority: self.authority.clone(),
            fail_if_busy: true,
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
    /// open, no job is running, and a shed's keeper has stopped its Nx daemon and finds none
    /// ([`NxDaemonKeeper`]), so no daemon outlives the supervisor that watched it. Only the
    /// process serving a supervisor asks, to retire it; that serving host is the macOS native
    /// project host.
    #[cfg(target_os = "macos")]
    pub async fn idle(&self) -> Result<bool> {
        self.call(|reply| Command::Idle { reply }).await
    }

    pub async fn retire(&self) -> Result<()> {
        self.call(|reply| Command::Retire {
            authority: self.authority.clone(),
            reply,
        })
        .await
    }

    /// Name the build volume mounted at `build_volume` for this checkout, in the background
    /// ([`VolumeLabels`]): a land or a refork moved the checkout's link onto it, and it carries
    /// the label of the checkout that built it until this runs. Answered once the rename is
    /// queued, never after it.
    pub async fn name_build_volume(&self, build_volume: Option<PathBuf>) -> Result<()> {
        self.call(|reply| Command::NameBuildVolume {
            authority: self.authority.clone(),
            build_volume,
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
        // A supervisor serves exactly the block, TMPDIR and runtime link its sandbox records, so
        // this is where the workspace's `.cowshed/env` is brought up to date with its metadata.
        // The TMPDIR it names exists before any host shell can source it.
        AnchoredDirectory::create(&config.sandbox.exec_temp_dir)
            .map_err(private_environment_error)?;
        crate::workspace_credentials::publish_workspace_environment(
            crate::workspace_environment::EnvironmentMount::Served {
                workspace_mount: &config.sandbox.workspace_mount,
                temp_dir: &config.sandbox.exec_temp_dir,
            },
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
        let job_spans = config.telemetry_root.map(JobSpanPublisher::start);
        let handle = WorkspaceSupervisorHandle {
            authority: config.authority.clone(),
            commands,
        };
        let nx_daemon =
            NxDaemonKeeper::for_role(WorkspaceRole::for_name(&config.authority.workspace));
        let mut actor = SupervisorActor {
            authority: config.authority,
            workspace_root: config.workspace_root,
            default_cwd: config.default_cwd,
            policy,
            build_volume_layout: config.build_volume_layout,
            volume_labels: config.volume_labels,
            workspace_volume: config.workspace_volume,
            credential_env_names: config.credential_env_names,
            group_ledger: config.group_ledger,
            job_spans,
            inherited_groups: config.inherited_groups,
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
            nx_daemon,
            labelled_build: None,
            labelling: tokio::task::JoinSet::new(),
        };
        // Whatever the checkout's volumes were cloned from, they are named for it from here.
        let build = actor.policy.ceiling().build_volume_mount.clone();
        actor.relabel(true, build.as_deref());
        tokio::spawn(actor.run());
        Ok(handle)
    }
}

pub(super) enum Command {
    AdvanceAuthority {
        expected: WorkspaceAuthoritySnapshot,
        authority: WorkspaceAuthoritySnapshot,
        sandbox: Box<SandboxConfig>,
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
        build_volume: Option<PathBuf>,
        request: Box<ExecRequest>,
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
    Sealed {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<SealedJob>>,
    },
    Resources {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<JobResourceSample>>,
    },
    TraceHealth {
        authority: WorkspaceAuthoritySnapshot,
        reply: oneshot::Sender<Result<TraceHealth>>,
    },
    ListeningPorts {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<JobListeningPorts>>,
    },
    JobByKey {
        authority: WorkspaceAuthoritySnapshot,
        key: AdmissionKey,
        reply: oneshot::Sender<Result<JobId>>,
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
    Tail {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        cursor: Option<JobJournalCursor>,
        limits: JobTailLimits,
        reply: oneshot::Sender<Result<JobTail>>,
    },
    Checkpoint {
        authority: WorkspaceAuthoritySnapshot,
        checkpoint_id: String,
        reply: oneshot::Sender<Result<CheckpointBarrier>>,
    },
    Quiesce {
        authority: WorkspaceAuthoritySnapshot,
        fail_if_busy: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    Retire {
        authority: WorkspaceAuthoritySnapshot,
        reply: oneshot::Sender<Result<()>>,
    },
    NameBuildVolume {
        authority: WorkspaceAuthoritySnapshot,
        build_volume: Option<PathBuf>,
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
    /// Unnamed sessions have no stable name, so their admission identity must not be a reused
    /// supervisor-local counter. Named sessions retain their existing name identity.
    admission_identity: Option<Uuid>,
    cwd: Option<WorkspacePath>,
    env: BTreeMap<String, String>,
    background_jobs: BTreeSet<JobId>,
}

/// How a job that ended was concluded; a job without one is still running.
enum Conclusion {
    /// The terminal record is durable and the job's status projects it. `commitment` is the
    /// audit record's refusal, which happens only when the commitment publisher is gone;
    /// `publication` is the refusal of an output copy the job asked for.
    Sealed {
        commitment: Option<CowshedError>,
        publication: Option<CowshedError>,
    },
    /// The store refused the terminal record. Nothing durable says how the job ended, so its
    /// status is the refusal, not a projection with no artifact behind it: the actor keeps
    /// serving the output it captured, the ledger keeps the group, and the workspace's next
    /// supervisor seals the unterminated record as lost.
    Unsealed(CowshedError),
}

struct JobStateRecord {
    info: JobInfo,
    started_at: Instant,
    process: Option<Box<dyn RunningProcess>>,
    stdout: VecDeque<Bytes>,
    stderr: VecDeque<Bytes>,
    /// Each stream's counts as its chunks were admitted; their bytes are the read cursors.
    stdout_tally: StreamTally,
    stderr_tally: StreamTally,
    stdout_eof: bool,
    stderr_eof: bool,
    exit: Option<ExitStatus>,
    /// Set when `wait(2)` could not name the child's termination. Keeps the job's terminal
    /// record free of a fabricated exit and hands the failure to everyone awaiting the job.
    wait_failure: Option<CowshedError>,
    /// Set once the job ended, whatever became of its record.
    conclusion: Option<Conclusion>,
    /// The job's process as its parent observed it, once its pid is known. Never observed again.
    birth: Option<Birth>,
    /// What the job's resource reads answer: nothing before it owns a process, live samples
    /// while it does, its terminal sample -- or why that failed -- once it ended.
    sampling: Sampling,
    /// The job's volumes as its admission found them, immediately before its first process was
    /// dispatched: every sample's volume deltas count from here, activation and command alike.
    volumes: VolumeBaseline<HostVolume>,
    output_limit: Option<OutputLimitInfo>,
    kill_reason: Option<KillReason>,
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
    /// The job's claim on the build volume it was admitted with, from admission until the job
    /// ends: while it lives, no release unmounts that volume, whatever moved the checkout's link
    /// meanwhile (16_build_volumes.md, "Process lifetime across a swap").
    build_hold: Option<crate::build_volume::BuildVolumeHold>,
}

impl JobStateRecord {
    fn terminal(&self) -> bool {
        self.conclusion.is_some()
    }

    /// Whether the job's terminal record is durable, so the store is the authority for its
    /// output and its group no longer belongs in the ledger.
    fn sealed(&self) -> bool {
        matches!(self.conclusion, Some(Conclusion::Sealed { .. }))
    }

    /// The job as status reports it.
    fn status(&self) -> Result<JobInfo> {
        match &self.conclusion {
            Some(Conclusion::Unsealed(error)) => Err(error.clone()),
            _ => Ok(self.info.clone()),
        }
    }

    /// The answer to "how did this job end", for every caller that asked to be told.
    ///
    /// A retained failure outranks the record, in the order it was incurred: a wait failure
    /// (nothing observed the child terminate), then a terminal record or commitment that could not
    /// be established, then a refused output copy. `Ok` would be a wrong-success channel for
    /// what the caller asked.
    fn terminal_outcome(&self) -> Result<JobInfo> {
        let failure = self.wait_failure.as_ref().or(match &self.conclusion {
            Some(Conclusion::Sealed {
                commitment,
                publication,
            }) => commitment.as_ref().or(publication.as_ref()),
            Some(Conclusion::Unsealed(error)) => Some(error),
            None => None,
        });
        match failure {
            Some(error) => Err(error.clone()),
            None => Ok(self.info.clone()),
        }
    }

    fn stream(&self, stream: StreamKind) -> (&VecDeque<Bytes>, u64, bool) {
        match stream {
            StreamKind::Stdout => (&self.stdout, self.stdout_tally.bytes(), self.stdout_eof),
            StreamKind::Stderr => (&self.stderr, self.stderr_tally.bytes(), self.stderr_eof),
        }
    }
}

struct SupervisorActor {
    authority: WorkspaceAuthoritySnapshot,
    group_ledger: Option<PathBuf>,
    /// See [`WorkspaceSupervisorConfig::telemetry_root`].
    job_spans: Option<JobSpanPublisher>,
    inherited_groups: Vec<super::job_groups::UnresolvedGroup>,
    workspace_root: PathBuf,
    default_cwd: Option<WorkspacePath>,
    /// The sandbox of the authority served, rendered once when it was taken.
    policy: SandboxPolicy,
    build_volume_layout: Option<crate::build_volume::BuildVolumeLayout>,
    /// See [`WorkspaceSupervisorConfig::volume_labels`].
    volume_labels: Option<VolumeLabels>,
    /// See [`WorkspaceSupervisorConfig::workspace_volume`].
    workspace_volume: Option<VolumeMountpoint>,
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
    /// A shed's keeper of its sandboxed Nx daemon ([`super::nx_daemon`]); main keeps none.
    nx_daemon: Option<NxDaemonKeeper>,
    /// The build volume this supervisor last asked to be named for its checkout.
    labelled_build: Option<PathBuf>,
    /// Background renames, reaped without delaying provisioning or lifecycle commands.
    labelling: tokio::task::JoinSet<()>,
}

/// The actor's run loop ends only once every job is terminal, so an actor dropped with a job still
/// running was torn down with the runtime that hosts it. No later controller knows that job: it
/// would run on unobserved and uncancellable, so its process tree ends here with its record.
///
/// It ends by the protocol every kill follows — SIGTERM, the term grace, then SIGKILL — so a job
/// that stops its own children on SIGTERM (Nx's task runner stops task trees it keeps in their
/// own sessions, beyond this group) gets to. Nothing runs after a drop, so the grace is waited out
/// here, blocking, and only when a job was still running. A host that wants its jobs' cleanup to
/// be asynchronous cancels them before it drops the runtime. The leaders are released only after
/// the last signal, when the job handles drop with the actor: releasing first would free a
/// leader's id and leave nothing to prove the group the job's. Any failure to signal is reported,
/// because the job then outlives its supervisor.
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
            report_unsignalled(*id, process.signal_process_tree(ProcessSignal::Term));
        }
        std::thread::sleep(grace);
        for (id, process) in &mut running {
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
                labelled = self.labelling.join_next(), if !self.labelling.is_empty() => {
                    if let Some(Err(error)) = labelled {
                        eprintln!("cowshed: volume label task failed: {error}");
                    }
                }
                () = super::nx_daemon::next_probe(&mut self.nx_daemon),
                    if self.nx_daemon.is_some()
                        && self.lifecycle == ActorLifecycle::Running
                        && !self.command_lane_closed =>
                {
                    self.keep_nx_daemon().await;
                }
            }
            self.finish_ready_jobs().await;
            self.finish_lifecycle_waiters();
        }
        self.labelling.abort_all();
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::AdvanceAuthority {
                expected,
                authority,
                sandbox,
                reply,
            } => {
                let result = self.advance_authority(expected, authority, *sandbox);
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
                build_volume,
                request,
                background,
                reply,
            } => {
                let result = self
                    .admit_exec(authority, session, build_volume, *request, background)
                    .await;
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
                    .and_then(|()| self.job(job_id).and_then(JobStateRecord::status));
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
            Command::Resources {
                authority,
                job_id,
                reply,
            } => {
                let result = self
                    .validate_authority(&authority)
                    .and_then(|()| self.job_mut(job_id))
                    .and_then(sample_job);
                let _ = reply.send(result);
            }
            Command::TraceHealth { authority, reply } => {
                let writer = self.validate_authority(&authority).and_then(|()| {
                    self.job_spans.clone().ok_or_else(|| {
                        CowshedError::environment_missing(
                            "this workspace's supervisor writes no job spans",
                            "start the supervisor with a telemetry root",
                        )
                    })
                });
                match writer {
                    // The writer answers once every earlier row is sealed; the actor does not
                    // wait for that.
                    Ok(writer) => {
                        tokio::spawn(async move {
                            let _ = reply.send(writer.health().await);
                        });
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Command::JobByKey {
                authority,
                key,
                reply,
            } => {
                let result = self.validate_authority(&authority).and_then(|()| {
                    match self.artifacts.admitted(&key) {
                        AdmissionLookup::Admitted { job_id, .. } => Ok(job_id),
                        AdmissionLookup::Absent => Err(CowshedError::not_found(
                            "the admission key admitted no job in this workspace incarnation",
                            "exec the request under the key; nothing ran under it",
                        )),
                        AdmissionLookup::Unprovable { set_aside } => Err(
                            CowshedError::admission_refusal(AdmissionRefusal::Unprovable {
                                set_aside: set_aside.to_path_buf(),
                            }),
                        ),
                    }
                });
                let _ = reply.send(result);
            }
            Command::List { authority, reply } => {
                // A job the store refused to seal makes the list fail with that refusal rather
                // than vanish from it or appear with a projection no record backs.
                let result = self.validate_authority(&authority).and_then(|()| {
                    self.jobs
                        .values()
                        .map(JobStateRecord::status)
                        .collect::<Result<Vec<_>>>()
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
            Command::Tail {
                authority,
                job_id,
                cursor,
                limits,
                reply,
            } => self.tail(&authority, job_id, cursor, limits, reply),
            Command::ListeningPorts {
                authority,
                job_id,
                reply,
            } => {
                let result = self
                    .validate_authority(&authority)
                    .and_then(|()| self.job(job_id))
                    .and_then(listening_ports);
                let _ = reply.send(result);
            }
            Command::Checkpoint {
                authority,
                checkpoint_id,
                reply,
            } => {
                let result = self.checkpoint(&authority, checkpoint_id).await;
                let _ = reply.send(result);
            }
            Command::Quiesce {
                authority,
                fail_if_busy,
                reply,
            } => {
                if let Err(error) = self.validate_authority(&authority) {
                    let _ = reply.send(Err(error));
                } else if self.lifecycle == ActorLifecycle::Retired {
                    let _ = reply.send(Ok(()));
                } else if fail_if_busy && self.has_running_jobs() {
                    let _ = reply.send(Err(CowshedError::conflict(
                        "workspace port capacity cannot change while jobs are active",
                        "request port capacity before starting the workload",
                    )));
                } else {
                    if self.lifecycle == ActorLifecycle::Running {
                        self.lifecycle = ActorLifecycle::Quiescing;
                    }
                    self.quiesce_waiters.push(reply);
                }
            }
            Command::NameBuildVolume {
                authority,
                build_volume,
                reply,
            } => {
                let named = self.validate_authority(&authority).and_then(|()| {
                    if let Some(mount) = build_volume.as_deref()
                        && !self.build_volume_layout.as_ref().is_some_and(|layout| {
                            layout.volume_at(mount).is_some()
                        })
                    {
                        return Err(CowshedError::integrity(
                            format!(
                                "build volume label request names {}, outside this project's build volumes",
                                mount.display()
                            ),
                            "cowshed doctor --json",
                        ));
                    }
                    Ok(())
                });
                if named.is_ok() {
                    self.relabel(false, build_volume.as_deref());
                }
                let _ = reply.send(named);
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
                    && self.jobs.values().all(JobStateRecord::terminal)
                    && self.nx_daemon.as_ref().is_none_or(|keeper| keeper.settled);
                let _ = reply.send(Ok(idle));
            }
        }
    }

    /// Replace the group ledger with the groups of the jobs whose terminal record is not durable,
    /// or whose end was never observed, and the inherited unresolved groups processes still hold.
    /// One found released is dropped for good: its id may later name a stranger's group, which
    /// this ledger has no claim to.
    fn record_groups(&mut self) {
        let Some(path) = &self.group_ledger else {
            return;
        };
        let groups: Vec<(u64, super::job_groups::GroupLeader)> = self
            .jobs
            .iter()
            // A job sealed after a wait failure may have left its group running: nothing saw it
            // end, so the ledger keeps it for whoever finds this supervisor gone.
            .filter(|(_, job)| !job.sealed() || job.wait_failure.is_some())
            .filter_map(|(job_id, job)| Some((job_id.get(), job.birth.as_ref()?.leader()?)))
            .collect();
        match super::job_groups::record(path, &groups, &self.inherited_groups) {
            Ok(recorded) => {
                for error in &recorded.uninspected {
                    eprintln!(
                        "cowshed: an inherited unresolved process group in {} could not be \
                         inspected and is carried as it was: {error}",
                        path.display()
                    );
                }
                self.inherited_groups = recorded.carried;
            }
            Err(error) => {
                // The jobs run either way; what is lost is only the means to end them should
                // this supervisor die, which is worth saying where the daemon log shows it.
                eprintln!(
                    "cowshed: cannot record the job process groups at {}: {error}",
                    path.display()
                );
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
            admission_identity: name.is_none().then(Uuid::new_v4),
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

    /// Name the checkout's volumes in the background (see [`VolumeLabels`]): its workspace
    /// volume when `workspace`, and the build volume mounted at `build` unless this supervisor
    /// already named that one. A Disk Arbitration rename is queued only when the file system's
    /// own name differs; the outcome goes to stderr.
    fn relabel(&mut self, workspace: bool, build: Option<&Path>) {
        let Some(labels) = &self.volume_labels else {
            return;
        };
        let mut volumes = Vec::new();
        if workspace {
            volumes.push((
                "volume",
                self.workspace_root.clone(),
                labels.workspace.clone(),
            ));
        }
        if let Some(build) = build
            && self.labelled_build.as_deref() != Some(build)
            && self
                .build_volume_layout
                .as_ref()
                .is_some_and(|layout| layout.volume_at(build).is_some())
        {
            self.labelled_build = Some(build.to_owned());
            volumes.push(("build volume", build.to_owned(), labels.build.clone()));
        }
        if volumes.is_empty() {
            return;
        }
        let labeller = Arc::clone(&labels.labeller);
        let name = self.authority.workspace.clone();
        let checkout = self.workspace_root.clone();
        let layout = self.build_volume_layout.clone();
        self.labelling.spawn_blocking(move || {
            for (what, mount, label) in volumes {
                // Links are not authority. After a land, this checkout may still link the
                // adopted volume, but its sidecar belongs to the target, so it cannot name it.
                if what == "build volume" {
                    let Some(layout) = &layout else {
                        continue;
                    };
                    match layout.grant(&name, &checkout) {
                        Ok(Some(granted)) if granted == mount => {}
                        Ok(_) => continue,
                        Err(error) => {
                            eprintln!(
                                "cowshed: could not authorize workspace {name}'s build volume label at {}: {error}",
                                mount.display()
                            );
                            continue;
                        }
                    }
                }
                let started = Instant::now();
                match labeller.ensure_label(&mount, &label) {
                    Ok(Labelled::Renamed) => eprintln!(
                        "cowshed: named workspace {name}'s {what} {label:?} in {:?}",
                        started.elapsed()
                    ),
                    Ok(Labelled::Already | Labelled::NotMounted) => {}
                    Err(error) => eprintln!(
                        "cowshed: could not name workspace {name}'s {what} at {} {label:?} after {:?}: {error}",
                        mount.display(),
                        started.elapsed()
                    ),
                }
            }
        });
    }

    /// What `key` binds: `request` as the caller sent it, in `session`, before the session lends
    /// it a cwd or an environment. Withheld credential names are no part of it, as they are no
    /// part of any job's environment, and so never reach the job record.
    fn job_admission(
        &self,
        key: AdmissionKey,
        request: &ExecRequest,
        session: Option<&SessionToken>,
    ) -> Result<JobAdmission> {
        let stdin = match &request.stdin {
            StdinSource::Empty => AdmittedStdin::Empty,
            StdinSource::Inline(bytes) => AdmittedStdin::Inline {
                sha256: Sha256Digest::compute(bytes),
                bytes: u64::try_from(bytes.len())
                    .map_err(|_| CowshedError::internal("inline stdin length exceeds u64"))?,
            },
            StdinSource::Stream(_) => AdmittedStdin::Stream,
            StdinSource::WorkspaceFile(path) => AdmittedStdin::WorkspaceFile(path.clone()),
        };
        Ok(JobAdmission {
            key,
            cwd: request.cwd.clone(),
            mode: request.mode,
            env: request
                .env
                .iter()
                .filter(|(name, _)| !self.credential_env_names.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            stdin,
            session: match session {
                None => AdmittedSession::None,
                Some(token) => match &token.name {
                    Some(name) => AdmittedSession::Named(name.clone()),
                    None => AdmittedSession::Unnamed(
                        self.sessions
                            .get(&token.identity)
                            .expect("validated session exists")
                            .admission_identity
                            .expect("unnamed session has an admission identity"),
                    ),
                },
            },
            stdout_copy: request.stdout_copy.clone(),
            stderr_copy: request.stderr_copy.clone(),
        })
    }

    /// Compare the authored request by borrowing it; only a new admission needs owned metadata.
    fn admission_differences(
        &self,
        admitted: &JobAdmission,
        command: &ExecCommand,
        request: &ExecRequest,
        session: Option<&SessionToken>,
    ) -> Vec<AdmissionField> {
        let mut env_entries = 0;
        let mut env_matches = true;
        for (name, value) in &request.env {
            if self.credential_env_names.contains(name) {
                continue;
            }
            env_entries += 1;
            if admitted.env.get(name) != Some(value) {
                env_matches = false;
                break;
            }
        }
        env_matches &= env_entries == admitted.env.len();
        let stdin_matches = match &request.stdin {
            StdinSource::Empty => matches!(admitted.stdin, AdmittedStdin::Empty),
            StdinSource::Inline(input) => matches!(
                &admitted.stdin,
                AdmittedStdin::Inline { sha256, bytes }
                    if *bytes == byte_count(input.len()) && *sha256 == Sha256Digest::compute(input)
            ),
            StdinSource::Stream(_) => matches!(admitted.stdin, AdmittedStdin::Stream),
            StdinSource::WorkspaceFile(path) => matches!(
                &admitted.stdin,
                AdmittedStdin::WorkspaceFile(known) if known == path
            ),
        };
        let session_matches = match &admitted.session {
            AdmittedSession::None => session.is_none(),
            AdmittedSession::Named(name) => {
                session.and_then(|token| token.name.as_deref()) == Some(name.as_str())
            }
            AdmittedSession::Unnamed(identity) => {
                session
                    .and_then(|token| self.sessions.get(&token.identity))
                    .and_then(|state| state.admission_identity)
                    == Some(*identity)
            }
        };
        [
            (AdmissionField::Command, command != &request.command),
            (AdmissionField::Cwd, admitted.cwd != request.cwd),
            (AdmissionField::Mode, admitted.mode != request.mode),
            (AdmissionField::Env, !env_matches),
            (AdmissionField::Stdin, !stdin_matches),
            (AdmissionField::Session, !session_matches),
            (
                AdmissionField::StdoutCopy,
                admitted.stdout_copy != request.stdout_copy,
            ),
            (
                AdmissionField::StderrCopy,
                admitted.stderr_copy != request.stderr_copy,
            ),
        ]
        .into_iter()
        .filter_map(|(field, differs)| differs.then_some(field))
        .collect()
    }

    /// Admit and spawn one job, granted `build_volume`.
    async fn admit_exec(
        &mut self,
        authority: WorkspaceAuthoritySnapshot,
        session: Option<SessionToken>,
        build_volume: Option<PathBuf>,
        request: ExecRequest,
        background: bool,
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
        // A key's repeat answers before anything else is touched: its session keeps the cwd and
        // environment it had, no build volume is held and no job id is taken.
        let admission = match &request.admission_key {
            None => None,
            Some(key) => match self.artifacts.admitted(key) {
                AdmissionLookup::Absent => {
                    Some(self.job_admission(key.clone(), &request, session.as_ref())?)
                }
                AdmissionLookup::Admitted {
                    job_id,
                    command,
                    admission: admitted,
                } => {
                    let fields =
                        self.admission_differences(admitted, command, &request, session.as_ref());
                    return if !fields.is_empty() {
                        Err(CowshedError::admission_refusal(
                            AdmissionRefusal::KeyConflict { job_id, fields },
                        ))
                    } else if matches!(request.stdin, StdinSource::Stream(_)) {
                        Err(CowshedError::admission_refusal(
                            AdmissionRefusal::StdinBound { job_id },
                        ))
                    } else {
                        Ok(job_id)
                    };
                }
                AdmissionLookup::Unprovable { set_aside } => {
                    return Err(CowshedError::admission_refusal(
                        AdmissionRefusal::Unprovable {
                            set_aside: set_aside.to_path_buf(),
                        },
                    ));
                }
            },
        };
        let build_hold = self.hold_build_volume(build_volume.as_deref())?;
        // A volume the link moved to (a land, a refork) was another checkout's: it takes this
        // checkout's name, once.
        self.relabel(false, build_volume.as_deref());
        // The checkout's build volume moved (a land adopted another, a mount re-pointed a stale
        // link): this job and every later one, the Nx daemon keeper's included, run with the
        // volume the controller resolved. A running job keeps the profile it was spawned with,
        // so it stays on its old volume until it exits; nothing is relaunched.
        if build_volume != self.policy.ceiling().build_volume_mount {
            let _span = crate::timing::span("admit", "build-volume-profile");
            self.policy = SandboxPolicy::render(SandboxConfig {
                build_volume_mount: build_volume,
                ..self.policy.ceiling().clone()
            })?;
        }
        let ExecRequest {
            command,
            cwd,
            mode,
            env,
            trace,
            stdin,
            stdout_copy,
            stderr_copy,
            admission_key: _,
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
            self.artifacts
                .admit(job_id, self.authority.grant_revision, &command, admission)?;
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
            resources: None,
            exit: None,
            stdout: empty_stream(),
            stderr: empty_stream(),
            trace,
            output_limit: None,
            stdin: stdin_info,
            failure: None,
        };
        if let Some(job_spans) = &self.job_spans {
            job_spans.record(&info, JobSpanEdge::Start);
        }
        // Read before the job's first process is dispatched, so nothing it writes precedes its
        // baseline: a warm host may start the command before its start is reported here.
        let volumes = {
            let _span = crate::timing::span("admit", "volume-baseline");
            VolumeBaseline::admitted(
                self.workspace_volume.as_ref(),
                self.policy.ceiling().build_volume_mount.as_deref(),
            )
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
            stdout: VecDeque::new(),
            stderr: VecDeque::new(),
            stdout_tally: StreamTally::default(),
            stderr_tally: StreamTally::default(),
            stdout_eof: false,
            stderr_eof: false,
            exit: None,
            wait_failure: None,
            conclusion: None,
            birth: None,
            sampling: Sampling::Unowned,
            volumes,
            output_limit: None,
            kill_reason: None,
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
            build_hold,
        };
        match spawn {
            Ok(process) => {
                if let Some(owned) = process.process() {
                    // A one-shot command is the job's first process.
                    own_process(&mut job, owned.clone());
                    adopt_birth(&mut job, self.group_ledger.is_some(), owned.birth.clone());
                }
                job.process = Some(process);
                let started = job.birth.is_some();
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

    /// Hold the build volume mounted at `grant` for one job (16_build_volumes.md, "Garbage
    /// collection"): a land that moves the checkout's link meanwhile leaves the volume unlinked,
    /// and the hold is what tells its release that this job still runs there. Refused while a
    /// release claims the volume, since the job would start on a volume being deleted.
    fn hold_build_volume(
        &self,
        grant: Option<&Path>,
    ) -> Result<Option<crate::build_volume::BuildVolumeHold>> {
        let Some(grant) = grant else {
            return Ok(None);
        };
        let Some(layout) = &self.build_volume_layout else {
            return Err(CowshedError::integrity(
                format!(
                    "the supervisor of {} was handed build volume {} but has no controller-owned \
                     build-volume layout to hold it by",
                    self.authority.workspace,
                    grant.display()
                ),
                "restart this workspace's supervisor with the current cowshed",
            ));
        };
        let id = layout.volume_at(grant).ok_or_else(|| {
            CowshedError::integrity(
                format!(
                    "build volume grant {} is not one of this project's build volumes",
                    grant.display()
                ),
                "cowshed doctor --json",
            )
        })?;
        match layout.hold(&id) {
            Ok(hold) => Ok(Some(hold)),
            Err(error) if error.kind() == std::io::ErrorKind::ResourceBusy => {
                Err(CowshedError::conflict(
                    format!(
                        "{}'s job cannot start on build volume {id}: {error}",
                        self.authority.workspace
                    ),
                    "retry: the next admission resolves the checkout's current build volume",
                ))
            }
            Err(error) => Err(CowshedError::environment_missing(
                format!(
                    "cannot hold build volume {id} at {}: {error}",
                    layout.hold_path(&id).display()
                ),
                "check the cowshed store's permissions and retry",
            )),
        }
    }

    fn current_build_volume_grant(&self) -> Result<Option<PathBuf>> {
        match &self.build_volume_layout {
            Some(layout) => layout.grant(&self.authority.workspace, &self.workspace_root),
            None => match crate::build_volume::link::linked(&self.workspace_root)? {
                None => Ok(None),
                Some(_) => Err(CowshedError::integrity(
                    format!(
                        "the supervisor of {} has no controller-owned build-volume layout",
                        self.authority.workspace,
                    ),
                    "restart this workspace's supervisor with the current cowshed",
                )),
            },
        }
    }

    /// One probe of a shed's Nx daemon: nothing while the keeper's start job runs; once it has
    /// ended, say how it went unless it left the daemon live; then judge whether the shed still
    /// wants a daemon ([`NxDaemonKeeper::judge`]), stopping one it no longer wants; start the
    /// daemon, inside the sandbox, when the shed wants one and the probe finds none live. Nobody
    /// waits for the start, so a start that is refused is said where the supervisor's own
    /// failures go, and the next probe tries again.
    async fn keep_nx_daemon(&mut self) {
        let Some(keeper) = &mut self.nx_daemon else {
            return;
        };
        if let Some(job_id) = keeper.starting
            && self.jobs.get(&job_id).is_some_and(|job| !job.terminal())
        {
            return;
        }
        let ended = keeper.starting.take();
        let (read_write, _) = self
            .policy
            .child(crate::api::dto::RunSandboxMode::ReadWrite);
        let Some(project_root) = super::nx_daemon::kept_project(read_write).map(Path::to_path_buf)
        else {
            // No Nx to keep: nothing for the supervisor to wait for.
            keeper.settled = true;
            return;
        };
        let record = crate::capabilities::nx::daemon_record(&project_root);
        let daemon = super::nx_daemon::probe(&record);
        if let Some(job_id) = ended {
            let job = self.jobs.get(&job_id).map(|job| &job.info);
            super::nx_daemon::report_start(job_id, job, &project_root, daemon);
        }
        let working =
            !self.named_sessions.is_empty() || self.jobs.values().any(|job| !job.terminal());
        if keeper.judge(&project_root, working, daemon).await == Verdict::Release {
            return;
        }
        if daemon == Probe::Live {
            return;
        }
        let authority = self.authority.clone();
        let started = match self.current_build_volume_grant().and_then(|grant| {
            super::nx_daemon::start_request(&self.workspace_root, &project_root)
                .map(|request| (grant, request))
        }) {
            Ok((grant, request)) => self.admit_exec(authority, None, grant, request, true).await,
            Err(error) => Err(error),
        };
        match started {
            Ok(job_id) => {
                if let Some(keeper) = &mut self.nx_daemon {
                    keeper.starting = Some(job_id);
                }
            }
            Err(error) => eprintln!(
                "cowshed: the Nx daemon of {} did not start in the sandbox: {}; the next probe in \
                 {}s tries again",
                project_root.display(),
                error.message,
                PROBE_INTERVAL.as_secs()
            ),
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
        if job.terminal() {
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
        if job.sealed() {
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

    fn tail(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        cursor: Option<JobJournalCursor>,
        limits: JobTailLimits,
        reply: oneshot::Sender<Result<JobTail>>,
    ) {
        if let Err(error) = self.validate_authority(authority) {
            let _ = reply.send(Err(error));
            return;
        }
        let (stdout, stderr) = match self.job_mut(job_id) {
            Ok(job) if !job.sealed() => {
                let lengths = (job.stdout_tally.bytes(), job.stderr_tally.bytes());
                let _ = reply.send(job_tail(cursor, limits, lengths, |stream, range| {
                    read_live_range(job.stream(stream).0, range)
                }));
                return;
            }
            // The sealed artifact is the authoritative copy of both streams.
            Ok(job) => (job.info.stdout.clone(), job.info.stderr.clone()),
            // A job an earlier supervisor of this incarnation sealed: its artifact answers.
            Err(_) => match self.artifacts.sealed(job_id) {
                Some(sealed) => (sealed.stdout, sealed.stderr),
                None => {
                    let _ = reply.send(Err(not_found_job(job_id)));
                    return;
                }
            },
        };
        // Off the actor thread, as a sealed log read is: it may read a protected file.
        let workspace_root = self.workspace_root.clone();
        tokio::task::spawn_blocking(move || {
            let _ = reply.send(sealed_tail(
                &workspace_root,
                &stdout,
                &stderr,
                cursor,
                limits,
            ));
        });
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
                    end_job_stdin(job);
                }
            }
            ProcessEvent::WaitFailed { job_id, error } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                if job.terminal() {
                    // The job's record is final; what failed is the release of its process,
                    // whose group is no longer provably the job's.
                    eprintln!(
                        "cowshed: job {}'s process was not released after the job concluded: {}",
                        job_id.get(),
                        error.message
                    );
                    return;
                }
                // `exit` stays `None`: there is no truthful status to publish. The job seals as
                // `Failed` and every waiter gets the integrity error, so a still-running child
                // can never be read as a completed one.
                job.wait_failure = Some(error);
                job.kill_reason = Some(KillReason::WaitFailure);
                // The child's end was not observed, so it may still be running. Kill the group
                // while the handle still holds it: an unobservable process must not outlive its
                // record. The spawn sink's own wait task kills too, because it is the one
                // component that is still alive if this actor has already stopped.
                if let Some(process) = job.process.as_mut() {
                    let _ = process.signal_process_tree(ProcessSignal::Kill);
                }
                end_job_stdin(job);
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
            ProcessEvent::Activating { job_id, process } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    own_process(job, process);
                }
            }
            #[cfg(target_os = "macos")]
            ProcessEvent::ActivationEnded { job_id, usage } => {
                // An ended job's sampling stays as it ended.
                if let Some(sampler) = self
                    .jobs
                    .get_mut(&job_id)
                    .and_then(|job| job.sampling.live())
                {
                    sampler.rusage().end(usage);
                }
            }
            ProcessEvent::Started { job_id, process } => {
                let ledger = self.group_ledger.is_some();
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    let birth = process.birth.clone();
                    own_process(job, process);
                    adopt_birth(job, ledger, birth);
                    self.record_groups();
                }
            }
            ProcessEvent::LaunchFailed { job_id, error } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                if job.terminal() {
                    return;
                }
                // The same terminal shape as a spawn that failed synchronously; the job's
                // stderr already carries the reason.
                job.exit = Some(ExitStatus::Exited {
                    code: error.exec_wrapper_exit_code().into(),
                });
                job.kill_reason = Some(KillReason::SpawnFailure);
                end_job_stdin(job);
            }
            ProcessEvent::ScriptSyntax { job_id } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                if job.terminal() {
                    return;
                }
                // Bash's own status for a script that does not parse.
                job.exit = Some(ExitStatus::Exited { code: 2 });
                job.kill_reason = Some(KillReason::ScriptSyntax);
                end_job_stdin(job);
            }
            ProcessEvent::Escalate { job_id } => {
                if let Some(job) = self.jobs.get_mut(&job_id)
                    && !job.terminal()
                    && let Some(process) = job.process.as_mut()
                {
                    let _ = process.signal_process_tree(ProcessSignal::Kill);
                }
            }
            ProcessEvent::SignalFailed { job_id, error } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                // The job runs on: whoever asked for it to end is told it was not signalled, and
                // the daemon log says so for the ends nobody waits on (retirement, output limit).
                eprintln!(
                    "cowshed: a signal for job {} did not reach it: {}",
                    job_id.get(),
                    error.message
                );
                for waiter in job.kill_waiters.drain(..) {
                    let _ = waiter.send(Err(error.clone()));
                }
            }
        }
    }

    fn process_output(&mut self, job_id: JobId, stream: StreamKind, bytes: Bytes) {
        let Some(job) = self.jobs.get(&job_id) else {
            return;
        };
        if job.terminal() {
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
                            job.stdout_tally.admit(&accepted);
                            job.stdout.push_back(accepted);
                        }
                        StreamKind::Stderr => {
                            job.stderr_tally.admit(&accepted);
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
                // the terminal guard in `process_output`.
                (!job.terminal()
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
        if job.terminal() {
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
        let duration_ms = job
            .started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        // The terminal sample, frozen into the job's record; none for a job that never owned a
        // process, or whose last observation failed -- a read says which.
        job.info.resources = job
            .sampling
            .freeze(|sampler| observe(sampler, &job.volumes, job.stdout_tally, job.stderr_tally));
        let ending = JobEnding {
            state,
            exit: job.exit.clone(),
            duration_ms: Some(duration_ms),
            resources: job.info.resources.clone(),
        };
        let seal_span = crate::timing::span("seal", "record");
        let sealed = self.artifacts.seal(
            job_id,
            ending,
            job.stdout_copy.take(),
            job.stderr_copy.take(),
        );
        drop(seal_span);
        // The child's end is a fact whatever becomes of its record, so the job concludes on
        // every path below and every waiter is answered. A job left running after its artifact
        // was consumed has nobody to finish it: later waits and kills queue forever and
        // retirement never sees the workspace idle.
        let conclusion = match sealed {
            Ok(seal) => {
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
                job.info.stdout = seal.stdout;
                job.info.stderr = seal.stderr;
                job.info.output_limit = seal.output_limit;
                Conclusion::Sealed {
                    commitment: commitment.err(),
                    publication: seal.publication_failure,
                }
            }
            Err(error) => Conclusion::Unsealed(error),
        };
        job.conclusion = Some(conclusion);
        // The job concluded: nothing signals its group any more, and its leader is reaped.
        job.process = None;
        // Nor does it run on its build volume any more: a release may take it.
        job.build_hold = None;
        job.info.state = state;
        job.info.duration_ms = Some(duration_ms);
        job.info.exit = job.exit.clone();
        // The span ends before any waiter learns of the end, so a span read after a wait has it.
        if let Some(job_spans) = &self.job_spans {
            job_spans.record(&job.info, JobSpanEdge::end(state, job.exit.as_ref()));
        }
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
        if !job.sealed() {
            // Without a durable record the actor's copy is the output it serves, and the ledger
            // keeps the group for the workspace's next supervisor to end.
            return;
        }
        // Release the actor's copy of the output. The store already holds every byte, under a
        // committed digest, and its per-job quota is a gigabyte -- so retaining this second copy
        // for the supervisor's lifetime is growth with no closed form, and a workspace that
        // execs often exhausts memory while its disk quota still holds. Ordered after the
        // waiters so a follower that is mid-stream is served from the live deque first.
        job.stdout = VecDeque::new();
        job.stderr = VecDeque::new();
        if job.birth.is_some() {
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
            self.labelling.abort_all();
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
    let next_offset = len.min(offset.saturating_add(byte_count(MAX_LOG_READ)));
    Ok(Some(LogChunk {
        bytes: Bytes::from(read_live_range(chunks, offset..next_offset)?),
        next_offset,
        eof: (eof || job.terminal()) && next_offset == len,
    }))
}

/// The admitted bytes `range` names of a live stream, out of the chunks the actor retains. The
/// chunks hold every admitted byte, so a range they fall short of is an internal fault, never a
/// shorter answer a caller would take for the whole range.
fn read_live_range(chunks: &VecDeque<Bytes>, range: Range<u64>) -> Result<Vec<u8>> {
    let want = usize::try_from(range.end - range.start)
        .map_err(|_| CowshedError::internal("log range exceeds platform range"))?;
    let mut skip = range.start;
    let mut output = Vec::with_capacity(want);
    for chunk in chunks {
        if output.len() == want {
            break;
        }
        let chunk_len = byte_count(chunk.len());
        if skip >= chunk_len {
            skip -= chunk_len;
            continue;
        }
        let start = usize::try_from(skip)
            .map_err(|_| CowshedError::internal("log offset exceeds platform range"))?;
        skip = 0;
        let take = (want - output.len()).min(chunk.len() - start);
        output.extend_from_slice(&chunk[start..start + take]);
    }
    if output.len() != want {
        return Err(CowshedError::internal(format!(
            "retained output holds {} of the {want} admitted bytes from offset {}",
            output.len(),
            range.start
        )));
    }
    Ok(output)
}

/// Where one stream's slice of a tail sits.
#[derive(Clone, Copy, Debug)]
enum TailAnchor {
    /// The start of what was admitted after this many bytes.
    After(u64),
    /// The end of what was admitted.
    End,
}

/// One stream's tail before it is read: the admitted window to read, and where it sits.
struct TailWindow {
    anchor: TailAnchor,
    len: u64,
    range: Range<u64>,
}

impl TailWindow {
    /// The window of at most `max` admitted bytes a tail of `stream` reads: after the anchor's
    /// cursor, or ending at the stream's `len` admitted bytes. A cursor past them is a usage
    /// error.
    fn new(stream: StreamKind, len: u64, anchor: TailAnchor, max: u64) -> Result<Self> {
        let range = match anchor {
            TailAnchor::After(cursor) if cursor > len => {
                let name = match stream {
                    StreamKind::Stdout => "stdout",
                    StreamKind::Stderr => "stderr",
                };
                return Err(CowshedError::usage(
                    format!("tail cursor {cursor} is past the {len} admitted {name} bytes"),
                    "resume from a cursor a tail of this job returned",
                ));
            }
            TailAnchor::After(cursor) => cursor..len.min(cursor.saturating_add(max)),
            TailAnchor::End => len.saturating_sub(max)..len,
        };
        Ok(Self { anchor, len, range })
    }

    /// The window's `bytes` cut to `lines` lines: the first ones after a cursor, the last ones
    /// at the end. Lines cut the byte window, so the slice never exceeds the byte limit.
    fn cut(self, mut bytes: Vec<u8>, lines: u32) -> StreamTail {
        match self.anchor {
            TailAnchor::After(cursor) => {
                bytes.truncate(first_lines_len(&bytes, lines));
                let next = cursor + byte_count(bytes.len());
                StreamTail {
                    bytes,
                    next,
                    truncated: next < self.len,
                }
            }
            TailAnchor::End => {
                let skip = last_lines_start(&bytes, lines);
                bytes.drain(..skip);
                StreamTail {
                    bytes,
                    next: self.len,
                    truncated: self.range.start + byte_count(skip) > 0,
                }
            }
        }
    }
}

/// One stream's slice of a tail.
struct StreamTail {
    bytes: Vec<u8>,
    next: u64,
    truncated: bool,
}

/// A tail of both streams, whose admitted lengths are `lengths` and whose admitted bytes `read`
/// returns exactly, by range. Both cursors are checked before either stream is read, so a
/// refused tail reads nothing; the two streams are then cut independently under the same limits.
fn job_tail(
    cursor: Option<JobJournalCursor>,
    limits: JobTailLimits,
    lengths: (u64, u64),
    mut read: impl FnMut(StreamKind, Range<u64>) -> Result<Vec<u8>>,
) -> Result<JobTail> {
    let max = u64::from(limits.bytes_per_stream.get());
    let lines = limits.lines_per_stream.get();
    let anchor = |position: Option<u64>| position.map_or(TailAnchor::End, TailAnchor::After);
    let stdout = TailWindow::new(
        StreamKind::Stdout,
        lengths.0,
        anchor(cursor.map(|cursor| cursor.stdout)),
        max,
    )?;
    let stderr = TailWindow::new(
        StreamKind::Stderr,
        lengths.1,
        anchor(cursor.map(|cursor| cursor.stderr)),
        max,
    )?;
    let stdout_bytes = read(StreamKind::Stdout, stdout.range.clone())?;
    let stderr_bytes = read(StreamKind::Stderr, stderr.range.clone())?;
    let stdout = stdout.cut(stdout_bytes, lines);
    let stderr = stderr.cut(stderr_bytes, lines);
    let data = |bytes: Vec<u8>| {
        BinaryData::new(bytes)
            .map_err(|error| CowshedError::internal(format!("tail slice is unbounded: {error}")))
    };
    Ok(JobTail {
        next: JobJournalCursor {
            stdout: stdout.next,
            stderr: stderr.next,
        },
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
        stdout: data(stdout.bytes)?,
        stderr: data(stderr.bytes)?,
    })
}

/// The length of the first `lines` lines of `bytes`; all of it when it holds no more.
fn first_lines_len(bytes: &[u8], lines: u32) -> usize {
    let mut seen = 0_u32;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            seen += 1;
            if seen == lines {
                return index + 1;
            }
        }
    }
    bytes.len()
}

/// Where the last `lines` lines of `bytes` start; zero when it holds no more. A newline followed
/// by more bytes starts a line, so a trailing newline ends the last line and starts none.
fn last_lines_start(bytes: &[u8], lines: u32) -> usize {
    let Some((_, body)) = bytes.split_last() else {
        return 0;
    };
    let mut seen = 0_u32;
    for (index, byte) in body.iter().enumerate().rev() {
        if *byte == b'\n' {
            seen += 1;
            if seen == lines {
                return index + 1;
            }
        }
    }
    0
}

/// A tail of a sealed job, read out of the artifact store.
fn sealed_tail(
    workspace_root: &Path,
    stdout: &StreamInfo,
    stderr: &StreamInfo,
    cursor: Option<JobJournalCursor>,
    limits: JobTailLimits,
) -> Result<JobTail> {
    job_tail(
        cursor,
        limits,
        (stdout.bytes, stderr.bytes),
        |stream, range| {
            let info = match stream {
                StreamKind::Stdout => stdout,
                StreamKind::Stderr => stderr,
            };
            read_sealed_range(workspace_root, info, range)
        },
    )
}

/// The job owns `process`: its first process starts its sampling, and a later one -- its command,
/// after its activation -- takes the lead. Either is observed at once as the job's latest sample.
/// An ended job's sampling stays as it ended, whatever arrives after.
fn own_process(job: &mut JobStateRecord, process: OwnedProcess) {
    if job.terminal() {
        return;
    }
    let job_id = job.info.job_id;
    if let Some(sampler) = job.sampling.own(job_id, process) {
        match observe(sampler, &job.volumes, job.stdout_tally, job.stderr_tally) {
            Ok(sample) => job.info.resources = Some(sample),
            // The latest sample stands, dated by its own `sampledAt`; a read reports the failure.
            Err(error) => eprintln!(
                "cowshed: job {} was not sampled: {}",
                job_id.get(),
                error.message
            ),
        }
    }
}

/// Observe the job's processes and volumes now, with its streams as admitted so far: the one
/// imperative step of a sample. Each member's resident memory is read right after the
/// membership; a member that exited in between holds nothing and is no member any more. Each
/// volume answers for itself: one that cannot be read is its own unavailability, never the
/// sample's failure.
fn observe(
    sampler: &mut JobSampler,
    volumes: &VolumeBaseline<HostVolume>,
    stdout: StreamTally,
    stderr: StreamTally,
) -> Result<JobResourceSample> {
    let sampled_at = utc_now()?;
    let leader = sampler.leader().pid();
    let unreadable = |what: &str, error: std::io::Error| {
        CowshedError::environment_missing(
            format!("the {what} of job group {leader} could not be read: {error}"),
            "the job runs on; read its resources again",
        )
    };
    let host = crate::host_load::read_host_load().map_err(|error| {
        CowshedError::environment_missing(
            format!("the sample's host observation could not be read: {error}"),
            "the command runs on; inspect the host's load and core reporting",
        )
    })?;
    let processes = super::job_groups::job_members(sampler.leader())
        .map_err(|error| unreadable("membership", error))?;
    let mut members = Vec::with_capacity(processes.len());
    for process in &processes {
        let pid = u32::try_from(process.pid()).map_err(|error| {
            CowshedError::internal(format!(
                "job group {leader} holds process {}, which is no pid: {error}",
                process.pid()
            ))
        })?;
        let Some(resident) = process
            .resident_bytes()
            .map_err(|error| unreadable("resident memory", error))?
        else {
            continue;
        };
        let resident = ResidentBytes::new(resident).map_err(|error| {
            CowshedError::internal(format!("process {pid} of job group {leader}: {error}"))
        })?;
        members.push(Member { pid, resident });
    }
    // The leader's own and reaped-children CPU, read after the membership: a member that exited
    // and was reaped since is already in its parent's children total.
    #[cfg(target_os = "macos")]
    let accounting = Some(
        sampler
            .rusage()
            .account(super::job_accounting::read_leader)
            .map_err(|error| unaccounted(leader, error))?,
    );
    #[cfg(not(target_os = "macos"))]
    let accounting = None;
    let volumes = volumes.sample(&HostVolumeStat);
    sampler.sample(Observation {
        now: Instant::now(),
        sampled_at,
        members,
        host,
        volumes,
        stdout,
        stderr,
        accounting,
    })
}

/// Why job group `leader`'s CPU totals cannot be stated: the kernel would not answer for a
/// leader, or the readings contradict each other.
#[cfg(target_os = "macos")]
fn unaccounted(leader: u32, error: super::job_accounting::AccountingError) -> CowshedError {
    use super::job_accounting::AccountingError;
    match error {
        AccountingError::Leader(_) => CowshedError::environment_missing(
            format!("the CPU accounting of job group {leader} is unavailable: {error}"),
            "the job runs on; its CPU totals cannot be stated",
        ),
        AccountingError::Regressed { .. }
        | AccountingError::OtherLeader { .. }
        | AccountingError::NoneCharged { .. }
        | AccountingError::Unclosed { .. }
        | AccountingError::Inexact(_) => CowshedError::internal(format!(
            "the CPU accounting of job group {leader} is inconsistent: {error}"
        )),
    }
}

/// A resources read: a running job observed now and kept as its latest sample, an ended job's
/// terminal sample or why it has none.
fn sample_job(job: &mut JobStateRecord) -> Result<JobResourceSample> {
    let sample = job.sampling.read(job.info.job_id, |sampler| {
        observe(sampler, &job.volumes, job.stdout_tally, job.stderr_tally)
    })?;
    if !job.terminal() {
        job.info.resources = Some(sample.clone());
    }
    Ok(sample)
}

/// A listening-ports read: the group the job's sampling follows while it runs -- its shell
/// activation's, then its command's -- and the group that last led once it ended. A job that
/// never owned a process has no group to read, and says so.
fn listening_ports(job: &JobStateRecord) -> Result<JobListeningPorts> {
    let job_id = job.info.job_id;
    let leader = job.sampling.leader().ok_or_else(|| {
        CowshedError::conflict(
            format!(
                "job {} owns no process: it has not started one, or ended before it did",
                job_id.get()
            ),
            "read its listening ports once the job has started; its status says how it ended",
        )
    })?;
    let sampled_at = utc_now()?;
    let ports = super::job_groups::job_listening_ports(leader).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "the listening sockets of job {}'s group {} could not be read: {error}",
                job_id.get(),
                leader.pid()
            ),
            "the job runs on; read its listening ports again",
        )
    })?;
    Ok(JobListeningPorts {
        job_id,
        sampled_at,
        ports: ports.into_iter().collect(),
    })
}

/// Record the job's process as its parent observed it. A leader no one could identify keeps the
/// job's pid but never enters the ledger, which loses only the means to end its group should this
/// supervisor die; a supervisor that keeps a ledger says so where the daemon log shows it.
fn adopt_birth(job: &mut JobStateRecord, ledger: bool, birth: Birth) {
    if ledger && let Birth::Unobserved { pid, reason } = &birth {
        eprintln!(
            "cowshed: job {} process group {pid} was not identified at its start: {reason}",
            job.info.job_id.get()
        );
    }
    job.info.pid = Some(birth.pid());
    job.birth = Some(birth);
}

/// The job's leader ended: its stdin closes for good and everything waiting on it is answered.
/// The process stays held until the job concludes, so its group can still be signalled.
fn end_job_stdin(job: &mut JobStateRecord) {
    if let Some(process) = job.process.as_mut() {
        process.end_stdin();
    }
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
fn read_sealed_chunk(workspace_root: &Path, stream: &StreamInfo, offset: u64) -> Result<LogChunk> {
    let len = stream.bytes;
    if offset > len {
        return Err(CowshedError::conflict(
            "log offset is beyond the captured stream",
            "restart the read at the returned stream length",
        ));
    }
    let next_offset = len.min(offset.saturating_add(byte_count(MAX_LOG_READ)));
    Ok(LogChunk {
        bytes: Bytes::from(read_sealed_range(
            workspace_root,
            stream,
            offset..next_offset,
        )?),
        next_offset,
        eof: next_offset == len,
    })
}

/// The bytes `range` names of a sealed stream, which must lie within its recorded length.
///
/// The store's reader verifies bytes and digest as it goes and cannot seek, so an offset is
/// reached by reading and discarding. Paging a whole stream is therefore quadratic in chunk
/// count -- which is the right trade against keeping every terminal job's full output resident
/// forever, and is bounded anyway by the same `MAX_LOG_READ` frame the live path uses. An inline
/// stream, which is every output under the store's inline cap, touches no filesystem at all.
fn read_sealed_range(
    workspace_root: &Path,
    stream: &StreamInfo,
    range: Range<u64>,
) -> Result<Vec<u8>> {
    if range.is_empty() {
        return Ok(Vec::new());
    }
    let mut reader = crate::storage::job_artifact::open_stream_reader(workspace_root, stream)
        .map_err(map_artifact_error)?;
    if range.start > 0 {
        // The skipped prefix is verified and dropped through a fixed scratch on the stack.
        let mut discard = [0_u8; 16 * 1024];
        let mut skipped = 0_u64;
        while skipped < range.start {
            let want = usize::try_from(range.start - skipped)
                .unwrap_or(discard.len())
                .min(discard.len());
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
    }
    let want = usize::try_from(range.end - range.start)
        .map_err(|_| CowshedError::internal("log range exceeds platform range"))?;
    let mut output = vec![0_u8; want];
    let mut filled = 0;
    while filled < want {
        let read = reader
            .read_chunk(&mut output[filled..])
            .map_err(map_artifact_error)?;
        if read == 0 {
            return Err(CowshedError::integrity(
                "sealed stream ended before its recorded length",
                "cowshed doctor --json",
            ));
        }
        filled += read;
    }
    Ok(output)
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
    utc_at_second(seconds)
}

/// The UTC second `seconds` after the epoch as the API's timestamp type.
pub(crate) fn utc_at_second(seconds: u64) -> Result<UtcTimestamp> {
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

pub(super) fn byte_count(value: usize) -> u64 {
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
/// conflict this build must not touch, and a key another writer bound first is the caller's to
/// look up; anything else is damage to the store.
pub(super) fn map_artifact_error(error: ArtifactError) -> CowshedError {
    match error {
        ArtifactError::NewerLayout { .. } => CowshedError::conflict(
            error.to_string(),
            "run the cowshed that wrote these records; this build is older",
        ),
        ArtifactError::AdmissionKeyTaken { admitted, .. } => CowshedError::conflict(
            error.to_string(),
            format!("reach job {} with jobByKey", admitted.get()),
        ),
        ArtifactError::AdmissionKeyUnprovable { set_aside } => {
            CowshedError::admission_refusal(AdmissionRefusal::Unprovable { set_aside })
        }
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
    use crate::fork_lock::Run as _;
    #[cfg(target_os = "macos")]
    use crate::sandbox::{RunSandboxMode, SandboxConfig, SandboxGrants};

    // Only macOS tests build a sandbox; on the Linux cross lint the helper would be dead code.
    // The mount root is a sibling of the fixture's home and stores, as it is on a host: its deny
    // covers other workspaces, never the host home a capability grants into.
    #[cfg(target_os = "macos")]
    fn sandbox_at(mount: &Path) -> SandboxConfig {
        SandboxConfig {
            home: mount.parent().expect("root").join("home"),
            mount_root: mount.parent().expect("root").join("mounts"),
            workspace_mount: mount.to_path_buf(),
            exec_temp_dir: mount.parent().expect("root").join("tmp"),
            port_block: crate::metadata::PortBlock::new(40_960, 16).expect("port block"),
            retained_port_blocks: Vec::new(),
            mode: RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            capabilities: Default::default(),
        }
    }

    fn scratch(test: &str) -> crate::temp_root::TempRoot {
        crate::temp_root::TempRoot::new(&format!("cowshed-toolchain-{test}"))
    }

    /// A workspace whose project uses cargo, Go, uv, Nix and Bun gets each tool's wiring from
    /// its capability, and every one-file TLS client finds a bundle that trusts both the platform
    /// roots and the workspace CA — from the files and variables the host prepared before the
    /// spawn, with no tracked file touched. No registry client is wired to a gateway route and
    /// the workspace token reaches the child only through its proxy variables: the bunfig and
    /// netrc an earlier wiring left, token and all, are gone.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_detected_project_gets_its_tools_wiring_and_a_trust_bundle() {
        let root = scratch("clients");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::create_dir_all(root.join("home")).expect("home");
        let token = format!("{}A", "canary".repeat(7));
        std::fs::write(mount.join(".cowshed/token"), &token).expect("token");
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
        // The wiring before this one left bun's global bunfig in the private config: the
        // loopback mirror registry and the workspace token.
        std::fs::create_dir_all(mount.join(".cowshed/config")).expect("private config");
        std::fs::write(
            mount.join(".cowshed/config/.bunfig.toml"),
            "[install]\nregistry = { url = \"http://127.0.0.1:49104/npm/\", token = \"stale-token\" }\n",
        )
        .expect("stale bunfig");
        for convention in [
            "Cargo.toml",
            "go.mod",
            "uv.lock",
            "flake.nix",
            "package.json",
            "bun.lock",
        ] {
            std::fs::write(mount.join(convention), "").expect("project convention");
        }
        let mut sandbox = sandbox_at(&mount);
        sandbox.port_block = crate::metadata::PortBlock::new(49_104, 16).expect("port block");
        sandbox
            .configure_capabilities()
            .expect("detect capabilities");
        let mut env = BTreeMap::new();
        env.insert(
            "NIX_CONFIG".to_owned(),
            "extra-experimental-features = flakes".to_owned(),
        );
        env.insert("CALLER_ONLY".to_owned(), "kept".to_owned());
        env.insert("NODE_USE_ENV_PROXY".to_owned(), "0".to_owned());
        env.insert("HOME".to_owned(), "/caller/home".to_owned());

        let environment = sandbox_environment(&sandbox, &env)
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
        fn tree_contains(directory: &Path, needle: &str) -> bool {
            std::fs::read_dir(directory)
                .expect("private directory")
                .any(|entry| {
                    let path = entry.expect("directory entry").path();
                    let kind = std::fs::symlink_metadata(&path).expect("entry metadata");
                    if kind.is_dir() {
                        tree_contains(&path, needle)
                    } else if kind.is_file() {
                        String::from_utf8_lossy(&std::fs::read(&path).expect("file bytes"))
                            .contains(needle)
                    } else {
                        false
                    }
                })
        }
        assert!(
            std::fs::symlink_metadata(private.join("config/.bunfig.toml")).is_err(),
            "the bunfig a previous wiring left, mirror registry and token alike, is gone"
        );
        for directory in ["config", "cache", "home"] {
            for needle in ["stale-token", token.as_str()] {
                assert!(
                    !tree_contains(&private.join(directory), needle),
                    "no file under {directory} carries a workspace token"
                );
            }
        }
        // The token's one channel to a registry client is its proxy userinfo: bun, cargo and Go
        // reach their public registries through the gateway's proxy endpoint.
        for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
            assert_eq!(
                vars.get(name).map(String::as_str),
                Some(format!("http://cowshed:{token}@127.0.0.1:49104").as_str()),
                "{name}"
            );
        }
        assert_eq!(
            vars.get("NODE_USE_ENV_PROXY").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            vars.get("XDG_CONFIG_HOME").map(PathBuf::from),
            Some(private.join("config"))
        );
        // Go's caches are named directly at the host's own defaults; there is no Go env file
        // and no Go policy.
        assert!(!vars.contains_key("GOENV"));
        assert!(!private.join("cache/go/env").exists());
        let home = root.join("home");
        assert_eq!(
            vars.get("GOMODCACHE").map(PathBuf::from),
            Some(home.join("go/pkg/mod"))
        );
        assert_eq!(
            vars.get("GOCACHE").map(PathBuf::from),
            Some(home.join("Library/Caches/go-build"))
        );
        assert_eq!(
            vars.get("CARGO_HOME").map(PathBuf::from),
            Some(home.join(".cargo"))
        );
        for cache in [".cargo/registry", ".cargo/git", "go/pkg/mod", ".cache/uv"] {
            assert!(home.join(cache).is_dir(), "{cache} is prepared");
        }
        assert_eq!(
            vars.get("CARGO_NET_GIT_FETCH_WITH_CLI").map(String::as_str),
            Some("true")
        );
        assert!(
            !private.join("home/.netrc").exists(),
            "the netrc a previous wiring left, token and all, is gone"
        );
    }

    /// A plain repository gets the core contract and nothing tool-specific: no tool's variables,
    /// its trust bundle only through the core's Git and OpenSSL anchors.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_plain_repository_gets_no_tool_wiring() {
        let root = scratch("plain");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::create_dir_all(root.join("home")).expect("home");
        std::fs::write(mount.join(".cowshed/token"), "A".repeat(43)).expect("token");
        std::fs::write(
            mount.join(".cowshed/ca.pem"),
            b"-----BEGIN CERTIFICATE-----\nWORKSPACE\n-----END CERTIFICATE-----\n",
        )
        .expect("workspace CA");
        std::fs::write(mount.join("README.md"), "plain\n").expect("plain file");
        let mut sandbox = sandbox_at(&mount);
        sandbox
            .configure_capabilities()
            .expect("detect capabilities");
        assert!(sandbox.capabilities.active.is_empty());

        let environment = sandbox_environment(&sandbox, &BTreeMap::new())
            .await
            .expect("environment");
        let names: Vec<String> = environment
            .child(&BTreeMap::new())
            .into_keys()
            .filter_map(|name| name.into_string().ok())
            .collect();
        for prefix in [
            "NX_", "CARGO_", "RUSTUP_", "GO", "UV_", "NIX_", "NODE_", "BUN_", "NPM_", "PNPM_",
            "SCCACHE_", "RUSTC_", "ZIG_", "GRADLE_",
        ] {
            assert!(
                !names.iter().any(|name| name.starts_with(prefix)),
                "a plain repository got {prefix}* wiring: {names:?}"
            );
        }
        let bundle = mount.join(".cowshed/ca-bundle.pem").into_os_string();
        let child = environment.child(&BTreeMap::new());
        for core in ["GIT_SSL_CAINFO", "SSL_CERT_FILE"] {
            assert_eq!(child.get(OsStr::new(core)), Some(&bundle), "{core}");
        }
        std::fs::remove_file(sandbox_runtime_link(&sandbox)).ok();
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
        std::fs::write(mount.join("Cargo.toml"), "").expect("cargo project");
        let sandbox = sandbox_at(&mount);
        std::fs::create_dir_all(&sandbox.home).expect("home");
        let caller = BTreeMap::from([
            ("RUSTC_WRAPPER".to_owned(), "/bin/false".to_owned()),
            ("SCCACHE_BASEDIR_CWD".to_owned(), "0".to_owned()),
            ("SCCACHE_BASEDIR".to_owned(), "/elsewhere".to_owned()),
        ]);
        // Detection reads the host's pinned client, so each case configures a fresh snapshot,
        // as a supervisor start does.
        let wiring = async || {
            let mut configured = sandbox.clone();
            configured
                .configure_capabilities()
                .expect("detect capabilities");
            let environment = sandbox_environment(&configured, &caller)
                .await
                .expect("environment");
            let child = environment.child(&caller);
            let mut pooled = environment.base();
            pooled.extend(environment.overlay(&caller));
            assert_eq!(
                pooled, child,
                "pooled and one-shot children see one environment"
            );
            [
                "RUSTC_WRAPPER",
                "SCCACHE_BASEDIR_CWD",
                "SCCACHE_BASEDIR",
                "CARGO_INCREMENTAL",
            ]
            .map(|name| child.get(OsStr::new(name)).cloned())
        };
        // A cargo project on a host with no pinned client builds without a wrapper, and the
        // caller's wrapper and request bases never reach the child either.
        let unwrapped = [None, None, None, None];
        assert_eq!(wiring().await, unwrapped, "no sccache pinned");

        let store_path = root.join("store/0000-sccache-cowshed");
        std::fs::create_dir_all(store_path.join("bin")).expect("store path");
        std::fs::write(store_path.join("bin/sccache"), b"").expect("program");
        let gc_root = crate::capabilities::sccache::gc_root(&sandbox.home);
        std::fs::create_dir_all(gc_root.parent().expect("parent")).expect("support directory");
        std::os::unix::fs::symlink(&store_path, &gc_root).expect("gc root");
        assert_eq!(
            wiring().await,
            [
                Some(store_path.join("bin/sccache").into_os_string()),
                Some(OsString::from("1")),
                Some(mount.clone().into_os_string()),
                None,
            ],
            "the pinned program"
        );

        std::fs::remove_dir_all(&store_path).expect("collect the store path");
        assert_eq!(wiring().await, unwrapped, "a collected store path");
        std::fs::remove_file(sandbox_runtime_link(&sandbox)).ok();
    }

    #[test]
    fn caller_ci_is_local_unless_a_real_runner_identifies_itself() {
        for marker in [None, Some(""), Some("false"), Some("true")] {
            let mut caller = BTreeMap::from([
                ("CI".to_owned(), "true".to_owned()),
                (
                    "CC_aarch64_apple_darwin".to_owned(),
                    "/usr/bin/clang".to_owned(),
                ),
                (
                    "CXX_aarch64_apple_darwin".to_owned(),
                    "/usr/bin/clang++".to_owned(),
                ),
                (
                    "AR_aarch64_apple_darwin".to_owned(),
                    "/usr/bin/ar".to_owned(),
                ),
            ]);
            if let Some(marker) = marker {
                caller.insert("GITHUB_ACTIONS".to_owned(), marker.to_owned());
            }
            let environment = SandboxEnvironment {
                owned: BTreeMap::new(),
                withheld: Vec::new(),
                defaults: BTreeMap::new(),
                appended: BTreeMap::new(),
            };
            let one_shot = environment.child(&caller);
            let warm: BTreeMap<_, _> = environment.overlay(&caller).into_iter().collect();
            assert_eq!(one_shot, warm, "{marker:?}");
            assert_eq!(
                one_shot.contains_key(OsStr::new("CI")),
                marker == Some("true")
            );
            for name in [
                "CC_aarch64_apple_darwin",
                "CXX_aarch64_apple_darwin",
                "AR_aarch64_apple_darwin",
            ] {
                assert_eq!(
                    one_shot.get(OsStr::new(name)),
                    caller.get(name).map(OsString::from).as_ref()
                );
            }
        }
    }

    /// Names the fixture workspace a re-executed probe describes; see
    /// [`environment_probe_describes_the_job_environment`].
    #[cfg(target_os = "macos")]
    const ENVIRONMENT_PROBE: &str = "COWSHED_ENVIRONMENT_PROBE";
    #[cfg(target_os = "macos")]
    const ENVIRONMENT_PROBE_LINE: &str = "environment-probe ";

    /// The half of the regression below that runs as a process of its own: the job environment of
    /// the workspace at `$COWSHED_ENVIRONMENT_PROBE`, computed by whatever environment started
    /// this process. Run without one, it has nothing to describe.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn environment_probe_describes_the_job_environment() {
        let Some(mount) = std::env::var_os(ENVIRONMENT_PROBE) else {
            return;
        };
        let sandbox = sandbox_at(Path::new(&mount));
        let environment = sandbox_environment(&sandbox, &BTreeMap::new())
            .await
            .expect("environment");
        for (name, value) in environment.child(&BTreeMap::new()) {
            println!(
                "{ENVIRONMENT_PROBE_LINE}{}={}",
                name.to_string_lossy(),
                value.to_string_lossy()
            );
        }
    }

    /// A job's environment is cowshed's contract and nothing else: a supervisor started from a
    /// shell full of another checkout's PATH, Nx, Python and Cargo settings, locale, Xcode
    /// selection and a home with its own Git and npm configuration hands its children exactly
    /// what one started with launchd's bare environment does.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_supervisor_started_from_a_foreign_shell_hands_children_none_of_it() {
        let root = scratch("stuffed-shell");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::create_dir_all(root.join("home")).expect("home");
        std::fs::write(mount.join(".cowshed/token"), "A".repeat(43)).expect("token");
        let foreign_home = root.join("foreign-home");
        std::fs::create_dir_all(&foreign_home).expect("foreign home");
        std::fs::write(foreign_home.join(".gitconfig"), "[user]\nname = foreign\n")
            .expect("foreign gitconfig");
        std::fs::write(
            foreign_home.join(".npmrc"),
            "registry=http://foreign.invalid/\n",
        )
        .expect("foreign npmrc");
        let foreign = foreign_home.to_string_lossy().into_owned();
        let stuffed = [
            (
                "PATH",
                "/nix/store/0000-foreign-checkout/bin:/etc/profiles/per-user/foreign/bin:\
                 /opt/foreign/bin:/Library/Foreign/bin:/usr/bin:/bin",
            ),
            ("HOME", foreign.as_str()),
            ("XDG_CONFIG_HOME", foreign.as_str()),
            ("DIRENV_CONFIG", foreign.as_str()),
            ("GIT_CONFIG_GLOBAL", foreign.as_str()),
            ("NPM_CONFIG_USERCONFIG", foreign.as_str()),
            ("CARGO_HOME", foreign.as_str()),
            ("VIRTUAL_ENV", foreign.as_str()),
            ("NX_DAEMON", "true"),
            ("NX_SOCKET_DIR", foreign.as_str()),
            ("NX_WORKSPACE_ROOT_PATH", foreign.as_str()),
            ("NX_CACHE_DIRECTORY", foreign.as_str()),
            ("LANG", "foreign.UTF-8"),
            ("LC_ALL", "foreign.UTF-8"),
            ("TERM", "foreign-term"),
            ("COLORTERM", "foreign"),
            (
                "DEVELOPER_DIR",
                "/Applications/Foreign.app/Contents/Developer",
            ),
            ("NIX_CONFIG", "access-tokens = github.com=foreign"),
            ("SSL_CERT_FILE", foreign.as_str()),
        ];
        let probe = |environment: &[(&str, &str)]| -> Vec<String> {
            let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "runtime::supervisor::workspace_toolchain_tests::environment_probe_describes_the_job_environment",
                    "--nocapture",
                ])
                .env_clear()
                .envs(environment.iter().copied())
                .env(ENVIRONMENT_PROBE, &mount).output_locked()
                .expect("run the environment probe");
            assert!(
                output.status.success(),
                "environment probe: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout)
                .expect("utf-8 probe output")
                .lines()
                .filter_map(|line| line.strip_prefix(ENVIRONMENT_PROBE_LINE))
                .map(str::to_owned)
                .collect()
        };

        let launchd = probe(&[("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")]);
        let foreign_shell = probe(&stuffed);
        assert!(
            launchd.iter().any(|line| line.starts_with("HOME=")),
            "the probe described a job environment: {launchd:?}"
        );
        assert_eq!(
            foreign_shell, launchd,
            "the starting environment reached the job"
        );
        assert!(
            !foreign_shell
                .iter()
                .any(|line| line.contains("foreign") || line.contains("Foreign")),
            "{foreign_shell:#?}"
        );
        let sandbox = sandbox_at(&mount);
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
    }

    /// Stores hand out port blocks independently, so two live workspaces can hold one base.
    /// Each still takes the link its own mount names, so neither retargets the other's: spawns
    /// of both, interleaved, keep resolving to their own runtime dirs.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn workspaces_sharing_a_port_base_never_retarget_each_others_runtime_link() {
        let root = scratch("runtime-link-owner");
        let sandboxes =
            ["first", "second"].map(|store| sandbox_at(&root.join(store).join("workspace")));
        assert_eq!(sandboxes[0].port_block, sandboxes[1].port_block);
        for sandbox in &sandboxes {
            std::fs::create_dir_all(sandbox_runtime_dir(sandbox)).expect("runtime dir");
        }
        for _ in 0..2 {
            for sandbox in &sandboxes {
                link_runtime_dir(sandbox, &sandbox_runtime_dir(sandbox))
                    .await
                    .expect("runtime link");
                assert_eq!(
                    std::fs::read_link(sandbox_runtime_link(sandbox)).expect("runtime link"),
                    sandbox_runtime_dir(sandbox)
                );
            }
        }
        for sandbox in &sandboxes {
            assert_eq!(
                std::fs::read_link(sandbox_runtime_link(sandbox)).expect("runtime link"),
                sandbox_runtime_dir(sandbox),
                "the other workspace's spawn left this one's link in place"
            );
            std::fs::remove_file(sandbox_runtime_link(sandbox)).ok();
        }
    }

    /// The nearest workspace-contained `.envrc` above the command's cwd selects the shell, a
    /// nested one included when the root has none. A `devenv.nix` is not a shell, and an
    /// `.envrc` above the workspace is never the workspace's.
    #[cfg(target_os = "macos")]
    #[test]
    fn only_the_nearest_contained_envrc_selects_a_shell() {
        let root = scratch("envrc-selection");
        let mount = root.join("workspace");
        let nested = mount.join("packages/app");
        std::fs::create_dir_all(&nested).expect("nested project");
        std::fs::write(mount.join("devenv.nix"), "{ ... }: { }\n").expect("devenv.nix");
        std::fs::write(root.join(".envrc"), "exit 1\n").expect("ancestor envrc");
        let mut sandbox = sandbox_at(&mount);
        let mut selected = |cwd: &Path| {
            sandbox
                .configure_capabilities_for(cwd)
                .expect("detect capabilities");
            shell_directory(&sandbox)
        };

        assert_eq!(selected(&nested), None, "no contained .envrc, no shell");

        std::fs::write(nested.join(".envrc"), "").expect("nested envrc");
        assert_eq!(
            selected(&nested),
            Some(nested.clone()),
            "a nested-only .envrc"
        );
        assert_eq!(selected(&mount), None, "the root has no .envrc of its own");

        std::fs::write(mount.join(".envrc"), "").expect("workspace envrc");
        assert_eq!(selected(&nested), Some(nested.clone()), "the nearest wins");
        assert_eq!(selected(&mount), Some(mount.clone()));
    }

    /// A detected tool reaches PATH as its own command name in the mode's private `tools/bin`,
    /// linked to the program it resolved to — never a whole install directory, and never by the
    /// program's file name: npm's installed program is `npm-cli.js`, and a directory of it on
    /// PATH offers no `npm` at all. The links are exactly the current snapshot's: a tool no
    /// longer detected leaves PATH. Nothing comes from the PATH that started this process.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_bootstrap_program_reaches_path_by_its_command_name() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = scratch("bootstrap-programs");
        let mount = root.join("workspace");
        std::fs::create_dir_all(&mount).expect("mount");
        let target = root.join("host/lib/node_modules/npm/bin/npm-cli.js");
        std::fs::create_dir_all(target.parent().expect("package bin")).expect("package");
        std::fs::write(&target, "#!/usr/bin/env node\n").expect("program");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let mut sandbox = sandbox_at(&mount);
        sandbox.capabilities.contribution.bootstrap_programs =
            vec![crate::capabilities::BootstrapProgram {
                name: "npm",
                target: target.clone(),
            }];

        let platform = || {
            let mut tail: Vec<PathBuf> = developer_directory()
                .map(|directory| directory.join("usr/bin"))
                .into_iter()
                .collect();
            tail.extend(["/usr/bin", "/bin", "/usr/sbin", "/sbin"].map(PathBuf::from));
            tail
        };
        for (mode, environment_root) in [
            (RunSandboxMode::ReadWrite, mount.join(".cowshed")),
            (RunSandboxMode::ReadOnly, sandbox.exec_temp_dir.clone()),
        ] {
            sandbox.mode = mode;
            prepare_private_environment(
                &environment_root,
                None,
                &sandbox.capabilities.contribution,
            )
            .expect("prepare");
            let link = environment_root.join("tools/bin/npm");
            assert_eq!(std::fs::read_link(&link).expect("command link"), target);

            let path = bootstrap_path(&sandbox).expect("bootstrap PATH");
            let mut expected = vec![
                environment_root.join("tools/bin"),
                mount.join(".cowshed/bin"),
            ];
            expected.extend(platform());
            assert_eq!(
                std::env::split_paths(&path).collect::<Vec<_>>(),
                expected,
                "{mode:?}: the tools bin leads, then the workspace bin, then the platform"
            );

            prepare_private_environment(
                &environment_root,
                None,
                &crate::capabilities::CapabilityContribution::default(),
            )
            .expect("prepare without tools");
            assert!(
                std::fs::symlink_metadata(&link).is_err(),
                "{mode:?}: a tool no longer detected leaves PATH"
            );
        }
    }

    /// A spawn re-detects the conventions its command sees: unchanged ones reuse the rendered
    /// policy itself, a nested-only `.envrc` created after the policy activates for commands
    /// under it, and deleting it removes the activation again.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_policy_follows_the_conventions_a_command_sees() {
        let root = scratch("policy-conventions");
        let mount = root.join("workspace");
        let nested = mount.join("packages/app");
        std::fs::create_dir_all(&nested).expect("nested project");
        let mut ceiling = sandbox_at(&mount);
        ceiling
            .configure_capabilities()
            .expect("detect capabilities");
        let policy = SandboxPolicy::render(ceiling).expect("render");

        let unchanged = policy.for_cwd(&nested).expect("refresh");
        assert!(
            std::sync::Arc::ptr_eq(&policy.0, &unchanged.0),
            "unchanged conventions reuse the policy"
        );

        std::fs::write(nested.join(".envrc"), "").expect("nested envrc");
        let activated = policy.for_cwd(&nested).expect("refresh");
        assert!(!std::sync::Arc::ptr_eq(&policy.0, &activated.0));
        for mode in [
            crate::api::dto::RunSandboxMode::ReadWrite,
            crate::api::dto::RunSandboxMode::ReadOnly,
        ] {
            let (sandbox, _) = activated.child(mode);
            assert_eq!(shell_directory(sandbox), Some(nested.clone()), "{mode:?}");
        }

        std::fs::remove_file(nested.join(".envrc")).expect("remove envrc");
        let deactivated = activated.for_cwd(&nested).expect("refresh");
        assert_eq!(shell_directory(deactivated.ceiling()), None);
    }

    /// Every job shares the checkout's one Nx state and daemon. Read-only applies to source
    /// files, not to a private second Nx cache or a different socket namespace.
    #[cfg(target_os = "macos")]
    #[test]
    fn all_child_modes_share_the_checkouts_nx_state_and_daemon() {
        let root = scratch("policy-read-only");
        let mount = root.join("workspace");
        std::fs::create_dir_all(&mount).expect("mount");
        std::fs::write(mount.join("nx.json"), "{}").expect("nx project");
        let mut ceiling = sandbox_at(&mount);
        ceiling
            .configure_capabilities()
            .expect("detect capabilities");
        let policy = SandboxPolicy::render(ceiling).expect("render");
        let state = |mode, name| {
            let (sandbox, _) = policy.child(mode);
            sandbox.capabilities.contribution.env.get(name).cloned()
        };
        let own = |path: std::path::PathBuf| Some(crate::capabilities::EnvAction::Own(path.into()));
        let read_write = crate::api::dto::RunSandboxMode::ReadWrite;
        assert_eq!(
            state(read_write, "NX_WORKSPACE_DATA_DIRECTORY"),
            own(mount.join(".nx/workspace-data"))
        );
        assert_eq!(
            state(read_write, "NX_CACHE_DIRECTORY"),
            own(mount.join(".nx/cache"))
        );
        let read_only = crate::api::dto::RunSandboxMode::ReadOnly;
        assert_eq!(
            state(read_only, "NX_WORKSPACE_DATA_DIRECTORY"),
            own(mount.join(".nx/workspace-data"))
        );
        assert_eq!(
            state(read_only, "NX_CACHE_DIRECTORY"),
            own(mount.join(".nx/cache"))
        );
        let (read_only_sandbox, _) = policy.child(read_only);
        let (read_write_sandbox, _) = policy.child(read_write);
        assert_ne!(
            sandbox_runtime_dir(read_only_sandbox),
            sandbox_runtime_dir(read_write_sandbox)
        );
        assert_ne!(
            sandbox_runtime_link(read_only_sandbox),
            sandbox_runtime_link(read_write_sandbox)
        );
        assert_eq!(
            state(read_only, "NX_SOCKET_DIR"),
            state(read_write, "NX_SOCKET_DIR")
        );
        assert_eq!(
            state(read_only, "NX_DAEMON"),
            Some(crate::capabilities::EnvAction::Unset)
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
    /// Named for what it checks: ancestor incarnations named in the lineage are admitted when a
    /// replacement supervisor opens the store, and an incarnation that was never introduced is an
    /// integrity fault. Restart behaviour of the resident job set is asserted, not assumed.
    async fn lineage_admits_ancestor_records_and_refuses_an_unintroduced_incarnation() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-restored-supervisor-{}",
            Uuid::new_v4().simple()
        ));
        // Detection compares resolved paths, and the temp dir resolves through `/var` →
        // `/private/var`: the fixture's workspace is named by its canonical path.
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
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
                // A start creates the TMPDIR `.cowshed/env` names, through no symlink.
                exec_temp_dir: root.join("tmp"),
                ..defaults.sandbox
            },
            build_volume_layout: None,
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
            telemetry_root: None,
            inherited_groups: Vec::new(),
            volume_labels: None,
            workspace_volume: None,
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
    use crate::fork_lock::Run as _;

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

    /// A child granted writes inside a shared cache cannot create its parent, so the host
    /// prepares every contributed cache directory before any child runs, links each private
    /// target to it, and creates every contributed daemon directory in the private environment.
    #[test]
    fn contributed_caches_links_and_daemon_directories_exist_before_a_child_runs() {
        let root = scratch("contributed-environment");
        let environment = root.join("environment");
        let home = root.join("home");
        let contribution = crate::capabilities::CapabilityContribution {
            shared_caches: vec![
                crate::capabilities::SharedCache {
                    path: home.join("go/pkg/mod"),
                    private_link: None,
                    access: crate::capabilities::GrantAccess::ReadWrite,
                },
                crate::capabilities::SharedCache {
                    path: home.join(".local/state/nix"),
                    private_link: Some(environment.join("state/nix")),
                    access: crate::capabilities::GrantAccess::ReadWrite,
                },
            ],
            daemon_isolation: crate::capabilities::DaemonIsolation {
                directories: vec![environment.join("run/nx")],
                discard_at_mint: Vec::new(),
            },
            ..Default::default()
        };
        prepare_private_environment(&environment, None, &contribution).unwrap();
        assert!(home.join("go/pkg/mod").is_dir());
        assert!(home.join(".local/state/nix").is_dir());
        assert_eq!(
            std::fs::read_link(environment.join("state/nix")).ok(),
            Some(home.join(".local/state/nix"))
        );
        assert!(environment.join("run/nx").is_dir());

        // A link or daemon directory outside the private environment is refused, never made.
        let escaping = crate::capabilities::CapabilityContribution {
            shared_caches: vec![crate::capabilities::SharedCache {
                path: home.join(".cache/zig"),
                private_link: Some(root.join("outside/zig")),
                access: crate::capabilities::GrantAccess::ReadWrite,
            }],
            ..Default::default()
        };
        let error = prepare_private_environment(&environment, None, &escaping)
            .expect_err("an escaping cache link is refused");
        assert_eq!(error.code, crate::error::ErrorCode::Integrity);
        assert!(!root.join("outside").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shared_daemon_directories_are_prepared_through_the_runtime_mapping_not_the_alias() {
        let root = scratch("shared-daemon-environment");
        let checkout_environment = root.join("checkout/.cowshed");
        let read_only_environment = root.join("exec-temp");
        let runtime = checkout_environment.join("run");
        let alias = crate::sandbox::workspace_runtime_link(&root.join("checkout"));
        let alias = alias.as_path();
        let contribution = crate::capabilities::CapabilityContribution {
            daemon_isolation: crate::capabilities::DaemonIsolation {
                directories: vec![alias.join("nx")],
                discard_at_mint: Vec::new(),
            },
            ..Default::default()
        };
        prepare_private_environment(
            &checkout_environment,
            Some((alias, &runtime)),
            &contribution,
        )
        .unwrap();
        std::fs::write(runtime.join("nx/preserved"), "shared").unwrap();
        prepare_private_environment(
            &read_only_environment,
            Some((alias, &runtime)),
            &contribution,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(runtime.join("nx/preserved")).unwrap(),
            "shared"
        );
        assert!(
            !read_only_environment.join("run/nx").exists(),
            "no private second daemon state"
        );

        let outside = root.join("outside");
        let escaping = crate::capabilities::CapabilityContribution {
            daemon_isolation: crate::capabilities::DaemonIsolation {
                directories: vec![outside.join("nx")],
                discard_at_mint: Vec::new(),
            },
            ..Default::default()
        };
        assert!(
            prepare_private_environment(&read_only_environment, Some((alias, &runtime)), &escaping)
                .is_err()
        );
        assert!(
            !outside.exists(),
            "an unapproved alias never becomes a host-side write"
        );
        std::fs::create_dir(&outside).unwrap();
        std::fs::remove_dir_all(runtime.join("nx")).unwrap();
        std::os::unix::fs::symlink(&outside, runtime.join("nx")).unwrap();
        assert!(
            prepare_private_environment(
                &read_only_environment,
                Some((alias, &runtime)),
                &contribution
            )
            .is_err()
        );
        assert!(
            !outside.join("preserved").exists(),
            "anchored preparation refuses a runtime symlink"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A plain repository's private environment is the generic roots and nothing else: no tool
    /// cache is created in the host HOME and no tool link appears in the private roots.
    #[test]
    fn a_plain_contribution_prepares_only_the_generic_roots() {
        let root = scratch("plain-environment");
        let environment = root.join("environment");
        prepare_private_environment(
            &environment,
            None,
            &crate::capabilities::CapabilityContribution::default(),
        )
        .unwrap();
        let mut names: Vec<String> = std::fs::read_dir(&environment)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            ["cache", "config", "data", "home", "run", "state", "tools"]
        );
        for name in ["cache", "state", "tools/bin"] {
            let entries: Vec<String> = std::fs::read_dir(environment.join(name))
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|entry| entry != ".links")
                .collect();
            assert!(entries.is_empty(), "{name} holds tool state: {entries:?}");
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
                .output_locked()
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

    /// A process group target must be a strictly positive pid other than init: `kill(-1, ...)`
    /// would signal every process the daemon may signal, and `kill(0, ...)` its own group.
    #[test]
    fn a_pid_that_is_not_a_process_group_is_refused() {
        for pid in [0, 1] {
            assert!(
                ChildFence::new(pid).is_err(),
                "pid {pid} must not be negated into a process-group target"
            );
        }
    }

    /// The reaper and every signal share one fence. A group is signalled while its leader is
    /// unreaped -- running, or exited and not yet collected -- and refused from the moment the
    /// leader is reaped, even though a descendant still holds the id: by then a stranger's group
    /// could take it. A signaller racing the reap sees successes and then only refusals, never a
    /// success after the reap.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_group_is_signalled_only_while_its_reaper_holds_the_leader() {
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut job = std::process::Command::new("/bin/sh")
            .args(["-c", "(trap '' TERM; echo READY; exec sleep 300) & wait"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let pgid = i32::try_from(job.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(job.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        let fence = Arc::new(ChildFence::new(job.id()).unwrap());
        assert!(fence.birth().leader().is_some());
        fence
            .signal(0)
            .expect("a running leader's group is the job's");
        // SIGKILL the leader only; the descendant keeps the id. Until the fence reaps it the
        // exited leader still holds its id, so the group is still the job's.
        // SAFETY: the unreaped test child.
        unsafe { libc::kill(pgid, libc::SIGKILL) };
        std::thread::sleep(std::time::Duration::from_millis(100));
        fence
            .signal(0)
            .expect("an exited, unreaped leader's group is the job's");

        let done = Arc::new(AtomicBool::new(false));
        let signaller = {
            let (fence, done) = (Arc::clone(&fence), Arc::clone(&done));
            std::thread::spawn(move || {
                let mut outcomes = Vec::new();
                while !done.load(Ordering::SeqCst) {
                    outcomes.push(fence.signal(0).is_ok());
                }
                outcomes.push(fence.signal(0).is_ok());
                outcomes
            })
        };
        let status = fence.wait().await.unwrap();
        // The fence, not std's handle, owns waitpid. A second reaper must find no child left.
        assert_eq!(job.wait().unwrap_err().raw_os_error(), Some(libc::ECHILD));
        done.store(true, Ordering::SeqCst);
        let outcomes = signaller.join().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert_eq!(outcomes.last(), Some(&false), "no signal after the reap");
        assert!(
            outcomes.windows(2).all(|pair| pair[0] || !pair[1]),
            "a refusal is never followed by a success"
        );
        let refused = fence.signal(libc::SIGTERM).unwrap_err();
        assert_eq!(refused.code, crate::error::ErrorCode::Conflict);
        assert!(super::super::job_groups::group_has_live_members(pgid).unwrap());
        // SAFETY: the test's own descendant still holds the group it made.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
    }

    /// A job's leader is observed exiting, with its exact status, while it stays unreaped: a kill
    /// still reaches the descendants that outlive it and hold the job's output open. The leader
    /// is reaped only once the job releases it, and nothing is signalled after that.
    #[tokio::test]
    async fn an_exited_leader_is_held_until_its_job_releases_it() {
        use std::io::Read as _;
        use std::os::unix::process::CommandExt as _;

        let mut job = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 300 & exit 3"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let mut output = job.stdout.take().unwrap();
        let pgid = i32::try_from(job.id()).unwrap();
        let fence = ChildFence::new(job.id()).unwrap();
        assert_eq!(
            fence.exited().await.unwrap(),
            ExitStatus::Exited { code: 3 }
        );
        // Observing the exit did not reap it: it is observed again, and the descendant is still
        // in the group the held leader's id names.
        assert_eq!(
            fence.exited().await.unwrap(),
            ExitStatus::Exited { code: 3 }
        );
        assert!(super::super::job_groups::group_has_live_members(pgid).unwrap());
        fence
            .signal(libc::SIGKILL)
            .expect("an exited, held leader's group is the job's");
        let mut rest = Vec::new();
        output
            .read_to_end(&mut rest)
            .expect("the output ends with the descendant");
        assert!(rest.is_empty());
        fence.release().unwrap();
        assert_eq!(job.wait().unwrap_err().raw_os_error(), Some(libc::ECHILD));
        assert_eq!(
            fence.signal(libc::SIGTERM).unwrap_err().code,
            crate::error::ErrorCode::Conflict
        );
    }
}

#[cfg(test)]
mod tail_tests {
    use super::*;
    use crate::api::dto::JobTailBytes;
    use std::num::NonZeroU32;

    fn limits(bytes: u32, lines: u32) -> JobTailLimits {
        JobTailLimits {
            bytes_per_stream: JobTailBytes::new(bytes).unwrap(),
            lines_per_stream: NonZeroU32::new(lines).unwrap(),
        }
    }

    /// `journal`'s slice under `anchor` and `limits`, as `(bytes, next, truncated)`.
    fn slice(journal: &[u8], anchor: TailAnchor, limits: JobTailLimits) -> (Vec<u8>, u64, bool) {
        let window = TailWindow::new(
            StreamKind::Stdout,
            byte_count(journal.len()),
            anchor,
            u64::from(limits.bytes_per_stream.get()),
        )
        .unwrap();
        let start = usize::try_from(window.range.start).unwrap();
        let end = usize::try_from(window.range.end).unwrap();
        let tail = window.cut(journal[start..end].to_vec(), limits.lines_per_stream.get());
        (tail.bytes, tail.next, tail.truncated)
    }

    #[test]
    fn the_latest_tail_keeps_the_last_lines_counting_an_unterminated_one() {
        let wide = limits(64, 2);
        assert_eq!(
            slice(b"a\nb\nc", TailAnchor::End, wide),
            (b"b\nc".to_vec(), 5, true)
        );
        assert_eq!(
            slice(b"a\nb\nc\n", TailAnchor::End, wide),
            (b"b\nc\n".to_vec(), 6, true)
        );
        assert_eq!(
            slice(b"a\nb", TailAnchor::End, wide),
            (b"a\nb".to_vec(), 3, false)
        );
        assert_eq!(slice(b"", TailAnchor::End, wide), (Vec::new(), 0, false));
        assert_eq!(
            slice(b"0123456789", TailAnchor::End, limits(4, 2)),
            (b"6789".to_vec(), 10, true)
        );
    }

    #[test]
    fn a_tail_after_a_cursor_keeps_the_first_lines_and_names_what_follows() {
        let journal = b"a\nb\nc";
        assert_eq!(
            slice(journal, TailAnchor::After(0), limits(64, 2)),
            (b"a\nb\n".to_vec(), 4, true)
        );
        assert_eq!(
            slice(journal, TailAnchor::After(4), limits(64, 2)),
            (b"c".to_vec(), 5, false)
        );
        assert_eq!(
            slice(journal, TailAnchor::After(5), limits(64, 2)),
            (Vec::new(), 5, false)
        );
        assert_eq!(
            slice(journal, TailAnchor::After(1), limits(2, 9)),
            (b"\nb".to_vec(), 3, true)
        );
    }

    #[test]
    fn a_bad_second_cursor_is_refused_before_either_stream_is_read() {
        let cursor = JobJournalCursor {
            stdout: 0,
            stderr: 6,
        };
        let error = job_tail(Some(cursor), limits(64, 1), (5, 5), |_, _| {
            unreachable!("a refused tail reads no stream")
        })
        .err()
        .unwrap();
        assert_eq!(error.code, crate::error::ErrorCode::Usage);
        assert!(error.message.contains("stderr"), "{}", error.message);
    }

    #[test]
    fn retained_chunks_short_of_the_admitted_range_are_an_error_not_a_short_slice() {
        let chunks = VecDeque::from([Bytes::from_static(b"abc"), Bytes::from_static(b"de")]);
        assert_eq!(read_live_range(&chunks, 1..4).unwrap(), b"bcd");
        let error = read_live_range(&chunks, 3..8).err().unwrap();
        assert_eq!(error.code, crate::error::ErrorCode::Internal);
    }

    #[test]
    fn a_cursor_past_the_admitted_bytes_is_a_usage_error() {
        let error = TailWindow::new(StreamKind::Stderr, 5, TailAnchor::After(6), 64)
            .err()
            .unwrap();
        assert_eq!(error.code, crate::error::ErrorCode::Usage);
        assert!(error.message.contains("stderr"), "{}", error.message);
    }

    #[test]
    fn tail_bytes_are_positive_and_at_most_one_inline_frame() {
        assert!(JobTailBytes::new(0).is_err());
        assert!(JobTailBytes::new(65_536).is_ok());
        assert!(JobTailBytes::new(65_537).is_err());
        assert!(
            serde_json::from_str::<JobTailLimits>(r#"{"bytesPerStream":1,"linesPerStream":0}"#)
                .is_err()
        );
    }
}
