use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::io;
use std::ops::Range;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::api::dto::ExecClass;
use crate::api::dto::{
    AdmissionKey, BinaryData, CommandArg, ExecCommand, ExecRequest, ExecScope, ExitStatus,
    JobFailure, JobId, JobInfo, JobJournalCursor, JobListeningPorts, JobState, JobTail,
    JobTailLimits, OutputLimitInfo, OutputPublication, OutputStorage, OutputSummary,
    ProtectedOutput, SealedJob, Sha256Digest, StdinInfo, StdinKind, StdinSource, StreamInfo,
    TraceContext, UtcTimestamp, WorkspacePath,
};
use crate::api::journal_tail::tail_journals;
use crate::api::process::JobLeafAttribution;
use crate::api::process::{JobProcessRetirement, JobProcessTree};
#[cfg(target_os = "macos")]
use crate::api::process::{
    JobSubtreeAdmission, JobSubtreeHandle, JobSubtreeRefusal, JobSubtreeRetirement,
};
#[cfg(target_os = "macos")]
use crate::api::resources::UsageReconciliation;
use crate::api::resources::{
    HostLoadSample, JobAccounting, JobResourceSample, ResidentBytes, SampleInterval,
};
#[cfg(target_os = "macos")]
use crate::error::PriorityFailure;
use crate::error::{AdmissionField, AdmissionRefusal, CowshedError, Result, StdinRefusal};
use crate::exec::{
    ExecError, ProfileLaunch, SandboxExecRequest, SpawnPlan, classify_spawn_error, plan_exec_under,
    prepare_child_descriptors,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::sandbox::{
    SandboxConfig, SandboxProfileRole, SeatbeltProfile, sandbox_runtime_dir, sandbox_runtime_link,
    seatbelt_profile, shared_daemon_runtime_dir, shared_daemon_runtime_link,
};
use crate::storage::apfs::Labelled;
use crate::storage::audit::AuditSinkError;
use crate::workspace_environment::{PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV, WORKSPACE_TOKEN_ENV};
use cowshed_gateway_types::WorkspaceToken;
use cowshed_os::fork_lock::Spawn as _;
use cowshed_os::fsio::AnchoredDirectory;

use crate::runtime::child_fence::ChildFence;
use crate::runtime::job_cpu::{JobCpuAdmission, JobCpuAsk, JobCpuGrant, ProbationOutcome};
use crate::runtime::job_groups::Birth;
use crate::runtime::job_progress::{JobProgressStream, ProgressRead};
use crate::runtime::job_resources::{
    Boundary, JobSampler, Member, Observation, Sampling, StreamTally,
};
use crate::runtime::job_spans::{JobSpanEdge, JobSpanPublisher, TraceHealth};
pub use crate::runtime::nx_daemon::NxDaemonIdle;
use crate::runtime::nx_daemon::{NxDaemonKeeper, Probe, Verdict};
#[cfg(target_os = "macos")]
use crate::runtime::process_monitor::ProcessMonitor;
use crate::runtime::process_stream::JobProcessStream;
use crate::runtime::retirement_refusal::{AtEnd, Refusal, RetirementRun, at_end};
use crate::runtime::supervisor_socket::AdmissionSource;
use crate::runtime::volume_usage::{HostVolume, HostVolumeStat, VolumeBaseline, VolumeMountpoint};
use crate::storage::job_artifact::{
    AdmissionLookup, AdmittedSession, AdmittedStdin, ArtifactConfig, ArtifactError, ArtifactStore,
    CompletedJobArtifacts, JobAdmission, JobEnding, JobScheduling, OutputTargets,
    SealedCheckpointManifest, StreamKind,
};

#[cfg(test)]
const DEFAULT_ACTOR_CAPACITY: usize = 64;
#[cfg(test)]
const DEFAULT_EVENT_CAPACITY: usize = 64;
const PROCESS_IO_CHUNK: usize = 64 * 1024;
const MAX_LOG_READ: usize = 64 * 1024;
const MAX_PENDING_STDIN_BYTES: usize = 256 * 1024;
/// How much of a job's delivered input the actor keeps to compare a repeated write with: every
/// byte a write can still be waiting on (the queue's budget and the lane's two frames), so a
/// writer that repeats its unanswered writes after a lost answer is always provable.
const RETAINED_STDIN_BYTES: usize = MAX_PENDING_STDIN_BYTES + 2 * PROCESS_IO_CHUNK;

/// Exact immutable authority carried by every cheap supervisor handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceAuthoritySnapshot {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub grant_revision: u64,
    pub lifecycle_revision: u64,
}

/// Who owns Nx daemon cleanup when a supervisor retires. A land keeps that authority in
/// the controller's prevalidated process plans, never in a mutable daemon record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NxDaemonRetirement {
    Recorded,
    ControllerOwned,
}

type BuildVolumeReleaseFuture = std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// The controller's release a job's end owes the build volume it was admitted with, once its
/// hold is gone (16_build_volumes.md, "Garbage collection"): a volume an adoption or reseed
/// superseded while the job ran is released then, inside the controller, and the job's end waits
/// for it. A release that fails or cannot run is the job's terminal failure, and the volume's
/// record keeps naming its owner; nothing is left for a later collection.
#[derive(Clone)]
pub struct BuildVolumeReleased(
    Arc<dyn Fn(crate::build_volume::BuildVolumeId) -> BuildVolumeReleaseFuture + Send + Sync>,
);

impl BuildVolumeReleased {
    pub fn new<F>(
        released: impl Fn(crate::build_volume::BuildVolumeId) -> F + Send + Sync + 'static,
    ) -> Self
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        Self(Arc::new(move |volume| Box::pin(released(volume))))
    }

    async fn release(&self, volume: crate::build_volume::BuildVolumeId) -> Result<()> {
        (self.0)(volume).await
    }
}

impl std::fmt::Debug for BuildVolumeReleased {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BuildVolumeReleased")
    }
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
    /// The membership-only endpoint served beside the privileged coordinator socket.
    /// Only this endpoint is admitted into child sandboxes and exported to their environment.
    pub membership_socket: Option<PathBuf>,
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
    /// How each job's resources are sampled.
    pub job_sampling: JobSamplingPolicy,
    /// The host CPU budget every job's command is admitted through before it spawns (05_gateway.md
    /// "Host CPU budget"): the gateway's grant, held until the job concludes.
    pub cpu: Arc<dyn JobCpuAdmission>,
    /// Told each build volume whose job hold ended; `None` tells nothing.
    pub build_volume_released: Option<BuildVolumeReleased>,
    /// How long the shed may go unused before the keeper stops its Nx daemon: main's
    /// `.cowshed.toml` `[nx] daemon_idle` (04_sandbox.md, "The Nx daemon stays on").
    pub nx_daemon_idle: NxDaemonIdle,
}

/// How the supervisor samples its jobs' resources (07_api.md, "Job monitoring"): policy values,
/// set from measurement, never facts a job reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobSamplingPolicy {
    /// `jobCpuSamplePeriod`: each tick of the supervisor's sampling clock samples every job that
    /// owns a process, closing its CPU window and opening the next. Positive.
    pub cpu_sample_period: Duration,
    /// The share of a job's accounted CPU its observed processes must cover before the one with
    /// the most own CPU is named its leaf.
    pub leaf_coverage: LeafCoverage,
}

impl Default for JobSamplingPolicy {
    /// The initial settings: one second (pending measurement of its cost and of `cpuPct`'s
    /// stability), and 900‰ (pending measurement on real cargo, Nx and Bun gate jobs).
    fn default() -> Self {
        Self {
            cpu_sample_period: Duration::from_secs(1),
            leaf_coverage: LeafCoverage(900),
        }
    }
}

/// The share of a job's accounted CPU its observed processes must cover before the one with the
/// most own CPU is named its leaf, in thousandths: at least one, at most the whole.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeafCoverage(u16);

impl LeafCoverage {
    /// `permille` thousandths, if it is a share no empty observation could meet and no more
    /// than the whole.
    pub const fn new(permille: u16) -> Option<Self> {
        if permille == 0 || permille > crate::api::process::PERMILLE_WHOLE {
            None
        } else {
            Some(Self(permille))
        }
    }

    pub const fn permille(self) -> u16 {
        self.0
    }
}

/// Names the volume of a workspace's image through the APFS host, which renames only the volume
/// device the kernel's attachment inventory maps that image under the store to, and only when the
/// file system's own name differs (01a_apfs.md, "Ownership, identity, and the volume label").
/// Handed an image, never a mount path, so a checkout nothing is mounted at names nothing.
/// Injected, so the supervisor names its checkout's volumes without owning the storage backend.
pub trait VolumeLabeller: Send + Sync + 'static {
    fn ensure_label(&self, image: &Path, label: &str) -> std::io::Result<Labelled>;
}

/// The labels of one checkout's two volumes (16_build_volumes.md, "Substrate"): the workspace
/// image's, and the build volume's it links. A clone carries its source's label, the blank
/// template's `[cowshed]` or another checkout's, until its supervisor names it here: when the
/// supervisor starts, and whenever a job is admitted on a build volume the checkout's link moved
/// to (a land, a refork). Renaming queues on Disk Arbitration, measured at 10–28 s on a busy host,
/// so it runs in the background and never on a command's path; the outcome goes to stderr.
#[derive(Clone)]
pub struct VolumeLabels {
    /// The workspace's canonical image, whose volume `workspace` names.
    pub workspace_image: PathBuf,
    pub workspace: String,
    pub build: String,
    pub labeller: Arc<dyn VolumeLabeller>,
}

impl std::fmt::Debug for VolumeLabels {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VolumeLabels")
            .field("workspace_image", &self.workspace_image)
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
        if self.job_sampling.cpu_sample_period.is_zero() {
            return Err(CowshedError::usage(
                "the job CPU sample period must be positive",
                "configure a positive job CPU sample period",
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

/// A fixture's configuration: placeholder paths and a CPU budget that grants at once.
#[cfg(test)]
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
                store_root: PathBuf::from("/tmp/cowshed-store"),
                port_block: crate::metadata::PortBlock::new(49_136, 16).expect("static port block"),
                retained_port_blocks: Vec::new(),
                mode: crate::sandbox::RunSandboxMode::ReadWrite,
                grants: crate::sandbox::SandboxGrants::default(),
                allowed_unix_sockets: Vec::new(),
                cpu_pool_directory: None,
                additional_denies: Vec::new(),
                shed_links: Vec::new(),
                git_worktree_repository: None,
                build_volume_mount: None,
                repository_caches: Vec::new(),
                program_directories: Vec::new(),
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
            membership_socket: None,
            telemetry_root: None,
            inherited_groups: Vec::new(),
            volume_labels: None,
            workspace_volume: None,
            job_sampling: JobSamplingPolicy::default(),
            cpu: Arc::new(ImmediateCpu),
            build_volume_released: None,
            nx_daemon_idle: NxDaemonIdle::DEFAULT,
        }
    }
}

/// A CPU budget that grants each request its whole want at once, and a resident its pool alone:
/// in-crate tests' stand-in for the gateway. Its pool names no live socket; nothing in these
/// tests asks it for tokens.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct ImmediateCpu;

#[cfg(test)]
impl JobCpuAdmission for ImmediateCpu {
    fn background_share(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(4).expect("four")
    }

    fn admit(
        &self,
        ask: JobCpuAsk,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobCpuGrant>> + Send>> {
        Box::pin(async move {
            let pool = PathBuf::from("/nonexistent/cowshed-cpu-pool.sock");
            Ok(match ask.class {
                ExecClass::Resident | ExecClass::InteractiveResident => {
                    JobCpuGrant::resident(pool, Box::new(()))
                }
                ExecClass::Background | ExecClass::Interactive | ExecClass::Gate => {
                    let fifo = PathBuf::from("/nonexistent/cowshed-cpu-pool.fifo");
                    JobCpuGrant::new(
                        super::job_cpu::HeldTokens {
                            tokens: ask.want,
                            fifo,
                        },
                        pool,
                        Box::new(()),
                    )
                }
            })
        })
    }

    fn activation(
        &self,
        _checkout: PathBuf,
        _command: String,
        _parent: Option<String>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<super::job_cpu::ActivationStake>> + Send>>
    {
        Box::pin(async { Ok(super::job_cpu::ActivationStake::new(Box::new(()))) })
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
    /// Stop the job's processes where they are: its probation ended without a grant
    /// (05_gateway.md "A one-token job starts on probation").
    Stop,
    /// Continue a stopped job: its grant came, or it is being killed.
    Continue,
}

/// How long a job runs on the probation token before its grant: until it has used this much CPU
/// or run this long since its first process, whichever comes first, as the sampling clock reads
/// it (05_gateway.md "A one-token job starts on probation").
const PROBATION_CPU: Duration = Duration::from_secs(1);
const PROBATION_WALL: Duration = Duration::from_secs(2);

/// A job on probation's wall and CPU milliseconds at `sample`, once either reached its bound.
fn probation_spent(sample: &JobResourceSample) -> Option<(u64, u64)> {
    let wall = sample.wall_ms.get();
    let cpu = sample.cpu_user_ms.get() + sample.cpu_sys_ms.get();
    (u128::from(wall) >= PROBATION_WALL.as_millis() || u128::from(cpu) >= PROBATION_CPU.as_millis())
        .then_some((wall, cpu))
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
    ceiling_child: ChildProfile,
    read_only_child: ChildProfile,
}

/// An executed-child profile, rendered with the build volume its launches bind, and the store
/// directory its file lives in. The text depends only on the workspace's authority, so renders
/// before and after a build-volume pivot share one file and differ only in the binding.
#[derive(Clone, Debug)]
pub struct ChildProfile {
    profile: SeatbeltProfile,
    directory: PathBuf,
}

impl ChildProfile {
    fn new(profile: SeatbeltProfile, sandbox: &SandboxConfig) -> Self {
        Self {
            profile,
            directory: sandbox.profile_directory(),
        }
    }

    /// `text`, kept in `directory`, bound to no build volume: a test's host profile.
    #[cfg(test)]
    pub(super) fn unbound(text: &str, directory: &Path) -> Self {
        Self {
            profile: SeatbeltProfile::unparameterized(text),
            directory: directory.to_owned(),
        }
    }

    /// The rendered profile.
    pub fn text(&self) -> &str {
        self.profile.text()
    }

    /// How a launch is told this profile: its file, made sure of now (written when `cowshed gc`
    /// removed it or no launch wrote it yet, its mtime bumped otherwise), and its build volume.
    pub fn launch(&self) -> Result<ProfileLaunch> {
        ProfileLaunch::write(&self.directory, &self.profile).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot write the sandbox profile into {}: {error}",
                    self.directory.display()
                ),
                "check the store's run directory exists and is yours",
            )
        })
    }
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
            ceiling_child: ChildProfile::new(
                render(&ceiling, SandboxProfileRole::ExecutedChild)?,
                &ceiling,
            ),
            read_only_child: ChildProfile::new(
                render(&read_only, SandboxProfileRole::ExecutedChild)?,
                &read_only,
            ),
            ceiling,
            read_only,
        })))
    }

    fn for_cwd(&self, cwd: &Path) -> Result<Self> {
        let capabilities = {
            let _span = cowshed_os::timing::span("spawn", "capability-detection");
            self.ceiling().detect_capabilities_for(cwd)?
        };
        if capabilities == self.ceiling().capabilities {
            Ok(self.clone())
        } else {
            let _span = cowshed_os::timing::span("spawn", "capability-profile");
            Self::render(self.ceiling().with_capabilities(capabilities))
        }
    }

    pub fn ceiling(&self) -> &SandboxConfig {
        &self.0.ceiling
    }

    /// The sandbox and executed-child profile of a job that asked for `mode`.
    pub fn child(&self, mode: crate::api::dto::RunSandboxMode) -> (&SandboxConfig, &ChildProfile) {
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
    /// Where the command runs: in the workspace's sandbox, or a trusted coordinator's gate at the
    /// host boundary.
    pub scope: ExecScope,
    /// The CPU grant and class nice the command alone runs under.
    pub cpu: CpuPlacement,
    /// Native Linux admission owns this cgroup before any command can start. Injected sinks
    /// without native processes have no kernel membership.
    #[cfg(target_os = "linux")]
    pub cgroup: Option<Arc<super::job_cgroup::JobCgroup>>,
}

/// What a spawn places a job's owned command under: its CPU grant, served to the command as the
/// live pool and count every nested runner borrows from (05_gateway.md "Nested runners use a
/// parent-held sub-ledger") and as the make jobserver fifo Cargo reads (05_gateway.md "Cargo
/// reads its holder's jobserver"), and the nice its class runs at. A resident holds no tokens:
/// its count is 0, it has no fifo, and its pool asks the host for each child (05_gateway.md "A
/// resident holds no token").
#[derive(Clone, Debug)]
pub struct CpuPlacement {
    pub held: Option<super::job_cpu::HeldTokens>,
    pub pool: PathBuf,
    pub nice: i32,
    /// The job's class: a gate's pool is a relay whose runners each size their own test threads
    /// to their own host grant (05_gateway.md "Gate admission").
    pub class: ExecClass,
    /// Whether a shell activation the spawn runs is already under a token the job holds.
    pub activation: ActivationCover,
}

/// What a command's start activates, judged when a resident is admitted (05_gateway.md "A
/// resident's start holds one token"). Only an activation is work a resident's start does, so
/// only [`Self::Activates`] asks the stake that bounds it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShellStart {
    /// The command's directory selects no `.envrc`: no shell activates.
    Bare,
    /// An exec host of the command's shell has activated already and waits, idle or as the
    /// spare, of its current generation.
    Activated,
    /// The shell activates before the command: no activated host waits, or a one-shot child
    /// enters the shell itself.
    Activates,
}

impl ShellStart {
    /// Whether a resident starting so asks one background token for its start.
    pub fn stakes(self) -> bool {
        match self {
            Self::Bare | Self::Activated => false,
            Self::Activates => true,
        }
    }
}

/// Whether the host budget already bounds a shell activation a job's spawn runs (05_gateway.md
/// "A resident's start holds one token").
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActivationCover {
    /// The job holds tokens, or a resident holds its start stake: an activation counts against
    /// them.
    Held,
    /// A resident that asked no stake, its start judged to activate nothing. A pooled spawn that
    /// must activate after all -- its warm host taken, or its shell's inputs changed, since the
    /// judgment -- asks the stake then, drawing on `parent` beside the host when it names one.
    Unheld { parent: Option<String> },
}

/// The make flags a held grant exports to Cargo and make, ahead of its fifo's path.
const JOBSERVER_FIFO_FLAGS: &str = "-j --jobserver-auth=fifo:";

/// The jobserver fifo a command's environment names as its grant's, read back from the
/// `CARGO_MAKEFLAGS` [`CpuPlacement::environment`] sets. The exec host, the first process inside
/// the sandbox, opens it before the command runs and says on the job's stderr when it cannot.
pub fn granted_jobserver_fifo(environment: &BTreeMap<OsString, OsString>) -> Option<&Path> {
    environment
        .get(OsStr::new("CARGO_MAKEFLAGS"))?
        .as_bytes()
        .strip_prefix(JOBSERVER_FIFO_FLAGS.as_bytes())
        .map(|fifo| Path::new(OsStr::from_bytes(fifo)))
}

impl CpuPlacement {
    /// The variables only the grant's authority sets, laid over the caller's: a caller cannot
    /// name a pool, a count or a jobserver it was not granted. The fifo is the pool's own, whose
    /// bytes its socket children take, so the two never count one token twice. Test threads take
    /// no jobserver token, so they are sized to the grant where the caller named no count, except
    /// under a gate: its pool is a relay that lends nothing, every runner its command starts asks
    /// the host and sizes its own test threads to that grant, and a count here would be the
    /// ceiling every one of them reads (05_gateway.md "The holder exports it").
    pub(super) fn environment(
        &self,
        caller: &BTreeMap<String, String>,
    ) -> Vec<(OsString, OsString)> {
        let tokens = self.held.as_ref().map_or(0, |held| held.tokens.get());
        let mut environment = vec![
            (
                OsString::from("BOUNDED_EXEC_CPU_POOL"),
                self.pool.clone().into_os_string(),
            ),
            (
                OsString::from("BOUNDED_EXEC_CPU_TOKENS"),
                OsString::from(tokens.to_string()),
            ),
        ];
        if let Some(held) = &self.held {
            let mut flags = OsString::from(JOBSERVER_FIFO_FLAGS);
            flags.push(&held.fifo);
            environment.push((OsString::from("CARGO_MAKEFLAGS"), flags.clone()));
            environment.push((OsString::from("MAKEFLAGS"), flags));
            let sized_by_this_grant = match self.class {
                ExecClass::Background
                | ExecClass::Interactive
                | ExecClass::Resident
                | ExecClass::InteractiveResident => true,
                ExecClass::Gate => false,
            };
            for name in ["RUST_TEST_THREADS", "NEXTEST_TEST_THREADS"] {
                if sized_by_this_grant && !caller.contains_key(name) {
                    environment.push((OsString::from(name), OsString::from(tokens.to_string())));
                }
            }
            if sized_by_this_grant {
                let tasks = OsString::from(nx_parallel(caller, tokens).to_string());
                environment.push((OsString::from("NX_PARALLEL"), tasks.clone()));
                environment.push((
                    OsString::from(crate::workspace_environment::NX_PARALLEL_RESTORE_ENV),
                    tasks,
                ));
            }
        }
        environment
    }
}

/// How many Nx tasks a command holding `tokens` runs at once: a caller's count below the grant,
/// else the grant. A percentage, or anything else that is not a count, is the grant: Nx reads a
/// percentage of the host's cores, which the grant already bounds.
fn nx_parallel(caller: &BTreeMap<String, String>, tokens: usize) -> usize {
    caller
        .get("NX_PARALLEL")
        .and_then(|count| count.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .map_or(tokens, |count| count.min(tokens))
}

#[cfg(test)]
mod cpu_placement_tests {
    use super::*;

    /// Only a start that activates a shell asks a resident's stake: a directory with no
    /// `.envrc` activates nothing, and a host that has activated waits ready.
    #[test]
    fn a_resident_stakes_only_a_start_that_activates() {
        assert!(!ShellStart::Bare.stakes());
        assert!(!ShellStart::Activated.stakes());
        assert!(ShellStart::Activates.stakes());
    }

    fn placement(held: Option<usize>) -> CpuPlacement {
        CpuPlacement {
            held: held.map(|tokens| super::super::job_cpu::HeldTokens {
                tokens: std::num::NonZeroUsize::new(tokens).expect("a grant holds a token"),
                fifo: PathBuf::from("/pool/j"),
            }),
            pool: PathBuf::from("/pool/p.sock"),
            nice: 10,
            class: ExecClass::Background,
            activation: ActivationCover::Held,
        }
    }

    /// The environment `placement` exports over `caller`, as names and values.
    fn exported(placement: &CpuPlacement, caller: &[(&str, &str)]) -> BTreeMap<String, String> {
        let caller = caller
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        placement
            .environment(&caller)
            .into_iter()
            .map(|(name, value)| {
                (
                    name.into_string().expect("a UTF-8 name"),
                    value.into_string().expect("a UTF-8 value"),
                )
            })
            .collect()
    }

    fn expected(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    /// A held grant exports its fifo to Cargo and make, and sizes test threads to the grant
    /// (05_gateway.md "Cargo reads its holder's jobserver").
    #[test]
    fn a_held_grant_exports_its_jobserver_and_test_threads_to_the_command() {
        assert_eq!(
            exported(&placement(Some(3)), &[]),
            expected(&[
                ("BOUNDED_EXEC_CPU_POOL", "/pool/p.sock"),
                ("BOUNDED_EXEC_CPU_TOKENS", "3"),
                ("CARGO_MAKEFLAGS", "-j --jobserver-auth=fifo:/pool/j"),
                ("MAKEFLAGS", "-j --jobserver-auth=fifo:/pool/j"),
                ("RUST_TEST_THREADS", "3"),
                ("NEXTEST_TEST_THREADS", "3"),
                ("NX_PARALLEL", "3"),
                ("COWSHED_NX_PARALLEL", "3"),
            ])
        );
    }

    /// Nx's task fan-out is the grant's. Measured 2026-10-26: `cowshed exec <ws> -- nx run-many`
    /// on 2 tokens ran 11 task processes at once (16 GB RSS, host load 39), since the devenv
    /// shell's `NX_PARALLEL=100%` let Nx start a task per core whatever the job held. A
    /// grant-sized class exports `NX_PARALLEL` as its count, below any larger caller count and in
    /// place of a percentage, and `COWSHED_NX_PARALLEL`, which `.cowshed/env` restores after a
    /// devenv shell sets its own; a gate exports neither, since its tasks ask the host one by one.
    #[test]
    fn nx_task_fan_out_is_the_grants() {
        let nx = |placement: &CpuPlacement, caller: &[(&str, &str)]| {
            let exported = exported(placement, caller);
            (
                exported.get("NX_PARALLEL").cloned(),
                exported.get("COWSHED_NX_PARALLEL").cloned(),
            )
        };
        let count = |n: &str| (Some(n.to_owned()), Some(n.to_owned()));
        assert_eq!(nx(&placement(Some(2)), &[]), count("2"));
        assert_eq!(
            nx(&placement(Some(2)), &[("NX_PARALLEL", "100%")]),
            count("2")
        );
        assert_eq!(nx(&placement(Some(2)), &[("NX_PARALLEL", "8")]), count("2"));
        assert_eq!(nx(&placement(Some(4)), &[("NX_PARALLEL", "3")]), count("3"));
        let gate = CpuPlacement {
            class: ExecClass::Gate,
            ..placement(Some(1))
        };
        assert_eq!(nx(&gate, &[]), (None, None));
        assert_eq!(nx(&placement(None), &[]), (None, None));
    }

    /// A gate's pool is a relay that lends nothing: every runner its command starts asks the host
    /// and sizes its test threads to that runner's own grant, so the gate's own grant sets no
    /// test-thread count that a runner would read as its ceiling (05_gateway.md "The holder
    /// exports it").
    #[test]
    fn a_gate_relay_exports_its_jobserver_and_no_test_threads() {
        let gate = CpuPlacement {
            class: ExecClass::Gate,
            ..placement(Some(1))
        };
        assert_eq!(
            exported(&gate, &[]),
            expected(&[
                ("BOUNDED_EXEC_CPU_POOL", "/pool/p.sock"),
                ("BOUNDED_EXEC_CPU_TOKENS", "1"),
                ("CARGO_MAKEFLAGS", "-j --jobserver-auth=fifo:/pool/j"),
                ("MAKEFLAGS", "-j --jobserver-auth=fifo:/pool/j"),
            ])
        );
    }

    /// A test-thread count the caller named is the caller's: the grant sets only the other one.
    /// The jobserver is the grant's whatever the caller sent.
    #[test]
    fn a_caller_named_test_thread_count_is_kept_and_its_jobserver_is_not() {
        assert_eq!(
            exported(
                &placement(Some(2)),
                &[
                    ("RUST_TEST_THREADS", "8"),
                    ("CARGO_MAKEFLAGS", "-j --jobserver-auth=fifo:/caller"),
                ],
            ),
            expected(&[
                ("BOUNDED_EXEC_CPU_POOL", "/pool/p.sock"),
                ("BOUNDED_EXEC_CPU_TOKENS", "2"),
                ("CARGO_MAKEFLAGS", "-j --jobserver-auth=fifo:/pool/j"),
                ("MAKEFLAGS", "-j --jobserver-auth=fifo:/pool/j"),
                ("NEXTEST_TEST_THREADS", "2"),
                ("NX_PARALLEL", "2"),
                ("COWSHED_NX_PARALLEL", "2"),
            ])
        );
    }

    /// A resident holds no token: its pool and a zero count, no jobserver, no test-thread sizing
    /// (05_gateway.md "A resident holds no token").
    #[test]
    fn a_resident_exports_its_pool_and_no_jobserver() {
        assert_eq!(
            exported(&placement(None), &[]),
            expected(&[
                ("BOUNDED_EXEC_CPU_POOL", "/pool/p.sock"),
                ("BOUNDED_EXEC_CPU_TOKENS", "0"),
            ])
        );
    }

    /// The exec host reads back the fifo a held grant exported, and none where no grant set one.
    #[test]
    fn the_granted_fifo_is_read_back_from_the_exported_flags() {
        let environment = |placement: &CpuPlacement| -> BTreeMap<OsString, OsString> {
            placement
                .environment(&BTreeMap::new())
                .into_iter()
                .collect()
        };
        assert_eq!(
            granted_jobserver_fifo(&environment(&placement(Some(3)))),
            Some(Path::new("/pool/j"))
        );
        assert_eq!(granted_jobserver_fifo(&environment(&placement(None))), None);
        let foreign = BTreeMap::from([(
            OsString::from("CARGO_MAKEFLAGS"),
            OsString::from("-j4 --jobserver-auth=3,4"),
        )]);
        assert_eq!(granted_jobserver_fifo(&foreign), None);
    }

    /// A band lowers from whatever nice the supervisor itself runs at, so a supervisor started
    /// nice -- under `cowshed exec --host`, a gate's runner -- still places every class, and the
    /// lowest priority bounds the bands rather than refusing them (05_gateway.md "Bands").
    #[test]
    fn class_nice_lowers_from_the_supervisors_own() {
        let classes = [
            ExecClass::Interactive,
            ExecClass::InteractiveResident,
            ExecClass::Gate,
            ExecClass::Background,
            ExecClass::Resident,
        ];
        for (own, placed) in [
            (0, [0, 0, 5, 10, 10]),
            (10, [10, 10, 15, LOWEST_NICE, LOWEST_NICE]),
            (17, [17, 17, LOWEST_NICE, LOWEST_NICE, LOWEST_NICE]),
            (LOWEST_NICE, [LOWEST_NICE; 5]),
        ] {
            assert_eq!(
                classes.map(|class| class_nice(own, class)),
                placed,
                "supervisor at nice {own}"
            );
        }
    }

    /// The bound is the kernel's own: a held command placed at the lowest nice reads that same
    /// nice back, the readback every placement requires.
    #[cfg(target_os = "macos")]
    #[test]
    fn the_lowest_nice_is_placed_and_read_back() {
        use cowshed_os::fork_lock::Spawn as _;
        // The command waits on its stdin, whose writer this test holds until it has placed it.
        let mut child = std::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .spawn_locked()
            .expect("a command to place");
        let placed = place_nice(child.id(), LOWEST_NICE);
        child.kill().expect("end the placed command");
        child.wait().expect("reap the placed command");
        assert_eq!(
            placed.map_err(|failure| format!("{failure:?}")),
            Ok(LOWEST_NICE)
        );
    }
}

/// The lowest priority setpriority places and reads back: Darwin places PRIO_MAX, 20, itself;
/// Linux stops at MAX_NICE, 19, below its PRIO_MAX of 20, and clamps a larger request to it
/// (getpriority(2)), so a placement asking 20 there would read back 19 and be refused.
#[cfg(target_os = "macos")]
pub const LOWEST_NICE: i32 = libc::PRIO_MAX;
#[cfg(target_os = "linux")]
pub const LOWEST_NICE: i32 = 19;
/// The nice a class's command runs at, its row of 05_gateway.md "Bands", from `own`, the nice this
/// supervisor runs at: interactive and an interactive resident at the supervisor's own, a gate
/// five below it, background and a resident ten below it, never past the kernel's lowest
/// priority. Only lowering: raising a priority above the supervisor's own takes a privilege no
/// supervisor holds. The two residents differ here and nowhere else.
pub(super) const fn class_nice(own: i32, class: ExecClass) -> i32 {
    let below = match class {
        ExecClass::Interactive | ExecClass::InteractiveResident => 0,
        ExecClass::Gate => 5,
        ExecClass::Background | ExecClass::Resident => 10,
    };
    let nice = own.saturating_add(below);
    if nice > LOWEST_NICE {
        LOWEST_NICE
    } else {
        nice
    }
}

/// The nice this supervisor runs at, which its classes' bands are placed from ("Bands").
fn own_nice() -> i32 {
    super::shell_host::clear_errno();
    // SAFETY: a plain read of this process's own priority.
    let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
    // -1 is a nice as well as getpriority's error; a process reading its own priority has no
    // error to report, so a set errno here is an impossible state, not a placement.
    let read = io::Error::last_os_error();
    assert!(
        nice != -1 || read.raw_os_error() == Some(0),
        "this process reads its own nice: {read}"
    );
    nice
}

/// Set the held command `pid` -- forked, leading its own group, not yet running anything of the
/// job's -- to `nice`, and read back what its kernel entry holds: the nice the command is
/// observed at. Only a child held at its observation fence is placed here, never a host or this
/// supervisor.
#[cfg(target_os = "macos")]
pub(super) fn place_nice(pid: u32, nice: i32) -> std::result::Result<i32, PriorityFailure> {
    let failure = |observed, errno| PriorityFailure {
        pid,
        requested: nice,
        observed,
        errno,
    };
    // SAFETY: plain system calls naming a child this supervisor's spawn holds unreleased.
    if unsafe { libc::setpriority(libc::PRIO_PROCESS, pid, nice) } == -1 {
        return Err(failure(None, io::Error::last_os_error().raw_os_error()));
    }
    super::shell_host::clear_errno();
    // SAFETY: as above.
    let observed = unsafe { libc::getpriority(libc::PRIO_PROCESS, pid) };
    if observed == -1
        && let Some(errno) = io::Error::last_os_error().raw_os_error()
        && errno != 0
    {
        return Err(failure(None, Some(errno)));
    }
    if observed == nice {
        Ok(observed)
    } else {
        Err(failure(Some(observed), None))
    }
}

/// The command as the gateway's admission diagnostics name it: an argv's words, or that it is a
/// script.
fn command_label(command: &ExecCommand) -> String {
    match command {
        ExecCommand::Argv(argv) => argv
            .iter()
            .map(|argument| argument.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join(" "),
        ExecCommand::Script(_) => "script".to_owned(),
    }
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
    /// The native membership owner joined every retained member, or refused without
    /// releasing its authority. The leader's exit alone never supplies this proof.
    Retired {
        job_id: JobId,
        retirement: Result<JobProcessRetirement>,
    },
    /// `wait(2)` did not report how the child died. Carries the failure instead of a status so
    /// no consumer can mistake an unreaped child for a terminated one.
    WaitFailed {
        job_id: JobId,
        error: CowshedError,
    },
    /// The pump wrote the oldest write its lane took, whole, to the child's stdin: the input's
    /// cursor moves past it, and the lane has room again.
    StdinReady {
        job_id: JobId,
    },
    /// A write to the child's stdin failed partway, after an unknown number of its bytes. The
    /// pump has dropped the pipe.
    StdinBroken {
        job_id: JobId,
        error: String,
    },
    StdinPumpWrite {
        job_id: JobId,
        /// Where `bytes` start in the input the pump delivers.
        offset: u64,
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
        /// The nice the command was observed at once placed, before it ran anything.
        nice: i32,
    },
    /// A warm-shell job ended before any command started; nothing is left running.
    LaunchFailed {
        job_id: JobId,
        error: CowshedError,
    },
    /// A job started on the probation token heard how its wait for its grant ended
    /// (05_gateway.md "A one-token job starts on probation").
    Probation {
        job_id: JobId,
        outcome: ProbationOutcome,
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
    /// A native Linux spawn's complete kernel membership, retained through terminal accounting.
    #[cfg(target_os = "linux")]
    fn cgroup(&self) -> Option<&Arc<super::job_cgroup::JobCgroup>> {
        None
    }
    /// The command's process as its parent observed it before anything could reap it. `None`
    /// until the process exists; a warm-shell job reports it with [`ProcessEvent::Started`].
    fn process(&self) -> Option<&OwnedProcess>;
    /// The nice the command this handle spawned itself was observed at; `None` for one whose
    /// start, and nice, arrive with [`ProcessEvent::Started`].
    fn nice(&self) -> Option<i32>;
    /// The retained native source this spawn owns, or absence for a non-native spawn provider.
    #[cfg(target_os = "macos")]
    fn observations(&self) -> Option<&ProcessMonitor>;
    /// `Ok(false)` means the bounded process-input lane is full.
    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool>;
    /// Sends the job's one EOF. `false` means the lane still holds a write its pump has not
    /// taken: nothing was sent, and the pump's next [`ProcessEvent::StdinReady`] is the time to
    /// retry. Once it has answered `true`, every later call answers `true` and sends nothing.
    fn close_stdin(&mut self) -> bool;
    /// The leader ended: close the job's stdin for good, so descendants reading it see its end.
    fn end_stdin(&mut self);
    /// Signal the job's process group, only while the process that reaps its leader holds the
    /// leader unreaped, so the group id still names the job's group ([`ChildFence`]).
    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()>;
}

#[async_trait]
pub trait SpawnSink: Send {
    /// Prepare kernel membership before the actor records an admission. A non-native sink
    /// owns no OS process; the production sink must return a cgroup or refuse.
    #[cfg(target_os = "linux")]
    fn admit_cgroup(
        &mut self,
        _authority: &WorkspaceAuthoritySnapshot,
        _job_id: JobId,
    ) -> Result<Option<Arc<super::job_cgroup::JobCgroup>>> {
        Ok(None)
    }

    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>>;

    /// What a command in `mode` at `cwd`, under `policy`, would activate if it started now
    /// (05_gateway.md "A resident's start holds one token"). A sink that keeps no exec hosts
    /// knows of none that has activated, so by default the start activates.
    async fn shell_start(
        &mut self,
        _policy: &SandboxPolicy,
        _cwd: &Path,
        _mode: crate::api::dto::RunSandboxMode,
        _scope: ExecScope,
    ) -> ShellStart {
        ShellStart::Activates
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
    /// The first refusal of an output copy the job asked for, stdout's before stderr's. The seal
    /// stands regardless: the terminal record is durable before any copy is attempted, so a
    /// refused copy travels beside it instead of replacing it.
    pub publication_failure: Option<CowshedError>,
}

pub trait ArtifactSink: Send {
    fn next_job_id(&self) -> Result<JobId>;
    /// Durably admit `job_id`, under `admission`'s key when it has one, with the scheduling it
    /// asked for, before it spawns.
    fn admit(
        &mut self,
        job_id: JobId,
        grant_revision: u64,
        command: &ExecCommand,
        admission: Option<JobAdmission>,
        scheduling: JobScheduling,
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
    /// The terminal record under current store authority. `Ok(None)` proves absence; unread or
    /// set-aside history is the original refusal, never a missing-job answer.
    fn sealed(&mut self, job_id: JobId) -> Result<Option<SealedJob>>;
}

pub use crate::storage::audit::CommitmentDraft;
pub use cowshed_os::process::ProcessStatus;

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

    fn sealed(&mut self, job_id: JobId) -> Result<Option<SealedJob>> {
        Ok(self
            .store
            .sealed(job_id)
            .map_err(map_artifact_error)?
            .map(|record| SealedJob {
                job_id: record.job_id,
                state: record.state,
                exit: record.exit.clone(),
                failure: record.failure,
                duration_ms: record.duration_ms,
                resources: record.resources.clone(),
                containment: record.containment.clone(),
                outlived: record.outlived.clone(),
                outlived_incomplete: record.outlived_incomplete,
                output_limit: record.output_limit.clone(),
                stdout: record.stdout.clone(),
                stderr: record.stderr.clone(),
                class: record.scheduling.class(),
                scope: record.scheduling.scope(),
                nice: record.nice,
                cpu_tokens: record.cpu_tokens,
            }))
    }

    fn admit(
        &mut self,
        job_id: JobId,
        grant_revision: u64,
        command: &ExecCommand,
        admission: Option<JobAdmission>,
        scheduling: JobScheduling,
    ) -> Result<()> {
        let token = self
            .store
            .begin_job(
                job_id,
                grant_revision,
                command,
                admission,
                scheduling,
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
/// performed the acts and absorbs sink failures into [`AuditHealth`]. It group-commits: every
/// record already queued when it takes one joins that record's group, up to
/// [`crate::storage::audit::GROUP_RECORDS`], and nothing waits for more. The group is recorded
/// off the async workers in one sink call, so records queue while its sync runs, and every
/// record's caller is answered only once that call returns.
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
            // A request taken while gathering a group that does not belong to it: it is served
            // next, so requests are answered in the order they were queued.
            let mut next: Option<CommitmentRequest> = None;
            loop {
                let request = match next.take() {
                    Some(request) => request,
                    None => match receiver.recv().await {
                        Some(request) => request,
                        None => break,
                    },
                };
                let (draft, reply) = match request {
                    CommitmentRequest::Record { draft, reply } => (draft, reply),
                    CommitmentRequest::Health { reply } => {
                        let _ = reply.send(health.clone());
                        continue;
                    }
                };
                let mut group = crate::storage::audit::CommitmentGroup::one(*draft);
                let mut replies = vec![reply];
                while let Ok(request) = receiver.try_recv() {
                    match request {
                        CommitmentRequest::Record { draft, reply } => match group.try_push(draft) {
                            Ok(()) => replies.push(reply),
                            Err(draft) => {
                                next = Some(CommitmentRequest::Record { draft, reply });
                                break;
                            }
                        },
                        asked @ CommitmentRequest::Health { .. } => {
                            next = Some(asked);
                            break;
                        }
                    }
                }
                let records = u64::try_from(replies.len()).expect("a group fits in u64");
                let (returned, outcome) = tokio::task::spawn_blocking(move || {
                    let outcome = sink.record(group);
                    (sink, outcome)
                })
                .await
                .unwrap_or_else(|failure| std::panic::resume_unwind(failure.into_panic()));
                sink = returned;
                match outcome {
                    Ok(()) => health.recorded = health.recorded.saturating_add(records),
                    Err(error) => {
                        health.failed = health.failed.saturating_add(records);
                        health.last_failure = Some(error.to_string());
                    }
                }
                for reply in replies {
                    let _ = reply.send(());
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
    let command = plan.command_start();
    activation.extend(plan.args.drain(command..));
    plan.args.extend(activation);
}

/// The sandboxed launch of a one-shot child running `argv` in `cwd`: entered through the
/// workspace shell selected for that cwd when there is one.
fn one_shot_plan(
    sandbox: &SandboxConfig,
    profile: &ChildProfile,
    cwd: PathBuf,
    argv: Vec<OsString>,
) -> Result<SpawnPlan> {
    let mut plan = plan_exec_under(
        SandboxExecRequest { argv, cwd },
        sandbox,
        &profile.launch()?,
    )
    .map_err(map_exec_error)?;
    if let Some(shell) = &sandbox.capabilities.contribution.shell {
        wrap_one_shot(&mut plan, shell);
    }
    Ok(plan)
}

/// The installed cowshed's directory leads exact private-bin tool links; no host tool profile
/// joins PATH. Nothing there answers to `cargo`: the toolchain's own Cargo reads the job's grant
/// from its environment ([`CpuPlacement`]).
/// The package-manager wrapper route sits just ahead of the other bootstrap links: it is the one
/// directory the exec host re-leads after shell activation, so it leads before activation too.
fn bootstrap_path(sandbox: &SandboxConfig) -> Result<OsString> {
    let tools = match sandbox.mode {
        crate::sandbox::RunSandboxMode::ReadOnly => sandbox.exec_temp_dir.join("tools"),
        crate::sandbox::RunSandboxMode::ReadWrite => sandbox.workspace_mount.join(".cowshed/tools"),
    };
    let mut paths = vec![
        crate::host_dirs::installed_binary_directory(&sandbox.home),
        tools.join("manager-bin"),
        tools.join("bin"),
    ];
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
/// The token authenticates against nothing but this workspace's own loopback listener: a sibling
/// that reads it from this workspace's process environment (`kern.procargs2`, 04_sandbox.md) can
/// neither connect to that listener nor present it at its own, which answers 407. The username is
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
    workspace_root: &Path,
    environment_root: &Path,
    runtime: Option<(&Path, &Path)>,
    contribution: &crate::capabilities::CapabilityContribution,
) -> Result<AnchoredDirectory> {
    let environment =
        AnchoredDirectory::create(environment_root).map_err(private_environment_error)?;
    for name in [c"home", c"config", c"cache", c"data", c"state", c"run"] {
        environment.child(name).map_err(private_environment_error)?;
    }
    let tools = environment
        .child(c"tools")
        .map_err(private_environment_error)?;
    let bin = tools.child(c"bin").map_err(private_environment_error)?;
    let manager_host = if contribution
        .bootstrap_programs
        .iter()
        .any(|program| crate::developer_links::is_package_manager(OsStr::new(program.name)))
    {
        super::shell_host::registered().filter(|program| {
            program.arguments().len() == 1
                && program.arguments()[0] == super::shell_host::SHELL_HOST_ARGUMENT
        })
    } else {
        None
    };
    let manager_host = manager_host
        .as_ref()
        .map(|program| super::shell_job::stage_host_executable(workspace_root, program))
        .transpose()?;
    let real_bin = tools
        .child(c"real-bin")
        .map_err(private_environment_error)?;
    // Wrappers live apart from the other bootstrap links: the exec host leads PATH with this
    // directory alone after activation, so a bootstrap toolchain never shadows the shell's.
    let manager_bin = tools
        .child(c"manager-bin")
        .map_err(private_environment_error)?;
    let mut managers = Vec::new();
    let mut wrappers = Vec::new();
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
        if crate::developer_links::is_package_manager(OsStr::new(program.name))
            && let Some(host) = manager_host.as_deref()
        {
            managers.push((name.clone(), program.target.as_path()));
            wrappers.push((name, host));
        } else {
            programs.push((name, program.target.as_path()));
        }
    }
    real_bin
        .reconcile_links(c".links", &managers)
        .map_err(private_environment_error)?;
    manager_bin
        .reconcile_links(c".links", &wrappers)
        .map_err(private_environment_error)?;
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

/// The caller's variables a child may take: no bypass of managed Git configuration, no
/// agent-harness `CI` in a development checkout, and no CPU authority but the job's own grant.
/// GitHub/Forgejo Actions identifies real CI with `GITHUB_ACTIONS=true`; a bare `CI` changes
/// Cargo unit identities but names no runner. A caller's make jobserver (`CARGO_MAKEFLAGS`,
/// `MAKEFLAGS`, `MFLAGS`) would run the job's builds on someone else's tokens, and the job's own
/// jobserver and pool markers are the grant's to set ([`CpuPlacement`]).
/// What the sandbox owns or withholds is laid over this by [`SandboxEnvironment`].
/// Variables that name a CPU authority; only a job's own grant sets them.
const CALLER_CPU_AUTHORITY: [&str; 5] = [
    "CARGO_MAKEFLAGS",
    "MAKEFLAGS",
    "MFLAGS",
    "BOUNDED_EXEC_CPU_POOL",
    "BOUNDED_EXEC_CPU_TOKENS",
];
pub(super) fn caller_environment(
    caller: &BTreeMap<String, String>,
) -> impl Iterator<Item = (&str, &str)> {
    caller
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .filter(|(name, _)| crate::workspace_git_fetch::caller_git_environment_allowed(name))
        .filter(|(name, _)| !CALLER_CPU_AUTHORITY.contains(name))
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
    /// A clause the sandbox appends to the caller's value, or the whole value when the caller
    /// named none, so the sandbox's clause is the one that takes effect.
    appended: BTreeMap<OsString, Appended>,
}

/// What the sandbox adds after a caller's value of one variable: a configuration line after the
/// caller's lines, or a loader option after the caller's options, each joined by the separator
/// its variable is split on.
struct Appended {
    clause: String,
    separator: char,
}

impl Appended {
    /// The clause after the caller's own non-empty value, or the clause alone.
    fn after(&self, caller: Option<&str>) -> OsString {
        match caller {
            Some(caller) if !caller.is_empty() => {
                format!("{caller}{}{}", self.separator, self.clause).into()
            }
            _ => self.clause.as_str().into(),
        }
    }
}

impl SandboxEnvironment {
    fn reserved(&self, name: &str) -> bool {
        self.owned.contains_key(OsStr::new(name)) || self.withheld.contains(&name)
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
        for (name, appended) in &self.appended {
            let caller = name.to_str().and_then(|name| caller.get(name));
            environment.insert(name.clone(), appended.after(caller.map(String::as_str)));
        }
        environment
    }

    /// The environment a warm exec host is started with and activates from.
    pub(super) fn base(&self) -> BTreeMap<OsString, OsString> {
        let mut environment = self.owned.clone();
        environment.extend(self.defaults.clone());
        for (name, appended) in &self.appended {
            environment.insert(name.clone(), appended.after(None));
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
                    Some(appended) => appended.after(Some(value)),
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
    cowshed_os::fork_lock::RunAsync::output_locked(&mut command)
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
        &sandbox.workspace_mount,
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
    own(
        crate::developer_links::MANAGER_BIN_ENV,
        environment_root.join("tools/manager-bin").as_os_str(),
    );
    own("HOME", private_home.as_os_str());
    own("XDG_CONFIG_HOME", private_config.as_os_str());
    own("XDG_CACHE_HOME", private_cache.as_os_str());
    own(
        crate::workspace_environment::NODE_COMPILE_CACHE_ENV,
        private_cache
            .join(crate::workspace_environment::NODE_COMPILE_CACHE_DIRECTORY)
            .as_os_str(),
    );
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
    // The bare token is no job's to read: the proxy userinfo carries it where a client presents
    // it, and `.cowshed/workspace.json` marks a workspace. A caller's stale value (a host shell
    // that sourced an older `.cowshed/env`) is withheld, not forwarded.
    withheld.push(WORKSPACE_TOKEN_ENV);
    if sandbox
        .capabilities
        .contribution
        .env
        .contains_key("NX_CACHE_DIRECTORY")
    {
        own(
            crate::workspace_environment::NX_REMOTE_SERVER_ENV,
            OsStr::new(&format!("http://127.0.0.1:{port_base}/nx")),
        );
        own(
            crate::workspace_environment::NX_REMOTE_READ_ONLY_ENV,
            OsStr::new("true"),
        );
    } else {
        withheld.extend([
            crate::workspace_environment::NX_REMOTE_SERVER_ENV,
            crate::workspace_environment::NX_REMOTE_READ_ONLY_ENV,
        ]);
    }
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
                cowshed_os::eprintln!(
                    "cowshed: {name} was supplied by the caller; the workspace trust bundle is not \
                 being used for it, so an intercepted HTTPS origin may fail to verify"
                );
            }
            defaults.insert(OsString::from(name), bundle.clone().into_os_string());
        }
    }
    // Every Node and Bun of the job loads the port-0 preload published beside the trust bundle,
    // so a port-0 listener lands in the block the job can connect to (04_sandbox.md).
    let preload = environment_root.join(
        crate::port_zero::PRELOAD_NAME
            .to_str()
            .expect("the preload name is ASCII"),
    );
    let loader_options = crate::port_zero::loader_options(&preload)
        .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))?;
    for (name, clause) in loader_options {
        appended.insert(
            OsString::from(name),
            Appended {
                clause,
                separator: ' ',
            },
        );
    }
    for (&name, action) in &sandbox.capabilities.contribution.env {
        match action {
            crate::capabilities::EnvAction::Own(value) => {
                owned.insert(name.into(), value.clone());
            }
            crate::capabilities::EnvAction::Unset => withheld.push(name),
            crate::capabilities::EnvAction::Default(value) => {
                if caller.contains_key(name) {
                    cowshed_os::eprintln!(
                        "cowshed: {name} was supplied by the caller; the project capability \
                     default is not being used"
                    );
                }
                defaults.insert(name.into(), value.clone());
            }
            crate::capabilities::EnvAction::Append(value) => {
                appended.insert(
                    name.into(),
                    Appended {
                        clause: value.clone(),
                        separator: '\n',
                    },
                );
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
    #[cfg(target_os = "linux")]
    cgroups: Option<super::job_cgroup::IncarnationCgroups>,
}

impl SystemSpawnSink {
    /// A sink whose commands under a workspace `.envrc` run in warm hosts of `program`; a spare
    /// host's activation asks `cpu` for its token.
    pub fn with_shell_host(
        program: super::shell_host::ShellHostProgram,
        config: super::shell_pool::ShellPoolConfig,
        cpu: Arc<dyn JobCpuAdmission>,
    ) -> Self {
        Self {
            shells: Some(super::shell_job::WorkspaceShells::new(program, config, cpu)),
            #[cfg(target_os = "linux")]
            cgroups: None,
        }
    }
}

#[async_trait]
impl SpawnSink for SystemSpawnSink {
    #[cfg(target_os = "linux")]
    fn admit_cgroup(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
    ) -> Result<Option<Arc<super::job_cgroup::JobCgroup>>> {
        if !self
            .cgroups
            .as_ref()
            .is_some_and(|jobs| jobs.incarnation() == &authority.workspace_incarnation)
        {
            let delegated =
                super::job_cgroup::CgroupAuthority::delegated().map_err(cgroup_error)?;
            self.cgroups = Some(
                delegated
                    .incarnation(&authority.workspace_incarnation)
                    .map_err(cgroup_error)?,
            );
        }
        self.cgroups
            .as_ref()
            .expect("authority was established above")
            .admit(job_id)
            .map(Arc::new)
            .map(Some)
            .map_err(cgroup_error)
    }

    /// A directory with no `.envrc` activates nothing, one-shot or pooled. Under an `.envrc`,
    /// the shell's pool says whether an activated host waits; with no exec host program the
    /// one-shot child activates the shell itself. The host boundary's command enters whatever
    /// shell it names after its spawn, so it is judged to activate.
    async fn shell_start(
        &mut self,
        policy: &SandboxPolicy,
        cwd: &Path,
        mode: crate::api::dto::RunSandboxMode,
        scope: ExecScope,
    ) -> ShellStart {
        if scope == ExecScope::HostGate {
            return ShellStart::Activates;
        }
        // A cwd or profile that cannot be resolved fails the job's spawn with this same cause;
        // until then its start is judged to activate, which keeps its stake.
        let Ok(cwd) = crate::exec::contained_cwd(&policy.ceiling().workspace_mount, cwd) else {
            return ShellStart::Activates;
        };
        let Ok(policy) = policy.for_cwd(&cwd) else {
            return ShellStart::Activates;
        };
        let (sandbox, _) = policy.child(mode);
        let Some(directory) = shell_directory(sandbox) else {
            return ShellStart::Bare;
        };
        match &self.shells {
            Some(shells) => {
                shells
                    .start(
                        sandbox.mode == crate::sandbox::RunSandboxMode::ReadOnly,
                        directory,
                    )
                    .await
            }
            None => ShellStart::Activates,
        }
    }

    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        #[cfg(target_os = "linux")]
        let cgroup = request.cgroup.clone().ok_or_else(|| {
            CowshedError::environment_missing(
                format!(
                    "job {} has no admitted cgroup: refusing to run an uncontained command",
                    request.job_id.get()
                ),
                "delegate a writable cgroup v2 subtree and admit the job before spawning it",
            )
        })?;
        let cwd =
            crate::exec::contained_cwd(&request.policy.ceiling().workspace_mount, &request.cwd)
                .map_err(map_exec_error)?;
        if request.scope == ExecScope::HostGate {
            let command = host_gate_command(&request, &cwd)?;
            #[cfg(target_os = "macos")]
            let monitor =
                ProcessMonitor::prepare(super::process_tree::TreeOwner::Job(request.job_id))?;
            return spawn_fenced(
                command,
                request.job_id,
                request.cpu.nice,
                #[cfg(target_os = "linux")]
                cgroup,
                #[cfg(target_os = "macos")]
                monitor,
                events,
            )
            .await;
        }
        let policy = request.policy.for_cwd(&cwd)?;
        let (sandbox, profile) = policy.child(request.mode);
        let envrc_directory = shell_directory(sandbox);
        let environment = cowshed_os::timing::spanned(
            "spawn",
            "environment",
            sandbox_environment(sandbox, &request.env),
        )
        .await?;
        #[cfg(target_os = "macos")]
        let monitor = ProcessMonitor::prepare(super::process_tree::TreeOwner::Job(request.job_id))?;
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
            grants: &sandbox.grants,
            cpu: &request.cpu,
            #[cfg(target_os = "linux")]
            cgroup: Arc::clone(&cgroup),
            #[cfg(target_os = "macos")]
            monitor: monitor.clone(),
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
            // The activated PATH is the shell's, set inside the sandbox: unknown here.
            if let Some(program) = argv.first() {
                refuse_unstartable(program, &cwd, None)?;
            }
            return shells.spawn(
                pooled(super::shell_job::HostCommand::Argv(argv), environment),
                events,
            );
        }
        let mut environment = environment.child(&request.env);
        // The grant's pool, count and jobserver are the authority's, laid over whatever the
        // caller named.
        environment.extend(request.cpu.environment(&request.env));
        if let Some(program) = argv.first() {
            // Shell activation, when the profile has one, sets PATH inside the sandbox.
            let path = sandbox
                .capabilities
                .contribution
                .shell
                .is_none()
                .then(|| environment.get(OsStr::new("PATH")))
                .flatten();
            refuse_unstartable(program, &cwd, path.map(OsString::as_os_str))?;
        }
        let plan = one_shot_plan(sandbox, profile, cwd, argv)?;
        spawn_fenced(
            sandboxed_command(&plan, &environment),
            request.job_id,
            request.cpu.nice,
            #[cfg(target_os = "linux")]
            cgroup,
            #[cfg(target_os = "macos")]
            monitor,
            events,
        )
        .await
    }
}

#[cfg(target_os = "linux")]
pub(super) fn cgroup_error(error: super::job_cgroup::CgroupError) -> CowshedError {
    let hint = match &error {
        // Retirement's own wait expired: the delegation is fine, a member outlived its kill.
        super::job_cgroup::CgroupError::Unreaped { .. } => {
            "a member that outlives SIGKILL is in an uninterruptible kernel wait, which ends only \
             when that call returns: read its kernel stack (/proc/<pid>/stack, as root) for what \
             it waits on; the job keeps its claims, and killing it again retires it once the \
             member has exited"
        }
        _ => {
            "run the supervisor in a delegated cgroup v2 scope (Delegate=yes), with writable \
             cgroup.procs, cgroup.subtree_control and cgroup.kill; Linux 6.5+ is required"
        }
    };
    CowshedError::environment_missing(format!("job cgroup containment: {error}"), hint)
}

#[cfg(target_os = "linux")]
fn refused_cgroup(
    mut refusal: CowshedError,
    cgroup: Option<&super::job_cgroup::JobCgroup>,
) -> CowshedError {
    if let Some(cgroup) = cgroup
        && let Err(cleanup) = cgroup.finish(cowshed_os::deadline::REAP)
    {
        refusal.message.push_str(&format!(
            "; removing the unstarted job's cgroup also failed: {cleanup}"
        ));
    }
    refusal
}

/// The host-boundary command of a trusted coordinator's host exec, at whatever class it was
/// admitted: its argv exec'd directly in the workspace, outside every sandbox profile and without
/// shell activation, so it keeps the host authority real APFS images need. Its environment is exactly the caller's (credential
/// names already withheld), with the CPU grant's pool and count laid over it and
/// `NX_DAEMON_START=never` last, so no job's Nx client starts a daemon (04_sandbox.md). Admission
/// refused a host-gate script, which needs the sandboxed exec host's interpreter.
fn host_gate_command(request: &ProcessSpawnRequest, cwd: &Path) -> Result<tokio::process::Command> {
    let SpawnCommand::Argv(argv) = &request.command else {
        return Err(CowshedError::internal(format!(
            "job {} was admitted as a host-gate script",
            request.job_id.get()
        )));
    };
    let Some((program, arguments)) = argv.split_first() else {
        return Err(CowshedError::internal(format!(
            "job {} was admitted with an empty argv",
            request.job_id.get()
        )));
    };
    refuse_unstartable(program, cwd, request.env.get("PATH").map(OsStr::new))?;
    let mut command = tokio::process::Command::new(program);
    command
        .env_clear()
        .args(arguments)
        .current_dir(cwd)
        .envs(caller_environment(&request.env))
        .envs(request.cpu.environment(&request.env))
        .env("NX_DAEMON_START", "never")
        .env("PWD", cwd);
    Ok(command)
}

/// Refuse an argv command whose program, `#!` interpreter or `env` name is proven absent, naming
/// the missing one (`command_start.rs`): `sandbox-exec` would report any of them as the program
/// itself. `path` is the job's PATH, `None` when shell activation sets it inside the sandbox.
fn refuse_unstartable(program: &OsStr, cwd: &Path, path: Option<&OsStr>) -> Result<()> {
    match super::command_start::unstartable(program, cwd, path) {
        None => Ok(()),
        Some(missing) => Err(CowshedError::not_found(
            missing.to_string(),
            "create the missing file, or name a program and interpreter the job's PATH holds",
        )),
    }
}

/// Spawn `command` as a job's one-shot leader: in a process group of its own, at `nice`, reaped
/// only inside its [`ChildFence`]. On macOS the child is held before exec until its native
/// observer watches it and this process placed its nice; elsewhere the child sets and verifies its
/// own nice before exec, and a refusal fails the spawn with the kernel's errno.
async fn spawn_fenced(
    mut command: tokio::process::Command,
    job_id: JobId,
    nice: i32,
    #[cfg(target_os = "linux")] cgroup: Arc<super::job_cgroup::JobCgroup>,
    #[cfg(target_os = "macos")] monitor: ProcessMonitor,
    events: mpsc::Sender<ProcessEvent>,
) -> Result<Box<dyn RunningProcess>> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    prepare_child_descriptors(command.as_std_mut())
        .map_err(ExecError::from)
        .map_err(map_exec_error)?;
    #[cfg(target_os = "linux")]
    cgroup
        .placement()
        .map_err(cgroup_error)?
        .on_exec(command.as_std_mut());
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
    #[cfg(target_os = "macos")]
    let (mut child, nice) = {
        let (gate, held) = super::process_fence::leader_gate().map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot prepare the process observation fence: {error}"),
                "inspect descriptor availability before retrying the command",
            )
        })?;
        let hold = held.in_process(&gate);
        // SAFETY: the hold calls only close/getpid/write/read, all async-signal-safe.
        unsafe {
            command.pre_exec(move || hold.hold());
        }
        let command = command.into_std();
        let source = monitor.clone();
        tokio::task::spawn_blocking(move || spawn_observed(command, held, gate, &source, nice))
            .await
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()))
            .map_err(|error| match error {
                HeldSpawnError::Spawn(error) => {
                    map_exec_error(ExecError::from(classify_spawn_error(error)))
                }
                HeldSpawnError::Priority(failure) => CowshedError::priority(failure),
            })?
    };
    // No fence holds the child here: it places its own nice before exec and reports exactly what
    // it observed on a private pipe, so a refusal is typed and a placement is the read-back.
    #[cfg(not(target_os = "macos"))]
    let (mut child, nice) = {
        let (reported, report) = super::shell_host::nice_pipe().map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot prepare the command's nice report: {error}"),
                "inspect descriptor availability before retrying the command",
            )
        })?;
        let report_end = std::os::fd::AsRawFd::as_raw_fd(&report);
        // SAFETY: getpid/setpriority/getpriority/write and an errno store run between fork and
        // exec, on a stack buffer and a descriptor this closure only names.
        unsafe {
            command.pre_exec(move || super::shell_host::report_own_nice(report_end, nice));
        }
        let spawned = command.into_std().spawn_locked();
        // The child wrote before exec or never got that far; this end only kept the pipe open.
        drop(report);
        match (spawned, super::shell_host::read_nice_report(reported)) {
            (Ok(child), Ok(Some(Ok(observed)))) => (child, observed),
            (Err(_), Ok(Some(Err(failure)))) => return Err(CowshedError::priority(failure)),
            (Err(error), _) => {
                return Err(map_exec_error(ExecError::from(classify_spawn_error(error))));
            }
            (Ok(mut child), report) => {
                // Exec happened, so the child placed and reported its nice; a report that does not
                // say so leaves the command's nice unknown, and it must not run on unobserved.
                let pid = i32::try_from(child.id()).unwrap_or(i32::MAX);
                // SAFETY: this unreaped child leads its own group, made before its nice.
                let killed = unsafe { libc::kill(-pid, libc::SIGKILL) };
                let kill = (killed == -1).then(io::Error::last_os_error);
                let waited = child.wait();
                return Err(CowshedError::internal(format!(
                    "the command started without a readable nice report ({report:?}); group \
                     kill {kill:?}; reap {waited:?}"
                )));
            }
        }
    };
    let spawned = Instant::now();
    let fence = std::sync::Arc::new(ChildFence::new(child.id())?);
    let process = OwnedProcess {
        birth: fence.birth().clone(),
        spawned,
        host: crate::host_load::read_host_load(),
    };
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
        process,
        stdin: StdinLane::new(stdin_sender),
        nice,
        #[cfg(target_os = "linux")]
        cgroup,
        #[cfg(target_os = "macos")]
        monitor,
    }))
}

/// Why a held one-shot spawn did not release a command.
#[cfg(target_os = "macos")]
enum HeldSpawnError {
    /// The spawn, or the observation fence around it, failed.
    Spawn(io::Error),
    /// The held child could not be placed at its nice; it never ran.
    Priority(PriorityFailure),
}

/// std::Command waits for exec, so spawn and observation run concurrently while the child
/// is held. While it is held the observer attaches and its nice is placed; a failed attachment
/// or placement fails the exec, so no workload is released unobserved or at the wrong nice.
#[cfg(target_os = "macos")]
fn spawn_observed(
    mut command: std::process::Command,
    held: super::process_fence::HeldEnds,
    gate: super::process_fence::LeaderGate,
    monitor: &ProcessMonitor,
    nice: i32,
) -> std::result::Result<(std::process::Child, i32), HeldSpawnError> {
    std::thread::scope(|scope| {
        let spawning = scope.spawn(move || {
            let child = command.spawn_locked();
            drop(held);
            child
        });
        let mut placed = None;
        let observed = gate.observe(|pid| {
            monitor.watch(pid).map_err(io::Error::other)?;
            let placement = place_nice(pid, nice);
            let refused = placement
                .as_ref()
                .err()
                .map(|failure| io::Error::other(format!("{failure:?}")));
            placed = Some(placement);
            refused.map_or(Ok(()), Err)
        });
        let spawned = spawning
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        match (observed, placed, spawned) {
            (Ok(_), Some(Ok(nice)), Ok(child)) => Ok((child, nice)),
            (_, Some(Err(failure)), Err(_)) => Err(HeldSpawnError::Priority(failure)),
            (Ok(_), _, Err(spawn)) => Err(HeldSpawnError::Spawn(spawn)),
            (Err(observation), _, Err(spawn)) => {
                Err(HeldSpawnError::Spawn(io::Error::other(format!(
                    "cannot attach the process observer: {observation}; held spawn failed: {spawn}"
                ))))
            }
            (observed, placed, Ok(mut child)) => {
                let pid = i32::try_from(child.id())
                    .map_err(io::Error::other)
                    .map_err(HeldSpawnError::Spawn)?;
                // SAFETY: this unreaped child leads its own group, established before the hold.
                let killed = unsafe { libc::kill(-pid, libc::SIGKILL) };
                let kill = (killed == -1).then(io::Error::last_os_error);
                let waited = child.wait();
                match placed {
                    Some(Err(failure)) => {
                        if kill.is_some() || waited.is_err() {
                            cowshed_os::eprintln!(
                                "cowshed: the command refused nice {} was not ended cleanly: group kill {kill:?}; reap {waited:?}",
                                failure.requested
                            );
                        }
                        Err(HeldSpawnError::Priority(failure))
                    }
                    Some(Ok(_)) | None => Err(HeldSpawnError::Spawn(io::Error::other(format!(
                        "cannot attach the process observer: {observed:?}; unreleased group kill: \
                         {kill:?}; reap: {waited:?}"
                    )))),
                }
            }
        }
    })
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

    /// Queues the pump's one EOF: `false` while the lane's slot still holds a write.
    pub(super) fn close(&mut self) -> bool {
        if self.closed {
            return true;
        }
        let Some(sender) = &self.sender else {
            self.closed = true;
            return true;
        };
        match sender.try_send(SystemStdin::Close) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {
                self.closed = true;
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => false,
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
    /// The command as this process observed it right after spawning it.
    process: OwnedProcess,
    stdin: StdinLane,
    /// The nice the command was observed at once placed.
    nice: i32,
    #[cfg(target_os = "linux")]
    cgroup: Arc<super::job_cgroup::JobCgroup>,
    #[cfg(target_os = "macos")]
    monitor: ProcessMonitor,
}

impl RunningProcess for SystemRunningProcess {
    #[cfg(target_os = "linux")]
    fn cgroup(&self) -> Option<&Arc<super::job_cgroup::JobCgroup>> {
        Some(&self.cgroup)
    }

    fn process(&self) -> Option<&OwnedProcess> {
        Some(&self.process)
    }

    fn nice(&self) -> Option<i32> {
        Some(self.nice)
    }

    #[cfg(target_os = "macos")]
    fn observations(&self) -> Option<&ProcessMonitor> {
        Some(&self.monitor)
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        self.stdin.try_write(bytes)
    }

    fn close_stdin(&mut self) -> bool {
        self.stdin.close()
    }

    fn end_stdin(&mut self) {
        self.stdin.end();
    }

    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            match signal {
                ProcessSignal::Term => self.cgroup.terminate(),
                ProcessSignal::Kill => self.cgroup.kill(),
                ProcessSignal::Stop => self.cgroup.freeze(true),
                ProcessSignal::Continue => self.cgroup.freeze(false),
            }
            .map_err(cgroup_error)
        }
        #[cfg(not(target_os = "linux"))]
        self.fence
            .signal(match signal {
                ProcessSignal::Term => libc::SIGTERM,
                ProcessSignal::Kill => libc::SIGKILL,
                ProcessSignal::Stop => libc::SIGSTOP,
                ProcessSignal::Continue => libc::SIGCONT,
            })
            .map_err(crate::runtime::child_fence::NotSignalled::into_error)
    }
}

/// The job concluded, or its supervisor is going away: the leader is reaped now if it exited, or
/// by its watcher as soon as it does.
impl Drop for SystemRunningProcess {
    fn drop(&mut self) {
        if let Err(error) = self.fence.release() {
            cowshed_os::eprintln!(
                "cowshed: cannot reap the sandbox process {}: {error}",
                self.fence.birth().pid()
            );
        }
    }
}

/// Writes the lane's bytes to the child's stdin in order, reporting each write once it is whole.
/// A write that fails partway is reported as broken and ends the pipe; the lane then stays open
/// and unread until it ends, so no write sees a closed lane before the actor has heard why.
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
                if let Err(error) = stdin.write_all(&bytes).await {
                    drop(stdin);
                    let broken = ProcessEvent::StdinBroken {
                        job_id,
                        error: error.to_string(),
                    };
                    if events.send(broken).await.is_ok() {
                        while receiver.recv().await.is_some() {}
                    }
                    return;
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
    ///
    /// `source` is who asks: only a coordinator may run a gate or reach the host boundary. The
    /// answer comes at admission, once the job is durable and `queued` for its CPU grant, never
    /// at its spawn: the caller holds the job's id while it waits, reads its state, and kills it
    /// to withdraw it from the gateway's queue (07_api.md, "CLI and API exec parity"). A
    /// refusal that arrives after admission — the gateway's, or the class nice that cannot be
    /// placed — fails the job, typed on its record and said on its stderr.
    pub async fn exec(
        &self,
        session: Option<&SessionToken>,
        build_volume: Option<PathBuf>,
        request: ExecRequest,
        source: AdmissionSource,
    ) -> Result<JobId> {
        self.exec_from(session, build_volume, request, source, None)
            .await
    }

    /// [`Self::exec`] for a requester that holds a live CPU pool: the job waits on `cpu_parent`
    /// and on the host ledger at once, and the first grant serves it (05_gateway.md "Work a holder
    /// starts draws on its grant"). `None` asks the host alone, as `exec` does.
    pub async fn exec_from(
        &self,
        session: Option<&SessionToken>,
        build_volume: Option<PathBuf>,
        request: ExecRequest,
        source: AdmissionSource,
        cpu_parent: Option<String>,
    ) -> Result<JobId> {
        self.call(|reply| Command::Exec {
            authority: self.authority.clone(),
            session: session.cloned(),
            build_volume,
            request: Box::new(request),
            source,
            cpu_parent,
            reply,
        })
        .await
    }

    /// Leave the job running with nobody attached (11_shell.md, "`--background`"): its captured
    /// prefixes are made durable in their protected files, and a session's job joins that
    /// session's background set. A job that already ended is durable as it is, so it answers
    /// at once.
    pub async fn detach(&self, job_id: JobId) -> Result<()> {
        self.call(|reply| Command::Detach {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    /// Writes `bytes` as the job's input from byte `offset` on, answering once they reached the
    /// child's stdin; see the actor's `stdin_write`.
    pub async fn stdin_write(&self, job_id: JobId, offset: u64, bytes: Bytes) -> Result<()> {
        if bytes.len() > PROCESS_IO_CHUNK {
            return Err(CowshedError::usage(
                "stdin write exceeds the 64 KiB bounded frame",
                "split stdin into 64 KiB or smaller chunks",
            ));
        }
        self.call(|reply| Command::StdinWrite {
            authority: self.authority.clone(),
            job_id,
            offset,
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

    /// The retained process tree, read from one immutable native publication.
    pub async fn processes(&self, job_id: JobId) -> Result<JobProcessTree> {
        self.call(|reply| Command::Processes {
            authority: self.authority.clone(),
            job_id,
            reply,
        })
        .await
    }

    pub async fn process_events(
        &self,
        job_id: JobId,
        every: SampleInterval,
    ) -> Result<JobProcessStream> {
        self.call(|reply| Command::ProcessEvents {
            authority: self.authority.clone(),
            job_id,
            every,
            reply,
        })
        .await
    }
    #[cfg(target_os = "macos")]
    pub(super) async fn subtree_handle(
        &self,
        peer: super::job_groups::Process,
    ) -> Result<JobSubtreeAdmission> {
        self.call(|reply| Command::SubtreeHandle { peer, reply })
            .await
    }

    #[cfg(target_os = "macos")]
    pub(super) async fn retire_descendants(
        &self,
        peer: super::job_groups::Process,
        handle: JobSubtreeHandle,
        grace: Duration,
    ) -> Result<JobSubtreeRetirement> {
        self.call(|reply| Command::RetireDescendants {
            peer,
            handle,
            grace,
            reply,
        })
        .await
    }

    /// The job `key` admitted in this workspace incarnation, or proven absence in its durable
    /// records: answered after the exec's reply was lost, even by a supervisor that did not run it.
    pub async fn job_by_key(&self, key: AdmissionKey) -> Result<Option<JobId>> {
        self.call(|reply| Command::JobByKey {
            authority: self.authority.clone(),
            key,
            reply,
        })
        .await
    }

    /// The job's progress: its latest sample once it owns a process, another every `every`
    /// while it runs, then its terminal sample once, and the end. A job that owns no process
    /// yet is sampled once it does; one that ended with no sample ends the stream with why.
    /// Dropping the stream ends the subscription and nothing else.
    pub async fn progress(
        &self,
        job_id: JobId,
        every: SampleInterval,
    ) -> Result<JobProgressStream> {
        let first = self.read_progress(job_id).await?;
        let reader = self.clone();
        let waiter = self.clone();
        crate::runtime::job_progress::subscribe(
            first,
            every.duration(),
            move || {
                let reader = reader.clone();
                async move { reader.read_progress(job_id).await }
            },
            async move {
                // Only the wake matters: the read that follows answers how the job ended, and
                // says why when it cannot.
                let _ = waiter.wait(job_id).await;
            },
        )
    }

    /// Where the job is, for its progress: unowned, running and observed now, or ended with
    /// its terminal sample.
    pub(super) async fn read_progress(&self, job_id: JobId) -> Result<ProgressRead> {
        self.call(|reply| Command::Progress {
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

    /// Close admission and wait for the running jobs to end, retiring nothing. Only a caller
    /// in the supervisor's own process may leave it there, since it retires the supervisor
    /// itself afterwards; a remote handle refuses it. A caller in another process asks
    /// [`Self::retire_after_jobs`], so its going away can never leave the supervisor refusing
    /// work while it serves.
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
    /// open, no job is running, and a shed's keeper has stopped its Nx daemon and finds none
    /// ([`NxDaemonKeeper`]), so no daemon outlives the supervisor that watched it. Only the
    /// process serving a supervisor asks, to retire it; that serving host is the macOS native
    /// project host.
    #[cfg(target_os = "macos")]
    pub async fn idle(&self) -> Result<bool> {
        self.call(|reply| Command::Idle { reply }).await
    }

    /// What the shed's Nx daemon keeper decided at its next probe ([`super::NxKeeperState`]): the
    /// start job it waits on, and why it starts none -- a start that can never run, or one bound
    /// reached, named. `None` once the supervisor keeps no daemon: main's, or one retiring. Asked
    /// of the supervisor in this process; a remote handle refuses it.
    pub async fn nx_daemon_keeper(&self) -> Result<Option<super::NxKeeperState>> {
        self.call(|reply| Command::NxDaemonKeeper { reply }).await
    }

    /// Stop the shed's Nx daemon keeper from starting a daemon until the shed is used again, and
    /// end the start it has in flight, answered once that start concluded: a lifecycle verb asks
    /// before its census of the mount (02_workspaces.md, "Holders cowshed does not run"). A start
    /// whose daemon the checkout's record names live is serving and is left to the supervisor's
    /// stop. Main's supervisor keeps none, so it has nothing to stop.
    pub async fn stop_nx_keeper(&self) -> Result<()> {
        self.call(|reply| Command::StopNxKeeper {
            authority: self.authority.clone(),
            reply,
        })
        .await
    }

    /// Retire now: every running job is stopped (TERM, grace, KILL).
    pub async fn retire(&self, nx_daemon: NxDaemonRetirement) -> Result<()> {
        self.retire_when(nx_daemon, RetireWhen::Now).await
    }

    /// Admit nothing more and retire once the running jobs have ended, as one request: the
    /// supervisor retires whether or not its caller is still there to see it.
    pub async fn retire_after_jobs(&self, nx_daemon: NxDaemonRetirement) -> Result<()> {
        self.retire_when(nx_daemon, RetireWhen::AfterJobs).await
    }

    /// Retire now if no job runs (the Nx keeper's start excepted); refused as a `Conflict`,
    /// changing nothing, while one does.
    pub async fn retire_if_idle(&self, nx_daemon: NxDaemonRetirement) -> Result<()> {
        self.retire_when(nx_daemon, RetireWhen::IfIdle).await
    }

    async fn retire_when(&self, nx_daemon: NxDaemonRetirement, when: RetireWhen) -> Result<()> {
        self.call(|reply| Command::Retire {
            authority: self.authority.clone(),
            nx_daemon,
            when,
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
                host_home: &config.sandbox.home,
                temp_dir: &config.sandbox.exec_temp_dir,
                capability_env: &config.sandbox.capabilities.contribution,
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
            Some(program) => SystemSpawnSink::with_shell_host(
                program,
                config.shell_pool,
                Arc::clone(&config.cpu),
            ),
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
        let policy = job_sandbox_policy(config.sandbox, config.membership_socket.as_deref())?;
        let next_job_id = artifacts.next_job_id()?;
        let (commands, receiver) = mpsc::channel(config.actor_capacity);
        let (events, event_receiver) = mpsc::channel(config.event_capacity);
        let job_spans = config.telemetry_root.map(JobSpanPublisher::start);
        let handle = WorkspaceSupervisorHandle {
            authority: config.authority.clone(),
            commands,
        };
        let nx_daemon = NxDaemonKeeper::for_role(
            WorkspaceRole::for_name(&config.authority.workspace),
            config.nx_daemon_idle,
        );
        let mut actor = SupervisorActor {
            authority: config.authority,
            workspace_root: config.workspace_root,
            default_cwd: config.default_cwd,
            policy,
            build_volume_layout: config.build_volume_layout,
            volume_labels: config.volume_labels,
            workspace_volume: config.workspace_volume,
            credential_env_names: config.credential_env_names,
            group_ledger: config.group_ledger.map(super::job_groups::Recorder::new),
            membership_socket: config.membership_socket,
            job_spans,
            inherited_groups: config.inherited_groups,
            term_grace: config.term_grace,
            next_job_id,
            next_session_id: 1,
            lifecycle: ActorLifecycle::Running,
            commands: receiver,
            events,
            event_receiver,
            membership_changed: Arc::new(tokio::sync::Notify::new()),
            spawner,
            artifacts,
            commitments,
            jobs: BTreeMap::new(),
            sessions: BTreeMap::new(),
            named_sessions: BTreeMap::new(),
            quiesce_waiters: Vec::new(),
            retire_waiters: Vec::new(),
            retire_after_jobs: Vec::new(),
            nx_keeper_waiters: Vec::new(),
            command_lane_closed: false,
            nx_daemon,
            labelled_build: None,
            labelling: tokio::task::JoinSet::new(),
            job_sampling: config.job_sampling,
            sample_clock: None,
            cpu: config.cpu,
            build_volume_released: config.build_volume_released,
            admissions: tokio::task::JoinSet::new(),
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
        source: AdmissionSource,
        /// The requester's live CPU pool, which the job's tokens come from ([`JobCpuAsk::parent`]).
        cpu_parent: Option<String>,
        reply: oneshot::Sender<Result<JobId>>,
    },
    Detach {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<()>>,
    },
    StdinWrite {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        offset: u64,
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
    Processes {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<JobProcessTree>>,
    },
    ProcessEvents {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        every: SampleInterval,
        reply: oneshot::Sender<Result<JobProcessStream>>,
    },
    #[cfg(target_os = "macos")]
    SubtreeHandle {
        peer: super::job_groups::Process,
        reply: oneshot::Sender<Result<JobSubtreeAdmission>>,
    },
    #[cfg(target_os = "macos")]
    RetireDescendants {
        peer: super::job_groups::Process,
        handle: JobSubtreeHandle,
        grace: Duration,
        reply: oneshot::Sender<Result<JobSubtreeRetirement>>,
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
        reply: oneshot::Sender<Result<Option<JobId>>>,
    },
    Progress {
        authority: WorkspaceAuthoritySnapshot,
        job_id: JobId,
        reply: oneshot::Sender<Result<ProgressRead>>,
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
        reply: oneshot::Sender<Result<()>>,
    },
    Retire {
        authority: WorkspaceAuthoritySnapshot,
        nx_daemon: NxDaemonRetirement,
        when: RetireWhen,
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
    /// The keeper's decision at its next probe.
    NxDaemonKeeper {
        reply: oneshot::Sender<Result<Option<super::NxKeeperState>>>,
    },
    /// Stop the keeper's starts until the shed's next use, ending its start in flight.
    StopNxKeeper {
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

/// When a retirement takes effect (11_shell.md "Supervisor"). Admission closes only on the way to
/// retirement: a supervisor that refuses work is one that retires, whoever asked and whether or
/// not they are still there.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RetireWhen {
    /// Now: every running job is stopped.
    Now,
    /// Once the running jobs have ended; nothing more is admitted meanwhile.
    AfterJobs,
    /// Now if no job runs; a `Conflict`, changing nothing, while one does.
    IfIdle,
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
    /// The host CPU budget refused the job, so nothing ran.
    CpuAdmission,
}

/// A job's stdin as the actor delivers it, in input order: the bytes the pump wrote to the
/// child's stdin (`delivered`, the cursor), the writes in the pump's lane, and the writes waiting
/// for it. Every accepted write is answered once the cursor passes its end.
#[derive(Default)]
struct StdinDelivery {
    /// Bytes of the input the pump wrote, whole, to the child's stdin.
    delivered: u64,
    /// The end checked when input was accepted. Delivery changes no accepted boundary; only
    /// discarding queued or uncertain input moves it back.
    admitted_end: u64,
    /// The last delivered bytes, ending at `delivered` and at most [`RETAINED_STDIN_BYTES`]
    /// long: what a repeated write is compared with.
    retained: VecDeque<Bytes>,
    retained_bytes: usize,
    /// Writes the lane took, oldest first; each `StdinReady` delivers the front one.
    in_lane: VecDeque<Bytes>,
    /// Writes waiting for the lane, oldest first.
    queued: VecDeque<Bytes>,
    queued_bytes: usize,
    /// Each accepted write's answer, due once `delivered` reaches the write's end.
    waiters: Vec<(u64, oneshot::Sender<Result<()>>)>,
    /// Why a write to the pipe failed partway, once one did: nothing past `delivered` is known
    /// to have reached the child, and the input takes nothing more.
    broken: Option<String>,
}

/// What a write over input the job already holds compares as.
enum Replay {
    Equal,
    Mismatch,
    /// It starts before `retained_from`, the oldest byte kept to compare with.
    Unprovable {
        retained_from: u64,
    },
}

impl StdinDelivery {
    /// The byte a write continues the input at.
    fn admitted(&self) -> u64 {
        self.admitted_end
    }

    /// Compares `bytes`, which start at `offset` and end within the admitted input, with the
    /// input there: the retained delivered bytes, then the lane's, then the queue's.
    fn replay(&self, offset: u64, bytes: &[u8]) -> Replay {
        let mut at = self.delivered - byte_count(self.retained_bytes);
        if offset < at {
            return Replay::Unprovable { retained_from: at };
        }
        let mut rest = bytes;
        let mut next = offset;
        for held in self
            .retained
            .iter()
            .chain(&self.in_lane)
            .chain(&self.queued)
        {
            if rest.is_empty() {
                break;
            }
            let end = at + byte_count(held.len());
            if next < end {
                let from = usize::try_from(next - at).expect("an offset within one held write");
                let shared = (held.len() - from).min(rest.len());
                if held[from..from + shared] != rest[..shared] {
                    return Replay::Mismatch;
                }
                rest = &rest[shared..];
                next += byte_count(shared);
            }
            at = end;
        }
        Replay::Equal
    }

    /// The pump wrote the lane's front write: the cursor moves past it, its bytes become what a
    /// repeat is compared with, and every write it completes is answered.
    fn deliver_front(&mut self) {
        let Some(written) = self.in_lane.pop_front() else {
            return;
        };
        self.delivered += byte_count(written.len());
        self.retained_bytes += written.len();
        self.retained.push_back(written);
        while self.retained_bytes > RETAINED_STDIN_BYTES {
            let Some(oldest) = self.retained.pop_front() else {
                break;
            };
            self.retained_bytes -= oldest.len();
        }
        let delivered = self.delivered;
        let mut index = 0;
        while index < self.waiters.len() {
            if self.waiters[index].0 <= delivered {
                let (_, reply) = self.waiters.swap_remove(index);
                let _ = reply.send(Ok(()));
            } else {
                index += 1;
            }
        }
    }

    /// The queued writes can never reach the job: they are dropped, and every write waiting on
    /// a byte past the lane is refused with `error`.
    fn drop_queued(&mut self, error: &CowshedError) {
        self.admitted_end -= byte_count(self.queued_bytes);
        self.queued.clear();
        self.queued_bytes = 0;
        let reachable = self.admitted();
        let mut index = 0;
        while index < self.waiters.len() {
            if self.waiters[index].0 > reachable {
                let (_, reply) = self.waiters.swap_remove(index);
                let _ = reply.send(Err(error.clone()));
            } else {
                index += 1;
            }
        }
    }

    /// Nothing more reaches the child: every write still waiting is refused with `error`.
    fn refuse_waiting(&mut self, error: &CowshedError) {
        self.in_lane.clear();
        self.queued.clear();
        self.queued_bytes = 0;
        self.admitted_end = self.delivered;
        for (_, reply) in self.waiters.drain(..) {
            let _ = reply.send(Err(error.clone()));
        }
    }
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

/// The per-job marker the covered install wrapper writes before it runs the install, named by the
/// job's `COWSHED_INSTALL_OBSERVED` environment (`developer_links::dispatch` writes it before
/// `status_locked`). Terminal cleanup inspects it and attempts its removal after restoration, so
/// a removal failure is reported after the restore, never before it. An unreadable, non-regular or
/// otherwise unexpected marker counts as unobserved, with a finding. A job can write it, so forging
/// it only restores links the cleanup would otherwise report: it grants and attests nothing.
struct InstallObservation {
    directory: AnchoredDirectory,
    name: std::ffi::CString,
    path: PathBuf,
}

const INSTALL_OBSERVATION_DIRECTORY: &str = "install-observed";

impl InstallObservation {
    fn new(exec_temp_dir: &Path) -> Result<Self> {
        let parent = exec_temp_dir.join(INSTALL_OBSERVATION_DIRECTORY);
        let directory = AnchoredDirectory::create(&parent).map_err(private_environment_error)?;
        let leaf = uuid::Uuid::new_v4().simple().to_string();
        let name = std::ffi::CString::new(leaf.as_str())
            .map_err(|error| CowshedError::internal(error.to_string()))?;
        Ok(Self {
            directory,
            name,
            path: parent.join(leaf),
        })
    }

    /// Whether the wrapper left a regular marker. Any other entry is refused, never followed.
    fn observed(&self) -> std::io::Result<bool> {
        match self.directory.stat_child(&self.name)? {
            None => Ok(false),
            Some(stat) if stat.st_mode & libc::S_IFMT == libc::S_IFREG => Ok(true),
            Some(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "install observation is not a regular file",
            )),
        }
    }

    fn remove(&self) -> std::io::Result<()> {
        self.directory.remove_file(&self.name)
    }
}

/// Why a job's terminal cleanup failed. Each cause keeps its own error and label; the two together
/// are one typed value, never a string the caller must parse.
enum TerminalCleanupFailure {
    /// Restoring the developer links failed.
    Links(CowshedError),
    /// Removing the install observation marker failed (its own cleanup, after the restore).
    InstallObservation(CowshedError),
    /// The blocking cleanup task failed before either filesystem outcome could be known.
    Task(CowshedError),
    /// Both failed; the restore's error and the marker's are kept, in that order.
    Both {
        links: CowshedError,
        observation: CowshedError,
    },
}

impl TerminalCleanupFailure {
    /// The job's stderr line for this failure, with the label of each cause that occurred.
    fn label(&self) -> String {
        match self {
            Self::Links(error) => format!("developer-link restoration failed: {error}"),
            Self::InstallObservation(error) => {
                format!("install observation cleanup failed: {error}")
            }
            Self::Task(error) => format!("terminal cleanup task failed: {error}"),
            Self::Both { links, observation } => format!(
                "developer-link restoration failed: {links}; install observation cleanup failed: {observation}"
            ),
        }
    }

    /// The one error waiters and the sealed record carry. Each single cause keeps its own code and
    /// hint and is labelled with its cause; when both occurred, the message names both labels.
    fn into_error(self) -> CowshedError {
        match self {
            Self::Links(error) => relabel(error, "developer-link restoration failed"),
            Self::InstallObservation(error) => relabel(error, "install observation cleanup failed"),
            Self::Task(error) => relabel(error, "terminal cleanup task failed"),
            Self::Both { links, observation } => CowshedError::new(
                links.code,
                format!(
                    "developer-link restoration failed: {}; install observation cleanup also failed: {}",
                    links.message, observation.message
                ),
                links.hint,
            ),
        }
    }
}

/// `error` under a cause label: its code and hint kept, its message prefixed with `label`.
fn relabel(error: CowshedError, label: &str) -> CowshedError {
    CowshedError::new(
        error.code,
        format!("{label}: {}", error.message),
        error.hint,
    )
}

/// What terminal restoration reports: the findings for the job's stderr, and the cleanup failure,
/// if any, with each cause kept distinct until the caller classifies it.
struct JobFinish {
    findings: Vec<String>,
    failure: Option<TerminalCleanupFailure>,
}

impl JobFinish {
    fn task_failed(error: tokio::task::JoinError) -> Self {
        Self {
            findings: Vec::new(),
            failure: Some(TerminalCleanupFailure::Task(CowshedError::internal(
                format!("terminal cleanup task did not finish: {error}"),
            ))),
        }
    }
}

/// Terminal restoration of one job's developer links. The wrapper's marker decides the mode.
/// Restoration always runs: an unreadable or non-regular marker counts as unobserved, with a
/// finding, and a marker that cannot be removed is reported after the restore, never before it.
fn finish_terminal_restore(
    root: &Path,
    links: crate::developer_links::DeveloperLinks,
    observation: Option<InstallObservation>,
) -> JobFinish {
    let mut findings = Vec::new();
    // Inspect only: the marker is consumed after the restore, so a failure to remove it is reported
    // after the repair, never before it.
    let observed = match observation.as_ref() {
        Some(marker) => match marker.observed() {
            Ok(observed) => observed,
            Err(error) => {
                findings.push(format!(
                    "developer-link-ambiguous: install observation is unreadable ({error}); treated as unobserved"
                ));
                false
            }
        },
        None => false,
    };
    let mode = if observed {
        crate::developer_links::RestoreMode::JobTerminalObservedInstall
    } else {
        crate::developer_links::RestoreMode::JobTerminal
    };
    let restore_failure = match links.restore(root, mode) {
        Ok(restored) => {
            findings.extend(restored);
            None
        }
        Err(error) => Some(error),
    };
    let removal_failure = match observation {
        Some(marker) => marker
            .remove()
            .err()
            .map(|error| install_observation_error(&error)),
        None => None,
    };
    let failure = match (restore_failure, removal_failure) {
        (None, None) => None,
        (Some(links), None) => Some(TerminalCleanupFailure::Links(links)),
        (None, Some(observation)) => Some(TerminalCleanupFailure::InstallObservation(observation)),
        (Some(links), Some(observation)) => {
            Some(TerminalCleanupFailure::Both { links, observation })
        }
    };
    JobFinish { findings, failure }
}

fn install_observation_error(error: &std::io::Error) -> CowshedError {
    CowshedError::integrity(
        format!("install observation: {error}"),
        "inspect the job's install-observed directory, then retry",
    )
}

struct JobStateRecord {
    info: JobInfo,
    started_at: Instant,
    process: Option<Box<dyn RunningProcess>>,
    retirement: RetirementState,
    /// Admission owns membership even while the job is queued or its spawn was refused.
    #[cfg(target_os = "linux")]
    cgroup: Option<Arc<super::job_cgroup::JobCgroup>>,
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
    developer_links: crate::developer_links::DeveloperLinks,
    install_observation: Option<InstallObservation>,
    /// The terminal cleanup failure of this job, kept as its one error for waiters and the record.
    terminal_cleanup_failure: Option<CowshedError>,
    /// Set once the job ended, whatever became of its record.
    conclusion: Option<Conclusion>,
    /// The job's process as its parent observed it, once its pid is known. Never observed again.
    birth: Option<Birth>,
    /// What the job's resource reads answer: nothing before it owns a process, live samples
    /// while it does, its terminal sample -- or why that failed -- once it ended.
    sampling: Sampling,
    #[cfg(target_os = "macos")]
    monitor: Option<ProcessMonitor>,
    /// The keeper's foreground Nx daemon: its plugin workers end with it (04_sandbox.md).
    #[cfg(target_os = "macos")]
    kept_nx_daemon: bool,
    /// The job's volumes as its admission found them, immediately before its first process was
    /// dispatched: every sample's volume deltas count from here, activation and command alike.
    volumes: VolumeBaseline<HostVolume>,
    output_limit: Option<OutputLimitInfo>,
    kill_reason: Option<KillReason>,
    stdout_copy: Option<OutputPublication>,
    stderr_copy: Option<OutputPublication>,
    stdin_delivery: StdinDelivery,
    close_stdin_when_drained: bool,
    close_waiters: Vec<oneshot::Sender<Result<()>>>,
    waiters: Vec<oneshot::Sender<Result<JobInfo>>>,
    kill_waiters: Vec<oneshot::Sender<Result<()>>>,
    log_waiters: Vec<PendingLog>,
    session_identity: Option<u64>,
    /// The job's claim on the build volume it was admitted with, from admission until the job
    /// ends: while it lives, no release unmounts that volume, whatever moved the checkout's link
    /// meanwhile (16_build_volumes.md, "Process lifetime across a swap").
    build_hold: Option<(
        crate::build_volume::BuildVolumeId,
        crate::build_volume::BuildVolumeHold,
    )>,
    /// Where the job stands with the host CPU budget.
    cpu: CpuStanding,
    /// Why the job failed before any status of its own could say, when it did.
    failure: Option<JobFailure>,
}

/// Process, volume and CPU claims remain held through draining and every refusal.
enum RetirementState {
    Unstarted,
    Draining(RetirementRun),
    Ended,
    Refused(Refusal),
}

/// The complete authored environment's identity, before session or activation overlays. Values
/// withheld from the command are excluded; every other name/value binds a keyed request even
/// when it is private and therefore absent from the worker-readable admission projection.
fn admission_env_sha256(
    env: &std::collections::HashMap<String, String>,
    withheld: &BTreeSet<String>,
) -> Sha256Digest {
    let mut entries: Vec<(&String, &String)> = env
        .iter()
        .filter(|(name, _)| !withheld.contains(*name))
        .collect();
    entries.sort_unstable_by_key(|(name, _)| *name);
    let mut digest = Sha256::new();
    for (name, value) in entries {
        for part in [name.as_bytes(), value.as_bytes()] {
            let length = u64::try_from(part.len()).expect("an environment component fits in u64");
            digest.update(length.to_le_bytes());
            digest.update(part);
        }
    }
    Sha256Digest::from_bytes(digest.finalize().into())
}
/// Where a job stands with the host CPU budget (05_gateway.md "Host CPU budget").
enum CpuStanding {
    /// Asking the gateway; the command spawns when the grant arrives.
    Queued(Box<PendingSpawn>),
    /// Cancelled while queued: it left the gateway's queue, and nothing ever spawned.
    Withdrawn,
    /// The grant the command runs under, held until the job concludes, whatever ends it; a
    /// resident's start stake within it returns when its command starts.
    Granted { grant: JobCpuGrant },
    /// Holding nothing: the request was refused, or the job concluded and returned its grant.
    Settled,
}

/// What a queued job's spawn needs once its grant arrives, as admission settled it: the authority
/// and sandbox it was admitted under, never ones adopted while it waited.
struct PendingSpawn {
    /// Aborting it drops the gateway request, which leaves the queue. Its task id tells this
    /// ask's answer from one an earlier, withdrawn ask of the job delivered before its abort.
    asking: tokio::task::AbortHandle,
    /// The count the job asks for, kept to ask again ([`SupervisorActor::rebind_keeper_start`]).
    want: std::num::NonZeroUsize,
    /// The holder's live pool a resident's start stake draws on beside the host, if any.
    parent: Option<String>,
    /// What a resident's start activates, judged at its admission; `None` for a class that
    /// holds tokens, which an activation counts against.
    start: Option<ShellStart>,
    authority: WorkspaceAuthoritySnapshot,
    policy: SandboxPolicy,
    command: SpawnCommand,
    cwd: PathBuf,
    env: BTreeMap<String, String>,
    mode: crate::api::dto::RunSandboxMode,
    scope: ExecScope,
    stdin: StdinSource,
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
        let failure = self
            .wait_failure
            .as_ref()
            .or(self.terminal_cleanup_failure.as_ref())
            .or(match &self.conclusion {
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

    fn processes(&self) -> Result<JobProcessTree> {
        if self.sealed() {
            return Err(observation_released(self.info.job_id));
        }
        #[cfg(target_os = "macos")]
        if let Some(monitor) = &self.monitor {
            return monitor.tree();
        }
        Err(process_observation_unavailable(self.info.job_id))
    }

    fn process_events(
        &self,
        every: SampleInterval,
        reply: oneshot::Sender<Result<JobProcessStream>>,
    ) {
        if self.sealed() {
            let _ = reply.send(Err(observation_released(self.info.job_id)));
            return;
        }
        #[cfg(target_os = "macos")]
        if let Some(monitor) = &self.monitor {
            let monitor = monitor.clone();
            tokio::spawn(async move {
                let _ = reply.send(monitor.subscribe(every.duration()).await);
            });
            return;
        }
        #[cfg(not(target_os = "macos"))]
        let _ = every;
        let _ = reply.send(Err(process_observation_unavailable(self.info.job_id)));
    }

    fn stream(&self, stream: StreamKind) -> (&VecDeque<Bytes>, u64, bool) {
        match stream {
            StreamKind::Stdout => (&self.stdout, self.stdout_tally.bytes(), self.stdout_eof),
            StreamKind::Stderr => (&self.stderr, self.stderr_tally.bytes(), self.stderr_eof),
        }
    }
}

/// A sealed job's process tree and events were released with its seal (07_api.md, "Process-tree
/// observations"): its sealed record is what remains to read.
fn observation_released(job_id: JobId) -> CowshedError {
    CowshedError::conflict(
        format!(
            "job {} is sealed: its process tree and events were released with its sealed record",
            job_id.get()
        ),
        "read the job's sealed record: its exit, containment, outlived processes and terminal resources",
    )
}

fn process_observation_unavailable(job_id: JobId) -> CowshedError {
    #[cfg(target_os = "macos")]
    {
        CowshedError::conflict(
            format!("job {} has no native process observation yet", job_id.get()),
            "read its processes once its native command has started",
        )
    }
    #[cfg(not(target_os = "macos"))]
    {
        CowshedError::environment_missing(
            format!(
                "job {} has no placed Linux native observer: the selected ptrace source is not wired to this supervisor",
                job_id.get()
            ),
            "use a Linux execution substrate with the selected ptrace source and its source-owned reaper",
        )
    }
}

struct SupervisorActor {
    authority: WorkspaceAuthoritySnapshot,
    group_ledger: Option<super::job_groups::Recorder>,
    membership_socket: Option<PathBuf>,
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
    membership_changed: Arc<tokio::sync::Notify>,
    spawner: Box<dyn SpawnSink>,
    artifacts: Box<dyn ArtifactSink>,
    commitments: Box<dyn CommitmentSink>,
    jobs: BTreeMap<JobId, JobStateRecord>,
    sessions: BTreeMap<u64, SessionState>,
    named_sessions: BTreeMap<String, u64>,
    quiesce_waiters: Vec<oneshot::Sender<Result<()>>>,
    retire_waiters: Vec<oneshot::Sender<Result<()>>>,
    /// Retirements asked to wait for the running jobs ([`RetireWhen::AfterJobs`]): taken up
    /// once no job runs, whatever became of their callers.
    retire_after_jobs: Vec<(NxDaemonRetirement, oneshot::Sender<Result<()>>)>,
    /// Callers of [`WorkspaceSupervisorHandle::nx_daemon_keeper`], answered after the next
    /// probe, or with `None` once no keeper is left.
    nx_keeper_waiters: Vec<oneshot::Sender<Result<Option<super::NxKeeperState>>>>,
    command_lane_closed: bool,
    /// A shed's keeper of its sandboxed Nx daemon ([`super::nx_daemon`]); main keeps none.
    nx_daemon: Option<NxDaemonKeeper>,
    /// The build volume this supervisor last asked to be named for its checkout.
    labelled_build: Option<PathBuf>,
    /// Background renames, reaped without delaying provisioning or lifecycle commands.
    labelling: tokio::task::JoinSet<()>,
    /// See [`WorkspaceSupervisorConfig::job_sampling`].
    job_sampling: JobSamplingPolicy,
    /// The job sampling clock: running while a job owns a process.
    sample_clock: Option<tokio::time::Interval>,
    /// See [`WorkspaceSupervisorConfig::cpu`].
    cpu: Arc<dyn JobCpuAdmission>,
    /// See [`WorkspaceSupervisorConfig::build_volume_released`].
    build_volume_released: Option<BuildVolumeReleased>,
    /// Each queued job's CPU request, answered off the actor's path, so status, kill and every
    /// other command are served while jobs wait for their grants.
    admissions: tokio::task::JoinSet<(JobId, Result<JobCpuGrant>)>,
}

/// Runtime teardown transfers the same jobs to one retirement worker. The actor joins for a
/// bounded interval; the worker retains the actual parents and resource claims through native
/// retirement. A refusal stays in the existing ledger for recovery, never a terminal record.
impl Drop for SupervisorActor {
    fn drop(&mut self) {
        if self.jobs.values().all(JobStateRecord::terminal) {
            return;
        }
        self.record_groups();
        let jobs = std::mem::take(&mut self.jobs);
        #[cfg(target_os = "macos")]
        let observed: Vec<_> = jobs
            .iter()
            .filter_map(|(id, job)| job.monitor.as_ref().map(|monitor| (*id, monitor.clone())))
            .collect();
        #[cfg(target_os = "linux")]
        let contained: Vec<_> = jobs
            .iter()
            .filter_map(|(id, job)| job.cgroup.as_ref().map(|cgroup| (*id, Arc::clone(cgroup))))
            .collect();
        let report_retained = || {
            #[cfg(target_os = "macos")]
            for (id, monitor) in &observed {
                cowshed_os::eprintln!(
                    "cowshed: retained job {} observed lives {:?}",
                    id.get(),
                    monitor.running_lives()
                );
            }
            #[cfg(target_os = "linux")]
            for (id, cgroup) in &contained {
                cowshed_os::eprintln!(
                    "cowshed: retained job {} cgroup {:?} at {}",
                    id.get(),
                    cgroup.identity(),
                    cgroup.path().display()
                );
            }
        };
        let ledger = self.group_ledger.take();
        let inherited = std::mem::take(&mut self.inherited_groups);
        let grace = self.term_grace;
        let bound = grace
            .saturating_add(grace.max(Duration::from_secs(1)).saturating_mul(2))
            .saturating_add(Duration::from_secs(1));
        let held = Arc::new(std::sync::Mutex::new(Some((jobs, ledger, inherited))));
        let worker = held.clone();
        let (done, joined) = std::sync::mpsc::sync_channel(1);
        match std::thread::Builder::new()
            .name("cowshed-retire-jobs".into())
            .spawn(move || {
                let (jobs, ledger, inherited) = worker
                    .lock()
                    .expect("retirement handoff mutex poisoned")
                    .take()
                    .expect("one retirement worker");
                let result = retire_dropped_jobs(jobs, ledger.as_ref(), inherited, grace);
                let _ = done.send(result);
            }) {
            Ok(_worker) => match joined.recv_timeout(bound) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => cowshed_os::eprintln!(
                    "cowshed: supervisor drop left unproven job membership in its ledger: {error}"
                ),
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: supervisor drop could not join membership retirement within {bound:?}: {error}; the retirement worker retains the jobs and their ledger claims"
                    );
                    report_retained();
                }
            },
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: cannot start membership retirement during supervisor drop: {error}; retaining native claims and the ownership ledger"
                );
                report_retained();
                // No worker took the claims. Releasing their parent handles here would turn an
                // allocation failure into permission to release unproven membership.
                std::mem::forget(
                    held.lock()
                        .expect("retirement handoff mutex poisoned")
                        .take(),
                );
            }
        }
    }
}

/// Publish every job whose group may still run, with the lives its observation holds outside the
/// group: borrowed from the jobs and their published observations, never collected first.
fn record_job_groups(
    recorder: &super::job_groups::Recorder,
    jobs: &BTreeMap<JobId, JobStateRecord>,
    inherited: &[super::job_groups::UnresolvedGroup],
) -> std::io::Result<super::job_groups::Recorded> {
    let groups = jobs
        .iter()
        // A job sealed after a wait failure may have left its group running: nothing saw it
        // end, so the ledger keeps it for whoever finds this supervisor gone.
        .filter(|(_, job)| !job.sealed() || job.wait_failure.is_some())
        .filter_map(|(job_id, job)| {
            let leader = job.birth.as_ref()?.leader()?;
            #[cfg(target_os = "macos")]
            let members = job.monitor.as_ref().map(ProcessMonitor::running_lives);
            #[cfg(not(target_os = "macos"))]
            let members = None;
            Some(super::job_groups::JobGroupClaim {
                job_id: job_id.get(),
                leader,
                members,
            })
        });
    recorder.record(groups, inherited)
}

/// The sole ledger writer after actor teardown. No process is released before either native
/// exit proof or a successful durable handoff of its refreshed observed membership.
fn retire_dropped_jobs(
    mut jobs: BTreeMap<JobId, JobStateRecord>,
    ledger: Option<&super::job_groups::Recorder>,
    inherited: Vec<super::job_groups::UnresolvedGroup>,
    grace: Duration,
) -> std::io::Result<()> {
    let mut refused: Vec<String> = Vec::new();
    let mut non_native = Vec::new();
    let ids: Vec<_> = jobs.keys().copied().collect();
    for id in ids {
        let job = jobs.get(&id).expect("the retained job exists");
        if job.terminal() {
            jobs.remove(&id);
            continue;
        }
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &job.cgroup {
            let retired = cgroup
                .retire(grace, cowshed_os::deadline::REAP)
                .and_then(|_| cgroup.finish(cowshed_os::deadline::REAP));
            match retired {
                Ok(_) => {
                    jobs.remove(&id);
                }
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: job {} membership remains unproven during supervisor drop: {error}; retained cgroup {:?} at {}",
                        id.get(),
                        cgroup.identity(),
                        cgroup.path().display()
                    );
                    refused.push(format!(
                        "job {}: {error} (cgroup {:?} at {})",
                        id.get(),
                        cgroup.identity(),
                        cgroup.path().display()
                    ));
                }
            }
            continue;
        }
        #[cfg(target_os = "macos")]
        if let (Some(monitor), Some(birth)) = (&job.monitor, &job.birth) {
            match monitor.retire(birth.clone(), grace) {
                Ok(_) => {
                    jobs.remove(&id);
                }
                Err(error) => {
                    let lives = monitor.running_lives();
                    cowshed_os::eprintln!(
                        "cowshed: job {} membership remains unproven during supervisor drop: {}; retained lives {lives:?}",
                        id.get(),
                        error.message
                    );
                    refused.push(format!(
                        "job {}: {} (lives {lives:?})",
                        id.get(),
                        error.message
                    ));
                }
            }
            continue;
        }
        non_native.push(id);
    }
    for id in &non_native {
        if let Some(process) = jobs.get_mut(id).and_then(|job| job.process.as_mut()) {
            report_unsignalled(*id, process.signal_process_tree(ProcessSignal::Term));
        }
    }
    if !non_native.is_empty() {
        std::thread::sleep(grace);
    }
    for id in non_native {
        if let Some(process) = jobs.get_mut(&id).and_then(|job| job.process.as_mut()) {
            report_unsignalled(id, process.signal_process_tree(ProcessSignal::Kill));
        }
    }
    if let Some(recorder) = ledger {
        match record_job_groups(recorder, &jobs, &inherited) {
            Ok(recorded) => {
                for error in recorded.uninspected {
                    cowshed_os::eprintln!(
                        "cowshed: inherited membership remains uninspected during supervisor drop: {error}"
                    );
                }
            }
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: cannot hand unproven membership to {}: {error}; native claims remain held",
                    recorder.path().display()
                );
                std::mem::forget(jobs);
                return Err(error);
            }
        }
    } else if !refused.is_empty() {
        std::mem::forget(jobs);
        return Err(std::io::Error::other(format!(
            "no ownership ledger can receive unproven membership: {}",
            refused.join("; ")
        )));
    }
    if refused.is_empty() {
        Ok(())
    } else {
        Err(std::io::Error::other(refused.join("; ")))
    }
}

fn report_unsignalled(job: JobId, signalled: Result<()>) {
    if let Err(error) = signalled {
        cowshed_os::eprintln!(
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
                self.release_kept_nx_daemon().await;
                break;
            }
            tokio::select! {
                command = self.commands.recv(), if !self.command_lane_closed => {
                    match command {
                        Some(command) => self.handle_command(command).await,
                        None => {
                            self.command_lane_closed = true;
                            if let Some(id) = self.nx_daemon.as_ref().and_then(|keeper| keeper.starting) {
                                let _ = self.begin_kill(id, KillReason::Retire);
                            }
                        },
                    }
                }
                _ = self.membership_changed.notified() => {
                    self.record_groups();
                    self.rerun_refused();
                }
                event = self.event_receiver.recv() => {
                    match event {
                        Some(event) => self.handle_event(event),
                        None => break,
                    }
                }
                granted = self.admissions.join_next_with_id(), if !self.admissions.is_empty() => {
                    match granted {
                        Some(Ok((asked, (job_id, granted)))) => {
                            self.spawn_granted(asked, job_id, granted).await;
                            self.rebind_keeper_start();
                        }
                        // A withdrawn request: its job ended while it was queued.
                        Some(Err(error)) if error.is_cancelled() => {}
                        Some(Err(error)) => std::panic::resume_unwind(error.into_panic()),
                        None => {}
                    }
                }
                labelled = self.labelling.join_next(), if !self.labelling.is_empty() => {
                    if let Some(Err(error)) = labelled {
                        cowshed_os::eprintln!("cowshed: volume label task failed: {error}");
                    }
                }
                () = next_sample_tick(&mut self.sample_clock), if self.sample_clock.is_some() => {
                    self.sample_jobs();
                }
                () = super::nx_daemon::next_probe(&mut self.nx_daemon),
                    if self.nx_daemon.is_some()
                        && self.lifecycle == ActorLifecycle::Running
                        && !self.command_lane_closed =>
                {
                    self.keep_nx_daemon().await;
                    self.answer_nx_keeper_waiters();
                }
            }
            self.finish_ready_jobs().await;
            // A retirement that waited for the jobs takes effect once none runs, in this loop:
            // nothing outside the actor has to come back for it.
            if !self.retire_after_jobs.is_empty() && !self.has_running_jobs() {
                for (nx_daemon, reply) in std::mem::take(&mut self.retire_after_jobs) {
                    self.retire_now(nx_daemon, reply);
                }
            }
            if self.lifecycle == ActorLifecycle::Retiring && !self.has_running_jobs() {
                self.release_kept_nx_daemon().await;
            }
            self.finish_lifecycle_waiters();
            if self.nx_daemon.is_none() {
                self.answer_nx_keeper_waiters();
            }
        }
        self.labelling.abort_all();
    }

    /// Answer every caller waiting on the keeper's decision with what it is now.
    fn answer_nx_keeper_waiters(&mut self) {
        let state = self.nx_daemon.as_ref().map(NxDaemonKeeper::state);
        for waiter in self.nx_keeper_waiters.drain(..) {
            let _ = waiter.send(Ok(state.clone()));
        }
    }

    /// End the Nx daemon this supervisor's keeper kept when it retires.
    async fn release_kept_nx_daemon(&mut self) {
        if self.nx_daemon.take().is_none() {
            return;
        }
        let (read_write, _) = self
            .policy
            .child(crate::api::dto::RunSandboxMode::ReadWrite);
        if let Some(project_root) =
            super::nx_daemon::kept_project(read_write).map(Path::to_path_buf)
        {
            super::nx_daemon::release(&project_root).await;
        }
    }

    /// Arm the sampling clock, if it is not running: a job owns a process now. Its first tick
    /// is one period away; the ownership itself was a boundary.
    fn arm_sample_clock(&mut self) {
        if self.sample_clock.is_none() {
            let period = self.job_sampling.cpu_sample_period;
            let mut clock = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            self.sample_clock = Some(clock);
        }
    }

    /// A tick of the sampling clock: a supervisor boundary for every job that owns a process,
    /// each kept as its latest sample. With none left, the clock stops until a job owns one.
    /// A failed tick leaves the latest sample standing; every read observes again and returns
    /// that failure itself.
    fn sample_jobs(&mut self) {
        let mut live = false;
        let mut spent = Vec::new();
        for (job_id, job) in &mut self.jobs {
            let Some(sampler) = job.sampling.live() else {
                continue;
            };
            live = true;
            if let Ok(sample) = observe(
                sampler,
                &job.volumes,
                job.stdout_tally,
                job.stderr_tally,
                Boundary::Supervisor,
                #[cfg(target_os = "macos")]
                job.monitor.as_ref(),
            ) {
                if let CpuStanding::Granted { grant } = &job.cpu
                    && grant.on_probation()
                    && let Some((wall, cpu)) = probation_spent(&sample)
                {
                    spent.push((*job_id, wall, cpu));
                }
                job.info.resources = Some(sample);
            }
        }
        if !live {
            self.sample_clock = None;
        }
        for (job_id, wall, cpu) in spent {
            self.end_probation(job_id, wall, cpu);
        }
    }

    /// A job on probation reached its bound without a grant (05_gateway.md "A one-token job
    /// starts on probation"): stop its processes where they are, return the probation token, and
    /// say so on its stderr. Its grant continues it.
    fn end_probation(&mut self, job_id: JobId, wall_ms: u64, cpu_ms: u64) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let CpuStanding::Granted { grant } = &mut job.cpu else {
            return;
        };
        let stopped = job
            .process
            .as_mut()
            .map(|process| process.signal_process_tree(ProcessSignal::Stop));
        grant.end_probation();
        job.info.cpu_tokens = None;
        let line = match stopped {
            Some(Ok(())) | None => format!(
                "cowshed: jobserver probation over after {wall_ms} ms, {cpu_ms} ms CPU; stopped \
                 until its token is granted\n"
            ),
            Some(Err(error)) => format!(
                "cowshed: jobserver probation over after {wall_ms} ms, {cpu_ms} ms CPU; the job \
                 could not be stopped ({}), so it runs on outside the budget until its token is \
                 granted\n",
                error.message
            ),
        };
        self.process_output(job_id, StreamKind::Stderr, Bytes::from(line));
    }

    /// A job's probation wait ended: granted, its one token is the job's; or the gateway is gone
    /// without a grant, and the job runs on unaccounted (05_gateway.md "Crash safety by
    /// connection"). A job its probation's end stopped is continued either way.
    fn probation_settled(&mut self, job_id: JobId, outcome: ProbationOutcome) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if job.terminal() {
            return;
        }
        let CpuStanding::Granted { grant } = &mut job.cpu else {
            return;
        };
        let stopped = grant.probation_settled();
        job.info.cpu_tokens = match outcome {
            Ok(()) => grant.tokens(),
            Err(_) => None,
        };
        if !stopped {
            return;
        }
        if let Some(process) = job.process.as_mut()
            && let Err(error) = process.signal_process_tree(ProcessSignal::Continue)
        {
            let line = format!(
                "cowshed: job {} could not be continued after its probation: {}\n",
                job_id.get(),
                error.message
            );
            self.process_output(job_id, StreamKind::Stderr, Bytes::from(line));
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
                let result = self.advance_authority(expected, authority, *sandbox);
                let _ = reply.send(result);
            }
            Command::OpenSession {
                authority,
                name,
                reply,
            } => {
                let named = name.is_some();
                let result = self.open_session(&authority, name);
                if named && result.is_ok() {
                    self.nx_daemon_used();
                }
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
                source,
                cpu_parent,
                reply,
            } => {
                // Answered at admission: the job is durable and queued, or refused.
                let admitted = self
                    .admit_exec(
                        authority,
                        session,
                        build_volume,
                        *request,
                        source,
                        cpu_parent,
                    )
                    .await;
                if admitted.is_ok() {
                    self.nx_daemon_used();
                }
                let _ = reply.send(admitted);
            }
            Command::Detach {
                authority,
                job_id,
                reply,
            } => {
                let result = self
                    .validate_authority(&authority)
                    .and_then(|()| self.detach(job_id));
                let _ = reply.send(result);
            }
            Command::StdinWrite {
                authority,
                job_id,
                offset,
                bytes,
                reply,
            } => self.stdin_write(&authority, job_id, offset, bytes, reply),
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
                    // A job this supervisor holds unsealed has no terminal record yet.
                    // Its own state answers: no store refresh, no unread-history refusal.
                    match self.jobs.get(&job_id) {
                        Some(job) if !job.sealed() => Err(unsealed_job(job_id)),
                        Some(_) | None => self
                            .artifacts
                            .sealed(job_id)?
                            .ok_or_else(|| unsealed_job(job_id)),
                    }
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
            #[cfg(target_os = "macos")]
            Command::SubtreeHandle { peer, reply } => {
                let monitors: Vec<_> = self
                    .jobs
                    .values()
                    .filter(|job| !job.terminal())
                    .filter_map(|job| job.monitor.clone())
                    .collect();
                tokio::task::spawn_blocking(move || {
                    let outcome = (|| {
                        let Some(peer) = membership_peer(&peer)? else {
                            return Ok(JobSubtreeAdmission::Refused {
                                reason: JobSubtreeRefusal::NotMember,
                            });
                        };
                        for monitor in monitors {
                            match monitor.subtree_handle(peer)? {
                                admitted @ JobSubtreeAdmission::Admitted { .. } => {
                                    return Ok(admitted);
                                }
                                JobSubtreeAdmission::Refused {
                                    reason: JobSubtreeRefusal::NotMember,
                                } => {}
                                refused => return Ok(refused),
                            }
                        }
                        Ok(JobSubtreeAdmission::Refused {
                            reason: JobSubtreeRefusal::NotMember,
                        })
                    })();
                    let _ = reply.send(outcome);
                });
            }
            #[cfg(target_os = "macos")]
            Command::RetireDescendants {
                peer,
                handle,
                grace,
                reply,
            } => {
                // A child may shorten, but never extend, its owner's termination bound.
                let grace = grace.min(self.term_grace);
                let Some(job) = self.jobs.get(&handle.job_id) else {
                    let _ = reply.send(Ok(JobSubtreeRetirement::Refused {
                        reason: JobSubtreeRefusal::ForeignHandle,
                    }));
                    return;
                };
                if job.terminal() {
                    let _ = reply.send(Ok(JobSubtreeRetirement::Refused {
                        reason: JobSubtreeRefusal::JobEnded,
                    }));
                    return;
                }
                let Some(monitor) = job.monitor.clone() else {
                    let _ = reply.send(Ok(JobSubtreeRetirement::Refused {
                        reason: JobSubtreeRefusal::FoldUnavailable,
                    }));
                    return;
                };
                tokio::task::spawn_blocking(move || {
                    let outcome = (|| {
                        let Some(peer) = membership_peer(&peer)? else {
                            return Ok(JobSubtreeRetirement::Refused {
                                reason: JobSubtreeRefusal::NotMember,
                            });
                        };
                        if !monitor.owns_live(peer)? {
                            return Ok(JobSubtreeRetirement::Refused {
                                reason: JobSubtreeRefusal::NotMember,
                            });
                        }
                        monitor.retire_subtree(handle, grace)
                    })();
                    let _ = reply.send(outcome);
                });
            }
            Command::Processes {
                authority,
                job_id,
                reply,
            } => {
                let result = self
                    .validate_authority(&authority)
                    .and_then(|()| self.job(job_id))
                    .and_then(JobStateRecord::processes);
                let _ = reply.send(result);
            }
            Command::ProcessEvents {
                authority,
                job_id,
                every,
                reply,
            } => {
                match self
                    .validate_authority(&authority)
                    .and_then(|()| self.job(job_id))
                {
                    Ok(job) => job.process_events(every, reply),
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
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
                        AdmissionLookup::Admitted { job_id, .. } => Ok(Some(job_id)),
                        AdmissionLookup::Absent => Ok(None),
                        AdmissionLookup::Unprovable { set_aside } => Err(
                            CowshedError::admission_refusal(AdmissionRefusal::Unprovable {
                                set_aside: set_aside.to_path_buf(),
                            }),
                        ),
                    }
                });
                let _ = reply.send(result);
            }
            Command::Progress {
                authority,
                job_id,
                reply,
            } => {
                let result = self
                    .validate_authority(&authority)
                    .and_then(|()| self.job_mut(job_id))
                    .and_then(job_progress);
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
            Command::Quiesce { authority, reply } => {
                if let Err(error) = self.validate_authority(&authority) {
                    let _ = reply.send(Err(error));
                } else if self.lifecycle == ActorLifecycle::Retired {
                    let _ = reply.send(Ok(()));
                } else if let Some(error) = self.jobs.values().find_map(|job| {
                    if let RetirementState::Refused(refusal) = &job.retirement {
                        Some(refusal.error().clone())
                    } else {
                        None
                    }
                }) {
                    let _ = reply.send(Err(error));
                } else {
                    self.close_admission();
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
            Command::Retire {
                authority,
                nx_daemon,
                when,
                reply,
            } => {
                if let Err(error) = self.validate_authority(&authority) {
                    let _ = reply.send(Err(error));
                } else if self.lifecycle == ActorLifecycle::Retired {
                    let _ = reply.send(Ok(()));
                } else {
                    match when {
                        RetireWhen::Now => self.retire_now(nx_daemon, reply),
                        RetireWhen::IfIdle if self.has_user_jobs() => {
                            let _ = reply.send(Err(CowshedError::conflict(
                                "the workspace runs jobs, so its supervisor was not retired",
                                "let its jobs finish, then retry",
                            )));
                        }
                        RetireWhen::IfIdle => self.retire_now(nx_daemon, reply),
                        RetireWhen::AfterJobs => {
                            self.close_admission();
                            self.retire_after_jobs.push((nx_daemon, reply));
                        }
                    }
                }
            }
            Command::CurrentAuthority { reply } => {
                let _ = reply.send(Ok(self.authority.clone()));
            }
            #[cfg(target_os = "macos")]
            Command::Idle { reply } => {
                let idle = self.named_sessions.is_empty()
                    && !self.has_user_jobs()
                    && self.nx_daemon.as_ref().is_none_or(|keeper| keeper.settled);
                let _ = reply.send(Ok(idle));
            }
            // Answered after the next probe, or now when no probe is coming.
            Command::NxDaemonKeeper { reply } => match &self.nx_daemon {
                Some(_)
                    if self.lifecycle == ActorLifecycle::Running && !self.command_lane_closed =>
                {
                    self.nx_keeper_waiters.push(reply);
                }
                keeper => {
                    let _ = reply.send(Ok(keeper.as_ref().map(NxDaemonKeeper::state)));
                }
            },
            Command::StopNxKeeper { authority, reply } => self.stop_nx_keeper(&authority, reply),
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
        match record_job_groups(path, &self.jobs, &self.inherited_groups) {
            Ok(recorded) => {
                for error in &recorded.uninspected {
                    cowshed_os::eprintln!(
                        "cowshed: an inherited unresolved process group in {} could not be \
                     inspected and is carried as it was: {error}",
                        path.path().display()
                    );
                }
                self.inherited_groups = recorded.carried;
            }
            Err(error) => {
                // The jobs run either way; what is lost is only the means to end them should
                // this supervisor die, which is worth saying where the daemon log shows it.
                cowshed_os::eprintln!(
                    "cowshed: cannot record the job process groups at {}: {error}",
                    path.path().display()
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
        let policy = job_sandbox_policy(sandbox, self.membership_socket.as_deref())?;
        if authority != self.authority {
            self.sessions.clear();
            self.named_sessions.clear();
        }
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
    /// image's volume when `workspace`, and the build volume mounted at `build` unless this
    /// supervisor already named that one. Each is named by its image, never by the path it is
    /// mounted at; a volume is renamed, through its held mount root, only when the file system's
    /// own name differs, and the outcome goes to stderr.
    fn relabel(&mut self, workspace: bool, build: Option<&Path>) {
        let Some(labels) = &self.volume_labels else {
            return;
        };
        // What each is, the build link that authorizes it, the image named, and its label.
        let mut volumes = Vec::new();
        if workspace {
            volumes.push((
                "volume",
                None,
                labels.workspace_image.clone(),
                labels.workspace.clone(),
            ));
        }
        if let Some(build) = build
            && self.labelled_build.as_deref() != Some(build)
            && let Some(layout) = &self.build_volume_layout
            && let Some(id) = layout.volume_at(build)
        {
            self.labelled_build = Some(build.to_owned());
            volumes.push((
                "build volume",
                Some(build.to_owned()),
                layout.image(&id),
                labels.build.clone(),
            ));
        }
        if volumes.is_empty() {
            return;
        }
        let labeller = Arc::clone(&labels.labeller);
        let name = self.authority.workspace.clone();
        let checkout = self.workspace_root.clone();
        let layout = self.build_volume_layout.clone();
        self.labelling.spawn_blocking(move || {
            for (what, linked, image, label) in volumes {
                // Links are not authority. After a land, this checkout may still link the
                // adopted volume, but its sidecar belongs to the target, so it cannot name it.
                if let Some(mount) = linked {
                    let Some(layout) = &layout else {
                        continue;
                    };
                    match layout.grant(&name, &checkout) {
                        Ok(Some(granted)) if granted == mount => {}
                        Ok(_) => continue,
                        Err(error) => {
                            cowshed_os::eprintln!("cowshed: could not authorize workspace {name}'s build volume label at {}: {error}",
                            mount.display());
                            continue;
                        }
                    }
                }
                let started = Instant::now();
                let outcome = labeller.ensure_label(&image, &label);
                if let Some(line) =
                    label_report(&name, what, &image, &label, &outcome, started.elapsed())
                {
                    cowshed_os::eprintln!("{line}");
                }
            }
        });
    }

    /// Bind the authored request before a session lends cwd/environment: only public environment
    /// names are projected in plaintext, while the complete non-withheld environment binds the
    /// key through its digest. Activation and execution retain their existing environment rules.
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
            StdinSource::Stream(_) | StdinSource::Open => AdmittedStdin::Stream,
            StdinSource::WorkspaceFile(path) => AdmittedStdin::WorkspaceFile(path.clone()),
        };
        Ok(JobAdmission {
            key,
            cwd: request.cwd.clone(),
            mode: request.mode,
            env: request
                .env
                .iter()
                .filter(|(name, _)| {
                    !self.credential_env_names.contains(*name)
                        && crate::api::dto::host_gate_env_name_allowed(name)
                })
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            env_sha256: admission_env_sha256(&request.env, &self.credential_env_names),
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
            want: request.want,
            stdout_copy: request.stdout_copy.clone(),
            stderr_copy: request.stderr_copy.clone(),
        })
    }

    /// Compare the authored request by borrowing it; only a new admission needs owned metadata.
    /// Its want is compared as the caller named it: an unnamed want is the same request whatever
    /// share each supervisor resolved it to.
    fn admission_differences(
        &self,
        admitted: &JobAdmission,
        command: &ExecCommand,
        scheduling: JobScheduling,
        request: &ExecRequest,
        session: Option<&SessionToken>,
    ) -> Vec<AdmissionField> {
        let env_matches =
            admitted.env_sha256 == admission_env_sha256(&request.env, &self.credential_env_names);
        let stdin_matches = match &request.stdin {
            StdinSource::Empty => matches!(admitted.stdin, AdmittedStdin::Empty),
            StdinSource::Inline(input) => matches!(
                &admitted.stdin,
                AdmittedStdin::Inline { sha256, bytes }
                    if *bytes == byte_count(input.len()) && *sha256 == Sha256Digest::compute(input)
            ),
            StdinSource::Stream(_) | StdinSource::Open => {
                matches!(admitted.stdin, AdmittedStdin::Stream)
            }
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
            (AdmissionField::Scope, scheduling.scope() != request.scope),
            (AdmissionField::Class, scheduling.class() != request.class),
            (AdmissionField::Want, admitted.want != request.want),
        ]
        .into_iter()
        .filter_map(|(field, differs)| differs.then_some(field))
        .collect()
    }

    /// Admit one job, granted `build_volume`, and ask the host CPU budget for it: once this
    /// returns the job is durable and queued, and its command spawns when the grant arrives
    /// ([`Self::spawn_granted`]). The request is fenced first: only a coordinator's exec runs a
    /// gate or reaches the host boundary. A keyed repeat answers the job it already admitted.
    async fn admit_exec(
        &mut self,
        authority: WorkspaceAuthoritySnapshot,
        session: Option<SessionToken>,
        build_volume: Option<PathBuf>,
        request: ExecRequest,
        source: AdmissionSource,
        cpu_parent: Option<String>,
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
        source.admit(request.class, request.scope)?;
        // The host boundary takes no session overlay a worker controls. Environment names do
        // not restrict execution; the public-name projection is applied only when recording.
        crate::api::dto::validate_host_gate_request(request.scope, session.is_some())?;
        let share = self.cpu.background_share();
        let scheduling =
            JobScheduling::requested(request.scope, request.class, request.want, share)
                .ok_or_else(|| {
                    CowshedError::usage(
                        "a resident holds no CPU token, so it wants none: its descendants ask its \
                     pool for their own",
                        "drop --want from a resident exec",
                    )
                })?;
        // The host boundary has no sandbox to narrow and no exec host to interpret a script.
        if scheduling.scope() == ExecScope::HostGate {
            if matches!(request.command, ExecCommand::Script(_)) {
                return Err(CowshedError::usage(
                    "a host exec runs an argv; a script runs only in the workspace's sandboxed \
                     exec host",
                    "pass the command as an argv",
                ));
            }
            if request.mode == crate::api::dto::RunSandboxMode::ReadOnly {
                return Err(CowshedError::usage(
                    "a host exec runs outside every sandbox, so it cannot be narrowed read-only",
                    "run it read-write, or in the workspace scope",
                ));
            }
        }
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
                    scheduling: bound,
                } => {
                    let fields = self.admission_differences(
                        admitted,
                        command,
                        bound,
                        &request,
                        session.as_ref(),
                    );
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
        let child = self.policy.child(request.mode).0;
        let (developer_links, install_observation) = if child.mode
            == crate::sandbox::RunSandboxMode::ReadWrite
            && child.capabilities.active.iter().any(|id| {
                matches!(
                    id,
                    crate::capabilities::CapabilityId::Bun
                        | crate::capabilities::CapabilityId::Npm
                        | crate::capabilities::CapabilityId::Pnpm
                )
            }) {
            let exec_temp = child.exec_temp_dir.clone();
            let observation = InstallObservation::new(&exec_temp)?;
            let root = self.workspace_root.clone();
            let stores = crate::developer_links::PackageStores::from_home(&child.home)?;
            let links = tokio::task::spawn_blocking(move || {
                crate::developer_links::DeveloperLinks::capture_job(&root, stores)
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("developer-link snapshot failed: {error}"))
            })??;
            (links, Some(observation))
        } else {
            (crate::developer_links::DeveloperLinks::default(), None)
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
            let _span = cowshed_os::timing::span("admit", "build-volume-profile");
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
            class: _,
            want: _,
            scope: _,
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
        // Callers cannot redirect a job's capability requests to another endpoint.
        merged_env.remove(super::supervisor_socket::MEMBERSHIP_SOCKET_ENV);
        if let Some(socket) = &self.membership_socket {
            let socket = socket.to_str().ok_or_else(|| {
                CowshedError::integrity(
                    "membership endpoint path is not UTF-8",
                    "use a UTF-8 controller store path",
                )
            })?;
            merged_env.insert(
                super::supervisor_socket::MEMBERSHIP_SOCKET_ENV.to_owned(),
                socket.to_owned(),
            );
        }
        if let Some(marker) = &install_observation {
            merged_env.insert(
                crate::developer_links::INSTALL_OBSERVED_ENV.to_owned(),
                marker.path.to_string_lossy().into_owned(),
            );
        }
        let job_id = self.next_job_id;
        let expected_next = job_id
            .get()
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("job id allocation exhausted"))
            .and_then(|value| {
                JobId::new(value).map_err(|error| CowshedError::internal(error.to_string()))
            })?;
        #[cfg(target_os = "linux")]
        let cgroup = self.spawner.admit_cgroup(&self.authority, job_id)?;
        {
            let _span = cowshed_os::timing::span("admit", "record");
            let admitted = self.artifacts.admit(
                job_id,
                self.authority.grant_revision,
                &command,
                admission,
                scheduling,
            );
            #[cfg(target_os = "linux")]
            let admitted = admitted.map_err(|error| refused_cgroup(error, cgroup.as_deref()));
            admitted?;
        }
        self.next_job_id = expected_next;
        let admission = cowshed_os::timing::spanned(
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
            #[cfg(target_os = "linux")]
            let error = refused_cgroup(error, cgroup.as_deref());
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
                #[cfg(target_os = "linux")]
                let error = refused_cgroup(error, cgroup.as_deref());
                return Err(error);
            }
        };
        let trace = trace.unwrap_or_else(TraceContext::mint);
        let info = JobInfo {
            repo_id: self.authority.repo_id.clone(),
            workspace_incarnation: self.authority.workspace_incarnation.clone(),
            job_id,
            state: JobState::Queued,
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
            class: scheduling.class(),
            scope: scheduling.scope(),
            nice: None,
            cpu_tokens: None,
            containment: None,
            outlived: Vec::new(),
            outlived_incomplete: false,
        };
        // The admission's own view of the volumes, for a job that ends before it spawns; the
        // spawn reads them again immediately before the first process is dispatched.
        let volumes = {
            let _span = cowshed_os::timing::span("admit", "volume-baseline");
            VolumeBaseline::admitted(
                self.workspace_volume.as_ref(),
                self.policy.ceiling().build_volume_mount.as_deref(),
            )
        };
        if let Some(job_spans) = &self.job_spans {
            job_spans.record(
                &info,
                JobSpanEdge::Start {
                    host: admission_host(job_id),
                },
            );
        }
        let cwd = cwd
            .as_ref()
            .map(WorkspacePath::as_path)
            .map(Path::to_path_buf)
            .unwrap_or_default();
        let start = match scheduling.class() {
            ExecClass::Resident | ExecClass::InteractiveResident => Some(
                self.spawner
                    .shell_start(&self.policy, &cwd, mode, scheduling.scope())
                    .await,
            ),
            ExecClass::Background | ExecClass::Interactive | ExecClass::Gate => None,
        };
        let asking = self.ask_cpu(
            job_id,
            scheduling.class(),
            scheduling.want(),
            command_label(&info.command),
            cpu_parent.clone(),
            start,
        );
        let job = JobStateRecord {
            info,
            started_at: Instant::now(),
            process: None,
            retirement: RetirementState::Unstarted,
            #[cfg(target_os = "linux")]
            cgroup,
            stdout: VecDeque::new(),
            stderr: VecDeque::new(),
            stdout_tally: StreamTally::default(),
            stderr_tally: StreamTally::default(),
            stdout_eof: false,
            stderr_eof: false,
            exit: None,
            wait_failure: None,
            developer_links,
            install_observation,
            terminal_cleanup_failure: None,
            conclusion: None,
            birth: None,
            sampling: Sampling::Unowned,
            #[cfg(target_os = "macos")]
            monitor: None,
            #[cfg(target_os = "macos")]
            kept_nx_daemon: false,
            volumes,
            output_limit: None,
            kill_reason: None,
            stdout_copy,
            stderr_copy,
            stdin_delivery: StdinDelivery::default(),
            close_stdin_when_drained: false,
            close_waiters: Vec::new(),
            waiters: Vec::new(),
            kill_waiters: Vec::new(),
            log_waiters: Vec::new(),
            session_identity,
            build_hold,
            cpu: CpuStanding::Queued(Box::new(PendingSpawn {
                asking,
                want: scheduling.want(),
                parent: cpu_parent,
                start,
                authority: self.authority.clone(),
                // Rendered when this authority was taken. The request may narrow the ceiling,
                // never widen it, and narrowing one job alters no other job.
                policy: self.policy.clone(),
                command: spawn_command,
                cwd,
                env: merged_env,
                mode,
                scope: scheduling.scope(),
                stdin,
            })),
            failure: None,
        };
        self.jobs.insert(job_id, job);
        Ok(job_id)
    }

    /// Leave `job_id` running with nobody attached: both captured prefixes become durable in
    /// their protected files before this answers, and a session's job joins its session's
    /// background set until it ends. A job that ended is sealed, so it is durable already.
    fn detach(&mut self, job_id: JobId) -> Result<()> {
        let job = self.job(job_id)?;
        if job.terminal() {
            return Ok(());
        }
        let session_identity = job.session_identity;
        self.artifacts.prepare_background(job_id)?;
        if let Some(identity) = session_identity
            && let Some(session) = self.sessions.get_mut(&identity)
        {
            session.background_jobs.insert(job_id);
        }
        Ok(())
    }

    /// The host CPU budget answered `asked`, a queued job's ask: spawn its command under the
    /// grant, or fail the job with the refusal. A job that already ended, or one asking again
    /// since (`asked` is not its current ask), returns the grant at once, by dropping it.
    async fn spawn_granted(
        &mut self,
        asked: tokio::task::Id,
        job_id: JobId,
        granted: Result<JobCpuGrant>,
    ) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if job.terminal()
            || !matches!(&job.cpu, CpuStanding::Queued(pending) if pending.asking.id() == asked)
        {
            return;
        }
        let CpuStanding::Queued(pending) = std::mem::replace(&mut job.cpu, CpuStanding::Settled)
        else {
            return;
        };
        let PendingSpawn {
            asking: _,
            want: _,
            parent,
            start: _,
            authority,
            policy,
            command,
            cwd,
            env,
            mode,
            scope,
            stdin,
        } = *pending;
        let grant = match granted {
            Ok(grant) => grant,
            Err(error) => {
                job.failure = Some(JobFailure::CpuAdmission);
                self.fail_before_command(job_id, KillReason::CpuAdmission, error)
                    .await;
                return;
            }
        };
        let cpu = CpuPlacement {
            held: grant.held().cloned(),
            pool: grant.pool().to_path_buf(),
            nice: class_nice(own_nice(), job.info.class),
            class: job.info.class,
            activation: if grant.held().is_some() || grant.holds_start() {
                ActivationCover::Held
            } else {
                ActivationCover::Unheld { parent }
            },
        };
        job.info.cpu_tokens = grant.tokens();
        job.info.state = JobState::Running;
        let mut grant = grant;
        // A job lent the probation token hears its grant on a task of its own.
        if let Some(outcome) = grant.probation_outcome() {
            let events = self.events.clone();
            tokio::spawn(async move {
                // A grant dropped before its wait ended reports to a job that has concluded.
                let outcome = outcome
                    .await
                    .unwrap_or_else(|_| Err("the probation's wait was dropped".to_owned()));
                let _ = events
                    .send(ProcessEvent::Probation { job_id, outcome })
                    .await;
            });
        }
        job.cpu = CpuStanding::Granted { grant };
        // Read before the job's first process is dispatched, so nothing it writes precedes its
        // baseline: a warm host may start the command before its start is reported here. Its
        // run counts from here too: the wait for the host is not the command's time.
        job.volumes = VolumeBaseline::admitted(
            self.workspace_volume.as_ref(),
            policy.ceiling().build_volume_mount.as_deref(),
        );
        job.started_at = Instant::now();
        let spawn_span = cowshed_os::timing::span("admit", "spawn");
        let spawn = self
            .spawner
            .spawn(
                ProcessSpawnRequest {
                    authority,
                    job_id,
                    #[cfg(target_os = "linux")]
                    cgroup: job.cgroup.clone(),
                    command,
                    cwd,
                    env,
                    policy,
                    mode,
                    scope,
                    cpu,
                },
                self.events.clone(),
            )
            .await;
        drop(spawn_span);
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let process = match spawn {
            Ok(process) => process,
            Err(error) => {
                job.failure = Some(unlaunched(&error));
                self.fail_before_command(job_id, KillReason::SpawnFailure, error)
                    .await;
                return;
            }
        };
        #[cfg(target_os = "macos")]
        {
            job.monitor = process.observations().cloned();
            if let Some(monitor) = &job.monitor
                && let Err(error) = monitor.ledger_changes(self.membership_changed.clone())
            {
                cowshed_os::eprintln!(
                    "cowshed: cannot observe job {} membership for its ledger: {error}",
                    job_id.get()
                );
            }
            if let (Some(monitor), Some(publisher)) = (&job.monitor, &self.job_spans)
                && let Err(error) = monitor.trace(publisher.clone(), &job.info)
            {
                cowshed_os::eprintln!(
                    "cowshed: attaching job {}'s process spans: {error}",
                    job_id.get()
                );
            }
            clock_observation(job);
        }
        if let Some(owned) = process.process() {
            // A one-shot command is the job's first process: it has started.
            command_started(job_id, job);
            own_process(job, owned.clone(), self.job_sampling.leaf_coverage);
            adopt_birth(job, self.group_ledger.is_some(), owned.birth.clone());
            self.arm_sample_clock();
        }
        let job = self.jobs.get_mut(&job_id).expect("the job just spawned");
        job.info.nice = process.nice();
        job.process = Some(process);
        let started = job.birth.is_some();
        if started {
            self.record_groups();
        }
        launch_stdin_pump(
            job_id,
            stdin,
            self.workspace_root.clone(),
            self.events.clone(),
        );
        // Input a client wrote while the job waited for its grant goes to the command now.
        self.flush_stdin(job_id);
    }

    /// End a job whose command never ran, failed by `error`: its exit is the failure's wrapper
    /// code, and its caller, answered at admission, reads why on the job's stderr.
    async fn fail_before_command(
        &mut self,
        job_id: JobId,
        reason: KillReason,
        error: CowshedError,
    ) {
        self.process_output(
            job_id,
            StreamKind::Stderr,
            Bytes::from(format!("cowshed: {}\n", error.message)),
        );
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        job.stdout_eof = true;
        job.stderr_eof = true;
        job.exit = Some(ExitStatus::Exited {
            code: error.exec_wrapper_exit_code().into(),
        });
        job.kill_reason = Some(reason);
        end_job_stdin(job);
        self.finalize_job(job_id, Some(JobState::Failed)).await;
    }

    /// Hold the build volume mounted at `grant` for one job (16_build_volumes.md, "Garbage
    /// collection"): a land that moves the checkout's link meanwhile leaves the volume unlinked,
    /// and the hold is what tells its release that this job still runs there. Refused while a
    /// release or direct seed capture claims the volume, so no job starts during deletion or
    /// canonical restoration before the seed's freshness mark.
    fn hold_build_volume(
        &self,
        grant: Option<&Path>,
    ) -> Result<
        Option<(
            crate::build_volume::BuildVolumeId,
            crate::build_volume::BuildVolumeHold,
        )>,
    > {
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
                    "build volume grant {} is not one of this project's build volumes; expected a volume under {} (root aliases are resolved)",
                    grant.display(),
                    layout.mounts().display(),
                ),
                "cowshed doctor --json",
            )
        })?;
        match layout.admit(&id) {
            Ok(hold) => Ok(Some((id, hold))),
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

    /// Ask the host budget for `job_id`'s CPU, off the actor's path: the actor keeps serving
    /// every command while the gateway queues the request. Aborting the returned handle -- a
    /// withdrawal, or this supervisor ending -- drops the request and closes its connection,
    /// which leaves the queue. A resident holds no token for its life, but its shell's
    /// activation is work: a resident whose `start` activates first waits for one token, which
    /// it holds until its command starts, drawing on `parent`, a holder's live pool, when it
    /// names one; one whose start activates nothing asks its pool at once (05_gateway.md "A
    /// resident's start holds one token").
    fn ask_cpu(
        &mut self,
        job_id: JobId,
        class: ExecClass,
        want: std::num::NonZeroUsize,
        command: String,
        parent: Option<String>,
        start: Option<ShellStart>,
    ) -> tokio::task::AbortHandle {
        let checkout = self.workspace_root.clone();
        let start = start.filter(|start| start.stakes()).map(|_| {
            self.cpu
                .activation(checkout.clone(), command.clone(), parent.clone())
        });
        let ask = self.cpu.admit(JobCpuAsk {
            class,
            want,
            checkout,
            command,
            job: (
                self.authority.repo_id.clone(),
                self.authority.workspace_incarnation.clone(),
                job_id,
            ),
            parent,
        });
        self.admissions.spawn(super::job_cpu::reporting(
            job_id,
            self.events.clone(),
            async move {
                let granted = match start {
                    Some(start) => match start.await {
                        Ok(stake) => ask.await.map(|grant| grant.holding_until_started(stake)),
                        Err(error) => Err(error),
                    },
                    None => ask.await,
                };
                (job_id, granted)
            },
        ))
    }

    /// A kept daemon's start still waiting at the host, asked under no holder, draws on the
    /// oldest running gate job's pool once one runs (05_gateway.md "A kept daemon a land waits on
    /// starts as the land's gate task"): a land waits on that daemon, so its start is the gate's
    /// work whenever the gate began. Its background ask is withdrawn, leaving the host queue, and
    /// asked again with the gate's pool beside the host. A start already drawing on a pool keeps
    /// it, and one with no gate running keeps waiting as background.
    fn rebind_keeper_start(&mut self) {
        let Some(pool) = self.gate_pool() else {
            return;
        };
        let Some(job_id) = self.nx_daemon.as_ref().and_then(|keeper| keeper.starting) else {
            return;
        };
        let Some(job) = self.jobs.get(&job_id) else {
            return;
        };
        let CpuStanding::Queued(pending) = &job.cpu else {
            return;
        };
        if job.terminal() || pending.parent.is_some() {
            return;
        }
        pending.asking.abort();
        let (class, want, command, start) = (
            job.info.class,
            pending.want,
            command_label(&job.info.command),
            pending.start,
        );
        cowshed_os::eprintln!(
            "cowshed: the Nx daemon start job {} waited at the host as background; a gate job runs now, so its start asks again on that gate's pool {pool}",
            job_id.get()
        );
        let asking = self.ask_cpu(job_id, class, want, command, Some(pool.clone()), start);
        if let Some(job) = self.jobs.get_mut(&job_id)
            && let CpuStanding::Queued(pending) = &mut job.cpu
        {
            pending.asking = asking;
            pending.parent = Some(pool);
        }
    }

    /// The CPU pool of the oldest gate job this supervisor runs: a land's gate or its rebase's
    /// task-hash job, whose tasks ask the host through that pool at the gate's rank. A kept daemon's
    /// start draws its stake on it (05_gateway.md "A resident's start holds one token"), since the
    /// land waits on that daemon; with no gate running, the start asks the host as background.
    fn gate_pool(&self) -> Option<String> {
        self.jobs
            .values()
            .find_map(|job| match (job.info.class, &job.cpu) {
                (ExecClass::Gate, CpuStanding::Granted { grant }) => {
                    Some(grant.pool().to_string_lossy().into_owned())
                }
                _ => None,
            })
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

    /// A job or a named session was just admitted: the shed is in use (04_sandbox.md, the kept
    /// daemon), so the keeper hears it now and probes at once rather than at its next tick. A
    /// job that ends between two probes is use all the same; the ledger held it.
    fn nx_daemon_used(&mut self) {
        if let Some(keeper) = &mut self.nx_daemon {
            keeper.used();
        }
    }

    /// One probe of a shed's Nx daemon: observe the foreground keeper job independently of
    /// developer activity, report its terminal failure, then judge whether the shed still
    /// wants a daemon ([`NxDaemonKeeper::judge`]), stopping one it no longer wants; start the
    /// daemon, inside the sandbox, when the shed wants one, the probe finds none live and the
    /// keeper holds no start. Nobody waits for the start, so a failure is said where the
    /// supervisor's own failures go. A start that can never run is refused, named, and looked at
    /// again at each probe without a job; one that failed otherwise is tried again at the next
    /// probe, up to [`super::nx_daemon::START_ATTEMPTS`] in a row, then after each pause
    /// ([`super::nx_daemon::FIRST_PAUSE`] doubling to [`super::nx_daemon::PAUSE_CAP`]).
    async fn keep_nx_daemon(&mut self) {
        let Some(keeper) = &mut self.nx_daemon else {
            return;
        };
        let (read_write, _) = self
            .policy
            .child(crate::api::dto::RunSandboxMode::ReadWrite);
        let Some(project_root) = super::nx_daemon::kept_project(read_write).map(Path::to_path_buf)
        else {
            keeper.settled = true;
            return;
        };
        let daemon =
            super::nx_daemon::probe(&crate::capabilities::nx::daemon_record(&project_root));
        let running = keeper
            .starting
            .is_some_and(|id| self.jobs.get(&id).is_some_and(|job| !job.terminal()));
        keeper.observe(daemon, &project_root);
        if !running && let Some(job_id) = keeper.starting.take() {
            let job = self.jobs.get(&job_id).map(|job| &job.info);
            keeper.start_ended(
                job_id,
                super::nx_daemon::start_ending(job),
                &project_root,
                daemon,
            );
        }
        // The keeper's foreground job is maintenance, not use by the developer. It still belongs
        // to the ordinary job ledger and every lifecycle drain waits for its real conclusion.
        let working = self
            .jobs
            .iter()
            .find(|(id, job)| Some(**id) != keeper.starting && !job.terminal())
            .map(|(id, _)| super::nx_daemon::Use::Job(*id))
            .or_else(|| {
                self.named_sessions
                    .keys()
                    .next()
                    .cloned()
                    .map(super::nx_daemon::Use::Session)
            });
        if keeper.judge(&project_root, working, daemon).await == Verdict::Release {
            return;
        }
        if daemon == Probe::Live || running {
            return;
        }
        // A census of the mount stopped the keeper's starts until the shed's next use.
        if keeper.stopped() {
            return;
        }
        // The job's `node` is found as its own exec would find it: on the PATH it gets.
        let path = match read_write.capabilities.contribution.shell {
            Some(_) => None,
            None => match bootstrap_path(read_write) {
                Ok(path) => Some(path),
                Err(error) => {
                    keeper.refuse(format!(
                        "the Nx daemon of {} cannot start: {}",
                        project_root.display(),
                        error.message
                    ));
                    return;
                }
            },
        };
        let node = path.as_deref().map_or(
            super::nx_daemon::NodeSource::Shell,
            super::nx_daemon::NodeSource::Path,
        );
        let Some(request) = keeper.start(&self.workspace_root, &project_root, node) else {
            return;
        };
        let authority = self.authority.clone();
        let gate_pool = self.gate_pool();
        let started = match self.current_build_volume_grant() {
            // The keeper is the supervisor's own coordinator-side work.
            Ok(grant) => {
                // The keeper is a resident: it holds no token. A land waits on its daemon, so while
                // the shed runs a gate job its start's stake draws on that gate's pool.
                self.admit_exec(
                    authority,
                    None,
                    grant,
                    request,
                    AdmissionSource::Coordinator,
                    gate_pool,
                )
                .await
            }
            Err(error) => Err(error),
        };
        let Some(keeper) = &mut self.nx_daemon else {
            return;
        };
        match started {
            Ok(job_id) => {
                keeper.starting = Some(job_id);
                #[cfg(target_os = "macos")]
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.kept_nx_daemon = true;
                }
                // Nobody attaches to the keeper's job: it runs detached from its admission.
                if let Err(error) = self.detach(job_id) {
                    cowshed_os::eprintln!(
                        "cowshed: the Nx daemon job {} of {} runs, but its captured output is not durable: {}",
                        job_id.get(),
                        project_root.display(),
                        error.message
                    );
                }
            }
            Err(error) => keeper.start_failed(format!(
                "the Nx daemon of {} did not start in the sandbox: {}",
                project_root.display(),
                error.message
            )),
        }
    }

    /// Takes `bytes` as the job's input from byte `offset` on, answering once the pump wrote
    /// them to the child's stdin. A write continues the input at its next byte. One that lies
    /// within input the job already holds -- a writer repeating a write whose answer it lost --
    /// delivers nothing again, and is answered once those bytes reached the child, but only when
    /// its bytes equal them. Anything else is a [`StdinRefusal`] naming the cursor.
    fn stdin_write(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        offset: u64,
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
        let delivery = &mut job.stdin_delivery;
        let cursor = delivery.delivered;
        if let Some(broken) = &delivery.broken {
            let _ = reply.send(Err(stdin_delivery_unknown(cursor, broken)));
            return;
        }
        // A close that waits for the lane already ended this stdin for every later write.
        if job.conclusion.is_some() || job.info.stdin.complete || job.close_stdin_when_drained {
            let _ = reply.send(Err(stdin_ended(cursor)));
            return;
        }
        let admitted = delivery.admitted();
        let end = offset.checked_add(byte_count(bytes.len()));
        if let Some(end) = end
            && end <= admitted
        {
            let answer = match delivery.replay(offset, &bytes) {
                Replay::Equal if end <= cursor => Ok(()),
                Replay::Equal => {
                    delivery.waiters.push((end, reply));
                    return;
                }
                Replay::Mismatch => Err(CowshedError::stdin_refusal(
                    StdinRefusal::ReplayMismatch { offset, cursor },
                    format!(
                        "job stdin write at byte {offset} repeats input the job already holds, \
                         with other bytes; {cursor} bytes reached the job"
                    ),
                    "resend the input exactly as first written, or resume at the job's cursor",
                )),
                Replay::Unprovable { retained_from } => Err(CowshedError::stdin_refusal(
                    StdinRefusal::ReplayUnprovable {
                        offset,
                        cursor,
                        retained_from,
                    },
                    format!(
                        "job stdin write at byte {offset} repeats input older than byte \
                         {retained_from}, the oldest kept to compare with; {cursor} bytes \
                         reached the job"
                    ),
                    "resume writing at the job's cursor",
                )),
            };
            let _ = reply.send(answer);
            return;
        }
        let Some(end) = end.filter(|_| offset == admitted) else {
            let _ = reply.send(Err(CowshedError::stdin_refusal(
                StdinRefusal::Discontinuous {
                    offset,
                    cursor,
                    admitted,
                },
                format!(
                    "job stdin write at byte {offset} does not continue the input, whose next \
                     byte is {admitted}; {cursor} bytes reached the job"
                ),
                "resume writing at the job's cursor",
            )));
            return;
        };
        if delivery.queued_bytes.saturating_add(bytes.len()) > MAX_PENDING_STDIN_BYTES {
            let _ = reply.send(Err(CowshedError::conflict(
                "job stdin backpressure budget is full",
                "wait for the pending stdin write to drain",
            )));
            return;
        }
        let handed = match job.process.as_mut() {
            // Behind a queued write the lane is not this write's to take: it would overtake.
            Some(process) if delivery.queued.is_empty() => process.try_write_stdin(bytes.clone()),
            Some(_) => Ok(false),
            // Waiting for its CPU grant: the write waits in order until the command spawns.
            None if matches!(job.cpu, CpuStanding::Queued(_)) => Ok(false),
            None => {
                let _ = reply.send(Err(CowshedError::conflict(
                    "job process is unavailable",
                    "inspect the terminal job status",
                )));
                return;
            }
        };
        match handed {
            Ok(true) => delivery.in_lane.push_back(bytes),
            Ok(false) => {
                delivery.queued_bytes += bytes.len();
                delivery.queued.push_back(bytes);
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        }
        delivery.admitted_end = end;
        delivery.waiters.push((end, reply));
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
        if let Some(broken) = &job.stdin_delivery.broken {
            let cursor = job.stdin_delivery.delivered;
            let _ = reply.send(Err(stdin_delivery_unknown(cursor, broken)));
            return;
        }
        if !job.stdin_delivery.in_lane.is_empty()
            || !job.stdin_delivery.queued.is_empty()
            || job.close_stdin_when_drained
        {
            job.close_stdin_when_drained = true;
            job.close_waiters.push(reply);
            return;
        }
        let Some(process) = job.process.as_mut() else {
            // Waiting for its CPU grant: the close follows the input once the command spawns.
            if matches!(job.cpu, CpuStanding::Queued(_)) {
                job.close_stdin_when_drained = true;
                job.close_waiters.push(reply);
                return;
            }
            let _ = reply.send(Err(CowshedError::conflict(
                "job process is unavailable",
                "inspect job status",
            )));
            return;
        };
        if process.close_stdin() {
            job.info.stdin.complete = true;
            let _ = reply.send(Ok(()));
        } else {
            // The lane still holds a write its pump has not taken; its `StdinReady` retries.
            job.close_stdin_when_drained = true;
            job.close_waiters.push(reply);
        }
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
        } else if let RetirementState::Refused(refusal) = &job.retirement {
            let _ = reply.send(Err(refusal.error().clone()));
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
        self.kill_answering(job_id, KillReason::Requested, reply);
    }

    /// End `job_id` for `reason`, answering `reply` once it concluded.
    fn kill_answering(
        &mut self,
        job_id: JobId,
        reason: KillReason,
        reply: oneshot::Sender<Result<()>>,
    ) {
        if let Err(error) = self.begin_kill(job_id, reason) {
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

    /// A lifecycle verb is about to take its census of the mount (02_workspaces.md, "Holders
    /// cowshed does not run"): the keeper starts no daemon until the shed is used again, and the
    /// start it has in flight ends through the job it is, answered once that job concluded with
    /// its processes reaped and its plugin workers ended. A start whose daemon the checkout's
    /// record names live serves, perhaps a host run, so it is left: the census exempts it by that
    /// record, and the census must not end what it measures.
    fn stop_nx_keeper(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        reply: oneshot::Sender<Result<()>>,
    ) {
        if let Err(error) = self.validate_authority(authority) {
            let _ = reply.send(Err(error));
            return;
        }
        let (read_write, _) = self
            .policy
            .child(crate::api::dto::RunSandboxMode::ReadWrite);
        let (Some(keeper), Some(project_root)) = (
            &mut self.nx_daemon,
            super::nx_daemon::kept_project(read_write).map(Path::to_path_buf),
        ) else {
            let _ = reply.send(Ok(()));
            return;
        };
        keeper.stop_starts(&project_root);
        let in_flight = keeper
            .starting
            .filter(|id| self.jobs.get(id).is_some_and(|job| !job.terminal()));
        match in_flight {
            Some(job_id)
                if super::nx_daemon::probe(&crate::capabilities::nx::daemon_record(
                    &project_root,
                )) != Probe::Live =>
            {
                self.kill_answering(job_id, KillReason::Retire, reply);
            }
            _ => {
                let _ = reply.send(Ok(()));
            }
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
            let sealed = match self.artifacts.sealed(job_id) {
                Ok(Some(sealed)) => sealed,
                Ok(None) => {
                    let _ = reply.send(Err(not_found_job(job_id)));
                    return;
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return;
                }
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
            Ok(None) => match at_end(
                follow,
                job.terminal(),
                matches!(job.retirement, RetirementState::Refused(_)),
            ) {
                AtEnd::Wait => job.log_waiters.push(PendingLog {
                    stream,
                    offset,
                    reply,
                }),
                AtEnd::Refused => {
                    if let RetirementState::Refused(refusal) = &job.retirement {
                        let _ = reply.send(Err(refusal.error().clone()));
                    }
                }
                AtEnd::Answer => {
                    let (_, len, eof) = job.stream(stream);
                    let _ = reply.send(Ok(LogChunk {
                        bytes: Bytes::new(),
                        next_offset: len,
                        eof: eof || job.terminal(),
                    }));
                }
            },
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
                let _ = reply.send(tail_journals(cursor, limits, lengths, |stream, range| {
                    read_live_range(job.stream(stream.into()).0, range)
                }));
                return;
            }
            // The sealed artifact is the authoritative copy of both streams.
            Ok(job) => (job.info.stdout.clone(), job.info.stderr.clone()),
            // A job an earlier supervisor of this incarnation sealed: its artifact answers.
            Err(_) => match self.artifacts.sealed(job_id) {
                Ok(Some(sealed)) => (sealed.stdout, sealed.stderr),
                Ok(None) => {
                    let _ = reply.send(Err(not_found_job(job_id)));
                    return;
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
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
                // A descendant may still hold stdout/stderr open. Drain at leader exit, not
                // after EOF; sealing still waits for both streams and membership proof.
                self.retire_membership(job_id, RetirementRun::Requested);
            }
            ProcessEvent::Retired { job_id, retirement } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                let RetirementState::Draining(run) = job.retirement else {
                    return;
                };
                let retirement = retirement.and_then(|receipt| {
                    if job.info.pid.is_none()
                        && (!receipt.outlived.is_empty() || receipt.outlived_incomplete)
                    {
                        Err(CowshedError::integrity(
                            "membership retirement ended processes without an admitted child birth",
                            "retain the job claim and inspect the native spawn handshake",
                        ))
                    } else {
                        Ok(receipt)
                    }
                });
                match retirement {
                    Ok(receipt) => {
                        if job.info.pid.is_some() {
                            job.info.containment = Some(receipt.containment);
                            job.info.outlived = receipt.outlived;
                            job.info.outlived_incomplete = receipt.outlived_incomplete;
                        }
                        job.retirement = RetirementState::Ended;
                    }
                    Err(error) => self.refuse_membership(job_id, error, run),
                }
            }
            ProcessEvent::WaitFailed { job_id, error } => {
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return;
                };
                if job.terminal() {
                    // The job's record is final; what failed is the release of its process,
                    // whose group is no longer provably the job's.
                    cowshed_os::eprintln!(
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
            ProcessEvent::Probation { job_id, outcome } => {
                self.probation_settled(job_id, outcome);
            }
            ProcessEvent::StdinBroken { job_id, error } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    let refusal = stdin_delivery_unknown(job.stdin_delivery.delivered, &error);
                    job.stdin_delivery.refuse_waiting(&refusal);
                    job.stdin_delivery.broken = Some(error);
                    job.close_stdin_when_drained = false;
                    for waiter in job.close_waiters.drain(..) {
                        let _ = waiter.send(Err(refusal.clone()));
                    }
                }
            }
            ProcessEvent::StdinPumpWrite {
                job_id,
                offset,
                bytes,
                reply,
            } => {
                let authority = self.authority.clone();
                self.stdin_write(&authority, job_id, offset, bytes, reply);
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
                let coverage = self.job_sampling.leaf_coverage;
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    own_process(job, process, coverage);
                    self.arm_sample_clock();
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
            ProcessEvent::Started {
                job_id,
                process,
                nice,
            } => {
                let ledger = self.group_ledger.is_some();
                let coverage = self.job_sampling.leaf_coverage;
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    // A warm host's command started: its shell has activated.
                    command_started(job_id, job);
                    job.info.nice = Some(nice);
                    let birth = process.birth.clone();
                    own_process(job, process, coverage);
                    adopt_birth(job, ledger, birth);
                    self.record_groups();
                    self.arm_sample_clock();
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
                job.failure = Some(unlaunched(&error));
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
                cowshed_os::eprintln!(
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
        let delivery = &mut job.stdin_delivery;
        delivery.deliver_front();
        job.info.stdin.bytes = delivery.delivered;
        while let Some(next) = delivery.queued.front() {
            let Some(process) = job.process.as_mut() else {
                break;
            };
            match process.try_write_stdin(next.clone()) {
                Ok(true) => {
                    let next = delivery
                        .queued
                        .pop_front()
                        .expect("the front write was just seen");
                    delivery.queued_bytes -= next.len();
                    delivery.in_lane.push_back(next);
                }
                Ok(false) => break,
                Err(error) => {
                    delivery.drop_queued(&error);
                    break;
                }
            }
        }
        if delivery.in_lane.is_empty() && delivery.queued.is_empty() && job.close_stdin_when_drained
        {
            let closed = job
                .process
                .as_mut()
                .is_none_or(|process| process.close_stdin());
            // Unclosed, the last write is still in the lane; its own `StdinReady` retries.
            if closed {
                job.info.stdin.complete = true;
                job.close_stdin_when_drained = false;
                for waiter in job.close_waiters.drain(..) {
                    let _ = waiter.send(Ok(()));
                }
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
        // Only an explicit cancellation or retirement retries a refused native operation.
        if matches!(job.retirement, RetirementState::Refused(_)) {
            job.retirement = RetirementState::Unstarted;
        }
        let initiate = job.kill_reason.is_none();
        if initiate || reason == KillReason::OutputLimit {
            job.kill_reason = Some(reason);
        }
        // A job still waiting for its CPU grant has nothing to signal: withdrawing its request
        // leaves the gateway's queue, and the job ends without a command.
        if matches!(job.cpu, CpuStanding::Queued(_)) {
            if let CpuStanding::Queued(pending) =
                std::mem::replace(&mut job.cpu, CpuStanding::Withdrawn)
            {
                pending.asking.abort();
            }
            job.stdout_eof = true;
            job.stderr_eof = true;
            end_job_stdin(job);
            return Ok(());
        }
        // A job its probation's end stopped acts on its kill only once continued.
        if let CpuStanding::Granted { grant } = &job.cpu
            && grant.probation_stopped()
            && let Some(process) = job.process.as_mut()
            && let Err(error) = process.signal_process_tree(ProcessSignal::Continue)
        {
            cowshed_os::eprintln!(
                "cowshed: job {} stopped on probation could not be continued for its kill: {}",
                job_id.get(),
                error.message
            );
        }
        // A command not yet born -- its warm host still activating outside the job's cgroup, or
        // its placement pending -- has not entered the cgroup, so an empty membership proves
        // nothing about it. Its JobControl signals the activation host, or replays the signal
        // once the command is placed; retirement waits for the birth, as on macOS.
        #[cfg(target_os = "linux")]
        if job.cgroup.is_some() && job.birth.is_some() {
            self.retire_membership(job_id, RetirementRun::Requested);
            return Ok(());
        }
        #[cfg(target_os = "macos")]
        if job.monitor.is_some() && job.birth.is_some() {
            // Retirement takes the membership snapshot before sending a signal, so a process
            // ended by cancellation cannot disappear from the job's outlived receipt.
            self.retire_membership(job_id, RetirementRun::Requested);
            return Ok(());
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

    /// Retire membership off the actor while retaining the actual parent's handle, unreaped
    /// leader and all resource claims. Resident jobs use this same boundary when their own
    /// foreground leader ends; another job's terminal never ends the resident.
    fn retire_membership(&mut self, job_id: JobId, run: RetirementRun) -> bool {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return false;
        };
        match &job.retirement {
            RetirementState::Ended => return true,
            RetirementState::Draining(_) | RetirementState::Refused(_) => return false,
            RetirementState::Unstarted => {}
        }
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &job.cgroup {
            let cgroup = Arc::clone(cgroup);
            let grace = self.term_grace;
            let events = self.events.clone();
            job.retirement = RetirementState::Draining(run);
            if job.info.pid.is_some() {
                job.info.state = JobState::Draining;
            }
            tokio::spawn(async move {
                let retirement = tokio::task::spawn_blocking(move || {
                    let members = cgroup
                        .retire(grace, cowshed_os::deadline::REAP)
                        .map_err(cgroup_error)?;
                    Ok(JobProcessRetirement {
                        containment: crate::api::process::JobContainment::Kernel,
                        outlived: members
                            .members
                            .iter()
                            .map(|member| crate::api::process::OutlivedProcess {
                                index: None,
                                pid: member.pid,
                                image: member.image.clone(),
                                signal: member.signal,
                            })
                            .collect(),
                        outlived_incomplete: members.metadata_gap,
                    })
                })
                .await
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()));
                let _ = events
                    .send(ProcessEvent::Retired { job_id, retirement })
                    .await;
            });
            return false;
        }
        #[cfg(target_os = "macos")]
        if let (Some(monitor), Some(birth)) = (&job.monitor, &job.birth) {
            let monitor = monitor.clone();
            let birth = birth.clone();
            let grace = self.term_grace;
            let events = self.events.clone();
            job.retirement = RetirementState::Draining(run);
            job.info.state = JobState::Draining;
            tokio::spawn(async move {
                let retirement = tokio::task::spawn_blocking(move || monitor.retire(birth, grace))
                    .await
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()));
                let _ = events
                    .send(ProcessEvent::Retired { job_id, retirement })
                    .await;
            });
            return false;
        }
        // A refused start owns no membership. Non-native SpawnSinks supply their own process
        // lifecycle; neither case invents a native containment receipt.
        job.retirement = RetirementState::Ended;
        true
    }

    /// A refused retirement keeps the job's claims and reaches every waiter as its typed error,
    /// a log follower included: nothing ends its stream on its own any more (11_shell.md). Its
    /// observer's running lives ending re-runs it ([`Self::rerun_refused`]), at once if none runs.
    fn refuse_membership(&mut self, job_id: JobId, error: CowshedError, run: RetirementRun) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        cowshed_os::eprintln!(
            "cowshed: job {} membership retirement refused; process and volume claims retained: {}",
            job_id.get(),
            error.message
        );
        for waiter in job.waiters.drain(..) {
            let _ = waiter.send(Err(error.clone()));
        }
        for waiter in job.kill_waiters.drain(..) {
            let _ = waiter.send(Err(error.clone()));
        }
        for waiter in job.log_waiters.drain(..) {
            let _ = waiter.reply.send(Err(error.clone()));
        }
        for waiter in self.retire_waiters.drain(..) {
            let _ = waiter.send(Err(error.clone()));
        }
        for waiter in self.quiesce_waiters.drain(..) {
            let _ = waiter.send(Err(error.clone()));
        }
        job.retirement = RetirementState::Refused(Refusal::new(error, run));
        // Its lives may have ended while the refusal was on its way here.
        self.rerun_refused();
    }

    /// Re-run every refused retirement whose job's observer now holds no running life
    /// ([`Refusal::reruns`]): the survivors' exits, not a clock, conclude the job.
    fn rerun_refused(&mut self) {
        let due: Vec<JobId> = self
            .jobs
            .iter()
            .filter(|(_, job)| {
                matches!(&job.retirement, RetirementState::Refused(refusal)
                    if refusal.reruns(observed_running(job)))
            })
            .map(|(id, _)| *id)
            .collect();
        for job_id in due {
            if let Some(job) = self.jobs.get_mut(&job_id) {
                job.retirement = RetirementState::Unstarted;
            }
            self.retire_membership(job_id, RetirementRun::EmptyMembership);
        }
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
                        || matches!(job.cpu, CpuStanding::Withdrawn)
                        || (job.exit.is_some() && job.stdout_eof && job.stderr_eof)))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for job_id in ready {
            self.finalize_job(job_id, None).await;
        }
    }

    async fn finalize_job(&mut self, job_id: JobId, forced_state: Option<JobState>) {
        if !self.retire_membership(job_id, RetirementRun::Requested) {
            return;
        }
        let (links, observation) = match self.jobs.get_mut(&job_id) {
            Some(job) if !job.terminal() => (
                std::mem::take(&mut job.developer_links),
                job.install_observation.take(),
            ),
            _ => return,
        };
        if !links.is_empty() || observation.is_some() {
            let root = self.workspace_root.clone();
            let finish = tokio::task::spawn_blocking(move || {
                finish_terminal_restore(&root, links, observation)
            })
            .await
            .unwrap_or_else(JobFinish::task_failed);
            for finding in finish.findings {
                self.process_output(
                    job_id,
                    StreamKind::Stderr,
                    Bytes::from(format!("cowshed: {finding}\n")),
                );
            }
            if let Some(failure) = finish.failure {
                self.process_output(
                    job_id,
                    StreamKind::Stderr,
                    Bytes::from(format!("cowshed: {}\n", failure.label())),
                );
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.terminal_cleanup_failure = Some(failure.into_error());
                }
            }
        }
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if job.terminal() {
            return;
        }
        let state = forced_state.unwrap_or_else(|| {
            if job.terminal_cleanup_failure.is_some() {
                JobState::Failed
            } else {
                match job.kill_reason {
                    Some(KillReason::OutputLimit) => JobState::OutputLimit,
                    Some(KillReason::Requested | KillReason::Retire) => JobState::Killed,
                    Some(
                        KillReason::SpawnFailure
                        | KillReason::StdinFailure
                        | KillReason::ArtifactFailure
                        | KillReason::WaitFailure
                        | KillReason::ScriptSyntax
                        | KillReason::CpuAdmission,
                    ) => JobState::Failed,
                    None => match job.exit.as_ref().expect("ready terminal job has exit") {
                        ExitStatus::Exited { .. } => JobState::Exited,
                        ExitStatus::Signaled { .. } => JobState::Signaled,
                    },
                }
            }
        });
        let duration_ms = job
            .started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        #[cfg(target_os = "macos")]
        if let Some(monitor) = &job.monitor {
            let monitor = monitor.clone();
            if let Err(error) = tokio::task::spawn_blocking(move || monitor.finish())
                .await
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()))
            {
                cowshed_os::eprintln!(
                    "cowshed: job {}'s terminal process observation failed: {error}",
                    job_id.get()
                );
            }
        }
        // The daemon forks each plugin worker detached, outside the job's group, so the job's end
        // ended none of them; its observation followed each from its birth (04_sandbox.md).
        #[cfg(target_os = "macos")]
        if job.kept_nx_daemon
            && let (Some(monitor), Some(birth)) = (&job.monitor, &job.birth)
        {
            let monitor = monitor.clone();
            let daemon = birth.pid().cast_signed();
            let ended = tokio::task::spawn_blocking(move || {
                let lives = monitor.running().map_err(|error| error.to_string())?;
                super::daemon_watch::Orphans::of(daemon, lives)
                    .map(|orphans| orphans.end(crate::build_volume::nx::DAEMON_EXIT_GRACE))
                    .map_err(|error| error.to_string())
            })
            .await
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()));
            match ended {
                Ok(said) => {
                    for said in said {
                        cowshed_os::eprintln!("cowshed: {said}");
                    }
                }
                Err(why) => cowshed_os::eprintln!(
                    "cowshed: the plugin workers of the Nx daemon {daemon} could not be ended: {why}"
                ),
            }
        }
        // The terminal sample, frozen into the job's record; none for a job that never owned a
        // process, or whose last observation failed -- a read says which.
        job.info.resources = job.sampling.freeze(|sampler| {
            observe(
                sampler,
                &job.volumes,
                job.stdout_tally,
                job.stderr_tally,
                Boundary::Supervisor,
                #[cfg(target_os = "macos")]
                job.monitor.as_ref(),
            )
        });
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &job.cgroup {
            // Final accounting precedes removing the hierarchy. A refused finish retains all
            // claims and remains nonterminal; only an explicit request retries retirement.
            if let Err(error) = cgroup.finish(cowshed_os::deadline::REAP) {
                self.refuse_membership(job_id, cgroup_error(error), RetirementRun::Requested);
                return;
            }
            job.cgroup = None;
        }
        // State the terminal sample while the admitted build volume is still held. A missing
        // sample stays absent: no fresh boundary read invents host or volume facts for it.
        if let Some(job_spans) = &self.job_spans {
            job_spans.record(&job.info, JobSpanEdge::end(state, job.exit.as_ref()));
        }
        // Only a failed job names why; a job killed meanwhile ended by the kill.
        let failure = match job.kill_reason {
            Some(KillReason::ScriptSyntax) => Some(JobFailure::ScriptSyntax),
            _ => job.failure,
        }
        .filter(|_| state == JobState::Failed);
        let ending = JobEnding {
            state,
            exit: job.exit.clone(),
            duration_ms: Some(duration_ms),
            resources: job.info.resources.clone(),
            failure,
            cpu_tokens: job.info.cpu_tokens,
            nice: job.info.nice,
            containment: job.info.containment.clone(),
            outlived: job.info.outlived.clone(),
            outlived_incomplete: job.info.outlived_incomplete,
        };
        let seal_span = cowshed_os::timing::span("seal", "record");
        let sealed = self.artifacts.seal(
            job_id,
            ending,
            job.stdout_copy.take(),
            job.stderr_copy.take(),
        );
        drop(seal_span);
        // The membership and direct child have ended whatever becomes of their record, so the
        // job concludes on every path below and every waiter is answered. The claim was held
        // through the native proof, not merely through a successful artifact write.
        let conclusion = match sealed {
            Ok(seal) => {
                let commitment = cowshed_os::timing::spanned(
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
        // Nor does anything deliver its stdin any more: a write the pump never reported whole
        // may or may not have reached it.
        let cursor = job.stdin_delivery.delivered;
        job.stdin_delivery.refuse_waiting(&stdin_delivery_unknown(
            cursor,
            "the job concluded before the write was reported whole",
        ));
        job.process = None;
        // Nor does it run on its build volume any more: a release may take it, and the
        // controller's release is awaited before the job is told ended, so a volume superseded
        // while the job ran is gone by then, or the job's end says why it is not. Nor does it
        // hold its CPU grant: dropping it returns every token to the gateway's ledger.
        let mut state = state;
        if let Some((volume, hold)) = job.build_hold.take() {
            drop(hold);
            if let Some(released) = &self.build_volume_released
                && let Err(error) = released.release(volume).await
            {
                state = JobState::Failed;
                job.terminal_cleanup_failure.get_or_insert(error);
            }
        }
        job.cpu = CpuStanding::Settled;
        job.info.state = state;
        job.info.duration_ms = Some(duration_ms);
        job.info.exit = job.exit.clone();
        job.info.failure = failure;
        job.info.stdin.complete = true;
        // The job has concluded, every awaited cleanup behind it: its end starts the shed's idle
        // interval (04_sandbox.md, the kept daemon) here, not where its conclusion began, which a
        // slow seal, publication or release would leave a stale interval behind. The keeper's own
        // start job is maintenance, not use.
        if let Some(keeper) = &mut self.nx_daemon
            && keeper.starting != Some(job_id)
        {
            keeper.job_ended();
        }
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
        // Likewise its process observation, once the ledger has recorded the job's end with
        // nothing awaited between its waiters' answers and that record.
        #[cfg(target_os = "macos")]
        self.release_observation(job_id).await;
    }

    /// Release a sealed job's process observation (07_api.md, "Process-tree observations"):
    /// its lives have all exited and its sealed record is what is read of it from now on. Kept,
    /// the writer -- its thread, kqueue, fold, journal and argv copies -- grew every supervisor
    /// with each job it ran.
    #[cfg(target_os = "macos")]
    async fn release_observation(&mut self, job_id: JobId) {
        let Some(monitor) = self
            .jobs
            .get_mut(&job_id)
            .and_then(|job| job.monitor.take())
        else {
            return;
        };
        // The last clone stops and joins the writer, so it drops off the actor. A subscriber
        // still attached holds its own clone and reads to the end first.
        tokio::task::spawn_blocking(move || drop(monitor))
            .await
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()));
    }

    /// Admit nothing more: running jobs run on, and the keeper's daemon start is stopped, since
    /// the keeper probes only while the supervisor runs. Only the way to a retirement passes
    /// here; nothing returns a supervisor to `Running`.
    fn close_admission(&mut self) {
        if self.lifecycle == ActorLifecycle::Running {
            self.lifecycle = ActorLifecycle::Quiescing;
        }
        if let Some(id) = self.nx_daemon.as_ref().and_then(|keeper| keeper.starting) {
            let _ = self.begin_kill(id, KillReason::Retire);
        }
    }

    /// Retire now: every running job is stopped, and `reply` is answered once none runs.
    fn retire_now(&mut self, nx_daemon: NxDaemonRetirement, reply: oneshot::Sender<Result<()>>) {
        self.lifecycle = ActorLifecycle::Retiring;
        if nx_daemon == NxDaemonRetirement::ControllerOwned {
            // Land already captured the only processes its controller may signal.
            // Relinquish record cleanup, not any admitted job's retirement.
            self.nx_daemon.take();
        }
        self.retire_waiters.push(reply);
        // The keeper is the foreground daemon itself, so it stops and concludes through the
        // same lifecycle as every other admitted job.
        let running = self
            .jobs
            .iter()
            .filter_map(|(id, job)| (!job.terminal()).then_some(*id))
            .collect::<Vec<_>>();
        for job_id in running {
            let _ = self.begin_kill(job_id, KillReason::Retire);
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

    fn has_user_jobs(&self) -> bool {
        let keeper_job = self.nx_daemon.as_ref().and_then(|keeper| keeper.starting);
        self.jobs
            .iter()
            .any(|(id, job)| Some(*id) != keeper_job && !job.terminal())
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

/// Starts delivering `stdin`'s source, then its one EOF. Open stdin has no source: its attachment
/// writes it and ends it, so nothing starts.
fn launch_stdin_pump(
    job_id: JobId,
    stdin: StdinSource,
    workspace_root: PathBuf,
    events: mpsc::Sender<ProcessEvent>,
) {
    match stdin {
        StdinSource::Open => {}
        StdinSource::Empty => {
            tokio::spawn(async move { finish_stdin_pump(job_id, Ok(()), &events).await });
        }
        StdinSource::Inline(bytes) => {
            tokio::spawn(async move {
                let delivered = pump_one(job_id, 0, bytes, &events).await;
                finish_stdin_pump(job_id, delivered, &events).await;
            });
        }
        StdinSource::Stream(reader) => {
            tokio::spawn(async move {
                let delivered = pump_reader(job_id, reader, &events).await;
                finish_stdin_pump(job_id, delivered, &events).await;
            });
        }
        StdinSource::WorkspaceFile(path) => {
            tokio::spawn(async move {
                let delivered =
                    match tokio::fs::File::open(workspace_root.join(path.as_path())).await {
                        Ok(reader) => pump_reader(job_id, Box::pin(reader), &events).await,
                        Err(error) => Err(CowshedError::environment_missing(
                            format!("workspace stdin file could not be opened: {error}"),
                            "verify the workspace-relative stdin path",
                        )),
                    };
                finish_stdin_pump(job_id, delivered, &events).await;
            });
        }
    }
}

/// A delivered source ends the job's stdin; a failed one fails the job. A job whose stdin ended
/// or broke first -- its leader exited, or stopped reading and closed the pipe -- simply took no
/// more of the source: that is the job's business, not a failure of its input.
async fn finish_stdin_pump(
    job_id: JobId,
    delivered: Result<()>,
    events: &mpsc::Sender<ProcessEvent>,
) {
    let event = match delivered {
        Ok(()) => ProcessEvent::StdinPumpClose { job_id },
        Err(error)
            if matches!(
                error.stdin_source(),
                Some(StdinRefusal::Ended { .. } | StdinRefusal::DeliveryUnknown { .. })
            ) =>
        {
            return;
        }
        Err(error) => ProcessEvent::StdinPumpFailed { job_id, error },
    };
    let _ = events.send(event).await;
}

async fn pump_one(
    job_id: JobId,
    offset: u64,
    bytes: Bytes,
    events: &mpsc::Sender<ProcessEvent>,
) -> Result<()> {
    let (reply, receive) = oneshot::channel();
    events
        .send(ProcessEvent::StdinPumpWrite {
            job_id,
            offset,
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
    let mut offset = 0_u64;
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
        pump_one(
            job_id,
            offset,
            Bytes::copy_from_slice(&buffer[..count]),
            events,
        )
        .await?;
        offset += byte_count(count);
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
        StdinSource::Stream(_) | StdinSource::Open => StdinInfo {
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

/// A tail of a sealed job, read out of the artifact store.
fn sealed_tail(
    workspace_root: &Path,
    stdout: &StreamInfo,
    stderr: &StreamInfo,
    cursor: Option<JobJournalCursor>,
    limits: JobTailLimits,
) -> Result<JobTail> {
    tail_journals(
        cursor,
        limits,
        (stdout.bytes, stderr.bytes),
        |stream, range| {
            let info = match stream {
                crate::api::JobStream::Stdout => stdout,
                crate::api::JobStream::Stderr => stderr,
            };
            read_sealed_range(workspace_root, info, range)
        },
    )
}

/// The next tick of the job sampling clock; never, while it is stopped.
async fn next_sample_tick(clock: &mut Option<tokio::time::Interval>) {
    match clock {
        Some(clock) => {
            clock.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// The host as admission finds it, for the job span's start row: no resource sample exists yet,
/// and the sample's own `host_start` is read by the spawn that follows. A failed read leaves the
/// row's host columns null, said on stderr as a failed sample is.
fn admission_host(job_id: JobId) -> Option<HostLoadSample> {
    match crate::host_load::read_host_load() {
        Ok(host) => Some(host),
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: job {} start host observation failed: {error}",
                job_id.get()
            );
            None
        }
    }
}

/// The job owns `process`: its first process starts its sampling, judging its leaf by
/// `coverage`, and a later one -- its command, after its activation -- takes the lead. Either is
/// a supervisor boundary, observed at once as the job's latest sample. An ended job's sampling
/// stays as it ended, whatever arrives after.
fn own_process(job: &mut JobStateRecord, process: OwnedProcess, coverage: LeafCoverage) {
    if job.terminal() {
        return;
    }
    let job_id = job.info.job_id;
    if let Some(sampler) = job.sampling.own(job_id, process, coverage) {
        match observe(
            sampler,
            &job.volumes,
            job.stdout_tally,
            job.stderr_tally,
            Boundary::Supervisor,
            #[cfg(target_os = "macos")]
            job.monitor.as_ref(),
        ) {
            Ok(sample) => job.info.resources = Some(sample),
            // The latest sample stands, dated by its own `sampledAt`; a read reports the failure.
            Err(error) => cowshed_os::eprintln!(
                "cowshed: job {} was not sampled: {}",
                job_id.get(),
                error.message
            ),
        }
    }
    #[cfg(target_os = "macos")]
    clock_observation(job);
}

/// Start the job's process observation on the job's clock once both exist: its monitor, and the
/// spawn of its first owned process, the origin of `wallUs`. Either may come first; the origin
/// never changes, so saying it again changes nothing.
#[cfg(target_os = "macos")]
fn clock_observation(job: &JobStateRecord) {
    if let (Some(monitor), Some(origin)) = (&job.monitor, job.sampling.origin())
        && let Err(error) = monitor.clock(origin)
    {
        cowshed_os::eprintln!(
            "cowshed: starting job {}'s process windows on its clock: {error}",
            job.info.job_id.get()
        );
    }
}

/// Observe the job's processes and volumes now, with its streams as admitted so far, at
/// `boundary`: the one imperative step of a sample. Each member's resident memory is read right
/// after the membership; a member that exited in between holds nothing and is no member any
/// more. Each volume answers for itself: one that cannot be read is its own unavailability,
/// never the sample's failure.
fn observe(
    sampler: &mut JobSampler,
    volumes: &VolumeBaseline<HostVolume>,
    stdout: StreamTally,
    stderr: StreamTally,
    boundary: Boundary,
    #[cfg(target_os = "macos")] monitor: Option<&super::process_monitor::ProcessMonitor>,
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
    let (accounting, leaf) = account(
        sampler,
        &processes,
        #[cfg(target_os = "macos")]
        monitor,
    )?;
    let volumes = volumes.sample(&HostVolumeStat);
    sampler.sample(
        Observation {
            now: Instant::now(),
            sampled_at,
            members,
            host,
            volumes,
            stdout,
            stderr,
            accounting,
            leaf,
        },
        boundary,
    )
}

/// Read the independent job total after the process observation. Native jobs attribute from one
/// immutable publication of their retained event fold, without resampling or moving its window.
/// An explicitly supplied spawn sink without that source uses the transitional census in 07_api.md.
#[cfg(target_os = "macos")]
fn account(
    sampler: &mut JobSampler,
    members: &[super::job_groups::Process],
    monitor: Option<&super::process_monitor::ProcessMonitor>,
) -> Result<(JobAccounting, JobLeafAttribution)> {
    let leader = sampler.leader().pid();
    if monitor.is_none() {
        super::job_attribution::observe(sampler.census(), members, Instant::now()).map_err(
            |error| {
                CowshedError::environment_missing(
                    format!("the processes of job group {leader} could not be read: {error}"),
                    "the job runs on; read its resources again",
                )
            },
        )?;
    }
    let cpu = sampler
        .rusage()
        .account(super::job_accounting::read_leader)
        .map_err(|error| unaccounted(leader, error))?;
    let coverage = sampler.coverage();
    let attribution = match monitor {
        Some(monitor) => monitor.attribution(cpu, coverage)?,
        None => sampler.census().attribute(cpu, coverage).map_err(|error| {
            CowshedError::internal(format!(
                "the CPU of job group {leader} cannot be attributed: {error}"
            ))
        })?,
    };
    Ok((
        JobAccounting::MacOsRusageChildren {
            cpu,
            io: None,
            reconciliation: UsageReconciliation {
                cpu_us: attribution.cpu_us,
                // The source has no storage bytes to compare.
                io_read_bytes: None,
                io_write_bytes: None,
                coverage: attribution.coverage,
            },
        },
        attribution.leaf,
    ))
}

/// This sampler does not yet accept the native cgroup capability as an accounting input.
/// Membership and retirement still use that capability; an unavailable resource sample must
/// not claim the contained job lacks a cgroup or invent zero totals.
#[cfg(not(target_os = "macos"))]
fn account(
    sampler: &mut JobSampler,
    _members: &[super::job_groups::Process],
) -> Result<(JobAccounting, JobLeafAttribution)> {
    Err(CowshedError::environment_missing(
        format!(
            "job group {} cannot be sampled: the resource sampler has no cgroup accounting input",
            sampler.leader().pid()
        ),
        "job containment and retirement remain active; cgroup accounting is unavailable to this sampler",
    ))
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
        AccountingError::OtherLeader { .. }
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
        observe(
            sampler,
            &job.volumes,
            job.stdout_tally,
            job.stderr_tally,
            Boundary::Read,
            #[cfg(target_os = "macos")]
            job.monitor.as_ref(),
        )
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

/// A progress read: an ended job's terminal sample, a running job observed now, or nothing yet
/// for one that owns no process. A read never answers a running job with a frozen sample.
fn job_progress(job: &mut JobStateRecord) -> Result<ProgressRead> {
    if job.terminal() {
        sample_job(job).map(ProgressRead::Ended)
    } else if job.sampling.owns() {
        sample_job(job).map(ProgressRead::Running)
    } else {
        Ok(ProgressRead::Unowned)
    }
}

/// Record the job's process as its parent observed it. A leader no one could identify keeps the
/// job's pid but never enters the ledger, which loses only the means to end its group should this
/// supervisor die; a supervisor that keeps a ledger says so where the daemon log shows it.
fn adopt_birth(job: &mut JobStateRecord, ledger: bool, birth: Birth) {
    if ledger && let Birth::Unobserved { pid, reason } = &birth {
        cowshed_os::eprintln!(
            "cowshed: job {} process group {pid} was not identified at its start: {reason}",
            job.info.job_id.get()
        );
    }
    job.info.pid = Some(birth.pid());
    job.birth = Some(birth);
}

/// Why a job whose command was never launched failed: its nice could not be placed
/// (`priority`), or anything else kept the command from starting (`unlaunched`: an absent
/// program, `#!` interpreter or `env` name; a held child the observer refused before its exec;
/// a host that could not start it). Whatever a host process owned meanwhile, the command never
/// ran, and the failure says so where neither a pid nor a sample can.
fn unlaunched(error: &CowshedError) -> JobFailure {
    if error.priority_source().is_some() {
        JobFailure::Priority
    } else {
        JobFailure::Unlaunched
    }
}

/// The job's command started, however its spawn reports it: a one-shot spawn's process at once, a
/// warm host's with [`ProcessEvent::Started`] after its shell activated. A resident's start stake
/// returns its token here, at the first of the two it sees (05_gateway.md "A resident's start
/// holds one token").
fn command_started(job_id: JobId, job: &mut JobStateRecord) {
    if let CpuStanding::Granted { grant } = &mut job.cpu
        && grant.started()
    {
        cowshed_os::timing::event("jobserver", || {
            format!("release activation job={}", job_id.get())
        });
    }
}

/// The job's leader ended: its stdin closes for good and everything waiting on it is answered.
/// The process stays held until the job concludes, so its group can still be signalled.
fn end_job_stdin(job: &mut JobStateRecord) {
    if let Some(process) = job.process.as_mut() {
        process.end_stdin();
    }
    // The lane's writes may still reach a descendant holding the pipe; the queue's never will.
    let cursor = job.stdin_delivery.delivered;
    job.stdin_delivery.drop_queued(&stdin_ended(cursor));
    for waiter in job.close_waiters.drain(..) {
        let _ = waiter.send(Ok(()));
    }
    job.info.stdin.complete = true;
}

/// The refusal of a write to a job's stdin that has ended.
fn stdin_ended(cursor: u64) -> CowshedError {
    CowshedError::stdin_refusal(
        StdinRefusal::Ended { cursor },
        format!("job stdin has ended; {cursor} bytes reached the job"),
        "inspect the job status",
    )
}

/// The refusal of every write to a job's stdin once delivery past `cursor` is unknown, saying
/// `why`.
fn stdin_delivery_unknown(cursor: u64, why: &str) -> CowshedError {
    CowshedError::stdin_refusal(
        StdinRefusal::DeliveryUnknown { cursor },
        format!(
            "job stdin delivery is unknown past byte {cursor}: {why}; the job's stdin takes no \
             more input"
        ),
        "inspect the job status; resending cannot be told apart from what may have arrived",
    )
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

/// How many lives the job's observer holds running now; `None` for a job with no observer.
fn observed_running(job: &JobStateRecord) -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        job.monitor
            .as_ref()
            .map(|monitor| monitor.running_lives().len())
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = job;
        None
    }
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
/// look up; a records lock held past its bound is a wait on the process the census names; anything else is damage to the store.
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
        ArtifactError::HistoryUnprovable { set_aside } => {
            CowshedError::admission_refusal(AdmissionRefusal::Unprovable { set_aside })
        }
        ArtifactError::LockExpired(expired) => {
            crate::storage::job_artifact::records_lock_held(&expired)
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

/// The daemon log's line for naming a checkout's `what` (its workspace volume or a build volume)
/// after `image`. Every outcome is said, a typed skip included, except a name already right.
fn label_report(
    name: &WorkspaceName,
    what: &str,
    image: &Path,
    label: &str,
    outcome: &io::Result<Labelled>,
    elapsed: Duration,
) -> Option<String> {
    match outcome {
        Ok(Labelled::Renamed) => Some(format!(
            "cowshed: named workspace {name}'s {what} {label:?} in {elapsed:?}"
        )),
        Ok(Labelled::Already) => None,
        Ok(skipped @ (Labelled::NotAttached | Labelled::NotMounted)) => Some(format!(
            "cowshed: named no {what} for workspace {name}: its image {} is {skipped:?}",
            image.display()
        )),
        Err(error) => Some(format!(
            "cowshed: could not name workspace {name}'s {what} {} {label:?} after {elapsed:?}: {error}",
            image.display()
        )),
    }
}

#[cfg(test)]
mod workspace_toolchain_tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use crate::sandbox::{RunSandboxMode, SandboxConfig, SandboxGrants};
    #[cfg(target_os = "macos")]
    use cowshed_os::fork_lock::Run as _;

    // Only macOS tests build a sandbox; on the Linux cross lint the helper would be dead code.
    // The mount root is a sibling of the fixture's home and stores, as it is on a host: its deny
    // covers other workspaces, never the host home a capability grants into. These jobs run the
    // host's own tools (a linked bash, a detector's program), so the sandbox names the real
    // host's program directories, as production does (08_testing.md, "Fixtures").
    #[cfg(target_os = "macos")]
    fn sandbox_at(mount: &Path) -> SandboxConfig {
        let home = mount.parent().expect("root").join("home");
        SandboxConfig {
            program_directories: crate::capabilities::host_program_directories(&home),
            home,
            mount_root: mount.parent().expect("root").join("mounts"),
            workspace_mount: mount.to_path_buf(),
            exec_temp_dir: mount.parent().expect("root").join("tmp"),
            store_root: mount.parent().expect("root").join("store"),
            port_block: crate::metadata::PortBlock::new(40_960, 16).expect("port block"),
            retained_port_blocks: Vec::new(),
            mode: RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            cpu_pool_directory: None,
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            capabilities: Default::default(),
        }
    }

    fn scratch(test: &str) -> crate::temp_root::TempRoot {
        crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            &format!("cowshed-toolchain-{test}"),
        )
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
        env.insert(
            "NODE_OPTIONS".to_owned(),
            "--max-old-space-size=4096".to_owned(),
        );
        env.insert("NODE_USE_ENV_PROXY".to_owned(), "0".to_owned());
        env.insert("HOME".to_owned(), "/caller/home".to_owned());
        // A host shell that sourced an older `.cowshed/env` still carries the bare token.
        env.insert(
            "COWSHED_WORKSPACE_TOKEN".to_owned(),
            "caller-stale-token".to_owned(),
        );

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
        // Node and Bun load the port-0 preload published beside the bundle, after the caller's own
        // options and joined by the space each runtime splits its options on.
        let preload = mount.join(".cowshed/port-zero.cjs");
        assert_eq!(
            vars.get("NODE_OPTIONS").map(String::as_str),
            Some(format!("--max-old-space-size=4096 --require={}", preload.display()).as_str())
        );
        assert_eq!(
            vars.get("BUN_OPTIONS").map(String::as_str),
            Some(format!("--preload={}", preload.display()).as_str())
        );
        assert_eq!(
            std::fs::read(&preload).expect("preload"),
            crate::port_zero::PRELOAD
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
        // No other name carries it: the bare `COWSHED_WORKSPACE_TOKEN` is neither exported nor
        // forwarded from the caller, so a fork's inherited environment never holds it.
        assert!(!vars.contains_key("COWSHED_WORKSPACE_TOKEN"));
        let carriers: Vec<&str> = vars
            .iter()
            .filter(|(_, value)| value.contains(token.as_str()))
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(
            carriers,
            ["HTTPS_PROXY", "HTTP_PROXY", "http_proxy", "https_proxy"]
        );
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
        assert_eq!(
            vars.get("CARGO_UNSTABLE_MTIME_ON_USE").map(String::as_str),
            Some("true")
        );
        assert!(
            !private.join("home/.netrc").exists(),
            "the netrc a previous wiring left, token and all, is gone"
        );
    }

    /// A plain repository gets the core contract, including the two exact port-0 loader options,
    /// but no capability wiring. Its trust bundle uses only the core Git and OpenSSL anchors.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_plain_repository_gets_port_loaders_but_no_tool_wiring() {
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
        let mut child = environment.child(&BTreeMap::new());
        let preload = mount.join(".cowshed/port-zero.cjs");
        assert_eq!(
            child.remove(OsStr::new("NODE_OPTIONS")),
            Some(OsString::from(format!("--require={}", preload.display())))
        );
        assert_eq!(
            child.remove(OsStr::new("BUN_OPTIONS")),
            Some(OsString::from(format!("--preload={}", preload.display())))
        );
        // Core hygiene like TMPDIR, not capability wiring: any child can run Node or
        // Bun, so the compile cache is owned unconditionally and fenced from
        // capabilities. Every other NODE_* name below remains capability wiring.
        assert_eq!(
            child.remove(OsStr::new(
                crate::workspace_environment::NODE_COMPILE_CACHE_ENV
            )),
            Some(
                mount
                    .join(".cowshed/cache")
                    .join(crate::workspace_environment::NODE_COMPILE_CACHE_DIRECTORY)
                    .into_os_string()
            )
        );
        assert_eq!(
            std::fs::read(&preload).expect("port-0 preload"),
            crate::port_zero::PRELOAD
        );
        let names: Vec<String> = child
            .keys()
            .cloned()
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
        for core in ["GIT_SSL_CAINFO", "SSL_CERT_FILE"] {
            assert_eq!(child.get(OsStr::new(core)), Some(&bundle), "{core}");
        }
        std::fs::remove_file(sandbox_runtime_link(&sandbox)).ok();
    }

    /// A job in a workspace without shell activation meets the bootstrap PATH alone, whose fixed
    /// system directories hold no bash on NixOS. `#!/usr/bin/env bash` still finds the bash core
    /// detection linked into the mode's private `tools/bin`: run exactly as a one-shot job, a
    /// script with that line reports the link as its bash.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn an_env_bash_script_finds_the_linked_bash_without_shell_activation() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = scratch("env-bash");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::create_dir_all(root.join("home")).expect("home");
        std::fs::write(mount.join(".cowshed/token"), "A".repeat(43)).expect("token");
        std::fs::write(
            mount.join(".cowshed/ca.pem"),
            b"-----BEGIN CERTIFICATE-----\nWORKSPACE\n-----END CERTIFICATE-----\n",
        )
        .expect("workspace CA");
        let script = mount.join("which-bash");
        std::fs::write(
            &script,
            "#!/usr/bin/env bash\nset -euo pipefail\ncommand -v bash\n",
        )
        .expect("script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let sandbox = sandbox_at(&mount);

        let output = run_as_job(&sandbox, &mount, vec![OsString::from("./which-bash")])
            .await
            .expect("run the script as a job");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            String::from_utf8(output.stdout).expect("utf-8").trim_end(),
            mount
                .join(".cowshed/tools/bin/bash")
                .to_str()
                .expect("utf-8 path"),
        );
        std::fs::remove_file(sandbox_runtime_link(&sandbox)).ok();
    }

    /// `sandbox-exec` reports a script whose `#!` interpreter is absent as its own failure:
    /// "execvp() of './run' failed: No such file or directory", exit 71, for a script that exists
    /// and is executable. The production spawn refuses it first and names the interpreter.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_script_whose_interpreter_is_absent_is_refused_naming_the_interpreter() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = scratch("absent-interpreter");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::create_dir_all(root.join("home")).expect("home");
        std::fs::write(mount.join(".cowshed/token"), "A".repeat(43)).expect("token");
        std::fs::write(
            mount.join(".cowshed/ca.pem"),
            b"-----BEGIN CERTIFICATE-----\nWORKSPACE\n-----END CERTIFICATE-----\n",
        )
        .expect("workspace CA");
        let script = mount.join("run");
        std::fs::write(&script, "#!/nonexistent/cowshed-shell\nexit 0\n").expect("script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let sandbox = sandbox_at(&mount);
        let config = WorkspaceSupervisorConfig::default();
        let request = ProcessSpawnRequest {
            authority: config.authority,
            job_id: JobId::new(1).expect("static job id"),
            command: SpawnCommand::Argv(vec![OsString::from("./run")]),
            cwd: mount.clone(),
            env: BTreeMap::new(),
            policy: SandboxPolicy::render(sandbox.clone()).expect("render"),
            mode: crate::api::dto::RunSandboxMode::ReadWrite,
            scope: ExecScope::Workspace,
            cpu: CpuPlacement {
                held: None,
                pool: PathBuf::from("/pool/p.sock"),
                nice: 0,
                class: ExecClass::Background,
                activation: ActivationCover::Held,
            },
        };
        let (events, _received) = mpsc::channel(16);
        let spawned = SystemSpawnSink::default().spawn(request, events).await;
        let Err(refused) = spawned else {
            panic!(
                "the script was spawned; sandbox-exec then reports its interpreter as the script"
            );
        };
        assert_eq!(refused.code, crate::ErrorCode::NotFound);
        assert!(
            refused
                .message
                .contains("exists, but its interpreter /nonexistent/cowshed-shell"),
            "{}",
            refused.message
        );
        std::fs::remove_file(sandbox_runtime_link(&sandbox)).ok();
    }

    /// Every cargo a workspace runs wraps rustc with the sccache the host pinned: the program
    /// itself, not a name the repository shell's `PATH` may not resolve. A host that pinned none,
    /// or whose pinned store path was collected, builds without a wrapper rather than failing
    /// every cargo at its version probe. Neither the wrapper nor its cwd normalization is the
    /// caller's, in a one-shot child or a warm shell's command, and `CARGO_INCREMENTAL` stays
    /// cargo's to decide. Every cargo also records each unit use (`CARGO_UNSTABLE_MTIME_ON_USE`),
    /// whatever its caller names, and the host shell's `.cowshed/env` says the same.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn cargo_wraps_rustc_with_the_pinned_sccache_or_with_nothing() {
        let root = scratch("sccache-client");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::write(mount.join(".cowshed/token"), "A".repeat(43)).expect("token");
        std::fs::set_permissions(
            mount.join(".cowshed/token"),
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .expect("private token permissions");
        std::fs::write(mount.join("Cargo.toml"), "").expect("cargo project");
        let sandbox = sandbox_at(&mount);
        std::fs::create_dir_all(&sandbox.home).expect("home");
        let caller = BTreeMap::from([
            ("RUSTC_WRAPPER".to_owned(), "/bin/false".to_owned()),
            ("SCCACHE_BASEDIR_CWD".to_owned(), "0".to_owned()),
            ("SCCACHE_BASEDIR".to_owned(), "/elsewhere".to_owned()),
            (
                crate::capabilities::cargo::MTIME_ON_USE_ENV.to_owned(),
                "false".to_owned(),
            ),
        ]);
        // Detection reads the host's pinned client, so each case configures a fresh snapshot,
        // as a supervisor start does.
        let wiring = async || {
            let mut configured = sandbox.clone();
            configured
                .configure_capabilities()
                .expect("detect capabilities");
            crate::workspace_credentials::publish_workspace_environment(
                crate::workspace_environment::EnvironmentMount::Served {
                    workspace_mount: &mount,
                    temp_dir: &configured.exec_temp_dir,
                    host_home: &configured.home,
                    capability_env: &configured.capabilities.contribution,
                },
                crate::metadata::Platform::Macos,
                Some(configured.port_block),
            )
            .expect("publish host environment");
            let environment = sandbox_environment(&configured, &caller)
                .await
                .expect("environment");
            let child = environment.child(&caller);
            let mut host = std::process::Command::new("/bin/sh");
            host.env_clear()
                .args(["-c", ". \"$1\"; env", "host"])
                .arg(mount.join(".cowshed/env"));
            let output = cowshed_os::fork_lock::Run::output_locked(&mut host)
                .expect("source host environment");
            assert!(output.status.success());
            let text = String::from_utf8(output.stdout).unwrap();
            for name in crate::capabilities::sccache::HOST_ENVIRONMENT
                .into_iter()
                .chain(crate::capabilities::cargo::HOST_ENVIRONMENT)
            {
                let value = text
                    .lines()
                    .find_map(|line| line.strip_prefix(&format!("{name}=")));
                assert_eq!(
                    value.map(OsString::from),
                    child.get(OsStr::new(name)).cloned(),
                    "host and supervisor agree on {name}",
                );
            }
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
                crate::capabilities::cargo::MTIME_ON_USE_ENV,
            ]
            .map(|name| child.get(OsStr::new(name)).cloned())
        };
        // A cargo project on a host with no pinned client builds without a wrapper, and the
        // caller's wrapper and request bases never reach the child either.
        let unwrapped = [None, None, None, None, Some(OsString::from("true"))];
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
                Some(OsString::from("true")),
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
                &mount,
                &environment_root,
                None,
                &sandbox.capabilities.contribution,
            )
            .expect("prepare");
            let link = environment_root.join("tools/bin/npm");
            assert_eq!(std::fs::read_link(&link).expect("command link"), target);

            let path = bootstrap_path(&sandbox).expect("bootstrap PATH");
            let mut expected = vec![
                crate::host_dirs::installed_binary_directory(&sandbox.home),
                environment_root.join("tools/manager-bin"),
                environment_root.join("tools/bin"),
                mount.join(".cowshed/bin"),
            ];
            expected.extend(platform());
            assert_eq!(
                std::env::split_paths(&path).collect::<Vec<_>>(),
                expected,
                "{mode:?}: the installed launcher leads, then the wrapper route, private tools, workspace bin and platform"
            );

            prepare_private_environment(
                &mount,
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
        for mode in [read_write, read_only] {
            assert_eq!(
                state(mode, "NX_DAEMON_START"),
                Some(crate::capabilities::EnvAction::Own("never".into()))
            );
        }
    }

    /// The real host-side environment builder creates its own global short runtime alias. The
    /// caller's endpoint and read-only flag never reach the child; a non-Nx checkout withholds both.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    async fn host_controller_caller_supplied_nx_endpoint_and_read_only_flag_never_reach_the_child()
    {
        struct RuntimeAlias {
            path: PathBuf,
            target: PathBuf,
        }
        impl Drop for RuntimeAlias {
            fn drop(&mut self) {
                let removed = match std::fs::read_link(&self.path) {
                    Ok(target) if target == self.target => std::fs::remove_file(&self.path),
                    Ok(target) => Err(std::io::Error::other(format!(
                        "runtime alias {} now belongs to {}",
                        self.path.display(),
                        target.display()
                    ))),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(error),
                };
                if let Err(error) = removed {
                    if std::thread::panicking() {
                        cowshed_os::eprintln!(
                            "remove owned runtime alias {}: {error}",
                            self.path.display()
                        );
                    } else {
                        panic!(
                            "remove owned runtime alias {}: {error}",
                            self.path.display()
                        );
                    }
                }
            }
        }
        let parent = std::env::var_os(crate::workspace_environment::RUNTIME_LINK_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = crate::temp_root::TempRoot::new_in(&parent, "cowshed-toolchain-nx-endpoint");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("metadata directory");
        // Capability containment takes the controller's canonical workspace mount, not its alias.
        let mount = std::fs::canonicalize(&mount).expect("canonical fixture workspace mount");
        let token = cowshed_gateway_types::WorkspaceToken::from_bytes([7; 32]);
        std::fs::write(
            mount.join(crate::workspace_credentials::WORKSPACE_TOKEN_PATH),
            token.encode(),
        )
        .expect("workspace token");
        let caller = BTreeMap::from([
            (
                crate::workspace_environment::NX_REMOTE_SERVER_ENV.to_owned(),
                "http://attacker.example:9/nx".to_owned(),
            ),
            (
                crate::workspace_environment::NX_REMOTE_READ_ONLY_ENV.to_owned(),
                "false".to_owned(),
            ),
        ]);
        let sandbox = sandbox_at(&mount);
        let runtime_link = sandbox_runtime_link(&sandbox);
        assert!(
            matches!(std::fs::symlink_metadata(&runtime_link), Err(error) if error.kind() == std::io::ErrorKind::NotFound),
            "fixture runtime alias {} must not already exist",
            runtime_link.display()
        );
        let alias = RuntimeAlias {
            path: runtime_link.clone(),
            target: sandbox_runtime_dir(&sandbox),
        };
        std::fs::create_dir_all(&sandbox.home).expect("home");

        // Without an Nx project, the caller's endpoint and read-only flag are withheld.
        let mut plain = sandbox.clone();
        plain.configure_capabilities().expect("detect capabilities");
        let without_nx = sandbox_environment(&plain, &caller)
            .await
            .expect("environment")
            .child(&caller);
        assert!(!without_nx.contains_key(OsStr::new(
            crate::workspace_environment::NX_REMOTE_SERVER_ENV
        )));
        assert!(!without_nx.contains_key(OsStr::new(
            crate::workspace_environment::NX_REMOTE_READ_ONLY_ENV
        )));

        // With one, the endpoint is the workspace's own and the read-only flag is set.
        std::fs::write(mount.join("nx.json"), "{}").expect("nx project");
        let mut configured = sandbox.clone();
        configured
            .configure_capabilities()
            .expect("detect capabilities");
        let child = sandbox_environment(&configured, &caller)
            .await
            .expect("environment")
            .child(&caller);
        assert_eq!(
            child.get(OsStr::new(
                crate::workspace_environment::NX_REMOTE_SERVER_ENV
            )),
            Some(&std::ffi::OsString::from("http://127.0.0.1:40960/nx"))
        );
        assert_eq!(
            child.get(OsStr::new(
                crate::workspace_environment::NX_REMOTE_READ_ONLY_ENV
            )),
            Some(&std::ffi::OsString::from("true"))
        );
        assert_eq!(
            std::fs::read_link(&runtime_link).expect("owned runtime alias"),
            alias.target
        );
        drop(alias);
        assert!(
            matches!(std::fs::symlink_metadata(&runtime_link), Err(error) if error.kind() == std::io::ErrorKind::NotFound),
            "fixture must remove its runtime alias {}",
            runtime_link.display()
        );
    }

    /// Both JS runtimes compile a sandbox child's modules into the workspace's private XDG
    /// cache — the image's .cowshed/cache read-write, the exec-temp carve-back read-only —
    /// through the owned NODE_COMPILE_CACHE the real environment builder hands it. The
    /// caller's cache is never inherited, no loose node-compile-cache appears under TMPDIR
    /// (the residue that refused a retired workspace's restore), and every compile-cache
    /// file a read-only child leaves in its temp dir is inside the owned cache subtree the
    /// temp dir's own retirement reclaims (storage::directory::tests::
    /// a_retired_workspaces_read_only_compile_cache_is_reclaimed_with_its_temp_dir).
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn sandbox_children_compile_into_the_private_cache_not_the_temp_dir() {
        fn files_under(root: &Path) -> Vec<PathBuf> {
            let mut files = Vec::new();
            let mut pending = vec![root.to_path_buf()];
            while let Some(directory) = pending.pop() {
                let entries = match std::fs::read_dir(&directory) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => panic!("read {}: {error}", directory.display()),
                };
                for entry in entries {
                    let entry = entry.unwrap_or_else(|error| panic!("read entry: {error}"));
                    if entry.file_type().expect("entry type").is_dir() {
                        pending.push(entry.path());
                    } else {
                        files.push(entry.path());
                    }
                }
            }
            files.sort();
            files
        }

        fn host_program(name: &str) -> PathBuf {
            let path = std::env::var_os("PATH").expect("the gate PATH names the host toolchain");
            std::env::split_paths(&path)
                .map(|directory| directory.join(name))
                .find(|candidate| candidate.is_file())
                .unwrap_or_else(|| panic!("host {name} is required for this proof"))
        }

        // The producer's actual call, then a real module load whose bytecode the runtime
        // flushes into the resolved cache before it exits.
        const COMPILE: &str = r#"const m = require("node:module");
m.enableCompileCache();
const dir = m.getCompileCacheDir();
if (!dir) { console.error("compile cache is not enabled"); process.exit(3); }
console.log(dir);
import("./compile-cache-lib.mjs").then((lib) => {
  if (lib.answer() !== 42) { process.exit(4); }
  if (m.flushCompileCache) { m.flushCompileCache(); }
});
"#;

        let root = scratch("compile-cache");
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).expect("private root");
        std::fs::create_dir_all(root.join("home")).expect("home");
        let token = cowshed_gateway_types::WorkspaceToken::from_bytes([7; 32]);
        std::fs::write(
            mount.join(crate::workspace_credentials::WORKSPACE_TOKEN_PATH),
            token.encode(),
        )
        .expect("workspace token");
        std::fs::write(
            mount.join(crate::workspace_credentials::CA_CERTIFICATE_PATH),
            b"-----BEGIN CERTIFICATE-----\nWORKSPACE\n-----END CERTIFICATE-----\n",
        )
        .expect("workspace CA");
        std::fs::write(mount.join("package.json"), "{}\n").expect("project manifest");
        std::fs::write(mount.join("bun.lock"), "").expect("project convention");
        std::fs::write(
            mount.join("compile-cache-lib.mjs"),
            "export function answer() { return 42; }\n",
        )
        .expect("compiled module");
        let script = mount.join("compile.cjs");
        std::fs::write(&script, COMPILE).expect("compile script");
        let caller_cache = root.join("caller-cache");
        std::fs::create_dir_all(&caller_cache).expect("caller cache");
        let caller_sentinel = caller_cache.join("sentinel");
        std::fs::write(&caller_sentinel, b"the caller's own").expect("caller sentinel");
        let caller = BTreeMap::from([
            (
                crate::workspace_environment::NODE_COMPILE_CACHE_ENV.to_owned(),
                caller_cache
                    .to_str()
                    .expect("utf-8 caller cache")
                    .to_owned(),
            ),
            (
                "XDG_CACHE_HOME".to_owned(),
                caller_cache
                    .to_str()
                    .expect("utf-8 caller cache")
                    .to_owned(),
            ),
        ]);
        let runtimes = [host_program("node"), host_program("bun")];
        let sandbox = sandbox_at(&mount);
        for (mode, label) in [
            (RunSandboxMode::ReadWrite, "read-write"),
            (RunSandboxMode::ReadOnly, "read-only"),
        ] {
            let mut configured = sandbox.clone();
            configured.mode = mode;
            configured
                .configure_capabilities()
                .expect("detect capabilities");
            let environment = sandbox_environment(&configured, &caller)
                .await
                .expect("environment");
            let child = environment.child(&caller);
            let value = |name: &str| -> PathBuf {
                child
                    .get(OsStr::new(name))
                    .unwrap_or_else(|| panic!("{label}: child has no {name}"))
                    .clone()
                    .into()
            };
            let owned = value(crate::workspace_environment::NODE_COMPILE_CACHE_ENV);
            assert_eq!(
                owned,
                value("XDG_CACHE_HOME")
                    .join(crate::workspace_environment::NODE_COMPILE_CACHE_DIRECTORY),
                "{label}: the compile cache is the private XDG cache's own directory"
            );
            // A foreign file sharing the owned cache's parent is not the child's to move.
            std::fs::create_dir_all(value("XDG_CACHE_HOME")).expect("private cache");
            let keep = value("XDG_CACHE_HOME").join("keep");
            std::fs::write(&keep, b"not compile cache").expect("foreign cache neighbour");
            for program in &runtimes {
                let mut command = std::process::Command::new(program);
                command.arg(&script).current_dir(&mount).env_clear();
                for (name, entry) in &child {
                    command.env(name, entry);
                }
                let output = command.output_locked().expect("run child runtime");
                assert!(
                    output.status.success(),
                    "{label} {}: {output:?}",
                    program.display()
                );
                let stdout = String::from_utf8(output.stdout).expect("runtime output");
                let reported = PathBuf::from(stdout.lines().next().expect("cache dir"));
                assert!(
                    reported.starts_with(&owned),
                    "{label}: compiled into {}, not {}",
                    reported.display(),
                    owned.display()
                );
                assert!(
                    files_under(&reported)
                        .iter()
                        .any(|file| std::fs::metadata(file).expect("cache file").len() > 0),
                    "{label}: no bytecode under {}",
                    reported.display()
                );
            }
            // The whole private-environment inventory the temp dir's retirement must
            // outlive or remove: the trust bundle and port-0 preload at the environment
            // root, bootstrap tools and the XDG subtrees. In read-only mode all of it is
            // inside the temp dir, so a cleanup that removed only the compile cache
            // would still strand the retired workspace.
            let environment_root = match configured.mode {
                RunSandboxMode::ReadWrite => mount.join(".cowshed"),
                RunSandboxMode::ReadOnly => configured.exec_temp_dir.clone(),
            };
            for published in [
                crate::workspace_clients::TRUST_BUNDLE_NAME
                    .to_str()
                    .expect("the bundle name is ASCII"),
                crate::port_zero::PRELOAD_NAME
                    .to_str()
                    .expect("the preload name is ASCII"),
            ] {
                assert!(
                    environment_root.join(published).is_file(),
                    "{label}: no published {published} under {}",
                    environment_root.display()
                );
            }
            for subtree in ["home", "config", "cache", "data", "state", "run", "tools"] {
                assert!(
                    environment_root.join(subtree).is_dir(),
                    "{label}: no private {subtree} under {}",
                    environment_root.display()
                );
            }
            assert!(
                !configured
                    .exec_temp_dir
                    .join(crate::workspace_environment::NODE_COMPILE_CACHE_DIRECTORY)
                    .exists(),
                "{label}: a loose compile cache under TMPDIR"
            );
            // Every compile-cache byte in the temp dir is inside the owned cache subtree
            // (read-write leaves none there at all).
            for file in files_under(&configured.exec_temp_dir) {
                if file.components().any(|component| {
                    component.as_os_str()
                        == OsStr::new(crate::workspace_environment::NODE_COMPILE_CACHE_DIRECTORY)
                }) {
                    assert!(
                        file.starts_with(&owned),
                        "{label}: {} escaped {}",
                        file.display(),
                        owned.display()
                    );
                }
            }
            assert_eq!(std::fs::read(&keep).expect("kept"), b"not compile cache");
            assert_eq!(
                files_under(&caller_cache),
                std::slice::from_ref(&caller_sentinel)
            );
            assert_eq!(
                std::fs::read(&caller_sentinel).expect("kept"),
                b"the caller's own"
            );
        }
        std::fs::remove_file(sandbox_runtime_link(&sandbox)).ok();
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
        assert_eq!(
            sealed, 2,
            "each record awaited alone is a group, and a segment, of one"
        );
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
                JobScheduling::default(),
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
                JobScheduling::default(),
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
            membership_socket: None,
            telemetry_root: None,
            inherited_groups: Vec::new(),
            volume_labels: None,
            workspace_volume: None,
            job_sampling: JobSamplingPolicy::default(),
            cpu: defaults.cpu,
            build_volume_released: None,
            nx_daemon_idle: defaults.nx_daemon_idle,
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
                JobScheduling::default(),
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

    /// An existing shed's `.cowshed/env` from an older build exported the Nx cache bearer and no
    /// read-only flag. The first supervisor start of this build republishes it, so a host shell
    /// that re-sources it reads its project's Nx cache and never stores to it (05_gateway.md
    /// "`/nx/v1/cache/{hash}`").
    #[tokio::test]
    async fn a_supervisor_start_rewrites_an_older_env_with_the_nx_read_only_flag() {
        let root =
            std::env::temp_dir().join(format!("cowshed-env-republish-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let workspace_root = root.join("workspace");
        std::fs::create_dir_all(workspace_root.join(".cowshed")).unwrap();
        std::fs::write(workspace_root.join("nx.json"), "{}").unwrap();
        let env = workspace_root.join(".cowshed/env");
        std::fs::write(
            &env,
            "export NX_SELF_HOSTED_REMOTE_CACHE_SERVER=http://127.0.0.1:7644/nx\nexport NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN=older-bearer\n",
        )
        .unwrap();
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("5198f2c0b7e34dc795f17b238b331c80").unwrap();
        let mut publisher = CommitmentPublisher::open(
            root.join("telemetry"),
            crate::storage::audit::ContinuityAudit::Arrow,
            8,
        )
        .unwrap();
        publisher
            .record(CommitmentDraft::WorkspaceIntroduced {
                repo_id: repo_id.clone(),
                workspace_incarnation: incarnation.clone(),
            })
            .await
            .unwrap();
        let defaults = WorkspaceSupervisorConfig::default();
        let config = WorkspaceSupervisorConfig {
            authority: WorkspaceAuthoritySnapshot {
                repo_id: repo_id.clone(),
                workspace: WorkspaceName::new("raven").unwrap(),
                workspace_incarnation: incarnation,
                grant_revision: 1,
                lifecycle_revision: 1,
            },
            owned_repo_ids: OwnedRepoIds::sole(repo_id),
            workspace_root: workspace_root.clone(),
            sandbox: SandboxConfig {
                workspace_mount: workspace_root.clone(),
                exec_temp_dir: root.join("tmp"),
                ..defaults.sandbox.clone()
            },
            ..defaults
        };
        let supervisor = WorkspaceSupervisor::start(config, publisher.clone()).unwrap();
        let published = std::fs::read_to_string(&env).unwrap();
        assert!(
            published.contains("export NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY=true\n"),
            "{published}"
        );
        assert!(
            published.contains("export NX_SELF_HOSTED_REMOTE_CACHE_SERVER=http://127.0.0.1:"),
            "{published}"
        );
        assert!(
            !published.contains("NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN"),
            "{published}"
        );
        assert!(!published.contains("older-bearer"), "{published}");
        drop(supervisor);
        drop(publisher);
        tokio::task::yield_now().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod sandbox_environment_tests {
    use super::*;
    use cowshed_os::fork_lock::Run as _;

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
        prepare_private_environment(&root, &environment, None, &contribution).unwrap();
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
        let error = prepare_private_environment(&root, &environment, None, &escaping)
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
            &root.join("checkout"),
            &checkout_environment,
            Some((alias, &runtime)),
            &contribution,
        )
        .unwrap();
        std::fs::write(runtime.join("nx/preserved"), "shared").unwrap();
        prepare_private_environment(
            &root.join("checkout"),
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
            prepare_private_environment(
                &root.join("checkout"),
                &read_only_environment,
                Some((alias, &runtime)),
                &escaping
            )
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
                &root.join("checkout"),
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
            &root,
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

    /// A host gate runs at the host boundary, outside every sandbox profile, so its command's
    /// environment is the only guard against an Nx daemon there. Whatever a caller names, the
    /// command is given `never`: a daemon the client started would be shared through the
    /// rendezvous record and end with the job (04_sandbox.md).
    #[test]
    fn a_host_gate_command_is_given_nx_daemon_start_never_whatever_the_caller_names() {
        let root = scratch("host-gate-environment");
        let mount = root.join("workspace");
        std::fs::create_dir_all(&mount).expect("workspace");
        let config = WorkspaceSupervisorConfig::default();
        let sandbox = SandboxConfig {
            workspace_mount: mount.clone(),
            ..config.sandbox
        };
        let request = ProcessSpawnRequest {
            authority: config.authority,
            job_id: JobId::new(1).expect("static job id"),
            command: SpawnCommand::Argv(vec![OsString::from("true")]),
            cwd: mount.clone(),
            env: BTreeMap::from([("NX_DAEMON_START".to_owned(), "always".to_owned())]),
            policy: SandboxPolicy::render(sandbox).expect("render"),
            mode: crate::api::dto::RunSandboxMode::ReadWrite,
            scope: ExecScope::HostGate,
            cpu: CpuPlacement {
                held: None,
                pool: PathBuf::from("/pool/p.sock"),
                nice: 0,
                class: ExecClass::Gate,
                activation: ActivationCover::Held,
            },
            #[cfg(target_os = "linux")]
            cgroup: None,
        };
        let command = host_gate_command(&request, &mount).expect("host gate command");
        let environment: BTreeMap<&OsStr, Option<&OsStr>> = command.as_std().get_envs().collect();
        assert_eq!(
            environment.get(OsStr::new("NX_DAEMON_START")),
            Some(&Some(OsStr::new("never")))
        );
        std::fs::remove_dir_all(root).expect("cleanup");
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
}

#[cfg(test)]
mod tail_tests {
    use super::*;
    #[test]
    fn retained_chunks_short_of_the_admitted_range_are_an_error_not_a_short_slice() {
        let chunks = VecDeque::from([Bytes::from_static(b"abc"), Bytes::from_static(b"de")]);
        assert_eq!(read_live_range(&chunks, 1..4).unwrap(), b"bcd");
        let error = read_live_range(&chunks, 3..8).err().unwrap();
        assert_eq!(error.code, crate::error::ErrorCode::Internal);
    }
}

#[cfg(test)]
mod stdin_lane_tests {
    use super::*;

    #[test]
    fn delivered_and_refused_waiters_keep_their_storage() {
        let mut delivery = StdinDelivery {
            admitted_end: 3,
            in_lane: VecDeque::from([Bytes::from_static(b"one")]),
            waiters: Vec::with_capacity(4),
            ..StdinDelivery::default()
        };
        let (due, mut due_reply) = oneshot::channel();
        let (queued, mut queued_reply) = oneshot::channel();
        let (repeated, mut repeated_reply) = oneshot::channel();
        delivery
            .waiters
            .extend([(3, due), (6, queued), (3, repeated)]);
        let storage = delivery.waiters.as_ptr();
        delivery.deliver_front();
        assert_eq!((delivery.delivered, delivery.admitted()), (3, 3));
        assert_eq!(delivery.waiters.as_ptr(), storage);
        assert!(due_reply.try_recv().unwrap().is_ok());
        assert!(repeated_reply.try_recv().unwrap().is_ok());
        assert!(matches!(
            queued_reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        delivery.in_lane.push_back(Bytes::from_static(b"x"));
        delivery.queued.push_back(Bytes::from_static(b"yz"));
        delivery.queued_bytes = 2;
        delivery.admitted_end = 6;
        let (reachable, mut reachable_reply) = oneshot::channel();
        delivery.waiters.push((4, reachable));
        let refusal = CowshedError::conflict("input ended", "inspect the job");
        delivery.drop_queued(&refusal);
        assert_eq!((delivery.delivered, delivery.admitted()), (3, 4));
        assert_eq!(delivery.waiters.as_ptr(), storage);
        let rejected = queued_reply.try_recv().unwrap().unwrap_err();
        assert_eq!(rejected.code, refusal.code);
        assert_eq!(rejected.message, refusal.message);
        assert_eq!(rejected.hint, refusal.hint);
        assert!(matches!(
            reachable_reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        delivery.deliver_front();
        assert_eq!((delivery.delivered, delivery.admitted()), (4, 4));
        assert_eq!(delivery.waiters.as_ptr(), storage);
        assert!(reachable_reply.try_recv().unwrap().is_ok());
        assert!(delivery.waiters.is_empty());

        delivery.in_lane.push_back(Bytes::from_static(b"lost"));
        delivery.queued.push_back(Bytes::from_static(b"later"));
        delivery.queued_bytes = 5;
        delivery.admitted_end = 13;
        delivery.refuse_waiting(&refusal);
        assert_eq!((delivery.delivered, delivery.admitted()), (4, 4));
        assert!(delivery.in_lane.is_empty());
        assert!(delivery.queued.is_empty());
        assert_eq!(delivery.queued_bytes, 0);
    }

    /// A close that arrives while the lane's one slot still holds a write its pump has not taken
    /// is the ordinary end of a write-then-close: it waits for the lane instead of refusing, and
    /// the pipe then carries every byte and one EOF.
    #[tokio::test]
    async fn a_close_behind_an_untaken_write_waits_for_the_lane_instead_of_refusing() {
        use std::io::Read as _;
        let (sender, receiver) = mpsc::channel(1);
        let mut lane = StdinLane::new(sender);
        assert!(lane.try_write(Bytes::from_static(b"line\n")).unwrap());
        assert!(
            !lane.close(),
            "the slot still holds the write, so no EOF was queued behind it"
        );

        let (mut read_end, write_end) = std::io::pipe().unwrap();
        let pipe =
            tokio::net::unix::pipe::Sender::from_owned_fd(std::os::fd::OwnedFd::from(write_end))
                .unwrap();
        let job_id = JobId::new(1).unwrap();
        let (events, mut observed) = mpsc::channel(4);
        let pump = tokio::spawn(run_system_stdin(job_id, pipe, receiver, events));
        assert!(matches!(
            observed.recv().await,
            Some(ProcessEvent::StdinReady { job_id: ready }) if ready == job_id
        ));
        assert!(
            lane.close(),
            "the pump took the write, so the EOF is queued"
        );
        assert!(
            lane.close(),
            "a repeated close is answered and sends nothing"
        );
        pump.await.unwrap();
        let mut received = Vec::new();
        read_end.read_to_end(&mut received).unwrap();
        assert_eq!(received, b"line\n");
    }

    /// A child that closed its stdin makes the pump's write fail: the pump reports the input
    /// broken instead of a write taken, and keeps the lane open and unread, so a write after the
    /// failure is the actor's to refuse rather than the lane's to report closed.
    #[tokio::test]
    async fn a_write_the_pipe_refuses_is_reported_broken_and_the_lane_stays_open() {
        let (sender, receiver) = mpsc::channel(1);
        let mut lane = StdinLane::new(sender);
        let (read_end, write_end) = std::io::pipe().unwrap();
        drop(read_end);
        let pipe =
            tokio::net::unix::pipe::Sender::from_owned_fd(std::os::fd::OwnedFd::from(write_end))
                .unwrap();
        let job_id = JobId::new(1).unwrap();
        let (events, mut observed) = mpsc::channel(4);
        let pump = tokio::spawn(run_system_stdin(job_id, pipe, receiver, events));
        assert!(lane.try_write(Bytes::from_static(b"lost\n")).unwrap());
        assert!(matches!(
            observed.recv().await,
            Some(ProcessEvent::StdinBroken { job_id: broken, .. }) if broken == job_id
        ));
        assert!(
            lane.try_write(Bytes::from_static(b"later\n")).unwrap(),
            "the lane still takes writes: only the actor refuses them"
        );
        lane.end();
        pump.await.unwrap();
        assert!(
            observed.recv().await.is_none(),
            "nothing reported after the break"
        );
    }
}

#[cfg(test)]
mod label_report_tests {
    use super::*;

    /// A skipped volume label is said in the daemon log, with the image and why; only a name
    /// already right is silent.
    #[test]
    fn a_skipped_volume_label_is_said_and_only_a_right_one_is_silent() {
        use cowshed_os::namespace::{HostNamespace, TestNamespace};
        let name = WorkspaceName::new("slotted").expect("workspace name");
        let label = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"))
            .volume_label(format_args!("acme · widget — slotted"));
        let image = Path::new("/private/cowshed/store/acme/widget/sessions/slotted.dmg");
        let report = |outcome: io::Result<Labelled>| {
            label_report(
                &name,
                "volume",
                image,
                &label,
                &outcome,
                Duration::from_millis(5),
            )
        };
        for skipped in [Labelled::NotAttached, Labelled::NotMounted] {
            let line = report(Ok(skipped)).expect("a skip is said");
            assert!(
                line.contains(&format!("{skipped:?}"))
                    && line.contains(&image.display().to_string()),
                "{line}"
            );
        }
        assert!(report(Ok(Labelled::Renamed)).is_some());
        assert!(
            report(Err(io::Error::other("refused"))).is_some_and(|line| line.contains("refused"))
        );
        assert_eq!(report(Ok(Labelled::Already)), None);
    }
}

#[cfg(test)]
mod admission_environment_commitment_tests {
    use super::admission_env_sha256;
    use std::collections::{BTreeSet, HashMap};
    #[test]
    fn admission_environment_commitment_is_sorted_framed_and_credential_filtered() {
        let withheld = BTreeSet::from(["TOKEN".to_owned()]);
        let env = HashMap::from([
            ("PROJECT_CUSTOM".to_owned(), "first".to_owned()),
            ("LANG".to_owned(), "C".to_owned()),
        ]);
        let reordered = HashMap::from([
            ("LANG".to_owned(), "C".to_owned()),
            ("PROJECT_CUSTOM".to_owned(), "first".to_owned()),
            ("TOKEN".to_owned(), "private".to_owned()),
        ]);
        assert_eq!(
            admission_env_sha256(&env, &withheld),
            admission_env_sha256(&reordered, &withheld)
        );
        let changed = HashMap::from([
            ("PROJECT_CUSTOM".to_owned(), "second".to_owned()),
            ("LANG".to_owned(), "C".to_owned()),
        ]);
        assert_ne!(
            admission_env_sha256(&env, &withheld),
            admission_env_sha256(&changed, &withheld)
        );
        let left = HashMap::from([("A".to_owned(), "BC".to_owned())]);
        let right = HashMap::from([("AB".to_owned(), "C".to_owned())]);
        assert_ne!(
            admission_env_sha256(&left, &withheld),
            admission_env_sha256(&right, &withheld),
            "component boundaries are committed"
        );
    }
}

#[cfg(test)]
mod install_observation_tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;

    #[test]
    fn an_absent_marker_is_not_observed_and_removing_it_is_idempotent() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "install-observation-absent",
        );
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        assert!(!marker.observed().unwrap());
        marker.remove().unwrap();
        marker.remove().unwrap();
        assert!(!marker.observed().unwrap());
    }

    #[test]
    fn a_regular_marker_is_observed_until_it_is_removed() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "install-observation-regular",
        );
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        fs::write(&marker.path, b"observed\n").unwrap();
        assert!(marker.observed().unwrap());
        marker.remove().unwrap();
        assert!(!marker.observed().unwrap());
        assert!(fs::symlink_metadata(&marker.path).is_err());
    }

    #[test]
    fn a_symlink_marker_is_refused_and_unlinked_without_touching_its_target() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "install-observation-symlink",
        );
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let foreign = scratch.join("foreign");
        fs::write(&foreign, b"keep\n").unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        symlink(&foreign, &marker.path).unwrap();
        assert!(marker.observed().is_err());
        marker.remove().unwrap();
        assert!(fs::symlink_metadata(&marker.path).is_err());
        assert_eq!(fs::read(&foreign).unwrap(), b"keep\n");
    }

    #[test]
    fn a_swapped_parent_cannot_redirect_the_marker_to_a_foreign_directory() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "install-observation-swap",
        );
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        let leaf = marker.name.to_str().unwrap().to_owned();
        fs::write(&marker.path, b"observed\n").unwrap();
        let held_parent = exec.join(INSTALL_OBSERVATION_DIRECTORY);
        let moved = exec.join("moved");
        fs::rename(&held_parent, &moved).unwrap();
        let foreign_dir = scratch.join("foreign-dir");
        fs::create_dir_all(&foreign_dir).unwrap();
        fs::write(foreign_dir.join(&leaf), b"foreign\n").unwrap();
        symlink(&foreign_dir, &held_parent).unwrap();
        assert!(
            marker.observed().unwrap(),
            "the held directory still holds the marker"
        );
        marker.remove().unwrap();
        assert_eq!(fs::read(foreign_dir.join(&leaf)).unwrap(), b"foreign\n");
        assert!(fs::symlink_metadata(moved.join(&leaf)).is_err());
    }

    /// A tracked checkout with one scoped package link to a live sibling, captured as a job would.
    fn captured_checkout(
        scratch: &Path,
    ) -> (PathBuf, PathBuf, crate::developer_links::DeveloperLinks) {
        use cowshed_os::fork_lock::Run as _;
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            std::process::Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
                .unwrap()
                .success()
        );
        let sibling = scratch.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        let stores =
            crate::developer_links::PackageStores::from_home(&scratch.join("home")).unwrap();
        let links = crate::developer_links::DeveloperLinks::capture_job(&root, stores).unwrap();
        (root, sibling, links)
    }

    #[test]
    fn an_unreadable_marker_counts_unobserved_and_still_restores_a_registry_replacement() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "install-observation-directory-marker",
        );
        let (root, sibling, links) = captured_checkout(&scratch);
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        fs::create_dir_all(root.join("node_modules/@scope/dev")).unwrap();
        fs::write(
            root.join("node_modules/@scope/dev/registry-payload"),
            "installed\n",
        )
        .unwrap();
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        fs::create_dir(&marker.path).unwrap();
        let finish = finish_terminal_restore(&root, links, Some(marker));
        assert!(
            finish
                .findings
                .iter()
                .any(|finding| finding.contains("unreadable")),
            "an unreadable marker is a finding: {:?}",
            finish.findings
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
            sibling
        );
        assert!(
            finish.failure.is_some(),
            "a marker that cannot be removed is reported explicitly, after the restore"
        );
    }

    #[test]
    fn a_read_only_marker_parent_cannot_disable_the_restore_and_its_removal_is_reported() {
        use std::os::unix::fs::PermissionsExt as _;
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "install-observation-read-only",
        );
        let (root, sibling, links) = captured_checkout(&scratch);
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        fs::write(&marker.path, b"observed\n").unwrap();
        let parent = exec.join(INSTALL_OBSERVATION_DIRECTORY);
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
        let finish = finish_terminal_restore(&root, links, Some(marker));
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
            sibling,
            "the observed install's removed link is restored despite the read-only marker"
        );
        assert!(
            finish.failure.is_some(),
            "the marker that could not be removed is reported"
        );
    }

    /// A link restoration that cannot open its parent fails as the links cause alone, labelled as
    /// developer-link restoration, with the marker removed and no observation cleanup failure.
    #[test]
    fn a_refused_link_parent_is_a_links_failure_and_nothing_else() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "terminal-cleanup-links-only",
        );
        let (root, _sibling, links) = captured_checkout(&scratch);
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        fs::remove_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("node_modules/@scope"), b"not a directory\n").unwrap();
        let finish = finish_terminal_restore(&root, links, None);
        match &finish.failure {
            Some(TerminalCleanupFailure::Links(_)) => {}
            other => panic!(
                "expected a links failure, got {:?}",
                other.as_ref().map(|f| f.label())
            ),
        }
        let error = finish.failure.unwrap().into_error();
        assert!(
            error
                .message
                .starts_with("developer-link restoration failed: "),
            "{}",
            error.message
        );
    }

    /// A marker that cannot be removed, with the links restorable, fails as the observation cleanup
    /// alone. The registry replacement is still repaired first.
    #[test]
    fn a_marker_removal_failure_is_an_observation_failure_after_a_real_restore() {
        use std::os::unix::fs::PermissionsExt as _;
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "terminal-cleanup-marker-only",
        );
        let (root, sibling, links) = captured_checkout(&scratch);
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        fs::create_dir_all(root.join("node_modules/@scope/dev")).unwrap();
        fs::write(
            root.join("node_modules/@scope/dev/registry-payload"),
            "installed\n",
        )
        .unwrap();
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        fs::write(&marker.path, b"observed\n").unwrap();
        let parent = exec.join(INSTALL_OBSERVATION_DIRECTORY);
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
        let finish = finish_terminal_restore(&root, links, Some(marker));
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
            sibling
        );
        match &finish.failure {
            Some(TerminalCleanupFailure::InstallObservation(_)) => {}
            other => panic!(
                "expected an observation failure, got {:?}",
                other.as_ref().map(|f| f.label())
            ),
        }
        let error = finish.failure.unwrap().into_error();
        assert!(
            error
                .message
                .starts_with("install observation cleanup failed: "),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn a_cleanup_task_failure_does_not_claim_a_filesystem_failure() {
        let finish = tokio::task::spawn_blocking(|| -> JobFinish {
            panic!("cleanup task panic oracle");
        })
        .await
        .unwrap_or_else(JobFinish::task_failed);
        let failure = finish.failure.expect("the task failure is recorded");
        assert!(matches!(failure, TerminalCleanupFailure::Task(_)));
        let label = failure.label();
        assert!(label.starts_with("terminal cleanup task failed: "));
        assert!(!label.contains("developer-link restoration"));
        assert!(!label.contains("install observation cleanup"));
        let error = failure.into_error();
        assert!(error.message.starts_with("terminal cleanup task failed: "));
    }

    /// Both causes at once keep both errors: the label names each, and the one error carries both.
    #[test]
    fn both_cleanup_causes_are_kept_and_named() {
        use std::os::unix::fs::PermissionsExt as _;
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "terminal-cleanup-both",
        );
        let (root, _sibling, links) = captured_checkout(&scratch);
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        fs::remove_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("node_modules/@scope"), b"not a directory\n").unwrap();
        let exec = scratch.join("exec");
        fs::create_dir_all(&exec).unwrap();
        let marker = InstallObservation::new(&exec).unwrap();
        fs::write(&marker.path, b"observed\n").unwrap();
        let parent = exec.join(INSTALL_OBSERVATION_DIRECTORY);
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
        let finish = finish_terminal_restore(&root, links, Some(marker));
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let failure = finish.failure.expect("both causes fail");
        assert!(matches!(failure, TerminalCleanupFailure::Both { .. }));
        let label = failure.label();
        assert!(label.contains("developer-link restoration failed"));
        assert!(label.contains("install observation cleanup failed"));
        let error = failure.into_error();
        assert!(
            error
                .message
                .starts_with("developer-link restoration failed: "),
            "{}",
            error.message
        );
        assert!(
            error
                .message
                .contains("install observation cleanup also failed: "),
            "{}",
            error.message
        );
    }
}

/// Both initial admission and grant advance add exactly the controller's narrow endpoint.
fn job_sandbox_policy(
    mut sandbox: SandboxConfig,
    membership_socket: Option<&Path>,
) -> Result<SandboxPolicy> {
    if let Some(socket) = membership_socket
        && !sandbox
            .allowed_unix_sockets
            .iter()
            .any(|allowed| allowed == socket)
    {
        sandbox.allowed_unix_sockets.push(socket.to_owned());
    }
    SandboxPolicy::render(sandbox)
}

#[cfg(target_os = "macos")]
fn membership_peer(
    peer: &super::job_groups::Process,
) -> Result<Option<super::process_tree::ProcessIdentity>> {
    peer.record()
        .map(|record| {
            record.map(|record| super::process_tree::ProcessIdentity {
                pid: record.pid,
                birth: super::process_tree::BirthToken(record.unique_id),
            })
        })
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot authenticate membership endpoint peer: {error}"),
                "retain the subtree claim and retry while the requesting process is alive",
            )
        })
}

#[cfg(test)]
mod artifact_error_tests {
    use super::*;

    /// A records lock held past its bound is reported as the wait it is, naming the lock and its
    /// holder, and never sends the operator to repair a store that is intact.
    #[test]
    fn a_records_lock_expiry_is_a_wait_on_its_holder_not_store_damage() {
        let expired =
            cowshed_os::deadline::start("job-records-lock", cowshed_os::deadline::CONTROL).expired(
                cowshed_os::flock::LockHolder {
                    lock: "/w/.cowshed/job/records.lock".into(),
                    holder: "pid 7 (cowshed) holds it".into(),
                },
                cowshed_os::deadline::Progress::Nothing,
                cowshed_os::deadline::clock(),
            );
        let error = map_artifact_error(ArtifactError::LockExpired(Box::new(expired)));
        assert_eq!(error.code, crate::error::ErrorCode::EnvironmentMissing);
        assert!(
            error.message.starts_with(
                "TIMEOUT: job-records-lock on lock /w/.cowshed/job/records.lock held by pid 7 (cowshed) holds it"
            ),
            "{}",
            error.message
        );
        assert!(!error.hint.contains("doctor"), "{}", error.hint);
    }
}
