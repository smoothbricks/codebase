//! A workspace supervisor served over its Unix socket, and the handle that reaches one
//! (11_shell.md "Protocol").
//!
//! Each non-streaming call is one connection: one request and optional bytes, one answer and
//! optional bytes. A long wait holds only its own connection. Process events hold a separate
//! connection and use the worker stream's next/close demands; close remains readable while a
//! next is pending. The server fences every call by the authority the client names, so a client
//! with an older grant revision or incarnation is refused rather than served under another profile.
//!
//! The server accepts only peers with its own uid; the socket is created mode `0600` inside a
//! `0700` directory by the process that serves it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{CString, OsStr};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, ReadBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

use super::job_groups::{ExitObserved, Process};
use super::process_stream::JobProcessStream;
use super::supervisor::{
    Command, LogChunk, NxDaemonRetirement, RetireWhen, SessionSnapshot, SessionToken,
    WorkspaceAuthoritySnapshot, WorkspaceSupervisorHandle,
};
use super::wait_spans::{WaitFailure, peer_pid};
use crate::api::dto::{
    AdmissionKey, CommandArg, ExecClass, ExecCommand, ExecRequest, ExecScope, JobId, JobInfo,
    JobJournalCursor, JobState, JobTailLimits, OutputPublication, RunSandboxMode, ScriptCommand,
    Sha256Digest, StdinSource, TraceContext, WorkspacePath,
};
#[cfg(not(target_os = "macos"))]
use crate::api::process::JobSubtreeRefusal;
use crate::api::process::{
    JobProcessEvent, JobSubtreeAdmission, JobSubtreeHandle, JobSubtreeRetirement,
};
use crate::api::resources::SampleInterval;
use crate::api::server::codec::Demand;
use crate::census::CensusScope;
use crate::error::{CowshedError, Result};
use crate::storage::job_artifact::StreamKind;
use cowshed_os::deadline::{self, Occupant, PeerStream, Progress, Silent, WaitError, occupant};
use cowshed_os::flock::{self, LeaseMode, NamedLeaseError};
use cowshed_os::fork_lock::Fenced;

/// The identity of a cowshed build: the Mach-O `LC_UUID` the linker derives from the image's
/// contents on macOS, the SHA-256 of the executable elsewhere. Two binaries share one only when
/// they are the same build, so a supervisor started by another build — whatever changed, and
/// whether or not anybody remembered to say so — is refused by name at `hello` and drained by the
/// manager of a newly started daemon (11_shell.md "Draining a supervisor of another build").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BuildId(String);

impl BuildId {
    /// This process's build: read once, from the image this code was linked into.
    pub fn current() -> Result<&'static Self> {
        static CURRENT: std::sync::LazyLock<std::result::Result<BuildId, String>> =
            std::sync::LazyLock::new(|| image_build_id().map(BuildId));
        CURRENT.as_ref().map_err(|reason| {
            CowshedError::internal(format!("cannot identify this cowshed build: {reason}"))
        })
    }

    /// The build another process named on the wire.
    pub(crate) fn named(build: &str) -> Self {
        Self(build.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BuildId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The `LC_UUID` of the Mach-O image holding this function, as 32 lowercase hex digits.
#[cfg(target_os = "macos")]
fn image_build_id() -> std::result::Result<String, String> {
    const MH_MAGIC_64: u32 = 0xfeed_facf;
    const LC_UUID: u32 = 0x1b;
    /// `mach_header_64`: magic, cputype, cpusubtype, filetype, ncmds, sizeofcmds, flags, reserved.
    const HEADER_BYTES: usize = 32;
    const NCMDS_OFFSET: usize = 16;
    let mut info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
    // SAFETY: `dladdr` fills `info` for an address inside a loaded image, and a function of this
    // image is one; it returns 0 and leaves `info` unwritten otherwise.
    if unsafe { libc::dladdr(image_build_id as *const libc::c_void, info.as_mut_ptr()) } == 0 {
        return Err("dladdr found no image holding this code".to_owned());
    }
    // SAFETY: `dladdr` returned non-zero, so it initialized `info`.
    let base = unsafe { info.assume_init() }
        .dli_fbase
        .cast::<u8>()
        .cast_const();
    if base.is_null() {
        return Err("dladdr named no image header".to_owned());
    }
    let word = |offset: usize| {
        // SAFETY: every offset read lies inside the image's header or its load commands, which
        // dyld maps with the image for its whole lifetime; `read_unaligned` needs no alignment.
        unsafe { base.add(offset).cast::<u32>().read_unaligned() }
    };
    if word(0) != MH_MAGIC_64 {
        return Err(format!(
            "the image header is not 64-bit Mach-O (magic {:#x})",
            word(0)
        ));
    }
    let mut offset = HEADER_BYTES;
    for _ in 0..word(NCMDS_OFFSET) {
        let (command, size) = (word(offset), word(offset + 4));
        if command == LC_UUID {
            // SAFETY: `uuid_command` is its 8-byte header, then the 16-byte UUID.
            let uuid = unsafe { std::slice::from_raw_parts(base.add(offset + 8), 16) };
            return Ok(uuid.iter().map(|byte| format!("{byte:02x}")).collect());
        }
        offset += size as usize;
    }
    Err("the image carries no LC_UUID".to_owned())
}

/// The SHA-256 of the executable, read through `/proc/self/exe`, which names the executed file
/// even after its path is replaced. Read once per process.
#[cfg(not(target_os = "macos"))]
fn image_build_id() -> std::result::Result<String, String> {
    use sha2::Digest as _;
    let mut image = std::fs::File::open("/proc/self/exe")
        .map_err(|error| format!("cannot open /proc/self/exe: {error}"))?;
    let mut hasher = sha2::Sha256::new();
    io::copy(&mut image, &mut hasher)
        .map_err(|error| format!("cannot read /proc/self/exe: {error}"))?;
    Ok(Sha256Digest::from_bytes(hasher.finalize().into()).to_hex())
}

/// How long a commitments read waits for one to arrive before answering empty.
const COMMITMENT_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Bound on one JSON frame: an exec request carries at most a 1 MiB command plus its
/// environment; a job list is bounded by the jobs one supervisor keeps resident.
const MAX_JSON_FRAME: usize = 16 * 1024 * 1024;
/// Bound on one raw frame: inline stdin rides here whole.
const MAX_BYTES_FRAME: usize = 64 * 1024 * 1024;
/// Chunk size a stream stdin is forwarded in; the supervisor's own write bound.
const STREAM_CHUNK: usize = 64 * 1024;
/// Bounded queue between a forwarded stdin stream and the job that reads it.
const STREAM_DEPTH: usize = 4;

/// Hex digits of the workspace digest that names a supervisor's socket ([`socket_path`]).
const SOCKET_DIGEST_HEX: usize = 32;

/// Where the supervisor of `workspace` in `repo_id` listens: the store's `run` directory, under
/// a digest of the pair. Owner, repository and workspace names together can exceed the 104
/// bytes a Unix socket path may hold on macOS; the digest keeps every path at 64.
pub fn socket_path(
    store_root: &Path,
    repo_id: &crate::repository::RepoId,
    workspace: &crate::metadata::WorkspaceName,
) -> PathBuf {
    let mut identity = Vec::with_capacity(repo_id.as_str().len() + workspace.as_str().len() + 1);
    identity.extend_from_slice(repo_id.as_str().as_bytes());
    // NUL is in neither name, so no two pairs share an identity.
    identity.push(0);
    identity.extend_from_slice(workspace.as_str().as_bytes());
    let digest = Sha256Digest::compute(&identity).to_hex();
    store_root
        .join("run")
        .join(format!("{}.sock", &digest[..SOCKET_DIGEST_HEX]))
}

/// Whether `name`, an entry of the store's `run` directory, is a workspace supervisor's socket:
/// exactly the name [`socket_path`] gives one, its digest's first 32 lowercase hex digits and then
/// `.sock`. Nothing else there is one: not the manager's `manager.sock`, not a supervisor's
/// `<digest>.members.sock` membership endpoint ([`membership_socket_path`]), its `.groups`
/// ledger or `.sock.lock` lease, nor a `.bind` listener still being published.
pub fn is_socket_name(name: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt as _;
    name.as_bytes()
        .strip_suffix(b".sock")
        .is_some_and(|digest| {
            digest.len() == SOCKET_DIGEST_HEX
                && digest
                    .iter()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
}
/// Only the membership endpoint is exported to admitted children, never coordinator authority.
pub const MEMBERSHIP_SOCKET_ENV: &str = "COWSHED_SUPERVISOR_SOCKET";

pub fn membership_socket_path(supervisor: &Path) -> PathBuf {
    supervisor.with_extension("members.sock")
}

/// This separate closed grammar is the endpoint's authority boundary. New coordinator methods
/// cannot silently become callable by a sandbox job.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
enum MembershipRequest {
    SubtreeHandle,
    #[serde(rename_all = "camelCase")]
    RetireDescendants {
        handle: JobSubtreeHandle,
        grace_ms: u64,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
enum Request {
    /// The authority the supervisor serves, so a client can tell a stale socket from its own.
    Hello,
    /// Re-read the workspace's grants and serve under their revision from now on; answered
    /// with the authority served afterwards. Jobs already running keep the profile they
    /// started under.
    Advance,
    /// Admit nothing more, let every running job finish, then retire; answered at once with
    /// the serving pid. Its shape never changes across builds: it is how a daemon of a newer
    /// build retires a supervisor it cannot otherwise talk to.
    Drain,
    /// The commitments recorded after cursor `after`, waiting a while for one when there is
    /// none (`commitment_feed`).
    #[serde(rename_all = "camelCase")]
    Commitments { after: u64 },
    /// Forget every commitment up to and including cursor `through`: it is forwarded.
    #[serde(rename_all = "camelCase")]
    AcknowledgeCommitments { through: u64 },
    Call {
        authority: AuthorityWire,
        call: Box<Call>,
        /// Length of the raw frame that follows, if any.
        bytes: usize,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HelloWire {
    build: BuildId,
    authority: AuthorityWire,
    pid: u32,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AuthorityWire {
    repo_id: crate::repository::RepoId,
    workspace: crate::metadata::WorkspaceName,
    workspace_incarnation: crate::metadata::WorkspaceIncarnation,
    grant_revision: u64,
    lifecycle_revision: u64,
}

impl From<&WorkspaceAuthoritySnapshot> for AuthorityWire {
    fn from(authority: &WorkspaceAuthoritySnapshot) -> Self {
        Self {
            repo_id: authority.repo_id.clone(),
            workspace: authority.workspace.clone(),
            workspace_incarnation: authority.workspace_incarnation.clone(),
            grant_revision: authority.grant_revision,
            lifecycle_revision: authority.lifecycle_revision,
        }
    }
}

impl From<AuthorityWire> for WorkspaceAuthoritySnapshot {
    fn from(wire: AuthorityWire) -> Self {
        Self {
            repo_id: wire.repo_id,
            workspace: wire.workspace,
            workspace_incarnation: wire.workspace_incarnation,
            grant_revision: wire.grant_revision,
            lifecycle_revision: wire.lifecycle_revision,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "camelCase", deny_unknown_fields)]
enum Call {
    OpenSession {
        name: Option<String>,
    },
    SessionSnapshot {
        session: SessionWire,
    },
    CloseSession {
        session: SessionWire,
    },
    #[serde(rename_all = "camelCase")]
    Exec {
        session: Option<SessionWire>,
        /// The build volume the caller resolved for this job (`WorkspaceSupervisorHandle::exec`).
        build_volume: Option<PathBuf>,
        /// The controller route that admitted the exec. It is chosen by the route that decoded the
        /// call, never by a request field or an environment value.
        source: AdmissionSource,
        request: Box<ExecWire>,
    },
    #[serde(rename_all = "camelCase")]
    StdinWrite {
        job_id: JobId,
        /// Where the request's raw frame starts in the job's input.
        offset: u64,
    },
    #[serde(rename_all = "camelCase")]
    StdinClose {
        job_id: JobId,
    },
    /// The next chunk of a job admitted with a streamed stdin.
    #[serde(rename_all = "camelCase")]
    StreamChunk {
        job_id: JobId,
    },
    /// The end of a streamed stdin: clean when `error` is absent.
    #[serde(rename_all = "camelCase")]
    StreamEnd {
        job_id: JobId,
        error: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Info {
        job_id: JobId,
    },
    #[serde(rename_all = "camelCase")]
    Sealed {
        job_id: JobId,
    },
    #[serde(rename_all = "camelCase")]
    Resources {
        job_id: JobId,
    },
    #[serde(rename_all = "camelCase")]
    Processes {
        job_id: JobId,
    },
    #[serde(rename_all = "camelCase")]
    ProcessEvents {
        job_id: JobId,
        every: SampleInterval,
    },
    /// Whether a job may be admitted onto the workspace as it is, judged with the build volume
    /// the caller resolved: `null`, or why not (`crate::resident::lifecycle_settled`,
    /// `crate::resident::build_state_current`).
    #[serde(rename_all = "camelCase")]
    AsIs {
        build_volume: Option<PathBuf>,
    },
    ShellPool,
    TraceHealth,
    #[serde(rename_all = "camelCase")]
    ListeningPorts {
        job_id: JobId,
    },
    #[serde(rename_all = "camelCase")]
    JobByKey {
        key: AdmissionKey,
    },
    /// One progress read; the subscription that makes them runs on the caller's side.
    #[serde(rename_all = "camelCase")]
    Progress {
        job_id: JobId,
    },
    List,
    #[serde(rename_all = "camelCase")]
    Kill {
        job_id: JobId,
    },
    /// Leave the job running with nobody attached (`WorkspaceSupervisorHandle::detach`).
    #[serde(rename_all = "camelCase")]
    Detach {
        job_id: JobId,
    },
    #[serde(rename_all = "camelCase")]
    Wait {
        job_id: JobId,
    },
    /// Hold the job for as long as this connection stays open (`hold`). The job's end answers
    /// it; a [`HoldDemand::Release`] lets the job outlive it; a close before either kills it.
    #[serde(rename_all = "camelCase")]
    Hold {
        job_id: JobId,
    },
    #[serde(rename_all = "camelCase")]
    LogRead {
        job_id: JobId,
        stream: StreamWire,
        offset: u64,
        follow: bool,
    },
    #[serde(rename_all = "camelCase")]
    Tail {
        job_id: JobId,
        cursor: Option<JobJournalCursor>,
        limits: JobTailLimits,
    },
    #[serde(rename_all = "camelCase")]
    Retire {
        nx_daemon: NxDaemonRetirement,
        when: RetireWhen,
    },
    /// Stop the Nx daemon keeper's starts and end its start in flight before a census of the
    /// mount (`WorkspaceSupervisorHandle::stop_nx_keeper`).
    StopNxKeeper,
    /// Name the build volume a land or refork moved the checkout's link onto.
    #[serde(rename_all = "camelCase")]
    NameBuildVolume {
        build_volume: Option<PathBuf>,
    },
}

impl Call {
    /// The call as a wait on its answer names it.
    const fn op(&self) -> &'static str {
        match self {
            Self::OpenSession { .. } => "open-session",
            Self::SessionSnapshot { .. } => "session-snapshot",
            Self::CloseSession { .. } => "close-session",
            Self::Exec { .. } => "exec",
            Self::StdinWrite { .. } => "stdin-write",
            Self::StdinClose { .. } => "stdin-close",
            Self::StreamChunk { .. } => "stream-chunk",
            Self::StreamEnd { .. } => "stream-end",
            Self::Info { .. } => "info",
            Self::Sealed { .. } => "sealed",
            Self::Resources { .. } => "resources",
            Self::Processes { .. } => "processes",
            Self::ProcessEvents { .. } => "process-events",
            Self::AsIs { .. } => "as-is",
            Self::ShellPool => "shell-pool",
            Self::TraceHealth => "trace-health",
            Self::ListeningPorts { .. } => "listening-ports",
            Self::JobByKey { .. } => "job-by-key",
            Self::Progress { .. } => "progress",
            Self::List => "list",
            Self::Kill { .. } => "kill",
            Self::Detach { .. } => "detach",
            Self::Wait { .. } => "wait",
            Self::Hold { .. } => "hold",
            Self::LogRead { .. } => "log-read",
            Self::Tail { .. } => "tail",
            Self::Retire { .. } => "retire",
            Self::StopNxKeeper => "stop-nx-keeper",
            Self::NameBuildVolume { .. } => "name-build-volume",
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum StreamWire {
    Stdout,
    Stderr,
}

impl From<StreamKind> for StreamWire {
    fn from(stream: StreamKind) -> Self {
        match stream {
            StreamKind::Stdout => Self::Stdout,
            StreamKind::Stderr => Self::Stderr,
        }
    }
}

impl From<StreamWire> for StreamKind {
    fn from(stream: StreamWire) -> Self {
        match stream {
            StreamWire::Stdout => Self::Stdout,
            StreamWire::Stderr => Self::Stderr,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionWire {
    identity: u64,
    name: Option<String>,
}

impl From<&SessionToken> for SessionWire {
    fn from(session: &SessionToken) -> Self {
        Self {
            identity: session.identity(),
            name: session.name().map(str::to_owned),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecWire {
    argv: Option<Vec<CommandArg>>,
    script: Option<ScriptCommand>,
    cwd: Option<WorkspacePath>,
    mode: RunSandboxMode,
    env: HashMap<String, String>,
    trace: Option<TraceContext>,
    stdin: StdinWire,
    stdout_copy: Option<OutputPublication>,
    stderr_copy: Option<OutputPublication>,
    admission_key: Option<AdmissionKey>,
    class: ExecClass,
    want: Option<NonZeroUsize>,
    scope: ExecScope,
    /// The requester's own jobserver fifo, which the job reads too when its sandbox can open it
    /// (05_gateway.md "Parallelism inside a job"); absent when the requester was started by none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_jobserver: Option<super::job_cpu::InheritedJobserver>,
}

/// The controller route that admitted an exec. The route is the authority for the gate class and
/// the host-gate scope (05_gateway.md "Classes", 07_api.md "Capability split"): no request
/// field, environment value or caller claim selects it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AdmissionSource {
    /// worker.exec: a workspace's own capability, which admits background, interactive and
    /// resident work in its workspace sandbox only.
    Worker,
    /// coordinator.exec and the controller's own execs: the trusted coordinator, which may also
    /// admit the gate class and the host-gate scope.
    Coordinator,
}

impl AdmissionSource {
    /// Whether this route may admit an exec of class in scope. A worker admits background,
    /// interactive and either resident's work in the workspace sandbox and nothing else -- none of
    /// them claims anything a worker may not -- and anything more is refused as the typed
    /// class-not-permitted error, before any record or process exists.
    pub fn admit(self, class: ExecClass, scope: ExecScope) -> Result<()> {
        match (self, class, scope) {
            (Self::Coordinator, _, _)
            | (
                Self::Worker,
                ExecClass::Background
                | ExecClass::Interactive
                | ExecClass::Resident
                | ExecClass::InteractiveResident,
                ExecScope::Workspace,
            ) => Ok(()),
            (Self::Worker, _, _) => Err(CowshedError::class_not_permitted(class, scope)),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum StdinWire {
    Empty,
    /// The bytes are the request's raw frame.
    Inline,
    /// The bytes follow as `StreamChunk` calls and end with `StreamEnd`.
    Stream,
    /// Stdin stays open: `StdinWrite` calls feed it and `StdinClose` ends it.
    Open,
    #[serde(rename_all = "camelCase")]
    WorkspaceFile {
        workspace_path: WorkspacePath,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionSnapshotWire {
    identity: u64,
    name: Option<String>,
    cwd: Option<WorkspacePath>,
    env: BTreeMap<String, String>,
    background_jobs: BTreeSet<JobId>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LogChunkWire {
    next_offset: u64,
    eof: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
enum Response {
    Ok {
        value: serde_json::Value,
        /// Length of the raw frame that follows, if any.
        bytes: usize,
    },
    Err(CowshedError),
    /// A call not answered yet, said every [`deadline::HEARTBEAT`] until it is (05_gateway.md,
    /// "Deadlines").
    Progress(ProgressWire),
}

/// What a call still being answered says each heartbeat: how many it has said, for a call about
/// a job that job as the supervisor's actor last published it, and that actor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProgressWire {
    seq: u64,
    job: Option<JobPulse>,
    actor: ActorPulse,
}

/// The supervisor's actor as a heartbeat reports it (05_gateway.md, "Deadlines"): the turns it
/// has completed and the turn it is in, by what that turn handles and how long it has run. A
/// turn that holds a call up names itself here, and one that never ends shows a `forMs` that
/// keeps rising while the turn count stays put.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ActorPulse {
    turns: u64,
    busy: Option<TurnBusy>,
}

/// The turn an actor is in: the command or event it handles, and for how long so far.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TurnBusy {
    turn: String,
    for_ms: u64,
}

/// What one heartbeat reports besides its sequence number.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Pulse {
    job: Option<JobPulse>,
    actor: ActorPulse,
}

/// A job as a heartbeat reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct JobPulse {
    pub(crate) job_id: JobId,
    state: JobState,
    /// How many processes of the job's group its latest sample found running; absent before the
    /// job's first sample.
    members: Option<usize>,
}

impl JobPulse {
    pub(crate) fn of(info: &JobInfo) -> Self {
        Self {
            job_id: info.job_id,
            state: info.state,
            members: info.resources.as_ref().map(|sample| sample.members.len()),
        }
    }

    /// Whether the job is getting somewhere. Queued, it waits for tokens, which have no admission
    /// deadline; running or draining, something of its group still runs; ended, its answer is on
    /// the way. A job whose group is empty but which has not ended is stuck between its last
    /// process and its end, and a waiter must not take that for progress.
    fn advances(self) -> bool {
        match self.state {
            JobState::Queued => true,
            JobState::Running | JobState::Draining => self.members != Some(0),
            JobState::Exited
            | JobState::Signaled
            | JobState::Killed
            | JobState::OutputLimit
            | JobState::Failed => true,
        }
    }
}

pub(super) fn protocol_error(message: impl Into<String>) -> CowshedError {
    CowshedError::integrity(
        format!("workspace supervisor protocol: {}", message.into()),
        "cowshed doctor --json",
    )
}

fn unavailable(path: &Path, error: &io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!(
            "the workspace supervisor at {} is not reachable: {error}",
            path.display()
        ),
        "retry; cowshed restarts the workspace supervisor",
    )
}

async fn write_frame(
    stream: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
    maximum: usize,
) -> Result<()> {
    crate::api::frame::write_frame(
        stream,
        bytes,
        maximum,
        || protocol_error("frame exceeds its bound"),
        |error| protocol_error(format!("write failed: {error}")),
    )
    .await
}

async fn read_frame(stream: &mut (impl AsyncRead + Unpin), maximum: usize) -> Result<Vec<u8>> {
    crate::api::frame::read_frame(
        stream,
        maximum,
        || protocol_error("frame exceeds its bound"),
        |error| protocol_error(format!("read failed: {error}")),
    )
    .await
}

pub(super) async fn write_json<T: Serialize>(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &T,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| protocol_error(format!("encoding failed: {error}")))?;
    write_frame(stream, &bytes, MAX_JSON_FRAME).await
}

/// What one heartbeat reports, read from what the supervisor's actor last published and never
/// through the actor (05_gateway.md, "Deadlines"), so a turn that waits on the blocking pool
/// keeps every heartbeat going and is named in each. A call about a job hears that job as of
/// the actor's last completed turn; a job it has concluded, or never held, has no pulse. An
/// actor that has stopped publishes nothing more: no heartbeat goes out, and the caller's
/// silence bound reports it.
fn pulse(supervisor: &WorkspaceSupervisorHandle, job: Option<JobId>) -> Result<Pulse> {
    let liveness = supervisor.liveness()?;
    Ok(Pulse {
        job: job.and_then(|job_id| liveness.job(job_id)),
        actor: ActorPulse {
            turns: liveness.turns,
            busy: liveness.turn.map(|turn| TurnBusy {
                turn: turn.kind.to_owned(),
                for_ms: u64::try_from(turn.since.elapsed().as_millis()).unwrap_or(u64::MAX),
            }),
        },
    })
}

/// The heartbeat writer of a call or stream (05_gateway.md, "Deadlines"): every
/// [`deadline::HEARTBEAT`], what `probe` reads, as a [`Response::Progress`] frame.
struct Heartbeat<P> {
    probe: P,
    beats: tokio::time::Interval,
    seq: u64,
}

impl<P> Heartbeat<P>
where
    P: FnMut() -> Result<Pulse>,
{
    fn new(probe: P) -> Self {
        Self {
            probe,
            beats: deadline::beats(),
            seq: 0,
        }
    }

    async fn due(&mut self) {
        self.beats.tick().await;
    }

    /// What the next heartbeat reports, or `None` when the probe failed: no heartbeat goes out,
    /// the caller's silence bound then names this supervisor, and the probe's error is said here,
    /// where it happened.
    fn read(&mut self) -> Option<Pulse> {
        match (self.probe)() {
            Ok(pulse) => Some(pulse),
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: a call's heartbeat could not read the supervisor: {}",
                    error.message
                );
                None
            }
        }
    }

    /// Write one heartbeat frame, whole. Only a failed write is an error: the caller is gone.
    async fn write(&mut self, writer: &mut (impl AsyncWrite + Unpin), pulse: Pulse) -> Result<()> {
        self.seq += 1;
        let progress = ProgressWire {
            seq: self.seq,
            job: pulse.job,
            actor: pulse.actor,
        };
        write_json(writer, &Response::Progress(progress)).await
    }

    /// Read and write one heartbeat.
    async fn beat(&mut self, writer: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
        match self.read() {
            Some(pulse) => self.write(writer, pulse).await,
            None => Ok(()),
        }
    }
}

/// Answer a call with `work`, heartbeating with `probe` until it is done. The work is never
/// dropped for a heartbeat: one once begun is written whole, so no frame is ever left half
/// written for the answer to follow, and once a heartbeat cannot be written (the caller left)
/// the work runs to its end unheartbeated, so an admission or a retirement it started finishes
/// its bookkeeping as before.
async fn heartbeating<T, P>(
    writer: &mut (impl AsyncWrite + Unpin),
    probe: P,
    work: impl Future<Output = T>,
) -> T
where
    P: FnMut() -> Result<Pulse>,
{
    tokio::pin!(work);
    let mut heartbeat = Heartbeat::new(probe);
    loop {
        tokio::select! {
            biased;
            answer = &mut work => return answer,
            () = heartbeat.due() => {}
        }
        if heartbeat.beat(writer).await.is_err() {
            return work.await;
        }
    }
}

pub(super) async fn read_json<T: for<'de> Deserialize<'de>>(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<T> {
    let bytes = read_frame(stream, MAX_JSON_FRAME).await?;
    serde_json::from_slice(&bytes)
        .map_err(|error| protocol_error(format!("malformed frame: {error}")))
}

async fn read_bytes(stream: &mut UnixStream, length: usize) -> Result<Bytes> {
    if length == 0 {
        return Ok(Bytes::new());
    }
    announced(read_frame(stream, MAX_BYTES_FRAME).await?, length)
}

/// A raw frame, which its JSON frame announced as `length` bytes.
fn announced(bytes: Vec<u8>, length: usize) -> Result<Bytes> {
    if bytes.len() != length {
        return Err(protocol_error(format!(
            "raw frame is {} bytes, announced {length}",
            bytes.len()
        )));
    }
    Ok(Bytes::from(bytes))
}

/// The process every client connection of this module waits on, as its failures name it.
const SUPERVISOR: &str = "workspace supervisor";

/// A wait on the supervisor that ended without its answer, as the caller's error. The message
/// is the wait's own: its code, op, the supervisor's socket and pid, the time waited and the last
/// progress seen. An expired wait also rides on it as its [`WaitFailure`], for the boundary that
/// records its span (05_gateway.md, "Deadlines", Spans).
fn waited(failure: WaitError) -> CowshedError {
    let hint = match &failure {
        WaitError::Expired(_) | WaitError::Silent(_) => {
            "the supervisor is alive and not answering; cowshed doctor --json names its process"
        }
        // The supervisor went away mid-call: the call's outcome is unknown, so the caller reads
        // job state again rather than assuming either way.
        WaitError::PeerLost { .. } => {
            "read the job's state again; cowshed restarts the workspace supervisor"
        }
        WaitError::Absent { .. } => "retry; cowshed restarts the workspace supervisor",
        WaitError::Io { .. } => "cowshed doctor --json names the supervisor and its socket",
    };
    CowshedError::environment_missing(failure.to_string(), hint)
        .with_wait(WaitFailure::of(&failure))
}

/// Connect to the supervisor at `path` for `op`, whose request has `bound` from now.
async fn open(
    path: &Path,
    op: &'static str,
    bound: deadline::Bound,
) -> Result<PeerStream<UnixStream>> {
    match PeerStream::connect(SUPERVISOR, path, deadline::start(op, bound)).await {
        Ok(stream) => Ok(stream),
        Err(failure) => Err(match refused(path, failure) {
            Refused::Absent(failure) | Refused::Failed(failure) => waited(failure),
            Refused::Live(error) => error,
        }),
    }
}

/// What a failed connect to the supervisor socket at `path` means.
enum Refused {
    /// Nothing serves the path: no socket file, or one with no socket attached any more
    /// (`occupant`). A supervisor exiting closes its listener with connections in its queue.
    Absent(WaitError),
    /// A socket is still attached and refused anyway: its queue is full, or its holder does
    /// not accept. A live peer, never proof it stopped.
    Live(CowshedError),
    Failed(WaitError),
}

fn refused(path: &Path, failure: WaitError) -> Refused {
    match failure {
        WaitError::Io {
            op, peer, source, ..
        } if source.kind() == io::ErrorKind::ConnectionRefused => match occupant(path) {
            Ok(Occupant::Absent | Occupant::Stopped) => {
                Refused::Absent(WaitError::Absent { op, peer, source })
            }
            Ok(Occupant::Live) => Refused::Live(CowshedError::environment_missing(
                format!(
                    "the workspace supervisor socket {} refuses connections while a socket is \
                     still attached to it: its queue is full, or its holder does not accept",
                    path.display()
                ),
                "retry once the operation holding it finishes; cowshed doctor --json",
            )),
            Err(error) => Refused::Live(unavailable(path, &error)),
        },
        failure @ WaitError::Absent { .. } => Refused::Absent(failure),
        failure => Refused::Failed(failure),
    }
}

/// Send `request`, and `payload` as its raw frame, within the connection's bound.
async fn send(
    stream: &mut PeerStream<impl AsyncRead + AsyncWrite + Unpin>,
    request: &impl Serialize,
    payload: &[u8],
) -> Result<()> {
    let bytes = serde_json::to_vec(request)
        .map_err(|error| protocol_error(format!("encoding failed: {error}")))?;
    send_frame(stream, &bytes, MAX_JSON_FRAME).await?;
    if !payload.is_empty() {
        send_frame(stream, payload, MAX_BYTES_FRAME).await?;
    }
    Ok(())
}

async fn send_frame(
    stream: &mut PeerStream<impl AsyncWrite + Unpin>,
    bytes: &[u8],
    maximum: usize,
) -> Result<()> {
    let length = crate::api::frame::frame_length(bytes, maximum)
        .ok_or_else(|| protocol_error("frame exceeds its bound"))?;
    let written = crate::api::frame::write_frame_io(stream, length, bytes).await;
    written.map_err(|error| waited(stream.fail(error)))
}

async fn receive_frame(
    stream: &mut PeerStream<impl AsyncRead + Unpin>,
    maximum: usize,
) -> Result<Vec<u8>> {
    let read = crate::api::frame::read_frame_io(stream, maximum).await;
    read.map_err(|error| waited(stream.fail(error)))?
        .ok_or_else(|| protocol_error("frame exceeds its bound"))
}

/// Read a call's answer, past the heartbeats the supervisor sends while it is still answering:
/// its value and raw frame, or its error. The connection's bound decides how long the supervisor
/// may say nothing. A heartbeat about a job whose processes have all ended without the job ending
/// is no progress, and the same silence bound decides how long that may last before the wait
/// fails naming the job.
async fn answer_of(
    stream: &mut PeerStream<impl AsyncRead + Unpin>,
) -> Result<(serde_json::Value, Bytes)> {
    let mut stalled = deadline::idle(stream.op(), deadline::HEARTBEAT_SILENCE);
    loop {
        let frame = receive_frame(stream, MAX_JSON_FRAME).await?;
        stalled.look(deadline::HEARTBEAT, deadline::clock());
        let response = serde_json::from_slice::<Response>(&frame)
            .map_err(|error| protocol_error(format!("malformed frame: {error}")))?;
        let pulse = match response {
            Response::Ok { value, bytes } => {
                let raw = if bytes == 0 {
                    Bytes::new()
                } else {
                    announced(receive_frame(stream, MAX_BYTES_FRAME).await?, bytes)?
                };
                return Ok((value, raw));
            }
            Response::Err(error) => return Err(error),
            Response::Progress(pulse) => pulse,
        };
        match pulse.job {
            Some(job) if !job.advances() => {
                if stalled.remaining(deadline::clock()).is_none() {
                    // The job's pulse is as of the actor's last completed turn: a turn still
                    // running, which may be the one ending this job, is named with it.
                    let turn = pulse.actor.busy.as_ref().map_or_else(String::new, |busy| {
                        format!(
                            "; the actor's {} turn has run {} ms",
                            busy.turn, busy.for_ms
                        )
                    });
                    let holder = format!(
                        "job {} ({:?} with no process of its group running{turn}) of {}",
                        job.job_id.get(),
                        job.state,
                        stream.peer()
                    );
                    return Err(stuck(
                        stalled.silent(holder, deadline::clock()),
                        peer_pid(stream.peer()),
                    ));
                }
            }
            Some(_) | None => {
                stalled.observe(Progress::Heartbeat { seq: pulse.seq }, deadline::clock());
            }
        }
    }
}

/// A job that stopped between its last process and its end, as the waiter's error, carrying the
/// silence as its [`WaitFailure`]; `supervisor_pid` is the pid the kernel reported for the
/// supervisor the job is reached through.
fn stuck(silent: Silent<String>, supervisor_pid: Option<i32>) -> CowshedError {
    CowshedError::environment_missing(
        silent.to_string(),
        "the supervisor has not published the job's end; cowshed doctor --json names the supervisor",
    )
    .with_wait(Some(WaitFailure::silent(&silent, supervisor_pid, SUPERVISOR)))
}

// ---------------------------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------------------------

/// Writers of the stdin streams jobs of this supervisor are still reading.
type Streams = Arc<Mutex<BTreeMap<JobId, mpsc::Sender<io::Result<Bytes>>>>>;

/// How a served supervisor's process takes an `advance` request: it alone can read the
/// workspace's grants and compile the profile for them.
pub type Advances = mpsc::Sender<oneshot::Sender<Result<WorkspaceAuthoritySnapshot>>>;

/// Shared custody of one supervisor socket's existing fenced file lease.
///
/// The last drop releases that flock. Cloning does not take a new lock, path, or marker.
#[derive(Clone, Debug)]
#[must_use = "dropping the last clone releases the supervisor socket lease"]
pub struct PublicationLease {
    _lease: Arc<Fenced<std::fs::File>>,
}

/// Exclusive ownership of a workspace's supervisor socket, held while serving it or changing
/// its substrate. The persistent file lease, the inode its name names, serializes binders; an older build is drained before
/// a new build binds, and the occupant probe still refuses any live inherited listener. The
/// listener and the lease are fenced: once either is closed, no child still being spawned holds
/// it, so the next binder finds the socket stopped and the lease free (`fork_lock`), unless a
/// [`PublicationLease`] still shares that same lease.
#[derive(Debug)]
#[must_use = "hold the socket until serving or the workspace mutation has finished"]
pub struct BoundSocket {
    pub(super) listener: Fenced<UnixListener>,
    /// The socket flock, shared with every [`PublicationLease`] taken from it.
    _lease: Arc<Fenced<std::fs::File>>,
    /// Where its socket file is: the workspace's socket path once published, a private name
    /// before.
    path: PathBuf,
    instance: Instance,
    /// The drain report this socket's supervisor publishes beside it, once it serves
    /// ([`Self::drain_report`]): written only while this socket is held.
    drain: Option<DrainReport>,
}

impl BoundSocket {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Another owner of this socket's fenced file lease: the same flock, not a new lock.
    #[cfg(target_os = "macos")]
    #[must_use = "hold it until the group-ledger writer drops it after its final snapshot"]
    pub(crate) fn publication_lease(&self) -> PublicationLease {
        PublicationLease {
            _lease: Arc::clone(&self._lease),
        }
    }

    /// The drain report beside this socket. Only the lease's holder writes it, so the socket's
    /// release closes it to every writer of this process before it withdraws it.
    fn drain_report(&mut self) -> DrainReport {
        self.drain
            .get_or_insert_with(|| DrainReport::beside(&self.path))
            .clone()
    }
}

impl Drop for BoundSocket {
    fn drop(&mut self) {
        // The listener and lease remain held here. From now on no task of this process
        // publishes a drain report: the lease that made it the report's writer is going.
        if let Some(report) = &self.drain {
            report.close();
        }
        // Remove only this socket's own file, then the report it published.
        match Instance::at(&self.path) {
            Ok(Some(instance)) if instance == self.instance => {
                if let Err(error) = std::fs::remove_file(&self.path) {
                    cowshed_os::eprintln!(
                        "cowshed: cannot remove retired supervisor socket {}: {error}",
                        self.path.display()
                    );
                }
            }
            Ok(_) => {}
            Err(error) => cowshed_os::eprintln!(
                "cowshed: cannot inspect retired supervisor socket {}: {error}",
                self.path.display()
            ),
        }
        if let Some(report) = &self.drain {
            report.withdraw();
        }
    }
}

/// One socket file. A bind always creates a new file, and no other file is given an inode while
/// the file holding it exists, so no two existing files share an identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Instance {
    device: u64,
    inode: u64,
}

impl Instance {
    /// The socket file at `path`, read without following a symlink; `None` when nothing is
    /// there. Anything else there is no supervisor's socket, and an error.
    fn at(path: &Path) -> io::Result<Option<Self>> {
        use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => Ok(Some(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            })),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is not a socket", path.display()),
            )),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// When the supervisor serving the socket file at `path` started serving it -- the file's
/// modification time, set when its listener was bound and never after -- or `None` when no
/// socket is attached to it.
pub(crate) fn serving_since(path: &Path) -> io::Result<Option<std::time::SystemTime>> {
    match occupant(path)? {
        Occupant::Live => match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata.modified().map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        },
        Occupant::Absent | Occupant::Stopped => Ok(None),
    }
}

/// Rename `from` to `to` in one step the kernel performs whole, refusing an existing `to`:
/// [`cowshed_os::fsio::rename_noreplace`] against the working directory, which `from` and `to` resolve
/// from when relative. On Linux that is the `renameat2` system call itself, not glibc's wrapper,
/// which glibc only gained in 2.28 and the published arm64 Linux CLI does not link against.
fn rename_exclusive(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let native = |path: &Path| {
        std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    };
    cowshed_os::fsio::rename_noreplace(libc::AT_FDCWD, &native(from)?, libc::AT_FDCWD, &native(to)?)
}

fn socket_error(path: &Path, what: &str, error: io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {what} {}: {error}", path.display()),
        "check the cowshed runtime directory",
    )
}

/// The directory of the socket at `path`, made `0700` when missing, and the lease beside the
/// socket, `<digest>.sock.lock`: its path and the file name a system call takes.
fn lease_name(path: &Path) -> Result<(&Path, PathBuf, CString)> {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::PermissionsExt as _;
    let io = |what: &str, error: io::Error| socket_error(path, what, error);
    let directory = path.parent().ok_or_else(|| {
        CowshedError::internal(format!(
            "supervisor socket {} has no parent",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(directory).map_err(|error| io("create the directory of", error))?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
        .map_err(|error| io("restrict the directory of", error))?;
    let lease = path.with_extension("sock.lock");
    let name = lease
        .file_name()
        .and_then(|name| CString::new(name.as_bytes()).ok())
        .ok_or_else(|| {
            CowshedError::internal(format!(
                "socket lease {} has no file name a system call can take",
                lease.display()
            ))
        })?;
    Ok((directory, lease, name))
}

/// What a failed take of the lease `lease` beside the socket at `path` is; `lock` says what the
/// lock's own failure is.
fn lease_error<L>(
    path: &Path,
    lease: &Path,
    error: NamedLeaseError<io::Error, L>,
    lock: impl FnOnce(L) -> CowshedError,
) -> CowshedError {
    match error {
        NamedLeaseError::Directory(error) => socket_error(path, "open the directory of", error),
        NamedLeaseError::Open(error) => socket_error(path, "open the socket lease of", error),
        NamedLeaseError::Lock(error) => lock(error),
        NamedLeaseError::Inspect(error) => socket_error(path, "inspect the socket lease of", error),
        NamedLeaseError::Replaced => CowshedError::conflict(
            format!(
                "the socket lease {} was replaced under each of {} opens in a row",
                lease.display(),
                flock::NAMED_LEASE_REOPENS
            ),
            "check the cowshed runtime directory: only a holder of that lease may replace it",
        ),
    }
}

/// The lease beside the socket at `path`, `<digest>.sock.lock` (created `0600` in a `0700`
/// directory when missing), taken exclusive without waiting; `None` while another holder has it.
/// Whoever holds it is the one binder of that workspace's supervisor socket, or a lifecycle
/// operation that keeps every binder out. It is the inode its name names, taken through the named
/// lease protocol ([`flock::lock_named`], 11_shell.md "Protocol"): after the attempt, taken or
/// refused, the name must still name the inode it was made on, or it is reopened. `open` is
/// [`flock::open_lock`] outside tests.
fn take_lease(
    path: &Path,
    open: impl FnMut(&std::fs::File, &std::ffi::CStr) -> io::Result<Fenced<std::fs::File>>,
) -> Result<Option<Fenced<std::fs::File>>> {
    let (directory, lease, name) = lease_name(path)?;
    flock::lock_named(
        || cowshed_os::fsio::open_directory_nofollow(directory),
        &name,
        LeaseMode::Exclusive,
        open,
    )
    .map(|taken| taken.map(|(_, lease)| lease))
    .map_err(|error| {
        lease_error(path, &lease, error, |error| {
            socket_error(path, "lock the socket lease of", error)
        })
    })
}

/// The lease beside the socket at `path`, taken as [`take_lease`] takes it but as a waiter
/// ([`flock::lock_named_within`]) within the no-progress bound
/// ([`cowshed_os::test_backstop::NO_PROGRESS`]), once, with no retry. A holder past the bound is
/// refused as [`bind`] refuses one, named as `census` reads it.
fn await_lease(path: &Path, census: &CensusScope) -> Result<Fenced<std::fs::File>> {
    let (directory, lease, name) = lease_name(path)?;
    let deadline = cowshed_os::deadline::start(
        "supervisor-socket-lease",
        cowshed_os::deadline::Bound::new(
            cowshed_os::deadline::LatencyClass::Control,
            cowshed_os::test_backstop::NO_PROGRESS,
        ),
    );
    flock::lock_named_within(
        || cowshed_os::fsio::open_directory_nofollow(directory),
        &name,
        &lease,
        &deadline,
        |lock| crate::held_lock::holders(census, lock),
        flock::open_lock,
    )
    .map(|(_, lease)| lease)
    .map_err(|error| {
        lease_error(path, &lease, error, |error| match error {
            flock::LockError::Expired(expired) => CowshedError::conflict(
                format!(
                    "a supervisor or lifecycle operation owns {}: {expired}",
                    path.display()
                ),
                "wait for that operation to finish; do not unlink its socket or lock file",
            ),
            flock::LockError::Io { source, .. } => {
                socket_error(path, "lock the socket lease of", source)
            }
        })
    })
}

/// The lease of the supervisor socket at `path`, taken without waiting while no supervisor
/// serves it, so that none binds there until it is dropped; `None` while a supervisor serves it
/// or another lifecycle operation holds the lease. Making room for an attach detaches a workspace
/// only under it (media_room.rs).
pub(crate) fn lease_unserved(path: &Path) -> Result<Option<Fenced<std::fs::File>>> {
    let Some(lease) = take_lease(path, flock::open_lock)? else {
        return Ok(None);
    };
    match occupant(path)
        .map_err(|error| socket_error(path, "inspect the current server of", error))?
    {
        Occupant::Live => Ok(None),
        Occupant::Absent | Occupant::Stopped => Ok(Some(lease)),
    }
}

/// Bind the supervisor's socket at `path`: mode `0600` in a directory only this user can enter.
/// The lease serializes binders; old builds must drain first. Only a socket the kernel proves
/// unattached is removed before the new private listener is published. The lease is taken without
/// waiting: a holder is a live supervisor or another lifecycle operation, refused at once.
pub async fn bind(path: &Path) -> Result<BoundSocket> {
    let Some(lease) = take_lease(path, flock::open_lock)? else {
        return Err(CowshedError::conflict(
            format!(
                "a supervisor or lifecycle operation owns {}",
                path.display()
            ),
            "wait for that operation to finish; do not unlink its socket or lock file",
        ));
    };
    bind_leased(path, lease)
}

/// Bind the supervisor's socket at `path`, as [`bind`] does, once this process has seen the
/// supervisor that served it let it go: its retirement acknowledged, or its process exited. That
/// release closed its listener and lease, but a release can lag its close by a concurrent spawn's
/// teardown, and a waiter's grant, never a probe, proves it (01c_macos.md, "`posix_spawnp` leaves
/// transient descriptor copies"; 11_shell.md "Protocol"). So the lease is waited for
/// ([`await_lease`]), and a holder past the bound refused, named as `census` reads it.
pub async fn bind_released(path: &Path, census: &CensusScope) -> Result<BoundSocket> {
    let lease = {
        let (path, census) = (path.to_owned(), census.clone());
        tokio::task::spawn_blocking(move || await_lease(&path, &census))
            .await
            .map_err(|error| {
                CowshedError::internal(format!("waiting for a supervisor socket lease: {error}"))
            })??
    };
    bind_leased(path, lease)
}

/// Bind the supervisor's socket at `path` under `lease`, its lease, already held.
fn bind_leased(path: &Path, lease: Fenced<std::fs::File>) -> Result<BoundSocket> {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = path.parent().ok_or_else(|| {
        CowshedError::internal(format!(
            "supervisor socket {} has no parent",
            path.display()
        ))
    })?;
    let io = |what: &str, error: io::Error| socket_error(path, what, error);
    match occupant(path).map_err(|error| io("inspect the current server of", error))? {
        Occupant::Absent => {}
        Occupant::Stopped => {
            std::fs::remove_file(path)
                .map_err(|error| io("remove the stopped socket at", error))?;
        }
        Occupant::Live => {
            return Err(CowshedError::conflict(
                format!("a workspace supervisor already serves {}", path.display()),
                "stop that supervisor first",
            ));
        }
    }
    // Bound and restricted under a name of its own, then published at `path` in one rename: no
    // client reaches it with a wider mode. The process umask is not narrowed around the bind:
    // it is process-wide, and another thread's files created meanwhile would get it.
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|error| {
        io(
            "name the new socket of",
            io::Error::other(error.to_string()),
        )
    })?;
    let fresh = directory.join(format!("{:032x}.bind", u128::from_ne_bytes(random)));
    let listener =
        Fenced::create(|| UnixListener::bind(&fresh)).map_err(|error| io("bind", error))?;
    let instance = match Instance::at(&fresh) {
        Ok(Some(instance)) => instance,
        outcome => {
            // Nobody else knows the random name, so whatever is there is this listener's file.
            if let Err(error) = std::fs::remove_file(&fresh)
                && error.kind() != io::ErrorKind::NotFound
            {
                cowshed_os::eprintln!(
                    "cowshed: cannot remove unpublished supervisor socket {}: {error}",
                    fresh.display()
                );
            }
            return Err(io(
                "identify the new socket of",
                outcome.err().unwrap_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "the new socket vanished")
                }),
            ));
        }
    };
    let socket = BoundSocket {
        listener,
        _lease: Arc::new(lease),
        path: fresh,
        instance,
        drain: None,
    };
    std::fs::set_permissions(&socket.path, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| io("restrict", error))?;
    publish(socket, path)
}

/// Publish the private mode-0600 listener while its file lease is held.
fn publish(mut socket: BoundSocket, path: &Path) -> Result<BoundSocket> {
    match rename_exclusive(&socket.path, path) {
        Ok(()) => {
            socket.path = path.to_owned();
            Ok(socket)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(CowshedError::conflict(
            format!(
                "another process bound {} while this one was binding it",
                path.display()
            ),
            "stop that process first",
        )),
        Err(error) => Err(socket_error(path, "publish", error)),
    }
}

/// Serve `supervisor` on `socket`, one call per connection, until a `retire` call has
/// retired it and been answered. Releasing the bound socket removes only its own socket inode.
pub async fn serve(
    mut socket: BoundSocket,
    supervisor: WorkspaceSupervisorHandle,
    advances: Option<Advances>,
    feed: Option<super::commitment_feed::CommitmentFeed>,
) -> Result<()> {
    let membership = bind(&membership_socket_path(&socket.path)).await?;
    let streams: Streams = Arc::default();
    let retired = Arc::new(tokio::sync::Notify::new());
    let owed = Arc::new(DrainReads::default());
    let drain_report = socket.drain_report();
    let (release, mut released) = mpsc::channel::<(UnixStream, serde_json::Value, Bytes)>(1);
    loop {
        let (stream, is_membership) = tokio::select! {
            accepted = socket.listener.accept() => accepted.map(|(stream, _)| (stream, false)).map_err(|error| {
                CowshedError::environment_missing(
                    format!("workspace supervisor socket stopped accepting: {error}"),
                    "retry; cowshed restarts the workspace supervisor",
                )
            })?,
            accepted = membership.listener.accept() => accepted.map(|(stream, _)| (stream, true)).map_err(|error| {
                CowshedError::environment_missing(
                    format!("workspace membership socket stopped accepting: {error}"),
                    "retain membership claims and restart the workspace supervisor",
                )
            })?,
            response = released.recv() => {
                let (mut stream, value, bytes) = response.ok_or_else(|| {
                    CowshedError::internal("supervisor retirement reply lane closed")
                })?;
                // The reply follows the close of this socket's listener and lease. A replacement
                // binder still judges the release by its grant ([`bind_released`]): a concurrent
                // spawn's transient copy can outlast the close (01c_macos.md).
                drop(socket);
                drop(membership);
                write_json(&mut stream, &Response::Ok { value, bytes: bytes.len() }).await?;
                if !bytes.is_empty() {
                    write_frame(&mut stream, &bytes, MAX_BYTES_FRAME).await?;
                }
                return Ok(());
            }
            () = retired.notified() => return Ok(()),
        };
        let supervisor = supervisor.clone();
        let streams = Arc::clone(&streams);
        let retired = Arc::clone(&retired);
        let owed = Arc::clone(&owed);
        let advances = advances.clone();
        let feed = feed.clone();
        let release = release.clone();
        let socket_path = socket.path.clone();
        let report = drain_report.clone();
        tokio::spawn(async move {
            let served = if is_membership {
                serve_membership_call(stream, &supervisor).await
            } else {
                serve_call(
                    stream,
                    &supervisor,
                    &streams,
                    advances.as_ref(),
                    feed.as_ref(),
                    &owed,
                    &report,
                )
                .await
            };
            match served {
                Ok(Retirement::Retired {
                    stream,
                    value,
                    bytes,
                }) => {
                    if release.send((stream, value, bytes)).await.is_err() {
                        cowshed_os::eprintln!(
                            "cowshed: retired supervisor socket owner ended before its reply"
                        );
                    }
                }
                Ok(Retirement::Draining) => {
                    // Under the authority it holds now, which an advance may have moved.
                    let current = match supervisor.current_authority().await {
                        Ok(authority) => supervisor.with_authority(authority),
                        Err(error) => return report.stuck("read its authority", &error),
                    };
                    let jobs = match current.list().await {
                        Ok(jobs) => jobs,
                        Err(error) => return report.stuck("list its jobs", &error),
                    };
                    owed.listed(
                        jobs.iter()
                            .filter(|job| !job.state.is_terminal())
                            .map(|job| job.job_id),
                    );
                    owed.publish(&report);
                    if let Err(error) = current.quiesce().await {
                        return report.stuck("wait for its running jobs", &error);
                    }
                    owed.reading(&report);
                    let settled = owed.settle(super::project::SUPERVISOR_IDLE).await;
                    cowshed_os::eprintln!(
                        "cowshed: workspace supervisor {} of build {} retires after draining; {settled}",
                        socket_path.display(),
                        BuildId::current().map_or("(unnamed)", BuildId::as_str),
                    );
                    match current.retire(NxDaemonRetirement::Recorded).await {
                        // Releasing the socket withdraws the report, after it has closed
                        // publication to every task of this supervisor.
                        Ok(()) => retired.notify_one(),
                        Err(error) => report.stuck("retire", &error),
                    }
                }
                Ok(Retirement::Serving) | Err(_) => {}
            }
        });
    }
}

/// What a draining supervisor waits for, published beside its socket (`<socket>.drain`) so that
/// a command of another build it refuses can say why it still serves (11_shell.md "Draining a
/// supervisor of another build"). Read across builds: a reader decodes what it knows, and says
/// so when it cannot.
#[derive(Clone, Debug)]
struct DrainReport {
    path: PathBuf,
    /// Whether the socket it is beside was released: from then on this process is not its
    /// writer, and publishes nothing. A publication holds it across its write, so the release
    /// waits for the one in flight.
    released: Arc<Mutex<bool>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainReportWire {
    /// The supervisor that wrote it: a report whose pid is not the one serving the socket is a
    /// killed supervisor's, and says nothing.
    pid: u32,
    waits: DrainWait,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "for", rename_all = "camelCase")]
enum DrainWait {
    /// The jobs that ran when it was asked to drain, to end.
    Jobs {
        running: Vec<JobId>,
        /// The residents it ended because their requesters exited.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        ended: Vec<RequesterGone>,
    },
    /// A process these jobs are owed to, to read their ends or exit, or
    /// [`super::project::SUPERVISOR_IDLE`] with no read of them.
    Reads {
        unread: Vec<JobId>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        ended: Vec<RequesterGone>,
    },
    /// A step of its retirement failed: it serves on until it is killed.
    Stuck { step: String, error: String },
}

impl DrainWait {
    fn describe(&self) -> String {
        let jobs = |ids: &[JobId]| {
            ids.iter()
                .map(|job| job.get().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ended = |ended: &[RequesterGone]| {
            ended
                .iter()
                .map(|gone| format!("; it ended {gone}"))
                .collect::<String>()
        };
        match self {
            Self::Jobs {
                running,
                ended: gone,
            } => format!(
                "it drains: it waits for jobs {} to end{}",
                jobs(running),
                ended(gone)
            ),
            Self::Reads {
                unread,
                ended: gone,
            } if unread.is_empty() => {
                format!("it drains and is retiring{}", ended(gone))
            }
            Self::Reads {
                unread,
                ended: gone,
            } => format!(
                "it drains: its jobs ended, and it waits until a client reads the ends of jobs {} \
                 or every process they are owed to has exited, or {} minutes pass with no read \
                 of them{}",
                jobs(unread),
                super::project::SUPERVISOR_IDLE.as_secs() / 60,
                ended(gone)
            ),
            Self::Stuck { step, error } => format!(
                "its retirement failed to {step} ({error}), so it serves until it is killed"
            ),
        }
    }
}

impl DrainReport {
    fn beside(socket: &Path) -> Self {
        Self {
            path: socket.with_extension("drain"),
            released: Arc::default(),
        }
    }

    fn publish(&self, waits: DrainWait) {
        let released = self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *released {
            return;
        }
        let wire = DrainReportWire {
            pid: std::process::id(),
            waits,
        };
        let published = serde_json::to_vec(&wire)
            .map_err(|error| error.to_string())
            .and_then(|bytes| {
                cowshed_os::fsio::publish_private_file_with(
                    &self.path,
                    cowshed_os::fsio::Durability::Process,
                    |writer| std::io::Write::write_all(writer, &bytes),
                )
                .map_err(|error| match error {
                    cowshed_os::fsio::PublishError::Io { path, source } => {
                        crate::metadata::MetadataError::Io { path, source }.to_string()
                    }
                    cowshed_os::fsio::PublishError::Write(source) => {
                        crate::metadata::MetadataError::Io {
                            path: self.path.clone(),
                            source,
                        }
                        .to_string()
                    }
                })
            });
        if let Err(error) = published {
            cowshed_os::eprintln!(
                "cowshed: a draining supervisor cannot publish what it waits for at {}: {error}",
                self.path.display()
            );
        }
    }

    /// Say on stderr and in the report that retirement stopped at `step`.
    fn stuck(&self, step: &str, error: &CowshedError) {
        cowshed_os::eprintln!(
            "cowshed: a draining supervisor ({}) failed to {step}: {error}; it serves until it is \
             killed",
            self.path.display()
        );
        self.publish(DrainWait::Stuck {
            step: step.to_owned(),
            error: error.to_string(),
        });
    }

    /// Its socket is released: wait out a publication in flight and refuse every later one.
    fn close(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    }

    fn withdraw(&self) {
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => cowshed_os::eprintln!(
                "cowshed: a retired supervisor left its drain report {}: {error}",
                self.path.display()
            ),
        }
    }

    /// What the supervisor `pid` serving `socket` reported it waits for: `None` when it published
    /// nothing, or the report is another (killed) process's.
    fn read(socket: &Path, pid: u64) -> Option<String> {
        let path = socket.with_extension("drain");
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => {
                return Some(format!(
                    "its drain report {} is unreadable: {error}",
                    path.display()
                ));
            }
        };
        match serde_json::from_slice::<DrainReportWire>(&bytes) {
            Ok(wire) if u64::from(wire.pid) == pid => Some(wire.waits.describe()),
            Ok(_) => None,
            Err(error) => Some(format!(
                "its drain report {} does not decode: {error}",
                path.display()
            )),
        }
    }
}

/// The supervisor socket whose drain report, or one of whose report's writers' temps, the entry
/// `name` of the store's `run` directory is: `<digest>.drain`, or `.<digest>.drain.tmp.<uuid>`
/// as [`cowshed_os::fsio::temp_name`] names a publication's temp, its uuid 32 lowercase hex
/// digits. Anything else, a malformed temp included, is no drain residue.
pub(crate) fn drain_residue(run: &Path, name: &OsStr) -> Option<PathBuf> {
    let name = name.to_str()?;
    let report = match name.strip_prefix('.') {
        Some(temp) => {
            let (report, uuid) = temp.rsplit_once(".tmp.")?;
            let simple = uuid.len() == 32
                && uuid
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
            simple.then_some(report)?
        }
        None => name,
    };
    let socket = format!("{}.sock", report.strip_suffix(".drain")?);
    is_socket_name(OsStr::new(&socket)).then(|| run.join(socket))
}

/// Remove `residue`, the drain report beside `socket` and its writers' temps
/// ([`drain_residue`]), that a supervisor left when it was killed holding the socket: only the
/// holder of the socket's lease writes them, so they are removed only under that lease, taken
/// free with no socket attached ([`lease_unserved`]), when no writer of them remains. Each goes
/// only as a regular file of this user; anything else there is kept and named. Said on stderr, the
/// daemon's log: what this leaves, the next start finds again.
pub(crate) fn clear_dead_drain(socket: &Path, residue: &[PathBuf]) {
    use std::os::unix::fs::MetadataExt as _;
    let _lease = match lease_unserved(socket) {
        Ok(Some(lease)) => lease,
        Ok(None) => return,
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: cannot take the lease of supervisor socket {} to clear the drain report \
                 a killed supervisor left: {}",
                socket.display(),
                error.message
            );
            return;
        }
    };
    // SAFETY: geteuid takes nothing and cannot fail.
    let uid = unsafe { libc::geteuid() };
    for path in residue {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_file() && metadata.uid() == uid => {
                if let Err(error) = std::fs::remove_file(path)
                    && error.kind() != io::ErrorKind::NotFound
                {
                    cowshed_os::eprintln!(
                        "cowshed: cannot remove the drain report {} a killed supervisor left: \
                         {error}",
                        path.display()
                    );
                }
            }
            Ok(_) => cowshed_os::eprintln!(
                "cowshed: kept {}: it is named as a drain report but is not a regular file of \
                 this user",
                path.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => cowshed_os::eprintln!(
                "cowshed: cannot inspect the drain report {} a killed supervisor left: {error}",
                path.display()
            ),
        }
    }
}

/// Whether a call left the supervisor retired, or asked it to retire once its jobs end.
enum Retirement {
    Serving,
    Retired {
        stream: UnixStream,
        value: serde_json::Value,
        bytes: Bytes,
    },
    Draining,
}

/// What a draining supervisor still owes (11_shell.md "Draining a supervisor of another build"):
/// each job that ran when it was asked to drain, to the processes that asked for or about it,
/// until one of them has read its end and every stream it was reading to EOF, or every one of
/// them has exited. The next supervisor is another build, whose protocol the job's client cannot
/// speak, so the end is read from this one or not at all; a reader that is gone reads nothing,
/// so the drain never waits on it.
#[derive(Default)]
struct DrainReads {
    /// Who asked for or about each job this socket admitted that has not ended: what the job
    /// would be owed to if the supervisor drained now. An entry goes when its job ends, or into
    /// the drain.
    asked: Mutex<BTreeMap<JobId, Asked>>,
    /// `None` until the supervisor drains.
    drain: Mutex<Option<Drain>>,
    /// A read of an owed job: progress, and the idle bound starting over.
    read: tokio::sync::Notify,
    /// A process an owed job was owed to exited, or an exec in flight was recorded or refused:
    /// progress, the idle bound unchanged.
    gone: tokio::sync::Notify,
    /// Execs whose admission is in flight: each may yet admit a job the drain owes, so the
    /// drain cannot settle until it has recorded its requester or been refused.
    admitting: std::sync::atomic::AtomicUsize,
}

/// An exec in flight on [`DrainReads`]: held from before the actor is asked until the requester
/// of the job it admitted is recorded, or it is refused; dropped on every path.
struct Admitting<'a>(&'a DrainReads);

impl Drop for Admitting<'_> {
    fn drop(&mut self) {
        self.0
            .admitting
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.0.gone.notify_one();
    }
}

/// The processes that asked for or about one job, each held by the identity the kernel gave its
/// connection.
struct Asked {
    /// The class it was admitted under: a resident ends with its requester.
    class: ExecClass,
    /// The process whose connection admitted the job.
    requester: Option<Arc<Process>>,
    /// Every other process that held, waited on, sealed or read it.
    readers: Vec<Arc<Process>>,
    /// A process asked whose identity could not be read, the requester included: it is never
    /// seen exiting, so what the job is owed waits for a read or the idle bound.
    unidentified: bool,
}

impl Asked {
    fn add(&mut self, job: JobId, peer: io::Result<Process>) {
        match peer {
            Ok(peer) => {
                if !self
                    .requester
                    .iter()
                    .chain(&self.readers)
                    .any(|known| same_life(known, &peer))
                {
                    self.readers.push(Arc::new(peer));
                }
            }
            Err(error) => {
                if !self.unidentified {
                    self.unidentified = true;
                    unidentified(job, &error);
                }
            }
        }
    }
}

/// Whether `known` is `peer`'s life; a comparison the kernel cannot answer counts as another
/// process, which costs one more exit watch and never an early retirement.
fn same_life(known: &Process, peer: &Process) -> bool {
    matches!(known.same_life(peer), Ok(true))
}

fn unidentified(job: JobId, error: &io::Error) {
    cowshed_os::eprintln!(
        "cowshed: a process asked about job {} whose identity cannot be read ({error}); if the \
         supervisor drains, that job's end is owed until a read or the idle bound",
        job.get()
    );
}

/// A drain under way.
struct Drain {
    /// The jobs that ran when it was asked to drain, and any admitted before admission closed.
    running: Vec<JobId>,
    /// Whether they have all ended, so it waits for their reads.
    reading: bool,
    owed: BTreeMap<JobId, Owed>,
    /// The residents it ended because their requesters exited, in that order.
    ended: Vec<RequesterGone>,
    /// The number of the next exit watch.
    next: u64,
    /// What ends a resident whose requester exits.
    ends: ResidentEnd,
}

impl Drain {
    fn waits(&self) -> DrainWait {
        if self.reading {
            DrainWait::Reads {
                unread: self
                    .owed
                    .iter()
                    .filter(|(_, owed)| !owed.settled())
                    .map(|(job, _)| *job)
                    .collect(),
                ended: self.ended.clone(),
            }
        } else {
            DrainWait::Jobs {
                running: self.running.clone(),
                ended: self.ended.clone(),
            }
        }
    }

    /// Owe `job` to `peer` too, unless it is already owed to it: the exit to watch, if any.
    fn reader(&mut self, job: JobId, peer: io::Result<Process>) -> Option<Watch> {
        let owed = self.owed.get_mut(&job)?;
        match peer {
            Ok(peer) => {
                if owed.readers.values().any(|known| same_life(known, &peer)) {
                    return None;
                }
                Some(self.owe(job, Arc::new(peer), false))
            }
            Err(error) => {
                if !owed.unseen {
                    owed.unseen = true;
                    unidentified(job, &error);
                }
                None
            }
        }
    }

    /// Owe `job`, admitted under `class` once this drain had begun, to `requester`. Admission
    /// closes only once the drain has listed its jobs, so a job admitted meanwhile runs, and the
    /// drain waits for it as for those it listed: the exit to watch, if any.
    fn requester(
        &mut self,
        job: JobId,
        class: ExecClass,
        requester: io::Result<Process>,
    ) -> Option<Watch> {
        if !self.owed.contains_key(&job) {
            self.running.push(job);
        }
        let owed = self.owed.entry(job).or_default();
        match requester {
            Ok(requester) => Some(self.owe(job, Arc::new(requester), resident(class))),
            Err(error) => {
                owed.unseen = true;
                unidentified(job, &error);
                None
            }
        }
    }

    /// Owe `job` to `process`, whose exit ends the job when `ends_resident`: its watch.
    fn owe(&mut self, job: JobId, process: Arc<Process>, ends_resident: bool) -> Watch {
        let number = self.next;
        self.next += 1;
        self.owed
            .entry(job)
            .or_default()
            .readers
            .insert(number, Arc::clone(&process));
        Watch {
            job,
            number,
            process,
            resident: ends_resident.then(|| self.ends.clone()),
        }
    }
}

/// Whether a job of `class` serves only its requester, and so ends with it.
fn resident(class: ExecClass) -> bool {
    match class {
        ExecClass::Resident | ExecClass::InteractiveResident => true,
        ExecClass::Background | ExecClass::Interactive | ExecClass::Gate => false,
    }
}

/// One job's reads, as a client of a draining supervisor makes them, and who may still make them.
#[derive(Default)]
struct Owed {
    /// A `wait`, `hold` or `sealed` answer carried the job's end.
    end: bool,
    /// Per stream (stdout, stderr): a `logRead` was answered, and whether one answered EOF.
    /// A client that reads no stream owes none.
    reading: [bool; 2],
    eof: [bool; 2],
    /// The processes it is owed to that have not been seen exiting, by exit-watch number.
    readers: BTreeMap<u64, Arc<Process>>,
    /// One of them can never be seen exiting: its identity could not be read, or its exit
    /// cannot be watched.
    unseen: bool,
}

impl Owed {
    fn read(&self) -> bool {
        self.end && (0..2).all(|stream| !self.reading[stream] || self.eof[stream])
    }

    /// Nothing will read it: every process it was owed to has exited, or none ever asked about it
    /// (a job the supervisor started for itself, such as the Nx keeper's daemon, which no client
    /// requested or waits on).
    fn abandoned(&self) -> bool {
        !self.unseen && self.readers.is_empty()
    }

    fn settled(&self) -> bool {
        self.read() || self.abandoned()
    }
}

/// An exit a draining supervisor watches: of a process `job` is owed to.
struct Watch {
    job: JobId,
    number: u64,
    process: Arc<Process>,
    /// What ends the job once the process exits: it requested a running resident.
    resident: Option<ResidentEnd>,
}

/// What ends a resident whose requester exited: its supervisor, called under the authority it
/// holds then, and the report that says so.
#[derive(Clone)]
struct ResidentEnd {
    supervisor: WorkspaceSupervisorHandle,
    report: DrainReport,
}

/// A resident a draining supervisor ended because its requester exited (11_shell.md "Draining a
/// supervisor of another build"): nothing else sends it requests, so it would never end.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequesterGone {
    job: JobId,
    requester: i32,
    observed: ExitObserved,
}

impl std::fmt::Display for RequesterGone {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "resident job {}, whose requester pid {} exited ({})",
            self.job.get(),
            self.requester,
            self.observed
        )
    }
}

/// Which jobs' ends were read before a draining supervisor retired, which nobody was left to
/// read, and which it gave up on.
struct Settled {
    read: Vec<JobId>,
    abandoned: Vec<JobId>,
    unread: Vec<JobId>,
    ended: Vec<RequesterGone>,
    bound: std::time::Duration,
}

impl std::fmt::Display for Settled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ids = |jobs: &[JobId]| {
            jobs.iter()
                .map(|job| job.get().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        write!(formatter, "read by a client: [{}]", ids(&self.read))?;
        if !self.abandoned.is_empty() {
            write!(
                formatter,
                "; every process they were owed to exited: [{}]",
                ids(&self.abandoned)
            )?;
        }
        if !self.ended.is_empty() {
            write!(
                formatter,
                "; ended for their requesters' exits: [{}]",
                self.ended
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )?;
        }
        if !self.unread.is_empty() {
            write!(
                formatter,
                "; unread after {} minutes with no read: [{}]",
                self.bound.as_secs() / 60,
                ids(&self.unread)
            )?;
        }
        Ok(())
    }
}

impl DrainReads {
    /// An exec is about to be asked of the actor.
    fn admitting(&self) -> Admitting<'_> {
        self.admitting
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Admitting(self)
    }

    fn asked(&self) -> std::sync::MutexGuard<'_, BTreeMap<JobId, Asked>> {
        self.asked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Locked before [`Self::asked`] whenever both are held.
    fn drain(&self) -> std::sync::MutexGuard<'_, Option<Drain>> {
        self.drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `job` was admitted under `class` on a connection whose peer is `requester`: owe it there
    /// should the supervisor drain, until it ends. Decided under the drain's lock, so a job
    /// admitted while a drain begins is owed by that drain, never lost between the two.
    fn admitted(
        self: &Arc<Self>,
        job: JobId,
        class: ExecClass,
        requester: io::Result<Process>,
        supervisor: &WorkspaceSupervisorHandle,
    ) {
        let watch = {
            let mut drain = self.drain();
            match drain.as_mut() {
                Some(drain) => drain.requester(job, class, requester),
                None => {
                    let mut asked = Asked {
                        class,
                        requester: None,
                        readers: Vec::new(),
                        unidentified: false,
                    };
                    match requester {
                        Ok(requester) => asked.requester = Some(Arc::new(requester)),
                        Err(error) => asked.add(job, Err(error)),
                    }
                    self.asked().insert(job, asked);
                    let reads = Arc::clone(self);
                    let supervisor = supervisor.clone();
                    tokio::spawn(async move {
                        // An ended job is owed nothing by a drain that begins later, and a drain
                        // that began took its entry already. A wait refused leaves the entry for
                        // the drain to weigh.
                        if supervisor.wait(job).await.is_ok() {
                            reads.asked().remove(&job);
                        }
                    });
                    None
                }
            }
        };
        if let Some(watch) = watch {
            Arc::clone(self).watch(watch);
        }
    }

    /// `peer` held, waited on, sealed or read `job`: one more process its end is owed to, whose
    /// exit is watched when the supervisor drains.
    fn asked_about(self: &Arc<Self>, job: JobId, peer: io::Result<Process>) {
        let watch = {
            let mut drain = self.drain();
            match drain.as_mut() {
                Some(drain) => drain.reader(job, peer),
                None => {
                    if let Some(asked) = self.asked().get_mut(&job) {
                        asked.add(job, peer);
                    }
                    None
                }
            }
        };
        if let Some(watch) = watch {
            Arc::clone(self).watch(watch);
        }
    }

    /// Start owing every job this socket admitted that has not ended, each to the processes that
    /// asked for or about it, a resident's requester through `ends`: the exits to watch, or `None`
    /// when a drain already began. It begins from this record alone, before `drain` is answered,
    /// so a job that ends after the answer is owed (11_shell.md "Draining a supervisor of another
    /// build").
    fn begin(&self, ends: ResidentEnd) -> Option<Vec<Watch>> {
        let mut drain = self.drain();
        if drain.is_some() {
            return None;
        }
        let mut begun = Drain {
            running: Vec::new(),
            reading: false,
            owed: BTreeMap::new(),
            ended: Vec::new(),
            next: 0,
            ends,
        };
        let mut watches = Vec::new();
        for (
            job,
            Asked {
                class,
                requester,
                readers,
                unidentified,
            },
        ) in std::mem::take(&mut *self.asked())
        {
            begun.running.push(job);
            begun.owed.insert(
                job,
                Owed {
                    unseen: unidentified,
                    ..Owed::default()
                },
            );
            if let Some(requester) = requester {
                watches.push(begun.owe(job, requester, resident(class)));
            }
            for reader in readers {
                watches.push(begun.owe(job, reader, false));
            }
        }
        *drain = Some(begun);
        Some(watches)
    }

    /// The supervisor's jobs that ran when the drain listed them, after it began: one no process
    /// asked for or about, such as the Nx keeper's daemon, is owed to nobody, and the drain waits
    /// only for it to end.
    fn listed(&self, running: impl IntoIterator<Item = JobId>) {
        if let Some(drain) = self.drain().as_mut() {
            for job in running {
                if let std::collections::btree_map::Entry::Vacant(entry) = drain.owed.entry(job) {
                    entry.insert(Owed::default());
                    drain.running.push(job);
                }
            }
        }
    }

    /// Watch `watch`'s process to its exit, registered before this returns so no exit after it
    /// is missed. The job is no longer owed to the process then, and a resident it requested is
    /// ended first.
    fn watch(self: Arc<Self>, watch: Watch) {
        let Watch {
            job,
            number,
            process,
            resident,
        } = watch;
        let exit = process.exit();
        tokio::spawn(async move {
            let observed = match exit {
                Ok(exit) => exit.observed().await,
                Err(error) => Err(error),
            };
            let observed = match observed {
                Ok(observed) => observed,
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: a draining supervisor cannot watch pid {}, which asked about job \
                         {}: {error}; that job's end is owed to it until a read or the idle bound",
                        process.pid(),
                        job.get()
                    );
                    self.settle_reader(job, number, true);
                    return;
                }
            };
            if let Some(ResidentEnd { supervisor, report }) = resident {
                let gone = RequesterGone {
                    job,
                    requester: process.pid(),
                    observed,
                };
                match end_resident(&supervisor, &gone, &report).await {
                    Ok(true) => {
                        let mut drain = self.drain();
                        if let Some(drain) = drain.as_mut() {
                            drain.ended.push(gone);
                            report.publish(drain.waits());
                        }
                    }
                    Ok(false) => {}
                    Err(error) => report.stuck(&format!("end {gone}"), &error),
                }
            }
            self.settle_reader(job, number, false);
        });
    }

    /// The process watched as `number` will read nothing of `job` any more: it exited, or its
    /// exit cannot be watched (`unseen`), so it is never seen gone.
    fn settle_reader(&self, job: JobId, number: u64, unseen: bool) {
        if let Some(owed) = self
            .drain()
            .as_mut()
            .and_then(|drain| drain.owed.get_mut(&job))
        {
            owed.readers.remove(&number);
            owed.unseen |= unseen;
        }
        self.gone.notify_one();
    }

    /// Say in the report what the drain waits for now.
    fn publish(&self, report: &DrainReport) {
        if let Some(drain) = self.drain().as_ref() {
            report.publish(drain.waits());
        }
    }

    /// Every job it waited for has ended: it waits for their reads now.
    fn reading(&self, report: &DrainReport) {
        let mut drain = self.drain();
        if let Some(drain) = drain.as_mut() {
            drain.reading = true;
            report.publish(drain.waits());
        }
    }

    /// Record what a served call told its client about `job`, while draining: a read of an owed
    /// job starts the idle bound over, and nothing else does.
    fn record(&self, job: JobId, update: impl FnOnce(&mut Owed)) {
        let owed = match self
            .drain()
            .as_mut()
            .and_then(|drain| drain.owed.get_mut(&job))
        {
            Some(owed) => {
                update(owed);
                true
            }
            None => false,
        };
        if owed {
            self.read.notify_one();
        }
    }

    /// Whether the drain may settle now: no exec is in flight, and every owed job is read or
    /// owed to nobody, or the idle bound `expired`. An exec in flight holds it on either path:
    /// admission is closed, so its answer, a refusal or a recorded requester, comes at once.
    fn may_settle(&self, expired: bool) -> bool {
        self.admitting.load(std::sync::atomic::Ordering::Acquire) == 0
            && (expired
                || self
                    .drain()
                    .as_ref()
                    .is_none_or(|drain| drain.owed.values().all(Owed::settled)))
    }

    /// Wait until every owed job is read or every process it was owed to has exited, or until
    /// `bound` passes with no read of an owed job; on either path, until no exec is in flight.
    async fn settle(&self, bound: std::time::Duration) -> Settled {
        let mut deadline = tokio::time::Instant::now() + bound;
        let mut expired = false;
        while !self.may_settle(expired) {
            tokio::select! {
                () = self.read.notified() => {
                    deadline = tokio::time::Instant::now() + bound;
                    expired = false;
                }
                () = self.gone.notified() => {}
                () = tokio::time::sleep_until(deadline), if !expired => expired = true,
            }
        }
        let drain = self.drain();
        let mut settled = Settled {
            read: Vec::new(),
            abandoned: Vec::new(),
            unread: Vec::new(),
            ended: Vec::new(),
            bound,
        };
        if let Some(drain) = drain.as_ref() {
            for (job, owed) in &drain.owed {
                if owed.read() {
                    settled.read.push(*job);
                } else if owed.abandoned() {
                    settled.abandoned.push(*job);
                } else {
                    settled.unread.push(*job);
                }
            }
            settled.ended.clone_from(&drain.ended);
        }
        settled
    }
}

/// End the resident `gone` names, whose requester exited while the supervisor drains, under the
/// authority the supervisor holds now, which an advance may have moved since the drain began:
/// `false` when it had already ended.
async fn end_resident(
    supervisor: &WorkspaceSupervisorHandle,
    gone: &RequesterGone,
    report: &DrainReport,
) -> Result<bool> {
    let supervisor = supervisor.with_authority(supervisor.current_authority().await?);
    if supervisor.info(gone.job).await?.state.is_terminal() {
        return Ok(false);
    }
    cowshed_os::eprintln!(
        "cowshed: a draining supervisor ({}) ends {gone}",
        report.path.display()
    );
    supervisor.kill(gone.job).await?;
    Ok(true)
}

async fn serve_membership_call(
    mut stream: UnixStream,
    supervisor: &WorkspaceSupervisorHandle,
) -> Result<Retirement> {
    verify_peer(&stream)?;
    let request = match read_json::<MembershipRequest>(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            // Unknown operations are named in the serde refusal, not ignored or forwarded.
            write_json(&mut stream, &Response::Err(error)).await?;
            return Ok(Retirement::Serving);
        }
    };
    let (reader, mut writer) = stream.split();
    let outcome = heartbeating(
        &mut writer,
        || pulse(supervisor, None),
        membership_request(reader.as_ref(), supervisor, request),
    )
    .await;
    let response = match outcome {
        Ok(value) => Response::Ok { value, bytes: 0 },
        Err(error) => Response::Err(error),
    };
    write_json(&mut writer, &response).await?;
    Ok(Retirement::Serving)
}

async fn membership_request(
    stream: &UnixStream,
    supervisor: &WorkspaceSupervisorHandle,
    request: MembershipRequest,
) -> Result<serde_json::Value> {
    #[cfg(target_os = "macos")]
    {
        let peer = super::job_groups::Process::of_socket_peer(stream).map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot authenticate membership socket peer: {error}"),
                "request a subtree capability while its process is alive",
            )
        })?;
        match request {
            MembershipRequest::SubtreeHandle => to_value(&supervisor.subtree_handle(peer).await?),
            MembershipRequest::RetireDescendants { handle, grace_ms } => to_value(
                &supervisor
                    .retire_descendants(peer, handle, std::time::Duration::from_millis(grace_ms))
                    .await?,
            ),
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (stream, supervisor);
        match request {
            MembershipRequest::SubtreeHandle => to_value(&JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::FoldUnavailable,
            }),
            MembershipRequest::RetireDescendants { .. } => {
                to_value(&JobSubtreeRetirement::Refused {
                    reason: JobSubtreeRefusal::FoldUnavailable,
                })
            }
        }
    }
}

/// Ask about this connection's own live process, never a pid supplied by the caller.
pub async fn subtree_handle(path: &Path) -> Result<JobSubtreeAdmission> {
    let (value, _) = exchange(
        path,
        "subtree handle",
        &MembershipRequest::SubtreeHandle,
        Bytes::new(),
    )
    .await?;
    decode(value)
}

/// Spend a root-issued capability from a live peer of the same admitted job.
pub async fn retire_descendants(
    path: &Path,
    handle: JobSubtreeHandle,
    grace: std::time::Duration,
) -> Result<JobSubtreeRetirement> {
    let grace_ms = u64::try_from(grace.as_millis()).map_err(|_| {
        CowshedError::usage(
            "subtree TERM grace exceeds the wire range",
            "use a grace in milliseconds representable by u64",
        )
    })?;
    let (value, _) = exchange(
        path,
        "retire descendants",
        &MembershipRequest::RetireDescendants { handle, grace_ms },
        Bytes::new(),
    )
    .await?;
    decode(value)
}
async fn serve_call(
    mut stream: UnixStream,
    supervisor: &WorkspaceSupervisorHandle,
    streams: &Streams,
    advances: Option<&Advances>,
    feed: Option<&super::commitment_feed::CommitmentFeed>,
    owed: &Arc<DrainReads>,
    report: &DrainReport,
) -> Result<Retirement> {
    verify_peer(&stream)?;
    let request: Request = read_json(&mut stream).await?;
    let (authority, call, length) = match request {
        Request::Hello => {
            // Read from what the actor published, never through it: a turn waiting on the
            // blocking pool must not hold a hello past its handshake bound.
            let liveness = supervisor.liveness()?;
            let value = to_value(&HelloWire {
                build: BuildId::current()?.clone(),
                authority: liveness.authority.as_ref().into(),
                pid: std::process::id(),
            })?;
            write_json(&mut stream, &Response::Ok { value, bytes: 0 }).await?;
            return Ok(Retirement::Serving);
        }
        Request::Drain => {
            let value = to_value(&std::process::id())?;
            // The drain begins before it is answered, so a job that ran when it was asked is owed
            // even if it ends right after the answer (11_shell.md "Draining a supervisor of
            // another build"). A second `drain` finds the first one under way.
            let Some(watches) = owed.begin(ResidentEnd {
                supervisor: supervisor.clone(),
                report: report.clone(),
            }) else {
                write_json(&mut stream, &Response::Ok { value, bytes: 0 }).await?;
                return Ok(Retirement::Serving);
            };
            for watch in watches {
                Arc::clone(owed).watch(watch);
            }
            owed.publish(report);
            // Begun, it drains whether or not its asker hears the answer.
            if let Err(error) = write_json(&mut stream, &Response::Ok { value, bytes: 0 }).await {
                cowshed_os::eprintln!(
                    "cowshed: a draining supervisor ({}) could not answer its drain: {error}",
                    report.path.display()
                );
            }
            return Ok(Retirement::Draining);
        }
        Request::Commitments { after } => {
            let response = match feed {
                Some(feed) => Response::Ok {
                    value: to_value(
                        &heartbeating(
                            &mut stream,
                            || pulse(supervisor, None),
                            feed.since(after, COMMITMENT_WAIT),
                        )
                        .await,
                    )?,
                    bytes: 0,
                },
                None => Response::Err(CowshedError::conflict(
                    "this supervisor keeps no commitments for forwarding",
                    "read the host's own audit segments",
                )),
            };
            write_json(&mut stream, &response).await?;
            return Ok(Retirement::Serving);
        }
        Request::AcknowledgeCommitments { through } => {
            if let Some(feed) = feed {
                feed.acknowledge(through);
            }
            write_json(
                &mut stream,
                &Response::Ok {
                    value: serde_json::Value::Null,
                    bytes: 0,
                },
            )
            .await?;
            return Ok(Retirement::Serving);
        }
        Request::Advance => {
            let advanced = match advances {
                Some(advances) => {
                    let (reply, answer) = oneshot::channel();
                    match advances.send(reply).await {
                        Ok(()) => heartbeating(&mut stream, || pulse(supervisor, None), answer)
                            .await
                            .unwrap_or_else(|_| {
                                Err(CowshedError::internal(
                                    "the supervisor process stopped before advancing",
                                ))
                            }),
                        Err(_) => Err(CowshedError::internal(
                            "the supervisor process no longer takes advances",
                        )),
                    }
                }
                None => Err(CowshedError::conflict(
                    "this supervisor cannot re-read its grants",
                    "retire it; the next command starts one under the current grants",
                )),
            };
            let response = match advanced {
                Ok(authority) => Response::Ok {
                    value: to_value(&AuthorityWire::from(&authority))?,
                    bytes: 0,
                },
                Err(error) => Response::Err(error),
            };
            write_json(&mut stream, &response).await?;
            return Ok(Retirement::Serving);
        }
        Request::Call {
            authority,
            call,
            bytes,
        } => (authority, call, bytes),
    };
    let payload = read_bytes(&mut stream, length).await?;
    let caller = supervisor.with_authority(authority.into());
    // A process that asks for or about a job is one its end is owed to should the supervisor
    // drain (11_shell.md "Draining a supervisor of another build").
    if let Call::Hold { job_id }
    | Call::Wait { job_id }
    | Call::Sealed { job_id }
    | Call::LogRead { job_id, .. } = &*call
    {
        owed.asked_about(*job_id, Process::of_socket_peer(&stream));
    }
    let requester = match &*call {
        Call::Exec { request, .. } => Some((request.class, Process::of_socket_peer(&stream))),
        _ => None,
    };
    if let Call::ProcessEvents { job_id, every } = *call {
        serve_process_events(stream, &caller, job_id, every).await?;
        return Ok(Retirement::Serving);
    }
    if let Call::Hold { job_id } = *call {
        serve_hold(stream, supervisor, &caller, job_id, owed).await?;
        return Ok(Retirement::Serving);
    }
    // What this call can tell a draining supervisor's client about its job's end: `None` for
    // the end itself, else the stream (stdout 0, stderr 1) a log read advanced.
    let reads = match &*call {
        Call::Wait { job_id } | Call::Sealed { job_id } => Some((*job_id, None)),
        Call::LogRead { job_id, stream, .. } => Some((
            *job_id,
            Some(match stream {
                StreamWire::Stdout => 0,
                StreamWire::Stderr => 1,
            }),
        )),
        _ => None,
    };
    let retiring = matches!(*call, Call::Retire { .. });
    // Held across the admission and its bookkeeping, so no drain settles between them.
    let admitting = requester.as_ref().map(|_| owed.admitting());
    let Answer {
        value,
        bytes,
        admitted,
    } = match heartbeating(
        &mut stream,
        || pulse(supervisor, reads.map(|(job, _)| job)),
        answer(&caller, streams, *call, payload),
    )
    .await
    {
        Ok(answer) => answer,
        Err(error) => {
            // A refused exec admitted nothing: the drain need not wait for its caller to read
            // the refusal.
            drop(admitting);
            write_json(&mut stream, &Response::Err(error)).await?;
            return Ok(Retirement::Serving);
        }
    };
    if let (Some(job), Some((class, requester))) = (admitted, requester) {
        owed.admitted(job, class, requester, &caller);
    }
    drop(admitting);
    if retiring {
        // The serving loop releases the listener and lease before acknowledging retirement.
        return Ok(Retirement::Retired {
            stream,
            value,
            bytes,
        });
    }
    let eof = value
        .get("eof")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let answered = async {
        write_json(
            &mut stream,
            &Response::Ok {
                value,
                bytes: bytes.len(),
            },
        )
        .await?;
        if !bytes.is_empty() {
            write_frame(&mut stream, &bytes, MAX_BYTES_FRAME).await?;
        }
        Ok::<(), CowshedError>(())
    }
    .await;
    if answered.is_ok()
        && let Some((job, stream)) = reads
    {
        owed.record(job, |owed| match stream {
            None => owed.end = true,
            Some(stream) => {
                owed.reading[stream] = true;
                owed.eof[stream] |= eof;
            }
        });
    }
    answered.map(|()| Retirement::Serving)
}

async fn serve_process_events(
    mut stream: UnixStream,
    supervisor: &WorkspaceSupervisorHandle,
    job_id: JobId,
    every: SampleInterval,
) -> Result<()> {
    let events = match supervisor.process_events(job_id, every).await {
        Ok(events) => events,
        Err(error) => return write_json(&mut stream, &Response::Err(error)).await,
    };
    write_json(
        &mut stream,
        &Response::Ok {
            value: serde_json::Value::Null,
            bytes: 0,
        },
    )
    .await?;
    relay_process_events(stream, events, || pulse(supervisor, Some(job_id))).await
}

/// Hold `job_id` for its holder: answer `null` once the hold is in place, then the job's
/// terminal `JobInfo` when it ends, or `null` again when the holder releases it. A holder that
/// closes the connection first — it exited, or was killed, `SIGKILL` included, since the kernel
/// closes its socket — has left a job nobody waits for, and the job is killed
/// (05_gateway.md, "A job lives with its requester"). A refused retirement ends neither: the job
/// keeps running with its claims, and the holder's own waits carry the refusal, so the hold stays
/// until it is released or closed (11_shell.md, "Shell activation and process reuse").
async fn serve_hold(
    mut stream: UnixStream,
    supervisor: &WorkspaceSupervisorHandle,
    caller: &WorkspaceSupervisorHandle,
    job_id: JobId,
    owed: &DrainReads,
) -> Result<()> {
    if let Err(error) = caller.info(job_id).await {
        return write_json(&mut stream, &Response::Err(error)).await;
    }
    write_json(
        &mut stream,
        &Response::Ok {
            value: serde_json::Value::Null,
            bytes: 0,
        },
    )
    .await?;
    let (mut reader, mut writer) = stream.into_split();
    // One read of the holder's demand outlives a refused wait, so a frame it began stays whole.
    let read = read_json::<HoldDemand>(&mut reader);
    tokio::pin!(read);
    let demand = tokio::select! {
        biased;
        ended = caller.wait(job_id) => match ended {
            Ok(info) => {
                let written = write_json(
                    &mut writer,
                    &Response::Ok { value: to_value(&info)?, bytes: 0 },
                )
                .await;
                if written.is_ok() {
                    owed.record(job_id, |owed| owed.end = true);
                }
                return written;
            }
            // A refused retirement leaves the job running and held; its waiters have the error.
            // An ended job's error answers the hold, as its record would.
            Err(error) => match caller.info(job_id).await {
                Ok(info) if !info.state.is_terminal() => None,
                _ => return write_json(&mut writer, &Response::Err(error)).await,
            },
        },
        demand = &mut read => Some(demand),
    };
    let demand = match demand {
        Some(demand) => demand,
        None => read.await,
    };
    match demand {
        Ok(HoldDemand::Release) => {
            write_json(
                &mut writer,
                &Response::Ok {
                    value: serde_json::Value::Null,
                    bytes: 0,
                },
            )
            .await
        }
        Err(_) => kill_unheld(supervisor, job_id).await,
    }
}

/// Kill `job_id` because its holder went away while it ran. The job may have been advanced onto
/// a newer grant revision meanwhile, so it is reached under the authority served now.
pub(super) async fn kill_unheld(
    supervisor: &WorkspaceSupervisorHandle,
    job_id: JobId,
) -> Result<()> {
    let served = supervisor.current_authority().await?;
    let expected = &supervisor.snapshot().workspace_incarnation;
    if served.workspace_incarnation != *expected {
        return Err(CowshedError::conflict(
            format!(
                "job {} belongs to workspace incarnation {expected}, but the supervisor now serves {}; refusing cancellation",
                job_id.get(),
                served.workspace_incarnation
            ),
            "inspect the original workspace supervisor",
        ));
    }
    let current = supervisor.with_authority(served);
    if current.info(job_id).await?.state.is_terminal() {
        return Ok(());
    }
    cowshed_os::eprintln!(
        "cowshed: job {} lost its holder before it ended; killing it, since nothing waits for it",
        job_id.get()
    );
    current.kill(job_id).await
}

/// Relay `events` one per demand. While a demand waits for its event, `probe` heartbeats the
/// stream, so a subscriber can tell a job with nothing to report from a supervisor that stopped.
async fn relay_process_events<P>(
    stream: UnixStream,
    mut events: JobProcessStream,
    probe: P,
) -> Result<()>
where
    P: FnMut() -> Result<Pulse>,
{
    let mut heartbeat = Heartbeat::new(probe);
    let (mut reader, mut writer) = stream.into_split();
    let (send, mut demands) = mpsc::channel(1);
    // Read close even while the source has no event. A complete frame is read by
    // one task; cancelling a partial frame never changes the next frame's boundary.
    let reader = tokio::spawn(async move {
        loop {
            let demand = read_json::<Demand>(&mut reader).await;
            let failed = demand.is_err();
            if send.send(demand).await.is_err() || failed {
                break;
            }
        }
    });
    let result = async {
        loop {
            match demands.recv().await {
                Some(Ok(Demand::Next)) => {}
                Some(Ok(Demand::Close)) => {
                    return write_process_event(&mut writer, None).await;
                }
                Some(Err(error)) => return Err(error),
                None => return Ok(()),
            }
            let event = loop {
                tokio::select! {
                    biased;
                    demand = demands.recv() => match demand {
                        Some(Ok(Demand::Close)) => {
                            return write_process_event(&mut writer, None).await;
                        }
                        Some(Ok(Demand::Next)) => {
                            return Err(protocol_error("process event demanded while one is unanswered"));
                        }
                        Some(Err(error)) => return Err(error),
                        None => return Ok(()),
                    },
                    event = events.next() => break event,
                    () = heartbeat.due() => heartbeat.beat(&mut writer).await?,
                }
            };
            match event {
                Some(Ok(event)) => write_process_event(&mut writer, Some(event)).await?,
                Some(Err(error)) => return write_json(&mut writer, &Response::Err(error)).await,
                None => return write_process_event(&mut writer, None).await,
            }
        }
    }
    .await;
    reader.abort();
    match reader.await {
        Ok(()) => result,
        Err(error) if error.is_cancelled() => result,
        Err(error) => Err(CowshedError::internal(format!(
            "process demand reader failed: {error}; stream result: {result:?}"
        ))),
    }
}

async fn write_process_event(
    writer: &mut (impl AsyncWrite + Unpin),
    event: Option<JobProcessEvent>,
) -> Result<()> {
    write_json(
        writer,
        &Response::Ok {
            value: to_value(&event)?,
            bytes: 0,
        },
    )
    .await
}

pub(super) fn verify_peer(stream: &UnixStream) -> Result<()> {
    use std::os::fd::AsFd as _;
    let descriptor = stream
        .as_fd()
        .try_clone_to_owned()
        .map_err(|error| protocol_error(format!("cannot inspect the peer: {error}")))?;
    crate::api::frame::verify_peer(&descriptor, |error| {
        CowshedError::sandbox_denied(
            format!("workspace supervisor refused a peer: {error:?}"),
            "connect as the user that owns the workspace",
        )
    })
}

fn to_value<T: Serialize>(value: &T) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|error| protocol_error(format!("encoding failed: {error}")))
}

/// What a call answers, and the job it admitted when it was an `exec`.
struct Answer {
    value: serde_json::Value,
    bytes: Bytes,
    admitted: Option<JobId>,
}

async fn answer(
    supervisor: &WorkspaceSupervisorHandle,
    streams: &Streams,
    call: Call,
    payload: Bytes,
) -> Result<Answer> {
    let session =
        |wire: SessionWire| SessionToken::remote(supervisor.snapshot(), wire.identity, wire.name);
    let unit = || to_value(&());
    let mut admitted = None;
    let (value, bytes) = match call {
        Call::OpenSession { name } => {
            let token = supervisor.open_session(name).await?;
            (to_value(&SessionWire::from(&token))?, Bytes::new())
        }
        Call::SessionSnapshot { session: wire } => {
            let snapshot = supervisor.session_snapshot(&session(wire)).await?;
            (
                to_value(&SessionSnapshotWire {
                    identity: snapshot.identity,
                    name: snapshot.name,
                    cwd: snapshot.cwd,
                    env: snapshot.env,
                    background_jobs: snapshot.background_jobs,
                })?,
                Bytes::new(),
            )
        }
        Call::CloseSession { session: wire } => {
            supervisor.close_session(session(wire)).await?;
            (unit()?, Bytes::new())
        }
        Call::Exec {
            session: wire,
            build_volume,
            source,
            request,
        } => {
            let request = *request;
            let command =
                ExecCommand::from_fields(request.argv, request.script).map_err(|error| {
                    CowshedError::usage(error.to_string(), "provide a valid bounded command")
                })?;
            let mut writer = None;
            let stdin = match request.stdin {
                StdinWire::Empty => StdinSource::Empty,
                StdinWire::Inline => StdinSource::Inline(payload),
                StdinWire::WorkspaceFile { workspace_path } => {
                    StdinSource::WorkspaceFile(workspace_path)
                }
                StdinWire::Stream => {
                    let (sender, receiver) = mpsc::channel(STREAM_DEPTH);
                    writer = Some(sender);
                    StdinSource::Stream(Box::pin(ChannelReader::new(receiver)))
                }
                StdinWire::Open => StdinSource::Open,
            };
            let exec = ExecRequest {
                command,
                cwd: request.cwd,
                mode: request.mode,
                env: request.env,
                trace: request.trace,
                stdin,
                stdout_copy: request.stdout_copy,
                stderr_copy: request.stderr_copy,
                admission_key: request.admission_key,
                class: request.class,
                want: request.want,
                scope: request.scope,
            };
            let session = wire.map(session);
            let job_id = supervisor
                .exec_from(
                    session.as_ref(),
                    build_volume,
                    exec,
                    source,
                    request.parent_jobserver,
                )
                .await?;
            if let Some(writer) = writer {
                lock(streams).insert(job_id, writer);
            }
            admitted = Some(job_id);
            (to_value(&job_id)?, Bytes::new())
        }
        Call::StdinWrite { job_id, offset } => {
            supervisor.stdin_write(job_id, offset, payload).await?;
            (unit()?, Bytes::new())
        }
        Call::StdinClose { job_id } => {
            supervisor.stdin_close(job_id).await?;
            (unit()?, Bytes::new())
        }
        Call::StreamChunk { job_id } => {
            let writer = lock(streams).get(&job_id).cloned().ok_or_else(|| {
                CowshedError::conflict(
                    format!("job {} has no open stdin stream", job_id.get()),
                    "inspect the job status",
                )
            })?;
            // The job's reader applies backpressure through the bounded channel; a job that
            // stopped reading closed its receiver, which ends the stream here.
            writer.send(Ok(payload)).await.map_err(|_| {
                lock(streams).remove(&job_id);
                CowshedError::conflict(
                    format!("job {} stopped reading its stdin", job_id.get()),
                    "inspect the job status",
                )
            })?;
            (unit()?, Bytes::new())
        }
        Call::StreamEnd { job_id, error } => {
            let writer = lock(streams).remove(&job_id);
            if let (Some(writer), Some(error)) = (writer, error) {
                let _ = writer.send(Err(io::Error::other(error))).await;
            }
            (unit()?, Bytes::new())
        }
        Call::Info { job_id } => (to_value(&supervisor.info(job_id).await?)?, Bytes::new()),
        Call::Sealed { job_id } => (to_value(&supervisor.sealed(job_id).await?)?, Bytes::new()),
        Call::Resources { job_id } => (
            to_value(&supervisor.resources(job_id).await?)?,
            Bytes::new(),
        ),
        Call::Processes { job_id } => (
            to_value(&supervisor.processes(job_id).await?)?,
            Bytes::new(),
        ),
        Call::ProcessEvents { .. } => {
            return Err(protocol_error("process events require their demand stream"));
        }
        Call::Hold { .. } => {
            return Err(protocol_error("a hold requires its connection"));
        }
        Call::AsIs { build_volume } => (
            to_value(&supervisor.as_is(build_volume).await?)?,
            Bytes::new(),
        ),
        Call::ShellPool => (to_value(&supervisor.shell_pool().await?)?, Bytes::new()),
        Call::TraceHealth => (to_value(&supervisor.trace_health().await?)?, Bytes::new()),
        Call::ListeningPorts { job_id } => (
            to_value(&supervisor.listening_ports(job_id).await?)?,
            Bytes::new(),
        ),
        Call::JobByKey { key } => (to_value(&supervisor.job_by_key(key).await?)?, Bytes::new()),
        Call::Progress { job_id } => (
            to_value(&supervisor.read_progress(job_id).await?)?,
            Bytes::new(),
        ),
        Call::List => (to_value(&supervisor.list().await?)?, Bytes::new()),
        Call::Kill { job_id } => {
            supervisor.kill(job_id).await?;
            (unit()?, Bytes::new())
        }
        Call::Detach { job_id } => {
            supervisor.detach(job_id).await?;
            (unit()?, Bytes::new())
        }
        Call::Wait { job_id } => (to_value(&supervisor.wait(job_id).await?)?, Bytes::new()),
        Call::LogRead {
            job_id,
            stream,
            offset,
            follow,
        } => {
            let chunk = supervisor
                .log_read(job_id, stream.into(), offset, follow)
                .await?;
            (
                to_value(&LogChunkWire {
                    next_offset: chunk.next_offset,
                    eof: chunk.eof,
                })?,
                chunk.bytes,
            )
        }
        Call::Tail {
            job_id,
            cursor,
            limits,
        } => (
            to_value(&supervisor.tail(job_id, cursor, limits).await?)?,
            Bytes::new(),
        ),
        Call::Retire { nx_daemon, when } => {
            match when {
                RetireWhen::Now => supervisor.retire(nx_daemon).await?,
                RetireWhen::AfterJobs => supervisor.retire_after_jobs(nx_daemon).await?,
                RetireWhen::IfIdle => supervisor.retire_if_idle(nx_daemon).await?,

                RetireWhen::IfUnowned => supervisor.retire_if_unowned(nx_daemon).await?,
            }
            (unit()?, Bytes::new())
        }
        Call::NameBuildVolume { build_volume } => {
            supervisor.name_build_volume(build_volume).await?;
            (unit()?, Bytes::new())
        }
        Call::StopNxKeeper => {
            supervisor.stop_nx_keeper().await?;
            (unit()?, Bytes::new())
        }
    };
    Ok(Answer {
        value,
        bytes,
        admitted,
    })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // A poisoned map only means another call panicked mid-insert; the map itself is whole.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The job side of a forwarded stdin stream: bytes as the client sends them, an error when the
/// client's source failed, and end of file when it ended cleanly.
struct ChannelReader {
    receiver: mpsc::Receiver<io::Result<Bytes>>,
    pending: Bytes,
}

impl ChannelReader {
    fn new(receiver: mpsc::Receiver<io::Result<Bytes>>) -> Self {
        Self {
            receiver,
            pending: Bytes::new(),
        }
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        while self.pending.is_empty() {
            match self.receiver.poll_recv(context) {
                Poll::Ready(Some(Ok(bytes))) => self.pending = bytes,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
        let count = self.pending.len().min(buffer.remaining());
        let chunk = self.pending.split_to(count);
        buffer.put_slice(&chunk);
        Poll::Ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------------------------

/// What a supervisor reports about itself when a client first reaches it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hello {
    pub authority: WorkspaceAuthoritySnapshot,
    /// The process serving the socket.
    pub pid: u32,
}

/// Who serves `path`, or why it cannot be used: unreachable, or of another cowshed build.
pub async fn hello(path: &Path) -> Result<Hello> {
    hello_if_present(path).await?.ok_or_else(|| {
        unavailable(
            path,
            &io::Error::new(
                io::ErrorKind::NotConnected,
                "no supervisor serves this socket",
            ),
        )
    })
}

/// Absence is only no socket file, or one no socket is attached to any more (see `occupant`):
/// found by a refused connection, or after a connection the peer ended unanswered -- a supervisor
/// exiting closes its listener with a hello in its queue. A refusal or an ended exchange while a
/// socket is still attached -- a full queue, or a holder that does not accept or answer -- a live
/// peer's unknown response, malformed protocol, or bounded hello timeout is an error, never proof
/// it stopped.
pub async fn hello_if_present(path: &Path) -> Result<Option<Hello>> {
    let connected = PeerStream::connect(
        SUPERVISOR,
        path,
        deadline::start("hello", deadline::HANDSHAKE),
    )
    .await;
    let mut stream = match connected {
        Ok(stream) => stream,
        Err(failure) => {
            return match refused(path, failure) {
                Refused::Absent(_) => Ok(None),
                Refused::Live(error) => Err(error),
                Refused::Failed(failure) => Err(waited(failure)),
            };
        }
    };
    // Hello is answered at once, so the whole exchange stays inside the handshake bound.
    let answered = match send(&mut stream, &Request::Hello, &[]).await {
        Ok(()) => answer_of(&mut stream).await,
        Err(error) => Err(error),
    };
    let (value, _) = match answered {
        Ok(answer) => answer,
        Err(error) => {
            return match occupant(path) {
                Ok(Occupant::Absent | Occupant::Stopped) => Ok(None),
                Ok(Occupant::Live) | Err(_) => Err(error),
            };
        }
    };
    decode_hello(path, value).map(Some)
}

fn decode_hello(path: &Path, value: serde_json::Value) -> Result<Hello> {
    // Read the build before the rest: another build's hello may carry fields this one cannot
    // decode, and the build is what says why. A hello naming no build is another build's too.
    let ours = BuildId::current()?;
    let theirs = value.get("build").and_then(serde_json::Value::as_str);
    if theirs != Some(ours.as_str()) {
        let pid = value.get("pid").and_then(serde_json::Value::as_u64);
        let (serving, hint) = match pid {
            Some(pid) => (
                match DrainReport::read(path, pid) {
                    Some(waits) => format!(" (pid {pid}; {waits})"),
                    None => format!(" (pid {pid})"),
                },
                format!(
                    "it admits nothing more and retires once its running jobs end, and a land step \
                     waits for that; to end its jobs now, `kill {pid}`: they end when it exits, \
                     and the next command starts a supervisor of this build"
                ),
            ),
            None => (
                String::new(),
                "it admits nothing more and retires once its running jobs end; \
                 `cowshed doctor --json` names its process"
                    .to_owned(),
            ),
        };
        return Err(CowshedError::conflict(
            format!(
                "the workspace supervisor at {}{serving} is cowshed build {}; this cowshed is build {ours}",
                path.display(),
                theirs.unwrap_or("(unnamed)")
            ),
            hint,
        ));
    }
    let hello: HelloWire = decode(value)?;
    Ok(Hello {
        authority: hello.authority.into(),
        pid: hello.pid,
    })
}

/// The commitments the supervisor at `path` recorded after cursor `after`; waits a while for
/// one when there is none, then answers empty.
pub async fn commitments(path: &Path, after: u64) -> Result<super::commitment_feed::FeedPage> {
    let (value, _) = exchange(
        path,
        "commitments",
        &Request::Commitments { after },
        Bytes::new(),
    )
    .await?;
    decode(value)
}

/// Tell the supervisor at `path` that every commitment through cursor `through` is forwarded.
pub async fn acknowledge_commitments(path: &Path, through: u64) -> Result<()> {
    exchange(
        path,
        "acknowledge commitments",
        &Request::AcknowledgeCommitments { through },
        Bytes::new(),
    )
    .await
    .map(|_| ())
}

/// The supervisor a drain reached: the process the connection's peer credential names, which is
/// the identity any later signal to it must go through -- whatever happens to its pid, the handle
/// names only that process.
pub struct Drained {
    pub process: super::job_groups::Process,
    /// Its exit, watched from before it was asked to drain: a supervisor with no running job
    /// retires at once, and an exit that began before a watch could not be awaited to its end.
    pub exit: cowshed_os::process::ExitWatch,
}

impl Drained {
    pub fn pid(&self) -> u32 {
        self.process.pid().unsigned_abs()
    }
}

/// Ask the supervisor at `path` to admit nothing more and retire once its running jobs end,
/// whatever protocol it speaks otherwise; the process serving it.
pub async fn drain(path: &Path) -> Result<Drained> {
    let mut stream = open(path, "drain", deadline::CONTROL).await?;
    let process =
        super::job_groups::Process::of_socket_peer(stream.get_ref()).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot identify the supervisor at {}: {error}",
                    path.display()
                ),
                "use a host with kernel peer identities; retain the supervisor and its ledger",
            )
        })?;
    let exit = process.watch_exit().map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot watch the supervisor at {} to its exit: {error}",
                path.display()
            ),
            "retry once it has exited; its jobs and ledger are retained",
        )
    })?;
    // A drain is answered at once, so the whole exchange stays inside the control bound.
    send(&mut stream, &Request::Drain, &[]).await?;
    let (value, _) = answer_of(&mut stream).await?;
    let answered: u32 = decode(value)?;
    if i32::try_from(answered).ok() != Some(process.pid()) {
        return Err(protocol_error(format!(
            "the supervisor at {} answered as pid {answered}, but the process serving the \
             connection is pid {}",
            path.display(),
            process.pid()
        )));
    }
    Ok(Drained { process, exit })
}

/// The longest a land step waits for supervisors of another build to drain and retire
/// ([`await_other_build_retired`]). No bound on their jobs, which run on past it: the step then
/// refuses naming the supervisor, so whoever runs the land learns which job holds it.
pub const OTHER_BUILD_DRAIN_CEILING: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// When a supervisor of another cowshed build serves `path`, wait until `deadline` for it to
/// retire (11_shell.md, "Draining a supervisor of another build"): ask it to drain, which it
/// already does when this build's daemon recovered it, say on stderr which process and build this
/// waits for, and wait on the exit watch [`drain`] registered. Past `deadline` it refuses, naming
/// the process; it never ends one. Answers at once when nothing of another build serves `path`,
/// and leaves whatever else is wrong there to the command's own `hello`.
pub async fn await_other_build_retired(path: &Path, deadline: std::time::Instant) -> Result<()> {
    match hello_if_present(path).await {
        // Only another build's hello is a conflict (`decode_hello`).
        Err(refusal) if refusal.code == crate::error::ErrorCode::Conflict => {}
        _ => return Ok(()),
    }
    let drained = drain(path).await?;
    let pid = drained.pid();
    let started = std::time::Instant::now();
    cowshed_os::eprintln!(
        "cowshed: the workspace supervisor at {} (pid {pid}) is of another cowshed build and \
     drains its running jobs; waiting for it to retire, for at most {}s",
        path.display(),
        deadline.saturating_duration_since(started).as_secs()
    );
    let exit = drained.exit;
    let ceiling = deadline.saturating_duration_since(started);
    let retired = tokio::task::spawn_blocking(move || exit.within(ceiling))
        .await
        .map_err(|error| {
            CowshedError::internal(format!("waiting for supervisor pid {pid} failed: {error}"))
        })?
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot wait for the workspace supervisor at {} (pid {pid}) to exit: {error}",
                    path.display()
                ),
                "retry once it has exited; its jobs and ledger are retained",
            )
        })?;
    if !retired {
        return Err(CowshedError::conflict(
            format!(
                "the workspace supervisor at {} (pid {pid}) of another cowshed build still runs \
                 jobs after {}s of waiting for it to retire",
                path.display(),
                started.elapsed().as_secs()
            ),
            format!(
                "wait for its jobs to end, or end them now with `kill {pid}`: its jobs end when \
                 it exits, and the next command starts a supervisor of this build"
            ),
        ));
    }
    cowshed_os::eprintln!(
        "cowshed: the workspace supervisor at {} (pid {pid}) retired after {}s",
        path.display(),
        started.elapsed().as_secs()
    );
    Ok(())
}

/// Ask the supervisor at `path` to serve under its workspace's current grants; the authority
/// it serves afterwards.
pub async fn advance(path: &Path) -> Result<WorkspaceAuthoritySnapshot> {
    let (value, _) = exchange(path, "advance", &Request::Advance, Bytes::new()).await?;
    let authority: AuthorityWire = decode(value)?;
    Ok(authority.into())
}

/// A handle whose calls reach the supervisor serving `path` under `authority`.
pub fn connect(path: PathBuf, authority: WorkspaceAuthoritySnapshot) -> WorkspaceSupervisorHandle {
    let (commands, mut receiver) = mpsc::channel(64);
    let path = Arc::new(path);
    tokio::spawn(async move {
        while let Some(command) = receiver.recv().await {
            tokio::spawn(forward(Arc::clone(&path), command));
        }
    });
    WorkspaceSupervisorHandle::from_parts(authority, commands)
}

async fn round_trip(
    path: &Path,
    authority: AuthorityWire,
    call: Call,
    payload: Bytes,
) -> Result<(serde_json::Value, Bytes)> {
    let op = call.op();
    let request = Request::Call {
        authority,
        call: Box::new(call),
        bytes: payload.len(),
    };
    exchange(path, op, &request, payload).await
}

/// One call to the supervisor at `path` (05_gateway.md, "Deadlines"): connected and sent within
/// one control bound, then answered while the supervisor heartbeats, however long the answer
/// takes to be ready.
async fn exchange(
    path: &Path,
    op: &'static str,
    request: &impl Serialize,
    payload: Bytes,
) -> Result<(serde_json::Value, Bytes)> {
    let mut stream = open(path, op, deadline::CONTROL).await?;
    send(&mut stream, request, &payload).await?;
    stream.hold(deadline::idle(op, deadline::HEARTBEAT_SILENCE));
    answer_of(&mut stream).await
}

fn decode<T: for<'de> Deserialize<'de>>(value: serde_json::Value) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|error| protocol_error(format!("malformed answer: {error}")))
}

async fn call<T: for<'de> Deserialize<'de>>(
    path: &Path,
    authority: &WorkspaceAuthoritySnapshot,
    call: Call,
    payload: Bytes,
) -> Result<T> {
    let (value, _) = round_trip(path, authority.into(), call, payload).await?;
    decode(value)
}

async fn open_process_events(
    path: &Path,
    authority: &WorkspaceAuthoritySnapshot,
    job_id: JobId,
    every: SampleInterval,
) -> Result<JobProcessStream> {
    // The subscription is answered at once; each event after it may take as long as the job
    // takes to do something, while the supervisor heartbeats the stream.
    let mut stream = open(path, "process-events", deadline::CONTROL).await?;
    let request = Request::Call {
        authority: authority.into(),
        call: Box::new(Call::ProcessEvents { job_id, every }),
        bytes: 0,
    };
    send(&mut stream, &request, &[]).await?;
    match answer_of(&mut stream).await? {
        (value, raw) if value.is_null() && raw.is_empty() => {}
        _ => return Err(protocol_error("malformed process subscription answer")),
    }
    stream.hold(deadline::idle(
        "process-events",
        deadline::HEARTBEAT_SILENCE,
    ));
    let (send_event, receive) = mpsc::channel(1);
    tokio::spawn(async move {
        loop {
            if let Err(error) = send(&mut stream, &Demand::Next, &[]).await {
                let _ = send_event.send(Err(error)).await;
                break;
            }
            let answered = tokio::select! {
                biased;
                () = send_event.closed() => None,
                answered = answer_of(&mut stream) => Some(answered),
            };
            let Some(answered) = answered else {
                if let Err(error) = send(&mut stream, &Demand::Close, &[]).await {
                    cowshed_os::eprintln!("cowshed: closing process observation: {error}");
                }
                break;
            };
            let event = match answered {
                Ok((value, raw)) if raw.is_empty() => decode::<Option<JobProcessEvent>>(value),
                Ok(_) => Err(protocol_error("process event carried raw bytes")),
                Err(error) => Err(error),
            };
            let ended = !matches!(event, Ok(Some(_)));
            match event {
                Ok(Some(event)) => {
                    if send_event.send(Ok(event)).await.is_err() {
                        if let Err(error) = send(&mut stream, &Demand::Close, &[]).await {
                            cowshed_os::eprintln!("cowshed: closing process observation: {error}");
                        }
                        break;
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    let _ = send_event.send(Err(error)).await;
                }
            }
            if ended {
                break;
            }
        }
    });
    Ok(JobProcessStream::new(receive))
}

/// What a holder asks of its hold: to let the job outlive it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum HoldDemand {
    Release,
}

/// A job this process holds (`hold`). Dropping it while the job runs — this process ending
/// included, however it ends — kills the job; [`JobHold::release`] lets the job run on.
#[derive(Debug)]
pub struct JobHold {
    stream: PeerStream<UnixStream>,
}

impl JobHold {
    /// Let the held job outlive this hold. A job that ended meanwhile has already answered the
    /// hold with its terminal record, which releases it as well.
    pub async fn release(mut self) -> Result<()> {
        self.stream
            .request(deadline::start("release", deadline::CONTROL));
        let sent = send(&mut self.stream, &HoldDemand::Release, &[]).await;
        match (answer_of(&mut self.stream).await, sent) {
            (Ok(_), _) => Ok(()),
            (Err(_), Err(error)) | (Err(error), Ok(())) => Err(error),
        }
    }
}

/// Hold job `job_id` of the supervisor serving `path`, which must serve `incarnation`: the job
/// lives no longer than the returned [`JobHold`] unless it is released (05_gateway.md, "A gate
/// lives with its operator"). The hold names the authority served now, so a grant change
/// between the job's admission and its hold changes nothing.
pub async fn hold(
    path: &Path,
    incarnation: &crate::metadata::WorkspaceIncarnation,
    job_id: JobId,
) -> Result<JobHold> {
    let serving = hello(path).await?;
    if serving.authority.workspace_incarnation != *incarnation {
        return Err(CowshedError::conflict(
            format!(
                "the supervisor at {} serves another incarnation of the workspace than job {}'s",
                path.display(),
                job_id.get()
            ),
            "run the command again against the current workspace",
        ));
    }
    hold_as(path, &serving.authority, job_id).await
}

/// [`hold`] under `authority`, the one the caller already read from the supervisor at `path`,
/// without asking for it again. A grant change since then makes the supervisor refuse that
/// authority as stale, and that refusal is answered by [`hold`], which reads the authority
/// served now: a grant change between the job's admission and its hold still changes nothing.
pub async fn hold_under(
    path: &Path,
    authority: &WorkspaceAuthoritySnapshot,
    job_id: JobId,
) -> Result<JobHold> {
    match hold_as(path, authority, job_id).await {
        Err(error) if error.code == crate::ErrorCode::Conflict => {
            hold(path, &authority.workspace_incarnation, job_id).await
        }
        held => held,
    }
}

async fn hold_as(
    path: &Path,
    authority: &WorkspaceAuthoritySnapshot,
    job_id: JobId,
) -> Result<JobHold> {
    // The hold is in place once the supervisor answers, which it does at once.
    let mut stream = open(path, "hold", deadline::CONTROL).await?;
    let request = Request::Call {
        authority: authority.into(),
        call: Box::new(Call::Hold { job_id }),
        bytes: 0,
    };
    send(&mut stream, &request, &[]).await?;
    match answer_of(&mut stream).await? {
        (value, raw) if value.is_null() && raw.is_empty() => Ok(JobHold { stream }),
        _ => Err(protocol_error("malformed hold answer")),
    }
}

async fn forward(path: Arc<PathBuf>, command: Command) {
    let path = path.as_path();
    match command {
        #[cfg(target_os = "macos")]
        Command::SubtreeHandle { reply, .. } => {
            let _ = reply.send(Err(CowshedError::conflict(
                "an authenticated membership peer cannot be forwarded through coordinator authority",
                "connect directly to the membership-only endpoint",
            )));
        }
        #[cfg(target_os = "macos")]
        Command::RetireDescendants { reply, .. } => {
            let _ = reply.send(Err(CowshedError::conflict(
                "an authenticated membership peer cannot be forwarded through coordinator authority",
                "connect directly to the membership-only endpoint",
            )));
        }
        Command::AdvanceAuthority { reply, .. } => {
            // Only the serving runtime can rebuild the sandbox from controller-owned grants.
            // Remote callers request that through the manager's advance exchange (11_shell.md).
            let _ = reply.send(Err(CowshedError::conflict(
                "a remote supervisor handle cannot install a grant snapshot",
                "ensure the supervisor through its manager to re-read the controller-owned grants",
            )));
        }
        Command::OpenSession {
            authority,
            name,
            reply,
        } => {
            let result =
                call::<SessionWire>(path, &authority, Call::OpenSession { name }, Bytes::new())
                    .await
                    .map(|wire| SessionToken::remote(&authority, wire.identity, wire.name));
            let _ = reply.send(result);
        }
        Command::SessionSnapshot {
            authority,
            session,
            reply,
        } => {
            let result = call::<SessionSnapshotWire>(
                path,
                &authority,
                Call::SessionSnapshot {
                    session: SessionWire::from(&session),
                },
                Bytes::new(),
            )
            .await
            .map(|wire| SessionSnapshot {
                identity: wire.identity,
                name: wire.name,
                cwd: wire.cwd,
                env: wire.env,
                background_jobs: wire.background_jobs,
            });
            let _ = reply.send(result);
        }
        Command::CloseSession {
            authority,
            session,
            reply,
        } => {
            let result = call::<()>(
                path,
                &authority,
                Call::CloseSession {
                    session: SessionWire::from(&session),
                },
                Bytes::new(),
            )
            .await;
            let _ = reply.send(result);
        }
        Command::Exec {
            authority,
            session,
            build_volume,
            request,
            source,
            parent_jobserver,
            reply,
        } => {
            forward_exec(
                path,
                authority,
                session,
                build_volume,
                *request,
                source,
                parent_jobserver,
                reply,
            )
            .await;
        }
        Command::Detach {
            authority,
            job_id,
            reply,
        } => {
            let _ = reply.send(call(path, &authority, Call::Detach { job_id }, Bytes::new()).await);
        }
        Command::StdinWrite {
            authority,
            job_id,
            offset,
            bytes,
            reply,
        } => {
            let call_request = Call::StdinWrite { job_id, offset };
            let _ = reply.send(call(path, &authority, call_request, bytes).await);
        }
        Command::StdinClose {
            authority,
            job_id,
            reply,
        } => {
            let _ =
                reply.send(call(path, &authority, Call::StdinClose { job_id }, Bytes::new()).await);
        }
        Command::Info {
            authority,
            job_id,
            reply,
        } => {
            let _ = reply.send(call(path, &authority, Call::Info { job_id }, Bytes::new()).await);
        }
        Command::Sealed {
            authority,
            job_id,
            reply,
        } => {
            let _ = reply.send(call(path, &authority, Call::Sealed { job_id }, Bytes::new()).await);
        }
        Command::Resources {
            authority,
            job_id,
            reply,
        } => {
            let _ =
                reply.send(call(path, &authority, Call::Resources { job_id }, Bytes::new()).await);
        }
        Command::Processes {
            authority,
            job_id,
            reply,
        } => {
            let _ =
                reply.send(call(path, &authority, Call::Processes { job_id }, Bytes::new()).await);
        }
        Command::ProcessEvents {
            authority,
            job_id,
            every,
            reply,
        } => {
            let _ = reply.send(open_process_events(path, &authority, job_id, every).await);
        }
        Command::AsIs {
            authority,
            build_volume,
            reply,
        } => {
            let _ =
                reply.send(call(path, &authority, Call::AsIs { build_volume }, Bytes::new()).await);
        }
        Command::ShellPool { authority, reply } => {
            let _ = reply.send(call(path, &authority, Call::ShellPool, Bytes::new()).await);
        }
        Command::TraceHealth { authority, reply } => {
            let _ = reply.send(call(path, &authority, Call::TraceHealth, Bytes::new()).await);
        }
        Command::ListeningPorts {
            authority,
            job_id,
            reply,
        } => {
            let _ = reply.send(
                call(
                    path,
                    &authority,
                    Call::ListeningPorts { job_id },
                    Bytes::new(),
                )
                .await,
            );
        }
        Command::JobByKey {
            authority,
            key,
            reply,
        } => {
            let _ = reply.send(call(path, &authority, Call::JobByKey { key }, Bytes::new()).await);
        }
        Command::Progress {
            authority,
            job_id,
            reply,
        } => {
            let _ =
                reply.send(call(path, &authority, Call::Progress { job_id }, Bytes::new()).await);
        }
        Command::List { authority, reply } => {
            let _ = reply.send(call(path, &authority, Call::List, Bytes::new()).await);
        }
        Command::Kill {
            authority,
            job_id,
            reply,
        } => {
            let _ = reply.send(call(path, &authority, Call::Kill { job_id }, Bytes::new()).await);
        }
        Command::Wait {
            authority,
            job_id,
            reply,
        } => {
            let _ = reply.send(call(path, &authority, Call::Wait { job_id }, Bytes::new()).await);
        }
        Command::LogRead {
            authority,
            job_id,
            stream,
            offset,
            follow,
            reply,
        } => {
            let result = round_trip(
                path,
                (&authority).into(),
                Call::LogRead {
                    job_id,
                    stream: stream.into(),
                    offset,
                    follow,
                },
                Bytes::new(),
            )
            .await
            .and_then(|(value, bytes)| {
                let wire: LogChunkWire = decode(value)?;
                Ok(LogChunk {
                    bytes,
                    next_offset: wire.next_offset,
                    eof: wire.eof,
                })
            });
            let _ = reply.send(result);
        }
        Command::Tail {
            authority,
            job_id,
            cursor,
            limits,
            reply,
        } => {
            let _ = reply.send(
                call(
                    path,
                    &authority,
                    Call::Tail {
                        job_id,
                        cursor,
                        limits,
                    },
                    Bytes::new(),
                )
                .await,
            );
        }
        // Only the supervisor's own process may close its admission without retiring it
        // (`WorkspaceSupervisorHandle::quiesce`): a remote caller that went away afterwards would
        // leave it refusing work for good.
        Command::Quiesce { reply, .. } => {
            let _ = reply.send(Err(CowshedError::internal(
                "a remote supervisor handle cannot quiesce without retiring; ask retire_after_jobs",
            )));
        }
        Command::Retire {
            authority,
            nx_daemon,
            when,
            reply,
        } => {
            let _ = reply.send(
                call(
                    path,
                    &authority,
                    Call::Retire { nx_daemon, when },
                    Bytes::new(),
                )
                .await,
            );
        }
        Command::StopNxKeeper { authority, reply } => {
            let _ = reply.send(call(path, &authority, Call::StopNxKeeper, Bytes::new()).await);
        }
        Command::NameBuildVolume {
            authority,
            build_volume,
            reply,
        } => {
            let _ = reply.send(
                call(
                    path,
                    &authority,
                    Call::NameBuildVolume { build_volume },
                    Bytes::new(),
                )
                .await,
            );
        }
        Command::CurrentAuthority { reply } => {
            let _ = reply.send(hello(path).await.map(|hello| hello.authority));
        }
        #[cfg(target_os = "macos")]
        Command::Idle { reply } => {
            // Only the process running a supervisor asks it whether it is idle.
            let _ = reply.send(Err(CowshedError::internal(
                "idleness is asked of an in-process supervisor only",
            )));
        }
        Command::NxDaemonKeeper { reply } => {
            // The keeper's state has no wire form: the process running the supervisor asks it.
            let _ = reply.send(Err(CowshedError::internal(
                "the Nx daemon keeper's state is asked of an in-process supervisor only",
            )));
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the fields of one `Command::Exec`, forwarded as one supervisor call"
)]
async fn forward_exec(
    path: &Path,
    authority: WorkspaceAuthoritySnapshot,
    session: Option<SessionToken>,
    build_volume: Option<PathBuf>,
    request: ExecRequest,
    source: AdmissionSource,
    parent_jobserver: Option<super::job_cpu::InheritedJobserver>,
    reply: oneshot::Sender<Result<JobId>>,
) {
    let (stdin, payload, stream) = match request.stdin {
        StdinSource::Empty => (StdinWire::Empty, Bytes::new(), None),
        StdinSource::Inline(bytes) => (StdinWire::Inline, bytes, None),
        StdinSource::WorkspaceFile(workspace_path) => (
            StdinWire::WorkspaceFile { workspace_path },
            Bytes::new(),
            None,
        ),
        StdinSource::Stream(reader) => (StdinWire::Stream, Bytes::new(), Some(reader)),
        StdinSource::Open => (StdinWire::Open, Bytes::new(), None),
    };
    let (argv, script) = match request.command {
        ExecCommand::Argv(argv) => (Some(argv), None),
        ExecCommand::Script(script) => (None, Some(script)),
    };
    let call_request = Call::Exec {
        session: session.as_ref().map(SessionWire::from),
        build_volume,
        source,
        request: Box::new(ExecWire {
            argv,
            script,
            cwd: request.cwd,
            mode: request.mode,
            env: request.env,
            trace: request.trace,
            stdin,
            stdout_copy: request.stdout_copy,
            stderr_copy: request.stderr_copy,
            admission_key: request.admission_key,
            class: request.class,
            want: request.want,
            scope: request.scope,
            parent_jobserver,
        }),
    };
    let admitted = call::<JobId>(path, &authority, call_request, payload).await;
    let job_id = admitted.as_ref().ok().copied();
    let _ = reply.send(admitted);
    if let (Some(job_id), Some(mut reader)) = (job_id, stream) {
        let mut buffer = vec![0_u8; STREAM_CHUNK];
        let error = loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break None,
                Ok(count) => {
                    let chunk = Bytes::copy_from_slice(&buffer[..count]);
                    if call::<()>(path, &authority, Call::StreamChunk { job_id }, chunk)
                        .await
                        .is_err()
                    {
                        // The job stopped reading or the supervisor went away; either way the
                        // job's own state says what happened, and there is no one to send to.
                        return;
                    }
                }
                Err(error) => break Some(error.to_string()),
            }
        };
        let _ = call::<()>(
            path,
            &authority,
            Call::StreamEnd { job_id, error },
            Bytes::new(),
        )
        .await;
    }
}

#[cfg(test)]
mod process_demand_tests {
    use super::*;
    use crate::api::process::ProcessCoverageGap;

    #[tokio::test]
    async fn a_process_demand_delivers_a_canonical_gap_and_then_ends() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let (events, receive) = mpsc::channel(1);
        events
            .send(Ok(JobProcessEvent::Gap {
                reason: ProcessCoverageGap::EventsLost,
            }))
            .await
            .unwrap();
        drop(events);
        let relay = tokio::spawn(relay_process_events(
            server,
            JobProcessStream::new(receive),
            idle_probe,
        ));
        write_json(&mut client, &Demand::Next).await.unwrap();
        let Response::Ok { value, bytes: 0 } = read_json(&mut client).await.unwrap() else {
            panic!("one demand must answer with the event");
        };
        assert_eq!(
            decode::<Option<JobProcessEvent>>(value).unwrap(),
            Some(JobProcessEvent::Gap {
                reason: ProcessCoverageGap::EventsLost,
            })
        );
        write_json(&mut client, &Demand::Next).await.unwrap();
        let Response::Ok { value, bytes: 0 } = read_json(&mut client).await.unwrap() else {
            panic!("the next demand must answer with the stream end");
        };
        assert!(value.is_null());
        relay.await.unwrap().unwrap();
    }

    /// A heartbeat probe of an actor between turns: these relays answer before any is due.
    fn idle_probe() -> Result<Pulse> {
        Ok(Pulse {
            job: None,
            actor: ActorPulse {
                turns: 0,
                busy: None,
            },
        })
    }
    #[tokio::test]
    async fn closing_an_unanswered_process_demand_drops_the_subscription() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let (events, receive) = mpsc::channel(1);
        let relay = tokio::spawn(relay_process_events(
            server,
            JobProcessStream::new(receive),
            idle_probe,
        ));
        write_json(&mut client, &Demand::Next).await.unwrap();
        write_json(&mut client, &Demand::Close).await.unwrap();
        let response = read_json(&mut client).await.unwrap();
        let Response::Ok { value, bytes: 0 } = response else {
            panic!("close must answer with the stream end");
        };
        assert!(value.is_null());
        relay.await.unwrap().unwrap();
        assert!(events.is_closed());
    }

    #[tokio::test]
    async fn a_second_unanswered_process_demand_is_refused() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let (_events, receive) = mpsc::channel(1);
        let relay = tokio::spawn(relay_process_events(
            server,
            JobProcessStream::new(receive),
            idle_probe,
        ));
        write_json(&mut client, &Demand::Next).await.unwrap();
        write_json(&mut client, &Demand::Next).await.unwrap();
        let error = relay.await.unwrap().unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Integrity);
    }
}

#[cfg(test)]
mod socket_ownership_tests {
    use super::*;
    use cowshed_os::fork_lock::Spawn as _;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::UnixStreamBackstop as _;
    use cowshed_os::test_backstop::{
        AsyncChildBackstop as _, AsyncUnixStreamBackstop as _, ChildBackstop as _,
    };

    /// A supervisor's socket is recognized by exactly the name `socket_path` gives it; its
    /// membership twin and every other entry of `run` are not supervisor sockets.
    #[test]
    fn only_the_name_socket_path_gives_is_a_supervisor_socket() {
        let socket = socket_path(
            Path::new("/store"),
            &crate::repository::RepoId::parse("acme/widget").expect("repo id"),
            &crate::metadata::WorkspaceName::new("raven").expect("workspace"),
        );
        let named = |path: &Path| is_socket_name(path.file_name().expect("a file name"));
        assert!(named(&socket), "{}", socket.display());
        assert!(!named(&membership_socket_path(&socket)));
        assert!(!named(&super::super::job_groups::ledger_path(&socket)));
        let digest = "0123456789abcdef0123456789abcdef";
        for (name, supervisor) in [
            (format!("{digest}.sock"), true),
            (format!("{digest}.members.sock"), false),
            (format!("{digest}.sock.lock"), false),
            (format!("{digest}.groups"), false),
            (format!("{digest}.bind"), false),
            ("manager.sock".to_owned(), false),
            (format!("{}.sock", &digest[1..]), false),
            (format!("{digest}0.sock"), false),
            (format!("{}.sock", digest.to_uppercase()), false),
        ] {
            assert_eq!(is_socket_name(OsStr::new(&name)), supervisor, "{name}");
        }
    }

    fn path() -> PathBuf {
        crate::temp_parent::scratch_parent()
            .join(format!(
                "cowshed-fence-{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            ))
            .join("s.sock")
    }

    #[tokio::test]
    async fn concurrent_binders_never_replace_the_winning_socket() {
        use std::os::unix::fs::MetadataExt as _;
        let path = path();
        let (first, second) = tokio::join!(bind(&path), bind(&path));
        let winner = match (first, second) {
            (Ok(socket), Err(error)) | (Err(error), Ok(socket)) => {
                assert_eq!(error.code, crate::ErrorCode::Conflict);
                socket
            }
            results => panic!("exactly one binder must own the socket: {results:?}"),
        };
        let inode = std::fs::symlink_metadata(&path).unwrap().ino();
        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::Conflict
        );
        assert_eq!(std::fs::symlink_metadata(&path).unwrap().ino(), inode);
        drop(winner);
        assert!(!path.exists(), "only the owning socket removes its inode");
        let replacement = bind(&path).await.unwrap();
        assert!(
            path.exists(),
            "released ownership permits a replacement listener"
        );
        drop(replacement);
        assert!(!path.exists());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A released socket's lease can stay held after its owner's close by a spawn's transient
    /// copy (01c_macos.md, "`posix_spawnp` leaves transient descriptor copies"). The forked child
    /// here is that copy: a probe is refused while it lives, and the released binder is granted
    /// the lease once it closes.
    #[tokio::test]
    async fn a_released_socket_is_rebound_once_a_transient_copy_of_its_lease_closes() {
        use std::os::fd::AsRawFd as _;
        let path = path();
        let socket = bind(&path).await.unwrap();
        let (gate, release) = cowshed_os::fork_lock::create(std::io::pipe).unwrap();
        let (gate_fd, release_fd) = (gate.as_raw_fd(), release.as_raw_fd());
        // SAFETY: the child calls only `close`, `read` and `_exit`, which are async-signal-safe,
        // on descriptors it inherited, and never returns.
        let copy = unsafe { libc::fork() };
        assert!(copy >= 0, "fork: {}", io::Error::last_os_error());
        if copy == 0 {
            // SAFETY: as above.
            unsafe {
                libc::close(release_fd);
                let mut byte = 0_u8;
                libc::read(gate_fd, (&raw mut byte).cast(), 1);
                libc::_exit(0);
            }
        }
        drop(gate);
        drop(socket);
        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::Conflict,
            "a probe finds the copy holding the lease"
        );
        // The binder starts while the copy holds the lease: its task runs until it awaits its
        // blocking take, already handed to another thread, before the copy is let go. The copy's
        // exit, a process teardown, then follows that take in practice, though nothing orders the
        // two; the probe's refusal above is the ordered half.
        let (started, start) = tokio::sync::oneshot::channel();
        let rebound = tokio::spawn({
            let path = path.clone();
            async move {
                let _ = started.send(());
                bind_released(&path, &crate::census::this_test()).await
            }
        });
        start.await.unwrap();
        drop(release);
        let rebound = rebound
            .await
            .unwrap()
            .expect("the copy's close grants the released binder the lease");
        cowshed_os::test_backstop::waitpid(copy, 0).unwrap();
        drop(rebound);
        assert!(!path.exists());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_peer_closing_mid_hello_is_unknown_not_absent() {
        let path = path();
        let socket = bind(&path).await.unwrap();
        let peer = tokio::spawn(async move {
            let (stream, _) = socket.listener.accept().await.unwrap();
            drop(stream);
            socket
        });
        hello_if_present(&path)
            .await
            .expect_err("a connected peer's interrupted outcome is unknown");
        let socket = peer.await.unwrap();
        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::Conflict
        );
        drop(socket);
        assert_eq!(hello_if_present(&path).await.unwrap(), None);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    fn inode(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt as _;
        std::fs::symlink_metadata(path).unwrap().ino()
    }

    /// The names in the directory of `path`, sorted.
    fn entries(path: &Path) -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    /// `listener` takes a new connection made to `path`.
    async fn accepts(listener: &UnixListener, path: &Path) {
        let client = UnixStream::connect_bounded(path).await.unwrap();
        listener.accept().await.unwrap();
        drop(client);
    }

    /// A stream connection a socket refuses while it is still attached to its file -- here, bound
    /// by a binder without the lease and not listening -- is neither absence nor stale: the
    /// socket and its file are kept.
    #[tokio::test]
    async fn a_refusing_socket_still_attached_is_kept_not_absent() {
        let path = path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let held = tokio::net::UnixSocket::new_stream().unwrap();
        held.bind(&path).unwrap();
        let before = inode(&path);
        assert_eq!(
            UnixStream::connect_bounded(&path).await.unwrap_err().kind(),
            io::ErrorKind::ConnectionRefused
        );

        let error = hello_if_present(&path)
            .await
            .expect_err("a refusal by an attached socket is not absence");
        assert_eq!(error.code, crate::ErrorCode::EnvironmentMissing);
        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::Conflict
        );
        assert_eq!(inode(&path), before);
        let listener = held.listen(8).unwrap();
        accepts(&listener, &path).await;
        assert_eq!(entries(&path), ["s.sock", "s.sock.lock"]);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Darwin refuses a stream connection to a live listener whose queue is full, as it does
    /// one to a socket nothing listens on. A live supervisor of a build without the lease, so
    /// saturated, keeps its socket.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_saturated_live_listener_is_kept_not_absent() {
        let path = path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let socket = tokio::net::UnixSocket::new_stream().unwrap();
        socket.bind(&path).unwrap();
        let listener = socket.listen(1).unwrap();
        let before = inode(&path);
        let mut queued = Vec::new();
        let refused = loop {
            match std::os::unix::net::UnixStream::connect_bounded(&path) {
                Ok(stream) => queued.push(stream),
                Err(error) => break error,
            }
            assert!(queued.len() < 64, "the queue never filled");
        };
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);

        let error = hello_if_present(&path)
            .await
            .expect_err("a saturated live listener is not absent");
        assert_eq!(error.code, crate::ErrorCode::EnvironmentMissing);
        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::Conflict
        );
        assert_eq!(inode(&path), before);
        for _ in &queued {
            listener.accept().await.unwrap();
        }
        drop(queued);
        accepts(&listener, &path).await;
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A socket a binder without the lease left when it stopped -- every descriptor of it
    /// closed -- is absent to hello, and the next bind replaces it. The binder creates and closes
    /// its listener as `bind` and a product release do, fenced: otherwise a child another test
    /// of this process is spawning meanwhile would hold it attached.
    #[tokio::test]
    async fn a_stopped_socket_of_a_binder_without_the_lease_is_replaced() {
        let path = path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        drop(Fenced::create(|| UnixListener::bind(&path)).unwrap());
        let stopped = inode(&path);

        assert_eq!(hello_if_present(&path).await.unwrap(), None);
        let socket = bind(&path).await.unwrap();
        assert_ne!(inode(&path), stopped);
        accepts(&socket.listener, &path).await;
        assert_eq!(entries(&path), ["s.sock", "s.sock.lock"]);
        drop(socket);
        assert_eq!(entries(&path), ["s.sock.lock"]);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Names the socket [`holds_a_bound_socket_until_killed`] binds.
    const BOUND_SOCKET: &str = "COWSHED_TEST_BOUND_SOCKET";

    /// Not a test: the owner process [`a_killed_owner_is_recovered_from`] kills.
    #[test]
    #[ignore = "helper process of a_killed_owner_is_recovered_from"]
    fn holds_a_bound_socket_until_killed() {
        let Some(path) = std::env::var_os(BOUND_SOCKET) else {
            return;
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let _socket = bind(Path::new(&path)).await.unwrap();
                std::future::pending::<()>().await;
            });
    }

    /// An owner killed while it holds the lease and serves the socket leaves both: the lease
    /// ends with it, its socket is absent to hello, and the next bind replaces it.
    #[tokio::test]
    async fn a_killed_owner_is_recovered_from() {
        let path = path();
        let mut owner = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::supervisor_socket::socket_ownership_tests::holds_a_bound_socket_until_killed",
                "--ignored",
            ])
            .env(BOUND_SOCKET, &path)
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn_locked()
            .unwrap();
        // Published last, once the owner holds the lease.
        while Instance::at(&path).ok().flatten().is_none() {
            if let Some(status) = owner.try_wait().unwrap() {
                panic!("the owner exited before it bound the socket: {status}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let killed = inode(&path);
        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::Conflict
        );

        owner.kill_bounded().await.unwrap();

        assert_eq!(hello_if_present(&path).await.unwrap(), None);
        let socket = bind(&path).await.unwrap();
        assert_ne!(inode(&path), killed);
        accepts(&socket.listener, &path).await;
        assert_eq!(entries(&path), ["s.sock", "s.sock.lock"]);
        drop(socket);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Names the socket [`binds_and_hands_its_listener_to_a_child`] binds.
    const INHERITED_SOCKET: &str = "COWSHED_TEST_INHERITED_SOCKET";

    /// Not a test: a binder without the lease, for
    /// [`a_listener_a_child_inherited_is_kept_after_its_creator_exits`]. It binds and listens,
    /// starts a child that inherits the listener and runs until its standard input ends, and
    /// exits.
    #[test]
    #[ignore = "helper process of a_listener_a_child_inherited_is_kept_after_its_creator_exits"]
    fn binds_and_hands_its_listener_to_a_child() {
        use std::os::fd::AsRawFd as _;
        let Some(path) = std::env::var_os(INHERITED_SOCKET) else {
            return;
        };
        let listener = std::os::unix::net::UnixListener::bind(path).unwrap();
        // SAFETY: F_SETFD on a descriptor this process holds open changes only its own flag.
        assert_eq!(
            unsafe { libc::fcntl(listener.as_raw_fd(), libc::F_SETFD, 0) },
            0
        );
        // The intermediary is waited by its parent; its background cat inherits the listener
        // and explicit stdin, then remains alive after the creator exits, until stdin ends.
        assert!(
            std::process::Command::new("/bin/sh")
                .args(["-c", "cat <&0 & exit 0"])
                .stdin(std::process::Stdio::inherit())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn_locked()
                .unwrap()
                .wait_bounded()
                .unwrap()
                .success()
        );
    }

    /// A listener stays live while any process holds it, whatever became of the process that
    /// created it -- the one a connection's peer identity names. Its socket is kept until the last
    /// holder closes it, and only then replaced.
    #[tokio::test]
    async fn a_listener_a_child_inherited_is_kept_after_its_creator_exits() {
        let path = path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut creator = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::supervisor_socket::socket_ownership_tests::binds_and_hands_its_listener_to_a_child",
                "--ignored",
            ])
            .env(INHERITED_SOCKET, &path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn_locked()
            .unwrap();
        // The child that inherited the listener reads this until it is dropped.
        let holder_input = creator.stdin.take().unwrap();
        assert!(creator.wait_bounded().await.unwrap().success());
        let held = inode(&path);
        drop(
            UnixStream::connect_bounded(&path)
                .await
                .expect("the inherited listener queues"),
        );

        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::Conflict
        );
        assert_eq!(inode(&path), held);
        assert_eq!(entries(&path), ["s.sock", "s.sock.lock"]);

        drop(holder_input);
        while !matches!(occupant(&path), Ok(Occupant::Stopped)) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(hello_if_present(&path).await.unwrap(), None);
        let socket = bind(&path).await.unwrap();
        assert_ne!(inode(&path), held);
        accepts(&socket.listener, &path).await;
        drop(socket);
        assert_eq!(entries(&path), ["s.sock.lock"]);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A bound socket released after another file took its path leaves that file where it is.
    #[tokio::test]
    async fn release_leaves_a_file_that_replaced_the_socket() {
        let path = path();
        let socket = bind(&path).await.unwrap();
        std::fs::rename(&path, path.with_file_name("moved.sock")).unwrap();
        let foreign = UnixListener::bind(&path).unwrap();
        let before = inode(&path);

        drop(socket);

        assert_eq!(inode(&path), before);
        accepts(&foreign, &path).await;
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Something at the socket path that is no socket is never taken as stopped or absent.
    #[tokio::test]
    async fn a_file_that_is_no_socket_is_kept() {
        let path = path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not a socket").unwrap();

        hello_if_present(&path)
            .await
            .expect_err("a file that is no socket is not absence");
        assert_eq!(
            bind(&path).await.unwrap_err().code,
            crate::ErrorCode::EnvironmentMissing
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"not a socket");
        assert_eq!(entries(&path), ["s.sock", "s.sock.lock"]);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A taker that reached a socket lease inode its holder then removed takes the lease at the
    /// name, never the removed inode: whether the remover still holds that inode when the taker
    /// tries it (refused) or has let go (locked), the taker finds the name no longer names it and
    /// reopens. Where another taker made the lease anew and holds it, this one is refused there,
    /// so no two hold "the" lease; where none did, it makes the lease at the name and holds that.
    #[test]
    fn a_taker_that_reached_a_removed_socket_lease_takes_the_one_at_its_name() {
        use std::os::unix::fs::MetadataExt as _;
        for (still_held, recreated) in [(true, false), (true, true), (false, false), (false, true)]
        {
            let case = format!("still held {still_held}, recreated {recreated}");
            let path = path();
            let lease_at = path.with_extension("sock.lock");
            let mut removing = Some(
                take_lease(&path, flock::open_lock)
                    .unwrap()
                    .expect("the first taker holds the lease"),
            );
            let directory =
                cowshed_os::fsio::open_directory_nofollow(path.parent().unwrap()).unwrap();
            let name = CString::new("s.sock.lock").unwrap();
            let mut reached = Some(flock::open_lock(&directory, &name).unwrap());
            let removed = reached.as_ref().unwrap().metadata().unwrap().ino();
            std::fs::remove_file(&lease_at).unwrap();
            let replacement = recreated.then(|| {
                take_lease(&path, flock::open_lock)
                    .unwrap()
                    .expect("a taker after the removal holds the lease made anew")
            });
            if !still_held {
                removing = None;
            }

            let mut opens = 0;
            let taken = take_lease(&path, |directory, name| {
                opens += 1;
                reached
                    .take()
                    .map_or_else(|| flock::open_lock(directory, name), Ok)
            })
            .unwrap();
            assert_eq!(opens, 2, "{case}: the removed inode was not reopened");
            match (replacement.is_some(), taken) {
                (true, None) => {}
                (false, Some(lease)) => {
                    let locked = lease.metadata().unwrap().ino();
                    assert_eq!(locked, inode(&lease_at), "{case}");
                    assert_ne!(locked, removed, "{case}");
                }
                (_, taken) => panic!(
                    "{case}: took the lease {}, beside a holder at the name",
                    taken.is_some()
                ),
            }
            drop((removing, replacement));
            std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
        }
    }

    /// A supervisor's drain report is written only while its socket is held: releasing the
    /// socket withdraws the report, and a task of the supervisor that publishes after the release,
    /// as a requester's exit watch can, writes nothing. The lease itself stays.
    #[tokio::test]
    async fn a_released_socket_withdraws_its_drain_report_and_lets_no_later_one_be_written() {
        let path = path();
        let mut socket = bind(&path).await.unwrap();
        let report = socket.drain_report();
        report.publish(DrainWait::Jobs {
            running: Vec::new(),
            ended: Vec::new(),
        });
        assert_eq!(entries(&path), ["s.drain", "s.sock", "s.sock.lock"]);

        drop(socket);
        assert_eq!(entries(&path), ["s.sock.lock"]);
        report.stuck(
            "retire",
            &CowshedError::internal("published after the release"),
        );
        assert_eq!(
            entries(&path),
            ["s.sock.lock"],
            "a publication after the release writes nothing"
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Drain residue is the report beside a supervisor socket and its writers' temps, in the one
    /// temp grammar, and nothing else.
    #[test]
    fn only_a_drain_report_and_its_writers_temps_are_drain_residue() {
        let run = Path::new("/run");
        let digest = "0123456789abcdef0123456789abcdef";
        let socket = run.join(format!("{digest}.sock"));
        let uuid = "fedcba9876543210fedcba9876543210";
        let written = cowshed_os::fsio::temp_name(OsStr::new(&format!("{digest}.drain")), uuid)
            .into_string()
            .unwrap();
        for (name, residue) in [
            (format!("{digest}.drain"), true),
            (written, true),
            (format!(".{digest}.drain.tmp.{}", &uuid[..31]), false),
            (
                format!(".{digest}.drain.tmp.{}", uuid.to_uppercase()),
                false,
            ),
            (format!(".{digest}.drain"), false),
            (format!("{digest}.members.drain"), false),
            (format!(".{digest}.groups.tmp.{uuid}"), false),
            (format!("{digest}.sock.lock"), false),
            (format!("{digest}.sock"), false),
            ("manager.drain".to_owned(), false),
        ] {
            assert_eq!(
                drain_residue(run, OsStr::new(&name)),
                residue.then(|| socket.clone()),
                "{name}"
            );
        }
    }

    /// A killed supervisor's drain report and temps go only under its socket's lease, taken free,
    /// with no socket attached: a holder of the lease and a live listener each keep them. A file
    /// named like a temp that is no regular file is kept, and so is the lease.
    #[tokio::test]
    async fn a_dead_drain_report_is_cleared_only_under_its_free_lease_with_nothing_serving() {
        let root = path().parent().unwrap().to_owned();
        std::fs::create_dir_all(&root).unwrap();
        let digest = "0123456789abcdef0123456789abcdef";
        let socket = root.join(format!("{digest}.sock"));
        let temp = |uuid: uuid::Uuid| root.join(format!(".{digest}.drain.tmp.{}", uuid.simple()));
        let report = socket.with_extension("drain");
        let (torn, linked) = (temp(uuid::Uuid::new_v4()), temp(uuid::Uuid::new_v4()));
        std::fs::write(&report, br#"{"pid":1}"#).unwrap();
        std::fs::write(&torn, br#"{"pid""#).unwrap();
        std::os::unix::fs::symlink(&report, &linked).unwrap();
        let residue = [report, torn, linked];
        let present = || {
            residue
                .iter()
                .map(|path| std::fs::symlink_metadata(path).is_ok())
                .collect::<Vec<_>>()
        };

        let holder = take_lease(&socket, flock::open_lock)
            .unwrap()
            .expect("the test holds the lease");
        clear_dead_drain(&socket, &residue);
        assert_eq!(
            present(),
            [true, true, true],
            "the lease's holder writes them"
        );
        drop(holder);

        let live = Fenced::create(|| UnixListener::bind(&socket)).unwrap();
        clear_dead_drain(&socket, &residue);
        assert_eq!(
            present(),
            [true, true, true],
            "a supervisor serves the socket"
        );
        drop(live);

        clear_dead_drain(&socket, &residue);
        assert_eq!(
            present(),
            [false, false, true],
            "the report and its temp go; a link named like a temp is kept"
        );
        assert!(
            socket.with_extension("sock.lock").exists(),
            "the lease stays"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod admission_source_tests {
    use super::*;
    use crate::error::{CpuAdmissionRefusal, ErrorCode};

    /// The route is the authority: a worker admits background, interactive and resident work in
    /// the workspace sandbox and nothing else, the refusal is the typed class-not-permitted error
    /// naming both, and the coordinator admits every pairing.
    #[test]
    fn a_worker_admits_its_workspaces_work_but_no_gate() {
        for class in [
            ExecClass::Background,
            ExecClass::Interactive,
            ExecClass::Gate,
            ExecClass::Resident,
            ExecClass::InteractiveResident,
        ] {
            for scope in [ExecScope::Workspace, ExecScope::HostGate] {
                let worker = AdmissionSource::Worker.admit(class, scope);
                if scope == ExecScope::Workspace && class != ExecClass::Gate {
                    worker.expect("a worker admits its workspace's non-gate work");
                } else {
                    let error = worker.expect_err("a worker admits nothing else");
                    assert_eq!(error.code, ErrorCode::SandboxDenied);
                    assert_eq!(
                        error.cpu_admission_source(),
                        Some(&CpuAdmissionRefusal::ClassNotPermitted { class, scope })
                    );
                }
                AdmissionSource::Coordinator
                    .admit(class, scope)
                    .expect("the coordinator admits every pairing");
            }
        }
    }

    /// The route crosses the socket in camel case and an unknown route is refused: the wire has
    /// no spelling for another authority.
    #[test]
    fn the_wire_spells_the_route_and_refuses_another() {
        assert_eq!(
            serde_json::to_value(AdmissionSource::Worker).expect("encode"),
            serde_json::json!("worker")
        );
        assert_eq!(
            serde_json::to_value(AdmissionSource::Coordinator).expect("encode"),
            serde_json::json!("coordinator")
        );
        assert!(serde_json::from_value::<AdmissionSource>(serde_json::json!("operator")).is_err());
    }

    /// An exec call carries its route and scheduling on the wire: a gate with its bound and the
    /// host-gate scope decode exactly, and an exec that names no route is refused.
    #[test]
    fn an_exec_call_carries_its_route_and_scheduling() {
        let call = serde_json::json!({
            "call": "exec",
            "session": null,
            "buildVolume": null,
            "source": "coordinator",
            "request": {
                "argv": [{ "encoding": "utf8", "data": "true" }],
                "script": null,
                "cwd": null,
                "mode": "readWrite",
                "env": {},
                "trace": null,
                "stdin": { "kind": "empty" },
                "stdoutCopy": null,
                "stderrCopy": null,
                "admissionKey": null,
                "class": "gate",
                "want": 8,
                "scope": "hostGate"
            }
        });
        let Call::Exec {
            source, request, ..
        } = serde_json::from_value::<Call>(call.clone()).expect("decode the exec call")
        else {
            panic!("decoded another call");
        };
        assert_eq!(source, AdmissionSource::Coordinator);
        assert_eq!(request.class, ExecClass::Gate);
        assert_eq!(request.want.map(NonZeroUsize::get), Some(8));
        assert_eq!(request.scope, ExecScope::HostGate);

        let mut unrouted = call;
        unrouted
            .as_object_mut()
            .expect("an object")
            .remove("source");
        assert!(serde_json::from_value::<Call>(unrouted).is_err());
    }
}

#[cfg(test)]
mod drain_read_tests {
    use cowshed_os::test_backstop::AsyncChildBackstop as _;
    use std::time::Duration;

    use super::*;

    fn job(id: u64) -> JobId {
        JobId::new(id).expect("a job id")
    }

    const BOUND: Duration = Duration::from_secs(30 * 60);

    /// Owe job `id` to a process whose identity could not be read, so it is never seen exiting.
    fn unseen(owed: &DrainReads, id: u64) {
        owed.asked().insert(
            job(id),
            Asked {
                class: ExecClass::Background,
                requester: None,
                readers: Vec::new(),
                unidentified: true,
            },
        );
    }
    /// What would end a resident: a supervisor handle these tests never call, and a report path.
    fn ends() -> ResidentEnd {
        ResidentEnd {
            supervisor: connect(
                PathBuf::from("/nonexistent/cowshed-drain-test.sock"),
                WorkspaceAuthoritySnapshot {
                    repo_id: crate::repository::RepoId::parse("acme/widget").expect("a repo"),
                    workspace: crate::metadata::WorkspaceName::main(),
                    workspace_incarnation: crate::metadata::WorkspaceIncarnation::new(
                        "0".repeat(32),
                    )
                    .expect("an incarnation"),
                    grant_revision: 1,
                    lifecycle_revision: 1,
                },
            ),
            report: DrainReport::beside(Path::new("/nonexistent/cowshed-drain-test.sock")),
        }
    }

    /// A running process this test owns, held by its kernel identity, and its stdin: it exits
    /// once that closes.
    fn live() -> (tokio::process::Child, Process) {
        use cowshed_os::fork_lock::Spawn as _;
        let child = tokio::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("start a process");
        let pid = i32::try_from(child.id().expect("a running process")).expect("a pid");
        let process = Process::observe(pid)
            .expect("observe the process")
            .expect("the process runs");
        (child, process)
    }

    /// A drain closes admission only once it began and listed its jobs, so a resident can be
    /// admitted meanwhile: whether its requester is recorded before or after the drain begins,
    /// the drain owes it to that requester, waits for it, and ends it when the requester exits.
    /// Before, one recorded after the drain began went only to the pre-drain record, so no
    /// requester's exit ended it and the drain waited for it forever.
    #[tokio::test]
    async fn a_resident_admitted_after_the_drain_listed_its_jobs_ends_with_its_requester() {
        let ends = ends();
        let (_before_child, before) = live();
        let (_after_child, after) = live();
        let owed = Arc::new(DrainReads::default());
        // Admitted before the drain began: the drain takes it from the socket's record.
        owed.admitted(job(6), ExecClass::Resident, Ok(before), &ends.supervisor);
        let watches = owed.begin(ends).expect("the first drain");
        assert_eq!(watches.len(), 1, "job 6's requester is watched");
        assert!(
            watches
                .iter()
                .all(|watch| watch.job == job(6) && watch.resident.is_some()),
            "job 6 ends with its requester"
        );
        // Admitted before admission closed but recorded after the drain began.
        let watch = owed
            .drain()
            .as_mut()
            .expect("the drain")
            .requester(job(5), ExecClass::InteractiveResident, Ok(after))
            .expect("job 5's requester is watched");
        assert_eq!(watch.job, job(5));
        assert!(watch.resident.is_some(), "job 5 ends with its requester");
        let drain = owed.drain();
        let drain = drain.as_ref().expect("the drain");
        assert_eq!(drain.running, [job(6), job(5)], "the drain waits for both");
        assert!(drain.owed.values().all(|owed| owed.readers.len() == 1));
    }

    /// A background job recorded after the drain began is owed to its requester, never ended
    /// with it.
    #[tokio::test]
    async fn a_background_job_admitted_after_the_drain_listed_its_jobs_outlives_its_requester() {
        let (_child, requester) = live();
        let owed = DrainReads::default();
        owed.begin(ends()).expect("the first drain");
        let watch = owed
            .drain()
            .as_mut()
            .expect("the drain")
            .requester(job(4), ExecClass::Background, Ok(requester))
            .expect("job 4's requester is watched");
        assert!(
            watch.resident.is_none(),
            "a background job runs to its own end"
        );
    }

    /// A job is read once its end reached a client and every stream that client was reading
    /// reached EOF: the end alone leaves the tail of a relayed stream unread.
    #[tokio::test]
    async fn a_job_is_read_once_its_end_and_every_stream_being_read_reached_a_client() {
        let owed = DrainReads::default();
        unseen(&owed, 1);
        unseen(&owed, 2);
        assert!(owed.begin(ends()).is_some());
        assert!(
            owed.begin(ends()).is_none(),
            "a second drain owes nothing new"
        );
        owed.record(job(1), |owed| owed.reading[0] = true);
        owed.record(job(1), |owed| owed.end = true);
        owed.record(job(2), |owed| owed.end = true);
        let settled = |owed: &DrainReads, id: u64| {
            owed.drain()
                .as_ref()
                .is_some_and(|drain| drain.owed[&job(id)].settled())
        };
        assert!(!settled(&owed, 1), "job 1's stdout tail is still owed");
        assert!(settled(&owed, 2));
        owed.record(job(1), |owed| owed.eof[0] = true);
        let settled = owed.settle(Duration::from_secs(60 * 60)).await;
        assert_eq!(settled.read, [job(1), job(2)]);
        assert!(settled.unread.is_empty());
        assert_eq!(settled.to_string(), "read by a client: [1, 2]");
    }

    /// A job no process asked about is owed to nobody: the Nx keeper's daemon, which the
    /// supervisor started for itself, holds no drained supervisor once it has ended. Measured
    /// 2026-10-27: after an install, each shed whose keeper ran a daemon refused every command of
    /// the new build for the idle bound, its drain report waiting on the daemon's end.
    #[tokio::test(start_paused = true)]
    async fn a_job_no_process_asked_about_is_owed_to_nobody() {
        let owed = DrainReads::default();
        owed.begin(ends());
        owed.listed([job(7)]);
        let settled = owed.settle(BOUND).await;
        assert_eq!(settled.abandoned, [job(7)]);
        assert!(settled.unread.is_empty());
    }

    /// A job owed to a process the drain can never see exiting cannot keep a drained supervisor
    /// serving either: past the bound with no read it retires, and says which ends went unread.
    #[tokio::test(start_paused = true)]
    async fn an_end_no_process_can_be_seen_reading_is_given_up_after_the_idle_bound_and_named() {
        let owed = DrainReads::default();
        unseen(&owed, 7);
        owed.begin(ends());
        owed.listed([job(7)]);
        let settled = owed.settle(BOUND).await;
        assert!(settled.read.is_empty());
        assert_eq!(settled.unread, [job(7)]);
        assert_eq!(
            settled.to_string(),
            "read by a client: []; unread after 30 minutes with no read: [7]"
        );
    }

    /// An exec the actor admitted before the drain closed admission may have its requester
    /// recorded only after its job ended and the quiesce returned: the drain cannot settle while
    /// that exec is in flight, so the job's obligation is never created after retirement was
    /// decided. A refused exec owes nothing once it is done.
    #[tokio::test]
    async fn a_drain_settles_only_once_every_exec_in_flight_is_recorded() {
        fn ready<F: Future>(future: std::pin::Pin<&mut F>) -> bool {
            future
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_ready()
        }
        // Refused: once the exec is done the drain settles at once.
        let owed = DrainReads::default();
        owed.begin(ends()).expect("the first drain");
        let refused = owed.admitting();
        {
            let mut settle = std::pin::pin!(owed.settle(BOUND));
            assert!(!ready(settle.as_mut()), "an exec in flight holds the drain");
            drop(refused);
            assert!(ready(settle.as_mut()), "a refused exec owes nothing");
        }

        // Admitted: its job ended and the quiesce returned before its requester was recorded.
        let (mut child, requester) = live();
        let ends = ends();
        let owed = Arc::new(DrainReads::default());
        owed.begin(ends.clone()).expect("the first drain");
        let admitting = owed.admitting();
        let mut settle = std::pin::pin!(owed.settle(BOUND));
        assert!(
            !ready(settle.as_mut()),
            "the exec in flight holds the drain"
        );
        owed.admitted(
            job(9),
            ExecClass::Background,
            Ok(requester),
            &ends.supervisor,
        );
        drop(admitting);
        assert!(
            !ready(settle.as_mut()),
            "job 9 is owed to its live requester once recorded"
        );
        drop(child.stdin.take());
        assert!(
            child
                .wait_bounded()
                .await
                .expect("the requester exits")
                .success()
        );
        let settled = tokio::time::timeout(Duration::from_secs(60), settle)
            .await
            .expect("whole-leg watchdog: the drain settles once job 9's requester exited");
        assert_eq!(settled.abandoned, [job(9)]);
    }

    /// The idle bound gives up on unread ends, never on an exec in flight: past the bound the
    /// drain still waits for that exec to be recorded or refused, then settles.
    #[tokio::test]
    async fn the_idle_bound_gives_up_on_unread_ends_never_on_an_exec_in_flight() {
        let owed = DrainReads::default();
        unseen(&owed, 7);
        owed.begin(ends()).expect("the first drain");
        let admitting = owed.admitting();
        assert!(!owed.may_settle(false), "job 7 is unread");
        assert!(
            !owed.may_settle(true),
            "past the bound, the exec in flight holds it"
        );
        drop(admitting);
        assert!(!owed.may_settle(false), "job 7 is still unread");
        assert!(owed.may_settle(true), "past the bound, nothing holds it");
    }

    /// Measured 2026-10-22: a drained supervisor whose ended job's client was gone answered the
    /// `hello` of every command of the new build it refused, and each one started its idle bound
    /// over, so it served until it was killed. Only a read of an owed job starts the bound over:
    /// the bound's wait restarts on the `read` notification and on nothing else.
    #[tokio::test]
    async fn only_a_read_of_an_owed_job_starts_the_idle_bound_over() {
        let owed = DrainReads::default();
        owed.begin(ends());
        owed.listed([job(7)]);
        let restarted = |owed: &DrainReads| {
            let mut notified = std::pin::pin!(owed.read.notified());
            notified
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_ready()
        };
        owed.record(job(8), |owed| owed.reading[0] = true);
        assert!(
            !restarted(&owed),
            "a call about a job not owed reads nothing"
        );
        owed.record(job(7), |owed| owed.reading[0] = true);
        assert!(
            restarted(&owed),
            "a read of an owed job starts the bound over"
        );
    }

    /// A job owed to processes that have all exited is owed to nobody: the drain waits for no
    /// reader that no longer exists, however long its idle bound.
    #[tokio::test]
    async fn a_job_every_process_of_which_exited_is_owed_to_nobody() {
        use cowshed_os::fork_lock::Spawn as _;
        let mut reader = tokio::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("start a reader");
        let pid = i32::try_from(reader.id().expect("a running reader")).expect("a pid");
        let process = Process::observe(pid)
            .expect("observe the reader")
            .expect("the reader runs");
        let owed = Arc::new(DrainReads::default());
        owed.asked().insert(
            job(3),
            Asked {
                class: ExecClass::Background,
                requester: Some(Arc::new(process)),
                readers: Vec::new(),
                unidentified: false,
            },
        );
        let watches = owed.begin(ends()).expect("the first drain");
        assert_eq!(watches.len(), 1, "job 3's requester is watched");
        for watch in watches {
            Arc::clone(&owed).watch(watch);
        }
        drop(reader.stdin.take());
        assert!(
            reader
                .wait_bounded()
                .await
                .expect("the reader exits")
                .success()
        );
        let settled = tokio::time::timeout(
            Duration::from_secs(60),
            owed.settle(Duration::from_secs(60 * 60)),
        )
        .await
        .expect("it settles once every process job 3 was owed to exited, not at its bound");
        assert_eq!(settled.abandoned, [job(3)]);
        assert!(settled.unread.is_empty());
        assert_eq!(
            settled.to_string(),
            "read by a client: []; every process they were owed to exited: [3]"
        );
    }

    /// A draining supervisor's report says what it waits for at each step, read back by the
    /// pid serving the socket. A report another pid wrote is a killed supervisor's and says
    /// nothing, and one that does not decode says so rather than vanishing.
    #[tokio::test]
    async fn a_drain_report_names_what_the_supervisor_serving_the_socket_waits_for() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-drain-report-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&root).unwrap();
        let socket = root.join("0123.sock");
        let report = DrainReport::beside(&socket);
        let ours = u64::from(std::process::id());
        assert_eq!(DrainReport::read(&socket, ours), None, "nothing published");

        cowshed_os::fsio::take_syncs();
        report.publish(DrainWait::Jobs {
            running: vec![job(12), job(13)],
            ended: vec![RequesterGone {
                job: job(12),
                requester: 4242,
                observed: ExitObserved::Watched,
            }],
        });
        assert_eq!(
            cowshed_os::fsio::take_syncs(),
            cowshed_os::fsio::Syncs::default(),
            "the PID-bound live drain diagnostic requests no durability barriers"
        );
        assert_eq!(
            DrainReport::read(&socket, ours).as_deref(),
            Some(
                "it drains: it waits for jobs 12, 13 to end; it ended resident job 12, whose \
                 requester pid 4242 exited (its exit watch reported its exit)"
            )
        );
        assert_eq!(
            DrainReport::read(&socket, ours + 1),
            None,
            "another process's report"
        );

        let owed = DrainReads::default();
        unseen(&owed, 7024);
        unseen(&owed, 7025);
        owed.begin(ends());
        owed.record(job(7025), |owed| owed.end = true);
        owed.reading(&report);
        assert_eq!(
            DrainReport::read(&socket, ours).as_deref(),
            Some(
                "it drains: its jobs ended, and it waits until a client reads the ends of jobs \
                 7024 or every process they are owed to has exited, or 30 minutes pass with no \
                 read of them"
            )
        );

        report.stuck(
            "retire",
            &CowshedError::internal("the Nx daemon record is unwritable"),
        );
        let stuck = DrainReport::read(&socket, ours).unwrap();
        assert!(
            stuck.starts_with("its retirement failed to retire (")
                && stuck.ends_with("so it serves until it is killed"),
            "{stuck}"
        );

        report.withdraw();
        assert_eq!(
            DrainReport::read(&socket, ours),
            None,
            "withdrawn on retirement"
        );
        std::fs::write(socket.with_extension("drain"), b"{\"pid\":").unwrap();
        assert!(
            DrainReport::read(&socket, ours).is_some_and(|said| said.contains("does not decode")),
            "a torn report says so"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod call_deadline_tests {
    use super::*;

    use std::time::Duration;

    fn job(state: JobState, members: Option<usize>) -> JobPulse {
        JobPulse {
            job_id: JobId::new(7).unwrap(),
            state,
            members,
        }
    }

    #[test]
    fn only_a_job_with_no_process_left_and_no_end_is_stuck() {
        for (state, members, advances) in [
            (JobState::Queued, None, true),
            (JobState::Running, None, true),
            (JobState::Running, Some(3), true),
            (JobState::Running, Some(0), false),
            (JobState::Draining, Some(1), true),
            (JobState::Draining, Some(0), false),
            (JobState::Exited, Some(0), true),
            (JobState::Signaled, Some(0), true),
            (JobState::Killed, Some(0), true),
            (JobState::OutputLimit, Some(0), true),
            (JobState::Failed, None, true),
        ] {
            assert_eq!(
                job(state, members).advances(),
                advances,
                "{state:?} {members:?}"
            );
        }
    }

    /// The client half of a call, waiting on its answer. The halves are in memory: a kernel
    /// socket's readiness arrives outside the runtime, so paused time could pass a bound before
    /// the runtime saw bytes already sent.
    fn waiting(client: tokio::io::DuplexStream) -> PeerStream<tokio::io::DuplexStream> {
        let mut stream = PeerStream::connected(
            client,
            deadline::Peer::unix(SUPERVISOR, Path::new("/s.sock")),
            deadline::start("wait", deadline::CONTROL),
        );
        stream.hold(deadline::idle("wait", deadline::HEARTBEAT_SILENCE));
        stream
    }

    /// An actor between its turns.
    fn idle() -> ActorPulse {
        ActorPulse {
            turns: 1,
            busy: None,
        }
    }

    async fn beat(
        server: &mut tokio::io::DuplexStream,
        seq: u64,
        pulse: Option<JobPulse>,
    ) -> Result<()> {
        beat_with(server, seq, pulse, idle()).await
    }

    async fn beat_with(
        server: &mut tokio::io::DuplexStream,
        seq: u64,
        job: Option<JobPulse>,
        actor: ActorPulse,
    ) -> Result<()> {
        tokio::time::sleep(deadline::HEARTBEAT).await;
        write_json(
            server,
            &Response::Progress(ProgressWire { seq, job, actor }),
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_that_takes_a_minute_arrives_while_the_job_runs() {
        let (client, mut server) = tokio::io::duplex(4096);
        let supervisor = tokio::spawn(async move {
            for seq in 1..=12 {
                beat(&mut server, seq, Some(job(JobState::Running, Some(2))))
                    .await
                    .unwrap();
            }
            let value = serde_json::json!({ "done": true });
            write_json(&mut server, &Response::Ok { value, bytes: 0 })
                .await
                .unwrap();
            server
        });
        let started = tokio::time::Instant::now();
        let (value, raw) = answer_of(&mut waiting(client)).await.unwrap();
        assert_eq!(value, serde_json::json!({ "done": true }));
        assert!(raw.is_empty());
        assert_eq!(started.elapsed(), Duration::from_secs(60));
        drop(supervisor.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn a_supervisor_that_takes_the_call_and_falls_silent_is_named() {
        let (client, server) = tokio::io::duplex(4096);
        let error = answer_of(&mut waiting(client)).await.unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::EnvironmentMissing);
        assert!(
            error.message.starts_with(
                "SILENT: wait: workspace supervisor at /s.sock showed no progress for 15s"
            ),
            "{}",
            error.message
        );
        drop(server);
    }

    /// A job's pulse is as of its actor's last completed turn. An idle actor's pulse is the whole
    /// story; a turn still running when the stall is judged is named with the job, since it may
    /// be the very turn that ends it.
    #[tokio::test(start_paused = true)]
    async fn a_job_whose_processes_ended_without_its_end_is_named_stuck() {
        for (turn, named) in [
            (None, ""),
            (Some("exited"), "; the actor's exited turn has run 20000 ms"),
        ] {
            let (client, mut server) = tokio::io::duplex(4096);
            let actor = move |seq: u64| ActorPulse {
                turns: 1,
                busy: turn.map(|turn: &str| TurnBusy {
                    turn: turn.to_owned(),
                    for_ms: seq * 5000,
                }),
            };
            let supervisor = tokio::spawn(async move {
                beat_with(
                    &mut server,
                    1,
                    Some(job(JobState::Running, Some(1))),
                    actor(1),
                )
                .await
                .unwrap();
                // Heartbeats go on, about a job stuck draining, until the waiter hangs up.
                let mut seq = 2;
                while beat_with(
                    &mut server,
                    seq,
                    Some(job(JobState::Draining, Some(0))),
                    actor(seq),
                )
                .await
                .is_ok()
                {
                    seq += 1;
                }
                seq
            });
            let error = answer_of(&mut waiting(client)).await.unwrap_err();
            let expected = format!(
                "SILENT: wait: job 7 (Draining with no process of its group running{named}) of \
                 workspace supervisor at /s.sock showed no progress for 15s"
            );
            assert!(error.message.starts_with(&expected), "{}", error.message);
            // The job last advanced at heartbeat 1 (5 s); the stall is named at heartbeat 4
            // (20 s), 15 s later, and the waiter hangs up, so heartbeat 5 finds no one.
            assert_eq!(supervisor.await.unwrap(), 5);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_supervisor_that_ends_the_call_unanswered_loses_it() {
        let (client, server) = tokio::io::duplex(4096);
        drop(server);
        let error = answer_of(&mut waiting(client)).await.unwrap_err();
        assert!(
            error.message.starts_with("PEER_LOST: wait"),
            "{}",
            error.message
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_server_says_a_slow_answer_is_coming_and_then_gives_it() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let answered = tokio::spawn(async move {
            heartbeating(
                &mut server,
                || {
                    Ok(Pulse {
                        job: None,
                        actor: idle(),
                    })
                },
                async {
                    tokio::time::sleep(Duration::from_secs(12)).await;
                    42
                },
            )
            .await
        });
        for seq in 1..=2 {
            let Response::Progress(pulse) = read_json(&mut client).await.unwrap() else {
                panic!("a heartbeat comes before the answer");
            };
            assert_eq!(
                pulse,
                ProgressWire {
                    seq,
                    job: None,
                    actor: idle()
                }
            );
        }
        assert_eq!(answered.await.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_that_fails_writes_no_heartbeat_and_never_drops_the_answer() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let answered = tokio::spawn(async move {
            let probe = || -> Result<Pulse> { Err(protocol_error("the actor stopped")) };
            heartbeating(&mut server, probe, async {
                tokio::time::sleep(Duration::from_secs(12)).await;
                42
            })
            .await
        });
        assert_eq!(answered.await.unwrap(), 42);
        let mut nothing = Vec::new();
        client.read_to_end(&mut nothing).await.unwrap();
        assert!(nothing.is_empty(), "no heartbeat frame was written");
    }
}

#[cfg(test)]
mod held_turn_tests {
    use super::*;

    use crate::api::dto::{
        CommandArg, ExecClass, ExecCommand, ExecRequest, ExecScope, OutputPublication,
        RunSandboxMode, StdinSource,
    };
    use crate::runtime::supervisor::{
        ArtifactIndex, ArtifactSeal, ArtifactSink, ArtifactWrite, CommitmentDraft, CommitmentSink,
        NxDaemonRetirement, ProcessEvent, ProcessSpawnRequest, RunningProcess, SpawnSink,
        WorkspaceSupervisor, WorkspaceSupervisorConfig,
    };
    use crate::storage::job_artifact::{
        AdmissionLookup, JobAdmission, JobEnding, JobScheduling, SealedJobReader, StreamKind,
    };

    /// An artifact store whose admission holds its caller until the test lets it go, as a
    /// stalled APFS container held the observed `stat`s and `write`s, and then records nothing.
    struct HeldAdmission {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl ArtifactIndex for HeldAdmission {
        fn next_job_id(&self) -> Result<JobId> {
            Ok(JobId::new(1).expect("a job id"))
        }

        fn admitted(&self, _key: &crate::api::dto::AdmissionKey) -> AdmissionLookup<'_> {
            AdmissionLookup::Absent
        }
    }

    fn unrecorded() -> CowshedError {
        CowshedError::internal("the held admission was released unrecorded")
    }

    impl ArtifactSink for HeldAdmission {
        fn admit(
            &mut self,
            _job_id: JobId,
            _grant_revision: u64,
            _command: &ExecCommand,
            _admission: Option<JobAdmission>,
            _scheduling: JobScheduling,
        ) -> Result<()> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            // Ends when the test lets it go, or when the test ends and drops its sender.
            let _ = self.release.recv();
            Err(unrecorded())
        }

        fn prepare_background(&mut self, _job_id: JobId) -> Result<()> {
            Err(unrecorded())
        }

        fn write(
            &mut self,
            _job_id: JobId,
            _stream: StreamKind,
            _bytes: &[u8],
        ) -> Result<ArtifactWrite> {
            Err(unrecorded())
        }

        fn seal(
            &mut self,
            _job_id: JobId,
            _ending: JobEnding,
            _stdout_copy: Option<OutputPublication>,
            _stderr_copy: Option<OutputPublication>,
        ) -> Result<ArtifactSeal> {
            Err(unrecorded())
        }

        fn sealed(&mut self, _job_id: JobId) -> Result<Option<SealedJobReader>> {
            Ok(None)
        }
    }

    struct NoSpawn;

    #[async_trait::async_trait]
    impl SpawnSink for NoSpawn {
        async fn spawn(
            &mut self,
            request: ProcessSpawnRequest,
            _events: mpsc::Sender<ProcessEvent>,
        ) -> Result<Box<dyn RunningProcess>> {
            Err(CowshedError::internal(format!(
                "job {} reached a spawn this test admits none to",
                request.job_id.get()
            )))
        }
    }

    struct NoCommitments;

    #[async_trait::async_trait]
    impl CommitmentSink for NoCommitments {
        async fn record(&mut self, _draft: CommitmentDraft) -> Result<()> {
            Ok(())
        }
    }

    /// The test's workspace, removed however the test ends.
    struct Workspace(std::path::PathBuf);

    impl Drop for Workspace {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0) {
                cowshed_os::eprintln!(
                    "cowshed: cannot remove the test workspace {}: {error}",
                    self.0.display()
                );
            }
        }
    }
    /// The actor's turn, as the heartbeat `client` reads next names it.
    async fn heartbeat(client: &mut tokio::io::DuplexStream) -> (u64, ActorPulse) {
        let Response::Progress(progress) = read_json(client).await.expect("a heartbeat frame")
        else {
            panic!("a heartbeat, not an answer");
        };
        (progress.seq, progress.actor)
    }

    /// A turn held on the blocking pool keeps the supervisor's heartbeats going: the thread the
    /// actor shares with every heartbeat writer is free while the turn waits, each heartbeat
    /// reads what the actor published, names the held turn and shows no turn completed while it
    /// is held, and the turn's end is published once it is let go.
    #[tokio::test]
    async fn a_turn_held_on_the_blocking_pool_is_named_in_every_heartbeat() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-held-turn-{}",
            uuid::Uuid::new_v4().simple()
        ));
        // A minted workspace holds `.cowshed`, where its supervisor publishes the host environment.
        std::fs::create_dir_all(root.join(".cowshed")).expect("create the workspace");
        // Capability detection refuses a directory that resolves outside the workspace, and
        // `/var/folders` resolves into `/private/var`.
        let root = std::fs::canonicalize(&root).expect("the canonical workspace");
        let _workspace = Workspace(root.clone());
        let defaults = WorkspaceSupervisorConfig::default();
        let config = WorkspaceSupervisorConfig {
            workspace_root: root.clone(),
            default_cwd: None,
            sandbox: crate::sandbox::SandboxConfig {
                workspace_mount: root.clone(),
                ..defaults.sandbox
            },
            memory_pressure: crate::host_pressure::Source::calm(),
            ..defaults
        };
        let (entered, held) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let supervisor = WorkspaceSupervisor::start_with_sinks(
            config,
            Box::new(NoSpawn),
            Box::new(HeldAdmission {
                entered: Some(entered),
                release: released,
            }),
            Box::new(NoCommitments),
        )
        .expect("start the supervisor");
        let exec = tokio::spawn({
            let supervisor = supervisor.clone();
            async move {
                let request = ExecRequest {
                    command: ExecCommand::Argv(vec![CommandArg::new("held")]),
                    cwd: None,
                    mode: RunSandboxMode::ReadWrite,
                    env: std::collections::HashMap::new(),
                    trace: None,
                    stdin: StdinSource::Empty,
                    stdout_copy: None,
                    stderr_copy: None,
                    admission_key: None,
                    class: ExecClass::Background,
                    want: Some(std::num::NonZeroUsize::MIN),
                    scope: ExecScope::Workspace,
                };
                supervisor
                    .exec(None, None, request, AdmissionSource::Worker)
                    .await
            }
        });
        held.await.expect("the admission reached the store");

        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut beats = Heartbeat::new(|| pulse(&supervisor, None));
        beats.beat(&mut server).await.expect("write a heartbeat");
        beats
            .beat(&mut server)
            .await
            .expect("write a second heartbeat");
        let (first, held_at) = heartbeat(&mut client).await;
        let (second, still) = heartbeat(&mut client).await;
        assert_eq!((first, second), (1, 2));
        let (Some(held_turn), Some(still_turn)) = (&held_at.busy, &still.busy) else {
            panic!("a held turn is named: {held_at:?}, then {still:?}");
        };
        assert_eq!(
            (held_turn.turn.as_str(), still_turn.turn.as_str()),
            ("exec", "exec")
        );
        assert!(
            still_turn.for_ms >= held_turn.for_ms,
            "{held_at:?}, then {still:?}"
        );
        assert_eq!(
            still.turns, held_at.turns,
            "no turn completes while one is held"
        );

        release.send(()).expect("let the admission go");
        let refused = exec
            .await
            .expect("the exec call")
            .expect_err("the released admission recorded nothing");
        assert_eq!(refused.message, unrecorded().message);
        // Answered by a later turn, so the held one has ended and published its end.
        supervisor
            .current_authority()
            .await
            .expect("a later turn answers");
        let after = pulse(&supervisor, None).expect("the actor's pulse");
        assert!(
            after.actor.turns > held_at.turns,
            "{after:?} after {held_at:?}"
        );

        supervisor
            .retire(NxDaemonRetirement::Recorded)
            .await
            .expect("retire the supervisor");
    }
}
