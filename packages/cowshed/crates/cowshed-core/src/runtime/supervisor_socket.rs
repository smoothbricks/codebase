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

use super::process_stream::JobProcessStream;
use super::supervisor::{
    CheckpointBarrier, Command, LogChunk, NxDaemonRetirement, RetireWhen, SessionSnapshot,
    SessionToken, WorkspaceAuthoritySnapshot, WorkspaceSupervisorHandle,
};
use crate::api::dto::{
    AdmissionKey, CommandArg, ExecClass, ExecCommand, ExecRequest, ExecScope, JobId,
    JobJournalCursor, JobTailLimits, OutputPublication, RunSandboxMode, ScriptCommand,
    Sha256Digest, StdinSource, TraceContext, WorkspacePath,
};
use crate::api::process::JobProcessEvent;
use crate::api::resources::SampleInterval;
use crate::api::server::codec::Demand;
use crate::error::{CowshedError, Result};
use crate::storage::job_artifact::StreamKind;
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

/// How long a supervisor may take to answer hello.
const HELLO_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

/// Bound on one JSON frame: an exec request carries at most a 1 MiB command plus its
/// environment; a job list is bounded by the jobs one supervisor keeps resident.
const MAX_JSON_FRAME: usize = 16 * 1024 * 1024;
/// Bound on one raw frame: inline stdin rides here whole.
const MAX_BYTES_FRAME: usize = 64 * 1024 * 1024;
/// Chunk size a stream stdin is forwarded in; the supervisor's own write bound.
const STREAM_CHUNK: usize = 64 * 1024;
/// Bounded queue between a forwarded stdin stream and the job that reads it.
const STREAM_DEPTH: usize = 4;

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
        .join(format!("{}.sock", &digest[..32]))
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
    Checkpoint {
        checkpoint_id: String,
    },
    #[serde(rename_all = "camelCase")]
    Retire {
        nx_daemon: NxDaemonRetirement,
        when: RetireWhen,
    },
    /// Name the build volume a land or refork moved the checkout's link onto.
    #[serde(rename_all = "camelCase")]
    NameBuildVolume {
        build_volume: Option<PathBuf>,
    },
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
    want: NonZeroUsize,
    scope: ExecScope,
    /// The requester's live CPU pool ([`crate::runtime::job_cpu::JobCpuAsk::parent`]); absent
    /// when the requester holds none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cpu_parent: Option<String>,
}

/// The controller route that admitted an exec. The route is the authority for the gate class and
/// the host-gate scope (05_gateway.md "Host CPU budget", 07_api.md "Capability split"): no request
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
    /// interactive and either resident's work in the workspace sandbox and nothing else --
    /// interactive buys a band and no token, and a resident holds no token, so neither claims
    /// anything a worker may not -- and anything more is refused as the typed class-not-permitted
    /// error, before any record, process or gateway request exists.
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
struct CheckpointWire {
    checkpoint_id: String,
    barrier_id: u64,
    manifest_batch_sha256: Sha256Digest,
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
    let bytes = read_frame(stream, MAX_BYTES_FRAME).await?;
    if bytes.len() != length {
        return Err(protocol_error(format!(
            "raw frame is {} bytes, announced {length}",
            bytes.len()
        )));
    }
    Ok(Bytes::from(bytes))
}

// ---------------------------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------------------------

/// Writers of the stdin streams jobs of this supervisor are still reading.
type Streams = Arc<Mutex<BTreeMap<JobId, mpsc::Sender<io::Result<Bytes>>>>>;

/// How a served supervisor's process takes an `advance` request: it alone can read the
/// workspace's grants and compile the profile for them.
pub type Advances = mpsc::Sender<oneshot::Sender<Result<WorkspaceAuthoritySnapshot>>>;

/// Exclusive ownership of a workspace's supervisor socket, held while serving it or changing
/// its substrate. The persistent file lease serializes binders; an older build is drained before
/// a new build binds, and the occupant probe still refuses any live inherited listener. The
/// listener and the lease are fenced: once either is closed, no child still being spawned holds
/// it, so the next binder finds the socket stopped and the lease free (`fork_lock`).
#[derive(Debug)]
#[must_use = "hold the socket until serving or the workspace mutation has finished"]
pub struct BoundSocket {
    pub(super) listener: Fenced<UnixListener>,
    _lease: Fenced<std::fs::File>,
    /// Where its socket file is: the workspace's socket path once published, a private name
    /// before.
    path: PathBuf,
    instance: Instance,
}

impl BoundSocket {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BoundSocket {
    fn drop(&mut self) {
        // The listener and lease remain held here. Remove only this socket's own file.
        match Instance::at(&self.path) {
            Ok(Some(instance)) if instance == self.instance => {
                if let Err(error) = std::fs::remove_file(&self.path) {
                    eprintln!(
                        "cowshed: cannot remove retired supervisor socket {}: {error}",
                        self.path.display()
                    );
                }
            }
            Ok(_) => {}
            Err(error) => eprintln!(
                "cowshed: cannot inspect retired supervisor socket {}: {error}",
                self.path.display()
            ),
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

/// What holds the file at a socket path, as far as the kernel shows.
#[derive(Debug)]
enum Occupant {
    /// Nothing is there.
    Absent,
    /// A socket file no socket is attached to: nothing will accept on it again.
    Stopped,
    /// A socket file a socket is still attached to, whoever holds it.
    Live,
}

/// Whether a socket is still attached to the socket file at `path`. Neither a stream connection
/// nor its listener's process says so. Measured on Darwin, a live listener whose queue is full
/// refuses a stream connection, as does a socket that is bound and not listening; a listener
/// still accepts after the process that created it exited, from a child that inherited it, and
/// the kernel's peer identity names that exited creator. A datagram connection is refused only
/// when no socket at all is attached to the file -- every descriptor of it closed, in every
/// process -- and otherwise fails as the wrong type or, to a datagram socket, is made: Darwin's
/// `unp_connect` and Linux's `unix_find_bsd` find the attached socket before they compare types,
/// and neither looks at a queue. Measured on Darwin, a killed listener's file is detached by the
/// time its NOTE_EXIT is delivered. No bind attaches a socket to a file that already exists, so
/// a file found detached stays detached. Any other answer is an error, never stopped.
fn occupant(path: &Path) -> io::Result<Occupant> {
    if Instance::at(path)?.is_none() {
        return Ok(Occupant::Absent);
    }
    match std::os::unix::net::UnixDatagram::unbound()?.connect(path) {
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => Ok(Occupant::Stopped),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Occupant::Absent),
        Err(error) if error.raw_os_error() == Some(libc::EPROTOTYPE) => Ok(Occupant::Live),
        Ok(()) => Ok(Occupant::Live),
        Err(error) => Err(error),
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

/// Bind the supervisor's socket at `path`: mode `0600` in a directory only this user can enter.
/// The lease serializes binders; old builds must drain first. Only a socket the kernel proves
/// unattached is removed before the new private listener is published.
pub async fn bind(path: &Path) -> Result<BoundSocket> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
    let directory = path.parent().ok_or_else(|| {
        CowshedError::internal(format!(
            "supervisor socket {} has no parent",
            path.display()
        ))
    })?;
    let io = |what: &str, error: io::Error| socket_error(path, what, error);
    std::fs::create_dir_all(directory).map_err(|error| io("create the directory of", error))?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
        .map_err(|error| io("restrict the directory of", error))?;
    let lock_path = path.with_extension("sock.lock");
    let lease = Fenced::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&lock_path)
            .map_err(|error| io("open the socket lease of", error))?,
    );
    match lease.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(CowshedError::conflict(
                format!(
                    "a supervisor or lifecycle operation owns {}",
                    path.display()
                ),
                "wait for that operation to finish; do not unlink its socket or lock file",
            ));
        }
        Err(std::fs::TryLockError::Error(error)) => {
            return Err(io("lock the socket lease of", error));
        }
    }
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
                eprintln!(
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
        _lease: lease,
        path: fresh,
        instance,
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
    socket: BoundSocket,
    supervisor: WorkspaceSupervisorHandle,
    advances: Option<Advances>,
    feed: Option<super::commitment_feed::CommitmentFeed>,
) -> Result<()> {
    let streams: Streams = Arc::default();
    let retired = Arc::new(tokio::sync::Notify::new());
    let owed = Arc::new(DrainReads::default());
    let (release, mut released) = mpsc::channel::<(UnixStream, serde_json::Value, Bytes)>(1);
    loop {
        let (stream, _) = tokio::select! {
            accepted = socket.listener.accept() => accepted.map_err(|error| {
                CowshedError::environment_missing(
                    format!("workspace supervisor socket stopped accepting: {error}"),
                    "retry; cowshed restarts the workspace supervisor",
                )
            })?,
            response = released.recv() => {
                let (mut stream, value, bytes) = response.ok_or_else(|| {
                    CowshedError::internal("supervisor retirement reply lane closed")
                })?;
                // A successful retirement reply proves replacement admission can acquire the
                // socket fence already: no acknowledgement-before-listener-release race.
                drop(socket);
                write_json(&mut stream, &Response::Ok { value, bytes: bytes.len() }).await?;
                if !bytes.is_empty() {
                    write_frame(&mut stream, &bytes, MAX_BYTES_FRAME).await?;
                }
                return Ok(());
            }
            () = retired.notified() => return Ok(()),
        };
        // Any call is a client still there: a draining supervisor's idle bound starts over.
        owed.called.notify_one();
        let supervisor = supervisor.clone();
        let streams = Arc::clone(&streams);
        let retired = Arc::clone(&retired);
        let owed = Arc::clone(&owed);
        let advances = advances.clone();
        let feed = feed.clone();
        let release = release.clone();
        let socket_path = socket.path.clone();
        tokio::spawn(async move {
            match serve_call(
                stream,
                &supervisor,
                &streams,
                advances.as_ref(),
                feed.as_ref(),
                &owed,
            )
            .await
            {
                Ok(Retirement::Retired {
                    stream,
                    value,
                    bytes,
                }) => {
                    if release.send((stream, value, bytes)).await.is_err() {
                        eprintln!(
                            "cowshed: retired supervisor socket owner ended before its reply"
                        );
                    }
                }
                Ok(Retirement::Draining) => {
                    // Under the authority it holds now, which an advance may have moved.
                    let Ok(authority) = supervisor.current_authority().await else {
                        return;
                    };
                    let current = supervisor.with_authority(authority);
                    let Ok(jobs) = current.list().await else {
                        return;
                    };
                    // A second `drain` finds the first one's reckoning under way.
                    let running = jobs
                        .iter()
                        .filter(|job| !job.state.is_terminal())
                        .map(|job| job.job_id);
                    if !owed.begin(running) {
                        return;
                    }
                    if current.quiesce().await.is_err() {
                        return;
                    }
                    let settled = owed.settle(super::project::SUPERVISOR_IDLE).await;
                    eprintln!(
                        "cowshed: workspace supervisor {} of build {} retires after draining; {settled}",
                        socket_path.display(),
                        BuildId::current().map_or("(unnamed)", BuildId::as_str),
                    );
                    if current.retire(NxDaemonRetirement::Recorded).await.is_ok() {
                        retired.notify_one();
                    }
                }
                Ok(Retirement::Serving) | Err(_) => {}
            }
        });
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

/// What a draining supervisor still owes its clients (11_shell.md "Draining a supervisor of
/// another build"): each job that ran when it was asked to drain, until a client has read its end
/// and every stream it was reading to EOF. The next supervisor is another build, whose protocol
/// the job's client cannot speak, so the end is read from this one or not at all.
#[derive(Default)]
struct DrainReads {
    /// `None` until the supervisor drains.
    owed: std::sync::Mutex<Option<BTreeMap<JobId, Owed>>>,
    /// Every call, and every read: progress to check, and the idle bound starting over.
    called: tokio::sync::Notify,
}

/// One job's reads, as a client of a draining supervisor makes them.
#[derive(Default)]
struct Owed {
    /// A `wait`, `hold` or `sealed` answer carried the job's end.
    end: bool,
    /// Per stream (stdout, stderr): a `logRead` was answered, and whether one answered EOF.
    /// A client that reads no stream owes none.
    reading: [bool; 2],
    eof: [bool; 2],
}

impl Owed {
    fn read(&self) -> bool {
        self.end && (0..2).all(|stream| !self.reading[stream] || self.eof[stream])
    }
}

/// Which jobs' ends were read before a draining supervisor retired, and which it gave up on.
struct Settled {
    read: Vec<JobId>,
    unread: Vec<JobId>,
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
        if !self.unread.is_empty() {
            write!(
                formatter,
                "; unread after {} minutes with no client call: [{}]",
                self.bound.as_secs() / 60,
                ids(&self.unread)
            )?;
        }
        Ok(())
    }
}

impl DrainReads {
    fn owed(&self) -> std::sync::MutexGuard<'_, Option<BTreeMap<JobId, Owed>>> {
        self.owed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Start owing `running`; `false` when a drain already began.
    fn begin(&self, running: impl Iterator<Item = JobId>) -> bool {
        let mut owed = self.owed();
        if owed.is_some() {
            return false;
        }
        *owed = Some(running.map(|job| (job, Owed::default())).collect());
        true
    }

    /// Record what a served call told its client about `job`, while draining.
    fn record(&self, job: JobId, update: impl FnOnce(&mut Owed)) {
        if let Some(entry) = self.owed().as_mut().and_then(|owed| owed.get_mut(&job)) {
            update(entry);
        }
        self.called.notify_one();
    }

    /// Wait until every owed job is read, or until `bound` passes with no client call at all.
    async fn settle(&self, bound: std::time::Duration) -> Settled {
        loop {
            let all_read = self
                .owed()
                .as_ref()
                .is_none_or(|owed| owed.values().all(Owed::read));
            if all_read {
                break;
            }
            if tokio::time::timeout(bound, self.called.notified())
                .await
                .is_err()
            {
                break;
            }
        }
        let owed = self.owed();
        let (read, unread): (Vec<_>, Vec<_>) =
            owed.iter().flatten().partition(|(_, owed)| owed.read());
        Settled {
            read: read.into_iter().map(|(job, _)| *job).collect(),
            unread: unread.into_iter().map(|(job, _)| *job).collect(),
            bound,
        }
    }
}

async fn serve_call(
    mut stream: UnixStream,
    supervisor: &WorkspaceSupervisorHandle,
    streams: &Streams,
    advances: Option<&Advances>,
    feed: Option<&super::commitment_feed::CommitmentFeed>,
    owed: &DrainReads,
) -> Result<Retirement> {
    verify_peer(&stream)?;
    let request: Request = read_json(&mut stream).await?;
    let (authority, call, length) = match request {
        Request::Hello => {
            let authority = supervisor.current_authority().await?;
            let value = to_value(&HelloWire {
                build: BuildId::current()?.clone(),
                authority: (&authority).into(),
                pid: std::process::id(),
            })?;
            write_json(&mut stream, &Response::Ok { value, bytes: 0 }).await?;
            return Ok(Retirement::Serving);
        }
        Request::Drain => {
            write_json(
                &mut stream,
                &Response::Ok {
                    value: to_value(&std::process::id())?,
                    bytes: 0,
                },
            )
            .await?;
            return Ok(Retirement::Draining);
        }
        Request::Commitments { after } => {
            let response = match feed {
                Some(feed) => Response::Ok {
                    value: to_value(&feed.since(after, COMMITMENT_WAIT).await)?,
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
                        Ok(()) => answer.await.unwrap_or_else(|_| {
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
    let (value, bytes) = match answer(&caller, streams, *call, payload).await {
        Ok(answer) => answer,
        Err(error) => {
            write_json(&mut stream, &Response::Err(error)).await?;
            return Ok(Retirement::Serving);
        }
    };
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
    relay_process_events(stream, events).await
}

/// Hold `job_id` for its holder: answer `null` once the hold is in place, then the job's
/// terminal `JobInfo` when it ends, or `null` again when the holder releases it. A holder that
/// closes the connection first — it exited, or was killed, `SIGKILL` included, since the kernel
/// closes its socket — has left a job nobody waits for, and the job is killed
/// (05_gateway.md, "A job lives with its requester").
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
    tokio::select! {
        biased;
        ended = caller.wait(job_id) => {
            let delivered_end = ended.is_ok();
            let response = match ended {
                Ok(info) => Response::Ok { value: to_value(&info)?, bytes: 0 },
                Err(error) => Response::Err(error),
            };
            let written = write_json(&mut writer, &response).await;
            if delivered_end && written.is_ok() {
                owed.record(job_id, |owed| owed.end = true);
            }
            written
        }
        demand = read_json::<HoldDemand>(&mut reader) => match demand {
            Ok(HoldDemand::Release) => {
                write_json(
                    &mut writer,
                    &Response::Ok { value: serde_json::Value::Null, bytes: 0 },
                )
                .await
            }
            Err(_) => kill_unheld(supervisor, job_id).await,
        },
    }
}

/// Kill `job_id` because its holder went away while it ran. The job may have been advanced onto
/// a newer grant revision meanwhile, so it is reached under the authority served now.
async fn kill_unheld(supervisor: &WorkspaceSupervisorHandle, job_id: JobId) -> Result<()> {
    let current = supervisor.with_authority(supervisor.current_authority().await?);
    if current.info(job_id).await?.state.is_terminal() {
        return Ok(());
    }
    eprintln!(
        "cowshed: job {} lost its holder before it ended; killing it, since nothing waits for it",
        job_id.get()
    );
    current.kill(job_id).await
}

async fn relay_process_events(stream: UnixStream, mut events: JobProcessStream) -> Result<()> {
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
            let event = tokio::select! {
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
                event = events.next() => event,
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

async fn answer(
    supervisor: &WorkspaceSupervisorHandle,
    streams: &Streams,
    call: Call,
    payload: Bytes,
) -> Result<(serde_json::Value, Bytes)> {
    let session =
        |wire: SessionWire| SessionToken::remote(supervisor.snapshot(), wire.identity, wire.name);
    let unit = || to_value(&());
    Ok(match call {
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
                    request.cpu_parent,
                )
                .await?;
            if let Some(writer) = writer {
                lock(streams).insert(job_id, writer);
            }
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
        Call::Checkpoint { checkpoint_id } => {
            let barrier = supervisor.checkpoint_barrier(checkpoint_id).await?;
            (
                to_value(&CheckpointWire {
                    checkpoint_id: barrier.checkpoint_id,
                    barrier_id: barrier.barrier_id,
                    manifest_batch_sha256: barrier.manifest_batch_sha256,
                })?,
                Bytes::new(),
            )
        }
        Call::Retire { nx_daemon, when } => {
            match when {
                RetireWhen::Now => supervisor.retire(nx_daemon).await?,
                RetireWhen::AfterJobs => supervisor.retire_after_jobs(nx_daemon).await?,
                RetireWhen::IfIdle => supervisor.retire_if_idle(nx_daemon).await?,
            }
            (unit()?, Bytes::new())
        }
        Call::NameBuildVolume { build_volume } => {
            supervisor.name_build_volume(build_volume).await?;
            (unit()?, Bytes::new())
        }
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
    tokio::time::timeout(HELLO_BOUND, async {
        let mut stream = match UnixStream::connect(path).await {
            Ok(stream) => stream,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                return match occupant(path) {
                    Ok(Occupant::Absent | Occupant::Stopped) => Ok(None),
                    Ok(Occupant::Live) => Err(CowshedError::environment_missing(
                        format!(
                            "the workspace supervisor socket {} refuses connections while a \
                             socket is still attached to it: its queue is full, or its holder \
                             does not accept",
                            path.display()
                        ),
                        "retry once the operation holding it finishes; cowshed doctor --json",
                    )),
                    Err(error) => Err(unavailable(path, &error)),
                };
            }
            Err(error) => return Err(unavailable(path, &error)),
        };
        let (value, _) = match exchange_on(&mut stream, path, &Request::Hello, Bytes::new()).await {
            Ok(answer) => answer,
            Err(error) => {
                return match occupant(path) {
                    Ok(Occupant::Absent | Occupant::Stopped) => Ok(None),
                    Ok(Occupant::Live) | Err(_) => Err(error),
                };
            }
        };
        decode_hello(path, value).map(Some)
    })
    .await
    .map_err(|_| {
        CowshedError::environment_missing(
            format!(
                "the workspace supervisor at {} did not answer within {} seconds",
                path.display(),
                HELLO_BOUND.as_secs()
            ),
            "cowshed doctor --json",
        )
    })?
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
                format!(" (pid {pid})"),
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
    let (value, _) = exchange(path, &Request::Commitments { after }, Bytes::new()).await?;
    decode(value)
}

/// Tell the supervisor at `path` that every commitment through cursor `through` is forwarded.
pub async fn acknowledge_commitments(path: &Path, through: u64) -> Result<()> {
    exchange(
        path,
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
    pub exit: super::job_groups::ExitWatch,
}

impl Drained {
    pub fn pid(&self) -> u32 {
        self.process.pid().unsigned_abs()
    }
}

/// Ask the supervisor at `path` to admit nothing more and retire once its running jobs end,
/// whatever protocol it speaks otherwise; the process serving it.
pub async fn drain(path: &Path) -> Result<Drained> {
    let mut stream = UnixStream::connect(path)
        .await
        .map_err(|error| unavailable(path, &error))?;
    let process = super::job_groups::Process::of_socket_peer(&stream).map_err(|error| {
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
    let (value, _) = exchange_on(&mut stream, path, &Request::Drain, Bytes::new()).await?;
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
    eprintln!(
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
    eprintln!(
        "cowshed: the workspace supervisor at {} (pid {pid}) retired after {}s",
        path.display(),
        started.elapsed().as_secs()
    );
    Ok(())
}

/// Ask the supervisor at `path` to serve under its workspace's current grants; the authority
/// it serves afterwards.
pub async fn advance(path: &Path) -> Result<WorkspaceAuthoritySnapshot> {
    let (value, _) = exchange(path, &Request::Advance, Bytes::new()).await?;
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
    let request = Request::Call {
        authority,
        call: Box::new(call),
        bytes: payload.len(),
    };
    exchange(path, &request, payload).await
}

async fn exchange(
    path: &Path,
    request: &Request,
    payload: Bytes,
) -> Result<(serde_json::Value, Bytes)> {
    let mut stream = UnixStream::connect(path)
        .await
        .map_err(|error| unavailable(path, &error))?;
    exchange_on(&mut stream, path, request, payload).await
}

async fn exchange_on(
    stream: &mut UnixStream,
    path: &Path,
    request: &Request,
    payload: Bytes,
) -> Result<(serde_json::Value, Bytes)> {
    write_json(stream, request).await?;
    if !payload.is_empty() {
        write_frame(stream, &payload, MAX_BYTES_FRAME).await?;
    }
    match read_json::<Response>(stream).await {
        Ok(Response::Ok { value, bytes }) => Ok((value, read_bytes(stream, bytes).await?)),
        Ok(Response::Err(error)) => Err(error),
        // The supervisor went away mid-call: the call's outcome is unknown, which is what
        // "unavailable" says; the caller re-reads job state rather than assuming either way.
        Err(_) => Err(unavailable(
            path,
            &io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the supervisor closed the call",
            ),
        )),
    }
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
    let mut stream = UnixStream::connect(path)
        .await
        .map_err(|error| unavailable(path, &error))?;
    write_json(
        &mut stream,
        &Request::Call {
            authority: authority.into(),
            call: Box::new(Call::ProcessEvents { job_id, every }),
            bytes: 0,
        },
    )
    .await?;
    match read_json::<Response>(&mut stream).await? {
        Response::Err(error) => return Err(error),
        Response::Ok { value, bytes: 0 } if value.is_null() => {}
        Response::Ok { .. } => return Err(protocol_error("malformed process subscription answer")),
    }
    let (mut reader, mut writer) = stream.into_split();
    let (send, receive) = mpsc::channel(1);
    tokio::spawn(async move {
        loop {
            if let Err(error) = write_json(&mut writer, &Demand::Next).await {
                let _ = send.send(Err(error)).await;
                break;
            }
            let response = tokio::select! {
                biased;
                () = send.closed() => {
                    if let Err(error) = write_json(&mut writer, &Demand::Close).await {
                        eprintln!("cowshed: closing process observation: {error}");
                    }
                    break;
                }
                response = read_json::<Response>(&mut reader) => response,
            };
            let event = match response {
                Ok(Response::Ok { value, bytes: 0 }) => decode::<Option<JobProcessEvent>>(value),
                Ok(Response::Ok { .. }) => Err(protocol_error("process event carried raw bytes")),
                Ok(Response::Err(error)) | Err(error) => Err(error),
            };
            let ended = !matches!(event, Ok(Some(_)));
            match event {
                Ok(Some(event)) => {
                    if send.send(Ok(event)).await.is_err() {
                        if let Err(error) = write_json(&mut writer, &Demand::Close).await {
                            eprintln!("cowshed: closing process observation: {error}");
                        }
                        break;
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    let _ = send.send(Err(error)).await;
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
    stream: UnixStream,
}

impl JobHold {
    /// Let the held job outlive this hold. A job that ended meanwhile has already answered the
    /// hold with its terminal record, which releases it as well.
    pub async fn release(mut self) -> Result<()> {
        let sent = write_json(&mut self.stream, &HoldDemand::Release).await;
        match (read_json::<Response>(&mut self.stream).await, sent) {
            (Ok(Response::Ok { .. }), _) => Ok(()),
            (Ok(Response::Err(error)), _) | (Err(_), Err(error)) | (Err(error), Ok(())) => {
                Err(error)
            }
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
    let mut stream = UnixStream::connect(path)
        .await
        .map_err(|error| unavailable(path, &error))?;
    write_json(
        &mut stream,
        &Request::Call {
            authority: (&serving.authority).into(),
            call: Box::new(Call::Hold { job_id }),
            bytes: 0,
        },
    )
    .await?;
    match read_json::<Response>(&mut stream).await? {
        Response::Err(error) => Err(error),
        Response::Ok { value, bytes: 0 } if value.is_null() => Ok(JobHold { stream }),
        Response::Ok { .. } => Err(protocol_error("malformed hold answer")),
    }
}

async fn forward(path: Arc<PathBuf>, command: Command) {
    let path = path.as_path();
    match command {
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
            cpu_parent,
            reply,
        } => {
            forward_exec(
                path,
                authority,
                session,
                build_volume,
                *request,
                source,
                cpu_parent,
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
        Command::Checkpoint {
            authority,
            checkpoint_id,
            reply,
        } => {
            let result = call::<CheckpointWire>(
                path,
                &authority,
                Call::Checkpoint { checkpoint_id },
                Bytes::new(),
            )
            .await
            .map(|wire| CheckpointBarrier {
                checkpoint_id: wire.checkpoint_id,
                barrier_id: wire.barrier_id,
                manifest_batch_sha256: wire.manifest_batch_sha256,
            });
            let _ = reply.send(result);
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
    cpu_parent: Option<String>,
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
            cpu_parent,
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
        let relay = tokio::spawn(relay_process_events(server, JobProcessStream::new(receive)));
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

    #[tokio::test]
    async fn closing_an_unanswered_process_demand_drops_the_subscription() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let (events, receive) = mpsc::channel(1);
        let relay = tokio::spawn(relay_process_events(server, JobProcessStream::new(receive)));
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
        let relay = tokio::spawn(relay_process_events(server, JobProcessStream::new(receive)));
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

    fn path() -> PathBuf {
        PathBuf::from("/tmp")
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
        let client = UnixStream::connect(path).await.unwrap();
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
            UnixStream::connect(&path).await.unwrap_err().kind(),
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
            match std::os::unix::net::UnixStream::connect(&path) {
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

        owner.kill().await.unwrap();

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
                .wait()
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
        assert!(creator.wait().await.unwrap().success());
        let held = inode(&path);
        drop(
            UnixStream::connect(&path)
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
        assert_eq!(request.want.get(), 8);
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
    use std::time::Duration;

    use super::*;

    fn job(id: u64) -> JobId {
        JobId::new(id).expect("a job id")
    }

    /// A job is read once its end reached a client and every stream that client was reading
    /// reached EOF: the end alone leaves the tail of a relayed stream unread.
    #[tokio::test]
    async fn a_job_is_read_once_its_end_and_every_stream_being_read_reached_a_client() {
        let owed = DrainReads::default();
        assert!(owed.begin([job(1), job(2)].into_iter()));
        assert!(
            !owed.begin([job(3)].into_iter()),
            "a second drain owes nothing new"
        );
        owed.record(job(1), |owed| owed.reading[0] = true);
        owed.record(job(1), |owed| owed.end = true);
        owed.record(job(2), |owed| owed.end = true);
        let unread = |owed: &DrainReads| {
            owed.owed()
                .iter()
                .flatten()
                .filter(|(_, owed)| !owed.read())
                .map(|(job, _)| *job)
                .collect::<Vec<_>>()
        };
        assert_eq!(unread(&owed), [job(1)], "job 1's stdout tail is still owed");
        owed.record(job(1), |owed| owed.eof[0] = true);
        let settled = owed.settle(Duration::from_secs(60 * 60)).await;
        assert_eq!(settled.read, [job(1), job(2)]);
        assert!(settled.unread.is_empty());
        assert_eq!(settled.to_string(), "read by a client: [1, 2]");
    }

    /// A job nobody reads, a detached one, cannot keep a drained supervisor serving: past the
    /// bound with no client call it retires, and says which ends went unread.
    #[tokio::test(start_paused = true)]
    async fn an_end_nobody_reads_is_given_up_after_the_idle_bound_and_named() {
        let owed = DrainReads::default();
        owed.begin([job(7)].into_iter());
        let bound = Duration::from_secs(30 * 60);
        let settled = owed.settle(bound).await;
        assert!(settled.read.is_empty());
        assert_eq!(settled.unread, [job(7)]);
        assert_eq!(
            settled.to_string(),
            "read by a client: []; unread after 30 minutes with no client call: [7]"
        );
    }
}
