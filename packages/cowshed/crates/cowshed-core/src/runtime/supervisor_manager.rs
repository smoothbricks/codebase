//! The host's owner of workspace supervisor processes (11_shell.md "Supervisor").
//!
//! The gateway daemon runs one manager for the host. A controller asks it to ensure a
//! workspace's supervisor under the authority the controller needs; the manager answers with
//! the socket once a supervisor serves it. When nothing answers the workspace's socket it starts
//! `cowshed __workspace-supervisor <project-root> <workspace>` in a session of its own, so the
//! supervisor outlives the controller that asked for it and the daemon's own restarts, and it
//! watches every supervisor it started or, after a restart of its own, found still serving.
//!
//! Ensures of one workspace are serialized: two controllers asking at once get the same
//! supervisor, never two allocators.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt as _;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use super::supervisor::WorkspaceAuthoritySnapshot;
use super::supervisor_socket::{
    self, AuthorityWire, BuildId, protocol_error, read_json, verify_peer, write_json,
};
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::WorkspaceName;

/// How long a started supervisor may take to open its project, mount its workspace and answer.
const START_BOUND: Duration = Duration::from_secs(180);
/// How often a starting supervisor is asked whether it serves yet.
const START_POLL: Duration = Duration::from_millis(50);
/// Names the descriptor on which a supervisor the manager started says why it exits before it
/// serves ([`StartReport`]). A supervisor started by hand has none.
pub const START_REPORT_FD_ENV: &str = "COWSHED_SUPERVISOR_REPORT_FD";
/// The most a start report may hold; anything longer is not one.
const START_REPORT_LIMIT: u64 = 64 * 1024;
/// How long an exited supervisor's report may take to reach end of file. Its only writer is
/// gone, so only a descriptor leaked to a process it started could hold the pipe open.
const START_REPORT_BOUND: Duration = Duration::from_secs(2);

/// Where the host's manager listens.
pub fn manager_socket_path(store_root: &Path) -> PathBuf {
    store_root.join("run").join("manager.sock")
}

/// What starts a workspace's supervisor process.
pub trait SupervisorSpawner: Send + Sync + 'static {
    /// Start the supervisor of `workspace`, handing it `report`: the write end of the pipe on
    /// which it says why it exits, if it does, before it serves ([`StartReport`]). The spawner
    /// keeps no copy of it, so the manager reads the report to its end once the child exits.
    fn spawn(
        &self,
        project_root: &Path,
        workspace: &WorkspaceName,
        report: io::PipeWriter,
    ) -> io::Result<tokio::process::Child>;
}

/// Production: this same binary's hidden verb, in a new session, stdin closed, stdout discarded,
/// stderr to the daemon's own log, and the start report on the descriptor
/// [`START_REPORT_FD_ENV`] names.
#[derive(Clone, Debug)]
pub struct ProgramSpawner {
    executable: PathBuf,
    arguments: Vec<OsString>,
}

impl ProgramSpawner {
    pub fn new(executable: impl Into<PathBuf>, arguments: Vec<OsString>) -> Self {
        Self {
            executable: executable.into(),
            arguments,
        }
    }
}

impl SupervisorSpawner for ProgramSpawner {
    fn spawn(
        &self,
        project_root: &Path,
        workspace: &WorkspaceName,
        report: io::PipeWriter,
    ) -> io::Result<tokio::process::Child> {
        let report = OwnedFd::from(report);
        let report_fd = report.as_raw_fd();
        let mut command = tokio::process::Command::new(&self.executable);
        command
            .args(&self.arguments)
            .arg(project_root)
            .arg(workspace.as_str())
            .env(START_REPORT_FD_ENV, report_fd.to_string())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(false);
        // SAFETY: `setsid` and `fcntl` between fork and exec are async-signal-safe and touch only
        // this child. A session of its own takes the supervisor out of the daemon's process
        // group, which a service manager ends with the daemon. The report pipe is close-on-exec
        // in the daemon; clearing the flag on the child's copy of that same descriptor number is
        // what lets the supervisor alone inherit it.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::fcntl(report_fd, libc::F_SETFD, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn();
        // The child holds its own copy now; the manager's would keep the report from ending.
        drop(report);
        child
    }
}

/// The start report of a supervisor the manager started: the write end of its report pipe, or
/// nothing for a supervisor started by hand.
pub struct StartReport(Option<std::fs::File>);

impl StartReport {
    /// The report descriptor the manager handed this process, made close-on-exec so no process
    /// this one starts inherits it. Call before starting any.
    pub fn inherited() -> Self {
        Self::named(std::env::var(START_REPORT_FD_ENV).ok().as_deref())
    }

    /// The report pipe `value` names: an open pipe, never standard input, output or error.
    fn named(value: Option<&str>) -> Self {
        let Some(fd) = value
            .and_then(|value| value.parse::<RawFd>().ok())
            .filter(|&fd| fd > libc::STDERR_FILENO)
        else {
            return Self(None);
        };
        // SAFETY: `fstat` on a descriptor number is sound whether or not it is open; it fails
        // with EBADF when it is not, and `stat` is plain data it only writes.
        let is_pipe = unsafe {
            let mut stat = std::mem::zeroed::<libc::stat>();
            libc::fstat(fd, &mut stat) == 0 && stat.st_mode & libc::S_IFMT == libc::S_IFIFO
        };
        if !is_pipe {
            return Self(None);
        }
        // SAFETY: F_SETFD on an open descriptor changes only its own close-on-exec flag.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Self(None);
        }
        // SAFETY: the manager opened this pipe for this process alone and named it in the
        // environment, and it is open (checked above); nothing else in this process owns it.
        Self(Some(std::fs::File::from(unsafe {
            OwnedFd::from_raw_fd(fd)
        })))
    }

    /// Tell the manager why this supervisor ends before serving. A manager that stopped reading
    /// (the supervisor served, or the daemon restarted) answers `BrokenPipe`.
    pub fn send(self, error: &CowshedError) -> io::Result<()> {
        let Some(mut report) = self.0 else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(error).map_err(io::Error::other)?;
        report.write_all(&bytes)
    }
}

/// Why the supervisor that wrote `report` ended before serving, read to its end; `None` when it
/// said nothing this build can decode.
async fn read_start_report(report: io::PipeReader) -> Option<CowshedError> {
    let receiver = tokio::net::unix::pipe::Receiver::from_owned_fd(OwnedFd::from(report)).ok()?;
    let mut bytes = Vec::new();
    tokio::time::timeout(
        START_REPORT_BOUND,
        receiver.take(START_REPORT_LIMIT).read_to_end(&mut bytes),
    )
    .await
    .ok()?
    .ok()?;
    serde_json::from_slice(&bytes).ok()
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EnsureRequest {
    build: BuildId,
    project_root: PathBuf,
    authority: AuthorityWire,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
enum EnsureResponse {
    Serving {
        socket: PathBuf,
        pid: u32,
        authority: AuthorityWire,
    },
    Refused(CowshedError),
}

/// A supervisor serving a workspace, as the manager reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ensured {
    pub socket: PathBuf,
    pub pid: u32,
    /// The exact authority it serves, which every call names.
    pub authority: WorkspaceAuthoritySnapshot,
}

/// Whether a supervisor serving `served` can take the commands of a controller that needs
/// `needed`: the same workspace incarnation under the same grant revision. The lifecycle
/// revision a supervisor started under is its own; calls name the one it reports.
pub fn serves(served: &WorkspaceAuthoritySnapshot, needed: &WorkspaceAuthoritySnapshot) -> bool {
    served.repo_id == needed.repo_id
        && served.workspace == needed.workspace
        && served.workspace_incarnation == needed.workspace_incarnation
        && served.grant_revision == needed.grant_revision
}

pub struct SupervisorManager {
    store_root: PathBuf,
    spawner: Box<dyn SupervisorSpawner>,
    /// One lock per workspace socket: ensures of one workspace run one at a time.
    ensuring: Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>,
}

impl SupervisorManager {
    pub fn new(store_root: impl Into<PathBuf>, spawner: Box<dyn SupervisorSpawner>) -> Arc<Self> {
        Arc::new(Self {
            store_root: store_root.into(),
            spawner,
            ensuring: Mutex::default(),
        })
    }

    /// Watch every supervisor still serving from before this manager started.
    pub async fn adopt_running(&self) {
        let run = self.store_root.join("run");
        let Ok(entries) = std::fs::read_dir(&run) else {
            return;
        };
        let manager = manager_socket_path(&self.store_root);
        for entry in entries.flatten() {
            let path = entry.path();
            if path == manager || path.extension().is_none_or(|extension| extension != "sock") {
                continue;
            }
            match supervisor_socket::hello(&path).await {
                Ok(hello) => watch_pid(hello.pid, path),
                // A supervisor of another cowshed build: let it finish its jobs and retire, so
                // the next command for its workspace gets one of this build.
                Err(error) if error.code == ErrorCode::Conflict => {
                    match supervisor_socket::drain(&path).await {
                        Ok(pid) => {
                            eprintln!(
                                "cowshed: draining workspace supervisor {} (pid {pid}) of another cowshed build",
                                path.display()
                            );
                            watch_pid(pid, path);
                        }
                        Err(error) => eprintln!(
                            "cowshed: cannot drain workspace supervisor {}: {}",
                            path.display(),
                            error.message
                        ),
                    }
                }
                // A socket nothing answers is what a supervisor that died leaves: end the jobs
                // it left running now; the next supervisor for the workspace seals them.
                Err(error) => {
                    eprintln!(
                        "cowshed: workspace supervisor socket {} does not answer: {}",
                        path.display(),
                        error.message
                    );
                    end_lost_jobs(path, super::job_groups::Writer::Any).await;
                }
            }
        }
    }

    /// The supervisor serving `authority`'s workspace, started if nothing serves it.
    pub async fn ensure(
        &self,
        project_root: &Path,
        authority: &WorkspaceAuthoritySnapshot,
    ) -> Result<Ensured> {
        let socket = supervisor_socket::socket_path(
            &self.store_root,
            &authority.repo_id,
            &authority.workspace,
        );
        let lock = Arc::clone(
            self.ensuring
                .lock()
                .await
                .entry(socket.clone())
                .or_default(),
        );
        let _one_at_a_time = lock.lock().await;
        match supervisor_socket::hello(&socket).await {
            Ok(hello) if serves(&hello.authority, authority) => {
                return serving(&socket, &hello, authority);
            }
            // A grant change since it started: it re-reads the grants and serves under them,
            // while the jobs it already runs keep the profile they started under.
            Ok(hello)
                if hello.authority.workspace_incarnation == authority.workspace_incarnation
                    && hello.authority.grant_revision < authority.grant_revision =>
            {
                let advanced = supervisor_socket::advance(&socket).await?;
                let hello = supervisor_socket::Hello {
                    authority: advanced,
                    pid: hello.pid,
                };
                return serving(&socket, &hello, authority);
            }
            Ok(hello) => return serving(&socket, &hello, authority),
            Err(error) if error.code == ErrorCode::Conflict => return Err(error),
            Err(_) => {}
        }
        let cannot_start = |error: io::Error| {
            CowshedError::environment_missing(
                format!(
                    "cannot start the supervisor of workspace {}: {error}",
                    authority.workspace
                ),
                "reinstall cowshed with `cowshed gateway start`",
            )
        };
        let (report, report_writer) = io::pipe().map_err(cannot_start)?;
        let mut child = self
            .spawner
            .spawn(project_root, &authority.workspace, report_writer)
            .map_err(cannot_start)?;
        let deadline = tokio::time::Instant::now() + START_BOUND;
        loop {
            if let Some(status) = child.try_wait().map_err(|error| {
                CowshedError::internal(format!("cannot wait for a supervisor: {error}"))
            })? {
                return Err(match read_start_report(report).await {
                    // The supervisor's own error keeps its code: a caller that asked for this
                    // workspace fails exactly as the supervisor did.
                    Some(reason) => CowshedError::new(
                        reason.code,
                        format!(
                            "the supervisor of workspace {} could not start: {}",
                            authority.workspace, reason.message
                        ),
                        reason.hint,
                    ),
                    None => CowshedError::environment_missing(
                        format!(
                            "the supervisor of workspace {} exited ({status}) before serving",
                            authority.workspace
                        ),
                        "its reason is in the cowshed daemon log: ~/Library/Logs/cowshed/daemon-stderr.log",
                    ),
                });
            }
            if let Ok(hello) = supervisor_socket::hello(&socket).await {
                let answer = serving(&socket, &hello, authority);
                watch_child(child, socket);
                return answer;
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = child.start_kill();
                return Err(CowshedError::environment_missing(
                    format!(
                        "the supervisor of workspace {} did not serve within {} seconds",
                        authority.workspace,
                        START_BOUND.as_secs()
                    ),
                    "its reason is in the cowshed daemon log: ~/Library/Logs/cowshed/daemon-stderr.log",
                ));
            }
            tokio::time::sleep(START_POLL).await;
        }
    }
}

fn serving(
    socket: &Path,
    hello: &supervisor_socket::Hello,
    authority: &WorkspaceAuthoritySnapshot,
) -> Result<Ensured> {
    if serves(&hello.authority, authority) {
        return Ok(Ensured {
            socket: socket.to_path_buf(),
            pid: hello.pid,
            authority: hello.authority.clone(),
        });
    }
    Err(CowshedError::conflict(
        format!(
            "process {} serves workspace {}'s supervisor under incarnation {} and grant revision \
             {}; this command needs incarnation {} and grant revision {}",
            hello.pid,
            authority.workspace,
            hello.authority.workspace_incarnation,
            hello.authority.grant_revision,
            authority.workspace_incarnation,
            authority.grant_revision,
        ),
        format!(
            "let its jobs finish, or run `cowshed detach {}`, then retry",
            authority.workspace
        ),
    ))
}

/// How long a lost supervisor's jobs get between TERM and KILL.
const LOST_JOB_GRACE: Duration = Duration::from_secs(2);

/// When a started supervisor ends: report how, and end any job it left running.
fn watch_child(mut child: tokio::process::Child, socket: PathBuf) {
    let Some(pid) = child.id() else {
        return;
    };
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) if status.success() => {}
            Ok(status) => eprintln!(
                "cowshed: workspace supervisor {} ended: {status}",
                socket.display()
            ),
            Err(error) => eprintln!(
                "cowshed: cannot wait for workspace supervisor {}: {error}",
                socket.display()
            ),
        }
        end_lost_jobs(socket, super::job_groups::Writer::Process(pid)).await;
    });
}

/// When a supervisor this manager did not start, and cannot `wait` for, ends: end any job it
/// left running.
fn watch_pid(pid: u32, socket: PathBuf) {
    tokio::spawn(async move {
        let watched = tokio::task::spawn_blocking(move || wait_for_exit(pid)).await;
        match watched {
            Ok(Ok(())) => end_lost_jobs(socket, super::job_groups::Writer::Process(pid)).await,
            Ok(Err(error)) => eprintln!(
                "cowshed: cannot watch workspace supervisor {} (pid {pid}): {error}",
                socket.display()
            ),
            Err(error) => eprintln!(
                "cowshed: watching workspace supervisor {} failed: {error}",
                socket.display()
            ),
        }
    });
}

/// End the process groups a supervisor that is gone left running. A supervisor that retired
/// in order left none; the next supervisor of the workspace seals the jobs this ends.
async fn end_lost_jobs(socket: PathBuf, writer: super::job_groups::Writer) {
    let ledger = super::job_groups::ledger_path(&socket);
    let ended = tokio::task::spawn_blocking(move || {
        super::job_groups::end_recorded(&ledger, writer, LOST_JOB_GRACE)
    })
    .await;
    match ended {
        Ok(Ok(jobs)) if jobs.is_empty() => {}
        Ok(Ok(jobs)) => eprintln!(
            "cowshed: ended jobs {jobs:?}, left running by workspace supervisor {}",
            socket.display()
        ),
        Ok(Err(error)) => eprintln!(
            "cowshed: cannot end the jobs workspace supervisor {} left running: {error}",
            socket.display()
        ),
        Err(error) => eprintln!("cowshed: ending lost jobs failed: {error}"),
    }
}

/// Block until `pid`, which is not this process's child, exits.
fn wait_for_exit(pid: u32) -> io::Result<()> {
    exits_within(pid, None).map(|_| ())
}

/// Block until `pid`, which is not this process's child, exits, or `within` passes; whether it
/// exited. A process already gone has exited.
#[cfg(target_os = "macos")]
fn exits_within(pid: u32, within: Option<Duration>) -> io::Result<bool> {
    let ident = usize::try_from(pid).map_err(io::Error::other)?;
    let timeout = within
        .map(|within| {
            Ok::<_, io::Error>(libc::timespec {
                tv_sec: libc::time_t::try_from(within.as_secs()).map_err(io::Error::other)?,
                tv_nsec: libc::c_long::from(within.subsec_nanos()),
            })
        })
        .transpose()?;
    // SAFETY: kqueue returns a new descriptor this function owns and closes.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kevent structs and the timeout are plain data that outlive the call; `queue`
    // is live until the close below.
    let result = unsafe {
        let change = libc::kevent {
            ident,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let mut event = std::mem::zeroed::<libc::kevent>();
        let returned = libc::kevent(
            queue,
            &change,
            1,
            &mut event,
            1,
            timeout
                .as_ref()
                .map_or(std::ptr::null(), std::ptr::from_ref),
        );
        let outcome = match returned {
            ..0 => Err(io::Error::last_os_error()),
            0 => Ok(false),
            // A registration that failed comes back as an event carrying the error.
            _ if event.flags & libc::EV_ERROR != 0 => Err(i32::try_from(event.data)
                .map_or_else(io::Error::other, io::Error::from_raw_os_error)),
            _ => Ok(true),
        };
        libc::close(queue);
        outcome
    };
    // ESRCH: it already exited.
    match result {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(true),
        other => other,
    }
}

/// Block until `pid`, which is not this process's child, exits, or `within` passes; whether it
/// exited. A process already gone has exited.
#[cfg(target_os = "linux")]
fn exits_within(pid: u32, within: Option<Duration>) -> io::Result<bool> {
    let pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    let timeout = within.map_or(-1, |within| {
        i32::try_from(within.as_millis()).unwrap_or(i32::MAX)
    });
    // SAFETY: pidfd_open returns a new descriptor this function owns and closes.
    let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if descriptor < 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(true)
        } else {
            Err(error)
        };
    }
    let descriptor = i32::try_from(descriptor).map_err(io::Error::other)?;
    let mut poll = libc::pollfd {
        fd: descriptor,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `poll` describes one live descriptor this function owns.
    let waited = unsafe { libc::poll(&mut poll, 1, timeout) };
    // SAFETY: closing the descriptor opened above.
    unsafe { libc::close(descriptor) };
    if waited < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(waited > 0)
}

/// How long a supervisor of another build gets after each signal a lifecycle verb sends it.
const OTHER_BUILD_GRACE: Duration = Duration::from_secs(5);

/// TERM `pid`, then KILL it once `grace` passes without it exiting; blocks until it is gone.
fn terminate(pid: u32, grace: Duration) -> io::Result<()> {
    let target = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    for signal in [libc::SIGTERM, libc::SIGKILL] {
        // SAFETY: kill with a positive pid signals exactly that process and touches no memory.
        if unsafe { libc::kill(target, signal) } != 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(())
            } else {
                Err(error)
            };
        }
        if exits_within(pid, Some(grace))? {
            return Ok(());
        }
    }
    Err(io::Error::other(format!(
        "it is still running {}s after KILL",
        grace.as_secs()
    )))
}

/// Stop the supervisor of another cowshed build that serves `socket`, for a lifecycle verb about
/// to change its workspace's substrate (`detach`, `rm`, `restore`, `resize`, `mv`); the pid it
/// ran as.
///
/// This build speaks no request of that supervisor's protocol but `drain`, the one whose shape
/// never changes, and `drain` alone only waits for its jobs. So `drain` names the process serving
/// the socket and stops it admitting work, and the process is then signalled: TERM, and KILL
/// once the grace has passed. What its own retirement would have done is done for it once it is
/// gone: the jobs it left running are ended from the group ledger it kept beside the socket, the
/// ledger removed, and the socket unlinked unless something serves it again. The jobs' records
/// stay running until the workspace's next supervisor seals them, as after any lost supervisor.
pub async fn stop_other_build(socket: &Path) -> Result<u32> {
    let pid = supervisor_socket::drain(socket).await?;
    if pid <= 1 || pid == std::process::id() {
        return Err(CowshedError::integrity(
            format!(
                "the workspace supervisor at {} names pid {pid}, which no supervisor can be",
                socket.display()
            ),
            "stop the supervisor process yourself, then retry",
        ));
    }
    tokio::task::spawn_blocking(move || terminate(pid, OTHER_BUILD_GRACE))
        .await
        .map_err(|error| CowshedError::internal(format!("stopping pid {pid}: {error}")))?
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot stop the workspace supervisor at {} (pid {pid}) of another cowshed build: {error}",
                    socket.display()
                ),
                format!("stop pid {pid} yourself, then retry"),
            )
        })?;
    let ledger = super::job_groups::ledger_path(socket);
    let taken = ledger.clone();
    tokio::task::spawn_blocking(move || super::job_groups::take_lost(&taken, LOST_JOB_GRACE))
        .await
        .map_err(|error| CowshedError::internal(format!("ending lost jobs: {error}")))?
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot end the jobs pid {pid} left running, recorded in {}: {error}",
                    ledger.display()
                ),
                "end the jobs' process groups yourself, remove that file, then retry",
            )
        })?;
    match UnixStream::connect(socket).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => match std::fs::remove_file(socket) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CowshedError::environment_missing(
                    format!(
                        "cannot remove the socket {} pid {pid} served: {error}",
                        socket.display()
                    ),
                    "remove it yourself, then retry",
                ));
            }
        },
    }
    Ok(pid)
}

/// Serve ensures on `listener` for as long as the daemon runs.
pub async fn serve(listener: UnixListener, manager: Arc<SupervisorManager>) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await.map_err(|error| {
            CowshedError::environment_missing(
                format!("the supervisor manager stopped accepting: {error}"),
                "restart the gateway with `cowshed gateway start`",
            )
        })?;
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            let _ = serve_ensure(stream, &manager).await;
        });
    }
}

async fn serve_ensure(mut stream: UnixStream, manager: &SupervisorManager) -> Result<()> {
    verify_peer(&stream)?;
    let request: serde_json::Value = read_json(&mut stream).await?;
    let ensured = async {
        same_build(&request)?;
        let request: EnsureRequest = serde_json::from_value(request)
            .map_err(|error| protocol_error(format!("malformed ensure: {error}")))?;
        manager
            .ensure(&request.project_root, &request.authority.into())
            .await
    };
    let response = match ensured.await {
        Ok(ensured) => EnsureResponse::Serving {
            socket: ensured.socket,
            pid: ensured.pid,
            authority: (&ensured.authority).into(),
        },
        Err(error) => EnsureResponse::Refused(error),
    };
    write_json(&mut stream, &response).await
}

/// Refuse an ensure from another cowshed build by name. Read before the rest of the request:
/// another build's may carry fields this one cannot decode, and the build is what says why.
fn same_build(request: &serde_json::Value) -> Result<()> {
    let ours = BuildId::current()?;
    let theirs = request.get("build").and_then(serde_json::Value::as_str);
    if theirs == Some(ours.as_str()) {
        return Ok(());
    }
    Err(CowshedError::conflict(
        format!(
            "the cowshed daemon is build {ours}; this cowshed is build {}",
            theirs.unwrap_or("(unnamed)")
        ),
        "run `cowshed gateway start` from the cowshed you mean to use",
    ))
}

/// Ask the host's manager for the supervisor serving `authority`'s workspace.
pub async fn ensure(
    store_root: &Path,
    project_root: &Path,
    authority: &WorkspaceAuthoritySnapshot,
) -> Result<Ensured> {
    let path = manager_socket_path(store_root);
    let mut stream = UnixStream::connect(&path).await.map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "the cowshed daemon, which runs workspace supervisors, is not reachable at {}: {error}",
                path.display()
            ),
            "cowshed gateway start",
        )
    })?;
    write_json(
        &mut stream,
        &EnsureRequest {
            build: BuildId::current()?.clone(),
            project_root: project_root.to_path_buf(),
            authority: authority.into(),
        },
    )
    .await?;
    let response: EnsureResponse = tokio::time::timeout(
        START_BOUND + Duration::from_secs(30),
        read_json(&mut stream),
    )
    .await
    .map_err(|_| protocol_error("the supervisor manager did not answer"))??;
    match response {
        EnsureResponse::Serving {
            socket,
            pid,
            authority,
        } => Ok(Ensured {
            socket,
            pid,
            authority: authority.into(),
        }),
        EnsureResponse::Refused(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::os::fd::IntoRawFd as _;

    use super::*;

    /// The supervisor takes the report pipe the manager named, keeps it from every process it
    /// starts, and its error arrives whole; it never takes a descriptor that is not a pipe.
    #[test]
    fn a_supervisor_reports_on_the_named_pipe_and_takes_nothing_else() {
        let (mut reader, writer) = io::pipe().expect("pipe");
        let fd = OwnedFd::from(writer).into_raw_fd();
        let report = StartReport::named(Some(&fd.to_string()));
        let taken = report
            .0
            .as_ref()
            .expect("the named pipe is taken")
            .as_raw_fd();
        // SAFETY: F_GETFD on a descriptor this test holds open.
        let flags = unsafe { libc::fcntl(taken, libc::F_GETFD) };
        assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);
        let error = CowshedError::conflict("a newer cowshed wrote these records", "upgrade");
        report.send(&error).expect("send");
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("the report ends");
        assert_eq!(
            serde_json::from_slice::<CowshedError>(&bytes).unwrap(),
            error
        );

        let file = std::fs::File::open("/dev/null").expect("/dev/null");
        assert!(
            StartReport::named(Some(&file.as_raw_fd().to_string()))
                .0
                .is_none()
        );
        assert!(StartReport::named(Some("2")).0.is_none());
        assert!(StartReport::named(Some("report")).0.is_none());
        assert!(StartReport::named(None).0.is_none());
        StartReport::named(None)
            .send(&error)
            .expect("a supervisor started by hand has nobody to tell");
    }

    struct NoSpawner;

    impl SupervisorSpawner for NoSpawner {
        fn spawn(
            &self,
            _: &Path,
            _: &WorkspaceName,
            _: io::PipeWriter,
        ) -> io::Result<tokio::process::Child> {
            Err(io::Error::other("this manager starts no supervisor"))
        }
    }

    /// A supervisor socket that answers `hello` as a supervisor of `build` would, and records
    /// every request it is sent.
    fn fake_supervisor(socket: PathBuf, build: String) -> Arc<std::sync::Mutex<Vec<String>>> {
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let listener = UnixListener::bind(&socket).expect("bind the fake supervisor");
        // No such process: whatever watches it finds it already gone.
        let pid = i32::MAX;
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let request: serde_json::Value = read_json(&mut stream).await.expect("request");
                let value = match request.as_str() {
                    Some("hello") => serde_json::json!({
                        "build": build,
                        "authority": {
                            "repoId": "acme/widget",
                            "workspace": "raven",
                            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                            "grantRevision": 1,
                            "lifecycleRevision": 1,
                        },
                        "pid": pid,
                    }),
                    _ => serde_json::json!(pid),
                };
                seen.lock().expect("requests").push(request.to_string());
                write_json(
                    &mut stream,
                    &serde_json::json!({"ok": {"value": value, "bytes": 0}}),
                )
                .await
                .expect("answer");
            }
        });
        requests
    }

    /// A daemon upgrade drains every supervisor another build started — whatever it changed,
    /// named or not — and keeps serving through the ones its own build started (11_shell.md
    /// "Draining a supervisor of another build").
    #[tokio::test]
    async fn a_new_manager_drains_every_supervisor_of_another_build_and_keeps_its_own() {
        // Unix socket paths are short; the per-user temporary directory is not.
        let store = PathBuf::from("/tmp").join(format!(
            "cowshed-drain-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(store.join("run")).expect("run directory");
        let other = fake_supervisor(store.join("run/other.sock"), "another build".to_owned());
        let own = BuildId::current().expect("this build").as_str().to_owned();
        let same = fake_supervisor(store.join("run/same.sock"), own);

        SupervisorManager::new(&store, Box::new(NoSpawner))
            .adopt_running()
            .await;

        assert_eq!(
            *other.lock().expect("requests"),
            ["\"hello\"", "\"drain\""],
            "the supervisor of another build is asked to drain"
        );
        assert_eq!(
            *same.lock().expect("requests"),
            ["\"hello\""],
            "a supervisor of this build keeps serving"
        );
        std::fs::remove_dir_all(store).expect("cleanup");
    }

    /// Names the socket [`serves_as_a_supervisor_of_another_build`] serves.
    const OTHER_BUILD_SOCKET: &str = "COWSHED_TEST_OTHER_BUILD_SOCKET";

    /// Not a test: the separate process a supervisor of another build runs as, for
    /// [`a_verb_stops_a_supervisor_of_another_build_and_the_jobs_it_left`]. It answers every
    /// request as `drain` does, with its own pid, and serves until it is killed.
    #[test]
    #[ignore = "helper process of a_verb_stops_a_supervisor_of_another_build_and_the_jobs_it_left"]
    fn serves_as_a_supervisor_of_another_build() {
        let Some(socket) = std::env::var_os(OTHER_BUILD_SOCKET) else {
            return;
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async move {
                let listener = UnixListener::bind(socket).expect("bind");
                loop {
                    let (mut stream, _) = listener.accept().await.expect("accept");
                    // A readiness probe connects and sends nothing.
                    let Ok(serde_json::Value::String(_)) = read_json(&mut stream).await else {
                        continue;
                    };
                    let answer =
                        serde_json::json!({"ok": {"value": std::process::id(), "bytes": 0}});
                    write_json(&mut stream, &answer).await.expect("answer");
                }
            });
    }

    /// A lifecycle verb about to change a workspace's substrate stops the supervisor of another
    /// build that serves it, although that supervisor answers none of this build's requests but
    /// `drain`: its process is signalled, the jobs its ledger names are ended, and the ledger and
    /// the socket it can no longer unlink are gone.
    #[tokio::test]
    async fn a_verb_stops_a_supervisor_of_another_build_and_the_jobs_it_left() {
        use std::os::unix::process::ExitStatusExt as _;
        // Unix socket paths are short; the per-user temporary directory is not.
        let store = PathBuf::from("/tmp").join(format!(
            "cowshed-stop-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(store.join("run")).expect("run directory");
        let socket = store.join("run/other.sock");
        // Killed should the test fail before it is stopped: the helper serves until it is killed.
        let mut supervisor =
            tokio::process::Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "runtime::supervisor_manager::tests::serves_as_a_supervisor_of_another_build",
                    "--ignored",
                ])
                .env(OTHER_BUILD_SOCKET, &socket)
                .stdout(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .expect("start the supervisor of another build");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while UnixStream::connect(&socket).await.is_err() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the helper never served"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // `sleep` from PATH: NixOS keeps nothing but `sh` and `env` in /bin and /usr/bin.
        let mut job = tokio::process::Command::new("sleep")
            .arg("60")
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .expect("a job leading its own group");
        let group = job.id().expect("the job runs");
        let ledger = super::super::job_groups::ledger_path(&socket);
        super::super::job_groups::record(&ledger, &[(7, group)]).expect("ledger");

        let stopped = stop_other_build(&socket).await.expect("stopped");

        assert_eq!(Some(stopped), supervisor.id());
        assert_eq!(
            supervisor.wait().await.expect("supervisor").signal(),
            Some(libc::SIGTERM)
        );
        assert_eq!(job.wait().await.expect("job").signal(), Some(libc::SIGTERM));
        assert!(!ledger.exists(), "the ledger is taken");
        assert!(!socket.exists(), "the socket nothing serves is gone");
        std::fs::remove_dir_all(store).expect("cleanup");
    }
}
