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
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use super::supervisor::WorkspaceAuthoritySnapshot;
use super::supervisor_socket::{
    self, AuthorityWire, BuildId, protocol_error, read_json, verify_peer, write_json,
};
use crate::error::{CowshedError, ErrorCode, OtherBuild, Result};
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
            match supervisor_socket::hello_if_present(&path).await {
                Ok(Some(hello)) => watch_pid(hello.pid, path),
                // A supervisor of another cowshed build: let it finish its jobs and retire, so
                // the next command for its workspace gets one of this build.
                Err(error) if error.code == ErrorCode::Conflict => {
                    match supervisor_socket::drain(&path).await {
                        Ok(drained) => {
                            let pid = drained.pid();
                            eprintln!(
                                "cowshed: draining workspace supervisor {} (pid {pid}) of another cowshed build",
                                path.display()
                            );
                            recover_after_exit(pid, path, move || drained.exit.wait());
                        }
                        Err(error) => eprintln!(
                            "cowshed: cannot drain workspace supervisor {}: {}",
                            path.display(),
                            error.message
                        ),
                    }
                }
                // Only a positively absent listener authorizes lost-supervisor recovery. A
                // wedged live peer or an unknown protocol outcome retains its jobs and ledger.
                Ok(None) => end_lost_jobs(path, super::job_groups::Writer::Any).await,
                Err(error) => eprintln!(
                    "cowshed: cannot determine whether workspace supervisor {} stopped; its \
                     jobs and ledger are retained: {}",
                    path.display(),
                    error.message
                ),
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
            Err(error) => {
                eprintln!(
                    "cowshed: cannot wait for workspace supervisor {}; its jobs and ledger are \
                     retained: {error}",
                    socket.display()
                );
                return;
            }
        }
        end_lost_jobs(socket, super::job_groups::Writer::Process(pid)).await;
    });
}

/// When a supervisor this manager did not start, and cannot `wait` for, ends: end any job it
/// left running.
fn watch_pid(pid: u32, socket: PathBuf) {
    recover_after_exit(pid, socket, move || wait_for_exit(pid));
}

/// Once `exited` returns, the supervisor `pid` has ended: end any job it left running.
fn recover_after_exit(
    pid: u32,
    socket: PathBuf,
    exited: impl FnOnce() -> io::Result<()> + Send + 'static,
) {
    tokio::spawn(async move {
        match tokio::task::spawn_blocking(exited).await {
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
    // With no group to end, binding would only take the lease from whoever binds next: the
    // controller that retired the supervisor in order, about to change its workspace. A ledger
    // that cannot be read is read again, and reported, under the lease.
    if matches!(
        super::job_groups::names_groups(&super::job_groups::ledger_path(&socket), writer),
        Ok(false)
    ) {
        return;
    }
    let _socket = match supervisor_socket::bind(&socket).await {
        Ok(socket) => socket,
        Err(error) => {
            eprintln!(
                "cowshed: cannot exclusively recover workspace supervisor {}; its ledger is \
                 retained: {}",
                socket.display(),
                error.message
            );
            return;
        }
    };
    let ledger = super::job_groups::ledger_path(&socket);
    let ended = tokio::task::spawn_blocking(move || {
        super::job_groups::end_recorded(&ledger, writer, LOST_JOB_GRACE)
    })
    .await;
    match ended {
        Ok(Ok(ended)) => {
            if !ended.signalled.is_empty() {
                eprintln!(
                    "cowshed: ended jobs {:?}, left running by workspace supervisor {}",
                    ended.signalled,
                    socket.display()
                );
            }
            if !ended.unresolved.is_empty() {
                eprintln!(
                    "cowshed: jobs {:?} of workspace supervisor {} left processes whose group \
                     leader is gone; not signalled, and kept in its ledger for the next supervisor",
                    ended.unresolved,
                    socket.display()
                );
            }
        }
        Ok(Err(error)) => eprintln!(
            "cowshed: cannot end the jobs workspace supervisor {} left running: {error}",
            socket.display()
        ),
        Err(error) => eprintln!("cowshed: ending lost jobs failed: {error}"),
    }
}

/// Block until `pid`, which is not this process's child, exits. A process already gone, or
/// already exiting, has exited.
#[cfg(target_os = "macos")]
fn wait_for_exit(pid: u32) -> io::Result<()> {
    let ident = usize::try_from(pid).map_err(io::Error::other)?;
    // SAFETY: kqueue returns a new descriptor this function owns and closes.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kevent structs are plain data that outlive the call; `queue` is live until
    // the close below.
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
        let returned = libc::kevent(queue, &change, 1, &mut event, 1, std::ptr::null());
        let outcome = match returned {
            ..0 => Err(io::Error::last_os_error()),
            // A registration that failed comes back as an event carrying the error.
            _ if event.flags & libc::EV_ERROR != 0 => Err(i32::try_from(event.data)
                .map_or_else(io::Error::other, io::Error::from_raw_os_error)),
            _ => Ok(()),
        };
        libc::close(queue);
        outcome
    };
    // ESRCH: it already exited, or began to, and its descriptors may still be open; a
    // supervisor this manager signals is therefore watched before it is (`ExitWatch`).
    match result {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        other => other,
    }
}

/// Block until `pid`, which is not this process's child, exits. A process already gone has
/// exited.
#[cfg(target_os = "linux")]
fn wait_for_exit(pid: u32) -> io::Result<()> {
    let pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    // SAFETY: pidfd_open returns a new descriptor this function owns and closes.
    let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if descriptor < 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
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
    let waited = unsafe { libc::poll(&mut poll, 1, -1) };
    // SAFETY: closing the descriptor opened above.
    unsafe { libc::close(descriptor) };
    if waited < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// How long a supervisor of another build gets after each signal a lifecycle verb sends it.
const OTHER_BUILD_GRACE: Duration = Duration::from_secs(5);

/// TERM `process`, then KILL it once `grace` passes without it exiting; blocks until it is gone
/// and has released its descriptors, its socket's listener with them. Every signal goes through
/// the process's own identity, so one that exited -- and whose pid another process may since
/// hold -- is never reached through its pid. Its exit is awaited through `exit`, registered
/// while it ran: a watch registered once it had begun to exit is refused while its descriptors
/// may still be open (measured on Darwin), which a socket bound next would find still attached.
fn terminate(
    process: &super::job_groups::Process,
    exit: &super::job_groups::ExitWatch,
    grace: Duration,
) -> io::Result<()> {
    for signal in [libc::SIGTERM, libc::SIGKILL] {
        // A process that no longer runs may still be releasing what it held: its end comes
        // through the watch, which this signal's outcome does not change.
        process.signal(signal)?;
        if exit.within(grace)? {
            return Ok(());
        }
    }
    Err(io::Error::other(format!(
        "it is still running {}s after KILL",
        grace.as_secs()
    )))
}

/// A supervisor of another build that [`stop_other_build`] stopped.
#[must_use = "hold the socket until the workspace mutation has finished"]
pub struct OtherBuildStopped {
    /// The pid it ran as.
    pub pid: u32,
    /// The workspace's socket, bound since the supervisor stopped and held since: no other
    /// binder came between the stop and the caller's mutation.
    pub socket: supervisor_socket::BoundSocket,
}

/// Stop the supervisor of another cowshed build that serves `socket`, for a lifecycle verb about
/// to change its workspace's substrate (`detach`, `rm`, `restore`, `resize`, `mv`).
///
/// This build speaks no request of that supervisor's protocol but `drain`, the one whose shape
/// never changes, and `drain` alone only waits for its jobs. The connected peer's non-reusable
/// kernel identity, cross-checked with the drain reply, owns TERM and KILL after the grace: no
/// later process that took its pid can receive either. Once it is gone, the socket is bound,
/// and its group ledger ends only positively owned processes and carries unresolved groups for
/// the next supervisor. The jobs' records remain unterminated until that next supervisor seals
/// them, as after any lost supervisor.
pub async fn stop_other_build(socket: &Path) -> Result<OtherBuildStopped> {
    let drained = supervisor_socket::drain(socket).await?;
    let pid = drained.pid();
    if pid <= 1 || pid == std::process::id() {
        return Err(CowshedError::integrity(
            format!(
                "the workspace supervisor at {} names pid {pid}, which no supervisor can be",
                socket.display()
            ),
            "stop the supervisor process yourself, then retry",
        ));
    }
    tokio::task::spawn_blocking(move || {
        terminate(&drained.process, &drained.exit, OTHER_BUILD_GRACE)
    })
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
    let bound = supervisor_socket::bind(socket).await?;
    let ledger = super::job_groups::ledger_path(socket);
    let taken = ledger.clone();
    let unresolved =
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
    for group in &unresolved {
        eprintln!(
            "cowshed: job {} process group {} that pid {pid} left still has processes but its \
             leader is gone; not signalled, and kept in {} for the workspace's next supervisor",
            group.job_id(),
            group.pgid(),
            ledger.display()
        );
    }
    Ok(OtherBuildStopped { pid, socket: bound })
}

/// Serve ensures while holding the manager's bound socket and exclusive listener lease.
pub async fn serve(
    socket: supervisor_socket::BoundSocket,
    manager: Arc<SupervisorManager>,
) -> Result<()> {
    loop {
        let (stream, _) = socket.listener.accept().await.map_err(|error| {
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

/// Refuse an ensure from another cowshed build by name, and with both builds as data
/// ([`OtherBuild`]): the controller that asked can run nothing here, and whoever holds it replaces
/// it without reading the sentence. Read before the rest of the request: another build's may carry
/// fields this one cannot decode, and the build is what says why.
fn same_build(request: &serde_json::Value) -> Result<()> {
    let ours = BuildId::current()?;
    let theirs = request.get("build").and_then(serde_json::Value::as_str);
    if theirs == Some(ours.as_str()) {
        return Ok(());
    }
    Err(CowshedError::other_build(OtherBuild {
        daemon: ours.clone(),
        caller: theirs.map(BuildId::named),
    }))
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
    use tokio::net::UnixListener;

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

    /// The daemon refuses an ensure from another build with both builds as data, so a controller
    /// of the refused build is recognised — and replaced — by whoever holds it, without anybody
    /// reading the sentence (11_shell.md "hello").
    #[tokio::test]
    async fn an_ensure_from_another_build_is_refused_naming_both_builds() {
        // Unix socket paths are short; the per-user temporary directory is not.
        let store = PathBuf::from("/tmp").join(format!(
            "cowshed-other-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(store.join("run")).expect("run directory");
        let socket = manager_socket_path(&store);
        let listener = supervisor_socket::bind(&socket)
            .await
            .expect("bind the manager");
        let served = tokio::spawn(serve(
            listener,
            SupervisorManager::new(&store, Box::new(NoSpawner)),
        ));
        let mut stream = UnixStream::connect(&socket)
            .await
            .expect("reach the manager");
        write_json(
            &mut stream,
            &serde_json::json!({
                "build": "another build",
                "projectRoot": "/repo",
                "authority": {
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "grantRevision": 1,
                    "lifecycleRevision": 1,
                },
            }),
        )
        .await
        .expect("ask");
        let response: EnsureResponse = read_json(&mut stream).await.expect("answer");
        let EnsureResponse::Refused(error) = response else {
            panic!("an ensure from another build must be refused");
        };
        assert_eq!(error.code, ErrorCode::Conflict);
        assert_eq!(
            error.other_build_source(),
            Some(&OtherBuild {
                daemon: BuildId::current().expect("this build").clone(),
                caller: Some(serde_json::from_value(serde_json::json!("another build")).unwrap()),
            })
        );
        served.abort();
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
    /// `drain`: its process is signalled, the jobs its ledger names are ended, the ledger is
    /// taken, and the verb holds the workspace's socket from then on, with no second bind for
    /// anybody to win.
    #[tokio::test]
    async fn a_verb_stops_a_supervisor_of_another_build_and_the_jobs_it_left() {
        use std::os::unix::fs::MetadataExt as _;
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
        let leader = super::super::job_groups::GroupLeader::observe(group).expect("leader");
        super::super::job_groups::record(&ledger, &[(7, leader)], &[]).expect("ledger");
        let foreign = std::fs::symlink_metadata(&socket)
            .expect("its socket")
            .ino();

        let stopped = stop_other_build(&socket).await.expect("stopped");

        assert_eq!(Some(stopped.pid), supervisor.id());
        assert_eq!(
            supervisor.wait().await.expect("supervisor").signal(),
            Some(libc::SIGTERM)
        );
        assert_eq!(job.wait().await.expect("job").signal(), Some(libc::SIGTERM));
        assert!(!ledger.exists(), "the ledger is taken");
        assert_eq!(stopped.socket.path(), socket);
        let held = std::fs::symlink_metadata(&socket)
            .expect("the held socket")
            .ino();
        assert_ne!(held, foreign, "the stopped supervisor's socket is replaced");
        assert_eq!(
            supervisor_socket::bind(&socket).await.unwrap_err().code,
            ErrorCode::Conflict,
            "the socket stays held between the stop and the verb's mutation"
        );
        assert_eq!(
            std::fs::symlink_metadata(&socket)
                .expect("still held")
                .ino(),
            held
        );
        drop(stopped.socket);
        let mut left: Vec<_> = std::fs::read_dir(store.join("run"))
            .expect("run directory")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .into_string()
                    .expect("a name")
            })
            .collect();
        left.sort();
        assert_eq!(left, ["other.sock.lock"], "no socket file is left behind");
        std::fs::remove_dir_all(store).expect("cleanup");
    }
}
