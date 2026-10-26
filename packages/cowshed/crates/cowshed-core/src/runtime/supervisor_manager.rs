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
//!
//! The supervisors found serving at startup are recovered all at once and in the background
//! ([`SupervisorManager::recovery`]): the manager takes ensures throughout, refusing by type only
//! the workspaces whose supervisor it is still recovering.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ffi::OsString;
use std::io::{self, Write as _};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use cowshed_gateway_types::SupervisorRecovery;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt as _;
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};

use super::supervisor::WorkspaceAuthoritySnapshot;
use super::supervisor_socket::{
    self, AuthorityWire, current_build, protocol_error, read_json, verify_peer, write_json,
};
use crate::error::{CowshedError, ErrorCode, OtherBuild, Result};
use cowshed_os::fork_lock::Spawn as _;
use cowshed_os::timing;
use cowshed_types::metadata::WorkspaceName;
use cowshed_types::supervisor_socket::BuildId;

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
/// The span every startup recovery step reports under.
const RECOVERY_SCOPE: &str = "supervisor-recovery";

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
        let spawn_failure = |failure: crate::exec::SpawnFailure| {
            io::Error::new(
                failure.source.kind(),
                format!(
                    "supervisor spawn failed during {:?}: {}",
                    failure.stage, failure.source
                ),
            )
        };
        crate::exec::prepare_child_descriptors(command.as_std_mut()).map_err(spawn_failure)?;
        // SAFETY: `setsid` and `fcntl` between fork and exec are async-signal-safe and touch only
        // this child. A session of its own takes the supervisor out of the daemon's process
        // group, which a service manager ends with the daemon. Descriptor preparation registered
        // above seals every non-stdio descriptor first; this hook allows only the report pipe
        // through exec, never a foreground lock inherited by the manager.
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
        let child = command
            .spawn_locked()
            .map_err(crate::exec::classify_spawn_error)
            .map_err(spawn_failure);
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

/// One call to the manager. Every call names the caller's build first, and the manager refuses
/// another build's by name before it reads the rest ([`same_build`]); the verb says what the
/// caller asks once the builds agree.
#[derive(Serialize, Deserialize)]
#[serde(tag = "verb", rename_all = "camelCase", deny_unknown_fields)]
enum ManagerRequest {
    /// The supervisor serving a workspace, started if nothing serves it.
    Ensure {
        build: BuildId,
        #[serde(rename = "projectRoot")]
        project_root: PathBuf,
        authority: AuthorityWire,
    },
    /// Nothing but whether the manager runs the caller's build: what a verb asks before it
    /// changes anything, so a build the manager would refuse is refused up front rather than
    /// after the verb's first step ([`require_same_build`]).
    SameBuild { build: BuildId },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
enum EnsureResponse {
    Serving {
        socket: PathBuf,
        pid: u32,
        authority: AuthorityWire,
    },
    /// The answer to [`ManagerRequest::SameBuild`] from the manager's own build.
    SameBuild,
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

/// Where a supervisor serving one authority stands with a controller that read another.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Standing {
    /// The same workspace incarnation under the grant revision the controller read or a newer
    /// one, published between that read and the hello: it takes the controller's commands,
    /// which name the authority it reports. Grant revisions of one incarnation only grow, so
    /// the controller never runs under grants older than the ones it read.
    Serves,
    /// The same incarnation under grants older than the ones the controller read: it serves
    /// once it has re-read them.
    Behind,
    /// Another workspace or incarnation: it never serves this controller.
    Elsewhere,
}

/// How a supervisor serving `served` stands with a controller that read `needed`. The
/// lifecycle revision a supervisor started under is its own; calls name the one it reports.
pub fn standing(
    served: &WorkspaceAuthoritySnapshot,
    needed: &WorkspaceAuthoritySnapshot,
) -> Standing {
    if served.repo_id != needed.repo_id
        || served.workspace != needed.workspace
        || served.workspace_incarnation != needed.workspace_incarnation
    {
        Standing::Elsewhere
    } else if served.grant_revision >= needed.grant_revision {
        Standing::Serves
    } else {
        Standing::Behind
    }
}

/// How recovery reaches the supervisors that served before the manager started: over their
/// sockets in the daemon ([`SocketPeers`]), under a test's control in tests.
#[async_trait::async_trait]
pub trait SupervisorPeers: Send + Sync + 'static {
    /// Who serves `socket`, as [`supervisor_socket::hello_if_present`] answers it.
    async fn hello_if_present(&self, socket: &Path) -> Result<Option<supervisor_socket::Hello>>;
    /// Ask the supervisor of another build serving `socket` to drain, as
    /// [`supervisor_socket::drain`] does.
    async fn drain(&self, socket: &Path) -> Result<Draining>;
}

/// A supervisor of another build that answered `drain`: it admits nothing more and retires once
/// its running jobs end.
pub struct Draining {
    /// The process that answered.
    pub pid: u32,
    /// Blocks until that process has exited.
    pub exit: Box<dyn FnOnce() -> io::Result<()> + Send>,
}

/// Production: the supervisors' own sockets.
#[derive(Clone, Copy, Debug)]
pub struct SocketPeers;

#[async_trait::async_trait]
impl SupervisorPeers for SocketPeers {
    async fn hello_if_present(&self, socket: &Path) -> Result<Option<supervisor_socket::Hello>> {
        supervisor_socket::hello_if_present(socket).await
    }

    async fn drain(&self, socket: &Path) -> Result<Draining> {
        let drained = supervisor_socket::drain(socket).await?;
        Ok(Draining {
            pid: drained.pid(),
            exit: Box::new(move || drained.exit.wait()),
        })
    }
}

pub struct SupervisorManager {
    store_root: PathBuf,
    spawner: Box<dyn SupervisorSpawner>,
    /// One lock per workspace socket: ensures of one workspace run one at a time.
    ensuring: Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>,
    /// The sockets whose supervisors served before this manager started and are not recovered
    /// yet ([`Self::recovery`]). Held only to read or remove, never across an await.
    recovering: std::sync::Mutex<BTreeSet<PathBuf>>,
    /// The daemon's startup pass: a supervisor runs in a workspace that pass may still be
    /// mounting, so no ensure is served until it has finished.
    startup: Arc<crate::StartupHealState>,
}

impl SupervisorManager {
    pub fn new(
        store_root: impl Into<PathBuf>,
        spawner: Box<dyn SupervisorSpawner>,
        startup: Arc<crate::StartupHealState>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store_root: store_root.into(),
            spawner,
            ensuring: Mutex::default(),
            recovering: std::sync::Mutex::default(),
            startup,
        })
    }

    /// The supervisors still serving from before this manager started, listed: from now on
    /// [`Self::recovering`] counts every one of them and [`Self::ensure`] refuses each one's
    /// workspace until it is recovered. Nothing reaches them until [`Recovery::start`], so a
    /// daemon lists them before it answers anyone and starts recovering them only once it has
    /// claimed the host.
    pub fn recovery(self: &Arc<Self>) -> Recovery {
        let sockets: BTreeSet<PathBuf> = std::fs::read_dir(self.store_root.join("run"))
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| cowshed_types::supervisor_socket::is_socket_name(&entry.file_name()))
            .map(|entry| entry.path())
            .collect();
        self.recovering
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone_from(&sockets);
        Recovery {
            manager: Arc::clone(self),
            sockets,
        }
    }

    /// How many supervisors from before this manager started it has not recovered yet.
    pub fn recovering(&self) -> Option<SupervisorRecovery> {
        let recovering = self
            .recovering
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        NonZeroUsize::new(recovering.len()).map(|supervisors| SupervisorRecovery { supervisors })
    }

    /// [`Self::recovering`], when the supervisor of `socket` is one it has not recovered yet.
    fn recovering_including(&self, socket: &Path) -> Option<SupervisorRecovery> {
        let recovering = self
            .recovering
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !recovering.contains(socket) {
            return None;
        }
        NonZeroUsize::new(recovering.len()).map(|supervisors| SupervisorRecovery { supervisors })
    }

    /// Recover the supervisor that served `socket` before this manager started. Its workspace
    /// is refused until this ends, however it ends.
    async fn recover_one(self: Arc<Self>, peers: Arc<dyn SupervisorPeers>, socket: PathBuf) {
        let _recovered = Recovered {
            manager: &self,
            socket: &socket,
        };
        match peers.hello_if_present(&socket).await {
            Ok(Some(hello)) => watch_pid(hello.pid, socket.clone()),
            // A supervisor of another cowshed build: let it finish its jobs and retire, so the
            // next command for its workspace gets one of this build.
            Err(error) if error.code == ErrorCode::Conflict => {
                drain_other_build(&*peers, &socket).await;
            }
            // Only a positively absent listener authorizes lost-supervisor recovery. A wedged
            // live peer or an unknown protocol outcome retains its jobs and ledger.
            Ok(None) => end_lost_jobs(socket.clone(), super::job_groups::Writer::Any).await,
            Err(error) => cowshed_os::eprintln!(
                "cowshed: cannot determine whether workspace supervisor {} stopped; its jobs and \
             ledger are retained: {}",
                socket.display(),
                error.message
            ),
        }
    }

    /// The supervisor serving `authority`'s workspace, started if nothing serves it.
    pub async fn ensure(
        &self,
        project_root: &Path,
        authority: &WorkspaceAuthoritySnapshot,
    ) -> Result<Ensured> {
        let socket = cowshed_types::supervisor_socket::socket_path(
            &self.store_root,
            &authority.repo_id,
            &authority.workspace,
        );
        if let Some(heal) = self.startup.current() {
            return Err(CowshedError::healing(heal));
        }
        // Its recovery owns the socket until it ends: a supervisor started now would race it for
        // the socket and the ledger the old supervisor left.
        if let Some(recovery) = self.recovering_including(&socket) {
            return Err(CowshedError::recovering(recovery, &authority.workspace));
        }
        let lock = Arc::clone(
            self.ensuring
                .lock()
                .await
                .entry(socket.clone())
                .or_default(),
        );
        let _one_at_a_time = lock.lock().await;
        // Only a socket proven unserved (`Ok(None)`) starts a supervisor. A hello that timed out,
        // a live socket that refuses, or an answer that cannot be read says nothing about whether
        // a supervisor serves: it is returned as itself. Treating it as a vacancy started a
        // second supervisor beside a live one, which then lost the socket lease and failed as "a
        // supervisor or lifecycle operation owns" the socket (measured 2026-10-21).
        match supervisor_socket::hello_if_present(&socket).await {
            Ok(Some(hello)) => {
                let hello = match standing(&hello.authority, authority) {
                    Standing::Serves | Standing::Elsewhere => hello,
                    // A grant change since it started: it re-reads the grants and serves under
                    // them, while the jobs it already runs keep the profile they started under.
                    Standing::Behind => supervisor_socket::Hello {
                        authority: supervisor_socket::advance(&socket).await?,
                        pid: hello.pid,
                    },
                };
                return serving(&socket, &hello, authority);
            }
            Ok(None) => {}
            Err(error) => return Err(error),
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
                let killed = child.start_kill();
                let reaped = cowshed_os::deadline::reap_async(
                    "supervisor",
                    &mut child,
                    &cowshed_os::deadline::start(
                        "reap unserving supervisor",
                        cowshed_os::deadline::REAP,
                    ),
                    cowshed_os::deadline::Progress::Signalled {
                        signal: libc::SIGKILL,
                    },
                )
                .await;
                // A child dropped unreaped is collected by tokio's orphan reaper when it exits.
                let ended = match (killed, reaped) {
                    (_, Ok(_)) => String::new(),
                    (killed, Err(reap)) => {
                        format!(" (kill: {killed:?}; {reap}; it is collected when it exits)")
                    }
                };
                return Err(CowshedError::environment_missing(
                    format!(
                        "the supervisor of workspace {} did not serve within {} seconds{ended}",
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

/// The supervisors a manager found serving from before it started, not yet being recovered
/// ([`SupervisorManager::recovery`]).
#[must_use = "start the recovery, or its workspaces stay refused"]
pub struct Recovery {
    manager: Arc<SupervisorManager>,
    sockets: BTreeSet<PathBuf>,
}

impl Recovery {
    /// Recover every listed supervisor, each in a task of its own, and return at once; the
    /// handle resolves when every one is recovered. Each recovery reaches only its own socket
    /// and its own ledger, so they all run at once: a wedged supervisor holds back its own
    /// workspace, never the host's others.
    pub fn start(self, peers: Arc<dyn SupervisorPeers>) -> JoinHandle<()> {
        let Self { manager, sockets } = self;
        tokio::spawn(async move {
            let supervisors = sockets.len();
            let Ok(()) = timing::timed_async(RECOVERY_SCOPE, "startup", async move {
                timing::attribute(
                    RECOVERY_SCOPE,
                    format_args!("startup"),
                    "supervisors",
                    supervisors,
                );
                let mut recoveries = JoinSet::new();
                for socket in sockets {
                    recoveries.spawn(Arc::clone(&manager).recover_one(Arc::clone(&peers), socket));
                }
                while let Some(recovered) = recoveries.join_next().await {
                    if let Err(error) = recovered {
                        cowshed_os::eprintln!(
                            "cowshed: recovering a workspace supervisor failed: {error}"
                        );
                    }
                }
                Ok::<(), Infallible>(())
            })
            .await;
        })
    }
}

/// Takes a socket out of [`SupervisorManager::recovering`] when its recovery ends, however it
/// ends: a recovery that panicked must not leave its workspace refused for the daemon's life.
struct Recovered<'a> {
    manager: &'a SupervisorManager,
    socket: &'a Path,
}

impl Drop for Recovered<'_> {
    fn drop(&mut self) {
        self.manager
            .recovering
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(self.socket);
    }
}

/// Ask the supervisor of another build serving `socket` to drain, in a span of its own that
/// names the socket and reports the pid that answered, or why none did; once that process
/// exits, end any job it left running.
async fn drain_other_build(peers: &dyn SupervisorPeers, socket: &Path) {
    let step = format!("drain {}", socket.display());
    let drained = timing::timed_async(RECOVERY_SCOPE, step.clone(), async {
        let drained = peers.drain(socket).await;
        match &drained {
            Ok(draining) => {
                timing::attribute(RECOVERY_SCOPE, format_args!("{step}"), "pid", draining.pid);
            }
            Err(error) => {
                timing::attribute(
                    RECOVERY_SCOPE,
                    format_args!("{step}"),
                    "error",
                    &error.message,
                );
            }
        }
        drained
    })
    .await;
    if let Ok(Draining { pid, exit }) = drained {
        recover_after_exit(pid, socket.to_path_buf(), exit);
    }
}

fn serving(
    socket: &Path,
    hello: &supervisor_socket::Hello,
    authority: &WorkspaceAuthoritySnapshot,
) -> Result<Ensured> {
    match standing(&hello.authority, authority) {
        Standing::Serves => Ok(Ensured {
            socket: socket.to_path_buf(),
            pid: hello.pid,
            authority: hello.authority.clone(),
        }),
        Standing::Behind | Standing::Elsewhere => Err(CowshedError::conflict(
            format!(
                "process {} serves workspace {}'s supervisor under incarnation {} and grant \
                 revision {}; this command needs incarnation {} and grant revision {} or newer",
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
        )),
    }
}

/// How long a lost supervisor's jobs get between TERM and KILL.
const LOST_JOB_GRACE: Duration = Duration::from_secs(2);

/// When a started supervisor ends: report how, and end any job it left running.
// WHY unbounded: a detached task: nothing waits on it; it reports the supervisor's end whenever
// that comes.
#[allow(clippy::disallowed_methods)]
fn watch_child(mut child: tokio::process::Child, socket: PathBuf) {
    let Some(pid) = child.id() else {
        return;
    };
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) if status.success() => {}
            Ok(status) => cowshed_os::eprintln!(
                "cowshed: workspace supervisor {} ended: {status}",
                socket.display()
            ),
            Err(error) => {
                cowshed_os::eprintln!(
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
            Ok(Err(error)) => cowshed_os::eprintln!(
                "cowshed: cannot watch workspace supervisor {} (pid {pid}): {error}",
                socket.display()
            ),
            Err(error) => cowshed_os::eprintln!(
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
            cowshed_os::eprintln!(
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
                cowshed_os::eprintln!(
                    "cowshed: ended jobs {:?}, left running by workspace supervisor {}",
                    ended.signalled,
                    socket.display()
                );
            }
            if !ended.unresolved.is_empty() {
                cowshed_os::eprintln!(
                    "cowshed: jobs {:?} of workspace supervisor {} left processes whose group \
                 leader is gone; not signalled, and kept in its ledger for the next supervisor",
                    ended.unresolved,
                    socket.display()
                );
            }
        }
        Ok(Err(error)) => cowshed_os::eprintln!(
            "cowshed: cannot end the jobs workspace supervisor {} left running: {error}",
            socket.display()
        ),
        Err(error) => cowshed_os::eprintln!("cowshed: ending lost jobs failed: {error}"),
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
    exit: &cowshed_os::process::ExitWatch,
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
        cowshed_os::eprintln!(
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
    let answered = async {
        same_build(&request)?;
        let request: ManagerRequest = serde_json::from_value(request)
            .map_err(|error| protocol_error(format!("malformed request: {error}")))?;
        match request {
            ManagerRequest::Ensure {
                project_root,
                authority,
                ..
            } => {
                let ensured = manager.ensure(&project_root, &authority.into()).await?;
                Ok(EnsureResponse::Serving {
                    socket: ensured.socket,
                    pid: ensured.pid,
                    authority: (&ensured.authority).into(),
                })
            }
            ManagerRequest::SameBuild { .. } => Ok(EnsureResponse::SameBuild),
        }
    };
    let response = answered.await.unwrap_or_else(EnsureResponse::Refused);
    write_json(&mut stream, &response).await
}

/// Refuse an ensure from another cowshed build by name, and with both builds as data
/// ([`OtherBuild`]): the controller that asked can run nothing here, and whoever holds it replaces
/// it without reading the sentence. Read before the rest of the request: another build's may carry
/// fields this one cannot decode, and the build is what says why.
fn same_build(request: &serde_json::Value) -> Result<()> {
    let ours = current_build()?;
    let theirs = request.get("build").and_then(serde_json::Value::as_str);
    if theirs == Some(ours.as_str()) {
        return Ok(());
    }
    Err(CowshedError::other_build(OtherBuild {
        daemon: ours.clone(),
        caller: theirs.map(BuildId::named),
    }))
}

/// Connect to the supervisor manager at `path` within the handshake bound: a manager that should
/// already be running answers its connect at once, and one that never does is named.
async fn connect_manager(path: &Path) -> io::Result<UnixStream> {
    let budget = cowshed_os::deadline::start(
        "supervisor manager connect",
        cowshed_os::deadline::HANDSHAKE,
    );
    let peer = cowshed_os::deadline::Peer::unix("supervisor manager", path);
    Ok(cowshed_os::deadline::connect_unix(&budget, &peer, path).await?)
}

/// Connect to the daemon socket at `path`. A refused connection while an install holds the slot
/// in `support` is that install restarting the daemon: a positive signal, so the connection waits
/// for the install and is made once more against the daemon it installed. Any other refusal is
/// returned as it is (05_gateway.md, "A land step waits for an install in progress"). Measured
/// 2026-10-20: a land step's connection made mid-restart was refused and failed the land.
pub(crate) async fn connect_across_install(path: &Path, support: &Path) -> io::Result<UnixStream> {
    let refused = match connect_manager(path).await {
        Ok(stream) => return Ok(stream),
        Err(error) => error,
    };
    let support = support.to_path_buf();
    let waited =
        tokio::task::spawn_blocking(move || crate::host_dirs::wait_for_running_install(&support))
            .await
            .map_err(io::Error::other)?;
    match waited {
        Ok(true) => connect_manager(path).await,
        Ok(false) => Err(refused),
        Err(error) => Err(io::Error::other(format!(
            "{refused}; and the install slot could not be read: {error}"
        ))),
    }
}

/// Ask the host's manager for the supervisor serving `authority`'s workspace.
pub async fn ensure(
    store_root: &Path,
    project_root: &Path,
    authority: &WorkspaceAuthoritySnapshot,
) -> Result<Ensured> {
    let path = manager_socket_path(store_root);
    let support = crate::host_dirs::support_directory(&crate::gateway_sessions::canonical_home()?);
    let mut stream = connect_across_install(&path, &support)
        .await
        .map_err(|error| {
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
        &ManagerRequest::Ensure {
            build: current_build()?.clone(),
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
        EnsureResponse::SameBuild => Err(protocol_error(
            "the supervisor manager answered an ensure with a build check",
        )),
    }
}

/// How long the manager may take to answer a build check: it answers before it does anything
/// else, so a manager that takes longer is not serving.
const SAME_BUILD_BOUND: Duration = Duration::from_secs(10);

/// Refuse, before a verb changes anything, a manager that runs another build than this one.
///
/// Every call a verb makes to a workspace's supervisor goes through the manager, which refuses
/// another build's by name ([`same_build`]). A verb that learns this at its first such call has
/// already done what came before it — a removal has taken its workspace's supervisor down and
/// marked its intent — and the refusal then arrives again where it would put that state back. So
/// the verb asks first. The error is the manager's own refusal, both builds as data
/// ([`OtherBuild`]), whose hint names the install that ends the disagreement.
///
/// No manager answering is not a disagreement: there is no other build to refuse this one, and
/// the verbs that need a daemon say so themselves. No socket file, or one no socket is attached to
/// (`deadline::occupant`), is none answering. A refused connect while a socket is still attached
/// (a full queue, or a holder that does not accept), or one that has not completed at the
/// handshake bound, is a manager that is there, and fails the verb before it changes anything.
pub async fn require_same_build(store_root: &Path) -> Result<()> {
    let path = manager_socket_path(store_root);
    let mut stream = match connect_manager(&path).await {
        Ok(stream) => stream,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
            match cowshed_os::deadline::occupant(&path) {
                Ok(
                    cowshed_os::deadline::Occupant::Absent
                    | cowshed_os::deadline::Occupant::Stopped,
                ) => {
                    return Ok(());
                }
                Ok(cowshed_os::deadline::Occupant::Live) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "the cowshed daemon at {} refuses a build check while a socket is still \
                             attached to it: its queue is full, or its holder does not accept",
                            path.display()
                        ),
                        "retry once the daemon answers; cowshed doctor --json",
                    ));
                }
                Err(probe) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "the cowshed daemon at {} refused a build check ({error}), and whether \
                             it is still there cannot be read: {probe}",
                            path.display()
                        ),
                        "cowshed doctor --json",
                    ));
                }
            }
        }
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "the cowshed daemon at {} did not take a build check: {error}",
                    path.display()
                ),
                "cowshed gateway start",
            ));
        }
    };
    write_json(
        &mut stream,
        &ManagerRequest::SameBuild {
            build: current_build()?.clone(),
        },
    )
    .await?;
    let response: EnsureResponse = tokio::time::timeout(SAME_BUILD_BOUND, read_json(&mut stream))
        .await
        .map_err(|_| protocol_error("the supervisor manager did not answer a build check"))??;
    match response {
        EnsureResponse::SameBuild => Ok(()),
        EnsureResponse::Refused(error) => Err(error),
        EnsureResponse::Serving { .. } => Err(protocol_error(
            "the supervisor manager answered a build check with a supervisor",
        )),
    }
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::{
        AsyncChildBackstop as _, AsyncUnixStreamBackstop as _, FileBackstop as _,
    };
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

    /// Exercise ProgramSpawner from a process that really inherited a foreground lock across
    /// exec. Its supervisor stays alive while that process closes both the inherited lock and an
    /// unrelated inheritable descriptor; both locks must be free and the report must still work.
    #[tokio::test]
    async fn a_spawned_supervisor_seals_inherited_descriptors_and_preserves_its_report() {
        use crate::held_lock::{Opened, hand_to, keep_across_exec};
        use std::fs::{File, OpenOptions, TryLockError};
        use std::process::Stdio;
        use tokio::io::AsyncWriteExt as _;

        const ROOT: &str = "COWSHED_TEST_SUPERVISOR_SEAL_ROOT";
        const HELD_FD: &str = "COWSHED_TEST_SUPERVISOR_SEAL_HELD_FD";
        const TEST: &str = concat!(
            "runtime::supervisor_manager::tests::",
            "a_spawned_supervisor_seals_inherited_descriptors_and_preserves_its_report"
        );
        let reported = CowshedError::conflict("supervisor descriptor boundary", "test complete");

        if let Some(root) = std::env::var_os(ROOT) {
            let root = PathBuf::from(root);
            let socket = root.join("control.sock");
            if std::env::var_os(START_REPORT_FD_ENV).is_some() {
                // The actual ProgramSpawner child: its session, stdio and report are real.
                // SAFETY: getsid/getpid only query the calling process.
                assert_eq!(unsafe { libc::getsid(0) }, unsafe { libc::getpid() });
                for descriptor in 0..=2 {
                    // SAFETY: F_GETFD accepts any descriptor number.
                    assert_ne!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
                }
                assert_eq!(std::io::stdin().read(&mut [0]).expect("null stdin"), 0);
                std::io::stdout()
                    .write_all(b"discarded supervisor stdout\n")
                    .expect("null stdout");
                let report = StartReport::inherited();
                assert!(
                    report.0.is_some(),
                    "the allowlisted report descriptor survives exec"
                );
                report.send(&reported).expect("send real startup report");
                let mut control = UnixStream::connect_bounded(&socket)
                    .await
                    .expect("connect to starter");
                control
                    .write_all(b"ready")
                    .await
                    .expect("announce live supervisor");
                let mut release = [0];
                control
                    .read_exact(&mut release)
                    .await
                    .expect("release byte");
                return;
            }

            // The foreground starter inherited this descriptor from the outer test process.
            let descriptor: RawFd = std::env::var(HELD_FD)
                .expect("inherited descriptor number")
                .parse()
                .expect("descriptor");
            // SAFETY: hand_to passed this owned descriptor across exec; no Rust value in this
            // process owns it yet. This value closes only the starter's inherited copy.
            let inherited = unsafe { File::from_raw_fd(descriptor) };
            let mut parent_released = [0];
            std::io::stdin()
                .read_exact(&mut parent_released)
                .expect("parent closed its lock copy");
            let lock_path = root.join("held.lock");
            assert!(matches!(
                File::open(&lock_path)
                    .expect("probe inherited lock")
                    .try_lock(),
                Err(TryLockError::WouldBlock)
            ));
            let unrelated_path = root.join("unrelated.lock");
            let unrelated = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&unrelated_path)
                .expect("open unrelated descriptor");
            unrelated.lock_bounded().expect("hold unrelated descriptor");
            keep_across_exec(unrelated.as_raw_fd()).expect("make unrelated descriptor inheritable");
            let listener = UnixListener::bind(&socket).expect("starter control listener");
            let (report_read, report_write) = io::pipe().expect("startup report pipe");
            let spawner = ProgramSpawner::new(
                std::env::current_exe().expect("test binary"),
                vec!["--exact".into(), TEST.into(), "--nocapture".into()],
            );
            let mut supervisor = spawner
                .spawn(
                    &root,
                    &WorkspaceName::new("descriptor-seal").expect("workspace"),
                    report_write,
                )
                .expect("spawn actual supervisor");
            let (mut control, _) = listener.accept().await.expect("accept supervisor");
            let mut ready = [0; 5];
            control.read_exact(&mut ready).await.expect("ready bytes");
            assert_eq!(&ready, b"ready");
            assert_eq!(read_start_report(report_read).await, Some(reported));
            drop(inherited);
            drop(unrelated);
            assert!(supervisor.try_wait().expect("supervisor state").is_none());
            assert!(
                File::open(&lock_path)
                    .expect("open foreground lock")
                    .try_lock()
                    .is_ok(),
                "the live supervisor retained the foreground's inherited lock"
            );
            assert!(
                File::open(&unrelated_path)
                    .expect("open unrelated lock")
                    .try_lock()
                    .is_ok(),
                "the live supervisor retained an arbitrary inheritable descriptor"
            );
            control.write_all(b"x").await.expect("release supervisor");
            assert!(
                supervisor
                    .wait_bounded()
                    .await
                    .expect("reap supervisor")
                    .success()
            );
            return;
        }

        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "supervisor-seal",
        );
        let held = Opened::open(&root.join("held.lock"))
            .expect("open foreground lock")
            .try_hold()
            .expect("take foreground lock")
            .expect("free foreground lock");
        let mut command =
            tokio::process::Command::new(std::env::current_exe().expect("test binary"));
        command
            .args(["--exact", TEST, "--nocapture"])
            .env(ROOT, &*root)
            .env(HELD_FD, held.descriptor().to_string())
            .env_remove(START_REPORT_FD_ENV)
            .stdin(Stdio::piped())
            .kill_on_drop(true);
        hand_to(command.as_std_mut(), vec![held.descriptor()]);
        let mut starter = command
            .spawn_locked()
            .expect("start foreground lock inheritor");
        drop(held);
        starter
            .stdin
            .take()
            .expect("starter stdin")
            .write_all(b"x")
            .await
            .expect("confirm parent released its copy");
        assert!(
            starter
                .wait_bounded()
                .await
                .expect("reap starter")
                .success()
        );
    }

    /// A daemon whose startup pass has finished.
    fn healed() -> Arc<crate::StartupHealState> {
        Arc::new(crate::StartupHealState::healed())
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
        let store = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-other",
        );
        std::fs::create_dir_all(store.join("run")).expect("run directory");
        let socket = manager_socket_path(&store);
        let listener = supervisor_socket::bind(&socket)
            .await
            .expect("bind the manager");
        let served = tokio::spawn(serve(
            listener,
            SupervisorManager::new(&*store, Box::new(NoSpawner), healed()),
        ));
        let mut stream = UnixStream::connect_bounded(&socket)
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
                daemon: current_build().expect("this build").clone(),
                caller: Some(serde_json::from_value(serde_json::json!("another build")).unwrap()),
            })
        );
        served.abort();
    }

    /// A verb asks the manager whether it runs this build before it changes anything: the
    /// manager's own build is answered, another build's is refused with both builds as data and a
    /// hint that names the install, and no manager at all is no disagreement.
    #[tokio::test]
    async fn a_build_check_is_answered_for_this_build_and_refused_for_another() {
        let store = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-check",
        );
        std::fs::create_dir_all(store.join("run")).expect("run directory");
        require_same_build(&store)
            .await
            .expect("nothing answers, so nothing disagrees");

        let socket = manager_socket_path(&store);
        let listener = supervisor_socket::bind(&socket)
            .await
            .expect("bind the manager");
        let served = tokio::spawn(serve(
            listener,
            SupervisorManager::new(&*store, Box::new(NoSpawner), healed()),
        ));
        require_same_build(&store)
            .await
            .expect("the manager runs this build");

        let mut stream = UnixStream::connect_bounded(&socket)
            .await
            .expect("reach the manager");
        write_json(
            &mut stream,
            &serde_json::json!({"verb": "sameBuild", "build": "another build"}),
        )
        .await
        .expect("ask");
        let response: EnsureResponse = read_json(&mut stream).await.expect("answer");
        let EnsureResponse::Refused(error) = response else {
            panic!("a build check from another build must be refused");
        };
        assert_eq!(error.code, ErrorCode::Conflict);
        assert_eq!(
            error.other_build_source(),
            Some(&OtherBuild {
                daemon: current_build().expect("this build").clone(),
                caller: Some(serde_json::from_value(serde_json::json!("another build")).unwrap()),
            })
        );
        assert!(error.hint.contains("cowshed setup"), "{}", error.hint);
        served.abort();
    }

    /// Where the supervisor of `workspace` of `acme/widget` listens in `store`.
    fn supervisor_socket_of(store: &Path, workspace: &str) -> PathBuf {
        cowshed_types::supervisor_socket::socket_path(
            store,
            &cowshed_types::repository::RepoId::parse("acme/widget").expect("repo id"),
            &cowshed_types::metadata::WorkspaceName::new(workspace).expect("workspace"),
        )
    }

    /// A fresh store with a `run` directory holding the socket file of the supervisor of each of
    /// `workspaces`. Unix socket paths are short; the per-user temporary directory is not.
    fn recovery_store(prefix: &str, workspaces: &[&str]) -> crate::temp_root::TempRoot {
        let store =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), prefix);
        std::fs::create_dir_all(store.join("run")).expect("run directory");
        for workspace in workspaces {
            std::fs::write(supervisor_socket_of(&store, workspace), b"").expect("a socket file");
        }
        store
    }

    /// Every socket is served by another build, so recovery asks each to drain.
    fn another_build(socket: &Path) -> Result<Option<supervisor_socket::Hello>> {
        Err(CowshedError::conflict(
            format!("{} is served by another build", socket.display()),
            "let it drain",
        ))
    }

    /// A drained supervisor that has already exited and left no ledger.
    fn drained() -> Draining {
        Draining {
            pid: 4242,
            exit: Box::new(|| Ok(())),
        }
    }

    /// Supervisors of another build whose drains answer only as the test opens `gate`.
    struct GatedPeers {
        gate: tokio::sync::Semaphore,
    }

    #[async_trait::async_trait]
    impl SupervisorPeers for GatedPeers {
        async fn hello_if_present(
            &self,
            socket: &Path,
        ) -> Result<Option<supervisor_socket::Hello>> {
            another_build(socket)
        }

        async fn drain(&self, _: &Path) -> Result<Draining> {
            self.gate.acquire().await.expect("the gate opens").forget();
            Ok(drained())
        }
    }

    /// While a supervisor from before the manager started still drains, the manager refuses an
    /// ensure of its workspace by type, carrying how many supervisors are left, rather than start
    /// a second supervisor that would race the recovery for the socket; once it is recovered,
    /// the same ensure is no longer refused as recovering.
    #[tokio::test]
    async fn an_ensure_of_a_workspace_still_draining_is_refused_as_recovering() {
        let authority: WorkspaceAuthoritySnapshot =
            serde_json::from_value::<AuthorityWire>(serde_json::json!({
                "repoId": "acme/widget",
                "workspace": "raven",
                "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                "grantRevision": 1,
                "lifecycleRevision": 1,
            }))
            .expect("authority")
            .into();
        let store = recovery_store("cowshed-recovering", &["other", "raven"]);
        let manager = SupervisorManager::new(&*store, Box::new(NoSpawner), healed());
        let peers = Arc::new(GatedPeers {
            gate: tokio::sync::Semaphore::new(0),
        });
        let gated: Arc<GatedPeers> = Arc::clone(&peers);
        let recovered = manager.recovery().start(gated);
        let listener = supervisor_socket::bind(&manager_socket_path(&store))
            .await
            .expect("bind the manager");
        let served = tokio::spawn(serve(listener, Arc::clone(&manager)));

        let two = SupervisorRecovery {
            supervisors: NonZeroUsize::new(2).expect("two"),
        };
        assert_eq!(manager.recovering(), Some(two));
        let refused = ensure(&store, Path::new("/repo"), &authority)
            .await
            .expect_err("raven's supervisor is still draining");
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert_eq!(refused.recovering_source(), Some(two));
        assert!(refused.message.contains("raven"), "{}", refused.message);

        peers.gate.add_permits(2);
        recovered.await.expect("the recovery ends");
        assert_eq!(manager.recovering(), None);
        let unrecovered = ensure(&store, Path::new("/repo"), &authority)
            .await
            .expect_err("this manager starts no supervisor");
        assert_eq!(
            unrecovered.recovering_source(),
            None,
            "{}",
            unrecovered.message
        );
        served.abort();
    }

    /// While the daemon's startup pass still mounts projects or restores sessions, the manager
    /// answers an ensure with the typed healing refusal, carrying how far the pass has got,
    /// instead of starting a supervisor in a workspace that may not be mounted yet; once the pass
    /// is over, the same ensure is no longer refused as healing.
    #[tokio::test]
    async fn an_ensure_while_the_startup_pass_heals_is_refused_as_healing() {
        let authority: WorkspaceAuthoritySnapshot =
            serde_json::from_value::<AuthorityWire>(serde_json::json!({
                "repoId": "acme/widget",
                "workspace": "raven",
                "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                "grantRevision": 1,
                "lifecycleRevision": 1,
            }))
            .expect("authority")
            .into();
        let store = recovery_store("cowshed-healing", &[]);
        let startup = Arc::new(crate::StartupHealState::mounting(3));
        let manager = SupervisorManager::new(&*store, Box::new(NoSpawner), Arc::clone(&startup));
        let listener = supervisor_socket::bind(&manager_socket_path(&store))
            .await
            .expect("bind the manager");
        let served = tokio::spawn(serve(listener, manager));

        let refused = ensure(&store, Path::new("/repo"), &authority)
            .await
            .expect_err("the startup pass is still mounting");
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert_eq!(
            refused.healing_source(),
            Some(cowshed_gateway_types::StartupHeal::Mounting {
                projects: NonZeroUsize::new(3).expect("three"),
            })
        );

        startup.restored();
        let healed = ensure(&store, Path::new("/repo"), &authority)
            .await
            .expect_err("this manager starts no supervisor");
        assert_eq!(healed.healing_source(), None, "{}", healed.message);
        served.abort();
    }

    /// Supervisors of another build whose drains answer only once every one of them is draining.
    struct BarrierPeers {
        all_draining: tokio::sync::Barrier,
    }

    #[async_trait::async_trait]
    impl SupervisorPeers for BarrierPeers {
        async fn hello_if_present(
            &self,
            socket: &Path,
        ) -> Result<Option<supervisor_socket::Hello>> {
            another_build(socket)
        }

        async fn drain(&self, _: &Path) -> Result<Draining> {
            self.all_draining.wait().await;
            Ok(drained())
        }
    }

    /// Supervisors of another build are drained at once, not one after another: neither drain
    /// answers until both are in flight, so a recovery that waited for one before asking the
    /// next would never end.
    #[tokio::test]
    async fn supervisors_of_another_build_drain_at_once() {
        let store = recovery_store("cowshed-drains", &["first", "second"]);
        let manager = SupervisorManager::new(&*store, Box::new(NoSpawner), healed());
        manager
            .recovery()
            .start(Arc::new(BarrierPeers {
                all_draining: tokio::sync::Barrier::new(2),
            }))
            .await
            .expect("both drains reach the barrier");
        assert_eq!(manager.recovering(), None);
    }

    /// Supervisors that are positively gone, recording every socket recovery asks about.
    #[derive(Default)]
    struct AskedPeers {
        asked: std::sync::Mutex<Vec<PathBuf>>,
    }

    #[async_trait::async_trait]
    impl SupervisorPeers for AskedPeers {
        async fn hello_if_present(
            &self,
            socket: &Path,
        ) -> Result<Option<supervisor_socket::Hello>> {
            self.asked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(socket.to_path_buf());
            Ok(None)
        }

        async fn drain(&self, socket: &Path) -> Result<Draining> {
            unreachable!("no supervisor of another build serves {}", socket.display())
        }
    }

    /// Recovery lists the supervisors' own sockets and nothing else in `run`: each supervisor's
    /// `<digest>.members.sock` membership endpoint is its twin, never a second supervisor, so it is
    /// neither counted as recovering nor asked who serves it, and neither is `manager.sock`
    /// (11_shell.md "Recovering at daemon start").
    #[tokio::test]
    async fn recovery_lists_supervisor_sockets_and_never_their_membership_twins() {
        let store = recovery_store("cowshed-members", &["raven"]);
        std::fs::write(manager_socket_path(&store), b"").expect("the manager's socket file");
        let raven = supervisor_socket_of(&store, "raven");
        std::fs::write(
            cowshed_types::supervisor_socket::membership_socket_path(&raven),
            b"",
        )
        .expect("raven's membership socket file");
        let manager = SupervisorManager::new(&*store, Box::new(NoSpawner), healed());
        let recovery = manager.recovery();
        assert_eq!(
            manager.recovering(),
            Some(SupervisorRecovery {
                supervisors: NonZeroUsize::new(1).expect("one"),
            })
        );
        let peers = Arc::new(AskedPeers::default());
        let asking: Arc<AskedPeers> = Arc::clone(&peers);
        recovery.start(asking).await.expect("the recovery ends");
        assert_eq!(
            *peers.asked.lock().unwrap_or_else(PoisonError::into_inner),
            [raven]
        );
        assert_eq!(manager.recovering(), None);
    }

    /// Names the socket [`serves_as_a_supervisor_of_another_build`] serves.
    const OTHER_BUILD_SOCKET: &str = "COWSHED_TEST_OTHER_BUILD_SOCKET";

    /// Not a test: the separate process a supervisor of another build runs as, for
    /// [`a_verb_stops_a_supervisor_of_another_build_and_the_jobs_it_left`] and
    /// [`a_land_step_waits_for_a_supervisor_of_another_build_and_never_ends_it`]. It answers every
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
        let store = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-stop",
        );
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
                .spawn_locked()
                .expect("start the supervisor of another build");
        while UnixStream::connect_bounded(&socket).await.is_err() {
            if let Some(status) = supervisor.try_wait().expect("poll the supervisor") {
                panic!("the supervisor of another build exited before serving: {status}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // `cat` from PATH: NixOS keeps nothing but `sh` and `env` in /bin and /usr/bin. It reads
        // the stdin pipe `job` holds, so it runs until the stop signals it.
        let mut job = tokio::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            .spawn_locked()
            .expect("a job leading its own group");
        let group = job.id().expect("the job runs");
        let ledger = super::super::job_groups::ledger_path(&socket);
        let leader = super::super::job_groups::GroupLeader::observe(group).expect("leader");
        super::super::job_groups::Recorder::new(ledger.clone())
            .record(
                [super::super::job_groups::JobGroupClaim::group(7, leader)].into_iter(),
                &[],
            )
            .expect("ledger");
        let foreign = std::fs::symlink_metadata(&socket)
            .expect("its socket")
            .ino();

        let stopped = stop_other_build(&socket).await.expect("stopped");

        assert_eq!(Some(stopped.pid), supervisor.id());
        assert_eq!(
            supervisor
                .wait_bounded()
                .await
                .expect("supervisor")
                .signal(),
            Some(libc::SIGTERM)
        );
        assert_eq!(
            job.wait_bounded().await.expect("job").signal(),
            Some(libc::SIGTERM)
        );
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
    }

    /// A land step that meets a supervisor of another build waits on that process's exit: past
    /// its ceiling it refuses naming the pid and leaves the supervisor running, and once the
    /// supervisor exits the wait answers at once.
    #[tokio::test]
    async fn a_land_step_waits_for_a_supervisor_of_another_build_and_never_ends_it() {
        let store = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-wait",
        );
        std::fs::create_dir_all(store.join("run")).expect("run directory");
        let socket = store.join("run/other.sock");
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
                .spawn_locked()
                .expect("start the supervisor of another build");
        while UnixStream::connect_bounded(&socket).await.is_err() {
            if let Some(status) = supervisor.try_wait().expect("poll the supervisor") {
                panic!("the supervisor of another build exited before serving: {status}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let pid = supervisor.id().expect("it runs");

        let ceiling = Duration::from_millis(300);
        let refused = supervisor_socket::await_other_build_retired(
            &socket,
            std::time::Instant::now() + ceiling,
        )
        .await
        .expect_err("past its ceiling the step refuses");
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert!(
            refused.message.contains(&format!("pid {pid}"))
                && refused.hint.contains(&format!("kill {pid}")),
            "the refusal names the supervisor and the command that ends its jobs: {refused:?}"
        );
        assert!(
            supervisor.try_wait().expect("poll").is_none(),
            "the refusal leaves the supervisor and its jobs running"
        );

        // A ceiling no run reaches: past a ceiling the wait refuses, so an `Ok` is the exit event.
        let waited = tokio::spawn({
            let socket = socket.clone();
            async move {
                supervisor_socket::await_other_build_retired(
                    &socket,
                    std::time::Instant::now() + Duration::from_secs(24 * 60 * 60),
                )
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !waited.is_finished(),
            "the step waits while the supervisor runs"
        );
        supervisor.start_kill().expect("end the supervisor");
        waited
            .await
            .expect("the wait")
            .expect("the supervisor's exit ends the wait, not its ceiling");
    }

    /// A daemon socket that refuses while an install holds the slot is waited out and connected
    /// to once the install has its daemon serving; one that refuses with no install running is
    /// refused at once, waiting for nothing.
    #[tokio::test]
    async fn a_refused_daemon_connection_waits_for_a_running_install_only() {
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-across",
        );
        let socket = root.join("manager.sock");
        let support = root.join("support");
        std::fs::create_dir(&support).expect("support");

        let refused = connect_across_install(&socket, &support)
            .await
            .expect_err("nothing serves and no install runs");
        assert_eq!(refused.kind(), io::ErrorKind::NotFound, "{refused}");

        let install = std::fs::File::create(support.join(crate::host_dirs::INSTALL_LOCK_NAME))
            .expect("the install's lock");
        install.try_lock().expect("take the free install slot");
        let connecting = tokio::spawn({
            let (socket, support) = (socket.clone(), support.clone());
            async move { connect_across_install(&socket, &support).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !connecting.is_finished(),
            "the refusal waits for the install"
        );
        // The install's daemon serves, and only then does the install let the slot go.
        let _daemon = tokio::net::UnixListener::bind(&socket).expect("the new daemon");
        drop(install);
        connecting
            .await
            .expect("the connecting task")
            .expect("reaches the daemon the install started");
    }

    /// Measured 2026-10-21: the manager started a second supervisor beside a live one whose hello
    /// had timed out on a saturated drive, and the second failed as "a supervisor or lifecycle
    /// operation owns" the socket. A hello that does not answer is no proof that nothing serves:
    /// ensure returns that timeout as itself and starts nothing.
    #[tokio::test(start_paused = true)]
    async fn a_hello_that_times_out_is_returned_and_starts_no_second_supervisor() {
        let store = crate::temp_parent::scratch_parent().join(format!(
            "cowshed-slow-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        let authority = WorkspaceAuthoritySnapshot {
            repo_id: cowshed_types::repository::RepoId::parse("acme/widget").unwrap(),
            workspace: WorkspaceName::new("raven").unwrap(),
            workspace_incarnation: cowshed_types::metadata::WorkspaceIncarnation::new(
                "0198f2c0b7e34dc795f17b238b331c80",
            )
            .unwrap(),
            grant_revision: 1,
            lifecycle_revision: 1,
        };
        let socket = cowshed_types::supervisor_socket::socket_path(
            &store,
            &authority.repo_id,
            &authority.workspace,
        );
        std::fs::create_dir_all(socket.parent().unwrap()).expect("run directory");
        // A live supervisor too busy to answer: it accepts and holds the connection, and says
        // nothing.
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind the busy supervisor");
        let busy = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        let manager = SupervisorManager::new(&store, Box::new(NoSpawner), healed());

        let refused = manager
            .ensure(Path::new("/repo"), &authority)
            .await
            .expect_err("a supervisor that does not answer is not a vacancy");

        assert!(
            refused.message.starts_with("TIMEOUT: hello on ")
                && refused
                    .message
                    .contains("no answer within its handshake bound"),
            "the refusal is the hello's TIMEOUT: {refused:?}"
        );
        assert!(
            !refused.message.contains("starts no supervisor"),
            "no second supervisor was started: {refused:?}"
        );
        busy.abort();
        std::fs::remove_dir_all(store).expect("cleanup");
    }
}
