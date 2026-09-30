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
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use super::supervisor::WorkspaceAuthoritySnapshot;
use super::supervisor_socket::{
    self, AuthorityWire, PROTOCOL_VERSION, protocol_error, read_json, verify_peer, write_json,
};
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::WorkspaceName;

/// How long a started supervisor may take to open its project, mount its workspace and answer.
const START_BOUND: Duration = Duration::from_secs(180);
/// How often a starting supervisor is asked whether it serves yet.
const START_POLL: Duration = Duration::from_millis(50);

/// Where the host's manager listens.
pub fn manager_socket_path(store_root: &Path) -> PathBuf {
    store_root.join("run").join("manager.sock")
}

/// What starts a workspace's supervisor process.
pub trait SupervisorSpawner: Send + Sync + 'static {
    fn spawn(
        &self,
        project_root: &Path,
        workspace: &WorkspaceName,
    ) -> io::Result<tokio::process::Child>;
}

/// Production: this same binary's hidden verb, in a new session, stdin closed, stdout discarded
/// and stderr to the daemon's own log.
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
    ) -> io::Result<tokio::process::Child> {
        let mut command = tokio::process::Command::new(&self.executable);
        command
            .args(&self.arguments)
            .arg(project_root)
            .arg(workspace.as_str())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(false);
        // SAFETY: `setsid` between fork and exec is async-signal-safe and touches only this
        // child. A session of its own takes the supervisor out of the daemon's process group,
        // which a service manager ends with the daemon.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command.spawn()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EnsureRequest {
    protocol: u32,
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
                // A socket nothing answers is what a supervisor that died leaves; the next
                // supervisor for that workspace replaces it.
                Err(error) => eprintln!(
                    "cowshed: workspace supervisor socket {} does not answer: {}",
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
        let mut child = self
            .spawner
            .spawn(project_root, &authority.workspace)
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot start the supervisor of workspace {}: {error}",
                        authority.workspace
                    ),
                    "reinstall cowshed with `cowshed gateway start`",
                )
            })?;
        let deadline = tokio::time::Instant::now() + START_BOUND;
        loop {
            if let Some(status) = child.try_wait().map_err(|error| {
                CowshedError::internal(format!("cannot wait for a supervisor: {error}"))
            })? {
                return Err(CowshedError::environment_missing(
                    format!(
                        "the supervisor of workspace {} exited ({status}) before serving",
                        authority.workspace
                    ),
                    "its reason is in the cowshed daemon log: ~/Library/Logs/cowshed/daemon-stderr.log",
                ));
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

/// Report a started supervisor's end.
fn watch_child(mut child: tokio::process::Child, socket: PathBuf) {
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
    });
}

/// Report the end of a supervisor this manager did not start, which it cannot `wait` for.
fn watch_pid(pid: u32, socket: PathBuf) {
    tokio::task::spawn_blocking(move || {
        if let Err(error) = wait_for_exit(pid) {
            eprintln!(
                "cowshed: cannot watch workspace supervisor {} (pid {pid}): {error}",
                socket.display()
            );
        }
    });
}

/// Block until `pid`, which is not this process's child, exits.
#[cfg(target_os = "macos")]
fn wait_for_exit(pid: u32) -> io::Result<()> {
    let ident = usize::try_from(pid).map_err(io::Error::other)?;
    // SAFETY: kqueue returns a new descriptor this function owns and closes.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kevent structs are plain data; `queue` is live until the close below.
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
        let registered = libc::kevent(queue, &change, 1, &mut event, 1, std::ptr::null());
        let outcome = if registered < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        };
        libc::close(queue);
        outcome
    };
    // ESRCH: it already exited.
    match result {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        other => other,
    }
}

/// Block until `pid`, which is not this process's child, exits.
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
    let request: EnsureRequest = read_json(&mut stream).await?;
    let response = if request.protocol == PROTOCOL_VERSION {
        match manager
            .ensure(&request.project_root, &request.authority.into())
            .await
        {
            Ok(ensured) => EnsureResponse::Serving {
                socket: ensured.socket,
                pid: ensured.pid,
                authority: (&ensured.authority).into(),
            },
            Err(error) => EnsureResponse::Refused(error),
        }
    } else {
        EnsureResponse::Refused(version_conflict(request.protocol))
    };
    write_json(&mut stream, &response).await
}

fn version_conflict(theirs: u32) -> CowshedError {
    CowshedError::conflict(
        format!(
            "the cowshed daemon speaks supervisor protocol {PROTOCOL_VERSION}; this cowshed speaks {theirs}"
        ),
        "run `cowshed gateway start` from the cowshed you mean to use",
    )
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
            protocol: PROTOCOL_VERSION,
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
