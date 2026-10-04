//! A shed's Nx daemon runs inside the shed's sandbox, kept alive by the shed's supervisor.
//!
//! Every boundary of a checkout shares one Nx daemon through the rendezvous record in the
//! checkout's own `.nx` ([`crate::capabilities::nx`]). Nx's daemon executes the project-graph
//! plugins of the checkout it serves, and a shed's checkout is unsigned code that may run only
//! inside the sandbox. An Nx client that cannot reach the daemon its record names starts one
//! itself — outside any sandbox when the client is a host shell — and Nx has no setting that
//! makes a client connect to a live daemon without ever starting one. So a shed's supervisor
//! keeps the daemon alive as one of the shed's own read-write background jobs: a host client
//! finds it live through the shared record and connects to it instead of starting its own.
//!
//! Main is the operator's own checkout and its daemon is the host's, so main's supervisor keeps
//! none. The keeper is no job between its probes, so it never holds an idle supervisor from
//! retiring; it probes only while the supervisor admits jobs.

use std::collections::HashMap;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use tokio::time::{Interval, MissedTickBehavior};

use crate::api::dto::{
    CommandArg, ExecCommand, ExecRequest, ExitStatus, JobId, JobInfo, RunSandboxMode, StdinSource,
    WorkspacePath,
};
use crate::capabilities::nx;
use crate::error::{CowshedError, Result};
use crate::metadata::WorkspaceRole;
use crate::sandbox::SandboxConfig;

/// How often a shed's supervisor probes its Nx daemon. Short, because from the daemon's death to
/// the next probe a host client finds no daemon and starts its own outside the sandbox, and a
/// probe is one small file read, one `kill(pid, 0)` and one local connect. Not shorter, because
/// a start job takes Nx's own startup, seconds, and no probe starts another while one runs.
pub(crate) const PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// The daemon of an Nx project, as its rendezvous record and the world agree on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Probe {
    /// The record names a running process whose socket accepts a connection.
    Live,
    /// No record: no daemon was started, or the one that was removed its record on exit.
    Unrecorded,
    /// A record that cannot be read as Nx writes it.
    Unreadable,
    /// The recorded process is gone.
    Dead,
    /// The recorded socket refuses a connection.
    Refused,
}

impl std::fmt::Display for Probe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Live => "live",
            Self::Unrecorded => "not recorded",
            Self::Unreadable => "recorded unreadably",
            Self::Dead => "recorded by a process that is gone",
            Self::Refused => "recorded at a socket that refuses connections",
        })
    }
}

/// `d/server-process.json` as Nx's daemon server writes it; `nxVersion` is not consulted.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    process_id: u32,
    socket_path: PathBuf,
}

/// Whether the daemon `record` names is live: the process it records runs and its socket accepts
/// a connection. The connection is closed unused, as Nx's own availability probe closes it.
pub(crate) fn probe(record: &Path) -> Probe {
    let bytes = match std::fs::read(record) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Probe::Unrecorded,
        Err(_) => return Probe::Unreadable,
    };
    let Ok(recorded) = serde_json::from_slice::<Record>(&bytes) else {
        return Probe::Unreadable;
    };
    // `kill` reads a pid of 0, or one past `pid_t`, as a process group: never one process.
    let Ok(pid @ 1..) = libc::pid_t::try_from(recorded.process_id) else {
        return Probe::Unreadable;
    };
    // A relative path would be resolved against this process's directory, not the daemon's.
    if !recorded.socket_path.is_absolute() {
        return Probe::Unreadable;
    }
    if !running(pid) {
        return Probe::Dead;
    }
    match UnixStream::connect(&recorded.socket_path) {
        Ok(_) => Probe::Live,
        Err(_) => Probe::Refused,
    }
}

/// Whether the process `pid` exists, by the null signal: nothing is delivered.
fn running(pid: libc::pid_t) -> bool {
    // SAFETY: signal 0 performs only the existence and permission checks, and `pid` is positive,
    // so it names one process and never a group.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    // A process this one may not signal still exists.
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// The Nx project whose daemon a supervisor keeps: the project Nx is rooted at under the
/// supervisor's read-write sandbox, when that sandbox writes the checkout and has Nx active.
pub(crate) fn kept_project(read_write: &SandboxConfig) -> Option<&Path> {
    match read_write.mode {
        crate::sandbox::RunSandboxMode::ReadWrite => nx::project_root(&read_write.capabilities),
        // Nx state then lives in the job's exec temp dir, and its daemon is no other boundary's.
        crate::sandbox::RunSandboxMode::ReadOnly => None,
    }
}

/// The read-write background job that starts the daemon of the Nx project at `project_root`:
/// the project's own `nx`, run from the project root inside the workspace at `workspace_root`.
pub(crate) fn start_request(workspace_root: &Path, project_root: &Path) -> Result<ExecRequest> {
    let relative = project_root.strip_prefix(workspace_root).map_err(|_| {
        CowshedError::integrity(
            format!(
                "Nx project {} is outside workspace {}",
                project_root.display(),
                workspace_root.display()
            ),
            "use a workspace-relative capability directory",
        )
    })?;
    // The workspace root itself has no `WorkspacePath`; a job without a cwd runs there, because a
    // project's supervisor has no default directory. Nx takes its root from
    // `NX_WORKSPACE_ROOT_PATH`, which the Nx capability owns, whatever the cwd.
    let cwd = if relative.as_os_str().is_empty() {
        None
    } else {
        Some(WorkspacePath::new(relative).map_err(|error| {
            CowshedError::integrity(error.to_string(), "use a workspace-relative Nx project")
        })?)
    };
    Ok(ExecRequest {
        command: ExecCommand::Argv(vec![
            CommandArg::new(project_root.join("node_modules/.bin/nx")),
            CommandArg::from("daemon"),
            CommandArg::from("--start"),
        ]),
        cwd,
        mode: RunSandboxMode::ReadWrite,
        env: HashMap::new(),
        trace: None,
        stdin: StdinSource::Empty,
        stdout_copy: None,
        stderr_copy: None,
    })
}

/// Say on the supervisor's stderr how the start job `job_id` went, `job` being its record, unless
/// it exited 0 and left the daemon live. The next probe finding the daemon down starts another.
pub(crate) fn report_start(
    job_id: JobId,
    job: Option<&JobInfo>,
    project_root: &Path,
    daemon: Probe,
) {
    let ending = match job.and_then(|info| info.exit.as_ref()) {
        Some(ExitStatus::Exited { code: 0 }) if daemon == Probe::Live => return,
        Some(ExitStatus::Exited { code }) => format!("exited {code}"),
        Some(ExitStatus::Signaled { signal, .. }) => format!("ended on signal {signal}"),
        None => match job {
            Some(info) => format!("ended {:?} with no exit status", info.state),
            None => "has no record in its supervisor".to_owned(),
        },
    };
    let next = match daemon {
        Probe::Live => "",
        Probe::Unrecorded | Probe::Unreadable | Probe::Dead | Probe::Refused => {
            "; starting another"
        }
    };
    eprintln!(
        "cowshed: Nx daemon start job {} for {} {ending}, and the daemon is {daemon}{next}",
        job_id.get(),
        project_root.display()
    );
}

/// A shed supervisor's keeper of its Nx daemon: when to probe next, and the start job it waits on.
#[derive(Debug)]
pub(crate) struct NxDaemonKeeper {
    probes: Interval,
    /// The start job submitted last, until a probe sees it ended.
    pub(super) starting: Option<JobId>,
}

impl NxDaemonKeeper {
    /// The keeper of a supervisor of a workspace in `role`: a shed's, never main's. Its first
    /// probe is due at once, so a shed's daemon is ensured as its supervisor starts.
    pub(crate) fn for_role(role: WorkspaceRole) -> Option<Self> {
        match role {
            WorkspaceRole::Main => None,
            WorkspaceRole::Workspace => {
                let mut probes = tokio::time::interval(PROBE_INTERVAL);
                // A probe that waited behind a slow admission is not made up in a burst.
                probes.set_missed_tick_behavior(MissedTickBehavior::Delay);
                Some(Self {
                    probes,
                    starting: None,
                })
            }
        }
    }
}

/// The next probe of `keeper`; never, for a supervisor that keeps no daemon.
pub(crate) async fn next_probe(keeper: &mut Option<NxDaemonKeeper>) {
    match keeper {
        Some(keeper) => {
            keeper.probes.tick().await;
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;

    use super::*;

    fn write_record(record: &Path, pid: u32, socket: &Path) {
        std::fs::write(
            record,
            serde_json::json!({
                "processId": pid,
                "socketPath": socket,
                "nxVersion": "23.2.1",
            })
            .to_string(),
        )
        .expect("record");
    }

    /// The daemon is live only while its record names a running process whose socket accepts:
    /// an absent record, a dead pid and a socket nobody listens on are each a daemon to start.
    #[test]
    fn a_daemon_is_live_only_while_its_recorded_process_runs_and_its_socket_accepts() {
        // Short, under /tmp: a Unix socket path is bounded at 104 bytes on macOS.
        let root = Path::new("/tmp").join(format!(
            "cs-nxd-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(&root).expect("scratch");
        let record = root.join("server-process.json");
        let socket = root.join("d.sock");
        let own = std::process::id();

        assert_eq!(probe(&record), Probe::Unrecorded);

        let listener = UnixListener::bind(&socket).expect("listen");
        write_record(&record, own, &socket);
        assert_eq!(probe(&record), Probe::Live);

        let mut exited =
            crate::fork_lock::Spawn::spawn_locked(&mut std::process::Command::new("/usr/bin/true"))
                .expect("spawn");
        let dead = exited.id();
        exited.wait().expect("reap");
        write_record(&record, dead, &socket);
        assert_eq!(probe(&record), Probe::Dead);

        // The socket file outlives its listener, so only a connect tells it is served.
        drop(listener);
        write_record(&record, own, &socket);
        assert_eq!(probe(&record), Probe::Refused);

        std::fs::write(&record, b"{\"processId\":0,\"socketPath\":\"/tmp/x\"}").expect("record");
        assert_eq!(probe(&record), Probe::Unreadable);

        std::fs::remove_dir_all(&root).expect("remove scratch");
    }
}
