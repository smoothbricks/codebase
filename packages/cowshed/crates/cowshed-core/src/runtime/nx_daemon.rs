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
//! none.
//!
//! A kept daemon is a cost per shed: a resident Node process, its plugin workers, and a file
//! watcher that recomputes the project graph on every change in the tree. Nx stops a daemon after
//! three hours without a request or a file event, a bound it neither lets a caller set nor
//! applies to a tree that keeps changing, so the supervisor ends the daemon itself. A shed is
//! used while its supervisor runs a job or holds a session, while a process other than the daemon
//! holds one of its Nx task databases open (a host shell's run), and for [`IDLE`] after a daemon
//! comes up. Once none of that has happened for [`IDLE`] the keeper stops the daemon, as
//! `nx daemon --stop` does, and starts none until the shed is used again. The keeper is no job
//! between its probes, and the supervisor does not retire until its keeper holds no daemon, so a
//! daemon never outlives the supervisor that watches it.

use std::collections::HashMap;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use tokio::time::{Instant, Interval, MissedTickBehavior};

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
/// probe is one small file read, one process-state query and one local connect. Not shorter,
/// because a start job takes Nx's own startup, seconds, and no probe starts another while one
/// runs.
pub(crate) const PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// How long a shed must show no use of Nx before its supervisor stops the daemon it keeps. Long
/// enough that a gate's runs, an edit and the next run share one warm daemon (a cold one
/// recomputes the project graph and every plugin worker's state), short enough that a shed
/// whose agent has finished costs its host five minutes of a daemon, not the rest of the day.
pub(crate) const IDLE: Duration = Duration::from_secs(5 * 60);

/// How often a probe of an otherwise idle shed also looks for a client at work: one open-file
/// query over every process of the host, which took 0.34 to 0.73 s (median 0.46 s) over 669
/// processes at load 90, against the file read and one connect of a probe. A run holds a task
/// database for as long as its tasks run, far longer than this, so a look finds it; and the
/// stop itself looks once more.
pub(crate) const CLIENT_LOOK_INTERVAL: Duration = Duration::from_secs(30);

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
    read_and_probe(record).0
}

/// The pid of the daemon `record` names when that daemon is live, taken from the same read of
/// the record that verified it: a record rewritten between a check and a signal can never aim
/// the signal at another process.
pub(crate) fn live_pid(record: &Path) -> Option<libc::pid_t> {
    match read_and_probe(record) {
        (Probe::Live, Some((pid, _))) => Some(pid),
        _ => None,
    }
}

/// A live daemon's record, as the one read that verified it holds it.
#[cfg(target_os = "macos")]
pub(crate) struct LiveRecord {
    pub pid: libc::pid_t,
    pub bytes: Vec<u8>,
}

/// The record `record` holds when the daemon it names is live: its bytes and pid, from the one
/// read that verified the daemon, so a copy of it can only ever name that daemon.
#[cfg(target_os = "macos")]
pub(crate) fn live_record(record: &Path) -> Option<LiveRecord> {
    match read_and_probe(record) {
        (Probe::Live, Some((pid, bytes))) => Some(LiveRecord { pid, bytes }),
        _ => None,
    }
}

/// The probe of `record`, and for a live daemon the pid and bytes of the read that verified it.
fn read_and_probe(record: &Path) -> (Probe, Option<(libc::pid_t, Vec<u8>)>) {
    let bytes = match std::fs::read(record) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return (Probe::Unrecorded, None),
        Err(_) => return (Probe::Unreadable, None),
    };
    let Ok(recorded) = serde_json::from_slice::<Record>(&bytes) else {
        return (Probe::Unreadable, None);
    };
    // `kill` reads a pid of 0, or one past `pid_t`, as a process group: never one process.
    let Ok(pid @ 1..) = libc::pid_t::try_from(recorded.process_id) else {
        return (Probe::Unreadable, None);
    };
    // A relative path would be resolved against this process's directory, not the daemon's.
    if !recorded.socket_path.is_absolute() {
        return (Probe::Unreadable, None);
    }
    if !running(pid) {
        return (Probe::Dead, None);
    }
    match UnixStream::connect(&recorded.socket_path) {
        Ok(_) => (Probe::Live, Some((pid, bytes))),
        Err(_) => (Probe::Refused, None),
    }
}

/// Whether the process `pid` has not exited ([`crate::process::running`]): a daemon that exited
/// and is still a zombie awaiting its reaper is gone. A process whose state cannot be read is
/// left for the socket to judge.
fn running(pid: libc::pid_t) -> bool {
    crate::process::running(pid).unwrap_or(true)
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

/// A shed supervisor's keeper of its Nx daemon: when to probe next, the start job it waits on, and
/// how long the shed has gone without using Nx.
#[derive(Debug)]
pub(crate) struct NxDaemonKeeper {
    probes: Interval,
    /// The start job submitted last, until a probe sees it ended.
    pub(super) starting: Option<JobId>,
    /// The last probe that found the shed in use, or a daemon newly up.
    last_use: Instant,
    /// Whether the previous probe found the daemon live, to tell a daemon coming up from one
    /// that has been up.
    daemon_was_live: bool,
    /// When a probe last looked for a client at work ([`CLIENT_LOOK_INTERVAL`]).
    last_look: Option<Instant>,
    /// Whether the last probe found the shed unused for [`IDLE`] and no daemon live: nothing is
    /// left for the supervisor to wait for.
    pub(super) settled: bool,
}

/// What a probe decides about the daemon of a shed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Verdict {
    /// The shed is in use, or its daemon is young: one is to run.
    Keep,
    /// The shed has gone unused for [`IDLE`]: none is to run, and the one that did is stopped.
    Release,
}

impl NxDaemonKeeper {
    /// The keeper of a supervisor of a workspace in `role`: a shed's, never main's. Its first
    /// probe is due at once, so a shed's daemon is ensured as its supervisor starts, and that
    /// start counts as use.
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
                    last_use: Instant::now(),
                    daemon_was_live: false,
                    last_look: None,
                    settled: false,
                })
            }
        }
    }

    /// Judge one probe of the Nx project at `project_root`, whose daemon the probe found
    /// `daemon`, `working` being whether the supervisor runs a job or holds a session. Stops the
    /// daemon when the shed has gone unused for [`IDLE`].
    ///
    /// Use is `working`, or a process other than the daemon holding a task database open.
    /// Nx's graph and hash passes open none, so a client between its tasks, or one that only asks
    /// the daemon for the graph, shows no use of its own: [`IDLE`] outlasts both. The look at the
    /// databases is made at most every [`CLIENT_LOOK_INTERVAL`], and only for a shed with a live
    /// daemon and no job.
    pub(crate) async fn judge(
        &mut self,
        project_root: &Path,
        working: bool,
        daemon: Probe,
    ) -> Verdict {
        let live = daemon == Probe::Live;
        let now = Instant::now();
        let look = live
            && !working
            && self
                .last_look
                .is_none_or(|looked| now.duration_since(looked) >= CLIENT_LOOK_INTERVAL);
        let in_use = working
            || (look && {
                self.last_look = Some(now);
                clients_at_work(project_root).await
            });
        // A daemon that has just come up was asked for by someone: it gets a whole interval,
        // however idle the shed looks.
        if in_use || (live && !self.daemon_was_live) {
            self.last_use = now;
        }
        self.daemon_was_live = live;
        if now.duration_since(self.last_use) < IDLE {
            self.settled = false;
            return Verdict::Keep;
        }
        if !live {
            self.settled = true;
            return Verdict::Release;
        }
        self.settled = false;
        match stop(project_root).await {
            Stop::Stopped => Verdict::Release,
            Stop::InUse => {
                self.last_use = Instant::now();
                Verdict::Keep
            }
            Stop::Failed(why) => {
                eprintln!(
                    "cowshed: the Nx daemon of {} has been idle for {}s and did not stop: {why}; \
                     the next probe in {}s tries again",
                    project_root.display(),
                    IDLE.as_secs(),
                    PROBE_INTERVAL.as_secs()
                );
                Verdict::Release
            }
        }
    }
}

/// Whether a process other than the daemon holds one of the Nx project's task databases open
/// ([`crate::build_volume::nx::in_use`]). A look that fails proves the shed idle no more than a
/// holder does, so it counts as use, and is said.
async fn clients_at_work(project_root: &Path) -> bool {
    let data = nx::workspace_data(project_root);
    match tokio::task::spawn_blocking(move || crate::build_volume::nx::in_use(&data)).await {
        Ok(Ok(held)) => held.is_some(),
        Ok(Err(error)) => {
            eprintln!(
                "cowshed: cannot tell whether a client of the Nx daemon of {} is at work, so the \
                 shed counts as in use: {error}",
                project_root.display()
            );
            true
        }
        Err(error) => {
            eprintln!(
                "cowshed: the look at the clients of the Nx daemon of {} failed, so the shed \
                 counts as in use: {error}",
                project_root.display()
            );
            true
        }
    }
}

/// How stopping the daemon of an idle shed ended.
enum Stop {
    /// The daemon exited, or was already gone.
    Stopped,
    /// A process holds a task database open: the shed is in use after all, and nothing was
    /// stopped.
    InUse,
    Failed(String),
}

/// Stop the daemon of the Nx project at `project_root` as `nx daemon --stop` does, unless a
/// process other than the daemon holds a task database open
/// ([`crate::build_volume::nx::close_workspace_data`]), and say so on the supervisor's stderr.
async fn stop(project_root: &Path) -> Stop {
    let pid = live_pid(&nx::daemon_record(project_root));
    let data = nx::workspace_data(project_root);
    let closed =
        tokio::task::spawn_blocking(move || crate::build_volume::nx::close_workspace_data(&data))
            .await;
    match closed {
        Ok(Ok(Ok(()))) => {
            if let Some(pid) = pid {
                eprintln!(
                    "cowshed: stopped the Nx daemon (pid {pid}) of {}: the shed ran nothing and \
                     held no Nx database for {}s",
                    project_root.display(),
                    IDLE.as_secs()
                );
            }
            Stop::Stopped
        }
        Ok(Ok(Err(crate::build_volume::nx::Busy::Held { .. }))) => Stop::InUse,
        Ok(Ok(Err(busy))) => Stop::Failed(busy.to_string()),
        Ok(Err(error)) => Stop::Failed(error.to_string()),
        Err(error) => Stop::Failed(format!("the stop did not finish: {error}")),
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
    /// an absent record, a dead pid -- an exited one its reaper has not yet reaped included --
    /// and a socket nobody listens on are each a daemon to start.
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
        // Exited, not yet reaped: a zombie, which the null signal still reaches. The socket is
        // still served, so only the process's own state can tell it is gone.
        // SAFETY: an all-zero `siginfo_t` is a valid value of the plain C struct.
        let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
        // SAFETY: `info` is a valid out-pointer; WNOWAIT leaves the child waitable.
        let waited =
            unsafe { libc::waitid(libc::P_PID, dead, &mut info, libc::WEXITED | libc::WNOWAIT) };
        assert_eq!(waited, 0, "{}", io::Error::last_os_error());
        write_record(&record, dead, &socket);
        assert_eq!(probe(&record), Probe::Dead, "a zombie daemon is gone");
        exited.wait().expect("reap");
        assert_eq!(probe(&record), Probe::Dead);

        // The socket file outlives its listener, so only a connect tells it is served.
        drop(listener);
        write_record(&record, own, &socket);
        assert_eq!(probe(&record), Probe::Refused);

        std::fs::write(&record, b"{\"processId\":0,\"socketPath\":\"/tmp/x\"}").expect("record");
        assert_eq!(probe(&record), Probe::Unreadable);

        std::fs::remove_dir_all(&root).expect("remove scratch");
    }

    /// A project with no Nx state: stopping its daemon finds none to signal, so a verdict made
    /// against it touches no process.
    const NO_PROJECT: &str = "/cowshed-test-no-such-nx-project";

    fn keeper() -> NxDaemonKeeper {
        NxDaemonKeeper::for_role(WorkspaceRole::Workspace).expect("a shed keeps a daemon")
    }

    async fn judge(keeper: &mut NxDaemonKeeper, working: bool, daemon: Probe) -> Verdict {
        keeper.judge(Path::new(NO_PROJECT), working, daemon).await
    }

    /// A shed keeps a daemon for [`IDLE`] from its last use and no longer: its supervisor's start
    /// is a use, a job or session is one at every probe, and a probe that finds the shed in use
    /// after it was released asks for a daemon again.
    #[tokio::test(start_paused = true)]
    async fn a_keeper_releases_the_daemon_of_a_shed_unused_for_the_idle_interval() {
        let mut keeper = keeper();
        tokio::time::advance(IDLE - Duration::from_secs(1)).await;
        assert_eq!(
            judge(&mut keeper, false, Probe::Unrecorded).await,
            Verdict::Keep
        );
        assert!(!keeper.settled, "a daemon is still wanted");
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            judge(&mut keeper, false, Probe::Unrecorded).await,
            Verdict::Release
        );
        assert!(keeper.settled, "nothing is left to keep or to stop");

        // A job runs: the shed is used, so a daemon is wanted again, a whole interval of it.
        assert_eq!(judge(&mut keeper, true, Probe::Dead).await, Verdict::Keep);
        assert!(!keeper.settled);
        tokio::time::advance(IDLE - Duration::from_secs(1)).await;
        assert_eq!(judge(&mut keeper, false, Probe::Dead).await, Verdict::Keep);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            judge(&mut keeper, false, Probe::Dead).await,
            Verdict::Release
        );
    }

    /// A daemon that comes up, started by the keeper or by a client that found none, gets a whole
    /// interval however long the shed had been idle before it. A released shed is settled only
    /// once no daemon is live, so its supervisor never retires over one.
    #[tokio::test(start_paused = true)]
    async fn a_daemon_that_comes_up_gets_a_whole_interval_and_a_live_one_unsettles_the_shed() {
        let mut keeper = keeper();
        tokio::time::advance(IDLE * 2).await;
        assert_eq!(
            judge(&mut keeper, false, Probe::Unrecorded).await,
            Verdict::Release
        );
        assert!(keeper.settled);

        // A host client starts a daemon in the idle shed.
        assert_eq!(judge(&mut keeper, false, Probe::Live).await, Verdict::Keep);
        assert!(!keeper.settled);
        tokio::time::advance(IDLE - Duration::from_secs(1)).await;
        assert_eq!(judge(&mut keeper, false, Probe::Live).await, Verdict::Keep);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            judge(&mut keeper, false, Probe::Live).await,
            Verdict::Release,
            "the unused daemon is stopped"
        );
        assert!(
            !keeper.settled,
            "the daemon the probe found live is not yet seen gone"
        );
        assert_eq!(
            judge(&mut keeper, false, Probe::Dead).await,
            Verdict::Release
        );
        assert!(keeper.settled, "the next probe finds it gone");
    }
}
