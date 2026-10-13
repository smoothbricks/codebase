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

/// What stopping the daemon a record names may do. The record only nominates a pid: the job's
/// trusted ledger decides whether that pid is the job's own, and only then is it signalled.
#[cfg(target_os = "macos")]
pub(crate) enum DaemonStopPlan {
    /// No process runs the record's pid, or no record exists: there is nothing to stop.
    NotRunning,
    /// A live process the record names that the ledger does not prove the job's. Never signalled.
    Unproven { pid: i32, reason: String },
    /// The job's own process, proven by the ledger, with its exit watch registered before any
    /// signal is sent. Signalling it reaches that process or nothing.
    Owned {
        process: super::job_groups::OwnedGroupProcess,
        exit: super::job_groups::ExitWatch,
    },
}

/// Read the daemon `record` once, and decide what stopping its daemon may do, against the job
/// group `ledger` at `ledger`'s path. The record's modification time may not precede the process
/// it names, which a stale record does; that is a supplementary refusal only. Authority is the
/// ledger's alone: [`super::job_groups::owned_process`]. Unreadable or malformed records or ledgers,
/// and a pid below one, skip adoption before quiesce without signalling any nominee.
#[cfg(target_os = "macos")]
pub(crate) fn prepare_daemon_stop(
    record: &Path,
    ledger: &Path,
) -> std::result::Result<DaemonStopPlan, crate::api::dto::AdoptionSkip> {
    read_daemon_stop_plan(record, ledger).map_err(|error| {
        crate::api::dto::AdoptionSkip::LandingDaemonUnverifiable {
            record: record.to_owned(),
            reason: error.to_string(),
        }
    })
}

#[cfg(target_os = "macos")]
fn read_daemon_stop_plan(record: &Path, ledger: &Path) -> io::Result<DaemonStopPlan> {
    let record_error = |error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("cannot read daemon record {}: {error}", record.display()),
        )
    };
    let mut file = match std::fs::File::open(record) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(DaemonStopPlan::NotRunning);
        }
        Err(error) => return Err(record_error(error)),
    };
    // The modification time and the bytes come from the same descriptor, so they describe one write.
    let modified = file
        .metadata()
        .and_then(|metadata| metadata.modified())
        .map_err(record_error)?;
    let mut bytes = Vec::new();
    io::Read::read_to_end(&mut file, &mut bytes).map_err(record_error)?;
    let recorded: Record = serde_json::from_slice(&bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("cannot parse daemon record {}: {error}", record.display()),
        )
    })?;
    let pid = match libc::pid_t::try_from(recorded.process_id) {
        Ok(pid @ 1..) => pid,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "daemon record {} names no process: {}",
                    record.display(),
                    recorded.process_id
                ),
            ));
        }
    };
    let Some(named) = super::job_groups::process_record(pid).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot inspect process {pid} named by daemon record {}: {error}",
                record.display()
            ),
        )
    })?
    else {
        return Ok(DaemonStopPlan::NotRunning);
    };
    let written = modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "daemon record {} has an invalid modification time: {error}",
                    record.display()
                ),
            )
        })?
        .as_secs();
    if written < named.started_seconds {
        return Ok(DaemonStopPlan::Unproven {
            pid,
            reason: format!(
                "the record was written before process {pid} started, so it names another process"
            ),
        });
    }
    // A job's daemon runs inside its sandbox. Unsandboxed is refused here, but a sandboxed pid
    // still needs the ledger's proof: this guard only refuses, it never grants.
    if !crate::process::sandboxed(pid).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot inspect sandbox of process {pid}: {error}"),
        )
    })? {
        return Ok(DaemonStopPlan::Unproven {
            pid,
            reason: format!("process {pid} is not sandboxed, so it is not a job's daemon"),
        });
    }
    let Some(process) = super::job_groups::owned_process(ledger, pid).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot verify against job ledger {}: {error}",
                ledger.display()
            ),
        )
    })?
    else {
        return Ok(DaemonStopPlan::Unproven {
            pid,
            reason: format!("no job group in the ledger proves process {pid} is the job's own"),
        });
    };
    match process.watch_exit() {
        Ok(exit) => Ok(DaemonStopPlan::Owned { process, exit }),
        // It exited between the proof and its watch: nothing runs to stop.
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(DaemonStopPlan::NotRunning),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!("cannot watch exit of ledger-proven process {pid}: {error}"),
        )),
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

/// Whether the process `pid` has not exited ([`cowshed_os::process::running`]): a daemon that exited
/// and is still a zombie awaiting its reaper is gone. A process whose state cannot be read is
/// left for the socket to judge.
fn running(pid: libc::pid_t) -> bool {
    cowshed_os::process::running(pid).unwrap_or(true)
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
        admission_key: None,
        class: crate::api::dto::JobserverClass::Background,
        want: std::num::NonZeroUsize::MIN,
        wait_ms: None,
        scope: crate::api::dto::ExecScope::Workspace,
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
        let why = format!(
            "the shed ran nothing and held no Nx database for {}s",
            IDLE.as_secs()
        );
        match stop(project_root, &why).await {
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
/// ([`crate::build_volume::nx::close_workspace_data`]), and say on the supervisor's stderr that
/// it stopped and `why`.
async fn stop(project_root: &Path, why: &str) -> Stop {
    let pid = live_pid(&nx::daemon_record(project_root));
    let data = nx::workspace_data(project_root);
    let closed =
        tokio::task::spawn_blocking(move || crate::build_volume::nx::close_workspace_data(&data))
            .await;
    match closed {
        Ok(Ok(Ok(()))) => {
            if let Some(pid) = pid {
                eprintln!(
                    "cowshed: stopped the Nx daemon (pid {pid}) of {}: {why}",
                    project_root.display(),
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

/// End the daemon a retiring supervisor's keeper kept for the Nx project at `project_root`, so
/// none outlives the supervisor that watched it (04_sandbox.md). Its jobs have ended by now; a
/// host client still holding a task database keeps the daemon, and that is said.
pub(crate) async fn release(project_root: &Path) {
    match stop(project_root, "its supervisor retired").await {
        Stop::Stopped => {}
        Stop::InUse => eprintln!(
            "cowshed: the Nx daemon of {} outlives its retired supervisor: a host client holds \
             one of its task databases open",
            project_root.display()
        ),
        Stop::Failed(why) => eprintln!(
            "cowshed: the Nx daemon of {} did not stop as its supervisor retired: {why}",
            project_root.display()
        ),
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

/// What stopping one ledger-owned landing daemon did. Every failure is a diagnostic here, never an
/// error: a failed cleanup cannot abort a land whose HEAD has already moved.
#[cfg(target_os = "macos")]
#[derive(Debug)]
pub(crate) struct OwnedStopOutcome {
    pub(crate) pid: i32,
    pub(crate) exited: bool,
    pub(crate) signalled: bool,
    pub(crate) stderr: Vec<u8>,
    pub(crate) diagnostics: Vec<String>,
}

/// Stop the job's own daemon process: `SIGTERM` the exact identity the ledger proved, run the
/// caller's socket-cleanup command, then wait on the exit watch registered before any signal for
/// the shared grace. Only a join failure of the wait is an error.
#[cfg(target_os = "macos")]
pub(crate) async fn stop_owned_daemon<F, Fut, E>(
    process: super::job_groups::OwnedGroupProcess,
    exit: super::job_groups::ExitWatch,
    cleanup: F,
) -> Result<OwnedStopOutcome>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<std::process::Output, E>>,
    E: std::fmt::Display,
{
    let pid = process.pid();
    let mut diagnostics = Vec::new();
    let signalled = match process.terminate() {
        Ok(true) => true,
        Ok(false) => {
            diagnostics.push(format!(
                "the proven identity of pid {pid} no longer accepts a signal; no SIGTERM was delivered"
            ));
            false
        }
        Err(error) => {
            diagnostics.push(format!("sending SIGTERM to pid {pid} failed: {error}"));
            false
        }
    };
    let mut stderr = Vec::new();
    match cleanup().await {
        Ok(output) => {
            if !output.status.success() {
                diagnostics.push(format!("nx daemon --stop ended {}", output.status));
            }
            stderr = output.stderr;
        }
        Err(error) => diagnostics.push(format!("nx daemon --stop did not run: {error}")),
    }
    let grace = crate::build_volume::nx::DAEMON_EXIT_GRACE;
    let exited =
        match crate::storage::lifecycle::dispatch_blocking(move || exit.within(grace)).await {
            Ok(Ok(exited)) => exited,
            Ok(Err(error)) => {
                diagnostics.push(format!("waiting for pid {pid} to exit failed: {error}"));
                false
            }
            Err(error) => {
                return Err(CowshedError::internal(format!(
                    "waiting for the Nx daemon failed: {error}"
                )));
            }
        };
    Ok(OwnedStopOutcome {
        pid,
        exited,
        signalled,
        stderr,
        diagnostics,
    })
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

        // `/bin/sh` is the one program path every supported host has: NixOS keeps no `/usr/bin/true`.
        let mut exited = cowshed_os::fork_lock::Spawn::spawn_locked(
            std::process::Command::new("/bin/sh").args(["-c", ":"]),
        )
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

    #[cfg(target_os = "macos")]
    fn sleeper() -> std::process::Child {
        use crate::fork_lock::Spawn as _;
        std::process::Command::new("/bin/sleep")
            .arg("300")
            .spawn_locked()
            .expect("a sleeping process")
    }

    /// A process whose exit a watch reports only once it has ended, and whose pid observes nothing
    /// once it is reaped. Observing a pid below one is refused rather than read as a group.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_observed_process_is_watched_to_its_exit_and_a_reaped_pid_observes_nothing() {
        let mut child = sleeper();
        let pid = i32::try_from(child.id()).unwrap();
        let process = super::super::job_groups::Process::observe(pid)
            .unwrap()
            .expect("a running process is observed");
        assert!(super::super::job_groups::Process::observe(0).is_err());
        let watch = process.watch_exit().unwrap();
        assert!(
            child.try_wait().unwrap().is_none(),
            "still running before the kill"
        );
        child.kill().unwrap();
        watch.wait().unwrap();
        child.wait().unwrap();
        assert!(
            super::super::job_groups::Process::observe(pid)
                .unwrap()
                .is_none()
        );
    }

    /// A sleeping process that leads its own group, as a job's leader does.
    #[cfg(target_os = "macos")]
    fn group_sleeper() -> std::process::Child {
        use crate::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;
        std::process::Command::new("/bin/sleep")
            .arg("300")
            .process_group(0)
            .spawn_locked()
            .expect("a sleeping group leader")
    }

    /// A ledger that names `child`'s group as one admitted job's, the way the supervisor records it.
    #[cfg(target_os = "macos")]
    fn ledger_naming(scratch: &Path, child: &std::process::Child) -> PathBuf {
        let leader = super::super::job_groups::GroupLeader::observe(child.id()).unwrap();
        let ledger = scratch.join("supervisor.groups");
        super::super::job_groups::record(&ledger, &[(1, leader)], &[]).unwrap();
        ledger
    }

    /// A sandboxed group leader that announces READY from its own shell and then blocks reading
    /// its stdin, which stays open with the child. No exec follows READY, so the pid version the
    /// ledger observes is the one the process keeps, and `sandboxed` reads the profile it runs under.
    #[cfg(target_os = "macos")]
    fn sandboxed_group_leader() -> std::process::Child {
        use crate::fork_lock::Spawn as _;
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;
        let mut child = std::process::Command::new("/usr/bin/sandbox-exec")
            .args([
                "-p",
                "(version 1)(allow default)",
                "/bin/sh",
                "-c",
                "echo READY; read _",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("a sandboxed group leader");
        let mut ready = String::new();
        std::io::BufReader::new(child.stdout.take().expect("its stdout"))
            .read_line(&mut ready)
            .expect("the leader announces itself");
        assert_eq!(ready.trim(), "READY");
        child
    }

    /// A daemon the ledger proves the job's is stopped through its exact identity: the plan carries
    /// the exit watch from before the signal, the signal ends the process, and the watch reports it.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_daemon_the_ledger_proves_is_owned_and_stopped_through_its_watch() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-owned-stop");
        let mut child = sandboxed_group_leader();
        let pid = i32::try_from(child.id()).unwrap();
        let ledger = ledger_naming(&scratch, &child);
        let record = scratch.join("server-process.json");
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        let DaemonStopPlan::Owned { process, exit } =
            prepare_daemon_stop(&record, &ledger).unwrap()
        else {
            panic!("a ledger-proven live daemon is owned");
        };
        assert_eq!(process.pid(), pid);
        assert!(
            child.try_wait().unwrap().is_none(),
            "preparing signals nothing"
        );
        assert!(
            process.terminate().unwrap(),
            "the proven process was signalled"
        );
        exit.wait().unwrap();
        child.wait().unwrap();
    }

    /// A live process the ledger does not name is never signalled: a fresh, well-formed record
    /// naming it passes the supplementary checks, and the ledger alone refuses it.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_fresh_forged_record_naming_a_process_the_ledger_does_not_prove_is_unproven() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-forged");
        let mut foreign = sandboxed_group_leader();
        let ledger = scratch.join("supervisor.groups");
        super::super::job_groups::record(&ledger, &[], &[]).unwrap();
        let record = scratch.join("server-process.json");
        write_record(&record, foreign.id(), &scratch.join("refusing.sock"));
        let plan = prepare_daemon_stop(&record, &ledger).unwrap();
        let alive = foreign.try_wait().unwrap().is_none();
        foreign.kill().unwrap();
        foreign.wait().unwrap();
        let DaemonStopPlan::Unproven { pid, reason } = plan else {
            panic!("a sandboxed process the ledger does not name is unproven");
        };
        assert_eq!(pid, i32::try_from(foreign.id()).unwrap());
        assert!(
            reason.contains("ledger"),
            "the refusal comes from the ledger: {reason}"
        );
        assert!(
            !reason.contains("sandboxed"),
            "eligibility passed: {reason}"
        );
        assert!(alive, "the unproven process is untouched");
    }

    /// The race the ledger must win: the plan proves the owned leader, the record is then rewritten
    /// to name a settled foreign sandboxed process the ledger never named, and the real stop runs.
    /// The owned process exits, the foreign process stays alive, and the cleanup touches neither.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_record_rewritten_to_a_foreign_pid_after_the_plan_cannot_redirect_the_stop() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-race");
        let mut owned = sandboxed_group_leader();
        let owned_pid = i32::try_from(owned.id()).unwrap();
        let ledger = ledger_naming(&scratch, &owned);
        let record = scratch.join("server-process.json");
        write_record(&record, owned.id(), &scratch.join("refusing.sock"));
        let DaemonStopPlan::Owned { process, exit } =
            prepare_daemon_stop(&record, &ledger).unwrap()
        else {
            panic!("the ledger proves the owned leader before the race");
        };
        let mut foreign = sandboxed_group_leader();
        let foreign_pid = i32::try_from(foreign.id()).unwrap();
        let forged = scratch.join("forged.json");
        write_record(&forged, foreign.id(), &scratch.join("refusing.sock"));
        std::fs::rename(&forged, &record).unwrap();
        let outcome = stop_owned_daemon(process, exit, || async {
            std::process::Command::new("/bin/sh")
                .args(["-c", "exit 0"])
                .output_locked()
        })
        .await
        .unwrap();
        let owned_exited = owned.try_wait().unwrap().is_some();
        if !owned_exited {
            owned.kill().unwrap();
        }
        owned.wait().unwrap();
        let foreign_alive = foreign.try_wait().unwrap().is_none();
        if foreign_alive {
            foreign.kill().unwrap();
        }
        foreign.wait().unwrap();
        assert_eq!(outcome.pid, owned_pid);
        assert!(
            outcome.exited && owned_exited,
            "the owned process exits: {outcome:?}"
        );
        assert!(
            foreign_alive,
            "the settled foreign process {foreign_pid} stays alive"
        );
    }
    /// A record written before the process it names started is stale: it names another process
    /// than the one it describes, and is refused even when the ledger proves the pid.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_record_older_than_its_process_is_unproven_even_when_the_ledger_proves_it() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-stale");
        let mut child = group_sleeper();
        let ledger = ledger_naming(&scratch, &child);
        let record = scratch.join("server-process.json");
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        std::fs::File::options()
            .write(true)
            .open(&record)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))
            .unwrap();
        assert!(matches!(
            prepare_daemon_stop(&record, &ledger).unwrap(),
            DaemonStopPlan::Unproven { .. }
        ));
        assert!(
            child.try_wait().unwrap().is_none(),
            "a stale record signals nothing"
        );
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// A reaped leader names no process and proves nothing: its group id may belong to a later
    /// process, so the record is nothing to stop and the ledger no longer owns anything.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_reaped_leader_proves_nothing_and_its_record_names_nothing() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-reaped");
        let mut child = group_sleeper();
        let pid = i32::try_from(child.id()).unwrap();
        let ledger = ledger_naming(&scratch, &child);
        let record = scratch.join("server-process.json");
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            super::super::job_groups::owned_process(&ledger, pid)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            prepare_daemon_stop(&record, &ledger).unwrap(),
            DaemonStopPlan::NotRunning
        ));
    }

    /// A leader reaped while a member of its group still runs leaves the group unidentified: the
    /// ledger proves nothing about the survivor, and the record that names the reaped pid is not
    /// renewed as the job's. The survivor is stopped by its own group id, after the assertions.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_reaped_leader_with_a_live_member_leaves_the_group_unproven() {
        use crate::fork_lock::Spawn as _;
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-survivor");
        let mut leader = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 300 & echo READY; exit 0"])
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("a leader that exits and leaves a member");
        let mut ready = String::new();
        std::io::BufReader::new(leader.stdout.take().expect("its stdout"))
            .read_line(&mut ready)
            .expect("the leader announces itself");
        assert_eq!(ready.trim(), "READY");
        let pid = i32::try_from(leader.id()).unwrap();
        let ledger = ledger_naming(&scratch, &leader);
        let record = scratch.join("server-process.json");
        write_record(&record, leader.id(), &scratch.join("refusing.sock"));
        leader.wait().unwrap();
        assert!(
            super::super::job_groups::owned_process(&ledger, pid)
                .unwrap()
                .is_none(),
            "a reaped leader's survivor is not the job's proven process"
        );
        assert!(matches!(
            prepare_daemon_stop(&record, &ledger).unwrap(),
            DaemonStopPlan::NotRunning
        ));
        std::process::Command::new("/bin/kill")
            .args(["-KILL", &format!("-{pid}")])
            .status_locked()
            .expect("kill the survivor's group");
    }

    /// Absent records and ledgers name nothing, a live pid without a ledger is unproven, and a
    /// malformed record or a pid below one is an error, never a guess.
    #[cfg(target_os = "macos")]
    #[test]
    fn absent_and_malformed_records_are_scoped_and_never_guessed() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-scoped");
        let ledger = scratch.join("absent.groups");
        assert!(matches!(
            prepare_daemon_stop(&scratch.join("absent.json"), &ledger).unwrap(),
            DaemonStopPlan::NotRunning
        ));
        let mut child = sandboxed_group_leader();
        let record = scratch.join("server-process.json");
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        let plan = prepare_daemon_stop(&record, &ledger).unwrap();
        std::fs::write(&record, b"{ not json").unwrap();
        let malformed = prepare_daemon_stop(&record, &ledger);
        write_record(&record, 0, &scratch.join("refusing.sock"));
        let invalid_pid = prepare_daemon_stop(&record, &ledger);
        child.kill().unwrap();
        child.wait().unwrap();
        let DaemonStopPlan::Unproven { reason, .. } = plan else {
            panic!("a settled nominee without a ledger is unproven");
        };
        assert!(reason.contains("ledger"), "{reason}");
        assert!(malformed.is_err());
        assert!(invalid_pid.is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn unreadable_and_malformed_records_and_ledgers_are_typed_adoption_skips() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-unverifiable");
        let record = scratch.join("server-process.json");
        let ledger = scratch.join("supervisor.groups");
        let mut child = sandboxed_group_leader();
        let assert_skip = |result, operation: &str, source: &Path| match result {
            Err(crate::api::dto::AdoptionSkip::LandingDaemonUnverifiable {
                record: refused,
                reason,
            }) => {
                assert_eq!(refused, record);
                let expected = format!("{operation} {}: ", source.display());
                assert!(
                    reason.starts_with(&expected),
                    "{reason}; expected {expected}"
                );
                let skip = crate::api::dto::AdoptionSkip::LandingDaemonUnverifiable {
                    record: refused,
                    reason,
                };
                assert_eq!(skip.name(), "landingDaemonUnverifiable");
                assert!(skip.to_string().contains("no process was signalled"));
                let wire = serde_json::to_value(&skip).unwrap();
                assert_eq!(wire["kind"], "landingDaemonUnverifiable");
                assert_eq!(
                    serde_json::from_value::<crate::api::dto::AdoptionSkip>(wire).unwrap(),
                    skip
                );
            }
            _ => panic!("unreadable authority must skip, never guess"),
        };
        std::fs::write(&record, b"{not json").unwrap();
        let malformed_record = prepare_daemon_stop(&record, &ledger);
        std::fs::remove_file(&record).unwrap();
        std::fs::create_dir(&record).unwrap();
        let unreadable_record = prepare_daemon_stop(&record, &ledger);
        std::fs::remove_dir(&record).unwrap();
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        std::fs::write(&ledger, b"{not a ledger").unwrap();
        let malformed_ledger = prepare_daemon_stop(&record, &ledger);
        std::fs::remove_file(&ledger).unwrap();
        std::fs::create_dir(&ledger).unwrap();
        let result = prepare_daemon_stop(&record, &ledger);
        let alive = child.try_wait().unwrap().is_none();
        child.kill().unwrap();
        child.wait().unwrap();
        assert_skip(malformed_record, "cannot parse daemon record", &record);
        assert_skip(unreadable_record, "cannot read daemon record", &record);
        assert_skip(
            malformed_ledger,
            "cannot verify against job ledger",
            &ledger,
        );
        assert_skip(result, "cannot verify against job ledger", &ledger);
        assert!(alive, "no error path signals");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn an_exec_after_the_plan_refuses_the_stale_identity_and_reports_no_delivered_signal() {
        use crate::fork_lock::Spawn as _;
        use std::io::{BufRead as _, Write as _};
        use std::os::unix::process::CommandExt as _;
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-exec-identity");
        let mut child = std::process::Command::new("/usr/bin/sandbox-exec")
            .args([
                "-p",
                "(version 1)(allow default)",
                "/bin/sh",
                "-c",
                "echo READY; read _; exec /bin/sh -c 'echo EXECUTED; read _'",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "READY");
        let ledger = ledger_naming(&scratch, &child);
        let record = scratch.join("server-process.json");
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        let DaemonStopPlan::Owned { process, exit } =
            prepare_daemon_stop(&record, &ledger).unwrap()
        else {
            panic!("the settled pre-exec leader is ledger-owned");
        };
        child.stdin.as_mut().unwrap().write_all(b"exec\n").unwrap();
        line.clear();
        stdout.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "EXECUTED");
        let outcome = stop_owned_daemon(process, exit, || async {
            std::process::Command::new("/bin/true").output_locked()
        })
        .await
        .unwrap();
        let alive = child.try_wait().unwrap().is_none();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(!outcome.signalled, "a stale identity signals nothing");
        assert!(!outcome.exited, "the exec'd process is still alive");
        assert!(
            outcome
                .diagnostics
                .iter()
                .any(|line| line.contains("no SIGTERM was delivered"))
        );
        assert!(alive);
    }
}

/// Lifecycle authority over a landing daemon: a process the job ledger proves is the job's own is
/// stopped through the controller's exact identity, and a record naming any other process signals
/// nothing. Every process here runs under `sandbox-exec` with the profile denying signals to other
/// pids, so the kernel's own refusal is part of each proof.
#[cfg(all(test, target_os = "macos"))]
mod lifecycle_authority_tests {
    use std::os::unix::process::CommandExt as _;

    use super::*;
    use crate::fork_lock::{Run as _, RunAsync, Spawn as _};
    use crate::runtime::job_groups::{GroupLeader, record};

    const SELF_ONLY: &str = "(version 1)(allow default)(deny signal (target others))";

    fn sandboxed_sleeper() -> std::process::Child {
        use std::io::BufRead as _;
        let mut child = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", SELF_ONLY, "/bin/sh", "-c", "echo READY; read _"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("the sandboxed sleeper starts");
        let mut ready = String::new();
        std::io::BufReader::new(child.stdout.take().expect("its stdout"))
            .read_line(&mut ready)
            .expect("the final sandboxed shell announces itself");
        assert_eq!(ready.trim(), "READY");
        child
    }

    fn ledger_for(scratch: &Path, child: &std::process::Child) -> PathBuf {
        let leader = GroupLeader::observe(child.id()).expect("observe the group leader");
        let ledger = scratch.join("supervisor.groups");
        record(&ledger, &[(1, leader)], &[]).expect("record the job's group");
        ledger
    }

    fn write_daemon_record(path: &Path, pid: u32) {
        std::fs::write(
            path,
            serde_json::json!({ "processId": pid, "socketPath": "/nonexistent.sock" }).to_string(),
        )
        .expect("the daemon record");
    }

    fn sandboxed_kill(pid: u32) -> std::process::Output {
        std::process::Command::new("/usr/bin/sandbox-exec")
            .args([
                "-p",
                SELF_ONLY,
                "/bin/sh",
                "-c",
                &format!("kill -TERM {pid}"),
            ])
            .output_locked()
            .expect("the sandboxed kill runs")
    }

    #[tokio::test]
    async fn an_owned_daemon_refuses_a_sandboxed_signal_and_a_failed_cleanup_never_aborts() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-owned-stop");
        let mut child = sandboxed_sleeper();
        let pid = child.id();
        let ledger = ledger_for(&scratch, &child);
        let record_path = scratch.join("server-process.json");
        write_daemon_record(&record_path, pid);

        let refused = sandboxed_kill(pid);
        assert!(
            !refused.status.success(),
            "a sandboxed signal to another process fails"
        );
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("Operation not permitted"),
            "the kernel refuses it: {}",
            String::from_utf8_lossy(&refused.stderr)
        );

        let DaemonStopPlan::Owned { process, exit } =
            prepare_daemon_stop(&record_path, &ledger).expect("the plan reads")
        else {
            panic!("a ledger-proven live process is owned");
        };
        assert!(
            !exit.within(Duration::ZERO).expect("the watch answers"),
            "still running"
        );

        let outcome = stop_owned_daemon(process, exit, || async {
            let mut command = tokio::process::Command::new("/bin/sh");
            command.args(["-c", "exit 17"]);
            RunAsync::output_locked(&mut command).await
        })
        .await
        .expect("a failed cleanup is a diagnostic, never an error");
        assert!(
            outcome.exited,
            "the owned process exited on the controller's SIGTERM: {outcome:?}"
        );
        assert!(
            outcome.diagnostics.iter().any(|line| line.contains("17")),
            "the failed cleanup is reported: {outcome:?}"
        );
        child.wait().expect("reap the stopped process");
    }

    #[test]
    fn a_record_naming_a_process_the_ledger_does_not_own_signals_nothing() {
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-forged-nominee");
        let mut foreign = sandboxed_sleeper();
        let ledger = scratch.join("supervisor.groups");
        record(&ledger, &[], &[]).expect("an empty ledger");
        let record_path = scratch.join("server-process.json");
        write_daemon_record(&record_path, foreign.id());

        let plan = prepare_daemon_stop(&record_path, &ledger).expect("the plan reads");
        let pid_before_cleanup = foreign.id();
        let alive = foreign.try_wait().expect("poll").is_none();
        foreign.kill().expect("the test stops its own sleeper");
        foreign.wait().expect("reap");
        match plan {
            DaemonStopPlan::Unproven { pid, reason } => {
                assert_eq!(
                    pid,
                    i32::try_from(pid_before_cleanup).expect("a pid fits i32"),
                    "the nominee is named"
                );
                assert!(reason.contains("ledger"), "the ledger refuses it: {reason}");
            }
            _ => panic!("a nominee the ledger does not name is unproven"),
        }
        assert!(alive, "the forged nominee survives");
    }

    /// The Bun-installed Nx product this checkout resolves, and the node on the test's PATH.
    fn declared_nx() -> (PathBuf, PathBuf) {
        let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
            .expect("cargo exports CARGO_MANIFEST_DIR to the test process");
        let nx = PathBuf::from(manifest)
            .join("../../../../node_modules/nx")
            .canonicalize()
            .expect("the Bun-installed Nx product");
        let node = std::env::var_os("PATH")
            .and_then(|path| {
                std::env::split_paths(&path)
                    .map(|directory| directory.join("node"))
                    .find(|candidate| candidate.is_file())
            })
            .expect("node on the test's PATH")
            .canonicalize()
            .expect("node resolves");
        (nx, node)
    }

    /// Removes every `NX_*` variable and `CI` the test process inherited, as stock Nx's own run does,
    /// so a daemon never adopts the host's socket directory or cache roots.
    trait ClearNxEnvironment {
        fn clear_nx_environment(&mut self) -> &mut Self;
    }

    impl ClearNxEnvironment for std::process::Command {
        fn clear_nx_environment(&mut self) -> &mut Self {
            for (key, _) in std::env::vars_os() {
                if key.to_string_lossy().starts_with("NX_") || key == "CI" {
                    self.env_remove(key);
                }
            }
            self
        }
    }

    fn nx_environment(root: &Path) -> [(&'static str, PathBuf); 3] {
        [
            ("NX_WORKSPACE_ROOT_PATH", root.to_owned()),
            (
                "NX_WORKSPACE_DATA_DIRECTORY",
                root.join(".nx/workspace-data"),
            ),
            ("NX_CACHE_DIRECTORY", root.join(".nx/cache")),
        ]
    }

    /// The real Nx daemon server, started by its own entry point under the same-pid signal profile
    /// in a group of its own, so the ledger can prove it.
    fn real_daemon(root: &Path, node: &Path, nx: &Path) -> std::process::Child {
        std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", SELF_ONLY])
            .arg(node)
            .arg(nx.join("dist/src/daemon/server/start.js"))
            .clear_nx_environment()
            .envs(nx_environment(root))
            .current_dir(root)
            .process_group(0)
            .spawn_locked()
            .expect("the real Nx daemon starts")
    }

    /// The stock `nx daemon --stop`, run inside the same sandbox profile.
    fn sandboxed_nx_stop(root: &Path, node: &Path, nx: &Path) -> std::process::Command {
        let mut command = std::process::Command::new("/usr/bin/sandbox-exec");
        command
            .args(["-p", SELF_ONLY])
            .arg(node)
            .arg(nx.join("dist/bin/nx.js"))
            .args(["daemon", "--stop"])
            .clear_nx_environment()
            .envs(nx_environment(root))
            .current_dir(root);
        command
    }

    #[tokio::test]
    async fn the_real_nx_daemon_is_stopped_only_through_the_ledger_proven_identity() {
        let (nx, node) = declared_nx();
        let scratch = crate::temp_root::TempRoot::new("nx-daemon-real-authority");
        let root = scratch.join("workspace");
        std::fs::create_dir_all(&root).expect("workspace");
        std::fs::write(
            root.join("package.json"),
            r#"{"name":"fixture","private":true}"#,
        )
        .expect("package.json");
        std::fs::write(root.join("nx.json"), "{}").expect("nx.json");
        // The daemon resolves its own version through the workspace's node_modules/nx, so the fixture
        // links the owned declared product there, as Nx's own fixtures do.
        std::fs::create_dir_all(root.join("node_modules")).expect("node_modules");
        std::os::unix::fs::symlink(&nx, root.join("node_modules/nx"))
            .expect("the workspace's own nx");
        let mut daemon = real_daemon(&root, &node, &nx);
        let record_path = root.join(".nx/workspace-data/d/server-process.json");
        let started = std::time::Instant::now();
        while !record_path.is_file() {
            if let Some(status) = daemon.try_wait().expect("poll the daemon") {
                let mut stderr = String::new();
                if let Some(mut pipe) = daemon.stderr.take() {
                    std::io::Read::read_to_string(&mut pipe, &mut stderr).ok();
                }
                panic!("the real daemon exited before publishing its record ({status}): {stderr}");
            }
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "the real daemon did not publish its record within 120s"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        let leader = GroupLeader::observe(daemon.id()).expect("observe the daemon's group leader");
        let ledger = scratch.join("supervisor.groups");
        record(&ledger, &[(1, leader)], &[]).expect("record the job's group");
        let DaemonStopPlan::Owned { process, exit } =
            prepare_daemon_stop(&record_path, &ledger).expect("the plan reads")
        else {
            panic!("the ledger proves the real daemon is the job's own");
        };

        // RED: the sandboxed stock stop is refused by the kernel, and the exit watch stays silent.
        let refused = sandboxed_nx_stop(&root, &node, &nx)
            .output_locked()
            .expect("the sandboxed stop runs");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&refused.stdout),
            String::from_utf8_lossy(&refused.stderr)
        );
        assert!(
            text.contains("Operation not permitted") || text.contains("EPERM"),
            "the kernel refused the sandboxed signal: {text}"
        );
        assert!(
            !exit.within(Duration::ZERO).expect("the watch answers"),
            "the sandboxed stop signalled nothing"
        );

        let outcome = stop_owned_daemon(process, exit, || async {
            let mut command = tokio::process::Command::from(sandboxed_nx_stop(&root, &node, &nx));
            RunAsync::output_locked(&mut command).await
        })
        .await
        .expect("the controller stops its owned daemon");
        assert!(
            outcome.exited,
            "the exact-identity SIGTERM ended the real daemon: {outcome:?}"
        );
        daemon.wait().expect("reap the real daemon");
    }
}
