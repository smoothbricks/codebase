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
//! watcher that recomputes the project graph on every change in the tree. Nx stops a daemon only
//! after its own inactivity interval without a request or a file event, which a tree that keeps
//! changing never reaches, so the supervisor ends the daemon itself. A shed is used while its
//! supervisor's ledger holds a job or a named session, when a job is admitted or ends, when a
//! session opens, and when a daemon comes up; nothing else is asked. Once none of that has happened
//! for the shed's [`NxDaemonIdle`] the keeper stops the daemon, as `nx daemon --stop` does, and
//! starts none until the shed is used again. Its foreground daemon is one admitted maintenance
//! job, excluded from developer activity. Quiescence and retirement stop and wait for that job;
//! the supervisor also releases any daemon its keeper watched.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
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

/// How many start jobs in a row may leave no daemon live before the keeper pauses its starts
/// ([`FIRST_PAUSE`]). A start that can never run -- no executable `node` on the job's PATH -- is
/// refused before any job ([`daemon_node`]). This bounds what no exit status classifies:
/// `sandbox-exec` exits 71 both for an `execvp` it could not make and for a child that exited 71,
/// a shell host's 127 is also a program's own, and Node's failures are the program's. A slow start
/// is never counted (no probe starts another while one runs), and neither is a daemon that served
/// and then stopped (a kept daemon whose installation changed stops so its keeper starts the
/// successor): that start succeeded.
pub(crate) const START_ATTEMPTS: u32 = 3;

/// How long the keeper starts nothing once [`START_ATTEMPTS`] starts in a row failed; after it, one
/// start is tried, and each further failure doubles the pause up to [`PAUSE_CAP`]. A keeper that
/// halted instead would leave every host client to start its own daemon outside the sandbox, the
/// very thing it exists to prevent; one that tried at every probe would turn a failure that does
/// not clear into a start every 5 s for the life of the shed. A live daemon, whoever started it,
/// closes the pause and resets it to this.
pub(crate) const FIRST_PAUSE: Duration = Duration::from_secs(60);

/// The longest pause between the keeper's starts after failures: at most one start per this long.
pub(crate) const PAUSE_CAP: Duration = Duration::from_secs(15 * 60);

/// How many stops of an idle shed's live daemon may fail in a row before no probe tries again
/// while that daemon stays live. A failed stop is a daemon that outlived its `SIGTERM` hang guard
/// or a task database that could not be read; a fourth try of either meets the same daemon. It
/// costs a resident daemon, not a stalled job: the supervisor's retirement stops it once more,
/// and a probe that finds it gone counts afresh.
pub(crate) const STOP_ATTEMPTS: u32 = 3;

/// How long a shed must show no use of Nx before its supervisor stops the daemon it keeps: main's
/// `.cowshed.toml` `[nx] daemon_idle`, [`NxDaemonIdle::DEFAULT`] without it (04_sandbox.md, "The
/// Nx daemon stays on"). Never zero: a zero bound would stop a daemon at the probe after the one
/// that found it come up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NxDaemonIdle(Duration);

impl NxDaemonIdle {
    /// Long enough that a gate's runs, an edit and the next run share one warm daemon (a cold one
    /// recomputes the project graph and every plugin worker's state), short enough that a shed
    /// whose agent has finished costs its host five minutes of a daemon, not the rest of the day.
    pub const DEFAULT: Self = Self(Duration::from_secs(5 * 60));

    /// The bound `idle`; `None` for zero.
    pub fn new(idle: Duration) -> Option<Self> {
        (!idle.is_zero()).then_some(Self(idle))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// The daemon of an Nx project, as its rendezvous record and the world agree on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Probe {
    /// The record names a running process whose socket file still has a socket attached: it
    /// accepts, or is busy with a full queue (`deadline::occupant`).
    Live,
    /// No record: no daemon was started, or the one that was removed its record on exit.
    Unrecorded,
    /// A record that cannot be read as Nx writes it.
    Unreadable,
    /// The recorded process is gone.
    Dead,
    /// The recorded socket file has no socket attached, or cannot be read.
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
    /// The job's own process, proven by the ledger, with its exit and the children it forks watched
    /// before any signal is sent ([`super::daemon_watch::DaemonWatch`]), so its plugin workers are
    /// held while they are still its children. Signalling it reaches that process or nothing.
    Owned {
        process: super::job_groups::OwnedGroupProcess,
        watch: super::daemon_watch::DaemonWatch,
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
    if !cowshed_os::process::sandboxed(pid).map_err(|error| {
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
    let watched = process
        .life()
        .and_then(|life| life.map_or(Ok(None), super::daemon_watch::DaemonWatch::of));
    match watched {
        Ok(Some(watch)) => Ok(DaemonStopPlan::Owned { process, watch }),
        // It exited between the proof and its watch: nothing runs to stop.
        Ok(None) => Ok(DaemonStopPlan::NotRunning),
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
    match cowshed_os::deadline::occupant(&recorded.socket_path) {
        Ok(cowshed_os::deadline::Occupant::Live) => (Probe::Live, Some((pid, bytes))),
        Ok(cowshed_os::deadline::Occupant::Absent | cowshed_os::deadline::Occupant::Stopped)
        | Err(_) => (Probe::Refused, None),
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

/// The read-write resident job that starts the daemon of the Nx project at `project_root`: the
/// project's own `nx`, run by [`daemon_node`] from the project root inside the workspace at
/// `workspace_root`. The daemon stays up to answer clients and is idle between them, and an idle
/// process costs the host nothing (05_gateway.md "Classes"): it runs at the resident class.
///
/// Every error is permanent for the supervisor's sandbox: a start that can never run is refused
/// here, named, before any job exists.
fn start_request(
    workspace_root: &Path,
    project_root: &Path,
    node: NodeSource<'_>,
) -> Result<ExecRequest> {
    let node = daemon_node(node).map_err(|why| {
        CowshedError::environment_missing(
            format!(
                "the Nx daemon of {} cannot start: {why}",
                project_root.display()
            ),
            "give the shed a `node`: an `.envrc` whose shell puts one on PATH, or one installed \
             where the javascript capability finds it, then restart the shed's supervisor",
        )
    })?;
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
            node,
            CommandArg::new(project_root.join("node_modules/nx/dist/src/daemon/server/start.js")),
            // The keeper owns this daemon's restart: the patched daemon stops, rather than spawning a successor
            // outside this job, when its installation changes, and the next probe starts the owned one
            // (04_sandbox.md, "The process that starts a daemon owns its restart").
            CommandArg::from("--kept"),
        ]),
        cwd,
        mode: RunSandboxMode::ReadWrite,
        env: HashMap::new(),
        trace: None,
        stdin: StdinSource::Empty,
        stdout_copy: None,
        stderr_copy: None,
        admission_key: None,
        class: crate::api::dto::ExecClass::Resident,
        want: None,
        scope: crate::api::dto::ExecScope::Workspace,
    })
}

/// What a shed's Nx daemon keeper has decided, as its supervisor answers it after a probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NxKeeperState {
    /// The start job the keeper waits on.
    pub starting: Option<JobId>,
    /// Why the keeper starts no daemon now, however much the shed wants one.
    pub held: Option<NxKeeperHold>,
}

/// Why a shed's Nx daemon keeper starts no daemon now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NxKeeperHold {
    /// No start can run as the sandbox stands -- no executable `node` on the job's PATH, named
    /// with its `ENOENT` or `EACCES`. No job is admitted; each probe looks again, and the first
    /// that finds one starts it.
    Refused { why: String },
    /// `failures` starts in a row left no daemon live, the last as `why`: none starts for
    /// `resumes_in`, then one does. `pause` is how long this pause is, `cap` the longest any is.
    Paused {
        why: String,
        failures: u32,
        resumes_in: Duration,
        pause: Duration,
        cap: Duration,
    },
    /// A lifecycle verb about to take its census of the mount stopped the keeper's starts and
    /// ended the start it had in flight: none starts until the shed is used again -- a job
    /// admitted or a named session opened.
    Stopped,
}

/// The keeper's own record of why it starts nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Hold {
    Refused(String),
    Paused {
        why: String,
        until: Instant,
        pause: Duration,
    },
}

/// Where a start job's `node` comes from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum NodeSource<'a> {
    /// The shed activates a shell, which declares its own toolchain (15_capabilities.md,
    /// "Bootstrap executables"): `node` is whatever the activated PATH names, which only the job
    /// can tell.
    Shell,
    /// The shed activates none: the job's PATH is this bootstrap PATH and nothing else.
    Path(&'a OsStr),
}

/// The `node` a start job runs, or why none can run. In a shed that activates a shell, `node`.
/// In one that activates none, `node` as `execvp` would find it on the job's own bootstrap
/// `path`, by its absolute path: the first executable `node` along it, past ones that cannot
/// execute. When the lookup cannot tell -- a directory it may not read -- the job runs `node`
/// and its start is a failure bounded by [`START_ATTEMPTS`]. When the lookup proves no `node`
/// along the PATH can run, its `ENOENT`, or `EACCES` when one exists but cannot execute, is named
/// here rather than met as `sandbox-exec`'s exit 71 at every probe.
pub(crate) fn daemon_node(node: NodeSource<'_>) -> std::result::Result<CommandArg, String> {
    let path = match node {
        NodeSource::Shell => return Ok(CommandArg::from("node")),
        NodeSource::Path(path) => path,
    };
    let mut unexecutable = false;
    let mut undecided = false;
    for candidate in std::env::split_paths(path).map(|directory| directory.join("node")) {
        match std::fs::metadata(&candidate) {
            Ok(metadata) if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 => {
                return Ok(CommandArg::new(candidate));
            }
            Ok(_) => unexecutable = true,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => unexecutable = true,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) => {}
            Err(_) => undecided = true,
        }
    }
    if undecided {
        return Ok(CommandArg::from("node"));
    }
    let errno = if unexecutable {
        libc::EACCES
    } else {
        libc::ENOENT
    };
    Err(format!(
        "no executable `node` on its PATH {}: {}",
        path.display(),
        io::Error::from_raw_os_error(errno)
    ))
}

/// How the start job `job` ended, as the keeper says it; `None` for an exit 0.
pub(crate) fn start_ending(job: Option<&JobInfo>) -> Option<String> {
    Some(match job.and_then(|info| info.exit.as_ref()) {
        Some(ExitStatus::Exited { code: 0 }) => return None,
        Some(ExitStatus::Exited { code }) => format!("exited {code}"),
        Some(ExitStatus::Signaled { signal, .. }) => format!("ended on signal {signal}"),
        None => match job {
            Some(info) => format!("ended {:?} with no exit status", info.state),
            None => "has no record in its supervisor".to_owned(),
        },
    })
}

/// A shed supervisor's keeper of its Nx daemon: when to probe next, the start job it waits on, and
/// how long the shed has gone without using Nx.
#[derive(Debug)]
pub(crate) struct NxDaemonKeeper {
    probes: Interval,
    /// One owned probe survives cancellation of the actor's select branch.
    probing: Option<tokio::task::JoinHandle<(PathBuf, Probe)>>,
    /// The foreground daemon job admitted last, retained until a probe sees its terminal result.
    pub(super) starting: Option<JobId>,
    /// How long the shed may go unused before its daemon is stopped.
    idle: NxDaemonIdle,
    /// When the shed was last used, and whether the last probe found its daemon live.
    clock: IdleClock,
    /// What in the ledger kept the live daemon at the last probe, said each time it changes.
    kept_by: Option<Use>,
    /// Whether the last probe found the shed unused for its idle bound and no daemon live:
    /// nothing is left for the supervisor to wait for.
    pub(super) settled: bool,
    /// Starts in a row that left no daemon live, since a probe last found one live.
    failed_starts: u32,
    /// Whether a probe found a daemon live while the current start job ran: its ending is then a
    /// daemon that served and stopped, never a failed start.
    starting_served: bool,
    /// Why the keeper starts nothing now. Said each time it changes.
    hold: Option<Hold>,
    /// Whether a lifecycle verb's census of the mount stopped the keeper's starts
    /// ([`Self::stop_starts`]): none starts until the shed's next use ([`Self::used`]).
    stopped: bool,
    /// The length of the next pause ([`FIRST_PAUSE`], doubling to [`PAUSE_CAP`]).
    next_pause: Duration,
    /// Stops of the idle shed's live daemon in a row that failed ([`STOP_ATTEMPTS`]); a probe
    /// that finds no daemon live clears it.
    failed_stops: u32,
    // Process ownership belongs only to the admitted job's birth and ledger, never an Nx record.
}

/// What a probe decides about the daemon of a shed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Verdict {
    /// The shed is in use, or its daemon is young: one is to run.
    Keep,
    /// The shed has gone unused for its idle bound: none is to run, and the one that did is stopped.
    Release,
    /// A mount census leaves a live daemon alone and starts none until the next use.
    Hold,
}

impl NxDaemonKeeper {
    /// The keeper of a supervisor of a workspace in `role`: a shed's, never main's. Its first
    /// probe is due at once, but the supervisor's start is no use: a daemon is started only once
    /// the ledger holds a job or a session, and one found live gets its interval. A supervisor
    /// started for anything else (a failed `rm` restoring the workspace, an ensure that runs
    /// nothing) would otherwise launch a daemon whose start processes then hold the mount.
    pub(crate) fn for_role(role: WorkspaceRole, idle: NxDaemonIdle) -> Option<Self> {
        match role {
            WorkspaceRole::Main => None,
            WorkspaceRole::Workspace => {
                let mut probes = tokio::time::interval(PROBE_INTERVAL);
                // A probe that waited behind a slow admission is not made up in a burst.
                probes.set_missed_tick_behavior(MissedTickBehavior::Delay);
                Some(Self {
                    probes,
                    probing: None,
                    starting: None,
                    idle,
                    clock: IdleClock::default(),
                    kept_by: None,
                    settled: false,
                    failed_starts: 0,
                    starting_served: false,
                    hold: None,
                    stopped: false,
                    next_pause: FIRST_PAUSE,
                    failed_stops: 0,
                })
            }
        }
    }

    /// The start job to admit for the Nx project at `project_root` in the workspace at
    /// `workspace_root`, running the `node` from `node`; none while a pause runs, or while no
    /// start can run. That is looked up afresh at each call -- a few `stat`s, no job -- so a
    /// `node` that appears is started at the next probe; the refusal is said when it changes.
    pub(crate) fn start(
        &mut self,
        workspace_root: &Path,
        project_root: &Path,
        node: NodeSource<'_>,
    ) -> Option<ExecRequest> {
        if let Some(Hold::Paused { until, .. }) = &self.hold {
            if Instant::now() < *until {
                return None;
            }
            cowshed_os::eprintln!(
                "cowshed: the Nx daemon keeper of {}'s pause is over; trying one start",
                project_root.display()
            );
            self.hold = None;
        }
        match start_request(workspace_root, project_root, node) {
            Ok(request) => {
                if let Some(Hold::Refused(_)) = self.hold.take() {
                    cowshed_os::eprintln!(
                        "cowshed: the Nx daemon of {} can start now; starting it",
                        project_root.display()
                    );
                }
                Some(request)
            }
            Err(error) => {
                self.refuse(format!("{} (next: {})", error.message, error.hint));
                None
            }
        }
    }

    /// Start nothing while no start can run, for `why`; said once for each new reason.
    pub(crate) fn refuse(&mut self, why: String) {
        if self.hold != Some(Hold::Refused(why.clone())) {
            cowshed_os::eprintln!(
                "cowshed: {why}; the shed's supervisor starts no Nx daemon until one can run, \
             looking again at every probe"
            );
        }
        self.hold = Some(Hold::Refused(why));
    }

    /// What the keeper has decided: the start job it waits on, and why it starts none.
    pub(crate) fn state(&self) -> NxKeeperState {
        NxKeeperState {
            starting: self.starting,
            held: if self.stopped {
                Some(NxKeeperHold::Stopped)
            } else {
                self.hold.as_ref().map(|hold| match hold {
                    Hold::Refused(why) => NxKeeperHold::Refused { why: why.clone() },
                    Hold::Paused { why, until, pause } => NxKeeperHold::Paused {
                        why: why.clone(),
                        failures: self.failed_starts,
                        resumes_in: until.saturating_duration_since(Instant::now()),
                        pause: *pause,
                        cap: PAUSE_CAP,
                    },
                })
            },
        }
    }

    /// A probe found the daemon `daemon`. A live one, whoever started it, proves a start can
    /// succeed: the failures before it are forgiven, a pause ends and the next is
    /// [`FIRST_PAUSE`] again.
    pub(crate) fn observe(&mut self, daemon: Probe, project_root: &Path) {
        if daemon != Probe::Live {
            return;
        }
        self.starting_served |= self.starting.is_some();
        self.failed_starts = 0;
        self.next_pause = FIRST_PAUSE;
        if let Some(Hold::Paused { .. }) = self.hold.take() {
            cowshed_os::eprintln!(
                "cowshed: the Nx daemon of {} is live; its keeper's pause is closed",
                project_root.display()
            );
        }
    }

    /// The start job `job_id` ended as `ending` (`None`: it exited 0), and the probe after it
    /// found the daemon `daemon`. A daemon that served and then stopped -- idle, or kept and its
    /// installation changed -- is followed by a successor at once, and is no failure. One that
    /// never served and left no daemon live is a failed start.
    pub(crate) fn start_ended(
        &mut self,
        job_id: JobId,
        ending: Option<String>,
        project_root: &Path,
        daemon: Probe,
    ) {
        let served = std::mem::take(&mut self.starting_served);
        // The census's stop ended it ([`Self::stop_starts`]): no failed start, and no successor.
        if self.stopped {
            return;
        }
        let what = |ending: &str| {
            format!(
                "Nx daemon start job {} for {} {ending}, and the daemon is {daemon}",
                job_id.get(),
                project_root.display()
            )
        };
        match (ending, daemon) {
            (None, Probe::Live) => {}
            (Some(ending), Probe::Live) => cowshed_os::eprintln!("cowshed: {}", what(&ending)),
            (ending, Probe::Unrecorded | Probe::Unreadable | Probe::Dead | Probe::Refused)
                if served =>
            {
                cowshed_os::eprintln!(
                    "cowshed: {} after it served; starting its successor",
                    what(ending.as_deref().unwrap_or("exited 0"))
                );
            }
            (ending, Probe::Unrecorded | Probe::Unreadable | Probe::Dead | Probe::Refused) => {
                self.start_failed(what(ending.as_deref().unwrap_or("exited 0")));
            }
        }
    }

    /// The supervisor admitted a job or opened a named session: the shed is in use from now, and
    /// the next probe is due at once. A job that ends before the next tick counts all the same;
    /// otherwise a short command would never be seen as use.
    pub(crate) fn used(&mut self) {
        if std::mem::take(&mut self.stopped) {
            cowshed_os::eprintln!(
                "cowshed: the shed is in use again, so its Nx daemon keeper starts daemons again"
            );
        }
        self.clock.use_at(Instant::now());
        self.probes.reset_immediately();
    }

    /// A lifecycle verb is about to take its census of the mount before it stops the supervisor
    /// (02_workspaces.md, "Holders cowshed does not run"): the keeper starts no daemon until the
    /// shed is used again ([`Self::used`]). A start that exits while the census reads the process
    /// table is a holder the census cannot prove cowshed's, and one admitted after the census is a
    /// process no census looked at. Said once.
    pub(crate) fn stop_starts(&mut self, project_root: &Path) {
        if !self.stopped {
            cowshed_os::eprintln!(
                "cowshed: the Nx daemon keeper of {} starts no daemon while a census of its mount \
                 decides a retirement; the shed's next job or session resumes it",
                project_root.display()
            );
        }
        self.stopped = true;
        self.probes.reset_immediately();
    }

    /// A job of the ledger other than the keeper's own start ended: the shed's idle interval
    /// starts now, not at the last probe that saw the job run, up to [`PROBE_INTERVAL`] earlier.
    pub(crate) fn job_ended(&mut self) {
        self.clock.use_at(Instant::now());
    }

    /// A start that left no daemon live, `what` saying how: another follows at the next probe
    /// until [`START_ATTEMPTS`] have failed in a row; then starts pause, said with the failures,
    /// the pause and its cap, and one start follows each pause.
    pub(crate) fn start_failed(&mut self, what: String) {
        self.failed_starts += 1;
        if self.failed_starts < START_ATTEMPTS {
            cowshed_os::eprintln!(
                "cowshed: {what}; starting another, {} of {START_ATTEMPTS}, in {}s",
                self.failed_starts + 1,
                PROBE_INTERVAL.as_secs()
            );
            return;
        }
        let pause = self.next_pause;
        self.next_pause = (pause * 2).min(PAUSE_CAP);
        cowshed_os::eprintln!(
            "cowshed: {what}; {} starts in a row left no daemon live, so the keeper starts none \
         for {}s, then one (pauses double to {}s); a live daemon ends the pause",
            self.failed_starts,
            pause.as_secs(),
            PAUSE_CAP.as_secs()
        );
        self.hold = Some(Hold::Paused {
            why: what,
            until: Instant::now() + pause,
            pause,
        });
    }

    /// Judge one probe of the Nx project at `project_root`, whose daemon the probe found
    /// `daemon`, `working` being what the supervisor's ledger holds that uses the shed: a job it
    /// runs or a named session it holds. Stops the daemon once the shed has gone unused for its
    /// idle bound ([`IdleClock::probe`]).
    ///
    /// The ledger is the only source of use: no process-table walk looks for work outside it.
    /// Record and liveness observations arrive from the owned off-actor probe. A kept daemon
    /// is said on stderr with the job or session that keeps it, each time that changes.
    pub(crate) async fn judge(
        &mut self,
        project_root: &Path,
        working: Option<Use>,
        daemon: Probe,
    ) -> Verdict {
        let live = daemon == Probe::Live;
        let idleness = self.clock.probe(
            Instant::now(),
            self.idle,
            working.is_some(),
            live,
            self.stopped,
        );
        let kept_by = working.filter(|_| live);
        if let Some(using) = kept_by
            .as_ref()
            .filter(|using| self.kept_by.as_ref() != Some(using))
        {
            cowshed_os::eprintln!(
                "cowshed: keeps the Nx daemon of {}: {using}",
                project_root.display()
            );
        }
        self.kept_by = kept_by;
        match idleness {
            Idleness::Suspended => {
                self.settled = false;
                Verdict::Hold
            }
            Idleness::InUse => {
                self.settled = false;
                Verdict::Keep
            }
            Idleness::Settled => {
                self.failed_stops = 0;
                self.settled = true;
                Verdict::Release
            }
            Idleness::Stop => {
                self.settled = false;
                if self.may_stop() {
                    let why = format!(
                        "its supervisor ran no job and held no named session for {}s",
                        self.idle.get().as_secs()
                    );
                    if let Err(why) = stop(project_root, &why).await {
                        self.stop_failed(project_root, &why);
                    }
                }
                Verdict::Release
            }
        }
    }

    /// Whether a probe may try to stop the idle shed's live daemon: not once [`STOP_ATTEMPTS`]
    /// stops of it failed in a row.
    fn may_stop(&self) -> bool {
        self.failed_stops < STOP_ATTEMPTS
    }

    /// A stop of the idle shed's live daemon failed for `why`: the next probe tries again until
    /// [`STOP_ATTEMPTS`] have failed in a row, and then none does while that daemon stays live.
    fn stop_failed(&mut self, project_root: &Path, why: &str) {
        self.failed_stops += 1;
        let next = if self.failed_stops < STOP_ATTEMPTS {
            format!(
                "the next probe in {}s tries again, {} of {STOP_ATTEMPTS}",
                PROBE_INTERVAL.as_secs(),
                self.failed_stops + 1
            )
        } else {
            "no probe tries again while it stays live; its supervisor's retirement stops it once \
             more"
                .to_owned()
        };
        cowshed_os::eprintln!(
            "cowshed: the Nx daemon of {} has been idle for {}s and did not stop: {why}; {next}",
            project_root.display(),
            self.idle.get().as_secs(),
        );
    }
}

/// The keeper's idle clock, the pure core of [`NxDaemonKeeper::judge`]: when the shed was last
/// used, and whether the last probe found the daemon live, to tell a daemon coming up from one
/// that has been up.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct IdleClock {
    /// The last use: a probe whose ledger held a job or a session, a job's admission or end, a
    /// session's opening, or a daemon newly up; `None` until one happens. The supervisor's own
    /// start is no use, so a supervisor nothing uses starts no daemon.
    last_use: Option<Instant>,
    daemon_was_live: bool,
}

/// What one probe of the [`IdleClock`] decides.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Idleness {
    /// Used within the bound: a daemon is to run.
    InUse,
    /// Unused for the bound and no daemon live: nothing is left to stop.
    Settled,
    /// Unused for the bound and the daemon live: it is to be stopped.
    Stop,
    /// A mount census owns the fence: neither stop nor start, and the clock is unchanged.
    Suspended,
}

impl IdleClock {
    /// The shed was used at `now`.
    fn use_at(&mut self, now: Instant) {
        self.last_use = Some(now);
    }

    /// One probe at `now`, whose ledger holds use when `working` and which found the daemon
    /// `live`, against the bound `idle`. A daemon that has just come up was asked for by someone:
    /// it gets a whole bound, however idle the shed looks.
    fn probe(
        &mut self,
        now: Instant,
        idle: NxDaemonIdle,
        working: bool,
        live: bool,
        suspended: bool,
    ) -> Idleness {
        if suspended {
            return Idleness::Suspended;
        }
        if working || (live && !self.daemon_was_live) {
            self.use_at(now);
        }
        self.daemon_was_live = live;
        match self.last_use {
            Some(used) if now.duration_since(used) < idle.get() => Idleness::InUse,
            _ if live => Idleness::Stop,
            _ => Idleness::Settled,
        }
    }
}

/// What in the supervisor's ledger uses the shed ([`NxDaemonKeeper::judge`]): a job it runs, or
/// a named session it holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Use {
    Job(JobId),
    Session(String),
}

impl std::fmt::Display for Use {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Job(job) => write!(formatter, "its supervisor runs job {}", job.get()),
            Self::Session(name) => {
                write!(formatter, "its supervisor holds the named session {name}")
            }
        }
    }
}

/// Stop the daemon of the Nx project at `project_root` as `nx daemon --stop` does
/// ([`crate::build_volume::nx::stop_recorded_daemon`]), and say on the supervisor's stderr that
/// it stopped and `why`. Nothing else is asked: the ledger has already said the shed is unused.
async fn stop(project_root: &Path, why: &str) -> std::result::Result<(), String> {
    let data = nx::workspace_data(project_root);
    let stopped =
        tokio::task::spawn_blocking(move || crate::build_volume::nx::stop_recorded_daemon(&data))
            .await
            .map_err(|error| format!("the stop did not finish: {error}"))?
            .map_err(|error| error.to_string())?
            .map_err(|busy| busy.to_string())?;
    if let Some(pid) = stopped {
        cowshed_os::eprintln!(
            "cowshed: stopped the Nx daemon (pid {pid}) of {}: {why}",
            project_root.display(),
        );
    }
    Ok(())
}

/// End the daemon a retiring supervisor's keeper kept for the Nx project at `project_root`, so
/// none outlives the supervisor that watched it (04_sandbox.md). Its jobs have ended by now, so
/// its ledger holds nothing that uses the daemon.
pub(crate) async fn release(project_root: &Path) {
    if let Err(why) = stop(project_root, "its supervisor retired").await {
        cowshed_os::eprintln!(
            "cowshed: the Nx daemon of {} did not stop as its supervisor retired: {why}",
            project_root.display()
        );
    }
}

/// The next completed probe, with at most one blocking read in flight. Dropping this future
/// leaves that read owned by the keeper, so another actor event never starts a duplicate.
pub(crate) async fn next_probe(
    keeper: &mut Option<NxDaemonKeeper>,
    sandbox: &SandboxConfig,
) -> Result<Option<(PathBuf, Probe)>> {
    let Some(keeper) = keeper else {
        return std::future::pending().await;
    };
    if keeper.probing.is_none() {
        keeper.probes.tick().await;
        let Some(project_root) = kept_project(sandbox).map(Path::to_path_buf) else {
            return Ok(None);
        };
        keeper.probing = Some(tokio::task::spawn_blocking(move || {
            let daemon = probe(&nx::daemon_record(&project_root));
            (project_root, daemon)
        }));
    }
    let result = keeper
        .probing
        .as_mut()
        .expect("the keeper owns its probe")
        .await;
    keeper.probing = None;
    result.map(Some).map_err(|error| {
        CowshedError::internal(format!("the Nx daemon probe did not finish: {error}"))
    })
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

/// Stop the job's own daemon process: take the forks it made since the plan while it still runs,
/// `SIGTERM` the exact identity the ledger proved, run the caller's socket-cleanup command, then
/// wait on the exit watch registered before any signal for the shared grace and end the plugin
/// workers it forked ([`super::daemon_watch::Orphans::end`]). Only a join failure of the wait is an
/// error.
#[cfg(target_os = "macos")]
pub(crate) async fn stop_owned_daemon<F, Fut, E>(
    process: super::job_groups::OwnedGroupProcess,
    mut watch: super::daemon_watch::DaemonWatch,
    cleanup: F,
) -> Result<OwnedStopOutcome>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<std::process::Output, E>>,
    E: std::fmt::Display,
{
    let pid = process.pid();
    let mut diagnostics = Vec::new();
    // Once the daemon exits its children are launchd's, and nothing names them its own.
    if let Err(error) = watch.follow_forks() {
        diagnostics.push(format!("following the forks of pid {pid} failed: {error}"));
    }
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
    let waited = crate::storage::lifecycle::dispatch_blocking(move || {
        watch
            .await_exit(grace)
            .map(|exit| exit.map(|orphans| orphans.end(grace)))
    })
    .await;
    let exited = match waited {
        Ok(Ok(Ok(said))) => {
            diagnostics.extend(said);
            true
        }
        Ok(Ok(Err(_running))) => false,
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
    use cowshed_os::test_backstop::ChildBackstop as _;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::CommandBackstop as _;
    use std::os::unix::net::UnixListener;

    use proptest::prelude::*;

    use super::*;
    /// The default bound the keeper tests run against.
    const IDLE: Duration = NxDaemonIdle::DEFAULT.get();

    /// One event the keeper's idle clock hears.
    #[derive(Clone, Debug)]
    enum ClockEvent {
        /// The supervisor admitted a job, opened a session, or a job ended.
        Used,
        /// A probe whose ledger holds use when `working`, finding the daemon `live`.
        Probe { working: bool, live: bool },
    }

    /// Events with the time since the one before each.
    fn clock_events() -> impl Strategy<Value = Vec<(Duration, ClockEvent)>> {
        prop::collection::vec(
            (
                (0_u64..600).prop_map(Duration::from_secs),
                prop_oneof![
                    Just(ClockEvent::Used),
                    (any::<bool>(), any::<bool>())
                        .prop_map(|(working, live)| ClockEvent::Probe { working, live }),
                ],
            ),
            1..40,
        )
    }

    proptest! {
        /// The idle clock stops a live daemon exactly once the shed has gone its whole bound
        /// without use -- a job or session in the ledger at a probe, a job admitted or ended, a
        /// session opened, or a daemon newly up -- and settles a shed whose daemon is gone. A probe
        /// whose ledger holds use never stops one, and a longer bound never stops one sooner.
        #[test]
        fn the_idle_clock_stops_a_live_daemon_only_after_its_whole_bound_without_use(
            idle in (1_u64..900).prop_map(Duration::from_secs),
            more in (0_u64..900).prop_map(Duration::from_secs),
            events in clock_events(),
        ) {
            let idle = NxDaemonIdle::new(idle).expect("a positive bound");
            let longer = NxDaemonIdle::new(idle.get() + more).expect("a positive bound");
            let mut now = Instant::now();
            let mut clock = IdleClock::default();
            let mut longer_clock = IdleClock::default();
            // The model: when the shed was last used, and whether the last probe found a daemon.
            let mut last_use: Option<Instant> = None;
            let mut was_live = false;
            for (after, event) in events {
                now += after;
                match event {
                    ClockEvent::Used => {
                        clock.use_at(now);
                        longer_clock.use_at(now);
                        last_use = Some(now);
                    }
                    ClockEvent::Probe { working, live } => {
                        if working || (live && !was_live) {
                            last_use = Some(now);
                        }
                        was_live = live;
                        let unused = last_use.is_none_or(|used| now - used >= idle.get());
                        let expected = match (unused, live) {
                            (false, _) => Idleness::InUse,
                            (true, true) => Idleness::Stop,
                            (true, false) => Idleness::Settled,
                        };
                        let decided = clock.probe(now, idle, working, live, false);
                        prop_assert_eq!(decided, expected);
                        if working {
                            prop_assert_eq!(decided, Idleness::InUse);
                        }
                        let under_longer = longer_clock.probe(now, longer, working, live, false);
                        if decided == Idleness::InUse {
                            prop_assert_eq!(under_longer, Idleness::InUse);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_delayed_probe_uses_the_current_ledger_and_idle_clock() {
        let epoch = Instant::now();
        for (label, used_at, completed_at, working, live, expected) in [
            ("unused startup", None, 20, false, false, Idleness::Settled),
            (
                "admitted during read",
                Some(19),
                20,
                false,
                false,
                Idleness::InUse,
            ),
            (
                "job still running",
                Some(0),
                900,
                true,
                true,
                Idleness::InUse,
            ),
            (
                "just below idle",
                Some(20),
                319,
                false,
                true,
                Idleness::InUse,
            ),
            (
                "idle live daemon",
                Some(20),
                320,
                false,
                true,
                Idleness::Stop,
            ),
            (
                "idle absent daemon",
                Some(20),
                320,
                false,
                false,
                Idleness::Settled,
            ),
        ] {
            for suspended in [false, true] {
                let mut clock = IdleClock {
                    last_use: used_at.map(|seconds| epoch + Duration::from_secs(seconds)),
                    daemon_was_live: live,
                };
                let before = clock;
                assert_eq!(
                    clock.probe(
                        epoch + Duration::from_secs(completed_at),
                        NxDaemonIdle::DEFAULT,
                        working,
                        live,
                        suspended,
                    ),
                    if suspended {
                        Idleness::Suspended
                    } else {
                        expected
                    },
                    "{label}, suspended={suspended}",
                );
                if suspended {
                    assert_eq!(clock, before, "a census changes no idle-clock fact");
                }
            }
        }
    }

    #[test]
    fn a_zero_idle_bound_is_refused() {
        assert_eq!(NxDaemonIdle::new(Duration::ZERO), None);
        assert_eq!(
            NxDaemonIdle::new(Duration::from_secs(1)).map(NxDaemonIdle::get),
            Some(Duration::from_secs(1))
        );
    }

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
        // Short, under the scratch parent: a Unix socket path is bounded at 104 bytes on macOS.
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cs-nxd");
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
        // WNOWAIT leaves the child waitable.
        cowshed_os::test_backstop::waitid(libc::P_PID, dead, libc::WEXITED | libc::WNOWAIT)
            .expect("wait for the daemon's exit");
        write_record(&record, dead, &socket);
        assert_eq!(probe(&record), Probe::Dead, "a zombie daemon is gone");
        exited.wait_bounded().expect("reap");
        assert_eq!(probe(&record), Probe::Dead);

        // The socket file outlives its listener, so only a connect tells it is served.
        drop(listener);
        write_record(&record, own, &socket);
        assert_eq!(probe(&record), Probe::Refused);

        std::fs::write(&record, b"{\"processId\":0,\"socketPath\":\"/tmp/x\"}").expect("record");
        assert_eq!(probe(&record), Probe::Unreadable);
    }

    /// A project with no Nx state: stopping its daemon finds none to signal, so a verdict made
    /// against it touches no process.
    const NO_PROJECT: &str = "/cowshed-test-no-such-nx-project";

    fn keeper() -> NxDaemonKeeper {
        NxDaemonKeeper::for_role(WorkspaceRole::Workspace, NxDaemonIdle::DEFAULT)
            .expect("a shed keeps a daemon")
    }

    async fn judge(keeper: &mut NxDaemonKeeper, working: bool, daemon: Probe) -> Verdict {
        let working = working.then(|| Use::Job(JobId::new(1).expect("a job id")));
        keeper.judge(Path::new(NO_PROJECT), working, daemon).await
    }

    /// A shed keeps a daemon for [`IDLE`] from its last use and no longer. Its supervisor's start
    /// is no use: a keeper nothing has used wants no daemon at its first probe. A job or session
    /// is use at every probe, and a probe that finds the shed in use after it was released asks
    /// for a daemon again.
    #[tokio::test(start_paused = true)]
    async fn a_keeper_releases_the_daemon_of_a_shed_unused_for_the_idle_interval() {
        let mut keeper = keeper();
        assert_eq!(
            judge(&mut keeper, false, Probe::Unrecorded).await,
            Verdict::Release,
            "a supervisor that just started, with nothing in its ledger, wants no daemon"
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

        // A job admitted and ended between two probes leaves nothing in the ledger at the next
        // one, and is use all the same: the supervisor told the keeper at its admission.
        keeper.used();
        assert_eq!(judge(&mut keeper, false, Probe::Dead).await, Verdict::Keep);
        assert!(!keeper.settled, "a daemon is wanted after a short job");
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

    /// The supervisor's ledger is the keeper's only source of use (04_sandbox.md, "The Nx daemon
    /// stays on"). A host run the ledger does not hold -- its working directory in the checkout and
    /// one of the checkout's task databases open -- is no use: the unused daemon is released an
    /// interval after the shed was last used, as it is with nothing running at all. The probes of
    /// that whole interval, one every [`PROBE_INTERVAL`], begin no process-table walk; a one-shot
    /// census, the guard of a destructive operation, still finds the host run.
    #[cfg(target_os = "macos")]
    #[tokio::test(start_paused = true)]
    async fn a_process_the_ledger_does_not_hold_keeps_no_daemon() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-keeper-host-run-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let data = nx::workspace_data(&root);
        std::fs::create_dir_all(&data).expect("scratch");
        let database = data.join("task-v3.db");
        std::fs::write(&database, b"").expect("database");
        // Backgrounded by an `sh` that exits at once, so launchd adopts it rather than this test
        // process, as it adopts a host shell's run; it holds the database open on descriptor 3 and
        // reads the shell's stdin, a pipe whose write end this test holds until it ends the run.
        let mut shell = std::process::Command::new("/bin/sh");
        shell
            .arg("-c")
            .arg(format!(
                "exec 4<&0; /bin/cat 3<'{}' <&4 >/dev/null 2>&1 & echo $!",
                database.display()
            ))
            .current_dir(&root)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        let mut starting =
            cowshed_os::fork_lock::Spawn::spawn_locked(&mut shell).expect("start a host run");
        let held = starting.stdin.take();
        let started = starting
            .wait_with_output_bounded()
            .expect("the shell exits");
        let run: i32 = String::from_utf8(started.stdout)
            .expect("a pid")
            .trim()
            .parse()
            .expect("a pid");
        let walks = crate::build_volume::nx::process_table_walks();
        let mut keeper = keeper();
        let mut verdicts = Vec::new();
        let mut waited = Duration::ZERO;
        while waited <= IDLE {
            verdicts.push(keeper.judge(&root, None, Probe::Live).await);
            tokio::time::advance(PROBE_INTERVAL).await;
            waited += PROBE_INTERVAL;
        }
        let walked = crate::build_volume::nx::process_table_walks() - walks;
        let census = crate::build_volume::nx::tree_holders(&root);
        let censused = crate::build_volume::nx::process_table_walks() - walks - walked;
        // SAFETY: kill takes plain integers and touches no memory of ours.
        unsafe { libc::kill(run, libc::SIGKILL) };
        drop(held);
        std::fs::remove_dir_all(&root).expect("remove scratch");
        let (idle, young) = verdicts.split_last().expect("probes");
        assert!(
            young.iter().all(|verdict| *verdict == Verdict::Keep),
            "a daemon is kept for the whole interval: {verdicts:?}"
        );
        assert_eq!(
            *idle,
            Verdict::Release,
            "a host run the ledger does not hold is no use"
        );
        assert_eq!(
            walked,
            0,
            "{} probes began {walked} process-table walks",
            verdicts.len()
        );
        assert!(censused > 0, "a one-shot census walks the process table");
        assert!(
            census
                .expect("the one-shot census")
                .iter()
                .any(|holder| holder.pid == run),
            "a one-shot census still finds the host run"
        );
    }

    /// A kept daemon names what in the ledger keeps it, a job or a named session, for as long as
    /// it lasts; once the ledger holds neither, nothing keeps it.
    #[tokio::test(start_paused = true)]
    async fn a_kept_daemon_names_what_in_the_ledger_keeps_it() {
        let mut keeper = keeper();
        let root = Path::new(NO_PROJECT);
        let job = Use::Job(JobId::new(7).expect("a job id"));
        assert_eq!(
            keeper.judge(root, Some(job.clone()), Probe::Live).await,
            Verdict::Keep
        );
        assert_eq!(keeper.kept_by, Some(job));
        tokio::time::advance(IDLE * 2).await;
        let session = Use::Session("build".to_owned());
        assert_eq!(
            keeper.judge(root, Some(session.clone()), Probe::Live).await,
            Verdict::Keep,
            "a job or session is use however long it lasts"
        );
        assert_eq!(keeper.kept_by, Some(session));
        assert_eq!(keeper.judge(root, None, Probe::Live).await, Verdict::Keep);
        assert_eq!(keeper.kept_by, None);
    }

    /// A scratch tree with two PATH directories, `a` and `b`, and the PATH that lists them.
    fn scratch() -> (PathBuf, std::ffi::OsString) {
        let root = std::env::temp_dir().join(format!(
            "cs-nxk-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(root.join("a")).expect("scratch");
        std::fs::create_dir_all(root.join("b")).expect("scratch");
        let path = std::env::join_paths([root.join("a"), root.join("b")]).expect("PATH");
        (root, path)
    }

    fn plant(node: &Path, mode: u32) {
        std::fs::write(node, b"#!/bin/sh\n").expect("node");
        std::fs::set_permissions(node, std::fs::Permissions::from_mode(mode)).expect("mode");
    }

    fn program(request: &ExecRequest) -> &std::ffi::OsStr {
        match &request.command {
            ExecCommand::Argv(argv) => argv[0].as_os_str(),
            ExecCommand::Script(_) => panic!("a start job is an argv"),
        }
    }

    /// One probe as the supervisor makes it of a shed that wants a daemon and finds none: the
    /// start job it would admit.
    fn probe_and_start(
        keeper: &mut NxDaemonKeeper,
        root: &Path,
        node: NodeSource<'_>,
    ) -> Option<ExecRequest> {
        keeper.observe(Probe::Unrecorded, root);
        keeper.start(root, root, node)
    }

    fn refused(keeper: &NxDaemonKeeper) -> String {
        match keeper.state().held {
            Some(NxKeeperHold::Refused { why }) => why,
            held => panic!("the keeper holds its start as refused, not {held:?}"),
        }
    }

    /// The 2026-10-20 incident: no shell activates the shed and no `node` lies on the job's
    /// bootstrap PATH. `sandbox-exec` met it as exit 71 at every probe, 272 starts in 1,361 s.
    /// The keeper now names the `ENOENT` and the PATH at its first probe and admits no job; it
    /// looks again at every probe, so the probe after a `node` appears starts it by its
    /// absolute path.
    #[tokio::test(start_paused = true)]
    async fn a_start_with_no_node_on_its_path_is_refused_without_a_job_until_one_appears() {
        let (root, path) = scratch();
        let node = NodeSource::Path(&path);
        let mut keeper = keeper();

        assert!(probe_and_start(&mut keeper, &root, node).is_none());
        let why = refused(&keeper);
        assert!(why.contains(&root.join("a").display().to_string()), "{why}");
        assert!(
            why.contains(&io::Error::from_raw_os_error(libc::ENOENT).to_string()),
            "the refusal names ENOENT: {why}"
        );
        for _ in 1..272 {
            assert!(
                probe_and_start(&mut keeper, &root, node).is_none(),
                "no job while no `node` can run"
            );
        }
        assert_eq!(refused(&keeper), why, "the same refusal stands");

        plant(&root.join("b/node"), 0o755);
        let request = probe_and_start(&mut keeper, &root, node).expect("the next probe starts it");
        assert_eq!(program(&request), root.join("b/node").as_os_str());
        assert_eq!(keeper.state().held, None);
        std::fs::remove_dir_all(&root).expect("remove scratch");
    }

    /// `node` is found as `execvp` finds it on the job's PATH: one that cannot execute is passed
    /// over for one further on, and when none further on can run, its `EACCES` is named at once.
    #[tokio::test(start_paused = true)]
    async fn a_start_whose_only_node_cannot_execute_is_refused_with_its_eacces_named() {
        let (root, path) = scratch();
        let node = NodeSource::Path(&path);
        plant(&root.join("a/node"), 0o644);
        plant(&root.join("b/node"), 0o755);
        let request = keeper()
            .start(&root, &root, node)
            .expect("the executable `node` further on the PATH starts");
        assert_eq!(program(&request), root.join("b/node").as_os_str());

        std::fs::remove_file(root.join("b/node")).expect("remove");
        let mut keeper = keeper();
        assert!(probe_and_start(&mut keeper, &root, node).is_none());
        let why = refused(&keeper);
        assert!(
            why.contains(&io::Error::from_raw_os_error(libc::EACCES).to_string()),
            "the refusal names EACCES: {why}"
        );
        std::fs::remove_dir_all(&root).expect("remove scratch");
    }

    /// A shed whose shell is activated runs the `node` that shell declares (15_capabilities.md),
    /// which only its start job can find, whatever the bootstrap PATH holds.
    #[tokio::test(start_paused = true)]
    async fn an_activated_shell_names_its_own_node() {
        let (root, _) = scratch();
        let request = keeper()
            .start(&root, &root, NodeSource::Shell)
            .expect("an activated shed starts");
        assert_eq!(program(&request), "node");
        std::fs::remove_dir_all(&root).expect("remove scratch");
    }

    /// One start as the supervisor makes it, its job ending as `ending` while the probe after it
    /// finds the daemon `daemon`; `served` whether a probe found it live while it ran.
    fn one_start(keeper: &mut NxDaemonKeeper, served: bool, ending: Option<&str>, daemon: Probe) {
        let root = Path::new(NO_PROJECT);
        keeper
            .start(root, root, NodeSource::Shell)
            .expect("a start is admitted");
        let job = JobId::new(1).expect("job id");
        keeper.starting = Some(job);
        if served {
            keeper.observe(Probe::Live, root);
        }
        keeper.observe(daemon, root);
        keeper.starting = None;
        keeper.start_ended(job, ending.map(str::to_owned), root, daemon);
    }

    /// The pause the keeper holds its starts in: failures, time left, length and cap.
    fn paused(keeper: &NxDaemonKeeper) -> (u32, Duration, Duration, Duration) {
        match keeper.state().held {
            Some(NxKeeperHold::Paused {
                why,
                failures,
                resumes_in,
                pause,
                cap,
            }) => {
                assert!(
                    why.contains("exited 71"),
                    "the pause names its failure: {why}"
                );
                (failures, resumes_in, pause, cap)
            }
            held => panic!("the keeper holds its starts paused, not {held:?}"),
        }
    }

    /// A start that ran and left no daemon live is a failure no exit status classifies: up to
    /// [`START_ATTEMPTS`] in a row are tried at successive probes, then starts pause for
    /// [`FIRST_PAUSE`], and one start follows each pause, which doubles after every failure up
    /// to [`PAUSE_CAP`]. The pause is answered with its failures, time left, length and cap. A
    /// live daemon closes it and the next pause is [`FIRST_PAUSE`] again.
    #[tokio::test(start_paused = true)]
    async fn failed_starts_pause_after_their_bound_and_a_live_daemon_resets_the_pause() {
        let root = Path::new(NO_PROJECT);
        let mut keeper = keeper();
        for _ in 0..START_ATTEMPTS {
            assert_eq!(keeper.state().held, None, "below the bound");
            one_start(&mut keeper, false, Some("exited 71"), Probe::Dead);
        }
        assert_eq!(
            paused(&keeper),
            (START_ATTEMPTS, FIRST_PAUSE, FIRST_PAUSE, PAUSE_CAP)
        );

        let mut pause = FIRST_PAUSE;
        for failures in START_ATTEMPTS + 1..START_ATTEMPTS + 7 {
            tokio::time::advance(pause - Duration::from_secs(1)).await;
            assert!(
                keeper.start(root, root, NodeSource::Shell).is_none(),
                "no start while the pause runs"
            );
            tokio::time::advance(Duration::from_secs(1)).await;
            one_start(&mut keeper, false, Some("exited 71"), Probe::Dead);
            pause = (pause * 2).min(PAUSE_CAP);
            assert_eq!(paused(&keeper), (failures, pause, pause, PAUSE_CAP));
        }
        assert_eq!(pause, PAUSE_CAP, "the pause stops doubling at its cap");

        keeper.observe(Probe::Live, root);
        assert_eq!(keeper.state().held, None, "a live daemon closes the pause");
        for _ in 0..START_ATTEMPTS {
            one_start(&mut keeper, false, Some("exited 71"), Probe::Dead);
        }
        assert_eq!(
            paused(&keeper),
            (START_ATTEMPTS, FIRST_PAUSE, FIRST_PAUSE, PAUSE_CAP),
            "a live daemon reset the doubling"
        );
    }

    /// A kept daemon whose installation changed stops, deleting its record and exiting 0, so its
    /// keeper starts the successor. That daemon served: its start succeeded, and no number of
    /// such stops counts toward [`START_ATTEMPTS`]. Nor does a start whose job ended while the
    /// daemon is live.
    #[tokio::test(start_paused = true)]
    async fn a_daemon_that_served_and_stopped_is_followed_by_its_successor_never_counted() {
        let mut keeper = keeper();
        for _ in 0..START_ATTEMPTS * 3 {
            one_start(&mut keeper, true, None, Probe::Unrecorded);
            assert_eq!(keeper.state().held, None);
        }
        for _ in 0..START_ATTEMPTS * 3 {
            one_start(&mut keeper, false, Some("exited 1"), Probe::Live);
            assert_eq!(keeper.state().held, None);
        }
        assert_eq!(keeper.failed_starts, 0);
    }

    /// An idle shed's live daemon whose stop fails is stopped again at the next probe up to
    /// [`STOP_ATTEMPTS`] in a row, and then not while it stays live; a probe that finds it gone
    /// counts afresh.
    #[tokio::test(start_paused = true)]
    async fn a_failed_stop_is_tried_again_up_to_its_bound() {
        let root = Path::new(NO_PROJECT);
        let mut keeper = keeper();
        for _ in 0..STOP_ATTEMPTS {
            assert!(keeper.may_stop());
            keeper.stop_failed(root, "the Nx daemon pid 1 did not exit");
        }
        assert!(!keeper.may_stop(), "at the bound");
        tokio::time::advance(IDLE).await;
        assert_eq!(
            judge(&mut keeper, false, Probe::Dead).await,
            Verdict::Release
        );
        assert!(keeper.may_stop(), "the daemon is gone");
    }

    #[cfg(target_os = "macos")]
    fn sleeper() -> std::process::Child {
        use cowshed_os::fork_lock::Spawn as _;
        std::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .expect("a process reading its stdin")
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
        child.wait_bounded().unwrap();
        assert!(
            super::super::job_groups::Process::observe(pid)
                .unwrap()
                .is_none()
        );
    }

    /// A process blocked reading its stdin, a pipe the returned child holds, that leads its own
    /// group, as a job's leader does.
    #[cfg(target_os = "macos")]
    fn group_sleeper() -> std::process::Child {
        use cowshed_os::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;
        std::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("a group leader reading its stdin")
    }

    /// A ledger that names `child`'s group as one admitted job's, the way the supervisor records it.
    #[cfg(target_os = "macos")]
    fn ledger_naming(scratch: &Path, child: &std::process::Child) -> PathBuf {
        let leader = super::super::job_groups::GroupLeader::observe(child.id()).unwrap();
        let ledger = scratch.join("supervisor.groups");
        super::super::job_groups::Recorder::new(ledger.clone())
            .record(
                [super::super::job_groups::JobGroupClaim::group(1, leader)].into_iter(),
                &[],
            )
            .unwrap();
        ledger
    }

    /// A sandboxed group leader that announces READY from its own shell and then blocks reading
    /// its stdin, which stays open with the child. No exec follows READY, so the pid version the
    /// ledger observes is the one the process keeps, and `sandboxed` reads the profile it runs under.
    #[cfg(target_os = "macos")]
    fn sandboxed_group_leader() -> std::process::Child {
        use cowshed_os::fork_lock::Spawn as _;
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

    /// A sandboxed group leader that forks one plugin worker as Nx's daemon forks each of its own
    /// (`plugins/isolation/isolated-plugin.js`): detached, leading a session of its own, and
    /// running on past its daemon's exit unless something ends it. The worker announces its pid
    /// once its session is its own; the leader then blocks reading its stdin.
    #[cfg(target_os = "macos")]
    fn sandboxed_daemon_with_worker() -> (std::process::Child, super::super::job_groups::Process) {
        use cowshed_os::fork_lock::Spawn as _;
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;
        let mut child = std::process::Command::new("/usr/bin/sandbox-exec")
            .args([
                "-p",
                "(version 1)(allow default)",
                "/bin/sh",
                "-c",
                "/usr/bin/perl -MPOSIX -e 'POSIX::setsid() > 0 or die \"setsid: $!\"; \
                 print \"WORKER $$\\n\"; close STDOUT; \
                 select(undef, undef, undef, undef) while 1' & read _",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("a sandboxed daemon with a worker");
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().expect("its stdout"))
            .read_line(&mut line)
            .expect("the worker announces itself");
        let worker = line
            .trim()
            .strip_prefix("WORKER ")
            .and_then(|pid| pid.parse().ok())
            .unwrap_or_else(|| panic!("the worker's pid: {line:?}"));
        let worker = super::super::job_groups::Process::observe(worker)
            .expect("observe the worker")
            .expect("the worker runs");
        (child, worker)
    }

    /// Whether `worker` outlived the stop that just returned; one that did is ended here, so a
    /// failing run leaves no sleeper behind.
    #[cfg(target_os = "macos")]
    fn outlived(worker: &super::super::job_groups::Process) -> bool {
        let alive = worker.record().expect("read the worker").is_some();
        if alive {
            worker.signal(libc::SIGKILL).expect("end the stray worker");
        }
        alive
    }

    /// Stopping a ledger-proven daemon, as land does, ends the plugin workers it forked
    /// (04_sandbox.md, "its plugin workers go with it"). Nx forks each worker detached, so it
    /// leads its own session, and once the daemon exits launchd adopts it: from then on nothing
    /// names it the daemon's. The worker here would sleep on past its daemon; the stop returns only
    /// once it has exited.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn stopping_an_owned_daemon_ends_the_plugin_workers_it_forked() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-owned-workers",
        );
        let (mut daemon, worker) = sandboxed_daemon_with_worker();
        let ledger = ledger_naming(&scratch, &daemon);
        let record = scratch.join("server-process.json");
        write_record(&record, daemon.id(), &scratch.join("refusing.sock"));
        let DaemonStopPlan::Owned { process, watch } =
            prepare_daemon_stop(&record, &ledger).unwrap()
        else {
            panic!("a ledger-proven live daemon is owned");
        };
        let outcome = stop_owned_daemon(process, watch, || async {
            std::process::Command::new("/bin/sh")
                .args(["-c", "exit 0"])
                .output_bounded()
        })
        .await
        .unwrap();
        daemon.wait_bounded().unwrap();
        let outlived = outlived(&worker);
        assert!(outcome.exited, "the daemon exits: {outcome:?}");
        assert!(
            !outlived,
            "the daemon's plugin worker {} outlived its stop: {outcome:?}",
            worker.pid()
        );
    }

    /// Closing a checkout's Nx state -- the keeper's stop of an idle shed's daemon, and every
    /// retirement's -- ends the daemon's plugin workers with it, so the census that follows finds
    /// none of them on the mount.
    #[cfg(target_os = "macos")]
    #[test]
    fn closing_a_checkouts_nx_state_ends_its_daemons_plugin_workers() {
        let scratch = crate::temp_root::TempRoot::new("nx-close-workers");
        let data = scratch.join("workspace-data");
        std::fs::create_dir_all(data.join("d")).unwrap();
        let socket = scratch.join("d.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let (mut daemon, worker) = sandboxed_daemon_with_worker();
        write_record(&data.join("d/server-process.json"), daemon.id(), &socket);
        let closed = crate::build_volume::nx::close_workspace_data(&data).unwrap();
        daemon.wait_bounded().unwrap();
        drop(listener);
        let outlived = outlived(&worker);
        assert_eq!(closed, Ok(()), "the daemon stops");
        assert!(
            !outlived,
            "the daemon's plugin worker {} outlived its stop",
            worker.pid()
        );
    }

    /// A daemon the ledger proves the job's is stopped through its exact identity: the plan carries
    /// the exit watch from before the signal, the signal ends the process, and the watch reports it.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_daemon_the_ledger_proves_is_owned_and_stopped_through_its_watch() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-owned-stop",
        );
        let mut child = sandboxed_group_leader();
        let pid = i32::try_from(child.id()).unwrap();
        let ledger = ledger_naming(&scratch, &child);
        let record = scratch.join("server-process.json");
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        let DaemonStopPlan::Owned { process, watch } =
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
        assert!(
            watch
                .await_exit(crate::build_volume::nx::DAEMON_EXIT_GRACE)
                .unwrap()
                .is_ok(),
            "the watch reports the exit"
        );
        child.wait_bounded().unwrap();
    }

    /// A live process the ledger does not name is never signalled: a fresh, well-formed record
    /// naming it passes the supplementary checks, and the ledger alone refuses it.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_fresh_forged_record_naming_a_process_the_ledger_does_not_prove_is_unproven() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-forged",
        );
        let mut foreign = sandboxed_group_leader();
        let ledger = scratch.join("supervisor.groups");
        super::super::job_groups::Recorder::new(ledger.clone())
            .record(std::iter::empty(), &[])
            .unwrap();
        let record = scratch.join("server-process.json");
        write_record(&record, foreign.id(), &scratch.join("refusing.sock"));
        let plan = prepare_daemon_stop(&record, &ledger).unwrap();
        let alive = foreign.try_wait().unwrap().is_none();
        foreign.kill().unwrap();
        foreign.wait_bounded().unwrap();
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
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-race",
        );
        let mut owned = sandboxed_group_leader();
        let owned_pid = i32::try_from(owned.id()).unwrap();
        let ledger = ledger_naming(&scratch, &owned);
        let record = scratch.join("server-process.json");
        write_record(&record, owned.id(), &scratch.join("refusing.sock"));
        let DaemonStopPlan::Owned { process, watch } =
            prepare_daemon_stop(&record, &ledger).unwrap()
        else {
            panic!("the ledger proves the owned leader before the race");
        };
        let mut foreign = sandboxed_group_leader();
        let foreign_pid = i32::try_from(foreign.id()).unwrap();
        let forged = scratch.join("forged.json");
        write_record(&forged, foreign.id(), &scratch.join("refusing.sock"));
        std::fs::rename(&forged, &record).unwrap();
        let outcome = stop_owned_daemon(process, watch, || async {
            std::process::Command::new("/bin/sh")
                .args(["-c", "exit 0"])
                .output_bounded()
        })
        .await
        .unwrap();
        let owned_exited = owned.try_wait().unwrap().is_some();
        if !owned_exited {
            owned.kill().unwrap();
        }
        owned.wait_bounded().unwrap();
        let foreign_alive = foreign.try_wait().unwrap().is_none();
        if foreign_alive {
            foreign.kill().unwrap();
        }
        foreign.wait_bounded().unwrap();
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
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-stale",
        );
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
        child.wait_bounded().unwrap();
    }

    /// A reaped leader names no process and proves nothing: its group id may belong to a later
    /// process, so the record is nothing to stop and the ledger no longer owns anything.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_reaped_leader_proves_nothing_and_its_record_names_nothing() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-reaped",
        );
        let mut child = group_sleeper();
        let pid = i32::try_from(child.id()).unwrap();
        let ledger = ledger_naming(&scratch, &child);
        let record = scratch.join("server-process.json");
        write_record(&record, child.id(), &scratch.join("refusing.sock"));
        child.kill().unwrap();
        child.wait_bounded().unwrap();
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
        use cowshed_os::fork_lock::Spawn as _;
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-survivor",
        );
        let mut leader = std::process::Command::new("/bin/sh")
            .args(["-c", "exec 3<&0; read _ <&3 & echo READY; exit 0"])
            .stdin(std::process::Stdio::piped())
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
        // The member reads this pipe: reaping the leader must not close it.
        let held = leader.stdin.take();
        leader.wait_bounded().unwrap();
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
            .status_bounded()
            .expect("kill the survivor's group");
        drop(held);
    }

    /// Absent records and ledgers name nothing, a live pid without a ledger is unproven, and a
    /// malformed record or a pid below one is an error, never a guess.
    #[cfg(target_os = "macos")]
    #[test]
    fn absent_and_malformed_records_are_scoped_and_never_guessed() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-scoped",
        );
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
        child.wait_bounded().unwrap();
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
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-unverifiable",
        );
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
        child.wait_bounded().unwrap();
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
        use cowshed_os::fork_lock::Spawn as _;
        use std::io::{BufRead as _, Write as _};
        use std::os::unix::process::CommandExt as _;
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-exec-identity",
        );
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
        let DaemonStopPlan::Owned { process, watch } =
            prepare_daemon_stop(&record, &ledger).unwrap()
        else {
            panic!("the settled pre-exec leader is ledger-owned");
        };
        child.stdin.as_mut().unwrap().write_all(b"exec\n").unwrap();
        line.clear();
        stdout.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "EXECUTED");
        let outcome = stop_owned_daemon(process, watch, || async {
            std::process::Command::new("/bin/true").output_bounded()
        })
        .await
        .unwrap();
        let alive = child.try_wait().unwrap().is_none();
        child.kill().unwrap();
        child.wait_bounded().unwrap();
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
    use cowshed_os::test_backstop::{
        AsyncCommandBackstop as _, ChildBackstop as _, CommandBackstop as _,
    };
    use std::os::unix::process::CommandExt as _;

    use super::*;
    use crate::runtime::job_groups::{GroupLeader, Recorder};
    use cowshed_os::fork_lock::Spawn as _;

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
        Recorder::new(ledger.clone())
            .record(
                [super::super::job_groups::JobGroupClaim::group(1, leader)].into_iter(),
                &[],
            )
            .expect("record the job's group");
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
            .output_bounded()
            .expect("the sandboxed kill runs")
    }

    #[tokio::test]
    async fn an_owned_daemon_refuses_a_sandboxed_signal_and_a_failed_cleanup_never_aborts() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-owned-stop",
        );
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

        let DaemonStopPlan::Owned { process, watch } =
            prepare_daemon_stop(&record_path, &ledger).expect("the plan reads")
        else {
            panic!("a ledger-proven live process is owned");
        };
        let Err(watch) = watch.await_exit(Duration::ZERO).expect("the watch answers") else {
            panic!("still running");
        };

        let outcome = stop_owned_daemon(process, watch, || async {
            let mut command = tokio::process::Command::new("/bin/sh");
            command.args(["-c", "exit 17"]);
            command.output_bounded().await
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
        child.wait_bounded().expect("reap the stopped process");
    }

    #[test]
    fn a_record_naming_a_process_the_ledger_does_not_own_signals_nothing() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-forged-nominee",
        );
        let mut foreign = sandboxed_sleeper();
        let ledger = scratch.join("supervisor.groups");
        Recorder::new(ledger.clone())
            .record(std::iter::empty(), &[])
            .expect("an empty ledger");
        let record_path = scratch.join("server-process.json");
        write_daemon_record(&record_path, foreign.id());

        let plan = prepare_daemon_stop(&record_path, &ledger).expect("the plan reads");
        let pid_before_cleanup = foreign.id();
        let alive = foreign.try_wait().expect("poll").is_none();
        foreign.kill().expect("the test stops its own sleeper");
        foreign.wait_bounded().expect("reap");
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
            .arg("--kept")
            .clear_nx_environment()
            .envs(nx_environment(root))
            // Nx Console's status check otherwise has the daemon fetch `nx@latest` in an npm child.
            .env("NX_USE_LOCAL", "true")
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
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-real-authority",
        );
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
        // The record is the event; a daemon that dies first is noticed by its exit.
        while !record_path.is_file() {
            if let Some(status) = daemon.try_wait().expect("poll the daemon") {
                let mut stderr = String::new();
                if let Some(mut pipe) = daemon.stderr.take() {
                    std::io::Read::read_to_string(&mut pipe, &mut stderr).ok();
                }
                panic!("the real daemon exited before publishing its record ({status}): {stderr}");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let leader = GroupLeader::observe(daemon.id()).expect("observe the daemon's group leader");
        let ledger = scratch.join("supervisor.groups");
        Recorder::new(ledger.clone())
            .record(
                [super::super::job_groups::JobGroupClaim::group(1, leader)].into_iter(),
                &[],
            )
            .expect("record the job's group");
        let DaemonStopPlan::Owned { process, watch } =
            prepare_daemon_stop(&record_path, &ledger).expect("the plan reads")
        else {
            panic!("the ledger proves the real daemon is the job's own");
        };

        // RED: the sandboxed stock stop is refused by the kernel, and the exit watch stays silent.
        let refused = sandboxed_nx_stop(&root, &node, &nx)
            .output_bounded()
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
        let Err(watch) = watch.await_exit(Duration::ZERO).expect("the watch answers") else {
            panic!("the sandboxed stop signalled nothing");
        };

        let outcome = stop_owned_daemon(process, watch, || async {
            let mut command = tokio::process::Command::from(sandboxed_nx_stop(&root, &node, &nx));
            command.output_bounded().await
        })
        .await
        .expect("the controller stops its owned daemon");
        assert!(
            outcome.exited,
            "the exact-identity SIGTERM ended the real daemon: {outcome:?}"
        );
        daemon.wait_bounded().expect("reap the real daemon");
    }

    /// Stops the daemon a `workspace-data` directory records when dropped, however the test ends.
    struct StopsRecordedDaemon(PathBuf);

    impl Drop for StopsRecordedDaemon {
        fn drop(&mut self) {
            match crate::build_volume::nx::stop_recorded_daemon(&self.0) {
                Ok(Ok(_)) => {}
                Ok(Err(busy)) => cowshed_os::eprintln!("the fixture's daemon did not stop: {busy}"),
                Err(error) => cowshed_os::eprintln!("the fixture's daemon did not stop: {error}"),
            }
        }
    }

    /// The keeper stops an idle shed's real kept daemon once the shed has gone its configured
    /// bound without use, and the plugin workers the daemon forked with it, although the daemon's
    /// own sandbox refuses it every signal to them; the next Nx call then starts a daemon, which
    /// the keeper keeps a whole bound (04_sandbox.md, "The Nx daemon stays on").
    #[tokio::test(start_paused = true)]
    async fn an_idle_sheds_real_kept_daemon_stops_with_its_workers_and_the_next_nx_call_starts_one()
    {
        use super::super::process_events::{Darwin, ProcessReads as _};
        let (nx, node) = declared_nx();
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "nx-daemon-idle-reap",
        );
        let root = scratch.join("workspace");
        std::fs::create_dir_all(root.join("p")).expect("workspace");
        std::fs::write(
            root.join("package.json"),
            r#"{"name":"fixture","private":true}"#,
        )
        .expect("package.json");
        // A graph plugin of the workspace's own, which the daemon runs in an isolated worker.
        std::fs::write(root.join("nx.json"), r#"{"plugins":["./plugin.js"]}"#).expect("nx.json");
        std::fs::write(
            root.join("plugin.js"),
            "exports.createNodesV2 = ['**/project.json', () => []];
",
        )
        .expect("plugin.js");
        std::fs::write(root.join("p/project.json"), r#"{"name":"p"}"#).expect("project.json");
        std::fs::create_dir_all(root.join("node_modules")).expect("node_modules");
        std::os::unix::fs::symlink(&nx, root.join("node_modules/nx"))
            .expect("the workspace's own nx");
        let record = nx::daemon_record(&root);
        let _stops = StopsRecordedDaemon(nx::workspace_data(&root));
        let mut daemon = real_daemon(&root, &node, &nx);
        // The record, then the daemon's first fork, is the event; a daemon that dies first is
        // noticed by its exit.
        let workers = loop {
            if let Some(status) = daemon.try_wait().expect("poll the daemon") {
                panic!("the real daemon exited before it forked a plugin worker ({status})");
            }
            let children = Darwin
                .children(daemon.id())
                .expect("list the daemon's children");
            if record.is_file() && !children.is_empty() {
                break children
                    .into_iter()
                    .filter_map(|pid| {
                        super::super::job_groups::Process::observe(
                            i32::try_from(pid).expect("a pid fits i32"),
                        )
                        .expect("observe a plugin worker")
                    })
                    .collect::<Vec<_>>();
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        assert!(!workers.is_empty(), "the daemon forked its plugin workers");

        let idle = NxDaemonIdle::new(Duration::from_secs(90)).expect("a positive bound");
        let mut keeper = NxDaemonKeeper::for_role(WorkspaceRole::Workspace, idle)
            .expect("a shed keeps a daemon");
        assert_eq!(probe(&record), Probe::Live);
        assert_eq!(
            keeper.judge(&root, None, Probe::Live).await,
            Verdict::Keep,
            "a daemon that comes up gets a whole bound"
        );
        tokio::time::advance(idle.get() - Duration::from_secs(1)).await;
        assert_eq!(
            keeper.judge(&root, None, probe(&record)).await,
            Verdict::Keep
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            keeper.judge(&root, None, probe(&record)).await,
            Verdict::Release,
            "the shed went its whole bound unused"
        );
        let status = daemon.wait_bounded().expect("reap the real daemon");
        let mut outlived = Vec::new();
        for worker in &workers {
            if worker.record().expect("read a plugin worker").is_some() {
                outlived.push(worker.pid());
                worker.signal(libc::SIGKILL).expect("end the stray worker");
            }
        }
        assert!(
            outlived.is_empty(),
            "plugin workers {outlived:?} outlived their daemon's stop ({status})"
        );
        assert_ne!(probe(&record), Probe::Live, "the daemon is gone");
        assert_eq!(
            keeper.judge(&root, None, probe(&record)).await,
            Verdict::Release
        );
        assert!(keeper.settled, "nothing is left to stop");

        // The next Nx call finds no daemon and starts one, as any client does.
        let shown = std::process::Command::new(&node)
            .arg(nx.join("dist/bin/nx.js"))
            .args(["show", "projects", "--json"])
            .clear_nx_environment()
            .envs(nx_environment(&root))
            .env("NX_DAEMON", "true")
            .env("NX_USE_LOCAL", "true")
            .current_dir(&root)
            .output_bounded()
            .expect("the Nx call runs");
        assert!(
            shown.status.success(),
            "the Nx call: {}",
            String::from_utf8_lossy(&shown.stderr)
        );
        assert_eq!(probe(&record), Probe::Live, "the Nx call started a daemon");
        assert_eq!(
            keeper.judge(&root, None, Probe::Live).await,
            Verdict::Keep,
            "the daemon the Nx call started gets a whole bound"
        );
        assert!(!keeper.settled);
    }
}
