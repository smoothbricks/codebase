//! The process groups a served supervisor's running jobs lead, kept beside its socket so that
//! whoever finds the supervisor gone can end them (11_shell.md "Supervisor recovery").
//!
//! The ledger is a complete snapshot, replaced whole (write a temporary, rename) each time a
//! job's group starts or its job is sealed, so a reader never sees half a write. Each group
//! carries its leader's birth time as the job's parent observed it before it could reap the
//! leader ([`Birth`]), never observed again: a later observation could record a process that has
//! since reused the pid.
//!
//! Whoever reads the ledger is not the parent of the processes it names, so it cannot stop them
//! being reaped and their ids reused, and `killpg` on a recorded id could reach a stranger's group
//! that took the id the instant after any check. So no group is signalled as a whole. The group's
//! processes are read while the recorded leader -- running, or exited but not yet reaped -- is
//! seen holding the group id both before and after: no process can be given the leader's pid
//! while it exists, so no other group could have had the id meanwhile, and every process read was
//! the job's. Each is then signalled through the identity the kernel never reissues -- its pid
//! and that pid's version ([`Member`]) -- so a signal reaches that process or nothing. Once the
//! leader is reaped nothing proves the processes still holding the id the job's: such a group is
//! never signalled, and the ledger that names it is kept, and reported, until nothing holds the
//! id.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::api::dto::ExitStatus;

/// How often a group signalled with SIGKILL is read again for processes still running in it,
/// and how many times before the group is reported as surviving SIGKILL.
const KILL_SETTLE: Duration = Duration::from_millis(10);
const KILL_ROUNDS: u32 = 100;

/// The ledger beside a supervisor socket.
pub fn ledger_path(socket: &Path) -> PathBuf {
    socket.with_extension("groups")
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Ledger {
    /// The supervisor process that wrote it.
    supervisor: u32,
    groups: Vec<Group>,
    /// Its groups were ended after the supervisor was lost; the jobs still await sealing.
    #[serde(default)]
    ended: bool,
    /// Groups a lost supervisor recorded that no later one could end or forget; see
    /// [`UnresolvedGroup`]. Every ledger of the workspace carries them until nothing holds their
    /// ids.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    unresolved: Vec<UnresolvedGroup>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Group {
    job_id: u64,
    pgid: i32,
    /// The leader's birth time as its parent observed it ([`birth_time`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leader_birth: Option<u64>,
    /// What earlier builds recorded instead: the leader's start time as observed when the ledger
    /// was written, `None` when it was already gone. Only read, so their ledgers stay recoverable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leader_start: Option<u64>,
}

/// A group a lost supervisor recorded whose leader was gone when a later supervisor took the
/// workspace, while processes still held its id. They may be the job's surviving processes, or a
/// stranger's group that took the id after the job's group had ended: nothing distinguishes the
/// two, so the group is never signalled. It is not forgotten either -- every later ledger of the
/// workspace carries it, and `doctor` reports it -- until nothing holds the id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnresolvedGroup {
    /// The supervisor process that recorded the group and was lost.
    lost_supervisor: u32,
    group: Group,
}

impl UnresolvedGroup {
    pub fn job_id(&self) -> u64 {
        self.group.job_id
    }

    pub fn pgid(&self) -> i32 {
        self.group.pgid
    }

    pub fn lost_supervisor(&self) -> u32 {
        self.lost_supervisor
    }

    /// Why the group could not be ended, derived from what its record holds.
    pub fn reason(&self) -> UnresolvedReason {
        if self.group.leader_birth.is_some() || self.group.leader_start.is_some() {
            UnresolvedReason::LeaderGone
        } else {
            UnresolvedReason::LeaderNeverIdentified
        }
    }

    /// Whether processes still hold the id. Anything else -- the id held by nobody, or led by a
    /// process that is not the recorded leader -- means the job's group has ended.
    fn still_held(&self) -> io::Result<bool> {
        Ok(!matches!(ownership(&self.group)?, Ownership::NotOwned))
    }
}

/// Why a group a lost supervisor recorded could not be ended ([`UnresolvedGroup`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnresolvedReason {
    /// Its recorded leader was reaped: the processes holding the id are no longer provably the
    /// job's.
    LeaderGone,
    /// An earlier build recorded the group without its leader's identity.
    LeaderNeverIdentified,
}

/// A job's process-group leader as its parent observed it: the group id and the leader's birth
/// time. Carried unchanged into every ledger rewrite; see [`Birth`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupLeader {
    pgid: i32,
    birth: u64,
}

impl GroupLeader {
    /// Observe the leader of the group `pid` leads, which must be the caller's own child, not yet
    /// reaped. A process already reaped has no identity left to observe, so that is an error.
    pub fn observe(pid: u32) -> io::Result<Self> {
        let pgid = i32::try_from(pid)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let leader = Self::from_parts(pgid, 0)?;
        let birth = birth_time(pgid)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("process {pgid} was already reaped"),
            )
        })?;
        Ok(Self { birth, ..leader })
    }

    /// A leader another process observed and reported over the shell-host wire.
    pub(super) fn from_parts(pgid: i32, birth: u64) -> io::Result<Self> {
        if pgid <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a job process group must be positive",
            ));
        }
        Ok(Self { pgid, birth })
    }

    pub fn pgid(self) -> i32 {
        self.pgid
    }

    pub fn birth(self) -> u64 {
        self.birth
    }
}

/// A job's process as its parent saw it before anything could reap it: the identity of the group
/// it leads, or why that identity could not be read.
///
/// Only the parent observes it truthfully. Once the leader is reaped its pid may name any later
/// process, so an observation made afterwards -- by the supervisor when an event reaches it, or
/// by a ledger rewrite -- could record a stranger as a group the ledger may end. The parent
/// observes before it waits, the supervisor carries the result, and nothing observes it again.
/// A leader that already exited but is not yet reaped still has its birth time ([`birth_time`]),
/// so a parent's observation names it even then.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Birth {
    Observed(GroupLeader),
    /// The leader's identity could not be read. The job still runs and reports its pid, but its
    /// group never enters a ledger: nothing may end a group no one could identify.
    Unobserved {
        pid: u32,
        reason: String,
    },
}

impl Birth {
    /// Observe `pid`, which leads a group and is the caller's own child, not yet reaped.
    pub fn of(pid: u32) -> Self {
        match GroupLeader::observe(pid) {
            Ok(leader) => Self::Observed(leader),
            Err(error) => Self::Unobserved {
                pid,
                reason: error.to_string(),
            },
        }
    }

    pub fn pid(&self) -> u32 {
        match self {
            Self::Observed(leader) => leader.pgid.unsigned_abs(),
            Self::Unobserved { pid, .. } => *pid,
        }
    }

    pub fn leader(&self) -> Option<GroupLeader> {
        match self {
            Self::Observed(leader) => Some(*leader),
            Self::Unobserved { .. } => None,
        }
    }
}

/// What a ledger rewrite carried over from the unresolved groups the supervisor inherited.
#[derive(Debug)]
pub struct Recorded {
    /// The inherited groups processes still hold, and those that could not be inspected: an
    /// inherited group is dropped only once it is seen released.
    pub carried: Vec<UnresolvedGroup>,
    /// Why some inherited groups could not be inspected. They are carried unchanged.
    pub uninspected: Vec<io::Error>,
}

/// Replace the ledger at `path` with these `(job id, group leader)` pairs and the `inherited`
/// unresolved groups not seen released. One that cannot be inspected is carried as it is, so the
/// current jobs' groups are recorded whatever an inherited group's inspection does.
pub fn record(
    path: &Path,
    groups: &[(u64, GroupLeader)],
    inherited: &[UnresolvedGroup],
) -> io::Result<Recorded> {
    let mut carried = Vec::with_capacity(inherited.len());
    let mut uninspected = Vec::new();
    for group in inherited {
        match group.still_held() {
            Ok(false) => {}
            Ok(true) => carried.push(*group),
            Err(error) => {
                carried.push(*group);
                uninspected.push(error);
            }
        }
    }
    let ledger = Ledger {
        supervisor: std::process::id(),
        ended: false,
        groups: groups
            .iter()
            .map(|&(job_id, leader)| Group {
                job_id,
                pgid: leader.pgid,
                leader_birth: Some(leader.birth),
                leader_start: None,
            })
            .collect(),
        unresolved: carried.clone(),
    };
    replace(path, &ledger)?;
    Ok(Recorded {
        carried,
        uninspected,
    })
}

fn still_held(groups: &[UnresolvedGroup]) -> io::Result<Vec<UnresolvedGroup>> {
    let mut held = Vec::with_capacity(groups.len());
    for group in groups {
        if group.still_held()? {
            held.push(*group);
        }
    }
    Ok(held)
}

fn replace(path: &Path, ledger: &Ledger) -> io::Result<()> {
    let bytes = serde_json::to_vec(ledger).map_err(io::Error::other)?;
    let temporary = path.with_extension(format!("groups.{}", std::process::id()));
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(&temporary, path)
}

/// Whose ledger may be acted on.
#[derive(Clone, Copy, Debug)]
pub enum Writer {
    /// Only the one this supervisor process wrote: a watcher of that process, which must not
    /// touch a ledger a newer supervisor of the workspace has written since.
    Process(u32),
    /// Whichever supervisor wrote it: the caller holds the workspace's socket, so every
    /// earlier supervisor is gone.
    Any,
}

/// What ending a ledger's groups did.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Ended {
    /// Jobs whose groups were signalled.
    pub signalled: Vec<u64>,
    /// Jobs whose groups processes still hold although their recorded leader is gone: not
    /// signalled ([`UnresolvedGroup`]).
    pub unresolved: Vec<u64>,
}

/// End every group the ledger at `path` names that is still the job's — TERM, `grace`, then
/// KILL — when `writer` wrote it, and mark it ended; the ledger stays until the next supervisor
/// of the workspace takes it ([`take_lost`]).
pub fn end_recorded(path: &Path, writer: Writer, grace: Duration) -> io::Result<Ended> {
    let Some(mut ledger) = read(path)? else {
        return Ok(Ended::default());
    };
    if let Writer::Process(pid) = writer
        && pid != ledger.supervisor
    {
        return Ok(Ended::default());
    }
    let (signalled, unresolved) = end_groups(&ledger.groups, grace)?;
    ledger.ended = true;
    replace(path, &ledger)?;
    Ok(Ended {
        signalled,
        unresolved: unresolved.iter().map(|group| group.job_id).collect(),
    })
}

/// For the supervisor now holding the workspace's socket: end whatever its lost predecessor's
/// ledger still names that is still the job's, and take over the ledger. Which jobs to seal as
/// lost is the job records' to say, not the ledger's: power loss can take the ledger with the
/// processes it names.
///
/// Returns the unresolved groups -- the predecessor's, and those it inherited -- that processes
/// still hold. They are written into a ledger of this process, which its supervisor carries on
/// ([`record`]); with none, the ledger is removed.
pub fn take_lost(path: &Path, grace: Duration) -> io::Result<Vec<UnresolvedGroup>> {
    let Some(ledger) = read(path)? else {
        return Ok(Vec::new());
    };
    // A prior signaling pass is not authority to discard a still-live group's evidence.
    let (_, newly) = end_groups(&ledger.groups, grace)?;
    let mut unresolved = still_held(&ledger.unresolved)?;
    unresolved.extend(newly.into_iter().map(|group| UnresolvedGroup {
        lost_supervisor: ledger.supervisor,
        group,
    }));
    if unresolved.is_empty() {
        std::fs::remove_file(path)?;
    } else {
        replace(
            path,
            &Ledger {
                supervisor: std::process::id(),
                groups: Vec::new(),
                ended: false,
                unresolved: unresolved.clone(),
            },
        )?;
    }
    Ok(unresolved)
}

/// The unresolved groups the ledger at `path` carries that processes still hold, observed now.
/// Reads only: the ledger's writer drops released ones.
pub fn unresolved(path: &Path) -> io::Result<Vec<UnresolvedGroup>> {
    match read(path)? {
        Some(ledger) => still_held(&ledger.unresolved),
        None => Ok(Vec::new()),
    }
}

fn read(path: &Path) -> io::Result<Option<Ledger>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Whether a recorded group may be signalled as the job's.
enum Ownership {
    /// The recorded leader held the group id while these processes were read holding it: they
    /// were the job's.
    Owned(Vec<Process>),
    /// Nothing holds the group id, or its leader is another process.
    NotOwned,
    /// The recorded leader is gone (or was never identified) and processes hold the group id.
    /// They may be the job's, or a stranger's that took the id after the job's group had ended:
    /// no signal.
    Unidentified,
}

/// TERM the owned groups' processes, wait `grace`, prove ownership again, then KILL those still
/// running, reading the group again after each KILL until nothing in it runs: a process that
/// forked between a read and its own KILL leaves a child the next read finds. Returns the jobs
/// signalled and the groups left unresolved: their recorded leader is gone -- before TERM, or
/// after it -- while processes still hold the id, so they are not signalled. A failed inspection
/// or signal, or a group still running after every KILL round, is an error and leaves the
/// caller's original ledger available for retry.
fn end_groups(groups: &[Group], grace: Duration) -> io::Result<(Vec<u64>, Vec<Group>)> {
    let mut signalled = Vec::with_capacity(groups.len());
    let mut unresolved = Vec::new();
    for group in groups {
        match ownership(group)? {
            Ownership::Owned(members) => {
                let mut reached = false;
                for member in &members {
                    reached |= member.signal(libc::SIGTERM)?;
                }
                if reached {
                    signalled.push(group.job_id);
                }
            }
            Ownership::NotOwned => {}
            Ownership::Unidentified => unresolved.push(*group),
        }
    }
    if !signalled.is_empty() {
        std::thread::sleep(grace);
        for group in groups
            .iter()
            .filter(|group| signalled.contains(&group.job_id))
        {
            if kill_owned(group)? == Killed::Unidentified {
                // The leader died and was reaped while processes held on.
                unresolved.push(*group);
            }
        }
    }
    Ok((signalled, unresolved))
}

/// What KILLing a group's processes until none runs ended with.
#[derive(Debug, Eq, PartialEq)]
enum Killed {
    /// Nothing of the job's runs in the group any more.
    Ended,
    /// The recorded leader is gone while processes hold the id: they are not signalled.
    Unidentified,
}

fn kill_owned(group: &Group) -> io::Result<Killed> {
    for _ in 0..KILL_ROUNDS {
        match ownership(group)? {
            Ownership::Owned(members) if members.is_empty() => return Ok(Killed::Ended),
            Ownership::Owned(members) => {
                for member in &members {
                    member.signal(libc::SIGKILL)?;
                }
                std::thread::sleep(KILL_SETTLE);
            }
            Ownership::NotOwned => return Ok(Killed::Ended),
            Ownership::Unidentified => return Ok(Killed::Unidentified),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "process group {} still has running processes after {KILL_ROUNDS} rounds of SIGKILL",
            group.pgid
        ),
    ))
}

fn ownership(group: &Group) -> io::Result<Ownership> {
    if group.pgid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a recorded job process group must be positive",
        ));
    }
    let unverifiable = |error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("cannot verify process group {}: {error}", group.pgid),
        )
    };
    // `Some(true)` while the recorded leader holds its pid, `Some(false)` while another process
    // does; `None` once no process holds it, or the group never had an identified leader.
    let leader = || -> io::Result<Option<bool>> {
        Ok(match (group.leader_birth, group.leader_start) {
            (Some(recorded), _) => birth_time(group.pgid)
                .map_err(unverifiable)?
                .map(|now| now == recorded),
            (None, Some(recorded)) => start_time(group.pgid)
                .map_err(unverifiable)?
                .map(|now| now == recorded),
            (None, None) => None,
        })
    };
    match leader()? {
        Some(false) => return Ok(Ownership::NotOwned),
        Some(true) => {
            let members = members_of(group.pgid)?;
            // The recorded leader held its pid -- the group's id -- before the processes were
            // read and still holds it after, so it held it throughout: no other group can have
            // had the id meanwhile, and every process read holding it was the job's.
            if leader()? == Some(true) {
                return Ok(Ownership::Owned(members));
            }
        }
        None => {}
    }
    Ok(if members_of(group.pgid)?.is_empty() {
        Ownership::NotOwned
    } else {
        Ownership::Unidentified
    })
}

/// Whether a process that has not exited holds the group id.
pub fn group_has_live_members(pgid: i32) -> io::Result<bool> {
    Ok(!members_of(pgid)?.is_empty())
}

/// Signal the group `pgid` leads. Only for the leader's parent, while it holds the leader
/// unreaped -- running, or exited and not yet collected: until the parent reaps it, the leader's
/// pid, and with it the group's id, names nothing else. A group with nothing left running in it
/// is not an error: Darwin refuses one whose only member is the unreaped leader.
pub fn signal_unreaped_group(pgid: i32, signal: libc::c_int) -> io::Result<()> {
    // SAFETY: killpg takes plain integers and touches no memory of ours.
    if unsafe { libc::killpg(pgid, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(()),
        Some(libc::EPERM) if matches!(group_has_live_members(pgid), Ok(false)) => Ok(()),
        _ => Err(io::Error::new(
            error.kind(),
            format!("cannot signal process group {pgid} with signal {signal}: {error}"),
        )),
    }
}

/// How the caller's own child `pid` ended, read without reaping it; `None` while it runs. The
/// child stays unreaped, so its pid -- and the id of the group it leads -- names nothing else
/// until its parent collects it.
pub fn exit_unreaped(pid: i32) -> io::Result<Option<ExitStatus>> {
    read_exit(pid, libc::WNOHANG)
}

/// Wait for the caller's own child `pid` to end and say how, without reaping it. For a caller
/// told the child is exiting: Darwin reports a process exiting (`NOTE_EXIT`, or `ESRCH` when the
/// exit is watched) before `waitid` can collect it (measured: 17 of 2000 children were not yet
/// waitable right after either), so only a blocking read is sure to find the exit.
pub fn await_exit_unreaped(pid: i32) -> io::Result<ExitStatus> {
    read_exit(pid, 0)?.ok_or_else(|| {
        io::Error::other(format!(
            "waiting for process {pid} returned before it exited"
        ))
    })
}

fn read_exit(pid: i32, options: libc::c_int) -> io::Result<Option<ExitStatus>> {
    let id = libc::id_t::try_from(pid)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    loop {
        // SAFETY: an all-zero `siginfo_t` is valid storage for the call to fill.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: `pid` is the caller's own child and `info` writable storage; WNOWAIT leaves the
        // child unreaped.
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                id,
                &mut info,
                libc::WEXITED | libc::WNOWAIT | options,
            )
        };
        if waited != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        // SAFETY: waitid filled `info` as a child-state record, or left it zeroed.
        let (reporter, status) = unsafe { (info.si_pid(), info.si_status()) };
        if reporter == 0 {
            return Ok(None);
        }
        return match info.si_code {
            libc::CLD_EXITED => Ok(Some(ExitStatus::Exited { code: status })),
            libc::CLD_KILLED => Ok(Some(ExitStatus::Signaled {
                signal: status,
                core_dumped: false,
            })),
            libc::CLD_DUMPED => Ok(Some(ExitStatus::Signaled {
                signal: status,
                core_dumped: true,
            })),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process {pid} reported child state {other}, not an exit"),
            )),
        };
    }
}

/// A process held by an identity the kernel never gives another: its pid and that pid's version,
/// as an audit token carries them. Signalling it reaches that process or nothing.
#[cfg(target_os = "macos")]
pub struct Process {
    pid: i32,
    version: u32,
}

/// `struct proc_uniqidentifierinfo` (`<sys/proc_info.h>`), which the `libc` crate does not
/// declare: `p_uuid[16]`, `p_uniqueid`, `p_puniqueid`, then `p_idversion`, then 20 reserved
/// bytes. Only the pid version is read; the rest is kept as opaque bytes of the same layout.
#[cfg(target_os = "macos")]
#[repr(C, align(8))]
struct UniqueIdentifierInfo {
    _identity: [u8; 32],
    id_version: i32,
    _reserved: [u8; 20],
}

/// `struct proc_bsdinfowithuniqid`: a process's BSD record and its identity, read in one call.
#[cfg(target_os = "macos")]
#[repr(C)]
struct BsdInfoWithUniqueId {
    bsd: libc::proc_bsdinfo,
    unique: UniqueIdentifierInfo,
}

#[cfg(target_os = "macos")]
const PROC_PIDT_BSDINFOWITHUNIQID: libc::c_int = 18;

/// `audit_token_t`.
#[cfg(target_os = "macos")]
#[repr(C)]
struct AuditToken {
    val: [u32; 8],
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    /// Signals the process the token's pid and pid version name, if it still runs. Returns
    /// the error number itself, not -1 (measured: `ESRCH` for a stale version, an exited and a
    /// reaped process; `EPERM` where the sandbox denies signalling).
    fn proc_signal_with_audittoken(token: *mut AuditToken, signal: libc::c_int) -> libc::c_int;
}

/// Every running process that holds the group id, each with the identity it had when read.
#[cfg(target_os = "macos")]
fn members_of(pgid: i32) -> io::Result<Vec<Process>> {
    let mut members = Vec::new();
    for pid in group_pids(pgid)? {
        let mut info = std::mem::MaybeUninit::<BsdInfoWithUniqueId>::zeroed();
        let size = libc::c_int::try_from(std::mem::size_of::<BsdInfoWithUniqueId>())
            .map_err(io::Error::other)?;
        // SAFETY: `info` is writable storage of exactly `size` bytes.
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                PROC_PIDT_BSDINFOWITHUNIQID,
                0,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        if written != size {
            if written <= 0 {
                let error = io::Error::last_os_error();
                // Exited (an unreaped process answers ESRCH too) or gone: nothing to signal.
                if error.raw_os_error() == Some(libc::ESRCH) {
                    continue;
                }
                return Err(error);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("proc_pidinfo returned {written} bytes, expected {size}"),
            ));
        }
        // SAFETY: proc_pidinfo filled all `size` bytes.
        let info = unsafe { info.assume_init() };
        // One call read both, so the group is this very process's: one the id now names a
        // newer process for would have answered with that process's version.
        if i32::try_from(info.bsd.pbi_pgid).ok() == Some(pgid) {
            members.push(Process {
                pid,
                version: info.unique.id_version.cast_unsigned(),
            });
        }
    }
    Ok(members)
}

#[cfg(target_os = "macos")]
impl Process {
    /// The process at the other end of a connected Unix socket, as the kernel recorded it when
    /// the connection was made: whatever happens to its pid later, the handle names only it.
    pub(super) fn of_socket_peer(socket: &impl std::os::fd::AsRawFd) -> io::Result<Self> {
        let mut token = AuditToken { val: [0; 8] };
        let mut length = libc::socklen_t::try_from(std::mem::size_of::<AuditToken>())
            .map_err(io::Error::other)?;
        // SAFETY: `token` is writable storage of `length` bytes for the option's value.
        let read = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_LOCAL,
                libc::LOCAL_PEERTOKEN,
                std::ptr::from_mut(&mut token).cast(),
                &mut length,
            )
        };
        if read != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            pid: token.val[5].cast_signed(),
            version: token.val[7],
        })
    }

    pub(super) fn pid(&self) -> i32 {
        self.pid
    }

    /// Signal the process; `false` when it no longer runs.
    pub(super) fn signal(&self, signal: libc::c_int) -> io::Result<bool> {
        let mut token = AuditToken { val: [0; 8] };
        token.val[5] = self.pid.cast_unsigned();
        token.val[7] = self.version;
        // SAFETY: `token` is a live audit token the call only reads.
        match unsafe { proc_signal_with_audittoken(&mut token, signal) } {
            0 => Ok(true),
            libc::ESRCH => Ok(false),
            error => Err(io::Error::new(
                io::Error::from_raw_os_error(error).kind(),
                format!(
                    "signaling process {} with signal {signal} failed: {}",
                    self.pid,
                    io::Error::from_raw_os_error(error)
                ),
            )),
        }
    }
}

/// The pids holding the group id, in exited processes too.
#[cfg(target_os = "macos")]
fn group_pids(pgid: i32) -> io::Result<Vec<i32>> {
    let mut pids = vec![0; 32];
    loop {
        let bytes =
            i32::try_from(std::mem::size_of_val(pids.as_slice())).map_err(io::Error::other)?;
        // SAFETY: errno is thread-local; libproc writes at most `bytes` into this live slice.
        let count = unsafe {
            *libc::__error() = 0;
            libc::proc_listpgrppids(pgid, pids.as_mut_ptr().cast(), bytes)
        };
        if count <= 0 {
            let error = io::Error::last_os_error();
            return if count == 0 && error.raw_os_error() == Some(0) {
                Ok(Vec::new())
            } else {
                Err(error)
            };
        }
        let count = usize::try_from(count).map_err(io::Error::other)?;
        // A full buffer is never evidence of complete membership.
        if count < pids.len() {
            pids.truncate(count);
            if pids.iter().any(|pid| *pid <= 0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "process group membership contains an invalid pid",
                ));
            }
            return Ok(pids);
        }
        let length = pids.len().checked_mul(2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process group membership overflowed",
            )
        })?;
        pids.resize(length, 0);
    }
}

/// A process held by an identity the kernel never gives another: a pidfd, which names the one
/// process it was opened for. Signalling it reaches that process or nothing.
#[cfg(target_os = "linux")]
pub struct Process {
    pid: i32,
    handle: std::os::fd::OwnedFd,
}

/// Every running process that holds the group id, each held by a pidfd.
#[cfg(target_os = "linux")]
fn members_of(pgid: i32) -> io::Result<Vec<Process>> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};

    let mut members = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let Some(pid) = entry?
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        // SAFETY: pidfd_open takes plain integers; the descriptor is owned from here on.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if opened < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                continue;
            }
            return Err(error);
        }
        let opened = libc::c_int::try_from(opened).map_err(io::Error::other)?;
        // SAFETY: a fresh descriptor this function alone owns.
        let handle = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) };
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        // A pidfd becomes readable when its process exits. Still unreadable after the stat was
        // read, the process it names ran throughout, so the stat was that process's.
        let mut exited = libc::pollfd {
            fd: handle.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one live pollfd entry.
        if unsafe { libc::poll(&mut exited, 1, 0) } < 0 {
            return Err(io::Error::last_os_error());
        }
        if exited.revents != 0 {
            continue;
        }
        let closing = stat.rfind(')').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process stat has no command delimiter",
            )
        })?;
        let mut fields = stat[closing + 1..].split_whitespace();
        let state = fields.next();
        let group = fields.nth(1).and_then(|group| group.parse::<i32>().ok());
        if state != Some("Z") && group == Some(pgid) {
            members.push(Process { pid, handle });
        }
    }
    Ok(members)
}

#[cfg(target_os = "linux")]
impl Process {
    /// The process at the other end of a connected Unix socket, as the kernel recorded it when
    /// the connection was made: whatever happens to its pid later, the handle names only it.
    pub(super) fn of_socket_peer(socket: &impl std::os::fd::AsRawFd) -> io::Result<Self> {
        use std::os::fd::FromRawFd as _;

        let mut opened: libc::c_int = -1;
        let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
            .map_err(io::Error::other)?;
        // SAFETY: `opened` is writable storage of `length` bytes for the option's value.
        if unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERPIDFD,
                std::ptr::from_mut(&mut opened).cast(),
                &mut length,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the kernel handed this process a fresh descriptor it alone owns.
        let handle = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) };
        let mut credentials = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::ucred>())
            .map_err(io::Error::other)?;
        // SAFETY: `credentials` is writable storage of `length` bytes for the option's value.
        if unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::from_mut(&mut credentials).cast(),
                &mut length,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            pid: credentials.pid,
            handle,
        })
    }

    pub(super) fn pid(&self) -> i32 {
        self.pid
    }

    /// Signal the process; `false` when it no longer runs.
    pub(super) fn signal(&self, signal: libc::c_int) -> io::Result<bool> {
        use std::os::fd::AsRawFd as _;

        // SAFETY: pidfd_send_signal takes a live descriptor and plain integers.
        let sent = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.handle.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if sent == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        Err(error)
    }
}

/// The leader's start time, or confirmed absence. An unreadable process is never absent.
#[cfg(target_os = "macos")]
fn start_time(pid: i32) -> io::Result<Option<u64>> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size =
        i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).map_err(io::Error::other)?;
    // SAFETY: `info` is writable storage of exactly `size` bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        if written <= 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("proc_pidinfo returned {written} bytes, expected {size}"),
        ));
    }
    // SAFETY: proc_pidinfo filled all `size` bytes.
    let info = unsafe { info.assume_init() };
    info.pbi_start_tvsec
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(info.pbi_start_tvusec))
        .map(Some)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process start time overflowed"))
}

/// A process's birth time as `proc_pid_rusage` reports it, which -- unlike `proc_pidinfo` --
/// still answers for a process that exited and is not yet reaped. `None` once it is reaped.
#[cfg(target_os = "macos")]
fn birth_time(pid: i32) -> io::Result<Option<u64>> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v0>::zeroed();
    // SAFETY: `info` is writable storage of the size the V0 flavor writes.
    let result =
        unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V0, info.as_mut_ptr().cast()) };
    if result != 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: a successful call filled the V0 record.
    Ok(Some(unsafe { info.assume_init() }.ri_proc_start_abstime))
}

/// A process's birth time: procfs keeps an exited process's stat until it is reaped.
#[cfg(target_os = "linux")]
fn birth_time(pid: i32) -> io::Result<Option<u64>> {
    start_time(pid)
}

/// The leader's start time in ticks since boot, or confirmed absence on a mounted procfs.
#[cfg(target_os = "linux")]
fn start_time(pid: i32) -> io::Result<Option<u64>> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            std::fs::metadata("/proc/self/stat")?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    // The command name is parenthesized and may hold spaces; fields resume after the last ')'.
    let closing = stat.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command delimiter",
        )
    })?;
    let start = stat[closing + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "process stat has no start time")
        })?;
    start
        .parse()
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use super::{GroupLeader, Writer, end_recorded, ledger_path, record, take_lost};

    fn leader(pid: u32) -> GroupLeader {
        GroupLeader::observe(pid).expect("observe a live test group leader")
    }

    /// A job-shaped process tree: a group leader with a child, both sleeping.
    fn job_group() -> std::process::Child {
        Command::new("/bin/sh")
            .args(["-c", "sleep 300 & wait"])
            .stdin(Stdio::null())
            .process_group(0)
            .spawn()
            .expect("spawn a job group")
    }

    #[cfg(target_os = "macos")]
    struct OwnedGroup(std::process::Child);

    #[cfg(target_os = "macos")]
    impl Drop for OwnedGroup {
        fn drop(&mut self) {
            let pgid = i32::try_from(self.0.id()).expect("owned child pid");
            // SAFETY: this unreaped child is the group leader spawned by this test.
            unsafe { libc::killpg(pgid, libc::SIGKILL) };
            let _ = self.0.wait();
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn an_absent_leader_does_not_hide_its_live_group_descendant() {
        use std::io::BufRead as _;

        let child = Command::new("/bin/sh")
            .args(["-c", "(trap '' TERM; echo READY; exec sleep 300) & wait"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn()
            .unwrap();
        let mut group = OwnedGroup(child);
        let pgid = i32::try_from(group.0.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(group.0.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready.trim(), "READY");
        let ledger = scratch();
        record(&ledger, &[(9, leader(group.0.id()))], &[]).unwrap();
        group.0.kill().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while super::start_time(pgid).unwrap().is_some() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(super::start_time(pgid).unwrap().is_none());
        assert!(super::group_has_live_members(pgid).unwrap());
        assert_eq!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO)
                .unwrap()
                .signalled,
            vec![9]
        );
        drop(group);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while super::group_has_live_members(pgid).unwrap() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!super::group_has_live_members(pgid).unwrap());
        assert!(take_lost(&ledger, Duration::ZERO).unwrap().is_empty());
        assert!(!ledger.exists());
    }

    fn group_alive(pgid: i32) -> bool {
        // SAFETY: signal 0 only checks that the group exists.
        unsafe { libc::killpg(pgid, 0) == 0 }
    }

    fn scratch() -> std::path::PathBuf {
        let directory =
            std::env::temp_dir().join(format!("cowshed-groups-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&directory).unwrap();
        ledger_path(&directory.join("s.sock"))
    }

    #[test]
    fn a_lost_supervisor_s_groups_end_whole_and_its_ledger_goes() {
        let mut job = job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let ledger = scratch();
        record(&ledger, &[(7, leader(job.id()))], &[]).unwrap();
        assert!(group_alive(pgid));

        let ended = end_recorded(&ledger, Writer::Any, Duration::from_millis(200)).unwrap();
        assert_eq!(ended.signalled, vec![7]);
        let _ = job.wait();
        std::thread::sleep(Duration::from_millis(100));
        assert!(!group_alive(pgid), "the leader's child died with it");
        assert_eq!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO).unwrap(),
            super::Ended::default(),
            "an ended ledger ends nothing twice"
        );
        assert!(take_lost(&ledger, Duration::ZERO).unwrap().is_empty());
        assert!(!ledger.exists());
    }

    #[test]
    fn a_watcher_of_one_supervisor_never_acts_on_a_newer_one_s_ledger() {
        let mut job = job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let ledger = scratch();
        record(&ledger, &[(3, leader(job.id()))], &[]).unwrap();

        let other_supervisor = std::process::id() + 1;
        let ended =
            end_recorded(&ledger, Writer::Process(other_supervisor), Duration::ZERO).unwrap();
        assert_eq!(ended, super::Ended::default());
        assert!(
            group_alive(pgid),
            "a newer supervisor's running job is left alone"
        );
        assert!(ledger.exists());

        end_recorded(
            &ledger,
            Writer::Process(std::process::id()),
            Duration::from_millis(100),
        )
        .unwrap();
        let _ = job.wait();
    }

    /// A ledger rewrite carries the identity a group's leader had when the job started. Here
    /// the job's pid now names another live group leader -- what a pid reuse looks like from the
    /// ledger -- and recording it again must not adopt that stranger as the job's group, nor may
    /// ending the ledger signal it.
    #[test]
    fn a_rewrite_never_adopts_a_process_that_reused_the_job_s_pid() {
        let mut gone = job_group();
        let first_seen = leader(gone.id());
        let gone_pgid = i32::try_from(gone.id()).unwrap();
        // SAFETY: the unreaped test child leads this group.
        unsafe { libc::killpg(gone_pgid, libc::SIGKILL) };
        let _ = gone.wait();

        let mut stranger = job_group();
        let stranger_pgid = i32::try_from(stranger.id()).unwrap();
        let reused = GroupLeader {
            pgid: stranger_pgid,
            birth: first_seen.birth,
        };
        assert_ne!(reused, leader(stranger.id()));
        let ledger = scratch();
        record(&ledger, &[(5, reused)], &[]).unwrap();
        record(&ledger, &[(5, reused)], &[]).unwrap();

        assert_eq!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO).unwrap(),
            super::Ended::default()
        );
        assert!(take_lost(&ledger, Duration::ZERO).unwrap().is_empty());
        assert!(
            group_alive(stranger_pgid),
            "the stranger's group was signalled"
        );
        // SAFETY: the unreaped test child leads this group.
        unsafe { libc::killpg(stranger_pgid, libc::SIGKILL) };
        let _ = stranger.wait();
    }

    /// A process is signalled only through the identity it was read with: the pid and that pid's
    /// version. The same pid under any other version -- the shape a reused pid takes -- is never
    /// reached, and neither is the process once it has exited and been reaped.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_signal_reaches_only_the_process_its_identity_names() {
        let mut job = job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let members = super::members_of(pgid).unwrap();
        let leader = members
            .iter()
            .find(|member| member.pid() == pgid)
            .expect("the running leader holds its group id");
        let other_incarnation = super::Process {
            pid: leader.pid,
            version: leader.version.wrapping_add(1),
        };
        assert!(!other_incarnation.signal(libc::SIGKILL).unwrap());
        assert!(
            group_alive(pgid),
            "a process under another version was signalled"
        );
        assert!(leader.signal(libc::SIGKILL).unwrap());
        job.wait().unwrap();
        assert!(
            !leader.signal(libc::SIGKILL).unwrap(),
            "a reaped process was signalled"
        );
        // SAFETY: the test's own group; its backgrounded sleep may still hold the id.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
    }

    /// Once a group's leader is reaped, the processes holding its id cannot be told from a
    /// stranger's group that took the id after the job's group ended: here a group whose leader
    /// exited and was reaped, leaving a descendant that holds the id. Whatever identity the ledger
    /// recorded -- this build's birth time, an earlier build's start time, or none -- recovery
    /// never signals it. The next supervisor takes it over as unresolved and every later ledger
    /// carries it until nothing holds the id; only then is its record dropped.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_group_whose_recorded_leader_was_reaped_is_carried_never_signalled() {
        use std::io::BufRead as _;

        type Recorded = fn(i32) -> (Option<u64>, Option<u64>);
        let identities: [(&str, Recorded); 3] = [
            ("birth", |pgid| (super::birth_time(pgid).unwrap(), None)),
            ("legacy start", |pgid| {
                (None, super::start_time(pgid).unwrap())
            }),
            ("none", |_| (None, None)),
        ];
        for (label, recorded) in identities {
            let child = Command::new("/bin/sh")
                .args(["-c", "(trap '' TERM; echo READY; exec sleep 300) & wait"])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .process_group(0)
                .spawn()
                .unwrap();
            let mut group = OwnedGroup(child);
            let pgid = i32::try_from(group.0.id()).unwrap();
            let mut ready = String::new();
            std::io::BufReader::new(group.0.stdout.take().unwrap())
                .read_line(&mut ready)
                .unwrap();
            assert_eq!(ready.trim(), "READY");
            let (leader_birth, leader_start) = recorded(pgid);
            assert_eq!(
                (leader_birth.is_some(), leader_start.is_some()),
                (label == "birth", label == "legacy start"),
                "{label}: recorded while the leader ran"
            );
            group.0.kill().unwrap();
            group.0.wait().unwrap();
            assert_eq!(super::birth_time(pgid).unwrap(), None, "{label}");
            assert!(super::group_has_live_members(pgid).unwrap(), "{label}");

            let ledger = scratch();
            let lost_supervisor = std::process::id() + 1;
            super::replace(
                &ledger,
                &super::Ledger {
                    supervisor: lost_supervisor,
                    ended: false,
                    groups: vec![super::Group {
                        job_id: 4,
                        pgid,
                        leader_birth,
                        leader_start,
                    }],
                    unresolved: Vec::new(),
                },
            )
            .unwrap();
            assert_eq!(
                end_recorded(&ledger, Writer::Any, Duration::ZERO).unwrap(),
                super::Ended {
                    signalled: Vec::new(),
                    unresolved: vec![4],
                },
                "{label}"
            );
            let taken = take_lost(&ledger, Duration::ZERO).unwrap();
            assert_eq!(
                taken
                    .iter()
                    .map(|group| (group.job_id(), group.pgid(), group.lost_supervisor()))
                    .collect::<Vec<_>>(),
                vec![(4, pgid, lost_supervisor)],
                "{label}"
            );
            assert!(group_alive(pgid), "{label}: the group was signalled");
            // The next supervisor carries it in every ledger it writes, and a later recovery
            // takes it over again rather than forgetting it.
            assert_eq!(super::unresolved(&ledger).unwrap(), taken, "{label}");
            assert_eq!(
                record(&ledger, &[], &taken).unwrap().carried,
                taken,
                "{label}"
            );
            assert_eq!(
                take_lost(&ledger, Duration::ZERO).unwrap(),
                taken,
                "{label}"
            );
            assert!(group_alive(pgid), "{label}: the group was signalled");

            drop(group);
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while super::group_has_live_members(pgid).unwrap()
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(super::unresolved(&ledger).unwrap().is_empty(), "{label}");
            assert!(
                record(&ledger, &[], &taken).unwrap().carried.is_empty(),
                "{label}"
            );
            assert!(
                take_lost(&ledger, Duration::ZERO).unwrap().is_empty(),
                "{label}"
            );
            assert!(!ledger.exists(), "{label}");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn permission_denials_preserve_the_owned_ledger_and_allow_retry() {
        const LEDGER_ENV: &str = "COWSHED_TEST_GROUP_LEDGER";
        if let Some(path) = std::env::var_os(LEDGER_ENV) {
            let path = std::path::PathBuf::from(path);
            let error = end_recorded(&path, Writer::Any, Duration::ZERO)
                .expect_err("denied group authority cannot mark the ledger ended");
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            let error = take_lost(&path, Duration::ZERO)
                .expect_err("denied group authority cannot remove the ledger");
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            return;
        }

        for (profile, previously_ended) in [
            ("(version 1)(allow default)(deny signal)", false),
            (
                "(version 1)(allow default)(deny process-info*)(deny signal)",
                false,
            ),
            ("(version 1)(allow default)(deny signal)", true),
        ] {
            let group = OwnedGroup(job_group());
            let pgid = i32::try_from(group.0.id()).unwrap();
            let ledger = scratch();
            record(&ledger, &[(7, leader(group.0.id()))], &[]).unwrap();
            if previously_ended {
                let mut recorded = super::read(&ledger).unwrap().unwrap();
                recorded.ended = true;
                super::replace(&ledger, &recorded).unwrap();
            }
            let before = std::fs::read(&ledger).unwrap();
            let output = Command::new("/usr/bin/sandbox-exec")
                .args(["-p", profile, "--"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::job_groups::tests::permission_denials_preserve_the_owned_ledger_and_allow_retry",
                    "--nocapture",
                ])
                .env(LEDGER_ENV, &ledger)
                .output()
                .expect("real restricted cleanup process");
            let after = std::fs::read(&ledger).unwrap();
            let survived_refusal = group_alive(pgid);
            let retried = end_recorded(&ledger, Writer::Any, Duration::ZERO);
            let taken = take_lost(&ledger, Duration::ZERO);
            drop(group);

            assert!(
                output.status.success(),
                "restricted cleanup: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(after, before, "failed cleanup retains the original ledger");
            assert!(survived_refusal, "the refused group remained alive");
            assert_eq!(
                retried.unwrap().signalled,
                vec![7],
                "host authority can retry cleanup"
            );
            taken.unwrap();
            assert!(!ledger.exists(), "successful retry consumes the ledger");
        }
    }
}
