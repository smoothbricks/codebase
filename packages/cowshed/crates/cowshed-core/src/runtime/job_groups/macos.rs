//! macOS's process identity for job groups (11_shell.md "Process identity and signal ownership").
//!
//! - A [`Process`] is a pid and that pid's version, as an audit token carries them:
//!   `proc_signal_with_audittoken` reaches that process or nothing, and an exec gives the pid a
//!   new version.
//! - A life's token ([`Life`], [`ProcessRecord::unique_id`]) is `p_uniqueid`
//!   (`PROC_PIDUNIQIDENTIFIERINFO`), which the kernel never gives another process while the
//!   system runs and an exec keeps.
//! - A leader's start is `KERN_PROC_PID`'s `p_starttime`, answered for every process whoever
//!   owns it; an earlier build's birth is `proc_pid_rusage`'s `ri_proc_start_abstime`.
//! - A group's members are `proc_listpgrppids`'s, each read inside its own identity fence.
//! - A socket peer is `LOCAL_PEERTOKEN`'s audit token; the boot is `kern.bootsessionuuid`.

use super::*;

/// The boot session recorded lives are fenced by: `kern.bootsessionuuid`.
pub(super) fn boot_session() -> io::Result<String> {
    Ok(cowshed_apfs::attach_budget::boot()?.session)
}

/// The record of every running process that holds the group id, as the kernel names each.
pub(super) fn member_records(pgid: i32) -> io::Result<Vec<ProcessRecord>> {
    Ok(running_in_group(pgid)?
        .into_iter()
        .map(|life| life.record)
        .collect())
}

pub(super) fn recorded_life(life: ProcessIdentity) -> io::Result<Option<Process>> {
    let pid = i32::try_from(life.pid).map_err(io::Error::other)?;
    if pid <= 0 || life.birth.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "recorded process life needs a positive pid and unique id",
        ));
    }
    Ok(running_identity(pid)?
        .filter(|identity| identity.unique_id == life.birth.0)
        .map(|identity| Process {
            pid,
            version: identity.id_version.cast_unsigned(),
            unique_id: Some(identity.unique_id),
        }))
}

pub(super) fn end_recorded_members(groups: &[Group], grace: Duration) -> io::Result<Vec<u64>> {
    if groups.iter().all(|group| group.members.is_none()) {
        return Ok(Vec::new());
    }
    let boot = cowshed_apfs::attach_budget::boot()?.session;
    let mut watched = Vec::new();
    let mut signalled = BTreeSet::new();
    for group in groups {
        let Some(members) = group
            .members
            .as_ref()
            .filter(|members| members.boot_session == boot)
        else {
            continue;
        };
        for &life in &members.lives {
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while let Some(process) = recorded_life(life)? {
                match process.watch_exit() {
                    Ok(exit) => {
                        watched.push((group.job_id, life, exit, false));
                        break;
                    }
                    Err(error) => {
                        let current = recorded_life(life)?;
                        match current {
                            None => break,
                            Some(now)
                                if now.version != process.version
                                    && std::time::Instant::now() < deadline =>
                            {
                                continue;
                            }
                            Some(_) => return Err(error),
                        }
                    }
                }
            }
        }
    }
    for (job, life, _, _) in &watched {
        if signal_life(life.pid, life.birth.0, libc::SIGTERM)? {
            signalled.insert(*job);
        }
    }
    let deadline = std::time::Instant::now() + grace;
    for (_, _, exit, ended) in &mut watched {
        *ended = exit.within(deadline.saturating_duration_since(std::time::Instant::now()))?;
    }
    for (job, life, _, ended) in &watched {
        if !ended && signal_life(life.pid, life.birth.0, libc::SIGKILL)? {
            signalled.insert(*job);
        }
    }
    let deadline = std::time::Instant::now() + grace.max(Duration::from_secs(1));
    for (_, life, exit, ended) in watched {
        if !ended && !exit.within(deadline.saturating_duration_since(std::time::Instant::now()))? {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "recorded process {} (unique id {}) has no exit proof after SIGKILL; its ledger claim is retained",
                    life.pid, life.birth.0
                ),
            ));
        }
    }
    Ok(signalled.into_iter().collect())
}

/// A process held by an identity the kernel never gives another: its pid and that pid's version,
/// as an audit token carries them, plus its immutable life token (`p_uniqueid`) when read from
/// libproc. Signalling it reaches that process or nothing; census reads fence by `unique_id` so
/// an exec of the same life stays one life while any later process given the pid is rejected.
#[derive(Debug)]
pub struct Process {
    pid: i32,
    version: u32,
    unique_id: Option<u64>,
}

/// What a signal `proc_signal_with_audittoken` refused with `EPERM` means ([`Process::signal`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PermissionRefusal {
    /// No process with the signalled identity runs: the token named a life that has ended.
    Gone,
    /// The process with the signalled identity runs and the kernel denied the signal.
    Denied,
}

/// [`PermissionRefusal`] from `running`, the pid version of the life the pid holds as read after
/// the refusal. The kernel refuses an audit token with `EPERM` not only when it denies the signal
/// but also when the token names no running process (its process exited, or its pid now holds
/// another life), so the refusal is a denial only while the life with `version` still runs. A read
/// that fails proves nothing gone: it stays a denial.
fn permission_refusal(version: u32, running: Result<Option<u32>, &io::Error>) -> PermissionRefusal {
    match running {
        Ok(Some(running)) if running == version => PermissionRefusal::Denied,
        Ok(_) => PermissionRefusal::Gone,
        Err(_) => PermissionRefusal::Denied,
    }
}

/// An observation after a denied signal, never an identity renewed for a retry.
#[derive(Debug, thiserror::Error)]
#[error(
    "signaling process {pid} with signal {signal} failed: {source}; expected_version={expected_version}, caller_euid={caller_euid}, post_failure(uid,flags,version)={post_failure:?}, exit_watch=unavailable (none held at the signal boundary)"
)]
struct SignalPermissionDenied {
    pid: i32,
    signal: libc::c_int,
    expected_version: u32,
    caller_euid: u32,
    post_failure: io::Result<Option<(u32, u32, u32)>>,
    source: io::Error,
}

/// `struct proc_uniqidentifierinfo` (`<sys/proc_info.h>`), which the `libc` crate does not
/// declare: `p_uuid[16]`, `p_uniqueid`, `p_puniqueid`, then `p_idversion`, then 20 reserved
/// bytes. The unique ids are never given to another process while the system runs.
#[repr(C, align(8))]
struct UniqueIdentifierInfo {
    _uuid: [u8; 16],
    unique_id: u64,
    parent_unique_id: u64,
    id_version: i32,
    _reserved: [u8; 20],
}

/// `PROC_PIDUNIQIDENTIFIERINFO`: a life's unique ids and pid version. The kernel answers it,
/// and `PROC_PIDT_SHORTBSDINFO`, for a process of any effective uid. It refuses
/// `PROC_PIDT_BSDINFOWITHUNIQID` with `EPERM` for a process whose effective uid is not the
/// caller's: a job member running a setuid-root program, such as the `/bin/ps` Nx runs to find a
/// task's processes when it is told to stop, whose real uid is still the job's. Measured on Darwin
/// 25.6 with `/usr/bin/top` (euid 0, ruid ours): flavor 18 answers `EPERM`, flavors 17 and 13
/// answer, and `proc_signal_with_audittoken` delivers STOP, CONT and TERM to it. An exited
/// process, reaped or not, answers `ESRCH` to all three.
const PROC_PIDUNIQIDENTIFIERINFO: libc::c_int = 17;

/// One running life as [`running_life`] reads it: its record, the pid version it carried at the
/// read's end, and the group and BSD flags it held within the read.
struct RunningLife {
    record: ProcessRecord,
    version: u32,
    pgid: u32,
    flags: u32,
}

/// `audit_token_t`.
#[repr(C)]
struct AuditToken {
    val: [u32; 8],
}

unsafe extern "C" {
    /// Signals the process the token's pid and pid version name, if it still runs. Returns
    /// the error number itself, not -1 (measured: `ESRCH` for a stale version, an exited and a
    /// reaped process; `EPERM` where the sandbox denies signalling; `EINVAL` for signal 0, so it
    /// cannot probe whether a process runs).
    fn proc_signal_with_audittoken(token: *mut AuditToken, signal: libc::c_int) -> libc::c_int;
}

/// Every running process that holds the group id, each with the identity it had when read.
pub(super) fn members_of(pgid: i32) -> io::Result<Vec<Process>> {
    Ok(running_in_group(pgid)?
        .into_iter()
        .map(|life| Process {
            pid: life.record.pid.cast_signed(),
            version: life.version,
            unique_id: Some(life.record.unique_id),
        })
        .collect())
}

/// The record of every running process that holds the group id.
fn running_in_group(pgid: i32) -> io::Result<Vec<RunningLife>> {
    let mut members = Vec::new();
    for pid in procinfo::group_pids(pgid)? {
        // Exited (an unreaped process answers ESRCH too) or gone: nothing to signal.
        let Some(life) = running_life(pid)? else {
            continue;
        };
        // The group was read inside the life's identity fence, so it is this very process's:
        // a newer process given the id would have answered with another unique id.
        if i32::try_from(life.pgid).ok() == Some(pgid) {
            members.push(life);
        }
    }
    Ok(members)
}

/// Signal the life `pid` holds while the kernel's unique id for it is `unique_id`; `false` once
/// that life no longer runs. Its pid version is read in the same call as that id, so a signal
/// reaches that life or nothing: a later process given the pid has another id. An exec gives the
/// life a new version and turns the signal away (`ESRCH`); the life is then read again by its
/// id, never renewed from the pid alone. A refusal under the version just read means the life
/// is exiting.
pub(crate) fn signal_life(pid: u32, unique_id: u64, signal: libc::c_int) -> io::Result<bool> {
    let pid = i32::try_from(pid).map_err(io::Error::other)?;
    let mut refused = None;
    loop {
        match signal_step(running_identity(pid), unique_id, refused)? {
            SignalStep::Gone => return Ok(false),
            SignalStep::Send(version) => {
                if (Process {
                    pid,
                    version,
                    unique_id: Some(unique_id),
                })
                .signal(signal)?
                {
                    return Ok(true);
                }
                refused = Some(version);
            }
        }
    }
}

/// One step of signalling a life by its unique id, decided from the identity just read and the pid
/// version a signal was last refused under ([`signal_life`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalStep {
    /// Signal the life under this pid version.
    Send(u32),
    /// Stop: the life no longer runs (no life, or another one, holds the pid), or a signal was
    /// already refused under this very version, so the life is exiting.
    Gone,
}

/// [`SignalStep`] from `identity`, what flavor 17 answered for the pid. That flavor answers for a
/// process of any effective uid, a member running a setuid-root program included, so its read is
/// the retirement's one proof. A read that fails, `EPERM` included, is the step's error: a life
/// that cannot be read is never taken as gone, and its retirement says so instead of ending.
fn signal_step(
    identity: io::Result<Option<UniqueIdentifierInfo>>,
    unique_id: u64,
    refused: Option<u32>,
) -> io::Result<SignalStep> {
    let Some(identity) = identity?.filter(|identity| identity.unique_id == unique_id) else {
        return Ok(SignalStep::Gone);
    };
    let version = identity.id_version.cast_unsigned();
    Ok(if refused == Some(version) {
        SignalStep::Gone
    } else {
        SignalStep::Send(version)
    })
}

/// The life one pid holds, decided from its identity reads in their order: its unique ids
/// (`PROC_PIDUNIQIDENTIFIERINFO`), its short BSD info (`PROC_PIDT_SHORTBSDINFO`), its start time
/// (`KERN_PROC_PID`), and its unique ids again. These are the reads the kernel answers for a
/// process of any effective uid, so a member running a setuid-root program reads like any other;
/// `PROC_PIDT_BSDINFOWITHUNIQID` and `proc_pid_rusage`, which decline such a process with `EPERM`,
/// prove nothing here. The first read that finds no running life (`ESRCH`, `None`) answers
/// `None`; the first that fails otherwise, `EPERM` included, is the answer: an unreadable process
/// is never an absent one. Unique ids that changed between the first and the last read are
/// another life, so the pid holds none of this one.
fn decide_life(
    before: io::Result<Option<UniqueIdentifierInfo>>,
    bsd: io::Result<Option<libc::proc_bsdshortinfo>>,
    started: io::Result<Option<u64>>,
    after: io::Result<Option<UniqueIdentifierInfo>>,
) -> io::Result<Option<RunningLife>> {
    let Some(before) = before? else {
        return Ok(None);
    };
    let Some(bsd) = bsd? else {
        return Ok(None);
    };
    let Some(started) = started? else {
        return Ok(None);
    };
    let Some(after) = after? else {
        return Ok(None);
    };
    if before.unique_id != after.unique_id {
        return Ok(None);
    }
    Ok(Some(RunningLife {
        record: ProcessRecord {
            pid: bsd.pbsi_pid,
            ppid: bsd.pbsi_ppid,
            unique_id: after.unique_id,
            parent_unique_id: after.parent_unique_id,
            // xnu `proc_pidshortbsdinfo`: `pbsi_uid` is the credential's effective uid.
            uid: bsd.pbsi_uid,
            started_seconds: started / 1_000_000,
        },
        version: after.id_version.cast_unsigned(),
        pgid: bsd.pbsi_pgid,
        flags: bsd.pbsi_flags,
    }))
}

/// The identity of the life `pid` holds now; `None` once it has exited, reaped or not.
fn running_identity(pid: i32) -> io::Result<Option<UniqueIdentifierInfo>> {
    // SAFETY: flavor 17 writes exactly struct proc_uniqidentifierinfo.
    unsafe { pid_info(pid, PROC_PIDUNIQIDENTIFIERINFO) }
}

/// The public metadata of the life `pid` holds now, fenced by its immutable unique id; `None` once
/// it has exited, reaped or not. BSD credentials, ppid and group are a snapshot of that life, not
/// an atomic image claim or signal authority. An exec may change the pid version without changing
/// the birth or ancestry. No failed call is classified as absence.
fn running_life(pid: i32) -> io::Result<Option<RunningLife>> {
    let before = running_identity(pid);
    // SAFETY: SHORTBSDINFO writes exactly libc's struct proc_bsdshortinfo.
    let bsd = unsafe { pid_info::<libc::proc_bsdshortinfo>(pid, libc::PROC_PIDT_SHORTBSDINFO) };
    let started = start_time(pid);
    let after = running_identity(pid);
    decide_life(before, bsd, started, after)
}

/// Read one exact libproc payload; only ESRCH proves that no running life holds the pid.
///
/// # Safety
/// T is the C payload structure for flavor, with no invalid initialized bit patterns.
unsafe fn pid_info<T>(pid: i32, flavor: libc::c_int) -> io::Result<Option<T>> {
    let mut info = std::mem::MaybeUninit::<T>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<T>()).map_err(io::Error::other)?;
    // SAFETY: the caller supplies the exact payload type, writable for size bytes.
    let written = unsafe { libc::proc_pidinfo(pid, flavor, 0, info.as_mut_ptr().cast(), size) };
    if written != size {
        if written <= 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(None);
            }
            return Err(error);
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "proc_pidinfo flavor {flavor} for process {pid} returned {written} bytes, expected {size}"
            ),
        ));
    }
    // SAFETY: proc_pidinfo filled the exact payload, whose initialized bits the caller admits.
    Ok(Some(unsafe { info.assume_init() }))
}

/// The public metadata of the life `pid` holds now ([`running_life`]).
pub(crate) fn process_record(pid: i32) -> io::Result<Option<ProcessRecord>> {
    Ok(running_life(pid)?.map(|life| life.record))
}

/// The kernel's identity of the life `pid` names now: its unique id (`p_uniqueid`), which the
/// kernel never gives another process. `None` once it has exited, reaped or not.
pub(crate) fn life_of(pid: i32) -> io::Result<Option<u64>> {
    Ok(process_record(pid)?.map(|record| record.unique_id))
}

impl Process {
    /// This process's record, read now; `None` once it has exited, or once its pid names a later
    /// process, whose record is never this one's: the pid's running process must still carry this
    /// process's version.
    pub(crate) fn record(&self) -> io::Result<Option<ProcessRecord>> {
        Ok(running_life(self.pid)?
            .filter(|life| life.version == self.version)
            .map(|life| life.record))
    }

    /// This process's record and current pid version, read now through its immutable life
    /// identity (`p_uniqueid`) when known -- surviving an exec of the same life while rejecting
    /// any later process given the pid -- or through its pid version when only an audit token
    /// identified it. `None` once the life has exited or its pid names another.
    pub(crate) fn life_record(&self) -> io::Result<Option<(ProcessRecord, u32)>> {
        Ok(running_life(self.pid)?
            .filter(|life| match self.unique_id {
                Some(unique_id) => life.record.unique_id == unique_id,
                None => life.version == self.version,
            })
            .map(|life| (life.record, life.version)))
    }

    /// The process at the other end of a connected Unix socket, as the kernel recorded it when
    /// the connection was made: whatever happens to its pid later, the handle names only it.
    pub fn of_socket_peer(socket: &impl std::os::fd::AsRawFd) -> io::Result<Self> {
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
            unique_id: None,
        })
    }

    /// The process `pid` names now, held by the identity its kernel record carries; `None` once it
    /// has exited, reaped or not. A pid below one names no process, so it is refused, not read.
    pub fn observe(pid: i32) -> io::Result<Option<Self>> {
        if pid < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pid {pid} names no process"),
            ));
        }
        Ok(running_identity(pid)?.map(|identity| Self {
            pid,
            version: identity.id_version.cast_unsigned(),
            unique_id: Some(identity.unique_id),
        }))
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Whether this very life still runs: its pid's running process carries its pid version, which
    /// no later process given the pid has.
    pub(crate) fn running(&self) -> io::Result<bool> {
        Ok(self.record()?.is_some())
    }

    /// Signal the process; `false` when it no longer runs.
    pub fn signal(&self, signal: libc::c_int) -> io::Result<bool> {
        let mut token = AuditToken { val: [0; 8] };
        token.val[5] = self.pid.cast_unsigned();
        token.val[7] = self.version;
        // SAFETY: `token` is a live audit token the call only reads.
        match unsafe { proc_signal_with_audittoken(&mut token, signal) } {
            0 => Ok(true),
            libc::ESRCH => Ok(false),
            libc::EPERM => {
                let after = running_life(self.pid)
                    .map(|life| life.map(|life| (life.record.uid, life.flags, life.version)));
                let running = after
                    .as_ref()
                    .map(|life| life.map(|(_, _, version)| version));
                match permission_refusal(self.version, running) {
                    PermissionRefusal::Gone => Ok(false),
                    PermissionRefusal::Denied => Err(self.signal_permission_denied(signal, after)),
                }
            }
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

    /// The denial, with the facts read after the refusal (`post_failure`) and the caller's uid.
    #[cold]
    fn signal_permission_denied(
        &self,
        signal: libc::c_int,
        post_failure: io::Result<Option<(u32, u32, u32)>>,
    ) -> io::Error {
        // SAFETY: geteuid takes no pointers and only reads the caller's credential.
        let caller_euid = unsafe { libc::geteuid() };
        let source = io::Error::from_raw_os_error(libc::EPERM);
        io::Error::new(
            source.kind(),
            SignalPermissionDenied {
                pid: self.pid,
                signal,
                expected_version: self.version,
                caller_euid,
                post_failure,
                source,
            },
        )
    }

    /// Watch for this process's exit, from now. A watch registered by pid once the process has
    /// begun to exit is refused (`ESRCH`) while its descriptors may still be open, and one made
    /// after it is gone could name another process: so it must be watched while it runs. The
    /// pid's running process read after the registration carries this process's pid version,
    /// which no other process is given: it ran throughout, and the watch names it.
    pub fn watch_exit(&self) -> io::Result<ExitWatch> {
        let watch = ExitWatch::register(self.pid).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "process {} cannot be watched to its exit, which may have begun: {error}",
                    self.pid
                ),
            )
        })?;
        match running_identity(self.pid)? {
            Some(identity) if identity.id_version.cast_unsigned() == self.version => Ok(watch),
            Some(_) => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "process {} exited before it could be watched; its pid names another",
                    self.pid
                ),
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("process {} exited before it could be watched", self.pid),
            )),
        }
    }

    /// The bytes this process holds resident now; `None` once it has exited. The read names
    /// only the pid, so it counts only if the pid's running process still carries this
    /// process's version afterwards: then this process ran throughout and the size read was its
    /// own, not that of a later process given the pid. A read that failed for a process gone
    /// since is its exit, not an error.
    pub(in crate::runtime) fn resident_bytes(&self) -> io::Result<Option<u64>> {
        let read = procinfo::resident_bytes(self.pid);
        match running_identity(self.pid)? {
            Some(identity) if identity.id_version.cast_unsigned() == self.version => read,
            Some(_) | None => Ok(None),
        }
    }

    /// The ports of the TCP sockets this process listens on now; `None` once it has exited. The
    /// read names only the pid, so this process's identity, read afterwards, judges it: still
    /// carrying this process's version, it ran throughout and the read -- its answer or why it
    /// failed -- was its own; gone, or another process's now, it holds nothing, whatever the
    /// read met.
    pub(in crate::runtime) fn listening_ports(&self) -> io::Result<Option<BTreeSet<u16>>> {
        let read = procinfo::listening_ports(self.pid);
        match running_identity(self.pid)? {
            Some(identity) if identity.id_version.cast_unsigned() == self.version => read.map(Some),
            Some(_) | None => Ok(None),
        }
    }
}

impl Life {
    /// The process this life is now, in one read of its pid; `None` once it has exited, reaped
    /// or not, since no other process is given its unique id.
    pub(crate) fn current(self) -> io::Result<Option<Process>> {
        Ok(running_identity(self.pid)?
            .filter(|identity| identity.unique_id == self.unique_id)
            .map(|identity| Process {
                pid: self.pid,
                version: identity.id_version.cast_unsigned(),
                unique_id: Some(identity.unique_id),
            }))
    }

    /// The life `pid` names now; `None` once it has exited, reaped or not.
    pub fn of(pid: i32) -> io::Result<Option<Self>> {
        Ok(running_identity(pid)?.map(|identity| Self {
            pid,
            unique_id: identity.unique_id,
        }))
    }

    /// Its exit, watched from now through any exec; `Before` once it has exited: a later process
    /// given its pid is never taken for it ([`Process::watch_life`]).
    pub fn exit(&self) -> io::Result<ProcessExit> {
        Ok(self
            .watch_exit()?
            .map_or(ProcessExit::Before, ProcessExit::Watching))
    }

    /// Watch this life's exit from now, through any exec; `None` once it has exited. The pid's
    /// running process read after the registration still carries this life's unique id: it ran
    /// throughout, and the watch names it.
    pub(crate) fn watch_exit(self) -> io::Result<Option<ExitWatch>> {
        let watch = match ExitWatch::register(self.pid) {
            Ok(watch) => watch,
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(self.current()?.map(|_| watch))
    }
}

impl Process {
    /// Whether `other` is this very life: the same pid at the same pid version, which no later
    /// process given the pid carries.
    pub(crate) fn same_life(&self, other: &Self) -> io::Result<bool> {
        Ok(self.pid == other.pid && self.version == other.version)
    }

    /// Watch for this process's exit from now; `None` once it has exited or begun to: the kernel
    /// refuses a new watch of an exiting process (`ESRCH`), and a pid whose running process no
    /// longer carries this version names another.
    pub(super) fn watch_running(&self) -> io::Result<Option<ExitWatch>> {
        let watch = match ExitWatch::register(self.pid) {
            Ok(watch) => watch,
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(self.running()?.then_some(watch))
    }
}

/// `struct kinfo_proc` (`<sys/sysctl.h>`), which the `libc` crate does not declare. Only its
/// leading field is read: `kp_proc.p_un.__p_starttime`, the `p_start` the kernel stamps at fork
/// and `proc_pidinfo` reports as `pbi_start`.
#[repr(C)]
struct KinfoProc {
    start: libc::timeval,
    _rest: [u8; 632],
}

const _: () = assert!(std::mem::size_of::<KinfoProc>() == 648);

/// A process's start time: the calendar time its fork stamped, in microseconds, or confirmed
/// absence. `KERN_PROC_PID` answers it for every process whoever owns it -- unlike
/// `proc_pidinfo` and `proc_pid_rusage`, which decline another user's process with `EPERM` --
/// and for one that exited and is not yet reaped; `None` once nothing holds the pid. An
/// unreadable process is never absent.
pub fn start_time(pid: i32) -> io::Result<Option<u64>> {
    let mut name = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, pid];
    let mut info = std::mem::MaybeUninit::<KinfoProc>::zeroed();
    let mut size = std::mem::size_of::<KinfoProc>();
    // SAFETY: `name` holds the four MIB entries passed, and `info` is writable storage of
    // `size` bytes.
    let read = unsafe {
        libc::sysctl(
            name.as_mut_ptr(),
            4,
            info.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if read != 0 {
        return Err(io::Error::last_os_error());
    }
    if size == 0 {
        return Ok(None);
    }
    if size != std::mem::size_of::<KinfoProc>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "KERN_PROC_PID returned {size} bytes, expected {}",
                std::mem::size_of::<KinfoProc>()
            ),
        ));
    }
    // SAFETY: sysctl filled all `size` bytes.
    let start = unsafe { info.assume_init() }.start;
    let invalid = |error| io::Error::new(io::ErrorKind::InvalidData, error);
    let seconds = u64::try_from(start.tv_sec).map_err(invalid)?;
    let micros = u64::try_from(start.tv_usec).map_err(invalid)?;
    seconds
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(micros))
        .map(Some)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process start time overflowed"))
}

/// A process's birth time as `proc_pid_rusage` reports it, which -- unlike `proc_pidinfo` --
/// still answers for a process that exited and is not yet reaped. `None` once it is reaped.
pub(super) fn birth_time(pid: i32) -> io::Result<Option<u64>> {
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

#[cfg(test)]
mod tests {
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};

    use super::super::tests::{
        OwnedGroup, group_alive, group_with_child, job_group, leader, scratch,
    };
    use super::{Writer, end_recorded, take_lost};

    /// What `PROC_PIDUNIQIDENTIFIERINFO` answers for a life: its unique id and pid version.
    fn identity(unique_id: u64, version: i32) -> super::UniqueIdentifierInfo {
        super::UniqueIdentifierInfo {
            _uuid: [0; 16],
            unique_id,
            parent_unique_id: 1,
            id_version: version,
            _reserved: [0; 20],
        }
    }

    /// What `PROC_PIDT_SHORTBSDINFO` answers for a group leader of effective uid `uid`.
    fn short_bsd(uid: u32) -> libc::proc_bsdshortinfo {
        // SAFETY: proc_bsdshortinfo is plain C data; all-zero is a valid value.
        let mut bsd: libc::proc_bsdshortinfo = unsafe { std::mem::zeroed() };
        bsd.pbsi_pid = 42;
        bsd.pbsi_ppid = 1;
        bsd.pbsi_pgid = 42;
        bsd.pbsi_uid = uid;
        bsd
    }

    fn errno<T>(code: libc::c_int) -> std::io::Result<T> {
        Err(std::io::Error::from_raw_os_error(code))
    }

    /// Which reads prove a member's life, and what an `EPERM` from one means: a process of root's
    /// effective uid (a member running a setuid-root program) reads like any other from the
    /// flavors that answer every uid; the first read that finds no life answers absent; the first
    /// that fails otherwise, `EPERM` included, is unreadable, never absent; a unique id that
    /// changed between the fence's two reads is another life.
    #[test]
    fn a_life_is_decided_from_the_reads_every_effective_uid_answers() {
        let life = super::decide_life(
            Ok(Some(identity(7, 3))),
            Ok(Some(short_bsd(0))),
            Ok(Some(5_000_000)),
            Ok(Some(identity(7, 3))),
        )
        .expect("every read answered")
        .expect("one life held the pid throughout");
        assert_eq!(
            (
                life.record.uid,
                life.record.unique_id,
                life.version,
                life.pgid,
                life.record.started_seconds
            ),
            (0, 7, 3, 42, 5)
        );
        type Reads = (
            std::io::Result<Option<super::UniqueIdentifierInfo>>,
            std::io::Result<Option<libc::proc_bsdshortinfo>>,
            std::io::Result<Option<u64>>,
            std::io::Result<Option<super::UniqueIdentifierInfo>>,
        );
        type Case = (&'static str, fn() -> Reads, Result<bool, libc::c_int>);
        let cases: [Case; 7] = [
            (
                "gone at the first read",
                || (Ok(None), Ok(Some(short_bsd(0))), Ok(Some(1)), Ok(None)),
                Ok(false),
            ),
            (
                "gone first, declined after: the first answer stands",
                || (Ok(None), errno(libc::EPERM), Ok(Some(1)), Ok(None)),
                Ok(false),
            ),
            (
                "its unique ids declined",
                || {
                    (
                        errno(libc::EPERM),
                        Ok(Some(short_bsd(0))),
                        Ok(Some(1)),
                        Ok(Some(identity(7, 3))),
                    )
                },
                Err(libc::EPERM),
            ),
            (
                "its BSD info declined",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        errno(libc::EPERM),
                        Ok(Some(1)),
                        Ok(Some(identity(7, 3))),
                    )
                },
                Err(libc::EPERM),
            ),
            (
                "its start time declined",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        Ok(Some(short_bsd(0))),
                        errno(libc::EPERM),
                        Ok(Some(identity(7, 3))),
                    )
                },
                Err(libc::EPERM),
            ),
            (
                "exited before the fence closed",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        Ok(Some(short_bsd(0))),
                        Ok(Some(1)),
                        Ok(None),
                    )
                },
                Ok(false),
            ),
            (
                "another life by the fence's end",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        Ok(Some(short_bsd(0))),
                        Ok(Some(1)),
                        Ok(Some(identity(8, 1))),
                    )
                },
                Ok(false),
            ),
        ];
        for (case, reads, expected) in cases {
            let (before, bsd, started, after) = reads();
            let decided = super::decide_life(before, bsd, started, after);
            match (decided, expected) {
                (Ok(life), Ok(present)) => assert_eq!(life.is_some(), present, "{case}"),
                (Err(error), Err(code)) => assert_eq!(error.raw_os_error(), Some(code), "{case}"),
                (Ok(life), Err(code)) => panic!(
                    "{case}: decided {:?}, expected errno {code}",
                    life.map(|life| life.record)
                ),
                (Err(error), Ok(_)) => panic!("{case}: decided {error}"),
            }
        }
    }

    /// What a signal refused with `EPERM` means, from the life read after the refusal: the
    /// kernel refuses an audit token whose process has exited, or whose pid another life now holds,
    /// with `EPERM` as well, so only the very life still running is a denial; a read that fails
    /// proves nothing gone, so it stays a denial.
    #[test]
    fn a_refused_signal_is_a_denial_only_while_the_signalled_life_runs() {
        use super::PermissionRefusal::{Denied, Gone};
        let unreadable = std::io::Error::from_raw_os_error(libc::EPERM);
        type Case<'a> = (
            &'static str,
            Result<Option<u32>, &'a std::io::Error>,
            super::PermissionRefusal,
        );
        let cases: [Case<'_>; 4] = [
            ("the signalled life runs", Ok(Some(88)), Denied),
            ("no life holds the pid", Ok(None), Gone),
            ("another life holds the pid", Ok(Some(89)), Gone),
            ("the pid cannot be read", Err(&unreadable), Denied),
        ];
        for (case, running, expected) in cases {
            assert_eq!(super::permission_refusal(88, running), expected, "{case}");
        }
    }

    /// What retirement does with the identity it reads before each signal: signal the life under
    /// the version just read; stop once no life or another holds the pid, or a signal was refused
    /// under this very version (the life is exiting); and an `EPERM` read is the retirement's
    /// error, never a life taken as gone.
    #[test]
    fn a_retirement_signals_the_life_it_reads_and_never_takes_an_unreadable_one_as_gone() {
        use super::SignalStep::{Gone, Send};
        type Identity = std::io::Result<Option<super::UniqueIdentifierInfo>>;
        type Case = (
            &'static str,
            fn() -> Identity,
            Option<u32>,
            Result<super::SignalStep, libc::c_int>,
        );
        let cases: [Case; 6] = [
            (
                "the life runs",
                || Ok(Some(identity(7, 3))),
                None,
                Ok(Send(3)),
            ),
            (
                "refused under this version: exiting",
                || Ok(Some(identity(7, 3))),
                Some(3),
                Ok(Gone),
            ),
            (
                "an exec gave it a new version",
                || Ok(Some(identity(7, 4))),
                Some(3),
                Ok(Send(4)),
            ),
            (
                "another life holds the pid",
                || Ok(Some(identity(8, 1))),
                None,
                Ok(Gone),
            ),
            ("no life holds the pid", || Ok(None), None, Ok(Gone)),
            (
                "its identity declined",
                || errno(libc::EPERM),
                None,
                Err(libc::EPERM),
            ),
        ];
        for (case, identity, refused, expected) in cases {
            match (super::signal_step(identity(), 7, refused), expected) {
                (Ok(step), Ok(expected)) => assert_eq!(step, expected, "{case}"),
                (Err(error), Err(code)) => assert_eq!(error.raw_os_error(), Some(code), "{case}"),
                (step, expected) => panic!("{case}: {step:?}, expected {expected:?}"),
            }
        }
    }

    /// A kqueue watch of one pid's exec (`EVFILT_PROC` `NOTE_EXEC`), registered before the exec.
    struct ExecWatch(std::os::fd::OwnedFd);

    impl ExecWatch {
        fn on(pid: u32) -> Self {
            use std::os::fd::{AsRawFd as _, FromRawFd as _};
            // SAFETY: kqueue takes nothing and returns a new descriptor or -1.
            let queue = unsafe { libc::kqueue() };
            assert!(queue >= 0, "kqueue: {}", std::io::Error::last_os_error());
            // SAFETY: `queue` is a new descriptor this watch owns.
            let queue = unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) };
            let change = libc::kevent {
                ident: usize::try_from(pid).expect("a pid"),
                filter: libc::EVFILT_PROC,
                flags: libc::EV_ADD | libc::EV_ONESHOT,
                fflags: libc::NOTE_EXEC,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            // SAFETY: one change record is read, no event buffer is written.
            let added = unsafe {
                libc::kevent(
                    queue.as_raw_fd(),
                    &raw const change,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            };
            assert_eq!(
                added,
                0,
                "watch the exec of {pid}: {}",
                std::io::Error::last_os_error()
            );
            Self(queue)
        }

        /// Whether the exec came within `bound`.
        fn within(&self, bound: Duration) -> bool {
            use std::os::fd::AsRawFd as _;
            let timeout = libc::timespec {
                tv_sec: libc::time_t::try_from(bound.as_secs()).expect("a bound in seconds"),
                tv_nsec: libc::c_long::from(bound.subsec_nanos()),
            };
            loop {
                // SAFETY: an all-zero kevent is valid storage for the kernel to fill.
                let mut event: libc::kevent = unsafe { std::mem::zeroed() };
                // SAFETY: no change is read; one event record is written.
                let seen = unsafe {
                    libc::kevent(
                        self.0.as_raw_fd(),
                        std::ptr::null(),
                        0,
                        &raw mut event,
                        1,
                        &raw const timeout,
                    )
                };
                match seen {
                    1 => return event.fflags & libc::NOTE_EXEC != 0,
                    0 => return false,
                    _ if std::io::Error::last_os_error().kind()
                        == std::io::ErrorKind::Interrupted => {}
                    _ => panic!("wait for the exec: {}", std::io::Error::last_os_error()),
                }
            }
        }
    }

    /// The shell of the decisions above, on a real setuid-root program: a leader that execs
    /// `/usr/bin/top` (setuid root on every macOS host) runs as root's effective uid and the
    /// job's real uid. Read once its exec is seen (no uid poll), it reads with the flavors
    /// `decide_life` takes, while `proc_pid_rusage` declines it with `EPERM`; it proves its group
    /// running and once exited and held unreaped; and retirement's signal reaches it. The shell's
    /// sibling report proves its body started before the watch: /bin/sh first execs its selected
    /// shell, and that earlier NOTE_EXEC is not top's exec.
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_a_setuid_root_leader_reads_proves_its_group_and_retires() {
        let (job, sibling) =
            group_with_child("sleep 600 & echo $!; read _; exec /usr/bin/top -l 0 -s 1 >/dev/null");
        let mut group = OwnedGroup(job);
        let pid = group.0.id();
        let leader_pid = i32::try_from(pid).expect("a pid");
        let birth = super::Birth::Observed(leader(pid));
        let exec = ExecWatch::on(pid);
        std::io::Write::write_all(group.0.stdin.as_mut().expect("piped stdin"), b"\n")
            .expect("let the leader exec top");
        assert!(
            exec.within(cowshed_os::test_backstop::NO_PROGRESS),
            "the leader did not exec /usr/bin/top within the test backstop"
        );
        let record = super::process_record(leader_pid)
            .expect("read the leader")
            .expect("top runs");
        assert_eq!(
            record.uid, 0,
            "precondition not met: /usr/bin/top runs as effective uid {}, not root. This check needs \
             setuid honored, outside every cow sandbox (nx run cowshed:host-controller-test)",
            record.uid
        );
        assert_eq!(
            super::birth_time(leader_pid)
                .expect_err("rusage declines another effective uid")
                .raw_os_error(),
            Some(libc::EPERM)
        );
        let members = super::job_member_records(&birth).expect("the job's membership");
        assert_eq!(
            members.len(),
            2,
            "the leader and its sleeping sibling: {members:?}"
        );
        assert!(
            members
                .iter()
                .any(|member| member.pid == pid && member.uid == 0),
            "{members:?}"
        );
        assert!(super::signal_life(pid, record.unique_id, libc::SIGTERM).expect("signal top"));
        assert_eq!(
            super::await_exit_unreaped(leader_pid).expect("top exits"),
            crate::api::dto::ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false,
            }
        );
        assert_eq!(
            super::job_member_records(&birth)
                .expect("the exited leader's group")
                .iter()
                .map(|member| member.pid)
                .collect::<Vec<_>>(),
            [sibling]
        );
    }

    /// A job-shaped process tree whose leader has finished starting: an exec gives a process a
    /// new pid version, and macOS's `/bin/sh` execs the shell it selects (measured: about 2ms
    /// after the spawn returns). Its leader announces itself only after that last exec, so the
    /// identity read once this returns is the one it keeps.
    fn started_job_group() -> std::process::Child {
        use std::io::BufRead as _;

        let mut child = Command::new("/bin/sh")
            .args(["-c", "exec 3<&0; read _ <&3 & echo READY; wait"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a job group");
        let mut ready = String::new();
        std::io::BufReader::new(child.stdout.take().expect("its stdout"))
            .read_line(&mut ready)
            .expect("the leader announces itself");
        assert_eq!(ready.trim(), "READY");
        child
    }

    /// End what remains of a [`started_job_group`] once its leader is reaped: the group's id
    /// still names its backgrounded reader. Returns once no process holds the id, so nothing the
    /// test started outlives it holding the runner's output.
    fn end_started_group(pgid: i32) {
        // SAFETY: the test's own group; its backgrounded reader may still hold the id.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
        while super::group_has_live_members(pgid).unwrap() {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// A process is signalled only through the identity it was read with: the pid and that pid's
    /// version. The same pid under any other version -- the shape a reused pid takes -- is never
    /// reached, and neither is the process once it has exited and been reaped.
    #[test]
    fn a_signal_reaches_only_the_process_its_identity_names() {
        let mut job = started_job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let members = super::members_of(pgid).unwrap();
        let leader = members
            .iter()
            .find(|member| member.pid() == pgid)
            .expect("the running leader holds its group id");
        let other_incarnation = super::Process {
            pid: leader.pid,
            version: leader.version.wrapping_add(1),
            unique_id: leader.unique_id.map(|id| id.wrapping_add(1)),
        };
        assert!(!other_incarnation.signal(libc::SIGKILL).unwrap());
        assert!(
            group_alive(pgid),
            "a process under another version was signalled"
        );
        assert!(leader.signal(libc::SIGKILL).unwrap());
        job.wait_bounded().unwrap();
        assert!(
            !leader.signal(libc::SIGKILL).unwrap(),
            "a reaped process was signalled"
        );
        end_started_group(pgid);
    }

    /// A process that execs takes a new pid version, so a TERM sent under the identity read just
    /// before the exec reaches nothing while the process still runs as the job's. Its group is
    /// ended all the same: the KILL that follows the grace reads the group again. Here the group
    /// is read, its leader then execs, and the membership read before the exec is what TERM goes
    /// to -- the window a loaded host opens between a recovery's read and its TERM.
    #[test]
    fn a_group_whose_member_execed_after_it_was_read_is_still_ended() {
        use std::io::{BufRead as _, Write as _};

        let child = Command::new("/bin/sh")
            .args(["-c", "echo READY; read line; exec /bin/cat"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a job group");
        let mut job = OwnedGroup(child);
        let pgid = i32::try_from(job.0.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(job.0.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready.trim(), "READY");
        let group = super::Group {
            job_id: 6,
            pgid,
            leader_start: super::start_time(pgid).unwrap(),
            leader_birth: None,
            members: None,
        };
        let super::Ownership::Owned(read) = super::ownership(&group).unwrap() else {
            panic!("the running leader proves its group");
        };
        let [leader] = read.as_slice() else {
            panic!("the leader alone runs in the group: {read:?}");
        };
        let stale = leader.version;

        writeln!(job.0.stdin.as_mut().unwrap(), "go").unwrap();
        while super::Process::observe(pgid)
            .unwrap()
            .is_some_and(|now| now.version == stale)
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            super::group_has_live_members(pgid).unwrap(),
            "the leader runs on as `cat` under a new version"
        );

        assert_eq!(
            super::end_running(&[(group, read)], Duration::ZERO).unwrap(),
            (vec![6], Vec::new())
        );
        assert!(
            !super::group_has_live_members(pgid).unwrap(),
            "the exec'd leader was left running"
        );
    }

    /// A member's record is read only through the identity it was found with: under another
    /// unique id and version of its pid -- the shape a reused pid takes -- the census finds no
    /// process, so a stranger is never charged the job's CPU or named its leaf.
    #[test]
    fn a_census_reads_only_the_member_its_identity_names() {
        use crate::api::process::JobLeafAttribution;
        use crate::api::resources::{CpuMicros, CpuTotals};
        use crate::runtime::job_attribution::{Census, observe};
        use crate::runtime::supervisor::LeafCoverage;

        let mut job = started_job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let members = super::members_of(pgid).unwrap();
        let leader = members
            .iter()
            .find(|member| member.pid() == pgid)
            .expect("the running leader holds its group id");
        let stranger = super::Process {
            pid: leader.pid,
            version: leader.version.wrapping_add(1),
            unique_id: leader.unique_id.map(|id| id.wrapping_add(1)),
        };
        assert_eq!(stranger.record().unwrap(), None);
        assert_eq!(stranger.life_record().unwrap(), None);
        assert_eq!(
            leader.record().unwrap().map(|record| record.pid),
            Some(leader.pid.cast_unsigned())
        );
        assert_eq!(
            leader.life_record().unwrap().map(|(record, _)| record.pid),
            Some(leader.pid.cast_unsigned())
        );
        // One microsecond accounted: any own CPU of an identified member covers all of it.
        let total = CpuTotals {
            user_us: CpuMicros::new(1).unwrap(),
            sys_us: CpuMicros::new(0).unwrap(),
        };
        let coverage = LeafCoverage::new(900).unwrap();
        let leader_pid = leader.pid.cast_unsigned();

        let mut census = Census::led_by(leader_pid, std::time::Instant::now());
        observe(
            &mut census,
            std::slice::from_ref(&stranger),
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(
            census.attribute(total, coverage).unwrap().leaf,
            JobLeafAttribution::Unattributed {
                observed_cpu_permille: 0,
                required_permille: 900,
            },
            "another life of the pid is no member"
        );

        let mut census = Census::led_by(leader_pid, std::time::Instant::now());
        observe(
            &mut census,
            std::slice::from_ref(leader),
            std::time::Instant::now(),
        )
        .unwrap();
        let JobLeafAttribution::Attributed { leaf, .. } =
            census.attribute(total, coverage).unwrap().leaf
        else {
            panic!("the member itself is observed");
        };
        assert_eq!(leaf.pid, leader_pid);

        assert!(leader.signal(libc::SIGKILL).unwrap());
        job.wait_bounded().unwrap();
        end_started_group(pgid);
    }

    /// A process's exit is watched only while it runs: a watch made once it exited -- here, after
    /// it was reaped, when its pid could already name another process -- is refused, never taken
    /// as its exit. One made while it ran sees it exit, and not before.
    #[test]
    fn an_exit_is_watched_only_from_while_the_process_ran() {
        let mut job = started_job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let members = super::members_of(pgid).unwrap();
        let leader = members
            .iter()
            .find(|member| member.pid() == pgid)
            .expect("the running leader holds its group id");
        let exit = leader.watch_exit().expect("a running process is watched");
        assert!(!exit.within(Duration::from_millis(50)).unwrap());

        assert!(leader.signal(libc::SIGTERM).unwrap());
        exit.wait().unwrap();
        job.wait_bounded().unwrap();
        assert!(
            leader.watch_exit().is_err(),
            "a reaped process's exit was watched"
        );
        end_started_group(pgid);
    }

    /// A pid another user's process now holds is told from the recorded leader by its start
    /// time, which the kernel answers whoever owns the process: a start that differs names
    /// another process, so the group is not the job's, and a lost supervisor's ledger naming it is
    /// taken without a signal or a refusal. A start that matches is inspected: every member's
    /// identity is public whoever owns it (11_shell.md), so the group is read, and signalling
    /// another user's process stays the kernel's refusal. An earlier build's rusage birth cannot
    /// be compared at all -- the kernel declines another user's rusage -- and stays a refusal.
    /// launchd (pid 1, root) stands in for the recycled pid.
    #[test]
    fn a_pid_another_user_now_holds_is_told_from_the_leader_by_its_start_time() {
        const HOLDER: i32 = 1;
        let started = super::start_time(HOLDER)
            .unwrap()
            .expect("launchd holds pid 1");
        assert_eq!(
            super::birth_time(HOLDER).unwrap_err().raw_os_error(),
            Some(libc::EPERM),
            "the kernel declines this user another user's rusage"
        );
        let group = |leader_start, leader_birth| super::Group {
            job_id: 68,
            pgid: HOLDER,
            leader_start,
            leader_birth,
            members: None,
        };
        assert!(matches!(
            super::ownership(&group(Some(started + 1), None)).unwrap(),
            super::Ownership::NotOwned
        ));
        let super::Ownership::Owned(members) = super::ownership(&group(Some(started), None))
            .expect("a holder whose start matches the record is inspected")
        else {
            panic!("a holder whose start matches the record is never released uninspected");
        };
        let launchd = members
            .iter()
            .find(|member| member.pid() == HOLDER)
            .expect("launchd holds its own group");
        assert_eq!(
            launchd.signal(libc::SIGCONT).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied,
            "the kernel refuses this user a signal to another user's process"
        );
        let Err(legacy) = super::ownership(&group(None, Some(3_131_429_805_919))) else {
            panic!("an earlier build's birth was compared against another user's process");
        };
        assert_eq!(legacy.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            legacy
                .to_string()
                .contains("against its recorded rusage birth from an earlier build"),
            "{legacy}"
        );

        let (_scratch, ledger) = scratch();
        super::replace(
            &ledger,
            &super::Ledger {
                supervisor: std::process::id() + 1,
                ended: true,
                groups: vec![group(Some(started + 1), None)],
                unresolved: Vec::new(),
                darwinbg: None,
            },
        )
        .unwrap();
        assert!(
            take_lost(&ledger, Duration::ZERO)
                .unwrap()
                .unresolved
                .is_empty()
        );
        assert!(!ledger.exists());
    }

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
            let (_scratch, ledger) = scratch();
            super::Recorder::new(ledger.clone())
                .record(
                    [super::JobGroupClaim::group(7, leader(group.0.id()))].into_iter(),
                    &[],
                )
                .unwrap();
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
                    "runtime::job_groups::macos::tests::permission_denials_preserve_the_owned_ledger_and_allow_retry",
                    "--nocapture",
                ])
                .env(LEDGER_ENV, &ledger).output_bounded()
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
