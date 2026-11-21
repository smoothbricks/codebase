//! Linux's process identity for job groups (11_shell.md "Process identity and signal ownership").
//!
//! - A [`Process`] is a pidfd, which names the one process it was opened for whatever later
//!   takes its pid: `pidfd_send_signal` reaches that process or nothing, and the descriptor is
//!   readable once it has exited. An exec keeps the pidfd.
//! - A life's token ([`Life`], [`ProcessRecord::unique_id`]) is `starttime`, field 22 of
//!   `/proc/<pid>/stat`, in clock ticks since boot. It is no identity by itself: two pids may
//!   share it, and a pid given away within one tick of its last life's start repeats it. It is
//!   only ever compared together with its pid, and wherever a signal or a watch follows, the
//!   comparison is made under a pidfd opened before the read and polled after it, so what is
//!   then held is that one process.
//! - A leader's start and its birth are both that `starttime`, which procfs keeps for an exited
//!   process until it is reaped.
//! - A group's members are `cowshed_os::process::group_pids`'s, each opened as a pidfd and
//!   read again under it.
//! - A socket peer is `SO_PEERPIDFD`'s pidfd, with `SO_PEERCRED`'s pid for its number; the boot
//!   is `/proc/sys/kernel/random/boot_id`.
//!
//! procfs names a process by pid alone, so every read here is fenced: by a pidfd polled after
//! the read where one is held, otherwise by the `starttime` read before and after. A read the
//! kernel refuses is an error, never an absent process; only `ENOENT` and `ESRCH` on a mounted
//! procfs say no process holds the pid. Each procfs file is read into a stack buffer, so a read
//! allocates only the text of a malformed one.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::io;
use std::num::NonZeroU32;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use cowshed_os::process::ExitWatch;

use super::{Group, Life, ProcessExit, ProcessRecord};
use crate::runtime::process_tree::ProcessIdentity;

/// Room for `/proc/<pid>/stat` whole: 52 numeric fields and a command name of at most 64 bytes
/// come to about 400 bytes on Linux 6.x.
const STAT_BYTES: usize = 2048;

/// Room for the head of `/proc/<pid>/status` that holds its `Uid:` line, the ninth, before the
/// masks whose length grows with the host.
const STATUS_HEAD_BYTES: usize = 1024;

/// The boot session recorded lives are fenced by: the kernel's random id of this boot.
pub(super) fn boot_session() -> io::Result<String> {
    const BOOT_ID: &str = "/proc/sys/kernel/random/boot_id";
    let mut session = std::fs::read_to_string(BOOT_ID)?;
    session.truncate(session.trim_end().len());
    if session.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{BOOT_ID} names no boot"),
        ));
    }
    Ok(session)
}

/// `Ok` when `error` is the kernel saying no process holds the pid a procfs path named, the
/// error itself otherwise. An unmounted procfs has no such path for any pid and proves nothing,
/// so it is an error too.
fn absent(error: io::Error) -> io::Result<()> {
    if error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH) {
        std::fs::metadata("/proc/self/stat").map(|_| ())
    } else {
        Err(error)
    }
}

/// Read `/proc/<pid>/<name>` from its start into `buffer`: the bytes read, which fill the
/// buffer when the file holds more, or `None` once no process holds the pid.
fn read_proc<'b>(pid: i32, name: &str, buffer: &'b mut [u8]) -> io::Result<Option<&'b [u8]>> {
    use std::io::{Read as _, Write as _};

    // "/proc/", a pid of at most eleven characters, "/" and the longest name read here.
    let mut path = [0_u8; 32];
    let mut cursor = io::Cursor::new(&mut path[..]);
    write!(cursor, "/proc/{pid}/{name}")?;
    let length = usize::try_from(cursor.position()).map_err(io::Error::other)?;
    let mut file = match std::fs::File::open(Path::new(OsStr::from_bytes(&path[..length]))) {
        Ok(file) => file,
        Err(error) => return absent(error).map(|()| None),
    };
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            // The process was reaped between the open and the read.
            Err(error) => return absent(error).map(|()| None),
        }
    }
    Ok(Some(&buffer[..filled]))
}

/// The fields of `/proc/<pid>/stat` the runtime reads, as the kernel printed them in one read.
/// An exited process keeps them until it is reaped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime) struct Stat {
    /// Field 3: `Z` once exited and not yet reaped, `X` while it is released.
    state: u8,
    /// Field 4: the parent's pid, 0 for a process with no parent in this pid namespace.
    pub(in crate::runtime) ppid: i32,
    /// Field 5.
    pgrp: i32,
    /// Fields 14 and 15: the process's own CPU, every thread's, in clock ticks.
    pub(in crate::runtime) utime: u64,
    pub(in crate::runtime) stime: u64,
    /// Fields 16 and 17: the CPU of the children it waited for, with what they waited for in
    /// turn, in clock ticks.
    pub(in crate::runtime) cutime: u64,
    pub(in crate::runtime) cstime: u64,
    /// Field 22: when it started, in clock ticks since boot.
    pub(in crate::runtime) starttime: u64,
}

impl Stat {
    fn parse(pid: i32, bytes: &[u8]) -> io::Result<Self> {
        fn field<T: std::str::FromStr>(
            fields: &mut std::str::SplitAsciiWhitespace<'_>,
            skip: usize,
        ) -> Option<T> {
            fields.nth(skip)?.parse().ok()
        }

        let invalid = |what: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "process {pid}'s stat {:?} {what}",
                    String::from_utf8_lossy(bytes)
                ),
            )
        };
        // The command name is parenthesized and may hold anything -- spaces, parentheses, bytes
        // that are no UTF-8 -- so the fields are those after its last ')', which are ASCII.
        let closing = bytes
            .iter()
            .rposition(|&byte| byte == b')')
            .ok_or_else(|| invalid("has no command delimiter"))?;
        let mut fields = std::str::from_utf8(&bytes[closing + 1..])
            .map_err(|_| invalid("holds fields that are no text"))?
            .split_ascii_whitespace();
        let state = fields
            .next()
            .and_then(|state| state.bytes().next())
            .ok_or_else(|| invalid("has no state"))?;
        // Each skip counts the fields between the last one read and the one wanted.
        let ppid = field(&mut fields, 0).ok_or_else(|| invalid("has no parent pid"))?;
        let pgrp = field(&mut fields, 0).ok_or_else(|| invalid("has no process group"))?;
        let utime = field(&mut fields, 8).ok_or_else(|| invalid("has no user time"))?;
        let stime = field(&mut fields, 0).ok_or_else(|| invalid("has no system time"))?;
        let cutime = field(&mut fields, 0).ok_or_else(|| invalid("has no children user time"))?;
        let cstime = field(&mut fields, 0).ok_or_else(|| invalid("has no children system time"))?;
        let starttime = field(&mut fields, 4).ok_or_else(|| invalid("has no start time"))?;
        Ok(Self {
            state,
            ppid,
            pgrp,
            utime,
            stime,
            cutime,
            cstime,
            starttime,
        })
    }

    /// Whether the process has exited: held unreaped, or being released.
    fn exited(&self) -> bool {
        matches!(self.state, b'Z' | b'X')
    }
}

/// `/proc/<pid>/stat` of whatever holds `pid` now, running or exited and not yet reaped; `None`
/// once nothing does.
pub(in crate::runtime) fn stat(pid: i32) -> io::Result<Option<Stat>> {
    let mut buffer = [0_u8; STAT_BYTES];
    let Some(bytes) = read_proc(pid, "stat", &mut buffer)? else {
        return Ok(None);
    };
    if bytes.len() == STAT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid}'s stat is longer than the {STAT_BYTES} bytes read of it"),
        ));
    }
    Stat::parse(pid, bytes).map(Some)
}

/// [`stat`] of a process that has not exited.
fn running_stat(pid: i32) -> io::Result<Option<Stat>> {
    Ok(stat(pid)?.filter(|stat| !stat.exited()))
}

/// The effective uid of whatever holds `pid` now: the second of the four `Uid:` names in
/// `/proc/<pid>/status`. `None` once nothing holds the pid.
fn effective_uid(pid: i32) -> io::Result<Option<u32>> {
    let mut buffer = [0_u8; STATUS_HEAD_BYTES];
    let Some(head) = read_proc(pid, "status", &mut buffer)? else {
        return Ok(None);
    };
    // Only lines the read holds whole: one cut by the buffer's end could name a shorter uid.
    let whole = head
        .iter()
        .rposition(|&byte| byte == b'\n')
        .map_or(&[][..], |end| &head[..end]);
    whole
        .split(|&byte| byte == b'\n')
        .find_map(|line| line.strip_prefix(b"Uid:"))
        .and_then(|uids| std::str::from_utf8(uids).ok())
        .and_then(|uids| uids.split_ascii_whitespace().nth(1))
        .and_then(|uid| uid.parse().ok())
        .map(Some)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process {pid}'s status names no effective uid"),
            )
        })
}

/// How many clock ticks the kernel counts a second in (`USER_HZ`), the unit of every time in
/// `/proc/<pid>/stat`; the refused answer itself when `sysconf` gives no rate.
pub(in crate::runtime) fn clock_ticks_per_second() -> Result<NonZeroU32, libc::c_long> {
    // SAFETY: sysconf takes a plain integer and touches no memory of ours.
    let hertz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u32::try_from(hertz)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(hertz)
}

/// `clock` now, in microseconds.
fn clock_micros(clock: libc::clockid_t) -> io::Result<u64> {
    let mut now = std::mem::MaybeUninit::<libc::timespec>::zeroed();
    // SAFETY: `now` is writable storage for one timespec.
    if unsafe { libc::clock_gettime(clock, now.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful call filled the timespec.
    let now = unsafe { now.assume_init() };
    let invalid = |error| io::Error::new(io::ErrorKind::InvalidData, error);
    let seconds = u64::try_from(now.tv_sec).map_err(invalid)?;
    let nanos = u64::try_from(now.tv_nsec).map_err(invalid)?;
    seconds
        .checked_mul(1_000_000)
        .and_then(|micros| micros.checked_add(nanos / 1_000))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "the clock overflowed"))
}

/// The calendar time of this boot in microseconds: the wall clock less the clock `starttime`
/// counts on. Read once per process, so every record of one life names the same start second
/// whatever steps the wall clock later takes.
fn boot_epoch_micros() -> io::Result<u64> {
    static BOOT: OnceLock<u64> = OnceLock::new();
    if let Some(&boot) = BOOT.get() {
        return Ok(boot);
    }
    let wall = clock_micros(libc::CLOCK_REALTIME)?;
    let since_boot = clock_micros(libc::CLOCK_BOOTTIME)?;
    let boot = wall.checked_sub(since_boot).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "the wall clock reads earlier than the boot it counts from",
        )
    })?;
    Ok(*BOOT.get_or_init(|| boot))
}

/// The calendar second a process whose `starttime` is `ticks` started.
fn started_seconds(ticks: u64) -> io::Result<u64> {
    let hertz = clock_ticks_per_second().map_err(|hertz| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the clock tick rate {hertz} is no rate"),
        )
    })?;
    let since_boot = u128::from(ticks) * 1_000_000 / u128::from(hertz.get());
    u64::try_from((u128::from(boot_epoch_micros()?) + since_boot) / 1_000_000)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// The pidfd of the process `pid` names now, running or exited and not yet reaped; `None` when
/// no process holds the pid. That is `ESRCH`, and also `EINVAL`: the number of a leader reaped
/// while its process group lives on is kept for the group, with no process to open; a number
/// now given to a thread names no process; and neither does one below one.
fn open_pidfd(pid: i32) -> io::Result<Option<OwnedFd>> {
    // SAFETY: pidfd_open takes plain integers; a non-negative answer is a new descriptor.
    let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if opened < 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH | libc::EINVAL) => Ok(None),
            _ => Err(error),
        };
    }
    let opened = libc::c_int::try_from(opened).map_err(io::Error::other)?;
    // SAFETY: a fresh descriptor this function alone owns.
    Ok(Some(unsafe { OwnedFd::from_raw_fd(opened) }))
}

/// Whether the process `handle` names has exited: a pidfd becomes readable when it does.
fn pidfd_exited(handle: &OwnedFd) -> io::Result<bool> {
    let mut exited = libc::pollfd {
        fd: handle.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one live pollfd entry.
    if unsafe { libc::poll(&mut exited, 1, 0) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(exited.revents != 0)
}

/// Resolve a procfs read, which named a pid alone, against the lifetime of the process the
/// pidfd of that pid names.
fn read_while_running<T>(handle: &OwnedFd, read: io::Result<T>) -> io::Result<Option<T>> {
    // A departed process is no current member, even when its procfs read returned ESRCH.
    // Only a process still live afterwards can contribute its read or its read failure.
    if pidfd_exited(handle)? {
        return Ok(None);
    }
    read.map(Some)
}

/// A process held by an identity the kernel never gives another: a pidfd, which names the one
/// process it was opened for. Signalling it reaches that process or nothing.
#[derive(Debug)]
pub struct Process {
    pid: i32,
    handle: OwnedFd,
}

/// Every running process that holds the group id, each held by a pidfd. A member counts only
/// if its stat, read after its pidfd was opened, shows it running in the group while the pidfd
/// still shows it running afterwards: the stat was then that very process's.
pub(super) fn members_of(pgid: i32) -> io::Result<Vec<Process>> {
    let mut members = Vec::new();
    for pid in cowshed_os::process::group_pids(pgid)? {
        let Some(handle) = open_pidfd(pid)? else {
            continue;
        };
        let Some(stat) = read_while_running(&handle, stat(pid))?.flatten() else {
            continue;
        };
        if !stat.exited() && stat.pgrp == pgid {
            members.push(Process { pid, handle });
        }
    }
    Ok(members)
}

/// The record of every running process that holds the group id, as the kernel names each.
pub(super) fn member_records(pgid: i32) -> io::Result<Vec<ProcessRecord>> {
    let mut records = Vec::new();
    for member in members_of(pgid)? {
        // A member that exited since it was read is no member any more.
        if let Some(record) = member.record()? {
            records.push(record);
        }
    }
    Ok(records)
}

/// The running process `pid` names if it started at `started`, held by a pidfd; `None` once
/// that life has exited, reaped or not. The pidfd is opened before the stat is read and polled
/// after it, so the start compared is that of the process held.
fn held_life(pid: i32, started: u64) -> io::Result<Option<Process>> {
    let Some(handle) = open_pidfd(pid)? else {
        return Ok(None);
    };
    let Some(stat) = read_while_running(&handle, stat(pid))?.flatten() else {
        return Ok(None);
    };
    Ok((!stat.exited() && stat.starttime == started).then_some(Process { pid, handle }))
}

/// The recorded life if it still runs, held by a pidfd.
pub(super) fn recorded_life(life: ProcessIdentity) -> io::Result<Option<Process>> {
    let pid = i32::try_from(life.pid).map_err(io::Error::other)?;
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "recorded process life needs a positive pid",
        ));
    }
    held_life(pid, life.birth.0)
}

/// End every recorded life of this boot that still runs: TERM, `grace`, then KILL, and the exit
/// of each as its proof. Each life is held by a pidfd from its first read on, so both signals
/// and the proof name that one process, through any exec and whatever takes its pid later. A
/// life with no exit proof after the KILL is an error, and its ledger claim stays.
pub(super) fn end_recorded_members(groups: &[Group], grace: Duration) -> io::Result<Vec<u64>> {
    if groups.iter().all(|group| group.members.is_none()) {
        return Ok(Vec::new());
    }
    let boot = boot_session()?;
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
            if let Some(process) = recorded_life(life)? {
                let exit = process.watch_exit()?;
                watched.push((group.job_id, life, process, exit, false));
            }
        }
    }
    for (job, _, process, _, _) in &watched {
        if process.signal(libc::SIGTERM)? {
            signalled.insert(*job);
        }
    }
    let deadline = Instant::now() + grace;
    for (_, _, _, exit, ended) in &mut watched {
        *ended = exit.within(deadline.saturating_duration_since(Instant::now()))?;
    }
    for (job, _, process, _, ended) in &watched {
        if !ended && process.signal(libc::SIGKILL)? {
            signalled.insert(*job);
        }
    }
    let deadline = Instant::now() + grace.max(Duration::from_secs(1));
    for (_, life, _, exit, ended) in watched {
        if !ended && !exit.within(deadline.saturating_duration_since(Instant::now()))? {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "recorded process {} (started at tick {}) has no exit proof after SIGKILL; its ledger claim is retained",
                    life.pid, life.birth.0
                ),
            ));
        }
    }
    Ok(signalled.into_iter().collect())
}

/// Signal the life `pid` holds while its start is `unique_id`; `false` once that life no longer
/// runs. The signal goes through a pidfd proven to name that life ([`held_life`]), so it
/// reaches that life or nothing, through any exec.
pub(crate) fn signal_life(pid: u32, unique_id: u64, signal: libc::c_int) -> io::Result<bool> {
    let pid = i32::try_from(pid).map_err(io::Error::other)?;
    match held_life(pid, unique_id)? {
        Some(process) => process.signal(signal),
        None => Ok(false),
    }
}

/// The public metadata of the life `pid` holds now, fenced by its start: its stat is read
/// before and after its uid and its parent's start, and a start that changed is another life,
/// so the pid holds none of this one. `None` once it has exited, reaped or not. The parent's
/// start is the parent's only if the process still names that parent afterwards -- a parent
/// that exits hands its children to another at once -- so a process reparented during the reads
/// is read again from the parent it has now. Credentials, parent and group are a snapshot of
/// that life, not signal authority. No failed read is classified as absence.
pub(crate) fn process_record(pid: i32) -> io::Result<Option<ProcessRecord>> {
    let Some(mut before) = running_stat(pid)? else {
        return Ok(None);
    };
    loop {
        let Some(uid) = effective_uid(pid)? else {
            return Ok(None);
        };
        // A process with no parent in this pid namespace has no parent life to name.
        let parent = match before.ppid {
            0 => Some(0),
            ppid => stat(ppid)?.map(|parent| parent.starttime),
        };
        let Some(after) = running_stat(pid)? else {
            return Ok(None);
        };
        if after.starttime != before.starttime {
            return Ok(None);
        }
        if after.ppid != before.ppid {
            before = after;
            continue;
        }
        let parent_unique_id = parent.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "process {pid} names parent {}, which procfs shows no process holding",
                    after.ppid
                ),
            )
        })?;
        let invalid = |error| io::Error::new(io::ErrorKind::InvalidData, error);
        return Ok(Some(ProcessRecord {
            pid: u32::try_from(pid).map_err(invalid)?,
            ppid: u32::try_from(after.ppid).map_err(invalid)?,
            unique_id: after.starttime,
            parent_unique_id,
            uid,
            started_seconds: started_seconds(after.starttime)?,
        }));
    }
}

/// The token of the life `pid` names now: its start in clock ticks since boot, which no later
/// life of the pid shares unless it starts within the same tick. `None` once it has exited,
/// reaped or not.
pub(crate) fn life_of(pid: i32) -> io::Result<Option<u64>> {
    Ok(running_stat(pid)?.map(|stat| stat.starttime))
}

/// A process's start time in clock ticks since boot, or confirmed absence on a mounted procfs.
/// procfs answers it for every process whoever owns it, and for one that exited and is not yet
/// reaped; `None` once nothing holds the pid. An unreadable process is never absent.
pub fn start_time(pid: i32) -> io::Result<Option<u64>> {
    Ok(stat(pid)?.map(|stat| stat.starttime))
}

/// A process's birth time: its start, which procfs keeps for an exited process until it is
/// reaped. Linux has one clock for both.
pub(super) fn birth_time(pid: i32) -> io::Result<Option<u64>> {
    start_time(pid)
}

impl Process {
    /// The process `pid` names now, held by a pidfd, which names that one process whatever later
    /// takes its pid; `None` once it has exited. A pid below one names no process, so it is
    /// refused, not read.
    pub fn observe(pid: i32) -> io::Result<Option<Self>> {
        if pid < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pid {pid} names no process"),
            ));
        }
        let Some(handle) = open_pidfd(pid)? else {
            return Ok(None);
        };
        if pidfd_exited(&handle)? {
            return Ok(None);
        }
        Ok(Some(Self { pid, handle }))
    }

    /// The process at the other end of a connected Unix socket, as the kernel recorded it when
    /// the connection was made: whatever happens to its pid later, the handle names only it.
    pub fn of_socket_peer(socket: &impl std::os::fd::AsRawFd) -> io::Result<Self> {
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
        let handle = unsafe { OwnedFd::from_raw_fd(opened) };
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

    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// The pidfd that names this process, for a read that fences itself by it.
    pub(in crate::runtime) fn pidfd(&self) -> BorrowedFd<'_> {
        self.handle.as_fd()
    }

    /// This process's record, read now; `None` once it has exited. The reads name only the pid,
    /// so they count only if the pidfd -- which names this process alone -- says it still runs
    /// afterwards: a record read for a later process given the pid is never this one's.
    pub(crate) fn record(&self) -> io::Result<Option<ProcessRecord>> {
        Ok(read_while_running(&self.handle, process_record(self.pid))?.flatten())
    }

    /// Whether this very life still runs: its pidfd names it alone, whatever later takes its pid.
    pub(crate) fn running(&self) -> io::Result<bool> {
        Ok(!pidfd_exited(&self.handle)?)
    }

    /// Whether `other` is this very life. Each names a process that held the pid when it was
    /// opened, and while this one has not exited the pid names no other process.
    pub(crate) fn same_life(&self, other: &Self) -> io::Result<bool> {
        Ok(self.pid == other.pid && !pidfd_exited(&self.handle)?)
    }

    /// Signal the process; `false` when it no longer runs. A pidfd still takes a signal for a
    /// process that exited and is not yet reaped, to no effect, so its exit is asked first.
    pub fn signal(&self, signal: libc::c_int) -> io::Result<bool> {
        if pidfd_exited(&self.handle)? {
            return Ok(false);
        }
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

    /// Watch for this process's exit: its pidfd names it alone, whenever it is polled.
    pub fn watch_exit(&self) -> io::Result<ExitWatch> {
        self.handle.try_clone().map(ExitWatch::from_pidfd)
    }

    /// Watch for this process's exit: its pidfd names it alone, exited or not, so the watch of
    /// one that already exited reports that exit at once.
    pub(super) fn watch_running(&self) -> io::Result<Option<ExitWatch>> {
        self.watch_exit().map(Some)
    }

    /// The bytes this process holds resident now; `None` once it has exited. The read names
    /// only the pid, so it counts only if the pidfd -- which names this process alone -- says it
    /// still runs afterwards: then the size read was its own, not that of a later process given
    /// the pid. A read that failed for a process gone since is its exit, not an error.
    pub(in crate::runtime) fn resident_bytes(&self) -> io::Result<Option<u64>> {
        let read = cowshed_os::process::resident_bytes(self.pid);
        if pidfd_exited(&self.handle)? {
            return Ok(None);
        }
        read
    }

    /// The ports of the TCP sockets this process listens on now; `None` once it has exited. The
    /// pidfd, polled after the read, judges it: still running, this process's descriptors and
    /// network tables were read -- or failed to be -- as its own; exited, it holds nothing,
    /// whatever the read met. A missing table of a process still running is an error.
    pub(in crate::runtime) fn listening_ports(&self) -> io::Result<Option<BTreeSet<u16>>> {
        read_while_running(&self.handle, cowshed_os::process::listening_ports(self.pid))
    }
}

impl Life {
    /// The process this life is now, held by a pidfd; `None` once it has exited, reaped or not,
    /// or once its pid names a process with another start.
    pub(crate) fn current(self) -> io::Result<Option<Process>> {
        held_life(self.pid, self.unique_id)
    }

    /// The life `pid` names now; `None` once it has exited, reaped or not.
    pub fn of(pid: i32) -> io::Result<Option<Self>> {
        Ok(life_of(pid)?.map(|unique_id| Self { pid, unique_id }))
    }

    /// Its exit, watched from now through any exec; `Before` once it has exited: a later process
    /// given its pid is never taken for it ([`Process::watch_life`]).
    pub fn exit(&self) -> io::Result<ProcessExit> {
        Ok(self
            .watch_exit()?
            .map_or(ProcessExit::Before, ProcessExit::Watching))
    }

    /// Watch this life's exit from now, through any exec; `None` once it has exited. The watch
    /// is the pidfd [`Self::current`] proved to name this life.
    pub(crate) fn watch_exit(self) -> io::Result<Option<ExitWatch>> {
        self.current()?
            .map(|process| process.watch_exit())
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};

    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;

    fn pipe_held_process() -> (std::process::Child, std::os::fd::OwnedFd) {
        use std::os::fd::FromRawFd as _;

        let child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .process_group(0)
            .spawn_locked()
            .expect("a pipe-held child");
        // SAFETY: pidfd_open takes plain integers; the child remains live on its stdin.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0) };
        assert!(
            opened >= 0,
            "pidfd_open: {}",
            std::io::Error::last_os_error()
        );
        let opened = libc::c_int::try_from(opened).expect("a pidfd fits a descriptor");
        // SAFETY: this fresh descriptor is owned only by this test.
        let handle = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) };
        (child, handle)
    }

    #[test]
    fn a_proc_stat_read_of_a_reaped_process_is_its_departure_not_a_membership_failure() {
        use std::io::Read as _;

        let (mut child, handle) = pipe_held_process();
        let mut stat = std::fs::File::open(format!("/proc/{}/stat", child.id()))
            .expect("open the live child's proc-stat inode");
        drop(child.stdin.take());
        assert!(
            child
                .wait_bounded()
                .expect("reap the departed child")
                .success()
        );
        let mut text = String::new();
        let read = stat.read_to_string(&mut text).map(|_| text);
        assert_eq!(
            read.as_ref().unwrap_err().raw_os_error(),
            Some(libc::ESRCH),
            "the open proc-stat inode reports its process was reaped"
        );
        assert_eq!(
            super::read_while_running(&handle, read).unwrap(),
            None,
            "the retained pidfd proves this process departed during the read"
        );
    }

    #[test]
    fn a_proc_stat_read_failure_of_a_live_process_remains_an_error() {
        let (mut child, handle) = pipe_held_process();
        let failures = [libc::EACCES, libc::ESRCH, libc::ENOENT].map(|code| {
            (
                code,
                super::read_while_running(
                    &handle,
                    Err::<String, _>(std::io::Error::from_raw_os_error(code)),
                ),
            )
        });
        drop(child.stdin.take());
        assert!(
            child
                .wait_bounded()
                .expect("reap the child after its live reads")
                .success()
        );
        for (code, read) in failures {
            assert_eq!(read.unwrap_err().raw_os_error(), Some(code));
        }
    }

    /// The fields are those after the command name's last ')', whatever the name holds: spaces,
    /// parentheses, or bytes that are no UTF-8.
    #[test]
    fn a_stat_is_read_past_whatever_its_command_name_holds() {
        let stat = b"4242 (a) b\xff) c) S 1 4242 4242 0 -1 4194560 1 2 3 4 50 60 70 80 20 0 1 0 987654 1 2\n";
        assert_eq!(
            super::Stat::parse(4242, stat).expect("a whole stat"),
            super::Stat {
                state: b'S',
                ppid: 1,
                pgrp: 4242,
                utime: 50,
                stime: 60,
                cutime: 70,
                cstime: 80,
                starttime: 987_654,
            }
        );
        let cut = b"4242 (cat) Z 1 4242 4242 0 -1 4194560 1 2 3 4 50 60 70 80 20 0 1 0";
        assert_eq!(
            super::Stat::parse(4242, cut)
                .expect_err("a stat with no start time is refused")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            super::Stat::parse(4242, b"4242 cat S 1")
                .expect_err("a stat with no command delimiter is refused")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    /// A record names the life its pid holds: this process's own pid, parent, effective uid and
    /// start, with its parent's start beside it; a life is found again only under that start.
    #[test]
    fn a_record_names_the_life_its_pid_holds_and_a_life_is_found_only_under_its_start() {
        let pid = i32::try_from(std::process::id()).expect("a pid");
        let record = super::process_record(pid)
            .expect("read this process")
            .expect("this process runs");
        // SAFETY: getppid and geteuid take nothing and only read this process's own state.
        let (parent, uid) = unsafe { (libc::getppid(), libc::geteuid()) };
        assert_eq!(
            (record.pid, record.ppid, record.uid),
            (std::process::id(), parent.cast_unsigned(), uid)
        );
        assert_eq!(super::start_time(pid).unwrap(), Some(record.unique_id));
        assert_eq!(super::life_of(pid).unwrap(), Some(record.unique_id));
        assert_eq!(
            super::start_time(parent).unwrap(),
            Some(record.parent_unique_id)
        );
        let life = record.life();
        assert_eq!(
            life.current().unwrap().map(|process| process.pid()),
            Some(pid)
        );
        let other = super::Life {
            unique_id: life.unique_id + 1,
            ..life
        };
        assert!(
            other.current().unwrap().is_none(),
            "the pid under another start was taken for this life"
        );
        assert!(
            !super::signal_life(std::process::id(), life.unique_id + 1, libc::SIGKILL).unwrap(),
            "a signal under another start reached this process"
        );
    }
}
